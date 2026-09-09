//! MQTT client registry state: the per-client handle and supervisor state.
//!
//! `MqttClientHandle` is the single handle type serving both the publish path
//! (pending/inflight ack tracking) and the ingress delivery + reconnect
//! re-assert path. One is built per declared `[[mqtt_client]]`, whether or not
//! anything is bound through it, and held on `MqttService` in a
//! `client_slug`-keyed registry.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, RwLock};

use brenn_lib::mqtt::config::MqttClientConfig;
use brenn_lib::mqtt::error::MqttError;

/// How long [`MqttClientHandle::stop_and_join`] waits for a supervisor it has
/// asked to stop.
///
/// Wide enough for every path the supervisor can be on when the signal lands —
/// a DISCONNECT drain, a panic-respawn pause, then another drain — and narrow
/// enough that a reload's commit cannot sit here unnoticed.
const STOP_JOIN_TIMEOUT: Duration = Duration::from_secs(20);

// ---------------------------------------------------------------------------
// Supervisor state (visible to the registry for health reporting)
// ---------------------------------------------------------------------------

/// What the broker answered for one topic filter's SUBSCRIBE on the current
/// session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubAckOutcome {
    /// Granted: the broker delivers on this filter until the session ends.
    Granted,
    /// Refused, carrying the SUBACK's own words. Final for the session — the
    /// broker does not change its answer without a new SUBSCRIBE.
    Refused(String),
}

/// The live connection state of a supervisor, as readable from outside.
#[derive(Debug, Clone)]
pub enum SupervisorState {
    /// Not yet attempted, or in backoff between attempts.
    Disconnected {
        /// Most recent failure reason, if any.
        last_error: Option<String>,
        /// When the next connect attempt is scheduled.
        next_attempt_at: Instant,
    },
    /// A connect is in progress.
    Connecting { since: Instant },
    /// Connected and subscriptions are active.
    Connected,
    /// Authoritative failure — supervisor has stopped retrying.
    Failed { reason: String },
}

/// Health label surfaced to the LLM (via `MessageChannelList` channel
/// enrichment).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorHealthLabel {
    Connected,
    Disconnected,
    /// Authoritative failure — supervisor has stopped retrying.
    Failed,
}

impl ConnectorHealthLabel {
    /// Lowercase wire string for this label — the single source of truth for the
    /// `health` field rendered to the LLM (`MessageChannelList` enrichment).
    ///
    /// Must stay identical to the `#[serde(rename_all = "lowercase")]` form; the
    /// `wire_str_matches_serde` test pins that equality so a new variant cannot
    /// drift the two apart.
    pub fn wire_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Disconnected => "disconnected",
            Self::Failed => "failed",
        }
    }
}

impl SupervisorState {
    pub fn health_label(&self) -> ConnectorHealthLabel {
        match self {
            SupervisorState::Connected => ConnectorHealthLabel::Connected,
            SupervisorState::Connecting { .. } => ConnectorHealthLabel::Disconnected,
            SupervisorState::Disconnected { .. } => ConnectorHealthLabel::Disconnected,
            SupervisorState::Failed { .. } => ConnectorHealthLabel::Failed,
        }
    }

    pub fn last_error(&self) -> Option<&str> {
        match self {
            SupervisorState::Connected => None,
            SupervisorState::Connecting { .. } => Some("reconnecting"),
            SupervisorState::Disconnected { last_error, .. } => {
                last_error.as_deref().or(Some("unknown"))
            }
            SupervisorState::Failed { reason } => Some(reason.as_str()),
        }
    }
}

// ---------------------------------------------------------------------------
// Pending publish tracking (for QoS 1/2 ack binding)
// ---------------------------------------------------------------------------

/// Outcome of a QoS ≥ 1 publish waiting for PUBACK/PUBCOMP.
#[derive(Debug, Clone)]
pub enum PubackOutcome {
    /// Broker acknowledged with a success reason code.
    Success,
    /// Broker returned a failure PUBACK reason code.
    BrokerRejected { reason: String },
}

/// A publish awaiting pkid binding (pending) or awaiting PUBACK/PUBCOMP (inflight).
pub struct PendingPublish {
    pub ack_tx: tokio::sync::oneshot::Sender<Result<PubackOutcome, MqttError>>,
}

// ---------------------------------------------------------------------------
// Ingress subscription set
// ---------------------------------------------------------------------------

/// One entry in a client's deduplicated ingress subscription set: a topic filter
/// to SUBSCRIBE to, the QoS to request, and the MQTT 5 Subscription Identifier
/// assigned to it.
///
/// `sub_id` exists only to keep the SUBSCRIBE call well-formed; the ingress path
/// does not route by it (the router matches the actual inbound topic against
/// channel filters).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressSubscription {
    /// The MQTT topic filter to subscribe to.
    pub topic_filter: String,
    /// QoS requested from the broker (the client's `[[mqtt_client]].qos`; the
    /// `.max()` fold is a no-op since there is exactly one QoS per client).
    pub qos: u8,
    /// Stable 1-based MQTT 5 Subscription Identifier.
    pub sub_id: u32,
}

// ---------------------------------------------------------------------------
// Client handle
// ---------------------------------------------------------------------------

/// Per-client handle held in the registry. The connection supervisor task keeps
/// a clone of the shared fields and updates them; callers read them through the
/// registry.
///
/// This single handle serves both directions on one session: the publish path
/// (`pending_publishes`/`inflight_publishes`) and the ingress delivery +
/// reconnect re-assert path (`subscriptions`, `pending_subscribes`/
/// `inflight_subscribes` for SubAck filter attribution).
pub struct MqttClientHandle {
    /// Resolved per-client config — the single source of truth for per-client
    /// constants (slug, QoS, urgency, session expiry, broker coordinates,
    /// backoff). Shared with the connection supervisor.
    pub config: Arc<MqttClientConfig>,

    /// The deduplicated union subscription set for this client — the
    /// reconnect-survival set the supervisor re-asserts on every connect.
    /// Runtime-mutable: a dynamic `mqtt:` subscribe pushes a new entry here.
    pub subscriptions: RwLock<Vec<IngressSubscription>>,

    /// Live supervisor state (shared with the supervisor task via `Arc`).
    pub supervisor_state: RwLock<SupervisorState>,

    /// The MQTT client handle, set by the supervisor on connect and cleared on
    /// disconnect. Wrapped in a Mutex so callers can snapshot client + state
    /// atomically for outbound publishes.
    pub client: Mutex<Option<rumqttc::AsyncClient>>,

    /// Pending QoS ≥ 1 publishes waiting for pkid binding (FIFO order).
    pub pending_publishes: Mutex<std::collections::VecDeque<PendingPublish>>,

    /// Inflight publishes (pkid → waiter).
    pub inflight_publishes: Mutex<HashMap<u16, PendingPublish>>,

    /// Filters awaiting pkid binding (FIFO) — one entry per SUBSCRIBE packet,
    /// for SubAck attribution.
    pub pending_subscribes: Mutex<std::collections::VecDeque<String>>,

    /// Inflight subscribes (pkid → filter), resolved at SubAck by `ack.pkid`.
    pub inflight_subscribes: Mutex<HashMap<u16, String>>,

    /// What the broker answered for each topic filter on the *current* session,
    /// as the SubAck arm attributed the answers by pkid.
    ///
    /// Observation only: nothing in the process decides on this map. It answers
    /// "how did my SUBSCRIBE fare at the broker right now", which is a stronger
    /// fact than "the SUBSCRIBE was sent". An entry exists only while
    /// [`Self::subscriptions`] holds the filter, and the map is cleared with the
    /// rest of the per-connect bookkeeping, so it is empty across a session drop
    /// until the re-assert loop's SUBSCRIBEs are answered again.
    subscribe_outcomes: Mutex<HashMap<String, SubAckOutcome>>,

    /// The wake channel used to send a "stop" signal to the supervisor task.
    pub stop_tx: tokio::sync::watch::Sender<bool>,

    /// The outer supervisor task's join handle, set by
    /// [`Self::set_supervisor`] right after the spawn. `None` before the spawn
    /// and after [`Self::stop_and_join`] has taken it.
    supervisor: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl MqttClientHandle {
    pub fn new(
        config: Arc<MqttClientConfig>,
        subscriptions: Vec<IngressSubscription>,
        stop_tx: tokio::sync::watch::Sender<bool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            subscriptions: RwLock::new(subscriptions),
            supervisor_state: RwLock::new(SupervisorState::Disconnected {
                last_error: None,
                next_attempt_at: Instant::now(),
            }),
            client: Mutex::new(None),
            pending_publishes: Mutex::new(std::collections::VecDeque::new()),
            inflight_publishes: Mutex::new(HashMap::new()),
            pending_subscribes: Mutex::new(std::collections::VecDeque::new()),
            inflight_subscribes: Mutex::new(HashMap::new()),
            subscribe_outcomes: Mutex::new(HashMap::new()),
            stop_tx,
            supervisor: Mutex::new(None),
        })
    }

    /// Read the current health label + last_error without holding a long-lived lock.
    pub async fn health_snapshot(&self) -> (ConnectorHealthLabel, Option<String>) {
        let state = self.supervisor_state.read().await;
        let label = state.health_label();
        let err = state.last_error().map(|s| s.to_string());
        (label, err)
    }

    /// Fail all pending and inflight publishes with `MqttError::NotConnected`.
    /// Called by the supervisor when the connection drops.
    pub async fn fail_all_publishes(&self, last_error: Option<String>) {
        let error = MqttError::NotConnected {
            client_slug: self.config.identity.slug.clone(),
            last_error,
        };

        let mut pending = self.pending_publishes.lock().await;
        for p in pending.drain(..) {
            let _ = p.ack_tx.send(Err(error.clone()));
        }

        let mut inflight = self.inflight_publishes.lock().await;
        for (_, p) in inflight.drain() {
            let _ = p.ack_tx.send(Err(error.clone()));
        }
    }

    /// Add a topic filter to this client's runtime subscription set if not already
    /// present, returning the `IngressSubscription` to assert (a fresh entry, or the
    /// existing one if the filter is already subscribed).
    ///
    /// The `sub_id` of a new entry is `max(existing sub_ids) + 1` so it stays unique
    /// within the client's set. If the filter already exists, its existing entry is
    /// returned unchanged and the set is not grown (idempotent). Does not touch the
    /// broker — the caller issues the SUBSCRIBE on the live client.
    pub async fn add_subscription(&self, topic_filter: String, qos: u8) -> IngressSubscription {
        let mut subs = self.subscriptions.write().await;
        if let Some(existing) = subs.iter().find(|s| s.topic_filter == topic_filter) {
            // The higher-level subscribe path forbids re-subscribe-with-different-
            // params, so by the time a re-subscribe for an existing filter reaches
            // here the QoS must match the stored entry. A mismatch means a caller
            // bypassed that guard — a host bug, not bad input — so assert loudly
            // rather than silently keeping the old QoS.
            assert_eq!(
                existing.qos, qos,
                "add_subscription: qos {qos} for existing filter {:?} differs from stored qos {} \
                 — the subscribe path must reject a differing-params re-subscribe before reaching \
                 add_subscription (host bug)",
                existing.topic_filter, existing.qos,
            );
            return existing.clone();
        }
        let next_id = subs.iter().map(|s| s.sub_id).max().unwrap_or(0) + 1;
        let entry = IngressSubscription {
            topic_filter,
            qos,
            sub_id: next_id,
        };
        subs.push(entry.clone());
        entry
    }

    /// Remove the subscription for `topic_filter` from the runtime set if present,
    /// returning `true` if a matching entry was removed. Does not touch the broker.
    ///
    /// The broker's answer for the filter goes with it, whether or not an entry
    /// was present: a filter being taken down is not a filter granted on this
    /// session, and no caller has to remember the pairing.
    pub async fn remove_subscription(&self, topic_filter: &str) -> bool {
        let removed = {
            let mut subs = self.subscriptions.write().await;
            let before = subs.len();
            subs.retain(|s| s.topic_filter != topic_filter);
            subs.len() != before
        };
        self.forget_outcome(topic_filter).await;
        removed
    }

    /// Record the broker's answer for `topic_filter` on the current session.
    ///
    /// A filter the runtime set no longer holds records nothing: an UNSUBSCRIBE
    /// and a SUBACK can cross, and an answer for a filter this process has taken
    /// down would outlive every reader's meaning of it.
    async fn record_outcome(&self, topic_filter: &str, outcome: SubAckOutcome) {
        let held = {
            let subs = self.subscriptions.read().await;
            subs.iter().any(|s| s.topic_filter == topic_filter)
        };
        if !held {
            return;
        }
        self.subscribe_outcomes
            .lock()
            .await
            .insert(topic_filter.to_string(), outcome);
    }

    /// Record `topic_filter` as granted by the broker on the current session.
    pub async fn record_grant(&self, topic_filter: &str) {
        self.record_outcome(topic_filter, SubAckOutcome::Granted)
            .await;
    }

    /// Record the broker's refusal of `topic_filter`, in the SUBACK's own words.
    pub async fn record_refusal(&self, topic_filter: &str, reason: String) {
        self.record_outcome(topic_filter, SubAckOutcome::Refused(reason))
            .await;
    }

    /// Forget the broker's answer for `topic_filter`. Idempotent.
    pub async fn forget_outcome(&self, topic_filter: &str) {
        self.subscribe_outcomes.lock().await.remove(topic_filter);
    }

    /// Whether the broker has granted `topic_filter` on the current session.
    pub async fn is_granted(&self, topic_filter: &str) -> bool {
        matches!(
            self.subscribe_outcomes.lock().await.get(topic_filter),
            Some(SubAckOutcome::Granted)
        )
    }

    /// The broker's answer for `topic_filter`, or `None` if none has come back
    /// on this session.
    pub async fn subscribe_outcome(&self, topic_filter: &str) -> Option<SubAckOutcome> {
        self.subscribe_outcomes
            .lock()
            .await
            .get(topic_filter)
            .cloned()
    }

    /// Forget every answer — the per-connect clear, since a SUBACK belongs to
    /// the session that sent it.
    pub async fn clear_outcomes(&self) {
        self.subscribe_outcomes.lock().await.clear();
    }

    /// Signal the supervisor to stop (sends `true` on the stop watch channel).
    /// Idempotent: repeated calls are safe.
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    /// Record the supervisor task's join handle.
    ///
    /// # Panics
    ///
    /// Panics if a handle is already recorded. One handle serves one client for
    /// as long as that client is registered; a second spawn against the same
    /// handle would leave two supervisors sharing one broker client id.
    pub async fn set_supervisor(&self, join: tokio::task::JoinHandle<()>) {
        let mut slot = self.supervisor.lock().await;
        assert!(
            slot.is_none(),
            "MqttClientHandle::set_supervisor called twice for client {:?}",
            self.config.identity.slug,
        );
        *slot = Some(join);
    }

    /// Signal the supervisor to stop, then wait for it to exit.
    ///
    /// Bounded twice over. The supervisor's stop arm drains the DISCONNECT
    /// under its own timeout and its connect and backoff sleeps race the stop
    /// signal, so every path out of it is short; and the wait itself is under
    /// [`STOP_JOIN_TIMEOUT`], because a commit step that stalls here stalls the
    /// whole reload with no `mqtt:` route installed and no status published,
    /// which is a worse failure than a loud death. Returning means the broker
    /// connection is gone and the client id is free for a successor to take.
    ///
    /// # Panics
    ///
    /// Panics if no handle was recorded; if the wait outlives
    /// [`STOP_JOIN_TIMEOUT`], which means the supervisor has a path out that
    /// does not race the stop signal; or if the supervisor task ended in a way
    /// `tokio` reports as an error — the outer task catches its body's panics
    /// itself, so a join error here means the runtime tore the task down.
    pub async fn stop_and_join(&self) {
        self.stop();
        let join = self.supervisor.lock().await.take().unwrap_or_else(|| {
            panic!(
                "MqttClientHandle::stop_and_join with no supervisor recorded for client {:?}",
                self.config.identity.slug,
            )
        });
        let joined = tokio::time::timeout(STOP_JOIN_TIMEOUT, join)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "MQTT supervisor for client {:?} did not exit within {}s of being asked to \
                     stop — every path out of it is supposed to race the stop signal, so this is \
                     a host bug",
                    self.config.identity.slug,
                    STOP_JOIN_TIMEOUT.as_secs(),
                )
            });
        if let Err(e) = joined {
            panic!(
                "MQTT supervisor for client {:?} could not be joined: {e}",
                self.config.identity.slug,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::mqtt::test_support::test_client_config;

    fn make_handle(client: &str) -> Arc<MqttClientHandle> {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        MqttClientHandle::new(Arc::new(test_client_config(client)), vec![], tx)
    }

    /// `ConnectorHealthLabel::wire_str` must equal the serde `lowercase` form for
    /// every variant, so the single-source `wire_str` cannot drift from the serde
    /// derive.
    #[test]
    fn wire_str_matches_serde() {
        for label in [
            ConnectorHealthLabel::Connected,
            ConnectorHealthLabel::Disconnected,
            ConnectorHealthLabel::Failed,
        ] {
            let serde_form = serde_json::to_value(label)
                .expect("serialize")
                .as_str()
                .expect("string")
                .to_string();
            assert_eq!(label.wire_str(), serde_form);
        }
    }

    #[test]
    fn health_label_mapping() {
        let connected = SupervisorState::Connected;
        assert_eq!(connected.health_label(), ConnectorHealthLabel::Connected);

        let disconnected = SupervisorState::Disconnected {
            last_error: Some("timeout".to_string()),
            next_attempt_at: Instant::now(),
        };
        assert_eq!(
            disconnected.health_label(),
            ConnectorHealthLabel::Disconnected
        );

        let connecting = SupervisorState::Connecting {
            since: Instant::now(),
        };
        assert_eq!(
            connecting.health_label(),
            ConnectorHealthLabel::Disconnected
        );
    }

    #[test]
    fn health_label_failed_state() {
        let failed = SupervisorState::Failed {
            reason: "test authoritative failure".into(),
        };
        assert_eq!(failed.health_label(), ConnectorHealthLabel::Failed);
        assert_eq!(failed.last_error(), Some("test authoritative failure"));
    }

    #[tokio::test]
    async fn add_subscription_appends_with_next_sub_id() {
        let handle = make_handle("broker");
        let added = handle.add_subscription("sensors/#".into(), 0).await;
        assert_eq!(added.topic_filter, "sensors/#");
        assert_eq!(added.sub_id, 1);
        let added2 = handle.add_subscription("home/#".into(), 0).await;
        assert_eq!(added2.sub_id, 2, "new sub_id must be max existing + 1");
    }

    #[tokio::test]
    async fn add_subscription_is_idempotent_for_existing_filter() {
        let handle = make_handle("broker");
        handle.add_subscription("home/+/state".into(), 1).await;
        let added = handle.add_subscription("home/+/state".into(), 1).await;
        assert_eq!(added.sub_id, 1);
        assert_eq!(handle.subscriptions.read().await.len(), 1);
    }

    #[tokio::test]
    #[should_panic(expected = "differs from stored qos")]
    async fn add_subscription_different_qos_for_existing_filter_panics() {
        let handle = make_handle("broker");
        handle.add_subscription("home/+/state".into(), 1).await;
        let _ = handle.add_subscription("home/+/state".into(), 2).await;
    }

    /// Removing a filter reports whether an entry went, and takes the broker's
    /// answer with it either way — including the second call, where no entry is
    /// present.
    #[tokio::test]
    async fn remove_subscription_reports_match() {
        let handle = make_handle("broker");
        handle.add_subscription("sensors/#".into(), 1).await;
        handle.record_grant("sensors/#").await;
        assert!(handle.is_granted("sensors/#").await);

        assert!(handle.remove_subscription("sensors/#").await);
        assert!(!handle.is_granted("sensors/#").await);

        assert!(!handle.remove_subscription("sensors/#").await);
        assert!(!handle.is_granted("sensors/#").await);
    }

    /// An answer is recorded only for a filter the runtime set holds: with the
    /// filter gone, the SubAck arm's `record_*` is a no-op rather than an entry
    /// no reader can retire.
    #[tokio::test]
    async fn an_outcome_for_an_unheld_filter_is_not_recorded() {
        let handle = make_handle("broker");
        handle.record_grant("sensors/#").await;
        handle
            .record_refusal("sensors/#", "Failure".to_string())
            .await;
        assert_eq!(handle.subscribe_outcome("sensors/#").await, None);

        handle.add_subscription("sensors/#".into(), 1).await;
        handle.record_grant("sensors/#").await;
        assert_eq!(
            handle.subscribe_outcome("sensors/#").await,
            Some(SubAckOutcome::Granted)
        );
    }

    #[test]
    fn stop_fires_watch_channel() {
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        let handle = MqttClientHandle::new(Arc::new(test_client_config("broker")), vec![], stop_tx);
        assert!(!*stop_rx.borrow_and_update());
        handle.stop();
        assert!(
            *stop_rx.borrow(),
            "stop() must set the watch channel to true"
        );
    }
}

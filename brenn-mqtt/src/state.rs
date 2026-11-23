//! MQTT client registry state: the per-client handle and supervisor state.
//!
//! `MqttClientHandle` is the single handle type serving both the publish path
//! (the current client and the facts of its connection) and the ingress
//! delivery + reconnect re-assert path. One is built per declared `[[mqtt_client]]`, whether or not
//! anything is bound through it, and held on `MqttService` in a
//! `client_slug`-keyed registry.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify, RwLock, watch};

use brenn_lib::mqtt::config::MqttClientConfig;

use crate::reader::ReaderSlot;

/// Poisoning message for [`MqttClientHandle::pending_subscribes`]. Its critical
/// sections are synchronous and short, so poisoning means a holder panicked
/// mid-section and the FIFO no longer matches the request channel.
pub(crate) const PENDING_SUBSCRIBES_POISONED: &str =
    "pending_subscribes lock poisoned: a holder panicked inside its critical section";

/// Poisoning message for [`MqttClientHandle::deferred_subscribes`].
pub(crate) const DEFERRED_SUBSCRIBES_POISONED: &str =
    "deferred_subscribes lock poisoned: a holder panicked inside its critical section";

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
// Publish outcomes
// ---------------------------------------------------------------------------

/// What the client library reported for one tracked publish when its notice
/// resolved. The egress reader reads every outcome through
/// [`notice_outcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoticeOutcome {
    /// Flushed to the network (QoS 0), or acknowledged with a success reason
    /// (QoS 1/2).
    Delivered,
    /// The broker answered with a failure reason: the packet's reason string
    /// when it carried one, else the reason code's name; and the code.
    Rejected { reason: String, code: u8 },
    /// The connection the publish was submitted on ended before an answer.
    ConnectionEnded,
}

/// Map a resolved publish notice to its outcome.
///
/// # Panics
///
/// On a topic-alias replay failure or a completion-check error: Brenn sets no
/// topic alias and never asks the library to check completion, so either is a
/// host bug.
pub fn notice_outcome(
    result: Result<rumqttc::PublishResult, rumqttc::PublishNoticeError>,
) -> NoticeOutcome {
    use rumqttc::{PubAckReason, PubCompReason, PublishNoticeError as E, PublishResult as R};
    match result {
        Ok(R::Qos0Flushed) => NoticeOutcome::Delivered,
        Ok(R::Qos1(ack)) => {
            if ack.reason == PubAckReason::Success
                || ack.reason == PubAckReason::NoMatchingSubscribers
            {
                NoticeOutcome::Delivered
            } else {
                NoticeOutcome::Rejected {
                    reason: reason_text(ack.properties.and_then(|p| p.reason_string), &ack.reason),
                    code: ack.reason as u8,
                }
            }
        }
        // TODO(mqtt-qos2-completion-refusal-told-rejected)
        Ok(R::Qos2Completed(comp)) => {
            if comp.reason == PubCompReason::Success {
                NoticeOutcome::Delivered
            } else {
                NoticeOutcome::Rejected {
                    reason: reason_text(
                        comp.properties.and_then(|p| p.reason_string),
                        &comp.reason,
                    ),
                    code: comp.reason as u8,
                }
            }
        }
        Ok(R::Qos2PubRecRejected(rec)) => NoticeOutcome::Rejected {
            reason: reason_text(rec.properties.and_then(|p| p.reason_string), &rec.reason),
            code: rec.reason as u8,
        },
        Err(E::Recv | E::SessionReset | E::Qos0NotFlushed) => NoticeOutcome::ConnectionEnded,
        Err(
            e @ (E::TopicAliasReplayUnavailable(_)
            | E::V5PubAck(_)
            | E::V5PubRec(_)
            | E::V5PubComp(_)),
        ) => {
            panic!(
                "publish notice resolved with {e:?}: Brenn sets no topic alias and never asks for a completion check"
            )
        }
    }
}

/// The broker's reason string when it sent one, else the reason code.
fn reason_text(reason_string: Option<String>, code: &dyn std::fmt::Debug) -> String {
    reason_string.unwrap_or_else(|| format!("{code:?}"))
}

/// The Receive Maximum MQTT 5.0 section 3.2.2.3.3 gives a CONNACK that states
/// none.
pub const DEFAULT_RECEIVE_MAXIMUM: u16 = u16::MAX;

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
/// (`client` and the facts of its connection) and the ingress delivery +
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

    /// The Maximum Packet Size the broker advertised in the CONNACK of the
    /// connection whose client [`Self::client`] holds; `0` when it advertised
    /// none, a value MQTT 5.0 section 3.2.2.3.6 forbids the broker to send.
    /// Written and read only under the `client` lock, so a reader sees the
    /// value of the connection whose client it took.
    pub(crate) broker_max_packet_size: AtomicU32,

    /// The Receive Maximum the broker stated in the CONNACK of the connection
    /// whose client [`Self::client`] holds, [`DEFAULT_RECEIVE_MAXIMUM`] when it
    /// stated none. Written and read only under the `client` lock.
    pub(crate) broker_receive_max: AtomicU16,

    /// Counts the connections installed on this handle; the current one's
    /// number. Written and read only under the `client` lock, so a submitter
    /// knows which connection a publish went out on.
    pub(crate) connection_epoch: AtomicU64,

    /// Woken when a connection is installed and whenever the event loop writes
    /// a PUBLISH, which is when room may have freed on the request channel. A
    /// wake, not a state: "connected" is the `client` cell alone.
    pub egress_wake: Notify,

    /// Filters awaiting pkid binding (FIFO) — one entry per SUBSCRIBE packet,
    /// for SubAck attribution.
    ///
    /// The lock exists only to make a push onto this FIFO atomic with the
    /// non-blocking submit of the matching SUBSCRIBE onto the session's request
    /// channel, so the FIFO's order is the order the event loop receives the
    /// requests. Nothing may hold it across an await: the request channel is
    /// drained only by the supervisor's poll, and a holder parked on that
    /// channel would make every other acquirer — the supervisor included — wait
    /// on a poll that cannot happen. It is a `std::sync::Mutex` so that holding
    /// it across an await makes the future `!Send`, which `tokio::spawn`
    /// rejects. A sender that finds the request channel full releases it and
    /// either polls (the supervisor) or hands the subscribe to the supervisor
    /// through [`Self::deferred_subscribes`] (everyone else).
    pub pending_subscribes: std::sync::Mutex<std::collections::VecDeque<String>>,

    /// Subscribes a caller could not submit because the request channel was
    /// full, handed to the supervisor, which takes them in its own loop before
    /// each poll and sends them at the next free slot. See
    /// [`Self::pending_subscribes`] for why a caller never waits for a slot
    /// itself.
    ///
    /// A plain queue, not a channel: nothing awaits it. Callers push and the
    /// supervisor takes everything, both in short synchronous sections, so a
    /// `std::sync::Mutex` suffices and cannot be held across an await. It
    /// lives on the handle rather than in the supervisor task so a supervisor
    /// respawned after a panic drains the same queue.
    ///
    /// A caller holds this lock across its send and the push that follows, so
    /// a drain cannot slip in between and leave the push for a poll that only
    /// traffic or keepalive would end. It therefore takes this lock before
    /// [`Self::pending_subscribes`]; nothing takes the two in the other order,
    /// and the supervisor never holds both.
    pub deferred_subscribes: std::sync::Mutex<std::collections::VecDeque<IngressSubscription>>,

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

    /// The stop signal, observed by the supervisor and the egress reader.
    pub stop_tx: watch::Sender<bool>,

    /// The outer supervisor task, recorded by [`Self::set_supervisor`] right
    /// after the spawn and joined by [`Self::stop_and_join`]. A
    /// `tokio::sync::Mutex` because the join is awaited under it, so a second
    /// stopper queues behind the first and finds the session joined.
    supervisor: Mutex<SupervisorSlot>,

    /// The egress reader, recorded by [`crate::reader::start_egress_reader`].
    /// A `std::sync::Mutex`: every section is a synchronous take or store.
    pub(crate) egress_reader: std::sync::Mutex<ReaderSlot>,

    /// Whether an egress reader is running on this handle: set `true` before
    /// its task is spawned, and `false` by the task as its last act, after its
    /// last report has committed. Every wait for the reader's exit reads this.
    pub(crate) egress_reader_live: watch::Sender<bool>,
}

/// The supervisor recorded on a handle.
enum SupervisorSlot {
    /// No supervisor was spawned for this handle yet.
    NotSpawned,
    Running(tokio::task::JoinHandle<()>),
    /// The supervisor has been joined after the session's stop.
    Joined,
}

/// Poisoning message for [`MqttClientHandle::egress_reader`].
pub(crate) const EGRESS_READER_POISONED: &str =
    "egress reader slot poisoned: a holder panicked inside its critical section";

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
            broker_max_packet_size: AtomicU32::new(0),
            broker_receive_max: AtomicU16::new(DEFAULT_RECEIVE_MAXIMUM),
            connection_epoch: AtomicU64::new(0),
            egress_wake: Notify::new(),
            pending_subscribes: std::sync::Mutex::new(std::collections::VecDeque::new()),
            deferred_subscribes: std::sync::Mutex::new(std::collections::VecDeque::new()),
            inflight_subscribes: Mutex::new(HashMap::new()),
            subscribe_outcomes: Mutex::new(HashMap::new()),
            stop_tx,
            supervisor: Mutex::new(SupervisorSlot::NotSpawned),
            egress_reader: std::sync::Mutex::new(ReaderSlot::Never),
            egress_reader_live: watch::Sender::new(false),
        })
    }

    /// The longest a stop of this session waits for the broker: the client's
    /// `stop_wait_secs`.
    pub fn stop_wait(&self) -> Duration {
        Duration::from_secs(u64::from(self.config.identity.stop_wait_secs))
    }

    /// Read the current health label + last_error without holding a long-lived lock.
    pub async fn health_snapshot(&self) -> (ConnectorHealthLabel, Option<String>) {
        let state = self.supervisor_state.read().await;
        let label = state.health_label();
        let err = state.last_error().map(|s| s.to_string());
        (label, err)
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

    /// Signal the session to stop: sets the stop watch to `true`, whether or
    /// not anything is subscribed yet, so a task that subscribes later sees the
    /// stop. Idempotent.
    pub fn stop(&self) {
        self.stop_tx.send_replace(true);
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
            matches!(*slot, SupervisorSlot::NotSpawned),
            "MqttClientHandle::set_supervisor called twice for client {:?}",
            self.config.identity.slug,
        );
        *slot = SupervisorSlot::Running(join);
    }

    /// Whether a supervisor has been recorded on this handle.
    pub async fn supervisor_spawned(&self) -> bool {
        !matches!(*self.supervisor.lock().await, SupervisorSlot::NotSpawned)
    }

    /// Signal the session to stop, wait for its egress reader to exit, then
    /// join its supervisor.
    ///
    /// The reader stops handing and waits up to [`Self::stop_wait`] for the
    /// outcomes of what it has in flight, reports them, and exits. The
    /// supervisor keeps the connection serviced until then, so the broker's
    /// answers arrive, and then disconnects under the same bound. Neither wait
    /// here has a timeout of its own: every wait a stopping session can be in
    /// is that bound, a bus call, or a select that races the stop signal.
    /// Returning means the broker connection is gone and the client id is free
    /// for a successor to take.
    ///
    /// Idempotent and shareable: a second caller, concurrent or later, queues
    /// on the supervisor slot behind the first and returns once the session is
    /// joined.
    ///
    /// # Panics
    ///
    /// Panics if no supervisor was ever recorded, or if the supervisor task
    /// ended in a way `tokio` reports as an error — the outer task catches its
    /// body's panics itself, so a join error here means the runtime tore the
    /// task down.
    pub async fn stop_and_join(&self) {
        self.stop();
        self.egress_reader_exited().await;
        let mut slot = self.supervisor.lock().await;
        match std::mem::replace(&mut *slot, SupervisorSlot::Joined) {
            SupervisorSlot::NotSpawned => panic!(
                "MqttClientHandle::stop_and_join with no supervisor recorded for client {:?}",
                self.config.identity.slug,
            ),
            SupervisorSlot::Running(join) => {
                if let Err(e) = join.await {
                    panic!(
                        "MQTT supervisor for client {:?} could not be joined: {e}",
                        self.config.identity.slug,
                    );
                }
                // The one place a session's stop has completed, once per
                // session: its reader has exited and its supervisor has ended.
            }
            SupervisorSlot::Joined => {}
        }
    }

    /// Retire the egress reader and wait for it to exit, leaving the session
    /// running. The reader waits up to [`Self::stop_wait`] for the outcomes of
    /// what it has in flight, reports what it had outstanding, moves its
    /// position past everything the channel retains and reports how many that
    /// skipped.
    ///
    /// A reader still ending for the session's stop carries the retire out
    /// before it exits. One that has already finished ending for the stop is
    /// not retired: what the channel still holds for it is not abandoned or
    /// reported dropped, a `warn` line says so, and this returns once the
    /// reader has exited.
    ///
    /// # Panics
    ///
    /// If no reader was started on this handle or it was already retired, or
    /// if the reader has exited and the session's stop is not set.
    pub async fn retire_egress_reader(&self) {
        let retire = self.take_egress_reader().unwrap_or_else(|| {
            panic!(
                "MqttClientHandle::retire_egress_reader with no egress reader running for client \
                 {:?}",
                self.config.identity.slug,
            )
        });
        if retire.send(()).is_err() {
            assert!(
                *self.stop_tx.borrow(),
                "the egress reader of MQTT client {:?} exited without being retired or stopped",
                self.config.identity.slug,
            );
            // TODO(mqtt-egress-removed-client-rows): the messages this reader
            // left unread are never reported `dropped`, `removed`: nothing
            // reports a removed client's channel once its reader has ended.
            tracing::warn!(
                client = %self.config.identity.slug,
                "MQTT egress reader already ended for the session's stop when its client was \
                 removed: messages still on its channel are not reported dropped for the removal",
            );
        }
        self.egress_reader_exited().await;
    }

    /// Whether an egress reader is running on this handle: started, and not
    /// yet exited for a retire or the session's stop.
    #[cfg(any(test, feature = "testutils"))]
    pub fn egress_reader_running(&self) -> bool {
        *self.egress_reader_live.borrow()
    }

    /// Take the running reader's retire sender out of its slot, leaving the
    /// slot `Gone`. `None` when no reader is running.
    fn take_egress_reader(&self) -> Option<tokio::sync::oneshot::Sender<()>> {
        let mut slot = self.egress_reader.lock().expect(EGRESS_READER_POISONED);
        match std::mem::replace(&mut *slot, ReaderSlot::Gone) {
            ReaderSlot::Running { retire } => Some(retire),
            previous @ (ReaderSlot::Never | ReaderSlot::Gone) => {
                *slot = previous;
                None
            }
        }
    }

    /// Return once no egress reader is running on this handle: at once when
    /// none was started or the reader has already exited.
    pub(crate) async fn egress_reader_exited(&self) {
        self.egress_reader_live
            .subscribe()
            .wait_for(|live| !*live)
            .await
            .expect("the handle owns the reader-live sender and outlives this wait");
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

    fn puback(reason: rumqttc::PubAckReason, reason_string: Option<&str>) -> rumqttc::PubAck {
        rumqttc::PubAck {
            pkid: 1,
            reason,
            properties: reason_string.map(|r| rumqttc::PubAckProperties {
                reason_string: Some(r.to_string()),
                user_properties: vec![],
            }),
        }
    }

    /// Every row of the notice table: what is delivered, what is rejected
    /// and with which words, and what means the connection ended first.
    #[test]
    fn notice_outcome_maps_every_resolution() {
        use rumqttc::{
            PubAckReason, PubComp, PubCompProperties, PubCompReason, PubRec, PubRecProperties,
            PubRecReason, PublishNoticeError as E, PublishResult as R,
        };
        let rejected = |reason: &str, code: u8| NoticeOutcome::Rejected {
            reason: reason.to_string(),
            code,
        };

        assert_eq!(notice_outcome(Ok(R::Qos0Flushed)), NoticeOutcome::Delivered);
        for reason in [PubAckReason::Success, PubAckReason::NoMatchingSubscribers] {
            assert_eq!(
                notice_outcome(Ok(R::Qos1(puback(reason, None)))),
                NoticeOutcome::Delivered
            );
        }
        assert_eq!(
            notice_outcome(Ok(R::Qos1(puback(PubAckReason::NotAuthorized, None)))),
            rejected("NotAuthorized", 135),
            "with no reason string the reason code's name is the reason",
        );
        assert_eq!(
            notice_outcome(Ok(R::Qos1(puback(
                PubAckReason::QuotaExceeded,
                Some("over quota")
            )))),
            rejected("over quota", 151),
        );

        let comp = |reason, reason_string: Option<&str>| PubComp {
            pkid: 2,
            reason,
            properties: reason_string.map(|r| PubCompProperties {
                reason_string: Some(r.to_string()),
                user_properties: vec![],
            }),
        };
        assert_eq!(
            notice_outcome(Ok(R::Qos2Completed(comp(PubCompReason::Success, None)))),
            NoticeOutcome::Delivered
        );
        assert_eq!(
            notice_outcome(Ok(R::Qos2Completed(comp(
                PubCompReason::PacketIdentifierNotFound,
                None
            )))),
            rejected("PacketIdentifierNotFound", 146),
        );
        assert_eq!(
            notice_outcome(Ok(R::Qos2Completed(comp(
                PubCompReason::PacketIdentifierNotFound,
                Some("lost it")
            )))),
            rejected("lost it", 146),
        );

        let rec = PubRec {
            pkid: 3,
            reason: PubRecReason::NotAuthorized,
            properties: Some(PubRecProperties {
                reason_string: Some("denied".to_string()),
                user_properties: vec![],
            }),
        };
        assert_eq!(
            notice_outcome(Ok(R::Qos2PubRecRejected(rec))),
            rejected("denied", 135)
        );

        for e in [E::Recv, E::SessionReset, E::Qos0NotFlushed] {
            assert_eq!(notice_outcome(Err(e)), NoticeOutcome::ConnectionEnded);
        }
    }

    #[test]
    #[should_panic(expected = "Brenn sets no topic alias")]
    fn a_topic_alias_replay_failure_panics() {
        notice_outcome(Err(
            rumqttc::PublishNoticeError::TopicAliasReplayUnavailable(1),
        ));
    }

    #[test]
    #[should_panic(expected = "never asks for a completion check")]
    fn a_completion_check_error_panics() {
        notice_outcome(Err(rumqttc::PublishNoticeError::V5PubAck(
            rumqttc::PubAckReason::NotAuthorized,
        )));
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

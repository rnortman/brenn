//! `MqttService` — the MQTT service object held on `AppState`.
//!
//! Owns the per-client handle registry and the `MqttEventRouter` (set after
//! startup via `set_router`). Provides the public API called from
//! `mqtt_intercept.rs` / `mqtt_subscribe.rs` in the binary crate.
//!
//! The `MqttEventRouter` trait is the analogue of `WakeRouter` for messaging:
//! the library crate defines the trait; the binary crate implements it against
//! `AppState` + `ActiveBridges`.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::connection::{assert_ingress_subscription, assert_ingress_unsubscribe};
use crate::payload::InboundPayload;
use crate::state::{
    ConnectorHealthLabel, MqttClientHandle, PendingPublish, PubackOutcome, SubAckOutcome,
    SupervisorState,
};
use brenn_lib::messaging::Urgency;
use brenn_lib::mqtt::config::MqttClientConfig;
use brenn_lib::mqtt::error::MqttError;

// ---------------------------------------------------------------------------
// MqttEventRouter trait
// ---------------------------------------------------------------------------

/// Inbound delivery surface implemented by the binary crate.
///
/// `MqttService` lives in `brenn-lib` and must not depend on binary-crate types.
/// The binary crate provides an adapter that closes over `AppState` and
/// implements this trait; the connection supervisor calls into it via
/// `Arc<dyn MqttEventRouter>`.
///
/// Bridge model: the router owns the bridge routing table and performs
/// topic-filter matching itself. It needs only the **client** the message arrived
/// on (the ACL/provenance boundary), the **actual** published `topic`, the
/// decoded `payload`, and the delivery `qos`.
#[async_trait::async_trait]
pub trait MqttEventRouter: Send + Sync + 'static {
    /// Deliver an inbound MQTT message arriving on `client_slug`.
    ///
    /// The router matches `topic` against every bridge's topic filter for this
    /// client and fans out one typed `mqtt:` channel publish per match. `qos` is
    /// the QoS at which the broker actually delivered this PUBLISH.
    async fn deliver_inbound(
        &self,
        client_slug: &str,
        topic: &str,
        payload: InboundPayload,
        qos: u8,
    );
}

// ---------------------------------------------------------------------------
// Runtime ingress SUBSCRIBE outcome
// ---------------------------------------------------------------------------

/// Outcome of a runtime ingress SUBSCRIBE ([`MqttService::subscribe_filter`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressSubscribeOutcome {
    /// The client was live and the SUBSCRIBE was sent now.
    SubscribedLive,
    /// The client is currently disconnected but its supervisor is still
    /// retrying; the filter is registered and the SUBSCRIBE is deferred to the
    /// next reconnect. Not an error.
    DeferredDisconnected,
    /// The client's supervisor hit an authoritative failure and has stopped
    /// retrying, so there is no reconnect for the filter to be deferred to. The
    /// filter is registered — a later process with working credentials asserts
    /// it — but nothing will be subscribed in this one. Carries the failure
    /// reason.
    ClientFailed(String),
    /// The client was live but the SUBSCRIBE *send* failed. The filter stays
    /// registered (the next reconnect re-asserts it). Carries the client error.
    SendFailed(String),
}

/// Outcome of a runtime ingress UNSUBSCRIBE ([`MqttService::unsubscribe_filter`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressUnsubscribeOutcome {
    /// The client was live and the UNSUBSCRIBE was sent now.
    UnsubscribedLive,
    /// The client is currently disconnected, so no UNSUBSCRIBE was sent. The
    /// filter is out of the reconnect set, so the next reconnect does not
    /// re-assert it — but the session is persistent (`clean_start(false)` with
    /// a session expiry), so the broker resumes it holding the filter and keeps
    /// publishing on it. Nothing in the process converges that; see
    /// `TODO(mqtt-deferred-unsubscribe-not-withdrawn)` at
    /// [`MqttService::unsubscribe_filter`].
    DeferredDisconnected,
    /// The client was live but the UNSUBSCRIBE *send* failed. Carries the client
    /// error string.
    SendFailed(String),
}

// ---------------------------------------------------------------------------
// MqttService
// ---------------------------------------------------------------------------

/// The MQTT service: per-client session registry, event router, and the
/// publish + ingress paths. Always present whether or not any
/// `[[mqtt_client]]` is declared.
pub struct MqttService {
    pub(crate) router: Arc<RwLock<Option<Arc<dyn MqttEventRouter>>>>,
    /// Per-client session handles, keyed by `client_slug`. One session per client
    /// carries both the publish path and the ingress delivery path.
    ///
    /// A reload's commit adds, replaces and removes entries. The lock is held
    /// only for a lookup or a mutation, never across an await, so a caller that
    /// obtained a handle keeps serving through a commit that replaces it.
    pub(crate) clients: std::sync::RwLock<HashMap<String, Arc<MqttClientHandle>>>,
}

/// `last_error` reported for a client with no registered session — the honest
/// "MQTT runtime not present for this client" state.
const NO_CLIENT_SESSION: &str = "no session for client";

/// Poisoning message for the client registry lock. A panic while the registry
/// was being mutated means a commit step died mid-swap; there is no honest way
/// to keep serving from a half-written registry.
const CLIENTS_LOCK: &str = "MqttService client registry lock poisoned";

/// How many times a filter edit may be re-aimed at a freshly registered
/// successor before the churn is read as a defect rather than a reload.
const MAX_RETARGETS: u32 = 8;

/// Count one re-aim of a filter edit that landed on a handle the registry had
/// already replaced, and return the new count.
///
/// # Panics
///
/// Panics once the count passes [`MAX_RETARGETS`]. Each swap is one reload
/// commit restarting this client, and a reload is an operator's deliberate act,
/// so eight of them inside one filter edit is not a busy system — it is a
/// registry being rewritten by something that is not a commit.
fn retarget_on_swap(client_slug: &str, attempts: u32) -> u32 {
    let attempts = attempts + 1;
    assert!(
        attempts <= MAX_RETARGETS,
        "MqttService: a filter edit on client {client_slug:?} was re-aimed {MAX_RETARGETS} times \
         and the registry moved under every one of them — the session for a slug is replaced \
         only by a reload commit, so this is a host bug",
    );
    tracing::debug!(
        client = client_slug,
        attempts,
        "filter edit re-aimed: the client's session was replaced while the edit was in flight",
    );
    attempts
}

impl MqttService {
    /// Construct an empty service. Populate with `add_client` then `set_router`
    /// before serving requests.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            router: Arc::new(RwLock::new(None)),
            clients: std::sync::RwLock::new(HashMap::new()),
        })
    }

    /// Set the inbound event router (called once from the binary crate after
    /// `AppState` is constructed, same deferred-state pattern as `WakeRouter`).
    pub async fn set_router(&self, router: Arc<dyn MqttEventRouter>) {
        let mut guard = self.router.write().await;
        *guard = Some(router);
    }

    /// Get the router (for use by the supervisor).
    pub async fn router(&self) -> Option<Arc<dyn MqttEventRouter>> {
        self.router.read().await.clone()
    }

    /// Register a per-client session handle, keyed by `client_slug`, replacing
    /// any entry the slug already had.
    pub fn add_client(&self, handle: Arc<MqttClientHandle>) {
        self.clients
            .write()
            .expect(CLIENTS_LOCK)
            .insert(handle.config.identity.slug.clone(), handle);
    }

    /// Take the session handle for `client_slug` out of the registry.
    ///
    /// The caller owns the returned handle and is responsible for stopping its
    /// supervisor.
    ///
    /// # Panics
    ///
    /// Panics if no entry is registered for the slug — an absent entry is a
    /// host bug.
    pub fn remove_client(&self, client_slug: &str) -> Arc<MqttClientHandle> {
        self.clients
            .write()
            .expect(CLIENTS_LOCK)
            .remove(client_slug)
            .unwrap_or_else(|| {
                panic!(
                    "MqttService::remove_client: no session registered for client \
                     {client_slug:?}"
                )
            })
    }

    /// Signal every registered supervisor to stop, without waiting for any of
    /// them to exit. Returns the number of supervisors signalled.
    pub fn stop_all(&self) -> usize {
        let clients = self.clients.read().expect(CLIENTS_LOCK);
        for handle in clients.values() {
            handle.stop();
        }
        clients.len()
    }

    /// Look up the session handle for `client_slug`.
    pub fn get_client(&self, client_slug: &str) -> Option<Arc<MqttClientHandle>> {
        self.clients
            .read()
            .expect(CLIENTS_LOCK)
            .get(client_slug)
            .cloned()
    }

    /// Whether `handle` is still the session the registry holds for
    /// `client_slug` — the same allocation, not an equal value.
    fn still_registered(&self, client_slug: &str, handle: &Arc<MqttClientHandle>) -> bool {
        self.clients
            .read()
            .expect(CLIENTS_LOCK)
            .get(client_slug)
            .is_some_and(|current| Arc::ptr_eq(current, handle))
    }

    /// The resolved config of every registered session, by slug.
    ///
    /// A reload's baseline for `[[mqtt_client]]`: what this process is
    /// connected as right now, which is the only side that answers "would a
    /// fresh boot of the candidate dial something else". Secrets included —
    /// they are what the config is compared on.
    pub fn baseline(&self) -> HashMap<String, Arc<MqttClientConfig>> {
        self.clients
            .read()
            .expect(CLIENTS_LOCK)
            .iter()
            .map(|(slug, handle)| (slug.clone(), handle.config.clone()))
            .collect()
    }

    /// Every client that has a registered session, sorted by slug.
    ///
    /// Sorted here rather than by each caller: the registry is a `HashMap`, so
    /// an unsorted return is a different order on every run, and the callers
    /// are assertions comparing the set element for element.
    pub fn client_slugs(&self) -> Vec<String> {
        let mut slugs: Vec<String> = self
            .clients
            .read()
            .expect(CLIENTS_LOCK)
            .keys()
            .cloned()
            .collect();
        slugs.sort();
        slugs
    }

    /// Snapshot the connection health for `client_slug`.
    ///
    /// Returns `(Disconnected, Some(NO_CLIENT_SESSION))` for a client with no
    /// registered session — honest "MQTT runtime not present for this client"
    /// state. For a registered client, health is the three-state supervisor state.
    pub async fn ingress_health(
        &self,
        client_slug: &str,
    ) -> (ConnectorHealthLabel, Option<String>) {
        match self.get_client(client_slug) {
            None => (
                ConnectorHealthLabel::Disconnected,
                Some(NO_CLIENT_SESSION.to_string()),
            ),
            Some(h) => h.health_snapshot().await,
        }
    }

    /// The broker SUBSCRIBE QoS this client's session holds for `topic_filter`.
    ///
    /// Returns `None` if the client has no session or no subscription for that
    /// exact filter.
    pub async fn ingress_filter_qos(&self, client_slug: &str, topic_filter: &str) -> Option<u8> {
        let handle = self.get_client(client_slug)?;
        let subs = handle.subscriptions.read().await;
        subs.iter()
            .find(|s| s.topic_filter == topic_filter)
            .map(|s| s.qos)
    }

    /// Whether the broker has granted `topic_filter` on `client_slug`'s current
    /// session, as the SubAck arm saw it.
    ///
    /// `None` if `client_slug` has no registered session. This is the SubAck as
    /// the process observed it, not the state of the reconnect-survival set: it
    /// answers "the filter reached the broker" rather than "the SUBSCRIBE was
    /// sent", and it goes false again on a session drop.
    pub async fn subscription_acked(&self, client_slug: &str, topic_filter: &str) -> Option<bool> {
        let handle = self.get_client(client_slug)?;
        Some(handle.is_granted(topic_filter).await)
    }

    /// The broker's refusal of `topic_filter` on `client_slug`'s current
    /// session, in the SUBACK's own words.
    ///
    /// `None` if the client has no registered session, if no SUBACK has come
    /// back for the filter, or if the answer was a grant. A refusal is final for
    /// the session, so a reader waiting for the filter can stop waiting on it
    /// rather than wait out a deadline the broker has already answered.
    pub async fn subscription_refusal(
        &self,
        client_slug: &str,
        topic_filter: &str,
    ) -> Option<String> {
        let handle = self.get_client(client_slug)?;
        match handle.subscribe_outcome(topic_filter).await {
            Some(SubAckOutcome::Refused(reason)) => Some(reason),
            Some(SubAckOutcome::Granted) | None => None,
        }
    }

    /// Combined `(qos, health, last_error)` for one `mqtt:` channel's
    /// `(client, topic_filter)`, resolving the handle **once**.
    pub async fn ingress_filter_status(
        &self,
        client_slug: &str,
        topic_filter: &str,
    ) -> (Option<u8>, ConnectorHealthLabel, Option<String>) {
        let Some(handle) = self.get_client(client_slug) else {
            return (
                None,
                ConnectorHealthLabel::Disconnected,
                Some(NO_CLIENT_SESSION.to_string()),
            );
        };
        let qos = handle
            .subscriptions
            .read()
            .await
            .iter()
            .find(|s| s.topic_filter == topic_filter)
            .map(|s| s.qos);
        let (label, last_error) = handle.health_snapshot().await;
        (qos, label, last_error)
    }

    /// The sender-side injection `urgency` for `client_slug`'s session.
    ///
    /// Returns `None` if the client has no session (the caller maps that to a tool
    /// error; we never spawn supervisors at runtime).
    pub async fn ingress_urgency(&self, client_slug: &str) -> Option<Urgency> {
        self.get_client(client_slug)
            .map(|h| h.config.identity.urgency)
    }

    /// The default broker SUBSCRIBE QoS for `client_slug`'s session.
    ///
    /// Returns `None` if the client has no session.
    pub async fn ingress_qos(&self, client_slug: &str) -> Option<u8> {
        self.get_client(client_slug).map(|h| h.config.identity.qos)
    }

    /// Register `topic_filter` (at `qos`) on `client_slug`'s reconnect-survival
    /// set and issue the broker SUBSCRIBE now if the client is live.
    ///
    /// Returns `None` if `client_slug` has no registered session (an unconfigured
    /// MQTT client — the caller maps this to a tool error). This method does
    /// **not** touch the channel directory, the durable subscription row, or the
    /// router table — those are the caller's responsibility.
    ///
    /// A client with no live connection is two different answers: a supervisor
    /// in backoff will assert the filter on its next connect
    /// ([`IngressSubscribeOutcome::DeferredDisconnected`]), one that gave up on
    /// an authoritative failure never will
    /// ([`IngressSubscribeOutcome::ClientFailed`]). The supervisor state is the
    /// only thing that distinguishes them, so it is read here rather than left
    /// for a caller to guess from the empty client cell.
    ///
    /// The filter is added to whichever handle the registry holds when the add
    /// completes: a reload restarting this client swaps a successor in
    /// underneath, and a filter left on the predecessor is lost outright —
    /// its durable row is on both sides of every later reload's comparison, so
    /// no move ever re-asserts it. See [`retarget_on_swap`]. The successor's
    /// set is claimed by the handover until its inherit has been written, so an
    /// edit that reaches the successor mid-handover applies on top of the
    /// inherited set and cannot be discarded by it.
    pub async fn subscribe_filter(
        &self,
        client_slug: &str,
        topic_filter: String,
        qos: u8,
    ) -> Option<IngressSubscribeOutcome> {
        let mut attempts = 0;
        let (handle, sub) = loop {
            let handle = self.get_client(client_slug)?;
            let sub = handle.add_subscription(topic_filter.clone(), qos).await;
            if self.still_registered(client_slug, &handle) {
                break (handle, sub);
            }
            attempts = retarget_on_swap(client_slug, attempts);
        };
        let client = handle.client.lock().await.clone();
        let outcome = match client {
            None => match &*handle.supervisor_state.read().await {
                SupervisorState::Failed { reason } => {
                    IngressSubscribeOutcome::ClientFailed(reason.clone())
                }
                _ => IngressSubscribeOutcome::DeferredDisconnected,
            },
            Some(client) => match assert_ingress_subscription(&handle, &client, &sub).await {
                Ok(()) => IngressSubscribeOutcome::SubscribedLive,
                Err(e) => IngressSubscribeOutcome::SendFailed(e),
            },
        };
        Some(outcome)
    }

    /// Remove `topic_filter` from `client_slug`'s reconnect-survival set and issue
    /// the broker UNSUBSCRIBE now if the client is live. The inverse of
    /// [`Self::subscribe_filter`].
    ///
    /// **The caller must only call this when the removed subscriber was the last
    /// subscriber on the filter.**
    ///
    /// Returns `None` if `client_slug` has no registered session.
    ///
    /// Retargets across a restart for the reason
    /// [`Self::subscribe_filter`] does, in the other direction: a removal left
    /// on the predecessor would let the successor inherit a filter the document
    /// no longer binds and re-assert it at the broker forever.
    pub async fn unsubscribe_filter(
        &self,
        client_slug: &str,
        topic_filter: &str,
    ) -> Option<IngressUnsubscribeOutcome> {
        let mut attempts = 0;
        let (handle, removed) = loop {
            let handle = self.get_client(client_slug)?;
            let removed = handle.remove_subscription(topic_filter).await;
            if self.still_registered(client_slug, &handle) {
                break (handle, removed);
            }
            attempts = retarget_on_swap(client_slug, attempts);
        };
        if !removed {
            tracing::warn!(
                client = client_slug,
                topic_filter,
                "unsubscribe_filter: filter absent from the reconnect set while a durable \
                 dynamic row existed — state inconsistency (durable table vs in-memory set); \
                 sending broker UNSUBSCRIBE anyway"
            );
        }
        let client = handle.client.lock().await.clone();
        let outcome = match client {
            // TODO(mqtt-deferred-unsubscribe-not-withdrawn): the filter stays
            // at the broker for the life of the persistent session.
            None => IngressUnsubscribeOutcome::DeferredDisconnected,
            Some(client) => match assert_ingress_unsubscribe(&client, topic_filter).await {
                Ok(()) => IngressUnsubscribeOutcome::UnsubscribedLive,
                Err(e) => IngressUnsubscribeOutcome::SendFailed(e),
            },
        };
        Some(outcome)
    }

    // Intentionally no MQTT-specific egress listing tool — send-time errors are
    // the signal; do not re-add. Per-client health survives via `ingress_health`
    // (`SupervisorState`-based) and the `MessageChannelList` enrichment.

    // -----------------------------------------------------------------------
    // Publish path
    // -----------------------------------------------------------------------

    /// Publish a message on the session for `handle`.
    ///
    /// - QoS 0: returns success once the client accepts the packet.
    /// - QoS 1/2: blocks until PUBACK/PUBCOMP.
    ///
    /// Returns `Err(MqttError::NotConnected)` synchronously if the client is
    /// currently disconnected (no queueing).
    pub async fn publish_on_handle(
        &self,
        handle: &Arc<MqttClientHandle>,
        topic: String,
        payload: Vec<u8>,
        content_type: Option<String>,
        qos: u8,
        retain: bool,
    ) -> Result<PubackOutcome, MqttError> {
        use rumqttc::PublishProperties;
        use rumqttc::mqttbytes::QoS;

        let rumq_qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => {
                return Err(MqttError::NotConnected {
                    client_slug: handle.config.identity.slug.clone(),
                    last_error: Some(format!("invalid qos: {qos}")),
                });
            }
        };

        let properties = content_type.map(|ct| PublishProperties {
            content_type: Some(ct),
            ..Default::default()
        });

        // Lock client + pending together to maintain FIFO submission order.
        let client_guard = handle.client.lock().await;
        let client = match client_guard.as_ref() {
            None => {
                let state = handle.supervisor_state.read().await;
                return Err(MqttError::NotConnected {
                    client_slug: handle.config.identity.slug.clone(),
                    last_error: state.last_error().map(|s| s.to_string()),
                });
            }
            Some(c) => c.clone(),
        };

        if qos == 0 {
            drop(client_guard);
            let result = if let Some(props) = properties {
                client
                    .publish_with_properties(topic, rumq_qos, retain, payload, props)
                    .await
            } else {
                client.publish(topic, rumq_qos, retain, payload).await
            };
            result.map_err(|e| MqttError::NotConnected {
                client_slug: handle.config.identity.slug.clone(),
                last_error: Some(e.to_string()),
            })?;
            return Ok(PubackOutcome::Success);
        }

        // QoS 1/2: allocate oneshot, push to pending queue, then submit — all
        // under the still-held `client` mutex so the pkid binding order (bound in
        // the supervisor's `Outgoing::Publish` arm, FIFO) matches submission order
        // across concurrent publishers on the shared session.
        //
        // Submission uses the non-blocking `try_publish`, not `publish(...).await`.
        // The blocking form awaits request-channel capacity; awaiting it while
        // holding this mutex deadlocks the session — on a stalled/disconnecting
        // broker the supervisor drains and rebuilds the event loop only after
        // re-acquiring this same `client` mutex, which the awaiting publisher
        // holds. A full channel therefore surfaces synchronously as `NotConnected`
        // rather than blocking the caller and wedging the shared session.
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        {
            let mut pending = handle.pending_publishes.lock().await;
            pending.push_back(PendingPublish { ack_tx });
        }

        let result = if let Some(props) = properties {
            client.try_publish_with_properties(topic, rumq_qos, retain, payload, props)
        } else {
            client.try_publish(topic, rumq_qos, retain, payload)
        };

        if let Err(e) = result {
            // Still holding `client_guard`: no other publisher can have pushed a
            // pending entry since our push above, so ours is provably at the back.
            // (Popping after dropping the guard would race a concurrent publisher
            // and remove *their* waiter.)
            handle.pending_publishes.lock().await.pop_back();
            drop(client_guard);
            return Err(MqttError::NotConnected {
                client_slug: handle.config.identity.slug.clone(),
                last_error: Some(e.to_string()),
            });
        }
        drop(client_guard);

        match ack_rx.await {
            Ok(outcome) => outcome,
            Err(_) => Err(MqttError::NotConnected {
                client_slug: handle.config.identity.slug.clone(),
                last_error: Some("supervisor task dropped the ack channel".to_string()),
            }),
        }
    }
}

impl Default for MqttService {
    fn default() -> Self {
        Self {
            router: Arc::new(RwLock::new(None)),
            clients: std::sync::RwLock::new(HashMap::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handle(client: &str) -> Arc<MqttClientHandle> {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        let config = Arc::new(brenn_lib::mqtt::test_support::test_client_config(client));
        MqttClientHandle::new(config, vec![], tx)
    }

    /// An `AsyncClient` whose eventloop has been dropped: every request send fails
    /// with a closed-channel `ClientError`. Never connects to anything.
    fn dead_live_client() -> rumqttc::AsyncClient {
        use rumqttc::{AsyncClient, MqttOptions};
        let opts = MqttOptions::new("test-dead", ("127.0.0.1", 1));
        let (client, eventloop) = AsyncClient::builder(opts).capacity(1).build();
        drop(eventloop);
        client
    }

    /// A client whose eventloop is still alive: a request send lands in its
    /// channel, so the SUBSCRIBE is answered on the spot.
    fn live_client() -> (rumqttc::AsyncClient, rumqttc::EventLoop) {
        use rumqttc::{AsyncClient, MqttOptions};
        let opts = MqttOptions::new("test-live", ("127.0.0.1", 1));
        AsyncClient::builder(opts).capacity(10).build()
    }

    /// The live arm: a client installed in the cell answers the SUBSCRIBE now
    /// rather than deferring it. The broker cases can only assert this
    /// tolerantly — a session drop mid-test is a deferral they cannot prevent —
    /// so the distinction between the two answers is pinned here.
    #[tokio::test]
    async fn subscribe_filter_live_client_reports_subscribed_live() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        let (client, _eventloop) = live_client();
        *handle.client.lock().await = Some(client);
        svc.add_client(handle.clone());

        let outcome = svc
            .subscribe_filter("home", "sensors/+/temp".to_string(), 1)
            .await;
        assert_eq!(outcome, Some(IngressSubscribeOutcome::SubscribedLive));
        assert_eq!(
            svc.ingress_filter_qos("home", "sensors/+/temp").await,
            Some(1)
        );
    }

    /// `subscription_acked` answers for the session, not for the
    /// reconnect-survival set: an unregistered client has no answer, and a
    /// registered filter nothing has acked is not granted.
    #[tokio::test]
    async fn subscription_acked_reports_the_session_grant() {
        let svc = MqttService::new();
        assert_eq!(svc.subscription_acked("home", "sensors/#").await, None);

        let handle = make_handle("home");
        let (client, _eventloop) = live_client();
        *handle.client.lock().await = Some(client);
        svc.add_client(handle.clone());
        svc.subscribe_filter("home", "sensors/#".to_string(), 1)
            .await;
        assert_eq!(
            svc.subscription_acked("home", "sensors/#").await,
            Some(false),
            "the SUBSCRIBE went out; no SubAck has come back",
        );
    }

    /// `subscription_refusal` is the broker's own answer, and only the broker's:
    /// no session, no SubAck and a grant are all "not refused", and a refusal
    /// carries the reason a waiting reader stops on.
    #[tokio::test]
    async fn subscription_refusal_reports_the_brokers_reason() {
        let svc = MqttService::new();
        assert_eq!(svc.subscription_refusal("home", "sensors/#").await, None);

        let handle = make_handle("home");
        let (client, _eventloop) = live_client();
        *handle.client.lock().await = Some(client);
        svc.add_client(handle.clone());
        svc.subscribe_filter("home", "sensors/#".to_string(), 1)
            .await;
        assert_eq!(
            svc.subscription_refusal("home", "sensors/#").await,
            None,
            "no SubAck has come back, which is not a refusal",
        );

        handle.record_grant("sensors/#").await;
        assert_eq!(
            svc.subscription_refusal("home", "sensors/#").await,
            None,
            "a grant is not a refusal",
        );

        handle
            .record_refusal("sensors/#", "Failure".to_string())
            .await;
        assert_eq!(
            svc.subscription_refusal("home", "sensors/#").await,
            Some("Failure".to_string()),
        );
        assert_eq!(
            svc.subscription_acked("home", "sensors/#").await,
            Some(false),
            "a refused filter is not granted",
        );
    }

    /// Taking a filter down drops its grant, whether or not the UnsubAck has
    /// arrived: the set answers "granted on this session", and an unsubscribed
    /// filter is not.
    #[tokio::test]
    async fn unsubscribe_filter_drops_the_grant() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        let (client, _eventloop) = live_client();
        *handle.client.lock().await = Some(client);
        svc.add_client(handle.clone());
        svc.subscribe_filter("home", "sensors/#".to_string(), 1)
            .await;
        handle.record_grant("sensors/#").await;
        assert_eq!(
            svc.subscription_acked("home", "sensors/#").await,
            Some(true),
            "the grant the broker gave is the answer the barrier reads",
        );

        svc.unsubscribe_filter("home", "sensors/#").await;
        assert_eq!(
            svc.subscription_acked("home", "sensors/#").await,
            Some(false)
        );
    }

    #[tokio::test]
    async fn subscribe_filter_send_failure_reports_sendfailed_and_registers() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        *handle.client.lock().await = Some(dead_live_client());
        svc.add_client(handle.clone());

        let outcome = svc
            .subscribe_filter("home", "sensors/+/temp".to_string(), 1)
            .await;
        match outcome {
            Some(IngressSubscribeOutcome::SendFailed(msg)) => assert!(!msg.is_empty()),
            other => panic!("expected SendFailed, got {other:?}"),
        }
        assert!(
            handle
                .subscriptions
                .read()
                .await
                .iter()
                .any(|s| s.topic_filter == "sensors/+/temp"),
        );
    }

    #[tokio::test]
    async fn add_get_client_round_trip() {
        let svc = MqttService::new();
        svc.add_client(make_handle("home"));

        let got = svc.get_client("home");
        assert!(got.is_some());
        assert_eq!(got.unwrap().config.identity.slug, "home");
        assert!(svc.get_client("nonesuch").is_none());
    }

    #[tokio::test]
    async fn ingress_health_unknown_client_is_disconnected_with_reason() {
        let svc = MqttService::new();
        let (label, err) = svc.ingress_health("nonesuch").await;
        assert_eq!(label, ConnectorHealthLabel::Disconnected);
        assert_eq!(err.as_deref(), Some("no session for client"));
    }

    #[tokio::test]
    async fn ingress_health_registered_but_disconnected() {
        let svc = MqttService::new();
        svc.add_client(make_handle("home"));
        let (label, err) = svc.ingress_health("home").await;
        // A fresh handle starts in Disconnected with no prior error.
        assert_eq!(label, ConnectorHealthLabel::Disconnected);
        assert_eq!(err.as_deref(), Some("unknown"));
    }

    #[tokio::test]
    async fn ingress_filter_qos_reads_subscribed_filter() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        handle
            .add_subscription("sensors/+/temp".to_string(), 2)
            .await;
        svc.add_client(handle);

        assert_eq!(
            svc.ingress_filter_qos("home", "sensors/+/temp").await,
            Some(2)
        );
        assert_eq!(svc.ingress_filter_qos("home", "other/topic").await, None);
        assert_eq!(
            svc.ingress_filter_qos("nonesuch", "sensors/+/temp").await,
            None
        );
    }

    #[tokio::test]
    async fn ingress_urgency_and_qos_read_handle_values() {
        let svc = MqttService::new();
        let (tx, _rx) = tokio::sync::watch::channel(false);
        let mut config = brenn_lib::mqtt::test_support::test_client_config("home");
        config.identity.urgency = Urgency::High;
        config.identity.qos = 2;
        let handle = MqttClientHandle::new(Arc::new(config), vec![], tx);
        svc.add_client(handle);

        assert_eq!(svc.ingress_urgency("home").await, Some(Urgency::High));
        assert_eq!(svc.ingress_qos("home").await, Some(2));
        assert_eq!(svc.ingress_urgency("nonesuch").await, None);
        assert_eq!(svc.ingress_qos("nonesuch").await, None);
    }

    #[tokio::test]
    async fn subscribe_filter_unknown_client_returns_none() {
        let svc = MqttService::new();
        assert!(
            svc.subscribe_filter("nonesuch", "sensors/+/temp".to_string(), 1)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn subscribe_filter_disconnected_defers_but_registers() {
        let svc = MqttService::new();
        svc.add_client(make_handle("home"));

        let outcome = svc
            .subscribe_filter("home", "sensors/+/temp".to_string(), 2)
            .await;
        assert_eq!(outcome, Some(IngressSubscribeOutcome::DeferredDisconnected));
        assert_eq!(
            svc.ingress_filter_qos("home", "sensors/+/temp").await,
            Some(2)
        );
    }

    #[tokio::test]
    async fn unsubscribe_filter_disconnected_removes_from_set() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        svc.add_client(handle.clone());

        svc.subscribe_filter("home", "sensors/+/temp".to_string(), 2)
            .await;
        let outcome = svc.unsubscribe_filter("home", "sensors/+/temp").await;
        assert_eq!(
            outcome,
            Some(IngressUnsubscribeOutcome::DeferredDisconnected)
        );
        assert_eq!(svc.ingress_filter_qos("home", "sensors/+/temp").await, None);
        assert!(handle.subscriptions.read().await.is_empty());
    }

    #[tokio::test]
    async fn unsubscribe_filter_unknown_client_returns_none() {
        let svc = MqttService::new();
        assert!(
            svc.unsubscribe_filter("nonesuch", "sensors/+/temp")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn publish_returns_not_connected_when_disconnected() {
        let svc = MqttService::new();
        let handle = make_handle("broker");
        svc.add_client(handle.clone());
        let result = svc
            .publish_on_handle(
                &handle,
                "test/topic".to_string(),
                b"hello".to_vec(),
                None,
                1,
                false,
            )
            .await;
        assert!(matches!(result, Err(MqttError::NotConnected { .. })));
    }

    #[tokio::test]
    async fn unsubscribe_filter_dead_client_reports_sendfailed_and_removes() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        handle
            .add_subscription("sensors/+/temp".to_string(), 1)
            .await;
        *handle.client.lock().await = Some(dead_live_client());
        svc.add_client(handle.clone());

        let outcome = svc.unsubscribe_filter("home", "sensors/+/temp").await;
        match outcome {
            Some(IngressUnsubscribeOutcome::SendFailed(msg)) => assert!(
                !msg.is_empty(),
                "SendFailed must carry the stringified client error"
            ),
            other => panic!("expected SendFailed, got {other:?}"),
        }
        // Reconnect-set removal precedes the live send, so the filter is gone
        // despite the failed UNSUBSCRIBE — the next reconnect will not re-assert it.
        assert!(
            handle
                .subscriptions
                .read()
                .await
                .iter()
                .all(|s| s.topic_filter != "sensors/+/temp"),
            "a send-failed unsubscribe still removes the filter from the reconnect set"
        );
    }

    #[tokio::test]
    async fn ingress_health_failed_state_reports_failed_with_reason() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        // Supervisor gave up permanently: the terminal state wins over the client cell.
        *handle.supervisor_state.write().await = crate::state::SupervisorState::Failed {
            reason: "authoritative connect failure: boom".to_string(),
        };
        svc.add_client(handle);

        let (label, err) = svc.ingress_health("home").await;
        assert_eq!(label, ConnectorHealthLabel::Failed);
        assert_eq!(err.as_deref(), Some("authoritative connect failure: boom"));
    }

    #[tokio::test]
    async fn ingress_filter_status_failed_state_reports_failed_with_qos() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        svc.add_client(handle.clone());
        // Register a filter so qos is a fact, then drive the session to Failed.
        svc.subscribe_filter("home", "sensors/+/temp".to_string(), 2)
            .await;
        *handle.supervisor_state.write().await = crate::state::SupervisorState::Failed {
            reason: "authoritative disconnect: gone".to_string(),
        };

        let (qos, label, err) = svc.ingress_filter_status("home", "sensors/+/temp").await;
        // The configured filter's QoS is still reported on a failed session.
        assert_eq!(qos, Some(2));
        assert_eq!(label, ConnectorHealthLabel::Failed);
        assert_eq!(err.as_deref(), Some("authoritative disconnect: gone"));
    }

    /// An empty client cell is two different futures, and the caller's report
    /// to an operator differs by which: a supervisor in backoff will assert the
    /// filter on its next connect, one that gave up never will. Reporting the
    /// second as deferred promises delivery that is not coming.
    #[tokio::test]
    async fn subscribe_on_a_disconnected_client_defers_and_on_a_failed_one_does_not() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        svc.add_client(handle.clone());

        // Fresh handle: no client installed, supervisor still retrying.
        assert_eq!(
            svc.subscribe_filter("home", "sensors/+/temp".to_string(), 1)
                .await,
            Some(IngressSubscribeOutcome::DeferredDisconnected)
        );

        *handle.supervisor_state.write().await = SupervisorState::Failed {
            reason: "authoritative connect failure: bad user name or password".to_string(),
        };
        assert_eq!(
            svc.subscribe_filter("home", "sensors/other".to_string(), 1)
                .await,
            Some(IngressSubscribeOutcome::ClientFailed(
                "authoritative connect failure: bad user name or password".to_string()
            ))
        );
        // Either way the filter is in the reconnect-survival set: a process with
        // working credentials asserts it without the document changing.
        assert_eq!(handle.subscriptions.read().await.len(), 2);
    }

    /// The registry is a `HashMap`, so an unsorted return is a fresh order on
    /// every run and every caller comparing the set is intermittently red.
    #[tokio::test]
    async fn client_slugs_is_sorted() {
        let svc = MqttService::new();
        for slug in ["spare", "ha", "attic"] {
            svc.add_client(make_handle(slug));
        }

        assert_eq!(
            svc.client_slugs(),
            vec!["attic".to_string(), "ha".to_string(), "spare".to_string()]
        );
    }

    /// `remove_client` hands the caller the exact handle the registry held, and
    /// the slug stops resolving. The caller owns stopping the supervisor, which
    /// is why it gets the handle rather than a bool.
    #[tokio::test]
    async fn remove_client_takes_the_handle() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        svc.add_client(handle.clone());
        svc.add_client(make_handle("attic"));

        let taken = svc.remove_client("home");
        assert!(
            Arc::ptr_eq(&taken, &handle),
            "remove_client must return the registered handle, not a copy of it"
        );
        assert!(svc.get_client("home").is_none());
        assert_eq!(svc.client_slugs(), vec!["attic".to_string()]);
    }

    /// Removing a slug the registry does not hold is a host bug.
    #[test]
    #[should_panic(expected = "no session registered for client")]
    fn remove_client_panics_on_an_absent_slug() {
        MqttService::new().remove_client("ghost");
    }

    /// A second `add_client` on one slug replaces the entry.
    #[tokio::test]
    async fn add_client_replaces_by_slug() {
        let svc = MqttService::new();
        svc.add_client(make_handle("home"));
        let replacement = make_handle("home");
        svc.add_client(replacement.clone());

        assert_eq!(svc.client_slugs(), vec!["home".to_string()]);
        assert!(Arc::ptr_eq(
            &svc.get_client("home").expect("the slug still resolves"),
            &replacement
        ));
    }

    /// `stop_all` signals every registered handle and nothing else.
    #[tokio::test]
    async fn stop_all_signals_every_registered_handle() {
        let svc = MqttService::new();
        let mut receivers = Vec::new();
        for slug in ["home", "attic"] {
            let handle = make_handle(slug);
            receivers.push(handle.stop_tx.subscribe());
            svc.add_client(handle);
        }
        let unregistered = make_handle("spare");
        let mut spare_rx = unregistered.stop_tx.subscribe();

        assert_eq!(svc.stop_all(), 2);
        for rx in &receivers {
            assert!(*rx.borrow(), "every registered handle is signalled");
        }
        assert!(
            !*spare_rx.borrow_and_update(),
            "a handle the registry does not hold is untouched"
        );
    }

    /// `stop_and_join` signals the supervisor and returns only once the task
    /// has exited.
    #[tokio::test]
    async fn stop_and_join_returns_after_the_supervisor_exits() {
        let handle = make_handle("home");
        let mut stop_rx = handle.stop_tx.subscribe();
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag2 = flag.clone();
        let join = tokio::spawn(async move {
            stop_rx
                .changed()
                .await
                .expect("the stop sender outlives us");
            flag2.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        handle.set_supervisor(join).await;

        handle.stop_and_join().await;
        assert!(
            flag.load(std::sync::atomic::Ordering::SeqCst),
            "stop_and_join must not return before the task ran to completion"
        );
    }

    /// A filter edit that resolved a handle the registry then replaced is
    /// re-aimed at the successor rather than left on the predecessor.
    ///
    /// The predecessor is dropped from the registry by a reload restarting the
    /// client, and a filter left on it is lost for the life of the process: the
    /// durable row behind it is on both sides of every later reload's
    /// comparison, so no move re-asserts it, and the broker is never told.
    ///
    /// Staged rather than raced: holding the predecessor's subscription set
    /// parks the edit inside `add_subscription`, and the swap lands while it is
    /// parked, which is the interleaving the re-aim exists for.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_filter_edit_is_re_aimed_at_the_successor_the_registry_swapped_in() {
        let svc = MqttService::new();
        let old = make_handle("home");
        svc.add_client(old.clone());

        let held = old.subscriptions.write().await;
        let editing = svc.clone();
        let edit = tokio::spawn(async move {
            editing
                .subscribe_filter("home", "home/state".to_string(), 1)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let new = make_handle("home");
        svc.add_client(new.clone());
        drop(held);

        assert!(
            edit.await.expect("the subscribe task").is_some(),
            "the slug resolves throughout",
        );
        assert_eq!(
            old.subscriptions.read().await.len(),
            1,
            "the edit landed on the predecessor first, which is the interleaving under test: \
             an edit that started after the swap would leave this set empty",
        );
        assert_eq!(
            new.subscriptions
                .read()
                .await
                .iter()
                .map(|sub| sub.topic_filter.clone())
                .collect::<Vec<_>>(),
            vec!["home/state".to_string()],
            "the filter is on the session the registry holds",
        );
    }

    /// A second `set_supervisor` would leave two supervisors sharing one broker
    /// client id, so it is a panic rather than a replace.
    #[tokio::test]
    #[should_panic(expected = "set_supervisor called twice")]
    async fn set_supervisor_twice_panics() {
        let handle = make_handle("home");
        handle.set_supervisor(tokio::spawn(async {})).await;
        handle.set_supervisor(tokio::spawn(async {})).await;
    }

    /// Joining a client that never had a supervisor recorded is a host bug.
    #[tokio::test]
    #[should_panic(expected = "no supervisor recorded")]
    async fn stop_and_join_without_a_supervisor_panics() {
        make_handle("home").stop_and_join().await;
    }
}

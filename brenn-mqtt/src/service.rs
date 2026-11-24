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
use std::sync::{Arc, Weak};

use tokio::sync::RwLock;

use crate::connection::unsendable_filter;
use crate::payload::InboundPayload;
use crate::state::{ConnectorHealthLabel, MqttClientHandle, SubAckOutcome, SupervisorState};
use brenn_lib::messaging::Urgency;
use brenn_lib::mqtt::config::MqttClientConfig;
use brenn_lib::panic_util::join_set_reraising;

// ---------------------------------------------------------------------------
// MqttEventRouter trait
// ---------------------------------------------------------------------------

/// Which channels an inbound PUBLISH is for, as the supervisor resolved it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundRouting {
    /// The PUBLISH carried no Subscription Identifier: route by topic into
    /// every channel on the client whose filter covers it.
    ByTopic,
    /// The PUBLISH carried identifiers. These are the topic filters of the
    /// subscriptions it was sent for, each once; never empty.
    BySubscription(Vec<String>),
}

/// A fault in a client's broker session that the operator must hear about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionFault {
    /// The broker acknowledged or released packet identifier `pkid`, which the
    /// client's session state does not hold. The supervisor discarded the
    /// session: the next CONNECT is a clean start, the broker's queued messages
    /// for the client are lost, and publishes in flight end as after a session
    /// reset.
    SessionDiscarded { pkid: u16 },
    /// The connection made by that clean start ended the same way. The client
    /// is stopped until its `mqtt_client` block is reloaded or the server
    /// restarts.
    GaveUp { reason: String },
}

/// Inbound delivery surface implemented by the binary crate, and the way the
/// connection supervisor reports its session's faults.
///
/// `MqttService` lives in `brenn-lib` and must not depend on binary-crate types.
/// The binary crate provides an adapter that closes over `AppState` and
/// implements this trait; the connection supervisor calls into it via
/// `Arc<dyn MqttEventRouter>`.
///
/// Bridge model: the router owns the bridge routing table. It needs the
/// **client** the message arrived on (the ACL/provenance boundary), the
/// **actual** published `topic`, the decoded `payload`, the delivery `qos`,
/// and the subscriptions the broker sent the copy for.
#[async_trait::async_trait]
pub trait MqttEventRouter: Send + Sync + 'static {
    /// Whether a delivery made now would be routed. A supervisor is spawned
    /// only against a router that answers `true`
    /// ([`crate::UnspawnedSession::spawn`] panics otherwise), because a message
    /// the broker hands over is acknowledged on receipt and a router that
    /// cannot route it loses it.
    fn ready(&self) -> bool;

    /// Deliver an inbound MQTT message arriving on `client_slug`.
    ///
    /// With [`InboundRouting::BySubscription`] the router publishes the
    /// message once into the channel of each named filter on this client whose
    /// filter covers `topic`, and into no other channel. With
    /// [`InboundRouting::ByTopic`] it publishes into every channel on this
    /// client whose filter covers `topic`. `qos` is the QoS at which the broker
    /// actually delivered this PUBLISH. `retained` is the PUBLISH's RETAIN flag
    /// as received; Brenn subscribes with Retain As Published 0, so it is set
    /// on a retained message the broker sends because of a SUBSCRIBE and clear
    /// on every message it forwards.
    async fn deliver_inbound(
        &self,
        client_slug: &str,
        topic: &str,
        payload: InboundPayload,
        qos: u8,
        retained: bool,
        routing: InboundRouting,
    );

    /// Report a fault in `client_slug`'s broker session to the operator. The
    /// supervisor has already logged it at `error`.
    async fn session_fault(&self, client_slug: &str, fault: SessionFault);
}

// ---------------------------------------------------------------------------
// Runtime ingress SUBSCRIBE outcome
// ---------------------------------------------------------------------------

/// Outcome of a runtime ingress SUBSCRIBE ([`MqttService::subscribe_filter`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressSubscribeOutcome {
    /// The client was live and the SUBSCRIBE is in the supervisor's hands for
    /// this session, sent at its next free slot.
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
    /// The client is connected and its supervisor has the request: the
    /// UNSUBSCRIBE goes out at its next free slot.
    UnsubscribedLive,
    /// The client has no connection. The filter is withdrawn at the next
    /// connection this process makes for the client, if the broker still holds
    /// the session then; if it holds none there is nothing to withdraw. True
    /// whether the supervisor is in backoff, has not connected yet, or has
    /// failed for good, when the next connection is a reload successor's, which
    /// inherits the filter. A process that ends before that connection leaves
    /// the filter in a session the broker kept; see
    /// `TODO(mqtt-deferred-unsubscribe-not-withdrawn)`.
    DeferredDisconnected,
}

// ---------------------------------------------------------------------------
// MqttService
// ---------------------------------------------------------------------------

/// The MQTT service: per-client session registry, event router, and the
/// publish + ingress paths. Always present whether or not any
/// `[[mqtt_client]]` is declared.
pub struct MqttService {
    pub(crate) router: Arc<RwLock<Option<Arc<dyn MqttEventRouter>>>>,
    /// The session registry. One lock over the client map, the stopping flag and
    /// the displaced sessions, so a session cannot slip between `stop_all`'s
    /// signal and an `add_client` or `remove_client` racing it.
    ///
    /// A reload's commit adds, replaces and removes entries. The lock is held
    /// only for a lookup or a mutation, never across an await, so a caller that
    /// obtained a handle keeps serving through a commit that replaces it.
    registry: std::sync::RwLock<Registry>,
}

/// What [`MqttService`] holds under its registry lock.
#[derive(Default)]
struct Registry {
    /// Per-client session handles, keyed by `client_slug`. One session per
    /// client carries both the publish path and the ingress delivery path.
    clients: HashMap<String, Arc<MqttClientHandle>>,
    /// Set by [`MqttService::stop_all`] and never cleared: the process is
    /// shutting down, so every session registered from here on is born stopped.
    stopping: bool,
    /// Sessions the registry has let go of — replaced by `add_client` or taken
    /// by `remove_client` — that may not have finished stopping. Weak: the
    /// caller that took the session holds it for as long as it is stopping it,
    /// so a dead entry is a session nobody is stopping any more.
    displaced: Vec<Weak<MqttClientHandle>>,
}

impl Registry {
    /// Record `handle` as let go of, pruning entries nothing holds.
    fn displace(&mut self, handle: &Arc<MqttClientHandle>) {
        self.displaced.retain(|weak| weak.strong_count() > 0);
        self.displaced.push(Arc::downgrade(handle));
    }

    /// Every registered session and every displaced one still held.
    fn every_session(&self) -> Vec<Arc<MqttClientHandle>> {
        self.clients
            .values()
            .cloned()
            .chain(self.displaced.iter().filter_map(Weak::upgrade))
            .collect()
    }
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
        Arc::new(Self::default())
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
    /// any entry the slug already had. The replaced handle is kept as displaced
    /// for [`Self::join_all`].
    ///
    /// On a service that [`Self::stop_all`] has stopped, the handle's stop is
    /// signalled before it is inserted: its supervisor never connects and an
    /// egress reader started on it hands nothing.
    pub fn add_client(&self, handle: Arc<MqttClientHandle>) {
        let mut registry = self.registry.write().expect(CLIENTS_LOCK);
        if registry.stopping {
            handle.stop();
        }
        let slug = handle.config.identity.slug.clone();
        if let Some(replaced) = registry.clients.insert(slug, handle) {
            registry.displace(&replaced);
        }
    }

    /// Take the session handle for `client_slug` out of the registry.
    ///
    /// The caller owns the returned handle and is responsible for stopping its
    /// supervisor. The handle is kept as displaced for [`Self::join_all`].
    ///
    /// # Panics
    ///
    /// Panics if no entry is registered for the slug — an absent entry is a
    /// host bug.
    pub fn remove_client(&self, client_slug: &str) -> Arc<MqttClientHandle> {
        let mut registry = self.registry.write().expect(CLIENTS_LOCK);
        let handle = registry.clients.remove(client_slug).unwrap_or_else(|| {
            panic!("MqttService::remove_client: no session registered for client {client_slug:?}")
        });
        registry.displace(&handle);
        handle
    }

    /// Mark the service stopping and signal every registered and every
    /// displaced session to stop, without waiting for any of them. Every
    /// session registered afterwards is born stopped ([`Self::add_client`]).
    /// Returns the number of sessions signalled.
    pub fn stop_all(&self) -> usize {
        let mut registry = self.registry.write().expect(CLIENTS_LOCK);
        registry.stopping = true;
        let sessions = registry.every_session();
        for handle in &sessions {
            handle.stop();
        }
        sessions.len()
    }

    /// Stop and join every registered and every displaced session, all
    /// concurrently, and return how many were joined. Each join waits for the
    /// session's egress reader to report what it had in flight and for its
    /// supervisor to end ([`MqttClientHandle::stop_and_join`]), so the wait as a
    /// whole ends at the largest `stop_wait_secs` among them plus bus commits.
    ///
    /// A session whose supervisor was never spawned is skipped: it is a
    /// successor a reload registered and has not yet spawned, born stopped, so
    /// it never connects and its reader hands nothing.
    ///
    /// # Panics
    ///
    /// Panics if [`Self::stop_all`] has not been called: a session registered
    /// after this snapshot would not be born stopped and would outlive the
    /// join. Panics with a join's own panic.
    pub async fn join_all(&self) -> usize {
        let sessions = {
            let registry = self.registry.read().expect(CLIENTS_LOCK);
            assert!(
                registry.stopping,
                "MqttService::join_all before stop_all: sessions registered after the join's \
                 snapshot would not be stopped",
            );
            registry.every_session()
        };
        let mut joins = tokio::task::JoinSet::new();
        for handle in sessions {
            joins.spawn(async move {
                if handle.supervisor_spawned().await {
                    handle.stop_and_join().await;
                    true
                } else {
                    false
                }
            });
        }
        join_set_reraising(joins, "MqttService::join_all")
            .await
            .into_iter()
            .filter(|joined| *joined)
            .count()
    }

    /// Look up the session handle for `client_slug`.
    pub fn get_client(&self, client_slug: &str) -> Option<Arc<MqttClientHandle>> {
        self.registry
            .read()
            .expect(CLIENTS_LOCK)
            .clients
            .get(client_slug)
            .cloned()
    }

    /// Whether `handle` is still the session the registry holds for
    /// `client_slug` — the same allocation, not an equal value.
    fn still_registered(&self, client_slug: &str, handle: &Arc<MqttClientHandle>) -> bool {
        self.registry
            .read()
            .expect(CLIENTS_LOCK)
            .clients
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
        self.registry
            .read()
            .expect(CLIENTS_LOCK)
            .clients
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
            .registry
            .read()
            .expect(CLIENTS_LOCK)
            .clients
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

    /// The broker SUBSCRIBE QoS this client's session holds `topic_filter` at:
    /// the `qos` of the session's configuration, which every filter of the
    /// session is subscribed at.
    ///
    /// Returns `None` if the client has no session or no subscription for that
    /// exact filter.
    pub async fn ingress_filter_qos(&self, client_slug: &str, topic_filter: &str) -> Option<u8> {
        let handle = self.get_client(client_slug)?;
        let subs = handle.subscriptions.read().await;
        subs.get(topic_filter).map(|_| handle.config.identity.qos)
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
    /// `(client, topic_filter)`, resolving the handle **once**. The QoS is the
    /// session's configured `qos` when the session holds the filter, as in
    /// [`Self::ingress_filter_qos`].
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
            .get(topic_filter)
            .map(|_| handle.config.identity.qos);
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

    /// Register `topic_filter` on `client_slug`'s reconnect-survival set and,
    /// if the client is live, hand the broker SUBSCRIBE to its supervisor, which
    /// sends it at the `qos` of the session it goes out on. A filter the MQTT
    /// library would refuse is answered [`IngressSubscribeOutcome::SendFailed`]
    /// and handed to nobody.
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
    ) -> Option<IngressSubscribeOutcome> {
        let mut attempts = 0;
        let (handle, sub) = loop {
            let handle = self.get_client(client_slug)?;
            let sub = handle.add_subscription(topic_filter.clone()).await;
            if self.still_registered(client_slug, &handle) {
                break (handle, sub);
            }
            attempts = retarget_on_swap(client_slug, attempts);
        };
        let connected = handle.client.lock().await.is_some();
        let outcome = if !connected {
            match &*handle.supervisor_state.read().await {
                SupervisorState::Failed { reason } => {
                    IngressSubscribeOutcome::ClientFailed(reason.clone())
                }
                _ => IngressSubscribeOutcome::DeferredDisconnected,
            }
        } else if let Some(e) = unsendable_filter(&sub.topic_filter) {
            IngressSubscribeOutcome::SendFailed(e)
        } else {
            // Only the supervisor submits a SUBSCRIBE, so the order in which
            // the event loop takes SUBSCRIBEs is the order the supervisor
            // records them for attribution, across a reconnect too.
            handle.request_settle(sub.topic_filter);
            IngressSubscribeOutcome::SubscribedLive
        };
        Some(outcome)
    }

    /// Remove `topic_filter` from `client_slug`'s held set and, if the client
    /// has a connection, name the filter to its supervisor, which sends the
    /// broker UNSUBSCRIBE at its next free slot. With no connection nothing is
    /// named: the filter stays asserted, and the next connection that finds the
    /// broker's session withdraws it. The inverse of [`Self::subscribe_filter`].
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
                 the supervisor withdraws it if it is asserted"
            );
        }
        let connected = handle.client.lock().await.is_some();
        let outcome = if connected {
            handle.request_settle(topic_filter.to_string());
            IngressUnsubscribeOutcome::UnsubscribedLive
        } else {
            IngressUnsubscribeOutcome::DeferredDisconnected
        };
        Some(outcome)
    }

    // Intentionally no MQTT-specific egress listing tool — send-time errors are
    // the signal; do not re-add. Per-client health survives via `ingress_health`
    // (`SupervisorState`-based) and the `MessageChannelList` enrichment.
}

/// The library's QoS for an MQTT QoS number; `None` above 2.
pub(crate) fn qos_of(qos: u8) -> Option<rumqttc::mqttbytes::QoS> {
    match qos {
        0 => Some(rumqttc::mqttbytes::QoS::AtMostOnce),
        1 => Some(rumqttc::mqttbytes::QoS::AtLeastOnce),
        2 => Some(rumqttc::mqttbytes::QoS::ExactlyOnce),
        _ => None,
    }
}

/// The registry is the client table the bus admits an `mqtt:` publish against:
/// reload already keeps it current, so the bus holds no second copy to swap.
impl brenn_lib::mqtt::MqttClientDirectory for MqttService {
    fn outbound_payload_cap(&self, client: &str) -> Option<u32> {
        self.get_client(client)
            .map(|handle| handle.config.identity.outbound_payload_cap_bytes)
    }
}

impl Default for MqttService {
    fn default() -> Self {
        Self {
            router: Arc::new(RwLock::new(None)),
            registry: std::sync::RwLock::new(Registry::default()),
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::unpolled_client;
    use crate::state::IngressSubscription;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn make_handle(client: &str) -> Arc<MqttClientHandle> {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        let config = Arc::new(brenn_lib::mqtt::test_support::test_client_config(client));
        MqttClientHandle::new(config, vec![], tx)
    }

    /// A handle for `client` whose configuration's `qos` is `qos`.
    fn make_handle_at(client: &str, qos: u8) -> Arc<MqttClientHandle> {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        let mut config = brenn_lib::mqtt::test_support::test_client_config(client);
        config.identity.qos = qos;
        MqttClientHandle::new(Arc::new(config), vec![], tx)
    }

    /// A client whose eventloop is still alive: a request send lands in its
    /// channel, so the SUBSCRIBE is answered on the spot.
    fn live_client() -> (rumqttc::AsyncClient, rumqttc::EventLoop) {
        unpolled_client(10)
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
            .subscribe_filter("home", "sensors/+/temp".to_string())
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
        svc.subscribe_filter("home", "sensors/#".to_string()).await;
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
        svc.subscribe_filter("home", "sensors/#".to_string()).await;
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
        svc.subscribe_filter("home", "sensors/#".to_string()).await;
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

    /// A filter rumqttc would refuse is a failed send: it is reported, not
    /// handed to the supervisor, and stays registered.
    #[tokio::test]
    async fn subscribe_filter_rejected_filter_reports_sendfailed_and_registers() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        let (client, _eventloop) = live_client();
        *handle.client.lock().await = Some(client);
        svc.add_client(handle.clone());

        let outcome = svc.subscribe_filter("home", "a/#/b".to_string()).await;
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
                .any(|s| s.topic_filter == "a/#/b"),
        );
        assert!(
            handle.pending_subscribes.lock().unwrap().is_empty(),
            "a refused filter is never pushed for attribution",
        );
        assert!(
            handle.filter_requests.lock().unwrap().is_empty(),
            "and never handed to the supervisor",
        );
    }

    /// With a client installed, a subscribe is handed to the supervisor: it is
    /// queued for the supervisor's next pass with a wake, recorded nowhere for
    /// attribution, and answered `SubscribedLive`. The supervisor is the one
    /// task that submits SUBSCRIBEs.
    #[tokio::test]
    async fn subscribe_filter_with_a_client_hands_the_subscribe_to_the_supervisor() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        let (client, mut eventloop) = live_client();
        *handle.client.lock().await = Some(client);
        svc.add_client(handle.clone());

        let first = svc.subscribe_filter("home", "sensors/a".to_string()).await;
        let second = svc.subscribe_filter("home", "sensors/b".to_string()).await;
        assert_eq!(first, Some(IngressSubscribeOutcome::SubscribedLive));
        assert_eq!(second, Some(IngressSubscribeOutcome::SubscribedLive));
        let requested: Vec<String> = handle
            .filter_requests
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect();
        assert_eq!(
            requested,
            vec!["sensors/a".to_string(), "sensors/b".to_string()],
            "both subscribes are in the supervisor's hands, in order",
        );
        assert!(
            handle.pending_subscribes.lock().unwrap().is_empty(),
            "only the supervisor records a SUBSCRIBE for attribution",
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.filter_wake.notified(),
        )
        .await
        .expect("the hand-off wakes the supervisor");
        eventloop.clean();
        assert_eq!(
            eventloop.pending_len(),
            0,
            "nothing was put on the client's request channel",
        );
        assert!(
            handle
                .subscriptions
                .read()
                .await
                .iter()
                .any(|s| s.topic_filter == "sensors/b"),
            "a handed filter is in the reconnect set, so a connection that ends before \
             the supervisor sends it re-asserts it on the next connect",
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
        let handle = make_handle_at("home", 0);
        handle.add_subscription("sensors/+/temp".to_string()).await;
        svc.add_client(handle);

        assert_eq!(
            svc.ingress_filter_qos("home", "sensors/+/temp").await,
            Some(0)
        );
        assert_eq!(svc.ingress_filter_qos("home", "other/topic").await, None);
        assert_eq!(
            svc.ingress_filter_qos("nonesuch", "sensors/+/temp").await,
            None
        );
    }

    #[tokio::test]
    async fn ingress_urgency_reads_the_handle_value() {
        let svc = MqttService::new();
        let (tx, _rx) = tokio::sync::watch::channel(false);
        let mut config = brenn_lib::mqtt::test_support::test_client_config("home");
        config.identity.urgency = Urgency::High;
        let handle = MqttClientHandle::new(Arc::new(config), vec![], tx);
        svc.add_client(handle);

        assert_eq!(svc.ingress_urgency("home").await, Some(Urgency::High));
        assert_eq!(svc.ingress_urgency("nonesuch").await, None);
    }

    #[tokio::test]
    async fn subscribe_filter_unknown_client_returns_none() {
        let svc = MqttService::new();
        assert!(
            svc.subscribe_filter("nonesuch", "sensors/+/temp".to_string())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn subscribe_filter_disconnected_defers_but_registers() {
        let svc = MqttService::new();
        svc.add_client(make_handle_at("home", 0));

        let outcome = svc
            .subscribe_filter("home", "sensors/+/temp".to_string())
            .await;
        assert_eq!(outcome, Some(IngressSubscribeOutcome::DeferredDisconnected));
        assert_eq!(
            svc.ingress_filter_qos("home", "sensors/+/temp").await,
            Some(0)
        );
    }

    /// The entry for `topic_filter` on `client_slug`'s registered handle.
    async fn entry(
        svc: &MqttService,
        client_slug: &str,
        topic_filter: &str,
    ) -> IngressSubscription {
        svc.get_client(client_slug)
            .expect("client registered")
            .subscriptions
            .read()
            .await
            .get(topic_filter)
            .cloned()
            .expect("filter held")
    }

    /// A second subscribe of a held filter, the shape a second subscriber of the
    /// filter's channel takes, answers like the first and does not grow the set.
    #[tokio::test]
    async fn subscribe_filter_on_a_held_filter_keeps_one_entry() {
        let svc = MqttService::new();
        svc.add_client(make_handle("home"));
        svc.subscribe_filter("home", "sensors/#".to_string()).await;
        let before = entry(&svc, "home", "sensors/#").await;

        let outcome = svc.subscribe_filter("home", "sensors/#".to_string()).await;
        assert_eq!(outcome, Some(IngressSubscribeOutcome::DeferredDisconnected));
        assert_eq!(entry(&svc, "home", "sensors/#").await, before);
        assert_eq!(
            svc.get_client("home")
                .expect("client registered")
                .subscriptions
                .read()
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn unsubscribe_filter_disconnected_removes_from_set() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        svc.add_client(handle.clone());

        svc.subscribe_filter("home", "sensors/+/temp".to_string())
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

    /// With a client installed, a withdrawal is handed to the supervisor: the
    /// filter leaves the held set, stays asserted, is queued for the
    /// supervisor with a wake, and nothing is put on the request channel.
    #[tokio::test]
    async fn unsubscribe_filter_with_a_client_hands_the_withdrawal_to_the_supervisor() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        handle.add_subscription("sensors/+/temp".to_string()).await;
        let (client, mut eventloop) = live_client();
        *handle.client.lock().await = Some(client);
        svc.add_client(handle.clone());

        let outcome = svc.unsubscribe_filter("home", "sensors/+/temp").await;
        assert_eq!(outcome, Some(IngressUnsubscribeOutcome::UnsubscribedLive));
        let set = handle.subscriptions.read().await;
        assert!(
            set.get("sensors/+/temp").is_none(),
            "the filter is no longer held"
        );
        assert!(
            set.is_asserted("sensors/+/temp"),
            "the filter stays asserted until the broker answers its UNSUBSCRIBE",
        );
        drop(set);
        assert_eq!(
            handle
                .filter_requests
                .lock()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec!["sensors/+/temp"],
        );
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.filter_wake.notified(),
        )
        .await
        .expect("the hand-off wakes the supervisor");
        eventloop.clean();
        assert_eq!(eventloop.pending_len(), 0, "only the supervisor submits");
    }

    /// With no client installed a withdrawal is deferred whether the
    /// supervisor is retrying or has failed for good: the filter stays
    /// asserted for the next connection, and nothing is queued, since that
    /// connection settles every asserted filter.
    #[tokio::test]
    async fn unsubscribe_filter_without_a_client_defers_in_backoff_and_failed_alike() {
        for failed in [false, true] {
            let svc = MqttService::new();
            let handle = make_handle("home");
            handle.add_subscription("sensors/#".to_string()).await;
            if failed {
                *handle.supervisor_state.write().await = SupervisorState::Failed {
                    reason: "bad credentials".to_string(),
                };
            }
            svc.add_client(handle.clone());

            let outcome = svc.unsubscribe_filter("home", "sensors/#").await;
            assert_eq!(
                outcome,
                Some(IngressUnsubscribeOutcome::DeferredDisconnected),
                "failed: {failed}",
            );
            assert!(handle.subscriptions.read().await.is_asserted("sensors/#"));
            assert!(handle.filter_requests.lock().unwrap().is_empty());
        }
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
        let handle = make_handle_at("home", 0);
        svc.add_client(handle.clone());
        // Register a filter so qos is a fact, then drive the session to Failed.
        svc.subscribe_filter("home", "sensors/+/temp".to_string())
            .await;
        *handle.supervisor_state.write().await = crate::state::SupervisorState::Failed {
            reason: "authoritative disconnect: gone".to_string(),
        };

        let (qos, label, err) = svc.ingress_filter_status("home", "sensors/+/temp").await;
        // The configured filter's QoS is still reported on a failed session.
        assert_eq!(qos, Some(0));
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
            svc.subscribe_filter("home", "sensors/+/temp".to_string())
                .await,
            Some(IngressSubscribeOutcome::DeferredDisconnected)
        );

        *handle.supervisor_state.write().await = SupervisorState::Failed {
            reason: "authoritative connect failure: bad user name or password".to_string(),
        };
        assert_eq!(
            svc.subscribe_filter("home", "sensors/other".to_string())
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

    /// The bus's client directory reads the registry: a registered slug answers
    /// its configured outbound cap, and an absent or removed one answers none.
    #[tokio::test]
    async fn the_client_directory_reads_the_registry() {
        use brenn_lib::mqtt::MqttClientDirectory;
        let svc = MqttService::new();
        let handle = make_handle("home");
        let cap = handle.config.identity.outbound_payload_cap_bytes;
        svc.add_client(handle);

        assert_eq!(svc.outbound_payload_cap("home"), Some(cap));
        assert_eq!(svc.outbound_payload_cap("attic"), None);
        svc.remove_client("home");
        assert_eq!(svc.outbound_payload_cap("home"), None);
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

    /// A handle whose supervisor is a task that ends at the stop signal and sets
    /// the returned flag as it does.
    async fn supervised_handle(client: &str) -> (Arc<MqttClientHandle>, Arc<AtomicBool>) {
        let handle = make_handle(client);
        let mut stop_rx = handle.stop_tx.subscribe();
        let ended = Arc::new(AtomicBool::new(false));
        let ended_by_task = ended.clone();
        let join = tokio::spawn(async move {
            stop_rx
                .wait_for(|stop| *stop)
                .await
                .expect("the stop sender outlives us");
            ended_by_task.store(true, Ordering::SeqCst);
        });
        handle.set_supervisor(join).await;
        (handle, ended)
    }

    /// `stop_all` reaches the sessions the registry has let go of while their
    /// takers still hold them — a predecessor a reload replaced, a client it
    /// removed — and not one nobody holds any more.
    #[tokio::test]
    async fn stop_all_signals_displaced_handles_and_marks_the_service_stopping() {
        let svc = MqttService::new();
        let replaced = make_handle("home");
        svc.add_client(replaced.clone());
        svc.add_client(make_handle("home"));
        let removed = make_handle("attic");
        svc.add_client(removed.clone());
        assert!(Arc::ptr_eq(&svc.remove_client("attic"), &removed));
        svc.add_client(make_handle("spare"));
        svc.remove_client("spare");

        assert_eq!(
            svc.stop_all(),
            3,
            "the registered successor and the two displaced handles still held; the dropped \
             one is pruned",
        );
        assert!(*replaced.stop_tx.borrow(), "the replaced predecessor");
        assert!(*removed.stop_tx.borrow(), "the removed client");
        assert!(*svc.get_client("home").unwrap().stop_tx.borrow());
        assert!(svc.registry.read().unwrap().stopping);
    }

    /// A client a reload registers after the shutdown signal is born stopped,
    /// so its supervisor never connects and its reader hands nothing.
    #[tokio::test]
    async fn a_client_added_to_a_stopping_service_is_born_stopped() {
        let svc = MqttService::new();
        svc.stop_all();
        let late = make_handle("home");
        let stop_rx = late.stop_tx.subscribe();
        svc.add_client(late);
        assert!(*stop_rx.borrow(), "signalled before it was registered");
    }

    /// `join_all` joins the registered sessions and a displaced predecessor,
    /// and skips a successor whose supervisor was never spawned, which would
    /// otherwise panic in `stop_and_join`.
    #[tokio::test]
    async fn join_all_joins_displaced_sessions_and_skips_unspawned_ones() {
        let svc = MqttService::new();
        let (predecessor, predecessor_ended) = supervised_handle("home").await;
        svc.add_client(predecessor.clone());
        let unspawned = make_handle("home");
        svc.add_client(unspawned.clone());
        let (attic, attic_ended) = supervised_handle("attic").await;
        svc.add_client(attic);

        svc.stop_all();
        assert_eq!(svc.join_all().await, 2);
        assert!(predecessor_ended.load(Ordering::SeqCst));
        assert!(attic_ended.load(Ordering::SeqCst));
        assert!(
            !unspawned.supervisor_spawned().await,
            "the unspawned successor was not joined",
        );
        assert_eq!(
            svc.join_all().await,
            2,
            "a second join finds both sessions already joined and returns",
        );
    }

    /// Joining before the service is stopping would let a session registered
    /// after the snapshot run past the join.
    #[tokio::test]
    #[should_panic(expected = "join_all before stop_all")]
    async fn join_all_before_stop_all_panics() {
        MqttService::new().join_all().await;
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
                .subscribe_filter("home", "home/state".to_string())
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

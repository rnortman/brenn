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

use crate::messaging::Urgency;
use crate::mqtt::connection::{assert_ingress_subscription, assert_ingress_unsubscribe};
use crate::mqtt::error::MqttError;
use crate::mqtt::payload::InboundPayload;
use crate::mqtt::state::{ConnectorHealthLabel, MqttClientHandle, PendingPublish, PubackOutcome};

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
    /// The client is currently disconnected; the filter is registered and the
    /// SUBSCRIBE is deferred to the next reconnect. Not an error.
    DeferredDisconnected,
    /// The client was live but the SUBSCRIBE *send* failed. The filter stays
    /// registered (the next reconnect re-asserts it). Carries the client error.
    SendFailed(String),
}

/// Outcome of a runtime ingress UNSUBSCRIBE ([`MqttService::unsubscribe_filter`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressUnsubscribeOutcome {
    /// The client was live and the UNSUBSCRIBE was sent now.
    UnsubscribedLive,
    /// The client is currently disconnected; the filter is removed from the
    /// reconnect set, so the next reconnect will not re-subscribe it. Not an error.
    DeferredDisconnected,
    /// The client was live but the UNSUBSCRIBE *send* failed. Carries the client
    /// error string.
    SendFailed(String),
}

// ---------------------------------------------------------------------------
// MqttService
// ---------------------------------------------------------------------------

/// The MQTT service. Owned once at startup and held on `AppState` as
/// `Option<Arc<MqttService>>`.
pub struct MqttService {
    pub(crate) router: Arc<RwLock<Option<Arc<dyn MqttEventRouter>>>>,
    /// Per-client session handles, keyed by `client_slug`. One session per client
    /// carries both the publish path and the ingress delivery path. Populated once
    /// at startup; read-only thereafter.
    pub(crate) clients: Arc<RwLock<HashMap<String, Arc<MqttClientHandle>>>>,
}

/// `last_error` reported for a client with no registered session — the honest
/// "MQTT runtime not present for this client" state.
const NO_CLIENT_SESSION: &str = "no session for client";

impl MqttService {
    /// Construct an empty service. Populate with `add_client` then `set_router`
    /// before serving requests.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            router: Arc::new(RwLock::new(None)),
            clients: Arc::new(RwLock::new(HashMap::new())),
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

    /// Register a per-client session handle, keyed by `client_slug`. Called once
    /// per client at startup (bootstrap spawn loop).
    pub async fn add_client(&self, handle: Arc<MqttClientHandle>) {
        self.clients
            .write()
            .await
            .insert(handle.config.slug.clone(), handle);
    }

    /// Look up the session handle for `client_slug`.
    ///
    /// The registry is written only at startup; after that it is read-only. A
    /// write-lock held at this point is a bug (unexpected state), so we panic
    /// (fail-loud posture on the lookup path).
    pub fn get_client(&self, client_slug: &str) -> Option<Arc<MqttClientHandle>> {
        let clients = self.clients.try_read().expect(
            "MqttService clients map write lock held unexpectedly — the registry is read-only \
             after startup",
        );
        clients.get(client_slug).cloned()
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
        self.get_client(client_slug).map(|h| h.config.urgency)
    }

    /// The default broker SUBSCRIBE QoS for `client_slug`'s session.
    ///
    /// Returns `None` if the client has no session.
    pub async fn ingress_qos(&self, client_slug: &str) -> Option<u8> {
        self.get_client(client_slug).map(|h| h.config.qos)
    }

    /// Register `topic_filter` (at `qos`) on `client_slug`'s reconnect-survival
    /// set and issue the broker SUBSCRIBE now if the client is live.
    ///
    /// Returns `None` if `client_slug` has no registered session (an unconfigured
    /// MQTT client — the caller maps this to a tool error). This method does
    /// **not** touch the channel directory, the durable subscription row, or the
    /// router table — those are the caller's responsibility.
    pub async fn subscribe_filter(
        &self,
        client_slug: &str,
        topic_filter: String,
        qos: u8,
    ) -> Option<IngressSubscribeOutcome> {
        let handle = self.get_client(client_slug)?;
        let sub = handle.add_subscription(topic_filter, qos).await;
        let client = handle.client.lock().await.clone();
        let outcome = match client {
            None => IngressSubscribeOutcome::DeferredDisconnected,
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
    pub async fn unsubscribe_filter(
        &self,
        client_slug: &str,
        topic_filter: &str,
    ) -> Option<IngressUnsubscribeOutcome> {
        let handle = self.get_client(client_slug)?;
        if !handle.remove_subscription(topic_filter).await {
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
                    client_slug: handle.config.slug.clone(),
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
                    client_slug: handle.config.slug.clone(),
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
                client_slug: handle.config.slug.clone(),
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
                client_slug: handle.config.slug.clone(),
                last_error: Some(e.to_string()),
            });
        }
        drop(client_guard);

        match ack_rx.await {
            Ok(outcome) => outcome,
            Err(_) => Err(MqttError::NotConnected {
                client_slug: handle.config.slug.clone(),
                last_error: Some("supervisor task dropped the ack channel".to_string()),
            }),
        }
    }
}

impl Default for MqttService {
    fn default() -> Self {
        Self {
            router: Arc::new(RwLock::new(None)),
            clients: Arc::new(RwLock::new(HashMap::new())),
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
        let config = Arc::new(crate::mqtt::test_support::test_client_config(client));
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

    #[tokio::test]
    async fn subscribe_filter_send_failure_reports_sendfailed_and_registers() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        *handle.client.lock().await = Some(dead_live_client());
        svc.add_client(handle.clone()).await;

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
        svc.add_client(make_handle("home")).await;

        let got = svc.get_client("home");
        assert!(got.is_some());
        assert_eq!(got.unwrap().config.slug, "home");
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
        svc.add_client(make_handle("home")).await;
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
        svc.add_client(handle).await;

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
        let mut config = crate::mqtt::test_support::test_client_config("home");
        config.urgency = Urgency::High;
        config.qos = 2;
        let handle = MqttClientHandle::new(Arc::new(config), vec![], tx);
        svc.add_client(handle).await;

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
        svc.add_client(make_handle("home")).await;

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
        svc.add_client(handle.clone()).await;

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
        svc.add_client(handle.clone()).await;
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
        svc.add_client(handle.clone()).await;

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
        *handle.supervisor_state.write().await = crate::mqtt::state::SupervisorState::Failed {
            reason: "authoritative connect failure: boom".to_string(),
        };
        svc.add_client(handle).await;

        let (label, err) = svc.ingress_health("home").await;
        assert_eq!(label, ConnectorHealthLabel::Failed);
        assert_eq!(err.as_deref(), Some("authoritative connect failure: boom"));
    }

    #[tokio::test]
    async fn ingress_filter_status_failed_state_reports_failed_with_qos() {
        let svc = MqttService::new();
        let handle = make_handle("home");
        svc.add_client(handle.clone()).await;
        // Register a filter so qos is a fact, then drive the session to Failed.
        svc.subscribe_filter("home", "sensors/+/temp".to_string(), 2)
            .await;
        *handle.supervisor_state.write().await = crate::mqtt::state::SupervisorState::Failed {
            reason: "authoritative disconnect: gone".to_string(),
        };

        let (qos, label, err) = svc.ingress_filter_status("home", "sensors/+/temp").await;
        // The configured filter's QoS is still reported on a failed session.
        assert_eq!(qos, Some(2));
        assert_eq!(label, ConnectorHealthLabel::Failed);
        assert_eq!(err.as_deref(), Some("authoritative disconnect: gone"));
    }
}

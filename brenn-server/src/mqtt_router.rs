//! `MqttEventRouterImpl` — inbound MQTT delivery adapter (client-in-address model).
//!
//! Implements `brenn_mqtt::MqttEventRouter` against `AppState`. Mirrors
//! `webhook_router.rs`: an inbound MQTT message is published to its
//! `mqtt:<client>:<topic>` bus channel via `Messenger::publish_transport_ingress`
//! — wrapped in a typed `MqttEnvelope` on a text-only channel, or as the payload
//! itself with `TransportMeta::Mqtt` on a channel declared `non_text`. There is
//! no singleton conversation, no `submit_ingress`, and no per-app conversation
//! cache.
//!
//! Uses the same deferred-state pattern as `WebhookEventRouterImpl`: the
//! `AppState` is not yet constructed when the connection supervisors are spawned,
//! so we stash a `OnceCell<RouterState>` and call `set_state` after `AppState`
//! construction, before any supervisor can deliver an inbound message.
//!
//! Routing: the router owns the ingress routing table (built from the distinct
//! ingress channels at `set_state` time), keyed by client and topic filter. The
//! supervisor resolves each inbound copy's Subscription Identifiers to the
//! filters the broker sent it for, and the router publishes the message once
//! into the channel of each of those filters on that client, after checking that
//! the channel's filter covers the **actual** published topic (standard MQTT
//! `+`/`#` matching). Only a copy that names no subscription is routed by topic
//! alone, into every channel on the client whose filter covers it. A copy that
//! reaches no channel is logged and dropped, not panicked — untrusted broker
//! input must not crash the host.

use std::sync::RwLock;

use brenn_common::sanitize_untrusted_str;
use brenn_envelope::body::MAX_CONTENT_TYPE_LEN;
use brenn_lib::messaging::{
    BodyError, ChannelEntry, MessageBody, MqttEnvelope, MqttPayloadBody, MqttTransport,
    TransportMeta, Urgency,
};
use brenn_lib::mqtt::address::topic_name_has_wildcard;
use brenn_lib::mqtt::topic_filter;
use brenn_messaging::IngressMessage;
use brenn_mqtt::payload::{InboundClass, InboundPayload, classify_inbound};
use brenn_mqtt::service::{InboundRouting, MqttEventRouter, SessionFault};
use brenn_obs::alerting::AlertSeverity;
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::state::AppState;

/// One resolved ingress route. Built from a `ResolvedMqttIngressChannel` at
/// `set_state` time. An inbound copy selects the route whose
/// `(client_slug, topic_filter)` is a subscription the broker sent it for — or,
/// when it names none, every route on its client whose filter covers its topic —
/// and the router publishes it to the route's `mqtt:<client>:<topic>` channel
/// with `urgency`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressRoute {
    /// Client this channel subscribes on (the ACL/provenance boundary).
    pub client_slug: String,
    /// MQTT topic filter (the subscribed pattern).
    pub topic_filter: String,
    /// Channel identity; the `mqtt:<client>:<topic>` address.
    pub channel_address: String,
    /// Channel UUID (= `mqtt_channel_uuid_from_address(channel_address)`).
    pub channel_uuid: Uuid,
    /// Sender intent for inbound messages routed through this channel
    /// (the client's `[[mqtt_client]].urgency`).
    pub urgency: Urgency,
}

impl From<&brenn_lib::mqtt::config::ResolvedMqttIngressChannel> for IngressRoute {
    /// The single projection both boot and reload share, so the two cannot
    /// diverge in what they carry.
    fn from(channel: &brenn_lib::mqtt::config::ResolvedMqttIngressChannel) -> Self {
        Self {
            client_slug: channel.client_slug.clone(),
            topic_filter: channel.topic.clone(),
            channel_address: channel.channel_address.clone(),
            channel_uuid: channel.channel_uuid,
            urgency: channel.urgency,
        }
    }
}

/// State bundle stored in `MqttEventRouterImpl`'s inner `OnceCell`.
///
/// `app_state` is write-once (set at `set_state`, never replaced). `routes` is
/// behind an `RwLock` so a runtime `mqtt:` subscribe can push a new
/// `IngressRoute` while `deliver_inbound` keeps scanning the table under a brief
/// read-lock. The `OnceCell` itself is still set exactly once (the deferred
/// `AppState` wiring); only the route set inside it mutates afterward.
struct RouterState {
    app_state: AppState,
    /// Ingress routing table. One entry per distinct ingress channel.
    /// Matching is linear; the table is small (one entry per declared channel)
    /// and only consulted per inbound message. Mutable so runtime subscribes
    /// can add a route; `deliver_inbound` reads under a read-lock.
    routes: RwLock<Vec<IngressRoute>>,
}

/// Concrete `MqttEventRouter` impl. Closes over `AppState` (via `OnceCell`).
pub struct MqttEventRouterImpl {
    inner: OnceCell<RouterState>,
}

impl Default for MqttEventRouterImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl MqttEventRouterImpl {
    pub fn new() -> Self {
        Self {
            inner: OnceCell::new(),
        }
    }

    /// Fill in the `AppState` and the ingress routing table. Must be called
    /// before any inbound message can reach this router (before supervisors are
    /// spawned, or at least before they have a live connection).
    pub fn set_state(&self, state: AppState, routes: Vec<IngressRoute>) {
        self.inner
            .set(RouterState {
                app_state: state,
                routes: RwLock::new(routes),
            })
            .map_err(|_| ())
            .expect("MqttEventRouterImpl state already set");
    }

    /// Add an ingress route at runtime. A dynamic `mqtt:` subscribe to a new
    /// topic filter needs a matching `IngressRoute` so `deliver_inbound` routes
    /// the copies the broker sends for that filter's subscription to the new
    /// channel. The route is added before the SUBSCRIBE is sent, so it is in the
    /// table before the broker can answer.
    ///
    /// **Idempotent on `channel_uuid`** (correctness-1): a route is added only if
    /// no route for that channel already exists; a second subscriber joining an
    /// existing `mqtt:` filter (the core returns `Created` per-(app, channel), not
    /// per-channel) must NOT append a duplicate route, or `deliver_inbound` would
    /// fan one inbound broker message out to the channel once per route — storing
    /// and delivering it N times for N subscribers. `remove_route` retains-out all
    /// matching `channel_uuid`s, so one-route-per-channel is preserved either way.
    /// Returns `true` if a new route was inserted, `false` if one already existed.
    ///
    /// Requires `set_state` to have run (the route table lives inside the
    /// `OnceCell`); calling before then is a host wiring bug (runtime subscribes
    /// happen long after startup), so the missing-state case panics rather than
    /// silently dropping the route.
    pub fn add_route(&self, route: IngressRoute) -> bool {
        let router_state = self.inner.get().expect(
            "MqttEventRouterImpl::add_route called before set_state — runtime route additions \
             happen after startup wiring; this is a host bug",
        );
        let mut routes = router_state
            .routes
            .write()
            .expect("mqtt router routes lock poisoned");
        if routes.iter().any(|r| r.channel_uuid == route.channel_uuid) {
            return false;
        }
        routes.push(route);
        true
    }

    /// The channel uuid of every route in the table, in insertion order.
    ///
    /// The route set is the half of an `mqtt:` channel's runtime that nothing
    /// else can be asked about: the directory knows the entry and the service
    /// knows the filter, and a reload that moved one without the other is
    /// exactly the drift a test has to be able to see.
    ///
    /// Empty before `set_state`, which is honest — there is no table yet.
    pub fn route_uuids(&self) -> Vec<Uuid> {
        let Some(router_state) = self.inner.get() else {
            return Vec::new();
        };
        router_state
            .routes
            .read()
            .expect("mqtt router routes lock poisoned")
            .iter()
            .map(|route| route.channel_uuid)
            .collect()
    }

    /// Remove the ingress route for `channel_uuid` at runtime. A dynamic `mqtt:`
    /// unsubscribe that removes the last subscriber on a filter drops the matching
    /// `IngressRoute` so the broker's deliveries on that filter — should any still
    /// arrive before the UNSUBSCRIBE takes effect — no longer reach the
    /// now-unsubscribed channel.
    ///
    /// `channel_uuid` is the route's stable identity (one route per distinct
    /// ingress channel), so the removal is keyed on it rather than the
    /// `(client, filter)` pair. Returns `true` if a matching route was removed.
    ///
    /// Requires `set_state` to have run (the route table lives inside the
    /// `OnceCell`); calling before then is a host wiring bug (runtime unsubscribes
    /// happen long after startup), so the missing-state case panics — symmetric
    /// with [`Self::add_route`].
    pub fn remove_route(&self, channel_uuid: Uuid) -> bool {
        let router_state = self.inner.get().expect(
            "MqttEventRouterImpl::remove_route called before set_state — runtime route removals \
             happen after startup wiring; this is a host bug",
        );
        let mut routes = router_state
            .routes
            .write()
            .expect("mqtt router routes lock poisoned");
        let before = routes.len();
        routes.retain(|r| r.channel_uuid != channel_uuid);
        routes.len() != before
    }
}

#[async_trait::async_trait]
impl MqttEventRouter for MqttEventRouterImpl {
    async fn deliver_inbound(
        &self,
        client_slug: &str,
        topic: &str,
        payload: InboundPayload,
        qos: u8,
        routing: InboundRouting,
    ) {
        // Use `get()` rather than `expect()` so a fast-broker startup race (broker
        // connects and delivers a retained message before AppState is wired in) drops
        // the message with a log rather than panicking the process.
        let router_state = match self.inner.get() {
            Some(s) => s,
            None => {
                tracing::error!(
                    client = client_slug,
                    topic,
                    qos,
                    "mqtt_router: AppState not yet initialized — dropping inbound message. \
                     This is a startup race; if it persists after the process is up, it is a bug."
                );
                return;
            }
        };
        let state = &router_state.app_state;

        // A PUBLISH topic name holding `+` or `#` is malformed MQTT, and the match
        // below requires a wildcard-free topic. Broker input: warn and drop, never
        // panic.
        if topic_name_has_wildcard(topic) {
            tracing::warn!(
                client = client_slug,
                topic,
                "mqtt_router: inbound PUBLISH topic name contains a wildcard character \
                 ('+' or '#'), which is malformed MQTT — dropping unrouted."
            );
            return;
        }

        // Clone the selected routes out of the read-lock so the lock is
        // released before the publish loop below `.await`s (a std
        // `RwLockReadGuard` is not `Send`, and routes are small and rarely added).
        let matches: Vec<IngressRoute> = {
            let routes = router_state
                .routes
                .read()
                .expect("mqtt router routes lock poisoned");
            match &routing {
                InboundRouting::ByTopic => {
                    let matches: Vec<IngressRoute> = routes
                        .iter()
                        .filter(|r| {
                            r.client_slug == client_slug
                                && topic_filter::covers(&r.topic_filter, topic)
                        })
                        .cloned()
                        .collect();
                    if matches.is_empty() {
                        // Untrusted broker input must not crash the host: log
                        // and drop, do not panic.
                        tracing::warn!(
                            client = client_slug,
                            topic,
                            "mqtt_router: inbound topic carrying no Subscription Identifier \
                             matched no channel on this client — dropping."
                        );
                    }
                    matches
                }
                InboundRouting::BySubscription(filters) => filters
                    .iter()
                    .filter_map(|filter| {
                        // One route at most: a route is one per channel, and the
                        // channel's identity is the address `mqtt:<client>:<filter>`.
                        let Some(route) = routes
                            .iter()
                            .find(|r| r.client_slug == client_slug && &r.topic_filter == filter)
                        else {
                            tracing::warn!(
                                client = client_slug,
                                topic,
                                filter = %filter,
                                "mqtt_router: inbound copy sent for a subscription with no \
                                 channel — dropped for that subscription."
                            );
                            return None;
                        };
                        // The identifier selects among channels and never
                        // overrides the topic check.
                        // TODO(mqtt-shared-subscription-filters-unrouted): a
                        // `$share/<group>/…` filter never covers a delivered topic,
                        // by this rule or by topic, so its messages drop here.
                        if !topic_filter::covers(&route.topic_filter, topic) {
                            tracing::warn!(
                                client = client_slug,
                                topic,
                                filter = %filter,
                                "mqtt_router: the broker sent a topic under a subscription \
                                 whose filter does not cover it — dropped for that subscription."
                            );
                            return None;
                        }
                        Some(route.clone())
                    })
                    .collect(),
            }
        };

        if matches.is_empty() {
            return;
        }

        let received_at = chrono::Utc::now();

        let messenger = state.messenger.as_ref().unwrap_or_else(|| {
            panic!(
                "mqtt_router: Messenger not present in AppState while delivering on client \
                 '{client_slug}' — startup invariant violated (messenger required when \
                 mqtt ingress channels exist)"
            )
        });

        // Each shape's body is built at most once per message and shared by every
        // matched channel of that shape: the wrapper JSON on the first wrapped
        // channel, a raw body once per distinct typing among the raw channels.
        let mut wrapped: Option<String> = None;
        let mut raw_bodies: Vec<RawMemo> = Vec::new();

        for route in matches {
            // Resolve the mqtt:<client>:<topic> channel by its derived UUID. It was
            // derived from this channel at startup and upserted, so an unresolvable
            // channel is a host-internal invariant violation: panic (fail-fast).
            let channel = messenger
                .directory()
                .by_uuid(&route.channel_uuid)
                .unwrap_or_else(|| {
                    panic!(
                        "mqtt_router: mqtt channel '{}' (uuid {}) not found in directory \
                         — channel must be derived at startup",
                        route.channel_address, route.channel_uuid
                    )
                });

            // The shape is the channel entry's, read from the entry the message is
            // published on.
            let message = if channel.resolved_channel.non_text {
                let typing = raw_typing(&payload, &channel);
                let memo = raw_bodies
                    .iter()
                    .find(|((ct, source), _)| ct.as_deref() == typing.0 && *source == typing.1);
                let built = match memo {
                    Some((_, built)) => built.clone(),
                    None => {
                        let built = raw_body(&payload, typing, messenger.max_body_bytes());
                        raw_bodies.push(((typing.0.map(str::to_owned), typing.1), built.clone()));
                        built
                    }
                };
                match built {
                    Ok(body) => IngressMessage::Raw {
                        body,
                        transport: TransportMeta::Mqtt(MqttTransport {
                            client_slug: client_slug.to_string(),
                            topic: topic.to_string(),
                            qos,
                            received_at,
                        }),
                    },
                    Err(refusal) => {
                        tracing::warn!(
                            client = client_slug,
                            topic,
                            channel = %channel.address,
                            size = payload.bytes.len(),
                            reason = %refusal,
                            "mqtt_router: inbound payload refused for a raw channel — dropped \
                             for this channel"
                        );
                        continue;
                    }
                }
            } else {
                let json = wrapped.get_or_insert_with(|| {
                    // A binary payload is a placeholder in the wrapper; its bytes
                    // are not carried.
                    let payload_body =
                        match classify_inbound(&payload.bytes, payload.content_type.as_deref()) {
                            InboundClass::Text(text) => MqttPayloadBody::Text {
                                text: text.to_owned(),
                            },
                            InboundClass::Binary => MqttPayloadBody::Binary {
                                binary_placeholder: true,
                                content_type: payload.content_type.clone(),
                            },
                        };
                    serde_json::to_string(&MqttEnvelope {
                        client_slug: client_slug.to_string(),
                        topic: topic.to_string(),
                        payload: payload_body,
                        received_at,
                        qos,
                    })
                    .expect("MqttEnvelope serialization is infallible")
                });
                IngressMessage::Wrapped(json.clone())
            };

            // source == "mqtt:<client>:<topic>", sender == client_slug (provenance).
            messenger
                .publish_transport_ingress(
                    channel,
                    &route.channel_address,
                    client_slug,
                    message,
                    route.urgency,
                )
                .await;
        }
    }

    async fn session_fault(&self, client_slug: &str, fault: SessionFault) {
        let (severity, title, body) = session_fault_alert(client_slug, &fault);
        // The supervisors can run before boot wires the state in; the
        // supervisor has already logged the fault, so this says only that the
        // alert is not raised.
        let Some(router_state) = self.inner.get() else {
            tracing::error!(
                client = client_slug,
                ?fault,
                "mqtt_router: AppState not yet initialized — the operator alert for this MQTT \
                 session fault could not be raised"
            );
            return;
        };
        router_state
            .app_state
            .alert_dispatcher
            .alert(severity, title, body);
    }
}

/// The operator alert for a session fault on `client_slug`.
fn session_fault_alert(client_slug: &str, fault: &SessionFault) -> (AlertSeverity, String, String) {
    match fault {
        SessionFault::SessionDiscarded { pkid } => (
            AlertSeverity::Warning,
            format!("MQTT session discarded: {client_slug}"),
            format!(
                "MQTT client '{client_slug}' had its broker session discarded after the broker \
                 sent an unsolicited acknowledgement (packet identifier {pkid}). The broker's \
                 queued messages for this client are lost, and publishes in flight are reported \
                 as after a session reset. If the next connection fails the same way, the \
                 client stops."
            ),
        ),
        SessionFault::GaveUp { reason } => (
            AlertSeverity::Critical,
            format!("MQTT client stopped: {client_slug}"),
            format!(
                "MQTT client '{client_slug}' is stopped until its mqtt_client block is reloaded \
                 or the server restarts: discarding its broker session did not cure an \
                 unsolicited acknowledgement. {reason}"
            ),
        ),
    }
}

/// Whose statement typed a raw payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeSource {
    /// The broker delivered the publisher's Content-Type.
    Publisher,
    /// The payload had none; the channel's `untyped_content_type` typed it.
    Channel,
    /// The payload had none and the channel declares none: untyped text.
    Untyped,
}

/// Why a payload was not published on a raw channel. Outside input, so a
/// log line and a drop, never a panic.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RawRefusal {
    /// Longer than the bus's body cap.
    OverCap { cap: usize },
    /// The content type is not a media type the bus accepts.
    InvalidContentType {
        content_type: String,
        source: TypeSource,
    },
    /// The payload is typed as text and is not UTF-8.
    NotUtf8 {
        content_type: Option<String>,
        source: TypeSource,
    },
}

impl std::fmt::Display for RawRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let origin = |source: &TypeSource| match source {
            TypeSource::Publisher => "the publisher's Content-Type",
            TypeSource::Channel => "the channel's untyped_content_type",
            TypeSource::Untyped => "no content type",
        };
        match self {
            RawRefusal::OverCap { cap } => {
                write!(f, "payload is over the {cap}-byte messaging.max_body_bytes")
            }
            RawRefusal::InvalidContentType {
                content_type,
                source,
            } => write!(
                f,
                "content type \"{content_type}\" ({}) is not a media type the bus accepts",
                origin(source)
            ),
            RawRefusal::NotUtf8 {
                content_type: Some(content_type),
                source,
            } => write!(
                f,
                "payload typed \"{content_type}\" ({}) is a text type and is not UTF-8",
                origin(source)
            ),
            RawRefusal::NotUtf8 {
                content_type: None, ..
            } => f.write_str(
                "payload has no Content-Type and is not UTF-8, so it is not untyped text; \
                 state untyped_content_type in the channel's tuning block to type such payloads",
            ),
        }
    }
}

/// The content type a raw payload is typed with and whose statement it is:
/// the publisher's Content-Type when the broker delivered one, otherwise the
/// channel's `untyped_content_type`, otherwise none (untyped text).
type RawTyping<'a> = (Option<&'a str>, TypeSource);

/// A raw body already built for one message, by the typing it was built under.
type RawMemo = (
    (Option<String>, TypeSource),
    Result<MessageBody, RawRefusal>,
);

/// The typing of `payload` on the raw channel `channel`.
fn raw_typing<'a>(payload: &'a InboundPayload, channel: &'a ChannelEntry) -> RawTyping<'a> {
    match (
        payload.content_type.as_deref(),
        channel.resolved_channel.untyped_content_type.as_deref(),
    ) {
        (Some(ct), _) => (Some(ct), TypeSource::Publisher),
        (None, Some(ct)) => (Some(ct), TypeSource::Channel),
        (None, None) => (None, TypeSource::Untyped),
    }
}

/// The raw body of `payload` under `typing`, judged by the bus's own rule. A
/// logged content type is truncated to the bus's content-type limit.
fn raw_body(
    payload: &InboundPayload,
    (content_type, source): RawTyping<'_>,
    max_body_bytes: usize,
) -> Result<MessageBody, RawRefusal> {
    if payload.bytes.len() > max_body_bytes {
        return Err(RawRefusal::OverCap {
            cap: max_body_bytes,
        });
    }
    let logged = || content_type.map(|ct| sanitize_untrusted_str(ct, MAX_CONTENT_TYPE_LEN));
    MessageBody::from_parts(content_type, &payload.bytes).map_err(|error| match error {
        BodyError::InvalidContentType { .. } => RawRefusal::InvalidContentType {
            content_type: logged().unwrap_or_default(),
            source,
        },
        BodyError::NotUtf8 { .. } => RawRefusal::NotUtf8 {
            content_type: logged(),
            source,
        },
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use brenn_lib::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, MessagingGlobalConfig, NoiseLevel,
        ResolvedChannel, SubscriberEntry, SubscriberEntryKind, Urgency, WakeMin,
        mqtt_channel_uuid_from_address,
    };
    use brenn_lib::mqtt::config::parsed_address_canonical;
    use brenn_messaging::config::{Depth, Sink};
    use brenn_messaging_store::db::upsert_channels;
    use brenn_mqtt::payload::InboundPayload;
    use brenn_mqtt::service::{InboundRouting, MqttEventRouter};
    use indexmap::IndexMap;

    use super::*;
    use crate::test_support::state::test_state_with_user_and_app;

    /// Build a `ChannelEntry` for an `mqtt:<client>:<topic>` channel, optionally
    /// with one app subscriber.
    fn mqtt_channel_entry(address: &str, subscriber_app: Option<&str>) -> ChannelEntry {
        let subscribers = match subscriber_app {
            Some(app) => vec![SubscriberEntry {
                kind: SubscriberEntryKind::App(app.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            None => vec![],
        };
        ChannelEntry {
            uuid: mqtt_channel_uuid_from_address(address),
            address: address.to_string(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
                non_text: false,
                untyped_content_type: None,
            },
            subscribers,
            transport_type: ChannelScheme::Mqtt,
            mount: None,
        }
    }

    /// Build a `Messenger` over a set of `mqtt:` channel entries, with an apps
    /// map so each subscriber's policy and owner resolve.
    fn messenger_with_channels(
        db: brenn_db::Db,
        entries: Vec<ChannelEntry>,
        subscriber_app: Option<SubscriberSpec<'_>>,
    ) -> Arc<brenn_messaging::Messenger> {
        use brenn_lib::messaging::config::{ResolvedMessagingConfig, ResolvedSubscription};

        {
            let conn = db.try_lock().expect("db lock for channel upsert");
            upsert_channels(&conn, &entries);
        }
        let directory = Arc::new(MessagingDirectory::with_entries(entries));

        let mut apps_raw: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
        if let Some((app_slug, allowed_users, subs)) = subscriber_app {
            let mut app_cfg =
                crate::test_support::app_config::default_test_app_config(app_slug, app_slug);
            app_cfg.allowed_users = allowed_users;
            // Delivery-time ACL gate: the subscriber's policy must cover each
            // subscribed `mqtt:` channel (grant + matcher), else the wake pass
            // skips the app without advancing its position. Stamp a covering
            // policy derived from the subscription addresses.
            app_cfg.policy = std::sync::Arc::new(
                brenn_lib::access::test_fixtures::delivery_policy_for_addresses(
                    subs.iter().map(|(_, a)| a.as_str()),
                ),
            );
            app_cfg.messaging = Some(ResolvedMessagingConfig {
                send_budget: 0, // zero budget must not block host ingress
                subscriptions: subs
                    .into_iter()
                    .map(|(uuid, address)| ResolvedSubscription {
                        channel_uuid: uuid,
                        channel_address: address,
                        push_depth: Depth::Unbounded,
                        retain_depth: Depth::Unbounded,
                        noise: NoiseLevel::Silent,
                        wake_min: WakeMin::Normal,
                    })
                    .collect(),
            });
            apps_raw.insert(app_slug.to_string(), app_cfg);
        }

        brenn_messaging::Messenger::new(
            db,
            directory,
            Arc::from("mqtt-test"),
            Arc::new(apps_raw),
            Arc::new(brenn_messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_messaging::WakeRouter>,
            MessagingGlobalConfig::default(),
        )
    }

    /// Build an `mqtt:` `ChannelEntry` whose sole subscriber is a WASM consumer
    /// (`wasm:<wasm_slug>`).
    fn mqtt_channel_entry_wasm(address: &str, wasm_slug: &str) -> ChannelEntry {
        crate::test_support::wasm::wasm_subscriber_channel_entry(
            mqtt_channel_uuid_from_address(address),
            address,
            ChannelScheme::Mqtt,
            None,
            wasm_slug,
        )
    }

    /// Build a `Messenger` over `mqtt:` channel entries whose subscriber is a WASM
    /// consumer, with a `wasm_policies` entry derived through the real
    /// `build_wasm_policy` path from `mqtt_subscribe_acl`. A non-empty ACL yields a
    /// covering policy (grant + matcher); empty yields deny-by-default at the
    /// delivery gate.
    fn messenger_with_wasm_channels(
        db: brenn_db::Db,
        entries: Vec<ChannelEntry>,
        wasm_slug: &str,
        mqtt_subscribe_acl: Vec<brenn_lib::access::raw::MqttMatcherRaw>,
    ) -> Arc<brenn_messaging::Messenger> {
        let policy = brenn_lib::access::resolve::build_wasm_policy(
            wasm_slug,
            [],
            brenn_lib::access::raw::WasmAclsRaw {
                mqtt_subscribe: &mqtt_subscribe_acl,
                ..Default::default()
            },
        );
        crate::test_support::wasm::messenger_with_wasm_policy(
            db,
            entries,
            "mqtt-test",
            wasm_slug,
            policy,
        )
    }

    /// `(app_slug, allowed_users, subscribed (channel_uuid, address) pairs)` for
    /// the subscriber an `mqtt:` channel messenger should wire up.
    type SubscriberSpec<'a> = (&'a str, Vec<String>, Vec<(Uuid, String)>);

    /// Build an `IngressRoute` for client `client` subscribed to filter `filter`.
    /// The channel identity is `mqtt:<client>:<filter>`.
    fn route(client: &str, filter: &str, urgency: Urgency) -> IngressRoute {
        // Derive the address via the shared formatter, not ad-hoc concatenation,
        // so the test exercises the same two-caller UUID contract as production.
        let address = parsed_address_canonical(client, filter);
        IngressRoute {
            client_slug: client.to_string(),
            topic_filter: filter.to_string(),
            channel_uuid: mqtt_channel_uuid_from_address(&address),
            channel_address: address,
            urgency,
        }
    }

    /// Delivery stores an `MqttEnvelope` JSON body with `envelope_type='mqtt'`,
    /// a non-NULL `channel_uuid`, and the correct provenance fields.
    #[tokio::test]
    async fn deliver_inbound_stores_typed_mqtt_envelope() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry("mqtt:homeassistant:home/+/state", None)],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![route("homeassistant", "home/+/state", Urgency::Normal)],
        );

        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state",
                InboundPayload::new("22.5", None),
                1,
                InboundRouting::ByTopic,
            )
            .await;

        let (envelope_type, body, channel_uuid_bytes): (String, String, Vec<u8>) = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT envelope_type, body, channel_uuid \
                 FROM messaging_messages ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get::<_, Vec<u8>>(2)?)),
            )
            .expect("message must have been inserted")
        };

        assert_eq!(envelope_type, "mqtt", "must store envelope_type='mqtt'");
        // The stored channel_uuid must be the one derived from the canonical
        // `mqtt:<client>:<topic>` address — a non-NULL-but-wrong UUID (e.g. derived
        // from only the topic, or a legacy slug) would silently break the
        // two-caller contract and go undelivered at runtime.
        assert_eq!(
            channel_uuid_bytes,
            mqtt_channel_uuid_from_address("mqtt:homeassistant:home/+/state")
                .as_bytes()
                .to_vec(),
            "channel_uuid must be derived from the canonical mqtt:<client>:<topic> address"
        );

        let envelope: MqttEnvelope =
            serde_json::from_str(&body).expect("body must be a valid MqttEnvelope JSON");
        assert_eq!(envelope.client_slug, "homeassistant");
        assert_eq!(
            envelope.topic, "home/kitchen/state",
            "the actual topic, not the filter"
        );
        assert_eq!(envelope.qos, 1);
        match envelope.payload {
            MqttPayloadBody::Text { text } => assert_eq!(text, "22.5"),
            other => panic!("expected text payload, got {other:?}"),
        }
    }

    /// Binary payloads are represented as a placeholder; raw bytes never appear.
    #[tokio::test]
    async fn deliver_inbound_binary_payload_is_placeholder() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry("mqtt:broker1:data/#", None)],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(state, vec![route("broker1", "data/#", Urgency::Low)]);

        router
            .deliver_inbound(
                "broker1",
                "data/blob",
                InboundPayload::new(
                    vec![0u8, 1, 2],
                    Some("application/octet-stream".to_string()),
                ),
                0,
                InboundRouting::ByTopic,
            )
            .await;

        let body: String = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT body FROM messaging_messages WHERE envelope_type='mqtt' \
                 ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .expect("mqtt message must have been inserted")
        };
        assert!(
            !body.contains("AAEC"),
            "raw base64 bytes must NOT appear in the stored envelope (got: {body})"
        );
        let envelope: MqttEnvelope = serde_json::from_str(&body).expect("valid MqttEnvelope JSON");
        match envelope.payload {
            MqttPayloadBody::Binary {
                binary_placeholder,
                content_type,
            } => {
                assert!(binary_placeholder, "binary_placeholder must be true");
                assert_eq!(content_type.as_deref(), Some("application/octet-stream"));
            }
            other => panic!("expected binary placeholder, got {other:?}"),
        }
    }

    /// End-to-end MQTT receive for a WASM consumer: a real inbound MQTT message
    /// delivered through the router reaches the `wasm:` subscriber's push window
    /// when its policy carries a covering `mqtt_subscribe_acl`, and the pushed row
    /// carries the `MqttEnvelope` (envelope_type='mqtt').
    #[tokio::test]
    async fn deliver_inbound_reaches_wasm_subscriber_when_covered() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        let addr = "mqtt:homeassistant:home/+/state";
        state.messenger = Some(messenger_with_wasm_channels(
            db.clone(),
            vec![mqtt_channel_entry_wasm(addr, "consume-demo")],
            "consume-demo",
            vec![brenn_lib::access::raw::MqttMatcherRaw {
                client: "homeassistant".to_string(),
                topic_filter: "home/+/state".to_string(),
            }],
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![route("homeassistant", "home/+/state", Urgency::Normal)],
        );

        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state",
                InboundPayload::new("22.5", None),
                1,
                InboundRouting::ByTopic,
            )
            .await;

        let (retained, envelope_type) =
            crate::test_support::wasm::retained_on_channel(&db, addr).await;
        assert_eq!(
            retained, 1,
            "the inbound message must commit onto the subscribed channel"
        );
        assert_eq!(
            envelope_type.as_deref(),
            Some("mqtt"),
            "the committed message must carry the MqttEnvelope"
        );
    }

    /// End-to-end MQTT receive denial: with no covering `mqtt_subscribe_acl`, the
    /// delivery-time ACL gate at the activation read serves the WASM consumer an
    /// empty window and emits a denial warn (the post-boot runtime half of the
    /// fail-fast posture; the boot half panics).
    ///
    /// The commit is target-blind, so the message still reaches retention — what
    /// the gate decides is who may be served from it.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn deliver_inbound_denies_uncovered_wasm_subscriber() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        let addr = "mqtt:homeassistant:home/+/state";
        let messenger = messenger_with_wasm_channels(
            db.clone(),
            vec![mqtt_channel_entry_wasm(addr, "consume-demo")],
            "consume-demo",
            vec![],
        );
        let entry = messenger
            .directory()
            .resolve(addr)
            .expect("the mqtt channel must be in the directory");
        crate::test_support::wasm::attach_wasm_consumer(&messenger, &entry, "consume-demo").await;
        state.messenger = Some(Arc::clone(&messenger));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![route("homeassistant", "home/+/state", Urgency::Normal)],
        );

        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state",
                InboundPayload::new("22.5", None),
                1,
                InboundRouting::ByTopic,
            )
            .await;

        let served =
            crate::test_support::wasm::activation_new_messages(&messenger, &entry, "consume-demo")
                .await;
        assert!(
            served.is_empty(),
            "an uncovered WASM consumer must be served nothing, got {served:?}"
        );
        assert!(
            logs_contain("subscription delivery denied"),
            "the delivery gate must emit a denial warn for the uncovered subscriber"
        );
    }

    /// The restore half: the denied read never advanced the consumer's position,
    /// so a policy covering the channel again serves the backlog that accumulated
    /// under the revocation. Same database, same position — only the policy
    /// differs.
    #[tokio::test]
    async fn a_restored_mqtt_subscribe_acl_serves_the_denied_backlog() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        let addr = "mqtt:homeassistant:home/+/state";
        let denied = messenger_with_wasm_channels(
            db.clone(),
            vec![mqtt_channel_entry_wasm(addr, "consume-demo")],
            "consume-demo",
            vec![],
        );
        let entry = denied
            .directory()
            .resolve(addr)
            .expect("the mqtt channel must be in the directory");
        crate::test_support::wasm::attach_wasm_consumer(&denied, &entry, "consume-demo").await;
        state.messenger = Some(Arc::clone(&denied));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![route("homeassistant", "home/+/state", Urgency::Normal)],
        );

        for reading in ["22.5", "23.0"] {
            router
                .deliver_inbound(
                    "homeassistant",
                    "home/kitchen/state",
                    InboundPayload::new(reading.to_string(), None),
                    1,
                    InboundRouting::ByTopic,
                )
                .await;
        }
        assert!(
            crate::test_support::wasm::activation_new_messages(&denied, &entry, "consume-demo")
                .await
                .is_empty(),
            "nothing may be served while the ACL is revoked"
        );

        let restored = messenger_with_wasm_channels(
            db.clone(),
            vec![mqtt_channel_entry_wasm(addr, "consume-demo")],
            "consume-demo",
            vec![brenn_lib::access::raw::MqttMatcherRaw {
                client: "homeassistant".to_string(),
                topic_filter: "home/+/state".to_string(),
            }],
        );
        crate::test_support::wasm::attach_wasm_consumer(&restored, &entry, "consume-demo").await;

        let served =
            crate::test_support::wasm::activation_new_messages(&restored, &entry, "consume-demo")
                .await;
        assert_eq!(
            served.len(),
            2,
            "both messages published under the revocation must be served once it is restored, \
             got {served:?}"
        );
    }

    /// A copy that names no subscription is routed by topic: one inbound topic
    /// matching two distinct-channel filters on the same client reaches both
    /// channels, one stored message each.
    #[tokio::test]
    async fn a_copy_with_no_identifier_reaches_every_covering_channel() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        // Two distinct channels on client `b1`, both matching "home/kitchen/state".
        let addr_a = "mqtt:b1:home/+/state";
        let addr_b = "mqtt:b1:home/#";
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![
                mqtt_channel_entry(addr_a, None),
                mqtt_channel_entry(addr_b, None),
            ],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![
                route("b1", "home/+/state", Urgency::Normal),
                route("b1", "home/#", Urgency::Normal),
            ],
        );

        router
            .deliver_inbound(
                "b1",
                "home/kitchen/state",
                InboundPayload::new("x", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;

        // The two channels are distinguished by their channel_uuid (the envelope
        // carries client + actual topic, which are identical across both).
        let channel_uuids: Vec<Vec<u8>> = {
            let conn = db.lock().await;
            let mut stmt = conn
                .prepare(
                    "SELECT channel_uuid FROM messaging_messages WHERE envelope_type='mqtt' \
                     ORDER BY id",
                )
                .expect("prepare");
            let rows: Vec<Vec<u8>> = stmt
                .query_map([], |r| r.get::<_, Vec<u8>>(0))
                .expect("query")
                .map(|r| r.expect("row"))
                .collect();
            rows
        };
        assert_eq!(
            channel_uuids.len(),
            2,
            "must fan out to both matching channels"
        );
        let uuid_a = mqtt_channel_uuid_from_address(addr_a).as_bytes().to_vec();
        let uuid_b = mqtt_channel_uuid_from_address(addr_b).as_bytes().to_vec();
        assert!(channel_uuids.contains(&uuid_a));
        assert!(channel_uuids.contains(&uuid_b));
    }

    // --- Routing by subscription ---

    /// A router over channels `mqtt:c1:a/#` and `mqtt:c1:a/b`, with a route for
    /// each, plus a route and channel for `a/b` on another client `c2`.
    fn overlap_router() -> (MqttEventRouterImpl, brenn_db::Db) {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![
                mqtt_channel_entry("mqtt:c1:a/#", None),
                mqtt_channel_entry("mqtt:c1:a/b", None),
                mqtt_channel_entry("mqtt:c2:a/b", None),
            ],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![
                route("c1", "a/#", Urgency::Normal),
                route("c1", "a/b", Urgency::Normal),
                route("c2", "a/b", Urgency::Normal),
            ],
        );
        (router, db)
    }

    /// Rows stored in each of `mqtt:c1:a/#`, `mqtt:c1:a/b` and `mqtt:c2:a/b`.
    async fn overlap_rows(db: &brenn_db::Db) -> [i64; 3] {
        let conn = db.lock().await;
        ["mqtt:c1:a/#", "mqtt:c1:a/b", "mqtt:c2:a/b"].map(|address| {
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_messages WHERE channel_uuid = ?1",
                [mqtt_channel_uuid_from_address(address).as_bytes().to_vec()],
                |r| r.get(0),
            )
            .expect("count")
        })
    }

    async fn deliver_on_c1(router: &MqttEventRouterImpl, topic: &str, filters: &[&str]) {
        router
            .deliver_inbound(
                "c1",
                topic,
                InboundPayload::new("x", None),
                1,
                InboundRouting::BySubscription(filters.iter().map(|f| f.to_string()).collect()),
            )
            .await;
    }

    /// A broker that sends one copy per subscription: each copy reaches its
    /// own subscription's channel only, so each channel holds the message once.
    #[tokio::test]
    async fn one_copy_per_subscription_lands_once_in_each_channel() {
        let (router, db) = overlap_router();
        deliver_on_c1(&router, "a/b", &["a/#"]).await;
        deliver_on_c1(&router, "a/b", &["a/b"]).await;
        assert_eq!(overlap_rows(&db).await, [1, 1, 0]);
    }

    /// A broker that sends one copy naming both subscriptions: the copy lands
    /// once in each.
    #[tokio::test]
    async fn one_copy_naming_several_subscriptions_lands_once_in_each() {
        let (router, db) = overlap_router();
        deliver_on_c1(&router, "a/b", &["a/#", "a/b"]).await;
        assert_eq!(overlap_rows(&db).await, [1, 1, 0]);
    }

    /// A subscription with no route on this client stores nothing anywhere —
    /// not even in a covering channel the copy was not sent for.
    #[tokio::test]
    async fn a_subscription_with_no_channel_stores_nothing() {
        let (router, db) = overlap_router();
        deliver_on_c1(&router, "a/c", &["a/c"]).await;
        assert_eq!(overlap_rows(&db).await, [0, 0, 0]);
    }

    /// The identifier never overrides the topic check: a subscription whose
    /// filter does not cover the topic is skipped, and the others on the same
    /// copy are still served.
    #[tokio::test]
    async fn a_subscription_whose_filter_does_not_cover_the_topic_is_skipped() {
        let (router, db) = overlap_router();
        deliver_on_c1(&router, "x/y", &["a/#"]).await;
        assert_eq!(overlap_rows(&db).await, [0, 0, 0]);
        deliver_on_c1(&router, "a/c", &["a/b", "a/#"]).await;
        assert_eq!(overlap_rows(&db).await, [1, 0, 0]);
    }

    /// The by-topic rule is kept for a copy naming no subscription.
    #[tokio::test]
    async fn a_copy_naming_no_subscription_reaches_every_covering_channel_on_its_client() {
        let (router, db) = overlap_router();
        router
            .deliver_inbound(
                "c1",
                "a/b",
                InboundPayload::new("x", None),
                1,
                InboundRouting::ByTopic,
            )
            .await;
        assert_eq!(overlap_rows(&db).await, [1, 1, 0]);
    }

    /// A subscription whose route has gone stores nothing in either channel.
    #[tokio::test]
    async fn a_subscription_whose_route_was_removed_stores_nothing() {
        let (router, db) = overlap_router();
        assert!(router.remove_route(mqtt_channel_uuid_from_address("mqtt:c1:a/b")));
        deliver_on_c1(&router, "a/b", &["a/b"]).await;
        assert_eq!(overlap_rows(&db).await, [0, 0, 0]);
    }

    /// Another client's route for the same filter is never selected.
    #[tokio::test]
    async fn another_clients_route_for_the_same_filter_is_not_selected() {
        let (router, db) = overlap_router();
        deliver_on_c1(&router, "a/b", &["a/b"]).await;
        assert_eq!(overlap_rows(&db).await, [0, 1, 0]);
        router
            .deliver_inbound(
                "c2",
                "a/b",
                InboundPayload::new("x", None),
                1,
                InboundRouting::BySubscription(vec!["a/b".to_string()]),
            )
            .await;
        assert_eq!(overlap_rows(&db).await, [0, 1, 1]);
    }

    /// Inbound topic matching no bridge logs and drops without panic — and without
    /// storing any row.
    #[tokio::test]
    async fn deliver_inbound_no_match_drops_without_panic() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry("mqtt:homeassistant:home/+/state", None)],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![route("homeassistant", "home/+/state", Urgency::Normal)],
        );

        // Topic does not match the channel filter (extra level).
        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state/extra",
                InboundPayload::new("x", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;

        let count: i64 = {
            let conn = db.lock().await;
            conn.query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
                .expect("count")
        };
        assert_eq!(count, 0, "no-match delivery must store no row");
    }

    /// A broker topic holding `+` or `#` is malformed MQTT and is dropped before
    /// routing, without a panic. The route `home/#` would cover both topics if
    /// they were matched, so a stored row here means the check was skipped; the
    /// wildcard-free delivery afterwards shows the route itself is live.
    #[tokio::test]
    async fn deliver_inbound_drops_a_topic_holding_a_wildcard() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry("mqtt:homeassistant:home/#", None)],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![route("homeassistant", "home/#", Urgency::Normal)],
        );

        let count = || async {
            let conn = db.lock().await;
            conn.query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| {
                r.get::<_, i64>(0)
            })
            .expect("count")
        };

        for topic in ["home/+", "home/#"] {
            router
                .deliver_inbound(
                    "homeassistant",
                    topic,
                    InboundPayload::new("x", None),
                    0,
                    InboundRouting::ByTopic,
                )
                .await;
        }
        assert_eq!(count().await, 0, "a wildcard topic must store no row");

        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen",
                InboundPayload::new("x", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;
        assert_eq!(
            count().await,
            1,
            "a wildcard-free topic under the route is stored"
        );
    }

    /// A channel filter on a *different* client must not match — the client is
    /// part of the routing key.
    #[tokio::test]
    async fn deliver_inbound_wrong_client_drops() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry("mqtt:homeassistant:home/+/state", None)],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![route("homeassistant", "home/+/state", Urgency::Normal)],
        );

        // Matching topic but on a client the channel is not declared on.
        router
            .deliver_inbound(
                "other-client",
                "home/kitchen/state",
                InboundPayload::new("x", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;

        let count: i64 = {
            let conn = db.lock().await;
            conn.query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
                .expect("count")
        };
        assert_eq!(
            count, 0,
            "delivery on a non-matching client must store no row"
        );
    }

    /// A route added at runtime via `add_route` is picked up
    /// by `deliver_inbound`: a topic matching the runtime-added filter routes to
    /// its channel even though it was not in the `set_state` table. Regression
    /// guard that `deliver_inbound` reads the (now mutable) route set, not a frozen
    /// snapshot.
    #[tokio::test]
    async fn deliver_inbound_routes_runtime_added_route() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        let added_addr = "mqtt:homeassistant:home/+/state";
        // The messenger must already know the channel the runtime route targets
        // (channel creation is a separate step; this increment only proves the
        // route table is consulted live). Start with no routes in `set_state`.
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry(added_addr, None)],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(state, vec![]);

        // Before adding the route, a matching delivery routes nowhere → no row.
        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state",
                InboundPayload::new("pre", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;
        let pre_count: i64 = {
            let conn = db.lock().await;
            conn.query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
                .expect("count")
        };
        assert_eq!(pre_count, 0, "no route yet → no row");

        // Add the route at runtime, then the same delivery is routed.
        router.add_route(route("homeassistant", "home/+/state", Urgency::Normal));
        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state",
                InboundPayload::new("post", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;
        let post_count: i64 = {
            let conn = db.lock().await;
            conn.query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
                .expect("count")
        };
        assert_eq!(post_count, 1, "runtime-added route must be consulted live");
    }

    /// `add_route` is idempotent on `channel_uuid` (correctness-1): a second
    /// `add_route` for the same channel (the case where a 2nd app subscribes to an
    /// already-routed `mqtt:` filter) does NOT append a duplicate route, so an
    /// inbound delivery stores exactly one row, not one-per-subscriber. Without the
    /// guard the route table would hold two entries for one channel and
    /// `deliver_inbound` would double-store every message.
    #[tokio::test]
    async fn add_route_is_idempotent_on_channel_uuid() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        let addr = "mqtt:homeassistant:home/+/state";
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry(addr, None)],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(state, vec![]);

        // First add inserts; a second add for the same channel_uuid is a no-op.
        let r1 = route("homeassistant", "home/+/state", Urgency::Normal);
        let r2 = route("homeassistant", "home/+/state", Urgency::Normal);
        assert!(router.add_route(r1), "first add inserts");
        assert!(
            !router.add_route(r2),
            "second add for same channel_uuid is a no-op"
        );

        // One inbound delivery stores exactly one row (not two), proving the route
        // table holds a single entry for the channel.
        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state",
                InboundPayload::new("x", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;
        let count: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_messages WHERE envelope_type='mqtt'",
                [],
                |r| r.get(0),
            )
            .expect("count")
        };
        assert_eq!(
            count, 1,
            "idempotent route → exactly one stored row per inbound message"
        );
    }

    /// `add_route` before `set_state` is a host wiring bug → panic.
    #[tokio::test]
    #[should_panic(expected = "add_route called before set_state")]
    async fn add_route_before_set_state_panics() {
        let router = MqttEventRouterImpl::new();
        router.add_route(route("homeassistant", "home/+/state", Urgency::Normal));
    }

    /// `remove_route` drops the route for a channel:
    /// after removal a matching delivery routes nowhere (no new row), and the
    /// removal is keyed on `channel_uuid` so other routes are untouched. Inverse
    /// of `deliver_inbound_routes_runtime_added_route`.
    #[tokio::test]
    async fn remove_route_drops_matching_route() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        let addr = "mqtt:homeassistant:home/+/state";
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry(addr, None)],
            None,
        ));
        let r = route("homeassistant", "home/+/state", Urgency::Normal);
        let channel_uuid = r.channel_uuid;
        let router = MqttEventRouterImpl::new();
        router.set_state(state, vec![r]);

        // Route is live: a matching delivery stores a row.
        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state",
                InboundPayload::new("pre", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;
        let pre: i64 = {
            let conn = db.lock().await;
            conn.query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
                .expect("count")
        };
        assert_eq!(pre, 1, "route present → row stored");

        // Remove the route by channel_uuid, then the same delivery routes nowhere.
        assert!(router.remove_route(channel_uuid), "route removed");
        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state",
                InboundPayload::new("post", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;
        let post: i64 = {
            let conn = db.lock().await;
            conn.query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
                .expect("count")
        };
        assert_eq!(post, 1, "route removed → delivery no longer routed/stored");

        // A second remove of the now-absent route → false (nothing to remove).
        assert!(!router.remove_route(channel_uuid));
    }

    /// `remove_route` before `set_state` is a host wiring bug → panic.
    #[tokio::test]
    #[should_panic(expected = "remove_route called before set_state")]
    async fn remove_route_before_set_state_panics() {
        let router = MqttEventRouterImpl::new();
        router.remove_route(Uuid::new_v4());
    }

    /// Startup race: `set_state` not yet called → drop with a log, no panic, no row.
    #[tokio::test]
    async fn deliver_inbound_before_set_state_drops() {
        // Build a state-with-db but deliberately do NOT call `set_state`, so the
        // router's state guard fires. The db lets us assert the dropped delivery
        // writes zero rows (a regression that moved the guard below the row-write
        // path would otherwise pass silently).
        let (_state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        let router = MqttEventRouterImpl::new();
        router
            .deliver_inbound(
                "broker1",
                "home/kitchen/state",
                InboundPayload::new("x", None),
                0,
                InboundRouting::ByTopic,
            )
            .await;

        let count: i64 = {
            let conn = db.lock().await;
            conn.query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
                .expect("count query")
        };
        assert_eq!(count, 0, "before-set_state delivery must store no row");
    }

    /// End-to-end: a matched delivery enqueues a pending-push row for the
    /// subscribing app, even with `send_budget = 0` (host ingress bypasses the
    /// CC-facing budget gate). Also asserts the client's configured urgency is
    /// threaded into the stored message row (it drives push scheduling) — a
    /// non-default `High` is used so a hardcoded/dropped urgency would
    /// be caught.
    #[tokio::test]
    async fn deliver_inbound_enqueues_pending_push_for_subscriber() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        let address = "mqtt:homeassistant:home/+/state".to_string();
        let uuid = mqtt_channel_uuid_from_address(&address);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry(&address, Some("myapp"))],
            Some(("myapp", vec!["alice".to_string()], vec![(uuid, address)])),
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![route("homeassistant", "home/+/state", Urgency::High)],
        );

        router
            .deliver_inbound(
                "homeassistant",
                "home/kitchen/state",
                InboundPayload::new("22.5", None),
                1,
                InboundRouting::ByTopic,
            )
            .await;

        let retained: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_messages \
                 WHERE envelope_type = 'mqtt' AND retained_seq IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .expect("query must succeed")
        };
        assert_eq!(
            retained, 1,
            "exactly one committed message, even with send_budget=0"
        );

        // The client's configured urgency must reach the stored message row.
        let stored_urgency: String = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT urgency FROM messaging_messages WHERE envelope_type = 'mqtt' \
                 ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .expect("query must succeed")
        };
        assert_eq!(
            stored_urgency, "high",
            "the client's configured urgency must be threaded into the stored row"
        );
    }

    // --- the raw ingress shape ---

    /// An `mqtt:` channel entry declared `non_text`, with `untyped_content_type`.
    fn raw_mqtt_channel_entry(address: &str, untyped_content_type: Option<&str>) -> ChannelEntry {
        let mut entry = mqtt_channel_entry(address, None);
        entry.resolved_channel.non_text = true;
        entry.resolved_channel.untyped_content_type = untyped_content_type.map(str::to_owned);
        entry
    }

    /// One router over one raw channel `mqtt:broker:cam/#`.
    fn raw_router(untyped_content_type: Option<&str>) -> (MqttEventRouterImpl, brenn_db::Db) {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![raw_mqtt_channel_entry(
                "mqtt:broker:cam/#",
                untyped_content_type,
            )],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(state, vec![route("broker", "cam/#", Urgency::Normal)]);
        (router, db)
    }

    /// One stored row: body bytes, content type, transport column.
    type StoredRow = (Vec<u8>, Option<String>, Option<String>);

    /// Every stored row on `address`, oldest first.
    async fn stored_rows(db: &brenn_db::Db, address: &str) -> Vec<StoredRow> {
        let uuid = mqtt_channel_uuid_from_address(address);
        let conn = db.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT body, content_type, transport FROM messaging_messages \
                 WHERE channel_uuid = ?1 ORDER BY id",
            )
            .unwrap();
        stmt.query_map([uuid.as_bytes().to_vec()], |r| {
            let body = match r.get::<_, rusqlite::types::Value>(0)? {
                rusqlite::types::Value::Blob(b) => b,
                rusqlite::types::Value::Text(t) => t.into_bytes(),
                other => panic!("unexpected body column {other:?}"),
            };
            Ok((body, r.get(1)?, r.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
    }

    async fn deliver(router: &MqttEventRouterImpl, bytes: &[u8], content_type: Option<&str>) {
        router
            .deliver_inbound(
                "broker",
                "cam/front",
                InboundPayload::new(bytes.to_vec(), content_type.map(str::to_owned)),
                1,
                InboundRouting::ByTopic,
            )
            .await;
    }

    /// The one stored row on the raw fixture channel, checking its transport
    /// is the delivery's.
    async fn the_raw_row(db: &brenn_db::Db) -> (Vec<u8>, Option<String>) {
        let mut rows = stored_rows(db, "mqtt:broker:cam/#").await;
        assert_eq!(rows.len(), 1, "exactly one stored message");
        let (body, content_type, transport) = rows.remove(0);
        let transport: TransportMeta =
            serde_json::from_str(&transport.expect("a raw row carries transport")).unwrap();
        let TransportMeta::Mqtt(transport) = transport else {
            panic!("expected mqtt transport, got {transport:?}");
        };
        assert_eq!(transport.client_slug, "broker");
        assert_eq!(
            transport.topic, "cam/front",
            "the actual topic, not the filter"
        );
        assert_eq!(transport.qos, 1);
        (body, content_type)
    }

    const NOT_UTF8: &[u8] = &[0xFF, 0xD8, 0xFF, 0xE0];

    #[tokio::test]
    async fn raw_publisher_non_text_type_is_stored_byte_exact() {
        let (router, db) = raw_router(None);
        deliver(&router, NOT_UTF8, Some("image/jpeg")).await;
        assert_eq!(
            the_raw_row(&db).await,
            (NOT_UTF8.to_vec(), Some("image/jpeg".to_owned()))
        );
    }

    #[tokio::test]
    async fn raw_publisher_text_type_over_utf8_is_a_typed_text_body() {
        let (router, db) = raw_router(None);
        deliver(&router, br#"{"t":22.5}"#, Some("application/json")).await;
        assert_eq!(
            the_raw_row(&db).await,
            (
                br#"{"t":22.5}"#.to_vec(),
                Some("application/json".to_owned())
            )
        );
    }

    #[tokio::test]
    async fn raw_untyped_utf8_with_no_declaration_is_untyped_text() {
        let (router, db) = raw_router(None);
        deliver(&router, b"22.5", None).await;
        assert_eq!(the_raw_row(&db).await, (b"22.5".to_vec(), None));
    }

    #[tokio::test]
    async fn raw_untyped_payload_takes_a_declared_non_text_type_utf8_or_not() {
        let (router, db) = raw_router(Some("application/octet-stream"));
        deliver(&router, NOT_UTF8, None).await;
        deliver(&router, b"22.5", None).await;
        let rows = stored_rows(&db, "mqtt:broker:cam/#").await;
        let stored: Vec<_> = rows.into_iter().map(|(b, ct, _)| (b, ct)).collect();
        let octet = Some("application/octet-stream".to_owned());
        assert_eq!(
            stored,
            vec![
                (NOT_UTF8.to_vec(), octet.clone()),
                (b"22.5".to_vec(), octet)
            ]
        );
    }

    #[tokio::test]
    async fn raw_untyped_utf8_takes_a_declared_text_type() {
        let (router, db) = raw_router(Some("text/plain"));
        deliver(&router, b"22.5", None).await;
        assert_eq!(
            the_raw_row(&db).await,
            (b"22.5".to_vec(), Some("text/plain".to_owned()))
        );
    }

    #[tokio::test]
    async fn raw_publisher_type_wins_over_the_declared_one() {
        let (router, db) = raw_router(Some("application/octet-stream"));
        deliver(&router, NOT_UTF8, Some("image/png")).await;
        assert_eq!(
            the_raw_row(&db).await,
            (NOT_UTF8.to_vec(), Some("image/png".to_owned()))
        );
    }

    /// Each refused input stores nothing and logs the reason.
    async fn assert_dropped(
        untyped_content_type: Option<&str>,
        bytes: &[u8],
        content_type: Option<&str>,
    ) {
        let (router, db) = raw_router(untyped_content_type);
        deliver(&router, bytes, content_type).await;
        assert!(
            stored_rows(&db, "mqtt:broker:cam/#").await.is_empty(),
            "a refused payload must store nothing"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn raw_malformed_publisher_type_is_dropped_whatever_the_channel_declares() {
        assert_dropped(None, b"x", Some("not a type")).await;
        assert_dropped(Some("application/octet-stream"), b"x", Some("")).await;
        let long = format!("application/{}", "x".repeat(300));
        assert_dropped(None, b"x", Some(&long)).await;
        assert!(logs_contain("the publisher's Content-Type"));
        assert!(logs_contain("is not a media type the bus accepts"));
        assert!(
            !logs_contain(&long),
            "a logged content type is truncated, never the whole property"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn raw_publisher_text_type_over_non_utf8_is_dropped() {
        assert_dropped(None, NOT_UTF8, Some("text/plain")).await;
        assert!(logs_contain("is a text type and is not UTF-8"));
        assert!(logs_contain("the publisher's Content-Type"));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn raw_untyped_non_utf8_with_no_declaration_is_dropped_naming_the_remedy() {
        assert_dropped(None, NOT_UTF8, None).await;
        assert!(logs_contain("state untyped_content_type"));
        assert!(logs_contain("cam/front"));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn raw_untyped_non_utf8_under_a_declared_text_type_is_dropped() {
        assert_dropped(Some("text/plain"), NOT_UTF8, None).await;
        assert!(logs_contain("the channel's untyped_content_type"));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn raw_payload_over_the_body_cap_is_dropped() {
        let cap = MessagingGlobalConfig::default().max_body_bytes;
        assert_dropped(Some("application/octet-stream"), &vec![0u8; cap + 1], None).await;
        assert!(logs_contain("messaging.max_body_bytes"));
    }

    #[tokio::test]
    async fn raw_payload_at_the_body_cap_is_stored() {
        let cap = MessagingGlobalConfig::default().max_body_bytes;
        let (router, db) = raw_router(Some("application/octet-stream"));
        deliver(&router, &vec![7u8; cap], None).await;
        assert_eq!(the_raw_row(&db).await.0.len(), cap);
    }

    /// One inbound message matching a raw and a wrapped channel is stored once in
    /// each shape, and the wrapped JSON is pinned byte for byte.
    #[tokio::test]
    async fn one_message_on_a_raw_and_a_wrapped_channel_is_stored_in_each_shape() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![
                raw_mqtt_channel_entry("mqtt:broker:cam/#", None),
                mqtt_channel_entry("mqtt:broker:cam/+", None),
            ],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(
            state,
            vec![
                route("broker", "cam/#", Urgency::Normal),
                route("broker", "cam/+", Urgency::Normal),
            ],
        );
        deliver(&router, NOT_UTF8, Some("image/jpeg")).await;

        assert_eq!(
            the_raw_row(&db).await,
            (NOT_UTF8.to_vec(), Some("image/jpeg".to_owned()))
        );
        let wrapped = stored_rows(&db, "mqtt:broker:cam/+").await;
        assert_eq!(wrapped.len(), 1);
        let (body, content_type, transport) = &wrapped[0];
        assert_eq!((content_type, transport), (&None, &None));
        let body = std::str::from_utf8(body).unwrap();
        let received_at = received_at_of(body);
        assert_eq!(
            body,
            format!(
                r#"{{"client_slug":"broker","topic":"cam/front","payload":{{"binary_placeholder":true,"content_type":"image/jpeg"}},"received_at":"{received_at}","qos":1}}"#
            )
        );
    }

    /// The `received_at` string of a stored wrapper.
    fn received_at_of(body: &str) -> String {
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        value["received_at"].as_str().unwrap().to_owned()
    }

    /// The wrapped JSON for a text payload, pinned against a literal.
    #[tokio::test]
    async fn wrapped_text_payload_json_is_pinned() {
        let (mut state, db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.messenger = Some(messenger_with_channels(
            db.clone(),
            vec![mqtt_channel_entry("mqtt:broker:cam/+", None)],
            None,
        ));
        let router = MqttEventRouterImpl::new();
        router.set_state(state, vec![route("broker", "cam/+", Urgency::Normal)]);
        deliver(&router, b"22.5", None).await;
        let rows = stored_rows(&db, "mqtt:broker:cam/+").await;
        let body = std::str::from_utf8(&rows[0].0).unwrap();
        let received_at = received_at_of(body);
        assert_eq!(
            body,
            format!(
                r#"{{"client_slug":"broker","topic":"cam/front","payload":{{"text":"22.5"}},"received_at":"{received_at}","qos":1}}"#
            )
        );
    }

    // --- Session faults ---

    /// Each fault raises one alert at its severity, naming the client.
    #[tokio::test]
    async fn a_session_fault_raises_one_alert_naming_the_client() {
        use brenn_obs::alerting::make_capturing_alerter_with_severity;

        let (alert_dispatcher, captured, handle) = make_capturing_alerter_with_severity();
        let (mut state, _db, _) = test_state_with_user_and_app("myapp", vec!["alice".to_string()]);
        state.alert_dispatcher = alert_dispatcher;
        let router = MqttEventRouterImpl::new();
        router.set_state(state, vec![]);

        router
            .session_fault("sensors", SessionFault::SessionDiscarded { pkid: 7 })
            .await;
        router
            .session_fault(
                "sensors",
                SessionFault::GaveUp {
                    reason: "unsolicited again".to_string(),
                },
            )
            .await;
        drop(router);
        handle.await.expect("alert drainer");

        let alerts = captured.lock().unwrap();
        assert_eq!(alerts.len(), 2, "one alert per fault: {alerts:?}");
        assert!(matches!(alerts[0].0, AlertSeverity::Warning));
        assert!(alerts[0].1.contains("sensors") && alerts[0].2.contains("sensors"));
        assert!(alerts[0].2.contains("packet identifier 7"));
        assert!(matches!(alerts[1].0, AlertSeverity::Critical));
        assert!(alerts[1].1.contains("sensors") && alerts[1].2.contains("sensors"));
        assert!(alerts[1].2.contains("unsolicited again"));
    }

    /// Before boot wires the state in, a fault is logged and dropped: no
    /// panic, and no alert.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn a_session_fault_before_the_state_is_set_raises_nothing() {
        let router = MqttEventRouterImpl::new();
        router
            .session_fault("sensors", SessionFault::SessionDiscarded { pkid: 7 })
            .await;
        assert!(logs_contain("could not be raised"));
    }
}

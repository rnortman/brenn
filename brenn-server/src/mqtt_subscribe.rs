//! Runtime `mqtt:` subscribe-**activation** wrapper (design §2.3 composition).
//!
//! The transport-agnostic *core* of a dynamic subscribe lives in `brenn-lib`
//! ([`Messenger::subscribe_dynamic`]): it resolves params, persists the durable
//! row + `messaging_subscriptions` mirror, creates a not-yet-existing `mqtt:`
//! channel, and folds the subscriber into the in-memory directory. That core is
//! transport-blind and makes **no broker call**.
//!
//! For `mqtt:` addresses there is one more transport-specific step — *activation*
//! — that only the binary crate can do (it owns the live `MqttService` ingress
//! supervisors and the concrete `MqttEventRouterImpl`): issue the broker
//! SUBSCRIBE and add the routing-table `IngressRoute` so the broker's deliveries
//! actually flow to the new channel. This module is that activation, composed on
//! top of the lib core. `brenn:`/`webhook:` need no activation — the lib core is
//! the whole operation for them.
//!
//! Ordering (design §2.3 steps 1/3/4/5/6):
//! 1. **Configured-client guard, *before* persisting** (step 1). An `mqtt:`
//!    subscribe to a client with no running ingress supervisor is a tool error;
//!    we never spawn supervisors at runtime. The guard runs first so the lib core
//!    never creates a durable channel + row for an unconfigured client.
//! 2. **`qos` default resolution** (step 5). An omitted `qos` for `mqtt:` defaults
//!    to the client's `[[mqtt_client]].qos` (the same value a static subscription
//!    on this client would use), resolved from the live ingress handle.
//! 3. **Lib core** ([`Messenger::subscribe_dynamic`], steps 3/4): resolve +
//!    persist + create-channel-if-absent + directory fold.
//! 4. **Router `IngressRoute` add** (step 6), stamped with the client's ingress
//!    `urgency`, so `deliver_inbound` routes matching topics to the new channel.
//! 5. **Broker SUBSCRIBE + reconnect-set registration** (step 5), via
//!    [`MqttService::subscribe_filter`] (live now if connected, deferred to next
//!    reconnect if disconnected — both survive reconnect).
//!
//! Every failure path is a returned error, never a panic — a misconfigured
//! dynamic subscribe is LLM/attacker-shaped tool input, not a host bug (CLAUDE.md
//! "panic on host bug, error on bad input"). The caller is the `MessageSubscribe`
//! tool (design §2.4, a later increment).

use brenn_lib::messaging::ChannelScheme;
use brenn_lib::messaging::subscribe::{
    DynamicSubscribeParams, RuntimeSubscribeError, RuntimeUnsubscribeError, SubscribeOutcome,
    UnsubscribeOutcome,
};
use brenn_lib::mqtt::address::parse_mqtt_address;
use brenn_lib::mqtt::service::{IngressSubscribeOutcome, IngressUnsubscribeOutcome};

use crate::active_bridge::ActiveBridge;
use crate::mqtt_router::IngressRoute;

/// Successful outcome of a runtime dynamic subscribe (design §2.3).
///
/// Reports what activation did so the `MessageSubscribe` tool can give the LLM an
/// honest status — in particular distinguishing "subscribed and live now" from
/// "subscribed, delivery starts on the next broker reconnect" (design §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeActivation {
    /// The calling app already held an identical dynamic subscription on this
    /// channel — an idempotent no-op (design §2.4). Nothing was created and the
    /// transport was NOT re-activated (it is already live). Applies to every
    /// transport.
    AlreadySubscribed,
    /// `brenn:`/`webhook:` subscribe — no broker activation; the directory write
    /// is the whole operation.
    LocalOnly,
    /// `mqtt:` subscribe; the client was live and the broker SUBSCRIBE went out
    /// now (delivery — including any retained message — starts immediately).
    MqttLive,
    /// `mqtt:` subscribe; the client is currently disconnected. The subscription
    /// is durable and the route is added; the broker SUBSCRIBE is deferred to the
    /// next reconnect (design §3). Not an error.
    MqttDeferredDisconnected,
    /// `mqtt:` subscribe; the client was live but the broker SUBSCRIBE *send*
    /// failed (e.g. send-queue full). The subscription is durable and the route
    /// is added; the reconnect re-assert will retry. Carries the client error.
    MqttSendFailed(String),
}

impl SubscribeActivation {
    /// The LLM-facing status string for this outcome (design §2.4). Pure — the
    /// caller logs a warn separately for [`Self::MqttSendFailed`]. A send failure
    /// still leaves a durable subscription + route (the reconnect re-assert
    /// retries), so it is reported as `subscribed_pending_reconnect`, never an
    /// error — a future change that maps it to a hard error would lie to the LLM
    /// about whether the subscription persisted (test-5 pins this).
    pub fn status_str(&self) -> &'static str {
        match self {
            SubscribeActivation::AlreadySubscribed => "already_subscribed",
            SubscribeActivation::LocalOnly | SubscribeActivation::MqttLive => "subscribed",
            SubscribeActivation::MqttDeferredDisconnected
            | SubscribeActivation::MqttSendFailed(_) => "subscribed_pending_reconnect",
        }
    }
}

/// Error from the runtime subscribe-activation wrapper.
///
/// All variants are returned, never panicked (tool/LLM input, design §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeActivateError {
    /// MQTT is not configured at all (no `[[mqtt_client]]`), so this server has no
    /// `MqttService`. An `mqtt:` subscribe is impossible.
    MqttNotConfigured,
    /// The `mqtt:` address names a client that has no running ingress supervisor
    /// (no `[[mqtt_client]]` with that slug). We never spawn supervisors at
    /// runtime (design §2.3 step 1), so this is terminal — and it is checked
    /// **before** the lib core persists anything, so no durable channel/row is
    /// created for an unconfigured client.
    UnconfiguredMqttClient { client: String },
    /// The lib core (`Messenger::subscribe_dynamic`) rejected the subscribe —
    /// unknown `brenn:`/`webhook:` channel, invalid mqtt filter, a duplicate
    /// dynamic sub for this app, `qos` on a non-mqtt address, or a
    /// resolver/invariant violation. Carries the core's typed error.
    Core(RuntimeSubscribeError),
    /// The app's resolved `AppPolicy` does not permit a dynamic subscribe to this
    /// address (any transport: `mqtt:`/`brenn:`/`webhook:`). Either the
    /// `DynamicSubscribe` grant or the per-transport grant is missing, or no ACL
    /// matcher on the relevant list covers the requested resource. Returned (never
    /// panicked): the address is LLM/tool input, and CC output is
    /// attacker-influenceable.
    PolicyDenied { address: String },
}

impl std::fmt::Display for SubscribeActivateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscribeActivateError::MqttNotConfigured => write!(
                f,
                "MQTT is not configured on this server (no [[mqtt_client]]); \
                 mqtt: subscriptions are unavailable"
            ),
            SubscribeActivateError::UnconfiguredMqttClient { client } => write!(
                f,
                "mqtt client {client:?} is not a configured [[mqtt_client]] with a running \
                 ingress supervisor; cannot subscribe (clients are not created at runtime)"
            ),
            SubscribeActivateError::Core(e) => write!(f, "{e}"),
            // Deliberately does NOT echo whether the client/channel exists — avoid
            // leaking config topology to an unauthorized caller (§3.3).
            SubscribeActivateError::PolicyDenied { address } => {
                write!(
                    f,
                    "subscribe to {address:?} denied by this app's access policy"
                )
            }
        }
    }
}

impl std::error::Error for SubscribeActivateError {}

impl From<RuntimeSubscribeError> for SubscribeActivateError {
    fn from(e: RuntimeSubscribeError) -> Self {
        SubscribeActivateError::Core(e)
    }
}

/// Create a dynamic subscription for `app_slug` on `address` and activate the
/// transport (design §2.3 composition). The single runtime subscribe entry point
/// the `MessageSubscribe` tool calls.
///
/// `brenn:`/`webhook:` are pure lib-core operations (no activation).
/// `mqtt:` runs the configured-client guard + qos-default resolution **before**
/// the lib core (so nothing is persisted for an unconfigured client), then adds
/// the router `IngressRoute` and issues the broker SUBSCRIBE after the core
/// persists the durable state.
/// Fetch the resolved [`AppPolicy`] for a *live* app slug, panicking if absent.
///
/// The `MessageSubscribe` tool is only offered to apps that resolved a config, so
/// every live app carries a (possibly empty) policy; a missing one is a host
/// wiring bug, not bad input — hence a panic rather than a returned error (design
/// §3.1/§3.2).
fn app_policy<'a>(
    messenger: &'a brenn_lib::messaging::Messenger,
    app_slug: &str,
) -> &'a brenn_lib::access::AppPolicy {
    messenger.app_policy(app_slug).unwrap_or_else(|| {
        panic!(
            "subscribe_dynamic_activated: no resolved AppPolicy for app {app_slug:?} \
             — every resolved app carries a (possibly empty) policy"
        )
    })
}

pub async fn subscribe_dynamic_activated(
    bridge: &ActiveBridge,
    app_slug: &str,
    address: &str,
    params: DynamicSubscribeParams,
) -> Result<SubscribeActivation, SubscribeActivateError> {
    let messenger = bridge.messenger().unwrap_or_else(|| {
        // The tool is only registered when messaging is configured, so a missing
        // Messenger at this point is a host wiring bug, not bad input.
        panic!("subscribe_dynamic_activated: Messenger required but absent on ActiveBridge")
    });

    let is_mqtt = matches!(ChannelScheme::of(address), Some(ChannelScheme::Mqtt));
    if !is_mqtt {
        // --- Phase 1: per-app dynamic-subscribe ACL (brenn:/webhook:) ---
        // Runs before subscribe_dynamic persists anything (same no-side-effect
        // invariant as the mqtt: gate below). Classify by scheme and consult the
        // matching ACL list. An unrecognized/malformed prefix is NOT policy-denied:
        // we fall through to the lib core, which returns its canonical address
        // error rather than masking a bad address as PolicyDenied.
        // The address is classified into exactly one arm below, so the policy is
        // looked up once here (mirroring the mqtt: branch, which binds `policy`
        // before the guard) rather than per-arm.
        let policy = app_policy(messenger, app_slug);
        let allowed = match ChannelScheme::split(address) {
            Some((ChannelScheme::Brenn, channel)) => policy.allows_brenn_dynamic_subscribe(channel),
            Some((ChannelScheme::Webhook, endpoint)) => {
                policy.allows_webhook_dynamic_subscribe(endpoint)
            }
            // Ephemeral / PwaPush / Local / unrecognized: do not policy-deny here
            // — defer to the lib core's address validation, which rejects the
            // address with its canonical error below. A deliberate defer, not a
            // missing policy check. (`local:` is page-local and has no
            // server-side subscription at all, dynamic or otherwise.)
            Some((ChannelScheme::Ephemeral | ChannelScheme::PwaPush | ChannelScheme::Local, _))
            | None => true,
            // Mqtt has its own ACL gate in the `is_mqtt` branch above; the
            // `!is_mqtt` guard makes this arm unreachable. Reaching it means the
            // guard was broken — a host bug, so panic rather than guess a policy.
            Some((ChannelScheme::Mqtt, _)) => {
                unreachable!("mqtt address reached the non-mqtt dynamic-subscribe ACL branch")
            }
        };
        if !allowed {
            return Err(SubscribeActivateError::PolicyDenied {
                address: address.to_string(),
            });
        }

        // Non-MQTT: the lib core is the whole operation, no activation.
        let outcome = messenger
            .subscribe_dynamic(app_slug, address, params)
            .await?;
        return Ok(match outcome {
            SubscribeOutcome::Created(_) => SubscribeActivation::LocalOnly,
            SubscribeOutcome::AlreadySubscribedIdentical(_) => {
                SubscribeActivation::AlreadySubscribed
            }
        });
    }

    // --- mqtt: activation ---

    // MQTT must be configured to subscribe to any mqtt: address.
    let mqtt_svc = bridge
        .mqtt_service()
        .ok_or(SubscribeActivateError::MqttNotConfigured)?;

    // Parse the mqtt: address once. Both the configured-client guard and the
    // Phase-1 ACL gate need fields off the same parse (`client` and `topic`),
    // so we keep the whole `MqttAddress` rather than re-parsing. A malformed
    // mqtt: address (bad prefix/slug) is surfaced through the lib core's own
    // InvalidMqttFilter path, so a parse failure here just means "let the core
    // produce the error message": skip the guard and fall through to the core,
    // which rejects it cleanly.
    let parsed = parse_mqtt_address(address).ok();
    let parsed_client = parsed.as_ref().map(|a| a.client.as_str());

    // --- Phase 1: per-app dynamic-subscribe ACL (mqtt:) ---
    // Authorization precedes the configured-client guard so an unauthorized
    // subscribe is rejected without leaking whether the client exists, and
    // nothing is persisted (same no-side-effect invariant as the guard).
    if let Some(addr_parts) = parsed.as_ref() {
        // A malformed address has `parsed == None` and falls through to the
        // core, which produces the canonical InvalidMqttFilter error — we never
        // policy-deny a malformed address (no client to scope against).
        let client = addr_parts.client.as_str();
        let requested_filter = addr_parts.topic.as_str();
        // parse_mqtt_address validates the client slug + topic byte limits but
        // NOT MQTT wildcard placement — that lives in validate_topic_filter_str,
        // which on this path is only called later inside the lib core's
        // resolve_or_create_channel. So the requested filter may be syntactically
        // invalid here (e.g. `sensors/#/extra`, `home/+x`), which would violate
        // filter_covers' validated-input precondition (§5.2) and risk a mis-split
        // over-match (a silent authorization bug). Validate it FIRST; on failure
        // skip the ACL check and fall through to the core, which returns the
        // canonical InvalidMqttFilter — never PolicyDenied a malformed filter
        // (consistent with the malformed-address handling above).
        if brenn_lib::mqtt::address::validate_topic_filter_str(requested_filter).is_ok() {
            let policy = app_policy(messenger, app_slug);
            if !policy.allows_mqtt_dynamic_subscribe(client, requested_filter) {
                return Err(SubscribeActivateError::PolicyDenied {
                    address: address.to_string(),
                });
            }
        }
        // else: malformed wildcard placement — fall through to the core's
        // InvalidMqttFilter.
    }

    // Configured-client guard (design §2.3 step 1): the client must have a running
    // session. Checked before the core so no durable channel/row is created for an
    // unconfigured client. (If the address didn't parse, the core will reject it as
    // InvalidMqttFilter without creating anything.)
    let mut params = params;
    if let Some(client) = parsed_client {
        if mqtt_svc.get_client(client).is_none() {
            return Err(SubscribeActivateError::UnconfiguredMqttClient {
                client: client.to_string(),
            });
        }
        // qos default resolution (design §2.3 step 5): an omitted qos defaults to
        // the client's [[mqtt_client]].qos — the same value a static subscription
        // on this client uses. The client is configured (guard above), so
        // ingress_qos is Some.
        if params.qos.is_none() {
            params.qos = mqtt_svc.ingress_qos(client).await;
        }
    }

    // Lib core (design §2.3 steps 3/4): resolve + persist durable row + mirror +
    // create-channel-if-absent + directory fold. Returns the subscribe outcome.
    let outcome = messenger
        .subscribe_dynamic(app_slug, address, params.clone())
        .await?;
    // Idempotent re-subscribe (identical params, design §2.4): nothing was
    // created and the subscription is already live — do NOT re-add the route or
    // re-issue the broker SUBSCRIBE. Return the no-op status.
    let resolved = match outcome {
        SubscribeOutcome::Created(r) => r,
        SubscribeOutcome::AlreadySubscribedIdentical(_) => {
            return Ok(SubscribeActivation::AlreadySubscribed);
        }
    };

    // The route's channel identity comes straight from the core's resolved output
    // (`channel_uuid`/`channel_address`), which is the authority — never a second,
    // independent re-derivation (errhandling-1). A re-parse + re-hash here could
    // diverge from the core on any future canonicalization change and silently
    // install a route mapping to a UUID the directory does not hold (every inbound
    // message on the filter then dropped, no log), and the old `debug_assert_eq!`
    // guard against that was compiled out in release. The client/topic are still
    // parsed from the address — they are the route's `(client, filter)` *match
    // key* and the broker SUBSCRIBE target, not the channel identity. The core
    // already created the channel from this same address, so it parses here.
    let parsed = parse_mqtt_address(address).expect(
        "subscribe_dynamic_activated: mqtt: address parsed by the core must parse here too",
    );
    let channel_uuid = resolved.channel_uuid;
    let canonical = resolved.channel_address.clone();

    // Router IngressRoute add (design §2.3 step 6): stamp the route with the
    // client's ingress urgency (the same the client's static routes carry) so
    // deliver_inbound routes broker deliveries on this filter to the new channel.
    // The client is configured (guard above), so ingress_urgency is Some.
    let urgency = mqtt_svc
        .ingress_urgency(&parsed.client)
        .await
        .unwrap_or_else(|| {
            panic!(
                "subscribe_dynamic_activated: client {:?} passed the configured-client guard but \
                 has no ingress urgency — registry inconsistency (host bug)",
                parsed.client
            )
        });
    let router = bridge.mqtt_event_router().unwrap_or_else(|| {
        // mqtt_service() is Some (guarded above) but the concrete router is absent:
        // a startup wiring bug (both are populated together when MQTT is configured).
        panic!(
            "subscribe_dynamic_activated: mqtt_service present but mqtt_event_router absent — \
             startup wiring bug"
        )
    });
    router.add_route(IngressRoute {
        client_slug: parsed.client.clone(),
        topic_filter: parsed.topic.clone(),
        channel_address: canonical,
        channel_uuid,
        urgency,
    });

    // Broker SUBSCRIBE + reconnect-set registration (design §2.3 step 5). The
    // resolved qos is concrete (defaulted above if omitted).
    let qos = params.qos.unwrap_or_else(|| {
        panic!(
            "subscribe_dynamic_activated: qos unresolved for configured mqtt client {:?} \
             (default resolution should have filled it)",
            parsed.client
        )
    });
    let outcome = mqtt_svc
        .subscribe_filter(&parsed.client, parsed.topic.clone(), qos)
        .await
        .unwrap_or_else(|| {
            // get_client returned Some at the guard; a None here means the client
            // vanished from the registry mid-call, which is a host bug (the registry
            // is populated once at startup and read-only thereafter).
            panic!(
                "subscribe_dynamic_activated: client {:?} passed the configured-client guard but \
                 subscribe_filter found no session — registry inconsistency (host bug)",
                parsed.client
            )
        });

    Ok(match outcome {
        IngressSubscribeOutcome::SubscribedLive => SubscribeActivation::MqttLive,
        IngressSubscribeOutcome::DeferredDisconnected => {
            SubscribeActivation::MqttDeferredDisconnected
        }
        IngressSubscribeOutcome::SendFailed(e) => SubscribeActivation::MqttSendFailed(e),
    })
}

/// Successful outcome of a runtime dynamic unsubscribe (design §2.3 unsubscribe).
///
/// The inverse of [`SubscribeActivation`]. Reports what deactivation did so the
/// `MessageUnsubscribe` tool can give the LLM an honest status — in particular
/// distinguishing "the broker UNSUBSCRIBE went out now" from "removed, but other
/// subscribers remain so the broker subscription was left in place".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsubscribeActivation {
    /// `brenn:`/`webhook:` unsubscribe — no broker deactivation; the directory +
    /// durable-row delete (done by the lib core) is the whole operation.
    LocalOnly,
    /// `mqtt:` unsubscribe that removed the **last** subscriber on the filter; the
    /// client was live and the broker UNSUBSCRIBE went out now. The route was
    /// dropped from the router table.
    MqttUnsubscribedLive,
    /// `mqtt:` unsubscribe that removed the last subscriber; the client is
    /// currently disconnected. The filter is removed from the reconnect set (so a
    /// future reconnect will not re-subscribe it) and the route was dropped; no
    /// live UNSUBSCRIBE was needed (design §3). Not an error.
    MqttDeferredDisconnected,
    /// `mqtt:` unsubscribe that removed the last subscriber; the client was live
    /// but the broker UNSUBSCRIBE *send* failed. The reconnect set no longer
    /// carries the filter and the route was dropped; the broker keeps delivering
    /// until the next reconnect drops it. Carries the client error.
    MqttSendFailed(String),
    /// `mqtt:` unsubscribe where **other subscribers remain** on the filter; the
    /// broker subscription, the reconnect-set entry, and the `IngressRoute` are all
    /// left in place (design §2.3 unsubscribe). The caller's own dynamic sub was
    /// removed; the channel keeps receiving for the other subscribers.
    MqttOthersRemain,
}

impl UnsubscribeActivation {
    /// The LLM-facing status string for this outcome (design §2.4). Pure — the
    /// caller logs a warn separately for [`Self::MqttSendFailed`]. The caller's
    /// durable sub + directory subscriber are already gone and the filter was
    /// dropped from the reconnect set even on a send failure, so the removal is
    /// reported as done (`unsubscribed`); the next reconnect simply will not
    /// re-subscribe the filter (test-5 pins this).
    pub fn status_str(&self) -> &'static str {
        match self {
            UnsubscribeActivation::LocalOnly
            | UnsubscribeActivation::MqttUnsubscribedLive
            | UnsubscribeActivation::MqttSendFailed(_) => "unsubscribed",
            UnsubscribeActivation::MqttOthersRemain => "unsubscribed_others_remain",
            UnsubscribeActivation::MqttDeferredDisconnected => "unsubscribed_pending_reconnect",
        }
    }
}

/// Error from the runtime unsubscribe-activation wrapper.
///
/// All variants are returned, never panicked (tool/LLM input, design §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsubscribeActivateError {
    /// The lib core (`Messenger::unsubscribe_dynamic`) found no dynamic
    /// subscription to remove for this app on this channel (not-subscribed or
    /// static-only — static subs are config-managed and cannot be unsubscribed at
    /// runtime). Carries the core's typed error.
    Core(RuntimeUnsubscribeError),
}

impl std::fmt::Display for UnsubscribeActivateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnsubscribeActivateError::Core(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for UnsubscribeActivateError {}

impl From<RuntimeUnsubscribeError> for UnsubscribeActivateError {
    fn from(e: RuntimeUnsubscribeError) -> Self {
        UnsubscribeActivateError::Core(e)
    }
}

/// Remove `app_slug`'s dynamic subscription on `address` and deactivate the
/// transport (design §2.3 unsubscribe). The single runtime unsubscribe entry
/// point the `MessageUnsubscribe` tool calls — the inverse of
/// [`subscribe_dynamic_activated`].
///
/// `brenn:`/`webhook:` are pure lib-core operations (no deactivation).
/// `mqtt:` runs the lib core first (it is the source of truth for "did a dynamic
/// sub exist" and whether any other subscriber remains), then — **only** when the
/// removed sub was the last subscriber on the filter
/// ([`UnsubscribeOutcome::still_subscribed`] is `false`) — drops the router
/// `IngressRoute` and issues the broker UNSUBSCRIBE. If other subscribers remain,
/// the broker subscription / route are left in place.
///
/// Ordering note: the core mutates durable + directory state first; the broker
/// UNSUBSCRIBE + route drop follow. This is the mirror of the subscribe wrapper,
/// where the configured-client guard runs *before* the core — there is no
/// pre-core guard to run for unsubscribe (a dynamic `mqtt:` sub can only exist for
/// a client that was configured when it was created), so the core runs first and
/// its `still_subscribed` result drives the deactivation.
pub async fn unsubscribe_dynamic_activated(
    bridge: &ActiveBridge,
    app_slug: &str,
    address: &str,
) -> Result<UnsubscribeActivation, UnsubscribeActivateError> {
    let messenger = bridge.messenger().unwrap_or_else(|| {
        // The tool is only registered when messaging is configured, so a missing
        // Messenger at this point is a host wiring bug, not bad input.
        panic!("unsubscribe_dynamic_activated: Messenger required but absent on ActiveBridge")
    });

    // Lib core (design §2.3 unsubscribe): delete the durable row + mirror and fold
    // the subscriber out of the directory. Errors (StaticSubscription /
    // NotSubscribed) are returned as tool input, never panicked.
    let UnsubscribeOutcome {
        channel_uuid,
        still_subscribed,
        was_dormant,
    } = messenger.unsubscribe_dynamic(app_slug, address).await?;

    // A dormant row (boot-merge `revoked`: ACL revoked, or retain_depth over
    // standing) was never activated this boot — no broker SUBSCRIBE, no route. So
    // there is nothing to deactivate: skip mqtt deactivation entirely rather than
    // depend on the router/broker tolerating removal of a route/filter that was
    // never added. Checked before `still_subscribed` (which for a dormant row
    // reflects the other, untouched subscribers, not this removal).
    if was_dormant {
        return Ok(UnsubscribeActivation::LocalOnly);
    }

    let is_mqtt = matches!(ChannelScheme::of(address), Some(ChannelScheme::Mqtt));
    if !is_mqtt {
        // Non-MQTT: the lib core is the whole operation, no deactivation.
        return Ok(UnsubscribeActivation::LocalOnly);
    }

    // mqtt: deactivation. If other subscribers remain on the filter, leave the
    // broker subscription, the reconnect-set entry, and the route in place
    // (design §2.3 unsubscribe).
    if still_subscribed {
        return Ok(UnsubscribeActivation::MqttOthersRemain);
    }

    // Last subscriber removed: drop the route and issue the broker UNSUBSCRIBE.
    // A dynamic `mqtt:` sub can only have been created for a configured client
    // (subscribe_dynamic_activated's guard), so the MqttService + ingress
    // supervisor must be present here — their absence is a host bug, not bad input.
    let mqtt_svc = bridge.mqtt_service().unwrap_or_else(|| {
        panic!(
            "unsubscribe_dynamic_activated: removed a dynamic mqtt: sub on {address:?} but \
             mqtt_service is absent — a dynamic mqtt: sub cannot exist without a configured \
             client (host bug)"
        )
    });

    // Drop the router IngressRoute for this channel so any in-flight broker
    // delivery on the filter (before the UNSUBSCRIBE takes effect) no longer
    // routes to the now-unsubscribed channel.
    let router = bridge.mqtt_event_router().unwrap_or_else(|| {
        // mqtt_service() is Some but the concrete router is absent: a startup
        // wiring bug (both are populated together when MQTT is configured).
        panic!(
            "unsubscribe_dynamic_activated: mqtt_service present but mqtt_event_router absent — \
             startup wiring bug"
        )
    });
    router.remove_route(channel_uuid);

    // Parse the client + filter to issue the broker UNSUBSCRIBE. The address
    // resolved a channel in the directory (the core removed a subscriber from it),
    // so it must be a well-formed `mqtt:<client>:<topic>` address — a parse failure
    // here is host-state corruption (a stored mqtt: channel with a malformed
    // address), not bad input.
    let parsed = parse_mqtt_address(address).unwrap_or_else(|_| {
        panic!(
            "unsubscribe_dynamic_activated: mqtt: address {address:?} resolved a channel but \
             does not parse — stored channel-address corruption (host bug)"
        )
    });
    let outcome = mqtt_svc
        .unsubscribe_filter(&parsed.client, &parsed.topic)
        .await
        .unwrap_or_else(|| {
            // The dynamic sub existed (core removed it), so the client was
            // configured at creation and its ingress supervisor is registered
            // (registry is populated once at startup, read-only thereafter). A
            // None here means the client vanished from the registry — a host bug.
            panic!(
                "unsubscribe_dynamic_activated: removed a dynamic mqtt: sub for client {:?} but \
                 unsubscribe_filter found no ingress supervisor — registry inconsistency (host bug)",
                parsed.client
            )
        });

    Ok(match outcome {
        IngressUnsubscribeOutcome::UnsubscribedLive => UnsubscribeActivation::MqttUnsubscribedLive,
        IngressUnsubscribeOutcome::DeferredDisconnected => {
            UnsubscribeActivation::MqttDeferredDisconnected
        }
        IngressUnsubscribeOutcome::SendFailed(e) => UnsubscribeActivation::MqttSendFailed(e),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::messaging::SubscriberEntryKind;
    use brenn_lib::messaging::config::Depth;
    use brenn_lib::messaging::db::load_dynamic_subscriptions;
    use brenn_lib::mqtt::payload::InboundPayload;
    use brenn_lib::mqtt::service::MqttEventRouter;

    use crate::active_bridge::ActiveBridge;

    /// Pull-only params (the `push_depth=0` trick), with an optional explicit qos.
    fn pull_only(qos: Option<u8>) -> DynamicSubscribeParams {
        DynamicSubscribeParams {
            push_depth: Depth::Bounded(0),
            retain_depth: Depth::Bounded(5),
            noise: None,
            wake_min: None,
            qos,
        }
    }

    /// Load all durable dynamic-subscription rows for the bridge's messenger.
    async fn dynamic_rows(
        bridge: &ActiveBridge,
    ) -> Vec<brenn_lib::messaging::db::DynamicSubscriptionRow> {
        let conn = bridge.messenger().unwrap().db().lock().await;
        load_dynamic_subscriptions(&conn)
    }

    fn has_app_subscriber(bridge: &ActiveBridge, address: &str, app: &str) -> bool {
        bridge
            .messenger()
            .unwrap()
            .directory()
            .resolve(address)
            .map(|e| {
                e.subscribers
                    .iter()
                    .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == app))
            })
            .unwrap_or(false)
    }

    /// `brenn:` subscribe is a pure lib-core operation: no broker activation
    /// (`LocalOnly`), and the durable row + directory subscriber land.
    #[tokio::test]
    async fn subscribe_brenn_is_local_only() {
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "brenn:test-channel";

        let outcome = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .expect("brenn subscribe succeeds");
        assert_eq!(outcome, SubscribeActivation::LocalOnly);

        let rows = dynamic_rows(&bridge).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].app_slug, "testapp");
        assert_eq!(rows[0].qos, None, "brenn: carries no qos");
        assert!(has_app_subscriber(&bridge, addr, "testapp"));
    }

    /// An `mqtt:` subscribe on a bridge with no `MqttService` (MQTT not configured
    /// on this server) → `MqttNotConfigured`, nothing persisted.
    #[tokio::test]
    async fn subscribe_mqtt_not_configured_when_no_service() {
        // `test_new_with_combined_services` wires a Messenger but no MqttService.
        let bridge = ActiveBridge::test_new_with_combined_services().await;
        let err =
            subscribe_dynamic_activated(&bridge, "testapp", "mqtt:home:sensors/x", pull_only(None))
                .await
                .unwrap_err();
        assert_eq!(err, SubscribeActivateError::MqttNotConfigured);
        assert!(dynamic_rows(&bridge).await.is_empty());
    }

    /// An `mqtt:` subscribe naming a client with no ingress supervisor →
    /// `UnconfiguredMqttClient`, and — crucially — the guard fires **before** the
    /// lib core, so no channel is created and no durable row is written.
    ///
    /// The Phase-1 ACL gate runs *before* the configured-client guard, so the app
    /// must be authorized for the `nope` client to *reach* the guard — otherwise
    /// the subscribe is `PolicyDenied` first (which would not leak whether `nope`
    /// is configured). The fixture policy here authorizes `nope` so this test still
    /// exercises the guard it is about, not the gate.
    #[tokio::test]
    async fn subscribe_mqtt_unconfigured_client_errors_before_persist() {
        let policy = crate::test_support::app_config::mqtt_acl_policy("nope", "#");
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy).await;
        let addr = "mqtt:nope:sensors/x";

        let err = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SubscribeActivateError::UnconfiguredMqttClient {
                client: "nope".to_string()
            }
        );
        // No durable row, and the channel was NOT created (guard before core).
        assert!(dynamic_rows(&bridge).await.is_empty());
        assert!(
            bridge
                .messenger()
                .unwrap()
                .directory()
                .resolve(addr)
                .is_none(),
            "no channel created for an unconfigured client"
        );
    }

    /// A new `mqtt:` filter on the configured `home` client activates: the channel
    /// is created, the subscriber folded, the durable row persisted with the
    /// **defaulted** qos (client default 2, since omitted), and the router route is
    /// added — proven end-to-end by a post-subscribe `deliver_inbound` storing a
    /// row. The client cell is empty (no broker) → `MqttDeferredDisconnected`.
    #[tokio::test]
    async fn subscribe_new_mqtt_filter_activates() {
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "mqtt:home:sensors/+/temp";

        let outcome = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .expect("mqtt subscribe activates");
        assert_eq!(
            outcome,
            SubscribeActivation::MqttDeferredDisconnected,
            "client cell empty → deferred"
        );

        // Channel created + subscriber folded.
        assert!(
            bridge
                .messenger()
                .unwrap()
                .directory()
                .resolve(addr)
                .is_some(),
            "channel created"
        );
        assert!(has_app_subscriber(&bridge, addr, "testapp"));

        // Durable row persisted with the defaulted qos (client default = 2).
        let rows = dynamic_rows(&bridge).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].qos,
            Some(2),
            "omitted qos defaults to the client's [[mqtt_client]].qos"
        );

        // Route was added: a matching inbound delivery now routes to the channel
        // and stores a message row. Before the subscribe there was no route, so a
        // stored row proves the runtime-added IngressRoute is live.
        let router = bridge.mqtt_event_router().unwrap().clone();
        router
            .deliver_inbound(
                "home",
                "sensors/kitchen/temp",
                InboundPayload::Text("22.5".to_string()),
                0,
            )
            .await;
        let stored: i64 = {
            let conn = bridge.messenger().unwrap().db().lock().await;
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_messages WHERE envelope_type='mqtt'",
                [],
                |r| r.get(0),
            )
            .expect("count")
        };
        assert_eq!(stored, 1, "runtime-added route must route the delivery");
    }

    /// A SECOND app subscribing to an already-routed `mqtt:` filter must NOT add a
    /// duplicate `IngressRoute` (correctness-1). The core returns `Created`
    /// per-(app, channel), so the wrapper reaches `add_route` for the 2nd app too;
    /// the idempotency guard on `channel_uuid` keeps one route per channel, so one
    /// inbound delivery stores exactly one row — not one per subscriber. This
    /// drives the real `add_route` path (unlike `unsubscribe_mqtt_others_remain_…`,
    /// which adds the 2nd subscriber via a direct `directory.add_subscriber`).
    #[tokio::test]
    async fn second_app_on_existing_filter_does_not_duplicate_route() {
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "mqtt:home:sensors/+/temp";

        // App A subscribes: channel + route created.
        subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .expect("first app subscribe");
        // App B subscribes to the SAME filter: core returns Created (per-app), the
        // wrapper hits add_route again — but the route must stay single. "otherapp"
        // is in the fixture's apps map with a policy granting DynamicSubscribe +
        // MqttSubscribe + an mqtt_subscribe matcher covering (client "home", filter
        // "sensors/#"), so the Phase-1 gate admits its subscribe (§6.5).
        subscribe_dynamic_activated(&bridge, "otherapp", addr, pull_only(None))
            .await
            .expect("second app subscribe on same filter");

        // Both apps' durable rows exist (two subscribers on the channel).
        assert_eq!(dynamic_rows(&bridge).await.len(), 2);

        // One inbound delivery stores exactly ONE row (not two). A duplicate route
        // would double-store/double-deliver every message.
        assert_eq!(
            deliver_and_count(&bridge, "home", "sensors/k/temp").await,
            1,
            "two subscribers on one filter must not double-store an inbound message"
        );
    }

    /// An explicit `qos` is persisted verbatim, overriding the client default.
    #[tokio::test]
    async fn subscribe_mqtt_explicit_qos_overrides_default() {
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "mqtt:home:sensors/explicit";

        subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(Some(1)))
            .await
            .expect("mqtt subscribe with explicit qos");

        let rows = dynamic_rows(&bridge).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].qos,
            Some(1),
            "explicit qos overrides the client default (2)"
        );
    }

    // --- Phase 1: per-app dynamic-subscribe ACL gate (mqtt:) -------------------

    /// An app that lacks the `DynamicSubscribe` grant is `PolicyDenied` for an
    /// `mqtt:` subscribe — and the deny fires **before any persistence** (no
    /// durable row, no channel). This is the regression test for the closed
    /// `TODO(mqtt-dynamic-subscribe-acl)` hole: pre-Phase-1 this subscribe would
    /// have activated unconditionally.
    #[tokio::test]
    async fn subscribe_mqtt_denied_without_dynamic_subscribe_grant() {
        // Empty policy: no DynamicSubscribe, no MqttSubscribe, no matchers.
        let bridge =
            ActiveBridge::test_new_for_mqtt_subscribe_with_policy(Default::default()).await;
        let addr = "mqtt:home:sensors/+/temp";

        let err = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SubscribeActivateError::PolicyDenied {
                address: addr.to_string()
            }
        );
        // Deny before persistence: no durable row and no channel created.
        assert!(dynamic_rows(&bridge).await.is_empty());
        assert!(
            bridge
                .messenger()
                .unwrap()
                .directory()
                .resolve(addr)
                .is_none(),
            "no channel created on a policy-denied subscribe"
        );
    }

    /// An app holding both grants but whose `mqtt_subscribe` matcher does **not**
    /// cover the requested filter is `PolicyDenied` (the canonical over-grant
    /// trap: allowed `sensors/+/temp`, requested `sensors/#` — broader). Nothing
    /// is persisted.
    #[tokio::test]
    async fn subscribe_mqtt_denied_when_matcher_does_not_cover_request() {
        let policy = crate::test_support::app_config::mqtt_acl_policy("home", "sensors/+/temp");
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy).await;

        // Broader request not covered by the narrow matcher ⇒ deny.
        let broader = "mqtt:home:sensors/#";
        let err = subscribe_dynamic_activated(&bridge, "testapp", broader, pull_only(None))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SubscribeActivateError::PolicyDenied {
                address: broader.to_string()
            }
        );
        // Deny before persistence: no durable row and no channel created
        // (mirrors subscribe_mqtt_denied_without_dynamic_subscribe_grant).
        assert!(dynamic_rows(&bridge).await.is_empty());
        assert!(
            bridge
                .messenger()
                .unwrap()
                .directory()
                .resolve(broader)
                .is_none(),
            "no channel created on a policy-denied broader subscribe"
        );

        // The exact covered filter on the same client is admitted (proves the gate
        // is a real subset check, not a blanket deny).
        let covered = "mqtt:home:sensors/+/temp";
        subscribe_dynamic_activated(&bridge, "testapp", covered, pull_only(None))
            .await
            .expect("covered filter is admitted");
        assert_eq!(dynamic_rows(&bridge).await.len(), 1);
    }

    /// A subscribe to a **different client** than the app's `mqtt_subscribe`
    /// matcher names is `PolicyDenied` — and, because the ACL gate runs *before*
    /// the configured-client guard (design §6.4 ordering), a denied subscribe to
    /// an **unconfigured** client returns `PolicyDenied`, **not**
    /// `UnconfiguredMqttClient`. This pins two coupled security properties:
    /// (1) ordering — authorization precedes the existence check; (2) topology
    /// non-leakage (§7.6) — the error type never reveals whether `other` is a
    /// configured client. The fixture registers exactly one ingress (`home`), so
    /// `other` is both unauthorized (the matcher names `home`) AND unconfigured;
    /// the matcher grants `#` on `home`, proving the deny is the *client mismatch*
    /// and not a too-narrow filter. (Per §6.4: do NOT add an `other` ingress to
    /// the fixture — that would make this fail the configured-client guard
    /// instead, silently voiding the ordering/non-leak proof.)
    #[tokio::test]
    async fn subscribe_mqtt_wrong_client_denies_before_configured_client_guard() {
        // Matcher names `home` with the widest filter; the request names `other`.
        let policy = crate::test_support::app_config::mqtt_acl_policy("home", "#");
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy).await;
        // `other` is NOT a configured ingress (only `home` is) — so if the guard
        // ran first this would be `UnconfiguredMqttClient`.
        let addr = "mqtt:other:sensors/+/temp";

        let err = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SubscribeActivateError::PolicyDenied {
                address: addr.to_string()
            },
            "a wrong-client subscribe must be PolicyDenied by the ACL gate, never \
             UnconfiguredMqttClient — the ACL check precedes the configured-client \
             guard and must not leak whether the client is configured (§6.4/§7.6)"
        );
        // Deny before persistence (and before the guard): no durable row, no channel.
        assert!(dynamic_rows(&bridge).await.is_empty());
        assert!(
            bridge
                .messenger()
                .unwrap()
                .directory()
                .resolve(addr)
                .is_none(),
            "no channel created on a policy-denied wrong-client subscribe"
        );
    }

    /// A *malformed* requested filter (invalid `#` placement) is **not**
    /// `PolicyDenied` — the gate validates the requested filter first and, on
    /// failure, falls through to the lib core, which returns the canonical
    /// `Core(InvalidMqttFilter)`. (Policy-denying a malformed address would both
    /// pre-empt the proper error and risk a mis-split over-match inside
    /// `filter_covers`, whose precondition is validated input.) Asserted even with
    /// an empty policy: the malformed filter must short-circuit to the core
    /// *before* the ACL check is consulted.
    #[tokio::test]
    async fn subscribe_mqtt_malformed_filter_falls_through_to_core_not_policy_denied() {
        let bridge =
            ActiveBridge::test_new_for_mqtt_subscribe_with_policy(Default::default()).await;
        // `sensors/#/extra`: `#` is not terminal — invalid wildcard placement.
        let addr = "mqtt:home:sensors/#/extra";

        let err = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SubscribeActivateError::Core(RuntimeSubscribeError::InvalidMqttFilter { .. })
            ),
            "malformed filter must surface the core's InvalidMqttFilter, not PolicyDenied: {err:?}"
        );
        assert!(dynamic_rows(&bridge).await.is_empty());
    }

    /// Build an `AppPolicy` for the `brenn:`/`webhook:` gate tests: optionally
    /// grant `DynamicSubscribe`, a per-transport grant, and a matcher, so a single
    /// helper drives both the deny paths (omit a grant/matcher) and the allow path.
    fn brenn_webhook_policy(
        dynamic: bool,
        brenn_grant: bool,
        webhook_grant: bool,
        brenn_matchers: Vec<brenn_lib::access::acl::ChannelMatcher>,
        webhook_endpoints: Vec<&str>,
    ) -> brenn_lib::access::AppPolicy {
        use brenn_lib::access::AppCapability;
        use brenn_lib::access::acl::WebhookMatcher;

        let mut policy = brenn_lib::access::AppPolicy::default();
        if dynamic {
            policy.grants.insert(AppCapability::DynamicSubscribe);
        }
        if brenn_grant {
            policy.grants.insert(AppCapability::MessagingSubscribe);
        }
        if webhook_grant {
            policy.grants.insert(AppCapability::Webhook);
        }
        policy.acls.brenn_subscribe = brenn_matchers;
        policy.acls.webhook = webhook_endpoints
            .into_iter()
            .map(|e| WebhookMatcher {
                endpoint: e.to_string(),
            })
            .collect();
        policy
    }

    /// A `brenn:` runtime subscribe with **no** `DynamicSubscribe` grant is
    /// `PolicyDenied` before the lib core runs — nothing persisted, no subscriber
    /// folded (the regression test for the closed hole, brenn: transport). The
    /// `brenn:test-channel` exists in the fixture, so absent the gate this would
    /// have succeeded unconditionally.
    #[tokio::test]
    async fn subscribe_brenn_denied_without_dynamic_subscribe_grant() {
        // MessagingSubscribe + a covering matcher, but NO DynamicSubscribe.
        let policy = brenn_webhook_policy(
            false,
            true,
            false,
            vec![brenn_lib::access::acl::ChannelMatcher::Exact(
                "test-channel".to_string(),
            )],
            vec![],
        );
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy).await;
        let addr = "brenn:test-channel";

        let err = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SubscribeActivateError::PolicyDenied {
                address: addr.to_string()
            }
        );
        // Deny before persistence: no durable row, no subscriber folded.
        assert!(dynamic_rows(&bridge).await.is_empty());
        assert!(!has_app_subscriber(&bridge, addr, "testapp"));
    }

    /// A `brenn:` subscribe with both grants but **no** matcher covering the
    /// requested channel is `PolicyDenied` (deny-all-empty-ACL → `.any` is false);
    /// a matcher that *does* cover it is admitted (proves a real ACL check, not a
    /// blanket deny). Nothing persisted on the deny.
    #[tokio::test]
    async fn subscribe_brenn_denied_when_no_matcher_covers_then_allowed() {
        // Grants present, but the only matcher covers a different channel.
        let deny_policy = brenn_webhook_policy(
            true,
            true,
            false,
            vec![brenn_lib::access::acl::ChannelMatcher::Exact(
                "other-channel".to_string(),
            )],
            vec![],
        );
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(deny_policy).await;
        let addr = "brenn:test-channel";

        let err = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SubscribeActivateError::PolicyDenied {
                address: addr.to_string()
            }
        );
        assert!(dynamic_rows(&bridge).await.is_empty());
        assert!(!has_app_subscriber(&bridge, addr, "testapp"));

        // Now a matcher that DOES cover `test-channel`: admitted (LocalOnly), row
        // + subscriber land — proving the gate is a real ACL check.
        let allow_policy = brenn_webhook_policy(
            true,
            true,
            false,
            vec![brenn_lib::access::acl::ChannelMatcher::Exact(
                "test-channel".to_string(),
            )],
            vec![],
        );
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(allow_policy).await;
        let outcome = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .expect("covered brenn: channel is admitted");
        assert_eq!(outcome, SubscribeActivation::LocalOnly);
        assert_eq!(dynamic_rows(&bridge).await.len(), 1);
        assert!(has_app_subscriber(&bridge, addr, "testapp"));
    }

    /// A `webhook:` runtime subscribe with **no** `Webhook` grant is `PolicyDenied`
    /// before the core — nothing persisted (the webhook half of the closed hole).
    #[tokio::test]
    async fn subscribe_webhook_denied_without_webhook_grant() {
        // DynamicSubscribe + a matching endpoint, but NO Webhook grant.
        let policy = brenn_webhook_policy(true, false, false, vec![], vec!["test-ep"]);
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy).await;
        let addr = "webhook:test-ep";

        let err = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SubscribeActivateError::PolicyDenied {
                address: addr.to_string()
            }
        );
        // Deny before persistence: no durable row written (matches the other
        // deny-path tests' no-side-effect assertion).
        assert!(dynamic_rows(&bridge).await.is_empty());
    }

    /// A `brenn:` runtime subscribe with `DynamicSubscribe` present but the
    /// `MessagingSubscribe` transport grant **absent** is `PolicyDenied` — the
    /// integration-level guard for the second-grant condition of
    /// `allows_brenn_dynamic_subscribe` (parallel to the MQTT branch's
    /// `subscribe_mqtt_denied_without_dynamic_subscribe_grant`). A covering matcher
    /// is present, so absent the transport-grant check this would have succeeded.
    #[tokio::test]
    async fn subscribe_brenn_denied_without_messaging_subscribe_grant() {
        // DynamicSubscribe + a covering matcher, but NO MessagingSubscribe.
        let policy = brenn_webhook_policy(
            true,
            false,
            false,
            vec![brenn_lib::access::acl::ChannelMatcher::Exact(
                "test-channel".to_string(),
            )],
            vec![],
        );
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy).await;
        let addr = "brenn:test-channel";

        let err = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SubscribeActivateError::PolicyDenied {
                address: addr.to_string()
            }
        );
        // Deny before persistence: no durable row, no subscriber folded.
        assert!(dynamic_rows(&bridge).await.is_empty());
        assert!(!has_app_subscriber(&bridge, addr, "testapp"));
    }

    /// An address with an **unrecognized protocol prefix** is *not* `PolicyDenied`
    /// by the non-MQTT gate (the `true` sentinel, design §3.2): it falls through to
    /// the lib core, which returns its canonical address error. Pins that a future
    /// change flipping the sentinel to `false` (policy-denying an unparseable
    /// address) would be caught. The app is fully granted, so a `PolicyDenied` here
    /// could only come from the sentinel arm, not a missing grant.
    #[tokio::test]
    async fn subscribe_unknown_prefix_falls_through_not_policy_denied() {
        // Fully-permissive policy: any PolicyDenied could only be the sentinel arm.
        let policy = brenn_webhook_policy(
            true,
            true,
            true,
            vec![brenn_lib::access::acl::ChannelMatcher::Prefix(
                "test".to_string(),
            )],
            vec!["test-ep"],
        );
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy).await;
        let addr = "bogus-proto:whatever";

        let err = subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .unwrap_err();
        assert!(
            !matches!(err, SubscribeActivateError::PolicyDenied { .. }),
            "an unrecognized prefix must fall through to the core's address error, \
             never PolicyDenied (the §3.2 sentinel): {err:?}"
        );
        // Nothing persisted regardless of which core error fired.
        assert!(dynamic_rows(&bridge).await.is_empty());
    }

    /// A `webhook:` subscribe with the `Webhook` grant + a matching endpoint is
    /// **not** `PolicyDenied`: the gate admits it and it falls through to the lib
    /// core, which rejects the (absent) webhook channel with `Core(UnknownChannel)`
    /// (the fixture seeds no webhook channel). That the error is `UnknownChannel`
    /// and not `PolicyDenied` proves the gate let an authorized webhook subscribe
    /// through. A non-matching endpoint with the same grant is `PolicyDenied`.
    #[tokio::test]
    async fn subscribe_webhook_authorized_passes_gate_to_core() {
        let policy = brenn_webhook_policy(true, false, true, vec![], vec!["test-ep"]);
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy).await;

        // Authorized endpoint: gate passes, core rejects the absent channel.
        let err =
            subscribe_dynamic_activated(&bridge, "testapp", "webhook:test-ep", pull_only(None))
                .await
                .unwrap_err();
        assert!(
            matches!(
                err,
                SubscribeActivateError::Core(RuntimeSubscribeError::UnknownChannel { .. })
            ),
            "authorized webhook subscribe must pass the gate and hit the core's \
             UnknownChannel, not PolicyDenied: {err:?}"
        );

        // A different endpoint is not covered by the matcher ⇒ PolicyDenied.
        let other = "webhook:other-ep";
        let err = subscribe_dynamic_activated(&bridge, "testapp", other, pull_only(None))
            .await
            .unwrap_err();
        assert_eq!(
            err,
            SubscribeActivateError::PolicyDenied {
                address: other.to_string()
            }
        );
        assert!(dynamic_rows(&bridge).await.is_empty());
    }

    /// `qos` supplied for a non-MQTT (`brenn:`) address is rejected by the lib
    /// core, surfaced as `Core(QosOnNonMqtt)`; nothing persisted.
    #[tokio::test]
    async fn subscribe_qos_on_brenn_errors() {
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let err = subscribe_dynamic_activated(
            &bridge,
            "testapp",
            "brenn:test-channel",
            pull_only(Some(0)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            SubscribeActivateError::Core(RuntimeSubscribeError::QosOnNonMqtt { .. })
        ));
        assert!(dynamic_rows(&bridge).await.is_empty());
    }

    // --- unsubscribe activation -------------------------------------------------

    /// Count `mqtt:` rows stored via `deliver_inbound` on a topic, after the
    /// bridge's router. Used to prove a route is (or is no longer) live.
    async fn deliver_and_count(bridge: &ActiveBridge, client: &str, topic: &str) -> i64 {
        let router = bridge.mqtt_event_router().unwrap().clone();
        router
            .deliver_inbound(client, topic, InboundPayload::Text("v".to_string()), 0)
            .await;
        let conn = bridge.messenger().unwrap().db().lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE envelope_type='mqtt'",
            [],
            |r| r.get(0),
        )
        .expect("count")
    }

    /// The subscribe/unsubscribe status-string mappings are pure and stable
    /// (test-5). In particular a `MqttSendFailed` must map to
    /// `subscribed_pending_reconnect` / `unsubscribed` (not a hard error), because
    /// the durable subscription/removal persisted despite the failed broker send —
    /// a future change that lied to the LLM about persistence is caught here
    /// without needing a live broker.
    #[test]
    fn activation_status_strings_are_stable() {
        assert_eq!(
            SubscribeActivation::AlreadySubscribed.status_str(),
            "already_subscribed"
        );
        assert_eq!(SubscribeActivation::LocalOnly.status_str(), "subscribed");
        assert_eq!(SubscribeActivation::MqttLive.status_str(), "subscribed");
        assert_eq!(
            SubscribeActivation::MqttDeferredDisconnected.status_str(),
            "subscribed_pending_reconnect"
        );
        assert_eq!(
            SubscribeActivation::MqttSendFailed("queue full".to_string()).status_str(),
            "subscribed_pending_reconnect",
            "a failed SUBSCRIBE send still leaves a durable subscription"
        );

        assert_eq!(
            UnsubscribeActivation::LocalOnly.status_str(),
            "unsubscribed"
        );
        assert_eq!(
            UnsubscribeActivation::MqttUnsubscribedLive.status_str(),
            "unsubscribed"
        );
        assert_eq!(
            UnsubscribeActivation::MqttOthersRemain.status_str(),
            "unsubscribed_others_remain"
        );
        assert_eq!(
            UnsubscribeActivation::MqttDeferredDisconnected.status_str(),
            "unsubscribed_pending_reconnect"
        );
        assert_eq!(
            UnsubscribeActivation::MqttSendFailed("queue full".to_string()).status_str(),
            "unsubscribed",
            "a failed UNSUBSCRIBE send still removed the subscription"
        );
    }

    /// `brenn:` unsubscribe is a pure lib-core operation: no broker deactivation
    /// (`LocalOnly`), and the durable row + directory subscriber are removed.
    #[tokio::test]
    async fn unsubscribe_brenn_is_local_only() {
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "brenn:test-channel";

        subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .expect("brenn subscribe");
        assert!(has_app_subscriber(&bridge, addr, "testapp"));
        assert_eq!(dynamic_rows(&bridge).await.len(), 1);

        let outcome = unsubscribe_dynamic_activated(&bridge, "testapp", addr)
            .await
            .expect("brenn unsubscribe succeeds");
        assert_eq!(outcome, UnsubscribeActivation::LocalOnly);

        assert!(
            dynamic_rows(&bridge).await.is_empty(),
            "durable row removed"
        );
        assert!(
            !has_app_subscriber(&bridge, addr, "testapp"),
            "directory subscriber folded out"
        );
    }

    /// Unsubscribing a channel the app has no sub on at all → the lib core's
    /// `NotSubscribed`, surfaced as `Core(..)`.
    #[tokio::test]
    async fn unsubscribe_not_subscribed_errors() {
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let err = unsubscribe_dynamic_activated(&bridge, "testapp", "brenn:test-channel")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            UnsubscribeActivateError::Core(RuntimeUnsubscribeError::NotSubscribed { .. })
        ));
    }

    /// `mqtt:` unsubscribe that removes the **last** subscriber on the filter:
    /// the durable row + directory subscriber are removed, the route is dropped
    /// (proven by a post-unsubscribe `deliver_inbound` storing **no** row), and —
    /// the client cell being empty (no broker) — the outcome is
    /// `MqttDeferredDisconnected` (no live UNSUBSCRIBE needed).
    #[tokio::test]
    async fn unsubscribe_last_mqtt_subscriber_drops_route_deferred() {
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "mqtt:home:sensors/+/temp";

        subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .expect("mqtt subscribe");
        // Route is live: a matching delivery stores a row.
        assert_eq!(
            deliver_and_count(&bridge, "home", "sensors/k/temp").await,
            1
        );

        let outcome = unsubscribe_dynamic_activated(&bridge, "testapp", addr)
            .await
            .expect("mqtt unsubscribe succeeds");
        assert_eq!(
            outcome,
            UnsubscribeActivation::MqttDeferredDisconnected,
            "client cell empty → deferred (no live UNSUBSCRIBE)"
        );

        // Durable row + directory subscriber gone.
        assert!(dynamic_rows(&bridge).await.is_empty());
        assert!(!has_app_subscriber(&bridge, addr, "testapp"));

        // Route dropped: a further matching delivery stores no new row (count stays
        // at the 1 from before the unsubscribe).
        assert_eq!(
            deliver_and_count(&bridge, "home", "sensors/k/temp").await,
            1,
            "route dropped → delivery no longer routed/stored"
        );
    }

    /// A **dormant** `mqtt:` dynamic row (durable-only, no folded directory
    /// subscriber, never activated this boot — the shape a boot-merge `revoked` row
    /// leaves behind) unsubscribes to `LocalOnly`: the `was_dormant` short-circuit
    /// skips all mqtt deactivation, so the router/broker are never asked to drop a
    /// route/filter that was never added. Proven by the durable row being deleted
    /// while `deliver_and_count` stays at 0 (no route ever existed to drop).
    #[tokio::test]
    async fn unsubscribe_dormant_mqtt_row_skips_deactivation() {
        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "mqtt:home:sensors/dormant/temp";
        let messenger = bridge.messenger().unwrap().clone();

        // The lib core creates the channel + durable row + directory subscriber but
        // adds NO route (route activation is the bin-crate wrapper's job). Fold the
        // subscriber back out, leaving the durable row → the dormant shape:
        // durable-only, unfolded, and never activated (no route).
        messenger
            .subscribe_dynamic("testapp", addr, pull_only(None))
            .await
            .expect("core mqtt subscribe");
        let uuid = messenger.directory().resolve(addr).expect("channel").uuid;
        messenger.directory().remove_subscriber(&uuid, "testapp");
        assert!(
            !has_app_subscriber(&bridge, addr, "testapp"),
            "subscriber folded out → dormant"
        );
        assert_eq!(
            dynamic_rows(&bridge).await.len(),
            1,
            "durable row remains (dormant)"
        );
        assert_eq!(
            deliver_and_count(&bridge, "home", "sensors/dormant/temp").await,
            0,
            "no route was ever activated for the dormant row"
        );

        // Unsubscribe: the `was_dormant` short-circuit returns LocalOnly without
        // dropping a route or issuing a broker UNSUBSCRIBE, and the durable row goes.
        let outcome = unsubscribe_dynamic_activated(&bridge, "testapp", addr)
            .await
            .expect("dormant mqtt unsubscribe succeeds");
        assert_eq!(outcome, UnsubscribeActivation::LocalOnly);
        assert!(
            dynamic_rows(&bridge).await.is_empty(),
            "durable row deleted"
        );
        assert_eq!(
            deliver_and_count(&bridge, "home", "sensors/dormant/temp").await,
            0,
            "still no route (nothing added or dropped)"
        );
    }

    /// `mqtt:` unsubscribe where **other subscribers remain** on the filter:
    /// only the caller's dynamic sub is removed; the broker subscription, the
    /// reconnect-set entry, and the `IngressRoute` are left in place
    /// (`MqttOthersRemain`), proven by a post-unsubscribe `deliver_inbound` still
    /// storing a row.
    #[tokio::test]
    async fn unsubscribe_mqtt_others_remain_keeps_route() {
        use brenn_lib::messaging::SubscriberEntry;
        use brenn_lib::messaging::config::{Depth, NoiseLevel};

        let bridge = ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "mqtt:home:sensors/+/temp";

        subscribe_dynamic_activated(&bridge, "testapp", addr, pull_only(None))
            .await
            .expect("mqtt subscribe");

        // Add a second, independent subscriber (a WASM consumer) directly to the
        // channel so the filter has another subscriber after testapp leaves.
        let channel_uuid = bridge
            .messenger()
            .unwrap()
            .directory()
            .resolve(addr)
            .expect("channel exists")
            .uuid;
        let added = bridge.messenger().unwrap().directory().add_subscriber(
            &channel_uuid,
            SubscriberEntry {
                kind: SubscriberEntryKind::Wasm("other".to_string()),
                push_depth: Depth::Bounded(0),
                retain_depth: Depth::Bounded(1),
                noise: NoiseLevel::Silent,
                wake_min: None,
            },
        );
        assert!(added, "second subscriber added");

        let outcome = unsubscribe_dynamic_activated(&bridge, "testapp", addr)
            .await
            .expect("mqtt unsubscribe succeeds");
        assert_eq!(
            outcome,
            UnsubscribeActivation::MqttOthersRemain,
            "another subscriber remains → broker sub/route left in place"
        );

        // testapp's own dynamic state is gone, but the WASM subscriber remains.
        assert!(dynamic_rows(&bridge).await.is_empty());
        assert!(!has_app_subscriber(&bridge, addr, "testapp"));

        // Route kept: a matching delivery still stores a row.
        assert_eq!(
            deliver_and_count(&bridge, "home", "sensors/k/temp").await,
            1,
            "route retained for the remaining subscriber"
        );

        // The reconnect-survival set still carries the filter (broker sub left in
        // place), verified via the QoS lookup.
        assert_eq!(
            bridge
                .mqtt_service()
                .unwrap()
                .ingress_filter_qos("home", "sensors/+/temp")
                .await,
            Some(2),
            "filter remains on the reconnect set"
        );
    }

    /// The `PolicyDenied` `Display` arm must reflect the caller-supplied address
    /// (it is the caller's own input) but must NOT echo whether the named
    /// client/channel exists — leaking config topology to an unauthorized caller
    /// is the info-leak `§3.3` forbids. This pins that contract so a future
    /// refactor that adds `{client}`/`{channel}` to the message is caught.
    #[test]
    fn policy_denied_display_does_not_leak_topology() {
        let msg = SubscribeActivateError::PolicyDenied {
            address: "mqtt:home:sensors/#".to_string(),
        }
        .to_string();

        // (a) The caller's address is reflected (debug-quoted).
        assert!(
            msg.contains("\"mqtt:home:sensors/#\""),
            "address should be present (quoted): {msg:?}"
        );
        // (b) The only occurrence of the client substring is inside the quoted
        // address — a separate client-name echo would push the count past one.
        assert_eq!(
            msg.matches("home").count(),
            1,
            "client name must appear only as part of the quoted address: {msg:?}"
        );
        // (c) No words that would reveal whether the resource exists/is configured.
        for leak in ["exist", "configured", "unknown", "running", "supervisor"] {
            assert!(
                !msg.contains(leak),
                "message must not hint at config topology ({leak:?}): {msg:?}"
            );
        }
    }
}

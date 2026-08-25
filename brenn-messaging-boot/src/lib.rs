//! Build the messaging layer (channel directory, messenger, wake router).
//!
//! This is the boot-time lowering of the messaging configuration: `[[channel]]`,
//! `[[connection]]`, `[[wasm_consumer]]`, `[[surface]]` and `[[remote]]` blocks,
//! plus the transport endpoints resolved elsewhere, become one `MessagingDirectory`
//! and one live `Messenger` with its wake router. Nothing here serves a request or
//! owns a task: [`build_messaging`] returns a [`MessagingResult`] and the
//! composition root above spawns from it.

use std::sync::Arc;

use brenn_lib::config::{AppConfig, BrennConfig};
use brenn_lib::messaging::config::{
    NoiseLevel, ResolvedMessagingConfig, ResolvedSubscription, ResolvedSurface,
    ResolvedSurfaceSubscription, ResolvedWasmConsumer,
};
use brenn_lib::messaging::remote::{ResolvedRemote, resolve_remotes};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, webhook_channel_uuid_from_slug,
};
use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
use brenn_lib::webhook::ResolvedWebhookEndpoint;
use brenn_messaging as messaging;
use brenn_obs::alerting::AlertDispatcher;
use indexmap::IndexMap;

use brenn_server::active_bridge::ActiveBridges;
use brenn_server::messaging_router::WakeRouterImpl;

pub(crate) mod auto;
mod offline;
mod surfaces;
mod wasm;

/// The channel address a static port binding resolves to.
///
/// `declared` is the `channel` the operator wrote on the binding. A binding
/// without one is a *free port*: it declares the port and its tuning, and expects
/// exactly one `[[connection]]` to supply the channel — `lowered`, the address the
/// auto-wiring pass assigned it. Reaching this function with neither means no
/// connection claimed the port — dead config, and a boot panic in the same posture
/// as an output port on a consumer that never activates. Both at once means the
/// port name is claimed twice: by this address and by an auto channel (a
/// `[[connection]]` endpoint, or an io_port wearing the same name).
///
/// An operator-written address is also the one place a hand-computed
/// `auto.<cid>` could enter: auto cids are deterministic, so without a check here
/// a third party could bind an "anonymous" channel with no connection to show for
/// it. A lowered address bypasses the check by construction — the pass placed it.
/// That check is scoped to the pub/sub schemes, which are the only schemes an
/// auto channel is ever placed on: ingress/egress slugs live in their own
/// namespace and wear their own charset, so `webhook:auto.github` names an
/// endpoint's channel and has nothing to do with this machinery.
///
/// `owner` is a pre-formatted config-block label (`[[wasm_consumer]] "filter"`);
/// `port_label` names the binding within it (`subscription port "in"`).
fn bound_channel(
    owner: &str,
    port_label: &str,
    declared: Option<&str>,
    lowered: Option<&str>,
) -> String {
    let Some(address) = declared else {
        return lowered
            .unwrap_or_else(|| {
                panic!(
                    "config: {owner}: {port_label} declares no channel and no [[connection]] \
                     binds it — a free port must be named by exactly one connection's \
                     endpoints, or carry its own channel address"
                )
            })
            .to_string();
    };
    assert!(
        lowered.is_none(),
        "config: {owner}: {port_label} binds channel {address:?}, but the port name is also \
         claimed by an auto channel — either a [[connection]] lists it as an endpoint, or an \
         io_port declares the same name. A port binds exactly one channel: drop this address, \
         or drop the claim.",
    );
    let bare = match ChannelScheme::split(address) {
        Some((ChannelScheme::Brenn | ChannelScheme::Ephemeral | ChannelScheme::Local, name)) => {
            name
        }
        // A bare address defaults to `brenn:`, so it is pub/sub too.
        None => address,
        Some((ChannelScheme::Mqtt | ChannelScheme::Webhook | ChannelScheme::PwaPush, _)) => {
            return address.to_string();
        }
    };
    assert!(
        !messaging::is_auto_channel_name(bare),
        "config: {owner}: {port_label} binds channel {address:?}, which is in the reserved \
         auto namespace — an auto channel's endpoints are its ACL, so it cannot be joined by \
         address; list this port in the channel's [[connection]], or give that channel a name",
    );
    address.to_string()
}

/// Refuse to start when two channel entries claim one uuid.
///
/// The uuid is the channel's identity for cursors, parked messages, and the DB
/// row. Nothing downstream detects a collision — this is the one check, and it
/// must run over the merged set (declared, transport-derived, and synthesized).
fn assert_unique_channel_uuids<'a>(entries: impl Iterator<Item = &'a ChannelEntry>) {
    let mut seen: std::collections::HashMap<uuid::Uuid, &str> = std::collections::HashMap::new();
    for entry in entries {
        if let Some(previous) = seen.insert(entry.uuid, &entry.address) {
            panic!(
                "config: channels {previous:?} and {:?} both carry uuid {} — a channel's uuid is \
                 its identity for cursors, parked messages, and its DB row, so two channels \
                 sharing one would interleave both; give each its own uuid",
                entry.address, entry.uuid,
            );
        }
    }
}

/// Convert an optional `f64` publish-budget knob to integer millitokens, applying
/// the caller's default and fail-fast validation. `field` names the offending knob
/// (slug + subscription/port/client) for the panic message.
///
/// Shared by every declaration that carries the backend's sink-budget knobs —
/// `[[wasm_consumer]]`'s ports and MQTT clients, `[[surface.output]]`'s ports.
/// One resolver so the same knob spelled the same way on two blocks cannot
/// resolve two ways.
///
/// Rejections (all boot panics — host-authored config, BETTER DEAD THAN WRONG):
/// - not finite (NaN / ±inf), or negative;
/// - above [`MAX_WASM_PUBLISH_KNOB`] (keeps millitoken math far from `u64`
///   saturation);
/// - in the open interval `(0, 0.001)` — such a value rounds to 0 millitokens,
///   silently disabling the knob, which is never what the operator meant.
///
/// `0` is accepted (fill 0 = purely input-driven; amplification 0 = context-join).
fn resolve_publish_millitokens(value: Option<f64>, default: f64, field: &str) -> u64 {
    use brenn_lib::messaging::config::{MAX_WASM_PUBLISH_KNOB, MILLITOKENS_PER_PUBLISH};
    let v = value.unwrap_or(default);
    assert!(
        v.is_finite() && v >= 0.0,
        "{field}: publish budget knob must be finite and >= 0 (got {v})",
    );
    assert!(
        v <= MAX_WASM_PUBLISH_KNOB,
        "{field}: publish budget knob {v} exceeds the maximum {MAX_WASM_PUBLISH_KNOB}",
    );
    assert!(
        !(v > 0.0 && v < 0.001),
        "{field}: publish budget knob {v} is in (0, 0.001) and would round to 0 \
         millitokens (silently disabling the sink); use exactly 0 for input-driven, \
         or >= 0.001",
    );
    (v * MILLITOKENS_PER_PUBLISH as f64).round() as u64
}

#[cfg(test)]
mod auto_tests;
#[cfg(test)]
mod build_tests;
#[cfg(test)]
mod surface_tests;
#[cfg(any(test, feature = "testutils"))]
pub mod test_fixtures;
#[cfg(test)]
mod wasm_tests;

use brenn_surface_server::boot_policy::{
    assert_output_bindings_covered, inject_surface_geometry_status_grants,
};
pub use offline::resolve_messaging_offline;
pub(crate) use surfaces::{
    inject_surface_config_subscribe_grants, inject_surface_error_grant, resolve_surfaces,
};
pub(crate) use wasm::resolve_wasm_consumers;

/// Build the `apps_with_messaging` list from a resolved app map and global
/// messaging defaults.
///
/// An app is included when it has a `[app.messaging]` block, transport
/// subscriptions (webhook and/or MQTT bridge), or both. For transport-only apps
/// (no `[app.messaging]`), a minimal `ResolvedMessagingConfig` carrying only the
/// derived transport subscriptions is synthesised — this is the phonebuddy
/// target shape.
///
/// Extracted as a standalone function so it can be unit-tested without
/// constructing a full `BrennConfig` / running database setup.
pub(crate) fn build_apps_with_messaging(
    apps: &IndexMap<String, AppConfig>,
    global_defaults: &brenn_lib::messaging::config::MessagingGlobalConfig,
) -> Vec<(String, ResolvedMessagingConfig)> {
    let mut apps_with_messaging: Vec<(String, ResolvedMessagingConfig)> = Vec::new();

    for (slug, app) in apps.iter() {
        let mut transport_resolved_subs: Vec<ResolvedSubscription> = app
            .webhook_subscriptions
            .iter()
            .map(|ws| ResolvedSubscription {
                channel_uuid: webhook_channel_uuid_from_slug(&ws.endpoint_slug),
                channel_address: format!("webhook:{}", ws.endpoint_slug),
                push_depth: ws.push_depth,
                retain_depth: ws.retain_depth,
                noise: NoiseLevel::Silent,
                wake_min: ws.wake_min,
            })
            .collect();

        // MQTT ingress subscriptions are already fully resolved (address parsed,
        // channel UUID derived, generic params resolved via sub → channel →
        // global). Copy them straight across into the shared subscription list so
        // finalize_directory_with_subscribers populates channel.subscribers for
        // mqtt: channels exactly as it does for webhooks. The
        // per-app resolved sub already carries the full generic set, so this is a
        // direct copy, not a re-resolution.
        transport_resolved_subs.extend(app.mqtt_subscriptions.iter().map(|ms| {
            ResolvedSubscription {
                channel_uuid: ms.channel_uuid,
                channel_address: ms.channel_address.clone(),
                push_depth: ms.push_depth,
                retain_depth: ms.retain_depth,
                noise: ms.noise,
                wake_min: ms.wake_min,
            }
        }));

        match app.messaging.clone() {
            Some(mut resolved_msg) => {
                resolved_msg.subscriptions.extend(transport_resolved_subs);
                apps_with_messaging.push((slug.clone(), resolved_msg));
            }
            None if !transport_resolved_subs.is_empty() => {
                // Transport-only app (phonebuddy target shape): no [app.messaging]
                // block but has [[app.webhook_subscription]] and/or
                // [[app.mqtt_subscription]] entries. Build a minimal
                // ResolvedMessagingConfig so the app appears in
                // apps_with_messaging and its transport subscriptions reach
                // finalize_directory_with_subscribers.
                apps_with_messaging.push((
                    slug.clone(),
                    ResolvedMessagingConfig {
                        send_budget: global_defaults.default_send_budget,
                        subscriptions: transport_resolved_subs,
                    },
                ));
            }
            None => {}
        }
    }

    apps_with_messaging
}

/// The channel set every resolution pass runs against, and the auto-wiring the
/// `[[connection]]` blocks lowered to.
pub(crate) struct ChannelTopology {
    /// The durable `[[channel]]` entries, the caller's environment-derived
    /// entries, and the durable auto channels.
    pub(crate) durable_entries: Vec<ChannelEntry>,
    /// The non-durable `[[channel]]` entries and the non-durable auto channels.
    /// These carry no DB row and are wired to the in-memory substrate.
    pub(crate) nondurable_entries: Vec<ChannelEntry>,
    pub(crate) auto_wiring: auto::AutoWiring,
}

impl ChannelTopology {
    /// A directory over every entry, whatever its class. WASM inputs and surface
    /// bindings alike may target non-durable (`ephemeral:`/`local:`) channels, so
    /// one class-blind lookup answers every binding, whichever store holds the
    /// channel's retention. Subscribers are not populated: resolution needs only
    /// channel identity, transport, and the channel's resolved rungs.
    pub(crate) fn pre_directory(&self) -> MessagingDirectory {
        let mut entries = self.durable_entries.clone();
        entries.extend(self.nondurable_entries.iter().cloned());
        MessagingDirectory::with_entries(entries)
    }
}

/// Lower `[[channel]]` and `[[connection]]` into the channel set the resolvers
/// read, appending `extra_durable_entries` (the caller's environment-derived
/// `webhook:`/`mqtt:` entries, or nothing) to the durable half.
///
/// A pure computation over `config`, which is why the offline pass
/// ([`resolve_messaging_offline`]) and boot ([`build_messaging`]) share it
/// rather than transcribing it: the two must reach the same verdict, and the
/// order and inputs here decide which channels a binding can name at all.
///
/// Auto channels are lowered here, before any directory is built: they are
/// referenced by config bindings, so their entries and grants must be present
/// when the resolvers run. `declared_addresses` is drawn from the entries above
/// them, so an address a connection block mints cannot collide with one the
/// operator declared.
fn lower_channel_topology(
    config: &BrennConfig,
    extra_durable_entries: Vec<ChannelEntry>,
) -> ChannelTopology {
    let global_defaults = &config.messaging;
    let (mut nondurable_entries, durable_channels): (Vec<ChannelEntry>, Vec<ChannelEntry>) =
        messaging::config::build_channel_entries(&config.channels, global_defaults)
            .into_iter()
            .partition(|e| !e.capabilities().durable);
    let mut durable_entries = durable_channels;
    durable_entries.extend(extra_durable_entries);

    let declared_addresses: Vec<&str> = durable_entries
        .iter()
        .chain(nondurable_entries.iter())
        .map(|e| e.address.as_str())
        .collect();
    let auto_wiring = auto::lower_auto_wiring(
        &config.connections,
        &config.wasm_consumers,
        &config.surfaces,
        &declared_addresses,
        global_defaults,
    );
    durable_entries.extend(auto_wiring.durable_entries().iter().cloned());
    nondurable_entries.extend(auto_wiring.nondurable_entries().iter().cloned());

    ChannelTopology {
        durable_entries,
        nondurable_entries,
        auto_wiring,
    }
}

/// Inject the substrate grants every surface holds, then assert output coverage.
///
/// The three injections and the assert are one step, in this order, because the
/// assert reads the injected policy: an output bound to the configured error
/// channel is covered only by the error grant, so asserting before the injection
/// would refuse a configuration that boots.
///
/// # Panics
///
/// On a non-`brenn:` error-channel address, and on an output binding no policy
/// covers.
fn finish_surface_policies(resolved_surfaces: &mut [ResolvedSurface], config: &BrennConfig) {
    let error_channel_bare = config
        .observability
        .surface_error_channel
        .as_deref()
        .map(|addr| system_publisher_bare_channel("[observability] surface_error_channel", addr));
    if let Some(bare) = &error_channel_bare {
        inject_surface_error_grant(resolved_surfaces, bare);
    }

    inject_surface_geometry_status_grants(resolved_surfaces, &config.surface_description.prefix);

    inject_surface_config_subscribe_grants(resolved_surfaces, &config.surface_description.prefix);

    assert_output_bindings_covered(resolved_surfaces);
}

/// Scheme-strip an operator-configured system-publisher channel to its bare
/// `brenn:` name for a code-built `brenn_publish` ACL, panicking (boot fail-fast)
/// on a non-`brenn:` address. `label` names the config key in the panic.
fn system_publisher_bare_channel(label: &str, channel_address: &str) -> String {
    messaging::gates::well_formed_name(channel_address, messaging::ChannelScheme::Brenn)
        .unwrap_or_else(|| {
            panic!(
                "config: {label} channel {channel_address:?} must be a well-formed brenn: address"
            )
        })
        .to_string()
}

/// A durable dynamic `mqtt:` subscription that survived the boot merge and whose
/// channel has **no** static ingress channel backing it ("Dynamic sub +
/// restart → re-issues the MQTT SUBSCRIBE on connect"). The boot merge folds these
/// into the directory, but the ingress supervisor's broker SUBSCRIBE set and the
/// router's `IngressRoute` table are built only from the *static*
/// `mqtt_ingress_channels`; without re-deriving the dynamic ones here a runtime
/// `mqtt:` subscription to a never-statically-declared filter would silently stop
/// delivering after a restart (it would have a directory subscriber but no broker
/// SUBSCRIBE and no route). The caller (`bootstrap/mod.rs`) converts each into a
/// `ResolvedMqttIngressChannel` — filling `urgency` from the client's
/// `[[mqtt_client]]` — and appends it to the ingress-channel list threaded into
/// `start_mqtt`/`wire_mqtt_state`, so the SUBSCRIBE and the route are rebuilt.
pub struct DynamicMqttIngress {
    pub channel_address: String,
    pub channel_uuid: uuid::Uuid,
    pub client_slug: String,
    pub topic: String,
    /// The broker SUBSCRIBE QoS stored on the durable row at creation time (it was
    /// defaulted to the client's `[[mqtt_client]].qos` when omitted). MQTT dynamic
    /// rows always carry a `qos`; a missing one is a host bug.
    pub qos: u8,
}

/// Outcome of building the messaging layer.
pub struct MessagingResult {
    pub messenger: Option<Arc<messaging::Messenger>>,
    pub router: Option<Arc<WakeRouterImpl>>,
    /// Fully resolved WASM consumers, in declaration order.
    /// The caller (`bootstrap/mod.rs`) uses these to load each `ProcessorComponent`,
    /// create a `tokio::sync::Notify` per slug, and register it on the router.
    /// Empty when no `[[wasm_consumer]]` blocks are configured.
    pub wasm_consumers: Vec<ResolvedWasmConsumer>,
    /// Durable dynamic `mqtt:` subscriptions whose filter has no static ingress
    /// channel — these need their broker SUBSCRIBE + `IngressRoute` rebuilt at boot
    /// (see [`DynamicMqttIngress`]). Empty when no such rows survived the merge.
    pub dynamic_mqtt_ingress: Vec<DynamicMqttIngress>,
    /// Fully resolved `[[surface]]` blocks, in declaration order.
    /// Boot-cross-validated. Carried for later consumers (the surface WS
    /// endpoint); the only reader today is the boot-time observability log in
    /// `bootstrap/mod.rs`. Empty when no `[[surface]]` blocks are configured.
    pub surfaces: Vec<ResolvedSurface>,
    /// Fully resolved `[[remote]]` blocks, in declaration order, each carrying
    /// its loaded bearer token and lowered policy. Empty when no `[[remote]]`
    /// blocks are configured.
    pub remotes: Vec<ResolvedRemote>,
    /// Resolved non-durable `[[channel]]` entries (`ephemeral:` and `local:`),
    /// in declaration order. Carried for the store registry the `Messenger` is
    /// built with; the only other reader today is the boot log.
    pub nondurable_channels: Vec<ChannelEntry>,
    /// Collected system participant specs. The caller registers a parked-notify
    /// delivery binding (and spawns a drain task) for each spec with
    /// subscriptions. Empty when messaging is unconfigured or no system
    /// participant is active.
    pub system_participants: Vec<brenn_messaging::system::SystemParticipantSpec>,
}

/// Refuse to start when an operator-declared **static** subscription can never
/// receive a message because its resolved `AppPolicy` does not authorize delivery
/// on the channel.
///
/// The delivery-time ACL gate is universal and deny-by-default: a subscriber
/// receives on a channel only if its policy carries the transport grant **and** a
/// covering ACL matcher (`AppPolicy::allows_channel_access(channel_address)`). For a
/// *dynamically* created subscription that the operator may later re-grant, a
/// missing matcher is a transient/dormant state (handled non-destructively by the
/// boot merge's `revoked` classification). But a **static** subscription is
/// authored in the config and resolved at boot; if its policy does not cover its own
/// channel, the two declarations that must agree (the `[[…subscription]]` and the
/// `grants` + `[…acl…]` block) are out of sync and the subscription is *dead on
/// arrival* — it would silently never deliver. Per CLAUDE.md "BETTER DEAD THAN WRONG / fail
/// fast on bad config", that is a startup-fatal misconfiguration, not a warn.
///
/// Scope: every **static** subscriber in the directory — the config-declared
/// App / Wasm / Surface subscriptions and the code-declared system-participant
/// subscriptions (folded in from each `SystemParticipantSpec`). It runs
/// *before* the dynamic-row boot merge, so dynamic durable rows are not seen
/// here. There are no false positives: a subscription whose policy *does*
/// cover its channel passes the identical `allows_channel_access` check the
/// runtime gate uses, so any config that would actually deliver is accepted
/// unchanged.
///
/// `app_policy` for an `App(slug)` subscriber is the resolved `apps[slug].policy`;
/// for a `Wasm(slug)` subscriber it is the resolved `ResolvedWasmConsumer.policy`;
/// for a `Surface(slug)` subscriber the resolved `ResolvedSurface.policy`; for a
/// `System(component)` subscriber the spec's code-built policy — a system
/// subscription its own policy cannot deliver on is a host wiring bug, caught
/// here at boot rather than skipped.
/// A subscriber slug with no resolvable policy is itself a fatal wiring error
/// (every directory subscriber comes from a resolved app/consumer/spec), so it
/// is reported as a violation rather than skipped.
///
/// # Panics
///
/// Panics (operator-authored config — fail-fast) listing **every** offending
/// static subscription `(subscriber, channel)` so a misconfigured deployment is
/// fixed in one pass, not one boot-crash at a time.
fn validate_static_subscriptions_deliverable(
    directory: &messaging::MessagingDirectory,
    apps: &IndexMap<String, AppConfig>,
    resolved_wasm_consumers: &[ResolvedWasmConsumer],
    resolved_surfaces: &[ResolvedSurface],
    system_participants: &[brenn_messaging::system::SystemParticipantSpec],
) {
    use brenn_lib::messaging::SubscriberEntryKind;

    let wasm_policy_by_slug: std::collections::HashMap<&str, &brenn_lib::access::AppPolicy> =
        resolved_wasm_consumers
            .iter()
            .map(|c| (c.slug.as_str(), &c.policy))
            .collect();
    let surface_policy_by_slug: std::collections::HashMap<&str, &brenn_lib::access::AppPolicy> =
        resolved_surfaces
            .iter()
            .map(|s| (s.slug.as_str(), &s.policy))
            .collect();
    let system_policy_by_component: std::collections::HashMap<&str, &brenn_lib::access::AppPolicy> =
        system_participants
            .iter()
            .map(|s| (s.component, &s.policy))
            .collect();

    let mut violations: Vec<String> = Vec::new();
    for entry in directory.list() {
        for sub in &entry.subscribers {
            let (kind, slug, policy) = match &sub.kind {
                SubscriberEntryKind::App(slug) => {
                    ("app", slug.as_str(), apps.get(slug).map(|a| &a.policy))
                }
                SubscriberEntryKind::Wasm(slug) => (
                    "wasm_consumer",
                    slug.as_str(),
                    wasm_policy_by_slug.get(slug.as_str()).copied(),
                ),
                SubscriberEntryKind::Surface(slug) => (
                    "surface",
                    slug.as_str(),
                    surface_policy_by_slug.get(slug.as_str()).copied(),
                ),
                SubscriberEntryKind::System(component) => (
                    "system",
                    component.as_str(),
                    system_policy_by_component.get(component.as_str()).copied(),
                ),
                // A remote's entries are minted at runtime; one in the static
                // directory is a wiring bug (the unresolvable-policy violation
                // below names it).
                SubscriberEntryKind::Remote(slug) => ("remote", slug.as_str(), None),
                // A conversation reads its command channel under its app's
                // derived harness policy, so that is the policy the check has to
                // read — the same one the runtime read and wake gates consult.
                // This walk runs before chat provisioning today, so nothing
                // reaches this arm at boot; resolving it correctly costs one
                // lookup and keeps the answer right if that order changes.
                SubscriberEntryKind::ChatConversation { app_slug, .. } => (
                    "chat conversation",
                    app_slug.as_str(),
                    apps.get(app_slug).map(|a| &a.chat_harness_policy),
                ),
            };
            match policy {
                Some(policy) if policy.allows_channel_access(&entry.address) => {}
                Some(_) => violations.push(format!(
                    "  - {kind} {slug:?} subscribes to channel {:?} but its access policy \
                     does not authorize delivery there: the required transport capability \
                     grant and/or a covering ACL matcher is absent, so this subscription \
                     can never receive any message",
                    entry.address,
                )),
                None => violations.push(format!(
                    "  - {kind} {slug:?} subscribes to channel {:?} but has no resolved access \
                     policy (host wiring bug or missing app/consumer definition)",
                    entry.address,
                )),
            }
        }
    }

    assert!(
        violations.is_empty(),
        "config: {} static subscription(s) declare a channel their access policy can never \
         deliver on — refusing to start (CLAUDE.md: fail fast on bad config). Add the covering \
         ACL matcher for each channel so the transport grant is derived and delivery is \
         authorized — for an LLM app the `[[app.acl.*]]` block (`brenn_subscribe` / \
         `mqtt_subscribe` / `webhook`); for a `[[wasm_consumer]]` the matching flat list \
         (`subscribe_acl` for `brenn:`, `mqtt_subscribe_acl` for `mqtt:`, `webhook_acl` for \
         `webhook:`) — or remove the subscription:\n{}",
        violations.len(),
        violations.join("\n"),
    );
}

/// True iff `build_messaging` will take its full path (and therefore requires
/// a resolved `server_origin`). Both `build_messaging`'s early-return gate and
/// `run_server`'s `any_messaging` MUST call this — it is the single source of
/// truth for "does this config activate the messaging subsystem." Callers that
/// gate `resolve_source` may OR in additional terms for *other* consumers of
/// the origin (`build_pwa_push`), but must never gate messaging on less than
/// this.
///
/// Must be evaluated on the same `webhook_endpoints` / `mqtt_ingress_channels`
/// values later passed to `build_messaging` (in `run_server` both reads happen
/// before the post-`build_messaging` dynamic-MQTT reinsertion mutates
/// `mqtt_ingress_channels`).
pub fn messaging_configured(
    config: &BrennConfig,
    webhook_endpoints: &IndexMap<String, Arc<ResolvedWebhookEndpoint>>,
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
) -> bool {
    !config.channels.is_empty()
        || !webhook_endpoints.is_empty()
        || !mqtt_ingress_channels.is_empty()
        || !config.wasm_consumers.is_empty()
        || !config.surfaces.is_empty()
        // A remote attaches to the bus and nothing else; declaring one without
        // the subsystem it bridges onto would leave its every subscribe
        // refused.
        || !config.remotes.is_empty()
        // Every LLM app's conversations carry a chat channel family, and those
        // channels are messaging like any other. A deployment that declares an
        // app has therefore activated the subsystem whether or not it declared a
        // `[[channel]]`.
        || !config.apps.is_empty()
}

/// Cross-check every **exact** `[[channel]]` tuning block against the system
/// channels this config actually mints, so a typoed address fails boot instead
/// of silently tuning nothing.
///
/// `mqtt:` exact blocks and every prefix block are deliberately exempt: the MQTT
/// channel population is open-ended (a runtime dynamic subscribe mints channels
/// long after boot), and a prefix is a standing rule for a family whose
/// membership is dynamic.
///
/// # Panics
///
/// On a `webhook:` block naming no declared endpoint, a `brenn:tools/` block
/// naming no registered async tool, or a `brenn:tool-results/` block naming no
/// consumer that holds an async tool grant. Also on an exact key of any scheme
/// but `mqtt:` reaching the fall-through, which is a host bug.
fn validate_exact_tuning_blocks(
    tuning: &messaging::config::SystemChannelTuning,
    webhook_endpoints: &IndexMap<String, Arc<ResolvedWebhookEndpoint>>,
    async_tool_names: &[&'static str],
    inbox_slugs: &std::collections::HashSet<String>,
) {
    use brenn_tool_registry::bus_wiring::{TOOL_RESULTS_NAMESPACE, TOOLS_NAMESPACE};

    for address in tuning.exact_addresses() {
        let (found, what) = if let Some(slug) = address.strip_prefix("webhook:") {
            (
                webhook_endpoints.contains_key(slug),
                "a [[webhook_endpoint]] with that slug",
            )
        } else if let Some(name) = address
            .strip_prefix("brenn:")
            .and_then(|n| n.strip_prefix(TOOLS_NAMESPACE))
        {
            (
                async_tool_names.contains(&name),
                "a registered async tool with that name",
            )
        } else if let Some(slug) = address
            .strip_prefix("brenn:")
            .and_then(|n| n.strip_prefix(TOOL_RESULTS_NAMESPACE))
        {
            (
                inbox_slugs.contains(slug),
                "a consumer holding an async tool grant with that slug",
            )
        } else {
            // `mqtt:` — open-ended population, not boot-checkable. Nothing else
            // reaches here: the table admits only system-minted families and
            // holds their canonical spellings, so an unchecked address of any
            // other scheme would be a hole in exactly the check this function
            // is.
            assert_eq!(
                ChannelScheme::of(address),
                Some(ChannelScheme::Mqtt),
                "messaging: tuning table holds exact address {address:?}, which names no \
                 boot-checkable system family",
            );
            continue;
        };
        assert!(
            found,
            "config: [[channel]] {address:?} tunes a channel this config never mints — \
             there is no {what}",
        );
    }
}

/// Build the channel directory, upsert configured channels, rebuild
/// subscriptions, and construct the messenger + wake router.
///
/// Returns `None` values when `messaging_configured` is false (no `[[channel]]`,
/// `[[webhook_endpoint]]`, mqtt-ingress, `[[wasm_consumer]]`, or `[[surface]]`
/// blocks — messaging effectively disabled, no DB rows touched).
///
/// `server_origin` must be the value resolved once at bootstrap entry (via
/// `resolve_source`) and shared with `build_pwa_push` so both publish paths
/// produce consistent `app:<slug>@<server>` identities. This consistency is
/// enforced structurally by resolving `server_origin` once in `run_server` and
/// passing the same value to both builders; no runtime check verifies origin
/// consistency.
///
/// Background tasks are NOT spawned here — they run after `set_state` in
/// the caller so a server-restart-recovery scan that finds a past-deadline /
/// past-release row already has a fully-initialized router for
/// `spawn_eager_wake`.
#[allow(clippy::too_many_arguments)]
pub async fn build_messaging(
    config: &BrennConfig,
    db: brenn_db::Db,
    apps: &Arc<IndexMap<String, AppConfig>>,
    active_bridges: ActiveBridges,
    alert_dispatcher: AlertDispatcher,
    server_origin: Option<Arc<str>>,
    webhook_endpoints: &IndexMap<String, Arc<ResolvedWebhookEndpoint>>,
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
    system_channel_tuning: &messaging::config::SystemChannelTuning,
    resolved_mqtt_clients: &IndexMap<String, brenn_lib::mqtt::config::MqttClientConfig>,
    tool_registry: &Arc<brenn_tool_registry::ToolRegistry>,
) -> MessagingResult {
    if !messaging_configured(config, webhook_endpoints, mqtt_ingress_channels) {
        return MessagingResult {
            messenger: None,
            router: None,
            wasm_consumers: vec![],
            dynamic_mqtt_ingress: vec![],
            surfaces: vec![],
            remotes: vec![],
            nondurable_channels: vec![],
            system_participants: vec![],
        };
    }

    // --- Derive webhook channel entries from [[webhook_endpoint]] definitions ---
    //
    // Each endpoint produces one `webhook:` ChannelEntry with:
    //   - UUID derived deterministically from the slug (stable across restarts)
    //   - transport_type = Webhook
    //   - the ResolvedChannel every system-minted channel resolves to: the
    //     ingress family's bounded default window, or whatever a `[[channel]]`
    //     tuning block says instead
    //   - mount carried so list_channels() has a single source
    let global_defaults = &config.messaging;
    let webhook_channel_entries: Vec<ChannelEntry> = webhook_endpoints
        .values()
        .map(|ep| {
            let address = format!("webhook:{}", ep.slug);
            let resolved_channel = messaging::config::resolve_system_channel(
                &address,
                system_channel_tuning,
                global_defaults,
            );
            ChannelEntry {
                uuid: webhook_channel_uuid_from_slug(&ep.slug),
                address,
                description: ep.description.clone(),
                resolved_channel,
                subscribers: vec![],
                transport_type: ChannelScheme::Webhook,
                mount: Some(ep.mount.clone()),
            }
        })
        .collect();

    // --- Derive mqtt channel entries from the distinct ingress channels ---
    //
    // Mirrors the webhook channel-entry loop: each distinct ingress channel
    // produces one `mqtt:<client>:<topic>` ChannelEntry with:
    //   - UUID = the resolved-address derivation (stable across restarts,
    //     distinct UUIDv5 namespace from webhook so address spaces never collide)
    //   - transport_type = Mqtt
    //   - the same resolved system-channel set the webhook loop above takes
    // Subscribers start empty; they are populated by
    // `finalize_directory_with_subscribers` from each app's resolved
    // `[[app.mqtt_subscription]]` blocks. MQTT channels have no HTTP mount, so
    // `mount` is None.
    let mqtt_channel_entries: Vec<ChannelEntry> = mqtt_ingress_channels
        .iter()
        .map(|channel| ChannelEntry {
            uuid: channel.channel_uuid,
            address: channel.channel_address.clone(),
            description: None,
            resolved_channel: messaging::config::resolve_system_channel(
                &channel.channel_address,
                system_channel_tuning,
                global_defaults,
            ),
            subscribers: vec![],
            transport_type: ChannelScheme::Mqtt,
            mount: None,
        })
        .collect();

    // --- Build apps_with_messaging, merging webhook subscriptions ---
    let apps_with_messaging = build_apps_with_messaging(apps, global_defaults);

    // The channel set the resolvers read: `[[channel]]` first, then the
    // webhook: and mqtt: entries this host's environment produced, then the
    // auto channels the `[[connection]]` blocks lower to. The offline pass runs
    // the same helper with no environment-derived entries, so the two reach one
    // verdict over one ordering.
    let mut environment_entries = webhook_channel_entries;
    environment_entries.extend(mqtt_channel_entries);
    let topology = lower_channel_topology(config, environment_entries);

    // Resolve WASM consumer subscriptions before finalizing the directory:
    // the directory is built from these same entries, so the wasm_consumers
    // vec must be ready when finalize_directory_with_subscribers runs.
    let pre_directory = topology.pre_directory();
    let ChannelTopology {
        durable_entries: mut all_entries,
        nondurable_entries: nondurable_channels,
        auto_wiring,
    } = topology;
    let mut resolved_wasm_consumers = resolve_wasm_consumers(
        &config.wasm_consumers,
        &pre_directory,
        &config.wasm.store_size_limit,
        resolved_mqtt_clients,
        &auto_wiring,
    );
    // Every input registers in the directory, whatever its channel's class: the
    // registration is where a subscription's resolved parameters (push depth,
    // retain depth, noise rung) live, and the substrate reads them for a
    // ring-backed subscriber exactly as for a durable one. What follows
    // durability is where the subscriber's *position* lives: a durable cursor
    // row, or the ring's in-memory cursor that dies with the data it names.
    let mut wasm_consumers_for_dir: Vec<(String, Vec<ResolvedSubscription>)> =
        resolved_wasm_consumers
            .iter()
            .map(|c| {
                let subs = c.inputs.iter().map(|inp| inp.sub.clone()).collect();
                (c.slug.clone(), subs)
            })
            .collect();

    // Resolve the `[[surface]]` blocks *before* finalizing the directory: every
    // transportable surface subscription folds into a
    // `SubscriberEntryKind::Surface` directory entry, so they must be ready for
    // `finalize_directory_with_subscribers`. `resolve_surfaces` cross-validates
    // every binding against `pre_directory` (the same channel set the final
    // directory is built from — subscribers not yet populated, but resolution
    // needs only channel identity/transport and the channel's resolved rungs),
    // fail-fast on any dead / mis-scheme / policy-uncovered binding, exactly as
    // `resolve_wasm_consumers` does above. `auto_wiring` supplies the address for
    // every free or io port the lowering pass bound.
    let mut resolved_surfaces = resolve_surfaces(
        &config.surfaces,
        &pre_directory,
        global_defaults,
        &auto_wiring,
    );

    let resolved_remotes = resolve_remotes(&config.remotes, global_defaults);

    finish_surface_policies(&mut resolved_surfaces, config);

    // Every surface subscription is a component instance's, keyed
    // `<slug>#<instance>` (`#` is outside the operator slug charset), so surface
    // subscribers are disjoint from app/wasm-consumer slugs by construction — no
    // bare-slug surface subscription exists to collide in the durable
    // push-window keyspace. (The kernel-grain layout subscription, the last
    // bare-slug surface row, was retired.)

    // Strip to slug + durable-subs for the directory build and the DB mirror
    // (both need only these), mirroring `wasm_consumers_for_dir`.
    let surfaces_for_dir: Vec<(String, Vec<ResolvedSurfaceSubscription>)> = resolved_surfaces
        .iter()
        .map(|s| (s.slug.clone(), s.wire_subscriptions.clone()))
        .collect();

    // --- Async tool substrate: request channels, result inboxes, derived grants ---
    //
    // One `brenn:tools/<tool>` request channel per registered async tool (the
    // executor subscribes to each as `system:tool-executor`); for each wasm
    // consumer holding ≥1 async tool grant, one `brenn:tool-results/<slug>` inbox
    // plus the derived async bus grants. The channels ride the same
    // finalize/upsert path as every other channel; the inbox subscription is
    // folded through `wasm_consumers_for_dir` into the directory like a
    // configured wasm subscription, and as a triggering `WasmInputPort` on the
    // consumer's `inputs` so a delivered result activates the consumer.
    // The System request-channel subscriber is folded in from the executor's
    // `SystemParticipantSpec` below and validated by the deliverability check
    // like every other static subscriber.
    let async_tool_names = tool_registry.async_tool_names();
    for tool in &async_tool_names {
        all_entries.push(brenn_tool_registry::bus_wiring::request_channel_entry(
            tool,
            system_channel_tuning,
            global_defaults,
        ));
    }
    // TODO(tool-registry-unregistered-tool-sweep): once tools can be
    // dynamically (de)registered, sweep `brenn:tools/*` pending rows here for
    // tools no longer in the registry — alert and delete them at boot rather
    // than executing a request against a removed tool. Unreachable today: the
    // async tool set is fixed in code, so a pending row can only name a
    // registered tool.
    let mut tuned_inbox_slugs: std::collections::HashSet<String> = std::collections::HashSet::new();
    for consumer in resolved_wasm_consumers.iter_mut() {
        let async_tools =
            brenn_tool_registry::bus_wiring::consumer_async_tools(tool_registry, &consumer.policy);
        if async_tools.is_empty() {
            continue;
        }
        brenn_tool_registry::bus_wiring::derive_async_tool_bus_grants(
            &mut consumer.policy,
            &consumer.slug,
            &async_tools,
        );
        let inbox_entry = brenn_tool_registry::bus_wiring::result_inbox_entry(
            &consumer.slug,
            system_channel_tuning,
            global_defaults,
        );
        // The consumer's subscription follows its inbox's window, so the
        // subscriber never reaches past what the channel block sized.
        let inbox_window = inbox_entry.resolved_channel.retain_depth;
        all_entries.push(inbox_entry);
        let inbox_sub =
            brenn_tool_registry::bus_wiring::inbox_subscription(&consumer.slug, inbox_window);
        let dir_entry = wasm_consumers_for_dir
            .iter_mut()
            .find(|(slug, _)| slug == &consumer.slug)
            .expect("wasm_consumers_for_dir has an entry per resolved consumer");
        dir_entry.1.push(inbox_sub);
        consumer
            .inputs
            .push(brenn_tool_registry::bus_wiring::inbox_input_port(
                &consumer.slug,
                inbox_window,
            ));
        tuned_inbox_slugs.insert(consumer.slug.clone());
    }
    // Every exact tuning block must name a system channel that exists — a typoed
    // address must not silently tune nothing. Runs here because this is the
    // first point where all three populations are known.
    validate_exact_tuning_blocks(
        system_channel_tuning,
        webhook_endpoints,
        &async_tool_names,
        &tuned_inbox_slugs,
    );
    // System participant specs: every `system:` principal is declared here and
    // everything it needs — its subscriber registration, its (subscriber)
    // directory entries, deliverability validation, and (for subscribers) a
    // parked-notify delivery binding — is derived from the one declaration.
    //   - the tool executor, present whenever any async tool is registered
    //     (subscriber; it subscribes to every `brenn:tools/<tool>` request channel);
    //   - `system:surface-help`, publish-only — granted an exact-match publish ACL
    //     on every derived boot-published help/schema/index channel;
    //   - `system:surface-config`, publish-only — granted an exact-match
    //     `ephemeral_publish` ACL on every surface's bindings channel. Separate
    //     from the help identity so each holds exactly its own family.
    //   - `system:chat-roster`, publish-only — granted an exact-match
    //     `brenn_publish` ACL on every app's roster channel. Present whenever
    //     any app is configured; the roster is the only chat address no
    //     conversation's harness may write.
    // A publish-only spec carries no subscriptions, so it gets a registry entry
    // (publish authority) but no directory subscriber entry and no delivery
    // binding — it is never a dispatch target. The surface error channel has no
    // system participant: surfaces publish reports onto it under their own
    // `surface:<slug>` identities (a boot-injected substrate grant).
    let mut system_participants: Vec<brenn_messaging::system::SystemParticipantSpec> = Vec::new();
    if !async_tool_names.is_empty() {
        system_participants.push(brenn_tool_registry::bus_wiring::tool_executor_spec(
            &async_tool_names,
        ));
    }
    let boot_published_bares = brenn_surface_server::description::boot_published_bare_channels(
        &config.surface_description.prefix,
        &resolved_surfaces,
    );
    system_participants.push(brenn_surface_server::description::surface_help_spec(
        &boot_published_bares,
    ));
    let config_bares = brenn_surface_server::description::surface_config_bare_channels(
        &config.surface_description.prefix,
        &resolved_surfaces,
    );
    system_participants.push(brenn_surface_server::description::surface_config_spec(
        &config_bares,
    ));
    // One roster channel per configured app, and the single identity that
    // writes them. Boot-declared rather than provisioned with a conversation:
    // apps are static config, and an app with no conversations still owes a
    // peer the snapshot that says so.
    let roster_bares: Vec<String> = apps
        .keys()
        .map(|slug| brenn_envelope::chat::chat_roster_bare_name(&config.llm_chat.prefix, slug))
        .collect();
    if !roster_bares.is_empty() {
        for slug in apps.keys() {
            all_entries.push(brenn_messaging::chat_roster::chat_roster_entry(
                &config.llm_chat,
                slug,
                global_defaults,
            ));
        }
        system_participants.push(
            brenn_messaging::system::SystemParticipantSpec::publish_only(
                brenn_messaging::chat_roster::CHAT_ROSTER_COMPONENT,
                brenn_lib::messaging::ChannelScheme::Brenn,
                &roster_bares,
            ),
        );
    }
    brenn_messaging::system::fold_spec_subscriptions(&mut all_entries, &system_participants);

    // The non-durable channels join `all_entries` here, after the WASM and
    // surface resolution passes above read them off `pre_directory`. This is
    // the set the final subscriber-carrying directory is built from, and it is
    // built here rather than reusing `pre_directory` because the async-tool
    // channels and the system participants' folded subscriptions landed on
    // `all_entries` in between. `local:` entries are included: a backend
    // `local:` channel is a channel of this process.
    all_entries.extend(nondurable_channels.iter().cloned());

    // All entry sources (declared, webhook/mqtt-derived, auto, tool-substrate)
    // have contributed. This is the only uuid collision check; nothing
    // downstream detects one.
    assert_unique_channel_uuids(all_entries.iter());

    let ring_stores = Arc::new(brenn_messaging_store::store::RingStores::build(
        &nondurable_channels,
    ));

    let directory = Arc::new(messaging::config::finalize_directory_with_subscribers(
        all_entries,
        &apps_with_messaging,
        &wasm_consumers_for_dir,
        &surfaces_for_dir,
    ));

    // The depth ceiling, checked once over the assembled directory: every
    // subscriber sits within its channel's standing_retain_depth. Runs here
    // because this is the first point where every subscriber source — config
    // blocks, folded system participants, auto synthesis, the tool substrate —
    // has contributed.
    messaging::config::validate_subscriber_depth_ceilings(&directory);

    // Boot-time fail-fast validation ("Static subscription with no
    // covering ACL matcher"): every operator-declared STATIC subscription must
    // resolve to a policy that actually authorizes delivery on its channel. A
    // static subscriber whose resolved policy lacks the transport grant + covering
    // ACL matcher can *never* receive a message — the runtime delivery gate would
    // deny-by-default on every delivery. That is a misconfiguration ("BETTER
    // DEAD THAN WRONG / fail fast on bad config"), so we refuse to start with a precise
    // diagnostic naming the offending subscription rather than booting into a
    // silently-dead subscription. The check runs against the just-finalized
    // directory (config static subscribers only — App `[[app.messaging.subscribe]]`
    // / `[[app.mqtt_subscription]]` / `[[app.webhook_subscription]]`,
    // `[[wasm_consumer.subscription]]`, and `brenn:` `[[surface.subscription]]`);
    // it runs BEFORE the dynamic-row boot merge, so dynamic durable rows (handled
    // by the non-destructive `revoked` classification — those may be re-granted
    // later) are deliberately not subject to this panic.
    validate_static_subscriptions_deliverable(
        &directory,
        apps,
        &resolved_wasm_consumers,
        &resolved_surfaces,
        &system_participants,
    );

    // Sync DB state with config: upsert the configured channels here. The
    // durable dynamic subscriptions are merged back *after* the chat backfill
    // below, so the merge judges every row against a directory that already
    // holds every channel this boot can reconstruct.
    {
        let conn = db.lock().await;
        // Durable channels only: a non-durable channel has no `messaging_channels`
        // row to upsert, and writing one would outlive the data it names.
        let entries: Vec<messaging::ChannelEntry> = directory
            .list_durable()
            .iter()
            .map(|e| (**e).clone())
            .collect();
        brenn_messaging_store::db::upsert_channels(&conn, &entries);
    }

    // server_origin is always Some past the messaging_configured early return
    // above, because run_server resolves it whenever that same predicate is true.
    let source = server_origin
        .expect("server_origin must be Some when messaging is configured; this is a bootstrap bug");
    let mut router_inner = WakeRouterImpl::new(active_bridges);
    router_inner.set_alert_dispatcher(alert_dispatcher);
    let router = Arc::new(router_inner);
    // Build the unified subscriber registry: one entry per non-app subscriber
    // (WASM consumer, surface, system component), keyed by its
    // `SubscriberEntryKind`, carrying its resolved policy and declared wake
    // economics. WASM/surface/system policies are not in `merged_apps` — they
    // live on `ResolvedWasmConsumer` / `ResolvedSurface` / the collected
    // `SystemParticipantSpec`s — so `subscriber_policy` and the publish-authority
    // arms reach these subscribers through the registry. All three kinds are
    // cheap to wake, so each is `Eager`.
    let mut subscriber_registrations: std::collections::HashMap<
        brenn_lib::messaging::SubscriberEntryKind,
        brenn_lib::messaging::SubscriberRegistration,
    > = std::collections::HashMap::new();
    for c in &resolved_wasm_consumers {
        subscriber_registrations.insert(
            brenn_lib::messaging::SubscriberEntryKind::Wasm(c.slug.clone()),
            brenn_lib::messaging::SubscriberRegistration {
                policy: Arc::new(c.policy.clone()),
                wake: brenn_lib::messaging::WakeEconomics::Eager,
            },
        );
    }
    // One registration per surface, carrying its own policy. Authority is
    // per-surface — a component's grants are its config-declared bindings, which
    // boot proved the surface's ACLs cover — and so is the subscriber grain: the
    // directory is cut at (surface, channel), and a publish under a component
    // attribution resolves its policy here too.
    //
    // Every declared surface is registered, not just the ones holding a
    // transportable binding today: surface target resolution fails closed on a
    // missing registration, so deriving this set from the bindings would silently
    // deny delivery the moment a binding is added.
    for s in &resolved_surfaces {
        subscriber_registrations.insert(
            brenn_lib::messaging::SubscriberEntryKind::Surface(s.slug.clone()),
            brenn_lib::messaging::SubscriberRegistration {
                policy: Arc::new(s.policy.clone()),
                wake: brenn_lib::messaging::WakeEconomics::Eager,
            },
        );
    }
    // One registration per remote, carrying its lowered policy. A remote is a
    // single principal with no finer grain — no instances, no per-user split —
    // and its wake economics are the attacher's: eager, because a session
    // waiting on a socket is not a subprocess to be spared a wake.
    //
    // Registered here even though a remote holds no boot directory entry: the
    // entries its subscribes mint at runtime resolve their policy through this
    // map, and a runtime-minted entry with no registration would fail closed at
    // the delivery gate with nothing to point at.
    for r in &resolved_remotes {
        subscriber_registrations.insert(
            brenn_lib::messaging::SubscriberEntryKind::Remote(r.slug.clone()),
            brenn_lib::messaging::SubscriberRegistration {
                policy: Arc::new(r.policy.clone()),
                wake: brenn_lib::messaging::WakeEconomics::Eager,
            },
        );
    }
    subscriber_registrations.extend(brenn_messaging::system::registrations_from_specs(
        &system_participants,
    ));
    // The Messenger holds the operator apps as resolved. What it reads off this
    // map is `messaging_send_budget()`, which falls back to the global default
    // stamped on every app at resolve time — so a transport-only app carrying no
    // `[app.messaging]` block reports the same budget a synthesised block would.
    // What each app *subscribes to* is the directory's answer, not this map's.
    let boot_db = db.clone();
    let messenger = messaging::Messenger::new(
        db,
        directory,
        source,
        Arc::clone(apps),
        router.clone() as Arc<dyn messaging::WakeRouter>,
        config.messaging.clone(),
    )
    .with_subscriber_registrations(subscriber_registrations)
    // One budget per attach principal. A surface contributes its own kernel
    // identity plus each component instance it declares, each with its resolved
    // parameters (a declared override, or the defaults) — both from
    // `ResolvedSurface::principal_send_budgets`, built on the same declaration
    // set the sub-identity derivation admits an instance against, so the budget
    // map and the derivation cannot disagree about which principals exist. A
    // remote contributes one, at the shared defaults: it declares no
    // sub-identities and no per-remote knob, and this backstop is the same
    // defense-in-depth bound on the same substrate whoever writes into it. A
    // publish whose principal has no bucket is a boot invariant the publish gate
    // panics on.
    .with_attach_send_budgets(
        resolved_surfaces
            .iter()
            .flat_map(|s| {
                messaging::attach_principal_budgets(
                    messaging::AttachScope::surface(&s.slug),
                    s.principal_send_budgets().collect(),
                )
            })
            .chain(resolved_remotes.iter().flat_map(|r| {
                messaging::attach_principal_budgets(
                    messaging::AttachScope::remote(&r.slug),
                    vec![(None, messaging::config::AttachSendBudget::default())],
                )
            })),
    )
    .with_ring_stores(ring_stores)
    .with_llm_chat(config.llm_chat.clone())
    .with_system_channel_tuning(system_channel_tuning.clone());

    // A dormant conversation must be addressable before anything wakes it — a
    // peer's first command is a publish, and a publish to a name the directory
    // does not hold is refused.
    {
        let conn = boot_db.lock().await;
        messenger.backfill_conversation_chat_channels(&conn);
    }

    // Announce each app's conversation set, now that the backfill has made
    // every conversation on it addressable. A peer holding a fleet grant learns
    // what to subscribe to from here and from nowhere else, so the snapshot goes
    // out before the server accepts a connection.
    for slug in apps.keys() {
        match messenger.publish_chat_roster(slug).await {
            None | Some(brenn_messaging::PublishResult::Ok { .. }) => {}
            Some(other) => panic!(
                "boot: chat roster publish for app {slug:?} did not succeed ({other:?}) — the \
                 roster channel is boot-declared and its writer's policy is code-built, so a \
                 failure is a host bug. Refusing to start."
            ),
        }
    }

    // Fold the durable dynamic subscription rows back onto their channels.
    //
    // This runs after the chat backfill deliberately. A durable dynamic
    // subscription can name a channel no `[[channel]]` block declares — an
    // `mqtt:` filter a runtime subscribe minted, or another conversation's chat
    // record. System-minted families are reconstructed from their rows below;
    // the chat families cannot be, because their depths come from chat
    // provisioning rather than from a block or a family default. The backfill
    // above is what puts them in the directory, so the merge has to see it —
    // otherwise their rows fall to the undeclared-channel arm and go dormant
    // rather than being folded live.
    //
    // `dynamic_mqtt_ingress` collects the surviving dynamic `mqtt:` subscriptions
    // whose filter has no static ingress channel, so the caller can rebuild their
    // broker SUBSCRIBE + `IngressRoute` (boot re-activation gap — see
    // `DynamicMqttIngress`).
    let mut dynamic_mqtt_ingress: Vec<DynamicMqttIngress> = Vec::new();
    // Every dynamic row the merge held back. The rows stay in their table for a
    // later re-grant, so the cursor reconcile below must count them as
    // registrations — the directory alone cannot see them, and for the
    // undeclared-channel class it cannot even name their channel.
    let dormant_dynamic: Vec<messaging::config::DormantSubscription>;
    {
        let conn = boot_db.lock().await;
        let directory = messenger.directory();
        // Boot merge: the directory now holds the static + WASM
        // subscribers, so collision detection against static subs is accurate.
        // Re-fold the durable dynamic rows (the table boot never truncates) onto
        // their channels; rows whose channel row is gone from the table, or that
        // collide with a static sub, are dropped with a warn.
        let dynamic_rows = brenn_messaging_store::db::load_dynamic_subscriptions(&conn);
        // Reconstruct the remaining runtime-created channels into the boot
        // directory. The directory holds the config channels and the backfilled
        // chat families, so a channel that exists *only* in `messaging_channels`
        // because a runtime dynamic subscribe created it (the common `mqtt:`
        // case) is still absent — and the merge below would then hold its
        // surviving durable row dormant instead of folding the subscription live,
        // leaving a subscription that was meant to be running silent. Collect
        // the distinct `channel_uuid`s referenced by the surviving durable rows and
        // load *only* those channels (scoped, never a full-table load — orphan
        // channels are never referenced and so never materialized). Fold each loaded
        // channel that is not already in the directory (config channels are
        // authoritative and stay as-is); the merge then resolves its row by_uuid and
        // keeps it. A referenced UUID absent from `messaging_channels` is left out,
        // so its row classifies as genuine config drift (`dropped`) — unchanged.
        // A row that is present but not reconstructible comes back in the skip
        // report, which is what lets the merge tell a channel that still exists
        // from one an operator deleted.
        let referenced_uuids: Vec<uuid::Uuid> = {
            let mut seen: std::collections::HashSet<uuid::Uuid> = std::collections::HashSet::new();
            dynamic_rows
                .iter()
                .filter(|row| seen.insert(row.channel_uuid))
                .map(|row| row.channel_uuid)
                .collect()
        };
        let reconstruction = brenn_messaging_store::db::load_channels_by_uuids(
            &conn,
            &referenced_uuids,
            system_channel_tuning,
            global_defaults,
        );
        for channel in reconstruction.entries {
            if directory.by_uuid(&channel.uuid).is_none() {
                directory.add_channel(channel);
            }
        }
        let undeclared: std::collections::HashMap<uuid::Uuid, String> =
            reconstruction.skipped.into_iter().collect();
        // Boot-time delivery ACL gate: the merge re-authorizes each
        // folded dynamic row against the app's *current* resolved policy. Dynamic
        // rows only ever fold an `App(slug)` subscriber, so the policy view is the
        // per-app `AppPolicy` off the resolved `apps` map (no WASM lookup needed
        // here). A revoked-ACL (or missing-policy) row is classified `revoked` —
        // neither folded nor pruned — so it lies dormant until the ACL returns.
        let merge_outcome = messaging::config::merge_dynamic_subscriptions(
            directory,
            &dynamic_rows,
            &undeclared,
            &|slug| apps.get(slug).map(|a| &a.policy),
        );
        // Prune the dropped rows from the durable table so the conflict does not
        // recur next boot. The surviving (`kept`) rows need no write: the merge
        // folded them into the directory, which is where a subscription is read
        // from. The `revoked` rows are intentionally left untouched.
        brenn_messaging_store::db::prune_dropped_dynamic_subscriptions(
            &conn,
            &merge_outcome.dropped,
        );
        dormant_dynamic = merge_outcome.revoked;

        // Boot re-activation of dynamic `mqtt:` subs: the supervisor
        // SUBSCRIBE union and the router routes are built only from the *static*
        // `mqtt_ingress_channels`. Any kept dynamic `mqtt:` row whose channel is
        // NOT one of those static channels needs its broker SUBSCRIBE + route
        // rebuilt; collect a descriptor per such channel (deduped by channel_uuid,
        // since two apps can dynamically subscribe to one filter — one channel,
        // one SUBSCRIBE/route). Each row's stored `qos` is the SUBSCRIBE QoS it
        // chose; `urgency` is filled by the caller from the client config.
        let static_mqtt_uuids: std::collections::HashSet<uuid::Uuid> = mqtt_ingress_channels
            .iter()
            .map(|c| c.channel_uuid)
            .collect();
        let mut seen: std::collections::HashSet<uuid::Uuid> = std::collections::HashSet::new();
        for row in &merge_outcome.kept {
            if static_mqtt_uuids.contains(&row.channel_uuid) || !seen.insert(row.channel_uuid) {
                continue;
            }
            let Some(entry) = directory.by_uuid(&row.channel_uuid) else {
                continue;
            };
            if entry.transport_type != ChannelScheme::Mqtt {
                continue;
            }
            // The address is a stored `mqtt:<client>:<topic>` channel the dynamic
            // subscribe created; a parse failure here is host-state corruption.
            let parsed = brenn_lib::mqtt::address::parse_mqtt_address(&entry.address)
                .unwrap_or_else(|_| {
                    panic!(
                        "build_messaging: stored dynamic mqtt channel address {:?} does not parse \
                         — channel-address corruption (host bug)",
                        entry.address
                    )
                });
            let qos = row.qos.unwrap_or_else(|| {
                panic!(
                    "build_messaging: dynamic mqtt subscription on {:?} has no stored qos — \
                     mqtt dynamic rows always persist a qos (host bug)",
                    entry.address
                )
            });
            dynamic_mqtt_ingress.push(DynamicMqttIngress {
                channel_address: entry.address.clone(),
                channel_uuid: row.channel_uuid,
                client_slug: parsed.client,
                topic: parsed.topic,
                qos,
            });
        }
    }

    // The cursor rows the last boot left, judged against the directory just
    // assembled plus the dormant dynamic registrations it deliberately excludes,
    // and before this boot's attaches touch any of them: a row no registration
    // names is deleted, and one standing above its channel's head is reset to it.
    // Both states can only arise while the process is down, so this is the only
    // place they are asked about.
    let reconciled = messenger
        .reconcile_subscriber_cursors(&dormant_dynamic)
        .await;
    if !reconciled.is_clean() {
        tracing::warn!(
            orphans_removed = reconciled.orphans_removed,
            positions_reset = reconciled.positions_reset,
            "messaging: boot reconciled subscriber cursors"
        );
    }

    // Whether a queue is new is the store's determination, made from its own
    // per-subscriber position.
    let mut primed_any = false;
    for c in &resolved_wasm_consumers {
        let subscriber = brenn_lib::messaging::ParticipantId::for_wasm(&c.slug);
        for inp in &c.inputs {
            // The same spelling the port's window reads at, so the depth the
            // cursor row caches is the one the first read would retune it to.
            let push_depth = brenn_lib::messaging::config::Depth::Bounded(
                inp.sub
                    .push_depth
                    .clamped_to(brenn_messaging::WASM_WINDOW_MAX_NEW),
            );
            let attached = messenger
                .attach_subscriber(&inp.sub.channel_address, &c.slug, &subscriber, push_depth)
                .await;
            primed_any |= attached == brenn_messaging_store::store::Attached::Created;
        }
    }
    // Every push-enabled app subscriber gets its conversation's position before
    // anything can publish, so the position exists by the time the dispatcher
    // starts delivering here.
    messenger.attach_conversation_subscribers().await;
    // Kick so primed consumers drain immediately rather than at the next poll.
    if primed_any {
        messenger.dispatch_kick();
    }

    MessagingResult {
        messenger: Some(messenger),
        router: Some(router),
        wasm_consumers: resolved_wasm_consumers,
        dynamic_mqtt_ingress,
        surfaces: resolved_surfaces,
        remotes: resolved_remotes,
        nondurable_channels,
        system_participants,
    }
}

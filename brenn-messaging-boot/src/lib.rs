//! Build the messaging layer (channel directory, messenger, wake router).
//!
//! This is the boot-time lowering of the messaging configuration: `[[channel]]`,
//! `link`, `[[wasm_consumer]]`, `[[surface]]` and `[[remote]]` blocks,
//! plus the transport endpoints resolved elsewhere, become one `MessagingDirectory`
//! and one live `Messenger` with its wake router. Nothing here serves a request or
//! owns a task: [`build_messaging`] returns a [`MessagingResult`] and the
//! composition root above spawns from it.

use std::sync::Arc;

use brenn_lib::config::{AppConfig, BrennConfig};
use brenn_lib::messaging::config::{
    NoiseLevel, ResolvedMessagingConfig, ResolvedSubscription, ResolvedSurface,
    ResolvedWasmConsumer,
};
use brenn_lib::messaging::remote::{RemoteFacts, ResolvedRemote};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, webhook_channel_uuid_from_slug,
};
use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
use brenn_messaging as messaging;
use brenn_obs::alerting::AlertDispatcher;
use indexmap::IndexMap;

use brenn_server::active_bridge::ActiveBridges;
use brenn_server::messaging_router::WakeRouterImpl;

pub(crate) mod auto;
mod offline;
mod plan;
mod surfaces;
mod wasm;

/// Hold one port name to the charset every port name in the system shares:
/// non-empty, RFC 3986 unreserved characters only.
///
/// The one statement of the rule for this crate's four placements — a
/// consumer's bindings, a consumer's declared vocabulary, a surface's bindings,
/// a surface component's declared vocabulary — because the charset is shared
/// with channel addressing and two placements refusing different configs is a
/// divergence that only shows up on a deploy. Uniqueness is deliberately not
/// here: the binding sites also fold a name into a per-entity seen-set, which
/// the vocabularies have no business sharing.
///
/// `context` is a pre-formatted label naming the block, the placement and the
/// noun (`[[wasm_consumer]] "filter": output port name`), in the same style
/// [`bound_channel`] takes.
///
/// # Panics
///
/// On an empty name and on a name outside the unreserved charset.
fn assert_port_name(context: &str, port: &str) {
    assert!(!port.is_empty(), "{context} must be non-empty");
    assert!(
        port.chars().all(brenn_lib::messaging::is_unreserved_char),
        "{context} {port:?} must consist of RFC 3986 unreserved characters only (A-Za-z0-9._~-)",
    );
}

/// The channel address a static port binding resolves to.
///
/// `declared` is the `channel` the operator wrote on the binding. A binding
/// without one is a *free port*: it declares the port and its tuning, and expects
/// exactly one `link` to supply the channel — `lowered`, the address the
/// auto-wiring pass assigned it. Reaching this function with neither means no
/// link claimed the port — dead config, and a boot panic in the same posture as
/// an output port on a consumer that never activates. Both at once means the port
/// name is claimed twice: by this address and by an auto channel (a link
/// endpoint, or an io_port wearing the same name).
///
/// An operator-written address is also the one place a hand-computed
/// `auto.<cid>` could enter: auto cids are deterministic, so without a check here
/// a third party could bind an "anonymous" channel with no link to show for
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
                    "config: {owner}: {port_label} declares no channel and no link binds it — \
                     a free port must be bound to exactly one link, or carry its own channel \
                     address"
                )
            })
            .to_string();
    };
    assert!(
        lowered.is_none(),
        "config: {owner}: {port_label} binds channel {address:?}, but the port name is also \
         claimed by an auto channel — either a link binds it, or an io_port declares the same \
         name. A port binds exactly one channel: drop this address, or drop the claim.",
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
         address; bind this port to the channel's link, or give that channel a name",
    );
    address.to_string()
}

/// Refuse to start when two channel entries claim one uuid.
///
/// The uuid is the channel's identity for cursors, parked messages, and the DB
/// row. Nothing downstream detects a collision — this is the one check, and it
/// must run over the merged set (declared, transport-derived, and synthesized).
pub(crate) fn assert_unique_channel_uuids<'a>(entries: impl Iterator<Item = &'a ChannelEntry>) {
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

/// Assert that all replay store paths and consumer store paths are unique across
/// both sets.
///
/// `replay_paths`: canonical store paths from `[[webhook_endpoint]].replay_protection`.
/// `consumer_paths`: `(store_path, slug)` pairs from `[[wasm_consumer]]`.
///
/// Panics on the first duplicate with a human-readable message. Runs long
/// before `KvStore::open`, so the message names the config blocks rather than
/// the internal `OPEN_PATHS` guard's generic error.
pub(crate) fn assert_unique_store_paths(
    replay_paths: &[std::path::PathBuf],
    consumer_paths: &[(std::path::PathBuf, String)],
) {
    use std::collections::HashMap;
    let mut seen: HashMap<&std::path::Path, String> = HashMap::new();
    for path in replay_paths {
        if let Some(prior_owner) = seen.insert(path.as_path(), "replay endpoint".to_string()) {
            panic!(
                "bootstrap: store_path {:?} is shared between two replay endpoints \
                 (also owned by {prior_owner}) — each store_path must be unique \
                 across all replay and consumer stores",
                path
            );
        }
    }
    for (path, slug) in consumer_paths {
        let owner_label = format!("[[wasm_consumer]] {slug:?}");
        if let Some(prior_owner) = seen.insert(path.as_path(), owner_label) {
            panic!(
                "bootstrap: store_path {:?} is shared between [[wasm_consumer]] {slug:?} \
                 and {prior_owner} — each store_path must be unique across all \
                 replay and consumer stores",
                path
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
pub use plan::{MessagingPlan, PlanInputs, plan_messaging};
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
/// `link` declarations lowered to.
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

/// Lower `[[channel]]` and `link` into the channel set the resolvers
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
pub(crate) fn lower_channel_topology(
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
        &config.links,
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
pub(crate) fn finish_surface_policies(
    resolved_surfaces: &mut [ResolvedSurface],
    config: &BrennConfig,
) {
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
///
/// `Default` is the shape a document that configures no messaging gets: no
/// messenger, no router, and nothing resolved.
#[derive(Default)]
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

/// Plan values the caller needs that [`commit_messaging`] does not produce.
///
/// Carried beside the [`MessagingResult`] rather than on it: they are the
/// document's, not the running layer's, and a caller that took them off the
/// result would leave the result naming values it no longer holds.
#[derive(Default)]
pub struct PlanCarried {
    /// The distinct `mqtt:<client>:<topic>` ingress channels this document
    /// mints (see [`derive_mqtt_ingress_channels`]).
    pub mqtt_ingress_channels: Vec<ResolvedMqttIngressChannel>,
    /// The advisory, or `None`. Carried rather than logged because the callers
    /// report differently — boot has a `tracing` subscriber, an offline config
    /// check has none.
    pub surface_error_advisory: Option<brenn_surface_server::SurfaceErrorAdvisory>,
    /// The directory exactly as the document lowered to it, detached from the
    /// live one. Must be the planned snapshot, not the live directory, because
    /// the live one is edited after boot (dynamic subscription merge,
    /// attach-minted entries) and quickly diverges from what the document
    /// projects.
    pub planned_directory: MessagingDirectory,
    /// The per-caller tool grant table this document projects — what the
    /// executor's [`ToolCallerGrants`](brenn_tool_registry::ToolCallerGrants)
    /// is installed with, where the executor exists at all.
    pub tool_caller_grants: brenn_tool_registry::CallerGrantTable,
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
pub(crate) fn validate_static_subscriptions_deliverable(
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
pub fn messaging_configured(config: &BrennConfig) -> bool {
    !config.channels.is_empty()
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
        // An endpoint's `webhook:` channel is a channel of this bus, minted
        // whether or not anything else in the document is.
        || !config.webhook_endpoints.is_empty()
}

/// The distinct `mqtt:<client>:<topic>` ingress channels this document mints,
/// deduplicated by `channel_uuid` in mint order: every
/// `[[app.mqtt_subscription]]` first, then every `[[wasm_consumer.subscription]]`
/// naming an `mqtt:` address.
///
/// Both sources must feed the same channel entry, broker SUBSCRIBE, and
/// router route: a filter in one but not the other would leave deliveries
/// unrouted or the directory with an unknown channel.
///
/// No conflict check is needed: `qos`/`urgency` are connection-level on
/// `[[mqtt_client]]`, so the same `(client, topic)` across declarations always
/// carries identical delivery intent.
pub(crate) fn derive_mqtt_ingress_channels(
    config: &BrennConfig,
    mqtt_clients: &IndexMap<String, brenn_lib::mqtt::config::MqttClientIdentity>,
) -> Vec<ResolvedMqttIngressChannel> {
    let mut channels: Vec<ResolvedMqttIngressChannel> = Vec::new();
    let mut seen: std::collections::HashSet<uuid::Uuid> = std::collections::HashSet::new();
    for app in &config.apps {
        for sub in &app.mqtt_subscriptions {
            let owner_desc = format!("app {:?}: [[app.mqtt_subscription]]", app.slug);
            let channel = brenn_lib::mqtt::config::resolve_mqtt_ingress_channel(
                &sub.channel,
                mqtt_clients,
                &owner_desc,
            );
            if seen.insert(channel.channel_uuid) {
                channels.push(channel);
            }
        }
    }
    for consumer in &config.wasm_consumers {
        for sub in &consumer.subscriptions {
            // A free port rides an auto channel, never `mqtt:`.
            let Some(address) = sub.channel.as_deref() else {
                continue;
            };
            if !brenn_lib::mqtt::address::is_mqtt_address(address) {
                continue;
            }
            let owner_desc = format!("[[wasm_consumer]] {:?}", consumer.slug);
            let channel = brenn_lib::mqtt::config::resolve_mqtt_ingress_channel(
                address,
                mqtt_clients,
                &owner_desc,
            );
            if seen.insert(channel.channel_uuid) {
                channels.push(channel);
            }
        }
    }
    channels
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
pub(crate) fn validate_exact_tuning_blocks(
    tuning: &messaging::config::SystemChannelTuning,
    webhook_slugs: &[&str],
    async_tool_names: &[&'static str],
    inbox_slugs: &std::collections::HashSet<String>,
    check_tool_families: bool,
) {
    use brenn_tool_registry::bus_wiring::{TOOL_RESULTS_NAMESPACE, TOOLS_NAMESPACE};

    for address in tuning.exact_addresses() {
        let (found, what) = if let Some(slug) = address.strip_prefix("webhook:") {
            (
                webhook_slugs.contains(&slug),
                "a [[webhook_endpoint]] with that slug",
            )
        } else if let Some(name) = address
            .strip_prefix("brenn:")
            .and_then(|n| n.strip_prefix(TOOLS_NAMESPACE))
        {
            if !check_tool_families {
                continue;
            }
            (
                async_tool_names.contains(&name),
                "a registered async tool with that name",
            )
        } else if let Some(slug) = address
            .strip_prefix("brenn:")
            .and_then(|n| n.strip_prefix(TOOL_RESULTS_NAMESPACE))
        {
            if !check_tool_families {
                continue;
            }
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
    mqtt_clients: &IndexMap<String, brenn_lib::mqtt::config::MqttClientIdentity>,
    tool_registry: &Arc<brenn_tool_registry::ToolRegistry>,
    replay_store_paths: &[std::path::PathBuf],
) -> (MessagingResult, PlanCarried) {
    let plan = plan_messaging(&PlanInputs {
        config,
        apps: Some(apps),
        mqtt_clients,
        tool_registry: Some(tool_registry),
        replay_store_paths,
    });
    let Some(mut plan) = plan else {
        return (MessagingResult::default(), PlanCarried::default());
    };
    // Cloned: commit reads the ingress set too. The advisory is the caller's
    // alone (commit ignores it).
    let carried = PlanCarried {
        mqtt_ingress_channels: plan.mqtt_ingress_channels.clone(),
        surface_error_advisory: plan.surface_error_advisory.take(),
        // Copy-on-write: shares Arc entries with the live directory, no clones.
        planned_directory: MessagingDirectory::from_arcs(plan.directory.list()),
        tool_caller_grants: std::mem::take(&mut plan.tool_caller_grants),
    };
    let result = commit_messaging(
        plan,
        apps,
        db,
        active_bridges,
        alert_dispatcher,
        server_origin,
    )
    .await;
    (result, carried)
}

/// Turn a [`MessagingPlan`] into a running messaging layer: DB rows, bearer
/// tokens, the wake router, the messenger and its installed tables, the boot
/// merge of the durable dynamic subscriptions, the cursor reconcile, and the
/// priming that leaves every consumer's position behind the retained tail.
///
/// This is everything the lowering does that touches something other than the
/// document. Every refusal above it has already been made — a plan in hand is a
/// document that boots — so a failure here is a host bug and panics as one.
///
/// # Panics
///
/// On an `apps` map that is not the one the plan was derived from, on an
/// absent `server_origin` (a bootstrap bug past the plan), on a
/// `[[remote]]` token file that is missing, unreadable, empty or
/// group/world-readable, and on a roster publish or a stored dynamic `mqtt:`
/// row the host cannot make sense of.
pub async fn commit_messaging(
    plan: MessagingPlan,
    apps: &Arc<IndexMap<String, AppConfig>>,
    db: brenn_db::Db,
    active_bridges: ActiveBridges,
    alert_dispatcher: AlertDispatcher,
    server_origin: Option<Arc<str>>,
) -> MessagingResult {
    // The map installed into the messenger, whose publish and delivery gates
    // read it, must be the map the directory's `App` subscriber entries were
    // derived from. Two snapshots of one document are still two, and a
    // directory computed against one with gates consulting the other is an
    // authorization drift nothing downstream can detect.
    assert!(
        plan.was_derived_from(apps),
        "commit_messaging: the apps map is not the one this plan was derived from",
    );
    let MessagingPlan {
        directory,
        nondurable_channels,
        wasm_consumers: resolved_wasm_consumers,
        system_channel_tuning,
        surfaces: resolved_surfaces,
        remotes: remote_facts,
        mqtt_ingress_channels,
        system_participants,
        registrations,
        attach_send_budgets,
        messaging_globals,
        llm_chat,
        // The advisory is the caller's to report; nothing commit does reads it.
        surface_error_advisory: _,
        planned_apps: _,
        // Boot's, taken off the plan above; commit installs no grant table.
        tool_caller_grants: _,
    } = plan;
    let global_defaults = &messaging_globals;
    // The in-memory substrate for the non-durable entries. Built here rather
    // than in the plan: a ring store is state, not a derivation.
    let ring_stores = Arc::new(brenn_messaging_store::store::RingStores::build(
        &nondurable_channels,
    ));

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
    // Gate 6: each remote's bearer token, with the mode-bit check a credential
    // that authenticates a network principal earns. Deferred to here because
    // the file is the one part of a `[[remote]]` that only the deployment target
    // can answer, and the derivation above has to run wherever a document is
    // checked.
    let resolved_remotes: Vec<ResolvedRemote> = remote_facts
        .into_iter()
        .map(RemoteFacts::load_token)
        .collect();

    let mut router_inner = WakeRouterImpl::new(active_bridges);
    router_inner.set_alert_dispatcher(alert_dispatcher);
    let router = Arc::new(router_inner);

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
        messaging_globals.clone(),
    )
    .with_subscriber_registrations(registrations)
    .with_attach_send_budgets(attach_send_budgets)
    .with_ring_stores(ring_stores)
    .with_llm_chat(llm_chat)
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
            &system_channel_tuning,
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

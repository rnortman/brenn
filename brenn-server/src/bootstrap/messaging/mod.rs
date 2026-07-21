//! Build the messaging layer (channel directory, messenger, wake router).

use std::sync::Arc;

use brenn_lib::config::{AppConfig, BrennConfig};
use brenn_lib::messaging;
use brenn_lib::messaging::config::{
    Depth, EphemeralChannelEntry, NoiseLevel, ResolvedMessagingConfig, ResolvedSubscription,
    ResolvedSurface, ResolvedSurfaceSubscription, ResolvedWasmConsumer,
};
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, webhook_channel_uuid_from_slug,
};
use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::webhook::ResolvedWebhookEndpoint;
use indexmap::IndexMap;

use crate::active_bridge::ActiveBridges;
use crate::messaging_router::WakeRouterImpl;

mod surfaces;
mod wasm;

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
mod build_tests;
#[cfg(test)]
mod surface_tests;
#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
mod wasm_tests;

pub(crate) use surfaces::{
    assert_output_bindings_covered, inject_surface_error_grant,
    inject_surface_geometry_status_grants, resolve_surfaces,
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
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: ws.wake_min,
            })
            .collect();

        // MQTT ingress subscriptions are already fully resolved (address parsed,
        // channel UUID derived, generic params resolved via sub → channel →
        // global). Copy them straight across into the shared subscription list so
        // finalize_directory_with_subscribers and rebuild_subscriptions populate
        // channel.subscribers for mqtt: channels exactly as for webhooks. The
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
                // apps_with_messaging and its transport subscriptions reach both
                // rebuild_subscriptions and finalize_directory_with_subscribers.
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

/// Merge webhook-derived messaging configs from `apps_with_messaging` back into
/// the base `apps` map so the result can be handed to `Messenger::new`.
///
/// `Messenger::new` / `resolve_push_targets` reads each app's
/// `.messaging.subscriptions` to find the `ResolvedSubscription` for the
/// channel being published to. Without this merge, webhook-subscribed apps
/// appear in the channel's subscriber list (placed there by
/// `finalize_directory_with_subscribers`) but have no matching subscription in
/// their `.messaging` — causing an invariant panic.
///
/// Apps absent from `apps_with_messaging` (no messaging block, no webhook
/// subscriptions) are passed through unchanged with their original `.messaging`
/// value (which remains `None`).
///
/// Extracted as a standalone function so it can be called and tested
/// independently from `build_messaging` (which is async and DB-dependent).
pub(crate) fn merge_apps_for_messenger(
    apps: &IndexMap<String, brenn_lib::config::AppConfig>,
    apps_with_messaging: &[(String, ResolvedMessagingConfig)],
) -> IndexMap<String, brenn_lib::config::AppConfig> {
    let mut merged = apps.clone();
    for (slug, resolved_msg) in apps_with_messaging {
        merged
            .get_mut(slug)
            .unwrap_or_else(|| {
                panic!(
                    "merge_apps_for_messenger: apps_with_messaging names slug {slug:?} \
                     absent from the apps map (host bug)"
                )
            })
            .messaging = Some(resolved_msg.clone());
    }
    merged
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
pub(crate) struct DynamicMqttIngress {
    pub(crate) channel_address: String,
    pub(crate) channel_uuid: uuid::Uuid,
    pub(crate) client_slug: String,
    pub(crate) topic: String,
    /// The broker SUBSCRIBE QoS stored on the durable row at creation time (it was
    /// defaulted to the client's `[[mqtt_client]].qos` when omitted). MQTT dynamic
    /// rows always carry a `qos`; a missing one is a host bug.
    pub(crate) qos: u8,
}

/// Outcome of building the messaging layer.
pub(crate) struct MessagingResult {
    pub(crate) messenger: Option<Arc<messaging::Messenger>>,
    pub(crate) router: Option<Arc<WakeRouterImpl>>,
    /// Fully resolved WASM consumers, in declaration order.
    /// The caller (`bootstrap/mod.rs`) uses these to load each `ProcessorComponent`,
    /// create a `tokio::sync::Notify` per slug, and register it on the router.
    /// Empty when no `[[wasm_consumer]]` blocks are configured.
    pub(crate) wasm_consumers: Vec<ResolvedWasmConsumer>,
    /// Durable dynamic `mqtt:` subscriptions whose filter has no static ingress
    /// channel — these need their broker SUBSCRIBE + `IngressRoute` rebuilt at boot
    /// (see [`DynamicMqttIngress`]). Empty when no such rows survived the merge.
    pub(crate) dynamic_mqtt_ingress: Vec<DynamicMqttIngress>,
    /// Fully resolved `[[surface]]` blocks, in declaration order.
    /// Boot-cross-validated. Carried for later consumers (the `EphemeralBus` / surface WS
    /// endpoint); the only reader today is the boot-time observability log in
    /// `bootstrap/mod.rs`. Empty when no `[[surface]]` blocks are configured.
    pub(crate) surfaces: Vec<ResolvedSurface>,
    /// Resolved `[[ephemeral_channel]]` directory, in declaration
    /// order. Carried for later consumers' `EphemeralBus`; the only reader today is the boot log.
    /// Empty when no `[[ephemeral_channel]]` blocks are configured.
    pub(crate) ephemeral_channels: Vec<EphemeralChannelEntry>,
    /// Collected system participant specs. The caller registers a parked-notify
    /// delivery binding (and spawns a drain task) for each spec with
    /// subscriptions. Empty when messaging is unconfigured or no system
    /// participant is active.
    pub(crate) system_participants: Vec<brenn_lib::messaging::system::SystemParticipantSpec>,
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
/// authored in TOML and resolved at boot; if its policy does not cover its own
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
    system_participants: &[brenn_lib::messaging::system::SystemParticipantSpec],
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
                // Policy is per-surface at either grain (a component's grants are
                // its config-declared bindings, which the surface's own ACLs
                // cover), so the instance half decorates the label and never the
                // lookup.
                SubscriberEntryKind::Surface { slug, .. } => (
                    "surface",
                    slug.as_str(),
                    surface_policy_by_slug.get(slug.as_str()).copied(),
                ),
                SubscriberEntryKind::System(component) => (
                    "system",
                    component.as_str(),
                    system_policy_by_component.get(component.as_str()).copied(),
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
pub(crate) fn messaging_configured(
    config: &BrennConfig,
    webhook_endpoints: &IndexMap<String, Arc<ResolvedWebhookEndpoint>>,
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
) -> bool {
    !config.channels.is_empty()
        || !webhook_endpoints.is_empty()
        || !mqtt_ingress_channels.is_empty()
        || !config.wasm_consumers.is_empty()
        || !config.surfaces.is_empty()
        || !config.ephemeral_channels.is_empty()
}

/// Build the channel directory, upsert configured channels, rebuild
/// subscriptions, and construct the messenger + wake router.
///
/// Returns `None` values when `messaging_configured` is false (no `[[channel]]`,
/// `[[webhook_endpoint]]`, mqtt-ingress, `[[wasm_consumer]]`, `[[surface]]`, or
/// `[[ephemeral_channel]]` blocks — messaging effectively disabled, no DB rows
/// touched).
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
pub(crate) async fn build_messaging(
    config: &BrennConfig,
    db: brenn_lib::db::Db,
    apps: &Arc<IndexMap<String, AppConfig>>,
    active_bridges: ActiveBridges,
    alert_dispatcher: AlertDispatcher,
    server_origin: Option<Arc<str>>,
    webhook_endpoints: &IndexMap<String, Arc<ResolvedWebhookEndpoint>>,
    mqtt_ingress_channels: &[ResolvedMqttIngressChannel],
    resolved_mqtt_clients: &IndexMap<String, brenn_lib::mqtt::config::MqttClientConfig>,
    tool_registry: &Arc<crate::tool_registry::ToolRegistry>,
) -> MessagingResult {
    if !messaging_configured(config, webhook_endpoints, mqtt_ingress_channels) {
        return MessagingResult {
            messenger: None,
            router: None,
            wasm_consumers: vec![],
            dynamic_mqtt_ingress: vec![],
            surfaces: vec![],
            ephemeral_channels: vec![],
            system_participants: vec![],
        };
    }

    // --- Derive webhook channel entries from [[webhook_endpoint]] definitions ---
    //
    // Each endpoint produces one `webhook:` ChannelEntry with:
    //   - UUID derived deterministically from the slug (stable across restarts)
    //   - transport_type = Webhook
    //   - ResolvedChannel inheriting global messaging defaults (Unbounded depths,
    //     Silent noise, Drop sink) — guarantees retained until consumed
    //   - mount carried so list_channels() has a single source
    let global_defaults = &config.messaging;
    let webhook_channel_entries: Vec<ChannelEntry> = webhook_endpoints
        .values()
        .map(|ep| ChannelEntry {
            uuid: webhook_channel_uuid_from_slug(&ep.slug),
            address: format!("webhook:{}", ep.slug),
            description: ep.description.clone(),
            resolved_channel: messaging::config::ResolvedChannel {
                push_depth: global_defaults.default_push_depth,
                retain_depth: global_defaults.default_retain_depth,
                standing_retain_depth: global_defaults.default_standing_retain_depth,
                noise: global_defaults.default_noise,
                sink: global_defaults.default_sink,
                wake_min: global_defaults.default_wake_min,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Webhook,
            mount: Some(ep.mount.clone()),
        })
        .collect();

    // --- Derive mqtt channel entries from the distinct ingress channels ---
    //
    // Mirrors the webhook channel-entry loop: each distinct ingress channel
    // produces one `mqtt:<client>:<topic>` ChannelEntry with:
    //   - UUID = the resolved-address derivation (stable across restarts,
    //     distinct UUIDv5 namespace from webhook so address spaces never collide)
    //   - transport_type = Mqtt
    //   - ResolvedChannel inheriting global messaging defaults
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
            resolved_channel: messaging::config::ResolvedChannel {
                push_depth: global_defaults.default_push_depth,
                retain_depth: global_defaults.default_retain_depth,
                standing_retain_depth: global_defaults.default_standing_retain_depth,
                noise: global_defaults.default_noise,
                sink: global_defaults.default_sink,
                wake_min: global_defaults.default_wake_min,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Mqtt,
            mount: None,
        })
        .collect();

    // --- Build apps_with_messaging, merging webhook subscriptions ---
    let apps_with_messaging = build_apps_with_messaging(apps, global_defaults);

    // Build channel entries: brenn: channels first, then webhook:, then mqtt:.
    let mut all_entries =
        messaging::config::build_channel_entries(&config.channels, global_defaults);
    all_entries.extend(webhook_channel_entries);
    all_entries.extend(mqtt_channel_entries);

    // Resolve WASM consumer subscriptions against the built entries before
    // finalizing the directory (the directory itself is built from these same
    // entries; we resolve first so the wasm_consumers vec is ready for
    // finalize_directory_with_subscribers).
    //
    // Build a temporary MessagingDirectory from the raw entries for lookups.
    // `finalize_directory_with_subscribers` then re-uses `all_entries` to build
    // the final directory with subscribers populated.
    let pre_directory = MessagingDirectory::with_entries(all_entries.clone());
    let mut resolved_wasm_consumers = resolve_wasm_consumers(
        &config.wasm_consumers,
        &pre_directory,
        &config.wasm.store_size_limit,
        resolved_mqtt_clients,
    );
    // Strip to slug+subs for `finalize_directory_with_subscribers` (directory only needs these).
    let mut wasm_consumers_for_dir: Vec<(String, Vec<ResolvedSubscription>)> =
        resolved_wasm_consumers
            .iter()
            .map(|c| {
                let subs = c.inputs.iter().map(|inp| inp.sub.clone()).collect();
                (c.slug.clone(), subs)
            })
            .collect();

    // Resolve `[[ephemeral_channel]]` + `[[surface]]` blocks *before* finalizing
    // the directory: a `brenn:` surface subscription resolves to a
    // `SubscriberEntryKind::Surface` directory entry, so its durable subscriptions
    // must be ready for `finalize_directory_with_subscribers`. `resolve_surfaces`
    // cross-validates every binding against `pre_directory` (the same channel set
    // the final directory is built from — subscribers not yet populated, but
    // resolution needs only channel identity/transport) and the ephemeral-channel
    // set (fail-fast on any dead / mis-scheme / policy-uncovered binding), exactly
    // as `resolve_wasm_consumers` does above.
    let ephemeral_channels = messaging::config::build_ephemeral_channel_entries(
        &config.ephemeral_channels,
        global_defaults,
    );
    let mut resolved_surfaces = resolve_surfaces(
        &config.surfaces,
        &pre_directory,
        &ephemeral_channels,
        global_defaults,
    );

    // Substrate error-reporting grant: scheme-strip the configured error channel
    // once and inject a `MessagingPublish` grant + exact `brenn_publish` ACL onto
    // every surface, before the policies fan out to the registry and validators.
    // The bare name is reused for the relay spec below.
    let error_channel_bare = config
        .observability
        .surface_error_channel
        .as_deref()
        .map(|addr| system_publisher_bare_channel("[observability] surface_error_channel", addr));
    if let Some(bare) = &error_channel_bare {
        inject_surface_error_grant(&mut resolved_surfaces, bare);
    }

    // Substrate surface self-description grant: inject each surface's own
    // geometry/status `brenn_publish` coverage before the policies fan out to the
    // runtimes, the registry, and the single-writer sweep (which excludes exactly
    // this owning-surface coverage).
    inject_surface_geometry_status_grants(
        &mut resolved_surfaces,
        &config.surface_description.prefix,
    );

    // Item-6 output publish-coverage, asserted after the substrate error-report
    // grant is injected: an output bound to the configured error channel is
    // covered by that grant (the sanctioned many-writer shape), while any other
    // uncovered output is still dead config and fails fast here.
    assert_output_bindings_covered(&resolved_surfaces);

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
        .map(|s| (s.slug.clone(), s.durable_subscriptions.clone()))
        .collect();

    // --- Async tool substrate: request channels, result inboxes, derived grants ---
    //
    // One `brenn:tools/<tool>` request channel per registered async tool (the
    // executor subscribes to each as `system:tool-executor`); for each wasm
    // consumer holding ≥1 async tool grant, one `brenn:tool-results/<slug>` inbox
    // plus the derived async bus grants. The channels ride the same
    // finalize/upsert/rebuild path as every other channel; the inbox subscription
    // is folded through `wasm_consumers_for_dir` so it is written to both the
    // directory and `messaging_subscriptions` like a configured wasm subscription,
    // and as a triggering `WasmInputPort` on the consumer's `inputs` so a delivered
    // result activates the consumer (and survives the drain's residue
    // reconciliation, which retires rows for channels absent from `inputs`).
    // The System request-channel subscriber is programmatic (directory-only, no
    // `messaging_subscriptions` row); it is folded in from the executor's
    // `SystemParticipantSpec` below and validated by the deliverability check
    // like every other static subscriber.
    let async_tool_names = tool_registry.async_tool_names();
    for tool in &async_tool_names {
        all_entries.push(crate::tool_registry::bus_wiring::request_channel_entry(
            tool,
            global_defaults,
        ));
    }
    // TODO(tool-registry-unregistered-tool-sweep): once tools can be
    // dynamically (de)registered, sweep `brenn:tools/*` pending rows here for
    // tools no longer in the registry — alert and delete them at boot rather
    // than executing a request against a removed tool. Unreachable today: the
    // async tool set is fixed in code, so a pending row can only name a
    // registered tool.
    for consumer in resolved_wasm_consumers.iter_mut() {
        let async_tools =
            crate::tool_registry::bus_wiring::consumer_async_tools(tool_registry, &consumer.policy);
        if async_tools.is_empty() {
            continue;
        }
        crate::tool_registry::bus_wiring::derive_async_tool_bus_grants(
            &mut consumer.policy,
            &consumer.slug,
            &async_tools,
        );
        all_entries.push(crate::tool_registry::bus_wiring::result_inbox_entry(
            &consumer.slug,
            global_defaults,
        ));
        let inbox_sub = crate::tool_registry::bus_wiring::inbox_subscription(&consumer.slug);
        let dir_entry = wasm_consumers_for_dir
            .iter_mut()
            .find(|(slug, _)| slug == &consumer.slug)
            .expect("wasm_consumers_for_dir has an entry per resolved consumer");
        dir_entry.1.push(inbox_sub);
        consumer
            .inputs
            .push(crate::tool_registry::bus_wiring::inbox_input_port(
                &consumer.slug,
            ));
    }
    // System participant specs: every `system:` principal is declared here and
    // everything it needs — its subscriber registration, its (subscriber)
    // directory entries, deliverability validation, and (for subscribers) a
    // parked-notify delivery binding — is derived from the one declaration.
    //   - the tool executor, present whenever any async tool is registered
    //     (subscriber; it subscribes to every `brenn:tools/<tool>` request channel);
    //   - `system:surface-help`, publish-only — granted an exact-match publish ACL
    //     on every derived boot-published help/schema/index channel.
    // A publish-only spec carries no subscriptions, so it gets a registry entry
    // (publish authority) but no directory subscriber entry and no delivery
    // binding — it is never a dispatch target. The surface error channel has no
    // system participant: surfaces publish reports onto it under their own
    // `surface:<slug>` identities (a boot-injected substrate grant).
    let mut system_participants: Vec<brenn_lib::messaging::system::SystemParticipantSpec> =
        Vec::new();
    if !async_tool_names.is_empty() {
        system_participants.push(crate::tool_registry::bus_wiring::tool_executor_spec(
            &async_tool_names,
        ));
    }
    let boot_published_bares = crate::routes::surface::description::boot_published_bare_channels(
        &config.surface_description.prefix,
        &resolved_surfaces,
    );
    system_participants.push(crate::routes::surface::description::surface_help_spec(
        &boot_published_bares,
    ));
    brenn_lib::messaging::system::fold_spec_subscriptions(&mut all_entries, &system_participants);

    let directory = Arc::new(messaging::config::finalize_directory_with_subscribers(
        all_entries,
        &apps_with_messaging,
        &wasm_consumers_for_dir,
        &surfaces_for_dir,
    ));

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

    // Sync DB state with config: upsert channels, rebuild subscriptions, then
    // fold the durable dynamic subscriptions back into the directory.
    //
    // `dynamic_mqtt_ingress` collects the surviving dynamic `mqtt:` subscriptions
    // whose filter has no static ingress channel, so the caller can rebuild their
    // broker SUBSCRIBE + `IngressRoute` (boot re-activation gap — see
    // `DynamicMqttIngress`).
    let mut dynamic_mqtt_ingress: Vec<DynamicMqttIngress> = Vec::new();
    {
        let conn = db.lock().await;
        let entries: Vec<messaging::ChannelEntry> =
            directory.list().iter().map(|e| (**e).clone()).collect();
        messaging::db::upsert_channels(&conn, &entries);
        messaging::db::rebuild_subscriptions(
            &conn,
            &apps_with_messaging,
            &wasm_consumers_for_dir,
            &surfaces_for_dir,
        );
        // Boot merge: the directory now holds the static + WASM
        // subscribers, so collision detection against static subs is accurate.
        // Re-fold the durable dynamic rows (the table boot never truncates) onto
        // their channels; rows whose channel is gone or that collide with a
        // static sub are dropped with a warn.
        let dynamic_rows = messaging::db::load_dynamic_subscriptions(&conn);
        // Reconstruct runtime-created channels into the boot directory. The
        // directory above was built purely from config, so a channel
        // that exists *only* in `messaging_channels` because a runtime dynamic
        // subscribe created it (the common `mqtt:` case) is absent — and the merge
        // below would then classify its surviving durable row as a vanished channel
        // and prune it, erasing a subscription that was meant to persist. Collect
        // the distinct `channel_uuid`s referenced by the surviving durable rows and
        // load *only* those channels (scoped, never a full-table load — orphan
        // channels are never referenced and so never materialized). Fold each loaded
        // channel that is not already in the directory (config channels are
        // authoritative and stay as-is); the merge then resolves its row by_uuid and
        // keeps it. A referenced UUID absent from `messaging_channels` is left out,
        // so its row classifies as genuine config drift (`dropped`) — unchanged.
        let referenced_uuids: Vec<uuid::Uuid> = {
            let mut seen: std::collections::HashSet<uuid::Uuid> = std::collections::HashSet::new();
            dynamic_rows
                .iter()
                .filter(|row| seen.insert(row.channel_uuid))
                .map(|row| row.channel_uuid)
                .collect()
        };
        for channel in
            messaging::db::load_channels_by_uuids(&conn, &referenced_uuids, global_defaults)
        {
            if directory.by_uuid(&channel.uuid).is_none() {
                directory.add_channel(channel);
            }
        }
        // Boot-time delivery ACL gate: the merge re-authorizes each
        // folded dynamic row against the app's *current* resolved policy. Dynamic
        // rows only ever fold an `App(slug)` subscriber, so the policy view is the
        // per-app `AppPolicy` off the resolved `apps` map (no WASM lookup needed
        // here). A revoked-ACL (or missing-policy) row is classified `revoked` —
        // neither folded nor pruned — so it lies dormant until the ACL returns.
        let merge_outcome =
            messaging::config::merge_dynamic_subscriptions(&directory, &dynamic_rows, &|slug| {
                apps.get(slug).map(|a| &a.policy)
            });
        // Mirror the surviving dynamic rows into messaging_subscriptions (so the
        // urgency-recompute join in bus.rs sees dynamic subscribers) and prune
        // the dropped rows from the durable table (so the conflict does not recur
        // next boot). Both run under the same DB lock as the rebuild above. The
        // `revoked` rows are intentionally left untouched in the durable table.
        messaging::db::mirror_dynamic_subscriptions(&conn, &merge_outcome.kept);
        messaging::db::prune_dropped_dynamic_subscriptions(&conn, &merge_outcome.dropped);
        for row in &merge_outcome.revoked {
            // Surface the channel address, not just the UUID: a
            // revoked row's channel was reconstructed before the merge (revoked
            // rows are durable rows, so their UUID is in `referenced_uuids` and
            // folded above), so `by_uuid` resolves it. Logging the address lets an
            // on-call engineer see which channel's ACL was revoked without a DB
            // lookup, and distinguishes the expected dormant state (channel
            // present, ACL gone) from a secondary bug (channel absent → "<channel
            // not in directory>", which would indicate a boot-step reordering
            // regression).
            let channel_address = directory
                .by_uuid(&row.channel_uuid)
                .map(|c| c.address.clone())
                .unwrap_or_else(|| "<channel not in directory>".to_string());
            tracing::warn!(
                channel_uuid = %row.channel_uuid,
                channel = %channel_address,
                app = %row.app_slug,
                "build_messaging: dynamic subscription retained but dormant — no \
                 longer authorized by the current config (ACL revoked or retain \
                 depth exceeds the channel's standing depth); durable row left in \
                 place, not re-activated",
            );
        }

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

    // Build a merged apps map where each app's `.messaging` reflects the same
    // merged ResolvedMessagingConfig that `build_apps_with_messaging` produced
    // (including webhook-derived ResolvedSubscriptions). Without this,
    // `resolve_push_targets` reads the original per-app `.messaging` and finds
    // no subscription for the webhook channel — invariant panic.
    // The surface-description publisher is a `system:` participant (built into
    // `system_participants` above), not an injected app, so the merged app map is
    // exactly the operator apps with their webhook-derived subscriptions.
    let merged_apps = Arc::new(merge_apps_for_messenger(apps, &apps_with_messaging));

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
    // One registration per surface *principal* (`ResolvedSurface::principals`).
    // All carry the surface's own policy — authority is per-surface (a
    // component's grants are its config-declared bindings, which boot proved the
    // surface's ACLs cover), so the instance grain buys per-principal delivery
    // gating and lag tracking, not a separate ACL blob.
    //
    // Every instance is registered, not just the ones with a durable binding
    // today: `floor_decision` fails closed on a missing registration, so deriving
    // this set from the bindings would silently deny delivery the moment a
    // binding is added anywhere else. The declaration set is the authority.
    for s in &resolved_surfaces {
        let registration = brenn_lib::messaging::SubscriberRegistration {
            policy: Arc::new(s.policy.clone()),
            wake: brenn_lib::messaging::WakeEconomics::Eager,
        };
        for instance in s.principals() {
            subscriber_registrations.insert(
                brenn_lib::messaging::SubscriberEntryKind::Surface {
                    slug: s.slug.clone(),
                    instance,
                },
                registration.clone(),
            );
        }
    }
    subscriber_registrations.extend(brenn_lib::messaging::system::registrations_from_specs(
        &system_participants,
    ));
    // Build the config-resolved ephemeral bus and attach it to the Messenger,
    // replacing the empty default installed by `Messenger::new`.
    let ephemeral_bus = messaging::EphemeralBus::new(
        ephemeral_channels.clone(),
        source.clone(),
        config.messaging.max_body_bytes,
    );
    let messenger = messaging::Messenger::new(
        db,
        directory,
        source,
        merged_apps,
        router.clone() as Arc<dyn messaging::WakeRouter>,
        config.messaging.clone(),
    )
    .with_subscriber_registrations(subscriber_registrations)
    // One budget per surface principal: the surface's own kernel identity plus
    // each component instance it declares, each with its resolved parameters (a
    // declared override, or the defaults). Both come from
    // `ResolvedSurface::principal_send_budgets` — built on the same declaration
    // set the sub-identity derivation admits an instance against, so the budget
    // map and the derivation cannot disagree about which principals exist. A
    // publish whose principal has no bucket is a boot invariant the publish gate
    // panics on.
    .with_surface_send_budgets(
        resolved_surfaces
            .iter()
            .map(|s| (s.slug.clone(), s.principal_send_budgets().collect())),
    )
    .with_ephemeral_bus(ephemeral_bus);

    MessagingResult {
        messenger: Some(messenger),
        router: Some(router),
        wasm_consumers: resolved_wasm_consumers,
        dynamic_mqtt_ingress,
        surfaces: resolved_surfaces,
        ephemeral_channels,
        system_participants,
    }
}

//! Surface WS endpoint: the browser-facing projection of the message bus.
//!
//! `surface_ws_handler` fronts `GET /surface/{slug}/ws`; `session.rs` owns the
//! per-connection task; `registry.rs` tracks attached WS sessions per surface.

pub mod cursor;
pub mod description;
pub mod page;
pub mod processor_assets;
pub mod registry;
pub mod session;
pub mod telemetry;

#[cfg(test)]
mod client_tests;
#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
mod ws_tests;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::Extension;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use brenn_lib::access::AppPolicy;
use brenn_lib::auth::session::Session;
pub use brenn_lib::messaging::DeliveryClass;
use brenn_lib::messaging::config::{
    ResolvedSubscription, ResolvedSurface, ResolvedWasmConsumer, SurfaceBinding, SurfaceOutput,
};
use brenn_lib::messaging::gates::well_formed_name;
use brenn_lib::messaging::system::SystemParticipantSpec;
use brenn_lib::messaging::{
    ChannelScheme, EphemeralBus, MessagingDirectory, Messenger, ParticipantId, Urgency,
};
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_surface_contract::{
    ERROR_REPORT_INSTANCE, ERROR_REPORT_PORT, KERNEL_ARTIFACT, module_artifact,
};
use brenn_surface_proto::{
    Binding, ComponentEntry, LocalChannel, LogLevel, NoiseLevel as WireNoiseLevel, OutputBinding,
    SurfaceBindings, is_local_channel, max_client_frame_bytes, surface_delivery_class,
};
use chrono::Utc;
use tracing::warn;
use uuid::Uuid;

use self::registry::{DURABLE_QUEUE_FRAMES, RegisterRejection, SessionCaps, SurfaceSessionHandle};
use self::session::{SurfaceSessionParams, run_surface_session};
use crate::client_ip::ClientIp;
use crate::routes::ws::close_with_stale_client;
use crate::state::AppState;

/// Maximum concurrent attached WS sessions per surface, across all users.
///
/// Each attached session costs a broadcast receiver per subscription plus an
/// outbound queue, so an unbounded attach count is an authenticated-user memory
/// DoS. Exceeding this is answered with `503` (not a security event: a user with
/// many tabs is not fail2ban signal). The sibling
/// `MAX_SESSIONS_PER_USER_PER_SURFACE` bounds how much of this any one account
/// can hold. Config exposure is an additive change later.
pub const MAX_SESSIONS_PER_SURFACE: usize = 64;

/// Maximum concurrent attached WS sessions per (surface, user). Bounds how
/// much of a shared surface one account can pin: without it, one user's 64
/// healthy sockets deny attach to every other allowed user, and the
/// write-progress watchdog never reaps healthy connections. 16 is ~4x any
/// plausible honest single-account footprint (phone + tablet + several
/// desktops + tabs) while capping one account at 1/4 of a surface. Config
/// exposure is an additive change later (same posture as the shared cap).
pub const MAX_SESSIONS_PER_USER_PER_SURFACE: usize = 16;

// per_user > per_surface would make the per-user cap unreachable (the shared
// check trips at per_surface before any single account's count can reach
// per_user) and signal a botched edit; fail the build.
const _: () = assert!(MAX_SESSIONS_PER_USER_PER_SURFACE <= MAX_SESSIONS_PER_SURFACE);

/// Idle-heartbeat interval advertised in `Welcome`, in seconds. Constant in
/// production; test states set 1 for fast integration tests. Carried on
/// `AppState::surface_heartbeat_secs` solely for that test seam.
pub const HEARTBEAT_SECS: u32 = 20;

/// Compiled-in skin registry: skin name → static stylesheet path (served under
/// `/static/`, build-ID-stamped by the page handler).
///
/// A surface's configured `skin` is boot-validated against these keys; the page
/// handler emits a `<link>` to the matched path and stamps `data-skin` on the
/// surface root. Out-of-tree / file-based skin packs are a later extension of
/// this registry, not in this cut.
pub(crate) const SKIN_REGISTRY: &[(&str, &str)] = &[
    ("bench", "skins/bench.css"),
    ("foundry", "skins/foundry.css"),
];

/// Skin a surface wears when it omits `skin`.
pub(crate) const DEFAULT_SKIN: &str = "bench";

/// Resolve a skin name to its static stylesheet path, or `None` if the name is
/// not in [`SKIN_REGISTRY`].
pub(crate) fn skin_stylesheet_path(name: &str) -> Option<&'static str> {
    SKIN_REGISTRY
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, path)| *path)
}

/// Render a client-supplied string for inclusion in a security-event detail.
///
/// Truncates to a short prefix and control-character-escapes the result, so a
/// hostile client cannot inject unbounded length or raw newline/escape bytes
/// into the security log line or the phone-alert body.
pub(crate) fn sanitize_client_detail(s: &str) -> String {
    const MAX_CHARS: usize = 128;
    let mut rendered: String = s
        .chars()
        .take(MAX_CHARS)
        .flat_map(char::escape_debug)
        .collect();
    if s.chars().nth(MAX_CHARS).is_some() {
        rendered.push_str("...");
    }
    rendered
}

/// Shared pre-serve authorization for the surface page and WS handlers: resolve
/// the slug and enforce the access check, emitting the same fail2ban security
/// events from both entry points. `is_ws` selects the endpoint-specific detail
/// strings only. Unknown slug → 404 + `UnrecognizedUrl` (probe signal, slug
/// sanitized); denied user → 403 + `AuthFailure`.
pub(crate) fn authorize_surface(
    state: &AppState,
    slug: &str,
    username: &str,
    ip: std::net::IpAddr,
    is_ws: bool,
) -> Result<Arc<SurfaceRuntime>, StatusCode> {
    let Some(runtime) = state.surfaces.get(slug).cloned() else {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::UnrecognizedUrl,
            ip,
            &format!(
                "/surface/{}{}",
                sanitize_client_detail(slug),
                if is_ws { "/ws" } else { "" }
            ),
        );
        return Err(StatusCode::NOT_FOUND);
    };

    if !runtime.resolved.user_has_access(username) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::AuthFailure,
            ip,
            &format!(
                "user {} denied {}access to surface {}",
                username,
                if is_ws { "WS " } else { "" },
                slug
            ),
        );
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(runtime)
}

/// Per-surface runtime bundle, precomputed once at boot so the WS hot path does
/// no re-derivation.
///
/// Holds a bus clone (rather than reaching through `state.messenger`) so the
/// bus-present invariant is encoded once at boot and surface WS tests can build
/// a runtime over a bare `EphemeralBus` without assembling a full `Messenger`.
pub struct SurfaceRuntime {
    /// The resolved config block for this surface.
    pub resolved: ResolvedSurface,
    /// `surface:<slug>` participant identity used for bus publishes/subscribes.
    pub participant: ParticipantId,
    /// Resolved access policy, `Arc`-wrapped once for cheap per-op cloning.
    pub policy: Arc<AppPolicy>,
    /// The `EphemeralBus` this surface's ephemeral channels flow through.
    pub bus: Arc<EphemeralBus>,
    /// The `Messenger` durable (`brenn:`) subscriptions project through — the
    /// session reaches the directory, DB, and durable queries via it. `Some`
    /// whenever this surface has any durable subscription (boot invariant);
    /// `None` only for ephemeral-only test runtimes with no `Messenger`.
    pub messenger: Option<Arc<Messenger>>,
    /// Subscriptions this surface declares: `(instance, channel)` → the facts
    /// delivery turns on. The gate an inbound `Subscribe` is validated against,
    /// keyed at the subscription's own grain — so a client naming a channel some
    /// *other* instance binds is the same unbound violation as naming a channel
    /// nobody binds.
    pub subscription_channels: HashMap<SubKey, SubscriptionFacts>,
    /// Output ports: `(instance, port)` → the port's resolved dispatch facts.
    pub output_ports: HashMap<(String, String), OutputPort>,
    /// Prebuilt `Welcome.bindings` payload.
    pub bindings: SurfaceBindings,
    /// Server publish-body cap (config `messaging.max_body_bytes`): the
    /// `Welcome` field, the dispatch pre-check, and the derived WS read cap.
    pub max_body_bytes: usize,
    /// Publish floor for surface error reports, advertised in `Welcome` so the
    /// kernel knows the reserved `#brenn`/`error-reports` output port is live and
    /// at what level to start publishing. `Some` when `surface_error_channel` is
    /// configured (the reserved port is bound in `output_ports`); `None`
    /// otherwise (kernel console-only). Set by [`build_surface_runtimes`] alongside
    /// the reserved-port binding, not by [`SurfaceRuntime::build`].
    pub error_report_floor: Option<LogLevel>,
    /// Surface self-description runtime telemetry. Carries the status heartbeat
    /// interval (advertised in `Welcome`) and the surface's derived
    /// geometry/status channel addresses (the platform-telemetry publish
    /// targets). Every surface has one.
    pub description: SurfaceDescriptionRuntime,
}

/// The operator's `[surface_description]` parameters, as [`SurfaceRuntime::build`]
/// consumes them: the namespace the derived channel addresses hang off and the
/// heartbeat cadence handed to the kernel.
#[derive(Debug, Clone)]
pub struct SurfaceDescriptionParams {
    /// Bare-name namespace rooting every derived channel address.
    pub prefix: String,
    /// Status heartbeat cadence, seconds.
    pub status_interval_secs: u32,
}

/// Per-surface runtime parameters for the surface self-description telemetry.
/// Derived once at boot from [`SurfaceDescriptionParams`] and the surface slug;
/// the addresses are the platform-telemetry publish targets and the interval is
/// the `Welcome` heartbeat advertisement.
pub struct SurfaceDescriptionRuntime {
    /// Status heartbeat cadence handed to the kernel in `Welcome`.
    pub status_interval_secs: u32,
    /// `brenn:<prefix>.surface.<slug>.geometry` — the geometry publish target.
    pub geometry_channel: String,
    /// `brenn:<prefix>.surface.<slug>.status` — the status publish target.
    pub status_channel: String,
    /// Configured instance → kind, precomputed once at boot (a `Status` frame
    /// validates its reported instances against this; it is boot-constant, so it is
    /// not rebuilt per frame).
    pub configured_kinds: HashMap<String, String>,
    /// Configured instance → number of subscription bindings it should have an
    /// attached pump for, precomputed once at boot for health derivation. Every
    /// configured instance is a key (a zero means no bound subscription), so health
    /// derivation can require every configured instance to be reported mounted.
    pub expected_pumps: HashMap<String, u32>,
}

/// What one declared subscription is, at the grain the session delivers it:
/// which class routes it, and whether it has a push window at all.
///
/// `push_enabled` is the **fold** over the subscription's bindings — one
/// instance may bind one channel on two ports, and they share one subscription,
/// so the subscription pushes if any of them does (the max fold the boot
/// resolver and `reap_frontier` already take over depth). A subscription whose
/// fold is 0 is a *context feed*: its rows still reach the page — they are the
/// page's retained-ring diet, and `retain_depth` bounds page memory, not the
/// wire — but no push window exists behind them, so there is no overflow for
/// `Deliver.dropped` to account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionFacts {
    /// Delivery class, derived from the channel address's scheme.
    pub class: DeliveryClass,
    /// Whether any binding on this subscription resolves to `push_depth >= 1`.
    pub push_enabled: bool,
}

/// Map one resolved config binding to its wire form for the `Welcome` payload.
fn wire_binding(b: &SurfaceBinding) -> Binding {
    Binding {
        channel: b.channel_address.clone(),
        instance: b.instance.clone(),
        port: b.port.clone(),
        push_depth: b.push_depth,
        retain_depth: b.retain_depth,
        noise: wire_noise(b.noise),
    }
}

/// Map a resolved `brenn-lib` [`NoiseLevel`] to its wire form. Exhaustive: a new
/// rung that fails to map is a compile error, never a runtime fallback.
fn wire_noise(n: brenn_lib::messaging::config::NoiseLevel) -> WireNoiseLevel {
    use brenn_lib::messaging::config::NoiseLevel as N;
    match n {
        N::Silent => WireNoiseLevel::Silent,
        N::Metered => WireNoiseLevel::Metered,
        N::Alarm => WireNoiseLevel::Alarm,
        N::Fatal => WireNoiseLevel::Fatal,
    }
}

/// The identity of one durable subscription on a surface session: the principal
/// that owns it and the channel it covers.
///
/// This is the grain the whole subscription is cut at — its own push window, its
/// own resume cursor, its own lag — so it is what the session's active set, the
/// registry's shared set, and the router's fan-out filter all key on. Two
/// instances bound to one channel hold two of these and each is delivered its
/// own copy, exactly as two backend `[[app]]`s on one channel are.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubKey {
    /// The owning component instance. Every surface subscription is an
    /// instance's; the bare `surface:<slug>` grain is publisher-only and holds
    /// no durable subscription.
    pub instance: String,
    /// Full scheme-qualified channel address.
    pub channel: String,
}

impl SubKey {
    /// The subscriber identity this subscription's push rows are targeted at —
    /// the principal, at the grain it subscribed.
    pub fn participant(&self, slug: &str) -> ParticipantId {
        ParticipantId::for_surface_component(slug, &self.instance)
    }
}

/// Everything the publish path needs about one bound output port, resolved at
/// boot: where the message goes, which class routes it, and the urgency to send
/// it at when the component states none.
///
/// A named struct rather than a tuple: the address is a `String` and the class
/// and urgency are both small enums, so a transposed tuple field would
/// typecheck — the same argument `PublishRequest` already makes about
/// `instance`/`subject_instance`.
#[derive(Debug, Clone)]
pub struct OutputPort {
    /// Full scheme-qualified channel address the port publishes onto.
    pub address: String,
    /// Delivery class, derived from the address's scheme.
    pub class: DeliveryClass,
    /// The port's configured default urgency. Applies when the client's
    /// `Publish` frame carries no override; the client's override wins.
    ///
    /// Held server-side rather than trusted from the frame: it is operator
    /// config, and the boot-resolved value is the authoritative one even when a
    /// client's `Welcome` snapshot has gone stale under a reconnect.
    pub default_urgency: Urgency,
}

/// The `Welcome` wire form of a resolved output binding. Separate from
/// [`wire_binding`] because an output advertises its resolved default urgency —
/// the page needs it to stamp page-local envelopes, whose router never consults
/// the server.
fn wire_output(b: &SurfaceOutput) -> OutputBinding {
    OutputBinding {
        channel: b.channel_address.clone(),
        instance: b.instance.clone(),
        port: b.port.clone(),
        urgency: b.default_urgency,
        fill_mt: b.budget.fill_mt,
        capacity_mt: b.budget.capacity_mt,
    }
}

/// Classify a surface-bound channel address into its delivery class via the
/// shared `surface_delivery_class` derivation.
///
/// Panics on any scheme not surface-bindable (anything but `brenn:`/
/// `ephemeral:`/`local:`): `resolve_surfaces` already restricted surface
/// bindings to exactly those three, so any other prefix here is a broken boot
/// invariant, not attacker-reachable.
fn classify(address: &str) -> DeliveryClass {
    surface_delivery_class(address).unwrap_or_else(|| {
        panic!(
            "surface binding address {address:?} is not a surface-bindable scheme (brenn:, \
             ephemeral:, or local:) — resolve_surfaces should have rejected it at boot"
        )
    })
}

impl SurfaceRuntime {
    /// Resolve a durable channel's subscription (channel uuid + retain clamp).
    /// Boot classified the channel `Durable`, so a miss is a broken boot
    /// invariant, not attacker-reachable — panic (fail-fast) rather than skip.
    pub(crate) fn durable_subscription(&self, sub: &SubKey) -> &ResolvedSubscription {
        self.resolved
            .durable_subscriptions
            .iter()
            .find(|s| s.instance == sub.instance && s.subscription.channel_address == sub.channel)
            .map(|s| &s.subscription)
            .unwrap_or_else(|| {
                panic!(
                    "surface {}: durable subscription {} (instance {:?}) classified Durable but \
                     absent from resolved durable_subscriptions — boot invariant violated",
                    self.resolved.slug, sub.channel, sub.instance
                )
            })
    }

    /// Whether this subscription has a push window — i.e. whether an overflow
    /// behind it is expressible at all.
    ///
    /// A context feed (fold-max `push_depth = 0`) has none: its rows flow, but no
    /// window exists to overflow, so nothing may be reported as dropped on it.
    ///
    /// # Panics
    ///
    /// On a subscription this surface does not declare. Every caller holds a
    /// `SubKey` that came out of this same map (an inbound `Subscribe` naming
    /// anything else was killed as a violation before a stream existed), so a
    /// miss here is a broken invariant, not client input.
    pub(crate) fn push_enabled(&self, sub: &SubKey) -> bool {
        self.subscription_channels
            .get(sub)
            .unwrap_or_else(|| {
                panic!(
                    "broken invariant: surface {} delivered on undeclared subscription {} \
                     (instance {:?})",
                    self.resolved.slug, sub.channel, sub.instance,
                )
            })
            .push_enabled
    }

    /// Whether `instance` is in this surface's boot-resolved declaration set.
    ///
    /// The validity check for the `surface:<slug>#<instance>` publisher
    /// sub-identity. The principal *is* the instance, so the server derives
    /// nothing here — it admits or rejects the instance the client's frame
    /// named. That keeps the client's claim surface to a single field whose only
    /// legal values are ones the operator wrote: an instance outside this set is
    /// a protocol violation rather than a fallback identity.
    ///
    /// The kind is deliberately not consulted. It is the manifest — a load-time
    /// compatibility fact and an observability decoration — and never holds
    /// authority.
    ///
    /// Linear scan over the handful of declared components, matching
    /// `output_ports`' probe: allocation-free, and the list is config-sized.
    pub(crate) fn is_declared_instance(&self, instance: &str) -> bool {
        self.resolved
            .components
            .iter()
            .any(|c| c.instance == instance)
    }

    /// Build the runtime for one resolved surface, sharing the given bus.
    ///
    /// `max_body_bytes` is `messaging.max_body_bytes` from config.
    pub fn build(
        resolved: ResolvedSurface,
        bus: Arc<EphemeralBus>,
        messenger: Option<Arc<Messenger>>,
        max_body_bytes: usize,
        description: SurfaceDescriptionParams,
    ) -> Self {
        // Durable messaging needs a Messenger (directory + DB + durable queries).
        // A durable *subscription* with no Messenger is a broken boot invariant —
        // assert it here (fail-fast) rather than fail later in
        // `handle_durable_subscribe`. A durable *output* also needs the Messenger
        // (it publishes via `publish_from_surface`); that direction is enforced
        // fail-fast at the session publish site instead (`handle_publish` panics
        // on a `None` messenger). It is deliberately *not* asserted here: the
        // assert is expressible (it needs only `messenger.is_some()` plus the
        // output classes computed below), but (a) the combination is unreachable
        // from real config — a bound `brenn:` output implies a directory channel
        // implies messaging configured implies `Some` — and (b) asserting it would
        // force a real Messenger into the ephemeral-only tests that share the
        // durable-output-carrying `deskbar_pub` fixture, buying no production
        // coverage. The session-site panic is the fail-fast backstop.
        assert!(
            resolved.durable_subscriptions.is_empty() || messenger.is_some(),
            "surface {:?} has durable subscriptions but no Messenger — durable projection \
             requires messaging to be configured",
            resolved.slug,
        );
        let participant = ParticipantId::for_surface(&resolved.slug);
        let policy = Arc::new(resolved.policy.clone());

        // `local:` bindings are deliberately absent from this gate map: the page
        // routes that traffic itself and must never `Subscribe` to it, so the
        // channel is *unbound* as far as the wire is concerned and a `Subscribe`
        // naming one is the ordinary unbound-channel violation. Same for
        // `output_ports` below. That exclusion is what makes the
        // `DeliveryClass::Local` panic arms downstream (`handle_publish`'s
        // dispatch) structurally unreachable rather than merely unlikely.
        let mut subscription_channels: HashMap<SubKey, SubscriptionFacts> = HashMap::new();
        for b in resolved
            .subscriptions
            .iter()
            .filter(|b| !is_local_channel(&b.channel_address))
        {
            let facts = subscription_channels
                .entry(SubKey {
                    instance: b.instance.clone(),
                    channel: b.channel_address.clone(),
                })
                .or_insert(SubscriptionFacts {
                    class: classify(&b.channel_address),
                    push_enabled: false,
                });
            // Fold: two ports of one instance on one channel share a
            // subscription, and it pushes if either of them does.
            facts.push_enabled |= b.push_depth >= 1;
        }
        let output_ports: HashMap<(String, String), OutputPort> = resolved
            .outputs
            .iter()
            .filter(|b| !is_local_channel(&b.channel_address))
            .map(|b| {
                (
                    (b.instance.clone(), b.port.clone()),
                    OutputPort {
                        address: b.channel_address.clone(),
                        class: classify(&b.channel_address),
                        default_urgency: b.default_urgency,
                    },
                )
            })
            .collect();

        let bindings = SurfaceBindings {
            components: resolved
                .components
                .iter()
                .map(|c| ComponentEntry {
                    instance: c.instance.clone(),
                    kind: c.kind.clone(),
                    abi: c.abi,
                    parked_batch_depth: c.parked_batch_depth,
                    config: c.config.clone(),
                })
                .collect(),
            subscriptions: resolved.subscriptions.iter().map(wire_binding).collect(),
            outputs: resolved.outputs.iter().map(wire_output).collect(),
            // Local channels *do* ride `Welcome`: the client learns its wiring
            // from the backend and hardcodes nothing, page-local traffic
            // included. The server resolves them and then never touches them
            // again.
            local_channels: resolved
                .local_channels
                .iter()
                .map(|c| LocalChannel {
                    channel: c.address.clone(),
                    ring_depth: c.ring_depth,
                })
                .collect(),
            // The surface's chrome singleton. Resolution guarantees exactly one
            // chrome-marked component per surface (boot panics otherwise), so
            // this find always hits. One field, not a per-entry flag.
            chrome_instance: resolved
                .components
                .iter()
                .find(|c| c.chrome)
                .map(|c| c.instance.clone())
                .expect("resolve_surfaces enforces exactly one chrome component per surface"),
        };

        // Every configured instance is an `expected_pumps` key (a zero means no
        // bound subscription), so health derivation can require every configured
        // instance to be reported mounted.
        let configured_kinds: HashMap<String, String> = resolved
            .components
            .iter()
            .map(|c| (c.instance.clone(), c.kind.clone()))
            .collect();
        let mut expected_pumps: HashMap<String, u32> =
            configured_kinds.keys().map(|k| (k.clone(), 0)).collect();
        for binding in &resolved.subscriptions {
            if let Some(count) = expected_pumps.get_mut(&binding.instance) {
                *count += 1;
            }
        }
        let description = SurfaceDescriptionRuntime {
            status_interval_secs: description.status_interval_secs,
            geometry_channel: description::surface_geometry_channel(
                &description.prefix,
                &resolved.slug,
            ),
            status_channel: description::surface_status_channel(
                &description.prefix,
                &resolved.slug,
            ),
            configured_kinds,
            expected_pumps,
        };

        SurfaceRuntime {
            resolved,
            participant,
            policy,
            bus,
            messenger,
            subscription_channels,
            output_ports,
            bindings,
            max_body_bytes,
            // The reserved error-report port + floor are injected by
            // `build_surface_runtimes` (which holds the channel config), not here:
            // `build` also constructs the ephemeral-only test runtimes, which have
            // no error channel.
            error_report_floor: None,
            description,
        }
    }
}

/// Build the boot-time surface map: slug → runtime.
///
/// Every runtime shares the one process `EphemeralBus`. Empty when no
/// `[[surface]]` blocks are configured.
pub fn build_surface_runtimes(
    surfaces: Vec<ResolvedSurface>,
    bus: Arc<EphemeralBus>,
    messenger: Option<Arc<Messenger>>,
    max_body_bytes: usize,
    error_report: Option<(String, LogLevel)>,
    surface_description: SurfaceDescriptionParams,
) -> HashMap<String, Arc<SurfaceRuntime>> {
    surfaces
        .into_iter()
        .map(|resolved| {
            let slug = resolved.slug.clone();
            let mut runtime = SurfaceRuntime::build(
                resolved,
                bus.clone(),
                messenger.clone(),
                max_body_bytes,
                surface_description.clone(),
            );
            // Wire the reserved error-report output port + advertise the floor
            // when an error channel is configured. Addressed via the contract
            // constants (not `SurfaceBindings.outputs`, which is component wiring
            // the kernel renders); the reserved instance id can never collide with
            // a configured component (its `#` prefix fails `is_valid_kind`).
            if let Some((channel_address, floor)) = &error_report {
                runtime.output_ports.insert(
                    (
                        ERROR_REPORT_INSTANCE.to_string(),
                        ERROR_REPORT_PORT.to_string(),
                    ),
                    OutputPort {
                        address: channel_address.clone(),
                        class: DeliveryClass::Durable,
                        // The reserved port is wired from `surface_error_channel`,
                        // not from an `[[surface.output]]` block, so there is no
                        // operator urgency knob on it to read: it takes the same
                        // `normal` an unset one would resolve to. Widening this to
                        // a configurable knob is additive.
                        default_urgency: Urgency::Normal,
                    },
                );
                runtime.error_report_floor = Some(*floor);
            }
            (slug, Arc::new(runtime))
        })
        .collect()
}

/// Boot-time surface-asset existence check.
///
/// When any `[[surface]]` is configured, the kernel module pair
/// (`brenn_surface_kernel.js` + `…_bg.wasm`, referenced unconditionally by every
/// surface page) must exist under `surface_dist_dir`, and every configured
/// component kind must have the assets its ABI implies: a `dom` kind its
/// wasm-bindgen module pair (`brenn_<kind>.js` + `…_bg.wasm`), a `processor`
/// kind its transpiled tree plus a conforming manifest and import profile
/// (`processor_assets`). A missing or stale artifact is a deploy/packaging
/// mistake — config-shaped, boot-time, never attacker-reachable — so this panics
/// (house fail-fast policy). No-op when no surfaces are configured.
///
/// Lives beside `build_surface_runtimes` (a plain function over the resolved
/// list), not in `SurfaceRuntime::build`, so it never runs on the
/// `AppState`-constructing unit tests.
pub fn validate_surface_assets(surface_dist_dir: &std::path::Path, surfaces: &[ResolvedSurface]) {
    if surfaces.is_empty() {
        return;
    }
    assert_module_pair_exists(surface_dist_dir, KERNEL_ARTIFACT, "kernel");
    // A kind names one build artifact, so one kind under two ABIs is operator
    // error — swept across every surface at once, before any per-kind probing,
    // so the diagnosis is the collision rather than whichever asset shape
    // happened to be missing.
    processor_assets::assert_kind_abi_unique(
        surfaces
            .iter()
            .flat_map(|s| s.components.iter().map(|c| (c.kind.clone(), c.abi))),
    );
    // Kind-grain checks (asset existence, manifest, profile) run once per
    // distinct kind across the whole config — several instances, on one surface
    // or several, share one artifact. Alert-grant checking is per declaring
    // surface, so the validated manifests are kept for that second pass.
    let mut manifests: HashMap<&str, processor_assets::ProcessorManifest> = HashMap::new();
    let mut seen_dom: HashSet<&str> = HashSet::new();
    for surface in surfaces {
        for comp in &surface.components {
            match comp.abi {
                brenn_surface_proto::Abi::Dom => {
                    if seen_dom.insert(comp.kind.as_str()) {
                        assert_module_pair_exists(
                            surface_dist_dir,
                            &module_artifact(&comp.kind),
                            &format!("component {:?}", comp.kind),
                        );
                    }
                }
                brenn_surface_proto::Abi::Processor => {
                    if !manifests.contains_key(comp.kind.as_str()) {
                        let manifest =
                            processor_assets::validate_processor_kind(surface_dist_dir, &comp.kind);
                        manifests.insert(comp.kind.as_str(), manifest);
                    }
                }
                // `resolve_abi` rejects the reserved ABIs at config resolution,
                // so no resolved component can carry one.
                brenn_surface_proto::Abi::DomTs | brenn_surface_proto::Abi::Html => unreachable!(
                    "reserved abi {:?} resolved for component {:?} — resolve_abi must reject it",
                    comp.abi, comp.instance,
                ),
            }
        }
    }
    for surface in surfaces {
        let granted = surface
            .policy
            .grants
            .has(brenn_lib::access::AppCapability::SurfaceAlert);
        for comp in &surface.components {
            if let Some(manifest) = manifests.get(comp.kind.as_str()) {
                processor_assets::assert_alert_grant(&surface.slug, &comp.kind, manifest, granted);
            }
        }
    }
}

/// The durable-publisher principal classes swept by
/// [`validate_surface_error_channel`] for single-writer coverage of the surface
/// error channel: the boot-resolved app-policy map, WASM consumers, and
/// surfaces. Bundled so a new principal class extends one struct field rather
/// than another positional parameter (and empty test runs read by name).
#[derive(Default)]
pub struct SingleWriterPrincipals<'a> {
    /// The app map the publish gates consult: `(slug, policy)`.
    pub app_policies: &'a [(&'a str, &'a AppPolicy)],
    /// Resolved WASM consumers (output bindings + policies).
    pub wasm_consumers: &'a [ResolvedWasmConsumer],
    /// Resolved surfaces (output bindings + policies).
    pub surfaces: &'a [ResolvedSurface],
    /// Collected system-participant specs. Their code-built `brenn_publish`
    /// policies are swept too, so a *second* system participant aliasing a
    /// single-writer channel is caught (the channel's permitted writer is
    /// excluded by component name at the call site).
    pub system_participants: &'a [SystemParticipantSpec],
}

/// Worst-case serialized size of a conforming surface error-report body, used by
/// the boot-time headroom assertion so `BodyTooLarge` is structurally unreachable
/// for a conforming kernel's report rather than a runtime surprise on a small
/// `max_body_bytes`.
///
/// The body is the flat `{source, message, level}` object. The kernel truncates
/// `message` to [`MAX_LOG_MESSAGE_BYTES`] and `source` to [`MAX_LOG_SOURCE_BYTES`]
/// before composing it; every input byte of those two fields can expand to at
/// most six output bytes under JSON `\uXXXX` escaping. The fixed 256 allowance
/// covers the remaining envelope — the three object keys and the level string,
/// all genuinely fixed-size.
pub const SURFACE_ERROR_BODY_MAX_BYTES: usize = 6
    * (brenn_surface_proto::MAX_LOG_MESSAGE_BYTES + brenn_surface_proto::MAX_LOG_SOURCE_BYTES)
    + 256;

/// Boot-time validation of `[observability] surface_error_channel`.
///
/// Every failure here is operator config, never attacker-reachable, so each is a
/// boot panic (house fail-fast policy). No-op when the channel is unset
/// (surfaces console-only). Runs once the messaging directory exists, before any
/// session can attach:
///
/// - The address must parse under the `brenn:` scheme — a durable, replayable
///   channel; `ephemeral:`/`webhook:`/`mqtt:` are rejected.
/// - Messaging must be configured at all (a directory exists); the channel set
///   without any messaging is a contradiction, not an inert setting.
/// - The address must resolve to a declared `[[channel]]` — no implicit channel
///   creation.
/// - `max_body_bytes` must clear [`SURFACE_ERROR_BODY_MAX_BYTES`], so
///   `BodyTooLarge` is structurally unreachable for a max-size conforming report.
///
/// The channel is **many-writer by design**: every surface publishes onto it
/// under its own `surface:<slug>` identity (a boot-injected substrate grant), so
/// there is no single-writer sweep here. Subscriber trust keys on the envelope
/// sender's identity class (its minting authority), never on channel occupancy.
/// `system:` senders are legitimate on the channel only for errors genuinely
/// originating in Brenn's native code. The surviving single-writer machinery
/// ([`assert_channel_single_writer`], [`SingleWriterPrincipals`]) guards the
/// boot-published surface-description channels.
pub fn validate_surface_error_channel(
    channel: Option<&str>,
    directory: Option<&MessagingDirectory>,
    max_body_bytes: usize,
) {
    let Some(channel) = channel else {
        return;
    };

    // The address must be a well-formed brenn: channel (durable, replayable);
    // the parse is the validation, its bare name no longer needed downstream.
    well_formed_name(channel, ChannelScheme::Brenn).unwrap_or_else(|| {
        panic!(
            "boot: [observability] surface_error_channel {channel:?} is not a well-formed brenn: \
             address — error reports need a durable, replayable channel, so only the brenn: scheme \
             is accepted. Refusing to start (fail-fast on invalid config)."
        )
    });

    let directory = directory.unwrap_or_else(|| {
        panic!(
            "boot: [observability] surface_error_channel {channel:?} is set but no messaging is \
             configured (no [[channel]] blocks, no Messenger). Declare messaging or unset the \
             channel. Refusing to start (fail-fast on invalid config)."
        )
    });

    let Some(entry) = directory.resolve(channel) else {
        panic!(
            "boot: [observability] surface_error_channel {channel:?} does not resolve to any \
             declared [[channel]] block — error routing requires an explicit matching channel; no \
             implicit channel is created. Refusing to start (fail-fast on invalid config)."
        );
    };

    // A bounded eviction frontier at or below one surface's admitted send burst
    // means one fully-admitted burst can rotate every earlier report out of the
    // durable channel before the budget refills. Warn once at boot; the evicted
    // reports still survive the kernel's console copy, so this is a footgun, not a
    // fatal misconfiguration. A pinned channel (frontier None) never triggers.
    if let Some(frontier) = entry.reap_frontier()
        && frontier <= u64::from(brenn_lib::messaging::publish::SURFACE_SEND_BURST)
    {
        let refill_window_secs = u64::from(brenn_lib::messaging::publish::SURFACE_SEND_BURST)
            * brenn_lib::messaging::publish::SURFACE_SEND_REFILL.as_secs();
        tracing::warn!(
            channel,
            frontier,
            burst = brenn_lib::messaging::publish::SURFACE_SEND_BURST,
            refill_window_secs,
            "boot: [observability] surface_error_channel eviction frontier is at or below the \
             surface send burst — one admitted burst can rotate every earlier report out of \
             the channel, and the budget fully refills within the window. Evicted reports still \
             survive the kernel's console copy. Raise standing_retain_depth (or a subscriber's \
             retain/push depth) above the burst to close the window."
        );
    }

    assert!(
        max_body_bytes >= SURFACE_ERROR_BODY_MAX_BYTES,
        "boot: [messaging] max_body_bytes {max_body_bytes} is below the worst-case surface error \
         report body ({SURFACE_ERROR_BODY_MAX_BYTES} bytes) — a report publish could hit \
         BodyTooLarge at runtime. Raise max_body_bytes. Refusing to start (fail-fast on invalid \
         config).",
    );
}

/// The single principal permitted to write a single-writer `brenn:` channel.
///
/// A boot-published help/schema/index channel is written by one system
/// participant (`System`); a runtime geometry/status channel is written by its
/// owning surface (`Surface`), via the boot-injected geometry/status grant and
/// the platform publish path. The sweep excludes exactly that principal and
/// panics on any other covering writer.
#[derive(Clone, Copy)]
pub(super) enum ExpectedWriter<'a> {
    /// `system:<component>` — a boot-published channel's reserved publisher.
    System(&'a str),
    /// `surface:<slug>` — a runtime geometry/status channel's owning surface.
    Surface(&'a str),
}

impl ExpectedWriter<'_> {
    /// The permitted-writer identity, for the panic messages.
    fn describe(&self) -> String {
        match self {
            ExpectedWriter::System(component) => format!("system:{component}"),
            ExpectedWriter::Surface(slug) => format!("surface:{slug}"),
        }
    }
}

/// Sweep every durable-publisher class for a covering path onto a single-writer
/// `brenn:` channel, panicking (boot fail-fast) on any principal other than
/// `expected` that could write it. Used by the surface self-description
/// validator, which runs it once per derived channel — the boot-published
/// help/schema/index channels are single-writer under `system:surface-help`, and
/// each runtime geometry/status channel is single-writer under its owning
/// surface — so the "which classes can publish durably" checklist lives in
/// exactly one place.
///
/// `bare` is the scheme-stripped channel name; `channel` the full address (both
/// only for the panic messages and the ACL-coverage check). The sweep covers
/// surface + WASM output bindings (exact-address) and the resolved-policy
/// `brenn_publish` ACL coverage (Exact or accidental-broad Prefix) over the app
/// map, WASM consumers, surfaces, and the collected system-participant specs.
///
/// `expected` names the one principal permitted to write the channel; it is
/// excluded from its own class's sweep (the system participant by component name,
/// or the owning surface by slug). Every other principal in every class is swept
/// with no exception.
pub(super) fn assert_channel_single_writer(
    channel: &str,
    bare: &str,
    expected: ExpectedWriter<'_>,
    app_policies: &[(&str, &AppPolicy)],
    wasm_consumers: &[ResolvedWasmConsumer],
    surfaces: &[ResolvedSurface],
    system_participants: &[SystemParticipantSpec],
) {
    // Output bindings (canonical full addresses): surfaces...
    //
    // Deliberately *no* owner exclusion here, unlike the policy sweep below. The
    // owning surface's exemption is for its kernel identity's geometry/status
    // grant; a component of that same surface publishes under its own
    // `surface:<slug>#<kind>` sub-identity, which is a foreign writer to a
    // channel whose single writer is the bare `surface:<slug>`. A component can
    // only publish through a bound output port, so rejecting the binding is
    // where that reachability actually ends.
    for surface in surfaces {
        for output in &surface.outputs {
            assert!(
                output.channel_address != channel,
                "boot: [[surface]] {:?} output binding (instance {:?}, port {:?}) targets \
                 single-writer channel {channel:?} — only {} may write it. Remove the output \
                 binding. Refusing to start (fail-fast on invalid config).",
                surface.slug,
                output.instance,
                output.port,
                expected.describe(),
            );
        }
    }
    // ...and WASM consumers.
    for consumer in wasm_consumers {
        for output in &consumer.outputs {
            assert!(
                output.channel_address != channel,
                "boot: [[wasm_consumer]] {:?} output binding (port {:?}) targets single-writer \
                 channel {channel:?} — only {} may write it. Remove the output binding or \
                 retarget it. Refusing to start (fail-fast on invalid config).",
                consumer.slug,
                output.port,
                expected.describe(),
            );
        }
    }

    // Resolved-policy sweep: any principal whose policy covers the channel via a
    // `brenn_publish` matcher (Exact or Prefix — the accidental-broad-prefix
    // case) is a forgery path. Catches ACL coverage the exact-address binding
    // checks above never see. The owning surface (for a `Surface` expected
    // writer) is excluded from the surface sweep — its geometry/status grant is
    // the sanctioned single-writer coverage; every other principal is swept.
    let expected_desc = expected.describe();
    for (slug, policy) in app_policies {
        assert_no_covering_publish("[[app]]", slug, policy, bare, channel, &expected_desc);
    }
    for consumer in wasm_consumers {
        assert_no_covering_publish(
            "[[wasm_consumer]]",
            &consumer.slug,
            &consumer.policy,
            bare,
            channel,
            &expected_desc,
        );
    }
    for surface in surfaces {
        if matches!(expected, ExpectedWriter::Surface(owner) if owner == surface.slug) {
            continue; // the single permitted writer of a runtime channel
        }
        assert_no_covering_publish(
            "[[surface]]",
            &surface.slug,
            &surface.policy,
            bare,
            channel,
            &expected_desc,
        );
    }
    // System-participant sweep: the one permitted system writer (for a `System`
    // expected writer) is excluded; any *other* system participant whose
    // code-built policy covers the channel would break the single-writer premise.
    for spec in system_participants {
        if matches!(expected, ExpectedWriter::System(component) if component == spec.component) {
            continue; // the single permitted writer
        }
        assert_no_covering_publish(
            "system participant",
            spec.component,
            &spec.policy,
            bare,
            channel,
            &expected_desc,
        );
    }
}

/// Panic if `policy` holds a `brenn_publish` path covering `bare` (the
/// scheme-stripped channel name) — the single-writer forgery guard. The message
/// names the offending principal (`kind` + `slug`), the covering matcher list to
/// narrow, and the channel, so an operator can remediate without reading the
/// code.
fn assert_no_covering_publish(
    kind: &str,
    slug: &str,
    policy: &AppPolicy,
    bare: &str,
    channel: &str,
    expected_desc: &str,
) {
    assert!(
        !policy.allows_brenn_publish(bare),
        "boot: {kind} {slug:?} holds a brenn_publish ACL covering single-writer channel \
         {channel:?} (matchers: {:?}) — only {expected_desc} may write it, so any other covering \
         grant is a forgery path. Narrow the ACL, drop the MessagingPublish grant, or rename the \
         channel. Refusing to start (fail-fast on invalid config).",
        policy.acls.brenn_publish,
    );
}

/// Assert both halves of a wasm-bindgen `--target web` module — the `.js` loader
/// and its `_bg.wasm` sibling — exist under `dir`. `what` labels the module in
/// the panic message (e.g. `"kernel"` or `"component \"echo-stub\""`).
fn assert_module_pair_exists(dir: &std::path::Path, js_artifact: &str, what: &str) {
    let wasm_artifact = js_artifact
        .strip_suffix(".js")
        .map(|stem| format!("{stem}_bg.wasm"))
        .unwrap_or_else(|| panic!("surface artifact name {js_artifact:?} lacks a .js suffix"));
    for artifact in [js_artifact, wasm_artifact.as_str()] {
        let path = dir.join(artifact);
        assert!(
            path.exists(),
            "boot: {what} surface asset {artifact} missing at {} — surface assets are not \
             built/deployed (run `make surface-wasm`; on deploy ensure surface_dist_dir is \
             populated). Refusing to start (fail-fast on invalid config).",
            path.display(),
        );
    }
}

/// Query parameters for the surface WS endpoint.
///
/// `build` is `Option` for the same handler-controls-classification reason as
/// the legacy `WsQuery`: a missing value is a stale first-party tab (close with
/// the stale code, no security event), not a probe.
#[derive(serde::Deserialize)]
pub(crate) struct SurfaceWsQuery {
    build: Option<String>,
}

/// `GET /surface/{slug}/ws` — upgrade to the surface WebSocket.
///
/// Auth middleware has already validated the session and injected `Session` /
/// `ClientIp`. Pre-upgrade checks run in the order access → capacity → handshake
/// so an unauthorized user sees `403` (and never learns attach counts), and a
/// full surface never consumes an upgraded socket.
pub async fn surface_ws_handler(
    Path(slug): Path<String>,
    Query(query): Query<SurfaceWsQuery>,
    ws: WebSocketUpgrade,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
) -> Result<Response, StatusCode> {
    // 1-2. Surface must exist and the user must pass its access check.
    let runtime = authorize_surface(&state, &slug, &session.user.username, ip, true)?;

    // 3. Capacity: register the slot before upgrading so the check has no
    //    check-then-register race and a full surface never upgrades the socket.
    let session_id = Uuid::new_v4();
    // Durable-delivery handle, shared between the registry (read by the router
    // fan-out) and the session task (which drains it): a bounded live-delivery
    // queue, the active-durable-channel set, and the drain nudge.
    let (durable_tx, durable_rx) = tokio::sync::mpsc::channel(DURABLE_QUEUE_FRAMES);
    let durable_subs = Arc::new(Mutex::new(HashSet::new()));
    let drain_notify = Arc::new(tokio::sync::Notify::new());
    let handle = SurfaceSessionHandle {
        session_id,
        username: session.user.username.clone(),
        client_ip: ip,
        connected_at: Utc::now(),
        durable_tx,
        durable_subs: durable_subs.clone(),
        drain_notify: drain_notify.clone(),
    };
    let caps = SessionCaps {
        per_surface: MAX_SESSIONS_PER_SURFACE,
        per_user: MAX_SESSIONS_PER_USER_PER_SURFACE,
    };
    let guard = match state.surface_registry.try_register(&slug, handle, caps) {
        Ok(guard) => guard,
        Err(RegisterRejection::SurfaceFull { current }) => {
            // Not a security event: a user with many tabs is not fail2ban signal.
            warn!(
                surface = %slug,
                user = %session.user.username,
                ip = %ip,
                count = current,
                "surface session cap reached; rejecting with 503"
            );
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        Err(RegisterRejection::UserCapExceeded { user_current }) => {
            // Not a security event either: a legitimate user with many devices
            // or tabs can trip this, and banning that IP would lock out an
            // authenticated user. The distinct message + user attribution turns
            // "surface is mysteriously full" into a one-grep answer.
            warn!(
                surface = %slug,
                user = %session.user.username,
                ip = %ip,
                user_count = user_current,
                "per-user surface session cap reached; rejecting with 503"
            );
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // 4. Build-ID handshake. Missing or mismatched build is a stale first-party
    //    tab: accept the upgrade, then close with the stale code (dropping the
    //    guard releases the slot). No security event.
    let build_id = state.build_id;
    match query.build.as_deref() {
        Some(v) if v == build_id => {}
        other => {
            let client_build = other.unwrap_or("<missing>").to_string();
            drop(guard);
            return Ok(ws.on_upgrade(move |socket| async move {
                close_with_stale_client(socket, &client_build, build_id).await;
            }));
        }
    }

    // 5. Frame cap derived from config, then upgrade into the session task.
    let cap = max_client_frame_bytes(runtime.max_body_bytes);
    let params_username = session.user.username;
    let heartbeat_secs = state.surface_heartbeat_secs;
    let alert_dispatcher = state.alert_dispatcher.clone();
    Ok(ws
        .max_message_size(cap)
        .max_frame_size(cap)
        .on_upgrade(move |socket| {
            run_surface_session(SurfaceSessionParams {
                runtime,
                session_id,
                username: params_username,
                ip,
                guard,
                heartbeat_secs,
                alert_dispatcher,
                durable_rx,
                durable_subs,
                drain_notify,
                socket,
            })
        }))
}

#[cfg(test)]
mod tests {
    use brenn_lib::messaging::config::{
        ResolvedComponent, ResolvedSurface, SurfaceBinding, SurfaceSendBudget,
    };

    use super::test_fixtures::{
        TEST_MAX_BODY_BYTES, directory_with, directory_with_standing, fixture_bus,
    };
    use super::*;

    /// `wire_noise` is a four-arm hand-written match between two same-named,
    /// same-ordered enums: transposing `Alarm` and `Fatal` compiles clean and
    /// ships an overflow that should toast as one that kills the instance. Each
    /// rung is pinned to its port, the same argument `fold` makes in chrome.
    #[test]
    fn wire_noise_maps_every_rung_to_its_own() {
        use brenn_lib::messaging::config::NoiseLevel as N;
        assert_eq!(wire_noise(N::Silent), WireNoiseLevel::Silent);
        assert_eq!(wire_noise(N::Metered), WireNoiseLevel::Metered);
        assert_eq!(wire_noise(N::Alarm), WireNoiseLevel::Alarm);
        assert_eq!(wire_noise(N::Fatal), WireNoiseLevel::Fatal);
    }

    fn empty_bus() -> Arc<EphemeralBus> {
        fixture_bus(vec![])
    }

    fn resolved(slug: &str) -> ResolvedSurface {
        ResolvedSurface {
            slug: slug.to_string(),
            skin: "bench".to_string(),
            components: vec![
                ResolvedComponent {
                    instance: "protobar".to_string(),
                    kind: "protobar".to_string(),
                    abi: brenn_surface_proto::Abi::Dom,
                    send_budget: SurfaceSendBudget::default(),
                    parked_batch_depth: 8,
                    config: Default::default(),
                    chrome: true,
                },
                ResolvedComponent {
                    instance: "writer".to_string(),
                    kind: "writer".to_string(),
                    abi: brenn_surface_proto::Abi::Dom,
                    send_budget: SurfaceSendBudget::default(),
                    parked_batch_depth: 8,
                    config: Default::default(),
                    chrome: false,
                },
            ],
            subscriptions: vec![SurfaceBinding {
                channel_address: "ephemeral:protobar-demo".to_string(),
                instance: "protobar".to_string(),
                port: "messages".to_string(),
                push_depth: 8,
                retain_depth: 0,
                noise: brenn_lib::messaging::config::NoiseLevel::Silent,
            }],
            durable_subscriptions: vec![],
            local_channels: vec![],
            outputs: vec![SurfaceOutput {
                channel_address: "brenn:writer-out".to_string(),
                instance: "writer".to_string(),
                port: "out".to_string(),
                default_urgency: Urgency::Normal,
                budget: brenn_budget::SinkBudget {
                    fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                    capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                },
            }],
            policy: AppPolicy::default(),
            allowed_users: vec![],
            publish_burst: 60,
            publish_per_sec: 1,
        }
    }

    #[test]
    fn build_classifies_channels_and_prebuilds_bindings() {
        let rt = SurfaceRuntime::build(
            resolved("deskbar"),
            empty_bus(),
            None,
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        );

        assert_eq!(rt.participant.as_str(), "surface:deskbar");
        assert_eq!(rt.max_body_bytes, TEST_MAX_BODY_BYTES);

        // Subscription keyed by its owning principal, classified by scheme.
        assert_eq!(
            rt.subscription_channels.get(&SubKey {
                instance: "protobar".to_string(),
                channel: "ephemeral:protobar-demo".to_string(),
            }),
            Some(&SubscriptionFacts {
                class: DeliveryClass::Ephemeral,
                push_enabled: true,
            })
        );

        // Output port keyed by (instance, port) → its resolved dispatch facts.
        let out = rt
            .output_ports
            .get(&("writer".to_string(), "out".to_string()))
            .expect("the writer/out output port");
        assert_eq!(out.address, "brenn:writer-out");
        assert_eq!(out.class, DeliveryClass::Durable);
        assert_eq!(out.default_urgency, Urgency::Normal);

        // Bindings mirror the resolved config for the Welcome payload.
        let comp_pairs: Vec<(&str, &str)> = rt
            .bindings
            .components
            .iter()
            .map(|c| (c.instance.as_str(), c.kind.as_str()))
            .collect();
        assert_eq!(
            comp_pairs,
            vec![("protobar", "protobar"), ("writer", "writer")]
        );
        assert_eq!(rt.bindings.subscriptions.len(), 1);
        assert_eq!(
            rt.bindings.subscriptions[0].channel,
            "ephemeral:protobar-demo"
        );
        assert_eq!(rt.bindings.subscriptions[0].port, "messages");
        assert_eq!(rt.bindings.outputs.len(), 1);
        assert_eq!(rt.bindings.outputs[0].channel, "brenn:writer-out");
        // The fixture's chrome singleton is the `protobar` component.
        assert_eq!(rt.bindings.chrome_instance, "protobar");
    }

    /// The server advertises the resolved chrome instance in
    /// `SurfaceBindings.chrome_instance` — the singleton the kernel treats
    /// specially. One field, populated from the component that sets `chrome`.
    #[test]
    fn build_advertises_the_chrome_instance() {
        let mut resolved = resolved("deskbar");
        // Move the chrome designation off the default (protobar) onto writer, so
        // the assertion proves the field tracks the marked component, not the
        // first one.
        resolved.components[0].chrome = false;
        resolved.components[1].chrome = true;
        let rt = SurfaceRuntime::build(
            resolved,
            empty_bus(),
            None,
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        );
        assert_eq!(rt.bindings.chrome_instance, "writer");
    }

    /// A resolved surface wired page-locally in both directions, plus the
    /// resolved router table.
    fn resolved_with_local(slug: &str) -> ResolvedSurface {
        use brenn_lib::messaging::config::ResolvedLocalChannel;
        let mut r = resolved(slug);
        r.subscriptions.push(SurfaceBinding {
            channel_address: "local:page-bus".to_string(),
            instance: "protobar".to_string(),
            port: "local-in".to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: brenn_lib::messaging::config::NoiseLevel::Silent,
        });
        r.outputs.push(SurfaceOutput {
            channel_address: "local:page-bus".to_string(),
            instance: "writer".to_string(),
            port: "local-out".to_string(),
            default_urgency: Urgency::Normal,
            budget: brenn_budget::SinkBudget {
                fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            },
        });
        r.local_channels = vec![ResolvedLocalChannel {
            address: "local:page-bus".to_string(),
            ring_depth: 3,
        }];
        r
    }

    /// The invariant that keeps `local:` off the wire, checked at the only place
    /// it is enforced: a local binding rides `Welcome` (the page needs its
    /// wiring) but is absent from both gate maps. That absence is what makes a
    /// `Subscribe`/`Publish` naming it fall into the existing unbound-channel /
    /// unbound-port violation arms rather than reaching the bus.
    #[test]
    fn build_advertises_local_bindings_but_keeps_them_out_of_the_wire_maps() {
        let rt = SurfaceRuntime::build(
            resolved_with_local("deskbar"),
            empty_bus(),
            None,
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        );

        // Advertised: the client learns its page-local wiring from the backend.
        assert!(
            rt.bindings
                .subscriptions
                .iter()
                .any(|b| b.channel == "local:page-bus" && b.port == "local-in")
        );
        assert!(
            rt.bindings
                .outputs
                .iter()
                .any(|b| b.channel == "local:page-bus" && b.port == "local-out")
        );
        assert_eq!(
            rt.bindings.local_channels,
            vec![LocalChannel {
                channel: "local:page-bus".to_string(),
                ring_depth: 3,
            }]
        );

        // Unbound on the wire, in both directions.
        assert_eq!(
            rt.subscription_channels.get(&SubKey {
                instance: "protobar".to_string(),
                channel: "local:page-bus".to_string(),
            }),
            None
        );
        assert!(
            !rt.output_ports
                .contains_key(&("writer".to_string(), "local-out".to_string()))
        );
        // The non-local bindings on the same surface are unaffected: the filter
        // excludes the scheme, not the surface.
        assert!(rt.subscription_channels.contains_key(&SubKey {
            instance: "protobar".to_string(),
            channel: "ephemeral:protobar-demo".to_string(),
        }));
        assert!(
            rt.output_ports
                .contains_key(&("writer".to_string(), "out".to_string()))
        );
    }

    /// A surface with no local wiring advertises an empty router table — not a
    /// missing field the client has to treat as unknown.
    #[test]
    fn build_advertises_no_local_channels_when_none_are_declared() {
        let rt = SurfaceRuntime::build(
            resolved("deskbar"),
            empty_bus(),
            None,
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        );
        assert!(rt.bindings.local_channels.is_empty());
    }

    #[test]
    fn build_surface_runtimes_keys_by_slug_and_shares_bus() {
        let bus = empty_bus();
        let map = build_surface_runtimes(
            vec![resolved("deskbar"), resolved("kitchen")],
            bus.clone(),
            None,
            TEST_MAX_BODY_BYTES,
            None,
            crate::test_support::surface::description_params(),
        );

        assert_eq!(map.len(), 2);
        assert!(map.contains_key("deskbar"));
        assert!(map.contains_key("kitchen"));
        // Every runtime shares the one process bus.
        assert!(Arc::ptr_eq(&map["deskbar"].bus, &bus));
        assert!(Arc::ptr_eq(&map["kitchen"].bus, &bus));
    }

    #[test]
    fn build_surface_runtimes_empty_for_surfaceless_config() {
        // A config with zero `[[surface]]` blocks yields an empty runtime map,
        // the same value the boot path installs on `AppState.surfaces` for a
        // surface-free config. Nothing synthesizes a default surface.
        let map = build_surface_runtimes(
            vec![],
            empty_bus(),
            None,
            TEST_MAX_BODY_BYTES,
            None,
            crate::test_support::surface::description_params(),
        );
        assert!(map.is_empty());
    }

    #[test]
    fn build_surface_runtimes_wires_reserved_error_port_and_floor() {
        // With an error channel configured, every runtime gains the reserved
        // `#brenn`/`error-reports` durable output port and advertises the floor.
        let map = build_surface_runtimes(
            vec![resolved("deskbar")],
            empty_bus(),
            None,
            TEST_MAX_BODY_BYTES,
            Some(("brenn:surface-errors".to_string(), LogLevel::Warn)),
            crate::test_support::surface::description_params(),
        );
        let rt = &map["deskbar"];
        let reserved = rt
            .output_ports
            .get(&(
                ERROR_REPORT_INSTANCE.to_string(),
                ERROR_REPORT_PORT.to_string(),
            ))
            .expect("the reserved error-report output port");
        assert_eq!(reserved.address, "brenn:surface-errors");
        assert_eq!(reserved.class, DeliveryClass::Durable);
        // Wired from `surface_error_channel`, not an `[[surface.output]]` block,
        // so there is no operator urgency knob on it to read.
        assert_eq!(reserved.default_urgency, Urgency::Normal);
        assert_eq!(rt.error_report_floor, Some(LogLevel::Warn));
    }

    #[test]
    fn build_surface_runtimes_no_reserved_port_without_error_channel() {
        // Unset error channel: no reserved port, floor `None` (kernel console-only).
        let map = build_surface_runtimes(
            vec![resolved("deskbar")],
            empty_bus(),
            None,
            TEST_MAX_BODY_BYTES,
            None,
            crate::test_support::surface::description_params(),
        );
        let rt = &map["deskbar"];
        assert!(!rt.output_ports.contains_key(&(
            ERROR_REPORT_INSTANCE.to_string(),
            ERROR_REPORT_PORT.to_string()
        )));
        assert_eq!(rt.error_report_floor, None);
    }

    fn touch(dir: &std::path::Path, name: &str) {
        std::fs::write(dir.join(name), b"").expect("write test artifact");
    }

    fn write_kernel_pair(dir: &std::path::Path) {
        touch(dir, "brenn_surface_kernel.js");
        touch(dir, "brenn_surface_kernel_bg.wasm");
    }

    /// Touch a component's `.js` + `_bg.wasm` pair, deriving both names from the
    /// contract convention (`module_artifact`) rather than re-implementing it, so
    /// the test tracks the code under test.
    fn touch_component_pair(dir: &std::path::Path, kind: &str) {
        let js = module_artifact(kind);
        let wasm = format!(
            "{}_bg.wasm",
            js.strip_suffix(".js").expect("artifact ends in .js")
        );
        touch(dir, &js);
        touch(dir, &wasm);
    }

    #[test]
    fn validate_surface_assets_noop_when_no_surfaces() {
        // Empty surface list is a no-op even against a nonexistent directory:
        // the check only guards surfaces that actually exist.
        validate_surface_assets(std::path::Path::new("/nonexistent/surface/dist"), &[]);
    }

    #[test]
    fn validate_surface_assets_passes_with_all_pairs_present() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let surface = resolved("deskbar");
        for comp in &surface.components {
            touch_component_pair(dir.path(), &comp.kind);
        }
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "kernel surface asset brenn_surface_kernel.js missing")]
    fn validate_surface_assets_panics_on_missing_kernel_pair() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Component modules present, but the unconditionally-referenced kernel
        // pair is absent.
        let surface = resolved("deskbar");
        for comp in &surface.components {
            touch_component_pair(dir.path(), &comp.kind);
        }
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "component \"writer\" surface asset brenn_writer.js missing")]
    fn validate_surface_assets_panics_on_missing_component_artifact() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        // Only the first component's module exists; the second (declared
        // "writer" in `resolved`, matching the panic-message assertion above)
        // has no artifact on disk.
        let surface = resolved("deskbar");
        touch_component_pair(dir.path(), &surface.components[0].kind);
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// A surface whose sole component is a `processor` of `kind`, with no
    /// bindings — asset validation reads only the component list and the policy.
    fn resolved_with_processor(slug: &str, kind: &str) -> ResolvedSurface {
        let mut surface = resolved(slug);
        surface.components = vec![ResolvedComponent {
            instance: format!("{kind}-1"),
            kind: kind.to_string(),
            abi: brenn_surface_proto::Abi::Processor,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: false,
        }];
        surface.subscriptions = vec![];
        surface.outputs = vec![];
        surface
    }

    /// Write a conforming transpiled tree for `kind`: a stand-in component
    /// artifact, one transpiled file, and a manifest whose `source_sha256`
    /// actually hashes the artifact bytes. `imports` and any manifest edits are
    /// applied by the caller through `tweak` before serialization, so each
    /// failure test perturbs exactly one field of an otherwise valid tree.
    fn write_processor_tree(
        dist: &std::path::Path,
        kind: &str,
        imports: &[&str],
        tweak: impl FnOnce(&mut serde_json::Value),
    ) {
        // The manifest carries fully qualified import names (as the build emitter
        // does). A caller passing a bare interface name gets it qualified under
        // the processor package; a caller passing an already-qualified name (to
        // exercise a foreign namespace) keeps it verbatim.
        let qualified: Vec<String> = imports
            .iter()
            .map(|i| {
                if i.contains(':') {
                    (*i).to_string()
                } else {
                    format!("brenn:processor/{i}")
                }
            })
            .collect();
        let component_bytes = format!("component-bytes-for-{kind}").into_bytes();
        write_processor_tree_from_bytes(dist, kind, &component_bytes, qualified, true, tweak);
    }

    /// The one place test code constructs a deployed processor tree and its
    /// manifest schema. `component_bytes` are the shipped artifact (a stand-in
    /// string for the synthetic tests, real artifact bytes for the real-artifact
    /// test), `imports` the profile verbatim, and `with_module` controls whether a
    /// stand-in transpiled `<kind>.js` is written and listed.
    fn write_processor_tree_from_bytes(
        dist: &std::path::Path,
        kind: &str,
        component_bytes: &[u8],
        imports: Vec<String>,
        with_module: bool,
        tweak: impl FnOnce(&mut serde_json::Value),
    ) {
        let dir = processor_assets::kind_dir(dist, kind);
        std::fs::create_dir_all(&dir).expect("create processor dir");
        let component_name = format!("{kind}.component.wasm");
        std::fs::write(dir.join(&component_name), component_bytes).expect("write component");

        let mut files = Vec::new();
        if with_module {
            let module = format!("{kind}.js");
            std::fs::write(dir.join(&module), b"export function instantiate() {}")
                .expect("write module");
            files.push(module);
        }
        files.push(component_name);

        use sha2::Digest as _;
        let mut manifest = serde_json::json!({
            "v": 1,
            "kind": kind,
            "source_sha256": hex::encode(sha2::Sha256::digest(component_bytes)),
            "jco_version": PINNED_JCO_VERSION_FOR_TESTS,
            "imports": imports,
            "files": files,
        });
        tweak(&mut manifest);
        std::fs::write(
            dir.join("manifest.json"),
            serde_json::to_string(&manifest).expect("serialize manifest"),
        )
        .expect("write manifest");
    }

    /// Provenance only — boot validation never checks it (the source hash is the
    /// staleness authority), so any well-formed value serves.
    const PINNED_JCO_VERSION_FOR_TESTS: &str = "1.4.0";

    /// The valid-tree case: manifest parses, every listed file exists, the
    /// source hash matches the shipped bytes, and the imports are within the
    /// transpilable profile.
    #[test]
    fn validate_surface_assets_passes_with_conforming_processor_tree() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(
            dir.path(),
            "transplant",
            &["ports", "log", "config"],
            |_| {},
        );
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    #[test]
    #[should_panic(expected = "processor component \"transplant\" has no readable asset manifest")]
    fn validate_surface_assets_panics_on_missing_processor_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    #[test]
    #[should_panic(expected = "asset manifest at")]
    fn validate_surface_assets_panics_on_unknown_processor_manifest_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        // A key this server's schema does not define: the build wrote a manifest
        // under semantics these rules cannot evaluate, so it is rejected rather
        // than partially honoured.
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            m["future_field"] = serde_json::json!("whatever");
        });
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    #[test]
    #[should_panic(expected = "manifest declares v = 2")]
    fn validate_surface_assets_panics_on_processor_manifest_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            m["v"] = serde_json::json!(2);
        });
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    #[test]
    #[should_panic(expected = "manifest lists \"missing-chunk.core.wasm\", which is missing")]
    fn validate_surface_assets_panics_on_missing_listed_processor_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            m["files"]
                .as_array_mut()
                .expect("files is an array")
                .push(serde_json::json!("missing-chunk.core.wasm"));
        });
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    #[test]
    #[should_panic(expected = "has a stale transpile")]
    fn validate_surface_assets_panics_on_processor_source_hash_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        // The manifest's hash no longer describes the shipped component bytes:
        // exactly the shape of a component rebuilt without re-transpiling, or a
        // deploy that synced only half the tree.
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            m["source_sha256"] = serde_json::json!("00".repeat(32));
        });
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    #[test]
    #[should_panic(expected = "imports \"brenn:processor/store\", which no surface can satisfy")]
    fn validate_surface_assets_panics_on_backend_only_processor_import() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "store-rt", &["ports", "store"], |_| {});
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "store-rt")],
        );
    }

    /// The real backend fixture `processor-store-rt`, laid out as a deployed
    /// surface tree from its **actual bytes and actual import profile**, and run
    /// through the real boot validation.
    ///
    /// The synthetic sibling above pins the rejection *mechanism* against a
    /// hand-written `["ports", "store"]` manifest. This pins its *premise*: that
    /// the artifact backend tests load really does import `store`, so the
    /// mechanism is not rejecting a strawman. Nothing here is hand-written — the
    /// hash is of the shipped bytes and the profile is read out of the component
    /// — which is what makes this the executable negative half of the invariant:
    /// the same artifact that loads fine under `[[wasm_consumer]]` (pinned by the
    /// backend store tests) cannot be declared on a surface.
    #[test]
    #[should_panic(expected = "imports \"brenn:processor/store\", which no surface can satisfy")]
    fn validate_surface_assets_panics_on_real_store_importing_artifact() {
        let artifact = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../brenn-wasm/target/components/brenn_processor_store_rt.wasm");
        assert!(
            artifact.exists(),
            "the real store-rt component artifact is missing at {} — build it with \
             `make wasm-components`",
            artifact.display(),
        );

        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());

        let kind = "store-rt";
        let bytes = std::fs::read(&artifact).expect("read the real component artifact");
        // Nothing hand-written: the bytes are the shipped artifact and the profile
        // is read out of it exactly as the build's manifest emitter reads it.
        write_processor_tree_from_bytes(
            dir.path(),
            kind,
            &bytes,
            brenn_wasm::processor_component_imports(&artifact),
            false,
            |_| {},
        );

        validate_surface_assets(dir.path(), &[resolved_with_processor("deskbar", kind)]);
    }

    /// Parity pin: the shell emitter that writes real manifests and
    /// `processor_component_imports` must extract the *same* import profile from
    /// the same artifact.
    ///
    /// Two independent implementations of one load-bearing normalization
    /// (fully-qualified name, `@version` stripped) drift silently otherwise: a
    /// change to the emitter's `sed` pipeline, or a `wasm-tools` output-format
    /// change, would ship a differently-shaped profile that no test built on the
    /// Rust twin would notice until a deploy or page-load failure. This asserts
    /// the emitted `imports` equals what the twin reads out of the very component
    /// the emitted manifest was built from.
    ///
    /// The transpiled tree is build output, not a checked-in fixture, so `make
    /// test` takes `surface-transpile` as a prerequisite. Its absence is a hard
    /// failure naming that command — a skipped parity test reports green while
    /// asserting nothing, which is the failure mode this pin exists to close.
    #[test]
    fn emitted_processor_manifest_imports_match_the_in_process_extractor() {
        let kind = "processor-transplant";
        let tree = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../surface/dist/processor")
            .join(kind);
        let manifest_path = tree.join("manifest.json");
        assert!(
            manifest_path.exists(),
            "no transpiled tree at {} — build it with `make surface-transpile`",
            tree.display()
        );

        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&manifest_path).expect("read the emitted manifest"),
        )
        .expect("the emitted manifest parses as JSON");
        let emitted: Vec<String> = manifest["imports"]
            .as_array()
            .expect("the emitted manifest has an imports array")
            .iter()
            .map(|v| {
                v.as_str()
                    .expect("every emitted import is a string")
                    .to_string()
            })
            .collect();

        let component = tree.join(format!("{kind}.component.wasm"));
        let extracted = brenn_wasm::processor_component_imports(&component);

        assert_eq!(
            emitted,
            extracted,
            "the build's manifest emitter and `processor_component_imports` \
             disagree about {}'s import profile — one of the two normalizations \
             changed; they must stay identical",
            component.display(),
        );
    }

    #[test]
    #[should_panic(
        expected = "lists import \"brenn:processor/telepathy\", which names no interface"
    )]
    fn validate_surface_assets_panics_on_unknown_processor_import() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports", "telepathy"], |_| {});
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    /// A foreign-namespace import — a stray `wasi:*` a dependency dragged in — is
    /// rejected at boot by the namespace gate, not left to fail at browser
    /// `instantiate`. Stripping to a bare interface name would let it masquerade
    /// as a known surface import; the fully qualified name is what makes the
    /// rejection sound.
    #[test]
    #[should_panic(expected = "from package \"wasi:clocks\"")]
    fn validate_surface_assets_panics_on_foreign_namespace_import() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(
            dir.path(),
            "transplant",
            &["ports", "wasi:clocks/wall-clock"],
            |_| {},
        );
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    #[test]
    #[should_panic(expected = "imports the alert interface, but the surface holds no")]
    fn validate_surface_assets_panics_on_alert_import_without_grant() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "noisy", &["ports", "alert"], |_| {});
        validate_surface_assets(dir.path(), &[resolved_with_processor("deskbar", "noisy")]);
    }

    /// The same `alert`-importing kind passes on a surface that holds the grant:
    /// the profile check is per kind, the grant check per declaring surface.
    #[test]
    fn validate_surface_assets_passes_alert_import_with_grant() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "noisy", &["ports", "alert"], |_| {});
        let mut surface = resolved_with_processor("deskbar", "noisy");
        surface
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::SurfaceAlert);
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "is declared under 2 different ABIs")]
    fn validate_surface_assets_panics_on_kind_under_two_abis() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        // Two surfaces declaring one kind under different ABIs: the collision is
        // caught across the whole config, not just within one surface.
        let dom = resolved("deskbar");
        let mut clash = resolved_with_processor("kiosk", "protobar");
        clash.slug = "kiosk".to_string();
        for comp in &dom.components {
            touch_component_pair(dir.path(), &comp.kind);
        }
        validate_surface_assets(dir.path(), &[dom, clash]);
    }

    #[test]
    #[should_panic(expected = "is not a surface-bindable scheme (brenn:, ephemeral:, or local:)")]
    fn build_panics_on_foreign_scheme() {
        let mut r = resolved("deskbar");
        r.subscriptions[0].channel_address = "mqtt:sensors".to_string();
        SurfaceRuntime::build(
            r,
            empty_bus(),
            None,
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        );
    }

    /// A surface carrying a durable subscription but built with no `Messenger`
    /// is a broken boot invariant (durable projection needs the directory + DB +
    /// durable queries): `build` must panic rather than construct a runtime that
    /// would fail later, deep in `handle_durable_subscribe`.
    #[test]
    #[should_panic(expected = "has durable subscriptions but no Messenger")]
    fn build_panics_on_durable_subscription_without_messenger() {
        use brenn_lib::messaging::WakeMin;
        use brenn_lib::messaging::config::{Depth, NoiseLevel};
        let mut r = resolved("deskbar");
        use brenn_lib::messaging::config::ResolvedSurfaceSubscription;
        r.durable_subscriptions = vec![ResolvedSurfaceSubscription {
            instance: "protobar".to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid::Uuid::new_v4(),
                channel_address: "brenn:alerts".to_string(),
                push_depth: Depth::Bounded(8),
                retain_depth: Depth::Bounded(4),
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
        }];
        SurfaceRuntime::build(
            r,
            empty_bus(),
            None,
            TEST_MAX_BODY_BYTES,
            crate::test_support::surface::description_params(),
        );
    }

    fn directory_with_standing_depth(bare_address: &str, n: u64) -> MessagingDirectory {
        directory_with_standing(
            bare_address,
            Some(brenn_lib::messaging::config::Depth::Bounded(n)),
        )
    }

    /// Frontier exactly at the burst boundary → warns, naming the frontier.
    #[test]
    #[tracing_test::traced_test]
    fn validate_surface_error_channel_warns_at_frontier_boundary() {
        let n = u64::from(brenn_lib::messaging::publish::SURFACE_SEND_BURST);
        let dir = directory_with_standing_depth("surface-errors", n);
        validate_surface_error_channel(
            Some("brenn:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
        assert!(
            logs_contain("eviction frontier is at or below"),
            "frontier == burst must emit the retention warn"
        );
        assert!(
            logs_contain(&format!("frontier={n}")),
            "the warn must name the offending frontier value"
        );
    }

    /// Frontier one above the burst → no warn (a single burst leaves a report).
    #[test]
    #[tracing_test::traced_test]
    fn validate_surface_error_channel_no_warn_above_frontier_boundary() {
        let n = u64::from(brenn_lib::messaging::publish::SURFACE_SEND_BURST) + 1;
        let dir = directory_with_standing_depth("surface-errors", n);
        validate_surface_error_channel(
            Some("brenn:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
        assert!(
            !logs_contain("eviction frontier is at or below"),
            "frontier > burst must not warn"
        );
    }

    /// Default (unbounded) standing depth pins the channel → frontier None → no warn.
    #[test]
    #[tracing_test::traced_test]
    fn validate_surface_error_channel_no_warn_when_pinned() {
        let dir = directory_with("surface-errors");
        validate_surface_error_channel(
            Some("brenn:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
        assert!(
            !logs_contain("eviction frontier is at or below"),
            "a pinned (Unbounded) channel must never warn"
        );
    }

    #[test]
    fn validate_surface_error_channel_noop_when_unset() {
        // Unset channel is a no-op even with no directory (console-only path).
        validate_surface_error_channel(None, None, 1);
    }

    #[test]
    fn validate_surface_error_channel_passes_for_valid_config() {
        // The channel is many-writer by design — the validator no longer sweeps
        // surfaces/apps/wasm for a covering publish path (a surface's injected
        // error-channel ACL is legitimate). A valid brenn: channel that resolves
        // and clears the headroom bound passes.
        let dir = directory_with("surface-errors");
        validate_surface_error_channel(
            Some("brenn:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
    }

    #[test]
    #[should_panic(expected = "not a well-formed brenn: address")]
    fn validate_surface_error_channel_panics_on_foreign_scheme() {
        let dir = directory_with("surface-errors");
        validate_surface_error_channel(
            Some("ephemeral:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
    }

    #[test]
    #[should_panic(expected = "no messaging is configured")]
    fn validate_surface_error_channel_panics_when_messaging_absent() {
        validate_surface_error_channel(
            Some("brenn:surface-errors"),
            None,
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
    }

    #[test]
    #[should_panic(expected = "does not resolve to any declared")]
    fn validate_surface_error_channel_panics_on_undeclared_channel() {
        let dir = directory_with("some-other-channel");
        validate_surface_error_channel(
            Some("brenn:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
    }

    #[test]
    #[should_panic(expected = "below the worst-case surface error report body")]
    fn validate_surface_error_channel_panics_on_insufficient_body_headroom() {
        let dir = directory_with("surface-errors");
        validate_surface_error_channel(
            Some("brenn:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES - 1,
        );
    }
}

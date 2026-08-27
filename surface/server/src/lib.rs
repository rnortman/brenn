//! The server-side surface layer: what a browser-facing surface *is*, resolved
//! once at boot.
//!
//! Config lowers here into a [`SurfaceRuntime`] per surface — the access policy,
//! the messenger it projects through, the derived self-description channel
//! addresses, and the attachment authority (`profile.rs`) the generic session in
//! `brenn-attach-server` enforces against. The boot documents this layer
//! publishes (bindings, self-description) and the asset-tree validation it
//! panics on live beside that lowering, because all of them are pure functions
//! of the same resolved config.
//!
//! Nothing here knows about routing. The route that fronts
//! `GET /surface/{slug}/ws` lives a crate up and hands the runtime it resolved
//! into the attachment session.

pub mod bindings_doc;
pub mod boot_policy;
pub mod description;
pub mod dom_assets;
pub mod processor_assets;
pub mod profile;
pub mod telemetry;

#[cfg(any(test, feature = "testutils"))]
pub mod fixtures_config;
#[cfg(any(test, feature = "testutils"))]
pub mod test_fixtures;

use std::collections::HashMap;
use std::sync::Arc;

use brenn_envelope::{channel_capabilities, is_local_channel};
use brenn_lib::access::AppPolicy;
use brenn_lib::messaging::config::{ResolvedComponent, ResolvedSurface, ResolvedWasmConsumer};
use brenn_lib::messaging::gates::well_formed_name;
use brenn_lib::messaging::{ChannelScheme, MessagingDirectory};
use brenn_messaging::Messenger;
use brenn_messaging::system::SystemParticipantSpec;
use brenn_surface_contract::KERNEL_ARTIFACT;
use brenn_surface_schema::surface_bindable_address;

use self::profile::SurfaceProfile;

/// Maximum concurrent attached WS sessions per surface, across all users.
///
/// Each attached session costs a push queue plus an outbound queue, so an
/// unbounded attach count is an authenticated-user memory
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

/// Idle-heartbeat interval advertised in `Welcome`, in seconds. Shared by both
/// attach routes. Constant in production; test states set 1 for fast
/// integration tests. Carried on `AppState::attach_heartbeat_secs` solely for
/// that test seam.
pub const HEARTBEAT_SECS: u32 = 20;

/// Compiled-in skin registry: skin name → static stylesheet path (served under
/// `/static/`, build-ID-stamped by the page handler).
///
/// A surface's configured `skin` is boot-validated against these keys; the page
/// handler emits a `<link>` to the matched path and stamps `data-skin` on the
/// surface root. Out-of-tree / file-based skin packs are a later extension of
/// this registry, not in this cut.
pub const SKIN_REGISTRY: &[(&str, &str)] = &[
    ("bench", "skins/bench.css"),
    ("foundry", "skins/foundry.css"),
];

/// Skin a surface wears when it omits `skin`.
pub const DEFAULT_SKIN: &str = "bench";

/// Resolve a skin name to its static stylesheet path, or `None` if the name is
/// not in [`SKIN_REGISTRY`].
pub fn skin_stylesheet_path(name: &str) -> Option<&'static str> {
    SKIN_REGISTRY
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, path)| *path)
}

/// Per-surface runtime bundle, precomputed once at boot so the WS hot path does
/// no re-derivation.
pub struct SurfaceRuntime {
    /// The resolved config block for this surface.
    pub resolved: ResolvedSurface,
    /// Resolved access policy, `Arc`-wrapped once for cheap per-op cloning.
    pub policy: Arc<AppPolicy>,
    /// The `Messenger` this surface's messaging projects through — the session
    /// reaches the directory, the DB, durable queries, and the non-durable
    /// channels' live streams via it. `Some` whenever this surface has any
    /// subscription or output (boot invariant); `None` only for test runtimes
    /// that exercise resolution without touching messaging.
    pub messenger: Option<Arc<Messenger>>,
    /// Server publish-body cap (config `messaging.max_body_bytes`): the
    /// `Welcome` field, the dispatch pre-check, and the derived WS read cap.
    pub max_body_bytes: usize,
    /// Surface self-description runtime telemetry: the surface's derived
    /// geometry, status and config channel addresses. Every surface has one.
    pub description: SurfaceDescriptionRuntime,
    /// This surface's component structure lowered to the attachment grain: the
    /// per-channel subscribable fold, the per-attribution publishable sets, the
    /// declared sub-identities, and the parked-view targets. The authority half
    /// of an attachment, in the vocabulary the wire speaks. Behind an `Arc`
    /// because every attachment session of this surface holds it for its life.
    pub profile: Arc<SurfaceProfile>,
}

/// The operator's `[surface_description]` parameters, as [`SurfaceRuntime::build`]
/// consumes them: the namespace the derived channel addresses hang off.
#[derive(Debug, Clone)]
pub struct SurfaceDescriptionParams {
    /// Bare-name namespace rooting every derived channel address.
    pub prefix: String,
}

/// Per-surface derived channel addresses for the surface self-description
/// family, resolved once at boot from [`SurfaceDescriptionParams`] and the
/// surface slug. The page authors the documents that ride the first two; the
/// third is where the server publishes the wiring the page reads.
pub struct SurfaceDescriptionRuntime {
    /// `brenn:<prefix>.surface.<slug>.geometry` — the geometry publish target.
    pub geometry_channel: String,
    /// `brenn:<prefix>.surface.<slug>.status` — the status publish target, and
    /// where the server writes its `disconnected` stamps.
    pub status_channel: String,
    /// `ephemeral:<prefix>.surface.<slug>.bindings` — the config channel this
    /// surface's retained bindings document sits on. Rendered into the page as a
    /// meta, because a client cannot derive it: the prefix is the operator's.
    pub config_channel: String,
}

/// Assert that a channel the wire maps hold is transportable — the one channel
/// characteristic the surface bridge is allowed to see, because only
/// transportable channels cross the websocket at all.
///
/// Panics on any scheme not surface-bindable (anything but `brenn:`/
/// `ephemeral:`/`local:`) and on `local:` itself: `resolve_surfaces` restricted
/// surface bindings to those three, and the wire maps exclude `local:` by
/// construction, so either is a broken boot invariant rather than client input.
/// Past the bindability gate every remaining scheme carries capabilities, so the
/// read itself cannot come up empty.
fn assert_transportable(address: &str) {
    assert!(
        surface_bindable_address(address),
        "surface binding address {address:?} is not a surface-bindable scheme (brenn:, \
         ephemeral:, or local:) — resolve_surfaces should have rejected it at boot"
    );
    let capabilities =
        channel_capabilities(address).expect("every surface-bindable scheme carries capabilities");
    assert!(
        capabilities.transportable,
        "surface binding address {address:?} is not transportable — page-local traffic never \
         crosses the wire, and the wire maps exclude it by construction"
    );
}

impl SurfaceRuntime {
    /// The `Messenger` this surface's messaging projects through.
    ///
    /// # Panics
    ///
    /// If this runtime has none. Every surface that resolves a subscription or
    /// an output has one by boot invariant, so a miss is a broken boot, not a
    /// runtime condition.
    pub fn messenger(&self) -> &Arc<Messenger> {
        self.messenger.as_ref().unwrap_or_else(|| {
            panic!(
                "surface {:?} reached messaging with no Messenger — boot wires one whenever a \
                 surface declares any subscription or output",
                self.resolved.slug
            )
        })
    }

    /// The store's boot counter, stamped into every cursor this surface's
    /// sessions mint.
    ///
    /// A per-process constant the messenger resolved at its own boot, so a
    /// session reads it once at connect rather than querying the database per
    /// `Subscribe`.
    ///
    /// # Panics
    ///
    /// If this runtime has no `Messenger`; see [`SurfaceRuntime::messenger`].
    pub fn store_incarnation(&self) -> i64 {
        self.messenger().store_incarnation()
    }

    /// Build the runtime for one resolved surface.
    ///
    /// Does not validate that output channels exist in the directory — that is
    /// boot's gate (`brenn_messaging_boot::surfaces`).
    ///
    /// `max_body_bytes` is `messaging.max_body_bytes` from config.
    pub fn build(
        resolved: ResolvedSurface,
        messenger: Option<Arc<Messenger>>,
        max_body_bytes: usize,
        description: SurfaceDescriptionParams,
    ) -> Self {
        // Both wire directions need a Messenger — a subscription reads the
        // channel's retention through it, an output publishes through it — so a
        // surface carrying either and built without one is a broken boot
        // invariant, caught here by whoever starts the server. The session-site
        // panics (`handle_subscribe`, `handle_publish`) stay as the
        // defence-in-depth backstop; on their own they are weaker in kind,
        // because the misconfiguration would ship and surface as a broken page
        // in front of a user.
        let carries_wire_binding = resolved
            .subscriptions
            .iter()
            .map(|b| &b.channel_address)
            .chain(resolved.outputs.iter().map(|b| &b.channel_address))
            .any(|address| !is_local_channel(address));
        assert!(
            !carries_wire_binding || messenger.is_some(),
            "surface {:?} has wire bindings but no Messenger — every binding that crosses the \
             websocket reads or writes the bus through one",
            resolved.slug,
        );
        let policy = Arc::new(resolved.policy.clone());

        // `local:` bindings are deliberately absent from every wire-facing
        // lowering: the page routes that traffic itself and must never
        // `Subscribe` to it, so the channel is *unbound* as far as the wire is
        // concerned and a `Subscribe` naming one is the ordinary unbound-channel
        // violation.
        let description = SurfaceDescriptionRuntime {
            geometry_channel: description::surface_geometry_channel(
                &description.prefix,
                &resolved.slug,
            ),
            status_channel: description::surface_status_channel(
                &description.prefix,
                &resolved.slug,
            ),
            config_channel: description::surface_config_channel(
                &description.prefix,
                &resolved.slug,
            ),
        };
        let profile = Arc::new(SurfaceProfile::build(&resolved, &description));

        SurfaceRuntime {
            resolved,
            policy,
            messenger,
            max_body_bytes,
            description,
            profile,
        }
    }
}

/// Build the boot-time surface map: slug → runtime. Empty when no
/// `[[surface]]` blocks are configured.
pub fn build_surface_runtimes(
    surfaces: Vec<ResolvedSurface>,
    messenger: Option<Arc<Messenger>>,
    max_body_bytes: usize,
    error_channel: Option<String>,
    surface_description: SurfaceDescriptionParams,
) -> HashMap<String, Arc<SurfaceRuntime>> {
    surfaces
        .into_iter()
        .map(|resolved| {
            let slug = resolved.slug.clone();
            let mut runtime = SurfaceRuntime::build(
                resolved,
                messenger.clone(),
                max_body_bytes,
                surface_description.clone(),
            );
            // Admit the substrate error channel when one is configured: every
            // declared attribution and the bare identity may report onto it, and
            // it is the one channel whose publish refusals are reported rather
            // than fatal.
            if let Some(channel_address) = &error_channel {
                // The profile is shared with every session of this surface, so it
                // lives behind an `Arc` — still unique here, before the runtime
                // reaches the map any session reads.
                Arc::get_mut(&mut runtime.profile)
                    .expect("the boot-built profile is not shared until the runtime is published")
                    .bind_error_channel(channel_address);
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
/// wasm-bindgen module pair, packaged specification and the record binding them
/// (`dom_assets`), a `processor` kind its transpiled tree plus a conforming
/// manifest and import profile (`processor_assets`). A missing or stale artifact
/// is a deploy/packaging mistake — config-shaped, boot-time, never
/// attacker-reachable — so this panics (house fail-fast policy). No-op when no
/// surfaces are configured.
///
/// The kernel keeps a bare pair-existence check: it is not a component, so it
/// has no kind, no class and no specification to bind — nothing to record.
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
    // Kind-grain checks (asset existence, record, profile) run once per distinct
    // kind across the whole config — several instances, on one surface or
    // several, share one artifact. Import⊆grants and the specification binding
    // are per instance, so what those need is kept for the second pass.
    let mut kinds: HashMap<&str, KindAssets> = HashMap::new();
    for surface in surfaces {
        for comp in &surface.components {
            if kinds.contains_key(comp.kind.as_str()) {
                continue;
            }
            // The one place an ABI selects a record shape: a dom kind's record
            // sits flat beside its module pair, a processor kind's inside its
            // transpile directory. Downstream reads the assets, not the ABI.
            let assets = match comp.abi {
                brenn_surface_schema::Abi::Dom => {
                    let manifest = dom_assets::validate_dom_kind(surface_dist_dir, &comp.kind);
                    KindAssets {
                        spec_sha256: manifest.spec_sha256,
                        processor: None,
                    }
                }
                brenn_surface_schema::Abi::Processor => {
                    let manifest =
                        processor_assets::validate_processor_kind(surface_dist_dir, &comp.kind);
                    KindAssets {
                        spec_sha256: manifest.spec_sha256.clone(),
                        processor: Some(manifest),
                    }
                }
                // `resolve_abi` rejects the reserved ABIs at config resolution,
                // so no resolved component can carry one.
                brenn_surface_schema::Abi::DomTs | brenn_surface_schema::Abi::Html => unreachable!(
                    "reserved abi {:?} resolved for component {:?} — resolve_abi must reject it",
                    comp.abi, comp.instance,
                ),
            };
            kinds.insert(comp.kind.as_str(), assets);
        }
    }
    // Sibling instances of one kind may hold different grants, and each carries
    // its own class's hash — the kind fold admits comment-divergent class
    // copies, so both questions are asked once per declaration rather than once
    // per kind.
    for surface in surfaces {
        for comp in &surface.components {
            let assets = kinds.get(comp.kind.as_str()).unwrap_or_else(|| {
                // The kind-grain pass above visited every configured kind, so a
                // missing entry is this function's own bug, not a tree state.
                unreachable!(
                    "component {:?} of surface {:?} names kind {:?}, which the kind-grain pass \
                     did not record",
                    comp.instance, surface.slug, comp.kind,
                )
            });
            if let Some(manifest) = &assets.processor {
                processor_assets::assert_imports_granted(
                    &surface.slug,
                    &comp.instance,
                    &comp.kind,
                    manifest,
                    &comp.grants,
                );
            }
            assert_spec_bound(&surface.slug, comp, &assets.spec_sha256);
        }
    }
}

/// What one validated component kind's installed assets tell the per-instance
/// pass, whichever ABI they arrived in: the specification hash every instance of
/// the kind is bound to, and — for a processor kind — the record carrying the
/// reflected import profile the grants must cover. A dom bundle has no
/// component-model reflection, so it has no profile to check.
struct KindAssets {
    spec_sha256: String,
    processor: Option<processor_assets::ProcessorManifest>,
}

/// Bind one configured instance to the specification its kind's installed
/// artifacts were built against.
///
/// Byte equality, not a comparison of facts: the configuration compiled against
/// exactly these bytes, so equality carries the fit check, the port optionality,
/// the doctypes and the ABI over to the installed tree in one step.
///
/// The record shapes carrying the packaged hash and the reasoning behind the
/// backend twin of this check are documented in `docs/component-packages.md`.
///
/// # Panics
///
/// On an empty configured hash — the class fact is `serde(skip)` in the
/// document layer, so a lowering that stopped filling it must fail loudly
/// rather than match anything — and on a hash that is not the packaged one.
fn assert_spec_bound(slug: &str, comp: &ResolvedComponent, packaged: &str) {
    assert!(
        !comp.spec_sha256.is_empty(),
        "boot: [[surface]] {slug:?} component {:?} carries no specification hash — the class fact \
         is filled at lowering, so an empty one would match nothing a record can carry and this \
         is a lowering bug, not a deployment state. Refusing to start (fail-fast on invalid \
         config).",
        comp.instance,
    );
    assert!(
        comp.spec_sha256 == packaged,
        "boot: [[surface]] {slug:?} component {:?} of kind {:?} was configured against a \
         specification that hashes to {}, but the installed surface assets for that kind were \
         built against one that hashes to {packaged}. The author's specification travels with the \
         component; a deployment's copy of it is verbatim. Re-copy the specification from the \
         release that carries these assets, or install the release the configuration was written \
         for. Refusing to start (fail-fast on invalid config).",
        comp.instance,
        comp.kind,
        comp.spec_sha256,
    );
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
    * (brenn_surface_schema::MAX_LOG_MESSAGE_BYTES + brenn_surface_schema::MAX_LOG_SOURCE_BYTES)
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
        && frontier <= u64::from(brenn_messaging::publish::SURFACE_SEND_BURST)
    {
        let refill_window_secs = u64::from(brenn_messaging::publish::SURFACE_SEND_BURST)
            * brenn_messaging::publish::SURFACE_SEND_REFILL.as_secs();
        tracing::warn!(
            channel,
            frontier,
            burst = brenn_messaging::publish::SURFACE_SEND_BURST,
            refill_window_secs,
            "boot: [observability] surface_error_channel eviction frontier is at or below the \
             surface send burst — one admitted burst can rotate every earlier report out of \
             the channel, and the budget fully refills within the window. Evicted reports still \
             survive the kernel's console copy. Raise the channel's standing_retain_depth above \
             the burst to close the window."
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
pub(crate) enum ExpectedWriter<'a> {
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

/// Sweep every publisher class for a covering path onto a single-writer channel,
/// panicking (boot fail-fast) on any principal other than `expected` that could
/// write it. Used by the surface self-description validator, which runs it once
/// per derived channel — the boot-published help/schema/index channels are
/// single-writer under `system:surface-help`, each surface's config channel under
/// `system:surface-config`, and each runtime geometry/status channel under its
/// owning surface — so the "which classes can publish" checklist lives in exactly
/// one place.
///
/// `bare` is the scheme-stripped channel name; `channel` the full address (both
/// only for the panic messages and the ACL-coverage check). The sweep covers
/// surface + WASM output bindings (exact-address) and the resolved-policy ACL
/// coverage (Exact or accidental-broad Prefix) in the channel scheme's own
/// publish family over the app map, WASM consumers, surfaces, and the collected
/// system-participant specs.
///
/// `expected` names the one principal permitted to write the channel; it is
/// excluded from its own class's sweep (the system participant by component name,
/// or the owning surface by slug). Every other principal in every class is swept
/// with no exception.
pub(crate) fn assert_channel_single_writer(
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
    // matcher in its scheme's publish family (Exact or Prefix — the
    // accidental-broad-prefix case) is a forgery path. Catches ACL coverage the
    // exact-address binding checks above never see. The owning surface (for a
    // `Surface` expected writer) is excluded from the surface sweep — its
    // geometry/status grant is the sanctioned single-writer coverage; every other
    // principal is swept.
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

/// Panic if `policy` holds a publish path covering `bare` (the scheme-stripped
/// channel name) in the ACL family the channel's own scheme is gated by — the
/// single-writer forgery guard. The message names the offending principal
/// (`kind` + `slug`), the covering matcher list to narrow, and the channel, so an
/// operator can remediate without reading the code.
///
/// Scheme-matched rather than `brenn_publish`-only: a channel is gated by the
/// family its scheme dispatches to, so reading any other family would sweep
/// grants that cannot reach the channel while missing the ones that can. An
/// `ephemeral_publish` matcher covering a single-writer `ephemeral:` channel is
/// exactly the forgery path this guard exists to make boot-impossible.
///
/// # Panics
///
/// On any scheme but `brenn:` and `ephemeral:`. Single-writer channels are
/// derived addresses in those two families only; anything else is a host bug.
fn assert_no_covering_publish(
    kind: &str,
    slug: &str,
    policy: &AppPolicy,
    bare: &str,
    channel: &str,
    expected_desc: &str,
) {
    let scheme = ChannelScheme::of(channel).unwrap_or_else(|| {
        panic!("single-writer channel {channel:?} carries no recognized scheme — host bug")
    });
    let (covers, family, matchers) = match scheme {
        ChannelScheme::Brenn => (
            policy.allows_brenn_publish(bare),
            "brenn_publish",
            format!("{:?}", policy.acls.brenn_publish),
        ),
        ChannelScheme::Ephemeral => (
            policy.allows_ephemeral_publish(bare),
            "ephemeral_publish",
            format!("{:?}", policy.acls.ephemeral_publish),
        ),
        other => panic!(
            "single-writer channel {channel:?} is on scheme {} — the derived single-writer \
             families are brenn: and ephemeral: only; host bug",
            other.as_str(),
        ),
    };
    assert!(
        !covers,
        "boot: {kind} {slug:?} holds a {family} ACL covering single-writer channel {channel:?} \
         (matchers: {matchers}) — only {expected_desc} may write it, so any other covering grant \
         is a forgery path. Narrow the ACL, drop the publish grant, or rename the channel. \
         Refusing to start (fail-fast on invalid config).",
    );
}

/// Assert both halves of a wasm-bindgen `--target web` module — the `.js` loader
/// and its `_bg.wasm` sibling — exist under `dir`. `what` labels the module in
/// the panic message (e.g. `"kernel"` or `"component \"echo-stub\""`).
fn assert_module_pair_exists(dir: &std::path::Path, js_artifact: &str, what: &str) {
    let wasm_artifact = brenn_surface_contract::module_wasm_sibling(js_artifact);
    for artifact in [js_artifact, wasm_artifact.as_str()] {
        let path = dir.join(artifact);
        assert!(
            path.exists(),
            "boot: {what} surface asset {artifact} missing at {} — surface assets are not \
             built/deployed (run `make build`; on deploy ensure surface_dist_dir is \
             populated). Refusing to start (fail-fast on invalid config).",
            path.display(),
        );
    }
}

#[cfg(test)]
mod tests {
    use brenn_lib::messaging::Urgency;
    use brenn_lib::messaging::config::{
        ResolvedComponent, ResolvedSubscription, ResolvedSurface, ResolvedSurfaceSubscription,
        SurfaceBinding, SurfaceOutput,
    };

    use super::test_fixtures::{TEST_MAX_BODY_BYTES, directory_with, directory_with_standing};
    use super::*;
    use brenn_attach_server::profile::AttachProfile;
    use brenn_attach_server::profile::SubscriptionFacts;
    use brenn_messaging::testutils::empty_directory_messenger;
    use brenn_surface_contract::module_artifact;

    /// The bindings document this surface's resolved config lowers to, under the
    /// boot parameters the disconnected-stamp fixtures already use.
    fn document(resolved: &ResolvedSurface) -> brenn_surface_schema::bindings::BindingsDocument {
        super::bindings_doc::build_bindings_document(
            resolved,
            &super::bindings_doc::BindingsDocParams {
                prefix: "surface",
                status_interval_secs: 60,
                error_report: None,
            },
        )
    }

    fn resolved(slug: &str) -> ResolvedSurface {
        ResolvedSurface {
            slug: slug.to_string(),
            skin: "bench".to_string(),
            // The hashes are the ones the fixture trees package, so these
            // instances bind rather than merely resolve.
            components: vec![
                ResolvedComponent {
                    spec_sha256: fixture_spec_hash("protobar"),
                    chrome: true,
                    ..ResolvedComponent::minimal(
                        "protobar",
                        "protobar",
                        brenn_surface_schema::Abi::Dom,
                    )
                },
                ResolvedComponent {
                    spec_sha256: fixture_spec_hash("writer"),
                    ..ResolvedComponent::minimal("writer", "writer", brenn_surface_schema::Abi::Dom)
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
            wire_subscriptions: vec![ResolvedSurfaceSubscription {
                instance: "protobar".to_string(),
                subscription: ResolvedSubscription {
                    channel_uuid: uuid::Uuid::nil(),
                    channel_address: "ephemeral:protobar-demo".to_string(),
                    push_depth: brenn_lib::messaging::config::Depth::Bounded(8),
                    retain_depth: brenn_lib::messaging::config::Depth::Bounded(4),
                    noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                    wake_min: brenn_lib::messaging::WakeMin::Normal,
                },
            }],
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
    fn build_lowers_the_runtime_and_its_wiring() {
        let rt = SurfaceRuntime::build(
            resolved("deskbar"),
            Some(empty_directory_messenger("test")),
            TEST_MAX_BODY_BYTES,
            crate::fixtures_config::description_params(),
        );

        assert_eq!(rt.profile.attacher().as_str(), "surface:deskbar");
        assert_eq!(rt.max_body_bytes, TEST_MAX_BODY_BYTES);
        // The wire subscription's depths (push 8, retain 4), not the
        // per-binding numbers (retain 0).
        assert_eq!(
            rt.profile.subscribable("ephemeral:protobar-demo"),
            Some(SubscriptionFacts {
                push_depth: 8,
                retain_depth: 4,
            })
        );
        assert!(rt.profile.publishable(Some("writer"), "brenn:writer-out"));

        let bindings = document(&resolved("deskbar"));
        let comp_pairs: Vec<(&str, &str)> = bindings
            .components
            .iter()
            .map(|c| (c.instance.as_str(), c.kind.as_str()))
            .collect();
        assert_eq!(
            comp_pairs,
            vec![("protobar", "protobar"), ("writer", "writer")]
        );
        assert_eq!(bindings.subscriptions.len(), 1);
        assert_eq!(bindings.subscriptions[0].channel, "ephemeral:protobar-demo");
        assert_eq!(bindings.subscriptions[0].port, "messages");
        assert_eq!(bindings.outputs.len(), 1);
        assert_eq!(bindings.outputs[0].channel, "brenn:writer-out");
        assert_eq!(bindings.chrome_instance, "protobar");
    }

    /// The lowering names the resolved chrome instance — the singleton the page
    /// treats specially. One field, populated from the component that sets
    /// `chrome`.
    #[test]
    fn the_lowering_names_the_chrome_instance() {
        let mut resolved = resolved("deskbar");
        // Move the chrome designation off the default (protobar) onto writer, so
        // the assertion proves the field tracks the marked component, not the
        // first one.
        resolved.components[0].chrome = false;
        resolved.components[1].chrome = true;
        assert_eq!(document(&resolved).chrome_instance, "writer");
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

    /// The invariant that keeps `local:` off the wire, checked at the two places
    /// it is enforced: a local binding rides the bindings document (the page
    /// needs its wiring) but is absent from the attachment's own authority. That
    /// absence is what makes a `Subscribe`/`Publish` naming it fall into the
    /// unbound-channel violation arms rather than reaching the bus.
    #[test]
    fn local_bindings_are_lowered_but_never_reach_the_attachments_authority() {
        let resolved = resolved_with_local("deskbar");
        let bindings = document(&resolved);
        let rt = SurfaceRuntime::build(
            resolved,
            Some(empty_directory_messenger("test")),
            TEST_MAX_BODY_BYTES,
            crate::fixtures_config::description_params(),
        );

        assert!(
            bindings
                .subscriptions
                .iter()
                .any(|b| b.channel == "local:page-bus" && b.port == "local-in")
        );
        assert!(
            bindings
                .outputs
                .iter()
                .any(|b| b.channel == "local:page-bus" && b.port == "local-out")
        );
        assert_eq!(
            bindings.local_channels,
            vec![brenn_surface_schema::LocalChannel {
                channel: "local:page-bus".to_string(),
                ring_depth: 3,
            }]
        );

        // Unbound on the wire, in both directions.
        assert_eq!(rt.profile.subscribable("local:page-bus"), None);
        assert!(!rt.profile.publishable(Some("writer"), "local:page-bus"));
        // The non-local bindings on the same surface are unaffected: the filter
        // excludes the scheme, not the surface.
        assert!(rt.profile.subscribable("ephemeral:protobar-demo").is_some());
        assert!(rt.profile.publishable(Some("writer"), "brenn:writer-out"));
    }

    /// A surface with no local wiring lowers an empty router table — not a
    /// missing field the page has to treat as unknown.
    #[test]
    fn the_lowering_carries_no_local_channels_when_none_are_declared() {
        assert!(document(&resolved("deskbar")).local_channels.is_empty());
    }

    #[test]
    fn build_surface_runtimes_keys_by_slug() {
        let map = build_surface_runtimes(
            vec![resolved("deskbar"), resolved("kitchen")],
            Some(empty_directory_messenger("test")),
            TEST_MAX_BODY_BYTES,
            None,
            crate::fixtures_config::description_params(),
        );

        assert_eq!(map.len(), 2);
        assert!(map.contains_key("deskbar"));
        assert!(map.contains_key("kitchen"));
    }

    #[test]
    fn build_surface_runtimes_empty_for_surfaceless_config() {
        let map = build_surface_runtimes(
            vec![],
            None,
            TEST_MAX_BODY_BYTES,
            None,
            crate::fixtures_config::description_params(),
        );
        assert!(map.is_empty());
    }

    /// With an error channel configured, every surface's attachment authority
    /// admits it — under the bare identity and under every declared
    /// sub-identity, since a report carries the identity of whoever failed.
    #[test]
    fn build_surface_runtimes_binds_the_error_channel_to_every_attribution() {
        let map = build_surface_runtimes(
            vec![resolved("deskbar")],
            Some(empty_directory_messenger("test")),
            TEST_MAX_BODY_BYTES,
            Some("brenn:surface-errors".to_string()),
            crate::fixtures_config::description_params(),
        );
        let profile = &map["deskbar"].profile;
        assert!(profile.publishable(None, "brenn:surface-errors"));
        assert!(profile.publishable(Some("writer"), "brenn:surface-errors"));
        assert!(profile.publishable(Some("protobar"), "brenn:surface-errors"));
        // The one channel whose publish refusals are reported rather than fatal.
        assert_eq!(
            profile.publish_posture("brenn:surface-errors"),
            brenn_attach_server::profile::PublishPosture::Diagnostic
        );
    }

    /// Unset error channel: nothing may report anywhere, and no channel carries
    /// the diagnostics posture.
    #[test]
    fn build_surface_runtimes_binds_no_error_channel_when_none_is_configured() {
        let map = build_surface_runtimes(
            vec![resolved("deskbar")],
            Some(empty_directory_messenger("test")),
            TEST_MAX_BODY_BYTES,
            None,
            crate::fixtures_config::description_params(),
        );
        let profile = &map["deskbar"].profile;
        assert!(!profile.publishable(None, "brenn:surface-errors"));
        assert!(!profile.publishable(Some("writer"), "brenn:surface-errors"));
        assert_eq!(
            profile.publish_posture("brenn:surface-errors"),
            brenn_attach_server::profile::PublishPosture::Invariant
        );
    }

    fn touch(dir: &std::path::Path, name: &str) {
        std::fs::write(dir.join(name), b"").expect("write test artifact");
    }

    fn write_kernel_pair(dir: &std::path::Path) {
        touch(dir, "brenn_surface_kernel.js");
        touch(dir, "brenn_surface_kernel_bg.wasm");
    }

    /// Write a conforming dom kind: the module pair, the packaged
    /// specification, and a record whose three hashes actually hash those
    /// bytes. `tweak` edits the record before serialization, so each failure
    /// test is one perturbation of an otherwise valid tree.
    fn write_dom_kind(
        dist: &std::path::Path,
        kind: &str,
        tweak: impl FnOnce(&mut serde_json::Value),
    ) {
        let module_bytes = format!("export function init() {{}} // {kind}\n").into_bytes();
        let wasm_bytes = format!("wasm-bytes-for-{kind}").into_bytes();
        let spec_bytes = spec_bytes_for(kind);

        // The names come from the contract's dom file grammar, the same source
        // the code under test derives them from.
        let module = module_artifact(kind);
        let module_wasm = brenn_surface_contract::module_wasm_artifact(kind);
        let spec = brenn_surface_contract::dom_spec_artifact(kind);
        std::fs::write(dist.join(&module), &module_bytes).expect("write module");
        std::fs::write(dist.join(&module_wasm), &wasm_bytes).expect("write module wasm");
        std::fs::write(dist.join(&spec), &spec_bytes).expect("write spec");

        use sha2::Digest as _;
        let mut record = serde_json::json!({
            "v": 1,
            "kind": kind,
            "module": module,
            "module_sha256": hex::encode(sha2::Sha256::digest(&module_bytes)),
            "module_wasm": module_wasm,
            "module_wasm_sha256": hex::encode(sha2::Sha256::digest(&wasm_bytes)),
            "spec": spec,
            "spec_sha256": hex::encode(sha2::Sha256::digest(&spec_bytes)),
        });
        tweak(&mut record);
        std::fs::write(
            dom_assets::record_path(dist, kind),
            serde_json::to_vec_pretty(&record).expect("serialize dom record"),
        )
        .expect("write dom record");
    }

    /// The conforming tree, unperturbed.
    fn write_valid_dom_kind(dir: &std::path::Path, kind: &str) {
        write_dom_kind(dir, kind, |_| {});
    }

    #[test]
    fn validate_surface_assets_noop_when_no_surfaces() {
        // Empty surface list is a no-op even against a nonexistent directory:
        // the check only guards surfaces that actually exist.
        validate_surface_assets(std::path::Path::new("/nonexistent/surface/dist"), &[]);
    }

    #[test]
    fn validate_surface_assets_passes_with_all_dom_records_present() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_dom_kind(dir.path(), &comp.kind);
        }
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "kernel surface asset brenn_surface_kernel.js missing")]
    fn validate_surface_assets_panics_on_missing_kernel_pair() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_dom_kind(dir.path(), &comp.kind);
        }
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "dom component \"writer\" has no readable asset record")]
    fn validate_surface_assets_panics_on_missing_component_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let surface = resolved("deskbar");
        write_valid_dom_kind(dir.path(), &surface.components[0].kind);
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// The dom fixture: the kernel pair, a conforming record for the first
    /// configured dom kind, and `tweak` applied to `writer`'s record alone —
    /// so a failing test states exactly one divergence and the sibling kind
    /// proves the check is not refusing everything.
    fn dom_fixture(
        dir: &std::path::Path,
        tweak: impl FnOnce(&mut serde_json::Value),
    ) -> ResolvedSurface {
        write_kernel_pair(dir);
        let surface = resolved("deskbar");
        write_valid_dom_kind(dir, &surface.components[0].kind);
        write_dom_kind(dir, &surface.components[1].kind, tweak);
        surface
    }

    #[test]
    #[should_panic(expected = "dom component \"writer\" asset record at")]
    fn validate_surface_assets_panics_on_unknown_dom_record_field() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = dom_fixture(dir.path(), |record| {
            record["snippets_sha256"] = serde_json::json!("00");
        });
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "asset record declares v = 2")]
    fn validate_surface_assets_panics_on_wrong_dom_record_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = dom_fixture(dir.path(), |record| {
            record["v"] = serde_json::json!(2);
        });
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "carries a record for kind \"protobar\"")]
    fn validate_surface_assets_panics_on_dom_record_kind_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = dom_fixture(dir.path(), |record| {
            record["kind"] = serde_json::json!("protobar");
        });
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "asset record names its module \"brenn_other.js\"")]
    fn validate_surface_assets_panics_on_dom_record_stated_module_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = dom_fixture(dir.path(), |record| {
            record["module"] = serde_json::json!("brenn_other.js");
        });
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "asset record names its spec \"brenn_writer.brenn\"")]
    fn validate_surface_assets_panics_on_dom_record_stated_spec_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = dom_fixture(dir.path(), |record| {
            record["spec"] = serde_json::json!("brenn_writer.brenn");
        });
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "surface asset brenn_writer.js is unreadable")]
    fn validate_surface_assets_panics_on_missing_dom_module_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = dom_fixture(dir.path(), |_| {});
        std::fs::remove_file(dir.path().join("brenn_writer.js")).expect("remove module");
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "has a stale module: brenn_writer.js hashes to")]
    fn validate_surface_assets_panics_on_dom_module_hash_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = dom_fixture(dir.path(), |_| {});
        // The record is written from this release's bytes; the file is from
        // another. Editing the file rather than the record is the shape a
        // half-synced deploy actually takes.
        std::fs::write(
            dir.path().join("brenn_writer.js"),
            b"export function init() {}\n",
        )
        .expect("rewrite module");
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "has a stale module_wasm: brenn_writer_bg.wasm hashes to")]
    fn validate_surface_assets_panics_on_dom_module_wasm_hash_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = dom_fixture(dir.path(), |_| {});
        std::fs::write(dir.path().join("brenn_writer_bg.wasm"), b"stale").expect("rewrite wasm");
        validate_surface_assets(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "has a stale spec: brenn_writer.spec.brenn hashes to")]
    fn validate_surface_assets_panics_on_dom_spec_hash_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = dom_fixture(dir.path(), |_| {});
        std::fs::write(
            dir.path().join("brenn_writer.spec.brenn"),
            b"// a specification from some other release\n",
        )
        .expect("rewrite spec");
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// A surface whose sole component is a `processor` of `kind`, with no
    /// bindings — asset validation reads only the component list and the policy.
    fn resolved_with_processor(slug: &str, kind: &str) -> ResolvedSurface {
        let mut surface = resolved(slug);
        surface.components = vec![ResolvedComponent {
            spec_sha256: fixture_spec_hash(kind),
            // Every fixture tree imports `ports`; the grant that answers it is
            // the fixture's baseline, so a test perturbs one import at a time.
            grants: [brenn_lib::messaging::ComponentGrant::Ports].into(),
            ..ResolvedComponent::minimal(
                &format!("{kind}-1"),
                kind,
                brenn_surface_schema::Abi::Processor,
            )
        }];
        surface.subscriptions = vec![];
        surface.outputs = vec![];
        surface
    }

    /// Write a conforming transpiled tree for `kind`: a stand-in component
    /// artifact, one transpiled file, a stand-in packaged specification, and a
    /// manifest whose `source_sha256` and `spec_sha256` actually hash those
    /// bytes. `imports` and any manifest edits are applied by the caller through
    /// `tweak` before serialization, so each failure test perturbs exactly one
    /// field of an otherwise valid tree.
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
        write_processor_tree_from_bytes(
            dist,
            kind,
            &component_bytes,
            &spec_bytes_for(kind),
            qualified,
            true,
            tweak,
        );
    }

    /// A stand-in authored specification for `kind`. Boot validation binds
    /// hashes, never parses the document, so a per-kind byte string is a
    /// faithful stand-in and keeps two kinds' specifications distinguishable.
    fn spec_bytes_for(kind: &str) -> Vec<u8> {
        format!("// specification for {kind}\n").into_bytes()
    }

    /// What a configured instance of `kind` carries as its class hash when the
    /// configuration was compiled against the very bytes the fixture tree
    /// packages — the bound case, computed rather than pasted.
    fn fixture_spec_hash(kind: &str) -> String {
        brenn_lib::util::sha256_hex(&spec_bytes_for(kind))
    }

    /// The one place test code constructs a deployed processor tree and its
    /// manifest schema. `component_bytes` are the shipped artifact (a stand-in
    /// string for the synthetic tests, real artifact bytes for the real-artifact
    /// test), `spec_bytes` the packaged specification, `imports` the profile
    /// verbatim, and `with_module` controls whether a stand-in transpiled
    /// `<kind>.js` is written and listed.
    fn write_processor_tree_from_bytes(
        dist: &std::path::Path,
        kind: &str,
        component_bytes: &[u8],
        spec_bytes: &[u8],
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

        // The build stages the specification before the emitter's file walk, so
        // the record lists it like any other staged file.
        let spec_name = format!("{kind}.spec.brenn");
        std::fs::write(dir.join(&spec_name), spec_bytes).expect("write spec");
        files.push(spec_name.clone());

        use sha2::Digest as _;
        let mut manifest = serde_json::json!({
            "v": 2,
            "kind": kind,
            "source_sha256": hex::encode(sha2::Sha256::digest(component_bytes)),
            "jco_version": PINNED_JCO_VERSION_FOR_TESTS,
            "spec": spec_name,
            "spec_sha256": hex::encode(sha2::Sha256::digest(spec_bytes)),
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
        let mut surface = resolved_with_processor("deskbar", "transplant");
        surface.components[0].grants = [
            brenn_lib::messaging::ComponentGrant::Ports,
            brenn_lib::messaging::ComponentGrant::Log,
            brenn_lib::messaging::ComponentGrant::Config,
        ]
        .into();
        validate_surface_assets(dir.path(), &[surface]);
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
    #[should_panic(expected = "manifest declares v = 3")]
    fn validate_surface_assets_panics_on_processor_manifest_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            m["v"] = serde_json::json!(3);
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
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            m["source_sha256"] = serde_json::json!("00".repeat(32));
        });
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    /// The packaged specification is what the configuration's own copy is bound
    /// to, so a tree that lost it cannot answer the binding question at all. It
    /// is staged before the emitter walks the tree, so the record lists it and
    /// the file-set check is what reports its absence.
    #[test]
    #[should_panic(expected = "manifest lists \"transplant.spec.brenn\", which is missing")]
    fn validate_surface_assets_panics_on_missing_processor_spec_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports"], |_| {});
        std::fs::remove_file(
            processor_assets::kind_dir(dir.path(), "transplant").join("transplant.spec.brenn"),
        )
        .expect("remove the packaged spec");
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    /// A record that both omits the specification from its file list and ships
    /// without it: the file-set check has nothing to say, and the binding check
    /// refuses on its own rather than reading a hash it cannot verify.
    #[test]
    #[should_panic(expected = "packaged specification transplant.spec.brenn is unreadable")]
    fn validate_surface_assets_panics_on_unlisted_missing_processor_spec() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            let files = m["files"].as_array_mut().expect("files is an array");
            files.retain(|f| f != "transplant.spec.brenn");
        });
        std::fs::remove_file(
            processor_assets::kind_dir(dir.path(), "transplant").join("transplant.spec.brenn"),
        )
        .expect("remove the packaged spec");
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    /// A record whose spec hash is not the packaged file's: the tree was
    /// assembled from mismatched parts, or the copy was edited in place.
    #[test]
    #[should_panic(expected = "has a specification that does not match its record")]
    fn validate_surface_assets_panics_on_processor_spec_hash_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            m["spec_sha256"] = serde_json::json!("00".repeat(32));
        });
        validate_surface_assets(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    /// The record states the specification it hashed and the kind derives that
    /// name; a stated name the kind does not derive is emitter drift, and is
    /// diagnosed as such rather than as a missing file.
    #[test]
    #[should_panic(expected = "names its specification \"elsewhere.spec.brenn\"")]
    fn validate_surface_assets_panics_on_processor_spec_name_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            m["spec"] = serde_json::json!("elsewhere.spec.brenn");
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
        // Workspace-relative: a test target's runfiles tree is laid out like the
        // workspace, and this crate's directory is not on the path to the
        // staged fixture.
        let artifact =
            std::path::Path::new("brenn-wasm/target/components/brenn_processor_store_rt.wasm");
        assert!(
            artifact.exists(),
            "the real store-rt component artifact is missing at {} — build it with \
             the component artifacts",
            artifact.display(),
        );

        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());

        let kind = "store-rt";
        let bytes = std::fs::read(artifact).expect("read the real component artifact");
        write_processor_tree_from_bytes(
            dir.path(),
            kind,
            &bytes,
            &spec_bytes_for(kind),
            brenn_wasm::processor_component_imports(artifact),
            false,
            |_| {},
        );

        validate_surface_assets(dir.path(), &[resolved_with_processor("deskbar", kind)]);
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

    /// Import⊆grants, the surface twin of the backend linker's deny-by-default:
    /// jco hands a transpiled processor every surface import whatever the config
    /// said, so an import the operator never granted is caught here instead.
    #[test]
    #[should_panic(expected = "imports the alert interface, but \"alert\" is not in the")]
    fn validate_surface_assets_panics_on_an_ungranted_import() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "noisy", &["ports", "alert"], |_| {});
        validate_surface_assets(dir.path(), &[resolved_with_processor("deskbar", "noisy")]);
    }

    /// The same kind passes once the instance holds the grant its imports name.
    #[test]
    fn validate_surface_assets_passes_a_granted_import() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "noisy", &["ports", "alert"], |_| {});
        let mut surface = resolved_with_processor("deskbar", "noisy");
        surface.components[0]
            .grants
            .insert(brenn_lib::messaging::ComponentGrant::Alert);
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// The assert is per instance, not per kind: the module is shared, the
    /// instantiation and its imports are not, so a granted sibling does not
    /// cover an ungranted one.
    #[test]
    #[should_panic(expected = "component \"noisy-2\" runs processor kind \"noisy\"")]
    fn one_instances_grant_does_not_cover_its_sibling() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "noisy", &["ports", "alert"], |_| {});
        let mut surface = resolved_with_processor("deskbar", "noisy");
        surface.components[0]
            .grants
            .insert(brenn_lib::messaging::ComponentGrant::Alert);
        let mut sibling = surface.components[0].clone();
        sibling.instance = "noisy-2".to_string();
        sibling.grants = [brenn_lib::messaging::ComponentGrant::Ports].into();
        surface.components.push(sibling);
        validate_surface_assets(dir.path(), &[surface]);
    }

    // -----------------------------------------------------------------------
    // The specification binding, instance grain, both ABIs. The kind-grain
    // checks above prove the tree is internally consistent; these prove the
    // configuration was written against the tree that is installed.
    // -----------------------------------------------------------------------

    /// A dom instance whose class hash is not the one its kind's installed
    /// assets were built against — the comment-divergent copy the kind fold
    /// legally admits at compile time, refused here.
    #[test]
    #[should_panic(
        expected = "component \"writer\" of kind \"writer\" was configured against a specification"
    )]
    fn validate_surface_assets_panics_on_divergent_dom_instance_spec() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let mut surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_dom_kind(dir.path(), &comp.kind);
        }
        surface.components[1].spec_sha256 =
            brenn_lib::util::sha256_hex(b"// specification for writer, plus a note\n");
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// The processor twin: same refusal, same words, the other carrier.
    #[test]
    #[should_panic(
        expected = "component \"transplant-1\" of kind \"transplant\" was configured against a \
                    specification"
    )]
    fn validate_surface_assets_panics_on_divergent_processor_instance_spec() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports"], |_| {});
        let mut surface = resolved_with_processor("deskbar", "transplant");
        surface.components[0].spec_sha256 =
            brenn_lib::util::sha256_hex(b"// specification for transplant, plus a note\n");
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// Sibling instances of one kind are bound one at a time: a conforming
    /// sibling does not carry a divergent one past the check.
    #[test]
    #[should_panic(expected = "component \"protobar-2\" of kind \"protobar\"")]
    fn one_instances_specification_does_not_cover_its_sibling() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let mut surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_dom_kind(dir.path(), &comp.kind);
        }
        let mut sibling = surface.components[0].clone();
        sibling.instance = "protobar-2".to_string();
        sibling.spec_sha256 = brenn_lib::util::sha256_hex(b"another copy\n");
        surface.components.push(sibling);
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// The `serde(skip)` backstop: the class fact is filled at lowering, so an
    /// empty hash is a lowering bug and must not be read as "matches anything".
    #[test]
    #[should_panic(expected = "component \"writer\" carries no specification hash")]
    fn validate_surface_assets_panics_on_empty_dom_instance_spec_hash() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let mut surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_dom_kind(dir.path(), &comp.kind);
        }
        surface.components[1].spec_sha256 = String::new();
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// The processor twin of the backstop.
    #[test]
    #[should_panic(expected = "component \"transplant-1\" carries no specification hash")]
    fn validate_surface_assets_panics_on_empty_processor_instance_spec_hash() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports"], |_| {});
        let mut surface = resolved_with_processor("deskbar", "transplant");
        surface.components[0].spec_sha256 = String::new();
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// The kernel is not a component: it has no kind, no class and no
    /// specification, so nothing binds it. Asserted by planting a kernel record
    /// and a kernel specification that are internally *false* — the record's
    /// hashes match none of the bytes beside it — and requiring validation to
    /// pass anyway. A change that started reading the kernel's record would
    /// fail here, which an absence assertion could not do.
    #[test]
    fn the_kernel_is_not_bound_to_any_specification() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let stem = "brenn_surface_kernel";
        std::fs::write(
            dir.path().join(format!("{stem}.spec.brenn")),
            b"// not a specification the kernel has\n",
        )
        .expect("write kernel spec");
        std::fs::write(
            dir.path().join(format!("{stem}.manifest.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "v": 1,
                "kind": "surface-kernel",
                "module": format!("{stem}.js"),
                "module_sha256": "00".repeat(32),
                "module_wasm": format!("{stem}_bg.wasm"),
                "module_wasm_sha256": "00".repeat(32),
                "spec": format!("{stem}.spec.brenn"),
                "spec_sha256": "00".repeat(32),
            }))
            .expect("serialize kernel record"),
        )
        .expect("write kernel record");

        let surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_dom_kind(dir.path(), &comp.kind);
        }
        validate_surface_assets(dir.path(), &[surface]);
    }

    /// `types` is in every processor's import list and no host implements it —
    /// it defines the shared shapes the other interfaces speak. It names no
    /// capability, so it is granted by no one and demanded of no one.
    #[test]
    fn the_types_import_names_no_grant() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "plain", &["types"], |_| {});
        let mut surface = resolved_with_processor("deskbar", "plain");
        surface.components[0].grants = Default::default();
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
            write_valid_dom_kind(dir.path(), &comp.kind);
        }
        validate_surface_assets(dir.path(), &[dom, clash]);
    }

    /// A surface carrying a binding that crosses the websocket but built with no
    /// `Messenger` is a broken boot invariant: the subscription would read
    /// retention through it and the output would publish through it. Both
    /// directions, one assert — a boot panic is caught by whoever starts the
    /// server, where the first-subscribe panic behind it ships and surfaces as a
    /// broken page in front of a user.
    #[test]
    #[should_panic(expected = "has wire bindings but no Messenger")]
    fn build_panics_on_wire_binding_without_messenger() {
        // The subscription direction: the fixture's ephemeral input binding,
        // with the output side removed so only one direction is in play.
        let mut r = resolved("deskbar");
        r.outputs.clear();
        SurfaceRuntime::build(
            r,
            None,
            TEST_MAX_BODY_BYTES,
            crate::fixtures_config::description_params(),
        );
    }

    /// The output direction of the same invariant. Split from its twin so
    /// neither direction can be quietly dropped from the assert and still pass.
    #[test]
    #[should_panic(expected = "has wire bindings but no Messenger")]
    fn build_panics_on_wire_output_without_messenger() {
        let mut r = resolved("deskbar");
        r.subscriptions.clear();
        r.wire_subscriptions.clear();
        SurfaceRuntime::build(
            r,
            None,
            TEST_MAX_BODY_BYTES,
            crate::fixtures_config::description_params(),
        );
    }

    /// A surface with no wire binding in either direction owes no `Messenger` —
    /// a page-local-only surface is live config, and the assert must not demand
    /// messaging it never touches.
    #[test]
    fn build_accepts_no_messenger_when_nothing_crosses_the_wire() {
        let mut r = resolved_with_local("deskbar");
        r.subscriptions
            .retain(|b| is_local_channel(&b.channel_address));
        r.wire_subscriptions.clear();
        r.outputs.retain(|b| is_local_channel(&b.channel_address));
        let rt = SurfaceRuntime::build(
            r,
            None,
            TEST_MAX_BODY_BYTES,
            crate::fixtures_config::description_params(),
        );
        assert_eq!(rt.profile.subscribable("local:page-bus"), None);
        assert!(!rt.profile.publishable(Some("writer"), "local:page-bus"));
    }

    #[test]
    #[should_panic(expected = "is not a surface-bindable scheme (brenn:, ephemeral:, or local:)")]
    fn build_panics_on_foreign_scheme() {
        let mut r = resolved("deskbar");
        // On the wire subscription: that is what the attachment's authority is
        // lowered from, and a scheme the surface cannot bind must not reach it.
        r.wire_subscriptions[0].subscription.channel_address = "mqtt:sensors".to_string();
        SurfaceRuntime::build(
            r,
            Some(empty_directory_messenger("test")),
            TEST_MAX_BODY_BYTES,
            crate::fixtures_config::description_params(),
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
        let n = u64::from(brenn_messaging::publish::SURFACE_SEND_BURST);
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
        let n = u64::from(brenn_messaging::publish::SURFACE_SEND_BURST) + 1;
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
        // The error channel is many-writer by design: a surface's injected
        // error-channel ACL is legitimate, not a single-writer violation.
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

    // -----------------------------------------------------------------------
    // Emitter → reader parity.
    //
    // Every other test in this module hand-builds a record with `serde_json`,
    // and every test beside the emitters scrapes their output with shell. Both
    // halves therefore pin their own literals, and a field renamed on one side
    // leaves the whole build graph green: the divergence surfaces as a
    // `deny_unknown_fields` panic at the bounce, on the deploy host, which is
    // exactly the late failure the binding exists to move earlier.
    //
    // Source scripts, so the runfiles path is the workspace path.
    // TODO(bazel-fixture-list-guard): hand-held against the source, like the
    // other fixture paths in this file.
    // -----------------------------------------------------------------------

    /// Run one of the record emitters, failing the test with its own output.
    fn run_emitter(script: &str, args: &[&std::ffi::OsStr], env: &[(&str, &std::ffi::OsStr)]) {
        let path = std::path::Path::new(script);
        assert!(
            path.exists(),
            "the record emitter is missing at {script} — it is a data dependency of this test",
        );
        let mut command = std::process::Command::new("bash");
        command.arg(path).args(args);
        for (key, value) in env {
            command.env(key, value);
        }
        let out = command.output().expect("run the record emitter");
        assert!(
            out.status.success(),
            "{script} failed: {}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }

    #[test]
    fn the_dom_emitters_record_is_the_one_the_reader_reads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dist = dir.path();
        let kind = "protobar";

        let module_bytes = b"export function init() {}\n";
        let wasm_bytes = b"\0asm\x01\0\0\0";
        let spec_bytes = b"component Protobar { abi = dom; }\n";
        let module = dist.join(module_artifact(kind));
        let module_wasm = dist.join(brenn_surface_contract::module_wasm_artifact(kind));
        let spec_in = dist.join("authored.brenn");
        let spec_out = dist.join(brenn_surface_contract::dom_spec_artifact(kind));
        std::fs::write(&module, module_bytes).expect("write module");
        std::fs::write(&module_wasm, wasm_bytes).expect("write module wasm");
        std::fs::write(&spec_in, spec_bytes).expect("write spec");

        use std::ffi::OsStr;
        run_emitter(
            "bazel/surface/emit_dom_manifest.sh",
            &[
                OsStr::new(kind),
                module.as_os_str(),
                module_wasm.as_os_str(),
                spec_in.as_os_str(),
                dom_assets::record_path(dist, kind).as_os_str(),
                spec_out.as_os_str(),
            ],
            &[
                ("DOM_NAMES", OsStr::new("bazel/surface/dom_names.sh")),
                ("WIT_LIB", OsStr::new("bazel/wasm/wit_lib.sh")),
            ],
        );
        // The emitter's own input file, which no dom kind's record names.
        std::fs::remove_file(&spec_in).expect("remove the emitter's input spec");

        let manifest = dom_assets::validate_dom_kind(dist, kind);
        use sha2::Digest as _;
        assert_eq!(manifest.kind, kind);
        assert_eq!(
            manifest.spec_sha256,
            hex::encode(sha2::Sha256::digest(spec_bytes)),
            "the hash a configured instance is bound to is the hash of the packaged spec",
        );
        assert_eq!(
            manifest.module_sha256,
            hex::encode(sha2::Sha256::digest(module_bytes)),
        );
    }

    #[test]
    fn the_processor_emitters_record_is_the_one_the_reader_reads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dist = dir.path();
        let kind = "transplant";
        let kind_dir = processor_assets::kind_dir(dist, kind);
        std::fs::create_dir_all(&kind_dir).expect("create the kind directory");

        let component_bytes = b"\0asm\x01\0\0\0";
        let spec_bytes =
            b"component Transplant { abi = processor; requires = [ports]; out out; }\n";
        let component = kind_dir.join(format!("{kind}.component.wasm"));
        let spec = kind_dir.join(format!("{kind}.spec.brenn"));
        std::fs::write(&component, component_bytes).expect("write component");
        std::fs::write(
            kind_dir.join(format!("{kind}.js")),
            b"export function i() {}\n",
        )
        .expect("write module");
        std::fs::write(&spec, spec_bytes).expect("write spec");

        // The emitter reads the artifact's imports through `wasm-tools`, and
        // this artifact is eight bytes of fixture. What is under test is the
        // record's shape, not the import scrape.
        let wasm_tools = dist.join("wasm-tools-stub");
        std::fs::write(
            &wasm_tools,
            "#!/usr/bin/env bash\necho \"package brenn:fixture;\"\n",
        )
        .expect("write the wasm-tools stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&wasm_tools, std::fs::Permissions::from_mode(0o755))
                .expect("make the stub executable");
        }

        use std::ffi::OsStr;
        run_emitter(
            "surface/emit-processor-manifest.sh",
            &[
                OsStr::new(kind),
                component.as_os_str(),
                kind_dir.as_os_str(),
                OsStr::new("1.4.0"),
                spec.as_os_str(),
            ],
            &[
                ("WASM_TOOLS", wasm_tools.as_os_str()),
                ("WIT_LIB", OsStr::new("bazel/wasm/wit_lib.sh")),
            ],
        );
        // The stub is not part of the kind's tree; the record's observed file
        // list is taken from the kind directory alone.
        std::fs::remove_file(&wasm_tools).expect("remove the stub");

        let manifest = processor_assets::validate_processor_kind(dist, kind);
        use sha2::Digest as _;
        assert_eq!(manifest.kind, kind);
        assert_eq!(
            manifest.spec_sha256,
            hex::encode(sha2::Sha256::digest(spec_bytes)),
        );
        assert_eq!(
            manifest.source_sha256,
            hex::encode(sha2::Sha256::digest(component_bytes)),
        );
    }
}

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
use brenn_lib::panic_util::CONFIG_REFUSAL;
use brenn_messaging::Messenger;
use brenn_messaging::system::SystemParticipantSpec;
use brenn_surface_contract::{KERNEL_ARTIFACT, PROCESSOR_DIR};

pub use brenn_surface_contract::processor_kind_from_path;

const SURFACE_FLAG: &str = "--surface";
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

/// Where each served surface asset lives, once the installed roots are scanned.
///
/// The URL namespace under `/surface-static` is one tree; the filesystem behind
/// it is several, one per installed release. A page manifest never says which
/// root a kind came from, so this map is the whole of that knowledge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceRoots {
    /// The one root that holds the kernel module pair and the flat sidecars.
    /// `None` iff no `--surface` was given — a surface-less deployment.
    pub kernel: Option<std::path::PathBuf>,
    /// Wire kind → the one root whose `processor/<kind>/` holds it.
    pub kinds: std::collections::BTreeMap<String, std::path::PathBuf>,
}

impl SurfaceRoots {
    /// The root serving one kind, or `None` where no installed root offers it.
    pub fn kind_root(&self, kind: &str) -> Option<&std::path::Path> {
        self.kinds.get(kind).map(std::path::PathBuf::as_path)
    }
}

/// Boot-time surface-asset existence check, over every installed root.
///
/// Each root is a release's surface tree: brenn's own carries the kernel module
/// pair and the flat sidecars beside its kinds, a component bundle's carries
/// kinds alone. The scan is of the roots as declared, not of what the current
/// configuration happens to name — a kind installed under two roots is an
/// ambiguous deploy whether or not anything instantiates it today, so it is
/// refused either way.
///
/// Then, when any `[[surface]]` is configured, every configured component kind
/// must have the assets its ABI implies under *its* root: a `processor` kind its
/// transpiled tree plus a conforming manifest and import profile
/// (`processor_assets`). A missing or stale artifact is a deploy/packaging
/// mistake — config-shaped, boot-time, never attacker-reachable — so this panics
/// (house fail-fast policy).
///
/// The kernel keeps a bare pair-existence check: it is not a component, so it
/// has no kind, no class and no specification to bind — nothing to record.
///
/// Lives beside `build_surface_runtimes` (a plain function over the resolved
/// list), not in `SurfaceRuntime::build`, so it never runs on the
/// `AppState`-constructing unit tests.
///
/// # Panics
///
/// On a repeated root, a kind offered by two roots, zero or two kernel roots, a
/// root offering neither the kernel nor a kind, a configured kind no root
/// offers, and everything the per-kind and per-instance passes already panic
/// on.
pub fn validate_surface_assets(
    roots: &[std::path::PathBuf],
    surfaces: &[ResolvedSurface],
) -> SurfaceRoots {
    if roots.is_empty() {
        assert!(
            surfaces.is_empty(),
            "boot: {} [[surface]] block(s) are configured but the server was started without \
             {SURFACE_FLAG}. The surface asset tree is an artifact fact, so it is named on the command \
             line and never in the document: pass one --surface per installed release. Refusing to \
             start (fail-fast on invalid config).",
            surfaces.len(),
        );
        return SurfaceRoots::default();
    }
    let kinds = scan_surface_roots(roots);
    let kernel = sole_kernel_root(roots);
    assert_every_root_offers_something(roots, &kernel, &kinds);
    // The kernel pair is not a component — no kind, no class, no specification —
    // so the root it was served from is the whole of its identity.
    tracing::info!(root = %kernel.display(), "surface kernel root resolved");
    let roots = SurfaceRoots {
        kernel: Some(kernel),
        kinds,
    };
    if surfaces.is_empty() {
        return roots;
    }
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
            let root = roots.kind_root(&comp.kind).unwrap_or_else(|| {
                panic!(
                    "boot: [[surface]] {:?} component {:?} names kind {:?}, which no installed \
                     surface root offers. The roots scanned were {}, and between them they offer \
                     {}. Install the release carrying that kind, or name its root with another \
                     --surface. Refusing to start (fail-fast on invalid config).",
                    surface.slug,
                    comp.instance,
                    comp.kind,
                    root_list(&roots),
                    offered_kinds(&roots),
                )
            });
            let manifest = processor_assets::validate_processor_kind(root, &comp.kind);
            // Together with the kernel line above, this is the operator's answer
            // to which release each installed kind came from.
            tracing::info!(
                kind = %comp.kind,
                root = %root.display(),
                spec_sha256 = %manifest.spec_sha256,
                source_sha256 = %manifest.source_sha256,
                "surface processor kind resolved"
            );
            kinds.insert(
                comp.kind.as_str(),
                KindAssets {
                    spec_sha256: manifest.spec_sha256.clone(),
                    manifest,
                },
            );
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
            processor_assets::assert_imports_granted(
                &surface.slug,
                &comp.instance,
                &comp.kind,
                &assets.manifest,
                &comp.grants,
            );
            assert_spec_bound(&surface.slug, comp, &assets.spec_sha256);
        }
    }
    roots
}

/// The kind → root map, with every way the root list is not a set of distinct
/// releases holding distinct kinds refused in one pass.
///
/// A kind is a directory under `processor/`; nothing here reads its contents,
/// because which root owns a kind has to be settled before the per-kind pass
/// can ask a root anything.
fn scan_surface_roots(
    roots: &[std::path::PathBuf],
) -> std::collections::BTreeMap<String, std::path::PathBuf> {
    let is_kind = |entry: &std::fs::DirEntry| {
        if !entry.path().is_dir() {
            return None;
        }
        Some(entry.file_name().to_string_lossy().into_owned())
    };
    let (faults, holders) =
        brenn_dsl::roots::scan_roots_in(SURFACE_FLAG, roots, Some(PROCESSOR_DIR), is_kind);
    assert!(
        faults.is_empty(),
        "boot: the {SURFACE_FLAG} roots are not a set of distinct releases:\n{}\nA kind is served \
         from exactly one tree, and the page manifest names no root, so which one served it would \
         be an accident of scan order. Refusing to start (fail-fast on invalid config).",
        faults
            .iter()
            .map(|fault| fault.describe(SURFACE_FLAG, "surface kind"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    holders
        .into_iter()
        .map(|(kind, holders)| (kind, holders[0].to_path_buf()))
        .collect()
}

/// Every declared root must be one release's surface tree, and a release's
/// surface tree offers something: brenn's holds the kernel pair, a bundle's
/// holds at least one `processor/<kind>/` (a bundle with no kind stages no
/// `surface/` at all). A root that offers neither is a `--surface` pointed one
/// directory off — at a bundle's install root rather than its `surface/` — and
/// nothing else downstream would notice: the scan finds no kinds, the kernel
/// rule is satisfied by brenn's own root, and boot succeeds until some later
/// configuration first stamps the kind that was supposed to be there.
///
/// # Panics
///
/// Naming every root that offers nothing, and what a surface root holds.
fn assert_every_root_offers_something(
    roots: &[std::path::PathBuf],
    kernel: &std::path::Path,
    kinds: &std::collections::BTreeMap<String, std::path::PathBuf>,
) {
    let empty: Vec<&std::path::PathBuf> = roots
        .iter()
        .filter(|root| root.as_path() != kernel && !kinds.values().any(|held| held == *root))
        .collect();
    assert!(
        empty.is_empty(),
        "boot: {} {SURFACE_FLAG} root(s) offer nothing: {}. Every root is one installed \
         release's surface tree — brenn's own carries the kernel module pair, a component \
         bundle's carries at least one processor/<kind>/ directory — so a root with neither \
         is a flag pointed one directory off (a bundle's install root rather than its \
         surface/ tree). Refusing to start (fail-fast on invalid config).",
        empty.len(),
        brenn_dsl::roots::display_list(&empty),
    );
}

/// The one root carrying the kernel module pair.
///
/// Exactly one, because every surface page references the kernel by a path with
/// no kind in it: two candidates leave the served bytes to scan order, and none
/// is a deploy with no shell to boot. A bundle's surface root carries no kernel
/// by construction, so a second candidate is a mis-pointed `--surface`.
fn sole_kernel_root(roots: &[std::path::PathBuf]) -> std::path::PathBuf {
    let wasm = kernel_wasm_artifact();
    let holders: Vec<&std::path::PathBuf> = roots
        .iter()
        .filter(|root| root.join(KERNEL_ARTIFACT).exists() && root.join(&wasm).exists())
        .collect();
    match holders.as_slice() {
        [only] => (*only).clone(),
        [] => panic!(
            "boot: no --surface root holds the kernel module pair ({KERNEL_ARTIFACT} + {wasm}), \
             which every surface page references. The roots scanned were {}. One of them must be \
             brenn's own installed surface tree (run `make build`; on deploy ensure the surface \
             install ran). Refusing to start (fail-fast on invalid config).",
            brenn_dsl::roots::display_list(roots),
        ),
        many => panic!(
            "boot: {} --surface roots hold the kernel module pair ({KERNEL_ARTIFACT} + {wasm}): \
             {}. Exactly one root is brenn's own surface tree; a component bundle's root carries \
             kinds alone. Refusing to start (fail-fast on invalid config).",
            many.len(),
            brenn_dsl::roots::display_list(many),
        ),
    }
}

/// The kernel's wasm sibling, derived from the JS artifact name the contract
/// pins, so the two never drift apart here.
fn kernel_wasm_artifact() -> String {
    format!(
        "{}_bg.wasm",
        KERNEL_ARTIFACT
            .strip_suffix(".js")
            .expect("a wasm-bindgen module artifact ends in .js"),
    )
}

/// The distinct roots a scan reached, for a refusal that has to name them.
fn root_list(roots: &SurfaceRoots) -> String {
    let mut seen: Vec<&std::path::PathBuf> = roots.kernel.iter().collect();
    for root in roots.kinds.values() {
        if !seen.contains(&root) {
            seen.push(root);
        }
    }
    seen.iter()
        .map(|root| root.display().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn offered_kinds(roots: &SurfaceRoots) -> String {
    if roots.kinds.is_empty() {
        return "no kinds at all".to_string();
    }
    roots
        .kinds
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

/// What one validated component kind's installed assets tell the per-instance
/// pass: the specification hash every instance of the kind is bound to, and the
/// record carrying the reflected import profile the grants must cover.
struct KindAssets {
    spec_sha256: String,
    manifest: processor_assets::ProcessorManifest,
}

/// Bind one configured instance to the specification its kind's installed
/// artifacts were built against.
///
/// Byte equality, not a comparison of facts: the configuration compiled against
/// exactly these bytes, so equality carries the fit check, the port optionality
/// and the doctypes over to the installed tree in one step.
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

/// Validation of `[observability] surface_error_channel`.
///
/// Every failure here is operator config, never attacker-reachable, so each is a
/// panic (house fail-fast policy). A pure function of the document and the
/// directory, so the offline messaging pass runs it as well as boot. No-op when
/// the channel is unset (surfaces console-only). At boot it runs once the
/// messaging directory exists, before any session can attach:
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
///
/// Returns the one non-fatal finding this validator has, or `None`. It is
/// returned rather than logged because the callers report differently: boot has
/// a `tracing` subscriber and the offline config check has none, so a warning
/// emitted here would vanish on the gate an operator actually reads before a
/// deploy.
#[must_use]
pub fn validate_surface_error_channel(
    channel: Option<&str>,
    directory: Option<&MessagingDirectory>,
    max_body_bytes: usize,
) -> Option<SurfaceErrorAdvisory> {
    let channel = channel?;

    // The address must be a well-formed brenn: channel (durable, replayable);
    // the parse is the validation, its bare name no longer needed downstream.
    well_formed_name(channel, ChannelScheme::Brenn).unwrap_or_else(|| {
        panic!(
            "{CONFIG_REFUSAL}[observability] surface_error_channel {channel:?} is not a \
             well-formed brenn: address — error reports need a durable, replayable channel, so \
             only the brenn: scheme is accepted."
        )
    });

    let directory = directory.unwrap_or_else(|| {
        panic!(
            "{CONFIG_REFUSAL}[observability] surface_error_channel {channel:?} is set but no \
             messaging is configured (no [[channel]] blocks, no Messenger). Declare messaging or \
             unset the channel."
        )
    });

    let Some(entry) = directory.resolve(channel) else {
        panic!(
            "{CONFIG_REFUSAL}[observability] surface_error_channel {channel:?} does not resolve \
             to any declared [[channel]] block — error routing requires an explicit matching \
             channel; no implicit channel is created."
        );
    };

    // A bounded eviction frontier at or below one surface's admitted send burst
    // means one fully-admitted burst can rotate every earlier report out of the
    // durable channel before the budget refills. The evicted reports still
    // survive the kernel's console copy, so this is a footgun, not a fatal
    // misconfiguration. A pinned channel (frontier None) never triggers.
    let advisory = entry
        .reap_frontier()
        .filter(|frontier| *frontier <= u64::from(brenn_messaging::publish::SURFACE_SEND_BURST))
        .map(|frontier| SurfaceErrorAdvisory {
            channel: channel.to_string(),
            frontier,
            burst: brenn_messaging::publish::SURFACE_SEND_BURST,
            refill_window_secs: u64::from(brenn_messaging::publish::SURFACE_SEND_BURST)
                * brenn_messaging::publish::SURFACE_SEND_REFILL.as_secs(),
        });

    assert!(
        max_body_bytes >= SURFACE_ERROR_BODY_MAX_BYTES,
        "{CONFIG_REFUSAL}[messaging] max_body_bytes {max_body_bytes} is below the worst-case \
         surface error report body ({SURFACE_ERROR_BODY_MAX_BYTES} bytes) — a report publish \
         could hit BodyTooLarge at runtime. Raise max_body_bytes.",
    );

    advisory
}

/// The eviction-frontier finding [`validate_surface_error_channel`] raises: the
/// error channel's frontier sits at or below one surface's admitted send burst,
/// so one admitted burst can rotate every earlier report out of it.
///
/// Advice, not a refusal — the evicted reports still survive the kernel's
/// console copy — so it travels back to the caller and is reported the way that
/// caller reports.
pub struct SurfaceErrorAdvisory {
    /// The configured `surface_error_channel` address.
    pub channel: String,
    /// The channel's eviction frontier.
    pub frontier: u64,
    /// One surface's admitted send burst.
    pub burst: u32,
    /// How long the send-burst budget takes to refill in full.
    pub refill_window_secs: u64,
}

/// Report a [`SurfaceErrorAdvisory`] the way a caller with a `tracing`
/// subscriber reports it: the rendered sentence, plus its numbers as fields so a
/// subscriber can key on them rather than on the text.
///
/// Lives beside the struct rather than at the call site so that the one shape
/// this advisory takes in a log is written once and can be tested without a
/// boot.
pub fn log_surface_error_advisory(advisory: &SurfaceErrorAdvisory) {
    tracing::warn!(
        channel = %advisory.channel,
        frontier = advisory.frontier,
        burst = advisory.burst,
        refill_window_secs = advisory.refill_window_secs,
        "boot: {advisory}"
    );
}

impl std::fmt::Display for SurfaceErrorAdvisory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[observability] surface_error_channel {:?} has an eviction frontier ({}) at or below \
             the surface send burst ({}) — one admitted burst can rotate every earlier report out \
             of the channel, and the budget fully refills within {} seconds. Evicted reports \
             still survive the kernel's console copy. Raise the channel's standing_retain_depth \
             above the burst to close the window.",
            self.channel, self.frontier, self.burst, self.refill_window_secs,
        )
    }
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
                    ..ResolvedComponent::minimal("protobar", "protobar")
                },
                ResolvedComponent {
                    spec_sha256: fixture_spec_hash("writer"),
                    // `out` is bound below; `spare` is declared and left
                    // unwired, which is the case the vocabulary exists to carry.
                    declared_out_ports: ["out", "spare"].into_iter().map(str::to_string).collect(),
                    ..ResolvedComponent::minimal("writer", "writer")
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
        // The declared vocabulary travels sorted, and carries the unwired port
        // the bound-output table cannot represent.
        assert_eq!(bindings.components[1].declared_out_ports, ["out", "spare"]);
        assert!(bindings.components[0].declared_out_ports.is_empty());
        bindings
            .validate()
            .expect("the built document satisfies the schema's own rules");
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

    /// A conforming transpiled tree for `kind` that imports nothing, so it
    /// satisfies asset validation under a component holding no grants.
    fn write_valid_kind(dir: &std::path::Path, kind: &str) {
        write_processor_tree(dir, kind, &[], |_| {});
    }

    /// Every fixture below installs one root, which is what a deployment
    /// without bundles has. The multi-root arrangement gets its own cases at
    /// the end of this module.
    fn validate_one_root(root: &std::path::Path, surfaces: &[ResolvedSurface]) -> SurfaceRoots {
        validate_surface_assets(&[root.to_path_buf()], surfaces)
    }

    /// An offered kind whose directory holds nothing, so the scan maps it and
    /// the per-kind pass is the one that refuses it.
    fn write_empty_kind_dir(dir: &std::path::Path, kind: &str) {
        std::fs::create_dir_all(dir.join("processor").join(kind)).expect("kind dir");
    }

    #[test]
    fn validate_surface_assets_returns_empty_roots_when_neither_exists() {
        // No --surface and no surface: a surface-less deployment, which serves
        // nothing under /surface-static and is asked for nothing.
        let roots = validate_surface_assets(&[], &[]);
        assert_eq!(roots, SurfaceRoots::default());
    }

    #[test]
    #[should_panic(expected = "started without --surface")]
    fn validate_surface_assets_panics_on_a_surface_with_no_root() {
        validate_surface_assets(&[], &[resolved("deskbar")]);
    }

    #[test]
    fn validate_surface_assets_passes_with_all_records_present() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_kind(dir.path(), &comp.kind);
        }
        validate_one_root(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "no --surface root holds the kernel module pair")]
    fn validate_surface_assets_panics_on_missing_kernel_pair() {
        let dir = tempfile::tempdir().expect("tempdir");
        let surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_kind(dir.path(), &comp.kind);
        }
        validate_one_root(dir.path(), &[surface]);
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
            ..ResolvedComponent::minimal(&format!("{kind}-1"), kind)
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
        validate_one_root(dir.path(), &[surface]);
    }

    #[test]
    #[should_panic(expected = "processor component \"transplant\" has no readable asset manifest")]
    fn validate_surface_assets_panics_on_missing_processor_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        // The kind is installed — the directory is there — and empty.
        write_empty_kind_dir(dir.path(), "transplant");
        validate_one_root(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    #[test]
    #[should_panic(expected = "which no installed surface root offers")]
    fn validate_surface_assets_panics_on_a_kind_no_root_offers() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        validate_one_root(
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
        validate_one_root(
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
        validate_one_root(
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
        validate_one_root(
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
        validate_one_root(
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
        validate_one_root(
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
        validate_one_root(
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
        validate_one_root(
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
        validate_one_root(
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
        validate_one_root(
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

        validate_one_root(dir.path(), &[resolved_with_processor("deskbar", kind)]);
    }

    #[test]
    #[should_panic(
        expected = "lists import \"brenn:processor/telepathy\", which names no interface"
    )]
    fn validate_surface_assets_panics_on_unknown_processor_import() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        write_processor_tree(dir.path(), "transplant", &["ports", "telepathy"], |_| {});
        validate_one_root(
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
        validate_one_root(
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
        validate_one_root(dir.path(), &[resolved_with_processor("deskbar", "noisy")]);
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
        validate_one_root(dir.path(), &[surface]);
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
        validate_one_root(dir.path(), &[surface]);
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
    fn validate_surface_assets_panics_on_divergent_instance_spec() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let mut surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_kind(dir.path(), &comp.kind);
        }
        surface.components[1].spec_sha256 =
            brenn_lib::util::sha256_hex(b"// specification for writer, plus a note\n");
        validate_one_root(dir.path(), &[surface]);
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
        validate_one_root(dir.path(), &[surface]);
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
            write_valid_kind(dir.path(), &comp.kind);
        }
        let mut sibling = surface.components[0].clone();
        sibling.instance = "protobar-2".to_string();
        sibling.spec_sha256 = brenn_lib::util::sha256_hex(b"another copy\n");
        surface.components.push(sibling);
        validate_one_root(dir.path(), &[surface]);
    }

    /// The `serde(skip)` backstop: the class fact is filled at lowering, so an
    /// empty hash is a lowering bug and must not be read as "matches anything".
    #[test]
    #[should_panic(expected = "component \"writer\" carries no specification hash")]
    fn validate_surface_assets_panics_on_empty_instance_spec_hash() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let mut surface = resolved("deskbar");
        for comp in &surface.components {
            write_valid_kind(dir.path(), &comp.kind);
        }
        surface.components[1].spec_sha256 = String::new();
        validate_one_root(dir.path(), &[surface]);
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
        validate_one_root(dir.path(), &[surface]);
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
            write_valid_kind(dir.path(), &comp.kind);
        }
        validate_one_root(dir.path(), &[surface]);
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
        validate_one_root(dir.path(), &[surface]);
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

    /// Frontier exactly at the burst boundary → advises, naming the frontier.
    #[test]
    fn validate_surface_error_channel_advises_at_frontier_boundary() {
        let n = u64::from(brenn_messaging::publish::SURFACE_SEND_BURST);
        let dir = directory_with_standing_depth("surface-errors", n);
        let advisory = validate_surface_error_channel(
            Some("brenn:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        )
        .expect("frontier == burst must raise the retention advisory");
        assert_eq!(advisory.frontier, n);
        assert_eq!(advisory.burst, brenn_messaging::publish::SURFACE_SEND_BURST);
        // The arithmetic the struct introduced, and the one thing the operator
        // sizes `standing_retain_depth` against: computed here from the two
        // constants, so a swapped multiplicand or a millisecond unit is a red
        // test rather than a wrong number in the advice.
        let refill = u64::from(brenn_messaging::publish::SURFACE_SEND_BURST)
            * brenn_messaging::publish::SURFACE_SEND_REFILL.as_secs();
        assert_eq!(advisory.refill_window_secs, refill);
        assert_eq!(
            advisory.to_string(),
            format!(
                "[observability] surface_error_channel \"brenn:surface-errors\" has an eviction \
                 frontier ({n}) at or below the surface send burst ({}) — one admitted burst can \
                 rotate every earlier report out of the channel, and the budget fully refills \
                 within {refill} seconds. Evicted reports still survive the kernel's console \
                 copy. Raise the channel's standing_retain_depth above the burst to close the \
                 window.",
                brenn_messaging::publish::SURFACE_SEND_BURST,
            ),
        );
    }

    /// The boot half of the same advisory. `validate_surface_error_channel`
    /// returns it and boot logs it; nothing else asserts that the logging
    /// happens, so dropping or renaming a field there would fail nothing.
    #[test]
    #[tracing_test::traced_test]
    fn the_boot_advisory_carries_its_fields_into_the_log() {
        // One below the burst, so the frontier and the burst are different
        // numbers and a transposition between them is visible.
        let n = u64::from(brenn_messaging::publish::SURFACE_SEND_BURST) - 1;
        let dir = directory_with_standing_depth("surface-errors", n);
        let advisory = validate_surface_error_channel(
            Some("brenn:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        )
        .expect("a frontier below the burst must raise the retention advisory");
        log_surface_error_advisory(&advisory);
        for field in [
            format!("frontier={n}"),
            format!("burst={}", brenn_messaging::publish::SURFACE_SEND_BURST),
            format!(
                "refill_window_secs={}",
                u64::from(brenn_messaging::publish::SURFACE_SEND_BURST)
                    * brenn_messaging::publish::SURFACE_SEND_REFILL.as_secs()
            ),
            "channel=brenn:surface-errors".to_string(),
            "eviction frontier".to_string(),
        ] {
            assert!(logs_contain(&field), "the log carries no {field:?}");
        }
    }

    /// Frontier one above the burst → no advisory (a single burst leaves a
    /// report).
    #[test]
    fn validate_surface_error_channel_no_advisory_above_frontier_boundary() {
        let n = u64::from(brenn_messaging::publish::SURFACE_SEND_BURST) + 1;
        let dir = directory_with_standing_depth("surface-errors", n);
        assert!(
            validate_surface_error_channel(
                Some("brenn:surface-errors"),
                Some(&dir),
                SURFACE_ERROR_BODY_MAX_BYTES,
            )
            .is_none(),
            "frontier > burst must not advise"
        );
    }

    /// Default (unbounded) standing depth pins the channel → frontier None → no
    /// advisory.
    #[test]
    fn validate_surface_error_channel_no_advisory_when_pinned() {
        let dir = directory_with("surface-errors");
        assert!(
            validate_surface_error_channel(
                Some("brenn:surface-errors"),
                Some(&dir),
                SURFACE_ERROR_BODY_MAX_BYTES,
            )
            .is_none(),
            "a pinned (Unbounded) channel must never advise"
        );
    }

    #[test]
    fn validate_surface_error_channel_noop_when_unset() {
        // Unset channel is a no-op even with no directory (console-only path).
        assert!(validate_surface_error_channel(None, None, 1).is_none());
    }

    #[test]
    fn validate_surface_error_channel_passes_for_valid_config() {
        // The error channel is many-writer by design: a surface's injected
        // error-channel ACL is legitimate, not a single-writer violation.
        let dir = directory_with("surface-errors");
        assert!(
            validate_surface_error_channel(
                Some("brenn:surface-errors"),
                Some(&dir),
                SURFACE_ERROR_BODY_MAX_BYTES,
            )
            .is_none()
        );
    }

    #[test]
    #[should_panic(expected = "not a well-formed brenn: address")]
    fn validate_surface_error_channel_panics_on_foreign_scheme() {
        let dir = directory_with("surface-errors");
        let _ = validate_surface_error_channel(
            Some("ephemeral:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
    }

    #[test]
    #[should_panic(expected = "no messaging is configured")]
    fn validate_surface_error_channel_panics_when_messaging_absent() {
        let _ = validate_surface_error_channel(
            Some("brenn:surface-errors"),
            None,
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
    }

    #[test]
    #[should_panic(expected = "does not resolve to any declared")]
    fn validate_surface_error_channel_panics_on_undeclared_channel() {
        let dir = directory_with("some-other-channel");
        let _ = validate_surface_error_channel(
            Some("brenn:surface-errors"),
            Some(&dir),
            SURFACE_ERROR_BODY_MAX_BYTES,
        );
    }

    #[test]
    #[should_panic(expected = "below the worst-case surface error report body")]
    fn validate_surface_error_channel_panics_on_insufficient_body_headroom() {
        let dir = directory_with("surface-errors");
        let _ = validate_surface_error_channel(
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

    // ── more than one installed root ─────────────────────────────────────────
    //
    // A component bundle installs its kinds into a root of its own, so brenn's
    // next deploy — which empties its own — cannot delete them. The scan is of
    // the roots as declared, not of what today's configuration names.

    #[test]
    fn kinds_split_across_two_roots_pass_and_the_map_names_each_root() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_valid_kind(brenn.path(), "chrome");
        write_valid_kind(bundle.path(), "demo-panel");

        let mut surface = resolved_with_processor("deskbar", "chrome");
        surface.components.push(ResolvedComponent {
            spec_sha256: fixture_spec_hash("demo-panel"),
            grants: [brenn_lib::messaging::ComponentGrant::Ports].into(),
            ..ResolvedComponent::minimal("demo-panel-1", "demo-panel")
        });

        let roots = validate_surface_assets(
            &[brenn.path().to_path_buf(), bundle.path().to_path_buf()],
            &[surface],
        );
        assert_eq!(roots.kernel.as_deref(), Some(brenn.path()));
        assert_eq!(roots.kind_root("chrome"), Some(brenn.path()));
        assert_eq!(roots.kind_root("demo-panel"), Some(bundle.path()),);
    }

    /// The scan is not driven by the configuration: a kind installed twice is
    /// an ambiguous deploy whether or not anything mounts it today.
    #[test]
    #[should_panic(expected = "installed under more than one --surface root")]
    fn a_kind_offered_by_two_roots_is_refused_even_when_unconfigured() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_valid_kind(brenn.path(), "chrome");
        write_valid_kind(bundle.path(), "chrome");
        validate_surface_assets(
            &[brenn.path().to_path_buf(), bundle.path().to_path_buf()],
            &[],
        );
    }

    #[test]
    #[should_panic(expected = "roots hold the kernel module pair")]
    fn two_kernel_roots_are_refused() {
        let one = tempfile::tempdir().expect("tempdir");
        let two = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(one.path());
        write_kernel_pair(two.path());
        validate_surface_assets(&[one.path().to_path_buf(), two.path().to_path_buf()], &[]);
    }

    /// A bundle root alone: kinds and no kernel, which is a mis-pointed
    /// `--surface` rather than a bundle's fault.
    #[test]
    #[should_panic(expected = "no --surface root holds the kernel module pair")]
    fn a_root_set_with_no_kernel_is_refused() {
        let bundle = tempfile::tempdir().expect("tempdir");
        write_valid_kind(bundle.path(), "demo-panel");
        validate_surface_assets(&[bundle.path().to_path_buf()], &[]);
    }

    /// The realistic mis-pointing: `--surface $BUNDLES_DIR/<bundle>` instead of
    /// `.../<bundle>/surface`. It holds no kernel and no `processor/`, so
    /// nothing downstream would notice until some later configuration first
    /// stamps the kind that was supposed to be there.
    #[test]
    #[should_panic(expected = "root(s) offer nothing")]
    fn a_root_offering_neither_the_kernel_nor_a_kind_is_refused() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let one_directory_off = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_valid_kind(brenn.path(), "chrome");
        std::fs::create_dir_all(
            one_directory_off
                .path()
                .join("surface/processor/demo-panel"),
        )
        .expect("the bundle's real surface tree, one level below the flag");
        validate_surface_assets(
            &[
                brenn.path().to_path_buf(),
                one_directory_off.path().to_path_buf(),
            ],
            &[],
        );
    }

    #[test]
    #[should_panic(expected = "name the same directory")]
    fn the_same_root_named_twice_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        validate_surface_assets(
            &[dir.path().to_path_buf(), dir.path().join(".").to_path_buf()],
            &[],
        );
    }
}

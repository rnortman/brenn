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
mod publish;
pub mod telemetry;

#[cfg(any(test, feature = "testutils"))]
pub mod fixtures_config;
#[cfg(any(test, feature = "testutils"))]
pub mod test_fixtures;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use brenn_envelope::{channel_capabilities, is_local_channel};
use brenn_lib::access::AppPolicy;
use brenn_lib::messaging::config::{ResolvedComponent, ResolvedSurface, ResolvedWasmConsumer};
use brenn_lib::messaging::gates::well_formed_name;
use brenn_lib::messaging::{ChannelScheme, MessagingDirectory};
use brenn_lib::panic_util::CONFIG_REFUSAL;
use brenn_messaging::Messenger;
use brenn_messaging::system::SystemParticipantSpec;

pub use brenn_surface_contract::processor_kind_from_path;
/// Re-exported so a caller that stages or asserts on a surface tree names the
/// same constants the scan does.
pub use brenn_surface_contract::{KERNEL_ARTIFACT, PROCESSOR_DIR};

/// What a message calls the tree a surface kind is served from. Where it comes
/// from is the mounts document, not a flag: the host reads `<mount>/surface/`,
/// once per declared mount that offers one.
const SURFACE_TREE: &str = "`surface/` tree";
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
    /// The one root that holds the kernel module pair and the flat sidecars,
    /// with the fingerprint of the pair it holds. `None` iff no declared mount
    /// offers a surface tree — a surface-less deployment.
    pub kernel: Option<KernelRoot>,
    /// Wire kind → the one root whose `processor/<kind>/` holds it, and what
    /// that tree currently holds.
    pub kinds: std::collections::BTreeMap<String, KindRoot>,
    /// Wire kind → the tree offering it that this host declines to serve,
    /// because its record names a hosting contract this binary does not read.
    ///
    /// A kind is here instead of in `kinds`, never as well: nothing resolves an
    /// asset URL for it, no page brings it up, and its description documents
    /// say so. The state is the expected middle of a rolling upgrade — a bundle
    /// correct when it was built and correct again when it is rebuilt — so it
    /// is a warning and an alert rather than a refusal to start. A record
    /// mismatch under brenn's own mount is a broken install and still panics.
    pub withheld: std::collections::BTreeMap<String, WithheldKind>,
}

/// The title every "this kind is withheld" alert carries, at boot and at the
/// reload that adopts a scan holding one.
///
/// One string for one condition: the operator sees the same title whichever
/// path found it, and a test filtering for the alert names it rather than
/// re-spelling it.
pub const WITHHELD_ALERT_TITLE: &str = "surface kind withheld";

/// One installed kind this host will not serve: where it is, what its record
/// declares, and why that is not readable here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithheldKind {
    /// The mount offering this kind, as its root list names it.
    pub mount: String,
    /// The surface tree whose `processor/<kind>/` holds it.
    pub root: std::path::PathBuf,
    /// The schema version the record declares.
    pub record_v: u32,
    /// One sentence naming both versions and the migration. It is what the
    /// alert carries, what the page manifest hands the kernel, and what the
    /// kind's description documents state.
    pub reason: String,
}

impl WithheldKind {
    /// The alert body: the reason, plus where the tree that carries it is.
    ///
    /// Rendered here because boot and reload both announce this condition and
    /// an operator comparing the two notices should be reading one sentence,
    /// not two independently edited ones.
    pub fn alert_body(&self) -> String {
        format!(
            "{} (mount {}, tree {})",
            self.reason,
            self.mount,
            self.root.display()
        )
    }
}

/// Where one kind stands with this host.
///
/// The two states are exclusive and together exhaustive for any kind a declared
/// mount offers: boot refuses a configured kind no mount offers at all, so a
/// kind that reaches a reader here is served from a root or withheld from one.
/// `Served(None)` is the served kind whose root a caller does not need — or one
/// nothing offers, which only a caller outside the boot-validated set can see.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KindPlacement<'a> {
    /// Served, from this root where one is known.
    Served(Option<&'a std::path::Path>),
    /// Not served, for the reason the entry carries.
    Withheld(&'a WithheldKind),
}

/// The installed surface kernel: the tree serving the module pair every page
/// loads, and the fingerprint of that pair.
///
/// **The fingerprint is here because the path is not the identity.** Nothing
/// about the kernel is recorded in a manifest, so an in-place rewrite of the
/// pair — a `make build` under a dev mount, a sync into an unmoved release
/// mount — leaves the root exactly where it was. A page that keeps running is
/// running the bytes this fingerprint names, and the reload's refusal is what
/// keeps that from silently becoming untrue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KernelRoot {
    /// The surface tree holding the kernel module pair.
    pub root: std::path::PathBuf,
    /// SHA-256 over the JS module and its wasm sibling, in that order.
    pub module_sha256: String,
}

impl KernelRoot {
    /// A kernel root with a placeholder fingerprint, for tests that exercise
    /// path resolution alone.
    #[cfg(any(test, feature = "testutils"))]
    pub fn for_test(root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            root: root.into(),
            module_sha256: String::new(),
        }
    }
}

/// One installed component kind: which mount offers it, the tree currently
/// serving it, and the fingerprint of the artifact and specification that tree
/// holds.
///
/// **The change identity is the mount and the two fingerprints, never the
/// path.** An install under the versioned-tree scheme swaps the mount symlink
/// onto a fresh directory, so the canonical root of every kind under that mount
/// moves on every deploy whether or not a byte of the kind did. The
/// fingerprints are what say a kind changed; `root` is only where to read it
/// today, and it is refreshed rather than compared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindRoot {
    /// The mount offering this kind, as its root list names it (a mount name on
    /// a host, a path under the workstation's `--surface` form).
    pub mount: String,
    /// The surface tree whose `processor/<kind>/` holds this kind, as the
    /// current scan resolved it.
    pub root: std::path::PathBuf,
    /// SHA-256 of the component artifact the transpiled tree was built from.
    pub source_sha256: String,
    /// SHA-256 of the authored specification packaged beside it.
    pub spec_sha256: String,
    /// The core wasm modules the transpiled glue asks for, by file name, off
    /// the kind's record.
    ///
    /// The page manifest carries them because the sync-instantiation glue looks
    /// a core module up synchronously: it cannot fetch one, so the page
    /// compiles them all at bring-up and answers from a map. Held here rather
    /// than re-read per request — the record was already read and walked at
    /// scan time, and a page render must not touch the filesystem.
    pub cores: Vec<String>,
}

impl KindRoot {
    /// A kind root with placeholder fingerprints, for tests that exercise path
    /// resolution alone.
    #[cfg(any(test, feature = "testutils"))]
    pub fn for_test(root: std::path::PathBuf) -> Self {
        Self {
            mount: root.display().to_string(),
            root,
            source_sha256: String::new(),
            spec_sha256: String::new(),
            cores: Vec::new(),
        }
    }

    /// A kind root naming the core modules a page would compile for it, for
    /// tests that exercise the page manifest.
    #[cfg(any(test, feature = "testutils"))]
    pub fn for_test_with_cores(root: impl Into<std::path::PathBuf>, cores: &[&str]) -> Self {
        let mut held = Self::for_test(root.into());
        held.cores = cores.iter().map(|core| (*core).to_string()).collect();
        held
    }

    /// Whether two scans found the same installation of this kind: the same
    /// mount offering the same bytes. A root path that moved under one mount
    /// with both fingerprints intact is a relocation — the installer swapped a
    /// symlink onto a fresh versioned tree — and not a change.
    fn same_installation(&self, other: &KindRoot) -> bool {
        self.mount == other.mount
            && self.source_sha256 == other.source_sha256
            && self.spec_sha256 == other.spec_sha256
    }
}

impl SurfaceRoots {
    /// The root serving one kind, or `None` where no installed root offers it.
    pub fn kind_root(&self, kind: &str) -> Option<&std::path::Path> {
        self.kinds.get(kind).map(|held| held.root.as_path())
    }

    /// The withheld entry for one kind, or `None` where this host serves it
    /// (or has never heard of it). A caller that has to act on both states
    /// reads [`Self::placement`] instead, which cannot answer twice.
    pub fn withheld_kind(&self, kind: &str) -> Option<&WithheldKind> {
        self.withheld.get(kind)
    }

    /// Whether this host serves one kind, and from where, or withholds it and
    /// why — the one read that cannot represent both at once.
    pub fn placement(&self, kind: &str) -> KindPlacement<'_> {
        match self.withheld.get(kind) {
            Some(held) => KindPlacement::Withheld(held),
            None => KindPlacement::Served(self.kind_root(kind)),
        }
    }

    /// The core module file names one kind's transpiled glue asks for, or
    /// `None` where no installed root offers the kind. The page manifest names
    /// them so the browser can compile them before the first activation.
    pub fn kind_cores(&self, kind: &str) -> Option<&[String]> {
        self.kinds.get(kind).map(|held| held.cores.as_slice())
    }

    /// Every kind on which `self` and `other` disagree, keyed by kind name.
    ///
    /// `self` is the set held — what is being served — and `other` the set just
    /// scanned. A kind absent from the result is byte-for-byte the same
    /// installation in both, which is what makes this map the closure input for
    /// "which surfaces does an installed tree move".
    ///
    /// Withheld kinds are compared too, on their record version and their tree:
    /// a kind withheld on both sides whose bundle was re-installed is a change
    /// an operator just made, and everything a reader says about it — the page
    /// manifest's reason, the description documents, the alert — is derived
    /// from what moved.
    pub fn kind_differences(&self, other: &SurfaceRoots) -> BTreeMap<String, KindDifference> {
        let mut out = BTreeMap::new();
        for (kind, held) in &self.kinds {
            let difference = match other.kinds.get(kind) {
                // Includes the relocation: one mount, the same bytes, a fresh
                // versioned tree behind the symlink. Nothing about the kind
                // changed, so it is not a difference — only the path to read it
                // from moved, which the roots cell is refreshed with.
                Some(now) if now.same_installation(held) => continue,
                Some(now) if now.mount != held.mount => KindDifference::Moved {
                    from: held.mount.clone(),
                    to: now.mount.clone(),
                },
                // One mount, different bytes: the tree it offers was replaced,
                // which is what a bundle upgrade looks like.
                Some(now) => KindDifference::Reinstalled {
                    mount: now.mount.clone(),
                    root: now.root.clone(),
                },
                None => KindDifference::Withdrawn {
                    mount: held.mount.clone(),
                    root: held.root.clone(),
                },
            };
            out.insert(kind.clone(), difference);
        }
        for (kind, now) in &other.kinds {
            if !self.kinds.contains_key(kind) {
                out.insert(
                    kind.clone(),
                    KindDifference::Offered {
                        mount: now.mount.clone(),
                    },
                );
            }
        }
        // A kind withheld on both sides never enters the served map, so the
        // loops above cannot see it change.
        for (kind, held) in &self.withheld {
            let Some(now) = other.withheld.get(kind) else {
                // Withheld here and served there is already `Offered` above.
                continue;
            };
            if now.record_v == held.record_v && now.root == held.root {
                continue;
            }
            let difference = if now.mount == held.mount {
                KindDifference::Reinstalled {
                    mount: now.mount.clone(),
                    root: now.root.clone(),
                }
            } else {
                KindDifference::Moved {
                    from: held.mount.clone(),
                    to: now.mount.clone(),
                }
            };
            out.insert(kind.clone(), difference);
        }
        out
    }
}

/// How one kind's installation differs between two scans of the declared
/// mounts, phrased from the held set's point of view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KindDifference {
    /// A different mount offers the kind now.
    Moved { from: String, to: String },
    /// The same mount offers it, with different bytes behind it — a bundle
    /// upgrade. `root` is the tree the new scan found it in.
    Reinstalled {
        mount: String,
        root: std::path::PathBuf,
    },
    /// No declared mount offers it any more.
    Withdrawn {
        mount: String,
        root: std::path::PathBuf,
    },
    /// A declared mount offers it and the held set has never seen it.
    Offered { mount: String },
}

/// How a surface-asset validation failure is framed for whoever reads it.
///
/// The same checks run at boot, where the process is about to refuse to start,
/// and at reload, where the process keeps serving the document it already has
/// and the caller turns the panic into a refusal. The body of every message is
/// the same; only the moment it names and the instruction it ends with differ,
/// so a reload refusal never tells an operator that a running process refused
/// to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetContext {
    /// The moment the check ran, as the first word of every message.
    pub when: &'static str,
    /// What the reader should do, appended to every message. Empty where the
    /// caller frames the outcome itself.
    pub verdict: &'static str,
}

impl AssetContext {
    /// Boot: a failure ends the process before it serves anything.
    pub const BOOT: Self = Self {
        when: "boot",
        verdict: " Refusing to start (fail-fast on invalid config).",
    };
    /// Reload: a failure refuses the candidate and the process carries on
    /// serving what it already has, so the message names no restart.
    pub const RELOAD: Self = Self {
        when: "reload",
        verdict: "",
    };
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
    roots: &brenn_dsl::roots::RootList,
    surfaces: &[ResolvedSurface],
) -> SurfaceRoots {
    validate_surface_assets_in(AssetContext::BOOT, roots, surfaces)
}

/// The same validation, framed for whoever asked for it.
///
/// A reload re-runs every check here against the trees the declared mounts
/// offer now, with the process still serving the document it already has, so
/// the messages must not tell the operator that a process refused to start.
///
/// # Panics
///
/// On everything [`validate_surface_assets`] panics on.
pub fn validate_surface_assets_in(
    cx: AssetContext,
    roots: &brenn_dsl::roots::RootList,
    surfaces: &[ResolvedSurface],
) -> SurfaceRoots {
    let AssetContext { when, verdict } = cx;
    if roots.is_empty() {
        assert!(
            surfaces.is_empty(),
            "{when}: {} [[surface]] block(s) are configured but no declared mount offers a \
             {SURFACE_TREE}. The surface asset tree is an artifact fact, so it comes from the \
             mounts document (`--mounts`) and never from the deployment document: declare a \
             mount per installed release.{verdict}",
            surfaces.len(),
        );
        return SurfaceRoots::default();
    }
    let holders = scan_surface_roots(cx, roots);
    let kernel = sole_kernel_root(cx, roots);
    assert_every_root_offers_something(cx, roots, &kernel.root, &holders);
    // The kernel pair is not a component — no kind, no class, no specification,
    // nothing recorded about it anywhere — so the scan hashes the two files it
    // found, which is the only witness a later comparison has that the bytes
    // under an unmoved path were rewritten.
    tracing::info!(
        root = %kernel.root.display(),
        module_sha256 = %kernel.module_sha256,
        "surface kernel root resolved"
    );
    // Kind-grain record checks (manifest, listed files, naming, import profile)
    // run once per kind any root offers, whether or not a surface instantiates
    // it: the manifest read is what produces the kind's fingerprint, and a
    // fingerprint for a kind nothing uses yet is what lets the next comparison
    // of two root sets see the upgrade that installed it. The artifact digests
    // are not run here — hashing a whole component nothing runs would be paid
    // again on every reload for a tree no page can reach — they run below, per
    // instantiated kind.
    let mut manifests: HashMap<String, processor_assets::ProcessorManifest> = HashMap::new();
    let mut kinds: std::collections::BTreeMap<String, KindRoot> = std::collections::BTreeMap::new();
    let mut withheld: std::collections::BTreeMap<String, WithheldKind> =
        std::collections::BTreeMap::new();
    for (kind, root) in holders {
        let manifest = match processor_assets::read_processor_record_or_version_in(cx, &root, &kind)
        {
            Ok(manifest) => manifest,
            Err(record_v) => {
                let reason = processor_assets::version_mismatch_reason(&kind, record_v);
                // brenn's own surface tree travels in one tarball with the
                // kernel and the binary and is installed as a sync, so a record
                // the binary beside it cannot read is a broken install and
                // nothing an operator can converge. Under any other declared
                // mount it is the ordinary middle of a rolling upgrade: the
                // bundle is authored on its own schedule, and a host that
                // cannot start until every third party has shipped is not
                // deployable.
                assert!(root != kernel.root, "{when}: {reason}{verdict}");
                let mount = roots.source().name(&root);
                tracing::warn!(
                    kind = %kind,
                    mount = %mount,
                    root = %root.display(),
                    record_v,
                    "{WITHHELD_ALERT_TITLE}"
                );
                withheld.insert(
                    kind,
                    WithheldKind {
                        mount,
                        root,
                        record_v,
                        reason,
                    },
                );
                continue;
            }
        };
        // Together with the kernel line above, this is the operator's answer to
        // which release each installed kind came from.
        tracing::info!(
            kind = %kind,
            root = %root.display(),
            spec_sha256 = %manifest.spec_sha256,
            source_sha256 = %manifest.source_sha256,
            "surface processor kind resolved"
        );
        kinds.insert(
            kind.clone(),
            KindRoot {
                mount: roots.source().name(&root),
                root,
                source_sha256: manifest.source_sha256.clone(),
                spec_sha256: manifest.spec_sha256.clone(),
                cores: manifest.cores(&kind),
            },
        );
        manifests.insert(kind, manifest);
    }
    let roots = SurfaceRoots {
        kernel: Some(kernel),
        kinds,
        withheld,
    };
    if surfaces.is_empty() {
        return roots;
    }
    // Sibling instances of one kind may hold different grants, and each carries
    // its own class's hash — the kind fold admits comment-divergent class
    // copies, so both questions are asked once per declaration rather than once
    // per kind. The artifact digests are the third question and are per kind, so
    // the first instance naming a kind pays for them and the rest do not.
    let mut verified: BTreeSet<&str> = BTreeSet::new();
    for surface in surfaces {
        for comp in &surface.components {
            // A withheld kind has no record to bind anything against, and the
            // configuration that names it compiled against the bundle's
            // authored module, which did not move. So the instance is skipped
            // here and reported to its page as withheld; the kind's own
            // artifacts, grants and class hash are questions for the release
            // that can read its record.
            if roots.withheld.contains_key(comp.kind.as_str()) {
                continue;
            }
            let manifest = manifests.get(comp.kind.as_str()).unwrap_or_else(|| {
                panic!(
                    "{when}: [[surface]] {:?} component {:?} names kind {:?}, which no installed \
                     surface tree offers. The trees scanned were {}, and between them they offer \
                     {}. Install the release carrying that kind, or declare the mount that \
                     holds it.{verdict}",
                    surface.slug,
                    comp.instance,
                    comp.kind,
                    root_list(&roots),
                    offered_kinds(&roots),
                )
            });
            if verified.insert(comp.kind.as_str()) {
                processor_assets::verify_processor_artifacts_in(
                    cx,
                    roots
                        .kind_root(&comp.kind)
                        .expect("the kind resolved to a manifest, so it has a root"),
                    &comp.kind,
                    manifest,
                );
            }
            processor_assets::assert_imports_granted(
                cx,
                &surface.slug,
                &comp.instance,
                &comp.kind,
                manifest,
                &comp.grants,
            );
            assert_spec_bound(cx, &surface.slug, comp, &manifest.spec_sha256);
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
    cx: AssetContext,
    roots: &brenn_dsl::roots::RootList,
) -> std::collections::BTreeMap<String, std::path::PathBuf> {
    let AssetContext { when, verdict } = cx;
    let is_kind = |entry: &std::fs::DirEntry| {
        if !entry.path().is_dir() {
            return None;
        }
        Some(entry.file_name().to_string_lossy().into_owned())
    };
    let (faults, holders) = brenn_dsl::roots::scan_roots_in(roots, Some(PROCESSOR_DIR), is_kind);
    assert!(
        faults.is_empty(),
        "{when}: {} are not a set of distinct releases:\n{}\nA kind is served \
         from exactly one tree, and the page manifest names no root, so which one served it would \
         be an accident of scan order.{verdict}",
        roots.source().all(),
        faults
            .iter()
            .map(|fault| fault.describe(roots.source(), "surface kind"))
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
/// `surface/` at all). A root that offers neither is a mount path one
/// directory off — at a bundle's install root rather than its `surface/` — and
/// nothing else downstream would notice: the scan finds no kinds, the kernel
/// rule is satisfied by brenn's own root, and boot succeeds until some later
/// configuration first stamps the kind that was supposed to be there.
///
/// # Panics
///
/// Naming every root that offers nothing, and what a surface root holds.
fn assert_every_root_offers_something(
    cx: AssetContext,
    roots: &brenn_dsl::roots::RootList,
    kernel: &std::path::Path,
    kinds: &std::collections::BTreeMap<String, std::path::PathBuf>,
) {
    let AssetContext { when, verdict } = cx;
    let empty: Vec<&std::path::PathBuf> = roots
        .iter()
        .filter(|root| root.as_path() != kernel && !kinds.values().any(|held| held == *root))
        .collect();
    assert!(
        empty.is_empty(),
        "{when}: {} {SURFACE_TREE}(s) offer nothing: {}. Every one is one installed \
         release's surface tree — brenn's own carries the kernel module pair, a component \
         bundle's carries at least one processor/<kind>/ directory — so one with neither \
         is a mount path one directory off (a bundle's install root rather than its \
         surface/ tree).{verdict}",
        empty.len(),
        empty
            .iter()
            .map(|root| roots.source().locate(root))
            .collect::<Vec<_>>()
            .join(", "),
    );
}

/// The one root carrying the kernel module pair.
///
/// Exactly one, because every surface page references the kernel by a path with
/// no kind in it: two candidates leave the served bytes to scan order, and none
/// is a deploy with no shell to boot. A bundle's surface root carries no kernel
/// by construction, so a second candidate is a mis-pointed mount.
fn sole_kernel_root(cx: AssetContext, roots: &brenn_dsl::roots::RootList) -> KernelRoot {
    let AssetContext { when, verdict } = cx;
    let wasm = kernel_wasm_artifact();
    let holders: Vec<&std::path::PathBuf> = roots
        .iter()
        .filter(|root| root.join(KERNEL_ARTIFACT).exists() && root.join(&wasm).exists())
        .collect();
    match holders.as_slice() {
        [only] => KernelRoot {
            root: (*only).clone(),
            module_sha256: kernel_module_sha256(cx, only, &wasm),
        },
        [] => panic!(
            "{when}: no declared mount's {SURFACE_TREE} holds the kernel module pair \
             ({KERNEL_ARTIFACT} + {wasm}), which every surface page references. The trees \
             scanned were {}. One of them must be brenn's own installed surface tree (run \
             `make build`; on deploy ensure the surface install ran).{verdict}",
            brenn_dsl::roots::display_list(roots.paths()),
        ),
        many => panic!(
            "{when}: {} declared mounts' {SURFACE_TREE}s hold the kernel module pair \
             ({KERNEL_ARTIFACT} + {wasm}): {}. Exactly one is brenn's own surface tree; a \
             component bundle's carries kinds alone.{verdict}",
            many.len(),
            roots.source().names(
                many.iter()
                    .map(|root| root.as_path())
                    .collect::<Vec<_>>()
                    .iter()
            ),
        ),
    }
}

/// The fingerprint of the kernel module pair: one digest over the JS module
/// followed by its wasm sibling.
///
/// One digest and not two because the pair is one artifact — wasm-bindgen emits
/// the glue and the module together and neither is servable without the other,
/// so there is no state in which one moved and the answer is not "the kernel
/// moved".
fn kernel_module_sha256(cx: AssetContext, root: &std::path::Path, wasm: &str) -> String {
    let AssetContext { when, verdict } = cx;
    let mut bytes = Vec::new();
    for name in [KERNEL_ARTIFACT, wasm] {
        let path = root.join(name);
        let read = std::fs::read(&path).unwrap_or_else(|err| {
            panic!(
                "{when}: reading the surface kernel artifact {} failed ({err}) — the pair was \
                 found a moment ago, so the tree is being written under the scan.{verdict}",
                path.display(),
            )
        });
        bytes.extend_from_slice(&read);
    }
    brenn_lib::util::sha256_hex(&bytes)
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
    let mut seen: Vec<&std::path::PathBuf> =
        roots.kernel.iter().map(|kernel| &kernel.root).collect();
    for held in roots.kinds.values() {
        if !seen.contains(&&held.root) {
            seen.push(&held.root);
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
fn assert_spec_bound(cx: AssetContext, slug: &str, comp: &ResolvedComponent, packaged: &str) {
    let AssetContext { when, verdict } = cx;
    assert!(
        !comp.spec_sha256.is_empty(),
        "{when}: [[surface]] {slug:?} component {:?} carries no specification hash — the class fact \
         is filled at lowering, so an empty one would match nothing a record can carry and this \
         is a lowering bug, not a deployment state.{verdict}",
        comp.instance,
    );
    assert!(
        comp.spec_sha256 == packaged,
        "{when}: [[surface]] {slug:?} component {:?} of kind {:?} was configured against a \
         specification that hashes to {}, but the installed surface assets for that kind were \
         built against one that hashes to {packaged}. The author's specification travels with the \
         component; a deployment's copy of it is verbatim. Re-copy the specification from the \
         release that carries these assets, or install the release the configuration was written \
         for.{verdict}",
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

    /// Every fixture below installs one root, which is what a deployment
    /// without bundles has. The multi-root arrangement gets its own cases at
    /// the end of this module.
    fn validate_one_root(root: &std::path::Path, surfaces: &[ResolvedSurface]) -> SurfaceRoots {
        validate_surface_assets(&mount_roots(&[("brenn", root)]), surfaces)
    }

    /// The surface trees of the named mounts, as a host derives them: the
    /// refusals name the mount, so a fixture that named roots any other way
    /// would be testing wording no host produces.
    use crate::test_fixtures::{
        fixture_spec_hash, spec_bytes_for, write_kernel_pair, write_processor_tree,
        write_processor_tree_from_bytes, write_valid_kind,
    };

    fn mount_roots(mounts: &[(&str, &std::path::Path)]) -> brenn_dsl::roots::RootList {
        brenn_dsl::roots::RootList::mounts(
            "surface",
            mounts
                .iter()
                .map(|(name, path)| ((*name).to_string(), path.to_path_buf()))
                .collect(),
        )
    }

    /// An offered kind whose directory holds nothing, so the scan maps it and
    /// the per-kind pass is the one that refuses it.
    fn write_empty_kind_dir(dir: &std::path::Path, kind: &str) {
        std::fs::create_dir_all(dir.join("processor").join(kind)).expect("kind dir");
    }

    #[test]
    fn validate_surface_assets_returns_empty_roots_when_neither_exists() {
        // No mount offers a surface tree and no surface is configured: a
        // surface-less deployment, which serves nothing under /surface-static
        // and is asked for nothing.
        let roots = validate_surface_assets(&mount_roots(&[]), &[]);
        assert_eq!(roots, SurfaceRoots::default());
    }

    #[test]
    #[should_panic(expected = "no declared mount offers a `surface/` tree")]
    fn validate_surface_assets_panics_on_a_surface_with_no_root() {
        validate_surface_assets(&mount_roots(&[]), &[resolved("deskbar")]);
    }

    /// The page manifest's core-module URLs are minted from the scanned root's
    /// `cores`, and nothing else fills that field: a scan that stopped reading
    /// the record's file list would serve every kind an empty list and fail
    /// every component's bring-up in the browser, with no Rust test to see it.
    #[test]
    fn a_scanned_root_carries_the_records_core_modules() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        let kind = "panel";
        let core = format!("{kind}.core.wasm");
        let second = format!("{kind}.core2.wasm");
        write_processor_tree(dir.path(), kind, &[], |manifest| {
            let files = manifest["files"].as_array_mut().expect("files");
            files.push(serde_json::Value::String(core.clone()));
            files.push(serde_json::Value::String(second.clone()));
        });
        std::fs::write(
            crate::processor_assets::kind_dir(dir.path(), kind).join(&core),
            b"core",
        )
        .expect("write core");
        std::fs::write(
            crate::processor_assets::kind_dir(dir.path(), kind).join(&second),
            b"core2",
        )
        .expect("write second core");

        let roots = validate_one_root(dir.path(), &[]);
        assert_eq!(
            roots.kind_cores(kind),
            Some([core, second].as_slice()),
            "the source component artifact is not a core module, and the record's other files \
             are not `.wasm`"
        );
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
    #[should_panic(expected = "no declared mount's `surface/` tree holds the kernel module pair")]
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
    #[should_panic(expected = "which no installed surface tree offers")]
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
    #[should_panic(expected = "manifest declares v = 2")]
    fn a_stale_record_under_brenns_own_mount_is_a_boot_refusal() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        // brenn's surface tree, its kernel and its binary travel in one tarball
        // and are installed as a sync, so a record the binary cannot read is a
        // broken install and nothing an operator can converge.
        write_processor_tree(dir.path(), "transplant", &["ports"], |m| {
            m["v"] = serde_json::json!(2);
        });
        validate_one_root(
            dir.path(),
            &[resolved_with_processor("deskbar", "transplant")],
        );
    }

    /// The withheld case, and the whole of what it means for the served set: a
    /// bundle mount's stale kind is not served, every other kind still is, and
    /// the process is up.
    #[test]
    fn a_stale_record_under_a_bundle_mount_is_withheld() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_processor_tree(brenn.path(), "chrome", &["ports"], |_| {});
        write_processor_tree(bundle.path(), "fleet", &["ports"], |m| {
            m["v"] = serde_json::json!(2);
        });

        let roots = validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", bundle.path())]),
            &[],
        );

        assert!(roots.kinds.contains_key("chrome"));
        assert!(!roots.kinds.contains_key("fleet"));
        let held = roots.withheld_kind("fleet").expect("fleet is withheld");
        assert_eq!(held.record_v, 2);
        assert_eq!(held.mount, "fleet-bundle");
        assert_eq!(held.root, bundle.path());
        assert!(
            held.reason.contains("declares v = 2") && held.reason.contains("retained `io` port"),
            "the reason names both versions and the migration: {}",
            held.reason
        );
        assert_eq!(roots.kind_root("fleet"), None);
        assert_eq!(roots.kind_cores("fleet"), None);
    }

    /// A configured instance of a withheld kind is not a configuration error:
    /// the document compiled against the bundle's authored module, which did not
    /// move, and the instance's verdict is the page's to deliver.
    #[test]
    fn a_configured_instance_of_a_withheld_kind_does_not_refuse_the_boot() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_processor_tree(bundle.path(), "fleet", &["ports"], |m| {
            m["v"] = serde_json::json!(2);
        });

        let roots = validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", bundle.path())]),
            &[resolved_with_processor("deskbar", "fleet")],
        );
        assert!(roots.withheld.contains_key("fleet"));
    }

    /// The phase ordering: the version verdict is read off a shape that is only
    /// `{ v }`, so a record from a version that added a field is withheld as the
    /// skew it is rather than refused for the field's name.
    #[test]
    fn a_stale_record_of_a_foreign_shape_is_withheld_not_parsed() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_processor_tree(bundle.path(), "fleet", &["ports"], |m| {
            m["v"] = serde_json::json!(4);
            m["a_field_from_the_future"] = serde_json::json!("whatever");
        });

        let roots = validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", bundle.path())]),
            &[],
        );
        assert_eq!(
            roots.withheld_kind("fleet").map(|held| held.record_v),
            Some(4)
        );
    }

    /// The other half of the ordering: a record whose `v` this server *does*
    /// read is held to the whole shape, wherever it is installed. A foreign
    /// field there is build drift, not version skew.
    #[test]
    #[should_panic(expected = "does not parse")]
    fn a_current_record_of_a_foreign_shape_still_panics_under_a_bundle_mount() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_processor_tree(bundle.path(), "fleet", &["ports"], |m| {
            m["a_field_from_the_future"] = serde_json::json!("whatever");
        });
        validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", bundle.path())]),
            &[],
        );
    }

    /// A withheld kind's tree is a real surface tree, so the root it lives under
    /// offers something — the "mount path one directory off" refusal must not
    /// fire on it.
    #[test]
    fn a_root_offering_only_a_withheld_kind_offers_something() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_processor_tree(bundle.path(), "fleet", &["ports"], |m| {
            m["v"] = serde_json::json!(2);
        });
        validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", bundle.path())]),
            &[],
        );
    }

    /// A kind in neither map is still the configuration error it was: withheld
    /// is a verdict about an installed tree, not a way to accept a kind nothing
    /// offers.
    #[test]
    #[should_panic(expected = "which no installed surface tree offers")]
    fn a_kind_no_root_offers_is_still_refused_beside_a_withheld_one() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_processor_tree(bundle.path(), "fleet", &["ports"], |m| {
            m["v"] = serde_json::json!(2);
        });
        validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", bundle.path())]),
            &[resolved_with_processor("deskbar", "absent")],
        );
    }

    /// Withheld → served is an `Offered` difference and served → withheld a
    /// `Withdrawn` one, which is what promotes the surfaces stamping the kind to
    /// `changed` at both transitions. Both follow from a kind being in `kinds`
    /// or not, with no reference to the withheld map.
    #[test]
    fn the_withheld_transitions_are_ordinary_kind_differences() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_processor_tree(bundle.path(), "fleet", &["ports"], |m| {
            m["v"] = serde_json::json!(2);
        });
        let mounts = mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", bundle.path())]);
        let withholding = validate_surface_assets(&mounts, &[]);

        // The bundle is re-released against this brenn.
        write_processor_tree(bundle.path(), "fleet", &["ports"], |_| {});
        let serving = validate_surface_assets(&mounts, &[]);

        let offered = withholding.kind_differences(&serving);
        assert!(matches!(
            offered.get("fleet"),
            Some(KindDifference::Offered { mount }) if mount == "fleet-bundle"
        ));
        let withdrawn = serving.kind_differences(&withholding);
        assert!(matches!(
            withdrawn.get("fleet"),
            Some(KindDifference::Withdrawn { mount, .. }) if mount == "fleet-bundle"
        ));
    }

    /// A bundle re-installed and *still* unreadable is a difference, on either
    /// witness it moved on.
    ///
    /// This is the case the withholding exists for: the operator acted on the
    /// alert and the kind is still dead. Comparing only the served map would
    /// make the two scans identical, so the reload would report that nothing
    /// moved, keep the boot-time reason in the description documents, and say
    /// nothing — over an install the operator is waiting on.
    #[test]
    fn a_rereleased_bundle_this_host_still_cannot_read_is_a_difference() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        let moved = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_processor_tree(bundle.path(), "fleet", &["ports"], |m| {
            m["v"] = serde_json::json!(2);
        });
        let held = validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", bundle.path())]),
            &[],
        );

        // Rebuilt, but against a brenn newer than this one.
        write_processor_tree(bundle.path(), "fleet", &["ports"], |m| {
            m["v"] = serde_json::json!(crate::processor_assets::MANIFEST_VERSION + 1);
        });
        let rebuilt = validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", bundle.path())]),
            &[],
        );
        assert!(
            matches!(
                held.kind_differences(&rebuilt).get("fleet"),
                Some(KindDifference::Reinstalled { mount, .. }) if mount == "fleet-bundle"
            ),
            "{:?}",
            held.kind_differences(&rebuilt),
        );

        // Not rebuilt at all: the same stale record behind a fresh versioned
        // tree, which is what a re-install without a rebuild looks like.
        write_processor_tree(moved.path(), "fleet", &["ports"], |m| {
            m["v"] = serde_json::json!(2);
        });
        let reinstalled = validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("fleet-bundle", moved.path())]),
            &[],
        );
        assert!(
            matches!(
                held.kind_differences(&reinstalled).get("fleet"),
                Some(KindDifference::Reinstalled { root, .. }) if root == moved.path()
            ),
            "{:?}",
            held.kind_differences(&reinstalled),
        );

        // And the byte-identical re-scan is still nothing.
        assert!(held.kind_differences(&held.clone()).is_empty());
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
            &mount_roots(&[("brenn", brenn.path()), ("bundle", bundle.path())]),
            &[surface],
        );
        assert_eq!(
            roots.kernel.as_ref().map(|kernel| kernel.root.as_path()),
            Some(brenn.path())
        );
        assert_eq!(roots.kind_root("chrome"), Some(brenn.path()));
        assert_eq!(roots.kind_root("demo-panel"), Some(bundle.path()),);
    }

    /// The kind-grain pass is not driven by the configuration either: a kind a
    /// mount offers and nothing instantiates is read, verified and
    /// fingerprinted, which is what lets a later comparison of two root sets
    /// see the install that changed it.
    #[test]
    fn an_offered_kind_is_fingerprinted_even_when_nothing_instantiates_it() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_valid_kind(brenn.path(), "chrome");
        write_valid_kind(bundle.path(), "demo-panel");

        let scan = || {
            validate_surface_assets(
                &mount_roots(&[("brenn", brenn.path()), ("bundle", bundle.path())]),
                &[],
            )
        };
        let before = scan();
        let held = before.kinds.get("demo-panel").expect("the kind is mapped");
        assert_eq!(held.root, bundle.path());
        assert_eq!(held.spec_sha256, fixture_spec_hash("demo-panel"));
        assert_eq!(
            held.source_sha256,
            brenn_lib::util::sha256_hex(b"component-bytes-for-demo-panel"),
        );

        write_processor_tree_from_bytes(
            bundle.path(),
            "demo-panel",
            b"component-bytes-for-demo-panel-v2",
            &spec_bytes_for("demo-panel"),
            Vec::new(),
            true,
            |_| {},
        );
        let after = scan();
        assert_ne!(before, after, "the upgrade moves the root set");
        assert_eq!(
            after.kinds["demo-panel"].root, before.kinds["demo-panel"].root,
            "the path is unmoved; the fingerprint is the whole of the difference",
        );
        assert_ne!(
            after.kinds["demo-panel"].source_sha256, held.source_sha256,
            "the artifact hash follows the installed bytes",
        );
    }

    // -- SurfaceRoots::kind_differences -----------------------------------

    /// A `SurfaceRoots` spelled directly, as `(kind, mount, root, source, spec)`:
    /// these cases are about the comparison, not about what a scan produces.
    fn roots_with(kinds: &[(&str, &str, &str, &str, &str)]) -> SurfaceRoots {
        SurfaceRoots {
            kernel: Some(KernelRoot::for_test("/kernel")),
            withheld: Default::default(),
            kinds: kinds
                .iter()
                .map(|(kind, mount, root, source, spec)| {
                    (
                        (*kind).to_string(),
                        KindRoot {
                            mount: (*mount).to_string(),
                            root: std::path::PathBuf::from(root),
                            source_sha256: (*source).to_string(),
                            spec_sha256: (*spec).to_string(),
                            cores: Vec::new(),
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn two_identical_root_sets_differ_on_no_kind() {
        let held = roots_with(&[
            ("chart", "brenn", "/a", "s1", "p1"),
            ("chrome", "brenn", "/a", "s2", "p2"),
        ]);
        assert!(held.kind_differences(&held.clone()).is_empty());
    }

    /// The versioned-tree install: the mount symlink is swapped onto a fresh
    /// directory, so every kind under it resolves to a new canonical path with
    /// byte-identical contents. That is a relocation, not a change — treating
    /// it as one would make every bundle deploy a restart.
    #[test]
    fn a_kind_whose_tree_relocated_under_one_mount_is_not_a_difference() {
        let held = roots_with(&[("chart", "bundle", "/bundle.v1/surface", "s1", "p1")]);
        let scanned = roots_with(&[("chart", "bundle", "/bundle.v2/surface", "s1", "p1")]);
        assert!(
            held.kind_differences(&scanned).is_empty(),
            "same mount, same bytes, new path",
        );
    }

    #[test]
    fn a_kind_offered_by_another_mount_is_moved() {
        let held = roots_with(&[("chart", "brenn", "/a", "s1", "p1")]);
        let scanned = roots_with(&[("chart", "bundle", "/b", "s1", "p1")]);
        assert_eq!(
            held.kind_differences(&scanned),
            BTreeMap::from([(
                "chart".to_string(),
                KindDifference::Moved {
                    from: "brenn".to_string(),
                    to: "bundle".to_string(),
                },
            )]),
        );
    }

    /// The bundle-upgrade shape: the symlink is swapped, so the path an
    /// operator reads is unchanged and only the fingerprints move.
    #[test]
    fn a_kind_whose_bytes_moved_under_one_mount_is_reinstalled() {
        let held = roots_with(&[("chart", "bundle", "/a", "s1", "p1")]);
        let source = roots_with(&[("chart", "bundle", "/a", "s2", "p1")]);
        let spec = roots_with(&[("chart", "bundle", "/a", "s1", "p2")]);
        let expected = BTreeMap::from([(
            "chart".to_string(),
            KindDifference::Reinstalled {
                mount: "bundle".to_string(),
                root: std::path::PathBuf::from("/a"),
            },
        )]);
        assert_eq!(held.kind_differences(&source), expected);
        assert_eq!(held.kind_differences(&spec), expected);
    }

    #[test]
    fn a_kind_no_mount_offers_any_more_is_withdrawn_and_a_new_one_is_offered() {
        let held = roots_with(&[("chart", "brenn", "/a", "s1", "p1")]);
        let scanned = roots_with(&[("gauge", "bundle", "/b", "s3", "p3")]);
        assert_eq!(
            held.kind_differences(&scanned),
            BTreeMap::from([
                (
                    "chart".to_string(),
                    KindDifference::Withdrawn {
                        mount: "brenn".to_string(),
                        root: std::path::PathBuf::from("/a"),
                    },
                ),
                (
                    "gauge".to_string(),
                    KindDifference::Offered {
                        mount: "bundle".to_string(),
                    },
                ),
            ]),
        );
    }

    /// The comparison is directional: what the held set calls withdrawn the
    /// scanned set calls newly offered.
    #[test]
    fn the_comparison_reads_from_the_held_sets_side() {
        let held = roots_with(&[("chart", "brenn", "/a", "s1", "p1")]);
        let scanned = roots_with(&[]);
        assert_eq!(
            held.kind_differences(&scanned)["chart"],
            KindDifference::Withdrawn {
                mount: "brenn".to_string(),
                root: std::path::PathBuf::from("/a"),
            },
        );
        assert_eq!(
            scanned.kind_differences(&held)["chart"],
            KindDifference::Offered {
                mount: "brenn".to_string(),
            },
        );
    }

    /// The reload framing of an asset failure. A running process is still
    /// serving the document it has, so the message names the reload and must
    /// never tell the operator that anything refused to start.
    #[test]
    #[should_panic(expected = "reload:")]
    fn a_reload_framed_asset_failure_names_the_reload() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_empty_kind_dir(bundle.path(), "demo-panel");
        validate_surface_assets_in(
            AssetContext::RELOAD,
            &mount_roots(&[("brenn", brenn.path()), ("bundle", bundle.path())]),
            &[],
        );
    }

    /// The other half of the same contract, which `should_panic` alone cannot
    /// see: the reload framing appends no verdict, so "Refusing to start" is
    /// not in the message a live process produces.
    #[test]
    fn a_reload_framed_asset_failure_does_not_say_refusing_to_start() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_empty_kind_dir(bundle.path(), "demo-panel");
        let roots = mount_roots(&[("brenn", brenn.path()), ("bundle", bundle.path())]);
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            validate_surface_assets_in(AssetContext::RELOAD, &roots, &[]);
        }))
        .expect_err("a kind with no manifest is refused");
        let message = payload
            .downcast_ref::<String>()
            .expect("the panic payload is a formatted message")
            .clone();
        assert!(message.starts_with("reload:"), "{message}");
        assert!(!message.contains("Refusing to start"), "{message}");
    }

    /// The cost of fingerprinting every offered kind: a mount shipping a broken
    /// tree is refused at boot whether or not today's document names it. That
    /// is the intent — the tree is installed and would be served the moment a
    /// surface stamped it.
    #[test]
    #[should_panic(expected = "has no readable asset manifest")]
    fn an_offered_kind_with_no_manifest_is_refused_even_when_unconfigured() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_empty_kind_dir(bundle.path(), "demo-panel");
        validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("bundle", bundle.path())]),
            &[],
        );
    }

    /// The limit of that cost: an offered kind is held to its record, not to a
    /// digest of its bytes. A tree whose artifact hash does not match its
    /// manifest boots while nothing instantiates it, because re-hashing every
    /// installed component on every scan buys nothing for a kind no page can
    /// reach — and the same tree is refused the moment a surface names it (the
    /// stale-transpile case above).
    #[test]
    fn an_offered_kind_that_nothing_instantiates_is_not_rehashed() {
        let brenn = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_processor_tree(brenn.path(), "transplant", &["ports"], |m| {
            m["source_sha256"] = serde_json::json!("00".repeat(32));
        });
        let roots = validate_surface_assets(&mount_roots(&[("brenn", brenn.path())]), &[]);
        assert_eq!(
            roots.kinds["transplant"].source_sha256,
            "00".repeat(32),
            "the fingerprint is the record's, which is what a scan comparison reads",
        );
    }

    /// The scan is not driven by the configuration: a kind installed twice is
    /// an ambiguous deploy whether or not anything mounts it today.
    #[test]
    #[should_panic(
        expected = "surface kind `chrome` is installed under more than one mount: brenn, bundle"
    )]
    fn a_kind_offered_by_two_roots_is_refused_even_when_unconfigured() {
        let brenn = tempfile::tempdir().expect("tempdir");
        let bundle = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(brenn.path());
        write_valid_kind(brenn.path(), "chrome");
        write_valid_kind(bundle.path(), "chrome");
        validate_surface_assets(
            &mount_roots(&[("brenn", brenn.path()), ("bundle", bundle.path())]),
            &[],
        );
    }

    #[test]
    #[should_panic(expected = "trees hold the kernel module pair")]
    fn two_kernel_roots_are_refused() {
        let one = tempfile::tempdir().expect("tempdir");
        let two = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(one.path());
        write_kernel_pair(two.path());
        validate_surface_assets(
            &mount_roots(&[("brenn", one.path()), ("other", two.path())]),
            &[],
        );
    }

    /// A bundle root alone: kinds and no kernel, which is a mis-declared mount
    /// rather than a bundle's fault.
    #[test]
    #[should_panic(expected = "no declared mount's `surface/` tree holds the kernel module pair")]
    fn a_root_set_with_no_kernel_is_refused() {
        let bundle = tempfile::tempdir().expect("tempdir");
        write_valid_kind(bundle.path(), "demo-panel");
        validate_surface_assets(&mount_roots(&[("bundle", bundle.path())]), &[]);
    }

    /// The realistic mis-pointing: a mount whose `surface/` tree is the
    /// bundle's install root one level up. It holds no kernel and no
    /// `processor/`, so
    /// nothing downstream would notice until some later configuration first
    /// stamps the kind that was supposed to be there.
    #[test]
    #[should_panic(expected = "`surface/` tree(s) offer nothing")]
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
        .expect("the bundle's real surface tree, one level below the mount's");
        validate_surface_assets(
            &mount_roots(&[
                ("brenn", brenn.path()),
                ("bundle", one_directory_off.path()),
            ]),
            &[],
        );
    }

    #[test]
    #[should_panic(expected = "name the same directory")]
    fn the_same_root_named_twice_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_kernel_pair(dir.path());
        validate_surface_assets(
            &mount_roots(&[("brenn", dir.path()), ("other", &dir.path().join("."))]),
            &[],
        );
    }
}

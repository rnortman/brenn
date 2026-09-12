//! The mounts document: which installed trees the host may read.
//!
//! A **mount** is one directory in the bundle shape — `components/`,
//! `surface/`, `modules/`, `config/`, whichever the release ships, plus a
//! `VERSION` file — that the operator has *declared* to the host.  The
//! declaration is the act of consent: what is under a declared mount may be
//! resolved, loaded and served; what is not declared is invisible.  Brenn's
//! own release is a mount like any other.  Nothing discovers a mount — no
//! directory scan, no installer — so the only way a tree becomes readable is
//! that someone wrote a line naming it.
//!
//! The document is read in two stages, and the split is what the `mounts`
//! subcommand and the installer need:
//!
//! - [`compile_mounts`] answers "is this a mounts document, and what does it
//!   declare" — the compile, the lowering, and the checks that read no
//!   environment: a `path` is a string, it is absolute, and no two declared
//!   paths are the same or nested. An installer runs this against a mount whose
//!   directory does not exist yet.
//! - [`MountsDocument::verify`] answers "is every declaration installed" — the
//!   canonicalization, the `VERSION` file, and the trees. Boot and reload run
//!   both; a fault here is a boot panic or a reload refusal, never a mount that
//!   quietly contributes nothing.
//!
//! Canonicalization matters because a bundle's mount path is a symlink to a
//! versioned sibling: the installer swaps the symlink atomically, so a read
//! sees the old tree or the new and never a half-copy. The host canonicalizes
//! once per read and uses the canonical path for every scan.

use std::path::{Path, PathBuf};

use brenn_dsl::diag::Diagnostic;
use brenn_dsl::roots::RootList;
use brenn_dsl::{DocumentInputs, MountedRoot, Span, Spanned};

use super::dsl_lower::{expect_str, keep};

/// The name of the file that says which revision of a mount is installed.
const VERSION_FILE: &str = "VERSION";

/// One of the four trees a mount may offer.
///
/// Four, because the four have four different consumers with four different
/// install cadences: `modules/` is read by the compiler at import, `config/` by
/// the compiler at document load, `components/` by the WASM loader, `surface/`
/// by the HTTP server. The unification a mount performs is at the operator's
/// surface and the install unit, not in the loaders.
///
/// `config/` is the one tree a mount does not merely *offer*: its entry
/// document is compiled as part of the deployment, under the ceiling of the
/// principal the mount is declared `under`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MountTree {
    Components,
    Surface,
    Modules,
    Config,
}

impl MountTree {
    /// Every tree, in the order a listing names them.
    pub const ALL: [MountTree; 4] = [
        MountTree::Components,
        MountTree::Surface,
        MountTree::Modules,
        MountTree::Config,
    ];

    /// The subdirectory of a mount this tree is installed as.
    pub fn dir_name(self) -> &'static str {
        match self {
            MountTree::Components => "components",
            MountTree::Surface => "surface",
            MountTree::Modules => "modules",
            MountTree::Config => "config",
        }
    }
}

/// One `mount name { path = "…"; }`, as the document declares it.
///
/// The path is what was written, not what it resolves to: an installer reads
/// these to learn where to install, and the directory may not exist yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountDecl {
    pub name: String,
    pub path: PathBuf,
    /// The principal this mount's `config/` tree runs under, as written, with
    /// the span of the `under` clause. `None` for a mount that carries no
    /// config.
    pub under: Option<Spanned<String>>,
    /// Where the `path` value was written, so an on-disk fault found later can
    /// still be cited at the value that caused it.
    pub span: Span,
    /// Where the mount's own name was written. Distinct from `span` because a
    /// fault about the *mount* — its config tree, its ceiling, its name — is
    /// not a fault about the path it declares, and sending an operator to the
    /// `path` value to answer one is sending them to the wrong word.
    pub name_span: Span,
}

/// A compiled mounts document: what it declares.
#[derive(Debug, Clone)]
pub struct MountsDocument {
    pub mounts: Vec<MountDecl>,
}

/// One declared mount, verified against the filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    pub name: String,
    /// The declared path, canonicalized: what every scan reads and what a `Verified`
    /// package reports as its root.
    pub path: PathBuf,
    /// The first line of the mount's `VERSION` file.
    pub version: String,
    /// Which of the four trees this mount offers, in [`MountTree::ALL`] order.
    pub trees: Vec<MountTree>,
    /// The principal this mount's `config/` tree runs under, as written.
    pub under: Option<Spanned<String>>,
    /// Where the mount's name was written. Carried forward from the
    /// declaration because a mount's config is compiled as part of the
    /// deployment, and every diagnostic about what it declares has to be able
    /// to cite the declaration that admitted it.
    pub span: Span,
}

/// Every mount the host has been told about.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MountsConfig {
    pub mounts: Vec<Mount>,
}

/// The roots the mounts derive, in declaration order.
///
/// A mount that does not offer a tree contributes no root for it.
///
/// `config_roots` is not a [`RootList`] and is not searched: a config root is
/// loaded, one document per mount, so what a reader needs of it is the mount's
/// name and its ceiling rather than a search order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Roots {
    pub module_roots: RootList,
    pub components_roots: RootList,
    pub surface_roots: RootList,
    pub config_roots: Vec<MountedRoot>,
}

impl Roots {
    /// The roots a workstation tool derives from its flags: the `--modules`
    /// list, and one config root per `--mounted` flag.
    ///
    /// The two flags are separate because the host's own mounts document is not
    /// available off-host. A root that declares a ceiling `principal` needs a
    /// mount under it to be live, so a check with no `--mounted` refuses one —
    /// correctly for what it read. `--mounted` is how the caller says which
    /// mounts the host declares; what it points `DIR` at is either an empty
    /// stub (certifying the root's ceilings and nothing about any fragment) or
    /// the real tree, where the check runs where the fragment lives.
    pub fn modules(module_roots: RootList, config_roots: Vec<MountedRoot>) -> Self {
        Self {
            module_roots,
            config_roots,
            ..Self::default()
        }
    }
}

impl Default for Roots {
    /// What a host with no declared mount has: three empty lists, each still
    /// *sourced* at the mounts. The source is what a diagnostic reads for its
    /// remedy, and `RootList`'s own default is the workstation flag form — so
    /// deriving this would have a host tell an operator to pass `--modules`,
    /// which is a flag `serve` refuses.
    fn default() -> Self {
        let empty = |tree: MountTree| RootList::mounts(tree.dir_name(), Vec::new());
        Self {
            module_roots: empty(MountTree::Modules),
            components_roots: empty(MountTree::Components),
            surface_roots: empty(MountTree::Surface),
            config_roots: Vec::new(),
        }
    }
}

/// A mounts document, read and verified, with its roots derived.
#[derive(Debug, Clone)]
pub struct LoadedMounts {
    /// Where the document was read from, or `None` for a host with no
    /// `--mounts`. Held here rather than beside the value it produced, so a
    /// re-read cannot be aimed at a different file than the one the running
    /// mounts came from.
    pub path: Option<PathBuf>,
    pub config: MountsConfig,
    pub roots: Roots,
}

impl LoadedMounts {
    /// What a boot with no `--mounts` is running: no mount at all, and so no
    /// components, no surfaces and no packaged vocabulary.
    fn empty() -> Self {
        Self {
            path: None,
            config: MountsConfig::default(),
            roots: Roots::default(),
        }
    }
}

/// What one declared mount looks like on disk, for a listing.
///
/// Distinct from [`MountsDocument::verify`]'s all-or-nothing answer because the
/// installer needs the per-mount detail: it installs into a mount that is
/// `Missing` today, and refuses when a *different* mount is anything but `Ok`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MountStatus {
    /// The canonical path exists and passes every on-disk check.
    Ok {
        version: String,
        trees: Vec<MountTree>,
    },
    /// The declared path does not exist. Legal in a document an installer is
    /// about to satisfy; a refusal at boot and at reload.
    Missing,
    /// The path exists and fails an on-disk check.
    Fault(String),
}

impl MountsConfig {
    /// The roots the mounts derive, in declaration order.
    pub fn roots(&self) -> Roots {
        let roots_of = |tree: MountTree| -> RootList {
            RootList::mounts(
                tree.dir_name(),
                self.mounts
                    .iter()
                    .filter(|mount| mount.trees.contains(&tree))
                    .map(|mount| (mount.name.clone(), mount.path.join(tree.dir_name())))
                    .collect(),
            )
        };
        // A mount offering `config/` is `under` a principal: `verify_one`
        // refuses the pair in either order, so the `expect` is that refusal
        // having run.
        let config_roots = self
            .mounts
            .iter()
            .filter(|mount| mount.trees.contains(&MountTree::Config))
            .map(|mount| {
                let under = mount
                    .under
                    .as_ref()
                    .expect("a mount offering `config/` is under a principal");
                MountedRoot {
                    mount: mount.name.clone(),
                    dir: mount.path.join(MountTree::Config.dir_name()),
                    under: under.value().clone(),
                    span: mount.span.clone(),
                    under_span: under.span().clone(),
                }
            })
            .collect();
        Roots {
            module_roots: roots_of(MountTree::Modules),
            components_roots: roots_of(MountTree::Components),
            surface_roots: roots_of(MountTree::Surface),
            config_roots,
        }
    }
}

/// What the deployment document compiles against, on a host with these mounts.
///
/// The one place the root, the module roots and the config-carrying mounts are
/// put together. Boot, reload step 1 and `config-check --mounts` all reach the
/// compiler through it, so the three cannot disagree about which fragments are
/// part of the document — a disagreement that would let a check certify a
/// document the host then refuses, or the reverse.
///
/// A free function rather than an inherent constructor because
/// [`DocumentInputs`] is `brenn-dsl`'s and [`Roots`] is this crate's.
pub fn deployment_inputs(root: &Path, roots: &Roots) -> DocumentInputs {
    DocumentInputs {
        root: root.to_path_buf(),
        module_roots: roots.module_roots.clone(),
        mounted: roots.config_roots.clone(),
        role: brenn_dsl::DocumentRole::Deployment,
    }
}

/// Compile a mounts document and apply the checks that read no environment.
///
/// The role is what refuses a deployment statement here and a `mount` there;
/// the module roots are empty because a mounts document imports nothing.
pub fn compile_mounts(path: &Path) -> Result<MountsDocument, String> {
    match path.extension().and_then(std::ffi::OsStr::to_str) {
        Some("brenn") => {}
        _ => {
            return Err(format!(
                "mounts file {}: unrecognized extension — a mounts document is a `.brenn` file",
                path.display(),
            ));
        }
    }
    let inputs = DocumentInputs::mounts(path);
    let derived =
        brenn_dsl::compile(&inputs).map_err(|diagnostics| render(path, "compile", &diagnostics))?;

    let mut errors = Vec::new();
    let mut mounts = Vec::new();
    for declared in &derived.resolved.mounts {
        let value = &declared.attrs.path.value;
        let Some(text) = keep(expect_str(value, "path"), &mut errors) else {
            continue;
        };
        if let Some(bad) = text.chars().find(|c| c.is_control()) {
            errors.push(Diagnostic::at(
                format!(
                    "mount `{}`: `path` holds the control character {bad:?}. A mount path is \
                     printed as one tab-separated field of the `brenn mounts` listing an \
                     installer reads, and no such path is ever intended",
                    declared.handle.dotted(),
                ),
                value.span().clone(),
            ));
            continue;
        }
        let declared_path = PathBuf::from(&text);
        if !declared_path.is_absolute() {
            errors.push(Diagnostic::at(
                format!(
                    "mount `{}`: `path` is `{text}`, which is not an absolute path. A mount \
                     path is where the host reads a tree from, and the host's working \
                     directory is not the operator's",
                    declared.handle.dotted(),
                ),
                value.span().clone(),
            ));
            continue;
        }
        // The `under` handle is not resolved here: a mounts document declares
        // no principal, and the name has to resolve in the deployment document
        // the ceiling governs.
        let under = declared.under.as_ref().map(|handle| {
            Spanned::new(
                handle.dotted(),
                declared
                    .under_span
                    .clone()
                    .expect("an `under` handle carries its clause's span"),
            )
        });
        let name_span = declared
            .handle
            .0
            .first()
            .expect("a mount handle is one segment")
            .span()
            .clone();
        mounts.push(MountDecl {
            name: declared.handle.dotted(),
            path: declared_path,
            under,
            span: value.span().clone(),
            name_span,
        });
    }
    let entries: Vec<(&str, &Path, &Span)> = mounts
        .iter()
        .map(|mount| (mount.name.as_str(), mount.path.as_path(), &mount.span))
        .collect();
    check_disjoint(&entries, "declared", &mut errors);
    if !errors.is_empty() {
        return Err(render(path, "read", &errors));
    }
    Ok(MountsDocument { mounts })
}

impl MountsDocument {
    /// Verify every declaration against the filesystem.
    ///
    /// All-or-nothing, and every fault reported rather than the first: an
    /// operator who mistyped two paths should learn both from one reload.
    pub fn verify(&self, path: &Path) -> Result<MountsConfig, String> {
        let mut errors = Vec::new();
        let mut mounts = Vec::new();
        for declared in &self.mounts {
            match verify_one(declared) {
                Ok(mount) => mounts.push((mount, &declared.span)),
                Err(message) => errors.push(Diagnostic::at(message, declared.span.clone())),
            }
        }
        // The declared paths are already pairwise disjoint; two of them can
        // still canonicalize to one tree through symlinks, which is a mount
        // installed twice under two names.
        let entries: Vec<(&str, &Path, &Span)> = mounts
            .iter()
            .map(|(mount, span)| (mount.name.as_str(), mount.path.as_path(), *span))
            .collect();
        check_disjoint(&entries, "canonical", &mut errors);
        if !errors.is_empty() {
            return Err(render(path, "verify", &errors));
        }
        Ok(MountsConfig {
            mounts: mounts.into_iter().map(|(mount, _)| mount).collect(),
        })
    }

    /// What each declaration looks like on disk, in declaration order.
    pub fn statuses(&self) -> Vec<(&MountDecl, MountStatus)> {
        self.mounts
            .iter()
            .map(|declared| {
                let status = match declared.path.try_exists() {
                    Ok(false) => MountStatus::Missing,
                    Ok(true) => match verify_one(declared) {
                        Ok(mount) => MountStatus::Ok {
                            version: mount.version,
                            trees: mount.trees,
                        },
                        Err(message) => MountStatus::Fault(message),
                    },
                    // Neither present nor absent — an unreadable parent, a
                    // symlink loop. Read as `missing` the operator would be
                    // told to install what is already there.
                    Err(error) => MountStatus::Fault(format!("path cannot be read: {error}")),
                };
                (declared, status)
            })
            .collect()
    }
}

/// One declaration, verified: canonical path, `VERSION`, trees.
fn verify_one(declared: &MountDecl) -> Result<Mount, String> {
    let name = &declared.name;
    let path = declared.path.canonicalize().map_err(|error| {
        format!(
            "mount `{name}`: {} cannot be resolved: {error}",
            declared.path.display(),
        )
    })?;
    if !path.is_dir() {
        return Err(format!(
            "mount `{name}`: {} is not a directory",
            path.display(),
        ));
    }
    let version_path = path.join(VERSION_FILE);
    let version = std::fs::read_to_string(&version_path).map_err(|error| {
        format!(
            "mount `{name}`: no readable {VERSION_FILE} at {}: {error} — the install is \
             incomplete, or this directory is not a mount",
            version_path.display(),
        )
    })?;
    let version = version
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();
    if version.is_empty() {
        return Err(format!(
            "mount `{name}`: {} is empty",
            version_path.display(),
        ));
    }
    let trees: Vec<MountTree> = MountTree::ALL
        .into_iter()
        .filter(|tree| path.join(tree.dir_name()).is_dir())
        .collect();
    if trees.is_empty() {
        return Err(format!(
            "mount `{name}`: {} holds none of `components/`, `surface/`, `modules/`, \
             `config/`, so it offers the host nothing — the path is one directory off, or \
             the install did not finish",
            path.display(),
        ));
    }
    // A `config/` tree and an `under` clause are one declaration written in two
    // places, and either without the other is a fault. A `config/` tree under
    // nobody would be text the compiler has no ceiling for; an `under` on a
    // mount that carries none is a ceiling over nothing, and it is not a cap on
    // the mount's other trees — what an instance may hold is written at the
    // instantiation site.
    match (&declared.under, trees.contains(&MountTree::Config)) {
        (None, true) => {
            return Err(format!(
                "mount `{name}` carries config and is under no principal: a mount's config \
                 runs under a ceiling, and the root document is the only text that runs \
                 under none",
            ));
        }
        (Some(under), false) => {
            return Err(format!(
                "mount `{name}` is under `{}` and carries no config: a ceiling caps what a \
                 mount's config declares, and this mount declares nothing",
                under.value(),
            ));
        }
        (None, false) | (Some(_), true) => {}
    }
    Ok(Mount {
        name: name.clone(),
        path,
        version,
        trees,
        under: declared.under.clone(),
        span: declared.name_span.clone(),
    })
}

/// Refuse a list in which two mounts name one directory or one inside another.
///
/// Nesting is refused rather than tolerated because a nested mount makes one
/// mount's contents another's: every name under the inner tree would be
/// installed under two roots, and the scans would refuse the pair with a
/// sentence about duplicate modules rather than about the arrangement that
/// caused them. `what` names which paths are being compared — the declared
/// ones or the canonical ones — so the two passes read differently.
fn check_disjoint(entries: &[(&str, &Path, &Span)], what: &str, errors: &mut Vec<Diagnostic>) {
    for (index, (name, here, _)) in entries.iter().enumerate() {
        for (other_name, there, other_span) in &entries[index + 1..] {
            let message = if here == there {
                format!(
                    "mounts `{name}` and `{other_name}` have the same {what} path {}: one \
                     installed tree is one mount",
                    here.display(),
                )
            } else if here.starts_with(there) || there.starts_with(here) {
                format!(
                    "mount `{name}` ({}) is inside mount `{other_name}` ({}): a mount holds \
                     its own trees and no other mount's",
                    here.display(),
                    there.display(),
                )
            } else {
                continue;
            };
            errors.push(Diagnostic::at(message, (*other_span).clone()));
        }
    }
}

/// A run of diagnostics as one report, naming the file and the stage.
fn render(path: &Path, stage: &str, diagnostics: &[Diagnostic]) -> String {
    format!(
        "failed to {stage} mounts file {}:\n{}",
        path.display(),
        brenn_dsl::diag::render_all(diagnostics),
    )
}

/// Read and verify the mounts document, reporting instead of dying.
///
/// `None` means no `--mounts` was given, which is zero mounts: a dev server
/// with no components, no surfaces and no packaged imports. It is not an error,
/// and it is not a default document — there is nothing to read.
pub fn try_load_mounts(path: Option<&Path>) -> Result<LoadedMounts, String> {
    let Some(path) = path else {
        return Ok(LoadedMounts::empty());
    };
    let document = compile_mounts(path)?;
    let config = document.verify(path)?;
    let roots = config.roots();
    Ok(LoadedMounts {
        path: Some(path.to_path_buf()),
        config,
        roots,
    })
}

/// Read and verify the mounts document, dying on any refusal.
///
/// Boot is [`try_load_mounts`] plus the one thing a boot does that a check does
/// not. One dispatch, so what a reload accepts and what boots cannot diverge in
/// either direction.
///
/// # Panics
///
/// Panics on any refusal: a mounts document that does not compile, a path that
/// is not absolute, a mount that is not installed. Every one of them means the
/// host would serve a different set of trees than the operator declared.
pub fn load_mounts(path: Option<&Path>) -> LoadedMounts {
    try_load_mounts(path).unwrap_or_else(|report| panic!("{report}"))
}

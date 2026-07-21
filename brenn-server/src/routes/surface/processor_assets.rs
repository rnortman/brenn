//! Boot validation for `abi = "processor"` surface assets.
//!
//! A processor kind ships as a jco-transpiled tree under
//! `<surface_dist_dir>/processor/<kind>/`: the transpiled JS + core wasm, the
//! source component artifact it was transpiled from, and a `manifest.json`
//! recording the source hash, the pinned jco version, the component's WIT import
//! list, and the emitted file set.
//!
//! Every check here is a named boot panic. The trust argument: the manifest is
//! operator-deployed build output, and the source-hash check binds it to the
//! shipped component bytes — a manifest whose `imports` lies about its own
//! artifact requires tampering with the deploy, which is outside this
//! validation's threat model. In-page separation is bug containment, not a
//! security boundary; the server-side gates on what a page *does* are unchanged,
//! and the browser-side backstop is structural (the kernel supplies only the
//! four surface imports, so a lying manifest yields an instantiation failure,
//! never a capability).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Manifest schema version this server understands. A tree written by a
/// different version is a deploy/toolchain mismatch, not something to
/// best-effort parse.
const MANIFEST_VERSION: u32 = 1;

/// The package namespace every processor host interface lives under. An import
/// outside this namespace (a stray `wasi:*` a dependency dragged in, or a future
/// `brenn:` sibling package) names something no surface host provides, so it is
/// rejected at boot rather than left to fail at browser `instantiate`. Stripping
/// to a bare interface name before this check would be worse than useless: a
/// foreign `wasi:logging/log` would masquerade as the surface `log`.
const PROCESSOR_PACKAGE: &str = "brenn:processor";

/// The WIT interfaces a surface-hosted processor may import: the transpilable
/// profile. `store`/`mqtt`/`tools` are backend-only in v1.
///
/// `types` is in the set but is not a capability: it defines the shared record
/// and enum shapes the other interfaces speak, so every processor imports it and
/// no host implements it (jco resolves it structurally). It is listed here
/// because the manifest reports the world's imports truthfully, and a
/// type-carrying import must not read as an unsatisfiable one.
const SURFACE_IMPORTS: [&str; 5] = ["types", "ports", "log", "alert", "config"];

/// Every WIT interface name `processor.wit` defines. An import outside this set
/// is manifest/toolchain drift (the build wrote a name no world declares), which
/// is a different operator problem from declaring a backend-only component on a
/// surface — and gets its own panic.
const KNOWN_IMPORTS: [&str; 8] = [
    "types", "ports", "log", "alert", "config", "store", "mqtt", "tools",
];

/// The build manifest emitted beside a transpiled processor kind.
///
/// `deny_unknown_fields` is deliberate: an unrecognized key means the build
/// wrote a manifest this server does not understand, and silently ignoring it
/// would let a newer build's semantics pass validation under older rules.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessorManifest {
    /// Manifest schema version; must equal [`MANIFEST_VERSION`].
    pub v: u32,
    /// The kind this tree backs. Must match the directory it was found in.
    pub kind: String,
    /// SHA-256 of the component artifact the transpile consumed, hex-encoded.
    pub source_sha256: String,
    /// The pinned jco version that produced the tree. Provenance for debugging
    /// only — the source hash is the staleness authority, and a second authority
    /// would only invite the two to disagree. Declared (rather than dropped)
    /// because `deny_unknown_fields` would otherwise reject every manifest the
    /// build writes.
    #[allow(dead_code)]
    pub jco_version: String,
    /// The component world's import list, extracted from the artifact at build
    /// time. This is the import profile.
    pub imports: Vec<String>,
    /// Every file the transpile emitted. jco's output set is version-dependent,
    /// so validation trusts this list rather than hard-coding a file shape.
    pub files: Vec<String>,
}

/// Directory holding a processor kind's transpiled tree.
pub fn kind_dir(surface_dist_dir: &Path, kind: &str) -> PathBuf {
    surface_dist_dir.join("processor").join(kind)
}

/// The source component artifact copied beside the transpiled output, so the
/// staleness check verifies provenance against actual bytes rather than trusting
/// a hash written next to them.
fn component_artifact(kind: &str) -> String {
    format!("{kind}.component.wasm")
}

/// Validate one processor kind's deployed tree, returning its manifest so the
/// caller can run the per-surface grant checks against the import profile.
///
/// # Panics
///
/// On a missing/unparseable manifest, a wrong schema version, a kind/directory
/// mismatch, a missing listed file, a source-hash mismatch (stale transpile or
/// partial deploy), a backend-only import, or an import name no WIT interface
/// defines.
pub fn validate_processor_kind(surface_dist_dir: &Path, kind: &str) -> ProcessorManifest {
    let dir = kind_dir(surface_dist_dir, kind);
    let manifest_path = dir.join("manifest.json");

    let raw = std::fs::read_to_string(&manifest_path).unwrap_or_else(|err| {
        panic!(
            "boot: processor component {kind:?} has no readable asset manifest at {} ({err}) — \
             the transpiled tree is not built/deployed (run `make surface-wasm`; on deploy ensure \
             surface_dist_dir is populated). Refusing to start (fail-fast on invalid config).",
            manifest_path.display(),
        )
    });
    let manifest: ProcessorManifest = serde_json::from_str(&raw).unwrap_or_else(|err| {
        panic!(
            "boot: processor component {kind:?} asset manifest at {} does not parse ({err}) — the \
             build wrote a manifest this server does not understand. Rebuild the surface assets \
             with a matching toolchain (`make surface-wasm`). Refusing to start (fail-fast on \
             invalid config).",
            manifest_path.display(),
        )
    });

    assert!(
        manifest.v == MANIFEST_VERSION,
        "boot: processor component {kind:?} asset manifest declares v = {}, but this server reads \
         v = {MANIFEST_VERSION} — the deployed surface assets were built by a different version. \
         Rebuild and redeploy. Refusing to start (fail-fast on invalid config).",
        manifest.v,
    );
    assert!(
        manifest.kind == kind,
        "boot: processor asset tree at {} carries a manifest for kind {:?} — the tree and its \
         manifest disagree about which component this is, which means a partial or crossed \
         deploy. Rebuild and redeploy. Refusing to start (fail-fast on invalid config).",
        dir.display(),
        manifest.kind,
    );

    for file in &manifest.files {
        let path = dir.join(file);
        assert!(
            path.exists(),
            "boot: processor component {kind:?} asset manifest lists {file:?}, which is missing at \
             {} — the transpiled tree is incomplete (run `make surface-wasm`; on deploy ensure \
             surface_dist_dir is populated). Refusing to start (fail-fast on invalid config).",
            path.display(),
        );
    }

    assert_source_hash_matches(&dir, kind, &manifest);
    assert_import_profile(kind, &manifest);

    manifest
}

/// The stale-transpile check: the manifest's `source_sha256` was computed from
/// the transpile's *input*, so a component rebuilt without re-transpiling — or a
/// partially synced deploy — produces a mismatch here rather than a page-load
/// surprise.
fn assert_source_hash_matches(dir: &Path, kind: &str, manifest: &ProcessorManifest) {
    let artifact = component_artifact(kind);
    let path = dir.join(&artifact);
    let bytes = std::fs::read(&path).unwrap_or_else(|err| {
        panic!(
            "boot: processor component {kind:?} source artifact {artifact} is unreadable at {} \
             ({err}) — without it the transpiled tree's provenance cannot be verified. Rebuild the \
             surface assets (`make surface-wasm`) and redeploy. Refusing to start (fail-fast on \
             invalid config).",
            path.display(),
        )
    });
    let actual = hex::encode(Sha256::digest(&bytes));
    assert!(
        actual == manifest.source_sha256,
        "boot: processor component {kind:?} has a stale transpile: {artifact} hashes to {actual}, \
         but its manifest was written from {} — the component was rebuilt without re-transpiling, \
         or the deploy synced only part of the tree. Re-run `make surface-wasm` and redeploy the \
         whole surface_dist_dir. Refusing to start (fail-fast on invalid config).",
        manifest.source_sha256,
    );
}

/// The interface name of a fully qualified processor import (`brenn:processor/log`
/// → `log`), panicking if the import is malformed or names a foreign package.
///
/// The manifest reports imports fully qualified (package namespace included) so
/// this gate can reject a foreign-namespace import outright. A dependency
/// compiled without the right adapter can drag in a `wasi:*` import that no
/// surface host implements; caught here it is a named boot panic, not a page-load
/// `instantiate` failure.
///
/// # Panics
///
/// On an import with no `<pkg>/<iface>` shape, or one outside [`PROCESSOR_PACKAGE`].
fn processor_import_interface<'a>(kind: &str, import: &'a str) -> &'a str {
    let (package, interface) = import.rsplit_once('/').unwrap_or_else(|| {
        panic!(
            "boot: processor component {kind:?} asset manifest lists import {import:?}, which is \
             not a `<package>/<interface>` name — the build wrote a manifest this server cannot \
             read. Rebuild the surface assets with a matching toolchain. Refusing to start \
             (fail-fast on invalid config)."
        )
    });
    assert!(
        package == PROCESSOR_PACKAGE,
        "boot: processor component {kind:?} imports {import:?}, from package {package:?} — no \
         surface host provides anything outside {PROCESSOR_PACKAGE:?} (a stray dependency import). \
         A surface-hosted processor's imports must all live under {PROCESSOR_PACKAGE:?}; drop the \
         import or move it backend-side. Refusing to start (fail-fast on invalid config).",
    );
    interface
}

/// The import-profile check, mirroring wasmtime's ungranted-import load panic:
/// a backend-only import on a surface-declared kind is rejected at boot, never
/// discovered at page runtime.
fn assert_import_profile(kind: &str, manifest: &ProcessorManifest) {
    for import in &manifest.imports {
        let interface = processor_import_interface(kind, import);
        assert!(
            KNOWN_IMPORTS.contains(&interface),
            "boot: processor component {kind:?} asset manifest lists import {import:?}, which \
             names no interface the processor world defines. Known: {}. This is manifest or \
             toolchain drift, not operator error — rebuild the surface assets with a matching \
             toolchain. Refusing to start (fail-fast on invalid config).",
            KNOWN_IMPORTS.join(", "),
        );
        assert!(
            SURFACE_IMPORTS.contains(&interface),
            "boot: processor component {kind:?} imports {import:?}, which no surface can satisfy — \
             it is backend-only in v1. A surface-hosted processor's imports must be a subset of \
             {}. The same artifact runs fine under [[wasm_consumer]]; declare it there, or drop \
             the import. Refusing to start (fail-fast on invalid config).",
            SURFACE_IMPORTS.join(", "),
        );
    }
}

/// The per-declaring-surface half of the profile check: importing `alert` means
/// the component reaches the alert plane, which is the surface's grant to give.
///
/// # Panics
///
/// When a surface declares an `alert`-importing kind without holding the alert
/// grant.
pub fn assert_alert_grant(slug: &str, kind: &str, manifest: &ProcessorManifest, granted: bool) {
    if !manifest
        .imports
        .iter()
        .any(|i| processor_import_interface(kind, i) == "alert")
    {
        return;
    }
    assert!(
        granted,
        "boot: [[surface]] {slug:?} declares processor component {kind:?}, which imports the alert \
         interface, but the surface holds no \"alert\" grant — the alert plane is the surface's to \
         grant, and a component cannot reach it otherwise. Add \"alert\" to the surface's grants, \
         or declare this component on a surface that has it. Refusing to start (fail-fast on \
         invalid config).",
    );
}

/// Assert no kind is declared under two different ABIs anywhere in the config.
///
/// A kind names one artifact; two ABIs for one kind means two different build
/// outputs claiming one name, and whichever the loader picked would be a coin
/// flip. Swept across all surfaces at once, so a cross-surface collision is
/// caught too.
///
/// # Panics
///
/// When one kind appears under more than one ABI.
pub fn assert_kind_abi_unique(
    declarations: impl IntoIterator<Item = (String, brenn_surface_proto::Abi)>,
) {
    let mut seen: std::collections::BTreeMap<String, BTreeSet<&'static str>> =
        std::collections::BTreeMap::new();
    for (kind, abi) in declarations {
        seen.entry(kind).or_default().insert(abi.as_str());
    }
    for (kind, abis) in seen {
        assert!(
            abis.len() == 1,
            "config: component kind {kind:?} is declared under {} different ABIs ({}) — a kind \
             names one build artifact, so two ABIs for one kind means two artifacts claiming one \
             name. Give them distinct kinds. Refusing to start (fail-fast on invalid config).",
            abis.len(),
            abis.into_iter().collect::<Vec<_>>().join(", "),
        );
    }
}

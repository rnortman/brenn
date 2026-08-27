//! Boot validation for `abi = "dom"` surface assets.
//!
//! A dom kind ships flat in the surface asset root: the wasm-bindgen
//! `--target web` module pair (`brenn_<kind>.js` + `brenn_<kind>_bg.wasm`), a
//! verbatim copy of the specification the component was authored against
//! (`brenn_<kind>.spec.brenn`), and the record binding the three together
//! (`brenn_<kind>.manifest.json`).
//!
//! The record is the dom analog of a processor kind's `manifest.json`. It sits
//! beside the pair rather than inside a per-kind directory because that is
//! where a wasm-bindgen bundle ships; every file it names derives from the same
//! artifact stem, so a record found under one kind's name and describing
//! another is a crossed build rather than something to interpret.
//!
//! Not covered, deliberately: the shared `snippets/` tree, which wasm-bindgen
//! attributes to the crate emitting the inline JS rather than to the kind
//! linking it, so no per-kind record can state it truthfully. The intra-release
//! half of that gap is closed at build time (the stage merge refuses two stages
//! writing one path with different bytes) and the cross-release half by the
//! deploy installing the tree as a whole; a residual remains and is accepted.
//! The `.d.ts` and documentation sidecars are uncovered too — nothing loads
//! them.
//!
//! Every check here is a named boot panic, on the same footing as the processor
//! arm: a record that does not match the tree is a deploy/packaging mistake,
//! config-shaped and never attacker-reachable.

use std::path::{Path, PathBuf};

use brenn_surface_contract::{
    dom_record_artifact, dom_spec_artifact, module_artifact, module_wasm_artifact,
};

/// Record schema version this server understands. A tree written by a different
/// version is a deploy/toolchain mismatch, not something to best-effort parse.
const DOM_MANIFEST_VERSION: u32 = 1;

/// The build record emitted beside a dom kind's module pair.
///
/// `deny_unknown_fields` is deliberate: an unrecognized key means the build
/// wrote a record this server does not understand, and silently ignoring it
/// would let a newer build's semantics pass validation under older rules.
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomManifest {
    /// Record schema version; must equal [`DOM_MANIFEST_VERSION`].
    pub v: u32,
    /// The kind this record backs. Must match the kind it was looked up by.
    pub kind: String,
    /// The served ES-module loader, by name in the asset root. Stated as well as
    /// derivable, and checked against the derived name: a field nothing checks
    /// is a guarantee the record does not actually carry.
    pub module: String,
    /// SHA-256 of that loader, hex-encoded.
    pub module_sha256: String,
    /// The loader's `_bg.wasm` sibling, by name in the asset root.
    pub module_wasm: String,
    /// SHA-256 of that wasm, hex-encoded.
    pub module_wasm_sha256: String,
    /// The packaged copy of the component's authored specification, by name in
    /// the asset root.
    pub spec: String,
    /// SHA-256 of that specification, hex-encoded. The configuration naming this
    /// kind compiled against a copy of the same file, so byte equality between
    /// the two carries every compile-time check over to the installed artifact.
    pub spec_sha256: String,
}

/// Path of a dom kind's binding record in the asset root. Every name this
/// module derives comes from the contract's dom file grammar, never from a
/// literal spelled here.
pub fn record_path(surface_dist_dir: &Path, kind: &str) -> PathBuf {
    surface_dist_dir.join(dom_record_artifact(kind))
}

/// Validate one dom kind's deployed assets, returning its record so the caller
/// can bind each configured instance's own specification hash to it.
///
/// Every file the record names is read and hashed, not merely stated.
///
/// The window between this check and any later serve of the tree is not
/// defended: this is anti-drift, not anti-attacker — a writer to the surface
/// asset directory already owns the host. The backend record's statement of the
/// same stance is in `wasm_package.rs`.
///
/// # Panics
///
/// On a missing/unparseable record, a wrong schema version, a kind mismatch, a
/// stated file name that is not the one the kind derives, a missing named file,
/// or any of the three hash mismatches.
pub fn validate_dom_kind(surface_dist_dir: &Path, kind: &str) -> DomManifest {
    let path = record_path(surface_dist_dir, kind);

    let raw = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "boot: dom component {kind:?} has no readable asset record at {} ({err}) — the surface \
             assets are not built/deployed, or were built before component records existed (run \
             `make build`; on deploy ensure surface_dist_dir holds this release's tree). \
             Refusing to start (fail-fast on invalid config).",
            path.display(),
        )
    });
    let manifest: DomManifest = serde_json::from_str(&raw).unwrap_or_else(|err| {
        panic!(
            "boot: dom component {kind:?} asset record at {} does not parse ({err}) — the build \
             wrote a record this server does not understand. Rebuild the surface assets with a \
             matching toolchain. Refusing to start (fail-fast on invalid config).",
            path.display(),
        )
    });

    assert!(
        manifest.v == DOM_MANIFEST_VERSION,
        "boot: dom component {kind:?} asset record declares v = {}, but this server reads v = \
         {DOM_MANIFEST_VERSION} — the deployed surface assets were built by a different version. \
         Rebuild and redeploy. Refusing to start (fail-fast on invalid config).",
        manifest.v,
    );
    assert!(
        manifest.kind == kind,
        "boot: asset record at {} carries a record for kind {:?} — the record and the name it was \
         found under disagree about which component this is, which means a partial or crossed \
         deploy. Rebuild and redeploy. Refusing to start (fail-fast on invalid config).",
        path.display(),
        manifest.kind,
    );

    let checks = [
        (
            "module",
            module_artifact(kind),
            &manifest.module,
            &manifest.module_sha256,
        ),
        (
            "module_wasm",
            module_wasm_artifact(kind),
            &manifest.module_wasm,
            &manifest.module_wasm_sha256,
        ),
        (
            "spec",
            dom_spec_artifact(kind),
            &manifest.spec,
            &manifest.spec_sha256,
        ),
    ];
    for (field, derived, stated, recorded) in checks {
        assert_file_matches(surface_dist_dir, kind, field, &derived, stated, recorded);
    }

    manifest
}

/// One record field: the stated name is the one the kind derives, the named file
/// exists, and its bytes hash to what the record says.
///
/// The name is checked before the bytes: the record states the file it hashed
/// and the kind derives that name, so a record naming something else is drift in
/// the emitter rather than a stale deploy, and says so.
fn assert_file_matches(
    surface_dist_dir: &Path,
    kind: &str,
    field: &str,
    derived: &str,
    stated: &str,
    recorded: &str,
) {
    assert!(
        stated == derived,
        "boot: dom component {kind:?} asset record names its {field} {stated:?}, but this kind's \
         files are named {derived:?} — the record and the tree's naming disagree, which is build \
         drift. Rebuild the surface assets with a matching toolchain. Refusing to start (fail-fast \
         on invalid config).",
    );
    let path = surface_dist_dir.join(derived);
    let bytes = std::fs::read(&path).unwrap_or_else(|err| {
        panic!(
            "boot: dom component {kind:?} surface asset {derived} is unreadable at {} ({err}) — the \
             asset tree is incomplete (run `make build`; on deploy ensure surface_dist_dir \
             is populated). Refusing to start (fail-fast on invalid config).",
            path.display(),
        )
    });
    let actual = brenn_lib::util::sha256_hex(&bytes);
    assert!(
        actual == recorded,
        "boot: dom component {kind:?} has a stale {field}: {derived} hashes to {actual}, but its \
         record was written from {recorded} — the tree was assembled from more than one release, \
         or the file was edited in place. Rebuild the surface assets and redeploy the whole \
         surface_dist_dir. Refusing to start (fail-fast on invalid config).",
    );
}

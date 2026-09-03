//! Boot validation for `abi = "processor"` surface assets.
//!
//! A processor kind ships as a jco-transpiled tree under
//! `<surface root>/processor/<kind>/`: the transpiled JS + core wasm, the
//! source component artifact it was transpiled from, and a `manifest.json`
//! recording the source hash, the pinned jco version, the specification the
//! component was authored against and that specification's hash, the component's
//! WIT import list, and the emitted file set.
//!
//! Every check here is a named boot panic. The trust argument: the manifest is
//! operator-deployed build output, and the source-hash check binds it to the
//! shipped component bytes — a manifest whose `imports` lies about its own
//! artifact requires tampering with the deploy, which is outside this
//! validation's threat model. In-page separation is bug containment, not a
//! security boundary; the server-side gates on what a page *does* are unchanged,
//! and the browser-side backstop is structural (the kernel supplies only the
//! surface-profile imports, so a lying manifest yields an instantiation failure,
//! never a capability).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Manifest schema version this server understands. A tree written by a
/// different version is a deploy/toolchain mismatch, not something to
/// best-effort parse.
const MANIFEST_VERSION: u32 = 2;

/// The package namespace every processor host interface lives under. An import
/// outside this namespace (a stray `wasi:*` a dependency dragged in, or a future
/// `brenn:` sibling package) names something no surface host provides, so it is
/// rejected at boot rather than left to fail at browser `instantiate`. Stripping
/// to a bare interface name before this check would be worse than useless: a
/// foreign `wasi:logging/log` would masquerade as the surface `log`.
const PROCESSOR_PACKAGE: &str = "brenn:processor";

/// The WIT interfaces a surface-hosted processor may import: the transpilable
/// profile. `store`/`mqtt`/`tools` are backend-only in v1; `dom`/`page-dom` run
/// the other way and are surface-only, because only a page has a DOM.
///
/// `types` is in the set but is not a capability: it defines the shared record
/// and enum shapes the other interfaces speak, so every processor imports it and
/// no host implements it (jco resolves it structurally). It is listed here
/// because the manifest reports the world's imports truthfully, and a
/// type-carrying import must not read as an unsatisfiable one.
const SURFACE_IMPORTS: [&str; 7] = [
    "types", "ports", "log", "alert", "config", "dom", "page-dom",
];

/// Every WIT interface name `processor.wit` defines. An import outside this set
/// is manifest/toolchain drift (the build wrote a name no world declares), which
/// is a different operator problem from declaring a backend-only component on a
/// surface — and gets its own panic.
const KNOWN_IMPORTS: [&str; 10] = [
    "types", "ports", "log", "alert", "config", "store", "mqtt", "tools", "dom", "page-dom",
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
    /// The packaged copy of the component's authored specification, by name
    /// inside the kind directory. Stated as well as derivable, and checked
    /// against the derived name: a field nothing checks is a guarantee the
    /// record does not actually carry.
    pub spec: String,
    /// SHA-256 of that specification, hex-encoded. The configuration naming this
    /// kind compiled against a copy of the same file, so byte equality between
    /// the two carries every compile-time check over to the installed artifact.
    pub spec_sha256: String,
    /// The component world's import list, extracted from the artifact at build
    /// time. This is the import profile.
    pub imports: Vec<String>,
    /// Every file the transpile emitted. jco's output set is version-dependent,
    /// so validation trusts this list rather than hard-coding a file shape.
    pub files: Vec<String>,
}

/// Directory holding a processor kind's transpiled tree.
pub fn kind_dir(surface_root: &Path, kind: &str) -> PathBuf {
    surface_root
        .join(brenn_surface_contract::PROCESSOR_DIR)
        .join(kind)
}

/// The source component artifact copied beside the transpiled output, so the
/// staleness check verifies provenance against actual bytes rather than trusting
/// a hash written next to them.
fn component_artifact(kind: &str) -> String {
    format!("{kind}.component.wasm")
}

/// The packaged specification copied beside the transpiled output, named from
/// the kind exactly as the component artifact is.
fn spec_artifact(kind: &str) -> String {
    format!("{kind}.spec.brenn")
}

/// Validate one processor kind's deployed tree, returning its manifest so the
/// caller can run the per-surface grant checks against the import profile.
///
/// # Panics
///
/// On a missing/unparseable manifest, a wrong schema version, a kind/directory
/// mismatch, a missing listed file, a source-hash mismatch (stale transpile or
/// partial deploy), a specification name that is not the one the kind derives, a
/// missing or divergent packaged specification, a backend-only import, or an
/// import name no WIT interface defines.
pub fn validate_processor_kind(surface_root: &Path, kind: &str) -> ProcessorManifest {
    let dir = kind_dir(surface_root, kind);
    let manifest_path = dir.join("manifest.json");

    let raw = std::fs::read_to_string(&manifest_path).unwrap_or_else(|err| {
        panic!(
            "boot: processor component {kind:?} has no readable asset manifest at {} ({err}) — \
             the transpiled tree is not built/deployed (build the surface assets; on deploy \
             ensure the surface install ran). Refusing to start (fail-fast on invalid \
             config).",
            manifest_path.display(),
        )
    });
    let manifest: ProcessorManifest = serde_json::from_str(&raw).unwrap_or_else(|err| {
        panic!(
            "boot: processor component {kind:?} asset manifest at {} does not parse ({err}) — the \
             build wrote a manifest this server does not understand. Rebuild the surface assets \
             with a matching toolchain. Refusing to start (fail-fast on invalid config).",
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
             {} — the transpiled tree is incomplete (run `make build`; on deploy ensure \
             the surface install ran). Refusing to start (fail-fast on invalid config).",
            path.display(),
        );
    }

    assert_source_hash_matches(&dir, kind, &manifest);
    assert_spec_hash_matches(&dir, kind, &manifest);
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
             surface assets (`make build`) and redeploy. Refusing to start (fail-fast on \
             invalid config).",
            path.display(),
        )
    });
    let actual = brenn_lib::util::sha256_hex(&bytes);
    assert!(
        actual == manifest.source_sha256,
        "boot: processor component {kind:?} has a stale transpile: {artifact} hashes to {actual}, \
         but its manifest was written from {} — the component was rebuilt without re-transpiling, \
         or the deploy synced only part of the tree. Re-run `make build` and redeploy the \
         whole surface root. Refusing to start (fail-fast on invalid config).",
        manifest.source_sha256,
    );
}

/// The spec-binding half of the record: the packaged specification is the
/// author's file verbatim, and its hash is what a configured instance's own
/// spec hash is bound to.
///
/// The name is checked before the bytes: the manifest states the file it hashed
/// and the kind derives that name, so a record naming something else is drift in
/// the emitter rather than a stale deploy, and says so.
///
/// The window between this check and any later read of the tree is not defended:
/// this is anti-drift, not anti-attacker — a writer to the surface asset
/// directory already owns the host. The backend record's statement of the same
/// stance is in `wasm_package.rs`.
fn assert_spec_hash_matches(dir: &Path, kind: &str, manifest: &ProcessorManifest) {
    let derived = spec_artifact(kind);
    assert!(
        manifest.spec == derived,
        "boot: processor component {kind:?} asset manifest names its specification {:?}, but this \
         kind's packaged specification is {derived:?} — the record and the tree's naming disagree, \
         which is build drift. Rebuild the surface assets with a matching toolchain. Refusing to \
         start (fail-fast on invalid config).",
        manifest.spec,
    );
    let path = dir.join(&derived);
    let bytes = std::fs::read(&path).unwrap_or_else(|err| {
        panic!(
            "boot: processor component {kind:?} packaged specification {derived} is unreadable at \
             {} ({err}) — without it the configuration's specification cannot be bound to this \
             tree. Rebuild the surface assets (`make build`) and redeploy. Refusing to \
             start (fail-fast on invalid config).",
            path.display(),
        )
    });
    let actual = brenn_lib::util::sha256_hex(&bytes);
    assert!(
        actual == manifest.spec_sha256,
        "boot: processor component {kind:?} has a specification that does not match its record: \
         {derived} hashes to {actual}, but its manifest was written from {} — the tree was \
         assembled from mismatched parts, or the packaged specification was edited in place. \
         Rebuild the surface assets and redeploy the whole surface root. Refusing to start \
         (fail-fast on invalid config).",
        manifest.spec_sha256,
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

/// The per-instance half of the profile check: what a component *imports* must
/// be what it was *granted*.
///
/// The surface twin of the backend linker's deny-by-default — there, an
/// ungranted interface is simply never linked and the component fails to
/// instantiate. jco hands a transpiled processor every surface import
/// unconditionally, so the equivalent statement has to be made here, and made
/// per instance: two instances of one kind may hold different grants, because
/// the module is per kind but the instantiation and its imports are per
/// instance.
///
/// `types` carries no capability (it defines the shared shapes, no host
/// implements it), so it names no grant and is skipped — the same delta the
/// vocabulary's legality table records. Backend-only imports are refused
/// earlier, by the profile check.
///
/// This is a boot-time bound, not the enforcement point: the kernel gates each
/// privileged entry on the same grants at runtime, for every ABI. A `dom`
/// instance has no manifest to assert against and gets the runtime gate alone.
///
/// # Panics
///
/// When an instance's kind imports an interface the instance was not granted.
pub fn assert_imports_granted(
    slug: &str,
    instance: &str,
    kind: &str,
    manifest: &ProcessorManifest,
    grants: &BTreeSet<brenn_envelope::grants::ComponentGrant>,
) {
    for import in &manifest.imports {
        let interface = processor_import_interface(kind, import);
        let Some(grant) = brenn_envelope::grants::ComponentGrant::parse(interface) else {
            continue;
        };
        assert!(
            grants.contains(&grant),
            "boot: [[surface]] {slug:?}: component {instance:?} runs processor kind {kind:?}, \
             which imports the {interface} interface, but {:?} is not in the component's grants — \
             a component is given what it is granted and nothing else. Add {interface:?} to its \
             grants, or ship a build that does not import it. Refusing to start (fail-fast on \
             invalid config).",
            grant.word(),
        );
    }
}

#[cfg(test)]
mod tests {
    use brenn_envelope::grants::{ComponentGrant, ComponentHost};

    use super::{KNOWN_IMPORTS, SURFACE_IMPORTS};

    /// The interface name in an import list that is no capability: it carries
    /// the shared types every processor speaks and no host implements it.
    const TYPES: &str = "types";

    /// The grant that names no interface, so no import list can hold it.
    const NO_INTERFACE: ComponentGrant = ComponentGrant::Takeover;

    /// The interface names the world defines and only a surface host links.
    /// [`KNOWN_IMPORTS`] is "every interface `processor.wit` defines", so it
    /// carries them; a top-level component may not be granted them. That is the
    /// mirror image of `store`/`mqtt`/`tools`, which the world defines and only
    /// the backend links.
    const SURFACE_ONLY: [&str; 2] = ["dom", "page-dom"];

    /// One host's link profile, derived from the world's interfaces rather than
    /// hand-listed: every interface the world defines, minus the ones this host
    /// refuses the word for. [`KNOWN_IMPORTS`] answers "what does the world
    /// define", which is a different question from "what does this host link",
    /// and deriving the second from the first is what keeps them one statement.
    fn link_profile(host: ComponentHost) -> Vec<&'static str> {
        KNOWN_IMPORTS
            .iter()
            .copied()
            .filter(|import| {
                *import == TYPES
                    || ComponentGrant::parse(import)
                        .unwrap_or_else(|| panic!("{import} names an interface and no grant word"))
                        .illegal_on(host)
                        .is_none()
            })
            .collect()
    }

    /// Each host's import list and the words that host admits are two statements
    /// of one policy, written in two crates. They are held equal here, with
    /// every deviation named above rather than tolerated as a difference.
    fn assert_pinned(host: ComponentHost, imports: &[&str]) {
        for import in imports {
            if *import == TYPES {
                continue;
            }
            let grant = ComponentGrant::parse(import)
                .unwrap_or_else(|| panic!("{import} names an interface and no grant word"));
            assert!(
                grant.illegal_on(host).is_none(),
                "{import} is linkable on this host and its word is refused there"
            );
        }
        for grant in ComponentGrant::ALL {
            if grant.illegal_on(host).is_some() || grant == NO_INTERFACE {
                continue;
            }
            assert!(
                imports.contains(&grant.word()),
                "`{}` is granted on this host and names no interface it links",
                grant.word()
            );
        }
    }

    /// The surface-only words are the one place the two lists can silently
    /// diverge: the top-level profile derives them away, so nothing else would
    /// notice either list losing them.
    #[test]
    fn the_surface_only_words_are_in_both_import_lists() {
        for word in SURFACE_ONLY {
            assert!(
                SURFACE_IMPORTS.contains(&word),
                "`{word}` is a surface-only word and the surface links it nowhere"
            );
            assert!(
                KNOWN_IMPORTS.contains(&word),
                "`{word}` is a surface-only word and the world defines it nowhere"
            );
            let grant = ComponentGrant::parse(word).expect("a surface-only word is a grant word");
            assert!(
                grant.illegal_on(ComponentHost::TopLevel).is_some(),
                "`{word}` is a surface-only word and is legal at the top level"
            );
            assert!(
                !link_profile(ComponentHost::TopLevel).contains(&word),
                "`{word}` is refused at the top level and its profile links it"
            );
        }
    }

    #[test]
    fn the_surface_profile_matches_what_a_surface_component_may_be_granted() {
        assert_pinned(ComponentHost::Surface, &SURFACE_IMPORTS);
    }

    #[test]
    fn the_world_matches_what_a_top_level_component_may_be_granted() {
        assert_pinned(
            ComponentHost::TopLevel,
            &link_profile(ComponentHost::TopLevel),
        );
    }
}

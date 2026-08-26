//! The binding between a shipped WASM component and the specification it was
//! built against.
//!
//! A shipped backend component is three sibling files sharing the artifact's
//! stem: `<stem>.wasm`, `<stem>.spec.brenn` (the verbatim copy of the
//! specification its author wrote), and `<stem>.package.json` — the record read
//! here. The record states that those files were produced together; boot
//! re-computes both hashes and refuses to run when either disagrees.
//!
//! What that buys is not a signature. Byte equality between the deployment's
//! spec copy and the packaged one transfers every compile-time check the
//! configuration passed — spec fit, port optionality, doctypes — onto the
//! artifact actually installed, because the configuration compiled against
//! exactly those bytes. Comparing facts instead would require a second parse
//! and a decision about which facts count; the hash needs neither and is
//! strictly stricter.
//!
//! The record is an external contract. An out-of-tree component ships one in
//! this shape or it does not load, and there are no compatibility shims: `v` is
//! the whole story, an unknown field is a refusal, and a new field arrives
//! under a version bump.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::util::sha256_hex;

/// The record schema version this host reads. A record naming any other version
/// is refused rather than partially understood.
const RECORD_VERSION: u32 = 1;

/// The WIT package a processor-world component targets.
const WORLD_PROCESSOR: &str = "brenn:processor";

/// The WIT package a replay-world component targets.
const WORLD_REPLAY: &str = "brenn:replay";

/// Where to read about the contract when a component arrives without one.
const CONTRACT_DOC: &str = "docs/component-packages.md";

/// One component package's binding record, as the build emits it.
///
/// `deny_unknown_fields` is the compatibility stance made mechanical: a v2
/// record on a v1 host refuses loudly instead of half-parsing.
///
/// Private, with the reading and the verifying of it private too: the two entry
/// points below are the whole surface, so no caller can come to hold a record
/// it has not verified — which is the mistake this module exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackageRecord {
    /// Record schema version; must equal [`RECORD_VERSION`].
    v: u32,
    /// The component's name — the artifact's stem.
    name: String,
    /// The WIT package the artifact targets: [`WORLD_PROCESSOR`] or
    /// [`WORLD_REPLAY`].
    world: String,
    /// The artifact's basename, beside the record.
    artifact: String,
    /// Lowercase hex SHA-256 of the artifact's bytes.
    artifact_sha256: String,
    /// The packaged spec's basename. Present iff `world` is
    /// [`WORLD_PROCESSOR`].
    #[serde(default)]
    spec: Option<String>,
    /// Lowercase hex SHA-256 of the packaged spec's bytes. Present iff `world`
    /// is [`WORLD_PROCESSOR`].
    #[serde(default)]
    spec_sha256: Option<String>,
}

/// The record's path, derived from the artifact's: `<stem>.package.json` beside
/// it.
///
/// Derived rather than configured so every package file follows from the one
/// path the configuration states.
fn record_path(artifact_path: &Path) -> PathBuf {
    sibling(artifact_path, "package.json")
}

/// The packaged spec's path, derived the same way: `<stem>.spec.brenn`.
fn spec_path(artifact_path: &Path) -> PathBuf {
    sibling(artifact_path, "spec.brenn")
}

/// `<stem>.<extension>` beside `artifact_path`.
fn sibling(artifact_path: &Path, extension: &str) -> PathBuf {
    let mut name = std::ffi::OsString::from(stem_of(artifact_path));
    name.push(".");
    name.push(extension);
    artifact_path.with_file_name(name)
}

/// The artifact's filename stem — the component's name, and the stem every
/// package file shares.
fn stem_of(artifact_path: &Path) -> &str {
    artifact_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_else(|| {
            panic!(
                "boot: component path {} has no UTF-8 filename stem, so its package files cannot \
                 be derived. Refusing to start (fail-fast on invalid config).",
                artifact_path.display()
            )
        })
}

/// A path's filename, for comparison against the name a record states.
fn file_name_of(path: &Path) -> &str {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_else(|| {
            panic!(
                "boot: package file {} has no UTF-8 filename. Refusing to start (fail-fast on \
                 invalid config).",
                path.display()
            )
        })
}

/// Read and validate the record beside `artifact_path`.
///
/// Panics — a component whose record is missing, unreadable, or unparseable is
/// a component this host cannot bind to a specification, and better dead than
/// wrong.
fn load_record(artifact_path: &Path) -> PackageRecord {
    let path = record_path(artifact_path);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "boot: WASM component {} has no readable package record at {} ({err}) — the \
             component was installed without it. Reinstall the release package, or, for an \
             out-of-tree component, ship the record its author must emit (see {CONTRACT_DOC}). \
             Refusing to start (fail-fast on invalid config).",
            artifact_path.display(),
            path.display(),
        )
    });
    let record: PackageRecord = serde_json::from_str(&text).unwrap_or_else(|err| {
        panic!(
            "boot: package record {} is not a v{RECORD_VERSION} record ({err}). The record \
             schema is versioned and carries no compatibility shims: a record written by a \
             newer build is refused rather than partially read (see {CONTRACT_DOC}). Refusing \
             to start (fail-fast on invalid config).",
            path.display(),
        )
    });
    assert!(
        record.v == RECORD_VERSION,
        "boot: package record {} declares version {}, but this host reads v{RECORD_VERSION}. A \
         version bump is a breaking change with no shim: install the release whose binary and \
         components were built together (see {CONTRACT_DOC}). Refusing to start (fail-fast on \
         invalid config).",
        path.display(),
        record.v,
    );
    assert!(
        record.world == WORLD_PROCESSOR || record.world == WORLD_REPLAY,
        "boot: package record {} declares world {:?}, which this host does not link. The worlds \
         are {WORLD_PROCESSOR} and {WORLD_REPLAY}. Refusing to start (fail-fast on invalid \
         config).",
        path.display(),
        record.world,
    );
    let has_spec = record.spec.is_some() || record.spec_sha256.is_some();
    assert!(
        record.spec.is_some() == record.spec_sha256.is_some(),
        "boot: package record {} names a spec without its hash, or a hash without its spec. Both \
         travel together or neither does. Refusing to start (fail-fast on invalid config).",
        path.display(),
    );
    assert!(
        has_spec == (record.world == WORLD_PROCESSOR),
        "boot: package record {} declares world {:?} and {}. A \
         {WORLD_PROCESSOR} component packages the spec its author wrote; a {WORLD_REPLAY} \
         component has no component class, no ports and no grants, so it packages none. \
         Refusing to start (fail-fast on invalid config).",
        path.display(),
        record.world,
        if has_spec {
            "carries a specification"
        } else {
            "carries no specification"
        },
    );
    // The three names the record states are the three the host derives from the
    // artifact's path. Stated and derived are compared rather than one of them
    // being ignored: a record naming another component's artifact, or a spec
    // that is not the one beside it, describes a package that was assembled
    // wrong, and a field nothing checks is a guarantee the contract does not
    // actually carry.
    let stem = stem_of(artifact_path);
    assert!(
        record.name == stem,
        "boot: package record {} names component {:?}, but it sits beside {}, whose stem is \
         {stem:?}. The record belongs to another component. Reinstall the release package. \
         Refusing to start (fail-fast on invalid config).",
        path.display(),
        record.name,
        artifact_path.display(),
    );
    let artifact_name = file_name_of(artifact_path);
    assert!(
        record.artifact == artifact_name,
        "boot: package record {} names artifact {:?}, but it sits beside {artifact_name:?}. The \
         record belongs to another component. Reinstall the release package. Refusing to start \
         (fail-fast on invalid config).",
        path.display(),
        record.artifact,
    );
    if let Some(spec) = record.spec.as_deref() {
        let derived = spec_path(artifact_path);
        let derived_name = file_name_of(&derived);
        assert!(
            spec == derived_name,
            "boot: package record {} names specification {spec:?}, but the specification a \
             package carries is {derived_name:?}, beside the artifact. Reinstall the release \
             package. Refusing to start (fail-fast on invalid config).",
            path.display(),
        );
    }
    record
}

/// Re-compute what the record binds and refuse any disagreement.
///
/// The artifact's bytes are hashed and compared, and for a processor-world
/// package the packaged spec's bytes too.
///
/// There is a window between this read and the loader's own: the artifact could
/// change in between. Accepted deliberately — this binding is anti-drift, not
/// anti-attacker. Stale installs, half-synced deploys and hand-copied artifacts
/// are what it catches; a writer to the components directory between the two
/// reads already owns the host, because that directory is operator-installed
/// beside the operator's configuration.
fn verify(record: &PackageRecord, artifact_path: &Path) {
    let bytes = read_or_die(artifact_path, "component artifact");
    let actual = sha256_hex(&bytes);
    assert!(
        actual == record.artifact_sha256,
        "boot: WASM component {} hashes to {actual}, but its package record binds {}. The \
         artifact was replaced without its record, or the install carried only part of the \
         package. Reinstall the release package. Refusing to start (fail-fast on invalid \
         config).",
        artifact_path.display(),
        record.artifact_sha256,
    );
    let Some(expected) = record.spec_sha256.as_deref() else {
        return;
    };
    let path = spec_path(artifact_path);
    let spec = read_or_die(&path, "packaged component specification");
    let actual = sha256_hex(&spec);
    assert!(
        actual == expected,
        "boot: packaged specification {} hashes to {actual}, but the package record binds \
         {expected}. The spec beside the artifact is not the one it was built with. Reinstall \
         the release package. Refusing to start (fail-fast on invalid config).",
        path.display(),
    );
}

/// The full check for a top-level consumer: the package is internally bound,
/// targets the processor world, and its spec is byte-identical to the one the
/// running configuration compiled against.
///
/// `config_spec_sha256` is the class's `spec_sha256`, carried from the file the
/// class was declared in.
pub fn verify_consumer(artifact_path: &Path, slug: &str, config_spec_sha256: &str) {
    let record = load_record(artifact_path);
    assert!(
        record.world == WORLD_PROCESSOR,
        "boot: consumer {slug:?} loads {} as a processor component, but its package record \
         declares world {:?}. A replay artifact installed under a consumer's path is a \
         cross-wired deployment, not a component this host can run. Refusing to start \
         (fail-fast on invalid config).",
        artifact_path.display(),
        record.world,
    );
    verify(&record, artifact_path);
    let packaged = record
        .spec_sha256
        .as_deref()
        .expect("a processor record carries its spec hash");
    assert!(
        packaged == config_spec_sha256,
        "boot: consumer {slug:?} was configured against a specification that hashes to \
         {config_spec_sha256}, but the component at {} was built against one that hashes to \
         {packaged}. The author's specification travels with the component; a deployment's copy \
         of it is verbatim. Re-copy the specification from the release that carries this \
         component, or install the release the configuration was written for. Refusing to start \
         (fail-fast on invalid config).",
        artifact_path.display(),
    );
}

/// The full check for a replay component: internally bound and replay-world.
/// There is no spec side — a replay component declares no class.
pub fn verify_replay(artifact_path: &Path, slug: &str) {
    let record = load_record(artifact_path);
    assert!(
        record.world == WORLD_REPLAY,
        "boot: webhook endpoint {slug:?} loads {} as a replay component, but its package record \
         declares world {:?}. A processor artifact installed under a replay endpoint's path is a \
         cross-wired deployment. Refusing to start (fail-fast on invalid config).",
        artifact_path.display(),
        record.world,
    );
    verify(&record, artifact_path);
}

/// Read a package file, or die naming it and what it was.
fn read_or_die(path: &Path, what: &str) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|err| {
        panic!(
            "boot: {what} {} is unreadable ({err}) — the component package is incomplete. \
             Reinstall the release package. Refusing to start (fail-fast on invalid config).",
            path.display(),
        )
    })
}

#[cfg(test)]
mod tests;

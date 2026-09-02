//! The binding between a shipped WASM component and the specification it was
//! built against.
//!
//! A shipped backend component is a directory under the components root, named
//! by the package name — the same `<name>` a configuration writes in
//! `use @<name>::*;`. It holds `package.json` (the record read here),
//! `<name>.brenn` (the verbatim copy of the specification its author wrote, for
//! a processor-world package), and the artifact under its built basename. The
//! record states that those files were produced together; boot re-computes both
//! hashes and refuses to run when either disagrees.
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

use brenn_dsl::roots::{display_list, scan_roots};
use serde::Deserialize;

use crate::util::sha256_hex;

/// The record schema version this host reads. A record naming any other version
/// is refused rather than partially understood.
const RECORD_VERSION: u32 = 2;

/// The record's basename within a package directory. Fixed, not derived: the
/// directory is named by the package and the artifact keeps its built stem, so
/// there is no stem left for the record to share.
const RECORD_NAME: &str = "package.json";

/// The extension a packaged specification carries; the rest of its name is the
/// package's.
const SPEC_EXT: &str = ".brenn";

const ARTIFACT_EXT: &str = ".wasm";

/// The WIT package a processor-world component targets.
const WORLD_PROCESSOR: &str = "brenn:processor";

/// The WIT package a replay-world component targets.
const WORLD_REPLAY: &str = "brenn:replay";

/// Where to read about the contract when a component arrives without one.
const CONTRACT_DOC: &str = "docs/component-packages.md";

/// The flag that names the components root, for the messages that have to tell
/// an operator which one is missing.
const COMPONENTS_FLAG: &str = "--components";

/// One component package's binding record, as the build emits it.
///
/// `deny_unknown_fields` is the compatibility stance made mechanical: a v3
/// record on a v2 host refuses loudly instead of half-parsing.
///
/// Private, with the reading and the verifying of it private too: the two entry
/// points below are the whole surface, so no caller can come to hold a record
/// it has not verified — which is the mistake this module exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackageRecord {
    /// Record schema version; must equal [`RECORD_VERSION`].
    v: u32,
    /// The package's name — the directory's basename, and the module name a
    /// configuration imports the class from.
    name: String,
    /// The WIT package the artifact targets: [`WORLD_PROCESSOR`] or
    /// [`WORLD_REPLAY`].
    world: String,
    /// The artifact's basename within the package directory.
    artifact: String,
    /// Lowercase hex SHA-256 of the artifact's bytes.
    artifact_sha256: String,
    /// The packaged spec's basename, always `<name>.brenn`. Present iff `world`
    /// is [`WORLD_PROCESSOR`].
    #[serde(default)]
    spec: Option<String>,
    /// Lowercase hex SHA-256 of the packaged spec's bytes. Present iff `world`
    /// is [`WORLD_PROCESSOR`].
    #[serde(default)]
    spec_sha256: Option<String>,
}

/// The record's path within a package directory.
fn record_path(package_dir: &Path) -> PathBuf {
    package_dir.join(RECORD_NAME)
}

/// The packaged spec's path: `<name>.brenn` within the package directory.
fn spec_path(package_dir: &Path, name: &str) -> PathBuf {
    package_dir.join(format!("{name}{SPEC_EXT}"))
}

/// A package directory's basename, which is the package name the record must
/// state.
fn dir_name_of(package_dir: &Path) -> &str {
    package_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_else(|| {
            panic!(
                "boot: component package directory {} has no UTF-8 basename, so the package it \
                 holds cannot be named. Refusing to start (fail-fast on invalid config).",
                package_dir.display()
            )
        })
}

/// Read and validate the record in `package_dir`.
///
/// Panics — a component whose record is missing, unreadable, or unparseable is
/// a component this host cannot bind to a specification, and better dead than
/// wrong.
fn load_record(package_dir: &Path) -> PackageRecord {
    let path = record_path(package_dir);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "boot: component package {} has no readable record at {} ({err}) — the package was \
             installed without it, or the directory is not a package at all. Reinstall the \
             release package, or, for an out-of-tree component, ship the record its author must \
             emit (see {CONTRACT_DOC}). Refusing to start (fail-fast on invalid config).",
            package_dir.display(),
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
         version bump is a breaking change with no shim — a release older or newer than this \
         host is the usual cause. Install the release whose binary and components were built \
         together (see {CONTRACT_DOC}). Refusing to start (fail-fast on invalid config).",
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
    // The names the record states are the names the layout fixes. Stated and
    // derived are compared rather than one of them being ignored: a record
    // naming another package, or a spec that is not the one beside it,
    // describes a package that was assembled wrong, and a field nothing checks
    // is a guarantee the contract does not actually carry.
    let dir_name = dir_name_of(package_dir);
    assert!(
        record.name == dir_name,
        "boot: package record {} names package {:?}, but it sits in a directory named \
         {dir_name:?}. The package name is the directory's basename and the module name a \
         configuration imports; a record that disagrees belongs to another package. Reinstall \
         the release package. Refusing to start (fail-fast on invalid config).",
        path.display(),
        record.name,
    );
    assert!(
        !record.artifact.contains(std::path::MAIN_SEPARATOR) && !record.artifact.contains('/'),
        "boot: package record {} names artifact {:?}, which contains a path separator. An \
         artifact is a file in the package directory, not a path out of it. Refusing to start \
         (fail-fast on invalid config).",
        path.display(),
        record.artifact,
    );
    assert!(
        record.artifact.ends_with(ARTIFACT_EXT) && record.artifact.len() > ARTIFACT_EXT.len(),
        "boot: package record {} names artifact {:?}, which is not a {ARTIFACT_EXT} file. \
         Refusing to start (fail-fast on invalid config).",
        path.display(),
        record.artifact,
    );
    if let Some(spec) = record.spec.as_deref() {
        let expected = format!("{}{SPEC_EXT}", record.name);
        assert!(
            spec == expected,
            "boot: package record {} names specification {spec:?}, but the specification a \
             package carries is {expected:?} — the package's own name. Reinstall the release \
             package. Refusing to start (fail-fast on invalid config).",
            path.display(),
        );
    }
    record
}

/// Re-compute what the record binds and refuse any disagreement.
///
/// The artifact's bytes are hashed and compared, and for a processor-world
/// package the packaged spec's bytes too. Returns the artifact's path, which is
/// what the loader is handed.
///
/// There is a window between this read and the loader's own: the artifact could
/// change in between. Accepted deliberately — this binding is anti-drift, not
/// anti-attacker. Stale installs, half-synced deploys and hand-copied artifacts
/// are what it catches; a writer to the components directory between the two
/// reads already owns the host, because that directory is operator-installed
/// beside the operator's configuration.
fn verify(record: &PackageRecord, package_dir: &Path) -> PathBuf {
    let artifact_path = package_dir.join(&record.artifact);
    let bytes = read_or_die(&artifact_path, "component artifact");
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
        return artifact_path;
    };
    let path = spec_path(package_dir, &record.name);
    let spec = read_or_die(&path, "packaged component specification");
    let actual = sha256_hex(&spec);
    assert!(
        actual == expected,
        "boot: packaged specification {} hashes to {actual}, but the package record binds \
         {expected}. The spec beside the artifact is not the one it was built with. Reinstall \
         the release package. Refusing to start (fail-fast on invalid config).",
        path.display(),
    );
    artifact_path
}

/// Resolve `<root>/<package>` across the components roots and refuse unless
/// exactly one root holds it.
///
/// `what` names the thing the configuration wired, so the message says which
/// instantiation went looking. Zero hits is a package no installed release
/// ships; two is two releases shipping one name, which is not a deployment
/// anyone meant — a single root would let the second silently shadow the first,
/// and the whole point of a root per release is that the host can see both.
fn resolve_dir(components_roots: &[PathBuf], package: &str, what: &str) -> PathBuf {
    assert_package_name(package, what);
    require_components_root(components_roots, what);
    let mut hits: Vec<PathBuf> = components_roots
        .iter()
        .map(|root| root.join(package))
        .filter(|dir| dir.is_dir())
        .collect();
    assert!(
        hits.len() < 2,
        "boot: {what} names component package {package:?}, which is installed under more than \
         one {COMPONENTS_FLAG} root: {}. A package ships with exactly one release; two copies \
         mean a stale install or two bundles claiming one name. Remove or rename one. Refusing \
         to start (fail-fast on invalid config).",
        display_list(&hits),
    );
    let Some(dir) = hits.pop() else {
        panic!(
            "boot: {what} names component package {package:?}, but {package} is not an installed \
             package directory under any {COMPONENTS_FLAG} root (searched: {}). A configuration \
             may import any module the module roots ship, but only a component the release ships \
             as a backend package can be instantiated at top level — a surface kind ships its \
             module and no package. Check the name against the installed releases' components, \
             or the {COMPONENTS_FLAG} roots this host was started with. Refusing to start \
             (fail-fast on invalid config).",
            display_list(components_roots),
        )
    };
    dir
}

/// Refuse a package name that is not a single plain directory name or that
/// begins with `.`.
///
/// A name is joined onto the components root, and `join` treats an absolute
/// name as a whole new root and `..` as a step out of one. A configuration
/// names a component and never a location, so a name that could reach outside
/// the root is refused before it resolves to anything — the same refusal the
/// record's own `artifact` field earns for carrying a separator.
///
/// A leading dot is refused separately: nothing a release installs is named
/// that way, and a dot-named directory hides from the shell globs an install
/// sweeps a directory with, so it is the shape a package that outlives its
/// release would take.
fn assert_package_name(package: &str, what: &str) {
    let mut components = Path::new(package).components();
    let sole_normal = matches!(
        (components.next(), components.next()),
        (Some(std::path::Component::Normal(first)), None) if first == std::ffi::OsStr::new(package)
    );
    assert!(
        sole_normal,
        "boot: {what} names component package {package:?}, which is not a package name. A \
         package name is one directory name under the {COMPONENTS_FLAG} root — empty names, \
         path separators, `.` and `..` all name a location instead, and where a component is \
         installed is not a fact a configuration states. Refusing to start (fail-fast on \
         invalid config).",
    );
    assert!(
        !package.starts_with('.'),
        "boot: {what} names component package {package:?}, which is not a package name. A \
         package name does not begin with `.`: no release installs a dot-named package, and a \
         dot-named directory is skipped by the globs an install sweeps a components root with, \
         so such a name resolves to something no release put there. Refusing to start \
         (fail-fast on invalid config).",
    );
}

/// The full check for a top-level consumer: the package is installed and
/// internally bound, targets the processor world, and its spec is
/// byte-identical to the one the running configuration compiled against.
///
/// `config_spec_sha256` is the class's `spec_sha256`, carried from the file the
/// class was declared in. Returns the artifact path for the loader.
pub fn verify_consumer(
    components_roots: &[PathBuf],
    package: &str,
    slug: &str,
    config_spec_sha256: &str,
) -> PathBuf {
    let dir = resolve_dir(components_roots, package, &format!("consumer {slug:?}"));
    let record = load_record(&dir);
    assert!(
        record.world == WORLD_PROCESSOR,
        "boot: consumer {slug:?} loads package {package:?} as a processor component, but its \
         record declares world {:?}. A replay package instantiated as a consumer is a \
         cross-wired deployment, not a component this host can run. Refusing to start \
         (fail-fast on invalid config).",
        record.world,
    );
    let artifact_path = verify(&record, &dir);
    let packaged = record
        .spec_sha256
        .as_deref()
        .expect("a processor record carries its spec hash");
    assert!(
        packaged == config_spec_sha256,
        "boot: consumer {slug:?} was configured against a specification that hashes to \
         {config_spec_sha256}, but the component installed at {} was built against one that \
         hashes to {packaged}. The module root this host compiled the configuration against and \
         the release installed beside it disagree. Install the release the configuration was \
         written for, or point the module root at that release's modules. Refusing to start \
         (fail-fast on invalid config).",
        dir.display(),
    );
    artifact_path
}

/// The full check for a replay component: installed, internally bound and
/// replay-world. There is no spec side — a replay component declares no class.
pub fn verify_replay(components_roots: &[PathBuf], package: &str, slug: &str) -> PathBuf {
    let dir = resolve_dir(
        components_roots,
        package,
        &format!("webhook endpoint {slug:?} replay protection"),
    );
    let record = load_record(&dir);
    assert!(
        record.world == WORLD_REPLAY,
        "boot: webhook endpoint {slug:?} loads package {package:?} as a replay component, but \
         its record declares world {:?}. A processor package wired to a replay endpoint is a \
         cross-wired deployment. Refusing to start (fail-fast on invalid config).",
        record.world,
    );
    verify(&record, &dir)
}

/// The components roots a boot-time load resolves against, or a refusal naming
/// the flag that was not passed.
///
/// `what` names the instantiation that wanted one, so an operator learns both
/// that the flag is missing and which part of the configuration needs it.
pub fn require_components_root<'a>(components_roots: &'a [PathBuf], what: &str) -> &'a [PathBuf] {
    assert!(
        !components_roots.is_empty(),
        "boot: {what} loads an installed component package, but this host was started without \
         {COMPONENTS_FLAG} <DIR>. Where components are installed is an environment fact the \
         configuration never states, so it has to be named on the command line, once per \
         installed release. Refusing to start (fail-fast on invalid config).",
    );
    components_roots
}

/// Refuse a components-root list in which one package name is installed under
/// two roots, whatever the configuration goes on to instantiate. Every fault
/// the scan finds is in the one refusal, so a broken install is fixed in one
/// pass rather than one boot per fault.
///
/// A root per release is what lets the host see a collision at all; refusing it
/// at startup rather than when a consumer happens to resolve the name is the
/// same posture as the module roots' cross-root scan. Roots are compared after
/// canonicalization, so the same directory named twice is refused as one
/// directory rather than as a duplicate of everything in it. Directory names
/// only — a plain file in a root is the installer's refusal, not this one.
pub fn assert_disjoint_components_roots(components_roots: &[PathBuf]) {
    let is_package = |entry: &std::fs::DirEntry| {
        if !entry.path().is_dir() {
            return None;
        }
        entry.file_name().to_str().map(str::to_string)
    };
    let faults = scan_roots(COMPONENTS_FLAG, components_roots, is_package);
    assert!(
        faults.is_empty(),
        "boot: the {COMPONENTS_FLAG} roots are not a set of distinct releases:\n{}\nRefusing to \
         start (fail-fast on invalid config).",
        faults
            .iter()
            .map(|fault| fault.describe(COMPONENTS_FLAG, "component package"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// Refuse a components root that is not a directory, whatever the configuration
/// goes on to load.
///
/// Parity with the module root: a flag pointed at nothing is an operator error
/// worth reporting at startup, not one that hides until some later release
/// happens to configure a consumer.
pub fn assert_components_root(components_root: &Path) {
    assert!(
        components_root.is_dir(),
        "boot: {COMPONENTS_FLAG} names {}, which is not a directory. The components root holds \
         one directory per installed component package. Refusing to start (fail-fast on invalid \
         config).",
        components_root.display(),
    );
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

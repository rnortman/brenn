//! The binding check, over real files in a temp directory.
//!
//! Verification reads bytes and compares digests; it never instantiates, so the
//! "artifacts" here are ordinary files. What is under test is the record
//! reader, the two hash comparisons and the config↔package binding — not the
//! WASM loader behind them, which has its own suites.

use std::path::{Path, PathBuf};

use super::*;

const NAME: &str = "probe";

/// The artifact's built basename, deliberately unrelated to the package name:
/// the layout keeps the built stem, and nothing may derive one from the other.
const ARTIFACT: &str = "brenn_probe_component.wasm";

/// A components root holding one package directory: the artifact bytes, the
/// spec text, and a record naming both correctly.
struct Root {
    dir: tempfile::TempDir,
}

impl Root {
    /// A processor-world package whose record binds exactly what is on disk.
    fn processor(artifact: &[u8], spec: &str) -> Root {
        let root = Root::bare(artifact);
        std::fs::write(spec_path(&root.package(), NAME), spec).unwrap();
        root.write_record(&format!(
            "{{\n  \"v\": 2,\n  \"name\": \"{NAME}\",\n  \"world\": \"brenn:processor\",\n  \
             \"artifact\": \"{ARTIFACT}\",\n  \"artifact_sha256\": \"{}\",\n  \
             \"spec\": \"{NAME}.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
            sha256_hex(artifact),
            sha256_hex(spec.as_bytes()),
        ));
        root
    }

    /// A replay-world package: artifact and record, no spec.
    fn replay(artifact: &[u8]) -> Root {
        let root = Root::bare(artifact);
        root.write_record(&format!(
            "{{\n  \"v\": 2,\n  \"name\": \"{NAME}\",\n  \"world\": \"brenn:replay\",\n  \
             \"artifact\": \"{ARTIFACT}\",\n  \"artifact_sha256\": \"{}\"\n}}\n",
            sha256_hex(artifact),
        ));
        root
    }

    /// The package directory with its artifact alone — no record yet.
    fn bare(artifact: &[u8]) -> Root {
        let dir = tempfile::tempdir().unwrap();
        let package = dir.path().join(NAME);
        std::fs::create_dir(&package).unwrap();
        std::fs::write(package.join(ARTIFACT), artifact).unwrap();
        Root { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    /// This root as the one-entry list a host started with one `--components`
    /// resolves against.
    fn roots(&self) -> Vec<PathBuf> {
        vec![self.dir.path().to_path_buf()]
    }

    fn package(&self) -> PathBuf {
        self.dir.path().join(NAME)
    }

    fn write_record(&self, text: &str) {
        std::fs::write(record_path(&self.package()), text).unwrap();
    }

    /// Overwrite the artifact after the record was written, which is the
    /// stale-install shape.
    fn retouch_artifact(&self, bytes: &[u8]) {
        std::fs::write(self.package().join(ARTIFACT), bytes).unwrap();
    }
}

/// The spec text the processor packages here are built with.
const SPEC: &str = "component Sink { abi = processor; requires = []; in messages; }\n";

// ── the package paths ────────────────────────────────────────────────────────

#[test]
fn package_files_are_derived_from_the_package_directory() {
    let dir = Path::new("/srv/components/processor-demo");
    assert_eq!(
        record_path(dir),
        PathBuf::from("/srv/components/processor-demo/package.json")
    );
    assert_eq!(
        spec_path(dir, "processor-demo"),
        PathBuf::from("/srv/components/processor-demo/processor-demo.brenn")
    );
}

// ── reading a record ─────────────────────────────────────────────────────────

#[test]
fn a_processor_record_reads_back_what_the_build_wrote() {
    let root = Root::processor(b"artifact bytes", SPEC);
    let record = load_record(&root.package());
    assert_eq!(record.v, RECORD_VERSION);
    assert_eq!(record.name, NAME);
    assert_eq!(record.world, WORLD_PROCESSOR);
    assert_eq!(record.artifact, ARTIFACT);
    assert_eq!(record.artifact_sha256, sha256_hex(b"artifact bytes"));
    assert_eq!(record.spec.as_deref(), Some("probe.brenn"));
    assert_eq!(
        record.spec_sha256.as_deref(),
        Some(sha256_hex(SPEC.as_bytes())).as_deref()
    );
    assert_eq!(
        verify(&record, &root.package()),
        root.package().join(ARTIFACT)
    );
}

#[test]
fn a_replay_record_reads_back_and_verifies_without_a_spec() {
    let root = Root::replay(b"replay bytes");
    let record = load_record(&root.package());
    assert_eq!(record.world, WORLD_REPLAY);
    assert_eq!(record.spec, None);
    assert_eq!(record.spec_sha256, None);
    verify(&record, &root.package());
}

#[test]
#[should_panic(expected = "has no readable record")]
fn a_component_installed_without_its_record_is_refused() {
    let root = Root::bare(b"artifact bytes");
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "is not a v2 record")]
fn an_unknown_field_is_refused_rather_than_ignored() {
    let root = Root::replay(b"replay bytes");
    root.write_record(
        "{\n  \"v\": 2,\n  \"name\": \"probe\",\n  \"world\": \"brenn:replay\",\n  \
         \"artifact\": \"brenn_probe_component.wasm\",\n  \"artifact_sha256\": \"00\",\n  \
         \"signature\": \"later\"\n}\n",
    );
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "declares version 1")]
fn a_v1_record_from_a_pre_names_release_is_refused() {
    let root = Root::replay(b"replay bytes");
    root.write_record(
        "{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:replay\",\n  \
         \"artifact\": \"brenn_probe_component.wasm\",\n  \"artifact_sha256\": \"00\"\n}\n",
    );
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "which this host does not link")]
fn a_record_naming_a_world_this_host_has_no_linker_for_is_refused() {
    let root = Root::replay(b"replay bytes");
    root.write_record(
        "{\n  \"v\": 2,\n  \"name\": \"probe\",\n  \"world\": \"brenn:surface\",\n  \
         \"artifact\": \"brenn_probe_component.wasm\",\n  \"artifact_sha256\": \"00\"\n}\n",
    );
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "and carries a specification")]
fn a_replay_record_carrying_spec_fields_is_refused() {
    let root = Root::replay(b"replay bytes");
    root.write_record(
        "{\n  \"v\": 2,\n  \"name\": \"probe\",\n  \"world\": \"brenn:replay\",\n  \
         \"artifact\": \"brenn_probe_component.wasm\",\n  \"artifact_sha256\": \"00\",\n  \
         \"spec\": \"probe.brenn\",\n  \"spec_sha256\": \"00\"\n}\n",
    );
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "and carries no specification")]
fn a_processor_record_without_spec_fields_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    root.write_record(
        "{\n  \"v\": 2,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"brenn_probe_component.wasm\",\n  \"artifact_sha256\": \"00\"\n}\n",
    );
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "names a spec without its hash")]
fn a_record_naming_a_spec_with_no_hash_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    root.write_record(
        "{\n  \"v\": 2,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"brenn_probe_component.wasm\",\n  \"artifact_sha256\": \"00\",\n  \
         \"spec\": \"probe.brenn\"\n}\n",
    );
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "names package")]
fn a_record_assembled_for_another_package_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    root.write_record(&format!(
        "{{\n  \"v\": 2,\n  \"name\": \"other\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"{ARTIFACT}\",\n  \"artifact_sha256\": \"{}\",\n  \
         \"spec\": \"other.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
        sha256_hex(b"artifact bytes"),
        sha256_hex(SPEC.as_bytes()),
    ));
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "which contains a path separator")]
fn a_record_reaching_out_of_its_own_directory_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    root.write_record(&format!(
        "{{\n  \"v\": 2,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"../other/{ARTIFACT}\",\n  \"artifact_sha256\": \"{}\",\n  \
         \"spec\": \"probe.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
        sha256_hex(b"artifact bytes"),
        sha256_hex(SPEC.as_bytes()),
    ));
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "which is not a .wasm file")]
fn a_record_naming_a_non_artifact_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    root.write_record(&format!(
        "{{\n  \"v\": 2,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"probe.wat\",\n  \"artifact_sha256\": \"{}\",\n  \
         \"spec\": \"probe.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
        sha256_hex(b"artifact bytes"),
        sha256_hex(SPEC.as_bytes()),
    ));
    load_record(&root.package());
}

#[test]
#[should_panic(expected = "names specification")]
fn a_record_naming_a_spec_other_than_the_packaged_one_is_refused() {
    // The hashes agree and the named file is right there — what is wrong is
    // that the layout fixes the spec's name to the package's, so the host would
    // verify a different file than the one the record bound.
    let root = Root::processor(b"artifact bytes", SPEC);
    std::fs::write(root.package().join("elsewhere.brenn"), SPEC).unwrap();
    root.write_record(&format!(
        "{{\n  \"v\": 2,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"{ARTIFACT}\",\n  \"artifact_sha256\": \"{}\",\n  \
         \"spec\": \"elsewhere.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
        sha256_hex(b"artifact bytes"),
        sha256_hex(SPEC.as_bytes()),
    ));
    load_record(&root.package());
}

// ── verifying what the record binds ──────────────────────────────────────────

#[test]
#[should_panic(expected = "but its package record binds")]
fn an_artifact_replaced_after_packaging_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    let record = load_record(&root.package());
    root.retouch_artifact(b"other bytes entirely");
    verify(&record, &root.package());
}

#[test]
#[should_panic(expected = "but the package record binds")]
fn a_packaged_spec_replaced_after_packaging_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    let record = load_record(&root.package());
    std::fs::write(
        spec_path(&root.package(), NAME),
        "component Sink { abi = processor; }\n",
    )
    .unwrap();
    verify(&record, &root.package());
}

#[test]
#[should_panic(expected = "packaged component specification")]
fn a_processor_package_missing_its_spec_file_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    let record = load_record(&root.package());
    std::fs::remove_file(spec_path(&root.package(), NAME)).unwrap();
    verify(&record, &root.package());
}

#[test]
#[should_panic(expected = "component artifact")]
fn a_package_missing_its_artifact_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    let record = load_record(&root.package());
    std::fs::remove_file(root.package().join(ARTIFACT)).unwrap();
    verify(&record, &root.package());
}

// ── resolving a package by name ──────────────────────────────────────────────

#[test]
fn a_consumer_is_handed_the_artifact_the_record_names() {
    let root = Root::processor(b"artifact bytes", SPEC);
    let artifact = verify_consumer(&root.roots(), NAME, "demo", &sha256_hex(SPEC.as_bytes()));
    assert_eq!(artifact, root.package().join(ARTIFACT));
}

#[test]
#[should_panic(expected = "is not an installed package directory")]
fn a_consumer_naming_a_package_this_release_does_not_ship_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    verify_consumer(&root.roots(), "panel", "demo", &sha256_hex(SPEC.as_bytes()));
}

#[test]
#[should_panic(expected = "is not an installed package directory")]
fn a_replay_endpoint_naming_an_uninstalled_package_is_refused() {
    let root = Root::replay(b"replay bytes");
    verify_replay(&root.roots(), "replay-typo", "hooks");
}

#[test]
#[should_panic(expected = "which is not a package name")]
fn a_replay_package_naming_an_absolute_path_is_refused() {
    // `join` on an absolute name discards the root entirely, so a name like
    // this would load a package the release never installed.
    let root = Root::replay(b"replay bytes");
    verify_replay(&root.roots(), "/srv/old-flat/replay", "hooks");
}

#[test]
#[should_panic(expected = "which is not a package name")]
fn a_package_name_climbing_out_of_the_root_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    verify_consumer(
        &root.roots(),
        &format!(
            "../{}/{NAME}",
            root.path().file_name().unwrap().to_str().unwrap()
        ),
        "demo",
        &sha256_hex(SPEC.as_bytes()),
    );
}

#[test]
#[should_panic(expected = "does not begin with `.`")]
fn a_dot_named_package_is_refused() {
    // A dot-named directory survives a glob-driven install sweep, so a name
    // like this can only resolve to something no release installed.
    let root = Root::replay(b"replay bytes");
    verify_replay(&root.roots(), ".hidden-replay", "hooks");
}

#[test]
#[should_panic(expected = "which is not a package name")]
fn an_empty_package_name_is_refused_before_it_resolves_to_the_root() {
    let root = Root::processor(b"artifact bytes", SPEC);
    verify_consumer(&root.roots(), "", "demo", &sha256_hex(SPEC.as_bytes()));
}

#[test]
#[should_panic(expected = "without --components")]
fn a_load_with_no_components_root_names_the_flag_that_is_missing() {
    require_components_root(&[], "consumer \"demo\"");
}

#[test]
fn a_components_root_that_was_passed_is_handed_straight_back() {
    let root = Root::replay(b"replay bytes");
    assert_eq!(
        require_components_root(&root.roots(), "consumer \"demo\""),
        root.roots()
    );
    assert_components_root(root.path());
}

#[test]
#[should_panic(expected = "which is not a directory")]
fn a_components_root_that_is_not_a_directory_is_refused_at_startup() {
    let root = Root::replay(b"replay bytes");
    assert_components_root(&root.package().join(ARTIFACT));
}

// ── the config↔package binding ───────────────────────────────────────────────

#[test]
#[should_panic(expected = "the release installed beside it disagree")]
fn a_consumer_configured_against_a_divergent_spec_is_refused() {
    // A comment-only divergence is still a divergence: the module root's copy
    // is the release's or it is drift.
    let root = Root::processor(b"artifact bytes", SPEC);
    let divergent = format!("// a deployer's note\n{SPEC}");
    verify_consumer(
        &root.roots(),
        NAME,
        "demo",
        &sha256_hex(divergent.as_bytes()),
    );
}

#[test]
#[should_panic(expected = "but its record declares world")]
fn a_consumer_pointed_at_a_replay_package_is_refused() {
    let root = Root::replay(b"replay bytes");
    verify_consumer(&root.roots(), NAME, "demo", &sha256_hex(SPEC.as_bytes()));
}

#[test]
fn a_replay_endpoint_with_a_correct_record_loads() {
    let root = Root::replay(b"replay bytes");
    assert_eq!(
        verify_replay(&root.roots(), NAME, "hooks"),
        root.package().join(ARTIFACT)
    );
}

#[test]
#[should_panic(expected = "but its record declares world")]
fn a_replay_endpoint_pointed_at_a_processor_package_is_refused() {
    let root = Root::processor(b"artifact bytes", SPEC);
    verify_replay(&root.roots(), NAME, "hooks");
}

// ── more than one components root ────────────────────────────────────────────
//
// A host started with one `--components` per installed release. A package is
// under exactly one of them.

#[test]
fn a_package_resolves_in_whichever_root_holds_it() {
    let brenn = Root::processor(b"artifact bytes", SPEC);
    let bundle = tempfile::tempdir().unwrap();
    let roots = vec![bundle.path().to_path_buf(), brenn.path().to_path_buf()];
    assert_eq!(
        verify_consumer(&roots, NAME, "demo", &sha256_hex(SPEC.as_bytes())),
        brenn.package().join(ARTIFACT)
    );
    assert_disjoint_components_roots(&roots);
}

/// The text of the refusal `f` panics with.
fn refusal(f: impl FnOnce()) -> String {
    let payload = crate::panic_util::catch_quietly(std::panic::AssertUnwindSafe(f))
        .expect_err("the call must refuse");
    crate::panic_util::panic_message(&*payload)
        .expect("a refusal carries text")
        .to_string()
}

#[test]
fn a_package_no_root_holds_is_refused_naming_every_root() {
    let brenn = Root::processor(b"artifact bytes", SPEC);
    let bundle = tempfile::tempdir().unwrap();
    let roots = vec![bundle.path().to_path_buf(), brenn.path().to_path_buf()];
    let message = refusal(|| {
        verify_consumer(&roots, "panel", "demo", &sha256_hex(SPEC.as_bytes()));
    });
    assert!(
        message.contains("is not an installed package directory under any --components root"),
        "{message}"
    );
    assert!(
        message.contains(&format!(
            "(searched: {}, {})",
            bundle.path().display(),
            brenn.path().display()
        )),
        "{message}"
    );
}

#[test]
fn a_package_under_two_roots_is_refused_at_resolution() {
    let first = Root::processor(b"artifact bytes", SPEC);
    let second = Root::processor(b"artifact bytes", SPEC);
    let roots = vec![first.path().to_path_buf(), second.path().to_path_buf()];
    let message = refusal(|| {
        verify_consumer(&roots, NAME, "demo", &sha256_hex(SPEC.as_bytes()));
    });
    assert!(
        message.contains(&format!(
            "is installed under more than one --components root: {}, {}.",
            first.package().display(),
            second.package().display()
        )),
        "{message}"
    );
}

#[test]
#[should_panic(expected = "without --components")]
fn a_resolution_against_no_roots_names_the_flag_rather_than_an_empty_list() {
    // The public `verify_*` entry points do not rely on their caller having
    // gone through `require_components_root` first.
    verify_replay(&[], NAME, "tap");
}

#[test]
fn every_cross_root_fault_is_in_the_one_refusal() {
    // Two colliding names and one root named twice: the operator reads all
    // three, not the first.
    let first = Root::processor(b"artifact bytes", SPEC);
    let second = Root::replay(b"replay bytes");
    std::fs::create_dir(first.path().join("relay")).unwrap();
    std::fs::create_dir(second.path().join("relay")).unwrap();
    let mut trailing = first.path().as_os_str().to_os_string();
    trailing.push("/");
    let roots = [
        first.path().to_path_buf(),
        second.path().to_path_buf(),
        PathBuf::from(trailing),
    ];
    let message = refusal(|| assert_disjoint_components_roots(&roots));
    assert!(message.contains("name the same directory"), "{message}");
    assert!(
        message.contains(&format!(
            "component package `{NAME}` is installed under more than one --components root: {}, {}.",
            first.path().display(),
            second.path().display()
        )),
        "{message}"
    );
    assert!(
        message.contains(&format!(
            "component package `relay` is installed under more than one --components root: {}, {}.",
            first.path().display(),
            second.path().display()
        )),
        "{message}"
    );
}

#[test]
#[should_panic(expected = "is installed under more than one --components root")]
fn a_package_under_two_roots_is_refused_before_anything_resolves_it() {
    // Same install, checked at startup with no consumer configured at all.
    let first = Root::processor(b"artifact bytes", SPEC);
    let second = Root::replay(b"replay bytes");
    assert_disjoint_components_roots(&[first.path().to_path_buf(), second.path().to_path_buf()]);
}

#[test]
#[should_panic(expected = "name the same directory")]
fn the_same_root_named_twice_is_refused_as_one_directory() {
    let root = Root::replay(b"replay bytes");
    let mut trailing = root.path().as_os_str().to_os_string();
    trailing.push("/");
    assert_disjoint_components_roots(&[root.path().to_path_buf(), PathBuf::from(trailing)]);
}

#[test]
fn a_plain_file_in_a_root_is_not_a_package_name() {
    // The installer refuses a flat file before the service is bounced; the
    // cross-root scan compares package directories and lets that refusal be the
    // one that names it.
    let first = Root::replay(b"replay bytes");
    let second = tempfile::tempdir().unwrap();
    std::fs::write(second.path().join(NAME), b"a stray file").unwrap();
    assert_disjoint_components_roots(&[first.path().to_path_buf(), second.path().to_path_buf()]);
}

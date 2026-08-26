//! The binding check, over real files in a temp directory.
//!
//! Verification reads bytes and compares digests; it never instantiates, so the
//! "artifacts" here are ordinary files. What is under test is the record
//! reader, the two hash comparisons and the config↔package binding — not the
//! WASM loader behind them, which has its own suites.

use std::path::{Path, PathBuf};

use super::*;

/// A package written into a fresh temp directory: the artifact bytes, the spec
/// text, and a record naming both correctly.
struct Package {
    dir: tempfile::TempDir,
    artifact: PathBuf,
}

impl Package {
    /// A processor-world package whose record binds exactly what is on disk.
    fn processor(artifact: &[u8], spec: &str) -> Package {
        let package = Package::bare(artifact);
        std::fs::write(spec_path(&package.artifact), spec).unwrap();
        package.write_record(&format!(
            "{{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
             \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"{}\",\n  \
             \"spec\": \"probe.spec.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
            sha256_hex(artifact),
            sha256_hex(spec.as_bytes()),
        ));
        package
    }

    /// A replay-world package: artifact and record, no spec.
    fn replay(artifact: &[u8]) -> Package {
        let package = Package::bare(artifact);
        package.write_record(&format!(
            "{{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:replay\",\n  \
             \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"{}\"\n}}\n",
            sha256_hex(artifact),
        ));
        package
    }

    /// The artifact alone — no record yet.
    fn bare(artifact: &[u8]) -> Package {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("probe.wasm");
        std::fs::write(&path, artifact).unwrap();
        Package {
            dir,
            artifact: path,
        }
    }

    fn write_record(&self, text: &str) {
        std::fs::write(record_path(&self.artifact), text).unwrap();
    }

    fn path(&self) -> &Path {
        &self.artifact
    }

    /// Overwrite the artifact after the record was written, which is the
    /// stale-install shape.
    fn retouch_artifact(&self, bytes: &[u8]) {
        std::fs::write(&self.artifact, bytes).unwrap();
        // The directory is what keeps the paths alive for the length of a test.
        assert!(self.dir.path().exists());
    }
}

/// The spec text the processor packages here are built with.
const SPEC: &str = "component Sink { abi = processor; requires = []; in messages; }\n";

// ── the sidecar paths ────────────────────────────────────────────────────────

#[test]
fn package_paths_are_derived_from_the_artifact_stem() {
    let artifact = Path::new("/lib/brenn_processor_demo.wasm");
    assert_eq!(
        record_path(artifact),
        PathBuf::from("/lib/brenn_processor_demo.package.json")
    );
    assert_eq!(
        spec_path(artifact),
        PathBuf::from("/lib/brenn_processor_demo.spec.brenn")
    );
}

// ── reading a record ─────────────────────────────────────────────────────────

#[test]
fn a_processor_record_reads_back_what_the_build_wrote() {
    let package = Package::processor(b"artifact bytes", SPEC);
    let record = load_record(package.path());
    assert_eq!(record.v, RECORD_VERSION);
    assert_eq!(record.world, WORLD_PROCESSOR);
    assert_eq!(record.artifact, "probe.wasm");
    assert_eq!(record.artifact_sha256, sha256_hex(b"artifact bytes"));
    assert_eq!(record.spec.as_deref(), Some("probe.spec.brenn"));
    assert_eq!(
        record.spec_sha256.as_deref(),
        Some(sha256_hex(SPEC.as_bytes())).as_deref()
    );
    verify(&record, package.path());
}

#[test]
fn a_replay_record_reads_back_and_verifies_without_a_spec() {
    let package = Package::replay(b"replay bytes");
    let record = load_record(package.path());
    assert_eq!(record.world, WORLD_REPLAY);
    assert_eq!(record.spec, None);
    assert_eq!(record.spec_sha256, None);
    verify(&record, package.path());
}

#[test]
#[should_panic(expected = "has no readable package record")]
fn a_component_installed_without_its_record_is_refused() {
    let package = Package::bare(b"artifact bytes");
    load_record(package.path());
}

#[test]
#[should_panic(expected = "is not a v1 record")]
fn an_unknown_field_is_refused_rather_than_ignored() {
    let package = Package::replay(b"replay bytes");
    package.write_record(
        "{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:replay\",\n  \
         \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"00\",\n  \
         \"signature\": \"later\"\n}\n",
    );
    load_record(package.path());
}

#[test]
#[should_panic(expected = "declares version 2")]
fn a_v2_record_on_this_host_is_refused() {
    let package = Package::replay(b"replay bytes");
    package.write_record(
        "{\n  \"v\": 2,\n  \"name\": \"probe\",\n  \"world\": \"brenn:replay\",\n  \
         \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"00\"\n}\n",
    );
    load_record(package.path());
}

#[test]
#[should_panic(expected = "which this host does not link")]
fn a_record_naming_a_world_this_host_has_no_linker_for_is_refused() {
    let package = Package::replay(b"replay bytes");
    package.write_record(
        "{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:surface\",\n  \
         \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"00\"\n}\n",
    );
    load_record(package.path());
}

#[test]
#[should_panic(expected = "and carries a specification")]
fn a_replay_record_carrying_spec_fields_is_refused() {
    let package = Package::replay(b"replay bytes");
    package.write_record(
        "{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:replay\",\n  \
         \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"00\",\n  \
         \"spec\": \"probe.spec.brenn\",\n  \"spec_sha256\": \"00\"\n}\n",
    );
    load_record(package.path());
}

#[test]
#[should_panic(expected = "and carries no specification")]
fn a_processor_record_without_spec_fields_is_refused() {
    let package = Package::processor(b"artifact bytes", SPEC);
    package.write_record(
        "{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"00\"\n}\n",
    );
    load_record(package.path());
}

#[test]
#[should_panic(expected = "names a spec without its hash")]
fn a_record_naming_a_spec_with_no_hash_is_refused() {
    let package = Package::processor(b"artifact bytes", SPEC);
    package.write_record(
        "{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"00\",\n  \
         \"spec\": \"probe.spec.brenn\"\n}\n",
    );
    load_record(package.path());
}

#[test]
#[should_panic(expected = "names component")]
fn a_record_assembled_for_another_component_is_refused() {
    let package = Package::processor(b"artifact bytes", SPEC);
    package.write_record(&format!(
        "{{\n  \"v\": 1,\n  \"name\": \"other\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"{}\",\n  \
         \"spec\": \"probe.spec.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
        sha256_hex(b"artifact bytes"),
        sha256_hex(SPEC.as_bytes()),
    ));
    load_record(package.path());
}

#[test]
#[should_panic(expected = "names artifact")]
fn a_record_naming_another_artifact_is_refused() {
    let package = Package::processor(b"artifact bytes", SPEC);
    package.write_record(&format!(
        "{{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"other.wasm\",\n  \"artifact_sha256\": \"{}\",\n  \
         \"spec\": \"probe.spec.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
        sha256_hex(b"artifact bytes"),
        sha256_hex(SPEC.as_bytes()),
    ));
    load_record(package.path());
}

#[test]
#[should_panic(expected = "names specification")]
fn a_record_naming_a_spec_other_than_the_packaged_one_is_refused() {
    // The hashes agree and the named file is right there — what is wrong is
    // that the host reads the stem-derived name and would verify a different
    // file than the one the record bound.
    let package = Package::processor(b"artifact bytes", SPEC);
    std::fs::write(package.dir.path().join("elsewhere.brenn"), SPEC).unwrap();
    package.write_record(&format!(
        "{{\n  \"v\": 1,\n  \"name\": \"probe\",\n  \"world\": \"brenn:processor\",\n  \
         \"artifact\": \"probe.wasm\",\n  \"artifact_sha256\": \"{}\",\n  \
         \"spec\": \"elsewhere.brenn\",\n  \"spec_sha256\": \"{}\"\n}}\n",
        sha256_hex(b"artifact bytes"),
        sha256_hex(SPEC.as_bytes()),
    ));
    load_record(package.path());
}

// ── verifying what the record binds ──────────────────────────────────────────

#[test]
#[should_panic(expected = "but its package record binds")]
fn an_artifact_replaced_after_packaging_is_refused() {
    let package = Package::processor(b"artifact bytes", SPEC);
    let record = load_record(package.path());
    package.retouch_artifact(b"other bytes entirely");
    verify(&record, package.path());
}

#[test]
#[should_panic(expected = "but the package record binds")]
fn a_packaged_spec_replaced_after_packaging_is_refused() {
    let package = Package::processor(b"artifact bytes", SPEC);
    let record = load_record(package.path());
    std::fs::write(
        spec_path(package.path()),
        "component Sink { abi = processor; }\n",
    )
    .unwrap();
    verify(&record, package.path());
}

#[test]
#[should_panic(expected = "packaged component specification")]
fn a_processor_package_missing_its_spec_file_is_refused() {
    let package = Package::processor(b"artifact bytes", SPEC);
    let record = load_record(package.path());
    std::fs::remove_file(spec_path(package.path())).unwrap();
    verify(&record, package.path());
}

// ── the config↔package binding ───────────────────────────────────────────────

#[test]
fn a_consumer_whose_config_spec_matches_the_packaged_one_loads() {
    let package = Package::processor(b"artifact bytes", SPEC);
    verify_consumer(package.path(), "demo", &sha256_hex(SPEC.as_bytes()));
}

#[test]
#[should_panic(expected = "The author's specification travels with the component")]
fn a_consumer_configured_against_a_divergent_spec_is_refused() {
    // A comment-only divergence is still a divergence: the deployment's copy is
    // verbatim or it is drift.
    let package = Package::processor(b"artifact bytes", SPEC);
    let divergent = format!("// a deployer's note\n{SPEC}");
    verify_consumer(package.path(), "demo", &sha256_hex(divergent.as_bytes()));
}

#[test]
#[should_panic(expected = "but its package record declares world")]
fn a_consumer_pointed_at_a_replay_artifact_is_refused() {
    let package = Package::replay(b"replay bytes");
    verify_consumer(package.path(), "demo", &sha256_hex(SPEC.as_bytes()));
}

#[test]
fn a_replay_endpoint_with_a_correct_record_loads() {
    let package = Package::replay(b"replay bytes");
    verify_replay(package.path(), "hooks");
}

#[test]
#[should_panic(expected = "but its package record declares world")]
fn a_replay_endpoint_pointed_at_a_processor_artifact_is_refused() {
    let package = Package::processor(b"artifact bytes", SPEC);
    verify_replay(package.path(), "hooks");
}

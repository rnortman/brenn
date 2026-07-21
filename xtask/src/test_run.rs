// `xtask test`: run the workspace test suite via cargo-nextest with an
// incremental binary-hash result cache. A test binary is skipped when its bytes
// (a content hash of its entire transitive Rust input closure — cargo relinks it
// iff any crate in its graph changed) and the runtime environment key are
// unchanged since it last fully passed. Doctests always run (nextest skips them).

use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const SCHEMA_VERSION: u32 = 1;
const NEXTEST_PROFILE: &str = "default";
const NEXTEST_INSTALL: &str = "cargo install --locked cargo-nextest";

/// One test binary as enumerated by nextest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryInfo {
    pub binary_id: String,
    pub path: PathBuf,
    /// Count of non-ignored testcases nextest listed for this binary. Recording a
    /// pass requires the JUnit report to show exactly this many tests ran, so a
    /// run stopped mid-binary (signal, setup error) records nothing for it.
    pub test_count: u64,
}

/// Metadata fast-path entry: `(size, mtime) → content_hash`. Reused without
/// re-reading the file when a binary's on-disk metadata is unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct MetaEntry {
    size: u64,
    mtime_secs: i64,
    mtime_nanos: u32,
    content_hash: String,
}

/// Recorded pass for a binary: the content hash + environment key it passed under.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PassRecord {
    content_hash: String,
    env_key: String,
}

/// On-disk cache. A schema mismatch or parse failure discards the whole thing
/// (a cache miss is always safe); the JSON is a pure optimization artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cache {
    schema_version: u32,
    #[serde(default)]
    meta: BTreeMap<String, MetaEntry>,
    #[serde(default)]
    passed: BTreeMap<String, PassRecord>,
}

impl Default for Cache {
    fn default() -> Self {
        Cache {
            schema_version: SCHEMA_VERSION,
            meta: BTreeMap::new(),
            passed: BTreeMap::new(),
        }
    }
}

impl Cache {
    /// Load the cache, or return an empty cache if the file is absent,
    /// unparseable, or a different schema version. A read error other than
    /// absence (permissions, I/O) panics rather than silently disabling the
    /// cache.
    fn load(path: &Path) -> Cache {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            // Absence is the normal first-run state → empty cache silently. Any
            // other read error (EACCES, I/O) must not masquerade as a cache miss:
            // that would degrade every run to a full suite with no diagnostic.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Cache::default(),
            Err(e) => panic!("xtask test: failed to read cache {path:?}: {e}"),
        };
        Self::parse(&bytes)
    }

    fn parse(bytes: &[u8]) -> Cache {
        match serde_json::from_slice::<Cache>(bytes) {
            Ok(c) if c.schema_version == SCHEMA_VERSION => c,
            Ok(_) => {
                eprintln!("xtask test: cache schema mismatch — discarding cache, running all");
                Cache::default()
            }
            Err(_) => {
                eprintln!("xtask test: cache unparseable — discarding cache, running all");
                Cache::default()
            }
        }
    }

    /// Write atomically (temp file + rename), last-writer-wins. Two concurrent
    /// runs at worst lose a pass record → an extra rerun. No locking.
    fn save_atomic(&self, path: &Path) {
        let dir = path
            .parent()
            .unwrap_or_else(|| panic!("xtask test: cache path {path:?} has no parent"));
        std::fs::create_dir_all(dir)
            .unwrap_or_else(|e| panic!("xtask test: failed to create {dir:?}: {e}"));
        let json = serde_json::to_vec_pretty(self)
            .unwrap_or_else(|e| panic!("xtask test: failed to serialize cache: {e}"));
        let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));
        std::fs::write(&tmp, &json)
            .unwrap_or_else(|e| panic!("xtask test: failed to write {tmp:?}: {e}"));
        std::fs::rename(&tmp, path)
            .unwrap_or_else(|e| panic!("xtask test: failed to rename {tmp:?} → {path:?}: {e}"));
    }
}

/// Raw inputs to the environment key: everything tests consume at runtime that
/// is NOT captured by a test binary's own bytes. Kept as collected bytes/values
/// so the key computation is pure and each input is independently testable.
#[derive(Debug, Clone, Default)]
pub struct EnvInputs {
    /// `brenn-wasm/target/components/*.wasm` fixtures (integration tests load these).
    pub wasm_fixtures: Vec<(String, Vec<u8>)>,
    pub toolchain: Vec<u8>,
    pub nextest_config: Vec<u8>,
    /// Checked-in config TOMLs read at runtime by brenn-lib config tests.
    pub config_files: Vec<(String, Vec<u8>)>,
    /// `brenn-lib/tests/mqtt_assets/*` (certs/config read at runtime, not compiled in).
    pub mqtt_assets: Vec<(String, Vec<u8>)>,
    /// `cc-usage/tests/fixtures/**` (golden/input files read at runtime via `std::fs`,
    /// not compiled in). Recursive: the tree has subdirectories.
    pub test_fixtures: Vec<(String, Vec<u8>)>,
    /// Behavior-altering env vars: name → value (None = unset).
    pub env_vars: Vec<(String, Option<String>)>,
}

/// Content hash of raw bytes. A cache key, not a security boundary — a fast
/// non-cryptographic-strength use of a fast hash.
pub fn content_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// Hash the environment key. Each labeled section is length-prefixed so distinct
/// inputs cannot alias by concatenation.
pub fn compute_env_key(inputs: &EnvInputs) -> String {
    let mut h = blake3::Hasher::new();
    let mut section = |label: &str, items: &[(String, Vec<u8>)]| {
        h.update(label.as_bytes());
        h.update(&(items.len() as u64).to_le_bytes());
        for (name, bytes) in items {
            h.update(&(name.len() as u64).to_le_bytes());
            h.update(name.as_bytes());
            h.update(&(bytes.len() as u64).to_le_bytes());
            h.update(bytes);
        }
    };
    section("wasm_fixtures", &inputs.wasm_fixtures);
    section("config_files", &inputs.config_files);
    section("mqtt_assets", &inputs.mqtt_assets);
    section("test_fixtures", &inputs.test_fixtures);

    h.update(b"toolchain");
    h.update(&(inputs.toolchain.len() as u64).to_le_bytes());
    h.update(&inputs.toolchain);
    h.update(b"nextest_config");
    h.update(&(inputs.nextest_config.len() as u64).to_le_bytes());
    h.update(&inputs.nextest_config);

    h.update(b"env_vars");
    h.update(&(inputs.env_vars.len() as u64).to_le_bytes());
    for (name, val) in &inputs.env_vars {
        h.update(&(name.len() as u64).to_le_bytes());
        h.update(name.as_bytes());
        match val {
            Some(v) => {
                h.update(&[1u8]);
                h.update(&(v.len() as u64).to_le_bytes());
                h.update(v.as_bytes());
            }
            None => {
                h.update(&[0u8]);
            }
        };
    }
    h.finalize().to_hex().to_string()
}

/// Whether an existing metadata entry still describes the on-disk file. When it
/// does, its stored content hash is reused without re-reading the binary.
fn meta_hit(
    existing: Option<&MetaEntry>,
    size: u64,
    mtime_secs: i64,
    mtime_nanos: u32,
) -> Option<&str> {
    let e = existing?;
    if e.size == size && e.mtime_secs == mtime_secs && e.mtime_nanos == mtime_nanos {
        Some(&e.content_hash)
    } else {
        None
    }
}

/// Parse `cargo nextest list --message-format json` output into the binary set.
/// Enumerates only from nextest's suites — never from a `deps/` directory scan,
/// which accumulates stale artifacts.
pub fn parse_nextest_list(json: &str) -> Vec<BinaryInfo> {
    let v: serde_json::Value = serde_json::from_str(json)
        .unwrap_or_else(|e| panic!("xtask test: failed to parse nextest list JSON: {e}"));
    let suites = v
        .get("rust-suites")
        .and_then(|s| s.as_object())
        .unwrap_or_else(|| panic!("xtask test: nextest list JSON has no `rust-suites` object"));
    let mut out = Vec::new();
    for (binary_id, suite) in suites {
        let path = suite
            .get("binary-path")
            .and_then(|p| p.as_str())
            .unwrap_or_else(|| {
                panic!("xtask test: nextest suite {binary_id:?} has no `binary-path`")
            });
        // Non-ignored testcases are the ones nextest actually runs; that is the
        // count the JUnit report must match for a complete-suite pass record.
        let test_count = suite
            .get("testcases")
            .and_then(|t| t.as_object())
            .map(|cases| {
                cases
                    .values()
                    .filter(|tc| !tc.get("ignored").and_then(|i| i.as_bool()).unwrap_or(false))
                    .count() as u64
            })
            .unwrap_or(0);
        out.push(BinaryInfo {
            binary_id: binary_id.clone(),
            path: PathBuf::from(path),
            test_count,
        });
    }
    out.sort_by(|a, b| a.binary_id.cmp(&b.binary_id));
    out
}

/// Select the binaries that must run: those whose current (content_hash, env_key)
/// does not match a recorded pass. `hashes` maps binary_id → current content hash.
fn select_stale<'a>(
    binaries: &'a [BinaryInfo],
    hashes: &BTreeMap<String, String>,
    env_key: &str,
    cache: &Cache,
) -> Vec<&'a BinaryInfo> {
    binaries
        .iter()
        .filter(|b| {
            let cur = match hashes.get(&b.binary_id) {
                Some(h) => h,
                None => return true, // no hash → must run (defensive)
            };
            match cache.passed.get(&b.binary_id) {
                Some(rec) => !(rec.content_hash == *cur && rec.env_key == env_key),
                None => true,
            }
        })
        .collect()
}

/// A nextest filterset selecting exactly the given binary IDs (union of exact
/// `binary_id` matches). Empty input yields `none()`.
// TODO(nextest-e2e-verification): verify one green cache-off CI run after this
// lands (self-resolves on first push to main); local verification is in the ADR
// implementation log.
pub fn build_filterset(ids: &[String]) -> String {
    if ids.is_empty() {
        return "none()".to_string();
    }
    ids.iter()
        .map(|id| format!("binary_id(={id})"))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Per-binary pass/fail parsed from a nextest JUnit XML report. A binary passed
/// iff its testsuite reported zero failures and zero errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteResult {
    pub binary_id: String,
    /// Number of testcases the report says ran for this suite. Compared against the
    /// expected non-ignored count so a partially-run (cancelled) suite is not
    /// recorded as a full pass.
    pub tests: u64,
    pub passed: bool,
}

/// Parse a nextest JUnit XML report into per-testsuite results. The testsuite
/// `name` attribute is the nextest binary id.
pub fn parse_junit(xml: &str) -> Vec<SuiteResult> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut out = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Empty(e)) | Ok(Event::Start(e)) if e.name().as_ref() == b"testsuite" => {
                {
                    let mut name: Option<String> = None;
                    let mut tests: u64 = 0;
                    let mut failures: u64 = 0;
                    let mut errors: u64 = 0;
                    for attr in e.attributes() {
                        let attr = attr.unwrap_or_else(|err| {
                            panic!("xtask test: malformed JUnit attribute: {err}")
                        });
                        // name / tests / failures / errors carry no XML entities
                        // in practice; the raw value is what we key and count on.
                        let val = String::from_utf8_lossy(&attr.value).to_string();
                        match attr.key.as_ref() {
                            b"name" => name = Some(val),
                            // An unparseable count keys to 0 tests / MAX failures —
                            // both force the safe direction (not recorded as pass).
                            b"tests" => tests = val.parse().unwrap_or(0),
                            b"failures" => failures = val.parse().unwrap_or(u64::MAX),
                            b"errors" => errors = val.parse().unwrap_or(u64::MAX),
                            _ => {}
                        }
                    }
                    if let Some(binary_id) = name {
                        out.push(SuiteResult {
                            binary_id,
                            tests,
                            passed: failures == 0 && errors == 0,
                        });
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => panic!("xtask test: failed to parse JUnit XML: {e}"),
            _ => {}
        }
    }
    out
}

/// Resolve the cargo target directory (honors `CARGO_TARGET_DIR`) via
/// `cargo metadata`. The cache lives beside the binaries it describes.
fn resolve_target_dir(repo_root: &Path) -> PathBuf {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(repo_root)
        .output()
        .unwrap_or_else(|e| panic!("xtask test: failed to run cargo metadata: {e}"));
    if !output.status.success() {
        panic!(
            "xtask test: cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let v: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("xtask test: failed to parse cargo metadata: {e}"));
    let dir = v
        .get("target_directory")
        .and_then(|d| d.as_str())
        .unwrap_or_else(|| panic!("xtask test: cargo metadata has no target_directory"));
    PathBuf::from(dir)
}

/// Read a file's bytes, or empty if absent (absence is itself part of the key).
/// Any error other than NotFound panics — a file that exists but cannot be read
/// must not silently key as empty (that would hide all changes to it).
fn read_or_empty(path: &Path) -> Vec<u8> {
    match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("xtask test: failed to read {path:?}: {e}"),
    }
}

/// Collect every runtime input feeding the environment key. See the audit in
/// the implementation notes for how this set was derived.
fn collect_env_inputs(repo_root: &Path) -> EnvInputs {
    let mut inputs = EnvInputs {
        toolchain: read_or_empty(&repo_root.join("rust-toolchain.toml")),
        nextest_config: read_or_empty(&repo_root.join(".config").join("nextest.toml")),
        ..Default::default()
    };

    // WASM fixtures consumed by integration tests.
    let comp_dir = repo_root
        .join("brenn-wasm")
        .join("target")
        .join("components");
    inputs.wasm_fixtures = read_dir_files(&comp_dir, Some("wasm"));

    // Checked-in config TOMLs read at runtime by brenn-lib config tests.
    // TODO(scrub-template-drift-cache-skip): `.gitleaks.toml` and
    // `scrub/repo-template/gitleaks.toml` are read at runtime by the scrub
    // template-parity test but are absent from this env key, so drift between
    // them does not invalidate the scrub::rules cache entry and can pass
    // unnoticed until that binary is recompiled for another reason.
    for name in ["brenn.dev.toml", "brenn.e2e.toml"] {
        let p = repo_root.join(name);
        inputs
            .config_files
            .push((name.to_string(), read_or_empty(&p)));
    }

    // mqtt_assets read at runtime by brenn-lib mqtt tests.
    let assets_dir = repo_root
        .join("brenn-lib")
        .join("tests")
        .join("mqtt_assets");
    inputs.mqtt_assets = read_dir_files(&assets_dir, None);

    // cc-usage golden/input fixtures read at runtime via std::fs (recursive: the
    // fixture tree has subdirectories). A hand-edit or regeneration of a golden
    // changes no Rust source, so only the env key catches it.
    let cc_fixtures = repo_root.join("cc-usage").join("tests").join("fixtures");
    inputs.test_fixtures = read_dir_files_recursive(&cc_fixtures);

    // Behavior-altering env vars found by the audit.
    for name in [
        "BRENN_MQTT_INTEGRATION",
        "BRENN_MOSQUITTO_BIN",
        "UPDATE_GOLDEN",
    ] {
        inputs
            .env_vars
            .push((name.to_string(), std::env::var(name).ok()));
    }

    inputs
}

/// Read every file directly under `dir` (sorted by name), optionally filtered by
/// extension. Returns (name, bytes). Missing dir → empty.
fn read_dir_files(dir: &Path, ext: Option<&str>) -> Vec<(String, Vec<u8>)> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        // Absent dir is itself part of the key (empty). Any other error (permission,
        // I/O) must fail loudly — a dir that exists but can't be read must never key
        // as empty, which would hide every change to its contents.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => panic!("xtask test: failed to read {dir:?}: {e}"),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("xtask test: failed to read {dir:?}: {e}"));
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(want) = ext
            && path.extension().and_then(|e| e.to_str()) != Some(want)
        {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        out.push((name, read_or_empty(&path)));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Read every file under `dir` recursively (relative paths as names, sorted).
/// Missing dir → empty. Used for fixture trees that contain subdirectories.
fn read_dir_files_recursive(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    walk_dir(dir, dir, &mut out);
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn walk_dir(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => panic!("xtask test: failed to read {dir:?}: {e}"),
    };
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("xtask test: failed to read {dir:?}: {e}"));
        let path = entry.path();
        if path.is_dir() {
            walk_dir(root, &path, out);
        } else if path.is_file() {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .to_string();
            out.push((rel, read_or_empty(&path)));
        }
    }
}

/// Stat a binary for the cache's metadata fast path: (size, mtime secs, mtime
/// nanos). Any stat failure panics — a binary nextest just listed must be present.
fn stat_meta(path: &Path) -> (u64, i64, u32) {
    use std::time::UNIX_EPOCH;
    let meta = std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("xtask test: failed to stat {path:?}: {e}"));
    let mtime = meta
        .modified()
        .unwrap_or_else(|e| panic!("xtask test: no mtime for {path:?}: {e}"));
    let dur = mtime
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|e| panic!("xtask test: mtime before epoch for {path:?}: {e}"));
    (meta.len(), dur.as_secs() as i64, dur.subsec_nanos())
}

/// Compute current content hashes for every binary, using the metadata fast path
/// and hashing stale binaries in parallel. Returns (id → hash) and the fresh
/// metadata map to persist.
fn compute_hashes(
    binaries: &[BinaryInfo],
    prev_meta: &BTreeMap<String, MetaEntry>,
) -> (BTreeMap<String, String>, BTreeMap<String, MetaEntry>) {
    struct Job<'a> {
        info: &'a BinaryInfo,
        size: u64,
        mtime_secs: i64,
        mtime_nanos: u32,
        cached_hash: Option<String>,
    }

    let mut jobs: Vec<Job> = Vec::with_capacity(binaries.len());
    for b in binaries {
        let key = b.path.to_string_lossy().to_string();
        let (size, secs, nanos) = stat_meta(&b.path);
        let cached_hash = meta_hit(prev_meta.get(&key), size, secs, nanos).map(str::to_string);
        jobs.push(Job {
            info: b,
            size,
            mtime_secs: secs,
            mtime_nanos: nanos,
            cached_hash,
        });
    }

    // Hash the cache-miss binaries in parallel across a bounded thread pool.
    let stale: Vec<&Job> = jobs.iter().filter(|j| j.cached_hash.is_none()).collect();
    let hashed: BTreeMap<String, String> = std::thread::scope(|scope| {
        let n = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        let chunk = stale.len().div_ceil(n.max(1)).max(1);
        let handles: Vec<_> = stale
            .chunks(chunk)
            .map(|group| {
                scope.spawn(move || {
                    group
                        .iter()
                        .map(|j| {
                            let bytes = std::fs::read(&j.info.path).unwrap_or_else(|e| {
                                panic!("xtask test: failed to read {:?}: {e}", j.info.path)
                            });
                            (
                                j.info.path.to_string_lossy().to_string(),
                                content_hash(&bytes),
                            )
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("xtask test: hash thread panicked"))
            .collect()
    });

    let mut hashes = BTreeMap::new();
    let mut new_meta = BTreeMap::new();
    for j in &jobs {
        let key = j.info.path.to_string_lossy().to_string();
        let hash = match &j.cached_hash {
            Some(h) => h.clone(),
            None => hashed
                .get(&key)
                .unwrap_or_else(|| panic!("xtask test: missing hash for {key}"))
                .clone(),
        };
        hashes.insert(j.info.binary_id.clone(), hash.clone());
        new_meta.insert(
            key,
            MetaEntry {
                size: j.size,
                mtime_secs: j.mtime_secs,
                mtime_nanos: j.mtime_nanos,
                content_hash: hash,
            },
        );
    }
    (hashes, new_meta)
}

/// Fail fast unless cargo-nextest is present on PATH. Presence only — no version
/// assertion: whatever nextest is installed is used, and an incompatible one fails
/// the run loudly (unknown flag, missing JUnit output) rather than being pre-judged.
/// Never silently falls back to `cargo test` (that would flip the execution model
/// unnoticed).
fn assert_nextest_available() {
    let present = Command::new("cargo")
        .args(["nextest", "--version"])
        .output()
        .is_ok_and(|o| o.status.success());
    if !present {
        eprintln!("ERROR: cargo-nextest not found; install with: {NEXTEST_INSTALL}");
        std::process::exit(1);
    }
}

/// Parse the `BRENN_TEST_CACHE` control. Unset / empty / `"1"` enable the cache;
/// `"0"` disables it; any other value is a hard error. A silently-ignored spelling
/// like `off`/`false`/`no` would leave the cache ON in exactly the situation where
/// the user is reaching for the knob to turn it OFF (distrusting a suspected false
/// skip), so a non-conforming value fails loudly rather than no-opping.
fn cache_enabled_from(val: Option<&str>) -> bool {
    match val {
        None | Some("") | Some("1") => true,
        Some("0") => false,
        Some(other) => panic!(
            "xtask test: BRENN_TEST_CACHE must be unset, \"\", \"1\" (enabled) or \"0\" (disabled), got {other:?}"
        ),
    }
}

/// Pure pass-recording decision for one suite. A suite is recorded as a pass iff
/// every non-ignored test nextest listed actually ran and passed (`complete` and
/// `passed`), a current content hash exists for it, and its binary did not change
/// on disk during the run (`meta_unchanged`). Returns the content hash to record,
/// or `None` to record nothing — the safe direction, which reruns it next time.
fn record_decision(
    suite: &SuiteResult,
    expected: Option<u64>,
    cur_hash: Option<&str>,
    meta_unchanged: bool,
) -> Option<String> {
    let complete = expected.is_some_and(|n| n == suite.tests);
    if !suite.passed || !complete {
        return None;
    }
    let hash = cur_hash?;
    if !meta_unchanged {
        return None;
    }
    Some(hash.to_string())
}

/// Entry point for `cargo run -p xtask -- test`. Returns true on success.
pub fn run_test(repo_root: &Path) -> bool {
    assert_nextest_available();
    let cache_enabled = cache_enabled_from(std::env::var("BRENN_TEST_CACHE").ok().as_deref());

    // Enumerate (this also compiles the test binaries).
    let binaries = list_test_binaries(repo_root);
    println!("xtask test: {} test binaries", binaries.len());

    let target_dir = resolve_target_dir(repo_root);

    let run_ok = if cache_enabled {
        run_cached(repo_root, &target_dir, &binaries)
    } else {
        // Cache disabled (CI, or a local opt-out): run the whole set. No env-input
        // collection, no per-binary hashing (gigabytes of pointless I/O), no cache
        // consult/record — none of it feeds anything when the cache is off.
        println!(
            "xtask test: cache disabled — running all {} binaries",
            binaries.len()
        );
        run_nextest(repo_root, &target_dir, None)
    };

    // Doctests always run (nextest does not execute them).
    let doc_ok = run_doctests(repo_root);
    run_ok && doc_ok
}

/// Cache-enabled path: hash binaries, skip provably-unchanged suites, record the
/// passes from this run's fresh JUnit report. Returns the nextest exit status.
fn run_cached(repo_root: &Path, target_dir: &Path, binaries: &[BinaryInfo]) -> bool {
    let env_inputs = collect_env_inputs(repo_root);
    let env_key = compute_env_key(&env_inputs);

    let cache_path = target_dir.join("brenn-test-cache").join("cache.json");
    let mut cache = Cache::load(&cache_path);

    let (hashes, new_meta) = compute_hashes(binaries, &cache.meta);
    cache.meta = new_meta;

    let stale = select_stale(binaries, &hashes, &env_key, &cache);

    // A binary with no non-ignored tests never produces a JUnit testsuite, so it
    // can never be recorded as passed and would stay perpetually stale. Selecting
    // only such binaries also makes nextest exit with "no tests to run". They have
    // nothing to execute — drop them from the run set.
    let runnable: Vec<&BinaryInfo> = stale.into_iter().filter(|b| b.test_count > 0).collect();

    let mut ok = true;
    if runnable.is_empty() {
        println!(
            "xtask test: all runnable binaries cached — skipping nextest run ({} total)",
            binaries.len()
        );
    } else {
        println!(
            "xtask test: running {} of {} binaries",
            runnable.len(),
            binaries.len()
        );
        let ids: Vec<String> = runnable.iter().map(|b| b.binary_id.clone()).collect();

        // Delete any prior JUnit report before the run. Otherwise a nextest that
        // exits without writing a fresh one (killed mid-run, setup error, crash)
        // would leave a previous run's report to be parsed and recorded as passes
        // under the CURRENT hashes — a false skip. An interrupted run must record
        // nothing.
        // TODO(test-cache-concurrent-report): two concurrent cache-enabled runs in
        // one target dir share this junit path; run B can overwrite it between run
        // A's nextest write and A's read, so A may record B's results under A's env
        // key. A robust fix (a run-level lock, or a per-run report path) is a
        // concurrency-model decision beyond respond-mode patching.
        let junit_path = target_dir
            .join("nextest")
            .join(NEXTEST_PROFILE)
            .join("junit.xml");
        match std::fs::remove_file(&junit_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("xtask test: failed to remove stale JUnit report {junit_path:?}: {e}"),
        }

        let run_ok = run_nextest(repo_root, target_dir, Some(&build_filterset(&ids)));

        // Expected non-ignored test count and on-disk path per binary, for the
        // complete-suite and stable-binary gates below.
        let expected: BTreeMap<&str, u64> = binaries
            .iter()
            .map(|b| (b.binary_id.as_str(), b.test_count))
            .collect();
        let path_by_id: BTreeMap<&str, &Path> = binaries
            .iter()
            .map(|b| (b.binary_id.as_str(), b.path.as_path()))
            .collect();

        // Record passes from THIS run's report regardless of overall status. A
        // binary is recorded only when every one of its tests actually ran and
        // passed. A missing report (nextest crashed/was killed before writing)
        // records nothing — the safe direction.
        match std::fs::read_to_string(&junit_path) {
            Ok(xml) => {
                for suite in parse_junit(&xml) {
                    let expected_n = expected.get(suite.binary_id.as_str()).copied();
                    let cur_hash = hashes.get(&suite.binary_id).cloned();

                    // The complete-suite / passed / hash-present gates are a pure
                    // decision. Evaluate them first (stability assumed); if they
                    // already decline, record nothing without touching the disk.
                    if record_decision(&suite, expected_n, cur_hash.as_deref(), true).is_none() {
                        continue;
                    }
                    // Stable-binary gate: if a mid-run edit relinked this binary
                    // after it was hashed, the bytes that passed are not the bytes
                    // we hashed. Re-stat and record only if metadata is unchanged.
                    let Some(&path) = path_by_id.get(suite.binary_id.as_str()) else {
                        continue;
                    };
                    let prev = cache.meta.get(&path.to_string_lossy().to_string());
                    let (size, secs, nanos) = stat_meta(path);
                    let unchanged = meta_hit(prev, size, secs, nanos).is_some();
                    match record_decision(&suite, expected_n, cur_hash.as_deref(), unchanged) {
                        Some(hash) => {
                            cache.passed.insert(
                                suite.binary_id.clone(),
                                PassRecord {
                                    content_hash: hash,
                                    env_key: env_key.clone(),
                                },
                            );
                        }
                        None => {
                            eprintln!(
                                "xtask test: {} changed on disk during the run — not recording",
                                suite.binary_id
                            );
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("xtask test: no JUnit report after nextest run — recording no passes");
            }
            Err(e) => panic!("xtask test: failed to read JUnit report {junit_path:?}: {e}"),
        }
        ok = run_ok;
    }

    // Drop pass records for binaries that no longer exist (renamed/deleted crates);
    // stale IDs are never queried but would otherwise grow the file unbounded.
    let live: std::collections::BTreeSet<&str> =
        binaries.iter().map(|b| b.binary_id.as_str()).collect();
    cache.passed.retain(|id, _| live.contains(id.as_str()));

    cache.save_atomic(&cache_path);
    ok
}

fn list_test_binaries(repo_root: &Path) -> Vec<BinaryInfo> {
    let output = Command::new("cargo")
        .args(["nextest", "list", "--message-format", "json"])
        .current_dir(repo_root)
        .output()
        .unwrap_or_else(|e| panic!("xtask test: failed to run cargo nextest list: {e}"));
    if !output.status.success() {
        panic!(
            "xtask test: cargo nextest list failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    parse_nextest_list(&String::from_utf8_lossy(&output.stdout))
}

/// Run nextest. `filterset` restricts the run to specific binaries (cache path);
/// `None` runs the entire suite (cache disabled).
fn run_nextest(repo_root: &Path, target_dir: &Path, filterset: Option<&str>) -> bool {
    let mut cmd = Command::new("cargo");
    // --no-fail-fast: never cancel the run on the first failure. Cancellation would
    // leave later tests unrun while the JUnit report still shows their sibling
    // suites as failure-free, which the cache would wrongly record as a full pass.
    // It also gives aggregate failure reporting, matching the rest of the pipeline.
    cmd.args([
        "nextest",
        "run",
        "--no-fail-fast",
        "--profile",
        NEXTEST_PROFILE,
        "--target-dir",
    ])
    .arg(target_dir);
    if let Some(f) = filterset {
        cmd.args(["-E", f]);
    }
    cmd.current_dir(repo_root)
        .status()
        .unwrap_or_else(|e| panic!("xtask test: failed to run cargo nextest run: {e}"))
        .success()
}

fn run_doctests(repo_root: &Path) -> bool {
    Command::new("cargo")
        .args(["test", "--doc"])
        .current_dir(repo_root)
        .status()
        .unwrap_or_else(|e| panic!("xtask test: failed to run cargo test --doc: {e}"))
        .success()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn sample_inputs() -> EnvInputs {
        EnvInputs {
            wasm_fixtures: vec![("a.wasm".into(), bytes("wasmA"))],
            toolchain: bytes("channel=1.95.0"),
            nextest_config: bytes("[profile.default]"),
            config_files: vec![("brenn.dev.toml".into(), bytes("dev"))],
            mqtt_assets: vec![("ca.pem".into(), bytes("cert"))],
            test_fixtures: vec![("sub/g.golden".into(), bytes("golden"))],
            env_vars: vec![("BRENN_MQTT_INTEGRATION".into(), None)],
        }
    }

    #[test]
    fn cache_enabled_parsing() {
        assert!(cache_enabled_from(None));
        assert!(cache_enabled_from(Some("")));
        assert!(cache_enabled_from(Some("1")));
        assert!(!cache_enabled_from(Some("0")));
    }

    #[test]
    #[should_panic(expected = "BRENN_TEST_CACHE")]
    fn cache_enabled_rejects_bogus_value() {
        cache_enabled_from(Some("off"));
    }

    #[test]
    fn record_decision_gates() {
        let suite = |passed, tests| SuiteResult {
            binary_id: "x".into(),
            tests,
            passed,
        };
        // passed + complete + hash + stable → record that hash.
        assert_eq!(
            record_decision(&suite(true, 3), Some(3), Some("h"), true),
            Some("h".to_string())
        );
        // passed but incomplete (fewer tests than nextest listed) → nothing.
        assert_eq!(
            record_decision(&suite(true, 2), Some(3), Some("h"), true),
            None
        );
        // failed → nothing.
        assert_eq!(
            record_decision(&suite(false, 3), Some(3), Some("h"), true),
            None
        );
        // no expected count for this binary → nothing.
        assert_eq!(
            record_decision(&suite(true, 3), None, Some("h"), true),
            None
        );
        // no current hash → nothing.
        assert_eq!(record_decision(&suite(true, 3), Some(3), None, true), None);
        // binary changed on disk during the run → nothing.
        assert_eq!(
            record_decision(&suite(true, 3), Some(3), Some("h"), false),
            None
        );
    }

    #[test]
    fn content_hash_changes_with_bytes() {
        assert_ne!(content_hash(b"aaa"), content_hash(b"aab"));
        assert_eq!(content_hash(b"aaa"), content_hash(b"aaa"));
    }

    #[test]
    fn env_key_wasm_fixture_bytes_change_key() {
        let base = compute_env_key(&sample_inputs());
        let mut m = sample_inputs();
        m.wasm_fixtures[0].1 = bytes("wasmB");
        assert_ne!(base, compute_env_key(&m));
    }

    #[test]
    fn env_key_toolchain_changes_key() {
        let base = compute_env_key(&sample_inputs());
        let mut m = sample_inputs();
        m.toolchain = bytes("channel=1.96.0");
        assert_ne!(base, compute_env_key(&m));
    }

    #[test]
    fn env_key_nextest_config_changes_key() {
        let base = compute_env_key(&sample_inputs());
        let mut m = sample_inputs();
        m.nextest_config = bytes("[profile.ci]");
        assert_ne!(base, compute_env_key(&m));
    }

    #[test]
    fn env_key_env_var_value_changes_key() {
        let base = compute_env_key(&sample_inputs());
        let mut m = sample_inputs();
        m.env_vars[0].1 = Some("1".into());
        assert_ne!(base, compute_env_key(&m));
    }

    #[test]
    fn env_key_config_and_mqtt_asset_changes_key() {
        let base = compute_env_key(&sample_inputs());
        let mut a = sample_inputs();
        a.config_files[0].1 = bytes("prod");
        assert_ne!(base, compute_env_key(&a));
        let mut b = sample_inputs();
        b.mqtt_assets[0].1 = bytes("cert2");
        assert_ne!(base, compute_env_key(&b));
    }

    #[test]
    fn env_key_test_fixture_bytes_change_key() {
        let base = compute_env_key(&sample_inputs());
        let mut m = sample_inputs();
        m.test_fixtures[0].1 = bytes("golden2");
        assert_ne!(base, compute_env_key(&m));
    }

    #[test]
    fn read_dir_files_recursive_walks_subdirs() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir_all(root.join("sub/deep")).unwrap();
        std::fs::write(root.join("top.txt"), b"t").unwrap();
        std::fs::write(root.join("sub/mid.txt"), b"m").unwrap();
        std::fs::write(root.join("sub/deep/leaf.txt"), b"l").unwrap();
        let files = read_dir_files_recursive(root);
        let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["sub/deep/leaf.txt", "sub/mid.txt", "top.txt"]);
    }

    #[test]
    fn read_dir_files_recursive_missing_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read_dir_files_recursive(&tmp.path().join("nope")).is_empty());
    }

    #[test]
    fn env_key_stable_for_same_inputs() {
        assert_eq!(
            compute_env_key(&sample_inputs()),
            compute_env_key(&sample_inputs())
        );
    }

    #[test]
    fn meta_hit_matches_and_misses() {
        let e = MetaEntry {
            size: 10,
            mtime_secs: 100,
            mtime_nanos: 5,
            content_hash: "abc".into(),
        };
        assert_eq!(meta_hit(Some(&e), 10, 100, 5), Some("abc"));
        assert_eq!(meta_hit(Some(&e), 11, 100, 5), None); // size changed
        assert_eq!(meta_hit(Some(&e), 10, 101, 5), None); // mtime secs changed
        assert_eq!(meta_hit(Some(&e), 10, 100, 6), None); // mtime nanos changed
        assert_eq!(meta_hit(None, 10, 100, 5), None);
    }

    #[test]
    fn cache_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("sub").join("cache.json");
        let mut c = Cache::default();
        c.passed.insert(
            "brenn-lib".into(),
            PassRecord {
                content_hash: "h1".into(),
                env_key: "e1".into(),
            },
        );
        c.meta.insert(
            "/p/brenn-lib".into(),
            MetaEntry {
                size: 5,
                mtime_secs: 1,
                mtime_nanos: 2,
                content_hash: "h1".into(),
            },
        );
        c.save_atomic(&path);
        let loaded = Cache::load(&path);
        assert_eq!(loaded.passed, c.passed);
        assert_eq!(loaded.meta, c.meta);
    }

    #[test]
    fn cache_missing_file_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let c = Cache::load(&tmp.path().join("nope.json"));
        assert!(c.passed.is_empty() && c.meta.is_empty());
    }

    #[test]
    fn cache_unparseable_is_empty() {
        let c = Cache::parse(b"not json at all {{{");
        assert_eq!(c.schema_version, SCHEMA_VERSION);
        assert!(c.passed.is_empty());
    }

    #[test]
    fn cache_wrong_schema_is_empty() {
        let json =
            br#"{"schema_version": 999, "passed": {"x": {"content_hash":"h","env_key":"e"}}}"#;
        let c = Cache::parse(json);
        assert_eq!(c.schema_version, SCHEMA_VERSION);
        assert!(c.passed.is_empty());
    }

    fn bin(id: &str) -> BinaryInfo {
        BinaryInfo {
            binary_id: id.into(),
            path: PathBuf::from(format!("/deps/{id}")),
            test_count: 1,
        }
    }

    #[test]
    fn select_stale_picks_only_changed() {
        let binaries = vec![bin("a"), bin("b"), bin("c")];
        let mut hashes = BTreeMap::new();
        hashes.insert("a".to_string(), "ha".to_string());
        hashes.insert("b".to_string(), "hb".to_string());
        hashes.insert("c".to_string(), "hc".to_string());

        let mut cache = Cache::default();
        // a: recorded pass with matching hash+env → skip.
        cache.passed.insert(
            "a".into(),
            PassRecord {
                content_hash: "ha".into(),
                env_key: "E".into(),
            },
        );
        // b: recorded pass but stale hash → run.
        cache.passed.insert(
            "b".into(),
            PassRecord {
                content_hash: "OLD".into(),
                env_key: "E".into(),
            },
        );
        // c: no record → run.

        let stale = select_stale(&binaries, &hashes, "E", &cache);
        let ids: Vec<&str> = stale.iter().map(|b| b.binary_id.as_str()).collect();
        assert_eq!(ids, vec!["b", "c"]);
    }

    #[test]
    fn select_stale_env_key_mismatch_reruns_all() {
        let binaries = vec![bin("a")];
        let mut hashes = BTreeMap::new();
        hashes.insert("a".to_string(), "ha".to_string());
        let mut cache = Cache::default();
        cache.passed.insert(
            "a".into(),
            PassRecord {
                content_hash: "ha".into(),
                env_key: "OLDENV".into(),
            },
        );
        let stale = select_stale(&binaries, &hashes, "NEWENV", &cache);
        assert_eq!(stale.len(), 1);
    }

    #[test]
    fn build_filterset_shapes() {
        assert_eq!(build_filterset(&[]), "none()");
        assert_eq!(build_filterset(&["a".into()]), "binary_id(=a)");
        assert_eq!(
            build_filterset(&["a".into(), "b".into()]),
            "binary_id(=a) | binary_id(=b)"
        );
    }

    const JUNIT_MIXED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="nextest-run" tests="3" failures="1" errors="0">
  <testsuite name="brenn-lib" tests="1" failures="0" errors="0">
    <testcase name="ok_one" classname="brenn-lib"/>
  </testsuite>
  <testsuite name="brenn-lib::mqtt_integration" tests="1" failures="1" errors="0">
    <testcase name="bad" classname="brenn-lib::mqtt_integration">
      <failure message="boom"/>
    </testcase>
  </testsuite>
  <testsuite name="brenn::bin/brenn" tests="1" failures="0" errors="1">
    <testcase name="errored" classname="brenn::bin/brenn"/>
  </testsuite>
</testsuites>"#;

    #[test]
    fn parse_junit_pass_fail_mixed() {
        let results = parse_junit(JUNIT_MIXED);
        let by_id: BTreeMap<&str, bool> = results
            .iter()
            .map(|r| (r.binary_id.as_str(), r.passed))
            .collect();
        assert_eq!(by_id.get("brenn-lib"), Some(&true));
        assert_eq!(by_id.get("brenn-lib::mqtt_integration"), Some(&false));
        assert_eq!(by_id.get("brenn::bin/brenn"), Some(&false)); // errors > 0
    }

    #[test]
    fn parse_junit_all_pass() {
        let xml = r#"<testsuites><testsuite name="x" tests="1" failures="0" errors="0"><testcase name="t"/></testsuite></testsuites>"#;
        let results = parse_junit(xml);
        assert_eq!(
            results,
            vec![SuiteResult {
                binary_id: "x".into(),
                tests: 1,
                passed: true
            }]
        );
    }

    #[test]
    fn parse_junit_reads_test_count() {
        // The `tests` attribute is captured so the caller can detect a suite that
        // was cancelled mid-run (fewer tests than nextest listed).
        let by_id: BTreeMap<String, u64> = parse_junit(JUNIT_MIXED)
            .into_iter()
            .map(|r| (r.binary_id, r.tests))
            .collect();
        assert_eq!(by_id.get("brenn-lib"), Some(&1));
        assert_eq!(by_id.get("brenn-lib::mqtt_integration"), Some(&1));
    }

    #[test]
    fn parse_nextest_list_extracts_binaries_and_counts() {
        // brenn-lib: two testcases, one ignored → expected run count 1.
        // brenn::bin/brenn: no testcases object → count 0.
        let json = r#"{
            "rust-suites": {
                "brenn-lib": {
                    "binary-path": "/deps/brenn_lib-abc",
                    "kind": "lib",
                    "testcases": {
                        "runs": {"ignored": false},
                        "skipped": {"ignored": true}
                    }
                },
                "brenn::bin/brenn": {"binary-path": "/deps/brenn-def", "kind": "bin"}
            }
        }"#;
        let bins = parse_nextest_list(json);
        assert_eq!(bins.len(), 2);
        assert_eq!(bins[0].binary_id, "brenn-lib");
        assert_eq!(bins[0].path, PathBuf::from("/deps/brenn_lib-abc"));
        assert_eq!(bins[0].test_count, 1);
        assert_eq!(bins[1].binary_id, "brenn::bin/brenn");
        assert_eq!(bins[1].test_count, 0);
    }

    #[test]
    fn read_dir_files_filters_by_extension() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::write(root.join("b.wasm"), b"bb").unwrap();
        std::fs::write(root.join("a.wasm"), b"aa").unwrap();
        std::fs::write(root.join("note.txt"), b"skip").unwrap();
        let files = read_dir_files(root, Some("wasm"));
        let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
        // Only .wasm files, sorted by name; the .txt is excluded.
        assert_eq!(names, vec!["a.wasm", "b.wasm"]);
    }

    #[test]
    fn read_dir_files_missing_dir_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read_dir_files(&tmp.path().join("nope"), None).is_empty());
    }

    #[test]
    fn compute_hashes_uses_fast_path_and_rehashes_on_change() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("bin-a");
        std::fs::write(&p, b"original").unwrap();
        let binaries = vec![BinaryInfo {
            binary_id: "a".into(),
            path: p.clone(),
            test_count: 1,
        }];

        // First pass: empty prev_meta → real hash computed, meta captured.
        let (hashes1, meta1) = compute_hashes(&binaries, &BTreeMap::new());
        let real = hashes1.get("a").unwrap().clone();
        assert_eq!(real, content_hash(b"original"));

        // Second pass with a SENTINEL content_hash but matching (size, mtime): the
        // fast path must reuse the stored hash verbatim, never re-reading the file.
        let key = p.to_string_lossy().to_string();
        let mut sentinel_meta = meta1.clone();
        sentinel_meta.get_mut(&key).unwrap().content_hash = "SENTINEL".into();
        let (hashes2, _) = compute_hashes(&binaries, &sentinel_meta);
        assert_eq!(
            hashes2.get("a").map(String::as_str),
            Some("SENTINEL"),
            "unchanged metadata must reuse the stored hash"
        );

        // Mutate the file (new bytes + bumped mtime): metadata no longer matches,
        // so the stored hash is ignored and the real new hash is computed.
        std::fs::write(&p, b"changed contents!!").unwrap();
        filetime_bump(&p);
        let (hashes3, _) = compute_hashes(&binaries, &sentinel_meta);
        assert_eq!(
            hashes3.get("a").map(String::as_str),
            Some(content_hash(b"changed contents!!").as_str()),
            "changed metadata must force a re-hash"
        );
    }

    /// Force a distinct mtime by setting it well into the past, so the metadata
    /// fast path reliably sees a change even on coarse-granularity filesystems.
    fn filetime_bump(path: &Path) {
        use std::time::{Duration, SystemTime};
        let past = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let f = std::fs::File::options().write(true).open(path).unwrap();
        f.set_modified(past).unwrap();
    }
}

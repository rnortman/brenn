//! The parsers against files Bazel itself wrote.
//!
//! `xtask/tests/fixtures/timing/` holds one build event file and one compact
//! execution log per invocation of a scratch workspace, recorded by
//! `record-fixtures.sh` beside them with the pinned Bazel. Each test here says
//! what one of them was recorded to contain. Between them the fixtures hold a
//! non-zero value for every count, duration and flag the reader reads, so a
//! field Bazel renames — which the parser reads as absent, so zero — fails here.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::bep::{self, Bep, TestResult};
use super::execlog::{self, Spawn};
use super::metrics::{BazelMetrics, Metrics, load_metrics};
use super::store::tests::{ID_A, meta_text, write_invocation, write_run};
use super::store::{Invocation, load_run};

const FIXTURES: [&str; 7] = [
    "cold", "warm", "cached", "option", "failing", "cquery", "run",
];

const DISCARD: &str = "WARNING: Build option --define has changed, discarding analysis cache \
                       (this can be expensive, see \
                       https://bazel.build/advanced/performance/iteration-speed).";

fn fixture_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/timing")
        .join(name)
}

fn fixture_bep(name: &str) -> Bep {
    let text = std::fs::read_to_string(fixture_dir(name).join("bep.json")).unwrap();
    bep::parse(&text).unwrap_or_else(|e| panic!("{name}: {e}"))
}

fn fixture_spawns(name: &str) -> Vec<Spawn> {
    let bytes = std::fs::read(fixture_dir(name).join("exec.zst")).unwrap();
    execlog::parse(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"))
}

fn runners(rows: &[(&str, u64)]) -> BTreeMap<String, u64> {
    rows.iter().map(|(n, c)| (n.to_string(), *c)).collect()
}

fn check_test(cached_locally: bool, cached_remotely: bool, duration_ms: u64) -> Vec<TestResult> {
    vec![TestResult {
        label: "//:check_test".into(),
        status: "PASSED".into(),
        cached_locally,
        cached_remotely,
        duration_ms,
    }]
}

/// The fields of a spawn the reader reports.
fn summary(spawns: &[Spawn]) -> Vec<(&str, &str, &str, bool, i32, u64)> {
    spawns
        .iter()
        .map(|s| {
            (
                s.target_label.as_str(),
                s.mnemonic.as_str(),
                s.runner.as_str(),
                s.cache_hit,
                s.exit_code,
                s.total_ns / 1_000_000,
            )
        })
        .collect()
}

#[test]
fn the_fixtures_are_the_pinned_bazels() {
    let pinned =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.bazelversion"))
            .unwrap();
    for name in FIXTURES {
        assert_eq!(
            fixture_bep(name).tool_version,
            pinned.trim(),
            "the {name} fixture was recorded by another Bazel than .bazelversion pins; \
             run xtask/tests/fixtures/timing/record-fixtures.sh"
        );
    }
}

#[test]
fn cold_executes_everything_and_runs_the_test() {
    let bep = fixture_bep("cold");
    assert_eq!(
        bep,
        Bep {
            command: "test".into(),
            tool_version: "9.2.0".into(),
            start_time: "2026-11-23T04:03:55.836Z".into(),
            exit_name: "SUCCESS".into(),
            wall_ms: 4048,
            analysis_ms: 308,
            execution_ms: 877,
            targets_configured: 845,
            actions_executed: 13,
            runner_counts: runners(&[("internal", 11), ("linux-sandbox", 3), ("total", 13)]),
            action_cache_hits: 0,
            action_cache_misses: 13,
            tests: check_test(false, false, 420),
            discards: vec![],
        }
    );
    let spawns = fixture_spawns("cold");
    assert_eq!(
        summary(&spawns),
        [
            ("//:slow", "Genrule", "linux-sandbox", false, 0, 308),
            (
                "//:check_test",
                "TestRunner",
                "linux-sandbox",
                false,
                0,
                420
            ),
            ("//:check_test", "TestRunner", "linux-sandbox", false, 0, 10),
        ]
    );
}

#[test]
fn warm_executes_nothing_and_the_test_is_cached_locally() {
    let bep = fixture_bep("warm");
    assert_eq!(bep.wall_ms, 176);
    assert_eq!((bep.analysis_ms, bep.execution_ms), (31, 39));
    assert_eq!(bep.targets_configured, 0);
    assert_eq!(bep.runner_counts, runners(&[("internal", 1), ("total", 1)]));
    assert_eq!((bep.action_cache_hits, bep.action_cache_misses), (1, 1));
    assert_eq!(bep.tests, check_test(true, false, 420));
    assert_eq!(fixture_spawns("warm"), []);
}

#[test]
fn cached_serves_every_spawn_and_the_test_from_the_disk_cache() {
    let bep = fixture_bep("cached");
    assert_eq!(
        bep.runner_counts,
        runners(&[("disk cache hit", 3), ("internal", 11), ("total", 13)])
    );
    assert_eq!(bep.tests, check_test(false, true, 420));
    assert_eq!(bep.discards, Vec::<String>::new());
    let spawns = fixture_spawns("cached");
    assert_eq!(
        summary(&spawns),
        [
            ("//:slow", "Genrule", "disk cache hit", true, 0, 14),
            ("//:check_test", "TestRunner", "disk cache hit", true, 0, 2),
            ("//:check_test", "TestRunner", "disk cache hit", true, 0, 1),
        ]
    );
}

#[test]
fn option_reports_the_discard_and_mixes_executed_and_cached_spawns() {
    let bep = fixture_bep("option");
    assert_eq!(bep.discards, [DISCARD]);
    assert_eq!(bep.targets_configured, 796);
    assert_eq!((bep.action_cache_hits, bep.action_cache_misses), (9, 4));
    assert_eq!(
        bep.runner_counts,
        runners(&[
            ("disk cache hit", 1),
            ("internal", 2),
            ("linux-sandbox", 2),
            ("total", 4)
        ])
    );
    assert_eq!(bep.tests, check_test(false, false, 419));
    assert_eq!(
        summary(&fixture_spawns("option")),
        [
            ("//:slow", "Genrule", "linux-sandbox", false, 0, 308),
            (
                "//:check_test",
                "TestRunner",
                "linux-sandbox",
                false,
                0,
                419
            ),
            ("//:check_test", "TestRunner", "disk cache hit", true, 0, 2),
        ]
    );
}

#[test]
fn failing_is_complete_and_names_the_failed_spawn() {
    let bep = fixture_bep("failing");
    assert_eq!(bep.command, "build");
    assert_eq!(bep.exit_name, "BUILD_FAILURE");
    assert_eq!(bep.targets_configured, 1);
    // Bazel's runner counts leave the failed spawn out; the execution log has it.
    assert_eq!(bep.runner_counts, runners(&[("internal", 2), ("total", 2)]));
    assert_eq!(
        summary(&fixture_spawns("failing")),
        [("//:fails", "Genrule", "linux-sandbox", false, 3, 106)]
    );
}

#[test]
fn cquery_is_complete_and_executes_nothing() {
    let bep = fixture_bep("cquery");
    assert_eq!(bep.command, "cquery");
    assert_eq!(bep.wall_ms, 100);
    assert_eq!(bep.execution_ms, 0);
    assert_eq!(bep.actions_executed, 0);
    assert_eq!(bep.runner_counts, runners(&[("total", 0)]));
    assert_eq!(fixture_spawns("cquery"), []);
}

#[test]
fn run_is_complete_with_the_fields_of_a_build() {
    let bep = fixture_bep("run");
    assert_eq!(bep.command, "run");
    assert_eq!(bep.exit_name, "SUCCESS");
    assert_eq!((bep.wall_ms, bep.analysis_ms, bep.execution_ms), (46, 2, 3));
    assert_eq!(bep.actions_executed, 1);
    assert_eq!((bep.action_cache_hits, bep.action_cache_misses), (5, 1));
    assert_eq!(fixture_spawns("run"), []);
}

/// A field's name and whether a build event file holds a non-zero value in it.
type BepCheck = (&'static str, fn(&Bep) -> bool);

#[test]
fn every_field_the_reader_reads_is_non_zero_in_some_fixture() {
    let beps: Vec<Bep> = FIXTURES.iter().map(|n| fixture_bep(n)).collect();
    let spawns: Vec<Spawn> = FIXTURES.iter().flat_map(|n| fixture_spawns(n)).collect();
    let tests: Vec<&TestResult> = beps.iter().flat_map(|b| &b.tests).collect();
    let bep_fields: [BepCheck; 10] = [
        ("wall_ms", |b| b.wall_ms > 0),
        ("analysis_ms", |b| b.analysis_ms > 0),
        ("execution_ms", |b| b.execution_ms > 0),
        ("targets_configured", |b| b.targets_configured > 0),
        ("actions_executed", |b| b.actions_executed > 0),
        ("runner_counts", |b| {
            b.runner_counts.values().any(|c| *c > 0)
        }),
        ("action_cache_hits", |b| b.action_cache_hits > 0),
        ("action_cache_misses", |b| b.action_cache_misses > 0),
        ("tests", |b| !b.tests.is_empty()),
        ("discards", |b| !b.discards.is_empty()),
    ];
    for (name, set) in bep_fields {
        assert!(beps.iter().any(set), "no fixture has a non-zero {name}");
    }
    assert!(tests.iter().any(|t| t.cached_locally));
    assert!(tests.iter().any(|t| t.cached_remotely));
    assert!(tests.iter().all(|t| t.duration_ms > 0));
    assert!(spawns.iter().any(|s| s.cache_hit));
    assert!(spawns.iter().any(|s| s.exit_code != 0));
    assert!(spawns.iter().all(|s| s.total_ns > 0));
    assert!(
        spawns
            .iter()
            .all(|s| !s.target_label.is_empty() && !s.mnemonic.is_empty())
    );
    assert!(spawns.iter().all(|s| !s.runner.is_empty()));
}

/// One complete recorded command in `run` with the named fixture's two files,
/// its directory named for the `started_ns` in `meta`.
pub(super) fn add_fixture(run: &Path, fixture: &str, meta: &str, argv: &[&str]) -> PathBuf {
    let start = meta
        .lines()
        .find_map(|l| l.strip_prefix("started_ns "))
        .and_then(|v| v.parse().ok())
        .expect("meta has a started_ns");
    let inv = write_invocation(run, meta, start, argv);
    for file in ["bep.json", "exec.zst"] {
        std::fs::copy(fixture_dir(fixture).join(file), inv.join(file)).unwrap();
    }
    inv
}

// The store's rules for Bazel's files, over records built from the fixtures.

/// A run holding one recorded command with the given exit status, and the
/// named fixture's files copied into it (`None` for no files).
fn store_with(fixture: Option<&str>, kind: &str, exit: i32) -> (tempfile::TempDir, PathBuf) {
    let store = tempfile::tempdir().unwrap();
    let run = write_run(store.path(), ID_A);
    let inv = write_invocation(
        &run,
        &meta_text("bazel-check", kind, 1000, 2000, exit),
        1000,
        &["bazel", "test", "//..."],
    );
    if let Some(name) = fixture {
        for file in ["bep.json", "exec.zst"] {
            std::fs::copy(fixture_dir(name).join(file), inv.join(file)).unwrap();
        }
    }
    (store, inv)
}

fn metrics_of(store: &Path) -> Result<Metrics, super::store::RecordError> {
    let run = load_run(store, ID_A).unwrap();
    let Invocation::Complete { dir, meta, .. } = &run.invocations[0] else {
        panic!("complete");
    };
    load_metrics(&run.dir, dir, meta)
}

#[test]
fn a_complete_bazel_record_reads_both_files() {
    let (store, _) = store_with(Some("cold"), "bazel", 0);
    assert_eq!(
        metrics_of(store.path()).unwrap(),
        Metrics::Read(Box::new(BazelMetrics {
            bep: fixture_bep("cold"),
            spawns: fixture_spawns("cold"),
        }))
    );
}

#[test]
fn a_failed_command_with_complete_files_is_read_as_usual() {
    let (store, _) = store_with(Some("failing"), "bazel", 1);
    let Metrics::Read(metrics) = metrics_of(store.path()).unwrap() else {
        panic!("read");
    };
    assert_eq!(metrics.spawns[0].exit_code, 3);
}

#[test]
fn a_timed_command_has_no_bazel_files() {
    let (store, _) = store_with(None, "command", 0);
    assert_eq!(metrics_of(store.path()).unwrap(), Metrics::NotBazel);
}

#[test]
fn a_failed_command_without_files_or_with_cut_off_files_has_no_bazel_metrics() {
    let (store, _) = store_with(None, "bazel", 2);
    assert_eq!(metrics_of(store.path()).unwrap(), Metrics::Unavailable);

    let (store, inv) = store_with(Some("failing"), "bazel", 1);
    let text = std::fs::read_to_string(inv.join("bep.json")).unwrap();
    std::fs::write(inv.join("bep.json"), &text[..text.len() / 2]).unwrap();
    assert_eq!(metrics_of(store.path()).unwrap(), Metrics::Unavailable);

    let (store, inv) = store_with(Some("failing"), "bazel", 1);
    std::fs::remove_file(inv.join("exec.zst")).unwrap();
    assert_eq!(metrics_of(store.path()).unwrap(), Metrics::Unavailable);
}

#[test]
fn a_passed_command_missing_or_cut_off_files_is_refused_by_name() {
    let (store, inv) = store_with(None, "bazel", 0);
    let err = metrics_of(store.path()).unwrap_err();
    assert_eq!(err.path, inv.join("bep.json"));
    assert!(err.run_dir.is_some());

    let (store, inv) = store_with(Some("cold"), "bazel", 0);
    let text = std::fs::read_to_string(inv.join("bep.json")).unwrap();
    std::fs::write(inv.join("bep.json"), &text[..text.len() - 5]).unwrap();
    let err = metrics_of(store.path()).unwrap_err();
    assert_eq!(err.path, inv.join("bep.json"));
    assert!(err.message.contains("incomplete"), "{err}");

    let (store, inv) = store_with(Some("cold"), "bazel", 0);
    std::fs::remove_file(inv.join("exec.zst")).unwrap();
    assert_eq!(
        metrics_of(store.path()).unwrap_err().path,
        inv.join("exec.zst")
    );
}

#[test]
fn an_undecodable_execution_log_is_refused() {
    let (store, inv) = store_with(Some("cold"), "bazel", 0);
    std::fs::write(inv.join("exec.zst"), b"not a zstd frame").unwrap();
    assert_eq!(
        metrics_of(store.path()).unwrap_err().path,
        inv.join("exec.zst")
    );
}

#[test]
fn a_malformed_file_is_refused_whatever_the_exit_status() {
    let (store, inv) = store_with(Some("failing"), "bazel", 1);
    let text = std::fs::read_to_string(inv.join("bep.json")).unwrap();
    std::fs::write(inv.join("bep.json"), format!("{{oops\n{text}")).unwrap();
    let err = metrics_of(store.path()).unwrap_err();
    assert_eq!(err.path, inv.join("bep.json"));
    assert!(err.message.contains("malformed"), "{err}");
}

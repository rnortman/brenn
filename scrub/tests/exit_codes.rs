//! The exit-code contract between this binary and the hook shims.
//!
//! 2 blocks a PreToolUse write and feeds stderr back to the agent; 1 is a gate
//! failure or a setup problem; 0 is clean. A refactor that swaps 2 for 1 in
//! hook mode silently turns "block the write" into "hook errored" and lets
//! writes through, so the codes are asserted over the real binary.
//!
//! The hook cases that exercise the size cap and version probe target a gated
//! temp-repo destination, since only a gated destination reaches those steps;
//! broader rule behavior lives in `rules.rs`. An ungated destination exits 0
//! before any scanner runs, and a destination that cannot be resolved exits 2
//! (block, not the 101 a bare panic would give); both are pinned here too.

mod common;

use common::{Output, PINNED_VERSION, gated_repo, stub_gitleaks};

/// Mirrors `hook::SIZE_CAP_BYTES`; the crate is a binary, so the constant is
/// not importable here.
const SIZE_CAP_BYTES: usize = 1024 * 1024;

fn run(args: &[&str], stdin: &str) -> Output {
    run_with_path(args, stdin, None)
}

fn run_with_path(args: &[&str], stdin: &str, path_prefix: Option<&std::path::Path>) -> Output {
    common::run(args, stdin, &[], path_prefix, None)
}

#[test]
fn unknown_tool_blocks_rather_than_passing_unscanned() {
    let payload = r#"{"tool_name":"SomeFutureTool","tool_input":{}}"#;
    let out = run(&["hook"], payload);
    assert_eq!(out.code, Some(2), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("hook misconfigured"),
        "stderr: {}",
        out.stderr
    );
}

/// A panic exits 101, which PreToolUse treats as a non-blocking error -- the
/// write would land unscanned. Internal failures must surface as a block.
#[test]
fn malformed_hook_input_blocks_instead_of_exiting_101() {
    let out = run(&["hook"], "this is not json");
    assert_eq!(
        out.code,
        Some(2),
        "internal failure must block, not fail open; stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("refusing to pass this write unscanned"),
        "stderr: {}",
        out.stderr
    );
}

/// Exit 1 here would mean "warned, write landed unscanned": one stray argument
/// in the PreToolUse command string would turn the write-time layer off.
#[test]
fn a_malformed_hook_invocation_blocks_rather_than_warning() {
    let out = run(&["hook", "--x"], "");
    assert_eq!(
        out.code,
        Some(2),
        "a bad hook invocation must block, not fail open; stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("usage: brenn-scrub hook"),
        "{}",
        out.stderr
    );
}

#[test]
fn no_arguments_prints_usage_and_fails() {
    let out = run(&[], "");
    assert_eq!(out.code, Some(1));
    assert!(out.stderr.contains("usage: brenn-scrub"), "{}", out.stderr);
}

#[test]
fn a_mistyped_warn_only_flag_is_rejected_rather_than_enforcing() {
    let out = run(&["range", "--warnonly"], "");
    assert_eq!(
        out.code,
        Some(1),
        "a typo must not silently run the push gate enforcing"
    );
    assert!(out.stderr.contains("usage: brenn-scrub"), "{}", out.stderr);
}

#[test]
fn a_stray_tree_flag_is_rejected_rather_than_scanning_nothing() {
    let out = run(&["tree", "--anything"], "");
    assert_eq!(
        out.code,
        Some(1),
        "a flag read as a pathspec would match no files and report clean"
    );
    assert!(out.stderr.contains("usage: brenn-scrub"), "{}", out.stderr);
}

/// The trivial gated-repo config for cases that only need a destination to read
/// as gated (so it reaches the version probe, size cap, and scan) without
/// matching any rule.
const TRIVIAL_CONFIG: &str = "title = \"g\"\n";

fn write_payload(file_path: &std::path::Path, body: &str) -> String {
    serde_json::json!({
        "tool_name": "Write",
        "tool_input": {"file_path": file_path, "content": body}
    })
    .to_string()
}

/// The cap is the one deliberate fail-open in the write-time layer, so the
/// comparison itself is asserted at the boundary. Inverted, it would skip the
/// scan on *every* write -- the layer silently off, with the message test
/// still green because the message is never what broke.
#[test]
fn added_text_over_the_size_cap_is_skipped_with_a_warning() {
    let stub = stub_gitleaks(PINNED_VERSION);
    let repo = gated_repo(TRIVIAL_CONFIG);
    let out = run_with_path(
        &["hook"],
        &write_payload(&repo.path().join("a.rs"), &"x".repeat(SIZE_CAP_BYTES + 1)),
        Some(stub.path()),
    );
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("exceeds the") && out.stderr.contains("byte cap"),
        "stderr: {}",
        out.stderr
    );
}

#[test]
fn added_text_at_the_size_cap_still_reaches_the_scan() {
    let stub = stub_gitleaks(PINNED_VERSION);
    let repo = gated_repo(TRIVIAL_CONFIG);
    let out = run_with_path(
        &["hook"],
        &write_payload(&repo.path().join("a.rs"), &"x".repeat(SIZE_CAP_BYTES)),
        Some(stub.path()),
    );
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        !out.stderr.contains("byte cap"),
        "text at exactly the cap must not be skipped; stderr: {}",
        out.stderr
    );
}

/// The asymmetry is load-bearing: an unpinned engine must never decide a gate,
/// but must also never block an author mid-edit over a setup problem.
#[test]
fn an_unpinned_gitleaks_warns_in_hook_mode_but_fails_the_gates() {
    let stub = stub_gitleaks("0.0.1-not-the-pin");
    let repo = gated_repo(TRIVIAL_CONFIG);

    let out = run_with_path(
        &["hook"],
        &write_payload(&repo.path().join("a.rs"), "fn main() {}"),
        Some(stub.path()),
    );
    assert_eq!(
        out.code,
        Some(0),
        "a version mismatch must not block a write; stderr: {}",
        out.stderr
    );
    assert!(out.stderr.contains("version mismatch"), "{}", out.stderr);

    let out = run_with_path(&["tree"], "", Some(stub.path()));
    assert_eq!(
        out.code,
        Some(1),
        "a gate must refuse to run against an unvalidated engine; stderr: {}",
        out.stderr
    );
    assert!(out.stderr.contains("version mismatch"), "{}", out.stderr);
}

/// An ungated destination passes at exit 0 before any scanner runs -- so a
/// scratch write never depends on gitleaks being installed. No stub is on PATH
/// here: reaching the version probe at all would be the regression.
#[test]
fn an_ungated_destination_passes_before_the_scanner() {
    let scratch = tempfile::tempdir().expect("temp dir");
    let out = run(
        &["hook"],
        &write_payload(&scratch.path().join("note.rs"), "let x = 1;\n"),
    );
    assert_eq!(
        out.code,
        Some(0),
        "an ungated destination must pass; stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("not inside a gated repo"),
        "the pass must be audited: {}",
        out.stderr
    );
}

/// A destination whose non-existent tail contains `..` resolves to no single
/// true location, so the hook blocks. Exit 2, not the 101 a bare panic yields:
/// a resolution failure must land as a block, never a silent pass.
#[test]
fn an_unresolvable_destination_blocks_instead_of_exiting_101() {
    let scratch = tempfile::tempdir().expect("temp dir");
    let out = run(
        &["hook"],
        &write_payload(&scratch.path().join("nope/../evil.rs"), "let x = 1;\n"),
    );
    assert_eq!(
        out.code,
        Some(2),
        "a resolution failure must block, not fail open; stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("refusing to pass this write unscanned"),
        "stderr: {}",
        out.stderr
    );
}

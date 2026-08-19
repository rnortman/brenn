//! The `check` subcommand, run as the binary an operator runs.
//!
//! `CARGO_MANIFEST_DIR` is package-relative and a test starts in the runfiles
//! root, so the binary is found at the package path beside the fixtures — the
//! same trick the corpus suites use, applied to a declared tool rather than a
//! declared data file.

mod support;

use std::path::PathBuf;
use std::process::{Command, Output};

fn cli() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dsl_cli")
}

fn check(tree: &str, extra: &[&str]) -> Output {
    let root = support::corpus_dir()
        .join("trees")
        .join(tree)
        .join("main.brenn");
    Command::new(cli())
        .arg("check")
        .arg(&root)
        .args(extra)
        .output()
        .unwrap_or_else(|error| panic!("{}: {error}", cli().display()))
}

#[test]
fn a_tree_that_compiles_exits_zero() {
    let output = check("ok", &[]);
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).ends_with("main.brenn: ok\n"));
}

#[test]
fn dump_prints_the_resolved_configuration() {
    let output = check("ok", &["--dump"]);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with("ResolvedConfig {"), "{stdout}");
    assert!(
        stdout.contains("brenn:alice-desk.in.p1.messages"),
        "{stdout}"
    );
    assert!(stdout.contains("notes"), "{stdout}");
}

#[test]
fn a_tree_that_does_not_compile_reports_and_exits_nonzero() {
    let output = check("missing", &[]);
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no module `wiring::deskbar`"), "{stderr}");
}

#[test]
fn an_unreachable_file_is_reported_and_still_compiles() {
    let output = check("orphan", &[]);
    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no `use` reaches"), "{stderr}");
    // Diagnostics on stderr, the payload on stdout: `check --dump > file` has
    // to yield the dump alone.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.ends_with("main.brenn: ok\n"), "{stdout}");
    assert!(!stdout.contains("no `use` reaches"), "{stdout}");
}

#[test]
fn a_failing_tree_writes_nothing_to_stdout() {
    let output = check("missing", &[]);
    assert!(!output.status.success(), "{output:?}");
    assert_eq!(String::from_utf8_lossy(&output.stdout), "");
}

#[test]
fn a_secondary_location_prints_as_file_line_column() {
    let output = check("collide", &[]);
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let secondary = stderr
        .lines()
        .find(|line| line.starts_with("  "))
        .unwrap_or_else(|| panic!("a related line: {stderr}"));
    assert!(secondary.contains("main.brenn:1:5: "), "{secondary}");
}

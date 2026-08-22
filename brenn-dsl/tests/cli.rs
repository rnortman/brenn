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
fn dump_prints_the_derived_configuration() {
    let output = check("ok", &["--dump"]);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with("DerivedConfig {"), "{stdout}");
    assert!(
        stdout.contains("brenn:alice-desk.in.p1.messages"),
        "{stdout}"
    );
    assert!(stdout.contains("notes"), "{stdout}");
    // The derived payload and not merely the resolved one it wraps: the tree's
    // declarations are disk-backed, so the dump carries their identities.
    assert!(
        stdout.contains("0d178089-b13f-5a0e-8bec-0745e0475d78"),
        "{stdout}"
    );
}

#[test]
fn dump_prints_the_expanded_grants() {
    // `ephemeral_subscribe` is written nowhere in the tree: the document grants
    // `subscribe`, and the token per scheme the plane reaches is derivation's.
    let output = check("authority", &["--dump"]);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ephemeral_subscribe"), "{stdout}");
}

#[test]
fn a_tree_refused_in_derivation_reports_and_exits_nonzero() {
    let output = check("derive-error", &[]);
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("names no system-minted family"), "{stderr}");
}

#[test]
fn a_tree_that_does_not_compile_reports_and_exits_nonzero() {
    let output = check("missing", &[]);
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no module `wiring::deskbar`"), "{stderr}");
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

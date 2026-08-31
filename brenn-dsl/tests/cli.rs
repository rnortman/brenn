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
fn a_tree_of_packaged_modules_compiles_when_the_flag_names_their_root() {
    let modules = support::corpus_dir()
        .join("trees")
        .join("pkg-ok")
        .join("modules");
    let output = check("pkg-ok", &["--modules", &modules.display().to_string()]);
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn the_same_tree_without_the_flag_names_it() {
    // The module root is an environment fact, so a document that reaches for
    // one and is given none is refused rather than defaulted somewhere.
    let output = check("pkg-ok", &[]);
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("pass `--modules <dir>`"), "{stderr}");
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

// ── scaffold and grant-parity ────────────────────────────────────────────────

/// A private directory to write fixture inputs and generator output into.
///
/// `TEST_TMPDIR` is the runner's own scratch, cleaned up for us; the fallback
/// is for a run outside one.
fn scratch(name: &str) -> PathBuf {
    let base = std::env::var_os("TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(name);
    std::fs::create_dir_all(&base).unwrap_or_else(|error| panic!("{}: {error}", base.display()));
    base
}

fn write(dir: &std::path::Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    path
}

fn run(args: &[&str]) -> Output {
    Command::new(cli())
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("{}: {error}", cli().display()))
}

/// A specification stating `requires` and nothing else that matters here.
fn spec_text(requires: &str) -> String {
    format!(
        "component Fixture {{\n  abi = processor;\n  requires = [{requires}];\n  in orders;\n}}\n"
    )
}

#[test]
fn grant_parity_exits_zero_when_the_two_agree() {
    let dir = scratch("grant-parity-ok");
    let spec = write(&dir, "spec.brenn", &spec_text("ports"));
    let imports = write(
        &dir,
        "imports.txt",
        "brenn:processor/ports@0.1.0\nbrenn:processor/types@0.1.0\n",
    );
    let output = run(&[
        "grant-parity",
        "--spec",
        &spec.display().to_string(),
        "--imports",
        &imports.display().to_string(),
    ]);
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn grant_parity_exits_nonzero_on_drift_and_renders_the_diagnostic() {
    let dir = scratch("grant-parity-drift");
    let spec = write(&dir, "spec.brenn", &spec_text("ports, store"));
    let imports = write(
        &dir,
        "imports.txt",
        "brenn:processor/ports@0.1.0\nbrenn:processor/types@0.1.0\n",
    );
    let output = run(&[
        "grant-parity",
        "--spec",
        &spec.display().to_string(),
        "--imports",
        &imports.display().to_string(),
    ]);
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("requires `store`"), "{stderr}");
    assert!(stderr.contains("spec.brenn:"), "{stderr}");
}

/// An empty import list is the shape a broken scrape produces, and it is the
/// one thing that would make every per-package parity target pass while
/// comparing against nothing. It is refused by name.
#[test]
fn grant_parity_refuses_an_empty_import_list() {
    let dir = scratch("grant-parity-empty");
    let spec = write(&dir, "spec.brenn", &spec_text("ports"));
    let imports = write(&dir, "imports.txt", "\n  \n");
    let output = run(&[
        "grant-parity",
        "--spec",
        &spec.display().to_string(),
        "--imports",
        &imports.display().to_string(),
    ]);
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no imports listed"), "{stderr}");
    assert!(stderr.contains("imports.txt"), "{stderr}");
}

#[test]
fn grant_parity_names_an_import_list_it_cannot_read() {
    let dir = scratch("grant-parity-unreadable");
    let spec = write(&dir, "spec.brenn", &spec_text("ports"));
    let missing = dir.join("no-such-imports.txt");
    let output = run(&[
        "grant-parity",
        "--spec",
        &spec.display().to_string(),
        "--imports",
        &missing.display().to_string(),
    ]);
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no-such-imports.txt"), "{stderr}");
}

/// The two-class refusal, and the flag that resolves it, both through the
/// binary — including the `-o` write, which is the only thing that turns a
/// generated module into a file the build compiles.
#[test]
fn scaffold_refuses_a_two_class_specification_until_class_names_one() {
    let dir = scratch("scaffold-two-class");
    let spec = write(
        &dir,
        "spec.brenn",
        &format!("{}{}", spec_text("ports"), {
            "component Other {\n  abi = processor;\n  requires = [ports];\n  out results;\n}\n"
        }),
    );
    let out = dir.join("spec.rs");

    let refused = run(&[
        "scaffold",
        &spec.display().to_string(),
        "-o",
        &out.display().to_string(),
    ]);
    assert!(!refused.status.success(), "{refused:?}");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("--class <Name>"), "{stderr}");
    assert!(!out.exists(), "a refused generation writes no module");

    let generated = run(&[
        "scaffold",
        "--class",
        "Other",
        &spec.display().to_string(),
        "-o",
        &out.display().to_string(),
    ]);
    assert!(generated.status.success(), "{generated:?}");
    let module = std::fs::read_to_string(&out).expect("the module was written");
    assert!(
        module.starts_with("// Generated from spec.brenn — do not edit.\n"),
        "{module}"
    );
    assert!(
        module.contains("pub const RESULTS: &str = \"results\";"),
        "{module}"
    );
}

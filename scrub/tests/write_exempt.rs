//! The write-destination exemption, driven through the real binary.
//!
//! These prove the ordering and the fail-closed defaults that the unit tests
//! cannot: the exemption is consulted before repo/config resolution and the
//! gitleaks probe, a matched write exits 0, and every degenerate configuration
//! blocks. The exempt path needs no gitleaks -- it exits before the version
//! probe -- so most cases run without a stub.
//!
//! Both scrub env vars are cleared at the start of every run (by the shared
//! `common::run`). The harness inherits the parent environment, and the
//! operator's shell exports both `BRENN_SCRUB_WRITE_EXEMPT` and
//! `BRENN_SCRUB_DENYLIST`; without the clear, the "env unset" assertions would
//! test the live overlay instead of the unset branch, and a broken real
//! exemption file would fail unrelated tests.

mod common;

use common::{PINNED_VERSION, run, stub_gitleaks};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::Command;

fn write_payload(file_path: &Path, cwd: &Path, body: &str) -> String {
    json!({
        "tool_name": "Write",
        "tool_input": {"file_path": file_path, "content": body},
        "cwd": cwd,
    })
    .to_string()
}

fn edit_payload(file_path: &Path, cwd: &Path, body: &str) -> String {
    json!({
        "tool_name": "Edit",
        "tool_input": {"file_path": file_path, "new_string": body},
        "cwd": cwd,
    })
    .to_string()
}

/// A deep, non-repo directory (three-plus components, clearing the breadth
/// tripwire) usable as an exempt root. Returns (guard, canonical dir).
fn deep_dir() -> (tempfile::TempDir, PathBuf) {
    let d = tempfile::tempdir().expect("temp dir");
    let deep = d.path().join("a/b/c");
    std::fs::create_dir_all(&deep).expect("mkdir");
    let canonical = std::fs::canonicalize(&deep).expect("canonicalize");
    (d, canonical)
}

/// Write an exemption file listing `roots`, returning its path (kept alive by
/// the caller's guard).
fn exempt_file(dir: &Path, roots: &[&Path]) -> PathBuf {
    let listed = roots
        .iter()
        .map(|p| format!("  \"{}\",", p.display()))
        .collect::<Vec<_>>()
        .join("\n");
    let file = dir.join("exempt.toml");
    std::fs::write(&file, format!("paths = [\n{listed}\n]\n")).expect("write exempt file");
    file
}

fn git_init(dir: &Path) {
    let out = Command::new("git")
        .current_dir(dir)
        .args(["init", "-q", "."])
        .output()
        .expect("git init");
    assert!(out.status.success());
}

/// A non-repo directory to use as the payload cwd, so the *unexempt* flow
/// blocks on repo resolution rather than depending on the harness's own cwd.
fn non_repo_cwd() -> tempfile::TempDir {
    tempfile::tempdir().expect("temp dir")
}

// ---- the ordering proof: exempt precedes repo/config resolution ------------

#[test]
fn a_write_under_an_exempt_root_exits_zero_with_an_audit_line() {
    let (_root_guard, root) = deep_dir();
    let cfg = tempfile::tempdir().unwrap();
    let file = exempt_file(cfg.path(), &[&root]);
    let cwd = non_repo_cwd();
    let dest = root.join("sub/new.rs");

    let out = run(
        &["hook"],
        &write_payload(&dest, cwd.path(), "fn main() {}"),
        &[("BRENN_SCRUB_WRITE_EXEMPT", file.to_str().unwrap())],
        None,
        None,
    );

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("exempt from the write-time scrub"),
        "stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains(dest.to_str().unwrap()),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr.contains(root.to_str().unwrap()),
        "{}",
        out.stderr
    );
    assert!(
        out.stderr.contains(file.to_str().unwrap()),
        "{}",
        out.stderr
    );
}

#[test]
fn the_same_write_blocks_when_the_env_var_is_unset() {
    // The exact invocation of the exempt case, minus the env var: with a valid
    // gitleaks the flow reaches repo resolution, which panics on a non-repo cwd
    // and is translated to a block. This is what the exemption steps ahead of.
    let stub = stub_gitleaks(PINNED_VERSION);
    let (_root_guard, root) = deep_dir();
    let cwd = non_repo_cwd();
    let dest = root.join("sub/new.rs");

    let out = run(
        &["hook"],
        &write_payload(&dest, cwd.path(), "fn main() {}"),
        &[],
        Some(stub.path()),
        None,
    );

    assert_eq!(
        out.code,
        Some(2),
        "without the exemption this must block on repo resolution; stderr: {}",
        out.stderr
    );
}

// ---- degenerate configs block every write, exempt destination or not -------

#[test]
fn an_empty_paths_file_blocks_a_non_exempt_write() {
    let cfg = tempfile::tempdir().unwrap();
    let file = cfg.path().join("exempt.toml");
    std::fs::write(&file, "paths = []\n").unwrap();
    let cwd = non_repo_cwd();
    // A destination the file could never exempt: the misconfig must block first.
    let dest = PathBuf::from("/tmp/not-under-any-root.rs");

    let out = run(
        &["hook"],
        &write_payload(&dest, cwd.path(), "fn main() {}"),
        &[("BRENN_SCRUB_WRITE_EXEMPT", file.to_str().unwrap())],
        None,
        None,
    );

    assert_eq!(out.code, Some(2), "stderr: {}", out.stderr);
    assert!(out.stderr.contains("is empty"), "stderr: {}", out.stderr);
}

#[test]
fn a_nonexistent_exemption_file_blocks_a_non_exempt_write() {
    let cwd = non_repo_cwd();
    let dest = PathBuf::from("/tmp/not-under-any-root.rs");

    let out = run(
        &["hook"],
        &write_payload(&dest, cwd.path(), "fn main() {}"),
        &[("BRENN_SCRUB_WRITE_EXEMPT", "/no/such/exemption/file.toml")],
        None,
        None,
    );

    assert_eq!(out.code, Some(2), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("does not exist"),
        "stderr: {}",
        out.stderr
    );
}

#[test]
fn an_entry_inside_a_gated_repo_blocks() {
    let d = tempfile::tempdir().unwrap();
    let repo = d.path().join("x/y/repo");
    std::fs::create_dir_all(&repo).unwrap();
    git_init(&repo);
    std::fs::write(repo.join(".gitleaks.toml"), "title = \"g\"\n").unwrap();
    let canonical = std::fs::canonicalize(&repo).unwrap();
    let cfg = tempfile::tempdir().unwrap();
    let file = exempt_file(cfg.path(), &[&canonical]);
    let cwd = non_repo_cwd();
    let dest = canonical.join("f.rs");

    let out = run(
        &["hook"],
        &write_payload(&dest, cwd.path(), "fn main() {}"),
        &[("BRENN_SCRUB_WRITE_EXEMPT", file.to_str().unwrap())],
        None,
        None,
    );

    assert_eq!(out.code, Some(2), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("lies inside gated repo"),
        "stderr: {}",
        out.stderr
    );
}

#[test]
fn an_empty_env_value_blocks_rather_than_reading_as_unset() {
    // `BRENN_SCRUB_WRITE_EXEMPT=""` (the classic unset-variable-expanded-in-a-
    // profile accident) is deliberately treated as *set*, so it reaches
    // `exempt::load` and blocks on the missing target rather than silently
    // degrading to "no exemption consulted".
    let cwd = non_repo_cwd();
    let dest = PathBuf::from("/tmp/not-under-any-root.rs");

    let out = run(
        &["hook"],
        &write_payload(&dest, cwd.path(), "fn main() {}"),
        &[("BRENN_SCRUB_WRITE_EXEMPT", "")],
        None,
        None,
    );

    assert_eq!(out.code, Some(2), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("does not exist"),
        "stderr: {}",
        out.stderr
    );
}

// ---- destination-side gated-repo guard -------------------------------------

#[test]
fn a_write_into_a_gated_repo_nested_under_an_exempt_root_blocks() {
    let (_root_guard, root) = deep_dir();
    let inner = root.join("inner");
    std::fs::create_dir_all(&inner).unwrap();
    git_init(&inner);
    std::fs::write(inner.join(".gitleaks.toml"), "title = \"g\"\n").unwrap();
    let cfg = tempfile::tempdir().unwrap();
    let file = exempt_file(cfg.path(), &[&root]);
    let cwd = non_repo_cwd();
    let dest = inner.join("f.rs");

    let out = run(
        &["hook"],
        &write_payload(&dest, cwd.path(), "fn main() {}"),
        &[("BRENN_SCRUB_WRITE_EXEMPT", file.to_str().unwrap())],
        None,
        None,
    );

    assert_eq!(out.code, Some(2), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("both claim this destination"),
        "stderr: {}",
        out.stderr
    );
}

#[test]
fn a_non_repo_sibling_under_the_same_exempt_root_still_exits_zero() {
    let (_root_guard, root) = deep_dir();
    let inner = root.join("inner");
    std::fs::create_dir_all(&inner).unwrap();
    git_init(&inner);
    std::fs::write(inner.join(".gitleaks.toml"), "title = \"g\"\n").unwrap();
    let sibling = root.join("plain");
    std::fs::create_dir_all(&sibling).unwrap();
    let cfg = tempfile::tempdir().unwrap();
    let file = exempt_file(cfg.path(), &[&root]);
    let cwd = non_repo_cwd();
    let dest = sibling.join("f.rs");

    let out = run(
        &["hook"],
        &write_payload(&dest, cwd.path(), "fn main() {}"),
        &[("BRENN_SCRUB_WRITE_EXEMPT", file.to_str().unwrap())],
        None,
        None,
    );

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("exempt from the write-time scrub"),
        "stderr: {}",
        out.stderr
    );
}

// ---- Edit payloads take the same path --------------------------------------

#[test]
fn an_edit_under_an_exempt_root_exits_zero() {
    let (_root_guard, root) = deep_dir();
    let cfg = tempfile::tempdir().unwrap();
    let file = exempt_file(cfg.path(), &[&root]);
    let cwd = non_repo_cwd();
    let dest = root.join("sub/edited.rs");

    let out = run(
        &["hook"],
        &edit_payload(&dest, cwd.path(), "fn main() {}"),
        &[("BRENN_SCRUB_WRITE_EXEMPT", file.to_str().unwrap())],
        None,
        None,
    );

    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("exempt from the write-time scrub"),
        "stderr: {}",
        out.stderr
    );
}

// ---- the exemption is hook-only --------------------------------------------

#[test]
fn tree_and_staged_ignore_the_exemption() {
    // A gated repo with a staged file, so the gate has something to scan and
    // does not panic on an empty tree. A stub gitleaks satisfies the version
    // probe and finds nothing, so a clean run is exit 0.
    let stub = stub_gitleaks(PINNED_VERSION);
    let repo_guard = tempfile::tempdir().unwrap();
    let repo = repo_guard.path();
    git_init(repo);
    std::fs::write(repo.join(".gitleaks.toml"), "title = \"g\"\n").unwrap();
    std::fs::write(repo.join("f.rs"), "fn main() {}\n").unwrap();
    let add = Command::new("git")
        .current_dir(repo)
        .args(["add", "."])
        .output()
        .expect("git add");
    assert!(add.status.success());

    let (_root_guard, root) = deep_dir();
    let cfg = tempfile::tempdir().unwrap();
    let file = exempt_file(cfg.path(), &[&root]);

    for mode in ["tree", "staged"] {
        let set = run(
            &[mode],
            "",
            &[("BRENN_SCRUB_WRITE_EXEMPT", file.to_str().unwrap())],
            Some(stub.path()),
            Some(repo),
        );
        let unset = run(&[mode], "", &[], Some(stub.path()), Some(repo));

        assert_eq!(
            set.code, unset.code,
            "{mode}: the exemption must not change the exit code; \
             set={:?} unset={:?}",
            set.stderr, unset.stderr
        );
        assert!(
            !set.stderr.contains("exempt from the write-time scrub"),
            "{mode} must never emit the hook audit line: {}",
            set.stderr
        );
    }
}

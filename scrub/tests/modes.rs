//! End-to-end behavior of the modes whose *contract* is consumed by something
//! other than a human reading stderr: `range`'s warn-only rollout switch,
//! `tree`'s captured stdout, and `staged`'s reliance on the git environment a
//! hook hands it.
//!
//! The first two were covered only at the argument-parsing layer, where an
//! inverted or dropped branch downstream leaves every test green. These drive
//! the real binary against a real repo instead.
//!
//! Skipped with a message when the pinned gitleaks is absent, matching
//! `rules.rs`.

mod common;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use common::gitleaks_available;
use git_fixture::{git, init_repo};

const BIN: &str = env!("CARGO_BIN_EXE_brenn-scrub");

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("scrub crate has a parent directory")
        .to_path_buf()
}

/// A token the built-in rules catch, assembled at runtime so this file never
/// contains a literal the gate would flag.
fn canary() -> String {
    format!(
        "let gh = \"{}_{}\";\n",
        "ghp", "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8"
    )
}

fn write_file(repo: &Path, rel: &str, body: &str) {
    let path = repo.join(rel);
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(path, body).expect("write fixture");
}

/// A repo carrying the real public config, so these exercise the shipped rules.
fn repo_with(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    let p = dir.path();
    init_repo(p);
    std::fs::copy(repo_root().join(".gitleaks.toml"), p.join(".gitleaks.toml"))
        .expect("copy public config");
    for (rel, body) in files {
        write_file(p, rel, body);
    }
    git(p, &["add", "-A"]);
    git(p, &["commit", "-qm", "fixture"]);
    dir
}

struct Output {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn run_in(repo: &Path, args: &[&str], stdin: &str) -> Output {
    let mut cmd = Command::new(BIN);
    cmd.current_dir(repo)
        // The overlay is a local convention; these assertions are about the
        // public rules only, so a machine's local overlay must not leak in.
        .env_remove("BRENN_SCRUB_DENYLIST")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // The fixture repo is the whole subject here; a `GIT_DIR` from a hook
    // environment would point the spawned binary — and the `gitleaks` it
    // spawns — at some other repo entirely.
    git_fixture::hermetic(&mut cmd);
    let mut child = cmd.spawn().expect("failed to spawn brenn-scrub");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait");
    Output {
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// Pre-push stdin for pushing `main` as a ref the remote does not have.
fn new_ref_line(repo: &Path) -> String {
    let sha = git(repo, &["rev-parse", "HEAD"]).trim().to_string();
    format!("refs/heads/main {sha} refs/heads/main {}\n", "0".repeat(40))
}

/// Pre-push ships warn-only and later flips to enforcing by deleting the flag.
/// Only the flag's *parsing* was asserted, so an inverted branch could leave
/// the push gate off with nothing red -- or block every push before the tree
/// is green.
#[test]
fn warn_only_reports_the_same_findings_it_would_have_blocked_on() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[("src/a.rs", &canary())]);
    let stdin = new_ref_line(dir.path());

    let warned = run_in(dir.path(), &["range", "--warn-only"], &stdin);
    assert_eq!(
        warned.code,
        Some(0),
        "warn-only must let the push through; stderr: {}",
        warned.stderr
    );
    assert!(
        warned.stderr.contains("would fail the scrub gate"),
        "the findings must still be visible: {}",
        warned.stderr
    );

    let blocked = run_in(dir.path(), &["range"], &stdin);
    assert_eq!(
        blocked.code,
        Some(1),
        "the same findings must block without the flag; stderr: {}",
        blocked.stderr
    );
    assert!(
        blocked.stderr.contains("blocked this push"),
        "{}",
        blocked.stderr
    );
}

#[test]
fn a_clean_repo_passes_range_either_way() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[("src/a.rs", "let user = \"alice\";\n")]);
    let stdin = new_ref_line(dir.path());
    for args in [&["range"][..], &["range", "--warn-only"][..]] {
        let out = run_in(dir.path(), args, &stdin);
        assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    }
}

fn tree_json(out: &Output) -> serde_json::Value {
    serde_json::from_str(&out.stdout)
        .unwrap_or_else(|e| panic!("tree stdout must be JSON ({e}): {:?}", out.stdout))
}

/// Tree stdout is captured as the burndown worklist, and it self-documents its
/// scope so the artifact can never be read as covering more than it scanned.
/// A field rename or serializer change would silently break that.
#[test]
fn tree_stdout_carries_findings_and_an_empty_exclusion_list_by_default() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[("src/a.rs", &canary())]);
    let out = run_in(dir.path(), &["tree"], "");
    let json = tree_json(&out);

    assert_eq!(
        json["excluded"],
        serde_json::json!([]),
        "a bare scan excludes nothing"
    );
    let findings = json["findings"].as_array().expect("findings is an array");
    assert_eq!(findings.len(), 1, "{:?}", out.stdout);
    assert_eq!(findings[0]["File"], "src/a.rs");
    assert_eq!(out.code, Some(1), "findings must fail the scan");
}

/// Exclusion has to drop files *before* the mirror. Moving or dropping the
/// partition call would scan excluded content anyway, which surfaces as a
/// confusing red rather than as a failing test.
#[test]
fn an_excluded_prefix_is_neither_scanned_nor_counted() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[
        ("docs/adr/leak.rs", &canary()),
        ("src/clean.rs", "let user = \"alice\";\n"),
    ]);

    let out = run_in(dir.path(), &["tree", "--exclude", "docs/adr"], "");
    let json = tree_json(&out);
    assert_eq!(
        json["excluded"],
        serde_json::json!(["docs/adr"]),
        "the scope must be recorded verbatim"
    );
    assert_eq!(
        json["findings"].as_array().expect("array").len(),
        0,
        "excluded content must not be scanned: {:?}",
        out.stdout
    );
    assert_eq!(out.code, Some(0));
    assert!(
        out.stderr.contains("EXCLUDED: docs/adr"),
        "exclusion must be loud: {}",
        out.stderr
    );

    // Without the flag the same repo is red -- so the assertions above cannot
    // be passing merely because the scan found nothing anywhere.
    let bare = run_in(dir.path(), &["tree"], "");
    assert_eq!(bare.code, Some(1), "stderr: {}", bare.stderr);
    assert_eq!(
        tree_json(&bare)["findings"]
            .as_array()
            .expect("array")
            .len(),
        1
    );
}

/// `staged` runs as a pre-commit hook, and a hook is exactly where git exports
/// `GIT_DIR` and `GIT_INDEX_FILE`. Scrub must scan the index those name -- that
/// is how `git commit --only` and linked-worktree commits get scanned at all --
/// so its production spawns deliberately inherit the environment. Every other
/// harness here strips `GIT_*`, which would let that inheritance rot unnoticed.
///
/// The observable is staged *content*, not the resolved repo root: with
/// `GIT_DIR` set and no `GIT_WORK_TREE`, git treats the cwd as the work tree,
/// so the root follows the cwd while the index comes from `GIT_DIR`.
#[test]
fn staged_mode_scans_the_index_named_by_the_hook_environment() {
    if !gitleaks_available() {
        return;
    }
    let fixture = repo_with(&[("src/a.rs", "let user = \"alice\";\n")]);
    write_file(fixture.path(), "src/planted.rs", &canary());
    git(fixture.path(), &["add", "-A"]);

    // Somewhere else entirely: not a git repo, carrying only the public rules
    // so config resolution has something to load once the root lands here.
    let elsewhere = tempfile::tempdir().expect("temp dir");
    std::fs::copy(
        repo_root().join(".gitleaks.toml"),
        elsewhere.path().join(".gitleaks.toml"),
    )
    .expect("copy public config");

    // Hermetic first, then one explicit `GIT_DIR`: the contract under test is
    // that the environment passed to scrub overrides its cwd, and stripping
    // ambient `GIT_*` first means only this test's variable can decide that.
    let mut cmd = Command::new(BIN);
    git_fixture::hermetic(&mut cmd);
    cmd.current_dir(elsewhere.path())
        .env_remove("BRENN_SCRUB_DENYLIST")
        .env("GIT_DIR", fixture.path().join(".git"))
        .arg("staged")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = cmd.output().expect("failed to spawn brenn-scrub");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();

    assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
    assert!(
        stderr.contains("blocked this commit"),
        "the planted finding must block: {stderr}"
    );
    assert!(
        stderr.contains("src/planted.rs"),
        "the finding must be attributed to the staged path: {stderr}"
    );
}

/// A tracked path absent from the worktree used to be skipped silently, which
/// reads as a narrower scan reported as a full green. A staged deletion is the
/// one legitimate case and is announced.
#[test]
fn a_staged_deletion_is_skipped_out_loud_and_does_not_fail_the_scan() {
    if !gitleaks_available() {
        return;
    }
    let dir = repo_with(&[
        ("src/a.rs", "let user = \"alice\";\n"),
        ("src/gone.rs", "let peer = \"bob\";\n"),
    ]);
    std::fs::remove_file(dir.path().join("src/gone.rs")).expect("remove");

    let out = run_in(dir.path(), &["tree"], "");
    assert_eq!(out.code, Some(0), "stderr: {}", out.stderr);
    assert!(
        out.stderr.contains("SKIPPED: src/gone.rs"),
        "an unmirrored tracked path must be named: {}",
        out.stderr
    );
}

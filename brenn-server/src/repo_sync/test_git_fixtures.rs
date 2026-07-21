//! Scratch git repo helpers shared across repo-sync integration tests.

use std::path::Path;
use std::process::Command as StdCommand;

/// Run a git command in `dir`. Panics with a human-readable error on failure.
pub(crate) fn run_git(dir: &Path, args: &[&str]) {
    let out = StdCommand::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .expect("git invocation");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Build a scratch bare remote and one clone that tracks it.
/// Returns `(remote_tempdir, clone_tempdir)`. The clone is ready for
/// `pull_clone` to return `UpToDate` on the first call.
pub(crate) fn scratch_remote_and_clone() -> (tempfile::TempDir, tempfile::TempDir) {
    let remote = tempfile::tempdir().unwrap();
    run_git(remote.path(), &["init", "--bare", "-b", "main"]);

    // Seed the remote via an intermediate clone, then hand back a second
    // fresh clone. Using two separate clones prevents the seed push from
    // accidentally advancing the caller's local HEAD.
    // Clone into the live tempdir (git accepts an existing empty directory).
    // Deleting it first would free tempfile's name reservation, letting a
    // concurrent test process draw the same path and collide.
    let seed = tempfile::tempdir().unwrap();
    run_git(
        Path::new("/tmp"),
        &[
            "clone",
            &remote.path().display().to_string(),
            seed.path().to_str().unwrap(),
        ],
    );
    std::fs::write(seed.path().join("readme.md"), "hello").unwrap();
    run_git(seed.path(), &["add", "."]);
    run_git(seed.path(), &["commit", "-m", "initial"]);
    run_git(seed.path(), &["push", "-u", "origin", "main"]);

    let clone = tempfile::tempdir().unwrap();
    run_git(
        Path::new("/tmp"),
        &[
            "clone",
            &remote.path().display().to_string(),
            clone.path().to_str().unwrap(),
        ],
    );
    (remote, clone)
}

/// Same as `scratch_remote_and_clone` but pushes one extra commit from a
/// sibling clone before returning, so the tracking clone is already behind
/// `origin/main` by one commit. A subsequent `pull_clone` call on the
/// tracking clone will fast-forward its HEAD.
pub(crate) fn scratch_remote_and_clone_behind_by_one() -> (tempfile::TempDir, tempfile::TempDir) {
    let (remote, clone) = scratch_remote_and_clone();
    push_sibling_commit(remote.path(), "upstream advance");
    (remote, clone)
}

/// Push one commit from a fresh sibling clone to advance `origin/main`.
/// Returns the pushed commit's SHA.
pub(crate) fn push_sibling_commit(remote: &Path, message: &str) -> String {
    let sibling = tempfile::tempdir().unwrap();
    run_git(
        Path::new("/tmp"),
        &[
            "clone",
            &remote.display().to_string(),
            sibling.path().to_str().unwrap(),
        ],
    );
    let unique = format!("{message}.txt");
    std::fs::write(sibling.path().join(&unique), "x").unwrap();
    run_git(sibling.path(), &["add", "."]);
    run_git(sibling.path(), &["commit", "-m", message]);
    run_git(sibling.path(), &["push", "origin", "main"]);
    head(sibling.path())
}

pub(crate) fn head(path: &Path) -> String {
    let out = StdCommand::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(path)
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@test")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@test")
        .output()
        .expect("git rev-parse HEAD: spawn failed");
    assert!(
        out.status.success(),
        "git rev-parse HEAD failed: {}",
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Make a local commit in `path` without pushing.
pub(crate) fn local_commit(path: &Path, filename: &str, message: &str) {
    std::fs::write(path.join(filename), "content").unwrap();
    run_git(path, &["add", "."]);
    run_git(path, &["commit", "-m", message]);
}

/// Create an orphan branch with a single commit in `path` and return the
/// resulting commit SHA. The new branch has no common history with any
/// existing branch in the repo. Used by `is_ancestor` "unrelated tips" test.
///
/// After this call, `path` is left on the newly created orphan branch.
pub(crate) fn orphan_commit(path: &Path, branch_name: &str, message: &str) -> String {
    run_git(path, &["checkout", "--orphan", branch_name]);
    // Clear the index so we don't inherit staged files from the previous branch.
    run_git(path, &["rm", "-rf", "--cached", "."]);
    // Write a unique file so the commit has content.
    std::fs::write(path.join(format!("{branch_name}.txt")), branch_name).unwrap();
    run_git(path, &["add", "."]);
    run_git(path, &["commit", "-m", message]);
    head(path)
}

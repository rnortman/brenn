//! Scratch git repo helpers shared across repo-sync integration tests.

use std::path::Path;

use git_fixture::{clone_repo, git, init_bare_repo};

/// Build a scratch bare remote and one clone that tracks it.
/// Returns `(remote_tempdir, clone_tempdir)`. The clone is ready for
/// `pull_clone` to return `UpToDate` on the first call.
pub(crate) fn scratch_remote_and_clone() -> (tempfile::TempDir, tempfile::TempDir) {
    let remote = tempfile::tempdir().unwrap();
    init_bare_repo(remote.path());

    // Seed the remote via an intermediate clone, then hand back a second
    // fresh clone. Using two separate clones prevents the seed push from
    // accidentally advancing the caller's local HEAD.
    // Clone into the live tempdir (git accepts an existing empty directory).
    // Deleting it first would free tempfile's name reservation, letting a
    // concurrent test process draw the same path and collide.
    let seed = tempfile::tempdir().unwrap();
    clone_repo(remote.path(), seed.path());
    std::fs::write(seed.path().join("readme.md"), "hello").unwrap();
    git(seed.path(), &["add", "."]);
    git(seed.path(), &["commit", "-m", "initial"]);
    git(seed.path(), &["push", "-u", "origin", "main"]);

    let clone = tempfile::tempdir().unwrap();
    clone_repo(remote.path(), clone.path());
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
    clone_repo(remote, sibling.path());
    let unique = format!("{message}.txt");
    std::fs::write(sibling.path().join(&unique), "x").unwrap();
    git(sibling.path(), &["add", "."]);
    git(sibling.path(), &["commit", "-m", message]);
    git(sibling.path(), &["push", "origin", "main"]);
    head(sibling.path())
}

pub(crate) fn head(path: &Path) -> String {
    git(path, &["rev-parse", "HEAD"]).trim().to_string()
}

/// Make a local commit in `path` without pushing.
pub(crate) fn local_commit(path: &Path, filename: &str, message: &str) {
    std::fs::write(path.join(filename), "content").unwrap();
    git(path, &["add", "."]);
    git(path, &["commit", "-m", message]);
}

/// Create an orphan branch with a single commit in `path` and return the
/// resulting commit SHA. The new branch has no common history with any
/// existing branch in the repo. Used by `is_ancestor` "unrelated tips" test.
///
/// After this call, `path` is left on the newly created orphan branch.
pub(crate) fn orphan_commit(path: &Path, branch_name: &str, message: &str) -> String {
    git(path, &["checkout", "--orphan", branch_name]);
    // Clear the index so we don't inherit staged files from the previous branch.
    git(path, &["rm", "-rf", "--cached", "."]);
    // Write a unique file so the commit has content.
    std::fs::write(path.join(format!("{branch_name}.txt")), branch_name).unwrap();
    git(path, &["add", "."]);
    git(path, &["commit", "-m", message]);
    head(path)
}

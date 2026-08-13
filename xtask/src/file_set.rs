//! The set of repo files the policy guards scan.
//!
//! Two sources, chosen by the caller and never by fallback. `git ls-files` is
//! the workspace source, used by the checks that run outside the sandbox: the
//! tracked tree, so build output and untracked scratch are outside the scan by
//! construction. A manifest is the build-graph source: a listing produced from
//! declared inputs, so the guards are cacheable tests that rerun exactly when a
//! file they read changes.
//!
//! Both yield repo-root-relative paths, sorted, so a guard's output does not
//! depend on which source produced its input.

use std::path::{Path, PathBuf};

/// Below this many files, a guard's scan is broken rather than clean: the repo
/// holds thousands, and a guard over a collapsed file set passes vacuously.
/// Shared so the floor is one number rather than one per guard.
pub const MIN_SCANNED_FILES: usize = 200;

/// Tracked files, from git. Panics if git cannot be run or fails.
///
/// An empty listing is a failure for the same reason [`from_manifest`]'s is: a
/// guard whose file set collapsed to nothing passes vacuously. Both sources
/// fail closed identically, so a guard's floor never depends on which one the
/// caller picked.
pub fn from_git(root: &Path) -> Vec<PathBuf> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "-z"])
        .output()
        .unwrap_or_else(|e| panic!("policy file set: cannot run git ls-files: {e}"));
    assert!(
        out.status.success(),
        "policy file set: git ls-files failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let listing = String::from_utf8(out.stdout)
        .unwrap_or_else(|e| panic!("policy file set: git ls-files output is not UTF-8: {e}"));
    let files = sorted(listing.split('\0'));
    assert!(
        !files.is_empty(),
        "policy file set: git ls-files in {} listed no files",
        root.display()
    );
    files
}

/// Files listed in a manifest, one repo-root-relative path per line.
///
/// An empty manifest is a failure: a guard whose input closure collapsed to
/// nothing passes vacuously, which is indistinguishable from a clean run.
pub fn from_manifest(manifest: &Path) -> Vec<PathBuf> {
    let text = std::fs::read_to_string(manifest).unwrap_or_else(|e| {
        panic!(
            "policy file set: cannot read manifest {}: {e}",
            manifest.display()
        )
    });
    let files = sorted(text.lines());
    assert!(
        !files.is_empty(),
        "policy file set: manifest {} lists no files",
        manifest.display()
    );
    files
}

fn sorted<'a>(paths: impl Iterator<Item = &'a str>) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = paths
        .map(str::trim_end)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manifest_parses_to_sorted_relative_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = tmp.path().join("policy_manifest.txt");
        std::fs::write(&manifest, "b/two.rs\na/one.rs\n\nc/three.rs\n").unwrap();
        assert_eq!(
            from_manifest(&manifest),
            vec![
                PathBuf::from("a/one.rs"),
                PathBuf::from("b/two.rs"),
                PathBuf::from("c/three.rs"),
            ]
        );
    }

    #[test]
    #[should_panic(expected = "lists no files")]
    fn an_empty_manifest_is_a_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = tmp.path().join("policy_manifest.txt");
        std::fs::write(&manifest, "\n\n").unwrap();
        from_manifest(&manifest);
    }

    /// Root a fixture repo holding one nested tracked file, one top-level
    /// tracked file, one untracked file and one ignored file.
    fn fixture_repo(root: &Path) {
        git_fixture::init_repo(root);
        std::fs::create_dir_all(root.join("crate-a/src")).unwrap();
        std::fs::write(root.join("crate-a/src/deep.rs"), "fn f() {}\n").unwrap();
        std::fs::write(root.join("top.rs"), "fn g() {}\n").unwrap();
        std::fs::write(root.join(".gitignore"), "ignored.rs\n").unwrap();
        std::fs::write(root.join("ignored.rs"), "fn h() {}\n").unwrap();
        std::fs::write(root.join("untracked.rs"), "fn i() {}\n").unwrap();
        git_fixture::git(
            root,
            &["add", ".gitignore", "crate-a/src/deep.rs", "top.rs"],
        );
        git_fixture::git(root, &["commit", "-m", "base"]);
    }

    /// The git half of the file set: tracked files only, at every depth, sorted
    /// — and neither the untracked nor the ignored sibling. A regression in the
    /// NUL splitting or the `-C root` targeting shrinks the git-side scan, and
    /// four of the six guards have no floor to notice.
    #[test]
    fn from_git_lists_tracked_files_only() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fixture_repo(root);

        assert_eq!(
            from_git(root),
            vec![
                PathBuf::from(".gitignore"),
                PathBuf::from("crate-a/src/deep.rs"),
                PathBuf::from("top.rs"),
            ]
        );
    }

    #[test]
    #[should_panic(expected = "listed no files")]
    fn a_repo_with_nothing_tracked_is_a_failure() {
        let tmp = tempfile::tempdir().unwrap();
        git_fixture::init_repo(tmp.path());
        from_git(tmp.path());
    }
}

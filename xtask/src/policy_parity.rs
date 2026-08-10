//! Set equality between the Bazel policy scan's file set and the tracked tree.
//!
//! The guards scan `//:all_policy_srcs`, a hand-aggregated list of one
//! `filegroup` per Bazel package; the make lane scans `git ls-files`. Nothing
//! inside the sandbox can compare the two — a policy test sees neither git nor
//! `bazel query` — so this runs outside it, over a manifest the build produced
//! and the tracked listing beside it.
//!
//! Equality in both directions, never containment. A package added without
//! joining the aggregate drops its whole directory out of the scan and every
//! guard keeps passing over the smaller set. A tracked file that lands under
//! one of the glob's exclusions leaves the scan the same silent way. And an
//! untracked file joins the Bazel set alone, so the two verdicts diverge on any
//! dirty tree — which is why this is a CI step and not a lane of `make check`.
//!
//! Symlinks are dropped from both sides. Bazel's glob does not follow the
//! tracked `host-crates/` links, and a symlink carries no content for a
//! content-scanning guard to read, so its absence from one side is not drift.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::file_set;

/// Compare the two listings, printing every path either side is missing.
pub fn run_policy_parity(root: &Path, manifest: &Path) -> bool {
    let tracked = file_set::from_git(root);
    let declared = file_set::from_manifest(manifest);
    let found = parity_violations(
        root,
        tracked,
        declared,
        file_set::MIN_SCANNED_FILES,
        &mut |count| println!("policy scan parity: {count} paths, both sides equal"),
    );
    if found.is_empty() {
        return true;
    }
    eprintln!("policy scan parity: the Bazel scan and the tracked tree are not the same set:");
    for line in &found {
        eprintln!("  {line}");
    }
    false
}

/// Paths that name a symlink in the working tree, dropped.
///
/// A path in either listing that cannot be stat'd at all is not a symlink
/// question: git lists a tracked file whose worktree copy was deleted, and the
/// manifest lists what the build staged. Either way the comparison is being run
/// over a tree that does not match its own inputs.
fn without_symlinks(root: &Path, files: Vec<PathBuf>) -> BTreeSet<PathBuf> {
    files
        .into_iter()
        .filter(|rel| {
            let full = root.join(rel);
            let meta = std::fs::symlink_metadata(&full).unwrap_or_else(|e| {
                panic!("policy scan parity: cannot stat {}: {e}", full.display())
            });
            !meta.is_symlink()
        })
        .collect()
}

fn parity_violations(
    root: &Path,
    tracked: Vec<PathBuf>,
    declared: Vec<PathBuf>,
    floor: usize,
    on_equal: &mut dyn FnMut(usize),
) -> Vec<String> {
    let tracked = without_symlinks(root, tracked);
    let declared = without_symlinks(root, declared);
    let mut found = Vec::new();
    for rel in tracked.difference(&declared) {
        found.push(format!(
            "{} is tracked but no package's policy_srcs declares it: the guards do not scan it. \
             Either the package is missing from //:all_policy_srcs, or the file matches one of \
             POLICY_SRC_EXCLUDE's patterns.",
            rel.display()
        ));
    }
    for rel in declared.difference(&tracked) {
        found.push(format!(
            "{} is in the Bazel scan but is not tracked: the two lanes' guards read different \
             trees, and this file is in every policy test's input closure.",
            rel.display()
        ));
    }
    if found.is_empty() {
        // Equal sets over a collapsed listing agree about nothing. The floor is
        // the same one the guards themselves hold.
        if tracked.len() < floor {
            return vec![format!(
                "both sides list {} paths, below the floor of {floor}: the comparison is not \
                 clean, it is not running.",
                tracked.len()
            )];
        }
        on_equal(tracked.len());
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tree with the given relative files, and the same list as a listing.
    fn tree(files: &[&str]) -> (tempfile::TempDir, Vec<PathBuf>) {
        let dir = tempfile::tempdir().unwrap();
        for rel in files {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, "x\n").unwrap();
        }
        (dir, files.iter().map(PathBuf::from).collect())
    }

    fn violations(root: &Path, tracked: Vec<PathBuf>, declared: Vec<PathBuf>) -> Vec<String> {
        parity_violations(root, tracked, declared, 1, &mut |_| {})
    }

    #[test]
    fn two_equal_listings_pass_and_report_their_size() {
        let (dir, files) = tree(&["a/one.rs", "b/two.rs"]);
        let mut counted = None;
        let out = parity_violations(dir.path(), files.clone(), files, 1, &mut |n| {
            counted = Some(n)
        });
        assert!(out.is_empty(), "{out:?}");
        assert_eq!(counted, Some(2));
    }

    #[test]
    fn a_tracked_file_no_package_declares_is_reported() {
        let (dir, files) = tree(&["a/one.rs", "b/two.rs"]);
        let out = violations(dir.path(), files, vec![PathBuf::from("a/one.rs")]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("b/two.rs"), "{}", out[0]);
        assert!(out[0].contains("do not scan"), "{}", out[0]);
    }

    #[test]
    fn an_untracked_file_in_the_scan_is_reported() {
        let (dir, files) = tree(&["a/one.rs", "scratch.md"]);
        let out = violations(dir.path(), vec![PathBuf::from("a/one.rs")], files);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("scratch.md"), "{}", out[0]);
        assert!(out[0].contains("is not tracked"), "{}", out[0]);
    }

    #[test]
    fn a_tracked_symlink_the_glob_cannot_see_is_not_drift() {
        let (dir, _) = tree(&["a/one.rs"]);
        #[cfg(unix)]
        std::os::unix::fs::symlink("a", dir.path().join("link")).unwrap();
        let out = violations(
            dir.path(),
            vec![PathBuf::from("a/one.rs"), PathBuf::from("link")],
            vec![PathBuf::from("a/one.rs")],
        );
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn equal_but_collapsed_listings_fail_the_floor() {
        let (dir, files) = tree(&["a/one.rs"]);
        let out = parity_violations(
            dir.path(),
            files.clone(),
            files,
            file_set::MIN_SCANNED_FILES,
            &mut |_| {},
        );
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("below the floor"), "{}", out[0]);
    }

    #[test]
    #[should_panic(expected = "cannot stat")]
    fn a_listed_path_that_is_not_there_panics() {
        let (dir, _) = tree(&["a/one.rs"]);
        violations(dir.path(), vec![PathBuf::from("gone.rs")], vec![]);
    }
}

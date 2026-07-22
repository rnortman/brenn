//! Destination resolution for the write-time hook.
//!
//! The hook is handed a write *destination* and must decide two things from it
//! alone: which repo (if any) the write belongs to, and where inside that repo
//! it lands. Both are judged from the destination's true filesystem location,
//! never from the session's working directory, so a write is always scanned
//! against the config of the repo it actually targets.
//!
//! The decision reflects the filesystem at check time. The hook returns before
//! Claude Code performs the write, so a path element swapped between this
//! resolution and that write (a symlink retargeted, or a hardlink into gated
//! content aliased under an ungated path) lands the write somewhere other than
//! what was judged. This check-then-write race is inherent to a pre-write hook
//! and undetectable by path resolution; on the single-operator machine it
//! requires the operator to sabotage their own gate, and the commit and push
//! gates still scan the content before it can be published. Accepted residual,
//! not closable here.

use crate::config;
use crate::git;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Nearest existing ancestor (canonicalized) and the full resolved destination.
pub struct ResolvedDest {
    pub existing: PathBuf,
    pub resolved: PathBuf,
}

/// Resolve a write destination to its true filesystem location.
///
/// `file_path` is absolutized against `cwd` if relative, then symlink-resolved
/// against the real filesystem (nearest existing ancestor canonicalized,
/// non-existent tail rejoined). Panics when the resolved tail contains a `..`
/// (no lexical games around a later prefix match), when no existing ancestor
/// exists, or when canonicalization fails (a dangling symlink in the tail). In
/// hook mode each of those panics becomes a block.
pub fn resolve(file_path: &Path, cwd: &Path) -> ResolvedDest {
    let abs = if file_path.is_absolute() {
        file_path.to_path_buf()
    } else {
        cwd.join(file_path)
    };
    let (existing, resolved) = resolve_destination(&abs);
    ResolvedDest { existing, resolved }
}

/// Nearest existing ancestor (canonicalized) and the full resolved
/// destination. Panics if a `..` (or any non-`Normal` component) sits in the
/// not-yet-existing tail, or if no ancestor exists.
fn resolve_destination(abs: &Path) -> (PathBuf, PathBuf) {
    let mut suffix: Vec<OsString> = Vec::new();
    let mut cur = abs.to_path_buf();
    let existing = loop {
        // `symlink_metadata` (lstat) succeeds for a dangling symlink where
        // `exists` (which follows the link) reports missing. A dangling link in
        // the tail therefore stops the walk here and reaches `canonicalize`,
        // which follows it, fails on the absent target, and blocks -- rather
        // than being pushed onto the tail as a non-existent name and letting the
        // resolved path stay lexically under a repo while the real write follows
        // the link elsewhere.
        if cur.symlink_metadata().is_ok() {
            break std::fs::canonicalize(&cur)
                .unwrap_or_else(|e| panic!("cannot canonicalize {}: {e}", cur.display()));
        }
        // `file_name` is `None` for a component that is `..`, `.`, or the root,
        // so a `..` in the not-yet-existing tail lands here as a block.
        let name = cur.file_name().unwrap_or_else(|| {
            panic!(
                "write destination {} has a `..` or root component in its non-existent tail",
                abs.display()
            )
        });
        suffix.push(name.to_os_string());
        cur = cur
            .parent()
            .unwrap_or_else(|| {
                panic!(
                    "write destination {} has no existing ancestor to resolve against",
                    abs.display()
                )
            })
            .to_path_buf();
    };

    let mut resolved = existing.clone();
    for name in suffix.iter().rev() {
        resolved.push(name);
    }
    (existing, resolved)
}

/// A gated git repo (one whose root carries a `.gitleaks.toml` on disk)
/// containing `dir`, if any. A non-repo path and a repo without the config both
/// yield `None` -- those are the legitimate ungated-destination shapes.
///
/// Gatedness is filesystem presence of the file at the repo root, not
/// git-trackedness: a repo with an untracked `.gitleaks.toml` is gated.
pub fn gated_repo_containing(dir: &Path) -> Option<PathBuf> {
    let probe = if dir.is_dir() {
        dir.to_path_buf()
    } else {
        dir.parent()?.to_path_buf()
    };
    let root = git::try_repo_root(&probe)?;
    // This presence check is the sole gating decision: absent config ⇒ ungated
    // ⇒ the write passes unscanned. `exists()` would fold every stat error
    // (`EACCES`, `ELOOP`, `EIO`) into `false` and silently exempt a gated repo,
    // so a non-`ENOENT` failure panics (⇒ block) instead, matching the
    // fail-closed posture of `try_repo_root`.
    let config = root.join(config::PUBLIC_FILENAME);
    config
        .try_exists()
        .unwrap_or_else(|e| panic!("cannot stat {}: {e}", config.display()))
        .then_some(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use git_fixture::init_repo;

    /// A tempdir with a nested `a/b/c`, deep enough to sit clear of the
    /// filesystem root. Returns (guard, canonical deep dir).
    fn deep_dir() -> (tempfile::TempDir, PathBuf) {
        let d = tempfile::tempdir().unwrap();
        let deep = d.path().join("a/b/c");
        std::fs::create_dir_all(&deep).unwrap();
        let canonical = std::fs::canonicalize(&deep).unwrap();
        (d, canonical)
    }

    /// The message a panicking closure produces, for neutrality checks. In hook
    /// mode `catch_unwind` feeds this text back to the agent, so it carries the
    /// same neutrality bar as every other emitted string.
    fn panic_text<F: FnOnce() + std::panic::UnwindSafe>(f: F) -> String {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let res = std::panic::catch_unwind(f);
        std::panic::set_hook(prev);
        let payload = res.expect_err("expected a panic");
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .expect("panic payload was not a string")
    }

    // ---- resolve -----------------------------------------------------------

    #[test]
    fn a_relative_destination_is_absolutized_against_cwd() {
        let (_d, root) = deep_dir();
        let r = resolve(Path::new("newfile.rs"), &root);
        assert_eq!(r.existing, root);
        assert_eq!(r.resolved, root.join("newfile.rs"));
    }

    #[test]
    fn a_nonexistent_tail_resolves_under_its_nearest_existing_ancestor() {
        let (_d, root) = deep_dir();
        let r = resolve(&root.join("brand/new/tree/file.rs"), Path::new("/"));
        assert_eq!(r.existing, root);
        assert_eq!(r.resolved, root.join("brand/new/tree/file.rs"));
    }

    #[test]
    #[should_panic(expected = "non-existent tail")]
    fn a_dotdot_in_the_nonexistent_tail_panics() {
        let (_d, root) = deep_dir();
        resolve(&root.join("nope/../evil"), Path::new("/"));
    }

    #[test]
    #[should_panic(expected = "cannot canonicalize")]
    fn a_dangling_symlink_in_the_tail_panics() {
        let (_d, root) = deep_dir();
        let link = root.join("dangling");
        std::os::unix::fs::symlink(root.join("no/such/target"), &link).unwrap();
        resolve(&link, Path::new("/"));
    }

    #[test]
    fn a_symlinked_destination_resolves_to_its_true_target() {
        let (_d, root) = deep_dir();
        let outside = tempfile::tempdir().unwrap();
        let outside_dir = std::fs::canonicalize(outside.path()).unwrap();
        let link = root.join("escape");
        std::os::unix::fs::symlink(&outside_dir, &link).unwrap();
        let r = resolve(&link.join("f.rs"), Path::new("/"));
        assert_eq!(r.resolved, outside_dir.join("f.rs"));
    }

    // ---- gated_repo_containing ---------------------------------------------

    #[test]
    fn a_plain_directory_is_not_gated() {
        let (_d, deep) = deep_dir();
        assert_eq!(gated_repo_containing(&deep), None);
    }

    #[test]
    fn a_repo_without_the_config_is_not_gated() {
        let d = tempfile::tempdir().unwrap();
        let repo = d.path().join("x/y/repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);
        let canonical = std::fs::canonicalize(&repo).unwrap();
        assert_eq!(gated_repo_containing(&canonical), None);
    }

    /// A config that git *tracks* gates. Paired with `an_untracked_config_still_
    /// gates`, this pins that gatedness is filesystem presence of the file --
    /// tracked or not -- so a regression to a `git ls-files`-style probe fails
    /// on exactly one of the two.
    #[test]
    fn a_tracked_config_gates() {
        let d = tempfile::tempdir().unwrap();
        let repo = d.path().join("x/y/repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);
        std::fs::write(repo.join(config::PUBLIC_FILENAME), "title = \"g\"\n").unwrap();
        git_fixture::git(&repo, &["add", config::PUBLIC_FILENAME]);
        let canonical = std::fs::canonicalize(&repo).unwrap();
        assert_eq!(gated_repo_containing(&canonical), Some(canonical));
    }

    /// A config git does not track still gates -- the file's presence on disk is
    /// the opt-in marker, independent of the index.
    #[test]
    fn an_untracked_config_still_gates() {
        let d = tempfile::tempdir().unwrap();
        let repo = d.path().join("x/y/repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);
        std::fs::write(repo.join(config::PUBLIC_FILENAME), "title = \"g\"\n").unwrap();
        let canonical = std::fs::canonicalize(&repo).unwrap();
        assert_eq!(gated_repo_containing(&canonical), Some(canonical));
    }

    #[test]
    fn gatedness_is_probed_from_a_files_parent() {
        let d = tempfile::tempdir().unwrap();
        let repo = d.path().join("x/y/repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);
        std::fs::write(repo.join(config::PUBLIC_FILENAME), "title = \"g\"\n").unwrap();
        let canonical = std::fs::canonicalize(&repo).unwrap();
        let file = canonical.join("f.rs");
        std::fs::write(&file, "x").unwrap();
        assert_eq!(gated_repo_containing(&file), Some(canonical));
    }

    // ---- panic-message neutrality ------------------------------------------

    #[test]
    fn every_panic_message_is_neutral() {
        use crate::message::neutral::assert_neutral;
        assert_neutral(
            &panic_text(|| {
                let (_d, root) = deep_dir();
                resolve(&root.join("nope/../evil"), Path::new("/"));
            }),
            "dotdot tail",
        );
        assert_neutral(
            &panic_text(|| {
                let (_d, root) = deep_dir();
                let link = root.join("dangling");
                std::os::unix::fs::symlink(root.join("no/such/target"), &link).unwrap();
                resolve(&link, Path::new("/"));
            }),
            "dangling symlink",
        );
    }
}

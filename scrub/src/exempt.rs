//! Write-destination exemption for the write-time hook.
//!
//! A third, distinct kind of carve-out, kept apart from both `--exclude`
//! (tree-scan path exclusion) and gitleaks rule allowlists (content-based,
//! every-consumer): a set of write *destinations* that skip the write-time
//! scrub entirely. Consulted only by hook mode. The mechanism ships; the
//! destinations live in a file discovered through an env var, never in a
//! tracked config.
//!
//! Missing config is the safe state -- the exemption's absence only
//! strengthens the gate -- so discovery is env-var-only with no repo-root
//! fallback. Every degenerate configuration is a panic: in hook mode a panic
//! becomes a block, so misconfiguration fails closed rather than widening the
//! gate, and because `load` runs on every hook write whenever the var is set, a
//! broken file is discovered on the first write anywhere.

use crate::config;
use crate::git;
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};

/// Env var naming the write-exemption file. Setting it declares the file
/// required; an empty or non-UTF-8 value is rejected by the caller's strict
/// reader before it reaches here.
pub const EXEMPT_ENV: &str = "BRENN_SCRUB_WRITE_EXEMPT";

/// Canonical, absolute write destinations whose writes skip the write-time
/// scrub, plus the file they were loaded from for audit output.
pub struct WriteExempt {
    roots: Vec<PathBuf>,
    source: PathBuf,
}

/// `None` when the env var is unset. Panics on every degenerate configuration
/// (missing/dangling file, invalid TOML, unexpected or missing key, empty or
/// non-string/relative/nonexistent entry, an entry inside a gated repo, or an
/// entry broad enough to be a typo).
pub fn load(env_value: Option<&str>) -> Option<WriteExempt> {
    let raw = env_value?;
    let source = PathBuf::from(raw);

    match config::candidate_state(&source) {
        config::Candidate::Present => {}
        config::Candidate::Dangling => {
            panic!(
                "{EXEMPT_ENV} points at a dangling symlink: {}",
                source.display()
            )
        }
        config::Candidate::Absent => {
            panic!(
                "{EXEMPT_ENV} is set but its target does not exist: {}",
                source.display()
            )
        }
    }

    let text = std::fs::read_to_string(&source)
        .unwrap_or_else(|e| panic!("cannot read write-exemption file {}: {e}", source.display()));
    let table: toml::Table = text.parse().unwrap_or_else(|e| {
        panic!(
            "write-exemption file {} is not valid TOML: {e}",
            source.display()
        )
    });

    let paths = table.get("paths").unwrap_or_else(|| {
        panic!(
            "write-exemption file {} has no `paths` key",
            source.display()
        )
    });
    for key in table.keys() {
        assert!(
            key == "paths",
            "write-exemption file {} has unexpected key `{key}`; only `paths` is allowed \
             (a typo'd key must not read as no exemptions)",
            source.display()
        );
    }
    let entries = paths.as_array().unwrap_or_else(|| {
        panic!(
            "`paths` in write-exemption file {} must be an array of strings",
            source.display()
        )
    });
    assert!(
        !entries.is_empty(),
        "`paths` in write-exemption file {} is empty; a file that exempts nothing is a typo",
        source.display()
    );

    let home = canonical_home();
    let mut roots = Vec::new();
    for item in entries {
        let raw_entry = item.as_str().unwrap_or_else(|| {
            panic!(
                "every entry in `paths` in write-exemption file {} must be a string",
                source.display()
            )
        });
        let entry = PathBuf::from(raw_entry);
        assert!(
            entry.is_absolute(),
            "write-exemption entry {raw_entry:?} is not absolute; \
             a relative entry can never match a write destination"
        );
        let canonical = std::fs::canonicalize(&entry).unwrap_or_else(|e| {
            panic!(
                "write-exemption entry {raw_entry:?} does not resolve on disk: {e}; \
                 an entry that cannot exist is inert config"
            )
        });
        assert_not_too_broad(&canonical, &home);
        if let Some(gated) = gated_repo_containing(&canonical) {
            panic!(
                "write-exemption entry {} lies inside gated repo {}; carve-outs inside a \
                 gated repo belong in rule allowlists or --exclude, never in the exemption file",
                canonical.display(),
                gated.display()
            );
        }
        roots.push(canonical);
    }

    Some(WriteExempt { roots, source })
}

impl WriteExempt {
    /// The exempt root covering `dest`, or `None`.
    ///
    /// `dest` is absolutized against `cwd` if relative, then symlink-resolved
    /// against the real filesystem (nearest existing ancestor canonicalized,
    /// non-existent tail rejoined) so the decision is made on the destination's
    /// true location. Panics when the resolved tail contains a `..` (no lexical
    /// games around the prefix match) and when a matched destination lands
    /// inside a gated repo -- the destination side of the bidirectional guard.
    ///
    /// The decision reflects the filesystem at check time. The hook returns
    /// before Claude Code performs the write, so a path element swapped between
    /// this resolution and that write (a symlink retargeted, or a hardlink into
    /// gated content aliased under an exempt root) lands the write somewhere
    /// other than what was judged. This check-then-write race is inherent to a
    /// pre-write hook and undetectable by path resolution; on the single-operator
    /// machine it requires the operator to sabotage their own gate, and the
    /// commit and push gates still scan the content before it can be published.
    /// Accepted residual, not closable here.
    pub fn matched_root(&self, dest: &Path, cwd: &Path) -> Option<&Path> {
        let abs = if dest.is_absolute() {
            dest.to_path_buf()
        } else {
            cwd.join(dest)
        };
        let (existing, resolved) = resolve_destination(&abs);

        for root in &self.roots {
            if under_abs(&resolved, root) {
                if let Some(gated) = gated_repo_containing(&existing) {
                    panic!(
                        "write to {} is exempt yet lands inside gated repo {}; the exemption \
                         file and that repo's write gate both claim this destination",
                        resolved.display(),
                        gated.display()
                    );
                }
                return Some(root.as_path());
            }
        }
        None
    }

    /// The file the roots were loaded from, for audit output.
    pub fn source(&self) -> &Path {
        &self.source
    }
}

/// Whether `path` lies under `root`, comparing whole components including the
/// leading root component. A sibling to `exclude::under`, which rejects the
/// non-`Normal` components an absolute root necessarily begins with; both
/// arguments here are canonical absolute paths, so `/a/b` covers `/a/b/**` and
/// `/a/b` itself, never `/a/bc`.
fn under_abs(path: &Path, root: &Path) -> bool {
    let mut p = path.components();
    for r in root.components() {
        if p.next() != Some(r) {
            return false;
        }
    }
    true
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
        // resolved path stay lexically under an exempt root while the real write
        // follows the link elsewhere.
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

/// A gated git repo (one carrying a tracked `.gitleaks.toml`) containing `dir`,
/// if any. A non-repo path and a repo without the config both yield `None` --
/// those are the legitimate ungated-destination shapes.
fn gated_repo_containing(dir: &Path) -> Option<PathBuf> {
    let probe = if dir.is_dir() {
        dir.to_path_buf()
    } else {
        dir.parent()?.to_path_buf()
    };
    let root = git::try_repo_root(&probe)?;
    root.join(config::PUBLIC_FILENAME).exists().then_some(root)
}

/// `$HOME`, canonicalized. An unset or empty value is fatal: the breadth guard
/// cannot judge "covers home" without it, and silently skipping the check would
/// defeat its purpose.
fn canonical_home() -> PathBuf {
    let raw = match std::env::var_os("HOME") {
        Some(v) if !v.is_empty() => v,
        _ => panic!(
            "$HOME is unset or empty; refusing to evaluate the write-exemption breadth guard"
        ),
    };
    std::fs::canonicalize(PathBuf::from(&raw))
        .unwrap_or_else(|e| panic!("cannot canonicalize $HOME ({raw:?}): {e}"))
}

/// A deliberately dumb, lexical typo tripwire against profile-expansion
/// accidents: the filesystem root, any path covering the home directory, and
/// anything with fewer than three normal components are too broad to be a
/// real annex checkout and are refused loud.
fn assert_not_too_broad(entry: &Path, home: &Path) {
    let normals = entry
        .components()
        .filter(|c| matches!(c, Component::Normal(_)))
        .count();
    assert!(
        normals >= 3,
        "write-exemption entry {} is too broad (fewer than three path components); \
         refusing to exempt so wide a destination",
        entry.display()
    );
    assert!(
        !under_abs(home, entry),
        "write-exemption entry {} is too broad (covers the home directory); \
         refusing to exempt so wide a destination",
        entry.display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    impl WriteExempt {
        fn from_roots(roots: Vec<PathBuf>) -> WriteExempt {
            WriteExempt {
                roots,
                source: PathBuf::from("/exempt.toml"),
            }
        }
    }

    /// A tempdir with a nested `a/b/c`, deep enough that the breadth tripwire
    /// never fires on the happy path. Returns (guard, canonical deep dir).
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

    fn git_init(dir: &Path) {
        let out = Command::new("git")
            .current_dir(dir)
            .args(["init", "-q", "."])
            .output()
            .unwrap();
        assert!(out.status.success());
    }

    fn write_exempt_file(dir: &Path, roots: &[&Path]) -> PathBuf {
        let listed = roots
            .iter()
            .map(|p| format!("  \"{}\",", p.display()))
            .collect::<Vec<_>>()
            .join("\n");
        let file = dir.join("exempt.toml");
        std::fs::write(&file, format!("paths = [\n{listed}\n]\n")).unwrap();
        file
    }

    // ---- load: happy paths -------------------------------------------------

    #[test]
    fn unset_env_is_none() {
        assert!(load(None).is_none());
    }

    #[test]
    fn valid_file_yields_canonical_roots() {
        let (_d, deep) = deep_dir();
        let cfgdir = tempfile::tempdir().unwrap();
        let file = write_exempt_file(cfgdir.path(), &[&deep]);
        let loaded = load(Some(file.to_str().unwrap())).unwrap();
        assert_eq!(loaded.roots, vec![deep]);
        assert_eq!(loaded.source(), file);
    }

    #[test]
    fn duplicate_entries_are_tolerated() {
        let (_d, deep) = deep_dir();
        let cfgdir = tempfile::tempdir().unwrap();
        let file = write_exempt_file(cfgdir.path(), &[&deep, &deep]);
        let loaded = load(Some(file.to_str().unwrap())).unwrap();
        assert_eq!(loaded.roots.len(), 2);
    }

    // ---- load: degenerate config panics ------------------------------------

    #[test]
    #[should_panic(expected = "does not exist")]
    fn missing_file_panics() {
        load(Some("/no/such/exemption/file.toml"));
    }

    #[test]
    #[should_panic(expected = "dangling symlink")]
    fn dangling_symlink_panics() {
        let d = tempfile::tempdir().unwrap();
        let link = d.path().join("link.toml");
        std::os::unix::fs::symlink(d.path().join("gone.toml"), &link).unwrap();
        load(Some(link.to_str().unwrap()));
    }

    #[test]
    #[should_panic(expected = "not valid TOML")]
    fn invalid_toml_panics() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("x.toml");
        std::fs::write(&file, "this is not toml {{{").unwrap();
        load(Some(file.to_str().unwrap()));
    }

    #[test]
    #[should_panic(expected = "unexpected key")]
    fn extra_key_panics() {
        let (_d, deep) = deep_dir();
        let cfgdir = tempfile::tempdir().unwrap();
        let file = cfgdir.path().join("x.toml");
        std::fs::write(
            &file,
            format!("paths = [\"{}\"]\nextra = 1\n", deep.display()),
        )
        .unwrap();
        load(Some(file.to_str().unwrap()));
    }

    #[test]
    #[should_panic(expected = "has no `paths` key")]
    fn missing_paths_key_panics() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("x.toml");
        std::fs::write(&file, "path = [\"/a/b/c\"]\n").unwrap();
        load(Some(file.to_str().unwrap()));
    }

    #[test]
    #[should_panic(expected = "must be an array of strings")]
    fn paths_not_an_array_panics() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("x.toml");
        std::fs::write(&file, "paths = \"nope\"\n").unwrap();
        load(Some(file.to_str().unwrap()));
    }

    #[test]
    #[should_panic(expected = "is empty")]
    fn empty_paths_panics() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("x.toml");
        std::fs::write(&file, "paths = []\n").unwrap();
        load(Some(file.to_str().unwrap()));
    }

    #[test]
    #[should_panic(expected = "must be a string")]
    fn non_string_entry_panics() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("x.toml");
        std::fs::write(&file, "paths = [42]\n").unwrap();
        load(Some(file.to_str().unwrap()));
    }

    #[test]
    #[should_panic(expected = "is not absolute")]
    fn relative_entry_panics() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("x.toml");
        std::fs::write(&file, "paths = [\"relative/path\"]\n").unwrap();
        load(Some(file.to_str().unwrap()));
    }

    #[test]
    #[should_panic(expected = "does not resolve on disk")]
    fn nonexistent_entry_panics() {
        let d = tempfile::tempdir().unwrap();
        let file = d.path().join("x.toml");
        std::fs::write(&file, "paths = [\"/no/such/deep/path/here\"]\n").unwrap();
        load(Some(file.to_str().unwrap()));
    }

    // ---- gated-repo guard, entry side --------------------------------------

    #[test]
    #[should_panic(expected = "lies inside gated repo")]
    fn entry_inside_a_gated_repo_panics() {
        let d = tempfile::tempdir().unwrap();
        let repo = d.path().join("x/y/repo");
        std::fs::create_dir_all(&repo).unwrap();
        git_init(&repo);
        std::fs::write(repo.join(config::PUBLIC_FILENAME), "title = \"g\"\n").unwrap();
        let canonical = std::fs::canonicalize(&repo).unwrap();
        let cfgdir = tempfile::tempdir().unwrap();
        let file = write_exempt_file(cfgdir.path(), &[&canonical]);
        load(Some(file.to_str().unwrap()));
    }

    #[test]
    fn entry_inside_an_ungated_repo_is_ok() {
        let d = tempfile::tempdir().unwrap();
        let repo = d.path().join("x/y/repo");
        std::fs::create_dir_all(&repo).unwrap();
        git_init(&repo);
        let canonical = std::fs::canonicalize(&repo).unwrap();
        let cfgdir = tempfile::tempdir().unwrap();
        let file = write_exempt_file(cfgdir.path(), &[&canonical]);
        assert!(load(Some(file.to_str().unwrap())).is_some());
    }

    #[test]
    fn entry_in_a_plain_directory_is_ok() {
        let (_d, deep) = deep_dir();
        let cfgdir = tempfile::tempdir().unwrap();
        let file = write_exempt_file(cfgdir.path(), &[&deep]);
        assert!(load(Some(file.to_str().unwrap())).is_some());
    }

    // ---- breadth tripwire (lexical, no filesystem) -------------------------

    #[test]
    #[should_panic(expected = "too broad")]
    fn filesystem_root_is_too_broad() {
        assert_not_too_broad(Path::new("/"), Path::new("/home/alice"));
    }

    #[test]
    #[should_panic(expected = "too broad")]
    fn one_component_entry_is_too_broad() {
        assert_not_too_broad(Path::new("/home"), Path::new("/home/alice"));
    }

    #[test]
    #[should_panic(expected = "too broad")]
    fn home_itself_is_too_broad() {
        assert_not_too_broad(Path::new("/home/alice"), Path::new("/home/alice"));
    }

    #[test]
    #[should_panic(expected = "too broad")]
    fn a_two_component_entry_is_too_broad() {
        assert_not_too_broad(Path::new("/srv/data"), Path::new("/home/alice"));
    }

    #[test]
    #[should_panic(expected = "too broad")]
    fn an_ancestor_of_home_is_too_broad() {
        // Three components, so the count check passes; caught by covering home.
        assert_not_too_broad(
            Path::new("/home/alice/src"),
            Path::new("/home/alice/src/annex/deep"),
        );
    }

    #[test]
    fn a_deep_annex_shaped_path_is_fine() {
        assert_not_too_broad(Path::new("/home/alice/src/annex"), Path::new("/home/alice"));
        assert_not_too_broad(
            Path::new("/srv/data/annex/checkout"),
            Path::new("/home/alice"),
        );
    }

    // ---- matched_root: component boundaries --------------------------------

    #[test]
    fn matches_the_root_itself_and_nested_children() {
        let (_d, root) = deep_dir();
        let ex = WriteExempt::from_roots(vec![root.clone()]);
        let cwd = Path::new("/");
        assert_eq!(ex.matched_root(&root, cwd), Some(root.as_path()));
        let child = root.join("sub/newfile.rs");
        assert_eq!(ex.matched_root(&child, cwd), Some(root.as_path()));
    }

    #[test]
    fn prefix_look_alikes_do_not_match() {
        let d = tempfile::tempdir().unwrap();
        let annex = d.path().join("x/y/annex");
        let notes = d.path().join("x/y/annex-notes");
        std::fs::create_dir_all(&annex).unwrap();
        std::fs::create_dir_all(&notes).unwrap();
        let root = std::fs::canonicalize(&annex).unwrap();
        let ex = WriteExempt::from_roots(vec![root]);
        let dest = std::fs::canonicalize(&notes).unwrap().join("f.rs");
        assert!(ex.matched_root(&dest, Path::new("/")).is_none());
    }

    #[test]
    fn a_nonexistent_tail_under_the_root_matches() {
        let (_d, root) = deep_dir();
        let ex = WriteExempt::from_roots(vec![root.clone()]);
        let dest = root.join("brand/new/tree/file.rs");
        assert_eq!(ex.matched_root(&dest, Path::new("/")), Some(root.as_path()));
    }

    #[test]
    #[should_panic(expected = "non-existent tail")]
    fn a_dotdot_in_the_nonexistent_tail_panics() {
        let (_d, root) = deep_dir();
        let ex = WriteExempt::from_roots(vec![root.clone()]);
        let dest = root.join("nope/../evil");
        ex.matched_root(&dest, Path::new("/"));
    }

    #[test]
    fn a_relative_destination_is_absolutized_against_cwd() {
        let (_d, root) = deep_dir();
        let ex = WriteExempt::from_roots(vec![root.clone()]);
        assert_eq!(
            ex.matched_root(Path::new("newfile.rs"), &root),
            Some(root.as_path())
        );
    }

    // ---- matched_root: symlink resolution ----------------------------------

    #[test]
    fn a_destination_symlinking_out_of_the_root_is_not_exempt() {
        let (_d, root) = deep_dir();
        let outside = tempfile::tempdir().unwrap();
        let outside_dir = std::fs::canonicalize(outside.path()).unwrap();
        // A symlink inside the exempt root pointing at a directory outside it.
        let link = root.join("escape");
        std::os::unix::fs::symlink(&outside_dir, &link).unwrap();
        let ex = WriteExempt::from_roots(vec![root]);
        let dest = link.join("f.rs");
        assert!(ex.matched_root(&dest, Path::new("/")).is_none());
    }

    #[test]
    #[should_panic(expected = "cannot canonicalize")]
    fn a_dangling_symlink_in_the_tail_blocks_rather_than_matching() {
        let (_d, root) = deep_dir();
        // A dangling link inside the exempt root: `exists()` would report it
        // missing and let the resolved path stay lexically under the root, but
        // the real write would follow the link to its (absent) target.
        let link = root.join("dangling");
        std::os::unix::fs::symlink(root.join("no/such/target"), &link).unwrap();
        let ex = WriteExempt::from_roots(vec![root]);
        ex.matched_root(&link, Path::new("/"));
    }

    #[test]
    fn a_destination_symlinking_into_the_root_is_exempt() {
        let (_d, root) = deep_dir();
        let outside = tempfile::tempdir().unwrap();
        // A symlink outside the root pointing at a directory inside it.
        let inside = root.join("real");
        std::fs::create_dir_all(&inside).unwrap();
        let link = outside.path().join("into");
        std::os::unix::fs::symlink(&inside, &link).unwrap();
        let ex = WriteExempt::from_roots(vec![root.clone()]);
        let dest = link.join("f.rs");
        assert_eq!(ex.matched_root(&dest, Path::new("/")), Some(root.as_path()));
    }

    // ---- gated-repo guard, destination side --------------------------------

    #[test]
    #[should_panic(expected = "both claim this destination")]
    fn a_destination_inside_a_nested_gated_repo_panics() {
        let (_d, root) = deep_dir();
        let inner = root.join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        git_init(&inner);
        std::fs::write(inner.join(config::PUBLIC_FILENAME), "title = \"g\"\n").unwrap();
        let ex = WriteExempt::from_roots(vec![root]);
        ex.matched_root(&inner.join("f.rs"), Path::new("/"));
    }

    #[test]
    fn a_destination_in_a_non_repo_sibling_matches_normally() {
        let (_d, root) = deep_dir();
        let inner = root.join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        git_init(&inner);
        std::fs::write(inner.join(config::PUBLIC_FILENAME), "title = \"g\"\n").unwrap();
        let sibling = root.join("plain");
        std::fs::create_dir_all(&sibling).unwrap();
        let ex = WriteExempt::from_roots(vec![root.clone()]);
        assert_eq!(
            ex.matched_root(&sibling.join("f.rs"), Path::new("/")),
            Some(root.as_path())
        );
    }

    // ---- message neutrality ------------------------------------------------

    #[test]
    fn every_panic_message_is_neutral() {
        use crate::message::neutral::assert_neutral;

        // Load-time degenerate configs, one per distinct static template.
        assert_neutral(
            &panic_text(|| {
                load(Some("/no/such/exemption/file.toml"));
            }),
            "missing file",
        );
        assert_neutral(
            &panic_text(|| {
                let d = tempfile::tempdir().unwrap();
                let f = d.path().join("x.toml");
                std::fs::write(&f, "nope {{{").unwrap();
                load(Some(f.to_str().unwrap()));
            }),
            "invalid toml",
        );
        assert_neutral(
            &panic_text(|| {
                let d = tempfile::tempdir().unwrap();
                let f = d.path().join("x.toml");
                std::fs::write(&f, "paths = [\"/a/b/c\"]\nextra = 1\n").unwrap();
                load(Some(f.to_str().unwrap()));
            }),
            "extra key",
        );
        assert_neutral(
            &panic_text(|| {
                let d = tempfile::tempdir().unwrap();
                let f = d.path().join("x.toml");
                std::fs::write(&f, "path = [\"/a/b/c\"]\n").unwrap();
                load(Some(f.to_str().unwrap()));
            }),
            "missing paths key",
        );
        assert_neutral(
            &panic_text(|| {
                let d = tempfile::tempdir().unwrap();
                let f = d.path().join("x.toml");
                std::fs::write(&f, "paths = []\n").unwrap();
                load(Some(f.to_str().unwrap()));
            }),
            "empty paths",
        );
        assert_neutral(
            &panic_text(|| {
                let d = tempfile::tempdir().unwrap();
                let f = d.path().join("x.toml");
                std::fs::write(&f, "paths = [\"relative/path\"]\n").unwrap();
                load(Some(f.to_str().unwrap()));
            }),
            "relative entry",
        );
        assert_neutral(
            &panic_text(|| {
                let d = tempfile::tempdir().unwrap();
                let f = d.path().join("x.toml");
                std::fs::write(&f, "paths = [\"/no/such/deep/entry/here\"]\n").unwrap();
                load(Some(f.to_str().unwrap()));
            }),
            "nonexistent entry",
        );

        // Breadth tripwire and both sides of the gated-repo guard.
        assert_neutral(
            &panic_text(|| assert_not_too_broad(Path::new("/home"), Path::new("/home/alice"))),
            "too broad",
        );
        assert_neutral(
            &panic_text(|| {
                let d = tempfile::tempdir().unwrap();
                let repo = d.path().join("x/y/repo");
                std::fs::create_dir_all(&repo).unwrap();
                git_init(&repo);
                std::fs::write(repo.join(config::PUBLIC_FILENAME), "title = \"g\"\n").unwrap();
                let canonical = std::fs::canonicalize(&repo).unwrap();
                let cfgdir = tempfile::tempdir().unwrap();
                let file = write_exempt_file(cfgdir.path(), &[&canonical]);
                load(Some(file.to_str().unwrap()));
            }),
            "gated entry",
        );
        assert_neutral(
            &panic_text(|| {
                let (_d, root) = deep_dir();
                let inner = root.join("inner");
                std::fs::create_dir_all(&inner).unwrap();
                git_init(&inner);
                std::fs::write(inner.join(config::PUBLIC_FILENAME), "title = \"g\"\n").unwrap();
                let ex = WriteExempt::from_roots(vec![root]);
                ex.matched_root(&inner.join("f.rs"), Path::new("/"));
            }),
            "gated destination",
        );
        assert_neutral(
            &panic_text(|| {
                let (_d, root) = deep_dir();
                let ex = WriteExempt::from_roots(vec![root.clone()]);
                ex.matched_root(&root.join("nope/../evil"), Path::new("/"));
            }),
            "dotdot tail",
        );
    }

    #[test]
    fn a_destination_in_a_nested_ungated_repo_matches_normally() {
        let (_d, root) = deep_dir();
        let inner = root.join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        git_init(&inner);
        let ex = WriteExempt::from_roots(vec![root.clone()]);
        assert_eq!(
            ex.matched_root(&inner.join("f.rs"), Path::new("/")),
            Some(root.as_path())
        );
    }
}

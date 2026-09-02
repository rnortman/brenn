//! Set equality between the Bazel policy scan's file set and the tracked tree.
//!
//! The guards scan `//:all_policy_srcs`, a hand-aggregated list of one
//! `filegroup` per Bazel package; the workspace side scans `git ls-files`. Nothing
//! inside the sandbox can compare the two — a policy test sees neither git nor
//! `bazel query` — so this runs outside it, over a manifest the build produced
//! and the tracked listing beside it.
//!
//! Equality in both directions, never containment. A package added without
//! joining the aggregate drops its whole directory out of the scan and every
//! guard keeps passing over the smaller set. A tracked file that lands under
//! one of the glob's exclusions leaves the scan the same silent way. And an
//! untracked file joins the Bazel set alone, so the two verdicts diverge on any
//! dirty tree — which is why this is its own step over the workspace rather
//! than a test inside the Bazel graph.
//!
//! Symlinks are dropped from both sides. Bazel's glob does not follow the
//! tracked `host-crates/` links, and a symlink carries no content for a
//! content-scanning guard to read, so its absence from one side is not drift.
//!
//! Nested Bazel workspaces are dropped from the git side. A directory holding
//! its own `MODULE.bazel` is a repository boundary: no glob in this module can
//! enter it and no label in this module's graph can name a file inside it, so
//! the Bazel side never lists it and git always does. The set of such
//! directories is [`NESTED_WORKSPACES`], and because this is the one lane that
//! sees the real tree, the facts a nested workspace copies from brenn — the
//! Bazel version, the Rust version, the `fltk` commit — are held equal to
//! brenn's here as well.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::file_set;

/// Directories that are Bazel root modules of their own, repo-root-relative.
///
/// Each is subtracted from the tracked side before the comparison, must be
/// listed in `.bazelignore` (Bazel already treats it as a boundary; the listing
/// states the intent), must hold a `MODULE.bazel`, and must pin the same
/// `.bazelversion`, `RUST_VERSION` and fltk commit brenn does, since a consumer
/// copies all three and rules_rust and bzlmod read them from the root module
/// only.
const NESTED_WORKSPACES: &[&str] = &["examples/component"];

/// Compare the two listings, printing every path either side is missing, and
/// hold every nested workspace to its pins.
pub fn run_policy_parity(root: &Path, manifest: &Path) -> bool {
    let mut found = nested_workspace_violations(root, NESTED_WORKSPACES);
    let tracked = outside_nested(file_set::from_git(root), NESTED_WORKSPACES);
    let declared = file_set::from_manifest(manifest);
    found.extend(parity_violations(
        root,
        tracked,
        declared,
        file_set::MIN_SCANNED_FILES,
        &mut |count| println!("policy scan parity: {count} paths, both sides equal"),
    ));
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

/// The listing with every path under a nested workspace removed.
fn outside_nested(files: Vec<PathBuf>, nested: &[&str]) -> Vec<PathBuf> {
    files
        .into_iter()
        .filter(|rel| !nested.iter().any(|ws| rel.starts_with(ws)))
        .collect()
}

/// Everything wrong with the nested workspaces: a listed directory with no
/// `MODULE.bazel`, one absent from `.bazelignore`, and every pin that differs
/// from brenn's or cannot be read.
fn nested_workspace_violations(root: &Path, nested: &[&str]) -> Vec<String> {
    let mut found = Vec::new();
    if nested.is_empty() {
        return found;
    }
    let bazelignore = read(root, ".bazelignore");
    let brenn_bazelversion = bazel_version(&read(root, ".bazelversion"));
    let brenn_module = read(root, "MODULE.bazel");
    let brenn_rust = rust_version(&brenn_module);
    let brenn_fltk = fltk_commit(&brenn_module);
    for ws in nested {
        let dir = root.join(ws);
        if !dir.join("MODULE.bazel").is_file() {
            found.push(format!(
                "{ws}: listed in NESTED_WORKSPACES but holds no MODULE.bazel; it is an ordinary \
                 directory, and subtracting it hides its files from the parity check"
            ));
            continue;
        }
        if !bazelignore_lists(&bazelignore, ws) {
            found.push(format!(
                "{ws}: a nested Bazel workspace that .bazelignore does not list; add the line"
            ));
        }
        let pins = [
            (
                ".bazelversion",
                &brenn_bazelversion,
                bazel_version(&read(&dir, ".bazelversion")),
            ),
            (
                "RUST_VERSION",
                &brenn_rust,
                rust_version(&read(&dir, "MODULE.bazel")),
            ),
            (
                "fltk commit",
                &brenn_fltk,
                fltk_commit(&read(&dir, "MODULE.bazel")),
            ),
        ];
        for (what, brenn, theirs) in pins {
            match (brenn, theirs) {
                (Err(e), _) => found.push(format!("{ws}: cannot compare {what}: brenn's {e}")),
                (Ok(_), Err(e)) => found.push(format!("{ws}: cannot read its {what}: {e}")),
                (Ok(b), Ok(t)) if *b != t => found.push(format!(
                    "{ws}: pins {what} {t:?} but brenn pins {b:?}; a consumer copies this pin, \
                     so the copy that lives here must equal brenn's"
                )),
                (Ok(_), Ok(_)) => {}
            }
        }
    }
    found
}

fn read(dir: &Path, rel: &str) -> String {
    let path = dir.join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("policy scan parity: cannot read {}: {e}", path.display()))
}

/// Whether `.bazelignore` names `path` as an entry: a non-comment line equal to
/// it, whitespace trimmed.
fn bazelignore_lists(text: &str, path: &str) -> bool {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .any(|line| line == path)
}

/// The one non-empty line of a `.bazelversion`.
fn bazel_version(text: &str) -> Result<String, String> {
    let mut lines = text.lines().map(str::trim).filter(|l| !l.is_empty());
    match (lines.next(), lines.next()) {
        (Some(v), None) => Ok(v.to_owned()),
        (None, _) => Err(".bazelversion is empty".to_owned()),
        (Some(_), Some(_)) => Err(".bazelversion has more than one line".to_owned()),
    }
}

/// The value of a `MODULE.bazel`'s `RUST_VERSION = "..."` line.
fn rust_version(text: &str) -> Result<String, String> {
    let anchor = "RUST_VERSION = \"";
    let mut hits = text
        .lines()
        .filter_map(|line| line.strip_prefix(anchor)?.strip_suffix('"'));
    match (hits.next(), hits.next()) {
        (Some(v), None) => Ok(v.to_owned()),
        (None, _) => Err(format!("MODULE.bazel has no line matching `{anchor}...\"`")),
        (Some(_), Some(_)) => Err("MODULE.bazel assigns RUST_VERSION more than once".to_owned()),
    }
}

/// The `commit` of a `MODULE.bazel`'s `git_override(module_name = "fltk", ...)`
/// block: the lines from a `git_override(` line to the next `)` line.
fn fltk_commit(text: &str) -> Result<String, String> {
    let mut block: Option<Vec<&str>> = None;
    let mut commit = None;
    for line in text.lines() {
        let trimmed = line.trim();
        match &mut block {
            None if trimmed == "git_override(" => block = Some(Vec::new()),
            None => {}
            Some(lines) if trimmed == ")" => {
                if lines.contains(&"module_name = \"fltk\",") {
                    let found = lines
                        .iter()
                        .filter_map(|l| l.strip_prefix("commit = \"")?.strip_suffix("\","))
                        .collect::<Vec<_>>();
                    match (found.first(), found.len()) {
                        (Some(c), 1) if commit.is_none() => commit = Some((*c).to_owned()),
                        (Some(_), 1) => {
                            return Err("MODULE.bazel overrides fltk more than once".to_owned());
                        }
                        _ => {
                            return Err(
                                "the fltk git_override has no single `commit = \"...\",` line"
                                    .to_owned(),
                            );
                        }
                    }
                }
                block = None;
            }
            Some(lines) => lines.push(trimmed),
        }
    }
    commit.ok_or_else(|| {
        "MODULE.bazel has no `git_override(` block with `module_name = \"fltk\",`".to_owned()
    })
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
            "{} is in the Bazel scan but is not tracked: the two scans read different \
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

    #[test]
    fn paths_under_a_nested_workspace_leave_the_git_side() {
        let files = vec![
            PathBuf::from("a/one.rs"),
            PathBuf::from("examples/component/BUILD.bazel"),
            PathBuf::from("examples/component/src/lib.rs"),
            PathBuf::from("examples/componentx/other.rs"),
        ];
        assert_eq!(
            outside_nested(files, &["examples/component"]),
            vec![
                PathBuf::from("a/one.rs"),
                PathBuf::from("examples/componentx/other.rs"),
            ]
        );
    }

    #[test]
    fn bazelignore_lists_entries_not_comments_or_prefixes() {
        let text = "# examples/component
.git
  examples/component  
";
        assert!(bazelignore_lists(text, "examples/component"));
        assert!(!bazelignore_lists(text, "examples"));
        assert!(!bazelignore_lists(
            "# examples/component
",
            "examples/component"
        ));
    }

    #[test]
    fn bazel_version_is_the_one_line() {
        assert_eq!(bazel_version("9.2.0\n"), Ok("9.2.0".to_owned()));
        assert!(bazel_version("\n").unwrap_err().contains("empty"));
        assert!(
            bazel_version("9.2.0\n9.1.0\n")
                .unwrap_err()
                .contains("more than one")
        );
    }

    const MODULE: &str = r#"module(name = "x")
bazel_dep(name = "fltk", version = "")
git_override(
    module_name = "fltk",
    commit = "555824e6b1dee22161260c2843bd4dec68efd11a",
    remote = "https://github.com/rnortman/fltk.git",
)
RUST_VERSION = "1.95.0"
git_override(
    module_name = "other",
    commit = "0000000000000000000000000000000000000000",
    remote = "https://github.com/rnortman/other.git",
)
"#;

    #[test]
    fn rust_version_is_the_anchored_assignment() {
        assert_eq!(rust_version(MODULE), Ok("1.95.0".to_owned()));
        assert!(
            rust_version("rust_version = \"1.95.0\"\n")
                .unwrap_err()
                .contains("no line")
        );
        assert!(
            rust_version(&format!("{MODULE}RUST_VERSION = \"1.0.0\"\n"))
                .unwrap_err()
                .contains("more than once")
        );
    }

    #[test]
    fn fltk_commit_is_read_from_the_fltk_override_block_only() {
        assert_eq!(
            fltk_commit(MODULE),
            Ok("555824e6b1dee22161260c2843bd4dec68efd11a".to_owned())
        );
        let no_fltk = MODULE.replace("module_name = \"fltk\"", "module_name = \"fltq\"");
        assert!(
            fltk_commit(&no_fltk)
                .unwrap_err()
                .contains("no `git_override(`")
        );
        let no_commit = MODULE.replacen("    commit = ", "    # commit = ", 1);
        assert!(fltk_commit(&no_commit).unwrap_err().contains("no single"));
        let twice = format!("{MODULE}{MODULE}");
        assert!(fltk_commit(&twice).unwrap_err().contains("more than once"));
    }

    /// A brenn-shaped root with one nested workspace whose pins are `theirs`.
    fn nested_tree(bazelignore: &str, theirs: (&str, &str, &str)) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join(".bazelignore"), bazelignore).unwrap();
        std::fs::write(root.join(".bazelversion"), "9.2.0\n").unwrap();
        std::fs::write(root.join("MODULE.bazel"), MODULE).unwrap();
        let ws = root.join("examples/component");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(ws.join(".bazelversion"), format!("{}\n", theirs.0)).unwrap();
        let module = MODULE
            .replace("1.95.0", theirs.1)
            .replace("555824e6b1dee22161260c2843bd4dec68efd11a", theirs.2);
        std::fs::write(ws.join("MODULE.bazel"), module).unwrap();
        dir
    }

    const PINS: (&str, &str, &str) = (
        "9.2.0",
        "1.95.0",
        "555824e6b1dee22161260c2843bd4dec68efd11a",
    );

    #[test]
    fn a_nested_workspace_with_equal_pins_and_an_ignore_line_passes() {
        let dir = nested_tree("examples/component\n", PINS);
        let out = nested_workspace_violations(dir.path(), &["examples/component"]);
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn a_nested_workspace_missing_from_bazelignore_is_reported() {
        let dir = nested_tree(".git\n", PINS);
        let out = nested_workspace_violations(dir.path(), &["examples/component"]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains(".bazelignore does not list"), "{}", out[0]);
    }

    #[test]
    fn a_listed_directory_with_no_module_file_is_reported() {
        let dir = nested_tree("examples/component\n", PINS);
        std::fs::remove_file(dir.path().join("examples/component/MODULE.bazel")).unwrap();
        let out = nested_workspace_violations(dir.path(), &["examples/component"]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].contains("holds no MODULE.bazel"), "{}", out[0]);
    }

    #[test]
    fn every_differing_pin_is_reported_by_name() {
        let dir = nested_tree(
            "examples/component\n",
            (
                "9.1.0",
                "1.94.0",
                "1111111111111111111111111111111111111111",
            ),
        );
        let out = nested_workspace_violations(dir.path(), &["examples/component"]);
        assert_eq!(out.len(), 3, "{out:?}");
        assert!(out[0].contains(".bazelversion \"9.1.0\""), "{}", out[0]);
        assert!(out[1].contains("RUST_VERSION \"1.94.0\""), "{}", out[1]);
        assert!(out[2].contains("fltk commit \"1111"), "{}", out[2]);
    }

    #[test]
    fn a_pin_the_anchor_cannot_find_is_a_failure_not_a_skip() {
        let dir = nested_tree("examples/component\n", PINS);
        let module = dir.path().join("examples/component/MODULE.bazel");
        let text = std::fs::read_to_string(&module).unwrap();
        std::fs::write(&module, text.replace("RUST_VERSION = ", "RUST_VERSION= ")).unwrap();
        let out = nested_workspace_violations(dir.path(), &["examples/component"]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].contains("cannot read its RUST_VERSION"),
            "{}",
            out[0]
        );
    }

    #[test]
    fn a_pin_brenn_itself_has_lost_is_blamed_on_brenn() {
        let dir = nested_tree("examples/component\n", PINS);
        let module = dir.path().join("MODULE.bazel");
        let text = std::fs::read_to_string(&module).unwrap();
        std::fs::write(&module, text.replace("RUST_VERSION = ", "RUST_VERSION= ")).unwrap();
        let out = nested_workspace_violations(dir.path(), &["examples/component"]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].contains("cannot compare RUST_VERSION: brenn's"),
            "{}",
            out[0]
        );
    }
}

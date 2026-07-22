//! Git interrogation and parsing of the two textual git contracts this tool
//! consumes: staged diff output, and pre-push stdin.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Run `git <args>` in `repo`, returning stdout. Inherits the caller's environment.
pub fn run(repo: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .expect("failed to execute git");
    assert!(
        out.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr).trim()
    );
    // Not lossy: `-z` output is raw path bytes, and replacing an
    // unrepresentable byte with U+FFFD yields a path that names no file. Every
    // downstream use would then quietly scan nothing for it, so an
    // unrepresentable name is fatal rather than skipped.
    String::from_utf8(out.stdout)
        .unwrap_or_else(|_| panic!("git {} emitted non-UTF-8 output", args.join(" ")))
}

fn try_run(repo: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

pub fn repo_root(from: &Path) -> PathBuf {
    let out = Command::new("git")
        .current_dir(from)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .expect("failed to execute git");
    assert!(
        out.status.success(),
        "not inside a git repository: {}",
        from.display()
    );
    // Strip only the trailing newline, not `trim()`: a repo dirname ending in
    // whitespace is legal, and trimming it would yield a path naming no repo.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let root = stdout
        .strip_suffix('\n')
        .expect("git rev-parse --show-toplevel output was not newline-terminated");
    PathBuf::from(root)
}

/// The repo root containing `from`, or `None` when `from` is genuinely inside
/// no git repo. A resolution failure inside a security guard must fail closed
/// rather than read as "not a repo" and pass, so only two outcomes are
/// tolerated: a clean success (the repo root) or a clean "not a git repository"
/// (`None`). Every other non-zero exit -- dubious ownership, a locked or corrupt
/// repo, a probe stranded inside `.git` -- is a real error that panics rather
/// than collapsing into the not-a-repo answer and silently exempting a write.
/// `LC_ALL=C` pins the message so the recognition of the not-a-repo case does
/// not depend on the operator's locale. A failed spawn or non-UTF-8 output is
/// likewise fatal.
///
/// This function's `None` means "pass this write unscanned", so repo identity
/// must be a pure function of the probe directory. The inherited git discovery
/// variables (`GIT_DIR`, `GIT_WORK_TREE`, `GIT_CEILING_DIRECTORIES`,
/// `GIT_COMMON_DIR`, `GIT_INDEX_FILE`) would let a stray `export` in the
/// session's environment redirect discovery -- reporting the wrong root, or
/// halting it with a clean "not a git repository" that reads as ungated -- so
/// they are stripped from the child.
pub fn try_repo_root(from: &Path) -> Option<PathBuf> {
    let out = Command::new("git")
        .current_dir(from)
        .env("LC_ALL", "C")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_CEILING_DIRECTORIES")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_INDEX_FILE")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .expect("failed to execute git");
    if out.status.success() {
        let stdout = String::from_utf8(out.stdout)
            .unwrap_or_else(|_| panic!("git rev-parse --show-toplevel emitted non-UTF-8 output"));
        // Strip only the trailing newline, not `trim()`: a repo dirname ending
        // in whitespace is legal, and trimming it would yield a path naming no
        // repo whose config probe then reads absent -- a silent fail-open.
        let root = stdout
            .strip_suffix('\n')
            .expect("git rev-parse --show-toplevel output was not newline-terminated");
        return Some(PathBuf::from(root));
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    if stderr.contains("not a git repository") {
        return None;
    }
    panic!(
        "git rev-parse --show-toplevel failed in {} (not a clean 'not a repository', \
         so failing closed): {}",
        from.display(),
        stderr.trim()
    );
}

/// Tracked files, optionally scoped to a path.
pub fn tracked_files(repo: &Path, scope: Option<&str>) -> Vec<PathBuf> {
    let mut args = vec!["ls-files", "-z"];
    if let Some(s) = scope {
        args.push("--");
        args.push(s);
    }
    run(repo, &args)
        .split('\0')
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect()
}

/// Tracked paths with no file in the worktree: deleted, but not yet committed.
/// The only legitimate reason a tracked path is absent from disk.
pub fn deleted_files(repo: &Path) -> Vec<PathBuf> {
    run(repo, &["ls-files", "-z", "--deleted"])
        .split('\0')
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect()
}

/// Staged paths, taken from `--name-only -z` rather than from diff headers.
/// NUL separation means no quoting or escaping is ever applied to the names.
pub fn staged_files(repo: &Path) -> Vec<PathBuf> {
    run(repo, &["diff", "--cached", "--name-only", "-z"])
        .split('\0')
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect()
}

/// The staged diff for exactly one path.
pub fn staged_diff_for(repo: &Path, path: &Path) -> String {
    run(
        repo,
        &[
            "diff",
            "--cached",
            "-U0",
            "--no-color",
            "--",
            &path.to_string_lossy(),
        ],
    )
}

/// Added text of a single-file unified diff.
///
/// Only hunk bodies are read, and a hunk starts at a `@@ ` line in column
/// zero. Every line inside a hunk carries a `+`/`-`/space prefix, so no
/// content can reach column zero looking like a header: an added line whose
/// own text is `++ /dev/null` arrives as `+++ /dev/null` and is read as the
/// content it is. Inferring file identity from `+++ ` headers instead would
/// let such a line retarget or discard the rest of the scan.
///
/// Deletions contribute nothing; only added text is ever scanned.
pub fn added_lines(diff: &str) -> String {
    let mut added = String::new();
    let mut in_hunk = false;
    for line in diff.lines() {
        if line.starts_with("@@ ") {
            in_hunk = true;
            continue;
        }
        if !in_hunk {
            continue;
        }
        if let Some(text) = line.strip_prefix('+') {
            added.push_str(text);
            added.push('\n');
        }
    }
    added
}

fn is_zero_sha(sha: &str) -> bool {
    !sha.is_empty() && sha.chars().all(|c| c == '0')
}

/// One line of the pre-push stdin contract.
#[derive(Debug, PartialEq, Eq)]
pub enum RefUpdate {
    /// Ref being deleted: nothing arrives at the remote, nothing to scan.
    Delete,
    /// Ref new to the remote: no remote SHA to bound the range against.
    New {
        local: String,
    },
    Update {
        remote: String,
        local: String,
    },
}

/// Parse `<local ref> <local sha> <remote ref> <remote sha>` lines.
pub fn parse_push_refs(stdin: &str) -> Vec<RefUpdate> {
    stdin
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let f: Vec<&str> = line.split_whitespace().collect();
            assert!(
                f.len() == 4,
                "malformed pre-push stdin line (expected 4 fields): {line:?}"
            );
            let (local_sha, remote_sha) = (f[1], f[3]);
            if is_zero_sha(local_sha) {
                RefUpdate::Delete
            } else if is_zero_sha(remote_sha) {
                RefUpdate::New {
                    local: local_sha.to_string(),
                }
            } else {
                RefUpdate::Update {
                    remote: remote_sha.to_string(),
                    local: local_sha.to_string(),
                }
            }
        })
        .collect()
}

/// Whether a commit object is present locally.
fn commit_exists(repo: &Path, sha: &str) -> bool {
    try_run(repo, &["cat-file", "-e", &format!("{sha}^{{commit}}")]).is_some()
}

/// Range bounded by the merge-base with the default branch; unrelated history
/// has no merge-base, so the whole reachable history gets scanned rather than
/// nothing.
fn merge_base_opts(repo: &Path, local: &str) -> String {
    match default_branch_ref(repo) {
        Some(base_ref) => match try_run(repo, &["merge-base", &base_ref, local]) {
            Some(base) if !base.is_empty() => format!("{base}..{local}"),
            _ => local.to_string(),
        },
        None => local.to_string(),
    }
}

/// `git log` range for a ref update.
///
/// The remote SHA is only usable as a bound if it is in the local object
/// store. The pre-push contract permits it not to be -- the remote can have
/// advanced from another clone since the last fetch -- and a range against a
/// missing object makes `git log` fail, which would abort the push even under
/// `--warn-only`. Falling back to the merge-base bound over-scans, which is
/// safe; failing the push over a stale local view is not.
pub fn log_opts_for(repo: &Path, update: &RefUpdate) -> Option<String> {
    match update {
        RefUpdate::Delete => None,
        RefUpdate::Update { remote, local } if commit_exists(repo, remote) => {
            Some(format!("{remote}..{local}"))
        }
        RefUpdate::Update { local, .. } | RefUpdate::New { local } => {
            Some(merge_base_opts(repo, local))
        }
    }
}

fn default_branch_ref(repo: &Path) -> Option<String> {
    try_run(
        repo,
        &["symbolic-ref", "--short", "refs/remotes/origin/HEAD"],
    )
    .filter(|s| !s.is_empty())
    .or_else(|| {
        ["origin/main", "origin/master", "main", "master"]
            .into_iter()
            .find(|r| try_run(repo, &["rev-parse", "--verify", "--quiet", r]).is_some())
            .map(str::to_string)
    })
}

/// Extract a commit's tree into `dest`.
///
/// The scratch archive lives outside `dest`. Written inside it, a tree that
/// tracks a file of the same name would overwrite the archive mid-read and
/// then have its own content deleted from the mirror -- an attacker-choosable
/// filename that either crashes the push or exempts itself from the scan.
pub fn extract_tree(repo: &Path, sha: &str, dest: &Path) {
    let tar = tempfile::Builder::new()
        .prefix("brenn-scrub-archive")
        .suffix(".tar")
        .tempfile()
        .expect("cannot create temp archive");
    let tar_path = tar.path().to_string_lossy().into_owned();
    run(repo, &["archive", "--format=tar", "-o", &tar_path, sha]);
    let status = Command::new("tar")
        .args(["-xf", &tar_path, "-C", &dest.to_string_lossy()])
        .status()
        .expect("failed to execute tar");
    assert!(status.success(), "tar extraction of {sha} failed");
}

#[cfg(test)]
mod tests {
    use super::*;

    use git_fixture::{assert_repo_is, git as fixture_git, init_repo};

    /// A git repo with one commit, for the functions that must shell out.
    ///
    /// Fixture mutations go through `git_fixture`, never through the
    /// production `run`: `run` inherits the environment (a contract the
    /// staged-scan path depends on), so under a git hook it would drive these
    /// mutations into whatever repo `GIT_DIR` names. The read-only production
    /// calls under test stay non-hermetic on purpose; `init_repo`'s canary is
    /// what catches an environment that would redirect them.
    fn repo_with_commit() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        init_repo(p);
        std::fs::write(p.join("f.rs"), "base\n").unwrap();
        fixture_git(p, &["add", "f.rs"]);
        fixture_git(p, &["commit", "-qm", "base"]);
        dir
    }

    #[test]
    fn diff_yields_added_lines_only() {
        let diff = "\
diff --git a/src/a.rs b/src/a.rs
--- a/src/a.rs
+++ b/src/a.rs
@@ -1 +1 @@
-let name = \"removed\";
+let name = \"added\";
";
        let added = added_lines(diff);
        assert_eq!(added, "let name = \"added\";\n");
        assert!(!added.contains("removed"));
    }

    #[test]
    fn diff_joins_multiple_hunks() {
        let diff = "\
diff --git a/src/a.rs b/src/a.rs
--- a/src/a.rs
+++ b/src/a.rs
@@ -1 +1 @@
+first
@@ -9 +9 @@
+second
";
        assert_eq!(added_lines(diff), "first\nsecond\n");
    }

    #[test]
    fn diff_of_a_deleted_file_adds_nothing() {
        let diff = "\
diff --git a/gone.rs b/gone.rs
--- a/gone.rs
+++ /dev/null
@@ -1 +0,0 @@
-let name = \"x\";
";
        assert!(added_lines(diff).is_empty());
    }

    #[test]
    fn diff_headers_are_never_read_as_added_content() {
        let diff = "\
diff --git a/src/a.rs b/src/a.rs
--- /dev/null
+++ b/src/a.rs
@@ -0,0 +1 @@
+real
";
        assert_eq!(added_lines(diff), "real\n");
    }

    /// An added line of literal `++ ...` reaches the diff as `+++ ...`.
    /// Header-prefix parsing consumed it and skipped everything after it.
    #[test]
    fn added_line_shaped_like_a_header_is_scanned_as_content() {
        let diff = "\
diff --git a/src/a.rs b/src/a.rs
--- a/src/a.rs
+++ b/src/a.rs
@@ -1,0 +2,3 @@ base
+++ /dev/null
+let token = \"scanme\";
+++ b/docs/decoy.md
";
        let added = added_lines(diff);
        assert!(added.contains("let token = \"scanme\";"));
        // Each line keeps its own text with only the diff's `+` marker
        // removed; nothing is consumed as a header.
        assert_eq!(
            added,
            "++ /dev/null\nlet token = \"scanme\";\n++ b/docs/decoy.md\n"
        );
    }

    #[test]
    fn staged_files_and_per_file_diff_agree_end_to_end() {
        let dir = repo_with_commit();
        let p = dir.path();
        std::fs::write(
            p.join("f.rs"),
            "base\n++ /dev/null\nlet token = \"scanme\";\n",
        )
        .unwrap();
        std::fs::write(p.join("notes.md"), "doc line\n").unwrap();
        fixture_git(p, &["add", "f.rs", "notes.md"]);

        let files = staged_files(p);
        assert!(files.contains(&PathBuf::from("f.rs")));
        assert!(files.contains(&PathBuf::from("notes.md")));

        let added = added_lines(&staged_diff_for(p, Path::new("f.rs")));
        assert!(
            added.contains("let token = \"scanme\";"),
            "content after a header-shaped line must still be scanned: {added:?}"
        );
    }

    /// `-z` output is raw bytes, so a name needing quoting still round-trips.
    #[test]
    fn staged_files_handles_names_git_would_otherwise_quote() {
        let dir = repo_with_commit();
        let p = dir.path();
        std::fs::write(p.join("café.md"), "x\n").unwrap();
        fixture_git(p, &["add", "café.md"]);
        assert!(staged_files(p).contains(&PathBuf::from("café.md")));
    }

    #[test]
    fn push_refs_parse_a_normal_update() {
        let line = "refs/heads/main aaa111 refs/heads/main bbb222\n";
        assert_eq!(
            parse_push_refs(line),
            vec![RefUpdate::Update {
                remote: "bbb222".into(),
                local: "aaa111".into()
            }]
        );
    }

    #[test]
    fn push_refs_detect_a_new_branch() {
        let zero = "0".repeat(40);
        let line = format!("refs/heads/feat aaa111 refs/heads/feat {zero}\n");
        assert_eq!(
            parse_push_refs(&line),
            vec![RefUpdate::New {
                local: "aaa111".into()
            }]
        );
    }

    #[test]
    fn push_refs_detect_a_deletion() {
        let zero = "0".repeat(40);
        let line = format!("(delete) {zero} refs/heads/old bbb222\n");
        assert_eq!(parse_push_refs(&line), vec![RefUpdate::Delete]);
    }

    #[test]
    fn push_refs_handle_multiple_lines_and_blank_lines() {
        let zero = "0".repeat(64);
        let input = format!(
            "refs/heads/main aaa refs/heads/main bbb\n\n\
             refs/heads/feat ccc refs/heads/feat {zero}\n"
        );
        let refs = parse_push_refs(&input);
        assert_eq!(refs.len(), 2);
        assert_eq!(
            refs[1],
            RefUpdate::New {
                local: "ccc".into()
            }
        );
    }

    #[test]
    #[should_panic(expected = "malformed pre-push stdin line")]
    fn malformed_push_line_panics() {
        parse_push_refs("refs/heads/main aaa\n");
    }

    #[test]
    fn deletion_has_nothing_to_scan() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(log_opts_for(dir.path(), &RefUpdate::Delete), None);
    }

    /// Commit `n` more times on the current branch, returning the tip sha.
    fn commit_more(repo: &Path, n: usize) -> String {
        for i in 0..n {
            std::fs::write(repo.join(format!("extra{i}.rs")), "x\n").unwrap();
            fixture_git(repo, &["add", "."]);
            fixture_git(repo, &["commit", "-qm", "more"]);
        }
        fixture_git(repo, &["rev-parse", "HEAD"]).trim().to_string()
    }

    #[test]
    fn update_scans_exactly_the_pushed_range() {
        let dir = repo_with_commit();
        let p = dir.path();
        let remote = fixture_git(p, &["rev-parse", "HEAD"]).trim().to_string();
        let local = commit_more(p, 2);

        let update = RefUpdate::Update {
            remote: remote.clone(),
            local: local.clone(),
        };
        assert_eq!(log_opts_for(p, &update), Some(format!("{remote}..{local}")));
    }

    /// The remote can have advanced from another clone; its sha need not be
    /// in the local store. That must degrade to over-scanning, never fail.
    #[test]
    fn update_with_an_unfetched_remote_sha_falls_back_to_merge_base() {
        let dir = repo_with_commit();
        let p = dir.path();
        let base = fixture_git(p, &["rev-parse", "HEAD"]).trim().to_string();
        fixture_git(p, &["checkout", "-q", "-b", "feat"]);
        let local = commit_more(p, 1);

        let update = RefUpdate::Update {
            remote: "0123456789abcdef0123456789abcdef01234567".into(),
            local: local.clone(),
        };
        // Bounded by the merge-base with main, not the absent remote sha.
        assert_eq!(log_opts_for(p, &update), Some(format!("{base}..{local}")));
    }

    #[test]
    fn new_branch_is_bounded_by_the_merge_base_with_the_default_branch() {
        let dir = repo_with_commit();
        let p = dir.path();
        let base = fixture_git(p, &["rev-parse", "HEAD"]).trim().to_string();
        fixture_git(p, &["checkout", "-q", "-b", "feat"]);
        let local = commit_more(p, 2);

        let update = RefUpdate::New {
            local: local.clone(),
        };
        assert_eq!(log_opts_for(p, &update), Some(format!("{base}..{local}")));
    }

    /// No main/master to bound against: scan the whole reachable history
    /// rather than silently scanning nothing.
    #[test]
    fn new_branch_without_a_default_branch_scans_whole_history() {
        let dir = repo_with_commit();
        let p = dir.path();
        fixture_git(p, &["checkout", "-q", "-b", "solo"]);
        // The sharpest fixture in the suite: deleting `main` in the wrong repo
        // destroys real work. Re-confirm isolation immediately before it.
        assert_repo_is(p);
        fixture_git(p, &["branch", "-q", "-D", "main"]);
        let local = fixture_git(p, &["rev-parse", "HEAD"]).trim().to_string();

        let update = RefUpdate::New {
            local: local.clone(),
        };
        assert_eq!(log_opts_for(p, &update), Some(local));
    }

    #[test]
    fn tracked_files_lists_committed_paths_and_honors_a_scope() {
        let dir = repo_with_commit();
        let p = dir.path();
        std::fs::create_dir_all(p.join("docs")).unwrap();
        std::fs::write(p.join("docs/a.md"), "x\n").unwrap();
        std::fs::write(p.join("untracked.rs"), "x\n").unwrap();
        fixture_git(p, &["add", "docs/a.md"]);
        fixture_git(p, &["commit", "-qm", "docs"]);

        let all = tracked_files(p, None);
        assert!(all.contains(&PathBuf::from("f.rs")));
        assert!(all.contains(&PathBuf::from("docs/a.md")));
        assert!(
            !all.contains(&PathBuf::from("untracked.rs")),
            "untracked files must never enter a tree scan"
        );

        assert_eq!(
            tracked_files(p, Some("docs")),
            vec![PathBuf::from("docs/a.md")]
        );
    }

    #[test]
    fn extract_tree_reproduces_the_tree_and_leaves_no_scratch_archive() {
        let dir = repo_with_commit();
        let p = dir.path();
        let dest = tempfile::tempdir().unwrap();
        extract_tree(p, "HEAD", dest.path());

        assert_eq!(
            std::fs::read_to_string(dest.path().join("f.rs")).unwrap(),
            "base\n"
        );
        // The scratch tar is written inside dest; if it ever survives, it
        // gets scanned as mirror content.
        let left: Vec<String> = std::fs::read_dir(dest.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(left, vec!["f.rs".to_string()]);
    }

    /// The scratch archive used to be written inside `dest` under a fixed
    /// name. A tree tracking that name overwrote the archive mid-extraction
    /// and then had its own content deleted from the mirror -- so committing
    /// scannable content under that filename exempted it from the tip scan.
    #[test]
    fn a_tracked_file_named_like_the_scratch_archive_is_still_mirrored() {
        let dir = repo_with_commit();
        let p = dir.path();
        std::fs::write(p.join("..tree.tar"), "let token = \"scanme\";\n").unwrap();
        fixture_git(p, &["add", "--", "..tree.tar"]);
        fixture_git(p, &["commit", "-qm", "collide"]);

        let dest = tempfile::tempdir().unwrap();
        extract_tree(p, "HEAD", dest.path());
        assert_eq!(
            std::fs::read_to_string(dest.path().join("..tree.tar")).unwrap(),
            "let token = \"scanme\";\n",
            "the repo's own file must survive extraction and be scannable"
        );
    }

    #[test]
    fn try_repo_root_finds_a_repo_and_returns_none_outside_one() {
        let dir = repo_with_commit();
        let inside = std::fs::canonicalize(dir.path()).unwrap();
        assert_eq!(try_repo_root(dir.path()), Some(inside));

        // A bare tempdir is not a git repo (and, being under the system temp
        // root, is not nested inside this checkout either).
        let plain = tempfile::tempdir().unwrap();
        assert_eq!(try_repo_root(plain.path()), None);
    }

    /// A git error that is not a clean "not a git repository" must fail closed,
    /// never collapse into `None`. Probing from inside a repo's `.git` makes
    /// `rev-parse --show-toplevel` fail with "must be run in a work tree" -- a
    /// real error distinct from the no-repo case -- so the guard panics rather
    /// than reading the location as ungated and exempting a write.
    #[test]
    #[should_panic(expected = "failing closed")]
    fn try_repo_root_fails_closed_on_a_git_error_that_is_not_not_a_repo() {
        let dir = repo_with_commit();
        try_repo_root(&dir.path().join(".git"));
    }

    #[test]
    fn deleted_files_lists_only_index_entries_absent_from_the_worktree() {
        let dir = repo_with_commit();
        let p = dir.path();
        assert!(deleted_files(p).is_empty());
        std::fs::remove_file(p.join("f.rs")).unwrap();
        assert_eq!(deleted_files(p), vec![PathBuf::from("f.rs")]);
    }
}

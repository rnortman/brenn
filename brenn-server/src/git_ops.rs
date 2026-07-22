//! Git operations for managed repos.
//!
//! All operations use `git -C <path>` to target the correct repo, regardless
//! of what working directory is set.
//!
//! Network and credential legs (pull, push) run on the host; `git
//! add`/`git commit` run in the container so the commit inherits the
//! sandbox's `.gitconfig` identity. See each function's docstring for
//! which legs run where, and
//! `docs/designs/repo-sync-auth-and-host-unification.md` for the
//! rationale.

use std::path::Path;

use brenn_lib::config::{ContainerSpawnConfig, ResolvedMount};
use brenn_lib::subprocess::run_in_app_env;
use serde::Serialize;
use tracing::{info, warn};

/// Helper: stringify `mount.host_path` for `git -C` usage. Every
/// host-side git invocation in this file targets the host path directly,
/// regardless of whether the surrounding app is containerized.
fn host_path_str(mount: &ResolvedMount) -> String {
    mount.host_path.to_string_lossy().to_string()
}

/// Result of checking a single repo's status.
#[derive(Debug, Serialize)]
pub struct RepoStatusResult {
    pub slug: String,
    pub branch: Option<String>,
    pub dirty_files: Vec<String>,
    pub staged_files: Vec<String>,
    /// Commits ahead of upstream. `None` if no upstream tracking configured.
    pub unpushed_count: Option<u64>,
    /// Upstream tracking branch. `None` if not configured.
    pub upstream: Option<String>,
    /// Populated when one or more git queries failed for this repo.
    /// When set, the other fields may be partial; the LLM should treat
    /// this repo's status as unknown rather than clean.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result of committing and pushing a single repo.
#[derive(Debug, Serialize)]
pub struct CommitPushResult {
    pub slug: String,
    /// Whether the repo had changes to commit.
    pub had_changes: bool,
    /// Whether the commit succeeded (false if nothing to commit).
    pub commit_ok: bool,
    /// Whether the push succeeded. `None` if commit was skipped.
    pub push_ok: Option<bool>,
    /// Human-readable detail for the LLM.
    pub detail: String,
}

/// Result of running an arbitrary git command.
#[derive(Debug, Serialize)]
pub struct GitRunResult {
    pub slug: String,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Check the status of a single repo. Runs host-side against
/// `mount.host_path`; status is read-only and needs neither container
/// `.gitconfig` identity nor network access.
pub async fn repo_status(mount: &ResolvedMount, working_dir: &Path) -> RepoStatusResult {
    let git_path_str = host_path_str(mount);

    info!(repo = %mount.slug, "GitRepoStatus: querying");

    // Run the five independent git queries concurrently.
    // Build arg arrays first — tokio::join! borrows them across an await point.
    let branch_args = ["-C", &*git_path_str, "rev-parse", "--abbrev-ref", "HEAD"];
    let dirty_args = ["-C", &*git_path_str, "diff", "--name-only"];
    let staged_args = ["-C", &*git_path_str, "diff", "--cached", "--name-only"];
    let untracked_args = [
        "-C",
        &*git_path_str,
        "ls-files",
        "--others",
        "--exclude-standard",
    ];
    let upstream_args = [
        "-C",
        &*git_path_str,
        "rev-parse",
        "--abbrev-ref",
        "--symbolic-full-name",
        "@{upstream}",
    ];

    let (branch_result, dirty_result, staged_result, untracked_result, upstream_result) = tokio::join!(
        run_git_string(&branch_args, working_dir, None),
        run_git_string(&dirty_args, working_dir, None),
        run_git_string(&staged_args, working_dir, None),
        run_git_string(&untracked_args, working_dir, None),
        run_git_string(&upstream_args, working_dir, None),
    );

    let mut errors: Vec<String> = Vec::new();

    let branch = match branch_result {
        Ok(s) => Some(s.trim().to_string()),
        Err(e) => {
            errors.push(format!("branch: {e}"));
            None
        }
    };

    let mut dirty_files = match dirty_result {
        Ok(s) => parse_lines(&s),
        Err(e) => {
            errors.push(format!("dirty: {e}"));
            Vec::new()
        }
    };
    match untracked_result {
        Ok(s) => dirty_files.extend(parse_lines(&s)),
        Err(e) => errors.push(format!("untracked: {e}")),
    }

    let staged_files = match staged_result {
        Ok(s) => parse_lines(&s),
        Err(e) => {
            errors.push(format!("staged: {e}"));
            Vec::new()
        }
    };

    // upstream: a non-zero exit from `rev-parse @{upstream}` is the
    // documented signal for "no upstream configured" (per
    // docs/designs/multi-repo-git.md). Don't push that into `errors` —
    // upstream=None is the signal. We can't easily distinguish a real
    // subprocess failure here from "no upstream" without inspecting
    // stderr, so preserve existing behavior: any error → upstream=None,
    // no error recorded. The other four queries are robust enough to
    // surface real failures.
    let upstream = upstream_result.ok().map(|s| s.trim().to_string());

    // unpushed_count depends on upstream — only query if upstream exists.
    let unpushed_count = if upstream.is_some() {
        match run_git_string(
            &[
                "-C",
                &git_path_str,
                "rev-list",
                "--count",
                "@{upstream}..HEAD",
            ],
            working_dir,
            None,
        )
        .await
        {
            Ok(s) => {
                let trimmed = s.trim();
                match trimmed.parse::<u64>() {
                    Ok(n) => Some(n),
                    Err(_) => {
                        // git rev-list --count always emits a single decimal
                        // number on exit 0. Anything else is an invariant
                        // violation (e.g. a hook wrote extra text); surface it.
                        errors.push(format!(
                            "unpushed: git rev-list --count emitted non-numeric output: {trimmed:?}"
                        ));
                        None
                    }
                }
            }
            Err(e) => {
                errors.push(format!("unpushed: {e}"));
                None
            }
        }
    } else {
        None
    };

    let error = if errors.is_empty() {
        None
    } else {
        let joined = errors.join("; ");
        // Sanitize before logging: GitSubprocessError Display may embed newlines
        // from git's stderr, which would forge separate log records for fail2ban.
        warn!(
            repo = %mount.slug,
            "GitRepoStatus failed: {}",
            crate::git_subprocess::sanitize_log_line(&joined)
        );
        Some(joined)
    };

    RepoStatusResult {
        slug: mount.slug.clone(),
        branch,
        dirty_files,
        staged_files,
        unpushed_count,
        upstream,
        error,
    }
}

/// Commit all changes and push a single repo.
///
/// `git add` / `git commit` run in the container (when `container_spawn`
/// is set) against `mount.visible_path(containerized)` so the commit
/// inherits the container's `.gitconfig` identity. `git push` runs
/// host-side against `mount.host_path`. Podman's `keep-id` userns
/// mapping makes the working tree written by the in-container commit
/// directly readable/writable by the host-side push.
pub async fn repo_commit_and_push(
    mount: &ResolvedMount,
    working_dir: &Path,
    container_spawn: Option<&ContainerSpawnConfig>,
    message: &str,
) -> CommitPushResult {
    let containerized = container_spawn.is_some();
    // Path string the container or bare process sees for add/commit.
    let commit_path_str = mount
        .visible_path(containerized)
        .to_string_lossy()
        .to_string();
    // Host-side path for the push leg — always `mount.host_path`, even
    // for bare-process apps (where it already matches).
    let push_path_str = host_path_str(mount);

    info!(repo = %mount.slug, "GitRepoCommitAndPush: staging and committing");

    // 1. git add -A (containerized when applicable)
    if let Err(e) = run_git_string(
        &["-C", &commit_path_str, "add", "-A"],
        working_dir,
        container_spawn,
    )
    .await
    {
        warn!(
            repo = %mount.slug,
            "GitRepoCommitAndPush: git add failed: {}",
            crate::git_subprocess::sanitize_log_line(&format!("{e}"))
        );
        return CommitPushResult {
            slug: mount.slug.clone(),
            had_changes: false,
            commit_ok: false,
            push_ok: None,
            detail: format!("git add -A failed: {e}"),
        };
    }

    // 2. git commit -m "<message>" (containerized when applicable)
    let commit_result = run_git_string(
        &["-C", &commit_path_str, "commit", "-m", message],
        working_dir,
        container_spawn,
    )
    .await;

    match commit_result {
        Ok(stdout) => {
            // Sanitize before logging: even strict-UTF-8 stdout may contain
            // embedded newlines (e.g. commit hook output) that would forge
            // separate log records for fail2ban.
            info!(
                repo = %mount.slug,
                "GitRepoCommitAndPush: committed: {}",
                crate::git_subprocess::sanitize_log_line(stdout.trim())
            );
        }
        Err(
            ref e @ crate::git_subprocess::GitSubprocessError::NonZero {
                ref stdout,
                ref stderr,
                ..
            },
        ) => {
            // "nothing to commit" exits with code 1. Combine stdout and stderr
            // because git may emit the marker on either stream depending on
            // git version. The English string is intentional: we control the
            // git binary and do not set LANG/LC_ALL in the subprocess env.
            let combined = format!("{} {}", stdout.trim(), stderr.trim());
            if combined.contains("nothing to commit") {
                info!(repo = %mount.slug, "GitRepoCommitAndPush: nothing to commit");
                return CommitPushResult {
                    slug: mount.slug.clone(),
                    had_changes: false,
                    commit_ok: true,
                    push_ok: None,
                    detail: "Nothing to commit.".to_string(),
                };
            }
            // Use Display ({e}) rather than stderr directly: Display caps stderr
            // to 512 bytes, preventing unbounded strings in CommitPushResult.detail.
            return CommitPushResult {
                slug: mount.slug.clone(),
                had_changes: true,
                commit_ok: false,
                push_ok: None,
                detail: format!("git commit failed: {e}"),
            };
        }
        Err(e) => {
            return CommitPushResult {
                slug: mount.slug.clone(),
                had_changes: true,
                commit_ok: false,
                push_ok: None,
                detail: format!("git commit failed: {e}"),
            };
        }
    }

    // 3. git push — always host-side. Uses host path + host ~/.ssh/.
    //    `container_spawn = None` so `run_in_app_env` skips podman.
    let push_result = run_git_string(&["-C", &push_path_str, "push"], working_dir, None).await;

    match push_result {
        Ok(msg) => {
            info!(
                repo = %mount.slug,
                "GitRepoCommitAndPush: pushed: {}",
                crate::git_subprocess::sanitize_log_line(msg.trim())
            );
            CommitPushResult {
                slug: mount.slug.clone(),
                had_changes: true,
                commit_ok: true,
                push_ok: Some(true),
                detail: "Committed and pushed.".to_string(),
            }
        }
        Err(e) => {
            // Sanitize before logging: NonZero Display may embed newlines from
            // git's stderr (e.g. remote rejection messages) that forge log lines.
            warn!(
                repo = %mount.slug,
                "GitRepoCommitAndPush: push failed: {}",
                crate::git_subprocess::sanitize_log_line(&format!("{e}"))
            );
            CommitPushResult {
                slug: mount.slug.clone(),
                had_changes: true,
                commit_ok: true,
                push_ok: Some(false),
                detail: format!("Committed locally but push failed: {e}"),
            }
        }
    }
}

/// Run an arbitrary git command in a repo.
pub async fn repo_run(
    mount: &ResolvedMount,
    working_dir: &Path,
    container_spawn: Option<&ContainerSpawnConfig>,
    args: &[String],
) -> GitRunResult {
    let containerized = container_spawn.is_some();
    let git_path = mount.visible_path(containerized);
    let git_path_str = git_path.to_string_lossy().to_string();

    // Build full args: git -C <path> <user_args...>
    let mut full_args: Vec<&str> = vec!["-C", &git_path_str];
    full_args.extend(args.iter().map(|s| s.as_str()));

    info!(repo = %mount.slug, args = ?args, "GitRepoRun");

    // Lossy decode: repo_run is a pass-through surface; LLM observes raw git
    // output. Non-UTF-8 bytes (e.g., legacy Shift-JIS commit messages,
    // latin-1 filenames) must not produce a synthetic exit-(-1) when git
    // actually succeeded. U+FFFD substitution is acceptable here.
    let result = run_git_string_lossy(&full_args, working_dir, container_spawn).await;

    match result {
        Ok(stdout) => GitRunResult {
            slug: mount.slug.clone(),
            exit_code: 0,
            stdout,
            stderr: String::new(),
        },
        Err(crate::git_subprocess::GitSubprocessError::NonZero {
            stdout,
            stderr,
            exit_code,
            ..
        }) => {
            // Strip trailing newline for symmetry with the Ok arm.
            let stdout = crate::git_subprocess::strip_trailing_newline(stdout);
            GitRunResult {
                slug: mount.slug.clone(),
                exit_code: exit_code.unwrap_or(-1),
                stdout,
                stderr,
            }
        }
        Err(e) => GitRunResult {
            slug: mount.slug.clone(),
            exit_code: -1,
            stdout: String::new(),
            stderr: format!("{e}"),
        },
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a short diagnostic label from the git args.
///
/// Skips leading `-C <path>` tokens so the label names the subcommand, not the
/// working-directory flag. E.g. `["-C", "/repo", "status", "--short"]` →
/// `"git status"`.
fn git_label(args: &[&str]) -> String {
    let mut it = args.iter().peekable();
    while it.peek() == Some(&&"-C") {
        it.next(); // skip "-C"
        it.next(); // skip the path argument
    }
    format!("git {}", it.next().unwrap_or(&""))
}

/// Run a git command with bounded output and return decoded stdout on success,
/// or a typed `GitSubprocessError` on failure.
///
/// Delegates to `crate::git_subprocess::run_with_bounded_output_lazy` for the
/// 256 KiB cap, 60 s timeout, strict-UTF-8 stdout, and trailing-newline strip.
/// The label string is materialised lazily — only when an error is constructed —
/// so the success path (the norm) pays no allocation for the label.
async fn run_git_string(
    args: &[&str],
    working_dir: &Path,
    container_spawn: Option<&ContainerSpawnConfig>,
) -> Result<String, crate::git_subprocess::GitSubprocessError> {
    let cmd = run_in_app_env("git", args, working_dir, container_spawn, &[], &[]);
    crate::git_subprocess::run_with_bounded_output_lazy(cmd, || git_label(args)).await
}

/// Run a git command with bounded output and lossy-UTF-8 stdout decode.
///
/// Identical to `run_git_string` except that non-UTF-8 bytes on the success
/// path are replaced with U+FFFD rather than returning `DecodeError`. Use for
/// pass-through surfaces where the caller reports raw git output to the LLM.
/// Label allocation is also deferred to error paths.
async fn run_git_string_lossy(
    args: &[&str],
    working_dir: &Path,
    container_spawn: Option<&ContainerSpawnConfig>,
) -> Result<String, crate::git_subprocess::GitSubprocessError> {
    let cmd = run_in_app_env("git", args, working_dir, container_spawn, &[], &[]);
    crate::git_subprocess::run_with_lossy_output_lazy(cmd, || git_label(args)).await
}

/// Parse newline-separated output into a Vec of non-empty strings.
fn parse_lines(s: &str) -> Vec<String> {
    s.lines()
        .map(|l| l.to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::config::{AccessLevel, ResolvedMount};
    use git_fixture::{add_bare_origin, git as fixture_git, seed_repo};

    fn test_mount(dir: &std::path::Path) -> ResolvedMount {
        ResolvedMount {
            slug: "test".to_string(),
            host_path: dir.to_path_buf(),
            container_path: None,
            access: AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        }
    }

    // -----------------------------------------------------------------------
    // repo_status
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn status_clean_repo() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let _remote = add_bare_origin(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        let status = repo_status(&repo, working_dir.path()).await;
        assert_eq!(status.slug, "test");
        assert_eq!(status.branch.as_deref(), Some("main"));
        assert!(status.dirty_files.is_empty());
        assert!(status.staged_files.is_empty());
        assert_eq!(status.unpushed_count, Some(0));
        assert!(status.upstream.is_some());
        assert!(
            status.error.is_none(),
            "healthy repo should not emit error, got: {:?}",
            status.error
        );
    }

    #[tokio::test]
    async fn status_dirty_files() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        // Create a dirty file.
        std::fs::write(dir.path().join("file.txt"), "modified").unwrap();

        let status = repo_status(&repo, working_dir.path()).await;
        assert!(
            status.dirty_files.contains(&"file.txt".to_string()),
            "expected file.txt in dirty_files, got: {:?}",
            status.dirty_files
        );
    }

    #[tokio::test]
    async fn status_untracked_files() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        // Create an untracked file.
        std::fs::write(dir.path().join("new.txt"), "new").unwrap();

        let status = repo_status(&repo, working_dir.path()).await;
        assert!(
            status.dirty_files.contains(&"new.txt".to_string()),
            "expected new.txt in dirty_files (untracked), got: {:?}",
            status.dirty_files
        );
    }

    #[tokio::test]
    async fn status_staged_files() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        // Stage a change.
        std::fs::write(dir.path().join("file.txt"), "modified").unwrap();
        fixture_git(dir.path(), &["add", "file.txt"]);

        let status = repo_status(&repo, working_dir.path()).await;
        assert!(
            status.staged_files.contains(&"file.txt".to_string()),
            "expected file.txt in staged_files, got: {:?}",
            status.staged_files
        );
    }

    #[tokio::test]
    async fn status_unpushed_commits() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let _remote = add_bare_origin(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        // Make a local commit.
        std::fs::write(dir.path().join("local.txt"), "local").unwrap();
        fixture_git(dir.path(), &["add", "."]);
        fixture_git(dir.path(), &["commit", "-m", "local"]);

        let status = repo_status(&repo, working_dir.path()).await;
        assert_eq!(status.unpushed_count, Some(1));
    }

    #[tokio::test]
    async fn status_no_upstream() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        // No remote added.
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        let status = repo_status(&repo, working_dir.path()).await;
        assert!(
            status.upstream.is_none(),
            "expected no upstream, got: {:?}",
            status.upstream
        );
        assert!(
            status.unpushed_count.is_none(),
            "expected no unpushed count without upstream"
        );
    }

    #[tokio::test]
    async fn status_partial_failure() {
        // No-upstream case is documented as null-without-error per
        // multi-repo-git design; branch/dirty/staged still populate.
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        let status = repo_status(&repo, working_dir.path()).await;
        assert_eq!(status.branch.as_deref(), Some("main"));
        assert!(status.upstream.is_none());
        assert!(status.unpushed_count.is_none());
        assert!(
            status.error.is_none(),
            "no-upstream is not a failure, but got error: {:?}",
            status.error
        );
    }

    #[tokio::test]
    async fn status_subprocess_failure_reports_error() {
        // Point at a directory that isn't a git repo at all — every
        // git query should fail and show up in `error`.
        let dir = tempfile::tempdir().unwrap();
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        let status = repo_status(&repo, working_dir.path()).await;
        assert!(
            status.error.is_some(),
            "expected error to be populated for non-git dir"
        );
        let err = status.error.unwrap();
        assert!(
            err.contains("branch:") || err.contains("dirty:") || err.contains("staged:"),
            "expected error to mention a failed subcommand, got: {err}"
        );
        assert!(status.branch.is_none());
        assert!(status.dirty_files.is_empty());
        assert!(status.staged_files.is_empty());
        assert!(status.upstream.is_none());
        assert!(status.unpushed_count.is_none());
    }

    // -----------------------------------------------------------------------
    // repo_commit_and_push
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn commit_and_push_with_changes() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let _remote = add_bare_origin(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        std::fs::write(dir.path().join("new.txt"), "new").unwrap();

        let result = repo_commit_and_push(&repo, working_dir.path(), None, "test commit").await;
        assert!(result.had_changes);
        assert!(result.commit_ok);
        assert_eq!(result.push_ok, Some(true));
    }

    #[tokio::test]
    async fn commit_and_push_nothing_to_commit() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let _remote = add_bare_origin(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        let result = repo_commit_and_push(&repo, working_dir.path(), None, "test commit").await;
        assert!(!result.had_changes);
        assert!(
            result.commit_ok,
            "nothing-to-commit should report commit_ok=true"
        );
        assert!(result.detail.contains("Nothing to commit"));
    }

    #[tokio::test]
    async fn commit_and_push_no_upstream() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        // No remote — commit succeeds, push fails.
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        std::fs::write(dir.path().join("new.txt"), "new").unwrap();

        let result = repo_commit_and_push(&repo, working_dir.path(), None, "test commit").await;
        assert!(result.had_changes);
        assert!(result.commit_ok);
        assert_eq!(result.push_ok, Some(false));
        assert!(result.detail.contains("push failed"));
    }

    // -----------------------------------------------------------------------
    // repo_run
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_log() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        let result = repo_run(
            &repo,
            working_dir.path(),
            None,
            &["log".to_string(), "--oneline".to_string()],
        )
        .await;
        assert_eq!(result.exit_code, 0);
        assert!(
            result.stdout.contains("initial"),
            "expected 'initial' in log output, got: {}",
            result.stdout
        );
    }

    #[tokio::test]
    async fn run_bad_command() {
        let dir = tempfile::tempdir().unwrap();
        seed_repo(dir.path());
        let repo = test_mount(dir.path());
        let working_dir = tempfile::tempdir().unwrap();

        let result = repo_run(
            &repo,
            working_dir.path(),
            None,
            &["nonexistent-subcommand".to_string()],
        )
        .await;
        assert_ne!(result.exit_code, 0);
        assert!(
            !result.stderr.is_empty(),
            "stderr should be populated for unknown subcommand, got empty"
        );
        assert!(
            result.stdout.is_empty(),
            "stdout should be empty for unknown subcommand, got: {:?}",
            result.stdout
        );
    }

    // -----------------------------------------------------------------------
    // parse_lines
    // -----------------------------------------------------------------------

    #[test]
    fn parse_lines_basic() {
        assert_eq!(parse_lines("a\nb\nc"), vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_lines_empty() {
        assert!(parse_lines("").is_empty());
    }

    #[test]
    fn parse_lines_trailing_newline() {
        assert_eq!(parse_lines("a\nb\n"), vec!["a", "b"]);
    }

    #[test]
    fn parse_lines_blank_lines_filtered() {
        assert_eq!(parse_lines("a\n\nb\n\n"), vec!["a", "b"]);
    }
}

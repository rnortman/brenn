//! Auto-cloning of declared repos into `repo_sync.repo_dir`.
//!
//! Every network leg of git in this crate — clone, fetch, push — runs on the
//! host with the host's credentials. Containers never hold git credentials.

use std::path::{Path, PathBuf};

use futures::future::FutureExt as _;
use tracing::{error, info, warn};

use brenn_lib::config::BrennConfig;
use brenn_obs::alerting::{AlertDispatcher, AlertSeverity};

use crate::subprocess::{GitSubprocessError, run_git, sanitize_log_line};

/// Read the on-disk clone's `remote.origin.url` and compare it against the
/// configured remote URL. Fires a deduplicated warning alert on mismatch or
/// subprocess failure. Never panics on subprocess errors.
async fn check_remote_url_drift(
    slug: String,
    expected_remote: String,
    clone_path: PathBuf,
    alert_dispatcher: AlertDispatcher,
) {
    match run_git(&clone_path, &["config", "--get", "remote.origin.url"]).await {
        Ok(actual) => {
            let actual = actual.trim().to_string();
            if actual == expected_remote.trim() {
                return; // No drift.
            }
            // Sanitize subprocess/config strings to prevent newline injection
            // into fail2ban-watched log lines AND alert fields. Use sanitized
            // forms everywhere so the safety property doesn't depend on
            // git's config grammar disallowing literal newlines in values.
            let actual_safe = sanitize_log_line(&actual);
            let expected_safe = sanitize_log_line(&expected_remote);
            warn!(
                slug = %slug,
                expected_remote = %expected_safe,
                actual_remote = %actual_safe,
                clone_path = %clone_path.display(),
                "repo remote URL drift detected",
            );
            let dedup_key = format!("{}|{}|{}", slug, expected_safe, actual_safe);
            alert_dispatcher
                .with_field("slug", slug.clone())
                .with_field("expected_remote", expected_safe.clone())
                .with_field("actual_remote", actual_safe.clone())
                .with_field("clone_path", clone_path.display().to_string())
                .alert_once_per_process(
                    AlertSeverity::Warning,
                    "repo remote URL drift".to_string(),
                    &dedup_key,
                    format!(
                        "Repo {slug}: config remote `{expected_safe}` does not match \
                         on-disk `remote.origin.url` `{actual_safe}` at `{clone_path}`. \
                         Brenn will keep fetching the on-disk remote until you \
                         `git remote set-url origin {expected_safe}` or reconcile the config.",
                        clone_path = clone_path.display(),
                    ),
                );
        }
        Err(e) => {
            // All failure variants route here. Build the sanitized failure
            // string once; reuse in both warn! and alert body.
            let failure = sanitize_log_line(&e.to_string());
            warn!(
                slug = %slug,
                clone_path = %clone_path.display(),
                failure = %failure,
                "repo remote URL check failed",
            );
            let dedup_key = format!("remote-url-check-failed|{slug}");
            alert_dispatcher
                .with_field("slug", slug.clone())
                .with_field("clone_path", clone_path.display().to_string())
                .with_field("failure", failure.clone())
                .alert_once_per_process(
                    AlertSeverity::Warning,
                    "repo remote URL check failed".to_string(),
                    &dedup_key,
                    format!(
                        "Repo {slug}: failed to read `remote.origin.url` from clone at \
                         {clone_path}. Cannot detect config/clone URL drift for this repo \
                         until resolved. Failure: {failure}.",
                        clone_path = clone_path.display(),
                    ),
                );
        }
    }
}

/// One repo whose clone failed. `failure` is single-line (newlines sanitized).
struct CloneFailure {
    slug: String,
    remote: String,
    target: PathBuf,
    failure: String,
}

/// Render a clone error as one line: the error's `Display` plus the exit
/// status for a non-zero exit.
fn describe_clone_error(e: &GitSubprocessError) -> String {
    let rendered = match e {
        GitSubprocessError::NonZero {
            exit_code: Some(code),
            ..
        } => format!("{e} (exit {code})"),
        GitSubprocessError::NonZero {
            exit_code: None, ..
        } => format!("{e} (killed by signal)"),
        _ => e.to_string(),
    };
    sanitize_log_line(&rendered)
}

/// Clone `remote` into `target`, which the caller has established is an empty
/// directory.
async fn clone_repo(slug: String, remote: String, target: PathBuf) -> Result<(), CloneFailure> {
    match run_git(&target, &["clone", &remote, "."]).await {
        Ok(_) => {
            info!(slug = %slug, "auto-clone complete");
            Ok(())
        }
        Err(e) => Err(CloneFailure {
            failure: describe_clone_error(&e),
            slug,
            remote,
            target,
        }),
    }
}

/// Name `.git` is moved to inside a failed target before the target is emptied.
const DISCARDED_GIT_DIR: &str = ".git.failed-clone";

/// Empty a target whose clone failed, keeping the directory itself (so a
/// symlinked target stays a symlink). A clone killed on timeout leaves a
/// partial `.git` behind; emptying the target keeps `.git` a reliable "clone
/// completed" marker for the next boot. `.git` is renamed away in one step
/// before anything is removed, so a removal that fails partway leaves a
/// target without `.git`, which the next boot's emptiness check refuses.
fn reset_failed_target(target: &Path) -> Result<(), String> {
    let git_dir = target.join(".git");
    match std::fs::rename(&git_dir, target.join(DISCARDED_GIT_DIR)) {
        Ok(()) => {}
        // git already removed its own `.git`.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("failed to move aside {git_dir:?}: {e}")),
    }
    let entries =
        std::fs::read_dir(target).map_err(|e| format!("failed to read {target:?}: {e}"))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("failed to read {target:?}: {e}"))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|e| format!("failed to stat {path:?}: {e}"))?;
        let removed = if file_type.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        removed.map_err(|e| format!("failed to remove {path:?}: {e}"))?;
    }
    Ok(())
}

/// Log every failed clone, attempt to reset every failed target, then panic
/// once naming every failure and every target that could not be reset.
fn refuse_boot_on_clone_failures(failures: Vec<CloneFailure>) -> ! {
    for f in &failures {
        error!(
            slug = %f.slug,
            remote = %sanitize_log_line(&f.remote),
            target = %f.target.display(),
            failure = %f.failure,
            "auto-clone failed",
        );
    }
    let reset_errors: Vec<String> = failures
        .iter()
        .filter_map(|f| reset_failed_target(&f.target).err())
        .map(|e| sanitize_log_line(&e))
        .collect();
    let summary = failures
        .iter()
        .map(|f| format!("{}: {}", f.slug, f.failure))
        .collect::<Vec<_>>()
        .join("; ");
    if reset_errors.is_empty() {
        panic!("git clone failed for {} repo(s): {summary}", failures.len());
    }
    panic!(
        "git clone failed for {} repo(s): {summary}; could not reset {} failed target(s), \
         remove them by hand before restarting: {}",
        failures.len(),
        reset_errors.len(),
        reset_errors.join("; "),
    );
}

/// Auto-clone repos that don't have a `.git` directory yet, and check the
/// on-disk remote of those that do for drift against the config.
///
/// Clones run on the host, concurrently, into the empty per-repo directories
/// created before config validation.
///
/// A target with no `.git` that is not empty panics before any clone is
/// spawned, leaving the directory untouched. Every clone runs to completion
/// or failure; each failed target is then reset to empty, and a single panic
/// names every failed repo and any target whose reset failed.
pub async fn auto_clone_repos(config: &BrennConfig, alert_dispatcher: &AlertDispatcher) {
    let repo_dir = config
        .repo_sync
        .repo_dir
        .as_ref()
        .expect("caller checked repo_sync.repo_dir");

    // Futures are lazy: nothing runs until `join_all` polls them, which is
    // after the emptiness check below.
    let mut futs: Vec<futures::future::BoxFuture<'static, Result<(), CloneFailure>>> =
        Vec::with_capacity(config.repos.len());
    let mut to_clone: Vec<(&str, &str, PathBuf)> = Vec::new();
    for repo in &config.repos {
        let target = repo_dir.join(&repo.slug);
        if target.join(".git").exists() {
            let check = check_remote_url_drift(
                repo.slug.clone(),
                repo.remote.clone(),
                target,
                alert_dispatcher.clone(),
            );
            futs.push(check.map(Ok).boxed());
        } else {
            to_clone.push((&repo.slug, &repo.remote, target));
        }
    }

    // Every clone is launched into a directory known to be empty, so whatever
    // a failed clone leaves behind is its own and is safe to remove.
    for (slug, _, target) in &to_clone {
        let mut entries = std::fs::read_dir(target)
            .unwrap_or_else(|e| panic!("failed to read repo target {target:?}: {e}"));
        if entries.next().is_some() {
            panic!(
                "repo target {target:?} for {slug:?} is non-empty but has no .git; \
                 remove it or clone into it by hand"
            );
        }
    }

    for (slug, remote, target) in to_clone {
        info!(
            slug = %slug,
            remote = %remote,
            target = %target.display(),
            "auto-cloning repo",
        );
        futs.push(clone_repo(slug.to_string(), remote.to_string(), target).boxed());
    }

    let failures: Vec<CloneFailure> = futures::future::join_all(futs)
        .await
        .into_iter()
        .filter_map(Result::err)
        .collect();
    if !failures.is_empty() {
        refuse_boot_on_clone_failures(failures);
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use git_fixture::{git as fixture_git, init_bare_repo, init_repo};

    use brenn_lib::config::{BrennConfig, RepoDeclRaw, repo_sync_at};
    use brenn_obs::alerting::make_capturing_alerter;
    use tracing_test::traced_test;

    use super::*;

    // -----------------------------------------------------------------------
    // auto_clone_repos integration tests
    // -----------------------------------------------------------------------

    /// Create a bare git repository at `dir` with one commit so it can be
    /// cloned. Returns a `TempDir` for the bare remote.
    fn make_bare_remote() -> tempfile::TempDir {
        let remote = tempfile::tempdir().unwrap();
        // Init with a staging clone, commit, then push to the bare remote.
        let staging = tempfile::tempdir().unwrap();
        init_bare_repo(remote.path());
        init_repo(staging.path());
        std::fs::write(staging.path().join("file.txt"), "initial").unwrap();
        fixture_git(staging.path(), &["add", "."]);
        fixture_git(staging.path(), &["commit", "-m", "initial"]);
        fixture_git(
            staging.path(),
            &[
                "remote",
                "add",
                "origin",
                &remote.path().display().to_string(),
            ],
        );
        fixture_git(staging.path(), &["push", "-u", "origin", "main"]);
        remote
    }

    /// Build a minimal `BrennConfig` with `repo_sync.repo_dir = repo_dir` and a single
    /// repo declaration pointing at `remote_url`.
    fn minimal_config(repo_dir: &std::path::Path, slug: &str, remote_url: &str) -> BrennConfig {
        BrennConfig {
            repo_sync: repo_sync_at(repo_dir),
            repos: vec![RepoDeclRaw {
                slug: slug.to_string(),
                remote: remote_url.to_string(),
                auto_pull: true,
            }],
            ..BrennConfig::default()
        }
    }

    /// Replicate the startup `prepare_repo_dirs` step: create `<repo_dir>/<slug>/`
    /// so `auto_clone_repos` has a working directory to `current_dir` into.
    fn prepare_repo_dirs_for_test(repo_dir: &std::path::Path, slug: &str) {
        std::fs::create_dir_all(repo_dir.join(slug)).unwrap();
    }

    #[tokio::test]
    async fn auto_clone_repos_clones_unchecked_repo() {
        let remote = make_bare_remote();
        let repo_dir = tempfile::tempdir().unwrap();
        // Production calls prepare_repo_dirs before auto_clone_repos.
        prepare_repo_dirs_for_test(repo_dir.path(), "test-repo");
        let config = minimal_config(
            repo_dir.path(),
            "test-repo",
            &remote.path().display().to_string(),
        );
        let (dispatcher, captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &dispatcher).await;

        assert!(
            repo_dir.path().join("test-repo").join(".git").exists(),
            "expected .git dir after clone"
        );
        // No drift check runs on the clone-from-scratch path.
        drop(dispatcher);
        handle.await.expect("alert background task panicked");
        assert!(
            captured.lock().unwrap().is_empty(),
            "no alerts expected on fresh clone"
        );
    }

    #[tokio::test]
    async fn auto_clone_repos_skips_already_cloned_repo() {
        let remote = make_bare_remote();
        let repo_dir = tempfile::tempdir().unwrap();
        prepare_repo_dirs_for_test(repo_dir.path(), "test-repo");
        let config = minimal_config(
            repo_dir.path(),
            "test-repo",
            &remote.path().display().to_string(),
        );
        let (dispatcher, captured, handle) = make_capturing_alerter();

        // First clone.
        auto_clone_repos(&config, &dispatcher).await;
        assert!(repo_dir.path().join("test-repo").join(".git").exists());

        // Second call: .git already exists, drift check runs. Config remote
        // matches the on-disk remote.origin.url — no alert expected.
        auto_clone_repos(&config, &dispatcher).await;
        assert!(
            repo_dir.path().join("test-repo").join(".git").exists(),
            ".git must still be present after second call"
        );
        drop(dispatcher);
        handle.await.expect("alert background task panicked");
        assert!(
            captured.lock().unwrap().is_empty(),
            "no alerts expected when remote URLs match"
        );
    }

    // -----------------------------------------------------------------------
    // Drift-check tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn auto_clone_repos_alerts_on_remote_url_mismatch() {
        let remote_a = make_bare_remote();
        let remote_b = make_bare_remote();
        let repo_dir = tempfile::tempdir().unwrap();
        prepare_repo_dirs_for_test(repo_dir.path(), "drift-repo");
        let remote_a_url = remote_a.path().display().to_string();
        let remote_b_url = remote_b.path().display().to_string();

        // Clone from remote A.
        let config_a = minimal_config(repo_dir.path(), "drift-repo", &remote_a_url);
        let (dispatcher, _, handle_a) = make_capturing_alerter();
        auto_clone_repos(&config_a, &dispatcher).await;
        drop(dispatcher);
        handle_a.await.expect("alert background task panicked");

        // Now build a config pointing at remote B (simulating an operator
        // changing the config but forgetting git remote set-url).
        let config_b = minimal_config(repo_dir.path(), "drift-repo", &remote_b_url);
        let (dispatcher2, captured, handle_b) = make_capturing_alerter();
        auto_clone_repos(&config_b, &dispatcher2).await;
        drop(dispatcher2);
        handle_b.await.expect("alert background task panicked");

        let alerts = captured.lock().unwrap();
        assert_eq!(alerts.len(), 1, "expected exactly one drift alert");
        let (title, body) = &alerts[0];
        assert_eq!(title, "repo remote URL drift");
        assert!(body.contains("drift-repo"), "body should contain slug");
        // Assert URL roles specifically: config (expected) is remote B, on-disk
        // (actual) is remote A. Checking for role-specific substrings rather
        // than just URL presence detects a swap in the alert body.
        assert!(
            body.contains(&format!("config remote `{remote_b_url}`")),
            "expected (new) URL should appear as the config remote"
        );
        assert!(
            body.contains(&format!("on-disk `remote.origin.url` `{remote_a_url}`")),
            "actual (old) URL should appear as the on-disk remote"
        );
    }

    #[tokio::test]
    async fn auto_clone_repos_no_alert_on_match() {
        let remote = make_bare_remote();
        let repo_dir = tempfile::tempdir().unwrap();
        prepare_repo_dirs_for_test(repo_dir.path(), "no-drift-repo");
        let config = minimal_config(
            repo_dir.path(),
            "no-drift-repo",
            &remote.path().display().to_string(),
        );
        let (dispatcher, captured, handle) = make_capturing_alerter();

        // Clone, then check again with the same config.
        auto_clone_repos(&config, &dispatcher).await;
        auto_clone_repos(&config, &dispatcher).await;
        drop(dispatcher);
        handle.await.expect("alert background task panicked");

        assert!(
            captured.lock().unwrap().is_empty(),
            "no alerts expected when remote URLs match"
        );
    }

    #[tokio::test]
    async fn auto_clone_repos_drift_alert_dedup_within_process() {
        let remote_a = make_bare_remote();
        let remote_b = make_bare_remote();
        let repo_dir = tempfile::tempdir().unwrap();
        prepare_repo_dirs_for_test(repo_dir.path(), "dedup-drift-repo");

        let config_a = minimal_config(
            repo_dir.path(),
            "dedup-drift-repo",
            &remote_a.path().display().to_string(),
        );
        let (dispatcher, _, handle_a) = make_capturing_alerter();
        auto_clone_repos(&config_a, &dispatcher).await;
        drop(dispatcher);
        handle_a.await.expect("alert background task panicked");

        let config_b = minimal_config(
            repo_dir.path(),
            "dedup-drift-repo",
            &remote_b.path().display().to_string(),
        );
        let (dispatcher2, captured, handle_b) = make_capturing_alerter();

        // Two calls with the same mismatch — dedup should suppress the second.
        auto_clone_repos(&config_b, &dispatcher2).await;
        auto_clone_repos(&config_b, &dispatcher2).await;
        drop(dispatcher2);
        handle_b.await.expect("alert background task panicked");

        let alerts = captured.lock().unwrap();
        assert_eq!(
            alerts.len(),
            1,
            "dedup should suppress the second identical drift alert"
        );
    }

    #[tokio::test]
    async fn auto_clone_repos_alerts_on_subprocess_failure() {
        let repo_dir = tempfile::tempdir().unwrap();
        prepare_repo_dirs_for_test(repo_dir.path(), "corrupt-repo");

        // Create .git/ as an empty directory so the exists() check passes but
        // `git config --get remote.origin.url` exits non-zero.
        std::fs::create_dir_all(repo_dir.path().join("corrupt-repo").join(".git")).unwrap();

        let config = minimal_config(
            repo_dir.path(),
            "corrupt-repo",
            "https://example.com/repo.git",
        );
        let (dispatcher, captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &dispatcher).await;
        drop(dispatcher);
        handle.await.expect("alert background task panicked");

        let alerts = captured.lock().unwrap();
        assert_eq!(
            alerts.len(),
            1,
            "expected exactly one subprocess-failure alert"
        );
        let (title, body) = &alerts[0];
        assert_eq!(title, "repo remote URL check failed");
        assert!(body.contains("corrupt-repo"), "body should contain slug");
    }

    #[tokio::test]
    async fn auto_clone_repos_subprocess_failure_dedup_within_process() {
        let repo_dir = tempfile::tempdir().unwrap();
        prepare_repo_dirs_for_test(repo_dir.path(), "corrupt-dedup-repo");
        std::fs::create_dir_all(repo_dir.path().join("corrupt-dedup-repo").join(".git")).unwrap();

        let config = minimal_config(
            repo_dir.path(),
            "corrupt-dedup-repo",
            "https://example.com/repo.git",
        );
        let (dispatcher, captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &dispatcher).await;
        auto_clone_repos(&config, &dispatcher).await;
        drop(dispatcher);
        handle.await.expect("alert background task panicked");

        assert_eq!(
            captured.lock().unwrap().len(),
            1,
            "dedup should suppress the second subprocess-failure alert"
        );
    }

    #[tokio::test]
    async fn auto_clone_repos_drift_and_failure_keys_are_distinct() {
        let remote_a = make_bare_remote();
        let remote_b = make_bare_remote();
        let repo_dir = tempfile::tempdir().unwrap();

        // Repo 1: corrupted .git/ (subprocess failure).
        prepare_repo_dirs_for_test(repo_dir.path(), "fail-repo");
        std::fs::create_dir_all(repo_dir.path().join("fail-repo").join(".git")).unwrap();

        // Repo 2: proper clone but config points at a different remote.
        prepare_repo_dirs_for_test(repo_dir.path(), "drift-repo2");
        let config_for_clone = BrennConfig {
            repo_sync: repo_sync_at(repo_dir.path()),
            repos: vec![brenn_lib::config::RepoDeclRaw {
                slug: "drift-repo2".to_string(),
                remote: remote_a.path().display().to_string(),
                auto_pull: true,
            }],
            ..BrennConfig::default()
        };
        let (dispatcher_clone, _, handle_clone) = make_capturing_alerter();
        auto_clone_repos(&config_for_clone, &dispatcher_clone).await;
        drop(dispatcher_clone);
        handle_clone.await.expect("alert background task panicked");

        // Now a combined config: fail-repo points at a URL, drift-repo2 points at remote B.
        let config = BrennConfig {
            repo_sync: repo_sync_at(repo_dir.path()),
            repos: vec![
                brenn_lib::config::RepoDeclRaw {
                    slug: "fail-repo".to_string(),
                    remote: "https://example.com/fail.git".to_string(),
                    auto_pull: true,
                },
                brenn_lib::config::RepoDeclRaw {
                    slug: "drift-repo2".to_string(),
                    remote: remote_b.path().display().to_string(),
                    auto_pull: true,
                },
            ],
            ..BrennConfig::default()
        };
        let (dispatcher, captured, handle) = make_capturing_alerter();
        auto_clone_repos(&config, &dispatcher).await;
        drop(dispatcher);
        handle.await.expect("alert background task panicked");

        let alerts = captured.lock().unwrap();
        assert_eq!(alerts.len(), 2, "expected one drift + one failure alert");
        let titles: Vec<&str> = alerts.iter().map(|(t, _)| t.as_str()).collect();
        assert!(
            titles.contains(&"repo remote URL drift"),
            "expected a drift alert"
        );
        assert!(
            titles.contains(&"repo remote URL check failed"),
            "expected a failure alert"
        );
    }

    #[tokio::test]
    async fn auto_clone_repos_alerts_on_unset_origin() {
        let repo_dir = tempfile::tempdir().unwrap();
        prepare_repo_dirs_for_test(repo_dir.path(), "unset-origin-repo");

        // git init creates a .git/ but no remote.origin.url.
        init_repo(&repo_dir.path().join("unset-origin-repo"));

        let config = minimal_config(
            repo_dir.path(),
            "unset-origin-repo",
            "https://example.com/expected.git",
        );
        let (dispatcher, captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &dispatcher).await;
        drop(dispatcher);
        handle.await.expect("alert background task panicked");

        let alerts = captured.lock().unwrap();
        assert_eq!(alerts.len(), 1, "expected exactly one alert");
        let (title, body) = &alerts[0];
        assert_eq!(
            title, "repo remote URL check failed",
            "unset remote.origin.url routes through subprocess-failure, not drift"
        );
        assert!(
            body.contains("unset-origin-repo"),
            "body should contain slug"
        );
    }

    // ── Test 7c: drift-check failure log line contains no \n or \r ──────────

    /// Regression guard: the `"repo remote URL check failed"` warn! record must
    /// be a single line (no embedded `\n`/`\r`) even when the subprocess emits
    /// them in stderr. With a real git binary and an empty `.git/` dir, stderr
    /// is actually empty, but the assertion form pins the call-site invariant
    /// for future regressions.
    #[tokio::test]
    #[traced_test]
    async fn drift_check_failure_log_is_single_line() {
        let repo_dir = tempfile::tempdir().unwrap();
        prepare_repo_dirs_for_test(repo_dir.path(), "log-single-line-repo");

        // Create .git/ as an empty directory so the exists() check passes but
        // `git config --get remote.origin.url` exits non-zero.
        std::fs::create_dir_all(repo_dir.path().join("log-single-line-repo").join(".git")).unwrap();

        let config = minimal_config(
            repo_dir.path(),
            "log-single-line-repo",
            "https://example.com/repo.git",
        );
        let (dispatcher, _captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &dispatcher).await;
        drop(dispatcher);
        handle.await.expect("alert background task panicked");

        // Assert the captured "repo remote URL check failed" warn record
        // contains no embedded newlines or carriage returns.
        logs_assert(|lines: &[&str]| {
            for line in lines {
                if line.contains("repo remote URL check failed")
                    && (line.contains('\n') || line.contains('\r'))
                {
                    return Err(format!("log line contains embedded newline/CR: {line:?}"));
                }
            }
            Ok(())
        });
    }

    // ── Test 7d: drift-path warn! log line contains no \n or \r ────────────────
    //
    // Regression guard: the `"repo remote URL drift detected"` warn! record
    // must be a single line even when `actual` or `expected_remote` contain
    // embedded newlines. The unit tests (7a/7b) verify `sanitize_log_line`
    // itself; this integration test pins the call-site wiring.
    #[tokio::test]
    #[traced_test]
    async fn drift_check_drift_log_is_single_line() {
        let remote_a = make_bare_remote();
        let remote_b = make_bare_remote();
        let repo_dir = tempfile::tempdir().unwrap();
        prepare_repo_dirs_for_test(repo_dir.path(), "drift-log-test-repo");
        let remote_a_url = remote_a.path().display().to_string();
        let remote_b_url = remote_b.path().display().to_string();

        // Clone from remote A.
        let config_a = minimal_config(repo_dir.path(), "drift-log-test-repo", &remote_a_url);
        let (dispatcher, _, handle_a) = make_capturing_alerter();
        auto_clone_repos(&config_a, &dispatcher).await;
        drop(dispatcher);
        handle_a.await.expect("alert background task panicked");

        // Second call with a different expected remote → triggers drift path.
        let config_b = minimal_config(repo_dir.path(), "drift-log-test-repo", &remote_b_url);
        let (dispatcher2, _captured, handle_b) = make_capturing_alerter();
        auto_clone_repos(&config_b, &dispatcher2).await;
        drop(dispatcher2);
        handle_b.await.expect("alert background task panicked");

        // Assert the "repo remote URL drift detected" warn record has no embedded
        // newlines or carriage returns. The actual URL values from real git repos
        // are single-line paths, so this test currently passes trivially; it
        // pins the sanitization call-site wiring against future regressions.
        logs_assert(|lines: &[&str]| {
            for line in lines {
                if line.contains("repo remote URL drift detected")
                    && (line.contains('\n') || line.contains('\r'))
                {
                    return Err(format!("log line contains embedded newline/CR: {line:?}"));
                }
            }
            Ok(())
        });
    }

    // -----------------------------------------------------------------------
    // Clone failure handling
    // -----------------------------------------------------------------------

    /// `BrennConfig` with one repo declaration per `(slug, remote)` pair, each
    /// target directory created as `prepare_repo_dirs` does at startup.
    fn multi_repo_config(repo_dir: &std::path::Path, repos: &[(&str, &str)]) -> BrennConfig {
        for (slug, _) in repos {
            prepare_repo_dirs_for_test(repo_dir, slug);
        }
        BrennConfig {
            repo_sync: repo_sync_at(repo_dir),
            repos: repos
                .iter()
                .map(|(slug, remote)| RepoDeclRaw {
                    slug: slug.to_string(),
                    remote: remote.to_string(),
                    auto_pull: true,
                })
                .collect(),
            ..BrennConfig::default()
        }
    }

    /// Run `auto_clone_repos` expecting a panic; return the panic message.
    async fn auto_clone_repos_panic_message(config: &BrennConfig) -> String {
        let (dispatcher, _captured, _handle) = make_capturing_alerter();
        let payload = AssertUnwindSafe(auto_clone_repos(config, &dispatcher))
            .catch_unwind()
            .await
            .expect_err("auto_clone_repos should panic");
        panic_payload_message(payload)
    }

    fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
        match payload.downcast::<String>() {
            Ok(msg) => *msg,
            Err(payload) => payload
                .downcast_ref::<&str>()
                .expect("panic payload should be a string")
                .to_string(),
        }
    }

    /// Populate `target` the way a clone killed mid-fetch leaves it: a partial
    /// `.git` tree plus checked-out files.
    fn plant_clone_junk(target: &std::path::Path) {
        std::fs::create_dir_all(target.join(".git/objects/pack")).unwrap();
        std::fs::write(target.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
        std::fs::write(target.join(".git/objects/pack/tmp_pack_x"), "partial").unwrap();
        std::fs::create_dir_all(target.join("src")).unwrap();
        std::fs::write(target.join("src/lib.rs"), "").unwrap();
        std::fs::write(target.join("README"), "").unwrap();
    }

    fn clone_failure(slug: &str, target: &std::path::Path) -> CloneFailure {
        CloneFailure {
            slug: slug.to_string(),
            remote: format!("ssh://git.example.com/{slug}.git"),
            target: target.to_path_buf(),
            failure: format!("git clone: timeout ({slug})"),
        }
    }

    fn assert_empty_dir(dir: &std::path::Path) {
        assert!(dir.is_dir(), "{dir:?} should exist as a directory");
        let entries: Vec<_> = std::fs::read_dir(dir).unwrap().collect();
        assert!(entries.is_empty(), "{dir:?} should be empty: {entries:?}");
    }

    #[tokio::test]
    async fn auto_clone_repos_failure_resets_target_and_panics() {
        let missing = tempfile::tempdir().unwrap();
        let bad_remote = missing.path().join("no-such-remote");
        let repo_dir = tempfile::tempdir().unwrap();
        let config = multi_repo_config(
            repo_dir.path(),
            &[("bad-repo", &bad_remote.display().to_string())],
        );

        let msg = auto_clone_repos_panic_message(&config).await;

        assert!(msg.contains("git clone failed"), "message: {msg}");
        assert!(msg.contains("bad-repo"), "message: {msg}");
        assert!(msg.contains("(exit "), "message: {msg}");
        assert!(!msg.contains('\n'), "message must be one line: {msg:?}");
        let target = repo_dir.path().join("bad-repo");
        assert_empty_dir(&target);
        assert!(!target.join(".git").exists());
    }

    #[tokio::test]
    async fn auto_clone_repos_failure_does_not_abort_sibling_clone() {
        let good_remote = make_bare_remote();
        let missing = tempfile::tempdir().unwrap();
        let bad_remote = missing.path().join("no-such-remote");
        let repo_dir = tempfile::tempdir().unwrap();
        let config = multi_repo_config(
            repo_dir.path(),
            &[
                ("bad-repo", &bad_remote.display().to_string()),
                ("good-repo", &good_remote.path().display().to_string()),
            ],
        );

        let msg = auto_clone_repos_panic_message(&config).await;

        assert!(msg.contains("bad-repo"), "message: {msg}");
        assert!(!msg.contains("good-repo"), "message: {msg}");
        let good = repo_dir.path().join("good-repo");
        assert!(good.join(".git").join("HEAD").exists());
        assert_eq!(
            std::fs::read_to_string(good.join("file.txt")).unwrap(),
            "initial",
            "sibling clone must complete with its work tree checked out"
        );
        assert_empty_dir(&repo_dir.path().join("bad-repo"));
    }

    #[tokio::test]
    async fn auto_clone_repos_panic_names_every_failed_slug() {
        let missing = tempfile::tempdir().unwrap();
        let bad_a = missing.path().join("no-such-a");
        let bad_b = missing.path().join("no-such-b");
        let repo_dir = tempfile::tempdir().unwrap();
        let config = multi_repo_config(
            repo_dir.path(),
            &[
                ("bad-repo-a", &bad_a.display().to_string()),
                ("bad-repo-b", &bad_b.display().to_string()),
            ],
        );

        let msg = auto_clone_repos_panic_message(&config).await;

        assert!(msg.contains("2 repo(s)"), "message: {msg}");
        assert!(msg.contains("bad-repo-a"), "message: {msg}");
        assert!(msg.contains("bad-repo-b"), "message: {msg}");
        assert_empty_dir(&repo_dir.path().join("bad-repo-a"));
        assert_empty_dir(&repo_dir.path().join("bad-repo-b"));
    }

    #[tokio::test]
    async fn auto_clone_repos_refuses_nonempty_target_without_git_untouched() {
        let remote = make_bare_remote();
        let remote_url = remote.path().display().to_string();
        let repo_dir = tempfile::tempdir().unwrap();
        let config = multi_repo_config(
            repo_dir.path(),
            &[("occupied-repo", &remote_url), ("other-repo", &remote_url)],
        );
        let occupied = repo_dir.path().join("occupied-repo");
        std::fs::write(occupied.join("notes.txt"), "operator content").unwrap();

        let msg = auto_clone_repos_panic_message(&config).await;

        assert!(msg.contains("occupied-repo"), "message: {msg}");
        assert!(
            msg.contains(&format!("{occupied:?}")),
            "message should name the path: {msg}"
        );
        assert_eq!(
            std::fs::read_to_string(occupied.join("notes.txt")).unwrap(),
            "operator content"
        );
        assert!(!occupied.join(".git").exists());
        // The pre-check runs before any clone is spawned.
        assert_empty_dir(&repo_dir.path().join("other-repo"));
    }

    #[test]
    fn reset_failed_target_empties_partial_clone() {
        let target = tempfile::tempdir().unwrap();
        plant_clone_junk(target.path());

        reset_failed_target(target.path()).unwrap();

        assert_empty_dir(target.path());
    }

    #[test]
    fn reset_failed_target_keeps_symlinked_target() {
        let storage = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        let link = repo_dir.path().join("linked-repo");
        std::os::unix::fs::symlink(storage.path(), &link).unwrap();
        plant_clone_junk(&link);

        reset_failed_target(&link).unwrap();

        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink(),
            "the symlink must survive the reset"
        );
        assert_empty_dir(storage.path());
    }

    #[test]
    fn refuse_boot_resets_every_failed_target_and_reports_reset_failures() {
        let repo_dir = tempfile::tempdir().unwrap();
        // A regular file where a directory is expected: its reset fails.
        let unresettable = repo_dir.path().join("unresettable-repo");
        std::fs::write(&unresettable, "").unwrap();
        let partial = repo_dir.path().join("partial-repo");
        plant_clone_junk(&partial);

        let payload = std::panic::catch_unwind(AssertUnwindSafe(|| {
            refuse_boot_on_clone_failures(vec![
                clone_failure("unresettable-repo", &unresettable),
                clone_failure("partial-repo", &partial),
            ])
        }))
        .expect_err("refuse_boot_on_clone_failures should panic");
        let msg = panic_payload_message(payload);

        assert!(
            msg.contains("git clone failed for 2 repo(s)"),
            "message: {msg}"
        );
        assert!(
            msg.contains("unresettable-repo: git clone: timeout (unresettable-repo)"),
            "message: {msg}"
        );
        assert!(
            msg.contains("partial-repo: git clone: timeout (partial-repo)"),
            "message: {msg}"
        );
        assert!(
            msg.contains("could not reset 1 failed target(s)"),
            "message: {msg}"
        );
        assert!(
            msg.contains(&format!("{:?}", unresettable.join(".git"))),
            "message should name the unresettable path: {msg}"
        );
        assert!(!msg.contains('\n'), "message must be one line: {msg:?}");
        // A failed reset does not stop the later targets being reset.
        assert_empty_dir(&partial);
    }

    #[test]
    fn describe_clone_error_renders_every_variant_on_one_line() {
        let label = || "git clone".to_string();
        let cases = [
            (
                GitSubprocessError::NonZero {
                    label: label(),
                    stdout: String::new(),
                    stderr: "Host key verification failed.\nfatal: Could not read\r\n".to_string(),
                    exit_code: Some(128),
                },
                "git clone: non-zero exit: Host key verification failed. fatal: Could not read   (exit 128)",
            ),
            (
                GitSubprocessError::NonZero {
                    label: label(),
                    stdout: String::new(),
                    stderr: "partial\n".to_string(),
                    exit_code: None,
                },
                "git clone: non-zero exit: partial  (killed by signal)",
            ),
            (
                GitSubprocessError::Timeout { label: label() },
                "git clone: timeout",
            ),
            (
                GitSubprocessError::SpawnFailed {
                    label: label(),
                    source: std::io::Error::new(std::io::ErrorKind::NotFound, "no git\nhere"),
                },
                "git clone: spawn failed: no git here",
            ),
            (
                GitSubprocessError::OutputTooLarge {
                    label: label(),
                    stream: crate::subprocess::OutputStream::Stderr,
                },
                "git clone: git output too large (stderr)",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(describe_clone_error(&error), expected, "{error:?}");
        }
    }
}

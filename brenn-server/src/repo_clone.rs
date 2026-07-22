//! Repo clone selection and auto-cloning.

use std::path::PathBuf;

use futures::future::FutureExt as _;
use indexmap::IndexMap;
use tracing::{info, warn};

use brenn_lib::config::{AppConfig, BrennConfig, ContainerSpawnConfig};
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};

/// Pick the container config under which to clone a repo, if any.
///
/// Returns `Some(container_spawn)` if any containerized app mounts this repo
/// with writable access. Otherwise returns `None`, meaning clone on the host.
///
/// Read-only-only repos must clone on the host: every containerized app that
/// mounts the repo bind-mounts it `:ro`, so cloning into the container's view
/// of that path writes through a read-only bind and fails with EROFS.
pub(crate) fn select_clone_container<'a>(
    apps: &'a IndexMap<String, AppConfig>,
    repo_slug: &str,
) -> Option<&'a ContainerSpawnConfig> {
    apps.values()
        .find(|app| {
            app.container_spawn.is_some()
                && app.mounts.iter().any(|m| {
                    m.slug == repo_slug && m.access == brenn_lib::config::AccessLevel::ReadWrite
                })
        })
        .and_then(|app| app.container_spawn.as_ref())
}

/// Read the on-disk clone's `remote.origin.url` and compare it against the
/// configured remote URL. Fires a deduplicated warning alert on mismatch or
/// subprocess failure. Never panics on subprocess errors.
async fn check_remote_url_drift(
    slug: String,
    expected_remote: String,
    clone_path: PathBuf,
    alert_dispatcher: AlertDispatcher,
) {
    use crate::git_subprocess::{run_git, sanitize_log_line};

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

/// Auto-clone repos that don't have a `.git` directory yet.
///
/// Runs after `validate_and_resolve()` so we have `ContainerSpawnConfig` for
/// container-side clones. Empty directories were created pre-validation so
/// working_dir checks passed; now we populate them with actual git repos.
///
/// For each repo, uses `select_clone_container` to choose container-side
/// (container-side SSH keys) vs host-side (RO-only repos and bare-process-only
/// repos).
///
/// Clones run concurrently — each `git clone` is independent (different target
/// directory, immutable `apps` read).
pub(crate) async fn auto_clone_repos(
    config: &BrennConfig,
    apps: &IndexMap<String, AppConfig>,
    alert_dispatcher: &AlertDispatcher,
) {
    let repo_dir = config.repo_dir.as_ref().expect("caller checked repo_dir");

    let clone_futs: Vec<futures::future::BoxFuture<'static, ()>> = config
        .repos
        .iter()
        .map(|repo| {
            let target = repo_dir.join(&repo.slug);
            if target.join(".git").exists() {
                // Drift check: compare config remote against on-disk remote.origin.url.
                let slug = repo.slug.clone();
                let expected_remote = repo.remote.clone();
                let ad = alert_dispatcher.clone();
                return check_remote_url_drift(slug, expected_remote, target, ad).boxed();
            }

            info!(
                slug = %repo.slug,
                remote = %repo.remote,
                target = %target.display(),
                "auto-cloning repo",
            );

            let container_spawn = select_clone_container(apps, &repo.slug);

            // Clone target path: container-side path if containerized, host path if bare.
            let clone_target = if let Some(spawn) = container_spawn {
                spawn
                    .container_home
                    .join("repos")
                    .join(&repo.slug)
                    .to_string_lossy()
                    .to_string()
            } else {
                target.display().to_string()
            };

            let slug = repo.slug.clone();
            let remote = repo.remote.clone();
            let cmd = brenn_lib::subprocess::run_in_app_env(
                "git",
                &["clone", &remote, &clone_target],
                &target,
                container_spawn,
                &[],
                &[],
            );
            async move {
                let label = format!("git clone slug={slug}");
                match crate::git_subprocess::run_with_bounded_output(cmd, &label).await {
                    Ok(_) => info!(slug = %slug, "auto-clone complete"),
                    Err(crate::git_subprocess::GitSubprocessError::NonZero {
                        stderr,
                        exit_code,
                        // stdout intentionally omitted: git clone errors go to
                        // stderr; stdout is empty or progress noise in practice.
                        ..
                    }) => {
                        panic!("git clone failed for repo {slug:?} (exit {exit_code:?}): {stderr}")
                    }
                    Err(e) => panic!("git clone failed for repo {slug:?}: {e}"),
                }
            }
            .boxed()
        })
        .collect();

    futures::future::join_all(clone_futs).await;
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use git_fixture::{git as fixture_git, init_bare_repo, init_repo};

    use brenn_lib::config::{AccessLevel, BrennConfig, RepoDeclRaw};
    use brenn_lib::obs::alerting::make_capturing_alerter;
    use indexmap::IndexMap;
    use tracing_test::traced_test;

    use crate::test_support::app_config::{clone_test_app, clone_test_container, clone_test_mount};

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

    /// Build a minimal `BrennConfig` with `repo_dir = repo_dir` and a single
    /// repo declaration pointing at `remote_url`.
    fn minimal_config(repo_dir: &std::path::Path, slug: &str, remote_url: &str) -> BrennConfig {
        BrennConfig {
            repo_dir: Some(repo_dir.to_path_buf()),
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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &apps, &dispatcher).await;

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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, captured, handle) = make_capturing_alerter();

        // First clone.
        auto_clone_repos(&config, &apps, &dispatcher).await;
        assert!(repo_dir.path().join("test-repo").join(".git").exists());

        // Second call: .git already exists, drift check runs. Config remote
        // matches the on-disk remote.origin.url — no alert expected.
        auto_clone_repos(&config, &apps, &dispatcher).await;
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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, _, handle_a) = make_capturing_alerter();
        auto_clone_repos(&config_a, &apps, &dispatcher).await;
        drop(dispatcher);
        handle_a.await.expect("alert background task panicked");

        // Now build a config pointing at remote B (simulating an operator
        // changing the config but forgetting git remote set-url).
        let config_b = minimal_config(repo_dir.path(), "drift-repo", &remote_b_url);
        let (dispatcher2, captured, handle_b) = make_capturing_alerter();
        auto_clone_repos(&config_b, &apps, &dispatcher2).await;
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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, captured, handle) = make_capturing_alerter();

        // Clone, then check again with the same config.
        auto_clone_repos(&config, &apps, &dispatcher).await;
        auto_clone_repos(&config, &apps, &dispatcher).await;
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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, _, handle_a) = make_capturing_alerter();
        auto_clone_repos(&config_a, &apps, &dispatcher).await;
        drop(dispatcher);
        handle_a.await.expect("alert background task panicked");

        let config_b = minimal_config(
            repo_dir.path(),
            "dedup-drift-repo",
            &remote_b.path().display().to_string(),
        );
        let (dispatcher2, captured, handle_b) = make_capturing_alerter();

        // Two calls with the same mismatch — dedup should suppress the second.
        auto_clone_repos(&config_b, &apps, &dispatcher2).await;
        auto_clone_repos(&config_b, &apps, &dispatcher2).await;
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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &apps, &dispatcher).await;
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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &apps, &dispatcher).await;
        auto_clone_repos(&config, &apps, &dispatcher).await;
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
            repo_dir: Some(repo_dir.path().to_path_buf()),
            repos: vec![brenn_lib::config::RepoDeclRaw {
                slug: "drift-repo2".to_string(),
                remote: remote_a.path().display().to_string(),
                auto_pull: true,
            }],
            ..BrennConfig::default()
        };
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher_clone, _, handle_clone) = make_capturing_alerter();
        auto_clone_repos(&config_for_clone, &apps, &dispatcher_clone).await;
        drop(dispatcher_clone);
        handle_clone.await.expect("alert background task panicked");

        // Now a combined config: fail-repo points at a URL, drift-repo2 points at remote B.
        let config = BrennConfig {
            repo_dir: Some(repo_dir.path().to_path_buf()),
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
        auto_clone_repos(&config, &apps, &dispatcher).await;
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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &apps, &dispatcher).await;
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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, _captured, handle) = make_capturing_alerter();

        auto_clone_repos(&config, &apps, &dispatcher).await;
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
        let apps: IndexMap<String, _> = IndexMap::new();
        let (dispatcher, _, handle_a) = make_capturing_alerter();
        auto_clone_repos(&config_a, &apps, &dispatcher).await;
        drop(dispatcher);
        handle_a.await.expect("alert background task panicked");

        // Second call with a different expected remote → triggers drift path.
        let config_b = minimal_config(repo_dir.path(), "drift-log-test-repo", &remote_b_url);
        let (dispatcher2, _captured, handle_b) = make_capturing_alerter();
        auto_clone_repos(&config_b, &apps, &dispatcher2).await;
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

    #[test]
    fn select_clone_container_returns_none_for_ro_only_repo() {
        // A repo mounted read-only in two containerized apps — the RO bind in
        // each container makes container-side clone impossible. Must fall back
        // to host-side clone (return None).
        let mut apps = IndexMap::new();
        apps.insert(
            "pa-a".into(),
            clone_test_app(
                "pa-a",
                Some(clone_test_container("/tmp/home-a")),
                vec![clone_test_mount("src-brenn", AccessLevel::ReadOnly)],
            ),
        );
        apps.insert(
            "pa-b".into(),
            clone_test_app(
                "pa-b",
                Some(clone_test_container("/tmp/home-b")),
                vec![clone_test_mount("src-brenn", AccessLevel::ReadOnly)],
            ),
        );
        assert!(select_clone_container(&apps, "src-brenn").is_none());
    }

    #[test]
    fn select_clone_container_picks_container_for_rw_mount() {
        let mut apps = IndexMap::new();
        apps.insert(
            "pa-a".into(),
            clone_test_app(
                "pa-a",
                Some(clone_test_container("/tmp/home-a")),
                vec![clone_test_mount("life", AccessLevel::ReadWrite)],
            ),
        );
        let spawn = select_clone_container(&apps, "life").expect("RW mount should yield container");
        assert_eq!(spawn.home_dir, PathBuf::from("/tmp/home-a"));
    }

    #[test]
    fn select_clone_container_returns_none_when_only_bare_app_mounts_repo() {
        // An RW mount in a bare (non-containerized) app does not contribute a
        // container_spawn. No container → host-side clone (return None).
        let mut apps = IndexMap::new();
        apps.insert(
            "bare".into(),
            clone_test_app(
                "bare",
                None, // no container
                vec![clone_test_mount("life", AccessLevel::ReadWrite)],
            ),
        );
        assert!(select_clone_container(&apps, "life").is_none());
    }

    #[test]
    fn select_clone_container_prefers_rw_over_ro() {
        // Repo mounted RO in one app and RW in another. Return the RW's
        // container so clone can write through.
        let mut apps = IndexMap::new();
        apps.insert(
            "ro-app".into(),
            clone_test_app(
                "ro-app",
                Some(clone_test_container("/tmp/ro-home")),
                vec![clone_test_mount("mixed", AccessLevel::ReadOnly)],
            ),
        );
        apps.insert(
            "rw-app".into(),
            clone_test_app(
                "rw-app",
                Some(clone_test_container("/tmp/rw-home")),
                vec![clone_test_mount("mixed", AccessLevel::ReadWrite)],
            ),
        );
        let spawn =
            select_clone_container(&apps, "mixed").expect("RW mount should yield container");
        assert_eq!(spawn.home_dir, PathBuf::from("/tmp/rw-home"));
    }

    #[test]
    fn select_clone_container_returns_none_when_ro_containerized_and_rw_bare() {
        // Repo mounted RO in a containerized app AND RW in a bare app. No
        // writable containerized mount exists, so return None (host clone).
        // The host-side clone populates the dir that both apps see.
        let mut apps = IndexMap::new();
        apps.insert(
            "containerized".into(),
            clone_test_app(
                "containerized",
                Some(clone_test_container("/tmp/home-a")),
                vec![clone_test_mount("shared", AccessLevel::ReadOnly)],
            ),
        );
        apps.insert(
            "bare".into(),
            clone_test_app(
                "bare",
                None,
                vec![clone_test_mount("shared", AccessLevel::ReadWrite)],
            ),
        );
        assert!(select_clone_container(&apps, "shared").is_none());
    }

    #[test]
    fn select_clone_container_returns_none_for_unknown_slug() {
        let mut apps = IndexMap::new();
        apps.insert(
            "pa-a".into(),
            clone_test_app(
                "pa-a",
                Some(clone_test_container("/tmp/home-a")),
                vec![clone_test_mount("life", AccessLevel::ReadWrite)],
            ),
        );
        assert!(select_clone_container(&apps, "not-a-slug").is_none());
    }
}

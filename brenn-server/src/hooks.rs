//! Lifecycle hooks: conversation start, post-pull, and server startup.
//!
//! Start hooks run on new conversation start (never on resume).
//! Post-pull hooks run after a successful repo pull advances HEAD.
//! Startup hooks run once at server startup after all startup pulls succeed.

use std::path::Path;
use std::time::Duration;

use brenn_lib::config::{AppConfig, ContainerSpawnConfig, ResolvedMount};
use tokio::process::Command;
use tracing::{debug, info, warn};

use crate::repo_sync::git::{PullOutcome, pull_clone};

/// Timeout for each individual hook script.
const HOOK_TIMEOUT: Duration = Duration::from_secs(30);

/// Result of running start hooks. Contains warnings to surface to the user
/// (non-fatal issues like auto_pull failures) and any fatal error.
#[derive(Debug)]
pub struct HookResult {
    /// Non-fatal warnings to surface to the user at conversation start.
    pub warnings: Vec<String>,
}

/// Run all start hooks for a new conversation.
///
/// Execution order:
/// 1. Repo mount auto-pulls (concurrent, access-level-aware)
/// 2. `start_hooks.host` (in order)
/// 3. `start_hooks.container` (in order, containerized apps only)
///
/// Returns warnings for the user on success, or an error string on fatal failure.
pub async fn run_start_hooks(
    app_config: &AppConfig,
    conversation_id: i64,
) -> Result<HookResult, String> {
    let mut warnings = Vec::new();

    // 1. Repo mount auto-pulls (concurrent, host-side).
    let repo_warnings = auto_pull_mounts(&app_config.mounts).await;
    warnings.extend(repo_warnings);

    // 2. Host hooks (sequential)
    let env = hook_env(&app_config.slug, conversation_id, &app_config.working_dir);
    for hook in &app_config.start_hooks.host {
        run_host_hook(hook, &app_config.working_dir, &env, "start").await?;
    }

    // 3. Container hooks
    if let Some(ref container) = app_config.container_spawn {
        let container_env = hook_env(
            &app_config.slug,
            conversation_id,
            &container.container_working_dir,
        );
        for hook in &app_config.start_hooks.container {
            run_container_hook(hook, container, &container_env, "start").await?;
        }
    }

    Ok(HookResult { warnings })
}

/// Build environment variables for hook scripts.
fn hook_env(app_slug: &str, conversation_id: i64, working_dir: &Path) -> Vec<(String, String)> {
    vec![
        ("BRENN_APP_SLUG".into(), app_slug.into()),
        ("BRENN_CONVERSATION_ID".into(), conversation_id.to_string()),
        (
            "BRENN_WORKING_DIR".into(),
            working_dir.display().to_string(),
        ),
    ]
}

/// Run post-pull hooks for an app after a successful repo pull.
///
/// Unlike `run_start_hooks`, failures are collected as warnings (non-fatal)
/// rather than propagated as errors. Post-pull hooks fire while the server
/// is running; a hook failure must not crash it.
///
/// Env vars: `BRENN_APP_SLUG`, `BRENN_WORKING_DIR`, `BRENN_REPO_SLUG`.
/// No `BRENN_CONVERSATION_ID` — this is a server-level event.
pub async fn run_post_pull_hooks(app_config: &AppConfig, repo_slug: &str) -> Vec<String> {
    let mut warnings = Vec::new();

    let env = post_pull_hook_env(&app_config.slug, repo_slug, &app_config.working_dir);

    // Host hooks (sequential)
    for hook in &app_config.post_pull_hooks.host {
        if let Err(e) = run_host_hook(hook, &app_config.working_dir, &env, "post_pull").await {
            warnings.push(e);
        }
    }

    // Container hooks
    if let Some(ref container) = app_config.container_spawn {
        let container_env = post_pull_hook_env(
            &app_config.slug,
            repo_slug,
            &container.container_working_dir,
        );
        for hook in &app_config.post_pull_hooks.container {
            if let Err(e) = run_container_hook(hook, container, &container_env, "post_pull").await {
                warnings.push(e);
            }
        }
    }

    warnings
}

/// Run startup hooks for an app at server startup.
///
/// Runs host hooks then container hooks sequentially. Failure is an `Err` —
/// startup hooks failing is fatal because the operator configured them
/// because the app needs them.
///
/// Env vars: `BRENN_APP_SLUG`, `BRENN_WORKING_DIR`.
/// No `BRENN_CONVERSATION_ID` or `BRENN_REPO_SLUG`.
pub async fn run_startup_hooks(app_config: &AppConfig) -> Result<(), String> {
    let env = startup_hook_env(&app_config.slug, &app_config.working_dir);

    // Host hooks (sequential)
    for hook in &app_config.startup_hooks.host {
        run_host_hook(hook, &app_config.working_dir, &env, "startup").await?;
    }

    // Container hooks
    if let Some(ref container) = app_config.container_spawn {
        let container_env = startup_hook_env(&app_config.slug, &container.container_working_dir);
        for hook in &app_config.startup_hooks.container {
            run_container_hook(hook, container, &container_env, "startup").await?;
        }
    }

    Ok(())
}

fn post_pull_hook_env(
    app_slug: &str,
    repo_slug: &str,
    working_dir: &Path,
) -> Vec<(String, String)> {
    vec![
        ("BRENN_APP_SLUG".into(), app_slug.into()),
        ("BRENN_REPO_SLUG".into(), repo_slug.into()),
        (
            "BRENN_WORKING_DIR".into(),
            working_dir.display().to_string(),
        ),
    ]
}

fn startup_hook_env(app_slug: &str, working_dir: &Path) -> Vec<(String, String)> {
    vec![
        ("BRENN_APP_SLUG".into(), app_slug.into()),
        (
            "BRENN_WORKING_DIR".into(),
            working_dir.display().to_string(),
        ),
    ]
}

/// Pull all `auto_pull = true` mounts concurrently on the host.
/// Returns collected warnings.
pub(crate) async fn auto_pull_mounts(mounts: &[ResolvedMount]) -> Vec<String> {
    let auto_pull_mounts: Vec<&ResolvedMount> = mounts.iter().filter(|m| m.auto_pull).collect();

    if auto_pull_mounts.is_empty() {
        return vec![];
    }

    let futures: Vec<_> = auto_pull_mounts
        .iter()
        .map(|mount| auto_pull_mount(mount))
        .collect();

    futures::future::join_all(futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Pull a single mount host-side using `pull_clone` for rich outcome classification.
///
/// Returns `None` on success (`UpToDate` or `Advanced`), or a warning string
/// describing the failure on `TransientError`, `AuthError`, or `Conflict`.
///
/// Note: `pull_clone` is hard-coded to `origin/main`. Mounts whose current
/// branch is not `main` will see fetch/merge against main regardless.
async fn auto_pull_mount(mount: &ResolvedMount) -> Option<String> {
    match pull_clone(&mount.host_path).await {
        PullOutcome::UpToDate | PullOutcome::Advanced { .. } => None,
        PullOutcome::TransientError(msg) => {
            warn!(repo = %mount.slug, "auto_pull transient error: {msg}");
            Some(format!(
                "auto_pull repo {:?}: transient error during pull: {msg}",
                mount.slug,
            ))
        }
        PullOutcome::AuthError { reason, detail } => {
            warn!(repo = %mount.slug, "auto_pull auth error: {reason}. {detail}");
            Some(format!(
                "auto_pull repo {:?}: auth error during pull: {reason}. {detail}",
                mount.slug,
            ))
        }
        PullOutcome::Conflict { reason, detail } => {
            warn!(repo = %mount.slug, "auto_pull conflict: {reason}. {detail}");
            Some(format!(
                "auto_pull repo {:?}: conflict during pull: {reason}. {detail}",
                mount.slug,
            ))
        }
    }
}

/// Run a single host hook script via `sh -c`.
///
/// `phase` identifies the lifecycle trigger (e.g. `"start"`, `"post_pull"`,
/// `"startup"`) for log and error messages.
async fn run_host_hook(
    hook: &str,
    cwd: &Path,
    env: &[(String, String)],
    phase: &str,
) -> Result<(), String> {
    info!(hook, cwd = %cwd.display(), phase, "running host hook");
    let start = std::time::Instant::now();

    let result = tokio::time::timeout(
        HOOK_TIMEOUT,
        Command::new("sh")
            .args(["-c", hook])
            .current_dir(cwd)
            .envs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .output(),
    )
    .await;

    match result {
        Ok(Ok(output)) => {
            let elapsed = start.elapsed();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stdout.is_empty() {
                debug!(hook, "stdout: {}", stdout.trim());
            }
            if !stderr.is_empty() {
                debug!(hook, "stderr: {}", stderr.trim());
            }
            if output.status.success() {
                info!(
                    hook,
                    elapsed_ms = elapsed.as_millis(),
                    phase,
                    "host hook completed"
                );
                Ok(())
            } else {
                Err(format!(
                    "{phase} hook `{hook}` failed (exit {}): {}",
                    output.status,
                    stderr.trim(),
                ))
            }
        }
        Ok(Err(e)) => Err(format!("{phase} hook `{hook}` failed to execute: {e}")),
        Err(_) => Err(format!(
            "{phase} hook `{hook}` timed out after {}s",
            HOOK_TIMEOUT.as_secs(),
        )),
    }
}

/// Run a single container hook script via `podman run --rm`.
///
/// `phase` identifies the lifecycle trigger for log and error messages.
/// Not tested in unit tests — requires podman and a container image.
async fn run_container_hook(
    hook: &str,
    container: &ContainerSpawnConfig,
    env: &[(String, String)],
    phase: &str,
) -> Result<(), String> {
    info!(hook, image = %container.image, phase, "running container hook");
    let start = std::time::Instant::now();

    let mut args = container.base_podman_args();

    // Pass hook environment variables (inserted before the image name).
    let env_flags: Vec<String> = env
        .iter()
        .flat_map(|(k, v)| ["-e".into(), format!("{k}={v}")])
        .collect();
    ContainerSpawnConfig::insert_podman_flags(&mut args, &env_flags);

    // Command: sh -c "{hook}"
    args.push("sh".into());
    args.push("-c".into());
    args.push(hook.into());

    let result =
        tokio::time::timeout(HOOK_TIMEOUT, Command::new("podman").args(&args).output()).await;

    match result {
        Ok(Ok(output)) => {
            let elapsed = start.elapsed();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stdout.is_empty() {
                debug!(hook, "stdout: {}", stdout.trim());
            }
            if !stderr.is_empty() {
                debug!(hook, "stderr: {}", stderr.trim());
            }
            if output.status.success() {
                info!(
                    hook,
                    elapsed_ms = elapsed.as_millis(),
                    phase,
                    "container hook completed",
                );
                Ok(())
            } else {
                Err(format!(
                    "container {phase} hook `{hook}` failed (exit {}): {}",
                    output.status,
                    stderr.trim(),
                ))
            }
        }
        Ok(Err(e)) => Err(format!(
            "container {phase} hook `{hook}` failed to execute: {e}",
        )),
        Err(_) => Err(format!(
            "container {phase} hook `{hook}` timed out after {}s",
            HOOK_TIMEOUT.as_secs(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::config::{AccessLevel, AppConfig, PathMapper, ResolvedMount, StartHooksConfig};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::process::Command as StdCommand;

    /// Build a minimal AppConfig for testing. `working_dir` is the only
    /// field that matters for hook execution; the rest are inert defaults.
    fn test_app_config(working_dir: PathBuf) -> AppConfig {
        AppConfig {
            slug: "test".into(),
            name: "Test".into(),
            description: String::new(),
            icon: String::new(),
            working_dir,
            model: "sonnet".into(),
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout: None,
            compaction: None,
            idle_hook_secs: 0,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: false,
            prefix_timestamp: false,
            prefix_device: true,
            path_mapper: PathMapper::Identity,
            container_spawn: None,
            start_hooks: StartHooksConfig::default(),
            post_pull_hooks: brenn_lib::config::PostPullHooksConfig::default(),
            startup_hooks: brenn_lib::config::StartupHooksConfig::default(),
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: HashMap::new(),
            mounts: vec![],
            history_replay_limit: 2000,
            frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
            state_dir: PathBuf::from("/tmp/.brenn/test-state"),
            messaging: None,
            messaging_default_send_budget: 100,
            policy: brenn_lib::access::AppPolicy::default(),
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
        }
    }

    /// Run a git command with test credentials. Panics on failure.
    fn git_run(dir: &Path, args: &[&str]) {
        let out = StdCommand::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@test")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@test")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Create a git repo in `dir` with an initial commit.
    fn git_init(dir: &Path) {
        let run = |args: &[&str]| {
            let out = StdCommand::new("git")
                .args(args)
                .current_dir(dir)
                .env("GIT_AUTHOR_NAME", "Test")
                .env("GIT_AUTHOR_EMAIL", "test@test")
                .env("GIT_COMMITTER_NAME", "Test")
                .env("GIT_COMMITTER_EMAIL", "test@test")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
        };
        run(&["init", "-b", "main"]);
        std::fs::write(dir.join("file.txt"), "initial").unwrap();
        run(&["add", "."]);
        run(&["commit", "-m", "initial"]);
    }

    /// Create a "remote" bare repo, push `dir` to it, and set up tracking.
    /// Returns the path to the bare repo.
    fn git_add_remote(dir: &Path) -> tempfile::TempDir {
        let remote = tempfile::tempdir().unwrap();
        let run_at = |d: &Path, args: &[&str]| {
            let out = StdCommand::new("git")
                .args(args)
                .current_dir(d)
                .env("GIT_AUTHOR_NAME", "Test")
                .env("GIT_AUTHOR_EMAIL", "test@test")
                .env("GIT_COMMITTER_NAME", "Test")
                .env("GIT_COMMITTER_EMAIL", "test@test")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
        };
        run_at(remote.path(), &["init", "--bare", "-b", "main"]);
        run_at(
            dir,
            &[
                "remote",
                "add",
                "origin",
                &remote.path().display().to_string(),
            ],
        );
        run_at(dir, &["push", "-u", "origin", "main"]);
        remote
    }

    // -----------------------------------------------------------------------
    // mount auto_pull tests
    // -----------------------------------------------------------------------

    /// Helper: make a ResolvedMount for testing, pointing at a real directory.
    fn test_mount(slug: &str, path: PathBuf, auto_pull: bool) -> ResolvedMount {
        ResolvedMount {
            slug: slug.to_string(),
            host_path: path,
            container_path: None,
            access: AccessLevel::ReadWrite,
            auto_pull,
            is_working_dir: false,
            primary: false,
        }
    }

    #[tokio::test]
    async fn auto_pull_not_a_git_repo() {
        let dir = tempfile::tempdir().unwrap();
        let working_dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(working_dir.path().to_path_buf());
        config.mounts = vec![test_mount("testrepo", dir.path().to_path_buf(), true)];

        let result = run_start_hooks(&config, 1).await.unwrap();
        // A non-git dir fails rev-parse HEAD → classified as Conflict.
        assert_eq!(result.warnings.len(), 1);
        assert!(
            result.warnings[0].contains("conflict during pull"),
            "expected 'conflict during pull' in warning: {}",
            result.warnings[0]
        );
        assert!(
            result.warnings[0].contains("rev-parse HEAD failed"),
            "expected 'rev-parse HEAD failed' in warning: {}",
            result.warnings[0]
        );
    }

    #[tokio::test]
    async fn auto_pull_dirty_working_tree_already_up_to_date() {
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let _remote = git_add_remote(dir.path());

        // Dirty the working tree. Already up-to-date with remote, so
        // git pull --ff-only succeeds (nothing to pull).
        std::fs::write(dir.path().join("file.txt"), "dirty").unwrap();

        let working_dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(working_dir.path().to_path_buf());
        config.mounts = vec![test_mount("testrepo", dir.path().to_path_buf(), true)];

        let result = run_start_hooks(&config, 1).await.unwrap();
        assert!(
            result.warnings.is_empty(),
            "already up-to-date pull should succeed even with dirty tree"
        );
    }

    #[tokio::test]
    async fn auto_pull_no_upstream() {
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        // No remote added — `git fetch origin main` will fail with "no such remote".

        let working_dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(working_dir.path().to_path_buf());
        config.mounts = vec![test_mount("testrepo", dir.path().to_path_buf(), true)];

        let result = run_start_hooks(&config, 1).await.unwrap();
        // "No such remote 'origin'" → classify_fetch_error → Other → Conflict.
        assert_eq!(result.warnings.len(), 1);
        assert!(
            result.warnings[0].contains("conflict during pull"),
            "expected 'conflict during pull' in warning: {}",
            result.warnings[0]
        );
    }

    #[tokio::test]
    async fn auto_pull_already_up_to_date() {
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let _remote = git_add_remote(dir.path());

        let working_dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(working_dir.path().to_path_buf());
        config.mounts = vec![test_mount("testrepo", dir.path().to_path_buf(), true)];

        let result = run_start_hooks(&config, 1).await.unwrap();
        assert!(
            result.warnings.is_empty(),
            "up-to-date pull should succeed silently"
        );
    }

    #[tokio::test]
    async fn auto_pull_fast_forwards() {
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let remote = git_add_remote(dir.path());

        // Clone to a second working copy, commit, push — so `dir` is behind.
        let clone_dir = tempfile::tempdir().unwrap();
        let run = |d: &Path, args: &[&str]| {
            let out = StdCommand::new("git")
                .args(args)
                .current_dir(d)
                .env("GIT_AUTHOR_NAME", "Test")
                .env("GIT_AUTHOR_EMAIL", "test@test")
                .env("GIT_COMMITTER_NAME", "Test")
                .env("GIT_COMMITTER_EMAIL", "test@test")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
        };
        run(
            clone_dir.path(),
            &["clone", &remote.path().display().to_string(), "."],
        );
        std::fs::write(clone_dir.path().join("new.txt"), "new content").unwrap();
        run(clone_dir.path(), &["add", "."]);
        run(clone_dir.path(), &["commit", "-m", "new commit"]);
        run(clone_dir.path(), &["push"]);

        // Now dir is 1 commit behind origin.
        assert!(!dir.path().join("new.txt").exists());

        let working_dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(working_dir.path().to_path_buf());
        config.mounts = vec![test_mount("testrepo", dir.path().to_path_buf(), true)];

        let result = run_start_hooks(&config, 1).await.unwrap();
        assert!(
            result.warnings.is_empty(),
            "ff pull should succeed silently"
        );
        assert!(
            dir.path().join("new.txt").exists(),
            "pull should have brought in new file"
        );
    }

    #[tokio::test]
    async fn auto_pull_diverged_returns_warning() {
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let remote = git_add_remote(dir.path());

        let run = |d: &Path, args: &[&str]| {
            let out = StdCommand::new("git")
                .args(args)
                .current_dir(d)
                .env("GIT_AUTHOR_NAME", "Test")
                .env("GIT_AUTHOR_EMAIL", "test@test")
                .env("GIT_COMMITTER_NAME", "Test")
                .env("GIT_COMMITTER_EMAIL", "test@test")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
        };

        // Push a commit from a second clone — advance the remote.
        let clone_dir = tempfile::tempdir().unwrap();
        run(
            clone_dir.path(),
            &["clone", &remote.path().display().to_string(), "."],
        );
        std::fs::write(clone_dir.path().join("remote.txt"), "from remote").unwrap();
        run(clone_dir.path(), &["add", "."]);
        run(clone_dir.path(), &["commit", "-m", "remote commit"]);
        run(clone_dir.path(), &["push"]);

        // Make a local commit in dir — now diverged.
        std::fs::write(dir.path().join("local.txt"), "local change").unwrap();
        run(dir.path(), &["add", "."]);
        run(dir.path(), &["commit", "-m", "local commit"]);

        let working_dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(working_dir.path().to_path_buf());
        config.mounts = vec![test_mount("testrepo", dir.path().to_path_buf(), true)];

        let result = run_start_hooks(&config, 1).await.unwrap();
        assert_eq!(
            result.warnings.len(),
            1,
            "diverged should produce exactly one warning"
        );
        // pull_clone classifies a diverged repo as Conflict.
        assert!(
            result.warnings[0].contains("conflict during pull"),
            "warning: {}",
            result.warnings[0]
        );
    }

    #[tokio::test]
    async fn auto_pull_disabled_does_nothing() {
        let dir = tempfile::tempdir().unwrap();
        // Not even a git repo, but auto_pull is false — should be completely inert.
        let mut config = test_app_config(dir.path().to_path_buf());
        config.mounts = vec![test_mount("testrepo", dir.path().to_path_buf(), false)];

        let result = run_start_hooks(&config, 1).await.unwrap();
        assert!(result.warnings.is_empty());
    }

    // -----------------------------------------------------------------------
    // Host hook tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn host_hook_success() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.start_hooks = StartHooksConfig {
            host: vec!["true".into()],
            container: vec![],
        };

        let result = run_start_hooks(&config, 1).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn host_hook_failure_is_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.start_hooks = StartHooksConfig {
            host: vec!["exit 1".into()],
            container: vec![],
        };

        let result = run_start_hooks(&config, 1).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed"));
    }

    #[tokio::test]
    async fn host_hook_runs_in_working_dir() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        // Create a file using pwd — proves cwd is set correctly.
        config.start_hooks = StartHooksConfig {
            host: vec!["pwd > cwd_proof.txt".into()],
            container: vec![],
        };

        run_start_hooks(&config, 1).await.unwrap();
        let content = std::fs::read_to_string(dir.path().join("cwd_proof.txt")).unwrap();
        // Canonicalize both sides to handle /tmp vs /private/tmp on macOS.
        let expected = dir.path().canonicalize().unwrap();
        let actual = PathBuf::from(content.trim()).canonicalize().unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn host_hook_receives_env_vars() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.start_hooks = StartHooksConfig {
            host: vec!["echo $BRENN_APP_SLUG:$BRENN_CONVERSATION_ID > env_proof.txt".into()],
            container: vec![],
        };

        run_start_hooks(&config, 42).await.unwrap();
        let content = std::fs::read_to_string(dir.path().join("env_proof.txt")).unwrap();
        assert_eq!(content.trim(), "test:42");
    }

    #[tokio::test]
    async fn host_hooks_run_in_order_and_fail_fast() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.start_hooks = StartHooksConfig {
            host: vec![
                "echo first > order.txt".into(),
                "exit 1".into(),
                "echo third >> order.txt".into(), // Should never run.
            ],
            container: vec![],
        };

        let result = run_start_hooks(&config, 1).await;
        assert!(result.is_err());
        let content = std::fs::read_to_string(dir.path().join("order.txt")).unwrap();
        assert_eq!(content.trim(), "first", "third hook should not have run");
    }

    #[tokio::test]
    async fn host_hook_with_arguments_and_pipes() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.start_hooks = StartHooksConfig {
            host: vec!["echo hello world | tr a-z A-Z > pipe_proof.txt".into()],
            container: vec![],
        };

        run_start_hooks(&config, 1).await.unwrap();
        let content = std::fs::read_to_string(dir.path().join("pipe_proof.txt")).unwrap();
        assert_eq!(content.trim(), "HELLO WORLD");
    }

    #[tokio::test]
    async fn no_hooks_no_auto_pull_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_app_config(dir.path().to_path_buf());

        let result = run_start_hooks(&config, 1).await.unwrap();
        assert!(result.warnings.is_empty());
    }

    #[tokio::test]
    async fn auto_pull_runs_before_hooks() {
        // auto_pull mount + a hook that creates a marker file.
        // Both succeed — proves ordering is auto_pull first, then hooks.
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let _remote = git_add_remote(dir.path());

        let working_dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(working_dir.path().to_path_buf());
        config.mounts = vec![test_mount("testrepo", dir.path().to_path_buf(), true)];
        config.start_hooks = StartHooksConfig {
            host: vec!["touch hook_ran".into()],
            container: vec![],
        };

        let result = run_start_hooks(&config, 1).await.unwrap();
        assert!(result.warnings.is_empty());
        assert!(working_dir.path().join("hook_ran").exists());
    }

    // -----------------------------------------------------------------------
    // Mount auto-pull tests (via auto_pull_mounts)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn mount_auto_pull_succeeds() {
        let repo_dir = tempfile::tempdir().unwrap();
        git_init(repo_dir.path());
        let _remote = git_add_remote(repo_dir.path());

        let mounts = vec![test_mount("testrepo", repo_dir.path().to_path_buf(), true)];

        let warnings = auto_pull_mounts(&mounts).await;
        assert!(
            warnings.is_empty(),
            "expected no warnings, got: {warnings:?}"
        );
    }

    #[tokio::test]
    async fn mount_auto_pull_skips_non_auto_pull() {
        let repo_dir = tempfile::tempdir().unwrap();
        // Not even a git repo — but auto_pull is false, so this shouldn't matter.
        let mounts = vec![test_mount("testrepo", repo_dir.path().to_path_buf(), false)];

        let warnings = auto_pull_mounts(&mounts).await;
        assert!(
            warnings.is_empty(),
            "non-auto_pull mounts should be skipped"
        );
    }

    #[tokio::test]
    async fn mount_auto_pull_warns_on_failure() {
        let repo_dir = tempfile::tempdir().unwrap();
        // Not a git repo — auto_pull will fail.
        let mounts = vec![test_mount("testrepo", repo_dir.path().to_path_buf(), true)];

        let warnings = auto_pull_mounts(&mounts).await;
        assert_eq!(warnings.len(), 1, "expected one warning, got: {warnings:?}");
        assert!(
            warnings[0].contains("testrepo"),
            "warning should mention repo slug: {}",
            warnings[0]
        );
    }

    #[tokio::test]
    async fn mount_auto_pull_fast_forwards() {
        let repo_dir = tempfile::tempdir().unwrap();
        git_init(repo_dir.path());
        let remote = git_add_remote(repo_dir.path());

        // Push a commit from a second clone.
        let clone_dir = tempfile::tempdir().unwrap();
        git_run(
            clone_dir.path(),
            &["clone", &remote.path().display().to_string(), "."],
        );
        std::fs::write(clone_dir.path().join("new.txt"), "new content").unwrap();
        git_run(clone_dir.path(), &["add", "."]);
        git_run(clone_dir.path(), &["commit", "-m", "new commit"]);
        git_run(clone_dir.path(), &["push"]);

        // Repo should be behind.
        assert!(!repo_dir.path().join("new.txt").exists());

        let mounts = vec![test_mount("testrepo", repo_dir.path().to_path_buf(), true)];

        let warnings = auto_pull_mounts(&mounts).await;
        assert!(warnings.is_empty(), "ff pull should succeed");
        assert!(
            repo_dir.path().join("new.txt").exists(),
            "repo should have been pulled"
        );
    }

    #[tokio::test]
    async fn mount_auto_pull_concurrent_multiple_repos() {
        // Two mounts, both with auto_pull — both should pull concurrently.
        let repo_a = tempfile::tempdir().unwrap();
        let repo_b = tempfile::tempdir().unwrap();
        git_init(repo_a.path());
        git_init(repo_b.path());
        let _remote_a = git_add_remote(repo_a.path());
        let _remote_b = git_add_remote(repo_b.path());

        let mounts = vec![
            test_mount("repo-a", repo_a.path().to_path_buf(), true),
            test_mount("repo-b", repo_b.path().to_path_buf(), true),
        ];

        let warnings = auto_pull_mounts(&mounts).await;
        assert!(warnings.is_empty(), "both pulls should succeed");
    }

    #[tokio::test]
    async fn mount_auto_pull_works_on_read_only_mount() {
        // Regression guard: pre-host-unification, RO mounts took a
        // separate code path (`effective_container_for_pull` returned
        // None for RO, Some(container) for RW). Post-unification, all
        // mounts pull host-side uniformly, so RO vs RW doesn't matter
        // at the pull leg. Kept as a smoke test that `AccessLevel::ReadOnly`
        // doesn't re-introduce special-casing by accident.
        let repo_dir = tempfile::tempdir().unwrap();
        git_init(repo_dir.path());
        let remote = git_add_remote(repo_dir.path());

        // Push a commit from a second clone so the local repo has
        // something to pull.
        let clone_dir = tempfile::tempdir().unwrap();
        git_run(
            clone_dir.path(),
            &["clone", &remote.path().display().to_string(), "."],
        );
        std::fs::write(clone_dir.path().join("ro-file.txt"), "new content").unwrap();
        git_run(clone_dir.path(), &["add", "."]);
        git_run(clone_dir.path(), &["commit", "-m", "remote commit"]);
        git_run(clone_dir.path(), &["push"]);

        let mut mount = test_mount("ro-repo", repo_dir.path().to_path_buf(), true);
        mount.access = AccessLevel::ReadOnly;
        let mounts = vec![mount];

        let warnings = auto_pull_mounts(&mounts).await;
        assert!(
            warnings.is_empty(),
            "RO mount host-side pull should succeed"
        );
        assert!(
            repo_dir.path().join("ro-file.txt").exists(),
            "repo should have been pulled",
        );
    }

    #[tokio::test]
    async fn mount_auto_pull_integrated_in_run_start_hooks() {
        // Mount auto-pull runs as part of run_start_hooks.
        let repo_dir = tempfile::tempdir().unwrap();
        git_init(repo_dir.path());
        let remote = git_add_remote(repo_dir.path());

        // Push a commit from a second clone.
        let clone_dir = tempfile::tempdir().unwrap();
        git_run(
            clone_dir.path(),
            &["clone", &remote.path().display().to_string(), "."],
        );
        std::fs::write(clone_dir.path().join("new.txt"), "new from remote").unwrap();
        git_run(clone_dir.path(), &["add", "."]);
        git_run(clone_dir.path(), &["commit", "-m", "remote commit"]);
        git_run(clone_dir.path(), &["push"]);

        let working_dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(working_dir.path()).unwrap();
        let mut config = test_app_config(working_dir.path().to_path_buf());
        config.mounts = vec![test_mount("testrepo", repo_dir.path().to_path_buf(), true)];

        let result = run_start_hooks(&config, 1).await.unwrap();
        assert!(result.warnings.is_empty());
        assert!(
            repo_dir.path().join("new.txt").exists(),
            "repo should have been pulled during start hooks"
        );
    }

    // -----------------------------------------------------------------------
    // Post-pull hook tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn post_pull_hook_success() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.post_pull_hooks = brenn_lib::config::PostPullHooksConfig {
            host: vec!["true".into()],
            container: vec![],
        };

        let warnings = run_post_pull_hooks(&config, "test-repo").await;
        assert!(
            warnings.is_empty(),
            "successful hook should produce no warnings"
        );
    }

    #[tokio::test]
    async fn post_pull_hook_failure_returns_warning() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.post_pull_hooks = brenn_lib::config::PostPullHooksConfig {
            host: vec!["exit 1".into()],
            container: vec![],
        };

        let warnings = run_post_pull_hooks(&config, "test-repo").await;
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("failed"), "warning: {}", warnings[0]);
    }

    #[tokio::test]
    async fn post_pull_hook_does_not_panic_on_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.post_pull_hooks = brenn_lib::config::PostPullHooksConfig {
            host: vec!["exit 42".into()],
            container: vec![],
        };

        let warnings = run_post_pull_hooks(&config, "test-repo").await;
        assert!(!warnings.is_empty());
    }

    #[tokio::test]
    async fn post_pull_hook_receives_repo_slug_env_var() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.post_pull_hooks = brenn_lib::config::PostPullHooksConfig {
            host: vec!["echo $BRENN_REPO_SLUG > repo_slug_proof.txt".into()],
            container: vec![],
        };

        let warnings = run_post_pull_hooks(&config, "pfin-data").await;
        assert!(warnings.is_empty());
        let content = std::fs::read_to_string(dir.path().join("repo_slug_proof.txt")).unwrap();
        assert_eq!(content.trim(), "pfin-data");
    }

    #[tokio::test]
    async fn post_pull_hook_receives_app_slug_and_working_dir() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.post_pull_hooks = brenn_lib::config::PostPullHooksConfig {
            host: vec!["echo $BRENN_APP_SLUG:$BRENN_WORKING_DIR > env_proof.txt".into()],
            container: vec![],
        };

        let warnings = run_post_pull_hooks(&config, "repo").await;
        assert!(warnings.is_empty());
        let content = std::fs::read_to_string(dir.path().join("env_proof.txt")).unwrap();
        let expected_dir = dir.path().canonicalize().unwrap();
        // Format: "test:<working_dir>"
        assert!(content.starts_with("test:"), "got: {content}");
        let dir_part = PathBuf::from(content.trim().strip_prefix("test:").unwrap());
        assert_eq!(dir_part.canonicalize().unwrap(), expected_dir);
    }

    #[tokio::test]
    async fn post_pull_hook_does_not_have_conversation_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.post_pull_hooks = brenn_lib::config::PostPullHooksConfig {
            host: vec!["echo \"CONV=${BRENN_CONVERSATION_ID:-unset}\" > conv_proof.txt".into()],
            container: vec![],
        };

        let warnings = run_post_pull_hooks(&config, "repo").await;
        assert!(warnings.is_empty());
        let content = std::fs::read_to_string(dir.path().join("conv_proof.txt")).unwrap();
        assert_eq!(
            content.trim(),
            "CONV=unset",
            "post-pull hooks should not have BRENN_CONVERSATION_ID"
        );
    }

    #[tokio::test]
    async fn post_pull_hooks_run_in_order_and_collect_all_failures() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.post_pull_hooks = brenn_lib::config::PostPullHooksConfig {
            host: vec![
                "echo first > order.txt".into(),
                "exit 1".into(),
                "echo third >> order.txt".into(), // Still runs — post-pull doesn't fail-fast.
            ],
            container: vec![],
        };

        let warnings = run_post_pull_hooks(&config, "repo").await;
        // First hook succeeds, second fails (warning), third runs and succeeds.
        assert_eq!(
            warnings.len(),
            1,
            "only the failing hook should produce a warning"
        );
        let content = std::fs::read_to_string(dir.path().join("order.txt")).unwrap();
        assert!(content.contains("first"), "first hook should have run");
        assert!(
            content.contains("third"),
            "third hook should have run after failure"
        );
    }

    // -----------------------------------------------------------------------
    // Startup hook tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn startup_hook_success() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.startup_hooks = brenn_lib::config::StartupHooksConfig {
            host: vec!["true".into()],
            container: vec![],
        };

        let result = run_startup_hooks(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn startup_hook_failure_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.startup_hooks = brenn_lib::config::StartupHooksConfig {
            host: vec!["exit 1".into()],
            container: vec![],
        };

        let result = run_startup_hooks(&config).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed"));
    }

    #[tokio::test]
    async fn startup_hook_does_not_have_conversation_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.startup_hooks = brenn_lib::config::StartupHooksConfig {
            host: vec!["echo \"CONV=${BRENN_CONVERSATION_ID:-unset}\" > conv_proof.txt".into()],
            container: vec![],
        };

        let result = run_startup_hooks(&config).await;
        assert!(result.is_ok());
        let content = std::fs::read_to_string(dir.path().join("conv_proof.txt")).unwrap();
        assert_eq!(
            content.trim(),
            "CONV=unset",
            "startup hooks should not have BRENN_CONVERSATION_ID"
        );
    }

    #[tokio::test]
    async fn startup_hook_does_not_have_repo_slug() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.startup_hooks = brenn_lib::config::StartupHooksConfig {
            host: vec!["echo \"REPO=${BRENN_REPO_SLUG:-unset}\" > repo_proof.txt".into()],
            container: vec![],
        };

        let result = run_startup_hooks(&config).await;
        assert!(result.is_ok());
        let content = std::fs::read_to_string(dir.path().join("repo_proof.txt")).unwrap();
        assert_eq!(
            content.trim(),
            "REPO=unset",
            "startup hooks should not have BRENN_REPO_SLUG"
        );
    }

    #[tokio::test]
    async fn startup_hook_receives_app_slug() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.startup_hooks = brenn_lib::config::StartupHooksConfig {
            host: vec!["echo $BRENN_APP_SLUG > slug_proof.txt".into()],
            container: vec![],
        };

        let result = run_startup_hooks(&config).await;
        assert!(result.is_ok());
        let content = std::fs::read_to_string(dir.path().join("slug_proof.txt")).unwrap();
        assert_eq!(content.trim(), "test");
    }

    #[tokio::test]
    async fn startup_hooks_fail_fast() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_app_config(dir.path().to_path_buf());
        config.startup_hooks = brenn_lib::config::StartupHooksConfig {
            host: vec![
                "echo first > order.txt".into(),
                "exit 1".into(),
                "echo third >> order.txt".into(), // Should never run.
            ],
            container: vec![],
        };

        let result = run_startup_hooks(&config).await;
        assert!(result.is_err());
        let content = std::fs::read_to_string(dir.path().join("order.txt")).unwrap();
        assert_eq!(content.trim(), "first", "third hook should not have run");
    }

    #[tokio::test]
    async fn startup_hook_empty_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_app_config(dir.path().to_path_buf());

        let result = run_startup_hooks(&config).await;
        assert!(result.is_ok());
    }
}

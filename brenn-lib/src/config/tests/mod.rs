use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::config::*;

mod access_mount;
mod alerting;
mod app_parse;
mod attachment;
mod container;
mod events;
mod integrations;
mod invariants;
mod load_config;
mod mcp_servers;
mod path_mapper;
mod podman_args;
mod primary;
mod repo_mount;
mod resolve;
mod resolved_config;
mod server;
mod toml_parse;
mod webhook;

// -----------------------------------------------------------------------
// Shared test helpers
// -----------------------------------------------------------------------

/// Return a validated `PathBuf` suitable as the `runtime_dir` argument to
/// `validate_and_resolve` for tests that include at least one bare app.
///
/// Creates a 0700-owned tempdir exactly once per test binary invocation (via
/// `OnceLock`) and validates it through `crate::runtime_dir::validate_runtime_dir`
/// so the returned value is the same kind of validated path that production
/// `resolve_validated_xdg_runtime_dir` would produce. The `TempDir` is leaked
/// so the path remains alive for the full binary lifetime.
///
/// No environment variable is read or written. Tests call this once, borrow
/// the path as `Some(&validated)`, and pass that into `validate_and_resolve`.
pub(super) fn test_runtime_dir() -> &'static PathBuf {
    crate::runtime_dir::test_runtime_dir_once()
}

#[allow(dead_code)]
fn raw_with_token_thresholds(
    dir: &std::path::Path,
    reminder_tokens: Option<u64>,
    soft_tokens: Option<u64>,
    red_tokens: Option<u64>,
    hard_tokens: Option<u64>,
) -> AppConfigRaw {
    AppConfigRaw {
        slug: "pa".to_string(),
        working_dir: Some(dir.to_path_buf()),
        singleton: true,
        compact_reminder_tokens: reminder_tokens,
        compact_soft_tokens: soft_tokens,
        compact_red_tokens: red_tokens,
        compact_hard_tokens: hard_tokens,
        ..Default::default()
    }
}

/// Build a minimal `AppConfig` for `messaging_send_budget` tests.
#[allow(dead_code)]
fn minimal_app_config_for_budget_test(
    messaging: Option<crate::messaging::config::ResolvedMessagingConfig>,
    global_default: u32,
) -> AppConfig {
    AppConfig {
        slug: "test".to_string(),
        name: "test".to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: PathBuf::from("/tmp"),
        model: "sonnet".to_string(),
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
        post_pull_hooks: PostPullHooksConfig::default(),
        startup_hooks: StartupHooksConfig::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: HashMap::new(),
        mounts: vec![],
        history_replay_limit: 2000,
        frontmatter: FrontmatterRenderConfig::default(),
        state_dir: PathBuf::from("/tmp/.brenn/test-state"),
        messaging,
        messaging_default_send_budget: global_default,
        policy: crate::access::AppPolicy::default(),
        pwa_push: None,
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
    }
}

#[allow(dead_code)]
fn app_raw_with_targets(dir: &std::path::Path, targets: Vec<AttachmentTargetRaw>) -> AppConfigRaw {
    AppConfigRaw {
        slug: "test".to_string(),
        working_dir: Some(dir.to_path_buf()),
        attachment_targets: targets,
        ..Default::default()
    }
}

#[allow(dead_code)]
fn make_import_target(name: &str) -> AttachmentTargetRaw {
    AttachmentTargetRaw {
        name: name.to_string(),
        label: "Test target".to_string(),
        accept: vec![".ofx".to_string()],
        multi: false,
        handler: AttachmentHandlerConfig::Command {
            program: "echo".to_string(),
            args: vec!["{ofx}".to_string()],
            file_roles: HashMap::from([("ofx".to_string(), vec![".ofx".to_string()])]),
            timeout_secs: 60,
            cc_instructions: None,
        },
    }
}

#[allow(dead_code)]
fn mount_test_config(
    dir: &std::path::Path,
    repos: Vec<RepoDeclRaw>,
    mounts: Vec<MountConfigRaw>,
    working_dir: Option<PathBuf>,
    container_working_dir: Option<PathBuf>,
) -> BrennConfig {
    BrennConfig {
        repo_dir: Some(dir.to_path_buf()),
        repos,
        apps: vec![AppConfigRaw {
            slug: "test".to_string(),
            working_dir,
            container_working_dir,
            mounts,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[allow(dead_code)]
fn two_app_mount_config(
    dir: &std::path::Path,
    repos: Vec<RepoDeclRaw>,
    app_a_mounts: Vec<MountConfigRaw>,
    app_b_mounts: Vec<MountConfigRaw>,
) -> BrennConfig {
    let app_a_dir = dir.join("app-a");
    let app_b_dir = dir.join("app-b");
    std::fs::create_dir_all(&app_a_dir).unwrap();
    std::fs::create_dir_all(&app_b_dir).unwrap();
    let make_app = |slug: &str, wd: PathBuf, mounts: Vec<MountConfigRaw>| AppConfigRaw {
        slug: slug.to_string(),
        working_dir: Some(wd),
        mounts,
        ..Default::default()
    };
    BrennConfig {
        repo_dir: Some(dir.to_path_buf()),
        repos,
        apps: vec![
            make_app("app-a", app_a_dir, app_a_mounts),
            make_app("app-b", app_b_dir, app_b_mounts),
        ],
        ..Default::default()
    }
}

#[allow(dead_code)]
fn write_secret(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path
}

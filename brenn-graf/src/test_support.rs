//! Shared test helpers for `brenn-graf` tests.

use std::path::PathBuf;

/// Build a minimal `AppConfig` for tests.
///
/// `mounts` — pass `vec![]` when no mount is needed; supply mounts for
/// integration tests that exercise per-repo dispatch.
///
/// `create_state_dir` — pass `true` when the test code reads from or writes
/// to the state directory (e.g. lib.rs integration tests); `false` when
/// `run_graf_raw` / subprocess tests never touch it and the side-effect
/// would be noise.
pub(crate) fn test_app_config(
    working_dir: PathBuf,
    mounts: Vec<brenn_lib::config::ResolvedMount>,
    create_state_dir: bool,
) -> brenn_lib::config::AppConfig {
    let state_dir = working_dir.join(".brenn-state");
    if create_state_dir {
        std::fs::create_dir_all(&state_dir).unwrap();
    }
    brenn_lib::config::AppConfig {
        slug: "test".into(),
        name: "test".into(),
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
        mcp_servers: std::collections::HashMap::new(),
        multiuser: false,
        prefix_username: false,
        prefix_timestamp: false,
        prefix_device: true,
        path_mapper: brenn_lib::config::PathMapper::Identity,
        container_spawn: None,
        start_hooks: brenn_lib::config::StartHooksConfig::default(),
        post_pull_hooks: brenn_lib::config::PostPullHooksConfig::default(),
        startup_hooks: brenn_lib::config::StartupHooksConfig::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: std::collections::HashMap::new(),
        mounts,
        history_replay_limit: 2000,
        frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
        state_dir,
        messaging: None,
        messaging_default_send_budget: 100,
        policy: brenn_lib::access::AppPolicy::default(),
        pwa_push: None,
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
    }
}

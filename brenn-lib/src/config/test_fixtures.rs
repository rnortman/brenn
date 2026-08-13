//! Shared `AppConfig` fixture for test code in this crate and above it.
//!
//! `AppConfig` has no `Default` and many fields, so tests override only the
//! fields they care about on top of the single canonical baseline here.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::access::AppPolicy;
use crate::config::{
    AppConfig, FrontmatterRenderConfig, PathMapper, PostPullHooksConfig, StartHooksConfig,
    StartupHooksConfig,
};

/// A minimal `AppConfig`: `slug` for both the slug and the display name, and
/// default-shape values for everything else, so the fields a test actually
/// reads are the only ones it sets.
pub fn test_app_config(slug: &str) -> AppConfig {
    AppConfig {
        slug: slug.to_string(),
        name: slug.to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: PathBuf::from("."),
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
        messaging: None,
        messaging_default_send_budget: 100,
        policy: AppPolicy::default(),
        pwa_push: None,
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
        chat_harness_policy: AppPolicy::default(),
    }
}

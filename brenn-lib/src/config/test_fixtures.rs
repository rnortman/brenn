//! Shared config fixtures for test code in this crate and above it.
//!
//! `AppConfig` has no `Default` and many fields, so tests override only the
//! fields they care about on top of the single canonical baseline here.
//! `lower_document` is the other half, with `config_from_dsl` and
//! `sole_refusal` over it: a whole `BrennConfig` built from a `.brenn`
//! document, for tests whose subject is downstream of config loading, and the
//! refusal a document earns when that is the subject.
//!
//! The `remote_*` half builds `remote` raws for the suites that resolve them —
//! this crate's own, the remote server's, and the route rig above it — so the
//! fleet-driver block those three assert against is written once.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use brenn_dsl::diag::Diagnostic;

use crate::access::AppPolicy;
use crate::access::raw::ChannelMatcherRaw;
use crate::config::{
    AppConfig, BrennConfig, FrontmatterRenderConfig, PathMapper, PostPullHooksConfig,
    RepoSyncConfig, StartHooksConfig, StartupHooksConfig,
};
use crate::messaging::remote::{RemoteConfigRaw, RemoteGrant, RemoteSubscribeAclRaw};

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

/// A `[repo_sync]` section whose only stated key is the repo root.
pub fn repo_sync_at(dir: &std::path::Path) -> RepoSyncConfig {
    RepoSyncConfig {
        repo_dir: Some(dir.to_path_buf()),
        ..Default::default()
    }
}

/// Compile a `.brenn` document from a tempdir and lower it.
///
/// Compile-stage and lowering-stage diagnostics both come back in the error:
/// which stage refuses a document is an implementation detail of the vocabulary,
/// not a property a test states.
pub fn lower_document(document: &str) -> Result<BrennConfig, Vec<Diagnostic>> {
    let dir = tempfile::tempdir().expect("a tempdir");
    let root = dir.path().join("main.brenn");
    std::fs::write(&root, document).expect("write the root module");
    let compiled = brenn_dsl::compile(&root)?;
    crate::config::dsl_lower::lower(compiled)
}

/// The config a document loads to, panicking on any diagnostic.
pub fn config_from_dsl(document: &str) -> BrennConfig {
    lower_document(document).unwrap_or_else(|errors| {
        panic!(
            "the fixture document must load:\n{}",
            brenn_dsl::diag::render_all(&errors)
        )
    })
}

/// The one diagnostic the document produces.
///
/// A single diagnostic, not a blob: a caller asserting on the message it
/// expects would otherwise also pass on a document refused for an unrelated
/// reason, or on a regression that adds a second refusal.
pub fn sole_refusal(document: &str) -> Diagnostic {
    let mut errors = lower_document(document).expect_err("the document must be refused");
    assert_eq!(
        errors.len(),
        1,
        "exactly one refusal:\n{}",
        brenn_dsl::diag::render_all(&errors)
    );
    errors.pop().expect("one refusal")
}

/// A `remote` stating only its slug, token file and grants.
///
/// The caller keeps the token file alive: resolution reads it, so dropping the
/// handle before the fixture resolves leaves an unreadable path.
pub fn remote_raw(slug: &str, token_file: &Path, grants: &[RemoteGrant]) -> RemoteConfigRaw {
    RemoteConfigRaw {
        slug: slug.to_string(),
        token_file: token_file.to_path_buf(),
        grants: grants.to_vec(),
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        publish_burst: None,
        publish_per_sec: None,
        max_sessions: None,
        max_subscriptions: None,
    }
}

/// One durable-plane ceiling on an exact address.
pub fn remote_exact_ceiling(
    channel: &str,
    push_depth: u64,
    retain_depth: u64,
) -> RemoteSubscribeAclRaw {
    RemoteSubscribeAclRaw {
        exact: Some(channel.to_string()),
        prefix: None,
        push_depth,
        retain_depth,
    }
}

/// One durable-plane ceiling on a prefix.
pub fn remote_prefix_ceiling(
    prefix: &str,
    push_depth: u64,
    retain_depth: u64,
) -> RemoteSubscribeAclRaw {
    RemoteSubscribeAclRaw {
        exact: None,
        prefix: Some(prefix.to_string()),
        push_depth,
        retain_depth,
    }
}

/// The fleet-driver `remote` the route and profile suites boot against: a roster
/// read at 1/1, the outbound conversation leaves under prefixes, and publish
/// rights on the two inbound ones.
pub fn remote_fleet(token_file: &Path) -> RemoteConfigRaw {
    RemoteConfigRaw {
        subscribe_acl: vec![
            remote_exact_ceiling("chat.app.home.roster", 1, 1),
            remote_prefix_ceiling("chat.app.home.out.", 8, 64),
        ],
        ephemeral_subscribe_acl: vec![remote_prefix_ceiling("chat.app.home.stream.", 32, 32)],
        publish_acl: vec![ChannelMatcherRaw::Prefix("chat.app.home.in.".to_string())],
        ephemeral_publish_acl: vec![ChannelMatcherRaw::Prefix("chat.app.home.wake.".to_string())],
        ..remote_raw(
            "pod-kitchen",
            token_file,
            &[
                RemoteGrant::Subscribe,
                RemoteGrant::Publish,
                RemoteGrant::EphemeralSubscribe,
                RemoteGrant::EphemeralPublish,
                RemoteGrant::Alert,
            ],
        )
    }
}

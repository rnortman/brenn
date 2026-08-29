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
use crate::messaging::AttachGrant;
use crate::messaging::remote::{RemoteConfigRaw, RemoteSubscribeAclRaw};

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
    let (root, module_root) = stage_fixture(dir.path(), "main.brenn", document);
    let compiled = brenn_dsl::compile(&root, module_root.as_deref())?;
    crate::config::dsl_lower::lower(compiled)
}

/// Write a fixture document into `dir` as the root `name`, splitting its fenced
/// half out as a module root beside it.
///
/// Returns the root document's path and the module root to compile it against; the
/// module root is `None` for a fixture that fences nothing. Stated once because
/// every caller that compiles a fixture from disk — lowering here, the
/// config-check report in `brenn-bootstrap` — has to stage it the same way, and
/// a rule added to the fence transform has to reach both.
pub fn stage_fixture(dir: &Path, name: &str, document: &str) -> (PathBuf, Option<PathBuf>) {
    let root = dir.join(name);
    match split_packaged(document) {
        Some((module, rest)) => {
            let modules = dir.join("modules");
            std::fs::create_dir(&modules).expect("a module root");
            std::fs::write(modules.join(format!("{PACKAGED_MODULE}.brenn")), module)
                .expect("write the module");
            std::fs::write(&root, rest).expect("write the root module");
            (root, Some(modules))
        }
        None => {
            std::fs::write(&root, document).expect("write the root module");
            (root, None)
        }
    }
}

/// The fence a fixture writes around the class declarations a packaged module
/// has to hold, and the split it drives.
///
/// A top-level instance's class is declared in an installed component package,
/// and these fixtures are about lowering rather than about module structure.
/// Re-exported here so a lowering test names one module for its fixtures.
pub use brenn_dsl::fixture_text::{PACKAGED, PACKAGED_MODULE, split_packaged};

/// The text whose hash every class in a fixture carries: the packaged module
/// where the fixture writes one, and the document itself otherwise.
pub fn declaring_text(document: &str) -> String {
    split_packaged(document).map_or_else(|| document.to_string(), |(module, _)| module)
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
pub fn remote_raw(slug: &str, token_file: &Path, grants: &[AttachGrant]) -> RemoteConfigRaw {
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
                AttachGrant::Subscribe,
                AttachGrant::Publish,
                AttachGrant::EphemeralSubscribe,
                AttachGrant::EphemeralPublish,
                AttachGrant::Alert,
            ],
        )
    }
}

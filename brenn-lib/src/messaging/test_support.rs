//! Shared test infrastructure for the messaging module tree.
//!
//! Exposes `test_app_config` so `publish.rs` and `deliver_after.rs` tests do
//! not each maintain separate copies of the full `AppConfig` literal.
//! Adding a field to `AppConfig` requires only one edit here.

use crate::config::AppConfig;
use crate::messaging::config::ResolvedMessagingConfig;

/// Construct a minimal `AppConfig` for messaging tests.
///
/// `slug` — app slug (used as both `slug` and `name`).
/// `messaging` — optional resolved messaging config.
/// `allowed_users` — allowed user list.
///
/// Many `AppConfig` fields are not read by the messaging path and are filled
/// with their type defaults.
pub(super) fn test_app_config(
    slug: &str,
    messaging: Option<ResolvedMessagingConfig>,
    allowed_users: Vec<String>,
) -> AppConfig {
    AppConfig {
        slug: slug.to_string(),
        name: slug.to_string(),
        description: String::new(),
        icon: String::new(),
        working_dir: std::path::PathBuf::from("/tmp"),
        model: String::new(),
        single_instance: false,
        singleton: true,
        persistent: false,
        idle_timeout: None,
        compaction: None,
        idle_hook_secs: 0,
        allowed_users,
        disabled_tools: vec![],
        mcp_servers: Default::default(),
        multiuser: false,
        prefix_username: false,
        prefix_timestamp: false,
        prefix_device: false,
        path_mapper: crate::config::PathMapper::Identity,
        container_spawn: None,
        start_hooks: Default::default(),
        post_pull_hooks: Default::default(),
        startup_hooks: Default::default(),
        cc_extra_args: vec![],
        approval_rules: vec![],
        attachment_targets: vec![],
        integrations: Default::default(),
        mounts: vec![],
        history_replay_limit: 100,
        frontmatter: Default::default(),
        state_dir: std::path::PathBuf::from("/tmp"),
        // Grant both messaging capabilities whenever a messaging config is
        // supplied, so messaging_enabled() treats the app as a participant.
        // Also stamp a universal `brenn_subscribe` ACL matcher so the
        // delivery-time gate (design §2.2 Point A) authorizes these test apps to
        // receive on their `brenn:` channels — `MessagingSubscribe` alone is not
        // sufficient now that delivery requires a covering matcher. `Prefix("")`
        // covers every channel (the resolution-time narrowing that rejects an
        // empty prefix does not apply when the `AclSet` is constructed directly).
        // A matching universal `brenn_publish` matcher is stamped for the same
        // reason: Phase-2 Seam A (design §2.2) gates publish on a covering
        // `brenn_publish` matcher, so `MessagingPublish` alone no longer
        // authorizes a send.
        policy: {
            let mut p = crate::access::AppPolicy::default();
            if messaging.is_some() {
                p.grants
                    .insert(crate::access::AppCapability::MessagingPublish);
                p.grants
                    .insert(crate::access::AppCapability::MessagingSubscribe);
                p.acls
                    .brenn_subscribe
                    .push(crate::access::acl::ChannelMatcher::Prefix(String::new()));
                p.acls
                    .brenn_publish
                    .push(crate::access::acl::ChannelMatcher::Prefix(String::new()));
            }
            p
        },
        messaging,
        messaging_default_send_budget: 100,
        pwa_push: None,
        webhook_subscriptions: vec![],
        mqtt_subscriptions: vec![],
    }
}

/// An `AppPolicy` that authorizes `brenn:` delivery via the `MessagingSubscribe`
/// grant + a single `brenn_subscribe` matcher — the static-subscriber delivery
/// form (no `DynamicSubscribe`). `matcher` chooses the scope:
/// - `ChannelMatcher::Prefix(String::new())` → universal (covers every channel),
/// - `ChannelMatcher::Exact(ch)` → exactly one channel.
///
/// Single home for the "allow brenn: delivery" policy stamp so test modules
/// (`publish/tests/wasm.rs`, `config.rs`, `dispatcher.rs`) do not each maintain a
/// private copy that must be kept in sync as `AppPolicy` evolves (reuse-1/reuse-2).
pub(super) fn brenn_delivery_policy(
    matcher: crate::access::acl::ChannelMatcher,
) -> crate::access::AppPolicy {
    let mut p = crate::access::AppPolicy::default();
    p.grants
        .insert(crate::access::AppCapability::MessagingSubscribe);
    p.acls.brenn_subscribe.push(matcher);
    p
}

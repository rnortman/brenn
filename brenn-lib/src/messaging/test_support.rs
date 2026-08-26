//! Shared test infrastructure for the messaging module tree, here and in the
//! runtime crate above.
//!
//! Exposes `test_app_config` so `publish.rs` and `deliver_after.rs` tests do
//! not each maintain separate copies of the full `AppConfig` literal, and
//! `test_channel_entry` so the default `ChannelEntry` literal has one home.
//! Adding a field to either type requires only one edit here.

use uuid::Uuid;

use brenn_envelope::ChannelScheme;

use crate::config::AppConfig;
use crate::messaging::config::{Depth, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig, Sink};
use crate::messaging::directory::{ChannelEntry, SubscriberEntry, WakeMin};

/// Construct a minimal `AppConfig` for messaging tests.
///
/// `slug` — app slug (used as both `slug` and `name`).
/// `messaging` — optional resolved messaging config.
/// `allowed_users` — allowed user list.
///
/// Many `AppConfig` fields are not read by the messaging path and are filled
/// with their type defaults.
pub fn test_app_config(
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
        // delivery-time gate authorizes these test apps to receive on their
        // `brenn:` channels — `MessagingSubscribe` alone is not sufficient;
        // delivery requires a covering matcher too. `Prefix("")` covers every
        // channel (the resolution-time narrowing that rejects an empty prefix
        // does not apply when the `AclSet` is constructed directly). A
        // matching universal `brenn_publish` matcher is stamped for the same
        // reason: the publish gate requires a covering `brenn_publish` matcher,
        // so `MessagingPublish` alone does not authorize a send.
        policy: {
            let mut p = crate::access::AppPolicy::default();
            if messaging.is_some() {
                p.grants
                    .insert(brenn_envelope::grants::AppCapability::MessagingPublish);
                p.grants
                    .insert(brenn_envelope::grants::AppCapability::MessagingSubscribe);
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
        // Deny-everything: a test that drives the chat machinery stamps
        // `LlmChatConfig::harness_policy` over this itself, so nothing else
        // silently gains chat-tree reach.
        chat_harness_policy: crate::access::AppPolicy::default(),
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
pub fn brenn_delivery_policy(
    matcher: crate::access::acl::ChannelMatcher,
) -> crate::access::AppPolicy {
    let mut p = crate::access::AppPolicy::default();
    p.grants
        .insert(brenn_envelope::grants::AppCapability::MessagingSubscribe);
    p.acls.brenn_subscribe.push(matcher);
    p
}

/// Build a default `brenn:` `ChannelEntry` with the given subscribers.
///
/// Channel-level depths are `Depth::Unbounded`, `noise = Silent`, `sink = Drop`,
/// `transport_type = Brenn`, `mount = None`, `description = None`, and the uuid is
/// fresh. Pass `subscribers` (often `vec![]`) for the per-subscriber wiring a test
/// needs. Single home for the default `ChannelEntry` literal so a new field is one
/// edit rather than one per test module.
pub fn test_channel_entry(name: &str, subscribers: Vec<SubscriberEntry>) -> ChannelEntry {
    ChannelEntry {
        uuid: Uuid::new_v4(),
        address: crate::messaging::canonical_address(name),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers,
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

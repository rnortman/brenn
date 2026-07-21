use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use brenn_lib::config::{AccessLevel, AppConfig, PathMapper};
use indexmap::IndexMap;

/// Build a minimal `AppConfig` with the given slug and display name,
/// plus default-shape values for everything else. Used by
/// `test_apps()` and `test_apps_multi()` so the struct literal
/// only lives in one place.
pub(crate) fn default_test_app_config(slug: &str, name: &str) -> AppConfig {
    AppConfig {
        slug: slug.to_string(),
        name: name.to_string(),
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
        start_hooks: brenn_lib::config::StartHooksConfig::default(),
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

/// `AppPolicy` for dynamic-subscribe fixture/gate tests. Grants
/// `DynamicSubscribe` + `MqttSubscribe` + `MessagingSubscribe`. The `(client,
/// filter)` params scope the single `mqtt_subscribe` matcher, so the MQTT gate's
/// deny-path tests still bite. `brenn_subscribe` lists exact matchers for every
/// `brenn:` channel the `test_new_for_mqtt_subscribe` fixture-family tests
/// dynamically subscribe to, so those tests reach the core behavior they assert
/// while keeping the policy production-shaped (each matcher resolves cleanly,
/// unlike a `Prefix("")` catch-all which resolution rejects). `webhook` is empty;
/// webhook-gate coverage uses targeted policies built inline by those tests.
pub(crate) fn mqtt_acl_policy(client: &str, filter: &str) -> brenn_lib::access::AppPolicy {
    use brenn_lib::access::AppCapability;
    use brenn_lib::access::acl::{AclSet, ChannelMatcher, MqttSubMatcher};

    let mut policy = brenn_lib::access::AppPolicy::default();
    policy.grants.insert(AppCapability::DynamicSubscribe);
    policy.grants.insert(AppCapability::MqttSubscribe);
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy.acls = AclSet {
        mqtt_subscribe: vec![MqttSubMatcher {
            client: client.to_string(),
            topic_filter: filter.to_string(),
        }],
        // The `brenn:` channels the fixture-family tests dynamically subscribe to
        // (matched on the bare channel name after the `brenn:` prefix is stripped,
        // mqtt_subscribe.rs `subscribe_dynamic_activated`). Each is a resolvable
        // exact matcher, so the policy is production-shaped.
        brenn_subscribe: vec![
            ChannelMatcher::Exact("test-channel".to_string()),
            ChannelMatcher::Exact("does-not-exist".to_string()),
            ChannelMatcher::Exact("my-channel".to_string()),
            ChannelMatcher::Exact("no-such-channel".to_string()),
            ChannelMatcher::Exact("test".to_string()),
        ],
        ..AclSet::default()
    };
    policy
}

/// Build an `AppPolicy` that authorizes *delivery* on each given channel address
/// (design §2.2 Point A — the delivery-time ACL gate now requires every
/// subscriber's policy to cover its channel). For each `mqtt:`/`brenn:`/`webhook:`
/// address, insert the matching transport grant and an exact covering matcher.
/// `DynamicSubscribe` is intentionally **not** granted — delivery authorization
/// must not depend on the runtime-tool grant. A malformed address panics (test
/// fixtures pass valid addresses).
pub(crate) fn delivery_policy_for_addresses<'a>(
    addresses: impl IntoIterator<Item = &'a str>,
) -> brenn_lib::access::AppPolicy {
    use brenn_lib::access::AppCapability;
    use brenn_lib::access::acl::{ChannelMatcher, MqttSubMatcher, WebhookMatcher};
    use brenn_lib::messaging::ChannelScheme;

    let mut policy = brenn_lib::access::AppPolicy::default();
    for address in addresses {
        match ChannelScheme::split(address) {
            Some((ChannelScheme::Mqtt, _)) => {
                let parsed = brenn_lib::mqtt::address::parse_mqtt_address(address)
                    .expect("delivery_policy_for_addresses: valid mqtt address");
                policy.grants.insert(AppCapability::MqttSubscribe);
                policy.acls.mqtt_subscribe.push(MqttSubMatcher {
                    client: parsed.client,
                    topic_filter: parsed.topic,
                });
            }
            Some((ChannelScheme::Brenn, channel)) => {
                policy.grants.insert(AppCapability::MessagingSubscribe);
                policy
                    .acls
                    .brenn_subscribe
                    .push(ChannelMatcher::Exact(channel.to_string()));
            }
            Some((ChannelScheme::Webhook, endpoint)) => {
                policy.grants.insert(AppCapability::Webhook);
                policy.acls.webhook.push(WebhookMatcher {
                    endpoint: endpoint.to_string(),
                });
            }
            Some((ChannelScheme::Ephemeral | ChannelScheme::PwaPush | ChannelScheme::Local, _))
            | None => {
                panic!("delivery_policy_for_addresses: unrecognized address prefix in {address:?}");
            }
        }
    }
    policy
}

/// Build a `wasm_policies` map (slug → delivery policy) from a set of channel
/// entries: each `Wasm(slug)` subscriber gets a policy authorizing delivery on
/// *every* channel address it subscribes to (design §2.2 Point A — the
/// delivery-time ACL gate now denies any `Wasm` subscriber whose policy does not
/// cover the channel). Derived directly from the fixture's own entries so the
/// policy always matches the wiring. Wraps `delivery_policy_for_addresses`; lives
/// here (next to that leaf) so every `brenn`-crate test family that wires `Wasm`
/// subscribers can share it instead of hand-rolling the per-entry logic inline.
pub(crate) fn wasm_policies_from_entries(
    entries: &[brenn_lib::messaging::ChannelEntry],
) -> HashMap<String, brenn_lib::access::AppPolicy> {
    use brenn_lib::messaging::SubscriberEntryKind;

    let mut by_slug: HashMap<String, Vec<String>> = HashMap::new();
    for entry in entries {
        for sub in &entry.subscribers {
            if let SubscriberEntryKind::Wasm(slug) = &sub.kind {
                by_slug
                    .entry(slug.clone())
                    .or_default()
                    .push(entry.address.clone());
            }
        }
    }
    by_slug
        .into_iter()
        .map(|(slug, addrs)| {
            let policy = delivery_policy_for_addresses(addrs.iter().map(|a| a.as_str()));
            (slug, policy)
        })
        .collect()
}

/// Create a default test app registry with a single "test" app.
pub(crate) fn test_apps() -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    apps.insert(
        "test".to_string(),
        default_test_app_config("test", "Test App"),
    );
    Arc::new(apps)
}

/// Create a multi-app registry from a list of slugs. Each app's
/// display name is `"<slug> app"`.
pub(crate) fn test_apps_multi(slugs: &[&str]) -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    for slug in slugs {
        apps.insert(
            slug.to_string(),
            default_test_app_config(slug, &format!("{slug} app")),
        );
    }
    Arc::new(apps)
}

/// Build a minimal AppConfig for `select_clone_container` tests.
/// Only fields that matter: `slug`, `container_spawn`, `mounts`.
pub(crate) fn clone_test_app(
    slug: &str,
    container_spawn: Option<brenn_lib::config::ContainerSpawnConfig>,
    mounts: Vec<brenn_lib::config::ResolvedMount>,
) -> AppConfig {
    let mut cfg = default_test_app_config(slug, slug);
    cfg.container_spawn = container_spawn;
    cfg.mounts = mounts;
    cfg
}

pub(crate) fn clone_test_mount(
    slug: &str,
    access: AccessLevel,
) -> brenn_lib::config::ResolvedMount {
    brenn_lib::config::ResolvedMount {
        slug: slug.into(),
        host_path: PathBuf::from(format!("/tmp/{slug}")),
        container_path: Some(PathBuf::from(format!("/home/user/repos/{slug}"))),
        access,
        auto_pull: false,
        is_working_dir: false,
        primary: false,
    }
}

pub(crate) fn clone_test_container(home: &str) -> brenn_lib::config::ContainerSpawnConfig {
    brenn_lib::config::ContainerSpawnConfig {
        image: "brenn-cc:latest".into(),
        home_dir: PathBuf::from(home),
        container_home: PathBuf::from("/home/user"),
        host_working_dir: PathBuf::from("/tmp/workdir"),
        container_working_dir: PathBuf::from("/home/user/workdir"),
        working_dir_is_repo: false,
        repo_mounts: vec![],
        extra_mounts: vec![],
        extra_args: vec![],
    }
}

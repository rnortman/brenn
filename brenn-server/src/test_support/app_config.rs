use std::sync::Arc;

use brenn_lib::config::{AppConfig, test_app_config};
use brenn_lib::messaging::config::ResolvedMessagingConfig;
use brenn_lib::webhook::ResolvedWebhookSubscription;
use indexmap::IndexMap;

/// The shared minimal `AppConfig`, with a display name distinct from the slug.
pub fn default_test_app_config(slug: &str, name: &str) -> AppConfig {
    AppConfig {
        name: name.to_string(),
        ..test_app_config(slug)
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
pub fn mqtt_acl_policy(client: &str, filter: &str) -> brenn_lib::access::AppPolicy {
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

/// Create a default test app registry with a single "test" app.
pub fn test_apps() -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    apps.insert(
        "test".to_string(),
        default_test_app_config("test", "Test App"),
    );
    Arc::new(apps)
}

/// Create a multi-app registry from a list of slugs. Each app's
/// display name is `"<slug> app"`.
pub fn test_apps_multi(slugs: &[&str]) -> Arc<IndexMap<String, AppConfig>> {
    let mut apps = IndexMap::new();
    for slug in slugs {
        apps.insert(
            slug.to_string(),
            default_test_app_config(slug, &format!("{slug} app")),
        );
    }
    Arc::new(apps)
}

/// Minimal `AppConfig` for composition-level test fixtures: a singleton app
/// under `/tmp` with one allowed user, an unset model, and a short replay
/// window.
pub fn minimal_app_config(
    slug: &str,
    messaging: Option<ResolvedMessagingConfig>,
    webhook_subscriptions: Vec<ResolvedWebhookSubscription>,
) -> AppConfig {
    AppConfig {
        working_dir: std::path::PathBuf::from("/tmp"),
        model: String::new(),
        singleton: true,
        prefix_device: false,
        allowed_users: vec!["alice".to_string()],
        history_replay_limit: 100,
        state_dir: std::path::PathBuf::from("/tmp"),
        messaging,
        webhook_subscriptions,
        ..test_app_config(slug)
    }
}

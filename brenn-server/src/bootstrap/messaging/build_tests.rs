//! End-to-end tests for `build_messaging`, `build_apps_with_messaging`, and
//! `merge_apps_for_messenger`.

use super::test_fixtures::{
    minimal_app_config, minimal_surface_raw, minimal_wasm_consumer, resolved_ingress_sub,
    surface_sub_raw,
};
use super::*;
use brenn_lib::config::AppConfig;
use brenn_lib::messaging::config::{MessagingGlobalConfig, WasmGrant};
use brenn_lib::webhook::ResolvedWebhookSubscription;

/// An empty tool registry for `build_messaging` calls that do not exercise the
/// async tool substrate: with no async tools registered, no `brenn:tools/*`
/// channels or `system:tool-executor` policy are derived, so these tests observe
/// the pre-tool-substrate behavior unchanged.
fn empty_tool_registry() -> std::sync::Arc<crate::tool_registry::ToolRegistry> {
    std::sync::Arc::new(crate::tool_registry::ToolRegistry::new(vec![]))
}

/// A webhook-only app (no `[app.messaging]` block, only
/// `[[app.webhook_subscription]]` entries) must appear in
/// `apps_with_messaging` with a synthesised `ResolvedMessagingConfig`
/// whose `subscriptions` list carries one entry for each declared
/// webhook subscription. This is the phonebuddy target shape.
#[test]
fn webhook_only_app_included_in_apps_with_messaging() {
    let slug = "phonebuddy";
    let endpoint_slug = "pb-events";

    let app = minimal_app_config(
        slug,
        None, // no [app.messaging] block
        vec![ResolvedWebhookSubscription {
            endpoint_slug: endpoint_slug.to_string(),
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        }],
    );

    let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
    apps.insert(slug.to_string(), app);

    let global = MessagingGlobalConfig::default();
    let result = build_apps_with_messaging(&apps, &global);

    // The webhook-only app must appear in the result.
    assert_eq!(result.len(), 1, "webhook-only app must be included");
    let (result_slug, result_cfg) = &result[0];
    assert_eq!(result_slug, slug);

    // The synthesised config must carry the webhook subscription.
    assert_eq!(
        result_cfg.subscriptions.len(),
        1,
        "one subscription expected"
    );
    let sub = &result_cfg.subscriptions[0];
    assert_eq!(
        sub.channel_address,
        format!("webhook:{endpoint_slug}"),
        "channel_address must be webhook:<slug>"
    );
    assert_eq!(
        sub.channel_uuid,
        brenn_lib::messaging::webhook_channel_uuid_from_slug(endpoint_slug),
        "channel_uuid must be deterministically derived from endpoint slug"
    );
    // push_depth=Unbounded so Immediate wakes propagate.
    assert!(
        sub.push_depth.is_push_enabled(),
        "push_depth must be push-enabled so Immediate wakes survive"
    );
    // (The former `!result_cfg.enabled` assertion was removed with the
    // `ResolvedMessagingConfig::enabled` field — messaging-send authorization
    // is now decided by the app's `AppPolicy`, not this synthesised config.)
}

/// An MQTT-bridge-only app (no `[app.messaging]` block, only
/// `[[app.mqtt_subscription]]` entries) must appear in `apps_with_messaging`
/// with a synthesised `ResolvedMessagingConfig` whose `subscriptions` list
/// carries one `mqtt:<bridge>` entry per declared subscription, derived
/// exactly as webhook subscriptions are.
#[test]
fn mqtt_only_app_included_in_apps_with_messaging() {
    let slug = "pa-alice";
    let address = "mqtt:ha:home/+/state";

    let mut app = minimal_app_config(slug, None, vec![]);
    app.mqtt_subscriptions = vec![resolved_ingress_sub(address)];

    let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
    apps.insert(slug.to_string(), app);

    let global = MessagingGlobalConfig::default();
    let result = build_apps_with_messaging(&apps, &global);

    // The mqtt-only app must appear in the result.
    assert_eq!(result.len(), 1, "mqtt-only app must be included");
    let (result_slug, result_cfg) = &result[0];
    assert_eq!(result_slug, slug);

    // The synthesised config must carry exactly one mqtt: subscription with
    // the resolved channel UUID and `mqtt:<client>:<topic>` address.
    assert_eq!(
        result_cfg.subscriptions.len(),
        1,
        "one mqtt subscription expected"
    );
    let sub = &result_cfg.subscriptions[0];
    assert_eq!(
        sub.channel_address, address,
        "channel_address must be the resolved mqtt:<client>:<topic>"
    );
    assert_eq!(
        sub.channel_uuid,
        brenn_lib::messaging::mqtt_channel_uuid_from_address(address),
        "channel_uuid must be derived from the resolved address"
    );
    assert!(
        sub.push_depth.is_push_enabled(),
        "push_depth must be push-enabled so Immediate wakes survive"
    );
    // (The former `!result_cfg.enabled` assertion was removed with the
    // `ResolvedMessagingConfig::enabled` field — messaging-send authorization
    // is now decided by the app's `AppPolicy`, not this synthesised config.)
}

/// The merged apps map must expose an `[[app.mqtt_subscription]]` on the
/// app's `.messaging` so `resolve_push_targets` finds the matching
/// `ResolvedSubscription` when an MQTT envelope is published to the channel
/// — without it, `resolve_push_targets` would panic.
#[test]
fn merged_apps_map_contains_mqtt_subscription() {
    let slug = "pa-alice";
    let address = "mqtt:ha:home/+/state";

    let mut app = minimal_app_config(slug, None, vec![]);
    app.mqtt_subscriptions = vec![resolved_ingress_sub(address)];
    let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
    apps.insert(slug.to_string(), app);

    let global = MessagingGlobalConfig::default();
    let apps_with_messaging = build_apps_with_messaging(&apps, &global);

    // Call the production merge function — NOT a hand-replicated loop.
    let merged = merge_apps_for_messenger(&apps, &apps_with_messaging);

    let merged_app = merged.get(slug).expect("app must be in merged map");
    let merged_msg = merged_app
        .messaging
        .as_ref()
        .expect("merged app must have messaging (synthesised from mqtt sub)");
    let mqtt_sub = merged_msg
        .subscriptions
        .iter()
        .find(|s| s.channel_address == address)
        .expect(
            "merged apps map must contain the mqtt subscription for the app; \
                 without the fix, resolve_push_targets would panic",
        );
    assert!(
        mqtt_sub.push_depth.is_push_enabled(),
        "merged mqtt subscription must have push-enabled depth (Unbounded)"
    );
}

/// An app with no messaging block and no webhook subscriptions must not
/// appear in `apps_with_messaging` at all.
#[test]
fn app_with_no_messaging_excluded() {
    let app = minimal_app_config("silent-app", None, vec![]);
    let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
    apps.insert("silent-app".to_string(), app);

    let global = MessagingGlobalConfig::default();
    let result = build_apps_with_messaging(&apps, &global);
    assert!(result.is_empty(), "app with no messaging must be excluded");
}

/// Regression: `merge_apps_for_messenger` (production code, called by
/// `build_messaging`) must write the webhook-derived `ResolvedSubscription`
/// onto each app's `.messaging` field so `resolve_push_targets` can find it.
///
/// Shape (a): webhook-only app (phonebuddy target shape — no [app.messaging]).
///
/// This test calls `merge_apps_for_messenger` directly rather than
/// copy-pasting the merge loop. It would fail against the pre-fix code that
/// passed `apps.clone()` (unmerged) to `Messenger::new`, because the unmerged
/// map leaves `.messaging = None` on webhook-only apps.
#[test]
fn merged_apps_map_contains_webhook_subscription_webhook_only_shape() {
    let slug = "phonebuddy";
    let endpoint_slug = "pb-events";

    let app = minimal_app_config(
        slug,
        None,
        vec![ResolvedWebhookSubscription {
            endpoint_slug: endpoint_slug.to_string(),
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        }],
    );
    let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
    apps.insert(slug.to_string(), app);

    let global = MessagingGlobalConfig::default();
    let apps_with_messaging = build_apps_with_messaging(&apps, &global);

    // Call the production merge function — NOT a hand-replicated loop.
    let merged = merge_apps_for_messenger(&apps, &apps_with_messaging);

    // The merged map must expose the webhook subscription on the app's
    // `.messaging` — this is what `resolve_push_targets` reads.
    let merged_app = merged.get(slug).expect("app must be in merged map");
    let merged_msg = merged_app
        .messaging
        .as_ref()
        .expect("merged app must have messaging (synthesised from webhook sub)");
    let webhook_sub = merged_msg
        .subscriptions
        .iter()
        .find(|s| s.channel_address == format!("webhook:{endpoint_slug}"));
    let webhook_sub = webhook_sub.expect(
        "merged apps map must contain the webhook subscription for the app; \
             without the fix, resolve_push_targets would panic",
    );
    // push_depth must be push-enabled — the panic in resolve_push_targets is
    // gated behind is_push_enabled(), so a Bounded(0) sub would silently skip
    // rather than panic, masking the fix.
    assert!(
        webhook_sub.push_depth.is_push_enabled(),
        "merged webhook subscription must have push-enabled depth (Unbounded)"
    );
}

/// Regression: `merge_apps_for_messenger` must preserve existing brenn:
/// subscriptions and append the webhook subscription.
///
/// Shape (b): app with existing [app.messaging] brenn: subscriptions PLUS
/// a webhook subscription (pa-alice / prod target shape).
#[test]
fn merged_apps_map_contains_webhook_subscription_brenn_plus_webhook_shape() {
    use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedSubscription};

    let slug = "pa-alice";
    let endpoint_slug = "push-alice";
    let brenn_channel_uuid = uuid::Uuid::new_v4();
    let brenn_channel_addr = "brenn:some-channel".to_string();

    let existing_brenn_sub = ResolvedSubscription {
        channel_uuid: brenn_channel_uuid,
        channel_address: brenn_channel_addr.clone(),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: brenn_lib::messaging::WakeMin::Normal,
    };

    let app = minimal_app_config(
        slug,
        Some(brenn_lib::messaging::config::ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![existing_brenn_sub.clone()],
        }),
        vec![ResolvedWebhookSubscription {
            endpoint_slug: endpoint_slug.to_string(),
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        }],
    );
    let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
    apps.insert(slug.to_string(), app);

    let global = MessagingGlobalConfig::default();
    let apps_with_messaging = build_apps_with_messaging(&apps, &global);

    // Call the production merge function — NOT a hand-replicated loop.
    let merged = merge_apps_for_messenger(&apps, &apps_with_messaging);

    let merged_app = merged.get(slug).expect("app must be in merged map");
    let merged_msg = merged_app
        .messaging
        .as_ref()
        .expect("merged app must have messaging");

    // Existing brenn: subscription must still be present.
    assert!(
        merged_msg
            .subscriptions
            .iter()
            .any(|s| s.channel_address == brenn_channel_addr),
        "merged app must retain the original brenn: subscription"
    );

    // The webhook subscription must have been added.
    let webhook_sub = merged_msg
        .subscriptions
        .iter()
        .find(|s| s.channel_address == format!("webhook:{endpoint_slug}"));
    let webhook_sub = webhook_sub.expect(
        "merged apps map must contain the webhook subscription for the app; \
             without the fix, resolve_push_targets would panic",
    );
    // push_depth must be push-enabled — the panic in resolve_push_targets is
    // gated behind is_push_enabled(), so a Bounded(0) sub would silently skip
    // rather than panic, masking the fix.
    assert!(
        webhook_sub.push_depth.is_push_enabled(),
        "merged webhook subscription must have push-enabled depth (Unbounded)"
    );

    // Total: original + webhook.
    assert_eq!(
        merged_msg.subscriptions.len(),
        2,
        "merged config must have exactly 2 subscriptions (brenn + webhook)"
    );
}

// -------------------------------------------------------------------------
// End-to-end regression: drive resolve_push_targets through a real Messenger
// constructed the way build_messaging constructs it, for a webhook subscriber.
//
// These tests exercise the ACTUAL fix site: the apps map argument to
// Messenger::new. They fail against pre-fix code (apps.clone() unmerged).
// -------------------------------------------------------------------------

/// Build a webhook channel entry and directory for `endpoint_slug`.
fn webhook_channel_and_directory(
    slug: &str,
    endpoint_slug: &str,
) -> (
    brenn_lib::messaging::ChannelEntry,
    Arc<brenn_lib::messaging::MessagingDirectory>,
) {
    use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
    use brenn_lib::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind,
    };

    let uuid = brenn_lib::messaging::webhook_channel_uuid_from_slug(endpoint_slug);
    let address = format!("webhook:{endpoint_slug}");
    let entry = ChannelEntry {
        uuid,
        address: address.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::App(slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(brenn_lib::messaging::WakeMin::Normal),
        }],
        transport_type: ChannelScheme::Webhook,
        mount: None,
    };
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry.clone()]));
    (entry, directory)
}

/// Noop wake router for tests that don't care about delivery.
struct NoopWakeRouter;

#[async_trait::async_trait]
impl brenn_lib::messaging::WakeRouter for NoopWakeRouter {
    async fn deliver(
        &self,
        _: &brenn_lib::messaging::SubscriberEntryKind,
        _: &brenn_lib::messaging::ParticipantId,
        _: &brenn_lib::messaging::MessageEnvelope,
        _push_id: i64,
        _seq: i64,
    ) -> Result<bool, String> {
        Ok(false)
    }
    async fn deliver_ingress(
        &self,
        _: &brenn_lib::messaging::SubscriberEntryKind,
        _: &brenn_lib::messaging::ParticipantId,
        _: &brenn_lib::messaging::ingress::Event,
    ) -> Result<bool, String> {
        Ok(false)
    }
    fn spawn_eager_wake(
        &self,
        _: &brenn_lib::messaging::SubscriberEntryKind,
        _: &brenn_lib::messaging::ParticipantId,
    ) {
    }
    fn delivery_shape(
        &self,
        key: &brenn_lib::messaging::SubscriberEntryKind,
    ) -> brenn_lib::messaging::DeliveryShape {
        brenn_lib::messaging::default_delivery_shape(key)
    }
    fn alarm(&self, _: &str, _: &brenn_lib::messaging::ParticipantId) {}
}

/// Regression (end-to-end, webhook-only shape): `publish_transport_ingress`
/// must complete without panicking when the Messenger is constructed with the
/// merged apps map (post-fix). This tests the actual fix site — the apps
/// argument to `Messenger::new` — and would fail if `build_messaging` were
/// reverted to pass `apps.clone()` (unmerged).
///
/// The test drives `resolve_push_targets` through a Messenger built exactly
/// as `build_messaging` builds it (via `merge_apps_for_messenger`). A user +
/// singleton conversation is seeded in the in-memory DB so
/// `get_or_create_singleton_conversation` succeeds and returns a push target.
#[tokio::test]
async fn resolve_push_targets_no_panic_with_merged_apps_webhook_only() {
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::{Messenger, WebhookEnvelope};

    let slug = "phonebuddy";
    let endpoint_slug = "pb-events";

    let app = minimal_app_config(
        slug,
        None, // no [app.messaging] — webhook-only shape
        vec![ResolvedWebhookSubscription {
            endpoint_slug: endpoint_slug.to_string(),
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        }],
    );
    let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
    apps.insert(slug.to_string(), app);

    let global = MessagingGlobalConfig::default();
    let apps_with_messaging = build_apps_with_messaging(&apps, &global);

    // Production merge — this is the fix site.
    let merged = Arc::new(merge_apps_for_messenger(&apps, &apps_with_messaging));

    let (channel_entry, directory) = webhook_channel_and_directory(slug, endpoint_slug);

    // Seed DB: user "alice" + singleton conversation for "phonebuddy".
    let db = init_db_memory();
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
                 VALUES (1, 'alice', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&channel_entry));
    }

    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("brenn://test"),
        merged,
        Arc::new(NoopWakeRouter),
        MessagingGlobalConfig::default(),
    );

    // Build a minimal valid WebhookEnvelope body.
    let envelope = WebhookEnvelope {
        headers: vec![],
        key_id: "test-key".to_string(),
        client_ip: "127.0.0.1".to_string(),
        received_at: chrono::Utc::now(),
        body: "{}".to_string(),
        endpoint_slug: endpoint_slug.to_string(),
    };
    let body = serde_json::to_string(&envelope).unwrap();

    // publish_transport_ingress calls resolve_push_targets internally.
    // If the apps map is not merged it panics; with the fix it must complete.
    messenger
        .publish_transport_ingress(
            Arc::new(channel_entry),
            &format!("webhook:{endpoint_slug}"),
            "test-key",
            &body,
            brenn_lib::messaging::Urgency::Normal,
        )
        .await;
    // Reaching this line proves no panic occurred — the fix is in effect.
}

/// Regression (end-to-end, brenn+webhook shape): same guarantee for the
/// prod pa-alice shape (existing brenn: messaging config + webhook sub).
#[tokio::test]
async fn resolve_push_targets_no_panic_with_merged_apps_brenn_plus_webhook() {
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedSubscription};
    use brenn_lib::messaging::{Messenger, WebhookEnvelope};

    let slug = "pa-alice";
    let endpoint_slug = "push-alice";
    let brenn_channel_uuid = uuid::Uuid::new_v4();

    let existing_brenn_sub = ResolvedSubscription {
        channel_uuid: brenn_channel_uuid,
        channel_address: "brenn:some-channel".to_string(),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: brenn_lib::messaging::WakeMin::Normal,
    };

    let app = minimal_app_config(
        slug,
        Some(brenn_lib::messaging::config::ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![existing_brenn_sub],
        }),
        vec![ResolvedWebhookSubscription {
            endpoint_slug: endpoint_slug.to_string(),
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        }],
    );
    let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
    apps.insert(slug.to_string(), app);

    let global = MessagingGlobalConfig::default();
    let apps_with_messaging = build_apps_with_messaging(&apps, &global);

    // Production merge — this is the fix site.
    let merged = Arc::new(merge_apps_for_messenger(&apps, &apps_with_messaging));

    let (channel_entry, directory) = webhook_channel_and_directory(slug, endpoint_slug);

    // Seed DB: user "alice" + singleton conversation.
    let db = init_db_memory();
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
                 VALUES (1, 'alice', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&channel_entry));
    }

    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("brenn://test"),
        merged,
        Arc::new(NoopWakeRouter),
        MessagingGlobalConfig::default(),
    );

    let envelope = WebhookEnvelope {
        headers: vec![],
        key_id: "test-key".to_string(),
        client_ip: "127.0.0.1".to_string(),
        received_at: chrono::Utc::now(),
        body: "{}".to_string(),
        endpoint_slug: endpoint_slug.to_string(),
    };
    let body = serde_json::to_string(&envelope).unwrap();

    messenger
        .publish_transport_ingress(
            Arc::new(channel_entry),
            &format!("webhook:{endpoint_slug}"),
            "test-key",
            &body,
            brenn_lib::messaging::Urgency::Normal,
        )
        .await;
    // Reaching this line proves no panic occurred — the fix is in effect.
}

/// An ingress channel produces an `mqtt:<client>:<topic>` `ChannelEntry` in
/// the directory. Drives the real
/// `build_messaging` with one `ResolvedMqttIngressChannel` and asserts the
/// resulting `Messenger` directory carries the `mqtt:<client>:<topic>` channel
/// with `transport_type = Mqtt` and the resolved-address UUID. This is the
/// only test that exercises the `mqtt_channel_entries` derivation loop
/// end-to-end: if that loop were dropped, misconfigured, or used the wrong
/// UUID namespace, this test would fail.
#[tokio::test]
async fn build_messaging_derives_mqtt_channel_entry() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::mqtt_channel_uuid_from_address;
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use indexmap::IndexMap as IM;

    let address = "mqtt:homeassistant:home/+/state";
    let channel_def = ResolvedMqttIngressChannel {
        channel_address: address.to_string(),
        channel_uuid: mqtt_channel_uuid_from_address(address),
        client_slug: "homeassistant".to_string(),
        topic: "home/+/state".to_string(),
        qos: 1,
        urgency: brenn_lib::messaging::Urgency::Normal,
    };

    let config = BrennConfig::default();
    let db = init_db_memory();
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        db,
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        std::slice::from_ref(&channel_def),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    // Ingress channels alone bring the service up.
    let messenger = result
        .messenger
        .expect("a declared mqtt ingress channel must bring messaging up");

    let expected_uuid = mqtt_channel_uuid_from_address(address);
    let channel = messenger
        .directory()
        .by_uuid(&expected_uuid)
        .expect("mqtt:<client>:<topic> channel must be derived into the directory");
    assert_eq!(channel.address, address);
    assert_eq!(
        channel.transport_type,
        ChannelScheme::Mqtt,
        "derived channel must carry transport_type = Mqtt"
    );
    assert_eq!(
        channel.uuid, expected_uuid,
        "channel UUID must be the resolved-address derivation"
    );
}

/// A runtime-created `mqtt:` dynamic subscription survives a restart. Pre-seed
/// `messaging_channels` + `messaging_dynamic_subscriptions` with a channel
/// that is **not** in any config (exactly the runtime-created state), drive
/// the real `build_messaging` with a config that omits that filter, and
/// assert the persistence loop closes: the channel is folded back into the
/// directory (so the merge keeps the row instead of dropping it), the durable
/// row is **not** pruned, and a `DynamicMqttIngress` re-activation descriptor
/// is produced so the broker SUBSCRIBE + route get rebuilt. Before the boot
/// fold this test failed: the channel was absent from the config-built
/// directory, the merge dropped the row, and `prune` erased it.
#[tokio::test]
async fn build_messaging_reconstructs_runtime_created_mqtt_channel() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::mqtt_channel_uuid_from_address;
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use indexmap::IndexMap as IM;

    let address = "mqtt:homeassistant:home/runtime/+/state";
    let channel_uuid = mqtt_channel_uuid_from_address(address);

    // A restart still has the mqtt *client* configured (a dynamic mqtt
    // subscribe goes through one); only the runtime-created filter is absent
    // from config. Model that with one unrelated static ingress channel for
    // the same client — it brings messaging up (the `any_messaging` gate) and
    // is exactly the design's "config that omits that filter" restart state.
    let static_address = "mqtt:homeassistant:home/static/state";
    let static_channel = ResolvedMqttIngressChannel {
        channel_address: static_address.to_string(),
        channel_uuid: mqtt_channel_uuid_from_address(static_address),
        client_slug: "homeassistant".to_string(),
        topic: "home/static/state".to_string(),
        qos: 1,
        urgency: brenn_lib::messaging::Urgency::Normal,
    };

    let config = BrennConfig::default();
    let db = init_db_memory();
    // Seed the DB to look exactly like "a runtime dynamic subscribe created
    // this mqtt: channel and persisted its durable row" — but the channel is
    // in NO config (no static ingress channel, no app). This is the state a
    // genuine restart would find.
    {
        let conn = db.lock().await;
        let channel_entry = brenn_lib::messaging::ChannelEntry {
            uuid: channel_uuid,
            address: address.to_string(),
            description: None,
            transport_type: ChannelScheme::Mqtt,
            resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: brenn_lib::messaging::config::Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: Vec::new(),
            mount: None,
        };
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&channel_entry));
        conn.execute(
                "INSERT INTO messaging_dynamic_subscriptions \
                 (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, qos, created_at) \
                 VALUES (?1, 'graf', '0', '5', 'silent', 'normal', 1, '2026-06-20T00:00:00Z')",
                rusqlite::params![channel_uuid.as_bytes().to_vec()],
            )
            .expect("seed durable dynamic row");
    }

    // The dynamic row's app (`graf`) must carry a policy that authorizes
    // delivery on the runtime-created channel; otherwise the boot ACL gate
    // classifies the row `revoked` (correctly) instead of `kept`.
    // This test pins the *persistence* path (Fix 1), so grant the covering
    // policy — a separate test pins the revoked path.
    let mut apps_map: IM<String, AppConfig> = IM::new();
    let mut graf_app = minimal_app_config("graf", None, vec![]);
    graf_app.policy = crate::test_support::app_config::delivery_policy_for_addresses([address]);
    apps_map.insert("graf".to_string(), graf_app);
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    // The runtime-created filter is absent from config; only the unrelated
    // static channel for the same client is present.
    let result = build_messaging(
        &config,
        db.clone(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        std::slice::from_ref(&static_channel),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger = result
        .messenger
        .expect("a persisted dynamic mqtt subscription must bring messaging up");

    // (a) The runtime-created channel is folded back into the directory.
    let channel = messenger
        .directory()
        .by_uuid(&channel_uuid)
        .expect("runtime-created mqtt: channel must be reconstructed into the boot directory");
    assert_eq!(channel.address, address);
    assert_eq!(channel.transport_type, ChannelScheme::Mqtt);

    // (b) The durable row survives — it was kept, not dropped+pruned.
    {
        let conn = db.lock().await;
        let rows = brenn_lib::messaging::db::load_dynamic_subscriptions(&conn);
        assert_eq!(
            rows.len(),
            1,
            "durable dynamic row must survive the restart"
        );
        assert_eq!(rows[0].channel_uuid, channel_uuid);
        assert_eq!(rows[0].app_slug, "graf");
    }

    // (c) A re-activation descriptor is produced so the broker SUBSCRIBE +
    // IngressRoute get rebuilt for the reconstructed channel.
    assert_eq!(
        result.dynamic_mqtt_ingress.len(),
        1,
        "kept dynamic mqtt row must yield a DynamicMqttIngress re-activation descriptor"
    );
    assert_eq!(result.dynamic_mqtt_ingress[0].channel_uuid, channel_uuid);
    assert_eq!(result.dynamic_mqtt_ingress[0].channel_address, address);
}

/// An **orphan** channel — present in `messaging_channels` but with NO
/// surviving `messaging_dynamic_subscriptions` row (the unsubscribed state,
/// where `unsubscribe_dynamic` deleted the durable row) — must NOT be
/// reconstructed into the boot directory. The boot fold is scoped to UUIDs
/// referenced by surviving durable rows, so an orphan's UUID is never in
/// `referenced_uuids`.
/// This pins the scoped-load invariant at the boot-integration level:
/// if the fold were widened to a full-table `messaging_channels` load, the
/// orphan would appear in the directory and accumulate per-boot — caught here.
#[tokio::test]
async fn build_messaging_does_not_reconstruct_orphan_channel() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::mqtt_channel_uuid_from_address;
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use indexmap::IndexMap as IM;

    // The orphan: a runtime-created channel whose only dynamic subscription was
    // later unsubscribed, so its durable row is gone but the channel row lingers.
    let orphan_address = "mqtt:homeassistant:home/orphan/+/state";
    let orphan_uuid = mqtt_channel_uuid_from_address(orphan_address);

    // An unrelated static ingress channel brings messaging up (any_messaging gate).
    let static_address = "mqtt:homeassistant:home/static/state";
    let static_channel = ResolvedMqttIngressChannel {
        channel_address: static_address.to_string(),
        channel_uuid: mqtt_channel_uuid_from_address(static_address),
        client_slug: "homeassistant".to_string(),
        topic: "home/static/state".to_string(),
        qos: 1,
        urgency: brenn_lib::messaging::Urgency::Normal,
    };

    let config = BrennConfig::default();
    let db = init_db_memory();
    // Seed ONLY the channel row — NO messaging_dynamic_subscriptions row.
    {
        let conn = db.lock().await;
        let orphan_entry = brenn_lib::messaging::ChannelEntry {
            uuid: orphan_uuid,
            address: orphan_address.to_string(),
            description: None,
            transport_type: ChannelScheme::Mqtt,
            resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: brenn_lib::messaging::config::Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: Vec::new(),
            mount: None,
        };
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&orphan_entry));
    }

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        db.clone(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        std::slice::from_ref(&static_channel),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger = result
        .messenger
        .expect("the static ingress channel must bring messaging up");

    // The orphan channel must NOT be in the directory (it was never referenced
    // by a surviving durable row, so the scoped boot fold did not load it).
    assert!(
        messenger.directory().by_uuid(&orphan_uuid).is_none(),
        "orphan channel (no surviving durable row) must not be reconstructed into the directory"
    );
    // It also produces no re-activation descriptor.
    assert!(
        result
            .dynamic_mqtt_ingress
            .iter()
            .all(|d| d.channel_uuid != orphan_uuid),
        "orphan channel must not be re-activated"
    );
}

/// Cross-restart ACL revocation, end to end. A persisted dynamic `mqtt:` subscription
/// whose app's policy no longer covers the channel must, at boot:
///   (a) keep its durable `messaging_dynamic_subscriptions` row (NOT pruned —
///       the operator may re-grant; pruning would destroy durable user state);
///   (b) fold NO subscriber onto the channel (the merge classifies it
///       `revoked`, so the directory entry's subscriber list is empty); and
///   (c) produce NO `DynamicMqttIngress` re-activation descriptor (a `revoked`
///       row is not in `kept`, so the broker SUBSCRIBE is NOT re-asserted —
///       we stop pulling traffic from the broker, not just dropping it).
/// Then a *second* restart with the ACL restored resumes the subscription
/// (subscriber folded, re-activation descriptor produced) — the non-prune of
/// `revoked` rows is what makes resumption possible.
#[tokio::test]
async fn build_messaging_revokes_then_resumes_dynamic_mqtt_subscription_across_restart() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::{SubscriberEntryKind, mqtt_channel_uuid_from_address};
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use indexmap::IndexMap as IM;

    let address = "mqtt:homeassistant:home/runtime/+/state";
    let channel_uuid = mqtt_channel_uuid_from_address(address);

    // A restart still has the mqtt *client* configured; model the "config that
    // omits that filter" restart state with one unrelated static ingress
    // channel for the same client (same shape as the persistence test above).
    let static_address = "mqtt:homeassistant:home/static/state";
    let static_channel = ResolvedMqttIngressChannel {
        channel_address: static_address.to_string(),
        channel_uuid: mqtt_channel_uuid_from_address(static_address),
        client_slug: "homeassistant".to_string(),
        topic: "home/static/state".to_string(),
        qos: 1,
        urgency: brenn_lib::messaging::Urgency::Normal,
    };

    let config = BrennConfig::default();
    let db = init_db_memory();
    // Seed exactly as if a runtime dynamic subscribe created this mqtt: channel
    // and persisted its durable row — the channel is in NO config.
    {
        let conn = db.lock().await;
        let channel_entry = brenn_lib::messaging::ChannelEntry {
            uuid: channel_uuid,
            address: address.to_string(),
            description: None,
            transport_type: ChannelScheme::Mqtt,
            resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: brenn_lib::messaging::config::Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: Vec::new(),
            mount: None,
        };
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&channel_entry));
        conn.execute(
                "INSERT INTO messaging_dynamic_subscriptions \
                 (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, qos, created_at) \
                 VALUES (?1, 'graf', '0', '5', 'silent', 'normal', 1, '2026-06-20T00:00:00Z')",
                rusqlite::params![channel_uuid.as_bytes().to_vec()],
            )
            .expect("seed durable dynamic row");
    }

    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    // --- Phase 1: restart with the ACL REVOKED (graf's policy does not cover
    // the runtime channel — `AppPolicy::default()` has no covering matcher). ---
    let apps_revoked: Arc<IndexMap<String, AppConfig>> = {
        let mut m: IM<String, AppConfig> = IM::new();
        // `minimal_app_config` defaults `policy` to `AppPolicy::default()` — no
        // grant, no matcher — so `allows_channel_access` denies: the revoked case.
        m.insert("graf".to_string(), minimal_app_config("graf", None, vec![]));
        Arc::new(m)
    };

    let revoked = build_messaging(
        &config,
        db.clone(),
        &apps_revoked,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        std::slice::from_ref(&static_channel),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger = revoked
        .messenger
        .expect("a persisted dynamic mqtt subscription must bring messaging up");

    // (a) The durable row is RETAINED (not pruned) — the operator may re-grant.
    {
        let conn = db.lock().await;
        let rows = brenn_lib::messaging::db::load_dynamic_subscriptions(&conn);
        assert_eq!(
            rows.len(),
            1,
            "revoked-ACL dynamic row must be retained (not pruned) so it can resume"
        );
        assert_eq!(rows[0].channel_uuid, channel_uuid);
        assert_eq!(rows[0].app_slug, "graf");
    }

    // (b) The channel is reconstructed (Fix 1 folds it regardless of ACL) but
    // NO subscriber is folded onto it — the merge classified the row `revoked`.
    let channel = messenger
        .directory()
        .by_uuid(&channel_uuid)
        .expect("runtime-created mqtt: channel is reconstructed even when its ACL is revoked");
    assert!(
        !channel
            .subscribers
            .iter()
            .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == "graf")),
        "revoked-ACL row must NOT be folded as a subscriber; the channel ends boot empty"
    );

    // (c) NO re-activation descriptor — the broker SUBSCRIBE is NOT re-asserted
    // for a revoked-ACL channel (we stop pulling its traffic from the broker).
    assert!(
        revoked.dynamic_mqtt_ingress.is_empty(),
        "revoked-ACL row must NOT yield a DynamicMqttIngress descriptor (no broker re-SUBSCRIBE)"
    );

    // --- Phase 2: restart AGAIN with the ACL RESTORED. The non-prune of the
    // `revoked` row in phase 1 is what makes resumption possible. ---
    let (alert_dispatcher2, _alert_join2) = AlertDispatcher::noop();
    let apps_restored: Arc<IndexMap<String, AppConfig>> = {
        let mut m: IM<String, AppConfig> = IM::new();
        let mut graf_app = minimal_app_config("graf", None, vec![]);
        graf_app.policy = crate::test_support::app_config::delivery_policy_for_addresses([address]);
        m.insert("graf".to_string(), graf_app);
        Arc::new(m)
    };

    let resumed = build_messaging(
        &config,
        db.clone(),
        &apps_restored,
        ActiveBridges::new(),
        alert_dispatcher2,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        std::slice::from_ref(&static_channel),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger2 = resumed
        .messenger
        .expect("the retained dynamic row must bring messaging up after the ACL is restored");

    // The subscription resumes: subscriber folded back onto the channel...
    let channel2 = messenger2
        .directory()
        .by_uuid(&channel_uuid)
        .expect("channel still reconstructed after restore");
    assert!(
        channel2
            .subscribers
            .iter()
            .any(|s| matches!(&s.kind, SubscriberEntryKind::App(slug) if slug == "graf")),
        "restored-ACL row must be folded back as a subscriber (subscription resumes)"
    );
    // ...and the re-activation descriptor is produced again (broker re-SUBSCRIBE).
    assert_eq!(
        resumed.dynamic_mqtt_ingress.len(),
        1,
        "restored-ACL kept row must yield a DynamicMqttIngress re-activation descriptor"
    );
    assert_eq!(resumed.dynamic_mqtt_ingress[0].channel_uuid, channel_uuid);
}

// -----------------------------------------------------------------------
// Boot-time fail-fast: static subscription with no covering policy
// -----------------------------------------------------------------------

/// Build a `BrennConfig` carrying a single `[[channel]]` (`brenn:<address>`),
/// returning the config and the channel UUID so a subscriber can be wired to it.
fn config_with_one_brenn_channel(address: &str) -> (brenn_lib::config::BrennConfig, uuid::Uuid) {
    use brenn_lib::messaging::config::ChannelConfigRaw;
    let uuid = uuid::Uuid::new_v4();
    let config = brenn_lib::config::BrennConfig {
        channels: vec![ChannelConfigRaw {
            uuid: uuid.to_string(),
            address: address.to_string(),
            description: None,
            push_depth: None,
            retain_depth: None,
            standing_retain_depth: None,
            noise: None,
            sink: None,
            wake_min: None,
        }],
        ..brenn_lib::config::BrennConfig::default()
    };
    (config, uuid)
}

/// An `AppConfig` that statically subscribes to one `brenn:` channel, with the
/// given resolved `AppPolicy` (the field the boot validation reads).
fn app_subscribing_to(
    slug: &str,
    channel_uuid: uuid::Uuid,
    channel_address: &str,
    policy: brenn_lib::access::AppPolicy,
) -> AppConfig {
    let mut app = minimal_app_config(
        slug,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![ResolvedSubscription {
                channel_uuid,
                channel_address: channel_address.to_string(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            }],
        }),
        vec![],
    );
    app.policy = policy;
    app
}

/// A static `[[app.messaging.subscribe]]` whose app policy carries no covering
/// ACL matcher for the channel can never receive — boot must refuse to start
/// (ship-gate fail-fast). This pins that the misconfiguration
/// is loud at startup, not a silent per-delivery deny.
#[tokio::test]
#[should_panic(expected = "can never deliver on")]
async fn build_messaging_panics_on_static_app_sub_without_covering_policy() {
    use brenn_lib::db::init_db_memory;
    use indexmap::IndexMap as IM;

    let address = "boot-acl-app";
    let channel_address = format!("brenn:{address}");
    let (config, channel_uuid) = config_with_one_brenn_channel(address);

    // `AppPolicy::default()` — no `messaging_subscribe` grant, no matcher — so
    // `allows_channel_access("brenn:boot-acl-app")` is false: the dead subscription.
    let mut apps_map: IM<String, AppConfig> = IM::new();
    apps_map.insert(
        "deadsub".to_string(),
        app_subscribing_to(
            "deadsub",
            channel_uuid,
            &channel_address,
            brenn_lib::access::AppPolicy::default(),
        ),
    );
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    // Panics in validate_static_subscriptions_deliverable before any DB work.
    let _ = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// The same static `[[app.messaging.subscribe]]`, but now with a *covering*
/// policy (transport grant + matcher), boots cleanly — proving the validation
/// has no false positive on a grantable subscription (the channel that IS
/// deliverable passes the identical `allows_channel_access` check the runtime uses).
#[tokio::test]
async fn build_messaging_accepts_static_app_sub_with_covering_policy() {
    use brenn_lib::db::init_db_memory;
    use indexmap::IndexMap as IM;

    let address = "boot-acl-app-ok";
    let channel_address = format!("brenn:{address}");
    let (config, channel_uuid) = config_with_one_brenn_channel(address);

    let mut apps_map: IM<String, AppConfig> = IM::new();
    apps_map.insert(
        "livesub".to_string(),
        app_subscribing_to(
            "livesub",
            channel_uuid,
            &channel_address,
            crate::test_support::app_config::delivery_policy_for_addresses([
                channel_address.as_str()
            ]),
        ),
    );
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
    // No panic: a covered static subscription is accepted, and messaging comes up.
    assert!(
        result.messenger.is_some(),
        "a config whose static subscription is deliverable must boot"
    );
}

/// A `[[wasm_consumer.subscription]]` whose resolved WASM policy cannot
/// authorize delivery on the channel (e.g. an empty `subscribe_acl`, so
/// `build_wasm_policy` derives neither the `MessagingSubscribe` grant nor a
/// matcher) is also a dead subscription — boot must refuse to start. This is
/// the dead-subscription footgun for the WASM-consumer class: a subscription
/// authored without the flat ACL list that derives its transport receive grant.
#[tokio::test]
#[should_panic(expected = "can never deliver on")]
async fn build_messaging_panics_on_static_wasm_sub_without_covering_policy() {
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use indexmap::IndexMap as IM;

    let address = "boot-acl-wasm";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(address);
    // A consumer with `ports` granted (so it can output) but an EMPTY
    // `subscribe_acl`: build_wasm_policy derives no MessagingSubscribe grant and
    // no covering matcher, so allows_channel_access("brenn:boot-acl-wasm") is false.
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "deadwasm".to_string(),
        component_path: "/tmp/deadwasm.wasm".into(),
        grants: vec![],
        subscribe_acl: vec![],
        publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: format!("brenn:{address}"),
            port: "in".to_string(),
            push_depth: Some(Depth::Unbounded),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let _ = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// A consumer whose `mqtt_publish` ACL matcher names a client that no
/// `[[mqtt_client]]` declares must panic at boot — the client slug in the
/// guest's `mqtt:` address selects the session, so a matcher naming an
/// undeclared client would authorize a publish with no session to reach it
/// (parallel to the LLM-side `validate_mqtt_client` check). The consumer holds
/// the `mqtt` grant (so the matcher⇒grant check passes) and no `[[mqtt_client]]`
/// is declared, so resolution reaches the matcher⇒declared-client check.
#[tokio::test]
#[should_panic(expected = "no [[mqtt_client]] with that slug is declared")]
async fn build_messaging_panics_on_wasm_mqtt_matcher_undeclared_client() {
    use brenn_lib::access::raw::MqttClientMatcherRaw;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;
    use indexmap::IndexMap as IM;

    let address = "boot-mqtt-matcher-undeclared";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(address);
    // No `[[mqtt_client]]` is declared, but the matcher names client `home`.
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "undeclared".to_string(),
        component_path: "/tmp/undeclared.wasm".into(),
        grants: vec![WasmGrant::Mqtt],
        subscribe_acl: vec![],
        publish_acl: vec![],
        mqtt_publish_acl: vec![MqttClientMatcherRaw {
            client: "home".to_string(),
        }],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![],
        outputs: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let _ = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// A consumer that authors a non-empty `mqtt_publish` ACL but does **not** hold
/// the `mqtt` grant has dead matchers: `build_wasm_policy` derives `MqttPublish`
/// only from the `Mqtt` grant, so without it `allows_mqtt_publish` is
/// unconditionally false and the authored matchers can never authorize any MQTT
/// publish — same shape as the brenn `publish_acl` +
/// `Ports`-grant check. The operator wrote the ACL expecting it to grant
/// egress; silently dropping it is a runtime-only landmine, so fail-fast at boot.
/// The matcher names the declared `home` client (so the matcher⇒declared-client
/// check 2d passes) and the consumer has no inputs/outputs, so resolution
/// reaches the matcher⇒grant check (2f) in isolation.
#[tokio::test]
#[should_panic(expected = "\"mqtt\" is not in grants")]
async fn build_messaging_panics_on_wasm_mqtt_publish_acl_without_mqtt_grant() {
    use brenn_lib::access::raw::MqttClientMatcherRaw;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;
    use indexmap::IndexMap as IM;

    let address = "boot-mqtt-acl-nogrant";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(address);
    // Declare the `home` client so the matcher⇒declared-client check (2d) passes
    // and this exercises the matcher⇒grant check (2f) in isolation.
    config.mqtt_clients = vec![
        toml::from_str("slug = \"home\"\nurl = \"mqtts://127.0.0.1:1\"")
            .expect("minimal raw client config parses"),
    ];
    // Authors an mqtt_publish ACL matcher but `grants` is empty (no `mqtt`).
    // Without the grant the matcher can never authorize a publish — dead config.
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "aclless".to_string(),
        component_path: "/tmp/aclless.wasm".into(),
        grants: vec![],
        subscribe_acl: vec![],
        publish_acl: vec![],
        mqtt_publish_acl: vec![MqttClientMatcherRaw {
            client: "home".to_string(),
        }],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![],
        outputs: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let _ = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// Stronger WASM subscribe-ACL case: the consumer has a *non-empty* `subscribe_acl` — so
/// `build_wasm_policy` DOES derive the `MessagingSubscribe` grant — but the
/// single matcher covers a *different* channel than the one it statically
/// subscribes to. The grant is present yet the channel is outside the ACL, so
/// `allows_channel_access` still returns false and boot must refuse to start. Unlike
/// `build_messaging_panics_on_static_wasm_sub_without_covering_policy` (empty
/// ACL ⇒ no grant at all), this pins that a present-but-non-covering matcher is
/// caught — the precise "static subscription channel outside the subscribe ACL"
/// case — and that the panic names both the offending
/// channel and the consumer slug.
#[tokio::test]
#[should_panic(
    expected = "wasm_consumer \"scoped-wasm\" subscribes to channel \"brenn:secret-channel\""
)]
async fn build_messaging_panics_on_static_wasm_sub_channel_outside_subscribe_acl() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use indexmap::IndexMap as IM;

    // The consumer subscribes to `brenn:secret-channel`, but its subscribe_acl
    // only covers `brenn:allowed-channel`. The grant is derived (non-empty ACL),
    // yet the subscribed channel is outside the matcher set.
    let subscribed = "secret-channel";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(subscribed);
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "scoped-wasm".to_string(),
        component_path: "/tmp/scoped-wasm.wasm".into(),
        grants: vec![],
        // Non-empty ⇒ MessagingSubscribe grant is derived, but the matcher names
        // a different channel, so allows_channel_access("brenn:secret-channel") is false.
        subscribe_acl: vec![ChannelMatcherRaw::Exact("allowed-channel".to_string())],
        publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: format!("brenn:{subscribed}"),
            port: "in".to_string(),
            push_depth: Some(Depth::Unbounded),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let _ = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// Positive WASM-subscribe pair: a
/// `[[wasm_consumer.subscription]]` whose `subscribe_acl` *covers* the
/// subscribed channel boots cleanly — proving
/// `validate_static_subscriptions_deliverable` has no false positive on the
/// WASM path, mirroring `build_messaging_accepts_static_app_sub_with_covering_policy`
/// for the app path. Without this guard, a regression that wrongly rejected a
/// correctly configured WASM subscription would go uncaught.
#[tokio::test]
async fn build_messaging_accepts_static_wasm_sub_with_covering_subscribe_acl() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use indexmap::IndexMap as IM;

    // The consumer subscribes to `brenn:covered-channel` and its subscribe_acl
    // covers exactly that channel, so build_wasm_policy derives the
    // MessagingSubscribe grant AND a covering matcher: allows_channel_access is true.
    let subscribed = "covered-channel";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(subscribed);
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "covered-wasm".to_string(),
        component_path: "/tmp/covered-wasm.wasm".into(),
        grants: vec![],
        subscribe_acl: vec![ChannelMatcherRaw::Exact(subscribed.to_string())],
        publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: format!("brenn:{subscribed}"),
            port: "in".to_string(),
            push_depth: Some(Depth::Unbounded),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
    // No panic: a covered static WASM subscription is accepted and boots.
    assert!(
        result.messenger.is_some(),
        "a WASM config whose static subscription is deliverable must boot"
    );
}

// -----------------------------------------------------------------------
// WASM `webhook:` / `mqtt:` subscribe grants (receive path)
// -----------------------------------------------------------------------

/// One-endpoint webhook map for `build_messaging`, owned by `owning_app_slug`.
fn webhook_endpoint_map(
    endpoint_slug: &str,
    owning_app_slug: &str,
) -> IndexMap<String, Arc<ResolvedWebhookEndpoint>> {
    use brenn_lib::webhook::{ResolvedWebhookEndpoint, SignatureScheme, WebhookOwner};
    let mut m: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IndexMap::new();
    m.insert(
        endpoint_slug.to_string(),
        Arc::new(ResolvedWebhookEndpoint {
            slug: endpoint_slug.to_string(),
            mount: format!("/webhooks/{endpoint_slug}"),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            // Scheme is exercised only at HTTP ingress, not at build_messaging;
            // any valid variant suffices for deriving the channel entry.
            scheme: SignatureScheme::BearerToken {
                header: "authorization".parse().unwrap(),
                token_id_header: None,
                tokens: std::collections::HashMap::new(),
            },
            owner: WebhookOwner::App(Arc::from(owning_app_slug)),
            urgency: brenn_lib::messaging::Urgency::Normal,
            replay_protection: None,
        }),
    );
    m
}

/// New check 2e: a `[[wasm_consumer]]` whose `mqtt_subscribe` ACL matcher names
/// a client that no `[[mqtt_client]]` declares must panic at boot — the client
/// slug in the subscribed `mqtt:` address selects the session, so a matcher
/// naming an undeclared client would authorize delivery from a session that has
/// no broker connection to arrive on (parallel to check 2d for `mqtt_publish`).
/// No `[[mqtt_client]]` is declared and the consumer has no subscriptions, so
/// resolution reaches the matcher⇒declared-client check (2e) in isolation.
#[tokio::test]
#[should_panic(expected = "no [[mqtt_client]] with that slug is declared")]
async fn build_messaging_panics_on_wasm_mqtt_subscribe_matcher_undeclared_client() {
    use brenn_lib::access::raw::MqttSubMatcherRaw;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;
    use indexmap::IndexMap as IM;

    let address = "boot-mqtt-sub-matcher-undeclared";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(address);
    // No `[[mqtt_client]]` is declared, but the matcher names client `home`.
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "undeclared-sub".to_string(),
        component_path: "/tmp/undeclared-sub.wasm".into(),
        grants: vec![],
        subscribe_acl: vec![],
        publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![MqttSubMatcherRaw {
            client: "home".to_string(),
            topic_filter: "sensors/#".to_string(),
        }],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![],
        outputs: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let _ = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// Positive `webhook:` receive (the exact prod `consume-demo-alice` block
/// shape — subscription `webhook:<slug>` + `webhook_acl = [{ endpoint }]` +
/// `ports` grant + covering `publish_acl` + bound output). The non-empty
/// `webhook_acl` derives the `Webhook` receive grant and covers the channel, so
/// `validate_static_subscriptions_deliverable` admits the subscription and the
/// `Wasm` subscriber lands on the `webhook:` channel entry. Exercises the same
/// grant/ACL derivation prod boot runs, using the prod block's exact shape.
#[tokio::test]
async fn build_messaging_accepts_wasm_webhook_sub_prod_block_shape() {
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_lib::messaging::{SubscriberEntryKind, webhook_channel_uuid_from_slug};
    use indexmap::IndexMap as IM;

    let endpoint_slug = "push-alice";
    // The bound output resolves against this brenn: channel; publish_acl covers it.
    let (mut config, _uuid) = config_with_one_brenn_channel("wasm-demo-out");
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "consume-demo-alice".to_string(),
        component_path: "/tmp/brenn_processor_demo.wasm".into(),
        grants: vec![WasmGrant::Ports],
        subscribe_acl: vec![],
        publish_acl: vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "wasm-demo-out".to_string(),
        )],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![brenn_lib::access::raw::WebhookMatcherRaw {
            endpoint: endpoint_slug.to_string(),
        }],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: format!("webhook:{endpoint_slug}"),
            port: "in".to_string(),
            push_depth: Some(Depth::Bounded(50)),
            retain_depth: Some(Depth::Bounded(10)),
            noise: Some(NoiseLevel::Alarm),
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![brenn_lib::messaging::config::WasmConsumerOutputRaw {
            port: "out".to_string(),
            channel: "brenn:wasm-demo-out".to_string(),
            urgency: None,
            publish_per_activation: None,
            publish_capacity: None,
        }],
        config: None,
        activation_burst: Some(60),
        activation_min_period_ms: Some(1000),
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    // The endpoint is owned by an app that need not exist as an [[app]] here —
    // the webhook channel entry + WASM subscriber are what this test exercises.
    let webhook_endpoints = webhook_endpoint_map(endpoint_slug, "pa-alice");

    let result = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger = result
        .messenger
        .expect("a WASM webhook subscription with a covering webhook_acl must boot");
    let channel = messenger
        .directory()
        .by_uuid(&webhook_channel_uuid_from_slug(endpoint_slug))
        .expect("webhook: channel must be derived into the directory");
    assert!(
        channel.subscribers.iter().any(|s| matches!(
            &s.kind,
            SubscriberEntryKind::Wasm(slug) if slug == "consume-demo-alice"
        )),
        "the WASM consumer must be attached as a subscriber on the webhook: channel"
    );
}

/// The same WASM webhook subscription WITHOUT a covering `webhook_acl` (empty
/// list ⇒ no `Webhook` grant derived) is a dead subscription — boot must refuse
/// to start. This is the failure mode that parked the prod block; the un-parked
/// block is safe only because its `webhook_acl` is present.
#[tokio::test]
#[should_panic(expected = "can never deliver on")]
async fn build_messaging_panics_on_wasm_webhook_sub_without_covering_acl() {
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use indexmap::IndexMap as IM;

    let endpoint_slug = "push-alice";
    let (mut config, _uuid) = config_with_one_brenn_channel("unused");
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "consume-demo-alice".to_string(),
        component_path: "/tmp/brenn_processor_demo.wasm".into(),
        grants: vec![],
        subscribe_acl: vec![],
        publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        // Empty webhook_acl ⇒ no Webhook grant ⇒ allows_webhook_delivery is false.
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: format!("webhook:{endpoint_slug}"),
            port: "in".to_string(),
            push_depth: Some(Depth::Unbounded),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints = webhook_endpoint_map(endpoint_slug, "pa-alice");

    let _ = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// Positive `mqtt:` receive: a WASM consumer statically subscribed to an
/// `mqtt:<client>:<topic>` channel with a covering `mqtt_subscribe_acl` boots,
/// and the `Wasm` subscriber lands on the derived `mqtt:` channel entry — a
/// channel no LLM app declares, present only because it is derived from the WASM
/// consumer's own subscription. The `mqtt_subscribe_acl` derives the
/// `MqttSubscribe` grant and covers the filter, so the subscription is deliverable.
#[tokio::test]
async fn build_messaging_accepts_wasm_mqtt_sub_with_covering_acl() {
    use brenn_lib::access::raw::MqttSubMatcherRaw;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_lib::messaging::{SubscriberEntryKind, mqtt_channel_uuid_from_address};
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use indexmap::IndexMap as IM;

    let address = "mqtt:home:sensors/temp";
    // The ingress channel derived from this WASM consumer's subscription.
    let ingress = ResolvedMqttIngressChannel {
        channel_address: address.to_string(),
        channel_uuid: mqtt_channel_uuid_from_address(address),
        client_slug: "home".to_string(),
        topic: "sensors/temp".to_string(),
        qos: 1,
        urgency: brenn_lib::messaging::Urgency::Normal,
    };

    let mut config = brenn_lib::config::BrennConfig {
        mqtt_clients: vec![
            toml::from_str("slug = \"home\"\nurl = \"mqtts://127.0.0.1:1\"")
                .expect("minimal raw client config parses"),
        ],
        ..brenn_lib::config::BrennConfig::default()
    };
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "consume-mqtt".to_string(),
        component_path: "/tmp/consume-mqtt.wasm".into(),
        grants: vec![],
        subscribe_acl: vec![],
        publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![MqttSubMatcherRaw {
            client: "home".to_string(),
            topic_filter: "sensors/temp".to_string(),
        }],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: address.to_string(),
            port: "in".to_string(),
            push_depth: Some(Depth::Bounded(10)),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        std::slice::from_ref(&ingress),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger = result
        .messenger
        .expect("a WASM mqtt subscription with a covering mqtt_subscribe_acl must boot");
    let channel = messenger
        .directory()
        .by_uuid(&mqtt_channel_uuid_from_address(address))
        .expect("mqtt: channel must be derived into the directory");
    assert!(
        channel.subscribers.iter().any(|s| matches!(
            &s.kind,
            SubscriberEntryKind::Wasm(slug) if slug == "consume-mqtt"
        )),
        "the WASM consumer must be attached as a subscriber on the derived mqtt: channel"
    );
}

/// The same WASM `mqtt:` subscription WITHOUT a covering `mqtt_subscribe_acl`
/// (empty list ⇒ no `MqttSubscribe` grant derived) is a dead subscription —
/// boot must refuse to start. The MQTT path differs materially from the webhook
/// negative: the `mqtt:` ingress channel is derived from the subscription itself
/// regardless of ACL, so the channel resolves and the `Wasm` subscriber attaches,
/// and only then does `validate_static_subscriptions_deliverable` (via the mqtt
/// delivery gate) fire. A regression that derived `MqttSubscribe` unconditionally
/// or bypassed the mqtt gate at boot would leak an uncovered subscription past
/// boot validation; this pins that closed.
#[tokio::test]
#[should_panic(expected = "can never deliver on")]
async fn build_messaging_panics_on_wasm_mqtt_sub_without_covering_acl() {
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_lib::messaging::mqtt_channel_uuid_from_address;
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use indexmap::IndexMap as IM;

    let address = "mqtt:home:sensors/temp";
    // The ingress channel is still derived from the subscription, so the
    // channel resolves and the subscriber attaches — the empty mqtt_subscribe_acl
    // is what makes delivery unauthorized.
    let ingress = ResolvedMqttIngressChannel {
        channel_address: address.to_string(),
        channel_uuid: mqtt_channel_uuid_from_address(address),
        client_slug: "home".to_string(),
        topic: "sensors/temp".to_string(),
        qos: 1,
        urgency: brenn_lib::messaging::Urgency::Normal,
    };

    let mut config = brenn_lib::config::BrennConfig {
        mqtt_clients: vec![
            toml::from_str("slug = \"home\"\nurl = \"mqtts://127.0.0.1:1\"")
                .expect("minimal raw client config parses"),
        ],
        ..brenn_lib::config::BrennConfig::default()
    };
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "consume-mqtt".to_string(),
        component_path: "/tmp/consume-mqtt.wasm".into(),
        grants: vec![],
        subscribe_acl: vec![],
        publish_acl: vec![],
        mqtt_publish_acl: vec![],
        // Empty mqtt_subscribe_acl ⇒ no MqttSubscribe grant ⇒ delivery denied.
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: address.to_string(),
            port: "in".to_string(),
            push_depth: Some(Depth::Bounded(10)),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        config: None,
        activation_burst: None,
        activation_min_period_ms: None,
        mqtt_outputs: vec![],
        tool_grants: vec![],
    }];

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let _ = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        std::slice::from_ref(&ingress),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// A config carrying only `[[surface]]` + `[[ephemeral_channel]]` blocks
/// (no `[[channel]]`, webhook, mqtt-ingress, or `[[wasm_consumer]]`) must
/// still bring messaging up and carry both resolved lists — exercises the
/// `messaging_configured` gate end-to-end (paired with `run_server`'s
/// `any_messaging`), which every `resolve_surfaces`-direct test bypasses.
/// Mirrors `build_messaging_derives_mqtt_channel_entry`.
#[tokio::test]
async fn build_messaging_brings_up_surface_and_ephemeral_only_config() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::config::BrennConfig;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{EphemeralChannelConfigRaw, SurfaceConfigRaw, SurfaceGrant};
    use indexmap::IndexMap as IM;

    let config = BrennConfig {
        ephemeral_channels: vec![EphemeralChannelConfigRaw {
            name: "protobar-demo".to_string(),
            push_depth: None,
            retain_depth: None,
            noise: None,
            capacity: None,
        }],
        surfaces: vec![SurfaceConfigRaw {
            grants: vec![SurfaceGrant::EphemeralSubscribe],
            ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact("protobar-demo".to_string())],
            // Stock global defaults leave `default_push_depth` unbounded, which
            // cannot be a page queue, so a surface binding states its own depth.
            subscriptions: vec![brenn_lib::messaging::config::SurfaceSubscriptionRaw {
                push_depth: Some(brenn_lib::messaging::config::Depth::Bounded(8)),
                ..surface_sub_raw("ephemeral:protobar-demo", "protobar", "messages")
            }],
            ..minimal_surface_raw()
        }],
        ..BrennConfig::default()
    };

    let db = init_db_memory();
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        db,
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    assert!(
        result.messenger.is_some(),
        "a surface/ephemeral-only config must bring messaging up"
    );
    assert_eq!(result.ephemeral_channels.len(), 1);
    assert_eq!(result.ephemeral_channels[0].name, "protobar-demo");
    assert_eq!(result.surfaces.len(), 1);
    assert_eq!(result.surfaces[0].slug, "deskbar");
    assert_eq!(result.surfaces[0].subscriptions.len(), 1);
    assert_eq!(
        result.surfaces[0].subscriptions[0].channel_address,
        "ephemeral:protobar-demo"
    );

    // Prove the config-resolved channel is actually wired into the
    // Messenger's `EphemeralBus` — not just present in the intermediate
    // `ephemeral_channels` vec. A publish must resolve the channel; the
    // empty `Messenger::new` default bus would return `UnknownChannel`.
    use brenn_lib::access::acl::ChannelMatcher;
    use brenn_lib::access::{AppCapability, AppPolicy};
    use brenn_lib::messaging::{EphemeralPublishResult, ParticipantId, Urgency};

    let bus = result.messenger.as_ref().unwrap().ephemeral_bus();
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::EphemeralPublish);
    policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact("protobar-demo".to_string())];
    let sender = ParticipantId::for_app("deskbar", "brenn://test");
    let outcome = bus.publish(
        &sender,
        &policy,
        "ephemeral:protobar-demo",
        "hi",
        Urgency::Normal,
    );
    assert!(
        matches!(outcome, EphemeralPublishResult::Ok { .. }),
        "config-resolved ephemeral channel must be wired into the Messenger's \
             bus; got {outcome:?}"
    );
    assert_eq!(bus.publish_count("protobar-demo"), 1);
}

/// `messaging_configured` must fire on a config whose only messaging content
/// is one `[[wasm_consumer]]`, and stay false on a fully default config.
#[test]
fn messaging_configured_covers_wasm_consumer_only() {
    use brenn_lib::config::BrennConfig;
    let empty_webhooks: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IndexMap::new();

    assert!(
        !messaging_configured(&BrennConfig::default(), &empty_webhooks, &[]),
        "a fully default config activates no messaging subsystem"
    );

    let config = BrennConfig {
        wasm_consumers: vec![minimal_wasm_consumer()],
        ..BrennConfig::default()
    };
    assert!(
        messaging_configured(&config, &empty_webhooks, &[]),
        "a wasm-consumer-only config must activate messaging"
    );
}

/// A wasm-consumer-only config must bring messaging up through
/// `build_messaging` when given a resolved `server_origin`. Mirrors
/// `build_messaging_brings_up_surface_and_ephemeral_only_config`.
#[tokio::test]
async fn build_messaging_brings_up_wasm_consumer_only_config() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::db::init_db_memory;
    use indexmap::IndexMap as IM;

    let config = BrennConfig {
        wasm_consumers: vec![minimal_wasm_consumer()],
        ..BrennConfig::default()
    };

    let db = init_db_memory();
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        db,
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    assert!(
        result.messenger.is_some(),
        "a wasm-consumer-only config must bring messaging up"
    );
    assert_eq!(result.wasm_consumers.len(), 1);
    assert_eq!(result.wasm_consumers[0].slug, "probe");
}

/// A registry holding one async tool `apull` (acl key `repo`), matching the
/// git-repo-pull shape without needing the real repo-sync handles.
fn async_tool_registry() -> std::sync::Arc<crate::tool_registry::ToolRegistry> {
    use crate::tool_registry::ToolError;
    use crate::tool_registry::descriptor::{AclDenied, Idempotency, ToolClass, ToolDescriptor};
    use crate::tool_registry::tool::{AsyncTool, RegisteredTool, ToolCtx};
    use serde_json::{Value, json};

    struct APull(ToolDescriptor);
    #[async_trait::async_trait]
    impl AsyncTool for APull {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.0
        }
        fn check_acl(
            &self,
            _a: &Value,
            _c: &[brenn_lib::tools::AclClause],
        ) -> Result<(), AclDenied> {
            Ok(())
        }
        async fn execute(&self, _c: &ToolCtx, _a: Value) -> Result<Value, ToolError> {
            Ok(json!({}))
        }
    }
    std::sync::Arc::new(crate::tool_registry::ToolRegistry::new(vec![
        RegisteredTool::Async(std::sync::Arc::new(APull(ToolDescriptor {
            name: "apull",
            mcp_name: "mcp__brenn__APull",
            description: "stub async",
            input_schema: json!({ "type": "object" }),
            class: ToolClass::Async { max_concurrency: 4 },
            acl_keys: &["repo"],
            idempotency: Idempotency::Natural,
            auto_approve: true,
        }))),
    ]))
}

/// A wasm consumer holding an async tool grant makes `build_messaging` derive the
/// full async bus wiring: the consumer's resolved policy gains
/// subscribe visibility of its own `brenn:tool-results/<slug>` inbox and publish
/// visibility of the `brenn:tools/<tool>` request channel. Critically, the build
/// *does not panic*: `validate_static_subscriptions_deliverable` sees the injected
/// `Wasm` inbox subscriber and would refuse to start unless the inbox channel was
/// created AND the derived subscribe grant covers it — so a clean build is proof
/// the channel, subscriber, and grant all line up.
#[tokio::test]
async fn build_messaging_wires_async_tool_bus_for_granted_consumer() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::db::init_db_memory;
    use indexmap::IndexMap as IM;

    let mut repo_clause = toml::Table::new();
    repo_clause.insert("repo".to_string(), toml::Value::String("brenn".to_string()));
    let mut consumer = minimal_wasm_consumer();
    consumer.tool_grants = vec![brenn_lib::tools::config::ToolGrantRaw {
        tool: "apull".to_string(),
        acl: vec![repo_clause],
        rate_limit: None,
    }];

    let config = BrennConfig {
        wasm_consumers: vec![consumer],
        ..BrennConfig::default()
    };
    let db = init_db_memory();
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        db,
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &async_tool_registry(),
    )
    .await;

    assert!(
        result.messenger.is_some(),
        "async-tool consumer config comes up"
    );
    let policy = &result.wasm_consumers[0].policy;
    assert!(
        policy.allows_channel_access("brenn:tool-results/probe"),
        "derived transport grant must authorize delivery of the consumer's own inbox"
    );
    assert!(
        policy.allows_brenn_publish("tools/apull"),
        "derived grant must give publish visibility of the request channel"
    );
    // A different consumer's inbox is not covered (the derivation is per-slug).
    assert!(!policy.allows_channel_access("brenn:tool-results/other"));
}

/// `build_messaging` registers the `system:surface-help` participant, and
/// `publish_description` writes documents
/// a *non-subscriber* can pull via `Messenger::query` — the ungated-read path the
/// feature relies on. A second boot-publish supersedes the first:
/// `standing_retain_depth = 1` clamps the non-subscriber read to only the newest
/// doc (latest-wins).
#[tokio::test]
async fn boot_description_publish_is_pullable_by_a_non_subscriber_latest_wins() {
    use crate::routes::surface::description::{
        SURFACE_HELP_COMPONENT, build_description_docs, publish_description,
    };
    use brenn_lib::config::BrennConfig;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::Urgency;
    use brenn_lib::messaging::config::{ChannelConfigRaw, Depth};
    use brenn_lib::messaging::publish::PublishResult;
    use brenn_lib::messaging::query::MessageQuery;
    use indexmap::IndexMap as IM;

    let retained_channel = |uuid: &str, address: &str| ChannelConfigRaw {
        uuid: uuid.to_string(),
        address: address.to_string(),
        description: None,
        push_depth: None,
        retain_depth: None,
        standing_retain_depth: Some(Depth::Bounded(1)),
        noise: None,
        sink: None,
        wake_min: None,
    };

    // No configured surfaces ⇒ derived boot-published set is just the index.
    // Declare it plus an unrelated "other" channel.
    let config = BrennConfig {
        channels: vec![
            retained_channel("33333333-3333-4333-8333-333333333333", "surface.index"),
            retained_channel("44444444-4444-4444-8444-444444444444", "other"),
        ],
        ..BrennConfig::default()
    };
    let db = init_db_memory();
    // The reader must hold covering channel access to pass the read gate; it is a
    // non-subscriber (no subscription config), which is what the clamp exercises.
    let mut reader = minimal_app_config("some-reader", None, vec![]);
    reader.policy =
        crate::test_support::app_config::delivery_policy_for_addresses(["brenn:surface.index"]);
    let mut apps_map: IM<String, AppConfig> = IM::new();
    apps_map.insert("some-reader".to_string(), reader);
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        db,
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger = result.messenger.as_ref().expect("messaging must be up");

    // Two boot-publishes, distinct build ids (no configured surfaces, no sidecar
    // reads — the dist path is unused).
    let docs1 = build_description_docs(
        "surface",
        "build-1",
        &result.surfaces,
        std::path::Path::new("/nonexistent"),
    );
    publish_description(messenger, &docs1).await;
    let docs2 = build_description_docs(
        "surface",
        "build-2",
        &result.surfaces,
        std::path::Path::new("/nonexistent"),
    );
    publish_description(messenger, &docs2).await;

    // A non-subscriber pull is clamped to standing_retain_depth (1) — the newest.
    let envelopes = messenger
        .query(&MessageQuery {
            channel: "brenn:surface.index".to_string(),
            limit: 10,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: "some-reader".to_string(),
        })
        .await
        .expect("index channel query succeeds");

    assert_eq!(
        envelopes.len(),
        1,
        "standing_retain_depth=1 clamps a non-subscriber read to the newest doc"
    );
    let env = &envelopes[0];
    assert_eq!(
        env.sender, "system:surface-help",
        "description envelope sender must be the system:surface-help participant, got {:?}",
        env.sender
    );
    assert!(
        env.body.contains("build-2"),
        "latest-wins: the newer boot's index doc is what a reader sees, got {:?}",
        env.body
    );

    // The publisher's brenn_publish ACL is exact-scoped to the derived channels:
    // publishing to an unrelated channel is AclDenied, not blanket-allowed.
    let denied = messenger
        .publish_from_system(
            SURFACE_HELP_COMPONENT,
            "brenn:other",
            "{}",
            Urgency::Normal,
            None,
        )
        .await;
    assert!(
        matches!(denied, PublishResult::AclDenied(..)),
        "surface-help ACL is exact-scoped to the derived channels; publishing elsewhere must be \
         denied; got {denied:?}"
    );
}

/// The boot `disconnected` stamp: `publish_boot_disconnected_stamps` writes
/// a `disconnected` (reason "server restart") status document to each configured
/// surface's derived status channel, under the surface's own identity via the
/// send-budget-exempt platform path, and a non-subscriber can pull it. Proves the
/// injected geometry/status grant covers the write and the retained row reads
/// "down", not a stale "healthy".
#[tokio::test]
async fn boot_disconnected_stamp_written_per_surface_and_pullable() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::db::init_db_memory;
    use brenn_lib::messaging::config::{ChannelConfigRaw, Depth, SurfaceConfigRaw};
    use brenn_lib::messaging::query::MessageQuery;
    use indexmap::IndexMap as IM;

    let bounded_channel = |uuid: &str, address: &str| ChannelConfigRaw {
        uuid: uuid.to_string(),
        address: address.to_string(),
        description: None,
        push_depth: None,
        retain_depth: Some(Depth::Bounded(1)),
        standing_retain_depth: Some(Depth::Bounded(1)),
        noise: None,
        sink: None,
        wake_min: None,
    };

    // One surface (`deskbar`). The status channel is the only derived channel
    // this test writes/reads, so it is the only one declared — `build_messaging`
    // injects the surface's geometry/status publish grant, and the boot-stamp
    // path publishes only to the status channel.
    let config = BrennConfig {
        channels: vec![bounded_channel(
            "77777777-7777-4777-8777-777777777777",
            "surface.surface.deskbar.status",
        )],
        surfaces: vec![SurfaceConfigRaw {
            ..minimal_surface_raw()
        }],
        ..BrennConfig::default()
    };
    let db = init_db_memory();
    // A non-subscriber reader with covering read access to the status channel.
    let mut reader = minimal_app_config("some-reader", None, vec![]);
    reader.policy = crate::test_support::app_config::delivery_policy_for_addresses([
        "brenn:surface.surface.deskbar.status",
    ]);
    let mut apps_map: IM<String, AppConfig> = IM::new();
    apps_map.insert("some-reader".to_string(), reader);
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    let result = build_messaging(
        &config,
        db,
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger = result.messenger.as_ref().expect("messaging must be up");
    let epoch = messenger.ephemeral_bus().epoch();

    crate::routes::surface::telemetry::publish_boot_disconnected_stamps(
        messenger,
        "surface",
        &result.surfaces,
        epoch,
    )
    .await;

    let envelopes = messenger
        .query(&MessageQuery {
            channel: "brenn:surface.surface.deskbar.status".to_string(),
            limit: 10,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: "some-reader".to_string(),
        })
        .await
        .expect("status channel query succeeds");

    assert_eq!(envelopes.len(), 1, "one boot stamp per surface, retained");
    let env = &envelopes[0];
    assert_eq!(
        env.sender, "surface:deskbar",
        "the boot stamp is written under the surface's own identity, got {:?}",
        env.sender
    );
    let body: serde_json::Value = serde_json::from_str(&env.body).expect("stamp body is JSON");
    assert_eq!(body["health"], serde_json::json!("disconnected"));
    assert_eq!(body["reason"], serde_json::json!("server restart"));
    assert_eq!(body["session"], serde_json::json!(null));
    assert_eq!(body["instances"], serde_json::json!([]));
}

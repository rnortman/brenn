//! End-to-end tests for `build_messaging` and `build_apps_with_messaging`.

use super::test_fixtures::{
    boot_messaging_with, io_port_raw, local_sub_raw, minimal_app_config, minimal_surface_raw,
    minimal_wasm_consumer, resolved_ingress_sub, surface_sub_raw, tuning_for,
};
use super::*;
use brenn_lib::config::AppConfig;
use brenn_lib::messaging::ComponentGrant;
use brenn_lib::messaging::config::{Depth, MessagingGlobalConfig};
use brenn_lib::mqtt::config::MqttClientConfigRaw;
use brenn_lib::webhook::ResolvedWebhookSubscription;

/// An empty tool registry for `build_messaging` calls that do not exercise the
/// async tool substrate: with no async tools registered, no `brenn:tools/*`
/// channels or `system:tool-executor` policy are derived, so these tests observe
/// the pre-tool-substrate behavior unchanged.
fn empty_tool_registry() -> std::sync::Arc<brenn_tool_registry::ToolRegistry> {
    std::sync::Arc::new(brenn_tool_registry::ToolRegistry::new(vec![]))
}

/// A non-durable `[[channel]]` block at `depth` for both rungs. Standing is the
/// retained window on a non-durable channel, so it is left unstated. Tests use
/// it for the inert channel that brings messaging up when the channel under
/// test is not declared.
fn nondurable_channel(address: &str, depth: u64) -> brenn_lib::messaging::config::ChannelConfigRaw {
    brenn_lib::messaging::config::ChannelConfigRaw {
        send_rate: None,
        uuid: None,
        address: Some(address.to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(depth)),
        retain_depth: Some(Depth::Bounded(depth)),
        standing_retain_depth: None,
        noise: None,
        sink: None,
        wake_min: None,
    }
}

/// Boot `build_messaging` with the standard no-op periphery and no apps. Tests
/// that need a non-empty app map call `boot_messaging_with`; those that need
/// real webhooks or bridges call `build_messaging` directly.
async fn boot_messaging(
    config: &brenn_lib::config::BrennConfig,
    db: brenn_db::Db,
) -> MessagingResult {
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IndexMap::new());
    boot_messaging_with(config, db, &apps, alert_dispatcher, "brenn://test").await
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
            push_depth: Depth::Bounded(1),
            retain_depth: Depth::Bounded(8),
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
    // The block's own depths reach the directory subscriber, in the right
    // fields — distinct values so a swap is caught.
    assert_eq!(
        sub.push_depth,
        Depth::Bounded(1),
        "push_depth must be the [[app.webhook_subscription]] block's own"
    );
    assert_eq!(
        sub.retain_depth,
        Depth::Bounded(8),
        "retain_depth must be the [[app.webhook_subscription]] block's own"
    );
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

/// The Messenger holds the plain apps map, and the one thing it reads off an
/// app there is `messaging_send_budget()`. For a transport-only app — webhook
/// subscriptions, no `[app.messaging]` block — that number must be the same
/// whether the app carries a synthesised messaging block or none at all, because
/// `build_apps_with_messaging` synthesises the block from the same global
/// default that `config/resolve.rs` stamps on every app.
///
/// That equivalence is what lets the Messenger take the unmerged map. This pin
/// makes it a contract rather than a coincidence: it asserts both numbers and
/// asserts they agree.
#[test]
fn a_transport_only_apps_budget_survives_the_unmerged_map() {
    let slug = "phonebuddy";
    // Distinct from the type default, so a lost stamp cannot pass by coinciding
    // with it.
    let budget = MessagingGlobalConfig::default().default_send_budget + 7;
    let global = MessagingGlobalConfig {
        default_send_budget: budget,
        ..Default::default()
    };

    let mut app = minimal_app_config(
        slug,
        None,
        vec![ResolvedWebhookSubscription {
            endpoint_slug: "pb-events".to_string(),
            push_depth: Depth::Bounded(1),
            retain_depth: Depth::Bounded(8),
            wake_min: brenn_lib::messaging::WakeMin::Normal,
        }],
    );
    // The stamp `config/resolve.rs` puts on every app, from the same global the
    // synthesised block below is built from.
    app.messaging_default_send_budget = budget;
    app.policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::MessagingPublish);
    let mut apps: IndexMap<String, AppConfig> = IndexMap::new();
    apps.insert(slug.to_string(), app);

    // The block the app would have carried had it been merged.
    let apps_with_messaging = build_apps_with_messaging(&apps, &global);
    let (_, synthesised) = apps_with_messaging
        .iter()
        .find(|(s, _)| s == slug)
        .expect("a webhook-subscribed app is messaging-enabled");

    // The map the Messenger actually receives, read the way it reads it.
    let resolved = brenn_lib::messaging::gates::resolve_publish_sender(
        &apps,
        slug,
        brenn_envelope::grants::AppCapability::MessagingPublish,
    )
    .expect("a granted app resolves as a publish sender");

    assert_eq!(
        resolved.messaging_send_budget(),
        budget,
        "the unmerged map must report the stamped global default"
    );
    assert_eq!(
        resolved.messaging_send_budget(),
        synthesised.send_budget,
        "merged and unmerged must report the same budget — the equivalence the \
         Messenger's plain apps map rests on"
    );
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
    use brenn_lib::messaging::mqtt_channel_uuid_from_address;
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use brenn_server::test_support::init_db_memory;
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::mqtt_channel_uuid_from_address;
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use brenn_server::test_support::init_db_memory;
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
                send_rate: Default::default(),
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
        brenn_messaging_store::db::upsert_channels(&conn, std::slice::from_ref(&channel_entry));
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
    graf_app.policy = brenn_lib::access::test_fixtures::delivery_policy_for_addresses([address]);
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
        &tuning_for(&config),
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
        let rows = brenn_messaging_store::db::load_dynamic_subscriptions(&conn);
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

/// A durable dynamic subscription on another conversation's chat record — the
/// peer cross-subscription pattern — survives a restart.
///
/// A chat channel is runtime-created, `brenn:`, and declared by no `[[channel]]`
/// block, so nothing reconstructs it from its row: its depths come from chat
/// provisioning. What puts it back in the boot directory is
/// `backfill_conversation_chat_channels`, and the dynamic merge has to run after
/// that — under the wrong order the chat rows reach the merge only through the
/// skip report, which holds the subscription dormant instead of folding it live,
/// so the peer's cross-subscription is silently dead on every reboot.
#[tokio::test]
async fn build_messaging_keeps_a_dynamic_subscription_on_a_chat_channel() {
    use brenn_envelope::chat::{ChatLeaf, chat_address};
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::chat_channel_uuid_from_address;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    // One unrelated channel brings messaging up; the chat family is derived
    // from the conversation row, never declared.
    let config = BrennConfig {
        channels: vec![nondurable_channel("ephemeral:inert", 1)],
        ..BrennConfig::default()
    };
    let owner = "cchost";
    let peer = "cbpeer";
    let conversation_id = 1;
    let record = chat_address(
        &config.llm_chat.prefix,
        owner,
        ChatLeaf::Out,
        conversation_id,
    );
    let record_uuid = chat_channel_uuid_from_address(&record);

    let db = init_db_memory();
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'alice', 'h', '2024-01-01')",
            [],
        )
        .expect("seed user");
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (?1, 1, 'active', ?2, '2024-01-01', '2024-01-01')",
            rusqlite::params![conversation_id, owner],
        )
        .expect("seed conversation");
        // The record channel's row as a previous boot's provisioning left it.
        // Depths are not persisted, so what they say here is irrelevant — the
        // row exists so the dynamic subscription's FK resolves.
        let entry = brenn_lib::messaging::ChannelEntry {
            uuid: record_uuid,
            address: record.clone(),
            description: None,
            transport_type: ChannelScheme::Brenn,
            resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Bounded(8),
                retain_depth: Depth::Bounded(8),
                standing_retain_depth: Depth::Bounded(8),
                noise: NoiseLevel::Silent,
                sink: brenn_lib::messaging::config::Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: Vec::new(),
            mount: None,
        };
        brenn_messaging_store::db::upsert_channels(&conn, std::slice::from_ref(&entry));
        conn.execute(
            "INSERT INTO messaging_dynamic_subscriptions \
             (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, qos, created_at) \
             VALUES (?1, ?2, '8', '0', 'silent', 'normal', NULL, '2026-06-20T00:00:00Z')",
            rusqlite::params![record_uuid.as_bytes().to_vec(), peer],
        )
        .expect("seed the peer's cross-subscription");
    }

    let mut apps_map: IM<String, AppConfig> = IM::new();
    apps_map.insert(owner.to_string(), minimal_app_config(owner, None, vec![]));
    let mut peer_app = minimal_app_config(peer, None, vec![]);
    peer_app.policy =
        brenn_lib::access::test_fixtures::delivery_policy_for_addresses([record.as_str()]);
    apps_map.insert(peer.to_string(), peer_app);
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();

    let result = boot_messaging_with(&config, db.clone(), &apps, alert_dispatcher, "brenn://test")
        .await
        .messenger
        .expect("a configured app brings messaging up");

    let channel = result
        .directory()
        .by_uuid(&record_uuid)
        .expect("the backfill must put the conversation's record channel in the directory");
    assert_eq!(channel.address, record);
    assert!(
        channel.app_subscriber(peer).is_some(),
        "the peer's dynamic subscription must be folded back onto the channel"
    );

    let conn = db.lock().await;
    let rows = brenn_messaging_store::db::load_dynamic_subscriptions(&conn);
    assert_eq!(
        rows.len(),
        1,
        "the durable cross-subscription row must survive the restart, not be pruned"
    );
    assert_eq!(rows[0].app_slug, peer);
}

/// Removing a `[[channel]]` block does not destroy the durable dynamic
/// subscriptions on that channel. The channel's row survives, so the boot that
/// cannot see the block holds the subscription dormant — unfolded, unpruned, its
/// cursor position untouched — and the boot that restores the block folds it
/// back onto exactly that position. Commenting a block out to debug is a routine
/// edit on a single-operator system; it must be revertible.
#[tokio::test]
async fn build_messaging_holds_a_dynamic_subscription_dormant_while_its_block_is_gone() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::ParticipantId;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let app_slug = "graf";
    let channel_uuid = uuid::uuid!("2c9d7f11-84a2-4f0e-9b31-6f0c5a1d7e42");
    let address = brenn_lib::messaging::canonical_address("declared");
    let conversation_id = 1;

    let declared_block = brenn_lib::messaging::config::ChannelConfigRaw {
        send_rate: None,
        uuid: Some(channel_uuid.to_string()),
        address: Some("declared".to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(2)),
        retain_depth: Some(Depth::Bounded(4)),
        standing_retain_depth: Some(Depth::Bounded(4)),
        noise: None,
        sink: None,
        wake_min: None,
    };
    // The block that keeps messaging up when the declared one is commented out.
    let inert_block = nondurable_channel("ephemeral:inert", 1);
    let with_block = BrennConfig {
        channels: vec![declared_block, inert_block.clone()],
        ..BrennConfig::default()
    };
    let without_block = BrennConfig {
        channels: vec![inert_block],
        ..BrennConfig::default()
    };

    let db = init_db_memory();
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'alice', 'h', '2024-01-01')",
            [],
        )
        .expect("seed user");
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (?1, 1, 'active', ?2, '2024-01-01', '2024-01-01')",
            rusqlite::params![conversation_id, app_slug],
        )
        .expect("seed conversation");
    }

    let mut apps_map: IM<String, AppConfig> = IM::new();
    let mut app = minimal_app_config(app_slug, None, vec![]);
    app.policy =
        brenn_lib::access::test_fixtures::delivery_policy_for_addresses([address.as_str()]);
    apps_map.insert(app_slug.to_string(), app);
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);

    let boot = async |config: &BrennConfig, db: brenn_db::Db| {
        let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
        boot_messaging_with(config, db, &apps, alert_dispatcher, "brenn://test")
            .await
            .messenger
            .expect("a configured app brings messaging up")
    };

    // Boot 1: the block is there. The channel must hold messages with the
    // position strictly inside them: on an empty channel a preserved position
    // and one re-primed at head are the same number, and the assertions would
    // pin nothing.
    let first = boot(&with_block, db.clone()).await;
    warm_channel_with_messages(&db, channel_uuid, 5).await;
    let position = ParticipantId::for_conversation(conversation_id);
    let seeded_seq = 3;
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO messaging_dynamic_subscriptions \
             (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, qos, created_at) \
             VALUES (?1, ?2, '2', '4', 'silent', 'normal', NULL, '2026-06-20T00:00:00Z')",
            rusqlite::params![channel_uuid.as_bytes().to_vec(), app_slug],
        )
        .expect("seed the dynamic subscription");
        brenn_messaging_store::db::ensure_subscriber_cursor(
            &conn,
            channel_uuid,
            &position,
            app_slug,
            Depth::Bounded(2),
            seeded_seq,
        );
    }
    drop(first);

    // Boot 2: the block is commented out. The channel row is still there, so the
    // subscription is dormant, not drift.
    let second = boot(&without_block, db.clone()).await;
    assert!(
        second.directory().by_uuid(&channel_uuid).is_none(),
        "an undeclared channel is not conjured back into the directory",
    );
    {
        let conn = db.lock().await;
        let rows = brenn_messaging_store::db::load_dynamic_subscriptions(&conn);
        assert_eq!(rows.len(), 1, "the durable row must not be pruned");
        assert_eq!(rows[0].app_slug, app_slug);
        let cursor =
            brenn_messaging_store::db::load_subscriber_cursor(&conn, channel_uuid, &position)
                .expect("the dormant registration justifies the position");
        assert_eq!(
            cursor.next_owed_seq, seeded_seq,
            "a dormant boot must leave the position where it stood, not reset it"
        );
    }
    drop(second);

    // Boot 3: the block is back. The subscription folds again, onto the position
    // the dormant boots preserved.
    let third = boot(&with_block, db.clone()).await;
    let channel = third
        .directory()
        .by_uuid(&channel_uuid)
        .expect("the restored block redeclares the channel");
    assert!(
        channel.app_subscriber(app_slug).is_some(),
        "the dormant subscription folds back once its channel is declared",
    );
    let conn = db.lock().await;
    let cursor = brenn_messaging_store::db::load_subscriber_cursor(&conn, channel_uuid, &position)
        .expect("the position survived both boots");
    assert_eq!(
        cursor.next_owed_seq, seeded_seq,
        "the redeclaring boot folds the subscription back onto exactly that position"
    );
}

/// Deleting the channel's row from the database is the operator's retirement
/// path, and it is the case that still counts as drift: with no row to hold the
/// subscription dormant against, the next boot prunes the durable row instead of
/// warning about it forever. Pinned end to end, because the merge's `dropped`
/// condition and the prune that acts on it meet only in the boot path.
#[tokio::test]
async fn build_messaging_prunes_a_dynamic_subscription_when_the_channel_row_is_deleted() {
    use brenn_lib::config::BrennConfig;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let app_slug = "graf";
    let channel_uuid = uuid::uuid!("6b1f0a54-2f47-4a6c-8a1d-9e3b5c7d2f80");
    let address = brenn_lib::messaging::canonical_address("retired");

    let declared_block = brenn_lib::messaging::config::ChannelConfigRaw {
        send_rate: None,
        uuid: Some(channel_uuid.to_string()),
        address: Some("retired".to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(2)),
        retain_depth: Some(Depth::Bounded(4)),
        standing_retain_depth: Some(Depth::Bounded(4)),
        noise: None,
        sink: None,
        wake_min: None,
    };
    let inert_block = nondurable_channel("ephemeral:inert", 1);
    let with_block = BrennConfig {
        channels: vec![declared_block, inert_block.clone()],
        ..BrennConfig::default()
    };
    let without_block = BrennConfig {
        channels: vec![inert_block],
        ..BrennConfig::default()
    };

    let mut apps_map: IM<String, AppConfig> = IM::new();
    let mut app = minimal_app_config(app_slug, None, vec![]);
    app.policy =
        brenn_lib::access::test_fixtures::delivery_policy_for_addresses([address.as_str()]);
    apps_map.insert(app_slug.to_string(), app);
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);

    let boot = async |config: &BrennConfig, db: brenn_db::Db| {
        let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
        boot_messaging_with(config, db, &apps, alert_dispatcher, "brenn://test")
            .await
            .messenger
            .expect("a configured app brings messaging up")
    };

    let db = init_db_memory();
    let first = boot(&with_block, db.clone()).await;
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO messaging_dynamic_subscriptions \
             (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min, qos, created_at) \
             VALUES (?1, ?2, '2', '4', 'silent', 'normal', NULL, '2026-06-20T00:00:00Z')",
            rusqlite::params![channel_uuid.as_bytes().to_vec(), app_slug],
        )
        .expect("seed the dynamic subscription");
        // The retirement is an out-of-band edit, made with whatever tool the
        // operator has to hand — the sqlite shell leaves foreign keys off by
        // default, which is what lets the channel row go while the rows that
        // reference it stay. Modelled literally, since the whole point of the
        // case is a subscription row outliving its channel.
        conn.pragma_update(None, "foreign_keys", "OFF")
            .expect("relax foreign keys for the operator's edit");
        conn.execute(
            "DELETE FROM messaging_channels WHERE uuid = ?1",
            rusqlite::params![channel_uuid.as_bytes().to_vec()],
        )
        .expect("retire the channel row");
        conn.pragma_update(None, "foreign_keys", "ON")
            .expect("restore foreign keys");
    }
    drop(first);

    let second = boot(&without_block, db.clone()).await;
    {
        let conn = db.lock().await;
        assert!(
            brenn_messaging_store::db::load_dynamic_subscriptions(&conn).is_empty(),
            "a subscription whose channel row is gone is drift and must be pruned",
        );
    }
    drop(second);
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
    use brenn_lib::messaging::mqtt_channel_uuid_from_address;
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use brenn_server::test_support::init_db_memory;
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
                send_rate: Default::default(),
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
        brenn_messaging_store::db::upsert_channels(&conn, std::slice::from_ref(&orphan_entry));
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::{SubscriberEntryKind, mqtt_channel_uuid_from_address};
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use brenn_server::test_support::init_db_memory;
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
                send_rate: Default::default(),
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
        brenn_messaging_store::db::upsert_channels(&conn, std::slice::from_ref(&channel_entry));
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
        &tuning_for(&config),
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
        let rows = brenn_messaging_store::db::load_dynamic_subscriptions(&conn);
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
        graf_app.policy =
            brenn_lib::access::test_fixtures::delivery_policy_for_addresses([address]);
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
        &tuning_for(&config),
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
            send_rate: None,
            uuid: Some(uuid.to_string()),
            address: Some(address.to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(brenn_lib::messaging::config::Depth::Unbounded),
            retain_depth: Some(brenn_lib::messaging::config::Depth::Unbounded),
            standing_retain_depth: Some(brenn_lib::messaging::config::Depth::Unbounded),
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
    use brenn_server::test_support::init_db_memory;
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
        &tuning_for(&config),
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
    use brenn_server::test_support::init_db_memory;
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
            brenn_lib::access::test_fixtures::delivery_policy_for_addresses([
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let address = "boot-acl-wasm";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(address);
    // A consumer with `ports` granted (so it can output) but an EMPTY
    // `subscribe_acl`: build_wasm_policy derives no MessagingSubscribe grant and
    // no covering matcher, so allows_channel_access("brenn:boot-acl-wasm") is false.
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "deadwasm".to_string(),
        package: "deadwasm".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
        grants: vec![],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some(format!("brenn:{address}")),
            port: "in".to_string(),
            push_depth: Some(Depth::Unbounded),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        io_ports: vec![],
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let address = "boot-mqtt-matcher-undeclared";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(address);
    // No `[[mqtt_client]]` is declared, but the matcher names client `home`.
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "undeclared".to_string(),
        package: "undeclared".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
        grants: vec![ComponentGrant::Mqtt],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![MqttClientMatcherRaw {
            client: "home".to_string(),
        }],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![],
        outputs: vec![],
        io_ports: vec![],
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let address = "boot-mqtt-acl-nogrant";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(address);
    // Declare the `home` client so the matcher⇒declared-client check (2d) passes
    // and this exercises the matcher⇒grant check (2f) in isolation.
    config.mqtt_clients = vec![MqttClientConfigRaw::minimal("home", "mqtts://127.0.0.1:1")];
    // Authors an mqtt_publish ACL matcher but `grants` is empty (no `mqtt`).
    // Without the grant the matcher can never authorize a publish — dead config.
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "aclless".to_string(),
        package: "aclless".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
        grants: vec![],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![MqttClientMatcherRaw {
            client: "home".to_string(),
        }],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![],
        outputs: vec![],
        io_ports: vec![],
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    // The consumer subscribes to `brenn:secret-channel`, but its subscribe_acl
    // only covers `brenn:allowed-channel`. The grant is derived (non-empty ACL),
    // yet the subscribed channel is outside the matcher set.
    let subscribed = "secret-channel";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(subscribed);
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "scoped-wasm".to_string(),
        package: "scoped-wasm".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
        grants: vec![],
        // Non-empty ⇒ MessagingSubscribe grant is derived, but the matcher names
        // a different channel, so allows_channel_access("brenn:secret-channel") is false.
        subscribe_acl: vec![ChannelMatcherRaw::Exact("allowed-channel".to_string())],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some(format!("brenn:{subscribed}")),
            port: "in".to_string(),
            push_depth: Some(Depth::Unbounded),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        io_ports: vec![],
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    // The consumer subscribes to `brenn:covered-channel` and its subscribe_acl
    // covers exactly that channel, so build_wasm_policy derives the
    // MessagingSubscribe grant AND a covering matcher: allows_channel_access is true.
    let subscribed = "covered-channel";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(subscribed);
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "covered-wasm".to_string(),
        package: "covered-wasm".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
        grants: vec![],
        subscribe_acl: vec![ChannelMatcherRaw::Exact(subscribed.to_string())],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some(format!("brenn:{subscribed}")),
            port: "in".to_string(),
            push_depth: Some(Depth::Unbounded),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        io_ports: vec![],
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let address = "boot-mqtt-sub-matcher-undeclared";
    let (mut config, _channel_uuid) = config_with_one_brenn_channel(address);
    // No `[[mqtt_client]]` is declared, but the matcher names client `home`.
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "undeclared-sub".to_string(),
        package: "undeclared-sub".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
        grants: vec![],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
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
        io_ports: vec![],
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_lib::messaging::{SubscriberEntryKind, webhook_channel_uuid_from_slug};
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let endpoint_slug = "push-alice";
    // The bound output resolves against this brenn: channel; publish_acl covers it.
    let (mut config, _uuid) = config_with_one_brenn_channel("wasm-demo-out");
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "consume-demo-alice".to_string(),
        package: "processor-demo".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec!["out".to_string()],
        grants: vec![ComponentGrant::Ports],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![brenn_lib::access::raw::ChannelMatcherRaw::Exact(
            "wasm-demo-out".to_string(),
        )],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![brenn_lib::access::raw::WebhookMatcherRaw {
            endpoint: endpoint_slug.to_string(),
        }],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some(format!("webhook:{endpoint_slug}")),
            port: "in".to_string(),
            push_depth: Some(Depth::Bounded(50)),
            retain_depth: Some(Depth::Bounded(10)),
            noise: Some(NoiseLevel::Alarm),
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![brenn_lib::messaging::config::WasmConsumerOutputRaw {
            port: "out".to_string(),
            channel: Some("brenn:wasm-demo-out".to_string()),
            urgency: None,
            publish_per_activation: None,
            publish_capacity: None,
        }],
        io_ports: vec![],
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let endpoint_slug = "push-alice";
    let (mut config, _uuid) = config_with_one_brenn_channel("unused");
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "consume-demo-alice".to_string(),
        package: "processor-demo".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
        grants: vec![],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![],
        // Empty webhook_acl ⇒ no Webhook grant ⇒ allows_webhook_delivery is false.
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some(format!("webhook:{endpoint_slug}")),
            port: "in".to_string(),
            push_depth: Some(Depth::Bounded(8)),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        io_ports: vec![],
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_lib::messaging::{SubscriberEntryKind, mqtt_channel_uuid_from_address};
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use brenn_server::test_support::init_db_memory;
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
        mqtt_clients: vec![MqttClientConfigRaw::minimal("home", "mqtts://127.0.0.1:1")],
        ..brenn_lib::config::BrennConfig::default()
    };
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "consume-mqtt".to_string(),
        package: "consume-mqtt".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
        grants: vec![],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![],
        mqtt_subscribe_acl: vec![MqttSubMatcherRaw {
            client: "home".to_string(),
            topic_filter: "sensors/temp".to_string(),
        }],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some(address.to_string()),
            port: "in".to_string(),
            push_depth: Some(Depth::Bounded(10)),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        io_ports: vec![],
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw};
    use brenn_lib::messaging::mqtt_channel_uuid_from_address;
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use brenn_server::test_support::init_db_memory;
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
        mqtt_clients: vec![MqttClientConfigRaw::minimal("home", "mqtts://127.0.0.1:1")],
        ..brenn_lib::config::BrennConfig::default()
    };
    config.wasm_consumers = vec![WasmConsumerConfigRaw {
        slug: "consume-mqtt".to_string(),
        package: "consume-mqtt".to_string(),
        spec_sha256: String::new(),
        declared_out_ports: vec![],
        grants: vec![],
        subscribe_acl: vec![],
        ephemeral_subscribe_acl: vec![],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        local_subscribe_acl: vec![],
        local_publish_acl: vec![],
        mqtt_publish_acl: vec![],
        // Empty mqtt_subscribe_acl ⇒ no MqttSubscribe grant ⇒ delivery denied.
        mqtt_subscribe_acl: vec![],
        webhook_acl: vec![],
        store_path: None,
        store_size_limit: None,
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some(address.to_string()),
            port: "in".to_string(),
            push_depth: Some(Depth::Bounded(10)),
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        outputs: vec![],
        io_ports: vec![],
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
        &tuning_for(&config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// A config carrying only `[[surface]]` + an `ephemeral:` `[[channel]]`
/// (no durable channel, webhook, mqtt-ingress, or `[[wasm_consumer]]`) must
/// still bring messaging up and carry both resolved lists — exercises the
/// `messaging_configured` gate end-to-end (paired with `run_server`'s
/// `any_messaging`), which every `resolve_surfaces`-direct test bypasses.
/// Mirrors `build_messaging_derives_mqtt_channel_entry`.
#[tokio::test]
async fn build_messaging_brings_up_surface_and_ephemeral_only_config() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::AttachGrant;
    use brenn_lib::messaging::config::{ChannelConfigRaw, SurfaceConfigRaw};
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let config = BrennConfig {
        channels: vec![ChannelConfigRaw {
            send_rate: None,
            uuid: None,
            address: Some("ephemeral:protobar-demo".to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(brenn_lib::messaging::config::Depth::Bounded(1)),
            // A surface-bound channel must retain something: the subscription's
            // position is recovered by re-reading retention. On a non-durable
            // channel the retained window is also the standing buffer, so it is
            // the ceiling every binding's depth sits under.
            retain_depth: Some(brenn_lib::messaging::config::Depth::Bounded(8)),
            standing_retain_depth: None,
            noise: None,
            sink: None,
            wake_min: None,
        }],
        surfaces: vec![SurfaceConfigRaw {
            grants: vec![AttachGrant::EphemeralSubscribe],
            ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact("protobar-demo".to_string())],
            // The channel rung here is 1, which is not the page queue this
            // binding wants, so it states its own depth.
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
        &tuning_for(&config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    assert!(
        result.messenger.is_some(),
        "a surface/ephemeral-only config must bring messaging up"
    );
    assert_eq!(result.nondurable_channels.len(), 1);
    assert_eq!(
        result.nondurable_channels[0].address,
        "ephemeral:protobar-demo"
    );
    assert_eq!(result.surfaces.len(), 1);
    assert_eq!(result.surfaces[0].slug, "deskbar");
    assert_eq!(result.surfaces[0].subscriptions.len(), 1);
    assert_eq!(
        result.surfaces[0].subscriptions[0].channel_address,
        "ephemeral:protobar-demo"
    );

    // Prove the config-resolved channel is actually wired into the Messenger's
    // store registry — not just present in the intermediate
    // `nondurable_channels` vec. The empty `Messenger::new` default registry
    // carries no channel at all, so a resolvable store here is the wiring.
    let messenger = result.messenger.as_ref().unwrap();
    let store = messenger
        .ring_stores()
        .get_by_address("ephemeral:protobar-demo")
        .expect("config-resolved ephemeral channel must be wired into the Messenger");
    assert_eq!(store.address(), "ephemeral:protobar-demo");
    assert_eq!(store.epoch(), messenger.ring_epoch());
}

/// Every non-durable `[[channel]]` block joins the one directory and gets one
/// retention store. A `local:` channel additionally gets no wire fan-out: it is
/// confined to this process, so its store issues no live handle.
#[tokio::test]
async fn build_messaging_registers_nondurable_channels_with_stores() {
    use brenn_lib::config::BrennConfig;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let config = BrennConfig {
        channels: vec![
            nondurable_channel("ephemeral:wired", 4),
            nondurable_channel("local:inert", 4),
        ],
        ..BrennConfig::default()
    };

    let db = init_db_memory();
    let db_probe = db.clone();
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
        &tuning_for(&config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    {
        let conn = db_probe.lock().await;
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_channels WHERE address LIKE 'ephemeral:%' \
                 OR address LIKE 'local:%'",
                [],
                |r| r.get(0),
            )
            .expect("count non-durable channel rows");
        assert_eq!(rows, 0, "a non-durable channel is never persisted");
    }

    let addresses: Vec<&str> = result
        .nondurable_channels
        .iter()
        .map(|e| e.address.as_str())
        .collect();
    assert_eq!(addresses, vec!["ephemeral:wired", "local:inert"]);

    let messenger = result.messenger.as_ref().unwrap();
    assert!(
        messenger
            .ring_stores()
            .get_by_address("ephemeral:wired")
            .expect("the ephemeral channel has a store")
            .capabilities()
            .transportable,
        "an ephemeral channel carries a live fan-out"
    );
    assert!(
        !messenger
            .ring_stores()
            .get_by_address("local:inert")
            .expect("the local channel has a store")
            .capabilities()
            .transportable,
        "a local: channel is confined to this process, so it issues no live handle"
    );

    for address in ["ephemeral:wired", "local:inert"] {
        let entry = messenger
            .directory()
            .resolve(address)
            .unwrap_or_else(|| panic!("{address} must be registered in the one directory"));
        let store = messenger.store_for(&entry);
        assert_eq!(store.address(), address);
        assert!(!store.capabilities().durable);
        assert_eq!(
            store.channel_uuid(),
            entry.uuid,
            "the directory entry and its store name the same channel"
        );
    }
    for address in ["ephemeral:wired", "local:inert"] {
        assert_eq!(
            messenger
                .ring_stores()
                .get_by_address(address)
                .expect("registered")
                .epoch(),
            messenger.ring_epoch(),
            "every non-durable channel of one process shares one incarnation"
        );
    }

    // The non-durable channels are directory members, not database rows.
    let durable: Vec<String> = messenger
        .directory()
        .list_durable()
        .iter()
        .map(|e| e.address.clone())
        .collect();
    assert!(
        !durable
            .iter()
            .any(|a| a.starts_with("ephemeral:") || a.starts_with("local:")),
        "list_durable is the database's view of the directory, got {durable:?}"
    );
}

/// A WASM consumer subscribing to an `ephemeral:` channel takes its position on
/// an in-memory ring cursor and writes no durable row.
#[tokio::test]
async fn build_messaging_wires_wasm_ephemeral_consumer_to_a_ring_cursor() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::ParticipantId;
    use brenn_lib::messaging::config::{
        ChannelConfigRaw, Depth, WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw,
    };
    use brenn_server::test_support::init_db_memory;

    let channel = ChannelConfigRaw {
        send_rate: None,
        uuid: None,
        address: Some("ephemeral:sensors".to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(4)),
        retain_depth: Some(Depth::Bounded(8)),
        standing_retain_depth: None,
        noise: None,
        sink: None,
        wake_min: None,
    };
    let consumer = WasmConsumerConfigRaw {
        slug: "watcher".to_string(),
        package: "watcher".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some("ephemeral:sensors".to_string()),
            port: "in".to_string(),
            push_depth: None,
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact("sensors".to_string())],
        ..minimal_wasm_consumer()
    };
    let config = BrennConfig {
        channels: vec![channel],
        wasm_consumers: vec![consumer],
        ..BrennConfig::default()
    };

    let db = init_db_memory();
    let db_probe = db.clone();

    let result = boot_messaging(&config, db).await;

    let messenger = result.messenger.as_ref().unwrap();
    let uuid = brenn_lib::messaging::ephemeral_channel_uuid_from_name("sensors");
    let store = messenger
        .ring_stores()
        .get(&uuid)
        .expect("the ephemeral channel has a ring store");
    assert!(
        store.is_attached(&ParticipantId::for_wasm("watcher")),
        "the WASM consumer's ephemeral input is registered as an in-memory ring cursor"
    );

    let conn = db_probe.lock().await;
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_subscriber_cursors",
            [],
            |r| r.get(0),
        )
        .expect("count cursor rows");
    assert_eq!(
        rows, 0,
        "a ring-backed input's position is the in-memory cursor, not a durable row"
    );
}

/// Boot's registration split, both halves in one consumer: every input registers
/// in the directory whatever its channel's class — that registration is where the
/// noise rung a ring eviction escalates against is read from — while only the
/// durable input gets a *persisted position*. Filtering non-durable inputs back
/// out of the directory would leave every ring eviction unable to resolve a rung;
/// persisting the ring input's position would make the next boot treat its queue
/// as pre-existing and skip priming it.
#[tokio::test]
async fn build_messaging_registers_every_wasm_input_but_persists_only_durable_ones() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::SubscriberEntryKind;
    use brenn_lib::messaging::config::{
        ChannelConfigRaw, Depth, NoiseLevel, WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw,
    };
    use brenn_server::test_support::init_db_memory;

    fn chan(address: &str, uuid: Option<&str>) -> ChannelConfigRaw {
        ChannelConfigRaw {
            send_rate: None,
            uuid: uuid.map(str::to_string),
            address: Some(address.to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(Depth::Bounded(4)),
            retain_depth: Some(Depth::Bounded(8)),
            // The durable half of the pair takes the standing frontier; the
            // non-durable one has no third number to state.
            standing_retain_depth: uuid.map(|_| Depth::Bounded(8)),
            noise: None,
            sink: None,
            wake_min: None,
        }
    }
    fn sub(channel: &str, port: &str) -> WasmConsumerSubscriptionRaw {
        WasmConsumerSubscriptionRaw {
            channel: Some(channel.to_string()),
            port: port.to_string(),
            push_depth: None,
            retain_depth: None,
            noise: Some(NoiseLevel::Alarm),
            wake_min: None,
            amplification: None,
        }
    }

    let consumer = WasmConsumerConfigRaw {
        slug: "watcher".to_string(),
        package: "watcher".to_string(),
        subscriptions: vec![sub("brenn:durable-in", "d"), sub("ephemeral:ring-in", "r")],
        subscribe_acl: vec![ChannelMatcherRaw::Exact("durable-in".to_string())],
        ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact("ring-in".to_string())],
        ..minimal_wasm_consumer()
    };
    let config = BrennConfig {
        channels: vec![
            chan(
                "brenn:durable-in",
                Some("22222222-2222-4222-8222-222222222222"),
            ),
            chan("ephemeral:ring-in", None),
        ],
        wasm_consumers: vec![consumer],
        ..BrennConfig::default()
    };

    let db = init_db_memory();
    let db_probe = db.clone();
    let result = boot_messaging(&config, db).await;
    let messenger = result.messenger.as_ref().unwrap();

    for address in ["brenn:durable-in", "ephemeral:ring-in"] {
        let entry = messenger
            .directory()
            .resolve(address)
            .unwrap_or_else(|| panic!("{address} must be in the one directory"));
        let sub = entry
            .subscribers
            .iter()
            .find(|s| matches!(&s.kind, SubscriberEntryKind::Wasm(slug) if slug == "watcher"))
            .unwrap_or_else(|| {
                panic!("{address} must carry the consumer's registration: {entry:?}")
            });
        assert_eq!(
            sub.noise,
            NoiseLevel::Alarm,
            "{address}: the registration carries the resolved noise rung"
        );
    }

    let durable_uuid = messenger
        .directory()
        .resolve("brenn:durable-in")
        .expect("durable channel present")
        .uuid;
    let conn = db_probe.lock().await;
    let rows: Vec<Vec<u8>> = conn
        .prepare("SELECT channel_uuid FROM messaging_subscriber_cursors")
        .expect("prepare")
        .query_map([], |r| r.get(0))
        .expect("query")
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        rows,
        vec![durable_uuid.as_bytes().to_vec()],
        "only the durable input holds a persisted position"
    );
}

/// The confined-channel mirror of the ephemeral consumer test: a WASM consumer
/// with a `local:` input attaches an in-memory ring cursor (no durable row), a
/// confined publish reaches the cursor, and nothing lands in the DB.
#[tokio::test]
async fn build_messaging_wires_wasm_local_consumer_to_a_ring_cursor() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::ParticipantId;
    use brenn_lib::messaging::config::{
        ChannelConfigRaw, Depth, WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw,
    };
    use brenn_server::test_support::init_db_memory;

    let channel = ChannelConfigRaw {
        send_rate: None,
        uuid: None,
        address: Some("local:scratch".to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(4)),
        retain_depth: Some(Depth::Bounded(8)),
        standing_retain_depth: None,
        noise: None,
        sink: None,
        wake_min: None,
    };
    let consumer = WasmConsumerConfigRaw {
        slug: "watcher".to_string(),
        package: "watcher".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some("local:scratch".to_string()),
            port: "in".to_string(),
            push_depth: None,
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        local_subscribe_acl: vec![ChannelMatcherRaw::Exact("scratch".to_string())],
        ..minimal_wasm_consumer()
    };
    let config = BrennConfig {
        channels: vec![channel],
        wasm_consumers: vec![consumer],
        ..BrennConfig::default()
    };

    let db = init_db_memory();
    let db_probe = db.clone();

    let result = boot_messaging(&config, db).await;

    let messenger = result.messenger.as_ref().unwrap();
    let uuid = brenn_lib::messaging::local_channel_uuid_from_name("scratch");
    let store = messenger
        .ring_stores()
        .get(&uuid)
        .expect("the local channel has a ring store");
    let subscriber = ParticipantId::for_wasm("watcher");
    assert!(
        store.is_attached(&subscriber),
        "the WASM consumer's local input is registered as an in-memory ring cursor"
    );

    store.append(brenn_lib::messaging::MessageEnvelope {
        message_id: uuid::Uuid::new_v4(),
        source: "node".to_string(),
        channel: "local:scratch".to_string(),
        sender: "test-sender".to_string(),
        publish_ts: chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
        body: "ping".to_string(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: brenn_lib::messaging::Urgency::Normal,
        envelope_type: brenn_lib::messaging::ChannelScheme::Local,
    });
    let window = store
        .window(&subscriber, 4, 0)
        .expect("the case attached this subscriber");
    assert_eq!(
        window.new_len(),
        1,
        "the confined publish is delivered to the WASM consumer's ring cursor"
    );
    assert_eq!(window.new_entries()[0].message.body, "ping");

    let conn = db_probe.lock().await;
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_subscriber_cursors",
            [],
            |r| r.get(0),
        )
        .expect("count cursor rows");
    assert_eq!(
        rows, 0,
        "a confined ring-backed input's position is the in-memory cursor, not a durable row"
    );
    // A confined channel is never a message row either.
    let msg_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
        .expect("count message rows");
    assert_eq!(
        msg_rows, 0,
        "a confined local: channel writes no message rows"
    );
}

/// The whole io_port story through a real boot: a consumer whose only port is an
/// io_port, with no `[[channel]]` block, no `link`, and no ACL entry in
/// config. One channel serves both halves, so the component's own publish lands in
/// its own ring cursor — the self-loop the timer idiom rides, structural rather
/// than an operator convention.
#[tokio::test]
async fn build_messaging_wires_an_io_port_to_its_own_ring_cursor() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::ParticipantId;
    use brenn_lib::messaging::config::{Depth, WasmConsumerConfigRaw, WasmConsumerIoPortRaw};
    use brenn_messaging::publish::WasmPublish;
    use brenn_server::test_support::init_db_memory;

    let consumer = WasmConsumerConfigRaw {
        slug: "ticker".to_string(),
        package: "ticker".to_string(),
        grants: vec![ComponentGrant::Ports],
        io_ports: vec![WasmConsumerIoPortRaw {
            port: "timer".to_string(),
            channel: None,
            push_depth: Some(Depth::Bounded(2)),
            retain_depth: Some(Depth::Bounded(8)),
            noise: None,
            amplification: None,
            urgency: None,
            publish_per_activation: None,
            publish_capacity: None,
        }],
        ..minimal_wasm_consumer()
    }
    .implying_its_vocabulary();
    let config = BrennConfig {
        wasm_consumers: vec![consumer],
        ..BrennConfig::default()
    };

    let result = boot_messaging(&config, init_db_memory()).await;

    let resolved = &result.wasm_consumers[0];
    assert_eq!(resolved.inputs.len(), 1);
    assert_eq!(resolved.outputs.len(), 1);
    let address = resolved.outputs[0].channel_address.clone();
    assert!(
        address.starts_with("local:auto."),
        "the default is an anonymous non-transportable channel, got {address:?}",
    );
    assert_eq!(
        resolved.inputs[0].sub.channel_address, address,
        "both halves of an io_port bind one channel",
    );
    let bare = address.strip_prefix("local:").unwrap();
    assert!(resolved.policy.allows_local_publish(bare));
    assert!(resolved.policy.allows_local_delivery(bare));

    let messenger = result.messenger.as_ref().unwrap();
    let store = messenger
        .ring_stores()
        .get(&resolved.inputs[0].sub.channel_uuid)
        .expect("the anonymous auto channel has a ring store");
    let subscriber = ParticipantId::for_wasm("ticker");
    assert!(
        store.is_attached(&subscriber),
        "the io_port's input half is registered as a ring cursor"
    );

    messenger
        .publish_from_wasm(
            "ticker",
            &[WasmPublish {
                channel_address: &address,
                body: "tick",
                urgency: brenn_lib::messaging::Urgency::Normal,
                reply_to: None,
                deliver_after: None,
            }],
        )
        .await;

    let window = store
        .window(&subscriber, 2, 0)
        .expect("the io_port's input half is attached");
    assert_eq!(
        window.new_len(),
        1,
        "the component's own publish is delivered back to it"
    );
    assert_eq!(window.new_entries()[0].message.body, "tick");
    let first_seq = window.new_entries()[0].seq;

    // The timer idiom itself: a deferred self-publish parks, is invisible until
    // it comes due, and then lands on the very cursor the io_port's input half
    // holds. Wired to two channels this is where the wake would vanish.
    let due = chrono::Utc::now() + chrono::Duration::hours(1);
    messenger
        .publish_from_wasm(
            "ticker",
            &[WasmPublish {
                channel_address: &address,
                body: "wake",
                urgency: brenn_lib::messaging::Urgency::Normal,
                reply_to: None,
                deliver_after: Some(due),
            }],
        )
        .await;
    store.advance(&subscriber, first_seq, first_seq);
    assert_eq!(
        store
            .window(&subscriber, 2, 0)
            .expect("the io_port's input half is attached")
            .new_len(),
        0,
        "a parked schedule is observable to nobody before it releases",
    );

    let released = store.release_due(due + chrono::Duration::seconds(1));
    assert_eq!(released.messages.len(), 1, "the schedule came due");
    let window = store
        .window(&subscriber, 2, 0)
        .expect("the io_port's input half is attached");
    assert_eq!(
        window.new_len(),
        1,
        "the released wake reaches the same port that scheduled it"
    );
    assert_eq!(window.new_entries()[0].message.body, "wake");
}

/// Naming an io_port's channel `brenn:` is the one line that buys durability, and
/// this is the whole path behind it: the synthesized entry reaches
/// `pre_directory`, the resolvers, and `upsert_channels`, so the channel a timer's
/// parked schedules live on has a DB row after boot — with no `[[channel]]` block
/// and no ACL entry in config.
#[tokio::test]
async fn build_messaging_gives_a_named_brenn_io_port_channel_a_db_row() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::{Depth, WasmConsumerConfigRaw, WasmConsumerIoPortRaw};
    use brenn_server::test_support::init_db_memory;

    let consumer = WasmConsumerConfigRaw {
        slug: "ticker".to_string(),
        package: "ticker".to_string(),
        grants: vec![ComponentGrant::Ports],
        io_ports: vec![WasmConsumerIoPortRaw {
            port: "timer".to_string(),
            channel: Some("brenn:etl.timer".to_string()),
            push_depth: Some(Depth::Bounded(2)),
            retain_depth: Some(Depth::Bounded(8)),
            noise: None,
            amplification: None,
            urgency: None,
            publish_per_activation: None,
            publish_capacity: None,
        }],
        ..minimal_wasm_consumer()
    }
    .implying_its_vocabulary();
    let config = BrennConfig {
        wasm_consumers: vec![consumer],
        ..BrennConfig::default()
    };
    let db = init_db_memory();

    let result = boot_messaging(&config, db.clone()).await;

    let resolved = &result.wasm_consumers[0];
    assert_eq!(resolved.inputs[0].sub.channel_address, "brenn:etl.timer");
    assert_eq!(resolved.outputs[0].channel_address, "brenn:etl.timer");
    // Both roles, injected from the one io_port declaration.
    assert!(resolved.policy.allows_brenn_publish("etl.timer"));
    assert!(resolved.policy.allows_brenn_delivery("etl.timer"));

    let expected_uuid = brenn_lib::messaging::durable_auto_channel_uuid("etl.timer");
    assert_eq!(resolved.inputs[0].sub.channel_uuid, expected_uuid);

    let conn = db.lock().await;
    let (address, description): (String, Option<String>) = conn
        .query_row(
            "SELECT address, description FROM messaging_channels WHERE uuid = ?1",
            rusqlite::params![expected_uuid.as_bytes().to_vec()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("a durable auto channel is upserted like any other durable channel");
    assert_eq!(address, "brenn:etl.timer");
    assert_eq!(
        description.as_deref(),
        Some("auto channel: wasm:ticker/timer")
    );
}

/// The directory listings with auto channels present. `list_channels` walks the
/// durable half and panics on a non-durable entry reaching it, so an anonymous
/// auto channel mis-sorted into the durable set would surface as a runtime panic
/// in an LLM-facing path rather than a boot failure.
#[tokio::test]
async fn auto_channels_list_as_their_durability_says() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::{Depth, WasmConsumerConfigRaw, WasmConsumerIoPortRaw};
    use brenn_server::test_support::init_db_memory;

    let io_port = |port: &str, channel: Option<&str>| WasmConsumerIoPortRaw {
        port: port.to_string(),
        channel: channel.map(str::to_string),
        push_depth: Some(Depth::Bounded(2)),
        retain_depth: Some(Depth::Bounded(8)),
        noise: None,
        amplification: None,
        urgency: None,
        publish_per_activation: None,
        publish_capacity: None,
    };
    let consumer = WasmConsumerConfigRaw {
        slug: "ticker".to_string(),
        package: "ticker".to_string(),
        grants: vec![ComponentGrant::Ports],
        io_ports: vec![
            io_port("anon", None),
            io_port("named", Some("brenn:etl.timer")),
        ],
        ..minimal_wasm_consumer()
    }
    .implying_its_vocabulary();
    let config = BrennConfig {
        channels: vec![brenn_lib::messaging::config::ChannelConfigRaw {
            send_rate: None,
            uuid: Some("5b0e1c9a-2d44-4f18-9a3b-6c7e0d81f204".to_string()),
            address: Some("brenn:etl.feed".to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(Depth::Bounded(2)),
            retain_depth: Some(Depth::Bounded(8)),
            standing_retain_depth: Some(brenn_lib::messaging::config::Depth::Unbounded),
            noise: None,
            sink: None,
            wake_min: None,
        }],
        wasm_consumers: vec![consumer],
        ..BrennConfig::default()
    };

    // A bystander app holding a real grant on an ordinary declared channel. Its
    // listing is non-empty, so "neither auto channel is in it" is the policy
    // filter talking and not an empty vector: naming an auto channel grants
    // nothing, and an anonymous one is reachable by no policy at all.
    let mut policy = brenn_lib::access::AppPolicy::default();
    policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::MessagingSubscribe);
    policy.acls.brenn_subscribe = vec![brenn_lib::access::acl::ChannelMatcher::Exact(
        "etl.feed".to_string(),
    )];
    let mut apps_map: IndexMap<String, AppConfig> = IndexMap::new();
    apps_map.insert(
        "graf".to_string(),
        AppConfig {
            policy,
            ..minimal_app_config("graf", None, vec![])
        },
    );
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IndexMap::new();
    let result = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &tuning_for(&config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
    let messenger = result.messenger.as_ref().unwrap();

    let accessible: Vec<String> = messenger
        .list_accessible_channels("graf")
        .into_iter()
        .map(|row| row.address)
        .collect();
    assert!(
        accessible.contains(&"brenn:etl.feed".to_string()),
        "the caller's own grant reaches its own channel, got {accessible:?}",
    );
    assert!(
        !accessible
            .iter()
            .any(|address| address.contains("auto.") || address == "brenn:etl.timer"),
        "this listing is filtered by the caller's own policy, which covers neither \
         auto channel; an anonymous one is additionally visible to nothing at all, \
         got {accessible:?}",
    );

    let listed: Vec<String> = messenger
        .list_channels()
        .into_iter()
        .map(|row| row.address)
        .collect();
    assert!(
        listed.contains(&"brenn:etl.timer".to_string()),
        "a durable named auto channel is an ordinary durable channel, got {listed:?}",
    );
    assert!(
        !listed.iter().any(|address| address.contains("auto.")),
        "an anonymous auto channel is non-durable and never reaches this dump, got {listed:?}",
    );
}

/// `local:` gives each realm a private namespace, so one bare name may be a
/// backend server ring and a surface's page ring at once. Nothing in the boot
/// path may treat that coincidence as a misconfiguration: the two are unrelated
/// channels sharing only a spelling, which is what lets many surfaces stamped
/// from one config template carry identical `local:` names. This drives the whole
/// of `build_messaging`, so any stage that grew a conflict check would fail here.
#[tokio::test]
async fn a_shared_local_name_across_the_two_realms_boots() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::{Depth, SurfaceConfigRaw};
    use brenn_server::test_support::init_db_memory;

    let config = BrennConfig {
        wasm_consumers: vec![
            brenn_lib::messaging::config::WasmConsumerConfigRaw {
                slug: "ticker".to_string(),
                grants: vec![ComponentGrant::Ports],
                io_ports: vec![io_port_raw(
                    "tick",
                    Some("local:etl.tick"),
                    Depth::Bounded(2),
                    Depth::Bounded(8),
                )],
                ..minimal_wasm_consumer()
            }
            .implying_its_vocabulary(),
        ],
        surfaces: vec![SurfaceConfigRaw {
            subscriptions: vec![brenn_lib::messaging::config::SurfaceSubscriptionRaw {
                // The stock global push depth is unbounded, which no page queue
                // can be, so a surface binding states its own.
                push_depth: Some(Depth::Bounded(4)),
                retain_depth: Some(Depth::Bounded(3)),
                ..local_sub_raw("local:etl.tick", "protobar", "snoop")
            }],
            ..minimal_surface_raw()
        }],
        ..BrennConfig::default()
    };

    let result = boot_messaging(&config, init_db_memory()).await;
    let messenger = result
        .messenger
        .as_ref()
        .expect("a name coincidence across the two local: realms is not a misconfiguration");
    let store = messenger
        .ring_stores()
        .get_by_address("local:etl.tick")
        .expect("the backend io_port's server ring is wired into the Messenger");
    assert_eq!(store.address(), "local:etl.tick");
    assert_eq!(
        result.surfaces[0]
            .local_channels
            .iter()
            .filter(|channel| channel.address == "local:etl.tick")
            .count(),
        1,
        "the surface binding declares its own page ring under the same name, got {:?}",
        result.surfaces[0].local_channels,
    );
}

/// A `brenn:<address>` channel config carrying an explicit uuid and push_depth,
/// plus a WASM consumer "watcher" whose durable input binds it with a covering
/// `subscribe_acl`. The channel is present in both, so boot 1 (channel only) and
/// boot 2 (channel + consumer) name the same durable channel.
fn warm_brenn_priming_configs(
    address: &str,
    push_depth: brenn_lib::messaging::config::Depth,
) -> (
    brenn_lib::config::BrennConfig,
    brenn_lib::config::BrennConfig,
    uuid::Uuid,
) {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::{
        ChannelConfigRaw, WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw,
    };
    let uuid = uuid::Uuid::new_v4();
    let channel = ChannelConfigRaw {
        send_rate: None,
        uuid: Some(uuid.to_string()),
        address: Some(format!("brenn:{address}")),
        address_prefix: None,
        description: None,
        push_depth: Some(push_depth),
        retain_depth: Some(brenn_lib::messaging::config::Depth::Bounded(64)),
        standing_retain_depth: Some(brenn_lib::messaging::config::Depth::Unbounded),
        noise: None,
        sink: None,
        wake_min: None,
    };
    let channel_only = BrennConfig {
        channels: vec![channel.clone()],
        ..BrennConfig::default()
    };
    let consumer = WasmConsumerConfigRaw {
        slug: "watcher".to_string(),
        package: "watcher".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some(format!("brenn:{address}")),
            port: "in".to_string(),
            push_depth: None,
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        subscribe_acl: vec![ChannelMatcherRaw::Exact(address.to_string())],
        ..minimal_wasm_consumer()
    };
    let with_consumer = BrennConfig {
        channels: vec![channel],
        wasm_consumers: vec![consumer],
        ..BrennConfig::default()
    };
    (channel_only, with_consumer, uuid)
}

/// Insert `n` retained (deliver_after-free) messages onto `channel_uuid` with no
/// pending pushes — a warm channel tail that pre-dates any subscription.
async fn warm_channel_with_messages(db: &brenn_db::Db, channel_uuid: uuid::Uuid, n: u32) {
    let conn = db.lock().await;
    for i in 0..n {
        brenn_messaging_store::db::insert_message(
            &conn,
            channel_uuid,
            "brenn://test",
            "test-sender",
            &format!("body-{i}"),
            brenn_lib::messaging::Urgency::Normal,
            brenn_lib::messaging::ChannelScheme::Brenn,
            None,
            None,
            None,
            None,
            i as i64 + 1,
        );
    }
}

/// A newly-created durable WASM queue primes from the channel's retained tail
/// (attach is a delivery point): the tail, capped by push_depth, is seeded as
/// pending pushes so the consumer wakes on it as NEW rather than only on the
/// next publish.
#[tokio::test]
async fn build_messaging_primes_a_new_durable_wasm_queue_from_the_retained_tail() {
    use brenn_lib::messaging::ParticipantId;
    use brenn_lib::messaging::config::Depth;
    use brenn_server::test_support::init_db_memory;

    let (channel_only, with_consumer, channel_uuid) =
        warm_brenn_priming_configs("sensors", Depth::Bounded(4));

    let db = init_db_memory();

    // Boot 1: the channel exists (its row is written) but no consumer yet.
    boot_messaging(&channel_only, db.clone()).await;

    warm_channel_with_messages(&db, channel_uuid, 6).await;

    // Boot 2: the consumer is added — a brand-new durable queue.
    let booted = boot_messaging(&with_consumer, db.clone()).await;
    let messenger = booted
        .messenger
        .expect("boot with a consumer wires a messenger");

    // The primed position is the whole of the priming: it starts at the oldest of
    // the newest `push_depth` retained messages, so the tail is what the
    // consumer's first window serves as new. Reading it back pins both the
    // tail selection (newest four, not oldest) and the order (oldest-first) —
    // a count alone would pass on a regression in either.
    let owed: Vec<String> = brenn_messaging::testutils::owed_everywhere(
        &messenger,
        &ParticipantId::for_wasm("watcher"),
    )
    .await
    .into_iter()
    .map(|(_, envelope)| envelope.body.clone())
    .collect();
    assert_eq!(
        owed,
        vec!["body-2", "body-3", "body-4", "body-5"],
        "a new durable WASM queue primes at the newest push_depth retained messages, oldest first"
    );
}

/// A durable WASM queue whose subscription registration survived the prior boot
/// keeps its carried-over pending rows and does NOT re-prime — priming is a
/// first-attach event, not a per-boot one.
#[tokio::test]
async fn build_messaging_does_not_reprime_a_surviving_durable_wasm_queue() {
    use brenn_lib::messaging::ParticipantId;
    use brenn_lib::messaging::config::Depth;
    use brenn_server::test_support::init_db_memory;

    let (_channel_only, with_consumer, channel_uuid) =
        warm_brenn_priming_configs("sensors", Depth::Bounded(4));

    let db = init_db_memory();

    // Boot 1: consumer present, channel cold — registration is recorded.
    boot_messaging(&with_consumer, db.clone()).await;

    // Six messages arrive after the queue exists — more than the depth-4 cap, which
    // is what makes re-priming observable: a surviving position stays at head-of-boot-1
    // and is owed all six, while a re-prime would jump to the newest four.
    warm_channel_with_messages(&db, channel_uuid, 6).await;

    // Boot 2: the same consumer — a surviving registration.
    let booted = boot_messaging(&with_consumer, db.clone()).await;
    let messenger = booted
        .messenger
        .expect("boot with a consumer wires a messenger");

    let owed: Vec<String> = brenn_messaging::testutils::owed_everywhere(
        &messenger,
        &ParticipantId::for_wasm("watcher"),
    )
    .await
    .into_iter()
    .map(|(_, envelope)| envelope.body.clone())
    .collect();
    assert_eq!(
        owed,
        vec!["body-0", "body-1", "body-2", "body-3", "body-4", "body-5"],
        "a surviving durable WASM queue keeps its position — it is owed everything \
         published since, not re-primed to the newest push_depth"
    );
}

/// Plant `n` retained message rows directly under `channel_uuid`, bypassing the
/// channel-row foreign key. A non-durable channel never legitimately holds DB
/// message rows (no `messaging_channels` row backs it), so this fabricates the
/// only state under which the durable priming loop's non-durable skip guard is
/// observable: were the guard removed, `load_channel_retained_tail` would find
/// these rows and seed pushes for the ring-backed queue.
async fn plant_orphan_message_rows(db: &brenn_db::Db, channel_uuid: uuid::Uuid, n: u32) {
    let conn = db.lock().await;
    conn.pragma_update(None, "foreign_keys", "OFF")
        .expect("fk off");
    for i in 0..n {
        conn.execute(
            "INSERT INTO messaging_messages \
             (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at, envelope_type) \
             VALUES (?1, ?2, 'node', 'test-sender', ?3, 'normal', ?4, 'now', 'ephemeral')",
            rusqlite::params![
                uuid::Uuid::new_v4().as_bytes().to_vec(),
                channel_uuid.as_bytes().to_vec(),
                format!("body-{i}"),
                i as i64 + 1,
            ],
        )
        .expect("plant orphan message row");
    }
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("fk on");
}

/// A new WASM queue on a *non-durable* channel is primed via the ring at attach,
/// never by seeding durable pending-push rows: the durable priming loop skips
/// non-durable channels. Even with retained
/// message rows sitting under the channel's uuid — which absent the skip guard
/// `load_channel_retained_tail` would seed — no pending pushes are written.
#[tokio::test]
async fn build_messaging_does_not_seed_db_pushes_for_a_nondurable_wasm_queue() {
    use brenn_lib::access::raw::ChannelMatcherRaw;
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::ParticipantId;
    use brenn_lib::messaging::config::{
        ChannelConfigRaw, Depth, WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw,
    };
    use brenn_server::test_support::init_db_memory;

    let channel = ChannelConfigRaw {
        send_rate: None,
        uuid: None,
        address: Some("ephemeral:sensors".to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(4)),
        retain_depth: Some(Depth::Bounded(8)),
        standing_retain_depth: None,
        noise: None,
        sink: None,
        wake_min: None,
    };
    let consumer = WasmConsumerConfigRaw {
        slug: "watcher".to_string(),
        package: "watcher".to_string(),
        subscriptions: vec![WasmConsumerSubscriptionRaw {
            channel: Some("ephemeral:sensors".to_string()),
            port: "in".to_string(),
            push_depth: None,
            retain_depth: None,
            noise: None,
            wake_min: None,
            amplification: None,
        }],
        ephemeral_subscribe_acl: vec![ChannelMatcherRaw::Exact("sensors".to_string())],
        ..minimal_wasm_consumer()
    };
    let config = BrennConfig {
        channels: vec![channel],
        wasm_consumers: vec![consumer],
        ..BrennConfig::default()
    };

    let db = init_db_memory();

    let uuid = brenn_lib::messaging::ephemeral_channel_uuid_from_name("sensors");
    plant_orphan_message_rows(&db, uuid, 6).await;

    boot_messaging(&config, db.clone()).await;

    // A non-durable queue is primed through the ring, so the durable side holds no
    // position for it at all — the guard's observable effect.
    let conn = db.lock().await;
    let cursors: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_subscriber_cursors WHERE subscriber = ?1",
            rusqlite::params![ParticipantId::for_wasm("watcher").as_str()],
            |row| row.get(0),
        )
        .expect("count durable cursor rows");
    assert_eq!(
        cursors, 0,
        "a non-durable WASM queue is primed via the ring, never by a durable cursor row"
    );
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
    use brenn_server::test_support::init_db_memory;
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
        &tuning_for(&config),
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

/// One `[[remote]]` block plus the mode-0600 token file it names, returned
/// together so the file outlives the resolve that reads it.
fn minimal_remote(
    slug: &str,
) -> (
    brenn_lib::messaging::RemoteConfigRaw,
    tempfile::NamedTempFile,
) {
    use std::io::Write as _;
    let mut token = tempfile::NamedTempFile::new().expect("a temp token file");
    token.write_all(b"s3cret-token\n").expect("write the token");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(token.path(), std::fs::Permissions::from_mode(0o600))
            .expect("tighten the token file");
    }
    let raw = brenn_lib::config::remote_raw(
        slug,
        token.path(),
        &[brenn_lib::messaging::AttachGrant::Alert],
    );
    (raw, token)
}

/// `messaging_configured` must fire on a config whose only messaging content is
/// one `[[remote]]`. Without it a remote-only deployment boots with the
/// subsystem off and every subscribe refused — the exact failure the clause
/// exists to prevent.
#[test]
fn messaging_configured_covers_remote_only() {
    use brenn_lib::config::BrennConfig;
    let empty_webhooks: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IndexMap::new();
    let (remote, _token) = minimal_remote("pod-kitchen");
    let config = BrennConfig {
        remotes: vec![remote],
        ..BrennConfig::default()
    };
    assert!(
        messaging_configured(&config, &empty_webhooks, &[]),
        "a remote-only config must activate messaging"
    );
}

/// A remote-only config brings messaging up, resolves its `[[remote]]` blocks,
/// and registers the wake economics a runtime-minted `Remote` entry is
/// cross-checked against at boot. A dropped `subscriber_registrations` insert
/// would fail *closed* at the delivery ACL gate — a silent drop — so it is
/// asserted here rather than left to the first delivery.
#[tokio::test]
async fn build_messaging_brings_up_remote_only_config() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::{SubscriberEntryKind, WakeEconomics};
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let (remote, _token) = minimal_remote("pod-kitchen");
    let config = BrennConfig {
        remotes: vec![remote],
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
        &tuning_for(&config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger = result
        .messenger
        .as_ref()
        .expect("a remote-only config must bring messaging up");
    assert_eq!(result.remotes.len(), 1);
    assert_eq!(result.remotes[0].slug, "pod-kitchen");
    assert_eq!(
        messenger
            .subscriber_wake_economics(&SubscriberEntryKind::Remote("pod-kitchen".to_string())),
        Some(WakeEconomics::Eager),
        "a remote's entries are runtime-minted and never in the boot directory, so this \
         config-driven registration is all that stands between them and a fail-closed ACL gate"
    );
    assert_eq!(
        messenger.draw_attach_send_budget_for_batch(
            brenn_lib::messaging::AttachScope::remote("pod-kitchen"),
            None,
            1
        ),
        brenn_messaging::AttachSendVerdict::Admitted,
        "boot installs the remote's durable send budget; this is the call that panics on a \
         missing bucket, so a dropped or mis-scoped installer takes the remote's first durable \
         publish down with it"
    );
}

/// A `[[surface]]` and a `[[remote]]` may share a slug — nothing forbids an
/// operator naming a pod and a page alike — and the durable send budget is keyed
/// by *principal*, so the two must hold separate buckets. Draining one and
/// finding the other full is what proves the key carries the kind.
#[tokio::test]
async fn a_remote_and_a_surface_of_one_slug_hold_separate_send_budgets() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::AttachScope;
    use brenn_messaging::AttachSendVerdict;
    use brenn_server::test_support::init_db_memory;

    let (remote, _token) = minimal_remote("deskbar");
    let config = BrennConfig {
        remotes: vec![remote],
        surfaces: vec![minimal_surface_raw()],
        ..BrennConfig::default()
    };

    let result = boot_messaging(&config, init_db_memory()).await;
    let messenger = result
        .messenger
        .as_ref()
        .expect("a remote plus a surface brings messaging up");

    // Drain the remote's bucket whole: the burst is one all-or-nothing draw, and
    // the refill is minutes away.
    assert_eq!(
        messenger.draw_attach_send_budget_for_batch(
            AttachScope::remote("deskbar"),
            None,
            brenn_messaging::publish::SURFACE_SEND_BURST,
        ),
        AttachSendVerdict::Admitted,
    );
    assert_eq!(
        messenger.draw_attach_send_budget_for_batch(AttachScope::remote("deskbar"), None, 1),
        AttachSendVerdict::Denied,
        "the remote's own bucket is now empty"
    );
    assert_eq!(
        messenger.draw_attach_send_budget_for_batch(AttachScope::surface("deskbar"), None, 1),
        AttachSendVerdict::Admitted,
        "the surface of the same slug is a different principal and spends a different bucket"
    );
}

/// A registry holding one async tool `apull` (acl key `repo`), matching the
/// git-repo-pull shape without needing the real repo-sync handles.
fn async_tool_registry() -> std::sync::Arc<brenn_tool_registry::ToolRegistry> {
    use brenn_tool_registry::ToolError;
    use brenn_tool_registry::descriptor::{AclDenied, Idempotency, ToolClass, ToolDescriptor};
    use brenn_tool_registry::tool::{AsyncTool, RegisteredTool, ToolCtx};
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
    std::sync::Arc::new(brenn_tool_registry::ToolRegistry::new(vec![
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
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let repo_clause = std::collections::BTreeMap::from([("repo".to_string(), "brenn".to_string())]);
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
        &tuning_for(&config),
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

/// The uuid uniqueness assert covers tool-substrate entries too. A channel uuid
/// pasted from a `brenn:tools/<tool>` channel is only visible once all entry
/// sources have contributed — and nothing downstream catches the collision.
#[tokio::test]
#[should_panic(expected = "both carry uuid")]
async fn build_messaging_panics_when_a_channel_uuid_collides_with_a_tool_channel() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::WasmConsumerConfigRaw;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let repo_clause = std::collections::BTreeMap::from([("repo".to_string(), "brenn".to_string())]);
    let consumer = WasmConsumerConfigRaw {
        tool_grants: vec![brenn_lib::tools::config::ToolGrantRaw {
            tool: "apull".to_string(),
            acl: vec![repo_clause],
            rate_limit: None,
        }],
        ..minimal_wasm_consumer()
    };

    let config = BrennConfig {
        wasm_consumers: vec![consumer],
        channels: vec![brenn_lib::messaging::config::ChannelConfigRaw {
            uuid: Some(
                brenn_lib::messaging::tool_channel_uuid_from_address("brenn:tools/apull")
                    .to_string(),
            ),
            standing_retain_depth: Some(Depth::Bounded(8)),
            ..nondurable_channel("brenn:probe.loop", 4)
        }],
        ..BrennConfig::default()
    };
    let db = init_db_memory();
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let webhook_endpoints: IndexMap<String, Arc<ResolvedWebhookEndpoint>> = IM::new();

    build_messaging(
        &config,
        db,
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoints,
        &[],
        &tuning_for(&config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &async_tool_registry(),
    )
    .await;
}

/// `build_messaging` registers the `system:surface-help` participant, and
/// `publish_description` writes documents
/// a *non-subscriber* can pull via `Messenger::query` — the ungated-read path the
/// feature relies on. A second boot-publish supersedes the first:
/// `standing_retain_depth = 1` clamps the non-subscriber read to only the newest
/// doc (latest-wins).
#[tokio::test]
async fn boot_description_publish_is_pullable_by_a_non_subscriber_latest_wins() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::Urgency;
    use brenn_lib::messaging::config::{ChannelConfigRaw, Depth};
    use brenn_messaging::publish::PublishResult;
    use brenn_messaging::query::MessageQuery;
    use brenn_server::test_support::init_db_memory;
    use brenn_surface_server::description::{
        SURFACE_HELP_COMPONENT, build_description_docs, publish_description,
    };
    use indexmap::IndexMap as IM;

    let retained_channel = |uuid: &str, address: &str| ChannelConfigRaw {
        send_rate: None,
        uuid: Some(uuid.to_string()),
        address: Some(address.to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(1)),
        retain_depth: Some(Depth::Bounded(1)),
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
        brenn_lib::access::test_fixtures::delivery_policy_for_addresses(["brenn:surface.index"]);
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
        &tuning_for(&config),
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
    use brenn_lib::messaging::config::{ChannelConfigRaw, Depth, SurfaceConfigRaw};
    use brenn_messaging::query::MessageQuery;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let bounded_channel = |uuid: &str, address: &str| ChannelConfigRaw {
        send_rate: None,
        uuid: Some(uuid.to_string()),
        address: Some(address.to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(1)),
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
    reader.policy = brenn_lib::access::test_fixtures::delivery_policy_for_addresses([
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
        &tuning_for(&config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;

    let messenger = result.messenger.as_ref().expect("messaging must be up");
    let epoch = messenger.ring_epoch();

    brenn_surface_server::telemetry::publish_boot_disconnected_stamps(
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
    let stamp = brenn_surface_schema::telemetry::DisconnectedStamp::parse(&env.body)
        .expect("the boot stamp is a valid disconnected stamp");
    assert_eq!(stamp.reason, "server restart");
    assert_eq!(stamp.session, None, "a boot stamp precedes every session");
    assert_eq!(stamp.epoch, epoch);
}

/// A config with one surface plus its bindings channel declared as the operator
/// must declare it: `ephemeral:`, retained, one row wide.
fn surface_with_config_channel(max_body_bytes: usize) -> brenn_lib::config::BrennConfig {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::SurfaceConfigRaw;

    BrennConfig {
        channels: vec![nondurable_channel(
            "ephemeral:surface.surface.deskbar.bindings",
            1,
        )],
        surfaces: vec![SurfaceConfigRaw {
            ..minimal_surface_raw()
        }],
        messaging: MessagingGlobalConfig {
            max_body_bytes,
            ..MessagingGlobalConfig::default()
        },
        ..BrennConfig::default()
    }
}

/// The boot bindings-document path, composed. Every link is unit-tested
/// apart; a family mismatch between the registration grant and the
/// publish-gate dispatch is only visible here — and it would panic every
/// real boot.
#[tokio::test]
async fn boot_bindings_document_is_published_and_pullable() {
    use brenn_messaging::query::MessageQuery;
    use brenn_server::test_support::init_db_memory;
    use brenn_surface_schema::bindings::BindingsDocument;
    use brenn_surface_server::bindings_doc::{
        BindingsDocParams, build_bindings_documents, publish_bindings_documents,
    };
    use indexmap::IndexMap as IM;

    let config = surface_with_config_channel(65_536);
    // A non-subscriber reader with covering read access: the retained window is
    // pullable, which is the shape the attaching surface's replay reads.
    let mut reader = minimal_app_config("some-reader", None, vec![]);
    reader.policy = brenn_lib::access::test_fixtures::delivery_policy_for_addresses([
        "ephemeral:surface.surface.deskbar.bindings",
    ]);
    let mut apps_map: IM<String, AppConfig> = IM::new();
    apps_map.insert("some-reader".to_string(), reader);
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();

    let result = boot_messaging_with(
        &config,
        init_db_memory(),
        &apps,
        alert_dispatcher,
        "brenn://test",
    )
    .await;
    let messenger = result.messenger.as_ref().expect("messaging must be up");

    let params = BindingsDocParams {
        prefix: "surface",
        status_interval_secs: 60,
        error_report: None,
    };
    let docs = build_bindings_documents(&result.surfaces, &params);
    publish_bindings_documents(messenger, &docs).await;

    let envelopes = messenger
        .query(&MessageQuery {
            channel: "ephemeral:surface.surface.deskbar.bindings".to_string(),
            limit: 10,
            before: None,
            after: None,
            sender: None,
            search: None,
            calling_app_slug: "some-reader".to_string(),
        })
        .await
        .expect("config channel query succeeds");

    assert_eq!(envelopes.len(), 1, "one document per surface, retained");
    let env = &envelopes[0];
    assert_eq!(
        env.sender, "system:surface-config",
        "the document is written under the reserved single-writer identity, got {:?}",
        env.sender
    );
    let parsed =
        BindingsDocument::parse(&env.body).expect("the retained body parses and validates");
    let expected =
        brenn_surface_server::bindings_doc::build_bindings_document(&result.surfaces[0], &params);
    assert_eq!(
        parsed, expected,
        "what a surface replays is the document boot built"
    );
}

/// The operator-reachable arm of the publish: a `max_body_bytes` below the
/// document's own size. A surface cannot boot without its wiring, so this is a
/// boot panic naming the knob to raise rather than a surface that attaches to
/// nothing.
#[tokio::test]
#[should_panic(expected = "max_body_bytes")]
async fn boot_bindings_document_publish_panics_when_the_body_exceeds_the_cap() {
    use brenn_server::test_support::init_db_memory;
    use brenn_surface_server::bindings_doc::{
        BindingsDocParams, build_bindings_documents, publish_bindings_documents,
    };
    use indexmap::IndexMap as IM;

    let config = surface_with_config_channel(16);
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();

    let result = boot_messaging_with(
        &config,
        init_db_memory(),
        &apps,
        alert_dispatcher,
        "brenn://test",
    )
    .await;
    let messenger = result.messenger.as_ref().expect("messaging must be up");

    let params = BindingsDocParams {
        prefix: "surface",
        status_interval_secs: 60,
        error_report: None,
    };
    let docs = build_bindings_documents(&result.surfaces, &params);
    publish_bindings_documents(messenger, &docs).await;
}

/// A consumer whose sole port is an io_port on `channel` (absent ⇒ anonymous),
/// at bounded depths so the fold lands on a legal non-durable ring.
fn io_port_consumer(
    slug: &str,
    ports: &[(&str, Option<&str>)],
) -> brenn_lib::messaging::config::WasmConsumerConfigRaw {
    use brenn_lib::messaging::config::{Depth, WasmConsumerConfigRaw};

    WasmConsumerConfigRaw {
        slug: slug.to_string(),
        grants: vec![ComponentGrant::Ports],
        io_ports: ports
            .iter()
            .map(|(port, channel)| {
                io_port_raw(port, *channel, Depth::Bounded(2), Depth::Bounded(8))
            })
            .collect(),
        ..minimal_wasm_consumer()
    }
    .implying_its_vocabulary()
}

/// Naming an auto channel is what lets a third party reach it — with an
/// ordinary binding backed by an ordinary ACL entry, since naming alone grants
/// nothing. What the third party sees is the generated description: an auto
/// channel writes no `[[channel]]` block, so without it a listing row would
/// explain itself to nobody.
#[tokio::test]
async fn a_named_auto_channel_lists_to_a_third_party_with_its_description() {
    use brenn_envelope::grants::AppCapability;
    use brenn_lib::access::AppPolicy;
    use brenn_lib::access::acl::ChannelMatcher;
    use brenn_lib::config::BrennConfig;
    use brenn_server::test_support::init_db_memory;

    let config = BrennConfig {
        wasm_consumers: vec![io_port_consumer(
            "ticker",
            &[("timer", Some("brenn:etl.batches"))],
        )],
        ..BrennConfig::default()
    };

    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy.acls.brenn_subscribe = vec![ChannelMatcher::Exact("etl.batches".to_string())];
    let mut apps_map: IndexMap<String, AppConfig> = IndexMap::new();
    apps_map.insert(
        "graf".to_string(),
        AppConfig {
            policy,
            ..minimal_app_config("graf", None, vec![])
        },
    );
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let result = boot_messaging_with(
        &config,
        init_db_memory(),
        &apps,
        alert_dispatcher,
        "brenn://test",
    )
    .await;

    let row = result
        .messenger
        .as_ref()
        .unwrap()
        .list_accessible_channels("graf")
        .into_iter()
        .find(|row| row.address == "brenn:etl.batches")
        .expect("an ordinary ACL entry reaches a named auto channel");
    assert_eq!(
        row.description.as_deref(),
        Some("auto channel: wasm:ticker/timer"),
        "the generated description is what makes the row self-explaining",
    );
}

/// Auto-injection means a principal's ACL lists in config no longer enumerate
/// its full reach, so the boot log is the accounting a config security review
/// reads instead: one line per (principal, capability, channel).
#[tokio::test]
#[tracing_test::traced_test]
async fn boot_logs_every_injected_auto_grant() {
    use brenn_lib::config::BrennConfig;
    use brenn_server::test_support::init_db_memory;

    let config = BrennConfig {
        wasm_consumers: vec![io_port_consumer(
            "ticker",
            &[("wake", None), ("batches", Some("brenn:etl.batches"))],
        )],
        ..BrennConfig::default()
    };

    let result = boot_messaging(&config, init_db_memory()).await;
    let anonymous = result.wasm_consumers[0].outputs[0].channel_address.clone();
    assert!(anonymous.starts_with("local:auto."));

    logs_assert(|lines: &[&str]| {
        let injected: Vec<&&str> = lines
            .iter()
            .filter(|line| line.contains("auto channel grant injected"))
            .collect();
        // Two io_ports, each both publisher and subscriber on its own channel.
        if injected.len() != 4 {
            return Err(format!("expected 4 injection lines, got {injected:?}"));
        }
        for expected in [
            ("MessagingPublish", "brenn:etl.batches"),
            ("MessagingSubscribe", "brenn:etl.batches"),
            ("LocalPublish", anonymous.as_str()),
            ("LocalSubscribe", anonymous.as_str()),
        ] {
            let (capability, channel) = expected;
            if !injected.iter().any(|line| {
                line.contains(&format!("capability={capability}"))
                    && line.contains(&format!("channel={channel}"))
                    && line.contains("ticker")
            }) {
                return Err(format!("no line for {expected:?} in {injected:?}"));
            }
        }
        Ok(())
    });
}

// ---------------------------------------------------------------------------
// `[[channel]]` tuning blocks for system-minted channels
// ---------------------------------------------------------------------------

/// A tuning block naming a webhook endpoint this config never declares is a
/// typo, not a tuning — boot refuses rather than tuning nothing.
#[test]
#[should_panic(expected = "tunes a channel this config never mints")]
fn a_tuning_block_for_an_undeclared_webhook_panics() {
    let tuning = tuning_of(&[tuning_raw("webhook:ghost")]);
    validate_exact_tuning_blocks(
        &tuning,
        &IndexMap::new(),
        &[],
        &std::collections::HashSet::new(),
    );
}

/// Same for a tool that is not registered.
#[test]
#[should_panic(expected = "tunes a channel this config never mints")]
fn a_tuning_block_for_an_unregistered_tool_panics() {
    let tuning = tuning_of(&[tuning_raw("brenn:tools/nope")]);
    validate_exact_tuning_blocks(
        &tuning,
        &IndexMap::new(),
        &["apull"],
        &std::collections::HashSet::new(),
    );
}

/// A block written in the bare house spelling is the same block: it reaches the
/// boot check rather than slipping past it as an unrecognised address.
#[test]
#[should_panic(expected = "tunes a channel this config never mints")]
fn a_bare_tuning_block_for_an_unregistered_tool_panics() {
    let tuning = tuning_of(&[tuning_raw("tools/nope")]);
    validate_exact_tuning_blocks(
        &tuning,
        &IndexMap::new(),
        &["apull"],
        &std::collections::HashSet::new(),
    );
}

/// And for a result inbox whose consumer holds no async tool grant.
#[test]
#[should_panic(expected = "tunes a channel this config never mints")]
fn a_tuning_block_for_an_ungranted_inbox_panics() {
    let tuning = tuning_of(&[tuning_raw("brenn:tool-results/ghost")]);
    let slugs: std::collections::HashSet<String> = ["sync".to_string()].into_iter().collect();
    validate_exact_tuning_blocks(&tuning, &IndexMap::new(), &["apull"], &slugs);
}

/// An exact `mqtt:` block naming a channel nothing has minted yet boots
/// cleanly: the MQTT population is open-ended, so the block is a standing rule
/// for a channel a runtime subscribe may mint later. Blocks that do name
/// existing endpoints, tools and inboxes pass alongside it — the positive case
/// for each arm, so a lookup that can never succeed (matching the address where
/// the population is keyed by slug, say) is caught here rather than turning
/// every real tuning block into a boot refusal.
#[test]
fn an_mqtt_block_matching_nothing_boots_and_real_blocks_pass() {
    let tuning = tuning_of(&[
        tuning_raw("mqtt:home:sensors/never-seen"),
        tuning_raw("webhook:gh-events"),
        tuning_raw("brenn:tools/apull"),
        tuning_raw("brenn:tool-results/sync"),
    ]);
    let slugs: std::collections::HashSet<String> = ["sync".to_string()].into_iter().collect();
    validate_exact_tuning_blocks(
        &tuning,
        &webhook_endpoint_map("gh-events", "someapp"),
        &["apull"],
        &slugs,
    );
}

/// The tuning table reaches every site that mints a system channel.
///
/// Each mint site resolves independently, and each one falls back to the family
/// default, so an implementation that dropped the table at any one of them would
/// leave a suite that only ever passes an empty table entirely green — and the
/// operator's sizing would silently do nothing for that family. Distinct numbers
/// per family so a crossed wire is caught too.
#[tokio::test]
async fn a_tuning_block_reaches_every_system_channel_mint_site() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::Depth;
    use brenn_lib::messaging::mqtt_channel_uuid_from_address;
    use brenn_lib::mqtt::config::ResolvedMqttIngressChannel;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let mqtt_address = "mqtt:homeassistant:home/tuned/state";
    let ingress_channel = ResolvedMqttIngressChannel {
        channel_address: mqtt_address.to_string(),
        channel_uuid: mqtt_channel_uuid_from_address(mqtt_address),
        client_slug: "homeassistant".to_string(),
        topic: "home/tuned/state".to_string(),
        qos: 1,
        urgency: brenn_lib::messaging::Urgency::Normal,
    };

    let repo_clause = std::collections::BTreeMap::from([("repo".to_string(), "brenn".to_string())]);
    let mut consumer = minimal_wasm_consumer();
    consumer.tool_grants = vec![brenn_lib::tools::config::ToolGrantRaw {
        tool: "apull".to_string(),
        acl: vec![repo_clause],
        rate_limit: None,
    }];

    // One block per family, each with its own retained window.
    let tuned = |address: Option<&str>, prefix: Option<&str>, retain: u64| {
        brenn_lib::messaging::config::ChannelConfigRaw {
            send_rate: None,
            uuid: None,
            address: address.map(str::to_string),
            address_prefix: prefix.map(str::to_string),
            description: None,
            push_depth: Some(Depth::Bounded(1)),
            retain_depth: Some(Depth::Bounded(retain)),
            standing_retain_depth: Some(Depth::Bounded(retain)),
            noise: None,
            sink: None,
            wake_min: None,
        }
    };
    let config = BrennConfig {
        wasm_consumers: vec![consumer],
        channels: vec![
            tuned(Some("webhook:gh-events"), None, 30),
            tuned(Some("brenn:tools/apull"), None, 9),
            tuned(Some("brenn:tool-results/probe"), None, 7),
            tuned(None, Some("mqtt:homeassistant:"), 40),
        ],
        ..BrennConfig::default()
    };

    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
    let result = build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoint_map("gh-events", "someapp"),
        std::slice::from_ref(&ingress_channel),
        &tuning_for(&config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &async_tool_registry(),
    )
    .await;
    let directory = result
        .messenger
        .expect("a tuned system-channel config boots")
        .directory()
        .clone();

    for (address, retain) in [
        ("webhook:gh-events", 30u64),
        ("brenn:tools/apull", 9),
        ("brenn:tool-results/probe", 7),
        (mqtt_address, 40),
    ] {
        let entry = directory
            .resolve(address)
            .unwrap_or_else(|| panic!("{address} must be minted into the directory"));
        assert_eq!(
            entry.resolved_channel.retain_depth,
            Depth::Bounded(retain),
            "{address} must take its tuning block's retain_depth, not the family default",
        );
        assert_eq!(
            entry.resolved_channel.standing_retain_depth,
            Depth::Bounded(retain),
            "{address} must take its tuning block's standing_retain_depth",
        );
    }
}

/// The boot ceiling pass is wired into `build_messaging`, not merely callable.
/// Deleting the call, or moving it above the folds it exists to see, would leave
/// an over-ceiling config booting and surface the violation hours later as the
/// `reap_frontier` panic inside the GC pass.
#[tokio::test]
#[should_panic(expected = "exceeding the channel's standing_retain_depth")]
async fn build_messaging_refuses_a_subscriber_above_the_channel_ceiling() {
    use brenn_lib::config::BrennConfig;
    use brenn_lib::messaging::config::{
        ChannelConfigRaw, ResolvedMessagingConfig, ResolvedSubscription,
    };
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let address = "brenn:tight";
    let uuid = uuid::Uuid::new_v4();
    let config = BrennConfig {
        channels: vec![ChannelConfigRaw {
            send_rate: None,
            uuid: Some(uuid.to_string()),
            address: Some(address.to_string()),
            address_prefix: None,
            description: None,
            push_depth: Some(Depth::Bounded(1)),
            retain_depth: Some(Depth::Bounded(2)),
            standing_retain_depth: Some(Depth::Bounded(2)),
            noise: None,
            sink: None,
            wake_min: None,
        }],
        ..BrennConfig::default()
    };

    let mut app = minimal_app_config(
        "deep",
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: address.to_string(),
                push_depth: Depth::Bounded(1),
                retain_depth: Depth::Bounded(4),
                noise: NoiseLevel::Silent,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            }],
        }),
        vec![],
    );
    app.policy = brenn_lib::access::test_fixtures::delivery_policy_for_addresses([address]);
    let mut apps_map: IM<String, AppConfig> = IM::new();
    apps_map.insert("deep".to_string(), app);
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();

    boot_messaging_with(
        &config,
        init_db_memory(),
        &apps,
        alert_dispatcher,
        "brenn://test",
    )
    .await;
}

/// And the tuning-block existence check is wired in too, at a position where
/// every population it checks against is known.
#[tokio::test]
#[should_panic(expected = "tunes a channel this config never mints")]
async fn build_messaging_refuses_a_tuning_block_that_mints_nothing() {
    use brenn_lib::config::BrennConfig;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let config = BrennConfig {
        channels: vec![tuning_raw("webhook:githbu")],
        ..BrennConfig::default()
    };
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(IM::new());
    let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();

    build_messaging(
        &config,
        init_db_memory(),
        &apps,
        ActiveBridges::new(),
        alert_dispatcher,
        Some(Arc::from("brenn://test")),
        &webhook_endpoint_map("github", "someapp"),
        &[],
        &tuning_for(&config),
        &brenn_lib::mqtt::config::resolve_clients(&config.mqtt_clients),
        &empty_tool_registry(),
    )
    .await;
}

/// Build the tuning table over `raw` against the stock `[messaging]` globals.
fn tuning_of(
    raw: &[brenn_lib::messaging::config::ChannelConfigRaw],
) -> brenn_lib::messaging::config::SystemChannelTuning {
    brenn_lib::messaging::config::build_system_channel_tuning(
        raw,
        &MessagingGlobalConfig::default(),
    )
}

/// A `[[channel]]` block addressing a system-minted channel, with the three
/// depths every tuning block states.
fn tuning_raw(address: &str) -> brenn_lib::messaging::config::ChannelConfigRaw {
    use brenn_lib::messaging::config::Depth;
    brenn_lib::messaging::config::ChannelConfigRaw {
        send_rate: None,
        uuid: None,
        address: Some(address.to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(1)),
        retain_depth: Some(Depth::Bounded(8)),
        standing_retain_depth: Some(Depth::Bounded(8)),
        noise: None,
        sink: None,
        wake_min: None,
    }
}

/// The chat roster: one boot-declared channel per app, carrying a snapshot of
/// that app's conversations published under the reserved writer.
///
/// Two boots over the same database are asserted to produce byte-identical
/// snapshots, which is the property a peer's "compare, then reconcile" loop
/// rests on — anything time-varying in the body (a timestamp, a counter, row
/// order) would make every restart look like a change to every peer.
#[tokio::test]
async fn build_messaging_publishes_a_roster_snapshot_per_app() {
    use brenn_envelope::chat::chat_roster_address;
    use brenn_lib::config::BrennConfig;
    use brenn_server::test_support::init_db_memory;
    use indexmap::IndexMap as IM;

    let owner = "cchost";
    let quiet = "nobodyhome";
    // One unrelated channel brings messaging up; the rosters are derived from
    // the app set, never declared.
    let config = BrennConfig {
        channels: vec![nondurable_channel("ephemeral:inert", 1)],
        ..BrennConfig::default()
    };
    let roster = chat_roster_address(&config.llm_chat.prefix, owner);

    let db = init_db_memory();
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'alice', 'h', '2024-01-01')",
            [],
        )
        .expect("seed user");
        for id in [4_i64, 2] {
            conn.execute(
                "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
                 VALUES (?1, 1, 'active', ?2, '2024-01-01', '2024-01-01')",
                rusqlite::params![id, owner],
            )
            .expect("seed conversation");
        }
    }

    let mut apps_map: IM<String, AppConfig> = IM::new();
    apps_map.insert(owner.to_string(), minimal_app_config(owner, None, vec![]));
    apps_map.insert(quiet.to_string(), minimal_app_config(quiet, None, vec![]));
    let apps: Arc<IndexMap<String, AppConfig>> = Arc::new(apps_map);

    let boot = async |db: brenn_db::Db| {
        let (alert_dispatcher, _alert_join) = AlertDispatcher::noop();
        boot_messaging_with(&config, db, &apps, alert_dispatcher, "brenn://test")
            .await
            .messenger
            .expect("a configured app brings messaging up")
    };

    let messenger = boot(db.clone()).await;
    let entry = messenger
        .directory()
        .resolve(&roster)
        .expect("every configured app gets a roster channel at boot");
    assert!(
        entry.capabilities().durable,
        "the roster is the state a peer reconciles against after an outage"
    );
    assert!(
        messenger
            .directory()
            .resolve(&chat_roster_address(&config.llm_chat.prefix, quiet))
            .is_some(),
        "an app with no conversations still owes a peer the snapshot that says so"
    );

    let first: Vec<(String, String)> = {
        let conn = db.lock().await;
        published_rosters(&conn)
    };
    assert_eq!(
        first,
        vec![
            (
                roster.clone(),
                r#"{"v":1,"conversations":[{"id":2},{"id":4}]}"#.to_string()
            ),
            (
                chat_roster_address(&config.llm_chat.prefix, quiet),
                r#"{"v":1,"conversations":[]}"#.to_string()
            ),
        ],
        "each app's snapshot lists its own conversations, ascending by id"
    );

    // A second boot over the same database: the set is unchanged, so the bytes
    // are too.
    drop(boot(db.clone()).await);
    let second: Vec<(String, String)> = {
        let conn = db.lock().await;
        published_rosters(&conn)
    };
    assert_eq!(
        second,
        [first.clone(), first].concat(),
        "a restart republishes the same snapshot, byte for byte"
    );
}

/// Every roster publish in the database, in publish order, as
/// `(channel address, body)` — and each asserted to carry the reserved writer,
/// so a snapshot from any other principal fails here rather than being counted.
fn published_rosters(conn: &rusqlite::Connection) -> Vec<(String, String)> {
    let mut stmt = conn
        .prepare(
            "SELECT c.address, m.body, m.sender \
             FROM messaging_messages m \
             JOIN messaging_channels c ON c.uuid = m.channel_uuid \
             WHERE c.address LIKE '%.roster' \
             ORDER BY m.id",
        )
        .expect("prepare the roster scan");
    stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })
    .expect("query roster publishes")
    .map(|r| {
        let (address, body, sender) = r.expect("decode a roster publish");
        assert_eq!(
            sender, "system:chat-roster",
            "the roster has exactly one writer"
        );
        (address, body)
    })
    .collect()
}

/// A durable `[[channel]]` block at `address`.
fn durable_channel(address: &str) -> brenn_lib::messaging::config::ChannelConfigRaw {
    brenn_lib::messaging::config::ChannelConfigRaw {
        address: Some(address.to_string()),
        uuid: Some(uuid::Uuid::new_v4().to_string()),
        standing_retain_depth: Some(Depth::Bounded(8)),
        ..nondurable_channel(address, 4)
    }
}

/// One consumer whose io_port mints the auto channel at `address`: the io_port
/// is the spelling that gives an auto channel an address of its own.
fn io_port_at(address: &str) -> brenn_lib::config::BrennConfig {
    use super::test_fixtures::minimal_wasm_consumer;
    use brenn_lib::messaging::config::{WasmConsumerConfigRaw, WasmConsumerIoPortRaw};

    let consumer = WasmConsumerConfigRaw {
        slug: "etl".to_string(),
        grants: vec![ComponentGrant::Ports],
        io_ports: vec![WasmConsumerIoPortRaw {
            port: "loop".to_string(),
            channel: Some(address.to_string()),
            push_depth: Some(Depth::Bounded(2)),
            retain_depth: Some(Depth::Bounded(2)),
            noise: None,
            amplification: None,
            urgency: None,
            publish_per_activation: None,
            publish_capacity: None,
        }],
        ..minimal_wasm_consumer()
    }
    .implying_its_vocabulary();
    brenn_lib::config::BrennConfig {
        wasm_consumers: vec![consumer],
        ..brenn_lib::config::BrennConfig::default()
    }
}

/// `extra_durable_entries` is boot's environment-derived (`webhook:`/`mqtt:`)
/// half of the channel set, and the offline pass's empty vector. The extras must
/// reach both places a declared `[[channel]]` reaches — the durable half and the
/// directory every binding resolves against — or a boot-time binding to a
/// webhook channel stops resolving.
#[test]
fn extra_durable_entries_join_the_durable_half_and_the_directory() {
    let config = brenn_lib::config::BrennConfig {
        channels: vec![
            durable_channel("brenn:declared"),
            nondurable_channel("ephemeral:scratch", 4),
        ],
        ..brenn_lib::config::BrennConfig::default()
    };

    let topology = lower_channel_topology(
        &config,
        vec![super::test_fixtures::brenn_entry("webhook:push-alice")],
    );

    let durable: Vec<&str> = topology
        .durable_entries
        .iter()
        .map(|e| e.address.as_str())
        .collect();
    assert_eq!(durable, vec!["brenn:declared", "webhook:push-alice"]);
    let nondurable: Vec<&str> = topology
        .nondurable_entries
        .iter()
        .map(|e| e.address.as_str())
        .collect();
    assert_eq!(nondurable, vec!["ephemeral:scratch"]);

    let directory = topology.pre_directory();
    for address in ["brenn:declared", "ephemeral:scratch", "webhook:push-alice"] {
        assert!(
            directory.resolve(address).is_some(),
            "{address} must resolve in the pre-directory",
        );
    }
}

/// The extras are in the `declared_addresses` chain too, so an auto channel
/// cannot mint an address a webhook or mqtt channel already owns — the footgun
/// rule, which is otherwise enforced only against `[[channel]]` blocks. The
/// offline pass passes no extras, so this collision is boot's alone to catch.
#[test]
#[should_panic(expected = "is also declared elsewhere")]
fn an_auto_channel_colliding_with_an_extra_entry_panics() {
    let config = io_port_at("brenn:shared");
    lower_channel_topology(
        &config,
        vec![super::test_fixtures::brenn_entry("brenn:shared")],
    );
}

/// The green half of the same wiring: with no extra entry claiming the address,
/// the auto channel is minted and lands in the durable half beside the extras.
#[test]
fn an_auto_channel_lands_in_the_durable_half() {
    let config = io_port_at("brenn:shared");
    let topology = lower_channel_topology(
        &config,
        vec![super::test_fixtures::brenn_entry("webhook:push-alice")],
    );
    let durable: Vec<&str> = topology
        .durable_entries
        .iter()
        .map(|e| e.address.as_str())
        .collect();
    assert_eq!(durable, vec!["webhook:push-alice", "brenn:shared"]);
}

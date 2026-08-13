//! `publish_transport_ingress` tests: the webhook-ingress publish path inserts a
//! pending push row and signals the dispatcher without inline deliver / eager
//! wake (R1), and panics fail-fast on a malformed `WebhookEnvelope` body.
//!
//! Production items are reached via `use super::super::*;` (directly from
//! `publish/mod.rs`); the cross-family shared fixtures (`CountingRouter`,
//! `test_app_config`) are declared `pub(super)` in `tests/mod.rs` and pulled in
//! by the named `use super::{…};` below. `build_webhook_messenger` is used only
//! by this family, so it lives here rather than in the harness.

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::config::{
    Depth, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig, ResolvedSubscription, Sink,
};
use crate::db::init_db_memory;
use crate::db::upsert_channels;
use crate::{
    ChannelEntry, ChannelScheme, MQTT_ADDRESS_PREFIX, MessagingDirectory, MessagingGlobalConfig,
    SubscriberEntry, SubscriberEntryKind, Urgency, WEBHOOK_ADDRESS_PREFIX, WakeMin, WakeRouter,
    mqtt_channel_uuid_from_address, webhook_channel_uuid_from_slug,
};
use indexmap::IndexMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// `cover = true` stamps the covering delivery policy (`Webhook` grant + exact
/// matcher) so the Point-A gate admits the subscriber; `cover = false` leaves the
/// app with no covering policy so the gate denies it (test-2 deny variant).
async fn build_webhook_messenger(
    cover: bool,
) -> (
    Arc<Messenger>,
    Arc<ChannelEntry>,
    brenn_db::Db,
    Arc<CountingRouter>,
) {
    let db = init_db_memory();
    let conn = db.lock().await;
    conn.execute(
        "INSERT INTO users (id, username, password_hash, created_at) \
         VALUES (1, 'alice', 'h', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
         VALUES (1, 1, 'active', 'myapp', '2024-01-01', '2024-01-01')",
        [],
    )
    .unwrap();
    let slug = "wh-test";
    let channel_uuid = webhook_channel_uuid_from_slug(slug);
    let address = format!("{WEBHOOK_ADDRESS_PREFIX}{slug}");
    let entry = Arc::new(ChannelEntry {
        uuid: channel_uuid,
        address: address.clone(),
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
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::App("myapp".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        }],
        transport_type: ChannelScheme::Webhook,
        mount: Some(format!("/webhooks/{slug}")),
    });
    upsert_channels(&conn, std::slice::from_ref(&*entry));
    drop(conn);

    let directory = Arc::new(MessagingDirectory::with_entries(vec![(*entry).clone()]));
    let mut apps_raw: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
    let mut myapp = test_app_config(
        "myapp",
        Some(ResolvedMessagingConfig {
            send_budget: 0, // zero — host-originated must bypass this
            subscriptions: vec![ResolvedSubscription {
                channel_uuid,
                channel_address: address.clone(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            }],
        }),
        vec!["alice".to_string()],
    );
    // Delivery-time gate: a `webhook:` channel needs the
    // `Webhook` grant + an exact `webhook` matcher, which `test_app_config` (brenn
    // only) does not stamp. Omit it for the deny variant (test-2).
    if cover {
        myapp
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::Webhook);
        myapp
            .policy
            .acls
            .webhook
            .push(brenn_lib::access::acl::WebhookMatcher {
                endpoint: slug.to_string(),
            });
    }
    apps_raw.insert("myapp".to_string(), myapp);
    let router = Arc::new(CountingRouter::default());
    let messenger = Messenger::new(
        db.clone(),
        directory,
        Arc::from("webhook-test"),
        Arc::new(apps_raw),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    (messenger, entry, db, router)
}

/// Mirror of `build_webhook_messenger` for an `mqtt:` channel: one MQTT
/// channel with a single app subscriber and a zero send-budget
/// app (host-originated ingress must bypass the budget).
/// `cover = true` stamps the covering delivery policy (`MqttSubscribe` grant +
/// covering matcher); `cover = false` denies via the Point-A gate (test-2).
async fn build_mqtt_messenger(
    cover: bool,
) -> (
    Arc<Messenger>,
    Arc<ChannelEntry>,
    brenn_db::Db,
    Arc<CountingRouter>,
) {
    let db = init_db_memory();
    let conn = db.lock().await;
    conn.execute(
        "INSERT INTO users (id, username, password_hash, created_at) \
         VALUES (1, 'alice', 'h', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
         VALUES (1, 1, 'active', 'myapp', '2024-01-01', '2024-01-01')",
        [],
    )
    .unwrap();
    let address = format!("{MQTT_ADDRESS_PREFIX}homeassistant:home/+/state");
    let channel_uuid = mqtt_channel_uuid_from_address(&address);
    let entry = Arc::new(ChannelEntry {
        uuid: channel_uuid,
        address: address.clone(),
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
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::App("myapp".to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: Some(WakeMin::Normal),
        }],
        transport_type: ChannelScheme::Mqtt,
        mount: None,
    });
    upsert_channels(&conn, std::slice::from_ref(&*entry));
    drop(conn);

    let directory = Arc::new(MessagingDirectory::with_entries(vec![(*entry).clone()]));
    let mut apps_raw: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
    let mut myapp = test_app_config(
        "myapp",
        Some(ResolvedMessagingConfig {
            send_budget: 0, // zero — host-originated must bypass this
            subscriptions: vec![ResolvedSubscription {
                channel_uuid,
                channel_address: address.clone(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            }],
        }),
        vec!["alice".to_string()],
    );
    // Delivery-time gate: an `mqtt:` channel needs the
    // `MqttSubscribe` grant + a covering `mqtt_subscribe` matcher for
    // `(homeassistant, home/+/state)`. Omit it for the deny variant (test-2).
    if cover {
        myapp
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::MqttSubscribe);
        myapp
            .policy
            .acls
            .mqtt_subscribe
            .push(brenn_lib::access::acl::MqttSubMatcher {
                client: "homeassistant".to_string(),
                topic_filter: "home/+/state".to_string(),
            });
    }
    apps_raw.insert("myapp".to_string(), myapp);
    let router = Arc::new(CountingRouter::default());
    let messenger = Messenger::new(
        db.clone(),
        directory,
        Arc::from("mqtt-test"),
        Arc::new(apps_raw),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    (messenger, entry, db, router)
}

/// A structurally valid `MqttEnvelope` body JSON (wire shape pinned by
/// `brenn-envelope`'s `mqtt_envelope_golden_wire_shape`).
const VALID_MQTT_BODY: &str = r#"{"client_slug":"homeassistant","topic":"home/kitchen/state","payload":{"text":"22.5"},"received_at":"2023-11-14T22:13:20Z","qos":1}"#;

/// `publish_transport_ingress` panics when the body is not valid
/// `WebhookEnvelope` JSON — host-bug fail-fast.
#[tokio::test]
#[should_panic(expected = "malformed WebhookEnvelope")]
async fn publish_transport_ingress_panics_on_malformed_envelope() {
    let (messenger, channel, _, _) = build_webhook_messenger(true).await;
    messenger
        .publish_transport_ingress(
            channel,
            "webhook:wh-test",
            "key1",
            "not-valid-json-at-all",
            Urgency::Low,
        )
        .await;
}

/// `publish_transport_ingress` inserts the push row and signals the dispatcher
/// (R1). No inline deliver or eager-wake occurs on the publish call stack.
/// Retained (positioned) message rows on this fixture's single channel.
async fn retained_message_count(messenger: &Arc<Messenger>) -> i64 {
    let conn = messenger.db().lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_messages WHERE retained_seq IS NOT NULL",
        [],
        |r| r.get(0),
    )
    .expect("count retained messages")
}

#[tokio::test]
async fn publish_transport_ingress_commits_without_inline_dispatch() {
    let valid_body = r#"{"headers":[],"key_id":"k","client_ip":"1.2.3.4","received_at":"2024-01-01T00:00:00Z","body":"hi","endpoint_slug":"wh-test"}"#;

    let (messenger, channel, _db, router) = build_webhook_messenger(true).await;
    messenger
        .publish_transport_ingress(
            channel.clone(),
            "webhook:wh-test",
            "k",
            valid_body,
            Urgency::Normal,
        )
        .await;

    // The message is in retention, where every subscriber's position can reach it.
    assert_eq!(
        retained_message_count(&messenger).await,
        1,
        "the message must be committed into retention"
    );
    // No inline router calls — all dispatch is off-stack (R1).
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "publish_transport_ingress must not call spawn_eager_wake inline"
    );
    assert_eq!(
        router.deliveries.lock().await.len(),
        0,
        "publish_transport_ingress must not call deliver inline"
    );
}

/// `publish_transport_ingress` panics when an `Mqtt` channel body is not valid
/// `MqttEnvelope` JSON — host-bug fail-fast.
#[tokio::test]
#[should_panic(expected = "malformed MqttEnvelope")]
async fn publish_transport_ingress_panics_on_malformed_mqtt_envelope() {
    let (messenger, channel, _, _) = build_mqtt_messenger(true).await;
    messenger
        .publish_transport_ingress(
            channel,
            "mqtt:homeassistant:home/+/state",
            "homeassistant",
            "not-valid-json-at-all",
            Urgency::Low,
        )
        .await;
}

/// `publish_transport_ingress` on an `Mqtt` channel stores the row with
/// `envelope_type='mqtt'` and a non-NULL `channel_uuid`, enqueues a per-subscriber
/// pending push, bypasses the send-budget (app has `send_budget: 0`), and does no
/// inline dispatch (R1).
#[tokio::test]
async fn publish_transport_ingress_mqtt_stores_typed_row_and_pending_push() {
    let (messenger, channel, db, router) = build_mqtt_messenger(true).await;
    messenger
        .publish_transport_ingress(
            channel.clone(),
            "mqtt:homeassistant:home/+/state",
            "homeassistant",
            VALID_MQTT_BODY,
            Urgency::Normal,
        )
        .await;

    // The stored message row is typed `mqtt` and carries the channel UUID
    // (non-NULL) — not a legacy `ingress` row.
    {
        let conn = db.lock().await;
        let (envelope_type, channel_uuid_len): (String, i64) = conn
            .query_row(
                "SELECT envelope_type, length(channel_uuid) FROM messaging_messages",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(envelope_type, "mqtt", "row must be typed 'mqtt'");
        assert_eq!(
            channel_uuid_len, 16,
            "channel_uuid must be non-NULL (16-byte UUID)"
        );
    }

    // Committed into retention despite send_budget == 0 (host-originated ingress
    // bypasses the budget).
    assert_eq!(
        retained_message_count(&messenger).await,
        1,
        "the message must be committed into retention"
    );

    // No inline router calls — all dispatch is off-stack (R1).
    assert_eq!(
        router.eager_wakes.load(Ordering::SeqCst),
        0,
        "publish_transport_ingress must not call spawn_eager_wake inline"
    );
    assert_eq!(
        router.deliveries.lock().await.len(),
        0,
        "publish_transport_ingress must not call deliver inline"
    );
}

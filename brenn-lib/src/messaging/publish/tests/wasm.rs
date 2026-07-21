//! WASM push-target resolution, `publish_from_wasm`, and wake_min × eager_wake
//! integration tests (design §2.5 #3, §6 "Push-target resolution", §2.3).

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::db::init_db_memory;
use crate::messaging::config::{
    Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig,
    ResolvedSubscription, Sink,
};
use crate::messaging::db::upsert_channels;
use crate::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind, Urgency,
    WakeMin, WakeRouter, canonical_address,
};
use indexmap::IndexMap;
use std::sync::Arc;
use uuid::Uuid;

/// A `wasm_policies` map authorizing each `slug` to receive on any `brenn:`
/// channel: the `MessagingSubscribe` grant + a universal `brenn_subscribe`
/// matcher. The delivery-time ACL gate (design §2.2 Point A) now denies any
/// `Wasm` subscriber whose policy does not cover the channel, so every test
/// `Messenger` with a `Wasm` subscriber must install a covering policy.
fn wasm_delivery_policies(
    slugs: &[&str],
) -> std::collections::HashMap<String, crate::access::AppPolicy> {
    use crate::access::acl::ChannelMatcher;
    // Shared `test_support` constructor (reuse-1): one universal `brenn:` delivery
    // policy per slug.
    slugs
        .iter()
        .map(|slug| {
            (
                slug.to_string(),
                crate::messaging::test_support::brenn_delivery_policy(ChannelMatcher::Prefix(
                    String::new(),
                )),
            )
        })
        .collect()
}

// -----------------------------------------------------------------------
// Push-target resolution (§2.5 #3 / design §6 "Push-target resolution")
// -----------------------------------------------------------------------

/// `resolve_push_targets` builds a real push target for a `Surface` subscriber
/// whose surface policy authorizes delivery: the target's subscriber is the
/// `surface:<slug>` ParticipantId, keyed on the surface slug (mirroring the Wasm
/// arm), and the subscription's `push_depth` carries through. No config path
/// constructed a Surface directory entry before surface projection, so the entry
/// is hand-built and the covering policy installed via `with_surface_policies`.
#[tokio::test]
async fn resolve_push_targets_surface_builds_target() {
    use crate::access::acl::ChannelMatcher;
    let db = init_db_memory();
    let channel = canonical_address("surface-boot");
    let mut surface_policies = std::collections::HashMap::new();
    surface_policies.insert(
        "deskbar".to_string(),
        crate::messaging::test_support::brenn_delivery_policy(
            ChannelMatcher::Prefix(String::new()),
        ),
    );
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![])),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::surface_registrations(
        surface_policies,
    ));
    let sub = SubscriberEntry {
        kind: SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: None,
        },
        push_depth: Depth::Bounded(8),
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    };
    let conn = db.lock().await;
    let targets = messenger.resolve_push_targets(&conn, &channel, &[sub]);
    assert_eq!(targets.len(), 1);
    assert_eq!(
        targets[0].subscriber.as_str(),
        crate::messaging::ParticipantId::for_surface("deskbar").as_str()
    );
    assert_eq!(targets[0].app_slug, "deskbar");
    assert_eq!(targets[0].push_depth, Depth::Bounded(8));
}

/// A `Surface` subscriber with no resolved surface policy (a host-wiring bug, or
/// a revoked ACL) is skipped fail-closed at the delivery-time ACL gate — no
/// target, no panic, matching the App/Wasm deny behavior.
#[tokio::test]
async fn resolve_push_targets_surface_missing_policy_skips() {
    let db = init_db_memory();
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![])),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    let sub = SubscriberEntry {
        kind: SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: None,
        },
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    };
    let conn = db.lock().await;
    let targets = messenger.resolve_push_targets(&conn, &canonical_address("surface-boot"), &[sub]);
    assert!(targets.is_empty());
}

/// A depth-0 `Surface` subscriber is not a push target but **is** a row-less
/// context-feed target (design §6): `resolve_push_targets` skips it (no row),
/// while `resolve_context_targets` returns its key for the deliver-if-attached
/// feed. A push-enabled surface subscriber is the opposite — a push target, not
/// a context target.
#[tokio::test]
async fn resolve_context_targets_returns_fold_zero_surface_subscribers() {
    use crate::access::acl::ChannelMatcher;
    let db = init_db_memory();
    let channel = canonical_address("surface-boot");
    // Component-instance grain: authority is per-surface, installed at the
    // instance key the fold-0 subscriber carries.
    let policy = crate::messaging::test_support::brenn_delivery_policy(ChannelMatcher::Prefix(
        String::new(),
    ));
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![])),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(
        crate::messaging::testutils::surface_component_registrations(
            "deskbar",
            &["protobar"],
            policy,
        ),
    );
    let fold_zero = SubscriberEntry {
        kind: SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some("protobar".to_string()),
        },
        push_depth: Depth::Bounded(0),
        retain_depth: Depth::Bounded(4),
        noise: NoiseLevel::Silent,
        wake_min: None,
    };
    let conn = db.lock().await;
    // depth-0: no push target, one context target.
    assert!(
        messenger
            .resolve_push_targets(&conn, &channel, std::slice::from_ref(&fold_zero))
            .is_empty()
    );
    let context = messenger.resolve_context_targets(&channel, std::slice::from_ref(&fold_zero));
    assert_eq!(
        context,
        vec![SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some("protobar".to_string()),
        }]
    );

    // push-enabled: a push target, not a context target.
    let push_enabled = SubscriberEntry {
        push_depth: Depth::Bounded(8),
        ..fold_zero.clone()
    };
    assert_eq!(
        messenger
            .resolve_push_targets(&conn, &channel, std::slice::from_ref(&push_enabled))
            .len(),
        1
    );
    assert!(
        messenger
            .resolve_context_targets(&channel, std::slice::from_ref(&push_enabled))
            .is_empty()
    );
}

/// A depth-0 `Surface` subscriber whose policy no longer covers the channel is
/// not a context target — the feed runs the same delivery-time ACL gate as the
/// push path.
#[tokio::test]
async fn resolve_context_targets_skips_a_revoked_surface_subscriber() {
    let db = init_db_memory();
    // No surface policy registered → the ACL gate denies (fail-closed).
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![])),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    let sub = SubscriberEntry {
        kind: SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some("protobar".to_string()),
        },
        push_depth: Depth::Bounded(0),
        retain_depth: Depth::Bounded(4),
        noise: NoiseLevel::Silent,
        wake_min: None,
    };
    assert!(
        messenger
            .resolve_context_targets(&canonical_address("surface-boot"), &[sub])
            .is_empty()
    );
}

/// Build a `Messenger` whose channel has a `Wasm(slug)` subscriber with the
/// given `push_depth`. The `apps` map is empty — there are no app subscribers,
/// so any pending row must come from the WASM path.
async fn build_wasm_messenger(
    wasm_slug: &str,
    push_depth: Depth,
) -> (Arc<Messenger>, Uuid, Arc<CountingRouter>) {
    let db = init_db_memory();
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address("wasm-test-channel");
    let entry = ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(wasm_slug.to_string()),
            push_depth,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
        // Insert a sender user so publish doesn't fail auth.
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'sender', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (1, 1, 'active', 'sender-app', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
    let router = Arc::new(CountingRouter::default());
    // sender app just needs to exist in the apps map with a send budget.
    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        "sender-app".to_string(),
        test_app_config(
            "sender-app",
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            }),
            vec!["sender".to_string()],
        ),
    );
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(apps_raw),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
        wasm_delivery_policies(&[wasm_slug]),
    ));
    (messenger, channel_uuid, router)
}

/// Build a Messenger with a channel that has BOTH a Wasm subscriber and an App
/// subscriber. Returns `(messenger, channel_uuid, router, app_slug)`.
///
/// The App subscriber is `app_slug` with `singleton = true` so `publish()` can
/// call `get_or_create_singleton_conversation` against it. A sender user is seeded
/// in the DB so `publish()` succeeds.
async fn build_wasm_and_app_messenger(
    wasm_slug: &str,
    app_slug: &str,
) -> (Arc<Messenger>, Uuid, Arc<CountingRouter>) {
    let db = init_db_memory();
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address("wasm-app-fanout-ch");
    let sender_app_slug = "sender-app-fanout";
    let entry = ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![
            SubscriberEntry {
                kind: SubscriberEntryKind::Wasm(wasm_slug.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: None,
            },
            SubscriberEntry {
                kind: SubscriberEntryKind::App(app_slug.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            },
        ],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
        // Seed a user and conversation for the sender app.
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'sender-user', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            &format!(
                "INSERT INTO conversations \
                 (id, user_id, status, app_slug, created_at, updated_at) \
                 VALUES (1, 1, 'active', '{sender_app_slug}', '2024-01-01', '2024-01-01')"
            ),
            [],
        )
        .unwrap();
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
    let router = Arc::new(CountingRouter::default());
    // Three apps: sender (for publish auth), wasm is not an app, app_slug (subscriber).
    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        sender_app_slug.to_string(),
        test_app_config(
            sender_app_slug,
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            }),
            vec!["sender-user".to_string()],
        ),
    );
    apps_raw.insert(
        app_slug.to_string(),
        test_app_config(
            app_slug,
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid,
                    channel_address: channel_addr.clone(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                }],
            }),
            vec!["sender-user".to_string()],
        ),
    );
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(apps_raw),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
        wasm_delivery_policies(&[wasm_slug]),
    ));
    (messenger, channel_uuid, router)
}

/// Publishing one `brenn:` message to a channel with BOTH a Wasm and App subscriber
/// must create pending-push rows for both subscribers.
///
/// This is the §6 fan-out AC: "WASM push row created AND conversation push row
/// created/delivered." A regression in `resolve_push_targets` that accidentally
/// skips the `App` arm when a `Wasm` arm is present would fail this test.
#[tokio::test]
async fn wasm_and_app_subscriber_both_get_push_rows() {
    let wasm_slug = "fanout-wasm-consumer";
    let app_slug = "fanout-app";
    let sender_app_slug = "sender-app-fanout";
    let (m, chan_uuid, router) = build_wasm_and_app_messenger(wasm_slug, app_slug).await;

    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            sender_app_slug,
            &canonical_address("wasm-app-fanout-ch"),
            "hello-fanout",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PublishResult::Ok { .. }),
        "publish must succeed, got {result:?}"
    );

    // Wasm subscriber must have a pending-push row.
    let wasm_sub = ParticipantId::for_wasm(wasm_slug);
    let wasm_rows = m.load_pending_pushes(&wasm_sub).await;
    assert_eq!(
        wasm_rows.len(),
        1,
        "Wasm subscriber must get exactly one push row"
    );

    // App subscriber: get_or_create_singleton_conversation was called internally;
    // find the created conversation id (it is conversation id 2 — user 1 already
    // has conversation 1 for sender-app; app_slug gets conversation 2).
    let app_sub = ParticipantId::for_conversation(2);
    let app_rows = m.load_pending_pushes(&app_sub).await;
    assert_eq!(
        app_rows.len(),
        1,
        "App subscriber conversation must get exactly one push row"
    );

    // Publish is off-stack (R1) — no inline eager-wake or deliver calls.
    let wakes = router.eager_wakes.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        wakes, 0,
        "publish must not call spawn_eager_wake inline — dispatch is off-stack (R1)"
    );
    let _ = chan_uuid;
}

/// A `Wasm(slug)` subscriber with `push_depth > 0` resolves to a pending-push
/// row keyed on `for_wasm(slug)` — not touching `self.apps` or
/// `get_or_create_singleton_conversation`.
#[tokio::test]
async fn wasm_subscriber_gets_pending_push_row() {
    let slug = "demo-consumer";
    let (m, _chan_uuid, router) = build_wasm_messenger(slug, Depth::Unbounded).await;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "sender-app",
            "brenn:wasm-test-channel",
            "hello",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PublishResult::Ok { .. }),
        "publish should succeed, got {result:?}"
    );

    // Publish is off-stack (R1) — no inline eager-wake or deliver calls.
    assert_eq!(
        router.eager_wakes.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "publish must not call spawn_eager_wake inline — dispatch is off-stack (R1)"
    );
    // A pending-push row must exist for the wasm: subscriber.
    let wasm_sub = ParticipantId::for_wasm(slug);
    let rows = m.load_pending_pushes(&wasm_sub).await;
    assert_eq!(
        rows.len(),
        1,
        "exactly one pending-push row for the WASM subscriber"
    );
}

/// A `Wasm(slug)` subscriber with `push_depth=0` must never produce a
/// pending-push row (`push_depth=0`-never-a-target guard, design §2.5 #3).
#[tokio::test]
async fn wasm_subscriber_push_depth_zero_yields_no_target() {
    let slug = "no-push";
    let (m, _chan_uuid, router) = build_wasm_messenger(slug, Depth::Bounded(0)).await;
    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "sender-app",
            "brenn:wasm-test-channel",
            "hello",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PublishResult::Ok { .. }),
        "publish should succeed, got {result:?}"
    );

    // push_depth=0 → no target → no eager wake and no pending row.
    assert_eq!(
        router.eager_wakes.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "push_depth=0 WASM subscription must produce no spawn_eager_wake"
    );
    let wasm_sub = ParticipantId::for_wasm(slug);
    let rows = m.load_pending_pushes(&wasm_sub).await;
    assert!(
        rows.is_empty(),
        "push_depth=0 must produce no pending-push row"
    );
}

// -----------------------------------------------------------------------
// publish_from_wasm tests
// -----------------------------------------------------------------------

/// Build a Messenger with a `brenn:` output channel that has one Wasm subscriber.
/// Used by `publish_from_wasm` tests to verify the flush path in isolation.
async fn build_wasm_output_messenger(
    consumer_slug: &str,
) -> (Arc<Messenger>, String, Arc<CountingRouter>) {
    let db = init_db_memory();
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address("wasm-output-ch");
    let subscriber_slug = "wasm-output-receiver";
    let entry = ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Wasm(subscriber_slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
    let router = Arc::new(CountingRouter::default());
    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        consumer_slug.to_string(),
        test_app_config(
            consumer_slug,
            Some(ResolvedMessagingConfig {
                send_budget: 0,
                subscriptions: vec![],
            }),
            vec![],
        ),
    );
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(apps_raw),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
        wasm_delivery_policies(&[subscriber_slug]),
    ));
    (messenger, channel_addr, router)
}

/// `publish_from_wasm` with two publishes in one call inserts both rows with
/// correct sender, envelope_type, wake, and strictly increasing publish_ts_ns.
#[tokio::test]
async fn publish_from_wasm_two_publishes_correct_fields() {
    let consumer_slug = "wasm-flusher";
    let (m, channel_addr, _router) = build_wasm_output_messenger(consumer_slug).await;
    let receiver_sub = ParticipantId::for_wasm("wasm-output-receiver");

    let publishes = vec![
        WasmPublish {
            channel_address: &channel_addr,
            body: "msg-a",
            urgency: Urgency::Normal,
            reply_to: None,
        },
        WasmPublish {
            channel_address: &channel_addr,
            body: "msg-b",
            urgency: Urgency::Normal,
            reply_to: None,
        },
    ];
    m.publish_from_wasm(consumer_slug, &publishes).await;

    let rows = m.load_pending_pushes(&receiver_sub).await;
    assert_eq!(rows.len(), 2, "both publishes must land as push rows");

    // Load message rows from the DB to check field values.
    let conn = m.db().lock().await;
    let mut stmt = conn
        .prepare(
            "SELECT sender, envelope_type, urgency, publish_ts_ns, body \
             FROM messaging_messages ORDER BY publish_ts_ns ASC",
        )
        .unwrap();
    let rows_raw: Vec<(String, String, String, i64, String)> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?,
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows_raw.len(), 2, "two message rows");
    let expected_sender = format!("wasm:{consumer_slug}");
    for (sender, envelope_type, urgency, _, _) in &rows_raw {
        assert_eq!(sender, &expected_sender, "sender must be wasm:<slug>");
        assert_eq!(envelope_type, "brenn", "envelope_type must be brenn");
        assert_eq!(
            urgency, "normal",
            "urgency must be normal (WasmPublish.urgency = Normal in this test)"
        );
    }
    // Strictly increasing publish_ts_ns.
    assert!(
        rows_raw[0].3 < rows_raw[1].3,
        "publish_ts_ns must be strictly increasing: {} >= {}",
        rows_raw[0].3,
        rows_raw[1].3
    );
    // Bodies in call order.
    assert_eq!(rows_raw[0].4, "msg-a");
    assert_eq!(rows_raw[1].4, "msg-b");
}

/// `publish_from_wasm` with an empty slice is a no-op (no rows, no panic).
#[tokio::test]
async fn publish_from_wasm_empty_slice_noop() {
    let consumer_slug = "wasm-noop";
    let (m, _, _router) = build_wasm_output_messenger(consumer_slug).await;
    let receiver_sub = ParticipantId::for_wasm("wasm-output-receiver");

    m.publish_from_wasm(consumer_slug, &[]).await;

    let rows = m.load_pending_pushes(&receiver_sub).await;
    assert!(rows.is_empty(), "no rows on empty publish slice");
}

// -----------------------------------------------------------------------
// wake_min × eager_wake integration (urgency-redesign §2.3)
// -----------------------------------------------------------------------

/// Build a Messenger where one WASM and one App subscriber share a channel,
/// but with different `wake_min` policies. Returns (messenger, channel_uuid, router).
async fn build_mixed_wake_min_messenger(
    // The WASM subscriber is `Eager`, so its directory entry carries no
    // threshold; the parameter is retained for call-site symmetry only.
    _wasm_wake_min: WakeMin,
    app_wake_min: WakeMin,
) -> (Arc<Messenger>, Uuid, Arc<CountingRouter>) {
    let db = init_db_memory();
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address("wake-min-fanout-ch");
    let sender_slug = "wake-min-sender";
    let app_slug = "wake-min-app";
    let wasm_slug = "wake-min-wasm";
    let entry = ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![
            SubscriberEntry {
                kind: SubscriberEntryKind::Wasm(wasm_slug.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: None,
            },
            SubscriberEntry {
                kind: SubscriberEntryKind::App(app_slug.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(app_wake_min),
            },
        ],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&entry));
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'test-user', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            &format!(
                "INSERT INTO conversations \
                 (id, user_id, status, app_slug, created_at, updated_at) \
                 VALUES (1, 1, 'active', '{sender_slug}', '2024-01-01', '2024-01-01')"
            ),
            [],
        )
        .unwrap();
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
    let router = Arc::new(CountingRouter::default());
    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        sender_slug.to_string(),
        test_app_config(
            sender_slug,
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            }),
            vec!["test-user".to_string()],
        ),
    );
    apps_raw.insert(
        app_slug.to_string(),
        test_app_config(
            app_slug,
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid,
                    channel_address: channel_addr.clone(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: app_wake_min,
                }],
            }),
            vec!["test-user".to_string()],
        ),
    );
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(apps_raw),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
        wasm_delivery_policies(&[wasm_slug]),
    ));
    (messenger, channel_uuid, router)
}

/// Mixed-subscriber matrix: one `Normal` message, two subscribers with different
/// wake economics. The WASM subscriber is `Eager` (always woken); the App
/// subscriber is `UrgencyGated` with `wake_min=High` (parks on a Normal message,
/// since Normal < High). Proves `eager_wake` is computed per participant.
///
/// Expected: wasm row eager_wake=true (Eager), app row eager_wake=false (gated).
#[tokio::test]
async fn insert_pushes_mixed_wake_min_computes_eager_wake_per_subscriber() {
    // wasm: Eager → always eager_wake=true (its wake_min is ignored)
    // app:  UrgencyGated, wake_min=High → Normal >= High is false → eager_wake=false
    let (m, _chan_uuid, _router) =
        build_mixed_wake_min_messenger(WakeMin::Low, WakeMin::High).await;

    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "wake-min-sender",
            "brenn:wake-min-fanout-ch",
            "test body",
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::Ok { .. }), "{result:?}");

    // Load pending rows for each subscriber and verify eager_wake.
    // Query eager_wake directly from DB for both subscribers.
    let conn = m.db.lock().await;
    let wasm_rows: Vec<bool> = conn
        .prepare(
            "SELECT pp.eager_wake FROM messaging_pending_pushes pp \
             WHERE pp.target_subscriber = 'wasm:wake-min-wasm' AND pp.delivered_at IS NULL",
        )
        .unwrap()
        .query_map([], |r| r.get::<_, bool>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let app_rows: Vec<bool> = conn
        .prepare(
            "SELECT pp.eager_wake FROM messaging_pending_pushes pp \
             WHERE pp.target_app_slug = 'wake-min-app' AND pp.delivered_at IS NULL",
        )
        .unwrap()
        .query_map([], |r| r.get::<_, bool>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    drop(conn);
    assert_eq!(wasm_rows.len(), 1, "wasm subscriber must have one push row");
    assert!(
        wasm_rows[0],
        "wasm (wake_min=Low) must have eager_wake=true for Normal-urgency message"
    );
    assert_eq!(app_rows.len(), 1, "app subscriber must have one push row");
    assert!(
        !app_rows[0],
        "app (wake_min=High) must have eager_wake=false for Normal-urgency message"
    );
}

/// A WASM subscriber is `Eager`: its push row is created eager regardless of the
/// subscription's `wake_min` (waking a parked consumer is cheap, and the notify
/// is itself the delivery trigger). Even with `wake_min=Never` on the entry and
/// the lowest urgency, the row is eager — parking it would strand the consumer's
/// delivery until an unrelated drain, the same class of bug as 5.2.
#[tokio::test]
async fn insert_pushes_wasm_is_eager_ignoring_wake_min() {
    let (m, _chan_uuid, _router) =
        build_mixed_wake_min_messenger(WakeMin::Never, WakeMin::Never).await;

    let result = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "wake-min-sender",
            "brenn:wake-min-fanout-ch",
            "test body",
            Urgency::VeryLow,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PublishResult::Ok { .. }), "{result:?}");

    let conn = m.db.lock().await;
    let wasm_eager_wake: Vec<bool> = conn
        .prepare(
            "SELECT pp.eager_wake FROM messaging_pending_pushes pp \
             WHERE pp.target_subscriber = 'wasm:wake-min-wasm' AND pp.delivered_at IS NULL",
        )
        .unwrap()
        .query_map([], |r| r.get::<_, bool>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    drop(conn);
    assert_eq!(wasm_eager_wake.len(), 1);
    assert!(
        wasm_eager_wake[0],
        "an Eager WASM subscriber wakes on every publish — wake_min=Never is ignored"
    );
}

/// `publish_from_wasm` panics if the channel address is not in the directory.
#[tokio::test]
#[should_panic(expected = "not in directory")]
async fn publish_from_wasm_unknown_channel_panics() {
    let consumer_slug = "wasm-bad-ch";
    let (m, _, _router) = build_wasm_output_messenger(consumer_slug).await;
    let bad_publish = WasmPublish {
        channel_address: "brenn:nonexistent-channel",
        body: "x",
        urgency: Urgency::Normal,
        reply_to: None,
    };
    m.publish_from_wasm(consumer_slug, &[bad_publish]).await;
}

/// `publish_from_wasm` resolves a `reply_to` address to the channel's UUID and
/// stores it on the message row (the async tool-request path). Reuses the output
/// channel as the reply target — it exists in the directory, so resolution
/// succeeds and the stored `reply_to_uuid` matches it.
#[tokio::test]
async fn publish_from_wasm_reply_to_resolves_to_channel_uuid() {
    let consumer_slug = "wasm-reply";
    let (m, channel_addr, _router) = build_wasm_output_messenger(consumer_slug).await;

    let expected_uuid = m
        .directory()
        .resolve(&channel_addr)
        .expect("output channel resolves")
        .uuid;

    let publishes = vec![WasmPublish {
        channel_address: &channel_addr,
        body: "req",
        urgency: Urgency::Normal,
        reply_to: Some(&channel_addr),
    }];
    m.publish_from_wasm(consumer_slug, &publishes).await;

    let conn = m.db().lock().await;
    let stored: Vec<u8> = conn
        .query_row(
            "SELECT reply_to_uuid FROM messaging_messages LIMIT 1",
            [],
            |r| r.get::<_, Option<Vec<u8>>>(0),
        )
        .unwrap()
        .expect("reply_to_uuid must be set");
    assert_eq!(
        stored,
        expected_uuid.as_bytes().to_vec(),
        "stored reply_to_uuid must be the resolved channel UUID"
    );
}

/// A `reply_to` address absent from the directory is a host-wiring bug — fail fast.
#[tokio::test]
#[should_panic(expected = "reply_to channel")]
async fn publish_from_wasm_unknown_reply_to_panics() {
    let consumer_slug = "wasm-bad-reply";
    let (m, channel_addr, _router) = build_wasm_output_messenger(consumer_slug).await;
    let publishes = vec![WasmPublish {
        channel_address: &channel_addr,
        body: "req",
        urgency: Urgency::Normal,
        reply_to: Some("brenn:tool-results/nonexistent"),
    }];
    m.publish_from_wasm(consumer_slug, &publishes).await;
}

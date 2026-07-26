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
// Surface-feed target resolution
// -----------------------------------------------------------------------

/// Every `Surface` subscriber is a live-feed target at whatever depth it
/// subscribes — a surface holds no position, so the fan-out at commit is what
/// serves it. Depth is a property of the target, not a filter: it decides only
/// what a *detached* session can recover afterwards.
#[tokio::test]
async fn surface_feed_targets_cover_both_depths() {
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
    // depth-0: a feed target that is live-or-nothing.
    let feed = messenger.resolve_surface_feed_targets(&channel, std::slice::from_ref(&fold_zero));
    assert_eq!(feed.len(), 1);
    assert_eq!(
        feed[0].kind,
        SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some("protobar".to_string()),
        }
    );
    assert_eq!(feed[0].subscriber.as_str(), "surface:deskbar#protobar");
    assert!(!feed[0].push_enabled, "depth-0 is live-or-nothing");

    // push-enabled: a feed target that can resume.
    let push_enabled = SubscriberEntry {
        push_depth: Depth::Bounded(8),
        ..fold_zero.clone()
    };
    let feed =
        messenger.resolve_surface_feed_targets(&channel, std::slice::from_ref(&push_enabled));
    assert_eq!(feed.len(), 1);
    assert!(feed[0].push_enabled);
}

/// A `Surface` subscriber whose policy no longer covers the channel is not a
/// feed target — the fan-out runs the delivery-time ACL gate.
#[tokio::test]
async fn surface_feed_targets_skip_a_revoked_surface_subscriber() {
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
            .resolve_surface_feed_targets(&canonical_address("surface-boot"), &[sub])
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
            send_rate: Default::default(),
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
    // A subscriber holds a position or it is owed nothing; a sampled attach is
    // where the demotion rule removes one.
    messenger
        .attach_subscriber(
            &canonical_address("wasm-test-channel"),
            wasm_slug,
            &crate::messaging::ParticipantId::for_wasm(wasm_slug),
            push_depth,
            crate::messaging::store::Priming::Head,
        )
        .await;
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
            send_rate: Default::default(),
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
    // A subscriber holds a position or it is owed nothing: boot attaches both
    // kinds, and so does this fixture.
    messenger.attach_conversation_subscribers().await;
    messenger
        .attach_subscriber(
            &channel_addr,
            wasm_slug,
            &crate::messaging::ParticipantId::for_wasm(wasm_slug),
            Depth::Unbounded,
            crate::messaging::store::Priming::Head,
        )
        .await;
    (messenger, channel_uuid, router)
}

/// Publishing one `brenn:` message to a channel with BOTH a Wasm and App
/// subscriber leaves both of their positions trailing retention.
///
/// A commit is target-blind, so the fan-out is the *reads*: a regression that
/// wrote one subscriber's position past the commit — or failed to give one a
/// position at all — would fail here.
#[tokio::test]
async fn wasm_and_app_subscriber_are_both_owed_the_message() {
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

    // What each is owed, not merely that it is owed something: a commit that
    // positioned one of them against the wrong message would satisfy a
    // has-something probe.
    let wasm_owed =
        crate::messaging::testutils::owed_everywhere(&m, &ParticipantId::for_wasm(wasm_slug)).await;
    assert_eq!(
        wasm_owed
            .iter()
            .map(|(_, e)| e.body.as_str())
            .collect::<Vec<_>>(),
        vec!["hello-fanout"],
        "the Wasm subscriber must be owed the message just published"
    );
    // The App subscriber's conversation is the one the boot attach minted: user 1
    // already holds conversation 1 for the sender app, so the app gets id 2.
    let app_owed =
        crate::messaging::testutils::owed_everywhere(&m, &ParticipantId::for_conversation(2)).await;
    assert_eq!(
        app_owed
            .iter()
            .map(|(_, e)| e.body.as_str())
            .collect::<Vec<_>>(),
        vec!["hello-fanout"],
        "the App subscriber's conversation must be owed the same message"
    );

    // Publish is off-stack (R1) — no inline eager-wake or deliver calls.
    let wakes = router.eager_wakes.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        wakes, 0,
        "publish must not call spawn_eager_wake inline — dispatch is off-stack (R1)"
    );
    let _ = chan_uuid;
}

/// A `Wasm(slug)` subscriber with `push_depth > 0` is owed the commit through its
/// own position, keyed on `for_wasm(slug)` — not touching `self.apps` or
/// `get_or_create_singleton_conversation`.
#[tokio::test]
async fn wasm_subscriber_is_owed_the_published_message() {
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
    let owed =
        crate::messaging::testutils::owed_everywhere(&m, &ParticipantId::for_wasm(slug)).await;
    assert_eq!(
        owed.iter()
            .map(|(_, e)| e.body.as_str())
            .collect::<Vec<_>>(),
        vec!["hello"],
        "the WASM subscriber must be owed the message just published"
    );
}

/// A sampled (`push_depth=0`) `Wasm(slug)` subscriber wakes nobody on a commit:
/// the channel is visibility-only for it. Its owing nothing follows from the
/// attach — a sampled subscription writes no position — so what the publish
/// itself is on trial for here is the wake, and the nothing-owed assertion is
/// the standing property that makes the missing wake correct rather than a bug.
#[tokio::test]
async fn wasm_subscriber_push_depth_zero_is_owed_nothing() {
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

    // Sampled → no position → no eager wake and nothing owed.
    assert_eq!(
        router.eager_wakes.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "push_depth=0 WASM subscription must produce no spawn_eager_wake"
    );
    assert!(
        !m.store_for_address("brenn:wasm-test-channel")
            .has_deliverable(&ParticipantId::for_wasm(slug))
            .await,
        "a sampled subscriber is owed nothing"
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
            send_rate: Default::default(),
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
    // The receiver holds a position from here on, as boot would give it: without
    // one it is owed nothing whatever the flush commits, and a case asking what
    // the flush left it owed would read empty for the wrong reason.
    messenger
        .attach_subscriber(
            &channel_addr,
            subscriber_slug,
            &ParticipantId::for_wasm(subscriber_slug),
            Depth::Unbounded,
            crate::messaging::store::Priming::Head,
        )
        .await;
    (messenger, channel_addr, router)
}

/// `publish_from_wasm` with two publishes in one call inserts both rows with
/// correct sender, envelope_type, wake, and strictly increasing publish_ts_ns.
#[tokio::test]
async fn publish_from_wasm_two_publishes_correct_fields() {
    let consumer_slug = "wasm-flusher";
    let (m, channel_addr, _router) = build_wasm_output_messenger(consumer_slug).await;

    let publishes = vec![
        WasmPublish {
            channel_address: &channel_addr,
            body: "msg-a",
            urgency: Urgency::Normal,
            reply_to: None,
            deliver_after: None,
        },
        WasmPublish {
            channel_address: &channel_addr,
            body: "msg-b",
            urgency: Urgency::Normal,
            reply_to: None,
            deliver_after: None,
        },
    ];
    m.publish_from_wasm(consumer_slug, &publishes).await;

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
    drop(stmt);
    drop(conn);

    // And both reach the channel's subscriber: the rows above pin the commit,
    // this pins that the commit left the receiver owed them, in order.
    let owed = crate::messaging::testutils::owed_everywhere(
        &m,
        &ParticipantId::for_wasm("wasm-output-receiver"),
    )
    .await;
    assert_eq!(
        owed.iter()
            .map(|(_, e)| e.body.as_str())
            .collect::<Vec<_>>(),
        vec!["msg-a", "msg-b"],
        "the subscriber is owed both publishes, oldest first"
    );
}

/// Build a Messenger with a durable `brenn:` output channel and a non-durable
/// `ephemeral:` output channel, wired with ring stores and bus.
async fn build_wasm_mixed_output_messenger(
    consumer_slug: &str,
) -> (Arc<Messenger>, String, String) {
    use crate::messaging::store::RingStores;
    use crate::messaging::testutils::ephemeral_channel_entry;

    let db = init_db_memory();
    let subscriber_slug = "wasm-output-receiver";
    let durable_addr = canonical_address("wasm-output-ch");
    let durable = ChannelEntry {
        uuid: Uuid::new_v4(),
        address: durable_addr.clone(),
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
            kind: SubscriberEntryKind::Wasm(subscriber_slug.to_string()),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    let ephemeral = ephemeral_channel_entry("wasm-eph-out", 8);
    let ephemeral_addr = ephemeral.address.clone();
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&durable));
    }
    let nondurable = [ephemeral.clone()];
    let directory = Arc::new(MessagingDirectory::with_entries(vec![durable, ephemeral]));
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
    let stores = Arc::new(RingStores::build(&nondurable));
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test"),
        Arc::new(apps_raw),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
        wasm_delivery_policies(&[subscriber_slug]),
    ))
    .with_ring_stores(stores);
    // As above: the durable half's subscriber is positioned, so what the flush
    // leaves it owed is a question with a meaningful answer.
    messenger
        .attach_subscriber(
            &durable_addr,
            subscriber_slug,
            &ParticipantId::for_wasm(subscriber_slug),
            Depth::Unbounded,
            crate::messaging::store::Priming::Head,
        )
        .await;
    (messenger, durable_addr, ephemeral_addr)
}

/// A WASM output port bound to an `ephemeral:` channel publishes through the
/// unified commit: the message lands in the channel's ring store (no DB row) and
/// is fanned out to attached wire receivers.
#[tokio::test]
async fn publish_from_wasm_ephemeral_output_lands_in_the_ring() {
    let consumer_slug = "wasm-eph-flusher";
    let (m, _durable, ephemeral_addr) = build_wasm_mixed_output_messenger(consumer_slug).await;

    m.publish_from_wasm(
        consumer_slug,
        &[WasmPublish {
            channel_address: &ephemeral_addr,
            body: "eph-body",
            urgency: Urgency::Normal,
            reply_to: None,
            deliver_after: None,
        }],
    )
    .await;

    let store = m
        .ring_stores()
        .get_by_address("ephemeral:wasm-eph-out")
        .expect("registered ring channel")
        .clone();
    let retained = store.retained_tail(10);
    assert_eq!(retained.len(), 1, "ephemeral publish must land in the ring");
    assert_eq!(retained[0].envelope.body, "eph-body");
    assert_eq!(
        retained[0].envelope.sender,
        format!("wasm:{consumer_slug}"),
        "sender must be wasm:<slug>"
    );
    assert_eq!(
        retained[0].envelope.envelope_type,
        ChannelScheme::Ephemeral,
        "envelope_type follows the target scheme"
    );

    // Nothing durable was written.
    let conn = m.db().lock().await;
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "an ephemeral output writes no DB row");
}

/// A single flush mixing a durable and an ephemeral output commits both halves:
/// the durable one into the channel's retention, where its subscriber is left
/// owed it, the ephemeral one into the ring.
#[tokio::test]
async fn publish_from_wasm_mixed_batch_commits_both_halves() {
    let consumer_slug = "wasm-mixed-flusher";
    let (m, durable_addr, ephemeral_addr) = build_wasm_mixed_output_messenger(consumer_slug).await;

    m.publish_from_wasm(
        consumer_slug,
        &[
            WasmPublish {
                channel_address: &durable_addr,
                body: "durable-body",
                urgency: Urgency::Normal,
                reply_to: None,
                deliver_after: None,
            },
            WasmPublish {
                channel_address: &ephemeral_addr,
                body: "eph-body",
                urgency: Urgency::Normal,
                reply_to: None,
                deliver_after: None,
            },
        ],
    )
    .await;

    // Durable half: one DB message row, and the channel's subscriber owed it.
    {
        let conn = m.db().lock().await;
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "only the durable output writes a DB row");
    }
    let owed = crate::messaging::testutils::owed_everywhere(
        &m,
        &ParticipantId::for_wasm("wasm-output-receiver"),
    )
    .await;
    assert_eq!(
        owed.iter()
            .map(|(_, e)| e.body.as_str())
            .collect::<Vec<_>>(),
        vec!["durable-body"],
        "the durable half leaves its subscriber owed that message and no other"
    );

    // Ephemeral half: one ring entry, no DB row.
    let store = m
        .ring_stores()
        .get_by_address("ephemeral:wasm-eph-out")
        .expect("registered ring channel")
        .clone();
    let retained = store.retained_tail(10);
    assert_eq!(retained.len(), 1, "the ephemeral output lands in the ring");
    assert_eq!(retained[0].envelope.body, "eph-body");
}

/// A WASM ephemeral output with a future `deliver_after` parks in the channel's
/// ring: it is not observable in the retained tail until the store releases it at
/// its due time.
#[tokio::test]
async fn publish_from_wasm_ephemeral_deferred_parks_then_releases() {
    let consumer_slug = "wasm-eph-defer";
    let (m, _durable, ephemeral_addr) = build_wasm_mixed_output_messenger(consumer_slug).await;
    let release_at = chrono::Utc::now() + chrono::Duration::seconds(60);

    m.publish_from_wasm(
        consumer_slug,
        &[WasmPublish {
            channel_address: &ephemeral_addr,
            body: "later",
            urgency: Urgency::Normal,
            reply_to: None,
            deliver_after: Some(release_at),
        }],
    )
    .await;

    let store = m
        .ring_stores()
        .get_by_address("ephemeral:wasm-eph-out")
        .expect("registered ring channel")
        .clone();
    // Parked: nothing retained yet, but a release deadline exists.
    assert!(
        store.retained_tail(10).is_empty(),
        "a deferred publish is not observable before release"
    );
    assert!(
        store.next_release().is_some(),
        "the parked message has a deadline"
    );

    // Released at its due time: it lands in the ring.
    let released = store.release_due(release_at);
    assert_eq!(
        released.messages.len(),
        1,
        "the message releases at its due time"
    );
    let retained = store.retained_tail(10);
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].envelope.body, "later");
}

/// A WASM ephemeral output whose `deliver_after` is already in the past commits
/// immediately — the past instant is treated exactly like no deferral.
#[tokio::test]
async fn publish_from_wasm_ephemeral_past_deliver_after_is_immediate() {
    let consumer_slug = "wasm-eph-past";
    let (m, _durable, ephemeral_addr) = build_wasm_mixed_output_messenger(consumer_slug).await;

    m.publish_from_wasm(
        consumer_slug,
        &[WasmPublish {
            channel_address: &ephemeral_addr,
            body: "now",
            urgency: Urgency::Normal,
            reply_to: None,
            deliver_after: Some(chrono::Utc::now() - chrono::Duration::seconds(60)),
        }],
    )
    .await;

    let store = m
        .ring_stores()
        .get_by_address("ephemeral:wasm-eph-out")
        .expect("registered ring channel")
        .clone();
    let retained = store.retained_tail(10);
    assert_eq!(
        retained.len(),
        1,
        "a past deliver_after publishes immediately"
    );
    assert!(store.next_release().is_none(), "nothing is parked");
}

/// A WASM durable output with a future `deliver_after` parks the durable row:
/// the message row carries `deliver_after`, so a retention read (which filters
/// `deliver_after IS NULL`) does not surface it before release.
#[tokio::test]
async fn publish_from_wasm_durable_deferred_parks_the_row() {
    let consumer_slug = "wasm-dur-defer";
    let (m, durable_addr, _ephemeral) = build_wasm_mixed_output_messenger(consumer_slug).await;
    let release_at = chrono::Utc::now() + chrono::Duration::seconds(60);

    m.publish_from_wasm(
        consumer_slug,
        &[WasmPublish {
            channel_address: &durable_addr,
            body: "durable-later",
            urgency: Urgency::Normal,
            reply_to: None,
            deliver_after: Some(release_at),
        }],
    )
    .await;

    let conn = m.db().lock().await;
    // The row exists and is parked: its deliver_after is set (not NULL).
    let parked: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE deliver_after IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(parked, 1, "a deferred durable output parks its message row");
}

/// `publish_from_wasm` with an empty slice is a no-op (no rows, no panic).
#[tokio::test]
async fn publish_from_wasm_empty_slice_noop() {
    let consumer_slug = "wasm-noop";
    let (m, _, _router) = build_wasm_output_messenger(consumer_slug).await;

    m.publish_from_wasm(consumer_slug, &[]).await;

    let conn = m.db().lock().await;
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "no rows on empty publish slice");
}

// -----------------------------------------------------------------------
// wake_min × eager_wake integration (urgency-redesign §2.3)
// -----------------------------------------------------------------------

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
        deliver_after: None,
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
        deliver_after: None,
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
        deliver_after: None,
    }];
    m.publish_from_wasm(consumer_slug, &publishes).await;
}

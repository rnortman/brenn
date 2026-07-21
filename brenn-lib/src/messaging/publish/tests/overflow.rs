//! push_depth / push-window overflow tests (design §2.4, §2.8).

use super::super::*;
use super::{CountingRouter, build_messenger, test_app_config};
use crate::db::init_db_memory;
use crate::messaging::config::{
    Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, ResolvedMessagingConfig,
    ResolvedSubscription, Sink,
};
use crate::messaging::db::{
    PendingPushInsert, insert_message_with_pushes, upsert_channels, utc_to_ns,
};
use crate::messaging::{
    ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry, SubscriberEntryKind, Urgency,
    WakeMin, WakeRouter, canonical_address,
};
use chrono::Utc;
use indexmap::IndexMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use uuid::Uuid;

// -----------------------------------------------------------------------
// push_depth overflow tests (design §2.4, §2.8)
//
// Verify that bounded-push_depth subscribers see push-claim retirement
// when the window overflows, and that noise counters/alarms fire correctly.
// -----------------------------------------------------------------------

/// Build a messenger with a bounded-push_depth subscriber (pa-alice, depth=k)
/// and a counting router. Returns (messenger, channel_address, router).
async fn build_bounded_messenger(
    push_depth: u64,
    noise: NoiseLevel,
) -> (Arc<Messenger>, String, Arc<CountingRouter>) {
    let db = init_db_memory();
    let conn = db.lock().await;
    conn.execute(
        "INSERT INTO users (id, username, password_hash, created_at) \
         VALUES (1, 'bob', 'h', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO users (id, username, password_hash, created_at) \
         VALUES (2, 'alice', 'h', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
         VALUES (1, 1, 'active', 'pa-bob', '2024-01-01', '2024-01-01')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
         VALUES (2, 2, 'active', 'pa-alice', '2024-01-01', '2024-01-01')",
        [],
    )
    .unwrap();
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address("pa-alice");
    let sub_depth = Depth::Bounded(push_depth);
    let entry = ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: sub_depth,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::App("pa-alice".to_string()),
            push_depth: sub_depth,
            retain_depth: Depth::Unbounded,
            // LOAD-BEARING: must match ResolvedSubscription.noise above.
            // resolve_push_targets reads sub_entry.noise, not
            // ResolvedSubscription.noise; these must match or the
            // Metered/Alarm overflow tests break silently.
            noise,
            wake_min: Some(WakeMin::Normal),
        }],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    };
    upsert_channels(&conn, std::slice::from_ref(&entry));
    drop(conn);

    let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));

    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        "pa-bob".to_string(),
        test_app_config(
            "pa-bob",
            Some(ResolvedMessagingConfig {
                send_budget: 10000,
                subscriptions: vec![],
            }),
            vec!["bob".to_string()],
        ),
    );
    apps_raw.insert(
        "pa-alice".to_string(),
        test_app_config(
            "pa-alice",
            Some(ResolvedMessagingConfig {
                send_budget: 10000,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid,
                    channel_address: channel_addr.clone(),
                    push_depth: sub_depth,
                    retain_depth: Depth::Unbounded,
                    noise,
                    wake_min: WakeMin::Normal,
                }],
            }),
            vec!["alice".to_string()],
        ),
    );
    let apps = Arc::new(apps_raw);

    let router = Arc::new(CountingRouter::default());
    let messenger = Messenger::new(
        db.clone(),
        directory,
        Arc::from("test-source"),
        apps,
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    (messenger, channel_addr, router)
}

/// Count total bus push rows for a given conversation.
async fn count_bus_push_rows(messenger: &Arc<Messenger>, conversation_id: i64) -> i64 {
    let conn = messenger.db.lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_pending_pushes pp
         JOIN messaging_messages m ON pp.message_id = m.id
         WHERE pp.target_subscriber = ?1
           AND m.envelope_type = 'brenn'",
        rusqlite::params![
            crate::messaging::ParticipantId::for_conversation(conversation_id).as_str()
        ],
        |row| row.get(0),
    )
    .unwrap()
}

/// push_depth=0 subscriber: no pending_push row is ever created.
#[tokio::test]
async fn push_depth_zero_creates_no_push_rows() {
    let (m, channel_addr, _router) = build_bounded_messenger(0, NoiseLevel::Silent).await;
    // pa-bob publishes to pa-alice's channel (which has push_depth=0).
    let _ = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &channel_addr,
            "msg1",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    // No push rows for pa-alice (conversation 2).
    assert_eq!(count_bus_push_rows(&m, 2).await, 0);
}

/// Bounded push_depth=2: after 3 publishes, exactly push_depth (2) push rows exist.
/// The oldest one was retired when the 3rd arrived.
#[tokio::test]
async fn bounded_push_depth_retires_oldest_on_overflow() {
    let push_depth = 2;
    let (m, channel_addr, _router) = build_bounded_messenger(push_depth, NoiseLevel::Silent).await;

    for i in 0..3u32 {
        let _ = m
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "pa-bob",
                &channel_addr,
                &format!("msg{i}"),
                Urgency::Low,
                None,
                None,
                None,
            )
            .await;
    }
    // After 3 publishes with push_depth=2, exactly 2 push rows should exist.
    let rows = count_bus_push_rows(&m, 2).await;
    assert_eq!(
        rows, push_depth as i64,
        "expected exactly push_depth={push_depth} push rows, got {rows}"
    );
}

/// silent noise: push-overflow increments nothing.
#[tokio::test]
async fn push_overflow_silent_no_counter() {
    let (m, channel_addr, router) = build_bounded_messenger(1, NoiseLevel::Silent).await;
    let subscriber = crate::messaging::ParticipantId::for_conversation(2);
    // 2 publishes to a depth-1 subscriber → 1 overflow.
    for i in 0..2u32 {
        let _ = m
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "pa-bob",
                &channel_addr,
                &format!("m{i}"),
                Urgency::Low,
                None,
                None,
                None,
            )
            .await;
    }
    assert_eq!(
        m.drop_counter(&channel_addr, &subscriber),
        0,
        "silent must not increment"
    );
    assert_eq!(
        router.alarms.load(Ordering::SeqCst),
        0,
        "silent must not alarm"
    );
}

/// metered noise: overflow increments counter, no alarm.
#[tokio::test]
async fn push_overflow_metered_increments_counter() {
    let (m, channel_addr, router) = build_bounded_messenger(1, NoiseLevel::Metered).await;
    let subscriber = crate::messaging::ParticipantId::for_conversation(2);
    // 3 publishes → 2 overflows.
    for i in 0..3u32 {
        let _ = m
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "pa-bob",
                &channel_addr,
                &format!("m{i}"),
                Urgency::Low,
                None,
                None,
                None,
            )
            .await;
    }
    assert_eq!(
        m.drop_counter(&channel_addr, &subscriber),
        2,
        "expected 2 overflows counted"
    );
    assert_eq!(
        router.alarms.load(Ordering::SeqCst),
        0,
        "metered must not alarm"
    );
}

/// alarm noise: overflow increments counter AND fires alarm.
#[tokio::test]
async fn push_overflow_alarm_increments_counter_and_fires_alarm() {
    let (m, channel_addr, router) = build_bounded_messenger(1, NoiseLevel::Alarm).await;
    let subscriber = crate::messaging::ParticipantId::for_conversation(2);
    // 2 publishes → 1 overflow.
    for i in 0..2u32 {
        let _ = m
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "pa-bob",
                &channel_addr,
                &format!("m{i}"),
                Urgency::Low,
                None,
                None,
                None,
            )
            .await;
    }
    assert_eq!(
        m.drop_counter(&channel_addr, &subscriber),
        1,
        "alarm must increment counter"
    );
    assert_eq!(
        router.alarms.load(Ordering::SeqCst),
        1,
        "alarm must fire once"
    );
}

/// `fatal` is the surface-only kill rung and is rejected on every backend
/// subscription where its noise resolves, so the backend overflow path can never
/// see it. This test bypasses that resolver (constructing the subscription
/// directly with `fatal`) and drives an overflow to pin the named panic — the
/// unreachable-by-construction backstop.
#[tokio::test]
#[should_panic(expected = "fatal is surface-only")]
async fn push_overflow_fatal_panics_unreachable_backstop() {
    let (m, channel_addr, _router) = build_bounded_messenger(1, NoiseLevel::Fatal).await;
    // 2 publishes → 1 overflow → the fatal arm's panic.
    for i in 0..2u32 {
        let _ = m
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "pa-bob",
                &channel_addr,
                &format!("m{i}"),
                Urgency::Low,
                None,
                None,
                None,
            )
            .await;
    }
}

/// A push row for a future deliver_after must not be retired by push-overflow
/// (design §3 in-flight exclusion). With push_depth=1 and one parked row already
/// present, a second immediate publish must not touch the parked row.
#[tokio::test]
async fn deliver_after_parked_row_not_retired_on_push_overflow() {
    let push_depth = 1u64;
    let (m, channel_addr, _router) = build_bounded_messenger(push_depth, NoiseLevel::Silent).await;

    // First publish: future deliver_after → parked row.
    let future_da = Utc::now() + chrono::Duration::seconds(60);
    let _ = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &channel_addr,
            "parked",
            Urgency::Low,
            None,
            Some(future_da),
            None,
        )
        .await;
    // One push row exists, parked (release_after set).
    assert_eq!(count_bus_push_rows(&m, 2).await, 1, "parked row must exist");

    // Second publish: immediate → new row. push_depth=1, but the parked row
    // is in-flight and must not be retired; total rows = 2.
    let _ = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &channel_addr,
            "immediate",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    assert_eq!(
        count_bus_push_rows(&m, 2).await,
        2,
        "parked in-flight row must not be retired by push-overflow (design §3)"
    );
}

/// Gap B (a): pre-existing push rows at boot are seeded into the window on first
/// publish. With push_depth=2 and 2 pre-existing rows already in the DB (simulating
/// pre-restart state), a single post-boot publish must overflow by one — retiring
/// the oldest pre-existing row so exactly push_depth (2) rows remain.
#[tokio::test]
async fn push_window_seeded_from_db_on_first_touch() {
    let push_depth = 2u64;
    let (m, channel_addr, _router) = build_bounded_messenger(push_depth, NoiseLevel::Silent).await;

    // Insert push_depth pre-existing (non-parked, undelivered) push rows directly
    // into the DB, simulating rows that were there before the process restarted
    // (the fresh Messenger starts with an empty push_windows map).
    {
        let conn = m.db.lock().await;
        // Look up channel uuid by address (generated inside build_bounded_messenger).
        let uuid_bytes: Vec<u8> = conn
            .query_row(
                "SELECT uuid FROM messaging_channels WHERE address = ?1",
                rusqlite::params![channel_addr],
                |row| row.get(0),
            )
            .expect("channel must exist");
        let channel_uuid = Uuid::from_slice(&uuid_bytes).expect("valid uuid bytes");
        for i in 0..push_depth {
            insert_message_with_pushes(
                &conn,
                channel_uuid,
                "src",
                "app:pa-bob@test-source",
                &format!("pre-boot-{i}"),
                Urgency::Low,
                ChannelScheme::Brenn,
                None,
                None,
                None,
                utc_to_ns(Utc::now()) + i as i64,
                &[PendingPushInsert {
                    target_subscriber: crate::messaging::ParticipantId::for_conversation(2),
                    target_app_slug: "pa-alice".to_string(),
                    eager_wake: false,
                    release_after: None,
                    delivery_deadline: None,
                }],
            );
        }
    }

    // Sanity: 2 pre-existing rows.
    let pre_count = count_bus_push_rows(&m, 2).await;
    assert_eq!(
        pre_count, push_depth as i64,
        "pre-existing rows must be present before first publish"
    );

    // First post-boot publish. The push window is empty in memory; seed-on-first-touch
    // must load the 2 pre-existing rows, then the new row overflows → oldest retired.
    let _ = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &channel_addr,
            "post-boot",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;

    // Still exactly push_depth rows: the seed loaded 2, the new publish pushed it
    // to 3 and retired the oldest.
    assert_eq!(
        count_bus_push_rows(&m, 2).await,
        push_depth as i64,
        "push_depth must be enforced on first post-boot publish (seed from DB)"
    );
}

/// Gap B (a) — seed truncation: when DB has more than push_depth pre-existing rows
/// (backstop hadn't run), the seed must truncate to push_depth before adding the
/// new row. With push_depth=2 and 4 pre-existing rows, after the first post-boot
/// publish the in-memory deque is capped at push_depth (excess DB rows stay until
/// the GC backstop — seed is side-effect-free on the DB per design). A second
/// publish then overflows correctly (not as if the deque had 4 rows + the 2 new ones).
///
/// Specifically: first publish seeds deque to 2 (truncating 2 excess), overflows
/// by 1 → deque=[1st_kept, pub1], DB retired 1 row. Second publish → deque full
/// again → overflow retires oldest. Verifies that the deque is correctly bounded
/// at push_depth after seed truncation, not at the larger seed-before-truncation size.
#[tokio::test]
async fn push_window_seed_truncates_excess_db_rows() {
    let push_depth = 2u64;
    let (m, channel_addr, _router) = build_bounded_messenger(push_depth, NoiseLevel::Silent).await;

    let excess = push_depth + 2; // 4 pre-existing rows: 2 over push_depth
    {
        let conn = m.db.lock().await;
        let uuid_bytes: Vec<u8> = conn
            .query_row(
                "SELECT uuid FROM messaging_channels WHERE address = ?1",
                rusqlite::params![channel_addr],
                |row| row.get(0),
            )
            .expect("channel must exist");
        let channel_uuid = Uuid::from_slice(&uuid_bytes).expect("valid uuid bytes");
        for i in 0..excess {
            insert_message_with_pushes(
                &conn,
                channel_uuid,
                "src",
                "app:pa-bob@test-source",
                &format!("excess-pre-boot-{i}"),
                Urgency::Low,
                ChannelScheme::Brenn,
                None,
                None,
                None,
                utc_to_ns(Utc::now()) + i as i64,
                &[PendingPushInsert {
                    target_subscriber: crate::messaging::ParticipantId::for_conversation(2),
                    target_app_slug: "pa-alice".to_string(),
                    eager_wake: false,
                    release_after: None,
                    delivery_deadline: None,
                }],
            );
        }
    }

    // 4 pre-existing rows in DB.
    assert_eq!(count_bus_push_rows(&m, 2).await, excess as i64);

    // First post-boot publish triggers seed. Seed finds 4 rows (excluding the new id),
    // truncates in-memory deque to push_depth=2 (DB rows not deleted — side-effect-free),
    // then the new row overflows → 1 DB row deleted. DB: 4 + 1 - 1 = 4 rows.
    let _ = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &channel_addr,
            "post-boot-after-excess",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    // DB row count = 4 (seed truncation is side-effect-free; only one overflow retire).
    assert_eq!(count_bus_push_rows(&m, 2).await, excess as i64);

    // Drop counter is 1 (one overflow retire on the first publish).
    let subscriber = crate::messaging::ParticipantId::for_conversation(2);
    // NoiseLevel::Silent means drop_counter stays 0 (no metered/alarm).
    assert_eq!(m.drop_counter(&channel_addr, &subscriber), 0);

    // Second publish: deque was [kept_row, pub1] (size=2=push_depth) after first.
    // This publish overflows → 1 more row retired from DB. DB: 4 + 1 - 1 = 4.
    let _ = m
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            &channel_addr,
            "post-boot-second",
            Urgency::Low,
            None,
            None,
            None,
        )
        .await;
    // DB: 4 + 2 (two publishes) - 2 (two overflow retires) = 4.
    assert_eq!(
        count_bus_push_rows(&m, 2).await,
        excess as i64,
        "each publish after seed truncation must overflow exactly once (deque bounded at \
         push_depth, not at excess seed size)"
    );
}

/// Gap B — unbounded subscriber: first-touch seed must be a no-op.
#[tokio::test]
async fn unbounded_push_depth_no_overflow() {
    let (m, channel_uuid, _sender_conv, _sub_conv, _router) = build_messenger(0).await;
    // build_messenger uses brenn:pa-alice as the channel address.
    let channel_addr = canonical_address("pa-alice");
    let _ = channel_uuid; // uuid not needed here
    let subscriber = crate::messaging::ParticipantId::for_conversation(2);
    // Many publishes to an unbounded subscriber — no overflow, counter stays 0.
    for i in 0..10u32 {
        let _ = m
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "pa-bob",
                &channel_addr,
                &format!("m{i}"),
                Urgency::Low,
                None,
                None,
                None,
            )
            .await;
    }
    assert_eq!(m.drop_counter(&channel_addr, &subscriber), 0);
    // All 10 push rows still exist (unbounded).
    assert_eq!(count_bus_push_rows(&m, 2).await, 10);
    // Unbounded subscribers must never touch push_windows — the early-return
    // in record_push_and_check_overflow must fire before any deque entry is created.
    assert!(
        m.push_windows_is_empty(),
        "push_windows must remain empty for unbounded subscribers"
    );
}

// -----------------------------------------------------------------------
// Surface subscriber push-target coverage (resolve_push_targets Surface arm)
//
// The Surface arm of resolve_push_targets is unit-covered for target
// construction and the delivery-ACL skip; these drive it through the real
// publish path to pin the two behaviors the earlier unit tests do not:
// bounded-push_depth overflow retire, and the per-row eager_wake flag computed
// from the subscriber's wake_min.
// -----------------------------------------------------------------------

/// Build a messenger whose `brenn:surface-overflow` channel has a single
/// `Surface(deskbar)` subscriber at `push_depth`/`wake_min`, plus the `pa-bob`
/// sender app. The surface delivery policy is installed via
/// `with_surface_policies` so the delivery-time ACL gate authorizes the push.
async fn build_bounded_surface_messenger(
    push_depth: u64,
    wake_min: WakeMin,
) -> (Arc<Messenger>, String) {
    use crate::access::acl::ChannelMatcher;

    let db = init_db_memory();
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'bob', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (1, 1, 'active', 'pa-bob', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
    }
    let channel_uuid = Uuid::new_v4();
    let channel_addr = canonical_address("surface-overflow");
    let sub_depth = Depth::Bounded(push_depth);
    let entry = ChannelEntry {
        uuid: channel_uuid,
        address: channel_addr.clone(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: sub_depth,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min,
        },
        subscribers: vec![SubscriberEntry {
            kind: SubscriberEntryKind::Surface {
                slug: "deskbar".to_string(),
                instance: None,
            },
            push_depth: sub_depth,
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

    let mut apps_raw: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
    apps_raw.insert(
        "pa-bob".to_string(),
        test_app_config(
            "pa-bob",
            Some(ResolvedMessagingConfig {
                send_budget: 10000,
                subscriptions: vec![],
            }),
            vec!["bob".to_string()],
        ),
    );

    let mut surface_policies = std::collections::HashMap::new();
    surface_policies.insert(
        "deskbar".to_string(),
        crate::messaging::test_support::brenn_delivery_policy(
            ChannelMatcher::Prefix(String::new()),
        ),
    );
    let messenger = Messenger::new(
        db,
        directory,
        Arc::from("test-source"),
        Arc::new(apps_raw),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::messaging::testutils::surface_registrations(
        surface_policies,
    ));
    (messenger, channel_addr)
}

/// Count pending-push rows for the `surface:deskbar` subscriber.
async fn count_surface_push_rows(messenger: &Arc<Messenger>) -> i64 {
    let conn = messenger.db.lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_pending_pushes WHERE target_subscriber = ?1",
        rusqlite::params![ParticipantId::for_surface("deskbar").as_str()],
        |row| row.get(0),
    )
    .unwrap()
}

/// Publish `body` to the fixture channel as `pa-bob` at the given urgency.
async fn surface_publish(
    messenger: &Arc<Messenger>,
    channel_addr: &str,
    body: &str,
    urgency: Urgency,
) {
    let _ = messenger
        .publish(
            crate::messaging::PublishOrigin::Conversation { id: 1 },
            "pa-bob",
            channel_addr,
            body,
            urgency,
            None,
            None,
            None,
        )
        .await;
}

/// A bounded-push_depth Surface subscriber sees the same overflow retirement as
/// App/Wasm subscribers: after 3 publishes to a depth-2 window, exactly 2 push
/// rows survive (oldest retired), proving the Surface arm registers its push
/// window under the subscriber's `push_depth`.
#[tokio::test]
async fn surface_bounded_push_depth_retires_oldest_on_overflow() {
    let (m, channel_addr) = build_bounded_surface_messenger(2, WakeMin::Normal).await;
    for i in 0..3u32 {
        surface_publish(&m, &channel_addr, &format!("m{i}"), Urgency::Normal).await;
    }
    assert_eq!(
        count_surface_push_rows(&m).await,
        2,
        "depth-2 Surface window must retire the oldest on the third publish"
    );
}

/// A Surface subscriber is `Eager`: every push row is created eager regardless
/// of the message's urgency, because an attached surface session is cheap to
/// wake. This is the 5.2 fix — a below-`wake_min` publish to a live surface
/// session used to write `eager_wake = 0` and strand the row until reconnect.
/// Even with a `High` channel `wake_min` (so the old global formula would gate
/// a `Low` publish off), both publishes are eager now.
#[tokio::test]
async fn surface_push_is_always_eager_regardless_of_urgency() {
    let (m, channel_addr) = build_bounded_surface_messenger(8, WakeMin::High).await;
    surface_publish(&m, &channel_addr, "loud", Urgency::Normal).await;
    surface_publish(&m, &channel_addr, "quiet", Urgency::Low).await;

    let flags: Vec<i64> = {
        let conn = m.db.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT eager_wake FROM messaging_pending_pushes \
                 WHERE target_subscriber = ?1 ORDER BY id",
            )
            .unwrap();
        stmt.query_map(
            rusqlite::params![ParticipantId::for_surface("deskbar").as_str()],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
        .map(Result::unwrap)
        .collect()
    };
    assert_eq!(
        flags,
        vec![1, 1],
        "an Eager surface subscriber is woken on every publish, even a Low one below wake_min"
    );
}

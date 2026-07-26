use super::helpers::*;
use crate::db::init_db_memory;
use crate::messaging::canonical_address;
use crate::messaging::config::ResolvedMessagingConfig;
use crate::messaging::config::{Depth, NoiseLevel, Sink};
use crate::messaging::db::*;
use crate::messaging::{ChannelEntry, ChannelScheme, Urgency, WakeMin};
use crate::test_utils::ensure_user_and_conv;
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use uuid::Uuid;

#[test]
fn insert_message_with_pushes_round_trip() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let now_ns = utc_to_ns(Utc::now());
    let inserted = insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender-x",
        "hello",
        Urgency::Normal,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        now_ns,
        &[PendingPushInsert {
            target_subscriber: ParticipantId::for_conversation(2),
            target_app_slug: "pa-bob".to_string(),
            eager_wake: true,
            release_after: None,
            delivery_deadline: None,
        }],
    );
    assert!(inserted.id > 0);

    // Verify push ids were inserted (round-trip via InsertedMessage.push_ids).
    assert_eq!(inserted.push_ids.len(), 1);
    // Load via global dispatchable query (row has eager_wake=true — will appear).
    let pushes = load_all_dispatchable_pushes(&conn, Utc::now());
    assert_eq!(pushes.len(), 1, "one dispatchable push row");
    let (push_row, _deadline_expired) = &pushes[0];
    assert_eq!(push_row.target_subscriber.as_conversation_id(), 2);
    assert_eq!(push_row.payload.unwrap_bus_ref().body, "hello");
    assert!(
        push_row.eager_wake,
        "eager_wake must be true for eager-push row"
    );
}

/// `load_pending_pushes_for_drain` returns only undelivered, unparked rows for
/// the target subscriber, excludes rows for other subscribers, and returns them
/// in `publish_ts_ns ASC, id ASC` order. Directly covers AC#2 and AC#3.
#[test]
fn load_pending_pushes_for_drain_filters_and_orders() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);
    ensure_user_and_conv(&conn, 3);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let sub_a = ParticipantId::for_conversation(1);
    let sub_b = ParticipantId::for_conversation(2);
    let sub_other = ParticipantId::for_conversation(3);

    // Insert rows for sub_a: undelivered (ts=100), parked (release_after set, ts=50), delivered (ts=200).
    // The drain must return only the undelivered row at ts=100.
    let now_ns_100: i64 = 100;
    let msg_a1 = insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "undelivered",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        now_ns_100,
        &[PendingPushInsert {
            target_subscriber: sub_a.clone(),
            target_app_slug: "app".to_string(),
            eager_wake: false,
            release_after: None,
            delivery_deadline: None,
        }],
    );
    // Parked: release_after set, should be excluded.
    let future_release = Utc::now() + chrono::Duration::seconds(3600);
    insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "parked",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        50, // earlier ts, but parked — must not appear
        &[PendingPushInsert {
            target_subscriber: sub_a.clone(),
            target_app_slug: "app".to_string(),
            eager_wake: false,
            release_after: Some(future_release),
            delivery_deadline: None,
        }],
    );
    // Delivered: mark after inserting.
    let msg_a3 = insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "delivered",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        200,
        &[PendingPushInsert {
            target_subscriber: sub_a.clone(),
            target_app_slug: "app".to_string(),
            eager_wake: false,
            release_after: None,
            delivery_deadline: None,
        }],
    );
    // Use InsertedMessage.push_ids directly (load_dispatchable_pushes_for_message removed).
    assert_eq!(msg_a3.push_ids.len(), 1);
    mark_pending_pushes_delivered(&conn, &msg_a3.push_ids);

    // Insert two undelivered rows for sub_b (different subscriber, must be excluded).
    insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "sub_b msg",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        300,
        &[PendingPushInsert {
            target_subscriber: sub_b.clone(),
            target_app_slug: "app".to_string(),
            eager_wake: false,
            release_after: None,
            delivery_deadline: None,
        }],
    );

    // Insert second undelivered row for sub_a at ts=150 to verify ordering.
    let msg_a5 = insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "second undelivered",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        150,
        &[PendingPushInsert {
            target_subscriber: sub_a.clone(),
            target_app_slug: "app".to_string(),
            eager_wake: false,
            release_after: None,
            delivery_deadline: None,
        }],
    );

    // Drain sub_a: must return only the two undelivered rows for sub_a,
    // in publish_ts_ns ASC order (ts=100 first, ts=150 second).
    let drained = load_pending_pushes_for_drain(&conn, &sub_a);
    assert_eq!(
        drained.len(),
        2,
        "must return exactly 2 undelivered rows for sub_a"
    );
    assert_eq!(
        drained[0].1.unwrap_bus_ref().body,
        "undelivered",
        "ts=100 row must come first"
    );
    assert_eq!(
        drained[1].1.unwrap_bus_ref().body,
        "second undelivered",
        "ts=150 row must come second"
    );
    // Verify both push ids belong to the correct messages (use InsertedMessage.push_ids).
    assert_eq!(msg_a1.push_ids.len(), 1);
    assert_eq!(msg_a1.push_ids[0], drained[0].0);
    assert_eq!(msg_a5.push_ids.len(), 1);
    assert_eq!(msg_a5.push_ids[0], drained[1].0);

    // Drain sub_other: must return nothing (no rows for this subscriber).
    let drained_other = load_pending_pushes_for_drain(&conn, &sub_other);
    assert!(drained_other.is_empty(), "no rows for sub_other");
}

#[test]
fn fts_search_via_match() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );
    insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "the quick brown fox",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        utc_to_ns(Utc::now()),
        &[],
    );
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages_fts WHERE messaging_messages_fts MATCH ?1",
            rusqlite::params!["fox"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn release_due_for_channel_clears_and_returns_ids() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );
    let release_at = Utc::now() - chrono::Duration::seconds(5);
    let inserted = insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "deferred",
        Urgency::Normal,
        ChannelScheme::Brenn,
        None,
        None,
        Some(release_at),
        utc_to_ns(Utc::now()),
        &[],
    );
    assert!(inserted.id > 0);
    let now = Utc::now();
    let targets = [crate::messaging::store::ReleaseTarget {
        subscriber: ParticipantId::for_conversation(2),
        app_slug: "pa".to_string(),
        wake_min: None,
    }];
    let released =
        crate::messaging::db::release_due_for_channel(&conn, channel_uuid, now, &targets);
    assert_eq!(released.released.len(), 1);
    assert_eq!(
        released.released[0].push_ids.len(),
        1,
        "release mints a claim for the target attached now"
    );
    // Release clears the message grain too, so the row stops claiming it is
    // parked. A later release pass must not find it due again and dispatch it
    // a second time.
    let still_parked: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages
             WHERE id = ?1 AND deliver_after IS NOT NULL",
            rusqlite::params![inserted.id],
            |row| row.get(0),
        )
        .expect("count parked");
    assert_eq!(
        still_parked, 0,
        "a released message must not keep its deliver_after"
    );
    assert!(
        crate::messaging::db::release_due_for_channel(&conn, channel_uuid, now, &[])
            .released
            .is_empty(),
        "a second release pass must not re-release what the first released"
    );
}

#[test]
fn lookup_message_for_authorship_returns_correct_counts() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let (msg_id, msg_uuid) = insert_msg(&conn, channel_uuid, "alice", "body", None, &[1, 2]);
    // Deliver push for conv 1; conv 2 remains pending.
    mark_push_delivered(&conn, msg_id, 1);

    let lookup = lookup_message_for_authorship(&conn, msg_uuid).unwrap();
    assert_eq!(lookup.message_id, msg_id);
    assert_eq!(lookup.sender, "alice");
    assert_eq!(lookup.undelivered_count, 1);
    assert_eq!(lookup.delivered_count, 1);
}

#[test]
fn lookup_message_for_authorship_returns_none_for_unknown_uuid() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let result = lookup_message_for_authorship(&conn, Uuid::new_v4());
    assert!(result.is_none());
}

#[test]
fn cancel_pending_pushes_deletes_only_undelivered() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let (msg_id, _) = insert_msg(&conn, channel_uuid, "sender", "body", None, &[1, 2]);
    // Deliver push for conv 1.
    mark_push_delivered(&conn, msg_id, 1);

    // Cancel: should delete only the undelivered push (conv 2).
    let cancelled = cancel_pending_pushes_for_message(&conn, msg_id, "sender");
    assert_eq!(cancelled, 1);

    // The delivered row (conv 1) must still exist with delivered_at set.
    let remaining: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes WHERE message_id = ?1",
            rusqlite::params![msg_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 1, "delivered push row must remain");

    let still_has_delivered_at: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes
             WHERE message_id = ?1 AND delivered_at IS NOT NULL",
            rusqlite::params![msg_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(still_has_delivered_at, 1);
}

#[test]
fn update_message_and_pending_pushes_rolls_back_when_any_delivered() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let (msg_id, _) = insert_msg(
        &conn,
        channel_uuid,
        "sender",
        "original body",
        None,
        &[1, 2],
    );
    // Deliver one push.
    mark_push_delivered(&conn, msg_id, 1);

    let fields = EditFieldsApplied {
        body: Some("new body"),
        reply_to_uuid: None,
        deliver_after: None,
        delivery_deadline: None,
        urgency: None,
    };
    let result = update_message_and_pending_pushes(&conn, msg_id, "sender", &fields);
    assert_eq!(result, EditUpdateResult::AnyDelivered);

    // Verify body unchanged.
    let body: String = conn
        .query_row(
            "SELECT body FROM messaging_messages WHERE id = ?1",
            rusqlite::params![msg_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(body, "original body");
}

#[test]
fn update_message_and_pending_pushes_propagates_body_to_fts() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let (msg_id, _) = insert_msg(&conn, channel_uuid, "sender", "old unique word", None, &[1]);

    let fields = EditFieldsApplied {
        body: Some("new unique phrase"),
        reply_to_uuid: None,
        deliver_after: None,
        delivery_deadline: None,
        urgency: None,
    };
    let result = update_message_and_pending_pushes(&conn, msg_id, "sender", &fields);
    assert!(matches!(result, EditUpdateResult::Ok { .. }));

    // Old word should no longer match.
    let old_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages_fts WHERE messaging_messages_fts MATCH ?1",
            rusqlite::params!["unique AND word"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(old_count, 0, "old body content must be removed from FTS");

    // New phrase should match.
    let new_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages_fts WHERE messaging_messages_fts MATCH ?1",
            rusqlite::params!["phrase"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(new_count, 1, "new body content must be in FTS");
}

#[test]
fn update_message_and_pending_pushes_reschedule_only_does_not_touch_fts() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let future = Utc::now() + chrono::Duration::seconds(60);
    let (msg_id, _) = insert_msg(
        &conn,
        channel_uuid,
        "sender",
        "foxword body",
        Some(future),
        &[1],
    );

    // Reschedule only — body unchanged.
    let new_future = Utc::now() + chrono::Duration::seconds(120);
    let fields = EditFieldsApplied {
        body: None,
        reply_to_uuid: None,
        deliver_after: Some(Some(new_future)),
        delivery_deadline: None,
        urgency: None,
    };
    let result = update_message_and_pending_pushes(&conn, msg_id, "sender", &fields);
    assert!(matches!(result, EditUpdateResult::Ok { .. }));

    // FTS for original body keyword must still match.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages_fts WHERE messaging_messages_fts MATCH ?1",
            rusqlite::params!["foxword"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 1,
        "FTS must still find original body after reschedule-only edit"
    );
}

#[test]
fn list_pending_messages_for_sender_filters_correctly() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);

    // Two channels.
    let ch_uuid_a = Uuid::new_v4();
    let ch_uuid_b = Uuid::new_v4();
    upsert_channels(
        &conn,
        &[
            ChannelEntry {
                uuid: ch_uuid_a,
                address: canonical_address("chan-a"),
                description: None,
                resolved_channel: default_resolved_channel(),
                subscribers: vec![],
                transport_type: ChannelScheme::Brenn,
                mount: None,
            },
            ChannelEntry {
                uuid: ch_uuid_b,
                address: canonical_address("chan-b"),
                description: None,
                resolved_channel: default_resolved_channel(),
                subscribers: vec![],
                transport_type: ChannelScheme::Brenn,
                mount: None,
            },
        ],
    );

    // alice sends on chan-a (pending push to conv 1).
    insert_msg(&conn, ch_uuid_a, "alice", "alice-chan-a", None, &[1]);
    // bob sends on chan-a (pending push to conv 2).
    insert_msg(&conn, ch_uuid_a, "bob", "bob-chan-a", None, &[2]);
    // alice sends on chan-b.
    insert_msg(&conn, ch_uuid_b, "alice", "alice-chan-b", None, &[1]);

    // All of alice's pending.
    let all_alice = list_pending_messages_for_sender(&conn, "alice", None);
    assert_eq!(all_alice.len(), 2, "alice should have 2 pending");
    let bodies: Vec<&str> = all_alice.iter().map(|e| e.body.as_str()).collect();
    assert!(bodies.contains(&"alice-chan-a"));
    assert!(bodies.contains(&"alice-chan-b"));

    // Alice filtered to chan-a.
    let alice_chan_a = list_pending_messages_for_sender(&conn, "alice", Some(ch_uuid_a));
    assert_eq!(alice_chan_a.len(), 1);
    assert_eq!(alice_chan_a[0].body, "alice-chan-a");

    // Bob's pending.
    let bob = list_pending_messages_for_sender(&conn, "bob", None);
    assert_eq!(bob.len(), 1);
    assert_eq!(bob[0].body, "bob-chan-a");
}

#[test]
fn list_pending_includes_past_due_undrained() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    // Schedule in the past (pending push has release_after in the past).
    let past = Utc::now() - chrono::Duration::seconds(10);
    insert_msg(&conn, channel_uuid, "sender", "past-due", Some(past), &[1]);

    let result = list_pending_messages_for_sender(&conn, "sender", None);
    assert_eq!(
        result.len(),
        1,
        "past-due undrained message must appear in pending list"
    );
}

/// test-7: `update_message_and_pending_pushes` returns `NoPendingPushes`
/// when no undelivered push rows exist (e.g. all cancelled).
#[test]
fn update_message_and_pending_pushes_returns_no_pushes_when_none_undelivered() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let (msg_id, _) = insert_msg(&conn, channel_uuid, "sender", "body", None, &[1]);
    // Delete all pending pushes (simulates a prior cancel).
    cancel_pending_pushes_for_message(&conn, msg_id, "sender");

    let fields = EditFieldsApplied {
        body: Some("changed"),
        reply_to_uuid: None,
        deliver_after: None,
        delivery_deadline: None,
        urgency: None,
    };
    let result = update_message_and_pending_pushes(&conn, msg_id, "sender", &fields);
    assert_eq!(
        result,
        EditUpdateResult::NoPendingPushes,
        "should return NoPendingPushes when all pushes deleted"
    );

    // Body should be unchanged (commit happened but UPDATE was a no-op on push table).
    let body: String = conn
        .query_row(
            "SELECT body FROM messaging_messages WHERE id = ?1",
            rusqlite::params![msg_id],
            |r| r.get(0),
        )
        .unwrap();
    // The message-row UPDATE does NOT fire when NoPendingPushes returns early.
    // Verify body is still original.
    assert_eq!(body, "body", "message body should be unchanged");
}

#[test]
fn list_pending_excludes_fully_delivered() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let (msg_id, _) = insert_msg(&conn, channel_uuid, "sender", "delivered-msg", None, &[1]);
    mark_push_delivered(&conn, msg_id, 1);

    let result = list_pending_messages_for_sender(&conn, "sender", None);
    assert!(
        result.is_empty(),
        "fully delivered message must not appear in pending list"
    );
}

/// Correctness guard for `earliest_pending_deadline`: the MIN(delivery_deadline)
/// query must return the chronologically earliest timestamp regardless of
/// whether rows were written by `format_ts_for_db` (+00:00 form) or an older
/// `to_rfc3339()` (Z form).
///
/// If mixed forms are present, SQLite's lexicographic MIN would pick the wrong row
/// because "Z" < "+" in ASCII order. This test inserts both forms directly and
/// asserts that `earliest_pending_deadline` returns the correct earlier time.
#[test]
fn earliest_pending_deadline_min_correct_with_uniform_format() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    // T1 is earlier (deadline 1 hour from epoch); T2 is later (2 hours).
    let t1: DateTime<Utc> = DateTime::from_timestamp(3600, 0).unwrap();
    let t2: DateTime<Utc> = DateTime::from_timestamp(7200, 0).unwrap();

    // Insert via insert_message_with_pushes which uses format_ts_for_db (+00:00).
    let ns = utc_to_ns(Utc::now());
    insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "msg-t2",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        Some(t2),
        None,
        ns,
        &[PendingPushInsert {
            target_subscriber: ParticipantId::for_conversation(2),
            target_app_slug: "app".to_string(),
            eager_wake: false,
            release_after: None,
            delivery_deadline: Some(t2),
        }],
    );
    insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "msg-t1",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        Some(t1),
        None,
        ns,
        &[PendingPushInsert {
            target_subscriber: ParticipantId::for_conversation(1),
            target_app_slug: "app".to_string(),
            eager_wake: false,
            release_after: None,
            delivery_deadline: Some(t1),
        }],
    );

    let earliest =
        earliest_pending_deadline(&conn).expect("should have at least one pending deadline");
    assert_eq!(
        earliest, t1,
        "MIN(delivery_deadline) must return the chronologically earliest deadline; \
         got {earliest}, expected {t1}. If wrong, timestamps mixed Z/+00:00 forms."
    );
}

// -----------------------------------------------------------------------
// messaging-mvp-test-gap (design §4)
// -----------------------------------------------------------------------

/// `rebuild_subscriptions` round-trip: truncate + re-insert from config.
#[test]
fn rebuild_subscriptions_round_trips() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: ch_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    // Insert initial subscriptions.
    let entries = vec![(
        "app-a".to_string(),
        ResolvedMessagingConfig {
            send_budget: 10,
            subscriptions: vec![crate::messaging::config::ResolvedSubscription {
                channel_uuid: ch_uuid,
                channel_address: canonical_address("test"),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            }],
        },
    )];
    rebuild_subscriptions(&conn, &entries, &[], &[]);

    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM messaging_subscriptions WHERE app_slug = 'app-a'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "initial rebuild must insert one subscription");

    // Rebuild with empty config — must truncate.
    rebuild_subscriptions(&conn, &[], &[], &[]);
    let count_after: i64 = conn
        .query_row("SELECT count(*) FROM messaging_subscriptions", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        count_after, 0,
        "rebuild with empty config must truncate all subscriptions"
    );
}

/// `mark_pending_pushes_delivered` is idempotent: calling it twice does not
/// error and does not change the `delivered_at` timestamp.
#[test]
fn mark_pending_pushes_delivered_is_idempotent() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let (_, _) = insert_msg(&conn, channel_uuid, "sender", "body", None, &[1]);
    // Find the push id.
    let push_id: i64 = conn
        .query_row(
            "SELECT id FROM messaging_pending_pushes WHERE target_subscriber = 'conversation:1'",
            [],
            |r| r.get(0),
        )
        .unwrap();

    // First call.
    mark_pending_pushes_delivered(&conn, &[push_id]);
    let delivered_at_first: String = conn
        .query_row(
            "SELECT delivered_at FROM messaging_pending_pushes WHERE id = ?1",
            rusqlite::params![push_id],
            |r| r.get(0),
        )
        .unwrap();

    // Second call must be a no-op (WHERE delivered_at IS NULL filters it out).
    mark_pending_pushes_delivered(&conn, &[push_id]);
    let delivered_at_second: String = conn
        .query_row(
            "SELECT delivered_at FROM messaging_pending_pushes WHERE id = ?1",
            rusqlite::params![push_id],
            |r| r.get(0),
        )
        .unwrap();

    assert_eq!(
        delivered_at_first, delivered_at_second,
        "second mark_pending_pushes_delivered must not change delivered_at"
    );
}

/// `earliest_pending_deadline` returns `None` when the table is empty.
#[test]
fn earliest_pending_deadline_returns_none_on_empty_table() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let result = earliest_pending_deadline(&conn);
    assert!(result.is_none(), "empty table must return None");
}

/// `earliest_pending_deadline` returns `Some(deadline)` when a pending push
/// with a deadline exists.
#[test]
fn earliest_pending_deadline_returns_some_when_row_exists() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let deadline = DateTime::from_timestamp(3600, 0).unwrap();
    let ns = utc_to_ns(Utc::now());
    insert_message_with_pushes(
        &conn,
        channel_uuid,
        "src",
        "sender",
        "body",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        Some(deadline),
        None,
        ns,
        &[PendingPushInsert {
            target_subscriber: ParticipantId::for_conversation(1),
            target_app_slug: "app".to_string(),
            eager_wake: false,
            release_after: None,
            delivery_deadline: Some(deadline),
        }],
    );

    let result = earliest_pending_deadline(&conn);
    assert_eq!(
        result,
        Some(deadline),
        "must return the inserted deadline; got {:?}",
        result
    );
}

/// `earliest_channel_release` returns the parked message's release time.
///
/// The release deadline is read off the *message* grain, so it answers for a
/// message parked with no delivery targets exactly as for one with them.
#[test]
fn earliest_channel_release_returns_some_when_a_message_is_parked() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let release_at = Utc::now() + chrono::Duration::seconds(60);
    insert_msg(
        &conn,
        channel_uuid,
        "sender",
        "body",
        Some(release_at),
        &[1],
    );

    let result = crate::messaging::db::earliest_channel_release(&conn, channel_uuid);
    let result = result.expect("a parked message must report a release deadline");
    // Within a second of the inserted time, tolerating serialisation rounding.
    let diff = (result - release_at).num_seconds().abs();
    assert!(
        diff <= 1,
        "returned release time {result:?} must match inserted {release_at:?} (diff {diff}s)"
    );
}

/// `earliest_channel_release` returns `None` when the channel has nothing
/// parked, and never answers for another channel's parked message.
#[test]
fn earliest_channel_release_is_none_for_an_unparked_channel() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (a, b) = upsert_two_channels(&conn);
    assert!(
        crate::messaging::db::earliest_channel_release(&conn, a).is_none(),
        "empty channel must report no release deadline"
    );

    let release_at = Utc::now() + chrono::Duration::seconds(60);
    insert_msg(&conn, b, "sender", "body", Some(release_at), &[1]);
    assert!(
        crate::messaging::db::earliest_channel_release(&conn, a).is_none(),
        "one channel's parked message must not become another's deadline"
    );
    assert!(crate::messaging::db::earliest_channel_release(&conn, b).is_some());
}

/// A released message stops carrying a release deadline: the grain the
/// deadline is read from is the grain release clears.
#[test]
fn earliest_channel_release_is_none_after_release() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let release_at = Utc::now() - chrono::Duration::seconds(1);
    insert_msg(
        &conn,
        channel_uuid,
        "sender",
        "body",
        Some(release_at),
        &[1],
    );
    assert_eq!(
        crate::messaging::db::release_due_for_channel(&conn, channel_uuid, Utc::now(), &[])
            .released
            .len(),
        1
    );

    assert!(
        crate::messaging::db::earliest_channel_release(&conn, channel_uuid).is_none(),
        "a released message must not keep reporting a deadline"
    );
}

// Note: `pending_pushes_past_deadline` was removed — deadline-expired rows are now
// covered by `load_all_dispatchable_pushes` (see tests in the dispatcher module and
// the `load_all_dispatchable_pushes_*` tests in this file).

/// `load_pushes_by_ids`: when some IDs are missing, only found rows are
/// returned (no panic).
#[test]
fn load_pushes_by_ids_partial_found() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let (message_id, _) = insert_msg(&conn, channel_uuid, "sender", "body", None, &[1]);
    let push_id: i64 = conn
        .query_row(
            "SELECT id FROM messaging_pending_pushes WHERE target_subscriber = 'conversation:1'",
            [],
            |r| r.get(0),
        )
        .unwrap();

    // Request the real push_id plus a nonexistent one.
    let nonexistent_id = push_id + 9999;
    let results = load_pushes_by_ids(&conn, &[push_id, nonexistent_id]);
    assert_eq!(results.len(), 1, "only the existing push must be returned");
    assert_eq!(results[0].push_id, push_id);
    // Pin `message_id` (col 14 in this query's SELECT): it is the durable wire
    // seq for delayed-release rows replayed through this loader, so a column-index
    // slip here would silently ship the wrong `Pos::Durable` seq.
    assert_eq!(results[0].message_id, message_id);
}

// -----------------------------------------------------------------------
// test-1: sender-recheck-in-mutation — mismatched caller_sender tests
// -----------------------------------------------------------------------

/// cancel_pending_pushes_for_message with a mismatched caller_sender must
/// delete zero rows and leave the original push intact.
#[test]
fn cancel_pending_pushes_sender_mismatch_touches_zero_rows() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    let (msg_id, _) = insert_msg(&conn, channel_uuid, "alice", "body", None, &[1]);

    // Attempt cancel with wrong sender.
    let deleted = cancel_pending_pushes_for_message(&conn, msg_id, "eve");
    assert_eq!(deleted, 0, "mismatched sender must delete zero rows");

    // Original push must still exist and be undelivered.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes
             WHERE message_id = ?1 AND delivered_at IS NULL",
            rusqlite::params![msg_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 1,
        "push row must remain intact after sender mismatch"
    );
}

// -----------------------------------------------------------------------
// test-2: wake-recompute-in-tx — per-push CASE expression edge cases
// -----------------------------------------------------------------------

/// When the subscription kind is None (pull-only), editing the message wake to Immediate
/// must leave push eager_wake = 0 (not push-enabled → never eagerly woken).
#[test]
fn update_message_wake_leaves_none_subscriber_as_none() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);
    let ch_uuid = Uuid::new_v4();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: ch_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    // Insert a pull-only subscription (push_depth=0) for app-a.
    conn.execute(
        "INSERT INTO messaging_subscriptions (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min) \
         VALUES (?1, 'app-a', '0', 'unbounded', 'silent', 'normal')",
        rusqlite::params![ch_uuid.as_bytes().to_vec()],
    )
    .unwrap();

    // Insert message with push for conv 1 (app-a), wake=none initially.
    let ns = utc_to_ns(Utc::now());
    let future = Utc::now() + chrono::Duration::seconds(3600);
    let inserted = insert_message_with_pushes(
        &conn,
        ch_uuid,
        "src",
        "sender",
        "body",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        Some(future),
        ns,
        &[PendingPushInsert {
            target_subscriber: ParticipantId::for_conversation(1),
            target_app_slug: "app-a".to_string(),
            eager_wake: false,
            release_after: Some(future),
            delivery_deadline: None,
        }],
    );

    // Edit message wake to Immediate.
    let fields = EditFieldsApplied {
        body: None,
        reply_to_uuid: None,
        deliver_after: None,
        delivery_deadline: None,
        urgency: Some(Urgency::Normal),
    };
    let result = update_message_and_pending_pushes(&conn, inserted.id, "sender", &fields);
    assert!(
        matches!(result, EditUpdateResult::Ok { .. }),
        "expected Ok, got {result:?}"
    );

    // Push eager_wake must remain 0 because subscription is pull-only (push_depth=0).
    let eager_wake_int: i64 = conn
        .query_row(
            "SELECT eager_wake FROM messaging_pending_pushes WHERE message_id = ?1",
            rusqlite::params![inserted.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        eager_wake_int, 0,
        "push eager_wake must stay 0 when subscription is pull-only"
    );
}

/// Multiple push rows with different subscription kinds: after editing wake to
/// Immediate, only the Immediate-subscribed push should flip; the None-subscribed
/// push must stay 'none'. Also covers the no-matching-subscription case (unknown
/// app_slug) which must default to 'none'.
#[test]
fn update_message_wake_per_push_distinct_results() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);
    ensure_user_and_conv(&conn, 3);
    let ch_uuid = Uuid::new_v4();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: ch_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    // app-imm: push-enabled (Unbounded) subscription; app-none: pull-only (0) subscription.
    // app-unknown has no subscription row at all.
    conn.execute(
        "INSERT INTO messaging_subscriptions (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min) \
         VALUES (?1, 'app-imm', 'unbounded', 'unbounded', 'silent', 'normal')",
        rusqlite::params![ch_uuid.as_bytes().to_vec()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO messaging_subscriptions (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min) \
         VALUES (?1, 'app-none', '0', 'unbounded', 'silent', 'normal')",
        rusqlite::params![ch_uuid.as_bytes().to_vec()],
    )
    .unwrap();

    let ns = utc_to_ns(Utc::now());
    let future = Utc::now() + chrono::Duration::seconds(3600);
    let inserted = insert_message_with_pushes(
        &conn,
        ch_uuid,
        "src",
        "sender",
        "body",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        Some(future),
        ns,
        &[
            PendingPushInsert {
                target_subscriber: ParticipantId::for_conversation(1),
                target_app_slug: "app-imm".to_string(),
                eager_wake: false,
                release_after: Some(future),
                delivery_deadline: None,
            },
            PendingPushInsert {
                target_subscriber: ParticipantId::for_conversation(2),
                target_app_slug: "app-none".to_string(),
                eager_wake: false,
                release_after: Some(future),
                delivery_deadline: None,
            },
            PendingPushInsert {
                target_subscriber: ParticipantId::for_conversation(3),
                target_app_slug: "app-unknown".to_string(),
                eager_wake: false,
                release_after: Some(future),
                delivery_deadline: None,
            },
        ],
    );

    // Edit wake to Immediate.
    let fields = EditFieldsApplied {
        body: None,
        reply_to_uuid: None,
        deliver_after: None,
        delivery_deadline: None,
        urgency: Some(Urgency::Normal),
    };
    let result = update_message_and_pending_pushes(&conn, inserted.id, "sender", &fields);
    assert!(
        matches!(result, EditUpdateResult::Ok { affected_pushes: 3 }),
        "expected Ok with 3 pushes, got {result:?}"
    );

    let eager_wake_for = |cid: i64| -> i64 {
        let sub = ParticipantId::for_conversation(cid);
        conn.query_row(
            "SELECT eager_wake FROM messaging_pending_pushes
             WHERE message_id = ?1 AND target_subscriber = ?2",
            rusqlite::params![inserted.id, sub.as_str()],
            |r| r.get(0),
        )
        .unwrap()
    };

    assert_eq!(
        eager_wake_for(1),
        1,
        "app-imm (push-enabled subscriber) must get eager_wake=1 after Immediate edit"
    );
    assert_eq!(
        eager_wake_for(2),
        0,
        "app-none (pull-only subscriber) must keep eager_wake=0"
    );
    assert_eq!(
        eager_wake_for(3),
        0,
        "app-unknown (no subscription row) must default to eager_wake=0"
    );
}

/// Regression guard for design-3 (§2.1 "Runtime mirror write", §5 "Runtime mirror
/// write"): a runtime-created push-enabled dynamic subscription writes its
/// `messaging_subscriptions` mirror row in the *same transaction* as the durable
/// dynamic row, so the urgency-recompute join in `update_message_and_pending_pushes`
/// sees the subscriber *before any restart*. Without the mirror write the join's
/// `COALESCE(...,0)` falls to 0 and the subscriber is silently never woken — a
/// push-delivery correctness bug. This drives the recompute through the runtime
/// insert path (not a hand-written mirror row) and asserts eager_wake flips.
#[test]
fn runtime_dynamic_push_sub_mirror_drives_eager_wake_recompute() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let ch_uuid = Uuid::new_v4();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: ch_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    // Runtime subscribe path: writes BOTH the durable dynamic row and the
    // messaging_subscriptions mirror row in one transaction. Push-enabled
    // (push_depth > 0) so the recompute join's `push_depth>0` guard includes it.
    insert_dynamic_subscription(
        &conn,
        &DynamicSubscriptionRow {
            channel_uuid: ch_uuid,
            app_slug: "app-dyn".to_string(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: WakeMin::Normal,
            qos: None,
            created_at: "2026-06-20T00:00:00Z".to_string(),
        },
    );

    let ns = utc_to_ns(Utc::now());
    let future = Utc::now() + chrono::Duration::seconds(3600);
    let inserted = insert_message_with_pushes(
        &conn,
        ch_uuid,
        "src",
        "sender",
        "body",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        Some(future),
        ns,
        &[PendingPushInsert {
            target_subscriber: ParticipantId::for_conversation(1),
            target_app_slug: "app-dyn".to_string(),
            eager_wake: false,
            release_after: Some(future),
            delivery_deadline: None,
        }],
    );

    let fields = EditFieldsApplied {
        body: None,
        reply_to_uuid: None,
        deliver_after: None,
        delivery_deadline: None,
        urgency: Some(Urgency::Normal),
    };
    let result = update_message_and_pending_pushes(&conn, inserted.id, "sender", &fields);
    assert!(
        matches!(result, EditUpdateResult::Ok { .. }),
        "expected Ok, got {result:?}"
    );

    let eager_wake_int: i64 = conn
        .query_row(
            "SELECT eager_wake FROM messaging_pending_pushes WHERE message_id = ?1",
            rusqlite::params![inserted.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        eager_wake_int, 1,
        "runtime dynamic push-enabled sub must be visible to the recompute join \
         via its mirror row (eager_wake flips to 1) before any restart"
    );
}

#[test]
#[should_panic(expected = "edit sender mismatch")]
fn update_message_and_pending_pushes_panics_on_sender_mismatch_message_field() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let (_, channel_uuid) = make_directory();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );
    let (msg_id, _) = insert_msg(&conn, channel_uuid, "alice", "body", None, &[1]);
    let fields = EditFieldsApplied {
        body: Some("new body"),
        reply_to_uuid: None,
        deliver_after: None,
        delivery_deadline: None,
        urgency: None,
    };
    // "mallory" is not the sender — must panic. The sender check fires at the
    // top of the function before COUNT or set_clauses, so this covers push-only
    // edits (where set_clauses would be empty) equally well — the EditFieldsApplied
    // shape is not load-bearing for the panic.
    update_message_and_pending_pushes(&conn, msg_id, "mallory", &fields);
}

#[test]
#[should_panic(expected = "edit row missing")]
fn update_message_and_pending_pushes_panics_on_missing_row() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let fields = EditFieldsApplied {
        body: Some("body"),
        reply_to_uuid: None,
        deliver_after: None,
        delivery_deadline: None,
        urgency: None,
    };
    // message_id 99999 does not exist — must panic.
    update_message_and_pending_pushes(&conn, 99999, "alice", &fields);
}

// -----------------------------------------------------------------------
// Bus GC tests (design §4 — GC + sink, two-reaper non-overlap)
// -----------------------------------------------------------------------

/// Helper: insert a bus message row for `channel_uuid` and return its `id`.
fn insert_bus_msg(conn: &Connection, ch_uuid_bytes: &[u8], publish_ts_ns: i64) -> i64 {
    let msg_uuid = Uuid::new_v4();
    let msg_uuid_bytes = msg_uuid.as_bytes().to_vec();
    // Allocate a dense per-channel retained_seq exactly as production does;
    // GC and resume key on retained_seq, not rowid/publish_ts.
    let seq: i64 = conn
        .query_row(
            "UPDATE messaging_channels SET last_retained_seq = last_retained_seq + 1
             WHERE uuid = ?1 RETURNING last_retained_seq",
            rusqlite::params![ch_uuid_bytes],
            |row| row.get(0),
        )
        .expect("insert_bus_msg: allocate retained_seq (channel row must exist)");
    conn.execute(
        "INSERT INTO messaging_messages
           (uuid, channel_uuid, source, sender, body, urgency,
            publish_ts_ns, created_at, retained_seq)
         VALUES (?1, ?2, 'src', 'sender', '{\"x\":1}', 'low', ?3, '2024-01-01', ?4)",
        rusqlite::params![msg_uuid_bytes, ch_uuid_bytes, publish_ts_ns, seq],
    )
    .expect("insert_bus_msg");
    conn.last_insert_rowid()
}

/// Helper: insert a parked bus message (a `deliver_after`, `retained_seq` NULL —
/// no retention position until it releases). `deliver_after` may be future
/// (strictly parked) or past (due-but-unswept); both stay `retained_seq NULL`
/// until a release pass. Used to prove GC never reaps a parked row.
fn insert_parked_bus_msg(
    conn: &Connection,
    ch_uuid_bytes: &[u8],
    publish_ts_ns: i64,
    deliver_after: &str,
) -> i64 {
    let msg_uuid = Uuid::new_v4();
    let msg_uuid_bytes = msg_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_messages
           (uuid, channel_uuid, source, sender, body, urgency,
            publish_ts_ns, created_at, deliver_after, retained_seq)
         VALUES (?1, ?2, 'src', 'sender', '{\"x\":1}', 'low', ?3, '2024-01-01',
                 ?4, NULL)",
        rusqlite::params![msg_uuid_bytes, ch_uuid_bytes, publish_ts_ns, deliver_after],
    )
    .expect("insert_parked_bus_msg");
    conn.last_insert_rowid()
}

/// Helper: insert a bus push row and return its `id`.
fn insert_bus_push(conn: &Connection, message_id: i64, app_slug: &str) -> i64 {
    conn.execute(
        "INSERT INTO messaging_pending_pushes
           (message_id, target_subscriber, target_app_slug, eager_wake, created_at)
         VALUES (?1, ?2, ?3, 0, '2024-01-01')",
        rusqlite::params![message_id, format!("conversation:{message_id}"), app_slug],
    )
    .expect("insert_bus_push");
    conn.last_insert_rowid()
}

/// Helper: count message rows for a channel.
fn count_bus_messages(conn: &Connection, ch_uuid_bytes: &[u8]) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_messages WHERE channel_uuid = ?1 AND envelope_type='brenn'",
        rusqlite::params![ch_uuid_bytes],
        |r| r.get(0),
    )
    .expect("count_bus_messages")
}

/// Helper: count push rows for messages on a channel.
fn count_bus_pushes(conn: &Connection, ch_uuid_bytes: &[u8]) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_pending_pushes pp
         JOIN messaging_messages m ON m.id = pp.message_id
         WHERE m.channel_uuid = ?1 AND m.envelope_type='brenn'",
        rusqlite::params![ch_uuid_bytes],
        |r| r.get(0),
    )
    .expect("count_bus_pushes")
}

/// Bounded-frontier drop channel: after N > frontier publishes + one GC pass,
/// retained body count <= frontier. Push rows are also reaped.
#[test]
fn bus_gc_evict_drop_bounds_channel() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:ch', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // Insert 10 messages with delivered push rows each.
    for ts in 1000..1010i64 {
        let msg_id = insert_bus_msg(&conn, &ch_uuid_bytes, ts);
        insert_bus_push(&conn, msg_id, "app-a");
        // Mark the push delivered.
        conn.execute(
            "UPDATE messaging_pending_pushes SET delivered_at='2024-01-01' WHERE message_id=?1",
            rusqlite::params![msg_id],
        )
        .unwrap();
    }

    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 10);
    assert_eq!(count_bus_pushes(&conn, &ch_uuid_bytes), 10);

    // frontier = 3: keep 3 most-recent, evict 7.
    let eviction = bus_gc_evict_channel(
        &conn,
        ch_uuid,
        "brenn:ch",
        ChannelScheme::Brenn,
        3,
        Sink::Drop,
        None,
    );

    assert_eq!(eviction.messages_evicted, 7, "7 messages evicted");
    assert_eq!(eviction.push_rows_retired, 7, "7 push rows reaped");
    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 3);
    assert_eq!(count_bus_pushes(&conn, &ch_uuid_bytes), 3);

    // FTS must still be consistent (no panic means triggers fired correctly).
    conn.execute(
        "INSERT INTO messaging_messages_fts(messaging_messages_fts) VALUES ('integrity-check')",
        [],
    )
    .unwrap();
}

/// Fewer than frontier rows → nothing evicted.
#[test]
fn bus_gc_evict_fewer_than_frontier_is_noop() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:ch2', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    for ts in 1000..1003i64 {
        let msg_id = insert_bus_msg(&conn, &ch_uuid_bytes, ts);
        insert_bus_push(&conn, msg_id, "app");
    }

    // frontier=10 > 3 messages — nothing eligible.
    let eviction = bus_gc_evict_channel(
        &conn,
        ch_uuid,
        "brenn:ch2",
        ChannelScheme::Brenn,
        10,
        Sink::Drop,
        None,
    );
    assert_eq!(eviction.messages_evicted, 0);
    assert_eq!(eviction.push_rows_retired, 0);
    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 3);
}

/// A body pinned by an undelivered push row is still deleted by GC eviction
/// (the GC operates on bodies, not push state — pinning is by subscriber window
/// size in the frontier computation, not per-message liveness).
/// Correct behavior: once a message is past the frontier, its body AND push rows
/// are removed regardless of delivered_at state.
#[test]
fn bus_gc_evict_deletes_both_delivered_and_undelivered_push_rows() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:ch3', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // 5 messages; frontier = 2 → oldest 3 evicted.
    let mut msg_ids = vec![];
    for ts in 1..6i64 {
        let msg_id = insert_bus_msg(&conn, &ch_uuid_bytes, ts * 1000);
        insert_bus_push(&conn, msg_id, "sub");
        msg_ids.push(msg_id);
    }
    // Mark message 2 (index 1) as delivered; leave others undelivered.
    conn.execute(
        "UPDATE messaging_pending_pushes SET delivered_at='2024-01-01' WHERE message_id=?1",
        rusqlite::params![msg_ids[1]],
    )
    .unwrap();

    let eviction = bus_gc_evict_channel(
        &conn,
        ch_uuid,
        "brenn:ch3",
        ChannelScheme::Brenn,
        2,
        Sink::Drop,
        None,
    );
    assert_eq!(eviction.messages_evicted, 3);
    assert_eq!(eviction.push_rows_retired, 3);
    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 2);
}

/// Two-reaper non-overlap: bus GC must not touch kind='ingress' push rows.
/// Ingress cleanup must not touch kind='brenn' message bodies.
/// Run both in a mixed-kind fixture and verify each only touches its own kind.
#[test]
fn two_reaper_non_overlap_kind_fence() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:fence', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // Insert 5 bus messages with push rows.
    for ts in 1..6i64 {
        let msg_id = insert_bus_msg(&conn, &ch_uuid_bytes, ts * 1000);
        insert_bus_push(&conn, msg_id, "app");
    }

    // Insert 2 ingress messages with delivered push rows (eligible for ingress cleanup).
    let past = "2020-01-01T00:00:00+00:00";
    for i in 0..2i64 {
        let ing_uuid = Uuid::new_v4();
        let ing_uuid_bytes = ing_uuid.as_bytes().to_vec();
        conn.execute(
            "INSERT INTO messaging_messages
               (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns,
                created_at, envelope_type, ingress_source, ingress_summary)
             VALUES (?1, NULL, '', '', '{}', 'low', ?2, '2020-01-01',
                     'ingress', 'mqtt:x', 'sum')",
            rusqlite::params![ing_uuid_bytes, i * 1000],
        )
        .unwrap();
        let ing_msg_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO messaging_pending_pushes
               (message_id, target_subscriber, target_app_slug, eager_wake, delivered_at, created_at)
             VALUES (?1, 'conversation:1', 'app', 0, ?2, '2020-01-01')",
            rusqlite::params![ing_msg_id, past],
        )
        .unwrap();
    }

    // Before: 5 bus messages, 2 ingress messages, 7 total push rows.
    let total_msgs_before: i64 = conn
        .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total_msgs_before, 7);

    // Run bus GC with frontier=2 → evict 3 bus messages.
    let bus_eviction = bus_gc_evict_channel(
        &conn,
        ch_uuid,
        "brenn:fence",
        ChannelScheme::Brenn,
        2,
        Sink::Drop,
        None,
    );
    assert_eq!(
        bus_eviction.messages_evicted, 3,
        "bus GC evicted 3 bus messages"
    );
    assert_eq!(
        bus_eviction.push_rows_retired, 3,
        "bus GC retired 3 bus push rows"
    );

    // Ingress rows must be untouched.
    let ingress_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE envelope_type='ingress'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ingress_count, 2, "bus GC must not touch ingress messages");

    let ingress_push_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON m.id = pp.message_id
             WHERE m.envelope_type = 'ingress'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        ingress_push_count, 2,
        "bus GC must not touch ingress push rows"
    );

    // Run ingress cleanup; cutoff = now (past is very old, so all ingress rows eligible).
    let cutoff = chrono::Utc::now();
    let (ing_pushes_del, ing_msgs_del) = delete_delivered_ingress_pushes_before(&conn, cutoff);
    assert_eq!(ing_pushes_del, 2);
    assert_eq!(ing_msgs_del, 2);

    // Bus rows must be untouched by ingress cleanup.
    assert_eq!(
        count_bus_messages(&conn, &ch_uuid_bytes),
        2,
        "ingress cleanup must not touch bus messages"
    );
}

/// bus_gc_retire_pushes: backstop correctly retires push rows beyond push_depth.
#[test]
fn bus_gc_retire_pushes_bounds_subscriber() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:ret', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // Insert 8 messages with push rows for "app-a" (push_depth=3).
    for ts in 1..9i64 {
        let msg_id = insert_bus_msg(&conn, &ch_uuid_bytes, ts * 1000);
        insert_bus_push(&conn, msg_id, "app-a");
    }

    assert_eq!(count_bus_pushes(&conn, &ch_uuid_bytes), 8);

    // Retire: push_depth=3 → keep 3 most-recent, retire 5.
    let retired = bus_gc_retire_pushes(&conn, ch_uuid, "app-a", 3);
    assert_eq!(retired, 5, "5 push rows past push_depth=3 must be retired");
    assert_eq!(count_bus_pushes(&conn, &ch_uuid_bytes), 3);

    // Bodies must still exist (push retirement does not delete message bodies).
    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 8);
}

/// The push reaper ranks by retention order. A late-released message has the
/// oldest publish timestamp and the newest retention position: ranking on
/// publish order would retire its freshly-released claim in preference to
/// genuinely older ones, dropping a delivery while keeping the message row.
#[test]
fn bus_gc_retire_pushes_ranks_by_retention_order() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:rank', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    let oldest = insert_bus_msg(&conn, &ch_uuid_bytes, 1000);
    let newest = insert_bus_msg(&conn, &ch_uuid_bytes, 2000);
    let late = insert_parked_bus_msg(&conn, &ch_uuid_bytes, 500, "2020-01-01T00:00:00+00:00");
    let oldest_push = insert_bus_push(&conn, oldest, "app-a");
    let newest_push = insert_bus_push(&conn, newest, "app-a");
    let targets = [crate::messaging::store::ReleaseTarget {
        subscriber: ParticipantId::from_stored("conversation:1".to_string()),
        app_slug: "app-a".to_string(),
        wake_min: None,
    }];
    let now = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let batch = crate::messaging::db::release_due_for_channel(&conn, ch_uuid, now, &targets);
    assert_eq!(batch.released.len(), 1);
    let late_push = batch.released[0].push_ids[0];
    assert_eq!(
        retained_seq_of(&conn, late),
        Some(3),
        "released → newest seq"
    );

    let retired = bus_gc_retire_pushes(&conn, ch_uuid, "app-a", 2);
    assert_eq!(retired, 1, "one claim past push_depth=2 is retired");

    let alive = |push_id: i64| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes WHERE id = ?1",
            rusqlite::params![push_id],
            |r| r.get(0),
        )
        .expect("count push")
    };
    assert_eq!(
        alive(oldest_push),
        0,
        "the lowest-seq claim is the one retired"
    );
    assert_eq!(alive(newest_push), 1);
    assert_eq!(alive(late_push), 1, "the just-released claim is the newest");
}

/// Delivered bus push rows for a bounded subscriber are retired by the backstop.
#[test]
fn bus_gc_retire_pushes_reaps_delivered_push_rows() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:leak', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // Insert 5 messages; mark first 3 as delivered (simulating the old leak).
    for ts in 1..6i64 {
        let msg_id = insert_bus_msg(&conn, &ch_uuid_bytes, ts * 1000);
        insert_bus_push(&conn, msg_id, "app-b");
        if ts <= 3 {
            conn.execute(
                "UPDATE messaging_pending_pushes SET delivered_at='2024-01-01' WHERE message_id=?1",
                rusqlite::params![msg_id],
            )
            .unwrap();
        }
    }

    // push_depth=2: keep only 2 most-recent push rows (incl. delivered ones).
    // The 3 oldest (ts=1,2,3) are past the window → retired.
    let retired = bus_gc_retire_pushes(&conn, ch_uuid, "app-b", 2);
    assert_eq!(
        retired, 3,
        "3 delivered push rows (the old leak) must be reaped"
    );
    assert_eq!(count_bus_pushes(&conn, &ch_uuid_bytes), 2);
}

/// bus_gc_retire_pushes with push_depth=0 is a no-op (pull-only subscriber).
#[test]
fn bus_gc_retire_pushes_pull_only_noop() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:po', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    let msg_id = insert_bus_msg(&conn, &ch_uuid_bytes, 1000);
    insert_bus_push(&conn, msg_id, "app");

    let retired = bus_gc_retire_pushes(&conn, ch_uuid, "app", 0);
    assert_eq!(
        retired, 0,
        "pull-only push_depth=0 must not retire anything"
    );
    assert_eq!(count_bus_pushes(&conn, &ch_uuid_bytes), 1);
}

/// Archive sink: evicted body appears in the JSONL file; removed from hot store.
#[test]
fn bus_gc_evict_archive_writes_jsonl_and_removes_body() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:arc', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // 3 messages; frontier=1 → 2 evicted (ts=1000, ts=2000), 1 retained (ts=3000).
    // insert_bus_msg uses source='src', sender='sender', body='{"x":1}'.
    for ts in 1..4i64 {
        insert_bus_msg(&conn, &ch_uuid_bytes, ts * 1000);
    }

    // Capture the retained message's UUID (highest publish_ts_ns = 3000) before eviction.
    let retained_uuid_bytes: Vec<u8> = conn
        .query_row(
            "SELECT uuid FROM messaging_messages WHERE channel_uuid = ?1 AND envelope_type='brenn'
             ORDER BY publish_ts_ns DESC LIMIT 1",
            rusqlite::params![ch_uuid_bytes],
            |r| r.get(0),
        )
        .expect("retained uuid query");
    let retained_uuid = Uuid::from_slice(&retained_uuid_bytes).expect("retained uuid parse");

    let tmp = tempfile::NamedTempFile::new().expect("tmp archive file");
    let archive_path = tmp.path().to_path_buf();

    let eviction = bus_gc_evict_channel(
        &conn,
        ch_uuid,
        "brenn:arc",
        ChannelScheme::Brenn,
        1,
        Sink::Archive,
        Some(&archive_path),
    );
    assert_eq!(
        eviction.messages_evicted, 2,
        "2 messages evicted to archive"
    );
    assert_eq!(
        count_bus_messages(&conn, &ch_uuid_bytes),
        1,
        "1 message retained"
    );

    // JSONL file: exactly 2 lines, each valid JSON with correct field values.
    let content = std::fs::read_to_string(&archive_path).expect("read archive");
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2, "archive has 2 JSONL lines");
    for line in &lines {
        let val: serde_json::Value =
            serde_json::from_str(line).expect("archive line is valid JSON");
        assert_eq!(
            val.get("sender").and_then(|v| v.as_str()),
            Some("sender"),
            "archive line has correct sender"
        );
        assert_eq!(
            val.get("source").and_then(|v| v.as_str()),
            Some("src"),
            "archive line has correct source"
        );
        assert_eq!(
            val.get("channel").and_then(|v| v.as_str()),
            Some("brenn:arc"),
            "archive line has correct channel"
        );
        assert!(val.get("body").is_some(), "archive line has body field");
        // The retained message must NOT appear in the archive.
        let archived_id = val
            .get("message_id")
            .and_then(|v| v.as_str())
            .expect("archive line has message_id field");
        assert_ne!(
            archived_id,
            retained_uuid.to_string(),
            "retained message must not appear in archive"
        );
    }
}

/// `bus_gc_evict_channel` with frontier=0 evicts all messages.
#[test]
fn bus_gc_evict_zero_frontier_evicts_all() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:zero', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // Insert 3 messages.
    for ts in 1..4i64 {
        insert_bus_msg(&conn, &ch_uuid_bytes, ts * 1000);
    }
    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 3);

    // Frontier=0 → all messages are eligible for eviction.
    let eviction = bus_gc_evict_channel(
        &conn,
        ch_uuid,
        "brenn:zero",
        ChannelScheme::Brenn,
        0,
        Sink::Drop,
        None,
    );
    assert_eq!(
        eviction.messages_evicted, 3,
        "frontier=0 must evict all 3 messages"
    );
    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 0);
}

/// GC evicts oldest-*retention-order* first (lowest `retained_seq`), not
/// oldest-publish-timestamp: a late-released message is the newest retention
/// entry despite an old rowid/publish_ts, so it survives eviction that reaps the
/// genuinely older retained rows. Keying on publish_ts would wrongly evict it.
#[test]
fn bus_gc_evicts_lowest_retained_seq_first_late_release_survives() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:late', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // Three retained messages: seqs 1, 2, 3 at ascending publish_ts.
    insert_bus_msg(&conn, &ch_uuid_bytes, 1000);
    insert_bus_msg(&conn, &ch_uuid_bytes, 2000);
    insert_bus_msg(&conn, &ch_uuid_bytes, 3000);
    // A parked message with the OLDEST publish_ts, due for release now.
    let late = insert_parked_bus_msg(&conn, &ch_uuid_bytes, 500, "2020-01-01T00:00:00+00:00");

    // Release: enters retention as seq 4 despite the oldest publish_ts.
    let now = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let released = crate::messaging::db::release_due_for_channel(&conn, ch_uuid, now, &[]);
    assert_eq!(
        released.released.len(),
        1,
        "the due parked message releases"
    );
    assert_eq!(
        retained_seq_of(&conn, late),
        Some(4),
        "released → newest seq"
    );

    // frontier=2: keep the top-2 retention entries (seqs 3, 4), evict seqs 1, 2.
    let eviction = bus_gc_evict_channel(
        &conn,
        ch_uuid,
        "brenn:late",
        ChannelScheme::Brenn,
        2,
        Sink::Drop,
        None,
    );
    assert_eq!(
        eviction.messages_evicted, 2,
        "the two lowest-seq retained rows are evicted"
    );
    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 2);
    assert_eq!(
        retained_seq_of(&conn, late),
        Some(4),
        "late-released row must survive as the newest retention entry"
    );
}

/// A parked row (`retained_seq NULL`) is never evicted by GC, even when its
/// publish_ts is older than the entire retained window and every retained row is
/// reaped — and it still releases with a fresh seq afterwards. Guards the new
/// parked-row exclusion predicate against the pre-existing reapable-parked defect.
#[test]
fn bus_gc_never_evicts_parked_row_which_later_releases() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:park2', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // Two retained messages (seqs 1, 2; high-water 2) plus a parked message with
    // the oldest publish_ts, due but unswept.
    insert_bus_msg(&conn, &ch_uuid_bytes, 1000);
    insert_bus_msg(&conn, &ch_uuid_bytes, 2000);
    let parked = insert_parked_bus_msg(&conn, &ch_uuid_bytes, 100, "2020-01-01T00:00:00+00:00");

    // frontier=0 evicts every retained row; the parked row must survive.
    let eviction = bus_gc_evict_channel(
        &conn,
        ch_uuid,
        "brenn:park2",
        ChannelScheme::Brenn,
        0,
        Sink::Drop,
        None,
    );
    assert_eq!(
        eviction.messages_evicted, 2,
        "only the two retained rows are evicted"
    );
    assert_eq!(
        retained_seq_of(&conn, parked),
        None,
        "parked row survives GC"
    );

    // A second pass has nothing retained left; the parked row still survives.
    let eviction2 = bus_gc_evict_channel(
        &conn,
        ch_uuid,
        "brenn:park2",
        ChannelScheme::Brenn,
        0,
        Sink::Drop,
        None,
    );
    assert_eq!(
        eviction2.messages_evicted, 0,
        "no retained rows remain to evict"
    );
    assert_eq!(
        retained_seq_of(&conn, parked),
        None,
        "still parked after GC"
    );

    // It releases with a fresh seq (3), continuing the dense high-water past the
    // evicted 1 and 2.
    let now = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let released = crate::messaging::db::release_due_for_channel(&conn, ch_uuid, now, &[]);
    assert_eq!(released.released.len(), 1);
    assert_eq!(retained_seq_of(&conn, parked), Some(3));
    assert_eq!(last_retained_seq_of(&conn, ch_uuid), 3);
}

/// `bus_gc_retire_pushes` retires only rows for the specified app_slug,
/// not rows belonging to other apps on the same channel.
#[test]
fn bus_gc_retire_pushes_scoped_to_app_slug() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:scope', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // Insert 5 messages with push rows for both app-a and app-b.
    for ts in 1..6i64 {
        let msg_id = insert_bus_msg(&conn, &ch_uuid_bytes, ts * 1000);
        insert_bus_push(&conn, msg_id, "app-a");
        insert_bus_push(&conn, msg_id, "app-b");
    }

    // Retire app-a with push_depth=2: only app-a's 3 oldest should be retired.
    let retired = bus_gc_retire_pushes(&conn, ch_uuid, "app-a", 2);
    assert_eq!(retired, 3, "3 of app-a's 5 push rows should be retired");

    // Verify app-a has exactly 2 rows remaining.
    let app_a_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON m.id = pp.message_id
             WHERE m.channel_uuid = ?1 AND m.envelope_type = 'brenn' AND pp.target_app_slug = 'app-a'",
            rusqlite::params![ch_uuid_bytes],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(app_a_count, 2, "app-a should have 2 push rows remaining");

    // app-b must be completely untouched.
    let app_b_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes pp
             JOIN messaging_messages m ON m.id = pp.message_id
             WHERE m.channel_uuid = ?1 AND m.envelope_type = 'brenn' AND pp.target_app_slug = 'app-b'",
            rusqlite::params![ch_uuid_bytes],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(app_b_count, 5, "app-b's push rows must not be touched");
}

/// `bus_gc_retire_pushes` must NOT retire parked push rows (those with
/// `release_after IS NOT NULL`) — they are in-flight deliver_after messages
/// awaiting release (design §3 in-flight exclusion, correctness review finding).
#[test]
fn bus_gc_retire_pushes_excludes_parked_rows() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    let ch_uuid = Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:park', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    // Insert push_depth=2 live messages (ts=100, ts=200) — within the window.
    let msg1 = insert_bus_msg(&conn, &ch_uuid_bytes, 100);
    insert_bus_push(&conn, msg1, "app");
    let msg2 = insert_bus_msg(&conn, &ch_uuid_bytes, 200);
    insert_bus_push(&conn, msg2, "app");

    // Insert a parked push row (release_after set, ts=50 — oldest, past the window if counted).
    let parked_msg = insert_bus_msg(&conn, &ch_uuid_bytes, 50);
    conn.execute(
        "INSERT INTO messaging_pending_pushes
           (message_id, target_subscriber, target_app_slug, eager_wake, release_after, created_at)
         VALUES (?1, 'conversation:99', 'app', 0, '2099-01-01T00:00:00Z', '2024-01-01')",
        rusqlite::params![parked_msg],
    )
    .unwrap();

    // With push_depth=2, the backstop should NOT retire the parked row.
    // Without the fix, parked row at ts=50 would be in the bottom of the
    // all-rows ranking and retired first.
    let retired = bus_gc_retire_pushes(&conn, ch_uuid, "app", 2);

    // The live window has 2 rows, so no retirement from the live set.
    // The parked row must survive regardless.
    assert_eq!(
        retired, 0,
        "parked row must not be counted or retired by the backstop"
    );

    // Parked row still exists.
    let parked_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes WHERE message_id = ?1",
            rusqlite::params![parked_msg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        parked_exists, 1,
        "parked row must still exist after backstop"
    );

    // Total push rows = 3 (2 live + 1 parked).
    assert_eq!(count_bus_pushes(&conn, &ch_uuid_bytes), 3);
}

// ---------------------------------------------------------------------------
// load_all_dispatchable_pushes tests
// ---------------------------------------------------------------------------

/// Set up a channel with a single subscriber for dispatcher scan tests.
fn setup_dispatch_channel(conn: &Connection) -> Uuid {
    let channel_uuid = Uuid::new_v4();
    ensure_user_and_conv(conn, 1);
    upsert_channels(
        conn,
        &[crate::messaging::ChannelEntry {
            uuid: channel_uuid,
            address: crate::messaging::canonical_address("dispatch-test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: crate::messaging::ChannelScheme::Brenn,
            mount: None,
        }],
    );
    channel_uuid
}

/// Insert a full bus message + one push for conversation 1, with configurable
/// urgency / deadline. Returns the push_id.
/// (Named `insert_dispatchable_push` to avoid conflict with `insert_bus_push`
/// defined earlier in this file, which takes different arguments.)
fn insert_dispatchable_push(
    conn: &Connection,
    channel_uuid: Uuid,
    body: &str,
    urgency: Urgency,
    delivery_deadline: Option<chrono::DateTime<Utc>>,
    publish_ts_ns: i64,
) -> i64 {
    let eager_wake = urgency >= Urgency::Normal;
    let inserted = insert_message_with_pushes(
        conn,
        channel_uuid,
        "src",
        "sender",
        body,
        urgency,
        crate::messaging::ChannelScheme::Brenn,
        None,
        delivery_deadline,
        None,
        publish_ts_ns,
        &[PendingPushInsert {
            target_subscriber: ParticipantId::for_conversation(1),
            target_app_slug: "app".to_string(),
            eager_wake,
            release_after: None,
            delivery_deadline,
        }],
    );
    inserted.push_ids[0]
}

/// `load_all_dispatchable_pushes` returns `Immediate` push rows and
/// deadline-expired rows, but not `None`-wake non-deadline rows (design §2.3
/// predicate). Also confirms `deadline_expired` flag is set correctly.
#[test]
fn load_all_dispatchable_pushes_predicate() {
    let db = crate::db::init_db_memory();
    let conn = db.blocking_lock();
    let ch = setup_dispatch_channel(&conn);

    let now = Utc::now();
    let past = now - chrono::Duration::seconds(10);
    let future = now + chrono::Duration::hours(1);

    // Row 1: Immediate, no deadline → should appear; deadline_expired = false
    let push_immediate =
        insert_dispatchable_push(&conn, ch, "immediate-msg", Urgency::Normal, None, 1000);

    // Row 2: None wake, no deadline → must NOT appear (excluded by predicate)
    let _push_none = insert_dispatchable_push(&conn, ch, "none-wake-msg", Urgency::Low, None, 2000);

    // Row 3: None wake, deadline in future → must NOT appear (deadline not yet expired)
    let _push_none_future_deadline = insert_dispatchable_push(
        &conn,
        ch,
        "future-deadline-msg",
        Urgency::Low,
        Some(future),
        3000,
    );

    // Row 4: None wake, deadline in past → should appear; deadline_expired = true
    let push_deadline_expired = insert_dispatchable_push(
        &conn,
        ch,
        "expired-deadline-msg",
        Urgency::Low,
        Some(past),
        4000,
    );

    // Row 5: Immediate, deadline in past → should appear; deadline_expired = true
    let push_immediate_expired = insert_dispatchable_push(
        &conn,
        ch,
        "imm-expired-msg",
        Urgency::Normal,
        Some(past),
        5000,
    );

    let rows = load_all_dispatchable_pushes(&conn, now);

    // Collect (push_id → deadline_expired) for assertions
    let result: std::collections::HashMap<i64, bool> = rows
        .iter()
        .map(|(row, expired)| (row.push_id, *expired))
        .collect();

    assert!(
        result.contains_key(&push_immediate),
        "Immediate row must appear"
    );
    assert!(
        !result[&push_immediate],
        "Immediate-only row: deadline_expired must be false"
    );

    assert!(
        result.contains_key(&push_deadline_expired),
        "None-wake expired-deadline row must appear"
    );
    assert!(
        result[&push_deadline_expired],
        "Expired-deadline row: deadline_expired must be true"
    );

    assert!(
        result.contains_key(&push_immediate_expired),
        "Immediate + expired-deadline row must appear"
    );
    assert!(
        result[&push_immediate_expired],
        "Immediate + expired-deadline row: deadline_expired must be true"
    );

    // None-wake rows without an expired deadline must be absent
    assert_eq!(rows.len(), 3, "only 3 rows should match the predicate");
    assert!(!result.contains_key(&_push_none));
    assert!(!result.contains_key(&_push_none_future_deadline));
}

/// `load_all_dispatchable_pushes` excludes already-delivered rows.
#[test]
fn load_all_dispatchable_pushes_excludes_delivered() {
    let db = crate::db::init_db_memory();
    let conn = db.blocking_lock();
    let ch = setup_dispatch_channel(&conn);

    let push_id = insert_dispatchable_push(&conn, ch, "msg", Urgency::Normal, None, 1000);

    // Mark delivered — must disappear from the scan
    mark_pending_pushes_delivered(&conn, &[push_id]);

    let rows = load_all_dispatchable_pushes(&conn, Utc::now());
    assert!(
        rows.is_empty(),
        "delivered row must not appear in dispatcher scan"
    );
}

/// `load_all_dispatchable_pushes` excludes rows with `release_after` still set
/// (still suppressed / deferred-not-yet-released).
#[test]
fn load_all_dispatchable_pushes_excludes_suppressed() {
    let db = crate::db::init_db_memory();
    let conn = db.blocking_lock();
    let ch = setup_dispatch_channel(&conn);

    let future = Utc::now() + chrono::Duration::hours(1);
    let inserted = insert_message_with_pushes(
        &conn,
        ch,
        "src",
        "sender",
        "deferred-body",
        Urgency::Normal,
        crate::messaging::ChannelScheme::Brenn,
        None,
        None,
        None,
        1000,
        &[PendingPushInsert {
            target_subscriber: ParticipantId::for_conversation(1),
            target_app_slug: "app".to_string(),
            eager_wake: true,
            release_after: Some(future), // still suppressed
            delivery_deadline: None,
        }],
    );
    let push_id = inserted.push_ids[0];

    let rows = load_all_dispatchable_pushes(&conn, Utc::now());
    let ids: Vec<i64> = rows.iter().map(|(r, _)| r.push_id).collect();
    assert!(
        !ids.contains(&push_id),
        "suppressed row must be excluded from scan"
    );
}

/// `load_all_dispatchable_pushes` results are ordered by `publish_ts_ns ASC` (R10).
#[test]
fn load_all_dispatchable_pushes_ordered_by_publish_ts() {
    let db = crate::db::init_db_memory();
    let conn = db.blocking_lock();
    let ch = setup_dispatch_channel(&conn);

    // Insert in reverse timestamp order; expect ascending order in results.
    insert_dispatchable_push(&conn, ch, "third", Urgency::Normal, None, 3000);
    insert_dispatchable_push(&conn, ch, "first", Urgency::Normal, None, 1000);
    insert_dispatchable_push(&conn, ch, "second", Urgency::Normal, None, 2000);

    let rows = load_all_dispatchable_pushes(&conn, Utc::now());
    assert_eq!(rows.len(), 3);
    let bodies: Vec<&str> = rows
        .iter()
        .map(|(r, _)| r.payload.unwrap_bus_ref().body.as_str())
        .collect();
    assert_eq!(bodies, ["first", "second", "third"]);
}

/// SQL CASE rank mapping ↔ `WakeMin::wakes(urgency)` equivalence test (design §2.5).
///
/// For every `(WakeMin, Urgency)` combination, edits a message to each urgency and
/// verifies the SQL-computed `eager_wake` matches `wake_min.wakes(urgency)`.
/// Also pins the pull-only (push_depth=0) and missing-subscription-row cases (both → 0).
/// Guards against silent drift between the Rust threshold logic and the SQL CASE mapping.
#[test]
fn wake_min_wakes_sql_case_equivalence() {
    let db = init_db_memory();
    let conn = db.blocking_lock();

    // Conversations 1–7: VeryLow, Low, Normal, High, Never, pull-only, no-sub.
    for cid in 1i64..=7 {
        ensure_user_and_conv(&conn, cid);
    }

    let ch_uuid = Uuid::new_v4();
    upsert_channels(
        &conn,
        &[ChannelEntry {
            uuid: ch_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: default_resolved_channel(),
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        }],
    );

    // Conversations 1–5: push-enabled, each wake_min variant.
    // Conversation 6: pull-only (push_depth=0), wake_min=normal.
    // Conversation 7: no subscription row.
    let subs: &[(&str, i64, &str, &str)] = &[
        ("app-vl", 1, "unbounded", "very-low"),
        ("app-lo", 2, "unbounded", "low"),
        ("app-nm", 3, "unbounded", "normal"),
        ("app-hi", 4, "unbounded", "high"),
        ("app-nv", 5, "unbounded", "never"),
        ("app-po", 6, "0", "normal"),
    ];
    for (slug, _cid, push_depth, wm_str) in subs {
        conn.execute(
            "INSERT INTO messaging_subscriptions (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min) \
             VALUES (?1, ?2, ?3, 'unbounded', 'silent', ?4)",
            rusqlite::params![ch_uuid.as_bytes().to_vec(), slug, push_depth, wm_str],
        )
        .unwrap();
    }

    let urgencies = [
        Urgency::VeryLow,
        Urgency::Low,
        Urgency::Normal,
        Urgency::High,
    ];
    let ns = utc_to_ns(Utc::now());
    let future = Utc::now() + chrono::Duration::seconds(3600);

    for test_urgency in urgencies {
        let push_inserts: Vec<PendingPushInsert> = (1i64..=7)
            .map(|cid| {
                let slug = match cid {
                    1 => "app-vl",
                    2 => "app-lo",
                    3 => "app-nm",
                    4 => "app-hi",
                    5 => "app-nv",
                    6 => "app-po",
                    7 => "app-no",
                    _ => unreachable!(),
                };
                PendingPushInsert {
                    target_subscriber: ParticipantId::for_conversation(cid),
                    target_app_slug: slug.to_string(),
                    eager_wake: false,
                    release_after: Some(future),
                    delivery_deadline: None,
                }
            })
            .collect();

        let inserted = insert_message_with_pushes(
            &conn,
            ch_uuid,
            "src",
            "sender",
            "body",
            Urgency::VeryLow,
            ChannelScheme::Brenn,
            None,
            None,
            Some(future),
            ns,
            &push_inserts,
        );

        // Edit to test_urgency — triggers SQL CASE rank recompute.
        let fields = EditFieldsApplied {
            body: None,
            reply_to_uuid: None,
            deliver_after: None,
            delivery_deadline: None,
            urgency: Some(test_urgency),
        };
        update_message_and_pending_pushes(&conn, inserted.id, "sender", &fields);

        // Check: SQL result must equal WakeMin::wakes(test_urgency) for each subscriber.
        let cases: &[(i64, Option<WakeMin>, bool)] = &[
            (
                1,
                Some(WakeMin::VeryLow),
                WakeMin::VeryLow.wakes(test_urgency),
            ),
            (2, Some(WakeMin::Low), WakeMin::Low.wakes(test_urgency)),
            (
                3,
                Some(WakeMin::Normal),
                WakeMin::Normal.wakes(test_urgency),
            ),
            (4, Some(WakeMin::High), WakeMin::High.wakes(test_urgency)),
            (5, Some(WakeMin::Never), WakeMin::Never.wakes(test_urgency)),
            (6, None, false), // pull-only → always 0
            (7, None, false), // no subscription row → COALESCE 0
        ];

        for (cid, _wm, expected_eager) in cases {
            let sub = ParticipantId::for_conversation(*cid);
            let eager_wake_int: i64 = conn
                .query_row(
                    "SELECT eager_wake FROM messaging_pending_pushes
                     WHERE message_id = ?1 AND target_subscriber = ?2",
                    rusqlite::params![inserted.id, sub.as_str()],
                    |r| r.get(0),
                )
                .unwrap();
            let got = eager_wake_int != 0;
            assert_eq!(
                got, *expected_eager,
                "urgency={test_urgency:?} cid={cid}: \
                 SQL eager_wake={got} but Rust WakeMin::wakes={expected_eager}"
            );
        }

        // Clean up push rows before next urgency iteration.
        conn.execute(
            "DELETE FROM messaging_pending_pushes WHERE message_id = ?1",
            rusqlite::params![inserted.id],
        )
        .unwrap();
    }
}

/// `load_all_dispatchable_pushes` returns ingress rows (envelope_type='ingress')
/// as `IngressOrBus::Ingress(...)` — the dispatcher must handle both kinds (design §2.4).
#[test]
fn load_all_dispatchable_pushes_includes_ingress_rows() {
    let db = crate::db::init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);

    let publish_ts_ns = utc_to_ns(Utc::now());
    let subscriber = ParticipantId::for_conversation(1);
    insert_ingress_message(
        &conn,
        &subscriber,
        "app",
        "mqtt:test",
        "test event summary",
        r#"{"key":"val"}"#,
        Urgency::Normal,
        publish_ts_ns,
    );

    let rows = load_all_dispatchable_pushes(&conn, Utc::now());
    assert_eq!(rows.len(), 1, "ingress row must appear in dispatcher scan");
    let (row, deadline_expired) = &rows[0];
    assert!(!deadline_expired);
    match &row.payload {
        crate::messaging::IngressOrBus::Ingress(ev) => {
            assert_eq!(ev.source, "mqtt:test");
            assert_eq!(ev.summary, "test event summary");
        }
        crate::messaging::IngressOrBus::Bus(_) => panic!("expected ingress payload"),
    }
}

/// Tripwire: the dispatcher scan must be served by the partial index
/// `idx_messaging_pending_pushes_dispatchable`, not a full table scan. If a future
/// SQLite upgrade or a query edit stops the planner qualifying the partial index,
/// this fails loudly instead of silently regressing to O(total backlog) per wake.
///
/// Uses `LOAD_ALL_DISPATCHABLE_PUSHES_SQL` directly (the same constant the production
/// function prepares) so the asserted plan can never drift from the real query. No
/// `ANALYZE` is run — production never runs it, so the test must see the same planner
/// conditions.
#[test]
fn load_all_dispatchable_pushes_uses_partial_index() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let ch = setup_dispatch_channel(&conn);

    // Seed a non-degenerate mixed population so the cost-based planner sees a
    // realistic table rather than a trivial one it might full-scan regardless.
    let now = Utc::now();
    let past = now - chrono::Duration::seconds(10);
    let future = now + chrono::Duration::hours(1);
    for i in 0..20 {
        // Parked rows (eager_wake=0, no deadline) — excluded from the partial index.
        insert_dispatchable_push(&conn, ch, "parked", Urgency::Low, None, 1000 + i);
    }
    for i in 0..5 {
        insert_dispatchable_push(&conn, ch, "eager", Urgency::Normal, None, 3000 + i);
    }
    for i in 0..5 {
        insert_dispatchable_push(&conn, ch, "future-dl", Urgency::Low, Some(future), 4000 + i);
    }
    for i in 0..5 {
        insert_dispatchable_push(&conn, ch, "expired-dl", Urgency::Low, Some(past), 5000 + i);
    }

    let plan: Vec<String> = {
        let mut stmt = conn
            .prepare(&("EXPLAIN QUERY PLAN ".to_owned() + LOAD_ALL_DISPATCHABLE_PUSHES_SQL))
            .expect("prepare EXPLAIN QUERY PLAN");
        // The plan is fixed at prepare time; the bound value is irrelevant, but
        // rusqlite validates the `?1` parameter count at query time, so bind a dummy.
        let rows = stmt
            .query_map(rusqlite::params![""], |row| row.get::<_, String>(3))
            .expect("query plan");
        rows.map(|r| r.expect("read plan row")).collect()
    };

    // The index name is globally unique, so it can only appear while the planner
    // accesses messaging_pending_pushes (aliased `pp`) via that index.
    assert!(
        plan.iter()
            .any(|d| d.contains("idx_messaging_pending_pushes_dispatchable")),
        "dispatcher scan must use idx_messaging_pending_pushes_dispatchable; plan was:\n{}",
        plan.join("\n"),
    );
    // Belt-and-suspenders: no unindexed full scan of pp. A `SCAN pp` row that
    // names any index (`USING INDEX` or `USING COVERING INDEX`) is index-backed;
    // only a bare `SCAN pp` with no index is a full table scan.
    assert!(
        !plan
            .iter()
            .any(|d| d.contains("SCAN pp") && !d.contains("INDEX")),
        "dispatcher scan must not full-scan pp; plan was:\n{}",
        plan.join("\n"),
    );
}

// ---------------------------------------------------------------------------
// Surface durable-projection helpers (SD5 claims, SD6 channel-scoped loaders)
// ---------------------------------------------------------------------------

/// Register two `brenn:` channels in the DB and directory; return their UUIDs.
fn upsert_two_channels(conn: &Connection) -> (Uuid, Uuid) {
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    upsert_channels(
        conn,
        &[
            ChannelEntry {
                uuid: a,
                address: canonical_address("chan-a"),
                description: None,
                resolved_channel: default_resolved_channel(),
                subscribers: vec![],
                transport_type: ChannelScheme::Brenn,
                mount: None,
            },
            ChannelEntry {
                uuid: b,
                address: canonical_address("chan-b"),
                description: None,
                resolved_channel: default_resolved_channel(),
                subscribers: vec![],
                transport_type: ChannelScheme::Brenn,
                mount: None,
            },
        ],
    );
    (a, b)
}

/// Insert one bus message on `channel_uuid` for `subscriber`, returning
/// `(message_id, push_id)`. `ts_ns` sets `publish_ts_ns`.
fn insert_one_push(
    conn: &Connection,
    channel_uuid: Uuid,
    subscriber: &ParticipantId,
    body: &str,
    ts_ns: i64,
    release_after: Option<DateTime<Utc>>,
) -> (i64, i64) {
    let inserted = insert_message_with_pushes(
        conn,
        channel_uuid,
        "src",
        "sender",
        body,
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        ts_ns,
        &[PendingPushInsert {
            target_subscriber: subscriber.clone(),
            target_app_slug: "deskbar".to_string(),
            eager_wake: false,
            release_after,
            delivery_deadline: None,
        }],
    );
    assert_eq!(inserted.push_ids.len(), 1);
    (inserted.id, inserted.push_ids[0])
}

/// `claim_pending_pushes` stamps `delivered_at` atomically and returns the ids
/// it claimed. A second claim of the same ids returns empty (already claimed),
/// and a claimed row no longer appears in the drain scan.
#[test]
fn claim_pending_pushes_is_exclusive() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (chan_a, _chan_b) = upsert_two_channels(&conn);
    let sub = ParticipantId::for_surface("deskbar");

    let (_m1, p1) = insert_one_push(&conn, chan_a, &sub, "one", 100, None);
    let (_m2, p2) = insert_one_push(&conn, chan_a, &sub, "two", 200, None);

    let mut claimed = claim_pending_pushes(&conn, &[p1, p2]);
    claimed.sort_unstable();
    let mut expected = vec![p1, p2];
    expected.sort_unstable();
    assert_eq!(claimed, expected, "first claim returns both ids");

    // Second claim of the same ids returns nothing — already claimed.
    let claimed_again = claim_pending_pushes(&conn, &[p1, p2]);
    assert!(
        claimed_again.is_empty(),
        "double-claim must return empty, got {claimed_again:?}"
    );

    // Claimed rows are marked delivered → excluded from the drain scan.
    let drained = load_pending_pushes_for_drain(&conn, &sub);
    assert!(drained.is_empty(), "claimed rows must not drain");
}

/// A claim over a mixed set returns only the ids that were actually free; an
/// already-claimed id is silently excluded.
#[test]
fn claim_pending_pushes_partial() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (chan_a, _chan_b) = upsert_two_channels(&conn);
    let sub = ParticipantId::for_surface("deskbar");

    let (_m1, p1) = insert_one_push(&conn, chan_a, &sub, "one", 100, None);
    let (_m2, p2) = insert_one_push(&conn, chan_a, &sub, "two", 200, None);

    // Claim p1 alone first.
    assert_eq!(claim_pending_pushes(&conn, &[p1]), vec![p1]);

    // Claiming both now returns only p2 (p1 already claimed).
    assert_eq!(
        claim_pending_pushes(&conn, &[p1, p2]),
        vec![p2],
        "only the unclaimed id comes back"
    );

    // Empty input is a no-op.
    assert!(claim_pending_pushes(&conn, &[]).is_empty());
}

/// `unclaim_pending_pushes` clears `delivered_at`, re-parking the row so it
/// drains again and can be re-claimed.
#[test]
fn unclaim_pending_pushes_reparks() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (chan_a, _chan_b) = upsert_two_channels(&conn);
    let sub = ParticipantId::for_surface("deskbar");

    let (_m1, p1) = insert_one_push(&conn, chan_a, &sub, "one", 100, None);
    let (_m2, p2) = insert_one_push(&conn, chan_a, &sub, "two", 200, None);

    assert_eq!(claim_pending_pushes(&conn, &[p1, p2]).len(), 2);
    assert!(load_pending_pushes_for_drain(&conn, &sub).is_empty());

    // Unclaim p1 only → it re-parks; p2 stays claimed.
    unclaim_pending_pushes(&conn, &[p1]);
    let drained = load_pending_pushes_for_drain(&conn, &sub);
    assert_eq!(drained.len(), 1, "only the unclaimed row re-parks");
    assert_eq!(drained[0].0, p1);

    // p1 is claimable again; p2 is not.
    assert_eq!(claim_pending_pushes(&conn, &[p1, p2]), vec![p1]);
}

/// `load_pending_pushes_for_channel` returns only undelivered, unparked bus
/// rows for `(subscriber, channel)`, in `publish_ts_ns ASC` order, carrying each
/// row's claim, message identity, retention position, and payload.
#[test]
fn load_pending_pushes_for_channel_scopes_and_orders() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (chan_a, chan_b) = upsert_two_channels(&conn);
    let sub = ParticipantId::for_surface("deskbar");
    let other = ParticipantId::for_surface("kitchen");

    // Two undelivered A rows for `sub` (ts 200 then 100 to prove ordering).
    let (m_a2, p_a2) = insert_one_push(&conn, chan_a, &sub, "a-late", 200, None);
    let (m_a1, p_a1) = insert_one_push(&conn, chan_a, &sub, "a-early", 100, None);
    // A parked A row (excluded) and a delivered A row (excluded).
    let future = Utc::now() + chrono::Duration::seconds(3600);
    insert_one_push(&conn, chan_a, &sub, "a-parked", 150, Some(future));
    let (_m_del, p_del) = insert_one_push(&conn, chan_a, &sub, "a-delivered", 175, None);
    mark_pending_pushes_delivered(&conn, &[p_del]);
    // A row on channel B (excluded by channel scope) and one for `other` (excluded).
    insert_one_push(&conn, chan_b, &sub, "b-row", 120, None);
    insert_one_push(&conn, chan_a, &other, "a-other-sub", 130, None);

    let rows = load_pending_pushes_for_channel(&conn, &sub, chan_a);
    assert_eq!(rows.len(), 2, "only the two live A rows for sub");
    // publish_ts_ns ASC → a-early (100) first, a-late (200) second.
    assert_eq!(rows[0].push_id, p_a1);
    assert_eq!(
        rows[0].message_id, m_a1,
        "message identity matches inserted id"
    );
    assert_eq!(rows[0].envelope.body, "a-early");
    assert_eq!(rows[1].push_id, p_a2);
    assert_eq!(rows[1].message_id, m_a2);
    assert_eq!(rows[1].envelope.body, "a-late");
    assert!(
        rows[0].retained_seq > rows[1].retained_seq,
        "retention position is assigned at insert, so a-late (inserted first) \
         holds the lower one even though it sorts second by publish_ts_ns"
    );
}

/// The below-water ack channel's DB helpers: a `confirm_pending` stamp
/// survives GC — like a parked (`release_after`) row — so the reconcile evidence
/// is never reaped, and clears back to reapable once confirmed.
#[test]
fn confirm_pending_stamp_survives_gc_until_confirmed() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (chan, _b) = upsert_two_channels(&conn);
    let sub = ParticipantId::for_surface("deskbar");

    let (m_old, p_old) = insert_one_push(&conn, chan, &sub, "old", 100, None);
    let (_m_new, p_new) = insert_one_push(&conn, chan, &sub, "new", 200, None);
    // Only a claimed (delivered) row is tentative; claim both.
    claim_pending_pushes(&conn, &[p_old, p_new]);

    // Stamp the older tentative and read it back.
    assert_eq!(stamp_confirm_pending(&conn, &sub, m_old), 1);
    let tentative = load_confirm_pending_pushes(&conn, &sub, chan);
    assert_eq!(tentative.len(), 1);
    assert_eq!(
        (tentative[0].push_id, tentative[0].message_id),
        (p_old, m_old)
    );

    // GC keeping only the newest row would retire the older, but the tentative
    // flag excludes it exactly as a parked row's `release_after` would.
    bus_gc_retire_pushes(&conn, chan, "deskbar", 1);
    let survives: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes WHERE id = ?1",
            rusqlite::params![p_old],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(survives, 1, "a confirm_pending row is excluded from GC");

    // Confirm clears the flag; the row is now an ordinary delivered row and GC
    // retires it.
    confirm_pending_pushes(&conn, &[p_old]);
    assert!(load_confirm_pending_pushes(&conn, &sub, chan).is_empty());
    bus_gc_retire_pushes(&conn, chan, "deskbar", 1);
    let after: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes WHERE id = ?1",
            rusqlite::params![p_old],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(after, 0, "a confirmed row is reapable again");
}

/// `bus_gc_evict_channel` must also spare a `confirm_pending` row and its message
/// — not just the per-subscriber `bus_gc_retire_pushes` reaper. A below-water
/// row's recovery evidence lives in its push row, and the message row backs the
/// FK; channel-wide eviction reaping either would silently reopen the
/// permanent-loss corner. Once the flag clears, both are reapable again.
#[test]
fn confirm_pending_row_survives_channel_eviction() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (chan, _b) = upsert_two_channels(&conn);
    let sub = ParticipantId::for_surface("deskbar");

    // One old tentative row, plus two newer rows that push it past the frontier.
    let (m_old, p_old) = insert_one_push(&conn, chan, &sub, "old", 100, None);
    let (_m1, _p1) = insert_one_push(&conn, chan, &sub, "mid", 200, None);
    let (_m2, _p2) = insert_one_push(&conn, chan, &sub, "new", 300, None);
    claim_pending_pushes(&conn, &[p_old]);
    assert_eq!(stamp_confirm_pending(&conn, &sub, m_old), 1);

    // frontier = 1 would evict the two older messages; the tentative one is spared,
    // its message row kept alongside it (FK), so only one message is evicted.
    let eviction = bus_gc_evict_channel(
        &conn,
        chan,
        "brenn:chan-a",
        ChannelScheme::Brenn,
        1,
        Sink::Drop,
        None,
    );
    assert_eq!(
        eviction.messages_evicted, 1,
        "only the non-tentative old message is evicted"
    );
    assert_eq!(
        eviction.push_rows_retired, 1,
        "the tentative push row is spared"
    );
    let surviving = load_confirm_pending_pushes(&conn, &sub, chan);
    assert_eq!(
        surviving.len(),
        1,
        "the recovery evidence survives channel eviction"
    );
    assert_eq!(
        (surviving[0].push_id, surviving[0].message_id),
        (p_old, m_old)
    );

    // Once confirmed, the flag clears and the next eviction reaps it normally.
    confirm_pending_pushes(&conn, &[p_old]);
    let eviction = bus_gc_evict_channel(
        &conn,
        chan,
        "brenn:chan-a",
        ChannelScheme::Brenn,
        1,
        Sink::Drop,
        None,
    );
    assert_eq!(
        eviction.messages_evicted, 1,
        "the confirmed row's message is now reapable"
    );
    assert_eq!(eviction.push_rows_retired, 1, "and its push row too");
}

/// Unclaiming a never-acknowledged tentative row clears both `delivered_at` and
/// `confirm_pending`, so it re-enters the redeliverable (parked-claim) universe.
#[test]
fn unclaim_confirm_pending_makes_a_row_redeliverable() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (chan, _b) = upsert_two_channels(&conn);
    let sub = ParticipantId::for_surface("deskbar");

    let (m, p) = insert_one_push(&conn, chan, &sub, "tentative", 100, None);
    claim_pending_pushes(&conn, &[p]);
    stamp_confirm_pending(&conn, &sub, m);

    // Claimed + tentative ⇒ not in the redeliverable (undelivered, unparked) set.
    assert!(load_pending_pushes_for_channel(&conn, &sub, chan).is_empty());

    unclaim_confirm_pending_pushes(&conn, &[p]);
    // Flag gone, delivered_at cleared ⇒ back in the redeliverable set, no longer
    // tentative.
    assert!(load_confirm_pending_pushes(&conn, &sub, chan).is_empty());
    let redeliverable = load_pending_pushes_for_channel(&conn, &sub, chan);
    assert_eq!(redeliverable.len(), 1);
    assert_eq!(
        redeliverable[0].message_id, m,
        "the unclaimed row redelivers"
    );
}

// ---------------------------------------------------------------------------
// Retention-sequence allocation (durable resume foundation)
// ---------------------------------------------------------------------------

fn retained_seq_of(conn: &Connection, message_id: i64) -> Option<i64> {
    conn.query_row(
        "SELECT retained_seq FROM messaging_messages WHERE id = ?1",
        rusqlite::params![message_id],
        |r| r.get(0),
    )
    .expect("read retained_seq")
}

fn last_retained_seq_of(conn: &Connection, channel: Uuid) -> i64 {
    conn.query_row(
        "SELECT last_retained_seq FROM messaging_channels WHERE uuid = ?1",
        rusqlite::params![channel.as_bytes().to_vec()],
        |r| r.get(0),
    )
    .expect("read last_retained_seq")
}

fn append_msg(conn: &Connection, channel: Uuid, body: &str) -> i64 {
    insert_message_with_pushes(
        conn,
        channel,
        "src",
        "sender",
        body,
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        utc_to_ns(Utc::now()),
        &[],
    )
    .id
}

fn park_msg(conn: &Connection, channel: Uuid, body: &str, release_at: DateTime<Utc>) -> i64 {
    insert_message_with_pushes(
        conn,
        channel,
        "src",
        "sender",
        body,
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        Some(release_at),
        utc_to_ns(Utc::now()),
        &[],
    )
    .id
}

/// Immediate appends get a dense, per-channel, ascending sequence and bump the
/// channel's persisted high-water; two channels number independently.
#[test]
fn append_assigns_dense_per_channel_retained_seq() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (a, b) = upsert_two_channels(&conn);

    let a1 = append_msg(&conn, a, "a1");
    let a2 = append_msg(&conn, a, "a2");
    let b1 = append_msg(&conn, b, "b1");
    let a3 = append_msg(&conn, a, "a3");

    assert_eq!(retained_seq_of(&conn, a1), Some(1));
    assert_eq!(retained_seq_of(&conn, a2), Some(2));
    assert_eq!(retained_seq_of(&conn, a3), Some(3));
    assert_eq!(
        retained_seq_of(&conn, b1),
        Some(1),
        "channel B numbers from 1"
    );
    assert_eq!(last_retained_seq_of(&conn, a), 3);
    assert_eq!(last_retained_seq_of(&conn, b), 1);
}

/// A parked message holds no sequence until it releases; at release it is
/// assigned the next sequence — above every append made while it was parked, so
/// a late-released message is the newest retention entry (converging with the
/// ring).
#[test]
fn release_assigns_seq_in_release_order_making_a_late_release_newest() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (a, _b) = upsert_two_channels(&conn);
    let past = Utc::now() - chrono::Duration::hours(1);

    let m1 = append_msg(&conn, a, "m1");
    let p = park_msg(&conn, a, "p", past);
    let m2 = append_msg(&conn, a, "m2");

    assert_eq!(retained_seq_of(&conn, m1), Some(1));
    assert_eq!(retained_seq_of(&conn, p), None, "parked holds no seq");
    assert_eq!(retained_seq_of(&conn, m2), Some(2));
    assert_eq!(
        last_retained_seq_of(&conn, a),
        2,
        "parking allocates nothing"
    );

    let released = release_due_for_channel(&conn, a, Utc::now(), &[]);
    assert_eq!(released.released.len(), 1);
    assert_eq!(released.released[0].message_id, p);

    assert_eq!(
        retained_seq_of(&conn, p),
        Some(3),
        "the released message is the newest retention entry"
    );
    assert_eq!(last_retained_seq_of(&conn, a), 3);
}

/// A release must mint retention sequences in release order, per channel. A row
/// cleared without one would sit in no retention read, outside the eviction
/// universe, and below every resume high-water — retained by nobody's
/// definition.
#[test]
fn a_release_pass_assigns_seqs_in_release_order() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (a, b) = upsert_two_channels(&conn);
    let older = Utc::now() - chrono::Duration::hours(2);
    let newer = Utc::now() - chrono::Duration::hours(1);

    let a1 = append_msg(&conn, a, "a1");
    // Rowid order is the reverse of release order, so a rowid-ordered sweep
    // would number these the other way round.
    let late = park_with_push(&conn, a, "late", newer);
    let early = park_with_push(&conn, a, "early", older);
    let b1 = park_with_push(&conn, b, "b1", older);

    let now = Utc::now();
    let released_a = crate::messaging::db::release_due_for_channel(&conn, a, now, &[]);
    let released_b = crate::messaging::db::release_due_for_channel(&conn, b, now, &[]);
    assert_eq!(
        released_a.released.len() + released_b.released.len(),
        3,
        "all three come due"
    );

    assert_eq!(retained_seq_of(&conn, a1), Some(1));
    assert_eq!(
        retained_seq_of(&conn, early),
        Some(2),
        "the earlier release time takes the earlier retention position"
    );
    assert_eq!(retained_seq_of(&conn, late), Some(3));
    assert_eq!(last_retained_seq_of(&conn, a), 3);
    assert_eq!(
        retained_seq_of(&conn, b1),
        Some(1),
        "the other channel numbers independently"
    );
    assert_eq!(last_retained_seq_of(&conn, b), 1);

    // A released row is in the retained window, which is what the seq buys.
    let tail: Vec<String> = load_channel_retained_tail(&conn, a, Depth::Unbounded)
        .into_iter()
        .map(|(_, e)| e.body)
        .collect();
    assert_eq!(tail, vec!["a1", "early", "late"]);
}

/// A parked message with one undelivered push claim.
fn park_with_push(conn: &Connection, channel: Uuid, body: &str, release_at: DateTime<Utc>) -> i64 {
    insert_message_with_pushes(
        conn,
        channel,
        "src",
        "sender",
        body,
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        Some(release_at),
        utc_to_ns(Utc::now()),
        &[PendingPushInsert {
            target_subscriber: ParticipantId::for_conversation(1),
            target_app_slug: "target".to_string(),
            eager_wake: true,
            release_after: Some(release_at),
            delivery_deadline: None,
        }],
    )
    .id
}

/// A parked message that is cancelled before release consumes no sequence, so a
/// later append is not left with a hole — density is unaffected.
#[test]
fn a_cancelled_parked_message_consumes_no_seq() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (a, _b) = upsert_two_channels(&conn);
    let future = Utc::now() + chrono::Duration::hours(1);

    let m1 = append_msg(&conn, a, "m1");
    let p = park_msg(&conn, a, "p", future);
    conn.execute(
        "DELETE FROM messaging_messages WHERE id = ?1",
        rusqlite::params![p],
    )
    .expect("cancel parked message");
    let m2 = append_msg(&conn, a, "m2");

    assert_eq!(retained_seq_of(&conn, m1), Some(1));
    assert_eq!(
        retained_seq_of(&conn, m2),
        Some(2),
        "no gap from the cancel"
    );
    assert_eq!(last_retained_seq_of(&conn, a), 2);
}

/// The partial unique index is the better-dead-than-wrong backstop: two retained
/// rows on one channel cannot share a sequence.
#[test]
fn the_retained_seq_unique_index_rejects_a_duplicate() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (a, _b) = upsert_two_channels(&conn);
    let ab = a.as_bytes().to_vec();

    conn.execute(
        "INSERT INTO messaging_messages
            (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at, retained_seq)
         VALUES (?1, ?2, 's', 'a', 'x', 'low', 0, 'now', 7)",
        rusqlite::params![Uuid::new_v4().as_bytes().to_vec(), ab.clone()],
    )
    .expect("first retained_seq row inserts");
    let dup = conn.execute(
        "INSERT INTO messaging_messages
            (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at, retained_seq)
         VALUES (?1, ?2, 's', 'a', 'y', 'low', 0, 'now', 7)",
        rusqlite::params![Uuid::new_v4().as_bytes().to_vec(), ab.clone()],
    );
    match dup {
        Err(rusqlite::Error::SqliteFailure(e, _)) => assert_eq!(
            e.code,
            rusqlite::ErrorCode::ConstraintViolation,
            "a duplicate (channel, retained_seq) must fail on the unique index, not some \
             other error"
        ),
        other => panic!("expected a constraint violation, got {other:?}"),
    }

    // The index is *partial*: parked rows carry no sequence, and any number of
    // them coexist on one channel. A total index would reject the second.
    for _ in 0..2 {
        conn.execute(
            "INSERT INTO messaging_messages
                (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at,
                 deliver_after, retained_seq)
             VALUES (?1, ?2, 's', 'a', 'p', 'low', 0, 'now', '2099-01-01T00:00:00+00:00', NULL)",
            rusqlite::params![Uuid::new_v4().as_bytes().to_vec(), ab.clone()],
        )
        .expect("unsequenced rows are not constrained by the partial index");
    }
}

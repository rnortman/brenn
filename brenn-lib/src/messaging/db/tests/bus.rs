use super::helpers::*;
use crate::db::init_db_memory;
use crate::messaging::canonical_address;
use crate::messaging::config::{Depth, Sink};
use crate::messaging::db::*;
use crate::messaging::{ChannelEntry, ChannelScheme, Urgency};
use crate::test_utils::ensure_user_and_conv;
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use uuid::Uuid;

#[test]
fn insert_message_round_trip() {
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
    let inserted = insert_message(
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
    );
    assert!(inserted.id > 0);
    // A commit is target-blind: it writes the message and nothing per-subscriber.
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM messaging_pending_pushes", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        0,
        "a bus commit writes no pending-push row"
    );
    assert_eq!(
        inserted.retained_seq,
        Some(1),
        "the message takes the channel's first retention position"
    );
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
    insert_message(
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
fn release_due_for_channel_positions_the_message_and_clears_its_hold() {
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
    let inserted = insert_message(
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
    );
    assert!(inserted.id > 0);
    assert_eq!(
        inserted.retained_seq, None,
        "a parked message holds no retention position"
    );
    let now = Utc::now();
    let released = crate::messaging::db::release_due_for_channel(&conn, channel_uuid, now);
    assert_eq!(released.len(), 1);
    assert_eq!(
        released[0].retained_seq, 1,
        "release is what gives the message its position"
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
        crate::messaging::db::release_due_for_channel(&conn, channel_uuid, now).is_empty(),
        "a second release pass must not re-release what the first released"
    );
}

#[test]
fn lookup_message_for_authorship_reports_whether_the_message_is_still_parked() {
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

    let future = Utc::now() + chrono::Duration::seconds(60);
    let (parked_id, parked_uuid) = insert_msg(&conn, channel_uuid, "alice", "later", Some(future));
    let (live_id, live_uuid) = insert_msg(&conn, channel_uuid, "alice", "now", None);

    let parked = lookup_message_for_authorship(&conn, parked_uuid).unwrap();
    assert_eq!(parked.message_id, parked_id);
    assert_eq!(parked.sender, "alice");
    assert!(parked.parked, "a scheduled message is still the sender's");

    let live = lookup_message_for_authorship(&conn, live_uuid).unwrap();
    assert_eq!(live.message_id, live_id);
    assert!(
        !live.parked,
        "a message in retention is past recall — every reader has its own position over it"
    );
}

#[test]
fn lookup_message_for_authorship_returns_none_for_unknown_uuid() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let result = lookup_message_for_authorship(&conn, Uuid::new_v4());
    assert!(result.is_none());
}

#[test]
fn update_parked_message_propagates_body_to_fts() {
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
        "old unique word",
        Some(future),
    );

    let fields = EditFieldsApplied {
        body: Some("new unique phrase"),
        reply_to_uuid: None,
        deliver_after: None,
        delivery_deadline: None,
        urgency: None,
    };
    update_parked_message(&conn, msg_id, "sender", &fields);

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
fn update_parked_message_reschedule_only_does_not_touch_fts() {
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
    let (msg_id, _) = insert_msg(&conn, channel_uuid, "sender", "foxword body", Some(future));

    // Reschedule only — body unchanged.
    let new_future = Utc::now() + chrono::Duration::seconds(120);
    let fields = EditFieldsApplied {
        body: None,
        reply_to_uuid: None,
        deliver_after: Some(Some(new_future)),
        delivery_deadline: None,
        urgency: None,
    };
    update_parked_message(&conn, msg_id, "sender", &fields);

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

    let future = Utc::now() + chrono::Duration::seconds(60);
    // alice schedules on chan-a.
    insert_msg(&conn, ch_uuid_a, "alice", "alice-chan-a", Some(future));
    // bob schedules on chan-a.
    insert_msg(&conn, ch_uuid_a, "bob", "bob-chan-a", Some(future));
    // alice schedules on chan-b.
    insert_msg(&conn, ch_uuid_b, "alice", "alice-chan-b", Some(future));
    // alice publishes on chan-a immediately — in retention, so not pending.
    insert_msg(&conn, ch_uuid_a, "alice", "alice-live", None);

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

    // Scheduled in the past, but no release pass has taken it yet — still parked.
    let past = Utc::now() - chrono::Duration::seconds(10);
    insert_msg(&conn, channel_uuid, "sender", "past-due", Some(past));

    let result = list_pending_messages_for_sender(&conn, "sender", None);
    assert_eq!(
        result.len(),
        1,
        "past-due undrained message must appear in pending list"
    );
}

/// A message that has entered retention is past recall, so it leaves the pending
/// list at the release that positions it — not at any per-subscriber delivery,
/// of which the substrate keeps no record.
#[test]
fn list_pending_excludes_a_released_message() {
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

    let past = Utc::now() - chrono::Duration::seconds(10);
    insert_msg(&conn, channel_uuid, "sender", "released-msg", Some(past));
    release_due_for_channel(&conn, channel_uuid, Utc::now());

    let result = list_pending_messages_for_sender(&conn, "sender", None);
    assert!(
        result.is_empty(),
        "a released message must not appear in the pending list"
    );
}

// -----------------------------------------------------------------------
// Cancel / edit / list-pending
// -----------------------------------------------------------------------

/// `mark_pending_pushes_delivered` is idempotent: calling it twice does not
/// error and does not change the `delivered_at` timestamp.
#[test]
fn mark_pending_pushes_delivered_is_idempotent() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);

    let (_, push_id) = insert_ingress_message(
        &conn,
        &ParticipantId::for_conversation(1),
        "app",
        "mqtt:test",
        "summary",
        "{}",
        Urgency::Normal,
        utc_to_ns(Utc::now()),
    );

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
    insert_msg(&conn, channel_uuid, "sender", "body", Some(release_at));

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
    insert_msg(&conn, b, "sender", "body", Some(release_at));
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
    insert_msg(&conn, channel_uuid, "sender", "body", Some(release_at));
    assert_eq!(
        crate::messaging::db::release_due_for_channel(&conn, channel_uuid, Utc::now()).len(),
        1
    );

    assert!(
        crate::messaging::db::earliest_channel_release(&conn, channel_uuid).is_none(),
        "a released message must not keep reporting a deadline"
    );
}

/// Two `brenn:` channels on one DB, for cases about per-channel scoping.
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

// -----------------------------------------------------------------------
// test-1: sender-recheck-in-mutation — mismatched caller_sender tests
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// test-2: wake-recompute-in-tx — per-push CASE expression edge cases
// -----------------------------------------------------------------------

#[test]
#[should_panic(expected = "edit sender mismatch")]
fn update_parked_message_panics_on_sender_mismatch() {
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
    let (msg_id, _) = insert_msg(&conn, channel_uuid, "alice", "body", None);
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
    update_parked_message(&conn, msg_id, "mallory", &fields);
}

#[test]
#[should_panic(expected = "edit row missing")]
fn update_parked_message_panics_on_missing_row() {
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
    update_parked_message(&conn, 99999, "alice", &fields);
}

// -----------------------------------------------------------------------
// Bus GC tests — eviction + sink, two-reaper non-overlap
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

/// Helper: count message rows for a channel.
fn count_bus_messages(conn: &Connection, ch_uuid_bytes: &[u8]) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_messages WHERE channel_uuid = ?1 AND envelope_type='brenn'",
        rusqlite::params![ch_uuid_bytes],
        |r| r.get(0),
    )
    .expect("count_bus_messages")
}

/// Bounded-frontier drop channel: after N > frontier publishes + one GC pass,
/// retained body count <= frontier.
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

    // Insert 10 messages.
    for ts in 1000..1010i64 {
        insert_bus_msg(&conn, &ch_uuid_bytes, ts);
    }

    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 10);

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
    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 3);

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
        insert_bus_msg(&conn, &ch_uuid_bytes, ts);
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
    assert_eq!(count_bus_messages(&conn, &ch_uuid_bytes), 3);
}

/// Two-reaper non-overlap: bus GC must not touch kind='ingress' rows.
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

    // Insert 5 bus messages. A bus message carries no push row: the only rows
    // this table still holds are the channel-less ingress ones below.
    for ts in 1..6i64 {
        insert_bus_msg(&conn, &ch_uuid_bytes, ts * 1000);
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
    let released = crate::messaging::db::release_due_for_channel(&conn, ch_uuid, now);
    assert_eq!(released.len(), 1, "the due parked message releases");
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
    let released = crate::messaging::db::release_due_for_channel(&conn, ch_uuid, now);
    assert_eq!(released.len(), 1);
    assert_eq!(retained_seq_of(&conn, parked), Some(3));
    assert_eq!(last_retained_seq_of(&conn, ch_uuid), 3);
}

// ---------------------------------------------------------------------------
// load_dispatchable_ingress_pushes tests
// ---------------------------------------------------------------------------

/// The dispatcher's scan serves the channel-less ingress rows and nothing else:
/// delivery on a channel is a cursor position, so a bus message must not appear.
#[test]
fn the_ingress_scan_serves_ingress_rows_and_not_bus_messages() {
    let db = crate::db::init_db_memory();
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
    insert_msg(&conn, channel_uuid, "sender", "on a channel", None);

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

    let rows = load_dispatchable_ingress_pushes(&conn);
    assert_eq!(rows.len(), 1, "only the ingress row is dispatchable");
    assert_eq!(rows[0].event.source, "mqtt:test");
    assert_eq!(rows[0].event.summary, "test event summary");
    assert!(rows[0].eager_wake);
}

/// A non-eager ingress row is the startup/reconnect drain's, not the
/// dispatcher's: loading it on every poll would be pure waste.
#[test]
fn the_ingress_scan_excludes_non_eager_and_delivered_rows() {
    let db = crate::db::init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let subscriber = ParticipantId::for_conversation(1);
    let publish_ts_ns = utc_to_ns(Utc::now());

    // Urgency below the ingress path's Normal threshold ⇒ eager_wake = 0.
    insert_ingress_message(
        &conn,
        &subscriber,
        "app",
        "mqtt:quiet",
        "quiet",
        "{}",
        Urgency::Low,
        publish_ts_ns,
    );
    let (_, delivered_push) = insert_ingress_message(
        &conn,
        &subscriber,
        "app",
        "mqtt:loud",
        "loud",
        "{}",
        Urgency::Normal,
        publish_ts_ns + 1,
    );
    assert_eq!(load_dispatchable_ingress_pushes(&conn).len(), 1);

    mark_pending_pushes_delivered(&conn, &[delivered_push]);
    assert!(
        load_dispatchable_ingress_pushes(&conn).is_empty(),
        "a delivered row leaves the scan"
    );
}

/// Tripwire: the dispatcher scan must be served by the partial index
/// `idx_messaging_pending_pushes_ingress_dispatch`, not a full table scan. If a
/// future SQLite upgrade or a query edit stops the planner qualifying the partial
/// index, this fails loudly instead of silently regressing to O(total backlog)
/// per wake.
///
/// Uses `LOAD_DISPATCHABLE_INGRESS_SQL` directly (the same constant the production
/// function prepares) so the asserted plan can never drift from the real query. No
/// `ANALYZE` is run — production never runs it, so the test must see the same
/// planner conditions.
#[test]
fn the_ingress_scan_uses_its_partial_index() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    let subscriber = ParticipantId::for_conversation(1);

    // Seed a non-degenerate mixed population so the cost-based planner sees a
    // realistic table rather than a trivial one it might full-scan regardless.
    for i in 0..20i64 {
        insert_ingress_message(
            &conn,
            &subscriber,
            "app",
            "mqtt:quiet",
            "quiet",
            "{}",
            Urgency::Low,
            1000 + i,
        );
    }
    for i in 0..5i64 {
        insert_ingress_message(
            &conn,
            &subscriber,
            "app",
            "mqtt:loud",
            "loud",
            "{}",
            Urgency::Normal,
            3000 + i,
        );
    }

    let plan: Vec<String> = {
        let mut stmt = conn
            .prepare(&("EXPLAIN QUERY PLAN ".to_owned() + LOAD_DISPATCHABLE_INGRESS_SQL))
            .expect("prepare EXPLAIN QUERY PLAN");
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(3))
            .expect("query plan");
        rows.map(|r| r.expect("read plan row")).collect()
    };

    // The index name is globally unique, so it can only appear while the planner
    // accesses messaging_pending_pushes (aliased `pp`) via that index.
    assert!(
        plan.iter()
            .any(|d| d.contains("idx_messaging_pending_pushes_ingress_dispatch")),
        "ingress scan must use idx_messaging_pending_pushes_ingress_dispatch; plan was:\n{}",
        plan.join("\n"),
    );
    // Belt-and-suspenders: no unindexed full scan of pp. A `SCAN pp` row that
    // names any index (`USING INDEX` or `USING COVERING INDEX`) is index-backed;
    // only a bare `SCAN pp` with no index is a full table scan.
    assert!(
        !plan.iter().any(|d| d.trim() == "SCAN pp"),
        "ingress scan must not full-scan messaging_pending_pushes; plan was:\n{}",
        plan.join("\n"),
    );
}

// ---------------------------------------------------------------------------
// Surface durable-projection helpers (SD5 claims, SD6 channel-scoped loaders)
// ---------------------------------------------------------------------------

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
    insert_message(
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
    )
    .id
}

fn park_msg(conn: &Connection, channel: Uuid, body: &str, release_at: DateTime<Utc>) -> i64 {
    insert_message(
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

    let released = release_due_for_channel(&conn, a, Utc::now());
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].message_id, p);

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
    let released_a = crate::messaging::db::release_due_for_channel(&conn, a, now);
    let released_b = crate::messaging::db::release_due_for_channel(&conn, b, now);
    assert_eq!(released_a.len() + released_b.len(), 3, "all three come due");

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
    insert_message(
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

#[test]
fn message_retained_seq_reads_the_named_row_not_the_channel_high_water() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (a, _b) = upsert_two_channels(&conn);

    let first = insert_message(
        &conn,
        a,
        "src",
        "sender",
        "first",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        utc_to_ns(Utc::now()),
    );
    assert_eq!(
        message_retained_seq(&conn, first.uuid),
        first
            .retained_seq
            .expect("an unparked append is positioned"),
        "the read agrees with the position the insert reported"
    );

    append_msg(&conn, a, "second");
    assert_eq!(
        message_retained_seq(&conn, first.uuid),
        1,
        "a later append does not move an earlier row's position"
    );
    assert!(
        message_retained_seq(&conn, first.uuid) < last_retained_seq_of(&conn, a),
        "the high-water has moved past the first row, so the two are distinguishable"
    );
}

/// An unknown uuid is a broken caller invariant, not a `None` to handle.
#[test]
#[should_panic(expected = "has no row")]
fn message_retained_seq_panics_on_an_unknown_uuid() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (_a, _b) = upsert_two_channels(&conn);
    message_retained_seq(&conn, Uuid::new_v4());
}

/// A parked row holds no position; reporting one would be the silent-wrong
/// answer, so the read dies instead.
#[test]
#[should_panic(expected = "holds no retention position")]
fn message_retained_seq_panics_on_a_parked_row() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let (a, _b) = upsert_two_channels(&conn);
    let parked = insert_message(
        &conn,
        a,
        "src",
        "sender",
        "parked",
        Urgency::Low,
        ChannelScheme::Brenn,
        None,
        None,
        Some(Utc::now() + chrono::Duration::hours(1)),
        utc_to_ns(Utc::now()),
    );
    assert_eq!(
        parked.retained_seq, None,
        "the fixture row is really parked"
    );
    message_retained_seq(&conn, parked.uuid);
}

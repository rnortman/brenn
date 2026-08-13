use crate::db::init_db_memory;
use crate::db::*;
use crate::testutils::ensure_user_and_conv;

// cleanup scoping test
// -----------------------------------------------------------------------

/// A delivered ingress push older than cutoff is deleted; a delivered bus push
/// of equal age is NOT deleted (kind='brenn' fence); the orphaned ingress message
/// row is also removed; an equal-age bus message row is explicitly still present.
#[test]
fn cleanup_scoping_ingress_only() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);

    let now = chrono::Utc::now();
    let cutoff = now; // delivered_at is far in the past, so < cutoff

    // Seed a channel for bus messages.
    let ch_uuid = uuid::Uuid::new_v4();
    let ch_uuid_bytes = ch_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch) VALUES (?1, 'brenn:ch', '2024-01-01', X'00000000000000000000000000000001')",
        rusqlite::params![ch_uuid_bytes],
    )
    .unwrap();

    let past = "2020-01-01T00:00:00+00:00";

    // Ingress message + delivered push.
    let ing_uuid = uuid::Uuid::new_v4();
    let ing_uuid_bytes = ing_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_messages
           (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at,
            envelope_type, ingress_source, ingress_summary)
         VALUES (?1, NULL, '', '', '{}', 'normal', 1000, '2020-01-01', 'ingress', 'mqtt:b:t', 'sum')",
        rusqlite::params![ing_uuid_bytes],
    )
    .unwrap();
    let ing_msg_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO messaging_pending_pushes
           (message_id, target_subscriber, target_app_slug, eager_wake, delivered_at, created_at)
         VALUES (?1, 'conversation:1', 'myapp', 1, ?2, '2020-01-01')",
        rusqlite::params![ing_msg_id, past],
    )
    .unwrap();

    // Bus message + delivered push.
    let bus_uuid = uuid::Uuid::new_v4();
    let bus_uuid_bytes = bus_uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO messaging_messages
           (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at)
         VALUES (?1, ?2, 'brenn:ch', 'sender', 'hello', 'normal', 2000, '2020-01-01')",
        rusqlite::params![bus_uuid_bytes, ch_uuid_bytes],
    )
    .unwrap();
    let bus_msg_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO messaging_pending_pushes
           (message_id, target_subscriber, target_app_slug, eager_wake, delivered_at, created_at)
         VALUES (?1, 'conversation:1', 'myapp', 1, ?2, '2020-01-01')",
        rusqlite::params![bus_msg_id, past],
    )
    .unwrap();

    let (pushes_deleted, messages_deleted) = delete_delivered_ingress_pushes_before(&conn, cutoff);

    assert_eq!(
        pushes_deleted, 1,
        "exactly one ingress push must be deleted"
    );
    assert_eq!(
        messages_deleted, 1,
        "orphaned ingress message must be reaped"
    );

    // Bus push row must still exist.
    let bus_push_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes WHERE message_id = ?1",
            rusqlite::params![bus_msg_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        bus_push_count, 1,
        "bus push must not be deleted (brenn fence)"
    );

    // Bus message row must still exist.
    let bus_msg_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE id = ?1",
            rusqlite::params![bus_msg_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        bus_msg_count, 1,
        "bus message row must not be deleted (design-4 fence)"
    );
}

// -----------------------------------------------------------------------
// stale-undelivered repo_sync test
// -----------------------------------------------------------------------

/// An abandoned repo_sync:* ingress conversation (owning conversation updated_at
/// older than staleness_days) with an undelivered ingress push: janitor marks the
/// push delivered, and the next cleanup pass reaps it.
#[test]
fn stale_repo_sync_ingress_push_marked_and_reaped() {
    use crate::ingress::REPO_SYNC_SOURCE_PREFIX;

    let db = init_db_memory();
    let conn = db.blocking_lock();
    conn.execute_batch(
        "INSERT INTO users (id, username, password_hash, created_at) VALUES (1, 'u', 'h', '2024-01-01T00:00:00+00:00');
         INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at)
         VALUES (1, 1, 'active', 'app', '2020-01-01T00:00:00+00:00', '2020-01-01T00:00:00+00:00');",
    )
    .unwrap();

    // Insert an undelivered repo_sync:* ingress push.
    let ing_uuid = uuid::Uuid::new_v4();
    let ing_uuid_bytes = ing_uuid.as_bytes().to_vec();
    let source = format!("{REPO_SYNC_SOURCE_PREFIX}myrepo");
    conn.execute(
        "INSERT INTO messaging_messages
           (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at,
            envelope_type, ingress_source, ingress_summary)
         VALUES (?1, NULL, '', '', '{}', 'normal', 1000, '2020-01-01', 'ingress', ?2, 'sum')",
        rusqlite::params![ing_uuid_bytes, &source],
    )
    .unwrap();
    let ing_msg_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO messaging_pending_pushes
           (message_id, target_subscriber, target_app_slug, eager_wake, created_at)
         VALUES (?1, 'conversation:1', 'app', 1, '2020-01-01')",
        rusqlite::params![ing_msg_id],
    )
    .unwrap();

    // staleness_days=1 — the conversation was last updated in 2020 so it's stale.
    let marked = mark_stale_undelivered_ingress_repo_sync(&conn, 1);
    assert_eq!(marked, 1, "janitor must mark one push delivered");

    // The push must now have a delivered_at set.
    let delivered_at: Option<String> = conn
        .query_row(
            "SELECT delivered_at FROM messaging_pending_pushes WHERE message_id = ?1",
            rusqlite::params![ing_msg_id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        delivered_at.is_some(),
        "push must be marked delivered after janitor run"
    );

    // Next cleanup pass reaps it.
    let cutoff = chrono::Utc::now() + chrono::Duration::days(1);
    let (pushes_deleted, messages_deleted) = delete_delivered_ingress_pushes_before(&conn, cutoff);
    assert_eq!(
        pushes_deleted, 1,
        "cleanup must reap the now-delivered push"
    );
    assert_eq!(
        messages_deleted, 1,
        "cleanup must reap the orphaned message"
    );
}

/// Enqueue N ingress rows for one conversation in known order; assert drain
/// order matches publish_ts_ns, id order (i.e. enqueue order).
/// Also tests same-millisecond tie broken by rowid (message id).
#[test]
fn sort_key_parity_ingress_drain_order() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);

    let base_ns = utc_to_ns(chrono::Utc::now());

    // Insert three rows: two at base_ns (tie on publish_ts_ns, broken by m.id),
    // one at base_ns + 1_000_000 (one millisecond later).
    let summaries = ["tie-first", "tie-second", "later"];
    let ns_values = [base_ns, base_ns, base_ns + 1_000_000];

    for (summary, &ns) in summaries.iter().zip(ns_values.iter()) {
        let msg_uuid = uuid::Uuid::new_v4();
        let msg_uuid_bytes = msg_uuid.as_bytes().to_vec();
        conn.execute(
            "INSERT INTO messaging_messages
               (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns,
                created_at, envelope_type, ingress_source, ingress_summary)
             VALUES (?1, NULL, '', '', '{}', 'normal', ?2,
                     '2024-01-01', 'ingress', 'mqtt:b:t', ?3)",
            rusqlite::params![msg_uuid_bytes, ns, summary],
        )
        .unwrap();
        let msg_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO messaging_pending_pushes
               (message_id, target_subscriber, target_app_slug, eager_wake, created_at)
             VALUES (?1, 'conversation:1', 'app', 1, '2024-01-01')",
            rusqlite::params![msg_id],
        )
        .unwrap();
    }

    let subscriber = ParticipantId::for_conversation(1);
    let drain = load_pending_ingress_for_drain(&conn, &subscriber);
    assert_eq!(drain.len(), 3);

    let got_summaries: Vec<&str> = drain.iter().map(|(_, ev)| ev.summary.as_str()).collect();
    assert_eq!(
        got_summaries,
        ["tie-first", "tie-second", "later"],
        "drain must respect publish_ts_ns ASC, m.id ASC order"
    );
}

/// Tripwire: the drain read must be served by
/// `idx_messaging_pending_pushes_ingress_undelivered`, not a full table scan. A
/// drain that scans costs O(total ingress backlog) per conversation wake, and the
/// regression is silent — the query keeps returning the right rows.
///
/// Uses `LOAD_PENDING_INGRESS_FOR_DRAIN_SQL` directly (the constant the production
/// function prepares) so the asserted plan cannot drift from the real query. No
/// `ANALYZE` is run, because production never runs it.
#[test]
fn the_drain_read_uses_its_partial_index() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    ensure_user_and_conv(&conn, 1);
    ensure_user_and_conv(&conn, 2);

    // A mixed population across two subscribers, so the cost-based planner sees a
    // table worth indexing rather than a trivial one it may scan regardless.
    for i in 0..20i64 {
        insert_ingress_message(
            &conn,
            &ParticipantId::for_conversation(1),
            "app",
            "mqtt:b:t",
            "one",
            "{}",
            brenn_lib::messaging::Urgency::Normal,
            1000 + i,
        );
        insert_ingress_message(
            &conn,
            &ParticipantId::for_conversation(2),
            "app",
            "mqtt:b:t",
            "two",
            "{}",
            brenn_lib::messaging::Urgency::Normal,
            2000 + i,
        );
    }

    let plan: Vec<String> = {
        let mut stmt = conn
            .prepare(&("EXPLAIN QUERY PLAN ".to_owned() + LOAD_PENDING_INGRESS_FOR_DRAIN_SQL))
            .expect("prepare EXPLAIN QUERY PLAN");
        let rows = stmt
            .query_map(["conversation:1"], |row| row.get::<_, String>(3))
            .expect("query plan");
        rows.map(|r| r.expect("read plan row")).collect()
    };

    assert!(
        plan.iter()
            .any(|d| d.contains("idx_messaging_pending_pushes_ingress_undelivered")),
        "drain read must use idx_messaging_pending_pushes_ingress_undelivered; plan was:\n{}",
        plan.join("\n"),
    );
    // A bare `SCAN pp` naming no index is the full table scan this guards against.
    assert!(
        !plan.iter().any(|d| d.trim() == "SCAN pp"),
        "drain read must not full-scan messaging_pending_pushes; plan was:\n{}",
        plan.join("\n"),
    );
}

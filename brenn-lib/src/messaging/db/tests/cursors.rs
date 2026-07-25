//! Subscriber cursor rows: the accessors, and the one-shot seed that carries
//! standing delivery claims over onto positions.

use rusqlite::Connection;
use uuid::Uuid;

use crate::db::init_db_memory;
use crate::messaging::ParticipantId;
use crate::messaging::config::Depth;
use crate::messaging::db::{
    delete_subscriber_cursor, ensure_subscriber_cursor, load_subscriber_cursor,
    run_messaging_migrations,
};

/// A channel row with `count` retained messages, sequenced 1..=count.
fn seed_channel(conn: &Connection, address: &str, count: i64) -> Uuid {
    let channel = Uuid::new_v4();
    conn.execute(
        "INSERT INTO messaging_channels (uuid, address, created_at, resume_epoch,
                                         last_retained_seq)
         VALUES (?1, ?2, 'now', ?3, ?4)",
        rusqlite::params![
            channel.as_bytes().to_vec(),
            address,
            Uuid::new_v4().as_bytes().to_vec(),
            count,
        ],
    )
    .expect("insert channel");
    for seq in 1..=count {
        conn.execute(
            "INSERT INTO messaging_messages
                 (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at,
                  retained_seq)
             VALUES (?1, ?2, 'src', 'sender', ?3, 'normal', 0, 'now', ?4)",
            rusqlite::params![
                Uuid::new_v4().as_bytes().to_vec(),
                channel.as_bytes().to_vec(),
                format!("m{seq}"),
                seq,
            ],
        )
        .expect("insert message");
    }
    channel
}

/// The message row holding `seq` on `channel`.
fn message_id(conn: &Connection, channel: Uuid, seq: i64) -> i64 {
    conn.query_row(
        "SELECT id FROM messaging_messages WHERE channel_uuid = ?1 AND retained_seq = ?2",
        rusqlite::params![channel.as_bytes().to_vec(), seq],
        |row| row.get(0),
    )
    .expect("look up message id")
}

/// A delivery claim on the message at `seq`, owed or already delivered.
fn claim(conn: &Connection, channel: Uuid, seq: i64, subscriber: &str, slug: &str, owed: bool) {
    conn.execute(
        "INSERT INTO messaging_pending_pushes
             (message_id, target_subscriber, target_app_slug, eager_wake, delivered_at, created_at)
         VALUES (?1, ?2, ?3, 1, ?4, 'now')",
        rusqlite::params![
            message_id(conn, channel, seq),
            subscriber,
            slug,
            if owed { None } else { Some("now") },
        ],
    )
    .expect("insert claim");
}

/// A registration row, which is where the seed reads a depth from.
fn register(conn: &Connection, channel: Uuid, slug: &str, push_depth: &str) {
    conn.execute(
        "INSERT INTO messaging_subscriptions
             (channel_uuid, app_slug, push_depth, retain_depth, noise, wake_min)
         VALUES (?1, ?2, ?3, '4', 'metered', 'normal')",
        rusqlite::params![channel.as_bytes().to_vec(), slug, push_depth],
    )
    .expect("insert registration");
}

/// Re-run the migrations as they land on a store that predates the cursor
/// table: drop the table the fresh DB already carries, then migrate.
fn migrate_from_before_cursors(conn: &Connection) {
    conn.execute_batch("DROP TABLE messaging_subscriber_cursors;")
        .expect("drop cursor table");
    run_messaging_migrations(conn);
}

fn cursor(conn: &Connection, channel: Uuid, subscriber: &str) -> Option<(String, Depth, i64)> {
    load_subscriber_cursor(
        conn,
        channel,
        &ParticipantId::from_stored(subscriber.to_string()),
    )
    .map(|row| (row.app_slug, row.push_depth, row.next_owed_seq))
}

// ── The accessors ───────────────────────────────────────────────────────────

#[test]
fn a_second_ensure_retunes_the_caches_and_keeps_the_position() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:accessors", 3);
    let sub = ParticipantId::for_wasm("proc");

    assert!(
        ensure_subscriber_cursor(&conn, channel, &sub, "proc", Depth::Bounded(4), 2),
        "the first ensure creates the row"
    );
    assert!(
        !ensure_subscriber_cursor(&conn, channel, &sub, "proc", Depth::Unbounded, 99),
        "the second ensure finds it"
    );

    let row = load_subscriber_cursor(&conn, channel, &sub).expect("cursor exists");
    assert_eq!(row.next_owed_seq, 2, "the position is the subscriber's own");
    assert_eq!(row.push_depth, Depth::Unbounded, "the depth cache retunes");
}

#[test]
fn delete_reports_whether_a_position_was_there() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:delete", 1);
    let sub = ParticipantId::for_wasm("proc");

    ensure_subscriber_cursor(&conn, channel, &sub, "proc", Depth::Bounded(4), 1);
    assert!(delete_subscriber_cursor(&conn, channel, &sub));
    assert!(!delete_subscriber_cursor(&conn, channel, &sub));
    assert!(load_subscriber_cursor(&conn, channel, &sub).is_none());
}

#[test]
#[should_panic(expected = "a sampled subscriber holds no position")]
fn a_sampled_subscriber_cannot_be_given_a_cursor() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:sampled", 1);
    ensure_subscriber_cursor(
        &conn,
        channel,
        &ParticipantId::for_wasm("proc"),
        "proc",
        Depth::Bounded(0),
        1,
    );
}

#[test]
fn deleting_the_channel_row_cascades_to_its_cursors() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:cascade", 2);
    let sub = ParticipantId::for_wasm("proc");
    ensure_subscriber_cursor(&conn, channel, &sub, "proc", Depth::Bounded(4), 1);

    conn.execute(
        "DELETE FROM messaging_messages WHERE channel_uuid = ?1",
        rusqlite::params![channel.as_bytes().to_vec()],
    )
    .expect("clear messages");
    conn.execute(
        "DELETE FROM messaging_channels WHERE uuid = ?1",
        rusqlite::params![channel.as_bytes().to_vec()],
    )
    .expect("delete channel");

    assert!(load_subscriber_cursor(&conn, channel, &sub).is_none());
}

// ── The seed ────────────────────────────────────────────────────────────────

/// Every attach-managed subscriber holding standing claims comes out of the
/// migration positioned at the oldest sequence it is still owed — never at head,
/// which would skip the backlog, and never below what it was already delivered.
/// A surface subscriber gets nothing: its position lives on the wire.
#[test]
fn the_seed_positions_owed_subscribers_at_their_oldest_owed_sequence() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:seed", 4);
    register(&conn, channel, "proc", "2");
    register(&conn, channel, "pfin", "unbounded");
    register(&conn, channel, "deskbar#agenda", "3");

    // Delivered through 2, owed 3 and 4.
    claim(&conn, channel, 1, "wasm:proc", "proc", false);
    claim(&conn, channel, 2, "wasm:proc", "proc", false);
    claim(&conn, channel, 3, "wasm:proc", "proc", true);
    claim(&conn, channel, 4, "wasm:proc", "proc", true);
    // Owed everything.
    claim(&conn, channel, 1, "conversation:7", "pfin", true);
    // A system component holds no registration row at all.
    claim(
        &conn,
        channel,
        4,
        "system:tool-executor",
        "tool-executor",
        true,
    );
    // Surfaces are not attach-managed.
    claim(
        &conn,
        channel,
        2,
        "surface:deskbar#agenda",
        "deskbar#agenda",
        true,
    );

    migrate_from_before_cursors(&conn);

    assert_eq!(
        cursor(&conn, channel, "wasm:proc"),
        Some(("proc".to_string(), Depth::Bounded(2), 3)),
        "the component resumes at its oldest owed message, at its registered depth"
    );
    assert_eq!(
        cursor(&conn, channel, "conversation:7"),
        Some(("pfin".to_string(), Depth::Unbounded, 1)),
        "the conversation carries the app slug its claims name"
    );
    assert_eq!(
        cursor(&conn, channel, "system:tool-executor"),
        Some(("tool-executor".to_string(), Depth::Unbounded, 4)),
        "an unregistered kind is recorded unbounded and retuned at its first read"
    );
    assert_eq!(
        cursor(&conn, channel, "surface:deskbar#agenda"),
        None,
        "a surface holds no server-side position"
    );
}

/// Claims left behind for a subscriber that has since been retuned to sampled
/// are residue: it is never delivered to, so it gets no position and no
/// eviction pass will ever report against it.
#[test]
fn the_seed_skips_a_sampled_subscribers_residue() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:sampled-residue", 2);
    register(&conn, channel, "proc", "0");
    claim(&conn, channel, 1, "wasm:proc", "proc", true);

    migrate_from_before_cursors(&conn);

    assert_eq!(cursor(&conn, channel, "wasm:proc"), None);
}

/// A claim still parked behind its release hold is owed to nobody yet, so it
/// does not position anyone; neither does a channel-less ingress row, which the
/// bus does not carry at all.
#[test]
fn the_seed_reads_neither_parked_claims_nor_ingress_rows() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:parked", 1);
    register(&conn, channel, "proc", "4");
    conn.execute(
        "INSERT INTO messaging_pending_pushes
             (message_id, target_subscriber, target_app_slug, eager_wake, release_after, created_at)
         VALUES (?1, 'wasm:proc', 'proc', 1, 'later', 'now')",
        rusqlite::params![message_id(&conn, channel, 1)],
    )
    .expect("insert parked claim");
    conn.execute(
        "INSERT INTO messaging_messages
             (uuid, source, sender, body, urgency, publish_ts_ns, created_at, envelope_type)
         VALUES (?1, 'src', 'sender', 'ingress', 'normal', 0, 'now', 'ingress');
        ",
        rusqlite::params![Uuid::new_v4().as_bytes().to_vec()],
    )
    .expect("insert ingress message");
    let ingress_id: i64 = conn
        .query_row(
            "SELECT id FROM messaging_messages WHERE channel_uuid IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("look up ingress message");
    conn.execute(
        "INSERT INTO messaging_pending_pushes
             (message_id, target_subscriber, target_app_slug, eager_wake, created_at)
         VALUES (?1, 'conversation:3', 'repo-sync', 1, 'now')",
        rusqlite::params![ingress_id],
    )
    .expect("insert ingress claim");

    migrate_from_before_cursors(&conn);

    assert_eq!(cursor(&conn, channel, "wasm:proc"), None);
    assert_eq!(cursor(&conn, channel, "conversation:3"), None);
    let ingress_claims: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes WHERE message_id = ?1",
            rusqlite::params![ingress_id],
            |row| row.get(0),
        )
        .expect("count ingress claims");
    assert_eq!(ingress_claims, 1, "ingress rows are left where they are");
}

/// A subscriber that owes nothing is the steady state of a healthy component,
/// and its delivered claims are just as much a record of where it stands: it
/// comes out positioned one past the newest message it took. Re-priming it
/// instead would re-deliver the tail it had already consumed.
#[test]
fn the_seed_positions_a_caught_up_subscriber_past_its_last_delivery() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:caught-up", 3);
    register(&conn, channel, "proc", "2");
    claim(&conn, channel, 1, "wasm:proc", "proc", false);
    claim(&conn, channel, 2, "wasm:proc", "proc", false);

    migrate_from_before_cursors(&conn);

    assert_eq!(
        cursor(&conn, channel, "wasm:proc"),
        Some(("proc".to_string(), Depth::Bounded(2), 3)),
        "owed nothing through 2, so 3 is the first unseen sequence"
    );
}

/// Claims for one subscriber under two app slugs leave the seed no answer to
/// which application a lagging position is reported under, and the slug it
/// caches is what eviction resolves a noise rung from. Abort rather than cache
/// whichever one sorts first.
#[test]
#[should_panic(expected = "app slugs")]
fn the_seed_aborts_when_a_subscribers_claims_name_two_apps() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:two-slugs", 2);
    register(&conn, channel, "proc", "4");
    claim(&conn, channel, 1, "wasm:proc", "proc", true);
    claim(&conn, channel, 2, "wasm:proc", "other", true);

    migrate_from_before_cursors(&conn);
}

/// An owed claim on a message holding no retention position cannot be seeded
/// below: the cursor would start above a message that is still owed, and the
/// subscriber would never see it and never be told it lost it.
#[test]
#[should_panic(expected = "hold no retention position")]
fn the_seed_aborts_on_an_owed_claim_with_no_retention_position() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:positionless", 1);
    register(&conn, channel, "proc", "4");
    // Still parked, so the boot's retention backfill leaves it unsequenced —
    // while the claim below is unparked and owed, which is the disagreement the
    // seed refuses to guess at.
    conn.execute(
        "INSERT INTO messaging_messages
             (uuid, channel_uuid, source, sender, body, urgency, publish_ts_ns, created_at,
              deliver_after)
         VALUES (?1, ?2, 'src', 'sender', 'unsequenced', 'normal', 0, 'now', 'later')",
        rusqlite::params![
            Uuid::new_v4().as_bytes().to_vec(),
            channel.as_bytes().to_vec(),
        ],
    )
    .expect("insert unsequenced message");
    let unsequenced: i64 = conn
        .query_row(
            "SELECT id FROM messaging_messages
             WHERE channel_uuid = ?1 AND retained_seq IS NULL",
            rusqlite::params![channel.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .expect("look up unsequenced message");
    conn.execute(
        "INSERT INTO messaging_pending_pushes
             (message_id, target_subscriber, target_app_slug, eager_wake, created_at)
         VALUES (?1, 'wasm:proc', 'proc', 1, 'now')",
        rusqlite::params![unsequenced],
    )
    .expect("insert claim on the unsequenced message");

    migrate_from_before_cursors(&conn);
}

/// An owed set with a delivered message above it is not a suffix, and seeding
/// below that message would hand an at-most-once consumer work it already did.
/// Abort the boot instead.
#[test]
#[should_panic(expected = "are not a suffix")]
fn the_seed_aborts_on_a_delivered_message_above_the_owed_set() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:hole", 3);
    register(&conn, channel, "proc", "4");
    claim(&conn, channel, 1, "wasm:proc", "proc", true);
    claim(&conn, channel, 2, "wasm:proc", "proc", false);

    migrate_from_before_cursors(&conn);
}

/// The table's presence is the seed's run-once guard, so an aborted seed has to
/// take the table with it. A table left standing would read as already seeded on
/// the next boot and the positions the claims still hold would be gone for good
/// — including after the operator fixes exactly what the abort told them to.
#[test]
fn an_aborted_seed_leaves_no_table_for_the_next_boot_to_skip() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:abort", 3);
    register(&conn, channel, "proc", "4");
    claim(&conn, channel, 1, "wasm:proc", "proc", true);
    claim(&conn, channel, 2, "wasm:proc", "proc", false);

    let aborted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate_from_before_cursors(&conn);
    }));
    assert!(aborted.is_err(), "the non-suffix claim set aborts the seed");
    let table_present: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master
                           WHERE type = 'table' AND name = 'messaging_subscriber_cursors')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("query sqlite_master")
        != 0;
    assert!(!table_present, "the aborted seed took the table with it");

    // The operator retires the stray delivered claim the abort named, and the
    // next boot seeds from the claims that are still there.
    conn.execute(
        "DELETE FROM messaging_pending_pushes WHERE delivered_at IS NOT NULL",
        [],
    )
    .expect("retire the delivered claim");
    run_messaging_migrations(&conn);

    assert_eq!(
        cursor(&conn, channel, "wasm:proc").map(|c| c.2),
        Some(1),
        "the retried seed positions at the oldest owed sequence"
    );
}

#[test]
fn the_seed_does_not_run_again_on_a_later_boot() {
    let db = init_db_memory();
    let conn = db.blocking_lock();
    let channel = seed_channel(&conn, "brenn:once", 3);
    register(&conn, channel, "proc", "4");
    claim(&conn, channel, 1, "wasm:proc", "proc", true);
    claim(&conn, channel, 2, "wasm:proc", "proc", true);
    claim(&conn, channel, 3, "wasm:proc", "proc", true);

    migrate_from_before_cursors(&conn);
    assert_eq!(
        cursor(&conn, channel, "wasm:proc").map(|c| c.2),
        Some(1),
        "the seed positions at the oldest owed"
    );

    // As a consumer's advance would leave it.
    conn.execute(
        "UPDATE messaging_subscriber_cursors SET next_owed_seq = 4",
        [],
    )
    .expect("advance the cursor");
    run_messaging_migrations(&conn);

    assert_eq!(
        cursor(&conn, channel, "wasm:proc").map(|c| c.2),
        Some(4),
        "a later boot leaves the position where the consumer left it"
    );
}

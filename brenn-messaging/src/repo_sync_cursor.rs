//! Persisted `last_notified_head` cursor for repo_sync advance detection.
//!
//! The cursor is a performance cache over what could in principle be derived
//! from the unified ingress store (if `new_head` were added to the payload).
//! It is kept as a dedicated table so the atomic cursor-plus-fanout shape is
//! cheap: one transaction that updates a single row plus N ingress inserts.
//!
//! Naming: `repo_slug` is the `[[repo]].slug` config value (same as
//! `CloneInfo.slug` in `brenn-server/src/repo_sync/mod.rs`). Not app slug,
//! not mount slug.

use std::collections::HashMap;

use chrono::Utc;
use rusqlite::Connection;

use crate::ParticipantId;
use crate::Urgency;
use crate::db::{insert_ingress_message_raw, utc_to_ns};

/// This crate's own DDL set: the one table its production code writes.
/// Idempotent — `IF NOT EXISTS` throughout. The table has no foreign keys, so
/// it composes in any order after the base tables. Composed into this crate's
/// slice by [`crate::slice::run_slice_migrations`].
pub fn run_repo_sync_cursor_migrations(conn: &Connection) {
    conn.execute_batch(
        r#"
        -- Persisted `last_notified_head` cursor for repo_sync advance detection.
        -- Written in the same transaction as the corresponding event inserts
        -- so a crash mid-fan-out cannot drop the advance notification across
        -- restart. `updated_at` is forensics-only — no read path depends on
        -- it; keep it for post-hoc debugging of when a cursor last advanced.
        CREATE TABLE IF NOT EXISTS repo_sync_cursor (
            repo_slug  TEXT PRIMARY KEY,
            head       TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        "#,
    )
    .expect("run repo_sync_cursor migrations");
}

/// One row to be inserted into the unified ingress store as part of an atomic
/// cursor-plus-fanout transaction. Borrowed strings keep the caller
/// from having to clone per-row when building a batch.
pub struct EnqueueRow<'a> {
    pub conversation_id: i64,
    pub app_slug: &'a str,
    pub source: &'a str,
    pub summary: &'a str,
    pub payload: &'a str,
}

/// Atomically advance the cursor for `repo_slug` to `new_head` and
/// insert every row in `rows`. Either all land or none do. Panics on
/// DB error per Brenn's "BETTER DEAD THAN WRONG" policy.
///
/// `repo_slug` is the `[[repo]].slug` config value (same as
/// `CloneInfo.slug` in `brenn-server/src/repo_sync/mod.rs`). Not app slug,
/// not mount slug.
///
/// Rollback note: this function does not call `tx.rollback()` anywhere.
/// A panic in `tx.execute(...)` unwinds through the
/// `rusqlite::Transaction`'s `Drop` impl, which rolls back
/// automatically. Do NOT "fix" this by adding manual rollback branches
/// — the `Drop`-based rollback is the correct rusqlite idiom and
/// matches Brenn's panic-on-unexpected-state policy.
///
/// This table is a performance cache over what could in principle be
/// derived from the unified ingress store (if `new_head` were added to
/// the payload). Kept separate for now because the atomic
/// cursor-plus-fanout shape is what makes the restart story work, and
/// that shape is cheaper with a dedicated row than with a JSON derivation.
pub fn upsert_and_enqueue(
    conn: &mut Connection,
    repo_slug: &str,
    new_head: &str,
    rows: &[EnqueueRow<'_>],
) {
    let tx = conn.transaction().expect("begin tx");
    let now = brenn_db::format_ts_for_db(Utc::now());
    tx.execute(
        "INSERT INTO repo_sync_cursor (repo_slug, head, updated_at) \
         VALUES (?1, ?2, ?3) \
         ON CONFLICT(repo_slug) DO UPDATE SET head = ?2, updated_at = ?3",
        rusqlite::params![repo_slug, new_head, &now],
    )
    .expect("upsert cursor");
    let ts_ns = utc_to_ns(Utc::now());
    for row in rows {
        let subscriber = ParticipantId::for_conversation(row.conversation_id);
        // `&tx` coerces to `&Connection`; `insert_ingress_message_raw` does
        // not start its own transaction, so it operates within the outer `tx`.
        // TODO(ingress-retirement): publish onto a real bus channel instead of
        // writing channel-less ingress rows.
        insert_ingress_message_raw(
            &tx,
            &subscriber,
            row.app_slug,
            row.source,
            row.summary,
            row.payload,
            Urgency::Normal, // repo_sync notifications are Normal urgency (wake subscribers)
            ts_ns,
        );
    }
    tx.commit().expect("commit tx");
}

/// Atomically insert every row in `rows`. Used by the conflict fan-out
/// path where no cursor advance is implied (a conflict means HEAD did
/// not move to a new value). Either all land or none do. Panics on DB
/// error per Brenn's "BETTER DEAD THAN WRONG" policy.
///
/// Rollback note: same as `upsert_and_enqueue` — the
/// `rusqlite::Transaction`'s `Drop` impl handles rollback on panic.
pub fn enqueue_batch(conn: &mut Connection, rows: &[EnqueueRow<'_>]) {
    let tx = conn.transaction().expect("begin tx");
    let ts_ns = utc_to_ns(Utc::now());
    for row in rows {
        let subscriber = ParticipantId::for_conversation(row.conversation_id);
        // TODO(ingress-retirement): publish onto a real bus channel instead of
        // writing channel-less ingress rows.
        insert_ingress_message_raw(
            &tx,
            &subscriber,
            row.app_slug,
            row.source,
            row.summary,
            row.payload,
            Urgency::Normal, // repo_sync notifications are Normal urgency (wake subscribers)
            ts_ns,
        );
    }
    tx.commit().expect("commit tx");
}

/// Read every persisted cursor. Returned map is keyed by repo slug
/// (`CloneInfo.slug` / `[[repo]].slug`), ready to wrap in
/// `Arc<std::sync::Mutex<_>>` matching today's `last_notified_head`
/// init shape in `brenn-server/src/repo_sync/mod.rs`.
pub fn load_all(conn: &Connection) -> HashMap<String, String> {
    let mut stmt = conn
        .prepare("SELECT repo_slug, head FROM repo_sync_cursor")
        .expect("prepare load_all");
    let rows = stmt
        .query_map([], |row| {
            let slug: String = row.get(0)?;
            let head: String = row.get(1)?;
            Ok((slug, head))
        })
        .expect("query load_all");
    rows.map(|r| r.expect("read repo_sync_cursor row"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ParticipantId;
    use crate::db::load_pending_ingress_for_drain;
    use crate::slice::init_db_memory;

    fn seed_conversation(conn: &Connection, conv_id: i64) {
        conn.execute(
            "INSERT OR IGNORE INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'test', 'hash', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, created_at, updated_at, app_slug) \
             VALUES (?1, 1, 'active', '2024-01-01', '2024-01-01', 'appa')",
            rusqlite::params![conv_id],
        )
        .unwrap();
    }

    fn pending_count(conn: &Connection, conv_id: i64) -> usize {
        let subscriber = ParticipantId::for_conversation(conv_id);
        load_pending_ingress_for_drain(conn, &subscriber).len()
    }

    #[test]
    fn upsert_and_enqueue_commits_atomically() {
        let db = init_db_memory();
        let mut conn = db.blocking_lock();
        seed_conversation(&conn, 1);
        seed_conversation(&conn, 2);

        let rows = vec![
            EnqueueRow {
                conversation_id: 1,
                app_slug: "appa",
                source: "repo_sync:pulled",
                summary: "s1",
                payload: r#"{"slug":"r1"}"#,
            },
            EnqueueRow {
                conversation_id: 2,
                app_slug: "appa",
                source: "repo_sync:pulled",
                summary: "s2",
                payload: r#"{"slug":"r1"}"#,
            },
        ];
        upsert_and_enqueue(&mut conn, "r1", "abc123", &rows);

        // Cursor row landed.
        let head: String = conn
            .query_row(
                "SELECT head FROM repo_sync_cursor WHERE repo_slug = ?1",
                rusqlite::params!["r1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(head, "abc123");

        // Both event rows landed.
        assert_eq!(pending_count(&conn, 1), 1);
        assert_eq!(pending_count(&conn, 2), 1);
    }

    /// Reject the ingress insert whose summary is `summary`, so a fan-out that
    /// reaches that row fails mid-transaction the way a schema violation would.
    fn reject_ingress_row(conn: &Connection, summary: &str) {
        conn.execute(
            &format!(
                "CREATE TRIGGER reject_row BEFORE INSERT ON messaging_messages \
                 WHEN NEW.ingress_summary = '{summary}' \
                 BEGIN SELECT RAISE(ABORT, 'rejected'); END"
            ),
            [],
        )
        .unwrap();
    }

    #[test]
    fn upsert_and_enqueue_rolls_back_on_event_insert_failure() {
        let db = init_db_memory();
        let mut conn = db.blocking_lock();
        seed_conversation(&conn, 1);
        seed_conversation(&conn, 2);
        reject_ingress_row(&conn, "s2");

        let rows = vec![
            EnqueueRow {
                conversation_id: 1,
                app_slug: "appa",
                source: "repo_sync:pulled",
                summary: "s1",
                payload: "{}",
            },
            EnqueueRow {
                conversation_id: 2,
                app_slug: "appa",
                source: "repo_sync:pulled",
                summary: "s2",
                payload: "{}",
            },
        ];
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            upsert_and_enqueue(&mut conn, "r1", "abc123", &rows);
        }));
        assert!(outcome.is_err(), "a failed ingress insert must panic");

        // Transaction `Drop` rolled everything back.
        let cursor_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM repo_sync_cursor", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(cursor_count, 0, "cursor must not advance");
        assert_eq!(pending_count(&conn, 1), 0, "first row must roll back");
        assert_eq!(pending_count(&conn, 2), 0, "failing row must not land");
    }

    #[test]
    fn load_all_on_empty_table_returns_empty_map() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let map = load_all(&conn);
        assert!(map.is_empty());
    }

    #[test]
    fn load_all_returns_persisted_cursors() {
        let db = init_db_memory();
        let mut conn = db.blocking_lock();

        upsert_and_enqueue(&mut conn, "r1", "head-r1", &[]);
        upsert_and_enqueue(&mut conn, "r2", "head-r2", &[]);

        let map = load_all(&conn);
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("r1").map(String::as_str), Some("head-r1"));
        assert_eq!(map.get("r2").map(String::as_str), Some("head-r2"));
    }

    #[test]
    fn upsert_and_enqueue_with_empty_rows_still_advances_cursor() {
        let db = init_db_memory();
        let mut conn = db.blocking_lock();

        upsert_and_enqueue(&mut conn, "r1", "deadbeef", &[]);

        let head: String = conn
            .query_row(
                "SELECT head FROM repo_sync_cursor WHERE repo_slug = ?1",
                rusqlite::params!["r1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(head, "deadbeef");
    }

    #[test]
    fn upsert_overwrites_existing_head() {
        let db = init_db_memory();
        let mut conn = db.blocking_lock();
        upsert_and_enqueue(&mut conn, "r1", "first", &[]);
        upsert_and_enqueue(&mut conn, "r1", "second", &[]);

        let head: String = conn
            .query_row(
                "SELECT head FROM repo_sync_cursor WHERE repo_slug = ?1",
                rusqlite::params!["r1"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(head, "second");
    }

    #[test]
    fn enqueue_batch_commits_atomically() {
        let db = init_db_memory();
        let mut conn = db.blocking_lock();
        seed_conversation(&conn, 1);
        seed_conversation(&conn, 2);

        let rows = vec![
            EnqueueRow {
                conversation_id: 1,
                app_slug: "appa",
                source: "repo_sync:conflict",
                summary: "c1",
                payload: "{}",
            },
            EnqueueRow {
                conversation_id: 2,
                app_slug: "appa",
                source: "repo_sync:conflict",
                summary: "c2",
                payload: "{}",
            },
        ];
        enqueue_batch(&mut conn, &rows);

        // Cursor must NOT have been touched.
        let cursor_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM repo_sync_cursor", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(cursor_count, 0);

        // Both event rows landed.
        assert_eq!(pending_count(&conn, 1), 1);
        assert_eq!(pending_count(&conn, 2), 1);
    }

    #[test]
    fn enqueue_batch_rolls_back_on_failure() {
        let db = init_db_memory();
        let mut conn = db.blocking_lock();
        seed_conversation(&conn, 1);
        seed_conversation(&conn, 2);
        reject_ingress_row(&conn, "c2");

        let rows = vec![
            EnqueueRow {
                conversation_id: 1,
                app_slug: "appa",
                source: "repo_sync:conflict",
                summary: "c1",
                payload: "{}",
            },
            EnqueueRow {
                conversation_id: 2,
                app_slug: "appa",
                source: "repo_sync:conflict",
                summary: "c2",
                payload: "{}",
            },
        ];
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            enqueue_batch(&mut conn, &rows);
        }));
        assert!(outcome.is_err(), "a failed ingress insert must panic");

        assert_eq!(pending_count(&conn, 1), 0, "first row must roll back");
        assert_eq!(pending_count(&conn, 2), 0, "failing row must not land");
    }
}

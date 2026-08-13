//! Automation table schema: the DDL for the three automation tables, the
//! row-rewrite migration over them, and the fire-outcome constants that the
//! `automation_fires.outcome` `CHECK` constraint below enumerates.
//!
//! This crate owns these tables — its DAO ([`crate::db`]) is the only
//! production code that writes them. Callers must run
//! [`run_automation_migrations`] on any connection the automation engine uses.
//! `automation_app_event_conversation` is foreign-keyed to `conversations`,
//! which must exist first.

use rusqlite::Connection;

/// Create the automation tables and apply their row migrations.
///
/// Idempotent — uses `IF NOT EXISTS` everywhere.
pub fn run_automation_migrations(conn: &Connection) {
    conn.execute_batch(
        "
        -- Automation job rows. One row per job.
        CREATE TABLE IF NOT EXISTS automation_jobs (
            id                  INTEGER PRIMARY KEY,
            uuid                BLOB NOT NULL UNIQUE,
            owner_app_slug      TEXT NOT NULL,
            name                TEXT NOT NULL,

            -- Sum-type trigger: { kind, payload-JSON }.
            -- `kind` is a closed set; payload shape is per-kind and validated
            -- on read by serde_json::from_str into a typed Rust enum. A corrupt
            -- row at read time panics (better dead than wrong stance).
            trigger_kind        TEXT NOT NULL CHECK(trigger_kind IN ('cron')),
            trigger_payload     TEXT NOT NULL,

            -- Sum-type action: same shape as trigger.
            action_kind         TEXT NOT NULL CHECK(action_kind IN ('send_message')),
            action_payload      TEXT NOT NULL,

            enabled             INTEGER NOT NULL DEFAULT 1,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,

            created_at          TEXT NOT NULL,
            updated_at          TEXT NOT NULL,
            last_fired_at       TEXT,
            next_fire_at        TEXT NOT NULL,

            -- Stamped on disable to distinguish auto-disable vs. user-disable.
            auto_disabled_at    TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_automation_jobs_next_fire
            ON automation_jobs(next_fire_at)
            WHERE enabled = 1;
        CREATE INDEX IF NOT EXISTS idx_automation_jobs_owner
            ON automation_jobs(owner_app_slug);

        -- Per-job firing-rate-limit audit table.
        -- Rolling-window count is computed from this table at fire-decision
        -- time. Inline opportunistic prune (DELETE rows older than 24h on
        -- each INSERT) keeps the table small in practice.
        -- TODO(automation-fires-cleanup): consider a more sophisticated prune
        -- (e.g. per-N inserts batching) if table growth becomes a concern.
        CREATE TABLE IF NOT EXISTS automation_fires (
            id          INTEGER PRIMARY KEY,
            job_id      INTEGER NOT NULL REFERENCES automation_jobs(id) ON DELETE CASCADE,
            fired_at    TEXT NOT NULL,
            outcome     TEXT NOT NULL CHECK(outcome IN (
                            'ok',
                            'auth',
                            'budget',
                            'rate_limit',
                            'rate_limit_suppressed_report',
                            'action_error',
                            'app_gone'
                        )),
            error_class TEXT,
            detail      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_automation_fires_job_time
            ON automation_fires(job_id, fired_at DESC);

        -- Mapping from owner app slug to the per-app automation events
        -- conversation used for non-singleton apps. Created on first error;
        -- reused subsequently.
        CREATE TABLE IF NOT EXISTS automation_app_event_conversation (
            owner_app_slug  TEXT PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id)
        );
        ",
    )
    .expect("run_automation_migrations");

    migrate_automation_action_payload_wake_to_urgency(conn);
}

/// Migration 4: rewrite `action_payload` JSON rows that carry the legacy
/// `"wake"` key, renaming it to `"urgency"` and mapping values:
/// `"none"` → `"low"`, `"immediate"` → `"normal"`.
///
/// Idempotent: rows without `"wake"` are untouched (the `json_extract` guard
/// matches zero rows on re-run or on a fresh DB). Count guard: matched rows ==
/// rewritten rows (panic if not — host-internal inconsistency).
pub fn migrate_automation_action_payload_wake_to_urgency(conn: &Connection) {
    let legacy_count: i64 = conn
        .query_row(
            "SELECT count(*) FROM automation_jobs \
             WHERE json_extract(action_payload, '$.wake') IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .expect("migrate_automation_action_payload: count legacy rows");

    if legacy_count == 0 {
        return;
    }

    let rewritten: usize = conn
        .execute(
            "UPDATE automation_jobs \
             SET action_payload = json_remove( \
                 json_set( \
                     action_payload, \
                     '$.urgency', \
                     CASE json_extract(action_payload, '$.wake') \
                         WHEN 'immediate' THEN 'normal' \
                         ELSE 'low' \
                     END \
                 ), \
                 '$.wake' \
             ) \
             WHERE json_extract(action_payload, '$.wake') IS NOT NULL",
            [],
        )
        .expect("migrate_automation_action_payload: rewrite rows");

    assert_eq!(
        rewritten as i64, legacy_count,
        "migrate_automation_action_payload: rewritten rows ({rewritten}) != \
         matched rows ({legacy_count}) — host-internal inconsistency"
    );
}

// Fire outcome constants — single source of truth for both Rust code and the
// SQL CHECK constraint above.

pub const OUTCOME_OK: &str = "ok";
pub const OUTCOME_AUTH: &str = "auth";
pub const OUTCOME_BUDGET: &str = "budget";
pub const OUTCOME_RATE_LIMIT: &str = "rate_limit";
pub const OUTCOME_RATE_LIMIT_SUPPRESSED: &str = "rate_limit_suppressed_report";
pub const OUTCOME_ACTION_ERROR: &str = "action_error";
pub const OUTCOME_APP_GONE: &str = "app_gone";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::init_db_memory;

    #[test]
    fn automation_migrations_run_cleanly() {
        let db = init_db_memory();
        let conn = db.blocking_lock();

        conn.execute(
            "SELECT id, uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
             action_kind, action_payload, enabled, consecutive_failures, \
             created_at, updated_at, last_fired_at, next_fire_at, auto_disabled_at \
             FROM automation_jobs WHERE 0",
            [],
        )
        .expect("automation_jobs table should exist");

        conn.execute(
            "SELECT id, job_id, fired_at, outcome, error_class, detail \
             FROM automation_fires WHERE 0",
            [],
        )
        .expect("automation_fires table should exist");

        conn.execute(
            "SELECT owner_app_slug, conversation_id \
             FROM automation_app_event_conversation WHERE 0",
            [],
        )
        .expect("automation_app_event_conversation table should exist");
    }

    /// `init_db_memory` has already run the automation migrations once, so the
    /// explicit second call is the idempotence assertion.
    #[test]
    fn automation_migrations_are_idempotent() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        run_automation_migrations(&conn);
    }

    #[test]
    fn automation_jobs_insert_and_query() {
        let db = init_db_memory();
        let conn = db.blocking_lock();

        let now = "2026-05-07T09:00:00Z";
        let next = "2026-05-07T09:05:00Z";
        let uuid_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();

        conn.execute(
            "INSERT INTO automation_jobs \
             (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, 0, ?8, ?8, ?9)",
            rusqlite::params![
                uuid_bytes,
                "test-app",
                "My Job",
                "cron",
                r#"{"expr":"*/5 * * * *","tz":"UTC","persistent":false}"#,
                "send_message",
                r#"{"to":"brenn:ch","body":"hi","urgency":"low","reply_to":null,"delivery_deadline_secs":null}"#,
                now,
                next,
            ],
        )
        .expect("insert automation_jobs");

        let row_id = conn.last_insert_rowid();
        let name: String = conn
            .query_row(
                "SELECT name FROM automation_jobs WHERE id = ?1",
                rusqlite::params![row_id],
                |r| r.get(0),
            )
            .expect("query automation_jobs");
        assert_eq!(name, "My Job");
    }

    #[test]
    fn automation_fires_cascade_delete() {
        let db = init_db_memory();
        let conn = db.blocking_lock();

        let now = "2026-05-07T09:00:00Z";
        let uuid_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();

        conn.execute(
            "INSERT INTO automation_jobs \
             (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'app', 'j', 'cron', '{}', 'send_message', '{}', 1, 0, ?2, ?2, ?2)",
            rusqlite::params![uuid_bytes, now],
        )
        .expect("insert job");
        let job_id = conn.last_insert_rowid();

        conn.execute(
            "INSERT INTO automation_fires (job_id, fired_at, outcome) VALUES (?1, ?2, 'ok')",
            rusqlite::params![job_id, now],
        )
        .expect("insert fire");

        let fire_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM automation_fires WHERE job_id = ?1",
                rusqlite::params![job_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(fire_count, 1);

        conn.execute(
            "DELETE FROM automation_jobs WHERE id = ?1",
            rusqlite::params![job_id],
        )
        .expect("delete job");

        let fire_count_after: i64 = conn
            .query_row(
                "SELECT count(*) FROM automation_fires WHERE job_id = ?1",
                rusqlite::params![job_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(fire_count_after, 0, "cascade delete should remove fires");
    }

    fn insert_legacy_job(conn: &rusqlite::Connection, action_wake: &str) -> i64 {
        let uuid_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
        let now = "2026-06-01T09:00:00Z";
        let action_payload = format!(
            r#"{{"kind":"send_message","to":"brenn:ch","body":"hi","wake":"{}","reply_to":null,"delivery_deadline_secs":null}}"#,
            action_wake
        );
        conn.execute(
            "INSERT INTO automation_jobs \
             (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'test-app', 'job', 'cron', \
                     '{\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                     'send_message', ?2, 1, 0, ?3, ?3, ?3)",
            rusqlite::params![uuid_bytes, action_payload, now],
        )
        .expect("insert legacy job");
        conn.last_insert_rowid()
    }

    fn get_urgency(conn: &rusqlite::Connection, row_id: i64) -> String {
        conn.query_row(
            "SELECT json_extract(action_payload, '$.urgency') FROM automation_jobs WHERE id = ?1",
            rusqlite::params![row_id],
            |r| r.get::<_, Option<String>>(0),
        )
        .expect("query urgency")
        .unwrap_or_default()
    }

    fn has_wake_key(conn: &rusqlite::Connection, row_id: i64) -> bool {
        let v: Option<String> = conn
            .query_row(
                "SELECT json_extract(action_payload, '$.wake') FROM automation_jobs WHERE id = ?1",
                rusqlite::params![row_id],
                |r| r.get(0),
            )
            .expect("query wake");
        v.is_some()
    }

    #[test]
    fn migration4_maps_none_to_low() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let id = insert_legacy_job(&conn, "none");
        migrate_automation_action_payload_wake_to_urgency(&conn);
        assert_eq!(get_urgency(&conn, id), "low");
        assert!(!has_wake_key(&conn, id));
    }

    #[test]
    fn migration4_maps_immediate_to_normal() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let id = insert_legacy_job(&conn, "immediate");
        migrate_automation_action_payload_wake_to_urgency(&conn);
        assert_eq!(get_urgency(&conn, id), "normal");
        assert!(!has_wake_key(&conn, id));
    }

    #[test]
    fn migration4_idempotent_on_already_migrated_rows() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let uuid_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
        let now = "2026-06-01T09:00:00Z";
        conn.execute(
            "INSERT INTO automation_jobs \
             (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'test-app', 'job', 'cron', \
                     '{\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                     'send_message', \
                     '{\"to\":\"brenn:ch\",\"body\":\"hi\",\"urgency\":\"low\",\"reply_to\":null,\"delivery_deadline_secs\":null}', \
                     1, 0, ?2, ?2, ?2)",
            rusqlite::params![uuid_bytes, now],
        )
        .expect("insert migrated job");
        let id = conn.last_insert_rowid();
        migrate_automation_action_payload_wake_to_urgency(&conn);
        assert_eq!(get_urgency(&conn, id), "low");
        assert!(!has_wake_key(&conn, id));
    }

    #[test]
    fn migration4_handles_mixed_rows() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let legacy_id = insert_legacy_job(&conn, "none");
        let uuid_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
        let now = "2026-06-01T09:00:00Z";
        conn.execute(
            "INSERT INTO automation_jobs \
             (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'test-app', 'job2', 'cron', \
                     '{\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                     'send_message', \
                     '{\"to\":\"brenn:ch\",\"body\":\"hi\",\"urgency\":\"normal\",\"reply_to\":null,\"delivery_deadline_secs\":null}', \
                     1, 0, ?2, ?2, ?2)",
            rusqlite::params![uuid_bytes, now],
        )
        .expect("insert modern job");
        let modern_id = conn.last_insert_rowid();
        migrate_automation_action_payload_wake_to_urgency(&conn);
        assert_eq!(get_urgency(&conn, legacy_id), "low");
        assert!(!has_wake_key(&conn, legacy_id));
        assert_eq!(get_urgency(&conn, modern_id), "normal");
    }
}

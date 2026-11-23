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

            -- Sum-type action: JSON whose `kind` tag names the variant. The
            -- kind and the payload's shape are validated together by
            -- deserialisation at read; a corrupt row panics there.
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
    // The action's kind is its payload's `kind` tag; a table created with a
    // separate `action_kind` column loses it.
    brenn_db::drop_column_if_present(conn, "automation_jobs", "action_kind");
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
             action_payload, enabled, consecutive_failures, \
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
              action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, 0, ?7, ?7, ?8)",
            rusqlite::params![
                uuid_bytes,
                "test-app",
                "My Job",
                "cron",
                r#"{"expr":"*/5 * * * *","tz":"UTC","persistent":false}"#,
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
              action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'app', 'j', 'cron', '{}', '{}', 1, 0, ?2, ?2, ?2)",
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

    /// The `automation_jobs` and `automation_fires` tables as created before
    /// the action's kind lived only in its payload: an `action_kind` column
    /// under its own `CHECK`.
    fn rebuild_with_action_kind_column(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "
            DROP TABLE automation_fires;
            DROP TABLE automation_jobs;
            CREATE TABLE automation_jobs (
                id                  INTEGER PRIMARY KEY,
                uuid                BLOB NOT NULL UNIQUE,
                owner_app_slug      TEXT NOT NULL,
                name                TEXT NOT NULL,
                trigger_kind        TEXT NOT NULL CHECK(trigger_kind IN ('cron')),
                trigger_payload     TEXT NOT NULL,
                action_kind         TEXT NOT NULL CHECK(action_kind IN ('send_message')),
                action_payload      TEXT NOT NULL,
                enabled             INTEGER NOT NULL DEFAULT 1,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                created_at          TEXT NOT NULL,
                updated_at          TEXT NOT NULL,
                last_fired_at       TEXT,
                next_fire_at        TEXT NOT NULL,
                auto_disabled_at    TEXT
            );
            CREATE INDEX idx_automation_jobs_next_fire
                ON automation_jobs(next_fire_at) WHERE enabled = 1;
            CREATE INDEX idx_automation_jobs_owner ON automation_jobs(owner_app_slug);
            CREATE TABLE automation_fires (
                id          INTEGER PRIMARY KEY,
                job_id      INTEGER NOT NULL REFERENCES automation_jobs(id) ON DELETE CASCADE,
                fired_at    TEXT NOT NULL,
                outcome     TEXT NOT NULL CHECK(outcome IN (
                                'ok', 'auth', 'budget', 'rate_limit',
                                'rate_limit_suppressed_report', 'action_error', 'app_gone'
                            )),
                error_class TEXT,
                detail      TEXT
            );
            CREATE INDEX idx_automation_fires_job_time
                ON automation_fires(job_id, fired_at DESC);
            ",
        )
        .expect("rebuild with the action_kind column");
    }

    const OLD_PAYLOAD: &str = r#"{"kind":"send_message","to":"brenn:ch","body":"hi","urgency":"low","reply_to":null,"delivery_deadline_secs":null}"#;

    fn job_count_and_payload(conn: &rusqlite::Connection, job_id: i64) -> (i64, String) {
        let fires: i64 = conn
            .query_row(
                "SELECT count(*) FROM automation_fires WHERE job_id = ?1",
                rusqlite::params![job_id],
                |r| r.get(0),
            )
            .unwrap();
        let payload: String = conn
            .query_row(
                "SELECT action_payload FROM automation_jobs WHERE id = ?1",
                rusqlite::params![job_id],
                |r| r.get(0),
            )
            .unwrap();
        (fires, payload)
    }

    /// A table that still has `action_kind` loses it; its job, payload and
    /// fire rows survive and the job reads; a second run changes nothing.
    #[test]
    fn migration_drops_the_action_kind_column_and_keeps_the_rows() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        rebuild_with_action_kind_column(&conn);
        let uuid = uuid::Uuid::new_v4();
        let now = "2026-06-01T09:00:00Z";
        conn.execute(
            "INSERT INTO automation_jobs \
             (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'test-app', 'job', 'cron', \
                     '{\"kind\":\"cron\",\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                     'send_message', ?2, 1, 0, ?3, ?3, ?3)",
            rusqlite::params![uuid.as_bytes().to_vec(), OLD_PAYLOAD, now],
        )
        .expect("insert job with action_kind");
        let job_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO automation_fires (job_id, fired_at, outcome) VALUES (?1, ?2, 'ok')",
            rusqlite::params![job_id, now],
        )
        .expect("insert fire");

        run_automation_migrations(&conn);
        assert!(!brenn_db::column_exists(
            &conn,
            "automation_jobs",
            "action_kind"
        ));
        assert_eq!(
            job_count_and_payload(&conn, job_id),
            (1, OLD_PAYLOAD.to_string())
        );
        let job = crate::db::get_job(&conn, uuid).expect("the job reads");
        assert!(matches!(job.action, crate::job::Action::SendMessage(_)));

        run_automation_migrations(&conn);
        assert_eq!(
            job_count_and_payload(&conn, job_id),
            (1, OLD_PAYLOAD.to_string())
        );
        let foreign_key_faults: i64 = conn
            .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(foreign_key_faults, 0);
    }

    /// A fresh database has no `action_kind` column and holds an MQTT job.
    #[test]
    fn a_fresh_table_has_no_action_kind_and_holds_an_mqtt_job() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        assert!(!brenn_db::column_exists(
            &conn,
            "automation_jobs",
            "action_kind"
        ));
        let uuid = uuid::Uuid::new_v4();
        let action = crate::job::Action::MqttPublish(crate::job::MqttPublishAction {
            to: "mqtt:home:lights/kitchen/set".to_string(),
            payload: crate::job::MqttActionPayload::from_bytes(b"ON".to_vec()),
            qos: 1,
            retain: false,
            lifetime: brenn_lib::mqtt::MqttLifetime::Forever,
            content_type: None,
            status_to: None,
            reply_to: None,
        });
        let now = chrono::Utc::now();
        crate::db::insert_job(
            &conn,
            uuid,
            "test-app",
            "lights",
            "cron",
            r#"{"kind":"cron","expr":"0 18 * * *","tz":"UTC","persistent":false}"#,
            &serde_json::to_string(&action).unwrap(),
            true,
            now,
            now,
        );
        assert_eq!(crate::db::get_job(&conn, uuid).unwrap().action, action);
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
              action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'test-app', 'job', 'cron', \
                     '{\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                     ?2, 1, 0, ?3, ?3, ?3)",
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
              action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'test-app', 'job', 'cron', \
                     '{\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
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
              action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'test-app', 'job2', 'cron', \
                     '{\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
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

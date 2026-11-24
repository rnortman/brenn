//! Automation table schema: the DDL for the four automation tables, the
//! migrations over them, and the fire-outcome constants that the
//! `automation_fires.outcome` `CHECK` constraint below enumerates.
//!
//! This crate owns these tables — its DAO ([`crate::db`]) is the only
//! production code that writes them. Callers must run
//! [`run_automation_migrations`] on any connection the automation engine uses.
//! `automation_app_event_conversation` is foreign-keyed to `conversations`,
//! which must exist first.

use rusqlite::Connection;

/// Create the automation tables and apply their migrations.
///
/// Idempotent — uses `IF NOT EXISTS` everywhere, and each migration step
/// leaves an already-migrated table alone.
pub fn run_automation_migrations(conn: &Connection) {
    conn.execute_batch(&format!(
        "
        -- Automation job rows. One row per job.
        CREATE TABLE IF NOT EXISTS automation_jobs (
            id                  INTEGER PRIMARY KEY,
            uuid                BLOB NOT NULL UNIQUE,
            owner_app_slug      TEXT NOT NULL,
            name                TEXT NOT NULL,

            -- Sum-type trigger: {{ kind, payload-JSON }}.
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
        {fires_table}
        {fires_index}

        -- Per-app fire ledger. A `fire` row is one fire handed to its action;
        -- a `cap_report` row is one reported refusal by the per-app fire cap.
        -- No foreign key to `automation_jobs`: deleting, editing or
        -- re-creating jobs leaves it as it is. Rows older than the cap's
        -- window are pruned by the gate.
        CREATE TABLE IF NOT EXISTS automation_app_fires (
            id              INTEGER PRIMARY KEY,
            owner_app_slug  TEXT NOT NULL,
            kind            TEXT NOT NULL CHECK(kind IN ('fire', 'cap_report')),
            at              TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_automation_app_fires_app_kind_at
            ON automation_app_fires(owner_app_slug, kind, at);

        -- Mapping from owner app slug to the per-app automation events
        -- conversation used for non-singleton apps. Created on first error;
        -- reused subsequently.
        CREATE TABLE IF NOT EXISTS automation_app_event_conversation (
            owner_app_slug  TEXT PRIMARY KEY,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id)
        );
        ",
        fires_table = automation_fires_ddl("IF NOT EXISTS automation_fires"),
        fires_index = AUTOMATION_FIRES_INDEX,
    ))
    .expect("run_automation_migrations");

    migrate_automation_fires_outcome_check(conn);
    migrate_automation_action_payload_wake_to_urgency(conn);
    // The action's kind is its payload's `kind` tag; a table created with a
    // separate `action_kind` column loses it.
    brenn_db::drop_column_if_present(conn, "automation_jobs", "action_kind");
}

/// `CREATE TABLE <name_clause>` for `automation_fires`, the one definition of
/// its columns and its outcome `CHECK`.
fn automation_fires_ddl(name_clause: &str) -> String {
    let outcomes = FIRE_OUTCOMES
        .iter()
        .map(|o| format!("'{o}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE TABLE {name_clause} (
            id          INTEGER PRIMARY KEY,
            job_id      INTEGER NOT NULL REFERENCES automation_jobs(id) ON DELETE CASCADE,
            fired_at    TEXT NOT NULL,
            outcome     TEXT NOT NULL CHECK(outcome IN ({outcomes})),
            error_class TEXT,
            detail      TEXT
        );"
    )
}

const AUTOMATION_FIRES_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_automation_fires_job_time \
     ON automation_fires(job_id, fired_at DESC);";

/// Rebuild `automation_fires` under the current outcome `CHECK` when the
/// table on disk was created under an older one (its SQL lacks a member of
/// [`FIRE_OUTCOMES`]). Every row is copied; a `budget` row, an outcome no
/// longer stored, becomes `action_error` and keeps its `error_class` and
/// `detail`.
///
/// Idempotent: a table whose SQL names every member of [`FIRE_OUTCOMES`] is
/// left alone. Count guard: rows after == rows before, panic if not.
pub fn migrate_automation_fires_outcome_check(conn: &Connection) {
    let table_sql: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'automation_fires'",
            [],
            |row| row.get(0),
        )
        .expect("migrate_automation_fires_outcome_check: read table SQL");
    if FIRE_OUTCOMES
        .iter()
        .all(|o| table_sql.contains(&format!("'{o}'")))
    {
        return;
    }

    conn.execute("BEGIN", [])
        .expect("migrate_automation_fires_outcome_check: begin");
    let before: i64 = conn
        .query_row("SELECT count(*) FROM automation_fires", [], |row| {
            row.get(0)
        })
        .expect("migrate_automation_fires_outcome_check: count rows before");
    conn.execute_batch(&format!(
        "{new_table}
         INSERT INTO automation_fires_rebuilt (id, job_id, fired_at, outcome, error_class, detail)
             SELECT id, job_id, fired_at,
                    CASE outcome WHEN 'budget' THEN '{OUTCOME_ACTION_ERROR}' ELSE outcome END,
                    error_class, detail
             FROM automation_fires;
         DROP TABLE automation_fires;
         ALTER TABLE automation_fires_rebuilt RENAME TO automation_fires;
         {AUTOMATION_FIRES_INDEX}",
        new_table = automation_fires_ddl("automation_fires_rebuilt"),
    ))
    .expect("migrate_automation_fires_outcome_check: rebuild table");
    let after: i64 = conn
        .query_row("SELECT count(*) FROM automation_fires", [], |row| {
            row.get(0)
        })
        .expect("migrate_automation_fires_outcome_check: count rows after");
    assert_eq!(
        after, before,
        "migrate_automation_fires_outcome_check: rows after rebuild ({after}) != rows before \
         ({before}) — host-internal inconsistency"
    );
    conn.execute("COMMIT", [])
        .expect("migrate_automation_fires_outcome_check: commit");
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
pub const OUTCOME_RATE_LIMIT: &str = "rate_limit";
pub const OUTCOME_RATE_LIMIT_SUPPRESSED: &str = "rate_limit_suppressed_report";
pub const OUTCOME_ACTION_ERROR: &str = "action_error";
pub const OUTCOME_APP_GONE: &str = "app_gone";
/// The fire was refused by the per-app fire cap: its slot is skipped and it
/// counts toward neither auto-disable nor either per-job window.
pub const OUTCOME_APP_FIRE_CAP: &str = "app_fire_cap";

/// Every outcome `automation_fires` may store: its `CHECK` lists exactly
/// these, and a table on disk lacking one is rebuilt at startup.
pub const FIRE_OUTCOMES: [&str; 7] = [
    OUTCOME_OK,
    OUTCOME_AUTH,
    OUTCOME_APP_FIRE_CAP,
    OUTCOME_RATE_LIMIT,
    OUTCOME_RATE_LIMIT_SUPPRESSED,
    OUTCOME_ACTION_ERROR,
    OUTCOME_APP_GONE,
];

// `automation_app_fires.kind` values.

pub const APP_FIRE_KIND_FIRE: &str = "fire";
pub const APP_FIRE_KIND_CAP_REPORT: &str = "cap_report";

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

        conn.execute(
            "SELECT id, owner_app_slug, kind, at FROM automation_app_fires WHERE 0",
            [],
        )
        .expect("automation_app_fires table should exist");
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

    /// The per-app ledger has no foreign key to the jobs: deleting a job
    /// leaves the app's ledger rows where they were.
    #[test]
    fn deleting_a_job_leaves_the_app_fire_ledger() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let now = "2026-05-07T09:00:00Z";
        conn.execute(
            "INSERT INTO automation_jobs \
             (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'app', 'j', 'cron', '{}', '{}', 1, 0, ?2, ?2, ?2)",
            rusqlite::params![uuid::Uuid::new_v4().as_bytes().to_vec(), now],
        )
        .expect("insert job");
        let job_id = conn.last_insert_rowid();
        for kind in [APP_FIRE_KIND_FIRE, APP_FIRE_KIND_CAP_REPORT] {
            conn.execute(
                "INSERT INTO automation_app_fires (owner_app_slug, kind, at) VALUES ('app', ?1, ?2)",
                rusqlite::params![kind, now],
            )
            .expect("insert ledger row");
        }
        conn.execute(
            "DELETE FROM automation_jobs WHERE id = ?1",
            rusqlite::params![job_id],
        )
        .expect("delete job");
        let ledger: i64 = conn
            .query_row("SELECT count(*) FROM automation_app_fires", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(ledger, 2);
    }

    /// Every outcome the table held before `app_fire_cap` existed.
    const OLD_OUTCOMES: [&str; 7] = [
        "ok",
        "auth",
        "budget",
        "rate_limit",
        "rate_limit_suppressed_report",
        "action_error",
        "app_gone",
    ];

    fn fire_rows(
        conn: &rusqlite::Connection,
    ) -> Vec<(i64, String, Option<String>, Option<String>)> {
        let mut stmt = conn
            .prepare("SELECT id, outcome, error_class, detail FROM automation_fires ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// A table under the old `CHECK`, holding a row of every old outcome, is
    /// rebuilt: the row count holds, the `budget` row reads `action_error`
    /// with its `error_class` and `detail` kept, every other row is as it was,
    /// `app_fire_cap` can be stored, and a second run changes nothing.
    #[test]
    fn migration_rebuilds_the_outcome_check_and_retires_budget() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        rebuild_with_action_kind_column(&conn);
        let now = "2026-06-01T09:00:00Z";
        conn.execute(
            "INSERT INTO automation_jobs \
             (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, 'test-app', 'job', 'cron', \
                     '{\"kind\":\"cron\",\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                     'send_message', ?2, 1, 0, ?3, ?3, ?3)",
            rusqlite::params![uuid::Uuid::new_v4().as_bytes().to_vec(), OLD_PAYLOAD, now],
        )
        .expect("insert job");
        let job_id = conn.last_insert_rowid();
        for outcome in OLD_OUTCOMES {
            let detail = (outcome == "budget").then_some("budget exhausted");
            conn.execute(
                "INSERT INTO automation_fires (job_id, fired_at, outcome, error_class, detail) \
                 VALUES (?1, ?2, ?3, ?3, ?4)",
                rusqlite::params![job_id, now, outcome, detail],
            )
            .expect("insert old outcome");
        }
        let before = fire_rows(&conn);

        run_automation_migrations(&conn);
        let after = fire_rows(&conn);
        assert_eq!(after.len(), OLD_OUTCOMES.len());
        for (old, new) in before.iter().zip(&after) {
            if old.1 == "budget" {
                assert_eq!(
                    new,
                    &(
                        old.0,
                        OUTCOME_ACTION_ERROR.to_string(),
                        Some("budget".to_string()),
                        Some("budget exhausted".to_string())
                    )
                );
            } else {
                assert_eq!(new, old);
            }
        }
        conn.execute(
            "INSERT INTO automation_fires (job_id, fired_at, outcome, error_class) \
             VALUES (?1, ?2, ?3, ?3)",
            rusqlite::params![job_id, now, OUTCOME_APP_FIRE_CAP],
        )
        .expect("app_fire_cap is storable");
        assert!(
            conn.execute(
                "INSERT INTO automation_fires (job_id, fired_at, outcome) VALUES (?1, ?2, 'budget')",
                rusqlite::params![job_id, now],
            )
            .is_err(),
            "budget is no longer storable"
        );
        let index_present: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master \
                 WHERE type = 'index' AND name = 'idx_automation_fires_job_time'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(index_present, 1);

        let migrated = fire_rows(&conn);
        run_automation_migrations(&conn);
        assert_eq!(fire_rows(&conn), migrated, "a second run changes nothing");
        conn.execute(
            "DELETE FROM automation_jobs WHERE id = ?1",
            rusqlite::params![job_id],
        )
        .unwrap();
        assert!(
            fire_rows(&conn).is_empty(),
            "the rebuilt table still cascades"
        );
    }

    /// A fresh table already carries the current `CHECK` and is left alone.
    #[test]
    fn a_fresh_fires_table_is_not_rebuilt() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let sql = |conn: &rusqlite::Connection| -> String {
            conn.query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'automation_fires'",
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        let fresh = sql(&conn);
        assert!(fresh.contains("'app_fire_cap'") && !fresh.contains("'budget'"));
        migrate_automation_fires_outcome_check(&conn);
        assert_eq!(sql(&conn), fresh);
    }

    /// A table whose `CHECK` lacks any one current outcome is rebuilt, even
    /// when it already names `app_fire_cap`: for each outcome, a table under
    /// the current set minus that outcome refuses it before the migration
    /// and stores it after.
    #[test]
    fn a_check_missing_any_current_outcome_is_rebuilt() {
        for missing in FIRE_OUTCOMES {
            let db = init_db_memory();
            let conn = db.blocking_lock();
            let listed = FIRE_OUTCOMES
                .iter()
                .filter(|o| **o != missing)
                .map(|o| format!("'{o}'"))
                .collect::<Vec<_>>()
                .join(", ");
            conn.execute_batch(&format!(
                "DROP TABLE automation_fires;
                 CREATE TABLE automation_fires (
                     id          INTEGER PRIMARY KEY,
                     job_id      INTEGER NOT NULL REFERENCES automation_jobs(id) ON DELETE CASCADE,
                     fired_at    TEXT NOT NULL,
                     outcome     TEXT NOT NULL CHECK(outcome IN ({listed})),
                     error_class TEXT,
                     detail      TEXT
                 );"
            ))
            .expect("create the fires table under a reduced CHECK");
            let now = "2026-06-01T09:00:00Z";
            conn.execute(
                "INSERT INTO automation_jobs \
                 (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
                  action_payload, enabled, consecutive_failures, \
                  created_at, updated_at, next_fire_at) \
                 VALUES (?1, 'test-app', 'job', 'cron', \
                         '{\"kind\":\"cron\",\"expr\":\"*/5 * * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                         ?2, 1, 0, ?3, ?3, ?3)",
                rusqlite::params![uuid::Uuid::new_v4().as_bytes().to_vec(), OLD_PAYLOAD, now],
            )
            .expect("insert job");
            let job_id = conn.last_insert_rowid();
            let insert = |conn: &rusqlite::Connection| {
                conn.execute(
                    "INSERT INTO automation_fires (job_id, fired_at, outcome) VALUES (?1, ?2, ?3)",
                    rusqlite::params![job_id, now, missing],
                )
            };
            assert!(insert(&conn).is_err(), "{missing} refused before");

            migrate_automation_fires_outcome_check(&conn);
            insert(&conn).unwrap_or_else(|e| panic!("{missing} storable after: {e}"));
        }
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

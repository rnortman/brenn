//! Automation DB schema and DAO.
//!
//! All DDL is applied by [`run_automation_migrations`], invoked from
//! `crate::db::run_migrations`. Idempotent `IF NOT EXISTS` everywhere.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use crate::automation::job::{Action, JobSnapshot, JobView, Trigger};

/// Append automation DDL to the migrations batch.
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

/// Migration 4 (§2.7): rewrite `action_payload` JSON rows that carry the legacy
/// `"wake"` key, renaming it to `"urgency"` and mapping values:
/// `"none"` → `"low"`, `"immediate"` → `"normal"`.
///
/// Idempotent: rows without `"wake"` are untouched (the `json_extract` guard
/// matches zero rows on re-run or on a fresh DB). Count guard: matched rows ==
/// rewritten rows (panic if not — host-internal inconsistency).
pub fn migrate_automation_action_payload_wake_to_urgency(conn: &Connection) {
    // Count rows with legacy `wake` key.
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

    // Rewrite: rename key + map values in one UPDATE per row.
    // json_set / json_remove chain: set urgency from mapped wake, then remove wake.
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

// ---------------------------------------------------------------------------
// Fire outcome constants (single source of truth for both Rust code and the
// SQL CHECK constraint above — quality-4).
// ---------------------------------------------------------------------------

pub(crate) const OUTCOME_OK: &str = "ok";
pub(crate) const OUTCOME_AUTH: &str = "auth";
pub(crate) const OUTCOME_BUDGET: &str = "budget";
pub(crate) const OUTCOME_RATE_LIMIT: &str = "rate_limit";
pub(crate) const OUTCOME_RATE_LIMIT_SUPPRESSED: &str = "rate_limit_suppressed_report";
pub(crate) const OUTCOME_ACTION_ERROR: &str = "action_error";
pub(crate) const OUTCOME_APP_GONE: &str = "app_gone";

// ---------------------------------------------------------------------------
// DAO helpers
// ---------------------------------------------------------------------------

/// Count all jobs (enabled and disabled) owned by `owner_app_slug`.
///
/// Called by `AutomationEngine::create` under the DB mutex lock to enforce the
/// per-app job cap before issuing an INSERT. Disabled jobs count toward the cap
/// — they can be re-enabled, so they represent potential future load.
pub fn count_jobs_for_app(conn: &Connection, owner_app_slug: &str) -> u32 {
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM automation_jobs WHERE owner_app_slug = ?1",
            rusqlite::params![owner_app_slug],
            |row| row.get(0),
        )
        .unwrap_or_else(|e| panic!("count_jobs_for_app owner_app_slug={owner_app_slug}: {e}"));
    u32::try_from(count).expect("job count fits u32")
}

/// Insert a new automation job row. `uuid` is the caller-generated v4 UUID.
/// `trigger_kind` and `action_kind` are derived from the enum variant via
/// `Trigger::kind_str()` / `Action::kind_str()` (quality-2: avoids hardcoding
/// kind strings that go out of sync with the CHECK constraint when new
/// variants are added).
#[allow(clippy::too_many_arguments)]
pub fn insert_job(
    conn: &Connection,
    uuid: Uuid,
    owner_app_slug: &str,
    name: &str,
    trigger_kind: &str,
    trigger_payload: &str,
    action_kind: &str,
    action_payload: &str,
    enabled: bool,
    now: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) {
    let now_str = crate::db::format_ts_for_db(now);
    let next_str = crate::db::format_ts_for_db(next_fire_at);
    let uuid_bytes = uuid.as_bytes().to_vec();
    conn.execute(
        "INSERT INTO automation_jobs \
         (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
          action_kind, action_payload, enabled, consecutive_failures, \
          created_at, updated_at, next_fire_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?9, ?10)",
        rusqlite::params![
            uuid_bytes,
            owner_app_slug,
            name,
            trigger_kind,
            trigger_payload,
            action_kind,
            action_payload,
            enabled as i64,
            now_str,
            next_str,
        ],
    )
    .expect("insert_job");
}

/// Update an existing job's mutable fields in a single statement.
/// Returns `true` if a row was found and updated; `false` if the job was
/// deleted between the ownership check and this write.
///
/// `reset_failure_counter` — when `true` (set by the caller when transitioning
/// `enabled` from 0→1), also resets `consecutive_failures = 0` and clears
/// `auto_disabled_at` so the re-enabled job gets a fresh chance (correctness-4).
#[allow(clippy::too_many_arguments)]
pub fn update_job(
    conn: &Connection,
    uuid: Uuid,
    name: &str,
    trigger_payload: &str,
    action_payload: &str,
    enabled: bool,
    now: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
    reset_failure_counter: bool,
) -> bool {
    let now_str = crate::db::format_ts_for_db(now);
    let next_str = crate::db::format_ts_for_db(next_fire_at);
    let uuid_bytes = uuid.as_bytes().to_vec();
    let sql = if reset_failure_counter {
        "UPDATE automation_jobs \
         SET name = ?1, trigger_payload = ?2, action_payload = ?3, \
             enabled = ?4, updated_at = ?5, next_fire_at = ?6, \
             consecutive_failures = 0, auto_disabled_at = NULL \
         WHERE uuid = ?7"
    } else {
        "UPDATE automation_jobs \
         SET name = ?1, trigger_payload = ?2, action_payload = ?3, \
             enabled = ?4, updated_at = ?5, next_fire_at = ?6 \
         WHERE uuid = ?7"
    };
    let rows_updated = conn
        .execute(
            sql,
            rusqlite::params![
                name,
                trigger_payload,
                action_payload,
                enabled as i64,
                now_str,
                next_str,
                uuid_bytes,
            ],
        )
        .expect("update_job");
    rows_updated > 0
}

/// Delete a job by UUID (fires are cascade-deleted by the FK).
pub fn delete_job(conn: &Connection, uuid: Uuid) {
    let uuid_bytes = uuid.as_bytes().to_vec();
    conn.execute(
        "DELETE FROM automation_jobs WHERE uuid = ?1",
        rusqlite::params![uuid_bytes],
    )
    .expect("delete_job");
}

/// Load a single job snapshot by UUID. Returns `None` if not found.
pub fn get_job(conn: &Connection, uuid: Uuid) -> Option<JobSnapshot> {
    let uuid_bytes = uuid.as_bytes().to_vec();
    conn.query_row(
        "SELECT id, uuid, owner_app_slug, name, trigger_payload, action_payload, \
                enabled, consecutive_failures, created_at, last_fired_at, next_fire_at \
         FROM automation_jobs WHERE uuid = ?1",
        rusqlite::params![uuid_bytes],
        |row| {
            let row_id: i64 = row.get(0)?;
            let raw_uuid: Vec<u8> = row.get(1)?;
            let owner_app_slug: String = row.get(2)?;
            let name: String = row.get(3)?;
            let trigger_payload: String = row.get(4)?;
            let action_payload: String = row.get(5)?;
            let enabled: bool = row.get::<_, i64>(6).map(|v| v != 0)?;
            let consecutive_failures: i64 = row.get(7)?;
            let created_at_str: String = row.get(8)?;
            let last_fired_at_str: Option<String> = row.get(9)?;
            let next_fire_at_str: String = row.get(10)?;
            Ok((
                row_id,
                raw_uuid,
                owner_app_slug,
                name,
                trigger_payload,
                action_payload,
                enabled,
                consecutive_failures,
                created_at_str,
                last_fired_at_str,
                next_fire_at_str,
            ))
        },
    )
    .optional()
    .expect("get_job query")
    .map(|row| {
        let (
            row_id,
            raw_uuid,
            owner_app_slug,
            name,
            trigger_payload,
            action_payload,
            enabled,
            consecutive_failures,
            created_at_str,
            last_fired_at_str,
            next_fire_at_str,
        ) = row;
        let stored_uuid = Uuid::from_slice(&raw_uuid).expect("uuid bytes corrupt");
        let trigger: Trigger = serde_json::from_str(&trigger_payload)
            .unwrap_or_else(|e| panic!("corrupt trigger_payload for job {stored_uuid}: {e}"));
        let action: Action = serde_json::from_str(&action_payload)
            .unwrap_or_else(|e| panic!("corrupt action_payload for job {stored_uuid}: {e}"));
        let created_at = created_at_str
            .parse::<DateTime<Utc>>()
            .unwrap_or_else(|e| panic!("corrupt created_at for job {stored_uuid}: {e}"));
        let last_fired_at = last_fired_at_str.map(|s| {
            s.parse::<DateTime<Utc>>()
                .unwrap_or_else(|e| panic!("corrupt last_fired_at for job {stored_uuid}: {e}"))
        });
        let next_fire_at = next_fire_at_str
            .parse::<DateTime<Utc>>()
            .unwrap_or_else(|e| panic!("corrupt next_fire_at for job {stored_uuid}: {e}"));
        JobSnapshot {
            row_id,
            uuid: stored_uuid,
            owner_app_slug,
            name,
            trigger,
            action,
            enabled,
            consecutive_failures,
            created_at,
            last_fired_at,
            next_fire_at,
        }
    })
}

/// List all jobs owned by `owner_app_slug` as `JobView`s. If `enabled_only`,
/// only enabled jobs are returned.
pub fn list_jobs_by_owner(
    conn: &Connection,
    owner_app_slug: &str,
    enabled_only: bool,
) -> Vec<JobView> {
    let sql = if enabled_only {
        "SELECT id, uuid, owner_app_slug, name, trigger_payload, action_payload, \
                enabled, consecutive_failures, created_at, updated_at, last_fired_at, \
                next_fire_at, auto_disabled_at \
         FROM automation_jobs \
         WHERE owner_app_slug = ?1 AND enabled = 1 \
         ORDER BY created_at"
    } else {
        "SELECT id, uuid, owner_app_slug, name, trigger_payload, action_payload, \
                enabled, consecutive_failures, created_at, updated_at, last_fired_at, \
                next_fire_at, auto_disabled_at \
         FROM automation_jobs \
         WHERE owner_app_slug = ?1 \
         ORDER BY created_at"
    };

    let mut stmt = conn.prepare(sql).expect("list_jobs_by_owner prepare");
    stmt.query_map(rusqlite::params![owner_app_slug], |row| {
        let raw_uuid: Vec<u8> = row.get(1)?;
        let owner: String = row.get(2)?;
        let name: String = row.get(3)?;
        let trigger_payload: String = row.get(4)?;
        let action_payload: String = row.get(5)?;
        let enabled: bool = row.get::<_, i64>(6).map(|v| v != 0)?;
        let consecutive_failures: i64 = row.get(7)?;
        let created_at_str: String = row.get(8)?;
        let updated_at_str: String = row.get(9)?;
        let last_fired_at_str: Option<String> = row.get(10)?;
        let next_fire_at_str: String = row.get(11)?;
        let auto_disabled_at_str: Option<String> = row.get(12)?;
        Ok((
            raw_uuid,
            owner,
            name,
            trigger_payload,
            action_payload,
            enabled,
            consecutive_failures,
            created_at_str,
            updated_at_str,
            last_fired_at_str,
            next_fire_at_str,
            auto_disabled_at_str,
        ))
    })
    .expect("list_jobs_by_owner query")
    .map(|r| {
        let (
            raw_uuid,
            owner,
            name,
            trigger_payload,
            action_payload,
            enabled,
            consecutive_failures,
            created_at_str,
            updated_at_str,
            last_fired_at_str,
            next_fire_at_str,
            auto_disabled_at_str,
        ) = r.expect("list_jobs_by_owner row");
        let uuid = Uuid::from_slice(&raw_uuid).expect("uuid bytes corrupt");
        let trigger: Trigger = serde_json::from_str(&trigger_payload)
            .unwrap_or_else(|e| panic!("corrupt trigger_payload for job {uuid}: {e}"));
        let action: Action = serde_json::from_str(&action_payload)
            .unwrap_or_else(|e| panic!("corrupt action_payload for job {uuid}: {e}"));
        let created_at = created_at_str
            .parse::<DateTime<Utc>>()
            .unwrap_or_else(|e| panic!("corrupt created_at for job {uuid}: {e}"));
        let updated_at = updated_at_str
            .parse::<DateTime<Utc>>()
            .unwrap_or_else(|e| panic!("corrupt updated_at for job {uuid}: {e}"));
        let last_fired_at = last_fired_at_str.map(|s| {
            s.parse::<DateTime<Utc>>()
                .unwrap_or_else(|e| panic!("corrupt last_fired_at for job {uuid}: {e}"))
        });
        let next_fire_at = next_fire_at_str
            .parse::<DateTime<Utc>>()
            .unwrap_or_else(|e| panic!("corrupt next_fire_at for job {uuid}: {e}"));
        let auto_disabled_at = auto_disabled_at_str.map(|s| {
            s.parse::<DateTime<Utc>>()
                .unwrap_or_else(|e| panic!("corrupt auto_disabled_at for job {uuid}: {e}"))
        });
        JobView {
            id: uuid.to_string(),
            owner_app_slug: owner,
            name,
            trigger,
            action,
            enabled,
            consecutive_failures: consecutive_failures as u32,
            created_at,
            updated_at,
            last_fired_at,
            next_fire_at,
            auto_disabled_at,
        }
    })
    .collect()
}

/// Load all enabled jobs whose `next_fire_at <= now`, oldest first.
/// Each row is loaded into a `JobSnapshot` — this is the atomic read used by
/// the fire loop (see §2.7 atomicity boundary in design).
pub fn get_due_jobs(conn: &Connection, now: DateTime<Utc>) -> Vec<JobSnapshot> {
    let now_str = crate::db::format_ts_for_db(now);
    let mut stmt = conn
        .prepare(
            "SELECT id, uuid, owner_app_slug, name, trigger_payload, action_payload, \
                    enabled, consecutive_failures, created_at, last_fired_at, next_fire_at \
             FROM automation_jobs \
             WHERE enabled = 1 AND next_fire_at <= ?1 \
             ORDER BY next_fire_at",
        )
        .expect("get_due_jobs prepare");

    stmt.query_map(rusqlite::params![now_str], |row| {
        let row_id: i64 = row.get(0)?;
        let raw_uuid: Vec<u8> = row.get(1)?;
        let owner_app_slug: String = row.get(2)?;
        let name: String = row.get(3)?;
        let trigger_payload: String = row.get(4)?;
        let action_payload: String = row.get(5)?;
        let enabled: bool = row.get::<_, i64>(6).map(|v| v != 0)?;
        let consecutive_failures: i64 = row.get(7)?;
        let created_at_str: String = row.get(8)?;
        let last_fired_at_str: Option<String> = row.get(9)?;
        let next_fire_at_str: String = row.get(10)?;
        Ok((
            row_id,
            raw_uuid,
            owner_app_slug,
            name,
            trigger_payload,
            action_payload,
            enabled,
            consecutive_failures,
            created_at_str,
            last_fired_at_str,
            next_fire_at_str,
        ))
    })
    .expect("get_due_jobs query")
    .map(|r| {
        let (
            row_id,
            raw_uuid,
            owner_app_slug,
            name,
            trigger_payload,
            action_payload,
            enabled,
            consecutive_failures,
            created_at_str,
            last_fired_at_str,
            next_fire_at_str,
        ) = r.expect("get_due_jobs row");
        let uuid = Uuid::from_slice(&raw_uuid).expect("uuid bytes corrupt");
        let trigger: Trigger = serde_json::from_str(&trigger_payload)
            .unwrap_or_else(|e| panic!("corrupt trigger_payload for job {uuid}: {e}"));
        let action: Action = serde_json::from_str(&action_payload)
            .unwrap_or_else(|e| panic!("corrupt action_payload for job {uuid}: {e}"));
        let created_at = created_at_str
            .parse::<DateTime<Utc>>()
            .unwrap_or_else(|e| panic!("corrupt created_at for job {uuid}: {e}"));
        let last_fired_at = last_fired_at_str.map(|s| {
            s.parse::<DateTime<Utc>>()
                .unwrap_or_else(|e| panic!("corrupt last_fired_at for job {uuid}: {e}"))
        });
        let next_fire_at = next_fire_at_str
            .parse::<DateTime<Utc>>()
            .unwrap_or_else(|e| panic!("corrupt next_fire_at for job {uuid}: {e}"));
        JobSnapshot {
            row_id,
            uuid,
            owner_app_slug,
            name,
            trigger,
            action,
            enabled,
            consecutive_failures,
            created_at,
            last_fired_at,
            next_fire_at,
        }
    })
    .collect()
}

/// Return the earliest `next_fire_at` among enabled jobs, if any.
pub fn earliest_enabled_next_fire(conn: &Connection) -> Option<DateTime<Utc>> {
    conn.query_row(
        "SELECT min(next_fire_at) FROM automation_jobs WHERE enabled = 1",
        [],
        |row| row.get::<_, Option<String>>(0),
    )
    .expect("earliest_enabled_next_fire")
    .map(|s| {
        s.parse::<DateTime<Utc>>()
            .unwrap_or_else(|e| panic!("corrupt min(next_fire_at): {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db_memory;

    #[test]
    fn automation_migrations_run_cleanly() {
        // init_db_memory calls run_migrations which now calls
        // run_automation_migrations; this test verifies the tables exist.
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

    #[test]
    fn automation_migrations_are_idempotent() {
        let conn = rusqlite::Connection::open_in_memory().expect("open");
        conn.pragma_update(None, "foreign_keys", "ON").expect("fk");
        // Run the base migrations so FK targets (conversations) exist.
        // We call crate::db directly via init_db_memory pattern.
        let db = init_db_memory();
        let conn2 = db.blocking_lock();
        // Running automation migrations a second time must not fail.
        run_automation_migrations(&conn2);
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

    /// `count_jobs_for_app` returns the count of all rows (enabled and disabled)
    /// for the given slug, and does not count rows owned by other apps.
    #[test]
    fn count_jobs_for_app_counts_enabled_and_disabled() {
        let db = init_db_memory();
        let conn = db.blocking_lock();

        let now = "2026-05-25T10:00:00Z";

        let insert = |slug: &str, enabled: i64| {
            let uuid_bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
            conn.execute(
                "INSERT INTO automation_jobs \
                 (uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
                  action_kind, action_payload, enabled, consecutive_failures, \
                  created_at, updated_at, next_fire_at) \
                 VALUES (?1, ?2, 'j', 'cron', '{}', 'send_message', '{}', ?3, 0, ?4, ?4, ?4)",
                rusqlite::params![uuid_bytes, slug, enabled, now],
            )
            .expect("insert job");
        };

        // Two rows for "app-a": one enabled, one disabled.
        insert("app-a", 1);
        insert("app-a", 0);
        // One row for "app-b".
        insert("app-b", 1);

        assert_eq!(
            count_jobs_for_app(&conn, "app-a"),
            2,
            "app-a: both enabled and disabled rows must be counted"
        );
        assert_eq!(
            count_jobs_for_app(&conn, "app-b"),
            1,
            "app-b: only its own row must be counted"
        );
        assert_eq!(
            count_jobs_for_app(&conn, "app-c"),
            0,
            "app-c: no rows, must return 0"
        );
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

    // -------------------------------------------------------------------------
    // Migration 4 tests: action_payload wake → urgency rewrite (§2.7)
    // -------------------------------------------------------------------------

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

    /// `wake: "none"` → `urgency: "low"`, `wake` key removed.
    #[test]
    fn migration4_maps_none_to_low() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let id = insert_legacy_job(&conn, "none");
        migrate_automation_action_payload_wake_to_urgency(&conn);
        assert_eq!(get_urgency(&conn, id), "low");
        assert!(!has_wake_key(&conn, id));
    }

    /// `wake: "immediate"` → `urgency: "normal"`, `wake` key removed.
    #[test]
    fn migration4_maps_immediate_to_normal() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let id = insert_legacy_job(&conn, "immediate");
        migrate_automation_action_payload_wake_to_urgency(&conn);
        assert_eq!(get_urgency(&conn, id), "normal");
        assert!(!has_wake_key(&conn, id));
    }

    /// Already-migrated rows (with `urgency`, no `wake`) are untouched on re-run.
    #[test]
    fn migration4_idempotent_on_already_migrated_rows() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        // Insert a row already using `urgency`.
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
        // Running migration again must not error or corrupt the row.
        migrate_automation_action_payload_wake_to_urgency(&conn);
        assert_eq!(get_urgency(&conn, id), "low");
        assert!(!has_wake_key(&conn, id));
    }

    /// Both legacy and modern rows coexist: only legacy rows are rewritten.
    #[test]
    fn migration4_handles_mixed_rows() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let legacy_id = insert_legacy_job(&conn, "none");
        // Insert a modern row.
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

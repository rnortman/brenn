//! Automation DAO.
//!
//! The tables these statements read are created by
//! [`crate::schema::run_automation_migrations`].

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use crate::job::{Action, JobSnapshot, JobView, Trigger};

// ---------------------------------------------------------------------------
// DAO helpers
// ---------------------------------------------------------------------------

/// Count all jobs (enabled and disabled) owned by `owner_app_slug`.
///
/// Disabled jobs count toward the cap — they can be re-enabled, so they
/// represent potential future load.
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
/// `Trigger::kind_str()` / `Action::kind_str()` to stay in sync with the
/// CHECK constraint as new variants are added.
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
    let now_str = brenn_db::format_ts_for_db(now);
    let next_str = brenn_db::format_ts_for_db(next_fire_at);
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
/// `reset_failure_counter` — when `true`, also resets `consecutive_failures = 0`
/// and clears `auto_disabled_at` so the re-enabled job gets a fresh chance.
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
    let now_str = brenn_db::format_ts_for_db(now);
    let next_str = brenn_db::format_ts_for_db(next_fire_at);
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
///
/// Each row is loaded as a `JobSnapshot`. The fire loop runs against the
/// snapshot, never re-reading the row.
pub fn get_due_jobs(conn: &Connection, now: DateTime<Utc>) -> Vec<JobSnapshot> {
    let now_str = brenn_db::format_ts_for_db(now);
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
    use crate::test_support::init_db_memory;

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

        insert("app-a", 1);
        insert("app-a", 0);
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
}

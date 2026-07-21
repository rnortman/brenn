//! Background scheduler loop for the automation engine.
//!
//! Mirrors `messaging/deliver_after.rs`: poll for due jobs, fire them, sleep
//! until the next scheduled job (or up to `POLL_INTERVAL`), wake on kick.
//!
//! Startup catch-up (the `persistent` flag) is handled by
//! `run_startup_catchup` before the loop is spawned.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use super::AutomationEngine;
use super::error_payload::AutomationErrorPayload;
use super::fire::{fire_one, resolve_report_conversation};
use super::job::compute_next;
use crate::messaging::Urgency;

/// Maximum sleep between polls. Same constant as deliver_after/deadline loops.
pub const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Spawn the automation scheduler loop as a Tokio task.
pub fn spawn_automation_loop(engine: Arc<AutomationEngine>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(automation_loop(engine))
}

/// The automation background loop. Runs indefinitely.
pub async fn automation_loop(engine: Arc<AutomationEngine>) {
    loop {
        // Compute how long to sleep: until the earliest due job, or POLL_INTERVAL.
        let sleep_dur = {
            let earliest = engine.earliest_enabled_next_fire().await;
            match earliest {
                Some(dt) => {
                    let now = Utc::now();
                    if dt <= now {
                        Duration::from_millis(0)
                    } else {
                        let millis = (dt - now).num_milliseconds().max(0) as u64;
                        Duration::from_millis(millis).min(POLL_INTERVAL)
                    }
                }
                None => POLL_INTERVAL,
            }
        };

        tokio::select! {
            _ = tokio::time::sleep(sleep_dur) => {}
            _ = engine.kick.notified() => {}
        }

        // Load all due jobs (atomic snapshots) and fire them sequentially.
        let due_jobs = engine.get_due_jobs().await;
        for job in due_jobs {
            fire_one(&engine, job).await;
        }
    }
}

/// Startup catch-up pass: handle jobs that have `next_fire_at <= now`.
///
/// Per design §2.10:
/// - `persistent = true`: enqueue exactly one catch-up fire by resetting
///   `next_fire_at` to the present (the loop will pick it up immediately).
/// - `persistent = false`: advance `last_fired_at` to the most recent past
///   occurrence and `next_fire_at` to the next future occurrence, without
///   firing.
///
/// Must be called *before* `spawn_automation_loop`, while no loop is running.
pub async fn run_startup_catchup(engine: &AutomationEngine) {
    let now = Utc::now();
    let due_jobs = engine.get_due_jobs().await;

    for job in due_jobs {
        let ct = job.trigger.cron_trigger();
        let is_persistent = ct.persistent;

        if is_persistent {
            // Fire once immediately on restart: the job's next_fire_at is
            // already <= now, so the loop will pick it up on the first
            // iteration. Nothing to do here — the loop handles it.
            //
            // However, we do NOT want the loop to see the same slot on every
            // restart after an unclean shutdown. The loop will advance
            // next_fire_at after the fire. If the server crashed mid-fire,
            // last_fired_at was not updated, so the same slot is eligible
            // again — which is the desired behavior for `persistent = true`.
            tracing::debug!(
                job_id = job.row_id,
                name = %job.name,
                "startup catchup: persistent=true, will fire once"
            );
            // Leave next_fire_at as-is; the loop fires it.
        } else {
            // persistent = false: advance without firing.
            // Walk forward from last_fired_at (or created_at) to find the
            // most recent past occurrence, then set next_fire_at to the
            // next future occurrence.
            let anchor = job.last_fired_at.unwrap_or(job.created_at);
            let most_recent_past =
                walk_to_most_recent_past(job.trigger.cron_trigger(), anchor, now);
            let next_future = compute_next(job.trigger.cron_trigger(), now);

            let conn = engine.db.lock().await;
            let last_fired_str = most_recent_past.map(crate::db::format_ts_for_db);
            let next_str = match next_future {
                Some(t) => crate::db::format_ts_for_db(t),
                None => {
                    // Cron is now unsatisfiable; disable the job and notify the
                    // owner so the LLM/user sees it on next wake (design §3).
                    tracing::warn!(
                        job_id = job.row_id,
                        name = %job.name,
                        "startup catchup: persistent=false, cron now unsatisfiable; disabling"
                    );
                    conn.execute(
                        "UPDATE automation_jobs \
                         SET enabled = 0, auto_disabled_at = ?1, updated_at = ?1 \
                         WHERE id = ?2",
                        rusqlite::params![crate::db::format_ts_for_db(now), job.row_id],
                    )
                    .expect("disable unsatisfiable job in catchup");

                    // Insert an automation_fires row so the audit table is consistent.
                    conn.execute(
                        "INSERT INTO automation_fires \
                         (job_id, fired_at, outcome, error_class, detail) \
                         VALUES (?1, ?2, 'action_error', 'action_error', 'unsatisfiable cron at startup')",
                        rusqlite::params![job.row_id, crate::db::format_ts_for_db(now)],
                    )
                    .expect("insert unsatisfiable-startup fire record");
                    drop(conn);

                    // Report to owner so they learn about the auto-disable.
                    let fire_time_str = crate::db::format_ts_for_db(now);
                    let startup_payload = AutomationErrorPayload {
                        detail: "cron expression no longer has future occurrences (detected at startup)",
                        error_class: "unsatisfiable_cron",
                        fire_time: &fire_time_str,
                        job_id: job.row_id,
                        name: &job.name,
                    };
                    let payload = serde_json::to_string(&startup_payload)
                        .expect("AutomationErrorPayload serialization is infallible");
                    let summary = format!(
                        "automation job unsatisfiable cron at startup: job={}",
                        job.name
                    );
                    match resolve_report_conversation(engine, &job).await {
                        None => {
                            tracing::warn!(
                                job_id = job.row_id,
                                name = %job.name,
                                "startup catchup: unsatisfiable-cron report dropped: \
                                 could not resolve report conversation"
                            );
                        }
                        Some(conv_id) => {
                            engine
                                .ingress_router
                                .submit_ingress(
                                    conv_id,
                                    &job.owner_app_slug,
                                    "automation:error",
                                    &summary,
                                    &payload,
                                    Urgency::Normal,
                                )
                                .await;
                        }
                    }
                    continue;
                }
            };

            conn.execute(
                "UPDATE automation_jobs \
                 SET last_fired_at = ?1, next_fire_at = ?2, updated_at = ?3 \
                 WHERE id = ?4",
                rusqlite::params![
                    last_fired_str,
                    next_str,
                    crate::db::format_ts_for_db(now),
                    job.row_id
                ],
            )
            .expect("advance job in startup catchup");

            tracing::debug!(
                job_id = job.row_id,
                name = %job.name,
                "startup catchup: persistent=false, advanced without firing"
            );
        }
    }
}

/// Walk `compute_next` forward from `anchor` until we pass `now`, returning
/// the last occurrence that was <= now. Returns `None` if the first future
/// occurrence is already after `anchor` (nothing missed).
fn walk_to_most_recent_past(
    ct: &crate::automation::job::CronTrigger,
    anchor: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> Option<chrono::DateTime<Utc>> {
    let mut last_past: Option<chrono::DateTime<Utc>> = None;
    let mut cursor = anchor;

    // Cap the walk to protect startup latency.  For a high-frequency cron
    // (e.g. `* * * * *`) with a very old `created_at` this could otherwise
    // iterate millions of times and block the async executor.  If the gap
    // exceeds 90 days we treat it as "no prior fire" — the next_fire_at is
    // then set to the immediate next occurrence from now, which is correct
    // (there is no useful last_fired_at to preserve for a job that hasn't run
    // in 90+ days).
    const MAX_CATCHUP_DAYS: i64 = 90;
    if (now - anchor).num_days() > MAX_CATCHUP_DAYS {
        tracing::warn!(
            gap_days = (now - anchor).num_days(),
            "startup catchup: gap > {MAX_CATCHUP_DAYS} days; skipping walk (treating as no-prior-fire)"
        );
        return None;
    }

    // Walk forward, collecting occurrences <= now.
    while let Some(next) = compute_next(ct, cursor) {
        if next > now {
            break;
        }
        last_past = Some(next);
        cursor = next;
    }
    last_past
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::automation::AutomationEngine;
    use crate::automation::config::AutomationGlobalConfig;
    use crate::automation::test_support::{FakeIngressRouter, FakeWakeRouter, make_engine_full};
    use crate::db::init_db_memory;
    use crate::messaging::MessagingDirectory;
    use crate::obs::alerting::AlertDispatcher;
    use uuid::Uuid;

    fn make_engine_for_catchup(db: crate::db::Db) -> Arc<AutomationEngine> {
        make_engine_full(
            db,
            MessagingDirectory::new(),
            FakeIngressRouter::new(),
            Arc::new(FakeWakeRouter),
            AlertDispatcher::noop().0,
            AutomationGlobalConfig::default(),
            true,
        )
    }

    /// Like `make_engine_for_catchup` but returns the `Arc<FakeIngressRouter>` so
    /// tests can assert on submitted events.
    fn make_engine_for_catchup_with_ingress_router(
        db: crate::db::Db,
    ) -> (Arc<AutomationEngine>, Arc<FakeIngressRouter>) {
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine_full(
            db,
            MessagingDirectory::new(),
            ingress_router.clone() as Arc<dyn crate::automation::IngressRouter>,
            Arc::new(FakeWakeRouter),
            AlertDispatcher::noop().0,
            AutomationGlobalConfig::default(),
            true,
        );
        (engine, ingress_router)
    }

    fn insert_job_due_past(
        conn: &rusqlite::Connection,
        row_id: i64,
        persistent: bool,
        next_fire_at_str: &str,
        last_fired_at_str: Option<&str>,
        created_at_str: &str,
    ) {
        let uuid_bytes = Uuid::new_v4().as_bytes().to_vec();
        let trigger = serde_json::json!({
            "kind": "cron",
            "expr": "0 9 * * *",
            "tz": "UTC",
            "persistent": persistent,
        })
        .to_string();
        let action = serde_json::json!({
            "kind": "send_message",
            "to": "brenn:test",
            "body": "hello",
            "urgency": "low",
            "reply_to": null,
            "delivery_deadline_secs": null,
        })
        .to_string();
        conn.execute(
            "INSERT INTO automation_jobs \
             (id, uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, last_fired_at, next_fire_at) \
             VALUES (?1, ?2, 'test-app', 'test', 'cron', ?3, 'send_message', ?4, 1, 0, \
                     ?5, ?5, ?6, ?7)",
            rusqlite::params![
                row_id,
                uuid_bytes,
                trigger,
                action,
                created_at_str,
                last_fired_at_str,
                next_fire_at_str,
            ],
        )
        .expect("insert test job");
    }

    fn insert_test_user(conn: &rusqlite::Connection) {
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'testuser', 'hash', '2024-01-01')",
            [],
        )
        .unwrap();
    }

    /// Insert a job whose cron expression is unsatisfiable (e.g. Feb 31) and
    /// whose `next_fire_at` is already in the past, so `run_startup_catchup`
    /// picks it up and enters the unsatisfiable-cron branch.
    fn insert_unsatisfiable_cron_job(conn: &rusqlite::Connection, row_id: i64) {
        let uuid_bytes = Uuid::new_v4().as_bytes().to_vec();
        // "0 9 31 2 *" = 09:00 on Feb 31, which never occurs.
        let trigger = serde_json::json!({
            "kind": "cron",
            "expr": "0 9 31 2 *",
            "tz": "UTC",
            "persistent": false,
        })
        .to_string();
        let action = serde_json::json!({
            "kind": "send_message",
            "to": "brenn:test",
            "body": "hello",
            "urgency": "low",
            "reply_to": null,
            "delivery_deadline_secs": null,
        })
        .to_string();
        // Set next_fire_at to a past timestamp so get_due_jobs returns this job.
        conn.execute(
            "INSERT INTO automation_jobs \
             (id, uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, last_fired_at, next_fire_at) \
             VALUES (?1, ?2, 'test-app', 'test', 'cron', ?3, 'send_message', ?4, 1, 0, \
                     '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', NULL, '2026-01-01T09:00:00Z')",
            rusqlite::params![row_id, uuid_bytes, trigger, action],
        )
        .expect("insert unsatisfiable-cron test job");
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn restart_persistent_false_advances_without_firing() {
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            insert_test_user(&conn);
            // Job that was due 3 days ago, persistent=false.
            insert_job_due_past(
                &conn,
                1,
                false,
                "2026-05-04T09:00:00Z", // past
                None,
                "2026-05-01T00:00:00Z",
            );
        }
        let engine = make_engine_for_catchup(db.clone());
        run_startup_catchup(&engine).await;

        // After catchup: next_fire_at must be in the future; no fire record inserted.
        let conn = db.lock().await;
        let (next_fire_str, last_fired_str): (String, Option<String>) = conn
            .query_row(
                "SELECT next_fire_at, last_fired_at FROM automation_jobs WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();

        let next_fire: chrono::DateTime<Utc> = next_fire_str.parse().unwrap();
        assert!(
            next_fire > Utc::now(),
            "next_fire_at should be in the future after catchup"
        );

        // last_fired_at should have been advanced to a recent past occurrence.
        assert!(
            last_fired_str.is_some(),
            "last_fired_at should be set after catchup (most recent past occurrence)"
        );

        let fire_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM automation_fires WHERE job_id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            fire_count, 0,
            "no fires should be recorded for persistent=false catchup"
        );
    }

    #[tokio::test]
    async fn restart_persistent_true_leaves_job_due_for_loop() {
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            insert_test_user(&conn);
            // Job that was due 3 days ago, persistent=true.
            insert_job_due_past(
                &conn,
                1,
                true,
                "2026-05-04T09:00:00Z", // past
                None,
                "2026-05-01T00:00:00Z",
            );
        }
        let engine = make_engine_for_catchup(db.clone());
        run_startup_catchup(&engine).await;

        // With persistent=true, catchup does NOT modify the row; the loop fires it.
        // next_fire_at stays in the past (the loop will pick it up).
        let conn = db.lock().await;
        let next_fire_str: String = conn
            .query_row(
                "SELECT next_fire_at FROM automation_jobs WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let next_fire: chrono::DateTime<Utc> = next_fire_str.parse().unwrap();
        assert!(
            next_fire <= Utc::now(),
            "persistent=true: next_fire_at should still be in past after catchup (loop fires it)"
        );
    }

    #[tokio::test]
    async fn restart_no_missed_slots_is_noop() {
        let db = init_db_memory();
        let future_fire = crate::db::format_ts_for_db(Utc::now() + chrono::Duration::minutes(30));
        {
            let conn = db.lock().await;
            insert_test_user(&conn);
            // Job with next_fire_at in the future — no catchup needed.
            conn.execute(
                "INSERT INTO automation_jobs \
                 (id, uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
                  action_kind, action_payload, enabled, consecutive_failures, \
                  created_at, updated_at, next_fire_at) \
                 VALUES (1, x'00000000000000000000000000000001', 'test-app', 'test', \
                         'cron', '{\"kind\":\"cron\",\"expr\":\"0 9 * * *\",\"tz\":\"UTC\",\"persistent\":false}', \
                         'send_message', \
                         '{\"kind\":\"send_message\",\"to\":\"brenn:test\",\"body\":\"hi\",\"wake\":\"none\",\"reply_to\":null,\"delivery_deadline_secs\":null}', \
                         1, 0, '2026-01-01', '2026-01-01', ?1)",
                rusqlite::params![future_fire],
            )
            .unwrap();
        }

        let engine = make_engine_for_catchup(db.clone());
        run_startup_catchup(&engine).await;

        // No changes: get_due_jobs returns empty for this job.
        let conn = db.lock().await;
        let fire_count: i64 = conn
            .query_row("SELECT count(*) FROM automation_fires", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fire_count, 0, "no fires for a job not yet due");
    }

    /// When a `persistent=false` job has an unsatisfiable cron expression,
    /// `run_startup_catchup` must disable the job and call `submit_ingress` with
    /// source `"automation:error"` and `Urgency::Normal`, so the
    /// owner's conversation surfaces the auto-disable notification.
    #[tokio::test]
    async fn unsatisfiable_cron_at_startup_calls_submit_ingress() {
        let db = init_db_memory();
        {
            let conn = db.lock().await;
            insert_test_user(&conn);
            insert_unsatisfiable_cron_job(&conn, 1);
        }
        let (engine, ingress_router) = make_engine_for_catchup_with_ingress_router(db.clone());
        run_startup_catchup(&engine).await;

        // Job must be disabled.
        {
            let conn = db.lock().await;
            let enabled: i64 = conn
                .query_row(
                    "SELECT enabled FROM automation_jobs WHERE id = 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(enabled, 0, "unsatisfiable-cron job must be auto-disabled");
        }

        // Exactly one event must have been submitted.
        let events = ingress_router.events().await;
        assert_eq!(
            events.len(),
            1,
            "unsatisfiable-cron branch must call submit_ingress exactly once"
        );
        assert_eq!(
            events[0].2, "automation:error",
            "event source must be 'automation:error'"
        );
        assert_eq!(
            events[0].5,
            Urgency::Normal,
            "unsatisfiable-cron event must use Urgency::Normal"
        );
    }
}

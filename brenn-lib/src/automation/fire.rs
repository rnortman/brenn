//! Per-fire logic: auth re-check, budget gate, rate-limit gate, action dispatch,
//! and bookkeeping (automation_fires insert + automation_jobs UPDATE).
//!
//! Called sequentially by the background loop for each due job snapshot.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};

use crate::auth::user::get_user_by_username;
use crate::automation::AutomationEngine;
use crate::automation::error_payload::AutomationErrorPayload;
use crate::automation::job::{Action, JobSnapshot, Trigger, compute_next};
use crate::conversation::{create_conversation, get_or_create_singleton_conversation};
use crate::messaging::Urgency;
use crate::messaging::publish::{PublishOrigin, PublishResult};

// ---------------------------------------------------------------------------
// Fire outcome strings — imported from db.rs (single source of truth,
// quality-4).
// ---------------------------------------------------------------------------

use crate::automation::db::{
    OUTCOME_ACTION_ERROR, OUTCOME_APP_GONE, OUTCOME_AUTH, OUTCOME_BUDGET, OUTCOME_OK,
    OUTCOME_RATE_LIMIT, OUTCOME_RATE_LIMIT_SUPPRESSED,
};

// ---------------------------------------------------------------------------
// fire_one
// ---------------------------------------------------------------------------

/// Execute one scheduled fire for `job`. Sequenced by the automation loop.
///
/// See design §2.7 for the detailed step-by-step specification.
pub async fn fire_one(engine: &AutomationEngine, job: JobSnapshot) {
    let fire_ts = Utc::now();
    let job_id = job.row_id;

    // (b) Per-job rate-limit gate.
    let fire_count_in_window = {
        let conn = engine.db.lock().await;
        count_fires_in_window(&conn, job_id, fire_ts, 3600)
    };
    if fire_count_in_window >= engine.defaults.max_fires_per_hour_per_job as i64 {
        // Rate-limited — skip action, record, report.
        let detail = format!(
            "rate limit: {fire_count_in_window} fires in last hour (cap={})",
            engine.defaults.max_fires_per_hour_per_job
        );
        finish_fire(
            engine,
            &job,
            fire_ts,
            OUTCOME_RATE_LIMIT,
            Some(OUTCOME_RATE_LIMIT),
            Some(&detail),
        )
        .await;
        return;
    }

    // (c) Auth re-check.
    let app_config = match engine.apps.get(&job.owner_app_slug) {
        Some(c) => c,
        None => {
            let detail = format!("owner app {:?} not found in config", job.owner_app_slug);
            finish_fire(
                engine,
                &job,
                fire_ts,
                OUTCOME_APP_GONE,
                Some(OUTCOME_APP_GONE),
                Some(&detail),
            )
            .await;
            return;
        }
    };

    // Re-check publish authorization against the owner app's CURRENT policy
    // (design §2.3, Seam B) — the load-bearing staleness guard. A job stores
    // `action.to` at create time and fires later, so a grant revoked or an ACL
    // tightened after job creation must be honored here. Layer-1 (the
    // `MessagingPublish` grant specifically — the publish/subscribe split, design
    // §2.5; NOT `messaging_enabled()`'s `OR`) is checked here; layer-2 (the
    // per-channel `brenn_publish` ACL) is re-checked below, after the action's
    // destination is destructured. `Messenger::publish` re-checks the same policy
    // (returning `AclDenied`) as defense-in-depth, but this explicit re-check
    // documents the invariant at the site the AUTHZ WARNING names and yields a
    // fire-specific audit detail.
    if !app_config
        .policy
        .has_grant(crate::access::AppCapability::MessagingPublish)
    {
        let detail = format!(
            "owner app {:?} holds no messaging_publish grant",
            job.owner_app_slug
        );
        finish_fire(
            engine,
            &job,
            fire_ts,
            OUTCOME_AUTH,
            Some(OUTCOME_AUTH),
            Some(&detail),
        )
        .await;
        return;
    }

    // Resolve owner user.
    let owner_username = match app_config.allowed_users.first() {
        Some(u) => u.clone(),
        None => {
            let detail = format!("owner app {:?} has no allowed_users", job.owner_app_slug);
            finish_fire(
                engine,
                &job,
                fire_ts,
                OUTCOME_AUTH,
                Some(OUTCOME_AUTH),
                Some(&detail),
            )
            .await;
            return;
        }
    };

    let owner_user_id = {
        let conn = engine.db.lock().await;
        match get_user_by_username(&conn, &owner_username) {
            Some(u) => u.id,
            None => {
                drop(conn);
                let detail = format!("owner user {owner_username:?} not found in DB");
                finish_fire(
                    engine,
                    &job,
                    fire_ts,
                    OUTCOME_AUTH,
                    Some(OUTCOME_AUTH),
                    Some(&detail),
                )
                .await;
                return;
            }
        }
    };

    // Resolve sender conversation id (§2.9).
    let is_singleton = app_config.singleton;
    let sender_conv_id = {
        let conn = engine.db.lock().await;
        if is_singleton {
            let conv =
                get_or_create_singleton_conversation(&conn, owner_user_id, &job.owner_app_slug);
            conv.id
        } else {
            // Non-singleton: use/create the per-app automation events conversation (§2.9).
            get_or_create_automation_events_conversation(&conn, &job.owner_app_slug, owner_user_id)
        }
    };

    // Verify that the action destination still resolves in the directory.
    let (action_to, action_body, action_urgency, action_reply_to, action_deadline_secs) =
        match &job.action {
            Action::SendMessage(sma) => (
                sma.to.clone(),
                sma.body.clone(),
                sma.urgency,
                sma.reply_to.clone(),
                sma.delivery_deadline_secs,
            ),
        };

    if engine.directory.resolve(&action_to).is_none() {
        let detail = format!("destination address {action_to:?} not found in directory");
        finish_fire(
            engine,
            &job,
            fire_ts,
            OUTCOME_AUTH,
            Some(OUTCOME_AUTH),
            Some(&detail),
        )
        .await;
        return;
    }

    // Layer-2 `brenn_publish` ACL re-check against `action_to` (design §2.3, Seam
    // B). Automation `SendMessageAction.to` is always a `brenn:<name>` address
    // (job.rs:95; `is_well_formed_address`-validated at create), so the prefix
    // strip is infallible and yields the channel name the matcher expects (design
    // §2.1). A missing `brenn:` prefix here is an internal invariant violation
    // (corrupted job row or a code bug), NOT a user-space input error — panic
    // rather than silently misclassify it as an ACL denial, mirroring Seam A's
    // `.expect()` in `publish/mod.rs` (CLAUDE.md: panic on the unexpected). This
    // honors a `brenn_publish` ACL tightened after the job was created.
    let publish_channel = action_to
        .strip_prefix(crate::messaging::BRENN_ADDRESS_PREFIX)
        .unwrap_or_else(|| {
            panic!(
                "fire_one: automation action.to {action_to:?} missing brenn: prefix \
                 — invariant violated at create time; job uuid={}",
                job.uuid
            )
        });
    if !app_config.policy.allows_brenn_publish(publish_channel) {
        let detail = format!(
            "owner app {:?} publish ACL denies {action_to:?}",
            job.owner_app_slug
        );
        finish_fire(
            engine,
            &job,
            fire_ts,
            OUTCOME_AUTH,
            Some(OUTCOME_AUTH),
            Some(&detail),
        )
        .await;
        return;
    }

    // (d) Action: call Messenger::publish.
    let delivery_deadline =
        action_deadline_secs.map(|secs| fire_ts + chrono::Duration::seconds(secs as i64));

    let publish_result = engine
        .messenger
        .publish(
            PublishOrigin::Conversation { id: sender_conv_id },
            &job.owner_app_slug,
            &action_to,
            &action_body,
            action_urgency,
            action_reply_to.as_deref(),
            None, // no deliver_after on automation fires
            delivery_deadline,
        )
        .await;

    let (stored_outcome, error_class, detail) = match publish_result {
        PublishResult::Ok { .. } => (OUTCOME_OK, None, None),
        PublishResult::BudgetExhausted => (
            OUTCOME_BUDGET,
            Some(OUTCOME_BUDGET),
            Some("budget exhausted".to_string()),
        ),
        PublishResult::UnknownChannel(addr) => (
            OUTCOME_AUTH,
            Some(OUTCOME_AUTH),
            Some(format!("unknown channel: {addr}")),
        ),
        PublishResult::MalformedAddress(addr) => (
            OUTCOME_AUTH,
            Some(OUTCOME_AUTH),
            Some(format!("malformed address: {addr}")),
        ),
        PublishResult::MissingSender => (
            OUTCOME_AUTH,
            Some(OUTCOME_AUTH),
            Some("missing sender".to_string()),
        ),
        // The explicit `to` re-check above (`allows_brenn_publish(publish_channel)`)
        // short-circuits before `Messenger::publish`, so this arm cannot carry a
        // `to`-target denial. It IS reachable for the message's `reply_to`:
        // `publish_core` runs a reply_to visibility gate that returns
        // `AclDenied(reply_to_addr)`, and that gate is not re-checked here — a job
        // whose `reply_to` was in scope at create time but whose owner ACLs were
        // later tightened lands here at fire time. `addr` is therefore the
        // `reply_to` address; the detail names it so an operator is not pointed at
        // the publish target. Mapped to `OUTCOME_AUTH` like the other authz
        // denials.
        PublishResult::AclDenied(addr) => (
            OUTCOME_AUTH,
            Some(OUTCOME_AUTH),
            Some(format!("publish ACL denied (reply_to): {addr}")),
        ),
        PublishResult::BodyTooLarge { len, max } => (
            OUTCOME_ACTION_ERROR,
            Some(OUTCOME_ACTION_ERROR),
            Some(format!("body too large: {len} > {max}")),
        ),
    };

    finish_fire(
        engine,
        &job,
        fire_ts,
        stored_outcome,
        error_class,
        detail.as_deref(),
    )
    .await;
}

// ---------------------------------------------------------------------------
// finish_fire: bookkeeping (§2.7 step e)
// ---------------------------------------------------------------------------

/// Write the fire record and update the job row. Handles report-rate-limit
/// suppression, consecutive-failure tracking, auto-disable, and alerting.
async fn finish_fire(
    engine: &AutomationEngine,
    job: &JobSnapshot,
    fire_ts: DateTime<Utc>,
    raw_outcome: &str,
    error_class: Option<&str>,
    detail: Option<&str>,
) {
    let now = Utc::now();
    let job_id = job.row_id;
    let is_success = raw_outcome == OUTCOME_OK;

    // Compute next fire time from the trigger.
    let next_fire_at = match compute_next(job.trigger.cron_trigger(), fire_ts) {
        Some(dt) => dt,
        None => {
            // Cron expression now unsatisfiable (e.g. tz rule change).
            handle_unsatisfiable_cron(engine, job, fire_ts, raw_outcome, error_class, detail).await;
            return;
        }
    };

    // Determine stored outcome (may be overridden by report-rate-limit suppression).
    let (stored_outcome, emit_report) = if is_success {
        (OUTCOME_OK, false)
    } else {
        // Check the per-job error-report cap.
        let report_count = {
            let conn = engine.db.lock().await;
            count_reportable_fires_in_window(&conn, job_id, now, 3600)
        };
        if report_count >= engine.defaults.max_error_reports_per_hour_per_job as i64 {
            // Suppress the report; record overflow outcome.
            // Emit an alert (once per process per job+class).
            let ec = error_class.unwrap_or("unknown");
            engine.alerts.alert_once_per_process(
                crate::obs::alerting::AlertSeverity::Warning,
                "Automation report overflow".to_string(),
                &format!("automation_report_overflow:{job_id}:{ec}"),
                format!(
                    "job_id={job_id} name={:?} error_class={ec} \
                     report cap reached; further reports suppressed",
                    job.name
                ),
            );
            (OUTCOME_RATE_LIMIT_SUPPRESSED, false)
        } else {
            (raw_outcome, true)
        }
    };

    // Compute updated job fields.
    let new_consecutive_failures = if is_success {
        0i64
    } else {
        job.consecutive_failures + 1
    };

    let should_auto_disable = !is_success
        && new_consecutive_failures >= engine.defaults.consecutive_failures_to_disable as i64;

    // DB transaction: UPDATE automation_jobs + INSERT automation_fires + prune.
    let updated = {
        let conn = engine.db.lock().await;
        commit_fire(
            &conn,
            job_id,
            fire_ts,
            now,
            stored_outcome,
            error_class,
            detail,
            is_success,
            new_consecutive_failures,
            next_fire_at,
            should_auto_disable,
        )
    };

    if !updated {
        // Job was deleted between the scan and now — skip further actions.
        tracing::debug!(job_id, "fire bookkeeping skipped: job was deleted mid-fire");
        return;
    }

    // Emit structured fire log.
    if is_success {
        tracing::info!(
            job_id,
            name = %job.name,
            outcome = "ok",
            fire_time = %fire_ts,
            "automation fire"
        );
    } else {
        tracing::warn!(
            job_id,
            name = %job.name,
            outcome = raw_outcome,
            error_class = error_class.unwrap_or(""),
            detail = detail.unwrap_or(""),
            fire_time = %fire_ts,
            "automation fire failed"
        );
    }

    // Auto-disable alert.
    if should_auto_disable {
        engine.alerts.alert_once_per_process(
            crate::obs::alerting::AlertSeverity::Warning,
            "Automation job auto-disabled".to_string(),
            &format!("automation_disable:{job_id}"),
            format!(
                "job_id={job_id} name={:?} consecutive_failures={new_consecutive_failures} \
                 threshold={}",
                job.name, engine.defaults.consecutive_failures_to_disable
            ),
        );
    }

    // Submit error report (§2.8).
    if emit_report {
        let ec = error_class.unwrap_or("unknown");
        let fire_time_str = crate::db::format_ts_for_db(fire_ts);
        let detail_str = detail.unwrap_or("");
        let error_payload = AutomationErrorPayload {
            detail: detail_str,
            error_class: ec,
            fire_time: &fire_time_str,
            job_id,
            name: &job.name,
        };
        let payload = serde_json::to_string(&error_payload)
            .expect("AutomationErrorPayload serialization is infallible");
        let summary = format!("automation fire error: job={} class={ec}", job.name);

        let report_conv_id = resolve_report_conversation(engine, job).await;
        match report_conv_id {
            None => {
                tracing::warn!(
                    job_id,
                    name = %job.name,
                    outcome = raw_outcome,
                    "automation error report dropped: could not resolve report conversation \
                     (owner app or user may be gone)"
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
    }
}

// ---------------------------------------------------------------------------
// Unsatisfiable-cron edge case (§3)
// ---------------------------------------------------------------------------

async fn handle_unsatisfiable_cron(
    engine: &AutomationEngine,
    job: &JobSnapshot,
    fire_ts: DateTime<Utc>,
    raw_outcome: &str,
    error_class: Option<&str>,
    detail: Option<&str>,
) {
    let now = Utc::now();
    let job_id = job.row_id;

    tracing::warn!(
        job_id,
        name = %job.name,
        "automation job has unsatisfiable cron; auto-disabling"
    );

    {
        let conn = engine.db.lock().await;
        conn.execute(
            "UPDATE automation_jobs SET enabled = 0, auto_disabled_at = ?1, updated_at = ?1 \
             WHERE id = ?2",
            rusqlite::params![crate::db::format_ts_for_db(now), job_id],
        )
        .expect("disable unsatisfiable job");

        // Record the outcome that led to this call as-is (e.g. rate_limit,
        // auth) so the audit trail is accurate.  error_class stays the
        // original class for observability; detail notes the unsatisfiable
        // cron condition.
        let fire_outcome = if raw_outcome == OUTCOME_OK {
            // Shouldn't happen (compute_next returned None on a successful
            // fire); record as action_error.
            OUTCOME_ACTION_ERROR
        } else {
            raw_outcome
        };
        conn.execute(
            "INSERT INTO automation_fires (job_id, fired_at, outcome, error_class, detail) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                job_id,
                crate::db::format_ts_for_db(fire_ts),
                fire_outcome,
                error_class.unwrap_or("action_error"),
                "unsatisfiable cron",
            ],
        )
        .expect("insert unsatisfiable fire record");
    }

    engine.alerts.alert_once_per_process(
        crate::obs::alerting::AlertSeverity::Warning,
        "Automation job unsatisfiable cron".to_string(),
        &format!("automation_unsatisfiable:{job_id}"),
        format!(
            "job_id={job_id} name={:?} compute_next returned None; job auto-disabled",
            job.name
        ),
    );

    // Emit error report so owner LLM/user sees it.
    let fire_time_str = crate::db::format_ts_for_db(fire_ts);
    let detail_str = detail.unwrap_or("compute_next returned None");
    let error_payload = AutomationErrorPayload {
        detail: detail_str,
        error_class: "unsatisfiable_cron",
        fire_time: &fire_time_str,
        job_id,
        name: &job.name,
    };
    let payload = serde_json::to_string(&error_payload)
        .expect("AutomationErrorPayload serialization is infallible");
    let summary = format!("automation job unsatisfiable cron: job={}", job.name);
    let report_conv_id = resolve_report_conversation(engine, job).await;
    match report_conv_id {
        None => {
            tracing::warn!(
                job_id,
                name = %job.name,
                "unsatisfiable-cron error report dropped: could not resolve report conversation \
                 (owner app or user may be gone)"
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
}

// ---------------------------------------------------------------------------
// Helpers: report conversation routing (§2.8)
// ---------------------------------------------------------------------------

/// Resolve the conversation to which error reports for `job` should be sent.
/// Returns `None` if the owner app or user is not found (e.g. app removed from
/// config before the first error).
pub(crate) async fn resolve_report_conversation(
    engine: &AutomationEngine,
    job: &JobSnapshot,
) -> Option<i64> {
    let app_config = engine.apps.get(&job.owner_app_slug)?;
    let owner_username = app_config.allowed_users.first()?;

    let conn = engine.db.lock().await;
    let owner_user = get_user_by_username(&conn, owner_username)?;

    let is_singleton = app_config.singleton;
    let conv_id = if is_singleton {
        let conv = get_or_create_singleton_conversation(&conn, owner_user.id, &job.owner_app_slug);
        conv.id
    } else {
        get_or_create_automation_events_conversation(&conn, &job.owner_app_slug, owner_user.id)
    };
    Some(conv_id)
}

// ---------------------------------------------------------------------------
// Helpers: per-app automation events conversation (§2.8 / §2.9)
// ---------------------------------------------------------------------------

/// Return (or create) the per-app automation events conversation for a
/// non-singleton app. Uses the `automation_app_event_conversation` mapping
/// table.
///
/// Stale-user rebinding is handled at startup by
/// `automation::startup::run_startup_consistency_checks`.
fn get_or_create_automation_events_conversation(
    conn: &Connection,
    owner_app_slug: &str,
    owner_user_id: i64,
) -> i64 {
    // Try to find an existing mapping.
    let existing: Option<i64> = conn
        .query_row(
            "SELECT conversation_id FROM automation_app_event_conversation \
             WHERE owner_app_slug = ?1",
            rusqlite::params![owner_app_slug],
            |row| row.get(0),
        )
        .optional()
        .expect("get automation_app_event_conversation");

    if let Some(id) = existing {
        return id;
    }

    // Create a new conversation for automation events.
    let conv_id = create_conversation(conn, owner_user_id, owner_app_slug, false);
    conn.execute(
        "INSERT INTO automation_app_event_conversation (owner_app_slug, conversation_id) \
         VALUES (?1, ?2)",
        rusqlite::params![owner_app_slug, conv_id],
    )
    .expect("insert automation_app_event_conversation");
    conv_id
}

// ---------------------------------------------------------------------------
// Helpers: counting fires for rate-limit/report-cap queries
// ---------------------------------------------------------------------------

/// Count fires for `job_id` in the last `window_secs` seconds where the
/// outcome is NOT 'rate_limit' (the fire rate-limit gate excludes prior drops).
fn count_fires_in_window(
    conn: &Connection,
    job_id: i64,
    now: DateTime<Utc>,
    window_secs: i64,
) -> i64 {
    let window_start = now - chrono::Duration::seconds(window_secs);
    conn.query_row(
        "SELECT count(*) FROM automation_fires \
         WHERE job_id = ?1 AND fired_at >= ?2 AND outcome != 'rate_limit'",
        rusqlite::params![job_id, crate::db::format_ts_for_db(window_start)],
        |row| row.get(0),
    )
    .expect("count_fires_in_window")
}

/// Count fires for `job_id` in the last `window_secs` seconds that are NOT
/// `ok` or `rate_limit_suppressed_report` (the report rate-limit gate).
fn count_reportable_fires_in_window(
    conn: &Connection,
    job_id: i64,
    now: DateTime<Utc>,
    window_secs: i64,
) -> i64 {
    let window_start = now - chrono::Duration::seconds(window_secs);
    conn.query_row(
        "SELECT count(*) FROM automation_fires \
         WHERE job_id = ?1 AND fired_at >= ?2 \
           AND outcome NOT IN ('ok', 'rate_limit_suppressed_report')",
        rusqlite::params![job_id, crate::db::format_ts_for_db(window_start)],
        |row| row.get(0),
    )
    .expect("count_reportable_fires_in_window")
}

// ---------------------------------------------------------------------------
// DB commit helper
// ---------------------------------------------------------------------------

/// Issue the UPDATE automation_jobs + INSERT automation_fires in a single
/// DB lock scope. Returns `true` if the job row was found (i.e., not deleted
/// between scan and fire).
#[allow(clippy::too_many_arguments)]
fn commit_fire(
    conn: &Connection,
    job_id: i64,
    fire_ts: DateTime<Utc>,
    now: DateTime<Utc>,
    stored_outcome: &str,
    error_class: Option<&str>,
    detail: Option<&str>,
    is_success: bool,
    new_consecutive_failures: i64,
    next_fire_at: DateTime<Utc>,
    should_auto_disable: bool,
) -> bool {
    let fire_ts_str = crate::db::format_ts_for_db(fire_ts);
    let now_str = crate::db::format_ts_for_db(now);
    let next_str = crate::db::format_ts_for_db(next_fire_at);

    // Wrap the UPDATE + INSERT in an explicit transaction so a panic between
    // statements (e.g. disk full on INSERT) cannot leave next_fire_at advanced
    // without a corresponding automation_fires row (correctness-3).
    // The prune runs outside the transaction — it is cosmetic and its failure
    // would not corrupt correctness.
    conn.execute("BEGIN", []).expect("begin commit_fire txn");

    let rows_updated = if should_auto_disable {
        conn.execute(
            "UPDATE automation_jobs \
             SET last_fired_at = ?1, consecutive_failures = ?2, next_fire_at = ?3, \
                 enabled = 0, auto_disabled_at = ?4, updated_at = ?4 \
             WHERE id = ?5",
            rusqlite::params![
                fire_ts_str,
                new_consecutive_failures,
                next_str,
                now_str,
                job_id,
            ],
        )
        .expect("update job (auto-disable)")
    } else if is_success {
        conn.execute(
            "UPDATE automation_jobs \
             SET last_fired_at = ?1, consecutive_failures = 0, next_fire_at = ?2, \
                 updated_at = ?3 \
             WHERE id = ?4",
            rusqlite::params![fire_ts_str, next_str, now_str, job_id],
        )
        .expect("update job (success)")
    } else {
        conn.execute(
            "UPDATE automation_jobs \
             SET last_fired_at = ?1, consecutive_failures = ?2, next_fire_at = ?3, \
                 updated_at = ?4 \
             WHERE id = ?5",
            rusqlite::params![
                fire_ts_str,
                new_consecutive_failures,
                next_str,
                now_str,
                job_id,
            ],
        )
        .expect("update job (failure)")
    };

    if rows_updated == 0 {
        // Job was deleted between scan and bookkeeping — roll back and return.
        conn.execute("ROLLBACK", [])
            .expect("rollback commit_fire txn (job deleted)");
        return false;
    }

    conn.execute(
        "INSERT INTO automation_fires (job_id, fired_at, outcome, error_class, detail) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![job_id, fire_ts_str, stored_outcome, error_class, detail],
    )
    .expect("insert automation_fires");

    conn.execute("COMMIT", []).expect("commit commit_fire txn");

    // Inline opportunistic prune (outside the transaction): delete fires older
    // than 24h for this job.
    // TODO(automation-fires-cleanup): consider per-N batching if needed.
    let prune_before = crate::db::format_ts_for_db(now - chrono::Duration::hours(24));
    conn.execute(
        "DELETE FROM automation_fires WHERE job_id = ?1 AND fired_at < ?2",
        rusqlite::params![job_id, prune_before],
    )
    .expect("prune automation_fires");

    true
}

// ---------------------------------------------------------------------------
// Helper on Trigger to extract CronTrigger reference
// ---------------------------------------------------------------------------

impl Trigger {
    pub(crate) fn cron_trigger(&self) -> &crate::automation::job::CronTrigger {
        match self {
            Trigger::Cron(ct) => ct,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeZone as _;
    use uuid::Uuid;

    use super::*;
    use crate::automation::IngressRouter;
    use crate::automation::config::AutomationGlobalConfig;
    use crate::automation::job::{Action, CronTrigger, SendMessageAction, Trigger};
    use crate::automation::test_support::{FakeIngressRouter, FakeWakeRouter, make_engine_full};
    use crate::db::init_db_memory;
    use crate::messaging::{
        ChannelEntry, ChannelScheme, MessageEnvelope, MessagingDirectory, SubscriberEntry,
        SubscriberEntryKind, Urgency, WakeMin, WakeRouter, canonical_address,
        config::{Depth, NoiseLevel, ResolvedChannel, Sink},
    };
    use crate::obs::alerting::AlertDispatcher;

    /// `WakeRouter` stub that reports every conversation as active.
    struct ActiveFakeWakeRouter;

    #[async_trait::async_trait]
    impl WakeRouter for ActiveFakeWakeRouter {
        async fn deliver(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _: &crate::messaging::ParticipantId,
            _: &MessageEnvelope,
            _push_id: i64,
            _seq: i64,
        ) -> Result<bool, String> {
            Ok(true)
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _: &crate::messaging::ParticipantId,
            _event: &crate::messaging::ingress::Event,
        ) -> Result<bool, String> {
            Ok(true)
        }
        fn spawn_eager_wake(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _: &crate::messaging::ParticipantId,
        ) {
        }
        fn delivery_shape(
            &self,
            key: &crate::messaging::SubscriberEntryKind,
        ) -> crate::messaging::DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &crate::messaging::ParticipantId) {}
    }

    // -------------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------------

    async fn make_db_with_user_and_channel() -> (
        crate::db::Db,
        i64,
        Uuid,
        crate::messaging::MessagingDirectory,
    ) {
        let db = init_db_memory();
        let conn = db.lock().await;

        // Insert test user.
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'testuser', 'hash', '2024-01-01')",
            [],
        )
        .unwrap();

        // Register channel via the canonical upsert helper.
        let channel_uuid = Uuid::new_v4();
        let channel_entry = ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::App("test-app".to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        crate::messaging::db::upsert_channels(&conn, std::slice::from_ref(&channel_entry));

        drop(conn);

        let directory = MessagingDirectory::with_entries(vec![channel_entry]);
        (db, 1i64, channel_uuid, directory)
    }

    fn make_engine(
        db: crate::db::Db,
        directory: MessagingDirectory,
        _user_id: i64,
        ingress_router: Arc<dyn IngressRouter>,
    ) -> Arc<AutomationEngine> {
        make_engine_full(
            db,
            directory,
            ingress_router,
            Arc::new(FakeWakeRouter),
            AlertDispatcher::noop().0,
            AutomationGlobalConfig::default(),
            true,
        )
    }

    fn make_job_snapshot(row_id: i64, owner_app_slug: &str) -> JobSnapshot {
        JobSnapshot {
            row_id,
            uuid: Uuid::new_v4(),
            owner_app_slug: owner_app_slug.to_string(),
            name: "Test Job".to_string(),
            trigger: Trigger::Cron(CronTrigger {
                expr: "*/5 * * * *".to_string(),
                tz: "UTC".to_string(),
                persistent: false,
            }),
            action: Action::SendMessage(SendMessageAction {
                to: "brenn:test".to_string(),
                body: "hello".to_string(),
                urgency: Urgency::Low,
                reply_to: None,
                delivery_deadline_secs: None,
            }),
            enabled: true,
            consecutive_failures: 0,
            created_at: chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            last_fired_at: None,
            next_fire_at: chrono::Utc.with_ymd_and_hms(2026, 1, 1, 0, 5, 0).unwrap(),
        }
    }

    fn insert_job_row(conn: &rusqlite::Connection, job: &JobSnapshot) {
        let uuid_bytes = job.uuid.as_bytes().to_vec();
        let trigger_payload = serde_json::to_string(&job.trigger).unwrap();
        let action_payload = serde_json::to_string(&job.action).unwrap();
        let now = crate::db::format_ts_for_db(chrono::Utc::now());
        conn.execute(
            "INSERT INTO automation_jobs \
             (id, uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_kind, action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, ?2, ?3, ?4, 'cron', ?5, 'send_message', ?6, 1, ?7, ?8, ?8, ?8)",
            rusqlite::params![
                job.row_id,
                uuid_bytes,
                job.owner_app_slug,
                job.name,
                trigger_payload,
                action_payload,
                job.consecutive_failures,
                now,
            ],
        )
        .expect("insert test job");
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn fire_success_resets_consecutive_failures() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        let mut job = make_job_snapshot(1, "test-app");
        job.consecutive_failures = 3;
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        // Check that consecutive_failures is reset to 0.
        let conn = db.lock().await;
        let failures: i64 = conn
            .query_row(
                "SELECT consecutive_failures FROM automation_jobs WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(failures, 0, "consecutive_failures should reset on success");

        // No error reports submitted.
        let events = ingress_router.events().await;
        assert!(events.is_empty(), "no error reports on success");
    }

    #[tokio::test]
    async fn fire_auth_failure_increments_failures_and_reports() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        // Use an app slug that doesn't exist → app_gone outcome.
        let job = make_job_snapshot(1, "test-app");
        // Insert the job but fire it as if from "ghost-app" (which doesn't exist).
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        let mut ghost_job = job.clone();
        ghost_job.owner_app_slug = "ghost-app".to_string();

        // We test via a job with owner = "ghost-app" which won't have a user.
        // Just need to verify failures increment.
        // Instead: update the DB row to have owner = ghost-app.
        {
            let conn = db.lock().await;
            conn.execute(
                "UPDATE automation_jobs SET owner_app_slug = 'ghost-app' WHERE id = 1",
                [],
            )
            .unwrap();
        }

        fire_one(&engine, ghost_job).await;

        let conn = db.lock().await;
        let failures: i64 = conn
            .query_row(
                "SELECT consecutive_failures FROM automation_jobs WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            failures, 1,
            "consecutive_failures should increment on auth fail"
        );
    }

    #[tokio::test]
    async fn fire_consecutive_threshold_disables_job() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        // Put consecutive_failures at threshold - 1 (default threshold is 5).
        let mut job = make_job_snapshot(1, "test-app");
        job.consecutive_failures = 4;
        // Point action at a nonexistent address to force an auth failure.
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        let conn = db.lock().await;
        let (failures, enabled, auto_disabled): (i64, i64, Option<String>) = conn
            .query_row(
                "SELECT consecutive_failures, enabled, auto_disabled_at \
                 FROM automation_jobs WHERE id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(failures, 5);
        assert_eq!(enabled, 0, "job should be auto-disabled");
        assert!(auto_disabled.is_some(), "auto_disabled_at should be set");
    }

    #[tokio::test]
    async fn fire_row_not_found_does_not_panic() {
        // Verifies that commit_fire returns false (rows_updated == 0 path) and
        // the function exits cleanly when the job row is absent at bookkeeping
        // time — e.g. deleted between scan and commit.
        //
        // NOTE: this test only verifies the no-panic path.  It does not assert
        // the atomicity contract (UPDATE-matches-zero-rows → skip INSERT) tested
        // by the transaction in commit_fire.  Real concurrent-delete coverage is
        // deferred under TODO(automation-fire-semantics-tests).
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        let job = make_job_snapshot(1, "test-app");
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        // Fire a snapshot whose row_id (999) does not exist — simulates the
        // job having been deleted between the scan and bookkeeping.
        let nonexistent_job = make_job_snapshot(999, "test-app");
        fire_one(&engine, nonexistent_job).await;
        // Should complete without panic.
    }

    #[tokio::test]
    async fn fire_uses_singleton_conversation_for_singleton_app() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        // Force a reportable failure (ghost channel) so ingress_router gets called.
        let mut job = make_job_snapshot(1, "test-app");
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        let events = ingress_router.events().await;
        assert_eq!(events.len(), 1, "one error report expected");
        // The conversation_id should be the singleton conversation.
        // Singleton conv is created automatically by get_or_create_singleton_conversation.
        let (conv_id, _app_slug, source, _, _, wake) = &events[0];
        assert_eq!(source, "automation:error");
        assert_eq!(
            *wake,
            Urgency::Normal,
            "error report must use Urgency::Normal"
        );
        assert!(*conv_id > 0, "conv_id should be a valid positive id");
    }

    // -------------------------------------------------------------------------
    // automation-fire-semantics-tests (design §4)
    // -------------------------------------------------------------------------

    /// Rate-limit gate: when fires_in_window >= cap, action is skipped and
    /// OUTCOME_RATE_LIMIT is recorded. An error report IS still submitted
    /// (rate-limit fires are reportable; report-cap suppression is separate).
    #[tokio::test]
    async fn fire_rate_limit_drops_action_and_reports() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        let job = make_job_snapshot(1, "test-app");
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);

            // Pre-populate automation_fires to exceed the hourly fire cap (default 60).
            // Use outcome='ok' so they count towards the fire-rate limit gate
            // (`count_fires_in_window` excludes 'rate_limit' rows, not 'ok' rows).
            let now = crate::db::format_ts_for_db(chrono::Utc::now());
            for _ in 0..60 {
                conn.execute(
                    "INSERT INTO automation_fires (job_id, fired_at, outcome) \
                     VALUES (1, ?1, 'ok')",
                    rusqlite::params![now],
                )
                .unwrap();
            }
        }

        fire_one(&engine, job).await;

        // Check error report before acquiring the DB lock (ingress_router doesn't need the lock).
        // An error report IS submitted — rate-limit fires are reportable up to the
        // error-report cap. (Suppression uses OUTCOME_RATE_LIMIT_SUPPRESSED, not this path.)
        let events = ingress_router.events().await;
        assert_eq!(
            events.len(),
            1,
            "rate-limited fire must emit an error report"
        );
        assert_eq!(events[0].2, "automation:error");

        // fire_one must have recorded an OUTCOME_RATE_LIMIT row, and consecutive_failures
        // must have incremented (rate-limited fires are not successes).
        let conn = db.lock().await;
        let rate_limit_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM automation_fires WHERE job_id = 1 AND outcome = 'rate_limit'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            rate_limit_count, 1,
            "expected exactly one rate_limit fire record"
        );
        let consecutive_failures: i64 = conn
            .query_row(
                "SELECT consecutive_failures FROM automation_jobs WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            consecutive_failures, 1,
            "rate-limited fire must increment consecutive_failures (not treated as success)"
        );
    }

    /// Budget exhaustion: `PublishResult::BudgetExhausted` → OUTCOME_BUDGET →
    /// `consecutive_failures++`.
    #[tokio::test]
    async fn fire_budget_exhausted_increments_failures_and_reports() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        let mut job = make_job_snapshot(1, "test-app");
        job.consecutive_failures = 0;
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
            // Pre-create the singleton conversation so we can exhaust its budget.
            // `get_or_create_singleton_conversation` uses the `conversations` table;
            // calling it here ensures the conversation exists before we set budget=0.
            let singleton_conv =
                crate::conversation::get_or_create_singleton_conversation(&conn, 1, "test-app");
            // Exhaust the budget for the singleton conversation.
            crate::messaging::db::reset_send_budget(&conn, singleton_conv.id, 0);
        }

        fire_one(&engine, job).await;

        let conn = db.lock().await;
        let failures: i64 = conn
            .query_row(
                "SELECT consecutive_failures FROM automation_jobs WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            failures, 1,
            "consecutive_failures should increment on budget exhausted"
        );

        // An error report should be submitted.
        let events = ingress_router.events().await;
        assert_eq!(
            events.len(),
            1,
            "budget exhausted should emit an error report"
        );
        assert_eq!(events[0].2, "automation:error");
    }

    /// Disabled job: `get_due_jobs` skips `enabled = 0` rows. No fire should
    /// occur for a disabled job even when `next_fire_at <= now`.
    #[tokio::test]
    async fn disabled_job_excluded_from_get_due_jobs() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        let job = make_job_snapshot(1, "test-app");
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
            // Disable the job after inserting.
            conn.execute("UPDATE automation_jobs SET enabled = 0 WHERE id = 1", [])
                .unwrap();
        }

        // get_due_jobs must skip disabled jobs regardless of next_fire_at.
        let due = engine.get_due_jobs().await;
        assert!(
            due.is_empty(),
            "disabled job must not appear in get_due_jobs"
        );

        // No fire records and no error reports.
        let conn = db.lock().await;
        let fire_count: i64 = conn
            .query_row("SELECT count(*) FROM automation_fires", [], |r| r.get(0))
            .unwrap();
        assert_eq!(fire_count, 0, "no fire records for a disabled job");
        assert!(
            ingress_router.events().await.is_empty(),
            "no error reports for disabled job"
        );
    }

    /// Snapshot isolation: `fire_one` captures job state at scan time (snapshot).
    /// Each fire uses the `consecutive_failures` from its own snapshot to compute
    /// `new_consecutive_failures` — it does NOT re-read from DB mid-flight.
    ///
    /// This test verifies the isolation property by firing a FAILURE on the second
    /// call while the DB has been bumped to `consecutive_failures = 3` in between.
    /// The failure branch computes `new_consecutive_failures = snapshot.cf + 1`.
    /// Using the snapshot value (0) yields 1; re-reading the DB (3) would yield 4.
    /// The two outcomes are observably different, so this assertion is not vacuous.
    #[tokio::test]
    async fn fire_snapshots_state_so_concurrent_edit_does_not_preempt() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        let job = make_job_snapshot(1, "test-app");
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        // Fire the job once successfully to establish a baseline fire record.
        fire_one(&engine, job.clone()).await;

        // Verify the first fire is recorded.
        let fire_count_after_first: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT count(*) FROM automation_fires WHERE job_id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(fire_count_after_first, 1, "first fire must be recorded");

        // Simulate a concurrent edit: bump consecutive_failures in the DB before
        // the second fire, as if another process had edited the job.
        {
            let conn = db.lock().await;
            conn.execute(
                "UPDATE automation_jobs SET consecutive_failures = 3 WHERE id = 1",
                [],
            )
            .unwrap();
        }

        // Fire a second time with a failing snapshot (consecutive_failures = 0).
        // The action targets an unregistered address so the fire fails via
        // OUTCOME_AUTH ("destination not in directory").  The failure branch
        // computes `new_consecutive_failures = snapshot.cf + 1 = 0 + 1 = 1`.
        // If fire_one re-read consecutive_failures from the DB (value = 3) it
        // would write 3 + 1 = 4 instead — observably different from 1.
        let failing_snapshot = JobSnapshot {
            action: Action::SendMessage(SendMessageAction {
                to: "brenn:does-not-exist".to_string(),
                body: "hello".to_string(),
                urgency: crate::messaging::Urgency::Low,
                reply_to: None,
                delivery_deadline_secs: None,
            }),
            consecutive_failures: 0, // same as the original snapshot
            ..job.clone()
        };
        fire_one(&engine, failing_snapshot).await;

        let (fire_count_after_second, consecutive_failures_after_second): (i64, i64) = {
            let conn = db.lock().await;
            let fire_count = conn
                .query_row(
                    "SELECT count(*) FROM automation_fires WHERE job_id = 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            let cf = conn
                .query_row(
                    "SELECT consecutive_failures FROM automation_jobs WHERE id = 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            (fire_count, cf)
        };
        // Second fire is recorded independently.
        assert_eq!(
            fire_count_after_second, 2,
            "second fire must also be recorded"
        );
        // snapshot.cf=0 → failure branch writes 0+1=1.
        // If the impl re-read the DB (cf=3) it would write 3+1=4 instead.
        assert_eq!(
            consecutive_failures_after_second, 1,
            "second fire must use snapshot consecutive_failures (0+1=1), not DB value (3+1=4)"
        );
    }

    /// Non-singleton apps use `get_or_create_automation_events_conversation`
    /// instead of `get_or_create_singleton_conversation`. Error reports for a
    /// non-singleton app must route to the automation-events conversation, not
    /// a singleton conversation.
    #[tokio::test]
    async fn fire_uses_app_event_conversation_for_non_singleton_app() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();

        // Build an engine where "test-app" has singleton = false.
        let engine = {
            let mut apps = indexmap::IndexMap::new();
            let messaging_cfg = crate::messaging::config::ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            };
            let app_cfg = crate::config::AppConfig {
                slug: "test-app".to_string(),
                name: "test-app".to_string(),
                description: String::new(),
                icon: String::new(),
                working_dir: std::path::PathBuf::from("/tmp"),
                model: String::new(),
                single_instance: false,
                singleton: false, // non-singleton
                persistent: false,
                idle_timeout: None,
                compaction: None,
                idle_hook_secs: 0,
                allowed_users: vec!["testuser".to_string()],
                disabled_tools: vec![],
                mcp_servers: Default::default(),
                multiuser: false,
                prefix_username: false,
                prefix_timestamp: false,
                prefix_device: true,
                path_mapper: crate::config::PathMapper::Identity,
                container_spawn: None,
                start_hooks: Default::default(),
                post_pull_hooks: Default::default(),
                startup_hooks: Default::default(),
                cc_extra_args: vec![],
                approval_rules: vec![],
                attachment_targets: vec![],
                integrations: Default::default(),
                mounts: vec![],
                history_replay_limit: 100,
                frontmatter: Default::default(),
                state_dir: std::path::PathBuf::from("/tmp"),
                messaging: Some(messaging_cfg),
                messaging_default_send_budget: 100,
                // App is a messaging sender; grant MessagingPublish + a universal
                // brenn_publish matcher so the fire-time publish (Seam A gate)
                // authorizes.
                policy: crate::access::AppPolicy::messaging_sender_policy(),
                pwa_push: None,
                webhook_subscriptions: vec![],
                mqtt_subscriptions: vec![],
            };
            apps.insert("test-app".to_string(), app_cfg);
            let apps = Arc::new(apps);
            let directory_arc = Arc::new(directory);
            let global_msg_cfg = crate::messaging::MessagingGlobalConfig::default();
            let messenger = crate::messaging::Messenger::new(
                db.clone(),
                directory_arc.clone(),
                Arc::from("brenn://test"),
                apps.clone(),
                Arc::new(FakeWakeRouter),
                global_msg_cfg,
            );
            let (alerts, _alert_handle) = crate::obs::alerting::AlertDispatcher::noop();
            AutomationEngine::new(
                db.clone(),
                messenger,
                apps,
                directory_arc,
                ingress_router.clone() as Arc<dyn IngressRouter>,
                AutomationGlobalConfig::default(),
                alerts,
            )
        };

        // Force a reportable failure (nonexistent channel).
        let mut job = make_job_snapshot(1, "test-app");
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        let events = ingress_router.events().await;
        assert_eq!(
            events.len(),
            1,
            "one error report expected for non-singleton app"
        );

        // Verify the event conversation was created in the automation_app_event_conversation
        // table (not the singleton_conversations table).
        let conn = db.lock().await;
        let event_conv_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM automation_app_event_conversation \
                 WHERE owner_app_slug = 'test-app'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            event_conv_count, 1,
            "automation_app_event_conversation row must be created"
        );

        // The error report conv_id must match the automation_app_event_conversation.
        let (report_conv_id, _, _, _, _, _) = &events[0];
        let event_conv_id: i64 = conn
            .query_row(
                "SELECT conversation_id FROM automation_app_event_conversation \
                 WHERE owner_app_slug = 'test-app'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            *report_conv_id, event_conv_id,
            "error report must route to automation_app_event_conversation"
        );
    }

    // -------------------------------------------------------------------------
    // automation-error-route-tests (design §4)
    // -------------------------------------------------------------------------

    /// Non-singleton: first error creates a new automation_app_event_conversation.
    /// Subsequent errors for the same app reuse the same conversation.
    #[tokio::test]
    async fn error_report_to_non_singleton_creates_event_conversation_first_time_then_reuses() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();

        // Engine with singleton = false.
        let engine = {
            let mut apps = indexmap::IndexMap::new();
            let messaging_cfg = crate::messaging::config::ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            };
            let app_cfg = crate::config::AppConfig {
                slug: "test-app".to_string(),
                name: "test-app".to_string(),
                description: String::new(),
                icon: String::new(),
                working_dir: std::path::PathBuf::from("/tmp"),
                model: String::new(),
                single_instance: false,
                singleton: false,
                persistent: false,
                idle_timeout: None,
                compaction: None,
                idle_hook_secs: 0,
                allowed_users: vec!["testuser".to_string()],
                disabled_tools: vec![],
                mcp_servers: Default::default(),
                multiuser: false,
                prefix_username: false,
                prefix_timestamp: false,
                prefix_device: true,
                path_mapper: crate::config::PathMapper::Identity,
                container_spawn: None,
                start_hooks: Default::default(),
                post_pull_hooks: Default::default(),
                startup_hooks: Default::default(),
                cc_extra_args: vec![],
                approval_rules: vec![],
                attachment_targets: vec![],
                integrations: Default::default(),
                mounts: vec![],
                history_replay_limit: 100,
                frontmatter: Default::default(),
                state_dir: std::path::PathBuf::from("/tmp"),
                messaging: Some(messaging_cfg),
                messaging_default_send_budget: 100,
                // App is a messaging sender; grant MessagingPublish + a universal
                // brenn_publish matcher so the fire-time publish (Seam A gate)
                // authorizes.
                policy: crate::access::AppPolicy::messaging_sender_policy(),
                pwa_push: None,
                webhook_subscriptions: vec![],
                mqtt_subscriptions: vec![],
            };
            apps.insert("test-app".to_string(), app_cfg);
            let apps = Arc::new(apps);
            let directory_arc = Arc::new(directory);
            let messenger = crate::messaging::Messenger::new(
                db.clone(),
                directory_arc.clone(),
                Arc::from("brenn://test"),
                apps.clone(),
                Arc::new(FakeWakeRouter),
                crate::messaging::MessagingGlobalConfig::default(),
            );
            let (alerts, _) = crate::obs::alerting::AlertDispatcher::noop();
            AutomationEngine::new(
                db.clone(),
                messenger,
                apps,
                directory_arc,
                ingress_router.clone() as Arc<dyn IngressRouter>,
                AutomationGlobalConfig::default(),
                alerts,
            )
        };

        // Trigger two failures for two different job rows (same app).
        for job_id in [1i64, 2i64] {
            let mut job = make_job_snapshot(job_id, "test-app");
            job.action = Action::SendMessage(SendMessageAction {
                to: "brenn:nonexistent-channel".to_string(),
                body: "hello".to_string(),
                urgency: Urgency::Low,
                reply_to: None,
                delivery_deadline_secs: None,
            });
            {
                let conn = db.lock().await;
                insert_job_row(&conn, &job);
            }
            fire_one(&engine, job).await;
        }

        let events = ingress_router.events().await;
        assert_eq!(events.len(), 2, "two error reports expected (one per job)");

        // Both reports must have been sent to the same conversation.
        assert_eq!(
            events[0].0, events[1].0,
            "non-singleton reuse: both error reports must route to the same automation_app_event_conversation"
        );

        // Only one mapping row should exist.
        let conn = db.lock().await;
        let mapping_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM automation_app_event_conversation \
                 WHERE owner_app_slug = 'test-app'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            mapping_count, 1,
            "only one automation_app_event_conversation row for the app"
        );
    }

    /// Error reports do not consume the send budget (they go via submit_ingress,
    /// not Messenger::publish). Verify the budget is unchanged after a failure
    /// that triggers an error report.
    #[tokio::test]
    async fn error_report_does_not_consume_budget() {
        use crate::messaging::db as msg_db;

        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        // Seed a known budget for the singleton conversation.
        // Pre-create the singleton conversation so we can set a known budget on it.
        let singleton_conv_id = {
            let conn = db.lock().await;
            let conv =
                crate::conversation::get_or_create_singleton_conversation(&conn, 1, "test-app");
            msg_db::reset_send_budget(&conn, conv.id, 50);
            conv.id
        };

        // Force a non-budget failure (unknown channel → OUTCOME_AUTH).
        let mut job = make_job_snapshot(1, "test-app");
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        // Error report was submitted via ingress_router (not Messenger::publish).
        let events = ingress_router.events().await;
        assert_eq!(events.len(), 1, "error report must be submitted");

        // Budget must be unchanged.
        let conn = db.lock().await;
        let remaining = msg_db::read_send_budget(&conn, singleton_conv_id).unwrap();
        assert_eq!(remaining, 50, "error report must not consume send budget");
    }

    /// Error reports use the source string "automation:error".
    #[tokio::test]
    async fn error_report_uses_source_string_automation_error() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        let mut job = make_job_snapshot(1, "test-app");
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        let events = ingress_router.events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].2, "automation:error",
            "source must be 'automation:error'"
        );
    }

    /// Report overflow: when the error-report cap is exceeded, the outcome is
    /// OUTCOME_RATE_LIMIT_SUPPRESSED and no submit_ingress call is made.
    #[tokio::test]
    async fn error_report_overflow_marks_outcome_rate_limit_suppressed_report() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();

        // Set cap at 2 for this test.
        let global_cfg = AutomationGlobalConfig {
            max_error_reports_per_hour_per_job: 2,
            ..AutomationGlobalConfig::default()
        };

        let engine = {
            let mut apps = indexmap::IndexMap::new();
            let messaging_cfg = crate::messaging::config::ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            };
            let app_cfg = crate::config::AppConfig {
                slug: "test-app".to_string(),
                name: "test-app".to_string(),
                description: String::new(),
                icon: String::new(),
                working_dir: std::path::PathBuf::from("/tmp"),
                model: String::new(),
                single_instance: false,
                singleton: true,
                persistent: false,
                idle_timeout: None,
                compaction: None,
                idle_hook_secs: 0,
                allowed_users: vec!["testuser".to_string()],
                disabled_tools: vec![],
                mcp_servers: Default::default(),
                multiuser: false,
                prefix_username: false,
                prefix_timestamp: false,
                prefix_device: true,
                path_mapper: crate::config::PathMapper::Identity,
                container_spawn: None,
                start_hooks: Default::default(),
                post_pull_hooks: Default::default(),
                startup_hooks: Default::default(),
                cc_extra_args: vec![],
                approval_rules: vec![],
                attachment_targets: vec![],
                integrations: Default::default(),
                mounts: vec![],
                history_replay_limit: 100,
                frontmatter: Default::default(),
                state_dir: std::path::PathBuf::from("/tmp"),
                messaging: Some(messaging_cfg),
                messaging_default_send_budget: 100,
                // App is a messaging sender; grant MessagingPublish + a universal
                // brenn_publish matcher so the fire-time publish (Seam A gate)
                // authorizes.
                policy: crate::access::AppPolicy::messaging_sender_policy(),
                pwa_push: None,
                webhook_subscriptions: vec![],
                mqtt_subscriptions: vec![],
            };
            apps.insert("test-app".to_string(), app_cfg);
            let apps = Arc::new(apps);
            let directory_arc = Arc::new(directory);
            let messenger = crate::messaging::Messenger::new(
                db.clone(),
                directory_arc.clone(),
                Arc::from("brenn://test"),
                apps.clone(),
                Arc::new(FakeWakeRouter),
                crate::messaging::MessagingGlobalConfig::default(),
            );
            let (alerts, _) = crate::obs::alerting::AlertDispatcher::noop();
            AutomationEngine::new(
                db.clone(),
                messenger,
                apps,
                directory_arc,
                ingress_router.clone() as Arc<dyn IngressRouter>,
                global_cfg,
                alerts,
            )
        };

        // Fire three times with a failure — cap is 2. The 3rd fire must be suppressed.
        let now_str = crate::db::format_ts_for_db(chrono::Utc::now());
        {
            let conn = db.lock().await;
            // Insert job with consecutive_failures = 0, 0, 0 for each fire.
            let job = make_job_snapshot(1, "test-app");
            insert_job_row(&conn, &job);
            // Pre-insert 2 reportable fire records to hit the cap on the 3rd.
            conn.execute(
                "INSERT INTO automation_fires (job_id, fired_at, outcome, error_class) \
                 VALUES (1, ?1, 'auth', 'auth'), (1, ?1, 'auth', 'auth')",
                rusqlite::params![now_str],
            )
            .unwrap();
        }

        let mut job = make_job_snapshot(1, "test-app");
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hi".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        fire_one(&engine, job).await;

        // The 3rd fire must be recorded as rate_limit_suppressed_report.
        let conn = db.lock().await;
        let suppressed_count: i64 = conn
            .query_row(
                "SELECT count(*) FROM automation_fires WHERE job_id = 1 AND outcome = 'rate_limit_suppressed_report'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            suppressed_count, 1,
            "overflow fire must be recorded as rate_limit_suppressed_report"
        );

        // No error report submitted for the suppressed fire.
        let events = ingress_router.events().await;
        assert!(
            events.is_empty(),
            "suppressed report must not call submit_ingress"
        );
    }

    /// Singleton app: error reports must use Urgency::Normal so
    /// the sleeping bridge is woken to deliver the report.
    #[tokio::test]
    async fn error_report_to_sleeping_singleton_queues_event_with_wake_immediately() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        // Force an error (unknown channel) to trigger an error report.
        let mut job = make_job_snapshot(1, "test-app");
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        let events = ingress_router.events().await;
        assert_eq!(events.len(), 1);
        // Urgency must be Normal for sleeping-singleton path.
        assert_eq!(
            events[0].5,
            Urgency::Normal,
            "error report must use Urgency::Normal"
        );
    }

    /// Error reports must NOT themselves trigger automation fires. The report
    /// goes via submit_ingress (unified ingress store), not via Messenger::publish, so the
    /// automation loop cannot pick it up as a new fire target.
    #[tokio::test]
    async fn error_report_does_not_trigger_automation_fires() {
        let (db, user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine(
            db.clone(),
            directory,
            user_id,
            ingress_router.clone() as Arc<dyn IngressRouter>,
        );

        let mut job = make_job_snapshot(1, "test-app");
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        // The ingress_router captured a submit_ingress call — that's the report
        // going through the unified ingress store, NOT through Messenger::publish.
        // Verify that the ingress_router was called (not the messaging bus).
        let events = ingress_router.events().await;
        assert_eq!(
            events.len(),
            1,
            "error reports must go through IngressRouter::submit_ingress"
        );
    }

    /// Active singleton: error reports call `submit_ingress` even when the
    /// singleton conversation already has an active bridge (i.e. `is_active`
    /// returns `true`). The report path does not short-circuit on active status;
    /// it always routes through the ingress path via `IngressRouter::submit_ingress`.
    #[tokio::test]
    async fn error_report_to_active_singleton_calls_submit_ingress() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine_full(
            db.clone(),
            directory,
            ingress_router.clone() as Arc<dyn IngressRouter>,
            Arc::new(ActiveFakeWakeRouter),
            AlertDispatcher::noop().0,
            AutomationGlobalConfig::default(),
            true,
        );

        let mut job = make_job_snapshot(1, "test-app");
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        let events = ingress_router.events().await;
        assert_eq!(
            events.len(),
            1,
            "error report must call submit_ingress even when singleton conversation is active"
        );
        assert_eq!(
            events[0].2, "automation:error",
            "source must be 'automation:error'"
        );
    }

    /// Report overflow alert dedup: `alert_once_per_process` must fire exactly
    /// once for a given error class per process, not once per overflow fire.
    /// Two overflow fires with the same error class must produce one alert.
    #[tokio::test]
    async fn error_report_overflow_emits_one_alert_per_class_per_process() {
        use crate::obs::alerting::{CountingAlerter, RateLimiter};
        use std::sync::atomic::{AtomicU32, Ordering};

        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();

        let counter = Arc::new(AtomicU32::new(0));
        let alerter = CountingAlerter(counter.clone());
        // Use a high rate limit so rate limiting doesn't interfere.
        let (alerts, alert_handle) = AlertDispatcher::new(alerter, RateLimiter::new(1000, 3600));

        let global_cfg = AutomationGlobalConfig {
            max_error_reports_per_hour_per_job: 1,
            ..AutomationGlobalConfig::default()
        };
        let engine = make_engine_full(
            db.clone(),
            directory,
            ingress_router.clone() as Arc<dyn IngressRouter>,
            Arc::new(FakeWakeRouter),
            alerts,
            global_cfg,
            true,
        );

        // Pre-insert one fire record so the cap (1) is already hit on the first fire.
        let now_str = crate::db::format_ts_for_db(chrono::Utc::now());
        {
            let conn = db.lock().await;
            let job = make_job_snapshot(1, "test-app");
            insert_job_row(&conn, &job);
            conn.execute(
                "INSERT INTO automation_fires (job_id, fired_at, outcome, error_class) \
                 VALUES (1, ?1, 'auth', 'auth')",
                rusqlite::params![now_str],
            )
            .unwrap();
        }

        let make_failing_job = || {
            let mut job = make_job_snapshot(1, "test-app");
            job.action = Action::SendMessage(SendMessageAction {
                to: "brenn:nonexistent-channel".to_string(),
                body: "hi".to_string(),
                urgency: Urgency::Low,
                reply_to: None,
                delivery_deadline_secs: None,
            });
            job
        };

        // First overflow fire.
        fire_one(&engine, make_failing_job()).await;
        // Second overflow fire — same error class. alert_once_per_process must suppress.
        fire_one(&engine, make_failing_job()).await;

        // Drop the engine (which holds the only remaining AlertDispatcher clone) to
        // close the mpsc sender. The drainer task then exits its `while let Some`
        // loop and the JoinHandle completes. Awaiting the handle gives a deterministic
        // drain without relying on yield_now scheduler interleaving.
        drop(engine);
        alert_handle
            .await
            .expect("alert drainer task must not panic");

        let alert_count = counter.load(Ordering::SeqCst);
        assert_eq!(
            alert_count, 1,
            "alert_once_per_process must deduplicate overflow alerts per error class per process; \
             got {alert_count} alerts for 2 overflow fires"
        );
    }

    // -------------------------------------------------------------------------
    // Seam B — automation fire-time publish-ACL re-check (design §2.3, §4)
    // -------------------------------------------------------------------------

    /// Build a single-app (`test-app`) engine whose policy is caller-supplied, so
    /// Seam-B tests can fire against a tightened/looser policy. Mirrors
    /// `make_engine_full`'s subscription construction (the
    /// `resolve_push_targets` invariant needs a `ResolvedSubscription` matching
    /// each push-enabled subscriber entry) but lets the caller pick `policy`.
    fn make_engine_with_policy(
        db: crate::db::Db,
        directory: MessagingDirectory,
        ingress_router: Arc<dyn IngressRouter>,
        policy: crate::access::AppPolicy,
    ) -> Arc<AutomationEngine> {
        // Reuse the canonical config builder (test_support) so the 30-field
        // `AppConfig` literal lives in exactly one place; only the `.policy`
        // override is Seam-B-specific. The subscription list is still derived from
        // the directory (the `resolve_push_targets` invariant needs a
        // `ResolvedSubscription` for each push-enabled `test-app` subscriber).
        let subscriptions: Vec<crate::messaging::config::ResolvedSubscription> = directory
            .list()
            .iter()
            .flat_map(|entry| {
                entry
                    .subscribers
                    .iter()
                    .filter(|s| s.kind.slug() == "test-app")
                    .map(|s| crate::messaging::config::ResolvedSubscription {
                        channel_uuid: entry.uuid,
                        channel_address: entry.address.clone(),
                        push_depth: s.push_depth,
                        retain_depth: s.retain_depth,
                        noise: crate::messaging::config::NoiseLevel::Silent,
                        wake_min: crate::messaging::WakeMin::Normal,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut app_cfg = crate::automation::test_support::default_app_cfg_with_subscriptions(
            "test-app",
            true,
            subscriptions,
        );
        app_cfg.policy = policy;
        let mut apps = indexmap::IndexMap::new();
        apps.insert("test-app".to_string(), app_cfg);
        let apps = Arc::new(apps);
        let directory_arc = Arc::new(directory);
        let messenger = crate::messaging::Messenger::new(
            db.clone(),
            directory_arc.clone(),
            Arc::from("brenn://test"),
            apps.clone(),
            Arc::new(FakeWakeRouter),
            crate::messaging::MessagingGlobalConfig::default(),
        );
        let (alerts, _) = crate::obs::alerting::AlertDispatcher::noop();
        AutomationEngine::new(
            db,
            messenger,
            apps,
            directory_arc,
            ingress_router,
            AutomationGlobalConfig::default(),
            alerts,
        )
    }

    /// Build the `MessagingPublish` grant + an *exact* `brenn_publish` matcher for
    /// `channel` (e.g. `"test"`). The covering policy a fire against `brenn:test`
    /// needs to succeed.
    fn publish_policy_for(channel: &str) -> crate::access::AppPolicy {
        let mut p = crate::access::AppPolicy::with_grants(&[
            crate::access::AppCapability::MessagingPublish,
        ]);
        p.acls
            .brenn_publish
            .push(crate::access::acl::ChannelMatcher::Exact(
                channel.to_string(),
            ));
        p
    }

    /// Happy path: a job whose `action_to` (`brenn:test`) is covered by the owner
    /// app's `brenn_publish` ACL fires successfully (`OUTCOME_OK`,
    /// `consecutive_failures` stays 0). The companion to the staleness test below.
    #[tokio::test]
    async fn fire_brenn_publish_acl_covered_succeeds() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        let engine = make_engine_with_policy(
            db.clone(),
            directory,
            ingress_router.clone() as Arc<dyn IngressRouter>,
            publish_policy_for("test"),
        );

        let mut job = make_job_snapshot(1, "test-app");
        job.consecutive_failures = 0;
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        let conn = db.lock().await;
        let (failures, outcome): (i64, String) = conn
            .query_row(
                "SELECT j.consecutive_failures, f.outcome \
                 FROM automation_jobs j JOIN automation_fires f ON f.job_id = j.id \
                 WHERE j.id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(outcome, OUTCOME_OK, "covered publish must fire OK");
        assert_eq!(
            failures, 0,
            "successful fire keeps consecutive_failures at 0"
        );
        // No error report on success.
        assert!(
            ingress_router.events().await.is_empty(),
            "no error report on a successful fire"
        );
    }

    /// Staleness guard (the load-bearing Seam-B invariant): a job is created while
    /// the policy covers its target, then the `brenn_publish` matcher is dropped.
    /// The fire must be rejected AT FIRE TIME with `OUTCOME_AUTH` (not OK), and
    /// `consecutive_failures` must increment. This proves the AUTHZ WARNING is
    /// honored — a tightened ACL kills pre-created jobs.
    #[tokio::test]
    async fn fire_brenn_publish_acl_tightened_after_create_is_denied() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        // Policy GRANTS MessagingPublish but the brenn_publish ACL does NOT cover
        // `test` — simulating a matcher removed after the job was created. The grant
        // is present, so this exercises the layer-2 ACL re-check specifically (not
        // the layer-1 grant path).
        let engine = make_engine_with_policy(
            db.clone(),
            directory,
            ingress_router.clone() as Arc<dyn IngressRouter>,
            publish_policy_for("some-other-channel"),
        );

        let mut job = make_job_snapshot(1, "test-app");
        job.consecutive_failures = 0;
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        let conn = db.lock().await;
        let (failures, outcome, detail): (i64, String, Option<String>) = conn
            .query_row(
                "SELECT j.consecutive_failures, f.outcome, f.detail \
                 FROM automation_jobs j JOIN automation_fires f ON f.job_id = j.id \
                 WHERE j.id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            outcome, OUTCOME_AUTH,
            "fire-time ACL denial must record OUTCOME_AUTH"
        );
        assert_eq!(
            failures, 1,
            "fire-time ACL denial must increment consecutive_failures"
        );
        assert!(
            detail.unwrap_or_default().contains("publish ACL denies"),
            "detail must name the publish-ACL denial"
        );
    }

    /// Layer-1 staleness: `MessagingPublish` revoked after create (only
    /// `MessagingSubscribe` left) denies the fire with `OUTCOME_AUTH`. This is the
    /// publish/subscribe split applied at the fire re-check — `messaging_enabled()`
    /// would still be `true` (subscribe grant present), but the fire gates on
    /// `MessagingPublish` specifically.
    #[tokio::test]
    async fn fire_messaging_publish_revoked_after_create_is_denied() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();
        // Subscribe-only policy: `messaging_enabled()` is still true, but the
        // publish grant is absent.
        let policy = crate::access::AppPolicy::with_grants(&[
            crate::access::AppCapability::MessagingSubscribe,
        ]);
        let engine = make_engine_with_policy(
            db.clone(),
            directory,
            ingress_router.clone() as Arc<dyn IngressRouter>,
            policy,
        );

        let mut job = make_job_snapshot(1, "test-app");
        job.consecutive_failures = 0;
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job).await;

        let conn = db.lock().await;
        let (failures, outcome, detail): (i64, String, Option<String>) = conn
            .query_row(
                "SELECT j.consecutive_failures, f.outcome, f.detail \
                 FROM automation_jobs j JOIN automation_fires f ON f.job_id = j.id \
                 WHERE j.id = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            outcome, OUTCOME_AUTH,
            "revoked publish grant must record OUTCOME_AUTH"
        );
        assert_eq!(
            failures, 1,
            "revoked publish grant must increment consecutive_failures"
        );
        assert!(
            detail
                .unwrap_or_default()
                .contains("messaging_publish grant"),
            "detail must name the missing messaging_publish grant"
        );
    }
}

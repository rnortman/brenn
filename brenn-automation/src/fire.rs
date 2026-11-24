//! Per-fire logic: per-job rate-limit gate, auth re-check, per-app fire cap,
//! action dispatch, and bookkeeping (automation_fires insert +
//! automation_jobs UPDATE). A fire draws no conversation's send budget.
//!
//! Called sequentially by the background loop for each due job snapshot.

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};

use crate::AutomationEngine;
use crate::error_payload::AutomationErrorPayload;
use crate::job::{
    Action, JobSnapshot, MqttPublishAction, SendMessageAction, Trigger, compute_next,
};
use brenn_db::auth::user::get_user_by_username;
use brenn_db::conversation::{create_conversation, get_or_create_singleton_conversation};
use brenn_lib::messaging::Urgency;
use brenn_messaging::publish::{
    MqttPayload, MqttRefusal, PublishOrigin, PublishResult, ReplyToRefusal, StatusToRefusal,
};

use crate::schema::{
    APP_FIRE_KIND_CAP_REPORT, APP_FIRE_KIND_FIRE, OUTCOME_ACTION_ERROR, OUTCOME_APP_FIRE_CAP,
    OUTCOME_APP_GONE, OUTCOME_AUTH, OUTCOME_OK, OUTCOME_RATE_LIMIT, OUTCOME_RATE_LIMIT_SUPPRESSED,
};

/// The per-app fire cap's window: a sliding 24 hours before the fire.
const APP_FIRE_CAP_WINDOW_SECS: i64 = 86_400;

/// Execute one scheduled fire for `job`. Sequenced by the automation loop.
/// Every path ends in `finish_fire`, which writes the fire record and
/// updates the job row.
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

    // Staleness guard: a job stores `action.to` at create time and fires
    // later, so a grant revoked or an ACL tightened after job creation must
    // be honored here. For a `send_message` action, layer-1 checks the
    // `MessagingPublish` grant specifically (the publish/subscribe split);
    // layer-2 (per-channel `brenn_publish` ACL) is re-checked in
    // `fire_send_message`. `Messenger::publish` re-checks the same policy as
    // defense-in-depth, but this explicit re-check yields a fire-specific
    // audit detail. An `mqtt_publish` action's grants are the MQTT
    // admission's to decide, at the publish.
    if matches!(job.action, Action::SendMessage(_))
        && !app_config
            .policy
            .has_grant(brenn_envelope::grants::AppCapability::MessagingPublish)
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

    let owner_user_present = {
        let conn = engine.db.lock().await;
        get_user_by_username(&conn, &owner_username).is_some()
    };
    if !owner_user_present {
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

    // (d) Per-app fire cap, immediately before the action: a fire that passes
    // it is counted whatever the action then answers.
    let gate = {
        let conn = engine.db.lock().await;
        app_fire_gate(
            &conn,
            &job.owner_app_slug,
            fire_ts,
            engine.defaults.max_fires_per_day_per_app,
        )
    };
    if let AppFireGate::Refuse { report, count } = gate {
        let detail = format!(
            "app fire cap: {count} fires of app {:?} in last 24 hours (cap={})",
            job.owner_app_slug, engine.defaults.max_fires_per_day_per_app
        );
        record_fire(
            engine,
            &job,
            fire_ts,
            FireClass::AppCapRefused { report, count },
            OUTCOME_APP_FIRE_CAP,
            Some(OUTCOME_APP_FIRE_CAP),
            Some(&detail),
        )
        .await;
        return;
    }

    // A fire is published under the owner app and draws no conversation's
    // send budget; the engine's own caps bound it.
    let origin = PublishOrigin::AutomationFire;
    let (stored_outcome, error_class, detail) = match &job.action {
        Action::SendMessage(sma) => {
            fire_send_message(engine, &job, &app_config.policy, origin, fire_ts, sma).await
        }
        Action::MqttPublish(mpa) => fire_mqtt_publish(engine, &job, origin, mpa).await,
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

/// The per-app fire cap's answer for one fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppFireGate {
    /// Under the cap; the fire's ledger row is written.
    Admit,
    /// Over the cap with `count` fires in the window; `report` is true for
    /// the app's first refusal in the window and false once one was reported.
    Refuse { report: bool, count: i64 },
}

/// Decide one fire of `owner_app_slug` against the per-app cap, in one
/// transaction: prune ledger rows of every app older than the window, count
/// the app's `fire` rows left, and admit (writing a `fire` row) under `cap`.
/// Over it, refuse, writing a `cap_report` row when the app has none in the
/// window. Rows stamped after `fire_ts` still count.
fn app_fire_gate(
    conn: &Connection,
    owner_app_slug: &str,
    fire_ts: DateTime<Utc>,
    cap: u32,
) -> AppFireGate {
    let fire_ts_str = brenn_db::format_ts_for_db(fire_ts);
    let window_start =
        brenn_db::format_ts_for_db(fire_ts - chrono::Duration::seconds(APP_FIRE_CAP_WINDOW_SECS));
    let count_kind = |kind: &str| -> i64 {
        conn.query_row(
            "SELECT count(*) FROM automation_app_fires \
             WHERE owner_app_slug = ?1 AND kind = ?2",
            rusqlite::params![owner_app_slug, kind],
            |row| row.get(0),
        )
        .expect("app_fire_gate: count ledger rows")
    };
    let insert_kind = |kind: &str| {
        conn.execute(
            "INSERT INTO automation_app_fires (owner_app_slug, kind, at) VALUES (?1, ?2, ?3)",
            rusqlite::params![owner_app_slug, kind, fire_ts_str],
        )
        .expect("app_fire_gate: insert ledger row");
    };

    conn.execute("BEGIN", []).expect("begin app_fire_gate txn");
    conn.execute(
        "DELETE FROM automation_app_fires WHERE at < ?1",
        rusqlite::params![window_start],
    )
    .expect("app_fire_gate: prune ledger");
    let count = count_kind(APP_FIRE_KIND_FIRE);
    let gate = if count < i64::from(cap) {
        insert_kind(APP_FIRE_KIND_FIRE);
        AppFireGate::Admit
    } else if count_kind(APP_FIRE_KIND_CAP_REPORT) == 0 {
        insert_kind(APP_FIRE_KIND_CAP_REPORT);
        AppFireGate::Refuse {
            report: true,
            count,
        }
    } else {
        AppFireGate::Refuse {
            report: false,
            count,
        }
    };
    conn.execute("COMMIT", [])
        .expect("commit app_fire_gate txn");
    gate
}

/// How a recorded fire bears on its job: a success resets the failure count,
/// a failure raises it and may disable the job, and a refusal by the per-app
/// cap leaves it as it was and is reported only when the gate said so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FireClass {
    Success,
    Failure,
    AppCapRefused { report: bool, count: i64 },
}

/// A fire's stored outcome, its error class, and the detail naming its cause.
type FireOutcome = (&'static str, Option<&'static str>, Option<String>);

fn auth_failure(detail: String) -> FireOutcome {
    (OUTCOME_AUTH, Some(OUTCOME_AUTH), Some(detail))
}

fn action_error(detail: String) -> FireOutcome {
    (
        OUTCOME_ACTION_ERROR,
        Some(OUTCOME_ACTION_ERROR),
        Some(detail),
    )
}

/// Fire a `send_message` action: re-check the destination against the
/// directory and the owner's `brenn:` publish ACL, then publish.
async fn fire_send_message(
    engine: &AutomationEngine,
    job: &JobSnapshot,
    policy: &brenn_lib::access::AppPolicy,
    origin: PublishOrigin,
    fire_ts: DateTime<Utc>,
    sma: &SendMessageAction,
) -> FireOutcome {
    let action_to = &sma.to;
    if engine.directory.resolve(action_to).is_none() {
        return auth_failure(format!(
            "destination address {action_to:?} not found in directory"
        ));
    }

    // Layer-2 `brenn_publish` ACL re-check against `action_to`.
    // `SendMessageAction.to` is always a `brenn:<name>` address (validated
    // at create), so the prefix strip is infallible. A missing prefix is an
    // internal invariant violation — panic rather than silently misclassify
    // it as an ACL denial.
    let publish_channel = action_to
        .strip_prefix(brenn_lib::messaging::BRENN_ADDRESS_PREFIX)
        .unwrap_or_else(|| {
            panic!(
                "fire_one: automation action.to {action_to:?} missing brenn: prefix \
                 — invariant violated at create time; job uuid={}",
                job.uuid
            )
        });
    if !policy.allows_brenn_publish(publish_channel) {
        return auth_failure(format!(
            "owner app {:?} publish ACL denies {action_to:?}",
            job.owner_app_slug
        ));
    }

    let delivery_deadline = sma
        .delivery_deadline_secs
        .map(|secs| fire_ts + chrono::Duration::seconds(secs as i64));

    let publish_result = engine
        .messenger
        .publish(
            origin,
            &job.owner_app_slug,
            action_to,
            sma.body.as_str().into(),
            sma.urgency,
            sma.reply_to.as_deref(),
            None, // no deliver_after on automation fires
            delivery_deadline,
        )
        .await;

    match publish_result {
        PublishResult::Ok { .. } => (OUTCOME_OK, None, None),
        PublishResult::UnknownChannel(addr) => auth_failure(format!("unknown channel: {addr}")),
        PublishResult::MalformedAddress(addr) => auth_failure(format!("malformed address: {addr}")),
        PublishResult::MissingSender => auth_failure("missing sender".to_string()),
        // The explicit `to` re-check above short-circuits before
        // `Messenger::publish`, so this arm is only reachable for `reply_to`:
        // a job whose `reply_to` was in scope at create time but whose owner
        // ACLs were later tightened. `addr` is therefore the `reply_to`
        // address; the detail names it so the operator is not pointed at the
        // publish target.
        PublishResult::AclDenied(addr) => {
            auth_failure(format!("publish ACL denied (reply_to): {addr}"))
        }
        PublishResult::BodyTooLarge { len, max } => {
            action_error(format!("body too large: {len} > {max}"))
        }
        PublishResult::RateLimited => action_error("send rate limit exceeded".to_string()),
        PublishResult::DeferredQuotaExceeded { cap } => {
            action_error(format!("deferred-message quota exceeded (cap {cap})"))
        }
        PublishResult::BudgetExhausted => unreachable!(
            "automation fire publish returned BudgetExhausted: an AutomationFire origin draws \
             no send budget; job uuid={}",
            job.uuid
        ),
        // A job's publish passes no impetus — unreachable unless the
        // publish construction changes.
        PublishResult::ImpetusUnauthorized => unreachable!(
            "automation fire publish returned ImpetusUnauthorized: a job's publish passes no \
             impetus; job uuid={}",
            job.uuid
        ),
        // Only the MQTT entry runs the MQTT admission, and a `brenn:` publish
        // does not go through it.
        PublishResult::MqttRefused(refusal) => unreachable!(
            "automation fire publish returned MqttRefused({refusal:?}): only \
             Messenger::publish_mqtt runs the MQTT admission; job uuid={}",
            job.uuid
        ),
    }
}

/// Fire an `mqtt_publish` action: one publish through the MQTT admission
/// under the owner app, which decides against the current policy and client
/// table. A fire states no release time, so the lifetime counts from the
/// commit. Every answer a configuration
/// change or a stored row can cause is a recorded outcome; only answers the
/// call's own construction excludes panic.
async fn fire_mqtt_publish(
    engine: &AutomationEngine,
    job: &JobSnapshot,
    origin: PublishOrigin,
    mpa: &MqttPublishAction,
) -> FireOutcome {
    let to = &mpa.to;
    let result = engine
        .messenger
        .publish_mqtt(
            origin,
            &job.owner_app_slug,
            to,
            MqttPayload::Inline(mpa.payload.as_bytes().to_vec()),
            &mpa.options(),
            None,
        )
        .await;
    let refused = match result {
        Ok(_) => return (OUTCOME_OK, None, None),
        Err(refused) => refused,
    };
    let owner = &job.owner_app_slug;
    match refused {
        PublishResult::MissingSender => {
            auth_failure(format!("owner app {owner:?} holds no mqtt_publish grant"))
        }
        PublishResult::AclDenied(addr) => auth_failure(format!(
            "owner app {owner:?} MQTT publish ACL denies {addr:?}"
        )),
        PublishResult::UnknownChannel(addr) => auth_failure(format!(
            "MQTT client of {addr:?} is not configured (or was removed)"
        )),
        PublishResult::MalformedAddress(addr) => {
            action_error(format!("malformed MQTT address: {addr:?}"))
        }
        PublishResult::RateLimited => action_error("send rate limit exceeded".to_string()),
        PublishResult::MqttRefused(refusal) => mqtt_refusal_outcome(job, refusal),
        PublishResult::BudgetExhausted => unreachable!(
            "automation MQTT fire got BudgetExhausted: an AutomationFire origin draws no send \
             budget; job uuid={}",
            job.uuid
        ),
        other @ (PublishResult::Ok { .. }
        | PublishResult::BodyTooLarge { .. }
        | PublishResult::DeferredQuotaExceeded { .. }
        | PublishResult::ImpetusUnauthorized) => unreachable!(
            "automation MQTT fire got {other:?}: its publish carries an inline payload and no \
             release time, deadline or impetus; job uuid={}",
            job.uuid
        ),
    }
}

/// The outcome of a fire the MQTT admission refused on its options or
/// payload.
fn mqtt_refusal_outcome(job: &JobSnapshot, refusal: MqttRefusal) -> FireOutcome {
    match refusal {
        MqttRefusal::StatusTo { address, why } => match why {
            StatusToRefusal::GrantAbsent => auth_failure(format!(
                "status_to {address:?}: owner app {:?} holds no messaging_publish grant",
                job.owner_app_slug
            )),
            StatusToRefusal::AclDenied => auth_failure(format!(
                "status_to {address:?}: owner app {:?} publish ACL denies it",
                job.owner_app_slug
            )),
            StatusToRefusal::Unknown => {
                auth_failure(format!("status_to {address:?}: unknown channel"))
            }
            StatusToRefusal::Malformed => {
                action_error(format!("status_to {address:?}: malformed address"))
            }
            StatusToRefusal::StatusOverBodyCap { len, max } => action_error(format!(
                "status_to {address:?}: a delivery status of {len} bytes is over the body cap \
                 {max}"
            )),
        },
        MqttRefusal::ReplyTo { address, why } => match why {
            ReplyToRefusal::AclDenied => auth_failure(format!(
                "reply_to {address:?}: owner app {:?} MQTT publish ACL denies it",
                job.owner_app_slug
            )),
            ReplyToRefusal::Malformed { detail } => {
                action_error(format!("reply_to {address:?}: {detail}"))
            }
            ReplyToRefusal::OtherClient => action_error(format!(
                "reply_to {address:?}: not on the client the message is published through"
            )),
        },
        MqttRefusal::InvalidQos { qos } => action_error(format!("invalid QoS {qos}")),
        MqttRefusal::InvalidContentType { reason } => {
            action_error(format!("invalid content type: {reason}"))
        }
        MqttRefusal::PayloadOverClientCap { len, cap } => action_error(format!(
            "payload of {len} bytes is over the client's cap of {cap}"
        )),
        MqttRefusal::NeedsBlobStore { len, max_body } => action_error(format!(
            "payload of {len} bytes is over the body cap {max_body} and no blob store is \
             configured"
        )),
        MqttRefusal::BlobRefused { reason } => {
            action_error(format!("payload blob refused: {reason}"))
        }
        other @ (MqttRefusal::DeliveryDeadline | MqttRefusal::PayloadShape) => unreachable!(
            "automation MQTT fire refused with {other:?}: its publish carries an inline payload \
             and no deadline; job uuid={}",
            job.uuid
        ),
    }
}

/// [`record_fire`] for a fire that reached no per-app cap refusal: `ok` is a
/// success and every other outcome a failure.
async fn finish_fire(
    engine: &AutomationEngine,
    job: &JobSnapshot,
    fire_ts: DateTime<Utc>,
    raw_outcome: &str,
    error_class: Option<&str>,
    detail: Option<&str>,
) {
    let class = if raw_outcome == OUTCOME_OK {
        FireClass::Success
    } else {
        FireClass::Failure
    };
    record_fire(
        engine,
        job,
        fire_ts,
        class,
        raw_outcome,
        error_class,
        detail,
    )
    .await;
}

/// Write the fire record and update the job row. Handles report-rate-limit
/// suppression, consecutive-failure tracking, auto-disable, and alerting.
/// Panics unless `class` is `Success` exactly when `raw_outcome` is `ok` and
/// `AppCapRefused` exactly when it is `app_fire_cap`.
async fn record_fire(
    engine: &AutomationEngine,
    job: &JobSnapshot,
    fire_ts: DateTime<Utc>,
    class: FireClass,
    raw_outcome: &str,
    error_class: Option<&str>,
    detail: Option<&str>,
) {
    assert_eq!(
        (
            class == FireClass::Success,
            matches!(class, FireClass::AppCapRefused { .. })
        ),
        (
            raw_outcome == OUTCOME_OK,
            raw_outcome == OUTCOME_APP_FIRE_CAP
        ),
        "record_fire: class {class:?} disagrees with outcome {raw_outcome:?}"
    );
    let now = Utc::now();
    let job_id = job.row_id;
    let is_success = class == FireClass::Success;

    // Compute next fire time from the trigger.
    let next_fire_at = match compute_next(job.trigger.cron_trigger(), fire_ts) {
        Some(dt) => dt,
        None => {
            // Cron expression now unsatisfiable (e.g. tz rule change).
            handle_unsatisfiable_cron(engine, job, fire_ts, raw_outcome, error_class, detail).await;
            return;
        }
    };

    // Determine stored outcome (may be overridden by report-rate-limit
    // suppression). A cap refusal is reported under the per-app rule the gate
    // applied, never under the per-job report limit.
    let (stored_outcome, emit_report) = if is_success {
        (OUTCOME_OK, false)
    } else if let FireClass::AppCapRefused { report, .. } = class {
        (raw_outcome, report)
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
                brenn_obs::alerting::AlertSeverity::Warning,
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
    let new_consecutive_failures = match class {
        FireClass::Success => 0i64,
        FireClass::Failure => job.consecutive_failures + 1,
        FireClass::AppCapRefused { .. } => job.consecutive_failures,
    };

    let should_auto_disable = class == FireClass::Failure
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
            brenn_obs::alerting::AlertSeverity::Warning,
            "Automation job auto-disabled".to_string(),
            &format!("automation_disable:{job_id}"),
            format!(
                "job_id={job_id} name={:?} consecutive_failures={new_consecutive_failures} \
                 threshold={}",
                job.name, engine.defaults.consecutive_failures_to_disable
            ),
        );
    }

    if let FireClass::AppCapRefused { count, .. } = class {
        engine.alerts.alert_once_per_process(
            brenn_obs::alerting::AlertSeverity::Warning,
            "Automation app fire cap reached".to_string(),
            &format!("automation_app_fire_cap:{}", job.owner_app_slug),
            format!(
                "app={:?} fires_in_last_24h={count} cap={}; further fires of the app are \
                 skipped until the window has room",
                job.owner_app_slug, engine.defaults.max_fires_per_day_per_app
            ),
        );
    }

    if emit_report {
        let ec = error_class.unwrap_or("unknown");
        let fire_time_str = brenn_db::format_ts_for_db(fire_ts);
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
            rusqlite::params![brenn_db::format_ts_for_db(now), job_id],
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
                brenn_db::format_ts_for_db(fire_ts),
                fire_outcome,
                error_class.unwrap_or("action_error"),
                "unsatisfiable cron",
            ],
        )
        .expect("insert unsatisfiable fire record");
    }

    engine.alerts.alert_once_per_process(
        brenn_obs::alerting::AlertSeverity::Warning,
        "Automation job unsatisfiable cron".to_string(),
        &format!("automation_unsatisfiable:{job_id}"),
        format!(
            "job_id={job_id} name={:?} compute_next returned None; job auto-disabled",
            job.name
        ),
    );

    let fire_time_str = brenn_db::format_ts_for_db(fire_ts);
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
    engine
        .messenger
        .provision_conversation_chat_channels(&conn, &job.owner_app_slug, conv_id);
    drop(conn);
    engine
        .messenger
        .republish_chat_roster(&job.owner_app_slug)
        .await;
    Some(conv_id)
}

/// Return (or create) the per-app automation events conversation for a
/// non-singleton app. Uses the `automation_app_event_conversation` mapping
/// table.
///
/// Stale-user rebinding is handled at startup by
/// `startup::run_startup_consistency_checks`.
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

    let conv_id = create_conversation(conn, owner_user_id, owner_app_slug, false);
    conn.execute(
        "INSERT INTO automation_app_event_conversation (owner_app_slug, conversation_id) \
         VALUES (?1, ?2)",
        rusqlite::params![owner_app_slug, conv_id],
    )
    .expect("insert automation_app_event_conversation");
    conv_id
}

/// Count fires for `job_id` in the last `window_secs` seconds where the
/// outcome is NOT 'rate_limit' or 'app_fire_cap' (the fire rate-limit gate
/// excludes prior drops and per-app cap refusals).
fn count_fires_in_window(
    conn: &Connection,
    job_id: i64,
    now: DateTime<Utc>,
    window_secs: i64,
) -> i64 {
    let window_start = now - chrono::Duration::seconds(window_secs);
    conn.query_row(
        "SELECT count(*) FROM automation_fires \
         WHERE job_id = ?1 AND fired_at >= ?2 AND outcome NOT IN (?3, ?4)",
        rusqlite::params![
            job_id,
            brenn_db::format_ts_for_db(window_start),
            OUTCOME_RATE_LIMIT,
            OUTCOME_APP_FIRE_CAP
        ],
        |row| row.get(0),
    )
    .expect("count_fires_in_window")
}

/// Count fires for `job_id` in the last `window_secs` seconds that are NOT
/// `ok`, `rate_limit_suppressed_report` or `app_fire_cap` (the report
/// rate-limit gate; a cap refusal is reported under the per-app rule).
fn count_reportable_fires_in_window(
    conn: &Connection,
    job_id: i64,
    now: DateTime<Utc>,
    window_secs: i64,
) -> i64 {
    let window_start = now - chrono::Duration::seconds(window_secs);
    conn.query_row(
        "SELECT count(*) FROM automation_fires \
         WHERE job_id = ?1 AND fired_at >= ?2 AND outcome NOT IN (?3, ?4, ?5)",
        rusqlite::params![
            job_id,
            brenn_db::format_ts_for_db(window_start),
            OUTCOME_OK,
            OUTCOME_RATE_LIMIT_SUPPRESSED,
            OUTCOME_APP_FIRE_CAP
        ],
        |row| row.get(0),
    )
    .expect("count_reportable_fires_in_window")
}

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
    let fire_ts_str = brenn_db::format_ts_for_db(fire_ts);
    let now_str = brenn_db::format_ts_for_db(now);
    let next_str = brenn_db::format_ts_for_db(next_fire_at);

    // Wrap the UPDATE + INSERT in an explicit transaction so a panic between
    // statements cannot leave next_fire_at advanced without a corresponding
    // automation_fires row. The prune runs outside the transaction — its
    // failure would not corrupt correctness.
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
        .expect("update job (failure or cap refusal)")
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
    let prune_before = brenn_db::format_ts_for_db(now - chrono::Duration::hours(24));
    conn.execute(
        "DELETE FROM automation_fires WHERE job_id = ?1 AND fired_at < ?2",
        rusqlite::params![job_id, prune_before],
    )
    .expect("prune automation_fires");

    true
}

impl Trigger {
    pub(crate) fn cron_trigger(&self) -> &crate::job::CronTrigger {
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
    use crate::AutomationGlobalConfig;
    use crate::IngressRouter;
    use crate::job::{Action, CronTrigger, SendMessageAction, Trigger};
    use crate::test_support::init_db_memory;
    use crate::test_support::{
        FakeIngressRouter, FakeWakeRouter, app_subscriptions, default_app_cfg_with_subscriptions,
        make_engine_full, make_engine_on_the_bus, roster_snapshots,
    };
    use brenn_lib::messaging::{
        ChannelEntry, ChannelScheme, MessageEnvelope, MessagingDirectory, NoiseLevel,
        ResolvedChannel, SubscriberEntry, SubscriberEntryKind, Urgency, WakeMin, canonical_address,
    };
    use brenn_messaging::{
        WakeRouter,
        config::{Depth, Sink},
    };
    use brenn_obs::alerting::AlertDispatcher;

    /// `WakeRouter` stub that reports every conversation as active.
    struct ActiveFakeWakeRouter;

    #[async_trait::async_trait]
    impl WakeRouter for ActiveFakeWakeRouter {
        async fn deliver(
            &self,
            _key: &brenn_lib::messaging::SubscriberEntryKind,
            _envelope: &std::sync::Arc<MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            Ok(true)
        }
        async fn deliver_ingress(
            &self,
            _key: &brenn_lib::messaging::SubscriberEntryKind,
            _: &brenn_lib::messaging::ParticipantId,
            _event: &brenn_messaging_store::ingress::Event,
        ) -> Result<bool, String> {
            Ok(true)
        }
        fn spawn_eager_wake(
            &self,
            _key: &brenn_lib::messaging::SubscriberEntryKind,
            _: &brenn_lib::messaging::ParticipantId,
        ) {
        }
        fn delivery_shape(
            &self,
            key: &brenn_lib::messaging::SubscriberEntryKind,
        ) -> brenn_messaging::DeliveryShape {
            brenn_messaging::default_delivery_shape(key)
        }
        fn alarm(
            &self,
            _channel: &str,
            _subscriber: &brenn_lib::messaging::ParticipantId,
            _count: u64,
        ) {
        }
    }

    async fn make_db_with_user_and_channel() -> (
        brenn_db::Db,
        i64,
        Uuid,
        brenn_lib::messaging::MessagingDirectory,
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
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
                non_text: false,
                untyped_content_type: None,
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
        brenn_messaging_store::db::upsert_channels(&conn, std::slice::from_ref(&channel_entry));

        drop(conn);

        let directory = MessagingDirectory::with_entries(vec![channel_entry]);
        (db, 1i64, channel_uuid, directory)
    }

    fn make_engine(
        db: brenn_db::Db,
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

    /// [`make_engine`]'s bus-aware twin: the same fixture channel, plus the app's
    /// roster channel and its reserved writer, and "test-app" as a *non*-singleton
    /// so the conversation a fire creates is the automation-events one.
    async fn make_engine_announcing(
        db: brenn_db::Db,
        directory: MessagingDirectory,
    ) -> Arc<AutomationEngine> {
        let entries: Vec<ChannelEntry> = directory
            .list()
            .iter()
            .map(|entry| (**entry).clone())
            .collect();
        let mut apps = indexmap::IndexMap::new();
        apps.insert(
            "test-app".to_string(),
            default_app_cfg_with_subscriptions("test-app", false, app_subscriptions(&directory)),
        );
        make_engine_on_the_bus(
            db,
            Arc::new(apps),
            entries,
            FakeIngressRouter::new() as Arc<dyn IngressRouter>,
        )
        .await
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
        let now = brenn_db::format_ts_for_db(chrono::Utc::now());
        conn.execute(
            "INSERT INTO automation_jobs \
             (id, uuid, owner_app_slug, name, trigger_kind, trigger_payload, \
              action_payload, enabled, consecutive_failures, \
              created_at, updated_at, next_fire_at) \
             VALUES (?1, ?2, ?3, ?4, 'cron', ?5, ?6, 1, ?7, ?8, ?8, ?8)",
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

    /// A fire publishes under no conversation, so a successful one creates
    /// none and announces nothing. The error-report path is the creation site:
    /// the first failing fire of an app that is not a singleton mints the
    /// app's automation-events conversation, so it owes the bus a roster
    /// snapshot naming it. Nobody watches this path — the conversation is
    /// created by machinery, not a person — so an announcement that stopped
    /// happening would leave a fleet driver blind to it with nothing to notice.
    ///
    /// The second failing fire finds the conversation already there and
    /// announces nothing: provisioning is idempotent and runs on *every*
    /// report, so a republish per report would wake every subscribed peer at
    /// automation cadence to tell it nothing.
    #[tokio::test]
    async fn a_failing_fire_announces_the_events_conversation_it_creates_exactly_once() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let engine = make_engine_announcing(db.clone(), directory).await;

        let job = make_job_snapshot(1, "test-app");
        {
            let conn = db.lock().await;
            insert_job_row(&conn, &job);
        }

        fire_one(&engine, job.clone()).await;
        assert_eq!(last_fire(&engine).await.0, OUTCOME_OK);
        let conversations = |db: brenn_db::Db| async move {
            db.lock()
                .await
                .query_row(
                    "SELECT count(*) FROM conversations WHERE app_slug = 'test-app'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
        };
        assert_eq!(
            conversations(db.clone()).await,
            0,
            "a successful fire creates no conversation"
        );
        assert!(
            roster_snapshots(&engine, "test-app").await.is_empty(),
            "a successful fire announces nothing"
        );

        let mut failing = job;
        failing.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        fire_one(&engine, failing.clone()).await;

        let conv_id = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT id FROM conversations WHERE app_slug = 'test-app'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("the report created the automation-events conversation")
        };
        assert_eq!(
            roster_snapshots(&engine, "test-app").await,
            vec![format!(r#"{{"v":1,"conversations":[{{"id":{conv_id}}}]}}"#)],
            "the created conversation was announced"
        );

        fire_one(&engine, failing).await;
        assert_eq!(
            roster_snapshots(&engine, "test-app").await.len(),
            1,
            "a report that creates nothing announces nothing"
        );
    }

    /// The error-report path creates the same conversation when a fire fails
    /// before one exists, so it carries the same obligation — and it is the path
    /// that runs when something is already going wrong.
    #[tokio::test]
    async fn resolving_a_report_conversation_announces_the_one_it_creates() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let engine = make_engine_announcing(db.clone(), directory).await;
        let job = make_job_snapshot(1, "test-app");

        let conv_id = resolve_report_conversation(&engine, &job)
            .await
            .expect("the owner app and user resolve");

        assert_eq!(
            roster_snapshots(&engine, "test-app").await,
            vec![format!(r#"{{"v":1,"conversations":[{{"id":{conv_id}}}]}}"#)],
            "the report conversation is on the bus like any other"
        );
        assert_eq!(
            resolve_report_conversation(&engine, &job).await,
            Some(conv_id),
            "the second call finds it"
        );
        assert_eq!(
            roster_snapshots(&engine, "test-app").await.len(),
            1,
            "and announces nothing"
        );
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
            let now = brenn_db::format_ts_for_db(chrono::Utc::now());
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

    /// An empty send budget on the singleton conversation does not stop a
    /// fire of either action kind: it is `ok`, reports nothing, and leaves the
    /// budget at zero.
    #[tokio::test]
    async fn fire_with_an_empty_send_budget_succeeds() {
        for action in [
            send_message_to_status(),
            Action::MqttPublish(mqtt_action(KITCHEN, b"ON")),
        ] {
            let ingress = FakeIngressRouter::new();
            let (engine, _) = make_mqtt_engine(
                mqtt_sender_policy(),
                ingress.clone() as Arc<dyn IngressRouter>,
            )
            .await;
            let conv = singleton_conversation(&engine).await;
            brenn_messaging_store::db::reset_send_budget(&*engine.db.lock().await, conv, 0);
            let job = JobSnapshot {
                action,
                ..make_job_snapshot(1, "test-app")
            };
            insert_job_row(&*engine.db.lock().await, &job);

            fire_one(&engine, job.clone()).await;

            assert_eq!(
                last_fire(&engine).await,
                (OUTCOME_OK.to_string(), String::new(), 0),
                "{:?}",
                job.action
            );
            assert!(ingress.events().await.is_empty(), "{:?}", job.action);
            assert_eq!(
                send_budget(&engine, conv).await,
                Some(0),
                "{:?}",
                job.action
            );
        }
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
                urgency: brenn_lib::messaging::Urgency::Low,
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
            let messaging_cfg = brenn_lib::messaging::config::ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            };
            let app_cfg = brenn_lib::config::AppConfig {
                working_dir: std::path::PathBuf::from("/tmp"),
                model: String::new(),
                allowed_users: vec!["testuser".to_string()],
                mcp_servers: Default::default(),
                start_hooks: Default::default(),
                post_pull_hooks: Default::default(),
                startup_hooks: Default::default(),
                integrations: Default::default(),
                history_replay_limit: 100,
                frontmatter: Default::default(),
                state_dir: std::path::PathBuf::from("/tmp"),
                messaging: Some(messaging_cfg),
                policy: std::sync::Arc::new(brenn_lib::access::AppPolicy::messaging_sender_policy()),
                ..brenn_lib::config::test_app_config("test-app")
            };
            apps.insert("test-app".to_string(), app_cfg);
            let apps = Arc::new(apps);
            let directory_arc = Arc::new(directory);
            let global_msg_cfg = brenn_lib::messaging::MessagingGlobalConfig::default();
            let messenger = brenn_messaging::Messenger::new(
                db.clone(),
                directory_arc.clone(),
                Arc::from("brenn://test"),
                apps.clone(),
                Arc::new(FakeWakeRouter),
                global_msg_cfg,
            );
            let (alerts, _alert_handle) = brenn_obs::alerting::AlertDispatcher::noop();
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

    /// Non-singleton: first error creates a new automation_app_event_conversation.
    /// Subsequent errors for the same app reuse the same conversation.
    #[tokio::test]
    async fn error_report_to_non_singleton_creates_event_conversation_first_time_then_reuses() {
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress_router = FakeIngressRouter::new();

        // Engine with singleton = false.
        let engine = {
            let mut apps = indexmap::IndexMap::new();
            let messaging_cfg = brenn_lib::messaging::config::ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            };
            let app_cfg = brenn_lib::config::AppConfig {
                working_dir: std::path::PathBuf::from("/tmp"),
                model: String::new(),
                allowed_users: vec!["testuser".to_string()],
                mcp_servers: Default::default(),
                start_hooks: Default::default(),
                post_pull_hooks: Default::default(),
                startup_hooks: Default::default(),
                integrations: Default::default(),
                history_replay_limit: 100,
                frontmatter: Default::default(),
                state_dir: std::path::PathBuf::from("/tmp"),
                messaging: Some(messaging_cfg),
                policy: std::sync::Arc::new(brenn_lib::access::AppPolicy::messaging_sender_policy()),
                ..brenn_lib::config::test_app_config("test-app")
            };
            apps.insert("test-app".to_string(), app_cfg);
            let apps = Arc::new(apps);
            let directory_arc = Arc::new(directory);
            let messenger = brenn_messaging::Messenger::new(
                db.clone(),
                directory_arc.clone(),
                Arc::from("brenn://test"),
                apps.clone(),
                Arc::new(FakeWakeRouter),
                brenn_lib::messaging::MessagingGlobalConfig::default(),
            );
            let (alerts, _) = brenn_obs::alerting::AlertDispatcher::noop();
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
        use brenn_messaging_store::db as msg_db;

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
                brenn_db::conversation::get_or_create_singleton_conversation(&conn, 1, "test-app");
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
            let messaging_cfg = brenn_lib::messaging::config::ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![],
            };
            let app_cfg = brenn_lib::config::AppConfig {
                working_dir: std::path::PathBuf::from("/tmp"),
                model: String::new(),
                singleton: true,
                allowed_users: vec!["testuser".to_string()],
                mcp_servers: Default::default(),
                start_hooks: Default::default(),
                post_pull_hooks: Default::default(),
                startup_hooks: Default::default(),
                integrations: Default::default(),
                history_replay_limit: 100,
                frontmatter: Default::default(),
                state_dir: std::path::PathBuf::from("/tmp"),
                messaging: Some(messaging_cfg),
                policy: std::sync::Arc::new(brenn_lib::access::AppPolicy::messaging_sender_policy()),
                ..brenn_lib::config::test_app_config("test-app")
            };
            apps.insert("test-app".to_string(), app_cfg);
            let apps = Arc::new(apps);
            let directory_arc = Arc::new(directory);
            let messenger = brenn_messaging::Messenger::new(
                db.clone(),
                directory_arc.clone(),
                Arc::from("brenn://test"),
                apps.clone(),
                Arc::new(FakeWakeRouter),
                brenn_lib::messaging::MessagingGlobalConfig::default(),
            );
            let (alerts, _) = brenn_obs::alerting::AlertDispatcher::noop();
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
        let now_str = brenn_db::format_ts_for_db(chrono::Utc::now());
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
        use brenn_obs::alerting::{CountingAlerter, RateLimiter};
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
        let now_str = brenn_db::format_ts_for_db(chrono::Utc::now());
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

    /// Build a single-app (`test-app`) engine whose policy is caller-supplied, so
    /// Seam-B tests can fire against a tightened/looser policy. Mirrors
    /// `make_engine_full`'s subscription construction (a `ResolvedSubscription`
    /// matching each push-enabled subscriber entry, so app config and directory
    /// agree) but lets the caller pick `policy`.
    fn make_engine_with_policy(
        db: brenn_db::Db,
        directory: MessagingDirectory,
        ingress_router: Arc<dyn IngressRouter>,
        policy: brenn_lib::access::AppPolicy,
    ) -> Arc<AutomationEngine> {
        // Reuse the canonical config builder (test_support) so the 30-field
        // `AppConfig` literal lives in exactly one place; only the `.policy`
        // override is Seam-B-specific. The subscription list is still derived from
        // the directory, so there is a `ResolvedSubscription` for each
        // push-enabled `test-app` subscriber the directory names.
        let subscriptions: Vec<brenn_lib::messaging::config::ResolvedSubscription> = directory
            .list()
            .iter()
            .flat_map(|entry| {
                entry
                    .subscribers
                    .iter()
                    .filter(|s| s.kind.slug() == "test-app")
                    .map(|s| brenn_lib::messaging::config::ResolvedSubscription {
                        channel_uuid: entry.uuid,
                        channel_address: entry.address.clone(),
                        push_depth: s.push_depth,
                        retain_depth: s.retain_depth,
                        noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                        wake_min: brenn_lib::messaging::WakeMin::Normal,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut app_cfg = crate::test_support::default_app_cfg_with_subscriptions(
            "test-app",
            true,
            subscriptions,
        );
        app_cfg.policy = std::sync::Arc::new(policy);
        let mut apps = indexmap::IndexMap::new();
        apps.insert("test-app".to_string(), app_cfg);
        let apps = Arc::new(apps);
        let directory_arc = Arc::new(directory);
        let messenger = brenn_messaging::Messenger::new(
            db.clone(),
            directory_arc.clone(),
            Arc::from("brenn://test"),
            apps.clone(),
            Arc::new(FakeWakeRouter),
            brenn_lib::messaging::MessagingGlobalConfig::default(),
        );
        let (alerts, _) = brenn_obs::alerting::AlertDispatcher::noop();
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
    fn publish_policy_for(channel: &str) -> brenn_lib::access::AppPolicy {
        let mut p = brenn_lib::access::AppPolicy::with_grants(&[
            brenn_envelope::grants::AppCapability::MessagingPublish,
        ]);
        p.acls
            .brenn_publish
            .push(brenn_lib::access::acl::ChannelMatcher::Exact(
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
        let policy = brenn_lib::access::AppPolicy::with_grants(&[
            brenn_envelope::grants::AppCapability::MessagingSubscribe,
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

    // -----------------------------------------------------------------------
    // MQTT action
    // -----------------------------------------------------------------------

    use crate::test_support::{
        MQTT_STATUS, edit_app_policy, grant_only, make_mqtt_engine, mqtt_action, mqtt_sender_policy,
    };
    use brenn_envelope::grants::AppCapability;
    use brenn_lib::messaging::{MqttExpiry, TransportMeta};
    use brenn_lib::mqtt::MqttLifetime;
    use std::num::NonZeroU32;

    const KITCHEN: &str = "mqtt:home:lights/kitchen/set";

    fn mqtt_job(action: crate::job::MqttPublishAction) -> JobSnapshot {
        JobSnapshot {
            action: Action::MqttPublish(action),
            ..make_job_snapshot(1, "test-app")
        }
    }

    /// The rows on `client`'s egress channel.
    async fn egress_rows(engine: &AutomationEngine, client: &str) -> Vec<MessageEnvelope> {
        let channel = engine
            .directory
            .resolve(&brenn_messaging::mqtt_egress_address(client))
            .unwrap();
        let conn = engine.db.lock().await;
        brenn_messaging_store::db::load_channel_retained_tail(&conn, channel.uuid, Depth::Unbounded)
            .into_iter()
            .map(|(_, envelope)| envelope)
            .collect()
    }

    /// The outcome, detail and failure count the fire of job 1 left.
    async fn last_fire(engine: &AutomationEngine) -> (String, String, i64) {
        let conn = engine.db.lock().await;
        let (outcome, detail) = conn
            .query_row(
                "SELECT outcome, coalesce(detail, '') FROM automation_fires \
                 WHERE job_id = 1 ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let failures = conn
            .query_row(
                "SELECT consecutive_failures FROM automation_jobs WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        (outcome, detail, failures)
    }

    /// A fire commits one row to the client's egress channel carrying the
    /// stored topic, options and bytes, sent by the owner app, with the
    /// lifetime counted from the row's commit; an app without the `brenn:`
    /// grant fires it.
    #[tokio::test]
    async fn mqtt_fire_commits_the_stored_publish() {
        let mut policy = mqtt_sender_policy();
        grant_only(&mut policy, AppCapability::MessagingPublish);
        policy.grants.insert(AppCapability::MqttPublish);
        let (engine, _) = make_mqtt_engine(policy, FakeIngressRouter::new()).await;
        for (payload, lifetime) in [
            (
                b"{\"state\":\"ON\"}".to_vec(),
                MqttLifetime::Seconds(NonZeroU32::new(300).unwrap()),
            ),
            (vec![0xff, 0x00], MqttLifetime::Forever),
        ] {
            let action = crate::job::MqttPublishAction {
                qos: 2,
                retain: true,
                lifetime,
                content_type: Some("application/json".to_string()),
                status_to: Some(MQTT_STATUS.to_string()),
                ..mqtt_action(KITCHEN, &payload)
            };
            let mut job = mqtt_job(action);
            job.consecutive_failures = 2;
            {
                let conn = engine.db.lock().await;
                conn.execute("DELETE FROM automation_jobs", []).unwrap();
                insert_job_row(&conn, &job);
            }
            fire_one(&engine, job).await;
            assert_eq!(last_fire(&engine).await, ("ok".into(), String::new(), 0));
            let row = egress_rows(&engine, "home").await.pop().unwrap();
            assert_eq!(row.body.as_bytes(), payload.as_slice());
            assert!(row.sender.contains("test-app"), "{}", row.sender);
            let Some(TransportMeta::MqttPublish(t)) = row.transport else {
                panic!("no MQTT transport: {:?}", row.transport)
            };
            assert_eq!(
                (t.client_slug.as_str(), t.topic.as_str(), t.qos, t.retain),
                ("home", "lights/kitchen/set", 2, true)
            );
            assert_eq!(t.content_type.as_deref(), Some("application/json"));
            assert_eq!(t.status_to.as_deref(), Some(MQTT_STATUS));
            let expected = match lifetime {
                MqttLifetime::Seconds(_) => {
                    MqttExpiry::At(row.publish_ts + chrono::Duration::seconds(300))
                }
                MqttLifetime::Forever => MqttExpiry::Never,
            };
            assert_eq!(t.expires, expected);
        }
    }

    /// Each refusal a configuration change or a stored row can cause is
    /// recorded with its cause, counts as a failure, raises a report and
    /// commits nothing.
    #[tokio::test]
    async fn mqtt_fire_refusals_are_recorded_never_panicked() {
        type Edit = fn(&mut brenn_lib::access::AppPolicy);
        let none: Edit = |_| {};
        // (case, to, status_to, payload length, policy edit, outcome, detail)
        type Case = (
            &'static str,
            &'static str,
            Option<&'static str>,
            usize,
            Edit,
            &'static str,
            &'static str,
        );
        let cases: Vec<Case> = vec![
            (
                "grant revoked",
                KITCHEN,
                None,
                2,
                |p| grant_only(p, AppCapability::MessagingPublish),
                "auth",
                "mqtt_publish grant",
            ),
            (
                "acl tightened",
                KITCHEN,
                None,
                2,
                |p| p.acls.mqtt_publish.clear(),
                "auth",
                "ACL denies",
            ),
            (
                "client absent",
                "mqtt:loft:lights/a/set",
                None,
                2,
                none,
                "auth",
                "not configured",
            ),
            (
                "channel closed",
                "mqtt:away:lights/a/set",
                None,
                2,
                none,
                "auth",
                "not configured",
            ),
            (
                "status_to gone",
                KITCHEN,
                Some("brenn:gone"),
                2,
                none,
                "auth",
                "unknown channel",
            ),
            (
                "status_to acl",
                KITCHEN,
                Some(MQTT_STATUS),
                2,
                |p| p.acls.brenn_publish.clear(),
                "auth",
                "publish ACL denies",
            ),
            (
                "client cap",
                KITCHEN,
                None,
                65,
                none,
                "action_error",
                "over the client's cap",
            ),
        ];
        for (case, to, status_to, len, edit, outcome, detail) in cases {
            let ingress = FakeIngressRouter::new();
            let (engine, apps) = make_mqtt_engine(
                mqtt_sender_policy(),
                ingress.clone() as Arc<dyn IngressRouter>,
            )
            .await;
            let mut action = mqtt_action(to, &vec![b'x'; len]);
            action.status_to = status_to.map(str::to_string);
            let job = mqtt_job(action);
            insert_job_row(&*engine.db.lock().await, &job);
            edit_app_policy(&apps, edit);
            fire_one(&engine, job).await;
            let (got, got_detail, failures) = last_fire(&engine).await;
            assert_eq!(
                (got.as_str(), failures),
                (outcome, 1),
                "{case}: {got_detail}"
            );
            assert!(got_detail.contains(detail), "{case}: {got_detail}");
            assert_eq!(ingress.events().await.len(), 1, "{case}: one report");
            for client in ["home", "away"] {
                assert!(egress_rows(&engine, client).await.is_empty(), "{case}");
            }
        }
    }

    /// A stored reply address is admitted at every fire: it rides as the
    /// Response Topic while the ACL covers it, and a fire after the ACL stops
    /// covering it records an `auth` failure and commits nothing.
    #[tokio::test]
    async fn mqtt_fire_carries_the_reply_topic_and_records_an_acl_narrowed_since() {
        let ingress = FakeIngressRouter::new();
        let (engine, apps) = make_mqtt_engine(
            mqtt_sender_policy(),
            ingress.clone() as Arc<dyn IngressRouter>,
        )
        .await;
        let mut action = mqtt_action(KITCHEN, b"on");
        action.reply_to = Some("mqtt:home:lights/result/set".to_string());
        let job = mqtt_job(action);
        insert_job_row(&*engine.db.lock().await, &job);
        fire_one(&engine, job.clone()).await;
        assert_eq!(last_fire(&engine).await, ("ok".into(), String::new(), 0));
        let row = egress_rows(&engine, "home").await.pop().unwrap();
        let Some(TransportMeta::MqttPublish(t)) = row.transport else {
            panic!("no MQTT transport: {:?}", row.transport)
        };
        assert_eq!(t.response_topic.as_deref(), Some("lights/result/set"));
        assert_eq!(row.reply_to, None);

        edit_app_policy(&apps, |p| {
            p.acls.mqtt_publish = vec![brenn_lib::access::acl::MqttMatcher {
                client: "home".to_string(),
                topic_filter: "lights/kitchen/set".to_string(),
            }];
        });
        fire_one(&engine, job).await;
        let (outcome, detail, failures) = last_fire(&engine).await;
        assert_eq!((outcome.as_str(), failures), ("auth", 1), "{detail}");
        assert!(
            detail.contains("reply_to \"mqtt:home:lights/result/set\"")
                && detail.contains("MQTT publish ACL denies it"),
            "{detail}"
        );
        assert_eq!(
            egress_rows(&engine, "home").await.len(),
            1,
            "nothing more committed"
        );
    }

    /// A `send_message` action to the MQTT engine's `brenn:` channel.
    fn send_message_to_status() -> Action {
        Action::SendMessage(SendMessageAction {
            to: MQTT_STATUS.to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        })
    }

    /// "test-app"'s singleton conversation, created if absent.
    async fn singleton_conversation(engine: &AutomationEngine) -> i64 {
        let conn = engine.db.lock().await;
        brenn_db::conversation::get_or_create_singleton_conversation(&conn, 1, "test-app").id
    }

    /// The send budget row of `conv`, if one was ever written.
    async fn send_budget(engine: &AutomationEngine, conv: i64) -> Option<u32> {
        brenn_messaging_store::db::read_send_budget(&*engine.db.lock().await, conv)
    }

    /// Set "test-app"'s singleton flag and its configured send budget.
    fn shape_app(apps: &brenn_lib::config::AppTable, singleton: bool, send_budget: u32) {
        let mut map = (*apps.load()).clone();
        let app = map.get_mut("test-app").unwrap();
        app.singleton = singleton;
        app.messaging.as_mut().unwrap().send_budget = send_budget;
        apps.store(Arc::new(map));
    }

    /// With nobody interacting, a job of either action kind fires more times
    /// than its app's send budget holds, every fire `ok`, for a singleton app
    /// and for one that is not. No budget is touched: the singleton
    /// conversation's reads the same after the fires, and for the other app no
    /// conversation, events mapping or budget row is created at all.
    #[tokio::test]
    async fn fires_outlast_the_send_budget_and_leave_it_untouched() {
        const BUDGET: u32 = 2;
        for singleton in [true, false] {
            for action in [
                send_message_to_status(),
                Action::MqttPublish(mqtt_action(KITCHEN, b"ON")),
            ] {
                let case = format!("singleton={singleton} {action:?}");
                let ingress = FakeIngressRouter::new();
                let (engine, apps) = make_mqtt_engine(
                    mqtt_sender_policy(),
                    ingress.clone() as Arc<dyn IngressRouter>,
                )
                .await;
                shape_app(&apps, singleton, BUDGET);
                let conv = if singleton {
                    let conv = singleton_conversation(&engine).await;
                    brenn_messaging_store::db::reset_send_budget(
                        &*engine.db.lock().await,
                        conv,
                        BUDGET,
                    );
                    Some(conv)
                } else {
                    None
                };
                let job = JobSnapshot {
                    action,
                    ..make_job_snapshot(1, "test-app")
                };
                insert_job_row(&*engine.db.lock().await, &job);

                for _ in 0..BUDGET * 3 {
                    fire_one(&engine, job.clone()).await;
                    assert_eq!(
                        last_fire(&engine).await,
                        (OUTCOME_OK.to_string(), String::new(), 0),
                        "{case}"
                    );
                }
                assert!(ingress.events().await.is_empty(), "{case}");

                let conn = engine.db.lock().await;
                let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
                assert_eq!(
                    count("SELECT count(*) FROM automation_fires WHERE outcome = 'ok'"),
                    i64::from(BUDGET * 3),
                    "{case}"
                );
                match conv {
                    Some(conv) => assert_eq!(
                        brenn_messaging_store::db::read_send_budget(&conn, conv),
                        Some(BUDGET),
                        "{case}: the singleton conversation's budget is untouched"
                    ),
                    None => {
                        assert_eq!(
                            count("SELECT count(*) FROM conversations"),
                            0,
                            "{case}: no conversation"
                        );
                        assert_eq!(
                            count("SELECT count(*) FROM automation_app_event_conversation"),
                            0,
                            "{case}: no events conversation"
                        );
                        assert_eq!(
                            count("SELECT count(*) FROM messaging_send_budget"),
                            0,
                            "{case}: no budget row"
                        );
                    }
                }
            }
        }
    }

    /// The app's own LLM publishes still draw its conversation's budget in the
    /// same fixture: a `Conversation`-origin publish lowers it by one and is
    /// refused at zero, while an `AutomationFire`-origin publish to the same
    /// channel under the same app succeeds.
    #[tokio::test]
    async fn a_conversation_publish_still_draws_the_budget_a_fire_does_not() {
        let (engine, _) = make_mqtt_engine(mqtt_sender_policy(), FakeIngressRouter::new()).await;
        let conv = singleton_conversation(&engine).await;
        brenn_messaging_store::db::reset_send_budget(&*engine.db.lock().await, conv, 2);
        let publish = async |origin| {
            engine
                .messenger
                .publish(
                    origin,
                    "test-app",
                    MQTT_STATUS,
                    "hello".into(),
                    Urgency::Low,
                    None,
                    None,
                    None,
                )
                .await
        };

        let r = publish(PublishOrigin::Conversation { id: conv }).await;
        assert!(
            matches!(
                r,
                PublishResult::Ok {
                    remaining_budget: Some(1),
                    ..
                }
            ),
            "{r:?}"
        );
        assert_eq!(send_budget(&engine, conv).await, Some(1));

        brenn_messaging_store::db::reset_send_budget(&*engine.db.lock().await, conv, 0);
        let r = publish(PublishOrigin::Conversation { id: conv }).await;
        assert!(matches!(r, PublishResult::BudgetExhausted), "{r:?}");
        let r = publish(PublishOrigin::AutomationFire).await;
        assert!(
            matches!(
                r,
                PublishResult::Ok {
                    remaining_budget: None,
                    ..
                }
            ),
            "{r:?}"
        );
        assert_eq!(send_budget(&engine, conv).await, Some(0));
    }

    /// A committed fire leaves the send budget where it was; a fire to a
    /// removed client (its egress channel closed) is refused as `auth`,
    /// commits nothing and leaves the budget where it was; a failing fire at
    /// the threshold disables the job.
    #[tokio::test]
    async fn mqtt_fire_leaves_the_budget_and_a_removed_client_disables_at_threshold() {
        let (engine, _) = make_mqtt_engine(mqtt_sender_policy(), FakeIngressRouter::new()).await;
        let conv = singleton_conversation(&engine).await;
        brenn_messaging_store::db::reset_send_budget(&*engine.db.lock().await, conv, 5);
        let mut job = mqtt_job(mqtt_action(KITCHEN, b"ON"));
        insert_job_row(&*engine.db.lock().await, &job);
        fire_one(&engine, job.clone()).await;
        assert_eq!(last_fire(&engine).await.0, "ok");
        assert_eq!(
            send_budget(&engine, conv).await,
            Some(5),
            "a committed fire draws nothing"
        );
        assert_eq!(egress_rows(&engine, "home").await.len(), 1);

        job.action = Action::MqttPublish(mqtt_action("mqtt:away:lights/a/set", b"ON"));
        fire_one(&engine, job.clone()).await;
        let (outcome, detail, failures) = last_fire(&engine).await;
        assert_eq!((outcome.as_str(), failures), ("auth", 1), "{detail}");
        assert!(egress_rows(&engine, "away").await.is_empty());
        assert_eq!(
            send_budget(&engine, conv).await,
            Some(5),
            "the refused fire draws nothing"
        );

        job.consecutive_failures = i64::from(engine.defaults.consecutive_failures_to_disable) - 1;
        fire_one(&engine, job).await;
        let (outcome, _, _) = last_fire(&engine).await;
        assert_eq!(outcome, "auth");
        let enabled: i64 = engine
            .db
            .lock()
            .await
            .query_row(
                "SELECT enabled FROM automation_jobs WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(enabled, 0, "the threshold failure disables the job");
        assert_eq!(
            egress_rows(&engine, "home").await.len(),
            1,
            "the refused fire commits nothing"
        );
    }

    // -----------------------------------------------------------------------
    // Per-app fire cap
    // -----------------------------------------------------------------------

    type CapturedAlerts = Arc<std::sync::Mutex<Vec<(String, String)>>>;

    /// A `make_engine_full` engine on the `brenn:test` fixture under `cfg`,
    /// whose alerts are captured. Read the alerts with [`drain_alerts`].
    async fn make_capped_engine(
        cfg: AutomationGlobalConfig,
    ) -> (
        Arc<AutomationEngine>,
        Arc<FakeIngressRouter>,
        CapturedAlerts,
        tokio::task::JoinHandle<()>,
    ) {
        use brenn_obs::alerting::{CapturingAlerter, RateLimiter};
        let (db, _user_id, _channel_uuid, directory) = make_db_with_user_and_channel().await;
        let ingress = FakeIngressRouter::new();
        let captured = CapturedAlerts::default();
        let (alerts, handle) = AlertDispatcher::new(
            CapturingAlerter(captured.clone()),
            RateLimiter::new(1000, 3600),
        );
        let engine = make_engine_full(
            db,
            directory,
            ingress.clone() as Arc<dyn IngressRouter>,
            Arc::new(FakeWakeRouter),
            alerts,
            cfg,
            true,
        );
        (engine, ingress, captured, handle)
    }

    /// Drop `engine`, wait for its alerts to drain, and count them by title.
    async fn drain_alerts(
        engine: Arc<AutomationEngine>,
        captured: CapturedAlerts,
        handle: tokio::task::JoinHandle<()>,
        title: &str,
    ) -> usize {
        drop(engine);
        handle.await.expect("alert drainer task must not panic");
        captured
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| t == title)
            .count()
    }

    const CAP_ALERT: &str = "Automation app fire cap reached";
    const OVERFLOW_ALERT: &str = "Automation report overflow";

    fn cap_config(cap: u32) -> AutomationGlobalConfig {
        AutomationGlobalConfig {
            max_fires_per_day_per_app: cap,
            ..AutomationGlobalConfig::default()
        }
    }

    /// `n` ledger rows of `kind` for `slug`, stamped now.
    async fn add_ledger_rows(engine: &AutomationEngine, slug: &str, kind: &str, n: usize) {
        let conn = engine.db.lock().await;
        for _ in 0..n {
            conn.execute(
                "INSERT INTO automation_app_fires (owner_app_slug, kind, at) VALUES (?1, ?2, ?3)",
                rusqlite::params![slug, kind, brenn_db::format_ts_for_db(Utc::now())],
            )
            .unwrap();
        }
    }

    /// Stamp every ledger row of `kind` 25 hours ago, past the cap's window.
    async fn age_ledger_rows(engine: &AutomationEngine, kind: &str) {
        engine
            .db
            .lock()
            .await
            .execute(
                "UPDATE automation_app_fires SET at = ?1 WHERE kind = ?2",
                rusqlite::params![
                    brenn_db::format_ts_for_db(Utc::now() - chrono::Duration::hours(25)),
                    kind
                ],
            )
            .unwrap();
    }

    async fn ledger_rows(engine: &AutomationEngine, slug: &str, kind: &str) -> i64 {
        engine
            .db
            .lock()
            .await
            .query_row(
                "SELECT count(*) FROM automation_app_fires WHERE owner_app_slug = ?1 AND kind = ?2",
                rusqlite::params![slug, kind],
                |r| r.get(0),
            )
            .unwrap()
    }

    /// `(enabled, consecutive_failures, last_fired_at, next_fire_at)` of job `id`.
    async fn job_state(engine: &AutomationEngine, id: i64) -> (i64, i64, Option<String>, String) {
        engine
            .db
            .lock()
            .await
            .query_row(
                "SELECT enabled, consecutive_failures, last_fired_at, next_fire_at \
                 FROM automation_jobs WHERE id = ?1",
                rusqlite::params![id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap()
    }

    /// The `(outcome, error_class)` of job `id`'s fire rows, oldest first.
    async fn fire_outcomes(engine: &AutomationEngine, id: i64) -> Vec<(String, Option<String>)> {
        let conn = engine.db.lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT outcome, error_class FROM automation_fires WHERE job_id = ?1 ORDER BY id",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![id], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// The rows on the `brenn:test` fixture channel.
    async fn test_channel_rows(engine: &AutomationEngine) -> usize {
        let channel = engine.directory.resolve("brenn:test").unwrap();
        let conn = engine.db.lock().await;
        brenn_messaging_store::db::load_channel_retained_tail(&conn, channel.uuid, Depth::Unbounded)
            .len()
    }

    /// A job of "test-app" with row id `id`, inserted with `failures`.
    async fn insert_capped_job(engine: &AutomationEngine, id: i64, failures: i64) -> JobSnapshot {
        let mut job = make_job_snapshot(id, "test-app");
        job.consecutive_failures = failures;
        insert_job_row(&*engine.db.lock().await, &job);
        job
    }

    fn failing(mut job: JobSnapshot) -> JobSnapshot {
        job.action = Action::SendMessage(SendMessageAction {
            to: "brenn:nonexistent-channel".to_string(),
            body: "hi".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        job
    }

    /// The first fire over the cap is stored as `app_fire_cap`, commits no
    /// message, is reported once and alerted once, leaves the job's failure
    /// count where it was and the job enabled with its next slot advanced.
    /// Once the ledger rows age past the window the same job, unedited, fires
    /// `ok`.
    #[tokio::test]
    async fn the_app_fire_cap_skips_the_slot_and_the_job_comes_back_by_itself() {
        let (engine, ingress, captured, handle) = make_capped_engine(cap_config(2)).await;
        add_ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE, 2).await;
        let job = insert_capped_job(&engine, 1, 2).await;
        let (_, _, _, next_before) = job_state(&engine, 1).await;

        fire_one(&engine, job.clone()).await;
        let (outcome, detail, failures) = last_fire(&engine).await;
        assert_eq!((outcome.as_str(), failures), (OUTCOME_APP_FIRE_CAP, 2));
        assert!(detail.contains("cap=2"), "{detail}");
        assert_eq!(
            fire_outcomes(&engine, 1).await,
            vec![(
                OUTCOME_APP_FIRE_CAP.to_string(),
                Some(OUTCOME_APP_FIRE_CAP.to_string())
            )]
        );
        let (enabled, _, last_fired, next_after) = job_state(&engine, 1).await;
        assert_eq!(enabled, 1);
        assert!(last_fired.is_some());
        assert!(next_after > next_before, "{next_after} > {next_before}");
        assert_eq!(test_channel_rows(&engine).await, 0, "nothing committed");
        let events = ingress.events().await;
        assert_eq!(events.len(), 1);
        assert!(
            events[0].4.contains(OUTCOME_APP_FIRE_CAP),
            "{}",
            events[0].4
        );
        assert_eq!(
            ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE).await,
            2
        );

        age_ledger_rows(&engine, APP_FIRE_KIND_FIRE).await;
        fire_one(&engine, job).await;
        assert_eq!(
            last_fire(&engine).await,
            (OUTCOME_OK.into(), String::new(), 0)
        );
        assert_eq!(test_channel_rows(&engine).await, 1);
        assert_eq!(drain_alerts(engine, captured, handle, CAP_ALERT).await, 1);
    }

    /// More refusals in a row than the auto-disable threshold leave the job
    /// enabled with no failure counted; every row is `app_fire_cap`, none is
    /// `rate_limit_suppressed_report`, and neither per-job window counts them.
    /// A cap of 0 admits no fire.
    #[tokio::test]
    async fn a_cap_refusal_never_disables_and_stays_out_of_both_job_windows() {
        let (engine, ingress, _, _) = make_capped_engine(cap_config(0)).await;
        let job = insert_capped_job(&engine, 1, 0).await;
        let refusals = engine.defaults.consecutive_failures_to_disable as usize + 2;
        for _ in 0..refusals {
            fire_one(&engine, job.clone()).await;
        }
        assert_eq!(job_state(&engine, 1).await.0, 1, "still enabled");
        assert_eq!(job_state(&engine, 1).await.1, 0, "no failure counted");
        let outcomes = fire_outcomes(&engine, 1).await;
        assert_eq!(outcomes.len(), refusals);
        assert!(
            outcomes.iter().all(|(o, _)| o == OUTCOME_APP_FIRE_CAP),
            "{outcomes:?}"
        );
        let conn = engine.db.lock().await;
        assert_eq!(count_fires_in_window(&conn, 1, Utc::now(), 3600), 0);
        assert_eq!(
            count_reportable_fires_in_window(&conn, 1, Utc::now(), 3600),
            0
        );
        drop(conn);
        assert_eq!(ingress.events().await.len(), 1, "one report per app");
        assert_eq!(test_channel_rows(&engine).await, 0);
    }

    /// Several jobs of one app, each refused more than the per-job report
    /// limit across more than an hour, are reported once in all. Once the
    /// `cap_report` row ages past the window the next refusal is reported
    /// again. One cap alert is raised and no report-overflow alert.
    #[tokio::test]
    async fn cap_refusals_are_reported_once_per_app_per_window() {
        let (engine, ingress, captured, handle) = make_capped_engine(cap_config(1)).await;
        add_ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE, 1).await;
        let mut jobs = Vec::new();
        for id in 1..=3 {
            jobs.push(insert_capped_job(&engine, id, 0).await);
        }
        let per_job = engine.defaults.max_error_reports_per_hour_per_job as usize + 1;
        for _hour in 0..2 {
            for job in &jobs {
                for _ in 0..per_job {
                    fire_one(&engine, job.clone()).await;
                }
            }
            engine
                .db
                .lock()
                .await
                .execute(
                    "UPDATE automation_fires SET fired_at = ?1",
                    rusqlite::params![brenn_db::format_ts_for_db(
                        Utc::now() - chrono::Duration::minutes(70)
                    )],
                )
                .unwrap();
        }
        assert_eq!(ingress.events().await.len(), 1);
        for job in &jobs {
            assert!(
                fire_outcomes(&engine, job.row_id)
                    .await
                    .iter()
                    .all(|(o, _)| o == OUTCOME_APP_FIRE_CAP)
            );
        }

        age_ledger_rows(&engine, APP_FIRE_KIND_CAP_REPORT).await;
        fire_one(&engine, jobs[1].clone()).await;
        assert_eq!(
            ingress.events().await.len(),
            2,
            "a new window, a new report"
        );

        let alerts = captured.clone();
        assert_eq!(drain_alerts(engine, captured, handle, CAP_ALERT).await, 1);
        assert!(
            !alerts
                .lock()
                .unwrap()
                .iter()
                .any(|(t, _)| t == OVERFLOW_ALERT),
            "no report-overflow alert"
        );
    }

    /// Deleting and re-creating a job refills neither the cap nor the report
    /// allowance: the admitted fire's ledger row stays, the re-created job is
    /// refused and reported, and after a second re-creation it is refused
    /// without a report.
    #[tokio::test]
    async fn deleting_and_recreating_a_job_refills_neither_the_cap_nor_the_reports() {
        let (engine, ingress, _, _) = make_capped_engine(cap_config(1)).await;
        let job = insert_capped_job(&engine, 1, 0).await;
        fire_one(&engine, job).await;
        assert_eq!(last_fire(&engine).await.0, OUTCOME_OK);

        for reports in [1, 1] {
            engine
                .db
                .lock()
                .await
                .execute("DELETE FROM automation_jobs", [])
                .unwrap();
            let job = insert_capped_job(&engine, 1, 0).await;
            fire_one(&engine, job).await;
            assert_eq!(last_fire(&engine).await.0, OUTCOME_APP_FIRE_CAP);
            assert_eq!(ingress.events().await.len(), reports);
        }
        assert_eq!(test_channel_rows(&engine).await, 1);
    }

    /// A cap refusal does not use the job's hourly report allowance: the same
    /// job then failing for another cause in the same hour is still reported
    /// `max_error_reports_per_hour_per_job` times before suppression.
    #[tokio::test]
    async fn a_cap_refusal_leaves_the_jobs_report_allowance() {
        let (engine, ingress, _, _) = make_capped_engine(cap_config(10)).await;
        add_ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE, 10).await;
        let job = failing(insert_capped_job(&engine, 1, 0).await);
        fire_one(&engine, job.clone()).await;
        assert_eq!(last_fire(&engine).await.0, OUTCOME_APP_FIRE_CAP);
        assert_eq!(ingress.events().await.len(), 1);

        age_ledger_rows(&engine, APP_FIRE_KIND_FIRE).await;
        let limit = engine.defaults.max_error_reports_per_hour_per_job as usize;
        for _ in 0..limit {
            fire_one(&engine, job.clone()).await;
            assert_eq!(last_fire(&engine).await.0, OUTCOME_AUTH);
        }
        assert_eq!(ingress.events().await.len(), 1 + limit);
        fire_one(&engine, job).await;
        assert_eq!(last_fire(&engine).await.0, OUTCOME_RATE_LIMIT_SUPPRESSED);
        assert_eq!(ingress.events().await.len(), 1 + limit);
    }

    /// The cap counts `fire` rows only, and only the owner app's: with the
    /// cap at N, N−1 `fire` rows and a `cap_report` row of the app and N rows
    /// of another app in the window, the next fire is admitted.
    #[tokio::test]
    async fn the_cap_counts_the_apps_fire_rows_only() {
        let (engine, _, _, _) = make_capped_engine(cap_config(2)).await;
        add_ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE, 1).await;
        add_ledger_rows(&engine, "test-app", APP_FIRE_KIND_CAP_REPORT, 1).await;
        add_ledger_rows(&engine, "other-app", APP_FIRE_KIND_FIRE, 2).await;
        let job = insert_capped_job(&engine, 1, 0).await;
        fire_one(&engine, job).await;
        assert_eq!(last_fire(&engine).await.0, OUTCOME_OK);
        assert_eq!(
            ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE).await,
            2
        );
    }

    /// The gate on its own: two apps are admitted, refused with a report and
    /// refused silently independently; it prunes every app's rows older than
    /// the window; a row stamped after the fire still counts.
    #[test]
    fn the_gate_keeps_each_app_apart_and_prunes_every_app() {
        let db = init_db_memory();
        let conn = db.blocking_lock();
        let now = Utc::now();
        let stamp = |slug: &str, at: DateTime<Utc>| {
            conn.execute(
                "INSERT INTO automation_app_fires (owner_app_slug, kind, at) \
                 VALUES (?1, 'fire', ?2)",
                rusqlite::params![slug, brenn_db::format_ts_for_db(at)],
            )
            .unwrap();
        };
        stamp("gone-app", now - chrono::Duration::hours(25));
        for slug in ["app-a", "app-b"] {
            assert_eq!(app_fire_gate(&conn, slug, now, 1), AppFireGate::Admit);
            assert_eq!(
                app_fire_gate(&conn, slug, now, 1),
                AppFireGate::Refuse {
                    report: true,
                    count: 1
                }
            );
            assert_eq!(
                app_fire_gate(&conn, slug, now, 1),
                AppFireGate::Refuse {
                    report: false,
                    count: 1
                }
            );
        }
        let gone: i64 = conn
            .query_row(
                "SELECT count(*) FROM automation_app_fires WHERE owner_app_slug = 'gone-app'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(gone, 0, "another app's expired rows are pruned");

        stamp("app-c", now + chrono::Duration::hours(2));
        assert_eq!(
            app_fire_gate(&conn, "app-c", now, 1),
            AppFireGate::Refuse {
                report: true,
                count: 1
            },
            "a row stamped later than the fire still counts"
        );
    }

    /// A fire the action refuses still holds its ledger row: an MQTT fire to
    /// a removed client is `auth` and counted.
    #[tokio::test]
    async fn a_fire_refused_inside_its_action_is_counted() {
        let (engine, _) = make_mqtt_engine(mqtt_sender_policy(), FakeIngressRouter::new()).await;
        let job = mqtt_job(mqtt_action("mqtt:away:lights/a/set", b"ON"));
        insert_job_row(&*engine.db.lock().await, &job);
        fire_one(&engine, job).await;
        assert_eq!(last_fire(&engine).await.0, OUTCOME_AUTH);
        assert_eq!(
            ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE).await,
            1
        );
    }

    /// A fire the per-job hourly gate or an owner check stops never reaches
    /// the per-app cap: it writes no ledger row and leaves the app's allowance
    /// for its healthy jobs. With the cap full, an owner check still answers
    /// first.
    #[tokio::test]
    async fn a_fire_stopped_before_the_cap_leaves_no_ledger_row() {
        let (engine, _ingress, _captured, _handle) = make_capped_engine(cap_config(1)).await;

        let rate_limited = insert_capped_job(&engine, 1, 0).await;
        {
            let conn = engine.db.lock().await;
            let now = brenn_db::format_ts_for_db(Utc::now());
            for _ in 0..engine.defaults.max_fires_per_hour_per_job {
                conn.execute(
                    "INSERT INTO automation_fires (job_id, fired_at, outcome) VALUES (1, ?1, ?2)",
                    rusqlite::params![now, OUTCOME_OK],
                )
                .unwrap();
            }
        }
        fire_one(&engine, rate_limited).await;
        assert_eq!(
            fire_outcomes(&engine, 1).await.last().unwrap().0,
            OUTCOME_RATE_LIMIT
        );

        let mut gone = insert_capped_job(&engine, 2, 0).await;
        gone.owner_app_slug = "ghost-app".to_string();
        fire_one(&engine, gone).await;
        assert_eq!(fire_outcomes(&engine, 2).await[0].0, OUTCOME_APP_GONE);

        assert_eq!(
            ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE).await,
            0
        );
        assert_eq!(
            ledger_rows(&engine, "ghost-app", APP_FIRE_KIND_FIRE).await,
            0
        );

        let healthy = insert_capped_job(&engine, 3, 0).await;
        fire_one(&engine, healthy).await;
        assert_eq!(fire_outcomes(&engine, 3).await[0].0, OUTCOME_OK);
        assert_eq!(
            ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE).await,
            1
        );

        edit_app_policy(&engine.apps, |p| {
            grant_only(p, AppCapability::MessagingSubscribe)
        });
        let unauthorised = insert_capped_job(&engine, 4, 0).await;
        fire_one(&engine, unauthorised).await;
        assert_eq!(fire_outcomes(&engine, 4).await[0].0, OUTCOME_AUTH);
        assert_eq!(
            ledger_rows(&engine, "test-app", APP_FIRE_KIND_FIRE).await,
            1
        );
        assert_eq!(
            ledger_rows(&engine, "test-app", APP_FIRE_KIND_CAP_REPORT).await,
            0
        );
    }
}

//! Automation job types: trigger/action sum types, DTOs, and MCP tool name
//! constants.
//!
//! Validation logic (`validate_create_job`, `validate_edit_job`) lives here
//! too — it's exercised by tests without needing a running engine.
//!
//! `compute_next` lives here because it is pure logic (cron + tz → next
//! `DateTime<Utc>`) and is exercised by tests without a running engine.

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

use crate::messaging::Urgency;

// ---------------------------------------------------------------------------
// MCP tool name constants
// ---------------------------------------------------------------------------

/// `mcp__brenn__AutoCreate` — create an automation job.
pub const MCP_AUTO_CREATE_TOOL: &str = "mcp__brenn__AutoCreate";
/// `mcp__brenn__AutoList` — list automation jobs owned by the caller's app.
pub const MCP_AUTO_LIST_TOOL: &str = "mcp__brenn__AutoList";
/// `mcp__brenn__AutoEdit` — edit an automation job by id.
pub const MCP_AUTO_EDIT_TOOL: &str = "mcp__brenn__AutoEdit";
/// `mcp__brenn__AutoDelete` — delete an automation job by id.
pub const MCP_AUTO_DELETE_TOOL: &str = "mcp__brenn__AutoDelete";

// ---------------------------------------------------------------------------
// Trigger sum type
// ---------------------------------------------------------------------------

/// Sum type for automation trigger variants. Only `Cron` exists this
/// iteration; the `kind`/payload JSON shape admits future variants without
/// a migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Trigger {
    Cron(CronTrigger),
}

impl Trigger {
    /// Return the `trigger_kind` SQL column value for this variant.
    /// Must match the `CHECK(trigger_kind IN (...))` constraint in db.rs.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Trigger::Cron(_) => "cron",
        }
    }
}

/// Cron-time trigger parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronTrigger {
    /// 5-field cron expression (`min hour dom month dow`). Seconds-field
    /// (6-field) expressions are rejected at validation.
    pub expr: String,
    /// IANA timezone name (e.g. `America/New_York`, `UTC`). Required;
    /// no default.
    pub tz: String,
    /// Systemd-style missed-fire policy. `true`: fire once on restart for
    /// any missed occurrences. `false`: skip missed occurrences silently.
    pub persistent: bool,
}

// ---------------------------------------------------------------------------
// Action sum type
// ---------------------------------------------------------------------------

/// Sum type for automation action variants. Only `SendMessage` exists this
/// iteration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Action {
    SendMessage(SendMessageAction),
}

impl Action {
    /// Return the `action_kind` SQL column value for this variant.
    /// Must match the `CHECK(action_kind IN (...))` constraint in db.rs.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Action::SendMessage(_) => "send_message",
        }
    }
}

/// Send-message action parameters. Mirrors the MCP send-message tool inputs
/// except `deliver_after` (mutually exclusive with scheduled firing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageAction {
    /// Destination address in `brenn:<name>` form.
    pub to: String,
    /// Message body text.
    pub body: String,
    /// Message urgency (`very-low`, `low`, `normal`, or `high`).
    pub urgency: Urgency,
    /// Optional reply-to address in `brenn:<name>` form.
    pub reply_to: Option<String>,
    /// Relative delivery deadline in seconds from fire time. If `Some`,
    /// must be in `[1, 2_592_000]` (30 days).
    pub delivery_deadline_secs: Option<u32>,
}

// ---------------------------------------------------------------------------
// Input structs (what callers pass to create/edit)
// ---------------------------------------------------------------------------

/// Input for `AutoCreate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateJob {
    /// Short human-readable name. Non-empty, <= 128 bytes.
    pub name: String,
    /// Trigger spec.
    pub trigger: Trigger,
    /// Action spec.
    pub action: Action,
    /// Whether the job starts enabled. Default `true`.
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// Input for `AutoEdit`. All fields optional; unset fields are left unchanged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditJob {
    /// Job id (UUID string) returned by `AutoCreate`.
    pub id: String,
    /// New name, if updating.
    pub name: Option<String>,
    /// New trigger, if updating.
    pub trigger: Option<Trigger>,
    /// New action, if updating.
    pub action: Option<Action>,
    /// New enabled state, if updating.
    pub enabled: Option<bool>,
}

// ---------------------------------------------------------------------------
// View DTO (returned by list)
// ---------------------------------------------------------------------------

/// Read-only view of an automation job returned by `AutoList`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobView {
    /// Opaque job id (UUID string).
    pub id: String,
    /// Owning app slug (immutable post-create).
    pub owner_app_slug: String,
    /// Human-readable name.
    pub name: String,
    /// Trigger spec.
    pub trigger: Trigger,
    /// Action spec.
    pub action: Action,
    /// Whether the job is currently enabled.
    pub enabled: bool,
    /// Number of consecutive failures (resets to 0 on success).
    pub consecutive_failures: u32,
    /// When the job was created.
    pub created_at: DateTime<Utc>,
    /// When the job was last updated.
    pub updated_at: DateTime<Utc>,
    /// When the job last fired (`None` if never).
    pub last_fired_at: Option<DateTime<Utc>>,
    /// When the job is next scheduled to fire.
    pub next_fire_at: DateTime<Utc>,
    /// Set when `enabled = false` was caused by the auto-disable threshold.
    pub auto_disabled_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Internal snapshot type (used by fire logic)
// ---------------------------------------------------------------------------

/// Snapshot of a job row loaded by the scheduler loop. Used as the atomic
/// state for a single fire — see §2.7 atomicity boundary in the design.
#[derive(Debug, Clone)]
pub struct JobSnapshot {
    pub row_id: i64,
    pub uuid: Uuid,
    pub owner_app_slug: String,
    pub name: String,
    pub trigger: Trigger,
    pub action: Action,
    pub enabled: bool,
    pub consecutive_failures: i64,
    pub created_at: DateTime<Utc>,
    pub last_fired_at: Option<DateTime<Utc>>,
    pub next_fire_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Cron validation helpers
// ---------------------------------------------------------------------------

/// Errors returned when parsing/validating a `CronTrigger`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronValidationError {
    /// Expression does not have exactly 5 fields.
    WrongFieldCount { got: usize },
    /// `croner` rejected the expression.
    ParseError(String),
    /// The expression matched no future occurrence within the croner search
    /// limit (e.g., `0 2 30 2 *` — Feb 30 never exists).
    NeverMatches,
    /// IANA timezone name was empty or unrecognized.
    InvalidTimezone(String),
}

impl std::fmt::Display for CronValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongFieldCount { got } => {
                write!(f, "cron expression must have exactly 5 fields (got {got})")
            }
            Self::ParseError(msg) => write!(f, "invalid cron expression: {msg}"),
            Self::NeverMatches => write!(f, "cron expression never matches a future occurrence"),
            Self::InvalidTimezone(tz) => write!(f, "unrecognized IANA timezone: {tz:?}"),
        }
    }
}

/// Parse and validate a `CronTrigger`, returning the parsed `Cron` and `Tz`
/// if valid.  Called at create/edit time.
///
/// A `now` argument is needed to verify that the expression matches at least
/// one future occurrence.
pub fn validate_cron_trigger(
    trigger: &CronTrigger,
    now: DateTime<Utc>,
) -> Result<(Cron, Tz), CronValidationError> {
    // 1. Field count: exactly 5 (reject 6-field seconds form).
    let field_count = trigger.expr.split_whitespace().count();
    if field_count != 5 {
        return Err(CronValidationError::WrongFieldCount { got: field_count });
    }

    // 2. Parse the cron expression.
    let cron = Cron::from_str(&trigger.expr)
        .map_err(|e| CronValidationError::ParseError(e.to_string()))?;

    // 3. Parse the timezone.
    if trigger.tz.is_empty() {
        return Err(CronValidationError::InvalidTimezone(trigger.tz.clone()));
    }
    let tz: Tz = trigger
        .tz
        .parse()
        .map_err(|_| CronValidationError::InvalidTimezone(trigger.tz.clone()))?;

    // 4. Verify the expression produces at least one future occurrence.
    if compute_next_inner(&cron, tz, now).is_none() {
        return Err(CronValidationError::NeverMatches);
    }

    Ok((cron, tz))
}

// ---------------------------------------------------------------------------
// compute_next
// ---------------------------------------------------------------------------

/// Compute the next fire time strictly after `after` for the given
/// `CronTrigger`.  Returns `None` if the expression never fires again
/// (e.g., `0 2 30 2 *`).
///
/// DST policy (per design §2.7 + requirements A3):
/// - **Spring-forward nonexistent local times:** no fire; skip to the next
///   calendar occurrence.
/// - **Fall-back ambiguous local times:** fire once at the earlier (first)
///   instant.
///
/// Croner's `FixedTime` behaviour snaps spring-forward slots to the first
/// valid wall-clock second after the gap, which violates the "no fire"
/// policy.  We detect this by re-examining the returned datetime: if the
/// hour/minute do not match what the cron expression specifies, the returned
/// time came from a gap-snap and we discard it, then recurse from that point
/// to find the genuine next occurrence.
///
/// Croner's fall-back behaviour returns the earliest instant, which matches
/// the policy directly.
pub fn compute_next(trigger: &CronTrigger, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let tz: Tz = trigger.tz.parse().ok()?;
    let cron = Cron::from_str(&trigger.expr).ok()?;
    compute_next_inner(&cron, tz, after)
}

/// Inner implementation shared by `compute_next` and `validate_cron_trigger`.
fn compute_next_inner(cron: &Cron, tz: Tz, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    // Convert `after` to the job's wall-clock timezone for the search.
    let after_tz: chrono::DateTime<Tz> = after.with_timezone(&tz);

    // Ask croner for the next occurrence strictly after `after`.
    let candidate = cron.find_next_occurrence(&after_tz, false).ok()?;

    // DST spring-forward correction:
    // Croner's FixedTime-job logic snaps a nonexistent local time to the
    // first valid wall-clock second after the gap.  When that happens the
    // returned datetime's naive_local() minute/hour will differ from what
    // the cron expression specifies.
    //
    // Detection: `cron.is_time_matching` returns false when the candidate's
    // hour:minute don't match the pattern — this is the load-bearing check
    // for gap-snap detection.  We also accept ambiguous (fall-back) times as
    // genuine occurrences (fire once at the earlier instant).
    //
    // Note: the `LocalResult::None` arm that appeared in earlier drafts is
    // unreachable in practice (a DateTime<Tz> returned by croner always has a
    // valid local time), so we rely solely on `is_time_matching` here.
    // If croner ever changes this contract, the DST tests will catch it.
    let is_gap_artefact = !cron.is_time_matching(&candidate).unwrap_or(false);

    if is_gap_artefact {
        // The candidate is a gap-snap artefact.  Advance `after` to the
        // candidate (in UTC) and recurse to find the genuine next occurrence
        // after the gap.
        let new_after = candidate.with_timezone(&Utc);
        // Guard against infinite recursion on degenerate expressions: only
        // recurse if we actually advanced.
        if new_after > after {
            return compute_next_inner(cron, tz, new_after);
        }
        return None;
    }

    Some(candidate.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_tool_name_constants_correct() {
        assert_eq!(MCP_AUTO_CREATE_TOOL, "mcp__brenn__AutoCreate");
        assert_eq!(MCP_AUTO_LIST_TOOL, "mcp__brenn__AutoList");
        assert_eq!(MCP_AUTO_EDIT_TOOL, "mcp__brenn__AutoEdit");
        assert_eq!(MCP_AUTO_DELETE_TOOL, "mcp__brenn__AutoDelete");
    }

    #[test]
    fn trigger_cron_round_trips_json() {
        let t = Trigger::Cron(CronTrigger {
            expr: "*/5 * * * *".to_string(),
            tz: "America/New_York".to_string(),
            persistent: true,
        });
        let json = serde_json::to_string(&t).unwrap();
        let back: Trigger = serde_json::from_str(&json).unwrap();
        match back {
            Trigger::Cron(c) => {
                assert_eq!(c.expr, "*/5 * * * *");
                assert_eq!(c.tz, "America/New_York");
                assert!(c.persistent);
            }
        }
    }

    #[test]
    fn action_send_message_round_trips_json() {
        let a = Action::SendMessage(SendMessageAction {
            to: "brenn:test-channel".to_string(),
            body: "hello".to_string(),
            urgency: Urgency::Normal,
            reply_to: None,
            delivery_deadline_secs: Some(300),
        });
        let json = serde_json::to_string(&a).unwrap();
        let back: Action = serde_json::from_str(&json).unwrap();
        match back {
            Action::SendMessage(s) => {
                assert_eq!(s.to, "brenn:test-channel");
                assert_eq!(s.body, "hello");
                assert_eq!(s.delivery_deadline_secs, Some(300));
            }
        }
    }

    #[test]
    fn trigger_kind_tag_in_json() {
        let t = Trigger::Cron(CronTrigger {
            expr: "0 9 * * *".to_string(),
            tz: "UTC".to_string(),
            persistent: false,
        });
        let json = serde_json::to_string(&t).unwrap();
        // The `kind` field must be present for extensible deserialization.
        assert!(json.contains(r#""kind":"cron""#), "json={json}");
    }

    #[test]
    fn action_kind_tag_in_json() {
        let a = Action::SendMessage(SendMessageAction {
            to: "brenn:ch".to_string(),
            body: "b".to_string(),
            urgency: Urgency::Low,
            reply_to: None,
            delivery_deadline_secs: None,
        });
        let json = serde_json::to_string(&a).unwrap();
        assert!(json.contains(r#""kind":"send_message""#), "json={json}");
    }

    #[test]
    fn create_job_default_enabled_true() {
        let json = r#"{"name":"test","trigger":{"kind":"cron","expr":"* * * * *","tz":"UTC","persistent":false},"action":{"kind":"send_message","to":"brenn:ch","body":"hi","urgency":"low","reply_to":null,"delivery_deadline_secs":null}}"#;
        let cj: CreateJob = serde_json::from_str(json).unwrap();
        assert!(cj.enabled, "default enabled should be true");
    }

    // -------------------------------------------------------------------------
    // validate_cron_trigger tests
    // -------------------------------------------------------------------------

    fn mk_trigger(expr: &str, tz: &str) -> CronTrigger {
        CronTrigger {
            expr: expr.to_string(),
            tz: tz.to_string(),
            persistent: false,
        }
    }

    fn utc_now_fixed() -> DateTime<Utc> {
        // 2025-01-15 12:00:00 UTC — a stable, non-DST-transition reference.
        use chrono::TimeZone as _;
        chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()
    }

    #[test]
    fn validate_cron_5_field_accepts_standard() {
        let t = mk_trigger("*/5 * * * *", "UTC");
        assert!(validate_cron_trigger(&t, utc_now_fixed()).is_ok());
    }

    #[test]
    fn validate_cron_rejects_6_field_seconds() {
        let t = mk_trigger("0 */5 * * * *", "UTC");
        let err = validate_cron_trigger(&t, utc_now_fixed()).unwrap_err();
        assert!(
            matches!(err, CronValidationError::WrongFieldCount { got: 6 }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_cron_rejects_invalid_expr() {
        // 5 fields but with an invalid token (99 is out of range for minutes).
        let t = mk_trigger("99 99 99 99 99", "UTC");
        let err = validate_cron_trigger(&t, utc_now_fixed()).unwrap_err();
        assert!(
            matches!(
                err,
                CronValidationError::ParseError(_) | CronValidationError::NeverMatches
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_tz_rejects_unknown() {
        let t = mk_trigger("*/5 * * * *", "Mars/Olympus");
        let err = validate_cron_trigger(&t, utc_now_fixed()).unwrap_err();
        assert!(
            matches!(err, CronValidationError::InvalidTimezone(_)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_tz_rejects_empty() {
        let t = mk_trigger("*/5 * * * *", "");
        let err = validate_cron_trigger(&t, utc_now_fixed()).unwrap_err();
        assert!(
            matches!(err, CronValidationError::InvalidTimezone(_)),
            "unexpected error: {err}"
        );
    }

    // -------------------------------------------------------------------------
    // compute_next tests
    // -------------------------------------------------------------------------

    #[test]
    fn next_fire_basic_every_5_min_utc() {
        use chrono::TimeZone as _;
        // After 12:00:00 UTC, next */5 fire must be 12:05:00 UTC.
        let after = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap();
        let t = mk_trigger("*/5 * * * *", "UTC");
        let next = compute_next(&t, after).expect("should have next occurrence");
        let expected = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 5, 0).unwrap();
        assert_eq!(next, expected, "next fire should be 12:05 UTC");
    }

    // DST spring-forward: 2024-03-10 in America/New_York.
    // Clocks jump from 02:00 -> 03:00; the 02:30 naive time does not exist.
    // `30 2 * * *` should produce NO fire for 2024-03-10 and instead fire
    // on 2024-03-11 02:30 EDT (= 06:30 UTC).
    #[test]
    fn next_fire_dst_spring_forward_skips_nonexistent() {
        use chrono::TimeZone as _;
        // Start just before midnight going into the spring-forward day.
        // 2024-03-10 00:00:00 EST = 05:00:00 UTC
        let after = chrono::Utc.with_ymd_and_hms(2024, 3, 10, 5, 0, 0).unwrap();
        let t = mk_trigger("30 2 * * *", "America/New_York");
        let next = compute_next(&t, after).expect("should have next occurrence");

        // Expected: 2024-03-11 02:30 EDT = 06:30 UTC (one day later, past the gap).
        let expected = chrono::Utc.with_ymd_and_hms(2024, 3, 11, 6, 30, 0).unwrap();
        assert_eq!(
            next, expected,
            "spring-forward should skip to next day; got {next}"
        );
    }

    // DST fall-back: 2024-11-03 in America/New_York.
    // Clocks fall back from 02:00 -> 01:00; 01:30 exists twice.
    // `30 1 * * *` should fire at the EARLIER (first, EDT) 01:30.
    // 01:30 EDT = 05:30 UTC; 01:30 EST = 06:30 UTC.
    #[test]
    fn next_fire_dst_fall_back_picks_earlier_instant() {
        use chrono::TimeZone as _;
        // Start at midnight going into the fall-back day.
        // 2024-11-03 00:00:00 EDT = 04:00:00 UTC
        let after = chrono::Utc.with_ymd_and_hms(2024, 11, 3, 4, 0, 0).unwrap();
        let t = mk_trigger("30 1 * * *", "America/New_York");
        let next = compute_next(&t, after).expect("should have next occurrence");

        // Expected: earlier of the two 01:30 instances = EDT = 05:30 UTC.
        let expected = chrono::Utc.with_ymd_and_hms(2024, 11, 3, 5, 30, 0).unwrap();
        assert_eq!(
            next, expected,
            "fall-back should fire at earlier (EDT) instance; got {next}"
        );
    }

    // -------------------------------------------------------------------------
    // DST spike verification tests (design §4, TODO(automation-croner-dst-verify))
    // -------------------------------------------------------------------------

    /// Verifies croner's behaviour for a FixedTime job on a spring-forward day
    /// when our adapter wraps it.  The 02:30 slot in America/New_York on
    /// 2024-03-10 does not exist; the adapter must skip it entirely.
    #[test]
    fn croner_spring_forward_wrapper_skips_nonexistent_local_time() {
        use chrono::TimeZone as _;
        // Set `after` to 01:59 EST on the spring-forward day = 06:59 UTC.
        let after = chrono::Utc.with_ymd_and_hms(2024, 3, 10, 6, 59, 0).unwrap();
        let t = mk_trigger("30 2 * * *", "America/New_York");
        let next = compute_next(&t, after).expect("should have next occurrence");

        // Must be next-day 02:30 EDT = 06:30 UTC on 2024-03-11.
        let expected = chrono::Utc.with_ymd_and_hms(2024, 3, 11, 6, 30, 0).unwrap();
        assert_eq!(
            next, expected,
            "croner adapter must skip gap slot, not snap to post-gap time; got {next}"
        );
    }

    /// Verifies that for a fall-back day, `compute_next` returns the earlier
    /// (EDT = UTC-4) of the two ambiguous 01:30 instances, not the later EST.
    #[test]
    fn croner_fall_back_wrapper_picks_earliest_ambiguous_instance() {
        use chrono::TimeZone as _;
        // Set `after` to 01:00 EDT = 05:00 UTC — still before both 01:30s.
        let after = chrono::Utc.with_ymd_and_hms(2024, 11, 3, 5, 0, 0).unwrap();
        let t = mk_trigger("30 1 * * *", "America/New_York");
        let next = compute_next(&t, after).expect("should have next occurrence");

        // EDT 01:30 = 05:30 UTC.
        let expected = chrono::Utc.with_ymd_and_hms(2024, 11, 3, 5, 30, 0).unwrap();
        assert_eq!(
            next, expected,
            "fall-back must fire at earlier (EDT) 01:30; got {next}"
        );
    }
}

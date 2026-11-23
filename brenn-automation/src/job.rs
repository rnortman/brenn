//! Automation job types: trigger/action sum types and DTOs.
//!
//! Validation logic (`validate_create_job`, `validate_edit_job`) lives here
//! too — it's exercised by tests without needing a running engine.
//!
//! `compute_next` lives here because it is pure logic (cron + tz → next
//! `DateTime<Utc>`) and is exercised by tests without a running engine.

use base64ct::{Base64, Encoding as _};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::str::FromStr;
use uuid::Uuid;

use brenn_lib::messaging::Urgency;
use brenn_lib::mqtt::{MqttLifetime, MqttPublishOptions};

// The four automation tool names live with every other MCP tool name, so a
// crate that formats or approves them needs no edge to this engine. Re-exported
// here because this crate's callers address them as `brenn_automation::`.
pub use brenn_lib::mcp_tool_names::{
    MCP_AUTO_CREATE_TOOL, MCP_AUTO_DELETE_TOOL, MCP_AUTO_EDIT_TOOL, MCP_AUTO_LIST_TOOL,
};

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

/// Sum type for automation action variants: a `brenn:` message or an MQTT
/// publish. Stored as this JSON in `automation_jobs.action_payload`, the
/// variant named by its `kind` tag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Action {
    SendMessage(SendMessageAction),
    MqttPublish(MqttPublishAction),
}

/// Send-message action parameters. Mirrors the MCP send-message tool inputs
/// except `deliver_after` (mutually exclusive with scheduled firing).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// MQTT publish action parameters: what an `MqttSend` call carries, with the
/// payload fixed when the job is created or edited. It has no urgency or
/// delivery deadline, which a broker cannot carry, and no release time: the
/// job's trigger is its time.
///
/// Stored under the tool's own argument names, so a listed action given back
/// to `AutoEdit` publishes the same bytes: the payload as exactly one of
/// `body` (its text, when the bytes are UTF-8) and `body_base64` (standard
/// base64 of the bytes otherwise); `lifetime_s` as a whole number of seconds
/// or `"forever"`; `content_type`, `status_to` and `reply_to` omitted when not
/// set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "MqttPublishWire", into = "MqttPublishWire")]
pub struct MqttPublishAction {
    /// Target address in `mqtt:<client>:<topic>` form, as given.
    pub to: String,
    pub payload: MqttActionPayload,
    pub qos: u8,
    pub retain: bool,
    /// Counted from each fire's commit.
    pub lifetime: MqttLifetime,
    /// MQTT 5 Content Type to send; absent sends none.
    pub content_type: Option<String>,
    /// The `brenn:` channel that receives each fire's delivery statuses.
    pub status_to: Option<String>,
    /// The reply address as given, `mqtt:<client>:<topic>`, sent as each
    /// fire's MQTT 5 Response Topic.
    pub reply_to: Option<String>,
}

impl MqttPublishAction {
    /// The options each fire's publish takes.
    pub fn options(&self) -> MqttPublishOptions {
        MqttPublishOptions {
            qos: self.qos,
            retain: self.retain,
            lifetime: self.lifetime,
            content_type: self.content_type.clone(),
            status_to: self.status_to.clone(),
            reply_to: self.reply_to.clone(),
        }
    }
}

/// The fixed payload of an MQTT action: the bytes each fire publishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttActionPayload(Vec<u8>);

impl MqttActionPayload {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The stored JSON of an [`MqttPublishAction`], field for field.
#[derive(Serialize, Deserialize)]
struct MqttPublishWire {
    to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
    qos: u8,
    retain: bool,
    lifetime_s: StoredLifetime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reply_to: Option<String>,
}

impl TryFrom<MqttPublishWire> for MqttPublishAction {
    type Error = String;

    fn try_from(wire: MqttPublishWire) -> Result<Self, String> {
        let payload = match (wire.body, wire.body_base64) {
            (Some(text), None) => MqttActionPayload(text.into_bytes()),
            (None, Some(encoded)) => MqttActionPayload(
                Base64::decode_vec(&encoded)
                    .map_err(|e| format!("body_base64 is not standard base64: {e}"))?,
            ),
            (Some(_), Some(_)) => return Err("both body and body_base64 are set".to_string()),
            (None, None) => return Err("neither body nor body_base64 is set".to_string()),
        };
        Ok(Self {
            to: wire.to,
            payload,
            qos: wire.qos,
            retain: wire.retain,
            lifetime: wire.lifetime_s.0,
            content_type: wire.content_type,
            status_to: wire.status_to,
            reply_to: wire.reply_to,
        })
    }
}

impl From<MqttPublishAction> for MqttPublishWire {
    fn from(action: MqttPublishAction) -> Self {
        // Text is stored as text; any other byte string as base64.
        let (body, body_base64) = match String::from_utf8(action.payload.0) {
            Ok(text) => (Some(text), None),
            Err(e) => (None, Some(Base64::encode_string(e.as_bytes()))),
        };
        Self {
            to: action.to,
            body,
            body_base64,
            qos: action.qos,
            retain: action.retain,
            lifetime_s: StoredLifetime(action.lifetime),
            content_type: action.content_type,
            status_to: action.status_to,
            reply_to: action.reply_to,
        }
    }
}

/// `lifetime_s` as stored: a whole number from 1 to `u32::MAX`, or the string
/// `"forever"`.
struct StoredLifetime(MqttLifetime);

const FOREVER: &str = "forever";

impl Serialize for StoredLifetime {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            MqttLifetime::Seconds(s) => serializer.serialize_u32(s.get()),
            MqttLifetime::Forever => serializer.serialize_str(FOREVER),
        }
    }
}

impl<'de> Deserialize<'de> for StoredLifetime {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = StoredLifetime;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "a whole number from 1 to {} or \"{FOREVER}\"", u32::MAX)
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<StoredLifetime, E> {
                u32::try_from(v)
                    .ok()
                    .and_then(NonZeroU32::new)
                    .map(|s| StoredLifetime(MqttLifetime::Seconds(s)))
                    .ok_or_else(|| E::invalid_value(serde::de::Unexpected::Unsigned(v), &self))
            }

            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<StoredLifetime, E> {
                match u64::try_from(v) {
                    Ok(v) => self.visit_u64(v),
                    Err(_) => Err(E::invalid_value(serde::de::Unexpected::Signed(v), &self)),
                }
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<StoredLifetime, E> {
                if v == FOREVER {
                    Ok(StoredLifetime(MqttLifetime::Forever))
                } else {
                    Err(E::invalid_value(serde::de::Unexpected::Str(v), &self))
                }
            }
        }

        deserializer.deserialize_any(Visitor)
    }
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

/// Snapshot of a job row loaded by the scheduler loop. The fire runs against
/// this snapshot, never re-reading the row.
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
/// DST policy:
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
        assert_eq!(back, a);
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

    // -------------------------------------------------------------------------
    // Stored form of an MQTT action
    // -------------------------------------------------------------------------

    fn mqtt_action(payload: MqttActionPayload, lifetime: MqttLifetime) -> Action {
        Action::MqttPublish(MqttPublishAction {
            to: "mqtt:home:lights/kitchen/set".to_string(),
            payload,
            qos: 1,
            retain: false,
            lifetime,
            content_type: Some("application/json".to_string()),
            status_to: Some("brenn:light-status".to_string()),
            reply_to: Some("mqtt:home:lights/kitchen/result".to_string()),
        })
    }

    fn secs(n: u32) -> MqttLifetime {
        MqttLifetime::Seconds(NonZeroU32::new(n).unwrap())
    }

    /// Serialise `action`, assert the exact JSON, and assert it reads back
    /// equal.
    fn assert_stored(action: &Action, expected: &str) {
        let json = serde_json::to_string(action).unwrap();
        assert_eq!(json, expected);
        let back: Action = serde_json::from_str(&json).unwrap();
        assert_eq!(&back, action);
    }

    #[test]
    fn mqtt_action_with_a_text_payload_is_stored_under_the_tools_names() {
        assert_stored(
            &mqtt_action(
                MqttActionPayload::from_bytes(br#"{"state":"ON"}"#.to_vec()),
                secs(300),
            ),
            r#"{"kind":"mqtt_publish","to":"mqtt:home:lights/kitchen/set","body":"{\"state\":\"ON\"}","qos":1,"retain":false,"lifetime_s":300,"content_type":"application/json","status_to":"brenn:light-status","reply_to":"mqtt:home:lights/kitchen/result"}"#,
        );
    }

    #[test]
    fn mqtt_action_with_non_utf8_bytes_is_stored_as_base64() {
        let payload = MqttActionPayload::from_bytes(vec![0xff, 0x00, 0xfe]);
        assert_stored(
            &mqtt_action(payload, secs(300)),
            r#"{"kind":"mqtt_publish","to":"mqtt:home:lights/kitchen/set","body_base64":"/wD+","qos":1,"retain":false,"lifetime_s":300,"content_type":"application/json","status_to":"brenn:light-status","reply_to":"mqtt:home:lights/kitchen/result"}"#,
        );
    }

    #[test]
    fn mqtt_action_forever_and_unset_options_are_stored_so() {
        let action = Action::MqttPublish(MqttPublishAction {
            to: "mqtt:home:lights/kitchen/set".to_string(),
            payload: MqttActionPayload::from_bytes(b"ON".to_vec()),
            qos: 0,
            retain: true,
            lifetime: MqttLifetime::Forever,
            content_type: None,
            status_to: None,
            reply_to: None,
        });
        assert_stored(
            &action,
            r#"{"kind":"mqtt_publish","to":"mqtt:home:lights/kitchen/set","body":"ON","qos":0,"retain":true,"lifetime_s":"forever"}"#,
        );
    }

    #[test]
    fn mqtt_action_largest_lifetime_round_trips() {
        let action = mqtt_action(MqttActionPayload::from_bytes(Vec::new()), secs(u32::MAX));
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains(r#""lifetime_s":4294967295"#), "{json}");
        assert_eq!(serde_json::from_str::<Action>(&json).unwrap(), action);
    }

    #[test]
    fn mqtt_action_rows_of_the_wrong_shape_do_not_read() {
        let row = |rest: &str| {
            format!(r#"{{"kind":"mqtt_publish","to":"mqtt:home:a","qos":1,"retain":false,{rest}}}"#)
        };
        for bad in [
            row(r#""body":"x","body_base64":"eA==","lifetime_s":60"#),
            row(r#""lifetime_s":60"#),
            row(r#""body":"x","lifetime_s":0"#),
            row(r#""body":"x","lifetime_s":4294967296"#),
            row(r#""body":"x","lifetime_s":-1"#),
            row(r#""body":"x","lifetime_s":"never""#),
            row(r#""body":"x","lifetime_s":1.5"#),
            row(r#""body":"x""#),
            row(r#""body_base64":"not base64!","lifetime_s":60"#),
            r#"{"kind":"mqtt_subscribe","to":"mqtt:home:a"}"#.to_string(),
        ] {
            assert!(
                serde_json::from_str::<Action>(&bad).is_err(),
                "must not deserialise: {bad}"
            );
        }
    }

    /// A row whose base64 decodes to UTF-8 reads as those bytes, equal to the
    /// same bytes given as text, and is written back as text.
    #[test]
    fn mqtt_action_base64_of_text_reads_as_the_text() {
        let row = |payload: &str| {
            format!(
                r#"{{"kind":"mqtt_publish","to":"mqtt:home:a",{payload},"qos":1,"retain":false,"lifetime_s":60}}"#
            )
        };
        let from_base64: Action = serde_json::from_str(&row(r#""body_base64":"T04=""#)).unwrap();
        let from_text: Action = serde_json::from_str(&row(r#""body":"ON""#)).unwrap();
        assert_eq!(from_base64, from_text);
        assert_eq!(
            serde_json::to_string(&from_base64).unwrap(),
            row(r#""body":"ON""#)
        );
    }

    /// A `send_message` row as written before the MQTT action existed reads
    /// and serialises back byte for byte.
    #[test]
    fn send_message_row_written_earlier_round_trips_byte_identically() {
        let stored = r#"{"kind":"send_message","to":"brenn:ch","body":"hi","urgency":"low","reply_to":null,"delivery_deadline_secs":300}"#;
        let action: Action = serde_json::from_str(stored).unwrap();
        assert!(matches!(action, Action::SendMessage(_)));
        assert_eq!(serde_json::to_string(&action).unwrap(), stored);
    }

    #[test]
    fn mqtt_action_options_are_its_fields() {
        let Action::MqttPublish(action) =
            mqtt_action(MqttActionPayload::from_bytes(b"x".to_vec()), secs(9))
        else {
            unreachable!()
        };
        assert_eq!(
            action.options(),
            MqttPublishOptions {
                qos: 1,
                retain: false,
                lifetime: secs(9),
                content_type: Some("application/json".to_string()),
                status_to: Some("brenn:light-status".to_string()),
                reply_to: Some("mqtt:home:lights/kitchen/result".to_string()),
            }
        );
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

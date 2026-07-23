//! DOM-free meeting escalation state machine.
//!
//! Every branch here is host-tested. The wasm glue converts each `Err` into a
//! panic (operator misconfig or shell/proto skew), which the module's panic hook
//! turns into an error card. A well-formed delivery whose *body* violates the
//! agenda/ack convention is a semi-trusted publisher fault: it keeps the current
//! state, bumps a page-lifetime counter, and is reported to the operator log —
//! never a panic. Same posture as protobar and mode-clock.
//!
//! The escalation phase is a pure function of `(snapshot, acks, now)`: no stored
//! escalation state can diverge from the wall clock, so a reboot, suspend/resume,
//! or clock step self-corrects on the next recompute. The agenda channel is a
//! full snapshot, latest-wins; the ack channel accumulates dismiss/snooze acks
//! keyed by `meeting_id`, latest-wins per meeting by publish timestamp.
//!
//! Acks are scoped to an occurrence: an ack carries the acked meeting's `start`
//! and suppresses only a meeting whose `start` matches it, so a publisher that
//! reuses one `meeting_id` across days does not inherit yesterday's dismissal.

use std::collections::BTreeMap;

use brenn_envelope::MessageEnvelope;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

use brenn_surface_component_support::parse_delivery;
pub use brenn_surface_component_support::{ContractViolation, FaultReport};

/// The agenda-subscription input port name.
const AGENDA_PORT: &str = "agenda";
/// The ack subscribe-and-publish port name.
const ACKS_PORT: &str = "acks";

/// Escalation ladder defaults (seconds), used when a meeting carries no
/// `escalation` override or an invalid one. `takeover > critical` is the ordering
/// the phase ladder depends on.
const DEFAULT_TAKEOVER_SECS: i64 = 120;
const DEFAULT_CRITICAL_SECS: i64 = 60;
const DEFAULT_OVERDUE_SECS: i64 = 60;

/// A meeting is retired (stops escalating) once it is this many seconds past its
/// start with no dismissal — the "overdue forever" cap.
const RETIRE_AFTER_SECS: i64 = 60 * 60;

/// An ack whose `(meeting_id, start)` occurrence is absent from the current
/// snapshot is pruned once it is this stale (judged on the delivered envelope's
/// publish timestamp, so a reconnect replay of an old ack still counts as stale).
const ACK_STALE_SECS: i64 = 24 * 60 * 60;

/// The default snooze duration the Snooze button applies (5 minutes).
pub const SNOOZE_SECS: i64 = 5 * 60;

/// A meeting counts as "near" (fast 1 s countdown ticks) when its start is within
/// this window of now, in either direction; otherwise recomputes are coarse.
const NEAR_SECS: i64 = 60 * 60;

/// The rendered escalation state — the `data-state` hook the skins dress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisplayState {
    /// No active meeting: idle "no meetings" panel.
    Idle,
    /// A future meeting outside its takeover window.
    Ambient,
    /// Inside the takeover window (fullscreen overlay requested).
    Takeover,
    /// Inside the critical window (at/near start).
    Critical,
    /// Past start, undismissed.
    Overdue,
}

impl DisplayState {
    /// The `data-state` attribute value.
    pub fn as_wire_str(self) -> &'static str {
        match self {
            DisplayState::Idle => "idle",
            DisplayState::Ambient => "ambient",
            DisplayState::Takeover => "takeover",
            DisplayState::Critical => "critical",
            DisplayState::Overdue => "overdue",
        }
    }
}

/// The severity the glue logs an [`IngestWarning`] at. Two levels, not proto's
/// four: `LogLevel` lives in a wasm-only dependency and this module is
/// host-tested, so the glue maps this across.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarningLevel {
    /// Expected under a well-behaved publisher; noted, not blamed.
    Warn,
    /// A publisher fault the operator should see on the error channel.
    Error,
}

/// A body-level warning from a delivery that was otherwise accepted, and the
/// level it deserves. Level travels with the text because the two sources differ
/// in kind: an invalid escalation override is a publisher fault, a startless ack
/// is the designed drop of a pre-occurrence-scoping body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestWarning {
    pub level: WarningLevel,
    pub message: String,
}

impl IngestWarning {
    fn warn(message: String) -> Self {
        IngestWarning {
            level: WarningLevel::Warn,
            message,
        }
    }

    fn error(message: String) -> Self {
        IngestWarning {
            level: WarningLevel::Error,
            message,
        }
    }
}

/// The outcome of an accepted delivery on a known port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestOutcome {
    /// Body parsed and updated state. `warnings` are per-body notes the glue
    /// logs at each one's own level.
    Accepted { warnings: Vec<IngestWarning> },
    /// Body violated the convention. State untouched; the report carries what the
    /// DOM glue needs for a `COMPONENT_LOG` error.
    Malformed(FaultReport),
}

/// Per-meeting escalation thresholds (seconds before/after start).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Escalation {
    takeover_secs: i64,
    critical_secs: i64,
    overdue_secs: i64,
}

impl Default for Escalation {
    fn default() -> Self {
        Escalation {
            takeover_secs: DEFAULT_TAKEOVER_SECS,
            critical_secs: DEFAULT_CRITICAL_SECS,
            overdue_secs: DEFAULT_OVERDUE_SECS,
        }
    }
}

/// One meeting from the current snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Meeting {
    id: String,
    start: DateTime<Utc>,
    title: String,
    escalation: Escalation,
}

/// The escalation phase of a meeting relative to `now`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Ambient,
    Takeover,
    Critical,
    Overdue,
}

impl Meeting {
    /// The phase at `now`:
    /// `Ambient` (t < start − takeover) → `Takeover` → `Critical`
    /// (t ≥ start − critical) → `Overdue` (t ≥ start + overdue).
    fn phase(&self, now: DateTime<Utc>) -> Phase {
        let secs_to_start = (self.start - now).num_seconds();
        if secs_to_start > self.escalation.takeover_secs {
            Phase::Ambient
        } else if secs_to_start > self.escalation.critical_secs {
            Phase::Takeover
        } else if -secs_to_start < self.escalation.overdue_secs {
            Phase::Critical
        } else {
            Phase::Overdue
        }
    }

    /// Whether this meeting is escalating (in its takeover window or later) at
    /// `now` — the supersession trigger: a later escalating meeting retires an
    /// earlier one so the nearest alarm owns the screen.
    fn is_escalating(&self, now: DateTime<Utc>) -> bool {
        !matches!(self.phase(now), Phase::Ambient)
    }

    /// Retired by the 1 h overdue cap: undismissed meetings stop escalating so a
    /// bar does not flash all afternoon for a 9 AM meeting nobody dismissed.
    fn past_retire_cap(&self, now: DateTime<Utc>) -> bool {
        (now - self.start).num_seconds() >= RETIRE_AFTER_SECS
    }
}

/// A stored ack for one meeting: the latest action seen for its `meeting_id`,
/// the occurrence it acked, and the publish timestamp that ordered it (and bounds
/// its pruning).
#[derive(Debug, Clone, PartialEq, Eq)]
struct AckRecord {
    action: AckAction,
    start: DateTime<Utc>,
    publish_ts: DateTime<Utc>,
}

/// The occurrence a Dismiss/Snooze button acks: the active meeting's id and its
/// `start`. Both travel on the wire so an ack binds to one occurrence rather than
/// to a reusable id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckTarget {
    pub meeting_id: String,
    pub start: DateTime<Utc>,
}

/// A dismiss/snooze action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckAction {
    /// Permanent removal.
    Dismiss,
    /// Suppress until this instant, then re-manifest at whatever phase applies.
    Snooze { until: DateTime<Utc> },
}

impl AckRecord {
    /// Whether this ack suppresses the occurrence starting at `start` at `now`.
    /// The occurrence must match: a rescheduled or day-reused id starts clean.
    /// Dismiss is permanent; a snooze suppresses only until its `until`.
    fn suppresses(&self, start: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        if self.start != start {
            return false;
        }
        match self.action {
            AckAction::Dismiss => true,
            AckAction::Snooze { until } => now < until,
        }
    }
}

/// The result of a recompute: the full desired view plus the takeover intent and
/// the recommended next wakeup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recompute {
    pub state: DisplayState,
    /// Phase label (e.g. "NEXT MEETING", "OVERDUE", "NO MEETINGS").
    pub label: String,
    /// The active meeting's title, or empty in the idle state.
    pub title: String,
    /// The big countdown value (e.g. "2:00", "+1:30"), or empty in idle.
    pub countdown: String,
    /// A coarse human sub-line (e.g. "in 3 min", "1 min ago"), or empty in idle.
    pub subline: String,
    /// Whether the Dismiss/Snooze buttons should be interactive (takeover+).
    pub show_buttons: bool,
    /// Whether a fullscreen takeover overlay should be active (takeover+).
    pub want_takeover: bool,
    /// The active meeting's occurrence — the target for a Dismiss/Snooze press.
    pub active: Option<AckTarget>,
    /// Seconds until the next recommended recompute.
    pub next_tick_secs: u32,
}

/// Raw agenda snapshot as serde sees it. Unknown fields are ignored (additive
/// external contract). `meetings` may be empty (valid idle state).
#[derive(Deserialize)]
struct RawSnapshot {
    #[serde(default)]
    #[allow(dead_code)]
    v: Option<u32>,
    meetings: Vec<RawMeeting>,
}

#[derive(Deserialize)]
struct RawMeeting {
    id: String,
    start: String,
    title: String,
    #[serde(default)]
    #[allow(dead_code)]
    end: Option<String>,
    #[serde(default)]
    escalation: Option<RawEscalation>,
}

#[derive(Deserialize)]
struct RawEscalation {
    takeover_secs: i64,
    critical_secs: i64,
    overdue_secs: i64,
}

/// Raw ack as serde sees it. `start` is the acked occurrence; `until` is required
/// for `snooze`, absent for `dismiss`.
#[derive(Deserialize)]
struct RawAck {
    #[serde(default)]
    #[allow(dead_code)]
    v: Option<u32>,
    meeting_id: String,
    action: String,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    until: Option<String>,
}

/// What an ack body turned out to be.
enum AckParse {
    /// A well-formed occurrence-scoped ack.
    Ack(AckTarget, AckAction),
    /// An ack carrying no parseable `start`: it names no occurrence, so it is
    /// dropped and the reason reported as a warning.
    Startless(String),
}

/// Meeting escalation state: the last-good snapshot, the ack map, and a
/// page-lifetime fault counter.
#[derive(Debug, Default)]
pub struct MeetingState {
    meetings: Vec<Meeting>,
    acks: BTreeMap<String, AckRecord>,
    faults: u64,
}

impl MeetingState {
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of malformed bodies seen this page lifetime.
    pub fn faults(&self) -> u64 {
        self.faults
    }

    /// Handle a delivery on the `agenda` or `acks` port. Rejects a wrong port and
    /// an unparseable envelope (both `ContractViolation`, panic-worthy skew). A
    /// well-formed envelope whose body violates the convention returns
    /// `IngestOutcome::Malformed` — state untouched, counter bumped — so one buggy
    /// publisher cannot brick the panel.
    pub fn on_message(
        &mut self,
        port: &str,
        envelope_json: &str,
        now: DateTime<Utc>,
    ) -> Result<IngestOutcome, ContractViolation> {
        let envelope = parse_delivery(port, &[AGENDA_PORT, ACKS_PORT], envelope_json)?;

        let outcome = if port == AGENDA_PORT {
            match parse_snapshot(&envelope.body) {
                Ok((meetings, warnings)) => {
                    self.meetings = meetings;
                    IngestOutcome::Accepted { warnings }
                }
                Err(reason) => self.fault(&envelope, reason),
            }
        } else {
            match parse_ack(&envelope.body) {
                Ok(AckParse::Ack(target, action)) => {
                    self.store_ack(&target, action, envelope.publish_ts);
                    IngestOutcome::Accepted { warnings: vec![] }
                }
                // Warn, not error: an ack naming no occurrence is what every
                // body minted before occurrence scoping looks like, and the
                // acks ring replays those on each attach until they age out.
                // Reporting the expected as a fault would flood the error
                // channel the operator watches for real ones.
                Ok(AckParse::Startless(warning)) => IngestOutcome::Accepted {
                    warnings: vec![IngestWarning::warn(warning)],
                },
                Err(reason) => self.fault(&envelope, reason),
            }
        };
        self.prune_acks(now);
        Ok(outcome)
    }

    /// Apply a Dismiss/Snooze locally (immediately, before the ack echoes back),
    /// stamped with `now` as its publish time. Idempotent with the echoed ack.
    pub fn apply_local_ack(&mut self, target: &AckTarget, action: AckAction, now: DateTime<Utc>) {
        self.store_ack(target, action, now);
        self.prune_acks(now);
    }

    /// Record `report` and bump the counter.
    fn fault(&mut self, envelope: &MessageEnvelope, reason: String) -> IngestOutcome {
        self.faults += 1;
        IngestOutcome::Malformed(FaultReport::new(envelope, reason))
    }

    /// Store an ack, latest-wins per meeting by publish timestamp — an older
    /// replayed ack never overwrites a newer decision. One record per id is
    /// enough: ids are unique within a snapshot, so two occurrences of one id
    /// never coexist as candidates.
    fn store_ack(&mut self, target: &AckTarget, action: AckAction, publish_ts: DateTime<Utc>) {
        let record = AckRecord {
            action,
            start: target.start,
            publish_ts,
        };
        match self.acks.get(&target.meeting_id) {
            Some(existing) if existing.publish_ts >= publish_ts => {}
            _ => {
                self.acks.insert(target.meeting_id.clone(), record);
            }
        }
    }

    /// Drop acks whose occurrence — `(meeting_id, start)` — is absent from the
    /// current snapshot **and** whose publish timestamp is stale (> 24 h before
    /// `now`): the map's bound under a chatty publisher, and what stops an ack for
    /// a daily-reused id from living forever. Staleness is judged on the publish
    /// timestamp, not local receipt, so a reconnect replay of an old ack still
    /// counts as stale.
    fn prune_acks(&mut self, now: DateTime<Utc>) {
        let stale_before = now - Duration::seconds(ACK_STALE_SECS);
        self.acks.retain(|id, record| {
            let current = self
                .meetings
                .iter()
                .any(|m| &m.id == id && m.start == record.start);
            current || record.publish_ts >= stale_before
        });
    }

    /// Whether a meeting is suppressed at `now` by a dismiss or an active snooze
    /// of that same occurrence.
    fn suppressed(&self, meeting: &Meeting, now: DateTime<Utc>) -> bool {
        self.acks
            .get(&meeting.id)
            .is_some_and(|record| record.suppresses(meeting.start, now))
    }

    /// The active meeting at `now`: the earliest-start candidate not superseded by
    /// a later escalating one. Candidates exclude suppressed and retire-capped
    /// meetings.
    fn active(&self, now: DateTime<Utc>) -> Option<&Meeting> {
        let candidates: Vec<&Meeting> = self
            .meetings
            .iter()
            .filter(|m| !self.suppressed(m, now) && !m.past_retire_cap(now))
            .collect();
        candidates
            .iter()
            .copied()
            .filter(|m| {
                // Superseded when a later-starting candidate is already escalating
                // — nearest alarm wins the screen.
                !candidates
                    .iter()
                    .any(|other| other.start > m.start && other.is_escalating(now))
            })
            .min_by_key(|m| m.start)
    }

    /// Compute the full desired view and next wakeup at `now`.
    pub fn recompute(&self, now: DateTime<Utc>) -> Recompute {
        let Some(meeting) = self.active(now) else {
            return Recompute {
                state: DisplayState::Idle,
                label: "NO MEETINGS".to_string(),
                title: String::new(),
                countdown: String::new(),
                subline: String::new(),
                show_buttons: false,
                want_takeover: false,
                active: None,
                next_tick_secs: 60,
            };
        };
        let phase = meeting.phase(now);
        let state = match phase {
            Phase::Ambient => DisplayState::Ambient,
            Phase::Takeover => DisplayState::Takeover,
            Phase::Critical => DisplayState::Critical,
            Phase::Overdue => DisplayState::Overdue,
        };
        let escalating = !matches!(phase, Phase::Ambient);
        let secs_to_start = (meeting.start - now).num_seconds();
        Recompute {
            state,
            label: label_for(phase).to_string(),
            title: meeting.title.clone(),
            countdown: format_countdown(secs_to_start),
            subline: format_subline(secs_to_start),
            show_buttons: escalating,
            want_takeover: escalating,
            active: Some(AckTarget {
                meeting_id: meeting.id.clone(),
                start: meeting.start,
            }),
            // TODO(meeting-tick-visibility): a headless (no-layout-slot) meeting
            // ticks at 1 s for the whole ±1 h window even while hidden. Gating the
            // fast rate on visibility is not a plain glue check: a hidden meeting
            // must still fire its takeover-request near the boundary (that request
            // is what makes it visible), so the correct fix is exact next-boundary
            // scheduling plus 1 s only when a panel is shown — a scheduling change,
            // not a bucket flip.
            next_tick_secs: if secs_to_start.abs() < NEAR_SECS {
                1
            } else {
                60
            },
        }
    }
}

/// The phase label shown above the countdown.
fn label_for(phase: Phase) -> &'static str {
    match phase {
        Phase::Ambient => "NEXT MEETING",
        Phase::Takeover => "STARTING SOON",
        Phase::Critical => "STARTING NOW",
        Phase::Overdue => "OVERDUE",
    }
}

/// The big countdown value from `secs_to_start` (positive = future). Overdue
/// (past start) carries a leading `+`. Formats `H:MM:SS` past an hour, else
/// `M:SS`.
fn format_countdown(secs_to_start: i64) -> String {
    let overdue = secs_to_start < 0;
    let total = secs_to_start.unsigned_abs();
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let seconds = total % 60;
    let body = if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes}:{seconds:02}")
    };
    if overdue { format!("+{body}") } else { body }
}

/// A coarse human sub-line: minutes to/from start, rounded to whole minutes.
fn format_subline(secs_to_start: i64) -> String {
    let minutes = (secs_to_start.abs() + 59) / 60; // round up
    if secs_to_start >= 0 {
        format!("in {minutes} min")
    } else {
        format!("{minutes} min ago")
    }
}

/// Parse an RFC3339 timestamp to UTC, or a precise reason string.
fn parse_ts(field: &str, s: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| format!("invalid {field} {s:?}: {e}"))
}

/// Validate an agenda snapshot into meetings plus per-meeting override warnings
/// (the meeting is kept with defaults), or a precise whole-snapshot fault reason.
fn parse_snapshot(body: &str) -> Result<(Vec<Meeting>, Vec<IngestWarning>), String> {
    let raw: RawSnapshot =
        serde_json::from_str(body).map_err(|e| format!("unparseable agenda: {e}"))?;
    let mut meetings = Vec::with_capacity(raw.meetings.len());
    let mut warnings = Vec::new();
    for rm in raw.meetings {
        if rm.id.is_empty() {
            return Err("meeting id must be non-empty".to_string());
        }
        if meetings.iter().any(|m: &Meeting| m.id == rm.id) {
            return Err(format!("duplicate meeting id {:?}", rm.id));
        }
        let start = parse_ts("start", &rm.start)?;
        let escalation = match rm.escalation {
            None => Escalation::default(),
            Some(raw) => match validate_escalation(&raw) {
                Some(esc) => esc,
                None => {
                    warnings.push(IngestWarning::error(format!(
                        "meeting {:?} has an invalid escalation override \
                         (takeover_secs={}, critical_secs={}, overdue_secs={}); using defaults",
                        rm.id, raw.takeover_secs, raw.critical_secs, raw.overdue_secs
                    )));
                    Escalation::default()
                }
            },
        };
        meetings.push(Meeting {
            id: rm.id,
            start,
            title: rm.title,
            escalation,
        });
    }
    Ok((meetings, warnings))
}

/// An override is valid only when all three thresholds are ≥ 0,
/// `takeover_secs > critical_secs` (the phase ladder's ordering invariant), and
/// `overdue_secs < RETIRE_AFTER_SECS`. An `overdue_secs` at or beyond the 1 h
/// retire cap would retire the meeting while it is still Critical — the Overdue
/// phase the publisher configured could never manifest — so it is rejected to
/// defaults rather than silently swallowed.
fn validate_escalation(raw: &RawEscalation) -> Option<Escalation> {
    if raw.takeover_secs < 0
        || raw.critical_secs < 0
        || raw.overdue_secs < 0
        || raw.takeover_secs <= raw.critical_secs
        || raw.overdue_secs >= RETIRE_AFTER_SECS
    {
        return None;
    }
    Some(Escalation {
        takeover_secs: raw.takeover_secs,
        critical_secs: raw.critical_secs,
        overdue_secs: raw.overdue_secs,
    })
}

/// Validate an ack into its occurrence and action, or a precise fault reason. An
/// otherwise well-formed ack whose `start` is missing or unparseable names no
/// occurrence and cannot be scoped, so it is reported as `Startless` and dropped
/// rather than faulted — the shape a publisher predating occurrence scoping
/// emits. A bad id or action is still a fault, judged first.
fn parse_ack(body: &str) -> Result<AckParse, String> {
    let raw: RawAck = serde_json::from_str(body).map_err(|e| format!("unparseable ack: {e}"))?;
    if raw.meeting_id.is_empty() {
        return Err("ack meeting_id must be non-empty".to_string());
    }
    let action = match raw.action.as_str() {
        "dismiss" => AckAction::Dismiss,
        "snooze" => {
            let until = raw
                .until
                .as_deref()
                .ok_or_else(|| "snooze ack requires until".to_string())?;
            AckAction::Snooze {
                until: parse_ts("until", until)?,
            }
        }
        other => return Err(format!("unknown ack action {other:?}")),
    };
    let start = match raw.start.as_deref().map(|s| parse_ts("start", s)) {
        Some(Ok(start)) => start,
        Some(Err(reason)) => {
            return Ok(AckParse::Startless(format!(
                "ack for meeting {:?} dropped: {reason}",
                raw.meeting_id
            )));
        }
        None => {
            return Ok(AckParse::Startless(format!(
                "ack for meeting {:?} dropped: no start, so it names no occurrence",
                raw.meeting_id
            )));
        }
    };
    Ok(AckParse::Ack(
        AckTarget {
            meeting_id: raw.meeting_id,
            start,
        },
        action,
    ))
}

/// The ack body a Dismiss button publishes for `target`.
pub fn dismiss_body(target: &AckTarget) -> String {
    serde_json::json!({
        "v": 1,
        "meeting_id": target.meeting_id,
        "start": target.start.to_rfc3339(),
        "action": "dismiss",
    })
    .to_string()
}

/// The ack body a Snooze button publishes for `target`, suppressed until `until`.
pub fn snooze_body(target: &AckTarget, until: DateTime<Utc>) -> String {
    serde_json::json!({
        "v": 1,
        "meeting_id": target.meeting_id,
        "start": target.start.to_rfc3339(),
        "action": "snooze",
        "until": until.to_rfc3339(),
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_surface_test_fixtures::sample_envelope_json_at as envelope_json;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    /// A one-meeting snapshot body starting at `start`, default thresholds.
    fn snapshot(start: &str, title: &str) -> String {
        serde_json::json!({
            "v": 1,
            "meetings": [{ "id": "m1", "start": start, "title": title }],
        })
        .to_string()
    }

    /// The occurrence a button press would ack.
    fn target(meeting_id: &str, start: &str) -> AckTarget {
        AckTarget {
            meeting_id: meeting_id.to_string(),
            start: at(start),
        }
    }

    /// The active meeting's id at `now`.
    fn active_id(state: &MeetingState, now: DateTime<Utc>) -> Option<String> {
        state.recompute(now).active.map(|t| t.meeting_id)
    }

    fn feed_agenda(state: &mut MeetingState, body: &str, now: DateTime<Utc>) -> IngestOutcome {
        state
            .on_message("agenda", &envelope_json(body, "2026-07-12T00:00:00Z"), now)
            .unwrap()
    }

    fn feed_ack(
        state: &mut MeetingState,
        body: &str,
        publish_ts: &str,
        now: DateTime<Utc>,
    ) -> IngestOutcome {
        state
            .on_message("acks", &envelope_json(body, publish_ts), now)
            .unwrap()
    }

    #[test]
    fn empty_snapshot_is_idle() {
        let mut state = MeetingState::new();
        feed_agenda(
            &mut state,
            &serde_json::json!({ "v": 1, "meetings": [] }).to_string(),
            at("2026-07-12T15:00:00Z"),
        );
        let view = state.recompute(at("2026-07-12T15:00:00Z"));
        assert_eq!(view.state, DisplayState::Idle);
        assert_eq!(view.active, None);
        assert!(!view.want_takeover);
    }

    #[test]
    fn no_snapshot_ever_is_idle() {
        let state = MeetingState::new();
        assert_eq!(
            state.recompute(at("2026-07-12T15:00:00Z")).state,
            DisplayState::Idle
        );
    }

    #[test]
    fn phase_ladder_thresholds() {
        let mut state = MeetingState::new();
        // Start 15:00; defaults takeover 120 s, critical 60 s, overdue 60 s.
        let now0 = at("2026-07-12T14:00:00Z");
        feed_agenda(
            &mut state,
            &snapshot("2026-07-12T15:00:00Z", "Design"),
            now0,
        );

        // 10 min before: ambient.
        assert_eq!(
            state.recompute(at("2026-07-12T14:50:00Z")).state,
            DisplayState::Ambient
        );
        // 120 s before: just entered takeover (t == start - takeover is takeover).
        assert_eq!(
            state.recompute(at("2026-07-12T14:58:00Z")).state,
            DisplayState::Takeover
        );
        // 90 s before: still takeover.
        assert_eq!(
            state.recompute(at("2026-07-12T14:58:30Z")).state,
            DisplayState::Takeover
        );
        // 60 s before: critical.
        assert_eq!(
            state.recompute(at("2026-07-12T14:59:00Z")).state,
            DisplayState::Critical
        );
        // At start: critical.
        assert_eq!(
            state.recompute(at("2026-07-12T15:00:00Z")).state,
            DisplayState::Critical
        );
        // 59 s after start: still critical (overdue at +60 s).
        assert_eq!(
            state.recompute(at("2026-07-12T15:00:59Z")).state,
            DisplayState::Critical
        );
        // 60 s after start: overdue.
        assert_eq!(
            state.recompute(at("2026-07-12T15:01:00Z")).state,
            DisplayState::Overdue
        );
    }

    #[test]
    fn takeover_wanted_from_takeover_phase_on() {
        let mut state = MeetingState::new();
        feed_agenda(
            &mut state,
            &snapshot("2026-07-12T15:00:00Z", "Design"),
            at("2026-07-12T14:00:00Z"),
        );
        assert!(!state.recompute(at("2026-07-12T14:50:00Z")).want_takeover); // ambient
        assert!(state.recompute(at("2026-07-12T14:58:00Z")).want_takeover); // takeover
        assert!(state.recompute(at("2026-07-12T14:59:30Z")).want_takeover); // critical
        assert!(state.recompute(at("2026-07-12T15:05:00Z")).want_takeover); // overdue
    }

    #[test]
    fn valid_per_meeting_override_applies() {
        let mut state = MeetingState::new();
        let body = serde_json::json!({
            "v": 1,
            "meetings": [{
                "id": "m1", "start": "2026-07-12T15:00:00Z", "title": "Quick",
                "escalation": { "takeover_secs": 30, "critical_secs": 10, "overdue_secs": 20 },
            }],
        })
        .to_string();
        feed_agenda(&mut state, &body, at("2026-07-12T14:59:00Z"));
        // 40 s before: still ambient under a 30 s takeover.
        assert_eq!(
            state.recompute(at("2026-07-12T14:59:20Z")).state,
            DisplayState::Ambient
        );
        // 25 s before: takeover.
        assert_eq!(
            state.recompute(at("2026-07-12T14:59:35Z")).state,
            DisplayState::Takeover
        );
    }

    #[test]
    fn invalid_override_uses_defaults_and_warns_but_keeps_meeting() {
        let mut state = MeetingState::new();
        // takeover_secs <= critical_secs is invalid.
        let body = serde_json::json!({
            "v": 1,
            "meetings": [{
                "id": "m1", "start": "2026-07-12T15:00:00Z", "title": "Bad",
                "escalation": { "takeover_secs": 30, "critical_secs": 60, "overdue_secs": 60 },
            }],
        })
        .to_string();
        let outcome = feed_agenda(&mut state, &body, at("2026-07-12T14:00:00Z"));
        match outcome {
            IngestOutcome::Accepted { warnings } => {
                assert_eq!(warnings.len(), 1);
                assert!(warnings[0].message.contains("invalid escalation"));
                assert_eq!(warnings[0].level, WarningLevel::Error);
            }
            other => panic!("expected accepted-with-warning, got {other:?}"),
        }
        // Meeting kept; default 120 s takeover applies.
        assert_eq!(
            state.recompute(at("2026-07-12T14:58:00Z")).state,
            DisplayState::Takeover
        );
    }

    #[test]
    fn overdue_override_at_or_past_retire_cap_falls_back_to_defaults() {
        let mut state = MeetingState::new();
        // overdue_secs == RETIRE_AFTER_SECS (3600) would retire the meeting while
        // still Critical, so it is rejected to defaults with a warning.
        let body = serde_json::json!({
            "v": 1,
            "meetings": [{
                "id": "m1", "start": "2026-07-12T15:00:00Z", "title": "Long",
                "escalation": { "takeover_secs": 120, "critical_secs": 60, "overdue_secs": 3600 },
            }],
        })
        .to_string();
        let outcome = feed_agenda(&mut state, &body, at("2026-07-12T14:00:00Z"));
        match outcome {
            IngestOutcome::Accepted { warnings } => assert_eq!(warnings.len(), 1),
            other => panic!("expected accepted-with-warning, got {other:?}"),
        }
        // Default 60 s overdue applies: overdue by 15:01:00, well before the cap.
        assert_eq!(
            state.recompute(at("2026-07-12T15:01:00Z")).state,
            DisplayState::Overdue
        );
    }

    #[test]
    fn dismiss_removes_the_meeting() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:59:00Z");
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);
        assert_eq!(state.recompute(now).state, DisplayState::Critical);
        feed_ack(
            &mut state,
            &dismiss_body(&target("m1", "2026-07-12T15:00:00Z")),
            "2026-07-12T14:59:01Z",
            now,
        );
        assert_eq!(state.recompute(now).state, DisplayState::Idle);
    }

    #[test]
    fn snooze_suppresses_then_re_enters() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:59:00Z"); // 60 s before start
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);
        // Snooze for 5 min → until 15:04:00.
        let until = at("2026-07-12T15:04:00Z");
        feed_ack(
            &mut state,
            &snooze_body(&target("m1", "2026-07-12T15:00:00Z"), until),
            "2026-07-12T14:59:01Z",
            now,
        );
        // Suppressed during the snooze window.
        assert_eq!(
            state.recompute(at("2026-07-12T15:02:00Z")).state,
            DisplayState::Idle
        );
        // After `until` the meeting re-manifests — directly in overdue (started
        // meanwhile).
        assert_eq!(
            state.recompute(at("2026-07-12T15:04:30Z")).state,
            DisplayState::Overdue
        );
    }

    #[test]
    fn local_dismiss_matches_a_received_ack() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:59:00Z");
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);
        state.apply_local_ack(
            &target("m1", "2026-07-12T15:00:00Z"),
            AckAction::Dismiss,
            now,
        );
        assert_eq!(state.recompute(now).state, DisplayState::Idle);
    }

    #[test]
    fn later_escalating_meeting_supersedes_an_overdue_one() {
        let mut state = MeetingState::new();
        let body = serde_json::json!({
            "v": 1,
            "meetings": [
                { "id": "early", "start": "2026-07-12T09:00:00Z", "title": "Standup" },
                { "id": "late", "start": "2026-07-12T15:00:00Z", "title": "Review" },
            ],
        })
        .to_string();
        // 09:30 — early is overdue but within the 1 h cap; late is far away.
        let t1 = at("2026-07-12T09:30:00Z");
        feed_agenda(&mut state, &body, t1);
        assert_eq!(active_id(&state, t1).as_deref(), Some("early"));
        // 14:58 — late has entered its takeover window; it supersedes the overdue
        // early one. (early is also past its 1 h cap here, but supersession alone
        // suffices.)
        let t2 = at("2026-07-12T14:58:00Z");
        assert_eq!(active_id(&state, t2).as_deref(), Some("late"));
        assert_eq!(state.recompute(t2).state, DisplayState::Takeover);
    }

    #[test]
    fn overdue_1h_cap_retires_the_meeting() {
        let mut state = MeetingState::new();
        let now0 = at("2026-07-12T15:00:00Z");
        feed_agenda(
            &mut state,
            &snapshot("2026-07-12T15:00:00Z", "Design"),
            now0,
        );
        // 59 min after start: still overdue.
        assert_eq!(
            state.recompute(at("2026-07-12T15:59:00Z")).state,
            DisplayState::Overdue
        );
        // Exactly 1 h after: retired → idle.
        assert_eq!(
            state.recompute(at("2026-07-12T16:00:00Z")).state,
            DisplayState::Idle
        );
    }

    #[test]
    fn earliest_of_two_future_meetings_is_active() {
        let mut state = MeetingState::new();
        let body = serde_json::json!({
            "v": 1,
            "meetings": [
                { "id": "b", "start": "2026-07-12T16:00:00Z", "title": "Later" },
                { "id": "a", "start": "2026-07-12T15:00:00Z", "title": "Sooner" },
            ],
        })
        .to_string();
        let now = at("2026-07-12T13:00:00Z");
        feed_agenda(&mut state, &body, now);
        assert_eq!(active_id(&state, now).as_deref(), Some("a"));
    }

    #[test]
    fn malformed_snapshot_keeps_last_good() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:00:00Z");
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);
        // Bad JSON.
        let outcome = feed_agenda(&mut state, "{not json", now);
        assert!(matches!(outcome, IngestOutcome::Malformed(_)));
        assert_eq!(state.faults(), 1);
        // Last-good snapshot survives.
        assert_eq!(
            state.recompute(at("2026-07-12T14:58:00Z")).state,
            DisplayState::Takeover
        );
    }

    #[test]
    fn duplicate_id_is_a_fault() {
        let mut state = MeetingState::new();
        let body = serde_json::json!({
            "v": 1,
            "meetings": [
                { "id": "dup", "start": "2026-07-12T15:00:00Z", "title": "A" },
                { "id": "dup", "start": "2026-07-12T16:00:00Z", "title": "B" },
            ],
        })
        .to_string();
        let outcome = feed_agenda(&mut state, &body, at("2026-07-12T14:00:00Z"));
        match outcome {
            IngestOutcome::Malformed(r) => assert!(r.reason.contains("duplicate")),
            other => panic!("expected malformed, got {other:?}"),
        }
    }

    #[test]
    fn missing_required_field_is_a_fault() {
        let mut state = MeetingState::new();
        // Missing title.
        let body = serde_json::json!({
            "v": 1,
            "meetings": [{ "id": "m1", "start": "2026-07-12T15:00:00Z" }],
        })
        .to_string();
        assert!(matches!(
            feed_agenda(&mut state, &body, at("2026-07-12T14:00:00Z")),
            IngestOutcome::Malformed(_)
        ));
    }

    #[test]
    fn unparseable_start_is_a_fault() {
        let mut state = MeetingState::new();
        let body = serde_json::json!({
            "v": 1,
            "meetings": [{ "id": "m1", "start": "soon", "title": "X" }],
        })
        .to_string();
        assert!(matches!(
            feed_agenda(&mut state, &body, at("2026-07-12T14:00:00Z")),
            IngestOutcome::Malformed(_)
        ));
    }

    #[test]
    fn unknown_snapshot_fields_ignored() {
        let mut state = MeetingState::new();
        let body = serde_json::json!({
            "v": 1,
            "future": "field",
            "meetings": [{
                "id": "m1", "start": "2026-07-12T15:00:00Z", "title": "X", "room": "3B",
            }],
        })
        .to_string();
        assert!(matches!(
            feed_agenda(&mut state, &body, at("2026-07-12T14:00:00Z")),
            IngestOutcome::Accepted { .. }
        ));
    }

    #[test]
    fn malformed_ack_is_a_fault() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:00:00Z");
        // snooze without until.
        let body =
            serde_json::json!({ "v": 1, "meeting_id": "m1", "action": "snooze" }).to_string();
        let outcome = feed_ack(&mut state, &body, "2026-07-12T14:00:00Z", now);
        assert!(matches!(outcome, IngestOutcome::Malformed(_)));
        // unknown action.
        let body = serde_json::json!({ "meeting_id": "m1", "action": "poke" }).to_string();
        assert!(matches!(
            feed_ack(&mut state, &body, "2026-07-12T14:00:00Z", now),
            IngestOutcome::Malformed(_)
        ));
    }

    #[test]
    fn ack_before_its_meeting_still_suppresses_when_the_snapshot_arrives() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:59:00Z");
        // Ack arrives first (cross-channel replay order).
        feed_ack(
            &mut state,
            &dismiss_body(&target("m1", "2026-07-12T15:00:00Z")),
            "2026-07-12T14:58:00Z",
            now,
        );
        // Then the snapshot naming m1.
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);
        assert_eq!(state.recompute(now).state, DisplayState::Idle);
    }

    #[test]
    fn latest_ack_wins_by_publish_ts() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:59:00Z");
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);
        // A newer dismiss, then an older snooze replayed — the dismiss must win.
        feed_ack(
            &mut state,
            &dismiss_body(&target("m1", "2026-07-12T15:00:00Z")),
            "2026-07-12T14:59:10Z",
            now,
        );
        feed_ack(
            &mut state,
            &snooze_body(
                &target("m1", "2026-07-12T15:00:00Z"),
                at("2026-07-12T15:10:00Z"),
            ),
            "2026-07-12T14:59:05Z",
            now,
        );
        assert_eq!(state.recompute(now).state, DisplayState::Idle);
    }

    #[test]
    fn stale_ack_for_absent_meeting_is_pruned() {
        let mut state = MeetingState::new();
        let now = at("2026-07-13T15:00:00Z");
        // A dismiss for a meeting not in any snapshot, published > 24 h ago.
        feed_ack(
            &mut state,
            &dismiss_body(&target("ghost", "2026-07-12T10:30:00Z")),
            "2026-07-12T10:00:00Z",
            now,
        );
        // Now a snapshot with the same id and a fresh start — the stale ghost ack
        // was pruned, so the meeting is not suppressed.
        feed_agenda(&mut state, &snapshot("2026-07-13T15:00:30Z", "Ghost"), now);
        assert_ne!(state.recompute(now).state, DisplayState::Idle);
    }

    #[test]
    fn recent_ack_for_absent_meeting_is_retained() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T15:00:00Z");
        // A dismiss for a not-yet-seen meeting, published just now.
        feed_ack(
            &mut state,
            &dismiss_body(&target("m1", "2026-07-12T15:00:30Z")),
            "2026-07-12T14:59:00Z",
            now,
        );
        // The snapshot arrives later — the recent ack still suppresses.
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:30Z", "M"), now);
        assert_eq!(state.recompute(now).state, DisplayState::Idle);
    }

    #[test]
    fn a_reused_id_with_a_new_start_is_not_suppressed_by_the_earlier_dismissal() {
        let mut state = MeetingState::new();
        let day1 = at("2026-07-12T14:59:00Z");
        feed_agenda(
            &mut state,
            &snapshot("2026-07-12T15:00:00Z", "Standup"),
            day1,
        );
        feed_ack(
            &mut state,
            &dismiss_body(&target("m1", "2026-07-12T15:00:00Z")),
            "2026-07-12T14:59:01Z",
            day1,
        );
        assert_eq!(state.recompute(day1).state, DisplayState::Idle);

        // Tomorrow's occurrence of the same id, still inside the 24 h window so the
        // ack is stored — occurrence scoping alone must let the meeting through.
        let day2 = at("2026-07-13T14:58:00Z");
        feed_agenda(
            &mut state,
            &snapshot("2026-07-13T15:00:00Z", "Standup"),
            day2,
        );
        let view = state.recompute(day2);
        assert_eq!(view.state, DisplayState::Takeover);
        assert!(view.want_takeover);
        assert_eq!(active_id(&state, day2).as_deref(), Some("m1"));
    }

    #[test]
    fn the_acked_occurrence_stays_suppressed_across_re_delivery() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:59:00Z");
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);
        let body = dismiss_body(&target("m1", "2026-07-12T15:00:00Z"));
        feed_ack(&mut state, &body, "2026-07-12T14:59:01Z", now);
        // The ack channel replays it; the same occurrence stays suppressed.
        feed_ack(&mut state, &body, "2026-07-12T14:59:01Z", now);
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);
        assert_eq!(state.recompute(now).state, DisplayState::Idle);
    }

    #[test]
    fn snooze_is_occurrence_scoped_too() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:59:00Z");
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);
        feed_ack(
            &mut state,
            &snooze_body(
                &target("m1", "2026-07-12T15:00:00Z"),
                at("2026-07-12T15:30:00Z"),
            ),
            "2026-07-12T14:59:01Z",
            now,
        );
        assert_eq!(state.recompute(now).state, DisplayState::Idle);
        // The meeting is rescheduled inside the snooze window: a new occurrence, so
        // the snooze does not carry over.
        feed_agenda(&mut state, &snapshot("2026-07-12T15:20:00Z", "Design"), now);
        assert_eq!(state.recompute(now).state, DisplayState::Ambient);
    }

    #[test]
    fn an_ack_without_a_parseable_start_is_dropped_with_a_warning() {
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:59:00Z");
        feed_agenda(&mut state, &snapshot("2026-07-12T15:00:00Z", "Design"), now);

        // No start at all — the shape published before occurrence scoping.
        let body =
            serde_json::json!({ "v": 1, "meeting_id": "m1", "action": "dismiss" }).to_string();
        match feed_ack(&mut state, &body, "2026-07-12T14:59:01Z", now) {
            IngestOutcome::Accepted { warnings } => {
                assert_eq!(warnings.len(), 1);
                assert!(
                    warnings[0].message.contains("no start"),
                    "{}",
                    warnings[0].message
                );
                // Warn, not error: the expected shape of every pre-scoping ack
                // the ring still replays, and the error channel stays clean.
                assert_eq!(warnings[0].level, WarningLevel::Warn);
            }
            other => panic!("expected accepted-with-warning, got {other:?}"),
        }
        // An unparseable start is dropped the same way.
        let body = serde_json::json!({
            "v": 1, "meeting_id": "m1", "start": "soon", "action": "dismiss",
        })
        .to_string();
        match feed_ack(&mut state, &body, "2026-07-12T14:59:02Z", now) {
            IngestOutcome::Accepted { warnings } => {
                assert_eq!(warnings.len(), 1);
                assert!(
                    warnings[0].message.contains("invalid start"),
                    "{}",
                    warnings[0].message
                );
                assert_eq!(warnings[0].level, WarningLevel::Warn);
            }
            other => panic!("expected accepted-with-warning, got {other:?}"),
        }
        // Neither is a publisher fault, and neither suppresses anything.
        assert_eq!(state.faults(), 0);
        assert_eq!(state.recompute(now).state, DisplayState::Critical);
    }

    #[test]
    fn a_newer_ack_evicts_the_earlier_occurrences_dismissal() {
        // The load-bearing simplification of occurrence scoping: acks stay keyed
        // by id, one record each, because two occurrences of one id never coexist
        // as candidates. The cost, pinned here so it is a decision rather than a
        // surprise: dismiss 15:00, watch it move to 16:00, dismiss that too, and
        // the 15:00 dismissal is gone — a move back to 15:00 alarms again.
        let mut state = MeetingState::new();
        let now = at("2026-07-12T14:00:00Z");
        feed_agenda(
            &mut state,
            &snapshot("2026-07-12T15:00:00Z", "Standup"),
            now,
        );
        feed_ack(
            &mut state,
            &dismiss_body(&target("m1", "2026-07-12T15:00:00Z")),
            "2026-07-12T14:00:01Z",
            now,
        );
        feed_agenda(
            &mut state,
            &snapshot("2026-07-12T16:00:00Z", "Standup"),
            now,
        );
        feed_ack(
            &mut state,
            &dismiss_body(&target("m1", "2026-07-12T16:00:00Z")),
            "2026-07-12T14:00:02Z",
            now,
        );
        assert_eq!(state.recompute(now).state, DisplayState::Idle);

        // Moved back: the record now names 16:00, so the returning 15:00
        // occurrence is unsuppressed and escalates on its own schedule.
        let back = at("2026-07-12T14:58:00Z");
        feed_agenda(
            &mut state,
            &snapshot("2026-07-12T15:00:00Z", "Standup"),
            back,
        );
        assert_eq!(state.recompute(back).state, DisplayState::Takeover);
    }

    #[test]
    fn an_occurrence_mismatched_ack_ages_out_while_its_id_stays_present() {
        // Inside 24 h the ack survives snapshots of other occurrences: returning to
        // the acked occurrence is still suppressed.
        let mut fresh = MeetingState::new();
        feed_ack(
            &mut fresh,
            &dismiss_body(&target("m1", "2026-07-20T15:00:00Z")),
            "2026-07-12T10:00:00Z",
            at("2026-07-12T10:00:00Z"),
        );
        feed_agenda(
            &mut fresh,
            &snapshot("2026-07-21T15:00:00Z", "Standup"),
            at("2026-07-12T20:00:00Z"),
        );
        feed_agenda(
            &mut fresh,
            &snapshot("2026-07-20T15:00:00Z", "Standup"),
            at("2026-07-12T21:00:00Z"),
        );
        assert_eq!(
            fresh.recompute(at("2026-07-12T21:00:00Z")).state,
            DisplayState::Idle
        );

        // Past 24 h, a snapshot carrying only other occurrences of the same id no
        // longer exempts the ack from the staleness cap.
        let mut aged = MeetingState::new();
        feed_ack(
            &mut aged,
            &dismiss_body(&target("m1", "2026-07-20T15:00:00Z")),
            "2026-07-12T10:00:00Z",
            at("2026-07-12T10:00:00Z"),
        );
        feed_agenda(
            &mut aged,
            &snapshot("2026-07-21T15:00:00Z", "Standup"),
            at("2026-07-13T11:00:00Z"),
        );
        feed_agenda(
            &mut aged,
            &snapshot("2026-07-20T15:00:00Z", "Standup"),
            at("2026-07-13T12:00:00Z"),
        );
        assert_eq!(
            aged.recompute(at("2026-07-13T12:00:00Z")).state,
            DisplayState::Ambient
        );
    }

    #[test]
    fn a_matching_occurrence_ack_is_retained_past_the_stale_cap() {
        let mut state = MeetingState::new();
        feed_agenda(
            &mut state,
            &snapshot("2026-07-20T15:00:00Z", "Standup"),
            at("2026-07-12T10:00:00Z"),
        );
        feed_ack(
            &mut state,
            &dismiss_body(&target("m1", "2026-07-20T15:00:00Z")),
            "2026-07-12T10:00:00Z",
            at("2026-07-12T10:00:00Z"),
        );
        // Two days on, the same occurrence is still in the snapshot: a legitimately
        // long-lived dismissal survives.
        let later = at("2026-07-14T10:00:00Z");
        feed_agenda(
            &mut state,
            &snapshot("2026-07-20T15:00:00Z", "Standup"),
            later,
        );
        assert_eq!(state.recompute(later).state, DisplayState::Idle);
    }

    #[test]
    fn countdown_formatting() {
        assert_eq!(format_countdown(120), "2:00");
        assert_eq!(format_countdown(5), "0:05");
        assert_eq!(format_countdown(-90), "+1:30");
        assert_eq!(format_countdown(3661), "1:01:01");
        assert_eq!(format_countdown(0), "0:00");
    }

    #[test]
    fn subline_formatting() {
        assert_eq!(format_subline(120), "in 2 min");
        assert_eq!(format_subline(1), "in 1 min");
        assert_eq!(format_subline(-90), "2 min ago");
    }

    #[test]
    fn near_meeting_ticks_fast_far_ticks_coarse() {
        let mut state = MeetingState::new();
        feed_agenda(
            &mut state,
            &snapshot("2026-07-12T15:00:00Z", "Design"),
            at("2026-07-12T10:00:00Z"),
        );
        // 5 h away: coarse.
        assert_eq!(
            state.recompute(at("2026-07-12T10:00:00Z")).next_tick_secs,
            60
        );
        // 10 min away: fast.
        assert_eq!(
            state.recompute(at("2026-07-12T14:50:00Z")).next_tick_secs,
            1
        );
        // Idle: coarse.
        let empty = MeetingState::new();
        assert_eq!(
            empty.recompute(at("2026-07-12T14:50:00Z")).next_tick_secs,
            60
        );
    }

    #[test]
    fn wrong_port_is_a_contract_violation() {
        let mut state = MeetingState::new();
        assert_eq!(
            state.on_message(
                "messages",
                &envelope_json(
                    &snapshot("2026-07-12T15:00:00Z", "X"),
                    "2026-07-12T00:00:00Z"
                ),
                at("2026-07-12T14:00:00Z"),
            ),
            Err(ContractViolation::WrongPort {
                port: "messages".to_string()
            })
        );
    }

    #[test]
    fn unparseable_envelope_is_a_contract_violation() {
        let mut state = MeetingState::new();
        assert!(matches!(
            state.on_message("agenda", "not json", at("2026-07-12T14:00:00Z")),
            Err(ContractViolation::BadEnvelope(_))
        ));
    }
}

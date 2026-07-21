//! DOM-free protobar display state machine.
//!
//! Every branch here is host-tested. The wasm DOM glue converts each `Err`
//! into a panic (operator misconfig or shell/proto version skew), which the
//! module's panic hook turns into an error card — rendering anyway would mask
//! the fault. See `on_message`/`on_drops` for the contract each carries.
//!
//! A body on the `messages` port is interpreted as either **bare text** or a
//! **structured** JSON object `{text, priority, expires_at, format}`. A body is
//! structured only when it is a JSON object whose top level carries at least one
//! convention key — `text`, `priority`, `expires_at`, or `format`; these four
//! names are **reserved discriminators** on protobar-bound channels, so a
//! publisher whose natural JSON uses one of them at top level is claimed by the
//! convention. Everything else — a parse failure, a JSON non-object, or a JSON
//! object with no convention key — is bare text, rendered verbatim (for a JSON
//! object, the body string as delivered). Structured bodies land in one slot per
//! priority level; the highest-priority live (unexpired) slot displays. A body
//! claimed by the convention that then violates the schema is a semi-trusted
//! publisher fault: it leaves the slots untouched, bumps a page-lifetime
//! counter, and is reported to the operator log — never a panic.

use brenn_envelope::{MessageEnvelope, Urgency};
use brenn_surface_component_support::FaultReport;
use chrono::{DateTime, Utc};

use crate::markdown::{self, Block};

/// The config-bound input port name. A `[[surface.subscription]] port` must
/// match this string, or `on_message` rejects the delivery.
const INPUT_PORT: &str = "messages";

/// Number of priority slots — one per [`Urgency`] level, derived from
/// [`Urgency::ALL`] so the two never drift.
const SLOT_COUNT: usize = Urgency::ALL.len();

/// Rendered display: the message as a block tree the DOM glue walks, and the
/// status line it writes via `textContent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Display {
    /// The block tree for the message area. Plain and bare bodies are one
    /// paragraph; `format: "markdown"` bodies are the parsed markdown tree; the
    /// all-slots-empty/expired state is empty (`vec![]`).
    pub message: Vec<Block>,
    pub status_text: String,
    /// Priority of the displayed message, for the `data-priority` styling hook.
    /// `None` in the "awaiting data" and all-slots-empty/expired states, where
    /// no message occupies the bar.
    pub priority: Option<Urgency>,
}

/// A rejected port event. The DOM glue panics on any of these.
#[derive(Debug, Clone, PartialEq)]
pub enum ContractViolation {
    /// Event arrived on a port other than `messages`.
    WrongPort { port: String },
    /// `envelope_json` did not parse as a `MessageEnvelope`.
    BadEnvelope(String),
}

/// The outcome of an accepted `messages` delivery. `Malformed` is a publisher
/// fault, not a `ContractViolation`: the delivery was well-formed at the wire
/// boundary but its body violated the body convention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ingest {
    /// Body parsed (bare text or valid structured) and occupied its slot.
    Accepted,
    /// Body was a JSON object that violated the schema. Slots untouched; the
    /// report carries what the DOM glue needs for a `COMPONENT_LOG` error.
    Malformed(FaultReport),
}

/// One priority slot's occupant. The slot's priority is its index in
/// [`ProtobarState::slots`]. The body is parsed to its block tree once, at
/// store time, so `display` is a clone rather than a re-parse.
#[derive(Debug, Clone)]
struct SlotEntry {
    message: Vec<Block>,
    /// Display lifetime. `None` persists until replaced.
    expires_at: Option<DateTime<Utc>>,
}

impl SlotEntry {
    /// Live at `now` when it has no expiry or its expiry is still in the future.
    fn is_live(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_none_or(|exp| now < exp)
    }
}

/// A body parsed against the convention: bare text, valid structured, or a
/// schema-violating object.
enum ParsedBody {
    BareText(String),
    Structured {
        text: String,
        priority: Urgency,
        expires_at: Option<DateTime<Utc>>,
        /// `format == "markdown"` (else the text renders as one plain paragraph).
        markdown: bool,
    },
    Malformed(String),
}

/// Raw structured body as serde sees it — fields kept as strings so validation
/// (unknown `priority`/`format`, unparseable `expires_at`) is explicit and
/// produces a precise malformed reason rather than a generic serde error.
/// Unknown fields are ignored (no `deny_unknown_fields`): this is a de-facto
/// external contract that evolves additively.
#[derive(serde::Deserialize)]
struct RawBody {
    text: String,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    expires_at: Option<String>,
    #[serde(default)]
    format: Option<String>,
}

/// The reserved convention keys. A JSON object opts into the structured
/// convention only if its top level carries at least one of these; a publisher
/// that used any of them was unambiguously speaking the convention, so it is
/// held to the full schema (a typo like `{"priority": "high", "txt": ".."}`
/// stays malformed rather than silently downgrading to verbatim JSON).
const CONVENTION_KEYS: [&str; 4] = ["text", "priority", "expires_at", "format"];

impl ParsedBody {
    fn parse(body: &str) -> Self {
        // A JSON object opts into the structured convention only when its top
        // level carries at least one convention key. A parse failure, any
        // non-object JSON value (string/number/array/bool/null), or an object
        // with no convention key is bare text rendered verbatim — so every
        // existing plain-text and plain-JSON publisher is unchanged.
        match serde_json::from_str::<serde_json::Value>(body) {
            Ok(serde_json::Value::Object(map))
                if CONVENTION_KEYS.iter().any(|k| map.contains_key(*k)) =>
            {
                // Deserialize from the `Value` already in hand rather than
                // re-lexing the (up to 64 KiB) body a second time. `from_value`
                // errors lack a line/column, but bodies are single-line JSON and
                // the field-level reason is what the malformed report needs.
                match serde_json::from_value::<RawBody>(serde_json::Value::Object(map)) {
                    Ok(raw) => Self::validate(raw),
                    Err(e) => ParsedBody::Malformed(e.to_string()),
                }
            }
            _ => ParsedBody::BareText(body.to_string()),
        }
    }

    fn validate(raw: RawBody) -> Self {
        let priority = match raw.priority.as_deref() {
            None => Urgency::Normal,
            Some(s) => match Urgency::parse(s) {
                Some(u) => u,
                None => return ParsedBody::Malformed(format!("unrecognized priority {s:?}")),
            },
        };
        let markdown = match raw.format.as_deref() {
            None | Some("plain") => false,
            Some("markdown") => true,
            Some(other) => {
                return ParsedBody::Malformed(format!("unrecognized format {other:?}"));
            }
        };
        let expires_at = match raw.expires_at.as_deref() {
            None => None,
            Some(s) => match DateTime::parse_from_rfc3339(s) {
                Ok(dt) => Some(dt.with_timezone(&Utc)),
                Err(e) => return ParsedBody::Malformed(format!("invalid expires_at: {e}")),
            },
        };
        ParsedBody::Structured {
            text: raw.text,
            priority,
            expires_at,
            markdown,
        }
    }
}

/// Protobar display state. One slot per priority level; drops and the malformed
/// counter accumulate for the page lifetime.
#[derive(Debug, Default)]
pub struct ProtobarState {
    slots: [Option<SlotEntry>; SLOT_COUNT],
    drops: u64,
    malformed: u64,
}

impl ProtobarState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Handle a `messages`-port delivery. Rejects a wrong port and an
    /// unparseable envelope (both `ContractViolation`, panic-worthy skew). A
    /// well-formed envelope whose body violates the convention returns
    /// `Ingest::Malformed` — slots untouched, counter bumped — rather than
    /// failing: one buggy publisher must not brick a bar showing other
    /// publishers' live messages.
    pub fn on_message(
        &mut self,
        port: &str,
        envelope_json: &str,
        now: DateTime<Utc>,
    ) -> Result<Ingest, ContractViolation> {
        self.check_port(port)?;
        let envelope: MessageEnvelope = serde_json::from_str(envelope_json)
            .map_err(|e| ContractViolation::BadEnvelope(e.to_string()))?;
        match ParsedBody::parse(&envelope.body) {
            ParsedBody::Malformed(reason) => {
                self.malformed += 1;
                Ok(Ingest::Malformed(FaultReport::new(&envelope, reason)))
            }
            ParsedBody::BareText(text) => {
                self.store(Urgency::Normal, markdown::plain(&text), None, now);
                Ok(Ingest::Accepted)
            }
            ParsedBody::Structured {
                text,
                priority,
                expires_at,
                markdown,
            } => {
                // Parse once, here, so `display` never re-parses and every
                // security-relevant decision stays in host-tested code.
                let message = if markdown {
                    markdown::parse(&text)
                } else {
                    markdown::plain(&text)
                };
                self.store(priority, message, expires_at, now);
                Ok(Ingest::Accepted)
            }
        }
    }

    /// Occupy a priority's slot, replacing its previous occupant. An accepted
    /// message ends "awaiting data" even if it is already expired.
    fn store(
        &mut self,
        priority: Urgency,
        message: Vec<Block>,
        expires_at: Option<DateTime<Utc>>,
        _now: DateTime<Utc>,
    ) {
        let entry = SlotEntry {
            message,
            expires_at,
        };
        *self
            .slots
            .get_mut(priority.rank() as usize)
            .expect("Urgency rank within SLOT_COUNT: a new Urgency level needs a protobar slot") =
            Some(entry);
    }

    /// Fold a window's delivery loss in. Rejects a wrong port. Drops accumulate
    /// and never auto-clear — a diagnostic counter.
    ///
    /// The lost messages are not gone and this is not a staleness signal: a
    /// message dropped from the port's queue is still visible as retained context
    /// in this or any later window that retention covers, so the display
    /// reconverges on its own. The counter says only "delivery lost some", which
    /// is a diagnostic, not a display state.
    pub fn on_drops(&mut self, port: &str, count: u64) -> Result<(), ContractViolation> {
        self.check_port(port)?;
        self.drops = self
            .drops
            .checked_add(count)
            .expect("drop counter overflow");
        Ok(())
    }

    /// Current rendered display at `now`: the highest-priority live slot, or the
    /// "awaiting data"/empty fallback, plus the status line.
    pub fn display(&self, now: DateTime<Utc>) -> Display {
        let (message, priority) = match self.live_slot(now) {
            Some((urgency, entry)) => (entry.message.clone(), Some(urgency)),
            // Accepted at least one message (a slot is occupied, even if expired)
            // but nothing live: blank bar, deliberately distinct from the
            // pre-first-message "awaiting data".
            None if self.any_accepted() => (Vec::new(), None),
            None => (markdown::plain("awaiting data"), None),
        };
        Display {
            message,
            status_text: self.status_text(),
            priority,
        }
    }

    /// The earliest future expiry among occupied slots, or `None` if no slot has
    /// a future expiry. Drives the DOM glue's re-render timer; a past expiry is
    /// skipped (its slot is already filtered out of `display`).
    pub fn next_expiry(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.slots
            .iter()
            .flatten()
            .filter_map(|entry| entry.expires_at)
            .filter(|&exp| exp > now)
            .min()
    }

    /// The highest-priority slot that is occupied and live at `now`. Iterates
    /// [`Urgency::ALL`] (ascending rank) in reverse so a new level is picked up
    /// from the single canonical list rather than a hand-copied array.
    fn live_slot(&self, now: DateTime<Utc>) -> Option<(Urgency, &SlotEntry)> {
        Urgency::ALL.into_iter().rev().find_map(|urgency| {
            self.slots[urgency.rank() as usize]
                .as_ref()
                .filter(|entry| entry.is_live(now))
                .map(|entry| (urgency, entry))
        })
    }

    /// Whether any message has been accepted. A `store` occupies a slot and slots
    /// are never emptied (expiry filters at read time), so an occupied slot is the
    /// single source of truth for "past the pre-first-message state".
    fn any_accepted(&self) -> bool {
        self.slots.iter().any(Option::is_some)
    }

    fn check_port(&self, port: &str) -> Result<(), ContractViolation> {
        if port == INPUT_PORT {
            Ok(())
        } else {
            Err(ContractViolation::WrongPort {
                port: port.to_string(),
            })
        }
    }

    fn status_text(&self) -> String {
        let mut parts = Vec::new();
        if self.drops > 0 {
            parts.push(format!("dropped: {}", self.drops));
        }
        if self.malformed > 0 {
            parts.push(format!("malformed: {}", self.malformed));
        }
        parts.join(" · ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_surface_test_fixtures::sample_envelope_json;

    /// A fixed render clock for the slot/expiry tests.
    fn now() -> DateTime<Utc> {
        "2026-07-08T12:00:00Z".parse().unwrap()
    }

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    /// The visible text of a display's message tree, for assertions that only
    /// care about what the bar reads as. Delegates to the one shared block-tree
    /// text walker (`markdown::all_text`) so logic- and markdown-level tests never
    /// diverge on how containers are flattened.
    fn rendered_text(display: &Display) -> String {
        crate::markdown::all_text(&display.message)
    }

    /// A structured body as wire JSON text, wrapped in the sample envelope.
    fn structured(fields: serde_json::Value) -> String {
        sample_envelope_json(&fields.to_string())
    }

    #[test]
    fn awaiting_data_before_first_message() {
        let state = ProtobarState::new();
        let display = state.display(now());
        assert_eq!(rendered_text(&display), "awaiting data");
        assert_eq!(display.status_text, "");
        assert_eq!(display.priority, None);
    }

    #[test]
    fn bare_text_lands_in_normal_slot_verbatim() {
        let mut state = ProtobarState::new();
        assert_eq!(
            state.on_message("messages", &sample_envelope_json("hello world"), now()),
            Ok(Ingest::Accepted)
        );
        let display = state.display(now());
        assert_eq!(rendered_text(&display), "hello world");
        assert_eq!(display.priority, Some(Urgency::Normal));
    }

    #[test]
    fn json_non_objects_are_bare_text() {
        // A JSON string/number/array/bool/null body does not opt into the
        // structured convention: it renders verbatim as text.
        for raw in [r#""hi""#, "42", "[1,2]", "true", "null"] {
            let mut state = ProtobarState::new();
            assert_eq!(
                state.on_message("messages", &sample_envelope_json(raw), now()),
                Ok(Ingest::Accepted)
            );
            assert_eq!(rendered_text(&state.display(now())), raw);
        }
    }

    #[test]
    fn empty_body_renders_empty_not_awaiting() {
        let mut state = ProtobarState::new();
        state
            .on_message("messages", &sample_envelope_json(""), now())
            .unwrap();
        assert_eq!(rendered_text(&state.display(now())), "");
    }

    #[test]
    fn html_looking_body_passes_through_verbatim() {
        let mut state = ProtobarState::new();
        let body = "<script>alert('x')</script>";
        state
            .on_message("messages", &sample_envelope_json(body), now())
            .unwrap();
        assert_eq!(rendered_text(&state.display(now())), body);
    }

    #[test]
    fn json_object_without_convention_key_displays_verbatim() {
        // A JSON object with no convention key is not claimed by the convention:
        // it renders verbatim (the body string as delivered), no wrapping needed.
        let mut state = ProtobarState::new();
        let body = serde_json::json!({ "a": 1 });
        state
            .on_message("messages", &structured(body.clone()), now())
            .unwrap();
        assert_eq!(rendered_text(&state.display(now())), body.to_string());
    }

    #[test]
    fn convention_free_objects_are_bare_text_verbatim() {
        // An empty object and a `ReportBody`-shaped object (the S8 durabar
        // republish body — top level {surface, session, user, ts, client}, no
        // convention key) render verbatim in the normal slot rather than being
        // claimed as malformed structured bodies.
        for body in [
            serde_json::json!({}),
            serde_json::json!({
                "surface": "deskbar",
                "session": "s1",
                "user": "u1",
                "ts": "2026-07-08T12:00:00Z",
                "client": { "message": "boom" },
            }),
        ] {
            let mut state = ProtobarState::new();
            assert_eq!(
                state.on_message("messages", &structured(body.clone()), now()),
                Ok(Ingest::Accepted)
            );
            let display = state.display(now());
            assert_eq!(rendered_text(&display), body.to_string());
            assert_eq!(display.priority, Some(Urgency::Normal));
        }
    }

    #[test]
    fn json_object_with_convention_key_must_be_wrapped_to_display_literally() {
        // An object that *does* carry a convention key is claimed; to display it
        // literally a publisher wraps the JSON string in `text`.
        let mut state = ProtobarState::new();
        let literal = r#"{"text":"hi","priority":"high"}"#;
        let wrapped = structured(serde_json::json!({ "text": literal }));
        state.on_message("messages", &wrapped, now()).unwrap();
        assert_eq!(rendered_text(&state.display(now())), literal);
    }

    #[test]
    fn unknown_fields_ignored() {
        let mut state = ProtobarState::new();
        let body = structured(serde_json::json!({ "text": "hi", "future": "field" }));
        assert_eq!(
            state.on_message("messages", &body, now()),
            Ok(Ingest::Accepted)
        );
        assert_eq!(rendered_text(&state.display(now())), "hi");
    }

    #[test]
    fn markdown_format_parses_to_blocks() {
        let mut state = ProtobarState::new();
        let body = structured(serde_json::json!({
            "text": "## Title\n\n- a\n- b",
            "format": "markdown",
        }));
        state.on_message("messages", &body, now()).unwrap();
        let display = state.display(now());
        // A markdown body produces real block structure (a heading + a list),
        // not one plain paragraph.
        assert!(
            display
                .message
                .iter()
                .any(|b| matches!(b, Block::Heading { level: 2, .. })),
            "expected an h2 block, got {:?}",
            display.message
        );
        assert!(
            display
                .message
                .iter()
                .any(|b| matches!(b, Block::List { .. })),
            "expected a list block, got {:?}",
            display.message
        );
    }

    #[test]
    fn plain_format_does_not_parse_markdown() {
        let mut state = ProtobarState::new();
        // `format: "plain"` (and bare text) render the source verbatim in one
        // paragraph — a leading `#` must not become a heading.
        let body = structured(serde_json::json!({
            "text": "# not a heading",
            "format": "plain",
        }));
        state.on_message("messages", &body, now()).unwrap();
        let display = state.display(now());
        assert_eq!(
            display.message,
            vec![Block::Paragraph(vec![crate::markdown::Inline::Text(
                "# not a heading".to_string()
            )])]
        );
    }

    #[test]
    fn priority_selects_highest_live_slot() {
        let mut state = ProtobarState::new();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "text": "low", "priority": "low" })),
                now(),
            )
            .unwrap();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "text": "high", "priority": "high" })),
                now(),
            )
            .unwrap();
        let display = state.display(now());
        assert_eq!(rendered_text(&display), "high");
        assert_eq!(display.priority, Some(Urgency::High));
    }

    #[test]
    fn same_priority_replaces_slot() {
        let mut state = ProtobarState::new();
        for text in ["first", "second"] {
            state
                .on_message(
                    "messages",
                    &structured(serde_json::json!({ "text": text, "priority": "high" })),
                    now(),
                )
                .unwrap();
        }
        assert_eq!(rendered_text(&state.display(now())), "second");
    }

    #[test]
    fn expired_slot_reveals_lower_priority() {
        let mut state = ProtobarState::new();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "text": "normal", "priority": "normal" })),
                now(),
            )
            .unwrap();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({
                    "text": "alert",
                    "priority": "high",
                    "expires_at": "2026-07-08T12:00:30Z",
                })),
                now(),
            )
            .unwrap();
        // While the alert is live it wins.
        assert_eq!(rendered_text(&state.display(now())), "alert");
        // After it expires the normal slot shows through.
        assert_eq!(
            rendered_text(&state.display(at("2026-07-08T12:01:00Z"))),
            "normal"
        );
    }

    #[test]
    fn expired_on_arrival_never_displays_but_ends_awaiting() {
        let mut state = ProtobarState::new();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({
                    "text": "stale",
                    "expires_at": "2020-01-01T00:00:00Z",
                })),
                now(),
            )
            .unwrap();
        let display = state.display(now());
        // Never displays, but the bar is no longer "awaiting data".
        assert_eq!(rendered_text(&display), "");
        assert_eq!(display.priority, None);
    }

    #[test]
    fn retraction_via_past_expiry_reveals_lower_slot() {
        let mut state = ProtobarState::new();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "text": "normal", "priority": "normal" })),
                now(),
            )
            .unwrap();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "text": "alert", "priority": "high" })),
                now(),
            )
            .unwrap();
        assert_eq!(rendered_text(&state.display(now())), "alert");
        // Retraction idiom: same-priority replacement with a past expiry.
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({
                    "text": "cleared",
                    "priority": "high",
                    "expires_at": "2020-01-01T00:00:00Z",
                })),
                now(),
            )
            .unwrap();
        assert_eq!(rendered_text(&state.display(now())), "normal");
    }

    #[test]
    fn empty_text_blanks_the_bar_it_does_not_retract() {
        let mut state = ProtobarState::new();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "text": "normal", "priority": "normal" })),
                now(),
            )
            .unwrap();
        // Empty text at high priority is a live empty message: it wins and
        // blanks the bar over the live normal message (not a retraction).
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "text": "", "priority": "high" })),
                now(),
            )
            .unwrap();
        let display = state.display(now());
        assert_eq!(rendered_text(&display), "");
        assert_eq!(display.priority, Some(Urgency::High));
    }

    #[test]
    fn all_expired_renders_empty_not_awaiting() {
        let mut state = ProtobarState::new();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({
                    "text": "x",
                    "expires_at": "2026-07-08T12:00:10Z",
                })),
                now(),
            )
            .unwrap();
        let later = at("2026-07-08T12:05:00Z");
        assert_eq!(rendered_text(&state.display(later)), "");
        assert_eq!(state.display(later).priority, None);
    }

    #[test]
    fn next_expiry_picks_earliest_future_expiry() {
        let mut state = ProtobarState::new();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({
                    "text": "high", "priority": "high",
                    "expires_at": "2026-07-08T12:02:00Z",
                })),
                now(),
            )
            .unwrap();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({
                    "text": "low", "priority": "low",
                    "expires_at": "2026-07-08T12:01:00Z",
                })),
                now(),
            )
            .unwrap();
        assert_eq!(state.next_expiry(now()), Some(at("2026-07-08T12:01:00Z")));
    }

    #[test]
    fn next_expiry_none_when_no_future_expiry() {
        let mut state = ProtobarState::new();
        // No expiry at all.
        state
            .on_message("messages", &sample_envelope_json("plain"), now())
            .unwrap();
        assert_eq!(state.next_expiry(now()), None);
        // A past expiry does not schedule.
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({
                    "text": "gone", "priority": "high",
                    "expires_at": "2020-01-01T00:00:00Z",
                })),
                now(),
            )
            .unwrap();
        assert_eq!(state.next_expiry(now()), None);
    }

    #[test]
    fn malformed_bodies_report_and_leave_slots_untouched() {
        let cases: &[serde_json::Value] = &[
            serde_json::json!({ "priority": "high" }), // missing text
            serde_json::json!({ "text": 5 }),          // wrong type
            serde_json::json!({ "text": "x", "priority": "urgent" }), // bad priority
            serde_json::json!({ "text": "x", "format": "html" }), // bad format
            serde_json::json!({ "text": "x", "expires_at": "not-a-date" }), // bad expiry
        ];
        for (i, body) in cases.iter().enumerate() {
            let mut state = ProtobarState::new();
            // A live message occupies the bar first.
            state
                .on_message("messages", &sample_envelope_json("live"), now())
                .unwrap();
            let outcome = state
                .on_message("messages", &structured(body.clone()), now())
                .unwrap();
            assert!(
                matches!(outcome, Ingest::Malformed(_)),
                "case {i} should be malformed"
            );
            // Slots untouched: the live message still shows.
            assert_eq!(rendered_text(&state.display(now())), "live");
            // Counter reflects exactly one malformed body.
            assert_eq!(state.display(now()).status_text, "malformed: 1");
        }
    }

    #[test]
    fn malformed_report_names_the_publisher() {
        let mut state = ProtobarState::new();
        let outcome = state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "priority": "high" })),
                now(),
            )
            .unwrap();
        let Ingest::Malformed(report) = outcome else {
            panic!("expected malformed");
        };
        // Envelope fields from the shared fixture.
        assert_eq!(report.channel, "ephemeral:demo");
        assert_eq!(report.sender, "surface:deskbar");
        assert_eq!(report.message_id, "00000000-0000-0000-0000-000000000001");
        assert!(
            report
                .log_message("protobar body")
                .contains("ephemeral:demo")
        );
        assert!(
            report
                .log_message("protobar body")
                .contains("surface:deskbar")
        );
    }

    #[test]
    fn malformed_does_not_clear_awaiting_data() {
        let mut state = ProtobarState::new();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "text": 5 })),
                now(),
            )
            .unwrap();
        assert_eq!(rendered_text(&state.display(now())), "awaiting data");
    }

    #[test]
    fn drops_accumulate_across_events() {
        let mut state = ProtobarState::new();
        state.on_drops("messages", 2).unwrap();
        state.on_drops("messages", 3).unwrap();
        assert_eq!(state.display(now()).status_text, "dropped: 5");
    }

    #[test]
    fn status_line_composes_drops_and_malformed() {
        let mut state = ProtobarState::new();
        state.on_drops("messages", 4).unwrap();
        state
            .on_message(
                "messages",
                &structured(serde_json::json!({ "text": 5 })),
                now(),
            )
            .unwrap();
        assert_eq!(
            state.display(now()).status_text,
            "dropped: 4 · malformed: 1"
        );
    }

    #[test]
    fn wrong_port_rejected() {
        let mut state = ProtobarState::new();
        assert_eq!(
            state.on_message("wrong", &sample_envelope_json("x"), now()),
            Err(ContractViolation::WrongPort {
                port: "wrong".to_string()
            })
        );
        assert_eq!(
            state.on_drops("wrong", 1),
            Err(ContractViolation::WrongPort {
                port: "wrong".to_string()
            })
        );
    }

    #[test]
    fn bad_envelope_rejected() {
        let mut state = ProtobarState::new();
        let result = state.on_message("messages", "not json", now());
        assert!(matches!(result, Err(ContractViolation::BadEnvelope(_))));
    }
}

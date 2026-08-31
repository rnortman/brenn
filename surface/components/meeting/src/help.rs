//! The generator behind `surface/components/meeting/help.md`.
//!
//! The sidecar the build copies into the surface asset dir is a committed file,
//! but it is written by [`help_markdown`], never by hand: the escalation ladder,
//! the threshold defaults, the validation rules, the retire cap, the snooze
//! duration, the port names, and both body shapes are interpolated from the
//! identifiers the state machine itself uses, so the published doc cannot
//! advertise a threshold rule or an ack shape meeting rejects. Prose with no code
//! counterpart lives here as literals.
//!
//! The audience is an LLM publishing agendas and reading acks, so the text
//! optimizes for unambiguous facts over presentation.

use std::fmt::Write as _;

use brenn_surface_contract::HELP_SIDECAR_HEADER;
use brenn_surface_schema::LOCAL_TAKEOVER_CHANNEL;
use chrono::{DateTime, Duration, Utc};

use crate::logic::{
    AckTarget, DEFAULT_CRITICAL_SECS, DEFAULT_OVERDUE_SECS, DEFAULT_TAKEOVER_SECS, DisplayState,
    ESCALATION_LADDER, RETIRE_AFTER_SECS, RawEscalation, RawMeeting, RawSnapshot, SNOOZE_SECS,
    dismiss_body, snooze_body,
};
use crate::spec::port::{ACKS, AGENDA, TAKEOVER};

/// Meeting's help sidecar, in full.
pub fn help_markdown() -> String {
    let mut out = String::from(HELP_SIDECAR_HEADER);
    intro(&mut out);
    agenda_section(&mut out);
    ack_section(&mut out);
    takeover_section(&mut out);
    out
}

/// A meeting id an LLM would recognize as one; the ack examples need a concrete
/// occurrence, and `AckTarget` holds a real instant, not a placeholder.
const EXAMPLE_MEETING_ID: &str = "standup-2026-07-12";
const EXAMPLE_START: &str = "2026-07-12T15:00:00Z";

/// What the panel is, plus the escalation ladder in ascending order.
fn intro(out: &mut String) {
    let _ = writeln!(
        out,
        "Meeting-notice panel that shows time-to-next-meeting and escalates {}, \
         computing every threshold locally from the wall clock.",
        ladder(" → "),
    );
}

/// The escalating state names in ascending order, joined by `sep`.
fn ladder(sep: &str) -> String {
    ESCALATION_LADDER
        .iter()
        .map(|state| state.as_wire_str())
        .collect::<Vec<_>>()
        .join(sep)
}

/// How to publish an agenda, the body shape, and every rule the snapshot parser
/// applies to it.
fn agenda_section(out: &mut String) {
    let _ = writeln!(
        out,
        "\nPublish a full upcoming-meetings snapshot via BrennSend to the instance's \
         `{AGENDA}` channel (latest-wins; use a retained channel so it replays on \
         reconnect). Body:\n\n```json\n{}\n```",
        snapshot_sketch(),
    );
    let _ = writeln!(
        out,
        "\n`escalation` is an optional per-meeting override, shown above with the \
         defaults it takes when absent. All three values must be `>= 0`, \
         `takeover_secs > critical_secs`, and `overdue_secs < {RETIRE_AFTER_SECS}` (an \
         `overdue_secs` at or past the retire cap below would retire the meeting while \
         it is still `{}`); an override breaking any of those is ignored and the \
         defaults used. Unknown fields are ignored, and an empty `meetings` list is a \
         valid idle state. A malformed snapshot (bad JSON, missing id/start/title, \
         unparseable time, duplicate id) is ignored and the last snapshot kept.",
        DisplayState::Critical.as_wire_str(),
    );
    let _ = writeln!(
        out,
        "\nAn undismissed meeting retires {RETIRE_AFTER_SECS} s after its start: it \
         stops escalating and leaves the panel, so a morning meeting nobody dismissed \
         does not alarm all afternoon.",
    );
}

/// The documented snapshot body: a placeholder-filled `RawSnapshot` serialized by
/// the deserializer's own struct, so the field names are the accepted ones. The
/// escalation numbers are the real defaults (the override fields are `i64` and
/// cannot carry placeholder text — showing what they default to is the useful
/// reading anyway). `v` is a literal: the parser ignores it, so there is no
/// identifier to interpolate.
fn snapshot_sketch() -> String {
    let example = RawSnapshot {
        v: Some(1),
        meetings: vec![RawMeeting {
            id: "<opaque string, the ack join key>".to_string(),
            start: "<RFC3339>".to_string(),
            title: "<string>".to_string(),
            end: Some("<RFC3339, optional, display only>".to_string()),
            escalation: Some(RawEscalation {
                takeover_secs: DEFAULT_TAKEOVER_SECS,
                critical_secs: DEFAULT_CRITICAL_SECS,
                overdue_secs: DEFAULT_OVERDUE_SECS,
            }),
        }],
    };
    serde_json::to_string_pretty(&example).expect("a RawSnapshot of strings serializes to JSON")
}

/// Occurrence scoping and the two ack bodies, minted by the same functions the
/// buttons publish through.
fn ack_section(out: &mut String) {
    let target = AckTarget {
        meeting_id: EXAMPLE_MEETING_ID.to_string(),
        start: example_start(),
    };
    let _ = writeln!(
        out,
        "\nThe panel publishes dismiss/snooze acks to its `{ACKS}` channel and \
         subscribes to the same channel so all devices converge. A dismissal is \
         permanent:\n\n```json\n{}\n```",
        dismiss_body(&target),
    );
    let _ = writeln!(
        out,
        "\nA snooze suppresses the occurrence until its `until`, then re-manifests at \
         whatever rung applies; the panel's own Snooze button uses {SNOOZE_SECS} s:\n\n\
         ```json\n{}\n```",
        snooze_body(&target, example_start() + Duration::seconds(SNOOZE_SECS)),
    );
    out.push('\n');
    out.push_str(OCCURRENCE_PROSE);
}

/// The instant the ack examples ack. Any instant does; a fixed one keeps the
/// generated doc stable.
fn example_start() -> DateTime<Utc> {
    EXAMPLE_START
        .parse()
        .expect("the example start is a valid RFC3339 instant")
}

/// Why an ack carries `start`, and what an ack without one does. Behavior with no
/// enumerable counterpart.
const OCCURRENCE_PROSE: &str = "\
`start` is the acked meeting's `start`, copied verbatim from the snapshot, and it
scopes the ack to that one occurrence: a `meeting_id` reused tomorrow, or the same
id rescheduled to a different `start`, is not suppressed by today's dismissal. An
ack with no parseable `start` names no occurrence, so it is dropped with a warning
and suppresses nothing.
";

/// How to cancel an alarm, and what happens at the takeover rung.
fn takeover_section(out: &mut String) {
    let _ = writeln!(
        out,
        "\nTo cancel an alarm from the agent side, drop the meeting from the next \
         snapshot (or publish a dismiss ack). At the `{}` threshold \
         ({DEFAULT_TAKEOVER_SECS} s before start by default) the panel publishes a \
         takeover request on its `{TAKEOVER}` output port (bound to \
         `{LOCAL_TAKEOVER_CHANNEL}`); chrome pushes a fullscreen overlay, granted only \
         on a takeover-granted surface. The kernel's router stamps the publishing \
         instance onto the request, so a component cannot request or release \
         another's overlay.",
        DisplayState::Takeover.as_wire_str(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::logic::{AckAction, AckKind, IngestOutcome, MeetingState};
    use brenn_surface_test_fixtures::{json_blocks, sample_envelope_json_at as envelope_json};

    /// A render clock inside the example meeting's ambient window.
    fn now() -> DateTime<Utc> {
        "2026-07-12T14:00:00Z".parse().unwrap()
    }

    #[test]
    fn help_sidecar_matches_generator() {
        brenn_surface_test_fixtures::enforce_help_sidecar(
            env!("CARGO_MANIFEST_DIR"),
            &help_markdown(),
        );
    }

    /// The documented snapshot shape, with its placeholders filled, is one the
    /// agenda parser accepts with no warning — so the field names and the
    /// escalation values in the doc are the accepted ones, not a lookalike.
    #[test]
    fn the_documented_snapshot_is_accepted() {
        let body = json_blocks(&help_markdown())
            .first()
            .expect("the doc carries a snapshot block")
            .replace("<opaque string, the ack join key>", "m1")
            .replace("<RFC3339, optional, display only>", "2026-07-12T15:30:00Z")
            .replace("<RFC3339>", EXAMPLE_START)
            .replace("<string>", "Standup");
        let mut state = MeetingState::new();
        let outcome = state
            .on_message(AGENDA, &envelope_json(&body, EXAMPLE_START), now())
            .expect("a well-formed envelope on the agenda port");
        assert_eq!(
            outcome,
            IngestOutcome::Accepted { warnings: vec![] },
            "the documented snapshot shape is not accepted as written"
        );
        assert_eq!(state.faults(), 0);
    }

    /// Both documented ack bodies are accepted on the ack port — the doc's ack
    /// dialect is the one the component speaks.
    #[test]
    fn the_documented_acks_are_accepted() {
        let blocks = json_blocks(&help_markdown());
        let acks = &blocks[1..];
        assert_eq!(acks.len(), 2, "the doc carries a dismiss and a snooze ack");
        for ack in acks {
            let mut state = MeetingState::new();
            assert_eq!(
                state
                    .on_message(ACKS, &envelope_json(ack, EXAMPLE_START), now())
                    .expect("a well-formed envelope on the ack port"),
                IngestOutcome::Accepted { warnings: vec![] },
                "documented ack body is not accepted: {ack}"
            );
        }
    }

    /// Every ack action the vocabulary admits is named in the doc, and the whole
    /// escalation ladder appears in ascending order.
    ///
    /// Both halves are anchored: each name also occurs in unrelated generated
    /// text, so a bare `doc.contains(name)` passes on a doc that lost the ladder
    /// entirely. The rung expectation is rebuilt from [`ESCALATION_LADDER`] rather
    /// than taken from [`ladder`] so a filter inside the generator cannot shrink
    /// both sides together.
    #[test]
    fn every_ack_action_and_rung_is_documented() {
        let doc = help_markdown();
        for kind in AckKind::ALL {
            let anchored = format!("\"action\":\"{}\"", kind.as_str());
            assert!(
                doc.contains(&anchored),
                "generated meeting help documents no ack body carrying action {}",
                kind.as_str()
            );
        }
        let rungs: Vec<&str> = ESCALATION_LADDER
            .iter()
            .map(|state| state.as_wire_str())
            .collect();
        let rendering = rungs.join(" → ");
        assert!(
            doc.contains(&rendering),
            "generated meeting help does not carry the escalation ladder as `{rendering}`"
        );
    }

    /// The doc's `action` strings are the ones the publishers mint, so a renamed
    /// action cannot leave the doc naming the old one.
    #[test]
    fn documented_actions_are_the_published_ones() {
        let doc = help_markdown();
        let until = example_start();
        for action in [AckAction::Dismiss, AckAction::Snooze { until }] {
            assert!(
                doc.contains(&format!("\"action\":\"{}\"", action.as_str())),
                "no published ack body in the doc carries action {}",
                action.as_str()
            );
        }
    }

    /// The three validation rules and the retire cap are all stated, with the cap
    /// interpolated rather than spelled "1 h".
    #[test]
    fn every_validation_rule_is_stated() {
        let doc = help_markdown();
        assert!(doc.contains("`>= 0`"));
        assert!(doc.contains("`takeover_secs > critical_secs`"));
        assert!(doc.contains(&format!("`overdue_secs < {RETIRE_AFTER_SECS}`")));
        assert!(doc.contains(&format!("{RETIRE_AFTER_SECS} s after its start")));
    }

    /// The ports and the takeover channel come from their constants.
    #[test]
    fn ports_and_channel_are_interpolated() {
        let doc = help_markdown();
        for port in [AGENDA, ACKS, TAKEOVER] {
            assert!(
                doc.contains(&format!("`{port}`")),
                "generated meeting help does not name port {port}"
            );
        }
        assert!(doc.contains(&format!("`{LOCAL_TAKEOVER_CHANNEL}`")));
    }

    /// The snooze duration and the escalation defaults are interpolated, so the
    /// doc cannot state a threshold the state machine does not use.
    #[test]
    fn defaults_are_interpolated() {
        let doc = help_markdown();
        assert!(doc.contains(&format!("{SNOOZE_SECS} s")));
        assert!(doc.contains(&format!("{DEFAULT_TAKEOVER_SECS} s before start")));
        for value in [
            DEFAULT_TAKEOVER_SECS,
            DEFAULT_CRITICAL_SECS,
            DEFAULT_OVERDUE_SECS,
        ] {
            assert!(
                doc.contains(&value.to_string()),
                "generated meeting help does not carry default {value}"
            );
        }
    }
}

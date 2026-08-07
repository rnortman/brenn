//! The generator behind `surface/components/protobar/help.md`.
//!
//! The sidecar the build copies into the surface asset dir is a committed file,
//! but it is written by [`help_markdown`], never by hand: the body field list,
//! the priority ladder, and the `format` vocabulary are interpolated from the
//! identifiers the parser itself uses, so the published doc cannot advertise a
//! value protobar treats as malformed. Prose with no code counterpart lives here
//! as literals.
//!
//! The audience is an LLM composing message bodies, so the text optimizes for
//! unambiguous facts over presentation.

use std::fmt::Write as _;

use brenn_envelope::Urgency;
use brenn_surface_contract::HELP_SIDECAR_HEADER;

use crate::logic::{
    CONVENTION_KEYS, DEFAULT_PRIORITY, FORMAT_MARKDOWN, FORMAT_PLAIN, KEY_EXPIRES_AT, KEY_FORMAT,
    KEY_PRIORITY, KEY_TEXT,
};

/// Protobar's help sidecar, in full.
pub fn help_markdown() -> String {
    let mut out = String::from(HELP_SIDECAR_HEADER);
    out.push_str(INTRO);
    body_sketch(&mut out);
    slot_semantics(&mut out);
    out
}

/// How a body reaches protobar and the two shapes it may take. No enumerable
/// facts, so all prose.
const INTRO: &str = "\
Publish content to the instance's content channel via BrennSend. The body is
either bare text (rendered as one plain paragraph), or a JSON object:
";

/// Slot lifetime, expiry fallback, and the retraction idiom — behavior with no
/// enumerable counterpart beyond the ladder interpolated above it.
const SLOT_PROSE: &str = "\
When the displayed slot expires the panel autonomously falls back to the
next-highest unexpired slot — no new message needed. Set `expires_at` to
auto-dismiss a slot after a deadline. To retract a slot before its deadline,
republish that same priority with an `expires_at` already in the past. There is
no explicit dismiss message.
";

/// The structured-body shape, one line per reserved convention key.
fn body_sketch(out: &mut String) {
    out.push_str("\n```json\n{\n");
    let fields: Vec<String> = CONVENTION_KEYS
        .iter()
        .map(|key| format!("  \"{key}\": \"{}\"", field_shape(key)))
        .collect();
    out.push_str(&fields.join(",\n"));
    out.push_str("\n}\n```\n");
}

/// The placeholder documenting one convention key's accepted values. Panics on a
/// key with no documented shape, so a key added to [`CONVENTION_KEYS`] cannot
/// ship undocumented.
fn field_shape(key: &str) -> String {
    match key {
        KEY_TEXT => "<string>".to_string(),
        KEY_PRIORITY => format!(
            "<{}, default {}>",
            priority_ladder("|"),
            DEFAULT_PRIORITY.as_str()
        ),
        KEY_EXPIRES_AT => "<RFC3339 timestamp, optional>".to_string(),
        KEY_FORMAT => format!("<{FORMAT_PLAIN}|{FORMAT_MARKDOWN}, default {FORMAT_PLAIN}>"),
        other => panic!(
            "convention key {other:?} has no documented shape; add one to protobar's \
             help generator"
        ),
    }
}

/// The priority vocabulary in ascending order, joined by `sep`.
fn priority_ladder(sep: &str) -> String {
    Urgency::ALL
        .iter()
        .map(|u| u.as_str())
        .collect::<Vec<_>>()
        .join(sep)
}

/// One slot per priority level, the selection rule, and the expiry behavior.
fn slot_semantics(out: &mut String) {
    let _ = write!(
        out,
        "\nUnknown fields are ignored. The panel keeps one live slot per priority level \
         and displays the highest-priority slot whose expiry has not passed (ordering \
         low→high: {}). A bare-text body occupies the `{}` slot.\n\n",
        priority_ladder(", "),
        DEFAULT_PRIORITY.as_str(),
    );
    out.push_str(SLOT_PROSE);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{Ingest, ProtobarState};
    use brenn_surface_test_fixtures::sample_envelope_json;

    /// A fixed render clock; nothing here depends on its value.
    fn now() -> chrono::DateTime<chrono::Utc> {
        "2026-07-08T12:00:00Z".parse().unwrap()
    }

    #[test]
    fn help_sidecar_matches_generator() {
        brenn_surface_test_fixtures::enforce_help_sidecar(
            env!("CARGO_MANIFEST_DIR"),
            &help_markdown(),
        );
    }

    /// Both renderings of the ladder appear in the doc with every level in
    /// ascending order.
    ///
    /// The expected strings are rebuilt from [`Urgency::ALL`] here rather than
    /// taken from [`priority_ladder`], so a filter or truncation inside the
    /// generator fails this test instead of shrinking both sides together. The
    /// joined form is also what makes the check honest per level: `low` is a
    /// substring of `very-low`, so a bare `doc.contains("low")` is satisfied by
    /// the `very-low` entry alone.
    #[test]
    fn every_priority_level_is_documented() {
        let doc = help_markdown();
        let levels: Vec<&str> = Urgency::ALL.iter().map(|u| u.as_str()).collect();
        for rendering in [levels.join("|"), levels.join(", ")] {
            assert!(
                doc.contains(&rendering),
                "generated protobar help does not carry the priority ladder as `{rendering}`"
            );
        }
    }

    #[test]
    fn every_convention_key_is_documented() {
        let doc = help_markdown();
        for key in CONVENTION_KEYS {
            assert!(
                doc.contains(&format!("\"{key}\"")),
                "generated protobar help has no field line for {key}"
            );
        }
    }

    /// Every documented priority string is one the parser accepts, and a level
    /// the parser rejects can never appear in the doc — the divergence this
    /// generator exists to make impossible.
    #[test]
    fn documented_priorities_are_the_accepted_ones() {
        for urgency in Urgency::ALL {
            let mut state = ProtobarState::new();
            let body = serde_json::json!({ "text": "x", "priority": urgency.as_str() });
            assert_eq!(
                state.on_message("messages", &sample_envelope_json(&body.to_string()), now()),
                Ok(Ingest::Accepted),
                "documented priority {} is rejected by the parser",
                urgency.as_str()
            );
        }
        let mut state = ProtobarState::new();
        let body = serde_json::json!({ "text": "x", "priority": "never" });
        assert!(
            matches!(
                state.on_message("messages", &sample_envelope_json(&body.to_string()), now()),
                Ok(Ingest::Malformed(_))
            ),
            "`never` is not a priority level; the doc must not have grown one"
        );
        assert!(
            !priority_ladder("|").contains("never"),
            "the documented priority vocabulary lists a level the parser rejects"
        );
    }

    /// Both documented `format` values parse, so the vocabulary in the doc is the
    /// vocabulary the parse path admits.
    #[test]
    fn documented_formats_are_the_accepted_ones() {
        for format in [FORMAT_PLAIN, FORMAT_MARKDOWN] {
            let mut state = ProtobarState::new();
            let body = serde_json::json!({ "text": "x", "format": format });
            assert_eq!(
                state.on_message("messages", &sample_envelope_json(&body.to_string()), now()),
                Ok(Ingest::Accepted),
                "documented format {format} is rejected by the parser"
            );
            assert!(help_markdown().contains(format));
        }
    }

    /// Each documented key on its own claims a body for the structured
    /// convention, which pins [`CONVENTION_KEYS`] to the deserialized field
    /// names: a key the doc lists but serde does not know would render verbatim
    /// instead.
    #[test]
    fn every_documented_key_claims_a_body() {
        for key in CONVENTION_KEYS {
            let mut state = ProtobarState::new();
            let body = serde_json::json!({ key: "x" });
            let outcome = state
                .on_message("messages", &sample_envelope_json(&body.to_string()), now())
                .expect("a well-formed envelope on the bound port");
            let claimed = match outcome {
                Ingest::Malformed(_) => true,
                // `{"text": "x"}` is the one single-key object that is both
                // claimed and valid; it displays its text rather than the raw
                // JSON.
                Ingest::Accepted => crate::markdown::all_text(&state.display(now()).message) == "x",
            };
            assert!(claimed, "a body carrying only {key} was not claimed");
        }
    }
}

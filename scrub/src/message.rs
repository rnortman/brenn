//! Rejection messages.
//!
//! This is the education channel: on a blocked write the text below is the
//! only thing the author sees, so it names what matched, leads with the
//! preferred fix, and states both exemption mechanisms in the order they
//! should be reached for. It reports rule ids, never the matching regex, so an
//! overlay's rule definitions are not restated in its own failure output.

use crate::gitleaks::{EPHEMERAL_RULE, Finding};

fn ladder() -> String {
    [
        "How to resolve, in order of preference:",
        "  1. Rewrite the string. Use generic names -- alice, bob, charlie, ACME Co.,",
        "     example.com. See docs/comment-standard.md. Prefer this; every exemption",
        "     is permanent review burden.",
        "  2. Contextual allowlist. For site-local rules this belongs in the local",
        "     overlay, not in tracked code -- ask a maintainer. An inline marker in",
        "     tracked code restates which rule matched here.",
        "  3. Inline `gitleaks:allow` on the offending line, where a context regex would",
        "     be awkward. Add the marker in the SAME COMMIT that introduces the",
        "     tolerated string -- pushed-range scans read every commit's diff, so a",
        "     marker added later still fails on the earlier commit.",
    ]
    .join("\n")
}

fn ephemeral_note() -> String {
    [
        "Section-symbol references to design and ADR docs rot: those docs are ephemeral",
        "point-in-time artifacts, not ground truth (comment-standard Rule 1). Cite stable",
        "published specs or in-tree docs instead, or just describe what the code does.",
    ]
    .join("\n")
}

/// The neutrality tripwire, shared by every module that ships emitted prose.
///
/// The one-shot audit that removed private-context vocabulary from shipped
/// strings only stays true if something fails when it comes back, and the
/// strings most likely to be pasted into a public issue are help text and error
/// lines, not just this module's output.
#[cfg(test)]
pub mod neutral {
    /// Vocabulary that would tell a reader who runs this tooling or what it
    /// guards. Emitted text describes the mechanism only.
    pub const BANNED_TERMS: &[&str] = &[
        // Written as a join so this file does not itself contain the term.
        concat!("house", "hold"),
        "personal",
        "family",
        "the owner",
        "owner machine",
        "private denylist",
        "private rule",
        "private overlay",
    ];

    pub fn assert_neutral(text: &str, what: &str) {
        let lower = text.to_lowercase();
        for term in BANNED_TERMS {
            assert!(
                !lower.contains(term),
                "{what} contains private-context term {term:?}"
            );
        }
    }
}

/// Format findings for a blocked write or a failed gate.
pub fn rejection(findings: &[Finding], heading: &str) -> String {
    let mut out = String::new();
    out.push_str(heading);
    out.push_str("\n\n");

    for f in findings {
        out.push_str(&format!(
            "  [{}] {} at {}\n      matched: {}\n",
            f.rule_id,
            f.description,
            f.location(),
            f.matched
        ));
    }

    out.push('\n');
    out.push_str(&ladder());

    if findings.iter().any(|f| f.rule_id == EPHEMERAL_RULE) {
        out.push_str("\n\n");
        out.push_str(&ephemeral_note());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(rule: &str) -> Finding {
        Finding {
            rule_id: rule.into(),
            description: "a description".into(),
            start_line: 12,
            file: "src/a.rs".into(),
            matched: "matched-text".into(),
            commit: String::new(),
        }
    }

    #[test]
    fn names_the_rule_and_location() {
        let msg = rejection(&[finding("local-rule-a")], "Blocked.");
        assert!(msg.starts_with("Blocked."));
        assert!(msg.contains("[local-rule-a]"));
        assert!(msg.contains("src/a.rs:12"));
        assert!(msg.contains("matched-text"));
    }

    #[test]
    fn contains_the_three_option_exemption_ladder_in_order() {
        let msg = rejection(&[finding("local-rule-a")], "Blocked.");
        let rewrite = msg.find("Rewrite the string").expect("option 1 missing");
        let allowlist = msg.find("Contextual allowlist").expect("option 2 missing");
        let inline = msg.find("gitleaks:allow").expect("option 3 missing");
        assert!(
            rewrite < allowlist && allowlist < inline,
            "ladder must be in preference order"
        );
    }

    #[test]
    fn states_the_same_commit_marker_rule() {
        let msg = rejection(&[finding("local-rule-a")], "Blocked.");
        assert!(msg.contains("SAME COMMIT"));
    }

    #[test]
    fn points_at_the_generic_names_standard() {
        let msg = rejection(&[finding("local-rule-a")], "Blocked.");
        assert!(msg.contains("docs/comment-standard.md"));
        assert!(msg.contains("alice"));
    }

    #[test]
    fn adds_the_rule_one_note_only_for_ephemeral_hits() {
        let plain = rejection(&[finding("local-rule-a")], "Blocked.");
        assert!(!plain.contains("comment-standard Rule 1"));

        let ephemeral = rejection(&[finding(EPHEMERAL_RULE)], "Blocked.");
        assert!(ephemeral.contains("comment-standard Rule 1"));
    }

    #[test]
    fn mixed_findings_still_get_the_rule_one_note() {
        let msg = rejection(
            &[finding("local-rule-a"), finding(EPHEMERAL_RULE)],
            "Blocked.",
        );
        assert!(msg.contains("comment-standard Rule 1"));
        assert!(msg.contains("[local-rule-a]"));
    }

    #[test]
    fn never_leaks_a_regex() {
        let msg = rejection(&[finding("local-rule-a")], "Blocked.");
        assert!(!msg.contains("(?i)"));
    }

    use super::neutral::assert_neutral;

    #[test]
    fn every_message_variant_is_neutral() {
        assert_neutral(&ladder(), "ladder");
        assert_neutral(&ephemeral_note(), "ephemeral note");
        assert_neutral(
            &rejection(&[finding("local-rule-a")], "Blocked."),
            "plain rejection",
        );
        assert_neutral(
            &rejection(&[finding(EPHEMERAL_RULE)], "Blocked."),
            "section-ref rejection",
        );
        assert_neutral(
            &rejection(
                &[finding("local-rule-a"), finding(EPHEMERAL_RULE)],
                "Blocked.",
            ),
            "mixed rejection",
        );
    }
}

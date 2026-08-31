//! The generator behind `surface/chrome/help.md`.
//!
//! The sidecar the build copies into the surface asset dir is a committed file,
//! but it is written by [`help_markdown`], never by hand: every fact in it that
//! also exists as an identifier — port names, plane addresses, the layout
//! vocabulary, the ratio bounds, the default-layout mapping — is interpolated
//! from that identifier, so the published doc cannot describe a chrome that does
//! not exist. Prose with no code counterpart lives here as literals.
//!
//! The audience is an LLM writing config and layout documents, so the text
//! optimizes for unambiguous facts over presentation.

use std::fmt::Write as _;

use brenn_surface_contract::HELP_SIDECAR_HEADER;
use brenn_surface_schema as proto;

use crate::layout::{
    ALL_KINDS, LAYOUT_DOC_VERSION, LayoutDoc, LayoutKind, Panel, RATIO_MAX, RATIO_MIN,
};

use crate::logic::{
    OUTPUT_PORT_DOC, PortChannel, PortDoc, default_kind_for_count, input_port_docs,
};

/// Chrome's help sidecar, in full.
pub fn help_markdown() -> String {
    let mut out = String::from(HELP_SIDECAR_HEADER);
    out.push_str(INTRO);
    let inputs = input_port_docs();
    let _ = write!(
        out,
        "\n## Inputs (bind these on the chrome instance)\n\nChrome reads {} ports. Bind \
         each to the channel it carries:\n",
        inputs.len()
    );
    port_table(&mut out, inputs.iter());
    default_layout_paragraph(&mut out);
    theme_vocabulary(&mut out);
    out.push_str("\n## Output (bind this on the chrome instance)\n");
    port_table(&mut out, [&OUTPUT_PORT_DOC]);
    out.push_str(OVERLAY_STATE_PROSE);
    layout_doc_section(&mut out);
    out
}

/// What chrome is and what it owns — no enumerable facts, so all prose.
const INTRO: &str = "\
# chrome

The in-tree default chrome component. Chrome owns the page's layout, connection
banner, theme axis, takeover overlay, and toasts. It is an ordinary contract-v1
`dom` component: the kernel activates it like any other, and it learns everything
it renders from the ports bound to it — never from DOM queries or a side channel.

Exactly one component per surface is the chrome (`chrome = true`). Chrome places
every *other* mounted instance into a layout section; it never places itself.
";

/// What the overlay-state plane means and why to bind it. Behavior with no
/// enumerable counterpart.
const OVERLAY_STATE_PROSE: &str = "\
Chrome publishes it on every overlay transition and only on a transition:
`holder` names the instance that took the overlay, or is `null` when the overlay
popped; `since_stamp` is the wall-clock millisecond reading of the activation the
fold ran in. The kernel reads the plane and reports the holder in the surface's
status document, which is where a fullscreen-wedged surface becomes visible.

Bind it on every surface holding the `takeover` grant. A surface that leaves the
port unbound still renders identically, but its status document reports no
overlay whether or not one is held — the instrument is dark, and a wedged bar is
indistinguishable from a healthy one. The kernel draws one warn at connect when
a takeover-granted surface's chrome has no `overlay-state` output.
";

/// How chrome treats a layout doc it cannot apply. Behavior with no enumerable
/// counterpart.
const LAST_VALID_WINS_PROSE: &str = "\
Chrome keeps the **last valid** layout on screen: a doc that fails to parse or
names an unknown instance is dropped and reported, never partially applied, and
never blanks the surface. A doc published while a takeover overlay is up is
stored and applied when the overlay pops.
";

/// A markdown table of port rows, followed by a blank line.
fn port_table<'a>(out: &mut String, rows: impl IntoIterator<Item = &'a PortDoc>) {
    out.push_str("\n| Port | Channel | Carries |\n|---|---|---|\n");
    for row in rows {
        let channel = match row.channel {
            PortChannel::Address(addr) => format!("`{addr}`"),
            PortChannel::Described(text) => text.to_string(),
        };
        let _ = writeln!(out, "| `{}` | {channel} | {} |", row.port, row.carries);
    }
    out.push('\n');
}

/// The bare-surface default layout, with each count's kind read from the mapping
/// chrome actually applies.
fn default_layout_paragraph(out: &mut String) {
    let kind_for = |n: usize| {
        default_kind_for_count(n)
            .expect("a positive instance count has a default layout kind")
            .as_wire_str()
    };
    let _ = writeln!(
        out,
        "A surface with no `{layout}` binding renders the default layout: the first \
         three mounted instances in configured order, laid out by count (1 → `{one}`, \
         2 → `{two}`, 3 or more → `{three}`).",
        layout = crate::spec::port::LAYOUT,
        one = kind_for(1),
        two = kind_for(2),
        three = kind_for(3),
    );
}

/// The theme axis vocabulary, from the frozen wire constants.
fn theme_vocabulary(out: &mut String) {
    let _ = writeln!(
        out,
        "\nThe `theme` field on the theme plane is `{dark}` or `{light}`; a surface with \
         no theme-driving component stays `{dark}`.",
        dark = proto::THEME_DARK,
        light = proto::THEME_LIGHT,
    );
}

/// The layout-document section: a serialized example, the kind vocabulary with
/// slots and ratio rules, and the last-valid-wins behavior.
fn layout_doc_section(out: &mut String) {
    out.push_str("\n## The layout doc\n\nA JSON document naming which instance fills each slot of a layout kind:\n\n```json\n");
    out.push_str(&layout_doc_example());
    out.push_str("\n```\n\n`kind` is one of:\n\n");
    for kind in ALL_KINDS {
        let slots = kind
            .slots()
            .iter()
            .map(|s| format!("`{s}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let ratio = if kind.accepts_ratio() {
            "takes `ratio`"
        } else {
            "no `ratio`"
        };
        let _ = writeln!(
            out,
            "- `{}` — {}. Slots {slots}; {ratio}.",
            kind.as_wire_str(),
            kind.describe()
        );
    }
    let _ = write!(
        out,
        "\nEvery slot the kind names must be present in `panels`, and no other key may be.\n\
         Each panel's `instance` must be a mounted, arrangeable instance (not chrome \
         itself). `label`, if present, renders as a text header above the panel.\n\
         `ratio` is an optional split fraction, valid in `[{RATIO_MIN}, {RATIO_MAX}]` \
         inclusive, exposed to skin CSS as `--surface-ratio`; present on a kind that takes \
         no `ratio`, it rejects the whole doc. `v` must be `{LAYOUT_DOC_VERSION}`.\n\n"
    );
    out.push_str(LAST_VALID_WINS_PROSE);
}

/// A valid layout document, serialized from the real schema type so the example's
/// field names and shape are the parser's.
fn layout_doc_example() -> String {
    let doc = LayoutDoc {
        v: LAYOUT_DOC_VERSION,
        kind: LayoutKind::Columns2,
        panels: [
            (
                "a".to_string(),
                Panel {
                    instance: "left-panel".to_string(),
                    label: Some("Inbox".to_string()),
                },
            ),
            (
                "b".to_string(),
                Panel {
                    instance: "right-panel".to_string(),
                    label: None,
                },
            ),
        ]
        .into_iter()
        .collect(),
        ratio: Some(0.6),
    };
    serde_json::to_string_pretty(&doc).expect("a LayoutDoc serializes to JSON")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_sidecar_matches_generator() {
        brenn_surface_test_fixtures::enforce_help_sidecar(
            env!("CARGO_MANIFEST_DIR"),
            &help_markdown(),
        );
    }

    #[test]
    fn every_layout_kind_is_documented() {
        let doc = help_markdown();
        for kind in ALL_KINDS {
            let wire = kind.as_wire_str();
            assert!(
                doc.contains(&format!("`{wire}`")),
                "generated chrome help does not name layout kind {wire}"
            );
            assert!(
                doc.contains(kind.describe()),
                "generated chrome help does not describe layout kind {wire}"
            );
        }
    }

    #[test]
    fn every_port_is_documented() {
        let doc = help_markdown();
        let inputs = input_port_docs();
        for row in inputs.iter().chain([&OUTPUT_PORT_DOC]) {
            assert!(
                doc.contains(&format!("| `{}` |", row.port)),
                "generated chrome help has no table row for port {}",
                row.port
            );
            if let PortChannel::Address(addr) = row.channel {
                assert!(
                    doc.contains(&format!("`{addr}`")),
                    "port {} lost its channel address",
                    row.port
                );
            }
        }
    }

    /// The exclusion the exhaustive match makes deliberately, pinned. The
    /// compiler holds that every declared port is *considered*;
    /// `every_port_is_documented` then asserts over whatever rows it was
    /// handed, so it stays green if `toast-tick` gains one. It must not: the
    /// port is chrome's own deferred self-wake, nothing else ever publishes to
    /// it, and an operator has nothing to bind — a row would be a documented
    /// binding that cannot exist.
    #[test]
    fn the_self_wake_port_is_the_one_port_with_no_row() {
        let doc = help_markdown();
        assert!(
            !doc.contains("| `toast-tick` |"),
            "the self-wake port gained a help row"
        );
        assert_eq!(
            input_port_docs().len(),
            crate::spec::InPort::ALL.len() - 1,
            "every declared inbound port but the self-wake one carries a row"
        );
    }

    #[test]
    fn ratio_bounds_are_interpolated() {
        let doc = help_markdown();
        assert!(doc.contains(&format!("[{RATIO_MIN}, {RATIO_MAX}]")));
    }

    #[test]
    fn the_example_layout_doc_parses_and_validates() {
        let blocks = brenn_surface_test_fixtures::json_blocks(&help_markdown());
        assert_eq!(
            blocks.len(),
            1,
            "chrome's help carries one JSON example; a new one needs a check of its own"
        );
        let parsed: LayoutDoc = serde_json::from_str(&blocks[0]).expect("the example parses");
        parsed
            .validate(|i| matches!(i, "left-panel" | "right-panel"))
            .expect("the example validates");
    }

    #[test]
    fn the_version_the_example_carries_is_the_one_the_validator_accepts() {
        let doc = LayoutDoc {
            v: LAYOUT_DOC_VERSION,
            kind: LayoutKind::Single,
            panels: std::iter::once((
                "a".to_string(),
                Panel {
                    instance: "only".to_string(),
                    label: None,
                },
            ))
            .collect(),
            ratio: None,
        };
        doc.validate(|_| true).expect("LAYOUT_DOC_VERSION is valid");
    }
}

//! The generator behind `surface/components/mode-clock/help.md`.
//!
//! The sidecar the build copies into the surface asset dir is a committed file,
//! but it is written by [`help_markdown`], never by hand: the mode vocabulary,
//! the schedule defaults, the port names, the theme plane address, and the output
//! body example are interpolated from the identifiers the component itself runs
//! on, so the published doc cannot describe a clock that does not exist. Prose
//! with no code counterpart lives here as literals.
//!
//! The audience is an LLM writing config bodies and surface bindings, so the text
//! optimizes for unambiguous facts over presentation.

use std::fmt::Write as _;

use brenn_surface_contract::HELP_SIDECAR_HEADER;
use brenn_surface_schema::{
    CONTROL_PLANE_VERSION, LOCAL_THEME_CHANNEL, THEME_DARK, THEME_LIGHT, ThemeBody,
};

use crate::logic::{
    DEFAULT_DARK_START, DEFAULT_LIGHT_START, Mode, RawConfig, RawSchedule, fmt_hhmm,
};
use crate::spec::port::{CONFIG, THEME};

/// Mode-clock's help sidecar, in full.
pub fn help_markdown() -> String {
    let mut out = String::from(HELP_SIDECAR_HEADER);
    out.push_str(INTRO);
    config_intro(&mut out);
    config_sketch(&mut out);
    config_semantics(&mut out);
    output_section(&mut out);
    out
}

/// What the component is. No enumerable facts, so all prose.
const INTRO: &str = "\
Headless clock component (renders nothing) that drives the surface dark/light
theme by watching the wall clock.
";

/// How the schedule interval is defined, and what a malformed body does — the
/// two behaviors with no enumerable counterpart.
const SCHEDULE_PROSE: &str = "\
In auto mode the theme follows the schedule: light during the half-open local
wall-clock interval [`light_start`, `dark_start`) with midnight wraparound, dark
otherwise. A malformed body (bad JSON, unknown mode, unparseable time, or equal
boundaries) is ignored and the last config kept. The theme axis only affects
skins that ship a light variant (bench); dark-only skins are unaffected.
";

/// How a config body reaches the component, naming the port it binds.
fn config_intro(out: &mut String) {
    let _ = write!(
        out,
        "\nPublish a config body via BrennSend to the channel bound to the instance's \
         `{CONFIG}` port — use a retained channel so the last config replays on \
         reconnect. The body is a JSON object:\n\n"
    );
}

/// The config body shape, serialized from the struct the parser deserializes, so
/// the documented field names are the accepted ones.
fn config_sketch(out: &mut String) {
    let sketch = RawConfig {
        mode: format!(
            "<{}>",
            Mode::ALL
                .iter()
                .map(|mode| mode.as_str())
                .collect::<Vec<_>>()
                .join("|")
        ),
        schedule: Some(RawSchedule {
            light_start: "<HH:MM>".to_string(),
            dark_start: "<HH:MM>".to_string(),
        }),
    };
    out.push_str("```json\n");
    out.push_str(&serde_json::to_string_pretty(&sketch).expect("a RawConfig serializes to JSON"));
    out.push_str("\n```\n");
}

/// Which fields are required, what each mode does, and the default schedule.
fn config_semantics(out: &mut String) {
    let _ = write!(
        out,
        "\nUnknown fields are ignored. `mode` is required and is one of:\n\n"
    );
    for mode in Mode::ALL {
        let _ = writeln!(out, "- `{}` — {}", mode.as_str(), mode_description(mode));
    }
    let _ = write!(
        out,
        "\n`schedule` is optional; omitted, it resets to the default light \
         {light}, dark {dark}.\n\n",
        light = fmt_hhmm(DEFAULT_LIGHT_START),
        dark = fmt_hhmm(DEFAULT_DARK_START),
    );
    out.push_str(SCHEDULE_PROSE);
}

/// One line per mode. Exhaustive, so a new mode cannot ship undocumented.
fn mode_description(mode: Mode) -> &'static str {
    match mode {
        Mode::Auto => "day/night switching by the schedule below",
        Mode::Dark => "fixed dark; the schedule is ignored and nothing is scheduled",
        Mode::Light => "fixed light; the schedule is ignored and nothing is scheduled",
    }
}

/// The theme output: the real body serialized, its port, and the plane to bind.
fn output_section(out: &mut String) {
    let body = ThemeBody {
        v: CONTROL_PLANE_VERSION,
        theme: THEME_DARK.to_string(),
    };
    let _ = write!(
        out,
        "\nThe component's only output is a `ThemeBody` — `{example}`, where `theme` is \
         `{THEME_DARK}` or `{THEME_LIGHT}` — published on its `{THEME}` output port. \
         Bind that port to the reserved `{LOCAL_THEME_CHANNEL}` plane with a \
         `[[surface.output]]` block; chrome consumes the plane and writes the resulting \
         `data-theme` on `<body>`.\n",
        example = serde_json::to_string(&body).expect("a ThemeBody serializes to JSON"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{ConfigOutcome, ModeClock};
    use brenn_surface_test_fixtures::{json_blocks, sample_envelope_json};

    #[test]
    fn help_sidecar_matches_generator() {
        brenn_surface_test_fixtures::enforce_help_sidecar(
            env!("CARGO_MANIFEST_DIR"),
            &help_markdown(),
        );
    }

    /// Every documented mode is one `parse_config` accepts: the doc vocabulary
    /// and the accepted vocabulary are provably the same set (`Mode::ALL` drives
    /// both), and a mode the parser rejects cannot appear.
    #[test]
    fn documented_modes_are_the_accepted_ones() {
        let doc = help_markdown();
        for mode in Mode::ALL {
            assert!(
                doc.contains(&format!("`{}`", mode.as_str())),
                "generated mode-clock help does not name mode {}",
                mode.as_str()
            );
            let mut clock = ModeClock::new();
            let body = serde_json::json!({ "mode": mode.as_str() });
            assert_eq!(
                clock.on_config(CONFIG, &sample_envelope_json(&body.to_string())),
                Ok(ConfigOutcome::Accepted),
                "documented mode {} is rejected by the parser",
                mode.as_str()
            );
        }
    }

    /// The documented body shape is one the parser accepts once its placeholders
    /// are filled in, which pins the sketch's field names to the deserializer's.
    #[test]
    fn the_documented_body_shape_parses() {
        let blocks = json_blocks(&help_markdown());
        assert_eq!(
            blocks.len(),
            1,
            "mode-clock's help carries one JSON example; a new one needs a check of its own"
        );
        let filled = blocks[0]
            .replace(
                &format!(
                    "<{}>",
                    Mode::ALL
                        .iter()
                        .map(|mode| mode.as_str())
                        .collect::<Vec<_>>()
                        .join("|")
                ),
                Mode::Auto.as_str(),
            )
            // The two time placeholders are identical text; the schedule needs a
            // boundary, so they are filled in order with the two defaults.
            .replacen("<HH:MM>", &fmt_hhmm(DEFAULT_LIGHT_START), 1)
            .replacen("<HH:MM>", &fmt_hhmm(DEFAULT_DARK_START), 1);
        let mut clock = ModeClock::new();
        assert_eq!(
            clock.on_config(CONFIG, &sample_envelope_json(&filled)),
            Ok(ConfigOutcome::Accepted),
            "the documented body shape is not accepted: {filled}"
        );
    }

    /// The schedule defaults in the doc are the constants the parser falls back
    /// to, rendered in the form the parser reads back.
    #[test]
    fn schedule_defaults_are_interpolated() {
        let doc = help_markdown();
        for minutes in [DEFAULT_LIGHT_START, DEFAULT_DARK_START] {
            assert!(
                doc.contains(&fmt_hhmm(minutes)),
                "generated mode-clock help does not state the {} default",
                fmt_hhmm(minutes)
            );
        }
    }

    /// The plane address and the output body example come from the schema crate
    /// chrome parses, so the two ends of the theme axis cannot disagree in the
    /// doc.
    #[test]
    fn the_theme_plane_and_body_are_interpolated() {
        let doc = help_markdown();
        assert!(doc.contains(LOCAL_THEME_CHANNEL));
        let body = ThemeBody {
            v: CONTROL_PLANE_VERSION,
            theme: THEME_DARK.to_string(),
        };
        assert!(doc.contains(&serde_json::to_string(&body).unwrap()));
    }
}

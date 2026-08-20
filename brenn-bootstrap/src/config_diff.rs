//! `brenn config-diff <a> <b>`: are two config files the same configuration?
//!
//! The comparison is over parsed [`BrennConfig`] values, not over documents:
//! defaults are applied, key order is gone, and what is left is what the runtime
//! will actually see. Everything downstream of `load_config` is
//! provenance-blind, so TOML-vs-DSL, TOML-vs-TOML and DSL-vs-DSL are all the
//! same question, and each side loads through the same extension dispatch
//! `--config` uses.
//!
//! This is the migration tool for the `.brenn` front end: it is how an operator
//! proves a translated config is the config they were running.

use std::path::Path;

use brenn_lib::config::{BrennConfig, load_config};
use brenn_lib::messaging::canonicalize_channel_address;
use similar::TextDiff;

/// Load both files, compare, print the verdict. Returns whether they are equal,
/// which the binary turns into its exit status.
///
/// # Panics
///
/// Panics if either file fails to load — the differ compares valid configs, and
/// an invalid one is a louder failure than a diff.
pub fn run_config_diff(a: &Path, b: &Path) -> bool {
    let config_a = load_config(Some(a));
    let config_b = load_config(Some(b));
    let (equal, rendering) = diff(
        config_a,
        config_b,
        &a.display().to_string(),
        &b.display().to_string(),
    );
    print!("{rendering}");
    equal
}

/// Two loaded configs compared: whether they are equal, and the text to print.
/// The rendering ends in a newline in both arms.
pub(crate) fn diff(
    mut a: BrennConfig,
    mut b: BrennConfig,
    label_a: &str,
    label_b: &str,
) -> (bool, String) {
    canonicalize(&mut a);
    canonicalize(&mut b);
    if a == b {
        return (
            true,
            format!("{label_a} and {label_b} are the same config\n"),
        );
    }
    let text_a = format!("{a:#?}\n");
    let text_b = format!("{b:#?}\n");
    // Two configs that compare unequal and render identically hold a field that
    // is not equal to itself: a non-finite float, which TOML spells `nan` and
    // which nothing rejects before `validate_and_resolve`. Reporting "these
    // differ" over an empty diff is a verdict nobody can act on.
    assert!(
        text_a != text_b,
        "{label_a} and {label_b} compare unequal but render identically: a `nan` float is \
         never equal to itself, not even to the same file's own copy of it"
    );
    let rendering = TextDiff::from_lines(&text_a, &text_b)
        .unified_diff()
        .header(label_a, label_b)
        .to_string();
    (false, rendering)
}

/// Qualify every bare `[[channel]]` and tuning address with `brenn:`.
///
/// The runtime reads a bare address as `brenn:` anyway, so the two spellings are
/// one configuration; a lowered config is qualified on emit and a hand-written
/// TOML corpus is mostly bare, and comparing the two without this would report
/// every channel as changed. It lives here rather than in lowering on purpose:
/// canonical-on-emit means the config side keeps exactly one spelling, and this
/// tool is the temporary one. The rule itself is the runtime's
/// ([`canonicalize_channel_address`]), not a copy of it.
fn canonicalize(config: &mut BrennConfig) {
    for channel in &mut config.channels {
        for address in [&mut channel.address, &mut channel.address_prefix]
            .into_iter()
            .flatten()
        {
            *address = canonicalize_channel_address(address);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A config with one durable channel at `address`, everything else default.
    fn one_channel(address: &str) -> BrennConfig {
        toml::from_str(&format!(
            r#"
[[channel]]
address = "{address}"
uuid = "11111111-2222-3333-4444-555555555555"
push_depth = 4
retain_depth = 4
standing_retain_depth = 4
"#
        ))
        .unwrap()
    }

    #[test]
    fn equal_configs_report_equal() {
        let (equal, rendering) = diff(
            one_channel("brenn:alice-desk.in"),
            one_channel("brenn:alice-desk.in"),
            "a.brenn",
            "b.toml",
        );
        assert!(equal);
        assert_eq!(rendering, "a.brenn and b.toml are the same config\n");
    }

    #[test]
    fn unequal_configs_report_a_unified_diff() {
        let (equal, rendering) = diff(
            one_channel("brenn:alice-desk.in"),
            one_channel("brenn:bob-desk.in"),
            "a.brenn",
            "b.toml",
        );
        assert!(!equal);
        assert!(rendering.contains("--- a.brenn"), "{rendering}");
        assert!(rendering.contains("+++ b.toml"), "{rendering}");
        assert!(rendering.contains("-  "), "{rendering}");
        assert!(
            rendering.contains("\"brenn:alice-desk.in\","),
            "{rendering}"
        );
        assert!(rendering.contains("\"brenn:bob-desk.in\","), "{rendering}");
    }

    #[test]
    fn a_bare_address_equals_its_brenn_qualified_twin() {
        let (equal, _) = diff(
            one_channel("alice-desk.in"),
            one_channel("brenn:alice-desk.in"),
            "bare.toml",
            "qualified.toml",
        );
        assert!(equal);
    }

    #[test]
    fn a_bare_tuning_prefix_equals_its_brenn_qualified_twin() {
        let tuning = |address_prefix: &str| -> BrennConfig {
            toml::from_str(&format!(
                r#"
[[channel]]
address_prefix = "{address_prefix}"
push_depth = 4
retain_depth = 4
standing_retain_depth = 4
"#
            ))
            .unwrap()
        };
        let (equal, _) = diff(
            tuning("tool-results/"),
            tuning("brenn:tool-results/"),
            "bare.toml",
            "qualified.toml",
        );
        assert!(equal);
    }

    /// Exercises the extension dispatch: a `.brenn` document and the TOML it
    /// translates to are the same configuration. Also catches a differ that
    /// loads both sides as TOML or panics on a `.brenn` path.
    #[test]
    fn a_brenn_document_and_its_toml_twin_are_the_same_config() {
        let dir = tempfile::tempdir().unwrap();
        let document = dir.path().join("main.brenn");
        std::fs::write(
            &document,
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 16;
}
"#,
        )
        .unwrap();
        let twin = dir.path().join("twin.toml");
        std::fs::write(
            &twin,
            r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "alice-alerts"
push_depth = 8
retain_depth = 128
standing_retain_depth = 16
"#,
        )
        .unwrap();
        assert!(run_config_diff(&document, &twin));
    }

    /// Exit status 0 on configs that differ would be a false "safe to deploy".
    #[test]
    fn a_brenn_document_and_a_different_toml_are_not_the_same_config() {
        let dir = tempfile::tempdir().unwrap();
        let document = dir.path().join("main.brenn");
        std::fs::write(
            &document,
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 16;
}
"#,
        )
        .unwrap();
        let other = dir.path().join("other.toml");
        std::fs::write(
            &other,
            r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "alice-alerts"
push_depth = 4
retain_depth = 128
standing_retain_depth = 16
"#,
        )
        .unwrap();
        assert!(!run_config_diff(&document, &other));
    }

    /// A `nan` compares false against its own copy, so the equality check and
    /// the rendering disagree. Reporting "these differ" over an empty diff is a
    /// verdict nobody can act on, so the differ dies instead.
    #[test]
    #[should_panic(expected = "compare unequal but render identically")]
    fn a_non_finite_float_is_refused_rather_than_diffed_to_nothing() {
        let with_nan = || -> BrennConfig {
            toml::from_str(
                r#"
[[wasm_consumer]]
slug = "sink"
component_path = "/lib/brenn_sink.wasm"
grants = ["log"]

[[wasm_consumer.io_port]]
port = "tick"
push_depth = 1
retain_depth = 2
amplification = nan
"#,
            )
            .unwrap()
        };
        diff(with_nan(), with_nan(), "a.toml", "a.toml");
    }

    #[test]
    fn a_non_brenn_scheme_is_left_alone() {
        let (equal, rendering) = diff(
            one_channel("ephemeral:alice-desk.in"),
            one_channel("brenn:alice-desk.in"),
            "ephemeral.toml",
            "durable.toml",
        );
        assert!(!equal, "{rendering}");
    }
}

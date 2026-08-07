//! The generator behind `surface/components/echo-stub/help.md`.
//!
//! The stub has no vocabulary, no config schema, and no enumerable facts, so this
//! generator is one prose literal. It exists for uniformity: every in-tree help
//! sidecar is written by its crate's `help_markdown` and held there by a drift
//! test, so a component author never has to ask which components participate.

use brenn_surface_contract::HELP_SIDECAR_HEADER;

/// Echo-stub's help sidecar, in full.
pub fn help_markdown() -> String {
    format!("{HELP_SIDECAR_HEADER}{BODY}")
}

/// What the stub does. Behavior only — nothing here has a code counterpart to
/// interpolate.
const BODY: &str = "\
Dev/demo component, not a display panel: publish a body via BrennSend to the
instance's content channel and the stub echoes it to its output channel.
";

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
}

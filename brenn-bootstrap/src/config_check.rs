//! `brenn config-check <file>`: would the server load this config file?
//!
//! Runs the file through the same path `--config` boots from — parse, resolve,
//! derive, lower for a `.brenn` document, read and parse for a `.toml` one —
//! and renders diagnostics instead of panicking.
//!
//! Environment facts are out of scope: see [`brenn_lib::config::check_config`].

use std::path::Path;

use brenn_lib::config::check_config;

/// Check one config file, print the verdict. Returns whether it would load.
pub fn run_config_check(file: &Path) -> bool {
    match check_config(file) {
        Ok(_) => {
            println!("{}: ok", file.display());
            true
        }
        Err(report) => {
            eprintln!("{report}");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exercises both `run_config_check` (the boolean) and `check_config` (the
    /// text) and asserts they agree, so neither can pass alone.
    fn check(name: &str, contents: &str) -> (bool, String) {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(name);
        std::fs::write(&file, contents).unwrap();
        let ok = run_config_check(&file);
        let config = check_config(&file);
        assert_eq!(ok, config.is_ok(), "the verdict and the report disagree");
        (ok, config.err().unwrap_or_default())
    }

    /// `ok` means "this file is a config", not "this config will boot here":
    /// `validate_and_resolve` is deliberately not run, so environment facts —
    /// here a container whose home directory does not exist on this machine —
    /// do not decide the verdict. A workstation must be able to check a config
    /// destined for another host.
    #[test]
    fn a_document_boot_would_refuse_for_an_environment_fact_still_passes() {
        let (ok, report) = check(
            "main.brenn",
            r#"
container alice {
    image = "example.com/cc:latest";
    home_dir = "/nonexistent/alice";
}
"#,
        );
        assert!(ok, "{report}");
    }

    #[test]
    fn a_valid_brenn_document_passes() {
        let (ok, report) = check(
            "main.brenn",
            r#"
channel alerts at "brenn:alice-alerts" {
    push_depth = 8;
    retain_depth = 128;
    standing_retain_depth = 16;
}
"#,
        );
        assert!(ok, "{report}");
    }

    #[test]
    fn a_valid_toml_config_passes() {
        let (ok, report) = check(
            "brenn.toml",
            r#"
[[channel]]
uuid = "85a5cf7e-6874-5766-9d69-712784754a1f"
address = "alice-alerts"
push_depth = 8
retain_depth = 128
standing_retain_depth = 16
"#,
        );
        assert!(ok, "{report}");
    }

    /// A document that parses, resolves and derives, and is refused only at
    /// lowering: `noise` is a key of the subscribe tail's union vocabulary and a
    /// field no `webhook:` subscription has.
    #[test]
    fn a_lowering_only_refusal_fails_the_check() {
        let (ok, report) = check(
            "main.brenn",
            r#"
webhook push_alice {
    mount = "/webhooks/push-alice";

    signature {
        scheme = bearer-token;
        header = "authorization";
    }

    token phone { secret_file = "/home/alice/.secrets/push-alice.token"; }
}

agent Assistant() {
    grants = [subscribe];
    subscribe "webhook:push_alice" { push_depth = 4; noise = metered; }
}

new alice: Assistant();
"#,
        );
        assert!(!ok);
        // The stage matters: the document itself is well-formed, and a check
        // that stopped at compile would report this file as fine.
        assert!(report.contains("failed to lower"), "{report}");
        assert!(report.contains("noise"), "{report}");
    }

    #[test]
    fn an_unknown_key_fails_a_toml_config() {
        let (ok, report) = check("brenn.toml", "nope = 1\n");
        assert!(!ok);
        assert!(report.contains("nope"), "{report}");
    }

    /// The check tool reports; only boot panics.
    #[test]
    fn an_unrecognized_extension_fails_without_panicking() {
        let (ok, report) = check("brenn.conf", "nope = 1\n");
        assert!(!ok);
        assert!(report.contains("unrecognized extension"), "{report}");
    }
}

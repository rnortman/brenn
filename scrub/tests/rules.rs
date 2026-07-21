//! Semantics of the public rule file itself.
//!
//! The wrapper is unit-tested; the rules it exists to enforce were not. A
//! typo'd regex ships green through the wrapper's tests, and an *over-broad*
//! one is worse than a hole: it fires on the generic placeholders the
//! rejection ladder tells authors to use, training everyone to reach for
//! `gitleaks:allow`.
//!
//! These run gitleaks against the repo's real `.gitleaks.toml`. Skipped with a
//! message when the pinned gitleaks is absent, so a machine without it is not
//! broken by them.
//!
//! Fixture strings that a rule is meant to match are assembled at runtime, so
//! this file never contains a literal the gate would flag and never needs to
//! exempt itself from the rules under test.

use std::path::{Path, PathBuf};
use std::process::Command;

const PINNED_VERSION: &str = "8.30.0";

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is <repo>/scrub.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("scrub crate has a parent directory")
        .to_path_buf()
}

fn gitleaks_available() -> bool {
    match Command::new("gitleaks").arg("version").output() {
        Ok(out) if out.status.success() => {
            let found = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if found == PINNED_VERSION {
                true
            } else {
                eprintln!("skipping: gitleaks {found} is not the pinned {PINNED_VERSION}");
                false
            }
        }
        _ => {
            eprintln!("skipping: gitleaks not on PATH");
            false
        }
    }
}

/// Rule ids gitleaks reports for a directory of fixtures, scanned with the
/// repo's public config.
fn rule_ids_for(files: &[(&str, &str)]) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    for (name, body) in files {
        let path = dir.path().join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("fixture dir");
        }
        std::fs::write(&path, body).expect("fixture write");
    }
    scan(dir.path())
}

fn scan(dir: &Path) -> Vec<String> {
    let config = repo_root().join(".gitleaks.toml");
    let out = Command::new("gitleaks")
        .args(["dir", "--config"])
        .arg(&config)
        .args([
            "--report-format",
            "json",
            "--report-path",
            "-",
            "--no-banner",
            "--exit-code",
            "0",
            "--log-level",
            "error",
        ])
        .arg(dir)
        .output()
        .expect("failed to execute gitleaks");
    assert!(
        out.status.success(),
        "gitleaks failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    let text = stdout.trim();
    if text.is_empty() {
        return Vec::new();
    }
    let report: serde_json::Value = serde_json::from_str(text).expect("gitleaks report parses");
    let mut ids: Vec<String> = report
        .as_array()
        .expect("report is an array")
        .iter()
        .map(|f| {
            f["RuleID"]
                .as_str()
                .expect("finding has RuleID")
                .to_string()
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// The section symbol, built at runtime.
fn section() -> String {
    char::from_u32(0x00A7).expect("section symbol").to_string()
}

#[test]
fn ephemeral_reference_fires_on_source_and_not_on_markdown() {
    if !gitleaks_available() {
        return;
    }
    let s = section();
    let ids = rule_ids_for(&[("src/a.rs", &format!("// see design {s}3\n"))]);
    assert_eq!(
        ids,
        vec!["ephemeral-reference".to_string()],
        "a source comment citing an ephemeral doc section must be caught"
    );

    let ids = rule_ids_for(&[("docs/a.md", &format!("See {s}4 of the spec.\n"))]);
    assert!(
        ids.is_empty(),
        "markdown is where legitimate section references live: {ids:?}"
    );
}

#[test]
fn stable_spec_citations_and_inline_markers_pass() {
    if !gitleaks_available() {
        return;
    }
    let s = section();

    let ids = rule_ids_for(&[("src/a.rs", &format!("// RFC 8030 {s}5 push protocol\n"))]);
    assert!(ids.is_empty(), "RFC citations are blessed: {ids:?}");

    let marker = format!("{}:{}", "gitleaks", "allow");
    let ids = rule_ids_for(&[("src/a.rs", &format!("// see design {s}3 // {marker}\n"))]);
    assert!(ids.is_empty(), "inline marker must exempt: {ids:?}");
}

/// The failure mode that quietly destroys the gate: a rule broad enough to
/// fire on the very names the rejection message recommends.
#[test]
fn documented_generic_placeholders_are_never_flagged() {
    if !gitleaks_available() {
        return;
    }
    let ids = rule_ids_for(&[
        (
            "src/fixtures.rs",
            "let user = \"alice\";\n\
             let peer = \"bob\";\n\
             let third = \"charlie\";\n\
             let org = \"ACME Co.\";\n\
             let host = \"example.com\";\n\
             let other = \"example.org\";\n\
             let email = \"alice@example.com\";\n",
        ),
        ("docs/guide.md", "Use alice and bob with example.com.\n"),
    ]);
    assert!(
        ids.is_empty(),
        "generic placeholders must stay clean or authors learn to bypass the gate: {ids:?}"
    );
}

/// The public file extends gitleaks' defaults; a dropped `useDefault` would
/// silently stop catching real secrets while every wrapper test still passed.
#[test]
fn builtin_secret_rules_are_still_active() {
    if !gitleaks_available() {
        return;
    }
    let token = format!("{}_{}", "ghp", "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8");
    let ids = rule_ids_for(&[("src/config.rs", &format!("let gh = \"{token}\";\n"))]);
    assert!(
        !ids.is_empty(),
        "built-in secret rules must remain in force via [extend] useDefault"
    );
    assert!(
        !ids.contains(&"ephemeral-reference".to_string()),
        "unexpected rule fired: {ids:?}"
    );
}

/// The tracked template is what the other repos copy; drift means they get a
/// different gate than brenn. Only enforcement-bearing lines are compared:
/// comments and rule descriptions may differ, because the template cannot
/// cite in-tree docs that consuming repos do not have.
#[test]
fn repo_template_matches_the_tracked_public_config() {
    /// Drop comment lines, blank lines, and `description` (prose, not enforcement).
    fn enforcing_lines(src: &str) -> Vec<&str> {
        src.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with("description"))
            .collect()
    }

    let root = repo_root();
    let live = std::fs::read_to_string(root.join(".gitleaks.toml")).expect("public config");
    let template =
        std::fs::read_to_string(root.join("scrub/repo-template/gitleaks.toml")).expect("template");
    assert_eq!(
        enforcing_lines(&live),
        enforcing_lines(&template),
        "scrub/repo-template/gitleaks.toml has drifted from .gitleaks.toml"
    );
}

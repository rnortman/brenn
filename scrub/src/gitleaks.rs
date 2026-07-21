//! Invoking gitleaks and interpreting its JSON report.
//!
//! gitleaks is the only scanning engine. This module never reimplements a
//! rule; it prepares arguments, runs the binary, and parses the report.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;

/// Release this wrapper's CLI and config surface was validated against.
pub const PINNED_VERSION: &str = "8.30.0";

/// The section-symbol-reference rule id, shared by the tree filter and the rejection
/// message so the two cannot drift apart.
pub const EPHEMERAL_RULE: &str = "ephemeral-reference";

/// Rules enforced on diffs only. Tree scans drop findings with these ids:
/// the grandfathered backlog would otherwise make every tree scan permanently
/// red, while every diff-scanning layer still enforces them from day one.
pub const DIFF_ONLY_RULES: &[&str] = &[EPHEMERAL_RULE];

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Finding {
    #[serde(rename = "RuleID")]
    pub rule_id: String,
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "StartLine")]
    pub start_line: i64,
    #[serde(rename = "File")]
    pub file: String,
    #[serde(rename = "Match")]
    pub matched: String,
    #[serde(rename = "Commit", default)]
    pub commit: String,
}

impl Finding {
    /// `file:line` plus the commit when the scan was history-based.
    pub fn location(&self) -> String {
        if self.commit.is_empty() {
            format!("{}:{}", self.file, self.start_line)
        } else {
            format!(
                "{}:{} (commit {})",
                self.file, self.start_line, &self.commit
            )
        }
    }
}

/// Drop findings whose rule is diff-only. Expressed as a subtraction from the
/// full resolved config: narrowing the rule set gitleaks runs would silently
/// disable built-in and overlay rules too.
pub fn apply_tree_filter(findings: Vec<Finding>) -> Vec<Finding> {
    findings
        .into_iter()
        .filter(|f| !DIFF_ONLY_RULES.contains(&f.rule_id.as_str()))
        .collect()
}

/// Rewrite absolute scan paths as paths relative to the mirror root, so
/// reports name the file the user recognizes rather than a temp dir.
pub fn relativize(findings: Vec<Finding>, root: &Path) -> Vec<Finding> {
    findings
        .into_iter()
        .map(|mut f| {
            if let Ok(rel) = Path::new(&f.file).strip_prefix(root) {
                f.file = rel.to_string_lossy().into_owned();
            }
            f
        })
        .collect()
}

pub fn parse_report(stdout: &str) -> Vec<Finding> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    serde_json::from_str(trimmed).expect("gitleaks emitted a report this wrapper cannot parse")
}

#[derive(Debug, PartialEq, Eq)]
pub enum Version {
    Match,
    Mismatch(String),
    Missing,
}

impl Version {
    pub fn detect() -> Version {
        let Ok(out) = Command::new("gitleaks").arg("version").output() else {
            return Version::Missing;
        };
        if !out.status.success() {
            return Version::Missing;
        }
        let found = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if found == PINNED_VERSION {
            Version::Match
        } else {
            Version::Mismatch(found)
        }
    }

    pub fn mismatch_message(found: &str) -> String {
        format!(
            "gitleaks version mismatch: found {found}, this wrapper is pinned to \
             {PINNED_VERSION}. Install the pinned release, or re-validate the wrapper \
             against the newer one and move the pin."
        )
    }
}

/// Shared flags. `--exit-code 0` makes leaks a *reporting* outcome rather than
/// a process failure, so any nonzero exit is an actual gitleaks malfunction
/// and gets to panic.
fn base_args(config: &Path) -> Vec<String> {
    vec![
        "--config".into(),
        config.to_string_lossy().into_owned(),
        "--report-format".into(),
        "json".into(),
        "--report-path".into(),
        "-".into(),
        "--no-banner".into(),
        "--exit-code".into(),
        "0".into(),
        "--log-level".into(),
        "error".into(),
    ]
}

fn run(args: &[String], cwd: Option<&Path>) -> Vec<Finding> {
    let mut cmd = Command::new("gitleaks");
    cmd.args(args);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    let out = cmd.output().expect("failed to execute gitleaks");
    if !out.status.success() {
        panic!(
            "gitleaks exited {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    parse_report(&String::from_utf8_lossy(&out.stdout))
}

/// Scan a directory tree. Findings carry absolute paths under `dir`.
pub fn scan_dir(config: &Path, dir: &Path) -> Vec<Finding> {
    let mut args = vec!["dir".to_string()];
    args.extend(base_args(config));
    args.push(dir.to_string_lossy().into_owned());
    run(&args, None)
}

/// Scan commit history. `log_opts` is passed to `git log` by gitleaks.
pub fn scan_git(config: &Path, repo: &Path, log_opts: &str) -> Vec<Finding> {
    let mut args = vec!["git".to_string()];
    args.extend(base_args(config));
    args.push(format!("--log-opts={log_opts}"));
    args.push(repo.to_string_lossy().into_owned());
    run(&args, Some(repo))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(rule: &str) -> Finding {
        Finding {
            rule_id: rule.into(),
            description: "d".into(),
            start_line: 7,
            file: "/tmp/mirror/src/a.rs".into(),
            matched: "m".into(),
            commit: String::new(),
        }
    }

    #[test]
    fn tree_filter_drops_only_diff_only_rules() {
        let input = vec![
            finding("ephemeral-reference"),
            finding("local-rule-a"),
            finding("generic-api-key"),
            finding("example-host"),
        ];
        let kept = apply_tree_filter(input);
        let ids: Vec<&str> = kept.iter().map(|f| f.rule_id.as_str()).collect();
        assert_eq!(ids, vec!["local-rule-a", "generic-api-key", "example-host"]);
    }

    #[test]
    fn tree_filter_keeps_everything_when_no_diff_only_hits() {
        let input = vec![finding("local-rule-a"), finding("local-rule-b")];
        assert_eq!(apply_tree_filter(input.clone()), input);
    }

    #[test]
    fn tree_filter_can_empty_the_list() {
        assert!(apply_tree_filter(vec![finding("ephemeral-reference")]).is_empty());
    }

    #[test]
    fn relativize_strips_the_mirror_root() {
        let out = relativize(vec![finding("local-rule-a")], Path::new("/tmp/mirror"));
        assert_eq!(out[0].file, "src/a.rs");
    }

    #[test]
    fn relativize_leaves_unrelated_paths_alone() {
        let out = relativize(vec![finding("local-rule-a")], Path::new("/other/root"));
        assert_eq!(out[0].file, "/tmp/mirror/src/a.rs");
    }

    #[test]
    fn empty_report_parses_as_no_findings() {
        assert!(parse_report("").is_empty());
        assert!(parse_report("   \n").is_empty());
        assert!(parse_report("[]").is_empty());
    }

    #[test]
    fn report_parses_the_fields_the_wrapper_uses() {
        let json = concat!(
            r#"[{"RuleID":"ephemeral-reference","Description":"sec ref","StartLine":1,"#,
            r#""EndLine":1,"StartColumn":11,"EndColumn":12,"Match":"§","Secret":"§","#, // gitleaks:allow
            r#""File":"/m/src/a.rs","SymlinkFile":"","Commit":"","Entropy":0.5,"#,
            r#""Author":"","Email":"","Date":"","Message":"","Tags":[],"Fingerprint":"f"}]"#,
        );
        let findings = parse_report(json);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].rule_id, "ephemeral-reference");
        assert_eq!(findings[0].start_line, 1);
        assert_eq!(findings[0].file, "/m/src/a.rs");
    }

    #[test]
    fn location_names_the_commit_only_for_history_scans() {
        let mut f = finding("local-rule-a");
        assert_eq!(f.location(), "/tmp/mirror/src/a.rs:7");
        f.commit = "abc123".into();
        assert!(f.location().contains("commit abc123"));
    }

    #[test]
    fn mismatch_message_names_both_versions() {
        let msg = Version::mismatch_message("8.29.0");
        assert!(msg.contains("8.29.0"));
        assert!(msg.contains(PINNED_VERSION));
    }
}

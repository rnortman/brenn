//! AppTool implementations for git repo virtual tools.
//!
//! GitRepoCommitAndPush and GitRepoRun require user approval and get custom
//! display formatting. GitRepoStatus is auto-approved and handled entirely in
//! `handle_brenn_tools` — it doesn't need an AppTool entry. GitRepoPull is a
//! registry tool; its auto-approve and execution live on the registry
//! descriptor, not here.

use brenn_lib::app::{AppTool, wrap_in_tool_approve};
use brenn_lib::util::html_escape;
use brenn_lib::ws_types::ToolResponseDecision;

/// MCP tool name for git commit+push. Shared with `active_bridge.rs`.
pub const MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL: &str = "mcp__brenn__GitRepoCommitAndPush";
/// MCP tool name for arbitrary git commands. Shared with `active_bridge.rs`.
pub const MCP_GIT_REPO_RUN_TOOL: &str = "mcp__brenn__GitRepoRun";

/// Tool for committing all changes and pushing to upstream.
/// Requires user approval — shows the repos and commit message.
pub struct GitRepoCommitAndPushTool;

impl AppTool for GitRepoCommitAndPushTool {
    fn name(&self) -> &str {
        MCP_GIT_REPO_COMMIT_AND_PUSH_TOOL
    }

    fn format_display(&self, tool_input: &serde_json::Value) -> Option<String> {
        let repos = tool_input
            .get("repos")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
            .unwrap_or_default();
        let message = tool_input
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let repos_display = if repos.is_empty() {
            "(none)".to_string()
        } else {
            repos.join(", ")
        };

        let content = format!(
            r#"<div class="ts-summary">
  <div class="ts-row"><span class="ts-label">Repos:</span> <span class="ts-value">{repos}</span></div>
  <div class="ts-row"><span class="ts-label">Message:</span> <span class="ts-value">{message}</span></div>
  <div class="ts-note">Will stage all changes, commit, and push to upstream.</div>
</div>"#,
            repos = html_escape(&repos_display),
            message = html_escape(message),
        );

        Some(wrap_in_tool_approve(self.name(), tool_input, &content))
    }

    fn format_summary(
        &self,
        tool_input: &serde_json::Value,
        decision: &ToolResponseDecision,
    ) -> Option<String> {
        let repos = tool_input
            .get("repos")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let message = tool_input
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let status = match decision {
            ToolResponseDecision::Allow { .. } => "Approved",
            ToolResponseDecision::Deny { .. } => "Denied",
        };

        Some(format!(
            r#"<div class="ts-summary">
  <div class="ts-header">Git Commit &amp; Push — {status}</div>
  <div class="ts-row"><span class="ts-label">Repos:</span> <span class="ts-value">{repos}</span></div>
  <div class="ts-row"><span class="ts-label">Message:</span> <span class="ts-value">{message}</span></div>
</div>"#,
            repos = html_escape(&repos),
            message = html_escape(message),
        ))
    }
}

/// Tool for running arbitrary git commands in a managed repo.
/// Requires user approval — shows the repo and command.
pub struct GitRepoRunTool;

impl AppTool for GitRepoRunTool {
    fn name(&self) -> &str {
        MCP_GIT_REPO_RUN_TOOL
    }

    fn format_display(&self, tool_input: &serde_json::Value) -> Option<String> {
        let repo = tool_input
            .get("repo")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let args = tool_input
            .get("args")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();

        let content = format!(
            r#"<div class="ts-summary">
  <div class="ts-row"><span class="ts-label">Repo:</span> <span class="ts-value">{repo}</span></div>
  <div class="ts-row"><span class="ts-label">Command:</span> <code>git {args}</code></div>
</div>"#,
            repo = html_escape(repo),
            args = html_escape(&args),
        );

        Some(wrap_in_tool_approve(self.name(), tool_input, &content))
    }

    fn format_summary(
        &self,
        tool_input: &serde_json::Value,
        decision: &ToolResponseDecision,
    ) -> Option<String> {
        let repo = tool_input
            .get("repo")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let args = tool_input
            .get("args")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();

        let status = match decision {
            ToolResponseDecision::Allow { .. } => "Approved",
            ToolResponseDecision::Deny { .. } => "Denied",
        };

        Some(format!(
            r#"<div class="ts-summary">
  <div class="ts-header">Git Run — {status}</div>
  <div class="ts-row"><span class="ts-label">Repo:</span> <span class="ts-value">{repo}</span></div>
  <div class="ts-row"><span class="ts-label">Command:</span> <code>git {args}</code></div>
</div>"#,
            repo = html_escape(repo),
            args = html_escape(&args),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_and_push_name_matches_constant() {
        let tool = GitRepoCommitAndPushTool;
        assert_eq!(tool.name(), "mcp__brenn__GitRepoCommitAndPush");
    }

    #[test]
    fn git_run_name_matches_constant() {
        let tool = GitRepoRunTool;
        assert_eq!(tool.name(), "mcp__brenn__GitRepoRun");
    }

    #[test]
    fn commit_display_escapes_html() {
        let tool = GitRepoCommitAndPushTool;
        let input = serde_json::json!({
            "repos": ["life"],
            "message": "fix <script>alert('xss')</script>"
        });
        let html = tool.format_display(&input).unwrap();
        assert!(!html.contains("<script>"), "HTML should be escaped: {html}");
        assert!(
            html.contains("&lt;script&gt;"),
            "expected escaped tags: {html}"
        );
        assert!(html.contains("life"), "should contain repo name: {html}");
    }

    #[test]
    fn run_display_escapes_html() {
        let tool = GitRepoRunTool;
        let input = serde_json::json!({
            "repo": "life",
            "args": ["log", "--format=<b>%s</b>"]
        });
        let html = tool.format_display(&input).unwrap();
        assert!(!html.contains("<b>"), "HTML should be escaped: {html}");
        assert!(html.contains("&lt;b&gt;"), "expected escaped tags: {html}");
    }

    #[test]
    fn commit_summary_shows_approved() {
        let tool = GitRepoCommitAndPushTool;
        let input = serde_json::json!({
            "repos": ["life", "tech"],
            "message": "daily commit"
        });
        let html = tool
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(html.contains("Approved"), "should show Approved: {html}");
        assert!(html.contains("life, tech"), "should list repos: {html}");
        assert!(html.contains("daily commit"), "should show message: {html}");
    }

    #[test]
    fn commit_summary_shows_denied() {
        let tool = GitRepoCommitAndPushTool;
        let input = serde_json::json!({"repos": ["life"], "message": "test"});
        let html = tool
            .format_summary(
                &input,
                &ToolResponseDecision::Deny {
                    reason: Some("no".to_string()),
                },
            )
            .unwrap();
        assert!(html.contains("Denied"), "should show Denied: {html}");
    }

    #[test]
    fn run_summary_shows_command() {
        let tool = GitRepoRunTool;
        let input = serde_json::json!({
            "repo": "life",
            "args": ["log", "--oneline", "-5"]
        });
        let html = tool
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(
            html.contains("log --oneline -5"),
            "should show command: {html}"
        );
        assert!(html.contains("life"), "should show repo: {html}");
    }
}

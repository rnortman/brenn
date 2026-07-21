//! AppTool implementation for `mcp__brenn__ExportUsage`.
//!
//! Handles approval display and summary formatting for usage-export requests.
//! Shows the output path, kind/format, and a warning when the target is in a
//! git-synced mount (may be auto-pushed).

use brenn_lib::app::{AppTool, wrap_in_tool_approve};
use brenn_lib::mcp_tool_names::MCP_EXPORT_USAGE_TOOL;
use brenn_lib::util::html_escape;
use brenn_lib::ws_types::ToolResponseDecision;

/// Registry entry for `mcp__brenn__ExportUsage`.
///
/// Requires user approval before writing a usage export file.
/// Shows the output path, kind/format, and a warning when the target is in a
/// git-synced mount (may be auto-pushed).
pub struct ExportUsageTool;

impl AppTool for ExportUsageTool {
    fn name(&self) -> &str {
        MCP_EXPORT_USAGE_TOOL
    }

    fn format_display(&self, tool_input: &serde_json::Value) -> Option<String> {
        let content = format_export_usage_content(tool_input);
        Some(wrap_in_tool_approve(self.name(), tool_input, &content))
    }

    fn format_summary(
        &self,
        tool_input: &serde_json::Value,
        decision: &ToolResponseDecision,
    ) -> Option<String> {
        let denied = matches!(decision, ToolResponseDecision::Deny { .. });
        let detail = if let Some(value) = tool_input.get("output_file").and_then(|v| v.as_str()) {
            format!(r#"<span class="ts-file">{}</span>"#, html_escape(value))
        } else {
            r#"<span class="ts-error">malformed ExportUsage: missing output_file</span>"#
                .to_string()
        };
        if denied {
            Some(format!(
                r#"<span class="ts-denied" title="Denied">✘</span> {detail}"#
            ))
        } else {
            Some(detail)
        }
    }
}

/// Build the inner HTML for the ExportUsage approval display.
///
/// On missing `output_file`, returns an error div (still wrapped in
/// `<brenn-tool-approve>` by the caller — the user gets approve/deny buttons
/// even for malformed calls).
fn format_export_usage_content(input: &serde_json::Value) -> String {
    let Some(output_file) = input.get("output_file").and_then(|v| v.as_str()) else {
        return r#"<div class="tool-export-error">Malformed ExportUsage call: missing required field output_file</div>"#.to_string();
    };
    let kind = input
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let format = input
        .get("format")
        .and_then(|v| v.as_str())
        .unwrap_or("csv");

    let git_warn = if let Some(slug) = input.get("_git_sync_mount").and_then(|v| v.as_str()) {
        format!(
            r#"<div class="tool-export-warn">Target is in git-synced repo <code>{}</code> — may be auto-pushed</div>"#,
            html_escape(slug)
        )
    } else {
        String::new()
    };

    format!(
        r#"<div class="tool-file">{}</div><div class="tool-export-meta">Export {} ({}) — row scope: your account only</div>{}"#,
        html_escape(output_file),
        html_escape(kind),
        html_escape(format),
        git_warn,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn denied() -> ToolResponseDecision {
        ToolResponseDecision::Deny { reason: None }
    }

    fn allowed() -> ToolResponseDecision {
        ToolResponseDecision::Allow {
            updated_input: None,
        }
    }

    /// Denied + missing output_file → both `ts-error` and `ts-denied` are present.
    /// Verifies internal denied-prefix handling (§3.1) and Some(...) return on error (§3.5).
    #[test]
    fn export_usage_summary_denied_shows_prefix() {
        let tool = ExportUsageTool;
        let input = serde_json::json!({});
        let html = tool.format_summary(&input, &denied()).unwrap();
        assert!(html.contains("ts-error"), "must contain ts-error: {html}");
        assert!(html.contains("ts-denied"), "must contain ts-denied: {html}");
    }

    /// Denied + well-formed input → ts-denied prefix appears.
    #[test]
    fn export_usage_summary_denied_well_formed() {
        let tool = ExportUsageTool;
        let input = serde_json::json!({ "output_file": "/out/usage.csv" });
        let html = tool.format_summary(&input, &denied()).unwrap();
        assert!(html.contains("ts-denied"), "must contain ts-denied: {html}");
        assert!(
            html.contains("/out/usage.csv"),
            "must show output_file: {html}"
        );
    }

    /// Allowed + well-formed input → no ts-denied prefix.
    #[test]
    fn export_usage_summary_allowed_no_denied_prefix() {
        let tool = ExportUsageTool;
        let input = serde_json::json!({ "output_file": "/out/usage.csv" });
        let html = tool.format_summary(&input, &allowed()).unwrap();
        assert!(
            !html.contains("ts-denied"),
            "must not contain ts-denied: {html}"
        );
        assert!(html.contains("/out/usage.csv"), "must show path: {html}");
    }

    /// format_display always returns Some(...) even on malformed input.
    #[test]
    fn export_usage_display_malformed_returns_some_with_error_div() {
        let tool = ExportUsageTool;
        let input = serde_json::json!({});
        let html = tool.format_display(&input).unwrap();
        assert!(
            html.contains("tool-export-error"),
            "must contain error class: {html}"
        );
        assert!(
            html.contains("missing required field output_file"),
            "must contain error message text: {html}"
        );
        assert!(
            html.contains("<brenn-tool-approve"),
            "must be wrapped: {html}"
        );
    }
}

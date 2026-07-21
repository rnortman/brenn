//! Per-tool extension trait.
//!
//! Each tool (AskUserQuestion, pfin reconcile, etc.) implements `AppTool` with
//! per-tool formatting and auto-approve logic. Integrations register their tools via
//! `IntegrationFactory::tools()`. The tool registry is a flat
//! `HashMap<String, Arc<dyn AppTool>>` keyed by tool name.

use crate::approval_rules;
use crate::util::{html_escape, json_for_script_tag};
use crate::ws_types::ToolResponseDecision;

/// Per-tool extension point for app-specific behavior.
///
/// Each interactive tool implements this trait to customize formatting
/// and approval behavior. The tool name must be unique across all registered
/// tools in the global registry.
pub trait AppTool: Send + Sync + 'static {
    /// Tool name as CC knows it, e.g. "AskUserQuestion" or "mcp__pfin__reconcile".
    fn name(&self) -> &str;

    /// If true, auto-approved without user interaction.
    fn auto_approve(&self) -> bool {
        false
    }

    /// Custom HTML for the approval container. Must include an interactive
    /// component that dispatches `brenn-tool-response` events.
    /// Return `None` to use the default (`<brenn-tool-approve>` wrapping a JSON fallback).
    ///
    /// If the formatter encounters unexpected input (missing fields, wrong types,
    /// schema drift), it should return `None` and `warn!()` log the mismatch.
    /// The generic fallback (pretty-printed JSON in `<brenn-tool-approve>`) is always usable.
    fn format_display(&self, tool_input: &serde_json::Value) -> Option<String> {
        let _ = tool_input;
        None
    }

    /// Custom compact summary HTML for chat history.
    /// Return `None` for the generic formatter.
    ///
    /// Same error handling policy as `format_display`: return `None` on
    /// unexpected input, fall back to the generic formatter.
    fn format_summary(
        &self,
        tool_input: &serde_json::Value,
        decision: &ToolResponseDecision,
    ) -> Option<String> {
        let _ = (tool_input, decision);
        None
    }
}

/// An MCP tool that is auto-approved and uses the generic JSON fallback for display.
///
/// Shared by integration crates for read-only or idempotent tools that don't
/// need custom formatting or user approval.
pub struct AutoApproveTool(pub &'static str);

impl AppTool for AutoApproveTool {
    fn name(&self) -> &str {
        self.0
    }

    fn auto_approve(&self) -> bool {
        true
    }
}

/// Wrap tool content HTML in a `<brenn-tool-approve>` component with embedded
/// default patterns for the "Always Allow" UI.
///
/// Used by `AppTool::format_display` implementations that want standard
/// approve/deny/always-allow buttons around their custom content HTML.
/// Interactive components (AskUserQuestion, future pfin-propose) that provide
/// their own interaction do NOT use this.
pub fn wrap_in_tool_approve(
    tool_name: &str,
    tool_input: &serde_json::Value,
    content: &str,
) -> String {
    let patterns = approval_rules::default_patterns(tool_name, tool_input);
    let config = serde_json::json!({ "default_patterns": patterns });
    let config_json = json_for_script_tag(&config);

    format!(
        "<brenn-tool-approve tool-name=\"{name}\">\n\
         <script type=\"application/json\">{config_json}</script>\n\
         {content}\n\
         </brenn-tool-approve>",
        name = html_escape(tool_name),
    )
}

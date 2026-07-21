//! AppTool impls + chat-history HTML formatters for pwa_push tools.
//!
//! Both tools are auto-approved (`PwaPushSend` is budget-gated, not user-approval
//! gated). `PushSendTool` overrides `format_summary` to render a "Push sent"
//! card in chat history. `PwaPushChannelGet` uses `AutoApproveTool` (read-only).

use brenn_lib::app::AppTool;
use brenn_lib::util::html_escape;
use brenn_lib::ws_types::ToolResponseDecision;

use crate::markdown::render_markdown;

pub const MCP_PUSH_SEND_TOOL: &str = "mcp__brenn__PwaPushSend";
pub const MCP_PUSH_LIST_TARGETS_TOOL: &str = "mcp__brenn__PwaPushChannelGet";

/// Auto-approved `PwaPushSend` tool.
///
/// Budget is the control mechanism; no per-call user approval.
pub struct PushSendTool;

impl AppTool for PushSendTool {
    fn name(&self) -> &str {
        MCP_PUSH_SEND_TOOL
    }

    fn auto_approve(&self) -> bool {
        true
    }

    fn format_summary(
        &self,
        tool_input: &serde_json::Value,
        _decision: &ToolResponseDecision,
    ) -> Option<String> {
        let address = tool_input
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let body = tool_input
            .get("body")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let body_html = render_markdown(body);
        // Status badge: intercept injects a synthetic `_outcome` field on the
        // cloned tool_input before emit_tool_summary fires (same pattern as
        // messaging's `_outcome`). Falls back to "Sent" when absent.
        let status_badge = render_push_status_badge(tool_input.get("_outcome"));
        Some(format!(
            r#"<details class="brenn-message brenn-message-sent">
  <summary>
    {status_badge}
    <span class="brenn-msg-from">{address}</span>
  </summary>
  <div class="brenn-msg-body">{body_html}</div>
</details>"#,
            address = html_escape(address),
        ))
    }
}

/// Render the push status badge from the synthetic `_outcome` JSON.
/// Returns a "Sent" badge when absent/malformed.
fn render_push_status_badge(outcome: Option<&serde_json::Value>) -> String {
    let Some(outcome) = outcome else {
        return r#"<span class="brenn-msg-status brenn-msg-status-pending">Sent</span>"#
            .to_string();
    };
    let ok = outcome.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
    if ok {
        let delivered = outcome
            .get("delivered")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let gone = outcome.get("gone").and_then(|v| v.as_u64()).unwrap_or(0);
        let failed = outcome.get("failed").and_then(|v| v.as_u64()).unwrap_or(0);
        let remaining = outcome
            .get("remaining_budget")
            .and_then(|v| v.as_u64())
            .map(|n| format!(" (budget {n})"))
            .unwrap_or_default();
        format!(
            r#"<span class="brenn-msg-status brenn-msg-status-ok">push: {delivered} delivered, {gone} gone, {failed} failed{remaining}</span>"#,
            remaining = html_escape(&remaining),
        )
    } else {
        let err = outcome
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("push failed");
        format!(
            r#"<span class="brenn-msg-status brenn-msg-status-err">{err}</span>"#,
            err = html_escape(err),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_send_tool_is_auto_approve() {
        let t = PushSendTool;
        assert_eq!(t.name(), MCP_PUSH_SEND_TOOL);
        assert!(t.auto_approve());
    }

    #[test]
    fn push_send_summary_includes_address_and_body() {
        let t = PushSendTool;
        let input = serde_json::json!({
            "address": "pwa_push:alice",
            "body": "**hello**",
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(html.contains("brenn-message-sent"));
        assert!(html.contains("pwa_push:alice"));
        assert!(html.contains("<strong>hello</strong>"));
    }

    #[test]
    fn push_send_summary_renders_ok_badge_with_counts() {
        let t = PushSendTool;
        let input = serde_json::json!({
            "address": "pwa_push:alice",
            "body": "hi",
            "_outcome": { "ok": true, "delivered": 2, "gone": 0, "failed": 1, "remaining_budget": 5 },
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(html.contains("brenn-msg-status-ok"));
        assert!(html.contains("2 delivered"));
        assert!(html.contains("1 failed"));
        assert!(html.contains("budget 5"));
    }

    #[test]
    fn push_send_summary_renders_err_badge() {
        let t = PushSendTool;
        let input = serde_json::json!({
            "address": "pwa_push:alice",
            "body": "hi",
            "_outcome": { "ok": false, "error": "budget exhausted: 0 remaining" },
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(html.contains("brenn-msg-status-err"));
        assert!(html.contains("budget exhausted"));
    }

    #[test]
    fn push_send_summary_escapes_html_in_address() {
        let t = PushSendTool;
        let input = serde_json::json!({
            "address": "<script>alert(1)</script>",
            "body": "ok",
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(!html.contains("<script>alert"));
        assert!(html.contains("&lt;script&gt;"));
    }

    #[test]
    fn push_send_summary_renders_pending_badge_when_outcome_absent() {
        let t = PushSendTool;
        let input = serde_json::json!({ "address": "pwa_push:x", "body": "hi" });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(html.contains("brenn-msg-status-pending"));
        assert!(html.contains(">Sent<"));
    }
}

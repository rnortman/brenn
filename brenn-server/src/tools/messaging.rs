//! AppTool impls + chat-history HTML formatters for the messaging MVP.
//!
//! All three messaging tools are auto-approved (the budget is the
//! control, not per-call user approval — see design §3.4 / §7.1.1).
//! `BrennSend` is the only one with a custom `AppTool` impl: it
//! overrides `format_summary` to render a "Sent message" card in chat
//! history. The other two reuse `AutoApproveTool`.

use brenn_lib::app::AppTool;
use brenn_lib::messaging::MessageEnvelope;
use brenn_lib::util::html_escape;
use brenn_lib::ws_types::ToolResponseDecision;

use crate::markdown::render_markdown;

pub const MCP_MESSAGE_LIST_CHANNELS_TOOL: &str = "mcp__brenn__MessageChannelList";
pub const MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL: &str = "mcp__brenn__MessageSubscriptionList";
pub const MCP_MESSAGE_SEND_TOOL: &str = "mcp__brenn__BrennSend";
pub const MCP_MESSAGE_QUERY_CHANNEL_TOOL: &str = "mcp__brenn__MessageChannelGet";
pub const MCP_MESSAGE_SUBSCRIBE_TOOL: &str = "mcp__brenn__MessageSubscribe";
pub const MCP_MESSAGE_UNSUBSCRIBE_TOOL: &str = "mcp__brenn__MessageUnsubscribe";
pub const MCP_MESSAGE_PENDING_LIST_TOOL: &str = "mcp__brenn__BrennPendingList";
pub const MCP_MESSAGE_CANCEL_TOOL: &str = "mcp__brenn__BrennMessageCancel";
pub const MCP_MESSAGE_EDIT_TOOL: &str = "mcp__brenn__BrennMessageEdit";

/// Pseudo-tool name used as `tool_name` on chat-history cards emitted
/// for *received* messages. No real MCP tool has this name — it's a
/// tag that lets the chat-history renderer / frontend style inbound
/// messages distinctly from outbound `BrennSend` summaries. See
/// `active_bridge::drain_pending_events`.
pub const MCP_MESSAGE_RECEIVED_PSEUDO_TOOL: &str = "mcp__brenn__MessageReceived";

/// Auto-approved `BrennSend` tool.
///
/// `auto_approve` returns `true` so PreToolUse fast-paths to Allow and no
/// user-approval card is rendered. The custom `format_summary` impl
/// produces the "Sent message" chat-history card after the publish has
/// already executed.
pub struct MessageSendTool;

impl AppTool for MessageSendTool {
    fn name(&self) -> &str {
        MCP_MESSAGE_SEND_TOOL
    }

    fn auto_approve(&self) -> bool {
        true
    }

    fn format_summary(
        &self,
        tool_input: &serde_json::Value,
        _decision: &ToolResponseDecision,
    ) -> Option<String> {
        let to = tool_input.get("to").and_then(|v| v.as_str()).unwrap_or("");
        let body = tool_input
            .get("body")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let wake = tool_input
            .get("wake")
            .and_then(|v| v.as_str())
            .unwrap_or("none");
        let body_html = render_markdown(body);
        // Status badge: design §8.1 requires the card to show the final
        // outcome (delivered / budget exhausted / etc.). The intercept
        // injects a synthetic `_outcome` field on the cloned tool_input
        // immediately before emit_tool_summary fires, carrying the
        // PublishResult JSON. If `_outcome` is absent (e.g., a future
        // caller renders the card without going through
        // `messaging_intercept`), fall back to "Sent".
        let status_badge = render_status_badge(tool_input.get("_outcome"));
        Some(format!(
            r#"<details class="brenn-message brenn-message-sent">
  <summary>
    {status_badge}
    <span class="brenn-msg-from">{to}</span>
    <span class="brenn-msg-wake">wake: {wake}</span>
  </summary>
  <div class="brenn-msg-body">{body_html}</div>
</details>"#,
            to = html_escape(to),
            wake = html_escape(wake),
        ))
    }
}

/// Render the status-badge `<span>` from the synthetic `_outcome` JSON
/// the intercept injects. Returns `Sent` for absent / malformed
/// outcomes (no-op fallback) and a descriptive class+text for known
/// outcomes.
fn render_status_badge(outcome: Option<&serde_json::Value>) -> String {
    let Some(outcome) = outcome else {
        return r#"<span class="brenn-msg-status brenn-msg-status-pending">Sent</span>"#
            .to_string();
    };
    let ok = outcome.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
    if ok {
        let remaining = outcome
            .get("remaining_budget")
            .and_then(|v| v.as_u64())
            .map(|n| format!(" (budget {n})"))
            .unwrap_or_default();
        format!(
            r#"<span class="brenn-msg-status brenn-msg-status-ok">delivered{remaining}</span>"#,
            remaining = html_escape(&remaining),
        )
    } else {
        let err = outcome
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("send failed");
        format!(
            r#"<span class="brenn-msg-status brenn-msg-status-err">{err}</span>"#,
            err = html_escape(err),
        )
    }
}

/// Render an HTML chat-history card for a single received message.
/// Used when an active bridge accepts a delivered message.
pub fn format_message_html_single(env: &MessageEnvelope) -> String {
    let body_html = render_markdown(&env.body);
    let publish_ts = env.publish_ts.to_rfc3339();
    format!(
        r#"<details class="brenn-message brenn-message-recv">
  <summary>
    <span class="brenn-msg-from">{channel}</span>
    <span class="brenn-msg-sender">{sender}</span>
    <time>{publish_ts}</time>
  </summary>
  <div class="brenn-msg-body">{body_html}</div>
</details>"#,
        channel = html_escape(&env.channel),
        sender = html_escape(&env.sender),
        publish_ts = html_escape(&publish_ts),
    )
}

/// Render a batch of received messages as a wrapped `<div>` containing
/// one `<details>` per message. Used on wake.
pub fn format_message_batch_html(envelopes: &[MessageEnvelope]) -> String {
    let mut html = String::from(r#"<div class="brenn-message-batch">"#);
    for env in envelopes {
        html.push_str(&format_message_html_single(env));
    }
    html.push_str("</div>");
    html
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::messaging::Urgency;
    use chrono::Utc;
    use uuid::Uuid;

    fn fake_envelope(body: &str) -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: "host".into(),
            channel: "brenn:pa-bob".into(),
            sender: "bob-pa".into(),
            publish_ts: Utc::now(),
            body: body.into(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Normal,
            envelope_type: brenn_lib::messaging::ChannelScheme::Brenn,
        }
    }

    #[test]
    fn message_send_tool_is_auto_approve() {
        let t = MessageSendTool;
        assert_eq!(t.name(), MCP_MESSAGE_SEND_TOOL);
        assert!(t.auto_approve());
    }

    #[test]
    fn message_send_summary_includes_to_wake_and_body_html() {
        let t = MessageSendTool;
        let input = serde_json::json!({
            "to": "brenn:pa-bob",
            "body": "**hi**",
            "wake": "immediate"
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
        assert!(html.contains("brenn:pa-bob"));
        assert!(html.contains("wake: immediate"));
        assert!(html.contains("<strong>hi</strong>"));
    }

    #[test]
    fn message_send_summary_escapes_html_in_to_field() {
        let t = MessageSendTool;
        let input = serde_json::json!({
            "to": "<script>alert(1)</script>",
            "body": "ok"
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

    /// Body is the largest LLM-controlled surface; raw HTML inside the
    /// markdown source must be stripped (`render_markdown` policy) so it
    /// can't escape via the `<div class="brenn-msg-body">` injection
    /// site (review F24).
    #[test]
    fn message_send_summary_escapes_html_in_body() {
        let t = MessageSendTool;
        let input = serde_json::json!({
            "to": "brenn:pa-bob",
            "body": "<script>alert('xss')</script>",
        });
        let html = t
            .format_summary(
                &input,
                &ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .unwrap();
        assert!(
            !html.contains("<script>alert"),
            "body markdown raw HTML must be stripped: {html}"
        );
    }

    /// F12: outcome plumbing — `_outcome.ok = true` + `remaining_budget`
    /// produces a "delivered" badge with the budget appended.
    #[test]
    fn message_send_summary_renders_delivered_badge_on_ok_outcome() {
        let t = MessageSendTool;
        let input = serde_json::json!({
            "to": "brenn:pa-bob",
            "body": "hi",
            "_outcome": { "ok": true, "remaining_budget": 99 },
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
        assert!(html.contains("delivered"));
        assert!(html.contains("budget 99"));
    }

    /// F12: failed outcomes render the error string in the badge.
    #[test]
    fn message_send_summary_renders_error_badge_on_failed_outcome() {
        let t = MessageSendTool;
        let input = serde_json::json!({
            "to": "brenn:pa-bob",
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

    /// Without `_outcome` (no intercept plumbing yet) the badge falls
    /// back to "Sent" with a pending class.
    #[test]
    fn message_send_summary_renders_pending_badge_when_outcome_absent() {
        let t = MessageSendTool;
        let input = serde_json::json!({ "to": "brenn:x", "body": "hi" });
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

    #[test]
    fn format_message_html_single_renders_markdown_body() {
        let html = format_message_html_single(&fake_envelope("**bold**"));
        assert!(html.contains("brenn-message-recv"));
        assert!(html.contains("brenn:pa-bob"));
        assert!(html.contains("<strong>bold</strong>"));
    }

    #[test]
    fn format_message_batch_html_wraps_multiple_details() {
        let html = format_message_batch_html(&[fake_envelope("first"), fake_envelope("second")]);
        assert!(html.starts_with(r#"<div class="brenn-message-batch">"#));
        // Two <details> elements present.
        let count = html.matches("brenn-message-recv").count();
        assert_eq!(count, 2);
    }
}

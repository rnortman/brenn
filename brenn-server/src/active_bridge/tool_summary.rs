//! Tool-summary emission: tool_use → tool_result rendering, persistence, and broadcast.

use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
use brenn_lib::approval_rules::ApprovalMatch;
use brenn_lib::conversation::{self, MessageDirection};
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::ws_types::{ToolResponseDecision, WsServerMessage};
use tracing::{info, warn};

use super::ActiveBridge;

/// Approval outcome stored when Brenn handles the permission decision (auto-approve
/// or manual user approval). Consumed by the ToolResult handler for enrichment
/// (showing which rule approved the tool). Optional — CC-internal auto-approvals
/// won't have one.
pub(crate) struct ApprovalOutcome {
    pub(super) approval_match: ApprovalMatch,
}

/// Tool invocation context, populated from the assistant message's `tool_use`
/// content blocks. This is the source of truth for "what tools has CC invoked."
/// Keyed by tool_use_id, consumed when the ToolResult arrives.
pub(crate) struct PendingToolUse {
    pub(super) tool_name: String,
    pub(super) tool_input: serde_json::Value,
}

/// Result from `handle_brenn_tools`.
#[derive(Debug)]
pub(super) enum HandleBrennToolResult {
    /// Send this decision back to CC immediately.
    Respond(CcApprovalDecision),
    /// Persist to DB and broadcast to browser. Returns immediately to CC
    /// with `{"request_id":"..."}`. User's decision arrives later.
    PersistAndBroadcast {
        tool_name: String,
        tool_input: serde_json::Value,
        /// Tool-specific extra data (e.g. enrichment_failures for BatchReconcile).
        extra: Option<String>,
    },
}

/// Mark a tool_use_id as handled by a specialized path (e.g., noop tools that
/// emit their own summaries from PostToolUse). Consumes from `pending_tool_uses`
/// and inserts into `handled_tool_uses` so the ToolResult handler knows to skip it.
/// Remove `tool_use_id` from the pending set and record it as handled.
/// Used by intercept modules (messaging, pwa_push) to acknowledge tool calls
/// before returning a `Continue` decision.
pub(crate) async fn mark_tool_handled(bridge: &ActiveBridge, tool_use_id: &str) {
    bridge.pending_tool_uses.lock().await.remove(tool_use_id);
    bridge
        .handled_tool_uses
        .lock()
        .await
        .insert(tool_use_id.to_string());
}

/// Render and broadcast a tool-use summary for an auto-approved global tool.
/// All intercept modules (messaging, pwa_push) use `ApprovalMatch::GlobalTool`
/// with `tool_response = None`.
pub(crate) async fn emit_tool_summary_for_intercept(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
    is_error: bool,
) {
    emit_tool_summary(
        bridge,
        tool_name,
        tool_input,
        None,
        Some(&ApprovalMatch::GlobalTool),
        is_error,
    )
    .await;
}

/// Persist a pre-rendered tool-use summary HTML and broadcast it.
///
/// Sibling helper to `emit_tool_summary` that takes an already-rendered
/// HTML string (e.g., for the messaging "received message" pseudo-tool
/// where there's no `tool_input` to format from) and skips the
/// detail-pane rendering. Encapsulates the persist+broadcast pair so the
/// drain path doesn't have to duplicate `emit_tool_summary`'s body.
pub(super) async fn emit_prerendered_summary(
    bridge: &ActiveBridge,
    tool_name: &str,
    rendered: String,
) {
    let payload = serde_json::json!({
        "tool_name": tool_name,
        "rendered_summary": rendered,
        "detail_html": serde_json::Value::Null,
    });
    let db_seq = {
        let conn = bridge.db.lock().await;
        let (_id, seq) = conversation::append_message(
            &conn,
            bridge.conversation_id,
            MessageDirection::Incoming,
            "tool_summary",
            None,
            None,
            &payload.to_string(),
            None,
            None,
            None,
        );
        seq
    };
    // seq: Some(db_seq) lets the frontend deduplicate this live broadcast against
    // a concurrent history replay (reconnect-from-idle race fix).
    bridge.broadcast(WsServerMessage::ToolUseSummary {
        tool_name: tool_name.to_string(),
        rendered_summary: rendered,
        detail_html: None,
        seq: Some(db_seq),
    });
}

/// Render, persist, and broadcast a tool-use summary for the chat history.
///
/// Emit a tool-use summary to chat history and broadcast to connected clients.
///
/// Called from `emit_tool_result_summaries` (for generic tools) and from
/// brenn noop tool PostToolUse handlers (DisplayFile, ProposeReconciliation, etc.).
/// `approval_match` describes how the tool was approved (auto-rule, manual, etc.).
/// `None` means CC auto-approved internally — the detail view will say so.
pub(super) async fn emit_tool_summary(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
    tool_response: Option<&serde_json::Value>,
    approval_match: Option<&ApprovalMatch>,
    is_error: bool,
) {
    // All PostToolUse summaries are "allowed" — the tool ran.
    let decision = ToolResponseDecision::Allow {
        updated_input: None,
    };
    let mut rendered = crate::approval_formatter::format_tool_summary(
        &bridge.tool_registry,
        tool_name,
        tool_input,
        &decision,
    );

    // When the tool errored, prefix the summary with an error indicator so the
    // user can see immediately that it failed.
    if is_error {
        rendered = format!(r#"<span class="ts-denied" title="Error">✘</span> {rendered}"#,);
    }

    // Detail HTML: always included so users can expand to see input/result.
    // Custom summaries control the collapsed one-line view; detail is the
    // expanded view with full input JSON and tool response.
    let detail_html = Some(crate::approval_formatter::format_tool_detail(
        tool_input,
        tool_response,
        &decision,
        approval_match,
    ));

    let payload = serde_json::json!({
        "tool_name": tool_name,
        "rendered_summary": rendered,
        "detail_html": detail_html,
    });
    let db_seq = {
        let conn = bridge.db.lock().await;
        let (_id, seq) = conversation::append_message(
            &conn,
            bridge.conversation_id,
            MessageDirection::Incoming,
            "tool_summary",
            None,
            None,
            &payload.to_string(),
            None,
            None,
            None,
        );
        seq
    };
    // seq: Some(db_seq) lets the frontend deduplicate this live broadcast against
    // a concurrent history replay (reconnect-from-idle race fix).
    bridge.broadcast(WsServerMessage::ToolUseSummary {
        tool_name: tool_name.to_string(),
        rendered_summary: rendered,
        detail_html,
        seq: Some(db_seq),
    });
}

/// Emit tool-use summaries when CC feeds tool results back to the model.
///
/// This is the canonical path for summary emission — ToolResult always fires
/// for every tool invocation (CC needs it to continue the conversation).
/// PostToolUse hooks are unreliable (CC skips them on tool errors).
///
/// Tool name/input come from `pending_tool_uses` (populated when the assistant
/// message arrived). Approval info comes from `approval_outcomes` as optional
/// enrichment — absent means CC auto-approved internally, which is fine.
///
/// Brenn noop tools (DisplayFile, ProposeReconciliation, BatchReconcile) emit
/// their summaries from PostToolUse instead, since they have special handling
/// and their ToolResult is just `__NOOP__`.
pub(super) async fn emit_tool_result_summaries(
    bridge: &ActiveBridge,
    msg: &brenn_cc::protocol::incoming::UserMessage,
    alert_dispatcher: &AlertDispatcher,
) {
    // Extract tool_use_id from the tool_result content block.
    let content = match msg.message.get("content").and_then(|c| c.as_array()) {
        Some(c) => c,
        None => return,
    };

    for block in content {
        if block.get("type").and_then(|t| t.as_str()) != Some("tool_result") {
            continue;
        }
        let tool_use_id = match block.get("tool_use_id").and_then(|id| id.as_str()) {
            Some(id) => id,
            None => continue,
        };

        // Extract tool response from the content block. The "content" field
        // may be a string or structured; wrap in a JSON value for display.
        let tool_response = block.get("content").cloned().map(|c| {
            if c.is_string() {
                // Wrap string content as a text content block array for
                // consistent display in format_tool_detail.
                serde_json::json!([{"type": "text", "text": c}])
            } else {
                c
            }
        });

        let is_error = block
            .get("is_error")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Check if this tool was already handled by a specialized path (e.g., noop
        // tools that emit their own summaries from PostToolUse).
        {
            let mut handled = bridge.handled_tool_uses.lock().await;
            if handled.remove(tool_use_id) {
                continue;
            }
        }

        // Look up the tool invocation context from the assistant message.
        let pending = {
            let mut pending = bridge.pending_tool_uses.lock().await;
            pending.remove(tool_use_id)
        };

        let (tool_name, tool_input) = match pending {
            Some(p) => (p.tool_name, p.tool_input),
            None => {
                // Protocol violation: ToolResult for a tool_use_id we never saw
                // in an assistant message. Display a degraded summary and alert.
                warn!(
                    tool_use_id = %tool_use_id,
                    "ToolResult for unknown tool_use_id — no matching assistant tool_use block"
                );
                alert_dispatcher.alert(
                    brenn_lib::obs::alerting::AlertSeverity::Warning,
                    "ToolResult for unknown tool_use_id".into(),
                    format!(
                        "Received a ToolResult for tool_use_id {tool_use_id} but no matching \
                         tool_use block was seen in an assistant message. This may indicate a CC \
                         protocol change or a Brenn bug."
                    ),
                );
                // Best-effort degraded display.
                ("unknown".to_string(), serde_json::json!({}))
            }
        };

        // Optionally enrich with approval info (which rule approved it).
        // Absent = CC auto-approved internally; show "Auto-approved by CC" in detail.
        let approval_match = {
            let mut outcomes = bridge.approval_outcomes.lock().await;
            outcomes.remove(tool_use_id).map(|o| o.approval_match)
        };

        info!(
            tool = %tool_name,
            tool_use_id = %tool_use_id,
            "emitting tool summary from ToolResult"
        );

        emit_tool_summary(
            bridge,
            &tool_name,
            &tool_input,
            tool_response.as_ref(),
            approval_match.as_ref(),
            is_error,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        await_fence, drain_broadcast, event_fence, test_bridge, test_bridge_with_cwd,
    };
    use super::*;
    use brenn_cc::session::{ApprovalKind, ApprovalRequest, SessionEvent};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn post_tool_use_sends_continue_no_summary() {
        // PostToolUse just responds Continue — summary emission moved to ToolResult.
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_post".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "cb_1".into(),
                tool_name: "Read".into(),
                tool_input: serde_json::json!({"file_path": "/tmp/foo"}),
                tool_response: serde_json::json!("file contents"),
                tool_use_id: "t_post".into(),
            },
            response_tx: resp_tx,
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Must send Continue (not Allow) for PostToolUse.
        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Continue { .. }),
            "PostToolUse should send Continue, got {decision:?}"
        );

        // PostToolUse no longer emits summaries — that's done by ToolResult.
        await_fence(fence).await;
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert_eq!(msgs.len(), 0, "PostToolUse should not broadcast anything");
    }

    #[tokio::test]
    async fn tool_result_emits_summary_for_auto_approved_tool() {
        // When a tool is auto-approved (via Permission handler) and then completes,
        // the ToolResult handler should emit a ToolUseSummary.
        use brenn_cc::protocol::incoming::{
            AssistantContent, AssistantMessage, ContentBlock, UserMessage,
        };

        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Step 0: Send AssistantMessage with tool_use block to populate pending_tool_uses.
        let assistant_msg = AssistantMessage {
            uuid: "ast_1".into(),
            parent_tool_use_id: None,
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![ContentBlock::ToolUse {
                    id: "t_read_1".into(),
                    name: "Read".into(),
                    input: serde_json::json!({"file_path": "/tmp/foo"}),
                }],
                model: None,
                usage: None,
            },
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::AssistantMessage(assistant_msg))
            .await
            .unwrap();
        await_fence(fence).await;
        // Drain the AssistantMessage + Status broadcasts.
        drain_broadcast(&mut broadcast_rx);

        // Step 1: Auto-approve the tool via Permission handler.
        // Use "Read" which matches a global tool in the test config.
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_perm".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Read".into(),
                input: serde_json::json!({"file_path": "/tmp/foo"}),
                tool_use_id: "t_read_1".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Allow { .. }),
            "Read should be auto-approved"
        );

        // Step 2: Send ToolResult — this should emit the summary.
        let tool_result_msg = UserMessage {
            message: serde_json::json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "t_read_1",
                    "content": "file contents here"
                }]
            }),
            uuid: None,
            session_id: None,
            tool_use_result: Some(serde_json::json!("file contents here")),
            extra: serde_json::json!({}),
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ToolResult(tool_result_msg))
            .await
            .unwrap();
        await_fence(fence).await;
        let msgs = drain_broadcast(&mut broadcast_rx);
        let summary_count = msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ToolUseSummary { .. }))
            .count();
        assert_eq!(summary_count, 1, "should emit exactly one ToolUseSummary");
        assert!(
            matches!(&msgs.last().unwrap(), WsServerMessage::ToolUseSummary { tool_name, .. } if tool_name == "Read"),
            "should be a ToolUseSummary for Read"
        );
    }

    #[tokio::test]
    async fn tool_result_emits_summary_for_errored_tool() {
        // When a tool returns is_error:true, CC skips PostToolUse. The ToolResult
        // handler should still emit a summary.
        use brenn_cc::protocol::incoming::{
            AssistantContent, AssistantMessage, ContentBlock, UserMessage,
        };

        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Send AssistantMessage with tool_use block to populate pending_tool_uses.
        let assistant_msg = AssistantMessage {
            uuid: "ast_err".into(),
            parent_tool_use_id: None,
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![ContentBlock::ToolUse {
                    id: "t_err_1".into(),
                    name: "Read".into(),
                    input: serde_json::json!({"file_path": "/nonexistent"}),
                }],
                model: None,
                usage: None,
            },
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::AssistantMessage(assistant_msg))
            .await
            .unwrap();
        await_fence(fence).await;
        drain_broadcast(&mut broadcast_rx);

        // Auto-approve.
        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_perm2".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Read".into(),
                input: serde_json::json!({"file_path": "/nonexistent"}),
                tool_use_id: "t_err_1".into(),
            },
            response_tx: resp_tx,
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        await_fence(fence).await;

        // Send errored ToolResult.
        let tool_result_msg = UserMessage {
            message: serde_json::json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "t_err_1",
                    "content": "Error: file not found",
                    "is_error": true
                }]
            }),
            uuid: None,
            session_id: None,
            tool_use_result: Some(serde_json::json!("Error: file not found")),
            extra: serde_json::json!({}),
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ToolResult(tool_result_msg))
            .await
            .unwrap();
        await_fence(fence).await;
        let msgs = drain_broadcast(&mut broadcast_rx);
        let summary_count = msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ToolUseSummary { .. }))
            .count();
        assert_eq!(
            summary_count, 1,
            "should emit summary even for errored tools"
        );
    }

    #[tokio::test]
    async fn tool_result_emits_summary_without_permission_event() {
        // Core bug fix: when CC auto-approves a tool internally (no CanUseTool/
        // Permission event sent to Brenn), the ToolResult handler should still
        // emit a ToolUseSummary. This was the original silent-skip bug.
        use brenn_cc::protocol::incoming::{
            AssistantContent, AssistantMessage, ContentBlock, UserMessage,
        };

        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Step 1: AssistantMessage with tool_use block.
        let assistant_msg = AssistantMessage {
            uuid: "ast_cc_auto".into(),
            parent_tool_use_id: None,
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![ContentBlock::ToolUse {
                    id: "t_cc_auto_1".into(),
                    name: "Bash".into(),
                    input: serde_json::json!({"command": "git status"}),
                }],
                model: None,
                usage: None,
            },
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::AssistantMessage(assistant_msg))
            .await
            .unwrap();
        await_fence(fence).await;
        drain_broadcast(&mut broadcast_rx);

        // Step 2: ToolResult arrives directly — no Permission event in between.
        // This simulates CC auto-approving the tool via its own settings.
        let tool_result_msg = UserMessage {
            message: serde_json::json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "t_cc_auto_1",
                    "content": "On branch main\nnothing to commit"
                }]
            }),
            uuid: None,
            session_id: None,
            tool_use_result: Some(serde_json::json!("On branch main\nnothing to commit")),
            extra: serde_json::json!({}),
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ToolResult(tool_result_msg))
            .await
            .unwrap();
        await_fence(fence).await;
        let msgs = drain_broadcast(&mut broadcast_rx);
        let summary_count = msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ToolUseSummary { .. }))
            .count();
        assert_eq!(
            summary_count, 1,
            "should emit ToolUseSummary even without a Permission event"
        );
        assert!(
            matches!(&msgs.last().unwrap(), WsServerMessage::ToolUseSummary { tool_name, .. } if tool_name == "Bash"),
            "should be a ToolUseSummary for Bash"
        );
    }

    #[tokio::test]
    async fn tool_result_degraded_summary_on_unknown_tool_use_id() {
        // When a ToolResult arrives for a tool_use_id that was never seen in an
        // assistant message (and isn't in handled_tool_uses), we should still emit
        // a degraded summary — never silently skip.
        use brenn_cc::protocol::incoming::UserMessage;

        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Send a ToolResult with NO preceding AssistantMessage.
        let tool_result_msg = UserMessage {
            message: serde_json::json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "t_phantom",
                    "content": "some output"
                }]
            }),
            uuid: None,
            session_id: None,
            tool_use_result: Some(serde_json::json!("some output")),
            extra: serde_json::json!({}),
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ToolResult(tool_result_msg))
            .await
            .unwrap();
        await_fence(fence).await;
        let msgs = drain_broadcast(&mut broadcast_rx);

        // Should still emit a ToolUseSummary (degraded, with tool_name "unknown").
        let summaries: Vec<_> = msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ToolUseSummary { .. }))
            .collect();
        assert_eq!(
            summaries.len(),
            1,
            "should emit degraded summary, not silently skip"
        );
        match summaries[0] {
            WsServerMessage::ToolUseSummary { tool_name, .. } => {
                assert_eq!(
                    tool_name, "unknown",
                    "degraded summary should use 'unknown'"
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn noop_tool_does_not_emit_duplicate_summary() {
        // Noop tools (DisplayFile etc.) emit their summary from PostToolUse.
        // When the ToolResult arrives later, it should NOT emit a second summary.
        use super::super::mcp_constants::MCP_DISPLAY_FILE_TOOL;
        use brenn_cc::protocol::incoming::{
            AssistantContent, AssistantMessage, ContentBlock, UserMessage,
        };

        let (_bridge, event_tx, mut broadcast_rx) = test_bridge_with_cwd("/tmp").await;

        // Create the test file so DisplayFile doesn't error.
        let test_file = std::path::PathBuf::from("/tmp/test_noop_dedup.txt");
        std::fs::write(&test_file, "test content").unwrap();

        // Step 1: AssistantMessage with DisplayFile tool_use.
        let assistant_msg = AssistantMessage {
            uuid: "ast_noop".into(),
            parent_tool_use_id: None,
            message: AssistantContent {
                role: "assistant".into(),
                content: vec![ContentBlock::ToolUse {
                    id: "t_display_1".into(),
                    name: MCP_DISPLAY_FILE_TOOL.into(),
                    input: serde_json::json!({"file_path": "/tmp/test_noop_dedup.txt"}),
                }],
                model: None,
                usage: None,
            },
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::AssistantMessage(assistant_msg))
            .await
            .unwrap();
        await_fence(fence).await;

        // Step 2: PreToolUse hook for DisplayFile.
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_pre_display".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({"file_path": "/tmp/test_noop_dedup.txt"}),
                tool_use_id: "t_display_1".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        let _ = resp_rx.await.unwrap();

        // Step 3: PostToolUse hook for DisplayFile — this emits the summary.
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_post_display".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_DISPLAY_FILE_TOOL.into(),
                tool_input: serde_json::json!({"file_path": "/tmp/test_noop_dedup.txt"}),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_display_1".into(),
            },
            response_tx: resp_tx,
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        let _ = resp_rx.await.unwrap();
        await_fence(fence).await;

        // Drain: should have AssistantMessage + Status + ArtifactDisplay + ToolUseSummary.
        let pre_msgs = drain_broadcast(&mut broadcast_rx);
        let pre_summary_count = pre_msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ToolUseSummary { .. }))
            .count();
        assert_eq!(
            pre_summary_count, 1,
            "PostToolUse should emit exactly one summary"
        );

        // Step 4: ToolResult arrives — should NOT emit a second summary.
        let tool_result_msg = UserMessage {
            message: serde_json::json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "t_display_1",
                    "content": "File displayed"
                }]
            }),
            uuid: None,
            session_id: None,
            tool_use_result: Some(serde_json::json!("File displayed")),
            extra: serde_json::json!({}),
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ToolResult(tool_result_msg))
            .await
            .unwrap();
        await_fence(fence).await;
        let post_msgs = drain_broadcast(&mut broadcast_rx);
        let post_summary_count = post_msgs
            .iter()
            .filter(|m| matches!(m, WsServerMessage::ToolUseSummary { .. }))
            .count();
        assert_eq!(
            post_summary_count, 0,
            "ToolResult should NOT emit a duplicate summary for handled noop tool"
        );

        // Clean up.
        let _ = std::fs::remove_file(&test_file);
    }
}

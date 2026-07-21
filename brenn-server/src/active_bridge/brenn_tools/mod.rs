//! Per-tool-family handlers for brenn-namespaced MCP tools.
//!
//! The dispatcher (`handle_brenn_tools`) routes `ApprovalRequest` events from CC to
//! per-family handlers, each living in its own submodule. Each family handler has the
//! signature
//!
//! ```ignore
//! pub(super) async fn handle(
//!     bridge: &ActiveBridge,
//!     req: &ApprovalRequest,
//! ) -> Option<HandleBrennToolResult>
//! ```
//!
//! and matches both the `PreToolUse` and `PostToolUse` arms for its tool(s).
//! Returning `Some(...)` means the request is for this family; the dispatcher
//! returns immediately with that result. Returning `None` lets the dispatcher
//! fall through to the next family or the inline match arms (which will be
//! migrated to per-family modules in subsequent phases).
//!
//! Phase 6.1 extracts only `compaction_tool` to validate the pattern.
//! Subsequent phases extract the remaining families (display_file,
//! propose_reconciliation, batch_*, git_repo_*, device_*, export_usage).

use brenn_cc::session::ApprovalRequest;

use super::ActiveBridge;
use super::tool_summary::HandleBrennToolResult;

mod compaction_tool;
mod device;
mod display_file;
mod export_usage;
mod git;
mod pfin;
mod registry_adapter;
mod timezone;

pub(crate) use pfin::render_pending_tool_request;

/// Handle brenn noop MCP tool interception (DisplayFile, ProposeReconciliation).
///
/// Returns `Some(decision)` if this was a brenn MCP tool, `None` otherwise.
///
/// Pattern: CC calls the brenn noop MCP server that returns `__NOOP__`. We intercept
/// via PreToolUse (do real work or grant permission) and PostToolUse (persist to DB
/// and return immediately with `{"request_id":"..."}`).
///
/// For interactive tools (ProposeReconciliation, BatchReconcile), PostToolUse persists
/// the request to `pending_tool_requests` in the DB and returns immediately. The user's
/// response arrives later as an ApprovalResponse and is handled by handle_async_tool_response.
pub(in crate::active_bridge) async fn handle_brenn_tools(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<HandleBrennToolResult> {
    // First-class tool registry adapter. Runs ahead of the legacy per-family
    // handlers: any tool name that resolves to a registered tool is owned here.
    if let Some(result) = registry_adapter::handle(bridge, req).await {
        return Some(result);
    }
    // Try the messaging intercepts first. They cover all three messaging
    // tools (auto-approved per design §3.4 / §7.1.1).
    if let Some(crate::messaging_intercept::MessagingHandled::Respond(decision)) =
        crate::messaging_intercept::try_handle_messaging_tool(bridge, req).await
    {
        return Some(HandleBrennToolResult::Respond(decision));
    }
    // Try automation intercepts: AutoCreate, AutoList, AutoEdit, AutoDelete.
    if let Some(crate::automation_intercept::AutomationHandled::Respond(decision)) =
        crate::automation_intercept::try_handle_automation_tool(bridge, req).await
    {
        return Some(HandleBrennToolResult::Respond(decision));
    }
    // Try pwa_push intercepts: PushSend + PushListTargets.
    if let Some(crate::pwa_push_intercept::PwaPushHandled::Respond(decision)) =
        crate::pwa_push_intercept::try_handle_pwa_push_tool(bridge, req).await
    {
        return Some(HandleBrennToolResult::Respond(decision));
    }
    // Try MQTT intercepts: MqttSend.
    if let Some(crate::mqtt_intercept::MqttHandled::Respond(decision)) =
        crate::mqtt_intercept::try_handle_mqtt_tool(bridge, req).await
    {
        return Some(HandleBrennToolResult::Respond(decision));
    }
    // Try RequestCompaction (per-family handler — phase 6.1 extraction).
    if let Some(result) = compaction_tool::handle(bridge, req).await {
        return Some(result);
    }
    // Try DisplayFile (per-family handler — phase 6.2 extraction).
    if let Some(result) = display_file::handle(bridge, req).await {
        return Some(result);
    }
    // Try Device tools (per-family handler — phase 6.3 extraction).
    if let Some(result) = device::handle(bridge, req).await {
        return Some(result);
    }
    // Try Git tools (per-family handler — phase 6.4 extraction).
    if let Some(result) = git::handle(bridge, req).await {
        return Some(result);
    }
    // Try ExportUsage (per-family handler — phase 6.5 extraction).
    if let Some(result) = export_usage::handle(bridge, req).await {
        return Some(result);
    }
    // Try pfin tools: ProposeReconciliation, BatchReconcile, BatchAssign
    // (per-family handler — phase 6.6 extraction).
    if let Some(result) = pfin::handle(bridge, req).await {
        return Some(result);
    }
    // Try SetUserTimezone (timezone override tool).
    if let Some(result) = timezone::handle(bridge, req).await {
        return Some(result);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_cc::session::{ApprovalKind, ApprovalRequest};
    use tokio::sync::oneshot;

    use super::super::test_support::test_bridge;

    #[tokio::test]
    async fn non_brenn_tool_returns_none() {
        // A regular tool should not be intercepted by handle_brenn_tools.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_bash".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "hook_0".into(),
                tool_name: "Bash".into(),
                tool_input: serde_json::json!({"command": "ls"}),
                tool_use_id: "t_bash".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        assert!(result.is_none(), "non-brenn tool should return None");
    }
}

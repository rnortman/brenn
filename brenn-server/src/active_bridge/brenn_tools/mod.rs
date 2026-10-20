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

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use tracing::warn;

use super::ActiveBridge;
use super::mcp_constants::MCP_BRENN_PREFIX;
use super::tool_summary::{
    HandleBrennToolResult, emit_tool_summary_for_intercept, mark_tool_handled,
};

mod compaction_tool;
mod device;
mod display_file;
mod execute;
mod export_usage;
mod git;
mod pfin;
mod registry_adapter;
mod timezone;

pub(in crate::active_bridge) use execute::{CallOrigin, ExecuteError, execute_tool};
pub(crate) use execute::{ToolOutcome, execute_admitted};
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
    if let Some(refusal) = refuse_inadmissible_post(bridge, req).await {
        return Some(refusal);
    }
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

/// Admission for the hook path: every PostToolUse of an `mcp__brenn__` tool
/// is admitted here, before any family runs. A refused call is answered here
/// with an error card and `{"error":"<kind>","detail":…}`; an admitted one
/// returns `None` and the families run.
async fn refuse_inadmissible_post(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<HandleBrennToolResult> {
    let ApprovalKind::PostToolUse {
        tool_name,
        tool_input,
        tool_use_id,
        ..
    } = &req.kind
    else {
        return None;
    };
    let short_name = tool_name.strip_prefix(MCP_BRENN_PREFIX)?;
    let err = execute::admit_tool_call(bridge, execute::CallOrigin::Hook, short_name, tool_input)
        .err()?;
    warn!(
        tool = %tool_name,
        app = %bridge.app_slug,
        kind = err.to_wire(short_name).kind(),
        detail = %err.detail(short_name),
        "brenn tool call refused at admission",
    );
    mark_tool_handled(bridge, tool_use_id).await;
    emit_tool_summary_for_intercept(bridge, tool_name, tool_input, true).await;
    Some(HandleBrennToolResult::Respond(
        CcApprovalDecision::Continue {
            updated_output: Some(err.to_json(short_name).to_string()),
        },
    ))
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

    fn post_req(tool_name: &str, input: serde_json::Value, tool_use_id: &str) -> ApprovalRequest {
        let (resp_tx, _) = oneshot::channel();
        ApprovalRequest {
            request_id: format!("req_{tool_use_id}"),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: tool_name.into(),
                tool_input: input,
                tool_use_id: tool_use_id.into(),
                tool_response: serde_json::json!("__NOOP__"),
            },
            response_tx: resp_tx,
        }
    }

    /// The parsed `updated_output` of a `Continue` answer.
    fn continue_output(result: Option<HandleBrennToolResult>) -> serde_json::Value {
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => serde_json::from_str(&output).unwrap(),
            other => panic!("expected Continue with output, got {other:?}"),
        }
    }

    /// A string `repos` would read as an empty list in the push handler and
    /// answer a success-shaped `{"repos":[]}`; admission refuses it first.
    #[tokio::test]
    async fn commit_and_push_with_malformed_repos_is_refused_before_the_handler() {
        let dir = tempfile::tempdir().unwrap();
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel(4);
        let sender = brenn_git::sync::SyncTriggerSender::new(
            trigger_tx,
            std::sync::Arc::new(std::collections::HashMap::from([(
                "test-repo".to_string(),
                "ssh://example/test-repo.git".to_string(),
            )])),
        );
        let bridge = execute::tests::full_catalog_bridge(dir.path(), Some(sender)).await;
        let req = post_req(
            "mcp__brenn__GitRepoCommitAndPush",
            serde_json::json!({"message": "m", "repos": "x"}),
            "tu_cp",
        );
        let out = continue_output(handle_brenn_tools(&bridge, &req).await);
        assert_eq!(out["error"], "invalid_args", "{out}");
        assert!(bridge.handled_tool_uses.lock().await.contains("tu_cp"));
        assert!(
            trigger_rx.try_recv().is_err(),
            "nothing may reach repo sync"
        );
    }

    #[tokio::test]
    async fn export_usage_with_unknown_key_is_refused_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let bridge = execute::tests::full_catalog_bridge(dir.path(), None).await;
        let out_path = dir.path().join("export.csv");
        let req = post_req(
            "mcp__brenn__ExportUsage",
            serde_json::json!({
                "kind": "sessions",
                "from": "2026-01-01T00:00:00Z",
                "to": "2027-01-01T00:00:00Z",
                "output_file": out_path.to_str().unwrap(),
                "bogus": 1,
            }),
            "tu_export",
        );
        let mut rx = bridge.subscribe();
        let out = continue_output(handle_brenn_tools(&bridge, &req).await);
        assert_eq!(out["error"], "invalid_args", "{out}");
        assert!(!out_path.exists(), "no export may be written");
        // The refusal shows in the chat as one error card.
        let mut cards = 0;
        while let Ok(msg) = rx.try_recv() {
            if let brenn_ws_types::WsServerMessage::ToolUseSummary {
                tool_name,
                rendered_summary,
                ..
            } = msg
            {
                assert!(tool_name.ends_with("ExportUsage"), "{tool_name}");
                assert!(
                    rendered_summary.contains("ts-denied"),
                    "a refused call's card must be an error: {rendered_summary}"
                );
                cards += 1;
            }
        }
        assert_eq!(cards, 1);
    }

    /// Unknown keys are refused under every schema, so a `{"bogus":1}` call to
    /// every catalog name proves admission covers the whole catalog.
    #[tokio::test]
    async fn admission_covers_every_catalog_tool() {
        let dir = tempfile::tempdir().unwrap();
        let bridge = execute::tests::full_catalog_bridge(dir.path(), None).await;
        let catalog =
            super::super::cc_spawn_config::virtual_tools(&bridge.app_config(), &bridge.tools);
        assert!(catalog.len() > 25, "fixture should offer the whole catalog");
        for def in &catalog {
            let tool_use_id = format!("tu_{}", def.name);
            let req = post_req(
                &format!("{MCP_BRENN_PREFIX}{}", def.name),
                serde_json::json!({"bogus": 1}),
                &tool_use_id,
            );
            let out = continue_output(handle_brenn_tools(&bridge, &req).await);
            assert_eq!(out["error"], "invalid_args", "{}: {out}", def.name);
            assert!(
                bridge.handled_tool_uses.lock().await.contains(&tool_use_id),
                "{} should be marked handled",
                def.name
            );
        }
    }

    #[tokio::test]
    async fn post_tool_use_for_a_non_brenn_tool_is_not_admitted() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        let req = post_req("Bash", serde_json::json!({"command": "ls"}), "tu_bash");
        assert!(handle_brenn_tools(&bridge, &req).await.is_none());
    }
}

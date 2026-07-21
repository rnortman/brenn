//! RequestCompaction tool: LLM-initiated compaction trigger.
//!
//! Pre/Post handler for `mcp__brenn__RequestCompaction`. The LLM calls this
//! tool to voluntarily compact its context. Pre validates the request (only
//! singleton apps support compaction; refuse if compaction is already in
//! progress), Post sets `CompactionPhase::PendingTurnCompletion` so the
//! existing turn-completion path actually runs `/compact`.

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_lib::approval_rules::ApprovalMatch;
use tracing::info;

use super::super::ActiveBridge;
use super::super::compaction::CompactionPhase;
use super::super::mcp_constants::MCP_REQUEST_COMPACTION_TOOL;
use super::super::tool_summary::{HandleBrennToolResult, emit_tool_summary, mark_tool_handled};

/// Handle both PreToolUse and PostToolUse arms for `MCP_REQUEST_COMPACTION_TOOL`.
///
/// Returns `Some(...)` when the request is for this tool family (Pre or Post)
/// and `None` otherwise — letting the dispatcher fall through to other arms.
pub(super) async fn handle(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<HandleBrennToolResult> {
    match &req.kind {
        // --- RequestCompaction PreToolUse ---
        // Validate: only singleton apps support compaction.
        // Check that no compaction is already in progress.
        ApprovalKind::PreToolUse { tool_name, .. } if tool_name == MCP_REQUEST_COMPACTION_TOOL => {
            if !bridge.singleton {
                info!("RequestCompaction denied — not a singleton app");
                return Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny {
                    reason: "Compaction is not available for this app. Only singleton \
                             apps (one conversation per user) support compaction."
                        .to_string(),
                }));
            }
            let mut state = bridge.compaction.lock().await;
            match state.phase {
                CompactionPhase::Normal => {}
                CompactionPhase::WaitingForIdle => {
                    // LLM is voluntarily compacting — even better than our timer.
                    // Cancel the timer and let the LLM-initiated flow take over.
                    state.cancel_idle_timer();
                    state.phase = CompactionPhase::Normal;
                    info!("RequestCompaction during WaitingForIdle — cancelled idle timer");
                }
                _ => {
                    info!("RequestCompaction denied — compaction already in progress");
                    return Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny {
                        reason: "Compaction is already in progress.".to_string(),
                    }));
                }
            }
            drop(state);
            info!("intercepting RequestCompaction PreToolUse — granting permission");
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // --- RequestCompaction PostToolUse ---
        // Set compaction phase to PendingTurnCompletion. When the current turn
        // completes, handle_turn_completed will send /compact.
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } if tool_name == MCP_REQUEST_COMPACTION_TOOL => {
            mark_tool_handled(bridge, tool_use_id).await;

            let hints = tool_input
                .get("hints")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from);

            {
                let mut state = bridge.compaction.lock().await;
                state.phase = CompactionPhase::PendingTurnCompletion { hints };
            }

            info!("RequestCompaction PostToolUse — compaction pending turn completion");

            emit_tool_summary(
                bridge,
                tool_name,
                tool_input,
                None,
                Some(&ApprovalMatch::GlobalTool),
                false,
            )
            .await;

            Some(HandleBrennToolResult::Respond(
                CcApprovalDecision::Continue {
                    updated_output: Some(
                        "Compaction will begin after this turn completes.".to_string(),
                    ),
                },
            ))
        }

        _ => None,
    }
}

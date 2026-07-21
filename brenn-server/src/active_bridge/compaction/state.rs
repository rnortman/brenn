//! Compaction data types: phase enum, state struct, context-usage, trigger kind.

use std::time::Instant;

/// Compaction lifecycle phase.
///
/// Five valid states — covers both LLM-initiated and Brenn-initiated flows.
#[derive(Debug, Default)]
pub(in crate::active_bridge) enum CompactionPhase {
    /// No compaction in progress or planned.
    #[default]
    Normal,
    /// LLM called RequestCompaction; waiting for the current turn to complete
    /// before sending `/compact`.
    PendingTurnCompletion {
        /// Optional hints from the LLM to influence the compaction summary.
        /// Appended to "/compact" when sent (e.g., "/compact Remember X, Y, Z").
        hints: Option<String>,
    },
    /// Brenn soft trigger: context above soft_pct, idle timer running.
    /// Waiting for idle_duration before starting compaction.
    /// User messages cancel the timer and reset to Normal.
    WaitingForIdle,
    /// Brenn-initiated: sent "persist your state" message, waiting for CC
    /// to complete that turn. Messages are sent directly to the NDJSON stream.
    PersistingState,
    /// `/compact` has been sent; waiting for the compact sequence to finish.
    Compacting,
}

/// Combined compaction state: phase + compaction-related flags under a single lock.
pub(in crate::active_bridge) struct CompactionState {
    pub(in crate::active_bridge) phase: CompactionPhase,
    /// Cancel handle for the soft-trigger idle timer. Only meaningful when
    /// phase == WaitingForIdle. Aborted by user message or session death.
    pub(in crate::active_bridge) idle_timer: Option<tokio::task::JoinHandle<()>>,
    /// Whether the LLM nudge has been sent this cycle. Set when reminder
    /// fires, cleared when context drops below reminder_pct or session dies.
    pub(in crate::active_bridge) reminder_sent: bool,
    /// Context usage % at the time the soft trigger started (for the persist
    /// message text). Stored here so the timer callback can use it.
    pub(in crate::active_bridge) trigger_usage_pct: u8,
    /// Set when `CompactBoundary` is received while phase is `Compacting`.
    /// Guards the `Compacting → Normal` transition so user-turn `TurnCompleted`
    /// events received before `/compact` finishes do not prematurely reset phase.
    /// Cleared on transition to `Normal` and on `Died`.
    pub(in crate::active_bridge) compact_boundary_seen: bool,
    /// Consecutive background (CC-autonomous) turns held while a mid-compaction
    /// phase (`PersistingState` / `PendingTurnCompletion`) waits on the foreground
    /// turn it needs. Incremented on each hold, reset to zero on exit from those
    /// phases. A sustained high count means the awaited foreground turn is not
    /// completing and compaction is stalled — surfaced loudly past a threshold.
    pub(in crate::active_bridge) background_holds: u32,
}

impl CompactionState {
    /// Abort the idle timer if one is running.
    pub(in crate::active_bridge) fn cancel_idle_timer(&mut self) {
        if let Some(timer) = self.idle_timer.take() {
            timer.abort();
        }
    }
}

impl Default for CompactionState {
    fn default() -> Self {
        Self {
            phase: CompactionPhase::Normal,
            idle_timer: None,
            reminder_sent: false,
            trigger_usage_pct: 0,
            compact_boundary_seen: false,
            background_holds: 0,
        }
    }
}

impl std::fmt::Debug for CompactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionState")
            .field("phase", &self.phase)
            .field("has_idle_timer", &self.idle_timer.is_some())
            .field("reminder_sent", &self.reminder_sent)
            .field("trigger_usage_pct", &self.trigger_usage_pct)
            .field("compact_boundary_seen", &self.compact_boundary_seen)
            .field("background_holds", &self.background_holds)
            .finish()
    }
}

/// Most recent context fill derived from the NDJSON stream.
/// `checked_at` records when the values were last updated for diagnostics.
#[derive(Debug, Clone)]
pub(in crate::active_bridge) struct ContextUsage {
    pub(in crate::active_bridge) current_tokens: u64,
    pub(in crate::active_bridge) max_tokens: u64,
    pub(in crate::active_bridge) usage_pct: u8,
    /// Timestamp of last update. Retained for future adaptive-frequency logic.
    #[allow(dead_code)]
    pub(in crate::active_bridge) checked_at: Instant,
}

/// Which gate (percentage or absolute tokens) caused a compaction stage to
/// fire. Used purely for log/metrics labelling. File-local.
#[derive(Debug, Clone, Copy)]
pub(super) enum TriggerKind {
    Pct,
    Tokens,
}

impl TriggerKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            TriggerKind::Pct => "pct",
            TriggerKind::Tokens => "tokens",
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::super::handle_brenn_tools;
    use super::super::super::mcp_constants::MCP_REQUEST_COMPACTION_TOOL;
    use super::super::super::test_support::{
        install_recording_session, set_waiting_for_idle, test_bridge, test_bridge_singleton,
        user_text,
    };
    use super::super::super::tool_summary::HandleBrennToolResult;
    use super::*;
    use brenn_cc::session::{
        ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest,
    };
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn compaction_pre_tool_use_denied_non_singleton() {
        // Non-singleton bridge should deny RequestCompaction.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_compact_1".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "hook_0".into(),
                tool_name: MCP_REQUEST_COMPACTION_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_use_id: "t_compact_1".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny { reason })) => {
                assert!(
                    reason.contains("not available"),
                    "denial reason should mention not available: {reason}"
                );
            }
            other => panic!("expected Deny for non-singleton, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn compaction_pre_tool_use_allowed_singleton() {
        // Singleton bridge should allow RequestCompaction.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_compact_2".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "hook_0".into(),
                tool_name: MCP_REQUEST_COMPACTION_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_use_id: "t_compact_2".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow { .. })) => {}
            other => panic!("expected Allow for singleton, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn compaction_pre_tool_use_denied_already_in_progress() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        bridge.compaction.lock().await.phase =
            CompactionPhase::PendingTurnCompletion { hints: None };

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_compact_3".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "hook_0".into(),
                tool_name: MCP_REQUEST_COMPACTION_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_use_id: "t_compact_3".into(),
            },
            response_tx: resp_tx,
        };

        match handle_brenn_tools(&bridge, &req).await {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny { reason })) => {
                assert!(reason.contains("already in progress"));
            }
            other => panic!("expected Deny for duplicate compaction, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn compaction_post_tool_use_sets_pending() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_compact_post".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "hook_1".into(),
                tool_name: MCP_REQUEST_COMPACTION_TOOL.into(),
                tool_input: serde_json::json!({"hints": "Remember the grocery list"}),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_compact_post".into(),
            },
            response_tx: resp_tx,
        };

        match handle_brenn_tools(&bridge, &req).await {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output,
            })) => {
                assert!(
                    updated_output
                        .as_deref()
                        .unwrap_or("")
                        .contains("Compaction will begin"),
                    "output should confirm compaction: {updated_output:?}"
                );
            }
            other => panic!("expected Continue for PostToolUse, got {other:?}"),
        }

        let state = bridge.compaction.lock().await;
        match &state.phase {
            CompactionPhase::PendingTurnCompletion { hints } => {
                assert_eq!(hints.as_deref(), Some("Remember the grocery list"));
            }
            other => panic!("expected PendingTurnCompletion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn compaction_post_tool_use_no_hints() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_compact_no_hints".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "hook_1".into(),
                tool_name: MCP_REQUEST_COMPACTION_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_compact_no_hints".into(),
            },
            response_tx: resp_tx,
        };

        let _result = handle_brenn_tools(&bridge, &req).await;

        let state = bridge.compaction.lock().await;
        match &state.phase {
            CompactionPhase::PendingTurnCompletion { hints } => {
                assert!(hints.is_none(), "hints should be None when not provided");
            }
            other => panic!("expected PendingTurnCompletion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_message_succeeds_during_compacting() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        let mut rx = install_recording_session(&bridge).await;
        bridge.compaction.lock().await.phase = CompactionPhase::Compacting;
        bridge
            .cc_idle
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let result = bridge.send_message("hello during compaction").await;
        assert!(
            result.is_ok(),
            "send_message must succeed during Compacting"
        );
        assert!(
            !bridge.cc_idle.load(std::sync::atomic::Ordering::SeqCst),
            "cc_idle must flip to false after successful send during Compacting"
        );

        // Message must reach the session — not queued or dropped.
        let msg = rx
            .try_recv()
            .expect("session must have received the message");
        assert_eq!(user_text(&msg), "hello during compaction");
    }

    #[tokio::test]
    async fn send_message_succeeds_during_pending_turn_completion() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        let mut rx = install_recording_session(&bridge).await;
        bridge.compaction.lock().await.phase =
            CompactionPhase::PendingTurnCompletion { hints: None };
        bridge
            .cc_idle
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let result = bridge.send_message("hello during pending").await;
        assert!(
            result.is_ok(),
            "send_message must succeed during PendingTurnCompletion"
        );
        assert!(
            !bridge.cc_idle.load(std::sync::atomic::Ordering::SeqCst),
            "cc_idle must flip to false after successful send during PendingTurnCompletion"
        );

        let msg = rx
            .try_recv()
            .expect("session must have received the message");
        assert_eq!(user_text(&msg), "hello during pending");
    }

    #[tokio::test]
    async fn send_message_succeeds_during_persisting_state() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        let mut rx = install_recording_session(&bridge).await;
        bridge.compaction.lock().await.phase = CompactionPhase::PersistingState;
        bridge
            .cc_idle
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let result = bridge.send_message("hello during persisting").await;
        assert!(
            result.is_ok(),
            "send_message must succeed during PersistingState"
        );
        assert!(
            !bridge.cc_idle.load(std::sync::atomic::Ordering::SeqCst),
            "cc_idle must flip to false after successful send during PersistingState"
        );

        let msg = rx
            .try_recv()
            .expect("session must have received the message");
        assert_eq!(user_text(&msg), "hello during persisting");
    }

    #[tokio::test]
    async fn send_outgoing_preserves_non_text_block_during_compacting() {
        // Non-text content blocks (ToolResult) previously lost during compaction
        // must now be sent intact through the normal send path (AC 4).
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        let mut rx = install_recording_session(&bridge).await;
        bridge.compaction.lock().await.phase = CompactionPhase::Compacting;

        let tool_result_block = brenn_cc::protocol::outgoing::UserContentBlock::ToolResult {
            tool_use_id: "tu_preserve_test".into(),
            content: serde_json::json!([{"type": "text", "text": "tool output"}]),
        };
        let outgoing = brenn_cc::protocol::CcOutgoing::User {
            message: brenn_cc::protocol::outgoing::UserContent {
                role: "user".into(),
                content: vec![tool_result_block],
            },
        };

        let result = bridge.send_outgoing(outgoing).await;
        assert!(
            result.is_ok(),
            "send_outgoing with ToolResult block must succeed during Compacting"
        );

        let envelope = rx
            .try_recv()
            .expect("session must have received the message");
        match envelope.msg {
            brenn_cc::protocol::CcOutgoing::User { message } => {
                assert_eq!(
                    message.content.len(),
                    1,
                    "message must preserve the ToolResult block"
                );
                assert!(
                    matches!(
                        &message.content[0],
                        brenn_cc::protocol::outgoing::UserContentBlock::ToolResult { .. }
                    ),
                    "content block must be ToolResult, not dropped or mangled"
                );
            }
            other => panic!("expected CcOutgoing::User, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn compaction_pre_tool_use_denied_during_compacting_phase() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        bridge.compaction.lock().await.phase = CompactionPhase::Compacting;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_compact_dup".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "hook_0".into(),
                tool_name: MCP_REQUEST_COMPACTION_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_use_id: "t_compact_dup".into(),
            },
            response_tx: resp_tx,
        };

        match handle_brenn_tools(&bridge, &req).await {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny { reason })) => {
                assert!(reason.contains("already in progress"));
            }
            other => panic!("expected Deny during Compacting phase, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn compaction_pre_tool_use_allowed_during_waiting_for_idle() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        set_waiting_for_idle(&bridge).await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_compact_wfi".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "hook_0".into(),
                tool_name: MCP_REQUEST_COMPACTION_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_use_id: "t_compact_wfi".into(),
            },
            response_tx: resp_tx,
        };
        let result = handle_brenn_tools(&bridge, &req).await;

        // Should allow (LLM voluntarily compacting supersedes the idle timer).
        assert!(
            matches!(
                result,
                Some(HandleBrennToolResult::Respond(
                    CcApprovalDecision::Allow { .. }
                ))
            ),
            "should Allow RequestCompaction during WaitingForIdle, got {result:?}"
        );

        // Timer should be cancelled, phase reset to Normal (PostToolUse will set
        // PendingTurnCompletion).
        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "phase should be Normal after allowing RequestCompaction during WaitingForIdle"
        );
        assert!(state.idle_timer.is_none(), "idle_timer should be cleared");
    }

    #[tokio::test]
    async fn compaction_pre_tool_use_denied_during_persisting_state() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        bridge.compaction.lock().await.phase = CompactionPhase::PersistingState;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_compact_ps".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "hook_0".into(),
                tool_name: MCP_REQUEST_COMPACTION_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_use_id: "t_compact_ps".into(),
            },
            response_tx: resp_tx,
        };
        let result = handle_brenn_tools(&bridge, &req).await;

        assert!(
            matches!(
                result,
                Some(HandleBrennToolResult::Respond(
                    CcApprovalDecision::Deny { .. }
                ))
            ),
            "should Deny during PersistingState, got {result:?}"
        );
    }
}

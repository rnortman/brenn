//! CC subprocess event-loop dispatcher and per-event handlers (Stream, Assistant, Initialized, StatusChange, RateLimit, CompactBoundary, etc.).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use brenn_cc::session::SessionEvent;
use brenn_lib::conversation::{self, MessageDirection};
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::ws_types::WsServerMessage;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use super::approval_dispatch::handle_approval_required;
use super::bridge_io::persist_incoming_message;
use super::compaction::handle_turn_completed;
use super::{ActiveBridge, CompactionPhase, emit_tool_result_summaries};

mod death;
mod drain;
mod initialized;
mod rate_limit;
mod status;
mod streaming;

pub(in crate::active_bridge) use death::reset_dead_session;
use death::reset_session_runtime_state;
pub(super) use drain::drain_pending_events;
pub(in crate::active_bridge) use initialized::handle_initialized;
pub(in crate::active_bridge) use rate_limit::handle_rate_limit_utilization;
use status::{handle_compact_boundary, handle_status_change};
pub(in crate::active_bridge) use streaming::handle_assistant_message;
use streaming::handle_stream_event;

/// Reason CC's process exited. Computed once at the `SessionEvent::Died`
/// boundary from the two bridge-level flags that indicate intentional teardown.
pub(in crate::active_bridge) enum ShutdownReason {
    /// Intentional teardown — suppress alert and leave conversation Active.
    Intentional { drain: bool, server: bool },
    /// Unexpected death — fire alert, mark conversation Error.
    Unexpected,
}

impl ShutdownReason {
    pub(in crate::active_bridge) fn from_bridge(bridge: &ActiveBridge) -> Self {
        let drain = bridge.drain_on_idle.load(Ordering::SeqCst);
        let server = bridge.server_shutting_down.load(Ordering::SeqCst);
        if drain || server {
            ShutdownReason::Intentional { drain, server }
        } else {
            ShutdownReason::Unexpected
        }
    }

    /// Whether this death was an intentional teardown (drain or server
    /// shutdown). The watchdog uses this to suppress wedge handling.
    pub(in crate::active_bridge) fn is_intentional(&self) -> bool {
        matches!(self, ShutdownReason::Intentional { .. })
    }

    /// Whether a process-wide server shutdown is in progress. Unlike a
    /// per-conversation idle-drain, a server shutdown suppresses even the
    /// dead-event-loop wedge predicate — the whole process is going down.
    pub(in crate::active_bridge) fn is_server_shutdown(&self) -> bool {
        matches!(self, ShutdownReason::Intentional { server: true, .. })
    }
}

pub(super) async fn cc_event_loop(
    mut event_rx: mpsc::Receiver<SessionEvent>,
    bridge: Arc<ActiveBridge>,
    alert_dispatcher: AlertDispatcher,
) {
    // Drain any queued events that accumulated while CC was sleeping. The CC
    // subprocess is ready (control_response received) and the bridge is fully
    // constructed. Run before the loop starts processing CC events so the
    // queued batch is delivered as the first thing CC sees on this run.
    //
    // A user message arriving from a freshly-attached WS handler can race the
    // drain on the outgoing stdin channel; either order is acceptable to CC,
    // but the drain at least precedes any work the event loop itself drives.
    //
    // If there are events to drain, send_system_message calls
    // set_cc_busy → cc_idle = false. CC processes the events, completes a
    // turn, and set_idle_and_drain restores cc_idle = true.
    //
    // Ungated: the drain runs for every bridge, not just singletons.
    // See docs/designs/init-not-required.md and docs/designs/repo-sync.md
    // (M7 — "Ungate to all apps").
    drain_pending_events(&bridge).await;
    // Signal that the startup drain is complete. Tests use this to know that
    // any queued events were processed before asserting on broadcasts or DB state.
    #[cfg(test)]
    bridge.event_loop_epoch.send_modify(|e| *e += 1);

    while let Some(event) = event_rx.recv().await {
        match event {
            SessionEvent::Initialized(info) => {
                handle_initialized(&bridge, &info, &alert_dispatcher).await;
            }
            SessionEvent::StreamEvent(evt) => {
                handle_stream_event(&bridge, &evt);
            }
            SessionEvent::AssistantMessage(msg) => {
                handle_assistant_message(&bridge, &msg, &alert_dispatcher).await;
            }
            SessionEvent::ToolResult(msg) => {
                // Persist the raw ToolResult row for the diagnostic log. The returned
                // seq is not forwarded via broadcast — ToolResult itself is not broadcast.
                // The downstream emit_tool_result_summaries captures its own seq via
                // emit_tool_summary → append_message and stamps the ToolUseSummary broadcast.
                let _tool_result_seq =
                    persist_incoming_message(&bridge, "user", msg.uuid.as_deref(), None, &msg)
                        .await;
                // Emit tool-use summaries from ToolResult — this always fires,
                // unlike PostToolUse which CC skips on tool errors.
                emit_tool_result_summaries(&bridge, &msg, &alert_dispatcher).await;
            }
            SessionEvent::ApprovalRequired(req) => {
                handle_approval_required(&bridge, req).await;
            }
            SessionEvent::ApprovalCancelled { request_id } => {
                // Remove from synchronous permissions if present.
                {
                    let mut permissions = bridge.pending_permissions.lock().await;
                    permissions.remove(&request_id);
                }
                // Note: async tool requests in DB are not cancelled by CC — they
                // persist until the user responds. Only permissions are cancelled.
                bridge.broadcast(WsServerMessage::PermissionCancelled { request_id });
            }
            SessionEvent::StatusChange {
                status,
                compact_result,
            } => {
                handle_status_change(
                    &bridge,
                    status.as_deref(),
                    compact_result.as_deref(),
                    &alert_dispatcher,
                )
                .await;
            }
            SessionEvent::CompactBoundary { metadata } => {
                handle_compact_boundary(&bridge, metadata.as_ref()).await;
                let mut state = bridge.compaction.lock().await;
                if matches!(state.phase, CompactionPhase::Compacting) {
                    state.compact_boundary_seen = true;
                } else {
                    warn!(
                        conversation_id = bridge.conversation_id,
                        phase = ?state.phase,
                        "CompactBoundary received outside Compacting phase — anomalous, \
                         ignoring for flag purposes (possible CC protocol change)"
                    );
                }
            }
            SessionEvent::RateLimit(evt) => {
                let is_limited = evt
                    .rate_limit_info
                    .as_ref()
                    .and_then(|info| info.get("status"))
                    .and_then(|s| s.as_str())
                    .is_some_and(|s| s != "allowed");

                if is_limited {
                    warn!("CC rate limited");
                    bridge.broadcast(WsServerMessage::Error {
                        message: "Rate limited by Claude API".to_string(),
                    });
                } else {
                    info!("CC rate limit status: allowed");
                }
                // Log utilization from allowed_warning events; no frontend consumer.
                handle_rate_limit_utilization(&evt, &alert_dispatcher);
                // Persist to the diagnostic log. RateLimit events are not broadcast
                // to the frontend so the returned seq is not forwarded.
                let _rate_limit_seq = persist_incoming_message(
                    &bridge,
                    "rate_limit_event",
                    evt.uuid.as_deref(),
                    None,
                    &evt,
                )
                .await;
            }
            SessionEvent::TurnCompleted(result) => {
                handle_turn_completed(&bridge, &result, &alert_dispatcher).await;
            }
            SessionEvent::Died(err) => {
                if bridge.died_handled() {
                    // The watchdog (or a prior death) already ran the clean-slate
                    // reset and paged for this bridge. Do not re-alert, re-mark
                    // Error, or re-broadcast — that would double-page the same
                    // incident and confuse the timeline.
                    info!(
                        conversation_id = bridge.conversation_id,
                        "CC session died, but death already handled — skipping duplicate reset"
                    );
                    #[cfg(test)]
                    bridge.event_loop_epoch.send_modify(|e| *e += 1);
                    continue;
                }
                let shutdown_reason = ShutdownReason::from_bridge(&bridge);
                if let ShutdownReason::Intentional { drain, server } = shutdown_reason {
                    // Intentional shutdown — not an error. `drain` is a
                    // per-conversation drain (tab close, idle); `server`
                    // is a process-wide SIGTERM. Both skip the Warning alert
                    // and leave the conversation in Active state so the next
                    // restart can resume it. See docs/designs/silence-known-cc-warnings.md.
                    // Still clear runtime state so a reconnecting CC sees a
                    // clean slate, and mark the death handled so the watchdog
                    // does not treat the ended loop as a wedge.
                    reset_session_runtime_state(&bridge).await;
                    bridge
                        .died_handled
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    info!(
                        conversation_id = bridge.conversation_id,
                        drain_shutdown = drain,
                        server_shutdown = server,
                        "CC session ended (intentional shutdown)"
                    );
                } else {
                    error!("CC session died: {err}");
                    alert_dispatcher.alert(
                        brenn_lib::obs::alerting::AlertSeverity::Warning,
                        "CC session died".to_string(),
                        err.to_string(),
                    );
                    // Clean-slate reset: runtime state + mark conversation
                    // Error + error broadcasts + died_handled.
                    reset_dead_session(&bridge, format!("CC session died: {err}")).await;
                }
            }
            SessionEvent::UnrecognizedMessage { raw_line } => {
                // The alert for this was already dispatched (and per-process
                // deduped) by the reader task in `brenn-cc/src/session/tasks.rs`
                // when the parse failed. Our job here is only to log + persist
                // so the raw message survives in the conversation history.
                warn!("unrecognized CC message (persisting; alert already fired)");
                let conn = bridge.db.lock().await;
                let (_id, _seq) = conversation::append_message(
                    &conn,
                    bridge.conversation_id,
                    MessageDirection::Incoming,
                    "unrecognized",
                    None,
                    None,
                    &raw_line,
                    None,
                    None,
                    None,
                );
            }
        }
        // Signal that all in-handler observable state (broadcasts, DB writes,
        // mutex updates, oneshot replies) is committed for this event. Tests
        // subscribe to the epoch before sending an event and await changed()
        // after the action to get a deterministic fence.
        #[cfg(test)]
        bridge.event_loop_epoch.send_modify(|e| *e += 1);
    }

    // CC event channel closed — CC process exited. Complete the conversation
    // and clean up the bridge.
    info!(
        "CC event loop ended for conversation {}",
        bridge.conversation_id
    );
    // Complete the conversation (if still Active) and remove from registry.
    // Only complete if still Active — it may already be Error from a Died event.
    bridge.complete_and_kill().await;

    // Subprocess is gone: any pending synchronous permissions are now dead
    // (the oneshot sender drops when the bridge drops). Broadcast
    // `PermissionCancelled` for each so attached tabs dismiss their dialogs
    // rather than submitting a response that has no live oneshot to land on.
    bridge.drain_and_cancel_pending_permissions().await;
    // Signal teardown complete. Tests that drop event_tx and then assert on
    // registry removal or conversation status use this fence.
    #[cfg(test)]
    bridge.event_loop_epoch.send_modify(|e| *e += 1);
}

#[cfg(test)]
mod tests;

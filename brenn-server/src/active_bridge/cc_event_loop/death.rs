use std::sync::atomic::Ordering;

use brenn_lib::conversation::{self, ConversationStatus};
use brenn_lib::ws_types::{CcState, WsServerMessage};

use super::super::{ActiveBridge, CompactionPhase};

pub(super) async fn mark_conversation_error(bridge: &ActiveBridge) {
    let conn = bridge.db.lock().await;
    // Skip if already Completed (e.g., drain kill completed the conversation
    // before SIGKILL triggered this Died event).
    let conv = conversation::get_conversation_opt(&conn, bridge.conversation_id);
    if let Some(conv) = conv
        && conv.status != ConversationStatus::Completed
    {
        conversation::error_conversation(&conn, bridge.conversation_id);
    }
}

/// Reset the bridge's per-session runtime state to a clean slate: compaction
/// phase + idle timer, context fill, and active model. Any in-progress
/// compaction is moot once the session is gone, and a reconnecting CC must see
/// no stale fill or model slug.
///
/// Shared by every death/wedge path (intentional shutdown, unexpected death,
/// watchdog-detected wedge).
pub(in crate::active_bridge) async fn reset_session_runtime_state(bridge: &ActiveBridge) {
    {
        let mut state = bridge.compaction.lock().await;
        state.phase = CompactionPhase::Normal;
        state.cancel_idle_timer();
        state.reminder_sent = false;
        state.compact_boundary_seen = false;
        state.background_holds = 0;
    }
    // Null out context fill so a reconnecting CC sees a clean slate.
    //
    // Recover rather than propagate a poisoned lock: this reset is the recovery
    // routine the watchdog runs *after* an event-loop panic, and that panic can
    // be the very thing that poisoned this std mutex. The value is being nulled
    // unconditionally, so any poisoned prior value is irrelevant — panicking
    // here would kill the unsupervised watchdog and disable all self-healing.
    *bridge
        .context_usage
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;
    // Null active_model_slug so the next handle_initialized starts clean.
    // seed_max_tokens and cc_version are intentionally not reset — they are
    // overwritten unconditionally on the next CC spawn. Poison-tolerant for the
    // same reason as context_usage above.
    *bridge
        .active_model_slug
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = None;
}

/// Full clean-slate reset for a bridge that died or wedged unexpectedly: resets
/// runtime state, marks the conversation `Error`, broadcasts the error and error
/// status to attached tabs, and records that the death has been handled.
///
/// Used by both the event loop's unexpected-`Died` path and the wedge watchdog.
/// The watchdog fires its own `Critical` page before calling this; the event
/// loop fires its `Warning` alert before calling this. Alerting is the caller's
/// concern so each path pages at its own severity.
pub(in crate::active_bridge) async fn reset_dead_session(
    bridge: &ActiveBridge,
    error_message: String,
) {
    reset_session_runtime_state(bridge).await;
    mark_conversation_error(bridge).await;
    bridge.broadcast(WsServerMessage::Error {
        message: error_message,
    });
    bridge.broadcast(WsServerMessage::Status {
        state: CcState::Error,
    });
    bridge.died_handled.store(true, Ordering::SeqCst);
}

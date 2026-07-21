use std::sync::Arc;
use std::sync::atomic::Ordering;

use brenn_lib::ws_types::{CcState, WsServerMessage};
use tracing::debug;

use crate::active_bridge::ActiveBridge;

/// Mark CC as idle, broadcast the idle status to the browser, and run the
/// post-`/context` suppression path. Must be called when CC has finished
/// all work and is genuinely idle.
///
/// When `drain_on_idle` is set, runs `run_idle_hooks_for_shutdown` first
/// (giving registered hooks a last chance to nudge before CC is killed)
/// and then calls `drain_no_hooks` to perform the actual kill. If a hook
/// delivered a message, `set_cc_busy` flipped `cc_idle = false` again
/// and `drain_no_hooks` defers; the next turn end will reach this path
/// again with the hook one-shots set, and `run_idle_hooks_for_shutdown`
/// will return without sending anything new, after which the kill
/// proceeds. Otherwise, arms the per-bridge idle-hook timer fresh from
/// this moment.
pub(in crate::active_bridge) async fn set_idle_and_drain(bridge: &Arc<ActiveBridge>) {
    bridge.cc_idle.store(true, Ordering::SeqCst);
    debug!(conversation_id = bridge.conversation_id, "cc_idle → true");
    bridge.broadcast(WsServerMessage::Status {
        state: CcState::Idle,
    });
    if bridge.drain_on_idle.load(Ordering::SeqCst) {
        // Last chance: let hooks fire before we kill CC. Bounded by the
        // per-hook timeouts (`GIT_TIMEOUT` etc.) inside `check()` plus an
        // outer 60 s ceiling. Returns *after* the message has been sent
        // (or skipped). If a message was sent, `cc_idle` is now false
        // again; `drain_no_hooks` will defer to the next turn end.
        //
        // We use `drain_no_hooks` (not `maybe_drain`) so the hook
        // fan-out doesn't run *twice* on the common clean-shutdown path:
        // hooks return `None`, `cc_idle` stays `true`, and a
        // `maybe_drain` call would invoke `run_idle_hooks_for_shutdown`
        // a second time before killing.
        crate::idle_hooks::run_idle_hooks_for_shutdown(bridge).await;
        bridge.drain_no_hooks().await;
    } else {
        // Normal path: arm the shared timer fresh from this moment.
        bridge.maybe_arm_idle_hook_timer().await;
    }
}

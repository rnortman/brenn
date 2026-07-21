//! Compaction state machine: phase enum, state struct, context-usage tracking, trigger evaluation, idle timer, turn-completion handler, and the LLM-initiated compaction tool gate.

mod context;
mod idle;
mod origin;
mod phase_query;
mod state;
mod triggers;

use self::context::{broadcast_context_usage, update_max_tokens_from_result_sync};
use self::origin::{MAX_ORIGIN_KIND_BYTES, TurnOrigin, classify_turn_origin};
use self::triggers::evaluate_compaction_triggers;
pub(in crate::active_bridge) use context::update_context_from_assistant;
pub(in crate::active_bridge) use idle::set_idle_and_drain;
pub(in crate::active_bridge) use state::{CompactionPhase, CompactionState, ContextUsage};

use std::sync::Arc;
use std::sync::atomic::Ordering;

use brenn_cc::protocol::incoming::ResultMessage;
use brenn_common::sanitize_untrusted_str;
use brenn_lib::conversation;
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use brenn_lib::ws_types::WsServerMessage;
use tracing::{debug, error, info, warn};

/// Consecutive background-turn holds in a single mid-compaction phase entry
/// before the hold is escalated from a `debug!` trace to a `warn!` + paged
/// `Warning` alert. A generous backstop: the awaited foreground turn is
/// evidently not completing and compaction is stalled. A live session with a
/// live event loop is outside the wedge watchdog's remit, so this in-band signal
/// is the only page for this stall shape.
const BACKGROUND_HOLD_ALERT_THRESHOLD: u32 = 30;

/// Record a background-turn hold in a mid-compaction phase: bump the counter,
/// log at `debug!`, and — exactly once, as the count crosses the threshold —
/// escalate to a `warn!` + deduped-by-crossing `Warning` alert so an indefinite
/// stall is loud rather than silent.
fn note_background_hold(
    state: &mut CompactionState,
    bridge: &ActiveBridge,
    phase: &str,
    result: &ResultMessage,
    alert_dispatcher: &AlertDispatcher,
) {
    state.background_holds = state.background_holds.saturating_add(1);
    let kind = result
        .origin
        .as_ref()
        .map(|o| sanitize_untrusted_str(&o.kind, MAX_ORIGIN_KIND_BYTES));
    debug!(
        conversation_id = bridge.conversation_id,
        phase,
        origin_kind = kind.as_deref(),
        holds = state.background_holds,
        "holding mid-compaction phase for background turn"
    );
    if state.background_holds == BACKGROUND_HOLD_ALERT_THRESHOLD {
        warn!(
            conversation_id = bridge.conversation_id,
            phase,
            holds = state.background_holds,
            "compaction stalled — many consecutive background turns while awaiting foreground completion"
        );
        alert_dispatcher.alert(
            AlertSeverity::Warning,
            "Compaction stalled".to_string(),
            format!(
                "conversation {} held in {phase} across {} consecutive background turns without \
                 the awaited foreground turn completing — compaction is blocked and context is \
                 not shrinking.",
                bridge.conversation_id, state.background_holds
            ),
        );
    }
}

use super::ActiveBridge;
use super::bridge_io::persist_incoming_message_sync;

pub(in crate::active_bridge) async fn handle_turn_completed(
    bridge: &Arc<ActiveBridge>,
    result: &brenn_cc::protocol::incoming::ResultMessage,
    alert_dispatcher: &AlertDispatcher,
) {
    info!(
        conversation_id = bridge.conversation_id,
        "CC turn completed"
    );

    // --- Per-turn cost arithmetic (no DB, no await) ---
    // Computed before the unified DB scope so the results are ready for the
    // cost-bookkeeping region inside that scope.
    //
    // Only process cost when CC actually reported a value. Treating None as 0
    // would poison `last_total_cost_usd` — on the next real-cost turn the
    // delta would be `cur - 0 = cur`, double-counting the entire session
    // cumulative (correctness-1 / correctness-2).
    let last_turn;
    let cur_total_opt = result.total_cost_usd;
    {
        let mut guard = bridge
            .last_total_cost_usd
            .lock()
            .expect("last_total_cost_usd lock");
        let prev_total = guard.unwrap_or(0.0);
        last_turn = match cur_total_opt {
            None => {
                // CC didn't report cost for this turn (e.g. compaction-result
                // frames). Leave last_total_cost_usd unchanged so the next real
                // cost turn computes the correct delta.
                0.0
            }
            Some(cur) => {
                // If CC reset (compaction), cur < prev — treat the whole new
                // value as the per-turn cost (cost of the compact operation).
                let delta = if cur < prev_total {
                    cur
                } else {
                    cur - prev_total
                };
                *guard = Some(cur);
                delta
            }
        };
    }
    let cur_total = cur_total_opt.unwrap_or(0.0);

    // Hoist wall-clock read before the lock; a clock_gettime syscall under the
    // DB mutex contradicts the refactor's goal of minimising hold time.
    let now = chrono::Utc::now();
    let cutoff = now - chrono::Duration::hours(24);
    let now_secs = now.timestamp();

    // --- Unified DB scope: all three turn-end DB regions in one lock hold ---
    //
    // NO `.await` inside this scope. The three regions are sync precisely so
    // this scope holds the tokio mutex without yielding. Adding any `.await`
    // here is a refactor regression — see
    // docs/adr/2026/05/17-db-lock-coalesce-turn-end/design.md.
    //
    // Locking order: `bridge.db` (tokio mutex, held here) → `active_model_slug`,
    // `context_usage`, `cc_version` std mutexes (acquired inside
    // `update_max_tokens_from_result_sync`). No code path may acquire
    // `bridge.db.lock()` while holding any of these std mutexes — doing so
    // deadlocks. See design doc above.
    let model_window_updated;
    let last_24h;
    {
        let conn = bridge.db.lock().await;

        // Region 1: persist the CC result row.
        persist_incoming_message_sync(&conn, bridge.conversation_id, "result", None, None, result);

        // Region 2: model window cache upsert (returns whether to broadcast).
        model_window_updated =
            update_max_tokens_from_result_sync(bridge, result, alert_dispatcher, &conn);

        // Region 3: cost bookkeeping.
        // Only persist cost when CC reported a real value — writing NULL to
        // conversations.total_cost_usd destroys the cross-restart baseline.
        if cur_total_opt.is_some() {
            conversation::set_cost(&conn, bridge.conversation_id, cur_total_opt);
        }
        // Skip zero-valued rows: a last_turn == 0.0 insert is a no-op against
        // the 24h aggregate but bloats the table. Compaction-result frames and
        // turns where CC reported total_cost_usd = None produce zero; skipping
        // is correct in both cases.
        if last_turn > 0.0 {
            brenn_lib::cost_samples::insert(&conn, bridge.conversation_id, last_turn);

            // Record the llm_turn usage event only for real-cost turns. Zero-cost
            // frames (compaction-result, CC turns where total_cost_usd=None) must
            // not inflate llm_turns counters or produce phantom event rows.
            if let Some((device_id, user_id)) =
                brenn_lib::usage::resolve_sender_for_conversation(&conn, bridge.conversation_id)
            {
                brenn_lib::usage::record_llm_turn(
                    &conn,
                    device_id,
                    user_id,
                    &bridge.app_slug,
                    Some(bridge.conversation_id),
                    last_turn,
                    bridge.usage_session_gap_secs,
                );
            }
        }

        // Prune at most once per hour; the 24h window means the DELETE is rarely needed.
        // `now`/`cutoff`/`now_secs` are hoisted before the lock to minimise hold time.
        let last = bridge.last_cost_prune_at.load(Ordering::Relaxed);
        if now_secs - last >= 3600 {
            brenn_lib::cost_samples::prune_before(&conn, cutoff);
            bridge.last_cost_prune_at.store(now_secs, Ordering::Relaxed);
        }
        last_24h = brenn_lib::cost_samples::sum_since(&conn, cutoff);
    }
    // DB lock released; no further DB access on this path.

    // --- Stream-derived context broadcast (after scope: depends on model_window_updated) ---
    // Broadcast ContextUsage only when the authoritative window size is
    // available (A10: skip if absent). The post-scope re-read of context_usage
    // can race with cc_event_loop setters that null the slot — that race is
    // preexisting and benign (no broadcast is better than a stale broadcast).
    // Do not hoist this read into the scope above.
    if model_window_updated {
        let cur = bridge
            .context_usage
            .lock()
            .expect("context_usage lock")
            .clone();
        if let Some(cur) = cur {
            broadcast_context_usage(bridge, &cur);
        }
    }

    bridge.broadcast(WsServerMessage::CostUsage {
        last_turn_usd: last_turn,
        since_last_compaction_usd: cur_total,
        last_24h_usd: last_24h,
    });

    // Clear synchronous permission approvals — any in-flight permission requests
    // are invalid now that the turn is complete. Async tool requests (in the DB)
    // are NOT cleared — they persist across turns. A non-empty map here is a
    // CC-protocol anomaly (CC should not complete a turn with an open
    // can_use_tool). Broadcast so stranded dialogs dismiss.
    let cleared_ids = bridge.drain_and_cancel_pending_permissions().await;
    if !cleared_ids.is_empty() {
        warn!(
            count = cleared_ids.len(),
            "pending permissions cleared at turn completion — should be empty"
        );
    }

    // --- Compaction phase handling ---
    // Determine action under lock, then execute after dropping it.
    enum CompactionAction {
        SendCompact {
            hints: Option<String>,
        },
        Nothing,
        /// A `TurnCompleted` arrived while phase is `Compacting` but
        /// `compact_boundary_seen` is not set — this is a user-turn completing
        /// before `/compact` finishes. Stay in `Compacting` and skip idle
        /// broadcast.
        StayCompacting,
        /// A background (CC-autonomous) turn completed while a mid-compaction
        /// arm was waiting on a foreground turn. Hold the current phase and skip
        /// the idle broadcast — CC is still working on the awaited turn.
        HoldForBackgroundTurn,
    }
    let compaction_action = {
        let mut state = bridge.compaction.lock().await;
        match &state.phase {
            CompactionPhase::PendingTurnCompletion { .. } => {
                // Only the foreground turn that called RequestCompaction should
                // fire /compact and consume the hints. A background turn arriving
                // first would compact early and lose the hints — hold instead.
                match classify_turn_origin(result, alert_dispatcher) {
                    TurnOrigin::Background => {
                        note_background_hold(
                            &mut state,
                            bridge,
                            "PendingTurnCompletion",
                            result,
                            alert_dispatcher,
                        );
                        CompactionAction::HoldForBackgroundTurn
                    }
                    TurnOrigin::Foreground => {
                        state.background_holds = 0;
                        let hints = match std::mem::take(&mut state.phase) {
                            CompactionPhase::PendingTurnCompletion { hints } => hints,
                            _ => unreachable!(),
                        };
                        state.phase = CompactionPhase::Compacting;
                        CompactionAction::SendCompact { hints }
                    }
                }
            }
            CompactionPhase::PersistingState => {
                // Only the foreground persist turn's completion confirms CC is
                // ready to /compact. A background turn can arrive first (the idle
                // timer fires regardless of background activity) — hold for it.
                match classify_turn_origin(result, alert_dispatcher) {
                    TurnOrigin::Background => {
                        note_background_hold(
                            &mut state,
                            bridge,
                            "PersistingState",
                            result,
                            alert_dispatcher,
                        );
                        CompactionAction::HoldForBackgroundTurn
                    }
                    TurnOrigin::Foreground => {
                        state.background_holds = 0;
                        state.phase = CompactionPhase::Compacting;
                        CompactionAction::SendCompact { hints: None }
                    }
                }
            }
            CompactionPhase::Compacting => {
                if state.compact_boundary_seen {
                    state.phase = CompactionPhase::Normal;
                    state.compact_boundary_seen = false;
                    // Null out context_usage so evaluate_compaction_triggers does
                    // not re-fire the hard trigger against stale pre-compaction fill
                    // (correctness-11). The next assistant message will repopulate
                    // with post-compaction token counts.
                    // Preserves existing nested lock order: compaction tokio-mutex
                    // (held) → context_usage std-mutex (acquired here).
                    *bridge.context_usage.lock().expect("context_usage lock") = None;
                    CompactionAction::Nothing
                } else {
                    // User-turn TurnCompleted during compaction — /compact is still
                    // in flight. Skip idle broadcast and compaction trigger evaluation.
                    CompactionAction::StayCompacting
                }
            }
            CompactionPhase::Normal => CompactionAction::Nothing,
            CompactionPhase::WaitingForIdle => {
                // A TurnCompleted here is a CC-autonomous/background turn (e.g.
                // task-notification): a user turn cancels the timer in
                // send_outgoing before starting. Background turns deliberately do
                // not reset the idle clock — cancel-and-reset would starve soft
                // compaction in busy multi-agent sessions. Do nothing; the
                // trailing evaluate_compaction_triggers re-checks thresholds
                // against the still-armed WaitingForIdle state (escalating to the
                // hard trigger or cancelling if fill moved). This arm is
                // origin-agnostic — the fall-through is correct for any origin —
                // so the kind is logged only for diagnosability.
                debug!(
                    conversation_id = bridge.conversation_id,
                    origin_kind = result
                        .origin
                        .as_ref()
                        .map(|o| sanitize_untrusted_str(&o.kind, MAX_ORIGIN_KIND_BYTES))
                        .as_deref(),
                    "TurnCompleted during WaitingForIdle — background turn, idle timer preserved"
                );
                CompactionAction::Nothing
            }
        }
    };

    match compaction_action {
        CompactionAction::StayCompacting | CompactionAction::HoldForBackgroundTurn => {
            return;
        }
        CompactionAction::SendCompact { hints } => {
            let compact_cmd = match hints {
                Some(h) => format!("/compact {h}"),
                None => "/compact".to_string(),
            };
            info!(
                conversation_id = bridge.conversation_id,
                "sending {compact_cmd}"
            );
            // Bypasses DB persistence — /compact is sent from the event loop,
            // not from a WS handler, so no persist_and_send wrapper.
            let session = bridge.session.lock().await;
            if let Some(session) = session.as_ref()
                && session.is_alive()
            {
                // Set busy only after the send succeeds — same rationale as
                // send_message: a failed send must not leave cc_idle=false.
                match session.send_message(&compact_cmd).await {
                    Ok(()) => {
                        bridge.set_cc_busy("compaction");
                        // The next TurnCompleted (from /compact) will handle it.
                        return;
                    }
                    Err(e) => {
                        error!("failed to send /compact to CC: {e}");
                        bridge.compaction.lock().await.phase = CompactionPhase::Normal;
                    }
                }
            } else {
                warn!(
                    conversation_id = bridge.conversation_id,
                    "CC session absent or dead when /compact was due — compaction skipped"
                );
                bridge.compaction.lock().await.phase = CompactionPhase::Normal;
            }
        }
        CompactionAction::Nothing => {}
    }

    // Evaluate compaction triggers unconditionally on every turn. Previously
    // this was gated behind the /context check cadence; now it runs every turn
    // since context-fill data is available after every assistant message.
    let sent_message = evaluate_compaction_triggers(bridge).await;
    if sent_message {
        // A reminder or hard-trigger persist message was sent — CC is working.
        return;
    }

    set_idle_and_drain(bridge).await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(super) mod tests {
    use super::super::ActiveBridges;
    use super::super::test_support::{
        await_fence, drain_broadcast, event_fence, install_failing_session,
        install_recording_session, recv_broadcast, set_context_usage, set_waiting_for_idle,
        test_bridge, test_bridge_singleton, user_text,
    };
    use super::*;
    use brenn_cc::protocol::incoming::{ResultMessage, ResultOrigin};
    use brenn_cc::session::{ApprovalKind, ApprovalRequest, SessionEvent};
    use brenn_lib::conversation;
    use brenn_lib::ws_types::CcState;

    use crate::active_bridge::test_fixtures::TestBridgeConfig;
    use std::time::Duration;
    use tokio::sync::{broadcast, mpsc, oneshot};

    use super::super::cc_event_loop::cc_event_loop;

    /// Minimal ResultMessage for tests that just need a turn-completed event.
    fn stub_result() -> ResultMessage {
        ResultMessage {
            subtype: Some("success".into()),
            duration_ms: None,
            duration_api_ms: None,
            is_error: Some(false),
            num_turns: Some(1),
            session_id: None,
            total_cost_usd: None,
            usage: None,
            result: None,
            stop_reason: None,
            model_usage: None,
            origin: None,
            extra: serde_json::json!({}),
        }
    }

    /// A ResultMessage stamped with the given origin kind (a CC-autonomous turn).
    fn result_with_origin_kind(kind: &str) -> ResultMessage {
        ResultMessage {
            origin: Some(ResultOrigin {
                kind: kind.into(),
                extra: serde_json::Value::Null,
            }),
            ..stub_result()
        }
    }

    /// Primary regression: a `TurnCompleted` in `WaitingForIdle` must not panic.
    /// With fill below the soft threshold, the trailing trigger evaluation
    /// cancels the timer and resets to `Normal`. Against the pre-fix code this
    /// test panics in the test task.
    #[tokio::test]
    async fn turn_completed_during_waiting_for_idle_does_not_panic() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        set_waiting_for_idle(&bridge).await;
        set_context_usage(&bridge, 50); // below soft_pct (75)

        handle_turn_completed(&bridge, &stub_result(), &ad).await;

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "below-soft fill must reset phase to Normal"
        );
        assert!(
            state.idle_timer.is_none(),
            "below-soft branch must cancel the idle timer"
        );
    }

    /// Timer-preservation: a background `TurnCompleted` with fill still between
    /// soft and hard must leave the original idle timer running (soft compaction
    /// is not starved by continuous background turns).
    #[tokio::test]
    async fn turn_completed_during_waiting_for_idle_keeps_timer_when_still_above_soft() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        set_waiting_for_idle(&bridge).await;
        set_context_usage(&bridge, 80); // between soft_pct (75) and hard_pct (95)

        handle_turn_completed(&bridge, &result_with_origin_kind("task-notification"), &ad).await;

        {
            let state = bridge.compaction.lock().await;
            assert!(
                matches!(state.phase, CompactionPhase::WaitingForIdle),
                "phase must stay WaitingForIdle while fill is between soft and hard"
            );
            assert!(
                state.idle_timer.is_some(),
                "the original idle timer must be preserved"
            );
        }

        // Cleanup: abort the preserved timer so the test doesn't leak it.
        bridge
            .compaction
            .lock()
            .await
            .idle_timer
            .take()
            .unwrap()
            .abort();
    }

    /// Hard escalation: a background `TurnCompleted` with fill at/above the hard
    /// threshold cancels the timer and (send failing without a session) resets to
    /// `Normal`.
    #[tokio::test]
    async fn turn_completed_during_waiting_for_idle_escalates_when_hard() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        set_waiting_for_idle(&bridge).await;
        set_context_usage(&bridge, 96); // above hard_pct (95)

        handle_turn_completed(&bridge, &result_with_origin_kind("task-notification"), &ad).await;

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "hard escalation with no session resets to Normal after send failure"
        );
        assert!(state.idle_timer.is_none(), "timer must be cancelled");
    }

    /// End-to-end survival: a `TurnCompleted` in `WaitingForIdle` driven through
    /// the real event loop must not wedge it. Against the pre-fix code the loop
    /// task panics and the fence never advances (harness times out).
    #[tokio::test]
    async fn event_loop_survives_turn_completed_in_waiting_for_idle() {
        let (bridge, event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        set_waiting_for_idle(&bridge).await;
        set_context_usage(&bridge, 50); // below soft → resolves to Normal, no timer leak

        let fence = event_fence(&bridge);
        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .expect("event loop must accept the event (loop alive)");
        await_fence(fence).await;

        // The loop survived if it processes a second event.
        let fence2 = event_fence(&bridge);
        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .expect("event loop must still be alive for a second event");
        await_fence(fence2).await;
    }

    /// A background turn completing during `PersistingState` must hold the phase
    /// and not fire `/compact` (CC is still working the persist turn).
    #[tokio::test]
    async fn persisting_state_background_turn_stays() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let mut rx = install_recording_session(&bridge).await;
        bridge.compaction.lock().await.phase = CompactionPhase::PersistingState;

        handle_turn_completed(&bridge, &result_with_origin_kind("task-notification"), &ad).await;

        assert!(
            matches!(
                bridge.compaction.lock().await.phase,
                CompactionPhase::PersistingState
            ),
            "background turn must not leave PersistingState"
        );
        assert!(
            rx.try_recv().is_err(),
            "no /compact must be sent on a background turn"
        );
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(
            msgs.iter()
                .any(|m| matches!(m, WsServerMessage::CostUsage { .. })),
            "CostUsage still broadcasts"
        );
        assert!(
            !msgs.iter().any(|m| matches!(
                m,
                WsServerMessage::Status {
                    state: CcState::Idle
                }
            )),
            "no idle broadcast while holding for the awaited turn"
        );
    }

    /// A foreground turn completing during `PersistingState` proceeds to
    /// `Compacting` and sends `/compact` (today's behavior; the Foreground path).
    #[tokio::test]
    async fn persisting_state_foreground_turn_compacts() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let mut rx = install_recording_session(&bridge).await;
        bridge.compaction.lock().await.phase = CompactionPhase::PersistingState;

        handle_turn_completed(&bridge, &stub_result(), &ad).await; // origin absent → Foreground

        assert!(
            matches!(
                bridge.compaction.lock().await.phase,
                CompactionPhase::Compacting
            ),
            "foreground persist completion transitions to Compacting"
        );
        let msg = rx.try_recv().expect("/compact must be sent");
        assert_eq!(user_text(&msg), "/compact");
    }

    /// A background turn during `PendingTurnCompletion` preserves both the phase
    /// and the LLM hints; the following foreground turn compacts with the
    /// original hints intact.
    #[tokio::test]
    async fn pending_turn_completion_background_turn_stays() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let mut rx = install_recording_session(&bridge).await;
        bridge.compaction.lock().await.phase = CompactionPhase::PendingTurnCompletion {
            hints: Some("remember X".into()),
        };

        // Background turn: phase + hints preserved, nothing sent.
        handle_turn_completed(&bridge, &result_with_origin_kind("task-notification"), &ad).await;
        match &bridge.compaction.lock().await.phase {
            CompactionPhase::PendingTurnCompletion { hints } => {
                assert_eq!(hints.as_deref(), Some("remember X"), "hints must survive");
            }
            other => panic!("expected PendingTurnCompletion, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "no /compact on the background turn");

        // Foreground turn: compacts with the original hints.
        handle_turn_completed(&bridge, &stub_result(), &ad).await;
        assert!(matches!(
            bridge.compaction.lock().await.phase,
            CompactionPhase::Compacting
        ));
        let msg = rx
            .try_recv()
            .expect("/compact must be sent on the foreground turn");
        assert_eq!(user_text(&msg), "/compact remember X");
    }

    /// An unknown origin kind is treated as Foreground (proceeds to Compacting)
    /// and fires exactly one deduped Warning alert naming the kind.
    #[tokio::test]
    async fn unknown_origin_kind_treated_foreground_and_alerts() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, count, handle) = brenn_lib::obs::alerting::make_counting_alerter();
        let _rx = install_recording_session(&bridge).await;

        // First unknown-kind result in PersistingState → Compacting + 1 alert.
        bridge.compaction.lock().await.phase = CompactionPhase::PersistingState;
        handle_turn_completed(
            &bridge,
            &result_with_origin_kind("kind-from-the-future"),
            &ad,
        )
        .await;
        assert!(
            matches!(
                bridge.compaction.lock().await.phase,
                CompactionPhase::Compacting
            ),
            "unknown kind must fall through to the Foreground path"
        );

        // Reset to PersistingState and classify the same kind again — the alert
        // is deduped, so the count must stay at one.
        bridge.compaction.lock().await.phase = CompactionPhase::PersistingState;
        handle_turn_completed(
            &bridge,
            &result_with_origin_kind("kind-from-the-future"),
            &ad,
        )
        .await;

        drop(ad);
        handle.await.unwrap();
        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "unknown origin kind alerts exactly once per process (deduped)"
        );
    }

    #[tokio::test]
    async fn turn_completion_with_pending_permissions_broadcasts_cancel() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Insert a pending permission by firing ApprovalRequired.
        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_turn_cancel".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "tu_turn".into(),
                input: serde_json::json!({"command": "true"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();
        // Drain live PermissionRequest + Status(AwaitingApproval).
        let _ = recv_broadcast(&mut broadcast_rx).await;
        let _ = recv_broadcast(&mut broadcast_rx).await;

        // Fire TurnCompleted — should clear the map and emit
        // PermissionCancelled for the stranded entry, plus Status(Idle).
        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .unwrap();

        // Collect the next couple of broadcasts. We expect PermissionCancelled
        // (for the stranded entry) and Status(Idle). Order matches the
        // handler's emit sequence: cancel first, then idle.
        let mut saw_cancel = false;
        let mut saw_idle_status = false;
        for _ in 0..4 {
            match tokio::time::timeout(std::time::Duration::from_millis(200), broadcast_rx.recv())
                .await
            {
                Ok(Ok(WsServerMessage::PermissionCancelled { request_id })) => {
                    assert_eq!(request_id, "req_turn_cancel");
                    saw_cancel = true;
                }
                Ok(Ok(WsServerMessage::Status {
                    state: CcState::Idle,
                })) => {
                    saw_idle_status = true;
                }
                Ok(Ok(_other)) => continue,
                _ => break,
            }
        }
        assert!(
            saw_cancel,
            "TurnCompleted must emit PermissionCancelled for stranded entries"
        );
        assert!(saw_idle_status, "TurnCompleted must emit Status(Idle)");
        assert!(bridge.pending_permission_snapshots().await.is_empty());
    }

    /// Regression guard on the `!cleared_ids.is_empty()` check in
    /// `handle_turn_completed`: a normal turn completion with an empty
    /// `pending_permissions` map must NOT broadcast any `PermissionCancelled`
    /// (the frontend's `removeFromApprovalQueue` is a no-op on unknown ids
    /// but we should not broadcast spurious dismiss signals).
    #[tokio::test]
    async fn turn_completion_with_empty_permissions_is_silent() {
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .unwrap();

        // Drain broadcasts for a moment and assert no PermissionCancelled.
        let mut saw_cancel = false;
        let mut saw_idle_status = false;
        for _ in 0..4 {
            match tokio::time::timeout(std::time::Duration::from_millis(100), broadcast_rx.recv())
                .await
            {
                Ok(Ok(WsServerMessage::PermissionCancelled { .. })) => {
                    saw_cancel = true;
                }
                Ok(Ok(WsServerMessage::Status {
                    state: CcState::Idle,
                })) => {
                    saw_idle_status = true;
                }
                Ok(Ok(_other)) => continue,
                _ => break,
            }
        }
        assert!(
            !saw_cancel,
            "empty pending map must not broadcast PermissionCancelled"
        );
        assert!(
            saw_idle_status,
            "TurnCompleted must still emit Status(Idle)"
        );
    }

    /// Turn-completion cancel broadcast must fire for *every* entry in the
    /// map, not just the first. Guards against a `for id in [ids[0]]`-style
    /// regression.
    #[tokio::test]
    async fn turn_completion_broadcasts_cancel_for_multiple_entries() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Insert two pending permissions via the test helper (bypasses the
        // live ApprovalRequired flow — acceptable because this test doesn't
        // exercise the oneshot path).
        bridge
            .insert_pending_permission_for_test(
                "req_multi_a",
                "Bash",
                serde_json::json!({"command": "a"}),
            )
            .await;
        bridge
            .insert_pending_permission_for_test(
                "req_multi_b",
                "Bash",
                serde_json::json!({"command": "b"}),
            )
            .await;

        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .unwrap();

        // Collect the broadcast; both ids must appear in a PermissionCancelled
        // frame before Status(Idle).
        let mut cancelled_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        for _ in 0..8 {
            match tokio::time::timeout(std::time::Duration::from_millis(200), broadcast_rx.recv())
                .await
            {
                Ok(Ok(WsServerMessage::PermissionCancelled { request_id })) => {
                    cancelled_ids.insert(request_id);
                }
                Ok(Ok(_other)) => continue,
                _ => break,
            }
        }
        assert!(
            cancelled_ids.contains("req_multi_a") && cancelled_ids.contains("req_multi_b"),
            "TurnCompleted must cancel every entry; got: {cancelled_ids:?}"
        );
        assert!(bridge.pending_permission_snapshots().await.is_empty());
    }

    #[tokio::test]
    async fn turn_completed_with_pending_compaction_transitions_state() {
        // No session → /compact send falls through → resets to Normal.
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;
        bridge.compaction.lock().await.phase = CompactionPhase::PendingTurnCompletion {
            hints: Some("remember groceries".into()),
        };

        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .unwrap();

        // CostUsage is broadcast before Status::Idle — receive in order
        // (quality-5: no sleep+drain; use sequential recv_broadcast instead).
        let cost_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(cost_msg, WsServerMessage::CostUsage { .. }),
            "first broadcast should be CostUsage, got {cost_msg:?}"
        );
        let idle_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(
                idle_msg,
                WsServerMessage::Status {
                    state: CcState::Idle
                }
            ),
            "second broadcast should be Status::Idle, got {idle_msg:?}"
        );

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "phase should be Normal after session-dead fallthrough"
        );
    }

    #[tokio::test]
    async fn turn_completed_during_compacting_resets_phase() {
        // Turn completion during Compacting resets phase to Normal and broadcasts
        // CostUsage then Status::Idle — no drain attempt.
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;
        // Install a recording session so we can assert no send occurs (no drain attempt).
        let mut rx = install_recording_session(&bridge).await;
        {
            let mut state = bridge.compaction.lock().await;
            state.phase = CompactionPhase::Compacting;
            // Pre-set compact_boundary_seen so the Compacting → Normal transition
            // fires. Without this, TurnCompleted stays in Compacting (StayCompacting).
            state.compact_boundary_seen = true;
        }
        // Pre-seed context_usage to Some so the nulling assertion is non-trivial.
        *bridge.context_usage.lock().expect("context_usage lock") = Some(ContextUsage {
            current_tokens: 100_000,
            max_tokens: 200_000,
            usage_pct: 50,
            checked_at: std::time::Instant::now(),
        });

        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .unwrap();

        // CostUsage before Status::Idle (quality-5: sequential recv, no sleep).
        let cost_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(cost_msg, WsServerMessage::CostUsage { .. }),
            "first broadcast should be CostUsage, got {cost_msg:?}"
        );
        let idle_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(
                idle_msg,
                WsServerMessage::Status {
                    state: CcState::Idle
                }
            ),
            "second broadcast should be Status::Idle after compaction complete, got {idle_msg:?}"
        );

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "phase must be Normal after Compacting → TurnCompleted"
        );
        // context_usage is nulled out by handle_turn_completed in Compacting arm.
        // The pre-seeded Some(ContextUsage) above makes this assertion non-trivial.
        assert!(
            bridge
                .context_usage
                .lock()
                .expect("context_usage lock")
                .is_none(),
            "context_usage must be None after compaction completes"
        );
        // No send should occur during Compacting → Normal transition (no drain attempt).
        assert!(
            rx.try_recv().is_err(),
            "no message must be sent to CC during Compacting → Normal transition"
        );
    }

    /// A `TurnCompleted` arriving while phase is `Compacting` but
    /// `compact_boundary_seen` is false must stay in `Compacting` — this is a
    /// user-turn completing before `/compact` finishes. Phase must not transition
    /// to `Normal`, `context_usage` must not be nulled, and no idle broadcast
    /// must fire.
    #[tokio::test]
    async fn user_message_during_compacting_does_not_reset_phase() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;
        bridge.compaction.lock().await.phase = CompactionPhase::Compacting;
        // compact_boundary_seen defaults to false — no need to set it.
        // Pre-seed context_usage so we can assert it is not nulled.
        *bridge.context_usage.lock().expect("context_usage lock") = Some(ContextUsage {
            current_tokens: 80_000,
            max_tokens: 200_000,
            usage_pct: 40,
            checked_at: std::time::Instant::now(),
        });

        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .unwrap();

        // Only CostUsage should arrive (cost broadcast runs before the phase
        // match). No Status::Idle must be broadcast.
        let cost_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(cost_msg, WsServerMessage::CostUsage { .. }),
            "expected CostUsage broadcast, got {cost_msg:?}"
        );
        // No further broadcasts — no idle status.
        let result = tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            recv_broadcast(&mut broadcast_rx),
        )
        .await;
        assert!(
            result.is_err(),
            "no Status::Idle must be broadcast while phase stays Compacting"
        );

        // Phase must remain Compacting.
        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Compacting),
            "phase must remain Compacting when compact_boundary_seen is false"
        );
        // context_usage must not be nulled.
        assert!(
            bridge
                .context_usage
                .lock()
                .expect("context_usage lock")
                .is_some(),
            "context_usage must not be nulled when TurnCompleted fires before CompactBoundary"
        );
    }

    /// Multiple user-turn `TurnCompleted` events during `Compacting` (the motivating
    /// scenario) — phase must stay `Compacting` after each, then transition on the
    /// real `/compact` TurnCompleted after `CompactBoundary`.
    #[tokio::test]
    async fn multiple_user_turns_during_compacting_do_not_reset_phase() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;
        bridge.compaction.lock().await.phase = CompactionPhase::Compacting;
        *bridge.context_usage.lock().expect("context_usage lock") = Some(ContextUsage {
            current_tokens: 80_000,
            max_tokens: 200_000,
            usage_pct: 40,
            checked_at: std::time::Instant::now(),
        });

        // Fire two user-turn TurnCompleteds before CompactBoundary.
        for i in 0..2 {
            event_tx
                .send(SessionEvent::TurnCompleted(stub_result()))
                .await
                .unwrap();

            // Only CostUsage broadcast per turn — no Status::Idle.
            let cost_msg = recv_broadcast(&mut broadcast_rx).await;
            assert!(
                matches!(cost_msg, WsServerMessage::CostUsage { .. }),
                "turn {i}: expected CostUsage, got {cost_msg:?}"
            );
            let no_idle = tokio::time::timeout(
                tokio::time::Duration::from_millis(100),
                recv_broadcast(&mut broadcast_rx),
            )
            .await;
            assert!(
                no_idle.is_err(),
                "turn {i}: Status::Idle must not be broadcast while phase is Compacting"
            );

            let state = bridge.compaction.lock().await;
            assert!(
                matches!(state.phase, CompactionPhase::Compacting),
                "turn {i}: phase must remain Compacting"
            );
            assert!(
                bridge
                    .context_usage
                    .lock()
                    .expect("context_usage lock")
                    .is_some(),
                "turn {i}: context_usage must not be nulled"
            );
        }

        // CompactBoundary sets the flag.
        let fence = event_fence(&bridge);
        event_tx
            .send(SessionEvent::CompactBoundary { metadata: None })
            .await
            .unwrap();
        await_fence(fence).await;
        assert!(
            bridge.compaction.lock().await.compact_boundary_seen,
            "compact_boundary_seen must be true after CompactBoundary"
        );

        // /compact's TurnCompleted fires the real transition.
        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .unwrap();

        let cost_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(cost_msg, WsServerMessage::CostUsage { .. }),
            "expected CostUsage on /compact turn, got {cost_msg:?}"
        );
        let idle_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(
                idle_msg,
                WsServerMessage::Status {
                    state: CcState::Idle
                }
            ),
            "expected Status::Idle after /compact completes, got {idle_msg:?}"
        );

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "phase must be Normal after /compact TurnCompleted"
        );
        assert!(
            !state.compact_boundary_seen,
            "compact_boundary_seen must be cleared"
        );
        assert!(
            bridge
                .context_usage
                .lock()
                .expect("context_usage lock")
                .is_none(),
            "context_usage must be None after compaction completes"
        );
    }

    /// Full sequence: set phase `Compacting`, deliver `CompactBoundary`, deliver
    /// `TurnCompleted`. Phase must transition to `Normal`, `context_usage` → `None`,
    /// `compact_boundary_seen` → `false`.
    #[tokio::test]
    async fn compacting_resets_after_boundary_and_turn_completed() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;
        bridge.compaction.lock().await.phase = CompactionPhase::Compacting;
        *bridge.context_usage.lock().expect("context_usage lock") = Some(ContextUsage {
            current_tokens: 150_000,
            max_tokens: 200_000,
            usage_pct: 75,
            checked_at: std::time::Instant::now(),
        });

        // Step 1: CompactBoundary event sets compact_boundary_seen = true.
        let fence = event_fence(&bridge);
        event_tx
            .send(SessionEvent::CompactBoundary { metadata: None })
            .await
            .unwrap();
        await_fence(fence).await;
        {
            let state = bridge.compaction.lock().await;
            assert!(
                state.compact_boundary_seen,
                "compact_boundary_seen must be true after CompactBoundary event"
            );
            assert!(
                matches!(state.phase, CompactionPhase::Compacting),
                "phase must stay Compacting after CompactBoundary"
            );
        }

        // Step 2: /compact's TurnCompleted fires the transition.
        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .unwrap();

        let cost_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(cost_msg, WsServerMessage::CostUsage { .. }),
            "expected CostUsage, got {cost_msg:?}"
        );
        let idle_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(
                idle_msg,
                WsServerMessage::Status {
                    state: CcState::Idle
                }
            ),
            "expected Status::Idle after full CompactBoundary + TurnCompleted, got {idle_msg:?}"
        );

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "phase must be Normal after full compaction sequence"
        );
        assert!(
            !state.compact_boundary_seen,
            "compact_boundary_seen must be cleared after transition"
        );
        assert!(
            bridge
                .context_usage
                .lock()
                .expect("context_usage lock")
                .is_none(),
            "context_usage must be None after compaction completes"
        );
    }

    #[tokio::test]
    async fn died_event_resets_compaction_state() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;
        // Set up all compaction state fields to non-default values so the
        // assertions below are non-vacuous.
        {
            let mut state = bridge.compaction.lock().await;
            state.phase = CompactionPhase::Compacting;
            state.reminder_sent = true;
            state.compact_boundary_seen = true;
            state.idle_timer = Some(tokio::spawn(async {
                tokio::time::sleep(std::time::Duration::from_secs(9999)).await;
            }));
        }

        event_tx
            .send(SessionEvent::Died(brenn_cc::error::CcError::SendFailed))
            .await
            .unwrap();

        let _msg1 = recv_broadcast(&mut broadcast_rx).await;
        let _msg2 = recv_broadcast(&mut broadcast_rx).await;

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "phase should be Normal after Died"
        );
        assert!(
            state.idle_timer.is_none(),
            "idle_timer should be None after Died"
        );
        assert!(
            !state.reminder_sent,
            "reminder_sent should be false after Died"
        );
        assert!(
            !state.compact_boundary_seen,
            "compact_boundary_seen should be false after Died"
        );
    }

    #[tokio::test]
    async fn compaction_send_does_not_set_busy_when_send_fails() {
        // Session is alive but send channel is closed → send_message returns Err.
        // set_cc_busy must not fire; phase resets to Normal.
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;
        install_failing_session(&bridge).await;
        bridge.compaction.lock().await.phase =
            CompactionPhase::PendingTurnCompletion { hints: None };

        event_tx
            .send(SessionEvent::TurnCompleted(stub_result()))
            .await
            .unwrap();

        // CostUsage then Status::Idle — a spurious set_cc_busy would insert
        // Status::Thinking between them, breaking the second match.
        let cost_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(cost_msg, WsServerMessage::CostUsage { .. }),
            "first broadcast should be CostUsage, got {cost_msg:?}"
        );
        let idle_msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(
                idle_msg,
                WsServerMessage::Status {
                    state: CcState::Idle
                }
            ),
            "second broadcast should be Status::Idle, got {idle_msg:?}"
        );

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "phase should be Normal after send failure"
        );
    }

    #[test]
    fn cost_delta_basic() {
        // Two consecutive totals: 0.10 then 0.30 → delta 0.20.
        let prev: f64 = 0.10;
        let cur: f64 = 0.30;
        let last_turn = if cur < prev { cur } else { cur - prev };
        assert!((last_turn - 0.20).abs() < 1e-9);
    }

    #[test]
    fn cost_delta_handles_compaction_reset() {
        // Compaction: previous 1.50, current 0.001 → cur < prev → last_turn = cur.
        let prev: f64 = 1.50;
        let cur: f64 = 0.001;
        let last_turn = if cur < prev { cur } else { cur - prev };
        assert!((last_turn - 0.001).abs() < 1e-9);
    }

    /// Bridge-path cost delta: two consecutive `TurnCompleted` events with
    /// real `total_cost_usd` values exercise the full path through
    /// `handle_turn_completed` — `last_total_cost_usd` mutex read/write,
    /// `cost_samples::insert`, `CostUsage` broadcast — not just the inline
    /// arithmetic. The second turn must emit `CostUsage { last_turn_usd ≈ 0.20 }`.
    ///
    /// An all-zeros `CostUsage` broadcast would fail this test, proving the
    /// bridge-level path is exercised (not only the arithmetic unit tests above).
    #[tokio::test]
    async fn cost_delta_bridge_path() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Turn 1: total_cost_usd = 0.10 → last_turn = 0.10 (prev was 0).
        let fence = event_fence(&bridge);
        event_tx
            .send(SessionEvent::TurnCompleted(result_with_cost(0.10)))
            .await
            .unwrap();
        // Drain turn-1 broadcasts (CostUsage + Status(Idle)) after the handler
        // completes. Fence ensures the match arm fully completes before draining.
        // Valid because handle_turn_completed completes within one match-arm
        // invocation (no mid-handler yield releasing the loop to another arm).
        // If the handler is ever restructured across multiple invocations, this
        // drain would be incomplete and turn-2 could see stale messages.
        await_fence(fence).await;
        drain_broadcast(&mut broadcast_rx);

        // Turn 2: total_cost_usd = 0.30 → last_turn = 0.30 - 0.10 = 0.20.
        event_tx
            .send(SessionEvent::TurnCompleted(result_with_cost(0.30)))
            .await
            .unwrap();

        // CostUsage is broadcast before Status(Idle); receive it first.
        let cost_msg = recv_broadcast(&mut broadcast_rx).await;
        match cost_msg {
            WsServerMessage::CostUsage { last_turn_usd, .. } => {
                assert!(
                    (last_turn_usd - 0.20).abs() < 1e-9,
                    "second turn delta must be 0.20; got {last_turn_usd}"
                );
            }
            other => panic!("expected CostUsage, got {other:?}"),
        }

        // Confirm last_total_cost_usd is persisted in the bridge mutex.
        let stored = bridge
            .last_total_cost_usd
            .lock()
            .expect("lock")
            .expect("must be set after two cost turns");
        assert!(
            (stored - 0.30).abs() < 1e-9,
            "last_total_cost_usd must be 0.30 after turn 2; got {stored}"
        );

        // Verify cost_samples::insert was called — DB rows exist and sum correctly.
        // If the insert guard were inverted (or the DB call skipped), this fails.
        let cutoff = chrono::Utc::now() - chrono::Duration::hours(24);
        let db_sum = {
            let conn = bridge.db.lock().await;
            brenn_lib::cost_samples::sum_since(&conn, cutoff)
        };
        assert!(
            (db_sum - 0.30).abs() < 1e-6,
            "cost_samples DB rows must sum to 0.30 (both turns inserted); got {db_sum}"
        );
    }

    /// Drives a single `TurnCompleted` through `handle_turn_completed` with a
    /// `ResultMessage` carrying both `total_cost_usd` and a `modelUsage` map
    /// with an active-model entry having a `contextWindow`. Asserts that all
    /// three DB regions in the unified scope landed — if any region were
    /// silently dropped by the refactor this test catches it.
    ///
    /// Named "in_one_call" (not "in_one_lock"): asserts observable effects,
    /// not the number of `bridge.db.lock().await` acquisitions. The one-lock
    /// invariant is defended by comment blocks in `handle_turn_completed` and
    /// by the `!Send` compile-error backstop (see design doc §"Edge cases").
    #[tokio::test]
    async fn turn_end_writes_message_cost_and_window_in_one_call() {
        let active_slug = "claude-sonnet-4-6";
        let out = run_turn_completed_with_model_usage(
            active_slug,
            active_slug,
            0.042,
            180_000,
            &[active_slug],
        )
        .await;
        let cached = &out.cache_lookups[0];
        assert!(
            cached.1.is_some(),
            "model_window_cache must have entry for {}",
            cached.0
        );
        assert_eq!(
            cached.1.as_ref().unwrap().0,
            180_000,
            "cached max_tokens must match"
        );
    }

    /// Exercises the suffix-match path of `pick_and_observe_model_usage` through
    /// the full `handle_turn_completed` flow: the map key is `"claude-sonnet-4-6[200k]"`
    /// while `active_model_slug` is the bare `"claude-sonnet-4-6"`.
    ///
    /// Complements `turn_end_writes_message_cost_and_window_in_one_call` (exact-match)
    /// and `update_max_tokens_from_modelusage_overrides_seed` (suffix-match but bypasses
    /// the event-loop dispatch, DB lock coalescing, broadcast gate, and cost bookkeeping).
    ///
    /// Key assertions: suffixed key present in cache, bare key absent, and `ContextUsage`
    /// broadcast emitted (verifying the `model_window_updated` gate fires for suffix-match).
    #[tokio::test]
    async fn turn_end_suffix_match_writes_model_window_and_broadcasts() {
        let active_slug = "claude-sonnet-4-6";
        let suffixed_key = "claude-sonnet-4-6[200k]";
        let out = run_turn_completed_with_model_usage(
            active_slug,
            suffixed_key,
            0.037,
            180_000,
            &[suffixed_key, active_slug],
        )
        .await;
        let cached_suffixed = &out.cache_lookups[0];
        assert!(
            cached_suffixed.1.is_some(),
            "cache must have entry for {}",
            cached_suffixed.0
        );
        assert_eq!(
            cached_suffixed.1.as_ref().unwrap().0,
            180_000,
            "cached max_tokens must match for {}",
            cached_suffixed.0
        );
        let cached_bare = &out.cache_lookups[1];
        assert!(
            cached_bare.1.is_none(),
            "cache must NOT have entry for bare slug {}",
            cached_bare.0
        );
    }

    // -----------------------------------------------------------------------
    // last_cost_prune_at gate test
    // -----------------------------------------------------------------------

    /// Verify that `handle_turn_completed` stores a non-zero timestamp in
    /// `last_cost_prune_at` after the first call, and that a second call within
    /// 3600 seconds skips `prune_before` (prune-gate suppression).
    ///
    /// We verify skip-behaviour indirectly: insert a row older than 24h, call
    /// handle_turn_completed twice in quick succession, confirm the old row
    /// is present after the second call (prune was skipped on second call).
    #[tokio::test]
    async fn cost_prune_gate_stores_timestamp_and_suppresses_second_prune() {
        use chrono::Duration as CDuration;

        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();

        // Confirm gate starts at 0.
        assert_eq!(
            bridge.last_cost_prune_at.load(Ordering::Relaxed),
            0,
            "gate should start at 0"
        );

        // Insert a row older than 24h so prune would delete it on the first call.
        let old = chrono::Utc::now() - CDuration::hours(25);
        {
            let conn = bridge.db.lock().await;
            brenn_lib::cost_samples::insert_at(&conn, bridge.conversation_id, 0.10, old);
        }

        // First call: gate is 0, prune runs and deletes the old row; gate is updated.
        handle_turn_completed(&bridge, &stub_result(), &ad).await;

        let stored = bridge.last_cost_prune_at.load(Ordering::Relaxed);
        assert!(
            stored > 0,
            "last_cost_prune_at must be set after first call"
        );

        // Old row should be gone.
        {
            let conn = bridge.db.lock().await;
            let total = brenn_lib::cost_samples::sum_since(
                &conn,
                chrono::Utc::now() - CDuration::hours(26),
            );
            assert!(
                (total - 0.0).abs() < 1e-9,
                "old row should have been pruned on first call; sum={total}"
            );
        }

        // Insert another old row.
        let old2 = chrono::Utc::now() - CDuration::hours(25);
        {
            let conn = bridge.db.lock().await;
            brenn_lib::cost_samples::insert_at(&conn, bridge.conversation_id, 0.42, old2);
        }

        // Second call: gate was just set (< 3600s ago), prune should be skipped.
        handle_turn_completed(&bridge, &stub_result(), &ad).await;

        {
            let conn = bridge.db.lock().await;
            let total = brenn_lib::cost_samples::sum_since(
                &conn,
                chrono::Utc::now() - CDuration::hours(26),
            );
            assert!(
                (total - 0.42).abs() < 1e-9,
                "old row should NOT have been pruned on second call (gate suppresses); sum={total}"
            );
        }
    }

    /// `model_window_cache::get` return type: `(max_tokens, cc_version, updated_at)`.
    type ModelWindowCacheEntry = Option<(u64, Option<String>, String)>;

    /// Outcome returned by `run_turn_completed_with_model_usage`. Carries only
    /// variant-specific data; shared invariants are asserted inside the helper.
    struct TurnCompletedOutcome {
        /// One entry per key in the `cache_lookup_keys` slice passed to the
        /// helper, in the same order. Each entry is `(lookup_key,
        /// model_window_cache::get result)`.
        cache_lookups: Vec<(String, ModelWindowCacheEntry)>,
    }

    /// Shared harness for the two turn-end integration tests.
    ///
    /// Sets up a bridge with compaction config, pre-seeds `ContextUsage` (pre-seed
    /// `max_tokens = 200_000`, intentionally different from the upserted
    /// `context_window` value, so the cache assertion cannot pass accidentally from
    /// the seed), sends a `TurnCompleted` event, and awaits completion.
    ///
    /// Asserts all shared invariants internally:
    /// - Exactly one "result" message row appended.
    /// - `cost_samples` sum matches `cost`.
    /// - All three `CostUsage` broadcast fields match `cost`.
    /// - `ContextUsage` broadcast was emitted.
    ///
    /// Returns `TurnCompletedOutcome` with `model_window_cache::get` results for
    /// each key in `cache_lookup_keys` (positional — index matches slice order).
    async fn run_turn_completed_with_model_usage(
        active_slug: &str,
        map_key: &str,
        cost: f64,
        context_window: u64,
        cache_lookup_keys: &[&str],
    ) -> TurnCompletedOutcome {
        // Guard: context_window must differ from the pre-seed max_tokens (200_000)
        // so cache assertions cannot pass accidentally from the seed value.
        debug_assert!(
            context_window != 200_000,
            "context_window must differ from pre-seed max_tokens (200_000) to keep cache assertions meaningful"
        );

        use brenn_cc::protocol::incoming::{ModelUsageEntry, ResultMessage};
        use std::collections::HashMap;
        use std::time::Instant;

        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_compaction_config().await;

        // Pre-seed context_usage so broadcast_context_usage is not a no-op.
        // max_tokens is 200_000 — different from context_window — so the cache
        // assertion cannot pass accidentally from the seed value.
        *bridge.context_usage.lock().expect("lock") = Some(ContextUsage {
            current_tokens: 50_000,
            max_tokens: 200_000,
            usage_pct: 25,
            checked_at: Instant::now(),
        });
        *bridge.active_model_slug.lock().expect("lock") = Some(active_slug.to_string());

        let mut model_usage_map = HashMap::new();
        model_usage_map.insert(
            map_key.to_string(),
            ModelUsageEntry {
                context_window: Some(context_window),
                max_output_tokens: None,
                cost_usd: None,
                extra: serde_json::Value::Null,
            },
        );
        let result = ResultMessage {
            subtype: Some("success".into()),
            duration_ms: None,
            duration_api_ms: None,
            is_error: Some(false),
            num_turns: Some(1),
            session_id: None,
            total_cost_usd: Some(cost),
            usage: None,
            result: None,
            stop_reason: None,
            model_usage: Some(model_usage_map),
            origin: None,
            extra: serde_json::json!({}),
        };

        let fence = event_fence(&bridge);
        event_tx
            .send(SessionEvent::TurnCompleted(result))
            .await
            .expect("event_tx closed — event loop crashed before TurnCompleted was sent");

        // Wait for the event loop to finish processing TurnCompleted, then drain
        // all broadcasts atomically. This avoids the bounded-loop hazard: a
        // capped loop can exhaust its iterations before Status(Idle) arrives and
        // leave cost_msg_opt unset, producing a misleading "missing CostUsage"
        // panic instead of a "loop exhausted" one.
        await_fence(fence).await;
        let broadcasts = drain_broadcast(&mut broadcast_rx);
        let cost_msg_opt: Option<WsServerMessage> = broadcasts
            .iter()
            .find(|m| matches!(m, WsServerMessage::CostUsage { .. }))
            .cloned();
        let saw_context_usage = broadcasts
            .iter()
            .any(|m| matches!(m, WsServerMessage::ContextUsage { .. }));
        let cost_msg = cost_msg_opt.expect("a CostUsage broadcast must have been emitted");

        let cutoff = chrono::Utc::now() - chrono::Duration::hours(25);
        let (messages, db_sum, cache_lookups) = {
            let conn = bridge.db.lock().await;
            let messages = brenn_lib::conversation::get_messages(&conn, bridge.conversation_id);
            let db_sum = brenn_lib::cost_samples::sum_since(&conn, cutoff);
            let cache_lookups: Vec<(String, ModelWindowCacheEntry)> = cache_lookup_keys
                .iter()
                .map(|k| (k.to_string(), brenn_lib::model_window_cache::get(&conn, k)))
                .collect();
            (messages, db_sum, cache_lookups)
        };

        // Shared assertion 1: exactly one "result" row appended.
        assert_eq!(
            messages.len(),
            1,
            "exactly one message row must have been appended; got {:?}",
            messages
        );
        assert_eq!(
            messages[0].msg_type, "result",
            "the appended row must be a 'result' message"
        );

        // Shared assertion 2: cost_samples sum reflects the inserted turn cost.
        assert!(
            (db_sum - cost).abs() < 1e-9,
            "cost_samples must sum to {cost}; got {db_sum}"
        );

        // Shared assertion 3: CostUsage broadcast with expected values (all three fields).
        let WsServerMessage::CostUsage {
            last_turn_usd,
            last_24h_usd,
            since_last_compaction_usd,
        } = cost_msg
        else {
            unreachable!()
        };
        assert!(
            (last_turn_usd - cost).abs() < 1e-9,
            "CostUsage.last_turn_usd must be {cost}; got {last_turn_usd}"
        );
        assert!(
            (last_24h_usd - cost).abs() < 1e-9,
            "CostUsage.last_24h_usd must be {cost}; got {last_24h_usd}"
        );
        assert!(
            (since_last_compaction_usd - cost).abs() < 1e-9,
            "CostUsage.since_last_compaction_usd must be {cost}; got {since_last_compaction_usd}"
        );

        // Shared assertion 4: ContextUsage broadcast was emitted.
        assert!(
            saw_context_usage,
            "a ContextUsage broadcast must have been emitted"
        );

        TurnCompletedOutcome { cache_lookups }
    }

    /// Helper: create a singleton test bridge with compaction config for context
    /// tracking tests.
    pub(in crate::active_bridge) async fn test_bridge_with_compaction_config() -> (
        Arc<ActiveBridge>,
        mpsc::Sender<SessionEvent>,
        broadcast::Receiver<WsServerMessage>,
        ActiveBridges,
    ) {
        let db = brenn_lib::db::init_db_memory();
        let (alert_dispatcher, _handle) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        let active_bridges = ActiveBridges::new();

        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "testuser", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };

        let (broadcast_tx, broadcast_rx) = broadcast::channel(64);
        let config = brenn_lib::config::CompactionConfig {
            reminder_pct: 60,
            soft_pct: 75,
            red_pct: 80,
            hard_pct: 95,
            reminder_tokens: None,
            soft_tokens: None,
            red_tokens: None,
            hard_tokens: None,
            idle_duration: Duration::from_secs(300),
        };
        let bridge = ActiveBridge::inject_for_test_full(
            user_id,
            conv_id,
            "test",
            db,
            broadcast_tx,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges.clone()),
                singleton: true,
                compaction_config: Some(config),
                ..Default::default()
            },
        );

        let (event_tx, event_rx) = mpsc::channel(64);
        tokio::spawn(cc_event_loop(event_rx, bridge.clone(), alert_dispatcher));

        (bridge, event_tx, broadcast_rx, active_bridges)
    }

    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // LLM turn usage attribution integration tests
    // -----------------------------------------------------------------------

    /// Build a ResultMessage with a non-zero cost so the `if last_turn > 0.0`
    /// guard at compaction.rs:245 is satisfied and a usage event is attempted.
    fn result_with_cost(cost: f64) -> ResultMessage {
        ResultMessage {
            subtype: Some("success".into()),
            duration_ms: None,
            duration_api_ms: None,
            is_error: Some(false),
            num_turns: Some(1),
            session_id: None,
            total_cost_usd: Some(cost),
            usage: None,
            result: None,
            stop_reason: None,
            model_usage: None,
            origin: None,
            extra: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn llm_turn_attribution_uses_messages_sender_device_id() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();

        // Create a device and insert a message row with sender_device_id so
        // resolve_sender_for_conversation can return the attribution.
        let device_id = {
            let conn = bridge.db.lock().await;
            let resolved = brenn_lib::auth::device::resolve_or_create_device(
                &conn,
                None,
                bridge.user_id,
                "test-ua",
            );
            brenn_lib::conversation::append_message(
                &conn,
                bridge.conversation_id,
                brenn_lib::conversation::MessageDirection::Outgoing,
                "user",
                None,
                None,
                "hello",
                Some(bridge.user_id),
                None,
                Some(resolved.id),
            );
            resolved.id
        };

        handle_turn_completed(&bridge, &result_with_cost(0.05), &ad).await;

        let conn = bridge.db.lock().await;
        // Count first so that a failure (count != 1) produces a clear assertion
        // message rather than a rusqlite NULL type error from a combined
        // COUNT(*)+device_id query when no rows match.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_events WHERE event_type = 'llm_turn'",
                [],
                |row| row.get(0),
            )
            .expect("query usage_events for llm_turn count");
        assert_eq!(count, 1, "expected exactly one llm_turn usage event");
        let event_device_id: i64 = conn
            .query_row(
                "SELECT device_id FROM usage_events WHERE event_type = 'llm_turn'",
                [],
                |row| row.get(0),
            )
            .expect("query usage_events for llm_turn device_id");
        assert_eq!(
            event_device_id, device_id,
            "llm_turn event device_id must match messages.sender_device_id"
        );
    }

    #[tokio::test]
    async fn llm_turn_no_attribution_drops_event() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        let (ad, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();

        // Precondition: the bridge conversation must have no messages with
        // sender_device_id, so that resolve_sender_for_conversation returns None.
        // Assert explicitly so a future change to test_bridge_with_compaction_config
        // that seeds a message row doesn't silently invalidate this test.
        {
            let conn = bridge.db.lock().await;
            let msg_count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM messages WHERE conversation_id = ?1",
                    rusqlite::params![bridge.conversation_id],
                    |row| row.get(0),
                )
                .expect("precondition: count messages for conversation");
            assert_eq!(
                msg_count, 0,
                "precondition: expected no messages in conversation before handle_turn_completed"
            );
        }

        // No message row with sender_device_id — resolve_sender_for_conversation
        // returns None, so no usage event should be written.
        handle_turn_completed(&bridge, &result_with_cost(0.05), &ad).await;

        let conn = bridge.db.lock().await;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_events WHERE event_type = 'llm_turn'",
                [],
                |row| row.get(0),
            )
            .expect("query usage_events for llm_turn");
        assert_eq!(
            count, 0,
            "expected no llm_turn usage event when no sender_device_id"
        );
    }
}

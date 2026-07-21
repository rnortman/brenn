use std::sync::Arc;

use tracing::{debug, error, info, warn};

use crate::active_bridge::ActiveBridge;

use super::state::{CompactionPhase, ContextUsage, TriggerKind};

/// Returns `Some(kind)` if either the percentage or the (optional) absolute
/// token threshold for a stage is exceeded; `None` if both are below
/// threshold. The percentage is checked first, so when both gates fire the
/// label is `"pct"`.
fn stage_fires(usage: &ContextUsage, pct: u8, tokens: Option<u64>) -> Option<TriggerKind> {
    if usage.usage_pct >= pct {
        return Some(TriggerKind::Pct);
    }
    if let Some(t) = tokens
        && usage.current_tokens >= t
    {
        return Some(TriggerKind::Tokens);
    }
    None
}

pub(super) async fn evaluate_compaction_triggers(bridge: &Arc<ActiveBridge>) -> bool {
    let config = match &bridge.compaction_config {
        Some(c) => c,
        None => return false,
    };

    let usage = bridge
        .context_usage
        .lock()
        .expect("context_usage lock poisoned")
        .clone();
    let Some(usage) = usage else {
        return false; // No usage data yet — skip trigger evaluation.
    };

    // Broadcast is done by the call site in handle_turn_completed (only when
    // modelUsage was present). evaluate_compaction_triggers is a pure evaluator.

    let mut state = bridge.compaction.lock().await;

    // Handle committed phases and WaitingForIdle transitions first.
    match &state.phase {
        CompactionPhase::PersistingState
        | CompactionPhase::Compacting
        | CompactionPhase::PendingTurnCompletion { .. } => {
            return false; // Already committed.
        }
        CompactionPhase::WaitingForIdle => {
            if stage_fires(&usage, config.hard_pct, config.hard_tokens).is_some() {
                // Hard trigger supersedes soft timer. Cancel and fall through
                // to the shared hard-trigger path below.
                state.cancel_idle_timer();
                state.phase = CompactionPhase::Normal;
                info!(
                    conversation_id = bridge.conversation_id,
                    "hard trigger superseding soft idle timer"
                );
            } else if stage_fires(&usage, config.soft_pct, config.soft_tokens).is_none() {
                // Context dropped below soft threshold — cancel timer.
                state.cancel_idle_timer();
                state.phase = CompactionPhase::Normal;
                info!(
                    conversation_id = bridge.conversation_id,
                    usage_pct = usage.usage_pct,
                    current_tokens = usage.current_tokens,
                    "context dropped below soft threshold — cancelled idle timer"
                );
                // Fall through to check reminder_sent clearing below.
            } else {
                // Still between soft and hard — let the timer play out.
                return false;
            }
        }
        CompactionPhase::Normal => {} // Fall through to evaluation.
    }

    // Normal-phase evaluation: check thresholds in descending order.

    if let Some(kind) = stage_fires(&usage, config.hard_pct, config.hard_tokens) {
        // Hard trigger — immediate.
        warn!(
            conversation_id = bridge.conversation_id,
            usage_pct = usage.usage_pct,
            current_tokens = usage.current_tokens,
            hard_pct = config.hard_pct,
            hard_tokens = ?config.hard_tokens,
            trigger = kind.as_str(),
            "hard compaction trigger"
        );
        state.phase = CompactionPhase::PersistingState;
        drop(state);
        let rendered = crate::system_message::render_compaction_hard_trigger(usage.usage_pct);
        if let Err(e) = bridge.send_system_message(rendered, None).await {
            error!("failed to send hard-trigger persist message: {e}");
            bridge.compaction.lock().await.phase = CompactionPhase::Normal;
            return false;
        }
        return true;
    }

    if let Some(kind) = stage_fires(&usage, config.soft_pct, config.soft_tokens) {
        // Soft trigger — start idle timer.
        info!(
            conversation_id = bridge.conversation_id,
            usage_pct = usage.usage_pct,
            current_tokens = usage.current_tokens,
            soft_pct = config.soft_pct,
            soft_tokens = ?config.soft_tokens,
            idle_secs = config.idle_duration.as_secs(),
            trigger = kind.as_str(),
            "soft compaction trigger — starting idle timer"
        );
        state.trigger_usage_pct = usage.usage_pct;
        let weak_bridge = Arc::downgrade(bridge);
        let idle_duration = config.idle_duration;
        let timer = tokio::spawn(async move {
            tokio::time::sleep(idle_duration).await;
            let Some(bridge) = weak_bridge.upgrade() else {
                return; // Bridge is gone.
            };
            compaction_idle_timer_fired(&bridge).await;
        });
        state.idle_timer = Some(timer);
        state.phase = CompactionPhase::WaitingForIdle;
        return false; // CC is still idle — timer is just running.
    }

    if let Some(kind) = stage_fires(&usage, config.reminder_pct, config.reminder_tokens)
        && !state.reminder_sent
    {
        // LLM nudge — suggest compacting.
        state.reminder_sent = true;
        let pct = usage.usage_pct;
        drop(state);
        info!(
            conversation_id = bridge.conversation_id,
            usage_pct = pct,
            current_tokens = usage.current_tokens,
            trigger = kind.as_str(),
            "sending compaction reminder to LLM"
        );
        let rendered = crate::system_message::render_compaction_reminder(pct);
        if let Err(e) = bridge.send_system_message(rendered, None).await {
            error!("failed to send compaction reminder: {e}");
            return false;
        }
        return true; // CC is now working on the reminder response.
    }

    if stage_fires(&usage, config.reminder_pct, config.reminder_tokens).is_none()
        && state.reminder_sent
    {
        // Context dropped below reminder threshold — clear flag for next cycle.
        state.reminder_sent = false;
    }

    false
}

/// Callback for the soft-trigger idle timer.
///
/// Acquires the compaction lock, verifies we're still in WaitingForIdle,
/// checks session liveness, sends the persist message, and transitions
/// to PersistingState.
pub(super) async fn compaction_idle_timer_fired(bridge: &ActiveBridge) {
    let mut state = bridge.compaction.lock().await;
    if !matches!(state.phase, CompactionPhase::WaitingForIdle) {
        // Phase changed (user message, LLM-initiated compaction, session death).
        debug!("compaction idle timer fired but phase is {:?}", state.phase);
        return;
    }

    // Check session liveness.
    {
        let session = bridge.session.lock().await;
        if session.as_ref().is_none_or(|s| !s.is_alive()) {
            state.phase = CompactionPhase::Normal;
            state.idle_timer = None;
            debug!("compaction idle timer fired but session is dead");
            return;
        }
    }

    let usage_pct = state.trigger_usage_pct;
    state.phase = CompactionPhase::PersistingState;
    state.idle_timer = None;
    drop(state); // Release before async I/O — matches hard-trigger pattern.

    info!(
        conversation_id = bridge.conversation_id,
        usage_pct, "soft compaction idle timer fired — sending persist message"
    );

    let rendered = crate::system_message::render_compaction_idle_prompt(usage_pct);
    if let Err(e) = bridge.send_system_message(rendered, None).await {
        error!("failed to send soft-trigger persist message: {e}");
        bridge.compaction.lock().await.phase = CompactionPhase::Normal;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::super::ActiveBridges;
    use super::super::super::test_support::{
        await_fence, drain_broadcast, event_fence, set_context_usage, set_waiting_for_idle,
        test_bridge_singleton,
    };
    use super::super::context::broadcast_context_usage;
    use super::*;
    use brenn_lib::conversation;
    use brenn_lib::ws_types::WsServerMessage;

    use crate::active_bridge::compaction::tests::test_bridge_with_compaction_config;
    use crate::active_bridge::test_fixtures::TestBridgeConfig;
    use std::time::{Duration, Instant};
    use tokio::sync::{broadcast, mpsc};

    use super::super::super::cc_event_loop::cc_event_loop;
    use brenn_cc::session::SessionEvent;

    // -----------------------------------------------------------------------
    // Compaction trigger tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn send_message_cancels_waiting_for_idle() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        set_waiting_for_idle(&bridge).await;

        // send_message should cancel the timer and send normally.
        // It will fail (no CC session) but the phase should be reset.
        let _ = bridge.send_message("user message").await;

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "phase should be Normal after user message cancels WaitingForIdle"
        );
        assert!(state.idle_timer.is_none(), "idle_timer should be cleared");
    }

    #[tokio::test]
    async fn died_event_clears_idle_timer_and_reminder() {
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_singleton().await;
        {
            let mut state = bridge.compaction.lock().await;
            state.phase = CompactionPhase::WaitingForIdle;
            state.reminder_sent = true;
            state.idle_timer = Some(tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(9999)).await;
            }));
        }

        let fence = event_fence(&bridge);
        event_tx
            .send(SessionEvent::Died(brenn_cc::error::CcError::SendFailed))
            .await
            .unwrap();

        // Drain broadcasts from the Died event processing.
        await_fence(fence).await;
        let _ = drain_broadcast(&mut broadcast_rx);

        let state = bridge.compaction.lock().await;
        assert!(matches!(state.phase, CompactionPhase::Normal));
        assert!(!state.reminder_sent, "reminder_sent should be cleared");
        assert!(state.idle_timer.is_none(), "idle_timer should be cleared");
    }

    #[tokio::test]
    async fn evaluate_triggers_hard_trigger_sends_persist() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        set_context_usage(&bridge, 96); // Above hard_pct (95)

        // No CC session, so send_system_message will fail.
        // But the phase should transition before the error resets it.
        let sent = evaluate_compaction_triggers(&bridge).await;

        // send_system_message fails (no session) → phase reset to Normal.
        assert!(!sent, "should return false when send fails");
        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "phase should be Normal after send failure"
        );

        // evaluate_compaction_triggers is a pure evaluator and must NOT broadcast
        // ContextUsage — that broadcast belongs to handle_turn_completed.
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, WsServerMessage::ContextUsage { .. })),
            "evaluate_compaction_triggers must not broadcast ContextUsage"
        );
    }

    #[tokio::test]
    async fn evaluate_triggers_soft_trigger_starts_timer() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        set_context_usage(&bridge, 78); // Above soft_pct (75), below hard_pct (95)

        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent, "soft trigger starts timer, doesn't send message");

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::WaitingForIdle),
            "phase should be WaitingForIdle after soft trigger"
        );
        assert!(state.idle_timer.is_some(), "idle_timer should be set");
        assert_eq!(state.trigger_usage_pct, 78);

        // evaluate_compaction_triggers is a pure evaluator and must NOT broadcast
        // ContextUsage — that broadcast belongs to handle_turn_completed.
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, WsServerMessage::ContextUsage { .. })),
            "evaluate_compaction_triggers must not broadcast ContextUsage"
        );

        // Clean up: abort the timer so the test doesn't hang.
        drop(state);
        bridge
            .compaction
            .lock()
            .await
            .idle_timer
            .take()
            .unwrap()
            .abort();
    }

    #[tokio::test]
    async fn evaluate_triggers_soft_trigger_does_not_restart_timer() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;

        // Set WaitingForIdle with an existing timer.
        let timer = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(9999)).await;
        });
        {
            let mut state = bridge.compaction.lock().await;
            state.phase = CompactionPhase::WaitingForIdle;
            state.idle_timer = Some(timer);
        }

        set_context_usage(&bridge, 78); // Still above soft_pct

        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent, "should not restart timer");

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::WaitingForIdle),
            "should remain WaitingForIdle"
        );

        // Clean up.
        drop(state);
        bridge
            .compaction
            .lock()
            .await
            .idle_timer
            .take()
            .unwrap()
            .abort();
    }

    #[tokio::test]
    async fn evaluate_triggers_reminder_sent_once() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        set_context_usage(&bridge, 65); // Above reminder_pct (60), below soft_pct (75)

        // No CC session → send fails, but reminder_sent should be set.
        let _sent = evaluate_compaction_triggers(&bridge).await;
        let state = bridge.compaction.lock().await;
        assert!(state.reminder_sent, "reminder_sent should be set");
        drop(state);

        // Second call should not re-send (one-shot).
        let _ = drain_broadcast(&mut broadcast_rx);
        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent, "reminder should not re-send");
    }

    #[tokio::test]
    async fn evaluate_triggers_reminder_cleared_below_threshold() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        bridge.compaction.lock().await.reminder_sent = true;

        set_context_usage(&bridge, 50); // Below reminder_pct (60)

        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent);
        assert!(
            !bridge.compaction.lock().await.reminder_sent,
            "reminder_sent should be cleared when below threshold"
        );
    }

    #[tokio::test]
    async fn evaluate_triggers_skips_committed_phases() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        set_context_usage(&bridge, 96); // Would normally trigger hard

        // Set PersistingState — should skip evaluation.
        bridge.compaction.lock().await.phase = CompactionPhase::PersistingState;
        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent, "should skip when PersistingState");
        assert!(matches!(
            bridge.compaction.lock().await.phase,
            CompactionPhase::PersistingState
        ));

        // Same for Compacting.
        bridge.compaction.lock().await.phase = CompactionPhase::Compacting;
        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent, "should skip when Compacting");

        // Same for PendingTurnCompletion.
        bridge.compaction.lock().await.phase =
            CompactionPhase::PendingTurnCompletion { hints: None };
        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent, "should skip when PendingTurnCompletion");
    }

    #[tokio::test]
    async fn evaluate_triggers_hard_supersedes_soft_timer() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;

        // Set WaitingForIdle (soft timer running).
        {
            let mut state = bridge.compaction.lock().await;
            state.phase = CompactionPhase::WaitingForIdle;
            state.idle_timer = Some(tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(9999)).await;
            }));
        }

        set_context_usage(&bridge, 96); // Above hard_pct

        // Hard trigger should supersede the soft timer.
        // send_system_message will fail (no session) → reset to Normal.
        let _sent = evaluate_compaction_triggers(&bridge).await;

        let state = bridge.compaction.lock().await;
        // Phase is Normal because the send failed and reset it.
        assert!(matches!(state.phase, CompactionPhase::Normal));
        assert!(state.idle_timer.is_none(), "timer should be aborted");
    }

    #[tokio::test]
    async fn evaluate_triggers_context_drop_cancels_soft_timer() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;

        // Set WaitingForIdle.
        {
            let mut state = bridge.compaction.lock().await;
            state.phase = CompactionPhase::WaitingForIdle;
            state.idle_timer = Some(tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(9999)).await;
            }));
        }

        set_context_usage(&bridge, 50); // Dropped below soft_pct (75)

        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent);

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "should cancel timer and reset to Normal"
        );
        assert!(state.idle_timer.is_none());
    }

    #[tokio::test]
    async fn evaluate_triggers_no_config_returns_false() {
        // Bridge without compaction config (non-singleton).
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_singleton().await;
        // test_bridge_singleton has no compaction config.
        set_context_usage(&bridge, 96);

        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent, "should return false without compaction config");
    }

    /// Build a test bridge with configurable absolute token thresholds.
    /// Percentage thresholds are fixed at the standard test values
    /// (60/75/80/95).
    async fn test_bridge_with_token_thresholds(
        reminder_tokens: Option<u64>,
        soft_tokens: Option<u64>,
        red_tokens: Option<u64>,
        hard_tokens: Option<u64>,
    ) -> (
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
            reminder_tokens,
            soft_tokens,
            red_tokens,
            hard_tokens,
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

    /// Write a `ContextUsage` with explicit pct and token values, bypassing
    /// the usual pct-derived calculation. Lets tests construct states where
    /// pct and tokens disagree.
    fn set_context_usage_explicit(
        bridge: &ActiveBridge,
        usage_pct: u8,
        current_tokens: u64,
        max_tokens: u64,
    ) {
        *bridge.context_usage.lock().expect("lock") = Some(ContextUsage {
            current_tokens,
            max_tokens,
            usage_pct,
            checked_at: Instant::now(),
        });
    }

    // Note: a near-identical scoped-tracing-capture pattern exists at
    // `brenn-lib/src/pwa_push/publish.rs::install_scoped_cap_capture` —
    // different API shape (guard-struct there vs closure-wrapper here)
    // and different captured fields. If you find yourself about to add
    // a third one, talk to these first.
    /// Capture the value of the `trigger` field for any tracing event whose
    /// message is `target_message`, while `f` runs. Returns the captured
    /// trigger string (e.g. `"pct"` or `"tokens"`) or `None` if no matching
    /// event was emitted. Used by the cross-gate tests to verify that the
    /// `TriggerKind` label distinguishes pct-fires-first from
    /// tokens-fires-first.
    async fn capture_trigger_label<F, Fut, T>(target_message: &'static str, f: F) -> Option<String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        use std::sync::{Arc, Mutex};
        use tracing::field::{Field, Visit};
        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::SubscriberExt;

        struct TriggerVisitor {
            target_message: &'static str,
            message_matched: bool,
            trigger: Option<String>,
        }
        impl Visit for TriggerVisitor {
            fn record_str(&mut self, field: &Field, value: &str) {
                if field.name() == "message" && value == self.target_message {
                    self.message_matched = true;
                }
                if field.name() == "trigger" {
                    self.trigger = Some(value.to_string());
                }
            }
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                let formatted = format!("{value:?}");
                if field.name() == "message" {
                    let stripped = formatted.trim_matches('"');
                    if stripped == self.target_message {
                        self.message_matched = true;
                    }
                }
                if field.name() == "trigger" {
                    let stripped = formatted.trim_matches('"').to_string();
                    self.trigger = Some(stripped);
                }
            }
        }

        struct CaptureLayer {
            target_message: &'static str,
            captured: Arc<Mutex<Option<String>>>,
        }
        impl<S: tracing::Subscriber> Layer<S> for CaptureLayer {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let mut visitor = TriggerVisitor {
                    target_message: self.target_message,
                    message_matched: false,
                    trigger: None,
                };
                event.record(&mut visitor);
                if visitor.message_matched && visitor.trigger.is_some() {
                    *self.captured.lock().unwrap() = visitor.trigger;
                }
            }
        }

        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let layer = CaptureLayer {
            target_message,
            captured: captured.clone(),
        };
        let subscriber = tracing_subscriber::registry().with(layer);
        let dispatch = tracing::Dispatch::new(subscriber);
        // Set the dispatch as default for this async scope. Tracing's
        // `with_default` only covers the synchronous closure scope, but
        // `tracing::dispatcher::set_default` returns a guard that lasts
        // through awaits.
        let _guard = tracing::dispatcher::set_default(&dispatch);
        f().await;
        captured.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn evaluate_triggers_token_only_soft_starts_timer() {
        // soft_tokens = 200_000, soft_pct still defaulted to 75.
        // current_tokens = 200_001 with usage_pct = 20 (well below 75).
        // Expect: soft trigger fires via the tokens gate; WaitingForIdle.
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_token_thresholds(None, Some(200_000), None, None).await;

        set_context_usage_explicit(&bridge, 20, 200_001, 1_000_000);

        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent, "soft trigger starts timer; no message sent");

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::WaitingForIdle),
            "phase should be WaitingForIdle (token-only soft trigger)"
        );
        assert!(state.idle_timer.is_some());

        // Cleanup.
        drop(state);
        bridge
            .compaction
            .lock()
            .await
            .idle_timer
            .take()
            .unwrap()
            .abort();
    }

    #[tokio::test]
    async fn evaluate_triggers_pct_only_hard_supersedes_idle_timer() {
        // Regression guard for the stage_fires refactor: with no token
        // thresholds set and pct above hard_pct, the WaitingForIdle
        // supersede branch must run — aborting the soft-timer and
        // transitioning to Normal. A buggy `stage_fires` that returned
        // `None` on the pct path would skip the supersede, also skip the
        // "below soft" inverse (pct=96 is still above soft_pct=75), and
        // fall to the early `return false`, leaving the timer running and
        // the phase stuck at WaitingForIdle.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_compaction_config().await;
        {
            let mut state = bridge.compaction.lock().await;
            state.phase = CompactionPhase::WaitingForIdle;
            state.idle_timer = Some(tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(9999)).await;
            }));
        }
        set_context_usage(&bridge, 96); // Above hard_pct, no tokens config.

        let _sent = evaluate_compaction_triggers(&bridge).await;

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::Normal),
            "hard trigger should have superseded the idle timer (pct path)"
        );
        assert!(state.idle_timer.is_none(), "soft timer must be aborted");
    }

    #[tokio::test]
    async fn evaluate_triggers_both_configured_pct_fires_first() {
        // hard_pct = 95, hard_tokens = 200_000.
        // usage_pct = 95 (pct gate fires), current_tokens = 100_000 (under
        // tokens gate). Expect: hard trigger fires via pct gate, log
        // includes `trigger = "pct"`.
        let trigger_label = capture_trigger_label("hard compaction trigger", || async {
            let (bridge, _event_tx, _broadcast_rx, _ab) =
                test_bridge_with_token_thresholds(None, None, None, Some(200_000)).await;
            set_context_usage_explicit(&bridge, 95, 100_000, 1_000_000);
            let _sent = evaluate_compaction_triggers(&bridge).await;
            // Hard trigger fires; send fails (no session) → phase reset.
            let state = bridge.compaction.lock().await;
            assert!(
                matches!(state.phase, CompactionPhase::Normal),
                "hard trigger should have fired and then reset on send failure"
            );
        })
        .await;
        assert_eq!(
            trigger_label.as_deref(),
            Some("pct"),
            "trigger label must be `pct` when the percentage gate fires first"
        );
    }

    #[tokio::test]
    async fn evaluate_triggers_both_configured_tokens_fires_first() {
        // hard_pct = 95, hard_tokens = 200_000.
        // usage_pct = 50 (under pct gate), current_tokens = 250_000 (over
        // tokens gate). Expect: hard trigger fires via tokens gate, log
        // includes `trigger = "tokens"`.
        let trigger_label = capture_trigger_label("hard compaction trigger", || async {
            let (bridge, _event_tx, _broadcast_rx, _ab) =
                test_bridge_with_token_thresholds(None, None, None, Some(200_000)).await;
            set_context_usage_explicit(&bridge, 50, 250_000, 1_000_000);
            let _sent = evaluate_compaction_triggers(&bridge).await;
            let state = bridge.compaction.lock().await;
            assert!(
                matches!(state.phase, CompactionPhase::Normal),
                "hard trigger should have fired (tokens gate) and reset on send failure"
            );
        })
        .await;
        assert_eq!(
            trigger_label.as_deref(),
            Some("tokens"),
            "trigger label must be `tokens` when the tokens gate fires first"
        );
    }

    #[tokio::test]
    async fn evaluate_triggers_inverse_cancel_requires_both_below() {
        // In WaitingForIdle: usage_pct dropped below soft_pct (50 < 75) but
        // current_tokens still above soft_tokens (250_000 > 200_000).
        // The cancel path uses stage_fires(...).is_none(), so the timer
        // must NOT cancel — tokens still flagged.
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_token_thresholds(None, Some(200_000), None, None).await;
        {
            let mut state = bridge.compaction.lock().await;
            state.phase = CompactionPhase::WaitingForIdle;
            state.idle_timer = Some(tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(9999)).await;
            }));
        }

        set_context_usage_explicit(&bridge, 50, 250_000, 1_000_000);

        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent);

        let state = bridge.compaction.lock().await;
        assert!(
            matches!(state.phase, CompactionPhase::WaitingForIdle),
            "timer must NOT cancel while token gate still flags"
        );
        assert!(state.idle_timer.is_some());

        // Cleanup.
        drop(state);
        bridge
            .compaction
            .lock()
            .await
            .idle_timer
            .take()
            .unwrap()
            .abort();
    }

    #[tokio::test]
    async fn evaluate_triggers_reminder_clear_requires_both_below() {
        // Symmetric to `evaluate_triggers_inverse_cancel_requires_both_below`
        // but for the reminder gate. With reminder_tokens configured and
        // `reminder_sent = true`, dropping pct below `reminder_pct` while
        // tokens stays above `reminder_tokens` must NOT clear
        // `reminder_sent` — the tokens gate is still flagged.
        let (bridge, _event_tx, _broadcast_rx, _ab) =
            test_bridge_with_token_thresholds(Some(150_000), None, None, None).await;
        bridge.compaction.lock().await.reminder_sent = true;

        // pct=20 is below reminder_pct=60 (default test value);
        // tokens=160_000 is above reminder_tokens=150_000.
        set_context_usage_explicit(&bridge, 20, 160_000, 1_000_000);

        let sent = evaluate_compaction_triggers(&bridge).await;
        assert!(!sent);
        assert!(
            bridge.compaction.lock().await.reminder_sent,
            "reminder_sent must NOT clear while token gate still flags"
        );
    }

    #[tokio::test]
    async fn evaluate_triggers_broadcast_includes_token_thresholds() {
        // Confirm that broadcast_context_usage carries reminder_tokens and
        // red_tokens through to the wire schema.
        // Since evaluate_compaction_triggers is now a pure evaluator, we call
        // broadcast_context_usage directly.
        let (bridge, _event_tx, mut broadcast_rx, _ab) =
            test_bridge_with_token_thresholds(Some(150_000), None, Some(180_000), None).await;
        let usage = ContextUsage {
            current_tokens: 100_000,
            max_tokens: 1_000_000,
            usage_pct: 30,
            checked_at: Instant::now(),
        };
        broadcast_context_usage(&bridge, &usage);
        let msgs = drain_broadcast(&mut broadcast_rx);
        let found = msgs.iter().any(|m| {
            matches!(
                m,
                WsServerMessage::ContextUsage {
                    reminder_tokens: Some(150_000),
                    red_tokens: Some(180_000),
                    ..
                }
            )
        });
        assert!(found, "broadcast should include the token thresholds");
    }
}

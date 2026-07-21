use super::state::CompactionPhase;
use crate::active_bridge::ActiveBridge;

impl ActiveBridge {
    /// Check if a new compaction can be started. Returns true if the phase
    /// is Normal or WaitingForIdle (which would be cancelled by the compaction).
    pub async fn can_start_compaction(&self) -> bool {
        let state = self.compaction.lock().await;
        matches!(
            state.phase,
            CompactionPhase::Normal | CompactionPhase::WaitingForIdle
        )
    }

    /// Whether the LLM-initiated compaction state is currently `Normal`.
    /// Used by `run_idle_hooks` to gate fire-time evaluation.
    pub(crate) async fn compaction_phase_is_normal(&self) -> bool {
        let state = self.compaction.lock().await;
        matches!(state.phase, CompactionPhase::Normal)
    }

    /// Reset the compaction state machine to `Normal` and clear the boundary flag.
    ///
    /// Called when a `status: null` message arrives with any `compact_result` value,
    /// so the subsequent `TurnCompleted` proceeds normally through `set_idle_and_drain`
    /// and `evaluate_compaction_triggers` instead of staying stuck in `StayCompacting`.
    pub(in crate::active_bridge) async fn reset_compaction_state(&self) {
        let mut compaction = self.compaction.lock().await;
        compaction.phase = CompactionPhase::Normal;
        compaction.compact_boundary_seen = false;
        // Null context_usage so evaluate_compaction_triggers does not re-fire
        // the hard trigger against stale pre-compaction fill (mirrors the nulling
        // in handle_turn_completed's Compacting→Normal transition at compaction.rs:350).
        // Preserves lock order: compaction tokio-mutex (held) → context_usage std-mutex.
        *self.context_usage.lock().expect("context_usage lock") = None;
    }
}

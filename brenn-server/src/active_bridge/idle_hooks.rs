//! Idle-hook timer machinery: arming, cancellation, delay computation, and per-hook fan-out on fire.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tracing::debug;

use super::ActiveBridge;

impl ActiveBridge {
    /// Snapshot the registered hooks under the sync mutex, drop the
    /// guard, and return the cloned `Vec`. Callers iterate without
    /// holding the lock — never hold the sync mutex across an `.await`.
    pub(crate) fn snapshot_idle_hooks(&self) -> Vec<Arc<dyn crate::idle_hooks::IdleHook>> {
        self.idle_hooks
            .lock()
            .expect("idle_hooks lock poisoned")
            .clone()
    }

    /// Register an idle hook. Sole sanctioned mutator of `idle_hooks`.
    ///
    /// Panics at registration (startup time) if the hook declares a
    /// `min_idle_secs` below `idle_hook_secs` — that's a programming
    /// error (`min_idle_secs` must be `>=` the app default, which is
    /// the floor). When `idle_hook_secs == 0` (idle hooks disabled),
    /// the assertion is vacuously true and the hook is registered but
    /// never fired.
    pub(crate) fn register_idle_hook(&self, hook: Arc<dyn crate::idle_hooks::IdleHook>) {
        if let Some(min) = hook.min_idle_secs() {
            assert!(
                min >= self.idle_hook_secs,
                "hook {:?} declared min_idle_secs={} below app default {} \
                 (programming error — bridge default is the floor)",
                hook.name(),
                min,
                self.idle_hook_secs,
            );
        }
        self.idle_hooks
            .lock()
            .expect("idle_hooks lock poisoned")
            .push(hook);
    }

    /// Cancel any pending idle-hook timer. Safe to call when no timer
    /// is running.
    pub(crate) fn cancel_idle_hook_timer(&self) {
        let mut guard = self
            .idle_hook_timer
            .lock()
            .expect("idle_hook_timer lock poisoned");
        if let Some(handle) = guard.take() {
            handle.abort();
        }
    }

    /// Compute the timer delay: `max(idle_hook_secs, max_over_hooks(min_idle_secs))`.
    /// Hooks that report `None` for `min_idle_secs` participate as
    /// "use app default" — they're skipped in the inner max; the outer
    /// `max(app_default, ...)` covers the empty case. Returns `None`
    /// when `idle_hook_secs == 0` (idle hooks disabled).
    ///
    /// Takes a pre-snapshot of the hook list so callers that already
    /// snapshot for other reasons can avoid a second mutex acquisition.
    pub(crate) fn idle_hook_delay(
        &self,
        hooks: &[Arc<dyn crate::idle_hooks::IdleHook>],
    ) -> Option<Duration> {
        if self.idle_hook_secs == 0 {
            return None;
        }
        let inner_max = hooks
            .iter()
            .filter_map(|h| h.min_idle_secs())
            .max()
            .unwrap_or(0);
        let secs = self.idle_hook_secs.max(inner_max);
        Some(Duration::from_secs(secs))
    }

    /// Arm (or re-arm) the shared idle-hook timer if registered hooks
    /// have pending work. Returns immediately when:
    /// - `idle_hook_secs == 0` (idle hooks disabled), or
    /// - no hooks are registered, or
    /// - no registered hook reports `has_pending_work() == true`.
    ///
    /// Otherwise: aborts any existing timer and spawns a new one for
    /// the computed delay measured from now. See
    /// `docs/designs/idle-hooks.md` § "Flow".
    pub(crate) async fn maybe_arm_idle_hook_timer(self: &Arc<Self>) {
        // Snapshot once and reuse for the floor computation and the
        // pending-work check. `idle_hook_delay` returns `None` when
        // hooks are disabled (`idle_hook_secs == 0`).
        let hooks = self.snapshot_idle_hooks();
        if hooks.is_empty() {
            return;
        }
        let Some(delay) = self.idle_hook_delay(&hooks) else {
            return;
        };
        if !hooks.iter().any(|h| h.has_pending_work()) {
            return;
        }

        let conversation_id = self.conversation_id;
        let active_bridges = self.active_bridges.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let Some(bridge) = active_bridges.get(conversation_id).await else {
                return; // Bridge gone (CC died etc.) — nothing to do.
            };
            // Clear the slot *before* invoking hooks. The hook fan-out
            // ends with `send_system_message`, which calls
            // `set_cc_busy → cancel_idle_hook_timer`. If our own
            // `JoinHandle` were still in the slot, that cancel would
            // `abort()` this very task at the next `.await` and
            // `on_delivered` would never run. Clearing first means the
            // cancel finds an empty slot and does nothing. The timer is
            // conceptually done once it has fired.
            {
                let mut guard = bridge
                    .idle_hook_timer
                    .lock()
                    .expect("idle_hook_timer lock poisoned");
                // Don't `abort()` what we take — we are the running task.
                // Concurrent `maybe_arm_idle_hook_timer` may have replaced
                // us with a different handle already; in that case the
                // newer handle stays put.
                let taken = guard.take();
                if let Some(other) = taken
                    && other.id() != tokio::task::id()
                {
                    // A newer arm replaced us before we ran. Restore
                    // it so it is not orphaned.
                    *guard = Some(other);
                }
            }
            crate::idle_hooks::run_idle_hooks(&bridge).await;
        });

        let mut guard = self
            .idle_hook_timer
            .lock()
            .expect("idle_hook_timer lock poisoned");
        if let Some(old) = guard.take() {
            old.abort();
        }
        *guard = Some(handle);
    }

    /// UI-channel activity: cancel any pending idle-hook timer, and if
    /// CC is currently idle, re-arm it for `delay` from now. If CC is
    /// not idle, just cancel; the next `set_idle_and_drain` will arm
    /// fresh from that idle moment.
    pub(crate) async fn touch_ui_activity(self: &Arc<Self>, reason: &str) {
        debug!(
            conversation_id = self.conversation_id,
            reason, "touch_ui_activity"
        );
        if self.cc_idle.load(Ordering::SeqCst) {
            // `maybe_arm_idle_hook_timer` already cancels any existing
            // handle under the same lock acquisition — no need for a
            // separate cancel pass.
            self.maybe_arm_idle_hook_timer().await;
        } else {
            self.cancel_idle_hook_timer();
        }
    }

    /// Test-only: opaque id of the currently-armed idle-hook timer, or
    /// `None` if no timer is armed. Used by idle-hooks routing tests to
    /// detect cancel-and-rearm vs unchanged across a dispatch (a fresh
    /// arm produces a different `JoinHandle::id`).
    #[cfg(test)]
    pub(crate) fn idle_hook_timer_handle_id_for_test(&self) -> Option<tokio::task::Id> {
        self.idle_hook_timer
            .lock()
            .expect("idle_hook_timer lock poisoned")
            .as_ref()
            .map(|h| h.id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::idle_hooks::IdleHook;
    use brenn_lib::conversation;
    use brenn_lib::ws_types::WsServerMessage;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering as AtomicOrdering;
    use tokio::sync::broadcast;

    use super::super::CompactionPhase;
    use super::super::compaction::set_idle_and_drain;

    /// Test-only fake hook with cheap pending-work toggle, observable
    /// `check` / `on_delivered` / `on_resolved` counts, and a payload
    /// supplied as `Option<Value>`: `Some(...)` makes `check()` return
    /// `Some(payload)`; `None` makes it return `None` (resolved branch).
    struct FakeHook {
        name: &'static str,
        pending: AtomicBool,
        check_count: AtomicUsize,
        delivered_count: AtomicUsize,
        resolved_count: AtomicUsize,
        payload: Option<serde_json::Value>,
    }

    impl FakeHook {
        fn new(name: &'static str, payload: serde_json::Value) -> Arc<Self> {
            Arc::new(Self {
                name,
                pending: AtomicBool::new(true),
                check_count: AtomicUsize::new(0),
                delivered_count: AtomicUsize::new(0),
                resolved_count: AtomicUsize::new(0),
                payload: Some(payload),
            })
        }

        /// Hook whose `check()` returns `None` — exercises the
        /// `on_resolved` branch.
        fn new_resolved(name: &'static str) -> Arc<Self> {
            Arc::new(Self {
                name,
                pending: AtomicBool::new(true),
                check_count: AtomicUsize::new(0),
                delivered_count: AtomicUsize::new(0),
                resolved_count: AtomicUsize::new(0),
                payload: None,
            })
        }
    }

    impl IdleHook for FakeHook {
        fn name(&self) -> &str {
            self.name
        }

        fn check<'a>(
            &'a self,
            _bridge: &'a ActiveBridge,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Option<serde_json::Value>> + Send + 'a>,
        > {
            self.check_count.fetch_add(1, AtomicOrdering::SeqCst);
            let payload = self.payload.clone();
            Box::pin(async move { payload })
        }

        fn on_delivered(&self) {
            self.delivered_count.fetch_add(1, AtomicOrdering::SeqCst);
            // Mark as no-longer-pending so subsequent arms suppress.
            self.pending.store(false, AtomicOrdering::SeqCst);
        }

        fn on_resolved(&self) {
            self.resolved_count.fetch_add(1, AtomicOrdering::SeqCst);
        }

        fn has_pending_work(&self) -> bool {
            self.pending.load(AtomicOrdering::SeqCst)
        }
    }

    /// Hook with `min_idle_secs = Some(_)` for floor-assertion tests.
    struct MinIdleHook {
        min: u64,
    }

    impl IdleHook for MinIdleHook {
        fn name(&self) -> &str {
            "min_idle"
        }

        fn check<'a>(
            &'a self,
            _bridge: &'a ActiveBridge,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Option<serde_json::Value>> + Send + 'a>,
        > {
            Box::pin(async { None })
        }

        fn on_delivered(&self) {}
        fn on_resolved(&self) {}
        fn has_pending_work(&self) -> bool {
            true
        }
        fn min_idle_secs(&self) -> Option<u64> {
            Some(self.min)
        }
    }

    /// Hook that `pending = false` — used to test "no pending work" gate.
    struct InactiveHook;

    impl IdleHook for InactiveHook {
        fn name(&self) -> &str {
            "inactive"
        }

        fn check<'a>(
            &'a self,
            _bridge: &'a ActiveBridge,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Option<serde_json::Value>> + Send + 'a>,
        > {
            Box::pin(async { None })
        }

        fn on_delivered(&self) {}
        fn on_resolved(&self) {}
        fn has_pending_work(&self) -> bool {
            false
        }
    }

    async fn idle_hook_bridge(idle_hook_secs: u64) -> Arc<ActiveBridge> {
        let db = brenn_lib::db::init_db_memory();
        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "ihtest", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let (broadcast_tx, _rx) = broadcast::channel(64);
        ActiveBridge::inject_for_test_with_idle_hook_secs(
            user_id,
            conv_id,
            "test",
            db,
            broadcast_tx,
            idle_hook_secs,
            vec![],
        )
    }

    #[tokio::test]
    async fn arm_noop_when_idle_hook_secs_zero() {
        let bridge = idle_hook_bridge(0).await;
        let hook = FakeHook::new("zero", serde_json::json!({"x": 1}));
        bridge.register_idle_hook(hook);
        bridge.maybe_arm_idle_hook_timer().await;
        assert!(
            bridge.idle_hook_timer.lock().expect("lock").is_none(),
            "timer must not arm when idle_hook_secs == 0"
        );
    }

    #[tokio::test]
    async fn arm_noop_when_no_hooks() {
        let bridge = idle_hook_bridge(60).await;
        bridge.maybe_arm_idle_hook_timer().await;
        assert!(bridge.idle_hook_timer.lock().expect("lock").is_none());
    }

    #[tokio::test]
    async fn arm_noop_when_no_pending_work() {
        let bridge = idle_hook_bridge(60).await;
        bridge.register_idle_hook(Arc::new(InactiveHook));
        bridge.maybe_arm_idle_hook_timer().await;
        assert!(
            bridge.idle_hook_timer.lock().expect("lock").is_none(),
            "timer must not arm when no hook reports pending work"
        );
    }

    #[tokio::test]
    async fn arm_spawns_timer_when_pending_work() {
        let bridge = idle_hook_bridge(60).await;
        let hook = FakeHook::new("h", serde_json::json!({"x": 1}));
        bridge.register_idle_hook(hook);
        bridge.maybe_arm_idle_hook_timer().await;
        assert!(
            bridge.idle_hook_timer.lock().expect("lock").is_some(),
            "timer should arm when at least one hook has pending work"
        );
    }

    #[tokio::test]
    async fn set_cc_busy_cancels_timer() {
        let bridge = idle_hook_bridge(60).await;
        let hook = FakeHook::new("h", serde_json::json!({"x": 1}));
        bridge.register_idle_hook(hook);
        bridge.maybe_arm_idle_hook_timer().await;
        assert!(bridge.idle_hook_timer.lock().expect("lock").is_some());

        bridge.set_cc_busy("test");
        assert!(
            bridge.idle_hook_timer.lock().expect("lock").is_none(),
            "set_cc_busy must cancel any pending hook timer"
        );
    }

    #[tokio::test]
    async fn touch_ui_activity_cancels_and_rearms_when_idle() {
        let bridge = idle_hook_bridge(60).await;
        let hook = FakeHook::new("h", serde_json::json!({"x": 1}));
        bridge.register_idle_hook(hook);
        bridge.maybe_arm_idle_hook_timer().await;
        let original = {
            let g = bridge.idle_hook_timer.lock().expect("lock");
            g.as_ref().map(|h| h.id())
        };
        assert!(original.is_some());

        // CC idle → touch should cancel and re-arm.
        assert!(bridge.cc_idle.load(Ordering::SeqCst));
        bridge.touch_ui_activity("test").await;

        let after = {
            let g = bridge.idle_hook_timer.lock().expect("lock");
            g.as_ref().map(|h| h.id())
        };
        assert!(after.is_some(), "should re-arm when CC idle");
        assert_ne!(original, after, "should be a new handle (cancel-and-rearm)");
    }

    #[tokio::test]
    async fn touch_ui_activity_only_cancels_when_cc_busy() {
        let bridge = idle_hook_bridge(60).await;
        let hook = FakeHook::new("h", serde_json::json!({"x": 1}));
        bridge.register_idle_hook(hook);
        bridge.maybe_arm_idle_hook_timer().await;
        assert!(bridge.idle_hook_timer.lock().expect("lock").is_some());

        bridge.cc_idle.store(false, Ordering::SeqCst);
        bridge.touch_ui_activity("test").await;
        assert!(
            bridge.idle_hook_timer.lock().expect("lock").is_none(),
            "touch_ui_activity must NOT re-arm when CC is busy"
        );
    }

    #[tokio::test]
    async fn timer_fire_invokes_run_idle_hooks_and_delivers_message() {
        let db = brenn_lib::db::init_db_memory();
        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "fire", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "fire", false);
            (uid, cid)
        };
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel(64);
        // Use idle_hook_secs = 1 (the smallest non-zero u64 value); the
        // floor-assert allows it.
        let bridge = ActiveBridge::inject_for_test_with_idle_hook_secs(
            user_id,
            conv_id,
            "fire",
            db,
            broadcast_tx,
            1,
            vec![],
        );
        let active_bridges = bridge.active_bridges.clone();
        active_bridges.insert(conv_id, bridge.clone()).await;

        let hook = FakeHook::new(
            "dirty_repos",
            serde_json::json!({"by_slug": {"x": {"uncommitted": 1, "unpushed": 0}}}),
        );
        bridge.register_idle_hook(hook.clone());
        bridge.maybe_arm_idle_hook_timer().await;

        // Wait for timer + hook execution (1s timer + tiny work).
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // The hook should have been called.
        assert!(
            hook.check_count.load(AtomicOrdering::SeqCst) >= 1,
            "hook.check should have been invoked by the timer"
        );

        // The bridge should have broadcast a SystemMessageBroadcast for the
        // idle hook. (Note: in the test bridge the `session` is `None`, so
        // `send_system_message` errors after broadcasting — `on_delivered`
        // won't run, but the broadcast still proves `run_idle_hooks` reached
        // its delivery step with the right shape.)
        let mut found = false;
        while let Ok(msg) = broadcast_rx.try_recv() {
            if let WsServerMessage::SystemMessageBroadcast {
                rendered_html,
                category,
                ..
            } = msg
                && rendered_html.contains("brenn-system-idle-hook")
                && matches!(
                    category,
                    brenn_lib::ws_types::SystemMessageCategory::IdleHook
                )
            {
                found = true;
                break;
            }
        }
        assert!(
            found,
            "expected SystemMessageBroadcast for idle_hooks in broadcast"
        );
    }

    #[tokio::test]
    async fn timer_does_not_fire_during_compaction() {
        let bridge = idle_hook_bridge(1).await;
        let active_bridges = bridge.active_bridges.clone();
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;

        let hook = FakeHook::new("dirty_repos", serde_json::json!({"by_slug": {}}));
        bridge.register_idle_hook(hook.clone());

        // Force compaction phase out of Normal so run_idle_hooks bails.
        {
            let mut state = bridge.compaction.lock().await;
            state.phase = CompactionPhase::PersistingState;
        }

        bridge.maybe_arm_idle_hook_timer().await;
        tokio::time::sleep(Duration::from_millis(1500)).await;

        assert_eq!(
            hook.check_count.load(AtomicOrdering::SeqCst),
            0,
            "hook.check must NOT run while compaction is non-Normal"
        );
    }

    #[tokio::test]
    #[should_panic(expected = "below app default")]
    async fn register_panics_on_min_below_default() {
        let bridge = idle_hook_bridge(60).await;
        // min_idle_secs = 30 < idle_hook_secs = 60 → panic at registration.
        bridge.register_idle_hook(Arc::new(MinIdleHook { min: 30 }));
    }

    #[tokio::test]
    async fn register_ok_on_min_at_default() {
        let bridge = idle_hook_bridge(60).await;
        // min_idle_secs = 60 == app default → fine.
        bridge.register_idle_hook(Arc::new(MinIdleHook { min: 60 }));
    }

    #[tokio::test]
    async fn register_ok_on_min_above_default() {
        let bridge = idle_hook_bridge(60).await;
        // min_idle_secs = 90 > app default → fine.
        bridge.register_idle_hook(Arc::new(MinIdleHook { min: 90 }));
    }

    #[tokio::test]
    async fn register_permitted_when_disabled_zero_default() {
        // idle_hook_secs == 0 (disabled). Registration must still succeed
        // even though the hook will never fire. min_idle_secs assertion is
        // vacuously true since `>= 0` is always true.
        let bridge = idle_hook_bridge(0).await;
        bridge.register_idle_hook(Arc::new(MinIdleHook { min: 30 }));
        assert_eq!(bridge.snapshot_idle_hooks().len(), 1);
    }

    #[tokio::test]
    async fn shutdown_with_no_hooks_returns_immediately() {
        let bridge = idle_hook_bridge(60).await;
        // No hooks registered. Should be near-instant.
        let started = std::time::Instant::now();
        crate::idle_hooks::run_idle_hooks_for_shutdown(&bridge).await;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "shutdown with no hooks must return quickly"
        );
    }

    #[tokio::test]
    async fn shutdown_path_runs_hooks_when_drain_on_idle_set() {
        let bridge = idle_hook_bridge(60).await;
        let active_bridges = bridge.active_bridges.clone();
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;

        let hook = FakeHook::new(
            "dirty_repos",
            serde_json::json!({"by_slug": {"a": {"uncommitted": 1, "unpushed": 0}}}),
        );
        bridge.register_idle_hook(hook.clone());

        // Set drain_on_idle, but no subscribers (drain path entrance).
        bridge.drain_on_idle.store(true, Ordering::SeqCst);

        // Use set_idle_and_drain to trigger the shutdown hook path.
        set_idle_and_drain(&bridge).await;

        assert!(
            hook.check_count.load(AtomicOrdering::SeqCst) >= 1,
            "shutdown path should invoke hook.check"
        );
        // Note: `on_delivered` only runs when `send_system_message`
        // succeeds, which requires a live CC session. The test bridge has
        // `session: None`, so the send fails and `on_delivered` does not
        // run — that's the expected behavior on the documented "session
        // dead" branch (see `run_idle_hooks` step 5 in the design).
    }

    /// Hook whose `check()` sleeps for an hour. Used by the shutdown-bound
    /// test to verify the 60 s outer `tokio::time::timeout` in
    /// `run_idle_hooks_for_shutdown` actually trips.
    struct SleepyHook;

    impl IdleHook for SleepyHook {
        fn name(&self) -> &str {
            "sleepy"
        }
        fn check<'a>(
            &'a self,
            _bridge: &'a ActiveBridge,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Option<serde_json::Value>> + Send + 'a>,
        > {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                None
            })
        }
        fn on_delivered(&self) {}
        fn on_resolved(&self) {}
        fn has_pending_work(&self) -> bool {
            true
        }
    }

    /// `run_idle_hooks_for_shutdown` is bounded by an outer 60 s timeout
    /// (see `idle_hooks::SHUTDOWN_HOOK_RUN_TIMEOUT`). Drives the bound
    /// with `tokio::time::pause()`: a hook that would sleep for an hour
    /// must not block shutdown past the 60 s ceiling.
    #[tokio::test(start_paused = true)]
    async fn shutdown_bound_returns_within_outer_timeout_when_hook_hangs() {
        let bridge = idle_hook_bridge(60).await;
        let active_bridges = bridge.active_bridges.clone();
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;
        bridge.register_idle_hook(Arc::new(SleepyHook));

        // Spawn the shutdown path so we can advance the (paused) clock
        // around it.
        let bridge_clone = bridge.clone();
        let task = tokio::spawn(async move {
            crate::idle_hooks::run_idle_hooks_for_shutdown(&bridge_clone).await;
        });

        // Advance past the 60 s outer ceiling. The `+ 1 s` cushion crosses
        // the boundary regardless of timer rounding. The hook's inner
        // 3600 s sleep is also paused — it will not fire before the
        // outer timeout does, which is exactly the bound we're verifying.
        tokio::time::advance(Duration::from_secs(61)).await;

        // The spawned task should now be complete. Awaiting it directly
        // (no extra wall-clock guard needed — if the bound is broken,
        // we'd hang forever on the inner 3600 s sleep, which the test
        // harness will catch as a hang).
        task.await
            .expect("shutdown task must return after the 60 s outer bound elapses");
    }

    /// Hook that, during `check()`, calls `bridge.cancel_idle_hook_timer()`
    /// and then yields to give the runtime a chance to honour any abort.
    /// If `maybe_arm_idle_hook_timer`'s spawned task left its own
    /// `JoinHandle` in `idle_hook_timer`, that abort would target this
    /// running task and the post-yield assertion would never run.
    ///
    /// Used by the F1 regression test below.
    struct SelfCancelHook {
        post_yield_ran: Arc<AtomicBool>,
    }

    impl IdleHook for SelfCancelHook {
        fn name(&self) -> &str {
            "self_cancel"
        }
        fn check<'a>(
            &'a self,
            bridge: &'a ActiveBridge,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Option<serde_json::Value>> + Send + 'a>,
        > {
            let flag = self.post_yield_ran.clone();
            Box::pin(async move {
                // Synchronously schedule abort on whichever JoinHandle is
                // currently in `idle_hook_timer` (would-be the running
                // timer task itself, prior to F1 fix).
                bridge.cancel_idle_hook_timer();
                // Yield: any pending abort fires here.
                tokio::task::yield_now().await;
                // If we reach this line, we were not aborted by the
                // cancel above. That is the F1 invariant.
                flag.store(true, AtomicOrdering::SeqCst);
                None
            })
        }
        fn on_delivered(&self) {}
        fn on_resolved(&self) {}
        fn has_pending_work(&self) -> bool {
            true
        }
    }

    /// F1 regression: the timer-spawned task must not abort itself.
    ///
    /// Without the fix in `maybe_arm_idle_hook_timer`'s spawned closure
    /// (clear the slot before invoking hooks), `cancel_idle_hook_timer`
    /// fired during the hook fan-out would `abort()` the running task —
    /// killing it at the next `.await`. The asserted invariant: the
    /// hook's post-yield work completes.
    #[tokio::test]
    async fn timer_fired_task_is_not_aborted_by_in_flight_cancel() {
        let db = brenn_lib::db::init_db_memory();
        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "f1test", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "f1", false);
            (uid, cid)
        };
        let (broadcast_tx, _rx) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test_with_idle_hook_secs(
            user_id,
            conv_id,
            "f1",
            db,
            broadcast_tx,
            1, // 1-second timer
            vec![],
        );
        let active_bridges = bridge.active_bridges.clone();
        active_bridges.insert(conv_id, bridge.clone()).await;

        let post_yield_ran = Arc::new(AtomicBool::new(false));
        bridge.register_idle_hook(Arc::new(SelfCancelHook {
            post_yield_ran: post_yield_ran.clone(),
        }));
        bridge.maybe_arm_idle_hook_timer().await;

        // Wait for the timer to fire and the hook to run through.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        assert!(
            post_yield_ran.load(AtomicOrdering::SeqCst),
            "timer-spawned task must survive an in-flight `cancel_idle_hook_timer` \
             — without the F1 fix the running task would have been `abort()`ed \
             at the yield point and never set this flag"
        );
        // The slot should be empty: the spawned task cleared it before
        // running, and `cancel_idle_hook_timer` was a no-op against an
        // already-empty slot. (A concurrent re-arm could repopulate it,
        // but no such call happens in this test.)
        assert!(
            bridge.idle_hook_timer_handle_id_for_test().is_none(),
            "timer slot should be empty after the spawned task ran"
        );
    }

    // -----------------------------------------------------------------------
    // F2 regression: `idle_hook_delay` floor computation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn idle_hook_delay_returns_max_of_default_and_min_idle_secs() {
        let bridge = idle_hook_bridge(60).await;
        bridge.register_idle_hook(Arc::new(MinIdleHook { min: 7200 }));
        let hooks = bridge.snapshot_idle_hooks();
        let delay = bridge
            .idle_hook_delay(&hooks)
            .expect("idle_hook_delay should be Some when idle_hook_secs > 0");
        assert_eq!(
            delay,
            Duration::from_secs(7200),
            "delay must be max(idle_hook_secs={}, min_idle_secs={})",
            60,
            7200,
        );
    }

    #[tokio::test]
    async fn idle_hook_delay_uses_default_when_no_min_idle_hooks() {
        let bridge = idle_hook_bridge(120).await;
        bridge.register_idle_hook(FakeHook::new("plain", serde_json::json!({})));
        let hooks = bridge.snapshot_idle_hooks();
        let delay = bridge.idle_hook_delay(&hooks).expect("Some");
        assert_eq!(
            delay,
            Duration::from_secs(120),
            "no min_idle_secs hook → delay = idle_hook_secs"
        );
    }

    #[tokio::test]
    async fn idle_hook_delay_takes_max_across_multiple_hooks() {
        let bridge = idle_hook_bridge(60).await;
        bridge.register_idle_hook(Arc::new(MinIdleHook { min: 600 }));
        bridge.register_idle_hook(Arc::new(MinIdleHook { min: 7200 }));
        bridge.register_idle_hook(Arc::new(MinIdleHook { min: 120 }));
        let hooks = bridge.snapshot_idle_hooks();
        let delay = bridge.idle_hook_delay(&hooks).expect("Some");
        assert_eq!(
            delay,
            Duration::from_secs(7200),
            "delay must be max across all min_idle_secs"
        );
    }

    #[tokio::test]
    async fn idle_hook_delay_none_when_disabled() {
        let bridge = idle_hook_bridge(0).await;
        bridge.register_idle_hook(Arc::new(MinIdleHook { min: 7200 }));
        let hooks = bridge.snapshot_idle_hooks();
        assert!(
            bridge.idle_hook_delay(&hooks).is_none(),
            "idle_hook_secs == 0 must yield None regardless of registered hooks"
        );
    }

    // -----------------------------------------------------------------------
    // F3 regression: multi-hook aggregation + on_delivered / on_resolved
    // lifecycle. Closes test F4 and quality F8 (FakeHook counters) too.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn multi_hook_aggregation_and_lifecycle_callbacks() {
        let db = brenn_lib::db::init_db_memory();
        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "multi", "$argon2id$fake");
            let cid = conversation::create_conversation(&conn, uid, "multi", false);
            (uid, cid)
        };
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test_with_idle_hook_secs(
            user_id,
            conv_id,
            "multi",
            db,
            broadcast_tx,
            60,
            vec![],
        );
        let active_bridges = bridge.active_bridges.clone();
        active_bridges.insert(conv_id, bridge.clone()).await;

        // hook_a returns Some — must contribute to the envelope.
        let hook_a = FakeHook::new("hook_a", serde_json::json!({"x": 1}));
        // hook_b returns None — must trigger on_resolved, must NOT
        // appear in the envelope.
        let hook_b = FakeHook::new_resolved("hook_b");
        bridge.register_idle_hook(hook_a.clone());
        bridge.register_idle_hook(hook_b.clone());

        // Drive the timer-fired path directly (no real sleep needed).
        crate::idle_hooks::run_idle_hooks(&bridge).await;

        // Both hooks were checked.
        assert_eq!(hook_a.check_count.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(hook_b.check_count.load(AtomicOrdering::SeqCst), 1);

        // hook_a: Some → on_delivered called (only when send succeeds —
        // the test bridge has `session: None`, so the send fails and
        // `on_delivered` is NOT called for the broadcast-but-failed-send
        // case. The contributor list still excludes hook_b regardless.)
        // hook_b: None → on_resolved called.
        assert_eq!(
            hook_a.delivered_count.load(AtomicOrdering::SeqCst),
            0,
            "test bridge has no live session — send fails before on_delivered"
        );
        assert_eq!(hook_a.resolved_count.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(hook_b.delivered_count.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(
            hook_b.resolved_count.load(AtomicOrdering::SeqCst),
            1,
            "hook_b returned None → on_resolved must fire exactly once"
        );

        // The broadcast must include hook_a's payload but NOT hook_b's
        // key. Now arrives as SystemMessageBroadcast with rendered_html.
        // The rendered HTML for unknown hooks includes the hook key via
        // the fallback renderer's <details> block; hook_a uses the unknown-
        // key fallback (it's not "dirty_repos") and renders its content.
        // The DB row also carries the LLM-facing text for verification.
        let mut found_html = None;
        while let Ok(msg) = broadcast_rx.try_recv() {
            if let WsServerMessage::SystemMessageBroadcast { rendered_html, .. } = msg {
                found_html = Some(rendered_html);
            }
        }
        let html = found_html.expect("expected a SystemMessageBroadcast on the broadcast");
        assert!(
            html.contains("hook_a"),
            "hook_a payload must appear in rendered_html; got: {html}"
        );
        assert!(
            !html.contains("hook_b"),
            "hook_b returned None — must not appear in rendered_html; got: {html}"
        );
        // Verify the LLM text via the DB row: should be the
        // LLM text is wrapped in `<brenn-system-reminder>`, with the inner body
        // being `{"system":"idle_hooks", "hook_a": {...}}` JSON.
        let conn = bridge.db.lock().await;
        let messages = brenn_lib::conversation::get_messages(&conn, bridge.conversation_id);
        let row = messages
            .last()
            .expect("DB must have the persisted system row");
        let payload: serde_json::Value =
            serde_json::from_str(&row.payload).expect("DB payload must be valid JSON");
        let text = payload["message"]["content"]
            .as_str()
            .expect("DB payload must carry the LLM text");
        assert!(
            text.starts_with("<brenn-system-reminder>\n"),
            "LLM text must be wrapped in <brenn-system-reminder>: {text}"
        );
        // Extract the JSON body from between the outer tags.
        let json_body = text
            .strip_prefix("<brenn-system-reminder>\n")
            .and_then(|s| s.strip_suffix("\n</brenn-system-reminder>"))
            .expect("LLM text must have matching open/close brenn-system-reminder tags");
        let parsed: serde_json::Value =
            serde_json::from_str(json_body).expect("inner body must be valid JSON");
        assert_eq!(parsed["system"].as_str(), Some("idle_hooks"));
        assert_eq!(parsed["hook_a"]["x"].as_u64(), Some(1));
        assert!(
            parsed.get("hook_b").is_none(),
            "hook_b returned None — must not appear in LLM text; got: {json_body}"
        );
    }
}

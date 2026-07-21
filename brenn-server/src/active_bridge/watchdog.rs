//! Bridge-wedge watchdog.
//!
//! A single process-wide task sweeps the live-bridge registry on an interval,
//! looking for a bridge that can no longer make progress while the app still
//! believes CC is busy — the "wedge" a fail-stopped event loop leaves behind. A
//! wedge is otherwise silent past the initial panic page: `cc_idle` never
//! clears (UI spins forever), the CC container is orphaned, and every later user
//! message persists then fails on a closed stdin. The watchdog pages `Critical`,
//! runs the clean-slate death reset, reaps the orphaned session, and deregisters
//! the bridge so a later user message spawns a fresh one.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brenn_lib::config::WatchdogConfig;
use brenn_lib::obs::alerting::{AlertDispatcher, AlertSeverity};
use tracing::{error, info};

use super::ActiveBridge;
use super::cc_event_loop::{ShutdownReason, reset_dead_session};
use super::registry::ActiveBridges;

/// Which wedge predicate fired, for the alert body and logs.
#[derive(Clone, Copy)]
enum WedgePredicate {
    /// The stored event-loop `JoinHandle` reports the loop has finished — the
    /// incident signature. Deterministic; fires on the first sweep.
    DeadEventLoop,
    /// `cc_idle == false` but the session cannot make progress (absent, dead, or
    /// its I/O tasks have exited). Grace-gated to let an in-flight `Died` win.
    DeadIoBusy,
}

impl std::fmt::Display for WedgePredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WedgePredicate::DeadEventLoop => write!(f, "dead event loop"),
            WedgePredicate::DeadIoBusy => write!(f, "dead session I/O while bridge busy"),
        }
    }
}

/// Sweeps the bridge registry for wedged bridges and self-heals them.
pub(in crate::active_bridge) struct Watchdog {
    config: WatchdogConfig,
    active_bridges: ActiveBridges,
    alert_dispatcher: AlertDispatcher,
    /// Consecutive sweeps each bridge has looked wedged under the grace-gated
    /// predicate, keyed by bridge identity (`Arc::as_ptr`) rather than
    /// conversation_id so a replacement bridge for the same conversation cannot
    /// inherit a count. Rebuilt each sweep so recovered or vanished bridges drop
    /// out.
    wedge_counts: HashMap<usize, u32>,
}

impl Watchdog {
    pub(in crate::active_bridge) fn new(
        config: WatchdogConfig,
        active_bridges: ActiveBridges,
        alert_dispatcher: AlertDispatcher,
    ) -> Self {
        Self {
            config,
            active_bridges,
            alert_dispatcher,
            wedge_counts: HashMap::new(),
        }
    }

    /// One pass over the registry.
    pub(in crate::active_bridge) async fn sweep(&mut self) {
        let grace_sweeps = self.config.grace_sweeps();
        let mut next_counts: HashMap<usize, u32> = HashMap::new();

        for bridge in self.active_bridges.all().await {
            // An already-handled death, or a process-wide server shutdown, is
            // owned by the normal teardown paths — never a wedge.
            if bridge.died_handled() {
                continue;
            }
            let reason = ShutdownReason::from_bridge(&bridge);
            if reason.is_server_shutdown() {
                continue;
            }

            // Predicate 1: a dead event loop is deterministic and immediate. A
            // deferred idle-drain runs *from* the event loop, so it can never
            // complete once the loop is dead — a drain flag must not suppress
            // this, or a bridge that got flagged for drain after its loop died
            // would wedge silently forever.
            if bridge.event_loop_finished() {
                self.handle_wedge(&bridge, WedgePredicate::DeadEventLoop)
                    .await;
                continue;
            }

            // A live event loop will still carry a deferred drain to completion,
            // so an intentional drain does suppress the busy-I/O predicate below.
            if reason.is_intentional() {
                continue;
            }

            // Predicate 2: busy bridge with dead session I/O, sustained across
            // the grace window (avoids racing an in-flight Died that would clean
            // up on its own). Keyed by bridge identity so a replacement for the
            // same conversation starts its own grace. `count > grace_sweeps`
            // (not `>=`): N observations span (N-1) sweep intervals, so firing at
            // `grace_sweeps + 1` observations means at least `wedge_grace_secs` of
            // wall-clock has actually elapsed.
            if is_dead_io_busy(&bridge).await {
                let key = Arc::as_ptr(&bridge) as usize;
                let count = self.wedge_counts.get(&key).copied().unwrap_or(0) + 1;
                if count > grace_sweeps {
                    self.handle_wedge(&bridge, WedgePredicate::DeadIoBusy).await;
                } else {
                    next_counts.insert(key, count);
                }
            }
        }

        self.wedge_counts = next_counts;
    }

    /// Page, run the clean-slate reset, reap the session, and deregister.
    async fn handle_wedge(&self, bridge: &ActiveBridge, predicate: WedgePredicate) {
        let cid = bridge.conversation_id;
        error!(
            conversation_id = cid,
            app_slug = %bridge.app_slug,
            predicate = %predicate,
            "watchdog detected wedged bridge — resetting and reaping"
        );
        self.alert_dispatcher.alert(
            AlertSeverity::Critical,
            "Bridge wedged".to_string(),
            format!(
                "conversation {cid} ({}) wedged: {predicate}",
                bridge.app_slug
            ),
        );

        // Clean-slate reset: runtime state + mark Error + error broadcasts +
        // died_handled (so a further sweep before deregistration is a no-op).
        reset_dead_session(
            bridge,
            format!("bridge wedged ({predicate}); reset by watchdog"),
        )
        .await;

        // Reap the orphaned session: mark it shutting down first so a live
        // reader task's EOF branch does not fire its own Critical for a kill the
        // watchdog is performing and already paged, then drop the CcSession —
        // kill_on_drop kills the child, reaping the container.
        {
            let mut guard = bridge.session.lock().await;
            if let Some(ref s) = *guard {
                s.mark_shutting_down();
            }
            let taken = guard.take();
            drop(guard);
            drop(taken);
        }

        // Deregister so each wedge fires exactly once and the next user message
        // spawns a fresh bridge. No auto-respawn: recovery is on user demand.
        // Identity-checked so a replacement bridge registered under the same
        // conversation_id between the reset and here is not clobbered.
        self.active_bridges
            .remove_if_same(cid, std::ptr::from_ref(bridge) as usize)
            .await;
        info!(conversation_id = cid, "watchdog reaped wedged bridge");
    }
}

/// Whether a bridge is busy (`cc_idle == false`) but its session cannot make
/// progress: no session, session not alive, or its I/O tasks have exited.
async fn is_dead_io_busy(bridge: &ActiveBridge) -> bool {
    if bridge.cc_idle.load(Ordering::SeqCst) {
        return false;
    }
    let session = bridge.session.lock().await;
    match &*session {
        None => true,
        Some(s) => !s.is_alive() || !s.io_alive(),
    }
}

/// Spawn the process-wide watchdog task. Death of this task is accepted like the
/// other process-lifetime loops (panics are logged + `Critical`-alerted by the
/// global panic hook); it is not itself supervised.
pub(crate) fn spawn_watchdog(
    config: WatchdogConfig,
    active_bridges: ActiveBridges,
    alert_dispatcher: AlertDispatcher,
) {
    let mut watchdog = Watchdog::new(config, active_bridges, alert_dispatcher);
    let interval = Duration::from_secs(watchdog.config.sweep_interval_secs);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // First tick fires immediately; sweep tolerates an empty registry.
        loop {
            ticker.tick().await;
            watchdog.sweep().await;
        }
    });
}

#[cfg(test)]
mod tests;

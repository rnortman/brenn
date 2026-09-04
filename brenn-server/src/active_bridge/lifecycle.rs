//! Bridge lifecycle: subscribers (presence), drain-on-idle, persistent-app idle TTL, kill_session, and cc_idle/cc_busy state transitions.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use brenn_db::conversation::{self, ConversationStatus};
use brenn_ws_types::{CcState, PresenceUser, WsServerMessage};
use tracing::{debug, info, warn};

use super::ActiveBridge;
use super::lifetime::Verdict;
use super::registry::ActiveBridges;

/// Info about a user currently subscribed to a bridge (for presence).
/// `count` tracks the number of WS connections from this user (multi-tab).
pub(super) struct Subscriber {
    pub(super) username: String,
    pub(super) count: usize,
}

impl ActiveBridge {
    /// Register a user as present on this bridge. Ref-counted for multi-tab.
    /// Returns the current presence list (for sending to the newly attached connection).
    /// Broadcasts `PresenceUpdate` to all subscribers when a user first appears.
    /// Clears `drain_on_idle` and cancels any armed lifetime timer.
    pub async fn add_subscriber(&self, user_id: i64, username: &str) -> Vec<PresenceUser> {
        let mut subs = self.subscribers.write().await;

        // Cancel any pending drain — a user reconnected. Must happen under the
        // subscribers lock to maintain the invariant: drain_on_idle is only
        // mutated while holding this lock.
        //
        // Note: if drain_on_idle was true and kill_session is already in
        // progress, this subscriber will attach to a dying bridge. The
        // subscriber detects this via BroadcastResult::Closed and recovers
        // by spawning a new bridge. This is a known benign race.
        self.drain_on_idle.store(false, Ordering::SeqCst);

        {
            let mut handle = self
                .lifetime_timer
                .lock()
                .expect("lifetime_timer lock poisoned");
            if let Some(h) = handle.take() {
                h.abort();
            }
        }

        debug!(
            conversation_id = self.conversation_id,
            cc_idle = self.cc_idle.load(Ordering::SeqCst),
            drain_on_idle = self.drain_on_idle.load(Ordering::SeqCst),
            "subscriber attached"
        );

        let entry = subs.entry(user_id).or_insert(Subscriber {
            username: username.to_string(),
            count: 0,
        });
        let was_zero = entry.count == 0;
        entry.count += 1;

        let presence_list: Vec<PresenceUser> = subs
            .values()
            .map(|s| PresenceUser {
                username: s.username.clone(),
            })
            .collect();

        // Broadcast only when a user first appears (0→1).
        if was_zero {
            let msg = WsServerMessage::PresenceUpdate {
                conversation_id: self.conversation_id,
                users: presence_list.clone(),
            };
            drop(subs); // Release lock before broadcast.
            self.broadcast(msg);
        }

        presence_list
    }

    /// Unregister a user connection from this bridge. Ref-counted for multi-tab.
    /// Broadcasts `PresenceUpdate` when a user fully disappears (count reaches 0).
    ///
    /// When all subscribers leave, the browser door lets go and the lifetime
    /// question is re-asked: another door may still be holding the bridge, the
    /// browser's own post-detach grace may still be running, or nothing may want
    /// it and the drain begins.
    pub async fn remove_subscriber(self: &Arc<Self>, user_id: i64) {
        let mut subs = self.subscribers.write().await;
        let should_broadcast = if let Some(entry) = subs.get_mut(&user_id) {
            assert!(
                entry.count > 0,
                "subscriber count underflow for user {user_id}"
            );
            entry.count -= 1;
            if entry.count == 0 {
                subs.remove(&user_id);
                true
            } else {
                false
            }
        } else {
            warn!(user_id, "remove_subscriber called for unknown user");
            false
        };

        let all_gone = subs.is_empty();

        if should_broadcast {
            let presence_list: Vec<PresenceUser> = subs
                .values()
                .map(|s| PresenceUser {
                    username: s.username.clone(),
                })
                .collect();
            let msg = WsServerMessage::PresenceUpdate {
                conversation_id: self.conversation_id,
                users: presence_list,
            };
            drop(subs);
            self.broadcast(msg);
        } else {
            drop(subs);
        }

        // Note: `all_gone` was captured under the lock but we're acting on it
        // after the lock was released. A subscriber could have arrived in
        // between. This is safe: the drain path converges on `maybe_drain`,
        // which re-acquires the lock and re-asks before condemning the bridge.
        // At worst we start a timer that fires and no-ops.
        if all_gone {
            self.lifetime.note_detach();
            self.reconsider_lifetime().await;
        }
    }

    /// Ask every door whether it still wants this bridge, and act on the answer:
    /// drain now, or wait until the soonest hold expires and ask again.
    ///
    /// The single place a lifetime verdict becomes an action. Every trigger — the
    /// last tab closing, the timer firing, a bus peer speaking — routes through
    /// here, so which door moved never changes how the OR is applied.
    pub(in crate::active_bridge) async fn reconsider_lifetime(self: &Arc<Self>) {
        // Scoped: `maybe_drain` re-asks under the write lock, which this read
        // guard would deadlock against if it were still alive.
        let subscribers = { self.subscribers.read().await.len() };
        match self.lifetime.verdict(subscribers) {
            Verdict::Drain => self.maybe_drain().await,
            Verdict::KeepAlive {
                recheck: Some(after),
            } => {
                // A bus-driven conversation reaches this arm on every exchange,
                // so at info it is one line per utterance for the life of the
                // conversation. The verdict already debug-logs holders and
                // recheck.
                debug!(
                    conversation_id = self.conversation_id,
                    recheck_secs = after.as_secs(),
                    "bridge still held open; re-asking when the soonest hold expires"
                );
                self.start_lifetime_timer(after);
            }
            // Nothing expires by waiting — an attached tab has to detach before
            // the answer can change, and detaching re-asks.
            Verdict::KeepAlive { recheck: None } => {}
        }
    }

    /// A bus peer drove this conversation: the bus door takes hold for its idle
    /// window.
    ///
    /// A drain flagged while the bus was quiet is cancelled here, exactly as a
    /// reconnecting browser cancels one, and under the same lock — writing
    /// `drain_on_idle` only while holding the subscribers lock is what keeps the
    /// flag and the verdict that set it one step.
    pub(crate) async fn note_bus_activity(self: &Arc<Self>) {
        self.lifetime.note_bus_activity();
        {
            let _subs = self.subscribers.write().await;
            self.drain_on_idle.store(false, Ordering::SeqCst);
        }
        self.reconsider_lifetime().await;
    }

    /// Attempt to drain (kill) CC. Called when no door holds the bridge.
    ///
    /// Acquires the subscribers write lock and re-asks the doors + sets
    /// `drain_on_idle` atomically. This prevents races where a subscriber
    /// connects between the caller's check and the drain decision.
    ///
    /// In the "CC idle, all subscribers gone" branch, runs idle hooks
    /// before killing — see `docs/designs/idle-hooks.md` § "Bridge
    /// shutdown sequence". If a hook delivers a message, CC becomes busy
    /// again and the kill defers to the next turn end.
    ///
    /// Called from `remove_subscriber` (subscriber-leave path) and from
    /// the persistent-app idle timer. The turn-end path uses
    /// `drain_no_hooks` instead — it already ran hooks in
    /// `set_idle_and_drain` and would otherwise pay for the same
    /// `git status` fan-out twice.
    async fn maybe_drain(self: &Arc<Self>) {
        {
            let subs = self.subscribers.write().await;
            if let Verdict::KeepAlive { .. } = self.lifetime.verdict(subs.len()) {
                // A door took hold between the caller's check and now.
                return;
            }
            self.drain_on_idle.store(true, Ordering::SeqCst);
            // Lock released here.
        }

        info!(
            conversation_id = self.conversation_id,
            "nothing holds this bridge open, marked drain_on_idle"
        );

        // If CC is idle, try to kill now. Re-check drain_on_idle first.
        if self.cc_idle.load(Ordering::SeqCst) {
            // Re-read drain_on_idle: a subscriber may have arrived and
            // cleared it between steps 3 and 5. Not fully race-free (see
            // design doc), but narrows the window dramatically.
            if !self.drain_on_idle.load(Ordering::SeqCst) {
                info!(
                    conversation_id = self.conversation_id,
                    "drain_on_idle cleared by concurrent subscriber, aborting drain"
                );
                return;
            }

            // Last chance: let hooks fire before we kill CC. Same bounded
            // path as `set_idle_and_drain`.
            crate::idle_hooks::run_idle_hooks_for_shutdown(self).await;
            if !self.cc_idle.load(Ordering::SeqCst) {
                // Hooks delivered a message; CC is now busy. Let
                // `set_idle_and_drain` handle the kill at the next turn
                // end. `drain_on_idle` is still set.
                info!(
                    conversation_id = self.conversation_id,
                    "idle-hook delivered a message, deferring kill to next turn end"
                );
                return;
            }

            info!(
                conversation_id = self.conversation_id,
                "CC idle at drain time, killing session"
            );
            self.complete_and_kill().await;
        } else {
            // CC is mid-turn. Leave it alone — CC may be doing real work
            // (background coding task, long tool call) and we want it to
            // finish. `handle_turn_completed` will check `drain_on_idle`
            // when CC eventually goes idle and shut things down then.
            info!(
                conversation_id = self.conversation_id,
                "CC mid-turn at drain time, deferring kill until CC idles"
            );
        }
    }

    /// Turn-end drain helper: kill CC immediately, *without* re-running
    /// idle hooks. The caller (`set_idle_and_drain`) already ran them
    /// once on the same idle moment; running them again would duplicate
    /// the `git status` fan-out on the common clean-shutdown path.
    ///
    /// Must only be called when `drain_on_idle` is already set and we
    /// are at a CC idle boundary. Re-asks the doors under the subscribers
    /// lock and re-checks `drain_on_idle` to narrow the known race with a
    /// concurrent subscriber.
    pub(super) async fn drain_no_hooks(self: &Arc<Self>) {
        {
            let subs = self.subscribers.write().await;
            if let Verdict::KeepAlive { .. } = self.lifetime.verdict(subs.len()) {
                return;
            }
        }
        if !self.drain_on_idle.load(Ordering::SeqCst) {
            info!(
                conversation_id = self.conversation_id,
                "drain_on_idle cleared by concurrent subscriber, aborting drain"
            );
            return;
        }
        if !self.cc_idle.load(Ordering::SeqCst) {
            // A hook (or any other path) flipped CC busy after our
            // caller's check. Defer to the next turn end.
            return;
        }
        info!(
            conversation_id = self.conversation_id,
            "CC idle at turn end drain, killing session"
        );
        self.complete_and_kill().await;
    }

    /// Mark the conversation completed in the DB (if Active) and kill the CC session.
    ///
    /// Idempotent — no-ops if conversation is not Active. Safe to call from any teardown path.
    pub(super) async fn complete_and_kill(self: &Arc<Self>) {
        let conn = self.db.lock().await;
        let conv = conversation::get_conversation_opt(&conn, self.conversation_id);
        if let Some(conv) = conv {
            if conv.status == ConversationStatus::Active {
                conversation::complete_conversation(
                    &conn,
                    self.conversation_id,
                    conv.total_cost_usd,
                );
            }
        } else {
            warn!(
                conversation_id = self.conversation_id,
                "conversation row missing at complete_and_kill — possible double-teardown or DB corruption"
            );
        }
        drop(conn);
        self.kill_session(&self.active_bridges).await;
    }

    /// Start (or replace) the timer that re-asks the lifetime question when the
    /// soonest current hold is due to expire.
    ///
    /// The timer task looks up the bridge from the `ActiveBridges` registry
    /// when it fires (we can't capture `Arc<Self>` here since we only have
    /// `&self`). If the bridge has been removed (CC died), the lookup returns
    /// `None` and the timer is a no-op. It re-asks rather than draining outright,
    /// because the hold that expired need not be the only one — a bus
    /// interaction during the browser's grace re-arms this timer for its own
    /// window instead.
    fn start_lifetime_timer(&self, timeout: Duration) {
        let conversation_id = self.conversation_id;
        let active_bridges = self.active_bridges.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(timeout).await;

            // Look up the bridge from the registry. If it's been removed
            // (e.g., CC already died), there's nothing to drain.
            let bridge = active_bridges.get(conversation_id).await;
            if let Some(bridge) = bridge {
                info!(conversation_id, "lifetime timer fired, re-asking the doors");
                bridge.forget_lifetime_timer();
                bridge.reconsider_lifetime().await;
            }
        });

        let mut guard = self
            .lifetime_timer
            .lock()
            .expect("lifetime_timer lock poisoned");
        // Abort any existing timer before replacing.
        if let Some(old) = guard.take() {
            old.abort();
        }
        *guard = Some(handle);
    }

    /// Drop the stored timer handle without aborting it.
    ///
    /// For the timer task's own use, once it has fired: `start_lifetime_timer`
    /// aborts whatever handle it finds, and the re-ask a fired timer performs can
    /// arm a fresh one — which would otherwise abort the very task doing the
    /// asking, at its next await.
    fn forget_lifetime_timer(&self) {
        drop(
            self.lifetime_timer
                .lock()
                .expect("lifetime_timer lock poisoned")
                .take(),
        );
    }

    /// Whether CC is alive but idle (between turns, waiting for input).
    pub fn is_cc_idle(&self) -> bool {
        self.cc_idle.load(Ordering::SeqCst)
    }

    /// Snapshot the CC state for an attach-site `ConversationSwitched` message.
    ///
    /// Priority: pending permissions → active turn → idle.
    pub(crate) async fn resolve_cc_state(&self) -> CcState {
        if self.has_pending_permissions().await {
            CcState::AwaitingApproval
        } else if self.is_alive().await && !self.is_cc_idle() {
            CcState::Thinking
        } else {
            CcState::Idle
        }
    }

    /// Mark CC as busy (not idle). Broadcasts `Status { Thinking }` on the
    /// Idle → busy transition; subsequent calls while already busy are no-ops
    /// for the UI and skip the broadcast.
    ///
    /// Single source of truth for the Idle → Thinking state change. Higher-
    /// priority states (`AwaitingApproval`, `Compacting`) are broadcast from
    /// their own code paths and overwrite the Thinking state in the UI.
    pub(super) fn set_cc_busy(&self, reason: &str) {
        let was_idle = self.cc_idle.swap(false, Ordering::SeqCst);
        debug!(
            conversation_id = self.conversation_id,
            reason, was_idle, "cc_idle → false"
        );
        if was_idle {
            self.broadcast(WsServerMessage::Status {
                state: CcState::Thinking,
            });
        }
        // CC just started doing something. Cancel any pending idle-hook
        // timer; the next turn end (`set_idle_and_drain`) will arm a fresh
        // one with `delay` measured from that moment. See
        // `docs/designs/idle-hooks.md` § "Choice: one timer, two activity
        // sources, arm-from-now".
        self.cancel_idle_hook_timer();
    }

    /// Kill the CC subprocess and remove this bridge from the registry.
    ///
    /// Drops the `CcSession` first (SIGKILL via `kill_on_drop`, and for a
    /// containerized app a blocking `podman rm` of the container), and only then
    /// removes this bridge from `ActiveBridges`. That order is load-bearing:
    /// container names are stable per conversation, so a spawn that starts as
    /// soon as the registry slot frees would have its brand-new container
    /// force-removed by this session's still-pending teardown. Deregistering
    /// last means any container holding the name after the slot frees is this
    /// conversation's next one, not a corpse. The event loop's cleanup path is
    /// idempotent — removing an already-removed key is a no-op.
    ///
    /// When called from cc_event_loop teardown the CC process has already exited.
    /// Tokio's `kill_on_drop` drop impl calls `libc::kill(pid, SIGKILL)`, which
    /// returns `ESRCH` for a dead/reaped process — the kernel silently ignores
    /// it, and tokio's drop discards the error. No guard needed.
    pub async fn kill_session(&self, active_bridges: &ActiveBridges) {
        // Take the session out and release the guard *before* dropping it. The
        // drop tears down the container and blocks until podman has done so;
        // holding the lock across it would stall every other task that touches
        // this bridge's session behind a container teardown.
        //
        // `profile_swap::swap_session` deliberately does the opposite, and the
        // two must not be "fixed" to match: there the stall is the point, so a
        // send issued mid-swap parks on the mutex and lands on the replacement
        // process instead of finding a gap.
        let taken = {
            let mut session = self.session.lock().await;
            if let Some(ref s) = *session {
                s.mark_shutting_down();
            }
            session.take()
        };
        drop(taken);
        // This bridge is done regardless of whether a replacement holds the slot,
        // and its adapter is the one thing that would otherwise outlive it.
        self.stop_chat_adapter();
        // Identity-checked: if a replacement bridge has already taken this
        // conversation_id slot (this bridge was deregistered by the watchdog and
        // the user reconnected), do not remove the replacement.
        active_bridges
            .remove_if_same(self.conversation_id, std::ptr::from_ref(self) as usize)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::init_db_memory;
    use brenn_cc::protocol::incoming::ResultMessage;
    use brenn_cc::session::SessionEvent;
    use tokio::sync::broadcast;

    use super::super::test_fixtures::TestBridgeConfig;
    use super::super::test_support::{
        await_fence, drain_broadcast, event_fence, install_failing_session, recv_broadcast,
        test_bridge, test_bridge_with_config,
    };
    use brenn_obs::alerting::noop_alert_dispatcher;
    use tokio::sync::mpsc;

    /// Helper: create a test bridge with persistent mode and a given idle timeout.
    ///
    /// **Note:** The returned `ActiveBridges` does NOT contain the bridge.
    /// Callers that exercise registry-dependent paths (e.g., `maybe_drain`,
    /// `remove_subscriber` timer tests) must call
    /// `active_bridges.insert(bridge.conversation_id, bridge.clone()).await`
    /// immediately after destructuring. Callers that only test subscriber or
    /// flag state and never query the registry may safely omit the insert.
    async fn test_bridge_persistent(
        idle_timeout: Duration,
    ) -> (
        Arc<ActiveBridge>,
        mpsc::Sender<brenn_cc::session::SessionEvent>,
        broadcast::Receiver<WsServerMessage>,
        ActiveBridges,
    ) {
        let (alert_dispatcher, _handle) = noop_alert_dispatcher();
        test_bridge_with_config(
            TestBridgeConfig {
                idle_timeout: Some(idle_timeout),
                ..Default::default()
            },
            alert_dispatcher,
        )
        .await
    }

    /// Invariant: a freshly constructed bridge reports `is_cc_idle() == true`.
    /// The CC handshake (`control_response`) has completed by the time the
    /// bridge exists, so the subprocess is idle and waiting for input.
    /// Connecting clients depend on this — they compute their initial state as
    /// `is_alive() && !is_cc_idle()` (see `routes/ws.rs`
    /// `handle_switch_conversation`).
    #[tokio::test]
    async fn fresh_bridge_reports_cc_idle_true() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;
        assert!(
            bridge.is_cc_idle(),
            "fresh bridge must report idle (handshake completed, no turn in progress)"
        );

        // Same property for singleton bridges.
        let (sbridge, _, _, _) = super::super::test_support::test_bridge_singleton().await;
        assert!(
            sbridge.is_cc_idle(),
            "fresh singleton bridge must also report idle"
        );
    }

    /// `set_cc_busy` broadcasts `Status(Thinking)` on the Idle → busy
    /// transition, and only on that transition (subsequent calls while
    /// already busy are no-ops for the UI).
    #[tokio::test]
    async fn set_cc_busy_broadcasts_thinking() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge().await;
        assert!(bridge.is_cc_idle());

        bridge.set_cc_busy("first");
        assert!(!bridge.is_cc_idle());
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::Status { state } => assert_eq!(*state, CcState::Thinking),
            other => panic!("expected Status(Thinking), got {other:?}"),
        }

        // Second call while already busy: no broadcast. set_cc_busy only broadcasts
        // on idle→busy transition; a second call while busy takes no action and returns
        // without crossing an async boundary, so the decision is final before this line.
        bridge.set_cc_busy("second");
        let extra = drain_broadcast(&mut broadcast_rx);
        assert!(
            extra.is_empty(),
            "set_cc_busy while already busy must not re-broadcast, got: {extra:?}"
        );
    }

    #[tokio::test]
    async fn kill_session_drops_and_removes_from_registry() {
        let db = crate::test_support::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (broadcast_tx, _rx) = broadcast::channel(64);

        let user_id = {
            let conn = db.lock().await;
            brenn_db::auth::user::create_user(&conn, "testuser", "$argon2id$fake")
        };
        let conv_id = {
            let conn = db.lock().await;
            conversation::create_conversation(&conn, user_id, "test", false)
        };

        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        active_bridges.insert(conv_id, bridge.clone()).await;

        // Bridge is in registry.
        assert!(active_bridges.get(conv_id).await.is_some());

        // Kill it.
        bridge.kill_session(&active_bridges).await;

        // Bridge is removed from registry.
        assert!(active_bridges.get(conv_id).await.is_none());
        // Session is gone (inject_for_test has None session, but kill_session shouldn't panic).
        assert!(!bridge.is_alive().await);
    }

    /// Build a bridge with an installed session whose container teardown blocks
    /// until `release` exists, plus a `started` marker the test can wait on.
    async fn bridge_with_blocking_teardown(
        dir: &std::path::Path,
        active_bridges: &ActiveBridges,
    ) -> Arc<ActiveBridge> {
        use std::os::unix::fs::PermissionsExt;

        let started = dir.join("rm-started");
        let release = dir.join("rm-release");
        let script = dir.join("blocking-podman");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\n\
                 touch '{}'\n\
                 while [ ! -e '{}' ]; do sleep 0.01; done\n",
                started.display(),
                release.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        let db = crate::test_support::init_db_memory();
        let (broadcast_tx, _rx) = broadcast::channel(64);
        let user_id = {
            let conn = db.lock().await;
            brenn_db::auth::user::create_user(&conn, "testuser", "$argon2id$fake")
        };
        let conv_id = {
            let conn = db.lock().await;
            conversation::create_conversation(&conn, user_id, "test", false)
        };
        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        *bridge.session.lock().await =
            Some(brenn_cc::session::CcSession::dummy_with_container_for_test(
                &script.display().to_string(),
                "brenn-test-conv1",
            ));
        active_bridges.insert(conv_id, bridge.clone()).await;
        bridge
    }

    async fn wait_for_file(path: &std::path::Path) {
        for _ in 0..500 {
            if path.exists() {
                return;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
        panic!("{} never appeared", path.display());
    }

    /// A containerized session's drop blocks on `podman rm`. That must happen
    /// with the bridge's session mutex released — otherwise every task touching
    /// this bridge queues behind a container teardown, unbounded if podman is
    /// itself wedged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn kill_session_drops_the_session_outside_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let active_bridges = ActiveBridges::new();
        let bridge = bridge_with_blocking_teardown(dir.path(), &active_bridges).await;

        let killer = {
            let bridge = bridge.clone();
            let active_bridges = active_bridges.clone();
            tokio::spawn(async move { bridge.kill_session(&active_bridges).await })
        };

        wait_for_file(&dir.path().join("rm-started")).await;
        assert!(
            bridge.session.try_lock().is_ok(),
            "the session mutex must be free while the container teardown blocks"
        );

        std::fs::write(dir.path().join("rm-release"), b"go").unwrap();
        killer.await.unwrap();
    }

    /// Container names are stable per conversation, so a teardown still running
    /// `podman rm` when the registry slot frees would remove the *next* session's
    /// container. Deregistration must therefore come after the drop completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn kill_session_deregisters_only_after_the_container_is_gone() {
        let dir = tempfile::tempdir().unwrap();
        let active_bridges = ActiveBridges::new();
        let bridge = bridge_with_blocking_teardown(dir.path(), &active_bridges).await;
        let conv_id = bridge.conversation_id;

        let killer = {
            let bridge = bridge.clone();
            let active_bridges = active_bridges.clone();
            tokio::spawn(async move { bridge.kill_session(&active_bridges).await })
        };

        wait_for_file(&dir.path().join("rm-started")).await;
        assert!(
            active_bridges.get(conv_id).await.is_some(),
            "the slot must stay claimed until the old container is actually gone"
        );

        std::fs::write(dir.path().join("rm-release"), b"go").unwrap();
        killer.await.unwrap();
        assert!(active_bridges.get(conv_id).await.is_none());
    }

    #[tokio::test]
    async fn kill_session_idempotent() {
        let db = crate::test_support::init_db_memory();
        let active_bridges = ActiveBridges::new();
        let (broadcast_tx, _rx) = broadcast::channel(64);

        let user_id = {
            let conn = db.lock().await;
            brenn_db::auth::user::create_user(&conn, "testuser", "$argon2id$fake")
        };
        let conv_id = {
            let conn = db.lock().await;
            conversation::create_conversation(&conn, user_id, "test", false)
        };

        let bridge =
            ActiveBridge::inject_for_test(user_id, conv_id, "test", db.clone(), broadcast_tx);
        active_bridges.insert(conv_id, bridge.clone()).await;

        // Kill twice — should not panic.
        bridge.kill_session(&active_bridges).await;
        bridge.kill_session(&active_bridges).await;

        assert!(active_bridges.get(conv_id).await.is_none());
    }

    // -----------------------------------------------------------------------
    // Presence subscriber tracking
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_subscriber_returns_presence_list() {
        let db = init_db_memory();
        let (broadcast_tx, _) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test(1, 1, "test", db, broadcast_tx);

        let list = bridge.add_subscriber(1, "alice").await;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].username, "alice");
    }

    #[tokio::test]
    async fn add_subscriber_multi_tab_same_user() {
        let db = init_db_memory();
        let (broadcast_tx, _) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test(1, 1, "test", db, broadcast_tx);

        // First tab.
        let list = bridge.add_subscriber(1, "alice").await;
        assert_eq!(list.len(), 1);

        // Second tab — same user, still one entry.
        let list = bridge.add_subscriber(1, "alice").await;
        assert_eq!(list.len(), 1);
    }

    #[tokio::test]
    async fn add_subscriber_multiple_users() {
        let db = init_db_memory();
        let (broadcast_tx, _) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test(1, 1, "test", db, broadcast_tx);

        bridge.add_subscriber(1, "alice").await;
        let list = bridge.add_subscriber(2, "bob").await;
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn remove_subscriber_decrements_ref_count() {
        let db = init_db_memory();
        let (broadcast_tx, mut rx) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test(1, 1, "test", db, broadcast_tx);

        // Two tabs for alice.
        bridge.add_subscriber(1, "alice").await;
        bridge.add_subscriber(1, "alice").await;

        // Drain the PresenceUpdate from first add (was_zero).
        let _ = rx.try_recv();

        // Remove one tab — alice still present (count 2→1), no broadcast.
        bridge.remove_subscriber(1).await;
        assert!(
            rx.try_recv().is_err(),
            "no broadcast expected when count > 0"
        );

        // Remove second tab — alice gone (count 1→0), broadcast fires.
        bridge.remove_subscriber(1).await;
        let msg = rx
            .try_recv()
            .expect("should broadcast when user disappears");
        match msg {
            WsServerMessage::PresenceUpdate { users, .. } => {
                assert!(users.is_empty(), "alice should be gone");
            }
            _ => panic!("expected PresenceUpdate"),
        }
    }

    #[tokio::test]
    async fn add_subscriber_broadcasts_on_first_appearance() {
        let db = init_db_memory();
        let (broadcast_tx, mut rx) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test(1, 1, "test", db, broadcast_tx);

        bridge.add_subscriber(1, "alice").await;

        // First appearance (was_zero) should broadcast.
        let msg = rx.try_recv().expect("should broadcast on first appearance");
        match msg {
            WsServerMessage::PresenceUpdate { users, .. } => {
                assert_eq!(users.len(), 1);
                assert_eq!(users[0].username, "alice");
            }
            _ => panic!("expected PresenceUpdate"),
        }

        // Second tab — no broadcast (count 1→2).
        bridge.add_subscriber(1, "alice").await;
        assert!(
            rx.try_recv().is_err(),
            "no broadcast expected for multi-tab same user"
        );
    }

    #[tokio::test]
    async fn remove_unknown_subscriber_does_not_panic() {
        let db = init_db_memory();
        let (broadcast_tx, _) = broadcast::channel(64);
        let bridge = ActiveBridge::inject_for_test(1, 1, "test", db, broadcast_tx);

        // Removing a user who was never added should not panic (logs a warning).
        bridge.remove_subscriber(999).await;
    }

    #[tokio::test]
    async fn drain_on_idle_kills_when_cc_idle_at_detach() {
        let (bridge, event_tx, mut broadcast_rx, active_bridges) = test_bridge().await;
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;

        // Add a subscriber, then send TurnCompleted to mark CC as idle.
        bridge.add_subscriber(1, "alice").await;
        let result = ResultMessage {
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
            extra: serde_json::Value::Object(Default::default()),
        };
        event_tx
            .send(SessionEvent::TurnCompleted(result))
            .await
            .unwrap();

        // Drain the Idle broadcast.
        let _ = recv_broadcast(&mut broadcast_rx).await;

        // CC is now idle. Remove the subscriber → should trigger immediate kill.
        // remove_subscriber drives maybe_drain → complete_and_kill → kill_session
        // synchronously (all awaited in-process, not dispatched through cc_event_loop).
        // All postconditions are visible immediately on return; no fence needed.
        bridge.remove_subscriber(1).await;

        assert!(
            bridge.drain_on_idle.load(Ordering::SeqCst),
            "drain_on_idle should be set"
        );

        // Bridge should be removed from registry.
        assert!(
            active_bridges.get(bridge.conversation_id).await.is_none(),
            "bridge should be removed after drain kill"
        );

        // Conversation should be Completed, not Error.
        let conn = bridge.db.lock().await;
        let conv = conversation::get_conversation(&conn, bridge.conversation_id);
        assert_eq!(
            conv.status,
            ConversationStatus::Completed,
            "drain kill should complete the conversation, not error it"
        );
    }

    #[tokio::test]
    async fn drain_on_idle_waits_for_turn_completion() {
        let (bridge, event_tx, mut broadcast_rx, active_bridges) = test_bridge().await;
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;

        // Add a subscriber, then mark CC busy to simulate a turn in progress.
        // (After spawn cc_idle is true; flipping it here mimics what
        // set_cc_busy would do once a user message had been sent.)
        bridge.add_subscriber(1, "alice").await;
        bridge.cc_idle.store(false, Ordering::SeqCst);

        // Remove subscriber while CC is "working" (cc_idle = false).
        bridge.remove_subscriber(1).await;

        assert!(
            bridge.drain_on_idle.load(Ordering::SeqCst),
            "drain_on_idle should be set"
        );
        // Bridge should still be in registry — CC hasn't finished its turn.
        assert!(
            active_bridges.get(bridge.conversation_id).await.is_some(),
            "bridge should still exist while CC is working"
        );

        // Now CC finishes its turn → handle_turn_completed checks drain.
        let result = ResultMessage {
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
            extra: serde_json::Value::Object(Default::default()),
        };
        let fence = event_fence(&bridge);
        event_tx
            .send(SessionEvent::TurnCompleted(result))
            .await
            .unwrap();

        // Drain the Idle broadcast.
        let _ = recv_broadcast(&mut broadcast_rx).await;

        // Await the event loop epoch after TurnCompleted to ensure kill_session ran.
        await_fence(fence).await;

        // Now the bridge should be gone.
        assert!(
            active_bridges.get(bridge.conversation_id).await.is_none(),
            "bridge should be removed after turn completes with drain set"
        );

        // Conversation should be Completed, not Error.
        let conn = bridge.db.lock().await;
        let conv = conversation::get_conversation(&conn, bridge.conversation_id);
        assert_eq!(
            conv.status,
            ConversationStatus::Completed,
            "drain kill should complete the conversation, not error it"
        );
    }

    #[tokio::test]
    async fn reconnect_cancels_drain() {
        let (bridge, _event_tx, _broadcast_rx, active_bridges) = test_bridge().await;
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;

        // Mark CC busy so the drain is deferred (waits for turn completion)
        // rather than firing the immediate-kill path.
        bridge.cc_idle.store(false, Ordering::SeqCst);

        // Add then remove subscriber to trigger drain.
        bridge.add_subscriber(1, "alice").await;
        bridge.remove_subscriber(1).await;
        assert!(bridge.drain_on_idle.load(Ordering::SeqCst));

        // Reconnect before the drain fires.
        bridge.add_subscriber(2, "bob").await;
        assert!(
            !bridge.drain_on_idle.load(Ordering::SeqCst),
            "drain_on_idle should be cleared by add_subscriber"
        );

        // Bridge should still be in registry.
        assert!(
            active_bridges.get(bridge.conversation_id).await.is_some(),
            "bridge should survive after reconnect cancels drain"
        );
    }

    // -----------------------------------------------------------------------
    // Persistent / idle timer / maybe_drain tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn maybe_drain_kills_when_empty_and_idle() {
        let (bridge, _event_tx, _rx, active_bridges) =
            test_bridge_persistent(Duration::from_secs(60)).await;
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;
        let conv_id = bridge.conversation_id;

        // No subscribers, CC is idle.
        bridge.cc_idle.store(true, Ordering::SeqCst);

        bridge.maybe_drain().await;

        // drain_on_idle should be set.
        assert!(bridge.drain_on_idle.load(Ordering::SeqCst));
        // Bridge should be removed from the registry (kill_session was called).
        assert!(
            active_bridges.get(conv_id).await.is_none(),
            "bridge should be removed from registry after drain"
        );
    }

    #[tokio::test]
    async fn maybe_drain_defers_when_cc_mid_turn() {
        let (bridge, _event_tx, _rx, active_bridges) =
            test_bridge_persistent(Duration::from_secs(60)).await;
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;
        let conv_id = bridge.conversation_id;

        // No subscribers, CC is NOT idle (mid-turn).
        bridge.cc_idle.store(false, Ordering::SeqCst);

        bridge.maybe_drain().await;

        // drain_on_idle should be set (deferred kill).
        assert!(bridge.drain_on_idle.load(Ordering::SeqCst));
        // But bridge should still be alive — kill deferred to turn completion.
        assert!(
            active_bridges.get(conv_id).await.is_some(),
            "bridge should still be in registry (kill deferred)"
        );
    }

    #[tokio::test]
    async fn maybe_drain_noop_when_subscribers_present() {
        let (bridge, _event_tx, _rx, _ab) = test_bridge_persistent(Duration::from_secs(60)).await;

        bridge.add_subscriber(bridge.user_id, "testuser").await;
        bridge.cc_idle.store(true, Ordering::SeqCst);

        bridge.maybe_drain().await;

        assert!(!bridge.drain_on_idle.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn persistent_remove_subscriber_starts_timer_not_drain() {
        let (bridge, _event_tx, _rx, active_bridges) =
            test_bridge_persistent(Duration::from_secs(300)).await;
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;
        let conv_id = bridge.conversation_id;

        bridge.add_subscriber(bridge.user_id, "testuser").await;
        bridge.cc_idle.store(true, Ordering::SeqCst);

        bridge.remove_subscriber(bridge.user_id).await;

        // Persistent app: drain_on_idle should NOT be set immediately.
        assert!(!bridge.drain_on_idle.load(Ordering::SeqCst));
        // Timer should be running.
        {
            let guard = bridge.lifetime_timer.lock().expect("lock");
            assert!(guard.is_some(), "lifetime timer should be armed");
        }
        // Bridge should still be alive.
        assert!(
            active_bridges.get(conv_id).await.is_some(),
            "bridge should still be in registry"
        );
    }

    #[tokio::test]
    async fn add_subscriber_cancels_idle_timer() {
        let (bridge, _event_tx, _rx, _ab) = test_bridge_persistent(Duration::from_secs(300)).await;

        // Add and remove to start the timer.
        bridge.add_subscriber(bridge.user_id, "testuser").await;
        bridge.remove_subscriber(bridge.user_id).await;

        // Timer should be running.
        {
            let guard = bridge.lifetime_timer.lock().expect("lock");
            assert!(guard.is_some());
        }

        // Reconnect — timer should be cancelled.
        bridge.add_subscriber(bridge.user_id, "testuser").await;

        let guard = bridge.lifetime_timer.lock().expect("lock");
        assert!(
            guard.is_none(),
            "lifetime timer should be cancelled on reconnect"
        );
    }

    #[tokio::test]
    async fn idle_timer_fires_and_drains() {
        tokio::time::pause();
        let timeout = Duration::from_millis(50);
        let (bridge, _event_tx, _rx, active_bridges) = test_bridge_persistent(timeout).await;
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;
        let conv_id = bridge.conversation_id;

        bridge.add_subscriber(bridge.user_id, "testuser").await;
        bridge.cc_idle.store(true, Ordering::SeqCst);
        bridge.remove_subscriber(bridge.user_id).await;

        // Bridge should still be alive — timer hasn't fired.
        assert!(active_bridges.get(conv_id).await.is_some());
        assert!(!bridge.drain_on_idle.load(Ordering::SeqCst));

        // Yield once so the spawned timer task runs its first poll and registers
        // its sleep with the paused time driver. Without this yield, advance()
        // does not see the sleep as a pending timer and cannot expire it.
        tokio::task::yield_now().await;

        // Advance time past the timer duration so the timer fires. Then poll in a
        // bounded loop to allow the timer task to run maybe_drain and
        // complete_and_kill to completion. A single yield_now is insufficient if
        // any lock in the chain is contested — each .await in the timer task may
        // need a scheduling round.
        tokio::time::advance(Duration::from_millis(200)).await;
        let mut rounds = 0u32;
        while active_bridges.get(conv_id).await.is_some() {
            assert!(
                rounds < 100,
                "bridge not removed from registry after 100 yield rounds post-advance"
            );
            tokio::task::yield_now().await;
            rounds += 1;
        }

        // Timer should have fired, calling maybe_drain which kills the bridge.
        assert!(bridge.drain_on_idle.load(Ordering::SeqCst));
        assert!(
            active_bridges.get(conv_id).await.is_none(),
            "bridge should be removed from registry after idle timeout"
        );
    }

    #[tokio::test]
    async fn idle_timer_cancelled_by_reconnect_before_firing() {
        tokio::time::pause();
        let timeout = Duration::from_millis(500);
        let (bridge, _event_tx, _rx, active_bridges) = test_bridge_persistent(timeout).await;
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;
        let conv_id = bridge.conversation_id;

        bridge.add_subscriber(bridge.user_id, "testuser").await;
        bridge.cc_idle.store(true, Ordering::SeqCst);
        bridge.remove_subscriber(bridge.user_id).await;

        // Reconnect immediately — cancels the timer.
        bridge.add_subscriber(bridge.user_id, "testuser").await;

        // Advance past the original timeout — timer was cancelled, no drain.
        tokio::time::advance(Duration::from_millis(700)).await;

        assert!(!bridge.drain_on_idle.load(Ordering::SeqCst));
        assert!(
            active_bridges.get(conv_id).await.is_some(),
            "bridge should survive — timer was cancelled by reconnect"
        );
    }

    #[tokio::test]
    async fn maybe_drain_deferred_does_not_kill_busy_cc() {
        // Regression: a disconnected-but-busy CC must NOT be killed. Previously
        // a 120s drain deadline timer would force-kill the session even if it
        // was actively working. Now CC is allowed to run to completion of
        // whatever it's doing; only when it next goes idle does drain_on_idle
        // take effect.
        tokio::time::pause();
        let (bridge, _event_tx, _rx, active_bridges) =
            test_bridge_persistent(Duration::from_secs(60)).await;
        active_bridges
            .insert(bridge.conversation_id, bridge.clone())
            .await;
        let conv_id = bridge.conversation_id;

        // CC is NOT idle — deferred path.
        bridge.cc_idle.store(false, Ordering::SeqCst);

        bridge.maybe_drain().await;

        // drain_on_idle is set so the eventual idle transition will kill it.
        assert!(bridge.drain_on_idle.load(Ordering::SeqCst));

        // But the bridge stays alive — no kill timer.
        assert!(
            active_bridges.get(conv_id).await.is_some(),
            "busy bridge must not be killed when subscribers leave"
        );

        // Advance time to confirm no background timer fires to kill the bridge.
        tokio::time::advance(Duration::from_millis(300)).await;

        assert!(
            active_bridges.get(conv_id).await.is_some(),
            "busy bridge must keep running indefinitely while CC is mid-turn"
        );
        assert!(
            bridge.drain_on_idle.load(Ordering::SeqCst),
            "drain_on_idle must remain set while CC is busy"
        );
    }

    #[tokio::test]
    async fn ephemeral_remove_subscriber_drains_immediately() {
        let (bridge, _event_tx, _rx, active_bridges) = test_bridge().await;
        let conv_id = bridge.conversation_id;

        // Insert into registry so kill_session removal is observable.
        active_bridges.insert(conv_id, bridge.clone()).await;

        bridge.add_subscriber(bridge.user_id, "testuser").await;
        bridge.cc_idle.store(true, Ordering::SeqCst);

        bridge.remove_subscriber(bridge.user_id).await;

        // Ephemeral: drain_on_idle set and bridge killed immediately.
        assert!(bridge.drain_on_idle.load(Ordering::SeqCst));
        assert!(
            active_bridges.get(conv_id).await.is_none(),
            "bridge should be removed from registry after ephemeral drain"
        );
    }

    #[tokio::test]
    async fn ephemeral_deferred_drain_does_not_kill_busy_cc() {
        // Regression: in the ephemeral path, when subscribers leave while CC
        // is mid-turn, drain_on_idle is set but the bridge stays alive
        // (no force-kill timer). CC gets to finish what it's doing.
        //
        // pause() is called first so that any sleeps registered by spawned tasks
        // (e.g. cc_event_loop) are under controlled time from the start. Consistent
        // with idle_timer_fires_and_drains and siblings.
        tokio::time::pause();

        let (bridge, _event_tx, _rx, active_bridges) = test_bridge().await;
        let conv_id = bridge.conversation_id;

        active_bridges.insert(conv_id, bridge.clone()).await;

        bridge.add_subscriber(bridge.user_id, "testuser").await;
        // CC is NOT idle — deferred path.
        bridge.cc_idle.store(false, Ordering::SeqCst);

        bridge.remove_subscriber(bridge.user_id).await;

        // Ephemeral: drain_on_idle set but bridge not killed (CC mid-turn).
        assert!(bridge.drain_on_idle.load(Ordering::SeqCst));
        assert!(
            active_bridges.get(conv_id).await.is_some(),
            "bridge should still be alive (deferred drain)"
        );

        // The deferred drain path does not spawn a timer (CC is busy), so this
        // advance fires no timers. The assertion below is the real check; the
        // advance is a guard in case a timer is accidentally introduced on the
        // deferred path — it would fire here and cause the test to fail.
        tokio::time::advance(Duration::from_millis(200)).await;
        assert!(
            active_bridges.get(conv_id).await.is_some(),
            "busy bridge must not be killed by any background timer"
        );
    }

    /// `resolve_cc_state` covers all three branches:
    /// - pending permission → AwaitingApproval
    /// - alive && !idle (busy) → Thinking
    /// - alive && idle (or dead session) → Idle
    ///
    /// The third branch is tested with `is_alive()=true && is_cc_idle()=true`
    /// (the "alive-and-idle" sub-case) to confirm it resolves to Idle, not Thinking.
    /// This sub-case is not exercised by the integration test paths, which reach
    /// the `Idle` branch only via a dead session (no bridge in registry).
    #[tokio::test]
    async fn resolve_cc_state_all_branches() {
        let db = init_db_memory();
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel(64);

        // Branch 1: pending permission → AwaitingApproval.
        {
            let bridge =
                ActiveBridge::inject_for_test(1, 1, "test", db.clone(), broadcast_tx.clone());
            bridge
                .insert_pending_permission_for_test("req1", "bash", serde_json::json!({}))
                .await;
            assert_eq!(
                bridge.resolve_cc_state().await,
                CcState::AwaitingApproval,
                "pending permission must yield AwaitingApproval"
            );
        }

        // Branch 2: alive && busy → Thinking.
        {
            let bridge =
                ActiveBridge::inject_for_test(1, 1, "test", db.clone(), broadcast_tx.clone());
            install_failing_session(&bridge).await; // sets is_alive() = true
            bridge.set_cc_busy("test");
            drain_broadcast(&mut broadcast_rx);
            assert_eq!(
                bridge.resolve_cc_state().await,
                CcState::Thinking,
                "alive && !idle must yield Thinking"
            );
        }

        // Branch 3 (alive-and-idle sub-case): alive && idle → Idle.
        // This confirms the `else` arm maps alive-idle correctly, not just dead-session.
        {
            let bridge =
                ActiveBridge::inject_for_test(1, 1, "test", db.clone(), broadcast_tx.clone());
            install_failing_session(&bridge).await; // sets is_alive() = true
            // cc_idle starts true from inject_for_test; no change needed.
            assert!(bridge.is_cc_idle(), "precondition: bridge must be idle");
            assert_eq!(
                bridge.resolve_cc_state().await,
                CcState::Idle,
                "alive && idle must yield Idle"
            );
        }
    }
}

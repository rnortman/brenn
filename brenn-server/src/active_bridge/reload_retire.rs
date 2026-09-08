//! Retiring a session whose per-process view a reload moved.
//!
//! Authority is read per call, so a reload's swap reaches a live session at
//! once. What a CC process was *spawned with* — its model, MCP servers, cwd,
//! approval rules, compaction thresholds, idle timeouts, and the virtual-tools
//! file `noop_mcp.py` read at its start — is a snapshot taken at spawn and
//! never refreshed in place: a process running in one cwd with one MCP set has
//! to be described by one consistent view. A moved view reaches the agent by
//! retiring the process at its next idle moment and letting the wake path spawn
//! its successor from the table, resuming the conversation.
//!
//! Two things mark a bridge stale: its agent's spawn-shaped fields moved, or
//! the conversation belongs to a user the candidate does not allow. The second
//! retires whether or not the process view moved — no allowed user can attach
//! to that bridge and no delivery targets its conversation.
//!
//! A third arrives on its own: a process spawned from a map a reload has
//! already replaced, which registers after that reload's one-shot sweep has
//! walked past it and is condemned at registration
//! (`condemn_if_spawned_before_a_swap`).
//!
//! A retire never interrupts a turn. A bridge that is mid-turn keeps its flag
//! and retires at its turn end, where [`ActiveBridge::reconsider_profile`]
//! already runs.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tracing::info;

use super::ActiveBridge;
use super::registry::ActiveBridges;

impl ActiveBridge {
    /// Whether a reload has condemned this bridge's process.
    pub(in crate::active_bridge) fn is_reload_stale(&self) -> bool {
        self.reload_stale.load(Ordering::SeqCst)
    }

    /// Whether this bridge's own retirement is what killed the process.
    ///
    /// Condemnation is not a claim on a death: a bridge waits out its turn or
    /// its attached tab first, and a crash in that window is unexpected and
    /// must alert. Only the retirement's own kill sets this.
    pub(in crate::active_bridge) fn is_reload_killing(&self) -> bool {
        self.reload_killing.load(Ordering::SeqCst)
    }

    /// Condemn a process that was spawned from a map the table has already
    /// replaced, at the moment it registers.
    ///
    /// A spawn reads the map, builds a CC process from it over seconds, and
    /// registers the bridge; a reload's swap and its one-shot retirement sweep
    /// both fit in that window, and the bridge that lands afterwards carries a
    /// per-process view of a document nobody is running. It is condemned here
    /// and dies at its next idle moment like any other.
    pub(in crate::active_bridge) fn condemn_if_spawned_before_a_swap(&self) {
        let current = self.apps.generation();
        if current == self.spawned_generation {
            return;
        }
        info!(
            conversation_id = self.conversation_id,
            app_slug = %self.app_slug,
            spawned_generation = self.spawned_generation,
            current_generation = current,
            "session spawned from a superseded agent map; condemned at registration"
        );
        self.mark_reload_stale();
    }

    /// Condemn this bridge's process. It dies at the next moment it may.
    pub(crate) fn mark_reload_stale(&self) {
        self.reload_stale.store(true, Ordering::SeqCst);
    }

    /// Kill a condemned process, if this is a moment where that is allowed.
    /// Returns whether it was killed.
    ///
    /// The guard is [`ActiveBridge::reconsider_profile`]'s, for the same
    /// reasons: a draining bridge dies anyway and the next wake spawns from the
    /// table; a bridge mid-swap is having its process replaced already; a server
    /// on its way down must start no CC process and no podman container the
    /// shutdown has walked past. One condition is this path's own — a
    /// non-persistent agent with a subscriber attached is left alone, because
    /// the tab's detach kills it, and that death *is* the retirement. That
    /// question is asked of the map this process was spawned from, not of the
    /// table: what the detach does is decided by the `LifetimeArbiter` built
    /// from that same snapshot, and `persistent` is a field a reload moves.
    pub(crate) async fn retire_if_stale_and_idle(self: &Arc<Self>) -> bool {
        if !self.is_reload_stale()
            || !self.cc_idle.load(Ordering::SeqCst)
            || self.drain_on_idle.load(Ordering::SeqCst)
            || self.swapping.load(Ordering::SeqCst)
            || self.server_shutting_down.load(Ordering::SeqCst)
        {
            return false;
        }
        if !self.spawned_persistent && !self.subscribers.read().await.is_empty() {
            return false;
        }
        info!(
            conversation_id = self.conversation_id,
            app_slug = %self.app_slug,
            "retiring session for reload; the next wake spawns from the new agent"
        );
        // Claimed in the instant before the kill, so a death anywhere else in
        // the condemned window is still an unexpected one.
        self.reload_killing.store(true, Ordering::SeqCst);
        let active_bridges = self.active_bridges.clone();
        self.kill_session(&active_bridges).await;
        true
    }
}

/// What a reload's retirement step did to one agent's live sessions.
pub struct RetireOutcome {
    /// Conversations whose process died now.
    pub retired: Vec<i64>,
    /// Conversations condemned but mid-turn (or otherwise not retirable yet);
    /// each dies at its own turn end.
    pub pending: Vec<i64>,
}

impl ActiveBridges {
    /// Condemn the live sessions of one agent that a reload moved out from
    /// under, and kill the ones that can die now.
    ///
    /// `respawn` condemns every session of the agent — its per-process view
    /// moved. `allowed_user_ids` is `Some` when the candidate denies a user the
    /// baseline allowed: it is the candidate's own allowed set, and every
    /// session whose owner is outside it is condemned whether or not the view
    /// moved. `None` when the agent denies nobody new, which includes an agent
    /// whose list went empty — empty is open to all.
    ///
    /// Asked as "who is still allowed" rather than "who was removed" because an
    /// agent that was open to all and is now restricted names no removed user:
    /// the users it denies are everyone the new list omits.
    pub async fn retire_for_reload(
        &self,
        app_slug: &str,
        respawn: bool,
        allowed_user_ids: Option<&[i64]>,
    ) -> RetireOutcome {
        let mut outcome = RetireOutcome {
            retired: Vec::new(),
            pending: Vec::new(),
        };
        for bridge in self.get_for_app(app_slug).await {
            let denied = allowed_user_ids.is_some_and(|allowed| !allowed.contains(&bridge.user_id));
            if !respawn && !denied {
                continue;
            }
            bridge.mark_reload_stale();
            if bridge.retire_if_stale_and_idle().await {
                outcome.retired.push(bridge.conversation_id);
            } else {
                outcome.pending.push(bridge.conversation_id);
            }
        }
        outcome.retired.sort_unstable();
        outcome.pending.sort_unstable();
        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use tokio::sync::broadcast;

    use super::super::ActiveBridge;
    use super::super::registry::ActiveBridges;
    use super::super::test_fixtures::{TestBridgeConfig, single_app_table};
    use crate::test_support::init_db_memory;

    /// A registered bridge for `slug`, owned by a freshly minted user. One `db`
    /// per test, shared by every bridge in it: the registry is keyed on
    /// conversation id, and two databases would mint the same one twice.
    async fn registered(
        db: &brenn_db::Db,
        registry: &ActiveBridges,
        slug: &str,
        username: &str,
        persistent: bool,
    ) -> Arc<ActiveBridge> {
        let (uid, cid) = {
            let conn = db.lock().await;
            let uid = brenn_db::auth::user::create_user(&conn, username, "$argon2id$fake");
            let cid = brenn_db::conversation::create_conversation(&conn, uid, slug, false);
            (uid, cid)
        };
        let (tx, _rx) = broadcast::channel(16);
        let (alert, _h) = brenn_obs::alerting::noop_alert_dispatcher();
        let bridge = ActiveBridge::inject_for_test_full(
            uid,
            cid,
            slug,
            db.clone(),
            tx,
            alert,
            TestBridgeConfig {
                active_bridges: Some(registry.clone()),
                apps: Some(single_app_table(slug, |app| app.persistent = persistent)),
                ..Default::default()
            },
        );
        registry.insert(cid, bridge.clone()).await;
        bridge
    }

    /// A bridge built from one map and registered after the table was swapped
    /// out from under it — the spawn window a reload's sweep can fall inside.
    async fn registered_after_swap(
        db: &brenn_db::Db,
        registry: &ActiveBridges,
        slug: &str,
        username: &str,
    ) -> Arc<ActiveBridge> {
        let (uid, cid) = {
            let conn = db.lock().await;
            let uid = brenn_db::auth::user::create_user(&conn, username, "$argon2id$fake");
            let cid = brenn_db::conversation::create_conversation(&conn, uid, slug, false);
            (uid, cid)
        };
        let (tx, _rx) = broadcast::channel(16);
        let (alert, _h) = brenn_obs::alerting::noop_alert_dispatcher();
        let table = single_app_table(slug, |app| app.persistent = true);
        let bridge = ActiveBridge::inject_for_test_full(
            uid,
            cid,
            slug,
            db.clone(),
            tx,
            alert,
            TestBridgeConfig {
                active_bridges: Some(registry.clone()),
                apps: Some(table.clone()),
                ..Default::default()
            },
        );
        // The swap lands between the spawn's read of the map and the
        // registration of the process it built.
        table.store(single_app_table(slug, |app| app.persistent = true).load());
        registry.insert(cid, bridge.clone()).await;
        bridge
    }

    #[tokio::test]
    async fn a_stale_idle_bridge_retires() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = registered(&db, &registry, "test", "u1", true).await;
        bridge.mark_reload_stale();
        assert!(bridge.retire_if_stale_and_idle().await);
        assert!(
            registry.get(bridge.conversation_id).await.is_none(),
            "a retired bridge is deregistered"
        );
    }

    #[tokio::test]
    async fn an_unmarked_bridge_is_left_alone() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = registered(&db, &registry, "test", "u1", true).await;
        assert!(!bridge.retire_if_stale_and_idle().await);
        assert!(registry.get(bridge.conversation_id).await.is_some());
    }

    #[tokio::test]
    async fn each_guard_defers_the_retire() {
        for (name, set) in [
            ("mid-turn", 0usize),
            ("draining", 1),
            ("swapping", 2),
            ("shutting down", 3),
        ] {
            let db = init_db_memory();
            let registry = ActiveBridges::new();
            let bridge = registered(&db, &registry, "test", "u1", true).await;
            bridge.mark_reload_stale();
            match set {
                0 => bridge.cc_idle.store(false, Ordering::SeqCst),
                1 => bridge.drain_on_idle.store(true, Ordering::SeqCst),
                2 => bridge.swapping.store(true, Ordering::SeqCst),
                _ => bridge.server_shutting_down.store(true, Ordering::SeqCst),
            }
            assert!(
                !bridge.retire_if_stale_and_idle().await,
                "a {name} bridge must not be retired"
            );
            assert!(bridge.is_reload_stale(), "but it stays condemned");
        }
    }

    /// A non-persistent agent's bridge dies at its tab's detach anyway, and that
    /// death is the retirement. A persistent one has no such door.
    #[tokio::test]
    async fn a_non_persistent_bridge_with_a_subscriber_is_left_to_its_detach() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = registered(&db, &registry, "test", "u1", false).await;
        bridge.add_subscriber(bridge.user_id, "u1").await;
        bridge.mark_reload_stale();
        assert!(!bridge.retire_if_stale_and_idle().await);

        let persistent = registered(&db, &registry, "test2", "u2", true).await;
        persistent.add_subscriber(persistent.user_id, "u2").await;
        persistent.mark_reload_stale();
        assert!(
            persistent.retire_if_stale_and_idle().await,
            "a persistent bridge has no detach to wait for"
        );
    }

    #[tokio::test]
    async fn respawn_condemns_every_session_of_the_agent() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let a = registered(&db, &registry, "test", "u1", true).await;
        let b = registered(&db, &registry, "test", "u2", true).await;
        let other = registered(&db, &registry, "elsewhere", "u3", true).await;

        let outcome = registry.retire_for_reload("test", true, None).await;
        let mut expected = vec![a.conversation_id, b.conversation_id];
        expected.sort_unstable();
        assert_eq!(outcome.retired, expected);
        assert!(outcome.pending.is_empty());
        assert!(
            !other.is_reload_stale(),
            "another agent's session is untouched"
        );
    }

    /// Without `respawn`, only the sessions of users outside the candidate's
    /// allowed list are condemned.
    #[tokio::test]
    async fn a_denied_user_retires_without_a_respawn() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let removed = registered(&db, &registry, "test", "u1", true).await;
        let kept = registered(&db, &registry, "test", "u2", true).await;

        let outcome = registry
            .retire_for_reload("test", false, Some(&[kept.user_id]))
            .await;
        assert_eq!(outcome.retired, vec![removed.conversation_id]);
        assert!(!kept.is_reload_stale(), "an allowed user's session stays");
        assert!(registry.get(kept.conversation_id).await.is_some());
    }

    /// An agent open to all, restricted to one user: the document names nobody
    /// as removed, and every other user's session is still denied.
    #[tokio::test]
    async fn restricting_an_open_agent_retires_everyone_it_now_denies() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let allowed = registered(&db, &registry, "test", "u1", true).await;
        let denied = registered(&db, &registry, "test", "u2", true).await;

        let outcome = registry
            .retire_for_reload("test", false, Some(&[allowed.user_id]))
            .await;
        assert_eq!(outcome.retired, vec![denied.conversation_id]);
        assert!(
            !allowed.is_reload_stale(),
            "the user the candidate names keeps their session"
        );
    }

    /// The condemnation is not a claim on the death: a bridge waiting out its
    /// turn that dies on its own is an unexpected death and still alerts.
    #[tokio::test]
    async fn a_condemned_bridge_that_crashes_before_its_retirement_dies_unexpectedly() {
        use super::super::cc_event_loop::ShutdownReason;

        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = registered(&db, &registry, "test", "u1", true).await;
        bridge.cc_idle.store(false, Ordering::SeqCst);
        bridge.mark_reload_stale();

        assert!(
            !ShutdownReason::from_bridge(&bridge).is_intentional(),
            "a crash while condemned but not yet killed is unexpected"
        );
    }

    /// A process spawned from a map the table has already replaced is condemned
    /// when it registers: the reload's own sweep ran before it existed.
    #[tokio::test]
    async fn a_bridge_registered_after_a_swap_is_condemned_at_registration() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = registered(&db, &registry, "test", "u1", true).await;
        assert!(
            !bridge.is_reload_stale(),
            "a bridge registered against the map it was spawned from is not stale"
        );

        let late = registered_after_swap(&db, &registry, "test", "u2").await;
        assert!(
            late.is_reload_stale(),
            "one whose table moved while it spawned is"
        );
    }

    /// A retired process's death is expected: no alert, no `Error` on the
    /// conversation, runtime state reset — the same arm a drain takes.
    #[tokio::test]
    async fn a_retired_process_dies_intentionally() {
        use super::super::cc_event_loop::ShutdownReason;

        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = registered(&db, &registry, "test", "u1", true).await;
        assert!(
            !ShutdownReason::from_bridge(&bridge).is_intentional(),
            "an ordinary death is still unexpected"
        );

        bridge.mark_reload_stale();
        assert!(
            bridge.retire_if_stale_and_idle().await,
            "an idle condemned bridge is killed"
        );
        match ShutdownReason::from_bridge(&bridge) {
            ShutdownReason::Intentional {
                drain,
                server,
                reload,
            } => {
                assert!(reload, "a retired process names the reload");
                assert!(!drain && !server, "and neither of the other two");
            }
            other => panic!("a retired process must die intentionally, got {other:?}"),
        }
    }

    /// A session mid-turn is reported pending and dies at its turn end, which is
    /// where `reconsider_profile` already runs.
    #[tokio::test]
    async fn a_busy_session_is_pending_and_retires_at_its_turn_end() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = registered(&db, &registry, "test", "u1", true).await;
        bridge.cc_idle.store(false, Ordering::SeqCst);

        let outcome = registry.retire_for_reload("test", true, None).await;
        assert!(outcome.retired.is_empty());
        assert_eq!(outcome.pending, vec![bridge.conversation_id]);
        assert!(registry.get(bridge.conversation_id).await.is_some());

        // The turn ends.
        bridge.cc_idle.store(true, Ordering::SeqCst);
        assert!(bridge.retire_if_stale_and_idle().await);
        assert!(registry.get(bridge.conversation_id).await.is_none());
    }
}

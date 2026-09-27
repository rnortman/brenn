//! Retiring the session of a conversation a newer one superseded.
//!
//! The epoch counterpart of `reload_retire.rs`: a condemned bridge dies at the
//! next moment it may, and never mid-turn. It differs in one condition — an
//! attached tab does not hold the bridge open, because the tab is switched to
//! the agent's current conversation instead of being a reason to keep this one
//! alive.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tracing::info;

use super::ActiveBridge;

impl ActiveBridge {
    /// Whether a newer conversation has condemned this bridge's process.
    pub(crate) fn is_superseded(&self) -> bool {
        self.superseded.load(Ordering::SeqCst)
    }

    /// Whether this bridge's own supersede retirement is what killed the
    /// process.
    ///
    /// Condemnation is not a claim on a death: a bridge waits out its turn
    /// first, and a crash in that window is unexpected and must alert. Only the
    /// retirement's own kill sets this.
    pub(crate) fn is_superseded_killing(&self) -> bool {
        self.superseded_killing.load(Ordering::SeqCst)
    }

    /// Condemn this bridge's process: its conversation is no longer the
    /// agent's current one. It dies at the next moment it may.
    pub(crate) fn mark_superseded(&self) {
        self.superseded.store(true, Ordering::SeqCst);
    }

    /// Kill a superseded process, if this is a moment where that is allowed.
    /// Returns whether it was killed.
    ///
    /// The guards are `retire_if_stale_and_idle`'s: mid-turn waits for the turn
    /// end, a draining bridge dies anyway, a bridge mid-swap is having its
    /// process replaced already, and a server on its way down starts nothing
    /// and kills nothing the shutdown has walked past. Unlike that path, an
    /// attached subscriber does not defer the kill: the tab follows the agent to
    /// its current conversation. The row is completed before the kill, since
    /// nothing will resume it.
    pub(crate) async fn retire_if_superseded_and_idle(self: &Arc<Self>) -> bool {
        if !self.is_superseded()
            || !self.cc_idle.load(Ordering::SeqCst)
            || self.drain_on_idle.load(Ordering::SeqCst)
            || self.swapping.load(Ordering::SeqCst)
            || self.server_shutting_down.load(Ordering::SeqCst)
        {
            return false;
        }
        info!(
            conversation_id = self.conversation_id,
            app_slug = %self.app_slug,
            "retiring a superseded conversation's session; the agent's current conversation is a newer one"
        );
        // Claimed in the instant before the kill, so a death anywhere else in
        // the condemned window is still an unexpected one.
        self.superseded_killing.store(true, Ordering::SeqCst);
        self.complete_and_kill().await;
        true
    }

    /// Spawn a reconcile when this process was spawned under an epoch other
    /// than its agent's current one. Cheap and synchronous, like
    /// `reconsider_profile`.
    pub(in crate::active_bridge) fn reconsider_epoch(&self) {
        let Some(reconciler) = &self.epoch_reconciler else {
            return;
        };
        if self.server_shutting_down.load(Ordering::SeqCst) {
            return;
        }
        if reconciler.epochs().current(&self.app_slug) == self.spawned_epoch {
            return;
        }
        let reconciler = reconciler.clone();
        let slug = self.app_slug.clone();
        drop(tokio::spawn(
            async move { reconciler.reconcile(&slug).await },
        ));
    }

    /// At registration: condemn a bridge whose conversation is no longer its
    /// agent's current one.
    ///
    /// A spawn of an old conversation — a chat peer addressing it by id, or a
    /// wake racing a reconcile — can register after the reconcile's retirement
    /// step has walked the registry. Only an epoch agent's conversations are
    /// ever superseded, so a bridge without the reconciler is not asked.
    pub(in crate::active_bridge) async fn condemn_if_superseded(&self) {
        if self.epoch_reconciler.is_none() {
            return;
        }
        let newest = {
            let conn = self.db.lock().await;
            brenn_db::conversation::get_singleton_conversation_id(
                &conn,
                self.user_id,
                &self.app_slug,
            )
        };
        if let Some(newest) = newest
            && newest != self.conversation_id
        {
            info!(
                conversation_id = self.conversation_id,
                app_slug = %self.app_slug,
                newest,
                "session registered for a superseded conversation; condemned at registration"
            );
            self.mark_superseded();
        }
    }

    /// Hold this conversation's bus-delivery lock. The reconciler holds it
    /// across the position walk, so no batch is read here and inherited by the
    /// successor before it is advanced past.
    pub(crate) async fn lock_bus_delivery(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.bus_delivery.lock().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::sync::broadcast;

    use super::super::ActiveBridge;
    use super::super::cc_event_loop::ShutdownReason;
    use super::super::registry::ActiveBridges;
    use super::super::test_fixtures::{TestBridgeConfig, single_app_table};
    use crate::conversation_epoch::{EpochReconciler, test_reconciler};
    use crate::test_support::init_db_memory;

    const SLUG: &str = "test";

    fn epoch_table(slug: &str, username: &str, persistent: bool) -> brenn_lib::config::AppTable {
        single_app_table(slug, |app| {
            app.singleton = true;
            app.persistent = persistent;
            app.allowed_users = vec![username.to_string()];
            app.conversation_epoch = Some(format!("brenn:{slug}.epoch"));
        })
    }

    /// A user and a conversation of `SLUG` for them.
    async fn seat(db: &brenn_db::Db, username: &str) -> (i64, i64) {
        let conn = db.lock().await;
        let uid = brenn_db::auth::user::create_user(&conn, username, "$argon2id$fake");
        let cid = brenn_db::conversation::create_conversation(&conn, uid, SLUG, false);
        (uid, cid)
    }

    /// A registered bridge on `cid`.
    async fn registered(
        db: &brenn_db::Db,
        registry: &ActiveBridges,
        table: &brenn_lib::config::AppTable,
        uid: i64,
        cid: i64,
        epoch_reconciler: Option<Arc<EpochReconciler>>,
        spawned_epoch: Option<String>,
    ) -> Arc<ActiveBridge> {
        let (tx, _rx) = broadcast::channel(16);
        let (alert, _h) = brenn_obs::alerting::noop_alert_dispatcher();
        let bridge = ActiveBridge::inject_for_test_full(
            uid,
            cid,
            SLUG,
            db.clone(),
            tx,
            alert,
            TestBridgeConfig {
                active_bridges: Some(registry.clone()),
                apps: Some(table.clone()),
                epoch_reconciler,
                spawned_epoch,
                ..Default::default()
            },
        );
        registry.insert(cid, bridge.clone()).await;
        bridge
    }

    /// A registered bridge with no reconciler, owned by a fresh user.
    async fn plain(
        db: &brenn_db::Db,
        registry: &ActiveBridges,
        username: &str,
        persistent: bool,
    ) -> Arc<ActiveBridge> {
        let (uid, cid) = seat(db, username).await;
        let table = epoch_table(SLUG, username, persistent);
        registered(db, registry, &table, uid, cid, None, None).await
    }

    async fn status(db: &brenn_db::Db, cid: i64) -> brenn_db::conversation::ConversationStatus {
        let conn = db.lock().await;
        brenn_db::conversation::get_conversation(&conn, cid).status
    }

    async fn app_rows(db: &brenn_db::Db) -> i64 {
        let conn = db.lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM conversations WHERE app_slug = ?1",
            rusqlite::params![SLUG],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_superseded_idle_bridge_retires_and_dies_intentionally() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = plain(&db, &registry, "u1", true).await;
        assert!(
            !ShutdownReason::from_bridge(&bridge).is_intentional(),
            "an ordinary death is still unexpected"
        );

        bridge.mark_superseded();
        assert!(bridge.retire_if_superseded_and_idle().await);
        assert!(registry.get(bridge.conversation_id).await.is_none());
        assert_eq!(
            status(&db, bridge.conversation_id).await,
            brenn_db::conversation::ConversationStatus::Completed
        );
        match ShutdownReason::from_bridge(&bridge) {
            ShutdownReason::Intentional {
                drain,
                server,
                reload,
                superseded,
            } => {
                assert!(superseded, "a retired process names the supersede");
                assert!(!drain && !server && !reload, "and none of the other three");
            }
            other => panic!("a retired process must die intentionally, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn each_guard_defers_the_superseded_retire() {
        for (name, set) in [
            ("mid-turn", 0usize),
            ("draining", 1),
            ("swapping", 2),
            ("shutting down", 3),
        ] {
            let db = init_db_memory();
            let registry = ActiveBridges::new();
            let bridge = plain(&db, &registry, "u1", true).await;
            bridge.mark_superseded();
            match set {
                0 => bridge.cc_idle.store(false, Ordering::SeqCst),
                1 => bridge.drain_on_idle.store(true, Ordering::SeqCst),
                2 => bridge.swapping.store(true, Ordering::SeqCst),
                _ => bridge.server_shutting_down.store(true, Ordering::SeqCst),
            }
            assert!(
                !bridge.retire_if_superseded_and_idle().await,
                "a {name} bridge must not be retired"
            );
            assert!(bridge.is_superseded(), "but it stays condemned");
        }
    }

    /// The reload path leaves a non-persistent bridge with a tab to its detach;
    /// this one does not, because the tab moves to the current conversation.
    #[tokio::test]
    async fn an_attached_tab_does_not_hold_a_superseded_bridge() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = plain(&db, &registry, "u1", false).await;
        bridge.add_subscriber(bridge.user_id, "u1").await;
        bridge.mark_superseded();
        assert!(bridge.retire_if_superseded_and_idle().await);
    }

    /// The condemnation is not a claim on the death.
    #[tokio::test]
    async fn a_superseded_bridge_that_crashes_before_its_retire_dies_unexpectedly() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = plain(&db, &registry, "u1", true).await;
        bridge.cc_idle.store(false, Ordering::SeqCst);
        bridge.mark_superseded();
        assert!(
            !ShutdownReason::from_bridge(&bridge).is_intentional(),
            "a crash while condemned but not yet killed is unexpected"
        );
    }

    #[tokio::test]
    async fn a_bridge_condemned_mid_turn_dies_at_its_turn_end() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let bridge = plain(&db, &registry, "u1", true).await;
        bridge.cc_idle.store(false, Ordering::SeqCst);
        bridge.mark_superseded();
        crate::active_bridge::compaction::set_idle_and_drain(&bridge).await;
        assert!(
            registry.get(bridge.conversation_id).await.is_none(),
            "the turn end is the retirement"
        );
    }

    #[tokio::test]
    async fn a_bridge_registering_for_a_superseded_conversation_is_condemned() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let (uid, old) = seat(&db, "u1").await;
        let new = {
            let conn = db.lock().await;
            brenn_db::conversation::create_singleton_successor(&conn, uid, SLUG, "1")
        };
        let table = epoch_table(SLUG, "u1", true);
        let reconciler = test_reconciler(db.clone(), registry.clone(), table.clone(), SLUG);

        let stale = registered(
            &db,
            &registry,
            &table,
            uid,
            old,
            Some(reconciler.clone()),
            None,
        )
        .await;
        assert!(stale.is_superseded(), "the old conversation is condemned");

        let current = registered(
            &db,
            &registry,
            &table,
            uid,
            new,
            Some(reconciler),
            Some("1".to_string()),
        )
        .await;
        assert!(!current.is_superseded(), "the current one is not");

        // Without the reconciler, the same shape is not asked.
        let db2 = init_db_memory();
        let registry2 = ActiveBridges::new();
        let (uid2, old2) = seat(&db2, "u1").await;
        {
            let conn = db2.lock().await;
            brenn_db::conversation::create_singleton_successor(&conn, uid2, SLUG, "1");
        }
        let blind = registered(&db2, &registry2, &table, uid2, old2, None, None).await;
        assert!(
            !blind.is_superseded(),
            "a bridge without the reconciler is never condemned at registration"
        );
    }

    #[tokio::test]
    async fn a_bridge_spawned_under_a_stale_epoch_reconciles_at_its_idle_moment() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let (uid, old) = seat(&db, "u1").await;
        let table = epoch_table(SLUG, "u1", true);
        let reconciler = test_reconciler(db.clone(), registry.clone(), table.clone(), SLUG);
        let bridge = registered(
            &db,
            &registry,
            &table,
            uid,
            old,
            Some(reconciler.clone()),
            None,
        )
        .await;

        reconciler
            .epochs()
            .apply(&format!("brenn:{SLUG}.epoch"), "1");
        crate::active_bridge::compaction::set_idle_and_drain(&bridge).await;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while registry.get(old).await.is_some() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the stale bridge was not retired by the reconcile its idle moment asked for"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let current = {
            let conn = db.lock().await;
            brenn_db::conversation::get_singleton_conversation(&conn, uid, SLUG)
                .expect("the agent has a conversation")
        };
        assert_eq!(current.epoch.as_deref(), Some("1"));
        assert!(bridge.is_superseded_killing());
    }

    #[tokio::test]
    async fn a_current_epoch_asks_nothing() {
        let db = init_db_memory();
        let registry = ActiveBridges::new();
        let (uid, old) = seat(&db, "u1").await;
        let table = epoch_table(SLUG, "u1", true);
        let reconciler = test_reconciler(db.clone(), registry.clone(), table.clone(), SLUG);
        let bridge = registered(
            &db,
            &registry,
            &table,
            uid,
            old,
            Some(reconciler.clone()),
            Some("1".to_string()),
        )
        .await;

        reconciler
            .epochs()
            .apply(&format!("brenn:{SLUG}.epoch"), "1");
        crate::active_bridge::compaction::set_idle_and_drain(&bridge).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(registry.get(old).await.is_some(), "still registered");
        assert_eq!(app_rows(&db).await, 1);
    }
}

//! Global registry of active CC bridges keyed by conversation_id.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::ActiveBridge;

/// Global registry of live CC bridges, keyed by conversation_id.
///
/// A conversation has at most one active bridge regardless of who started it.
/// Re-keyed from `(user_id, conversation_id)` for multiuser support — the bridge
/// represents a CC subprocess per-conversation, not per-user.
///
/// RwLock because reads (lookup on every message send) vastly outnumber
/// writes (CC spawn / CC exit cleanup).
#[derive(Clone)]
pub struct ActiveBridges {
    inner: Arc<tokio::sync::RwLock<HashMap<i64, Arc<ActiveBridge>>>>,
}

impl ActiveBridges {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Look up a live bridge by conversation_id.
    pub async fn get(&self, conversation_id: i64) -> Option<Arc<ActiveBridge>> {
        let map = self.inner.read().await;
        map.get(&conversation_id).cloned()
    }

    /// Find any live bridge for a user in a specific app. Returns (conversation_id, bridge).
    /// If multiple exist, returns the one with the highest conversation_id
    /// (most recently created).
    ///
    /// In multiuser apps (`multiuser` = true), also returns bridges for shared conversations
    /// owned by other users.
    pub async fn get_for_user(
        &self,
        user_id: i64,
        app_slug: &str,
        multiuser: bool,
    ) -> Option<(i64, Arc<ActiveBridge>)> {
        let map = self.inner.read().await;
        map.iter()
            .filter(|(_, b)| {
                b.app_slug == app_slug
                    && (b.user_id == user_id || (multiuser && b.shared.load(Ordering::Relaxed)))
            })
            .max_by_key(|(cid, _)| **cid)
            .map(|(cid, bridge)| (*cid, bridge.clone()))
    }

    /// Find all live bridges for a given app (across all users).
    /// Used for single-instance enforcement.
    pub async fn get_for_app(&self, app_slug: &str) -> Vec<Arc<ActiveBridge>> {
        let map = self.inner.read().await;
        map.values()
            .filter(|b| b.app_slug == app_slug)
            .cloned()
            .collect()
    }

    /// Snapshot every live bridge. Returns cloned `Arc`s so the caller can
    /// iterate without holding the registry lock (and can safely `remove`
    /// bridges mid-iteration). Used by the wedge watchdog's sweep.
    pub async fn all(&self) -> Vec<Arc<ActiveBridge>> {
        let map = self.inner.read().await;
        map.values().cloned().collect()
    }

    /// Register a live bridge.
    pub async fn insert(&self, conversation_id: i64, bridge: Arc<ActiveBridge>) {
        let mut map = self.inner.write().await;
        map.insert(conversation_id, bridge);
    }

    /// Unconditionally remove a bridge, ignoring allocation identity.
    ///
    /// Test-only helper for clearing a conversation's slot during setup. Shipping
    /// deregistration (event-loop CC-exit teardown, watchdog reap) uses
    /// `remove_if_same` instead, so a late removal cannot clobber a replacement
    /// bridge that took the same conversation_id slot.
    #[cfg(test)]
    pub async fn remove(&self, conversation_id: i64) {
        let mut map = self.inner.write().await;
        map.remove(&conversation_id);
    }

    /// Remove a bridge only if the registered entry is the same allocation as
    /// `expected` (an `Arc::as_ptr(..) as usize` identity token).
    ///
    /// A late teardown (watchdog reap, or an old event loop's `kill_session`)
    /// must not clobber a *replacement* bridge that took the same
    /// conversation_id slot after the old one was already deregistered.
    /// Identity-checking the removal preserves the "at most one active bridge
    /// per conversation" invariant in the face of that race.
    pub async fn remove_if_same(&self, conversation_id: i64, expected: usize) {
        let mut map = self.inner.write().await;
        if let Some(existing) = map.get(&conversation_id)
            && Arc::as_ptr(existing) as usize == expected
        {
            map.remove(&conversation_id);
        }
    }

    /// Mark every live `CcSession` as shutting down so its reader task's EOF
    /// branch suppresses the Critical "CC process died" alert during
    /// intentional server teardown. The process-wide `server_shutting_down`
    /// flag handles the event-loop-side Warning alert separately; this
    /// method's job is only the reader-side suppression.
    ///
    /// Called from `shutdown_signal` on SIGTERM/SIGINT. Safe to call even if
    /// some bridges have already had `kill_session` invoked — `mark_shutting_down`
    /// is idempotent (flips a flag that's checked on EOF; no-op if already set).
    pub async fn mark_all_sessions_shutting_down(&self) {
        let map = self.inner.read().await;
        for bridge in map.values() {
            let session = bridge.session.lock().await;
            if let Some(ref s) = *session {
                s.mark_shutting_down();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    use tokio::sync::broadcast;

    use crate::active_bridge::test_fixtures::TestBridgeConfig;

    #[tokio::test]
    async fn mark_all_sessions_shutting_down_flips_each_session_flag() {
        // Simulates what shutdown_signal does on SIGTERM: walk the registry
        // and mark each live CcSession. Verifies every session's per-session
        // `shutting_down` flag flipped so its reader task's EOF branch stays
        // quiet.
        let db = brenn_lib::db::init_db_memory();
        let active_bridges = ActiveBridges::new();

        async fn install_session_on(bridge: &ActiveBridge) -> Arc<AtomicBool> {
            let session = brenn_cc::session::CcSession::dummy_for_test();
            let flag = session.shutting_down_flag();
            *bridge.session.lock().await = Some(session);
            flag
        }

        let (tx_a, _rx_a) = tokio::sync::broadcast::channel(8);
        let bridge_a = ActiveBridge::inject_for_test_full(
            1,
            101,
            "test",
            db.clone(),
            tx_a,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges.clone()),
                ..Default::default()
            },
        );
        let flag_a = install_session_on(&bridge_a).await;

        let (tx_b, _rx_b) = tokio::sync::broadcast::channel(8);
        let bridge_b = ActiveBridge::inject_for_test_full(
            1,
            102,
            "test",
            db.clone(),
            tx_b,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            TestBridgeConfig {
                active_bridges: Some(active_bridges.clone()),
                ..Default::default()
            },
        );
        let flag_b = install_session_on(&bridge_b).await;

        active_bridges.insert(101, bridge_a).await;
        active_bridges.insert(102, bridge_b).await;

        // Precondition: both flags unset.
        assert!(!flag_a.load(Ordering::SeqCst));
        assert!(!flag_b.load(Ordering::SeqCst));

        active_bridges.mark_all_sessions_shutting_down().await;

        assert!(
            flag_a.load(Ordering::SeqCst),
            "session A shutting_down must be set"
        );
        assert!(
            flag_b.load(Ordering::SeqCst),
            "session B shutting_down must be set"
        );
    }

    #[tokio::test]
    async fn active_bridges_keyed_by_conversation() {
        let active_bridges = ActiveBridges::new();
        let db = brenn_lib::db::init_db_memory();
        let (tx, _) = broadcast::channel(1);

        let bridge1 = ActiveBridge::inject_for_test(1, 10, "alpha", db.clone(), tx.clone());
        let bridge2 = ActiveBridge::inject_for_test(2, 20, "alpha", db.clone(), tx.clone());

        active_bridges.insert(10, bridge1).await;
        active_bridges.insert(20, bridge2).await;

        // Lookup by conversation_id works regardless of who owns it.
        assert!(active_bridges.get(10).await.is_some());
        assert!(active_bridges.get(20).await.is_some());
        assert!(active_bridges.get(99).await.is_none());

        // get_for_user without multiuser: each user only sees their own.
        let (cid, _) = active_bridges
            .get_for_user(1, "alpha", false)
            .await
            .unwrap();
        assert_eq!(cid, 10);
        let (cid, _) = active_bridges
            .get_for_user(2, "alpha", false)
            .await
            .unwrap();
        assert_eq!(cid, 20);
        // User 1 can't see user 2's bridge without multiuser.
        assert!(
            active_bridges
                .get_for_user(99, "alpha", false)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn get_for_user_multiuser_returns_shared() {
        let active_bridges = ActiveBridges::new();
        let db = brenn_lib::db::init_db_memory();
        let (tx, _) = broadcast::channel(1);

        // User 1 has a shared bridge, user 2 has a private bridge.
        let b1 = ActiveBridge::inject_for_test_shared(1, 10, "alpha", true, db.clone(), tx.clone());

        let b2 = ActiveBridge::inject_for_test(2, 20, "alpha", db.clone(), tx.clone());

        active_bridges.insert(10, b1).await;
        active_bridges.insert(20, b2).await;

        // In multiuser mode, user 3 (not an owner) can see the shared bridge.
        let result = active_bridges.get_for_user(3, "alpha", true).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().0, 10);

        // In non-multiuser mode, user 3 sees nothing.
        assert!(
            active_bridges
                .get_for_user(3, "alpha", false)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn get_for_user_scoped_to_app() {
        let active_bridges = ActiveBridges::new();
        let db = brenn_lib::db::init_db_memory();
        let (tx, _) = broadcast::channel(1);

        // Same user, two different apps.
        let bridge_a = ActiveBridge::inject_for_test(1, 10, "alpha", db.clone(), tx.clone());
        let bridge_b = ActiveBridge::inject_for_test(1, 20, "beta", db.clone(), tx.clone());

        active_bridges.insert(10, bridge_a).await;
        active_bridges.insert(20, bridge_b).await;

        // get_for_user returns only the bridge for the requested app.
        let (cid, _) = active_bridges
            .get_for_user(1, "alpha", false)
            .await
            .unwrap();
        assert_eq!(cid, 10);
        let (cid, _) = active_bridges.get_for_user(1, "beta", false).await.unwrap();
        assert_eq!(cid, 20);
        // Non-existent app returns None.
        assert!(
            active_bridges
                .get_for_user(1, "gamma", false)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn get_for_app_returns_all_bridges() {
        let active_bridges = ActiveBridges::new();
        let db = brenn_lib::db::init_db_memory();
        let (tx, _) = broadcast::channel(1);

        // Two users in the same app, one user in a different app.
        let b1 = ActiveBridge::inject_for_test(1, 10, "alpha", db.clone(), tx.clone());
        let b2 = ActiveBridge::inject_for_test(2, 20, "alpha", db.clone(), tx.clone());
        let b3 = ActiveBridge::inject_for_test(3, 30, "beta", db.clone(), tx.clone());

        active_bridges.insert(10, b1).await;
        active_bridges.insert(20, b2).await;
        active_bridges.insert(30, b3).await;

        let alpha_bridges = active_bridges.get_for_app("alpha").await;
        assert_eq!(alpha_bridges.len(), 2);

        let beta_bridges = active_bridges.get_for_app("beta").await;
        assert_eq!(beta_bridges.len(), 1);
        assert_eq!(beta_bridges[0].conversation_id, 30);

        let gamma_bridges = active_bridges.get_for_app("gamma").await;
        assert!(gamma_bridges.is_empty());
    }
}

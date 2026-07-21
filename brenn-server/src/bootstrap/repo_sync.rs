//! Start the repo-sync manager and build its webhook index.

use std::collections::HashMap;
use std::sync::Arc;

use brenn_lib::config::AppConfig;
use brenn_lib::config::RepoSyncConfig;
use brenn_lib::obs::alerting::AlertDispatcher;
use indexmap::IndexMap;
use tokio::sync::Mutex;

use crate::active_bridge::ActiveBridges;
use crate::repo_sync::{
    CloneInfo, RepoSyncManager, SyncTriggerSender, build_clone_index, build_remote_to_slugs,
};

/// Outcome of starting the repo-sync manager.
///
/// The clone index and per-remote locks are built here (once, at bootstrap) and
/// shared with both the manager and `GitRepoPullTool`, so tool-driven and
/// poller-driven pulls of one remote serialize on the same mutex. They are
/// present even when the manager itself does not spawn (feature disabled), so a
/// tool-driven pull still works against a mounted clone.
pub(crate) struct RepoSyncResult {
    pub(crate) sender: Option<SyncTriggerSender>,
    pub(crate) clones: Arc<HashMap<String, CloneInfo>>,
    pub(crate) remote_locks: Arc<HashMap<String, Arc<Mutex<()>>>>,
}

/// Spawn the repo-sync manager. Returns `None` sender if no sync-enabled clones
/// are configured; the shared clone index and locks are always returned.
pub(crate) async fn start_repo_sync(
    db: brenn_lib::db::Db,
    active_bridges: ActiveBridges,
    alert_dispatcher: AlertDispatcher,
    repos: &[brenn_lib::config::RepoDeclRaw],
    repo_sync_config: &RepoSyncConfig,
    apps: &Arc<IndexMap<String, AppConfig>>,
) -> RepoSyncResult {
    // Build the shared clone index + per-remote locks once. Both are handed to
    // the manager (when it spawns) and to the git-repo-pull tool.
    let clones: Arc<HashMap<String, CloneInfo>> = Arc::new(build_clone_index(repos, apps));
    let remote_locks: Arc<HashMap<String, Arc<Mutex<()>>>> = Arc::new(
        build_remote_to_slugs(&clones)
            .keys()
            .map(|r| (r.clone(), Arc::new(Mutex::new(()))))
            .collect(),
    );

    let mgr = RepoSyncManager::start(
        db,
        active_bridges,
        alert_dispatcher,
        clones.clone(),
        remote_locks.clone(),
        repo_sync_config,
        apps,
    )
    .await;

    let sender = mgr.as_ref().map(|m| m.sender.clone());

    RepoSyncResult {
        sender,
        clones,
        remote_locks,
    }
}

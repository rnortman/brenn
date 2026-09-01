//! Repo-sync manager.
//!
//! Keeps mounted git repos fresh and notifies live/idle agents when their
//! clones advance.
//!
//! Architecture at a glance:
//!
//! ```text
//!   Poller / Push  ── SyncTrigger ──▶  manager task
//!                                                    │
//!                                           per-remote Mutex
//!                                                    │
//!                                      for each sync-enabled clone:
//!                                        host-side pull_clone()
//!                                        classify PullOutcome
//!                                        for each consumer conversation:
//!                                          always enqueue (durable)
//!                                          if bridge alive: live-inject
//! ```
//!
//! **Poll and push detection paths, one reaction pipeline, one delivery
//! pipeline.** Webhook-driven pulls arrive as `Push` triggers fired by the
//! `git-repo-pull` tool, which the WASM git-sync pipeline invokes.

mod poller;
mod reactor;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use brenn_db::Db;
use brenn_git::sync::{CloneInfo, SyncTrigger, SyncTriggerSender};
use brenn_lib::config::{AccessLevel, AppConfig, RepoDeclRaw, RepoSyncConfig};
use brenn_obs::alerting::AlertDispatcher;
use indexmap::IndexMap;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::active_bridge::ActiveBridges;

/// Per-slug tracking for the escalation policy on AuthError and
/// TransientError outcomes.
///
/// Escalation policy:
///
/// - AuthError threshold: **1**. First occurrence fires an operator alert.
/// - TransientError threshold: **4**. Fires after four consecutive cycles
///   (~20 min at the default 5-min poll).
/// - Any non-auth / non-transient outcome (UpToDate / Advanced / Conflict)
///   resets *both* trackers for that slug — a cycle that successfully
///   talked to the remote and came back with data is proof the failure
///   mode ended, whichever it was.
/// - `alerted` gates re-firing within a single continuous run of failures,
///   so a long outage produces exactly one alert per class, not one every
///   threshold-N cycles. The flag clears only when the counter resets
///   (i.e. on a genuine recovery). Next fresh incident gets a fresh alert.
#[derive(Debug, Default)]
pub struct PersistentFailureState {
    /// slug → transient-failure tracker.
    pub transient: HashMap<String, FailureTracker>,
    /// slug → auth-failure tracker.
    pub auth: HashMap<String, FailureTracker>,
}

#[derive(Debug, Default)]
pub struct FailureTracker {
    /// Number of consecutive cycles the matching failure class has fired.
    /// Grows unbounded during an outage; reset to 0 on any non-matching
    /// outcome.
    pub consecutive: u32,
    /// `true` once we've fired the operator alert for the current
    /// continuous run. Cleared when `consecutive` resets. Prevents
    /// re-paging mid-incident while still allowing the next fresh
    /// incident to page.
    pub alerted: bool,
}

/// Shared runtime context for the reaction pipeline. Clone-able (Arcs).
#[derive(Clone)]
pub struct RepoSyncCtx {
    pub db: Db,
    pub active_bridges: ActiveBridges,
    pub alert_dispatcher: AlertDispatcher,
    /// Clone metadata keyed by slug.
    pub clones: Arc<HashMap<String, CloneInfo>>,
    /// Remote → clone slugs that share it.
    pub remote_to_slugs: Arc<HashMap<String, Vec<String>>>,
    /// Per-remote serialization mutex. Different remotes sync
    /// concurrently; cycles on the same remote serialize.
    pub remote_locks: Arc<HashMap<String, Arc<Mutex<()>>>>,
    /// Per-clone last-notified HEAD SHA. A cycle that finds the current
    /// HEAD differs from this value synthesizes an `Advanced` event,
    /// regardless of cause (poll pull, MCP pull, external Bash commit,
    /// operator edit on disk).
    ///
    /// Populated lazily on first cycle per clone (cold-start seed = no
    /// event fired — we just record where we stand). `std::sync::Mutex`
    /// because we never hold it across awaits.
    pub last_notified_head: Arc<std::sync::Mutex<HashMap<String, String>>>,
    /// Per-slug AuthError / TransientError trackers driving the
    /// operator-alert escalation. Populated lazily; a slug that never
    /// fails never has an entry. `std::sync::Mutex` — never held across
    /// awaits.
    pub failure_state: Arc<std::sync::Mutex<PersistentFailureState>>,
    /// Resolved app configs, for looking up post-pull hook definitions.
    pub apps: Arc<IndexMap<String, brenn_lib::config::AppConfig>>,
    /// Per-app mutex for coalescing concurrent post-pull hook invocations.
    /// Keyed by app slug. Built at startup from all apps that have
    /// non-empty `post_pull_hooks`. If a hook is already running for an
    /// app, the next trigger skips rather than queuing — the running hook
    /// already sees the latest repo state.
    pub post_pull_hook_locks: Arc<HashMap<String, Arc<Mutex<()>>>>,
    // NOTE: drain-time staleness is NOT stored here. `main.rs` forwards
    // `[repo_sync].stale_conversation_days` to the process-global atomic
    // in `event_queue` at startup; the drain code reads it from there.
    // Having two copies would create a divergence footgun.
    /// Test-only gate injected before the fan-out loop in
    /// `run_cycle_for_remote`. Initialized with 0 permits to block the
    /// fan-out; tests call `add_permits(1)` to unblock. `None` in
    /// production (field is `#[cfg(test)]` so zero runtime cost).
    #[cfg(test)]
    pub pre_fanout_gate: Option<Arc<tokio::sync::Semaphore>>,
    /// Test-only notify fired after `drop(guard)` and before the
    /// `pre_fanout_gate` check, but ONLY when `pending` is non-empty (same
    /// condition as the gate). Tests use this to know the spawned cycle has
    /// released the lock and is about to block at the gate, so a concurrent
    /// cycle can be dispatched at the right moment (provably after lock
    /// release, not earlier). `None` in production.
    #[cfg(test)]
    pub post_lock_release_notify: Option<Arc<tokio::sync::Notify>>,
}

/// Top-level manager — one per Brenn instance. Owns the spawned task handle.
pub struct RepoSyncManager {
    pub sender: SyncTriggerSender,
    #[allow(dead_code)] // Held so the task stays alive; not currently awaited.
    pub task: JoinHandle<()>,
    #[allow(dead_code)] // Retained for tests / observability inspection.
    pub ctx: RepoSyncCtx,
}

impl RepoSyncManager {
    /// Build the manager from the validated config and wire it up.
    ///
    /// - Computes the clone index from `&apps` (which, post-validation,
    ///   carries the authoritative `primary` flag per mount).
    /// - Builds per-remote mutexes.
    /// - Spawns the manager task (poll loop + reactor dispatch).
    /// - Fires a cold-start `Poll` for every unique remote so Brenn is
    ///   current before we start serving traffic. Non-blocking.
    ///
    /// Returns `None` when no sync-enabled clones exist — a defensible
    /// "feature disabled" state. Saves spawning an idle task.
    pub async fn start(
        db: Db,
        active_bridges: ActiveBridges,
        alert_dispatcher: AlertDispatcher,
        clones: Arc<HashMap<String, CloneInfo>>,
        remote_locks: Arc<HashMap<String, Arc<Mutex<()>>>>,
        repo_sync_cfg: &RepoSyncConfig,
        apps: &Arc<IndexMap<String, AppConfig>>,
    ) -> Option<Self> {
        if clones.is_empty() {
            info!("repo_sync: no clones configured — manager not spawned");
            return None;
        }

        // If no clone is sync-enabled, we could still technically accept
        // webhooks for audit, but the design gates the entire feature on
        // auto_pull. Skip spawning. The shared clone index and per-remote
        // locks still exist (built by the caller) so tool-driven pulls work.
        if !clones.values().any(|c| c.sync_enabled) {
            info!(
                clones = clones.len(),
                "repo_sync: no sync-enabled clones — manager not spawned"
            );
            return None;
        }

        let remote_to_slugs = build_remote_to_slugs(&clones);

        // Seed the in-memory `last_notified_head` cache from persisted
        // cursors. On cold boot the table is empty and seeding is a no-op,
        // matching the old "start empty, seed-on-first-cycle" behavior.
        // After a restart with prior cursor rows, we pick up where we left
        // off and don't fire a false "everything moved" alert storm.
        let seeded_cursor = {
            let conn = db.lock().await;
            brenn_messaging::repo_sync_cursor::load_all(&conn)
        };

        // Build per-app coalescing mutexes for post-pull hooks.
        let post_pull_hook_locks: HashMap<String, Arc<Mutex<()>>> = apps
            .iter()
            .filter(|(_, app)| {
                !app.post_pull_hooks.host.is_empty() || !app.post_pull_hooks.container.is_empty()
            })
            .map(|(slug, _)| (slug.clone(), Arc::new(Mutex::new(()))))
            .collect();

        let ctx = RepoSyncCtx {
            db,
            active_bridges,
            alert_dispatcher,
            clones,
            remote_to_slugs: Arc::new(remote_to_slugs),
            remote_locks,
            last_notified_head: Arc::new(std::sync::Mutex::new(seeded_cursor)),
            failure_state: Arc::new(std::sync::Mutex::new(PersistentFailureState::default())),
            apps: apps.clone(),
            post_pull_hook_locks: Arc::new(post_pull_hook_locks),
            #[cfg(test)]
            pre_fanout_gate: None,
            #[cfg(test)]
            post_lock_release_notify: None,
        };

        // Build a static slug → remote index. Held inside the sender so
        // MCP-tool fast-path call sites can look up by slug without
        // threading the clones map through every bridge.
        let slug_to_remote: Arc<HashMap<String, String>> = Arc::new(
            ctx.clones
                .values()
                .map(|c| (c.slug.clone(), c.remote.clone()))
                .collect(),
        );

        // Channel capacity per design: 16 * num_remotes. With coalescing
        // this is ample; overflow causes a warn-and-drop.
        let capacity = (16 * ctx.remote_to_slugs.len()).max(16);
        let (tx, rx) = mpsc::channel::<SyncTrigger>(capacity);

        let poll_interval = Duration::from_secs(repo_sync_cfg.poll_interval_secs);

        let manager_ctx = ctx.clone();
        let task = tokio::spawn(async move {
            manager_loop(manager_ctx, rx).await;
        });

        // Poll loop: fires `SyncTrigger::Poll` periodically for every remote.
        {
            let tx = tx.clone();
            let remotes: Vec<String> = ctx.remote_to_slugs.keys().cloned().collect();
            tokio::spawn(poller::poll_loop(remotes, poll_interval, tx));
        }

        // Cold-start: fire one Poll per unique remote synchronously. Each
        // go through the normal pipeline; non-blocking thanks to mpsc.
        {
            let sender = tx.clone();
            for remote in ctx.remote_to_slugs.keys() {
                // try_send is fine — capacity comfortably exceeds remote count.
                if let Err(e) = sender.try_send(SyncTrigger::Poll {
                    remote: remote.clone(),
                }) {
                    warn!(remote = %remote, error = ?e, "cold-start trigger dropped");
                }
            }
        }

        info!(
            remotes = ctx.remote_to_slugs.len(),
            clones = ctx.clones.len(),
            sync_enabled = ctx.clones.values().filter(|c| c.sync_enabled).count(),
            poll_interval_secs = repo_sync_cfg.poll_interval_secs,
            "repo_sync manager started"
        );

        Some(Self {
            sender: SyncTriggerSender::new(tx, slug_to_remote),
            task,
            ctx,
        })
    }
}

/// Main manager loop. Receives triggers and dispatches each to the reactor.
///
/// For MVP the loop is simple: each trigger spawns a per-remote task (the
/// per-remote Mutex serializes cycles on the same remote). Debouncing is
/// explicitly NOT applied here — the design's debounce/coalesce layer is a
/// Phase-2/3 concern once push-fast-path and webhooks are wired; polling
/// alone can't stampede (one tick per interval).
async fn manager_loop(ctx: RepoSyncCtx, mut rx: mpsc::Receiver<SyncTrigger>) {
    while let Some(trigger) = rx.recv().await {
        let ctx_cycle = ctx.clone();
        tokio::spawn(async move {
            reactor::run_cycle(ctx_cycle, trigger).await;
        });
    }
    info!("repo_sync manager loop exiting (channel closed)");
}

/// Build the slug → CloneInfo index from the post-validation app set.
///
/// Every slug that appears in any `ResolvedMount` becomes a clone entry.
/// `remote` comes from `repos` (lookup by slug). `sync_enabled` is true
/// iff any mount of the slug has `auto_pull = true`. `consumer_apps`
/// captures every app that mounts the slug; `primary_apps` captures the
/// subset where the mount is flagged primary (post-validation, that's at
/// most one app per clone — but it lives here as a set for uniform
/// downstream handling and future multiuser edge cases).
pub fn build_clone_index(
    repos: &[RepoDeclRaw],
    apps: &IndexMap<String, AppConfig>,
) -> HashMap<String, CloneInfo> {
    let slug_to_remote: HashMap<&str, &str> = repos
        .iter()
        .map(|r| (r.slug.as_str(), r.remote.as_str()))
        .collect();

    let mut clones: HashMap<String, CloneInfo> = HashMap::new();

    for app in apps.values() {
        for mount in &app.mounts {
            // Every mounted slug must be in `repos`; startup validation
            // already guarantees that. Defensive lookup — panic here
            // would be a Brenn bug, not a config bug.
            let remote = slug_to_remote
                .get(mount.slug.as_str())
                .unwrap_or_else(|| {
                    panic!(
                        "BUG: mount {:?} in app {:?} has no [[repo]] entry — \
                         validate_and_resolve should have rejected this",
                        mount.slug, app.slug,
                    )
                })
                .to_string();

            let entry = clones
                .entry(mount.slug.clone())
                .or_insert_with(|| CloneInfo {
                    slug: mount.slug.clone(),
                    host_path: mount.host_path.clone(),
                    remote: remote.clone(),
                    sync_enabled: false,
                    consumer_apps: HashSet::new(),
                    primary_apps: HashSet::new(),
                });

            entry.sync_enabled = entry.sync_enabled || mount.auto_pull;
            entry.consumer_apps.insert(app.slug.clone());
            if mount.primary && mount.access == AccessLevel::ReadWrite {
                entry.primary_apps.insert(app.slug.clone());
            }

            // Sanity: every mount of this slug must report the same remote
            // URL (config grouping), otherwise we'd have a broken clone
            // identity. validate_and_resolve doesn't check this directly,
            // but the cross-app slug mapping is by `[[repo]].slug`, so
            // divergence here would imply a config bug. Assert.
            assert_eq!(
                entry.remote, remote,
                "BUG: slug {:?} has inconsistent remote across apps ({:?} vs {:?})",
                mount.slug, entry.remote, remote,
            );

            // Host path should match too — same reasoning.
            assert_eq!(
                entry.host_path, mount.host_path,
                "BUG: slug {:?} has inconsistent host_path across apps",
                mount.slug,
            );
        }
    }

    clones
}

/// Invert the clone index: remote URL → slugs that share it. A remote
/// with no mounting apps is absent (no work to do there).
pub fn build_remote_to_slugs(clones: &HashMap<String, CloneInfo>) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for info in clones.values() {
        out.entry(info.remote.clone())
            .or_default()
            .push(info.slug.clone());
    }
    // Deterministic ordering for logging / cold-start reproducibility.
    for slugs in out.values_mut() {
        slugs.sort();
    }
    out
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use brenn_lib::config::{AccessLevel, AppConfig, CompactionConfig, RepoDeclRaw, ResolvedMount};

    fn mk_repo(slug: &str, remote: &str, auto_pull: bool) -> RepoDeclRaw {
        RepoDeclRaw {
            slug: slug.to_string(),
            remote: remote.to_string(),
            auto_pull,
        }
    }

    fn mk_mount(
        slug: &str,
        host_path: PathBuf,
        access: AccessLevel,
        auto_pull: bool,
        primary: bool,
    ) -> ResolvedMount {
        ResolvedMount {
            slug: slug.to_string(),
            host_path,
            container_path: None,
            access,
            auto_pull,
            is_working_dir: false,
            primary,
        }
    }

    /// Minimal AppConfig stub for `build_clone_index` input. Only the fields
    /// the function reads (`slug`, `mounts`) need to be meaningful.
    fn mk_app(slug: &str, mounts: Vec<ResolvedMount>) -> AppConfig {
        AppConfig {
            working_dir: PathBuf::from("/tmp"),
            compaction: None::<CompactionConfig>,
            mounts,
            state_dir: PathBuf::from("/tmp/state"),
            ..brenn_lib::config::test_app_config(slug)
        }
    }

    #[test]
    fn build_clone_index_single_app_single_mount() {
        let repos = vec![mk_repo("life", "ssh://example/life.git", true)];
        let apps: IndexMap<String, AppConfig> = [(
            "appa".to_string(),
            mk_app(
                "appa",
                vec![mk_mount(
                    "life",
                    PathBuf::from("/repos/life"),
                    AccessLevel::ReadWrite,
                    true,
                    true, // primary
                )],
            ),
        )]
        .into();
        let clones = build_clone_index(&repos, &apps);
        let info = clones.get("life").unwrap();
        assert_eq!(info.remote, "ssh://example/life.git");
        assert_eq!(info.host_path, PathBuf::from("/repos/life"));
        assert!(info.sync_enabled);
        assert_eq!(info.consumer_apps, ["appa".to_string()].into());
        assert_eq!(info.primary_apps, ["appa".to_string()].into());
    }

    #[test]
    fn build_clone_index_multi_app_shared_clone_aggregates_consumers() {
        // Two apps mount the same clone — consumer_apps collects both.
        // Only the primary-declared RW mount goes into primary_apps.
        let repos = vec![mk_repo("life", "ssh://example/life.git", true)];
        let apps: IndexMap<String, AppConfig> = [
            (
                "appa".to_string(),
                mk_app(
                    "appa",
                    vec![mk_mount(
                        "life",
                        PathBuf::from("/repos/life"),
                        AccessLevel::ReadWrite,
                        true,
                        true, // primary
                    )],
                ),
            ),
            (
                "appb".to_string(),
                mk_app(
                    "appb",
                    vec![mk_mount(
                        "life",
                        PathBuf::from("/repos/life"),
                        AccessLevel::ReadOnly,
                        true,
                        false,
                    )],
                ),
            ),
        ]
        .into();
        let clones = build_clone_index(&repos, &apps);
        let info = clones.get("life").unwrap();
        assert_eq!(
            info.consumer_apps,
            ["appa".to_string(), "appb".to_string()].into()
        );
        // Only appa is primary; appb's RO mount can't be primary.
        assert_eq!(info.primary_apps, ["appa".to_string()].into());
    }

    #[test]
    fn build_clone_index_sync_enabled_is_or_across_mounts() {
        // Mix of auto_pull=true and auto_pull=false across apps — clone
        // ends up sync_enabled iff *any* mount has auto_pull=true.
        let repos = vec![mk_repo("life", "ssh://example/life.git", true)];
        let apps: IndexMap<String, AppConfig> = [
            (
                "opted-out".to_string(),
                mk_app(
                    "opted-out",
                    vec![mk_mount(
                        "life",
                        PathBuf::from("/repos/life"),
                        AccessLevel::ReadOnly,
                        false, // auto_pull = false
                        false,
                    )],
                ),
            ),
            (
                "opted-in".to_string(),
                mk_app(
                    "opted-in",
                    vec![mk_mount(
                        "life",
                        PathBuf::from("/repos/life"),
                        AccessLevel::ReadWrite,
                        true, // auto_pull = true
                        true,
                    )],
                ),
            ),
        ]
        .into();
        let clones = build_clone_index(&repos, &apps);
        assert!(clones.get("life").unwrap().sync_enabled);
    }

    #[test]
    fn build_clone_index_primary_is_rw_only() {
        // Defense-in-depth: even if a RO mount somehow had `primary = true`
        // (validate_and_resolve would have rejected it, but buggy callers
        // might bypass), build_clone_index does NOT put it in primary_apps.
        let repos = vec![mk_repo("life", "ssh://example/life.git", true)];
        let apps: IndexMap<String, AppConfig> = [(
            "appa".to_string(),
            mk_app(
                "appa",
                vec![mk_mount(
                    "life",
                    PathBuf::from("/repos/life"),
                    AccessLevel::ReadOnly,
                    true,
                    true, // primary=true on RO — should be ignored
                )],
            ),
        )]
        .into();
        let clones = build_clone_index(&repos, &apps);
        assert!(
            clones.get("life").unwrap().primary_apps.is_empty(),
            "primary_apps must exclude RO mounts even if flagged",
        );
    }

    #[test]
    #[should_panic(expected = "no [[repo]] entry")]
    fn build_clone_index_orphan_mount_panics() {
        // validate_and_resolve rejects mounts that reference missing
        // [[repo]] entries. If one somehow reaches here it's a BUG; panic.
        let repos: Vec<RepoDeclRaw> = vec![];
        let apps: IndexMap<String, AppConfig> = [(
            "appa".to_string(),
            mk_app(
                "appa",
                vec![mk_mount(
                    "orphan",
                    PathBuf::from("/repos/orphan"),
                    AccessLevel::ReadWrite,
                    true,
                    true,
                )],
            ),
        )]
        .into();
        build_clone_index(&repos, &apps);
    }

    #[test]
    fn build_remote_to_slugs_inverts_single_shared_remote() {
        // Two clones of the same remote (graf / graf-review pattern).
        let remote = "ssh://example/graf.git";
        let mut clones = HashMap::new();
        clones.insert(
            "graf".to_string(),
            CloneInfo {
                slug: "graf".to_string(),
                host_path: PathBuf::from("/repos/graf"),
                remote: remote.to_string(),
                sync_enabled: true,
                consumer_apps: HashSet::new(),
                primary_apps: HashSet::new(),
            },
        );
        clones.insert(
            "graf-review".to_string(),
            CloneInfo {
                slug: "graf-review".to_string(),
                host_path: PathBuf::from("/repos/graf-review"),
                remote: remote.to_string(),
                sync_enabled: true,
                consumer_apps: HashSet::new(),
                primary_apps: HashSet::new(),
            },
        );
        let inv = build_remote_to_slugs(&clones);
        assert_eq!(inv.len(), 1);
        // Both slugs under the shared remote, sorted for determinism.
        assert_eq!(
            inv.get(remote).unwrap(),
            &vec!["graf".to_string(), "graf-review".to_string()]
        );
    }

    #[test]
    fn build_remote_to_slugs_distinct_remotes_are_separate_keys() {
        let mut clones = HashMap::new();
        clones.insert(
            "life".to_string(),
            CloneInfo {
                slug: "life".to_string(),
                host_path: PathBuf::from("/repos/life"),
                remote: "ssh://life".to_string(),
                sync_enabled: true,
                consumer_apps: HashSet::new(),
                primary_apps: HashSet::new(),
            },
        );
        clones.insert(
            "tech".to_string(),
            CloneInfo {
                slug: "tech".to_string(),
                host_path: PathBuf::from("/repos/tech"),
                remote: "ssh://tech".to_string(),
                sync_enabled: true,
                consumer_apps: HashSet::new(),
                primary_apps: HashSet::new(),
            },
        );
        let inv = build_remote_to_slugs(&clones);
        assert_eq!(inv.len(), 2);
        assert_eq!(inv.get("ssh://life").unwrap(), &vec!["life".to_string()]);
        assert_eq!(inv.get("ssh://tech").unwrap(), &vec!["tech".to_string()]);
    }
}

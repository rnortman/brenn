//! Repo-sync manager.
//!
//! Keeps mounted git repos fresh and notifies live/idle agents when their
//! clones advance. See `docs/designs/repo-sync.md` for the full design.
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

pub mod git;
mod poller;
mod reactor;
#[cfg(test)]
pub(crate) mod test_git_fixtures;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use brenn_lib::config::{AccessLevel, AppConfig, RepoDeclRaw, RepoSyncConfig};
use brenn_lib::db::Db;
use brenn_lib::obs::alerting::AlertDispatcher;
use indexmap::IndexMap;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::active_bridge::ActiveBridges;

// `PullOutcome` and `pull_clone` are public from `git.rs` but the only
// current in-tree consumer is `reactor.rs` (via a direct path import),
// so we don't re-export them here.

/// UTF-8-safe truncation for stderr / alert detail strings. When `s.len()`
/// exceeds `max` bytes, cuts at the largest char boundary `<= max - 3` and
/// appends `"..."`. Shared by the git-plumbing (fetch/merge stderr trim)
/// and the reactor (alert-body trim) so those two paths stay consistent.
pub(crate) fn truncate_detail(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let boundary = s.floor_char_boundary(max.saturating_sub(3));
    format!("{}...", &s[..boundary])
}

/// Why a sync cycle should run.
///
/// All variants converge on the same reaction pipeline; they only differ in
/// whether they bypass debounce (Push) or go through it (Poll), and in the
/// resume-time variant's scoped remotes list.
#[derive(Debug, Clone)]
pub enum SyncTrigger {
    /// Periodic poll tick for one remote. Debounced.
    Poll { remote: String },
    /// An agent just mutated a clone via an MCP git tool (`GitRepoPull` or
    /// `GitRepoCommitAndPush`). Bypasses debounce. `acting_conversation_id`
    /// is suppressed from the notification fan-out so the invoking bridge
    /// doesn't get a `repo_sync:pulled` for its own change.
    Push {
        remote: String,
        acting_conversation_id: Option<i64>,
    },
    /// A conversation just resumed. Run one cycle for each remote mounted by
    /// the app, so freshness is guaranteed before the bridge starts processing.
    #[allow(dead_code)] // Wired in Phase 4.
    ResumePoke { remotes: Vec<String> },
}

/// Handle returned by `RepoSyncManager::start`. Owning code (main.rs) keeps
/// it to inject triggers; dropping it does not kill the manager task (the
/// receiver lives inside the task).
#[derive(Clone)]
pub struct SyncTriggerSender {
    tx: mpsc::Sender<SyncTrigger>,
    /// Clone slug → remote URL. Built once at startup from the config.
    slug_to_remote: Arc<HashMap<String, String>>,
}

impl SyncTriggerSender {
    /// Test-only constructor. Production builds get the sender from
    /// `RepoSyncManager::start`.
    #[cfg(test)]
    pub(crate) fn new_for_test(
        tx: mpsc::Sender<SyncTrigger>,
        slug_to_remote: Arc<HashMap<String, String>>,
    ) -> Self {
        Self { tx, slug_to_remote }
    }

    /// Try to send a trigger. Non-blocking; a full channel drops the
    /// trigger with a warn — triggers are coalescable and polling will
    /// catch up on the next tick. Returns `true` on success, `false` if
    /// the channel was full and the trigger was dropped.
    pub fn try_send(&self, trigger: SyncTrigger) -> bool {
        match self.tx.try_send(trigger) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(t)) => {
                warn!(?t, "repo_sync trigger channel full — dropping");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!(
                    "repo_sync trigger channel closed — repo_sync manager task died; \
                     process cannot continue safely"
                );
            }
        }
    }

    /// Emit a `SyncTrigger::Push` for the clone identified by `slug`.
    /// Unknown slug is warned and dropped — it would indicate a
    /// config/code mismatch.
    pub fn push_for_slug(&self, slug: &str, acting_conversation_id: Option<i64>) {
        let Some(remote) = self.slug_to_remote.get(slug) else {
            warn!(
                slug = %slug,
                "repo_sync: push_for_slug on unknown slug — no trigger emitted"
            );
            return;
        };
        // Discard intentional: Full case is already logged by try_send as a
        // warn. Push triggers are coalescable; the poll loop catches up.
        let _delivered = self.try_send(SyncTrigger::Push {
            remote: remote.clone(),
            acting_conversation_id,
        });
    }
}

/// Per-clone static info derived from `validate_and_resolve`.
/// Consumer lookup happens at delivery time against this index
/// plus a live query of `conversations`.
#[derive(Debug, Clone)]
pub struct CloneInfo {
    pub slug: String,
    pub host_path: PathBuf,
    pub remote: String,
    /// `true` if *any* mount of this clone has `auto_pull = true`.
    /// Drives sync-enabled-ness per the design.
    pub sync_enabled: bool,
    /// Apps that mount this clone (any access, any auto_pull).
    /// Every active conversation of an app in this set is a consumer
    /// for notification purposes.
    pub consumer_apps: HashSet<String>,
    /// Apps whose mount of this clone is the declared primary (the
    /// primary-pool). Conflict notifications go only to consumers in
    /// conversations of these apps.
    ///
    /// Empty set → RO-only clone; conflicts route to `AlertDispatcher`
    /// instead of LLM events.
    pub primary_apps: HashSet<String>,
}

/// Per-slug tracking for the escalation policy on AuthError and
/// TransientError outcomes.
///
/// Semantics (per `docs/designs/repo-sync-auth-and-host-unification.md`
/// §"Escalation policy"):
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
    /// operator edit on disk). See "Phase 2 Part A" in the design doc.
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
            brenn_lib::repo_sync_cursor::load_all(&conn)
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
            sender: SyncTriggerSender { tx, slug_to_remote },
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
pub(crate) fn build_clone_index(
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
pub(crate) fn build_remote_to_slugs(
    clones: &HashMap<String, CloneInfo>,
) -> HashMap<String, Vec<String>> {
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
    use super::*;
    use brenn_lib::config::{
        AccessLevel, AppConfig, CompactionConfig, PathMapper, RepoDeclRaw, ResolvedMount,
        StartHooksConfig,
    };

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
            slug: slug.to_string(),
            name: slug.to_string(),
            description: String::new(),
            icon: String::new(),
            working_dir: PathBuf::from("/tmp"),
            model: "sonnet".to_string(),
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout: None,
            compaction: None::<CompactionConfig>,
            idle_hook_secs: 0,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: false,
            prefix_timestamp: false,
            prefix_device: true,
            path_mapper: PathMapper::Identity,
            container_spawn: None,
            start_hooks: StartHooksConfig::default(),
            post_pull_hooks: brenn_lib::config::PostPullHooksConfig::default(),
            startup_hooks: brenn_lib::config::StartupHooksConfig::default(),
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: HashMap::new(),
            mounts,
            history_replay_limit: 2000,
            frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
            state_dir: PathBuf::from("/tmp/state"),
            messaging: None,
            messaging_default_send_budget: 100,
            policy: brenn_lib::access::AppPolicy::default(),
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
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
    fn push_for_slug_emits_trigger_for_known_slug() {
        // Build a SyncTriggerSender with a small slug_to_remote map and
        // exercise the fast path directly (no tokio runtime needed for
        // try_send on a bounded channel).
        let (tx, mut rx) = mpsc::channel::<SyncTrigger>(4);
        let slug_to_remote = Arc::new(HashMap::from([(
            "src-x".to_string(),
            "ssh://example/x.git".to_string(),
        )]));
        let sender = SyncTriggerSender::new_for_test(tx, slug_to_remote);
        sender.push_for_slug("src-x", Some(42));
        match rx.try_recv() {
            Ok(SyncTrigger::Push {
                remote,
                acting_conversation_id,
            }) => {
                assert_eq!(remote, "ssh://example/x.git");
                assert_eq!(acting_conversation_id, Some(42));
            }
            other => panic!("expected Push, got {other:?}"),
        }
    }

    #[test]
    fn push_for_slug_drops_unknown_slug_without_panicking() {
        let (tx, mut rx) = mpsc::channel::<SyncTrigger>(4);
        let slug_to_remote = Arc::new(HashMap::new());
        let sender = SyncTriggerSender::new_for_test(tx, slug_to_remote);
        sender.push_for_slug("unknown", Some(1));
        assert!(rx.try_recv().is_err(), "unknown slug should emit nothing");
    }

    #[test]
    fn try_send_returns_false_when_channel_full() {
        // Channel of capacity 1; fill it, then confirm try_send returns false.
        let (tx, _rx) = mpsc::channel::<SyncTrigger>(1);
        let slug_to_remote = Arc::new(HashMap::new());
        let sender = SyncTriggerSender::new_for_test(tx.clone(), slug_to_remote);
        // Fill the channel.
        tx.try_send(SyncTrigger::Poll {
            remote: "ssh://example/x.git".to_string(),
        })
        .expect("first send into empty channel must succeed");
        // Now channel is full; try_send must return false.
        let delivered = sender.try_send(SyncTrigger::Poll {
            remote: "ssh://example/x.git".to_string(),
        });
        assert!(!delivered, "try_send must return false when channel full");
    }

    #[test]
    fn push_for_slug_returns_normally_when_channel_full() {
        // Fill the channel to capacity, then confirm push_for_slug does not
        // panic (it discards intentionally; the warn inside try_send fires).
        let (tx, mut rx) = mpsc::channel::<SyncTrigger>(1);
        let slug_to_remote = Arc::new(HashMap::from([(
            "src-x".to_string(),
            "ssh://example/x.git".to_string(),
        )]));
        let sender = SyncTriggerSender::new_for_test(tx.clone(), slug_to_remote);
        // Pre-fill the channel.
        tx.try_send(SyncTrigger::Poll {
            remote: "ssh://example/x.git".to_string(),
        })
        .expect("pre-fill must succeed");
        // push_for_slug must not panic and must not squeeze an extra item in.
        sender.push_for_slug("src-x", None);
        // Drain the one pre-filled item; nothing else should be present.
        let _ = rx.try_recv().expect("pre-filled item must be present");
        assert!(
            rx.try_recv().is_err(),
            "channel must contain no extra item after dropped push"
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

//! Idle hooks: a unified per-bridge timer that fires when both CC and the
//! UI have gone quiet, runs registered hooks, and delivers their aggregated
//! output to CC as a single compact-JSON system message.
//!
//! See `docs/designs/idle-hooks.md` for the full design.
//!
//! The bridge owns the timer and registration. This module holds the
//! `IdleHook` trait, the first concrete hook (`DirtyRepoHook`), and the
//! free functions `run_idle_hooks` / `run_idle_hooks_for_shutdown` that
//! drive a fire cycle.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tracing::{debug, error, info, warn};

use crate::active_bridge::ActiveBridge;

/// Outer bound for the shutdown-path hook fan-out. Individual git queries
/// inside `repo_status` are already bounded by the per-`git` 30 s timeout
/// in `git_ops`; this 60 s ceiling covers the concurrent fan-out plus
/// headroom.
const SHUTDOWN_HOOK_RUN_TIMEOUT: Duration = Duration::from_secs(60);

/// How long `run_idle_hooks_for_shutdown` will wait for compaction to
/// return to `Normal` before giving up and proceeding to drain. The
/// expected case is that compaction was the reason CC went busy in the
/// first place; once it finishes we want to fire hooks normally.
const SHUTDOWN_COMPACTION_WAIT: Duration = Duration::from_secs(30);

/// Trait for idle hooks. All methods take `&self`; implementations use
/// interior mutability (`AtomicBool`, `Mutex`) for any per-cycle state.
pub trait IdleHook: Send + Sync {
    /// JSON key under which this hook's output appears in the aggregated
    /// system message. Also used in logs.
    fn name(&self) -> &str;

    /// Minimum idle seconds before this hook should fire. `None` =
    /// use the app default. `Some(n)` is asserted `>= app default` at
    /// registration time.
    ///
    /// In the shared-timer floor computation, `None` is skipped; only
    /// `Some` values participate in the inner max. The outer
    /// `max(app_default, ...)` handles the empty case.
    fn min_idle_secs(&self) -> Option<u64> {
        None
    }

    /// Run the hook. `Some(value)` → contribute to the system message;
    /// `None` → nothing to report this cycle.
    fn check<'a>(
        &'a self,
        bridge: &'a ActiveBridge,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Value>> + Send + 'a>>;

    /// Called after the aggregated message has been accepted by CC.
    /// Use this to set one-shot flags (`reminder_sent = true`).
    fn on_delivered(&self);

    /// Called when `check()` returned `None`. Idempotent — resets one-shot
    /// flags so the hook can fire again if state re-dirties later.
    fn on_resolved(&self);

    /// Cheap "should we bother arming the timer?" predicate. Used at
    /// arm-time to decide whether the timer is worth running, and at
    /// fire-time to decide whether to invoke `check()`. It is **not**
    /// "is there definitely something to report" — that's what `check()`
    /// answers. A hook that needs an actual git/disk query to know
    /// should return `true` here unconditionally and let `check()` be
    /// the source of truth. A hook with a cached state flag (e.g.
    /// `reminder_sent`) can use it to suppress.
    fn has_pending_work(&self) -> bool;
}

/// First concrete hook: nudge the LLM about dirty/unpushed managed repos.
///
/// Routes through `crate::git_ops::repo_status` (the same code path the
/// `GitRepoStatus` MCP tool uses) so the LLM and the hook see identical
/// data. One-shot via `reminder_sent`: fires once per dirty cycle, clears
/// when all managed repos go clean.
pub struct DirtyRepoHook {
    pub reminder_sent: AtomicBool,
}

impl DirtyRepoHook {
    pub fn new() -> Self {
        Self {
            reminder_sent: AtomicBool::new(false),
        }
    }
}

impl Default for DirtyRepoHook {
    fn default() -> Self {
        Self::new()
    }
}

impl IdleHook for DirtyRepoHook {
    fn name(&self) -> &str {
        "dirty_repos"
    }

    fn check<'a>(
        &'a self,
        bridge: &'a ActiveBridge,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<Value>> + Send + 'a>> {
        Box::pin(self.check_impl(bridge))
    }

    fn on_delivered(&self) {
        self.reminder_sent.store(true, Ordering::SeqCst);
    }

    fn on_resolved(&self) {
        // No-op: suppression state is managed inside `check_impl` via
        // `reminder_sent`; the trait's on_resolved lifecycle doesn't
        // apply to this hook.
    }

    fn has_pending_work(&self) -> bool {
        // The hook can't know without a `git status` call whether repos
        // are dirty; suppression decisions also live in `check()`. Return
        // `true` unconditionally so the timer arms after every turn end
        // (assuming no other gating) and let `check()` be the source of
        // truth.
        true
    }
}

impl DirtyRepoHook {
    async fn check_impl(&self, bridge: &ActiveBridge) -> Option<Value> {
        if bridge.mounts.is_empty() {
            return None;
        }

        let working_dir = bridge.working_dir.clone();
        let futs = bridge
            .mounts
            .iter()
            .map(|m| crate::git_ops::repo_status(m, &working_dir));
        let results = futures::future::join_all(futs).await;

        // Collapse into the compact "dirty?" view the LLM cares about.
        // We deliberately do NOT include the file lists — that's noise
        // in a nudge. The LLM can call GitRepoStatus itself if it wants
        // detail before deciding what to commit.
        let mut by_slug = serde_json::Map::new();
        for r in &results {
            if r.error.is_some() {
                // Persistent errors surface through GitRepoStatus / the
                // log path. Don't nag on unknown — skip this repo.
                continue;
            }
            // dirty_files = (`git diff --name-only`) ∪ (untracked).
            // staged_files = `git diff --cached --name-only`.
            // A file that's both staged AND modified-again-after-staging
            // appears in both lists. Union to avoid double-counting; the
            // count is the only quantitative info the LLM gets so make
            // it correct.
            let mut uniq: HashSet<&str> = HashSet::new();
            uniq.extend(r.dirty_files.iter().map(String::as_str));
            uniq.extend(r.staged_files.iter().map(String::as_str));
            let uncommitted = uniq.len();
            let unpushed = r.unpushed_count.unwrap_or(0);
            if uncommitted > 0 || unpushed > 0 {
                by_slug.insert(
                    r.slug.clone(),
                    json!({
                        "uncommitted": uncommitted,
                        "unpushed": unpushed,
                    }),
                );
            }
        }
        if by_slug.is_empty() {
            // All clean. Clear the one-shot ourselves so the next dirty
            // cycle can fire again. (We don't rely on `on_resolved` here
            // because the framework calls `on_resolved` on every `None`,
            // including the suppressed-dirty branch below — see
            // "One-shot semantics" in the design.)
            self.reminder_sent.store(false, Ordering::SeqCst);
            return None;
        }
        if self.reminder_sent.load(Ordering::SeqCst) {
            // Still dirty, but we already nagged. Suppress until repos
            // go clean.
            return None;
        }
        Some(json!({ "by_slug": by_slug }))
    }
}

/// Run all registered hooks under the regular timer-fired path.
///
/// Steps (per `docs/designs/idle-hooks.md` "What `run_idle_hooks` does"):
/// 1. Re-check compaction phase. If anything other than `Normal`, bail —
///    the next post-compaction turn-end will arm a fresh timer.
/// 2. Concurrently call `hook.check()` on every registered hook whose
///    `has_pending_work()` returns `true`.
/// 3. For hooks that returned `None`, call `on_resolved()` (idempotent).
/// 4. For hooks that returned `Some(value)`, aggregate into one JSON
///    object, render via `render_idle_hook` and send via `send_system_message`, and on `Ok` call
///    `on_delivered()` on each contributing hook.
/// 5. If the send fails (CC died), log; the next turn-end (after
///    recovery) will re-evaluate.
pub async fn run_idle_hooks(bridge: &Arc<ActiveBridge>) {
    if !bridge.compaction_phase_is_normal().await {
        debug!(
            conversation_id = bridge.conversation_id,
            "run_idle_hooks: compaction phase != Normal, bailing"
        );
        return;
    }

    let hooks = bridge.snapshot_idle_hooks();
    if hooks.is_empty() {
        return;
    }

    invoke_hooks_and_deliver(bridge, &hooks).await;
}

/// Run hooks during bridge shutdown. Differs from `run_idle_hooks`:
///
/// - Waits up to `SHUTDOWN_COMPACTION_WAIT` for compaction to return to
///   `Normal` (compaction was likely *why* CC was busy). If still not
///   normal at the bound, give up and return — caller proceeds to drain.
/// - The hook fan-out is wrapped in an outer `SHUTDOWN_HOOK_RUN_TIMEOUT`.
/// - Returns after the message is sent (or skipped). The next turn-end
///   path picks up `drain_on_idle` and finishes the kill.
pub async fn run_idle_hooks_for_shutdown(bridge: &Arc<ActiveBridge>) {
    // Wait briefly for compaction to clear. Polling is cheap and the
    // common case (no compaction) returns immediately on the first check.
    let started = std::time::Instant::now();
    while !bridge.compaction_phase_is_normal().await {
        if started.elapsed() >= SHUTDOWN_COMPACTION_WAIT {
            warn!(
                conversation_id = bridge.conversation_id,
                "run_idle_hooks_for_shutdown: compaction did not clear within \
                 {SHUTDOWN_COMPACTION_WAIT:?}, proceeding to drain without firing"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let hooks = bridge.snapshot_idle_hooks();
    if hooks.is_empty() {
        return;
    }

    // `tokio::time::timeout` polls the inner future inline; no spawn,
    // no `'static` requirement, so we can borrow `bridge` and `hooks`.
    let result = tokio::time::timeout(
        SHUTDOWN_HOOK_RUN_TIMEOUT,
        invoke_hooks_and_deliver(bridge, &hooks),
    )
    .await;
    if result.is_err() {
        warn!(
            conversation_id = bridge.conversation_id,
            "run_idle_hooks_for_shutdown: outer timeout ({SHUTDOWN_HOOK_RUN_TIMEOUT:?}) elapsed; \
             proceeding to drain"
        );
    }
}

/// Shared inner: invoke each hook with `has_pending_work() == true`,
/// aggregate `Some` results into one `{"system":"idle_hooks", ...}`
/// object, send it, and call lifecycle hooks based on the outcome.
async fn invoke_hooks_and_deliver(bridge: &Arc<ActiveBridge>, hooks: &[Arc<dyn IdleHook>]) {
    // Filter to hooks that report pending work.
    let active: Vec<&Arc<dyn IdleHook>> = hooks.iter().filter(|h| h.has_pending_work()).collect();

    if active.is_empty() {
        return;
    }

    // Concurrent fan-out: each hook owns its own state, so independent
    // futures are safe.
    let futs = active.iter().map(|h| async move {
        let value = h.check(bridge).await;
        (*h, value)
    });
    let results: Vec<(&Arc<dyn IdleHook>, Option<Value>)> = futures::future::join_all(futs).await;

    let mut envelope = serde_json::Map::new();
    let mut contributors: Vec<&Arc<dyn IdleHook>> = Vec::new();
    for (hook, value) in results {
        match value {
            Some(v) => {
                envelope.insert(hook.name().to_string(), v);
                contributors.push(hook);
            }
            None => {
                hook.on_resolved();
            }
        }
    }

    if envelope.is_empty() {
        return;
    }

    info!(
        conversation_id = bridge.conversation_id,
        hooks = ?contributors.iter().map(|h| h.name()).collect::<Vec<_>>(),
        "delivering idle-hook system message"
    );

    // `render_idle_hook` builds the `{"system":"idle_hooks", ...}` wrapper
    // JSON itself — single source of truth (review F10).
    let rendered = crate::system_message::render_idle_hook(&envelope);
    match bridge.send_system_message(rendered, None).await {
        Ok(()) => {
            for hook in contributors {
                hook.on_delivered();
            }
        }
        Err(e) => {
            error!(
                conversation_id = bridge.conversation_id,
                error = %e,
                "idle-hook send_system_message failed; \
                 not retrying — next turn-end will re-evaluate"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::config::{AccessLevel, ResolvedMount};
    use std::path::Path;
    use std::process::Command as StdCommand;

    fn git_run(dir: &Path, args: &[&str]) {
        let out = StdCommand::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@test")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@test")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn git_init(dir: &Path) {
        git_run(dir, &["init", "-b", "main"]);
        std::fs::write(dir.join("file.txt"), "initial").unwrap();
        git_run(dir, &["add", "."]);
        git_run(dir, &["commit", "-m", "initial"]);
    }

    fn git_add_remote(dir: &Path) -> tempfile::TempDir {
        let remote = tempfile::tempdir().unwrap();
        git_run(remote.path(), &["init", "--bare", "-b", "main"]);
        git_run(
            dir,
            &[
                "remote",
                "add",
                "origin",
                &remote.path().display().to_string(),
            ],
        );
        git_run(dir, &["push", "-u", "origin", "main"]);
        remote
    }

    fn mount_for(dir: &Path, slug: &str) -> ResolvedMount {
        ResolvedMount {
            slug: slug.to_string(),
            host_path: dir.to_path_buf(),
            container_path: None,
            access: AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        }
    }

    /// A bridge with the given mounts. The bridge has no live CC; tests
    /// drive `DirtyRepoHook::check` directly against `bridge` to exercise
    /// the repo-status path without spinning up CC.
    async fn test_bridge_with_mounts(mounts: Vec<ResolvedMount>) -> Arc<ActiveBridge> {
        let db = brenn_lib::db::init_db_memory();
        let (user_id, conv_id) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "testuser", "$argon2id$fake");
            let cid = brenn_lib::conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel(64);
        ActiveBridge::inject_for_test_with_mounts(
            user_id,
            conv_id,
            "test",
            db,
            broadcast_tx,
            mounts,
        )
    }

    #[tokio::test]
    async fn dirty_repo_hook_clean_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let _remote = git_add_remote(dir.path());
        let mount = mount_for(dir.path(), "clean");
        let bridge = test_bridge_with_mounts(vec![mount]).await;

        let hook = DirtyRepoHook::new();
        let result = hook.check(&bridge).await;
        assert!(
            result.is_none(),
            "clean repo should produce no nudge, got {result:?}"
        );
        assert!(!hook.reminder_sent.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dirty_repo_hook_uncommitted_returns_some() {
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let _remote = git_add_remote(dir.path());
        std::fs::write(dir.path().join("file.txt"), "modified").unwrap();
        let mount = mount_for(dir.path(), "dirty");
        let bridge = test_bridge_with_mounts(vec![mount]).await;

        let hook = DirtyRepoHook::new();
        let result = hook.check(&bridge).await;
        let value = result.expect("dirty repo should produce a nudge");
        let by_slug = value.get("by_slug").expect("by_slug present");
        let entry = by_slug.get("dirty").expect("dirty slug present");
        assert_eq!(entry.get("uncommitted").and_then(|v| v.as_u64()), Some(1));
        assert_eq!(entry.get("unpushed").and_then(|v| v.as_u64()), Some(0));
    }

    #[tokio::test]
    async fn dirty_repo_hook_unpushed_returns_some() {
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let _remote = git_add_remote(dir.path());
        // Local commit not yet pushed.
        std::fs::write(dir.path().join("local.txt"), "local").unwrap();
        git_run(dir.path(), &["add", "."]);
        git_run(dir.path(), &["commit", "-m", "local"]);
        let mount = mount_for(dir.path(), "tech");
        let bridge = test_bridge_with_mounts(vec![mount]).await;

        let hook = DirtyRepoHook::new();
        let result = hook.check(&bridge).await;
        let value = result.expect("unpushed commits should produce a nudge");
        let entry = &value["by_slug"]["tech"];
        assert_eq!(entry["uncommitted"].as_u64(), Some(0));
        assert_eq!(entry["unpushed"].as_u64(), Some(1));
    }

    #[tokio::test]
    async fn dirty_repo_hook_no_upstream_is_clean() {
        // No remote → upstream=None, unpushed_count=None — but no error
        // should be emitted, and the repo should look clean.
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let mount = mount_for(dir.path(), "noremote");
        let bridge = test_bridge_with_mounts(vec![mount]).await;

        let hook = DirtyRepoHook::new();
        let result = hook.check(&bridge).await;
        assert!(
            result.is_none(),
            "no-upstream clean repo should produce no nudge, got {result:?}"
        );
    }

    #[tokio::test]
    async fn dirty_repo_hook_mixed_repos() {
        let clean_dir = tempfile::tempdir().unwrap();
        git_init(clean_dir.path());
        let _remote_a = git_add_remote(clean_dir.path());

        let dirty_dir = tempfile::tempdir().unwrap();
        git_init(dirty_dir.path());
        let _remote_b = git_add_remote(dirty_dir.path());
        std::fs::write(dirty_dir.path().join("new.txt"), "new").unwrap();

        let mounts = vec![
            mount_for(clean_dir.path(), "alpha"),
            mount_for(dirty_dir.path(), "beta"),
        ];
        let bridge = test_bridge_with_mounts(mounts).await;

        let hook = DirtyRepoHook::new();
        let result = hook.check(&bridge).await;
        let value = result.expect("at least one dirty repo should produce a nudge");
        let by_slug = &value["by_slug"];
        assert!(
            by_slug.get("alpha").is_none(),
            "alpha is clean, must be omitted"
        );
        let beta = &by_slug["beta"];
        assert_eq!(beta["uncommitted"].as_u64(), Some(1));
    }

    #[tokio::test]
    async fn dirty_repo_hook_one_shot_cycle() {
        let dir = tempfile::tempdir().unwrap();
        git_init(dir.path());
        let _remote = git_add_remote(dir.path());
        std::fs::write(dir.path().join("file.txt"), "modified").unwrap();
        let mount = mount_for(dir.path(), "cycle");
        let bridge = test_bridge_with_mounts(vec![mount]).await;

        let hook = DirtyRepoHook::new();

        // First check while dirty: Some.
        let r1 = hook.check(&bridge).await;
        assert!(r1.is_some());
        // Simulate the framework calling on_delivered.
        hook.on_delivered();
        assert!(hook.reminder_sent.load(Ordering::SeqCst));

        // Second check while still dirty: suppressed (None).
        let r2 = hook.check(&bridge).await;
        assert!(
            r2.is_none(),
            "suppressed once already-nagged in the same dirty cycle"
        );

        // Make repo clean.
        git_run(dir.path(), &["checkout", "--", "file.txt"]);

        // Third check while clean: None, and reminder_sent cleared.
        let r3 = hook.check(&bridge).await;
        assert!(r3.is_none());
        assert!(
            !hook.reminder_sent.load(Ordering::SeqCst),
            "reminder_sent must clear when all repos go clean"
        );

        // Dirty again: Some.
        std::fs::write(dir.path().join("file.txt"), "modified again").unwrap();
        let r4 = hook.check(&bridge).await;
        assert!(r4.is_some(), "next dirty cycle should fire");
    }

    #[tokio::test]
    async fn dirty_repo_hook_has_pending_work_constant() {
        // `has_pending_work()` is a constant predicate for this hook —
        // the suppression logic lives inside `check()`.
        let hook = DirtyRepoHook::new();
        assert!(hook.has_pending_work());
        hook.on_delivered();
        assert!(hook.has_pending_work());
        hook.on_resolved();
        assert!(hook.has_pending_work());
    }

    #[tokio::test]
    async fn dirty_repo_hook_no_mounts_returns_none() {
        let bridge = test_bridge_with_mounts(vec![]).await;
        let hook = DirtyRepoHook::new();
        let result = hook.check(&bridge).await;
        assert!(result.is_none());
    }
}

//! One-cycle reaction: fetch+classify+notify for a single trigger.

use std::time::Instant;

use brenn_lib::obs::alerting::AlertSeverity;
use brenn_lib::repo_sync_cursor::{self, EnqueueRow};
use serde::Serialize;
use tracing::{Instrument, debug, info, info_span, warn};

use super::git::{PullOutcome, collect_oneline, oneline_unavailable, pull_clone, rev_parse};
use super::{CloneInfo, RepoSyncCtx, SyncTrigger};
use crate::git_subprocess::sanitize_log_line;

// ---------------------------------------------------------------------------
// Typed event payload structs (A2, A3)
// ---------------------------------------------------------------------------

/// Typed payload for A2: repo-sync advance event.
///
/// Fields alphabetical (BTreeMap sort order from serde_json without preserve_order):
/// kind, oneline, remote, slug.
#[derive(Serialize)]
struct RepoSyncAdvanceEvent<'a> {
    // alphabetical: kind, oneline, remote, slug
    kind: &'a str,
    oneline: &'a [String],
    remote: &'a str,
    slug: &'a str,
}

/// Typed payload for A3: repo-sync conflict event.
///
/// Fields alphabetical: detail, kind, reason, remote, slug.
#[derive(Serialize)]
struct RepoSyncConflictEvent<'a> {
    // alphabetical: detail, kind, reason, remote, slug
    detail: &'a str,
    kind: &'a str,
    reason: &'a str,
    remote: &'a str,
    slug: &'a str,
}

/// Consecutive-failure threshold at which an `AuthError` escalates to an
/// operator alert. 1 = fire on first occurrence; auth rejections are
/// unambiguous server-side verdicts that retrying cannot fix.
const AUTH_ESCALATION_THRESHOLD: u32 = 1;

/// Consecutive-failure threshold at which a `TransientError` escalates.
/// 4 cycles ≈ 20 min at the default 5-min poll interval — long enough
/// that a brief forgejo restart (1-2 min) doesn't page, short enough
/// that a real outage reaches the operator before they'd have noticed
/// on their own.
const TRANSIENT_ESCALATION_THRESHOLD: u32 = 4;

/// Run one sync cycle for the given trigger.
///
/// Steps (per `docs/designs/repo-sync.md`):
/// 1. Identify the remote(s) in scope.
/// 2. For each remote, acquire its Mutex (serializes cycles on the same
///    remote across trigger sources).
/// 3. For each sync-enabled clone of the remote:
///    - Call `pull_clone` → `PullOutcome` (may advance local HEAD or not).
///    - Run advance-detection: compare current HEAD to
///      `last_notified_head[slug]`. Any mismatch synthesizes an
///      `Advanced` notification — whether the pull did the advance, or
///      a manual MCP / Bash operation got there first.
///    - Route notifications:
///      * HEAD unchanged or cold-start seed: no-op.
///      * HEAD moved: enqueue `repo_sync:pulled` for every consumer
///        (minus `acting_conversation_id` if set).
///      * `Conflict`: enqueue `repo_sync:conflict` for primary-pool
///        consumers, or fire AlertDispatcher Warning if the clone is RO-only.
pub async fn run_cycle(ctx: RepoSyncCtx, trigger: SyncTrigger) {
    match trigger {
        SyncTrigger::Poll { remote } => {
            run_cycle_for_remote(&ctx, &remote, "poll", None).await;
        }
        SyncTrigger::Push {
            remote,
            acting_conversation_id,
        } => {
            run_cycle_for_remote(&ctx, &remote, "push", acting_conversation_id).await;
        }
        SyncTrigger::ResumePoke { remotes } => {
            for remote in &remotes {
                run_cycle_for_remote(&ctx, remote, "resume", None).await;
            }
        }
    }
}

async fn run_cycle_for_remote(
    ctx: &RepoSyncCtx,
    remote: &str,
    trigger_kind: &'static str,
    acting_conversation_id: Option<i64>,
) {
    // Resolve remote → clones. Unknown remote => ignore. Unknown-remote
    // webhooks return 204 / info-log per the design; this branch handles
    // them plus any stale Poll triggers post-config-reload (not supported
    // today, but future-proof).
    let Some(slugs) = ctx.remote_to_slugs.get(remote) else {
        tracing::debug!(remote = %remote, "sync trigger for unknown remote — ignored");
        return;
    };

    // Per-remote Mutex. Cycles on distinct remotes run in parallel; cycles
    // on the same remote serialize.
    //
    // `remote_locks` is built from `remote_to_slugs` at startup, so a
    // missing entry is a startup invariant violation — panic rather than
    // silently skip (matches the missing-clone panic below).
    let lock = ctx
        .remote_locks
        .get(remote)
        .unwrap_or_else(|| panic!("BUG: remote_locks has no entry for {remote:?}"));
    let guard = lock.lock().await;

    let span = info_span!(
        "repo_sync::cycle",
        remote = %remote,
        trigger_kind,
        clones_total = slugs.len(),
    );
    let start = Instant::now();
    let mut pulled = 0u32;
    let mut conflicts = 0u32;
    let mut transient_errors = 0u32;
    let mut auth_errors = 0u32;
    let mut up_to_date = 0u32;
    let mut skipped = 0u32;
    let mut seeded = 0u32;
    let mut pending: Vec<PendingInject> = Vec::new();

    async {
        for slug in slugs {
            // `remote_to_slugs` is built from `clones` (see
            // `build_remote_to_slugs`), so a missing entry here is a
            // startup-invariant violation — panic rather than silently skip.
            let info = ctx.clones.get(slug).unwrap_or_else(|| {
                panic!(
                    "BUG: remote_to_slugs[{remote}] contains slug {slug:?} \
                     but ctx.clones has no entry for it",
                )
            });
            if !info.sync_enabled {
                skipped += 1;
                continue;
            }

            // Per-clone `repo_sync::pull` span (per design). Fields:
            // `outcome` is recorded below after `pull_clone` returns;
            // `prev_head` / `new_head` / `commit_count` are recorded from
            // inside `detect_and_notify_advance` via `Span::current()`.
            let pull_span = info_span!(
                "repo_sync::pull",
                slug = %info.slug,
                outcome = tracing::field::Empty,
                commit_count = tracing::field::Empty,
                prev_head = tracing::field::Empty,
                new_head = tracing::field::Empty,
            );
            let _pull_enter = pull_span.enter();

            let outcome = pull_clone(&info.host_path).await;
            pull_span.record("outcome", pull_outcome_label(&outcome));
            match &outcome {
                PullOutcome::UpToDate => {
                    up_to_date += 1;
                    reset_failure_trackers(ctx, slug);
                }
                PullOutcome::Advanced { .. } => {
                    // Counted via advance-detection below.
                    reset_failure_trackers(ctx, slug);
                }
                PullOutcome::TransientError(detail) => {
                    transient_errors += 1;
                    warn!(
                        slug = %slug,
                        detail = %sanitize_log_line(&super::truncate_detail(detail, 512)),
                        "repo_sync::pull transient error",
                    );
                    handle_transient_error(ctx, info, detail);
                }
                PullOutcome::AuthError { reason, detail } => {
                    auth_errors += 1;
                    warn!(
                        slug = %slug,
                        reason = %sanitize_log_line(reason),
                        detail = %sanitize_log_line(&super::truncate_detail(detail, 512)),
                        "repo_sync::pull auth error",
                    );
                    handle_auth_error(ctx, info, reason, detail);
                }
                PullOutcome::Conflict { reason, detail } => {
                    conflicts += 1;
                    reset_failure_trackers(ctx, slug);
                    pending.extend(handle_conflict(ctx, info, reason, detail).await);
                }
            }

            // Advance-detection runs after every pull outcome — even
            // `TransientError`, since HEAD may have moved locally while
            // the fetch leg failed (operator committed on disk while the
            // network was down). `detect_and_notify_advance` records
            // `prev_head`, `new_head`, and `commit_count` onto the active
            // `pull_span` via `Span::current()`.
            let (advance_outcome, work) =
                detect_and_notify_advance(ctx, info, acting_conversation_id, &outcome).await;
            pending.extend(work);
            match advance_outcome {
                AdvanceOutcome::Unchanged => {}
                AdvanceOutcome::Seeded => seeded += 1,
                AdvanceOutcome::Notified => pulled += 1,
                AdvanceOutcome::HeadReadFailed(err) => {
                    // rev-parse HEAD failing after pull_clone ran is
                    // abnormal but not fatal — log and move on. The
                    // `Conflict` branch above already reports the same
                    // shape via handle_conflict if this was what caused
                    // the pull to fail.
                    warn!(
                        slug = %slug,
                        error = %err,
                        "repo_sync: advance-detection rev-parse failed — skipping",
                    );
                }
            }
            drop(_pull_enter);
        }

        // Capture elapsed under-lock before releasing.
        let elapsed = start.elapsed();

        // Release the per-remote lock before fan-out. The dedup invariant
        // is preserved: `last_notified_head` was already written inside
        // `handle_advanced` (above, still under the lock) before we return
        // the PendingInject entries. A concurrent cycle that acquires the
        // lock now will observe the updated cursor and produce no duplicate
        // notification.
        //
        // Note: `guard` is captured by move into this `async { }` block;
        // this `drop` is the sole release point — `guard` is not held past
        // this line even though it was declared before the block.
        drop(guard);

        // Test-only: signal that the lock has been released and there is
        // pending fan-out work. The `!pending.is_empty()` guard mirrors the
        // `pre_fanout_gate` condition below — the notify fires only when the
        // gate will also fire, so a waiting test knows the spawned cycle is
        // about to block at the gate (not on the per-remote lock).
        #[cfg(test)]
        if !pending.is_empty()
            && let Some(notify) = &ctx.post_lock_release_notify
        {
            notify.notify_one();
        }

        info!(
            pulled,
            conflicts,
            transient_errors,
            auth_errors,
            up_to_date,
            seeded,
            skipped,
            elapsed_ms = elapsed.as_millis() as u64,
            "repo_sync cycle complete",
        );

        // Test-only: if the gate is set and there is actual fan-out work to
        // do, wait for a permit before beginning. This lets tests verify
        // that the per-remote lock is released before fan-out starts, by
        // blocking cycle 1 here and confirming cycle 2 can enter the lock
        // and complete. The guard only fires when `pending` is non-empty
        // so cycle 2 (which sees no advance) passes through unblocked.
        #[cfg(test)]
        if !pending.is_empty()
            && let Some(gate) = &ctx.pre_fanout_gate
        {
            gate.acquire()
                .await
                .expect("pre_fanout_gate semaphore closed")
                .forget();
        }

        // Per-consumer live-inject after lock release. Each entry performs
        // DB I/O and CC-stdio I/O; these are the operations that no longer
        // need to hold the per-remote lock. Rows are already committed to
        // the DB atomically (in `handle_advanced` / `handle_conflict`)
        // before this point, so a failure here leaves rows pending for the
        // next cycle or drain-on-wake.
        //
        // Hoist the span capture once before the loop; `Span::current()` is
        // already `span` here (we're inside `.instrument(span)`), and cloning
        // it per-call is equivalent but wastes an Arc clone + TLS read each
        // iteration.
        let cycle_span = tracing::Span::current();
        for p in pending {
            maybe_inject_pending(ctx, p.conversation_id, p.source)
                .instrument(cycle_span.clone())
                .await;
        }
    }
    .instrument(span)
    .await;
}

/// A single per-consumer fan-out task accumulated during a lock-held cycle.
/// After the per-remote guard is released, the caller drains this list
/// by calling `maybe_inject_pending` for each entry.
struct PendingInject {
    conversation_id: i64,
    source: &'static str,
}

impl PendingInject {
    /// Convert a list of `(conversation_id, app_slug)` pairs into a
    /// `Vec<PendingInject>` for the given source. Both `handle_advanced` and
    /// `handle_conflict` produce their work lists this way.
    fn from_targets(targets: Vec<(i64, String)>, source: &'static str) -> Vec<Self> {
        targets
            .into_iter()
            // `app_slug` is carried by the caller for `EnqueueRow` (used later via
            // `submit_ingress`), but `PendingInject` only needs the conversation_id
            // to look up the active bridge at inject time — the slug is not needed here.
            .map(|(conversation_id, _app_slug)| Self {
                conversation_id,
                source,
            })
            .collect()
    }
}

/// Short string label for a `PullOutcome`, used as the `outcome` field on
/// the `repo_sync::pull` span.
fn pull_outcome_label(outcome: &PullOutcome) -> &'static str {
    match outcome {
        PullOutcome::UpToDate => "up_to_date",
        PullOutcome::Advanced { .. } => "advanced",
        PullOutcome::TransientError(_) => "transient_error",
        PullOutcome::AuthError { .. } => "auth_error",
        PullOutcome::Conflict { .. } => "conflict",
    }
}

/// Which failure class a tracker/handler is operating on. Used by
/// `bump_and_maybe_alert` to pick the right map in
/// `PersistentFailureState`.
#[derive(Debug, Clone, Copy)]
enum FailureClass {
    Transient,
    Auth,
}

/// Clear both failure trackers for a slug. Called on any outcome that
/// proves we successfully talked to the remote (UpToDate / Advanced /
/// Conflict — Conflict means the fetch itself succeeded). Clears the
/// `alerted` flag too, so the next fresh failure incident can page.
///
/// Skips the lock entirely in the hot path: healthy clones never
/// populate the trackers, so the vast majority of cycles hit the
/// `contains_key` short-circuit without touching the mutex.
fn reset_failure_trackers(ctx: &RepoSyncCtx, slug: &str) {
    let mut state = ctx
        .failure_state
        .lock()
        .expect("failure_state mutex poisoned");
    if !state.transient.contains_key(slug) && !state.auth.contains_key(slug) {
        return;
    }
    state.transient.remove(slug);
    state.auth.remove(slug);
}

/// Increment the relevant per-slug counter, return the new count iff
/// this cycle just crossed the escalation threshold for a fresh
/// incident. Shared by both `handle_transient_error` and
/// `handle_auth_error` — the escalation state machine is identical;
/// only the map and threshold differ.
fn bump_and_maybe_alert(ctx: &RepoSyncCtx, slug: &str, class: FailureClass) -> Option<u32> {
    let mut state = ctx
        .failure_state
        .lock()
        .expect("failure_state mutex poisoned");
    let (map, threshold) = match class {
        FailureClass::Transient => (&mut state.transient, TRANSIENT_ESCALATION_THRESHOLD),
        FailureClass::Auth => (&mut state.auth, AUTH_ESCALATION_THRESHOLD),
    };
    let tracker = map.entry(slug.to_string()).or_default();
    tracker.consecutive = tracker.consecutive.saturating_add(1);
    if tracker.consecutive >= threshold && !tracker.alerted {
        tracker.alerted = true;
        Some(tracker.consecutive)
    } else {
        None
    }
}

/// Bookkeep a `TransientError` occurrence for `slug`; fire the operator
/// alert when the consecutive-count crosses the threshold for a fresh
/// incident.
fn handle_transient_error(ctx: &RepoSyncCtx, info: &CloneInfo, detail: &str) {
    let Some(consecutive) = bump_and_maybe_alert(ctx, &info.slug, FailureClass::Transient) else {
        return;
    };

    let body = format!(
        "Clone {slug} at {remote} has failed {consecutive} consecutive sync cycles \
         with transient errors (network / unreachable / server-side hiccup). \
         Polling will keep retrying. If this persists, investigate the remote \
         or network path.\n\nMost recent detail:\n{detail}",
        slug = info.slug,
        remote = info.remote,
        detail = super::truncate_detail(detail, 2048),
    );
    ctx.alert_dispatcher
        .with_field("slug", info.slug.clone())
        .with_field("remote", info.remote.clone())
        .with_field("consecutive", consecutive.to_string())
        .alert(
            AlertSeverity::Warning,
            format!("repo_sync: persistent transient errors on {}", info.slug),
            body,
        );
}

/// Bookkeep an `AuthError` occurrence for `slug`; fire on the first
/// cycle of a fresh incident. Threshold is 1 — auth rejection is a
/// server-side verdict that retrying cannot change.
fn handle_auth_error(ctx: &RepoSyncCtx, info: &CloneInfo, reason: &str, detail: &str) {
    if bump_and_maybe_alert(ctx, &info.slug, FailureClass::Auth).is_none() {
        return;
    }

    let body = format!(
        "Clone {slug} at {remote} is failing to authenticate against the \
         remote. Retrying will not help — the remote rejected the credential. \
         Check the SSH key, known_hosts entry, or remote-side authorized_keys.\n\n\
         Reason: {reason}\n\nDetail:\n{detail}",
        slug = info.slug,
        remote = info.remote,
        reason = reason,
        detail = super::truncate_detail(detail, 2048),
    );
    ctx.alert_dispatcher
        .with_field("slug", info.slug.clone())
        .with_field("remote", info.remote.clone())
        .with_field("reason", reason.to_string())
        .alert(
            AlertSeverity::Warning,
            format!("repo_sync: auth failure on {}", info.slug),
            body,
        );
}

/// Result of advance-detection for one clone in one cycle.
enum AdvanceOutcome {
    /// HEAD unchanged since the last notification.
    Unchanged,
    /// First cycle for this slug since manager startup — we recorded
    /// current HEAD as the baseline and fired no event. Prevents a
    /// one-shot "everything moved" alert storm on restart.
    Seeded,
    /// HEAD moved; `handle_advanced` was fired.
    Notified,
    /// `git rev-parse HEAD` failed. The pull path likely already reported
    /// the underlying problem; we carry a short message for observability.
    HeadReadFailed(String),
}

/// Compare current local HEAD to `last_notified_head[slug]`. If different
/// (or missing), synthesize a `repo_sync:pulled` or `repo_sync:local`
/// notification covering the range and record the "notified" head.
///
/// Classification rule (matches design `repo-sync-false-pulled-summary`):
/// `"pulled"` iff `PullOutcome::Advanced` produced the movement; otherwise
/// `"local"`. This is conservative — we only call a commit "pulled" when
/// this cycle actually did the fetch-merge dance that brought it in.
///
/// For `Advanced`: the range end is the `remote_head` SHA that `pull_clone`
/// captured post-fetch (threaded through `PullOutcome::Advanced`), not
/// local HEAD. That way a local commit made between the merge and
/// advance-detection reading HEAD doesn't get mis-labeled as pulled; the
/// residual `remote_head..HEAD` delta surfaces as a `"local"` event on
/// the next cycle.
///
/// The `last_notified_head` mutex is only held synchronously for the
/// read-then-conditionally-write; the git/log + notification fan-out runs
/// outside the lock. With the per-remote Mutex in the caller, this means
/// at most one cycle at a time touches a given slug's entry.
async fn detect_and_notify_advance(
    ctx: &RepoSyncCtx,
    info: &CloneInfo,
    acting_conversation_id: Option<i64>,
    pull_outcome: &PullOutcome,
) -> (AdvanceOutcome, Vec<PendingInject>) {
    // The current span is the per-clone `repo_sync::pull` span opened in
    // `run_cycle_for_remote`. We record `new_head`, `prev_head`, and
    // `commit_count` as they become known.
    let pull_span = tracing::Span::current();

    let current_head = match rev_parse(&info.host_path, "HEAD").await {
        Ok(sha) => sha,
        Err(e) => return (AdvanceOutcome::HeadReadFailed(e), Vec::new()),
    };
    pull_span.record("new_head", current_head.as_str());

    // Snapshot the previous notification point. Do the `Option` split
    // outside any `await` to avoid holding the std mutex across awaits.
    let previous: Option<String> = {
        let map = ctx
            .last_notified_head
            .lock()
            .expect("last_notified_head mutex poisoned");
        map.get(&info.slug).cloned()
    };

    let Some(prev) = previous else {
        // Cold start for this slug — seed cursor and cache, skip notification.
        {
            let mut conn = ctx.db.lock().await;
            repo_sync_cursor::upsert_and_enqueue(&mut conn, &info.slug, &current_head, &[]);
        }
        {
            let mut map = ctx
                .last_notified_head
                .lock()
                .expect("last_notified_head mutex poisoned");
            map.insert(info.slug.clone(), current_head);
        }
        return (AdvanceOutcome::Seeded, Vec::new());
    };
    pull_span.record("prev_head", prev.as_str());

    if prev == current_head {
        return (AdvanceOutcome::Unchanged, Vec::new());
    }

    // Classify the movement and determine the range-end SHA to use for
    // both the oneline collection and `last_notified_head` bookkeeping.
    //
    // `Advanced` carries the post-fetch `origin/main` SHA that
    // `pull_clone` already captured; using it as the range end means a
    // local commit that landed between the merge and this function
    // reading HEAD gets picked up as `"local"` on the next cycle rather
    // than smuggled into `"pulled"`. For all other outcomes the advance
    // must be local-only, so `HEAD` is the right range end.
    let (kind, range_end) = match pull_outcome {
        PullOutcome::Advanced { remote_head } => (AdvanceKind::Pulled, remote_head.clone()),
        _ => (AdvanceKind::Local, current_head.clone()),
    };

    // HEAD moved. Collect the oneline range. If the range can't be read
    // (history rewrite, git-log failure, …), fall back to a placeholder —
    // better to notify with partial data than silently drop.
    let oneline = match collect_oneline(&info.host_path, &prev, &range_end).await {
        Ok(lines) if !lines.is_empty() => {
            debug!(
                slug = %info.slug,
                prev = %prev,
                new = %range_end,
                commits = lines.len(),
                "repo_sync: advance-detection found HEAD movement",
            );
            lines
        }
        other => {
            warn!(
                slug = %info.slug,
                prev = %prev,
                new = %range_end,
                result = ?other,
                "repo_sync: could not derive oneline for advance — synthesizing placeholder",
            );
            oneline_unavailable(&prev, &range_end)
        }
    };
    pull_span.record("commit_count", oneline.len() as u64);

    // NOTE: the in-memory cache write lives INSIDE handle_advanced,
    // AFTER the DB transaction commits. Doing the cache write pre-commit
    // would silently advance past an un-persisted cursor if the process
    // restarted between cache-write and commit, losing the notification
    // across restart. See
    // `docs/designs/repo-sync-last-notified-head-loss-across-restart.md`.
    //
    // Serialization within a process is preserved because the per-remote
    // Mutex in `run_cycle_for_remote` still guards the whole cycle, so
    // concurrent triggers on the same remote can't both observe the
    // pre-advance cursor and double-fire.

    let work = handle_advanced(ctx, info, oneline, kind, acting_conversation_id, range_end).await;
    (AdvanceOutcome::Notified, work)
}

/// Classification of an observed HEAD advance: produced by this cycle's
/// fetch-merge (`Pulled`) vs anything else (`Local`). See
/// `detect_and_notify_advance` for the rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvanceKind {
    Pulled,
    Local,
}

/// Route an observed HEAD advance (from Part A's advance-detection).
///
/// `kind` distinguishes `Pulled` (fetched from remote via this cycle's
/// fetch-merge) from `Local` (HEAD moved locally; not from this cycle's
/// fetch). The two kinds are enqueued under distinct sources
/// (`repo_sync:pulled` vs `repo_sync:local`) and payload `kind` fields so
/// the drain-time collapser and the LLM can distinguish provenance.
///
/// `acting_conversation_id` is the conversation that triggered the cycle
/// via a `SyncTrigger::Push { .. }` emitted from an MCP git tool. Callers
/// that aren't tied to a specific conversation (poll, webhook, resume)
/// pass `None`. We filter the acting conversation out of the target list
/// so the conversation that just invoked `GitRepoPull` /
/// `GitRepoCommitAndPush` doesn't get a redundant notification for the
/// change it made itself.
async fn handle_advanced(
    ctx: &RepoSyncCtx,
    info: &CloneInfo,
    oneline: Vec<String>,
    kind: AdvanceKind,
    acting_conversation_id: Option<i64>,
    range_end: String,
) -> Vec<PendingInject> {
    let (source, kind_str) = match kind {
        AdvanceKind::Pulled => (
            brenn_lib::messaging::REPO_SYNC_SOURCE_PULLED,
            brenn_lib::messaging::REPO_SYNC_KIND_PULLED,
        ),
        AdvanceKind::Local => (
            brenn_lib::messaging::REPO_SYNC_SOURCE_LOCAL,
            brenn_lib::messaging::REPO_SYNC_KIND_LOCAL,
        ),
    };
    let n = oneline.len();
    let advance_event = RepoSyncAdvanceEvent {
        kind: kind_str,
        oneline: &oneline,
        remote: &info.remote,
        slug: &info.slug,
    };
    let payload = serde_json::to_string(&advance_event)
        .expect("RepoSyncAdvanceEvent serialization is infallible");
    let summary = match kind {
        AdvanceKind::Pulled => format!(
            "repo {} advanced from remote ({} new commit{})",
            info.slug,
            n,
            if n == 1 { "" } else { "s" },
        ),
        AdvanceKind::Local => format!(
            "repo {} advanced locally ({} new commit{})",
            info.slug,
            n,
            if n == 1 { "" } else { "s" },
        ),
    };

    let mut targets = consumer_conversations(ctx, &info.consumer_apps).await;
    if let Some(acting) = acting_conversation_id {
        targets.retain(|(id, _slug)| *id != acting);
    }
    if targets.is_empty() {
        tracing::debug!(
            slug = %info.slug,
            acting_conversation_id,
            kind = ?kind,
            "advance: no consumer conversations (after acting-conversation filter) — still advancing cursor",
        );
    }

    // Build the per-consumer event rows and commit them atomically with
    // the cursor upsert. Rows may be empty (acting conversation was the
    // only consumer, or apps set has no live conversations yet). We still
    // call `upsert_and_enqueue` so the cursor advances — this preserves
    // today's behavior where the cursor was written pre-fan-out at the
    // old reactor.rs site before the empty-targets early-return.
    let rows: Vec<EnqueueRow<'_>> = targets
        .iter()
        .map(|(cid, app_slug)| EnqueueRow {
            conversation_id: *cid,
            app_slug: app_slug.as_str(),
            source,
            summary: &summary,
            payload: &payload,
        })
        .collect();

    {
        let mut conn = ctx.db.lock().await;
        repo_sync_cursor::upsert_and_enqueue(&mut conn, &info.slug, &range_end, &rows);
    }

    // Cache write AFTER commit. A restart between commit and this write
    // is equivalent to a restart after the write: the new process
    // re-seeds from `repo_sync_cursor` via `load_all`, which reflects
    // the committed state. No double-notify path.
    {
        let mut map = ctx
            .last_notified_head
            .lock()
            .expect("last_notified_head mutex poisoned");
        map.insert(info.slug.clone(), range_end);
    }

    // Post-pull hooks: run integration commands for apps that have them.
    // Only fires on Pulled (externally-sourced data), not Local (the app
    // itself made the change).
    if kind == AdvanceKind::Pulled {
        for app_slug in &info.consumer_apps {
            if let Some(app) = ctx.apps.get(app_slug)
                && (!app.post_pull_hooks.host.is_empty()
                    || !app.post_pull_hooks.container.is_empty())
            {
                let app = app.clone();
                let slug = info.slug.clone();
                let alert = ctx.alert_dispatcher.clone();
                let lock = ctx
                    .post_pull_hook_locks
                    .get(app_slug)
                    .expect("BUG: app has post_pull_hooks but no lock entry")
                    .clone();
                tokio::spawn(async move {
                    // try_lock: if another hook invocation is already
                    // running for this app, skip. The running hook
                    // operates on the latest repo state already.
                    let Ok(_guard) = lock.try_lock() else {
                        debug!(
                            app = %app.slug,
                            repo = %slug,
                            "post_pull_hook already running — coalescing (skip)",
                        );
                        return;
                    };
                    let warnings = crate::hooks::run_post_pull_hooks(&app, &slug).await;
                    for w in warnings {
                        alert.alert(
                            AlertSeverity::Warning,
                            format!("post_pull_hook warning: {}", app.slug),
                            w,
                        );
                    }
                });
            }
        }
    }

    // Return fan-out work list. The caller (`run_cycle_for_remote`) will
    // drain this list after releasing the per-remote lock.
    PendingInject::from_targets(targets, source)
}

/// Route a pull that failed with a merge-side conflict.
///
/// Routing rule:
/// - `primary_apps` is non-empty (the common case): notify primary-pool
///   consumers. They have the write access to investigate and resolve.
/// - `primary_apps` is empty (RO-only clone): no LLM event — we fire a
///   `Warning` alert to the operator via `AlertDispatcher` instead,
///   deduped per (slug, reason) for the process lifetime so a recurring
///   conflict doesn't spam.
async fn handle_conflict(
    ctx: &RepoSyncCtx,
    info: &CloneInfo,
    reason: &str,
    detail: &str,
) -> Vec<PendingInject> {
    // Log the conflict for grep-based forensics. Truncate to 512 bytes
    // — the full 2KB detail rides the event payload below.
    warn!(
        slug = %info.slug,
        reason = %reason,
        detail = %super::truncate_detail(detail, 512),
        "repo_sync: pull conflict",
    );

    if info.primary_apps.is_empty() {
        // Per design: AlertSeverity::Warning, dedup on (slug, reason).
        ctx.alert_dispatcher
            .with_field("slug", info.slug.clone())
            .with_field("remote", info.remote.clone())
            .with_field("reason", reason.to_string())
            .with_field("detail", super::truncate_detail(detail, 2048))
            .alert_once_per_process(
                AlertSeverity::Warning,
                "repo_sync: RO-only conflict".to_string(),
                &format!("{}|{}", info.slug, reason),
                format!(
                    "Clone {slug} at {remote} failed to fast-forward. \
                     No read-write mount exists, so no agent can resolve it. \
                     Reason: {reason}\n\nDetail: {detail}",
                    slug = info.slug,
                    remote = info.remote,
                ),
            );
        return Vec::new();
    }

    let conflict_event = RepoSyncConflictEvent {
        detail,
        kind: brenn_lib::messaging::REPO_SYNC_KIND_CONFLICT,
        reason,
        remote: &info.remote,
        slug: &info.slug,
    };
    let payload = serde_json::to_string(&conflict_event)
        .expect("RepoSyncConflictEvent serialization is infallible");
    let summary = format!("repo {} pull conflict: {}", info.slug, reason);
    let source = brenn_lib::messaging::REPO_SYNC_SOURCE_CONFLICT;

    let targets = consumer_conversations(ctx, &info.primary_apps).await;
    if targets.is_empty() {
        // Primary-pool has no conversations yet (fresh app, never used).
        // Log and move on; when the user first spawns the app, they'll
        // see the current state of the clone naturally.
        tracing::debug!(
            slug = %info.slug,
            "conflict: no primary-pool conversations — skipping enqueue",
        );
        return Vec::new();
    }

    // Conflict fan-out goes through the cursor-less `enqueue_batch` variant:
    // a conflict means no new HEAD to record, so touching the cursor would
    // be a no-op. Same bug pattern as `handle_advanced`'s partial-fan-out
    // risk — the whole list of per-consumer INSERTs lands atomically.
    let rows: Vec<EnqueueRow<'_>> = targets
        .iter()
        .map(|(cid, app_slug)| EnqueueRow {
            conversation_id: *cid,
            app_slug: app_slug.as_str(),
            source,
            summary: &summary,
            payload: &payload,
        })
        .collect();
    {
        let mut conn = ctx.db.lock().await;
        repo_sync_cursor::enqueue_batch(&mut conn, &rows);
    }

    // Return fan-out work list. The caller (`run_cycle_for_remote`) will
    // drain this list after releasing the per-remote lock.
    PendingInject::from_targets(targets, source)
}

/// Best-effort inject the conversation's pending `repo_sync:*` rows into
/// a live bridge if one exists. The durable enqueue happened atomically
/// in `upsert_and_enqueue` / `enqueue_batch` before this runs.
///
/// **At-least-once semantics (design H3):** the inject rebuilds a
/// *collapsed* message from all of the conversation's currently-pending
/// `repo_sync:*` rows — not just the one this cycle produced. That way:
/// - Prior rows that failed to inject on an earlier cycle get re-attempted
///   here without a dedicated retry loop.
/// - Multiple rows accumulated in one cycle coalesce into one inject.
/// - Inject failure leaves every row pending; drain-on-wake handles them.
///
/// Non-repo_sync rows (cron, discord, pfin) are *not* touched by the live
/// path — those sources have their own submit_event path. We only pull
/// repo_sync:* rows into this inject.
///
/// **Known limitation (cross-remote inject race):** the per-remote Mutex
/// serializes cycles on the *same* remote, but cycles on *different*
/// remotes that happen to fan out to the same consumer conversation can
/// both interleave through fetch→send→mark. The worst case is a duplicate
/// summary inject (no data corruption — `mark_events_delivered` is
/// idempotent, and rows only leave the queue after a successful send).
/// Acceptable for MVP: the duplicate carries identical information, and
/// the operator sees it only when two unrelated remotes advance within
/// the bridge-inject window. If this shows up as a UX problem, add a
/// per-conversation inject mutex around the fetch→send→mark sequence.
async fn maybe_inject_pending(ctx: &RepoSyncCtx, conversation_id: i64, source: &str) {
    // `repo_sync::deliver` observability span (per design). Records the
    // conversation, whether a live bridge was present, and the counts of
    // queued/injected rows. Lets us see at a glance which consumers got
    // live-injected vs queued for drain.
    let span = info_span!(
        "repo_sync::deliver",
        conversation_id,
        source = %source,
        bridge_alive = tracing::field::Empty,
        // Count of this conversation's pending repo_sync:* rows at fetch
        // time (pre-collapse). `marked` logged below reports the count
        // actually written to the DB on success.
        pending_count = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    let _enter = span.enter();

    // Fetch pending repo_sync rows (the just-committed one plus any
    // stragglers from earlier cycles that failed to inject). The rows
    // are already persisted — we did NOT just enqueue here.
    // Load from the unified ingress store via load_pending_pushes_for_drain,
    // filtering to repo_sync:* ingress events.
    // `Event.id` is the push_id (set by row_to_drain_push) — used for marking delivered.
    let repo_sync_pending: Vec<brenn_lib::messaging::IngressEvent> = {
        let conn = ctx.db.lock().await;
        let subscriber = brenn_lib::messaging::ParticipantId::for_conversation(conversation_id);
        brenn_lib::messaging::db::load_pending_pushes_for_drain(&conn, &subscriber)
            .into_iter()
            .filter_map(|(_push_id, payload)| match payload {
                brenn_lib::messaging::IngressOrBus::Ingress(ev)
                    if brenn_lib::messaging::is_repo_sync_source(&ev.source) =>
                {
                    Some(ev)
                }
                _ => None,
            })
            .collect()
    };
    span.record("pending_count", repo_sync_pending.len());

    // Step 2: if a bridge is alive, collapse and inject. Rows marked
    // delivered only on successful send (at-least-once).
    let bridge_alive = ctx.active_bridges.get(conversation_id).await;
    span.record("bridge_alive", bridge_alive.is_some());
    let Some(bridge) = bridge_alive else {
        span.record("outcome", "queued_no_bridge");
        return;
    };

    if repo_sync_pending.is_empty() {
        // A concurrent fan-out from another cycle already fetched and
        // delivered the row(s) for this conversation. This is routine
        // post-refactor: the per-remote lock is released before fan-out,
        // so a second cycle can commit rows and deliver them before this
        // cycle's fan-out reaches this consumer.
        span.record("outcome", "no_pending_rows_concurrent_collapse");
        return;
    }

    let collapsed = brenn_lib::messaging::collapse_repo_sync(repo_sync_pending);
    // Render using render_event_drain, which produces both the LLM-facing text
    // (from format_event_batch) and the pre-rendered HTML card. This gives the
    // live repo_sync inject the same wire shape as the drain path.
    let rendered = match crate::system_message::render_event_drain(&collapsed.events) {
        Some(r) => r,
        None => {
            // Same routine concurrent-collapse scenario as above: the
            // collapsed events set is empty because another cycle's inject
            // already delivered them.
            span.record("outcome", "empty_batch_concurrent_collapse");
            return;
        }
    };

    match bridge.send_system_message(rendered, None).await {
        Ok(()) => {
            // Mark the synthesized summary's subsumed originals delivered,
            // plus any non-synthesized events the collapser passed through
            // (malformed rows). Dedupe because a malformed row can appear
            // in both sets. mark_pending_pushes_delivered is idempotent.
            // Event.id == push_id in the unified ingress store (set by row_to_drain_push).
            let mut ids: std::collections::HashSet<i64> = collapsed
                .events
                .iter()
                .filter(|e| e.id != brenn_lib::messaging::SYNTHETIC_EVENT_ID)
                .map(|e| e.id)
                .collect();
            ids.extend(collapsed.original_repo_sync_ids);
            let ids: Vec<i64> = ids.into_iter().collect();
            let marked = ids.len();
            let conn = ctx.db.lock().await;
            brenn_lib::messaging::db::mark_pending_pushes_delivered(&conn, &ids);
            // Release the DB lock before emitting tracing output so the
            // mutex isn't held across macro/formatter work.
            drop(conn);
            span.record("outcome", "injected");
            info!(marked, "repo_sync delivered and marked");
        }
        Err(e) => {
            span.record("outcome", "inject_failed");
            warn!(
                error = %e,
                "repo_sync live inject failed — rows stay queued for drain or next cycle"
            );
        }
    }
}

/// Resolve consumer conversations: every conversation whose `app_slug` is
/// in `apps`, regardless of status. Returns a list of conversation ids.
/// One SQL query. Status filtering is deliberately absent — see
/// `brenn_lib::conversation::conversation_ids_for_apps` docstring.
/// Returns `(conversation_id, app_slug)` pairs for all conversations belonging
/// to any of the given apps. Used to build `EnqueueRow` for repo_sync fan-out.
async fn consumer_conversations(
    ctx: &RepoSyncCtx,
    apps: &std::collections::HashSet<String>,
) -> Vec<(i64, String)> {
    if apps.is_empty() {
        return Vec::new();
    }
    let app_list: Vec<String> = apps.iter().cloned().collect();
    let conn = ctx.db.lock().await;
    brenn_lib::conversation::conversation_ids_and_slugs_for_apps(&conn, &app_list)
}

#[cfg(test)]
mod tests {
    //! Integration-style tests for the reaction pipeline.
    //!
    //! Each test builds a scratch git remote + one or more clones, a
    //! `RepoSyncCtx` backed by an in-memory DB, and calls
    //! `run_cycle_for_remote` directly. We then assert against the
    //! `events` table and the `last_notified_head` map.

    use super::*;
    use crate::active_bridge::ActiveBridges;
    use crate::repo_sync::CloneInfo;
    use crate::repo_sync::test_git_fixtures::{
        head, local_commit, push_sibling_commit, run_git, scratch_remote_and_clone,
    };
    use brenn_lib::obs::alerting::AlertDispatcher;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    /// Build a minimal `RepoSyncCtx` with one clone + one consumer app
    /// using a noop alerter. Returns `(ctx, remote_url)`.
    fn build_ctx(
        db: brenn_lib::db::Db,
        clone_path: PathBuf,
        slug: &str,
        app_slugs: &[&str],
    ) -> (RepoSyncCtx, String) {
        let (dispatcher, _handle) = AlertDispatcher::noop();
        let (ctx, remote_url, _info) =
            build_ctx_with_dispatcher(db, clone_path, slug, app_slugs, dispatcher);
        (ctx, remote_url)
    }

    /// Shared construction path for the test-scoped `RepoSyncCtx`. Callers
    /// pass in the `AlertDispatcher` they want (noop for integration tests,
    /// counting for escalation tests) and receive the matching `CloneInfo`
    /// back so unit tests can call the escalation helpers directly.
    fn build_ctx_with_dispatcher(
        db: brenn_lib::db::Db,
        clone_path: PathBuf,
        slug: &str,
        app_slugs: &[&str],
        alert_dispatcher: AlertDispatcher,
    ) -> (RepoSyncCtx, String, CloneInfo) {
        let remote_url = format!("ssh://example/{slug}.git");
        let info = CloneInfo {
            slug: slug.to_string(),
            host_path: clone_path,
            remote: remote_url.clone(),
            sync_enabled: true,
            consumer_apps: app_slugs.iter().map(|s| s.to_string()).collect(),
            primary_apps: app_slugs.iter().map(|s| s.to_string()).collect(),
        };
        let clones: HashMap<String, CloneInfo> =
            [(slug.to_string(), info.clone())].into_iter().collect();
        let mut remote_to_slugs: HashMap<String, Vec<String>> = HashMap::new();
        remote_to_slugs.insert(remote_url.clone(), vec![slug.to_string()]);
        let mut remote_locks: HashMap<String, Arc<TokioMutex<()>>> = HashMap::new();
        remote_locks.insert(remote_url.clone(), Arc::new(TokioMutex::new(())));
        let ctx = RepoSyncCtx {
            db,
            active_bridges: ActiveBridges::new(),
            alert_dispatcher,
            clones: Arc::new(clones),
            remote_to_slugs: Arc::new(remote_to_slugs),
            remote_locks: Arc::new(remote_locks),
            last_notified_head: Arc::new(std::sync::Mutex::new(HashMap::new())),
            failure_state: Arc::new(std::sync::Mutex::new(
                crate::repo_sync::PersistentFailureState::default(),
            )),
            apps: Arc::new(indexmap::IndexMap::new()),
            post_pull_hook_locks: Arc::new(HashMap::new()),
            pre_fanout_gate: None,
            post_lock_release_notify: None,
        };
        (ctx, remote_url, info)
    }

    /// Set up a user + one conversation for the given app, return conv id.
    /// Uses a unique username per call to avoid collisions when two
    /// conversations of the same app are needed in one test.
    async fn mk_conv(db: &brenn_lib::db::Db, app_slug: &str) -> i64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let username = format!("{app_slug}-u{n}");
        let conn = db.lock().await;
        let user_id = brenn_lib::auth::user::create_user(&conn, &username, "hash");
        brenn_lib::conversation::create_conversation(&conn, user_id, app_slug, false)
    }

    /// Fetch pending `repo_sync:*` events for a conversation, oldest first.
    /// Uses the unified ingress store.
    async fn pending_repo_sync_events(
        db: &brenn_lib::db::Db,
        conv_id: i64,
    ) -> Vec<brenn_lib::messaging::IngressEvent> {
        let conn = db.lock().await;
        let subscriber = brenn_lib::messaging::ParticipantId::for_conversation(conv_id);
        brenn_lib::messaging::db::load_pending_pushes_for_drain(&conn, &subscriber)
            .into_iter()
            .filter_map(|(_push_id, payload)| match payload {
                brenn_lib::messaging::IngressOrBus::Ingress(ev)
                    if brenn_lib::messaging::is_repo_sync_source(&ev.source) =>
                {
                    Some(ev)
                }
                _ => None,
            })
            .collect()
    }

    /// Count pending `repo_sync:*` events in the queue for a conversation.
    async fn pending_repo_sync_count(db: &brenn_lib::db::Db, conv_id: i64) -> usize {
        pending_repo_sync_events(db, conv_id).await.len()
    }

    /// Count all `repo_sync:*` ingress pushes across every conversation in the DB.
    async fn total_repo_sync_event_count(db: &brenn_lib::db::Db) -> i64 {
        let conn = db.lock().await;
        conn.query_row(
            "SELECT COUNT(*) FROM messaging_pending_pushes pp \
             JOIN messaging_messages m ON pp.message_id = m.id \
             WHERE m.envelope_type = 'ingress' AND m.ingress_source LIKE 'repo_sync:%'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("total_repo_sync_event_count query failed")
    }

    #[tokio::test]
    async fn cold_start_seeds_last_notified_head_without_firing() {
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // No events — cold start should seed without firing.
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 0);
        // last_notified_head should be seeded to current HEAD.
        let current = head(clone.path());
        let map = ctx.last_notified_head.lock().unwrap();
        assert_eq!(map.get("src-x"), Some(&current));
    }

    #[tokio::test]
    async fn second_cycle_with_no_movement_is_no_op() {
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 0);
        // Cycle 2: nothing moved, nothing to notify.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 0);
    }

    #[tokio::test]
    async fn advance_detection_notifies_on_remote_advance() {
        let (remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Someone else pushes to origin.
        push_sibling_commit(remote.path(), "external commit");

        // Cycle 2: pull_clone fast-forwards local HEAD; advance-detection
        // compares seeded (old) vs current (new) → notifies.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 1);

        // Third cycle: no further movement, no extra event.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 1);
    }

    #[tokio::test]
    async fn advance_detection_labels_local_commit_as_local() {
        // Models a manual `git commit` via Bash or an MCP
        // GitRepoCommitAndPush that advanced local HEAD: `pull_clone`
        // returns UpToDate (remote didn't move), but local HEAD did.
        // Advance-detection must catch this AND label it as `local`, not
        // `pulled`, because the commit was authored locally — it did not
        // come from a fetch.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Local commit, no push. This simulates the observed prod bug.
        local_commit(clone.path(), "local.txt", "local manual commit");

        // Cycle 2: pull_clone returns UpToDate (remote still at initial),
        // but advance-detection sees HEAD moved → notifies.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        let events = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].source,
            brenn_lib::messaging::REPO_SYNC_SOURCE_LOCAL
        );
        let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
        assert_eq!(payload["kind"], brenn_lib::messaging::REPO_SYNC_KIND_LOCAL);
        // Human-readable summary must say "advanced locally", not "advanced from remote".
        assert!(
            events[0].summary.contains("advanced locally"),
            "got {:?}",
            events[0].summary,
        );
    }

    #[tokio::test]
    async fn advance_detection_labels_remote_advance_as_pulled() {
        // Genuine remote pull: sibling pushes, the poll cycle fetches and
        // fast-forwards → `PullOutcome::Advanced`. The emitted event must
        // be labeled `pulled`.
        let (remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Sibling push advances origin/main.
        push_sibling_commit(remote.path(), "external commit");

        // Cycle 2: fast-forward → Advanced → labeled `pulled`.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        let events = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].source,
            brenn_lib::messaging::REPO_SYNC_SOURCE_PULLED
        );
        let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
        assert_eq!(payload["kind"], brenn_lib::messaging::REPO_SYNC_KIND_PULLED);
        // Human-readable summary must say "advanced from remote", not "advanced locally".
        assert!(
            events[0].summary.contains("advanced from remote"),
            "got {:?}",
            events[0].summary,
        );
    }

    #[tokio::test]
    async fn advance_detection_labels_local_after_transient_fetch_failure() {
        // Fetch fails (DNS-bogus remote) → PullOutcome::TransientError;
        // local HEAD has moved via a manual commit. The classification
        // rule is "pulled iff PullOutcome::Advanced", so TransientError
        // (and, by the same code path, AuthError / Conflict) labels the
        // movement as `local`. A single test covers all three non-Advanced
        // error branches — they share the `_ => AdvanceKind::Local` arm.
        let (_remote, clone) = scratch_remote_and_clone();
        // Point origin at a DNS-failing host so fetch classifies as transient.
        run_git(
            clone.path(),
            &[
                "remote",
                "set-url",
                "origin",
                "ssh://git@definitely-nonexistent.example.invalid/none.git",
            ],
        );
        // Cycle 1 seeds last_notified_head to current-HEAD even though
        // pull_clone returns TransientError (advance-detection runs after
        // every outcome). Then we commit, then cycle 2 observes the
        // movement with outcome=TransientError → labeled local.
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: fetch fails → TransientError. advance-detection still
        // runs and seeds last_notified_head (cold start) — no event.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 0);

        // Now commit locally.
        local_commit(clone.path(), "post-seed.txt", "local after fetch failed");

        // Cycle 2: fetch fails again → TransientError. advance-detection
        // sees HEAD moved → labeled `local` (NOT pulled; we cannot have
        // pulled anything since fetch failed).
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        let events = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].source,
            brenn_lib::messaging::REPO_SYNC_SOURCE_LOCAL
        );
        let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
        assert_eq!(payload["kind"], brenn_lib::messaging::REPO_SYNC_KIND_LOCAL);
    }

    #[tokio::test]
    async fn advance_detection_pulled_then_local_in_separate_cycles() {
        // Cycle N: sibling push → fast-forward → `pulled` event.
        // Cycle N+1: local commit on top of the just-merged HEAD →
        //           `pull_clone` returns UpToDate → `local` event.
        let (remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Sibling push.
        push_sibling_commit(remote.path(), "external commit");
        // Cycle 2: fast-forward → pulled.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        let events = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].source,
            brenn_lib::messaging::REPO_SYNC_SOURCE_PULLED
        );

        // Now author a local commit on top of the merged HEAD.
        local_commit(clone.path(), "after-pull.txt", "local after pull");
        // Cycle 3: pull_clone UpToDate (remote didn't move again),
        // advance-detection sees HEAD moved → labeled `local`.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        let events = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[1].source,
            brenn_lib::messaging::REPO_SYNC_SOURCE_LOCAL
        );
    }

    #[tokio::test]
    async fn advance_detection_pulled_range_excludes_local_commits() {
        // Range-constraint mitigation: on PullOutcome::Advanced the
        // oneline range is `prev..origin/main`, not `prev..HEAD`, and
        // `last_notified_head` is set to origin/main. A local commit
        // authored after the merge therefore surfaces as a separate
        // `local` event on the next cycle, not smuggled into `pulled`.
        //
        // The within-cycle race (commit landing between merge and
        // rev-parse) isn't reproducible in a test, so we verify the
        // mitigation's observable invariants: (a) the `pulled` oneline
        // contains only the sibling commit, and (b) `last_notified_head`
        // equals origin/main after the `pulled` cycle.
        let (remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        let sibling_sha = push_sibling_commit(remote.path(), "external commit");

        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        let events = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].source,
            brenn_lib::messaging::REPO_SYNC_SOURCE_PULLED
        );
        let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
        let oneline = payload["oneline"].as_array().unwrap();
        // Only the sibling commit — single entry.
        assert_eq!(
            oneline.len(),
            1,
            "pulled oneline should contain only the sibling commit"
        );
        // last_notified_head must equal origin/main (== sibling_sha), not HEAD.
        {
            let map = ctx.last_notified_head.lock().unwrap();
            assert_eq!(map.get("src-x"), Some(&sibling_sha));
        }

        // Now author a local commit and run another cycle.
        local_commit(clone.path(), "local.txt", "local after pull");
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        let events = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[1].source,
            brenn_lib::messaging::REPO_SYNC_SOURCE_LOCAL
        );
    }

    #[tokio::test]
    async fn acting_conversation_is_suppressed_from_notifications() {
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_a = mk_conv(&db, "appa").await;
        let conv_b = mk_conv(&db, "appa").await; // second conversation, same app
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // conv_a's MCP tool advances local HEAD.
        local_commit(clone.path(), "x.txt", "conv_a commit");

        // Push trigger with acting_conversation_id = conv_a. Only conv_b
        // should receive a notification.
        run_cycle_for_remote(&ctx, &remote_url, "push", Some(conv_a)).await;
        assert_eq!(pending_repo_sync_count(&db, conv_a).await, 0);
        assert_eq!(pending_repo_sync_count(&db, conv_b).await, 1);
    }

    #[tokio::test]
    async fn acting_conversation_none_notifies_everyone() {
        // Control for the previous test: None means no filter.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_a = mk_conv(&db, "appa").await;
        let conv_b = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        local_commit(clone.path(), "x.txt", "some commit");
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        assert_eq!(pending_repo_sync_count(&db, conv_a).await, 1);
        assert_eq!(pending_repo_sync_count(&db, conv_b).await, 1);
    }

    #[tokio::test]
    async fn run_cycle_dispatches_push_variant_with_acting_conversation() {
        // Verifies SyncTrigger::Push threads `acting_conversation_id`
        // through `run_cycle` → `run_cycle_for_remote` → filter. Uses the
        // high-level entry point rather than `run_cycle_for_remote`
        // directly, which is all the other tests do.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_a = mk_conv(&db, "appa").await;
        let conv_b = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Seed, then advance locally.
        run_cycle(
            ctx.clone(),
            SyncTrigger::Poll {
                remote: remote_url.clone(),
            },
        )
        .await;
        local_commit(clone.path(), "p.txt", "acting commit");

        // Dispatch a Push trigger through `run_cycle`.
        run_cycle(
            ctx.clone(),
            SyncTrigger::Push {
                remote: remote_url.clone(),
                acting_conversation_id: Some(conv_a),
            },
        )
        .await;
        assert_eq!(pending_repo_sync_count(&db, conv_a).await, 0);
        assert_eq!(pending_repo_sync_count(&db, conv_b).await, 1);
    }

    #[tokio::test]
    async fn run_cycle_dispatches_push_variant_none_notifies_everyone() {
        // Verifies SyncTrigger::Push { acting_conversation_id: None }
        // fans out to all conversations through `run_cycle` →
        // `run_cycle_for_remote` — the webhook-driven `git-repo-pull`
        // path. Successor to the retired dispatch-level webhook fan-out
        // test; complements the `Some(conv)` suppression case above.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_a = mk_conv(&db, "appa").await;
        let conv_b = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Seed, then advance locally.
        run_cycle(
            ctx.clone(),
            SyncTrigger::Poll {
                remote: remote_url.clone(),
            },
        )
        .await;
        local_commit(clone.path(), "p.txt", "webhook commit");

        // Dispatch a Push trigger with no acting conversation.
        run_cycle(
            ctx.clone(),
            SyncTrigger::Push {
                remote: remote_url.clone(),
                acting_conversation_id: None,
            },
        )
        .await;
        assert_eq!(pending_repo_sync_count(&db, conv_a).await, 1);
        assert_eq!(pending_repo_sync_count(&db, conv_b).await, 1);
    }

    /// Build a second `RepoSyncCtx` backed by the same `Db`, seeding
    /// `last_notified_head` from the persisted `repo_sync_cursor` rows
    /// via `load_all`. Simulates a restart: same DB, fresh ctx / cache.
    async fn rebuild_ctx_from_persisted(
        db: brenn_lib::db::Db,
        clone_path: PathBuf,
        slug: &str,
        app_slugs: &[&str],
    ) -> (RepoSyncCtx, String) {
        let (mut ctx, remote_url) = build_ctx(db.clone(), clone_path, slug, app_slugs);
        let seeded = {
            let conn = db.lock().await;
            brenn_lib::repo_sync_cursor::load_all(&conn)
        };
        ctx.last_notified_head = Arc::new(std::sync::Mutex::new(seeded));
        (ctx, remote_url)
    }

    #[tokio::test]
    async fn cursor_persists_across_simulated_restart() {
        // Cycle 1 advances HEAD (cursor row created, events enqueued).
        // Build a fresh ctx seeded via `load_all` from the same DB.
        // Cycle 2 on an unchanged HEAD must be `Unchanged`, not `Seeded`,
        // and must NOT enqueue any new events.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: seed, then advance.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        local_commit(clone.path(), "a.txt", "first advance");
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 1);

        // Drop ctx; rebuild from persisted cursor.
        drop(ctx);
        let (ctx2, remote_url) =
            rebuild_ctx_from_persisted(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"])
                .await;

        // Cycle 2 on unchanged HEAD: must not fire another notification.
        run_cycle_for_remote(&ctx2, &remote_url, "poll", None).await;
        assert_eq!(
            pending_repo_sync_count(&db, conv_id).await,
            1,
            "restart-seeded cursor must treat unchanged HEAD as Unchanged, not Seeded",
        );
    }

    #[tokio::test]
    async fn cursor_survives_head_movement_across_restart_and_notifies() {
        // Cycle 1 advances HEAD X → Y. Drop ctx. Move HEAD Y → Z on
        // disk (simulating "commit landed between restarts"). Rebuild
        // ctx with `load_all`-seeded cache. Cycle 2 must fire a
        // notification for Y → Z, not seed-and-skip.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Seed, then first advance.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        local_commit(clone.path(), "a.txt", "X->Y");
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 1);

        drop(ctx);

        // Simulate the restart: HEAD moves Y -> Z while Brenn is down.
        local_commit(clone.path(), "b.txt", "Y->Z during downtime");

        let (ctx2, remote_url) =
            rebuild_ctx_from_persisted(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"])
                .await;

        // Cycle 2 on the new ctx: advance-detection must fire.
        run_cycle_for_remote(&ctx2, &remote_url, "poll", None).await;
        assert_eq!(
            pending_repo_sync_count(&db, conv_id).await,
            2,
            "restart-seeded cursor must still detect HEAD movement across restart",
        );
    }

    #[tokio::test]
    async fn restart_after_commit_does_not_double_notify() {
        // Cycle 1 advances HEAD X → Y, writes the cursor row, enqueues
        // events. Drop ctx. Rebuild via `load_all`. Cycle 2 on
        // unchanged HEAD Y must assert (a) Unchanged (not Seeded), and
        // (b) no new event rows beyond cycle 1's.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Seed + advance + drop.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        local_commit(clone.path(), "x.txt", "one advance");
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        let count_before = pending_repo_sync_count(&db, conv_id).await;
        assert_eq!(count_before, 1);
        drop(ctx);

        // Rebuild from persisted state — no HEAD movement on disk.
        let (ctx2, remote_url) =
            rebuild_ctx_from_persisted(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"])
                .await;

        // Cycle 2: must be Unchanged; no new events.
        run_cycle_for_remote(&ctx2, &remote_url, "poll", None).await;
        assert_eq!(
            pending_repo_sync_count(&db, conv_id).await,
            count_before,
            "no new events should fire after restart when HEAD is unchanged",
        );
    }

    #[tokio::test]
    async fn seed_persists_across_restart_and_notifies_advance_during_downtime() {
        // Cycle 1 cold-seeds the slug (cursor row written, no events).
        // Drop ctx. Move HEAD while Brenn is "down". Rebuild ctx from the
        // persisted cursor. Cycle 2 must detect the downtime advance and
        // fire exactly one notification.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: cold seed — no events expected.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(
            pending_repo_sync_count(&db, conv_id).await,
            0,
            "cold seed must not fire any events",
        );

        // Assert cursor row was persisted to DB with current HEAD.
        let seed_head = head(clone.path());
        let persisted = {
            let conn = db.lock().await;
            brenn_lib::repo_sync_cursor::load_all(&conn)
        };
        assert_eq!(
            persisted.get("src-x").map(String::as_str),
            Some(seed_head.as_str()),
            "seed path must write cursor row to DB",
        );

        // Drop ctx — simulate Brenn going down.
        drop(ctx);

        // HEAD advances while Brenn is down.
        local_commit(clone.path(), "downtime.txt", "advance during downtime");

        // Rebuild ctx from persisted cursor (simulated restart).
        let (ctx2, remote_url) =
            rebuild_ctx_from_persisted(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"])
                .await;

        // Cycle 2: must detect the downtime advance and fire exactly one event.
        run_cycle_for_remote(&ctx2, &remote_url, "poll", None).await;
        assert_eq!(
            pending_repo_sync_count(&db, conv_id).await,
            1,
            "post-restart cycle must detect advance that landed during downtime",
        );
    }

    #[tokio::test]
    async fn full_cycle_advance_teardown_restart_advance_delivers_two_notifications() {
        // Integration smoke: full-cycle advance → tear down ctx → rebuild
        // ctx → advance again → assert exactly two notifications delivered,
        // prev_head of the second matches new_head of the first.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        // Advance X → Y locally.
        local_commit(clone.path(), "a.txt", "advance 1");
        let y_head = head(clone.path());
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Tear down ctx; rebuild from DB (simulated restart).
        drop(ctx);
        let (ctx2, remote_url) =
            rebuild_ctx_from_persisted(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"])
                .await;

        // Cursor row must equal Y at this point.
        let persisted_head = {
            let conn = db.lock().await;
            brenn_lib::repo_sync_cursor::load_all(&conn)
                .get("src-x")
                .cloned()
                .expect("cursor row present")
        };
        assert_eq!(persisted_head, y_head);

        // Advance Y → Z.
        local_commit(clone.path(), "b.txt", "advance 2");
        run_cycle_for_remote(&ctx2, &remote_url, "poll", None).await;

        let events = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(events.len(), 2, "exactly two notifications across restart");

        // prev_head of the second event isn't in the payload today, but
        // the invariant is captured transitively: the first event's
        // `oneline` range ended at Y, and the second event's range
        // starts from Y (the cursor we seeded from) — verified by the
        // cursor equalling Y at restart time above.
        let payload2: serde_json::Value = serde_json::from_str(&events[1].payload).unwrap();
        assert_eq!(payload2["kind"], "local");
    }

    #[tokio::test]
    async fn force_push_history_rewrite_after_restart_fires_oneline_unavailable_placeholder() {
        // Advance X → Y (cursor committed). Simulate a restart (rebuild ctx
        // from DB, last_notified_head seeded to Y). Force-push remote to Z
        // (rewrites history, Y unreachable). Manually advance local clone to Z
        // via `reset --hard origin/main`. Run a cycle:
        //   - `pull_clone` returns UpToDate (local == remote == Z)
        //   - `detect_and_notify_advance` sees prev=Y, current=Z; tries
        //     `collect_oneline(Y..Z)`, which fails because Y is not an ancestor
        //     of Z → falls back to `oneline_unavailable` placeholder.
        let (remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Advance X → Y: sibling pushes, poll cycle fetches + merges.
        push_sibling_commit(remote.path(), "commit-y");
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        let events_after_y = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(
            events_after_y.len(),
            1,
            "advance X->Y should produce one event"
        );
        // Y is now the persisted cursor head.

        // Simulate restart: drop ctx, rebuild from DB. The restored context
        // has last_notified_head seeded from the cursor row (= Y).
        drop(ctx);
        let (ctx2, remote_url) =
            rebuild_ctx_from_persisted(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"])
                .await;

        // Force-push: rewrite remote history so Y is no longer reachable.
        // Build a sibling clone, reset to the parent of Y, add a new commit
        // (Z), and force-push. Z is now origin/main; Y is unreachable.
        let force_sibling = tempfile::tempdir().unwrap();
        std::fs::remove_dir_all(force_sibling.path()).unwrap();
        run_git(
            std::path::Path::new("/tmp"),
            &[
                "clone",
                &remote.path().display().to_string(),
                force_sibling.path().to_str().unwrap(),
            ],
        );
        run_git(force_sibling.path(), &["reset", "--hard", "HEAD~1"]);
        std::fs::write(force_sibling.path().join("rewrite.txt"), "force").unwrap();
        run_git(force_sibling.path(), &["add", "."]);
        run_git(
            force_sibling.path(),
            &["commit", "-m", "force-pushed replacement"],
        );
        run_git(force_sibling.path(), &["push", "--force", "origin", "main"]);

        // Advance the local tracking clone to Z via fetch + hard-reset,
        // simulating an external "git reset --hard origin/main". After this
        // the local clone HEAD == Z; the cursor still says Y (from the DB
        // restored in rebuild_ctx_from_persisted).
        run_git(clone.path(), &["fetch", "origin"]);
        run_git(clone.path(), &["reset", "--hard", "origin/main"]);
        // Prune Y's objects from the local store so `collect_oneline(Y..Z)`
        // fails with a "bad object" error rather than traversing a stale pack.
        // This exercises the unreachable-ancestor failure mode, which is the
        // correct path to test here (a fabricated invalid SHA would test a
        // different "bad revision" path instead).
        run_git(clone.path(), &["reflog", "expire", "--expire=now", "--all"]);
        run_git(clone.path(), &["gc", "--prune=now", "--quiet"]);

        // Run a cycle. `pull_clone` returns UpToDate (local == remote == Z).
        // `detect_and_notify_advance` sees prev=Y, current=Z, tries
        // `collect_oneline(Y..Z)` -- fails because Y is not an ancestor of Z
        // in the rewritten history -- and falls back to `oneline_unavailable`.
        run_cycle_for_remote(&ctx2, &remote_url, "poll", None).await;

        let events = pending_repo_sync_events(&db, conv_id).await;
        assert_eq!(
            events.len(),
            2,
            "advance after force-push must fire a notification (got {} events)",
            events.len(),
        );
        let last_payload: serde_json::Value = serde_json::from_str(&events[1].payload).unwrap();
        // pull_clone returned UpToDate (local == Z already), so the advance
        // kind must be "local" (not "pulled").
        assert_eq!(
            last_payload["kind"], "local",
            "force-push scenario: pull returned UpToDate so kind must be 'local'"
        );
        let oneline = last_payload["oneline"]
            .as_array()
            .expect("oneline must be an array");
        assert_eq!(oneline.len(), 1, "unavailable placeholder is one entry");
        assert!(
            oneline[0]
                .as_str()
                .unwrap_or("")
                .contains("<oneline unavailable>"),
            "expected oneline_unavailable placeholder, got {oneline:?}",
        );
    }

    #[tokio::test]
    async fn advance_with_acting_only_consumer_still_persists_cursor() {
        // Design-load-bearing invariant for `handle_advanced`: when the
        // acting-conversation filter leaves `targets` empty, the cursor
        // must STILL advance (via `upsert_and_enqueue(..., &[])`). Without
        // this, the next cycle on the same HEAD would re-notify the
        // would-have-been-empty fan-out after any concurrent consumer
        // appeared — and worse, a restart would re-seed from the stale
        // prior cursor, potentially firing a duplicate on the next
        // genuine advance. Covered at the `repo_sync_cursor` unit level
        // with `upsert_and_enqueue_with_empty_rows_still_advances_cursor`;
        // this is the matching reactor-level integration check.
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_acting = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        // Advance locally; the only consumer is also the acting conv, so
        // after filtering, `targets` is empty.
        local_commit(clone.path(), "a.txt", "acting-only advance");
        let post_head = head(clone.path());
        run_cycle_for_remote(&ctx, &remote_url, "push", Some(conv_acting)).await;

        // Acting conv suppressed → no event rows.
        assert_eq!(pending_repo_sync_count(&db, conv_acting).await, 0);
        // But the cursor MUST have advanced.
        let persisted = {
            let conn = db.lock().await;
            brenn_lib::repo_sync_cursor::load_all(&conn)
        };
        assert_eq!(
            persisted.get("src-x").map(String::as_str),
            Some(post_head.as_str()),
            "cursor must advance even when acting-conversation filter empties targets",
        );

        // Second-order check: after restart (fresh cache seeded from the
        // persisted cursor), a cycle on the same HEAD must be Unchanged —
        // no late notification for an advance that was "only for the
        // acting conv".
        drop(ctx);
        let (ctx2, remote_url) =
            rebuild_ctx_from_persisted(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"])
                .await;
        run_cycle_for_remote(&ctx2, &remote_url, "poll", None).await;
        assert_eq!(
            pending_repo_sync_count(&db, conv_acting).await,
            0,
            "restart must not re-notify an advance whose fan-out was empty",
        );
    }

    /// Regression: ensures the per-remote Mutex lives across the whole
    /// cycle including advance-detection. Two concurrent cycles on the
    /// same remote must not both emit a notification for the same
    /// HEAD advance (that would duplicate notifications).
    #[tokio::test]
    async fn concurrent_cycles_on_same_remote_serialize_and_dedupe_advance() {
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        local_commit(clone.path(), "y.txt", "one advance");

        // Fire two concurrent cycles for the same remote.
        let (r1, r2) = tokio::join!(
            run_cycle_for_remote(&ctx, &remote_url, "poll", None),
            run_cycle_for_remote(&ctx, &remote_url, "poll", None),
        );
        let _ = (r1, r2);

        // Despite two cycles, only one notification fired — per-remote
        // Mutex serializes, and whoever wins second finds last_notified
        // == current and no-ops.
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 1);
    }

    /// Regression guard: fan-out runs AFTER the per-remote lock is released,
    /// so a second cycle for the same remote can begin (and complete) while the
    /// first cycle's fan-out is still in progress.
    ///
    /// Shape:
    /// 1. Seed cycle (cycle 1): records `last_notified_head`. No notify or
    ///    gate effect (pending is empty after seed).
    /// 2. Commit on disk.
    /// 3. Spawn cycle 2 in background with `pre_fanout_gate` at 0 permits
    ///    and `post_lock_release_notify` set.
    ///    Cycle 2 pulls the commit, writes `last_notified_head`, releases the
    ///    per-remote lock, fires the notify (only fires when pending is
    ///    non-empty), then blocks at the gate.
    /// 4. From the test task: wait on the notify (deterministic — fires only
    ///    after the lock is released and cycle 2 has pending work, i.e. is
    ///    about to block at the gate), then run cycle 3 for the same remote.
    ///    Because the gate only blocks when `pending` is non-empty, and cycle 3
    ///    observes `last_notified_head` == current HEAD (written by cycle 2
    ///    before the gate), cycle 3 produces no new notification and passes
    ///    through the gate instantly.
    ///    If the lock were still held by cycle 2, cycle 3 would deadlock here.
    /// 5. Release the gate → cycle 2's fan-out completes.
    /// 6. Assert exactly 1 pending event (the one cycle 2 enqueued).
    #[tokio::test]
    async fn fan_out_runs_after_lock_release_allowing_second_cycle_to_enter() {
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;

        // Gate: cycle 2 blocks before fan-out until we add a permit.
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        // Notify: cycle 2 fires this immediately after releasing the lock and
        // before reaching the gate. The test waits here before dispatching
        // cycle 3, making the ordering deterministic rather than probabilistic.
        let lock_released = Arc::new(tokio::sync::Notify::new());

        let (base_ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);
        let ctx_with_gate = RepoSyncCtx {
            pre_fanout_gate: Some(gate.clone()),
            post_lock_release_notify: Some(lock_released.clone()),
            ..base_ctx
        };

        // Cycle 1: cold-start seed. No gate or notify effect (pending is empty
        // after seed, so neither branch fires).
        run_cycle_for_remote(&ctx_with_gate, &remote_url, "poll", None).await;

        // Commit: advance HEAD so cycle 2 has real fan-out work.
        local_commit(clone.path(), "gated.txt", "advance for gate test");

        // Spawn cycle 2 — it will pull the commit, write last_notified_head,
        // release the lock, fire `lock_released`, then block at the gate.
        let ctx2 = ctx_with_gate.clone();
        let remote_url2 = remote_url.clone();
        let cycle2_handle = tokio::spawn(async move {
            run_cycle_for_remote(&ctx2, &remote_url2, "poll", None).await;
        });

        // Wait until cycle 2 has provably released the per-remote lock.
        // The notify fires inside run_cycle_for_remote immediately after
        // `drop(guard)` and before the gate check, so receiving it guarantees
        // the lock is free for cycle 3.
        lock_released.notified().await;

        // Cycle 3: same remote, gate still at 0 permits. Because cycle 2
        // released the per-remote lock before the gate, cycle 3 must be able
        // to acquire the lock and complete. It observes last_notified_head ==
        // current HEAD → Unchanged → pending stays empty → gate is skipped
        // (guard only fires when pending is non-empty).
        //
        // If the lock were still held by cycle 2, this call would deadlock.
        run_cycle_for_remote(&ctx_with_gate, &remote_url, "webhook", None).await;

        // No new events from cycle 3 (dedup invariant).
        assert_eq!(
            pending_repo_sync_count(&db, conv_id).await,
            1,
            "cycle 3 must not produce a duplicate notification (dedup invariant)",
        );

        // Release cycle 2's fan-out.
        gate.add_permits(1);
        cycle2_handle.await.expect("cycle 2 task should complete");

        // Final count still 1 — cycle 2's fan-out ran but no bridge was
        // registered, so the row stays pending (queued_no_bridge path).
        assert_eq!(
            pending_repo_sync_count(&db, conv_id).await,
            1,
            "exactly one notification queued after both cycles complete",
        );
    }

    // -----------------------------------------------------------------------
    // Conflict fanout tests.
    //
    // These exercise the `PullOutcome::Conflict` arm of `run_cycle_for_remote`
    // end-to-end, verifying that conflict events reach the DB and accumulate
    // `PendingInject` entries (or are suppressed when `primary_apps` is empty).
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn conflict_fanout_enqueues_event_and_pending() {
        // End-to-end: diverged git state → `run_cycle_for_remote` →
        // `handle_conflict` writes a DB row → event is queryable with the
        // correct source and payload.
        let (remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cycle 1: cold-start seed so last_notified_head is established.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(pending_repo_sync_count(&db, conv_id).await, 0);

        // Diverge: push a sibling commit so remote advances, then commit
        // locally so the clone cannot fast-forward → PullOutcome::Conflict.
        push_sibling_commit(remote.path(), "upstream diverge");
        local_commit(clone.path(), "local.txt", "local diverge");

        // Cycle 2: pull_clone returns Conflict → handle_conflict → DB write.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // At least one conflict event must be present. We filter by source
        // because advance-detection also fires a REPO_SYNC_SOURCE_LOCAL event
        // (the local commit moved HEAD past the seeded value).
        let events = pending_repo_sync_events(&db, conv_id).await;
        let conflict_events: Vec<_> = events
            .iter()
            .filter(|e| e.source == brenn_lib::messaging::REPO_SYNC_SOURCE_CONFLICT)
            .collect();
        assert!(
            !conflict_events.is_empty(),
            "expected at least one conflict event, got {:?}",
            events.iter().map(|e| &e.source).collect::<Vec<_>>(),
        );

        let ev = &conflict_events[0];
        let payload: serde_json::Value = serde_json::from_str(&ev.payload).unwrap_or_else(|e| {
            panic!(
                "conflict event payload not valid JSON: {e}\npayload: {:?}",
                ev.payload
            )
        });
        assert_eq!(
            payload["kind"],
            brenn_lib::messaging::REPO_SYNC_KIND_CONFLICT,
            "conflict event payload must have kind=conflict",
        );
        assert_eq!(
            payload["slug"]
                .as_str()
                .expect("slug field must be a string"),
            "src-x",
            "conflict event payload slug must match clone slug",
        );
        assert!(
            !payload["reason"]
                .as_str()
                .expect("reason field must be a string")
                .is_empty(),
            "conflict event payload reason must be non-empty",
        );
        assert_eq!(
            payload["remote"]
                .as_str()
                .expect("remote field must be a string"),
            remote_url.as_str(),
            "conflict event payload remote must match the remote URL",
        );
    }

    #[tokio::test]
    async fn conflict_ro_only_no_enqueue() {
        // RO-only (empty primary_apps): conflict path fires an alert via
        // AlertDispatcher but does NOT write any DB rows.
        let (remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        // Build ctx with empty app_slugs → both primary_apps and consumer_apps
        // are empty → handle_conflict returns Vec::new() and advance-detection
        // fans out to no one.
        let (ctx, remote_url) = build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &[]);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert_eq!(
            total_repo_sync_event_count(&db).await,
            0,
            "seed cycle must not write any events",
        );

        // Diverge: same setup as Test 1.
        push_sibling_commit(remote.path(), "upstream diverge");
        local_commit(clone.path(), "local.txt", "local diverge");

        // Cycle 2: Conflict outcome → handle_conflict returns Vec::new()
        // (RO-only branch) → zero DB writes → zero events across all
        // conversations. We check the global count so that a bug removing
        // the `primary_apps.is_empty()` short-circuit would be caught even
        // if the target conversation happened to differ from the one seeded
        // for the per-conv helper.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        assert_eq!(
            total_repo_sync_event_count(&db).await,
            0,
            "RO-only conflict must not write any repo_sync events to the DB",
        );
    }

    // -----------------------------------------------------------------------
    // Escalation policy for persistent-failure alerts.
    //
    // These tests exercise `handle_transient_error`, `handle_auth_error`, and
    // `reset_failure_trackers` at unit granularity. Going through
    // `run_cycle_for_remote` would require injecting a failing git fetch into
    // the filesystem layer, which is more plumbing than the escalation
    // state-machine needs.
    // -----------------------------------------------------------------------

    use brenn_lib::obs::alerting::{CountingAlerter, RateLimiter};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Build a test ctx whose `AlertDispatcher` is a `CountingAlerter`
    /// so escalation tests can observe how many alerts fired. Reuses
    /// `build_ctx_with_dispatcher` for the rest of the plumbing. The
    /// returned `JoinHandle` must stay alive (the dispatcher's
    /// background task would exit otherwise).
    async fn escalation_ctx(
        slug: &str,
    ) -> (
        RepoSyncCtx,
        Arc<AtomicU32>,
        CloneInfo,
        tokio::task::JoinHandle<()>,
    ) {
        let count = Arc::new(AtomicU32::new(0));
        let alerter = CountingAlerter(count.clone());
        let rate_limiter = RateLimiter::new(1024, 60);
        let (dispatcher, handle) =
            brenn_lib::obs::alerting::AlertDispatcher::new(alerter, rate_limiter);
        let (ctx, _remote_url, info) = build_ctx_with_dispatcher(
            brenn_lib::db::init_db_memory(),
            PathBuf::from("/tmp/unused"),
            slug,
            &[],
            dispatcher,
        );
        (ctx, count, info, handle)
    }

    /// Wait for the background alerter task to drain queued messages.
    /// `AlertDispatcher::alert` is non-blocking; alerts are delivered
    /// via an mpsc channel, so tests must yield to let the background
    /// task process them before asserting on the count.
    async fn wait_for_alerts(count: &AtomicU32, expected: u32) {
        for _ in 0..50 {
            if count.load(Ordering::SeqCst) >= expected {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn auth_error_fires_on_first_occurrence() {
        let (ctx, count, info, _h) = escalation_ctx("src-x").await;
        handle_auth_error(&ctx, &info, "ssh publickey rejected", "Permission denied");
        wait_for_alerts(&count, 1).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn auth_error_repeated_does_not_refire_during_incident() {
        let (ctx, count, info, _h) = escalation_ctx("src-x").await;
        for _ in 0..5 {
            handle_auth_error(&ctx, &info, "ssh publickey rejected", "Permission denied");
        }
        wait_for_alerts(&count, 1).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // Counter still ticks (observability); alert gate is the
        // `alerted` flag, not the counter itself.
        let state = ctx.failure_state.lock().unwrap();
        let tracker = state.auth.get("src-x").expect("tracker exists");
        assert_eq!(tracker.consecutive, 5);
        assert!(tracker.alerted);
    }

    #[tokio::test]
    async fn auth_error_fresh_incident_after_recovery_refires() {
        let (ctx, count, info, _h) = escalation_ctx("src-x").await;
        handle_auth_error(&ctx, &info, "ssh publickey rejected", "Permission denied");
        reset_failure_trackers(&ctx, "src-x");
        handle_auth_error(&ctx, &info, "ssh publickey rejected", "Permission denied");
        wait_for_alerts(&count, 2).await;
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn transient_error_does_not_fire_below_threshold() {
        let (ctx, count, info, _h) = escalation_ctx("src-x").await;
        for _ in 0..(TRANSIENT_ESCALATION_THRESHOLD - 1) {
            handle_transient_error(&ctx, &info, "connection timed out");
        }
        // Small pause to let the background alerter run if it was going to.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn transient_error_fires_at_threshold() {
        let (ctx, count, info, _h) = escalation_ctx("src-x").await;
        for _ in 0..TRANSIENT_ESCALATION_THRESHOLD {
            handle_transient_error(&ctx, &info, "connection timed out");
        }
        wait_for_alerts(&count, 1).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn transient_error_above_threshold_does_not_refire() {
        let (ctx, count, info, _h) = escalation_ctx("src-x").await;
        for _ in 0..(TRANSIENT_ESCALATION_THRESHOLD * 3) {
            handle_transient_error(&ctx, &info, "connection timed out");
        }
        wait_for_alerts(&count, 1).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reset_failure_trackers_clears_both_classes() {
        // reset_failure_trackers removes the slug's entry from both
        // the transient and auth maps, and the `alerted` flag is gone
        // with it. Subsequent failures start fresh.
        let (ctx, count, info, _h) = escalation_ctx("src-x").await;
        for _ in 0..TRANSIENT_ESCALATION_THRESHOLD {
            handle_transient_error(&ctx, &info, "connection timed out");
        }
        handle_auth_error(
            &ctx,
            &info,
            "authentication failed",
            "Authentication failed",
        );
        wait_for_alerts(&count, 2).await;
        assert_eq!(count.load(Ordering::SeqCst), 2);

        reset_failure_trackers(&ctx, "src-x");
        {
            let state = ctx.failure_state.lock().unwrap();
            assert!(state.transient.is_empty());
            assert!(state.auth.is_empty());
        }

        // Fresh transient run should require another full threshold to page.
        for _ in 0..(TRANSIENT_ESCALATION_THRESHOLD - 1) {
            handle_transient_error(&ctx, &info, "connection timed out");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::SeqCst), 2);
        handle_transient_error(&ctx, &info, "connection timed out");
        wait_for_alerts(&count, 3).await;
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    /// Integration test: drive `run_cycle_for_remote` end-to-end with a
    /// remote that will fail to fetch (bogus DNS name), and verify the
    /// reactor's match-arm wiring actually increments the transient
    /// counter. Catches any future refactor that forgets to call
    /// `handle_transient_error` from the match arm — the unit tests
    /// above call the handler directly and would pass silently if the
    /// reactor stopped invoking it.
    #[tokio::test]
    async fn run_cycle_routes_transient_fetch_failure_to_transient_tracker() {
        // Build a real clone, then swap its `origin` URL to a
        // DNS-failing host so `git fetch` classifies as transient.
        let (_remote, clone) = scratch_remote_and_clone();
        run_git(
            clone.path(),
            &[
                "remote",
                "set-url",
                "origin",
                "ssh://git@definitely-nonexistent.example.invalid/none.git",
            ],
        );

        let db = brenn_lib::db::init_db_memory();
        // Conv is required so `consumer_apps` resolves but we don't
        // actually need a notification for this test.
        let _conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Cold-start cycle: the fetch fails immediately, so we never
        // reach the cold-start seed-only path — we get a
        // `TransientError` outcome. The transient tracker should show
        // consecutive=1.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        let state = ctx.failure_state.lock().unwrap();
        let transient = state
            .transient
            .get("src-x")
            .expect("transient tracker must be populated after a TransientError cycle");
        assert_eq!(
            transient.consecutive, 1,
            "reactor match arm must dispatch TransientError to handle_transient_error",
        );
        assert!(
            !transient.alerted,
            "one failure should not cross the threshold",
        );
        // Auth tracker must be untouched — cross-class bleed would be
        // a bug in the dispatch wiring.
        assert!(
            !state.auth.contains_key("src-x"),
            "auth tracker must not be populated by a transient-class outcome",
        );
    }

    /// Integration test: drive `run_cycle_for_remote` end-to-end with a
    /// healthy clone and verify the match arm resets failure trackers
    /// on UpToDate. Catches a future regression where the reactor
    /// forgets to call `reset_failure_trackers` in the success path.
    #[tokio::test]
    async fn run_cycle_resets_failure_trackers_on_successful_fetch() {
        let (_remote, clone) = scratch_remote_and_clone();
        let db = brenn_lib::db::init_db_memory();
        let _conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx(db.clone(), clone.path().to_path_buf(), "src-x", &["appa"]);

        // Seed both trackers so we can observe them being cleared.
        {
            let mut state = ctx.failure_state.lock().unwrap();
            state.transient.insert(
                "src-x".to_string(),
                crate::repo_sync::FailureTracker {
                    consecutive: 3,
                    alerted: false,
                },
            );
            state.auth.insert(
                "src-x".to_string(),
                crate::repo_sync::FailureTracker {
                    consecutive: 1,
                    alerted: true,
                },
            );
        }

        // Healthy remote → UpToDate outcome → match arm calls
        // reset_failure_trackers.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        let state = ctx.failure_state.lock().unwrap();
        assert!(
            !state.transient.contains_key("src-x"),
            "UpToDate match arm must reset the transient tracker",
        );
        assert!(
            !state.auth.contains_key("src-x"),
            "UpToDate match arm must reset the auth tracker",
        );
    }

    // -----------------------------------------------------------------------
    // Post-pull hook dispatch tests.
    //
    // These verify the reactor's hook dispatch wiring: hooks fire on
    // AdvanceKind::Pulled, do NOT fire on Local, and coalesce via
    // try_lock when overlapping.
    // -----------------------------------------------------------------------

    use brenn_lib::config::{
        AppConfig, CompactionConfig, PathMapper, PostPullHooksConfig, StartHooksConfig,
        StartupHooksConfig,
    };

    /// Build an AppConfig with a post-pull hook that writes a marker file.
    fn mk_hook_app(slug: &str, working_dir: PathBuf, marker_name: &str) -> AppConfig {
        AppConfig {
            slug: slug.to_string(),
            name: slug.to_string(),
            description: String::new(),
            icon: String::new(),
            working_dir: working_dir.clone(),
            model: "sonnet".to_string(),
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout: None,
            compaction: None::<CompactionConfig>,
            idle_hook_secs: 0,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: std::collections::HashMap::new(),
            multiuser: false,
            prefix_username: false,
            prefix_timestamp: false,
            prefix_device: true,
            path_mapper: PathMapper::Identity,
            container_spawn: None,
            start_hooks: StartHooksConfig::default(),
            post_pull_hooks: PostPullHooksConfig {
                host: vec![format!("touch {marker_name}")],
                container: vec![],
            },
            startup_hooks: StartupHooksConfig::default(),
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: std::collections::HashMap::new(),
            mounts: vec![],
            history_replay_limit: 2000,
            frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
            state_dir: PathBuf::from("/tmp/.brenn/test-state"),
            messaging: None,
            messaging_default_send_budget: 100,
            policy: brenn_lib::access::AppPolicy::default(),
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
        }
    }

    /// Build a `RepoSyncCtx` that includes an app with post-pull hooks.
    fn build_ctx_with_hooks(
        db: brenn_lib::db::Db,
        clone_path: PathBuf,
        slug: &str,
        app_slug: &str,
        app: AppConfig,
    ) -> (RepoSyncCtx, String) {
        let (dispatcher, _handle) = AlertDispatcher::noop();
        let remote_url = format!("ssh://example/{slug}.git");
        let info = CloneInfo {
            slug: slug.to_string(),
            host_path: clone_path,
            remote: remote_url.clone(),
            sync_enabled: true,
            consumer_apps: [app_slug.to_string()].into(),
            primary_apps: [app_slug.to_string()].into(),
        };
        let clones: HashMap<String, CloneInfo> =
            [(slug.to_string(), info.clone())].into_iter().collect();
        let mut remote_to_slugs: HashMap<String, Vec<String>> = HashMap::new();
        remote_to_slugs.insert(remote_url.clone(), vec![slug.to_string()]);
        let mut remote_locks: HashMap<String, Arc<TokioMutex<()>>> = HashMap::new();
        remote_locks.insert(remote_url.clone(), Arc::new(TokioMutex::new(())));

        let has_hooks =
            !app.post_pull_hooks.host.is_empty() || !app.post_pull_hooks.container.is_empty();
        let mut apps = indexmap::IndexMap::new();
        apps.insert(app_slug.to_string(), app);
        let mut hook_locks: HashMap<String, Arc<TokioMutex<()>>> = HashMap::new();
        if has_hooks {
            hook_locks.insert(app_slug.to_string(), Arc::new(TokioMutex::new(())));
        }

        let ctx = RepoSyncCtx {
            db,
            active_bridges: ActiveBridges::new(),
            alert_dispatcher: dispatcher,
            clones: Arc::new(clones),
            remote_to_slugs: Arc::new(remote_to_slugs),
            remote_locks: Arc::new(remote_locks),
            last_notified_head: Arc::new(std::sync::Mutex::new(HashMap::new())),
            failure_state: Arc::new(std::sync::Mutex::new(
                crate::repo_sync::PersistentFailureState::default(),
            )),
            apps: Arc::new(apps),
            post_pull_hook_locks: Arc::new(hook_locks),
            pre_fanout_gate: None,
            post_lock_release_notify: None,
        };
        (ctx, remote_url)
    }

    /// Wait for a marker file to appear, with timeout.
    async fn wait_for_marker(path: &Path, timeout_ms: u64) -> bool {
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_ms);
        while tokio::time::Instant::now() < deadline {
            if path.exists() {
                return true;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
        false
    }

    #[tokio::test]
    async fn post_pull_hook_fires_on_pulled() {
        let (remote, clone) = scratch_remote_and_clone();
        let hook_dir = tempfile::tempdir().unwrap();
        let marker = hook_dir.path().join("hook_ran");

        let app = mk_hook_app("appa", hook_dir.path().to_path_buf(), "hook_ran");
        let db = brenn_lib::db::init_db_memory();
        let _conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx_with_hooks(db.clone(), clone.path().to_path_buf(), "src-x", "appa", app);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Sibling push → remote advance.
        push_sibling_commit(remote.path(), "external commit");

        // Cycle 2: fast-forward → Pulled → post-pull hook fires.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        assert!(
            wait_for_marker(&marker, 5000).await,
            "post-pull hook should have created marker file on Pulled advance"
        );
    }

    #[tokio::test]
    async fn post_pull_hook_does_not_fire_on_local() {
        let (_remote, clone) = scratch_remote_and_clone();
        let hook_dir = tempfile::tempdir().unwrap();
        let marker = hook_dir.path().join("hook_ran");

        let app = mk_hook_app("appa", hook_dir.path().to_path_buf(), "hook_ran");
        let db = brenn_lib::db::init_db_memory();
        let _conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx_with_hooks(db.clone(), clone.path().to_path_buf(), "src-x", "appa", app);

        // Cycle 1: cold-start seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Local commit (no push) — AdvanceKind::Local.
        local_commit(clone.path(), "local.txt", "local manual commit");

        // Cycle 2: advance detected as Local → hook must NOT fire.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;
        // Brief yield to let any erroneously spawned task run.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        assert!(
            !marker.exists(),
            "post-pull hook must not fire on AdvanceKind::Local"
        );
    }

    #[tokio::test]
    async fn post_pull_hook_coalesces_concurrent_invocations() {
        let (remote, clone) = scratch_remote_and_clone();
        let hook_dir = tempfile::tempdir().unwrap();

        // Hook sleeps 500ms then writes a counter file. Two rapid pulls
        // should result in only one invocation (second try_lock skips).
        let app = AppConfig {
            post_pull_hooks: PostPullHooksConfig {
                host: vec![format!(
                    "count=$(cat {dir}/count 2>/dev/null || echo 0); \
                     echo $((count + 1)) > {dir}/count; \
                     sleep 0.5",
                    dir = hook_dir.path().display(),
                )],
                container: vec![],
            },
            ..mk_hook_app("appa", hook_dir.path().to_path_buf(), "unused")
        };
        let db = brenn_lib::db::init_db_memory();
        let _conv_id = mk_conv(&db, "appa").await;
        let (ctx, remote_url) =
            build_ctx_with_hooks(db.clone(), clone.path().to_path_buf(), "src-x", "appa", app);

        // Seed.
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Two rapid remote advances.
        push_sibling_commit(remote.path(), "commit-1");
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        push_sibling_commit(remote.path(), "commit-2");
        run_cycle_for_remote(&ctx, &remote_url, "poll", None).await;

        // Wait for the first (and possibly only) hook to finish.
        let count_file = hook_dir.path().join("count");
        assert!(
            wait_for_marker(&count_file, 5000).await,
            "at least one hook invocation should have run"
        );
        // Give a bit more time for any second invocation.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let count: u32 = std::fs::read_to_string(&count_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // The first hook takes 500ms. The second cycle's try_lock fires
        // while the first is still running, so it should skip. Count = 1.
        assert_eq!(
            count, 1,
            "concurrent hook invocations should coalesce — expected 1, got {count}"
        );
    }

    #[tokio::test]
    async fn transient_and_auth_trackers_are_independent() {
        // Alternating auth and transient outcomes shouldn't reset each
        // other. Each tracker counts its own class in isolation.
        // (Only a non-auth / non-transient outcome resets — see
        // `reset_failure_trackers`.)
        let (ctx, count, info, _h) = escalation_ctx("src-x").await;
        handle_auth_error(&ctx, &info, "ssh publickey rejected", "Permission denied");
        for _ in 0..TRANSIENT_ESCALATION_THRESHOLD {
            handle_transient_error(&ctx, &info, "connection timed out");
        }
        wait_for_alerts(&count, 2).await;
        // One auth alert + one transient alert = two total.
        assert_eq!(count.load(Ordering::SeqCst), 2);

        let state = ctx.failure_state.lock().unwrap();
        assert_eq!(state.auth.get("src-x").unwrap().consecutive, 1);
        assert_eq!(
            state.transient.get("src-x").unwrap().consecutive,
            TRANSIENT_ESCALATION_THRESHOLD,
        );
    }

    // -----------------------------------------------------------------------
    // Wire-shape regression guards for typed event payload structs (A2, A3)
    // -----------------------------------------------------------------------

    /// A2: `RepoSyncAdvanceEvent` must serialize byte-identically to the source
    /// `json!` literal in `handle_advanced`.
    #[test]
    fn repo_sync_advance_matches_reference() {
        // Non-empty oneline.
        let oneline: Vec<String> = vec!["abc1234 add readme".to_string()];
        let event = super::RepoSyncAdvanceEvent {
            kind: brenn_lib::messaging::REPO_SYNC_KIND_PULLED,
            slug: "my-repo",
            remote: "https://git.example.com/user/repo.git",
            oneline: &oneline,
        };
        let produced = serde_json::to_string(&event)
            .expect("RepoSyncAdvanceEvent serialization is infallible");
        let reference = serde_json::json!({
            "kind": brenn_lib::messaging::REPO_SYNC_KIND_PULLED,
            "slug": "my-repo",
            "remote": "https://git.example.com/user/repo.git",
            "oneline": ["abc1234 add readme"],
        })
        .to_string();
        assert_eq!(produced, reference);

        // Empty oneline (no commits visible).
        let empty: Vec<String> = vec![];
        let event2 = super::RepoSyncAdvanceEvent {
            kind: brenn_lib::messaging::REPO_SYNC_KIND_LOCAL,
            slug: "another-repo",
            remote: "https://git.example.com/user/another.git",
            oneline: &empty,
        };
        let produced2 = serde_json::to_string(&event2)
            .expect("RepoSyncAdvanceEvent serialization is infallible");
        let reference2 = serde_json::json!({
            "kind": brenn_lib::messaging::REPO_SYNC_KIND_LOCAL,
            "slug": "another-repo",
            "remote": "https://git.example.com/user/another.git",
            "oneline": [],
        })
        .to_string();
        assert_eq!(produced2, reference2);
    }

    /// A3: `RepoSyncConflictEvent` must serialize byte-identically to the source
    /// `json!` literal in `handle_conflict`.
    #[test]
    fn repo_sync_conflict_matches_reference() {
        let event = super::RepoSyncConflictEvent {
            kind: brenn_lib::messaging::REPO_SYNC_KIND_CONFLICT,
            slug: "conflicted-repo",
            remote: "https://git.example.com/user/conflicted.git",
            reason: "diverged",
            detail: "local HEAD has commits not on remote",
        };
        let produced = serde_json::to_string(&event)
            .expect("RepoSyncConflictEvent serialization is infallible");
        let reference = serde_json::json!({
            "kind": brenn_lib::messaging::REPO_SYNC_KIND_CONFLICT,
            "slug": "conflicted-repo",
            "remote": "https://git.example.com/user/conflicted.git",
            "reason": "diverged",
            "detail": "local HEAD has commits not on remote",
        })
        .to_string();
        assert_eq!(produced, reference);
    }
}

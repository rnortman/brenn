//! Unified message dispatch: `dispatch_row` primitive and background dispatcher task.
//!
//! `dispatch_row` is the single site that decides how to deliver an
//! already-durably-parked pending-push row: inject into a live bridge,
//! eager-wake a sleeping bridge, or route to the WASM off-loop task.
//!
//! `spawn_dispatcher_task` / `dispatcher_loop` is the single background task
//! that replaces `deliver_after_loop` + `deadline_loop` (design §2.3, §2.7).
//! It folds:
//!   - Deliver-after release scanning (`release_due_pushes`, `register_released_pushes`)
//!   - Deadline scanning
//!   - Immediate/dispatchable-row global scan (`load_all_dispatchable_pushes`)
//!   - Per-bridge fan-out (D-a) for head-of-line isolation
//!   - In-flight dedup (R5, D-d) via `Mutex<HashSet<String>>` keyed by subscriber

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use rusqlite::OptionalExtension;
use tokio::sync::Notify;

use super::WakeRouter;
use super::config::Depth;
use super::db::{
    self, ReleasedPushRow, delete_pending_push_by_id, earliest_pending_deadline,
    earliest_pending_release, load_released_push_window_rows, release_due_pushes,
};
use super::ingress::IngressOrBus;
use super::publish::DispatchOutcome;
use super::{
    DeliveryShape, Messenger, SubscriberEntryKind, SubscriberKind, WakeEconomics, registration_key,
};
use crate::db::Db;

/// Polling fallback interval: maximum delay between automatic wake-ups even
/// with no kick. 60 seconds matches the former deliver-after and deadline loop
/// constants.
pub const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Debounce after firing eager wakes for past-deadline rows (review F4).
/// The wake takes seconds to land (CC spawn + drain); without the debounce the
/// loop re-queries the same past-deadline rows immediately and spins.
const PAST_DEADLINE_DEBOUNCE: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// dispatch_row — the single dispatch primitive (design §2.4, R12)
// ---------------------------------------------------------------------------

/// Dispatch an already-durably-parked pending-push row to the appropriate
/// delivery mechanism.
///
/// The row is *always already parked* in `messaging_pending_pushes` before
/// this function runs (the DB insert precedes any dispatch call). This
/// function's sole job is choosing the right mechanism:
/// The row's registration key ([`registration_key`]) selects the subscriber's
/// [`DeliveryShape`] from the router's binding:
/// - `Inline` → inject via `router.deliver()` (bus) or `router.deliver_ingress()`
///   (ingress). `Ok(true)` ⇒ `Delivered` (or `DeliveredNoRemark` when the
///   binding marks its own delivery); no bridge / bridge-died-mid-send →
///   eager-wake if `Immediate` or `deadline_expired`, then `Parked`.
/// - `ParkedWake` → route to the off-loop parked task via `spawn_eager_wake`;
///   `router.deliver()` is never called (WASM/system subscribers).
///
/// `deadline_expired`: when `true`, the `wake_kind`-gated eager-wake
/// check is overridden — a past-deadline row always triggers an eager wake
/// on the no-bridge / error branches regardless of `wake_kind` (R6 deadline
/// override, design §2.4).
///
/// `wake_gated`: when `true`, an eager wake is suppressed for `eager_wake` rows
/// (the caller's wake cooldown is active). Delivery is never gated — only the
/// re-firing of `spawn_eager_wake`. `deadline_expired` still forces the wake.
/// The caller sets this only for Conversation groups, so surface/wasm wakes are
/// never gated even though this function stays kind-agnostic.
///
/// Returns `DispatchOutcome::Delivered(push_id)` only when the underlying
/// `router.deliver()` / `router.deliver_ingress()` returns `Ok(true)`.
/// All other outcomes return `Parked { woke }`, where `woke` reports whether an
/// eager wake actually fired.
pub async fn dispatch_row(
    router: &dyn WakeRouter,
    row: &db::PendingPushRow,
    deadline_expired: bool,
    wake_gated: bool,
) -> DispatchOutcome {
    // Resolve the row's registration key once, then let the subscriber's
    // registered delivery binding — not its identity prefix — shape dispatch.
    let key = registration_key(&row.target_subscriber, &row.target_app_slug);

    // ParkedWake: never call router.deliver() for these targets (it panics by
    // design). Route to the off-loop dispatch task via spawn_eager_wake and
    // mirror the no-bridge eager-wake condition — only `Immediate` rows (or
    // deadline-expired rows) wake here.
    let marks_own_delivery = match router.delivery_shape(&key) {
        DeliveryShape::ParkedWake => {
            let should_wake = (row.eager_wake && !wake_gated) || deadline_expired;
            if should_wake {
                router.spawn_eager_wake(&key, &row.target_subscriber);
            }
            return DispatchOutcome::Parked { woke: should_wake };
        }
        DeliveryShape::Inline { marks_own_delivery } => marks_own_delivery,
    };

    // Dispatch based on payload kind: bus envelopes go through deliver(),
    // ingress events through deliver_ingress(). Both methods return the
    // same Ok(bool)/Err(String) contract.
    let result = match &row.payload {
        IngressOrBus::Bus(envelope) => {
            router
                .deliver(
                    &key,
                    &row.target_subscriber,
                    envelope,
                    row.push_id,
                    row.message_id,
                )
                .await
        }
        IngressOrBus::Ingress(event) => {
            router
                .deliver_ingress(&key, &row.target_subscriber, event)
                .await
        }
    };

    match result {
        // A binding that marks its own delivery (a surface session's atomic
        // claim is the mark) must not be re-marked by the dispatcher batch: a
        // concurrent session may unclaim a row it owns to re-park it, and a
        // batch re-mark would race that unclaim and permanently retire an
        // undelivered row.
        Ok(true) if marks_own_delivery => DispatchOutcome::DeliveredNoRemark,
        Ok(true) => DispatchOutcome::Delivered(row.push_id),
        Ok(false) => {
            // No active bridge. Eager-wake if this row demands it and the wake
            // is not gated by the caller's cooldown (deadline still forces it).
            let should_wake = (row.eager_wake && !wake_gated) || deadline_expired;
            if should_wake {
                router.spawn_eager_wake(&key, &row.target_subscriber);
            }
            DispatchOutcome::Parked { woke: should_wake }
        }
        Err(e) => {
            // Bridge raced with shutdown / send failed. Same outcome as
            // no-bridge eager-wake — fire so the next bridge can drain.
            // Without this, an eager-wake message with no `delivery_deadline`
            // is silently parked until the user happens to send a chat (review F3).
            tracing::warn!(
                target_subscriber = row.target_subscriber.as_str(),
                error = %e,
                "messaging dispatch failed; leaving pending push undelivered"
            );
            let should_wake = (row.eager_wake && !wake_gated) || deadline_expired;
            if should_wake {
                router.spawn_eager_wake(&key, &row.target_subscriber);
            }
            DispatchOutcome::Parked { woke: should_wake }
        }
    }
}

// ---------------------------------------------------------------------------
// Delivery-time ACL floor (design §2.2 "Enforcement point B")
// ---------------------------------------------------------------------------

/// Re-validate that the target app/WASM subscriber's current `AppPolicy` still
/// authorizes delivery of this parked row on its channel. This is the mandatory
/// delivery floor: a row parked while an ACL was permissive must still be denied
/// if the ACL is gone by the time it would be delivered (design §2.2, the
/// `requirements.md` "ACL enforcement: when and where" floor).
///
/// Gating is uniform over every `Bus` target — `App` and `Wasm`, static and
/// dynamic; there is no provenance branch. `Ingress` rows (synthetic events such
/// as repo_sync) are not channel-subscription deliveries and carry no channel, so
/// they are never gated and always return `true` (they fall through to normal
/// delivery without an envelope read — this is also what keeps the
/// `unwrap_bus_ref` panic path unreachable here).
///
/// Inputs are recovered from the flat `PendingPushRow`, which carries neither the
/// slug nor the address directly:
/// - **Channel address** — read off the bus envelope (`is_bus()`-guarded
///   `unwrap_bus_ref().channel`).
/// - **Subscriber slug** — for a `Conversation(id)` (app-backed) target, recover
///   the app slug via `SELECT app_slug FROM conversations WHERE id = ?` and gate
///   as `App(slug)`; for a `Wasm(slug)` target the slug is read directly off the
///   `ParticipantId` and gates as `Wasm(slug)` (no DB read).
///
/// Fail-closed (deny, not panic): the delivery path is reached by
/// attacker-influenceable inbound traffic, so a missing policy, a missing/NULL
/// `app_slug`, or a `subscriber_policy` miss returns `false` rather than
/// panicking (design §3). Returns `true` only when a resolved policy's
/// `allows_channel_access` covers the channel address.
/// Resolve a parked subscriber's directory `kind` once, for an entire fan-out
/// group. Every row in a group shares one `target_subscriber`
/// (`dispatcher_loop` keys groups on it), so the slug recovery — which is the
/// only DB-touching part of the floor gate — is invariant across the group and
/// is resolved here once rather than per row (efficiency-1/2).
///
/// Returns:
/// - `Some(kind)` — the resolved `App(slug)`/`Wasm(slug)` to gate every row in
///   the group against.
/// - `None` — fail-closed: deny the whole group. Either the `conversations` row
///   is gone / `app_slug` is NULL (a delivery-path wiring bug), or the slug
///   lookup hit a transient DB error. Both deny rather than panic, because the
///   delivery path is reached by attacker-influenceable inbound traffic (design
///   §3); a panic here would be a remotely-triggerable host crash and would
///   leave the group's rows stuck (errhandling-2). Each `None` is logged with
///   its distinct reason so a wiring bug is not silently conflated with an ACL
///   revocation (quality-2).
async fn resolve_subscriber_kind(
    db: &Db,
    target_subscriber: &super::ParticipantId,
) -> Option<SubscriberEntryKind> {
    match target_subscriber.kind() {
        SubscriberKind::Conversation(conversation_id) => {
            let app_slug: Option<String> = {
                let conn = db.lock().await;
                let queried = conn
                    .query_row(
                        "SELECT app_slug FROM conversations WHERE id = ?1",
                        rusqlite::params![conversation_id],
                        |r| r.get::<_, Option<String>>(0),
                    )
                    .optional();
                match queried {
                    Ok(slug) => slug.flatten(),
                    // Transient/unexpected DB error (e.g. SQLITE_BUSY) on the
                    // attacker-reachable delivery floor: fail closed (deny) with
                    // an error-level signal rather than panicking the fan-out
                    // task and stranding its rows (errhandling-2).
                    Err(e) => {
                        tracing::error!(
                            conversation_id,
                            target_subscriber = target_subscriber.as_str(),
                            error = %e,
                            "delivery floor: DB error resolving conversations.app_slug — denying \
                             group (fail-closed)"
                        );
                        return None;
                    }
                }
            };
            match app_slug {
                Some(slug) => Some(SubscriberEntryKind::App(slug)),
                // Conversation row gone or app_slug NULL: a wiring bug on the
                // delivery path — fail closed rather than panic (design §3).
                // Logged distinctly from an ACL deny (quality-2).
                None => {
                    tracing::warn!(
                        conversation_id,
                        target_subscriber = target_subscriber.as_str(),
                        "delivery floor: conversation missing or app_slug NULL — treating as \
                         wiring bug, denying group (not an ACL revocation)"
                    );
                    None
                }
            }
        }
        SubscriberKind::Wasm(slug) => Some(SubscriberEntryKind::Wasm(slug)),
        // Surface subscribers map directly to their entry kind (no DB read — same
        // shape as the Wasm arm). `floor_decision` is kind-agnostic once
        // `subscriber_policy` resolves the surface policy.
        SubscriberKind::Surface { slug, instance } => {
            Some(SubscriberEntryKind::Surface { slug, instance })
        }
        // System-substrate subscribers map directly to their entry kind (no DB
        // read), gated via `system_policies` the same as every other kind.
        SubscriberKind::System(component) => Some(SubscriberEntryKind::System(component)),
    }
}

/// Per-row delivery-floor decision, given the group's already-resolved
/// subscriber `kind` (`None` ⇒ slug recovery failed for the group — DB error or
/// wiring bug, already logged by `resolve_subscriber_kind`).
///
/// Ingress rows are checked **first**: they are not channel-subscription
/// deliveries, carry no channel, and are never gated — so they pass regardless
/// of `group_kind` (a group-level slug-recovery failure must not deny an ingress
/// row, and this keeps the `unwrap_bus_ref` panic path unreachable for them).
///
/// For a bus row: `None` group kind ⇒ fail-closed deny; otherwise a pure
/// in-memory `subscriber_policy` lookup + `allows_channel_access` over the channel
/// address read off the bus envelope. A missing policy fails closed (deny).
fn floor_decision(
    messenger: &Messenger,
    group_kind: Option<&SubscriberEntryKind>,
    row: &db::PendingPushRow,
) -> bool {
    if !row.payload.is_bus() {
        return true;
    }
    let Some(kind) = group_kind else {
        return false;
    };
    let channel_address = &row.payload.unwrap_bus_ref().channel;
    messenger
        .subscriber_policy(kind)
        .is_some_and(|p| p.allows_channel_access(channel_address))
}

// ---------------------------------------------------------------------------
// Background dispatcher task (design §2.3, D-a, D-b, R1, R2)
// ---------------------------------------------------------------------------

/// Spawn the unified background dispatcher task.
///
/// Returns the JoinHandle (process-lifetime task; caller typically drops it).
/// The task replaces the former `deliver_after_loop` + `deadline_loop` and
/// adds the global-scan fan-out required by R1/R2/D-a/D-b.
///
/// `kick` is the single `Arc<Notify>` that signals new-row availability.
/// All publish / edit / release callers notify it via `messenger.dispatch_kick()`.
pub fn spawn_dispatcher_task(
    db: Db,
    router: Arc<dyn WakeRouter>,
    kick: Arc<Notify>,
    messenger: Arc<Messenger>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(dispatcher_loop(db, router, kick, messenger))
}

async fn dispatcher_loop(
    db: Db,
    router: Arc<dyn WakeRouter>,
    kick: Arc<Notify>,
    messenger: Arc<Messenger>,
) {
    // In-flight dedup set (R5, D-d): subscriber keys currently being processed by
    // a spawned per-bridge fan-out task. Keyed by subscriber string (not push_id)
    // because subscriber-level exclusion is required for within-conversation ordering
    // (R10, correctness-1 fix): if any fan-out task for subscriber S is still running,
    // a later scan must not spawn a second concurrent task for S — that would allow
    // two tasks to race acquiring bridge.session.lock() and reorder CC-stdin delivery.
    //
    // A single subscriber-key in the set blocks all rows for that subscriber until the
    // running task finishes, at which point the next scan picks up any remaining rows
    // (including rows published while the task was mid-flight) in publish_ts_ns order.
    let in_flight: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    // Per-subscriber wake cooldown: records when each Conversation subscriber last
    // had spawn_eager_wake fired by a fan-out task. It coalesces Conversation eager
    // wakes only — the one wake with real cost (a CC spawn); re-waking every 60s is
    // a no-op storm. It NEVER suppresses a delivery attempt: every due group still
    // gets its fan-out and per-row dispatch_row, so a live durable/bridge delivery
    // is never delayed by the cooldown. Armed on a fresh wake, cleared when a
    // delivery proves the subscriber live, and left untouched (to expire via
    // elapsed()) on a gated pass. Only Conversation groups are ever gated —
    // surface/wasm wakes are free notify_one calls, and for wasm the notify is the
    // delivery trigger, so gating them would reintroduce delivery latency.
    let recently_woken: Arc<Mutex<HashMap<String, Instant>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut fired_wakes = false;
    loop {
        // Compute sleep target: min(earliest_pending_release, earliest_pending_deadline).
        let (next_release, next_deadline) = {
            let conn = db.lock().await;
            (
                earliest_pending_release(&conn),
                earliest_pending_deadline(&conn),
            )
        };
        let next_due = match (next_release, next_deadline) {
            (Some(r), Some(d)) => Some(r.min(d)),
            (Some(r), None) => Some(r),
            (None, Some(d)) => Some(d),
            (None, None) => None,
        };
        let sleep_dur = match next_due {
            Some(dt) => {
                let now = Utc::now();
                if dt <= now {
                    Duration::from_millis(0)
                } else {
                    let millis = (dt - now).num_milliseconds().max(0) as u64;
                    Duration::from_millis(millis).min(POLL_INTERVAL)
                }
            }
            None => POLL_INTERVAL,
        };

        // Wait for kick, timer expiry, or the debounce (if wakes fired last pass).
        if fired_wakes {
            // Debounce: the spawned wakes take seconds to land; without this the
            // loop would immediately re-query past-deadline rows and spin (review F4).
            // The kick still wins if a publish or drain changes state during the debounce.
            tokio::select! {
                _ = tokio::time::sleep(PAST_DEADLINE_DEBOUNCE) => {}
                _ = kick.notified() => {}
            }
            fired_wakes = false;
        } else {
            tokio::select! {
                _ = tokio::time::sleep(sleep_dur) => {}
                _ = kick.notified() => {}
            }
        }

        // Release any rows whose deliver_after has passed.
        let now = Utc::now();
        let released_ids = {
            let conn = db.lock().await;
            release_due_pushes(&conn, now)
        };
        if !released_ids.is_empty() {
            // Register released rows into their push windows (push-depth enforcement,
            // Gap B). Overflow-retired ids are deleted from DB and excluded from dispatch.
            register_released_pushes(&messenger, &db, &released_ids).await;
        }

        // Load all currently-dispatchable rows (global scan, design §2.3 D-b).
        // Predicate: delivered_at IS NULL AND release_after IS NULL AND
        //   (wake_kind='immediate' OR (delivery_deadline IS NOT NULL AND deadline <= now))
        let due_rows = {
            let conn = db.lock().await;
            db::load_all_dispatchable_pushes(&conn, now)
        };

        if due_rows.is_empty() {
            continue;
        }

        // Group due rows by target_subscriber so each bridge's rows are
        // processed in-order by a single fan-out task (R10: publish_ts_ns
        // order is preserved within a bridge group because the global query
        // is ORDER BY publish_ts_ns ASC).
        //
        // Subscriber-level dedup: if a subscriber already has a live fan-out task
        // (its key is in `in_flight`), skip the entire group. This prevents a second
        // concurrent task from racing the first to acquire bridge.session.lock() and
        // reordering CC-stdin delivery for that conversation (correctness-1 / R10 fix).
        // The skipped rows remain undelivered in the DB and will be picked up on the
        // next scan after the running task completes and removes the subscriber from
        // in_flight.
        let mut groups: HashMap<String, Vec<(db::PendingPushRow, bool)>> = HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for (row, deadline_expired) in due_rows {
                let sub_key = row.target_subscriber.as_str().to_string();
                if inflight.contains(&sub_key) {
                    // Subscriber already has a running fan-out task — skip all its rows.
                    // They will be re-scanned after the current task completes.
                    continue;
                }
                groups
                    .entry(sub_key)
                    .or_default()
                    .push((row, deadline_expired));
            }
            // Insert all new subscriber keys at once, after grouping, before spawning.
            // This is the critical section: all insertions happen before any spawn so
            // the loop cannot re-enter a group between grouping and spawning.
            for sub_key in groups.keys() {
                inflight.insert(sub_key.clone());
            }
        }

        // Snapshot the recently-woken map once for this pass.
        let woken_snapshot = {
            recently_woken
                .lock()
                .expect("recently_woken poisoned")
                .clone()
        };

        // Debug-level pass summary: row count, in-flight-skipped count, and group count.
        // Helps diagnose "why is this row not being dispatched?" without DB queries.
        {
            let inflight_size = in_flight.lock().expect("in_flight poisoned").len();
            tracing::debug!(
                groups = groups.len(),
                in_flight_subscribers = inflight_size,
                "dispatcher pass"
            );
        }

        // Spawn one transient per-bridge fan-out task per subscriber group.
        // The dispatcher loop does NOT await these tasks (no HOL across bridges, R11b).
        // A supervisor task awaits each fan-out JoinHandle to clean up in_flight and
        // recently_woken, and to log+recover from any fan-out task panic (errhandling-1
        // fix: a panic must not leave the subscriber permanently stuck in in_flight).
        for (subscriber_key, rows) in groups {
            // Retained solely for the fired_wakes debounce flag at the end of the
            // group loop (past-deadline rows take seconds to land).
            let has_deadline_rows = rows.iter().any(|(_, de)| *de);

            // Efficiency-1 wake gate: coalesce redundant eager wakes for an
            // `UrgencyGated` subscriber woken within POLL_INTERVAL. This gates only
            // the wake (a CC spawn) — the group is never skipped, so every row's
            // dispatch_row delivery attempt still runs. Only `UrgencyGated` groups
            // are gated: `Eager` (surface/wasm/system) wakes are free notify_one
            // calls, and for parked consumers the notify is the delivery trigger, so
            // gating it would delay delivery. The gate reads the subscriber's
            // declared wake economics per participant (via the registry), not its
            // identity prefix. Deadline-expired rows still wake unconditionally (the
            // override lives per-row inside dispatch_row).
            let first_row = &rows.first().expect("fan-out group is never empty").0;
            let group_key =
                registration_key(&first_row.target_subscriber, &first_row.target_app_slug);
            let urgency_gated = messenger.subscriber_wake_economics(&group_key)
                == Some(WakeEconomics::UrgencyGated);
            let wake_gated = urgency_gated
                && woken_snapshot
                    .get(&subscriber_key)
                    .map(|t| t.elapsed() < POLL_INTERVAL)
                    .unwrap_or(false);

            let router_clone = router.clone();
            let db_clone = db.clone();
            let messenger_clone = messenger.clone();
            // Clone Arcs for the supervisor task (fan-out task gets its own clones below).
            let in_flight_supervisor = in_flight.clone();
            let recently_woken_supervisor = recently_woken.clone();

            let fan_out_handle = tokio::spawn(async move {
                let mut delivered_ids: Vec<i64> = Vec::new();
                // `fired_wake`: any row actually fired an eager wake.
                // `delivered_any`: any row was delivered (Delivered /
                // DeliveredNoRemark) — floor-denied retirements do not count, as
                // they prove nothing about subscriber liveness.
                let mut fired_wake = false;
                let mut delivered_any = false;

                // Delivery floor (design §2.2 Point B): resolve the group's
                // subscriber kind once (slug recovery is invariant across the
                // group — efficiency-1/2). `None` means fail-closed deny for the
                // whole group (DB error or wiring bug); the reason is already
                // logged by `resolve_subscriber_kind`. Retire every row so none
                // redeliver.
                let group_kind = resolve_subscriber_kind(
                    &db_clone,
                    rows.first()
                        .map(|(r, _)| &r.target_subscriber)
                        .expect("fan-out group is never empty"),
                )
                .await;

                for (row, deadline_expired) in &rows {
                    // Delivery floor (design §2.2 Point B): re-validate the
                    // subscriber's current ACL before delivering any parked bus row.
                    // A revoked ACL (or a denied group) drops the row — mark it
                    // delivered/retired (in the same batch) so it does not
                    // redeliver, and skip dispatch. Ingress rows are never gated
                    // (they carry no channel) and pass regardless of group_kind —
                    // `floor_decision` short-circuits them before the group-kind
                    // check, so a slug-recovery failure on the group cannot deny an
                    // ingress row.
                    if !floor_decision(&messenger_clone, group_kind.as_ref(), row) {
                        // Surface the channel address so an operator can tell
                        // which ACL revocation backed up this subscriber's
                        // deliveries (quality-1). Group-level denies (group_kind
                        // None) are already logged with their distinct reason.
                        let channel = if row.payload.is_bus() {
                            row.payload.unwrap_bus_ref().channel.as_str()
                        } else {
                            "<ingress>"
                        };
                        tracing::warn!(
                            push_id = row.push_id,
                            target_subscriber = row.target_subscriber.as_str(),
                            channel,
                            "subscription delivery denied — ACL not satisfied"
                        );
                        delivered_ids.push(row.push_id);
                        continue;
                    }
                    match dispatch_row(router_clone.as_ref(), row, *deadline_expired, wake_gated)
                        .await
                    {
                        DispatchOutcome::Delivered(id) => {
                            delivered_ids.push(id);
                            delivered_any = true;
                        }
                        // Surface row already marked by the router's claim —
                        // must not be re-marked here (would race a session
                        // unclaim); leave it out of `delivered_ids`.
                        DispatchOutcome::DeliveredNoRemark => {
                            delivered_any = true;
                        }
                        DispatchOutcome::Parked { woke } => {
                            // Row stays parked (sleeping bridge, eager-wake fired if needed).
                            tracing::debug!(
                                push_id = row.push_id,
                                target_subscriber = row.target_subscriber.as_str(),
                                eager_wake = row.eager_wake,
                                deadline_expired,
                                woke,
                                "dispatch_row parked"
                            );
                            if woke {
                                fired_wake = true;
                            }
                        }
                    }
                }

                // Batch mark-delivered under one lock.
                if !delivered_ids.is_empty() {
                    let conn = db_clone.lock().await;
                    db::mark_pending_pushes_delivered(&conn, &delivered_ids);
                }

                (fired_wake, delivered_any)
            });

            // Supervisor task: awaits the fan-out JoinHandle and cleans up regardless
            // of whether the task completed normally or panicked.
            //
            // On panic: remove subscriber from in_flight so the rows are not permanently
            // stuck (errhandling-1 fix). The global panic hook (obs/panic_hook.rs) already
            // fires a Critical alert with location info when the fan-out task panics on its
            // Tokio worker thread; this log adds subscriber-specific context.
            //
            // On normal completion: remove subscriber from in_flight and update the
            // recently_woken cooldown map if the task fired any eager wakes (efficiency-1).
            tokio::spawn(async move {
                match fan_out_handle.await {
                    Ok((fired_wake, delivered_any)) => {
                        // TODO(dispatcher-completion-kick): a scan that skipped
                        // this subscriber because its key was in_flight left
                        // rows behind, and nothing kicks the dispatcher here, so
                        // those rows wait out the full POLL_INTERVAL.
                        // Normal completion: clean up in_flight.
                        in_flight_supervisor
                            .lock()
                            .expect("in_flight poisoned")
                            .remove(&subscriber_key);
                        if fired_wake {
                            // A fresh wake means a CC spawn is in flight; re-arm the
                            // cooldown so further wakes within the window coalesce.
                            // Takes precedence over delivered_any.
                            recently_woken_supervisor
                                .lock()
                                .expect("recently_woken poisoned")
                                .insert(subscriber_key, Instant::now());
                        } else if delivered_any {
                            // Delivery proves the subscriber is live; clear the entry
                            // so future parked rows may re-wake immediately.
                            recently_woken_supervisor
                                .lock()
                                .expect("recently_woken poisoned")
                                .remove(&subscriber_key);
                        }
                        // else: a gated or wake-less pass — leave the map untouched.
                        // Clearing it would halve the cooldown to every-other-pass
                        // under repeated kicks; entries expire via the elapsed() check.
                    }
                    Err(join_err) if join_err.is_panic() => {
                        // Fan-out task panicked. The global panic hook already fired a
                        // Critical alert. Remove the subscriber from in_flight so the
                        // affected rows can re-enter the scan on the next dispatcher pass
                        // rather than being permanently stranded for this process lifetime.
                        tracing::error!(
                            subscriber = %subscriber_key,
                            "dispatcher fan-out task panicked; removing subscriber from \
                             in-flight set so affected rows can be retried"
                        );
                        in_flight_supervisor
                            .lock()
                            .expect("in_flight poisoned")
                            .remove(&subscriber_key);
                        // Also clear any cooldown entry to allow the retry to re-wake.
                        recently_woken_supervisor
                            .lock()
                            .expect("recently_woken poisoned")
                            .remove(&subscriber_key);
                    }
                    Err(_) => {
                        // Task was cancelled (JoinError::is_cancelled). This should not
                        // happen in production (we never cancel fan-out tasks), but if it
                        // does, clean up in_flight the same way.
                        in_flight_supervisor
                            .lock()
                            .expect("in_flight poisoned")
                            .remove(&subscriber_key);
                    }
                }
            });

            if has_deadline_rows {
                // Conservative approximation: debounce if any deadline-expired row
                // was in the batch, even if dispatch_row returned Parked (no actual
                // eager wake). This may cause a spurious PAST_DEADLINE_DEBOUNCE sleep
                // on batches mixing deadline-expired None-wake rows with no real wakes.
                // Acceptable: a 5s extra delay on Immediate delivery in that rare case
                // is preferable to cross-task signaling complexity.
                fired_wakes = true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// register_released_pushes (moved from deliver_after.rs, design §2.7)
// ---------------------------------------------------------------------------

/// Register a set of just-released parked push ids into the push windows of
/// their respective bounded-`push_depth` subscribers.
///
/// Called after `release_due_pushes` clears `release_after` on a batch of
/// rows. Each released row now counts as a live undelivered push for its
/// subscriber; this function ensures the in-memory window is updated so the
/// hot-path bound is enforced correctly (design §2.4 Gap B).
///
/// Uses `record_push_batch_and_check_overflow` (not `record_push_and_check_overflow`)
/// so that multiple released ids for the same `(channel, subscriber)` key are
/// handled atomically: the seed runs once, all new ids are appended, and the
/// deque is trimmed once — preventing the data-loss bug that occurs when
/// per-id calls allow evicted ids to be re-added mid-batch.
///
/// On push-depth overflow the oldest ids are retired from the DB exactly as on
/// the publish hot path. Note: retired ids are removed from `messaging_pending_pushes`
/// before `load_all_dispatchable_pushes` re-queries; rows deleted here are silently
/// dropped from dispatch (correct: overflow losers should not be delivered).
pub(crate) async fn register_released_pushes(
    messenger: &Arc<Messenger>,
    db: &Db,
    released_ids: &[i64],
) {
    // Load the minimal columns needed for window registration.
    let released_rows: Vec<ReleasedPushRow> = {
        let conn = db.lock().await;
        load_released_push_window_rows(&conn, released_ids)
    };

    // Group released rows by (channel_address, target_app_slug, target_subscriber) so
    // multiple same-key rows are processed atomically in one batch call.
    // BTreeMap for deterministic iteration order (not required, but easier to test).
    use std::collections::BTreeMap;
    type GroupKey = (String, String, String); // (channel_addr, app_slug, subscriber_str)
    let mut groups: BTreeMap<GroupKey, Vec<i64>> = BTreeMap::new();
    // Also cache (push_depth, noise, channel_uuid) per group key.
    type GroupMeta = (Depth, super::config::NoiseLevel, uuid::Uuid);
    let mut group_meta: BTreeMap<GroupKey, GroupMeta> = BTreeMap::new();

    for row in &released_rows {
        // Resolve push_depth and noise from directory + app config, mirroring
        // the publish-path resolve_push_targets. Uses O(1) directory.resolve()
        // instead of directory.list()+find to avoid per-row Vec allocation.
        let channel_entry = messenger
            .directory
            .resolve(&row.channel_address)
            .unwrap_or_else(|| {
                // A push row exists for a channel not in the directory. This
                // indicates config drift (channel removed from config without
                // draining push rows) or a structural bug. Log before panicking
                // so the channel name is visible in logs even if the panic is
                // caught by a task monitor.
                tracing::warn!(
                    channel = %row.channel_address,
                    "register_released_pushes: channel not found in directory; \
                     this indicates config drift (channel removed from config) — \
                     panicking to prevent silent data divergence"
                );
                panic!(
                    "register_released_pushes: channel {:?} not found in directory",
                    row.channel_address
                )
            });
        let sub_entry = channel_entry
            .subscribers
            .iter()
            .find(|s| s.kind.slug() == row.target_app_slug)
            .unwrap_or_else(|| {
                panic!(
                    "register_released_pushes: subscriber {:?} not a subscriber of channel {:?}",
                    row.target_app_slug, row.channel_address
                )
            });

        // Bounded(0) subscribers never have push rows — skip.
        if sub_entry.push_depth == Depth::Bounded(0) {
            continue;
        }

        let noise = sub_entry.noise;

        let gkey: GroupKey = (
            row.channel_address.clone(),
            row.target_app_slug.clone(),
            row.target_subscriber.as_str().to_string(),
        );
        groups.entry(gkey.clone()).or_default().push(row.push_id);
        group_meta
            .entry(gkey)
            .or_insert((sub_entry.push_depth, noise, row.channel_uuid));
    }

    // Acquire the DB lock once for the entire registration pass.
    let conn = db.lock().await;
    for (gkey, push_ids) in &groups {
        let (channel_addr, app_slug, subscriber_str) = gkey;
        let &(push_depth, noise, channel_uuid) = group_meta
            .get(gkey)
            .expect("group_meta populated alongside groups");
        let subscriber = super::ParticipantId::from_stored(subscriber_str.clone());
        let retired_ids = messenger.record_push_batch_and_check_overflow(
            &super::PushRegistration {
                channel: channel_addr,
                channel_uuid,
                app_slug,
                subscriber: &subscriber,
                push_depth,
                noise,
            },
            push_ids,
            &conn,
        );
        if !retired_ids.is_empty() {
            tracing::info!(
                retired_count = retired_ids.len(),
                subscriber = subscriber.as_str(),
                "push-depth overflow on deliver-after release: retiring oldest push IDs"
            );
        }
        for retired_id in retired_ids {
            delete_pending_push_by_id(&conn, retired_id);
        }
    }
}

// ---------------------------------------------------------------------------
// Test helpers (cfg-gated)
// ---------------------------------------------------------------------------

/// Test-only one-shot helper: run a single release+dispatch pass synchronously.
/// Routes through the same `dispatch_row` primitive as production so tests
/// exercise the Wasm park gate and the `Immediate`-on-Err eager-wake branch
/// (review F3).
#[cfg(any(test, feature = "testutils"))]
pub async fn run_deliver_after_pass(
    db: &Db,
    router: &Arc<dyn WakeRouter>,
    messenger: &Arc<Messenger>,
) -> usize {
    let now = Utc::now();
    let released_ids = {
        let conn = db.lock().await;
        release_due_pushes(&conn, now)
    };
    if released_ids.is_empty() {
        return 0;
    }

    register_released_pushes(messenger, db, &released_ids).await;

    let pushes = {
        let conn = db.lock().await;
        db::load_pushes_by_ids(&conn, &released_ids)
    };
    let mut delivered = 0usize;
    for row in pushes {
        if let DispatchOutcome::Delivered(_) =
            dispatch_row(router.as_ref(), &row, false, false).await
        {
            // Per-row lock (test helper only; production fan-out batches all
            // delivered ids under one lock after the loop — see dispatcher_loop).
            let conn = db.lock().await;
            db::mark_pending_pushes_delivered(&conn, &[row.push_id]);
            delivered += 1;
        }
    }
    delivered
}

/// Test-only helper: do one pass of the deadline scan synchronously.
///
/// Mirrors the production `dispatcher_loop` body for deadline rows: for each
/// past-deadline row, active targets go through `dispatch_row` (so a successful
/// delivery marks the row delivered); sleeping targets get an eager wake.
#[cfg(any(test, feature = "testutils"))]
pub async fn run_deadline_pass(db: &Db, router: &Arc<dyn WakeRouter>) {
    let now = Utc::now();
    // load_all_dispatchable_pushes returns deadline-expired rows (deadline_expired=true)
    // as well as Immediate rows. Filter to deadline_expired-only for this pass.
    let due: Vec<super::db::PendingPushRow> = {
        let conn = db.lock().await;
        db::load_all_dispatchable_pushes(&conn, now)
            .into_iter()
            .filter_map(|(row, expired)| if expired { Some(row) } else { None })
            .collect()
    };
    if due.is_empty() {
        return;
    }
    let mut delivered_ids: Vec<i64> = Vec::new();
    for row in due {
        if let DispatchOutcome::Delivered(push_id) =
            dispatch_row(router.as_ref(), &row, true, false).await
        {
            delivered_ids.push(push_id);
        }
    }
    if !delivered_ids.is_empty() {
        let conn = db.lock().await;
        db::mark_pending_pushes_delivered(&conn, &delivered_ids);
    }
}

// ---------------------------------------------------------------------------
// Tests (migrated from deliver_after.rs + deadline.rs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db_memory;
    use crate::messaging::canonical_address;
    use crate::messaging::db::{
        PendingPushInsert, insert_message_with_pushes, upsert_channels, utc_to_ns,
    };
    use crate::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, Messenger, Urgency, WakeMin,
    };
    use crate::test_utils::ensure_user_and_conv;
    use std::sync::atomic::{AtomicU64, Ordering};
    use uuid::Uuid;

    // -------------------------------------------------------------------------
    // Shared fake router for deliver-after and deadline tests
    // -------------------------------------------------------------------------

    /// Counting router: `deliver` returns whatever `active` says,
    /// `spawn_eager_wake` increments `eager_wakes`, `deliver_calls` counts
    /// `deliver` invocations.
    #[derive(Default)]
    struct FakeRouter {
        active: AtomicU64,
        eager_wakes: AtomicU64,
        deliver_calls: AtomicU64,
    }

    impl FakeRouter {
        fn set_active(&self, active: bool) {
            self.active
                .store(if active { 1 } else { 0 }, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl super::super::WakeRouter for FakeRouter {
        async fn deliver(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _envelope: &crate::messaging::MessageEnvelope,
            _push_id: i64,
            _seq: i64,
        ) -> Result<bool, String> {
            self.deliver_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.active.load(Ordering::SeqCst) == 1)
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _event: &super::super::ingress::Event,
        ) -> Result<bool, String> {
            Ok(self.active.load(Ordering::SeqCst) == 1)
        }
        fn spawn_eager_wake(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
        ) {
            self.eager_wakes.fetch_add(1, Ordering::SeqCst);
        }
        fn delivery_shape(
            &self,
            key: &crate::messaging::SubscriberEntryKind,
        ) -> crate::messaging::DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &crate::messaging::ParticipantId) {}
    }

    /// Router that counts alarm invocations; used to verify Alarm-noise overflow signals.
    #[derive(Default)]
    struct AlarmCountingRouter {
        alarms: AtomicU64,
    }

    #[async_trait::async_trait]
    impl super::super::WakeRouter for AlarmCountingRouter {
        async fn deliver(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _envelope: &crate::messaging::MessageEnvelope,
            _push_id: i64,
            _seq: i64,
        ) -> Result<bool, String> {
            Ok(false)
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _event: &super::super::ingress::Event,
        ) -> Result<bool, String> {
            Ok(false)
        }
        fn spawn_eager_wake(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
        ) {
        }
        fn delivery_shape(
            &self,
            key: &crate::messaging::SubscriberEntryKind,
        ) -> crate::messaging::DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &crate::messaging::ParticipantId) {
            self.alarms.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn make_directory_and_channel(conn: &rusqlite::Connection) -> (MessagingDirectory, Uuid) {
        let uuid = Uuid::new_v4();
        let entry = ChannelEntry {
            uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: crate::messaging::config::ResolvedChannel {
                push_depth: crate::messaging::config::Depth::Unbounded,
                retain_depth: crate::messaging::config::Depth::Unbounded,
                standing_retain_depth: crate::messaging::config::Depth::Unbounded,
                noise: crate::messaging::config::NoiseLevel::Silent,
                sink: crate::messaging::config::Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![crate::messaging::SubscriberEntry {
                kind: crate::messaging::SubscriberEntryKind::App("target".to_string()),
                push_depth: crate::messaging::config::Depth::Unbounded,
                retain_depth: crate::messaging::config::Depth::Unbounded,
                noise: crate::messaging::config::NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        upsert_channels(conn, std::slice::from_ref(&entry));
        (MessagingDirectory::with_entries(vec![entry]), uuid)
    }

    fn insert_suppressed_immediate(
        conn: &rusqlite::Connection,
        channel: Uuid,
        release_at: chrono::DateTime<chrono::Utc>,
    ) -> i64 {
        let inserted = insert_message_with_pushes(
            conn,
            channel,
            "src",
            "sender",
            "body",
            Urgency::Normal,
            crate::messaging::ChannelScheme::Brenn,
            None,
            None,
            Some(release_at),
            utc_to_ns(Utc::now()),
            &[PendingPushInsert {
                target_subscriber: crate::messaging::ParticipantId::for_conversation(2),
                target_app_slug: "target".to_string(),
                eager_wake: true,
                release_after: Some(release_at),
                delivery_deadline: None,
            }],
        );
        inserted.id
    }

    fn make_messenger(
        db: &Db,
        dir: MessagingDirectory,
        channel_uuid: uuid::Uuid,
    ) -> Arc<Messenger> {
        make_messenger_with_depth(
            db,
            dir,
            channel_uuid,
            crate::messaging::config::Depth::Unbounded,
        )
    }

    fn make_messenger_with_depth(
        db: &Db,
        dir: MessagingDirectory,
        channel_uuid: uuid::Uuid,
        push_depth: crate::messaging::config::Depth,
    ) -> Arc<Messenger> {
        use crate::messaging::MessagingGlobalConfig;
        use crate::messaging::config::{NoiseLevel, ResolvedMessagingConfig, ResolvedSubscription};
        use crate::messaging::test_support::test_app_config;
        use indexmap::IndexMap;

        let app = test_app_config(
            "target",
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid,
                    channel_address: canonical_address("test"),
                    push_depth,
                    retain_depth: crate::messaging::config::Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: WakeMin::Normal,
                }],
            }),
            vec!["u".to_string()],
        );
        let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
        apps.insert("target".to_string(), app);

        let router: Arc<dyn super::super::WakeRouter> = Arc::new(FakeRouter::default());
        Messenger::new(
            db.clone(),
            Arc::new(dir),
            Arc::from("https://test.example"),
            Arc::new(apps),
            router,
            MessagingGlobalConfig::default(),
        )
    }

    // -------------------------------------------------------------------------
    // Deliver-after tests (migrated from deliver_after.rs)
    // -------------------------------------------------------------------------

    /// `run_deliver_after_pass` releases past-due rows and dispatches
    /// them. With an active bridge the row is marked delivered (count
    /// returns 1).
    #[tokio::test]
    async fn run_deliver_after_pass_delivers_to_active_bridge() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (dir, channel_uuid) = make_directory_and_channel(&conn);
        insert_suppressed_immediate(
            &conn,
            channel_uuid,
            Utc::now() - chrono::Duration::seconds(5),
        );
        drop(conn);

        let messenger = make_messenger(&db, dir, channel_uuid);
        let fake = Arc::new(FakeRouter::default());
        fake.set_active(true);
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        let n = run_deliver_after_pass(&db, &router, &messenger).await;
        assert_eq!(n, 1);
        // No eager wake — bridge was active, delivered directly.
        assert_eq!(fake.eager_wakes.load(Ordering::SeqCst), 0);
    }

    /// With a sleeping bridge, the row is parked (count returns 0)
    /// and an eager wake fires.
    #[tokio::test]
    async fn run_deliver_after_pass_eager_wakes_sleeping_immediate() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (dir, channel_uuid) = make_directory_and_channel(&conn);
        insert_suppressed_immediate(
            &conn,
            channel_uuid,
            Utc::now() - chrono::Duration::seconds(5),
        );
        drop(conn);

        let messenger = make_messenger(&db, dir, channel_uuid);
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        let n = run_deliver_after_pass(&db, &router, &messenger).await;
        assert_eq!(n, 0);
        assert_eq!(fake.eager_wakes.load(Ordering::SeqCst), 1);
    }

    /// No due rows → helper returns 0 without contacting the router.
    #[tokio::test]
    async fn run_deliver_after_pass_noop_when_nothing_due() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (dir, channel_uuid) = make_directory_and_channel(&conn);
        // Future release — not due.
        insert_suppressed_immediate(
            &conn,
            channel_uuid,
            Utc::now() + chrono::Duration::seconds(60),
        );
        drop(conn);
        let messenger = make_messenger(&db, dir, channel_uuid);
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        let n = run_deliver_after_pass(&db, &router, &messenger).await;
        assert_eq!(n, 0);
        assert_eq!(fake.eager_wakes.load(Ordering::SeqCst), 0);
    }

    /// Build a directory + channel with `Bounded(push_depth)` subscriber for "target".
    fn make_bounded_directory_and_channel(
        conn: &rusqlite::Connection,
        push_depth: u64,
    ) -> (MessagingDirectory, Uuid) {
        use crate::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
        use crate::messaging::{SubscriberEntry, SubscriberEntryKind};
        let uuid = Uuid::new_v4();
        let entry = ChannelEntry {
            uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Bounded(push_depth),
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::App("target".to_string()),
                push_depth: Depth::Bounded(push_depth),
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        upsert_channels(conn, std::slice::from_ref(&entry));
        (MessagingDirectory::with_entries(vec![entry]), uuid)
    }

    fn make_bounded_messenger(
        db: &Db,
        dir: MessagingDirectory,
        channel_uuid: uuid::Uuid,
        push_depth: u64,
    ) -> Arc<Messenger> {
        make_messenger_with_depth(
            db,
            dir,
            channel_uuid,
            crate::messaging::config::Depth::Bounded(push_depth),
        )
    }

    /// Noise-parameterized variant of `make_bounded_directory_and_channel`.
    /// Both `SubscriberEntry.noise` and `ResolvedChannel.noise` are set to
    /// `noise`; `register_released_pushes` reads `sub_entry.noise` directly.
    fn make_bounded_directory_and_channel_with_noise(
        conn: &rusqlite::Connection,
        push_depth: u64,
        noise: crate::messaging::config::NoiseLevel,
    ) -> (MessagingDirectory, Uuid) {
        use crate::messaging::config::{Depth, ResolvedChannel, Sink};
        use crate::messaging::{SubscriberEntry, SubscriberEntryKind};
        let uuid = Uuid::new_v4();
        let entry = ChannelEntry {
            uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Bounded(push_depth),
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::App("target".to_string()),
                push_depth: Depth::Bounded(push_depth),
                retain_depth: Depth::Unbounded,
                // LOAD-BEARING: must match ResolvedSubscription.noise in
                // make_bounded_messenger_with_noise; register_released_pushes
                // reads sub_entry.noise exclusively after wasm-noise-single-source.
                noise,
                wake_min: Some(WakeMin::Normal),
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        upsert_channels(conn, std::slice::from_ref(&entry));
        (MessagingDirectory::with_entries(vec![entry]), uuid)
    }

    /// Noise-parameterized variant of `make_bounded_messenger`. The supplied
    /// router is used so callers can inspect alarm counts.
    fn make_bounded_messenger_with_noise(
        db: &Db,
        dir: MessagingDirectory,
        channel_uuid: uuid::Uuid,
        push_depth: u64,
        noise: crate::messaging::config::NoiseLevel,
        router: Arc<dyn super::super::WakeRouter>,
    ) -> Arc<Messenger> {
        use crate::messaging::MessagingGlobalConfig;
        use crate::messaging::config::{Depth, ResolvedMessagingConfig, ResolvedSubscription};
        use crate::messaging::test_support::test_app_config;
        use indexmap::IndexMap;

        let app = test_app_config(
            "target",
            Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid,
                    channel_address: canonical_address("test"),
                    push_depth: Depth::Bounded(push_depth),
                    retain_depth: Depth::Unbounded,
                    noise,
                    wake_min: WakeMin::Normal,
                }],
            }),
            vec!["u".to_string()],
        );
        let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
        apps.insert("target".to_string(), app);

        Messenger::new(
            db.clone(),
            Arc::new(dir),
            Arc::from("https://test.example"),
            Arc::new(apps),
            router,
            MessagingGlobalConfig::default(),
        )
    }

    /// Gap B (b): a released parked row must be registered into the push window by
    /// `run_deliver_after_pass`. After release the in-memory window must contain the
    /// released id so subsequent `record_push_and_check_overflow` calls correctly
    /// enforce the bound.
    ///
    /// Verifies directly: after `run_deliver_after_pass` with one parked row,
    /// calling `record_push_and_check_overflow` with a new distinct id must
    /// see the window count as 1 (released row counts), not 0. With depth=1 the
    /// new id must cause an overflow (return Some(retired_id)).
    ///
    /// Note: this test verifies the in-memory window state only (which id is evicted).
    /// The full retire→delete→DB-cleanup path is covered end-to-end by
    /// `release_then_publish_counts_released_row_exactly_once`.
    #[tokio::test]
    async fn released_parked_row_registered_into_push_window() {
        let push_depth = 1u64;
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (dir, channel_uuid) = make_bounded_directory_and_channel(&conn, push_depth);
        let subscriber = crate::messaging::ParticipantId::for_conversation(2);
        let channel_addr = canonical_address("test");

        // Insert one parked row (past deadline → releases immediately).
        let release_at = Utc::now() - chrono::Duration::seconds(1);
        let parked = insert_message_with_pushes(
            &conn,
            channel_uuid,
            "src",
            "sender",
            "parked",
            Urgency::Low,
            crate::messaging::ChannelScheme::Brenn,
            None,
            None,
            Some(release_at),
            utc_to_ns(Utc::now()),
            &[PendingPushInsert {
                target_subscriber: subscriber.clone(),
                target_app_slug: "target".to_string(),
                eager_wake: false,
                release_after: Some(release_at),
                delivery_deadline: None,
            }],
        );
        drop(conn);

        let messenger = make_bounded_messenger(&db, dir, channel_uuid, push_depth);
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();

        // Release + register. After this, the push window should have [parked_push_id].
        run_deliver_after_pass(&db, &router, &messenger).await;

        // Get the actual push_id of the parked row.
        let parked_push_id: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT id FROM messaging_pending_pushes WHERE message_id = ?1",
                rusqlite::params![parked.id],
                |row| row.get(0),
            )
            .unwrap()
        };

        // Now register a new distinct id. With push_depth=1 and window=[parked_push_id],
        // the new id must overflow → return Some(parked_push_id).
        let new_id = parked_push_id + 1_000_000;
        let conn = db.lock().await;
        let retired = messenger.record_push_and_check_overflow(
            &crate::messaging::PushRegistration {
                channel: &channel_addr,
                channel_uuid,
                app_slug: "target",
                subscriber: &subscriber,
                push_depth: crate::messaging::config::Depth::Bounded(push_depth),
                noise: crate::messaging::config::NoiseLevel::Silent,
            },
            new_id,
            &conn,
        );

        // If the released row was NOT registered, the window would be empty → no overflow.
        assert_eq!(
            retired,
            Some(parked_push_id),
            "new push must overflow the window seeded by the released parked row"
        );
    }

    /// Gap B — idempotency (primitive level): the seed-then-contains guard prevents
    /// double-counting when `record_push_and_check_overflow` is called twice with the
    /// same push_id.
    ///
    /// With push_depth=2: after two calls with the same id the deque has [id]
    /// (count=1), not [id, id] (count=2 = overflow on third call).
    /// We verify by calling a third time with a distinct id: no overflow (count
    /// goes from 1→2, which is ≤ depth). If double-counted, the second distinct
    /// id would overflow (deque [id, id, id2] → retire) → retirement.
    #[tokio::test]
    async fn released_row_counted_exactly_once() {
        let push_depth = 2u64;
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (dir, channel_uuid) = make_bounded_directory_and_channel(&conn, push_depth);
        let subscriber = crate::messaging::ParticipantId::for_conversation(2);
        let channel_addr = canonical_address("test");

        // Insert a parked row and a second distinct non-parked row.
        let release_at = Utc::now() - chrono::Duration::seconds(1);
        let parked_id = insert_message_with_pushes(
            &conn,
            channel_uuid,
            "src",
            "sender",
            "parked",
            Urgency::Low,
            crate::messaging::ChannelScheme::Brenn,
            None,
            None,
            Some(release_at),
            utc_to_ns(Utc::now()),
            &[PendingPushInsert {
                target_subscriber: subscriber.clone(),
                target_app_slug: "target".to_string(),
                eager_wake: false,
                release_after: Some(release_at),
                delivery_deadline: None,
            }],
        );
        // A second distinct push row (will serve as push_id=9999 placeholder; use real id).
        let other_id = insert_message_with_pushes(
            &conn,
            channel_uuid,
            "src",
            "sender",
            "other",
            Urgency::Low,
            crate::messaging::ChannelScheme::Brenn,
            None,
            None,
            None,
            utc_to_ns(Utc::now()) + 1,
            &[PendingPushInsert {
                target_subscriber: subscriber.clone(),
                target_app_slug: "target".to_string(),
                eager_wake: false,
                release_after: None,
                delivery_deadline: None,
            }],
        );
        // Manually release the parked row so the seed query will see it.
        conn.execute(
            "UPDATE messaging_pending_pushes SET release_after = NULL WHERE id IN \
             (SELECT id FROM messaging_pending_pushes WHERE message_id = ?1)",
            rusqlite::params![parked_id.id],
        )
        .unwrap();
        drop(conn);

        let messenger = make_bounded_messenger(&db, dir, channel_uuid, push_depth);

        let parked_push_id: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT id FROM messaging_pending_pushes WHERE message_id = ?1",
                rusqlite::params![parked_id.id],
                |row| row.get(0),
            )
            .unwrap()
        };
        let other_push_id: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT id FROM messaging_pending_pushes WHERE message_id = ?1",
                rusqlite::params![other_id.id],
                |row| row.get(0),
            )
            .unwrap()
        };

        // Remove the "other" non-parked push row from the DB before seeding so the
        // seed only sees the parked row (now non-parked). We'll register other_push_id
        // directly as a second call to verify no overflow.
        {
            let conn = db.lock().await;
            conn.execute(
                "DELETE FROM messaging_pending_pushes WHERE id = ?1",
                rusqlite::params![other_push_id],
            )
            .unwrap();

            // First call: seeded=false → seed loads parked_push_id, sets seeded=true.
            // id already present → returns None.
            let reg = crate::messaging::PushRegistration {
                channel: &channel_addr,
                channel_uuid,
                app_slug: "target",
                subscriber: &subscriber,
                push_depth: crate::messaging::config::Depth::Bounded(push_depth),
                noise: crate::messaging::config::NoiseLevel::Silent,
            };
            let r1 = messenger.record_push_and_check_overflow(&reg, parked_push_id, &conn);
            assert!(
                r1.is_none(),
                "first registration of released id should not overflow (already in seeded deque)"
            );

            // Second call with same id: seeded=true, id still present → skip.
            let r2 = messenger.record_push_and_check_overflow(&reg, parked_push_id, &conn);
            assert!(
                r2.is_none(),
                "second registration of same id must be a no-op (no double-count)"
            );

            // Third call with a distinct id: deque should be [parked_push_id] (count=1).
            // Depth=2 → push distinct_id → count=2 ≤ depth → no overflow.
            // If double-counted: deque = [parked, parked] (count=2) → 3rd push → overflow.
            let distinct_id = parked_push_id + 1_000_000; // guaranteed distinct
            let r3 = messenger.record_push_and_check_overflow(&reg, distinct_id, &conn);
            assert!(
                r3.is_none(),
                "third registration with distinct id must not overflow (deque count=2 ≤ depth=2); \
                 overflow means the released id was double-counted"
            );
        }
    }

    /// Gap B — idempotency via integration path: release-pass then publish must count the
    /// released row exactly once.
    #[tokio::test]
    async fn release_then_publish_counts_released_row_exactly_once() {
        let push_depth = 1u64;
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        conn.execute(
            "INSERT OR IGNORE INTO conversations \
             (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (2, 1, 'active', 'target', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
        let (dir, channel_uuid) = make_bounded_directory_and_channel(&conn, push_depth);
        let subscriber = crate::messaging::ParticipantId::for_conversation(2);
        let channel_addr = canonical_address("test");

        let release_at = Utc::now() - chrono::Duration::seconds(1);
        let parked = insert_message_with_pushes(
            &conn,
            channel_uuid,
            "src",
            "sender",
            "parked body",
            Urgency::Low,
            crate::messaging::ChannelScheme::Brenn,
            None,
            None,
            Some(release_at),
            utc_to_ns(Utc::now()),
            &[PendingPushInsert {
                target_subscriber: subscriber.clone(),
                target_app_slug: "target".to_string(),
                eager_wake: false,
                release_after: Some(release_at),
                delivery_deadline: None,
            }],
        );
        drop(conn);

        let messenger = make_bounded_messenger(&db, dir, channel_uuid, push_depth);
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();

        run_deliver_after_pass(&db, &router, &messenger).await;

        let parked_push_id: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT id FROM messaging_pending_pushes WHERE message_id = ?1",
                rusqlite::params![parked.id],
                |row| row.get(0),
            )
            .unwrap()
        };

        let publish_result = messenger
            .publish(
                crate::messaging::PublishOrigin::Conversation { id: 1 },
                "target",
                &channel_addr,
                "new message",
                crate::messaging::Urgency::Low,
                None,
                None,
                None,
            )
            .await;
        assert!(
            matches!(
                publish_result,
                crate::messaging::publish::PublishResult::Ok { .. }
            ),
            "publish must succeed: {publish_result:?}"
        );

        let conn = db.lock().await;
        let parked_still_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_pending_pushes WHERE id = ?1",
                rusqlite::params![parked_push_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            > 0;
        assert!(
            !parked_still_exists,
            "released row must have been retired by the overflow on publish"
        );
    }

    /// Correctness-1 regression: releasing more than push_depth parked rows for the
    /// same (channel, subscriber) key in a single pass must not delete all of them.
    #[tokio::test]
    async fn releasing_more_than_push_depth_rows_retains_push_depth() {
        let push_depth = 2u64;
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (dir, channel_uuid) = make_bounded_directory_and_channel(&conn, push_depth);
        let subscriber = crate::messaging::ParticipantId::for_conversation(2);
        let release_at = Utc::now() - chrono::Duration::seconds(1);

        let n_parked = push_depth + 2;
        for i in 0..n_parked {
            insert_message_with_pushes(
                &conn,
                channel_uuid,
                "src",
                "sender",
                &format!("parked-{i}"),
                crate::messaging::Urgency::Low,
                crate::messaging::ChannelScheme::Brenn,
                None,
                None,
                Some(release_at),
                utc_to_ns(Utc::now()) + i as i64,
                &[PendingPushInsert {
                    target_subscriber: subscriber.clone(),
                    target_app_slug: "target".to_string(),
                    eager_wake: false,
                    release_after: Some(release_at),
                    delivery_deadline: None,
                }],
            );
        }
        drop(conn);

        let messenger = make_bounded_messenger(&db, dir, channel_uuid, push_depth);
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();

        run_deliver_after_pass(&db, &router, &messenger).await;

        let conn = db.lock().await;
        let surviving: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_pending_pushes \
                 WHERE delivered_at IS NULL AND release_after IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            surviving, push_depth as i64,
            "releasing {n_parked} rows for push_depth={push_depth} must retain exactly \
             push_depth rows; got {surviving}"
        );
    }

    /// `register_released_pushes` with Metered noise: releasing more rows than push_depth
    /// increments the drop counter; no alarm fires.
    ///
    /// Exercises the noise path in `register_released_pushes` at Metered level —
    /// the path that was refactored by wasm-noise-single-source to read
    /// `sub_entry.noise` directly. The publish-path overflow tests cover only
    /// `resolve_push_targets`; this test covers the deliver-after release path.
    #[tokio::test]
    async fn register_released_pushes_metered_increments_counter() {
        use crate::messaging::config::NoiseLevel;
        let push_depth = 1u64;
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (dir, channel_uuid) =
            make_bounded_directory_and_channel_with_noise(&conn, push_depth, NoiseLevel::Metered);
        let subscriber = crate::messaging::ParticipantId::for_conversation(2);
        let channel_addr = canonical_address("test");

        // Insert push_depth+1 past-due parked rows → releasing all will cause 1 overflow.
        let release_at = Utc::now() - chrono::Duration::seconds(1);
        for i in 0..(push_depth + 1) {
            insert_message_with_pushes(
                &conn,
                channel_uuid,
                "src",
                "sender",
                &format!("parked-{i}"),
                crate::messaging::Urgency::Low,
                crate::messaging::ChannelScheme::Brenn,
                None,
                None,
                Some(release_at),
                utc_to_ns(Utc::now()) + i as i64,
                &[PendingPushInsert {
                    target_subscriber: subscriber.clone(),
                    target_app_slug: "target".to_string(),
                    eager_wake: false,
                    release_after: Some(release_at),
                    delivery_deadline: None,
                }],
            );
        }
        drop(conn);

        let alarm_router = Arc::new(AlarmCountingRouter::default());
        let messenger = make_bounded_messenger_with_noise(
            &db,
            dir,
            channel_uuid,
            push_depth,
            NoiseLevel::Metered,
            alarm_router.clone() as Arc<dyn super::super::WakeRouter>,
        );

        let delivery_router: Arc<dyn super::super::WakeRouter> = Arc::new(FakeRouter::default());
        run_deliver_after_pass(&db, &delivery_router, &messenger).await;

        assert_eq!(
            messenger.drop_counter(&channel_addr, &subscriber),
            1,
            "metered: releasing push_depth+1 rows must increment drop counter by 1"
        );
        assert_eq!(
            alarm_router.alarms.load(Ordering::SeqCst),
            0,
            "metered: no alarm must fire"
        );
    }

    /// `register_released_pushes` with Alarm noise: releasing more rows than push_depth
    /// increments the drop counter AND fires the alarm.
    ///
    /// Mirrors `register_released_pushes_metered_increments_counter` at Alarm level.
    #[tokio::test]
    async fn register_released_pushes_alarm_fires_alarm() {
        use crate::messaging::config::NoiseLevel;
        let push_depth = 1u64;
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (dir, channel_uuid) =
            make_bounded_directory_and_channel_with_noise(&conn, push_depth, NoiseLevel::Alarm);
        let subscriber = crate::messaging::ParticipantId::for_conversation(2);
        let channel_addr = canonical_address("test");

        let release_at = Utc::now() - chrono::Duration::seconds(1);
        for i in 0..(push_depth + 1) {
            insert_message_with_pushes(
                &conn,
                channel_uuid,
                "src",
                "sender",
                &format!("parked-{i}"),
                crate::messaging::Urgency::Low,
                crate::messaging::ChannelScheme::Brenn,
                None,
                None,
                Some(release_at),
                utc_to_ns(Utc::now()) + i as i64,
                &[PendingPushInsert {
                    target_subscriber: subscriber.clone(),
                    target_app_slug: "target".to_string(),
                    eager_wake: false,
                    release_after: Some(release_at),
                    delivery_deadline: None,
                }],
            );
        }
        drop(conn);

        let alarm_router = Arc::new(AlarmCountingRouter::default());
        let messenger = make_bounded_messenger_with_noise(
            &db,
            dir,
            channel_uuid,
            push_depth,
            NoiseLevel::Alarm,
            alarm_router.clone() as Arc<dyn super::super::WakeRouter>,
        );

        let delivery_router: Arc<dyn super::super::WakeRouter> = Arc::new(FakeRouter::default());
        run_deliver_after_pass(&db, &delivery_router, &messenger).await;

        assert_eq!(
            messenger.drop_counter(&channel_addr, &subscriber),
            1,
            "alarm: releasing push_depth+1 rows must increment drop counter by 1"
        );
        assert_eq!(
            alarm_router.alarms.load(Ordering::SeqCst),
            1,
            "alarm: one alarm must fire"
        );
    }

    /// `run_deliver_after_pass` with a past-due Wasm-subscriber push row: the row stays
    /// pending (returns 0 delivered) and the eager wake fires once (Immediate).
    #[tokio::test]
    async fn run_deliver_after_pass_wasm_subscriber_parks_not_delivered() {
        use crate::messaging::config::{Depth, NoiseLevel};
        use crate::messaging::{ParticipantId, SubscriberEntry, SubscriberEntryKind};

        let wasm_slug = "test-wasm-consumer";
        let db = init_db_memory();
        let conn = db.lock().await;

        let channel_uuid = Uuid::new_v4();
        let wasm_sub = ParticipantId::for_wasm(wasm_slug);
        let entry = ChannelEntry {
            uuid: channel_uuid,
            address: canonical_address("wasm-deliver-after-ch"),
            description: None,
            resolved_channel: crate::messaging::config::ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: crate::messaging::config::Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![SubscriberEntry {
                kind: SubscriberEntryKind::Wasm(wasm_slug.to_string()),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: None,
            }],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        };
        upsert_channels(&conn, std::slice::from_ref(&entry));
        let dir = MessagingDirectory::with_entries(vec![entry]);

        let release_at = Utc::now() - chrono::Duration::seconds(5);
        insert_message_with_pushes(
            &conn,
            channel_uuid,
            "src",
            "sender",
            "body",
            Urgency::Normal,
            ChannelScheme::Brenn,
            None,
            None,
            Some(release_at),
            utc_to_ns(Utc::now()),
            &[PendingPushInsert {
                target_subscriber: wasm_sub.clone(),
                target_app_slug: wasm_slug.to_string(),
                eager_wake: true,
                release_after: Some(release_at),
                delivery_deadline: None,
            }],
        );
        drop(conn);

        let messenger = make_messenger(&db, dir, channel_uuid);
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();

        let n = run_deliver_after_pass(&db, &router, &messenger).await;

        assert_eq!(
            n, 0,
            "Wasm subscriber row must park, not be marked delivered"
        );
        assert_eq!(
            fake.eager_wakes.load(Ordering::SeqCst),
            1,
            "Immediate-wake Wasm row must fire exactly one eager wake on deliver-after release",
        );
    }

    // -------------------------------------------------------------------------
    // Deadline tests (migrated from deadline.rs)
    // -------------------------------------------------------------------------

    fn insert_past_deadline_row(conn: &rusqlite::Connection, channel_uuid: Uuid) -> i64 {
        let past = Utc::now() - chrono::Duration::seconds(10);
        let inserted = insert_message_with_pushes(
            conn,
            channel_uuid,
            "src",
            "sender",
            "body",
            crate::messaging::Urgency::Normal,
            crate::messaging::ChannelScheme::Brenn,
            None,
            Some(past),
            None,
            utc_to_ns(Utc::now()),
            &[PendingPushInsert {
                target_subscriber: crate::messaging::ParticipantId::for_conversation(2),
                target_app_slug: "target".to_string(),
                eager_wake: true,
                release_after: None,
                delivery_deadline: Some(past),
            }],
        );
        inserted.id
    }

    /// `run_deadline_pass` fires `spawn_eager_wake` for past-deadline
    /// rows whose target bridge is sleeping.
    #[tokio::test]
    async fn run_deadline_pass_fires_wake_for_sleeping_target() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (_, channel_uuid) = make_directory_and_channel(&conn);
        insert_past_deadline_row(&conn, channel_uuid);
        drop(conn);

        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        run_deadline_pass(&db, &router).await;
        // Sleeping target → exactly one eager wake.
        assert_eq!(fake.eager_wakes.load(Ordering::SeqCst), 1);
    }

    /// `run_deadline_pass` does NOT fire `spawn_eager_wake` when the
    /// target is already active. `deliver_calls` proves the branch was exercised.
    #[tokio::test]
    async fn run_deadline_pass_no_wake_for_active_target() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (_, channel_uuid) = make_directory_and_channel(&conn);
        insert_past_deadline_row(&conn, channel_uuid);
        drop(conn);

        let fake = Arc::new(FakeRouter::default());
        fake.set_active(true);
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        run_deadline_pass(&db, &router).await;
        assert_eq!(fake.eager_wakes.load(Ordering::SeqCst), 0);
        assert_eq!(
            fake.deliver_calls.load(Ordering::SeqCst),
            1,
            "active-target branch must go through dispatch_row (F4/N14)"
        );
    }

    /// Active target → `dispatch_row` returns `Delivered`, so the row's
    /// `delivered_at` is updated and no eager wake fires.
    #[tokio::test]
    async fn run_deadline_pass_active_target_delivers_and_marks_row() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (_, channel_uuid) = make_directory_and_channel(&conn);
        let inserted_id = insert_past_deadline_row(&conn, channel_uuid);
        // Confirm the row is undelivered before the pass runs.
        let push_id_before: Option<String> = conn
            .query_row(
                "SELECT delivered_at FROM messaging_pending_pushes WHERE message_id = ?1",
                rusqlite::params![inserted_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(push_id_before.is_none());
        drop(conn);

        let fake = Arc::new(FakeRouter::default());
        fake.set_active(true);
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        run_deadline_pass(&db, &router).await;

        assert_eq!(fake.eager_wakes.load(Ordering::SeqCst), 0);
        assert_eq!(fake.deliver_calls.load(Ordering::SeqCst), 1);
        let conn = db.lock().await;
        let delivered_at: Option<String> = conn
            .query_row(
                "SELECT delivered_at FROM messaging_pending_pushes WHERE message_id = ?1",
                rusqlite::params![inserted_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            delivered_at.is_some(),
            "dispatch_row should mark the row delivered for active target"
        );
    }

    /// No past-deadline rows ⇒ helper is a no-op.
    #[tokio::test]
    async fn run_deadline_pass_noop_when_no_due_rows() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        let (_, _) = make_directory_and_channel(&conn);
        drop(conn);
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        run_deadline_pass(&db, &router).await;
        assert_eq!(fake.eager_wakes.load(Ordering::SeqCst), 0);
    }

    // -------------------------------------------------------------------------
    // Acceptance-test additions (design §4, tests 2/3/4)
    // -------------------------------------------------------------------------

    // --- Shared helpers for acceptance tests ---

    /// Insert an Immediate, non-deferred bus push row directly into the DB.
    /// Returns the `push_id` of the inserted pending-push row.
    fn insert_immediate_push(conn: &rusqlite::Connection, channel_uuid: Uuid) -> i64 {
        let inserted = insert_message_with_pushes(
            conn,
            channel_uuid,
            "src",
            "sender",
            "acceptance-test-body",
            Urgency::Normal,
            crate::messaging::ChannelScheme::Brenn,
            None,
            None,
            None,
            utc_to_ns(Utc::now()),
            &[PendingPushInsert {
                target_subscriber: crate::messaging::ParticipantId::for_conversation(2),
                target_app_slug: "target".to_string(),
                eager_wake: true,
                release_after: None,
                delivery_deadline: None,
            }],
        );
        inserted.push_ids[0]
    }

    /// Return the `delivered_at` value for `push_id`, or `None` if NULL.
    fn get_delivered_at(conn: &rusqlite::Connection, push_id: i64) -> Option<String> {
        conn.query_row(
            "SELECT delivered_at FROM messaging_pending_pushes WHERE id = ?1",
            rusqlite::params![push_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    // --- D1 window: delivery error leaves push row undelivered (acceptance 2) ---

    /// A `WakeRouter` whose `deliver` always returns `Err("simulated flush failure")`.
    /// Models the D1 scenario: the message was enqueued to the mpsc (dispatch was
    /// attempted) but the flush to CC's stdin failed (writer errored, ack resolved Err).
    /// The dispatcher's fan-out task must leave the push row `delivered_at IS NULL`.
    #[derive(Default)]
    struct ErrorRouter;

    #[async_trait::async_trait]
    impl super::super::WakeRouter for ErrorRouter {
        async fn deliver(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _envelope: &crate::messaging::MessageEnvelope,
            _push_id: i64,
            _seq: i64,
        ) -> Result<bool, String> {
            Err("simulated flush failure (D1 acceptance test)".to_string())
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _event: &super::super::ingress::Event,
        ) -> Result<bool, String> {
            Err("simulated flush failure (D1 acceptance test)".to_string())
        }
        fn spawn_eager_wake(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
        ) {
        }
        fn delivery_shape(
            &self,
            key: &crate::messaging::SubscriberEntryKind,
        ) -> crate::messaging::DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &crate::messaging::ParticipantId) {}
    }

    /// Acceptance 2 — D1 window (mock-router level).
    ///
    /// When `dispatch_row` is called and `router.deliver()` returns `Err` (simulating
    /// a flush failure in the post-mpsc-enqueue/pre-flush window), the fan-out task
    /// must NOT call `mark_pending_pushes_delivered`, leaving the push row
    /// `delivered_at IS NULL` for redelivery on next drain/restart.
    ///
    /// NOTE: this test exercises the dispatch_row → Err → no-mark path at the
    /// mock-router level. The real D1 window (post-mpsc-enqueue/pre-flush in
    /// spawn_stdin_writer) is covered by
    /// `d1_real_window_broken_pipe_leaves_push_row_undelivered` in
    /// `brenn/src/active_bridge/cc_event_loop.rs`.
    #[tokio::test]
    async fn d1_window_flush_failure_leaves_row_undelivered() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (_, channel_uuid) = make_directory_and_channel(&conn);
        let push_id = insert_immediate_push(&conn, channel_uuid);

        // Precondition: row is undelivered.
        assert!(
            get_delivered_at(&conn, push_id).is_none(),
            "precondition: push row must be undelivered before dispatch"
        );
        drop(conn);

        // Load the row via the same query the dispatcher uses.
        let now = Utc::now();
        let due_rows = {
            let conn = db.lock().await;
            db::load_all_dispatchable_pushes(&conn, now)
        };
        assert_eq!(due_rows.len(), 1, "exactly one due row expected");

        // Simulate dispatcher fan-out: dispatch → only mark Delivered outcomes.
        let router = ErrorRouter;
        let mut delivered_ids: Vec<i64> = Vec::new();
        for (row, deadline_expired) in &due_rows {
            if let DispatchOutcome::Delivered(id) =
                dispatch_row(&router, row, *deadline_expired, false).await
            {
                delivered_ids.push(id);
            }
        }
        // Mark only what was delivered — matches fan-out task behavior.
        if !delivered_ids.is_empty() {
            let conn = db.lock().await;
            db::mark_pending_pushes_delivered(&conn, &delivered_ids);
        }

        // The row must still be undelivered: deliver() returned Err → Parked → no mark.
        let conn = db.lock().await;
        assert!(
            get_delivered_at(&conn, push_id).is_none(),
            "D1 window: flush failure must leave push row delivered_at IS NULL for redelivery"
        );
    }

    // --- Restart redelivery (acceptance 3) ---

    /// Acceptance 3 — restart redelivery.
    ///
    /// A push row left `delivered_at IS NULL` after a simulated flush failure
    /// (the D1 window) is re-dispatched and delivered on the next pass once the
    /// delivery mechanism succeeds. This simulates a Brenn restart: the DB retains
    /// all undelivered rows, and the dispatcher/drain picks them up on the next run.
    ///
    /// The test exercises the dispatcher path (not `drain_pending_events`) so it
    /// stays in `brenn-lib` without a CC session dependency:
    ///   1. Row inserted, dispatch attempted with `ErrorRouter` → row stays parked.
    ///   2. "Restart": fresh `FakeRouter` with `active=true` dispatches the same row.
    ///   3. Row is now `delivered_at IS NOT NULL`.
    ///
    /// NOTE: this does not validate the mpsc-loss/drain-path R4 scenario: a row that
    /// was enqueued into the mpsc buffer but not flushed before process death, then
    /// recovered via `drain_pending_events` on restart. That scenario is covered by
    /// `drain_recovers_push_row_left_undelivered_after_session_death` in
    /// `brenn/src/active_bridge/cc_event_loop.rs`.
    #[tokio::test]
    async fn restart_redelivery_delivers_row_that_was_left_undelivered() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (_, channel_uuid) = make_directory_and_channel(&conn);
        let push_id = insert_immediate_push(&conn, channel_uuid);
        drop(conn);

        // Pass 1: simulate flush failure — row stays undelivered.
        {
            let now = Utc::now();
            let due_rows = {
                let conn = db.lock().await;
                db::load_all_dispatchable_pushes(&conn, now)
            };
            let router = ErrorRouter;
            for (row, deadline_expired) in &due_rows {
                let _ = dispatch_row(&router, row, *deadline_expired, false).await;
            }
        }
        {
            let conn = db.lock().await;
            assert!(
                get_delivered_at(&conn, push_id).is_none(),
                "after flush failure, push row must still be undelivered"
            );
        }

        // Pass 2: simulate restart with working delivery.
        {
            let now = Utc::now();
            let due_rows = {
                let conn = db.lock().await;
                db::load_all_dispatchable_pushes(&conn, now)
            };
            assert_eq!(
                due_rows.len(),
                1,
                "row must still be visible to dispatcher after flush failure (delivered_at IS NULL)"
            );

            let fake = Arc::new(FakeRouter::default());
            fake.set_active(true);
            let router: Arc<dyn super::super::WakeRouter> = fake.clone();
            let mut delivered_ids: Vec<i64> = Vec::new();
            for (row, deadline_expired) in &due_rows {
                if let DispatchOutcome::Delivered(id) =
                    dispatch_row(router.as_ref(), row, *deadline_expired, false).await
                {
                    delivered_ids.push(id);
                }
            }
            assert_eq!(delivered_ids.len(), 1, "second pass must deliver the row");
            {
                let conn = db.lock().await;
                db::mark_pending_pushes_delivered(&conn, &delivered_ids);
            }
        }

        // Row must now be delivered.
        let conn = db.lock().await;
        assert!(
            get_delivered_at(&conn, push_id).is_some(),
            "after restart pass, push row must be delivered_at IS NOT NULL"
        );
        // And a second scan must return empty (delivered rows excluded by predicate).
        let still_due = db::load_all_dispatchable_pushes(&conn, Utc::now());
        assert!(
            still_due.is_empty(),
            "delivered row must not appear in subsequent dispatcher scan"
        );
    }

    // --- In-flight dedup (acceptance 4) ---

    /// A `WakeRouter` that blocks `deliver()` until released via a semaphore.
    /// Used to create genuine loop-vs-fan-out concurrency: one task holds `deliver`
    /// mid-flight while the dispatcher's main loop re-scans the DB.
    struct BlockingRouter {
        /// When acquire() returns, `deliver()` unblocks and returns `Ok(true)`.
        gate: Arc<tokio::sync::Semaphore>,
        deliver_calls: AtomicU64,
    }

    impl BlockingRouter {
        fn new() -> (Arc<Self>, Arc<tokio::sync::Semaphore>) {
            // Zero permits: deliver() blocks until the caller adds a permit.
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let router = Arc::new(Self {
                gate: gate.clone(),
                deliver_calls: AtomicU64::new(0),
            });
            (router, gate)
        }
    }

    #[async_trait::async_trait]
    impl super::super::WakeRouter for BlockingRouter {
        async fn deliver(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _envelope: &crate::messaging::MessageEnvelope,
            _push_id: i64,
            _seq: i64,
        ) -> Result<bool, String> {
            self.deliver_calls.fetch_add(1, Ordering::SeqCst);
            // Block until a permit is available (simulates slow CC flush).
            let _permit = self.gate.acquire().await.expect("semaphore closed");
            Ok(true)
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _event: &super::super::ingress::Event,
        ) -> Result<bool, String> {
            Ok(true)
        }
        fn spawn_eager_wake(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
        ) {
        }
        fn delivery_shape(
            &self,
            key: &crate::messaging::SubscriberEntryKind,
        ) -> crate::messaging::DeliveryShape {
            crate::messaging::default_delivery_shape(key)
        }
        fn alarm(&self, _channel: &str, _subscriber: &crate::messaging::ParticipantId) {}
    }

    /// Acceptance 4 — in-flight dedup: genuine loop-vs-fan-out concurrency.
    ///
    /// The threat model from design §2.5: a fan-out task spawned by loop iteration N
    /// is still in flight (awaiting deliver()) when loop iteration N+1 re-scans the DB
    /// (the row is still `delivered_at IS NULL` from the first task's perspective).
    /// The in-flight `Mutex<HashSet>` must prevent iteration N+1 from spawning a
    /// second fan-out task for the same push_id.
    ///
    /// Mechanism under test: filter-and-insert-before-spawn critical section (design §2.5).
    ///
    /// Test approach: use the in-flight set directly (white-box) — spawn a fan-out
    /// task that holds the blocking router mid-deliver, then simulate a second
    /// dispatcher scan by re-running the filter-insert-spawn logic with the same
    /// in-flight set. The second scan must skip the row.
    #[tokio::test]
    async fn in_flight_dedup_prevents_double_dispatch() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (_, channel_uuid) = make_directory_and_channel(&conn);
        let push_id = insert_immediate_push(&conn, channel_uuid);
        drop(conn);

        let now = Utc::now();
        let due_rows = {
            let conn = db.lock().await;
            db::load_all_dispatchable_pushes(&conn, now)
        };
        assert_eq!(due_rows.len(), 1);
        let (row, deadline_expired) = due_rows.into_iter().next().unwrap();
        assert_eq!(row.push_id, push_id);

        let subscriber_key = row.target_subscriber.as_str().to_string();
        let (blocking_router, gate) = BlockingRouter::new();
        let router: Arc<dyn super::super::WakeRouter> = blocking_router.clone();

        // Shared in-flight set (mirrors dispatcher_loop's `let in_flight = ...`).
        // Keyed by subscriber string (not push_id) — see correctness-1 fix.
        let in_flight: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        // --- Iteration 1: filter + insert + spawn fan-out task (blocking) ---
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            assert!(
                !inflight.contains(&subscriber_key),
                "subscriber must not be in-flight yet"
            );
            inflight.insert(subscriber_key.clone());
        }
        let in_flight_clone = in_flight.clone();
        let router_clone = router.clone();
        let db_clone = db.clone();
        let row_clone = row.clone();
        let sub_key_clone = subscriber_key.clone();
        let fanout_handle = tokio::spawn(async move {
            // This blocks at deliver() until the gate is released.
            if let DispatchOutcome::Delivered(id) =
                dispatch_row(router_clone.as_ref(), &row_clone, deadline_expired, false).await
            {
                let conn = db_clone.lock().await;
                db::mark_pending_pushes_delivered(&conn, &[id]);
            }
            // Remove subscriber from in-flight after dispatch (mirrors production fan-out).
            in_flight_clone
                .lock()
                .expect("in_flight poisoned")
                .remove(&sub_key_clone);
        });

        // Yield to let the fan-out task start and block inside deliver().
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Confirm the fan-out task is mid-flight (deliver_calls == 1 means it
        // entered deliver() and is blocked on the gate).
        assert_eq!(
            blocking_router.deliver_calls.load(Ordering::SeqCst),
            1,
            "fan-out task must have entered deliver() and be blocking on the gate"
        );

        // --- Iteration 2: simulate second scan (row still in DB, delivered_at IS NULL) ---
        // The in-flight set's filter must skip the subscriber because it is still owned by
        // the first fan-out task.
        let second_scan_rows = {
            let conn = db.lock().await;
            // Row is still undelivered (fan-out task hasn't finished yet).
            db::load_all_dispatchable_pushes(&conn, Utc::now())
        };
        assert_eq!(
            second_scan_rows.len(),
            1,
            "row must still be visible in DB while fan-out task is in flight"
        );

        // Apply the critical section: filter out in-flight subscribers.
        let mut second_pass_groups: HashMap<String, Vec<(db::PendingPushRow, bool)>> =
            HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for (r, de) in second_scan_rows {
                let sub = r.target_subscriber.as_str().to_string();
                if inflight.contains(&sub) {
                    // Subscriber already in-flight — skip (this is the dedup guard).
                    continue;
                }
                second_pass_groups.entry(sub).or_default().push((r, de));
            }
            for sub in second_pass_groups.keys() {
                inflight.insert(sub.clone());
            }
        }
        assert!(
            second_pass_groups.is_empty(),
            "in-flight dedup must skip subscriber={subscriber_key} on second scan; \
             second_pass_groups: {second_pass_groups:?}"
        );

        // Release the gate: first fan-out task completes and marks the row delivered.
        gate.add_permits(1);
        fanout_handle.await.expect("fan-out task panicked");

        // Confirm the row is delivered exactly once.
        let conn = db.lock().await;
        assert!(
            get_delivered_at(&conn, push_id).is_some(),
            "push row must be delivered after fan-out task completes"
        );
        // And the in-flight set is empty again.
        let inflight = in_flight.lock().expect("in_flight poisoned");
        assert!(
            inflight.is_empty(),
            "in-flight set must be empty after fan-out task removes subscriber"
        );
        // deliver() must have been called exactly once (no double-dispatch).
        assert_eq!(
            blocking_router.deliver_calls.load(Ordering::SeqCst),
            1,
            "deliver() must be called exactly once; in-flight dedup prevented a second call"
        );
    }

    // -------------------------------------------------------------------------
    // R10 ordering — concurrent same-subscriber fan-out must not reorder delivery
    // (correctness-1 fix: per-subscriber in-flight set, correctness-3 test)
    // -------------------------------------------------------------------------

    /// R10 ordering: the per-subscriber in-flight set prevents a second concurrent
    /// fan-out task for the same subscriber from racing the first to acquire
    /// `bridge.session.lock()` and reordering CC-stdin delivery.
    ///
    /// Test structure:
    ///   1. Insert two Immediate rows `p1`, `p2` for the same subscriber (publish_ts p1 < p2).
    ///   2. First scan: groups [p1, p2], inserts subscriber into in_flight, spawns task A
    ///      blocked on p1's deliver() via `BlockingRouter`.
    ///   3. Insert a third row `p3` for the same subscriber while task A is mid-flight.
    ///   4. Second scan: subscriber is still in in_flight → the entire group is skipped.
    ///      Assert no second fan-out task is spawned.
    ///   5. Release task A. Task A delivers p1, p2; subscriber removed from in_flight.
    ///   6. Third scan: subscriber no longer in in_flight → group [p3] is picked up.
    ///      Assert p3 is delivered exactly once.
    ///   7. Assert deliver_calls == 3 (p1, p2, p3 each once; no reorder or duplicate).
    ///
    /// Without the subscriber-level in-flight set (only push_id keyed), step 4 would
    /// allow a second task for [p3] to spawn concurrently with task A holding p1's
    /// deliver() lock, and the two tasks would race to acquire bridge.session.lock()
    /// to enqueue p3 vs. p2, potentially delivering p3 before p2 to CC (R10 violation).
    #[tokio::test]
    async fn per_subscriber_inflight_prevents_concurrent_fan_out_ordering_violation() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (_dir, channel_uuid) = make_directory_and_channel(&conn);
        // Insert p1 and p2 with distinct publish_ts_ns (p1 < p2).
        let ts_base = utc_to_ns(Utc::now());
        let push_id_p1 = {
            let ins = insert_message_with_pushes(
                &conn,
                channel_uuid,
                "src",
                "sender",
                "body-p1",
                Urgency::Normal,
                crate::messaging::ChannelScheme::Brenn,
                None,
                None,
                None,
                ts_base,
                &[PendingPushInsert {
                    target_subscriber: crate::messaging::ParticipantId::for_conversation(2),
                    target_app_slug: "target".to_string(),
                    eager_wake: true,
                    release_after: None,
                    delivery_deadline: None,
                }],
            );
            ins.id
        };
        let push_id_p2 = {
            let ins = insert_message_with_pushes(
                &conn,
                channel_uuid,
                "src",
                "sender",
                "body-p2",
                Urgency::Normal,
                crate::messaging::ChannelScheme::Brenn,
                None,
                None,
                None,
                ts_base + 1,
                &[PendingPushInsert {
                    target_subscriber: crate::messaging::ParticipantId::for_conversation(2),
                    target_app_slug: "target".to_string(),
                    eager_wake: true,
                    release_after: None,
                    delivery_deadline: None,
                }],
            );
            ins.id
        };
        drop(conn);

        // Shared per-subscriber in-flight set (mirrors production dispatcher_loop).
        let in_flight: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let (blocking_router, gate) = BlockingRouter::new();
        let router: Arc<dyn super::super::WakeRouter> = blocking_router.clone();

        // --- Scan 1: both p1 and p2 visible; subscriber inserted into in_flight ---
        let now = Utc::now();
        let scan1 = {
            let conn = db.lock().await;
            db::load_all_dispatchable_pushes(&conn, now)
        };
        assert_eq!(scan1.len(), 2, "scan 1 must see p1 and p2");

        // Build groups (mirroring dispatcher_loop critical section).
        let mut groups1: HashMap<String, Vec<(db::PendingPushRow, bool)>> = HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for (r, de) in scan1 {
                let sub = r.target_subscriber.as_str().to_string();
                if inflight.contains(&sub) {
                    continue;
                }
                groups1.entry(sub).or_default().push((r, de));
            }
            for sub in groups1.keys() {
                inflight.insert(sub.clone());
            }
        }
        assert_eq!(
            groups1.len(),
            1,
            "scan 1 must produce exactly one subscriber group"
        );
        let (sub_key, rows1) = groups1.into_iter().next().unwrap();
        assert_eq!(rows1.len(), 2, "scan 1 group must have p1 and p2");

        // Spawn task A — blocks inside deliver() on p1.
        let in_flight_clone = in_flight.clone();
        let router_clone = router.clone();
        let db_clone = db.clone();
        let sub_key_clone = sub_key.clone();
        let task_a = tokio::spawn(async move {
            let mut delivered_ids = Vec::new();
            for (row, de) in rows1 {
                if let DispatchOutcome::Delivered(id) =
                    dispatch_row(router_clone.as_ref(), &row, de, false).await
                {
                    delivered_ids.push(id);
                }
            }
            if !delivered_ids.is_empty() {
                let conn = db_clone.lock().await;
                db::mark_pending_pushes_delivered(&conn, &delivered_ids);
            }
            in_flight_clone
                .lock()
                .expect("in_flight poisoned")
                .remove(&sub_key_clone);
        });

        // Let task A enter deliver() and block.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(
            blocking_router.deliver_calls.load(Ordering::SeqCst),
            1,
            "task A must be blocking inside deliver() on p1"
        );

        // Insert p3 while task A is mid-flight.
        let push_id_p3 = {
            let conn = db.lock().await;
            let ins = insert_message_with_pushes(
                &conn,
                channel_uuid,
                "src",
                "sender",
                "body-p3",
                Urgency::Normal,
                crate::messaging::ChannelScheme::Brenn,
                None,
                None,
                None,
                ts_base + 2,
                &[PendingPushInsert {
                    target_subscriber: crate::messaging::ParticipantId::for_conversation(2),
                    target_app_slug: "target".to_string(),
                    eager_wake: true,
                    release_after: None,
                    delivery_deadline: None,
                }],
            );
            ins.id
        };

        // --- Scan 2: p1/p2 still undelivered (task A blocked), p3 newly visible.
        //     Subscriber is still in in_flight → entire group must be skipped. ---
        let scan2 = {
            let conn = db.lock().await;
            db::load_all_dispatchable_pushes(&conn, Utc::now())
        };
        // p1, p2 (still undelivered by task A), p3.
        assert_eq!(scan2.len(), 3, "scan 2 must see p1, p2, p3");

        let mut groups2: HashMap<String, Vec<(db::PendingPushRow, bool)>> = HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for (r, de) in scan2 {
                let sub = r.target_subscriber.as_str().to_string();
                if inflight.contains(&sub) {
                    continue; // subscriber still in-flight
                }
                groups2.entry(sub).or_default().push((r, de));
            }
            for sub in groups2.keys() {
                inflight.insert(sub.clone());
            }
        }
        assert!(
            groups2.is_empty(),
            "scan 2 must yield no groups while task A is in-flight (subscriber-level exclusion)"
        );
        assert_eq!(
            blocking_router.deliver_calls.load(Ordering::SeqCst),
            1,
            "no second deliver() call must happen while task A is blocked"
        );

        // Release task A; it processes p1 then p2 in order.
        gate.add_permits(2); // need 2 permits: one for p1, one for p2.
        task_a.await.expect("task A must not panic");

        // After task A completes, subscriber is removed from in_flight.
        {
            let inflight = in_flight.lock().expect("in_flight poisoned");
            assert!(
                inflight.is_empty(),
                "in_flight must be empty after task A completes"
            );
        }
        assert_eq!(
            blocking_router.deliver_calls.load(Ordering::SeqCst),
            2,
            "task A must have called deliver() for p1 and p2"
        );

        // p1 and p2 must be delivered; p3 still pending.
        {
            let conn = db.lock().await;
            assert!(
                get_delivered_at(&conn, push_id_p1).is_some(),
                "p1 must be delivered"
            );
            assert!(
                get_delivered_at(&conn, push_id_p2).is_some(),
                "p2 must be delivered"
            );
            assert!(
                get_delivered_at(&conn, push_id_p3).is_none(),
                "p3 must still be pending"
            );
        }

        // --- Scan 3: only p3 visible; subscriber no longer in in_flight → picked up. ---
        let scan3 = {
            let conn = db.lock().await;
            db::load_all_dispatchable_pushes(&conn, Utc::now())
        };
        assert_eq!(scan3.len(), 1, "scan 3 must see only p3");

        let mut groups3: HashMap<String, Vec<(db::PendingPushRow, bool)>> = HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for (r, de) in scan3 {
                let sub = r.target_subscriber.as_str().to_string();
                if inflight.contains(&sub) {
                    continue;
                }
                groups3.entry(sub).or_default().push((r, de));
            }
            for sub in groups3.keys() {
                inflight.insert(sub.clone());
            }
        }
        assert_eq!(groups3.len(), 1, "scan 3 must pick up p3");
        let (_, rows3) = groups3.into_iter().next().unwrap();
        assert_eq!(rows3.len(), 1);
        assert_eq!(rows3[0].0.push_id, push_id_p3);

        // Simulate task B for p3 (non-blocking: gate already has 0 permits, use FakeRouter).
        // We've already used all our permits. Use the FakeRouter for p3 delivery.
        let fake_router: Arc<dyn super::super::WakeRouter> = Arc::new(FakeRouter::default());
        {
            let fake = fake_router
                .as_ref()
                // SAFETY: we know this is a FakeRouter; downcast via the concrete type.
                // Actually, dispatch through the Arc directly.
                as &dyn super::super::WakeRouter;
            // Use a fresh FakeRouter (active=false by default) to simulate sleeping bridge.
            let _ = fake; // We'll use a local FakeRouter for simplicity.
        }
        let fake3 = Arc::new(FakeRouter::default());
        fake3.set_active(true);
        let fake3_router: Arc<dyn super::super::WakeRouter> = fake3.clone();
        for (row, de) in &rows3 {
            if let DispatchOutcome::Delivered(id) =
                dispatch_row(fake3_router.as_ref(), row, *de, false).await
            {
                let conn = db.lock().await;
                db::mark_pending_pushes_delivered(&conn, &[id]);
            }
        }

        // p3 must now be delivered.
        let conn = db.lock().await;
        assert!(
            get_delivered_at(&conn, push_id_p3).is_some(),
            "p3 must be delivered after scan 3"
        );
        // Total deliver calls from blocking_router: 2 (p1 + p2 only; p3 used fake3).
        assert_eq!(
            blocking_router.deliver_calls.load(Ordering::SeqCst),
            2,
            "blocking_router must have been called only for p1 and p2"
        );
        assert_eq!(
            fake3.deliver_calls.load(Ordering::SeqCst),
            1,
            "fake3 must have been called exactly once for p3"
        );
    }

    // -------------------------------------------------------------------------
    // Acceptance 6 — Startup sweep (R7)
    // -------------------------------------------------------------------------

    /// Acceptance 6 — startup sweep (design §4 item 6, R7).
    ///
    /// After a simulated Brenn restart with pending `Immediate` push rows in the DB,
    /// a startup dispatch kick causes the dispatcher to eager-wake the affected
    /// conversations without any user interaction.
    ///
    /// Test structure:
    ///   1. Insert a pending `Immediate` row for a sleeping bridge (FakeRouter inactive).
    ///   2. Simulate the startup kick by calling `load_all_dispatchable_pushes` then
    ///      `dispatch_row` for each row (exactly what one dispatcher loop iteration does).
    ///   3. Assert `eager_wakes == 1` — conversation was woken without user input.
    ///   4. Assert the row remains `delivered_at IS NULL` — the bridge is still sleeping;
    ///      the eager wake triggers the CC spawn whose drain will deliver the row later.
    ///
    /// This validates R7's "causes the spawn" requirement, not just "eventually delivers"
    /// (the drain itself is tested by acceptance 3 and the existing drain path).
    #[tokio::test]
    async fn startup_sweep_eager_wakes_pending_immediate_rows() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let (_, channel_uuid) = make_directory_and_channel(&conn);
        let push_id = insert_immediate_push(&conn, channel_uuid);
        drop(conn);

        // Simulate startup: sleeping bridge (FakeRouter active=false).
        let fake = Arc::new(FakeRouter::default());
        // active defaults to 0 (false) — bridge is sleeping, no CC session running.
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();

        // One dispatcher loop iteration: load due rows and dispatch each.
        let now = Utc::now();
        let due_rows = {
            let conn = db.lock().await;
            db::load_all_dispatchable_pushes(&conn, now)
        };
        assert_eq!(
            due_rows.len(),
            1,
            "startup: pending Immediate row must be visible to dispatcher scan"
        );

        let mut delivered_ids: Vec<i64> = Vec::new();
        for (row, deadline_expired) in &due_rows {
            match dispatch_row(router.as_ref(), row, *deadline_expired, false).await {
                DispatchOutcome::Delivered(id) => delivered_ids.push(id),
                DispatchOutcome::DeliveredNoRemark => {}
                DispatchOutcome::Parked { .. } => {}
            }
        }

        // Sleeping bridge: deliver() returns Ok(false) → Parked + eager_wake fired.
        assert_eq!(
            fake.eager_wakes.load(Ordering::SeqCst),
            1,
            "startup sweep must eager-wake the sleeping conversation holding the Immediate row"
        );
        assert_eq!(
            delivered_ids.len(),
            0,
            "row must not be marked delivered — bridge is sleeping; drain will deliver after spawn"
        );

        // Row stays undelivered — drain will mark it after the CC spawn.
        let conn = db.lock().await;
        assert!(
            get_delivered_at(&conn, push_id).is_none(),
            "startup sweep: push row must remain delivered_at IS NULL until CC drain runs"
        );
    }

    // -------------------------------------------------------------------------
    // Delivery floor ACL gate (design §2.2 "Enforcement point B")
    // -------------------------------------------------------------------------

    use crate::messaging::MessageEnvelope;
    use crate::messaging::ParticipantId;
    use crate::messaging::ingress::Event;

    /// Insert conversation 2 with `app_slug='target'` so the floor's
    /// `SELECT app_slug FROM conversations WHERE id = ?` recovers the slug.
    fn insert_conv_with_app_slug(conn: &rusqlite::Connection, conv_id: i64, app_slug: &str) {
        ensure_user_and_conv(conn, conv_id);
        conn.execute(
            "INSERT OR REPLACE INTO conversations \
             (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (?1, 1, 'active', ?2, '2024-01-01', '2024-01-01')",
            rusqlite::params![conv_id, app_slug],
        )
        .unwrap();
    }

    /// Build a `Messenger` whose single app "target" carries `policy`, with a
    /// `brenn:test` channel + conversation:2 subscriber wired in the directory.
    fn make_messenger_with_policy(
        db: &Db,
        dir: MessagingDirectory,
        policy: crate::access::AppPolicy,
    ) -> Arc<Messenger> {
        use crate::messaging::MessagingGlobalConfig;
        use indexmap::IndexMap;
        let mut app =
            crate::messaging::test_support::test_app_config("target", None, vec!["u".to_string()]);
        app.policy = policy;
        let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
        apps.insert("target".to_string(), app);
        let router: Arc<dyn super::super::WakeRouter> = Arc::new(FakeRouter::default());
        Messenger::new(
            db.clone(),
            Arc::new(dir),
            Arc::from("https://test.example"),
            Arc::new(apps),
            router,
            MessagingGlobalConfig::default(),
        )
    }

    /// A policy that covers a `brenn:` channel for delivery (transport grant +
    /// universal matcher), WITHOUT `DynamicSubscribe` — the static-subscriber form.
    /// Delegates to the shared `test_support` constructor (reuse-2).
    fn brenn_delivery_policy() -> crate::access::AppPolicy {
        crate::messaging::test_support::brenn_delivery_policy(
            crate::access::acl::ChannelMatcher::Prefix(String::new()),
        )
    }

    /// Mirror the production delivery floor exactly (dispatcher loop, design §2.2
    /// Point B): resolve the subscriber kind once, then run the per-row decision.
    /// `false` ⇒ the row would be retired (dropped, not delivered).
    async fn floor_allowed(messenger: &Messenger, db: &Db, row: &db::PendingPushRow) -> bool {
        let group_kind = resolve_subscriber_kind(db, &row.target_subscriber).await;
        floor_decision(messenger, group_kind.as_ref(), row)
    }

    fn bus_row(push_id: i64, subscriber: ParticipantId, channel: &str) -> db::PendingPushRow {
        db::PendingPushRow {
            push_id,
            message_id: push_id,
            payload: IngressOrBus::Bus(MessageEnvelope {
                message_id: Uuid::new_v4(),
                source: "src".to_string(),
                channel: channel.to_string(),
                sender: "sender".to_string(),
                publish_ts: Utc::now(),
                body: "body".to_string(),
                reply_to: None,
                delivery_deadline: None,
                deliver_after: None,
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Brenn,
            }),
            target_subscriber: subscriber,
            target_app_slug: "target".to_string(),
            eager_wake: false,
        }
    }

    /// A parked bus row for an App subscriber whose policy still covers the
    /// channel is allowed at the floor; one whose policy lost the matcher is
    /// denied — proving the floor re-validates the ACL at delivery time and the
    /// slug is recovered via `as_conversation_id()` + `conversations.app_slug`.
    #[tokio::test]
    async fn floor_gates_app_subscriber_on_current_acl() {
        let db = init_db_memory();
        let conn = db.lock().await;
        insert_conv_with_app_slug(&conn, 2, "target");
        let (dir_permit, _u1) = make_directory_and_channel(&conn);
        let (dir_deny, _u2) = make_directory_and_channel(&conn);
        drop(conn);
        let subscriber = ParticipantId::for_conversation(2);
        let channel = canonical_address("test");
        let row = bus_row(1, subscriber, &channel);

        // Permitting policy → allowed.
        let permit = make_messenger_with_policy(&db, dir_permit, brenn_delivery_policy());
        assert!(
            floor_allowed(&permit, &db, &row).await,
            "covering ACL must permit delivery at the floor"
        );

        // Revoked policy (no matcher / no grant) → denied.
        let deny = make_messenger_with_policy(&db, dir_deny, crate::access::AppPolicy::default());
        assert!(
            !floor_allowed(&deny, &db, &row).await,
            "removed ACL must deny delivery at the floor"
        );
    }

    /// The floor's fail-closed wiring-bug path (design §3, quality-2, test-4): a
    /// parked App row whose conversation row is gone cannot have its slug
    /// recovered, so `resolve_subscriber_kind` returns `None` and the floor denies
    /// (does not deliver). (The companion "app_slug NULL" case maps to the same
    /// deny branch but is unreachable in practice — `conversations.app_slug` is
    /// NOT NULL in the schema, so the missing-row case is the only reachable
    /// wiring-bug trigger; `query_row(...).get::<_, Option<String>>` would also
    /// fold a NULL to the same `None` deny if the column were ever nullable.)
    #[tokio::test]
    async fn floor_denies_app_row_with_missing_conversation() {
        let db = init_db_memory();
        let conn = db.lock().await;
        let (dir, _u) = make_directory_and_channel(&conn);
        drop(conn);
        let channel = canonical_address("test");
        // A permitting policy is installed, proving the deny is from slug recovery
        // failing — not from the ACL.
        let messenger = make_messenger_with_policy(&db, dir, brenn_delivery_policy());

        // conversation row entirely absent (id 999) → deny.
        let missing = bus_row(1, ParticipantId::for_conversation(999), &channel);
        assert!(
            !floor_allowed(&messenger, &db, &missing).await,
            "missing conversation row must fail closed (deny) at the floor"
        );
    }

    /// The retire contract (test-3): when the floor denies a parked bus row, the
    /// dispatcher loop must mark it delivered/retired (so it does not redeliver)
    /// and must NOT wake the subscriber. This drives the exact per-row floor +
    /// batch-retire sequence the loop runs (`dispatcher.rs` fan-out task) against a
    /// real persisted push row, then asserts `delivered_at` is set and the router
    /// was never woken.
    #[tokio::test]
    async fn floor_denied_row_is_retired_and_not_woken() {
        let db = init_db_memory();
        let conn = db.lock().await;
        insert_conv_with_app_slug(&conn, 2, "target");
        let (dir, channel_uuid) = make_directory_and_channel(&conn);
        // A real, dispatchable, immediate push row for conversation:2 / "target".
        let push_id = insert_immediate_push(&conn, channel_uuid);
        let due_rows = crate::messaging::db::load_all_dispatchable_pushes(&conn, Utc::now());
        drop(conn);
        assert_eq!(due_rows.len(), 1, "exactly one dispatchable row seeded");

        // Deny-everything policy → the floor denies this row.
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        let messenger = {
            use crate::messaging::MessagingGlobalConfig;
            use indexmap::IndexMap;
            let mut app = crate::messaging::test_support::test_app_config(
                "target",
                None,
                vec!["u".to_string()],
            );
            app.policy = crate::access::AppPolicy::default(); // no grant, no matcher → deny
            let mut apps: IndexMap<String, crate::config::AppConfig> = IndexMap::new();
            apps.insert("target".to_string(), app);
            Messenger::new(
                db.clone(),
                Arc::new(dir),
                Arc::from("https://test.example"),
                Arc::new(apps),
                router.clone(),
                MessagingGlobalConfig::default(),
            )
        };

        // Replicate the production fan-out floor + retire sequence exactly.
        let group_kind = resolve_subscriber_kind(&db, &due_rows[0].0.target_subscriber).await;
        let mut delivered_ids: Vec<i64> = Vec::new();
        let mut woke = false;
        for (row, deadline_expired) in &due_rows {
            if !floor_decision(&messenger, group_kind.as_ref(), row) {
                delivered_ids.push(row.push_id);
                continue;
            }
            // (not reached in this test) — would dispatch.
            let _ = dispatch_row(router.as_ref(), row, *deadline_expired, false).await;
            woke = true;
        }
        {
            let conn = db.lock().await;
            crate::messaging::db::mark_pending_pushes_delivered(&conn, &delivered_ids);
        }

        // The denied row is retired (delivered_at set) so it never redelivers, and
        // the subscriber was never woken.
        assert_eq!(delivered_ids, vec![push_id], "denied row must be retired");
        assert!(!woke, "denied row must not wake the subscriber");
        let conn = db.lock().await;
        assert!(
            get_delivered_at(&conn, push_id).is_some(),
            "retired push row must have delivered_at set so it does not redeliver"
        );
        assert_eq!(
            fake.deliver_calls.load(Ordering::SeqCst),
            0,
            "router must not have been asked to deliver a denied row"
        );
    }

    /// A `Wasm(slug)` parked row is gated via `wasm_policies`, with the slug read
    /// directly off the `wasm:` `ParticipantId` (no conversation DB read).
    #[tokio::test]
    async fn floor_gates_wasm_subscriber_via_wasm_policies() {
        let db = init_db_memory();
        let conn = db.lock().await;
        let (dir_bare, _u1) = make_directory_and_channel(&conn);
        let (dir_permit, _u2) = make_directory_and_channel(&conn);
        drop(conn);
        let channel = canonical_address("test");
        let row = bus_row(1, ParticipantId::for_wasm("comp"), &channel);

        // Messenger with no wasm_policies entry for "comp" → fail-closed deny.
        let bare = make_messenger_with_policy(&db, dir_bare, crate::access::AppPolicy::default());
        assert!(
            !floor_allowed(&bare, &db, &row).await,
            "missing wasm policy must fail closed (deny) at the floor"
        );

        // Install a covering wasm policy for "comp" → allowed.
        let mut wasm_policies = HashMap::new();
        wasm_policies.insert("comp".to_string(), brenn_delivery_policy());
        let permit =
            make_messenger_with_policy(&db, dir_permit, crate::access::AppPolicy::default())
                .with_subscriber_registrations(crate::messaging::testutils::wasm_registrations(
                    wasm_policies,
                ));
        assert!(
            floor_allowed(&permit, &db, &row).await,
            "covering wasm policy must permit delivery at the floor"
        );
    }

    /// A `Surface(slug)` parked row is gated via `surface_policies`, with the
    /// slug read directly off the `surface:` `ParticipantId` (no conversation DB
    /// read) — byte-for-byte the App/Wasm floor behavior.
    #[tokio::test]
    async fn floor_gates_surface_subscriber_via_surface_policies() {
        let db = init_db_memory();
        let conn = db.lock().await;
        let (dir_bare, _u1) = make_directory_and_channel(&conn);
        let (dir_permit, _u2) = make_directory_and_channel(&conn);
        drop(conn);
        let channel = canonical_address("test");
        let row = bus_row(1, ParticipantId::for_surface("deskbar"), &channel);

        // Messenger with no surface_policies entry for "deskbar" → fail-closed deny.
        let bare = make_messenger_with_policy(&db, dir_bare, crate::access::AppPolicy::default());
        assert!(
            !floor_allowed(&bare, &db, &row).await,
            "missing surface policy must fail closed (deny) at the floor"
        );

        // Install a covering surface policy for "deskbar" → allowed.
        let mut surface_policies = HashMap::new();
        surface_policies.insert("deskbar".to_string(), brenn_delivery_policy());
        let permit =
            make_messenger_with_policy(&db, dir_permit, crate::access::AppPolicy::default())
                .with_subscriber_registrations(crate::messaging::testutils::surface_registrations(
                    surface_policies,
                ));
        assert!(
            floor_allowed(&permit, &db, &row).await,
            "covering surface policy must permit delivery at the floor"
        );
    }

    /// `resolve_subscriber_kind` maps a `surface:` subscriber directly to a
    /// `SubscriberEntryKind::Surface` with no DB read — the same shape as the
    /// Wasm arm. The empty in-memory DB proves no query is issued.
    #[tokio::test]
    async fn resolve_subscriber_kind_maps_surface() {
        let db = init_db_memory();
        let kind = resolve_subscriber_kind(&db, &ParticipantId::for_surface("deskbar")).await;
        assert_eq!(
            kind,
            Some(crate::messaging::SubscriberEntryKind::Surface {
                slug: "deskbar".to_string(),
                instance: None,
            })
        );
    }

    /// The instance half survives the round trip through the push row's stored
    /// identity: a component instance's parked row must resolve back to *its*
    /// registration key, not its surface's, or the delivery gate reads the wrong
    /// principal's policy and the row lands on the wrong window.
    #[tokio::test]
    async fn resolve_subscriber_kind_maps_surface_component() {
        let db = init_db_memory();
        let kind = resolve_subscriber_kind(
            &db,
            &ParticipantId::for_surface_component("deskbar", "agenda-alice"),
        )
        .await;
        assert_eq!(
            kind,
            Some(crate::messaging::SubscriberEntryKind::Surface {
                slug: "deskbar".to_string(),
                instance: Some("agenda-alice".to_string()),
            })
        );
    }

    /// A `System(component)` parked row is gated via `system_policies`, with the
    /// component read directly off the `system:` `ParticipantId` (no conversation
    /// DB read) — byte-for-byte the App/Wasm/Surface floor behavior.
    #[tokio::test]
    async fn floor_gates_system_subscriber_via_system_policies() {
        let db = init_db_memory();
        let conn = db.lock().await;
        let (dir_bare, _u1) = make_directory_and_channel(&conn);
        let (dir_permit, _u2) = make_directory_and_channel(&conn);
        drop(conn);
        let channel = canonical_address("test");
        let row = bus_row(1, ParticipantId::for_system("tool-executor"), &channel);

        // Messenger with no system_policies entry → fail-closed deny.
        let bare = make_messenger_with_policy(&db, dir_bare, crate::access::AppPolicy::default());
        assert!(
            !floor_allowed(&bare, &db, &row).await,
            "missing system policy must fail closed (deny) at the floor"
        );

        // Install a covering system policy → allowed.
        let mut system_policies = HashMap::new();
        system_policies.insert("tool-executor".to_string(), brenn_delivery_policy());
        let permit =
            make_messenger_with_policy(&db, dir_permit, crate::access::AppPolicy::default())
                .with_subscriber_registrations(crate::messaging::testutils::system_registrations(
                    system_policies,
                ));
        assert!(
            floor_allowed(&permit, &db, &row).await,
            "covering system policy must permit delivery at the floor"
        );
    }

    /// `resolve_subscriber_kind` maps a `system:` subscriber directly to a
    /// `SubscriberEntryKind::System(component)` with no DB read — the same shape
    /// as the Wasm/Surface arms. The empty in-memory DB proves no query issues.
    #[tokio::test]
    async fn resolve_subscriber_kind_maps_system() {
        let db = init_db_memory();
        let kind = resolve_subscriber_kind(&db, &ParticipantId::for_system("tool-executor")).await;
        match kind {
            Some(crate::messaging::SubscriberEntryKind::System(component)) => {
                assert_eq!(component, "tool-executor")
            }
            other => panic!("expected System(tool-executor), got {other:?}"),
        }
    }

    /// An `Ingress` row is never ACL-gated — `delivery_allowed` returns true
    /// without reading the (absent) envelope channel, so the `unwrap_bus_ref`
    /// panic path is unreachable for ingress rows.
    #[tokio::test]
    async fn floor_never_gates_ingress_rows() {
        let db = init_db_memory();
        let conn = db.lock().await;
        let (dir, _uuid) = make_directory_and_channel(&conn);
        drop(conn);
        // Deny-everything messenger; an ingress row must still pass (not gated).
        let messenger = make_messenger_with_policy(&db, dir, crate::access::AppPolicy::default());
        let row = db::PendingPushRow {
            push_id: 1,
            message_id: 1,
            payload: IngressOrBus::Ingress(Event {
                id: 1,
                conversation_id: 2,
                source: "repo_sync:pulled".to_string(),
                summary: "s".to_string(),
                payload: "{}".to_string(),
                created_at: Utc::now(),
            }),
            target_subscriber: ParticipantId::for_conversation(2),
            target_app_slug: "target".to_string(),
            eager_wake: false,
        };
        assert!(
            floor_allowed(&messenger, &db, &row).await,
            "ingress rows are not channel deliveries and must never be ACL-gated"
        );
    }

    // -------------------------------------------------------------------------
    // Wake-cooldown loop semantics (delta-3): the cooldown coalesces Conversation
    // eager wakes but must never suppress a delivery attempt. These are the first
    // tests of the `recently_woken` cooldown at the real `dispatcher_loop` level;
    // they crib the loop-spawning setup from `dispatcher_loop_cross_bridge_isolation`
    // (brenn/src/active_bridge/bridge_io.rs).
    // -------------------------------------------------------------------------

    /// Poll `a` until `pred` holds, or panic after ~5s. Used to observe the
    /// background dispatcher task's atomic counters without a fixed sleep.
    async fn wait_atomic(a: &AtomicU64, pred: impl Fn(u64) -> bool, what: &str) {
        for _ in 0..1000 {
            if pred(a.load(Ordering::SeqCst)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!(
            "timed out waiting for {what}; current value = {}",
            a.load(Ordering::SeqCst)
        );
    }

    /// Poll `messaging_pending_pushes.delivered_at` for `push_id` until set, or
    /// panic after ~5s.
    async fn wait_delivered(db: &Db, push_id: i64, what: &str) {
        for _ in 0..500 {
            {
                let conn = db.lock().await;
                if get_delivered_at(&conn, push_id).is_some() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}: push {push_id} never marked delivered");
    }

    /// Delivery is never gated by the wake cooldown. A sleeping conversation is
    /// eager-woken (arming the cooldown); then, within the window, the bridge is
    /// live and a fresh eager row is published — the group's fan-out still runs
    /// and delivers, milliseconds later, instead of waiting out `POLL_INTERVAL`.
    /// This is the loop-level analogue of the e2e leg-2 live-delivery bug.
    #[tokio::test]
    async fn dispatcher_loop_delivery_never_gated_by_cooldown() {
        let db = init_db_memory();
        let (dir, channel_uuid) = {
            let conn = db.lock().await;
            insert_conv_with_app_slug(&conn, 2, "target");
            make_directory_and_channel(&conn)
        };
        let messenger = make_messenger_with_policy(&db, dir, brenn_delivery_policy());
        // Router the loop delivers through; starts inactive (sleeping bridge).
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        let kick = Arc::new(Notify::new());
        let handle = spawn_dispatcher_task(db.clone(), router, kick.clone(), messenger);

        // Pass 1: eager row, sleeping bridge → parked + exactly one eager wake
        // (cooldown armed for the conversation).
        {
            let conn = db.lock().await;
            insert_immediate_push(&conn, channel_uuid);
        }
        kick.notify_one();
        wait_atomic(
            &fake.eager_wakes,
            |n| n == 1,
            "first eager wake (cooldown armed)",
        )
        .await;

        // Let the supervisor record the cooldown before the next pass.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Pass 2: bridge now live; a new eager row is published within the
        // cooldown window. The cooldown gates only the wake, never delivery, so
        // the row is delivered promptly.
        fake.set_active(true);
        let p2 = {
            let conn = db.lock().await;
            insert_immediate_push(&conn, channel_uuid)
        };
        kick.notify_one();
        wait_delivered(&db, p2, "live delivery within the cooldown window").await;

        handle.abort();
    }

    /// Wake coalescing is preserved: a sleeping conversation is eager-woken once,
    /// and repeated kicks within `POLL_INTERVAL` re-run the fan-out (delivery
    /// attempts happen) but never re-fire the eager wake — exactly one CC spawn
    /// per window, the storm efficiency-1 exists to prevent.
    #[tokio::test]
    async fn dispatcher_loop_wake_coalescing_preserved() {
        let db = init_db_memory();
        let (dir, channel_uuid) = {
            let conn = db.lock().await;
            insert_conv_with_app_slug(&conn, 2, "target");
            make_directory_and_channel(&conn)
        };
        let messenger = make_messenger_with_policy(&db, dir, brenn_delivery_policy());
        // Router stays inactive for the whole test: every deliver returns Ok(false).
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        let kick = Arc::new(Notify::new());
        let handle = spawn_dispatcher_task(db.clone(), router, kick.clone(), messenger);

        // Pass 1: one eager row → exactly one eager wake, cooldown armed.
        {
            let conn = db.lock().await;
            insert_immediate_push(&conn, channel_uuid);
        }
        kick.notify_one();
        wait_atomic(&fake.eager_wakes, |n| n == 1, "first eager wake").await;

        // Let the supervisor arm the cooldown.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Pass 2: a second eager row + kick within the window. The fan-out runs
        // (deliver attempts grow) but the wake is gated — no second wake.
        let deliver_before = fake.deliver_calls.load(Ordering::SeqCst);
        {
            let conn = db.lock().await;
            insert_immediate_push(&conn, channel_uuid);
        }
        kick.notify_one();
        wait_atomic(
            &fake.deliver_calls,
            |n| n > deliver_before,
            "delivery attempt on the gated pass",
        )
        .await;

        // Give any (erroneous) extra wake time to land, then assert it did not:
        // the cooldown coalesced the wake.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            fake.eager_wakes.load(Ordering::SeqCst),
            1,
            "wake must coalesce within the cooldown window — exactly one CC spawn",
        );

        handle.abort();
    }
}

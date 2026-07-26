//! Unified message dispatch: `dispatch_row` primitive and background dispatcher task.
//!
//! `dispatch_row` is the single site that decides how to deliver one
//! direct-to-participant ingress row: inject into a live bridge, or eager-wake a
//! sleeping one.
//!
//! `spawn_dispatcher_task` / `dispatcher_loop` is the single background task
//! that owns all time-sensitive dispatch. It folds:
//!   - Deferred release across every channel's store
//!     (`Messenger::release_due_messages`)
//!   - The wake pass over every lagging cursor position
//!     (`Messenger::wake_owed_subscribers`), which is also where a delivery
//!     deadline is read
//!   - The ingress-row scan (`load_dispatchable_ingress_pushes`)
//!   - Per-bridge fan-out for head-of-line isolation
//!   - In-flight dedup via `Mutex<HashSet<String>>` keyed by subscriber

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Notify;

use super::WakeRouter;
use super::db;
use super::publish::DispatchOutcome;
use super::{DeliveryShape, Messenger, WakeEconomics, registration_key};
use crate::db::Db;

/// Polling fallback interval: maximum delay between automatic wake-ups even
/// with no kick. 60 seconds matches the former deliver-after and deadline loop
/// constants.
pub const POLL_INTERVAL: Duration = Duration::from_secs(60);

/// Debounce after firing eager wakes for past-deadline rows.
/// The wake takes seconds to land (CC spawn + drain); without the debounce the
/// loop re-queries the same past-deadline rows immediately and spins.
const PAST_DEADLINE_DEBOUNCE: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// dispatch_row — the single dispatch primitive
// ---------------------------------------------------------------------------

/// Dispatch an already-durably-stored ingress row to the appropriate delivery
/// mechanism.
///
/// The row is *always already stored* in `messaging_pending_pushes` before this
/// function runs (the DB insert precedes any dispatch call). This function's sole
/// job is choosing the right mechanism:
/// The row's registration key ([`registration_key`]) selects the subscriber's
/// [`DeliveryShape`] from the router's binding:
/// - `Inline` → inject via `router.deliver_ingress()`. `Ok(true)` ⇒ `Delivered`;
///   no bridge / bridge-died-mid-send → eager-wake if the row asks for one, then
///   `Parked`.
/// - `ParkedWake` → the row's own eager wake, and nothing else.
///
/// An ingress row carries no channel, so no ACL gates it: authorization is a
/// channel-subscription question, and these deliveries are addressed to a
/// participant directly.
///
/// `wake_gated`: when `true`, an eager wake is suppressed — the row's
/// subscriber is urgency-gated and this row is not loud enough to buy a
/// subprocess. Delivery is never gated, only the firing of `spawn_eager_wake`:
/// a live bridge is served whatever the urgency, and a sleeping one waits for
/// its next natural spawn, which is the same rule the bus applies to a
/// below-`wake_min` backlog. The caller sets this only for `UrgencyGated`
/// groups, so surface/wasm wakes are never gated even though this function
/// stays kind-agnostic.
///
/// Returns `DispatchOutcome::Delivered(push_id)` only when the underlying
/// `router.deliver_ingress()` returns `Ok(true)`. All other outcomes return
/// `Parked { woke }`, where `woke` reports whether an eager wake actually fired.
pub async fn dispatch_row(
    router: &dyn WakeRouter,
    row: &db::PendingPushRow,
    wake_gated: bool,
) -> DispatchOutcome {
    // Resolve the row's registration key once, then let the subscriber's
    // registered delivery binding — not its identity prefix — shape dispatch.
    let key = registration_key(&row.target_subscriber, &row.target_app_slug);
    let should_wake = row.eager_wake && !wake_gated;

    // ParkedWake: never call the inline delivery path for these targets (it
    // panics by design). The wake is the whole of the delivery trigger.
    if let DeliveryShape::ParkedWake = router.delivery_shape(&key) {
        if should_wake {
            router.spawn_eager_wake(&key, &row.target_subscriber);
        }
        return DispatchOutcome::Parked { woke: should_wake };
    }

    match router
        .deliver_ingress(&key, &row.target_subscriber, &row.event)
        .await
    {
        Ok(true) => DispatchOutcome::Delivered(row.push_id),
        Ok(false) => {
            // No active bridge. Eager-wake if this row demands it and the wake
            // is loud enough to buy one.
            if should_wake {
                router.spawn_eager_wake(&key, &row.target_subscriber);
            }
            DispatchOutcome::Parked { woke: should_wake }
        }
        Err(e) => {
            // Bridge raced with shutdown / send failed. Same outcome as
            // no-bridge eager-wake — fire so the next bridge can drain.
            // Without this, an eager-wake row is silently stored until the
            // next bridge connection happens to drain it.
            tracing::warn!(
                target_subscriber = row.target_subscriber.as_str(),
                error = %e,
                "messaging dispatch failed; leaving pending ingress row undelivered"
            );
            if should_wake {
                router.spawn_eager_wake(&key, &row.target_subscriber);
            }
            DispatchOutcome::Parked { woke: should_wake }
        }
    }
}

// ---------------------------------------------------------------------------
// Background dispatcher task
// ---------------------------------------------------------------------------

/// Spawn the unified background dispatcher task.
///
/// Returns the JoinHandle (process-lifetime task; caller typically drops it).
/// One task owns every time-sensitive dispatch decision — deferred release, the
/// wake pass over lagging positions, and the ingress scan — so nothing on a
/// publish path waits for delivery and there is one place a timer can live.
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
    // In-flight dedup set: subscriber keys currently being processed by a spawned
    // per-bridge fan-out task. Keyed by subscriber string (not push_id) because
    // ordering within a conversation demands subscriber-level exclusion: if any
    // fan-out task for subscriber S is still running, a later scan must not spawn a
    // second concurrent task for S — that would let two tasks race acquiring
    // bridge.session.lock() and reorder CC-stdin delivery.
    //
    // A single subscriber-key in the set blocks all rows for that subscriber until the
    // running task finishes, at which point the next scan picks up any remaining rows
    // (including rows published while the task was mid-flight) in publish_ts_ns order.
    let in_flight: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    // The urgency gate NEVER suppresses a delivery attempt: every due group
    // still gets its fan-out and per-row dispatch_row, so a live bridge is
    // served whatever the row's urgency. Only `UrgencyGated` groups are ever
    // gated — a surface wake is a free notify_one, and a parked subscriber's
    // channel-backed wake does not come from a fan-out task at all.

    let mut fired_wakes = false;
    // Earliest deferred release across every channel's store. Seeded by one
    // walk here and thereafter reported by each release sweep, so a pass asks
    // each store when its next release is due exactly once. A park that lands
    // after the sweep kicks the loop, so a stale value never delays a release.
    let mut next_release = messenger.next_deferred_release().await;
    // Earliest delivery deadline no position has passed, as of the last wake
    // pass, and the only deadline source there is: the message row has always
    // carried the deadline, and the wake pass is what reads it against each
    // lagging position. `None` until the first pass runs, which is one
    // POLL_INTERVAL at worst and only on a boot with a deadline already pending.
    let mut next_cursor_deadline: Option<chrono::DateTime<Utc>> = None;
    loop {
        let next_due = [next_cursor_deadline, next_release]
            .into_iter()
            .flatten()
            .min();
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
            // loop would immediately re-query past-deadline rows and spin.
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

        // Release every message whose deliver_after has passed, on every channel,
        // through the channel's own store.
        let now = Utc::now();
        let sweep = messenger.release_due_messages(now).await;
        next_release = sweep.next_release;
        if sweep.released > 0 {
            tracing::debug!(
                released = sweep.released,
                "dispatcher released parked messages"
            );
        }
        // Before the scan's early-continue, so a publish on any class (which
        // kicks this loop) reaches its consumer at once.
        let wake = messenger.wake_owed_subscribers(now).await;
        next_cursor_deadline = wake.next_deadline;
        if wake.fired_deadline_wake {
            fired_wakes = true;
        }

        // The channel-less ingress rows: undelivered and asking for a wake.
        let due_rows = {
            let conn = db.lock().await;
            db::load_dispatchable_ingress_pushes(&conn)
        };

        if due_rows.is_empty() {
            continue;
        }

        // Group due rows by target_subscriber so each bridge's rows are
        // processed in-order by a single fan-out task: publish_ts_ns order is
        // preserved within a bridge group because the global query is
        // ORDER BY publish_ts_ns ASC.
        //
        // Subscriber-level dedup: if a subscriber already has a live fan-out task
        // (its key is in `in_flight`), skip the entire group. This prevents a second
        // concurrent task from racing the first to acquire bridge.session.lock() and
        // reordering CC-stdin delivery for that conversation.
        // The skipped rows remain undelivered in the DB and will be picked up on the
        // next scan after the running task completes and removes the subscriber from
        // in_flight.
        let mut groups: HashMap<String, Vec<db::PendingPushRow>> = HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for row in due_rows {
                let sub_key = row.target_subscriber.as_str().to_string();
                if inflight.contains(&sub_key) {
                    // Subscriber already has a running fan-out task — skip all its rows.
                    // They will be re-scanned after the current task completes.
                    continue;
                }
                groups.entry(sub_key).or_default().push(row);
            }
            // Insert all new subscriber keys at once, after grouping, before spawning.
            // This is the critical section: all insertions happen before any spawn so
            // the loop cannot re-enter a group between grouping and spawning.
            for sub_key in groups.keys() {
                inflight.insert(sub_key.clone());
            }
        }

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
        // A supervisor task awaits each fan-out JoinHandle to clean up in_flight,
        // and to log+recover from any fan-out task panic (errhandling-1 fix: a
        // panic must not leave the subscriber permanently stuck in in_flight).
        for (subscriber_key, rows) in groups {
            // The wake gate is the subscriber's own urgency economics and
            // nothing else: an `UrgencyGated` subscriber's spawn is priced, and
            // an ingress row is not urgency-ranked, so it never buys one. It
            // gates only the wake (a CC spawn) — the group is never skipped, so
            // every row's dispatch_row delivery attempt still runs and a live
            // bridge is served regardless. An `Eager` wake is a free notify_one
            // and is never gated. The economics are read per participant from
            // the registry, not from the identity prefix.
            let first_row = rows.first().expect("fan-out group is never empty");
            let group_key =
                registration_key(&first_row.target_subscriber, &first_row.target_app_slug);
            let wake_gated = messenger.subscriber_wake_economics(&group_key)
                == Some(WakeEconomics::UrgencyGated);

            let router_clone = router.clone();
            let db_clone = db.clone();
            // Clone Arcs for the supervisor task (fan-out task gets its own clones below).
            let in_flight_supervisor = in_flight.clone();

            let fan_out_handle = tokio::spawn(async move {
                let mut delivered_ids: Vec<i64> = Vec::new();

                for row in &rows {
                    match dispatch_row(router_clone.as_ref(), row, wake_gated).await {
                        DispatchOutcome::Delivered(id) => {
                            delivered_ids.push(id);
                        }
                        DispatchOutcome::Parked { woke } => {
                            // Row stays stored (sleeping bridge, eager-wake fired if needed).
                            tracing::debug!(
                                push_id = row.push_id,
                                target_subscriber = row.target_subscriber.as_str(),
                                eager_wake = row.eager_wake,
                                woke,
                                "dispatch_row parked"
                            );
                        }
                    }
                }

                // Batch mark-delivered under one lock.
                if !delivered_ids.is_empty() {
                    let conn = db_clone.lock().await;
                    db::mark_pending_pushes_delivered(&conn, &delivered_ids);
                }
            });

            // Supervisor task: awaits the fan-out JoinHandle and cleans up regardless
            // of whether the task completed normally or panicked.
            //
            // On panic: remove subscriber from in_flight so the rows are not permanently
            // stuck (errhandling-1 fix). The global panic hook (obs/panic_hook.rs) already
            // fires a Critical alert with location info when the fan-out task panics on its
            // Tokio worker thread; this log adds subscriber-specific context.
            //
            // On normal completion: remove subscriber from in_flight. That is the
            // whole of it — nothing here paces a wake, so a completed fan-out
            // leaves no bookkeeping behind.
            tokio::spawn(async move {
                match fan_out_handle.await {
                    Ok(()) => {
                        // TODO(dispatcher-completion-kick): a scan that skipped
                        // this subscriber because its key was in_flight left
                        // rows behind, and nothing kicks the dispatcher here, so
                        // those rows wait out the full POLL_INTERVAL.
                        // Normal completion: clean up in_flight.
                        in_flight_supervisor
                            .lock()
                            .expect("in_flight poisoned")
                            .remove(&subscriber_key);
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_db_memory;
    use crate::messaging::canonical_address;
    use crate::messaging::db::{upsert_channels, utc_to_ns};
    use crate::messaging::{
        ChannelEntry, ChannelScheme, MessagingDirectory, Messenger, ParticipantId, Urgency, WakeMin,
    };
    use crate::test_utils::ensure_user_and_conv;
    use std::sync::atomic::{AtomicU64, Ordering};
    use uuid::Uuid;

    // -------------------------------------------------------------------------
    // Shared fake router for deliver-after and deadline tests
    // -------------------------------------------------------------------------

    /// Counting router: `deliver_ingress` returns whatever `active` says,
    /// `spawn_eager_wake` increments `eager_wakes`, `deliver_calls` counts
    /// `deliver_ingress` invocations.
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
            _envelope: &Arc<crate::messaging::MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            unreachable!("the ingress dispatch path never calls deliver")
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _event: &super::super::ingress::Event,
        ) -> Result<bool, String> {
            self.deliver_calls.fetch_add(1, Ordering::SeqCst);
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
        fn alarm(
            &self,
            _channel: &str,
            _subscriber: &crate::messaging::ParticipantId,
            _count: u64,
        ) {
        }
    }

    fn make_directory_and_channel(conn: &rusqlite::Connection) -> (MessagingDirectory, Uuid) {
        let uuid = Uuid::new_v4();
        let entry = ChannelEntry {
            uuid,
            address: canonical_address("test"),
            description: None,
            resolved_channel: crate::messaging::config::ResolvedChannel {
                send_rate: Default::default(),
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

    // --- Shared helpers for the dispatch-path tests ---

    /// Insert one eager, channel-less ingress row for conversation 1 at
    /// `publish_ts_ns`. Returns its `push_id`.
    ///
    /// These are the only rows the dispatcher still dispatches: what a subscriber
    /// is owed on a channel is its cursor position, walked by the wake pass.
    fn insert_ingress_row(conn: &rusqlite::Connection, publish_ts_ns: i64) -> i64 {
        let (_, push_id) = crate::messaging::db::insert_ingress_message(
            conn,
            &ParticipantId::for_conversation(1),
            "target",
            "mqtt:acceptance",
            "acceptance-test-summary",
            "acceptance-test-body",
            Urgency::Normal,
            publish_ts_ns,
        );
        push_id
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

    // --- Delivery error leaves the push row undelivered ---

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
            _envelope: &Arc<crate::messaging::MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            unreachable!("the ingress dispatch path never calls deliver")
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _event: &super::super::ingress::Event,
        ) -> Result<bool, String> {
            Err("simulated flush failure".to_string())
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
        fn alarm(
            &self,
            _channel: &str,
            _subscriber: &crate::messaging::ParticipantId,
            _count: u64,
        ) {
        }
    }

    /// A delivery error leaves the row undelivered (mock-router level).
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
        let push_id = insert_ingress_row(&conn, utc_to_ns(Utc::now()));

        // Precondition: row is undelivered.
        assert!(
            get_delivered_at(&conn, push_id).is_none(),
            "precondition: push row must be undelivered before dispatch"
        );
        drop(conn);

        // Load the row via the same query the dispatcher uses.
        let due_rows = {
            let conn = db.lock().await;
            db::load_dispatchable_ingress_pushes(&conn)
        };
        assert_eq!(due_rows.len(), 1, "exactly one due row expected");

        // Simulate dispatcher fan-out: dispatch → only mark Delivered outcomes.
        let router = ErrorRouter;
        let mut delivered_ids: Vec<i64> = Vec::new();
        for row in &due_rows {
            if let DispatchOutcome::Delivered(id) = dispatch_row(&router, row, false).await {
                delivered_ids.push(id);
            }
        }
        // Mark only what was delivered — matches fan-out task behavior.
        if !delivered_ids.is_empty() {
            let conn = db.lock().await;
            db::mark_pending_pushes_delivered(&conn, &delivered_ids);
        }

        // The row must still be undelivered: the send returned Err → Parked → no mark.
        let conn = db.lock().await;
        assert!(
            get_delivered_at(&conn, push_id).is_none(),
            "D1 window: flush failure must leave push row delivered_at IS NULL for redelivery"
        );
    }

    // --- Restart redelivery ---

    /// Restart redelivery.
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
    /// NOTE: this does not validate the mpsc-loss/drain-path scenario: a row that
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
        let push_id = insert_ingress_row(&conn, utc_to_ns(Utc::now()));
        drop(conn);

        // Pass 1: simulate flush failure — row stays undelivered.
        {
            let due_rows = {
                let conn = db.lock().await;
                db::load_dispatchable_ingress_pushes(&conn)
            };
            let router = ErrorRouter;
            for row in &due_rows {
                let _ = dispatch_row(&router, row, false).await;
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
            let due_rows = {
                let conn = db.lock().await;
                db::load_dispatchable_ingress_pushes(&conn)
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
            for row in &due_rows {
                if let DispatchOutcome::Delivered(id) =
                    dispatch_row(router.as_ref(), row, false).await
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
        let still_due = db::load_dispatchable_ingress_pushes(&conn);
        assert!(
            still_due.is_empty(),
            "delivered row must not appear in subsequent dispatcher scan"
        );
    }

    // --- In-flight dedup ---

    /// A `WakeRouter` that blocks `deliver_ingress()` until released via a semaphore.
    /// Used to create genuine loop-vs-fan-out concurrency: one task holds `deliver`
    /// mid-flight while the dispatcher's main loop re-scans the DB.
    struct BlockingRouter {
        /// When acquire() returns, `deliver_ingress()` unblocks and returns `Ok(true)`.
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
            _envelope: &Arc<crate::messaging::MessageEnvelope>,
            _retained_seq: i64,
        ) -> Result<bool, String> {
            unreachable!("the ingress dispatch path never calls deliver")
        }
        async fn deliver_ingress(
            &self,
            _key: &crate::messaging::SubscriberEntryKind,
            _subscriber: &crate::messaging::ParticipantId,
            _event: &super::super::ingress::Event,
        ) -> Result<bool, String> {
            self.deliver_calls.fetch_add(1, Ordering::SeqCst);
            // Block until a permit is available (simulates slow CC flush).
            let _permit = self.gate.acquire().await.expect("semaphore closed");
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
        fn alarm(
            &self,
            _channel: &str,
            _subscriber: &crate::messaging::ParticipantId,
            _count: u64,
        ) {
        }
    }

    /// In-flight dedup: genuine loop-vs-fan-out concurrency.
    ///
    /// The threat model: a fan-out task spawned by loop iteration N
    /// is still in flight (awaiting deliver()) when loop iteration N+1 re-scans the DB
    /// (the row is still `delivered_at IS NULL` from the first task's perspective).
    /// The in-flight `Mutex<HashSet>` must prevent iteration N+1 from spawning a
    /// second fan-out task for the same push_id.
    ///
    /// Mechanism under test: the filter-and-insert-before-spawn critical section.
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
        let push_id = insert_ingress_row(&conn, utc_to_ns(Utc::now()));
        drop(conn);

        let due_rows = {
            let conn = db.lock().await;
            db::load_dispatchable_ingress_pushes(&conn)
        };
        assert_eq!(due_rows.len(), 1);
        let row = due_rows.into_iter().next().unwrap();
        assert_eq!(row.push_id, push_id);

        let subscriber_key = row.target_subscriber.as_str().to_string();
        let (blocking_router, gate) = BlockingRouter::new();
        let router: Arc<dyn super::super::WakeRouter> = blocking_router.clone();

        // Shared in-flight set (mirrors dispatcher_loop's `let in_flight = ...`).
        // Keyed by subscriber string (not push_id), as ordering within a
        // conversation demands.
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
                dispatch_row(router_clone.as_ref(), &row_clone, false).await
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
            "fan-out task must have entered deliver_ingress() and be blocking on the gate"
        );

        // --- Iteration 2: simulate second scan (row still in DB, delivered_at IS NULL) ---
        // The in-flight set's filter must skip the subscriber because it is still owned by
        // the first fan-out task.
        let second_scan_rows = {
            let conn = db.lock().await;
            // Row is still undelivered (fan-out task hasn't finished yet).
            db::load_dispatchable_ingress_pushes(&conn)
        };
        assert_eq!(
            second_scan_rows.len(),
            1,
            "row must still be visible in DB while fan-out task is in flight"
        );

        // Apply the critical section: filter out in-flight subscribers.
        let mut second_pass_groups: HashMap<String, Vec<db::PendingPushRow>> = HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for r in second_scan_rows {
                let sub = r.target_subscriber.as_str().to_string();
                if inflight.contains(&sub) {
                    // Subscriber already in-flight — skip (this is the dedup guard).
                    continue;
                }
                second_pass_groups.entry(sub).or_default().push(r);
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
        // deliver_ingress() must have been called exactly once (no double-dispatch).
        assert_eq!(
            blocking_router.deliver_calls.load(Ordering::SeqCst),
            1,
            "deliver_ingress must be called exactly once; in-flight dedup prevented a second call"
        );
    }

    // -------------------------------------------------------------------------
    // Ordering — concurrent same-subscriber fan-out must not reorder delivery
    // (the per-subscriber in-flight set is what prevents it)
    // -------------------------------------------------------------------------

    /// Ordering: the per-subscriber in-flight set prevents a second concurrent
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
    /// to enqueue p3 vs. p2, potentially delivering p3 before p2 to CC — an ordering
    /// violation within one conversation.
    #[tokio::test]
    async fn per_subscriber_inflight_prevents_concurrent_fan_out_ordering_violation() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        // Insert p1 and p2 with distinct publish_ts_ns (p1 < p2).
        let ts_base = utc_to_ns(Utc::now());
        let push_id_p1 = insert_ingress_row(&conn, ts_base);
        let push_id_p2 = insert_ingress_row(&conn, ts_base + 1);
        drop(conn);

        // Shared per-subscriber in-flight set (mirrors production dispatcher_loop).
        let in_flight: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let (blocking_router, gate) = BlockingRouter::new();
        let router: Arc<dyn super::super::WakeRouter> = blocking_router.clone();

        // --- Scan 1: both p1 and p2 visible; subscriber inserted into in_flight ---
        let scan1 = {
            let conn = db.lock().await;
            db::load_dispatchable_ingress_pushes(&conn)
        };
        assert_eq!(scan1.len(), 2, "scan 1 must see p1 and p2");

        // Build groups (mirroring dispatcher_loop critical section).
        let mut groups1: HashMap<String, Vec<db::PendingPushRow>> = HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for r in scan1 {
                let sub = r.target_subscriber.as_str().to_string();
                if inflight.contains(&sub) {
                    continue;
                }
                groups1.entry(sub).or_default().push(r);
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
            for row in rows1 {
                if let DispatchOutcome::Delivered(id) =
                    dispatch_row(router_clone.as_ref(), &row, false).await
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
            "task A must be blocking inside deliver_ingress() on p1"
        );

        // Insert p3 while task A is mid-flight.
        let push_id_p3 = {
            let conn = db.lock().await;
            insert_ingress_row(&conn, ts_base + 2)
        };

        // --- Scan 2: p1/p2 still undelivered (task A blocked), p3 newly visible.
        //     Subscriber is still in in_flight → entire group must be skipped. ---
        let scan2 = {
            let conn = db.lock().await;
            db::load_dispatchable_ingress_pushes(&conn)
        };
        // p1, p2 (still undelivered by task A), p3.
        assert_eq!(scan2.len(), 3, "scan 2 must see p1, p2, p3");

        let mut groups2: HashMap<String, Vec<db::PendingPushRow>> = HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for r in scan2 {
                let sub = r.target_subscriber.as_str().to_string();
                if inflight.contains(&sub) {
                    continue; // subscriber still in-flight
                }
                groups2.entry(sub).or_default().push(r);
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
            "no second deliver_ingress call must happen while task A is blocked"
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
            "task A must have called deliver_ingress for p1 and p2"
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
            db::load_dispatchable_ingress_pushes(&conn)
        };
        assert_eq!(scan3.len(), 1, "scan 3 must see only p3");

        let mut groups3: HashMap<String, Vec<db::PendingPushRow>> = HashMap::new();
        {
            let mut inflight = in_flight.lock().expect("in_flight poisoned");
            for r in scan3 {
                let sub = r.target_subscriber.as_str().to_string();
                if inflight.contains(&sub) {
                    continue;
                }
                groups3.entry(sub).or_default().push(r);
            }
            for sub in groups3.keys() {
                inflight.insert(sub.clone());
            }
        }
        assert_eq!(groups3.len(), 1, "scan 3 must pick up p3");
        let (_, rows3) = groups3.into_iter().next().unwrap();
        assert_eq!(rows3.len(), 1);
        assert_eq!(rows3[0].push_id, push_id_p3);

        // Task B for p3: the blocking router's permits are spent, so a live
        // FakeRouter stands in for the bridge that serves it.
        let fake3 = Arc::new(FakeRouter::default());
        fake3.set_active(true);
        let fake3_router: Arc<dyn super::super::WakeRouter> = fake3.clone();
        for row in &rows3 {
            if let DispatchOutcome::Delivered(id) =
                dispatch_row(fake3_router.as_ref(), row, false).await
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
    // Startup sweep
    // -------------------------------------------------------------------------

    /// Startup sweep.
    ///
    /// After a simulated Brenn restart with pending `Immediate` push rows in the DB,
    /// a startup dispatch kick causes the dispatcher to eager-wake the affected
    /// conversations without any user interaction.
    ///
    /// Test structure:
    ///   1. Insert a pending `Immediate` row for a sleeping bridge (FakeRouter inactive).
    ///   2. Simulate the startup kick by calling `load_dispatchable_ingress_pushes` then
    ///      `dispatch_row` for each row (exactly what one dispatcher loop iteration does).
    ///   3. Assert `eager_wakes == 1` — conversation was woken without user input.
    ///   4. Assert the row remains `delivered_at IS NULL` — the bridge is still sleeping;
    ///      the eager wake triggers the CC spawn whose drain will deliver the row later.
    ///
    /// The point is that the sweep *causes the spawn*, not merely that the row is
    /// eventually delivered; the drain itself has its own tests.
    #[tokio::test]
    async fn startup_sweep_eager_wakes_pending_immediate_rows() {
        let db = init_db_memory();
        let conn = db.lock().await;
        ensure_user_and_conv(&conn, 1);
        ensure_user_and_conv(&conn, 2);
        let push_id = insert_ingress_row(&conn, utc_to_ns(Utc::now()));
        drop(conn);

        // Simulate startup: sleeping bridge (FakeRouter active=false).
        let fake = Arc::new(FakeRouter::default());
        // active defaults to 0 (false) — bridge is sleeping, no CC session running.
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();

        // One dispatcher loop iteration: load due rows and dispatch each.
        let due_rows = {
            let conn = db.lock().await;
            db::load_dispatchable_ingress_pushes(&conn)
        };
        assert_eq!(
            due_rows.len(),
            1,
            "startup: pending Immediate row must be visible to dispatcher scan"
        );

        let mut delivered_ids: Vec<i64> = Vec::new();
        for row in &due_rows {
            match dispatch_row(router.as_ref(), row, false).await {
                DispatchOutcome::Delivered(id) => delivered_ids.push(id),
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

    /// Build a `Messenger` whose single app "target" carries a delivery policy for
    /// `brenn:` channels, with the directory the loop tests dispatch against.
    fn make_messenger_with_policy(db: &Db, dir: MessagingDirectory) -> Arc<Messenger> {
        use crate::messaging::MessagingGlobalConfig;
        use indexmap::IndexMap;
        let mut app =
            crate::messaging::test_support::test_app_config("target", None, vec!["u".to_string()]);
        app.policy = crate::messaging::test_support::brenn_delivery_policy(
            crate::access::acl::ChannelMatcher::Prefix(String::new()),
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
    // Ingress wake-gate loop semantics: an eager wake is bought by the
    // subscriber's urgency economics alone, and delivery is never gated. These
    // run at the real `dispatcher_loop` level; they crib the loop-spawning setup
    // from `dispatcher_loop_cross_bridge_isolation`
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

    /// An `UrgencyGated` subscriber's ingress rows buy no eager wake at all: an
    /// ingress row carries no urgency to clear a threshold with, and `wake_min`
    /// prices a subprocess. So a sleeping bridge stays asleep and waits for a
    /// natural spawn — the bus rule that a below-threshold backlog wakes nobody,
    /// applied to the one row-kind left on this path.
    ///
    /// Delivery is never gated by that verdict, which is the other half of the
    /// rule: the verdict prices a spawn, not a delivery. The moment the bridge
    /// is live, the very next pass delivers.
    #[tokio::test]
    async fn dispatcher_loop_urgency_gated_ingress_wakes_nobody_but_still_delivers() {
        let db = init_db_memory();
        let dir = {
            let conn = db.lock().await;
            insert_conv_with_app_slug(&conn, 1, "target");
            make_directory_and_channel(&conn).0
        };
        // "target" is an `App` subscriber, so its economics are `UrgencyGated`.
        let messenger = make_messenger_with_policy(&db, dir);
        // Router the loop delivers through; starts inactive (sleeping bridge).
        let fake = Arc::new(FakeRouter::default());
        let router: Arc<dyn super::super::WakeRouter> = fake.clone();
        let kick = Arc::new(Notify::new());
        let handle = spawn_dispatcher_task(db.clone(), router, kick.clone(), messenger);

        // Pass 1: eager row, sleeping bridge → the delivery attempt runs and
        // parks; no spawn is bought.
        {
            let conn = db.lock().await;
            insert_ingress_row(&conn, utc_to_ns(Utc::now()));
        }
        kick.notify_one();
        wait_atomic(
            &fake.deliver_calls,
            |n| n >= 1,
            "delivery attempt against the sleeping bridge",
        )
        .await;
        // Give an (erroneous) wake time to land before asserting its absence.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            fake.eager_wakes.load(Ordering::SeqCst),
            0,
            "an ingress row for an urgency-gated subscriber buys no CC spawn",
        );

        // Pass 2: bridge now live. Nothing paces the pass, so the row published
        // here is delivered milliseconds later rather than at the next tick.
        fake.set_active(true);
        let p2 = {
            let conn = db.lock().await;
            insert_ingress_row(&conn, utc_to_ns(Utc::now()) + 1)
        };
        kick.notify_one();
        wait_delivered(&db, p2, "live delivery on the pass that follows the kick").await;
        assert_eq!(
            fake.eager_wakes.load(Ordering::SeqCst),
            0,
            "a live bridge is delivered to, not woken",
        );

        handle.abort();
    }
}

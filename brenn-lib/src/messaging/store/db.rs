//! `DbStore` — the durable retention store, one per `brenn:` / `mqtt:` /
//! `webhook:` channel.
//!
//! Channel-scoped face of the SQLite message tables, so a caller can hold
//! `Arc<dyn RetentionStore>` and not know which side of the durability line
//! it is on.
//!
//! Contents survive a restart, which is the only behavioural difference from
//! [`RingStore`](super::RingStore) that the contract admits.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use brenn_envelope::{ChannelCapabilities, MessageEnvelope};
use brenn_queue::{GapReason, QuotaExceeded, ReplayDecision};

use crate::db::Db;
use crate::messaging::ParticipantId;
use crate::messaging::config::Depth;
use crate::messaging::db::{self, PendingPushInsert};

use super::{
    AppendOutcome, Attached, Committed, DeferralOutcome, DeferredMessage, DeliveryTarget,
    DropTally, MessageSeq, NewMessage, OverflowEvent, Parked, Priming, ReleaseOutcome, Released,
    ResumeCursor, RetentionStore, StoreReplay, StoreRetained, TakenMessage, TakenWindow,
    TargetRecord, TargetResolver,
};

fn store_retained((seq, envelope): (i64, MessageEnvelope)) -> StoreRetained {
    StoreRetained {
        seq: u64::try_from(seq).expect("messaging: negative retained_seq"),
        message: Arc::new(envelope),
    }
}

/// Per-subscriber push window: ordered deque of live push_ids (oldest first)
/// plus a `seeded` flag that records whether the deque has been initialised
/// from the DB on first touch after boot.
///
/// `seeded = false` means the key has never been touched in this process; the
/// deque is empty regardless of what the DB holds. On first touch the DB is
/// queried once and `seeded` is set to `true`. The seeded/empty state
/// (`seeded = true`, `ids.is_empty()`) is distinguishable from the never-seeded
/// state, which is necessary because the deque can be legitimately empty after
/// seed-then-overflow.
struct PushWindow {
    seeded: bool,
    ids: VecDeque<i64>,
}

/// Parameters a caller supplies to the push-window overflow retirement methods.
/// The channel is the store's own; only the per-subscriber knobs vary per call.
pub(crate) struct PushRetireParams<'a> {
    pub(crate) app_slug: &'a str,
    pub(crate) subscriber: &'a ParticipantId,
    pub(crate) push_depth: Depth,
}

/// One freshly-written delivery claim, offered to the subscriber's push window:
/// the window's own parameters plus the claim being offered to it.
pub(crate) struct ClaimRetirement<'a> {
    pub(crate) params: PushRetireParams<'a>,
    pub(crate) push_id: i64,
}

/// The durable retention store for one channel.
pub struct DbStore {
    db: Db,
    channel_uuid: Uuid,
    /// Canonical `brenn:<name>` / `webhook:<slug>` / `mqtt:<topic>` form.
    address: String,
    /// The channel's `retain_depth`, which also caps its deferred set: a channel
    /// may hold at most as much parked future as it holds retained past.
    /// `Unbounded` is a legitimate durable choice and lifts the deferred cap
    /// with it.
    deferred_cap: Depth,
    /// In-memory push-window tracking for bounded-`push_depth` subscribers on
    /// this channel, keyed by subscriber id. Each entry is a deque of live
    /// push-claim ids (oldest first) whose capacity is the subscriber's
    /// `push_depth`; unbounded subscribers are never present. Mutated only while
    /// the caller holds the SQLite connection (the overflow methods take it), so
    /// the deque update is a brief in-memory operation under a sync mutex.
    push_windows: Mutex<HashMap<String, PushWindow>>,
    /// Lifetime push-overflow drop count per subscriber on this channel — the
    /// durable half of [`RetentionStore::dropped_total`]. Charged where the drop
    /// happens, in the retirement methods below, whatever the subscription's
    /// noise level. In memory only, like the window it accounts for.
    dropped: DropTally,
    /// The subset of those drops the noise ladder counted, written by the
    /// substrate's enactment point ([`RetentionStore::record_metered_drops`]).
    metered: DropTally,
    /// The channel's resume epoch, read from its row on first use. Immutable for
    /// the row's lifetime — it is minted with the row and dies with it, and no
    /// runtime path deletes a channel row — so one read serves every resume.
    resume_epoch: OnceLock<Uuid>,
    /// Who this channel's messages are owed to. A record-issuing store writes
    /// one claim per subscriber, so it must name them itself: the release pass
    /// asks this at the moment it runs, which is what makes the attached set at
    /// release the set a parked message is delivered to.
    targets: Arc<TargetResolver>,
}

impl std::fmt::Debug for DbStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbStore")
            .field("channel_uuid", &self.channel_uuid)
            .field("address", &self.address)
            .field("deferred_cap", &self.deferred_cap)
            .finish_non_exhaustive()
    }
}

impl DbStore {
    pub fn new(
        db: Db,
        channel_uuid: Uuid,
        address: impl Into<String>,
        retain_depth: Depth,
        targets: Arc<TargetResolver>,
    ) -> Self {
        Self {
            db,
            channel_uuid,
            address: address.into(),
            deferred_cap: retain_depth,
            push_windows: Mutex::new(HashMap::new()),
            dropped: DropTally::default(),
            metered: DropTally::default(),
            resume_epoch: OnceLock::new(),
            targets,
        }
    }

    /// The channel's resume epoch, cached after the first read.
    fn resume_epoch(&self, conn: &rusqlite::Connection) -> Uuid {
        *self
            .resume_epoch
            .get_or_init(|| db::channel_resume_epoch(conn, self.channel_uuid))
    }

    /// Register a newly-created push-claim id into this subscriber's in-memory
    /// push window. If the window is full, removes and returns the oldest
    /// push_id for the caller to retire; the caller then routes the drop through
    /// `Messenger::enact_overflow_noise`. Returns `None` when the window has
    /// capacity or the subscriber is unbounded.
    ///
    /// On first touch after boot the deque is seeded from the DB (lazy, one-shot
    /// per key): pre-existing undelivered non-parked push rows are loaded so the
    /// bound is enforced immediately rather than waiting for the GC backstop pass.
    /// For released parked rows the caller may pass an id already present in the
    /// freshly-seeded deque; it is skipped (no double-count).
    ///
    /// Must be called after the push row is inserted and its `push_id` known,
    /// while holding the SQLite connection (`conn`, used for the one-shot seed).
    /// The caller deletes the returned push_id from `messaging_pending_pushes`.
    pub(crate) fn record_push_and_check_overflow(
        &self,
        params: &PushRetireParams<'_>,
        push_id: i64,
        conn: &rusqlite::Connection,
    ) -> Option<i64> {
        let depth = match params.push_depth {
            Depth::Unbounded => return None, // never tracked, never overflows
            Depth::Bounded(0) => panic!(
                "record_push_and_check_overflow called for Bounded(0) subscriber on channel {:?} \
                 — Bounded(0) subscribers never get push rows",
                self.address
            ),
            Depth::Bounded(n) => n as usize,
        };

        let mut windows = self.push_windows.lock().expect("push_windows poisoned");
        let window = windows
            .entry(params.subscriber.as_str().to_string())
            .or_insert(PushWindow {
                seeded: false,
                ids: VecDeque::new(),
            });

        self.seed_window(window, conn, params, depth, push_id);

        // Released parked rows: if the id is already present (seed counted it),
        // skip rather than double-count. Valid under the held lock right after the
        // seed. O(push_depth), always a small bounded integer.
        if window.ids.contains(&push_id) {
            return None;
        }

        window.ids.push_back(push_id);

        if window.ids.len() <= depth {
            return None; // window has capacity — no overflow
        }

        // Window is full: retire the oldest push-claim.
        let retired = window.ids.pop_front().expect("deque non-empty after push");
        drop(windows);
        self.charge_drops(params.subscriber, 1);
        Some(retired)
    }

    /// Register a batch of just-released parked push ids for one subscriber into
    /// its push window, atomically under a single lock: seed once, append only
    /// ids not already present, then trim the front to `push_depth` in one pass —
    /// collecting all retired ids for the caller to delete. Doing the whole batch
    /// under one lock avoids the per-row hazard where an id evicted in iteration N
    /// is invisible to the presence guard in iteration N+1 and gets re-added.
    ///
    /// Returns the retired ids (empty when the subscriber is unbounded). The
    /// caller deletes them and routes `retired.len()` drops through
    /// `enact_overflow_noise`. `push_ids` must all belong to this subscriber.
    pub(crate) fn record_push_batch_and_check_overflow(
        &self,
        params: &PushRetireParams<'_>,
        push_ids: &[i64],
        conn: &rusqlite::Connection,
    ) -> Vec<i64> {
        if push_ids.is_empty() {
            return vec![];
        }
        let depth = match params.push_depth {
            Depth::Unbounded => return vec![], // never tracked
            Depth::Bounded(0) => panic!(
                "record_push_batch_and_check_overflow called for Bounded(0) subscriber on channel \
                 {:?} — Bounded(0) subscribers never get push rows",
                self.address
            ),
            Depth::Bounded(n) => n as usize,
        };

        let mut windows = self.push_windows.lock().expect("push_windows poisoned");
        let window = windows
            .entry(params.subscriber.as_str().to_string())
            .or_insert(PushWindow {
                seeded: false,
                ids: VecDeque::new(),
            });

        // Seed once, excluding the first batch id so the seed will not include it;
        // the rest are handled by the presence check below.
        self.seed_window(window, conn, params, depth, push_ids[0]);

        // Append each batch id not already in the deque. All additions happen
        // before any trimming so eviction-by-trim cannot cause a later id to
        // re-add an earlier one.
        for &id in push_ids {
            if !window.ids.contains(&id) {
                window.ids.push_back(id);
            }
        }

        let mut retired = Vec::new();
        while window.ids.len() > depth {
            retired.push(window.ids.pop_front().expect("deque non-empty during trim"));
        }
        drop(windows);
        self.charge_drops(params.subscriber, retired.len() as u64);
        retired
    }

    /// Offer each freshly-written claim to its subscriber's push window, delete
    /// whatever the window retired to make room, and report which claims caused
    /// a retirement — by index into `claims`, so the caller pairs the drop with
    /// the noise rung it resolved for that subscriber when it resolved the
    /// target.
    ///
    /// Runs under the caller's connection, and must run under the same hold as
    /// the insert that wrote the claims: the window's order is the insert order,
    /// and another publisher offering its own claim in between would put a newer
    /// claim in front of this one, so an overflow would retire the wrong end.
    pub(crate) fn retire_claims(
        &self,
        conn: &rusqlite::Connection,
        claims: &[ClaimRetirement<'_>],
    ) -> Vec<usize> {
        let mut overflowed = Vec::new();
        for (idx, claim) in claims.iter().enumerate() {
            let retired = self.record_push_and_check_overflow(&claim.params, claim.push_id, conn);
            if let Some(retired) = retired {
                db::delete_pending_push_by_id(conn, retired);
                overflowed.push(idx);
            }
        }
        overflowed
    }

    /// Drop `ids` from whichever push windows hold them, without charging a
    /// drop: these claims left the DB because they were never owed, not because
    /// a subscriber fell behind. A window that still counted them would retire
    /// live claims early.
    fn forget_pushes(&self, ids: &[i64]) {
        if ids.is_empty() {
            return;
        }
        let mut windows = self.push_windows.lock().expect("push_windows poisoned");
        for window in windows.values_mut() {
            window.ids.retain(|id| !ids.contains(id));
        }
    }

    /// Add `count` push-overflow drops to `subscriber`'s lifetime total. Called
    /// from the retirement methods at the moment a push claim is evicted, so the
    /// total is charged once per lost message and never depends on how the
    /// caller reacts to the drop.
    fn charge_drops(&self, subscriber: &ParticipantId, count: u64) {
        self.dropped.add(subscriber, count);
    }

    /// Lazy one-shot seed of a subscriber's push window from the DB. `exclude_id`
    /// is the just-inserted push id, excluded so the seed captures pre-existing
    /// rows only. No-op once `window.seeded` is set.
    fn seed_window(
        &self,
        window: &mut PushWindow,
        conn: &rusqlite::Connection,
        params: &PushRetireParams<'_>,
        depth: usize,
        exclude_id: i64,
    ) {
        if window.seeded {
            return;
        }
        let existing = db::load_push_window(
            conn,
            self.channel_uuid,
            params.app_slug,
            params.subscriber,
            exclude_id,
        );
        for id in existing {
            window.ids.push_back(id);
        }
        // Truncate to push_depth in case the DB drifted past bound before this
        // boot (backstop hadn't run). Seed is side-effect-free on the DB.
        let discarded = window.ids.len().saturating_sub(depth);
        if discarded > 0 {
            tracing::warn!(
                channel = %self.address,
                subscriber = params.subscriber.as_str(),
                push_depth = depth,
                found_in_db = window.ids.len(),
                discarded_from_window = discarded,
                "push window seed: DB has more rows than push_depth (backstop hadn't run); oldest \
                 {discarded} ids discarded from in-memory window (GC backstop will clean DB rows \
                 within ~1 hour)"
            );
            while window.ids.len() > depth {
                window.ids.pop_front();
            }
        }
        window.seeded = true;
    }

    /// Test-only: charge `amount` push-overflow drops to `subscriber` without
    /// driving a real publish through the window. Feature-gated rather than
    /// `#[cfg(test)]` so downstream test crates reach it through
    /// `messaging::testutils::inject_drop`.
    #[cfg(any(test, feature = "testutils"))]
    pub fn inject_dropped(&self, subscriber: &ParticipantId, amount: u64) {
        self.charge_drops(subscriber, amount);
    }

    /// Test-only: `true` if no push window has been touched since boot. Used to
    /// assert unbounded subscribers never create window entries.
    #[cfg(test)]
    pub(crate) fn push_windows_is_empty(&self) -> bool {
        self.push_windows
            .lock()
            .expect("push_windows poisoned")
            .is_empty()
    }

    fn push_inserts(targets: &[DeliveryTarget]) -> Vec<PendingPushInsert> {
        targets
            .iter()
            .map(|t| PendingPushInsert {
                target_subscriber: t.subscriber.clone(),
                target_app_slug: t.app_slug.clone(),
                eager_wake: t.eager_wake,
                release_after: None,
                delivery_deadline: t.delivery_deadline,
            })
            .collect()
    }

    fn committed(inserted: db::InsertedMessage) -> Committed {
        let seq = inserted
            .retained_seq
            .expect("messaging: an appended message is assigned a retained_seq");
        Committed {
            message_uuid: inserted.uuid,
            seq: MessageSeq(u64::try_from(seq).expect("messaging: negative retained_seq")),
            target_records: inserted.push_ids.into_iter().map(TargetRecord).collect(),
        }
    }

    fn assert_owner(&self, lookup: &db::DeferredLookup, sender: &str) {
        assert!(
            lookup.sender == sender,
            "messaging store: {} message {} belongs to {}, not {sender} — a sender-scoped view \
             was bypassed",
            self.address,
            lookup.message_id,
            lookup.sender
        );
    }
}

#[async_trait]
impl RetentionStore for DbStore {
    fn channel_uuid(&self) -> Uuid {
        self.channel_uuid
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities::DURABLE_TRANSPORTABLE
    }

    async fn deliverable_subscribers(&self) -> Vec<ParticipantId> {
        let conn = self.db.lock().await;
        db::channel_deliverable_subscribers(&conn, self.channel_uuid)
    }

    async fn has_deliverable(&self, subscriber: &ParticipantId) -> bool {
        let conn = self.db.lock().await;
        db::channel_has_deliverable_for(&conn, self.channel_uuid, subscriber)
    }

    /// Claims are left pending until the consumer settles them, so a host that
    /// dies mid-activation redelivers rather than losing the batch — which is
    /// why the durable take and peek are the same read.
    async fn take_new(&self, subscriber: &ParticipantId, limit: u64) -> TakenWindow {
        self.peek_new(subscriber, limit).await
    }

    /// Hands over the subscriber's undelivered claims, each carrying its claim
    /// id as the record to settle with. Claims past `limit` are held for the
    /// next read, not dropped: this charges nothing.
    ///
    /// The claim read and the drop total are taken under one connection hold,
    /// so the total names exactly what is missing from the window.
    async fn peek_new(&self, subscriber: &ParticipantId, limit: u64) -> TakenWindow {
        if limit == 0 {
            // A sampled subscriber holds no claims to take (no push row is ever
            // written for one); pending claims from before a config change are
            // the caller's residue to retire, not this take's business.
            return TakenWindow {
                messages: vec![],
                dropped: 0,
                dropped_total: self.dropped.get(subscriber),
                clamped_leftover: 0,
            };
        }
        let conn = self.db.lock().await;
        let claims = db::load_pending_pushes_for_channel(&conn, subscriber, self.channel_uuid);
        let held = claims.len();
        let messages: Vec<TakenMessage> = claims
            .into_iter()
            .take(usize::try_from(limit).expect("messaging: push depth out of range"))
            .map(|row| TakenMessage {
                record: Some(TargetRecord(row.push_id)),
                envelope: Arc::new(row.envelope),
            })
            .collect();
        TakenWindow {
            clamped_leftover: held - messages.len(),
            messages,
            dropped: 0,
            dropped_total: self.dropped.get(subscriber),
        }
    }

    /// Settles exactly the claims named, and only them: a claim the consumer
    /// did not accept stays pending and is handed over again by the next read.
    /// Charges nothing — a durable claim that reaches its consumer is delivered,
    /// not dropped.
    ///
    /// The subscriber is named for the contract's sake; the claim ids identify
    /// the rows on their own.
    async fn settle(&self, _subscriber: &ParticipantId, records: &[TargetRecord]) -> u64 {
        if records.is_empty() {
            return 0;
        }
        let ids: Vec<i64> = records.iter().map(|record| record.0).collect();
        let conn = self.db.lock().await;
        db::mark_pending_pushes_delivered(&conn, &ids);
        0
    }

    /// Requires no connection: safe to call while the caller already holds one.
    fn dropped_total(&self, subscriber: &ParticipantId) -> u64 {
        self.dropped.get(subscriber)
    }

    fn record_metered_drops(&self, subscriber: &ParticipantId, count: u64) {
        self.metered.add(subscriber, count);
    }

    fn metered_drops(&self, subscriber: &ParticipantId) -> u64 {
        self.metered.get(subscriber)
    }

    async fn attach(
        &self,
        subscriber: &ParticipantId,
        app_slug: &str,
        push_depth: u64,
        priming: Priming,
        fresh_queue: bool,
    ) -> Attached {
        // A surviving queue keeps its pending rows across the restart — no
        // re-prime. A fresh queue with retained priming seeds the tail as NEW
        // so the consumer wakes on existing messages, not only the next publish.
        if !fresh_queue {
            return Attached::Existing;
        }
        if let Priming::Retained = priming {
            let conn = self.db.lock().await;
            let tail = db::load_channel_retained_tail(
                &conn,
                self.channel_uuid,
                Depth::Bounded(push_depth),
            );
            if !tail.is_empty() {
                let ids: Vec<i64> = tail.iter().map(|(id, _)| *id).collect();
                db::seed_pending_pushes_for_messages(&conn, &ids, subscriber, app_slug);
            }
        }
        Attached::Created
    }

    async fn detach(&self, subscriber: &ParticipantId) {
        let conn = self.db.lock().await;
        db::delete_pushes_for_subscriber(&conn, self.channel_uuid, subscriber);
        self.push_windows
            .lock()
            .expect("push_windows poisoned")
            .remove(subscriber.as_str());
        self.dropped.forget(subscriber);
        self.metered.forget(subscriber);
    }

    /// Resolves the channel's subscribers, writes one claim per target, then
    /// offers each to its subscriber's push window — all under one connection
    /// hold. A subscriber already at its depth has its oldest claim retired to
    /// make room, and that retirement is the drop this reports. Charging it here
    /// is what lets a consumer that never runs escalate the noise ladder without
    /// waiting to be read.
    async fn append(&self, msg: NewMessage) -> AppendOutcome {
        let conn = self.db.lock().await;
        let targets = self.targets.commit_targets(
            &conn,
            self.channel_uuid,
            msg.urgency,
            msg.delivery_deadline,
        );
        let targets = targets.as_slice();
        let committed = Self::committed(db::insert_message_with_pushes(
            &conn,
            self.channel_uuid,
            &msg.source,
            &msg.sender,
            &msg.body,
            msg.urgency,
            msg.envelope_type,
            msg.reply_to_uuid,
            msg.delivery_deadline,
            None,
            msg.publish_ts_ns,
            &Self::push_inserts(targets),
        ));
        // One claim per target is what the insert was asked for; pairing them by
        // position is only sound while that holds. A disagreement would pair a
        // claim with another subscriber's window or skip a subscriber outright,
        // and the drops it mis-attributes feed the noise ladder.
        assert_eq!(
            targets.len(),
            committed.target_records.len(),
            "messaging store: {} wrote {} claims for {} targets",
            self.address,
            committed.target_records.len(),
            targets.len()
        );
        let claims: Vec<ClaimRetirement<'_>> = targets
            .iter()
            .zip(&committed.target_records)
            .map(|(target, record)| ClaimRetirement {
                params: PushRetireParams {
                    app_slug: &target.app_slug,
                    subscriber: &target.subscriber,
                    push_depth: target.push_depth,
                },
                push_id: record.0,
            })
            .collect();
        let overflow = self
            .retire_claims(&conn, &claims)
            .into_iter()
            .map(|idx| OverflowEvent {
                subscriber: targets[idx].subscriber.clone(),
                dropped: 1,
                app_slug: Some(targets[idx].app_slug.clone()),
            })
            .collect();
        AppendOutcome {
            committed,
            overflow,
        }
    }

    /// Writes the message row alone: a parked message holds no push claims, so
    /// there is nothing for a mid-park attach to miss or a mid-park departure to
    /// leave behind. Claims are minted by [`RetentionStore::release_due`].
    async fn park(
        &self,
        msg: NewMessage,
        release_at: DateTime<Utc>,
    ) -> Result<Parked, QuotaExceeded> {
        let conn = self.db.lock().await;
        if let Depth::Bounded(cap) = self.deferred_cap {
            // Cap check and insert share the connection guard, so the count a
            // publish is admitted against is the count it is added to.
            if db::count_deferred(&conn, self.channel_uuid) >= cap {
                return Err(QuotaExceeded { cap });
            }
        }
        let inserted = db::insert_message_with_pushes(
            &conn,
            self.channel_uuid,
            &msg.source,
            &msg.sender,
            &msg.body,
            msg.urgency,
            msg.envelope_type,
            msg.reply_to_uuid,
            msg.delivery_deadline,
            Some(release_at),
            msg.publish_ts_ns,
            &[],
        );
        Ok(Parked {
            message_uuid: inserted.uuid,
        })
    }

    async fn retained_tail(&self, limit: Depth) -> Vec<Arc<MessageEnvelope>> {
        let conn = self.db.lock().await;
        db::load_channel_retained_tail(&conn, self.channel_uuid, limit)
            .into_iter()
            .map(|(_, envelope)| Arc::new(envelope))
            .collect()
    }

    async fn replay_from(&self, cursor: Option<ResumeCursor>, limit: Depth) -> StoreReplay {
        let conn = self.db.lock().await;
        let epoch = self.resume_epoch(&conn);
        let cap = self.deferred_cap.narrowed_by(limit);

        let window = || -> Vec<StoreRetained> {
            db::load_channel_retained_window_seq(&conn, self.channel_uuid, cap)
                .into_iter()
                .map(store_retained)
                .collect()
        };
        let gap = |reason: GapReason| StoreReplay {
            epoch,
            messages: window(),
            decision: ReplayDecision::Gap(reason),
        };

        let Some(cursor) = cursor else {
            return StoreReplay {
                epoch,
                messages: window(),
                decision: ReplayDecision::Fresh,
            };
        };

        // A cursor from a different numbering domain — a wiped/recreated channel
        // row, or another store entirely — names seqs this epoch never assigned.
        if cursor.epoch != epoch {
            return gap(GapReason::EpochChanged);
        }

        // The persisted high-water decides the empty window: a cursor at it is up
        // to date even with no rows retained, one below it proves lost history,
        // one above it a store rolled backwards.
        let last_seq = u64::try_from(db::channel_last_retained_seq(&conn, self.channel_uuid))
            .expect("messaging: negative last_retained_seq");
        if cursor.seq == last_seq {
            return StoreReplay {
                epoch,
                messages: Vec::new(),
                decision: ReplayDecision::UpToDate,
            };
        }
        if cursor.seq > last_seq {
            return gap(GapReason::ResumeAhead);
        }

        // A trailing cursor is an exact suffix iff every sequence assigned above
        // it is still retained and the whole run fits the window cap. Counting
        // rather than testing the window's oldest edge is what makes this exact:
        // sequences are dense at assignment but eviction can leave a hole behind
        // a row a tentative push protects, and an edge test would read such a
        // window as a contiguous suffix and serve it as `Exact`.
        let after = i64::try_from(cursor.seq).expect("messaging: resume seq out of range");
        let owed = last_seq - cursor.seq;
        let present = u64::try_from(db::channel_retained_count_after_seq(
            &conn,
            self.channel_uuid,
            after,
        ))
        .expect("messaging: negative retained-row count");
        let fits = match cap {
            Depth::Unbounded => true,
            Depth::Bounded(cap) => owed <= cap,
        };
        if present == owed && fits {
            let messages =
                db::load_channel_messages_after_seq(&conn, self.channel_uuid, after, cap)
                    .into_iter()
                    .map(store_retained)
                    .collect();
            StoreReplay {
                epoch,
                messages,
                decision: ReplayDecision::Exact,
            }
        } else {
            gap(GapReason::BeyondRetained)
        }
    }

    async fn next_release(&self) -> Option<DateTime<Utc>> {
        let conn = self.db.lock().await;
        db::earliest_channel_release(&conn, self.channel_uuid)
    }

    /// Resolves the channel's subscribers, then mints one claim per target for
    /// each released message, so the set attached when the pass runs is exactly
    /// the set the message is delivered to. Resolution shares the connection
    /// with the release it targets, so a subscription cannot appear or vanish
    /// between the two. Claims predating the release — a message re-parked by an
    /// edit after it was published live — are stale by that rule and are dropped
    /// from the DB and from their subscriber's push window.
    ///
    /// Reports no overflow, for the same reason [`RetentionStore::append`] does:
    /// a released message writes claims, and a claim is retired — and charged —
    /// by the retirement pass, not by the release.
    async fn release_due(&self, now: DateTime<Utc>) -> ReleaseOutcome {
        let conn = self.db.lock().await;
        let targets = self.targets.release_targets(&conn, self.channel_uuid);
        let targets = targets.as_slice();
        // Every due message, whether or not it has anywhere to go: release moves
        // it into retention either way, so reporting only the ones with push
        // rows would hide it from the caller that accounts for the batch.
        let batch = db::release_due_for_channel(&conn, self.channel_uuid, now, targets);
        self.forget_pushes(&batch.stale_claims);
        let released = batch
            .released
            .into_iter()
            .map(|row| Released {
                seq: MessageSeq(
                    u64::try_from(row.retained_seq).expect("messaging: negative retained_seq"),
                ),
                envelope: Arc::new(row.envelope),
                target_records: row.push_ids.into_iter().map(TargetRecord).collect(),
            })
            .collect();
        ReleaseOutcome {
            released,
            overflow: Vec::new(),
        }
    }

    async fn deferred_for_sender(&self, sender: &str, now: DateTime<Utc>) -> Vec<DeferredMessage> {
        let conn = self.db.lock().await;
        db::list_deferred_for_sender(&conn, self.channel_uuid, sender, now)
            .into_iter()
            .map(|row| DeferredMessage {
                release_at: row.release_at,
                envelope: Arc::new(row.envelope),
            })
            .collect()
    }

    async fn deferred_len(&self) -> u64 {
        let conn = self.db.lock().await;
        db::count_deferred(&conn, self.channel_uuid)
    }

    async fn cancel_deferred(
        &self,
        sender: &str,
        message_uuid: Uuid,
        now: DateTime<Utc>,
    ) -> DeferralOutcome {
        let conn = self.db.lock().await;
        let Some(lookup) = db::lookup_deferred(&conn, self.channel_uuid, message_uuid, now) else {
            return DeferralOutcome::NotDeferred;
        };
        self.assert_owner(&lookup, sender);
        match db::delete_deferred(&conn, self.channel_uuid, lookup.message_id, now) {
            true => DeferralOutcome::Applied,
            false => DeferralOutcome::NotDeferred,
        }
    }

    async fn edit_deferred(
        &self,
        sender: &str,
        message_uuid: Uuid,
        body: Option<String>,
        release_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> DeferralOutcome {
        let conn = self.db.lock().await;
        let Some(lookup) = db::lookup_deferred(&conn, self.channel_uuid, message_uuid, now) else {
            return DeferralOutcome::NotDeferred;
        };
        self.assert_owner(&lookup, sender);
        let applied = db::edit_deferred(
            &conn,
            self.channel_uuid,
            lookup.message_id,
            body.as_deref(),
            release_at,
            now,
        );
        match applied {
            true => DeferralOutcome::Applied,
            false => DeferralOutcome::NotDeferred,
        }
    }
}

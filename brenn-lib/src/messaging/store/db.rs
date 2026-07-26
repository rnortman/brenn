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
    AdvanceOutcome, AppendOutcome, Attached, Committed, DeferralOutcome, DeferredMessage,
    DeliverableSubscriber, DeliveryTarget, MessageSeq, NewMessage, OverflowEvent, Parked, Priming,
    ReleaseOutcome, Released, ResumeCursor, RetentionStore, StoreReplay, StoreRetained,
    SubscriberWindow, TargetRecord, TargetResolver, compose_window, depth_bound,
};

fn store_retained((seq, envelope): (i64, MessageEnvelope)) -> StoreRetained {
    StoreRetained {
        seq: u64::try_from(seq).expect("messaging: negative retained_seq"),
        message: Arc::new(envelope),
    }
}

/// A `retained_seq` column value as a store position.
fn retained_seq(seq: i64) -> MessageSeq {
    MessageSeq(u64::try_from(seq).expect("messaging: negative retained_seq"))
}

/// A store position as a `retained_seq` column value.
fn seq_column(seq: MessageSeq) -> i64 {
    i64::try_from(seq.0).expect("messaging: retained_seq out of range")
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

    /// Where this channel's retained set begins, or the seq it will assign next
    /// when it holds nothing — the boundary below which every message is gone.
    fn frontier(&self, conn: &rusqlite::Connection) -> u64 {
        db::channel_retention_frontier(conn, self.channel_uuid)
            .map(|seq| retained_seq(seq).0)
            .unwrap_or_else(|| {
                retained_seq(db::channel_last_retained_seq(conn, self.channel_uuid)).0 + 1
            })
    }

    /// Bring `subscriber`'s cursor row into line with this attach: a
    /// push-enabled subscriber gets one at its primed position (or keeps the
    /// position it already holds, with the caches retuned), a sampled one loses
    /// whatever it held.
    ///
    /// The sampled branch is the demotion rule: a subscription that lands at
    /// depth 0 is never delivered to again, and a position left behind would be
    /// reported against by every eviction pass forever.
    ///
    /// Returns the store's own Created/Existing determination — row presence,
    /// and nothing else. A sampled attach creates no queue and so is always
    /// `Existing`.
    fn maintain_cursor(
        &self,
        conn: &rusqlite::Connection,
        subscriber: &ParticipantId,
        app_slug: &str,
        push_depth: Depth,
        priming: Priming,
    ) -> Attached {
        if !push_depth.is_push_enabled() {
            db::delete_subscriber_cursor(conn, self.channel_uuid, subscriber);
            return Attached::Existing;
        }
        let primed = self.primed_position(conn, push_depth, priming);
        let created = db::ensure_subscriber_cursor(
            conn,
            self.channel_uuid,
            subscriber,
            app_slug,
            push_depth,
            primed,
        );
        if created {
            Attached::Created
        } else {
            Attached::Existing
        }
    }

    /// Where a cursor coming into existence starts.
    ///
    /// `Head` starts one past every sequence the channel ever assigned — owed
    /// only what publishes next. `Retained` starts at the oldest of the
    /// `push_depth` newest retained messages, because attach is a delivery point
    /// for a component queue: a message published before the component existed
    /// still reaches it. A channel retaining nothing primes at head either way.
    fn primed_position(
        &self,
        conn: &rusqlite::Connection,
        push_depth: Depth,
        priming: Priming,
    ) -> i64 {
        let head = db::channel_last_retained_seq(conn, self.channel_uuid) + 1;
        match priming {
            Priming::Head => head,
            Priming::Retained => {
                db::retained_tail_floor_seq(conn, self.channel_uuid, push_depth).unwrap_or(head)
            }
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

    async fn deliverable_subscribers(&self) -> Vec<DeliverableSubscriber> {
        let conn = self.db.lock().await;
        db::deliverable_cursor_subscribers(&conn, self.channel_uuid)
    }

    async fn has_deliverable(&self, subscriber: &ParticipantId) -> bool {
        let conn = self.db.lock().await;
        db::cursor_has_deliverable(&conn, self.channel_uuid, subscriber)
    }

    /// The retained span this subscriber's cursor sits in, with the new boundary
    /// cut from the position the cursor row holds.
    ///
    /// Both reads happen under one connection hold, so the boundary cannot name
    /// a message the window does not carry. A push-enabled `push_limit` retunes
    /// the row's depth cache, the caller staying its single authority; a sampled
    /// one is a read the row does not record.
    ///
    /// A sampled subscriber holds no cursor and is never delivered to: its
    /// window is all context, cut at a position above everything the channel
    /// ever assigned. A push-enabled subscriber with no cursor is a wiring bug
    /// and panics.
    async fn window(
        &self,
        subscriber: &ParticipantId,
        push_limit: Depth,
        retain_limit: Depth,
    ) -> SubscriberWindow {
        let conn = self.db.lock().await;
        let next_owed = match db::load_subscriber_cursor(&conn, self.channel_uuid, subscriber) {
            Some(cursor) => {
                // A sampled read is a pure channel read: it delivers nothing, so
                // it does not touch the position it borrowed. Writing its zero
                // into the row would leave a depth the row's own contract says
                // cannot exist there, on a subscriber still listed as
                // deliverable.
                if push_limit.is_push_enabled() && cursor.push_depth != push_limit {
                    db::retune_subscriber_cursor_depth(
                        &conn,
                        self.channel_uuid,
                        subscriber,
                        push_limit,
                    );
                }
                retained_seq(cursor.next_owed_seq)
            }
            None => {
                assert!(
                    !push_limit.is_push_enabled(),
                    "messaging store: {} has no cursor for {} — a push-enabled window over a \
                     queue that was never created",
                    self.address,
                    subscriber.as_str()
                );
                MessageSeq(
                    retained_seq(db::channel_last_retained_seq(&conn, self.channel_uuid)).0 + 1,
                )
            }
        };
        let span = match (push_limit, retain_limit) {
            (Depth::Unbounded, _) | (_, Depth::Unbounded) => Depth::Unbounded,
            (p, r) => Depth::Bounded(depth_bound(p).max(depth_bound(r))),
        };
        let entries = db::load_channel_retained_window_seq(&conn, self.channel_uuid, span)
            .into_iter()
            .map(|(seq, envelope)| (retained_seq(seq), Arc::new(envelope)))
            .collect();
        compose_window(entries, next_owed, push_limit)
    }

    /// Move the cursor to `through + 1` and report the unseen seqs no window
    /// served, both figures subtractions between seqs.
    ///
    /// Panics for a subscriber holding no cursor — sampled or never attached.
    ///
    /// TODO(substrate-wake-relocation): the claim retirement below is wake
    /// bookkeeping, not delivery state. The dispatcher still scans owed claim
    /// rows to decide who to wake, so a claim left standing past its cursor
    /// would re-wake the consumer on every tick. It goes when the wake pass
    /// moves onto live registration and the scan is deleted.
    async fn advance(
        &self,
        subscriber: &ParticipantId,
        through: MessageSeq,
        seen_floor: MessageSeq,
    ) -> AdvanceOutcome {
        assert!(
            seen_floor.0 <= through.0.saturating_add(1),
            "messaging store: {} seen_floor {} is above the window it came from (through {})",
            self.address,
            seen_floor.0,
            through.0
        );
        let conn = self.db.lock().await;
        let cursor = db::load_subscriber_cursor(&conn, self.channel_uuid, subscriber)
            .unwrap_or_else(|| {
                panic!(
                    "messaging store: {} has no cursor for {} to advance",
                    self.address,
                    subscriber.as_str()
                )
            });
        let next_owed = retained_seq(cursor.next_owed_seq).0;
        let dropped = seen_floor.0.saturating_sub(next_owed);
        let outcome = AdvanceOutcome {
            dropped,
            // Losses below the frontier were already charged by eviction; only
            // the still-retained part is charged here. A consumer that lost
            // nothing is charged nothing whatever the frontier is, so the read
            // that finds it stays off the keeping-up path.
            noise_charge: if dropped == 0 {
                0
            } else {
                seen_floor
                    .0
                    .saturating_sub(next_owed.max(self.frontier(&conn)))
            },
        };
        // One transaction: a kill between the move and the retirement would
        // leave claims standing below a cursor that has passed them, and the
        // dispatcher would re-wake this consumer for work its position says it
        // has already done, on every pass, forever.
        let tx = conn
            .unchecked_transaction()
            .expect("messaging store: begin advance tx");
        if through.0 >= next_owed {
            db::set_subscriber_cursor_position(
                &tx,
                self.channel_uuid,
                subscriber,
                seq_column(MessageSeq(through.0 + 1)),
            );
        }
        let passed: Vec<i64> = db::owed_push_positions(&tx, subscriber, self.channel_uuid)
            .into_iter()
            .filter(|(_, seq)| *seq <= seq_column(through))
            .map(|(push_id, _)| push_id)
            .collect();
        db::mark_pending_pushes_delivered(&tx, &passed);
        tx.commit().expect("messaging store: commit advance tx");
        outcome
    }

    /// The cursor row is the whole of the determination: it exists, or this
    /// attach creates it at the primed position. Nothing outside the store gets
    /// a say, so a queue that survived a restart cannot be re-primed by a caller
    /// that mis-remembers it, and one that never existed cannot be skipped.
    ///
    /// TODO(substrate-wake-relocation): the retained-tail claim seed below is
    /// wake bookkeeping, as in `advance` — the dispatcher's scan is what wakes a
    /// freshly primed component, and it reads claim rows. The cursor is already
    /// primed to the same tail, so the seed decides nothing about delivery.
    async fn attach(
        &self,
        subscriber: &ParticipantId,
        app_slug: &str,
        push_depth: Depth,
        priming: Priming,
    ) -> Attached {
        let conn = self.db.lock().await;
        // One transaction: a position and the claims that wake it are one
        // fact, and a kill between them leaves the two disagreeing for good.
        let tx = conn
            .unchecked_transaction()
            .expect("messaging store: begin attach tx");
        let attached = self.maintain_cursor(&tx, subscriber, app_slug, push_depth, priming);
        if attached == Attached::Created && priming == Priming::Retained {
            let tail = db::load_channel_retained_tail(&tx, self.channel_uuid, push_depth);
            if !tail.is_empty() {
                let ids: Vec<i64> = tail.iter().map(|(id, _)| *id).collect();
                db::seed_pending_pushes_for_messages(&tx, &ids, subscriber, app_slug);
            }
        }
        tx.commit().expect("messaging store: commit attach tx");
        attached
    }

    async fn detach(&self, subscriber: &ParticipantId) {
        let conn = self.db.lock().await;
        db::delete_subscriber_cursor(&conn, self.channel_uuid, subscriber);
        db::delete_pushes_for_subscriber(&conn, self.channel_uuid, subscriber);
        self.push_windows
            .lock()
            .expect("push_windows poisoned")
            .remove(subscriber.as_str());
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

//! `DbStore` — the durable retention store, one per `brenn:` / `mqtt:` /
//! `webhook:` channel.
//!
//! Channel-scoped face of the SQLite message tables, so a caller can hold
//! `Arc<dyn RetentionStore>` and not know which side of the durability line
//! it is on.
//!
//! Contents survive a restart, which is the only behavioural difference from
//! [`RingStore`](super::RingStore) that the contract admits.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use brenn_envelope::{ChannelCapabilities, MessageEnvelope};
use brenn_queue::{GapReason, QuotaExceeded, ReplayDecision};

use crate::db::Db;
use crate::messaging::ParticipantId;
use crate::messaging::config::Depth;
use crate::messaging::db;

use super::{
    AdvanceOutcome, AppendOutcome, Attached, Committed, DeferralOutcome, DeferredMessage,
    DeliverableSubscriber, MessageSeq, NewMessage, Parked, Priming, ReleaseOutcome, Released,
    ResumeCursor, RetentionStore, StoreReplay, StoreRetained, SubscriberWindow, compose_window,
    depth_bound,
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
    /// The channel's resume epoch, read from its row on first use. Immutable for
    /// the row's lifetime — it is minted with the row and dies with it, and no
    /// runtime path deletes a channel row — so one read serves every resume.
    resume_epoch: OnceLock<Uuid>,
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
    ) -> Self {
        Self {
            db,
            channel_uuid,
            address: address.into(),
            deferred_cap: retain_depth,
            resume_epoch: OnceLock::new(),
        }
    }

    /// The channel's resume epoch, cached after the first read.
    fn resume_epoch(&self, conn: &rusqlite::Connection) -> Uuid {
        *self
            .resume_epoch
            .get_or_init(|| db::channel_resume_epoch(conn, self.channel_uuid))
    }

    fn committed(inserted: db::InsertedMessage) -> Committed {
        let seq = inserted
            .retained_seq
            .expect("messaging: an appended message is assigned a retained_seq");
        Committed {
            message_uuid: inserted.uuid,
            seq: MessageSeq(u64::try_from(seq).expect("messaging: negative retained_seq")),
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
    /// ever assigned. A push-enabled read finds no cursor and answers `None`
    /// for the caller to judge.
    async fn window(
        &self,
        subscriber: &ParticipantId,
        push_limit: Depth,
        retain_limit: Depth,
    ) -> Option<SubscriberWindow> {
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
                if push_limit.is_push_enabled() {
                    return None;
                }
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
        Some(compose_window(entries, next_owed, push_limit))
    }

    /// Move the cursor to `through + 1` and report the unseen seqs no window
    /// served, both figures subtractions between seqs.
    ///
    /// `None` for a subscriber holding no cursor: there is no position to move
    /// and nothing to report, and nothing is written on that path.
    async fn advance(
        &self,
        subscriber: &ParticipantId,
        through: MessageSeq,
        seen_floor: MessageSeq,
    ) -> Option<AdvanceOutcome> {
        assert!(
            seen_floor.0 <= through.0.saturating_add(1),
            "messaging store: {} seen_floor {} is above the window it came from (through {})",
            self.address,
            seen_floor.0,
            through.0
        );
        let conn = self.db.lock().await;
        let cursor = db::load_subscriber_cursor(&conn, self.channel_uuid, subscriber)?;
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
        if through.0 >= next_owed {
            db::set_subscriber_cursor_position(
                &conn,
                self.channel_uuid,
                subscriber,
                seq_column(MessageSeq(through.0 + 1)),
            );
        }
        Some(outcome)
    }

    /// The cursor row is the whole of the determination: it exists, or this
    /// attach creates it at the primed position. Nothing outside the store gets
    /// a say, so a queue that survived a restart cannot be re-primed by a caller
    /// that mis-remembers it, and one that never existed cannot be skipped.
    async fn attach(
        &self,
        subscriber: &ParticipantId,
        app_slug: &str,
        push_depth: Depth,
        priming: Priming,
    ) -> Attached {
        let conn = self.db.lock().await;
        self.maintain_cursor(&conn, subscriber, app_slug, push_depth, priming)
    }

    async fn detach(&self, subscriber: &ParticipantId) {
        let conn = self.db.lock().await;
        db::delete_subscriber_cursor(&conn, self.channel_uuid, subscriber);
    }

    /// Writes the message row, and nothing per subscriber.
    ///
    /// Reports no overflow: a commit charges nothing. What a subscriber lost is
    /// the distance between its position and the window its next read serves,
    /// reported at that read (or by the eviction pass that outran it), so a
    /// third figure taken here would double-count the same loss.
    async fn append(&self, msg: NewMessage) -> AppendOutcome {
        let conn = self.db.lock().await;
        AppendOutcome {
            committed: Self::committed(db::insert_message(
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
            )),
            overflow: Vec::new(),
        }
    }

    /// Writes the message row without a retention position, which is the whole of
    /// what parks it: every read that could observe it is a retention read.
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
        let inserted = db::insert_message(
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
        // rather than testing the window's oldest edge is what keeps this exact
        // whatever eviction leaves behind: an edge test would read a window with
        // an interior hole as a contiguous suffix and serve it as `Exact`.
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

    /// Moves each due message into retention with a fresh position, and writes
    /// nothing per subscriber: release is what puts the message where the
    /// positions can see it, and each subscriber reads it from its own.
    ///
    /// Reports no overflow, for the same reason [`RetentionStore::append`] does:
    /// entering retention charges nobody. A release that pushes a lagging
    /// subscriber's unseen messages out of the window is reported by the
    /// eviction pass and by that subscriber's next read.
    async fn release_due(&self, now: DateTime<Utc>) -> ReleaseOutcome {
        let conn = self.db.lock().await;
        let released = db::release_due_for_channel(&conn, self.channel_uuid, now)
            .into_iter()
            .map(|row| Released {
                seq: MessageSeq(
                    u64::try_from(row.retained_seq).expect("messaging: negative retained_seq"),
                ),
                envelope: Arc::new(row.envelope),
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

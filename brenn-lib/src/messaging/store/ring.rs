//! `RingStore` — the in-memory retention store for non-durable channels.
//!
//! One store per `ephemeral:` or `local:` channel. It owns four things, all of
//! them `brenn-queue` mechanics wrapped in the backend's identity and locking:
//! a bounded drop-oldest retained ring, one cursor per attached subscriber, a
//! release-time-ordered set of parked messages, and — on a transportable
//! channel — a broadcast fan-out to consumers that read the channel as a live
//! stream rather than from a cursor.
//!
//! The store holds no policy. Whether a subscriber's fresh cursor is primed
//! with the retained tail, whether a publish is allowed at all, and what a
//! reported drop count escalates to on the noise ladder are all decided by the
//! callers that know the subscriber kind and the channel's config; the store's
//! job is to make those decisions enactable identically to the durable path.
//!
//! Contents are lost on restart, by construction: a new store means a new
//! epoch, which is exactly the typed gap a resuming subscriber already knows
//! how to read.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::broadcast;
use tracing::debug;
use uuid::Uuid;

use brenn_envelope::{ChannelCapabilities, ChannelScheme, MessageEnvelope};
use brenn_queue::{
    Deferred, DeferredId, DeferredSet, NoSuchDeferred, QuotaExceeded, Replay, ReplayDecision,
    Resume, RetainedRing, SubscriberCursor, Take,
};

use crate::messaging::ParticipantId;
use crate::messaging::config::Depth;
use crate::messaging::db::ns_to_utc;
use crate::messaging::store::{
    AppendOutcome, Attached, Committed, DeferralOutcome, DeferredMessage, DropTally, MessageSeq,
    NewMessage, OverflowEvent, Parked, Priming, ReleaseOutcome, Released, ResumeCursor,
    RetentionStore, StoreReplay, TakenMessage, TakenWindow, TargetRecord, instant_of,
    release_time_of,
};

/// A ring position as the record naming one subscriber's copy of the message
/// sitting there.
///
/// The ring keeps no per-subscriber rows, so the position *is* the handle: a
/// consumer settles by naming how far it got, and the cursor moves there.
fn position_record(seq: u64) -> TargetRecord {
    TargetRecord(i64::try_from(seq).expect("messaging store: ring position out of range"))
}

/// Maximum retained-ring depth accepted at construction.
///
/// A sanity cap against absurd config, not a memory budget. Upstream validation
/// may reject some values, but this store owns the allocation and must bound it
/// independently. Worst-case live retention at the cap is
/// `depth × max_body_bytes` per channel.
pub const MAX_RING_RETAIN_DEPTH: u64 = 4_096;

/// Per-channel live fan-out capacity.
///
/// `tokio::sync::broadcast::channel` pre-allocates its ring, so this is an
/// allocation: ≈ `capacity × size_of::<slot>` per transportable channel. It
/// bounds how far a slow live consumer may lag before it is charged a typed
/// gap; it is not an operator knob.
pub const RING_FAN_OUT_CAPACITY: u32 = 256;

/// A message that has entered retention, with the sequence number the ring
/// assigned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedMessage {
    /// Dense and ascending from 1 within the store's epoch. Travels alongside
    /// the envelope, never inside it.
    pub seq: u64,
    pub envelope: Arc<MessageEnvelope>,
}

/// A message that entered retention, with the overflow that entry caused.
#[derive(Debug, Clone)]
pub struct Appended {
    pub retained: RetainedMessage,
    /// One entry per subscriber whose owed messages this append evicted.
    pub overflow: Vec<OverflowEvent>,
}

/// A release pass's messages, with the overflow the batch caused, merged per
/// subscriber.
#[derive(Debug, Clone)]
pub struct ReleasedBatch {
    pub messages: Vec<RetainedMessage>,
    pub overflow: Vec<OverflowEvent>,
}

/// A live consumer's attach point: what it is owed from the retained window,
/// why (the replay decision), and the stream of everything committed after.
///
/// The split between the two halves is exact — a message is either entirely in
/// `replay` or entirely on `receiver`, never both and never neither.
pub struct LiveAttach {
    pub replay: Vec<Arc<RetainedMessage>>,
    pub decision: ReplayDecision,
    pub receiver: broadcast::Receiver<Arc<RetainedMessage>>,
}

/// Per-channel mutable state. One lock covers all three structures, so seq
/// assignment, ring append, and cursor bookkeeping are atomic with respect to
/// an attach: for any attach, a message is either entirely before the cursor's
/// starting position or entirely after it.
#[derive(Debug)]
struct RingState {
    ring: RetainedRing<Arc<MessageEnvelope>, Uuid>,
    deferred: DeferredSet<Arc<MessageEnvelope>>,
    cursors: HashMap<ParticipantId, SubscriberCursor>,
}

impl RingState {
    /// Append into the ring and charge every attached cursor whose owed
    /// messages the append pushed out of the window.
    ///
    /// Accounting the eviction here, under the same lock that performed it, is
    /// what makes a drop attributable the moment it happens: a subscriber that
    /// never runs — wedged, starved, or simply idle — is still charged, and the
    /// noise ladder escalates against it without waiting for a take that may
    /// never come.
    fn append_charging_evictions(
        &mut self,
        message: Arc<MessageEnvelope>,
    ) -> (u64, Vec<OverflowEvent>) {
        let RingState { ring, cursors, .. } = self;
        let appended = ring.append(message);
        if appended.evicted == 0 {
            return (appended.seq, Vec::new());
        }
        let mut overflow = Vec::new();
        for (subscriber, cursor) in cursors.iter_mut() {
            let dropped = cursor.charge_evicted(ring);
            if dropped > 0 {
                overflow.push(OverflowEvent {
                    subscriber: subscriber.clone(),
                    dropped,
                    app_slug: None,
                });
            }
        }
        (appended.seq, overflow)
    }
}

/// Fold one append's overflow into a batch's, so a subscriber that lost
/// messages to several appends is named once with the total.
fn merge_overflow(into: &mut Vec<OverflowEvent>, more: Vec<OverflowEvent>) {
    for event in more {
        match into.iter_mut().find(|e| e.subscriber == event.subscriber) {
            Some(existing) => existing.dropped += event.dropped,
            None => into.push(event),
        }
    }
}

/// The in-memory retention store for one non-durable channel.
#[derive(Debug)]
pub struct RingStore {
    /// Minted per store instance. A resume carrying a different epoch is a
    /// guaranteed gap, which is how restart loss becomes visible rather than
    /// silent.
    epoch: Uuid,
    channel_uuid: Uuid,
    /// Canonical `ephemeral:<name>` / `local:<name>` form.
    address: String,
    /// Resolved once from the address scheme at construction: non-durable
    /// either way, transportable only for `ephemeral:`.
    capabilities: ChannelCapabilities,
    /// Drops the noise ladder counted for each subscriber, written by the
    /// substrate's enactment point. The raw per-subscriber drop total lives on
    /// the cursor, which is where the drop is accounted.
    metered: DropTally,
    /// Live fan-out to consumers that read this channel as a stream. `None` on
    /// a confined channel: it never leaves the process, so no receiver is ever
    /// issued for it and none can be.
    live: Option<broadcast::Sender<Arc<RetainedMessage>>>,
    /// Held across a message's entry into retention and its fan-out, and across
    /// a live attach's `subscribe()` + replay.
    ///
    /// The state lock cannot cover the broadcast handle, and the attach
    /// boundary needs both under one critical section: with this gate a message
    /// is either entirely in an attaching consumer's replay or entirely on its
    /// receiver — never lost, never delivered twice.
    fan_out_gate: Mutex<()>,
    /// Messages that entered retention on this channel, appended or released.
    publishes: AtomicU64,
    state: Mutex<RingState>,
}

impl RingStore {
    /// A store for a channel retaining at most `retain_depth` messages.
    ///
    /// `retain_depth` also caps the deferred set: a channel may hold at most as
    /// much parked future as it holds retained past. `Depth::Unbounded` is
    /// rejected — an unbounded in-memory ring is a memory leak with a config
    /// knob in front of it.
    pub fn new(channel_uuid: Uuid, address: impl Into<String>, retain_depth: Depth) -> Self {
        Self::with_epoch(channel_uuid, address, retain_depth, Uuid::new_v4())
    }

    /// A store whose incarnation identity is supplied rather than minted.
    ///
    /// Every non-durable channel in a process dies together, so a host that
    /// stamps one boot epoch across all of them passes that epoch here and gets
    /// the same restart-visible gap on every channel at once.
    pub fn with_epoch(
        channel_uuid: Uuid,
        address: impl Into<String>,
        retain_depth: Depth,
        epoch: Uuid,
    ) -> Self {
        Self::with_fan_out_capacity(
            channel_uuid,
            address,
            retain_depth,
            epoch,
            RING_FAN_OUT_CAPACITY,
        )
    }

    /// The same store with a chosen live-fan-out ring size.
    ///
    /// The size is not an operator knob — production always takes
    /// [`RING_FAN_OUT_CAPACITY`]. It is settable so that a test of
    /// lag-and-drop behaviour can overrun a small ring instead of committing
    /// hundreds of messages to overrun the real one.
    pub fn with_fan_out_capacity(
        channel_uuid: Uuid,
        address: impl Into<String>,
        retain_depth: Depth,
        epoch: Uuid,
        fan_out_capacity: u32,
    ) -> Self {
        let address = address.into();
        // A ring-backed durable channel would be a mis-wiring that loses data on
        // restart, so it is rejected here rather than at the first read.
        let capabilities = match ChannelScheme::split(&address).map(|(scheme, _)| scheme) {
            Some(ChannelScheme::Ephemeral) => ChannelCapabilities::TRANSPORTABLE,
            Some(ChannelScheme::Local) => ChannelCapabilities::CONFINED,
            other => panic!(
                "messaging store: {address} is ring-backed but its scheme {other:?} is not a \
                 non-durable pub/sub scheme"
            ),
        };
        let depth = match retain_depth {
            Depth::Bounded(n) => n,
            Depth::Unbounded => panic!(
                "messaging store: channel {address} is non-durable and cannot retain an unbounded \
                 window; give it a bounded retain_depth"
            ),
        };
        assert!(
            depth <= MAX_RING_RETAIN_DEPTH,
            "messaging store: channel {address} retain_depth {depth} exceeds the sanity cap of \
             {MAX_RING_RETAIN_DEPTH}"
        );

        // The initial receiver is dropped: a commit with no live consumer
        // attached is contract-conformant (the message is retained for a later
        // attach), so `send` erroring on no-receivers is expected and ignored.
        let live = capabilities
            .transportable
            .then(|| broadcast::channel(fan_out_capacity as usize).0);

        Self {
            epoch,
            channel_uuid,
            address,
            capabilities,
            metered: DropTally::default(),
            live,
            fan_out_gate: Mutex::new(()),
            publishes: AtomicU64::new(0),
            state: Mutex::new(RingState {
                ring: RetainedRing::new(epoch, depth),
                deferred: DeferredSet::new(Some(depth)),
                cursors: HashMap::new(),
            }),
        }
    }

    pub fn epoch(&self) -> Uuid {
        self.epoch
    }

    pub fn channel_uuid(&self) -> Uuid {
        self.channel_uuid
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    /// The channel's capability set, resolved once at construction.
    pub fn capabilities(&self) -> ChannelCapabilities {
        self.capabilities
    }

    fn state(&self) -> std::sync::MutexGuard<'_, RingState> {
        self.state
            .lock()
            .unwrap_or_else(|_| panic!("messaging store: {} state lock poisoned", self.address))
    }

    fn fan_out_gate(&self) -> std::sync::MutexGuard<'_, ()> {
        self.fan_out_gate
            .lock()
            .unwrap_or_else(|_| panic!("messaging store: {} fan-out gate poisoned", self.address))
    }

    /// Count, log, and fan out a message that just entered retention. The
    /// caller holds the fan-out gate, so an attach cannot split the boundary.
    fn announce(&self, retained: &RetainedMessage) {
        self.publishes.fetch_add(1, Ordering::Relaxed);
        debug!(
            channel = %self.address,
            sender = %retained.envelope.sender,
            seq = retained.seq,
            message_id = %retained.envelope.message_id,
            "ring retention entry"
        );
        if let Some(tx) = &self.live {
            // `send` errs exactly when no receiver is attached, which is
            // contract-conformant: the message is retained, so a later attach
            // still sees it in its replay.
            let _ = tx.send(Arc::new(retained.clone()));
        }
    }

    /// Messages that have entered this channel's retention — appended
    /// immediately, or released from the deferred set. A parked message is not
    /// counted until it releases, which is when it becomes observable.
    pub fn publish_count(&self) -> u64 {
        self.publishes.load(Ordering::Relaxed)
    }

    /// Attach a live consumer: the retained window it is owed at `resume`, the
    /// decision explaining that window, and the stream of everything committed
    /// after the attach.
    ///
    /// # Panics
    ///
    /// If the channel is confined. A `local:` channel never crosses the process
    /// boundary, so the handle a serializer would need is never issued — asking
    /// for one is a wiring bug.
    pub fn subscribe_live(&self, resume: Option<Resume<Uuid>>) -> LiveAttach {
        let tx = self.live.as_ref().unwrap_or_else(|| {
            panic!(
                "messaging store: {} is confined and issues no live receiver",
                self.address
            )
        });
        let _gate = self.fan_out_gate();
        let receiver = tx.subscribe();
        let replay = self.replay(resume);
        LiveAttach {
            replay: replay
                .messages
                .into_iter()
                .map(|m| {
                    Arc::new(RetainedMessage {
                        seq: m.seq,
                        envelope: m.message,
                    })
                })
                .collect(),
            decision: replay.decision,
            receiver,
        }
    }

    // ── Retention ─────────────────────────────────────────────────────────

    /// Commit a message into retention, fan it out to attached live consumers,
    /// and return it with the sequence number the ring assigned plus the
    /// overflow the append caused.
    ///
    /// The returned `Arc` is the retained one, so a caller that reads the
    /// message back shares the store's allocation instead of copying it.
    ///
    /// Evicting the oldest retained entries retires the delivery obligations of
    /// every subscriber still owed them, and each such subscriber is charged
    /// here, by name. The caller enacts the noise ladder over those events; the
    /// store holds no policy about what they are worth.
    pub fn append(&self, envelope: MessageEnvelope) -> Appended {
        let envelope = Arc::new(envelope);
        let _gate = self.fan_out_gate();
        let (seq, overflow) = self
            .state()
            .append_charging_evictions(Arc::clone(&envelope));
        let retained = RetainedMessage { seq, envelope };
        self.announce(&retained);
        Appended { retained, overflow }
    }

    /// The most recent `n` retained messages, oldest first — the channel's
    /// ambience, independent of any subscriber's position.
    pub fn retained_tail(&self, n: u64) -> Vec<RetainedMessage> {
        self.state()
            .ring
            .tail(n)
            .map(|e| RetainedMessage {
                seq: e.seq,
                envelope: Arc::clone(&e.message),
            })
            .collect()
    }

    /// Number of retained messages.
    pub fn retained_len(&self) -> usize {
        self.state().ring.len()
    }

    /// The highest sequence number this epoch ever assigned, or 0 if none.
    pub fn newest_seq(&self) -> u64 {
        self.state().ring.newest_seq()
    }

    /// What a subscriber resuming at `resume` is owed, and whether its
    /// continuity broke. The whole retained window plus a typed gap when it did.
    pub fn replay(&self, resume: Option<Resume<Uuid>>) -> Replay<Arc<MessageEnvelope>> {
        self.state().ring.replay(resume)
    }

    // ── Subscribers ───────────────────────────────────────────────────────

    /// Register `subscriber`'s position on this channel, or retune an existing
    /// one's push depth.
    ///
    /// `priming` applies only when the queue comes into existence; a subscriber
    /// that is already attached keeps its position, because re-registering the
    /// same queue is not a new attach.
    pub fn attach(
        &self,
        subscriber: &ParticipantId,
        push_depth: u64,
        priming: Priming,
    ) -> Attached {
        let mut state = self.state();
        if let Some(cursor) = state.cursors.get_mut(subscriber) {
            cursor.set_push_depth(push_depth);
            return Attached::Existing;
        }
        let cursor = match priming {
            Priming::Retained => SubscriberCursor::primed(&state.ring, push_depth),
            Priming::Head => SubscriberCursor::at_head(&state.ring, push_depth),
        };
        state.cursors.insert(subscriber.clone(), cursor);
        Attached::Created
    }

    /// Drop a subscriber's position and drop tally. Its unread obligations go
    /// with them — the messages themselves stay retained for whoever else is
    /// owed them.
    pub fn detach(&self, subscriber: &ParticipantId) {
        self.metered.forget(subscriber);
        self.state().cursors.remove(subscriber);
    }

    pub fn is_attached(&self, subscriber: &ParticipantId) -> bool {
        self.state().cursors.contains_key(subscriber)
    }

    /// Every attached subscriber, in no particular order.
    pub fn attached(&self) -> Vec<ParticipantId> {
        self.state().cursors.keys().cloned().collect()
    }

    /// Every attached subscriber that currently has owed, deliverable messages,
    /// resolved under a single lock hold — the dispatcher's ring-wake source.
    /// Empty when the channel has no attached subscribers or none are owed work.
    pub fn deliverable_subscribers(&self) -> Vec<ParticipantId> {
        let state = self.state();
        state
            .cursors
            .iter()
            .filter(|(_, cursor)| cursor.has_deliverable(&state.ring))
            .map(|(subscriber, _)| subscriber.clone())
            .collect()
    }

    /// Whether this subscriber has retained messages it is owed and can still
    /// be delivered — the dispatcher's "is there work here" question.
    ///
    /// Panics for a subscriber that is not attached: asking about a queue that
    /// does not exist is a wiring bug, not a state to tolerate.
    pub fn has_deliverable(&self, subscriber: &ParticipantId) -> bool {
        let state = self.state();
        self.cursor(&state, subscriber).has_deliverable(&state.ring)
    }

    /// Take up to this subscriber's push depth of owed messages, oldest first,
    /// and report how many owed messages were retired undelivered since its
    /// previous take.
    ///
    /// The drop count is the noise ladder's input, and it is per subscriber
    /// precisely so an escalation names the party that fell behind.
    ///
    /// Panics for a subscriber that is not attached.
    pub fn take(&self, subscriber: &ParticipantId) -> Take<Arc<MessageEnvelope>> {
        let mut state = self.state();
        let RingState { ring, cursors, .. } = &mut *state;
        cursors
            .get_mut(subscriber)
            .unwrap_or_else(|| Self::unattached(&self.address, subscriber))
            .take(ring)
    }

    /// Lifetime drop count for one subscriber, including drops a take has not
    /// yet reported.
    ///
    /// Panics for a subscriber that is not attached.
    pub fn dropped_total(&self, subscriber: &ParticipantId) -> u64 {
        let state = self.state();
        self.cursor(&state, subscriber).dropped_total()
    }

    fn cursor<'a>(&self, state: &'a RingState, subscriber: &ParticipantId) -> &'a SubscriberCursor {
        state
            .cursors
            .get(subscriber)
            .unwrap_or_else(|| Self::unattached(&self.address, subscriber))
    }

    fn unattached(address: &str, subscriber: &ParticipantId) -> ! {
        panic!(
            "messaging store: {} has no queue for subscriber {}",
            address,
            subscriber.as_str()
        )
    }

    // ── Deferral ──────────────────────────────────────────────────────────

    /// Park a message until `release_at`.
    ///
    /// A parked message is in no subscriber's owed set, no replay, and no
    /// retained tail until it is released. Both store implementations must
    /// enforce this not-observable-before-release invariant.
    ///
    /// Rejects at the channel-wide cap rather than dropping the oldest parked
    /// message: silently cancelling scheduled work is worse than refusing to
    /// schedule more.
    pub fn park(
        &self,
        envelope: MessageEnvelope,
        release_at: DateTime<Utc>,
    ) -> Result<Uuid, QuotaExceeded> {
        let sender = envelope.sender.clone();
        let message_uuid = envelope.message_id;
        self.state()
            .deferred
            .park(sender, Arc::new(envelope), release_time_of(release_at))?;
        Ok(message_uuid)
    }

    /// When this channel's next unreleased message comes due, or `None` when
    /// nothing is parked. The release loop's deadline for this channel.
    ///
    /// An entry that is already due reports its own past release time rather
    /// than being skipped: a loop that computed its sleep from a fresher `now`
    /// than its release pass used would otherwise never hear about the entry
    /// that matured in between.
    pub fn next_release(&self) -> Option<DateTime<Utc>> {
        // The set is release-ordered, so the head is the earliest deadline.
        self.state().deferred.next_release().map(instant_of)
    }

    /// Release every message due at or before `now` into retention, in release
    /// order, fan each out to attached live consumers, and return them with the
    /// sequence numbers they were assigned plus the overflow the batch caused.
    ///
    /// Release and fan-out run under the same gate an append holds, so an
    /// attaching consumer sees each released message either entirely in its
    /// replay or entirely on its receiver.
    pub fn release_due(&self, now: DateTime<Utc>) -> ReleasedBatch {
        let _gate = self.fan_out_gate();
        let mut state = self.state();
        let due = state.deferred.release_due(release_time_of(now));
        let mut messages = Vec::with_capacity(due.len());
        let mut overflow: Vec<OverflowEvent> = Vec::new();
        for entry in due {
            let envelope = Arc::clone(&entry.message);
            let (seq, evicted) = state.append_charging_evictions(entry.message);
            messages.push(RetainedMessage { seq, envelope });
            merge_overflow(&mut overflow, evicted);
        }
        drop(state);
        for retained in &messages {
            self.announce(retained);
        }
        ReleasedBatch { messages, overflow }
    }

    /// One sender's messages still parked at `now`, ordered by release time.
    ///
    /// The sender filter is the whole authorization story: a caller scoped to a
    /// sender can never observe another sender's parked message, so an edit or
    /// cancel naming an id from this view needs no further identity check.
    pub fn deferred_for_sender(&self, sender: &str, now: DateTime<Utc>) -> Vec<DeferredMessage> {
        let cutoff = release_time_of(now);
        self.state()
            .deferred
            .for_sender(sender)
            .filter(|e| e.release_at > cutoff)
            .map(Self::view)
            .collect()
    }

    /// Every message still parked at `now`, release order. Operator-facing; not
    /// a per-sender view.
    pub fn deferred(&self, now: DateTime<Utc>) -> Vec<DeferredMessage> {
        let cutoff = release_time_of(now);
        self.state()
            .deferred
            .iter()
            .filter(|e| e.release_at > cutoff)
            .map(Self::view)
            .collect()
    }

    /// Number of unreleased messages held channel-wide — the deferred set's
    /// occupancy against its cap.
    ///
    /// A message whose release time has arrived is out of the sender views
    /// (there is nothing left to cancel or edit) but still occupies its slot
    /// until the release loop takes it, because it is still held in memory.
    /// That is what the cap bounds, so that is what this counts.
    pub fn deferred_len(&self) -> usize {
        self.state().deferred.len()
    }

    /// Cancel one of `sender`'s parked messages.
    ///
    /// `false` means the entry is no longer parked — it released between the
    /// view the caller acted on and this call. That race is inherent to
    /// scheduling, so it is a reportable no-op rather than a failure.
    ///
    /// Panics if the entry exists under a different sender: the caller obtained
    /// the id from a sender-scoped view, so reaching another sender's entry
    /// means the scoping was bypassed.
    pub fn cancel_deferred(&self, sender: &str, message_uuid: Uuid, now: DateTime<Utc>) -> bool {
        let mut state = self.state();
        let Some(id) = self.owned_id(&state, sender, message_uuid, now) else {
            return false;
        };
        state.deferred.cancel(id).is_some()
    }

    /// Replace one of `sender`'s parked messages' body, release time, or both.
    /// Same race semantics and same authorization rule as
    /// [`RingStore::cancel_deferred`].
    pub fn edit_deferred(
        &self,
        sender: &str,
        message_uuid: Uuid,
        body: Option<String>,
        release_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> bool {
        let mut state = self.state();
        let Some(id) = self.owned_id(&state, sender, message_uuid, now) else {
            return false;
        };
        let edited = body.map(|body| {
            let mut envelope = (*state.deferred.get(id).expect("checked above").message).clone();
            envelope.body = body;
            Arc::new(envelope)
        });
        let release_at = release_at.map(release_time_of);
        match state.deferred.edit(id, edited, release_at) {
            Ok(()) => true,
            Err(NoSuchDeferred(_)) => false,
        }
    }

    /// The parking slot holding `message_uuid`, if it is still parked at `now`
    /// and belongs to `sender`. Absent is the benign release race; present under
    /// another sender means a sender-scoped view was bypassed, and panics.
    ///
    /// The scan is bounded by the deferred cap, which is the channel's
    /// `retain_depth`.
    fn owned_id(
        &self,
        state: &RingState,
        sender: &str,
        message_uuid: Uuid,
        now: DateTime<Utc>,
    ) -> Option<DeferredId> {
        let cutoff = release_time_of(now);
        let entry = state
            .deferred
            .iter()
            .find(|e| e.message.message_id == message_uuid && e.release_at > cutoff)?;
        assert!(
            entry.sender == sender,
            "messaging store: {} message {message_uuid} belongs to {}, not {sender} — a \
             sender-scoped view was bypassed",
            self.address,
            entry.sender
        );
        Some(entry.id)
    }

    fn view(entry: &Deferred<Arc<MessageEnvelope>>) -> DeferredMessage {
        DeferredMessage {
            release_at: instant_of(entry.release_at),
            envelope: Arc::clone(&entry.message),
        }
    }

    /// Turn a publish-time record into the envelope the ring retains.
    ///
    /// The durable-only options are rejected for non-durable channels by the
    /// publish ladder, so seeing one here means the gate was bypassed.
    fn envelope_of(&self, msg: NewMessage) -> MessageEnvelope {
        assert!(
            msg.reply_to_uuid.is_none(),
            "messaging store: {} is non-durable and cannot carry reply_to",
            self.address
        );
        assert!(
            msg.delivery_deadline.is_none(),
            "messaging store: {} is non-durable and cannot carry delivery_deadline",
            self.address
        );
        MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: msg.source,
            channel: self.address.clone(),
            sender: msg.sender,
            publish_ts: ns_to_utc(msg.publish_ts_ns),
            body: msg.body,
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: msg.urgency,
            envelope_type: msg.envelope_type,
        }
    }
}

#[async_trait]
impl RetentionStore for RingStore {
    fn channel_uuid(&self) -> Uuid {
        self.channel_uuid
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.capabilities
    }

    async fn deliverable_subscribers(&self) -> Vec<ParticipantId> {
        RingStore::deliverable_subscribers(self)
    }

    /// The inherent `has_deliverable` panics for an unattached subscriber (its
    /// callers hold a live cursor and a missing one is a wiring bug); the trait
    /// contract is looser — an unknown subscriber is owed nothing — so this
    /// answers `false` rather than panicking when no cursor exists.
    ///
    /// Resolved under a single lock hold: a check-then-act over two `state()`
    /// acquisitions would let a concurrent `detach` remove the cursor between
    /// the attach check and the deliverable read, dropping into the inherent
    /// method's unattached panic.
    async fn has_deliverable(&self, subscriber: &ParticipantId) -> bool {
        let state = self.state();
        state
            .cursors
            .get(subscriber)
            .is_some_and(|cursor| cursor.has_deliverable(&state.ring))
    }

    /// The cursor is the ack: what this hands over, the subscriber has had, and
    /// the messages carry no record to settle later. `limit` retunes the
    /// cursor's push depth first, so the window the caller asked for is the
    /// window the cursor cuts — and the drops the cut charges are charged
    /// against that same depth.
    ///
    /// Panics for a subscriber with no cursor on this channel.
    async fn take_new(&self, subscriber: &ParticipantId, limit: u64) -> TakenWindow {
        let mut state = self.state();
        let RingState { ring, cursors, .. } = &mut *state;
        let cursor = cursors
            .get_mut(subscriber)
            .unwrap_or_else(|| Self::unattached(&self.address, subscriber));
        cursor.set_push_depth(limit);
        if limit == 0 {
            // A sampled subscriber takes nothing, and a zero-depth take would
            // charge everything it is owed as dropped.
            return TakenWindow {
                messages: vec![],
                dropped: 0,
                dropped_total: cursor.dropped_total(),
                clamped_leftover: 0,
            };
        }
        let take = cursor.take(ring);
        TakenWindow {
            messages: take
                .messages
                .into_iter()
                .map(|envelope| TakenMessage {
                    record: None,
                    envelope,
                })
                .collect(),
            dropped: take.dropped,
            dropped_total: cursor.dropped_total(),
            clamped_leftover: usize::from(cursor.has_deliverable(ring)),
        }
    }

    /// The cursor stays where it is: each message carries its ring position as
    /// its record, and only [`RingStore::settle`] moves the cursor past what
    /// the consumer accepted. `limit` retunes the cursor's push depth first,
    /// exactly as a take does, so the window read is the window a settle
    /// accounts against.
    ///
    /// Panics for a subscriber with no cursor on this channel.
    async fn peek_new(&self, subscriber: &ParticipantId, limit: u64) -> TakenWindow {
        let mut state = self.state();
        let RingState { ring, cursors, .. } = &mut *state;
        let cursor = cursors
            .get_mut(subscriber)
            .unwrap_or_else(|| Self::unattached(&self.address, subscriber));
        cursor.set_push_depth(limit);
        if limit == 0 {
            // A sampled subscriber is owed nothing it could be handed.
            return TakenWindow {
                messages: vec![],
                dropped: 0,
                dropped_total: cursor.dropped_total(),
                clamped_leftover: 0,
            };
        }
        let peeked = cursor.peek(ring);
        TakenWindow {
            messages: peeked
                .messages
                .into_iter()
                .map(|entry| TakenMessage {
                    record: Some(position_record(entry.seq)),
                    envelope: entry.message,
                })
                .collect(),
            dropped: 0,
            dropped_total: cursor.dropped_total(),
            clamped_leftover: usize::try_from(peeked.clamped)
                .expect("messaging store: clamped window out of range"),
        }
    }

    /// Moves the cursor to the newest settled position. Everything owed below
    /// it that the consumer passed over is charged there and then — accepting a
    /// clamped window is accepting that the older messages are gone — and that
    /// charge is what this returns. Records above the newest settled one stay
    /// owed, so a delivery that got partway through redelivers its remainder.
    ///
    /// Panics for a subscriber with no cursor on this channel.
    async fn settle(&self, subscriber: &ParticipantId, records: &[TargetRecord]) -> u64 {
        let Some(through) = records.iter().map(|r| r.0).max() else {
            return 0;
        };
        let through = u64::try_from(through).expect("messaging store: negative ring position");
        let delivered = u64::try_from(records.len()).expect("messaging store: settle batch size");
        let mut state = self.state();
        let RingState { ring, cursors, .. } = &mut *state;
        cursors
            .get_mut(subscriber)
            .unwrap_or_else(|| Self::unattached(&self.address, subscriber))
            .settle(ring, through, delivered)
    }

    /// The cursor's own count, which already covers both ring drop sources (the
    /// push-depth clamp and eviction of an owed message). Answers `0` for an
    /// unattached subscriber, where the inherent method panics.
    fn dropped_total(&self, subscriber: &ParticipantId) -> u64 {
        let state = self.state();
        state
            .cursors
            .get(subscriber)
            .map_or(0, |cursor| cursor.dropped_total())
    }

    fn record_metered_drops(&self, subscriber: &ParticipantId, count: u64) {
        self.metered.add(subscriber, count);
    }

    fn metered_drops(&self, subscriber: &ParticipantId) -> u64 {
        self.metered.get(subscriber)
    }

    /// Cursor-tracked: the store self-determines Created vs Existing from
    /// cursor presence; `app_slug` and `fresh_queue` are unused.
    async fn attach(
        &self,
        subscriber: &ParticipantId,
        _app_slug: &str,
        push_depth: u64,
        priming: Priming,
        _fresh_queue: bool,
    ) -> Attached {
        RingStore::attach(self, subscriber, push_depth, priming)
    }

    async fn detach(&self, subscriber: &ParticipantId) {
        RingStore::detach(self, subscriber);
    }

    /// Names no targets: subscribers on a ring-backed channel carry their own
    /// cursors, so the attached set already is the target set and nothing is
    /// recorded per subscriber. The bounded ring retires obligations as it
    /// commits, so this is where its overflow is charged and reported.
    async fn append(&self, msg: NewMessage) -> AppendOutcome {
        let envelope = self.envelope_of(msg);
        let message_uuid = envelope.message_id;
        let appended = RingStore::append(self, envelope);
        AppendOutcome {
            committed: Committed {
                message_uuid,
                seq: MessageSeq(appended.retained.seq),
                target_records: vec![],
            },
            overflow: appended.overflow,
        }
    }

    async fn park(
        &self,
        msg: NewMessage,
        release_at: DateTime<Utc>,
    ) -> Result<Parked, QuotaExceeded> {
        let envelope = self.envelope_of(msg);
        let message_uuid = RingStore::park(self, envelope, release_at)?;
        Ok(Parked { message_uuid })
    }

    async fn retained_tail(&self, limit: Depth) -> Vec<Arc<MessageEnvelope>> {
        let n = match limit {
            Depth::Bounded(n) => n,
            Depth::Unbounded => u64::MAX,
        };
        RingStore::retained_tail(self, n)
            .into_iter()
            .map(|m| m.envelope)
            .collect()
    }

    async fn replay_from(&self, cursor: Option<ResumeCursor>, limit: Depth) -> StoreReplay {
        let replay = self.replay(cursor);
        super::clamp_replay(
            StoreReplay {
                epoch: self.epoch,
                messages: replay.messages,
                decision: replay.decision,
            },
            limit,
        )
    }

    async fn next_release(&self) -> Option<DateTime<Utc>> {
        RingStore::next_release(self)
    }

    /// Names no targets: every attached cursor is owed the released message the
    /// moment it enters the ring, so the attached set is already the target set.
    async fn release_due(&self, now: DateTime<Utc>) -> ReleaseOutcome {
        let batch = RingStore::release_due(self, now);
        ReleaseOutcome {
            released: batch
                .messages
                .into_iter()
                .map(|m| Released {
                    seq: MessageSeq(m.seq),
                    envelope: m.envelope,
                    target_records: vec![],
                })
                .collect(),
            overflow: batch.overflow,
        }
    }

    async fn deferred_for_sender(&self, sender: &str, now: DateTime<Utc>) -> Vec<DeferredMessage> {
        RingStore::deferred_for_sender(self, sender, now)
    }

    async fn deferred_len(&self) -> u64 {
        u64::try_from(RingStore::deferred_len(self))
            .expect("messaging store: deferred set larger than u64")
    }

    async fn cancel_deferred(
        &self,
        sender: &str,
        message_uuid: Uuid,
        now: DateTime<Utc>,
    ) -> DeferralOutcome {
        match RingStore::cancel_deferred(self, sender, message_uuid, now) {
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
        match RingStore::edit_deferred(self, sender, message_uuid, body, release_at, now) {
            true => DeferralOutcome::Applied,
            false => DeferralOutcome::NotDeferred,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_envelope::{ChannelScheme, Urgency};

    fn store(retain_depth: u64) -> RingStore {
        RingStore::new(
            Uuid::new_v4(),
            "ephemeral:room",
            Depth::Bounded(retain_depth),
        )
    }

    fn envelope(sender: &str, body: &str) -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: "node".to_string(),
            channel: "ephemeral:room".to_string(),
            sender: sender.to_string(),
            publish_ts: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
            body: body.to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Ephemeral,
        }
    }

    fn at(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).unwrap()
    }

    /// The reading instant every deferral case uses: before every release time
    /// in this module, so nothing is due unless a case released it.
    fn now() -> DateTime<Utc> {
        at(0)
    }

    fn sub(slug: &str) -> ParticipantId {
        ParticipantId::for_wasm(slug)
    }

    fn bodies(take: &Take<Arc<MessageEnvelope>>) -> Vec<String> {
        take.messages.iter().map(|m| m.body.clone()).collect()
    }

    fn publish(store: &RingStore, sender: &str, bodies: &[&str]) {
        for body in bodies {
            store.append(envelope(sender, body));
        }
    }

    // ── Construction ──────────────────────────────────────────────────────

    /// A ring-backed durable channel would lose data on restart, so the mismatch
    /// is a boot panic rather than a latent one at the first capability read.
    #[test]
    #[should_panic(expected = "not a non-durable pub/sub scheme")]
    fn a_durable_scheme_is_rejected_at_construction() {
        RingStore::new(Uuid::new_v4(), "brenn:room", Depth::Bounded(4));
    }

    #[test]
    #[should_panic(expected = "cannot retain an unbounded window")]
    fn unbounded_retention_panics() {
        RingStore::new(Uuid::new_v4(), "ephemeral:room", Depth::Unbounded);
    }

    #[test]
    #[should_panic(expected = "exceeds the sanity cap")]
    fn retain_depth_over_sanity_cap_panics() {
        store(MAX_RING_RETAIN_DEPTH + 1);
    }

    #[test]
    fn retain_depth_at_sanity_cap_accepted() {
        assert_eq!(store(MAX_RING_RETAIN_DEPTH).retained_len(), 0);
    }

    #[test]
    fn distinct_stores_get_distinct_epochs() {
        assert_ne!(store(4).epoch(), store(4).epoch());
    }

    // ── Retention ─────────────────────────────────────────────────────────

    #[test]
    fn append_assigns_dense_seqs_from_one() {
        let s = store(8);
        assert_eq!(s.append(envelope("alice", "a")).retained.seq, 1);
        assert_eq!(s.append(envelope("alice", "b")).retained.seq, 2);
        assert_eq!(s.newest_seq(), 2);
    }

    /// Real OS-thread contention on one store: seq assignment and ring append
    /// are one critical section, so the union of every assigned seq is exactly
    /// `1..=N` — no duplicate (which makes a resume cursor ambiguous) and no
    /// skip (which every attached consumer reads as a gap).
    #[test]
    fn concurrent_appends_get_dense_unique_seqs() {
        const THREADS: u64 = 4;
        const PER_THREAD: u64 = 200;
        let s = Arc::new(store(8));

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let s = Arc::clone(&s);
                std::thread::spawn(move || {
                    (0..PER_THREAD)
                        .map(|_| s.append(envelope(&format!("pub{t}"), "x")).retained.seq)
                        .collect::<Vec<u64>>()
                })
            })
            .collect();

        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().expect("publisher thread"))
            .collect();
        all.sort_unstable();
        assert_eq!(all, (1..=THREADS * PER_THREAD).collect::<Vec<u64>>());
        assert_eq!(s.publish_count(), THREADS * PER_THREAD);
    }

    #[test]
    fn append_returns_the_retained_allocation() {
        let s = store(8);
        let appended = s.append(envelope("alice", "a"));
        let retained = s.retained_tail(1);
        assert!(Arc::ptr_eq(
            &appended.retained.envelope,
            &retained[0].envelope
        ));
    }

    #[test]
    fn with_epoch_stamps_the_supplied_incarnation() {
        let epoch = Uuid::new_v4();
        let s = RingStore::with_epoch(Uuid::new_v4(), "ephemeral:room", Depth::Bounded(4), epoch);
        assert_eq!(s.epoch(), epoch);
        s.append(envelope("alice", "a"));
        let replay = s.replay(Some(brenn_queue::Resume { epoch, seq: 1 }));
        assert_eq!(replay.decision, brenn_queue::ReplayDecision::UpToDate);
    }

    #[test]
    fn retention_is_bounded_drop_oldest() {
        let s = store(2);
        publish(&s, "alice", &["a", "b", "c"]);
        let tail: Vec<String> = s
            .retained_tail(10)
            .iter()
            .map(|m| m.envelope.body.clone())
            .collect();
        assert_eq!(tail, vec!["b", "c"]);
        assert_eq!(s.newest_seq(), 3);
    }

    // ── Live fan-out ──────────────────────────────────────────────────────

    fn confined_store(retain_depth: u64) -> RingStore {
        RingStore::new(Uuid::new_v4(), "local:room", Depth::Bounded(retain_depth))
    }

    /// Everything that enters retention reaches an attached live consumer, in
    /// retention order, whether it was appended directly or released from the
    /// deferred set.
    #[tokio::test]
    async fn a_live_consumer_receives_appends_and_releases_in_order() {
        let s = store(8);
        let mut receiver = s.subscribe_live(None).receiver;

        s.append(envelope("alice", "now"));
        s.park(envelope("alice", "later"), at(60_000))
            .expect("park");
        // Parked is not retained, so nothing is fanned out for it yet.
        s.release_due(at(30_000));
        s.release_due(at(90_000));

        let mut bodies = Vec::new();
        for _ in 0..2 {
            bodies.push(
                receiver
                    .recv()
                    .await
                    .expect("delivery")
                    .envelope
                    .body
                    .clone(),
            );
        }
        assert_eq!(bodies, vec!["now", "later"]);
    }

    /// The retained window and the live stream partition the channel: an attach
    /// hands over what is already retained and streams only what comes after.
    #[tokio::test]
    async fn an_attach_replays_the_window_and_streams_the_rest() {
        let s = store(8);
        publish(&s, "alice", &["a", "b"]);

        let attach = s.subscribe_live(None);
        let replayed: Vec<u64> = attach.replay.iter().map(|m| m.seq).collect();
        assert_eq!(replayed, vec![1, 2]);
        assert_eq!(attach.decision, ReplayDecision::Fresh);

        let mut receiver = attach.receiver;
        s.append(envelope("alice", "c"));
        assert_eq!(receiver.recv().await.expect("delivery").seq, 3);
    }

    /// A publisher racing an attach: every seq appears exactly once across
    /// (replay ∪ live). The fan-out gate covers both the subscribe and the
    /// replay snapshot, so a message committed at the boundary is neither lost
    /// between them nor delivered on both sides.
    #[tokio::test]
    async fn an_attach_racing_a_publisher_loses_and_duplicates_nothing() {
        const N: u64 = 200;
        // Retention and fan-out both exceed N, so nothing is evicted or lagged
        // out and every seq must be accounted for on one side or the other.
        let s = Arc::new(RingStore::with_fan_out_capacity(
            Uuid::new_v4(),
            "ephemeral:room",
            Depth::Bounded(256),
            Uuid::new_v4(),
            1024,
        ));

        let publisher = Arc::clone(&s);
        let handle = std::thread::spawn(move || {
            for _ in 0..N {
                publisher.append(envelope("alice", "x"));
                std::thread::yield_now();
            }
        });

        let attach = s.subscribe_live(None);
        let mut seen: Vec<u64> = attach.replay.iter().map(|m| m.seq).collect();
        let live_expected = N as usize - seen.len();
        let mut receiver = attach.receiver;
        for _ in 0..live_expected {
            seen.push(receiver.recv().await.expect("delivery").seq);
        }
        handle.join().expect("publisher thread");

        seen.sort_unstable();
        assert_eq!(seen, (1..=N).collect::<Vec<u64>>());
    }

    /// Fan-out is a fan-out: every attached consumer receives every message, in
    /// retention order, independently of the others.
    #[tokio::test]
    async fn every_live_consumer_receives_every_message_in_order() {
        const CONSUMERS: usize = 3;
        const MSGS: u64 = 10;
        let s = store(4);

        let receivers: Vec<_> = (0..CONSUMERS)
            .map(|_| s.subscribe_live(None).receiver)
            .collect();
        for i in 0..MSGS {
            s.append(envelope("alice", &format!("m{i}")));
        }

        for mut receiver in receivers {
            let mut seqs = Vec::new();
            for _ in 0..MSGS {
                seqs.push(receiver.recv().await.expect("delivery").seq);
            }
            assert_eq!(seqs, (1..=MSGS).collect::<Vec<u64>>());
        }
    }

    /// Every entry into retention is counted where it becomes observable: a
    /// parked message counts at release, not at park.
    #[test]
    fn publish_count_counts_retention_entries() {
        let s = store(8);
        assert_eq!(s.publish_count(), 0);
        s.append(envelope("alice", "a"));
        s.park(envelope("alice", "later"), at(60_000))
            .expect("park");
        assert_eq!(s.publish_count(), 1);
        s.release_due(at(90_000));
        assert_eq!(s.publish_count(), 2);
    }

    /// A confined channel never leaves the process, so the handle a serializer
    /// would need is never issued — asking for one is a wiring bug, not a
    /// tolerable no-op.
    #[test]
    #[should_panic(expected = "is confined and issues no live receiver")]
    fn a_confined_store_issues_no_live_receiver() {
        confined_store(4).subscribe_live(None);
    }

    /// A confined channel still retains and still serves its cursor consumers —
    /// only the live stream is absent.
    #[test]
    fn a_confined_store_retains_without_a_fan_out() {
        let s = confined_store(4);
        assert_eq!(s.append(envelope("alice", "a")).retained.seq, 1);
        assert_eq!(s.retained_tail(10).len(), 1);
        assert_eq!(s.publish_count(), 1);
    }

    // ── Subscribers ───────────────────────────────────────────────────────

    #[test]
    fn head_priming_owes_nothing_already_published() {
        let s = store(8);
        publish(&s, "alice", &["old"]);
        assert_eq!(s.attach(&sub("proc"), 4, Priming::Head), Attached::Created);
        assert!(bodies(&s.take(&sub("proc"))).is_empty());

        publish(&s, "alice", &["new"]);
        assert_eq!(bodies(&s.take(&sub("proc"))), vec!["new"]);
    }

    /// Attach is a delivery point: what was published before the queue existed
    /// is new to it, capped by the queue's push depth.
    #[test]
    fn retained_priming_delivers_the_tail_as_new() {
        let s = store(8);
        publish(&s, "alice", &["a", "b", "c"]);
        s.attach(&sub("proc"), 2, Priming::Retained);

        let take = s.take(&sub("proc"));
        assert_eq!(bodies(&take), vec!["b", "c"]);
        assert_eq!(take.dropped, 0);
    }

    #[test]
    fn reattach_keeps_position_and_retunes_depth() {
        let s = store(8);
        s.attach(&sub("proc"), 4, Priming::Head);
        publish(&s, "alice", &["a"]);
        assert_eq!(bodies(&s.take(&sub("proc"))), vec!["a"]);

        publish(&s, "alice", &["b", "c"]);
        assert_eq!(
            s.attach(&sub("proc"), 1, Priming::Retained),
            Attached::Existing
        );
        // Position carried over, so `b` is still owed — and the retuned depth of
        // 1 clamps the take to the newest, charging the older as a drop.
        let take = s.take(&sub("proc"));
        assert_eq!(bodies(&take), vec!["c"]);
        assert_eq!(take.dropped, 1);
    }

    #[test]
    fn overflow_is_charged_per_subscriber() {
        let s = store(8);
        s.attach(&sub("fast"), 8, Priming::Head);
        s.attach(&sub("slow"), 1, Priming::Head);
        publish(&s, "alice", &["a", "b", "c"]);

        let fast = s.take(&sub("fast"));
        assert_eq!(bodies(&fast), vec!["a", "b", "c"]);
        assert_eq!(fast.dropped, 0);

        let slow = s.take(&sub("slow"));
        assert_eq!(bodies(&slow), vec!["c"]);
        assert_eq!(slow.dropped, 2);
        assert_eq!(s.dropped_total(&sub("slow")), 2);
        assert_eq!(s.dropped_total(&sub("fast")), 0);
    }

    /// A subscriber whose owed messages the ring overwrites is charged by the
    /// append that overwrote them, and named in that append's overflow — so the
    /// loss is escalatable while the subscriber is still absent. The take that
    /// eventually arrives reports nothing new.
    #[test]
    fn eviction_of_owed_messages_is_charged_to_the_laggard_at_the_append() {
        let s = store(2);
        s.attach(&sub("proc"), 8, Priming::Head);
        publish(&s, "alice", &["a", "b"]);
        assert_eq!(s.dropped_total(&sub("proc")), 0, "nothing evicted yet");

        let evicting = s.append(envelope("alice", "c"));
        assert_eq!(
            evicting.overflow,
            vec![OverflowEvent {
                subscriber: sub("proc"),
                dropped: 1,
                app_slug: None,
            }]
        );
        s.append(envelope("alice", "d"));
        assert_eq!(s.dropped_total(&sub("proc")), 2);

        let take = s.take(&sub("proc"));
        assert_eq!(bodies(&take), vec!["c", "d"]);
        assert_eq!(take.dropped, 0, "both drops were already reported");
    }

    /// The overflow names only the subscribers that were actually owed the
    /// evicted messages: a caught-up subscriber loses nothing to an eviction.
    #[test]
    fn eviction_overflow_names_only_the_subscribers_that_lost_messages() {
        let s = store(2);
        s.attach(&sub("caught-up"), 8, Priming::Head);
        s.attach(&sub("absent"), 8, Priming::Head);
        publish(&s, "alice", &["a", "b"]);
        s.take(&sub("caught-up"));

        let evicting = s.append(envelope("alice", "c"));
        assert_eq!(
            evicting.overflow,
            vec![OverflowEvent {
                subscriber: sub("absent"),
                dropped: 1,
                app_slug: None,
            }]
        );
    }

    /// A released message enters retention like any other, so it can evict an
    /// absent subscriber's owed messages — and the batch reports that overflow
    /// once per subscriber, whatever the batch size.
    #[test]
    fn a_release_batch_reports_its_evictions_merged_per_subscriber() {
        let s = store(2);
        s.attach(&sub("absent"), 8, Priming::Head);
        s.attach(&sub("partial"), 8, Priming::Head);
        publish(&s, "alice", &["a", "b"]);
        // `partial` drains, then falls one message behind; `absent` never reads.
        // The two now lag by different amounts, so the batch must name each with
        // its own count rather than folding both into one entry.
        s.take(&sub("partial"));
        publish(&s, "alice", &["c"]);
        s.park(envelope("alice", "x"), at(1_000)).unwrap();
        s.park(envelope("alice", "y"), at(1_000)).unwrap();

        let batch = s.release_due(at(1_000));
        assert_eq!(batch.messages.len(), 2);
        let mut overflow = batch.overflow;
        overflow.sort_by(|a, b| a.subscriber.as_str().cmp(b.subscriber.as_str()));
        assert_eq!(
            overflow,
            vec![
                // Merged from both released appends.
                OverflowEvent {
                    subscriber: sub("absent"),
                    dropped: 2,
                    app_slug: None,
                },
                OverflowEvent {
                    subscriber: sub("partial"),
                    dropped: 1,
                    app_slug: None,
                },
            ]
        );
    }

    #[test]
    fn has_deliverable_tracks_owed_work() {
        let s = store(4);
        s.attach(&sub("proc"), 4, Priming::Head);
        assert!(!s.has_deliverable(&sub("proc")));
        publish(&s, "alice", &["a"]);
        assert!(s.has_deliverable(&sub("proc")));
        s.take(&sub("proc"));
        assert!(!s.has_deliverable(&sub("proc")));
    }

    #[test]
    fn deliverable_subscribers_lists_only_the_owed() {
        let s = store(4);
        s.attach(&sub("owed"), 4, Priming::Head);
        s.attach(&sub("caught-up"), 4, Priming::Head);
        // Nothing published yet: neither is owed.
        assert!(s.deliverable_subscribers().is_empty());
        publish(&s, "alice", &["a"]);
        // Both are owed the new message.
        let mut owed = s.deliverable_subscribers();
        owed.sort_by_key(|p| p.as_str().to_string());
        assert_eq!(owed, vec![sub("caught-up"), sub("owed")]);
        // One drains; only the other remains owed.
        s.take(&sub("caught-up"));
        assert_eq!(s.deliverable_subscribers(), vec![sub("owed")]);
    }

    #[test]
    fn detach_drops_the_queue_but_not_the_messages() {
        let s = store(4);
        s.attach(&sub("proc"), 4, Priming::Head);
        publish(&s, "alice", &["a"]);
        s.detach(&sub("proc"));
        assert!(!s.is_attached(&sub("proc")));
        assert_eq!(s.retained_len(), 1);
        assert!(s.attached().is_empty());
    }

    #[test]
    #[should_panic(expected = "has no queue for subscriber")]
    fn take_for_unattached_subscriber_panics() {
        store(4).take(&sub("proc"));
    }

    // ── Deferral ──────────────────────────────────────────────────────────

    #[test]
    fn parked_messages_are_not_observable_before_release() {
        let s = store(4);
        s.attach(&sub("proc"), 4, Priming::Head);
        s.park(envelope("alice", "later"), at(2_000)).unwrap();

        assert_eq!(s.retained_len(), 0);
        assert!(!s.has_deliverable(&sub("proc")));
        assert!(s.retained_tail(10).is_empty());
        assert_eq!(s.next_release(), Some(at(2_000)));

        assert!(s.release_due(at(1_999)).messages.is_empty());
        let released = s.release_due(at(2_000));
        assert_eq!(released.messages.len(), 1);
        assert_eq!(released.messages[0].seq, 1);
        assert_eq!(bodies(&s.take(&sub("proc"))), vec!["later"]);
        assert_eq!(s.next_release(), None);
    }

    #[test]
    fn releases_are_ordered_by_release_time() {
        let s = store(4);
        s.park(envelope("alice", "third"), at(3_000)).unwrap();
        s.park(envelope("alice", "first"), at(1_000)).unwrap();
        s.park(envelope("alice", "second"), at(2_000)).unwrap();

        let released: Vec<String> = s
            .release_due(at(9_000))
            .messages
            .iter()
            .map(|m| m.envelope.body.clone())
            .collect();
        assert_eq!(released, vec!["first", "second", "third"]);
    }

    /// The deferred cap is the channel's `retain_depth`, shared across senders:
    /// a channel may hold at most as much parked future as retained past.
    #[test]
    fn deferred_cap_is_channel_wide_and_shared() {
        let s = store(2);
        s.park(envelope("alice", "a"), at(1_000)).unwrap();
        s.park(envelope("bob", "b"), at(1_000)).unwrap();
        assert_eq!(
            s.park(envelope("alice", "c"), at(1_000)),
            Err(QuotaExceeded { cap: 2 })
        );
        assert_eq!(s.deferred_len(), 2);
    }

    #[test]
    fn zero_retain_depth_parks_nothing() {
        let s = store(0);
        assert_eq!(
            s.park(envelope("alice", "a"), at(1_000)),
            Err(QuotaExceeded { cap: 0 })
        );
    }

    #[test]
    fn deferred_view_is_sender_scoped_and_release_ordered() {
        let s = store(8);
        s.park(envelope("alice", "a-late"), at(3_000)).unwrap();
        s.park(envelope("bob", "b"), at(2_000)).unwrap();
        s.park(envelope("alice", "a-soon"), at(1_000)).unwrap();

        let alice: Vec<String> = s
            .deferred_for_sender("alice", now())
            .iter()
            .map(|d| d.envelope.body.clone())
            .collect();
        assert_eq!(alice, vec!["a-soon", "a-late"]);
        assert_eq!(s.deferred_for_sender("bob", now()).len(), 1);
        assert_eq!(s.deferred(now()).len(), 3);
    }

    #[test]
    fn cancel_removes_the_entry_and_reports_the_release_race() {
        let s = store(4);
        let id = s.park(envelope("alice", "a"), at(1_000)).unwrap();
        assert!(s.cancel_deferred("alice", id, now()));
        assert_eq!(s.deferred_len(), 0);
        // Second cancel names an entry that is gone: a reportable no-op.
        assert!(!s.cancel_deferred("alice", id, now()));
    }

    #[test]
    fn cancel_after_release_is_a_no_op() {
        let s = store(4);
        let id = s.park(envelope("alice", "a"), at(1_000)).unwrap();
        assert_eq!(s.release_due(at(1_000)).messages.len(), 1);
        assert!(!s.cancel_deferred("alice", id, now()));
    }

    #[test]
    fn edit_replaces_body_and_reschedules() {
        let s = store(4);
        let late = s.park(envelope("alice", "late"), at(3_000)).unwrap();
        s.park(envelope("alice", "soon"), at(1_000)).unwrap();

        assert!(s.edit_deferred("alice", late, Some("edited".into()), Some(at(500)), now()));
        let view = s.deferred_for_sender("alice", now());
        assert_eq!(view[0].envelope.body, "edited");
        assert_eq!(view[0].release_at, at(500));
        assert_eq!(view[1].envelope.body, "soon");
        assert_eq!(s.next_release(), Some(at(500)));
    }

    #[test]
    fn edit_after_release_is_a_no_op() {
        let s = store(4);
        let id = s.park(envelope("alice", "a"), at(1_000)).unwrap();
        s.release_due(at(1_000));
        assert!(!s.edit_deferred("alice", id, Some("edited".into()), None, now()));
    }

    /// A component may only reach the ids a sender-scoped view gave it, so an
    /// id belonging to someone else means that scoping was bypassed.
    #[test]
    #[should_panic(expected = "a sender-scoped view was bypassed")]
    fn touching_another_senders_deferred_message_panics() {
        let s = store(4);
        let id = s.park(envelope("bob", "b"), at(1_000)).unwrap();
        s.cancel_deferred("alice", id, now());
    }
}

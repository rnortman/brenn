//! `RingStore` — the in-memory retention store for non-durable channels.
//!
//! One store per `ephemeral:` or `local:` channel. The channel itself — a
//! bounded drop-oldest retained ring, one cursor per attached subscriber, and a
//! release-time-ordered set of parked messages, composed — is
//! [`brenn_queue::RingCore`], shared with every other host that owns
//! non-durable channels. This store is what the backend adds around it: one
//! lock, participant and application identity, envelope minting, and
//! `DateTime` conversion.
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
use tracing::debug;
use uuid::Uuid;

use brenn_envelope::{ChannelCapabilities, ChannelScheme, MessageEnvelope, Urgency};
use brenn_queue::{
    Advance, CursorOverflow, Deferred, DeferredId, OwnedDeferred, QuotaExceeded, Replay, Resume,
    RingCore,
};

/// One subscriber's activation view over this channel's retained ring.
pub type RingWindow = brenn_queue::Window<Arc<MessageEnvelope>>;

use crate::db::ns_to_utc;
use crate::store::{
    AdvanceOutcome, AppendOutcome, Attached, Committed, DeferralOutcome, DeferredMessage,
    DeliverableSubscriber, MessageSeq, NewMessage, OverflowEvent, Parked, ReleaseOutcome, Released,
    ResumeCursor, RetentionStore, StoreReplay, SubscriberWindow, depth_bound, instant_of,
    release_time_of,
};
use brenn_lib::messaging::ParticipantId;
use brenn_lib::messaging::config::Depth;

/// Maximum retained-ring depth accepted at construction.
///
/// A sanity cap against absurd config, not a memory budget. Upstream validation
/// may reject some values, but this store owns the allocation and must bound it
/// independently. Worst-case live retention at the cap is
/// `depth × max_body_bytes` per channel.
pub const MAX_RING_RETAIN_DEPTH: u64 = 4_096;

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

/// This channel, plus the identity the backend reports its positions under. One
/// lock covers both, so seq assignment, ring append, and cursor bookkeeping are
/// atomic with respect to every reader: a replay sees a message either entirely
/// before its resume position or entirely after it.
#[derive(Debug)]
struct RingState {
    core: RingCore<Arc<MessageEnvelope>, Uuid, ParticipantId>,
    /// The application each attached subscriber holds its position under, kept
    /// in lockstep with the core's cursors.
    ///
    /// The slug is identity, not policy: a conversation is named by its id, while
    /// the registration carrying its wake economics and noise rung is keyed by
    /// app, so naming the app is the only route from a position back to the
    /// registration that governs it.
    app_slugs: HashMap<ParticipantId, String>,
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
        let address = address.into();
        let capabilities = ChannelScheme::of(&address)
            .and_then(ChannelScheme::capabilities)
            .unwrap_or_else(|| {
                panic!(
                    "messaging store: {address} names no pub/sub channel, so it holds no retention"
                )
            });
        // A ring-backed durable channel would be a mis-wiring that loses data on
        // restart, so it is rejected here rather than at the first read.
        assert!(
            !capabilities.durable,
            "messaging store: {address} is ring-backed but its scheme is durable, so its \
             retention would not survive a restart"
        );
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

        Self {
            epoch,
            channel_uuid,
            address,
            capabilities,
            publishes: AtomicU64::new(0),
            state: Mutex::new(RingState {
                core: RingCore::new(epoch, depth),
                app_slugs: HashMap::new(),
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

    /// Count and log a message that just entered retention.
    fn announce(&self, retained: &RetainedMessage) {
        self.publishes.fetch_add(1, Ordering::Relaxed);
        debug!(
            channel = %self.address,
            sender = %retained.envelope.sender,
            seq = retained.seq,
            message_id = %retained.envelope.message_id,
            "ring retention entry"
        );
    }

    /// Messages that have entered this channel's retention — appended
    /// immediately, or released from the deferred set. A parked message is not
    /// counted until it releases, which is when it becomes observable.
    pub fn publish_count(&self) -> u64 {
        self.publishes.load(Ordering::Relaxed)
    }

    // ── Retention ─────────────────────────────────────────────────────────

    /// Commit a message into retention and return it with the sequence number
    /// the ring assigned plus the overflow the append caused.
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
        let mut state = self.state();
        let report = state.core.append(Arc::clone(&envelope));
        let overflow = Self::overflow_events(&state, report.overflow);
        drop(state);
        let retained = RetainedMessage {
            seq: report.seq,
            envelope,
        };
        self.announce(&retained);
        Appended { retained, overflow }
    }

    /// Name each charged position with the application it is held under: the
    /// noise rung a drop escalates on belongs to the registration, and the slug
    /// is the only route from a position back to it.
    fn overflow_events(
        state: &RingState,
        overflow: Vec<CursorOverflow<ParticipantId>>,
    ) -> Vec<OverflowEvent> {
        overflow
            .into_iter()
            .map(|event| OverflowEvent {
                app_slug: Some(Self::app_slug(state, &event.subscriber)),
                subscriber: event.subscriber,
                dropped: event.evicted,
            })
            .collect()
    }

    /// The application `subscriber` holds its position under. Recorded at every
    /// attach and dropped at every detach, so a position without one is a
    /// bookkeeping bug rather than a subscriber the store may report anonymously.
    fn app_slug(state: &RingState, subscriber: &ParticipantId) -> String {
        state.app_slugs.get(subscriber).cloned().unwrap_or_else(|| {
            panic!(
                "messaging store: subscriber {} holds a position under no application",
                subscriber.as_str()
            )
        })
    }

    /// The most recent `n` retained messages, oldest first — the channel's
    /// ambience, independent of any subscriber's position.
    pub fn retained_tail(&self, n: u64) -> Vec<RetainedMessage> {
        self.state()
            .core
            .ring()
            .tail(n)
            .map(|e| RetainedMessage {
                seq: e.seq,
                envelope: Arc::clone(&e.message),
            })
            .collect()
    }

    /// Number of retained messages.
    pub fn retained_len(&self) -> usize {
        self.state().core.ring().len()
    }

    /// The highest sequence number this epoch ever assigned, or 0 if none.
    pub fn newest_seq(&self) -> u64 {
        self.state().core.ring().newest_seq()
    }

    /// What a subscriber resuming at `resume` is owed, and whether its
    /// continuity broke. The whole retained window plus a typed gap when it did.
    pub fn replay(&self, resume: Option<Resume<Uuid>>) -> Replay<Arc<MessageEnvelope>> {
        self.state().core.ring().replay(resume)
    }

    // ── Subscribers ───────────────────────────────────────────────────────

    /// Register `subscriber`'s position on this channel, or retune an existing
    /// one's push depth.
    ///
    /// A queue coming into existence is primed behind the retained tail, capped
    /// by `push_depth`; a subscriber that is already attached keeps its
    /// position, because re-registering the same queue is not a new attach.
    /// `app_slug` is refreshed either way — the registration is its authority,
    /// and a re-attach is where a changed one arrives.
    ///
    /// A sampled (`push_depth = 0`) attach holds no position, so it records no
    /// application either: the slug exists to name a charged position, and a
    /// sampled subscriber has none to charge.
    pub fn attach(&self, subscriber: &ParticipantId, app_slug: &str, push_depth: u64) -> Attached {
        let mut state = self.state();
        let attached = state.core.attach(subscriber.clone(), push_depth);
        if state.core.is_attached(subscriber) {
            state
                .app_slugs
                .insert(subscriber.clone(), app_slug.to_string());
        } else {
            state.app_slugs.remove(subscriber);
        }
        attached
    }

    /// Drop a subscriber's position. Its unread obligations go with it — the
    /// messages themselves stay retained for whoever else is owed them.
    pub fn detach(&self, subscriber: &ParticipantId) {
        let mut state = self.state();
        state.core.detach(subscriber);
        state.app_slugs.remove(subscriber);
    }

    pub fn is_attached(&self, subscriber: &ParticipantId) -> bool {
        self.state().core.is_attached(subscriber)
    }

    /// Every attached subscriber, in no particular order.
    pub fn attached(&self) -> Vec<ParticipantId> {
        self.state().core.cursors().keys().cloned().collect()
    }

    /// Every attached subscriber that currently has owed, deliverable messages,
    /// resolved under a single lock hold — the dispatcher's ring-wake source.
    /// Empty when the channel has no attached subscribers or none are owed work.
    ///
    /// The unseen suffix is scanned for its loudest urgency and its earliest
    /// delivery deadline under that same hold, so the wake decision reads a
    /// suffix no append can have changed between the questions.
    ///
    /// "Loudest thing I have not seen" and "soonest deadline I have not passed"
    /// are both suffix questions, so the whole window's suffix folds are
    /// computed once and every cursor indexes into them — asking per cursor
    /// would re-walk the retained window per subscriber, lock held, on every
    /// dispatcher pass.
    pub fn deliverable_subscribers(&self) -> Vec<DeliverableSubscriber> {
        let state = self.state();
        let ring = state.core.ring();
        let retained: Vec<(u64, Urgency, Option<DateTime<Utc>>)> = ring
            .tail(u64::MAX)
            .map(|entry| {
                (
                    entry.seq,
                    entry.message.urgency,
                    entry.message.delivery_deadline,
                )
            })
            .collect();
        // `loudest_from[i]` is the loudest urgency among `retained[i..]`;
        // `earliest_from[i]` the earliest deadline any of them names.
        let mut loudest_from: Vec<Urgency> = Vec::with_capacity(retained.len());
        let mut earliest_from: Vec<Option<DateTime<Utc>>> = Vec::with_capacity(retained.len());
        for (_, urgency, deadline) in retained.iter().rev() {
            let running = loudest_from
                .last()
                .map_or(*urgency, |max| (*max).max(*urgency));
            loudest_from.push(running);
            let soonest = match (earliest_from.last().copied().flatten(), *deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            earliest_from.push(soonest);
        }
        loudest_from.reverse();
        earliest_from.reverse();
        state
            .core
            .cursors()
            .iter()
            .filter(|(_, cursor)| cursor.has_deliverable(ring))
            .map(|(subscriber, cursor)| {
                let first_unseen =
                    retained.partition_point(|(seq, _, _)| *seq < cursor.next_owed());
                DeliverableSubscriber {
                    subscriber: subscriber.clone(),
                    app_slug: Some(Self::app_slug(&state, subscriber)),
                    max_unseen_urgency: *loudest_from
                        .get(first_unseen)
                        .expect("ring: a deliverable cursor trails a retained message"),
                    earliest_unseen_deadline: earliest_from.get(first_unseen).copied().flatten(),
                }
            })
            .collect()
    }

    /// Whether this subscriber has retained messages it is owed and can still
    /// be delivered — the dispatcher's "is there work here" question.
    ///
    /// Panics for a subscriber that is not attached: asking about a queue that
    /// does not exist is a wiring bug, not a state to tolerate.
    pub fn has_deliverable(&self, subscriber: &ParticipantId) -> bool {
        let state = self.state();
        if !state.core.is_attached(subscriber) {
            Self::unattached(&self.address, subscriber);
        }
        state.core.has_deliverable(subscriber)
    }

    /// This subscriber's activation view: the most recent
    /// `max(push_limit, retain_limit)` retained messages, with the boundary
    /// where its unseen ones begin. `push_limit` retunes the cursor's stored
    /// depth first, so the window the caller asked for is the window it gets.
    ///
    /// Pure read: the cursor does not move and nothing is reported.
    ///
    /// A sampled (`push_limit = 0`) read holds no position: it is served the
    /// whole span as context, whether or not the subscriber has a cursor from
    /// some other port, and never retunes one — writing its zero into a cursor
    /// would leave a depth the model says the cursor cannot hold.
    ///
    /// `None` for a push-enabled read by a subscriber holding no cursor.
    pub fn window(
        &self,
        subscriber: &ParticipantId,
        push_limit: u64,
        retain_limit: u64,
    ) -> Option<RingWindow> {
        self.state()
            .core
            .window(subscriber, push_limit, retain_limit)
    }

    /// Move this subscriber's cursor to `through + 1` and report the unseen
    /// seqs no window ever served it — `seen_floor` being the oldest seq the
    /// window it is advancing over carried.
    ///
    /// `None` for a subscriber holding no cursor: nothing to move, nothing to
    /// report, and nothing mutated.
    pub fn advance(
        &self,
        subscriber: &ParticipantId,
        through: u64,
        seen_floor: u64,
    ) -> Option<Advance> {
        self.state().core.advance(subscriber, through, seen_floor)
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
            .core
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
        self.state().core.next_release().map(instant_of)
    }

    /// Release every message due at or before `now` into retention, in release
    /// order, and return them with the sequence numbers they were assigned plus
    /// the overflow the batch caused.
    pub fn release_due(&self, now: DateTime<Utc>) -> ReleasedBatch {
        let mut state = self.state();
        let report = state.core.release_due(release_time_of(now));
        let overflow = Self::overflow_events(&state, report.overflow);
        drop(state);
        let messages: Vec<RetainedMessage> = report
            .released
            .into_iter()
            .map(|released| RetainedMessage {
                seq: released.seq,
                envelope: released.message,
            })
            .collect();
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
        self.state()
            .core
            .deferred_for_sender(sender, release_time_of(now))
            .map(Self::view)
            .collect()
    }

    /// Every sender holding something parked at `now`, once each, sorted.
    ///
    /// The scan is bounded by the deferred cap, which is the channel's depth.
    pub fn deferred_senders(&self, now: DateTime<Utc>) -> Vec<String> {
        let mut senders: Vec<String> = self
            .state()
            .core
            .deferred_at(release_time_of(now))
            .map(|entry| entry.sender.clone())
            .collect();
        senders.sort_unstable();
        senders.dedup();
        senders
    }

    /// Every message still parked at `now`, release order. Operator-facing; not
    /// a per-sender view.
    pub fn deferred(&self, now: DateTime<Utc>) -> Vec<DeferredMessage> {
        self.state()
            .core
            .deferred_at(release_time_of(now))
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
        self.state().core.deferred_len()
    }

    /// Cancel one of `sender`'s parked messages.
    ///
    /// `NotDeferred` means the entry is no longer parked — it released between
    /// the view the caller acted on and this call. That race is inherent to
    /// scheduling, so it is a reportable no-op rather than a failure.
    /// `WrongSender` means the entry is parked under someone else; the caller
    /// judges that, since only the caller knows where the id came from.
    pub fn cancel_deferred(
        &self,
        sender: &str,
        message_uuid: Uuid,
        now: DateTime<Utc>,
    ) -> DeferralOutcome {
        let mut state = self.state();
        let id = match self.owned_id(&state, sender, message_uuid, now) {
            Ok(id) => id,
            Err(outcome) => return outcome,
        };
        match state.core.cancel_deferred(id).is_some() {
            true => DeferralOutcome::Applied,
            false => DeferralOutcome::NotDeferred,
        }
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
    ) -> DeferralOutcome {
        let mut state = self.state();
        let id = match self.owned_id(&state, sender, message_uuid, now) {
            Ok(id) => id,
            Err(outcome) => return outcome,
        };
        let edited = body.map(|body| {
            let mut envelope = (*state
                .core
                .deferred_entry(id)
                .expect("checked above")
                .message)
                .clone();
            envelope.body = body;
            Arc::new(envelope)
        });
        let release_at = release_at.map(release_time_of);
        match state.core.edit_deferred(id, edited, release_at).is_ok() {
            true => DeferralOutcome::Applied,
            false => DeferralOutcome::NotDeferred,
        }
    }

    /// The parking slot holding `message_uuid`, if it is still parked at `now`
    /// and belongs to `sender`. Otherwise the outcome the caller reports: the
    /// benign release race, or an entry parked under another sender.
    ///
    /// The scan is bounded by the deferred cap, which is the channel's
    /// `retain_depth`.
    fn owned_id(
        &self,
        state: &RingState,
        sender: &str,
        message_uuid: Uuid,
        now: DateTime<Utc>,
    ) -> Result<DeferredId, DeferralOutcome> {
        match state.core.owned_deferred(
            sender,
            |message| message.message_id == message_uuid,
            release_time_of(now),
        ) {
            OwnedDeferred::Owned(id, _) => Ok(id),
            OwnedDeferred::WrongSender { .. } => Err(DeferralOutcome::WrongSender),
            OwnedDeferred::NotFound => Err(DeferralOutcome::NotDeferred),
        }
    }

    fn view(entry: &Deferred<Arc<MessageEnvelope>>) -> DeferredMessage {
        DeferredMessage {
            release_at: instant_of(entry.release_at),
            envelope: Arc::clone(&entry.message),
        }
    }

    /// Turn a publish-time record into the envelope the ring retains.
    ///
    /// The reply address is retained verbatim: there is no join to resolve it
    /// through, so the consumer reads the address directly.
    fn envelope_of(&self, msg: NewMessage) -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: msg.source,
            channel: self.address.clone(),
            sender: msg.sender,
            publish_ts: ns_to_utc(msg.publish_ts_ns),
            body: msg.body,
            reply_to: msg.reply_to.map(|target| target.address),
            delivery_deadline: msg.delivery_deadline,
            deliver_after: None,
            impetus: msg.impetus,
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

    async fn deliverable_subscribers(&self) -> Vec<DeliverableSubscriber> {
        RingStore::deliverable_subscribers(self)
    }

    /// The inherent `has_deliverable` panics for an unattached subscriber (its
    /// callers hold a live cursor and a missing one is a wiring bug); the trait
    /// contract is looser — an unknown subscriber is owed nothing — so this
    /// takes the core's own answer, which is `false` when no cursor exists.
    async fn has_deliverable(&self, subscriber: &ParticipantId) -> bool {
        self.state().core.has_deliverable(subscriber)
    }

    /// The cursor's own view, repacked to the trait's shape. `push_limit`
    /// retunes the cursor's stored depth first, so the window the caller asked
    /// for is the window the cursor cuts.
    ///
    /// `None` for a push-enabled read by a subscriber with no cursor on this
    /// channel.
    async fn window(
        &self,
        subscriber: &ParticipantId,
        push_limit: Depth,
        retain_limit: Depth,
    ) -> Option<SubscriberWindow> {
        let window = RingStore::window(
            self,
            subscriber,
            depth_bound(push_limit),
            depth_bound(retain_limit),
        )?;
        Some(SubscriberWindow {
            new_from: window.new_from,
            push_enabled: window.push_enabled,
            entries: window
                .entries
                .into_iter()
                .map(|entry| (MessageSeq(entry.seq), entry.message))
                .collect(),
        })
    }

    /// `None` for a subscriber with no cursor on this channel.
    async fn advance(
        &self,
        subscriber: &ParticipantId,
        through: MessageSeq,
        seen_floor: MessageSeq,
    ) -> Option<AdvanceOutcome> {
        let advance = RingStore::advance(self, subscriber, through.0, seen_floor.0)?;
        Some(AdvanceOutcome {
            dropped: advance.dropped,
            noise_charge: advance.noise_charge,
        })
    }

    /// Cursor-tracked: the store self-determines Created vs Existing from
    /// cursor presence. `app_slug` is unused — a ring cursor is reported against
    /// under the registration the substrate resolves at the time.
    ///
    /// A sampled attach creates no cursor and removes any the subscriber held
    /// before the demotion: it is never delivered to, so a position kept for it
    /// would be one every eviction reports against and nothing can ever serve.
    ///
    /// The depth collapses to a count here: a ring cursor holds nothing past a
    /// restart, so an unbounded depth costs it nothing to spell as a bound no
    /// ring can reach.
    async fn attach(
        &self,
        subscriber: &ParticipantId,
        app_slug: &str,
        push_depth: Depth,
    ) -> Attached {
        RingStore::attach(self, subscriber, app_slug, depth_bound(push_depth))
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
                })
                .collect(),
            overflow: batch.overflow,
        }
    }

    async fn deferred_for_sender(&self, sender: &str, now: DateTime<Utc>) -> Vec<DeferredMessage> {
        RingStore::deferred_for_sender(self, sender, now)
    }

    async fn deferred_senders(&self, now: DateTime<Utc>) -> Vec<String> {
        RingStore::deferred_senders(self, now)
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
        RingStore::cancel_deferred(self, sender, message_uuid, now)
    }

    async fn edit_deferred(
        &self,
        sender: &str,
        message_uuid: Uuid,
        body: Option<String>,
        release_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> DeferralOutcome {
        RingStore::edit_deferred(self, sender, message_uuid, body, release_at, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_envelope::{ChannelScheme, Urgency};
    use brenn_queue::ReplayDecision;

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
            impetus: None,
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

    /// Just the identities from the owed walk, for rows that assert on who is
    /// owed rather than on what they are owed.
    fn owed_ids(s: &RingStore) -> Vec<ParticipantId> {
        s.deliverable_subscribers()
            .into_iter()
            .map(|owed| owed.subscriber)
            .collect()
    }

    /// Serve a subscriber its push window and advance over it, as a push
    /// consumer does: the bodies it was handed as new, and the drops the
    /// advance reported.
    fn serve(s: &RingStore, subscriber: &ParticipantId, push_limit: u64) -> (Vec<String>, u64) {
        let window = s
            .window(subscriber, push_limit, 0)
            .expect("the case attached this subscriber");
        let bodies = window
            .new_entries()
            .iter()
            .map(|e| e.message.body.clone())
            .collect();
        let dropped = match window.advance_span() {
            Some((through, seen_floor)) => {
                s.advance(subscriber, through, seen_floor)
                    .expect("the case attached this subscriber")
                    .noise_charge
            }
            None => 0,
        };
        (bodies, dropped)
    }

    /// The bodies of a subscriber's new window, for a case that does not care
    /// what the advance reported.
    fn bodies(s: &RingStore, subscriber: &ParticipantId, push_limit: u64) -> Vec<String> {
        serve(s, subscriber, push_limit).0
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
    #[should_panic(expected = "its scheme is durable")]
    fn a_durable_scheme_is_rejected_at_construction() {
        RingStore::new(Uuid::new_v4(), "brenn:room", Depth::Bounded(4));
    }

    /// An address that names no channel at all answers neither capability
    /// question, so it cannot be ring-backed either.
    #[test]
    #[should_panic(expected = "names no pub/sub channel")]
    fn a_scheme_that_names_no_channel_is_rejected_at_construction() {
        RingStore::new(Uuid::new_v4(), "pwa_push:device", Depth::Bounded(4));
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

    // ── Retention as the one delivery surface ─────────────────────────────

    fn confined_store(retain_depth: u64) -> RingStore {
        RingStore::new(Uuid::new_v4(), "local:room", Depth::Bounded(retain_depth))
    }

    /// Everything that enters retention is readable from retention, in
    /// retention order, whether it was appended directly or released from the
    /// deferred set. A parked message is in neither until it releases.
    #[test]
    fn appends_and_releases_enter_retention_in_order() {
        let s = store(8);

        s.append(envelope("alice", "now"));
        s.park(envelope("alice", "later"), at(60_000))
            .expect("park");
        s.release_due(at(30_000));
        assert_eq!(
            retained_bodies(&s.retained_tail(10)),
            vec!["now"],
            "a message not yet due is in retention for nobody"
        );

        s.release_due(at(90_000));
        assert_eq!(retained_bodies(&s.retained_tail(10)), vec!["now", "later"]);
        assert_eq!(
            s.retained_tail(10)
                .iter()
                .map(|m| m.seq)
                .collect::<Vec<_>>(),
            vec![1, 2],
            "the released message takes the next position, not the one it was parked at"
        );
    }

    /// A resume reads forward from a position and sees everything committed
    /// after it.
    #[test]
    fn a_resume_reads_everything_committed_after_its_position() {
        let s = store(8);
        publish(&s, "alice", &["a", "b"]);

        let fresh = s.replay(None);
        assert_eq!(fresh.decision, ReplayDecision::Fresh);
        assert_eq!(seqs(&fresh), vec![1, 2]);

        s.append(envelope("alice", "c"));
        let resumed = s.replay(Some(Resume {
            epoch: s.epoch(),
            seq: 2,
        }));
        assert_eq!(resumed.decision, ReplayDecision::Exact);
        assert_eq!(seqs(&resumed), vec![3]);
    }

    /// The bodies of a retained window, oldest first.
    fn retained_bodies(window: &[RetainedMessage]) -> Vec<String> {
        window.iter().map(|m| m.envelope.body.clone()).collect()
    }

    /// The sequence numbers a replay answered with, oldest first.
    fn seqs(replay: &Replay<Arc<MessageEnvelope>>) -> Vec<u64> {
        replay.messages.iter().map(|m| m.seq).collect()
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

    /// A confined channel retains and serves its cursor consumers exactly as a
    /// transportable one does; the only thing it lacks is a way off the process.
    #[test]
    fn a_confined_store_retains_like_any_other() {
        let s = confined_store(4);
        assert_eq!(s.append(envelope("alice", "a")).retained.seq, 1);
        assert_eq!(s.retained_tail(10).len(), 1);
        assert_eq!(s.publish_count(), 1);
    }

    // ── Subscribers ───────────────────────────────────────────────────────
    //
    // Priming, retuning and window arithmetic are `RingCore`'s and are pinned
    // in `brenn-queue`. What these cases pin is what the store adds: the
    // participant and application a position is charged under, and the
    // repacking of the core's answers.

    /// A subscriber whose unseen messages the ring overwrites is reported
    /// against by the append that overwrote them, and named in that append's
    /// overflow — so the loss is escalatable while the subscriber is still
    /// absent. The window that eventually arrives charges nothing more.
    #[test]
    fn eviction_of_unseen_messages_is_reported_at_the_append() {
        let s = store(2);
        s.attach(&sub("proc"), "proc", 8);
        for body in ["a", "b"] {
            assert!(
                s.append(envelope("alice", body)).overflow.is_empty(),
                "nothing evicted yet"
            );
        }

        let evicting = s.append(envelope("alice", "c"));
        assert_eq!(
            evicting.overflow,
            vec![OverflowEvent {
                subscriber: sub("proc"),
                dropped: 1,
                app_slug: Some("proc".to_string()),
            }]
        );
        // Each append reports only the span it evicted, so the two together
        // report the two lost messages exactly once each.
        assert_eq!(
            s.append(envelope("alice", "d")).overflow,
            vec![OverflowEvent {
                subscriber: sub("proc"),
                dropped: 1,
                app_slug: Some("proc".to_string()),
            }]
        );

        let (new, dropped) = serve(&s, &sub("proc"), 8);
        assert_eq!(new, vec!["c", "d"]);
        assert_eq!(dropped, 0, "both drops were already reported");
    }

    /// The overflow names only the subscribers that were actually owed the
    /// evicted messages: a caught-up subscriber loses nothing to an eviction.
    #[test]
    fn eviction_overflow_names_only_the_subscribers_that_lost_messages() {
        let s = store(2);
        s.attach(&sub("caught-up"), "caught-up", 8);
        s.attach(&sub("absent"), "absent", 8);
        publish(&s, "alice", &["a", "b"]);
        serve(&s, &sub("caught-up"), 8);

        let evicting = s.append(envelope("alice", "c"));
        assert_eq!(
            evicting.overflow,
            vec![OverflowEvent {
                subscriber: sub("absent"),
                dropped: 1,
                app_slug: Some("absent".to_string()),
            }]
        );
    }

    /// A released message enters retention like any other, so it can evict an
    /// absent subscriber's owed messages — and the batch reports that overflow
    /// once per subscriber, whatever the batch size.
    #[test]
    fn a_release_batch_reports_its_evictions_merged_per_subscriber() {
        let s = store(2);
        s.attach(&sub("absent"), "absent", 8);
        s.attach(&sub("partial"), "partial", 8);
        publish(&s, "alice", &["a", "b"]);
        // `partial` drains, then falls one message behind; `absent` never reads.
        // The two now lag by different amounts, so the batch must name each with
        // its own count rather than folding both into one entry.
        serve(&s, &sub("partial"), 8);
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
                    app_slug: Some("absent".to_string()),
                },
                OverflowEvent {
                    subscriber: sub("partial"),
                    dropped: 1,
                    app_slug: Some("partial".to_string()),
                },
            ]
        );
    }

    #[test]
    fn has_deliverable_tracks_owed_work() {
        let s = store(4);
        s.attach(&sub("proc"), "proc", 4);
        assert!(!s.has_deliverable(&sub("proc")));
        publish(&s, "alice", &["a"]);
        assert!(s.has_deliverable(&sub("proc")));
        serve(&s, &sub("proc"), 4);
        assert!(!s.has_deliverable(&sub("proc")));
    }

    #[test]
    fn deliverable_subscribers_lists_only_the_owed() {
        let s = store(4);
        s.attach(&sub("owed"), "owed", 4);
        s.attach(&sub("caught-up"), "caught-up", 4);
        // Nothing published yet: neither is owed.
        assert!(s.deliverable_subscribers().is_empty());
        publish(&s, "alice", &["a"]);
        // Both are owed the new message.
        let mut owed = owed_ids(&s);
        owed.sort_by_key(|p| p.as_str().to_string());
        assert_eq!(owed, vec![sub("caught-up"), sub("owed")]);
        // One drains; only the other remains owed.
        serve(&s, &sub("caught-up"), 4);
        assert_eq!(owed_ids(&s), vec![sub("owed")]);
    }

    #[test]
    fn detach_drops_the_queue_but_not_the_messages() {
        let s = store(4);
        s.attach(&sub("proc"), "proc", 4);
        publish(&s, "alice", &["a"]);
        s.detach(&sub("proc"));
        assert!(!s.is_attached(&sub("proc")));
        assert_eq!(s.retained_len(), 1);
        assert!(s.attached().is_empty());
    }

    /// A push-enabled read needs a cursor; without one the ring reports the
    /// absence rather than fabricating an empty window. The sampled read of the
    /// same subscriber is served regardless — it borrows no position.
    #[test]
    fn a_push_window_for_an_unattached_subscriber_reports_no_position() {
        let s = store(4);
        publish(&s, "alice", &["a"]);
        assert!(s.window(&sub("proc"), 4, 0).is_none());
        let sampled = s
            .window(&sub("proc"), 0, 4)
            .expect("a sampled window needs no position");
        assert_eq!(sampled.entries.len(), 1);
    }

    /// An advance for a subscriber whose cursor is gone moves nothing and
    /// reports the absence: the ring half of the departed-subscriber contract.
    #[test]
    fn an_advance_for_an_unattached_subscriber_reports_no_position() {
        let s = store(4);
        s.attach(&sub("proc"), "proc", 4);
        publish(&s, "alice", &["a"]);
        let window = s
            .window(&sub("proc"), 4, 0)
            .expect("the case attached this subscriber");
        let (through, seen_floor) = window.advance_span().expect("entries were served");
        s.detach(&sub("proc"));
        assert!(s.advance(&sub("proc"), through, seen_floor).is_none());
        assert!(!s.is_attached(&sub("proc")));
    }

    // ── Deferral ──────────────────────────────────────────────────────────

    #[test]
    fn parked_messages_are_not_observable_before_release() {
        let s = store(4);
        s.attach(&sub("proc"), "proc", 4);
        s.park(envelope("alice", "later"), at(2_000)).unwrap();

        assert_eq!(s.retained_len(), 0);
        assert!(!s.has_deliverable(&sub("proc")));
        assert!(s.retained_tail(10).is_empty());
        assert_eq!(s.next_release(), Some(at(2_000)));

        assert!(s.release_due(at(1_999)).messages.is_empty());
        let released = s.release_due(at(2_000));
        assert_eq!(released.messages.len(), 1);
        assert_eq!(released.messages[0].seq, 1);
        assert_eq!(bodies(&s, &sub("proc"), 4), vec!["later"]);
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
        assert_eq!(
            s.cancel_deferred("alice", id, now()),
            DeferralOutcome::Applied
        );
        assert_eq!(s.deferred_len(), 0);
        assert_eq!(
            s.cancel_deferred("alice", id, now()),
            DeferralOutcome::NotDeferred
        );
    }

    #[test]
    fn cancel_after_release_is_a_no_op() {
        let s = store(4);
        let id = s.park(envelope("alice", "a"), at(1_000)).unwrap();
        assert_eq!(s.release_due(at(1_000)).messages.len(), 1);
        assert_eq!(
            s.cancel_deferred("alice", id, now()),
            DeferralOutcome::NotDeferred
        );
    }

    #[test]
    fn edit_replaces_body_and_reschedules() {
        let s = store(4);
        let late = s.park(envelope("alice", "late"), at(3_000)).unwrap();
        s.park(envelope("alice", "soon"), at(1_000)).unwrap();

        assert_eq!(
            s.edit_deferred("alice", late, Some("edited".into()), Some(at(500)), now()),
            DeferralOutcome::Applied
        );
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
        assert_eq!(
            s.edit_deferred("alice", id, Some("edited".into()), None, now()),
            DeferralOutcome::NotDeferred
        );
    }

    /// An id parked under another sender is reported, not enacted and not
    /// panicked on: the store leaves the entry alone and lets the caller judge
    /// how an id it was never shown reached it.
    #[test]
    fn touching_another_senders_deferred_message_is_reported() {
        let s = store(4);
        let id = s.park(envelope("bob", "b"), at(1_000)).unwrap();
        assert_eq!(
            s.cancel_deferred("alice", id, now()),
            DeferralOutcome::WrongSender
        );
        assert_eq!(
            s.edit_deferred("alice", id, Some("edited".into()), None, now()),
            DeferralOutcome::WrongSender
        );
        assert_eq!(s.deferred_for_sender("bob", now())[0].envelope.body, "b");
        assert_eq!(s.deferred_len(), 1);
    }
}

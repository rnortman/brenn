//! Per-channel retention stores: where a channel's messages live between
//! publish and delivery.
//!
//! A channel's store holds its retained window, its per-subscriber positions,
//! and its parked (deferred) messages. Which store a channel gets is decided
//! once, at registration, from the channel's
//! [`ChannelCapabilities`](brenn_envelope::ChannelCapabilities): a durable
//! channel's data lives in the database, a non-durable channel's lives in
//! memory. The contract either one implements is identical — bounded
//! drop-oldest retention, replay with typed gaps, per-subscriber cursors and
//! their overflow accounting, and park/release/view/edit/cancel of deferred
//! messages.
//!
//! [`RingStore`] is the in-memory implementation, backed by the host-agnostic
//! mechanics in `brenn-queue`. It serves both non-durable schemes: `ephemeral:`
//! and `local:` channels differ only in transportability, which is a property
//! of the channel entry, not of where its messages sit.

pub mod db;
pub mod registry;
pub mod ring;
pub mod targets;

#[cfg(test)]
mod parity_tests;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use brenn_envelope::{ChannelCapabilities, ChannelScheme, MessageEnvelope, Urgency};
use brenn_queue::{GapReason, QuotaExceeded, ReplayDecision, Resume, Retained};

use crate::messaging::config::Depth;
use crate::messaging::{ParticipantId, SubscriberEntryKind, WakeMin};

pub use db::DbStore;
pub(crate) use db::{ClaimRetirement, PushRetireParams};
pub use registry::RingStores;
pub use ring::RingStore;
pub use targets::{PushTarget, TargetResolver, eager_wake_for};

/// A per-subscriber drop tally a store keeps in memory, keyed by subscriber id.
///
/// Both stores hold one for the drops the noise ladder metered, and the durable
/// store holds a second for its raw push-overflow accounting. In memory only:
/// a restart forgets the counts, which is why the guest-facing gap signal is
/// documented as within-lifetime.
#[derive(Default)]
pub(crate) struct DropTally(std::sync::Mutex<std::collections::HashMap<String, u64>>);

impl DropTally {
    /// Add `count` drops to `subscriber`'s tally. Zero is a no-op and creates
    /// no entry.
    pub(crate) fn add(&self, subscriber: &ParticipantId, count: u64) {
        if count == 0 {
            return;
        }
        let mut tally = self.0.lock().expect("drop tally poisoned");
        *tally.entry(subscriber.as_str().to_string()).or_insert(0) += count;
    }

    /// `subscriber`'s tally, `0` for a subscriber that never appeared in it.
    pub(crate) fn get(&self, subscriber: &ParticipantId) -> u64 {
        self.0
            .lock()
            .expect("drop tally poisoned")
            .get(subscriber.as_str())
            .copied()
            .unwrap_or(0)
    }

    /// Drop `subscriber`'s tally — used when its delivery state is torn down.
    pub(crate) fn forget(&self, subscriber: &ParticipantId) {
        self.0
            .lock()
            .expect("drop tally poisoned")
            .remove(subscriber.as_str());
    }
}

impl std::fmt::Debug for DropTally {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DropTally")
            .field(&self.0.lock().expect("drop tally poisoned").len())
            .finish()
    }
}

/// A message on its way into a channel, before the store has given it an
/// identity or a position.
#[derive(Debug, Clone)]
pub struct NewMessage {
    /// Node identity of the publishing process.
    pub source: String,
    pub sender: String,
    pub body: String,
    pub urgency: Urgency,
    /// The address scheme recorded on the envelope. Follows the channel except
    /// on ingress-bridged channels, where it names the originating transport.
    pub envelope_type: ChannelScheme,
    /// Durable-only; non-durable channels reject it in the publish ladder, so a
    /// non-durable store treats a populated value as a gate bug and panics.
    pub reply_to_uuid: Option<Uuid>,
    /// Durable-only, same rule as `reply_to_uuid`.
    pub delivery_deadline: Option<DateTime<Utc>>,
    pub publish_ts_ns: i64,
}

/// One subscriber a message is being committed for.
///
/// Resolved by the store that records a per-subscriber delivery row, at the
/// moment it commits. Stores whose subscribers carry their own cursor name no
/// targets at all — their attached set already is the target set.
#[derive(Debug, Clone)]
pub struct DeliveryTarget {
    pub subscriber: ParticipantId,
    pub app_slug: String,
    /// Resolved at publish time from the subscriber's wake economics and the
    /// message's urgency.
    pub eager_wake: bool,
    pub delivery_deadline: Option<DateTime<Utc>>,
    /// How many undelivered records this subscriber may hold on the channel.
    /// A record-issuing store retires the oldest beyond it as the commit lands,
    /// so the overflow is charged at the drop; a cursor-tracked store bounds the
    /// same quantity with the cursor and ignores this.
    pub push_depth: Depth,
}

/// One subscriber attached to the channel when a release pass runs.
///
/// Distinct from [`DeliveryTarget`] because a release batch carries several
/// messages of different urgencies: the wake decision cannot be resolved before
/// the store knows which message it is making, so the target carries the
/// threshold and the store applies it per message.
#[derive(Debug, Clone)]
pub struct ReleaseTarget {
    pub subscriber: ParticipantId,
    pub app_slug: String,
    /// `None` — wake on every released message. `Some(min)` — wake only for a
    /// message whose urgency meets the threshold.
    pub wake_min: Option<WakeMin>,
}

impl ReleaseTarget {
    /// Whether a released message of this urgency wakes the target.
    pub fn wakes(&self, urgency: Urgency) -> bool {
        self.wake_min.is_none_or(|min| min.wakes(urgency))
    }
}

/// A store-assigned position, ascending within the store's lifetime. Comparable
/// only against other sequence numbers from the same store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageSeq(pub u64);

/// A store's handle on one subscriber's copy of a message. Meaningful only to
/// the store that issued it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetRecord(pub i64);

/// What a store did with a committed message.
#[derive(Debug, Clone)]
pub struct Committed {
    pub message_uuid: Uuid,
    pub seq: MessageSeq,
    /// One per subscriber the store wrote a delivery record for, in the order it
    /// resolved them. Empty for stores that track subscriber positions by cursor
    /// rather than by record.
    pub target_records: Vec<TargetRecord>,
}

/// What a store did with a parked message.
///
/// No sequence number and no delivery records: a parked message holds no
/// position in retention until it releases, and it is owed to nobody until then
/// — the subscribers attached when the release pass runs are the ones it is
/// delivered to.
#[derive(Debug, Clone)]
pub struct Parked {
    pub message_uuid: Uuid,
}

/// Whether a subscriber's delivery state already existed when it attached.
///
/// Shared vocabulary across the stores: priming is a delivery point, so it
/// applies only when the queue comes into existence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attached {
    /// The queue came into existence on this attach. The caller's priming
    /// choice took effect, and a primed queue is owed the retained tail.
    Created,
    /// The subscriber was already attached; its position carried over and only
    /// its push depth was retuned.
    Existing,
}

/// Where a subscriber's delivery state starts when its queue comes into
/// existence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priming {
    /// Owed the channel's retained tail, capped by the subscriber's push depth
    /// — attach is a delivery point. The rule for component queues.
    Retained,
    /// Owed only what is published from now on. The rule for subscriber kinds
    /// that get their attach history by another route, or none at all.
    Head,
}

/// Where a subscriber of this kind starts when its queue comes into existence
/// — the one site that decides priming.
///
/// A component queue is primed because attach is a delivery point for it: a
/// message published before the component existed still reaches and still wakes
/// it. No other kind is primed, and each for its own reason: a surface
/// subscription's fresh-attach replay already delivers the retained tail, so
/// seeding it here would deliver that tail twice; a conversation or a system
/// subscriber reads channel ambience on demand and is not woken with old
/// messages presented as new.
pub fn priming_for_kind(kind: &SubscriberEntryKind) -> Priming {
    match kind {
        SubscriberEntryKind::Wasm(_) => Priming::Retained,
        SubscriberEntryKind::App(_)
        | SubscriberEntryKind::System(_)
        | SubscriberEntryKind::Surface { .. } => Priming::Head,
    }
}

/// One message handed to a subscriber by [`RetentionStore::take_new`].
#[derive(Debug, Clone)]
pub struct TakenMessage {
    /// The store's handle on this subscriber's copy, for the stores that issue
    /// one. `None` when the take itself settled the message — a cursor-tracked
    /// store advances past it and has nothing left to name.
    pub record: Option<TargetRecord>,
    pub envelope: Arc<MessageEnvelope>,
}

/// One subscriber's NEW window, as [`RetentionStore::take_new`] hands it over.
#[derive(Debug, Clone)]
pub struct TakenWindow {
    /// Owed messages, oldest first, capped by the caller's limit.
    pub messages: Vec<TakenMessage>,
    /// Owed messages this take charged as dropped — the per-take overflow the
    /// noise ladder reacts to. Nonzero only for a store whose take is also the
    /// point where overflow is accounted; a store that accounts its drops when
    /// they happen reports `0` here and carries them in
    /// [`RetentionStore::dropped_total`].
    pub dropped: u64,
    /// The subscriber's lifetime drop total, read with the take so the pair
    /// cannot disagree: the count reflects exactly the messages missing from
    /// the window handed over.
    pub dropped_total: u64,
    /// How much this take left behind because of the cap. Exact for a store
    /// that can count what it did not hand over; a presence flag (`0` or `1`)
    /// for one that can only say whether more remains.
    pub clamped_leftover: usize,
}

/// One subscriber's overflow, as the store that dropped the messages reports
/// it.
///
/// Stores count overflow; the substrate enacts the noise ladder over it. An
/// event names the party that fell behind and how many of its owed messages
/// went undelivered, and carries no policy: what the count is worth is the
/// caller's decision, made from the subscription's resolved noise level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverflowEvent {
    pub subscriber: ParticipantId,
    pub dropped: u64,
    /// The application the retired delivery record was written under, for a
    /// store that writes one. This is identity, not policy: an app's delivery
    /// participant is a conversation, while the registration carrying its noise
    /// rung is keyed by app, so naming the app is the only route from the event
    /// back to the registration. `None` from a cursor-tracked store, which
    /// records nothing per subscriber and whose subscribers are named by kind.
    pub app_slug: Option<String>,
}

/// What a store did with an appended message, plus the overflow that append
/// caused.
///
/// A commit retires the delivery obligations it displaces — a bounded ring
/// overwriting an unread position, a record-issuing store retiring a
/// subscriber's oldest claim to stay within its depth — and reports them here,
/// so a subscriber that never runs still has its losses escalated.
#[derive(Debug, Clone)]
pub struct AppendOutcome {
    pub committed: Committed,
    pub overflow: Vec<OverflowEvent>,
}

/// What a release pass moved into retention, plus the overflow that caused.
///
/// A released message enters retention exactly as an appended one does, so it
/// can push an already-lagging subscriber's owed messages out of the window
/// the same way; the batch's events are merged per subscriber.
#[derive(Debug, Clone)]
pub struct ReleaseOutcome {
    pub released: Vec<Released>,
    pub overflow: Vec<OverflowEvent>,
}

/// A message that has just crossed from parked to retained.
#[derive(Debug, Clone)]
pub struct Released {
    pub seq: MessageSeq,
    pub envelope: Arc<MessageEnvelope>,
    /// The delivery records that came due with it, for the stores that keep
    /// them. Empty for cursor-tracked stores.
    pub target_records: Vec<TargetRecord>,
}

/// One of a sender's parked messages, as the sender-scoped view shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredMessage {
    pub release_at: DateTime<Utc>,
    pub envelope: Arc<MessageEnvelope>,
}

impl DeferredMessage {
    pub fn message_uuid(&self) -> Uuid {
        self.envelope.message_id
    }
}

/// Result of an edit or cancel naming a message from a deferred view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferralOutcome {
    Applied,
    /// The message is no longer parked — it released between the view the
    /// caller acted on and this call. Inherent to scheduling, so it is a
    /// reportable no-op rather than a failure.
    NotDeferred,
}

/// A resume-style consumer's last-read position: the epoch that numbered its
/// seqs and the last dense sequence it saw.
///
/// One shape for both stores. A non-durable ring's epoch dies every boot; a
/// durable channel's `resume_epoch` is persisted on the channel row and dies
/// only with it. A cursor is meaningful only within the epoch that assigned its
/// seq — a foreign epoch answers `Gap(EpochChanged)` on either store, so there
/// is no "wrong store" condition left to detect: durability is derived from the
/// scheme in the address, and a channel cannot change class without becoming a
/// different channel.
pub type ResumeCursor = Resume<Uuid>;

/// One replayed message with the sequence number its store assigned it, so a
/// consumer can mint its next cursor from replay output alone.
///
/// The ring's own retained-entry shape, carried through to the trait boundary
/// rather than repacked: one concept, one type, as with [`ResumeCursor`].
pub type StoreRetained = Retained<Arc<MessageEnvelope>>;

/// The result of [`RetentionStore::replay_from`]: the owed retained-window
/// messages (oldest first, seq ascending), the typed decision that produced
/// them, and the epoch that numbered them.
///
/// A consumer mints its next cursor from this alone — `epoch` plus the last
/// message's `seq`, or the current cursor's seq when nothing was owed — never
/// from a side channel.
#[derive(Debug, Clone)]
pub struct StoreReplay {
    pub epoch: Uuid,
    pub messages: Vec<StoreRetained>,
    pub decision: ReplayDecision,
}

/// Narrow `replay` to `limit`, keeping the newest messages — the consumer's
/// window bound applied to a store that answered over a wider one.
///
/// Trimming the front of an `Exact` suffix loses messages the cursor was owed,
/// which is the same condition the retained window itself reports, so the
/// decision becomes `Gap(BeyondRetained)`. Every other decision keeps its
/// meaning: a `Fresh` consumer is owed nothing older than its own window, and a
/// gap stays the gap it already was.
///
/// For a store that can push the bound into its own read this is a fallback,
/// not the mechanism — it exists so a store whose window is already in memory
/// spends nothing to honor the same contract.
pub(crate) fn clamp_replay(mut replay: StoreReplay, limit: Depth) -> StoreReplay {
    let Depth::Bounded(n) = limit else {
        return replay;
    };
    let n = usize::try_from(n).expect("messaging: replay limit out of range");
    if replay.messages.len() <= n {
        return replay;
    }
    let excess = replay.messages.len() - n;
    replay.messages.drain(..excess);
    if let ReplayDecision::Exact = replay.decision {
        replay.decision = ReplayDecision::Gap(GapReason::BeyondRetained);
    }
    replay
}

/// Where a channel's messages live between publish and delivery.
///
/// One implementation per durability class, chosen once at channel
/// registration. The contract is identical either way: bounded drop-oldest
/// retention, parking of `deliver_after` messages that no read may observe
/// before release, and a sender-scoped view of those parked messages with edit
/// and cancel over it.
///
/// Authorization on the deferred surface is *structural*. A caller reaches a
/// parked message only by naming one the sender-scoped view handed it, so an
/// implementation that finds the named message under a different sender must
/// panic: the scoping was bypassed, which is a wiring bug, not a state to
/// tolerate.
///
/// Time enters as a parameter everywhere it matters: a store never reads an
/// ambient clock, so one caller's notion of `now` decides the deferred surface
/// it applies to — which messages a sender may still see, edit, or cancel, and
/// which ones release — and two calls in one pass cannot disagree about it. A
/// message is *parked* exactly while its release time is after the caller's
/// `now`.
///
/// Release itself is a state, not a comparison. Retention reads and the cap
/// take no clock at all: a message joins the retained window and frees its cap
/// slot when [`RetentionStore::release_due`] moves it, not when its instant
/// passes. Deciding those by clock instead would make a durable channel and a
/// ring-backed one answer differently in the window between a release time and
/// the loop that acts on it.
#[async_trait]
pub trait RetentionStore: Send + Sync + std::fmt::Debug {
    fn channel_uuid(&self) -> Uuid;

    /// Canonical scheme-prefixed address of the channel this store serves.
    fn address(&self) -> &str;

    fn capabilities(&self) -> ChannelCapabilities;

    /// Every subscriber this store currently owes deliverable work — the
    /// dispatcher's class-blind "where is there work" question. Empty when no
    /// subscriber is owed anything.
    ///
    /// "Owed, deliverable" means a retained message the subscriber has not yet
    /// taken and that has not been dropped from under it: a ring subscriber
    /// whose cursor trails the ring head, a durable subscriber with an
    /// undelivered, unparked push row. Parked (not-yet-released) messages are
    /// owed to nobody until they release.
    async fn deliverable_subscribers(&self) -> Vec<ParticipantId>;

    /// Whether `subscriber` is owed deliverable work on this channel.
    ///
    /// A subscriber this store has never seen is owed nothing and answers
    /// `false`; it is not an error to ask.
    async fn has_deliverable(&self, subscriber: &ParticipantId) -> bool;

    /// Hand `subscriber` the messages it is owed, oldest first, at most `limit`
    /// of them — the backend push consumer's read of its queue.
    ///
    /// `limit` is the subscriber's resolved push depth, and the caller is its
    /// single authority: a store holding a copy of the depth retunes it from
    /// this argument rather than deciding the window itself. `limit = 0` is a
    /// sampled subscriber — it takes nothing, and the call leaves no trace.
    ///
    /// The take advances the subscriber past the window it returns. Where that
    /// is the whole story — a cursor-tracked store — the take *is* the ack and
    /// the messages carry no record. A store that issues a per-subscriber
    /// delivery record instead returns the records with them and settles when
    /// the consumer says so, which is what keeps a durable message from being
    /// lost to a host that dies mid-activation.
    ///
    /// What happens to the excess beyond `limit` is the store's own overflow
    /// mechanic: held for the next take, or charged as a drop and gone. The
    /// window reports which, in `clamped_leftover` and `dropped`; the cap
    /// itself is the part every store owes the caller.
    ///
    /// A store that keeps an explicit per-subscriber queue panics when asked
    /// for a subscriber that has none: taking from a queue that was never
    /// created is a wiring bug, not an empty window. A store whose delivery
    /// state is per-message records has no queue to miss and answers with an
    /// empty window.
    async fn take_new(&self, subscriber: &ParticipantId, limit: u64) -> TakenWindow;

    /// Read the same window [`RetentionStore::take_new`] would hand over,
    /// without settling any of it.
    ///
    /// This is the read for a consumer that must prove delivery before its
    /// queue advances — one that may be asleep, or whose far end can refuse the
    /// message. It peeks, delivers, and settles exactly what was accepted, so a
    /// delivery attempt that fails costs nothing. A consumer whose read is its
    /// own acknowledgement takes instead.
    ///
    /// Every message carries the record naming this subscriber's copy of it,
    /// whatever the store's own bookkeeping is — a per-message delivery record,
    /// or a position — because the consumer settles by naming what it accepted
    /// and nothing else.
    ///
    /// `limit`, the sampled (`limit = 0`) rule, and the missing-queue rule are
    /// exactly [`RetentionStore::take_new`]'s. Nothing is charged: the overflow
    /// this window leaves behind is charged by the settle that accepts it, so a
    /// read that is never settled costs the subscriber nothing.
    async fn peek_new(&self, subscriber: &ParticipantId, limit: u64) -> TakenWindow;

    /// Settle records [`RetentionStore::peek_new`] handed over — the consumer's
    /// statement that these reached their destination.
    ///
    /// The subscriber advances past them: a cursor-tracked store moves to the
    /// newest settled position, charging as dropped every owed message the
    /// consumer passed over; a record-issuing store settles exactly the records
    /// named. Anything above the newest settled record stays owed, so a partial
    /// delivery redelivers its remainder rather than losing it. Settling nothing
    /// is a no-op — a delivery attempt that failed outright keeps every
    /// obligation.
    ///
    /// Returns the drops this settle charged, on the same footing as
    /// [`TakenWindow::dropped`]: the store counts, and the substrate decides
    /// what the count is worth on the noise ladder.
    ///
    /// The records must be ones this store issued for this subscriber. They are
    /// meaningless to any other store, and a caller reaches them only by having
    /// been handed them.
    async fn settle(&self, subscriber: &ParticipantId, records: &[TargetRecord]) -> u64;

    /// Lifetime count of messages this store dropped out from under
    /// `subscriber` before it could take them — the within-lifetime gap signal a
    /// consumer reads to know its stream is incomplete.
    ///
    /// This is accounting, not policy: the count rises on every drop whatever
    /// the subscription's noise level, which decides only how loudly the
    /// substrate reacts to one. Both stores hold it in memory only, so it starts
    /// at zero after a restart and a gap that predates the restart is not
    /// reflected in it.
    ///
    /// A subscriber this store has never seen has had nothing dropped: `0`, not
    /// an error.
    fn dropped_total(&self, subscriber: &ParticipantId) -> u64;

    /// Add `count` to `subscriber`'s metered drop tally.
    ///
    /// The noise ladder's counting rung, written only by the substrate's single
    /// enactment point. The store keeps the number next to the subscriber's
    /// other delivery state; deciding whether a drop is worth counting is the
    /// substrate's call, never the store's.
    fn record_metered_drops(&self, subscriber: &ParticipantId, count: u64);

    /// `subscriber`'s metered drop tally: the drops the noise ladder counted —
    /// all of them on a `metered` or `alarm` subscription, none on a `silent`
    /// one. Distinct from [`RetentionStore::dropped_total`], which counts every
    /// drop whatever the noise level.
    fn metered_drops(&self, subscriber: &ParticipantId) -> u64;

    /// Register `subscriber`'s delivery state on this channel, or retune an
    /// existing one's push depth. Priming seeds owed messages only when the
    /// queue comes into existence; a re-registration keeps its position and
    /// returns [`Attached::Existing`].
    ///
    /// `fresh_queue`: caller's assertion that this queue did not survive a
    /// restart. Implementations that track their own cursors may ignore it.
    ///
    /// `app_slug`: the subscriber's application name, used by stores that
    /// record per-target push rows.
    async fn attach(
        &self,
        subscriber: &ParticipantId,
        app_slug: &str,
        push_depth: u64,
        priming: Priming,
        fresh_queue: bool,
    ) -> Attached;

    /// Tear down `subscriber`'s delivery state on this channel — the inverse of
    /// [`RetentionStore::attach`]. Its position and unread obligations go; the
    /// retained messages stay for whoever else is owed them.
    ///
    /// Idempotent: detaching a subscriber the store never saw removes nothing
    /// and is not an error.
    async fn detach(&self, subscriber: &ParticipantId);

    /// Commit a message into retention, immediately deliverable, and report any
    /// delivery obligations the commit retired.
    ///
    /// The store names its own targets, as [`RetentionStore::release_due`] does:
    /// a cursor-tracked store's attached set is already the target set, and a
    /// record-issuing one resolves the channel's registrations as they stand
    /// when the commit runs — under the same connection it writes on, so a
    /// subscription cannot appear or vanish between the two.
    async fn append(&self, msg: NewMessage) -> AppendOutcome;

    /// Park a message until `release_at`, invisible to every read until then.
    ///
    /// Takes no targets: a parked message is owed to nobody, so nothing is
    /// resolved or recorded per subscriber until release, when the attached set
    /// is asked again.
    ///
    /// Rejects at the channel-wide cap rather than evicting the oldest parked
    /// message: silently cancelling scheduled work is worse than refusing to
    /// schedule more. Takes no clock: the cap counts unreleased messages, and
    /// whether `release_at` is far enough ahead to be worth parking at all is
    /// the publish ladder's decision, not the store's.
    async fn park(
        &self,
        msg: NewMessage,
        release_at: DateTime<Utc>,
    ) -> Result<Parked, QuotaExceeded>;

    /// The most recent retained messages, oldest first, capped by `limit` — the
    /// channel's ambience, independent of any subscriber's position.
    ///
    /// Parked messages are absent regardless of the clock: a message enters this
    /// window when a release pass moves it there, not when its release instant
    /// passes, so neither store needs a `now` to answer and both answer the
    /// same.
    async fn retained_tail(&self, limit: Depth) -> Vec<Arc<MessageEnvelope>>;

    /// Cursor-relative retained-window replay with a typed gap on overflow or an
    /// epoch change, over a persisted dense sequence — one piece of math, both
    /// stores.
    ///
    /// `None` resumes fresh: the whole retained window, no gap. A cursor bearing
    /// a foreign epoch gaps `EpochChanged`. A cursor at the highest sequence the
    /// channel ever assigned is up to date and owed nothing — including when the
    /// retained window is empty, because nothing newer was ever assigned. A
    /// trailing cursor is owed the messages after it, `Exact` when the retained
    /// window still reaches back to its position and `Gap(BeyondRetained)` when
    /// the window dropped older messages out from under it. A cursor ahead of the
    /// highest assigned sequence never happened within one epoch on an honest
    /// client and is reported as its own gap reason so a transport can escalate
    /// it.
    ///
    /// `limit` is the consumer's own window cap, narrowing the channel's
    /// retention bound for this answer: the store returns at most that many
    /// messages, keeping the newest. Truncating a cursor's owed suffix is a
    /// loss like any other, so an `Exact` that does not fit `limit` becomes
    /// `Gap(BeyondRetained)`.
    ///
    /// Serves resume-style consumers that own their cursor; backend push
    /// consumers advance by taking their window instead. Both stores answer over
    /// a dense per-channel sequence assigned in retention order, so a
    /// late-released message sorts newest on either — the ring by construction,
    /// the durable store by assigning its sequence at release.
    async fn replay_from(&self, cursor: Option<ResumeCursor>, limit: Depth) -> StoreReplay;

    /// When this channel's next unreleased message comes due, or `None` when
    /// nothing is parked.
    ///
    /// Takes no clock, and a deadline already in the past is reported like any
    /// other. A release loop asking "when next" must be told about work it has
    /// not yet done: filtering due entries out would hide an entry that matured
    /// between the loop's release pass and its next sleep from both calls, and
    /// on a single-publisher timer channel nothing else ever comes along to
    /// wake it.
    async fn next_release(&self) -> Option<DateTime<Utc>>;

    /// Move every message due at or before `now` into retention, in release
    /// order, delivering it to the subscribers attached now. Every due message
    /// is reported, including one released to no subscriber at all, along with
    /// any overflow the batch caused.
    ///
    /// The store names its own targets: a cursor-tracked store's attached set is
    /// already the target set, and a record-issuing one resolves the channel's
    /// registrations as they stand when the release runs. Targeting therefore
    /// happens here rather than at park time, so a subscriber that attached
    /// while the message was parked receives it and one that left does not.
    async fn release_due(&self, now: DateTime<Utc>) -> ReleaseOutcome;

    /// One sender's parked messages on this channel, soonest release first.
    async fn deferred_for_sender(&self, sender: &str, now: DateTime<Utc>) -> Vec<DeferredMessage>;

    /// How many unreleased messages this channel holds, channel-wide — the
    /// quantity the deferred cap bounds, and the one [`RetentionStore::park`]
    /// admits against.
    ///
    /// Clock-free, and deliberately not the size of the sender views: a message
    /// that has come due but that no release pass has taken yet is out of the
    /// deferred *view* (it can no longer be cancelled or edited) while still
    /// holding its cap slot, because the resources the cap bounds are still
    /// held. Both stores count it the same way, which is what keeps a park
    /// admitted on one class from being refused on the other.
    async fn deferred_len(&self) -> u64;

    async fn cancel_deferred(
        &self,
        sender: &str,
        message_uuid: Uuid,
        now: DateTime<Utc>,
    ) -> DeferralOutcome;

    /// Replace a parked message's body, its release time, or both. `None`
    /// leaves that field alone.
    async fn edit_deferred(
        &self,
        sender: &str,
        message_uuid: Uuid,
        body: Option<String>,
        release_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> DeferralOutcome;
}

/// Convert a wall-clock instant to the epoch-millisecond form `brenn-queue`
/// works in.
///
/// Panics before 1970. A release time that far outside the representable range
/// is a corrupt value, not a schedule anyone meant.
pub fn release_time_of(ts: DateTime<Utc>) -> u64 {
    u64::try_from(ts.timestamp_millis())
        .unwrap_or_else(|_| panic!("messaging store: release time precedes the Unix epoch: {ts}"))
}

/// Inverse of [`release_time_of`].
pub fn instant_of(release_time: u64) -> DateTime<Utc> {
    let millis = i64::try_from(release_time).expect("messaging store: release time out of range");
    DateTime::from_timestamp_millis(millis)
        .unwrap_or_else(|| panic!("messaging store: release time out of range: {release_time}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_time_round_trips() {
        let ts = DateTime::from_timestamp_millis(1_700_000_000_123).unwrap();
        assert_eq!(release_time_of(ts), 1_700_000_000_123);
        assert_eq!(instant_of(1_700_000_000_123), ts);
    }

    #[test]
    #[should_panic(expected = "precedes the Unix epoch")]
    fn pre_epoch_release_time_panics() {
        release_time_of(DateTime::from_timestamp_millis(-1).unwrap());
    }
}

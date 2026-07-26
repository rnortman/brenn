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
use crate::messaging::{ParticipantId, SubscriberEntryKind};

pub use db::DbStore;
pub use registry::RingStores;
pub use ring::RingStore;
pub use targets::{SurfaceFeedTarget, TargetResolver};

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

/// A store-assigned position, ascending within the store's lifetime. Comparable
/// only against other sequence numbers from the same store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageSeq(pub u64);

/// What a store did with a committed message.
#[derive(Debug, Clone)]
pub struct Committed {
    pub message_uuid: Uuid,
    pub seq: MessageSeq,
}

/// What a store did with a parked message.
///
/// No sequence number: a parked message holds no position in retention until it
/// releases, and nothing can read it before then.
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

/// One subscriber's activation view: the most recent
/// `max(push_limit, retain_limit)` retained messages, with the boundary where
/// its unseen ones begin.
///
/// Reading one moves nothing and charges nothing. Everything below `new_from`
/// is context — messages the subscriber has already seen, or unseen ones the
/// push limit did not promote to new. Unseen messages served as context were
/// delivered, so passing them charges nothing; only seqs below the whole
/// window were never visible, and the advance that passes them reports those.
#[derive(Debug, Clone)]
pub struct SubscriberWindow {
    /// Oldest first, each entry carrying the seq its store assigned it.
    pub entries: Vec<(MessageSeq, Arc<MessageEnvelope>)>,
    /// Index of the first new entry; equal to `entries.len()` when nothing in
    /// the window is new.
    pub new_from: usize,
    /// Whether a position may move over this window. False for a sampled
    /// subscriber (`push_limit = 0`), which holds no position, and false for a
    /// window withheld rather than read — a channel the delivery-time ACL gate
    /// denies. Either way nothing advances over it.
    pub push_enabled: bool,
}

impl SubscriberWindow {
    /// A window that served nothing and that nothing advances over: the shape
    /// a caller stands in for a port it did not read, such as one the
    /// delivery-time ACL gate denied.
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
            new_from: 0,
            push_enabled: false,
        }
    }

    /// The new entries, oldest first.
    pub fn new_entries(&self) -> &[(MessageSeq, Arc<MessageEnvelope>)] {
        &self.entries[self.new_from..]
    }

    /// The `(through, seen_floor)` pair an advance over this window is made
    /// with, or `None` when there is nothing to advance: a window that served
    /// nothing, or a sampled subscriber, which holds no position to move.
    pub fn advance_span(&self) -> Option<(MessageSeq, MessageSeq)> {
        if !self.push_enabled {
            return None;
        }
        Some((self.entries.last()?.0, self.entries.first()?.0))
    }
}

/// What an advance passed over without ever having served it.
///
/// Both figures are subtractions between sequence numbers, computed at the
/// advance and stored nowhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AdvanceOutcome {
    /// Unseen seqs the advance stepped past that no window served — the
    /// subscriber's visible loss since its previous advance, and the figure a
    /// guest reads as `dropped`.
    pub dropped: u64,
    /// The portion of `dropped` no eviction pass has already reported. The
    /// caller routes this into the noise ladder; eviction passes report their
    /// own, so no span is enacted twice.
    pub noise_charge: u64,
}

/// One subscriber a store currently owes work, with what the wake decision
/// needs to be made about it.
///
/// The wake pass reads this and nothing else: who is behind, under what
/// application, and how loud the loudest thing they have not seen is. Nothing
/// here is stored per message — a store answers it from the position and its
/// own retention at the moment it is asked, which is why a registration change
/// takes effect at the next pass rather than at the next publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverableSubscriber {
    pub subscriber: ParticipantId,
    /// The application this subscriber holds its position under — a
    /// conversation's registration is keyed by app. `None` only where the
    /// reporter never held one.
    pub app_slug: Option<String>,
    /// The loudest urgency among the messages this subscriber has not seen and
    /// retention still holds. Never `None` — a subscriber with nothing unseen is
    /// not deliverable and does not appear here at all.
    pub max_unseen_urgency: Urgency,
    /// The earliest `delivery_deadline` among those same unseen messages, or
    /// `None` when none of them carries one.
    ///
    /// A deadline says "wake for this by T even if urgency alone would not", so
    /// the wake pass needs it from the same snapshot as the urgency. It leaves
    /// the set the moment the position passes the message carrying it, which is
    /// what stops a served deadline from waking anyone again.
    pub earliest_unseen_deadline: Option<DateTime<Utc>>,
}

/// The bound `limit` places on a window read, with `Unbounded` spelled as the
/// largest count a store can hold.
pub(crate) fn depth_bound(limit: Depth) -> u64 {
    match limit {
        Depth::Bounded(n) => n,
        Depth::Unbounded => u64::MAX,
    }
}

/// Cut the window shape out of a retained span: the newest
/// `min(unseen, push_limit)` entries are new, the rest is context.
///
/// `entries` must already be the most recent `max(push_limit, retain_limit)`
/// retained messages, oldest first — the part a store answers from its own
/// retention. The boundary itself comes from [`brenn_queue::new_boundary`], the
/// one authority for the rule, so both classes cut it identically.
pub(crate) fn compose_window(
    entries: Vec<(MessageSeq, Arc<MessageEnvelope>)>,
    next_owed: MessageSeq,
    push_limit: Depth,
) -> SubscriberWindow {
    SubscriberWindow {
        new_from: brenn_queue::new_boundary(
            entries.iter().map(|(seq, _)| seq.0),
            next_owed.0,
            depth_bound(push_limit),
        ),
        entries,
        push_enabled: push_limit.is_push_enabled(),
    }
}

/// One subscriber's overflow, as the store that dropped the messages reports
/// it.
///
/// Stores report overflow; the substrate enacts the noise ladder over it. An
/// event names the party that fell behind and how many of its owed messages
/// went undelivered, and carries no policy: what the count is worth is the
/// caller's decision, made from the subscription's resolved noise level. The
/// figure is computed where the loss happened, never accumulated — no store
/// holds a drop counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverflowEvent {
    pub subscriber: ParticipantId,
    pub dropped: u64,
    /// The application the losing subscriber holds its state under, where the
    /// reporter knows it — from the retired delivery record, or from the
    /// cursor's cached slug. This is identity, not policy: an app's delivery
    /// participant is a conversation, while the registration carrying its noise
    /// rung is keyed by app, so naming the app is the only route from the event
    /// back to the registration. `None` only where the reporter never held one.
    pub app_slug: Option<String>,
}

/// What a store did with an appended message, plus the overflow that append
/// caused.
///
/// A commit charges nobody for falling behind, but entering retention can push
/// an unread message out of the window: a bounded ring overwriting an unread
/// position reports that eviction here, so a subscriber that never runs still
/// has its losses escalated.
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
    /// "Owed, deliverable" means a retained message the subscriber's position
    /// trails and that retention still holds. Parked (not-yet-released)
    /// messages are owed to nobody until they release, and a sampled subscriber
    /// holds no position and is owed nothing.
    ///
    /// Each entry carries the loudest urgency in that subscriber's unseen
    /// suffix, because the wake decision is made here rather than stored per
    /// message: an urgency-gated subscriber wakes only when something it is owed
    /// clears its threshold, and the loudest unseen message is what decides it.
    async fn deliverable_subscribers(&self) -> Vec<DeliverableSubscriber>;

    /// Whether `subscriber` is owed deliverable work on this channel: anything
    /// unseen that retention still holds.
    ///
    /// The activation trigger gate, answered from positions alone so a consumer
    /// that turns out to be owed nothing never pays for a window read. It is
    /// conservative in the direction that matters: `false` means no window of
    /// this channel can present the subscriber anything new.
    ///
    /// A subscriber this store has never seen is owed nothing and answers
    /// `false`; it is not an error to ask.
    async fn has_deliverable(&self, subscriber: &ParticipantId) -> bool;

    /// `subscriber`'s activation view: the most recent
    /// `max(push_limit, retain_limit)` retained messages, with the new/context
    /// boundary decided from the subscriber's position.
    ///
    /// New is the *newest* `min(unseen, push_limit)` of them — the same
    /// drop-oldest rule retention itself follows, so a consumer woken late acts
    /// on the freshest messages. Unseen entries below that boundary are served
    /// as context and are not lost; unseen seqs below the whole window were
    /// never visible, and the advance that passes them reports them.
    ///
    /// `push_limit` is the subscriber's resolved push depth and the caller is
    /// its single authority: a store holding a copy of the depth retunes it
    /// from this argument rather than deciding the window itself. A sampled
    /// (`push_limit = 0`) subscriber is never delivered to, so nothing in its
    /// window is ever new — it still sees `retain_limit` of context.
    ///
    /// Pure read: no position moves and nothing is charged. A store that keeps
    /// an explicit per-subscriber position panics when asked for a subscriber
    /// that has none: reading a queue that was never created is a wiring bug,
    /// not an empty window.
    async fn window(
        &self,
        subscriber: &ParticipantId,
        push_limit: Depth,
        retain_limit: Depth,
    ) -> SubscriberWindow;

    /// Advance `subscriber` past everything through `through`, and report the
    /// unseen seqs no window ever served it.
    ///
    /// `seen_floor` is the seq of the oldest entry the window being advanced
    /// over carried; unseen seqs below it were never visible to this
    /// subscriber and are its drops, computed as a subtraction and stored
    /// nowhere. Idempotent for a `through` at or below the current position, so
    /// a consumer that accepted nothing keeps everything it had and a
    /// re-advance reports nothing twice.
    ///
    /// This is the consumer's own bookkeeping, not a settlement of debt: a
    /// consumer that never advances just has an ever-lagging position that
    /// eviction eventually reports against.
    ///
    /// Only a push-enabled subscriber may be advanced. A sampled
    /// (`push_limit = 0`) subscriber holds no position: its window yields no
    /// [`SubscriberWindow::advance_span`], and a store that keeps an explicit
    /// position panics when asked to move one it does not have.
    async fn advance(
        &self,
        subscriber: &ParticipantId,
        through: MessageSeq,
        seen_floor: MessageSeq,
    ) -> AdvanceOutcome;

    /// Register `subscriber`'s delivery state on this channel, or retune an
    /// existing one's push depth. Priming positions the queue only when it comes
    /// into existence; a re-registration keeps its position and returns
    /// [`Attached::Existing`].
    ///
    /// Created versus Existing is the store's own determination, made from
    /// whether it already holds a position for this subscriber. No caller
    /// asserts it: a durable queue survives restarts and a ring one does not,
    /// and each store knows which of its queues are which.
    ///
    /// A sampled (`push_depth = 0`) attach creates no queue — there is nothing
    /// to deliver to and nothing to prime — and removes any position the
    /// subscriber held before the demotion.
    ///
    /// `app_slug`: the subscriber's application name, cached by stores that
    /// must attribute a lagging position without a second lookup.
    ///
    /// `push_depth` is a [`Depth`] because a system subscriber attaches
    /// unbounded, and a store that caches the depth must record that as what it
    /// is rather than as a fabricated bound.
    async fn attach(
        &self,
        subscriber: &ParticipantId,
        app_slug: &str,
        push_depth: Depth,
        priming: Priming,
    ) -> Attached;

    /// Tear down `subscriber`'s delivery state on this channel — the inverse of
    /// [`RetentionStore::attach`]. Its position and unread obligations go; the
    /// retained messages stay for whoever else is owed them.
    ///
    /// Idempotent: detaching a subscriber the store never saw removes nothing
    /// and is not an error.
    async fn detach(&self, subscriber: &ParticipantId);

    /// Commit a message into retention, immediately deliverable, and report
    /// whatever the message it displaced from the window cost a subscriber.
    ///
    /// Target-blind: the message is written once and nothing per-subscriber is.
    /// Who reads it is decided by each subscriber's own position, at its own
    /// read, so a commit resolves no subscriber set and owes nobody anything.
    async fn append(&self, msg: NewMessage) -> AppendOutcome;

    /// Park a message until `release_at`, invisible to every read until then.
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
    /// order. Every due message is reported, along with any overflow the batch
    /// caused.
    ///
    /// Target-blind, as [`RetentionStore::append`] is: release is what puts a
    /// parked message where the positions can see it, and each subscriber reads
    /// it from its own. A subscriber that attached while the message waited
    /// therefore receives it and one that left does not, without anyone
    /// resolving a subscriber set here.
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

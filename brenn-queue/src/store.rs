//! `RingCore` — the three mechanics composed into one channel's non-durable
//! state.
//!
//! A retained ring, one cursor per subscriber, and a set of parked messages are
//! independently simple; the way they compose is where a channel's semantics
//! live. An append must charge every cursor it outran, at the moment it outran
//! it. A release must enter retention exactly as an append does, and charge the
//! same way. An attach must decide priming from the ring's current tail, and a
//! sampled subscriber must hold no position at all. A window must be cut the
//! same width whether the subscriber holds a cursor or not.
//!
//! Every host that owns non-durable channels needs all of that, identically —
//! so it lives here once, above the primitives and below any host's identity,
//! locking, clock, or wake machinery. `M` (the payload), `Ep` (epoch identity)
//! and `S` (subscriber identity) are the host's; time is still the caller's
//! epoch-millisecond `u64`.

use std::collections::HashMap;
use std::hash::Hash;

use crate::cursor::{Advance, SubscriberCursor, Window, retention_frontier};
use crate::deferred::{
    Deferred, DeferredId, DeferredSet, NoSuchDeferred, QuotaExceeded, ReleaseTime,
};
use crate::ring::{Retained, RetainedRing};

/// One subscriber whose owed messages an entry into retention evicted.
///
/// The figure is what *this* eviction retired, so a subscriber stuck below the
/// frontier is named once per lost message however many appends it took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorOverflow<S> {
    pub subscriber: S,
    pub evicted: u64,
}

/// What an entry into retention did: the seq it took, and who it cost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendReport<S> {
    pub seq: u64,
    /// One entry per subscriber the append pushed retention past. Empty when
    /// nothing was evicted, which is the common case.
    pub overflow: Vec<CursorOverflow<S>>,
}

/// What a release pass moved into retention, and who its entries cost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseReport<M, S> {
    /// Release order, each with the seq retention assigned it.
    pub released: Vec<Retained<M>>,
    /// Merged across the batch, so a subscriber that several released messages
    /// pushed past is named once with the total.
    pub overflow: Vec<CursorOverflow<S>>,
}

/// Whether an attach brought a subscriber's position into existence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attached {
    /// The position came into existence on this attach, primed behind the
    /// retained tail.
    Created,
    /// The subscriber already held a position; it carried over and only its
    /// push depth was retuned.
    Existing,
}

/// Whether a parked entry named by a payload predicate is the caller's to touch.
///
/// The cross-sender case is *returned* rather than punished here: one host
/// reaches this only through its own snapshot, where it is an internal
/// invariant violation, while another can be handed the name by an untrusted
/// client, where it is a protocol violation. Deciding between a panic and a
/// rejection is the host's; recognizing the case is the model's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedDeferred<'a, M> {
    Owned(DeferredId, &'a Deferred<M>),
    /// Still parked, but under `owner`.
    WrongSender {
        owner: &'a str,
    },
    /// Not parked at all: released, cancelled, or never parked. The benign
    /// outcome of racing the release loop.
    NotFound,
}

/// Fold one entry's overflow into a batch's, so a subscriber that lost messages
/// to several entries is named once with the total.
///
/// Crate-private: the only batch of appends either host performs is a release
/// sweep, which this crate runs itself and reports pre-merged. A host that ever
/// folds overflow across appends of its own is what would make this a contract
/// worth publishing.
fn merge_overflow<S: PartialEq>(into: &mut Vec<CursorOverflow<S>>, more: Vec<CursorOverflow<S>>) {
    for event in more {
        match into.iter_mut().find(|e| e.subscriber == event.subscriber) {
            Some(existing) => existing.evicted += event.evicted,
            None => into.push(event),
        }
    }
}

/// One non-durable channel's whole state: what it retains, who is where in it,
/// and what is parked for later.
#[derive(Debug, Clone)]
pub struct RingCore<M, Ep, S> {
    ring: RetainedRing<M, Ep>,
    deferred: DeferredSet<M>,
    cursors: HashMap<S, SubscriberCursor>,
}

impl<M: Clone, Ep: Copy + PartialEq, S: Eq + Hash + Clone> RingCore<M, Ep, S> {
    /// A channel retaining at most `depth` messages under epoch identity
    /// `epoch`.
    ///
    /// `depth` also caps the deferred set: a channel may hold at most as much
    /// parked future as it holds retained past.
    pub fn new(epoch: Ep, depth: u64) -> Self {
        Self {
            ring: RetainedRing::new(epoch, depth),
            deferred: DeferredSet::new(Some(depth)),
            cursors: HashMap::new(),
        }
    }

    /// Retune the retained window and, with it, the parked-message cap,
    /// reporting every attached cursor a shrink pushed retention past.
    ///
    /// A shrink trims retention in place, and a trim retires messages a lagging
    /// cursor was still owed exactly as an append's eviction does — so it is
    /// reported exactly as an append's eviction is. Reporting it here is what
    /// keeps the two books one: a cursor's next advance excludes everything
    /// below the frontier from its `noise_charge` on the grounds that the
    /// eviction which retired it already accounted for it, which is only true if
    /// a trim accounts for its own.
    ///
    /// It never cancels parked work, so a deferred set above the new cap simply
    /// refuses further parks until it drains.
    pub fn set_depth(&mut self, depth: u64) -> Vec<CursorOverflow<S>> {
        let frontier = retention_frontier(&self.ring);
        self.ring.set_depth(depth);
        self.deferred.set_cap(Some(depth));
        self.overflow_since(frontier)
    }

    /// The retained window, for host-specific reads that compose over it —
    /// a replay, a scan for the loudest unseen message, an ambience read.
    pub fn ring(&self) -> &RetainedRing<M, Ep> {
        &self.ring
    }

    /// Every attached subscriber's position, for the same reason.
    pub fn cursors(&self) -> &HashMap<S, SubscriberCursor> {
        &self.cursors
    }

    // ── Retention ─────────────────────────────────────────────────────────

    /// Retain a message and report every attached cursor the append pushed
    /// retention past.
    ///
    /// Reporting the eviction from the append that caused it is what makes a
    /// drop attributable the moment it happens: a subscriber that never reads —
    /// wedged, starved, or simply idle — is still reported against. The cursors
    /// do not move, because a cursor left below the frontier *is* the record of
    /// what it lost. The frontier is captured before the append so each report
    /// covers only the span this append evicted.
    pub fn append(&mut self, message: M) -> AppendReport<S> {
        let frontier = retention_frontier(&self.ring);
        let appended = self.ring.append(message);
        if appended.evicted == 0 {
            return AppendReport {
                seq: appended.seq,
                overflow: Vec::new(),
            };
        }
        AppendReport {
            seq: appended.seq,
            overflow: self.overflow_since(frontier),
        }
    }

    /// Every cursor the retention frontier has moved past since it stood at
    /// `frontier`, with what each of them lost.
    ///
    /// The one place a retirement becomes a charge, so an append and a depth
    /// shrink report identically.
    fn overflow_since(&self, frontier: u64) -> Vec<CursorOverflow<S>> {
        let mut overflow = Vec::new();
        for (subscriber, cursor) in self.cursors.iter() {
            let evicted = cursor.evicted_since(&self.ring, frontier);
            if evicted > 0 {
                overflow.push(CursorOverflow {
                    subscriber: subscriber.clone(),
                    evicted,
                });
            }
        }
        overflow
    }

    // ── Subscribers ───────────────────────────────────────────────────────

    /// Give `subscriber` a position on this channel, or retune the one it has.
    ///
    /// A position that comes into existence is primed behind the retained tail,
    /// capped by push depth: attach is a delivery point, and what a channel
    /// retains is its history, all of it. Priming applies only then —
    /// re-attaching an existing subscriber is not a new attach, so its position
    /// carries over.
    ///
    /// A sampled (`push_depth = 0`) attach creates no position and removes any
    /// held before the demotion: a sampled subscriber is never delivered to, so
    /// a position kept for it would be one every eviction charges and no window
    /// can ever serve.
    pub fn attach(&mut self, subscriber: S, push_depth: u64) -> Attached {
        if push_depth == 0 {
            self.cursors.remove(&subscriber);
            return Attached::Existing;
        }
        if let Some(cursor) = self.cursors.get_mut(&subscriber) {
            cursor.set_push_depth(push_depth);
            return Attached::Existing;
        }
        self.cursors
            .insert(subscriber, SubscriberCursor::primed(&self.ring, push_depth));
        Attached::Created
    }

    /// Drop a subscriber's position. Its unread obligations go with it; the
    /// messages stay retained for whoever else is owed them.
    pub fn detach(&mut self, subscriber: &S) {
        self.cursors.remove(subscriber);
    }

    /// Whether `subscriber` holds a position on this channel.
    pub fn is_attached(&self, subscriber: &S) -> bool {
        self.cursors.contains_key(subscriber)
    }

    /// Whether this subscriber is owed something the ring still holds — the
    /// wake question. `false` for a subscriber holding no position, which is
    /// owed nothing by definition.
    pub fn has_deliverable(&self, subscriber: &S) -> bool {
        self.cursors
            .get(subscriber)
            .is_some_and(|cursor| cursor.has_deliverable(&self.ring))
    }

    /// This subscriber's activation view: the most recent
    /// `max(push_limit, retain_limit)` retained messages with the boundary where
    /// its unseen ones begin. `push_limit` retunes the stored depth first, so
    /// the window the caller asked for is the window it gets.
    ///
    /// Pure read as far as delivery goes: no position moves and nothing is
    /// charged.
    ///
    /// A sampled (`push_limit = 0`) read holds no position: it is served the
    /// span as context whether or not the subscriber has a position from
    /// somewhere else, and never retunes one — writing that zero into a cursor
    /// would leave a depth the model says a position cannot hold.
    ///
    /// `None` for a push-enabled read by a subscriber holding no position:
    /// there is no window to cut, and inventing one would deliver messages
    /// nothing will ever advance over.
    pub fn window(
        &mut self,
        subscriber: &S,
        push_limit: u64,
        retain_limit: u64,
    ) -> Option<Window<M>> {
        let Some(cursor) = self.cursors.get_mut(subscriber) else {
            if push_limit > 0 {
                return None;
            }
            let entries: Vec<Retained<M>> = self.ring.tail(retain_limit).cloned().collect();
            return Some(Window {
                new_from: entries.len(),
                entries,
                push_enabled: false,
            });
        };
        if push_limit > 0 {
            cursor.set_push_depth(push_limit);
        }
        Some(cursor.window(&self.ring, push_limit, retain_limit))
    }

    /// Move this subscriber's position to `through + 1` and report the unseen
    /// seqs no window ever served it, `seen_floor` being the oldest seq the
    /// window it is advancing over carried.
    ///
    /// `None` for a subscriber holding no position: nothing to move, nothing to
    /// report, nothing mutated.
    pub fn advance(&mut self, subscriber: &S, through: u64, seen_floor: u64) -> Option<Advance> {
        let ring = &self.ring;
        self.cursors
            .get_mut(subscriber)
            .map(|cursor| cursor.advance(ring, through, seen_floor))
    }

    // ── Deferral ──────────────────────────────────────────────────────────

    /// Park a message until `release_at`, under the channel-wide cap.
    ///
    /// A parked message is in no position's owed set, no replay, and no
    /// retained tail until it releases.
    pub fn park(
        &mut self,
        sender: impl Into<String>,
        message: M,
        release_at: ReleaseTime,
    ) -> Result<DeferredId, QuotaExceeded> {
        self.deferred.park(sender, message, release_at)
    }

    /// When this channel's next parked message comes due, or `None` when
    /// nothing is parked — the deadline a release loop arms from.
    ///
    /// An already-due entry reports its own past release time rather than being
    /// skipped, so a loop that computed its wait from a fresher instant than
    /// its last release pass used still hears about what matured in between.
    pub fn next_release(&self) -> Option<ReleaseTime> {
        self.deferred.next_release()
    }

    /// Move every message due at or before `now` into retention, in release
    /// order, and report what the batch cost.
    ///
    /// A released message enters retention exactly as an appended one does —
    /// same fresh seq at the tail, same eviction charges — because from every
    /// subscriber's point of view it is simply a message that just arrived.
    pub fn release_due(&mut self, now: ReleaseTime) -> ReleaseReport<M, S> {
        let due = self.deferred.release_due(now);
        let mut released = Vec::with_capacity(due.len());
        let mut overflow: Vec<CursorOverflow<S>> = Vec::new();
        for entry in due {
            let report = self.append(entry.message.clone());
            released.push(Retained {
                seq: report.seq,
                message: entry.message,
            });
            merge_overflow(&mut overflow, report.overflow);
        }
        ReleaseReport { released, overflow }
    }

    /// One sender's messages still parked at `now`, release order.
    ///
    /// Parked is exactly `release_at > now`: an entry whose time has come is
    /// out of the view even before the release pass takes it, because there is
    /// nothing left to cancel or edit.
    ///
    /// The sender filter is the whole authorization story for a per-sender
    /// view: a caller scoped to a sender can never observe another sender's
    /// parked message.
    pub fn deferred_for_sender<'a>(
        &'a self,
        sender: &'a str,
        now: ReleaseTime,
    ) -> impl Iterator<Item = &'a Deferred<M>> {
        self.deferred
            .for_sender(sender)
            .filter(move |e| e.release_at > now)
    }

    /// Every message still parked at `now`, release order — the operator's read
    /// across senders.
    ///
    /// Carries no authorization: unlike [`RingCore::deferred_for_sender`] this
    /// spans senders, so a host may only serve it to a caller entitled to the
    /// whole channel.
    pub fn deferred_at(&self, now: ReleaseTime) -> impl Iterator<Item = &Deferred<M>> {
        self.deferred.iter().filter(move |e| e.release_at > now)
    }

    /// Messages held channel-wide, released or not — the deferred set's
    /// occupancy against its cap.
    pub fn deferred_len(&self) -> usize {
        self.deferred.len()
    }

    /// Whether the parked entry a payload predicate names is `sender`'s to
    /// cancel or edit, and still parked at `cutoff`.
    ///
    /// The predicate is the host's, because the identity a guest names a parked
    /// message by is a payload field this crate knows nothing about. The
    /// ownership rule is not the host's, which is why it is decided here.
    ///
    /// The scan is bounded by the deferred cap, which is the channel's depth.
    pub fn owned_deferred(
        &self,
        sender: &str,
        matches: impl Fn(&M) -> bool,
        cutoff: ReleaseTime,
    ) -> OwnedDeferred<'_, M> {
        let Some(entry) = self
            .deferred
            .iter()
            .find(|e| e.release_at > cutoff && matches(&e.message))
        else {
            return OwnedDeferred::NotFound;
        };
        if entry.sender != sender {
            return OwnedDeferred::WrongSender {
                owner: &entry.sender,
            };
        }
        OwnedDeferred::Owned(entry.id, entry)
    }

    /// The parked entry under `id`, for a host that must read a payload before
    /// rewriting part of it.
    pub fn deferred_entry(&self, id: DeferredId) -> Option<&Deferred<M>> {
        self.deferred.get(id)
    }

    /// Unpark one entry. `None` when it is no longer parked — the benign
    /// release race.
    pub fn cancel_deferred(&mut self, id: DeferredId) -> Option<Deferred<M>> {
        self.deferred.cancel(id)
    }

    /// Replace one parked entry's payload, its release time, or both, keeping
    /// its identity. Errs on the same release race `cancel_deferred` reports as
    /// `None`.
    pub fn edit_deferred(
        &mut self,
        id: DeferredId,
        message: Option<M>,
        release_at: Option<ReleaseTime>,
    ) -> Result<(), NoSuchDeferred> {
        self.deferred.edit(id, message, release_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Core = RingCore<&'static str, u8, &'static str>;

    fn core(depth: u64) -> Core {
        RingCore::new(1, depth)
    }

    fn publish(core: &mut Core, bodies: &[&'static str]) {
        for body in bodies {
            core.append(body);
        }
    }

    /// Serve a subscriber its window and advance over it, as a push consumer
    /// does: the bodies it was handed as new, and what the advance reported.
    fn serve(
        core: &mut Core,
        subscriber: &'static str,
        push_limit: u64,
    ) -> (Vec<&'static str>, Advance) {
        let window = core
            .window(&subscriber, push_limit, 0)
            .expect("the case attached this subscriber");
        let new: Vec<&'static str> = window.new_entries().iter().map(|e| e.message).collect();
        let advance = match window.advance_span() {
            Some((through, seen_floor)) => core
                .advance(&subscriber, through, seen_floor)
                .expect("the case attached this subscriber"),
            None => Advance {
                dropped: 0,
                noise_charge: 0,
            },
        };
        (new, advance)
    }

    fn overflow_of(report: &AppendReport<&'static str>) -> Vec<(&'static str, u64)> {
        let mut events: Vec<(&'static str, u64)> = report
            .overflow
            .iter()
            .map(|e| (e.subscriber, e.evicted))
            .collect();
        events.sort_unstable();
        events
    }

    // ── Retention and the eviction fan-out ────────────────────────────────

    #[test]
    fn append_charges_every_cursor_it_outran_and_nobody_else() {
        let mut c = core(2);
        c.attach("fast", 8);
        c.attach("slow", 8);
        publish(&mut c, &["a", "b"]);
        serve(&mut c, "fast", 8);

        // `a` leaves retention: only the cursor still owed it is charged.
        let report = c.append("c");
        assert_eq!(report.seq, 3);
        assert_eq!(overflow_of(&report), vec![("slow", 1)]);
    }

    /// Each append charges only the span it evicted, so a wedged subscriber is
    /// named once per lost message however many appends it took.
    #[test]
    fn successive_appends_never_double_charge() {
        let mut c = core(2);
        c.attach("wedged", 8);
        publish(&mut c, &["a", "b"]);

        let mut charged = 0;
        for body in ["c", "d", "e"] {
            charged += c
                .append(body)
                .overflow
                .iter()
                .map(|e| e.evicted)
                .sum::<u64>();
        }
        assert_eq!(charged, 3, "a, b and c, each charged once");
        // The position did not move: it is the record of the loss.
        assert_eq!(c.cursors()["wedged"].next_owed(), 1);
    }

    #[test]
    fn a_sampled_subscriber_holds_no_position_and_is_never_charged() {
        let mut c = core(1);
        assert_eq!(c.attach("sampler", 0), Attached::Existing);
        assert!(!c.is_attached(&"sampler"));
        publish(&mut c, &["a"]);
        assert!(overflow_of(&c.append("b")).is_empty());
        assert!(!c.has_deliverable(&"sampler"));
    }

    /// A demotion to sampled removes the position the subscriber held: keeping
    /// it would leave one every eviction charges and no window can serve.
    #[test]
    fn attaching_sampled_removes_a_position_already_held() {
        let mut c = core(4);
        assert_eq!(c.attach("proc", 4), Attached::Created);
        assert_eq!(c.attach("proc", 0), Attached::Existing);
        assert!(!c.is_attached(&"proc"));
    }

    // ── Attach ────────────────────────────────────────────────────────────

    #[test]
    fn retained_priming_delivers_the_capped_tail_as_new() {
        let mut c = core(8);
        publish(&mut c, &["a", "b", "c"]);
        assert_eq!(c.attach("proc", 2), Attached::Created);
        let (new, advance) = serve(&mut c, "proc", 2);
        assert_eq!(new, vec!["b", "c"]);
        assert_eq!(advance.dropped, 0);
    }

    /// A push reach deeper than the tail primes the whole tail and no more: the
    /// prime is `min(push_depth, tail)`, and there is nothing to invent below
    /// the oldest message the ring holds.
    #[test]
    fn a_push_reach_wider_than_the_tail_primes_the_whole_tail() {
        let mut c = core(8);
        publish(&mut c, &["a", "b", "c"]);
        c.attach("proc", 8);
        let (new, advance) = serve(&mut c, "proc", 8);
        assert_eq!(new, vec!["a", "b", "c"]);
        assert_eq!(advance.dropped, 0);
    }

    /// There is one priming: a fresh position is owed the retained tail whether
    /// it attaches before or after the messages were published. Attach is a
    /// delivery point, and unseen is unseen however old the message is.
    #[test]
    fn a_fresh_position_is_owed_the_tail_it_attached_after() {
        let mut c = core(8);
        publish(&mut c, &["old"]);
        c.attach("proc", 4);
        assert_eq!(serve(&mut c, "proc", 4).0, vec!["old"]);
        publish(&mut c, &["new"]);
        assert_eq!(serve(&mut c, "proc", 4).0, vec!["new"]);
    }

    #[test]
    fn reattach_keeps_the_position_and_retunes_the_depth() {
        let mut c = core(8);
        c.attach("proc", 4);
        publish(&mut c, &["a", "b", "c"]);
        assert_eq!(c.attach("proc", 1), Attached::Existing);
        let (new, advance) = serve(&mut c, "proc", 1);
        assert_eq!(new, vec!["c"]);
        assert_eq!(advance.dropped, 2, "the retune clamped the window");
    }

    #[test]
    fn detach_drops_the_position_and_leaves_retention() {
        let mut c = core(8);
        c.attach("proc", 4);
        publish(&mut c, &["a"]);
        c.detach(&"proc");
        assert!(!c.has_deliverable(&"proc"));
        assert_eq!(c.ring().len(), 1);
    }

    // ── Windows ───────────────────────────────────────────────────────────

    /// A sampled or unattached subscriber still gets the span it asked for as
    /// context, and none of it is new — the same width an attached one sees.
    #[test]
    fn an_unattached_context_read_is_a_synthetic_all_seen_window() {
        let mut c = core(8);
        publish(&mut c, &["a", "b", "c"]);
        let window = c
            .window(&"observer", 0, 2)
            .expect("a context read is served");
        assert_eq!(
            window.entries.iter().map(|e| e.message).collect::<Vec<_>>(),
            vec!["b", "c"]
        );
        assert_eq!(window.new_len(), 0);
        assert!(!window.push_enabled);
        assert_eq!(window.advance_span(), None);
        assert!(!c.is_attached(&"observer"));
    }

    #[test]
    fn a_push_read_by_an_unattached_subscriber_has_no_window() {
        let mut c = core(8);
        publish(&mut c, &["a"]);
        assert!(c.window(&"proc", 1, 0).is_none());
        assert!(c.advance(&"proc", 1, 1).is_none());
    }

    /// Retention wider than the push depth serves the excess as context, so a
    /// burst larger than the push depth is not counted lost.
    #[test]
    fn retention_above_push_depth_back_fills_as_context() {
        let mut c = core(8);
        c.attach("proc", 1);
        publish(&mut c, &["a", "b", "c"]);
        let window = c.window(&"proc", 1, 4).expect("attached");
        assert_eq!(window.entries.len(), 3);
        assert_eq!(window.new_len(), 1);
        let (through, floor) = window.advance_span().expect("served entries");
        let advance = c.advance(&"proc", through, floor).expect("attached");
        assert_eq!(advance.dropped, 0, "the window served every unseen entry");
    }

    /// Two subscribers reading one ring at different push depths: the retention
    /// is shared, the positions are not. This is the property the cursor model
    /// exists to provide, so it is pinned at the layer that owns it.
    #[test]
    fn two_subscribers_read_one_ring_at_their_own_depths() {
        let mut c = core(4);
        c.attach("deep", 4);
        c.attach("shallow", 1);
        publish(&mut c, &["a", "b", "c"]);

        let (deep_new, deep_advance) = serve(&mut c, "deep", 4);
        assert_eq!(
            deep_new,
            vec!["a", "b", "c"],
            "the deep reach serves them all"
        );
        assert_eq!(deep_advance.dropped, 0);
        assert_eq!(deep_advance.noise_charge, 0);

        // The shallow subscriber's own window promoted only the newest, and its
        // own advance charges the two it never saw — nothing of the deep
        // subscriber's reach leaked into it.
        let (shallow_new, shallow_advance) = serve(&mut c, "shallow", 1);
        assert_eq!(shallow_new, vec!["c"]);
        assert_eq!(shallow_advance.dropped, 2);
        assert_eq!(
            shallow_advance.noise_charge, 2,
            "still-retained loss: no eviction reported it"
        );

        assert!(!c.has_deliverable(&"deep"));
        assert!(!c.has_deliverable(&"shallow"));
    }

    #[test]
    fn set_depth_retunes_retention_and_the_park_cap() {
        let mut c = core(4);
        publish(&mut c, &["a", "b", "c", "d"]);
        c.park("alice", "x", 100).expect("under the cap");
        c.set_depth(1);
        assert_eq!(c.ring().len(), 1);
        assert_eq!(c.deferred_len(), 1, "a cap shrink never cancels work");
        assert_eq!(c.park("alice", "y", 200), Err(QuotaExceeded { cap: 1 }));
    }

    /// A shrink retires messages out from under a lagging cursor, and reports it
    /// exactly as the append that evicted them would have: the trim is the same
    /// kind of retirement, so it lands in the same book. A caught-up cursor loses
    /// nothing and is not named.
    #[test]
    fn a_depth_shrink_charges_the_cursors_it_trimmed_past() {
        let mut c = core(4);
        c.attach("lagging", 4);
        c.attach("caught-up", 4);
        publish(&mut c, &["a", "b", "c", "d"]);
        serve(&mut c, "caught-up", 4);

        let mut charged: Vec<(&str, u64)> = c
            .set_depth(1)
            .into_iter()
            .map(|e| (e.subscriber, e.evicted))
            .collect();
        charged.sort_unstable();
        assert_eq!(charged, vec![("lagging", 3)]);
        assert_eq!(c.ring().len(), 1);

        // The charge is not repeated at the advance that passes the trimmed span:
        // it is below the frontier now, so the shrink's report is the only one.
        let (new, advance) = serve(&mut c, "lagging", 4);
        assert_eq!(new, vec!["d"]);
        assert_eq!(advance.dropped, 3, "the guest is still told the truth");
        assert_eq!(advance.noise_charge, 0, "the shrink already charged it");
    }

    /// A grow retires nothing, so it charges nothing.
    #[test]
    fn a_depth_grow_charges_nobody() {
        let mut c = core(2);
        c.attach("lagging", 4);
        publish(&mut c, &["a", "b"]);
        assert!(c.set_depth(8).is_empty());
        assert_eq!(c.ring().len(), 2);
    }

    // ── Deferral ──────────────────────────────────────────────────────────

    #[test]
    fn the_park_cap_is_the_retained_depth() {
        let mut c = core(2);
        c.park("alice", "a", 100).expect("under the cap");
        c.park("bob", "b", 100).expect("under the cap");
        assert_eq!(c.park("alice", "c", 100), Err(QuotaExceeded { cap: 2 }));
        assert_eq!(c.deferred_len(), 2);
    }

    /// A parked message is invisible until it releases, and then it is an
    /// ordinary arrival: fresh tail seq, and it wakes whoever is attached.
    #[test]
    fn release_enters_retention_as_an_ordinary_arrival() {
        let mut c = core(8);
        c.attach("proc", 4);
        c.append("now");
        c.park("alice", "later", 100).expect("under the cap");
        assert_eq!(serve(&mut c, "proc", 4).0, vec!["now"]);
        assert!(!c.has_deliverable(&"proc"));

        assert!(c.release_due(99).released.is_empty(), "not yet due");
        let report = c.release_due(100);
        assert_eq!(
            report.released.iter().map(|r| r.seq).collect::<Vec<_>>(),
            vec![2]
        );
        assert!(c.has_deliverable(&"proc"));
        assert_eq!(serve(&mut c, "proc", 4).0, vec!["later"]);
        assert_eq!(c.deferred_len(), 0);
    }

    /// A release pass is release-ordered, and its charges are merged per
    /// subscriber rather than reported once per released message.
    #[test]
    fn a_release_batch_merges_its_charges_per_subscriber() {
        let mut c = core(3);
        c.attach("wedged", 8);
        c.park("alice", "third", 300).expect("under the cap");
        c.park("alice", "first", 100).expect("under the cap");
        c.park("alice", "second", 200).expect("under the cap");
        // Narrow retention under the parked set: the batch now overruns the
        // window it releases into. A cap shrink cancels nothing already parked.
        c.set_depth(1);

        let report = c.release_due(300);
        assert_eq!(
            report
                .released
                .iter()
                .map(|r| (r.seq, r.message))
                .collect::<Vec<_>>(),
            vec![(1, "first"), (2, "second"), (3, "third")]
        );
        // Two of the three appends evicted from under the wedged cursor, and it
        // is named once with the total rather than once per append.
        assert_eq!(
            report
                .overflow
                .iter()
                .map(|e| (e.subscriber, e.evicted))
                .collect::<Vec<_>>(),
            vec![("wedged", 2)]
        );
    }

    #[test]
    fn next_release_is_the_earliest_deadline() {
        let mut c = core(8);
        assert_eq!(c.next_release(), None);
        c.park("alice", "late", 300).expect("under the cap");
        c.park("alice", "early", 100).expect("under the cap");
        assert_eq!(c.next_release(), Some(100));
        c.release_due(100);
        assert_eq!(c.next_release(), Some(300));
    }

    #[test]
    fn a_sender_view_holds_only_its_own_still_parked_entries() {
        let mut c = core(8);
        c.park("alice", "mine-due", 100).expect("under the cap");
        c.park("alice", "mine-later", 300).expect("under the cap");
        c.park("bob", "theirs", 300).expect("under the cap");
        let view: Vec<&'static str> = c
            .deferred_for_sender("alice", 200)
            .map(|e| e.message)
            .collect();
        assert_eq!(view, vec!["mine-later"], "due entries leave the view");
    }

    // ── Ownership of a parked entry ───────────────────────────────────────

    fn is(body: &'static str) -> impl Fn(&&'static str) -> bool {
        move |m: &&'static str| *m == body
    }

    #[test]
    fn owned_deferred_names_the_senders_own_entry() {
        let mut c = core(8);
        let id = c.park("alice", "mine", 300).expect("under the cap");
        match c.owned_deferred("alice", is("mine"), 200) {
            OwnedDeferred::Owned(found, entry) => {
                assert_eq!(found, id);
                assert_eq!(entry.release_at, 300);
            }
            other => panic!("expected the entry, got {other:?}"),
        }
    }

    #[test]
    fn owned_deferred_reports_another_senders_entry_rather_than_panicking() {
        let mut c = core(8);
        c.park("bob", "theirs", 300).expect("under the cap");
        assert_eq!(
            c.owned_deferred("alice", is("theirs"), 200),
            OwnedDeferred::WrongSender { owner: "bob" }
        );
    }

    #[test]
    fn owned_deferred_finds_nothing_once_the_entry_is_due_or_gone() {
        let mut c = core(8);
        let id = c.park("alice", "mine", 100).expect("under the cap");
        assert_eq!(
            c.owned_deferred("alice", is("mine"), 200),
            OwnedDeferred::NotFound,
            "a due entry is past cancelling"
        );
        c.cancel_deferred(id).expect("still held");
        assert_eq!(
            c.owned_deferred("alice", is("mine"), 50),
            OwnedDeferred::NotFound
        );
        assert_eq!(
            c.owned_deferred("alice", is("never"), 50),
            OwnedDeferred::NotFound
        );
    }

    #[test]
    fn editing_a_parked_entry_keeps_its_identity_and_reorders_it() {
        let mut c = core(8);
        let a = c.park("alice", "a", 100).expect("under the cap");
        c.park("alice", "b", 200).expect("under the cap");
        c.edit_deferred(a, Some("a-edited"), Some(300))
            .expect("still parked");
        assert_eq!(c.deferred_entry(a).map(|e| e.message), Some("a-edited"));
        assert_eq!(c.next_release(), Some(200));
        let released: Vec<&'static str> = c
            .release_due(300)
            .released
            .into_iter()
            .map(|r| r.message)
            .collect();
        assert_eq!(released, vec!["b", "a-edited"]);
    }

    #[test]
    fn editing_a_released_entry_reports_the_race() {
        let mut c = core(8);
        let a = c.park("alice", "a", 100).expect("under the cap");
        c.release_due(100);
        assert_eq!(c.edit_deferred(a, Some("x"), None), Err(NoSuchDeferred(a)));
        assert!(c.cancel_deferred(a).is_none());
    }
}

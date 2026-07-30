//! The per-subscriber cursor: one subscriber's position on a shared retained
//! ring.

use crate::ring::{Retained, RetainedRing};

/// A subscriber's activation view: a window of retained messages with the
/// boundary where its unseen messages begin.
///
/// The window is the most recent `max(push_limit, retain_limit)` retained
/// entries; everything below `new_from` is context the subscriber has already
/// seen, or unseen messages the push limit did not promote to new. Reading it
/// moves nothing and charges nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window<M> {
    /// Oldest first, each entry carrying the seq the ring assigned it.
    pub entries: Vec<Retained<M>>,
    /// Index of the first new entry; equal to `entries.len()` when nothing in
    /// the window is new.
    pub new_from: usize,
    /// Whether the subscriber this window was cut for holds a position at all.
    /// A sampled subscriber (`push_limit = 0`) does not.
    pub push_enabled: bool,
}

impl<M> Window<M> {
    /// The `(through, seen_floor)` pair an advance over this window is made
    /// with, or `None` when there is nothing to advance: a window that served
    /// nothing, or a sampled subscriber, which holds no position to move.
    pub fn advance_span(&self) -> Option<(u64, u64)> {
        if !self.push_enabled {
            return None;
        }
        Some((self.entries.last()?.seq, self.entries.first()?.seq))
    }

    /// How many entries are new to the subscriber.
    pub fn new_len(&self) -> usize {
        self.entries.len() - self.new_from
    }

    /// The new entries, oldest first.
    pub fn new_entries(&self) -> &[Retained<M>] {
        &self.entries[self.new_from..]
    }
}

/// What an advance passed over without ever having served it.
///
/// Both figures are subtractions between sequence numbers, computed at the
/// advance and stored nowhere: a cursor holds a position and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Advance {
    /// Unseen seqs the advance stepped past that no window served — the
    /// subscriber's visible loss since its previous advance.
    pub dropped: u64,
    /// The portion of `dropped` that is still inside the retained window, and
    /// so has not already been reported by the eviction that retired it. The
    /// figure a noise ladder acts on, so a span is never enacted twice.
    pub noise_charge: u64,
}

/// One subscriber's position on a channel.
///
/// A cursor is a position, not a copy of the queue: the messages live once in
/// the channel's [`RetainedRing`] and every subscriber reads them from there.
/// That is what makes a loss *this subscriber's* accountable drop rather than
/// an anonymous channel-wide one, which is what the noise ladder needs to
/// escalate against the right party.
///
/// Nothing but [`SubscriberCursor::advance`] moves it. Eviction reports against
/// a cursor it has outrun and leaves it where it is: a cursor below the
/// retention frontier is not an error state, it *is* the drop record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriberCursor {
    /// The lowest seq this subscriber has not yet passed.
    next_owed: u64,
    /// The most messages one window may present as new. `0` is a sampled
    /// subscriber: it is never delivered to and never reported against.
    push_depth: u64,
}

impl SubscriberCursor {
    /// A cursor positioned behind the channel's retained tail, capped at
    /// `push_depth` — the position for a queue that has just come into
    /// existence.
    ///
    /// Attach is a delivery point: a message published before its consumer
    /// existed still reaches and still wakes that consumer, so a fresh queue is
    /// primed rather than started empty.
    pub fn primed<M: Clone, Ep: Copy + PartialEq>(
        ring: &RetainedRing<M, Ep>,
        push_depth: u64,
    ) -> Self {
        Self::new(ring.primed_from(push_depth), push_depth)
    }

    fn new(last_seen: u64, push_depth: u64) -> Self {
        Self {
            next_owed: last_seen + 1,
            push_depth,
        }
    }

    /// The lowest seq this subscriber has not yet passed.
    pub fn next_owed(&self) -> u64 {
        self.next_owed
    }

    pub fn push_depth(&self) -> u64 {
        self.push_depth
    }

    /// Retune the delivery bound. Takes effect at the next window.
    pub fn set_push_depth(&mut self, push_depth: u64) {
        self.push_depth = push_depth;
    }

    /// Whether this subscriber is push-enabled: a sampled (`push_depth = 0`)
    /// subscriber reads the channel but is never delivered to and never
    /// reported against.
    pub fn is_push_enabled(&self) -> bool {
        self.push_depth > 0
    }

    /// Whether the ring holds anything this subscriber has not passed and can
    /// still be handed — the wake question.
    pub fn has_deliverable<M: Clone, Ep: Copy + PartialEq>(
        &self,
        ring: &RetainedRing<M, Ep>,
    ) -> bool {
        // The ring's entries are a dense ascending run ending at the newest
        // assigned seq, so the newest one alone answers this.
        self.is_push_enabled() && !ring.is_empty() && ring.newest_seq() >= self.next_owed
    }

    /// How many of this subscriber's unseen messages the ring no longer holds.
    ///
    /// Report-only: the cursor does not move, because a cursor below the
    /// retention frontier is exactly the record of what was lost. Calling this
    /// from the append that evicted them is what makes an eviction accountable
    /// when it happens rather than whenever the subscriber next runs — a
    /// subscriber that never reads still has its losses reported. The caller is
    /// the eviction, so it reports only the span it itself evicted: pass the
    /// frontier from before the eviction as `old_frontier`.
    pub fn evicted_since<M: Clone, Ep: Copy + PartialEq>(
        &self,
        ring: &RetainedRing<M, Ep>,
        old_frontier: u64,
    ) -> u64 {
        if !self.is_push_enabled() {
            return 0;
        }
        frontier(ring).saturating_sub(self.next_owed.max(old_frontier))
    }

    /// The subscriber's activation view: the most recent
    /// `max(push_limit, retain_limit)` retained entries, with the new boundary
    /// decided from the cursor.
    ///
    /// New is the *newest* `min(unseen, push_limit)` of them: a subscriber woken
    /// late acts on the freshest messages, which is the same drop-oldest rule
    /// the ring itself follows. Unseen entries below that boundary are context
    /// — served, not lost. Unseen seqs below the whole window were never
    /// visible, and the advance that passes them reports them.
    ///
    /// Pure read: moves no cursor and charges nothing.
    pub fn window<M: Clone, Ep: Copy + PartialEq>(
        &self,
        ring: &RetainedRing<M, Ep>,
        push_limit: u64,
        retain_limit: u64,
    ) -> Window<M> {
        let entries: Vec<Retained<M>> = ring.tail(push_limit.max(retain_limit)).cloned().collect();
        Window {
            new_from: new_boundary(entries.iter().map(|e| e.seq), self.next_owed, push_limit),
            entries,
            push_enabled: push_limit > 0,
        }
    }

    /// Move the cursor to `through + 1` and report what it passed unserved.
    ///
    /// `seen_floor` is the seq of the oldest entry the window served — unseen
    /// seqs below it were never visible to this subscriber and are its drops.
    /// Idempotent for a `through` at or below the current position: a consumer
    /// that accepted nothing keeps everything it had.
    ///
    /// Panics for a sampled (`push_depth = 0`) subscriber: it is never delivered
    /// to and never reported against, so it holds no position to move.
    pub fn advance<M: Clone, Ep: Copy + PartialEq>(
        &mut self,
        ring: &RetainedRing<M, Ep>,
        through: u64,
        seen_floor: u64,
    ) -> Advance {
        assert!(
            self.is_push_enabled(),
            "cursor: advance over a sampled subscriber, which holds no position"
        );
        assert!(
            seen_floor <= through.saturating_add(1),
            "cursor: seen_floor {seen_floor} is above the window it came from (through {through})"
        );
        let advance = Advance {
            dropped: seen_floor.saturating_sub(self.next_owed),
            // Everything below the frontier was reported by the eviction that
            // retired it, so only the still-retained part is charged here.
            noise_charge: seen_floor.saturating_sub(self.next_owed.max(frontier(ring))),
        };
        if through >= self.next_owed {
            self.next_owed = through + 1;
        }
        advance
    }
}

/// Where a window's new entries begin: the newest `min(unseen, push_limit)` of
/// the entries `seqs` names are new, everything before them is context.
///
/// The one authority for the boundary rule, so a store composing the same
/// window from its own retention rather than from a ring cuts it identically.
/// `seqs` is the window's entries, oldest first; unseen means at or above
/// `next_owed`.
pub fn new_boundary(seqs: impl IntoIterator<Item = u64>, next_owed: u64, push_limit: u64) -> usize {
    let mut len = 0usize;
    let mut unseen = 0usize;
    for seq in seqs {
        len += 1;
        if seq >= next_owed {
            unseen += 1;
        }
    }
    len - unseen.min(usize::try_from(push_limit).unwrap_or(usize::MAX))
}

/// The oldest seq the ring still holds, or the seq it will assign next when it
/// holds nothing — the boundary below which every message is gone.
fn frontier<M: Clone, Ep: Copy + PartialEq>(ring: &RetainedRing<M, Ep>) -> u64 {
    ring.oldest_seq().unwrap_or_else(|| ring.newest_seq() + 1)
}

/// The retention frontier of `ring`, for a caller that must capture it before
/// an append evicts past it.
pub fn retention_frontier<M: Clone, Ep: Copy + PartialEq>(ring: &RetainedRing<M, Ep>) -> u64 {
    frontier(ring)
}

/// Cut a gap window to the deliverable suffix above `resume_seq`, and count the
/// positions lost between the resume point and the suffix's oldest entry.
///
/// A gap answer's window is the channel's newest retained entries rather than a
/// suffix of the resume point, so a resuming consumer cannot take it whole:
/// entries at or below `resume_seq` are copies of positions it already holds,
/// and the seqs between `resume_seq` and the oldest surviving entry are gone.
///
/// `lost` is the same subtraction [`Advance::dropped`] performs at a cursor
/// advance — unseen seqs the position stepped past that no window served —
/// computed at a resume instead of at an advance. An empty suffix loses nothing:
/// the position never moves, so nothing was stepped past.
pub fn gap_suffix<M>(window: Vec<Retained<M>>, resume_seq: u64) -> (Vec<Retained<M>>, u64) {
    let suffix: Vec<Retained<M>> = window
        .into_iter()
        .filter(|retained| retained.seq > resume_seq)
        .collect();
    let lost = suffix
        .first()
        .map_or(0, |retained| retained.seq - resume_seq - 1);
    (suffix, lost)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring_of(depth: u64, messages: &[&'static str]) -> RetainedRing<&'static str, u8> {
        let mut ring = RetainedRing::new(1, depth);
        for m in messages {
            ring.append(*m);
        }
        ring
    }

    fn empty(depth: u64) -> RetainedRing<&'static str, u8> {
        ring_of(depth, &[])
    }

    /// Read the window and advance over it, as a push consumer does.
    fn serve(
        cursor: &mut SubscriberCursor,
        ring: &RetainedRing<&'static str, u8>,
        push_limit: u64,
        retain_limit: u64,
    ) -> (Vec<&'static str>, Advance) {
        let window = cursor.window(ring, push_limit, retain_limit);
        let new: Vec<&'static str> = window.new_entries().iter().map(|e| e.message).collect();
        let advance = match window.advance_span() {
            Some((through, seen_floor)) => cursor.advance(ring, through, seen_floor),
            None => Advance {
                dropped: 0,
                noise_charge: 0,
            },
        };
        (new, advance)
    }

    /// A position that has advanced over everything the ring holds owes
    /// nothing that already exists: the next append is its next delivery, and
    /// re-serving in between hands over nothing and drops nothing.
    #[test]
    fn a_caught_up_position_owes_only_what_comes_next() {
        let mut ring = ring_of(8, &["a", "b"]);
        let mut cursor = SubscriberCursor::primed(&ring, 4);
        assert_eq!(serve(&mut cursor, &ring, 4, 0).0, vec!["a", "b"]);
        assert!(serve(&mut cursor, &ring, 4, 0).0.is_empty());
        ring.append("c");
        let (new, advance) = serve(&mut cursor, &ring, 4, 0);
        assert_eq!(new, vec!["c"]);
        assert_eq!(advance.dropped, 0);
    }

    #[test]
    fn primed_starts_behind_the_retained_tail_capped_by_push_depth() {
        let ring = ring_of(8, &["a", "b", "c", "d"]);
        let mut cursor = SubscriberCursor::primed(&ring, 2);
        let (new, advance) = serve(&mut cursor, &ring, 2, 0);
        assert_eq!(new, vec!["c", "d"]);
        assert_eq!(advance.dropped, 0);
    }

    #[test]
    fn primed_on_empty_ring_serves_nothing() {
        let ring = empty(8);
        let mut cursor = SubscriberCursor::primed(&ring, 4);
        let (new, advance) = serve(&mut cursor, &ring, 4, 0);
        assert!(new.is_empty());
        assert_eq!(advance.dropped, 0);
    }

    /// The window is the newest `push_limit` unseen entries; the older unseen
    /// ones it never served are the drops the advance reports.
    #[test]
    fn a_window_clamped_by_push_depth_drops_oldest() {
        let ring = ring_of(8, &["a", "b", "c", "d"]);
        let mut cursor = SubscriberCursor::primed(&empty(8), 2);
        let (new, advance) = serve(&mut cursor, &ring, 2, 0);
        assert_eq!(new, vec!["c", "d"]);
        assert_eq!(advance.dropped, 2);
        assert_eq!(advance.noise_charge, 2, "a and b are still retained");
    }

    /// `retain_limit` above `push_limit` widens the window without widening
    /// what is new: the extra unseen entries are served as context, so nothing
    /// is charged for them.
    #[test]
    fn unseen_entries_served_as_context_are_not_dropped() {
        let ring = ring_of(8, &["a", "b", "c", "d"]);
        let mut cursor = SubscriberCursor::primed(&empty(8), 1);
        let window = cursor.window(&ring, 1, 4);
        assert_eq!(
            window.entries.iter().map(|e| e.message).collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
        assert_eq!(window.new_entries().len(), 1);
        let (through, seen_floor) = window.advance_span().expect("the window served entries");
        let advance = cursor.advance(&ring, through, seen_floor);
        assert_eq!(advance.dropped, 0, "the window served every unseen entry");
        assert_eq!(cursor.next_owed(), 5);
    }

    /// `retain_limit` below `push_limit` back-fills with seen entries as
    /// context up to the window bound.
    #[test]
    fn a_window_back_fills_with_seen_context() {
        let mut ring = ring_of(8, &["a", "b", "c"]);
        let mut cursor = SubscriberCursor::primed(&ring, 4);
        serve(&mut cursor, &ring, 4, 0);
        ring.append("d");
        let window = cursor.window(&ring, 4, 0);
        assert_eq!(
            window.entries.iter().map(|e| e.message).collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
        assert_eq!(window.new_from, 3);
        assert_eq!(window.new_entries()[0].message, "d");
    }

    #[test]
    fn eviction_reports_without_moving_the_cursor() {
        let mut ring = ring_of(2, &["a"]);
        let cursor = SubscriberCursor::primed(&empty(2), 8);
        let before = retention_frontier(&ring);
        for m in ["b", "c", "d"] {
            ring.append(m);
        }
        // The ring retains only c,d; a and b were pushed out from under the
        // cursor, which is still at 1.
        assert_eq!(cursor.evicted_since(&ring, before), 2);
        assert_eq!(cursor.next_owed(), 1);
    }

    /// Each eviction reports only the span it evicted, so a wedged subscriber
    /// is reported against once per lost message however many appends it took.
    #[test]
    fn successive_evictions_do_not_double_report() {
        let mut ring = ring_of(2, &["a", "b"]);
        let cursor = SubscriberCursor::primed(&empty(2), 8);
        let mut reported = 0;
        for m in ["c", "d", "e"] {
            let before = retention_frontier(&ring);
            ring.append(m);
            reported += cursor.evicted_since(&ring, before);
        }
        assert_eq!(reported, 3, "a, b and c, each reported once");
    }

    /// A sampled subscriber is never delivered to, so it is never reported
    /// against either.
    #[test]
    fn a_sampled_cursor_is_never_reported_against() {
        let mut ring = ring_of(1, &["a"]);
        let cursor = SubscriberCursor::primed(&empty(1), 0);
        let before = retention_frontier(&ring);
        ring.append("b");
        assert_eq!(cursor.evicted_since(&ring, before), 0);
        assert!(!cursor.has_deliverable(&ring));
    }

    /// The two reports are separate streams: the guest-visible `dropped` counts
    /// every seq the cursor passed unserved, while the noise charge excludes
    /// what an eviction already reported.
    #[test]
    fn advance_charges_noise_only_for_still_retained_losses() {
        let mut ring = ring_of(4, &["a"]);
        let mut cursor = SubscriberCursor::primed(&empty(4), 1);
        for m in ["b", "c", "d", "e"] {
            ring.append(m);
        }
        // `a` was evicted; b,c,d are retained but the depth-1 window serves
        // only `e`.
        let (new, advance) = serve(&mut cursor, &ring, 1, 0);
        assert_eq!(new, vec!["e"]);
        assert_eq!(advance.dropped, 4, "a, b, c and d were never served");
        assert_eq!(advance.noise_charge, 3, "the eviction reported `a` itself");
    }

    #[test]
    fn a_window_read_moves_nothing() {
        let ring = ring_of(8, &["a", "b", "c"]);
        let cursor = SubscriberCursor::primed(&empty(8), 4);
        let window = cursor.window(&ring, 4, 0);
        assert_eq!(window.new_entries().len(), 3);
        assert_eq!(window.entries.first().map(|e| e.seq), Some(1));
        assert_eq!(cursor.window(&ring, 4, 0), window);
        assert_eq!(cursor.next_owed(), 1);
        assert!(cursor.has_deliverable(&ring));
    }

    /// Advancing over a prefix leaves the rest unseen, so a delivery that got
    /// partway through re-serves the remainder rather than losing it.
    #[test]
    fn advancing_over_a_prefix_leaves_the_rest_unseen() {
        let ring = ring_of(8, &["a", "b", "c"]);
        let mut cursor = SubscriberCursor::primed(&empty(8), 4);
        let window = cursor.window(&ring, 4, 0);
        let accepted = &window.entries[..2];
        let advance = cursor.advance(&ring, accepted[1].seq, accepted[0].seq);
        assert_eq!(advance.dropped, 0);
        let next = cursor.window(&ring, 4, 0);
        assert_eq!(next.new_entries().len(), 1);
        assert_eq!(next.new_entries()[0].message, "c");
    }

    #[test]
    fn a_failed_delivery_advances_nothing() {
        let ring = ring_of(8, &["a", "b"]);
        let mut cursor = SubscriberCursor::primed(&empty(8), 4);
        let before = cursor.clone();
        let advance = cursor.advance(&ring, cursor.next_owed() - 1, cursor.next_owed());
        assert_eq!(advance.dropped, 0);
        assert_eq!(cursor, before);
        assert!(cursor.has_deliverable(&ring));
    }

    /// Re-advancing over a window already passed reports nothing a second time.
    #[test]
    fn advance_is_idempotent() {
        let ring = ring_of(8, &["a", "b", "c"]);
        let mut cursor = SubscriberCursor::primed(&empty(8), 1);
        let (_, first) = serve(&mut cursor, &ring, 1, 0);
        assert_eq!(first.dropped, 2);
        let advance = cursor.advance(&ring, 3, 3);
        assert_eq!(advance.dropped, 0);
        assert_eq!(cursor.next_owed(), 4);
    }

    #[test]
    fn has_deliverable_tracks_unseen_and_retained() {
        let mut ring = empty(8);
        let mut cursor = SubscriberCursor::primed(&ring, 4);
        assert!(!cursor.has_deliverable(&ring));
        ring.append("a");
        assert!(cursor.has_deliverable(&ring));
        serve(&mut cursor, &ring, 4, 0);
        assert!(!cursor.has_deliverable(&ring));
    }

    #[test]
    fn push_depth_zero_serves_nothing_and_charges_nothing() {
        let ring = ring_of(8, &["a", "b"]);
        let cursor = SubscriberCursor::primed(&empty(8), 0);
        assert!(!cursor.has_deliverable(&ring));
        let window = cursor.window(&ring, 0, 0);
        assert!(window.entries.is_empty());
        assert_eq!(window.new_from, 0);
    }

    /// A sampled subscriber that still asks for context sees it, and none of it
    /// is new.
    #[test]
    fn push_depth_zero_with_retention_is_all_context() {
        let ring = ring_of(8, &["a", "b"]);
        let cursor = SubscriberCursor::primed(&empty(8), 0);
        let window = cursor.window(&ring, 0, 4);
        assert_eq!(window.entries.len(), 2);
        assert_eq!(window.new_from, 2);
        assert_eq!(window.new_len(), 0);
    }

    #[test]
    fn set_push_depth_applies_from_the_next_window() {
        let ring = ring_of(8, &["a", "b", "c"]);
        let mut cursor = SubscriberCursor::primed(&empty(8), 1);
        cursor.set_push_depth(4);
        let (new, advance) = serve(&mut cursor, &ring, 4, 0);
        assert_eq!(new, vec!["a", "b", "c"]);
        assert_eq!(advance.dropped, 0);
    }

    #[test]
    #[should_panic(expected = "is above the window it came from")]
    fn a_seen_floor_above_its_window_panics() {
        let ring = ring_of(8, &["a", "b"]);
        let mut cursor = SubscriberCursor::primed(&empty(8), 4);
        cursor.advance(&ring, 1, 3);
    }

    fn window_of(seqs: &[u64]) -> Vec<Retained<&'static str>> {
        seqs.iter()
            .map(|seq| Retained {
                seq: *seq,
                message: "m",
            })
            .collect()
    }

    fn suffix_seqs(window: Vec<Retained<&'static str>>, resume: u64) -> (Vec<u64>, u64) {
        let (suffix, lost) = gap_suffix(window, resume);
        (suffix.into_iter().map(|r| r.seq).collect(), lost)
    }

    #[test]
    fn gap_suffix_of_an_empty_window_serves_and_loses_nothing() {
        assert_eq!(suffix_seqs(window_of(&[]), 7), (vec![], 0));
    }

    /// Nothing above the resume point means the position does not move, so
    /// nothing was stepped past — a window entirely of copies the consumer
    /// already holds is not a loss.
    #[test]
    fn a_window_at_or_below_the_resume_serves_and_loses_nothing() {
        assert_eq!(suffix_seqs(window_of(&[5, 6, 7]), 7), (vec![], 0));
    }

    #[test]
    fn an_adjacent_suffix_loses_nothing() {
        assert_eq!(suffix_seqs(window_of(&[5, 6, 7, 8]), 7), (vec![8], 0));
    }

    /// The interior span between the resume point and the oldest surviving
    /// entry is the loss, counted the way an advance counts what it passed
    /// unserved.
    #[test]
    fn an_interior_loss_counts_the_span_below_the_suffix() {
        assert_eq!(suffix_seqs(window_of(&[12, 13]), 7), (vec![12, 13], 4));
    }

    #[test]
    fn a_window_wholly_above_the_resume_is_served_whole() {
        assert_eq!(suffix_seqs(window_of(&[1, 2, 3]), 0), (vec![1, 2, 3], 0));
    }

    /// A sampled cursor is never delivered to, so there is no position for an
    /// advance to move. Tolerating the call would let a demoted subscriber
    /// silently keep consuming a queue the model says it does not have.
    #[test]
    #[should_panic(expected = "advance over a sampled subscriber")]
    fn advancing_a_sampled_cursor_panics() {
        let ring = ring_of(8, &["a", "b"]);
        let mut cursor = SubscriberCursor::primed(&empty(8), 0);
        cursor.advance(&ring, 2, 1);
    }
}

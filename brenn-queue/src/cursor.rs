//! The per-subscriber cursor: one subscriber's delivery obligation against a
//! shared retained ring.

use crate::ring::{Retained, RetainedRing};

/// What a cursor take produced: the messages owed to this subscriber now, and
/// how many owed messages were retired without delivery since the previous take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Take<M> {
    /// Owed messages, oldest first, at most `push_depth` of them.
    pub messages: Vec<M>,
    /// Owed messages this take skipped, plus any skipped by earlier takes and
    /// not yet reported. Overflow retires the delivery obligation, never the
    /// message body: a skipped body stays readable in the ring for as long as
    /// the ring's depth covers it.
    pub dropped: u64,
}

/// What a subscriber is owed right now, read without moving its position.
///
/// The messages carry their sequence numbers because a consumer that settles
/// later must name what it accepted: the position only moves when
/// [`SubscriberCursor::settle`] is told how far delivery actually got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peek<M> {
    /// Owed messages, oldest first, at most `push_depth` of them.
    pub messages: Vec<Retained<M>>,
    /// Owed retained messages older than `messages` — the ones the push-depth
    /// window leaves behind. Nothing is charged for them until the window is
    /// settled past them.
    pub clamped: u64,
    /// The highest owed sequence the ring currently holds, or `None` when
    /// nothing is owed. Settling through it consumes the whole owed window,
    /// clamped messages included.
    pub owed_through: Option<u64>,
}

/// One subscriber's position on a channel, plus its overflow accounting.
///
/// A cursor is a position, not a copy of the queue: the messages live once in
/// the channel's [`RetainedRing`] and every subscriber reads them from there.
/// That is what makes an overflow *this subscriber's* accountable drop rather
/// than an anonymous channel-wide loss, which is what the noise ladder needs to
/// escalate against the right party.
///
/// Two things retire an obligation without delivering it: a take clamped by
/// `push_depth` (the subscriber fell behind by more than one activation's
/// worth), and the ring evicting an owed message before the subscriber read it
/// (the subscriber fell behind by more than the ring's depth).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriberCursor {
    /// The lowest seq this subscriber has not yet been delivered or charged a
    /// drop for.
    next_owed: u64,
    /// The most messages one take may deliver.
    push_depth: u64,
    /// Lifetime drop count, monotone. Never reset — a take reports the delta.
    dropped: u64,
    /// The `dropped` value as of the previous take.
    reported: u64,
}

impl SubscriberCursor {
    /// A cursor owing nothing that already exists — the position for a
    /// subscriber that must see only what is published from now on.
    pub fn at_head<M: Clone, Ep: Copy + PartialEq>(
        ring: &RetainedRing<M, Ep>,
        push_depth: u64,
    ) -> Self {
        Self::new(ring.newest_seq(), push_depth)
    }

    /// A cursor owing the channel's retained tail, capped at `push_depth` — the
    /// position for a queue that has just come into existence.
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
            dropped: 0,
            reported: 0,
        }
    }

    /// The lowest seq not yet delivered or charged as dropped.
    pub fn next_owed(&self) -> u64 {
        self.next_owed
    }

    pub fn push_depth(&self) -> u64 {
        self.push_depth
    }

    /// Retune the delivery bound. Takes effect at the next take; it does not
    /// retroactively re-charge or forgive drops already counted.
    pub fn set_push_depth(&mut self, push_depth: u64) {
        self.push_depth = push_depth;
    }

    /// Lifetime drops, including any not yet reported by a take.
    pub fn dropped_total(&self) -> u64 {
        self.dropped
    }

    /// Whether the ring holds anything this subscriber is owed and can still be
    /// delivered. A subscriber whose entire owed range was evicted has no
    /// deliverable work, so this is false even though a drop is pending.
    pub fn has_deliverable<M: Clone, Ep: Copy + PartialEq>(
        &self,
        ring: &RetainedRing<M, Ep>,
    ) -> bool {
        // The ring's entries are a dense ascending run ending at the newest
        // assigned seq, so the newest one alone answers this.
        self.push_depth > 0 && !ring.is_empty() && ring.newest_seq() >= self.next_owed
    }

    /// Charge, without delivering, every owed message the ring no longer holds,
    /// and report how many that was.
    ///
    /// The return value *is* the report: the charged drops are marked as handed
    /// to this caller, so a later [`SubscriberCursor::take`] does not count them
    /// again. Calling this from the append that evicted them is what makes an
    /// eviction accountable when it happens rather than whenever the subscriber
    /// next runs — a subscriber that never takes still has its losses reported.
    ///
    /// Idempotent: a second call against the same ring charges nothing and
    /// reports `0`.
    pub fn charge_evicted<M: Clone, Ep: Copy + PartialEq>(
        &mut self,
        ring: &RetainedRing<M, Ep>,
    ) -> u64 {
        let first_available = match ring.oldest_seq() {
            Some(seq) => seq,
            // Nothing retained: everything assigned and not yet read is gone.
            None => ring.newest_seq() + 1,
        };
        if first_available <= self.next_owed {
            return 0;
        }
        let charged = first_available - self.next_owed;
        self.dropped = self.dropped.saturating_add(charged);
        self.reported = self.reported.saturating_add(charged);
        self.next_owed = first_available;
        charged
    }

    /// What this subscriber is owed, up to `push_depth` messages, without
    /// moving its position or charging anything.
    ///
    /// For a consumer that must prove delivery before its queue advances: it
    /// peeks, delivers, and then settles exactly what the far end accepted, so
    /// a message is never consumed by a delivery attempt that failed. A
    /// consumer whose read is its own acknowledgement uses
    /// [`SubscriberCursor::take`] instead, which is this plus a settle of the
    /// whole owed window.
    ///
    /// Messages evicted from under this subscriber are simply absent from the
    /// window; they are charged when the window is settled, since charging is a
    /// mutation and this is a read.
    pub fn peek<M: Clone, Ep: Copy + PartialEq>(&self, ring: &RetainedRing<M, Ep>) -> Peek<M> {
        // `next_owed` is at least 1, and `since` is exclusive, so this is the
        // owed suffix. Only the delivered part of it is ever visited.
        let owed = ring.since(self.next_owed - 1);
        let owed_len = owed.len();
        let depth = usize::try_from(self.push_depth).unwrap_or(usize::MAX);
        let skip = owed_len.saturating_sub(depth);
        let owed_through = (owed_len > 0).then(|| ring.newest_seq());
        Peek {
            messages: owed.skip(skip).cloned().collect(),
            clamped: u64::try_from(skip).unwrap_or(u64::MAX),
            owed_through,
        }
    }

    /// Advance past everything up to and including `through`, of which
    /// `delivered` messages actually reached the subscriber, and report the
    /// drop delta.
    ///
    /// Everything in the settled span that was not delivered is charged: a
    /// consumer that accepts a window clamped by `push_depth` is accepting that
    /// the older owed messages are gone, which is the same drop-oldest rule the
    /// ring itself follows. Settling through a sequence at or below the current
    /// position advances nothing — a consumer that accepted nothing keeps its
    /// obligations.
    ///
    /// The return value is the report, exactly as [`SubscriberCursor::take`]'s
    /// is: whatever it counts is not counted again by a later call.
    pub fn settle<M: Clone, Ep: Copy + PartialEq>(
        &mut self,
        ring: &RetainedRing<M, Ep>,
        through: u64,
        delivered: u64,
    ) -> u64 {
        // Whatever this charges is reported here and nowhere else. In a host
        // that charges evictions as they happen it is always 0; a host that does
        // not still gets the count, exactly once.
        let evicted = self.charge_evicted(ring);
        if through >= self.next_owed {
            let span = through - self.next_owed + 1;
            self.dropped = self.dropped.saturating_add(span.saturating_sub(delivered));
            self.next_owed = through + 1;
        }
        let dropped = evicted + (self.dropped - self.reported);
        self.reported = self.dropped;
        dropped
    }

    /// Take up to `push_depth` owed messages and report the drop delta.
    ///
    /// When more than `push_depth` messages are owed, the *newest* ones are
    /// delivered and the older ones are charged as drops: a subscriber woken
    /// late can still act on the freshest messages, which is the same
    /// drop-oldest rule the ring itself follows.
    ///
    /// The read *is* the acknowledgement — the position moves past the whole
    /// owed window whether or not the messages reach their destination.
    pub fn take<M: Clone, Ep: Copy + PartialEq>(&mut self, ring: &RetainedRing<M, Ep>) -> Take<M> {
        let peeked = self.peek(ring);
        let delivered = u64::try_from(peeked.messages.len()).unwrap_or(u64::MAX);
        // Nothing owed: settle at the current position, which advances nothing
        // and reports only evictions.
        let through = peeked.owed_through.unwrap_or(self.next_owed - 1);
        let dropped = self.settle(ring, through, delivered);
        Take {
            messages: peeked.messages.into_iter().map(|e| e.message).collect(),
            dropped,
        }
    }
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

    #[test]
    fn at_head_owes_nothing_already_published() {
        let mut ring = ring_of(8, &["a", "b"]);
        let mut cursor = SubscriberCursor::at_head(&ring, 4);
        assert_eq!(cursor.take(&ring).messages, Vec::<&str>::new());
        ring.append("c");
        let take = cursor.take(&ring);
        assert_eq!(take.messages, vec!["c"]);
        assert_eq!(take.dropped, 0);
    }

    #[test]
    fn primed_owes_the_retained_tail_capped_by_push_depth() {
        let ring = ring_of(8, &["a", "b", "c", "d"]);
        let mut cursor = SubscriberCursor::primed(&ring, 2);
        let take = cursor.take(&ring);
        assert_eq!(take.messages, vec!["c", "d"]);
        assert_eq!(take.dropped, 0);
    }

    #[test]
    fn primed_on_empty_ring_takes_nothing() {
        let ring = ring_of(8, &[]);
        let mut cursor = SubscriberCursor::primed(&ring, 4);
        assert!(cursor.take(&ring).messages.is_empty());
        assert_eq!(cursor.dropped_total(), 0);
    }

    #[test]
    fn take_clamped_by_push_depth_drops_oldest() {
        let ring = ring_of(8, &["a", "b", "c", "d"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(8, &[]), 2);
        let take = cursor.take(&ring);
        assert_eq!(take.messages, vec!["c", "d"]);
        assert_eq!(take.dropped, 2);
    }

    #[test]
    fn eviction_from_the_ring_is_charged_as_a_drop() {
        let mut ring = ring_of(2, &["a"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(2, &[]), 8);
        for m in ["b", "c", "d"] {
            ring.append(m);
        }
        // The ring retains only c,d; a and b were evicted while owed.
        let take = cursor.take(&ring);
        assert_eq!(take.messages, vec!["c", "d"]);
        assert_eq!(take.dropped, 2);
    }

    #[test]
    fn drop_delta_is_reported_once() {
        let ring = ring_of(8, &["a", "b", "c"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(8, &[]), 1);
        assert_eq!(cursor.take(&ring).dropped, 2);
        assert_eq!(cursor.take(&ring).dropped, 0);
        assert_eq!(cursor.dropped_total(), 2);
    }

    #[test]
    fn charge_evicted_is_idempotent() {
        let mut ring = ring_of(1, &["a"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(1, &[]), 4);
        ring.append("b");
        assert_eq!(cursor.charge_evicted(&ring), 1);
        assert_eq!(cursor.charge_evicted(&ring), 0);
        assert_eq!(cursor.dropped_total(), 1);
    }

    /// An eviction charged as it happens is reported by that call, so the next
    /// take does not report it a second time — the count reaches a noise ladder
    /// exactly once whichever host charges it.
    #[test]
    fn a_charged_eviction_is_not_reported_again_by_the_next_take() {
        let mut ring = ring_of(2, &["a"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(2, &[]), 8);
        for m in ["b", "c", "d"] {
            ring.append(m);
            cursor.charge_evicted(&ring);
        }
        assert_eq!(cursor.dropped_total(), 2);
        let take = cursor.take(&ring);
        assert_eq!(take.messages, vec!["c", "d"]);
        assert_eq!(take.dropped, 0, "both drops were reported at eviction time");
    }

    /// Eviction drops charged eagerly and clamp drops charged by the take are
    /// separate reports: the take carries only what it charged itself.
    #[test]
    fn take_reports_only_its_own_clamp_after_an_eager_eviction_charge() {
        let mut ring = ring_of(4, &["a"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(4, &[]), 1);
        for m in ["b", "c", "d", "e"] {
            ring.append(m);
            cursor.charge_evicted(&ring);
        }
        // `a` was evicted while owed; b,c,d,e are retained but the depth-1 take
        // delivers only `e`.
        assert_eq!(cursor.dropped_total(), 1);
        let take = cursor.take(&ring);
        assert_eq!(take.messages, vec!["e"]);
        assert_eq!(take.dropped, 3, "b, c and d, clamped by push_depth 1");
        assert_eq!(cursor.dropped_total(), 4);
    }

    #[test]
    fn peek_reads_the_owed_window_without_moving_the_position() {
        let ring = ring_of(8, &["a", "b", "c"]);
        let cursor = SubscriberCursor::at_head(&ring_of(8, &[]), 4);
        let peeked = cursor.peek(&ring);
        assert_eq!(
            peeked
                .messages
                .iter()
                .map(|e| e.message)
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert_eq!(peeked.messages.first().map(|e| e.seq), Some(1));
        assert_eq!(peeked.clamped, 0);
        assert_eq!(peeked.owed_through, Some(3));
        // Nothing moved and nothing was charged: a second peek answers the same.
        assert_eq!(cursor.peek(&ring), peeked);
        assert_eq!(cursor.dropped_total(), 0);
        assert!(cursor.has_deliverable(&ring));
    }

    #[test]
    fn peek_reports_what_the_push_depth_window_leaves_behind() {
        let ring = ring_of(8, &["a", "b", "c", "d"]);
        let cursor = SubscriberCursor::at_head(&ring_of(8, &[]), 2);
        let peeked = cursor.peek(&ring);
        assert_eq!(
            peeked
                .messages
                .iter()
                .map(|e| e.message)
                .collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        assert_eq!(peeked.clamped, 2, "a and b are older than the window");
        assert_eq!(peeked.owed_through, Some(4));
    }

    /// Settling only what was accepted leaves the rest owed, so a delivery that
    /// got partway through redelivers the remainder rather than losing it.
    #[test]
    fn settling_a_prefix_leaves_the_rest_owed_and_charges_nothing() {
        let ring = ring_of(8, &["a", "b", "c"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(8, &[]), 4);
        let peeked = cursor.peek(&ring);
        let accepted = &peeked.messages[..2];
        let dropped = cursor.settle(&ring, accepted.last().unwrap().seq, 2);
        assert_eq!(dropped, 0);
        assert_eq!(cursor.dropped_total(), 0);
        assert_eq!(
            cursor
                .peek(&ring)
                .messages
                .iter()
                .map(|e| e.message)
                .collect::<Vec<_>>(),
            vec!["c"]
        );
    }

    /// Accepting a clamped window is accepting that the older owed messages are
    /// gone — they are charged when the window is settled past them, not when
    /// it is read.
    #[test]
    fn settling_a_clamped_window_charges_what_it_skipped() {
        let ring = ring_of(8, &["a", "b", "c", "d"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(8, &[]), 2);
        let peeked = cursor.peek(&ring);
        assert_eq!(cursor.dropped_total(), 0, "the read charges nothing");
        let dropped = cursor.settle(&ring, peeked.owed_through.unwrap(), 2);
        assert_eq!(dropped, 2);
        assert_eq!(cursor.dropped_total(), 2);
        assert!(!cursor.has_deliverable(&ring));
    }

    #[test]
    fn a_failed_delivery_settles_nothing_and_keeps_its_obligations() {
        let ring = ring_of(8, &["a", "b"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(8, &[]), 4);
        let before = cursor.clone();
        assert_eq!(cursor.settle(&ring, cursor.next_owed() - 1, 0), 0);
        assert_eq!(cursor, before);
        assert!(cursor.has_deliverable(&ring));
    }

    #[test]
    fn settle_charges_evictions_that_happened_under_the_subscriber() {
        let mut ring = ring_of(2, &["a"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(2, &[]), 8);
        for m in ["b", "c"] {
            ring.append(m);
        }
        // `a` was evicted while owed; the peek simply does not show it.
        let peeked = cursor.peek(&ring);
        assert_eq!(
            peeked
                .messages
                .iter()
                .map(|e| e.message)
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );
        assert_eq!(cursor.settle(&ring, peeked.owed_through.unwrap(), 2), 1);
        assert_eq!(cursor.dropped_total(), 1);
    }

    #[test]
    fn has_deliverable_tracks_owed_and_retained() {
        let mut ring = ring_of(8, &[]);
        let mut cursor = SubscriberCursor::at_head(&ring, 4);
        assert!(!cursor.has_deliverable(&ring));
        ring.append("a");
        assert!(cursor.has_deliverable(&ring));
        cursor.take(&ring);
        assert!(!cursor.has_deliverable(&ring));
    }

    #[test]
    fn push_depth_zero_delivers_nothing_and_charges_everything() {
        let ring = ring_of(8, &["a", "b"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(8, &[]), 0);
        assert!(!cursor.has_deliverable(&ring));
        let take = cursor.take(&ring);
        assert!(take.messages.is_empty());
        assert_eq!(take.dropped, 2);
    }

    #[test]
    fn set_push_depth_applies_from_the_next_take() {
        let ring = ring_of(8, &["a", "b", "c"]);
        let mut cursor = SubscriberCursor::at_head(&ring_of(8, &[]), 1);
        cursor.set_push_depth(4);
        let take = cursor.take(&ring);
        assert_eq!(take.messages, vec!["a", "b", "c"]);
        assert_eq!(take.dropped, 0);
    }
}

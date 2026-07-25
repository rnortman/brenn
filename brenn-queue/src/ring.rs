//! The bounded, drop-oldest retained ring and its replay/gap computation.

use std::collections::VecDeque;

/// One retained message plus the dense per-channel sequence number the ring
/// assigned it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retained<M> {
    /// Dense and ascending from 1 within an epoch. Assigned by the ring, never
    /// carried inside the message.
    pub seq: u64,
    pub message: M,
}

/// A subscriber's resume position on a channel: the epoch it was reading and the
/// last sequence number it saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resume<Ep> {
    pub epoch: Ep,
    pub seq: u64,
}

/// Why a replay carries a discontinuity the subscriber must be told about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapReason {
    /// The resume epoch differs from the ring's — the ring's host restarted, or
    /// the resume came from a different instance.
    EpochChanged,
    /// The messages between the subscriber's last-seen seq and the oldest
    /// retained entry have been evicted — the store no longer holds the history
    /// the cursor resumes from. Store-neutral: the same condition on a ring
    /// whose depth dropped them and on a durable channel whose window reaped
    /// them.
    BeyondRetained,
    /// The subscriber claims a seq this epoch never assigned. Not reachable for
    /// an honest subscriber; the distinct reason lets a transport escalate it as
    /// a protocol violation.
    ResumeAhead,
}

/// What a replay computation concluded, alongside the messages it produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayDecision {
    /// No resume position was supplied: the whole retained window, no gap.
    Fresh,
    /// The resume position matched the newest assigned seq: nothing owed.
    UpToDate,
    /// Exactly the messages after the resume position, no gap.
    Exact,
    /// A discontinuity: the whole retained window, plus a gap signal.
    Gap(GapReason),
}

/// The result of a replay: the messages (oldest first, seq ascending) and the
/// decision that produced them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replay<M> {
    pub messages: Vec<Retained<M>>,
    pub decision: ReplayDecision,
}

/// What an append did: the seq it assigned, and how many oldest entries the
/// depth bound evicted to make room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Append {
    pub seq: u64,
    pub evicted: u64,
}

/// One channel's retained window: the most recent messages, bounded by depth,
/// drop-oldest, with dense sequence numbering and typed gap detection on resume.
///
/// The ring is the channel's *view*, independent of any subscriber's delivery
/// obligation: a message evicted from a subscriber's pending set is still
/// readable here for as long as depth covers it, which is why loss inside the
/// depth window needs no gap vocabulary.
///
/// `Ep` is the epoch identity — whatever token the host mints per lifetime of
/// the ring's contents. Comparing two of them is all this crate does with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedRing<M, Ep> {
    epoch: Ep,
    depth: u64,
    /// The next seq to assign. Starts at 1, so an assigned seq is always `>= 1`
    /// and `0` unambiguously means "nothing assigned yet".
    next_seq: u64,
    entries: VecDeque<Retained<M>>,
}

impl<M: Clone, Ep: Copy + PartialEq> RetainedRing<M, Ep> {
    /// A ring holding at most `depth` messages under epoch identity `epoch`.
    /// A `depth` of 0 retains nothing while still assigning sequence numbers.
    pub fn new(epoch: Ep, depth: u64) -> Self {
        Self {
            epoch,
            depth,
            next_seq: 1,
            entries: VecDeque::new(),
        }
    }

    pub fn epoch(&self) -> Ep {
        self.epoch
    }

    pub fn depth(&self) -> u64 {
        self.depth
    }

    /// Retune the bound, trimming in place if it shrank. What the ring already
    /// holds is still honest history, so a grow keeps it and a shrink drops the
    /// oldest.
    pub fn set_depth(&mut self, depth: u64) {
        self.depth = depth;
        self.trim();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The highest seq ever assigned in this epoch, or 0 if none has been.
    /// Distinct from the newest *retained* seq only when depth is 0.
    pub fn newest_seq(&self) -> u64 {
        self.next_seq - 1
    }

    /// The oldest still-retained seq, or `None` when the ring holds nothing.
    pub fn oldest_seq(&self) -> Option<u64> {
        self.entries.front().map(|e| e.seq)
    }

    /// Assign the next seq, retain the message, and evict oldest to stay within
    /// depth.
    pub fn append(&mut self, message: M) -> Append {
        let seq = self.next_seq;
        self.next_seq += 1;
        if self.depth == 0 {
            return Append { seq, evicted: 0 };
        }
        self.entries.push_back(Retained { seq, message });
        Append {
            seq,
            evicted: self.trim(),
        }
    }

    /// Every retained entry, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = &Retained<M>> {
        self.entries.iter()
    }

    /// The most recent `n` retained entries, oldest first.
    pub fn tail(&self, n: u64) -> impl Iterator<Item = &Retained<M>> {
        let take = usize::try_from(n)
            .unwrap_or(usize::MAX)
            .min(self.entries.len());
        self.entries.iter().skip(self.entries.len() - take)
    }

    /// The retained entries strictly after `seq`, oldest first.
    ///
    /// The entries are a dense ascending run, so the starting point is
    /// arithmetic and the iterator visits only the answer.
    pub fn since(&self, seq: u64) -> impl ExactSizeIterator<Item = &Retained<M>> {
        self.entries.iter().skip(self.offset_after(seq))
    }

    /// Index of the first retained entry with a seq above `seq`, or `len()` when
    /// the ring holds nothing that new.
    fn offset_after(&self, seq: u64) -> usize {
        let Some(front) = self.entries.front() else {
            return 0;
        };
        let skip = seq.saturating_add(1).saturating_sub(front.seq);
        usize::try_from(skip)
            .unwrap_or(usize::MAX)
            .min(self.entries.len())
    }

    /// The seq a subscriber must treat as already-seen in order to be owed at
    /// most `depth` messages from the current retained window — the priming
    /// position for a queue that has just come into existence.
    ///
    /// A queue primed here is owed the retained tail capped at `depth`, so the
    /// tail arrives as new to a consumer that attached after it was published.
    pub fn primed_from(&self, depth: u64) -> u64 {
        match self.tail(depth).next() {
            Some(first) => first.seq - 1,
            // Nothing retained (empty ring, or depth 0 on either side): owe
            // nothing that already exists.
            None => self.newest_seq(),
        }
    }

    /// Compute what a subscriber attaching with `resume` is owed, and whether
    /// its continuity broke.
    ///
    /// Every gap arm replays the whole retained window: the subscriber's
    /// position is untrustworthy, so the best recovery is the freshest history
    /// the ring still has, announced as a gap.
    pub fn replay(&self, resume: Option<Resume<Ep>>) -> Replay<M> {
        let whole = || self.entries.iter().cloned().collect::<Vec<_>>();

        let Some(resume) = resume else {
            return Replay {
                messages: whole(),
                decision: ReplayDecision::Fresh,
            };
        };

        if resume.epoch != self.epoch {
            return Replay {
                messages: whole(),
                decision: ReplayDecision::Gap(GapReason::EpochChanged),
            };
        }

        let newest = self.newest_seq();
        if resume.seq > newest {
            return Replay {
                messages: whole(),
                decision: ReplayDecision::Gap(GapReason::ResumeAhead),
            };
        }
        if resume.seq == newest {
            return Replay {
                messages: Vec::new(),
                decision: ReplayDecision::UpToDate,
            };
        }

        // Some messages are owed. The ring covers the hole exactly when its
        // oldest retained seq is at or before the first owed seq.
        match self.entries.front() {
            Some(oldest) if oldest.seq <= resume.seq + 1 => Replay {
                messages: self.since(resume.seq).cloned().collect(),
                decision: ReplayDecision::Exact,
            },
            _ => Replay {
                messages: whole(),
                decision: ReplayDecision::Gap(GapReason::BeyondRetained),
            },
        }
    }

    /// Drop oldest entries until the ring is within depth; returns how many
    /// went. Depth is a `u64` from config while `len` is a `usize`; the compare
    /// happens in `u64` so it stays exact on 32-bit targets.
    fn trim(&mut self) -> u64 {
        let mut evicted = 0;
        while self.entries.len() as u64 > self.depth {
            self.entries.pop_front();
            evicted += 1;
        }
        evicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(depth: u64) -> RetainedRing<&'static str, u8> {
        RetainedRing::new(1, depth)
    }

    #[test]
    fn append_assigns_dense_seqs_from_one() {
        let mut r = ring(8);
        let seqs: Vec<u64> = ["a", "b", "c"].map(|m| r.append(m).seq).to_vec();
        assert_eq!(seqs, vec![1, 2, 3]);
        assert_eq!(r.newest_seq(), 3);
        assert_eq!(r.oldest_seq(), Some(1));
    }

    #[test]
    fn depth_bound_drops_oldest_and_counts() {
        let mut r = ring(2);
        assert_eq!(r.append("a").evicted, 0);
        assert_eq!(r.append("b").evicted, 0);
        assert_eq!(r.append("c").evicted, 1);
        assert_eq!(r.len(), 2);
        assert_eq!(
            r.iter().map(|e| e.message).collect::<Vec<_>>(),
            vec!["b", "c"]
        );
    }

    #[test]
    fn depth_zero_retains_nothing_but_still_assigns() {
        let mut r = ring(0);
        assert_eq!(r.append("a").seq, 1);
        assert_eq!(r.append("b").seq, 2);
        assert!(r.is_empty());
        assert_eq!(r.newest_seq(), 2);
        assert_eq!(r.oldest_seq(), None);
    }

    #[test]
    fn set_depth_shrinks_in_place_and_grows_without_loss() {
        let mut r = ring(4);
        for m in ["a", "b", "c", "d"] {
            r.append(m);
        }
        r.set_depth(2);
        assert_eq!(
            r.iter().map(|e| e.message).collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        r.set_depth(4);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn tail_and_since_window_the_ring() {
        let mut r = ring(8);
        for m in ["a", "b", "c", "d"] {
            r.append(m);
        }
        assert_eq!(
            r.tail(2).map(|e| e.message).collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        assert_eq!(r.tail(99).count(), 4);
        assert_eq!(
            r.since(2).map(|e| e.message).collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        assert_eq!(r.since(4).count(), 0);
    }

    #[test]
    fn replay_without_resume_is_fresh_whole_window() {
        let mut r = ring(8);
        for m in ["a", "b"] {
            r.append(m);
        }
        let replay = r.replay(None);
        assert_eq!(replay.decision, ReplayDecision::Fresh);
        assert_eq!(replay.messages.len(), 2);
    }

    #[test]
    fn replay_across_epochs_gaps() {
        let mut r = ring(8);
        r.append("a");
        let replay = r.replay(Some(Resume { epoch: 2, seq: 1 }));
        assert_eq!(
            replay.decision,
            ReplayDecision::Gap(GapReason::EpochChanged)
        );
        assert_eq!(replay.messages.len(), 1);
    }

    #[test]
    fn replay_caught_up_is_empty() {
        let mut r = ring(8);
        r.append("a");
        let replay = r.replay(Some(Resume { epoch: 1, seq: 1 }));
        assert_eq!(replay.decision, ReplayDecision::UpToDate);
        assert!(replay.messages.is_empty());
    }

    #[test]
    fn replay_covered_hole_is_exact() {
        let mut r = ring(8);
        for m in ["a", "b", "c"] {
            r.append(m);
        }
        let replay = r.replay(Some(Resume { epoch: 1, seq: 1 }));
        assert_eq!(replay.decision, ReplayDecision::Exact);
        assert_eq!(
            replay
                .messages
                .iter()
                .map(|e| e.message)
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );
    }

    #[test]
    fn replay_hole_older_than_ring_gaps() {
        let mut r = ring(2);
        for m in ["a", "b", "c", "d"] {
            r.append(m);
        }
        let replay = r.replay(Some(Resume { epoch: 1, seq: 1 }));
        assert_eq!(
            replay.decision,
            ReplayDecision::Gap(GapReason::BeyondRetained)
        );
        assert_eq!(replay.messages.len(), 2);
    }

    #[test]
    fn replay_ahead_of_assigned_range_gaps() {
        let mut r = ring(8);
        r.append("a");
        let replay = r.replay(Some(Resume { epoch: 1, seq: 9 }));
        assert_eq!(replay.decision, ReplayDecision::Gap(GapReason::ResumeAhead));
    }

    #[test]
    fn primed_from_owes_the_capped_retained_tail() {
        let mut r = ring(8);
        for m in ["a", "b", "c", "d"] {
            r.append(m);
        }
        // Capped below the retained window: owe the last two.
        assert_eq!(r.primed_from(2), 2);
        // Cap wider than the window: owe everything retained.
        assert_eq!(r.primed_from(99), 0);
        // Cap of zero: owe nothing that already exists.
        assert_eq!(r.primed_from(0), 4);
    }

    #[test]
    fn primed_from_on_empty_ring_owes_nothing() {
        let r = ring(8);
        assert_eq!(r.primed_from(4), 0);
        let mut zero = ring(0);
        zero.append("a");
        assert_eq!(zero.primed_from(4), 1);
    }
}

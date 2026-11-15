//! The ranges of an unsealed blob that writes have covered.
//!
//! Pure arithmetic, compiled for every host. How many disjoint ranges a
//! writer tracks is each host's own bound.

use std::ops::Range;

/// Disjoint, ascending ranges, adjacent ones merged.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WrittenRanges(Vec<Range<u64>>);

impl WrittenRanges {
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of disjoint ranges.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[Range<u64>] {
        &self.0
    }

    /// These ranges with `new` merged in, overlapping and adjacent ranges
    /// joined. `self` is unchanged, so a caller can weigh the result against
    /// its bound before keeping it.
    pub fn with(&self, new: Range<u64>) -> Self {
        let mut out = Vec::with_capacity(self.0.len() + 1);
        let mut pending = new;
        let mut placed = false;
        for r in &self.0 {
            if placed || r.end < pending.start {
                out.push(r.clone());
            } else if pending.end < r.start {
                out.push(pending.clone());
                out.push(r.clone());
                placed = true;
            } else {
                pending = pending.start.min(r.start)..pending.end.max(r.end);
            }
        }
        if !placed {
            out.push(pending);
        }
        Self(out)
    }

    /// These ranges with `gone` taken out of them.
    pub fn without(&self, gone: Range<u64>) -> Self {
        let mut out = Vec::with_capacity(self.0.len() + 1);
        for r in &self.0 {
            if r.end <= gone.start || gone.end <= r.start {
                out.push(r.clone());
                continue;
            }
            if r.start < gone.start {
                out.push(r.start..gone.start);
            }
            if gone.end < r.end {
                out.push(gone.end..r.end);
            }
        }
        Self(out)
    }

    /// The lowest range below `extent` these ranges do not cover.
    pub fn first_gap(&self, extent: u64) -> Option<Range<u64>> {
        let mut covered_to = 0;
        for r in &self.0 {
            if r.start > covered_to {
                return Some(covered_to..r.start);
            }
            covered_to = r.end;
        }
        (covered_to < extent).then_some(covered_to..extent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(pairs: &[(u64, u64)]) -> WrittenRanges {
        WrittenRanges(pairs.iter().map(|&(s, e)| s..e).collect())
    }

    #[test]
    fn with_merges_overlapping_and_adjacent() {
        assert_eq!(ranges(&[]).with(3..5), ranges(&[(3, 5)]));
        assert_eq!(
            ranges(&[(0, 2), (8, 9)]).with(4..5),
            ranges(&[(0, 2), (4, 5), (8, 9)])
        );
        assert_eq!(ranges(&[(0, 2), (8, 9)]).with(2..8), ranges(&[(0, 9)]));
        assert_eq!(
            ranges(&[(0, 2), (4, 6), (8, 9)]).with(1..5),
            ranges(&[(0, 6), (8, 9)])
        );
        assert_eq!(ranges(&[(3, 4)]).with(0..1), ranges(&[(0, 1), (3, 4)]));
        assert_eq!(ranges(&[(3, 4)]).with(5..6), ranges(&[(3, 4), (5, 6)]));
    }

    #[test]
    fn without_cuts_and_splits() {
        assert_eq!(ranges(&[]).without(3..5), ranges(&[]));
        assert_eq!(ranges(&[(0, 10)]).without(3..5), ranges(&[(0, 3), (5, 10)]));
        assert_eq!(
            ranges(&[(0, 4), (6, 10)]).without(2..8),
            ranges(&[(0, 2), (8, 10)])
        );
        assert_eq!(ranges(&[(0, 4), (6, 10)]).without(0..10), ranges(&[]));
        assert_eq!(
            ranges(&[(0, 4), (6, 10)]).without(4..6),
            ranges(&[(0, 4), (6, 10)])
        );
        assert_eq!(ranges(&[(0, 4)]).without(2..100), ranges(&[(0, 2)]));
    }

    #[test]
    fn first_gap_finds_the_lowest_uncovered_range() {
        assert_eq!(ranges(&[]).first_gap(0), None);
        assert_eq!(ranges(&[(0, 5)]).first_gap(5), None);
        assert_eq!(ranges(&[(2, 5)]).first_gap(5), Some(0..2));
        assert_eq!(ranges(&[(0, 2), (3, 5)]).first_gap(5), Some(2..3));
        assert_eq!(ranges(&[(0, 2)]).first_gap(5), Some(2..5));
    }
}

//! Generic token bucket with whole-interval integer refill.
//!
//! A single reusable rate limiter: a burst capacity that refills by a fixed
//! amount every fixed interval. Refill advances by whole intervals of elapsed
//! time (never wall-clock fractions), so there is no floating-point drift under
//! sustained load. The bucket carries the transition signals a caller needs to
//! log rate-limit entry/exit exactly once per suppression window, but does no
//! logging itself — logging is domain-specific (the caller owns the identity to
//! attribute a flood to) and stays with the caller.
//!
//! Admission is a **sufficiency gate**: a draw of `n` is admitted iff the
//! balance covers `n` whole, and it then deducts exactly `n`. A refused draw
//! changes nothing. The balance is therefore never negative, on any path: the
//! burst is a real bound on an instantaneous spike, not an overdraft limit.
//!
//! An indivisible unit of work larger than the burst is refused, not starved:
//! the caller sizes the bucket so that its largest atomic draw fits. A backstop
//! whose burst sits below the flush it backstops is mis-sized, and the fix
//! belongs at the sizing, not in the arithmetic.
//!
//! Time is measured with `tokio::time::Instant`, so tests drive refill
//! deterministically with `tokio::time::pause`/`advance`.

use std::time::Duration;

use tokio::time::Instant;

/// Outcome of a single [`TokenBucket::try_consume`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenBucketOutcome {
    /// A token was available and consumed; no suppression window was in effect.
    Granted,
    /// A token was consumed, ending a suppression window. Carries the number of
    /// consume attempts denied while suppressed, for the caller's
    /// "limit lifted, N suppressed" log.
    GrantedAfterSuppression { suppressed: u64 },
    /// No token available. `first` is true exactly once per suppression window —
    /// on the attempt that opened it — so the caller logs the transition into
    /// the limited state once, not on every drop.
    Denied { first: bool },
}

/// A burst-capacity bucket refilled by `refill_amount` tokens every
/// `refill_interval`.
pub struct TokenBucket {
    capacity: u32,
    refill_interval: Duration,
    refill_amount: u32,
    /// Never negative: a draw is admitted only when the balance covers it whole.
    tokens: u32,
    /// Advanced by whole intervals, not wall time — avoids drift under load.
    last_refill: Instant,
    /// Attempts denied since the current suppression window opened. Reset to 0
    /// when a token next becomes available.
    suppressed: u64,
    /// True while a suppression window is open (the transition-in signal has
    /// already been reported for it).
    in_suppression: bool,
}

impl TokenBucket {
    /// Create a full bucket: it starts with `capacity` tokens, not limited.
    ///
    /// Panics if `refill_interval` is zero — a zero interval has no meaning and
    /// would divide by zero on refill.
    pub fn new(capacity: u32, refill_interval: Duration, refill_amount: u32) -> Self {
        assert!(
            !refill_interval.is_zero(),
            "TokenBucket refill_interval must be non-zero"
        );
        Self {
            capacity,
            refill_interval,
            refill_amount,
            tokens: capacity,
            last_refill: Instant::now(),
            suppressed: 0,
            in_suppression: false,
        }
    }

    /// Attempt to consume one token, refilling first for whole intervals elapsed
    /// since the last refill.
    ///
    /// The `n = 1` case of [`TokenBucket::try_consume_n`].
    pub fn try_consume(&mut self) -> TokenBucketOutcome {
        self.try_consume_n(1)
    }

    /// Attempt to consume `n` tokens as one indivisible draw, refilling first for
    /// whole intervals elapsed since the last refill.
    ///
    /// Admission is sufficiency: the draw is admitted iff the balance covers `n`
    /// whole, and then deducts exactly `n`. A refused draw leaves the balance
    /// untouched. A draw wider than the burst can therefore never be admitted —
    /// sizing the bucket to the caller's largest atomic draw is the caller's job.
    ///
    /// `n == 0` is a caller bug — a draw that consumes nothing has no meaning
    /// here, and admitting it would report a rate-limit verdict on a
    /// non-existent unit of work.
    pub fn try_consume_n(&mut self, n: u32) -> TokenBucketOutcome {
        assert!(n > 0, "TokenBucket::try_consume_n needs a non-zero draw");
        let elapsed = self.last_refill.elapsed();
        let intervals = elapsed.as_nanos() / self.refill_interval.as_nanos();
        if intervals > 0 {
            let intervals = u32::try_from(intervals).unwrap_or(u32::MAX);
            let refill = self.refill_amount.saturating_mul(intervals);
            self.tokens = self.tokens.saturating_add(refill).min(self.capacity);
            // Advance by exactly the whole intervals consumed; the fractional
            // remainder stays owed to the next refill.
            self.last_refill += self.refill_interval.saturating_mul(intervals);
        }

        if self.tokens >= n {
            self.tokens -= n;
            if self.suppressed > 0 {
                let suppressed = self.suppressed;
                self.suppressed = 0;
                self.in_suppression = false;
                TokenBucketOutcome::GrantedAfterSuppression { suppressed }
            } else {
                TokenBucketOutcome::Granted
            }
        } else {
            self.suppressed = self.suppressed.saturating_add(1);
            let first = !self.in_suppression;
            self.in_suppression = true;
            TokenBucketOutcome::Denied { first }
        }
    }

    /// Attempts denied since the current suppression window opened (0 when not
    /// suppressed).
    pub fn suppressed(&self) -> u64 {
        self.suppressed
    }

    /// Whether a suppression window is currently open.
    pub fn in_suppression(&self) -> bool {
        self.in_suppression
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    #[tokio::test(start_paused = true)]
    async fn starts_full_and_grants_burst() {
        let mut bucket = TokenBucket::new(3, SECOND, 1);
        assert_eq!(bucket.try_consume(), TokenBucketOutcome::Granted);
        assert_eq!(bucket.try_consume(), TokenBucketOutcome::Granted);
        assert_eq!(bucket.try_consume(), TokenBucketOutcome::Granted);
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::Denied { first: true }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn suppression_transition_signals() {
        let mut bucket = TokenBucket::new(1, SECOND, 1);
        assert_eq!(bucket.try_consume(), TokenBucketOutcome::Granted);
        // Window opens on the first denial, stays open on subsequent ones.
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::Denied { first: true }
        );
        assert!(bucket.in_suppression());
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::Denied { first: false }
        );
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::Denied { first: false }
        );
        assert_eq!(bucket.suppressed(), 3);

        // A refill grants once and closes the window, reporting the exact count.
        tokio::time::advance(SECOND).await;
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::GrantedAfterSuppression { suppressed: 3 }
        );
        assert_eq!(bucket.suppressed(), 0);
        assert!(!bucket.in_suppression());
    }

    #[tokio::test(start_paused = true)]
    async fn whole_interval_refill_multiplies_amount() {
        let mut bucket = TokenBucket::new(10, SECOND, 2);
        // Drain the bucket.
        for _ in 0..10 {
            assert_eq!(bucket.try_consume(), TokenBucketOutcome::Granted);
        }
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::Denied { first: true }
        );

        // Three whole intervals refill 3 * 2 = 6 tokens.
        tokio::time::advance(SECOND * 3).await;
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::GrantedAfterSuppression { suppressed: 1 }
        );
        for _ in 0..5 {
            assert_eq!(bucket.try_consume(), TokenBucketOutcome::Granted);
        }
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::Denied { first: true }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refill_caps_at_capacity() {
        let mut bucket = TokenBucket::new(4, SECOND, 3);
        for _ in 0..4 {
            assert_eq!(bucket.try_consume(), TokenBucketOutcome::Granted);
        }
        // A long idle refills far more than capacity, but tokens cap at 4.
        tokio::time::advance(SECOND * 100).await;
        for _ in 0..4 {
            assert!(matches!(
                bucket.try_consume(),
                TokenBucketOutcome::Granted | TokenBucketOutcome::GrantedAfterSuppression { .. }
            ));
        }
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::Denied { first: true }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fractional_time_owes_remainder() {
        let mut bucket = TokenBucket::new(1, SECOND, 1);
        assert_eq!(bucket.try_consume(), TokenBucketOutcome::Granted);
        // Less than a whole interval: no refill yet.
        tokio::time::advance(Duration::from_millis(900)).await;
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::Denied { first: true }
        );
        // Crossing the interval boundary refills exactly one.
        tokio::time::advance(Duration::from_millis(200)).await;
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::GrantedAfterSuppression { suppressed: 1 }
        );
    }

    #[test]
    #[should_panic(expected = "refill_interval must be non-zero")]
    fn zero_interval_panics() {
        TokenBucket::new(1, Duration::ZERO, 1);
    }

    /// A draw the balance covers exactly is admitted and lands on zero.
    #[tokio::test(start_paused = true)]
    async fn an_exact_balance_draw_is_admitted() {
        let mut bucket = TokenBucket::new(10, SECOND, 1);
        assert_eq!(bucket.try_consume_n(10), TokenBucketOutcome::Granted);
        assert_eq!(
            bucket.try_consume(),
            TokenBucketOutcome::Denied { first: true }
        );
    }

    /// One short is refused, and the refusal costs nothing: the very next draw of
    /// the balance that is there succeeds, which it could not if the refused draw
    /// had deducted anything.
    #[tokio::test(start_paused = true)]
    async fn a_draw_one_over_the_balance_is_refused_and_changes_nothing() {
        let mut bucket = TokenBucket::new(10, SECOND, 1);
        assert_eq!(
            bucket.try_consume_n(11),
            TokenBucketOutcome::Denied { first: true }
        );
        // The refusal deducted nothing — the full balance is still drawable. It
        // reports the window the refusal opened, which is the refusal itself.
        assert_eq!(
            bucket.try_consume_n(10),
            TokenBucketOutcome::GrantedAfterSuppression { suppressed: 1 }
        );
    }

    /// A draw wider than the whole burst can never be admitted, however long the
    /// bucket idles — refill clamps at capacity, so sizing is the only answer.
    #[tokio::test(start_paused = true)]
    async fn a_draw_wider_than_the_burst_is_never_admitted() {
        let mut bucket = TokenBucket::new(4, SECOND, 1);
        tokio::time::advance(SECOND * 100).await;
        assert_eq!(
            bucket.try_consume_n(5),
            TokenBucketOutcome::Denied { first: true }
        );
        // The bucket is untouched and still full.
        assert_eq!(
            bucket.try_consume_n(4),
            TokenBucketOutcome::GrantedAfterSuppression { suppressed: 1 }
        );
    }

    #[test]
    #[should_panic(expected = "non-zero draw")]
    fn a_zero_draw_panics() {
        TokenBucket::new(1, SECOND, 1).try_consume_n(0);
    }
}

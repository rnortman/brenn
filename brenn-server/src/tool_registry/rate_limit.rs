//! Per-`(participant, tool)` token-bucket rate limiting.
//!
//! `burst` is bucket capacity, `sustained_per_minute` is the refill rate. The
//! bucket is the optional throttle; the grant itself is the gate. Enforcement is
//! class-based: fast callers take-or-fail immediately, async callers reserve a
//! token and are told how long to wait for it (delay-not-drop).
//!
//! Buckets are created lazily at full capacity and keyed by the caller's full
//! `ParticipantId` string plus the tool name, so an app's conversations share
//! one budget (matching how `AppPolicy` is per-app).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use brenn_lib::tools::ResolvedRateLimit;

/// One bucket's mutable state.
struct Bucket {
    /// Current tokens. May go negative when async callers reserve ahead of
    /// accrual (that reservation is what serializes their waits).
    tokens: f64,
    /// When `tokens` was last refilled.
    last_refill: Instant,
}

/// Token-bucket rate limiter shared across the registry. Interior-mutable so the
/// registry can hand out `&self`.
#[derive(Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<(String, String), Bucket>>,
}

impl RateLimiter {
    /// Fast-class check: consume one token if available. Returns `true` if the
    /// call may proceed now, `false` if the bucket is empty (immediate error —
    /// a sync call cannot wait). A `None` limit is unlimited (always `true`).
    pub fn try_take(
        &self,
        participant: &str,
        tool: &str,
        limit: Option<ResolvedRateLimit>,
    ) -> bool {
        self.try_take_at(participant, tool, limit, Instant::now())
    }

    /// Async-class admission: reserve one token and return how long to wait
    /// before it has accrued (`Duration::ZERO` when a token is available now).
    /// The token is reserved immediately (the balance may go negative), so
    /// concurrent reservations serialize into increasing waits. A `None` limit
    /// is unlimited (always `Duration::ZERO`).
    pub fn reserve(
        &self,
        participant: &str,
        tool: &str,
        limit: Option<ResolvedRateLimit>,
    ) -> Duration {
        self.reserve_at(participant, tool, limit, Instant::now())
    }

    fn try_take_at(
        &self,
        participant: &str,
        tool: &str,
        limit: Option<ResolvedRateLimit>,
        now: Instant,
    ) -> bool {
        let Some(limit) = limit else {
            return true;
        };
        let mut buckets = self.buckets.lock().expect("rate-limiter mutex poisoned");
        let bucket = refill(&mut buckets, participant, tool, limit, now);
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn reserve_at(
        &self,
        participant: &str,
        tool: &str,
        limit: Option<ResolvedRateLimit>,
        now: Instant,
    ) -> Duration {
        let Some(limit) = limit else {
            return Duration::ZERO;
        };
        let mut buckets = self.buckets.lock().expect("rate-limiter mutex poisoned");
        let bucket = refill(&mut buckets, participant, tool, limit, now);
        // Deficit is measured before reserving; a full/positive bucket waits 0.
        let deficit = (1.0 - bucket.tokens).max(0.0);
        bucket.tokens -= 1.0;
        let per_second = refill_per_second(limit);
        Duration::from_secs_f64(deficit / per_second)
    }
}

/// Refill tokens per second from the sustained-per-minute rate.
fn refill_per_second(limit: ResolvedRateLimit) -> f64 {
    // `resolve_tool_grants` guarantees `sustained_per_minute >= 1`, so this is
    // strictly positive.
    f64::from(limit.sustained_per_minute) / 60.0
}

/// Get-or-create the bucket for `(participant, tool)` and refill it up to
/// capacity based on elapsed time since the last refill. New buckets start
/// full. Returns a mutable handle to the refilled bucket.
fn refill<'a>(
    buckets: &'a mut HashMap<(String, String), Bucket>,
    participant: &str,
    tool: &str,
    limit: ResolvedRateLimit,
    now: Instant,
) -> &'a mut Bucket {
    let capacity = f64::from(limit.burst);
    let bucket = buckets
        .entry((participant.to_string(), tool.to_string()))
        .or_insert_with(|| Bucket {
            tokens: capacity,
            last_refill: now,
        });
    let elapsed = now
        .saturating_duration_since(bucket.last_refill)
        .as_secs_f64();
    bucket.tokens = (bucket.tokens + elapsed * refill_per_second(limit)).min(capacity);
    bucket.last_refill = now;
    bucket
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(burst: u32, per_min: u32) -> Option<ResolvedRateLimit> {
        Some(ResolvedRateLimit {
            burst,
            sustained_per_minute: per_min,
        })
    }

    #[test]
    fn fast_take_drains_burst_then_fails() {
        let rl = RateLimiter::default();
        let t0 = Instant::now();
        // Burst of 2 ⇒ two immediate takes, third fails.
        assert!(rl.try_take_at("wasm:a", "tool", limit(2, 60), t0));
        assert!(rl.try_take_at("wasm:a", "tool", limit(2, 60), t0));
        assert!(!rl.try_take_at("wasm:a", "tool", limit(2, 60), t0));
    }

    #[test]
    fn fast_refills_over_time() {
        let rl = RateLimiter::default();
        let t0 = Instant::now();
        // 60/min = 1/sec. Drain the single-token burst.
        assert!(rl.try_take_at("wasm:a", "tool", limit(1, 60), t0));
        assert!(!rl.try_take_at("wasm:a", "tool", limit(1, 60), t0));
        // One second later, one token has accrued.
        let t1 = t0 + Duration::from_secs(1);
        assert!(rl.try_take_at("wasm:a", "tool", limit(1, 60), t1));
        assert!(!rl.try_take_at("wasm:a", "tool", limit(1, 60), t1));
    }

    #[test]
    fn refill_caps_at_burst() {
        let rl = RateLimiter::default();
        let t0 = Instant::now();
        // Idle for a long time; the bucket cannot exceed its capacity of 2.
        let t1 = t0 + Duration::from_secs(3600);
        assert!(rl.try_take_at("wasm:a", "tool", limit(2, 60), t1));
        assert!(rl.try_take_at("wasm:a", "tool", limit(2, 60), t1));
        assert!(!rl.try_take_at("wasm:a", "tool", limit(2, 60), t1));
    }

    #[test]
    fn async_reserve_yields_increasing_waits() {
        let rl = RateLimiter::default();
        let t0 = Instant::now();
        // Burst 1, 60/min = 1/sec. First reservation is free; each further
        // reservation at the same instant waits one more second.
        let l = limit(1, 60);
        assert_eq!(rl.reserve_at("wasm:a", "tool", l, t0), Duration::ZERO);
        assert_eq!(
            rl.reserve_at("wasm:a", "tool", l, t0),
            Duration::from_secs(1)
        );
        assert_eq!(
            rl.reserve_at("wasm:a", "tool", l, t0),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn buckets_are_isolated_per_participant_and_tool() {
        let rl = RateLimiter::default();
        let t0 = Instant::now();
        let l = limit(1, 60);
        // Drain participant a's bucket for `tool`.
        assert!(rl.try_take_at("wasm:a", "tool", l, t0));
        assert!(!rl.try_take_at("wasm:a", "tool", l, t0));
        // A different participant is unaffected.
        assert!(rl.try_take_at("wasm:b", "tool", l, t0));
        // Same participant, different tool, is unaffected.
        assert!(rl.try_take_at("wasm:a", "other", l, t0));
    }

    #[test]
    fn absent_limit_is_unlimited() {
        let rl = RateLimiter::default();
        let t0 = Instant::now();
        for _ in 0..1000 {
            assert!(rl.try_take_at("wasm:a", "tool", None, t0));
        }
        assert_eq!(rl.reserve_at("wasm:a", "tool", None, t0), Duration::ZERO);
    }
}

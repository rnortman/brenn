//! DOM-free `setTimeout`-delay clamping, shared by the surface component state
//! machines. Host-tested; no wasm dependency, so a component's `logic.rs` can
//! call it directly under the host test sweep.

use chrono::{DateTime, Utc};

/// Clamp a `setTimeout` delay (ms until `target`) into a schedulable range.
///
/// A target already at or before `now` yields `0` (fire on the next tick). A
/// far-future target is clamped to `max_ms` when given, else to `i32::MAX` —
/// browsers treat a delay above 2^31−1 ms as ~0, so an unclamped far-future
/// wakeup would busy-fire instead of waiting. A caller passing `max_ms` wakes
/// at least that often and recomputes against the true target on each fire, so
/// a suspend/resume, NTP step, or DST transition self-corrects within one
/// interval rather than trusting elapsed time.
pub fn clamp_timeout_ms(now: DateTime<Utc>, target: DateTime<Utc>, max_ms: Option<i32>) -> i32 {
    let remaining = target.signed_duration_since(now);
    let ms = remaining.num_milliseconds();
    // `num_milliseconds` truncates toward zero; a sub-millisecond remainder
    // (RFC3339 fractional seconds vs. a whole-ms `now`) would yield 0 and refire
    // immediately while the target is still in the future. Round a positive
    // remainder up so the fire lands at or after the target instant.
    let ms = if remaining > chrono::Duration::milliseconds(ms) {
        ms + 1
    } else {
        ms
    };
    let ceiling = max_ms.map_or(i32::MAX as i64, i64::from);
    ms.clamp(0, ceiling) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn future_target_is_the_remaining_delay() {
        let base = at("2026-07-08T12:00:00Z");
        assert_eq!(
            clamp_timeout_ms(base, base + chrono::Duration::seconds(5), None),
            5_000
        );
    }

    #[test]
    fn target_at_now_is_zero() {
        let base = at("2026-07-08T12:00:00Z");
        assert_eq!(clamp_timeout_ms(base, base, None), 0);
    }

    #[test]
    fn past_target_clamps_to_zero() {
        let base = at("2026-07-08T12:00:00Z");
        assert_eq!(
            clamp_timeout_ms(base, base - chrono::Duration::seconds(5), None),
            0
        );
    }

    #[test]
    fn far_future_clamps_to_i32_max_without_a_ceiling() {
        let base = at("2026-07-08T12:00:00Z");
        assert_eq!(
            clamp_timeout_ms(base, base + chrono::Duration::days(365), None),
            i32::MAX
        );
    }

    #[test]
    fn sub_millisecond_remainder_rounds_up() {
        let base = at("2026-07-08T12:00:00Z");
        assert_eq!(
            clamp_timeout_ms(base, base + chrono::Duration::microseconds(400), None),
            1
        );
    }

    #[test]
    fn max_ms_caps_a_delay_beyond_it() {
        let base = at("2026-07-08T12:00:00Z");
        let max = 15 * 60 * 1000;
        assert_eq!(
            clamp_timeout_ms(base, base + chrono::Duration::hours(2), Some(max)),
            max
        );
    }

    #[test]
    fn max_ms_leaves_a_shorter_delay_alone() {
        let base = at("2026-07-08T12:00:00Z");
        let max = 15 * 60 * 1000;
        assert_eq!(
            clamp_timeout_ms(base, base + chrono::Duration::minutes(3), Some(max)),
            3 * 60 * 1000
        );
    }
}

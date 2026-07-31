//! Per-target clock shims: the monotonic `now()` a driver stamps on every core
//! input, and the wall clock it stamps on every locally minted envelope.
//!
//! Confined to the transport layer so the sans-I/O layers above carry no `cfg`
//! logic. wasm32 has no working `std::time::Instant`, so each target reads its
//! own monotonic source — `tokio::time::Instant` natively (which honours paused
//! time under tests), `performance.now()` in the browser — and hands the core a
//! plain millisecond [`Millis`] it only ever compares.
//!
//! [`wall_now`] is the separate, deliberately-distinct concern: [`Clock`] is
//! monotonic and attachment-relative, so it can date nothing. An attacher that
//! hosts its own confined channels mints real envelopes locally — the server is
//! not in the loop to stamp `publish_ts` as it does for a channel that crosses
//! the wire — so the driver reads a true wall clock and hands the result to the
//! core as data. The two must not be conflated: a wall clock steps (NTP, user
//! clock changes) and `Millis` must not.

use brenn_queue::ReleaseTime;
use chrono::{DateTime, Utc};

use crate::Millis;

/// Epoch milliseconds UTC of a wall-clock instant, or a diagnosis of why the
/// reading cannot be used.
///
/// A pre-epoch clock poisons every `publish_ts` and release time this currency
/// carries. Clamping would silently turn every deferred publish into an
/// immediate one, so the reading is refused rather than repaired; an embedder
/// that gets `Err` here must not connect.
pub fn checked_epoch_ms(ts: DateTime<Utc>) -> Result<ReleaseTime, String> {
    u64::try_from(ts.timestamp_millis()).map_err(|_| {
        format!(
            "the clock reads before the Unix epoch ({}); nothing can be timestamped or scheduled \
             honestly",
            ts.to_rfc3339()
        )
    })
}

/// Epoch milliseconds UTC of a wall-clock instant — the currency every release
/// time in the channel model is expressed in, and the one a schedule survives a
/// restart in.
///
/// Panics on an instant before the Unix epoch. The host clock is expected to
/// have been checked with [`checked_epoch_ms`] before the attachment started, so
/// this panic fires only if a clock that was sane then is stepped back past 1970
/// mid-attachment.
pub fn epoch_ms(ts: DateTime<Utc>) -> ReleaseTime {
    checked_epoch_ms(ts).unwrap_or_else(|detail| panic!("attach client: {detail}"))
}

/// The current wall-clock instant, for stamping a synthesized envelope's
/// `publish_ts`. Read by the driver and passed into the core as data — the core
/// reads no clock itself (sans-I/O), exactly as it takes [`Clock::now`] as the
/// `now` argument on every input.
///
/// Never used for ordering or deadlines: it can step backwards. `local:`
/// ordering rests on the router's dense per-channel seq, and every deadline in
/// the client rests on the monotonic [`Clock`].
#[cfg(not(target_arch = "wasm32"))]
pub fn wall_now() -> DateTime<Utc> {
    Utc::now()
}

/// The current wall-clock instant, from `Date.now()` (milliseconds since the
/// Unix epoch — the browser's only wall clock). See the native twin for the
/// contract.
///
/// `Date.now()` is whole milliseconds within `i64` range for any plausible
/// system clock, so the conversion cannot fail; a clock set far enough into the
/// future to overflow is a broken host, not a case to absorb.
#[cfg(target_arch = "wasm32")]
pub fn wall_now() -> DateTime<Utc> {
    let ms = js_sys::Date::now() as i64;
    DateTime::from_timestamp_millis(ms)
        .expect("attach client: Date.now() outside representable range")
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn an_epoch_or_later_reading_is_its_millisecond_value() {
        let epoch = DateTime::from_timestamp_millis(0).expect("in range");
        assert_eq!(checked_epoch_ms(epoch), Ok(0));
        let later = DateTime::from_timestamp_millis(1_700_000_000_123).expect("in range");
        assert_eq!(checked_epoch_ms(later), Ok(1_700_000_000_123));
    }

    /// Refused rather than clamped: clamping would turn every deferred publish
    /// into an immediate one, which is the failure the refusal exists to prevent.
    #[test]
    fn a_pre_epoch_reading_is_refused_and_names_the_reading() {
        let before = DateTime::from_timestamp_millis(-1).expect("in range");
        let detail = checked_epoch_ms(before).expect_err("before the epoch");
        assert!(detail.contains("before the Unix epoch"), "{detail}");
        assert!(detail.contains("1969-12-31"), "{detail}");
    }

    /// The mid-attachment backstop: the host clock was checked before the
    /// attachment started, so a reading this bad now is a clock stepped back past
    /// 1970 under a running attacher.
    #[test]
    #[should_panic(expected = "before the Unix epoch")]
    fn epoch_ms_panics_on_a_pre_epoch_reading() {
        epoch_ms(DateTime::from_timestamp_millis(-1).expect("in range"));
    }
}

/// A monotonic clock. Constructed once per driver; `now()` returns milliseconds
/// since construction (native) or since navigation start (wasm) — the core only
/// compares these values, so the origin is irrelevant.
#[cfg(not(target_arch = "wasm32"))]
pub struct Clock {
    base: tokio::time::Instant,
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Clock {
    pub fn new() -> Self {
        Self {
            base: tokio::time::Instant::now(),
        }
    }

    pub fn now(&self) -> Millis {
        // Config windows are seconds-to-minutes; a process running long enough to
        // overflow u64 millis is not a concern. Saturating keeps it monotone.
        Millis(u64::try_from(self.base.elapsed().as_millis()).unwrap_or(u64::MAX))
    }
}

/// Browser monotonic clock backed by `performance.now()`.
#[cfg(target_arch = "wasm32")]
pub struct Clock;

#[cfg(target_arch = "wasm32")]
impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_arch = "wasm32")]
impl Clock {
    pub fn new() -> Self {
        Self
    }

    pub fn now(&self) -> Millis {
        let perf = web_sys::window()
            .expect("browser attacher requires a Window global")
            .performance()
            .expect("browser attacher requires performance.now()");
        Millis(perf.now() as u64)
    }
}

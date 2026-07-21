//! Per-target clock shims: the monotonic `now()` the driver stamps on every core
//! input, and the wall clock it stamps on every publish.
//!
//! Confined to the transport layer so the core and driver carry no `cfg` logic.
//! wasm32 has no working `std::time::Instant`, so each target reads its own
//! monotonic source — `tokio::time::Instant` natively (which honours paused time
//! under tests), `performance.now()` in the browser — and hands the core a plain
//! millisecond [`Millis`] it only ever compares.
//!
//! [`wall_now`] is the separate, deliberately-distinct concern: [`Clock`] is
//! monotonic and page-relative, so it can date nothing. The `local:` router
//! synthesizes real [`MessageEnvelope`](brenn_envelope::MessageEnvelope)s in the
//! page — the server is not in the loop to stamp `publish_ts` as it does for
//! `brenn:`/`ephemeral:` — so the driver reads a true wall clock and hands the
//! result to the core as data. The two must not be conflated: a wall clock steps
//! (NTP, user clock changes) and `Millis` must not.

use chrono::{DateTime, Utc};

use crate::Millis;

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
        .expect("surface client: Date.now() outside representable range")
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
            .expect("surface kernel requires a Window global")
            .performance()
            .expect("surface kernel requires performance.now()");
        Millis(perf.now() as u64)
    }
}

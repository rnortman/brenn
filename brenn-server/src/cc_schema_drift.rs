//! NDJSON schema-drift detector for CC fields extracted via `serde_json::Value`.
//!
//! This module tracks which field paths we have *ever* seen present on the CC
//! NDJSON stream. The first time we see `field == present`, we record that the
//! field exists. Any subsequent observation with `present == false` — meaning
//! the field was expected but is now missing or has the wrong type — fires a
//! single `Warning` alert and causes the function to return `false` so the
//! caller can skip the dependent broadcast.
//!
//! Behavior:
//! - **Never panics.** Schema drift is observability, not a hard stop.
//! - **Once-per-process dedup.** Each field name produces at most one alert per
//!   process lifetime (both the HAVE_SEEN set and the alert dedup key live in
//!   process-global state).
//! - **Persists across `SessionEvent::Died`.** CC respawns don't reset the
//!   "have seen" set — a CC version that previously emitted a field is still
//!   expected to emit it after restart.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use brenn_lib::obs::alerting::AlertDispatcher;

/// Process-wide set of field names we have seen `present == true` at least once.
///
/// # Invariant: append-only for process lifetime
/// Entries are never removed. The per-callsite `AtomicBool` caches in
/// [`observe_with_cache`] rely on this: once a cache flag is set to `true`,
/// the corresponding field is permanently in `HAVE_SEEN`. If this set were ever
/// cleared (e.g. on CC respawn or per-tenant reset), stale cache flags would
/// cause `present=false` observations to skip alerting — the fast path only
/// engages on `present=true`, so a cleared `HAVE_SEEN` + true cache flag would
/// make a subsequent `present=false` call enter `observe()` with `was_seen=false`
/// and silently return `false` without alerting.
static HAVE_SEEN: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();

fn have_seen() -> &'static Mutex<HashSet<&'static str>> {
    HAVE_SEEN.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Record an observation of an expected NDJSON field.
///
/// The first time `field` is observed with `present == true`, the field is
/// marked "have seen." Once marked, any observation with `present == false`
/// fires a `Warning` alert (at most once per process lifetime per field name —
/// deduplication is provided by `alert_once_per_process` using `field` as the
/// dedup key).
///
/// Returns `true` if the caller should proceed with dependent work (field is
/// present or we haven't established it should be), `false` if the field is
/// missing/wrong-type after we've previously seen it.
pub fn observe(alert_dispatcher: &AlertDispatcher, field: &'static str, present: bool) -> bool {
    if present {
        // Record that we've seen this field.
        have_seen()
            .lock()
            .expect("cc_schema_drift HAVE_SEEN lock")
            .insert(field);
        return true;
    }

    // Field is absent. Only alert if we've previously seen it.
    let was_seen = have_seen()
        .lock()
        .expect("cc_schema_drift HAVE_SEEN lock")
        .contains(field);

    if !was_seen {
        // We never observed this field before; its absence is not necessarily
        // drift (maybe this is a CC version that never emitted it).
        return false;
    }

    // Field was previously seen but is now absent. Fire a Warning alert.
    // `alert_once_per_process` deduplicates per (severity, dedup_key) so only
    // one alert fires per field per process lifetime — no separate ALERTED set
    // needed here (reuse-1).
    alert_dispatcher.alert_once_per_process(
        brenn_lib::obs::alerting::AlertSeverity::Warning,
        format!("CC NDJSON schema drift: {field} disappeared"),
        field,
        format!(
            "CC field `{field}` was previously present on the NDJSON stream \
             but is now missing or has an unexpected type. This likely indicates \
             a CC upgrade changed the protocol. The dependent broadcast was skipped."
        ),
    );

    false
}

/// Per-call-site caching variant of [`observe`].
///
/// `seen_cache` is a `'static AtomicBool` owned by the call site. On the hot
/// path (`present == true`, cache already set) this is a single relaxed
/// load+branch with no mutex acquisition.
///
/// # Contract
/// Pass only `seen_cache` flags associated with a single `field` string —
/// mixing flags across fields produces incorrect fast-path behaviour.
///
/// # Performance note
/// The fast path (`present=true` after cache warm) bypasses the mutex with a
/// single relaxed atomic load. For consistently-absent fields (`present=false`
/// on every call), the cache is never warmed and every call acquires the mutex.
/// If a loop calls this for many entries where the field is always absent, mutex
/// contention occurs per entry — use direct [`observe`] calls or restructure
/// the loop if that becomes a concern.
pub fn observe_with_cache(
    alert_dispatcher: &AlertDispatcher,
    field: &'static str,
    present: bool,
    seen_cache: &'static AtomicBool,
) -> bool {
    // Fast path: field previously observed present AND still present.
    // Both conditions must hold: the cache guarantees `was_seen`, and
    // `present` being true means there is nothing to alert on.
    if present && seen_cache.load(Ordering::Relaxed) {
        return true;
    }

    let result = observe(alert_dispatcher, field, present);

    // Warm the cache on first successful presence observation so subsequent
    // `present == true` calls bypass the mutex.
    if result && present {
        seen_cache.store(true, Ordering::Relaxed);
    }

    result
}

#[cfg(test)]
mod tests {
    use brenn_lib::obs::alerting::{AlertDispatcher, CountingAlerter, RateLimiter};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    // Tests use a fresh CountingAlerter to inspect how many alerts were fired.
    // The OnceLock state is process-global, so we can't reset it between test
    // runs in the same process. Each test must use *unique* field names to avoid
    // cross-test interference.

    fn make_alerter() -> (AlertDispatcher, Arc<AtomicU32>) {
        let count = Arc::new(AtomicU32::new(0));
        let (ad, _h) =
            AlertDispatcher::new(CountingAlerter(count.clone()), RateLimiter::new(1000, 60));
        (ad, count)
    }

    #[tokio::test]
    async fn cc_schema_drift_alerts_on_disappearance() {
        let (ad, count) = make_alerter();
        // First: field is present — mark it seen.
        assert!(super::observe(&ad, "test_field_disappears_1", true));
        // Yield to let the async alert dispatcher process any queued items.
        tokio::task::yield_now().await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "no alert on first presence"
        );
        // Second: field disappears — alert fires.
        let result = super::observe(&ad, "test_field_disappears_1", false);
        assert!(!result, "should return false when field disappears");
        tokio::task::yield_now().await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "exactly one alert on disappearance"
        );
    }

    #[tokio::test]
    async fn cc_schema_drift_silent_on_first_absence() {
        let (ad, count) = make_alerter();
        // Observe absent first — no alert since we haven't established presence.
        let result = super::observe(&ad, "test_field_never_seen_1", false);
        assert!(!result, "should return false for absent field");
        tokio::task::yield_now().await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "no alert on first-ever absence"
        );
    }

    #[tokio::test]
    async fn cc_schema_drift_dedupes_repeated_alerts() {
        let (ad, count) = make_alerter();
        // Establish presence.
        super::observe(&ad, "test_field_dedup_1", true);
        // Three absent observations — alert_once_per_process deduplicates, so
        // only one alert fires.
        super::observe(&ad, "test_field_dedup_1", false);
        super::observe(&ad, "test_field_dedup_1", false);
        super::observe(&ad, "test_field_dedup_1", false);
        tokio::task::yield_now().await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "should only fire one alert regardless of repeated absences"
        );
    }

    #[tokio::test]
    async fn observe_with_cache_fast_path_skips_mutex_after_warming() {
        use std::sync::atomic::AtomicBool;
        static CACHE_A: AtomicBool = AtomicBool::new(false);
        let (ad, count) = make_alerter();

        // First call: present=true — warms the cache; calls observe() internally.
        let r1 = super::observe_with_cache(&ad, "owc_fast_path_1", true, &CACHE_A);
        assert!(r1, "present=true should return true");
        assert!(CACHE_A.load(Ordering::Relaxed), "cache should be warmed");

        // Second call: present=true with cache set — fast path, no mutex.
        // Indistinguishable from observe() result, but verifiable by return value.
        let r2 = super::observe_with_cache(&ad, "owc_fast_path_1", true, &CACHE_A);
        assert!(r2, "fast path should return true");

        // No alerts should have fired — field never disappeared.
        tokio::task::yield_now().await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "no alert when field stays present"
        );
    }

    #[tokio::test]
    async fn observe_with_cache_absent_falls_through_after_warming() {
        use std::sync::atomic::AtomicBool;
        static CACHE_B: AtomicBool = AtomicBool::new(false);
        let (ad, count) = make_alerter();

        // Warm the cache via a present=true call.
        super::observe_with_cache(&ad, "owc_absent_fallthrough_1", true, &CACHE_B);
        assert!(CACHE_B.load(Ordering::Relaxed), "cache should be warmed");

        // present=false: cache is set but present=false → must NOT fast-path;
        // falls through to observe() which fires an alert since field was seen.
        let r = super::observe_with_cache(&ad, "owc_absent_fallthrough_1", false, &CACHE_B);
        assert!(!r, "absent after presence should return false");
        tokio::task::yield_now().await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "alert fires when field disappears"
        );
    }
}

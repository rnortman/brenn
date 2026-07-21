//! Activation pacing tests (mqtt-wasm-republish-pacing design §8).
//!
//! These exercise `ActivationPacer` — the per-component activation gate — at the
//! `admit` level, which is where every drain step (startup sweep + each notified
//! wake) is paced. The pacer owns everything it touches (its bucket, slug, and
//! alert sink), so these tests construct it directly — no `WasmConsumerConfig`,
//! no messenger, no SQLite. That keeps the timing pure (bucket + `sleep`), so
//! paused-time assertions are deterministic — the caveat design §8 raises about
//! auto-advance racing a blocking call does not apply on the `admit` path.
//!
//! The one exception is `clamp_chain_drains_fully_and_paced`, which drives the
//! real consumer task end-to-end (`spawn_wasm_consumer_task`) and therefore does
//! build a messenger and use real time (see its own comment).
//!
//! Config-validation tests (`activation_burst`/`activation_min_period_ms` zero
//! rejection + default resolution) live with the resolve harness in
//! `brenn/src/bootstrap/messaging.rs`.

use super::*;

use std::time::Duration;

use brenn_lib::messaging::config::ActivationPacing;
use brenn_lib::obs::alerting::{
    AlertSeverity, make_capturing_alerter_with_severity, noop_alert_dispatcher,
};
use tokio::time::Instant;
use tracing_test::traced_test;

const SECOND: Duration = Duration::from_millis(1000);

/// A fresh pacer with the given slug/pacing wired to a noop alert sink. For tests
/// that inspect only pacing/timing/episode state, not the throttle alert; the
/// noop drainer handle is detached and exits when the pacer (its sole dispatcher
/// clone) drops.
fn noop_pacer(slug: &str, pacing: ActivationPacing) -> ActivationPacer {
    let (dispatcher, _handle) = noop_alert_dispatcher();
    ActivationPacer::new(pacing, slug.to_string(), dispatcher)
}

/// Test 1 (design §8, item 1) + Test 5 (startup sweep): a fresh bucket admits `burst`
/// activations back-to-back with no delay and never opens a throttle episode.
/// The first admit models the startup sweep — paced through the same gate, but
/// never delayed from a full bucket.
#[tokio::test(start_paused = true)]
async fn burst_passes_untouched() {
    let pacing = ActivationPacing {
        burst: 3,
        min_period: SECOND,
    };
    let mut pacer = noop_pacer("pacing-burst", pacing);

    let start = Instant::now();
    for _ in 0..3 {
        pacer.admit().await;
    }
    // No token was ever denied, so no `sleep` ran and the paused clock is
    // unchanged: the gate is invisible to a within-burst load.
    assert_eq!(
        Instant::now(),
        start,
        "burst of `burst` admits must not advance the clock"
    );
    assert!(
        pacer.episode.is_none(),
        "no throttle episode may open within the burst"
    );
}

/// Test 2 (design §8, item 2): a sustained flood above 1/min_period is paced — after
/// the burst, grant-to-grant spacing equals exactly `min_period` (the stable
/// quantity per design §2.1's oversleep note). Delay, not drop: `admit` always
/// eventually returns, and the post-sleep-grant invariant (Test 7) is asserted
/// inside `admit` — a violation would panic and fail this test.
#[tokio::test(start_paused = true)]
async fn sustained_flood_is_paced() {
    let pacing = ActivationPacing {
        burst: 2,
        min_period: SECOND,
    };
    let mut pacer = noop_pacer("pacing-flood", pacing);

    // Drain the burst — no delay.
    let t_burst = Instant::now();
    pacer.admit().await;
    pacer.admit().await;
    assert_eq!(
        Instant::now(),
        t_burst,
        "burst of 2 must not delay under a fresh bucket"
    );

    // Each further admit is paced by exactly one min_period, grant-to-grant.
    let mut last = Instant::now();
    for i in 0..3 {
        pacer.admit().await;
        let now = Instant::now();
        assert_eq!(
            now.duration_since(last),
            SECOND,
            "paced admit {i}: grant-to-grant spacing must equal min_period"
        );
        last = now;
        assert!(
            pacer.episode.is_some(),
            "a throttle episode must stay open across the flood"
        );
    }
    assert_eq!(
        pacer.episode.as_ref().expect("episode open").delayed,
        3,
        "every paced activation increments the episode's delayed count"
    );
}

/// Test 3 (design §8, item 3): entry signals fire exactly once per episode — a `warn`
/// + a component security event (`wasm_activation_throttled`) + one phone alert
/// per process per slug — and the exit `info` fires with the delayed count on the
/// first unthrottled activation after the flood.
#[tokio::test(start_paused = true)]
#[traced_test]
async fn episode_signals_entry_once_and_exit() {
    let pacing = ActivationPacing {
        burst: 1,
        min_period: SECOND,
    };
    let (dispatcher, captured, handle) = make_capturing_alerter_with_severity();
    let mut pacer = ActivationPacer::new(pacing, "pacing-episode".to_string(), dispatcher);

    // Burst of 1: first admit granted, unthrottled.
    pacer.admit().await;
    // Flood: 3 paced admits → one episode, one entry alert.
    for _ in 0..3 {
        pacer.admit().await;
    }
    assert!(pacer.episode.is_some(), "episode open during flood");

    // Let the bucket refill past min_period, then a wake grants without sleeping
    // and closes the episode (exit `info`).
    tokio::time::advance(SECOND).await;
    pacer.admit().await;
    assert!(
        pacer.episode.is_none(),
        "episode closes on the first unthrottled admit after the flood"
    );

    // Security event + episode-lifecycle logs.
    assert!(
        logs_contain("wasm_activation_throttled"),
        "entry must emit the component security event"
    );
    assert!(
        logs_contain("activation pacing episode ended"),
        "exit must emit the episode-ended info log"
    );

    // Drain the alert channel: drop the pacer (the sole dispatcher clone) so the
    // drainer channel closes, then await the drainer before reading.
    drop(pacer);
    handle.await.unwrap();
    let alerts = captured.lock().unwrap();
    assert_eq!(
        alerts.len(),
        1,
        "exactly one throttle alert per episode per process: {alerts:?}"
    );
    let (sev, title, body) = &alerts[0];
    assert!(matches!(sev, AlertSeverity::Warning), "severity: {sev:?}");
    assert_eq!(
        title, "Security: wasm_activation_throttled",
        "stable title so the per-slug dedup key keys correctly"
    );
    assert!(
        body.contains("pacing-episode"),
        "alert body names the throttled consumer: {body}"
    );
}

/// Repeated paced admits within one still-open episode (no intervening
/// unthrottled grant to close it) alert exactly once: the episode opens on the
/// first delayed admit and every later delay is folded into the same open
/// episode, so five paced admits produce one entry alert, not five.
#[tokio::test(start_paused = true)]
async fn repeat_flood_same_episode_does_not_realert() {
    let pacing = ActivationPacing {
        burst: 1,
        min_period: SECOND,
    };
    let (dispatcher, captured, handle) = make_capturing_alerter_with_severity();
    let mut pacer = ActivationPacer::new(pacing, "pacing-repeat".to_string(), dispatcher);

    pacer.admit().await; // burst
    // Two separate paced admits without an intervening unthrottled grant: the
    // episode stays open the whole time, so only the first opens it (one alert).
    for _ in 0..5 {
        pacer.admit().await;
    }
    assert_eq!(
        pacer.episode.as_ref().expect("episode open").delayed,
        5,
        "all five paced admits counted in the one open episode"
    );

    // Release the pacer (the sole dispatcher clone) so the drainer channel closes.
    drop(pacer);
    handle.await.unwrap();
    let alerts = captured.lock().unwrap();
    assert_eq!(
        alerts.len(),
        1,
        "one open episode alerts once, no matter how many activations it delays"
    );
}

/// Two *separate* throttle episodes on one pacer/slug: the entry `warn` +
/// component security event fire on every unthrottled→throttled transition
/// (`open_episode` has no cross-episode memory), but the phone alert is deduped
/// for the process lifetime — so a second flood, after the first episode closed,
/// must re-open (re-warn/re-log) yet NOT re-alert. This pins the design §4
/// contract ("Once per process per slug ... the security log still records each
/// episode") that a single-episode test cannot: `episode.is_some()` after the
/// second flood proves `open_episode` ran to completion the second time (the
/// episode is set only after the warn/security-event/alert calls), guarding
/// against a future first-episode-only early return that would also swallow them,
/// while `captured.len() == 1` proves the alert itself deduped.
#[tokio::test(start_paused = true)]
#[traced_test]
async fn second_episode_relogs_but_does_not_realert() {
    let pacing = ActivationPacing {
        burst: 1,
        min_period: SECOND,
    };
    let (dispatcher, captured, handle) = make_capturing_alerter_with_severity();
    let mut pacer = ActivationPacer::new(pacing, "pacing-two-ep".to_string(), dispatcher);

    // Episode 1: burst grant, then a paced admit opens it (warn + security event
    // + one alert). The paced admit's internal sleep+retry drains the refilled
    // token, so the bucket is empty and `last_refill` is now.
    pacer.admit().await; // burst grant
    pacer.admit().await; // paced → episode 1 opens
    assert!(pacer.episode.is_some(), "episode 1 open during first flood");

    // Close episode 1: refill one interval, next admit grants without sleeping.
    tokio::time::advance(SECOND).await;
    pacer.admit().await;
    assert!(
        pacer.episode.is_none(),
        "episode 1 closes on the first unthrottled admit"
    );

    // Episode 2: the closing grant drained the token, so the very next admit is
    // denied and reopens a fresh episode — re-warning and re-logging the security
    // event, but the alert dedup must suppress a second page.
    pacer.admit().await; // paced → episode 2 opens
    assert!(
        pacer.episode.is_some(),
        "episode 2 must reopen (open_episode ran again: warn + security event)"
    );
    assert!(
        logs_contain("wasm_activation_throttled"),
        "each episode re-emits the component security event"
    );

    drop(pacer);
    handle.await.unwrap();
    let alerts = captured.lock().unwrap();
    assert_eq!(
        alerts.len(),
        1,
        "phone alert dedups for the process lifetime — episode 2 must not re-alert: {alerts:?}"
    );
}

/// Test 4 (design §8, item 4 — clamp-chain pacing): a backlog larger than
/// `burst × cap` drains fully, paced, via the self-renotify chain. This is the
/// one scenario that crosses the pacer with the clamp/self-renotify mechanism
/// (`drain_step`'s `notify_one`, `mod.rs`) *inside a single oversized backlog* —
/// pacing kicks in partway through draining one backlog, not merely across
/// independent external wakes. The admit-level tests above pin pacing in
/// isolation; the `renotify` family pins the clamp chain in isolation; this pins
/// the two together.
///
/// Unlike the admit-level tests, this drives the real consumer task
/// (`spawn_wasm_consumer_task`) end-to-end with no external wake — only the
/// startup sweep + self-renotify chain — so `spawn_blocking` + real SQLite run
/// and paused time would race the pacer's sleep (design §8). Hence real time
/// with a small `min_period`, asserting a wall-clock *lower bound*: real sleeps
/// never shorten, so the elapsed floor is a hard guarantee that pacing engaged,
/// robust to poll/scheduling slack.
#[tokio::test]
#[traced_test]
async fn clamp_chain_drains_fully_and_paced() {
    const MIN_PERIOD: Duration = Duration::from_millis(100);
    let slug = "pacing-clamp-chain";
    // push_depth = Bounded(2) → each activation caps at 2 new rows. 6 rows ⇒
    // ceil(6/2) = 3 activations. burst = 1 ⇒ the startup sweep is free and the
    // two follow-up self-renotify activations are each paced by a full
    // min_period. 6 > burst × cap = 1 × 2, matching design §8 item 4's "backlog
    // larger than burst × cap".
    let (messenger, channel, wasm_sub) = testutils::build_wasm_messenger(
        slug,
        "pacing-clamp-ch",
        Depth::Bounded(2),
        Depth::Bounded(0),
    )
    .await;

    for i in 0..6 {
        testutils::insert_wasm_push(
            &messenger,
            &channel,
            &wasm_sub,
            &format!("row-{i}"),
            ChannelScheme::Brenn,
        )
        .await;
    }

    let (mut cfg, _handle, _db) = build_cfg(
        slug,
        Arc::clone(&messenger),
        &channel,
        Depth::Bounded(2),
        Depth::Bounded(0),
    );
    cfg.activation_pacing = ActivationPacing {
        burst: 1,
        min_period: MIN_PERIOD,
    };

    // No external wake is ever sent: only the startup sweep + self-renotify
    // chain drive the drain, so the clamp chain is what crosses the pacer.
    let start = std::time::Instant::now();
    let _task = spawn_wasm_consumer_task(cfg);

    assert!(
        wait_pending_empty(&messenger, &wasm_sub, Duration::from_secs(5)).await,
        "clamp self-renotify chain must drain the full backlog even while paced"
    );

    // Paced, not instant: 3 activations, burst = 1 ⇒ 2 paced sleeps of a full
    // min_period each, run sequentially before their drain steps ack the last
    // rows. The pending set cannot empty until the third activation's drain step,
    // which runs after both sleeps — so elapsed ≥ 2 × min_period, a lower bound
    // (real sleeps never shorten).
    let elapsed = start.elapsed();
    assert!(
        elapsed >= 2 * MIN_PERIOD,
        "clamp-chain drain must be paced: elapsed {elapsed:?} < 2 × min_period {:?}",
        2 * MIN_PERIOD
    );
    // And the pacer actually engaged mid-chain (not merely slow for unrelated
    // reasons): the throttle episode's entry warn must have fired.
    assert!(
        logs_contain("activation pacing engaged"),
        "the clamp self-renotify chain must trip the activation pacer"
    );
}

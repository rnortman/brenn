//! Rate-limit event-family tests: `SessionEvent::RateLimit` browser-error
//! gating and `handle_rate_limit_utilization` schema-drift observation.

use super::super::super::test_support::{
    await_fence, drain_broadcast, event_fence, recv_broadcast, test_bridge,
};
use super::super::*;

use brenn_cc::protocol::incoming::RateLimitEventMessage;
use brenn_lib::obs::alerting::{AlertDispatcher, CountingAlerter, RateLimiter};
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn rate_limit_allowed_does_not_show_error() {
    let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    let rate_limit = RateLimitEventMessage {
        uuid: Some("msg-1".into()),
        session_id: None,
        rate_limit_info: Some(serde_json::json!({"status": "allowed"})),
        extra: serde_json::Value::Object(Default::default()),
    };
    let fence = event_fence(&bridge);
    event_tx
        .send(SessionEvent::RateLimit(rate_limit))
        .await
        .unwrap();

    await_fence(fence).await;
    let msgs = drain_broadcast(&mut broadcast_rx);
    assert!(
        msgs.is_empty(),
        "allowed rate limit should not send error to browser"
    );
}

#[tokio::test]
async fn rate_limit_rejected_sends_error() {
    let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

    let rate_limit = RateLimitEventMessage {
        uuid: Some("msg-1".into()),
        session_id: None,
        rate_limit_info: Some(serde_json::json!({"status": "rejected"})),
        extra: serde_json::Value::Object(Default::default()),
    };
    event_tx
        .send(SessionEvent::RateLimit(rate_limit))
        .await
        .unwrap();

    let msg = recv_broadcast(&mut broadcast_rx).await;
    match &msg {
        WsServerMessage::Error { message } => {
            assert!(message.contains("Rate limited"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

/// `handle_rate_limit_utilization`: "allowed" status — gate returns early,
/// no schema-drift alert fires (status field is present and recognized).
#[tokio::test]
async fn rate_limit_utilization_allowed_no_alert() {
    let alert_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (dispatcher, _h) = AlertDispatcher::new(
        CountingAlerter(alert_count.clone()),
        RateLimiter::new(10, 60),
    );
    let evt = RateLimitEventMessage {
        uuid: Some("u1".into()),
        session_id: None,
        rate_limit_info: Some(serde_json::json!({
            "status": "allowed",
            "utilization": 0.3,
            "rateLimitType": "requests_per_minute",
            "resetsAt": 1_700_000_000_i64,
        })),
        extra: serde_json::Value::Object(Default::default()),
    };
    // "allowed" gate returns early before touching downstream fields.
    // No schema-drift alert fires — all observed fields are present.
    handle_rate_limit_utilization(&evt, &dispatcher);
    assert_eq!(
        alert_count.load(Ordering::SeqCst),
        0,
        "allowed status with all fields present must not trigger schema-drift alert"
    );
}

/// `handle_rate_limit_utilization`: "allowed_warning" with all four fields —
/// no schema-drift alert fires (all expected fields present), warn!() log reached.
#[tokio::test]
async fn rate_limit_utilization_warning_all_fields_no_alert() {
    let alert_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (dispatcher, _h) = AlertDispatcher::new(
        CountingAlerter(alert_count.clone()),
        RateLimiter::new(10, 60),
    );
    let evt = RateLimitEventMessage {
        uuid: Some("u2".into()),
        session_id: None,
        rate_limit_info: Some(serde_json::json!({
            "status": "allowed_warning",
            "utilization": 0.9,
            "rateLimitType": "tokens_per_minute",
            "resetsAt": 1_700_000_060_i64,
        })),
        extra: serde_json::Value::Object(Default::default()),
    };
    // All four schema fields present — all observe calls return true,
    // warn!() branch fires. No schema-drift alert (no missing fields).
    handle_rate_limit_utilization(&evt, &dispatcher);
    assert_eq!(
        alert_count.load(Ordering::SeqCst),
        0,
        "allowed_warning with all fields present must not trigger schema-drift alert"
    );
}

/// `handle_rate_limit_utilization`: "allowed_warning" with missing downstream
/// fields — schema-drift alerts fire for each field that was previously seen
/// present but is now absent.
///
/// The test primes HAVE_SEEN with a first call (all fields present), then
/// makes a second call with the three downstream fields absent and asserts
/// that at least one alert fires. The at-most-3 upper bound is preserved
/// (one per missing field; process-global dedup caps each at one per
/// process lifetime, so repeat runs in the same process produce 0).
#[tokio::test]
async fn rate_limit_utilization_warning_missing_fields_schema_drift() {
    let alert_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (dispatcher, _h) = AlertDispatcher::new(
        CountingAlerter(alert_count.clone()),
        RateLimiter::new(100, 60),
    );

    // Prime HAVE_SEEN: call once with all fields present.
    let prime_evt = RateLimitEventMessage {
        uuid: Some("u3-prime".into()),
        session_id: None,
        rate_limit_info: Some(serde_json::json!({
            "status": "allowed_warning",
            "utilization": 0.8,
            "rateLimitType": "tokens_per_minute",
            "resetsAt": 1_700_000_060_i64,
        })),
        extra: serde_json::Value::Object(Default::default()),
    };
    handle_rate_limit_utilization(&prime_evt, &dispatcher);
    // No alerts on presence — asserts that the priming call is clean.
    tokio::task::yield_now().await;
    let count_after_prime = alert_count.load(Ordering::SeqCst);

    // Now call with the three downstream fields absent.
    let missing_evt = RateLimitEventMessage {
        uuid: Some("u3".into()),
        session_id: None,
        rate_limit_info: Some(serde_json::json!({
            "status": "allowed_warning",
            // utilization, rateLimitType, resetsAt intentionally absent
        })),
        extra: serde_json::Value::Object(Default::default()),
    };
    handle_rate_limit_utilization(&missing_evt, &dispatcher);
    tokio::task::yield_now().await;
    let total = alert_count.load(Ordering::SeqCst);

    // At least one alert must have fired for missing fields (each field
    // was seen in the priming call; once-per-process dedup means subsequent
    // runs of this test in the same process see 0 new alerts, which is why
    // we compare against count_after_prime rather than 0).
    assert!(
        total > count_after_prime,
        "schema-drift alerts must fire when previously-seen fields disappear \
             (got {total} total, {count_after_prime} before missing-fields call)"
    );
    assert!(
        total <= count_after_prime + 3,
        "at most one alert per missing field (utilization, rateLimitType, resetsAt)"
    );
}

/// `handle_rate_limit_utilization`: `rate_limit_info: None` — function returns
/// immediately, no schema-drift observations fire.
#[tokio::test]
async fn rate_limit_utilization_none_rate_limit_info_no_alerts() {
    let alert_count = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let (dispatcher, _h) = AlertDispatcher::new(
        CountingAlerter(alert_count.clone()),
        RateLimiter::new(10, 60),
    );
    let evt = RateLimitEventMessage {
        uuid: Some("u4".into()),
        session_id: None,
        rate_limit_info: None,
        extra: serde_json::Value::Object(Default::default()),
    };
    handle_rate_limit_utilization(&evt, &dispatcher);
    assert_eq!(
        alert_count.load(Ordering::SeqCst),
        0,
        "rate_limit_info: None must return immediately with no schema-drift observations"
    );
}

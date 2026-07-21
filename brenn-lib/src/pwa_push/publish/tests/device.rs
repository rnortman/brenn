//! Device-targeted stale-user counter tests (§2.5 `device.rs` family): a
//! device-targeted send whose subscription exists but whose user is stale on
//! that device reports `failed_stale_user == 1`; one with no subscription at all
//! reports all five counters zero; one whose user is current delivers normally.
//! Peeled out of `tests/mod.rs` per design §2.5.
//!
//! The shared HTTP mocks (`MockResponse`, `MockHttpPoster`,
//! `make_service_with_poster`, `valid_p256dh`, `valid_auth`) and the
//! `make_db_with_users` builder live in `tests/mod.rs` as `pub(super)` and are
//! reached here via `super::` (§2.4).

use super::super::*;
use super::{
    MockHttpPoster, MockResponse, make_db_with_users, make_service_with_poster, valid_auth,
    valid_p256dh,
};
use crate::pwa_push::db::upsert_subscription;
use crate::pwa_push::endpoint_validator::ValidatedEndpoint;
use crate::pwa_push::test_helpers::{fake_auth, fake_p256dh};

use std::collections::HashMap as StdHashMap;
use std::sync::Arc;
use std::time::Duration;

/// Device-targeted send where the subscription exists but the user is stale
/// must report `failed_stale_user == 1` and all other counters zero.
#[tokio::test]
async fn device_targeted_stale_user_reports_failed_stale_user_one() {
    let db = make_db_with_users().await;
    {
        let conn = db.lock().await;
        let later = "2030-01-01T00:00:00+00:00";
        // Add bob to device_users for laptop and make him the current user.
        conn.execute_batch(&format!(
            "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (1, 2, '{later}', '{later}');"
        ))
        .expect("add bob to laptop");
        // alice's last_seen is now earlier than bob's → alice is stale on laptop.
        // Subscribe alice on laptop.
        upsert_subscription(
            &conn,
            1,
            1,
            &ValidatedEndpoint::for_testing("https://ep.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
    }

    let poster = MockHttpPoster::new(StdHashMap::new());
    let svc = make_service_with_poster(db, poster);
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice@laptop",
            "body",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;

    match result {
        PushSendResult::Ok {
            delivered,
            gone,
            failed,
            failed_stale_user,
            failed_invalid_endpoint,
            ..
        } => {
            assert_eq!(delivered, 0, "stale user: no delivery");
            assert_eq!(gone, 0);
            assert_eq!(failed, 0);
            assert_eq!(failed_stale_user, 1, "must count stale-user as 1");
            assert_eq!(failed_invalid_endpoint, 0);
            // Note: `attempted` (= sum of all five counters) is derived in
            // pwa_push_intercept.rs:524-528 and not part of PushSendResult.
            // An integration test at the intercept layer covering
            // `attempted == 1` for device-targeted stale sends is not
            // included in this diff.
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

/// Device-targeted send where no subscription exists at all must report all
/// five counters as zero (no regression from prior behavior).
#[tokio::test]
async fn device_targeted_no_subscription_reports_zero_counters() {
    let db = make_db_with_users().await;
    // No subscription seeded.
    let poster = MockHttpPoster::new(StdHashMap::new());
    let svc = make_service_with_poster(db, poster);
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice@laptop",
            "body",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;

    match result {
        PushSendResult::Ok {
            delivered,
            gone,
            failed,
            failed_stale_user,
            failed_invalid_endpoint,
            ..
        } => {
            assert_eq!(delivered, 0);
            assert_eq!(gone, 0);
            assert_eq!(failed, 0);
            assert_eq!(
                failed_stale_user, 0,
                "no subscription → stale counter must be 0"
            );
            assert_eq!(failed_invalid_endpoint, 0);
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

/// Device-targeted send where the subscription exists and the user is current
/// must deliver normally (no regression on the happy path).
#[tokio::test(flavor = "multi_thread")]
async fn device_targeted_current_user_delivers_normally() {
    let db = make_db_with_users().await;
    let ep_url = "https://mock.example.com/push/alice-laptop";
    {
        let conn = db.lock().await;
        // alice is the only user on laptop → she is already current.
        upsert_subscription(
            &conn,
            1,
            1,
            &ValidatedEndpoint::for_testing(ep_url),
            &valid_p256dh(),
            &valid_auth(),
        );
    }

    let mut responses = StdHashMap::new();
    responses.insert(
        ep_url.to_string(),
        MockResponse::Ok {
            status: 201,
            delay: Duration::ZERO,
        },
    );
    let poster = MockHttpPoster::new(responses);
    let svc = make_service_with_poster(db, poster);
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice@laptop",
            "body",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;

    match result {
        PushSendResult::Ok {
            delivered,
            gone,
            failed,
            failed_stale_user,
            failed_invalid_endpoint,
            ..
        } => {
            assert_eq!(delivered, 1, "current user must be delivered");
            assert_eq!(gone, 0);
            assert_eq!(failed, 0);
            assert_eq!(failed_stale_user, 0, "happy path: stale counter must be 0");
            assert_eq!(failed_invalid_endpoint, 0);
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

/// Publish-layer wiring proof: a real delivery carries the VAPID `Authorization`
/// header with the `vapid t=<jwt>, k=<key>` shape. Guards against the header
/// injection in `deliver_to_subscription` being dropped — every counter-only
/// test would still pass with the header missing, yet every real push service
/// would return 401. Header *content* correctness (claims, signature, k= value)
/// is covered by the `vapid.rs` unit tests; this asserts only that the wiring is
/// present and well-shaped.
#[tokio::test(flavor = "multi_thread")]
async fn delivery_attaches_vapid_authorization_header() {
    let db = make_db_with_users().await;
    let ep_url = "https://ep.example.com/push/alice";
    {
        let conn = db.lock().await;
        upsert_subscription(
            &conn,
            1,
            1,
            &ValidatedEndpoint::for_testing(ep_url),
            &valid_p256dh(),
            &valid_auth(),
        );
    }

    let mut responses = StdHashMap::new();
    responses.insert(
        ep_url.to_string(),
        MockResponse::Ok {
            status: 201,
            delay: Duration::ZERO,
        },
    );
    let poster = MockHttpPoster::new(responses);
    // Retain a concrete-typed clone so we can read captured requests after the
    // send; the service upcasts its own clone to `dyn HttpPoster`.
    let mock = Arc::clone(&poster);
    let svc = make_service_with_poster(db, poster);
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice@laptop",
            "body",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;

    match result {
        PushSendResult::Ok { delivered, .. } => {
            assert_eq!(delivered, 1, "delivery must go through the mock");
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    let captured = mock.take_captured();
    assert_eq!(captured.len(), 1, "exactly one delivery request expected");
    let auth = captured[0]
        .headers()
        .get(http::header::AUTHORIZATION)
        .expect("Authorization header must be attached");
    const VAPID_PREFIX: &str = "vapid t=";
    const K_SEP: &str = ", k=";
    let auth_str = auth.to_str().expect("VAPID header is ASCII");
    assert!(
        auth_str.starts_with(VAPID_PREFIX),
        "Authorization must be a VAPID token; got: {auth_str}"
    );
    let k_pos = auth_str
        .find(K_SEP)
        .unwrap_or_else(|| panic!("VAPID header must carry the k= public key; got: {auth_str}"));

    // The JWT (between the `t=` prefix and `, k=`) must be three non-empty dot-parts.
    let jwt = &auth_str[VAPID_PREFIX.len()..k_pos];
    let parts: Vec<&str> = jwt.split('.').collect();
    assert_eq!(parts.len(), 3, "JWT must have three dot-separated parts");
    assert!(
        parts.iter().all(|p| !p.is_empty()),
        "each JWT segment must be non-empty; got: {jwt}"
    );
}

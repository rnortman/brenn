//! Persistence-family tests (§2.5 `persistence.rs`): message-row persistence,
//! lazy channel interning, address canonicalization, sender stamping, title
//! fallback, the single-budget-unit invariant, and invalid-endpoint / allowlist-miss
//! row purging. Moved verbatim from `publish/tests/mod.rs`; no body changed.

use super::super::*; // production items: PwaPushService, PushSendResult, Urgency, MessagingGlobalConfig, IndexMap, Arc
use super::{make_app_config, make_db_with_users, make_pwa_push_config, make_service}; // pub(super) fixtures
// Types imported privately in `tests/mod.rs` and therefore not re-exported via the
// `super::super::*` glob (private imports don't re-export). Imported directly here.
use crate::pwa_push::config::ResolvedPwaPushConfig;
use crate::pwa_push::db::upsert_subscription;
use crate::pwa_push::endpoint_validator::ValidatedEndpoint;
use crate::pwa_push::test_helpers::{fake_auth, fake_p256dh};
use crate::pwa_push::vapid::load_or_generate;

/// AC2/AC3: pwa_push send stores `app:<slug>@<server>` as the sender in messaging_messages.
/// Regression guard: the old code stamped `app.name` via a fallback chain; deleting the
/// fallback without this test would leave no coverage of the replacement.
#[tokio::test]
async fn pwa_push_send_stores_structured_sender_in_db() {
    let db = make_db_with_users().await;
    // AC3: app with pwa_push enabled but no [app.messaging] block.
    let app = {
        let mut a = make_app_config("graf", true, vec![]);
        a.messaging = None;
        a
    };
    let mut apps = IndexMap::new();
    apps.insert("graf".to_string(), app);
    let svc = Arc::new(PwaPushService::new(
        db.clone(),
        make_pwa_push_config(),
        Arc::new(apps),
        MessagingGlobalConfig {
            default_send_budget: 100,
            max_body_bytes: 4096,
            ..MessagingGlobalConfig::default()
        },
        Arc::from("https://brenn.test"),
        crate::obs::alerting::noop_alert_dispatcher().0,
    ));
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            "test body",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PushSendResult::Ok { .. }),
        "expected Ok; got {result:?}"
    );
    // Verify stored sender in messaging_messages.
    let conn = db.lock().await;
    let stored_sender: String = conn
        .query_row(
            "SELECT sender FROM messaging_messages WHERE envelope_type = 'brenn' ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .expect("messaging_messages row must exist after send");
    assert_eq!(
        stored_sender, "app:graf@https://brenn.test",
        "pwa_push send must stamp app:<slug>@<server> as sender (AC2/AC3)"
    );
}

#[tokio::test]
async fn send_persists_message_row_on_success() {
    let db = make_db_with_users().await;
    let svc = make_service(db.clone());
    // No subscription — but message should still be persisted.
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            "hello world",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PushSendResult::Ok { .. }));

    // Verify the messaging_messages row was created.
    let conn = db.lock().await;
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages m
                 JOIN messaging_channels c ON c.uuid = m.channel_uuid
                 WHERE c.address = 'pwa_push:alice' AND m.body = 'hello world'",
            [],
            |row| row.get(0),
        )
        .expect("count query");
    assert_eq!(count, 1, "message must be persisted in messaging_messages");
}

#[tokio::test]
async fn first_publish_lazily_interns_channel_row() {
    let db = make_db_with_users().await;
    let svc = make_service(db.clone());

    // Before send: no messaging_channels row for pwa_push:alice.
    let before: i64 = db
        .lock()
        .await
        .query_row(
            "SELECT COUNT(*) FROM messaging_channels WHERE address = 'pwa_push:alice'",
            [],
            |row| row.get(0),
        )
        .expect("count");
    assert_eq!(before, 0);

    // After send: channel row is lazily created.
    let _ = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            "hi",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;

    let after: i64 = db
        .lock()
        .await
        .query_row(
            "SELECT COUNT(*) FROM messaging_channels WHERE address = 'pwa_push:alice'",
            [],
            |row| row.get(0),
        )
        .expect("count");
    assert_eq!(
        after, 1,
        "channel row must be lazily created on first publish"
    );
}

#[tokio::test]
async fn subscribe_does_not_create_messaging_channels_rows() {
    // Confirm that subscribing does NOT touch messaging_channels.
    let db = make_db_with_users().await;
    {
        let conn = db.lock().await;
        upsert_subscription(
            &conn,
            1,
            1,
            &ValidatedEndpoint::for_testing("https://ep.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
    }
    let count: i64 = db
        .lock()
        .await
        .query_row(
            "SELECT COUNT(*) FROM messaging_channels WHERE address LIKE 'pwa_push:%'",
            [],
            |row| row.get(0),
        )
        .expect("count");
    assert_eq!(
        count, 0,
        "subscribe must not pre-create messaging_channels rows"
    );
}

#[tokio::test]
async fn title_fallback_uses_app_default_title_when_set() {
    // The app fixture has default_title = "Test App".
    // When PushSend omits title, the payload template should use "Test App".
    let db = make_db_with_users().await;
    let svc = make_service(db.clone());
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            "msg body",
            None, /* no title */
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PushSendResult::Ok { .. }));
    // Verify persisted message body.
    let conn = db.lock().await;
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE body = 'msg body'",
            [],
            |row| row.get(0),
        )
        .expect("count");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn body_within_limit_consumes_one_budget_unit_regardless_of_fanout() {
    // Register two subscriptions for alice on two devices. Even with fan-out,
    // budget decrements by exactly 1.
    let db = make_db_with_users().await;
    {
        let conn = db.lock().await;
        upsert_subscription(
            &conn,
            1,
            1,
            &ValidatedEndpoint::for_testing("https://ep1.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
        upsert_subscription(
            &conn,
            2,
            1,
            &ValidatedEndpoint::for_testing("https://ep2.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
    }
    let svc = make_service(db.clone());
    // The send will attempt 2 real HTTP calls that will fail (no actual push
    // service), but the budget should still decrement by 1.
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
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
            remaining_budget, ..
        } => {
            // Budget starts at 100, decrements by 1 → 99.
            assert_eq!(
                remaining_budget, 99,
                "budget must decrement by exactly 1 regardless of fan-out"
            );
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

/// A subscription row with an invalid endpoint (private IP) at delivery
/// time must be purged and counted in `failed_invalid_endpoint`.
///
/// The service under test uses the unenforced empty policy from
/// `make_pwa_push_config()`, which still blocks private IPs. The loopback
/// endpoint fails at `PrivateHost` (independent of the allowlist), so
/// `make_service` is used directly without overriding the policy.
#[tokio::test]
async fn delivery_invalid_endpoint_purges_row_and_counts_failure() {
    let db = make_db_with_users().await;
    let svc = make_service(db.clone());

    // Seed a subscription with a loopback endpoint that will fail
    // delivery-time validation.
    {
        let conn = db.lock().await;
        upsert_subscription(
            &conn,
            1,                                                         // device_id (laptop)
            1,                                                         // user_id (alice)
            &ValidatedEndpoint::for_testing("https://127.0.0.1/push"), // invalid: loopback (for test only)
            &fake_p256dh(),
            &fake_auth(),
        );
    }

    // Confirm row exists before send.
    let exists_before = {
        let conn = db.lock().await;
        crate::pwa_push::db::subscription_exists(&conn, 1, 1)
    };
    assert!(exists_before, "row must exist before send");

    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            "hello",
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
            failed_invalid_endpoint,
            ..
        } => {
            assert_eq!(delivered, 0, "no successful deliveries");
            assert_eq!(gone, 0, "not Gone");
            assert_eq!(failed, 0, "not generic Failed");
            assert_eq!(failed_invalid_endpoint, 1, "must count as invalid_endpoint");
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    // Row must be purged.
    let exists_after = {
        let conn = db.lock().await;
        crate::pwa_push::db::subscription_exists(&conn, 1, 1)
    };
    assert!(
        !exists_after,
        "invalid-endpoint subscription row must be deleted after delivery attempt"
    );
}

/// Delivery-time AllowlistMiss (not just PrivateHost) must also fire
/// `InvalidEndpoint` and purge the row. This covers the case where a
/// subscription was registered before allowlist enforcement was turned on:
/// the endpoint passes IP-block rules but is not in the allowlist.
#[tokio::test]
async fn delivery_allowlist_miss_purges_row_and_counts_failure() {
    let db = make_db_with_users().await;

    // Build a service with enforce=true and the three standard vendor hosts.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vapid.json");
    let vapid = load_or_generate(&path);
    let config = ResolvedPwaPushConfig {
        vapid,
        subject: "mailto:test@example.com".to_string(),
        endpoint_policy: crate::pwa_push::endpoint_validator::EndpointPolicy::new(
            vec![
                "fcm.googleapis.com".to_string(),
                "updates.push.services.mozilla.com".to_string(),
                "web.push.apple.com".to_string(),
            ],
            true, // enforce
        ),
    };
    let mut apps = IndexMap::new();
    apps.insert("graf".to_string(), make_app_config("graf", true, vec![]));
    let svc = Arc::new(PwaPushService::new(
        db.clone(),
        config,
        Arc::new(apps),
        MessagingGlobalConfig {
            default_send_budget: 100,
            max_body_bytes: 4096,
            ..MessagingGlobalConfig::default()
        },
        Arc::from("https://brenn.test"),
        crate::obs::alerting::noop_alert_dispatcher().0,
    ));

    // Seed a subscription with a public-domain endpoint that is NOT in the
    // allowlist (simulates a subscription registered before allowlist enforcement).
    {
        let conn = db.lock().await;
        upsert_subscription(
            &conn,
            1, // device_id (laptop)
            1, // user_id (alice)
            // Public domain but not in allowlist — for test only.
            &ValidatedEndpoint::for_testing("https://evil.example.com/push"),
            &fake_p256dh(),
            &fake_auth(),
        );
    }

    let exists_before = {
        let conn = db.lock().await;
        crate::pwa_push::db::subscription_exists(&conn, 1, 1)
    };
    assert!(exists_before, "row must exist before send");

    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            "hello",
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
            failed_invalid_endpoint,
            ..
        } => {
            assert_eq!(delivered, 0);
            assert_eq!(gone, 0);
            assert_eq!(failed, 0);
            assert_eq!(
                failed_invalid_endpoint, 1,
                "allowlist-miss at delivery time must count as invalid_endpoint"
            );
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    let exists_after = {
        let conn = db.lock().await;
        crate::pwa_push::db::subscription_exists(&conn, 1, 1)
    };
    assert!(
        !exists_after,
        "allowlist-miss subscription row must be deleted after delivery attempt"
    );
}

// -----------------------------------------------------------------------
// Canonicalization tests (correctness-2, security-1, test-1)
// -----------------------------------------------------------------------

/// Sending `pwa_push:Alice` when the DB has `alice` (lowercase) must intern
/// exactly one `messaging_channels` row keyed by the DB-canonical form
/// (`pwa_push:alice`), preventing address fragmentation.
#[tokio::test]
async fn case_mismatch_address_interns_single_canonical_channel_row() {
    let db = make_db_with_users().await; // seeds `alice` (lowercase)
    let svc = make_service(db.clone());

    // Send with mixed-case address.
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:Alice", // uppercase A; DB has "alice"
            "hello",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PushSendResult::Ok { .. }),
        "expected Ok, got {result:?}"
    );

    let conn = db.lock().await;
    // Exactly one channel row — the DB-canonical form.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_channels WHERE address LIKE 'pwa_push:%'",
            [],
            |row| row.get(0),
        )
        .expect("count channels");
    assert_eq!(count, 1, "must intern exactly one channel row");

    let addr: String = conn
        .query_row(
            "SELECT address FROM messaging_channels WHERE address LIKE 'pwa_push:%'",
            [],
            |row| row.get(0),
        )
        .expect("get address");
    assert_eq!(
        addr, "pwa_push:alice",
        "channel address must be DB-canonical casing"
    );
}

/// Sending to `pwa_push:Nobody` (a user that does not exist) must produce
/// an `Ok` result with 0 deliveries (not a panic) and intern the address
/// in lowercase (`pwa_push:nobody`) for determinism.
#[tokio::test]
async fn unknown_user_send_succeeds_with_zero_deliveries_and_lowercased_channel() {
    let db = make_db_with_users().await;
    let svc = make_service(db.clone());

    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:Nobody", // not in DB
            "hello",
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
            assert_eq!(delivered, 0, "unknown user has no subscriptions");
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    // Channel address must be lowercased for determinism across subsequent
    // sends with differing case.
    let addr: String = db
        .lock()
        .await
        .query_row(
            "SELECT address FROM messaging_channels WHERE address LIKE 'pwa_push:Nobody' OR address LIKE 'pwa_push:nobody'",
            [],
            |row| row.get(0),
        )
        .expect("channel row must exist");
    assert_eq!(
        addr, "pwa_push:nobody",
        "unknown-user channel address must be lowercase for determinism"
    );
}

/// The `Ok.address` field must echo the caller's parsed address (not the
/// DB-canonical form) to avoid leaking whether a username exists under a
/// different casing (security-1).
#[tokio::test]
async fn ok_address_echoes_caller_parsed_form_not_canonical() {
    let db = make_db_with_users().await; // seeds `alice` (lowercase)
    let svc = make_service(db);

    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:Alice", // uppercase A; DB has "alice"
            "hello",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    match result {
        PushSendResult::Ok { address, .. } => {
            assert_eq!(
                address, "pwa_push:Alice",
                "Ok.address must echo the caller's form, not the DB-canonical form"
            );
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

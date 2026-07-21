//! App-gate / body-cap / ACL / malformed / budget tests (§2.5 `gate.rs`
//! family): the pre-fanout gating branches of `send` — push-disabled and
//! unknown-app → `MissingSender`, oversize body → `BodyTooLarge` (no budget
//! consumed), ACL deny/permit → `Forbidden` / `Ok`, malformed address →
//! `MalformedAddress`, budget exhaustion → `BudgetExhausted`, and the
//! worst-case-user-id-width size precheck. Peeled out of `tests/mod.rs` per
//! design §2.5; the shared fixtures (`make_db_with_users`, `make_service`,
//! `make_app_config`, `make_pwa_push_config`) remain in `tests/mod.rs` and are
//! reached here via `super::`.

use super::super::*;
use super::{make_app_config, make_db_with_users, make_pwa_push_config, make_service};
use crate::messaging::config::ResolvedMessagingConfig;

#[tokio::test]
async fn push_disabled_returns_missing_sender() {
    // "other" app is in the map but has pwa_push_enabled = false.
    // This exercises the channel-disabled-via-config branch → MissingSender.
    let db = make_db_with_users().await;
    let svc = make_service(db);
    let result = Arc::clone(&svc)
        .send(
            1,
            "other",
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
    assert!(matches!(result, PushSendResult::MissingSender));
}

#[tokio::test]
async fn unknown_app_returns_missing_sender() {
    // Exercises the unknown-app branch (apps.get() returns None) → MissingSender.
    // Distinct from push_disabled_returns_missing_sender which uses an app
    // that IS in the map but has pwa_push_enabled = false.
    let db = make_db_with_users().await;
    let svc = make_service(db);
    let result = Arc::clone(&svc)
        .send(
            1,
            "not-in-map",
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
    assert!(matches!(result, PushSendResult::MissingSender));
}

#[tokio::test]
async fn push_enabled_without_messaging_config_succeeds() {
    // Regression: apps with pwa_push enabled but no [app.messaging] must
    // still send. The budget seed comes from messaging_default_send_budget
    // via messaging_send_budget().
    let db = make_db_with_users().await;
    // Build a service where the "graf" app has pwa_push enabled but
    // messaging = None.
    let app = {
        let mut a = make_app_config("graf", true, vec![]);
        a.messaging = None;
        a
    };
    let mut apps = IndexMap::new();
    apps.insert("graf".to_string(), app);
    let svc = Arc::new(PwaPushService::new(
        db,
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
    // No push subscriptions registered, so deliver count is 0 — but the
    // call must reach PushSendResult::Ok, not MissingSender.
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
    assert!(
        matches!(result, PushSendResult::Ok { .. }),
        "push send with messaging=None should succeed; got {result:?}"
    );
}

#[tokio::test]
async fn body_too_large_rejected_pre_encrypt_no_budget_consumed() {
    let db = make_db_with_users().await;
    let svc = make_service(db.clone());
    // Send an oversize body (max is 4096 in our test config).
    let big_body = "x".repeat(5000);
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            &big_body,
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PushSendResult::BodyTooLarge { .. }));

    // Budget must be unchanged (no row created means remaining = default).
    let conn = db.lock().await;
    let remaining: Option<i64> = conn
        .query_row(
            "SELECT remaining FROM messaging_send_budget WHERE conversation_id = 1",
            [],
            |row| row.get(0),
        )
        .ok();
    // No budget row created means budget was never decremented.
    assert!(
        remaining.is_none(),
        "budget row must not be created on BodyTooLarge"
    );
}

#[tokio::test]
async fn out_of_allowed_users_returns_forbidden() {
    let db = make_db_with_users().await;
    // App with only "bob" allowed.
    let mut apps = IndexMap::new();
    apps.insert(
        "graf".to_string(),
        make_app_config("graf", true, vec!["bob".to_string()]),
    );
    let svc = Arc::new(PwaPushService::new(
        db,
        make_pwa_push_config(),
        Arc::new(apps),
        MessagingGlobalConfig::default(),
        Arc::from("https://brenn.test"),
        crate::obs::alerting::noop_alert_dispatcher().0,
    ));
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
    assert!(matches!(result, PushSendResult::Forbidden { .. }));
}

#[tokio::test]
async fn empty_allowed_users_permits_all_users() {
    // Empty allowed_users = all users allowed (user_has_access semantics).
    let db = make_db_with_users().await;
    let svc = make_service(db.clone()); // make_service uses allowed_users=[]
    // alice has no subscription, but we should NOT get Forbidden.
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
    // Expect Ok with 0 delivered (no subscription), not Forbidden.
    // Also verify budget was decremented and message was persisted — the
    // zero-subscription path falls through to the shared tail which does both.
    match result {
        PushSendResult::Ok {
            delivered,
            remaining_budget,
            ..
        } => {
            assert_eq!(delivered, 0, "no subscriptions to deliver to");
            assert_eq!(
                remaining_budget, 99,
                "budget must decrement by 1 on zero-sub path"
            );
        }
        other => panic!("expected Ok(0 delivered), got {other:?}"),
    }
    let conn = db.lock().await;
    let msg_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messaging_messages WHERE body = 'body'",
            [],
            |row| row.get(0),
        )
        .expect("count messages");
    assert_eq!(
        msg_count, 1,
        "message must be persisted even when 0 subscriptions"
    );
}

#[tokio::test]
async fn malformed_address_rejected() {
    let db = make_db_with_users().await;
    let svc = make_service(db);
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "not:a:valid:address",
            "body",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PushSendResult::MalformedAddress(_)));
}

#[tokio::test]
async fn budget_exhausted_after_limit_reached() {
    let db = make_db_with_users().await;
    let mut apps = IndexMap::new();
    apps.insert(
        "graf".to_string(),
        AppConfig {
            messaging: Some(ResolvedMessagingConfig {
                send_budget: 1,
                subscriptions: vec![],
            }),
            ..make_app_config("graf", true, vec![])
        },
    );
    let svc = Arc::new(PwaPushService::new(
        db,
        make_pwa_push_config(),
        Arc::new(apps),
        MessagingGlobalConfig::default(),
        Arc::from("https://brenn.test"),
        crate::obs::alerting::noop_alert_dispatcher().0,
    ));
    // First send ok (0 delivered, 0 subscriptions).
    let r1 = Arc::clone(&svc)
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
    assert!(matches!(
        r1,
        PushSendResult::Ok {
            remaining_budget: 0,
            ..
        }
    ));
    // Second send exhausted.
    let r2 = Arc::clone(&svc)
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
    assert!(matches!(r2, PushSendResult::BudgetExhausted));
}

#[tokio::test]
async fn size_precheck_uses_worst_case_user_id_width() {
    // Build a payload that fits with real user_id but check that the
    // worst-case (i64::MAX = 19 digits) is used for the size check.
    // We can't easily trigger the precheck to fail here without a real
    // large payload, but we can verify that the code path is exercised
    // for a small payload.
    let db = make_db_with_users().await;
    let svc = make_service(db);
    // Small payload well under limit — should pass.
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            "small body",
            None,
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(matches!(result, PushSendResult::Ok { .. }));
}

/// The plaintext precheck (Step 5) rejects a body whose JSON-wrapped size
/// exceeds the 3993-byte cap, even though it passes the Step 2 body cap (4096).
/// The boundary is computed from the same `PushPayload` template Step 5 builds
/// (worst-case `user_id: i64::MAX`), so a regression swapping the worst-case
/// width for the real (1-digit) user id shrinks the production payload below the
/// cap and returns `Ok`, failing the `BodyTooLarge` assertion.
#[tokio::test]
async fn size_precheck_rejects_payload_over_plaintext_cap() {
    use crate::pwa_push::payload::PushPayload;

    // Deliberately re-states the private PLAINTEXT_CAP in send.rs: a mismatch
    // here is the intended "did you mean to change the wire limit?" signal.
    const PLAINTEXT_CAP: usize = 3993;

    // Fixed JSON overhead of the Step 5 template with an empty body. Passing
    // title=Some("T") in the send call keeps this off the default_title chain.
    let overhead = {
        let template = PushPayload {
            title: "T".to_string(),
            body: String::new(),
            icon: None,
            badge: None,
            tag: None,
            data: None,
            user_id: i64::MAX,
        };
        serde_json::to_vec(&template)
            .expect("PushPayload serialization is infallible")
            .len()
    };
    // If fixed overhead ever reached the cap, the body-length computation below
    // would underflow; fail with a clear message instead.
    assert!(
        overhead <= PLAINTEXT_CAP,
        "PushPayload fixed overhead ({overhead}) reached the {PLAINTEXT_CAP} plaintext cap"
    );

    // ASCII 'x' adds exactly one JSON byte per char (no escaping), so the
    // serialized template is exactly PLAINTEXT_CAP+1 bytes — one over the cap.
    // The body itself is always < 4096, so Step 2 passes by construction.
    let over_body = "x".repeat(PLAINTEXT_CAP - overhead + 1);

    let db = make_db_with_users().await;
    let svc = make_service(db.clone());

    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            &over_body,
            Some("T"),
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    // max == 3993 proves Step 5 fired (Step 2 would report max == 4096);
    // len == 3994 pins the serialized size.
    assert!(
        matches!(
            result,
            PushSendResult::BodyTooLarge { len, max }
                if len == PLAINTEXT_CAP + 1 && max == PLAINTEXT_CAP
        ),
        "expected Step 5 BodyTooLarge{{len:{},max:{PLAINTEXT_CAP}}}, got {result:?}",
        PLAINTEXT_CAP + 1
    );

    // Step 5 rejects before budget consumption: no budget row for conversation 1.
    // Scope this guard — the async DB mutex is re-acquired inside the next send,
    // so holding it across that call would deadlock. COUNT(*) always returns one
    // row, so a broken query surfaces as a panic rather than a vacuous pass.
    {
        let conn = db.lock().await;
        let budget_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messaging_send_budget WHERE conversation_id = 1",
                [],
                |row| row.get(0),
            )
            .expect("budget-row count query must succeed");
        assert_eq!(
            budget_rows, 0,
            "budget row must not be created on size-precheck rejection"
        );
    }

    // Boundary pass: one byte shorter serializes to exactly 3993 (== cap, not >),
    // so it proceeds (alice has no subscriptions → Ok with 0 delivered). Pins the
    // `>` comparison against `>=`.
    let at_cap_body = "x".repeat(PLAINTEXT_CAP - overhead);
    let result = Arc::clone(&svc)
        .send(
            1,
            "graf",
            "pwa_push:alice",
            &at_cap_body,
            Some("T"),
            86400,
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(result, PushSendResult::Ok { .. }),
        "body serializing to exactly 3993 must pass the > cap check; got {result:?}"
    );
}

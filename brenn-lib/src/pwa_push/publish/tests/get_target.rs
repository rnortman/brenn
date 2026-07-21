//! `get_target` tests (§2.5 `get_target.rs` family): device-address and
//! user-address resolution (max-last-seen tie-breaking across device
//! `last_seen_at` and subscription `last_used_at`), not-found / forbidden /
//! disabled / unknown-app outcomes. Peeled out of `tests/mod.rs` per design
//! §2.5; the shared fixtures (`make_db_with_users`, `make_service`,
//! `make_app_config`, `make_pwa_push_config`) remain in `tests/mod.rs` and are
//! reached here via `super::`. `make_db_for_get_target` is used only by this
//! family, so per §2.4 it lives here rather than in `tests/mod.rs`.

use super::super::*;
use super::{make_app_config, make_db_with_users, make_pwa_push_config, make_service};
use crate::pwa_push::db::upsert_subscription;
use crate::pwa_push::endpoint_validator::ValidatedEndpoint;
use crate::pwa_push::test_helpers::{fake_auth, fake_p256dh};

/// Build a DB with alice@laptop subscription (device_last_seen_at = T2,
/// sub.last_used_at = T1 so device timestamp wins) and alice@phone
/// subscription (device_last_seen_at = T1, sub.last_used_at = T2 so sub
/// timestamp wins). Used to exercise both orderings of the max().
async fn make_db_for_get_target() -> crate::db::Db {
    let db = make_db_with_users().await;
    {
        let conn = db.lock().await;
        let t1 = "2024-01-01T00:00:00Z";
        let t2 = "2024-06-01T00:00:00Z";
        // Update device last_seen_at: laptop=T2, phone=T1.
        conn.execute(
            "UPDATE devices SET last_seen_at = ?1 WHERE id = 1",
            rusqlite::params![t2],
        )
        .expect("set laptop last_seen_at");
        conn.execute(
            "UPDATE devices SET last_seen_at = ?1 WHERE id = 2",
            rusqlite::params![t1],
        )
        .expect("set phone last_seen_at");
        // alice@laptop: sub.last_used_at=T1 → device_last_seen wins (T2).
        upsert_subscription(
            &conn,
            1,
            1,
            &ValidatedEndpoint::for_testing("https://ep1.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
        conn.execute(
            "UPDATE pwa_push_subscriptions SET last_used_at = ?1 WHERE device_id = 1 AND user_id = 1",
            rusqlite::params![t1],
        )
        .expect("set alice@laptop last_used_at");
        // alice@phone: sub.last_used_at=T2 → sub timestamp wins (T2).
        upsert_subscription(
            &conn,
            2,
            1,
            &ValidatedEndpoint::for_testing("https://ep2.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
        conn.execute(
            "UPDATE pwa_push_subscriptions SET last_used_at = ?1 WHERE device_id = 2 AND user_id = 1",
            rusqlite::params![t2],
        )
        .expect("set alice@phone last_used_at");
    }
    db
}

#[tokio::test]
async fn get_target_device_address_returns_entry_with_max_last_seen() {
    let db = make_db_for_get_target().await;
    let svc = make_service(db);
    let addr =
        crate::pwa_push::targets::parse_pwa_push_address("pwa_push:alice@laptop").expect("parse");
    let result = svc.get_target("graf", &addr).await;
    let e = match result {
        GetTargetResult::Found(e) => e,
        other => panic!("expected Found, got {other:?}"),
    };
    assert_eq!(e.user, "alice");
    assert_eq!(e.device, Some("laptop".to_string()));
    // device_last_seen_at(T2) > sub.last_used_at(T1) → T2 wins.
    assert_eq!(e.last_seen_at, "2024-06-01T00:00:00Z");
}

#[tokio::test]
async fn get_target_device_address_sub_wins_when_newer() {
    let db = make_db_for_get_target().await;
    let svc = make_service(db);
    let addr =
        crate::pwa_push::targets::parse_pwa_push_address("pwa_push:alice@phone").expect("parse");
    let result = svc.get_target("graf", &addr).await;
    let e = match result {
        GetTargetResult::Found(e) => e,
        other => panic!("expected Found, got {other:?}"),
    };
    // sub.last_used_at(T2) > device_last_seen_at(T1) → T2 wins.
    assert_eq!(e.last_seen_at, "2024-06-01T00:00:00Z");
}

#[tokio::test]
async fn get_target_device_address_absent_returns_not_found() {
    let db = make_db_with_users().await; // no subscriptions
    let svc = make_service(db);
    let addr = crate::pwa_push::targets::parse_pwa_push_address("pwa_push:alice@laptop").unwrap();
    assert!(matches!(
        svc.get_target("graf", &addr).await,
        GetTargetResult::NotFound
    ));
}

#[tokio::test]
async fn get_target_user_address_returns_max_across_devices() {
    let db = make_db_for_get_target().await;
    let svc = make_service(db);
    let addr = crate::pwa_push::targets::parse_pwa_push_address("pwa_push:alice").unwrap();
    let result = svc.get_target("graf", &addr).await;
    let e = match result {
        GetTargetResult::Found(e) => e,
        other => panic!("expected Found, got {other:?}"),
    };
    assert_eq!(e.user, "alice");
    assert!(e.device.is_none());
    // Both laptop and phone effective last_seen = T2; max = T2.
    assert_eq!(e.last_seen_at, "2024-06-01T00:00:00Z");
}

#[tokio::test]
async fn get_target_user_address_no_subscription_returns_not_found() {
    let db = make_db_with_users().await;
    let svc = make_service(db);
    let addr = crate::pwa_push::targets::parse_pwa_push_address("pwa_push:alice").unwrap();
    assert!(matches!(
        svc.get_target("graf", &addr).await,
        GetTargetResult::NotFound
    ));
}

#[tokio::test]
async fn get_target_user_access_denied_returns_forbidden() {
    let db = make_db_for_get_target().await;
    let mut apps = IndexMap::new();
    // Only "bob" allowed — alice is forbidden.
    apps.insert(
        "graf".to_string(),
        make_app_config("graf", true, vec!["bob".to_string()]),
    );
    let svc = PwaPushService::new(
        db,
        make_pwa_push_config(),
        Arc::new(apps),
        MessagingGlobalConfig::default(),
        Arc::from("https://brenn.test"),
        crate::obs::alerting::noop_alert_dispatcher().0,
    );
    let addr = crate::pwa_push::targets::parse_pwa_push_address("pwa_push:alice").unwrap();
    assert!(matches!(
        svc.get_target("graf", &addr).await,
        GetTargetResult::Forbidden
    ));
    let addr2 = crate::pwa_push::targets::parse_pwa_push_address("pwa_push:alice@laptop").unwrap();
    assert!(matches!(
        svc.get_target("graf", &addr2).await,
        GetTargetResult::Forbidden
    ));
}

#[tokio::test]
async fn get_target_pwa_push_disabled_returns_none() {
    let db = make_db_for_get_target().await;
    let svc = make_service(db);
    let addr = crate::pwa_push::targets::parse_pwa_push_address("pwa_push:alice").unwrap();
    // "other" app has pwa_push disabled.
    assert!(matches!(
        svc.get_target("other", &addr).await,
        GetTargetResult::Disabled
    ));
}

#[tokio::test]
async fn get_target_unknown_app_returns_not_found() {
    let db = make_db_for_get_target().await;
    let svc = make_service(db);
    let addr = crate::pwa_push::targets::parse_pwa_push_address("pwa_push:alice").unwrap();
    // "nonexistent" is not in the apps map — server config/routing bug.
    assert!(matches!(
        svc.get_target("nonexistent", &addr).await,
        GetTargetResult::NotFound
    ));
}

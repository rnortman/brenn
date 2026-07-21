//! `list_targets` tests: the app-visible push-target listing
//! (`list_targets_impl`, service.rs). Covers the `pwa_push_enabled` /
//! unknown-app gates, per-app ACL filtering (`user_has_access`),
//! assigned-over-guessed device slug preference, `effective_last_seen` =
//! `max(device.last_seen_at, subscription.last_used_at)` (per entry and as the
//! per-user fan-out max), the one-fan-out-entry-per-user + ordering contract,
//! and stale-subscription exclusion.
//!
//! Stale-subscription exclusion is **not** service-local logic: it is enforced
//! by the `TOP_USER_JOIN` inside `list_all_subscriptions_with_device_info`
//! (db.rs), which keeps only the max-`last_seen_at` user per device. The
//! full-listing test exercises that end-to-end through the service (via SQL),
//! not as a service-layer branch.
//!
//! Mirrors `get_target.rs`: shared fixtures (`make_db_with_users`,
//! `make_service`, `make_app_config`, `make_pwa_push_config`) live in
//! `tests/mod.rs` and are reached via `super::`; `make_db_for_list_targets` is
//! used only here so it lives in this file.

use super::super::*;
use super::{make_app_config, make_db_with_users, make_service, make_service_with_apps};
use crate::pwa_push::db::upsert_subscription;
use crate::pwa_push::endpoint_validator::ValidatedEndpoint;
use crate::pwa_push::test_helpers::{fake_auth, fake_p256dh};

const T1: &str = "2024-01-01T00:00:00Z";
const T2: &str = "2024-06-01T00:00:00Z";
const T3: &str = "2024-09-01T00:00:00Z";

/// Build on `make_db_with_users` (alice=1, bob=2; laptop=1 guessed `laptop`,
/// phone=2 guessed `phone`; device_users (1,1) and (2,1)) to exercise every
/// `list_targets` behavior:
///
/// - laptop `last_seen_at` = T2, phone = T1.
/// - alice@laptop sub `last_used_at` = T1 → device T2 wins (effective T2).
/// - alice@phone sub `last_used_at` = T3 → sub T3 wins (effective T3). Alice's
///   two entries thus have distinct effective values, so the per-user fan-out
///   max is non-degenerate (expected T3; a keep-first/min regression yields T2).
/// - device_users (1,1).assigned_slug = `workstation` → laptop entry uses the
///   assigned slug; phone (NULL assigned) falls through to guessed `phone`.
/// - device 3 (`tablet`, `last_seen_at` = T2) with bob current (T2) and alice
///   stale (T1), plus subscriptions for both. `TOP_USER_JOIN` drops alice@tablet
///   and keeps bob@tablet.
async fn make_db_for_list_targets() -> crate::db::Db {
    let db = make_db_with_users().await;
    {
        let conn = db.lock().await;
        // Device last_seen_at: laptop = T2, phone = T1.
        conn.execute(
            "UPDATE devices SET last_seen_at = ?1 WHERE id = 1",
            rusqlite::params![T2],
        )
        .expect("set laptop last_seen_at");
        conn.execute(
            "UPDATE devices SET last_seen_at = ?1 WHERE id = 2",
            rusqlite::params![T1],
        )
        .expect("set phone last_seen_at");
        // Assigned slug for alice@laptop → the entry must use `workstation`.
        conn.execute(
            "UPDATE device_users SET assigned_slug = 'workstation' \
             WHERE device_id = 1 AND user_id = 1",
            [],
        )
        .expect("set alice@laptop assigned_slug");

        // alice@laptop: sub.last_used_at = T1 → device T2 wins.
        upsert_subscription(
            &conn,
            1,
            1,
            &ValidatedEndpoint::for_testing("https://ep1.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
        conn.execute(
            "UPDATE pwa_push_subscriptions SET last_used_at = ?1 \
             WHERE device_id = 1 AND user_id = 1",
            rusqlite::params![T1],
        )
        .expect("set alice@laptop last_used_at");
        // alice@phone: sub.last_used_at = T3 → sub T3 wins.
        upsert_subscription(
            &conn,
            2,
            1,
            &ValidatedEndpoint::for_testing("https://ep2.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
        conn.execute(
            "UPDATE pwa_push_subscriptions SET last_used_at = ?1 \
             WHERE device_id = 2 AND user_id = 1",
            rusqlite::params![T3],
        )
        .expect("set alice@phone last_used_at");

        // Device 3 (tablet): bob is current (T2), alice is stale (T1).
        conn.execute(
            "INSERT INTO devices (id, token, guessed_slug, last_seen_at, created_at) \
             VALUES (3, 'tok3', 'tablet', ?1, ?1)",
            rusqlite::params![T2],
        )
        .expect("insert tablet device");
        conn.execute(
            "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at) \
             VALUES (3, 2, ?1, ?1)",
            rusqlite::params![T2],
        )
        .expect("insert bob@tablet device_user");
        conn.execute(
            "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at) \
             VALUES (3, 1, ?1, ?1)",
            rusqlite::params![T1],
        )
        .expect("insert alice@tablet device_user");
        // Subscriptions for both users on tablet; only bob's survives TOP_USER_JOIN.
        upsert_subscription(
            &conn,
            3,
            2,
            &ValidatedEndpoint::for_testing("https://ep3.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
        conn.execute(
            "UPDATE pwa_push_subscriptions SET last_used_at = ?1 \
             WHERE device_id = 3 AND user_id = 2",
            rusqlite::params![T1],
        )
        .expect("set bob@tablet last_used_at");
        upsert_subscription(
            &conn,
            3,
            1,
            &ValidatedEndpoint::for_testing("https://ep4.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
        conn.execute(
            "UPDATE pwa_push_subscriptions SET last_used_at = ?1 \
             WHERE device_id = 3 AND user_id = 1",
            rusqlite::params![T1],
        )
        .expect("set alice@tablet last_used_at");
    }
    db
}

/// Project entries to comparable tuples. `PushTargetEntry` derives no
/// `PartialEq`, so the tests compare `(address, user, device, last_seen_at)`.
fn shape(entries: Vec<PushTargetEntry>) -> Vec<(String, String, Option<String>, String)> {
    entries
        .into_iter()
        .map(|e| (e.address, e.user, e.device, e.last_seen_at))
        .collect()
}

#[tokio::test]
async fn list_targets_unknown_app_returns_empty() {
    let db = make_db_for_list_targets().await;
    let svc = make_service(db);
    assert!(svc.list_targets("nonexistent").await.is_empty());
}

#[tokio::test]
async fn list_targets_disabled_app_returns_empty() {
    // "other" is in the map but has pwa_push disabled.
    let db = make_db_for_list_targets().await;
    let svc = make_service(db);
    assert!(svc.list_targets("other").await.is_empty());
}

#[tokio::test]
async fn list_targets_no_subscriptions_returns_empty() {
    // No subscriptions → no fan-out entries at all.
    let db = make_db_with_users().await;
    let svc = make_service(db);
    assert!(svc.list_targets("graf").await.is_empty());
}

#[tokio::test]
async fn list_targets_full_listing_shape() {
    // Stock make_service (empty allowed_users = all users allowed).
    // Covers assigned>guessed slug, effective_last_seen (per-entry and per-user
    // max), one fan-out per user + ordering, and stale-sub SQL exclusion of
    // alice@tablet.
    let db = make_db_for_list_targets().await;
    let svc = make_service(db);
    let got = shape(svc.list_targets("graf").await);
    let expected: Vec<(String, String, Option<String>, String)> = vec![
        // Fan-out entries first, sorted by user.
        (
            "pwa_push:alice".into(),
            "alice".into(),
            None,
            T3.into(), // max(T2 workstation, T3 phone)
        ),
        ("pwa_push:bob".into(), "bob".into(), None, T2.into()),
        // Device entries, SQL-ordered by username then device_id.
        (
            "pwa_push:alice@workstation".into(),
            "alice".into(),
            Some("workstation".into()),
            T2.into(), // device T2 > sub T1
        ),
        (
            "pwa_push:alice@phone".into(),
            "alice".into(),
            Some("phone".into()),
            T3.into(), // sub T3 > device T1
        ),
        (
            "pwa_push:bob@tablet".into(),
            "bob".into(),
            Some("tablet".into()),
            T2.into(),
        ),
    ];
    assert_eq!(got, expected);
}

#[tokio::test]
async fn list_targets_acl_excludes_disallowed_users() {
    // graf app allows only "bob" — alice must not appear anywhere.
    let db = make_db_for_list_targets().await;
    let mut apps = IndexMap::new();
    apps.insert(
        "graf".to_string(),
        make_app_config("graf", true, vec!["bob".to_string()]),
    );
    let svc = make_service_with_apps(db, apps);
    let got = shape(svc.list_targets("graf").await);
    let expected: Vec<(String, String, Option<String>, String)> = vec![
        ("pwa_push:bob".into(), "bob".into(), None, T2.into()),
        (
            "pwa_push:bob@tablet".into(),
            "bob".into(),
            Some("tablet".into()),
            T2.into(),
        ),
    ];
    // Exact-listing compare is the whole contract: any stray alice entry fails it.
    assert_eq!(got, expected);
}

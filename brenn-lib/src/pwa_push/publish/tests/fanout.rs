//! Fanout / concurrency tests (§2.5 `fanout.rs` family, §4.1–4.6): parallel
//! delivery, the publish-wide cap firing on a hung endpoint (and its warn-log
//! event), per-send SQL-lock (`BEGIN`/`COMMIT`) accounting, network-error and
//! task-panic outcome mapping, mixed-outcome counter cross-contamination, and
//! the zero-subscription no-op. Peeled out of `tests/mod.rs` per design §2.5.
//!
//! The shared mocks reused by the device family (`MockResponse`,
//! `MockHttpPoster`, `make_service_with_poster`, `valid_p256dh`, `valid_auth`)
//! stay in `tests/mod.rs` as `pub(super)` and are reached here via `super::`.
//! The fanout-only helpers — the `CapOverride` guard, the scoped tracing-capture
//! cluster, the thread-local SQL-trace counters, and `seed_subscriptions` — live
//! here because no other test file uses them (§2.4, narrowest home).

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
use tracing_subscriber::layer::SubscriberExt as _;

// The `CapOverride` guard writes this test-only atomic. It is imported privately
// (not re-exported) into `publish/mod.rs`, so `super::super::*` does not surface
// it — reach it via the (descendant-visible) `delivery` submodule directly.
use super::super::delivery::PUBLISH_WIDE_CAP_OVERRIDE_MS;

/// RAII guard that resets `PUBLISH_WIDE_CAP_OVERRIDE_MS` to `u64::MAX` on drop,
/// ensuring test cap overrides do not leak to subsequent tests in the process.
struct CapOverride;

impl CapOverride {
    fn set(ms: u64) -> Self {
        PUBLISH_WIDE_CAP_OVERRIDE_MS.store(ms, std::sync::atomic::Ordering::Relaxed);
        Self
    }
}

impl Drop for CapOverride {
    fn drop(&mut self) {
        PUBLISH_WIDE_CAP_OVERRIDE_MS.store(u64::MAX, std::sync::atomic::Ordering::Relaxed);
    }
}

// -----------------------------------------------------------------------
// Tracing capture helpers for §4.2 / scope-2 and §4.5 log-event assertions.
// -----------------------------------------------------------------------

/// Per-test sinks for cap-fired and panic-warn tracing events. Constructed
/// fresh for each test via `install_scoped_cap_capture`; the `_guard` keeps the
/// thread-local default alive for the test's scope.
struct CapCapture {
    cap_sink: Arc<std::sync::Mutex<Vec<usize>>>,
    panic_sink: Arc<std::sync::Mutex<Vec<(i64, String)>>>,
    _guard: tracing::subscriber::DefaultGuard,
}

/// Build fresh per-test sinks, install a thread-local tracing default that
/// pushes into them, and return the sinks plus the guard. The guard restores
/// the previous thread-local default on drop (i.e. at test return).
///
/// Both cap-firing tests must use `#[tokio::test]` (current_thread flavor) so
/// that all `tracing::warn!` calls from spawned tasks fire on the test thread,
/// where this thread-local default applies.
///
// Note: a near-identical scoped-tracing-capture pattern exists at
// `brenn/src/active_bridge/compaction.rs::capture_trigger_label` —
// different API shape (closure-wrapper there vs guard-struct here)
// and different captured fields. If you find yourself about to add
// a third one, talk to these first.
fn install_scoped_cap_capture() -> CapCapture {
    let cap_sink = Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
    let panic_sink = Arc::new(std::sync::Mutex::new(Vec::<(i64, String)>::new()));
    let layer = PublishWideCapLayer {
        cap_sink: Arc::clone(&cap_sink),
        panic_sink: Arc::clone(&panic_sink),
    };
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);
    CapCapture {
        cap_sink,
        panic_sink,
        _guard,
    }
}

/// Layer that captures `aborted_count` and `(subscription_id, panic_payload)`
/// fields from events emitted by `pwa_push::publish`. Each instance carries its
/// own sinks; no process-global state.
struct PublishWideCapLayer {
    cap_sink: Arc<std::sync::Mutex<Vec<usize>>>,
    panic_sink: Arc<std::sync::Mutex<Vec<(i64, String)>>>,
}

struct PublishWideCapVisitor {
    aborted_count: Option<usize>,
    subscription_id: Option<i64>,
    panic_payload: Option<String>,
}

impl tracing::field::Visit for PublishWideCapVisitor {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "aborted_count" {
            self.aborted_count = Some(value as usize);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if field.name() == "subscription_id" {
            self.subscription_id = Some(value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "panic_payload" {
            self.panic_payload = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PublishWideCapLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if !event
            .metadata()
            .module_path()
            .is_some_and(|m| m.contains("pwa_push::publish"))
        {
            return;
        }
        let mut visitor = PublishWideCapVisitor {
            aborted_count: None,
            subscription_id: None,
            panic_payload: None,
        };
        event.record(&mut visitor);
        if let Some(count) = visitor.aborted_count {
            self.cap_sink.lock().unwrap().push(count);
        }
        if let (Some(sub_id), Some(payload)) = (visitor.subscription_id, visitor.panic_payload) {
            self.panic_sink.lock().unwrap().push((sub_id, payload));
        }
    }
}

// Thread-local counters used by `install_sql_trace`.
// rusqlite's `Connection::trace` accepts only bare `fn` pointers, not
// closures. We use thread-locals to pass state into the callback.
thread_local! {
    static TL_BEGIN_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static TL_COMMIT_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn sql_trace_fn(event: rusqlite::trace::TraceEvent<'_>) {
    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
        if sql.starts_with("BEGIN") {
            TL_BEGIN_COUNT.with(|c| c.set(c.get() + 1));
        } else if sql.starts_with("COMMIT") {
            TL_COMMIT_COUNT.with(|c| c.set(c.get() + 1));
        }
    }
}

/// Install a SQL statement counter on a `Db` connection (test helper).
///
/// Counts `BEGIN` and `COMMIT` statements via `Connection::trace_v2`. Values
/// are read back via `TL_BEGIN_COUNT` / `TL_COMMIT_COUNT` on the same thread.
async fn install_sql_trace(db: &crate::db::Db) {
    use rusqlite::trace::TraceEventCodes;
    let conn = db.lock().await;
    conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(sql_trace_fn));
}

fn read_begin_count() -> usize {
    TL_BEGIN_COUNT.with(|c| c.get())
}

fn read_commit_count() -> usize {
    TL_COMMIT_COUNT.with(|c| c.get())
}

/// Seed N subscriptions for alice via `upsert_subscription` with mock
/// endpoints `https://mock.example.com/push/<n>`.
///
/// `make_db_with_users` seeds exactly 2 devices (id=1, id=2). For N>2 we
/// add extra devices before seeding subscriptions.
async fn seed_subscriptions(db: &crate::db::Db, count: usize) -> Vec<String> {
    let conn = db.lock().await;
    // Ensure enough device rows exist (make_db_with_users seeds 2).
    if count > 2 {
        let now = crate::db::format_ts_for_db(chrono::Utc::now());
        for i in 3..=(count as i64) {
            conn.execute_batch(&format!(
                "INSERT OR IGNORE INTO devices (id, token, guessed_slug, last_seen_at, created_at)
                     VALUES ({i}, 'tok_extra_{i}', 'device_{i}', '{now}', '{now}');
                 INSERT OR IGNORE INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                     VALUES ({i}, 1, '{now}', '{now}');"
            ))
            .expect("seed extra device");
        }
    }
    let mut endpoints = Vec::new();
    for i in 0..count {
        let ep = format!("https://mock.example.com/push/{i}");
        let device_id = (i + 1) as i64;
        upsert_subscription(
            &conn,
            device_id,
            1, // alice user_id
            &ValidatedEndpoint::for_testing(&ep),
            &valid_p256dh(),
            &valid_auth(),
        );
        endpoints.push(ep);
    }
    endpoints
}

/// §4.1 — 3 subs each taking ~200ms in parallel should finish in <450ms,
/// well under the ~600ms sequential lower bound.
#[tokio::test(flavor = "multi_thread")]
async fn fanout_completes_in_parallel_not_serial() {
    let db = make_db_with_users().await;
    let eps = seed_subscriptions(&db, 3).await;

    let mut responses = StdHashMap::new();
    for ep in &eps {
        responses.insert(
            ep.clone(),
            MockResponse::Ok {
                status: 201,
                delay: Duration::from_millis(200),
            },
        );
    }
    let poster = MockHttpPoster::new(responses);
    let svc = make_service_with_poster(db, poster);

    let t0 = std::time::Instant::now();
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
    let elapsed = t0.elapsed();

    match result {
        PushSendResult::Ok {
            delivered,
            gone,
            failed,
            ..
        } => {
            assert_eq!(delivered, 3, "all 3 must be delivered");
            assert_eq!(gone, 0);
            assert_eq!(failed, 0);
        }
        other => panic!("expected Ok, got {other:?}"),
    }
    // Concurrent: ~200ms. Sequential: ~600ms.
    assert!(
        elapsed < Duration::from_millis(450),
        "fanout must be concurrent; elapsed={elapsed:?}"
    );
}

/// §4.2 — Publish-wide cap fires when one endpoint hangs past the (overridden) cap.
#[tokio::test(flavor = "multi_thread")]
async fn publish_wide_cap_fires_on_hung_endpoint() {
    // Override cap to 500ms; guard resets it to u64::MAX on drop.
    let _cap = CapOverride::set(500);

    let db = make_db_with_users().await;
    let eps = seed_subscriptions(&db, 3).await;

    let mut responses = StdHashMap::new();
    // First two respond fast.
    responses.insert(
        eps[0].clone(),
        MockResponse::Ok {
            status: 201,
            delay: Duration::from_millis(100),
        },
    );
    responses.insert(
        eps[1].clone(),
        MockResponse::Ok {
            status: 201,
            delay: Duration::from_millis(100),
        },
    );
    // Third hangs.
    responses.insert(eps[2].clone(), MockResponse::Hang);

    let poster = MockHttpPoster::new(responses);
    let svc = make_service_with_poster(db, poster);

    let t0 = std::time::Instant::now();
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
    let elapsed = t0.elapsed();

    match result {
        PushSendResult::Ok {
            delivered, failed, ..
        } => {
            assert_eq!(delivered, 2, "two fast subs must be delivered");
            assert_eq!(failed, 1, "hung sub must be counted as failed");
        }
        other => panic!("expected Ok, got {other:?}"),
    }
    assert!(
        elapsed >= Duration::from_millis(500),
        "must wait for cap; elapsed={elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(800),
        "must not wait much beyond cap; elapsed={elapsed:?}"
    );
}

/// §4.3 — After fanout, the DB batch write acquires exactly one BEGIN/COMMIT pair.
///
/// Note: uses thread-local SQL counters. The current-thread runtime
/// (`#[tokio::test]` default, no `flavor`) ensures all `Db` mutex
/// acquisitions — and thus all `rusqlite` trace callbacks — execute on the
/// same OS thread as the `TL_*` thread-locals.
///
/// We record the baseline BEGIN/COMMIT count immediately before `send()` and
/// assert the delta is exactly 1, not the absolute value. This accounts for
/// the one implicit `BEGIN` issued by `insert_message_with_pushes` in the
/// pre-fanout phase (step 7), which is a separate DB operation.
///
/// **Runtime/task invariant:** Two code-change vectors silently break the counters
/// without crashing or panicking — the test simply reports wrong values, which can
/// cause the assertion to pass or fail spuriously:
///
/// 1. Switching this test to `#[tokio::test(flavor = "multi_thread")]` moves the
///    `rusqlite` trace callbacks onto worker threads where the `TL_*` thread-locals
///    are never initialized, so the counts stay at zero.
/// 2. Moving DB operations into `tokio::spawn(...)` (or any other off-current-task
///    execution) has the same effect even with the default current-thread runtime,
///    because the spawned task may run on a different thread.
///
/// If the runtime flavor changes, replace `TL_BEGIN_COUNT` / `TL_COMMIT_COUNT` with
/// an `Arc<Mutex<(usize, usize)>>` shared between the trace callback and the
/// assertion site (passed in via a global or a `Connection` wrapper).
#[tokio::test]
async fn outcome_batch_acquires_db_lock_once() {
    let db = make_db_with_users().await;
    let eps = seed_subscriptions(&db, 3).await;

    install_sql_trace(&db).await;

    let mut responses = StdHashMap::new();
    responses.insert(
        eps[0].clone(),
        MockResponse::Ok {
            status: 201,
            delay: Duration::ZERO,
        },
    ); // Delivered
    responses.insert(
        eps[1].clone(),
        MockResponse::Ok {
            status: 410,
            delay: Duration::ZERO,
        },
    ); // Gone
    responses.insert(
        eps[2].clone(),
        MockResponse::Ok {
            status: 500,
            delay: Duration::ZERO,
        },
    ); // Failed

    let poster = MockHttpPoster::new(responses);
    let svc = make_service_with_poster(db, poster);

    // Snapshot counters just before send().
    let begin_before = read_begin_count();
    let commit_before = read_commit_count();

    let _ = Arc::clone(&svc)
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

    // The batch write (step 10) must issue exactly one BEGIN/COMMIT pair.
    // Other BEGINs in send() (e.g. insert_message_with_pushes) are accounted
    // for in the delta.
    let delta_begin = read_begin_count() - begin_before;
    let delta_commit = read_commit_count() - commit_before;

    // send() issues exactly two BEGIN/COMMIT pairs: one from
    // insert_message_with_pushes (pre-fanout, step 7) and one from our
    // batch write (post-fanout, step 10). We assert the total delta is 2
    // (not 1) to confirm no extra transactions were introduced.
    assert_eq!(
        delta_begin, 2,
        "expected 2 BEGINs per send: one for message insert, one for batch write"
    );
    assert_eq!(
        delta_commit, 2,
        "expected 2 COMMITs per send: one for message insert, one for batch write"
    );
}

/// Device-targeted sends on the non-happy paths (stale user, no subscription)
/// must issue exactly two BEGIN/COMMIT pairs — same as the happy path — because
/// `lookup_device_subscription` now resolves everything in a single lock acquire.
///
/// A regression that re-introduced a second lock acquire (e.g. by reverting to
/// the old two-step pattern) would produce delta_begin == 3.
#[tokio::test]
async fn device_targeted_acquires_db_lock_once_on_non_happy_paths() {
    // --- Stale-user path ---
    let db = make_db_with_users().await;
    {
        let conn = db.lock().await;
        let later = "2030-01-01T00:00:00+00:00";
        conn.execute_batch(&format!(
            "INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (1, 2, '{later}', '{later}');"
        ))
        .expect("make bob current on laptop");
        upsert_subscription(
            &conn,
            1,
            1,
            &ValidatedEndpoint::for_testing("https://ep.example.com"),
            &fake_p256dh(),
            &fake_auth(),
        );
    }
    install_sql_trace(&db).await;
    let begin_before = read_begin_count();
    let commit_before = read_commit_count();
    let poster = MockHttpPoster::new(StdHashMap::new());
    let svc = make_service_with_poster(db, poster);
    let _ = Arc::clone(&svc)
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
    let delta_begin = read_begin_count() - begin_before;
    let delta_commit = read_commit_count() - commit_before;
    assert_eq!(
        delta_begin, 2,
        "stale-user Device path: expected 2 BEGINs (insert_message + batch), got {delta_begin}"
    );
    assert_eq!(
        delta_commit, 2,
        "stale-user Device path: expected 2 COMMITs (insert_message + batch), got {delta_commit}"
    );

    // --- Not-found path ---
    let db2 = make_db_with_users().await;
    // No subscription seeded — pure NotFound path.
    install_sql_trace(&db2).await;
    let begin_before2 = read_begin_count();
    let commit_before2 = read_commit_count();
    let poster2 = MockHttpPoster::new(StdHashMap::new());
    let svc2 = make_service_with_poster(db2, poster2);
    let _ = Arc::clone(&svc2)
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
    let delta_begin2 = read_begin_count() - begin_before2;
    let delta_commit2 = read_commit_count() - commit_before2;
    assert_eq!(
        delta_begin2, 2,
        "not-found Device path: expected 2 BEGINs (insert_message + batch), got {delta_begin2}"
    );
    assert_eq!(
        delta_commit2, 2,
        "not-found Device path: expected 2 COMMITs (insert_message + batch), got {delta_commit2}"
    );
}

/// §4.4 — Mixed outcomes (including stale-user) map to correct counters with
/// no cross-contamination across all five outcome types.
///
/// Five subscriptions across five devices:
/// - device 1: alice, 201 → Delivered
/// - device 2: alice, 410 → Gone
/// - device 3: bob is current user, alice's sub is stale → failed_stale_user
/// - device 4: alice, 500 → Failed
/// - device 5: alice, loopback endpoint → InvalidEndpoint
#[tokio::test(flavor = "multi_thread")]
async fn counters_match_sequential_for_mixed_outcomes_and_stale_user_invariant() {
    let db = make_db_with_users().await;
    {
        let conn = db.lock().await;
        let now = crate::db::format_ts_for_db(chrono::Utc::now());

        // Device 3: current user is bob; alice's sub on it is stale.
        conn.execute_batch(&format!(
            "INSERT INTO devices (id, token, guessed_slug, last_seen_at, created_at)
                 VALUES (3, 'tok3', 'tablet', '{now}', '{now}');
             INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (3, 2, '{now}', '{now}');
             INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (3, 1, '{now}', '2020-01-01T00:00:00+00:00');
             INSERT INTO devices (id, token, guessed_slug, last_seen_at, created_at)
                 VALUES (4, 'tok4', 'watch', '{now}', '{now}');
             INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (4, 1, '{now}', '{now}');
             INSERT INTO devices (id, token, guessed_slug, last_seen_at, created_at)
                 VALUES (5, 'tok5', 'tv', '{now}', '{now}');
             INSERT INTO device_users (device_id, user_id, first_seen_at, last_seen_at)
                 VALUES (5, 1, '{now}', '{now}');
             "
        ))
        .expect("seed devices 3-5");

        // Sub on device 1 → 201 (Delivered).
        upsert_subscription(
            &conn,
            1,
            1,
            &ValidatedEndpoint::for_testing("https://mock.example.com/push/0"),
            &valid_p256dh(),
            &valid_auth(),
        );
        // Sub on device 2 → 410 (Gone).
        upsert_subscription(
            &conn,
            2,
            1,
            &ValidatedEndpoint::for_testing("https://mock.example.com/push/1"),
            &valid_p256dh(),
            &valid_auth(),
        );
        // Sub on device 3 for alice — stale (bob is current user).
        upsert_subscription(
            &conn,
            3,
            1,
            &ValidatedEndpoint::for_testing("https://mock.example.com/push/stale"),
            &valid_p256dh(),
            &valid_auth(),
        );
        // Sub on device 4 → 500 (Failed).
        upsert_subscription(
            &conn,
            4,
            1,
            &ValidatedEndpoint::for_testing("https://mock.example.com/push/2"),
            &valid_p256dh(),
            &valid_auth(),
        );
        // Sub on device 5 → loopback endpoint, rejected by validate_endpoint
        // at delivery time (InvalidEndpoint). Uses for_testing to bypass
        // insertion-time SSRF check.
        upsert_subscription(
            &conn,
            5,
            1,
            &ValidatedEndpoint::for_testing("https://127.0.0.1/push"),
            &valid_p256dh(),
            &valid_auth(),
        );
    }

    let mut responses = StdHashMap::new();
    responses.insert(
        "https://mock.example.com/push/0".to_string(),
        MockResponse::Ok {
            status: 201,
            delay: Duration::ZERO,
        },
    );
    responses.insert(
        "https://mock.example.com/push/1".to_string(),
        MockResponse::Ok {
            status: 410,
            delay: Duration::ZERO,
        },
    );
    responses.insert(
        "https://mock.example.com/push/2".to_string(),
        MockResponse::Ok {
            status: 500,
            delay: Duration::ZERO,
        },
    );
    // Loopback endpoint is rejected before MockHttpPoster is called, so no
    // entry needed for "https://127.0.0.1/push".

    let poster = MockHttpPoster::new(responses);
    let svc = make_service_with_poster(db.clone(), poster);

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
            delivered,
            gone,
            failed,
            failed_stale_user,
            failed_invalid_endpoint,
            ..
        } => {
            assert_eq!(delivered, 1, "one Delivered (device 1)");
            assert_eq!(gone, 1, "one Gone (device 2, 410)");
            assert_eq!(failed, 1, "one Failed (device 4, 500)");
            assert_eq!(
                failed_stale_user, 1,
                "one stale-user sub excluded pre-fanout (device 3)"
            );
            assert_eq!(
                failed_invalid_endpoint, 1,
                "one InvalidEndpoint (device 5, loopback)"
            );
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    // Verify Gone row (device 2) was actually deleted from the DB by the batch
    // write — not just counted. A regression that counted Gone but skipped
    // delete_subscription_by_id would pass counter assertions but fail here.
    let gone_row_exists = {
        let conn = db.lock().await;
        crate::pwa_push::db::subscription_exists(&conn, 2, 1)
    };
    assert!(
        !gone_row_exists,
        "Gone subscription row (device 2) must be deleted by batch write"
    );
}

/// scope-2 coverage — the `publish_wide_cap_fired` warn event must be emitted
/// with `aborted_count == 1` when the outer cap fires.
///
/// Uses a per-test scoped tracing capture so concurrent cap-firing tests
/// cannot cross-contaminate each other's assertions.
#[tokio::test]
async fn publish_wide_cap_log_event_emitted() {
    // Override cap to 500ms; guard resets it to u64::MAX on drop.
    let _cap = CapOverride::set(500);

    // INVARIANT: must run under #[tokio::test] (current_thread). set_default installs a
    // thread-local dispatcher; JoinSet tasks only fire on the test thread under current_thread.
    // Switching to multi_thread would silently empty the sinks.
    let cap = install_scoped_cap_capture();

    let db = make_db_with_users().await;
    let eps = seed_subscriptions(&db, 2).await;

    let mut responses = StdHashMap::new();
    responses.insert(
        eps[0].clone(),
        MockResponse::Ok {
            status: 201,
            delay: Duration::from_millis(50),
        },
    );
    responses.insert(eps[1].clone(), MockResponse::Hang);

    let poster = MockHttpPoster::new(responses);
    let svc = make_service_with_poster(db, poster);

    Arc::clone(&svc)
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

    let new_events = cap.cap_sink.lock().unwrap().clone();
    assert!(
        !new_events.is_empty(),
        "publish_wide_cap_fired must be emitted at least once"
    );
    for v in &new_events {
        assert_eq!(*v, 1, "each aborted_count must be 1; got {new_events:?}");
    }
}

/// scope-1 coverage — `reqwest::Error` from `execute()` must map to
/// `DeliveryOutcome::Failed`, not panic or be swallowed.
#[tokio::test(flavor = "multi_thread")]
async fn network_error_counts_as_failed() {
    let db = make_db_with_users().await;
    let eps = seed_subscriptions(&db, 2).await;

    let mut responses = StdHashMap::new();
    responses.insert(
        eps[0].clone(),
        MockResponse::NetworkError {
            delay: Duration::ZERO,
        },
    );
    responses.insert(
        eps[1].clone(),
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
            delivered, failed, ..
        } => {
            assert_eq!(delivered, 1, "non-erroring sub must be delivered");
            assert_eq!(failed, 1, "network-error sub must count as failed");
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

/// §4.5 — A task panic must count as `failed`, not propagate out of `send`.
/// Also verifies that the panic warn-log event is emitted with the panicking
/// subscription's `subscription_id` and `panic_payload == "mock task panic"`.
#[tokio::test]
async fn task_panic_counts_as_failed_not_propagated() {
    let db = make_db_with_users().await;
    let eps = seed_subscriptions(&db, 2).await;

    let mut responses = StdHashMap::new();
    responses.insert(eps[0].clone(), MockResponse::Panic);
    responses.insert(
        eps[1].clone(),
        MockResponse::Ok {
            status: 201,
            delay: Duration::ZERO,
        },
    );

    // Look up the sub_id of the panicking endpoint before send().
    let panicking_sub_id: i64 = {
        let conn = db.lock().await;
        conn.query_row(
            "SELECT id FROM pwa_push_subscriptions WHERE endpoint = ?1",
            rusqlite::params![&eps[0]],
            |row| row.get(0),
        )
        .expect("panicking sub must exist in DB")
    };

    // Per-test scoped capture; sinks start empty.
    // INVARIANT: must run under #[tokio::test] (current_thread). set_default installs a
    // thread-local dispatcher; JoinSet tasks only fire on the test thread under current_thread.
    // Switching to multi_thread would silently empty the sinks.
    let cap = install_scoped_cap_capture();

    let poster = MockHttpPoster::new(responses);
    let svc = make_service_with_poster(db, poster);

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
            delivered, failed, ..
        } => {
            assert_eq!(delivered, 1, "non-panicking sub delivered");
            assert_eq!(failed, 1, "panicking sub counted as failed");
        }
        other => panic!("expected Ok, got {other:?}"),
    }

    // Assert the panic warn-log event was captured with the correct fields.
    // Design §4.5 requires: subscription_id matches the panicking sub and
    // panic_payload == "mock task panic".
    let new_panic_events = cap.panic_sink.lock().unwrap().clone();
    assert!(
        !new_panic_events.is_empty(),
        "panic warn-log event must be emitted"
    );
    assert!(
        new_panic_events
            .iter()
            .any(|(sid, payload)| *sid == panicking_sub_id && payload == "mock task panic"),
        "panic warn must carry correct subscription_id={panicking_sub_id} \
         and panic_payload=\"mock task panic\"; got {new_panic_events:?}"
    );

    // Panic path must not trigger the publish-wide-cap event.
    assert_eq!(
        cap.cap_sink.lock().unwrap().len(),
        0,
        "task panic must not emit publish_wide_cap_fired event"
    );
}

/// §4.6 — Zero subscriptions: send must return Ok with all counters zero,
/// not hang or panic.
#[tokio::test]
async fn zero_subscriptions_no_lock_or_spawn() {
    let db = make_db_with_users().await;
    // No subscriptions seeded.
    let poster = MockHttpPoster::new(StdHashMap::new());
    let svc = make_service_with_poster(db, poster);

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
            assert_eq!(failed_stale_user, 0);
            assert_eq!(failed_invalid_endpoint, 0);
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

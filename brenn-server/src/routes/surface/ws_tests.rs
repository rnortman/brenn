//! Native WS integration tests for `GET /surface/{slug}/ws`: pre-upgrade
//! checks, the build-ID handshake, the `Welcome`-first contract, transport-plane
//! liveness (idle heartbeat + silent-client reap), the fail-closed binary/data
//! frame rejection, inbound-frame parse-failure and oversized-frame
//! classification, the lenient `Log` frame (size-cap, rate-limit,
//! log-only), `Subscribe`/delivery,
//! `Unsubscribe` (removal + not-active violation), and `Publish` (port
//! resolution, durable/oversize outcomes, rate limiting).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::session::CONFIRM_SET_SOFT_CAP;
use crate::test_support::TEST_BUILD_ID;
use axum::http::StatusCode;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::access::{AppCapability, AppPolicy};
use brenn_lib::db;
use brenn_lib::messaging::config::{
    ChannelConfigRaw, Depth, NoiseLevel, ResolvedChannel, ResolvedComponent, ResolvedSubscription,
    ResolvedSurface, ResolvedSurfaceSubscription, Sink, SurfaceBinding, SurfaceOutput,
    SurfacePrincipalBudgets, SurfaceSendBudget, build_channel_entries,
};
use brenn_lib::messaging::db::{
    PendingPushInsert, insert_message_with_pushes, load_all_dispatchable_pushes,
    release_due_pushes, upsert_channels, utc_to_ns,
};
use brenn_lib::messaging::testutils::ephemeral_channel_entry;
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, EphemeralBus, EphemeralPublishResult, MessagingDirectory,
    MessagingGlobalConfig, Messenger, ParticipantId, PublishResult, SubscriberEntry,
    SubscriberEntryKind, Urgency, WakeMin, WakeRouter, dispatcher,
};
use brenn_lib::obs::alerting::{
    AlertDispatcher, AlertSeverity as NativeAlertSeverity, make_capturing_alerter_with_severity,
};
use brenn_surface_contract::{ERROR_REPORT_INSTANCE, ERROR_REPORT_PORT};
use brenn_surface_proto::{
    AlertSeverity, BatchEntry, ClientFrame, Cursor, DeliverTarget, GapInfo, GapReason,
    InstanceReport, InstanceState, LogLevel, MAX_ALERT_BODY_BYTES, MAX_ALERT_TITLE_BYTES,
    PublishBatchOutcome, PublishOutcome, ServerFrame, StatusCounters, SubscribeOutcome,
    max_client_frame_bytes,
};

use super::cursor::{self, CursorState};
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use super::registry::{SessionCaps, SurfaceSessionHandle};
use super::session::ALERT_BURST;
use super::test_fixtures::{
    COMPONENT, EPH_ADDR, EPH_NAME, PORT, SurfaceTestHarness, TEST_MAX_BODY_BYTES, TEST_ORIGIN,
    assert_no_alerts, deskbar_context_feed, deskbar_sub, durable_resume,
    durable_resume_with_confirm, fixture_bus, publish, publish_as, subscribe_harness,
    surface_harness,
};

/// Read the `confirm_pending` flag on a pending-push row.
async fn confirm_pending_flag(db: &db::Db, push_id: i64) -> i64 {
    let conn = db.lock().await;
    conn.query_row(
        "SELECT confirm_pending FROM messaging_pending_pushes WHERE id = ?1",
        rusqlite::params![push_id],
        |r| r.get(0),
    )
    .expect("push row exists")
}
use super::{MAX_SESSIONS_PER_SURFACE, MAX_SESSIONS_PER_USER_PER_SURFACE, build_surface_runtimes};
use crate::active_bridge::ActiveBridges;
use crate::messaging_router::WakeRouterImpl;
use crate::state::AppState;
use crate::test_support::http::{
    TEST_USERNAME, assert_stale_client_close_and_no_alert, http_to_ws_url,
    setup_authenticated_user, spawn_test_server, surface_ws_open, ws_connect_first_frame,
    ws_upgrade_status,
};
use crate::test_support::state::test_state_with_capturing_alerter;
use crate::test_support::surface::SurfaceFixture;

type SurfaceWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The one target of a `Deliver` these tests expect. Each drives a single
/// subscription, so more than one target would mean the coalescer folded in a
/// subscription the test never opened.
fn sole_target(targets: &[DeliverTarget]) -> &DeliverTarget {
    let [target] = targets else {
        panic!("expected a single-target Deliver, got {targets:?}");
    };
    target
}

/// A `deskbar` surface with one ephemeral subscription binding and the given
/// access list (empty ⇒ any authenticated user).
fn deskbar(allowed_users: Vec<String>) -> ResolvedSurface {
    SurfaceFixture::new("deskbar", COMPONENT)
        .subscribe(EPH_ADDR, COMPONENT, PORT)
        .allowed_users(allowed_users)
        .build()
}

/// Capturing-alerter test state with the given surface installed over a bus with
/// no channels. Heartbeat is 1 s (via `AppState::for_test`) so liveness tests run
/// fast.
fn surface_state(db: &db::Db, resolved: ResolvedSurface) -> SurfaceTestHarness {
    surface_harness(db, resolved, vec![])
}

/// A `deskbar` surface granted the alert plane (`SurfaceAlert`), so an `Alert`
/// frame reaches `handle_alert`'s dispatch arm and `Welcome.alert_granted` is
/// advertised.
fn deskbar_alert_granted() -> ResolvedSurface {
    let mut resolved = deskbar(vec![]);
    resolved.policy.grants.insert(AppCapability::SurfaceAlert);
    resolved
}

/// Test state whose alert dispatcher captures `(severity, title, body)` triples,
/// with the given surface installed. Used by the alert-dispatch integration
/// test, which asserts the native severity mapping in addition to the title
/// prefix and attribution.
#[allow(clippy::type_complexity)]
fn surface_state_severity(
    db: &db::Db,
    resolved: ResolvedSurface,
) -> (
    AppState,
    Arc<Mutex<Vec<(NativeAlertSeverity, String, String)>>>,
    tokio::task::JoinHandle<()>,
) {
    let (alert_dispatcher, captured, handle) = make_capturing_alerter_with_severity();
    let mut state = crate::test_support::state::test_state(db);
    state.alert_dispatcher = alert_dispatcher;
    // Barrier channel on the bus so `drain_barrier` (an ephemeral no-op publish)
    // resolves for any severity test that uses it.
    let bus = fixture_bus(vec![ephemeral_channel_entry(BARRIER_EPH_NAME, 0, 16)]);
    state.surfaces = Arc::new(build_surface_runtimes(
        vec![resolved],
        bus,
        None,
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    (state, captured, handle)
}

/// Production caps, for prefill calls that must reproduce a state the live
/// handler actually permits.
const PROD_CAPS: SessionCaps = SessionCaps {
    per_surface: MAX_SESSIONS_PER_SURFACE,
    per_user: MAX_SESSIONS_PER_USER_PER_SURFACE,
};

/// Poll `container` until it holds at least `want` elements (or ~2 s), then
/// return a clone. The count is bounded by what the test sent, so a caller that
/// sent exactly `want` can then assert the length with no drainer race.
async fn wait_for_len<T: Clone>(container: &Arc<Mutex<Vec<T>>>, want: usize) -> Vec<T> {
    for _ in 0..200 {
        if container.lock().unwrap().len() >= want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    container.lock().unwrap().clone()
}

/// The client-observable shape of a connection close — the surface an existence
/// oracle could leak. A polite WS `Close` frame carries a code and a reason
/// string (both readable by a browser via `CloseEvent.code`/`.reason`); an
/// abrupt end (clean EOF or TCP reset, which a client cannot tell apart and
/// which is collapsed here so the comparison is not timing-flaky) carries none.
/// The reason is captured, not just the code, so a same-code/different-reason
/// close — e.g. a future polite-close ceremony leaking the sanitized channel
/// address into the reason — still diverges between two probe inputs.
#[derive(Debug, PartialEq, Eq)]
enum CloseObservation {
    CloseFrame(Option<(u16, String)>),
    Abrupt,
}

/// Read frames until the server closes the connection, returning the observed
/// close shape (or `None` on a 5 s timeout) so callers can assert two inputs
/// close identically.
///
/// Used only by violation paths, so it also pins the "no response frame" half of
/// the violation contract: transport keep-alive (`Ping`/`Pong`) and an
/// idle `Heartbeat` are allowed through, but any other `ServerFrame` reaching the
/// client before the close means a handler leaked a response to the offending
/// frame — a hard test failure.
async fn drain_until_closed_observing(ws: &mut SurfaceWs) -> Option<CloseObservation> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Close(frame)))) => {
                return Some(CloseObservation::CloseFrame(
                    frame.map(|f| (u16::from(f.code), f.reason.to_string())),
                ));
            }
            Ok(None) | Ok(Some(Err(_))) => return Some(CloseObservation::Abrupt),
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)))) => continue,
            Ok(Some(Ok(Message::Text(t)))) => {
                let frame: ServerFrame =
                    serde_json::from_str(t.as_str()).expect("server frame parses");
                assert!(
                    matches!(frame, ServerFrame::Heartbeat),
                    "violation path leaked a response frame before close: {frame:?}"
                );
            }
            Ok(Some(Ok(Message::Binary(_)))) => {
                panic!("violation path sent a binary frame before close")
            }
            Err(_) => return None,
        }
    }
}

/// `drain_until_closed_observing` reduced to "did the connection close within the
/// deadline", for callers that do not compare close shapes.
async fn drain_until_closed(ws: &mut SurfaceWs) -> bool {
    drain_until_closed_observing(ws).await.is_some()
}

// ---------------------------------------------------------------------------
// Bus-plane (Subscribe / delivery) fixtures
// ---------------------------------------------------------------------------

/// A throwaway ephemeral channel used as a processing-drain barrier: a surface
/// publishes an empty body to a bound output on it and awaits the `Ok` reply.
/// No test subscribes to or reads this channel, so the barrier publish is
/// side-effect-free w.r.t. any assertion. (A durable-output publish answered
/// `Unsupported` was the barrier before durable surface publish existed; now
/// durable publish actually persists, so the barrier moved to this ephemeral
/// no-op channel.)
const BARRIER_EPH_NAME: &str = "drain-barrier";
const BARRIER_EPH_ADDR: &str = "ephemeral:drain-barrier";

/// Bare name of a channel bound only to the `otherbar` surface — present in
/// committed config *and* on the fixture bus, but never in `deskbar`'s
/// subscription map. The "exists but not yours" probe for the no-oracle test.
const OTHERBAR_NAME: &str = "otherbar-only";
/// Its scheme-qualified address.
const OTHERBAR_ADDR: &str = "ephemeral:otherbar-only";

/// A `Subscribe` for `COMPONENT`'s binding on `channel` — every subscription
/// binding in these fixtures belongs to that instance. Tests exercising the
/// grain itself (a sibling's binding, the kernel grain, an undeclared instance)
/// use [`subscribe_frame_as`].
fn subscribe_frame(channel: &str, resume: Option<Cursor>) -> Message {
    subscribe_frame_as(channel, COMPONENT, resume)
}

/// A `Subscribe` naming an explicit principal.
fn subscribe_frame_as(channel: &str, instance: &str, resume: Option<Cursor>) -> Message {
    let frame = ClientFrame::Subscribe {
        channel: channel.to_string(),
        instance: instance.to_owned(),
        resume,
    };
    Message::Text(serde_json::to_string(&frame).expect("serialize").into())
}

/// Consume the leading `Welcome` frame, asserting it is a text frame.
async fn consume_welcome(ws: &mut SurfaceWs) {
    let first = ws.next().await.expect("a frame").expect("frame ok");
    assert!(matches!(first, Message::Text(_)), "first frame is Welcome");
}

/// Assert exactly one security alert was captured and its combined
/// source+detail text contains `needle`. The caller must already have observed a
/// happens-before edge (an observed close or response) proving the triggering
/// action finished; `flush` then makes the dispatched alert visible without
/// racing the drainer, so the exact-one count cannot lose to a second in-flight
/// alert.
async fn assert_single_alert(
    flusher: &AlertDispatcher,
    alerts: &Arc<Mutex<Vec<(String, String)>>>,
    needle: &str,
) {
    flusher.flush().await;
    let captured = alerts.lock().unwrap().clone();
    assert_eq!(captured.len(), 1, "expected one alert, got {captured:?}");
    let combined = format!("{} {}", captured[0].0, captured[0].1);
    assert!(
        combined.contains(needle),
        "expected alert containing {needle:?}, got {combined}"
    );
}

/// Read the next server frame, skipping pings and idle `Heartbeat`s. Panics on
/// close or timeout.
async fn next_server_frame(ws: &mut SurfaceWs) -> ServerFrame {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let frame: ServerFrame =
                    serde_json::from_str(t.as_str()).expect("server frame parses");
                if matches!(frame, ServerFrame::Heartbeat) {
                    continue;
                }
                return frame;
            }
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            other => panic!("expected a server frame, got {other:?}"),
        }
    }
}

/// Assert the given `Deliver` frame carries the expected channel, body, seq, and
/// drop count, with an ephemeral position on the given epoch.
fn assert_deliver(
    frame: ServerFrame,
    channel: &str,
    body: &str,
    seq: u64,
    dropped: u64,
    epoch: Uuid,
) {
    match frame {
        ServerFrame::Deliver {
            channel: got_channel,
            envelope,
            targets,
        } => {
            assert_eq!(got_channel, channel);
            assert_eq!(envelope.body, body);
            let target = sole_target(&targets);
            assert_eq!(
                target.instance, COMPONENT,
                "every fixture binding is COMPONENT's, so its deliveries name it"
            );
            assert_eq!(target.dropped, dropped);
            // The ephemeral ring position lives in the opaque cursor; parse it to
            // recover the (bus epoch, ring seq) the delivery carries.
            match cursor::parse(&target.cursor) {
                Ok(CursorState::Ephemeral {
                    epoch: got_epoch,
                    seq: got_seq,
                }) => {
                    assert_eq!(got_seq, seq);
                    assert_eq!(got_epoch, epoch);
                }
                other => panic!("expected ephemeral cursor, got {other:?}"),
            }
        }
        other => panic!("expected Deliver, got {other:?}"),
    }
}

/// Open a `deskbar` session, consume `Welcome`, send `msg`, and drain until the
/// server closes — returning the observed close shape. The shared wire-driving
/// primitive behind both the single-violation assertion and the no-oracle
/// close-shape comparison. Panics (via the "no response frame" contract inside
/// the drainer) if the connection does not close cleanly.
async fn send_frame_observe_close(base: &str, token: &str, msg: Message) -> CloseObservation {
    let ws_url = http_to_ws_url(base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, token).await;
    consume_welcome(&mut ws).await;

    ws.send(msg).await.expect("send frame");
    drain_until_closed_observing(&mut ws)
        .await
        .expect("connection must close after a protocol violation")
}

/// Open a session, consume `Welcome`, send `msg`, and assert the server tore the
/// connection down with exactly one `SurfaceProtocolViolation`.
async fn assert_frame_is_violation(
    base: &str,
    token: &str,
    msg: Message,
    flusher: &AlertDispatcher,
    alerts: &Arc<Mutex<Vec<(String, String)>>>,
) {
    send_frame_observe_close(base, token, msg).await;
    assert_single_alert(flusher, alerts, "surface_protocol_violation").await;
}

// ---------------------------------------------------------------------------
// Pre-upgrade checks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_unknown_slug_returns_404() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let status = ws_upgrade_status(&format!("{base}/surface/nonexistent/ws"), Some(&token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    assert_single_alert(&flusher, &alerts, "unrecognized_url").await;
}

#[tokio::test]
async fn surface_ws_access_denied_returns_403() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar(vec!["otheruser".to_string()]));
    let (token, _) = setup_authenticated_user(&db).await; // testuser
    let (base, _sd) = spawn_test_server(state).await;

    let status = ws_upgrade_status(&format!("{base}/surface/deskbar/ws"), Some(&token)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    assert_single_alert(&flusher, &alerts, "auth_failure").await;
}

#[tokio::test]
async fn surface_ws_session_cap_returns_503_no_alert() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar(vec![]));
    // Pre-fill the shared registry to capacity; guards keep the slots occupied.
    // Distinct usernames so the per-user cap does not trip first — this must
    // reproduce a surface-full state the production caps actually permit.
    let registry = state.surface_registry.clone();
    let mut guards = Vec::new();
    for i in 0..MAX_SESSIONS_PER_SURFACE {
        guards.push(
            registry
                .try_register(
                    "deskbar",
                    SurfaceSessionHandle::for_test(&format!("filler-{i}")),
                    PROD_CAPS,
                )
                .expect("prefill under cap"),
        );
    }

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let status = ws_upgrade_status(
        &format!("{base}/surface/deskbar/ws?build={TEST_BUILD_ID}"),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    // Capacity rejection is not a security event.
    assert_no_alerts(&flusher, &alerts, "session-cap 503 must not fire an alert").await;
    drop(guards);
}

#[tokio::test]
async fn surface_ws_per_user_cap_returns_503_no_alert() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar(vec![]));
    // Fill only the authenticated user's per-user allotment (the surface is far
    // below its shared cap); the next attach by that user must trip the per-user
    // cap, not the shared one.
    let registry = state.surface_registry.clone();
    let mut guards = Vec::new();
    for _ in 0..MAX_SESSIONS_PER_USER_PER_SURFACE {
        guards.push(
            registry
                .try_register(
                    "deskbar",
                    SurfaceSessionHandle::for_test(TEST_USERNAME),
                    PROD_CAPS,
                )
                .expect("prefill under per-user cap"),
        );
    }

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let status = ws_upgrade_status(
        &format!("{base}/surface/deskbar/ws?build={TEST_BUILD_ID}"),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    // Per-user cap 503 must not fire an alert, same as the shared-cap trip.
    assert_no_alerts(&flusher, &alerts, "per-user cap 503 must not fire an alert").await;
    drop(guards);
}

// ---------------------------------------------------------------------------
// Build-ID handshake + Welcome
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_missing_build_closes_stale_no_alert() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, alerts, .. } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, "/surface/deskbar/ws");
    let msg = ws_connect_first_frame(&ws_url, &token).await;
    assert_stale_client_close_and_no_alert(msg, &alerts, "surface missing build").await;
}

#[tokio::test]
async fn surface_ws_matching_build_welcome_is_first_frame() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, .. } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let msg = ws_connect_first_frame(&ws_url, &token).await;
    let text = match msg {
        Message::Text(t) => t,
        other => panic!("expected Welcome text frame, got {other:?}"),
    };
    let frame: ServerFrame = serde_json::from_str(text.as_str()).expect("Welcome parses");
    match frame {
        ServerFrame::Welcome {
            surface,
            participant_id,
            heartbeat_secs,
            max_body_bytes,
            alert_granted,
            bindings,
            ..
        } => {
            assert_eq!(surface, "deskbar");
            assert_eq!(participant_id, "surface:deskbar");
            assert_eq!(heartbeat_secs, 1);
            assert_eq!(max_body_bytes, TEST_MAX_BODY_BYTES as u64);
            // Default-policy surface: the alert plane is deny-by-default.
            assert!(!alert_granted);
            assert_eq!(bindings.components.len(), 1);
            assert_eq!(bindings.components[0].instance, "protobar");
            assert_eq!(bindings.components[0].kind, "protobar");
            assert_eq!(bindings.subscriptions.len(), 1);
            assert_eq!(bindings.subscriptions[0].channel, "ephemeral:protobar-demo");
            assert_eq!(bindings.subscriptions[0].port, "messages");
        }
        other => panic!("expected Welcome, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Transport-plane behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_binary_frame_is_violation_and_kills() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // A binary frame is a protocol violation → security event + kill.
    assert_frame_is_violation(
        &base,
        &token,
        Message::Binary(vec![1, 2, 3].into()),
        &flusher,
        &alerts,
    )
    .await;
}

/// Send `payload` as a text frame and assert it is a protocol violation.
async fn assert_text_frame_is_violation(
    base: &str,
    token: &str,
    payload: &str,
    flusher: &AlertDispatcher,
    alerts: &Arc<Mutex<Vec<(String, String)>>>,
) {
    assert_frame_is_violation(
        base,
        token,
        Message::Text(payload.to_string().into()),
        flusher,
        alerts,
    )
    .await;
}

#[tokio::test]
async fn surface_ws_malformed_json_is_violation_and_kills() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    assert_text_frame_is_violation(&base, &token, "{ not valid json", &flusher, &alerts).await;
}

#[tokio::test]
async fn surface_ws_unknown_type_is_violation_and_kills() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    assert_text_frame_is_violation(&base, &token, r#"{"type":"Bogus"}"#, &flusher, &alerts).await;
}

#[tokio::test]
async fn surface_ws_oversized_frame_is_violation_and_kills() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // A frame past the derived read cap trips the server's `max_message_size`,
    // surfacing as a tungstenite `Capacity(MessageTooLong)` read error the
    // session loop downcasts and classifies as a protocol violation. No
    // config-legal frame can reach this size, so it is tampering or a bug.
    let over_cap = "a".repeat(max_client_frame_bytes(TEST_MAX_BODY_BYTES) + 1);
    assert_text_frame_is_violation(&base, &token, &over_cap, &flusher, &alerts).await;
}

/// Wait up to `secs` for a `Heartbeat` frame, skipping non-heartbeat traffic.
/// A heartbeat proves the connection is still live (server-side idle emission);
/// `false` means it closed or fell silent within the window.
async fn saw_heartbeat_within(ws: &mut SurfaceWs, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if let Ok(ServerFrame::Heartbeat) = serde_json::from_str::<ServerFrame>(t.as_str())
                {
                    return true;
                }
            }
            Ok(Some(Ok(_))) => continue,
            _ => return false,
        }
    }
    false
}

fn alert_frame(severity: AlertSeverity, title: &str, body: &str) -> Message {
    let frame = ClientFrame::Alert {
        severity,
        title: title.to_string(),
        body: body.to_string(),
    };
    Message::Text(serde_json::to_string(&frame).expect("serialize").into())
}

/// `resolved` and grant it `EphemeralPublish` + a covering `ephemeral_publish`
/// matcher, so a barrier `Publish` resolves to a bound output and passes the bus
/// ACL. The harness bus must also carry `BARRIER_EPH_NAME` (via
/// `ephemeral_channel_entry`).
fn push_barrier_binding(resolved: &mut ResolvedSurface) {
    resolved.outputs.push(SurfaceOutput {
        channel_address: BARRIER_EPH_ADDR.to_string(),
        instance: "protobar".to_string(),
        port: "barrier".to_string(),
        default_urgency: Urgency::Normal,
        budget: brenn_budget::SinkBudget {
            fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
        },
    });
    resolved
        .policy
        .grants
        .insert(AppCapability::EphemeralPublish);
    resolved
        .policy
        .acls
        .ephemeral_publish
        .push(ChannelMatcher::Exact(BARRIER_EPH_NAME.to_string()));
}

/// Send a `Publish` to the bound barrier output and consume its `Ok` reply. The
/// barrier publishes an empty body onto a throwaway ephemeral channel with no
/// subscriber, so it has no side effect on any asserted channel; each inbound
/// frame is fully awaited (including any durable publish and its DB commit)
/// before the next is read, so receiving this reply proves every prior frame on
/// the connection has finished processing — a deterministic drain barrier with no
/// sleep-then-count race.
///
/// This is a real publish and consumes one per-connection publish token per
/// call. Callers combining it with a tight `publish_burst` must budget for that
/// or the barrier itself will `RateLimited`.
async fn drain_barrier(ws: &mut SurfaceWs) {
    ws.send(publish_frame("protobar", "barrier", "", None))
        .await
        .expect("send Publish barrier");
    match next_server_frame(ws).await {
        ServerFrame::PublishResult { outcome, .. } => assert!(
            matches!(outcome, PublishOutcome::Ok),
            "drain barrier Publish must answer Ok, got {outcome:?}"
        ),
        other => panic!("expected PublishResult barrier, got {other:?}"),
    }
}

/// Read every `(sender, body)` on `channel_uuid`, ordered by insertion. Call
/// after `drain_barrier` so every prior durable publish is committed: the read
/// is a deterministic snapshot of the final channel state, not a poll.
async fn read_channel_messages(db: &db::Db, channel_uuid: Uuid) -> Vec<(String, String)> {
    let uuid_bytes = channel_uuid.as_bytes().to_vec();
    let conn = db.lock().await;
    let mut stmt = conn
        .prepare(
            "SELECT sender, body FROM messaging_messages \
             WHERE channel_uuid = ?1 ORDER BY id",
        )
        .unwrap();
    let rows = stmt
        .query_map(rusqlite::params![uuid_bytes], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })
        .unwrap()
        .map(Result::unwrap);
    rows.collect()
}

// ---------------------------------------------------------------------------
// Alert plane: an `Alert` frame on an alert-granted surface dispatches to the
// process `AlertDispatcher` with the `Surface <slug>: ` provenance prefix,
// server-attested attribution appended to the body, and severity mapped 1:1.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_alert_on_granted_surface_dispatches_with_prefix_severity_and_attribution() {
    let db = db::init_db_memory();
    let (state, alerts, _h) = surface_state_severity(&db, deskbar_alert_granted());
    let dispatcher = state.alert_dispatcher.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;

    // The granted surface advertises the alert plane at attach time.
    match next_server_frame(&mut ws).await {
        ServerFrame::Welcome { alert_granted, .. } => {
            assert!(
                alert_granted,
                "granted surface's Welcome advertises the plane"
            );
        }
        other => panic!("expected Welcome, got {other:?}"),
    }

    // One frame per severity; the title names the wire severity so each captured
    // dispatch can be correlated to its native mapping order-independently. Three
    // is under ALERT_BURST, so none is bucket-suppressed.
    for (severity, name) in [
        (AlertSeverity::Info, "info"),
        (AlertSeverity::Warning, "warning"),
        (AlertSeverity::Critical, "critical"),
    ] {
        ws.send(alert_frame(
            severity,
            &format!("panic {name}"),
            "the detail",
        ))
        .await
        .expect("send Alert");
    }

    // wait_for_len bounds the wall-clock wait; the flush then makes every
    // dispatched alert visible so the exact-three count cannot race the drainer.
    wait_for_len(&alerts, 3).await;
    dispatcher.flush().await;
    let captured = alerts.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        3,
        "exactly the three sent alerts dispatch, got {captured:?}"
    );

    for (severity, title, body) in captured {
        assert!(
            title.starts_with("Surface deskbar: panic "),
            "title carries the host provenance prefix, got {title:?}"
        );
        assert!(
            body.starts_with("the detail"),
            "body leads with the sanitized client body, got {body:?}"
        );
        assert!(
            body.contains("surface=deskbar user=testuser session="),
            "body carries server-attested attribution, got {body:?}"
        );
        // Severity maps 1:1: the title's trailing wire-severity word must match
        // the captured native severity.
        let wire = title.rsplit(' ').next().expect("title has a severity word");
        match (wire, severity) {
            ("info", NativeAlertSeverity::Info)
            | ("warning", NativeAlertSeverity::Warning)
            | ("critical", NativeAlertSeverity::Critical) => {}
            other => panic!("severity did not map 1:1: {other:?}"),
        }
    }
}

#[tokio::test]
async fn surface_ws_alert_bucket_drops_beyond_burst_and_keeps_session_alive() {
    let db = db::init_db_memory();
    // Granted surface + a barrier output binding so the drain barrier Publish
    // resolves to a bound output (answered Ok) instead of an unbound-port kill.
    let mut resolved = deskbar_alert_granted();
    push_barrier_binding(&mut resolved);
    let (state, alerts, _h) = surface_state_severity(&db, resolved);
    // A surviving dispatcher clone for the flush barrier: the spawned server owns
    // the other clone, so dropping-all-clones-then-await is impossible here.
    let dispatcher = state.alert_dispatcher.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // The per-connection alert bucket starts full (burst ALERT_BURST). Send
    // exactly ALERT_BURST admitted alerts, then one more the bucket must deny —
    // the beyond-burst alert is dropped before dispatch, never a kill.
    for i in 0..ALERT_BURST {
        ws.send(alert_frame(
            AlertSeverity::Warning,
            &format!("admitted {i}"),
            "the detail",
        ))
        .await
        .expect("send admitted Alert");
    }
    ws.send(alert_frame(
        AlertSeverity::Warning,
        "beyond burst",
        "the detail",
    ))
    .await
    .expect("send beyond-burst Alert");

    // The barrier proves every prior frame — including the beyond-burst one — has
    // finished processing before the channel is read, and its SubscribeResult
    // (rather than a close) proves the noisy session was not killed.
    drain_barrier(&mut ws).await;

    // Flush the capturing dispatcher's FIFO drainer: every alert the session
    // enqueued — the ALERT_BURST admitted ones and any wrongly-dispatched
    // beyond-burst sixth — was sent before the barrier reply, so once flush
    // returns the captured vec is complete and the count cannot race the drainer.
    dispatcher.flush().await;
    let captured = alerts.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        ALERT_BURST as usize,
        "beyond-burst alert must be dropped, not dispatched; captured {captured:?}"
    );
    assert!(
        captured
            .iter()
            .all(|(_, title, _)| !title.contains("beyond burst")),
        "the dropped alert must not reach the dispatcher, got {captured:?}"
    );

    // A second barrier confirms the session is still processing frames after the
    // drop — a killed session would have closed, not answered.
    drain_barrier(&mut ws).await;
}

#[tokio::test]
async fn surface_ws_alert_on_ungranted_surface_is_violation_and_kills() {
    let db = db::init_db_memory();
    // Default-policy `deskbar` carries no `SurfaceAlert` grant, so the alert
    // plane does not exist for it — the same surface_state capturing alerter
    // records the resulting security event.
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // An `Alert` from a surface without the grant is a protocol violation:
    // the session is killed and a security event fires (fail2ban signal). A
    // conforming shell never sends one — it reads `Welcome.alert_granted`, which
    // is `false` here (covered by the granted counterpart above and the Welcome
    // population test) — so only a non-conforming client reaches this path.
    assert_frame_is_violation(
        &base,
        &token,
        alert_frame(AlertSeverity::Warning, "forged page", "the detail"),
        &flusher,
        &alerts,
    )
    .await;
}

#[tokio::test]
async fn surface_ws_oversized_alert_on_granted_surface_is_violation_and_kills() {
    let db = db::init_db_memory();
    // The surface *is* granted the alert plane, so the grant check passes and the
    // size-cap check is the gate that fires — the opposite outcome from the
    // bucket-drop leg: an oversized field is not throttled, it kills. The default
    // `surface_state` two-tuple capturing alerter records the security event.
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar_alert_granted());
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // An over-cap title is a protocol violation on the granted plane: session
    // killed + one security event. A conforming client never sends one — it
    // truncates to the proto caps before send (client core).
    let huge_title = "x".repeat(MAX_ALERT_TITLE_BYTES + 1);
    assert_frame_is_violation(
        &base,
        &token,
        alert_frame(AlertSeverity::Warning, &huge_title, "detail"),
        &flusher,
        &alerts,
    )
    .await;
}

#[tokio::test]
async fn surface_ws_oversized_alert_body_on_granted_surface_is_violation_and_kills() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = surface_state(&db, deskbar_alert_granted());
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // An over-cap body is the same violation as an over-cap title — both fields
    // are capped on the granted plane.
    let huge_body = "x".repeat(MAX_ALERT_BODY_BYTES + 1);
    assert_frame_is_violation(
        &base,
        &token,
        alert_frame(AlertSeverity::Warning, "page", &huge_body),
        &flusher,
        &alerts,
    )
    .await;
}

#[tokio::test]
async fn surface_ws_idle_client_receives_heartbeat() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, .. } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;

    // The idle connection (heartbeat = 1 s) must yield a Heartbeat frame after
    // Welcome, well within this window.
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut saw_heartbeat = false;
    while Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if let Ok(ServerFrame::Heartbeat) = serde_json::from_str::<ServerFrame>(t.as_str())
                {
                    saw_heartbeat = true;
                    break;
                }
            }
            Ok(Some(Ok(_))) => continue,
            _ => break,
        }
    }
    assert!(
        saw_heartbeat,
        "idle client should receive a Heartbeat frame"
    );
}

#[tokio::test]
async fn surface_ws_silent_client_is_reaped() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, .. } = surface_state(&db, deskbar(vec![]));
    let registry = state.surface_registry.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    // Never poll the stream: tungstenite never auto-pongs the server's pings, so
    // the server sees no inbound liveness and reaps at ~3x heartbeat. Keep the
    // stream alive (the socket stays open) so this tests the server-side reap,
    // not a client disconnect.
    let _ws = surface_ws_open(&ws_url, &token).await;

    let mut count = registry.count("deskbar");
    for _ in 0..160 {
        count = registry.count("deskbar");
        if count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        count, 0,
        "silent client should be reaped and its slot released"
    );
}

#[tokio::test]
async fn surface_ws_stalled_reader_is_torn_down_by_watchdog() {
    let db = db::init_db_memory();
    // Broadcast capacity far above the outbound queue so the flood is retained
    // (not broadcast-dropped) and keeps piling deliveries onto the session task
    // even after the writer stalls; retain_depth 0 keeps it a pure live flood.
    let SurfaceTestHarness { state, bus, .. } = subscribe_harness(&db, 0, 2048);
    let registry = state.surface_registry.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send Subscribe");
    // From here the client never reads again: a stalled reader. Large deliveries
    // fill its socket buffer, so the writer's watchdog-bounded sink.send() pends;
    // the writer stops draining the outbound queue, it fills, and the session
    // loop blocks on `tx.send` backpressure. Blocked there (not idling in
    // select), the inbound reap cannot fire — only the writer's write-progress
    // watchdog (3x heartbeat) frees the slot: it drops the receiver, the blocked
    // `tx.send` errors, and the session tears down.

    // 60 KB bodies (under the 64 KiB cap) fill TCP buffers with few frames; 600
    // frames overfill the 256-deep OUTBOUND_QUEUE_FRAMES plus any autotuned
    // socket buffer, guaranteeing the loop blocks on backpressure. Split across
    // three senders to stay under the per-sender publish burst.
    let big = "x".repeat(60_000);
    publish_as(&bus, "flood-a", EPH_ADDR, EPH_NAME, &big, 200);
    publish_as(&bus, "flood-b", EPH_ADDR, EPH_NAME, &big, 200);
    publish_as(&bus, "flood-c", EPH_ADDR, EPH_NAME, &big, 200);

    // The slot must release within a generous multiple of the watchdog window
    // (3x heartbeat = 3 s here) despite the client never draining.
    let mut count = registry.count("deskbar");
    for _ in 0..300 {
        count = registry.count("deskbar");
        if count == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        count, 0,
        "stalled reader must be torn down by the write watchdog and its slot released"
    );
}

/// Ephemeral subscriptions key at the instance grain too (D-18): two declared
/// instances bound to one `ephemeral:` channel are two subscriptions on the one
/// session, and one publish becomes two `Deliver`s, each under its own name.
///
/// The durable sibling test's ephemeral twin, and the pin for the wire-volume
/// change increment 9 flagged: N instances on one ephemeral channel now cost N
/// copies over the one socket. Folding the ephemeral arm back to channel keying
/// inverts this sharply rather than subtly — the second `Subscribe` becomes a
/// duplicate-subscribe violation that kills the connection.
#[tokio::test]
async fn surface_ws_ephemeral_sibling_instances_each_get_their_own_subscription() {
    let db = db::init_db_memory();
    let mut surface = deskbar_sub();
    surface.components.extend(
        ["agenda-alice", "agenda-bob"].map(|instance| ResolvedComponent {
            instance: instance.to_string(),
            kind: "agenda".to_string(),
            abi: brenn_surface_proto::Abi::Dom,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: false,
        }),
    );
    surface.subscriptions.extend(
        ["agenda-alice", "agenda-bob"].map(|instance| SurfaceBinding {
            channel_address: EPH_ADDR.to_string(),
            instance: instance.to_string(),
            port: PORT.to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: NoiseLevel::Silent,
        }),
    );
    // retain_depth 0 keeps each fresh subscribe replay-free, so every Deliver
    // below comes from the one live publish.
    let SurfaceTestHarness { state, bus, .. } =
        surface_harness(&db, surface, vec![ephemeral_channel_entry(EPH_NAME, 0, 16)]);
    let epoch = bus.epoch();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;
    for instance in ["agenda-alice", "agenda-bob"] {
        ws.send(subscribe_frame_as(EPH_ADDR, instance, None))
            .await
            .expect("send Subscribe");
        assert_eq!(
            next_subscribe_result(&mut ws, EPH_ADDR, instance).await.0,
            0,
            "{instance}'s subscription is answered under its own name rather than \
             killed as a duplicate of its sibling's"
        );
    }

    publish_eph(&bus, EPH_NAME, EPH_ADDR, "hello-both");

    // One publish reaches both principals, each at its own cursor, in one frame:
    // the write boundary coalesces the sibling subscriptions' copies of a live
    // publish. Target order within the frame is unspecified, so collect and sort.
    let mut got: Vec<String> = Vec::new();
    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            channel,
            envelope,
            targets,
        } => {
            assert_eq!(channel, EPH_ADDR);
            assert_eq!(envelope.body, "hello-both");
            assert_eq!(
                targets.len(),
                2,
                "one publish, one connection, two sibling subscriptions → one \
                 frame carrying the envelope once: {targets:?}"
            );
            for target in targets {
                assert!(
                    matches!(cursor::parse(&target.cursor), Ok(CursorState::Ephemeral { epoch: got, .. }) if got == epoch),
                    "an ephemeral delivery carries the bus epoch: {:?}",
                    target.cursor
                );
                assert_eq!(target.seq, 1, "each target's seq comes from its own span");
                got.push(target.instance.clone());
            }
        }
        other => panic!("expected Deliver, got {other:?}"),
    }
    got.sort();
    assert_eq!(
        got,
        vec!["agenda-alice".to_string(), "agenda-bob".to_string()],
        "each instance is delivered under its own name at its own cursor — \
         per-subscription state, which coalescing folds no part of"
    );
}

/// A sibling that does not have the message at the head of its own stream stays
/// out of the coalesced frame — ordering beats coalescing.
///
/// Driven here by a mid-stream subscribe, the deterministic way to give two
/// sibling streams different heads: alice alone sees M1, so M1 goes out
/// single-target; both see M2, which coalesces. Whatever puts a sibling's head
/// somewhere other than the message being written — backlog or a late attach —
/// takes this same arm.
#[tokio::test]
async fn surface_ws_sibling_without_the_message_at_its_head_stays_out_of_the_frame() {
    let db = db::init_db_memory();
    let mut surface = deskbar_sub();
    surface.components.extend(
        ["agenda-alice", "agenda-bob"].map(|instance| ResolvedComponent {
            instance: instance.to_string(),
            kind: "agenda".to_string(),
            abi: brenn_surface_proto::Abi::Dom,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: false,
        }),
    );
    surface.subscriptions.extend(
        ["agenda-alice", "agenda-bob"].map(|instance| SurfaceBinding {
            channel_address: EPH_ADDR.to_string(),
            instance: instance.to_string(),
            port: PORT.to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: NoiseLevel::Silent,
        }),
    );
    // retain_depth 0: no replay, so bob's stream starts at his subscribe.
    let SurfaceTestHarness { state, bus, .. } =
        surface_harness(&db, surface, vec![ephemeral_channel_entry(EPH_NAME, 0, 16)]);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;
    ws.send(subscribe_frame_as(EPH_ADDR, "agenda-alice", None))
        .await
        .expect("send Subscribe");
    next_subscribe_result(&mut ws, EPH_ADDR, "agenda-alice").await;

    publish_eph(&bus, EPH_NAME, EPH_ADDR, "m1");
    // Read M1 out before bob attaches: only alice's subscription existed for it.
    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            envelope, targets, ..
        } => {
            assert_eq!(envelope.body, "m1");
            let target = sole_target(&targets);
            assert_eq!(target.instance, "agenda-alice");
        }
        other => panic!("expected Deliver, got {other:?}"),
    }

    ws.send(subscribe_frame_as(EPH_ADDR, "agenda-bob", None))
        .await
        .expect("send Subscribe");
    next_subscribe_result(&mut ws, EPH_ADDR, "agenda-bob").await;

    publish_eph(&bus, EPH_NAME, EPH_ADDR, "m2");
    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            envelope, targets, ..
        } => {
            assert_eq!(envelope.body, "m2");
            assert_eq!(
                targets.len(),
                2,
                "once both streams hold it, the message coalesces again: {targets:?}"
            );
            let alice = targets
                .iter()
                .find(|t| t.instance == "agenda-alice")
                .expect("alice targeted");
            let bob = targets
                .iter()
                .find(|t| t.instance == "agenda-bob")
                .expect("bob targeted");
            assert_eq!(alice.seq, 2, "alice's span counted M1 then M2");
            assert_eq!(bob.seq, 1, "bob's span starts at his own subscribe");
        }
        other => panic!("expected Deliver, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Bus-plane: slow-client drop accounting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_slow_client_drops_are_counted_exactly() {
    let db = db::init_db_memory();
    // Capacity-2 broadcast ring, no retained ring: a subscribed-but-not-draining
    // client falls behind, the ring overflows, and the excess is dropped and
    // counted. retain_depth 0 keeps the fresh subscribe replay-free so every
    // Deliver comes from the live flood.
    let SurfaceTestHarness { state, bus, .. } = subscribe_harness(&db, 0, 2);
    let (token, _) = setup_authenticated_user(&db).await;
    let epoch = bus.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send Subscribe");
    assert!(matches!(
        next_server_frame(&mut ws).await,
        ServerFrame::SubscribeResult {
            outcome: SubscribeOutcome::Ok,
            replay_count: 0,
            ..
        }
    ));

    // Flood while the client does not read: the capacity-2 ring cannot hold this,
    // so most messages overflow and are dropped. 60 KB bodies fill socket/queue
    // buffers so the drops are real backpressure, not merely cooperative
    // scheduling; split across three senders to stay under the per-sender burst.
    const FLOOD: u64 = 600;
    let big = "x".repeat(60_000);
    publish_as(&bus, "flood-a", EPH_ADDR, EPH_NAME, &big, 200);
    publish_as(&bus, "flood-b", EPH_ADDR, EPH_NAME, &big, 200);
    publish_as(&bus, "flood-c", EPH_ADDR, EPH_NAME, &big, 200);

    // Drain: read every Deliver up to and including the newest seq (always
    // retained in the ring, so always delivered last). Each delivery's `dropped`
    // must equal exactly the seq gap since the previous delivery — the overflow
    // between them folded onto this frame — and undropped deliveries carry 0.
    let mut prev: u64 = 0;
    let mut sum_dropped: u64 = 0;
    loop {
        let seq = match next_server_frame(&mut ws).await {
            ServerFrame::Deliver {
                channel, targets, ..
            } => {
                assert_eq!(channel, EPH_ADDR);
                let target = sole_target(&targets);
                let dropped = target.dropped;
                let seq = match cursor::parse(&target.cursor) {
                    Ok(CursorState::Ephemeral {
                        epoch: got_epoch,
                        seq,
                    }) => {
                        assert_eq!(got_epoch, epoch);
                        seq
                    }
                    other => panic!("expected ephemeral cursor, got {other:?}"),
                };
                assert!(seq > prev, "seqs strictly increase: {prev} then {seq}");
                assert_eq!(
                    dropped,
                    seq - prev - 1,
                    "dropped count must equal the exact seq gap since the previous delivery"
                );
                sum_dropped += dropped;
                prev = seq;
                seq
            }
            other => panic!("expected Deliver, got {other:?}"),
        };
        if seq == FLOOD {
            break;
        }
    }

    assert!(
        sum_dropped > 0,
        "a capacity-2 ring flooded with {FLOOD} messages must drop some"
    );
    assert_eq!(prev, FLOOD, "the newest message is always delivered last");
    // Cross-check the wire-reported total against the bus's own drop counter for
    // this (channel, participant): they must agree exactly. The participant is
    // the subscribing *instance* — the principal that asked for the stream — so
    // the bus's own attribution names the component, not the page it rode in on.
    assert_eq!(
        bus.drop_count(EPH_NAME, "surface:deskbar#protobar"),
        sum_dropped,
        "summed Deliver.dropped must match the bus drop counter"
    );
    assert_eq!(
        bus.drop_count(EPH_NAME, "surface:deskbar"),
        0,
        "the bare surface grain subscribes nothing here, so it counts nothing"
    );
}

/// The context-feed counterpart of the test above, and the one place the two
/// halves of §4.2's depth-0 contract are visible at once: an ephemeral
/// subscription whose fold-max `push_depth` is 0 still gets **every** row — the
/// rows are the page ring's diet, and `retain_depth` bounds page memory, not the
/// wire — but never a drop count, because no push window exists behind them to
/// overflow.
///
/// The same flood as the test above, so the bus's own broadcast-lag counter goes
/// up exactly as it does there: the loss is real, and the wire's silence about it
/// is the point. On a context-only subscription lag surfaces, if at all, as
/// thinner retained context — never as `Deliver.dropped`.
#[tokio::test]
async fn surface_ws_context_feed_delivers_rows_but_never_reports_drops() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, bus, .. } = surface_harness(
        &db,
        deskbar_context_feed(),
        vec![ephemeral_channel_entry(EPH_NAME, 0, 2)],
    );
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send Subscribe");
    assert!(matches!(
        next_server_frame(&mut ws).await,
        ServerFrame::SubscribeResult {
            outcome: SubscribeOutcome::Ok,
            replay_count: 0,
            ..
        }
    ));

    const FLOOD: u64 = 600;
    let big = "x".repeat(60_000);
    publish_as(&bus, "flood-a", EPH_ADDR, EPH_NAME, &big, 200);
    publish_as(&bus, "flood-b", EPH_ADDR, EPH_NAME, &big, 200);
    publish_as(&bus, "flood-c", EPH_ADDR, EPH_NAME, &big, 200);

    let mut prev: u64 = 0;
    loop {
        let seq = match next_server_frame(&mut ws).await {
            ServerFrame::Deliver { targets, .. } => {
                let target = sole_target(&targets);
                let Ok(CursorState::Ephemeral { seq, .. }) = cursor::parse(&target.cursor) else {
                    panic!("expected ephemeral cursor, got {:?}", target.cursor)
                };
                assert_eq!(
                    target.dropped, 0,
                    "a context feed has no push window, so nothing may be reported dropped \
                     (seq {seq}, gap since {prev})"
                );
                prev = seq;
                seq
            }
            other => panic!("expected Deliver, got {other:?}"),
        };
        if seq == FLOOD {
            break;
        }
    }

    // The rows themselves flowed, and the loss the wire stayed silent about is
    // real: the bus counted it against this very subscription.
    assert_eq!(prev, FLOOD, "the newest message is always delivered last");
    assert!(
        bus.drop_count(EPH_NAME, "surface:deskbar#protobar") > 0,
        "a capacity-2 ring flooded with {FLOOD} messages must drop some — the point is that \
         the wire never says so on a context feed"
    );
}

// ---------------------------------------------------------------------------
// Bus-plane: Subscribe + delivery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_subscribe_fresh_replays_retained_ring() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, bus, .. } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    // Publish two before anyone connects: the retained ring holds them.
    publish(&bus, "first");
    publish(&bus, "second");
    let epoch = bus.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send Subscribe");

    match next_server_frame(&mut ws).await {
        ServerFrame::SubscribeResult {
            channel,
            instance,
            outcome,
            replay_count,
            gap,
        } => {
            assert_eq!(channel, EPH_ADDR);
            assert_eq!(instance, COMPONENT);
            assert!(matches!(outcome, SubscribeOutcome::Ok));
            assert_eq!(replay_count, 2);
            assert!(gap.is_none(), "fresh subscribe within ring has no gap");
        }
        other => panic!("expected SubscribeResult, got {other:?}"),
    }

    assert_deliver(
        next_server_frame(&mut ws).await,
        EPH_ADDR,
        "first",
        1,
        0,
        epoch,
    );
    assert_deliver(
        next_server_frame(&mut ws).await,
        EPH_ADDR,
        "second",
        2,
        0,
        epoch,
    );
}

#[tokio::test]
async fn surface_ws_subscribe_then_live_publish_delivers() {
    let db = db::init_db_memory();
    // retain_depth 0: no ring, so the subscribe replays nothing and the live
    // publish is the only delivery.
    let SurfaceTestHarness { state, bus, .. } = subscribe_harness(&db, 0, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    let epoch = bus.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send Subscribe");
    match next_server_frame(&mut ws).await {
        ServerFrame::SubscribeResult {
            replay_count, gap, ..
        } => {
            assert_eq!(replay_count, 0);
            assert!(gap.is_none());
        }
        other => panic!("expected SubscribeResult, got {other:?}"),
    }

    // Publish after the subscription is live: it arrives over the delivery arm.
    publish(&bus, "live");
    assert_deliver(
        next_server_frame(&mut ws).await,
        EPH_ADDR,
        "live",
        1,
        0,
        epoch,
    );
}

#[tokio::test]
async fn surface_ws_subscribe_unbound_channel_is_violation() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    assert_frame_is_violation(
        &base,
        &token,
        subscribe_frame("ephemeral:not-bound", None),
        &flusher,
        &alerts,
    )
    .await;
}

/// A second surface binding a distinct channel, so that channel exists in
/// committed config but is *not* bound to `deskbar`.
fn otherbar() -> ResolvedSurface {
    ResolvedSurface {
        slug: "otherbar".to_string(),
        skin: "bench".to_string(),
        components: vec![ResolvedComponent {
            instance: COMPONENT.to_string(),
            kind: COMPONENT.to_string(),
            abi: brenn_surface_proto::Abi::Dom,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: true,
        }],
        subscriptions: vec![SurfaceBinding {
            channel_address: OTHERBAR_ADDR.to_string(),
            instance: COMPONENT.to_string(),
            port: PORT.to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: NoiseLevel::Silent,
        }],
        durable_subscriptions: vec![],
        local_channels: vec![],
        outputs: vec![],
        policy: AppPolicy::default(),
        allowed_users: vec![],
        publish_burst: 60,
        publish_per_sec: 1,
    }
}

/// Subscribe `channel` on a fresh `deskbar` session and return the observed
/// close shape, enforcing the "no leaked response frame" contract along the way.
async fn subscribe_and_observe_close(base: &str, token: &str, channel: &str) -> CloseObservation {
    send_frame_observe_close(base, token, subscribe_frame(channel, None)).await
}

/// The no-existence-oracle property: a channel bound to a *different* surface
/// (present in committed config *and* on the bus) and a channel that exists
/// nowhere both produce byte-identical client-observable behavior — no response
/// frame, then an identical close shape — so nothing on the wire distinguishes
/// "exists but not yours" from "doesn't exist". Both fire one
/// `SurfaceProtocolViolation` of the same event type; the server-side alert
/// *details* legitimately differ by channel address (diagnostics, never sent to
/// the client), so only the event type and the wire behavior are compared.
#[tokio::test]
async fn surface_ws_no_existence_oracle_unbound_vs_nonexistent() {
    let db = db::init_db_memory();
    let (mut state, alerts, _h) = test_state_with_capturing_alerter(&db);
    // Both channels exist at the bus layer, so `otherbar-only` is genuinely
    // "exists on the bus AND in another surface's config, but not in deskbar's
    // map" — the real exists-but-not-yours probe. `pure-fiction` exists nowhere.
    // Any code path that consulted bus existence to answer differently would
    // diverge between the two inputs and fail the close-shape assertion below.
    let bus = fixture_bus(vec![
        ephemeral_channel_entry(EPH_NAME, 4, 16),
        ephemeral_channel_entry(OTHERBAR_NAME, 4, 16),
    ]);
    // `deskbar` binds EPH_ADDR; `otherbar` binds OTHERBAR_ADDR. From a deskbar
    // session the latter is a channel absent from deskbar's own
    // `subscription_channels` — indistinguishable, in the single fail-closed
    // lookup, from a channel bound nowhere.
    state.surfaces = Arc::new(build_surface_runtimes(
        vec![deskbar_sub(), otherbar()],
        bus,
        None,
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    let dispatcher = state.alert_dispatcher.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // Input 1: bound to a *different* surface (exists in config and on the bus).
    let close_bound_elsewhere = subscribe_and_observe_close(&base, &token, OTHERBAR_ADDR).await;
    // Input 2: bound to no surface at all, and absent from the bus.
    let close_nonexistent =
        subscribe_and_observe_close(&base, &token, "ephemeral:pure-fiction").await;

    // Byte-identical observable behavior: both inputs produced the same
    // fail-closed wire outcome — no response frame (the drainer asserts that) and
    // an identical close shape, including any close code *and reason* (a divergent
    // code or reason would be an existence oracle) — and each fired exactly one
    // `SurfaceProtocolViolation` for the subscribe.
    assert_eq!(
        close_bound_elsewhere, close_nonexistent,
        "the two inputs must close identically on the wire (no existence oracle): \
         {close_bound_elsewhere:?} vs {close_nonexistent:?}"
    );
    wait_for_len(&alerts, 2).await;
    dispatcher.flush().await;
    let captured = alerts.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        2,
        "expected two violations, got {captured:?}"
    );
    for alert in &captured {
        let combined = format!("{} {}", alert.0, alert.1);
        assert!(
            combined.contains("surface_protocol_violation"),
            "expected a protocol violation, got {combined}"
        );
        assert!(
            combined.contains("Subscribe to unbound subscription"),
            "both kills must be for the unbound Subscribe, got {combined}"
        );
    }
}

#[tokio::test]
async fn surface_ws_subscribe_duplicate_is_violation() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = subscribe_harness(&db, 0, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // First subscribe succeeds.
    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send Subscribe");
    assert!(matches!(
        next_server_frame(&mut ws).await,
        ServerFrame::SubscribeResult {
            outcome: SubscribeOutcome::Ok,
            ..
        }
    ));

    // Second subscribe to the same active channel is a violation.
    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send duplicate Subscribe");
    assert!(
        drain_until_closed(&mut ws).await,
        "duplicate Subscribe must close the connection"
    );
    assert_single_alert(&flusher, &alerts, "surface_protocol_violation").await;
}

#[tokio::test]
async fn surface_ws_subscribe_class_mismatched_resume_is_violation() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // A durable resume token on an ephemeral channel is a class mismatch.
    assert_frame_is_violation(
        &base,
        &token,
        subscribe_frame(
            EPH_ADDR,
            Some(cursor::mint_durable(Uuid::nil(), 0, 3, vec![])),
        ),
        &flusher,
        &alerts,
    )
    .await;
}

#[tokio::test]
async fn surface_ws_subscribe_resume_ahead_is_violation() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        bus,
        ..
    } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    // Matching epoch, but a seq far past anything this boot assigned (nothing
    // published, so newest seq is 0): impossible for an honest client.
    let resume = Some(cursor::mint_ephemeral(bus.epoch(), 999));
    let (base, _sd) = spawn_test_server(state).await;

    assert_frame_is_violation(
        &base,
        &token,
        subscribe_frame(EPH_ADDR, resume),
        &flusher,
        &alerts,
    )
    .await;
}

// ---------------------------------------------------------------------------
// Bus-plane: resume / gap mapping
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_subscribe_resume_exact_replays_tail() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, bus, .. } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    publish(&bus, "one");
    publish(&bus, "two");
    publish(&bus, "three");
    let epoch = bus.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Resume from seq 1: seqs 2 and 3 are owed and within the ring → Replay::Exact,
    // no gap.
    ws.send(subscribe_frame(
        EPH_ADDR,
        Some(cursor::mint_ephemeral(epoch, 1)),
    ))
    .await
    .expect("send Subscribe");
    match next_server_frame(&mut ws).await {
        ServerFrame::SubscribeResult {
            replay_count, gap, ..
        } => {
            assert_eq!(replay_count, 2);
            assert!(gap.is_none(), "exact resume within ring has no gap");
        }
        other => panic!("expected SubscribeResult, got {other:?}"),
    }
    assert_deliver(
        next_server_frame(&mut ws).await,
        EPH_ADDR,
        "two",
        2,
        0,
        epoch,
    );
    assert_deliver(
        next_server_frame(&mut ws).await,
        EPH_ADDR,
        "three",
        3,
        0,
        epoch,
    );
}

#[tokio::test]
async fn surface_ws_subscribe_resume_up_to_date_no_replay() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, bus, .. } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    publish(&bus, "one");
    publish(&bus, "two");
    let epoch = bus.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Resume from the newest seq: caught up → Replay::UpToDate, nothing replayed,
    // no gap.
    ws.send(subscribe_frame(
        EPH_ADDR,
        Some(cursor::mint_ephemeral(epoch, 2)),
    ))
    .await
    .expect("send Subscribe");
    match next_server_frame(&mut ws).await {
        ServerFrame::SubscribeResult {
            replay_count, gap, ..
        } => {
            assert_eq!(replay_count, 0);
            assert!(gap.is_none());
        }
        other => panic!("expected SubscribeResult, got {other:?}"),
    }
}

#[tokio::test]
async fn surface_ws_subscribe_resume_hole_exceeds_ring_gaps() {
    let db = db::init_db_memory();
    // retain_depth 1: the ring keeps only the newest message, so a resume from an
    // older seq cannot be healed exactly.
    let SurfaceTestHarness { state, bus, .. } = subscribe_harness(&db, 1, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    publish(&bus, "one");
    publish(&bus, "two");
    publish(&bus, "three");
    let epoch = bus.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Resume from seq 1 but the ring only retains seq 3 → Gap(HoleExceedsRing)
    // with a full-ring replay.
    ws.send(subscribe_frame(
        EPH_ADDR,
        Some(cursor::mint_ephemeral(epoch, 1)),
    ))
    .await
    .expect("send Subscribe");
    match next_server_frame(&mut ws).await {
        ServerFrame::SubscribeResult {
            replay_count, gap, ..
        } => {
            assert_eq!(replay_count, 1, "full ring (depth 1) replayed");
            assert!(
                matches!(
                    gap,
                    Some(GapInfo {
                        reason: GapReason::HoleExceedsRing
                    })
                ),
                "expected HoleExceedsRing, got {gap:?}"
            );
        }
        other => panic!("expected SubscribeResult, got {other:?}"),
    }
    assert_deliver(
        next_server_frame(&mut ws).await,
        EPH_ADDR,
        "three",
        3,
        0,
        epoch,
    );
}

#[tokio::test]
async fn surface_ws_subscribe_resume_wrong_epoch_gaps() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, bus, .. } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    publish(&bus, "one");
    publish(&bus, "two");
    let epoch = bus.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // A resume epoch that doesn't match the bus (e.g. a pre-restart token) →
    // Gap(EpochChanged) with a full-ring replay. Deliveries carry the live bus
    // epoch, not the stale resume epoch.
    ws.send(subscribe_frame(
        EPH_ADDR,
        Some(cursor::mint_ephemeral(Uuid::new_v4(), 1)),
    ))
    .await
    .expect("send Subscribe");
    match next_server_frame(&mut ws).await {
        ServerFrame::SubscribeResult {
            replay_count, gap, ..
        } => {
            assert_eq!(replay_count, 2, "full ring replayed on epoch change");
            assert!(
                matches!(
                    gap,
                    Some(GapInfo {
                        reason: GapReason::EpochChanged
                    })
                ),
                "expected EpochChanged, got {gap:?}"
            );
        }
        other => panic!("expected SubscribeResult, got {other:?}"),
    }
    assert_deliver(
        next_server_frame(&mut ws).await,
        EPH_ADDR,
        "one",
        1,
        0,
        epoch,
    );
    assert_deliver(
        next_server_frame(&mut ws).await,
        EPH_ADDR,
        "two",
        2,
        0,
        epoch,
    );
}

// ---------------------------------------------------------------------------
// Bus-plane: Unsubscribe
// ---------------------------------------------------------------------------

fn unsubscribe_frame(channel: &str) -> Message {
    unsubscribe_frame_as(channel, COMPONENT)
}

fn unsubscribe_frame_as(channel: &str, instance: &str) -> Message {
    let frame = ClientFrame::Unsubscribe {
        channel: channel.to_string(),
        instance: instance.to_owned(),
    };
    Message::Text(serde_json::to_string(&frame).expect("serialize").into())
}

#[tokio::test]
async fn surface_ws_unsubscribe_removes_active_subscription() {
    let db = db::init_db_memory();
    let SurfaceTestHarness { state, alerts, .. } = subscribe_harness(&db, 0, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Subscribe, then unsubscribe (fire-and-forget: no ack).
    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send Subscribe");
    assert!(matches!(
        next_server_frame(&mut ws).await,
        ServerFrame::SubscribeResult {
            outcome: SubscribeOutcome::Ok,
            ..
        }
    ));
    ws.send(unsubscribe_frame(EPH_ADDR))
        .await
        .expect("send Unsubscribe");

    // Re-subscribing the same channel now succeeds: it would be a duplicate
    // violation had the Unsubscribe not removed the active subscription. Inbound
    // frames are processed in order on one task, so this SubscribeResult proves
    // the Unsubscribe took effect.
    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send re-Subscribe");
    assert!(matches!(
        next_server_frame(&mut ws).await,
        ServerFrame::SubscribeResult {
            outcome: SubscribeOutcome::Ok,
            ..
        }
    ));
    assert!(
        alerts.lock().unwrap().is_empty(),
        "Unsubscribe of an active channel must not fire a security event, got {:?}",
        alerts.lock().unwrap()
    );
}

#[tokio::test]
async fn surface_ws_unsubscribe_not_subscribed_is_violation() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = subscribe_harness(&db, 4, 16);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // Never subscribed: unknown, unbound, and never-active are all the same
    // violation (no existence oracle).
    assert_frame_is_violation(
        &base,
        &token,
        unsubscribe_frame(EPH_ADDR),
        &flusher,
        &alerts,
    )
    .await;
}

// ---------------------------------------------------------------------------
// Bus-plane: Publish
// ---------------------------------------------------------------------------

/// A durable output channel bound on the publish fixture; published for real via
/// `publish_from_surface` when the runtime carries a `Messenger`.
const DUR_OUT_ADDR: &str = "brenn:writer-out";
/// Bare name of `DUR_OUT_ADDR` (for `brenn_publish` ACL matchers + channel decl).
const DUR_OUT_NAME: &str = "writer-out";
/// The `writer`/`durable` port's configured default urgency on the publish
/// fixture. Deliberately not `Normal` — the rung the removed hard-coded constant
/// used — so a test asserting a persisted row's urgency proves the port's
/// configured default was actually resolved and applied.
const DUR_OUT_DEFAULT_URGENCY: Urgency = Urgency::Low;

/// A `deskbar` surface wired for Publish: an ephemeral output port
/// (`writer`/`out` → `EPH_ADDR`), a durable output port (`writer`/`durable` →
/// `DUR_OUT_ADDR`), and an ephemeral subscription on `EPH_ADDR` so a second
/// session can observe the published message. Its policy covers ephemeral
/// publish + subscribe on the fixture channel, so the runtime's own
/// `bus.publish`/`bus.subscribe` pass their ACL checks. Rate caps are
/// parameterized so flood/no-token-consumed tests can pin small budgets.
/// The budgeted surface principals for a set of resolved surfaces, derived from
/// `ResolvedSurface::principal_send_budgets` — the same authority boot uses in
/// `bootstrap::messaging`, so a fixture cannot budget a different principal set
/// than the surfaces it installs, nor meter one differently than boot would. A
/// drift here would surface as a "no send budget" panic in an unrelated test.
fn budget_principals(surfaces: &[ResolvedSurface]) -> Vec<(String, SurfacePrincipalBudgets)> {
    surfaces
        .iter()
        .map(|s| (s.slug.clone(), s.principal_send_budgets().collect()))
        .collect()
}

fn deskbar_pub(publish_burst: u32, publish_per_sec: u32) -> ResolvedSurface {
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::EphemeralPublish);
    policy.grants.insert(AppCapability::EphemeralSubscribe);
    policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact(EPH_NAME.to_string())];
    policy.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(EPH_NAME.to_string())];
    ResolvedSurface {
        slug: "deskbar".to_string(),
        skin: "bench".to_string(),
        components: vec![
            ResolvedComponent {
                instance: COMPONENT.to_string(),
                kind: COMPONENT.to_string(),
                abi: brenn_surface_proto::Abi::Dom,
                send_budget: SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: true,
            },
            ResolvedComponent {
                instance: "writer".to_string(),
                // Deliberately *not* the instance id: the sub-identity is
                // instance-grain, so a fixture whose instance equals its kind
                // could not tell "the instance was stamped" from "the kind was".
                // With them distinct, every `surface:deskbar#writer` assertion
                // below is a live proof of which half the server reads.
                kind: "writer-module".to_string(),
                abi: brenn_surface_proto::Abi::Dom,
                send_budget: SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: false,
            },
        ],
        subscriptions: vec![SurfaceBinding {
            channel_address: EPH_ADDR.to_string(),
            instance: COMPONENT.to_string(),
            port: PORT.to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: NoiseLevel::Silent,
        }],
        durable_subscriptions: vec![],
        local_channels: vec![],
        outputs: vec![
            SurfaceOutput {
                channel_address: EPH_ADDR.to_string(),
                instance: "writer".to_string(),
                port: "out".to_string(),
                default_urgency: Urgency::Normal,
                budget: brenn_budget::SinkBudget {
                    fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                    capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                },
            },
            SurfaceOutput {
                channel_address: DUR_OUT_ADDR.to_string(),
                instance: "writer".to_string(),
                port: "durable".to_string(),
                // Deliberately *not* the `Normal` the old hard-coded call site
                // sent: the persisted-row assertions below would pass either way
                // at `Normal`, and could not tell "the port's default was
                // applied" from "the dead constant is still there".
                default_urgency: DUR_OUT_DEFAULT_URGENCY,
                budget: brenn_budget::SinkBudget {
                    fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                    capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                },
            },
        ],
        policy,
        allowed_users: vec![],
        publish_burst,
        publish_per_sec,
    }
}

/// Capturing-alerter harness whose `deskbar` surface is the publish fixture
/// (`retain_depth 0`: no ring, so a subscriber sees only live deliveries).
fn publish_state(db: &db::Db, publish_burst: u32, publish_per_sec: u32) -> SurfaceTestHarness {
    surface_harness(
        db,
        deskbar_pub(publish_burst, publish_per_sec),
        vec![ephemeral_channel_entry(EPH_NAME, 0, 16)],
    )
}

/// Publish-fixture state whose `deskbar_pub` surface can publish to its durable
/// output (`brenn:writer-out`) for real: a `Messenger` declares that channel and
/// `surface_policies` grants `deskbar` `MessagingPublish` + `brenn_publish`
/// coverage, so `handle_publish`'s durable arm runs `publish_from_surface`.
/// Returns the capturing alerts and the durable channel UUID (to read back the
/// persisted row).
async fn durable_publish_state(
    db: &db::Db,
    publish_burst: u32,
) -> (AppState, Arc<Mutex<Vec<(String, String)>>>, Uuid) {
    let (mut state, alerts, _handle) = test_state_with_capturing_alerter(db);

    // Declare brenn:writer-out in the DB + directory.
    let channel_uuid = Uuid::new_v4();
    let raw = ChannelConfigRaw {
        uuid: channel_uuid.to_string(),
        address: DUR_OUT_NAME.to_string(),
        description: None,
        push_depth: None,
        retain_depth: None,
        standing_retain_depth: None,
        noise: None,
        sink: None,
        wake_min: None,
    };
    let entry = build_channel_entries(&[raw], &MessagingGlobalConfig::default())
        .pop()
        .expect("one channel entry");
    {
        let conn = db.lock().await;
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&entry));
    }

    // deskbar's surface policy: MessagingPublish + a covering brenn_publish matcher.
    let mut deskbar_policy = AppPolicy::default();
    deskbar_policy
        .grants
        .insert(AppCapability::MessagingPublish);
    deskbar_policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Exact(DUR_OUT_NAME.to_string()));
    let mut surface_policies = std::collections::HashMap::new();
    surface_policies.insert("deskbar".to_string(), deskbar_policy);
    let surfaces = vec![deskbar_pub(publish_burst, 1)];

    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![entry])),
        Arc::from(TEST_ORIGIN),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
            as Arc<dyn brenn_lib::messaging::WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_lib::messaging::testutils::surface_registrations(
        surface_policies,
    ))
    .with_surface_send_budgets(budget_principals(&surfaces));

    let bus = fixture_bus(vec![ephemeral_channel_entry(EPH_NAME, 0, 16)]);
    state.surfaces = Arc::new(build_surface_runtimes(
        surfaces,
        bus,
        Some(messenger),
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    (state, alerts, channel_uuid)
}

/// Publish-fixture state wired for the reserved error-report port: the error
/// channel `brenn:surface-errors` is declared, `deskbar`'s policy carries the
/// substrate-injected error-channel grant (as `resolve`+injection would produce),
/// and `build_surface_runtimes` binds the reserved `#brenn`/`error-reports` port
/// and advertises the `warn` floor. A `Publish` to that port routes through
/// `publish_from_surface` exactly like any bound durable output. Returns the
/// error channel UUID so the persisted report can be read back.
async fn error_report_publish_state(db: &db::Db) -> (AppState, Uuid) {
    let (mut state, _alerts, _handle) = test_state_with_capturing_alerter(db);

    let channel_uuid = Uuid::new_v4();
    let raw = ChannelConfigRaw {
        uuid: channel_uuid.to_string(),
        address: "surface-errors".to_string(),
        description: None,
        push_depth: None,
        retain_depth: None,
        standing_retain_depth: None,
        noise: None,
        sink: None,
        wake_min: None,
    };
    let entry = build_channel_entries(&[raw], &MessagingGlobalConfig::default())
        .pop()
        .expect("one channel entry");
    {
        let conn = db.lock().await;
        brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&entry));
    }

    // Substrate-injected grant: deskbar may publish onto the error channel.
    let mut deskbar_policy = AppPolicy::default();
    deskbar_policy
        .grants
        .insert(AppCapability::MessagingPublish);
    deskbar_policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Exact("surface-errors".to_string()));
    let mut surface_policies = std::collections::HashMap::new();
    surface_policies.insert("deskbar".to_string(), deskbar_policy);
    let surfaces = vec![deskbar_pub(60, 1)];

    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![entry])),
        Arc::from(TEST_ORIGIN),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
            as Arc<dyn brenn_lib::messaging::WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_lib::messaging::testutils::surface_registrations(
        surface_policies,
    ))
    .with_surface_send_budgets(budget_principals(&surfaces));

    let bus = fixture_bus(vec![ephemeral_channel_entry(EPH_NAME, 0, 16)]);
    state.surfaces = Arc::new(build_surface_runtimes(
        surfaces,
        bus,
        Some(messenger),
        TEST_MAX_BODY_BYTES,
        Some(("brenn:surface-errors".to_string(), LogLevel::Warn)),
        crate::test_support::surface::description_params(),
    ));
    (state, channel_uuid)
}

/// A surface error report is an ordinary `Publish` to the reserved port. With no
/// `subject_instance` it is the kernel's own report: it lands on the error
/// channel under the bare `surface:<slug>` identity (no relay, no `system:`
/// sender), the client body verbatim, and `Welcome` advertises the reserved-port
/// floor.
#[tokio::test]
async fn surface_ws_error_report_publishes_under_surface_sender() {
    let db = db::init_db_memory();
    let (state, channel_uuid) = error_report_publish_state(&db).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;

    match next_server_frame(&mut ws).await {
        ServerFrame::Welcome {
            error_report_floor, ..
        } => assert_eq!(
            error_report_floor,
            Some(LogLevel::Warn),
            "Welcome advertises the configured reserved-port floor"
        ),
        other => panic!("expected Welcome, got {other:?}"),
    }

    let body = r#"{"source":"component:echo-stub","message":"boom","level":"error"}"#;
    ws.send(publish_frame(
        ERROR_REPORT_INSTANCE,
        ERROR_REPORT_PORT,
        body,
        Some(3),
    ))
    .await
    .expect("send Publish");
    let outcome = publish_result_outcome(next_server_frame(&mut ws).await, Some(3));
    assert!(
        matches!(outcome, PublishOutcome::Ok),
        "expected Ok, got {outcome:?}"
    );

    let rows = read_channel_messages(&db, channel_uuid).await;
    assert_eq!(
        rows,
        vec![("surface:deskbar".to_string(), body.to_string())],
        "a subject-less report persists one row with the bare surface sender and the client body \
         verbatim"
    );
}

/// End-to-end: a report naming a declared `subject_instance` is attributed to
/// that component's sub-identity. The body's `source` string is *not* the
/// derivation input — it names `echo-stub` here while the validated subject names
/// `writer`, and the sender follows the subject.
#[tokio::test]
async fn surface_ws_error_report_with_subject_publishes_under_sub_identity() {
    let db = db::init_db_memory();
    let (state, channel_uuid) = error_report_publish_state(&db).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    let body = r#"{"source":"component:echo-stub","message":"boom","level":"error"}"#;
    ws.send(publish_frame_with_subject(
        ERROR_REPORT_INSTANCE,
        ERROR_REPORT_PORT,
        body,
        Some(3),
        Some("writer"),
    ))
    .await
    .expect("send Publish");
    let outcome = publish_result_outcome(next_server_frame(&mut ws).await, Some(3));
    assert!(
        matches!(outcome, PublishOutcome::Ok),
        "expected Ok, got {outcome:?}"
    );

    let rows = read_channel_messages(&db, channel_uuid).await;
    assert_eq!(
        rows,
        vec![("surface:deskbar#writer".to_string(), body.to_string())],
        "the sender follows the validated subject_instance, never the body's source string"
    );
}

/// End-to-end: a report naming an undeclared subject kills the connection and
/// fires the security event. This is the fail2ban signal for a client trying to
/// spell an identity the server never declared.
#[tokio::test]
async fn surface_ws_error_report_with_undeclared_subject_is_killed() {
    let db = db::init_db_memory();
    let (state, channel_uuid) = error_report_publish_state(&db).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    let body = r#"{"source":"component:echo-stub","message":"boom","level":"error"}"#;
    ws.send(publish_frame_with_subject(
        ERROR_REPORT_INSTANCE,
        ERROR_REPORT_PORT,
        body,
        Some(3),
        Some("never-declared"),
    ))
    .await
    .expect("send Publish");

    assert!(
        drain_until_closed(&mut ws).await,
        "an undeclared subject_instance must tear the session down"
    );
    assert!(
        read_channel_messages(&db, channel_uuid).await.is_empty(),
        "a violating frame must publish nothing"
    );
}

fn publish_frame(component: &str, port: &str, body: &str, correlation: Option<u64>) -> Message {
    publish_frame_with_subject(component, port, body, correlation, None)
}

/// [`publish_frame`] carrying a `subject_instance` — the shape only the reserved
/// error-report port may legally send, and the shape a non-conforming client
/// uses to try to launder attribution onto another component.
fn publish_frame_with_subject(
    component: &str,
    port: &str,
    body: &str,
    correlation: Option<u64>,
    subject_instance: Option<&str>,
) -> Message {
    let frame = ClientFrame::Publish {
        instance: component.to_string(),
        port: port.to_string(),
        body: body.to_string(),
        correlation,
        subject_instance: subject_instance.map(str::to_owned),
        urgency: None,
    };
    Message::Text(serde_json::to_string(&frame).expect("serialize").into())
}

/// A `PublishBatch` frame — one activation flush, its entries in call order.
fn publish_batch_frame(instance: &str, correlation: u64, entries: &[(&str, &str)]) -> Message {
    let frame = ClientFrame::PublishBatch {
        instance: instance.to_string(),
        correlation,
        publishes: entries
            .iter()
            .map(|(port, body)| BatchEntry {
                port: port.to_string(),
                body: body.to_string(),
                urgency: None,
            })
            .collect(),
    };
    Message::Text(serde_json::to_string(&frame).expect("serialize").into())
}

/// Assert a `PublishResult` frame with the expected correlation, returning its
/// outcome for the caller to match.
fn publish_result_outcome(frame: ServerFrame, correlation: Option<u64>) -> PublishOutcome {
    match frame {
        ServerFrame::PublishResult {
            correlation: got,
            outcome,
        } => {
            assert_eq!(got, correlation, "PublishResult echoes correlation");
            outcome
        }
        other => panic!("expected PublishResult, got {other:?}"),
    }
}

#[tokio::test]
async fn surface_ws_publish_ok_delivers_to_sibling() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state, alerts, bus, ..
    } = publish_state(&db, 60, 1);
    let (token, _) = setup_authenticated_user(&db).await;
    let epoch = bus.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));

    // Session A subscribes to the channel the output port publishes onto.
    let mut a = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut a).await;
    a.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("send Subscribe");
    assert!(matches!(
        next_server_frame(&mut a).await,
        ServerFrame::SubscribeResult {
            outcome: SubscribeOutcome::Ok,
            replay_count: 0,
            ..
        }
    ));

    // Session B publishes through the ephemeral output port.
    let mut b = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut b).await;
    b.send(publish_frame("writer", "out", "hello", Some(7)))
        .await
        .expect("send Publish");

    let outcome = publish_result_outcome(next_server_frame(&mut b).await, Some(7));
    assert!(
        matches!(outcome, PublishOutcome::Ok),
        "expected Ok, got {outcome:?}"
    );

    // The sibling subscriber observes the published message.
    assert_deliver(
        next_server_frame(&mut a).await,
        EPH_ADDR,
        "hello",
        1,
        0,
        epoch,
    );

    assert!(
        alerts.lock().unwrap().is_empty(),
        "a successful Publish must not fire a security event, got {:?}",
        alerts.lock().unwrap()
    );
}

#[tokio::test]
async fn surface_ws_publish_durable_output_persists_and_stays_open() {
    let db = db::init_db_memory();
    let (state, alerts, channel_uuid) = durable_publish_state(&db, 60).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // A durable-bound output now publishes for real via publish_from_surface:
    // answered Ok and persisted with the backend-derived component sub-identity.
    ws.send(publish_frame("writer", "durable", "hi", None))
        .await
        .expect("send Publish");
    let outcome = publish_result_outcome(next_server_frame(&mut ws).await, None);
    assert!(
        matches!(outcome, PublishOutcome::Ok),
        "expected Ok, got {outcome:?}"
    );

    // The message persisted on the durable channel, stamped with the sub-identity
    // the server admitted from the frame's `instance` — the client asserted no
    // identity to get here. `writer`'s kind is `writer-module`, so this also pins
    // that the instance half, not the kind, is what lands.
    let rows = read_channel_messages(&db, channel_uuid).await;
    assert_eq!(
        rows,
        vec![("surface:deskbar#writer".to_string(), "hi".to_string())],
        "a component publish persists under that component's instance sub-identity"
    );

    assert!(
        saw_heartbeat_within(&mut ws, 4).await,
        "connection must stay open after a durable Publish"
    );
    assert!(
        alerts.lock().unwrap().is_empty(),
        "a successful durable Publish must not fire a security event, got {:?}",
        alerts.lock().unwrap()
    );
}

/// The per-connection publish bucket gates the durable arm exactly as it does the
/// ephemeral arm: with burst 1, the first durable publish spends the only token
/// and the second is `RateLimited` (never a kill), and only the first persists.
#[tokio::test]
async fn surface_ws_publish_durable_output_rate_limited() {
    let db = db::init_db_memory();
    let (state, _alerts, channel_uuid) = durable_publish_state(&db, 1).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;

    ws.send(publish_frame("writer", "durable", "one", Some(1)))
        .await
        .expect("send first durable Publish");
    assert!(
        matches!(
            publish_result_outcome(next_server_frame(&mut ws).await, Some(1)),
            PublishOutcome::Ok
        ),
        "first durable publish spends the token and is Ok"
    );

    ws.send(publish_frame("writer", "durable", "two", Some(2)))
        .await
        .expect("send second durable Publish");
    assert!(
        matches!(
            publish_result_outcome(next_server_frame(&mut ws).await, Some(2)),
            PublishOutcome::RateLimited
        ),
        "second durable publish is denied by the connection bucket"
    );

    // Only the first publish persisted; the rate-limited one wrote nothing.
    let rows = read_channel_messages(&db, channel_uuid).await;
    assert_eq!(
        rows,
        vec![("surface:deskbar#writer".to_string(), "one".to_string())]
    );
}

#[tokio::test]
async fn surface_ws_publish_oversized_body_consumes_no_token() {
    let db = db::init_db_memory();
    // Burst 1: exactly one token. If the oversized publish consumed it, the
    // subsequent legal publish would be RateLimited; asserting it is Ok proves
    // the oversized publish was gated before the bucket.
    let SurfaceTestHarness { state, alerts, .. } = publish_state(&db, 1, 1);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Over the body cap but well under the derived WS frame cap.
    let huge = "x".repeat(TEST_MAX_BODY_BYTES + 1);
    ws.send(publish_frame("writer", "out", &huge, Some(1)))
        .await
        .expect("send oversized Publish");
    match publish_result_outcome(next_server_frame(&mut ws).await, Some(1)) {
        PublishOutcome::BodyTooLarge { len, max } => {
            assert_eq!(len, TEST_MAX_BODY_BYTES as u64 + 1);
            assert_eq!(max, TEST_MAX_BODY_BYTES as u64);
        }
        other => panic!("expected BodyTooLarge, got {other:?}"),
    }

    // The one token was not consumed: a legal publish still succeeds.
    ws.send(publish_frame("writer", "out", "ok", Some(2)))
        .await
        .expect("send legal Publish");
    let outcome = publish_result_outcome(next_server_frame(&mut ws).await, Some(2));
    assert!(
        matches!(outcome, PublishOutcome::Ok),
        "oversized publish must consume no rate token, got {outcome:?}"
    );

    assert!(
        alerts.lock().unwrap().is_empty(),
        "oversized Publish must not fire a security event, got {:?}",
        alerts.lock().unwrap()
    );
}

#[tokio::test]
async fn surface_ws_persistent_oversized_body_escalates_to_violation() {
    let db = db::init_db_memory();
    // Generous burst so the interleaved valid publish is never rate-limited;
    // oversized rejects consume no token, so only the valid publish needs one.
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = publish_state(&db, 8, 8);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    let huge = "x".repeat(TEST_MAX_BODY_BYTES + 1);

    // Rejects 1..=7: each answered BodyTooLarge, connection stays live.
    for i in 1..=7u64 {
        ws.send(publish_frame("writer", "out", &huge, Some(i)))
            .await
            .expect("send oversized Publish");
        match publish_result_outcome(next_server_frame(&mut ws).await, Some(i)) {
            PublishOutcome::BodyTooLarge { .. } => {}
            other => panic!("reject {i} must be BodyTooLarge, got {other:?}"),
        }
    }

    // A valid publish still succeeds — the connection is live after 7 rejects.
    ws.send(publish_frame("writer", "out", "ok", Some(100)))
        .await
        .expect("send valid Publish");
    assert!(
        matches!(
            publish_result_outcome(next_server_frame(&mut ws).await, Some(100)),
            PublishOutcome::Ok
        ),
        "connection must remain usable through the first 7 oversized rejects"
    );

    // The 8th oversized reject escalates: no PublishResult, the socket closes,
    // and exactly one surface_protocol_violation is logged for fail2ban.
    ws.send(publish_frame("writer", "out", &huge, Some(8)))
        .await
        .expect("send 8th oversized Publish");
    assert!(
        drain_until_closed(&mut ws).await,
        "the 8th oversized Publish must close the connection"
    );
    assert_single_alert(&flusher, &alerts, "surface_protocol_violation").await;
}

#[tokio::test]
async fn surface_ws_publish_flood_rate_limited_sibling_unaffected() {
    let db = db::init_db_memory();
    // Burst 2, slow refill: a tight flood exhausts the connection bucket.
    let SurfaceTestHarness { state, alerts, .. } = publish_state(&db, 2, 1);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));

    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Six rapid publishes against a burst of 2: the excess is RateLimited (not a
    // kill). Collect all six outcomes.
    for _ in 0..6 {
        ws.send(publish_frame("writer", "out", "spam", None))
            .await
            .expect("send Publish");
    }
    let mut rate_limited = 0;
    for _ in 0..6 {
        match publish_result_outcome(next_server_frame(&mut ws).await, None) {
            PublishOutcome::Ok => {}
            PublishOutcome::RateLimited => rate_limited += 1,
            other => panic!("expected Ok or RateLimited, got {other:?}"),
        }
    }
    assert!(
        rate_limited >= 1,
        "a burst-2 flood of 6 must produce at least one RateLimited outcome"
    );
    assert!(
        saw_heartbeat_within(&mut ws, 4).await,
        "a Publish flood must be rate-limited, not kill the connection"
    );

    // A second session has its own bucket: unaffected by the sibling's flood.
    let mut sibling = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut sibling).await;
    sibling
        .send(publish_frame("writer", "out", "fresh", None))
        .await
        .expect("send sibling Publish");
    let outcome = publish_result_outcome(next_server_frame(&mut sibling).await, None);
    assert!(
        matches!(outcome, PublishOutcome::Ok),
        "a sibling session's fresh bucket must admit its publish, got {outcome:?}"
    );

    assert!(
        alerts.lock().unwrap().is_empty(),
        "a rate-limited Publish flood must not fire a security event, got {:?}",
        alerts.lock().unwrap()
    );
}

#[tokio::test]
async fn surface_ws_publish_unbound_port_is_violation() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = publish_state(&db, 60, 1);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // A (component, port) pair with no config-bound output is a violation
    // (indistinguishable on the wire from an unknown port — no oracle).
    assert_frame_is_violation(
        &base,
        &token,
        publish_frame("ghost", "nope", "hi", None),
        &flusher,
        &alerts,
    )
    .await;
}

// ===========================================================================
// Durable-channel projection integration (design §5 "Integration (native, real
// server)"): publish-while-detached → attach → drain in seq order, live
// delivery + mark-delivered idempotence, the per-delivery drain nudge that
// flushes quiet parked rows, `Resume::Durable` exact/beyond-window replay,
// resume across server restart, multi-session fan-out, mixed
// ephemeral+durable on one session, and ACL-floor retire parity.
//
// A real `WakeRouterImpl`-backed `Messenger` is wired into the spawned server's
// `AppState` (shared `surface_registry`), so driving the dispatcher
// (`dispatch_pending`) fans live rows out to the attached WS sessions exactly as
// the production background dispatcher would. Parked/retained rows are inserted
// straight into the DB (the publish-while-detached and retained-window sources);
// the delivery, drain, and resume machinery under test then reads them back.
// ===========================================================================

/// Bare durable channel name (ACL matcher key) + its canonical address.
const DURABLE_NAME: &str = "durable-demo";
const DURABLE_ADDR: &str = "brenn:durable-demo";

/// A durable `brenn:durable-demo` channel entry with the given retain depth.
fn durable_channel_entry(uuid: Uuid, retain_depth: Depth) -> ChannelEntry {
    ChannelEntry {
        uuid,
        address: DURABLE_ADDR.to_string(),
        description: None,
        resolved_channel: ResolvedChannel {
            push_depth: Depth::Unbounded,
            retain_depth,
            standing_retain_depth: retain_depth,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
        },
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// A `deskbar` surface with one durable subscription on `brenn:durable-demo`
/// (retain/wake as given) and, optionally, a second ephemeral subscription. When
/// `allow_delivery` the policy authorizes brenn delivery on the channel; when
/// false it does not, so the session-side delivery floor denies (retire parity).
fn durable_surface(
    uuid: Uuid,
    retain_depth: Depth,
    wake_min: WakeMin,
    allow_delivery: bool,
    extra_eph: Option<&str>,
) -> ResolvedSurface {
    let mut policy = AppPolicy::default();
    if allow_delivery {
        policy.grants.insert(AppCapability::MessagingSubscribe);
        policy.acls.brenn_subscribe = vec![ChannelMatcher::Exact(DURABLE_NAME.to_string())];
    }
    let mut subscriptions = vec![SurfaceBinding {
        channel_address: DURABLE_ADDR.to_string(),
        instance: COMPONENT.to_string(),
        port: PORT.to_string(),
        push_depth: 8,
        retain_depth: 0,
        noise: NoiseLevel::Silent,
    }];
    if let Some(eph) = extra_eph {
        policy.grants.insert(AppCapability::EphemeralSubscribe);
        policy.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(eph.to_string())];
        subscriptions.push(SurfaceBinding {
            channel_address: format!("ephemeral:{eph}"),
            instance: COMPONENT.to_string(),
            port: "ticker".to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: NoiseLevel::Silent,
        });
    }
    ResolvedSurface {
        slug: "deskbar".to_string(),
        skin: "bench".to_string(),
        components: vec![ResolvedComponent {
            instance: COMPONENT.to_string(),
            kind: COMPONENT.to_string(),
            abi: brenn_surface_proto::Abi::Dom,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: true,
        }],
        local_channels: vec![],
        subscriptions,
        durable_subscriptions: vec![ResolvedSurfaceSubscription {
            instance: COMPONENT.to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: DURABLE_ADDR.to_string(),
                push_depth: Depth::Unbounded,
                retain_depth,
                noise: NoiseLevel::Silent,
                wake_min,
            },
        }],
        outputs: vec![],
        policy,
        allowed_users: vec![],
        publish_burst: 60,
        publish_per_sec: 1,
    }
}

/// Wire a real `WakeRouterImpl`-backed `Messenger` into a fresh test `AppState`
/// whose `deskbar` runtime projects the durable channel, sharing the state's
/// `surface_registry` so live dispatch reaches attached WS sessions. Returns the
/// state (to spawn) and the messenger clone (to park/persist/dispatch from the
/// test). The channel row is upserted into `db` so message inserts satisfy the FK.
async fn durable_rig(
    db: &db::Db,
    resolved: ResolvedSurface,
    channel_entry: ChannelEntry,
    bus: Arc<EphemeralBus>,
) -> (AppState, Arc<Messenger>) {
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&channel_entry));
    }
    let router = Arc::new(WakeRouterImpl::new(ActiveBridges::new()));
    register_surface_routes(&router, std::slice::from_ref(&resolved));
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![channel_entry])),
        Arc::from(TEST_ORIGIN),
        Arc::new(indexmap::IndexMap::new()),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    let mut state = crate::test_support::state::test_state(db);
    state.messenger = Some(messenger.clone());
    state.surfaces = Arc::new(build_surface_runtimes(
        vec![resolved],
        bus,
        Some(messenger.clone()),
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    router.set_state(state.clone());
    (state, messenger)
}

/// A `deskbar` surface that both **subscribes to** and **publishes on**
/// `DURABLE_ADDR`: a durable subscription (`protobar`/`messages`) for receiving
/// plus a durable output (`writer`/`durable`) for publishing, its policy granting
/// both directions on the channel. Exercises the design §5 self-delivery case
/// end-to-end (publish → `resolve_push_targets` → persist → dispatch → deliver to
/// its own session).
fn durable_pubsub_surface(uuid: Uuid) -> ResolvedSurface {
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy.grants.insert(AppCapability::MessagingPublish);
    policy.acls.brenn_subscribe = vec![ChannelMatcher::Exact(DURABLE_NAME.to_string())];
    policy.acls.brenn_publish = vec![ChannelMatcher::Exact(DURABLE_NAME.to_string())];
    ResolvedSurface {
        slug: "deskbar".to_string(),
        skin: "bench".to_string(),
        local_channels: vec![],
        components: vec![
            ResolvedComponent {
                instance: COMPONENT.to_string(),
                kind: COMPONENT.to_string(),
                abi: brenn_surface_proto::Abi::Dom,
                send_budget: SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: true,
            },
            ResolvedComponent {
                instance: "writer".to_string(),
                // Distinct from the instance id for the same reason as
                // `deskbar_pub`'s: it makes the `surface:deskbar#writer`
                // self-delivery assertions prove the instance grain.
                kind: "writer-module".to_string(),
                abi: brenn_surface_proto::Abi::Dom,
                send_budget: SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: false,
            },
        ],
        subscriptions: vec![SurfaceBinding {
            channel_address: DURABLE_ADDR.to_string(),
            instance: COMPONENT.to_string(),
            port: PORT.to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: NoiseLevel::Silent,
        }],
        durable_subscriptions: vec![ResolvedSurfaceSubscription {
            instance: COMPONENT.to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: DURABLE_ADDR.to_string(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
        }],
        outputs: vec![SurfaceOutput {
            channel_address: DURABLE_ADDR.to_string(),
            instance: "writer".to_string(),
            port: "durable".to_string(),
            default_urgency: Urgency::Normal,
            budget: brenn_budget::SinkBudget {
                fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            },
        }],
        policy,
        allowed_users: vec![],
        publish_burst: 60,
        publish_per_sec: 1,
    }
}

/// Register a `SurfaceSessions` delivery route for every principal each resolved
/// surface declares (`ResolvedSurface::principals`), mirroring what boot wires.
///
/// Every instance is its own subscriber, and the router resolves a row's route
/// by the subscriber's registration key: an unregistered instance falls into the
/// no-route arm and its rows silently park, so a rig that registered only the
/// kernel grain would make every per-instance delivery test fail for the wrong
/// reason.
fn register_surface_routes(router: &Arc<WakeRouterImpl>, surfaces: &[ResolvedSurface]) {
    for s in surfaces {
        for instance in s.principals() {
            router.register_delivery_binding(
                brenn_lib::messaging::SubscriberEntryKind::Surface {
                    slug: s.slug.clone(),
                    instance,
                },
                crate::messaging_router::DeliveryBinding::SurfaceSessions,
            );
        }
    }
}

/// Subscriber registrations at every grain each surface declares, all carrying
/// that surface's policy — the shape boot installs (authority is per-surface;
/// the instance grain buys per-principal gating, not a separate ACL blob).
fn surface_registrations_all_grains(
    surface_policies: std::collections::HashMap<String, AppPolicy>,
    surfaces: &[ResolvedSurface],
) -> std::collections::HashMap<
    brenn_lib::messaging::SubscriberEntryKind,
    brenn_lib::messaging::SubscriberRegistration,
> {
    let mut out = brenn_lib::messaging::testutils::surface_registrations(surface_policies.clone());
    for s in surfaces {
        if let Some(policy) = surface_policies.get(&s.slug) {
            let instances: Vec<String> = s.instance_ids().collect();
            let instances: Vec<&str> = instances.iter().map(String::as_str).collect();
            out.extend(
                brenn_lib::messaging::testutils::surface_component_registrations(
                    &s.slug,
                    &instances,
                    policy.clone(),
                ),
            );
        }
    }
    out
}

/// Like `durable_rig`, but installs `surface_policies` on the `Messenger` (via
/// `with_surface_policies`). Each entry keys a surface slug to its policy: a
/// `brenn_publish` ACL lets that principal pass its publish gate
/// (`publish_from_surface`); a `brenn_subscribe` ACL lets it pass the
/// delivery-time gate in `resolve_push_targets` when it is a channel
/// subscriber. Self-delivery passes one slug holding both ACLs; a
/// cross-principal round trip passes a publisher slug and a distinct
/// subscriber slug. The channel entry must list the intended `Surface`
/// subscriber for the publish to fan out.
async fn durable_pubsub_rig(
    db: &db::Db,
    resolved: ResolvedSurface,
    channel_entry: ChannelEntry,
    surface_policies: std::collections::HashMap<String, AppPolicy>,
) -> (AppState, Arc<Messenger>) {
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&channel_entry));
    }
    let router = Arc::new(WakeRouterImpl::new(ActiveBridges::new()));
    let surfaces = vec![resolved];
    register_surface_routes(&router, &surfaces);
    // A publisher-only principal (a policy with no `ResolvedSurface`) never
    // subscribes, so it needs no route — but register its kernel grain anyway to
    // match boot, which registers every configured surface.
    for slug in surface_policies.keys() {
        if !surfaces.iter().any(|s| &s.slug == slug) {
            router.register_delivery_binding(
                brenn_lib::messaging::SubscriberEntryKind::Surface {
                    slug: slug.clone(),
                    instance: None,
                },
                crate::messaging_router::DeliveryBinding::SurfaceSessions,
            );
        }
    }
    // Budget every principal the Messenger knows a policy for, not just the ones
    // with a runtime here: this rig's cross-principal tests install a
    // publisher-only surface (a policy with no `ResolvedSurface`) to prove
    // fan-out reaches a subscriber that is not the sender. Principals come from
    // the resolved surfaces where one exists; a publisher-only principal
    // publishes under its kernel identity and needs only that grain.
    let budgets: Vec<(String, SurfacePrincipalBudgets)> = surface_policies
        .keys()
        .map(|slug| {
            let principals = surfaces.iter().find(|s| &s.slug == slug).map_or_else(
                || vec![(None, SurfaceSendBudget::default())],
                |s| s.principal_send_budgets().collect(),
            );
            (slug.clone(), principals)
        })
        .collect();
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(vec![channel_entry])),
        Arc::from(TEST_ORIGIN),
        Arc::new(indexmap::IndexMap::new()),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(surface_registrations_all_grains(
        surface_policies,
        &surfaces,
    ))
    .with_surface_send_budgets(budgets);
    let mut state = crate::test_support::state::test_state(db);
    state.messenger = Some(messenger.clone());
    state.surfaces = Arc::new(build_surface_runtimes(
        surfaces,
        fixture_bus(vec![]),
        Some(messenger.clone()),
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    router.set_state(state.clone());
    (state, messenger)
}

/// End-to-end: a surface durable publish reaches a durable subscriber. `deskbar`
/// subscribes to `brenn:durable-demo`, then publishes through its durable output;
/// `publish_from_surface` persists a push row targeting the channel's `deskbar`
/// Surface subscriber, a dispatcher pass runs, and the same session receives the
/// durable `Deliver` — proving publish and S8 projection compose (the §5
/// self-delivery case).
#[tokio::test]
async fn surface_ws_durable_publish_delivers_to_subscriber() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut channel_entry = durable_channel_entry(uuid, Depth::Unbounded);
    channel_entry.subscribers = vec![SubscriberEntry {
        kind: SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some(COMPONENT.to_string()),
        },
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    }];
    let resolved = durable_pubsub_surface(uuid);
    let surface_policies =
        std::collections::HashMap::from([("deskbar".to_string(), resolved.policy.clone())]);
    let (state, messenger) =
        durable_pubsub_rig(&db, resolved, channel_entry, surface_policies).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;
    // Subscribe so live dispatch reaches this session; no backlog yet.
    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("send Subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        0
    );

    // Publish durably through the surface's own output port.
    ws.send(publish_frame("writer", "durable", "hello-durable", Some(7)))
        .await
        .expect("send durable Publish");
    assert!(
        matches!(
            publish_result_outcome(next_server_frame(&mut ws).await, Some(7)),
            PublishOutcome::Ok
        ),
        "durable publish is Ok"
    );

    // A dispatcher pass delivers the parked row live to the subscribed session.
    dispatch_pending(&messenger).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "hello-durable", 1).await;
}

/// The durable depth-0 context feed (design §6): a fold-0 durable subscription
/// creates **no** `messaging_pending_pushes` row, yet an attached session still
/// receives the message live, as a row-less deliver-if-attached fan-out at
/// publish time. The message persists and is retained; a later dispatcher pass
/// finds no row and delivers nothing again (no duplicate).
///
/// The zero-row assertion is load-bearing and not incidental:
/// `bus_gc_retire_pushes` early-returns at depth 0, so a row created here would
/// never be reaped and the table would grow without bound behind a disconnected
/// surface. The feed owes a disconnected session nothing — its context arrives
/// at the next subscribe/resume (the paired test below).
#[tokio::test]
async fn surface_ws_durable_context_feed_delivers_live_with_no_push_row() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut channel_entry = durable_channel_entry(uuid, Depth::Bounded(4));
    channel_entry.subscribers = vec![SubscriberEntry {
        kind: SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some(COMPONENT.to_string()),
        },
        push_depth: Depth::Bounded(0),
        retain_depth: Depth::Bounded(4),
        noise: NoiseLevel::Silent,
        wake_min: None,
    }];
    let mut resolved = durable_pubsub_surface(uuid);
    // The binding matches the subscriber: a context feed at both grains.
    resolved.subscriptions[0].push_depth = 0;
    resolved.subscriptions[0].retain_depth = 4;
    resolved.durable_subscriptions[0].subscription.push_depth = Depth::Bounded(0);
    let surface_policies =
        std::collections::HashMap::from([("deskbar".to_string(), resolved.policy.clone())]);
    let (state, messenger) =
        durable_pubsub_rig(&db, resolved, channel_entry, surface_policies).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;
    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("send Subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        0,
        "a fresh attach replays parked rows only, and a context feed has none"
    );

    ws.send(publish_frame("writer", "durable", "hello-durable", Some(7)))
        .await
        .expect("send durable Publish");
    assert!(
        matches!(
            publish_result_outcome(next_server_frame(&mut ws).await, Some(7)),
            PublishOutcome::Ok
        ),
        "the publish itself is unaffected — the message persists"
    );

    // The fold-0 subscription receives the message live, with no push row: the
    // row-less context feed fanned it in at publish time.
    assert_durable_deliver_to(&mut ws, COMPONENT, "hello-durable", 1).await;

    // A dispatcher pass finds no push row and delivers nothing again — the feed
    // is not backed by a claimable row, so there is nothing to duplicate.
    dispatch_pending(&messenger).await;
    assert_no_deliver(&mut ws).await;

    let conn = messenger.db().lock().await;
    let pushes: i64 = conn
        .query_row("SELECT COUNT(*) FROM messaging_pending_pushes", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        pushes, 0,
        "a depth-0 subscriber is not a push target — the feed creates no row"
    );
    let messages: i64 = conn
        .query_row("SELECT COUNT(*) FROM messaging_messages", [], |r| r.get(0))
        .unwrap();
    assert_eq!(messages, 1, "the message itself persisted and is retained");
}

/// The durable depth-0 context feed on the **batch path** (design §6): an
/// activation *flush* (a `PublishBatch` frame), not the ad-hoc single `Publish`,
/// live-feeds an attached fold-0 durable subscriber with no push row. The
/// single-publish path is covered above; this pins the batch-specific
/// accumulation-and-fan-out glue in `publish_batch_from_surface`, which is
/// wired separately from the ad-hoc path.
#[tokio::test]
async fn surface_ws_durable_context_feed_delivers_live_on_a_batch_flush() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut channel_entry = durable_channel_entry(uuid, Depth::Bounded(4));
    channel_entry.subscribers = vec![SubscriberEntry {
        kind: SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some(COMPONENT.to_string()),
        },
        push_depth: Depth::Bounded(0),
        retain_depth: Depth::Bounded(4),
        noise: NoiseLevel::Silent,
        wake_min: None,
    }];
    let mut resolved = durable_pubsub_surface(uuid);
    resolved.subscriptions[0].push_depth = 0;
    resolved.subscriptions[0].retain_depth = 4;
    resolved.durable_subscriptions[0].subscription.push_depth = Depth::Bounded(0);
    let surface_policies =
        std::collections::HashMap::from([("deskbar".to_string(), resolved.policy.clone())]);
    let (state, messenger) =
        durable_pubsub_rig(&db, resolved, channel_entry, surface_policies).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;
    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("send Subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        0,
        "a fresh attach replays parked rows only, and a context feed has none"
    );

    // The flush: one durable entry through the batch path, answered on its
    // correlation. The batch result and the row-less feed race on the wire, so
    // read both, matching either order.
    ws.send(publish_batch_frame(
        "writer",
        9,
        &[("durable", "hello-batch")],
    ))
    .await
    .expect("send PublishBatch");
    let mut saw_result = false;
    let mut saw_deliver = false;
    for _ in 0..2 {
        match next_server_frame(&mut ws).await {
            ServerFrame::PublishBatchResult {
                correlation,
                outcome,
            } => {
                assert_eq!(correlation, 9, "the batch result echoes its correlation");
                assert_eq!(outcome, PublishBatchOutcome::Ok);
                saw_result = true;
            }
            ServerFrame::Deliver {
                channel,
                envelope,
                targets,
            } => {
                assert_eq!(channel, DURABLE_ADDR);
                assert_eq!(envelope.body, "hello-batch");
                let target = sole_target(&targets);
                assert_eq!(target.instance, COMPONENT);
                assert_eq!(target.dropped, 0);
                assert!(
                    matches!(
                        cursor::parse(&target.cursor),
                        Ok(CursorState::Durable { high_water: 1, .. })
                    ),
                    "got {:?}",
                    target.cursor
                );
                saw_deliver = true;
            }
            other => panic!("expected batch result or Deliver, got {other:?}"),
        }
    }
    assert!(
        saw_result && saw_deliver,
        "both the Ok and the live feed arrive"
    );

    // No push row, and no duplicate on a later dispatcher pass.
    dispatch_pending(&messenger).await;
    assert_no_deliver(&mut ws).await;
    let conn = messenger.db().lock().await;
    let pushes: i64 = conn
        .query_row("SELECT COUNT(*) FROM messaging_pending_pushes", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(pushes, 0, "a fold-0 flush is not a push target — no row");
}

/// …and the retained window is how its context arrives: the resume-time replay is
/// not gated on `push_depth`, so a durable context feed still serves its window,
/// clamped to `retain_depth` like any other durable subscription.
///
/// The pair with the test above is the whole durable depth-0 story: a live
/// row-less feed while attached, and the retained window on resume for whatever
/// a disconnected session missed.
#[tokio::test]
async fn surface_ws_durable_context_feed_still_replays_the_retained_window_on_resume() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut resolved = durable_surface(uuid, Depth::Bounded(2), WakeMin::Normal, true, None);
    resolved.subscriptions[0].push_depth = 0;
    resolved.subscriptions[0].retain_depth = 2;
    resolved.durable_subscriptions[0].subscription.push_depth = Depth::Bounded(0);
    let (state, messenger) = durable_rig(
        &db,
        resolved,
        durable_channel_entry(uuid, Depth::Bounded(2)),
        fixture_bus(vec![]),
    )
    .await;
    let s1 = persist_durable(&messenger, uuid, "r1").await;
    let _s2 = persist_durable(&messenger, uuid, "r2").await;
    let s3 = persist_durable(&messenger, uuid, "r3").await;
    let s4 = persist_durable(&messenger, uuid, "r4").await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, s1).await),
    ))
    .await
    .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 2, "the clamp is retain_depth, not push_depth");
    assert_eq!(gap, Some(GapReason::BeyondRetained));
    assert_durable_deliver_to(&mut ws, COMPONENT, "r3", s3).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "r4", s4).await;
}

/// End-to-end **sibling-instance** durable fan-out: two instances of one kind on
/// one surface, both bound to one channel, are two principals — two
/// subscriptions, two push windows, two `Deliver`s, each naming its own
/// instance. The twelve-agendas case, at two.
///
/// This is the property the whole per-instance keying exists for, asserted where
/// nothing can fake it: a page-grained subscription would deliver the row once
/// (or twice under one identity), and a channel-keyed fan-out would put both
/// copies on both instances' ports. The `Deliver.instance` assertions are what
/// make the two copies distinguishable at all.
#[tokio::test]
async fn surface_ws_durable_sibling_instances_each_get_their_own_subscription() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut channel_entry = durable_channel_entry(uuid, Depth::Unbounded);
    // Two subscriber entries on one channel — one per principal, exactly as two
    // `[[app]]` blocks on one channel would produce.
    channel_entry.subscribers = ["agenda-alice", "agenda-bob"]
        .into_iter()
        .map(|instance| SubscriberEntry {
            kind: SubscriberEntryKind::Surface {
                slug: "deskbar".to_string(),
                instance: Some(instance.to_string()),
            },
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        })
        .collect();

    let mut resolved = durable_pubsub_surface(uuid);
    resolved.components.extend(
        ["agenda-alice", "agenda-bob"].map(|instance| ResolvedComponent {
            instance: instance.to_string(),
            kind: "agenda".to_string(),
            abi: brenn_surface_proto::Abi::Dom,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: false,
        }),
    );
    resolved.subscriptions = ["agenda-alice", "agenda-bob"]
        .into_iter()
        .map(|instance| SurfaceBinding {
            channel_address: DURABLE_ADDR.to_string(),
            instance: instance.to_string(),
            port: PORT.to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: NoiseLevel::Silent,
        })
        .collect();
    resolved.durable_subscriptions = ["agenda-alice", "agenda-bob"]
        .into_iter()
        .map(|instance| ResolvedSurfaceSubscription {
            instance: instance.to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: DURABLE_ADDR.to_string(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
        })
        .collect();

    let surface_policies =
        std::collections::HashMap::from([("deskbar".to_string(), resolved.policy.clone())]);
    let (state, messenger) =
        durable_pubsub_rig(&db, resolved, channel_entry, surface_policies).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;
    // Both instances subscribe the same channel on the one session. Under a
    // page-grained model the second of these would be a duplicate-Subscribe
    // violation and kill the connection.
    for instance in ["agenda-alice", "agenda-bob"] {
        ws.send(subscribe_frame_as(DURABLE_ADDR, instance, None))
            .await
            .expect("send Subscribe");
        assert_eq!(
            next_subscribe_result(&mut ws, DURABLE_ADDR, instance)
                .await
                .0,
            0,
            "{instance}'s subscription is answered under its own name"
        );
    }

    ws.send(publish_frame("writer", "durable", "hello-both", Some(7)))
        .await
        .expect("send durable Publish");
    assert!(
        matches!(
            publish_result_outcome(next_server_frame(&mut ws).await, Some(7)),
            PublishOutcome::Ok
        ),
        "durable publish is Ok"
    );

    // One publish, two push rows, two deliveries — one per principal, from its
    // own window, coalesced into one frame at the write boundary. Target order
    // within the frame is unspecified, so collect and sort.
    dispatch_pending(&messenger).await;
    let mut got: Vec<String> = Vec::new();
    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            channel,
            envelope,
            targets,
        } => {
            assert_eq!(channel, DURABLE_ADDR);
            assert_eq!(envelope.body, "hello-both");
            assert_eq!(
                targets.len(),
                2,
                "the row's sibling deliveries coalesce into one frame: {targets:?}"
            );
            for target in targets {
                got.push(target.instance.clone());
            }
        }
        other => panic!("expected Deliver, got {other:?}"),
    }
    got.sort();
    assert_eq!(
        got,
        vec!["agenda-alice".to_string(), "agenda-bob".to_string()],
        "each instance is delivered under its own name — one publish, two \
         independent subscriptions, two windows"
    );
}

/// The router's fan-out filter keys on the whole subscription, and that is
/// load-bearing rather than defense-in-depth.
///
/// The filter (`h.is_subscribed(&sub)`) runs *before* `claim_pending_pushes`
/// stamps `delivered_at`, and a fan-out that accepts on any session returns
/// `Ok(true)` — the row does not re-park. So the session's own `is_active` drop
/// does not neutralise a misroute; it **consumes** the row. Under a
/// channel-keyed filter: alice is subscribed nowhere, bob is active on session
/// S, alice's row matches S on the channel, is claimed, is sent to S, and S
/// discards it — alice's row marked delivered and never seen by anyone.
///
/// Constructible on one session precisely because the mutation's damage is to
/// the *unsubscribed* principal's row, not to what the subscribed one receives.
#[tokio::test]
async fn surface_ws_a_row_for_an_unsubscribed_instance_parks_rather_than_being_consumed() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut channel_entry = durable_channel_entry(uuid, Depth::Unbounded);
    // Both principals are registered subscribers, so one publish resolves two
    // push rows — push rows exist per registered principal regardless of what
    // the page has attached.
    channel_entry.subscribers = ["agenda-alice", "agenda-bob"]
        .into_iter()
        .map(|instance| SubscriberEntry {
            kind: SubscriberEntryKind::Surface {
                slug: "deskbar".to_string(),
                instance: Some(instance.to_string()),
            },
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            wake_min: None,
        })
        .collect();

    let mut resolved = durable_pubsub_surface(uuid);
    resolved.components.extend(
        ["agenda-alice", "agenda-bob"].map(|instance| ResolvedComponent {
            instance: instance.to_string(),
            kind: "agenda".to_string(),
            abi: brenn_surface_proto::Abi::Dom,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: false,
        }),
    );
    resolved.subscriptions = ["agenda-alice", "agenda-bob"]
        .into_iter()
        .map(|instance| SurfaceBinding {
            channel_address: DURABLE_ADDR.to_string(),
            instance: instance.to_string(),
            port: PORT.to_string(),
            push_depth: 8,
            retain_depth: 0,
            noise: NoiseLevel::Silent,
        })
        .collect();
    resolved.durable_subscriptions = ["agenda-alice", "agenda-bob"]
        .into_iter()
        .map(|instance| ResolvedSurfaceSubscription {
            instance: instance.to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: DURABLE_ADDR.to_string(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
        })
        .collect();

    let surface_policies =
        std::collections::HashMap::from([("deskbar".to_string(), resolved.policy.clone())]);
    let (state, messenger) =
        durable_pubsub_rig(&db, resolved, channel_entry, surface_policies).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;
    // Only bob activates his subscription. Alice's binding is declared and her
    // push rows resolve, but no session holds her subscription.
    ws.send(subscribe_frame_as(DURABLE_ADDR, "agenda-bob", None))
        .await
        .expect("send Subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, "agenda-bob")
            .await
            .0,
        0,
        "bob's subscription is answered under his own name"
    );

    ws.send(publish_frame(
        "writer",
        "durable",
        "only-bob-is-here",
        Some(7),
    ))
    .await
    .expect("send durable Publish");
    assert!(
        matches!(
            publish_result_outcome(next_server_frame(&mut ws).await, Some(7)),
            PublishOutcome::Ok
        ),
        "durable publish is Ok"
    );

    dispatch_pending(&messenger).await;

    // (a) Exactly one Deliver, naming bob. A channel-keyed filter would send
    //     alice's row here too, where the session drops it silently.
    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            channel,
            envelope,
            targets,
        } => {
            assert_eq!(channel, DURABLE_ADDR);
            assert_eq!(envelope.body, "only-bob-is-here");
            let target = sole_target(&targets);
            assert_eq!(
                target.instance, "agenda-bob",
                "the only delivery belongs to the subscribed instance"
            );
        }
        other => panic!("expected Deliver, got {other:?}"),
    }
    assert_no_deliver(&mut ws).await;

    // (b) Alice's row is still pending. This is the assertion the mutation
    //     inverts: channel-keyed, her row is claimed and marked delivered by a
    //     session that then discards it — silent per-instance loss, and she can
    //     never be sent it again.
    let pending_for = |key: &str| {
        let key = key.to_string();
        let messenger = messenger.clone();
        async move {
            let conn = messenger.db().lock().await;
            conn.query_row(
                "SELECT COUNT(*) FROM messaging_pending_pushes
                 WHERE target_app_slug = ?1 AND delivered_at IS NULL",
                rusqlite::params![key],
                |row| row.get::<_, i64>(0),
            )
            .expect("count pending pushes")
        }
    };
    assert_eq!(
        pending_for("deskbar#agenda-alice").await,
        1,
        "the unsubscribed instance's row stays parked for a later Subscribe"
    );
    assert_eq!(
        pending_for("deskbar#agenda-bob").await,
        0,
        "the subscribed instance's row was claimed and delivered"
    );
}

/// End-to-end **cross-principal** durable round trip — design §4's named
/// "surface→surface durable round trip live" test. One surface principal
/// (`wallbar`) publishes durably; a *different* subscribed surface principal
/// (`deskbar`) receives the live `Deliver`. This is the case
/// `surface_ws_durable_publish_delivers_to_subscriber` does not cover: that
/// test is a single self-subscribing principal (sender == subscriber), so it
/// cannot rule out a bug specific to *cross-principal* fan-out. Here the
/// publisher is not on the channel's subscriber list at all, proving
/// `resolve_push_targets` routes a surface-published row to a subscriber that
/// is not the sender.
#[tokio::test]
async fn surface_ws_durable_publish_delivers_cross_principal() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut channel_entry = durable_channel_entry(uuid, Depth::Unbounded);
    // Only `deskbar` subscribes; `wallbar` (the publisher) is not a subscriber.
    channel_entry.subscribers = vec![SubscriberEntry {
        kind: SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some(COMPONENT.to_string()),
        },
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    }];
    // Subscriber principal: deskbar, durable-subscribed with brenn delivery ACL.
    let subscriber = durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None);
    let subscriber_policy = subscriber.policy.clone();
    // Publisher principal: wallbar, a *distinct* slug granted brenn publish on
    // the same channel. It has no runtime and no subscription — it only holds a
    // surface publish policy on the Messenger. deskbar's policy is installed too
    // so it clears the delivery-time ACL gate in `resolve_push_targets`.
    let mut publisher_policy = AppPolicy::default();
    publisher_policy
        .grants
        .insert(AppCapability::MessagingPublish);
    publisher_policy.acls.brenn_publish = vec![ChannelMatcher::Exact(DURABLE_NAME.to_string())];
    let surface_policies = std::collections::HashMap::from([
        ("deskbar".to_string(), subscriber_policy),
        ("wallbar".to_string(), publisher_policy),
    ]);
    let (state, messenger) =
        durable_pubsub_rig(&db, subscriber, channel_entry, surface_policies).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;
    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("send Subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        0
    );

    // wallbar publishes durably; the row targets deskbar (a different principal).
    let outcome = messenger
        .publish_from_surface(
            "wallbar",
            None,
            DURABLE_ADDR,
            "cross-principal-hello",
            Urgency::Normal,
        )
        .await;
    assert!(
        matches!(outcome, PublishResult::Ok { .. }),
        "cross-principal durable publish is Ok, got {outcome:?}"
    );

    // A dispatcher pass delivers the parked row live to deskbar's session.
    dispatch_pending(&messenger).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "cross-principal-hello", 1).await;
}

/// End-to-end **cold-start drain** — design §4's named "surface durable publish →
/// parked → drained by a second (detached-at-publish) surface session" test. A
/// `deskbar` connection publishes durably over its WS output *while no session is
/// subscribed* to the channel (the eventual subscriber does not yet exist), then
/// that publisher connection drops; a fresh `deskbar` session opens later and, on
/// `Subscribe`, drains the parked row as `SubscribeResult` replay. This is the
/// one composition the self/cross-principal live tests do not reach: they publish
/// through `handle_publish`'s durable arm *with a subscriber already attached*, so
/// they cannot catch a bug specific to a WS-driven durable publish whose row is
/// loaded by a *later* Surface subscriber that was offline at publish time (the
/// "publish while my other device is offline, it catches up later" story).
#[tokio::test]
async fn surface_ws_durable_publish_parks_then_drains_on_late_subscribe() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut channel_entry = durable_channel_entry(uuid, Depth::Unbounded);
    channel_entry.subscribers = vec![SubscriberEntry {
        kind: SubscriberEntryKind::Surface {
            slug: "deskbar".to_string(),
            instance: Some(COMPONENT.to_string()),
        },
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    }];
    let resolved = durable_pubsub_surface(uuid);
    let surface_policies =
        std::collections::HashMap::from([("deskbar".to_string(), resolved.policy.clone())]);
    let (state, _messenger) =
        durable_pubsub_rig(&db, resolved, channel_entry, surface_policies).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // Publisher connection: publish durably through the WS output port. It is not
    // subscribed to the channel, so the row parks targeting `surface:deskbar`.
    let mut publisher = open_deskbar(&base, &token).await;
    publisher
        .send(publish_frame("writer", "durable", "offline-hello", Some(9)))
        .await
        .expect("send durable Publish");
    assert!(
        matches!(
            publish_result_outcome(next_server_frame(&mut publisher).await, Some(9)),
            PublishOutcome::Ok
        ),
        "durable publish is Ok"
    );
    // The subscriber session is detached at publish time: drop the publisher and
    // never dispatch, so the row can only reach the wire via subscribe-time drain.
    drop(publisher);

    // A fresh session subscribes and drains the parked row as replay backlog.
    let mut subscriber = open_deskbar(&base, &token).await;
    subscriber
        .send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("send Subscribe");
    let (replay, gap) = next_subscribe_result(&mut subscriber, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "the one parked row replays on late subscribe");
    assert_eq!(gap, None, "fresh subscribe gaps nothing");
    assert_durable_deliver_to(&mut subscriber, COMPONENT, "offline-hello", 1).await;
}

/// Insert a parked (undelivered) push row targeting `surface:deskbar` on the
/// durable channel — a publish-while-detached. `eager` sets the `eager_wake` flag
/// (a non-eager row is not dispatchable and only drains on a nudge/subscribe).
/// Returns `(push_id, seq = message_id)`.
async fn park_durable(messenger: &Messenger, uuid: Uuid, body: &str, eager: bool) -> (i64, i64) {
    park_durable_at(messenger, uuid, body, eager, None).await
}

/// Insert a parked push row held until `release_after` — a delayed-release row.
/// Held rows are excluded from both the parked claim and the dispatchable set
/// (`release_after IS NULL` on each), so the row reaches the wire only once
/// [`release_due_pushes`] clears the hold. Returns `(push_id, seq = message_id)`.
async fn park_durable_delayed(
    messenger: &Messenger,
    uuid: Uuid,
    body: &str,
    release_after: chrono::DateTime<Utc>,
) -> (i64, i64) {
    park_durable_at(messenger, uuid, body, true, Some(release_after)).await
}

async fn park_durable_at(
    messenger: &Messenger,
    uuid: Uuid,
    body: &str,
    eager: bool,
    release_after: Option<chrono::DateTime<Utc>>,
) -> (i64, i64) {
    let conn = messenger.db().lock().await;
    // Targeted at the subscribing *instance*, mirroring what
    // `resolve_push_targets` stamps for these fixtures' bindings: the push window
    // belongs to the principal, so a row seeded at the bare surface grain would
    // be nobody's and claim-drain nothing.
    let subscriber = ParticipantId::for_surface_component("deskbar", COMPONENT);
    let push = PendingPushInsert {
        target_app_slug: subscriber.as_surface_subscriber_key().to_string(),
        target_subscriber: subscriber,
        eager_wake: eager,
        release_after,
        delivery_deadline: None,
    };
    let msg = insert_message_with_pushes(
        &conn,
        uuid,
        "host",
        "sender",
        body,
        Urgency::Normal,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        utc_to_ns(Utc::now()),
        &[push],
    );
    (msg.push_ids[0], msg.id)
}

/// Insert a retained-only message (no push row): present in the retained window
/// for a `Resume::Durable` re-send but never parked. Returns `seq = message_id`.
async fn persist_durable(messenger: &Messenger, uuid: Uuid, body: &str) -> i64 {
    let conn = messenger.db().lock().await;
    insert_message_with_pushes(
        &conn,
        uuid,
        "host",
        "sender",
        body,
        Urgency::Normal,
        ChannelScheme::Brenn,
        None,
        None,
        None,
        utc_to_ns(Utc::now()),
        &[],
    )
    .id
}

/// Drive one dispatcher pass over every currently-dispatchable row — the live
/// delivery trigger the production background dispatcher fires. Uses the same
/// `dispatch_row` entry point, so the real `WakeRouterImpl` Surface arm runs.
async fn dispatch_pending(messenger: &Messenger) {
    let rows = {
        let conn = messenger.db().lock().await;
        load_all_dispatchable_pushes(&conn, Utc::now())
    };
    for (row, expired) in &rows {
        dispatcher::dispatch_row(messenger.router().as_ref(), row, *expired, false).await;
    }
}

/// Open a `deskbar` WS session and consume its `Welcome`.
async fn open_deskbar(base: &str, token: &str) -> SurfaceWs {
    let url = http_to_ws_url(base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&url, token).await;
    consume_welcome(&mut ws).await;
    ws
}

/// Read the next `SubscribeResult` for `instance`'s subscription on `channel`,
/// asserting `Ok`, and return `(replay_count, gap_reason)`.
///
/// The instance is asserted, not ignored: the answer to a `Subscribe` must name
/// the principal that asked, or a page with sibling instances on one channel
/// cannot tell whose subscription it settles.
async fn next_subscribe_result(
    ws: &mut SurfaceWs,
    channel: &str,
    instance: &str,
) -> (u32, Option<GapReason>) {
    match next_server_frame(ws).await {
        ServerFrame::SubscribeResult {
            channel: got,
            instance: got_instance,
            outcome,
            replay_count,
            gap,
        } => {
            assert_eq!(got, channel);
            assert_eq!(got_instance, instance, "SubscribeResult instance");
            assert!(
                matches!(outcome, SubscribeOutcome::Ok),
                "outcome {outcome:?}"
            );
            (replay_count, gap.map(|g| g.reason))
        }
        other => panic!("expected SubscribeResult, got {other:?}"),
    }
}

/// Assert the next server frame is a durable `Deliver` to `instance` carrying
/// `body` at `seq`.
async fn assert_durable_deliver_to(ws: &mut SurfaceWs, instance: &str, body: &str, seq: i64) {
    match next_server_frame(ws).await {
        ServerFrame::Deliver {
            channel,
            envelope,
            targets,
        } => {
            assert_eq!(channel, DURABLE_ADDR);
            assert_eq!(envelope.body, body);
            let target = sole_target(&targets);
            assert_eq!(target.instance, instance, "delivery instance");
            assert_eq!(
                target.dropped, 0,
                "durable deliveries carry no drop signal in v1"
            );
            match cursor::parse(&target.cursor) {
                Ok(CursorState::Durable {
                    high_water: got, ..
                }) => {
                    assert_eq!(got, seq, "durable cursor high-water")
                }
                other => panic!("expected durable cursor, got {other:?}"),
            }
        }
        other => panic!("expected durable Deliver, got {other:?}"),
    }
}

/// Read the next `Deliver` frame, returning its `(body, parsed cursor state)`.
async fn next_deliver(ws: &mut SurfaceWs) -> (String, CursorState) {
    match next_server_frame(ws).await {
        ServerFrame::Deliver {
            envelope, targets, ..
        } => {
            let target = sole_target(&targets);
            (
                envelope.body.clone(),
                cursor::parse(&target.cursor).expect("a server-minted cursor parses"),
            )
        }
        other => panic!("expected Deliver, got {other:?}"),
    }
}

/// Assert no `Deliver` frame arrives within a short window (idle `Heartbeat`s and
/// keep-alive pings are allowed through). Used to prove at-most-once — that a
/// claimed/duplicate row never reaches the wire twice.
async fn assert_no_deliver(ws: &mut SurfaceWs) {
    let deadline = Instant::now() + Duration::from_millis(500);
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let frame: ServerFrame =
                    serde_json::from_str(t.as_str()).expect("server frame parses");
                assert!(
                    matches!(frame, ServerFrame::Heartbeat),
                    "expected silence but got {frame:?}"
                );
            }
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            Ok(Some(Ok(other))) => panic!("unexpected ws message: {other:?}"),
            Ok(Some(Err(e))) => panic!("ws error while expecting silence: {e}"),
            Ok(None) => panic!("ws closed while expecting silence"),
            Err(_) => break,
        }
    }
}

/// Publish one ephemeral message onto `addr` as a distinct sender.
fn publish_eph(bus: &EphemeralBus, name: &str, addr: &str, body: &str) {
    let participant = ParticipantId::for_surface("eph-pub");
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::EphemeralPublish);
    policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact(name.to_string())];
    assert!(
        matches!(
            bus.publish(&participant, &policy, addr, body, Urgency::Normal),
            EphemeralPublishResult::Ok { .. }
        ),
        "ephemeral fixture publish must succeed"
    );
}

/// Publish-while-detached: three rows park; on attach + `Subscribe` they drain in
/// seq order (each as `Pos::Durable`) inside the `SubscribeResult` replay.
#[tokio::test]
async fn surface_ws_durable_parked_rows_drain_in_seq_order_on_subscribe() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;
    let (_p1, s1) = park_durable(&messenger, uuid, "one", true).await;
    let (_p2, s2) = park_durable(&messenger, uuid, "two", true).await;
    let (_p3, s3) = park_durable(&messenger, uuid, "three", true).await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 3, "all three parked rows replay");
    assert_eq!(gap, None, "fresh subscribe gaps nothing");
    assert_durable_deliver_to(&mut ws, COMPONENT, "one", s1).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "two", s2).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "three", s3).await;
}

/// Live delivery after subscribe: a dispatched row reaches the attached session
/// as a `Pos::Durable` `Deliver`; a second dispatch pass finds the row claimed
/// and the drain nudge finds nothing, so no duplicate reaches the wire.
#[tokio::test]
async fn surface_ws_durable_live_delivery_after_subscribe_no_duplicate() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe");
    let (replay, _) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 0, "nothing parked before subscribe");

    let (_p, seq) = park_durable(&messenger, uuid, "live", true).await;
    dispatch_pending(&messenger).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "live", seq).await;

    // Idempotence: re-dispatch cannot re-deliver the claimed row.
    dispatch_pending(&messenger).await;
    assert_no_deliver(&mut ws).await;
}

/// A quiet parked row (non-eager, so never dispatchable and never eager-woken)
/// stays put until a louder live delivery fires the per-delivery drain nudge
/// (SD5 step 6), which flushes it. Both rows reach the wire (order between the
/// live loud row and the drained quiet row is the accepted SD5 inversion).
#[tokio::test]
async fn surface_ws_durable_quiet_row_flushed_by_live_delivery_nudge() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        0
    );

    // Park a quiet row after subscribe: nothing nudges the session, so it waits.
    park_durable(&messenger, uuid, "quiet", false).await;
    assert_no_deliver(&mut ws).await;

    // A louder eager row is dispatchable → live-delivered AND nudges the drain,
    // which flushes the quiet backlog.
    park_durable(&messenger, uuid, "loud", true).await;
    dispatch_pending(&messenger).await;
    let mut bodies = std::collections::HashSet::new();
    bodies.insert(next_deliver(&mut ws).await.0);
    bodies.insert(next_deliver(&mut ws).await.0);
    assert!(
        bodies.contains("loud") && bodies.contains("quiet"),
        "both the live loud row and the nudged quiet row must arrive, got {bodies:?}"
    );
}

/// `Resume::Durable` exact continuation: with the retained window covering, a
/// resume from `last_seq` re-sends exactly the retained messages with a greater
/// id and signals no gap.
#[tokio::test]
async fn surface_ws_durable_resume_exact_continuation() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Bounded(10), WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Bounded(10)),
        fixture_bus(vec![]),
    )
    .await;
    let s1 = persist_durable(&messenger, uuid, "r1").await;
    let s2 = persist_durable(&messenger, uuid, "r2").await;
    let s3 = persist_durable(&messenger, uuid, "r3").await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, s1).await),
    ))
    .await
    .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 2,
        "ids above last_seq re-send from the retained window"
    );
    assert_eq!(gap, None, "window covers → no gap");
    assert_durable_deliver_to(&mut ws, COMPONENT, "r2", s2).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "r3", s3).await;
}

/// Fresh attach (no resume token) on a durable channel: the server replays the
/// channel's most recent rows clamped to `retain_depth`, with no gap — the
/// retained-window parity with the ephemeral fresh arm. A resume-less durable
/// subscribe is no longer answered empty.
#[tokio::test]
async fn surface_ws_durable_fresh_attach_replays_retained_window() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Bounded(2), WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Bounded(2)),
        fixture_bus(vec![]),
    )
    .await;
    let _s1 = persist_durable(&messenger, uuid, "r1").await;
    let _s2 = persist_durable(&messenger, uuid, "r2").await;
    let s3 = persist_durable(&messenger, uuid, "r3").await;
    let s4 = persist_durable(&messenger, uuid, "r4").await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 2,
        "fresh attach replays the retained window, clamped to retain_depth"
    );
    assert_eq!(gap, None, "fresh is fresh — nothing was missed, so no gap");
    assert_durable_deliver_to(&mut ws, COMPONENT, "r3", s3).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "r4", s4).await;
}

/// `Resume::Durable` beyond the retained window: a resume older than the clamp
/// can serve truncates to the newest window and signals `BeyondRetained`.
#[tokio::test]
async fn surface_ws_durable_resume_beyond_window_gaps_and_replays_newest() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Bounded(2), WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Bounded(2)),
        fixture_bus(vec![]),
    )
    .await;
    let s1 = persist_durable(&messenger, uuid, "r1").await;
    let _s2 = persist_durable(&messenger, uuid, "r2").await;
    let s3 = persist_durable(&messenger, uuid, "r3").await;
    let s4 = persist_durable(&messenger, uuid, "r4").await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, s1).await),
    ))
    .await
    .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 2, "clamp Bounded(2) serves only the newest two");
    assert_eq!(
        gap,
        Some(GapReason::BeyondRetained),
        "a clamp-truncated window signals BeyondRetained"
    );
    assert_durable_deliver_to(&mut ws, COMPONENT, "r3", s3).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "r4", s4).await;
}

/// Durable state is SQLite, so a resume works across a full server restart: a
/// second server over the same DB replays the retained window from the first
/// server's persisted messages.
#[tokio::test]
async fn surface_ws_durable_resume_survives_server_restart() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (token, _) = setup_authenticated_user(&db).await;

    // Server 1: attach, drain two parked rows, capture the last seq.
    let (state1, messenger1) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Bounded(10), WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Bounded(10)),
        fixture_bus(vec![]),
    )
    .await;
    let (_p1, s1) = park_durable(&messenger1, uuid, "a", true).await;
    let (_p2, s2) = park_durable(&messenger1, uuid, "b", true).await;
    let (base1, sd1) = spawn_test_server(state1).await;
    {
        let mut ws = open_deskbar(&base1, &token).await;
        ws.send(subscribe_frame(DURABLE_ADDR, None))
            .await
            .expect("subscribe");
        assert_eq!(
            next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT)
                .await
                .0,
            2
        );
        assert_durable_deliver_to(&mut ws, COMPONENT, "a", s1).await;
        assert_durable_deliver_to(&mut ws, COMPONENT, "b", s2).await;
    }
    drop(sd1); // graceful-shutdown server 1

    // Server 2 over the SAME db: resume from s1 replays the persisted retained row.
    let (state2, _messenger2) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Bounded(10), WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Bounded(10)),
        fixture_bus(vec![]),
    )
    .await;
    let (base2, _sd2) = spawn_test_server(state2).await;
    let mut ws2 = open_deskbar(&base2, &token).await;
    ws2.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, s1).await),
    ))
    .await
    .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws2, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "the second message resumes across the restart");
    assert_eq!(gap, None);
    assert_durable_deliver_to(&mut ws2, COMPONENT, "b", s2).await;
}

/// Multi-session fan-out: a live row reaches both attached sessions; the parked
/// backlog push row is claimed once (only the first subscriber drains it as a
/// parked claim), but the second session's fresh attach still replays the row
/// from the retained window — fresh attach is the retained window, not a
/// per-subscriber delivered log.
#[tokio::test]
async fn surface_ws_durable_multi_session_fanout_and_backlog_once() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;
    let (_pb, sb) = park_durable(&messenger, uuid, "backlog", true).await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws1 = open_deskbar(&base, &token).await;
    let mut ws2 = open_deskbar(&base, &token).await;

    // ws1 subscribes first → claims and drains the backlog.
    ws1.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe ws1");
    assert_eq!(
        next_subscribe_result(&mut ws1, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        1
    );
    assert_durable_deliver_to(&mut ws1, COMPONENT, "backlog", sb).await;

    // ws2 subscribes after → the parked row is already claimed, so it drains no
    // parked backlog, but a fresh attach replays the retained window, which still
    // holds the row.
    ws2.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe ws2");
    assert_eq!(
        next_subscribe_result(&mut ws2, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        1,
        "fresh attach replays the retained window even for a row another session drained"
    );
    assert_durable_deliver_to(&mut ws2, COMPONENT, "backlog", sb).await;

    // A live row after both subscribed fans out to both sessions.
    let (_pl, sl) = park_durable(&messenger, uuid, "live", true).await;
    dispatch_pending(&messenger).await;
    assert_durable_deliver_to(&mut ws1, COMPONENT, "live", sl).await;
    assert_durable_deliver_to(&mut ws2, COMPONENT, "live", sl).await;
}

// ===========================================================================
// The below-water ack channel. A parked row claimed at resume below the
// echoed high-water is a below-water send: it is stamped `confirm_pending`, its
// id enters the cursor's confirm set, and the next reconnect's reconcile either
// confirms it (echoed set names it → not redelivered) or redelivers it exactly
// once (absent → lost). Rig: one parked row at id 1 plus retained rows 2..5, so a
// resume at high-water 5 claims the id-1 parked row strictly below the cursor.
// ===========================================================================

/// Build the below-water rig and return `(base, db, messenger, token, parked
/// push_id, parked message_id)`. The parked row is at message id 1; ids 2..5 are
/// retained-only, lifting the channel max to 5 so a resume at high-water 5 is not
/// caught by the stale-store above-max arm.
async fn below_water_rig(uuid: Uuid) -> BelowWaterRig {
    below_water_rig_at(uuid, Depth::Bounded(10), None).await
}

/// The below-water rig's parts. Named rather than a bare tuple because the
/// variants below take three parameters and the positional form stopped reading.
struct BelowWaterRig {
    base: String,
    db: db::Db,
    messenger: Arc<Messenger>,
    token: String,
    /// The below-water push row's `(push_id, message_id)`.
    push_id: i64,
    seq: i64,
    /// Tears the test server down at the holder's scope end.
    _sd: crate::test_support::http::TestServer,
}

/// `below_water_rig` generalized on the two axes the design's matrix varies:
/// `retain_depth` (0 is the permanent-loss corner — the fresh-attach window
/// carries nothing, so the parked claim is the only recovery path) and
/// `release_after` (`Some` makes row 1 a *delayed-release* row, held out of both
/// the parked claim and the dispatchable set until [`release_all_due`]).
async fn below_water_rig_at(
    uuid: Uuid,
    retain_depth: Depth,
    release_after: Option<chrono::DateTime<Utc>>,
) -> BelowWaterRig {
    let db = db::init_db_memory();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, retain_depth, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, retain_depth),
        fixture_bus(vec![]),
    )
    .await;
    let (p1, s1) = match release_after {
        Some(at) => park_durable_delayed(&messenger, uuid, "d", at).await,
        None => park_durable(&messenger, uuid, "d", true).await,
    };
    for body in ["m2", "m3", "m4", "m5"] {
        persist_durable(&messenger, uuid, body).await;
    }
    assert_eq!(s1, 1, "parked row anchors below the retained tail");

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, sd) = spawn_test_server(state).await;
    BelowWaterRig {
        base,
        db,
        messenger,
        token,
        push_id: p1,
        seq: s1,
        _sd: sd,
    }
}

/// The store's current incarnation, so a test can prove a cursor it minted
/// earlier really does carry a lower one than the post-restart store.
async fn store_incarnation(db: &db::Db) -> i64 {
    let conn = db.lock().await;
    brenn_lib::messaging::db::read_store_identity(&conn).incarnation
}

/// Clear every outstanding `release_after` hold — the deliver-after task's own
/// entry point, driven on demand with a clock far enough ahead that the release
/// is deterministic rather than a wall-clock race.
async fn release_all_due(messenger: &Messenger) {
    let conn = messenger.db().lock().await;
    release_due_pushes(&conn, Utc::now() + chrono::Duration::hours(1));
}

/// Restart the below-water rig's server on the same store: tear down the old
/// server and its messenger/dispatcher, then build a whole fresh rig — new
/// `Messenger`, new dispatcher, new wake router, new test server — over the same
/// `Db`. Every scrap of pre-restart in-memory state (ack channel, drop counters,
/// wake routes) is gone, so a session attached to the returned base is served
/// only by post-restart state plus what genuinely persisted in the store.
///
/// The boot also bumps the store incarnation (generation unchanged), which every
/// session opened after it reads at its first durable `Subscribe`. A cursor
/// minted before the restart carries a *lower* incarnation, so it resumes
/// normally rather than tripping a stale-store arm — callers must mint the
/// cursor they resume with *before* calling this, since minting reads the
/// store's identity live and would otherwise pick up the bumped value.
async fn restart_rig(rig: BelowWaterRig, uuid: Uuid, retain_depth: Depth) -> BelowWaterRig {
    let BelowWaterRig {
        db,
        token,
        push_id,
        seq,
        _sd,
        messenger,
        ..
    } = rig;
    // Await the old server's termination and retire its messenger before the
    // fresh `Messenger::new`, so it boots against a `Db` no live task can be
    // holding — the same uniquely-owned-at-boot precondition a real boot
    // enjoys — and against no surviving in-memory dispatcher or ack state.
    _sd.shutdown().await;
    drop(messenger);
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, retain_depth, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, retain_depth),
        fixture_bus(vec![]),
    )
    .await;
    let (base, sd) = spawn_test_server(state).await;
    BelowWaterRig {
        base,
        db,
        messenger,
        token,
        push_id,
        seq,
        _sd: sd,
    }
}

/// The parsed confirm set of the next durable `Deliver`.
async fn next_deliver_confirm(ws: &mut SurfaceWs) -> (String, i64, Vec<i64>) {
    match next_deliver(ws).await {
        (
            body,
            CursorState::Durable {
                high_water,
                confirm,
                ..
            },
        ) => (body, high_water, confirm),
        (body, other) => panic!("expected durable cursor for {body:?}, got {other:?}"),
    }
}

/// A below-water send (a parked row claimed below the resume high-water) is
/// stamped `confirm_pending` on the DB row and carries its id in the delivered
/// cursor's confirm set, while the cursor's high-water stays put.
#[tokio::test]
async fn surface_ws_below_water_delivery_stamps_confirm_and_carries_it() {
    let uuid = Uuid::new_v4();
    let BelowWaterRig {
        base,
        db,
        token,
        push_id: p1,
        seq: s1,
        _sd,
        ..
    } = below_water_rig(uuid).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, 5).await),
    ))
    .await
    .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "only the below-water parked row replays");
    assert_eq!(gap, None);

    let (body, high_water, confirm) = next_deliver_confirm(&mut ws).await;
    assert_eq!(body, "d");
    assert_eq!(
        high_water, 5,
        "a below-water send leaves the high-water put"
    );
    assert_eq!(confirm, vec![s1], "its id enters the confirm set");
    assert_eq!(
        confirm_pending_flag(&db, p1).await,
        1,
        "the DB row is stamped tentative before the socket write"
    );
}

/// received + reconnect: the reconnect echoes a cursor whose confirm set names the
/// below-water row, so the reconcile confirms it and it is not redelivered.
#[tokio::test]
async fn surface_ws_below_water_received_is_confirmed_and_not_redelivered() {
    let uuid = Uuid::new_v4();
    let BelowWaterRig {
        base,
        db,
        token,
        push_id: p1,
        seq: s1,
        _sd,
        ..
    } = below_water_rig(uuid).await;

    let mut ws1 = open_deskbar(&base, &token).await;
    ws1.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, 5).await),
    ))
    .await
    .expect("subscribe ws1");
    assert_eq!(
        next_subscribe_result(&mut ws1, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        1
    );
    let (_body, _hw, confirm) = next_deliver_confirm(&mut ws1).await;
    assert_eq!(confirm, vec![s1]);
    drop(ws1);

    // Reconnect echoing the confirm set: the row was received.
    let mut ws2 = open_deskbar(&base, &token).await;
    ws2.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume_with_confirm(&db, 5, vec![s1]).await),
    ))
    .await
    .expect("subscribe ws2");
    let (replay, _gap) = next_subscribe_result(&mut ws2, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 0, "a confirmed below-water row is not redelivered");
    assert_eq!(
        confirm_pending_flag(&db, p1).await,
        0,
        "confirm clears the tentative flag; the row ages out via GC"
    );
    assert_no_deliver(&mut ws2).await;
}

/// lost + reconnect: the reconnect echoes a cursor whose confirm set omits the
/// below-water row, so the reconcile unclaims it and the parked claim redelivers
/// it exactly once.
#[tokio::test]
async fn surface_ws_below_water_lost_is_redelivered_exactly_once() {
    let uuid = Uuid::new_v4();
    let BelowWaterRig {
        base,
        db,
        token,
        push_id: p1,
        seq: s1,
        _sd,
        ..
    } = below_water_rig(uuid).await;

    let mut ws1 = open_deskbar(&base, &token).await;
    ws1.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, 5).await),
    ))
    .await
    .expect("subscribe ws1");
    assert_eq!(
        next_subscribe_result(&mut ws1, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        1
    );
    let _ = next_deliver_confirm(&mut ws1).await;
    drop(ws1);

    // Reconnect echoing no confirm set: the row was lost.
    let mut ws2 = open_deskbar(&base, &token).await;
    ws2.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, 5).await),
    ))
    .await
    .expect("subscribe ws2");
    let (replay, _gap) = next_subscribe_result(&mut ws2, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 1,
        "a lost below-water row redelivers via the parked claim"
    );
    let (body, _hw, confirm) = next_deliver_confirm(&mut ws2).await;
    assert_eq!(body, "d");
    assert_eq!(
        confirm,
        vec![s1],
        "redelivery is itself below-water and re-stamps"
    );
    assert_eq!(confirm_pending_flag(&db, p1).await, 1);
    assert_no_deliver(&mut ws2).await;
}

/// fresh attach: no cursor means no evidence, so every tentative row is unclaimed
/// and redelivered through the parked claim — recovering the row even where the
/// retained window would otherwise not carry it. The redelivery is itself stamped
/// tentative even though the fresh-attach high-water is 0, so a second dead socket
/// during the redelivery is still recoverable (a parked row's recovery channel is
/// the confirm set, never the window).
#[tokio::test]
async fn surface_ws_below_water_fresh_attach_redelivers_as_parked() {
    let uuid = Uuid::new_v4();
    let BelowWaterRig {
        base,
        db,
        token,
        push_id: p1,
        seq: s1,
        _sd,
        ..
    } = below_water_rig(uuid).await;

    let mut ws1 = open_deskbar(&base, &token).await;
    ws1.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, 5).await),
    ))
    .await
    .expect("subscribe ws1");
    assert_eq!(
        next_subscribe_result(&mut ws1, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        1
    );
    let _ = next_deliver_confirm(&mut ws1).await;
    drop(ws1);

    // Fresh attach: the retained window (ids 2..5, clamp 10) plus the unclaimed
    // parked row (id 1) — five rows, the parked one recovered.
    let mut ws2 = open_deskbar(&base, &token).await;
    ws2.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe ws2");
    let (replay, _gap) = next_subscribe_result(&mut ws2, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 5,
        "fresh attach recovers the tentative row plus the window"
    );
    // The parked row (id 1) sorts first among the merged replay.
    let (body, high_water, confirm) = next_deliver_confirm(&mut ws2).await;
    assert_eq!(body, "d", "the recovered parked row leads the replay");
    assert_eq!(
        high_water, s1,
        "the fresh-attach anchor is 0; delivering the parked row advances it to \
         the row's own id"
    );
    assert_eq!(
        confirm,
        vec![s1],
        "the fresh-attach redelivery is force-stamped so it stays recoverable"
    );
    assert_eq!(
        confirm_pending_flag(&db, p1).await,
        1,
        "the redelivered parked row is stamped tentative on the DB row"
    );
}

/// The canonical below-water producer: a delayed-release row released **live
/// while attached**, reaching the below-water branch from the live-delivery arm
/// rather than the subscribe replay. The row is held at id 1 while the
/// subscription resumes at high-water 5, so when the hold clears the live send is
/// below-water by construction: it stamps, carries the confirm set, and leaves the
/// high-water put. Echoing that cursor confirms it — no redelivery.
#[tokio::test]
async fn surface_ws_below_water_live_release_stamps_and_confirms() {
    let uuid = Uuid::new_v4();
    let BelowWaterRig {
        base,
        db,
        messenger,
        token,
        push_id: p1,
        seq: s1,
        _sd,
    } = below_water_rig_at(
        uuid,
        Depth::Bounded(10),
        Some(Utc::now() + chrono::Duration::hours(1)),
    )
    .await;

    let mut ws = open_deskbar(&base, &token).await;
    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, 5).await),
    ))
    .await
    .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 0,
        "the held row is not claimable while released_after stands"
    );
    assert_eq!(gap, None);

    // The hold clears and the dispatcher fans the row to the attached session:
    // the live arm's below-water send.
    release_all_due(&messenger).await;
    dispatch_pending(&messenger).await;

    let (body, high_water, confirm) = next_deliver_confirm(&mut ws).await;
    assert_eq!(body, "d", "the released row arrives live");
    assert_eq!(
        high_water, 5,
        "a below-water live send leaves the high-water put"
    );
    assert_eq!(
        confirm,
        vec![s1],
        "the live send's id enters the confirm set"
    );
    assert_eq!(
        confirm_pending_flag(&db, p1).await,
        1,
        "the live path stamps the DB row before the socket write"
    );
    drop(ws);

    // The page received it and echoes the set: confirmed, never redelivered.
    let mut ws2 = open_deskbar(&base, &token).await;
    ws2.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume_with_confirm(&db, 5, vec![s1]).await),
    ))
    .await
    .expect("subscribe ws2");
    let (replay, _gap) = next_subscribe_result(&mut ws2, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 0,
        "a confirmed live below-water row is not redelivered"
    );
    assert_eq!(confirm_pending_flag(&db, p1).await, 0);
    assert_no_deliver(&mut ws2).await;
}

/// Restart survival, timing 1 — **released while attached**, then the socket dies
/// before the page received the frame and the server restarts. The tentative row
/// and the incarnation bump are both store state, so the pre-restart cursor
/// resumes normally and the reconcile (no confirm set echoed → lost) redelivers
/// the row exactly once across the boundary.
#[tokio::test]
async fn surface_ws_below_water_delayed_release_attached_survives_restart_once() {
    let uuid = Uuid::new_v4();
    let rig = below_water_rig_at(
        uuid,
        Depth::Bounded(10),
        Some(Utc::now() + chrono::Duration::hours(1)),
    )
    .await;
    let (p1, s1) = (rig.push_id, rig.seq);

    let mut ws = open_deskbar(&rig.base, &rig.token).await;
    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&rig.db, 5).await),
    ))
    .await
    .expect("subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        0
    );
    release_all_due(&rig.messenger).await;
    dispatch_pending(&rig.messenger).await;
    let (_body, _hw, confirm) = next_deliver_confirm(&mut ws).await;
    assert_eq!(confirm, vec![s1]);
    // The socket dies with the frame in flight — the page never saw it.
    drop(ws);

    // Minted before the restart, so it carries the pre-restart incarnation —
    // what a page holding a cursor across a deploy actually presents.
    let pre_restart_cursor = durable_resume(&rig.db, 5).await;
    let pre_restart_incarnation = store_incarnation(&rig.db).await;

    let BelowWaterRig {
        base,
        db,
        token,
        _sd,
        ..
    } = restart_rig(rig, uuid, Depth::Bounded(10)).await;
    assert_eq!(
        confirm_pending_flag(&db, p1).await,
        1,
        "the tentative stamp is store state and survives the restart"
    );
    assert!(
        store_incarnation(&db).await > pre_restart_incarnation,
        "the restart bumped the incarnation, so the held cursor is genuinely the lower one"
    );

    // Resume with the pre-restart cursor: lower incarnation, so not stale.
    let mut ws2 = open_deskbar(&base, &token).await;
    ws2.send(subscribe_frame(DURABLE_ADDR, Some(pre_restart_cursor)))
        .await
        .expect("subscribe ws2");
    let (replay, _gap) = next_subscribe_result(&mut ws2, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "the lost row redelivers across the restart");
    let (body, _hw, confirm) = next_deliver_confirm(&mut ws2).await;
    assert_eq!(body, "d");
    assert_eq!(confirm, vec![s1], "the redelivery re-stamps");
    assert_no_deliver(&mut ws2).await;
}

/// Restart survival, timing 2 — **released while detached**. Nothing is owed to a
/// dead session, so the release leaves the row parked and unclaimed; the restart
/// changes nothing about it, and the next attach drains it exactly once through
/// the ordinary parked claim.
#[tokio::test]
async fn surface_ws_below_water_delayed_release_detached_survives_restart_once() {
    let uuid = Uuid::new_v4();
    let rig = below_water_rig_at(
        uuid,
        Depth::Bounded(10),
        Some(Utc::now() + chrono::Duration::hours(1)),
    )
    .await;
    let (p1, s1) = (rig.push_id, rig.seq);

    // No session attached: the release and dispatch pass find nobody.
    release_all_due(&rig.messenger).await;
    dispatch_pending(&rig.messenger).await;
    assert_eq!(
        confirm_pending_flag(&rig.db, p1).await,
        0,
        "a row released with nobody attached is never sent, so never stamped"
    );

    // Minted before the restart: the resumed cursor carries the lower incarnation.
    let pre_restart_cursor = durable_resume(&rig.db, 5).await;
    let pre_restart_incarnation = store_incarnation(&rig.db).await;

    let BelowWaterRig {
        base,
        db,
        token,
        _sd,
        ..
    } = restart_rig(rig, uuid, Depth::Bounded(10)).await;
    assert!(
        store_incarnation(&db).await > pre_restart_incarnation,
        "the restart bumped the incarnation, so the held cursor is genuinely the lower one"
    );

    let mut ws = open_deskbar(&base, &token).await;
    ws.send(subscribe_frame(DURABLE_ADDR, Some(pre_restart_cursor)))
        .await
        .expect("subscribe");
    let (replay, _gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "the released row drains once at the next attach");
    let (body, high_water, confirm) = next_deliver_confirm(&mut ws).await;
    assert_eq!(body, "d");
    assert_eq!(high_water, 5, "still below the resumed high-water");
    assert_eq!(confirm, vec![s1]);
    assert_no_deliver(&mut ws).await;
}

/// The permanent-loss corner at **every depth resolved to 0**: the retained window
/// carries nothing at all, so the parked claim is the row's only recovery path. A
/// fresh attach has no cursor and therefore no evidence, unclaims the tentative
/// row, and redelivers it — proving recovery does not depend on the window
/// carrying the message.
#[tokio::test]
async fn surface_ws_below_water_fresh_attach_recovers_at_depth_zero() {
    let uuid = Uuid::new_v4();
    let BelowWaterRig {
        base,
        db,
        token,
        push_id: p1,
        seq: s1,
        _sd,
        ..
    } = below_water_rig_at(uuid, Depth::Bounded(0), None).await;

    let mut ws1 = open_deskbar(&base, &token).await;
    ws1.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, 5).await),
    ))
    .await
    .expect("subscribe ws1");
    assert_eq!(
        next_subscribe_result(&mut ws1, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        1,
        "the parked claim is id-agnostic: it delivers at depth 0 too"
    );
    let (_body, _hw, confirm) = next_deliver_confirm(&mut ws1).await;
    assert_eq!(confirm, vec![s1]);
    // Lost: the socket dies before the page stored the cursor.
    drop(ws1);

    let mut ws2 = open_deskbar(&base, &token).await;
    ws2.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe ws2");
    let (replay, _gap) = next_subscribe_result(&mut ws2, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 1,
        "at depth 0 the window carries nothing; the recovered row is the whole replay"
    );
    let (body, _hw, confirm) = next_deliver_confirm(&mut ws2).await;
    assert_eq!(
        body, "d",
        "the tentative row recovers through the parked claim"
    );
    assert_eq!(
        confirm,
        vec![s1],
        "the fresh-attach redelivery is force-stamped"
    );
    assert_eq!(confirm_pending_flag(&db, p1).await, 1);
    assert_no_deliver(&mut ws2).await;
}

/// A stale confirm-set entry — an id the echoed cursor names but that matches no
/// tentative row (e.g. an already-confirmed row under a newer cursor the client
/// never saw, or a bogus id) — is harmless: the reconcile's partition simply finds
/// no matching tentative row, and the genuine below-water row is still confirmed.
#[tokio::test]
async fn surface_ws_below_water_stale_confirm_entry_is_ignored() {
    let uuid = Uuid::new_v4();
    let BelowWaterRig {
        base,
        db,
        token,
        push_id: p1,
        seq: s1,
        _sd,
        ..
    } = below_water_rig(uuid).await;

    let mut ws1 = open_deskbar(&base, &token).await;
    ws1.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, 5).await),
    ))
    .await
    .expect("subscribe ws1");
    assert_eq!(
        next_subscribe_result(&mut ws1, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        1
    );
    let _ = next_deliver_confirm(&mut ws1).await;
    drop(ws1);

    // Reconnect echoing the real below-water id plus a stale one (9999) that names
    // no tentative row: the reconcile confirms s1 and ignores 9999 — no panic, no
    // redelivery.
    let mut ws2 = open_deskbar(&base, &token).await;
    ws2.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume_with_confirm(&db, 5, vec![s1, 9999]).await),
    ))
    .await
    .expect("subscribe ws2");
    let (replay, _gap) = next_subscribe_result(&mut ws2, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 0, "the confirmed row is not redelivered");
    assert_eq!(
        confirm_pending_flag(&db, p1).await,
        0,
        "the stale entry is inert; the real one still confirms"
    );
    assert_no_deliver(&mut ws2).await;
}

/// One session holding an ephemeral and a durable subscription concurrently
/// receives interleaved deliveries of both classes, each with its own `Pos`
/// kind — the single-session mixed-class coverage the two-surface demo split
/// would otherwise lose.
#[tokio::test]
async fn surface_ws_durable_and_ephemeral_on_one_session() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let eph = "ticker";
    let bus = fixture_bus(vec![ephemeral_channel_entry(eph, 0, 64)]);
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, Some(eph)),
        durable_channel_entry(uuid, Depth::Unbounded),
        bus.clone(),
    )
    .await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe durable");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        0
    );
    ws.send(subscribe_frame("ephemeral:ticker", None))
        .await
        .expect("subscribe ephemeral");
    assert_eq!(
        next_subscribe_result(&mut ws, "ephemeral:ticker", COMPONENT)
            .await
            .0,
        0
    );

    // Interleave a durable live delivery and an ephemeral publish; both arrive,
    // each with its own Pos kind (order between the two classes is unspecified).
    let (_p, seq) = park_durable(&messenger, uuid, "dur", true).await;
    dispatch_pending(&messenger).await;
    publish_eph(&bus, eph, "ephemeral:ticker", "eph");

    let mut saw_durable = false;
    let mut saw_ephemeral = false;
    for _ in 0..2 {
        let (body, state) = next_deliver(&mut ws).await;
        match state {
            CursorState::Durable {
                high_water: got, ..
            } => {
                assert_eq!(body, "dur");
                assert_eq!(got, seq);
                saw_durable = true;
            }
            CursorState::Ephemeral { .. } => {
                assert_eq!(body, "eph");
                saw_ephemeral = true;
            }
        }
    }
    assert!(
        saw_durable && saw_ephemeral,
        "both a durable and an ephemeral delivery must reach the one session"
    );
}

/// Session-side delivery-floor parity: when the surface policy does not authorize
/// brenn delivery on the channel, a durable subscribe claims (retires) the parked
/// backlog without delivering — `replay_count = 0`, no `Deliver` — the same
/// retire-without-deliver the dispatcher floor applies to App/Wasm subscribers.
#[tokio::test]
async fn surface_ws_durable_floor_denied_retires_without_delivery() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, false, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;
    let (p1, _s1) = park_durable(&messenger, uuid, "denied-1", true).await;
    let (p2, _s2) = park_durable(&messenger, uuid, "denied-2", true).await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe");
    let (replay, _) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 0, "the floor denies → empty replay");
    assert_no_deliver(&mut ws).await;

    // The parked rows were claimed (retired), not left for a later drain.
    let conn = messenger.db().lock().await;
    assert!(
        brenn_lib::messaging::db::claim_pending_pushes(&conn, &[p1, p2]).is_empty(),
        "floor-denied rows are retired (claimed), matching the dispatcher floor"
    );
}

/// Unsubscribe then re-subscribe a durable channel: the fresh subscription drains
/// a newly parked backlog exactly once and never re-delivers a stale row. The
/// exact queued-copy dedup race the retained `replay_sent` set closes is covered
/// deterministically by the `DurableSessionState` unit tests; this pins the
/// end-to-end lifecycle over the wire.
#[tokio::test]
async fn surface_ws_durable_unsubscribe_then_resubscribe_delivers_fresh_backlog_once() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT)
            .await
            .0,
        0
    );

    ws.send(Message::Text(
        serde_json::to_string(&ClientFrame::Unsubscribe {
            channel: DURABLE_ADDR.to_string(),
            instance: COMPONENT.to_string(),
        })
        .expect("serialize")
        .into(),
    ))
    .await
    .expect("unsubscribe");

    // A row parked after unsubscribe drains on the fresh re-subscribe, once.
    let (_p, seq) = park_durable(&messenger, uuid, "after", true).await;
    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("re-subscribe");
    let (replay, _) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "the freshly parked row drains on re-subscribe");
    assert_durable_deliver_to(&mut ws, COMPONENT, "after", seq).await;
    assert_no_deliver(&mut ws).await;
}

/// The session-side `already_replayed` dedup covers the drain pass, not just the
/// live arm: a seq the replay already put on the wire is never re-sent by a later
/// drain, even after the push row is unclaimed and re-claimed. Without the
/// drain-path check, the retained re-send plus a re-claim would duplicate the seq
/// on the wire — the at-most-once violation the dedup exists to prevent.
#[tokio::test]
async fn surface_ws_durable_drain_skips_already_replayed_seq() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;

    // A parked (non-eager) row; pre-claim it to simulate another actor (a second
    // session's drain) owning the row, so this session's parked load misses it and
    // it can only reach the wire via the retained window on resume.
    let (p, s) = park_durable(&messenger, uuid, "dup", false).await;
    {
        let conn = messenger.db().lock().await;
        assert_eq!(
            brenn_lib::messaging::db::claim_pending_pushes(&conn, &[p]),
            vec![p],
            "pre-claim the row so the resume replays it from the retained window"
        );
    }

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    // Resume below S: the parked load misses the claimed row, but the retained
    // window re-sends S and records it in this connection's replay_sent.
    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, s - 1).await),
    ))
    .await
    .expect("subscribe");
    let (replay, _gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "the retained window re-sends S once");
    assert_durable_deliver_to(&mut ws, COMPONENT, "dup", s).await;

    // The other actor "disconnects": its claim is released, re-parking the row.
    {
        let conn = messenger.db().lock().await;
        brenn_lib::messaging::db::unclaim_pending_pushes(&conn, &[p]);
    }

    // A louder live row nudges this session's drain (SD5 step 6). The drain
    // re-claims the re-parked row, but its seq is already in replay_sent, so it is
    // retired without a second Deliver — only the live row reaches the wire.
    let (_p2, s2) = park_durable(&messenger, uuid, "live", true).await;
    dispatch_pending(&messenger).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "live", s2).await;
    assert_no_deliver(&mut ws).await;

    // The de-duped row is retired (claimed) by the drain, not re-delivered.
    let conn = messenger.db().lock().await;
    assert!(
        brenn_lib::messaging::db::claim_pending_pushes(&conn, &[p]).is_empty(),
        "the already-replayed row is retired by the drain, not left parked"
    );
}

/// The session-side `already_replayed` dedup covers the **live arm**
/// (`durable_rx`), not just the drain: a seq the subscribe replay already put on
/// the wire is dropped when the router later `try_send`s the same seq straight
/// into `durable_rx` as a live delivery. Removing the live-arm skip
/// (`session.rs`, the `durable_rx` arm) fails this test — the router's live copy
/// of the already-replayed row would reach the wire a second time, the
/// at-most-once violation delta item 1's original fix exists to prevent.
#[tokio::test]
async fn surface_ws_durable_live_arm_skips_already_replayed_seq() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;

    // An eager row, pre-claimed to simulate another actor owning it, so this
    // session's parked load misses it and it reaches the wire only via the
    // retained window on resume.
    let (p, s) = park_durable(&messenger, uuid, "dup", true).await;
    {
        let conn = messenger.db().lock().await;
        assert_eq!(
            brenn_lib::messaging::db::claim_pending_pushes(&conn, &[p]),
            vec![p],
            "pre-claim the row so the resume replays it from the retained window"
        );
    }

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    // Resume below S: the parked load misses the claimed row, but the retained
    // window re-sends S and records it in this connection's replay_sent.
    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, s - 1).await),
    ))
    .await
    .expect("subscribe");
    let (replay, _gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "the retained window re-sends S once");
    assert_durable_deliver_to(&mut ws, COMPONENT, "dup", s).await;

    // The other actor "disconnects": its claim is released, re-parking the eager
    // row so the dispatcher picks it up as a live delivery.
    {
        let conn = messenger.db().lock().await;
        brenn_lib::messaging::db::unclaim_pending_pushes(&conn, &[p]);
    }

    // The dispatcher claims the re-parked eager row and the router `try_send`s its
    // live copy straight into `durable_rx`. Its seq is already in replay_sent, so
    // the live arm drops it — no second Deliver reaches the wire. (The router's
    // synchronous claim, not a later drain, retires the row, so this exercises the
    // live arm and not the drain-path check.)
    dispatch_pending(&messenger).await;
    assert_no_deliver(&mut ws).await;

    // The live-delivered row is retired (claimed) by the router, not re-parked.
    let conn = messenger.db().lock().await;
    assert!(
        brenn_lib::messaging::db::claim_pending_pushes(&conn, &[p]).is_empty(),
        "the already-replayed row is retired by the router claim, not left parked"
    );
}

/// The retained `replay_sent` set gates the subscribe **merged-replay** path
/// across an unsubscribe/re-subscribe cycle: a seq delivered under the first
/// subscription is excluded from the second subscription's retained re-send.
/// Removing the merged-replay retain (`handle_durable_subscribe`) fails this
/// test — the retained window would re-deliver S on the re-subscribe, the
/// duplicate the connection-lifetime `replay_sent` set closes (delta item 1).
#[tokio::test]
async fn surface_ws_durable_resubscribe_replay_skips_already_replayed_seq() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;

    let (_p, s) = park_durable(&messenger, uuid, "dup", true).await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    // First subscribe drains the parked row and records S in replay_sent.
    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe");
    let (replay, _) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "parked S drains on the first subscribe");
    assert_durable_deliver_to(&mut ws, COMPONENT, "dup", s).await;

    // Unsubscribe retains replay_sent for the connection lifetime.
    ws.send(unsubscribe_frame(DURABLE_ADDR))
        .await
        .expect("unsubscribe");

    // Re-subscribe below S: the retained window loads S (S's push row is claimed,
    // so the parked set is empty), but the merged-replay retain drops it because S
    // is already in replay_sent — an empty replay, no second Deliver.
    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, s - 1).await),
    ))
    .await
    .expect("re-subscribe");
    let (replay, _) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 0,
        "the already-replayed seq is excluded from the re-send"
    );
    assert_no_deliver(&mut ws).await;
}

/// An ephemeral resume token on a durable channel is a class mismatch a correct
/// client cannot produce — the symmetric case of the durable-resume-on-ephemeral
/// violation. It is a protocol violation that tears the connection down.
#[tokio::test]
async fn surface_ws_durable_subscribe_ephemeral_resume_is_violation() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, _messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(cursor::mint_ephemeral(Uuid::new_v4(), 3)),
    ))
    .await
    .expect("send");
    assert!(
        drain_until_closed(&mut ws).await,
        "ephemeral resume on a durable channel must close the connection"
    );
}

/// A second Subscribe to an already-active durable channel exercises the
/// duplicate-Subscribe check's `durable.is_active` half — a protocol violation.
#[tokio::test]
async fn surface_ws_durable_subscribe_duplicate_is_violation() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, _messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Unbounded, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        fixture_bus(vec![]),
    )
    .await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("first subscribe");
    let _ = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("duplicate subscribe");
    assert!(
        drain_until_closed(&mut ws).await,
        "duplicate durable Subscribe must close the connection"
    );
}

// ---------------------------------------------------------------------------
// Telemetry plane: `Geometry` / `Status` frames. The session arm charges
// the publish bucket, converts a validation failure into a protocol violation
// (teardown + fail2ban security event), and publishes the server-stamped
// document to the surface's derived channel under the `surface:<slug>` identity.
// A terminal `disconnected` snapshot is written when the last session closes.
// ---------------------------------------------------------------------------

/// Bare derived-channel names for the `deskbar` surface under prefix `surface`.
const GEOMETRY_NAME: &str = "surface.surface.deskbar.geometry";
const STATUS_NAME: &str = "surface.surface.deskbar.status";

/// Telemetry rig: the two derived channels declared in DB + directory
/// (bounded retain), a `Messenger` whose `surface_policies` grant `deskbar` the
/// injected `MessagingPublish` + `brenn_publish` coverage build_messaging would
/// inject, and `build_surface_runtimes` wired with the description runtime
/// (prefix `surface`, 60 s interval). `flusher`/`alerts` back the protocol-
/// violation assertions; the channel UUIDs read back the persisted telemetry.
struct GeoStatusRig {
    state: AppState,
    flusher: AlertDispatcher,
    alerts: Arc<Mutex<Vec<(String, String)>>>,
    geometry_uuid: Uuid,
    status_uuid: Uuid,
}

async fn geometry_status_rig(db: &db::Db) -> GeoStatusRig {
    let (mut state, alerts, _handle) = test_state_with_capturing_alerter(db);
    let flusher = state.alert_dispatcher.clone();

    let geometry_uuid = Uuid::new_v4();
    let status_uuid = Uuid::new_v4();
    let bounded = |uuid: Uuid, address: &str| ChannelConfigRaw {
        uuid: uuid.to_string(),
        address: address.to_string(),
        description: None,
        push_depth: None,
        retain_depth: Some(Depth::Bounded(1)),
        standing_retain_depth: Some(Depth::Bounded(1)),
        noise: None,
        sink: None,
        wake_min: None,
    };
    let entries = build_channel_entries(
        &[
            bounded(geometry_uuid, GEOMETRY_NAME),
            bounded(status_uuid, STATUS_NAME),
        ],
        &MessagingGlobalConfig::default(),
    );
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &entries);
    }

    let mut deskbar_policy = AppPolicy::default();
    deskbar_policy
        .grants
        .insert(AppCapability::MessagingPublish);
    deskbar_policy.acls.brenn_publish = vec![
        ChannelMatcher::Exact(GEOMETRY_NAME.to_string()),
        ChannelMatcher::Exact(STATUS_NAME.to_string()),
    ];
    let mut surface_policies = std::collections::HashMap::new();
    surface_policies.insert("deskbar".to_string(), deskbar_policy);
    let surfaces = vec![deskbar_pub(60, 60)];

    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(entries)),
        Arc::from(TEST_ORIGIN),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(brenn_lib::messaging::query::NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(brenn_lib::messaging::testutils::surface_registrations(
        surface_policies,
    ))
    .with_surface_send_budgets(budget_principals(&surfaces));

    let bus = fixture_bus(vec![ephemeral_channel_entry(EPH_NAME, 0, 16)]);
    state.surfaces = Arc::new(build_surface_runtimes(
        surfaces,
        bus,
        Some(messenger),
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    GeoStatusRig {
        state,
        flusher,
        alerts,
        geometry_uuid,
        status_uuid,
    }
}

fn geometry_frame(width: u32, height: u32, device_pixel_ratio: f64) -> Message {
    let frame = ClientFrame::Geometry {
        width,
        height,
        device_pixel_ratio,
    };
    Message::Text(
        serde_json::to_string(&frame)
            .expect("serialize Geometry")
            .into(),
    )
}

fn instance_report(instance: &str, kind: &str, state: InstanceState, ports: u32) -> InstanceReport {
    InstanceReport {
        instance: instance.to_string(),
        kind: kind.to_string(),
        state,
        reason: None,
        ports_attached: ports,
    }
}

fn status_frame(instances: &[InstanceReport], uptime_secs: u64) -> Message {
    status_frame_with_counters(instances, uptime_secs, StatusCounters::default())
}

/// [`status_frame`] with the counters object spelled out, for the tests that
/// care about the per-instance breakdown rather than the instance states.
fn status_frame_with_counters(
    instances: &[InstanceReport],
    uptime_secs: u64,
    counters: StatusCounters,
) -> Message {
    let frame = ClientFrame::Status {
        instances: instances.to_vec(),
        uptime_secs,
        counters,
    };
    Message::Text(
        serde_json::to_string(&frame)
            .expect("serialize Status")
            .into(),
    )
}

/// Poll `channel_uuid` until `pred` holds over its persisted rows (or ~2 s).
/// Telemetry frames have no wire ack, so a reader waits on the row rather than a
/// response. Robust to bounded-retain pruning by asserting over the current rows.
async fn wait_for_channel<F>(db: &db::Db, channel_uuid: Uuid, pred: F) -> Vec<(String, String)>
where
    F: Fn(&[(String, String)]) -> bool,
{
    for _ in 0..200 {
        let rows = read_channel_messages(db, channel_uuid).await;
        if pred(&rows) {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("channel {channel_uuid} did not reach the expected state within timeout");
}

fn body_health(body: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(body).expect("telemetry body is JSON");
    v["health"]
        .as_str()
        .expect("body carries a health string")
        .to_string()
}

#[tokio::test]
async fn surface_ws_geometry_publishes_to_derived_channel_under_surface_identity() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let geometry_uuid = rig.geometry_uuid;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    ws.send(geometry_frame(1920, 515, 2.0))
        .await
        .expect("send Geometry");

    let rows = wait_for_channel(&db, geometry_uuid, |r| !r.is_empty()).await;
    let (sender, body) = rows.last().expect("a geometry row");
    assert_eq!(
        sender, "surface:deskbar",
        "geometry published under the surface identity"
    );
    let v: serde_json::Value = serde_json::from_str(body).expect("geometry body is JSON");
    assert_eq!(v["surface"], serde_json::json!("deskbar"));
    assert_eq!(
        v["viewport"],
        serde_json::json!({ "width": 1920, "height": 515 })
    );
    assert_eq!(v["device_pixel_ratio"], serde_json::json!(2.0));
    assert!(
        v["session"].is_string(),
        "server stamps the reporting session id"
    );
}

#[tokio::test]
async fn surface_ws_status_publishes_derived_health_under_surface_identity() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let status_uuid = rig.status_uuid;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Both configured instances mounted, protobar covering its one expected pump
    // ⇒ server derives `ok`.
    ws.send(status_frame(
        &[
            instance_report("protobar", "protobar", InstanceState::Mounted, 1),
            instance_report("writer", "writer-module", InstanceState::Mounted, 0),
        ],
        42,
    ))
    .await
    .expect("send Status");

    let rows = wait_for_channel(&db, status_uuid, |r| !r.is_empty()).await;
    let (sender, body) = rows.last().expect("a status row");
    assert_eq!(sender, "surface:deskbar");
    let v: serde_json::Value = serde_json::from_str(body).expect("status body is JSON");
    assert_eq!(
        v["health"],
        serde_json::json!("ok"),
        "server derives ok from the reported facts"
    );
    assert_eq!(v["uptime_secs"], serde_json::json!(42));
    assert!(v["instances"].as_array().is_some_and(|a| a.len() == 2));
}

#[tokio::test]
async fn surface_ws_geometry_out_of_bounds_is_violation() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let flusher = rig.flusher.clone();
    let alerts = rig.alerts.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    // Feature is on, but a DPR of 100 is out of the accepted 0.1..=16 range.
    assert_frame_is_violation(
        &base,
        &token,
        geometry_frame(1920, 1080, 100.0),
        &flusher,
        &alerts,
    )
    .await;
}

#[tokio::test]
async fn surface_ws_status_unknown_instance_is_violation() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let flusher = rig.flusher.clone();
    let alerts = rig.alerts.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    // `ghost` is not a configured instance of `deskbar`.
    assert_frame_is_violation(
        &base,
        &token,
        status_frame(
            &[instance_report(
                "ghost",
                "protobar",
                InstanceState::Mounted,
                1,
            )],
            0,
        ),
        &flusher,
        &alerts,
    )
    .await;
}

/// The per-instance counter map is client input naming principals, so it wears
/// the configured-instance rule the `instances` list does: a key the surface does
/// not configure kills the session. The retained status document is where an
/// operator reads attribution, and a client must not be able to write a
/// principal into it that the operator never declared.
#[tokio::test]
async fn surface_ws_status_counters_unknown_instance_is_violation() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let flusher = rig.flusher.clone();
    let alerts = rig.alerts.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    // Every reported *instance* is configured; only the counters name `ghost`,
    // so this fails only if the counters map is validated on its own.
    assert_frame_is_violation(
        &base,
        &token,
        status_frame_with_counters(
            &[instance_report(
                "protobar",
                "protobar",
                InstanceState::Mounted,
                1,
            )],
            0,
            StatusCounters {
                instances: [(
                    "ghost".to_string(),
                    brenn_surface_proto::InstanceCounters {
                        publishes: 1,
                        drops: 0,
                    },
                )]
                .into_iter()
                .collect(),
                ..StatusCounters::default()
            },
        ),
        &flusher,
        &alerts,
    )
    .await;
}

/// A conforming per-instance breakdown reaches the retained status document
/// end-to-end: the shell's map survives validation, the server-stamped body
/// carries it, and an operator reading the channel sees which instance published
/// and which lost messages.
#[tokio::test]
async fn surface_ws_status_counters_per_instance_reach_the_status_document() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let status_uuid = rig.status_uuid;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    ws.send(status_frame_with_counters(
        &[
            instance_report("protobar", "protobar", InstanceState::Mounted, 1),
            instance_report("writer", "writer-module", InstanceState::Mounted, 0),
        ],
        7,
        StatusCounters {
            deliveries: 9,
            publishes: 4,
            errors: 0,
            instances: [(
                "protobar".to_string(),
                brenn_surface_proto::InstanceCounters {
                    publishes: 4,
                    drops: 2,
                },
            )]
            .into_iter()
            .collect(),
        },
    ))
    .await
    .expect("send Status");

    let rows = wait_for_channel(&db, status_uuid, |r| {
        r.last()
            .is_some_and(|(_, b)| b.contains("\"instances\":{\"protobar\""))
    })
    .await;
    let body: serde_json::Value =
        serde_json::from_str(&rows.last().expect("status row").1).expect("status body is JSON");
    assert_eq!(
        body["counters"]["instances"],
        serde_json::json!({ "protobar": { "publishes": 4, "drops": 2 } }),
        "the breakdown lands in the document verbatim; `writer` counted nothing \
         and is legitimately absent"
    );
}

#[tokio::test]
async fn surface_ws_last_session_close_writes_disconnected_terminal_snapshot() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let status_uuid = rig.status_uuid;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // A status report populates the session's last-known instance list so the
    // terminal snapshot can carry it. protobar failed ⇒ derived `degraded`.
    ws.send(status_frame(
        &[
            instance_report("protobar", "protobar", InstanceState::Failed, 0),
            instance_report("writer", "writer-module", InstanceState::Mounted, 0),
        ],
        7,
    ))
    .await
    .expect("send Status");
    // Wait for the live row: proves the server processed the frame (and set the
    // last-known instances) before we close — a happens-before for the teardown.
    let live = wait_for_channel(&db, status_uuid, |r| {
        r.last().is_some_and(|(_, b)| body_health(b) == "degraded")
    })
    .await;
    assert_eq!(body_health(&live.last().unwrap().1), "degraded");

    // Close the socket; the last-session teardown must write a terminal
    // `disconnected` snapshot as the retained value (bounded retain may prune the
    // live row, so assert over the current last row rather than a count).
    drop(ws);
    let rows = wait_for_channel(&db, status_uuid, |r| {
        r.last()
            .is_some_and(|(_, b)| body_health(b) == "disconnected")
    })
    .await;
    let (sender, body) = rows.last().expect("a terminal row");
    assert_eq!(sender, "surface:deskbar");
    let v: serde_json::Value = serde_json::from_str(body).expect("terminal body is JSON");
    assert_eq!(v["health"], serde_json::json!("disconnected"));
    assert_eq!(v["reason"], serde_json::json!("session closed"));
    assert!(
        v["session"].is_string(),
        "terminal snapshot carries the closing session id"
    );
    assert_eq!(
        v["instances"][0]["instance"],
        serde_json::json!("protobar"),
        "terminal snapshot carries the last-known instances"
    );
    assert_eq!(v["instances"][0]["state"], serde_json::json!("failed"));
}

#[tokio::test]
async fn surface_ws_non_last_session_close_writes_no_terminal_snapshot() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let status_uuid = rig.status_uuid;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));

    // Two sessions on the same surface (under the per-user cap). One reports a
    // live status; the other closes first. Because a session remains attached,
    // the closer is not the last decider and must stamp nothing.
    let mut survivor = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut survivor).await;
    let mut leaver = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut leaver).await;

    // The survivor publishes a live `degraded` status; wait for it to land as a
    // happens-before for the leaver's teardown.
    survivor
        .send(status_frame(
            &[
                instance_report("protobar", "protobar", InstanceState::Failed, 0),
                instance_report("writer", "writer-module", InstanceState::Mounted, 0),
            ],
            7,
        ))
        .await
        .expect("send Status");
    wait_for_channel(&db, status_uuid, |r| {
        r.last().is_some_and(|(_, b)| body_health(b) == "degraded")
    })
    .await;

    // Close the non-last session and give its teardown ample time to run. The
    // retained row must stay `degraded` — a `disconnected` stamp here would
    // clobber a live device's health.
    drop(leaver);
    for _ in 0..50 {
        let rows = read_channel_messages(&db, status_uuid).await;
        assert_eq!(
            rows.last().map(|(_, b)| body_health(b)),
            Some("degraded".to_string()),
            "a non-last session close must not write a terminal snapshot"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Now close the last session; the terminal `disconnected` stamp lands.
    drop(survivor);
    let rows = wait_for_channel(&db, status_uuid, |r| {
        r.last()
            .is_some_and(|(_, b)| body_health(b) == "disconnected")
    })
    .await;
    let (sender, body) = rows.last().expect("a terminal row");
    assert_eq!(sender, "surface:deskbar");
    let v: serde_json::Value = serde_json::from_str(body).expect("terminal body is JSON");
    assert_eq!(v["reason"], serde_json::json!("session closed"));
}

// ===========================================================================
// The confirm-set cap. The set only shrinks at a resume's reconcile, so on a
// connection that never reconnects it — and every cursor carrying it — grows
// with the below-water sends since the last reconcile. Past the soft cap the
// server asks the kernel to re-anchor the subscription, whose resubscribe runs
// the reconcile without waiting for a reconnect.
// ===========================================================================

/// A resume claiming more below-water rows than the soft cap allows puts a
/// `ReAnchor` on the wire for that subscription — exactly one, naming it — and
/// keeps delivering: the cap is a trigger, not a gate.
#[tokio::test]
async fn surface_ws_confirm_set_past_the_soft_cap_asks_the_client_to_re_anchor() {
    let uuid = Uuid::new_v4();
    let db = db::init_db_memory();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Bounded(10), WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Bounded(10)),
        fixture_bus(vec![]),
    )
    .await;
    // One parked row per below-water send, one more than the soft cap admits.
    let below_water = CONFIRM_SET_SOFT_CAP + 1;
    for i in 0..below_water {
        park_durable(&messenger, uuid, &format!("d{i}"), true).await;
    }
    // Lift the channel max above the resume high-water, so the resume is not
    // caught by the stale-store above-max arm.
    for i in 0..4 {
        persist_durable(&messenger, uuid, &format!("tail{i}")).await;
    }
    let high_water = (below_water + 4) as i64;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;
    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, high_water).await),
    ))
    .await
    .expect("subscribe");
    let (replay, _gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay as usize, below_water,
        "every parked row is claimed below the resume high-water"
    );

    let mut delivered = 0usize;
    let mut re_anchors = 0usize;
    for _ in 0..below_water + 1 {
        match next_server_frame(&mut ws).await {
            ServerFrame::Deliver { .. } => delivered += 1,
            ServerFrame::ReAnchor { channel, instance } => {
                re_anchors += 1;
                assert_eq!(channel, DURABLE_ADDR, "the ask names the subscription");
                assert_eq!(instance, COMPONENT);
                assert_eq!(
                    delivered, below_water,
                    "the ask rides behind the delivery that triggered it"
                );
            }
            other => panic!("expected Deliver or ReAnchor, got {other:?}"),
        }
    }
    assert_eq!(
        delivered, below_water,
        "the cap is a trigger, not a gate: every row is still delivered"
    );
    assert_eq!(re_anchors, 1, "exactly one ask while it is outstanding");
}

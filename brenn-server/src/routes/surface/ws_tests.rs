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
use brenn_lib::messaging::db::{insert_message, message_retained_seq, upsert_channels, utc_to_ns};
use brenn_lib::messaging::testutils::ephemeral_channel_entry;
use brenn_lib::messaging::{
    ChannelEntry, ChannelScheme, MessageEnvelope, MessagingDirectory, MessagingGlobalConfig,
    Messenger, ParticipantId, PublishResult, SubscriberEntry, SubscriberEntryKind, Urgency,
    WakeMin, WakeRouter,
};
use brenn_lib::obs::alerting::{
    AlertDispatcher, AlertSeverity as NativeAlertSeverity, make_capturing_alerter_with_severity,
};
use brenn_surface_contract::{ERROR_REPORT_INSTANCE, ERROR_REPORT_PORT};
use brenn_surface_schema::{
    AlertSeverity, BatchEntry, ClientFrame, Cursor, DeliverTarget, GapInfo, GapReason,
    InstanceReport, InstanceState, LogLevel, MAX_ALERT_BODY_BYTES, MAX_ALERT_TITLE_BYTES,
    OverlayReport, PublishBatchOutcome, PublishOutcome, ServerFrame, StatusCounters,
    SubscribeOutcome, max_client_frame_bytes,
};

use brenn_lib::messaging::store::ResumeCursor;

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
    assert_no_alerts, brenn_channel_entry, declare_channels, deskbar_context_feed, deskbar_sub,
    durable_resume, fixture_stores, install_surface_runtimes, publish, publish_as,
    subscribe_harness, subscribe_policy, surface_harness, surface_harness_with_durable,
};
use super::{MAX_SESSIONS_PER_SURFACE, MAX_SESSIONS_PER_USER_PER_SURFACE};
use crate::active_bridge::ActiveBridges;
use crate::bootstrap::messaging::{
    inject_surface_error_grant, inject_surface_geometry_status_grants,
};
use crate::messaging_router::WakeRouterImpl;
use crate::state::AppState;
use crate::test_support::http::{
    TEST_USERNAME, TestServer, assert_stale_client_close_and_no_alert, http_to_ws_url,
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

/// The retention position a delivery's cursor carries.
fn deliver_seq(target: &DeliverTarget) -> u64 {
    cursor::parse(&target.cursor)
        .unwrap_or_else(|e| panic!("a server-minted cursor parses: {e:?}"))
        .resume
        .seq
}

/// A `deskbar` surface with one ephemeral subscription binding and the given
/// access list (empty ⇒ any authenticated user).
fn deskbar(allowed_users: Vec<String>) -> ResolvedSurface {
    SurfaceFixture::new("deskbar", COMPONENT)
        .subscribe(EPH_ADDR, COMPONENT, PORT)
        .allowed_users(allowed_users)
        .build()
}

/// Capturing-alerter test state with the given surface installed over a registry
/// with no channels. Heartbeat is 1 s (via `AppState::for_test`) so liveness tests run
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
    // Barrier channel so `drain_barrier` (an ephemeral no-op publish) resolves
    // for any severity test that uses it.
    let entries = vec![ephemeral_channel_entry(BARRIER_EPH_NAME, 0)];
    let stores = fixture_stores(&entries);
    let messenger = super::test_fixtures::fixture_messenger(
        db,
        &entries,
        &resolved,
        stores,
        Arc::new(WakeRouterImpl::new(ActiveBridges::new())),
    );
    state.surfaces = Arc::new(install_surface_runtimes(
        vec![resolved],
        Some(messenger),
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

/// The next non-heartbeat server frame, or `None` once the socket has been quiet
/// for a beat — for the tests whose subject is what does *not* arrive.
async fn try_next_server_frame(ws: &mut SurfaceWs) -> Option<ServerFrame> {
    loop {
        let quiet = tokio::time::Duration::from_millis(750);
        match tokio::time::timeout(quiet, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let frame: ServerFrame =
                    serde_json::from_str(t.as_str()).expect("server frame parses");
                if matches!(frame, ServerFrame::Heartbeat) {
                    continue;
                }
                return Some(frame);
            }
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            Err(_) => return None,
            other => panic!("expected a server frame or quiet, got {other:?}"),
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
            // recover the (epoch, ring seq) the delivery carries.
            match cursor::parse(&target.cursor) {
                Ok(state) => {
                    assert_eq!(state.resume.seq, seq);
                    assert_eq!(state.resume.epoch, epoch);
                }
                other => panic!("expected a parseable cursor, got {other:?}"),
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
        messenger: _,
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
        messenger: _,
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
        messenger: _,
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
        messenger: _,
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
    let SurfaceTestHarness {
        state,
        alerts,
        messenger: _,
        ..
    } = surface_state(&db, deskbar(vec![]));
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, "/surface/deskbar/ws");
    let msg = ws_connect_first_frame(&ws_url, &token).await;
    assert_stale_client_close_and_no_alert(msg, &alerts, "surface missing build").await;
}

#[tokio::test]
async fn surface_ws_matching_build_welcome_is_first_frame() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        messenger: _,
        ..
    } = surface_state(&db, deskbar(vec![]));
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
        messenger: _,
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
        messenger: _,
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
        messenger: _,
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
        messenger: _,
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
        messenger: _,
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
        messenger: _,
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
        messenger: _,
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
    let SurfaceTestHarness {
        state,
        messenger: _,
        ..
    } = surface_state(&db, deskbar(vec![]));
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
    let SurfaceTestHarness {
        state,
        messenger: _,
        ..
    } = surface_state(&db, deskbar(vec![]));
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
    let SurfaceTestHarness {
        state,
        stores: _,
        messenger,
        ..
    } = subscribe_harness(&db, 0);
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
    publish_as(&messenger, "flood-a", EPH_ADDR, &big, 200).await;
    publish_as(&messenger, "flood-b", EPH_ADDR, &big, 200).await;
    publish_as(&messenger, "flood-c", EPH_ADDR, &big, 200).await;

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
            abi: brenn_surface_schema::Abi::Dom,
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
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = surface_harness(&db, surface, vec![ephemeral_channel_entry(EPH_NAME, 0)]);
    let epoch = stores.epoch();
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

    publish_eph(&messenger, EPH_ADDR, "hello-both").await;

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
                    matches!(cursor::parse(&target.cursor), Ok(state) if state.resume.epoch == epoch),
                    "an ephemeral delivery carries the store epoch: {:?}",
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

/// Coalescing is `(channel, retention position)` grouping, so a sibling standing
/// at a different position takes its own frame.
///
/// Driven by a mid-stream subscribe: alice alone is subscribed for M1, so M1 is
/// hers single-target; bob's own subscribe replays it to him, also
/// single-target, which is what brings the two to the same position; M2 then
/// coalesces into one two-target frame. Every target's `seq` is its own span's,
/// which coalescing folds no part of.
#[tokio::test]
async fn siblings_coalesce_only_at_a_shared_position() {
    let db = db::init_db_memory();
    let mut surface = deskbar_sub();
    surface.components.extend(
        ["agenda-alice", "agenda-bob"].map(|instance| ResolvedComponent {
            instance: instance.to_string(),
            kind: "agenda".to_string(),
            abi: brenn_surface_schema::Abi::Dom,
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
    let SurfaceTestHarness {
        state,
        stores: _,
        messenger,
        ..
    } = surface_harness(&db, surface, vec![ephemeral_channel_entry(EPH_NAME, 4)]);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let mut ws = open_deskbar(&base, &token).await;
    ws.send(subscribe_frame_as(EPH_ADDR, "agenda-alice", None))
        .await
        .expect("send Subscribe");
    next_subscribe_result(&mut ws, EPH_ADDR, "agenda-alice").await;

    publish_eph(&messenger, EPH_ADDR, "m1").await;
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
    let (replay, _) = next_subscribe_result(&mut ws, EPH_ADDR, "agenda-bob").await;
    assert_eq!(replay, 1, "bob's own subscribe serves him the retained M1");
    // Bob's replay is his alone: a replay is a subscription's own answer and can
    // never share a frame with a sibling's live copy.
    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            envelope, targets, ..
        } => {
            assert_eq!(envelope.body, "m1");
            let target = sole_target(&targets);
            assert_eq!(target.instance, "agenda-bob");
            assert_eq!(target.seq, 1, "bob's span starts at his own subscribe");
        }
        other => panic!("expected Deliver, got {other:?}"),
    }

    publish_eph(&messenger, EPH_ADDR, "m2").await;
    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            envelope, targets, ..
        } => {
            assert_eq!(envelope.body, "m2");
            assert_eq!(
                targets.len(),
                2,
                "both stand at the same position, so M2 coalesces: {targets:?}"
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
            assert_eq!(bob.seq, 2, "bob's span counted his replayed M1 then M2");
        }
        other => panic!("expected Deliver, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Bus-plane: slow-client drop accounting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_slow_client_recovers_its_suffix_from_retention() {
    let db = db::init_db_memory();
    // A retained ring and a client that does not read: the session's push queue
    // fills, the router's per-delivery nudge fires, and the drain re-reads the
    // channel's retention above the subscription's position. Loss is real only
    // where retention no longer covers the span, and that is exactly what rides
    // `Deliver.dropped`.
    const RETAIN: u64 = 8;
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = subscribe_harness(&db, RETAIN);
    let (token, _) = setup_authenticated_user(&db).await;
    let epoch = stores.epoch();
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

    // Flood while the client does not read. 60 KB bodies fill socket/queue
    // buffers so the loss is real backpressure, not merely cooperative
    // scheduling; split across three senders as the fixture's publishers.
    const FLOOD: u64 = 600;
    let big = "x".repeat(60_000);
    publish_as(&messenger, "flood-a", EPH_ADDR, &big, 200).await;
    publish_as(&messenger, "flood-b", EPH_ADDR, &big, 200).await;
    publish_as(&messenger, "flood-c", EPH_ADDR, &big, 200).await;

    // Drain: read every Deliver up to and including the newest seq. The drain
    // always reaches it — the last publish nudges, and retention holds the tail.
    // Each delivery's `dropped` is exactly the seq gap since the previous one:
    // the suffix cut the drain made, or zero on a contiguous live row.
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
                    Ok(state) => {
                        assert_eq!(state.resume.epoch, epoch);
                        state.resume.seq
                    }
                    other => panic!("expected a parseable cursor, got {other:?}"),
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
        "a {RETAIN}-deep ring flooded with {FLOOD} messages past an unread socket must lose \
         spans retention no longer covers"
    );
    assert_eq!(
        prev, FLOOD,
        "the drain always ends at the newest retained seq"
    );
}

/// The context-feed counterpart of the test above, and the one place the two
/// halves of the depth-0 contract are visible at once: a fold-0 subscription's
/// rows still reach the page — they are the page ring's diet, and `retain_depth`
/// bounds page memory, not the wire — but never a drop count, because no push
/// window exists behind them to overflow.
///
/// The same flood, so the loss is real: a fold-0 subscription is live-or-nothing
/// (its rows take the row-less context feed, which fires no drain nudge), so a
/// full session queue costs it those rows outright. The wire's silence about
/// that is the point — on a context-only subscription loss surfaces, if at all,
/// as thinner retained context, never as `Deliver.dropped`.
#[tokio::test]
async fn surface_ws_context_feed_delivers_rows_but_never_reports_drops() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = surface_harness(
        &db,
        deskbar_context_feed(),
        vec![ephemeral_channel_entry(EPH_NAME, 8)],
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
    publish_as(&messenger, "flood-a", EPH_ADDR, &big, 200).await;
    publish_as(&messenger, "flood-b", EPH_ADDR, &big, 200).await;
    publish_as(&messenger, "flood-c", EPH_ADDR, &big, 200).await;

    // Read until the socket goes quiet: a fold-0 subscription is owed no tail, so
    // "the last one arrives" is not a property to assert here. What every frame
    // that *does* arrive must carry is `dropped = 0`.
    let mut delivered = 0u64;
    let mut prev = 0u64;
    while let Some(frame) = try_next_server_frame(&mut ws).await {
        match frame {
            ServerFrame::Deliver { targets, .. } => {
                let target = sole_target(&targets);
                let Ok(CursorState { resume, .. }) = cursor::parse(&target.cursor) else {
                    panic!("expected a parseable cursor, got {:?}", target.cursor)
                };
                assert!(
                    resume.seq > prev,
                    "positions advance: {prev} then {}",
                    resume.seq
                );
                assert_eq!(
                    target.dropped, 0,
                    "a context feed has no push window, so nothing may be reported dropped \
                     (seq {}, gap since {prev})",
                    resume.seq
                );
                prev = resume.seq;
                delivered += 1;
            }
            other => panic!("expected Deliver, got {other:?}"),
        }
    }
    assert!(
        delivered > 0,
        "a context feed still receives rows; only its drop accounting is silent"
    );
    // The premise the `dropped = 0` assertions rest on: the flood really did
    // outrun the reader, so rows this connection never heard about exist. Both
    // witnesses are needed — `delivered < FLOOD` says the wire fell behind, and
    // the ring's own newest seq says the rows it fell behind on were committed
    // rather than never published.
    assert!(
        delivered < FLOOD,
        "the flood must outrun the reader for the silence to be about anything: \
         {delivered} of {FLOOD} delivered"
    );
    assert_eq!(
        stores
            .get_by_address(EPH_ADDR)
            .expect("the fixture declares the ephemeral channel")
            .newest_seq(),
        FLOOD,
        "every flooded row entered retention; the wire simply never mentioned most of them"
    );
}

/// The half of the depth-0 contract the live path cannot reach: a **drained**
/// context feed. `drain_channel` reports a retention gap as `dropped` only on a
/// subscription that has a push window, and a fold-0 subscription fires no drain
/// nudge of its own — so the drain reaches one only beside a push-enabled
/// sibling, since any nudge drains every active subscription. That is how
/// production reaches it, and it is how this test does.
///
/// The gap is proved, not assumed: nothing publishes on the context channel after
/// the socket goes quiet, so every frame that arrives afterwards came from the
/// drain the ticker's nudge fired, and its first seq sits far above the position
/// the connection had reached. Contiguous replay would resume at that position;
/// only the gap arm's suffix cut can jump.
#[tokio::test]
async fn surface_ws_a_drained_context_feed_reports_no_drop_across_its_gap() {
    /// The push-enabled sibling: its delivery is what nudges the drain.
    const TICKER_NAME: &str = "ticker-demo";
    const TICKER_ADDR: &str = "ephemeral:ticker-demo";
    const CONTEXT_PORT: &str = "context";
    const RETAIN: u64 = 8;
    /// Well past the session's `PUSH_QUEUE_FRAMES` and anything the socket
    /// buffers, so the tail of the flood is dropped rather than queued and the
    /// connection's position is left below the newest row.
    const FLOOD: u64 = 600;

    let db = db::init_db_memory();
    let surface = SurfaceFixture::new("deskbar", COMPONENT)
        .subscribe(TICKER_ADDR, COMPONENT, PORT)
        .subscribe_at_depths(EPH_ADDR, COMPONENT, CONTEXT_PORT, 0, 4)
        .policy(subscribe_policy(&[TICKER_NAME, EPH_NAME]))
        .build();
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = surface_harness(
        &db,
        surface,
        vec![
            ephemeral_channel_entry(EPH_NAME, RETAIN),
            ephemeral_channel_entry(TICKER_NAME, RETAIN),
        ],
    );
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    for channel in [TICKER_ADDR, EPH_ADDR] {
        ws.send(subscribe_frame(channel, None))
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
    }

    // Flood the context channel past its 4-deep clamp and past the channel's own
    // retention, with bodies large enough that the loss is real backpressure.
    let big = "x".repeat(60_000);
    publish_as(&messenger, "flood", EPH_ADDR, &big, FLOOD as usize).await;

    // Read to quiet. Where the connection's context position lands is the
    // session's scheduling business; what matters is that it is far below the
    // newest row, which the flood guarantees.
    let mut position = 0u64;
    while let Some(frame) = try_next_server_frame(&mut ws).await {
        match frame {
            ServerFrame::Deliver {
                channel, targets, ..
            } => {
                assert_eq!(
                    channel, EPH_ADDR,
                    "only the context channel has traffic yet"
                );
                position = deliver_seq(sole_target(&targets));
            }
            other => panic!("expected Deliver, got {other:?}"),
        }
    }
    assert!(
        position + 1 < FLOOD,
        "the flood must leave the context position behind for there to be a gap to drain \
         (position {position}, newest {FLOOD})"
    );

    // One row on the sibling: it is delivered, and its delivery nudges the
    // session, which drains *every* active subscription — the context feed
    // included.
    publish_as(&messenger, "tick", TICKER_ADDR, "tick", 1).await;

    let mut ticks = 0u64;
    let mut drained: Vec<(u64, u64)> = Vec::new();
    while let Some(frame) = try_next_server_frame(&mut ws).await {
        match frame {
            ServerFrame::Deliver {
                channel, targets, ..
            } => {
                let target = sole_target(&targets);
                if channel == TICKER_ADDR {
                    ticks += 1;
                } else {
                    assert_eq!(channel, EPH_ADDR, "no third channel is subscribed");
                    drained.push((deliver_seq(target), target.dropped));
                }
            }
            other => panic!("expected Deliver, got {other:?}"),
        }
    }

    assert_eq!(ticks, 1, "the sibling's one row is delivered");
    let (first_seq, _) = *drained
        .first()
        .expect("the drain serves the context feed the suffix retention still covers");
    assert!(
        first_seq > position + 1,
        "the drained suffix must skip the span retention no longer covers, or there was no gap \
         to report on: position {position}, first drained seq {first_seq}"
    );
    for (seq, dropped) in &drained {
        assert_eq!(
            *dropped, 0,
            "a context feed has no push window, so a drain gap may not be reported as dropped \
             (seq {seq})"
        );
    }
    assert_eq!(
        drained.last().expect("non-empty").0,
        stores
            .get_by_address(EPH_ADDR)
            .expect("the fixture declares the context channel")
            .newest_seq(),
        "the drain ends at the newest retained seq"
    );
}

// ---------------------------------------------------------------------------
// Bus-plane: Subscribe + delivery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_subscribe_fresh_replays_retained_ring() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = subscribe_harness(&db, 4);
    let (token, _) = setup_authenticated_user(&db).await;
    // Publish two before anyone connects: the retained ring holds them.
    publish(&messenger, "first").await;
    publish(&messenger, "second").await;
    let epoch = stores.epoch();
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
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = subscribe_harness(&db, 0);
    let (token, _) = setup_authenticated_user(&db).await;
    let epoch = stores.epoch();
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
    publish(&messenger, "live").await;
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
        messenger: _,
        ..
    } = subscribe_harness(&db, 4);
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
            abi: brenn_surface_schema::Abi::Dom,
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
        wire_subscriptions: vec![],
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
    // Both channels exist in the registry, so `otherbar-only` is genuinely
    // "exists as a channel AND in another surface's config, but not in deskbar's
    // map" — the real exists-but-not-yours probe. `pure-fiction` exists nowhere.
    // Any code path that consulted channel existence to answer differently would
    // diverge between the two inputs and fail the close-shape assertion below.
    let entries = vec![
        ephemeral_channel_entry(EPH_NAME, 4),
        ephemeral_channel_entry(OTHERBAR_NAME, 4),
    ];
    let messenger = super::test_fixtures::fixture_messenger(
        &db,
        &entries,
        &deskbar_sub(),
        fixture_stores(&entries),
        Arc::new(WakeRouterImpl::new(ActiveBridges::new())),
    );
    // `deskbar` binds EPH_ADDR; `otherbar` binds OTHERBAR_ADDR. From a deskbar
    // session the latter is a channel absent from deskbar's own
    // `subscription_channels` — indistinguishable, in the single fail-closed
    // lookup, from a channel bound nowhere.
    state.surfaces = Arc::new(install_surface_runtimes(
        vec![deskbar_sub(), otherbar()],
        Some(messenger),
        TEST_MAX_BODY_BYTES,
        None,
        crate::test_support::surface::description_params(),
    ));
    let dispatcher = state.alert_dispatcher.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // Input 1: bound to a *different* surface (exists in config and as a channel).
    let close_bound_elsewhere = subscribe_and_observe_close(&base, &token, OTHERBAR_ADDR).await;
    // Input 2: bound to no surface at all, and absent from the registry.
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
        messenger: _,
        ..
    } = subscribe_harness(&db, 0);
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

/// A resume position above anything the channel ever assigned is answered as a
/// fresh attach under `EpochChanged`, on every channel — the connection
/// survives. One answer for both classes: a durable store restored from backup
/// legitimately produces it, and the response (the retained window) is what a
/// bare re-subscribe would give anyway.
#[tokio::test]
async fn surface_ws_subscribe_resume_ahead_is_a_fresh_attach() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        stores,
        messenger,
        ..
    } = subscribe_harness(&db, 4);
    let (token, _) = setup_authenticated_user(&db).await;
    publish(&messenger, "one").await;
    let epoch = stores.epoch();
    let resume = Some(cursor::mint(0, ResumeCursor { epoch, seq: 999 }));
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    ws.send(subscribe_frame(EPH_ADDR, resume))
        .await
        .expect("send Subscribe");
    match next_server_frame(&mut ws).await {
        ServerFrame::SubscribeResult {
            outcome,
            replay_count,
            gap,
            ..
        } => {
            assert!(matches!(outcome, SubscribeOutcome::Ok));
            assert_eq!(replay_count, 1, "the whole retained ring is replayed");
            assert_eq!(
                gap.expect("a resume ahead gaps").reason,
                GapReason::EpochChanged,
                "answered as a fresh attach",
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
    assert_no_alerts(&flusher, &alerts, "resume ahead is not a violation").await;
}

// ---------------------------------------------------------------------------
// Bus-plane: resume / gap mapping
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_subscribe_resume_exact_replays_tail() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = subscribe_harness(&db, 4);
    let (token, _) = setup_authenticated_user(&db).await;
    publish(&messenger, "one").await;
    publish(&messenger, "two").await;
    publish(&messenger, "three").await;
    let epoch = stores.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Resume from seq 1: seqs 2 and 3 are owed and within the ring → Replay::Exact,
    // no gap.
    ws.send(subscribe_frame(
        EPH_ADDR,
        Some(cursor::mint(0, ResumeCursor { epoch, seq: 1 })),
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
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = subscribe_harness(&db, 4);
    let (token, _) = setup_authenticated_user(&db).await;
    publish(&messenger, "one").await;
    publish(&messenger, "two").await;
    let epoch = stores.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Resume from the newest seq: caught up → Replay::UpToDate, nothing replayed,
    // no gap.
    ws.send(subscribe_frame(
        EPH_ADDR,
        Some(cursor::mint(0, ResumeCursor { epoch, seq: 2 })),
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
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = subscribe_harness(&db, 1);
    let (token, _) = setup_authenticated_user(&db).await;
    publish(&messenger, "one").await;
    publish(&messenger, "two").await;
    publish(&messenger, "three").await;
    let epoch = stores.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // Resume from seq 1 but the ring only retains seq 3 → Gap(BeyondRetained)
    // with a full-ring replay.
    ws.send(subscribe_frame(
        EPH_ADDR,
        Some(cursor::mint(0, ResumeCursor { epoch, seq: 1 })),
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
                        reason: GapReason::BeyondRetained
                    })
                ),
                "expected BeyondRetained, got {gap:?}"
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
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = subscribe_harness(&db, 4);
    let (token, _) = setup_authenticated_user(&db).await;
    publish(&messenger, "one").await;
    publish(&messenger, "two").await;
    let epoch = stores.epoch();
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    // A resume epoch that doesn't match the store (e.g. a pre-restart token) →
    // Gap(EpochChanged) with a full-ring replay. Deliveries carry the live
    // epoch, not the stale resume epoch.
    ws.send(subscribe_frame(
        EPH_ADDR,
        Some(cursor::mint(
            0,
            ResumeCursor {
                epoch: Uuid::new_v4(),
                seq: 1,
            },
        )),
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
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        messenger: _,
        ..
    } = subscribe_harness(&db, 0);
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
    assert_no_alerts(&flusher, &alerts, "Unsubscribe of an active channel").await;
}

#[tokio::test]
async fn surface_ws_unsubscribe_not_subscribed_is_violation() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        messenger: _,
        ..
    } = subscribe_harness(&db, 4);
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
/// fixture. Deliberately not `Normal`, so a test asserting a persisted row's
/// urgency proves the port's configured default was actually resolved and applied.
const DUR_OUT_DEFAULT_URGENCY: Urgency = Urgency::Low;

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

/// The publish fixture: the surface and the channels its bindings obligate,
/// which travel together so a rig cannot install one without the other.
struct DeskbarPubFixture {
    surface: ResolvedSurface,
    /// `EPH_ADDR` and `DUR_OUT_ADDR` — the set a rig must declare, the durable
    /// half of which owes a DB row (`declare_channels`).
    entries: Vec<ChannelEntry>,
    /// `DUR_OUT_ADDR`'s channel uuid, for reading its persisted rows back.
    dur_out_uuid: Uuid,
}

/// A `deskbar` surface wired for Publish: an ephemeral output port
/// (`writer`/`out` → `EPH_ADDR`), a durable output port (`writer`/`durable` →
/// `DUR_OUT_ADDR`), and an ephemeral subscription on `EPH_ADDR` so a second
/// session can observe the published message. Rate caps are parameterized so
/// flood/no-token-consumed tests can pin small budgets.
///
/// The policy authorizes each port through its own scheme's half, and the two
/// halves are disjoint: `writer`/`out` publishes `ephemeral:` and is admitted by
/// `EphemeralPublish` + an `ephemeral_publish` matcher on `EPH_NAME` alone;
/// `writer`/`durable` publishes `brenn:` and is admitted by `MessagingPublish` +
/// a `brenn_publish` matcher on `DUR_OUT_NAME` alone. Neither channel appears in
/// the other's matcher list, so a gate that read the grant sets as a union would
/// still be denied here on the ACL. That per-scheme split is pinned where it
/// belongs, at the publish ladder
/// (`brenn-lib` `publish::tests::scheme_parity`), not by these WS tests.
///
/// A surface's transportable outputs must name declared channels, so the
/// channel set comes with the surface: a rig that declares `entries` gives both
/// output bindings a directory entry and a store, which is what the deferred-view
/// seeding pass requires of a bound output. One uuid per fixture means the
/// durable channel has one identity per rig.
///
/// The policy covers every output binding — the shape boot demands, enforced by
/// `fixture_messenger` once each rig's substrate grants are injected. Rigs
/// simulating a substrate injection must add their grant through the production
/// injector, not replace the policy: boot keeps the subscriber registration
/// identical to the surface policy.
fn deskbar_pub_fixture(publish_burst: u32, publish_per_sec: u32) -> DeskbarPubFixture {
    let dur_out_uuid = Uuid::new_v4();
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::EphemeralPublish);
    policy.grants.insert(AppCapability::EphemeralSubscribe);
    policy.grants.insert(AppCapability::MessagingPublish);
    policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact(EPH_NAME.to_string())];
    policy.acls.ephemeral_subscribe = vec![ChannelMatcher::Exact(EPH_NAME.to_string())];
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Exact(DUR_OUT_NAME.to_string()));
    let surface = ResolvedSurface {
        slug: "deskbar".to_string(),
        skin: "bench".to_string(),
        components: vec![
            ResolvedComponent {
                instance: COMPONENT.to_string(),
                kind: COMPONENT.to_string(),
                abi: brenn_surface_schema::Abi::Dom,
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
                abi: brenn_surface_schema::Abi::Dom,
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
        wire_subscriptions: vec![],
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
                // Deliberately *not* `Normal`: the persisted-row assertions would
                // pass either way at `Normal` and could not tell "the port's
                // default was applied" from "a hard-coded constant".
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
    };
    DeskbarPubFixture {
        surface,
        entries: vec![
            ephemeral_channel_entry(EPH_NAME, 0),
            brenn_channel_entry(DUR_OUT_NAME, dur_out_uuid),
        ],
        dur_out_uuid,
    }
}

/// Capturing-alerter harness whose `deskbar` surface is the publish fixture
/// (`retain_depth 0`: no ring, so a subscriber sees only live deliveries).
///
/// Returns the whole harness — its `flusher` is what lets a caller barrier the
/// alert drainer before reading `alerts` — and the durable output channel's UUID,
/// for the tests that read `brenn:writer-out`'s persisted rows back.
async fn publish_state(
    db: &db::Db,
    publish_burst: u32,
    publish_per_sec: u32,
) -> (SurfaceTestHarness, Uuid) {
    let fixture = deskbar_pub_fixture(publish_burst, publish_per_sec);
    let dur_out_uuid = fixture.dur_out_uuid;
    let harness = surface_harness_with_durable(db, fixture.surface, fixture.entries).await;
    (harness, dur_out_uuid)
}

/// Publish-fixture state wired for the reserved error-report port: the error
/// channel `brenn:surface-errors` is declared, `deskbar`'s policy carries the
/// substrate-injected error-channel grant, and `install_surface_runtimes` binds
/// the reserved `#brenn`/`error-reports` port with a `warn` floor. Returns the
/// error channel UUID so the persisted report can be read back.
async fn error_report_publish_state(db: &db::Db) -> (AppState, Uuid) {
    let (mut state, _alerts, _handle) = test_state_with_capturing_alerter(db);

    let channel_uuid = Uuid::new_v4();
    let fixture = deskbar_pub_fixture(60, 1);

    let mut surfaces = vec![fixture.surface];
    inject_surface_error_grant(&mut surfaces, "surface-errors");

    let mut entries = vec![brenn_channel_entry("surface-errors", channel_uuid)];
    entries.extend(fixture.entries);
    let stores = declare_channels(db, &entries).await;
    let router = Arc::new(WakeRouterImpl::new(ActiveBridges::new()));
    let messenger =
        super::test_fixtures::fixture_messenger(db, &entries, &surfaces[0], stores, router.clone());

    state.surfaces = Arc::new(install_surface_runtimes(
        surfaces,
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
                deliver_after: None,
            })
            .collect(),
        deferred_ops: Vec::new(),
    };
    Message::Text(serde_json::to_string(&frame).expect("serialize").into())
}

/// A `PublishBatch` frame carrying one scheduled entry: the flush shape that
/// parks rather than publishes.
fn deferred_batch_frame(
    instance: &str,
    correlation: u64,
    port: &str,
    body: &str,
    deliver_after: u64,
) -> Message {
    let frame = ClientFrame::PublishBatch {
        instance: instance.to_string(),
        correlation,
        publishes: vec![BatchEntry {
            port: port.to_string(),
            body: body.to_string(),
            urgency: None,
            deliver_after: Some(deliver_after),
        }],
        deferred_ops: Vec::new(),
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
    let (
        SurfaceTestHarness {
            state,
            alerts,
            flusher,
            stores,
            ..
        },
        _,
    ) = publish_state(&db, 60, 1).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let epoch = stores.epoch();
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

    // The observed `Deliver` is the happens-before edge; the flush barrier then
    // makes any alert the server already dispatched visible to the read.
    assert_no_alerts(&flusher, &alerts, "a successful Publish").await;
}

#[tokio::test]
async fn surface_ws_publish_durable_output_persists_and_stays_open() {
    let db = db::init_db_memory();
    let (
        SurfaceTestHarness {
            state,
            alerts,
            flusher,
            ..
        },
        channel_uuid,
    ) = publish_state(&db, 60, 1).await;
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
    // The observed heartbeat is the happens-before edge; the flush barrier then
    // makes any alert the server already dispatched visible to the read.
    assert_no_alerts(&flusher, &alerts, "a successful durable Publish").await;
}

/// A durable schedule outlives the connection that made it, so a page that
/// arrives afterwards has to be *told* what it has parked — and told before it
/// can act. The seeding pass rides the same FIFO writer queue `Welcome` just
/// entered, which is what puts the view immediately behind it: end to end, the
/// frame the page reads after `Welcome` is the snapshot of its own parked set.
#[tokio::test]
async fn surface_ws_a_fresh_session_is_seeded_with_what_is_still_parked() {
    let db = db::init_db_memory();
    let (SurfaceTestHarness { state, .. }, _channel_uuid) = publish_state(&db, 60, 1).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    // On a whole second: a durable row keeps its release time to the second, so
    // this is the value that comes back in the view.
    let release_at = u64::try_from((Utc::now() + chrono::Duration::hours(1)).timestamp_millis())
        .expect("a positive epoch")
        / 1_000
        * 1_000;

    let mut first = open_deskbar(&base, &token).await;
    first
        .send(deferred_batch_frame(
            "writer",
            5,
            "durable",
            "scheduled",
            release_at,
        ))
        .await
        .expect("send PublishBatch");
    // The park's own view push and the batch result race on the wire; the result is
    // the happens-before edge the second session needs.
    loop {
        match next_server_frame(&mut first).await {
            ServerFrame::PublishBatchResult {
                correlation,
                outcome,
            } => {
                assert_eq!(correlation, 5);
                assert_eq!(outcome, PublishBatchOutcome::Ok, "the entry parked");
                break;
            }
            ServerFrame::DeferredView { .. } => continue,
            other => panic!("expected the batch result, got {other:?}"),
        }
    }

    // A second session, seeded from the truth rather than from anything the first
    // one said.
    let url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut second = surface_ws_open(&url, &token).await;
    match next_server_frame(&mut second).await {
        ServerFrame::Welcome { surface, .. } => assert_eq!(surface, "deskbar"),
        other => panic!("expected Welcome first, got {other:?}"),
    }
    match next_server_frame(&mut second).await {
        ServerFrame::DeferredView {
            channel,
            instance,
            entries,
        } => {
            assert_eq!(channel, DUR_OUT_ADDR);
            assert_eq!(instance, "writer");
            assert_eq!(
                entries
                    .iter()
                    .map(|e| (e.body.as_str(), e.deliver_after))
                    .collect::<Vec<_>>(),
                vec![("scheduled", release_at)],
                "the seed is the backend's own recompute, body and release time"
            );
        }
        other => panic!("expected the seeded DeferredView behind Welcome, got {other:?}"),
    }
}

/// The per-connection publish bucket gates the durable arm exactly as it does the
/// ephemeral arm: with burst 1, the first durable publish spends the only token
/// and the second is `RateLimited` (never a kill), and only the first persists.
#[tokio::test]
async fn surface_ws_publish_durable_output_rate_limited() {
    let db = db::init_db_memory();
    let (SurfaceTestHarness { state, .. }, channel_uuid) = publish_state(&db, 1, 1).await;
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
    let (
        SurfaceTestHarness {
            state,
            alerts,
            flusher,
            ..
        },
        _,
    ) = publish_state(&db, 1, 1).await;
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

    assert_no_alerts(&flusher, &alerts, "an oversized Publish").await;
}

#[tokio::test]
async fn surface_ws_persistent_oversized_body_escalates_to_violation() {
    let db = db::init_db_memory();
    // Generous burst so the interleaved valid publish is never rate-limited;
    // oversized rejects consume no token, so only the valid publish needs one.
    let (
        SurfaceTestHarness {
            state,
            alerts,
            flusher,
            ..
        },
        _,
    ) = publish_state(&db, 8, 8).await;
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
    let (
        SurfaceTestHarness {
            state,
            alerts,
            flusher,
            ..
        },
        _,
    ) = publish_state(&db, 2, 1).await;
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

    assert_no_alerts(&flusher, &alerts, "a rate-limited Publish flood").await;
}

#[tokio::test]
async fn surface_ws_publish_unbound_port_is_violation() {
    let db = db::init_db_memory();
    let (
        SurfaceTestHarness {
            state,
            alerts,
            flusher,
            ..
        },
        _,
    ) = publish_state(&db, 60, 1).await;
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
// `AppState` (shared `surface_registry`), so a commit's surface fan-out reaches
// the attached WS sessions exactly as in production. Retained-but-unfed rows are
// inserted straight into the DB (the publish-while-detached and retained-window
// sources); the drain and resume machinery under test then reads them back.
// ===========================================================================

/// Bare durable channel name (ACL matcher key) + its canonical address.
const DURABLE_NAME: &str = "durable-demo";
const DURABLE_ADDR: &str = "brenn:durable-demo";

/// The stand-in for "everything the channel holds" in a fixture's wire
/// subscription. Boot proves both depths of every wire subscription bounded, so
/// a fixture asks for a window wider than any test's traffic rather than an
/// unbounded one no config can resolve.
const WIDE_CLAMP_N: u64 = 64;
const WIDE_CLAMP: Depth = Depth::Bounded(WIDE_CLAMP_N);

/// A durable `brenn:durable-demo` channel entry with the given retain depth.
fn durable_channel_entry(uuid: Uuid, retain_depth: Depth) -> ChannelEntry {
    ChannelEntry {
        uuid,
        address: DURABLE_ADDR.to_string(),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
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
///
/// Both the binding and the stated wire subscription carry the caller's depth on
/// both knobs — a disagreement would pin a replay clamp no config resolves.
/// `derive_wire_subscriptions` asserts the agreement.
fn durable_surface(
    uuid: Uuid,
    retain_depth: Depth,
    wake_min: WakeMin,
    allow_delivery: bool,
    extra_eph: Option<&str>,
) -> ResolvedSurface {
    let Depth::Bounded(depth) = retain_depth else {
        panic!("the durable fixture's depth is bounded; boot bounds every wire depth")
    };
    let mut policy = AppPolicy::default();
    if allow_delivery {
        policy.grants.insert(AppCapability::MessagingSubscribe);
        policy.acls.brenn_subscribe = vec![ChannelMatcher::Exact(DURABLE_NAME.to_string())];
    }
    let mut subscriptions = vec![SurfaceBinding {
        channel_address: DURABLE_ADDR.to_string(),
        instance: COMPONENT.to_string(),
        port: PORT.to_string(),
        push_depth: depth,
        retain_depth: depth,
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
            abi: brenn_surface_schema::Abi::Dom,
            send_budget: SurfaceSendBudget::default(),
            parked_batch_depth: 8,
            config: Default::default(),
            chrome: true,
        }],
        local_channels: vec![],
        subscriptions,
        wire_subscriptions: vec![ResolvedSurfaceSubscription {
            instance: COMPONENT.to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: DURABLE_ADDR.to_string(),
                push_depth: retain_depth,
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

/// Add the publish half of the durable channel's authority — the
/// `MessagingPublish` grant plus a `brenn_publish` ACL naming it — to a surface
/// that already subscribes. Opt-in per test, so the cases whose principal must
/// stay receive-only keep it.
fn grant_durable_publish(mut resolved: ResolvedSurface) -> ResolvedSurface {
    resolved
        .policy
        .grants
        .insert(AppCapability::MessagingPublish);
    resolved
        .policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Exact(DURABLE_NAME.to_string()));
    resolved
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
    nondurable: Vec<ChannelEntry>,
) -> (AppState, Arc<Messenger>) {
    // Project the surface's own subscriptions onto the channel entry, as boot
    // does: the commit's surface fan-out resolves its targets from the directory,
    // so a subscription the entry does not carry is fed nothing.
    let mut resolved = resolved;
    crate::test_support::surface::derive_wire_subscriptions(&mut resolved);
    let mut all_entries = vec![channel_entry];
    all_entries.extend(nondurable.iter().cloned());
    crate::test_support::surface::bind_wire_subscription_uuids(&mut resolved, &all_entries);
    let all_entries = super::test_fixtures::project_surface_subscribers(&all_entries, &resolved);
    let channel_entry = all_entries[0].clone();
    {
        let conn = db.lock().await;
        upsert_channels(&conn, std::slice::from_ref(&channel_entry));
    }
    let router = Arc::new(WakeRouterImpl::new(ActiveBridges::new()));
    router.register_surface_delivery_routes(&resolved);
    // Registered as boot does, at every principal grain: a release pass resolves
    // its targets through the same gate the publish path uses, so an
    // unregistered subscriber would be resolved as no target at all.
    let surface_policies =
        std::collections::HashMap::from([(resolved.slug.clone(), resolved.policy.clone())]);
    let surfaces = std::slice::from_ref(&resolved);
    let messenger = Messenger::new(
        db.clone(),
        Arc::new(MessagingDirectory::with_entries(all_entries)),
        Arc::from(TEST_ORIGIN),
        Arc::new(indexmap::IndexMap::new()),
        router.clone() as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(surface_registrations_all_grains(surface_policies, surfaces))
    // The publish path requires every principal grain in the budget map — a
    // missing entry is a broken invariant; a non-publishing principal never draws.
    .with_surface_send_budgets(budget_principals(surfaces))
    .with_ring_stores(fixture_stores(&nondurable));
    let mut state = crate::test_support::state::test_state(db);
    state.messenger = Some(messenger.clone());
    state.surfaces = Arc::new(install_surface_runtimes(
        vec![resolved],
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
/// both directions on the channel. Exercises the self-delivery case end-to-end:
/// publish → retain → the commit's surface fan-out → deliver to its own session.
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
                abi: brenn_surface_schema::Abi::Dom,
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
                abi: brenn_surface_schema::Abi::Dom,
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
            push_depth: WIDE_CLAMP_N,
            retain_depth: WIDE_CLAMP_N,
            noise: NoiseLevel::Silent,
        }],
        wire_subscriptions: vec![ResolvedSurfaceSubscription {
            instance: COMPONENT.to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: DURABLE_ADDR.to_string(),
                push_depth: WIDE_CLAMP,
                retain_depth: WIDE_CLAMP,
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
/// delivery-time gate in `TargetResolver::surface_feed_targets` when it is a
/// channel subscriber. Self-delivery passes one slug holding both ACLs; a
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
    for surface in &surfaces {
        router.register_surface_delivery_routes(surface);
    }
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
    state.surfaces = Arc::new(install_surface_runtimes(
        surfaces,
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
    let (state, _messenger) =
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

    // The commit's fan-out delivered the message live to the subscribed session.
    assert_durable_deliver_to(&mut ws, COMPONENT, "hello-durable", 1).await;
}

/// The durable depth-0 context feed: a fold-0 durable subscription creates
/// **no** `messaging_pending_pushes` row, yet an attached session still receives
/// the message live, as a row-less deliver-if-attached fan-out at publish time.
/// The message persists and is retained, and the commit's fan-out is the whole
/// trigger — nothing delivers it a second time.
///
/// The zero-row assertion is load-bearing and not incidental: nothing on a
/// channel writes to that table any more, so a row appearing here would mean a
/// path started minting per-subscriber delivery records again — behind a
/// disconnected surface, with nothing to reap them. The feed owes a disconnected
/// session nothing — its context arrives at the next subscribe/resume (the
/// paired test below).
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
    resolved.wire_subscriptions[0].subscription.push_depth = Depth::Bounded(0);
    resolved.wire_subscriptions[0].subscription.retain_depth = Depth::Bounded(4);
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

    // Nothing delivers it a second time: the fan-out at the commit is the whole
    // of the trigger, and no row survives it to be re-dispatched.
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
    resolved.wire_subscriptions[0].subscription.push_depth = Depth::Bounded(0);
    resolved.wire_subscriptions[0].subscription.retain_depth = Depth::Bounded(4);
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
                        Ok(CursorState { resume, .. }) if resume.seq == 1
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

    // No row written, and no duplicate delivery.
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
    resolved.wire_subscriptions[0].subscription.push_depth = Depth::Bounded(0);
    let (state, messenger) = durable_rig(
        &db,
        resolved,
        durable_channel_entry(uuid, Depth::Bounded(2)),
        vec![],
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

/// A trigger port — `push_depth >= 1, retain_depth = 0` — is served the rows it
/// missed while detached. The bus admits that binding (wake me on new messages,
/// keep me no context) and retains `max(push, retain)` rows for it, so the wire
/// clamp is the max too: clamping to the stated retain alone would replay an
/// empty window forever, leave the position where it was, and strand the
/// subscription silently.
#[tokio::test]
async fn surface_ws_durable_trigger_port_with_no_retained_context_recovers_its_suffix() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut resolved = durable_surface(uuid, Depth::Bounded(8), WakeMin::Normal, true, None);
    // The deskbar shape: a trigger port stating no retained context at all.
    resolved.subscriptions[0].retain_depth = 0;
    resolved.wire_subscriptions[0].subscription.push_depth = Depth::Bounded(8);
    resolved.wire_subscriptions[0].subscription.retain_depth = Depth::Bounded(0);
    let (state, messenger) = durable_rig(
        &db,
        resolved,
        durable_channel_entry(uuid, Depth::Bounded(8)),
        vec![],
    )
    .await;
    let s1 = persist_durable(&messenger, uuid, "r1").await;
    let s2 = persist_durable(&messenger, uuid, "r2").await;
    let s3 = persist_durable(&messenger, uuid, "r3").await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 3,
        "the clamp is max(push, retain), not retain alone"
    );
    assert!(gap.is_none(), "nothing was lost — the window covers it");
    assert_durable_deliver_to(&mut ws, COMPONENT, "r1", s1).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "r2", s2).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "r3", s3).await;
}

/// Class parity for the trigger port: the ephemeral twin of the case above is
/// served the same window (`push 8, retain 0`).
#[tokio::test]
async fn surface_ws_ephemeral_trigger_port_with_no_retained_context_recovers_its_suffix() {
    let db = db::init_db_memory();
    let SurfaceTestHarness {
        state,
        stores,
        messenger,
        ..
    } = subscribe_harness(&db, 8);
    let (token, _) = setup_authenticated_user(&db).await;
    // `deskbar_sub` binds `push 8, retain 0` — the trigger port, stated.
    publish(&messenger, "first").await;
    publish(&messenger, "second").await;
    let epoch = stores.epoch();
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(EPH_ADDR, None))
        .await
        .expect("subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, EPH_ADDR, COMPONENT).await;
    assert_eq!(
        replay, 2,
        "the clamp is max(push, retain), not retain alone"
    );
    assert!(gap.is_none());
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

/// A subscription whose push window is deeper than its retained context
/// recovers up to its **push** depth — rows the reap frontier already pinned for
/// it, and rows it would have received in full had it stayed connected. Four
/// missed rows behind `push 4, retain 1`: all four, no gap.
#[tokio::test]
async fn surface_ws_durable_resume_serves_up_to_the_push_depth_above_the_retain() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut resolved = durable_surface(uuid, Depth::Bounded(4), WakeMin::Normal, true, None);
    resolved.subscriptions[0].push_depth = 4;
    resolved.subscriptions[0].retain_depth = 1;
    resolved.wire_subscriptions[0].subscription.push_depth = Depth::Bounded(4);
    resolved.wire_subscriptions[0].subscription.retain_depth = Depth::Bounded(1);
    let (state, messenger) = durable_rig(
        &db,
        resolved,
        durable_channel_entry(uuid, Depth::Bounded(8)),
        vec![],
    )
    .await;
    let s1 = persist_durable(&messenger, uuid, "r1").await;
    let s2 = persist_durable(&messenger, uuid, "r2").await;
    let s3 = persist_durable(&messenger, uuid, "r3").await;
    let s4 = persist_durable(&messenger, uuid, "r4").await;
    let s5 = persist_durable(&messenger, uuid, "r5").await;

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
    assert_eq!(replay, 4, "push 4 is the clamp, not retain 1");
    assert!(gap.is_none(), "the push window covers the whole span");
    assert_durable_deliver_to(&mut ws, COMPONENT, "r2", s2).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "r3", s3).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "r4", s4).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "r5", s5).await;
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
            abi: brenn_surface_schema::Abi::Dom,
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
            push_depth: WIDE_CLAMP_N,
            retain_depth: WIDE_CLAMP_N,
            noise: NoiseLevel::Silent,
        })
        .collect();
    resolved.wire_subscriptions = ["agenda-alice", "agenda-bob"]
        .into_iter()
        .map(|instance| ResolvedSurfaceSubscription {
            instance: instance.to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: DURABLE_ADDR.to_string(),
                push_depth: WIDE_CLAMP,
                retain_depth: WIDE_CLAMP,
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
        })
        .collect();

    let surface_policies =
        std::collections::HashMap::from([("deskbar".to_string(), resolved.policy.clone())]);
    let (state, _messenger) =
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

    // One publish, two deliveries — one per principal, from its own window.
    // Sibling targets coalesce into one frame when they are co-available at the
    // write boundary, which is an encoding choice and not a wire guarantee: each
    // principal is resolved separately, so the two can also land as two frames.
    // What is pinned is the delivery set, so collect across frames and sort.
    let mut got: Vec<String> = Vec::new();
    while got.len() < 2 {
        match next_server_frame(&mut ws).await {
            ServerFrame::Deliver {
                channel,
                envelope,
                targets,
            } => {
                assert_eq!(channel, DURABLE_ADDR);
                assert_eq!(envelope.body, "hello-both");
                for target in targets {
                    got.push(target.instance.clone());
                }
            }
            other => panic!("expected Deliver, got {other:?}"),
        }
    }
    got.sort();
    assert_eq!(
        got,
        vec!["agenda-alice".to_string(), "agenda-bob".to_string()],
        "each instance is delivered under its own name — one publish, two \
         independent subscriptions, two windows"
    );
    assert_no_deliver(&mut ws).await;
}

/// The router's fan-out filter keys on the whole subscription, and that is
/// load-bearing rather than defense-in-depth.
///
/// A fan-out that any session accepts reports `Ok(true)`, and the dispatcher
/// retires the row on that report — so the session's own `is_active` drop does
/// not neutralise a misroute; it **consumes** the row. Under a channel-keyed
/// filter: alice is subscribed nowhere, bob is active on session S, alice's row
/// matches S on the channel, is sent to S, and S discards it — alice's row
/// retired and never woken for again.
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
            abi: brenn_surface_schema::Abi::Dom,
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
            push_depth: WIDE_CLAMP_N,
            retain_depth: WIDE_CLAMP_N,
            noise: NoiseLevel::Silent,
        })
        .collect();
    resolved.wire_subscriptions = ["agenda-alice", "agenda-bob"]
        .into_iter()
        .map(|instance| ResolvedSurfaceSubscription {
            instance: instance.to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: DURABLE_ADDR.to_string(),
                push_depth: WIDE_CLAMP,
                retain_depth: WIDE_CLAMP,
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

    // (b) Alice lost nothing: her subscription's cursor is its own, so the message
    //     bob was served is still hers to collect. This is the assertion the
    //     mutation inverts — channel-keyed, bob's session would consume and then
    //     discard her copy, and she could never be sent it again.
    ws.send(subscribe_frame_as(DURABLE_ADDR, "agenda-alice", None))
        .await
        .expect("send Alice's Subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, "agenda-alice")
            .await
            .0,
        1,
        "the late-subscribing instance is served the message from retention"
    );
    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            envelope, targets, ..
        } => {
            assert_eq!(envelope.body, "only-bob-is-here");
            assert_eq!(
                sole_target(&targets).instance,
                "agenda-alice",
                "and it is served under her own name"
            );
        }
        other => panic!("expected Deliver, got {other:?}"),
    }
    let _ = &messenger;
}

/// End-to-end **cross-principal** durable round trip — design §4's named
/// "surface→surface durable round trip live" test. One surface principal
/// (`wallbar`) publishes durably; a *different* subscribed surface principal
/// (`deskbar`) receives the live `Deliver`. This is the case
/// `surface_ws_durable_publish_delivers_to_subscriber` does not cover: that
/// test is a single self-subscribing principal (sender == subscriber), so it
/// cannot rule out a bug specific to *cross-principal* fan-out. Here the
/// publisher is not on the channel's subscriber list at all, proving the commit's
/// surface fan-out reaches a subscriber that is not the sender.
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
    let subscriber = durable_surface(uuid, WIDE_CLAMP, WakeMin::Normal, true, None);
    let subscriber_policy = subscriber.policy.clone();
    // Publisher principal: wallbar, a *distinct* slug granted brenn publish on
    // the same channel. It has no runtime and no subscription — it only holds a
    // surface publish policy on the Messenger. deskbar's policy is installed too
    // so it clears the fan-out's delivery-time ACL gate.
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

    // The commit's surface fan-out delivers it live to deskbar's session.
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

/// Publish `body` onto the durable channel as the `deskbar` surface's own
/// (bare-grain, publisher-only) principal, through the production publish
/// path — attached sessions receive live delivery. Returns the message's
/// retention position.
///
/// Every non-`Ok` outcome is a rig bug — a missing grant, ACL, or budget —
/// and panics rather than being absorbed into a delivery-count mismatch later.
async fn feed_durable(messenger: &Messenger, body: &str) -> i64 {
    let message_id = match messenger
        .publish_from_surface("deskbar", None, DURABLE_ADDR, body, Urgency::Normal)
        .await
    {
        PublishResult::Ok { message_id, .. } => message_id,
        other => panic!("feed_durable expected Ok, got {other:?}"),
    };
    let conn = messenger.db().lock().await;
    message_retained_seq(&conn, message_id)
}

/// Commit `body` onto the durable channel with **no** fan-out: present in the
/// retained window for a resume or a drain, but no session was ever handed it.
/// Returns the message's retention position.
async fn persist_durable(messenger: &Messenger, uuid: Uuid, body: &str) -> i64 {
    let conn = messenger.db().lock().await;
    insert_message(
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
        None,
        utc_to_ns(Utc::now()),
    )
    .retained_seq
    .expect("a committed message holds a retention position")
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
                "retention covered this subscription's span, so nothing was lost"
            );
            match cursor::parse(&target.cursor) {
                Ok(state) => assert_eq!(state.resume.seq, seq as u64, "durable cursor high-water"),
                other => panic!("expected a parseable durable cursor, got {other:?}"),
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
/// position already at or below the subscription's high-water never reaches the
/// wire twice.
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
async fn publish_eph(messenger: &Messenger, addr: &str, body: &str) {
    let participant = ParticipantId::for_surface("eph-pub");
    super::test_fixtures::commit_eph(messenger, addr, &participant, body).await;
}

/// Publish-while-detached: three rows park; on attach + `Subscribe` they drain in
/// seq order (each as `Pos::Durable`) inside the `SubscribeResult` replay.
#[tokio::test]
async fn surface_ws_durable_parked_rows_drain_in_seq_order_on_subscribe() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        grant_durable_publish(durable_surface(
            uuid,
            WIDE_CLAMP,
            WakeMin::Normal,
            true,
            None,
        )),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
    )
    .await;
    let s1 = feed_durable(&messenger, "one").await;
    let s2 = feed_durable(&messenger, "two").await;
    let s3 = feed_durable(&messenger, "three").await;

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

/// Live delivery after subscribe: the commit's fan-out reaches the attached
/// session as a `Pos::Durable` `Deliver`, once. There is no second trigger to be
/// idempotent against any more — the commit fan-out is the only one — so what
/// the trailing silence pins is that the fan-out itself sends one copy per
/// session, not one per resolution pass.
#[tokio::test]
async fn surface_ws_durable_live_delivery_after_subscribe_arrives_once() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        grant_durable_publish(durable_surface(
            uuid,
            WIDE_CLAMP,
            WakeMin::Normal,
            true,
            None,
        )),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
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

    let seq = feed_durable(&messenger, "live").await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "live", seq).await;
    assert_no_deliver(&mut ws).await;
}

/// A quiet parked row (non-eager, so never dispatchable and never eager-woken)
/// stays put until a louder live delivery arrives. That louder row sits two
/// positions above the session's high-water, so its live copy is dropped and the
/// subscription is served its whole suffix from retention instead — the gap-heal
/// arm. Both rows reach the wire, in seq order, exactly once: the quiet one the
/// live path skipped, then the loud one that exposed it.
#[tokio::test]
async fn surface_ws_durable_live_copy_above_the_high_water_heals_the_interior_gap() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        grant_durable_publish(durable_surface(
            uuid,
            WIDE_CLAMP,
            WakeMin::Normal,
            true,
            None,
        )),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
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
    let quiet_seq = persist_durable(&messenger, uuid, "quiet").await;
    assert_no_deliver(&mut ws).await;

    // A louder eager row is dispatchable → fanned out live. Its position is
    // high_water + 2, so the live copy is dropped and retention serves both.
    let loud_seq = feed_durable(&messenger, "loud").await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "quiet", quiet_seq).await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "loud", loud_seq).await;
    assert_no_deliver(&mut ws).await;
}

/// A drain whose suffix is longer than the subscription's `retain_depth` clamp
/// serves only the newest window, and says so: the positions between the
/// high-water and that window are lost to this subscription and ride the first
/// delivery's `dropped`. Silence there would be unrecoverable — the drain
/// advances the high-water past the hole, so no later Subscribe can report it.
#[tokio::test]
async fn surface_ws_durable_drain_reports_the_span_the_clamp_left_behind() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        // One position per drain: a suffix of three cannot fit.
        grant_durable_publish(durable_surface(
            uuid,
            Depth::Bounded(1),
            WakeMin::Normal,
            true,
            None,
        )),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
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
        0,
        "nothing retained before subscribe"
    );

    // Two retained rows with no wake row: nothing nudges the session, so the
    // high-water stays at 0 while retention climbs.
    persist_durable(&messenger, uuid, "lost-1").await;
    persist_durable(&messenger, uuid, "lost-2").await;
    assert_no_deliver(&mut ws).await;

    // A dispatchable third row: its live copy is above the contiguous next
    // position, so the drain runs — and finds a suffix the clamp cannot cover.
    let newest = feed_durable(&messenger, "newest").await;
    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            channel,
            envelope,
            targets,
        } => {
            assert_eq!(channel, DURABLE_ADDR);
            assert_eq!(
                envelope.body, "newest",
                "the clamp serves the newest window"
            );
            let target = sole_target(&targets);
            assert_eq!(
                target.dropped, 2,
                "the two positions the clamped window skipped are reported as lost"
            );
            match cursor::parse(&target.cursor) {
                Ok(state) => assert_eq!(state.resume.seq, newest as u64),
                other => panic!("expected a parseable durable cursor, got {other:?}"),
            }
        }
        other => panic!("expected durable Deliver, got {other:?}"),
    }
    assert_no_deliver(&mut ws).await;
}

/// Two sibling subscriptions in one live batch are each decided against **their
/// own** high-water, not the batch's or the channel's. One sits at the position
/// below the arriving row and takes it live; the other is two behind and is
/// served its whole suffix from retention instead. A decision made against the
/// wrong sibling's high-water would either strand the laggard's interior span or
/// put the caught-up instance's row on the other's ports.
#[tokio::test]
async fn surface_ws_durable_siblings_are_decided_against_their_own_high_waters() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let mut channel_entry = durable_channel_entry(uuid, Depth::Unbounded);
    channel_entry.subscribers = ["agenda-behind", "agenda-current"]
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
    resolved
        .components
        .extend(
            ["agenda-behind", "agenda-current"].map(|instance| ResolvedComponent {
                instance: instance.to_string(),
                kind: "agenda".to_string(),
                abi: brenn_surface_schema::Abi::Dom,
                send_budget: SurfaceSendBudget::default(),
                parked_batch_depth: 8,
                config: Default::default(),
                chrome: false,
            }),
        );
    resolved.subscriptions = ["agenda-behind", "agenda-current"]
        .into_iter()
        .map(|instance| SurfaceBinding {
            channel_address: DURABLE_ADDR.to_string(),
            instance: instance.to_string(),
            port: PORT.to_string(),
            push_depth: WIDE_CLAMP_N,
            retain_depth: WIDE_CLAMP_N,
            noise: NoiseLevel::Silent,
        })
        .collect();
    resolved.wire_subscriptions = ["agenda-behind", "agenda-current"]
        .into_iter()
        .map(|instance| ResolvedSurfaceSubscription {
            instance: instance.to_string(),
            subscription: ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: DURABLE_ADDR.to_string(),
                push_depth: WIDE_CLAMP,
                retain_depth: WIDE_CLAMP,
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

    // The laggard subscribes before anything is retained, so its high-water stays
    // at 0 through the two rows that follow (no wake row, so no nudge).
    ws.send(subscribe_frame_as(DURABLE_ADDR, "agenda-behind", None))
        .await
        .expect("subscribe behind");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, "agenda-behind")
            .await
            .0,
        0
    );
    persist_durable(&messenger, uuid, "one").await;
    persist_durable(&messenger, uuid, "two").await;

    // The caught-up instance subscribes after: its replay carries both rows, so
    // it starts one position below what comes next.
    ws.send(subscribe_frame_as(DURABLE_ADDR, "agenda-current", None))
        .await
        .expect("subscribe current");
    assert_eq!(
        next_subscribe_result(&mut ws, DURABLE_ADDR, "agenda-current")
            .await
            .0,
        2
    );
    assert_durable_deliver_to(&mut ws, "agenda-current", "one", 1).await;
    assert_durable_deliver_to(&mut ws, "agenda-current", "two", 2).await;

    ws.send(publish_frame("writer", "durable", "three", Some(9)))
        .await
        .expect("send durable Publish");
    assert!(
        matches!(
            publish_result_outcome(next_server_frame(&mut ws).await, Some(9)),
            PublishOutcome::Ok
        ),
        "durable publish is Ok"
    );

    let mut behind: Vec<String> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    while behind.len() + current.len() < 4 {
        match next_server_frame(&mut ws).await {
            ServerFrame::Deliver {
                envelope, targets, ..
            } => {
                for target in targets {
                    match target.instance.as_str() {
                        "agenda-behind" => behind.push(envelope.body.clone()),
                        "agenda-current" => current.push(envelope.body.clone()),
                        other => panic!("delivery to an unsubscribed instance {other}"),
                    }
                }
            }
            other => panic!("expected Deliver, got {other:?}"),
        }
    }
    assert_eq!(
        behind,
        vec!["one", "two", "three"],
        "the laggard is served its whole suffix in seq order, exactly once"
    );
    assert_eq!(
        current,
        vec!["three"],
        "the caught-up sibling takes the live copy and nothing else"
    );
    assert_no_deliver(&mut ws).await;
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
        vec![],
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
        vec![],
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
        vec![],
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
        vec![],
    )
    .await;
    let s1 = persist_durable(&messenger1, uuid, "a").await;
    let s2 = persist_durable(&messenger1, uuid, "b").await;
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
    // Signals server 1's shutdown; the task winds down on its own and nothing
    // awaits it. Server 2 binds its own port, so the two never contend.
    drop(sd1);

    // Server 2 over the SAME db: resume from s1 replays the persisted retained row.
    let (state2, _messenger2) = durable_rig(
        &db,
        durable_surface(uuid, Depth::Bounded(10), WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Bounded(10)),
        vec![],
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

/// Multi-session fan-out: a live row reaches both attached sessions, and a
/// session that attaches after the backlog was parked still replays it from the
/// retained window — a fresh attach is answered from retention, not from any
/// per-subscriber delivered log.
#[tokio::test]
async fn surface_ws_durable_multi_session_fanout_and_backlog_once() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        grant_durable_publish(durable_surface(
            uuid,
            WIDE_CLAMP,
            WakeMin::Normal,
            true,
            None,
        )),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
    )
    .await;
    let sb = persist_durable(&messenger, uuid, "backlog").await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws1 = open_deskbar(&base, &token).await;
    let mut ws2 = open_deskbar(&base, &token).await;

    // ws1 subscribes first → its replay drains the backlog.
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

    // ws2 subscribes after → a fresh attach replays the retained window, which
    // holds the row whether or not another session was served it.
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
    let sl = feed_durable(&messenger, "live").await;
    assert_durable_deliver_to(&mut ws1, COMPONENT, "live", sl).await;
    assert_durable_deliver_to(&mut ws2, COMPONENT, "live", sl).await;
}

/// The live fan-out and the drain nudge it fires both reach for the same
/// position, and the client sees it once: the subscription's high-water is the
/// only delivery state, so the second path finds the position already written
/// and sends nothing. Neither path arbitrates with the other; the comparison at
/// send time settles it.
#[tokio::test]
async fn surface_ws_durable_live_fanout_and_its_drain_nudge_deliver_once() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        grant_durable_publish(durable_surface(
            uuid,
            WIDE_CLAMP,
            WakeMin::Normal,
            true,
            None,
        )),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
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

    // One eager row: the router fans it out and nudges the drain, which re-reads
    // retention above the high-water the fan-out just advanced.
    let s = feed_durable(&messenger, "once").await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "once", s).await;
    assert_no_deliver(&mut ws).await;
}

/// A row published while the session held no subscription is served by the next
/// resume, from the cursor the session echoes — the surface's whole delivery
/// state — and the session goes on receiving live commits from the position the
/// resume left it at.
#[tokio::test]
async fn surface_ws_durable_row_missed_while_detached_is_served_on_resume() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        grant_durable_publish(durable_surface(
            uuid,
            WIDE_CLAMP,
            WakeMin::Normal,
            true,
            None,
        )),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
    )
    .await;
    let s1 = persist_durable(&messenger, uuid, "first").await;

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
        1
    );
    assert_durable_deliver_to(&mut ws, COMPONENT, "first", s1).await;

    // Committed with no session subscribed: nothing reaches the wire live.
    let s2 = persist_durable(&messenger, uuid, "missed").await;

    // Unsubscribe then resume at the first row's position. Frames are handled in
    // order, so the answered re-subscribe is itself the proof the unsubscribe
    // landed — a duplicate Subscribe would have been a violation.
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
    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(durable_resume(&db, s1).await),
    ))
    .await
    .expect("re-subscribe");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "the resume serves only what the cursor lacks");
    assert!(gap.is_none(), "an exact suffix reports no gap");
    assert_durable_deliver_to(&mut ws, COMPONENT, "missed", s2).await;

    // And the resume left the session live, not merely caught up: the next
    // commit's fan-out is one above the high-water the resume set, so it sends.
    // (The below-water duplicate arm is pinned by
    // `a_live_batch_decides_each_sibling_against_its_own_high_water`.)
    let late = feed_durable(&messenger, "missed-again").await;
    assert_durable_deliver_to(&mut ws, COMPONENT, "missed-again", late).await;
    assert_no_deliver(&mut ws).await;
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
    let (state, messenger) = durable_rig(
        &db,
        grant_durable_publish(durable_surface(
            uuid,
            WIDE_CLAMP,
            WakeMin::Normal,
            true,
            Some(eph),
        )),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![ephemeral_channel_entry(eph, 0)],
    )
    .await;
    let _stores = Arc::clone(messenger.ring_stores());

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
    // each carrying its own store position (order between the two classes is
    // unspecified, and one cursor shape serves both, so the body names the class).
    let seq = feed_durable(&messenger, "dur").await;
    publish_eph(&messenger, "ephemeral:ticker", "eph").await;

    let mut saw_durable = false;
    let mut saw_ephemeral = false;
    for _ in 0..2 {
        let (body, state) = next_deliver(&mut ws).await;
        match body.as_str() {
            "dur" => {
                assert_eq!(state.resume.seq, seq as u64);
                saw_durable = true;
            }
            "eph" => saw_ephemeral = true,
            other => panic!("unexpected delivery body {other}"),
        }
    }
    assert!(
        saw_durable && saw_ephemeral,
        "both a durable and an ephemeral delivery must reach the one session"
    );
}

/// Session-side delivery-floor parity: when the surface policy does not
/// authorize brenn delivery on the channel, a durable subscribe is a silent wire
/// — `replay_count = 0` and no `Deliver`. Those two are what the floor decides;
/// restore the authorization and both change.
#[tokio::test]
async fn surface_ws_durable_floor_denied_delivers_nothing() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, WIDE_CLAMP, WakeMin::Normal, false, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
    )
    .await;
    persist_durable(&messenger, uuid, "denied-1").await;
    persist_durable(&messenger, uuid, "denied-2").await;

    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("subscribe");
    let (replay, _) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 0, "the floor denies → empty replay");
    assert_no_deliver(&mut ws).await;

    // Not a consequence of the floor — nothing on a bus channel writes that
    // table any more, denied or not. Kept as the cheap backstop it is: a row
    // here means some path started minting per-subscriber delivery records
    // again, behind a subscription that is not even authorized to read.
    let conn = messenger.db().lock().await;
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM messaging_pending_pushes", [], |row| {
            row.get(0)
        })
        .expect("count pending pushes");
    assert_eq!(rows, 0, "nothing mints delivery records on a bus channel");
}

/// Unsubscribe then re-subscribe a durable channel: the fresh subscription drains
/// a newly parked backlog exactly once and never re-delivers a stale row. The
/// re-subscribe re-anchors from the client's echoed cursor, which is what makes
/// the second span start above everything the first one wrote.
#[tokio::test]
async fn surface_ws_durable_unsubscribe_then_resubscribe_delivers_fresh_backlog_once() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, WIDE_CLAMP, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
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
    let seq = persist_durable(&messenger, uuid, "after").await;
    ws.send(subscribe_frame(DURABLE_ADDR, None))
        .await
        .expect("re-subscribe");
    let (replay, _) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(replay, 1, "the freshly parked row drains on re-subscribe");
    assert_durable_deliver_to(&mut ws, COMPONENT, "after", seq).await;
    assert_no_deliver(&mut ws).await;
}

/// One cursor shape serves both classes, so a cursor minted under some other
/// channel's numbering domain is no longer a class mismatch the session can
/// recognize — it is simply an epoch the durable store never minted, which
/// `replay_from` answers as a fresh attach with an `EpochChanged` gap. The
/// connection survives.
#[tokio::test]
async fn surface_ws_durable_subscribe_foreign_epoch_resume_gaps() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, messenger) = durable_rig(
        &db,
        durable_surface(uuid, WIDE_CLAMP, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
    )
    .await;
    let s = persist_durable(&messenger, uuid, "retained").await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;
    let mut ws = open_deskbar(&base, &token).await;

    ws.send(subscribe_frame(
        DURABLE_ADDR,
        Some(cursor::mint(
            0,
            ResumeCursor {
                epoch: Uuid::new_v4(),
                seq: 3,
            },
        )),
    ))
    .await
    .expect("send");
    let (replay, gap) = next_subscribe_result(&mut ws, DURABLE_ADDR, COMPONENT).await;
    assert_eq!(gap, Some(GapReason::EpochChanged));
    assert_eq!(
        replay, 1,
        "a fresh attach replays the whole retained window"
    );
    assert_durable_deliver_to(&mut ws, COMPONENT, "retained", s).await;
}

/// A second Subscribe to an already-active durable channel exercises the
/// duplicate-Subscribe check's `durable.is_active` half — a protocol violation.
#[tokio::test]
async fn surface_ws_durable_subscribe_duplicate_is_violation() {
    let db = db::init_db_memory();
    let uuid = Uuid::new_v4();
    let (state, _messenger) = durable_rig(
        &db,
        durable_surface(uuid, WIDE_CLAMP, WakeMin::Normal, true, None),
        durable_channel_entry(uuid, Depth::Unbounded),
        vec![],
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
/// (bounded retain), a `Messenger` with `deskbar` registered for
/// geometry/status publishing, and `install_surface_runtimes` wired with the
/// description runtime (prefix `surface`, 60 s interval). `flusher`/`alerts`
/// back the protocol-violation assertions; the channel UUIDs read back the
/// persisted telemetry.
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
        send_rate: None,
        uuid: Some(uuid.to_string()),
        address: Some(address.to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Bounded(1)),
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
    let fixture = deskbar_pub_fixture(60, 60);

    // The injector derives channel names from the same prefix the description
    // runtime below is built with, so the two cannot drift apart.
    let mut surfaces = vec![fixture.surface];
    let params = crate::test_support::surface::description_params();
    inject_surface_geometry_status_grants(&mut surfaces, &params.prefix);

    let mut directory_entries = entries;
    directory_entries.extend(fixture.entries);
    let stores = declare_channels(db, &directory_entries).await;
    let router = Arc::new(WakeRouterImpl::new(ActiveBridges::new()));
    let messenger = super::test_fixtures::fixture_messenger(
        db,
        &directory_entries,
        &surfaces[0],
        stores,
        router.clone(),
    );

    state.surfaces = Arc::new(install_surface_runtimes(
        surfaces,
        Some(messenger),
        TEST_MAX_BODY_BYTES,
        None,
        params,
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
    status_frame_full(instances, uptime_secs, counters, None)
}

/// [`status_frame`] reporting a held overlay, for the tests whose subject is the
/// overlay field's trip from the frame to the retained document.
fn status_frame_with_overlay(instances: &[InstanceReport], holder: &str) -> Message {
    status_frame_full(
        instances,
        0,
        StatusCounters::default(),
        Some(OverlayReport {
            holder: holder.to_string(),
            since: "2026-07-22T13:10:00Z".parse().expect("an RFC 3339 instant"),
        }),
    )
}

fn status_frame_full(
    instances: &[InstanceReport],
    uptime_secs: u64,
    counters: StatusCounters,
    overlay: Option<OverlayReport>,
) -> Message {
    let frame = ClientFrame::Status {
        instances: instances.to_vec(),
        uptime_secs,
        counters,
        overlay,
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
                    brenn_surface_schema::InstanceCounters {
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

/// The overlay a shell reports reaches the retained status document end-to-end.
/// This is the whole point of the field: a fullscreen-wedged surface is
/// `mounted`, pumping, and error-free, so the document reads `health: ok` — the
/// holder is the one fact that distinguishes it from a healthy bar, and it is
/// worthless if the frame's value is dropped anywhere between the frame and the
/// channel.
#[tokio::test]
async fn surface_ws_status_overlay_reaches_the_status_document() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let status_uuid = rig.status_uuid;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    consume_welcome(&mut ws).await;

    ws.send(status_frame_with_overlay(
        &[
            instance_report("protobar", "protobar", InstanceState::Mounted, 1),
            instance_report("writer", "writer-module", InstanceState::Mounted, 0),
        ],
        "writer",
    ))
    .await
    .expect("send Status");

    let rows = wait_for_channel(&db, status_uuid, |r| {
        r.last().is_some_and(|(_, b)| b.contains("\"overlay\":{"))
    })
    .await;
    let body: serde_json::Value =
        serde_json::from_str(&rows.last().expect("status row").1).expect("status body is JSON");
    assert_eq!(body["overlay"]["holder"], "writer");
    assert_eq!(body["overlay"]["since"], "2026-07-22T13:10:00Z");
    // Reported, not judged: every instance is mounted and pumping, so a held
    // overlay leaves health alone.
    assert_eq!(body["health"], "ok");
}

/// An overlay naming an instance the surface does not configure is a protocol
/// violation, not a filtered field: the holder is a principal name reaching the
/// document an operator reads attribution off, and an untrusted shell inventing
/// one gets the session killed and the fail2ban-grade log written.
#[tokio::test]
async fn surface_ws_status_overlay_unknown_holder_is_violation() {
    let db = db::init_db_memory();
    let rig = geometry_status_rig(&db).await;
    let flusher = rig.flusher.clone();
    let alerts = rig.alerts.clone();
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(rig.state).await;

    // Every reported *instance* is configured; only the overlay names `ghost`,
    // so this fails only if the overlay is validated on its own.
    assert_frame_is_violation(
        &base,
        &token,
        status_frame_with_overlay(
            &[instance_report(
                "protobar",
                "protobar",
                InstanceState::Mounted,
                1,
            )],
            "ghost",
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
                brenn_surface_schema::InstanceCounters {
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

// ── auto channels across the wire ─────────────────────────────────────────────

/// A backend WASM consumer and a surface component, each declaring one io_port,
/// joined by a single `[[connection]]`. The endpoint set spans the wire, so the
/// auto channel is `ephemeral:` — and nothing in this config names a channel,
/// writes an ACL entry, or tunes a depth outside the two port declarations.
///
/// Both endpoints are io_ports, so each side both publishes and subscribes on
/// the one channel: the two directions this suite drives are the same wire read
/// from either end.
fn spanning_connection_config() -> brenn_lib::config::BrennConfig {
    use crate::bootstrap::messaging::test_fixtures::{
        io_port_raw, minimal_surface_raw, minimal_wasm_consumer, surface_io_port_raw,
    };
    use brenn_lib::messaging::config::{ConnectionConfigRaw, WasmConsumerConfigRaw, WasmGrant};

    let worker = WasmConsumerConfigRaw {
        slug: "worker".to_string(),
        component_path: "/nonexistent/worker.wasm".into(),
        grants: vec![WasmGrant::Ports],
        io_ports: vec![io_port_raw(
            LINK_PORT,
            None,
            Depth::Bounded(4),
            Depth::Bounded(4),
        )],
        ..minimal_wasm_consumer()
    };
    let deskbar = brenn_lib::messaging::config::SurfaceConfigRaw {
        io_ports: vec![surface_io_port_raw(
            COMPONENT,
            LINK_PORT,
            None,
            Depth::Bounded(4),
            Depth::Bounded(4),
        )],
        ..minimal_surface_raw()
    };
    brenn_lib::config::BrennConfig {
        messaging: MessagingGlobalConfig::default(),
        wasm_consumers: vec![worker],
        surfaces: vec![deskbar],
        connections: vec![ConnectionConfigRaw {
            endpoints: vec![
                "wasm:worker/link".to_string(),
                format!("surface:deskbar#{COMPONENT}/{LINK_PORT}"),
            ],
            channel: None,
            uuid: None,
            description: None,
        }],
        ..brenn_lib::config::BrennConfig::default()
    }
}

const LINK_PORT: &str = "link";

/// The delivery-time ACL is satisfied by the matcher the lowering pass injected
/// into the surface's policy; the session panics rather than denying, so a
/// missing injection fails here loudly.
#[tokio::test]
async fn surface_ws_auto_channel_delivers_a_backend_publish() {
    use brenn_lib::messaging::publish::WasmPublish;

    let db = db::init_db_memory();
    let harness =
        super::test_fixtures::booted_surface_harness(&db, &spanning_connection_config()).await;
    let address = harness.surface_sub_address(COMPONENT, LINK_PORT);
    assert!(
        address.starts_with("ephemeral:auto."),
        "a connection spanning the wire needs a transportable channel, got {address:?}",
    );
    assert!(
        harness.surfaces[0].policy.allows_channel_access(&address),
        "the connection is the authorization signal: boot injected the surface's matcher",
    );

    let messenger = Arc::clone(&harness.messenger);
    let flusher = harness.flusher.clone();
    let alerts = Arc::clone(&harness.alerts);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(harness.state).await;

    let mut ws = open_deskbar(&base, &token).await;
    ws.send(subscribe_frame_as(&address, COMPONENT, None))
        .await
        .expect("send Subscribe");
    assert_eq!(
        next_subscribe_result(&mut ws, &address, COMPONENT).await.0,
        0,
        "a fresh ring has nothing to replay"
    );

    messenger
        .publish_from_wasm(
            "worker",
            &[WasmPublish {
                channel_address: &address,
                body: "from-the-backend",
                urgency: Urgency::Normal,
                reply_to: None,
                deliver_after: None,
            }],
        )
        .await;

    match next_server_frame(&mut ws).await {
        ServerFrame::Deliver {
            channel,
            envelope,
            targets,
        } => {
            assert_eq!(channel, address);
            assert_eq!(envelope.body, "from-the-backend");
            assert_eq!(sole_target(&targets).instance, COMPONENT);
        }
        other => panic!("expected Deliver on the auto channel, got {other:?}"),
    }

    assert_no_alerts(&flusher, &alerts, "auto channel delivery to a surface").await;
}

/// Both injected matchers — `ephemeral_publish` on the surface side and
/// subscribe on the WASM side — with no ACL entry anywhere in config.
#[tokio::test]
async fn surface_ws_auto_channel_publish_reaches_the_backend_consumer() {
    let db = db::init_db_memory();
    let harness =
        super::test_fixtures::booted_surface_harness(&db, &spanning_connection_config()).await;
    let address = harness.surface_sub_address(COMPONENT, LINK_PORT);
    let consumer = ParticipantId::for_wasm("worker");

    let messenger = Arc::clone(&harness.messenger);
    let flusher = harness.flusher.clone();
    let alerts = Arc::clone(&harness.alerts);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(harness.state).await;

    let mut ws = open_deskbar(&base, &token).await;
    ws.send(publish_frame(
        COMPONENT,
        LINK_PORT,
        "from-the-page",
        Some(3),
    ))
    .await
    .expect("send Publish");
    let outcome = publish_result_outcome(next_server_frame(&mut ws).await, Some(3));
    assert!(
        matches!(outcome, PublishOutcome::Ok),
        "the injected publish matcher covers the auto channel, got {outcome:?}",
    );

    // The answered publish is already committed, so this reads a settled ring.
    let store = messenger
        .ring_stores()
        .get_by_address(&address)
        .expect("the auto channel has a ring store");
    let window = store
        .window(&consumer, 4, 0)
        .expect("the consumer's io_port input half is attached at boot");
    assert_eq!(
        window.new_len(),
        1,
        "the page's publish is owed to the backend consumer"
    );
    assert_eq!(window.new_entries()[0].message.body, "from-the-page");
    assert_eq!(
        window.new_entries()[0].message.sender.as_ref(),
        format!("surface:deskbar#{COMPONENT}"),
        "the publisher is named at the instance grain the connection declared",
    );

    assert_no_alerts(&flusher, &alerts, "auto channel publish from a surface").await;
}

// ===========================================================================
// Class parity: one scenario, two classes, one transcript
// ===========================================================================
//
// The bridge is supposed to hold no durable/ephemeral distinction at all, and
// a reviewer's eye is a poor guard against one growing back. So one script —
// fresh subscribe with replay, a live row, at-most-once, reconnect resume
// (`UpToDate` then `Exact`), a forced gap with its `dropped` accounting, a
// resume ahead of everything assigned, and a resume under a stale incarnation —
// runs against one `brenn:` and one `ephemeral:` channel, and the two frame
// transcripts must be identical.
//
// The class appears below only where a fixture must build a channel of a given
// class or put a message into one behind the bridge's back. Everything the
// script does *through* the bridge — subscribe, resume, publish, read frames —
// is one call for both.

/// The parity channel's bare name; the two rigs differ in exactly its scheme.
const PARITY_NAME: &str = "parity-demo";

/// What the parity channel retains. Wide enough that the script evicts nothing,
/// so every gap it forces comes from the subscription's own clamp — the bound
/// both classes resolve identically — rather than from one store's physical
/// eviction and the other's row count.
const PARITY_CHANNEL_RETAIN: u64 = 16;

/// The parity subscription's push depth. Its retain depth is 0, so this is also
/// its replay clamp (`max(push, retain)`) — narrow enough that a span of four
/// unseen rows overruns it.
const PARITY_PUSH_DEPTH: u64 = 2;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Class {
    Durable,
    Ephemeral,
}

impl Class {
    fn address(self) -> String {
        match self {
            Class::Durable => format!("brenn:{PARITY_NAME}"),
            Class::Ephemeral => format!("ephemeral:{PARITY_NAME}"),
        }
    }
}

/// A running server whose `deskbar` surface holds one subscription on the
/// parity channel of one class, plus everything the script needs to drive it.
struct ParityRig {
    db: db::Db,
    messenger: Arc<Messenger>,
    class: Class,
    channel: String,
    uuid: Uuid,
    base: String,
    token: String,
    alerts: Arc<Mutex<Vec<(String, String)>>>,
    flusher: AlertDispatcher,
    /// Dropping this shuts the server down, so the rig owns it.
    _server: TestServer,
}

/// The parity channel entry for a class: one bare name, one retain depth, one
/// subscriber set — the scheme and the transport are all that differ.
fn parity_channel_entry(class: Class) -> ChannelEntry {
    match class {
        Class::Ephemeral => ephemeral_channel_entry(PARITY_NAME, PARITY_CHANNEL_RETAIN),
        Class::Durable => ChannelEntry {
            uuid: Uuid::new_v4(),
            address: class.address(),
            description: None,
            resolved_channel: ResolvedChannel {
                send_rate: Default::default(),
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Bounded(PARITY_CHANNEL_RETAIN),
                standing_retain_depth: Depth::Bounded(PARITY_CHANNEL_RETAIN),
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Brenn,
            mount: None,
        },
    }
}

/// A `deskbar` surface subscribing to — and publishing on — the parity channel.
/// The two policies differ only in which scheme's grant and ACL name the same
/// channel, which is config, not a delivery path.
fn parity_surface(class: Class, uuid: Uuid) -> ResolvedSurface {
    let address = class.address();
    let mut policy = AppPolicy::default();
    let matcher = vec![ChannelMatcher::Exact(PARITY_NAME.to_string())];
    match class {
        Class::Durable => {
            policy.grants.insert(AppCapability::MessagingSubscribe);
            policy.grants.insert(AppCapability::MessagingPublish);
            policy.acls.brenn_subscribe = matcher.clone();
            policy.acls.brenn_publish = matcher;
        }
        Class::Ephemeral => {
            policy.grants.insert(AppCapability::EphemeralSubscribe);
            policy.grants.insert(AppCapability::EphemeralPublish);
            policy.acls.ephemeral_subscribe = matcher.clone();
            policy.acls.ephemeral_publish = matcher;
        }
    }
    SurfaceFixture::new("deskbar", COMPONENT)
        .subscribe_at_depths(&address, COMPONENT, PORT, PARITY_PUSH_DEPTH, 0)
        .durable_subscribe(
            COMPONENT,
            ResolvedSubscription {
                channel_uuid: uuid,
                channel_address: address,
                push_depth: Depth::Bounded(PARITY_PUSH_DEPTH),
                retain_depth: Depth::Bounded(0),
                noise: NoiseLevel::Silent,
                wake_min: WakeMin::Normal,
            },
        )
        .policy(policy)
        .build()
}

/// Build and spawn the rig for one class. The harness is class-blind: one
/// call builds both substrates.
async fn parity_rig(db: &db::Db, class: Class) -> ParityRig {
    let entry = parity_channel_entry(class);
    let uuid = entry.uuid;
    // The script disconnects mid-run, so the rig owes the surface its derived
    // telemetry channels and the grants over them, exactly as boot does — the
    // last-session teardown writes a terminal snapshot onto one of them.
    let params = crate::test_support::surface::description_params();
    let mut surfaces = vec![parity_surface(class, uuid)];
    inject_surface_geometry_status_grants(&mut surfaces, &params.prefix);
    let surface = surfaces.pop().expect("the one parity surface");
    let entries = vec![
        entry,
        brenn_channel_entry(GEOMETRY_NAME, Uuid::new_v4()),
        brenn_channel_entry(STATUS_NAME, Uuid::new_v4()),
    ];
    let harness = surface_harness_with_durable(db, surface, entries).await;
    let (token, _) = setup_authenticated_user(db).await;
    let (base, server) = spawn_test_server(harness.state).await;
    ParityRig {
        db: db.clone(),
        messenger: harness.messenger,
        class,
        channel: class.address(),
        uuid,
        base,
        token,
        alerts: harness.alerts,
        flusher: harness.flusher,
        _server: server,
    }
}

/// Publish onto the parity channel through the production publish path.
async fn parity_feed(rig: &ParityRig, body: &str) {
    match rig
        .messenger
        .publish_from_surface("deskbar", None, &rig.channel, body, Urgency::Normal)
        .await
    {
        PublishResult::Ok { .. } => {}
        other => panic!("parity feed expected Ok, got {other:?}"),
    }
}

/// Commit onto the parity channel *behind* the bridge: the message enters
/// retention with no surface fed, which is how the script manufactures a span a
/// subscription never saw. Publishing while no session is attached produces the
/// same state; this is just the deterministic way to reach it.
async fn parity_commit_unfed(rig: &ParityRig, body: &str) {
    match rig.class {
        Class::Durable => {
            let conn = rig.db.lock().await;
            insert_message(
                &conn,
                rig.uuid,
                "host",
                "sender",
                body,
                Urgency::Normal,
                ChannelScheme::Brenn,
                None,
                None,
                None,
                None,
                utc_to_ns(Utc::now()),
            );
        }
        Class::Ephemeral => {
            rig.messenger
                .ring_stores()
                .get_by_address(&rig.channel)
                .expect("the parity ring")
                .append(MessageEnvelope {
                    message_id: Uuid::new_v4(),
                    source: TEST_ORIGIN.into(),
                    channel: rig.channel.clone(),
                    sender: "surface:deskbar".into(),
                    publish_ts: Utc::now(),
                    body: body.to_string(),
                    reply_to: None,
                    delivery_deadline: None,
                    deliver_after: None,
                    impetus: None,
                    urgency: Urgency::Normal,
                    envelope_type: ChannelScheme::Ephemeral,
                });
        }
    }
}

/// One server frame reduced to what the parity script pins: everything except
/// the values a class is entitled to differ in — the channel address, the epoch
/// numbering its seqs, and the store incarnation stamped beside it.
fn parity_line(frame: &ServerFrame) -> String {
    match frame {
        ServerFrame::SubscribeResult {
            instance,
            outcome,
            replay_count,
            gap,
            ..
        } => format!(
            "subscribe_result instance={instance} outcome={outcome:?} replay={replay_count} \
             gap={:?}",
            gap.as_ref().map(|g| g.reason)
        ),
        ServerFrame::Deliver {
            envelope, targets, ..
        } => {
            let target = sole_target(targets);
            let state = cursor::parse(&target.cursor).expect("a server-minted cursor parses");
            // Both numbers, because they are different numbers and both must
            // match across classes: `span` is the per-subscribe wire counter the
            // page orders on, `pos` the retention position the cursor carries.
            format!(
                "deliver body={} instance={} span={} pos={} dropped={}",
                envelope.body, target.instance, target.seq, state.resume.seq, target.dropped
            )
        }
        other => panic!("the parity script expects no {other:?}"),
    }
}

/// Read `n` frames, appending each one's normalized form to the transcript, and
/// hand back the cursor the last `Deliver` among them carried.
async fn parity_record(
    ws: &mut SurfaceWs,
    n: usize,
    transcript: &mut Vec<String>,
    cursor_out: &mut Option<Cursor>,
) {
    for _ in 0..n {
        let frame = next_server_frame(ws).await;
        if let ServerFrame::Deliver { targets, .. } = &frame {
            *cursor_out = Some(sole_target(targets).cursor.clone());
        }
        transcript.push(parity_line(&frame));
    }
}

/// Assert nothing more arrives, and say so in the transcript — silence where a
/// duplicate could have been is the at-most-once assertion.
async fn parity_quiet(ws: &mut SurfaceWs, transcript: &mut Vec<String>) {
    assert_no_deliver(ws).await;
    transcript.push("quiet".to_string());
}

/// Drive the whole scenario against one class and return its frame transcript.
async fn run_parity_script(class: Class) -> Vec<String> {
    let db = db::init_db_memory();
    let rig = parity_rig(&db, class).await;
    let mut transcript: Vec<String> = Vec::new();
    let mut held: Option<Cursor> = None;

    // 1. Two rows land with nobody attached; a fresh subscribe replays them.
    parity_commit_unfed(&rig, "m1").await;
    parity_commit_unfed(&rig, "m2").await;
    let mut ws = open_deskbar(&rig.base, &rig.token).await;
    ws.send(subscribe_frame(&rig.channel, None))
        .await
        .expect("subscribe");
    parity_record(&mut ws, 3, &mut transcript, &mut held).await;

    // 2. A live row on the attached subscription, contiguous with its position.
    parity_feed(&rig, "m3").await;
    parity_record(&mut ws, 1, &mut transcript, &mut held).await;

    // 3. Nothing it already holds is sent again.
    parity_quiet(&mut ws, &mut transcript).await;

    // 4. Reconnect echoing the cursor the page holds: caught up, nothing owed.
    drop(ws);
    let mut ws = open_deskbar(&rig.base, &rig.token).await;
    ws.send(subscribe_frame(&rig.channel, held.clone()))
        .await
        .expect("subscribe");
    parity_record(&mut ws, 1, &mut transcript, &mut held).await;
    parity_quiet(&mut ws, &mut transcript).await;

    // 5. A row lands while detached; the reconnect resumes exactly onto it.
    drop(ws);
    parity_commit_unfed(&rig, "m4").await;
    let mut ws = open_deskbar(&rig.base, &rig.token).await;
    ws.send(subscribe_frame(&rig.channel, held.clone()))
        .await
        .expect("subscribe");
    parity_record(&mut ws, 2, &mut transcript, &mut held).await;

    // 6. Three rows the subscription never saw, then a live one far above its
    //    position: the live copy is dropped, the drain serves the suffix its
    //    clamp allows, and the span between rides the first delivery's
    //    `dropped`.
    parity_commit_unfed(&rig, "m5").await;
    parity_commit_unfed(&rig, "m6").await;
    parity_commit_unfed(&rig, "m7").await;
    parity_feed(&rig, "m8").await;
    parity_record(&mut ws, 2, &mut transcript, &mut held).await;
    parity_quiet(&mut ws, &mut transcript).await;

    // 7. A resume above everything the channel ever assigned is answered as a
    //    fresh attach, on both classes — never as a connection-killing
    //    violation.
    drop(ws);
    let echoed = cursor::parse(&held.expect("the script received a cursor")).expect("parses");
    let ahead = cursor::mint(
        echoed.incarnation,
        ResumeCursor {
            epoch: echoed.resume.epoch,
            seq: echoed.resume.seq + 500,
        },
    );
    let mut ws = open_deskbar(&rig.base, &rig.token).await;
    ws.send(subscribe_frame(&rig.channel, Some(ahead)))
        .await
        .expect("subscribe");
    let mut held = None;
    parity_record(&mut ws, 3, &mut transcript, &mut held).await;
    parity_quiet(&mut ws, &mut transcript).await;

    // 8. A cursor stamped with an incarnation the store never reached — what a
    //    backup restore leaves a page holding. Answered as a fresh attach on
    //    both classes, which pins two things at once: the cursors this
    //    connection minted carry the real boot incarnation (a ring cursor
    //    stamped 0 could not be one above it), and the staleness check runs
    //    whatever the class.
    drop(ws);
    let echoed = cursor::parse(&held.expect("step 7 delivered a cursor")).expect("parses");
    let stale = cursor::mint(echoed.incarnation + 1, echoed.resume);
    let mut ws = open_deskbar(&rig.base, &rig.token).await;
    ws.send(subscribe_frame(&rig.channel, Some(stale)))
        .await
        .expect("subscribe");
    let mut ignored = None;
    parity_record(&mut ws, 3, &mut transcript, &mut ignored).await;
    parity_quiet(&mut ws, &mut transcript).await;

    assert_no_alerts(&rig.flusher, &rig.alerts, "the parity script is conforming").await;
    transcript
}

/// The transcript both classes must produce, spelled out so the harness pins
/// the behavior rather than only pinning the two classes to each other.
const PARITY_TRANSCRIPT: &[&str] = &[
    // 1. Fresh subscribe over a two-row window. The span counter opens at 1 and
    //    tracks the retention position while the subscription stays attached
    //    from position 0.
    "subscribe_result instance=protobar outcome=Ok replay=2 gap=None",
    "deliver body=m1 instance=protobar span=1 pos=1 dropped=0",
    "deliver body=m2 instance=protobar span=2 pos=2 dropped=0",
    // 2. The live row.
    "deliver body=m3 instance=protobar span=3 pos=3 dropped=0",
    // 3. At most once.
    "quiet",
    // 4. Resume at the held cursor: up to date.
    "subscribe_result instance=protobar outcome=Ok replay=0 gap=None",
    "quiet",
    // 5. Resume onto the one row missed while detached. A new subscribe opens a
    //    new span, so the span counter restarts at 1 while the position carries
    //    on from where the last connection left it — the two numbers part
    //    company here, and both must part company identically on both classes.
    "subscribe_result instance=protobar outcome=Ok replay=1 gap=None",
    "deliver body=m4 instance=protobar span=1 pos=4 dropped=0",
    // 6. The clamp cannot cover seqs 5..8, so 5 and 6 are lost and counted on
    //    the first delivery that follows them.
    "deliver body=m7 instance=protobar span=2 pos=7 dropped=2",
    "deliver body=m8 instance=protobar span=3 pos=8 dropped=0",
    "quiet",
    // 7. Resume ahead: a fresh attach under EpochChanged, clamped to the two
    //    newest rows.
    "subscribe_result instance=protobar outcome=Ok replay=2 gap=Some(EpochChanged)",
    "deliver body=m7 instance=protobar span=1 pos=7 dropped=0",
    "deliver body=m8 instance=protobar span=2 pos=8 dropped=0",
    "quiet",
    // 8. Resume under an incarnation above the store's: the same fresh attach,
    //    which is only reachable if the cursor this connection minted carried
    //    the real boot incarnation.
    "subscribe_result instance=protobar outcome=Ok replay=2 gap=Some(EpochChanged)",
    "deliver body=m7 instance=protobar span=1 pos=7 dropped=0",
    "deliver body=m8 instance=protobar span=2 pos=8 dropped=0",
    "quiet",
];

/// The maxim's pin. Any future re-divergence of the classes at the surface
/// bridge fails here rather than passing review.
#[tokio::test]
async fn surface_ws_the_two_classes_produce_one_transcript() {
    let durable = run_parity_script(Class::Durable).await;
    let ephemeral = run_parity_script(Class::Ephemeral).await;

    assert_eq!(
        durable, ephemeral,
        "the bridge answered a durable channel differently from an ephemeral one"
    );
    assert_eq!(
        durable,
        PARITY_TRANSCRIPT
            .iter()
            .map(|s| (*s).to_string())
            .collect::<Vec<_>>(),
        "the transcript both classes agree on is not the one the design specifies"
    );
}

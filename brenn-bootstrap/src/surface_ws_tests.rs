//! Route-level integration tests for `GET /surface/{slug}/ws`: the pre-upgrade
//! ladder (access → capacity → served-asset build check), the opening frames the
//! attachment session writes over a real socket, the round trip between an
//! attached page and the bus through the real `WakeRouter`, and the terminal
//! `disconnected` stamp the route writes when the last attachment ends.
//!
//! What a frame *means* is the attachment session's own business and is pinned
//! crate-local beside it (`brenn-attach-server`'s suites, over a driven duplex with a
//! stub profile). What this suite owes is the wiring those tests cannot see: that
//! the surface route hands the socket to that session with this surface's
//! profile, that the registry it registers into is the one the router fans out
//! through, and that the identities and channels a real boot resolved are the
//! ones a real page reaches.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use brenn_attach_proto::{
    ClientFrame, SUPPORTED_VERSIONS, ServerFrame, SubscribeOutcome, Urgency, VersionRange,
    max_client_frame_bytes,
};
use brenn_messaging::testutils::ephemeral_channel_entry;
use brenn_obs::alerting::AlertDispatcher;
use futures::{SinkExt, StreamExt};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use brenn_attach_server::registry::{AttachSessionHandle, SessionCaps};
use brenn_server::routes::surface::test_fixtures::{
    SurfaceTestHarness, assert_no_alerts, surface_harness,
};
use brenn_server::test_support::TEST_BUILD_ID;
use brenn_server::test_support::http::{
    TEST_USERNAME, assert_stale_client_close_and_no_alert, http_to_ws_url,
    setup_authenticated_user, spawn_test_server, surface_ws_open, ws_connect_first_frame,
    ws_upgrade_status,
};
use brenn_surface_server::test_fixtures::{
    COMPONENT, EPH_ADDR, EPH_NAME, PORT, TEST_MAX_BODY_BYTES, deskbar_loop,
};
use brenn_surface_server::{MAX_SESSIONS_PER_SURFACE, MAX_SESSIONS_PER_USER_PER_SURFACE};

/// A channel `otherbar` binds, bare and scheme-qualified: the exists-but-not-
/// yours half of the no-existence-oracle probe.
const OTHERBAR_NAME: &str = "otherbar-only";
const OTHERBAR_ADDR: &str = "ephemeral:otherbar-only";

type SurfaceWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The caps a production surface runs under. The capacity tests prefill the
/// registry against these rather than a test value, so what they exercise is the
/// number an operator's page actually meets.
const PROD_CAPS: SessionCaps = SessionCaps {
    per_attacher: MAX_SESSIONS_PER_SURFACE,
    per_account: MAX_SESSIONS_PER_USER_PER_SURFACE,
};

/// The harness most tests here run against: the `deskbar` surface over a store
/// carrying the fixture channel at the given retain depth.
async fn deskbar_harness(
    db: &brenn_db::Db,
    allowed_users: Vec<String>,
    retain_depth: u64,
) -> SurfaceTestHarness {
    surface_harness(
        db,
        deskbar_loop(allowed_users),
        vec![ephemeral_channel_entry(EPH_NAME, retain_depth)],
    )
    .await
}

/// Assert exactly one security alert was captured and its combined
/// source+detail text contains `needle`. The caller must already hold a
/// happens-before edge (an observed response or close) proving the triggering
/// action finished; `flush` then makes the dispatched alert visible without
/// racing the drainer.
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

async fn send(ws: &mut SurfaceWs, frame: &ClientFrame) {
    let text = serde_json::to_string(frame).expect("a client frame serializes");
    ws.send(Message::text(text)).await.expect("send a frame");
}

/// Read the peer's `Hello`, answer it, and read what follows — the opening
/// ladder every other test here starts from.
async fn open_attachment(ws: &mut SurfaceWs) -> ServerFrame {
    match next_server_frame(ws).await {
        ServerFrame::Hello { versions, .. } => assert_eq!(versions, SUPPORTED_VERSIONS),
        other => panic!("expected the peer's Hello first, got {other:?}"),
    }
    send(
        ws,
        &ClientFrame::Hello {
            versions: SUPPORTED_VERSIONS,
            ident: "ws-tests".to_string(),
        },
    )
    .await;
    next_server_frame(ws).await
}

/// Open an attached socket, through the whole pre-upgrade ladder and the version
/// handshake, handing back the socket and the `session_id` the `Welcome` named.
async fn attach(base: &str, token: &str) -> (SurfaceWs, String) {
    let url = http_to_ws_url(base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&url, token).await;
    match open_attachment(&mut ws).await {
        ServerFrame::Welcome { session_id, .. } => (ws, session_id),
        other => panic!("expected Welcome after the handshake, got {other:?}"),
    }
}

/// What a client can observe of the peer's teardown. A close frame carries a
/// code and reason; an abrupt end (clean EOF or TCP reset, which a client cannot
/// tell apart and which is collapsed here so a comparison is not timing-flaky)
/// carries none. The reason is captured, not just the code, so a
/// same-code/different-reason close still diverges between two probe inputs.
#[derive(Debug, PartialEq, Eq)]
enum CloseObservation {
    CloseFrame(Option<(u16, String)>),
    Abrupt,
}

/// Drain until the peer closes, returning the observed close shape (or `None` on
/// a 5 s timeout) so callers can compare two inputs' teardowns.
///
/// Used only on violation paths, so it also pins the "no response frame" half of
/// the violation contract: keep-alive and an idle `Heartbeat` pass, but any other
/// `ServerFrame` reaching the client before the close means a handler leaked a
/// response to the offending frame.
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

/// [`drain_until_closed_observing`] reduced to "did the peer close", for callers
/// that do not compare close shapes.
async fn drain_until_closed(ws: &mut SurfaceWs) -> bool {
    drain_until_closed_observing(ws).await.is_some()
}

// ---------------------------------------------------------------------------
// Pre-upgrade ladder: access → capacity → served-asset build check
// ---------------------------------------------------------------------------

#[tokio::test]
async fn surface_ws_unknown_slug_returns_404() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = deskbar_harness(&db, vec![], 4).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let status = ws_upgrade_status(&format!("{base}/surface/nonexistent/ws"), Some(&token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    assert_single_alert(&flusher, &alerts, "unrecognized_url").await;
}

#[tokio::test]
async fn surface_ws_access_denied_returns_403() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = deskbar_harness(&db, vec!["otheruser".to_string()], 4).await;
    let (token, _) = setup_authenticated_user(&db).await; // testuser
    let (base, _sd) = spawn_test_server(state).await;

    let status = ws_upgrade_status(&format!("{base}/surface/deskbar/ws"), Some(&token)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    assert_single_alert(&flusher, &alerts, "auth_failure").await;
}

/// **Capacity is checked before the upgrade, against the attach registry the
/// session task registers into.** Prefilled with distinct accounts so the shared
/// cap is what trips, not the per-account one.
#[tokio::test]
async fn surface_ws_session_cap_returns_503_no_alert() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = deskbar_harness(&db, vec![], 4).await;
    let registry = state.attach_registry.clone();
    let mut guards = Vec::new();
    for i in 0..MAX_SESSIONS_PER_SURFACE {
        guards.push(
            registry
                .try_register(
                    "deskbar",
                    AttachSessionHandle::for_test(&format!("filler-{i}")),
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

    // A user with too many tabs is not fail2ban signal.
    assert_no_alerts(&flusher, &alerts, "session-cap 503 must not fire an alert").await;
    drop(guards);
}

/// The per-account cap trips on its own, well below the shared one — otherwise
/// one account's devices could deny every other allowed user.
#[tokio::test]
async fn surface_ws_per_user_cap_returns_503_no_alert() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = deskbar_harness(&db, vec![], 4).await;
    let registry = state.attach_registry.clone();
    let mut guards = Vec::new();
    for _ in 0..MAX_SESSIONS_PER_USER_PER_SURFACE {
        guards.push(
            registry
                .try_register(
                    "deskbar",
                    AttachSessionHandle::for_test(TEST_USERNAME),
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

    assert_no_alerts(&flusher, &alerts, "per-user cap 503 must not fire an alert").await;
    drop(guards);
}

/// The served-asset check is the surface route's own, ahead of any protocol
/// frame: a stale tab is closed with the stale code and never reaches the
/// attachment session.
#[tokio::test]
async fn surface_ws_missing_build_closes_stale_no_alert() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness { state, alerts, .. } = deskbar_harness(&db, vec![], 4).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, "/surface/deskbar/ws");
    let msg = ws_connect_first_frame(&ws_url, &token).await;
    assert_stale_client_close_and_no_alert(msg, &alerts, "surface missing build").await;
}

// ---------------------------------------------------------------------------
// The opening frames
// ---------------------------------------------------------------------------

/// **`Welcome` carries transport facts and nothing else.** Every field is a fact
/// of *this attachment* — who it is, which connection, what the caps are — and
/// the surface's wiring reaches the page as a retained document on the config
/// channel instead.
#[tokio::test]
async fn surface_ws_welcome_carries_this_attachments_transport_facts() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness { state, .. } = deskbar_harness(&db, vec![], 4).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    match open_attachment(&mut ws).await {
        ServerFrame::Welcome {
            version,
            participant_id,
            session_id,
            heartbeat_secs,
            max_body_bytes,
            max_frame_bytes,
            alert_granted,
        } => {
            assert_eq!(version, SUPPORTED_VERSIONS.max);
            assert_eq!(participant_id, "surface:deskbar");
            assert!(
                Uuid::parse_str(&session_id).is_ok(),
                "the session id is this connection's minted uuid, got {session_id:?}"
            );
            assert_eq!(heartbeat_secs, 1);
            assert_eq!(max_body_bytes, TEST_MAX_BODY_BYTES as u64);
            assert_eq!(
                max_frame_bytes,
                max_client_frame_bytes(TEST_MAX_BODY_BYTES) as u64
            );
            // Default-policy surface: the alert plane is deny-by-default.
            assert!(!alert_granted);
        }
        other => panic!("expected Welcome, got {other:?}"),
    }
}

/// A peer with no version in common is closed on its own arithmetic — no refusal
/// frame, and no security event: an incompatible peer is deploy skew, not an
/// attacker.
#[tokio::test]
async fn surface_ws_incompatible_peer_is_closed_without_a_security_event() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = deskbar_harness(&db, vec![], 4).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let ws_url = http_to_ws_url(&base, &format!("/surface/deskbar/ws?build={TEST_BUILD_ID}"));
    let mut ws = surface_ws_open(&ws_url, &token).await;
    match next_server_frame(&mut ws).await {
        ServerFrame::Hello { .. } => {}
        other => panic!("expected the peer's Hello first, got {other:?}"),
    }
    send(
        &mut ws,
        &ClientFrame::Hello {
            versions: VersionRange {
                min: SUPPORTED_VERSIONS.max + 7,
                max: SUPPORTED_VERSIONS.max + 9,
            },
            ident: "from-the-future".to_string(),
        },
    )
    .await;

    assert!(
        drain_until_closed(&mut ws).await,
        "a peer with no version in common is closed"
    );
    assert_no_alerts(
        &flusher,
        &alerts,
        "a version mismatch is not fail2ban signal",
    )
    .await;
}

// ---------------------------------------------------------------------------
// The round trip through the bus
// ---------------------------------------------------------------------------

/// **The whole loop, on the wiring a boot resolved.** The attachment subscribes
/// a channel its profile admits, publishes onto it under a declared attribution,
/// and is delivered its own message — which happens only if the route registered
/// into the registry the router fans out through, at the channel grain that
/// registry now keys on, and if the profile minted the component's sub-identity
/// as the sender.
#[tokio::test]
async fn surface_ws_a_publish_reaches_the_bus_and_comes_back_on_the_subscription() {
    let db = brenn_server::test_support::init_db_memory();
    // retain_depth 0: the subscribe replays nothing, so the delivery under test
    // is unambiguously the live one.
    let SurfaceTestHarness { state, .. } = deskbar_harness(&db, vec![], 0).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let (mut ws, _session_id) = attach(&base, &token).await;
    send(
        &mut ws,
        &ClientFrame::Subscribe {
            channel: EPH_ADDR.to_string(),
            push_depth: 4,
            retain_depth: 4,
            resume: None,
        },
    )
    .await;
    match next_server_frame(&mut ws).await {
        ServerFrame::SubscribeResult {
            channel,
            outcome,
            replay_count,
            gap,
        } => {
            assert_eq!(channel, EPH_ADDR);
            assert_eq!(outcome, SubscribeOutcome::Ok);
            assert_eq!(replay_count, 0);
            assert!(gap.is_none());
        }
        other => panic!("expected SubscribeResult, got {other:?}"),
    }

    send(
        &mut ws,
        &ClientFrame::Publish {
            channel: EPH_ADDR.to_string(),
            attribution: Some(COMPONENT.to_string()),
            body: "round trip".to_string(),
            urgency: Urgency::Normal,
            correlation: Some(7),
        },
    )
    .await;

    // The publish is answered, and the same message arrives back over the
    // subscription — in either order, since the answer and the fan-out are
    // separate paths.
    let mut answered = false;
    let mut delivered = false;
    while !(answered && delivered) {
        match next_server_frame(&mut ws).await {
            ServerFrame::PublishResult {
                correlation,
                outcome,
            } => {
                assert_eq!(correlation, Some(7));
                assert!(
                    matches!(outcome, brenn_attach_proto::PublishOutcome::Ok),
                    "the fixture's policy covers this port: {outcome:?}"
                );
                answered = true;
            }
            ServerFrame::Deliver { channel, rows } => {
                assert_eq!(channel, EPH_ADDR);
                let [row] = rows.as_slice() else {
                    panic!("one live publish is a one-row pass, got {rows:?}");
                };
                assert_eq!(row.envelope.body, "round trip");
                assert_eq!(
                    row.envelope.sender, "surface:deskbar#protobar",
                    "the attribution mints the component's own sub-identity"
                );
                delivered = true;
            }
            other => panic!("expected a publish answer or a delivery, got {other:?}"),
        }
    }
}

/// A channel this surface binds nowhere is a protocol violation, whatever else
/// exists on the bus: the route's profile is the whole of the attachment's
/// subscribable set, and a miss closes the socket with one security event.
#[tokio::test]
async fn surface_ws_subscribe_to_an_unbound_channel_is_a_violation() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = deskbar_harness(&db, vec![], 4).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let (mut ws, _session_id) = attach(&base, &token).await;
    send(
        &mut ws,
        &ClientFrame::Subscribe {
            channel: "ephemeral:not-bound".to_string(),
            push_depth: 1,
            retain_depth: 1,
            resume: None,
        },
    )
    .await;

    assert!(
        drain_until_closed(&mut ws).await,
        "a violation tears the attachment down"
    );
    assert_single_alert(&flusher, &alerts, "attach_protocol_violation").await;
}

/// **No existence oracle.** A channel that exists — declared, on the bus, and
/// bound by *another* surface — and a channel that exists nowhere are one answer
/// to a `deskbar` attachment: no response frame, the same close shape, the same
/// violation text. Anything that distinguished them would hand an authenticated
/// page a probe for the server's channel inventory across surfaces.
#[tokio::test]
async fn surface_ws_subscribe_gives_no_existence_oracle_across_surfaces() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness {
        state,
        alerts,
        flusher,
        ..
    } = brenn_server::routes::surface::test_fixtures::surface_harness_with_siblings(
        &db,
        deskbar_loop(vec![]),
        vec![otherbar()],
        vec![
            ephemeral_channel_entry(EPH_NAME, 4),
            ephemeral_channel_entry(OTHERBAR_NAME, 4),
        ],
    )
    .await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let bound_elsewhere = subscribe_and_observe_close(&base, &token, OTHERBAR_ADDR).await;
    let nonexistent = subscribe_and_observe_close(&base, &token, "ephemeral:pure-fiction").await;

    assert_eq!(
        bound_elsewhere, nonexistent,
        "the two inputs must close identically: {bound_elsewhere:?} vs {nonexistent:?}"
    );

    flusher.flush().await;
    let captured = alerts.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        2,
        "one violation per probe, got {captured:?}"
    );
    let texts: Vec<String> = captured
        .iter()
        .map(|(source, detail)| format!("{source} {detail}"))
        .collect();
    for text in &texts {
        assert!(
            text.contains("attach_protocol_violation"),
            "expected a protocol violation, got {text}"
        );
    }
    // The alert *details* carry the probed address for diagnostics and so differ;
    // what must not differ is the violation the two probes are judged under.
    let unsubscribable: Vec<bool> = texts.iter().map(|t| t.contains("unsubscribable")).collect();
    assert_eq!(
        unsubscribable,
        vec![true, true],
        "both probes must be the same violation, got {texts:?}"
    );
}

/// A second surface binding a channel of its own. Nothing attaches to it; it is
/// here so `OTHERBAR_ADDR` is a channel that genuinely exists in another
/// surface's resolved config.
fn otherbar() -> brenn_lib::messaging::config::ResolvedSurface {
    brenn_surface_server::fixtures_config::SurfaceFixture::new("otherbar", COMPONENT)
        .subscribe(OTHERBAR_ADDR, COMPONENT, PORT)
        .build()
}

/// Open a `deskbar` attachment, subscribe `channel`, and return the close shape
/// the peer answered with.
async fn subscribe_and_observe_close(base: &str, token: &str, channel: &str) -> CloseObservation {
    let (mut ws, _session_id) = attach(base, token).await;
    send(
        &mut ws,
        &ClientFrame::Subscribe {
            channel: channel.to_string(),
            push_depth: 1,
            retain_depth: 1,
            resume: None,
        },
    )
    .await;
    drain_until_closed_observing(&mut ws)
        .await
        .expect("a violation closes the attachment")
}

// ---------------------------------------------------------------------------
// Auto channels across the wire
// ---------------------------------------------------------------------------

/// The port both endpoints of the spanning link declare.
const LINK_PORT: &str = "link";

/// A backend WASM consumer and a surface component, each declaring one io_port,
/// joined by a single `link`. The endpoint set spans the wire, so the auto
/// channel is `ephemeral:` — and nothing in this config names that channel,
/// writes an ACL entry, or tunes a depth outside the two port declarations.
///
/// The one `channel` declaration is the surface's derived status channel, which
/// an operator declares for every surface and without which the last attachment's
/// terminal stamp has nowhere to land.
fn spanning_link_config() -> brenn_lib::config::BrennConfig {
    use brenn_lib::messaging::ComponentGrant;
    use brenn_lib::messaging::config::{
        ChannelConfigRaw, Depth, LinkConfigRaw, LinkEndpointRaw, LinkHostRaw,
        MessagingGlobalConfig, WasmConsumerConfigRaw,
    };
    use brenn_messaging_boot::test_fixtures::{
        io_port_raw, minimal_surface_raw, minimal_wasm_consumer, surface_io_port_raw,
    };

    let worker = WasmConsumerConfigRaw {
        slug: "worker".to_string(),
        package: "worker".to_string(),
        grants: vec![ComponentGrant::Ports],
        io_ports: vec![io_port_raw(
            LINK_PORT,
            None,
            Depth::Bounded(4),
            Depth::Bounded(4),
        )],
        ..minimal_wasm_consumer()
    }
    .implying_its_vocabulary();
    let deskbar = brenn_lib::messaging::config::SurfaceConfigRaw {
        io_ports: vec![surface_io_port_raw(
            COMPONENT,
            LINK_PORT,
            None,
            Depth::Bounded(4),
            Depth::Bounded(4),
        )],
        ..minimal_surface_raw()
    }
    .implying_component_vocabularies();
    let status_channel = ChannelConfigRaw {
        address: Some(brenn_surface_server::description::surface_status_bare(
            &brenn_surface_server::fixtures_config::description_params().prefix,
            "deskbar",
        )),
        send_rate: None,
        // A durable channel's uuid names its DB row, so config must state one.
        uuid: Some(Uuid::new_v4().to_string()),
        address_prefix: None,
        description: None,
        // Both depths are sizing decisions config must state; the status channel
        // carries one latest-wins document.
        push_depth: Some(Depth::Bounded(1)),
        retain_depth: Some(Depth::Bounded(1)),
        // A durable channel states the reaper's disk frontier too.
        standing_retain_depth: Some(Depth::Bounded(1)),
        noise: None,
        sink: None,
        wake_min: None,
    };
    brenn_lib::config::BrennConfig {
        messaging: MessagingGlobalConfig::default(),
        channels: vec![status_channel],
        wasm_consumers: vec![worker],
        surfaces: vec![deskbar],
        links: vec![LinkConfigRaw {
            link: "span".to_string(),
            description: None,
            endpoints: vec![
                LinkEndpointRaw {
                    host: LinkHostRaw::Wasm {
                        slug: "worker".to_string(),
                    },
                    port: LINK_PORT.to_string(),
                    publishes: true,
                    subscribes: true,
                    io_port: true,
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(4)),
                },
                LinkEndpointRaw {
                    host: LinkHostRaw::Surface {
                        slug: "deskbar".to_string(),
                        instance: COMPONENT.to_string(),
                    },
                    port: LINK_PORT.to_string(),
                    publishes: true,
                    subscribes: true,
                    io_port: true,
                    push_depth: Some(Depth::Bounded(4)),
                    retain_depth: Some(Depth::Bounded(4)),
                },
            ],
        }],
        ..brenn_lib::config::BrennConfig::default()
    }
}

/// **The address boot registered and the address the page is handed are one.**
/// An anonymous auto channel's address is a uuid nobody wrote down, derived at
/// boot from a `link` alone; if the directory's derivation and the
/// surface resolution's ever drift, the page subscribes a name the directory
/// does not hold. Everything below rides the boot-derived address read back off
/// the resolution — the subscribe, the delivery-time ACL (satisfied only by the
/// matcher the lowering pass injected), and the backend publish.
#[tokio::test]
async fn surface_ws_an_auto_channel_carries_a_backend_publish_to_the_page() {
    use brenn_messaging::publish::WasmPublish;

    let db = brenn_server::test_support::init_db_memory();
    let harness =
        crate::surface_boot_harness::booted_surface_harness(&db, &spanning_link_config()).await;
    let address = harness.surface_sub_address(COMPONENT, LINK_PORT);
    assert!(
        address.starts_with("ephemeral:auto."),
        "a connection spanning the wire needs a transportable channel, got {address:?}"
    );
    assert!(
        harness.surfaces[0].policy.allows_channel_access(&address),
        "the connection is the authorization signal: boot injected the surface's matcher"
    );

    let messenger = Arc::clone(&harness.messenger);
    let flusher = harness.flusher.clone();
    let alerts = Arc::clone(&harness.alerts);
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(harness.state).await;

    let (mut ws, _session_id) = attach(&base, &token).await;
    send(
        &mut ws,
        &ClientFrame::Subscribe {
            channel: address.clone(),
            push_depth: 4,
            retain_depth: 4,
            resume: None,
        },
    )
    .await;
    match next_server_frame(&mut ws).await {
        ServerFrame::SubscribeResult {
            channel,
            outcome,
            replay_count,
            ..
        } => {
            assert_eq!(channel, address);
            assert_eq!(outcome, SubscribeOutcome::Ok);
            assert_eq!(replay_count, 0, "a fresh ring has nothing to replay");
        }
        other => panic!("expected SubscribeResult, got {other:?}"),
    }

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
        ServerFrame::Deliver { channel, rows } => {
            assert_eq!(channel, address);
            let [row] = rows.as_slice() else {
                panic!("one backend publish is a one-row pass, got {rows:?}");
            };
            assert_eq!(row.envelope.body, "from-the-backend");
        }
        other => panic!("expected Deliver on the auto channel, got {other:?}"),
    }

    assert_no_alerts(&flusher, &alerts, "auto channel delivery to a surface").await;
}

// ---------------------------------------------------------------------------
// The terminal stamp
// ---------------------------------------------------------------------------

/// Read `(sender, body)` for every row persisted on a durable channel.
async fn read_channel_messages(db: &brenn_db::Db, channel_uuid: Uuid) -> Vec<(String, String)> {
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

/// Poll `channel_uuid` until `pred` holds over its persisted rows (or ~2 s). The
/// stamp is written after the socket closes and has no wire ack, so a reader
/// waits on the row rather than on a response.
async fn wait_for_channel<F>(
    db: &brenn_db::Db,
    channel_uuid: Uuid,
    pred: F,
) -> Vec<(String, String)>
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

/// **The surface's last detach is what the server stamps.** A page that is gone
/// cannot report that it is gone, so the route writes the `disconnected`
/// document itself, under the surface's bare identity, once the attachment that
/// ended was the last one.
#[tokio::test]
async fn surface_ws_the_last_detach_stamps_the_status_channel() {
    let db = brenn_server::test_support::init_db_memory();
    let SurfaceTestHarness {
        state, status_uuid, ..
    } = deskbar_harness(&db, vec![], 4).await;
    let (token, _) = setup_authenticated_user(&db).await;
    let (base, _sd) = spawn_test_server(state).await;

    let (mut ws, session_id) = attach(&base, &token).await;
    ws.close(None).await.expect("close the socket");
    drain_until_closed(&mut ws).await;

    let rows = wait_for_channel(&db, status_uuid, |rows| !rows.is_empty()).await;
    let (sender, body) = rows.last().expect("the terminal stamp landed");
    assert_eq!(
        sender, "surface:deskbar",
        "the stamp is the server's, written under the surface's bare identity"
    );
    let stamp = brenn_surface_schema::telemetry::DisconnectedStamp::parse(body)
        .expect("the stamp is a valid disconnected document");
    assert_eq!(stamp.reason, "session closed");
    // The whole point of the field is answering "which attachment went away", so
    // it is compared against the id the `Welcome` named rather than merely
    // checked present: a freshly minted uuid, or a sibling's, would pass that.
    assert_eq!(
        stamp.session.as_deref(),
        Some(session_id.as_str()),
        "a terminal stamp names the attachment that closed"
    );
}

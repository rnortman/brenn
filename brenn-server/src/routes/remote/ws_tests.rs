//! Route-level integration tests for `GET /remote/{slug}/ws`: the auth ladder,
//! the capacity gate, and the opening frames the shared attachment session
//! writes over a real socket once a daemon is admitted.
//!
//! What a frame *means* is the attachment session's own business and is pinned
//! beside it (`brenn-attach-server`'s suites). What this suite owes is what those
//! cannot see: that an unauthenticated caller learns nothing, that a valid token
//! reaches the session with *this* remote's profile, and that the registry key
//! the route registers under is the one the delivery path looks up.

use brenn_attach_proto::{ClientFrame, SUPPORTED_VERSIONS, ServerFrame, max_client_frame_bytes};
use brenn_lib::messaging::AttachScope;
use brenn_lib::messaging::remote::RemoteConfigRaw;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use super::test_fixtures::{SLUG, TEST_MAX_BODY_BYTES, TOKEN, fleet, remote_harness};
use crate::test_support::http::{
    UpgradeProbe, http_to_ws_url, remote_ws_open, spawn_test_server, ws_upgrade_probe,
};
use brenn_attach_server::registry::{AttachSessionHandle, SessionCaps};

type RemoteWs =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The upgrade URL for a slug on a running rig, for the tungstenite client.
fn remote_ws_url(base: &str, slug: &str) -> String {
    http_to_ws_url(base, &format!("/remote/{slug}/ws"))
}

/// The same URL for the reqwest probes, which speak `http://` and ask for the
/// upgrade by header.
fn remote_http_url(base: &str, slug: &str) -> String {
    format!("{base}/remote/{slug}/ws")
}

/// Probe the route with a bearer credential, or with no `Authorization` header
/// at all when `credential` is `None`.
async fn probe(base: &str, slug: &str, credential: Option<&str>) -> UpgradeProbe {
    let url = remote_http_url(base, slug);
    match credential {
        Some(value) => ws_upgrade_probe(&url, &[("authorization", value)]).await,
        None => ws_upgrade_probe(&url, &[]).await,
    }
}

async fn next_server_frame(ws: &mut RemoteWs) -> ServerFrame {
    loop {
        match ws.next().await.expect("stream ended").expect("frame error") {
            Message::Text(text) => {
                return serde_json::from_str(&text).expect("a server frame");
            }
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("expected a text frame, got {other:?}"),
        }
    }
}

async fn send(ws: &mut RemoteWs, frame: &ClientFrame) {
    let text = serde_json::to_string(frame).unwrap();
    ws.send(Message::Text(text.into())).await.unwrap();
}

/// Read the peer's `Hello`, answer it, and read what follows.
async fn open_attachment(ws: &mut RemoteWs) -> ServerFrame {
    match next_server_frame(ws).await {
        ServerFrame::Hello { versions, .. } => assert_eq!(versions, SUPPORTED_VERSIONS),
        other => panic!("expected the peer's Hello first, got {other:?}"),
    }
    send(
        ws,
        &ClientFrame::Hello {
            versions: SUPPORTED_VERSIONS,
            ident: "remote-ws-tests".to_string(),
        },
    )
    .await;
    next_server_frame(ws).await
}

/// A valid token upgrades, the handshake negotiates, and the `Welcome` names the
/// remote's own principal and its own limits — the proof that the route reached
/// `run_attach_session` with *this* remote's profile.
#[tokio::test]
async fn a_valid_token_attaches_and_welcomes_as_the_remote_principal() {
    let db = crate::test_support::init_db_memory();
    let harness = remote_harness(&db, fleet).await;
    let (base, _sd) = spawn_test_server(harness.state.clone()).await;

    let mut ws = remote_ws_open(&remote_ws_url(&base, SLUG), TOKEN).await;
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
            assert_eq!(participant_id, "remote:pod-kitchen");
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
            assert!(alert_granted, "the fleet block grants alert");
        }
        other => panic!("expected Welcome after the handshake, got {other:?}"),
    }
    assert!(
        harness.captured().await.is_empty(),
        "a clean attach fires no security event"
    );
}

/// The route registers under `remote:<slug>`, which is where the delivery path
/// looks a remote's sessions up — and which is disjoint from the bare slug a
/// surface of the same name would hold.
#[tokio::test]
async fn the_session_registers_under_the_remote_prefixed_key() {
    let db = crate::test_support::init_db_memory();
    let harness = remote_harness(&db, fleet).await;
    let registry = harness.state.attach_registry.clone();
    let (base, _sd) = spawn_test_server(harness.state.clone()).await;

    let mut ws = remote_ws_open(&remote_ws_url(&base, SLUG), TOKEN).await;
    let _welcome = open_attachment(&mut ws).await;

    let key = AttachScope::remote(SLUG).registry_key().into_owned();
    assert_eq!(key, "remote:pod-kitchen");
    assert_eq!(
        registry.sessions(&key).len(),
        1,
        "the attached session is findable under the prefixed key"
    );
    assert_eq!(
        registry.sessions(SLUG).len(),
        0,
        "and not under the bare slug, which belongs to the surface keyspace"
    );
}

/// Every authentication failure answers the same bytes, so a prober cannot tell
/// an unconfigured slug from a wrong token from a malformed header.
#[tokio::test]
async fn every_auth_failure_answers_identically() {
    let db = crate::test_support::init_db_memory();
    let harness = remote_harness(&db, fleet).await;
    let (base, _sd) = spawn_test_server(harness.state.clone()).await;

    let unknown_slug = probe(&base, "no-such-remote", Some(&format!("Bearer {TOKEN}"))).await;
    let missing_header = probe(&base, SLUG, None).await;
    let malformed_header = probe(&base, SLUG, Some("Basic aGk6dGhlcmU=")).await;
    let empty_credential = probe(&base, SLUG, Some("Bearer ")).await;
    let wrong_token = probe(&base, SLUG, Some("Bearer not-the-token")).await;

    assert_eq!(
        unknown_slug.status,
        axum::http::StatusCode::UNAUTHORIZED,
        "the whole matrix answers 401"
    );
    for (name, observed) in [
        ("missing header", &missing_header),
        ("malformed header", &malformed_header),
        ("empty credential", &empty_credential),
        ("wrong token", &wrong_token),
    ] {
        assert_eq!(
            observed, &unknown_slug,
            "{name} must be byte-identical to the unknown-slug answer"
        );
    }
    assert!(
        unknown_slug.body.is_empty(),
        "the 401 carries no body to distinguish on, got {:?}",
        unknown_slug.body
    );
}

/// Each failure is one `AuthFailure` security event carrying the client IP —
/// including a *missing* header, unlike the cookie middleware, because nothing
/// legitimate reaches `/remote/` anonymously.
#[tokio::test]
async fn each_auth_failure_is_one_auth_failure_event() {
    let db = crate::test_support::init_db_memory();
    let harness = remote_harness(&db, fleet).await;
    let (base, _sd) = spawn_test_server(harness.state.clone()).await;

    for credential in [
        None,
        Some("Basic aGk6dGhlcmU="),
        Some("Bearer not-the-token"),
    ] {
        let _ = probe(&base, SLUG, credential).await;
    }
    let _ = probe(&base, "no-such-remote", Some(&format!("Bearer {TOKEN}"))).await;

    let events = harness.captured().await;
    assert_eq!(events.len(), 4, "one event per refusal, got {events:?}");
    for (source, detail) in &events {
        assert!(
            source.contains("auth_failure"),
            "each refusal is an AuthFailure, got source {source:?}"
        );
        assert!(
            detail.contains("/remote/"),
            "the detail names the route, got {detail:?}"
        );
    }
}

/// An unknown slug's detail is sanitized: a probe cannot inject control bytes
/// into the security-event record through the path segment.
#[tokio::test]
async fn an_unknown_slug_is_sanitized_in_the_security_event() {
    let db = crate::test_support::init_db_memory();
    let harness = remote_harness(&db, fleet).await;
    let (base, _sd) = spawn_test_server(harness.state.clone()).await;

    // Percent-encoded newline + a fake event prefix: if the slug reached the log
    // verbatim it would split the record.
    let url = format!("{base}/remote/evil%0aAuthFailure:%20forged/ws");
    let observed = ws_upgrade_probe(&url, &[("authorization", "Bearer x")]).await;
    assert_eq!(observed.status, axum::http::StatusCode::UNAUTHORIZED);

    let events = harness.captured().await;
    assert_eq!(events.len(), 1, "one refusal, one event");
    let (_source, detail) = &events[0];
    // Everything after `/remote/` is client-controlled; the dispatcher's own
    // `IP: …` prefix ahead of it carries the only legitimate newline.
    let (_prefix, client_bytes) = detail
        .split_once("/remote/")
        .expect("the detail names the route");
    assert!(
        !client_bytes.contains('\n'),
        "the slug must not carry a newline into the record, got {detail:?}"
    );
    assert!(
        detail.contains("evil\\nAuthFailure"),
        "the newline is escaped rather than dropped, so the probe is still \
         legible in the record, got {detail:?}"
    );
}

/// At the configured session cap the route answers 503 with a warning and no
/// security event: a remote at its cap is an operator's topology or a netsplit
/// corpse, and banning the pod's IP for either would turn a transient into an
/// outage.
#[tokio::test]
async fn at_the_session_cap_the_route_answers_503_without_a_security_event() {
    let db = crate::test_support::init_db_memory();
    let harness = remote_harness(&db, fleet).await;
    let registry = harness.state.attach_registry.clone();
    let caps = SessionCaps {
        per_attacher: 2,
        per_account: 2,
    };
    let key = AttachScope::remote(SLUG).registry_key().into_owned();
    // Fill both slots with handles the route itself would have registered.
    let mut guards = Vec::new();
    for _ in 0..2 {
        guards.push(
            registry
                .try_register(
                    &key,
                    AttachSessionHandle::for_test("remote:pod-kitchen"),
                    caps,
                )
                .expect("both configured slots are free"),
        );
    }

    let (base, _sd) = spawn_test_server(harness.state.clone()).await;
    let observed = probe(&base, SLUG, Some(&format!("Bearer {TOKEN}"))).await;
    assert_eq!(
        observed.status,
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "a third session is refused at the default cap of 2"
    );
    assert!(
        harness.captured().await.is_empty(),
        "a full remote is not fail2ban signal"
    );

    guards.pop();
    let admitted = probe(&base, SLUG, Some(&format!("Bearer {TOKEN}"))).await;
    assert_eq!(
        admitted.status,
        axum::http::StatusCode::SWITCHING_PROTOCOLS,
        "the freed slot admits the next dial"
    );
}

/// `max_sessions = 1` is the strict single-connection knob: one session, and the
/// second dial is refused until the first drains.
#[tokio::test]
async fn max_sessions_one_admits_exactly_one_session() {
    let db = crate::test_support::init_db_memory();
    let harness = remote_harness(&db, |token| RemoteConfigRaw {
        max_sessions: Some(1),
        ..fleet(token)
    })
    .await;
    let (base, _sd) = spawn_test_server(harness.state.clone()).await;

    let mut first = remote_ws_open(&remote_ws_url(&base, SLUG), TOKEN).await;
    let _welcome = open_attachment(&mut first).await;

    let second = probe(&base, SLUG, Some(&format!("Bearer {TOKEN}"))).await;
    assert_eq!(
        second.status,
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "the strict knob admits one"
    );
}

/// The route sits outside the cookie-auth group: a session cookie is not a
/// credential here, and its absence produces no login redirect.
#[tokio::test]
async fn a_session_cookie_is_not_a_credential_and_there_is_no_redirect() {
    let db = crate::test_support::init_db_memory();
    let harness = remote_harness(&db, fleet).await;
    let (base, _sd) = spawn_test_server(harness.state.clone()).await;
    let (session_token, _csrf) = crate::test_support::http::setup_authenticated_user(&db).await;

    let observed = ws_upgrade_probe(
        &remote_http_url(&base, SLUG),
        &[("cookie", &format!("brenn_session={session_token}"))],
    )
    .await;
    assert_eq!(
        observed.status,
        axum::http::StatusCode::UNAUTHORIZED,
        "an authenticated browser is not a remote"
    );
    assert!(
        !observed.headers.iter().any(|(name, _)| name == "location"),
        "daemons do not log in: no redirect, got {:?}",
        observed.headers
    );
}

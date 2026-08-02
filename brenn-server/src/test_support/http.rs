use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use brenn_lib::auth::password::hash_password;
use brenn_lib::auth::session::Session;
use brenn_lib::auth::session::create_session;
use brenn_lib::auth::user::{User, create_user};
use brenn_lib::db;

use crate::client_ip::ClientIp;
use crate::state::AppState;

/// Canonical username created by `setup_authenticated_user`. Any test that
/// prefills registry state for that same authenticated user references this
/// constant so the join is a named coupling, not a repeated literal.
pub(crate) const TEST_USERNAME: &str = "testuser";

/// Helper: collect response body as string.
pub(crate) async fn body_string(body: Body) -> String {
    use http_body_util::BodyExt;
    let bytes = body.collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Helper: extract the Set-Cookie header value from a response.
pub(crate) fn get_set_cookie(response: &axum::http::Response<Body>) -> Option<String> {
    response
        .headers()
        .get("set-cookie")
        .map(|v| v.to_str().unwrap().to_string())
}

/// Helper: extract the session token from a Set-Cookie header.
pub(crate) fn extract_session_token(set_cookie: &str) -> &str {
    set_cookie
        .strip_prefix("brenn_session=")
        .unwrap()
        .split(';')
        .next()
        .unwrap()
}

/// Helper: register a user and return session token + CSRF token.
pub(crate) async fn setup_authenticated_user(db: &db::Db) -> (String, String) {
    let conn = db.lock().await;
    let password_hash = hash_password(b"test-password-12chars");
    let user_id = create_user(&conn, TEST_USERNAME, &password_hash);
    let (session_token, csrf_token) = create_session(&conn, user_id);
    (session_token, csrf_token)
}

/// Inject a `Session` (fixed test-token / testuser) and `ClientIp` into the
/// request extensions. Used by middleware unit tests that exercise layers
/// expecting these extensions to already be present.
pub(crate) fn inject_extensions(mut req: Request<Body>, user_id: i64, ip: IpAddr) -> Request<Body> {
    req.extensions_mut().insert(Session {
        token: "test-token".to_string(),
        csrf_token: "test-csrf".to_string(),
        user: User {
            id: user_id,
            username: "testuser".to_string(),
        },
    });
    req.extensions_mut().insert(ClientIp(ip));
    req
}

/// A running test server. Dropping the handle drops `_shutdown_tx`, which
/// signals graceful shutdown; the serve task then winds down asynchronously and
/// nothing awaits it, so a dropped handle does not prove the server has stopped
/// touching shared state.
pub(crate) struct TestServer {
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
    _serve: tokio::task::JoinHandle<()>,
}

/// Spin up a real server on a random port. Returns the base URL and a
/// [`TestServer`]. The server runs in a background task and stops when the
/// returned handle is dropped.
///
/// A panic on a spawned connection task is absorbed by tokio and reaches no
/// assertion, so a regression that panics after the last frame a test reads
/// leaves that test green.
// TODO(test-task-panic-visibility): make a panicking connection task fail the
// test that provoked it.
pub(crate) async fn spawn_test_server(state: AppState) -> (String, TestServer) {
    use crate::router::build_router;
    let app =
        build_router(state, None, 0, 2576).into_make_service_with_connect_info::<SocketAddr>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let serve = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
    });

    (
        format!("http://{addr}"),
        TestServer {
            _shutdown_tx: shutdown_tx,
            _serve: serve,
        },
    )
}

/// Make an HTTP request with WebSocket upgrade headers to trigger the WS handler.
/// Returns the response status code.
///
/// The cookie-authenticated shorthand over [`ws_upgrade_probe`], which composes
/// the handshake request. One spelling of that request serves both, so a change
/// to it cannot leave the two suites probing different things.
pub(crate) async fn ws_upgrade_status(url: &str, session_token: Option<&str>) -> StatusCode {
    let cookie = session_token.map(|token| format!("brenn_session={token}"));
    let extra: Vec<(&str, &str)> = cookie
        .as_deref()
        .map(|value| ("cookie", value))
        .into_iter()
        .collect();
    ws_upgrade_probe(url, &extra).await.status
}

/// Everything a client can observe of a refused WS upgrade, for the routes whose
/// contract is that two different failures answer *identically*.
///
/// `headers` excludes `date` — it moves with the clock, so including it would
/// make a byte-identity comparison flap — and is sorted so header ordering does
/// not enter the comparison either.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct UpgradeProbe {
    pub status: StatusCode,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// Send a WS upgrade request carrying arbitrary extra headers and return the
/// whole observable response. The counterpart of [`ws_upgrade_status`] for
/// routes that authenticate with something other than the session cookie.
pub(crate) async fn ws_upgrade_probe(url: &str, extra: &[(&str, &str)]) -> UpgradeProbe {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    use base64ct::{Base64, Encoding as _};
    let ws_key = Base64::encode_string(&uuid::Uuid::new_v4().into_bytes());
    let mut req = client
        .get(url)
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", ws_key)
        .version(reqwest::Version::HTTP_11);
    for (name, value) in extra {
        req = req.header(*name, *value);
    }
    let response = req.send().await.unwrap();
    let status = response.status();
    let mut headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .filter(|(name, _)| name.as_str() != "date")
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    headers.sort();
    UpgradeProbe {
        status,
        headers,
        body: response.text().await.unwrap(),
    }
}

/// Open a real WebSocket against `url` with a bearer token and return the live
/// stream. The remote route's counterpart of [`surface_ws_open`]; it calls the
/// same client-side helper the native bearer connector does, so the header shape
/// under test is the production one.
pub(crate) async fn remote_ws_open(
    url: &str,
    token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().unwrap();
    brenn_attach_client::insert_bearer_token(req.headers_mut(), token).unwrap();
    let (ws, _resp) = tokio_tungstenite::connect_async(req)
        .await
        .expect("WS handshake should succeed (HTTP 101) for this test");
    ws
}

/// Convert an `http://` base URL to a `ws://` URL with the given path.
/// `spawn_test_server` hands back `http://127.0.0.1:<port>`; the
/// tungstenite client needs `ws://…`. Swaps the scheme.
pub(crate) fn http_to_ws_url(http_base: &str, path: &str) -> String {
    let ws_base = http_base.strip_prefix("http://").unwrap();
    format!("ws://{ws_base}{path}")
}

/// Parse a `spawn_test_server` base URL (`http://<addr>`) into its `SocketAddr`.
/// One of two sites (alongside `http_to_ws_url`) that knows
/// `spawn_test_server`'s `http://{addr}` shape.
pub(crate) fn http_base_addr(http_base: &str) -> SocketAddr {
    http_base
        .strip_prefix("http://")
        .expect("the test server hands back an http:// base")
        .parse()
        .expect("the base names a socket address")
}

/// Open a real WebSocket against `url` with the session cookie and
/// return the first frame the server sends. Uses tokio-tungstenite
/// so the test can distinguish a Close(3001) from a successful
/// Welcome text frame.
pub(crate) async fn ws_connect_first_frame(
    url: &str,
    session_token: &str,
) -> tokio_tungstenite::tungstenite::Message {
    use futures::StreamExt;
    let mut ws = surface_ws_open(url, session_token).await;
    ws.next()
        .await
        .expect("WS stream ended before any frame")
        .expect("WS frame error")
}

/// Open a real WebSocket against `url` with the session cookie and return the
/// live stream so a test can drive the full session (send frames, read
/// heartbeats, observe teardown). Unlike `ws_connect_first_frame`, the stream
/// is kept open for the caller.
pub(crate) async fn surface_ws_open(
    url: &str,
    session_token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = url.into_client_request().unwrap();
    brenn_attach_client::insert_session_cookie(req.headers_mut(), session_token).unwrap();
    let (ws, _resp) = tokio_tungstenite::connect_async(req)
        .await
        .expect("WS handshake should succeed (HTTP 101) for this test");
    ws
}

/// Shared body for the two stale-client tests: expect a Close
/// frame with STALE_CLIENT_CLOSE_CODE carrying the server's
/// BUILD_ID, and no captured alerts afterwards.
pub(crate) async fn assert_stale_client_close_and_no_alert(
    msg: tokio_tungstenite::tungstenite::Message,
    alerts: &Arc<std::sync::Mutex<Vec<(String, String)>>>,
    context: &str,
) {
    match msg {
        tokio_tungstenite::tungstenite::Message::Close(Some(frame)) => {
            assert_eq!(
                u16::from(frame.code),
                crate::routes::ws::STALE_CLIENT_CLOSE_CODE
            );
            let reason: &str = frame.reason.as_ref();
            assert_eq!(reason, crate::test_support::TEST_BUILD_ID);
        }
        other => panic!("{context}: expected Close(3001), got {other:?}"),
    }
    tokio::task::yield_now().await;
    let captured = alerts.lock().unwrap().clone();
    assert!(
        captured.is_empty(),
        "{context}: stale-client close must not fire any alert, got: {captured:?}"
    );
}

/// Build a minimal multipart/form-data body with one file field.
pub(crate) fn multipart_body(filename: &str, content: &[u8]) -> (String, Vec<u8>) {
    let boundary = "----TestBoundary12345";
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
    body.extend_from_slice(content);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

/// Fetch the landing page `/` with the given session cookie.
pub(crate) async fn fetch_landing_page(
    app: Router,
    session_token: &str,
) -> axum::http::Response<Body> {
    use tower::ServiceExt;
    app.oneshot(
        Request::get("/")
            .header("cookie", format!("brenn_session={session_token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
}

/// Send a GET to `path` from `xff_ip` (via `X-Forwarded-For`) and return the
/// response status.
///
/// Clones the router so callers can fire multiple requests against the same
/// shared rate-limiter state (the `Arc` inside `GovernorConfig` keeps the
/// bucket alive across clones).
pub(crate) async fn xff_get_status(app: &Router, path: &str, xff_ip: &str) -> StatusCode {
    use tower::ServiceExt;
    let req = Request::get(path)
        .header("x-forwarded-for", xff_ip)
        .body(Body::empty())
        .unwrap();
    app.clone().oneshot(req).await.unwrap().status()
}

/// Send a GET to `/auth/login` from `xff_ip` and return the response status.
/// Clones the router so callers can fire multiple requests against the
/// same shared rate-limiter state (the Arc inside `GovernorConfig`
/// keeps the bucket alive across clones).
pub(crate) async fn auth_login_status(app: &Router, xff_ip: &str) -> StatusCode {
    xff_get_status(app, "/auth/login", xff_ip).await
}

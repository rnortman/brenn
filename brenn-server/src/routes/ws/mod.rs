//! WebSocket endpoint for the CC ↔ browser bridge.

mod artifacts;
mod bridge;
mod connection;
mod conversation;
mod dispatch;
mod event_loop;
mod history;
mod messaging;
mod target;
mod todos;
mod usage;

use axum::Extension;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use brenn_lib::auth::session::Session;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::ws_types::ViewportClass;
use futures::SinkExt;
use tracing::{error, info};

use self::event_loop::{WsHandshake, handle_ws};

use crate::client_ip::ClientIp;
use crate::state::AppState;

/// WebSocket close code (RFC 6455 §7.4.2 private range 3000-3999) that
/// tells the browser "your bundle predates the deployed server; reload
/// to pick up the new JS." The frontend maps this code to
/// `location.reload()` with a 3-strike sessionStorage loop guard. Kept
/// in sync with the `STALE_CLIENT_CLOSE_CODE` constant in
/// `frontend/src/ws.ts`.
pub const STALE_CLIENT_CLOSE_CODE: u16 = 3001;

const _: () = assert!(STALE_CLIENT_CLOSE_CODE == brenn_surface_proto::STALE_BUILD_CLOSE_CODE);

/// Query parameters for the WS endpoint.
#[derive(serde::Deserialize)]
pub(crate) struct WsQuery {
    /// Optional conversation ID to select on connect (initial load or reconnect).
    conv: Option<i64>,
    /// Last seen sequence number for incremental reconnect. When present with
    /// `conv`, the server replays only messages with `seq > last_seq` instead
    /// of the full history.
    seq: Option<i64>,
    /// Viewport class from the browser. Required on every connect so the
    /// server can emit the correct SetLayout BEFORE any history frames —
    /// eliminates the "replay into the wrong DOM shape" race.
    ///
    /// Typed as `Option<_>` deliberately (not bare `ViewportClass`): the
    /// handler matches on `None` so it can dispatch a `SchemaViolation`
    /// security event (fail2ban signal) before returning 400, rather than
    /// letting Axum's `Query` extractor reject silently with no security
    /// log.
    viewport: Option<ViewportClass>,
    /// Build identifier (short SHA, semver tag, or `unknown-dev`) from
    /// the client bundle. Used to force-refresh stale browser tabs after
    /// protocol-breaking deploys: the server compares it to its own
    /// `BUILD_ID`, and anything that doesn't match (or is missing) is
    /// classified as a stale client rather than a schema violation.
    ///
    /// Typed as `Option<_>` deliberately — missing vs mismatched land in
    /// explicit separate branches in `ws_handler`, and a missing value
    /// must not fire `SchemaViolation` (it's the user's own stale tab,
    /// not a malicious probe).
    build: Option<String>,
}

/// GET /app/{slug}/ws — upgrade to WebSocket. Auth middleware has already validated the session.
/// Validates the app slug and user access before upgrading the connection.
pub async fn ws_handler(
    Path(slug): Path<String>,
    Query(query): Query<WsQuery>,
    ws: WebSocketUpgrade,
    Extension(session): Extension<Session>,
    Extension(device): Extension<brenn_lib::auth::device::Device>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode> {
    // Validate app exists.
    let app = match state.apps.get(&slug) {
        Some(app) => app,
        None => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::UnrecognizedUrl,
                ip,
                &format!("/app/{slug}/ws"),
            );
            return Err(StatusCode::NOT_FOUND);
        }
    };

    // Validate user has access.
    if !app.user_has_access(&session.user.username) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::AuthFailure,
            ip,
            &format!(
                "user {} denied WS access to app {}",
                session.user.username, slug
            ),
        );
        return Err(StatusCode::FORBIDDEN);
    }

    info!(
        user = %session.user.username,
        app = %slug,
        conv = ?query.conv,
        seq = ?query.seq,
        viewport = ?query.viewport,
        build = ?query.build,
        "WebSocket upgrade"
    );

    // Build-version handshake. Runs BEFORE the viewport check so a
    // stale tab (pre-handshake JS: no `build`, no `viewport`) gets the
    // stale-client close path rather than a SchemaViolation alert.
    // A missing `build` is definitionally an old bundle — we never
    // deploy one without it — so missing and mismatched both take the
    // same Close(3001) path.
    let build_id = state.build_id;
    match query.build.as_deref() {
        Some(v) if v == build_id => {}
        Some(mismatched) => {
            let mismatched = mismatched.to_string();
            return Ok(ws.on_upgrade(move |socket| async move {
                close_with_stale_client(socket, &mismatched, build_id).await;
            }));
        }
        None => {
            return Ok(ws.on_upgrade(move |socket| async move {
                close_with_stale_client(socket, "<missing>", build_id).await;
            }));
        }
    }

    // Viewport is required. Both endpoints are under our control (ws.ts
    // always emits it); a missing param means a stale client or a manual
    // probe. Per CLAUDE.md (no fallbacks, fail fast on unexpected input),
    // reject the upgrade and feed fail2ban via the SchemaViolation path.
    let viewport = match query.viewport {
        Some(v) => v,
        None => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                &format!(
                    "WS connect missing required viewport query param (user {}, app {})",
                    session.user.username, slug
                ),
            );
            error!(
                user = %session.user.username,
                app = %slug,
                "rejecting WS upgrade: missing viewport query param"
            );
            return Err(StatusCode::BAD_REQUEST);
        }
    };
    let device_id = device.id;
    Ok(ws.on_upgrade(move |socket| {
        handle_ws(WsHandshake {
            socket,
            session,
            client_ip: ip,
            state,
            app_slug: slug,
            requested_conversation_id: query.conv,
            requested_last_seq: query.seq,
            viewport_class: viewport,
            device_id,
        })
    }))
}

/// Accept the upgrade, send `Close(STALE_CLIENT_CLOSE_CODE)` carrying
/// the server build, and tear down. The upgrade-then-close shape is
/// required: a pre-upgrade 4xx surfaces in the browser as a generic
/// connection failure (ws.ts would backoff), whereas an accepted
/// upgrade + Close reaches `onclose` with `event.code`, which is what
/// drives the reload.
///
/// The awaited `.close()` is not cosmetic: without it, some TLS impls
/// can deliver an abrupt TCP close before the Close frame bytes hit
/// the wire, which the browser surfaces as `wasClean: false` /
/// code 1006 — the client would then take the reconnect path.
///
/// Errors on `send`/`close` are ignored: the socket is being dropped
/// and the stale event is already logged.
///
/// `pub(crate)` so the surface WS route reuses the exact stale-client
/// close ceremony and close code rather than copying it.
pub(crate) async fn close_with_stale_client(
    mut socket: WebSocket,
    client_build: &str,
    server_build: &str,
) {
    info!(
        client_build = %client_build,
        server_build = %server_build,
        "closing stale-client WS connection"
    );
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: STALE_CLIENT_CLOSE_CODE,
            reason: server_build.to_string().into(),
        })))
        .await;
    let _ = socket.close().await;
}

#[cfg(test)]
mod testing;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::StatusCode;
    use brenn_lib::db;
    use indexmap::IndexMap;

    use super::testing::poll_until_db_count;
    use crate::test_support::app_config::default_test_app_config;
    use crate::test_support::http::{
        assert_stale_client_close_and_no_alert, http_to_ws_url, setup_authenticated_user,
        spawn_test_server, ws_connect_first_frame, ws_upgrade_status,
    };
    use crate::test_support::state::{
        test_state, test_state_with_apps, test_state_with_capturing_alerter,
    };

    // These tests use a real TcpListener because Axum's WebSocketUpgrade extractor
    // requires a real HTTP/1.1 upgrade handshake that oneshot() cannot provide.

    #[tokio::test]
    async fn ws_handler_unknown_slug_returns_404() {
        let db = db::init_db_memory();
        let state = test_state(&db);
        let (session_token, _) = setup_authenticated_user(&db).await;

        let (base_url, _shutdown) = spawn_test_server(state).await;

        let status = ws_upgrade_status(
            &format!("{base_url}/app/nonexistent/ws"),
            Some(&session_token),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn ws_handler_missing_viewport_returns_400() {
        // Per the mobile-startup-refresh-architecture rewrite: the viewport
        // query param is required on every WS connect so the server can
        // emit the correct SetLayout BEFORE any history frames. Missing
        // `viewport` is treated as a stale client / probe and rejected
        // with `BAD_REQUEST` + a `SchemaViolation` security event
        // (fail2ban signal). No silent fallback.
        //
        // Passes a matching `build` because the handshake check runs
        // first; without it, the request would take the Close(3001)
        // path and return HTTP 101 instead of BAD_REQUEST.
        let db = db::init_db_memory();
        let state = test_state(&db);
        let (session_token, _) = setup_authenticated_user(&db).await;

        let (base_url, _shutdown) = spawn_test_server(state).await;

        let status = ws_upgrade_status(
            &format!(
                "{base_url}/app/test/ws?build={}",
                crate::test_support::TEST_BUILD_ID
            ),
            Some(&session_token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn ws_handler_access_denied_returns_403() {
        let db = db::init_db_memory();
        let mut apps = IndexMap::new();
        let mut cfg = default_test_app_config("restricted", "Restricted App");
        cfg.allowed_users = vec!["otheruser".to_string()];
        apps.insert("restricted".to_string(), cfg);
        let state = test_state_with_apps(&db, Arc::new(apps));
        let (session_token, _) = setup_authenticated_user(&db).await;

        let (base_url, _shutdown) = spawn_test_server(state).await;

        let status = ws_upgrade_status(
            &format!("{base_url}/app/restricted/ws"),
            Some(&session_token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    // ---------------------------------------------------------------------
    // Stale-tab force-refresh handshake (force-refresh-stale-browser-tabs)
    // ---------------------------------------------------------------------
    //
    // These tests stand up a real axum server and open a real WebSocket
    // using tokio-tungstenite so they can observe the Close frame payload
    // the browser-side `ws.ts` uses as the reload trigger. The existing
    // `ws_upgrade_status` helper only returns the HTTP status and cannot
    // see the Close frame, which is why it's not reused here.

    #[tokio::test]
    async fn ws_handler_missing_build_closes_stale_client_no_alert() {
        // Regression test for the ticket's done criterion: a stale tab
        // running the pre-handshake JS (no `build`, no `viewport`) must
        // be classified as stale-client, not SchemaViolation. The build
        // check runs first, so the absence of `viewport` is irrelevant
        // here — the server closes with 3001 before it gets to the
        // viewport branch, and no alert is dispatched.
        let db = db::init_db_memory();
        let (state, alerts, _alert_handle) = test_state_with_capturing_alerter(&db);
        let (session_token, _) = setup_authenticated_user(&db).await;

        let (base_url, _shutdown) = spawn_test_server(state).await;
        let ws_url = http_to_ws_url(&base_url, "/app/test/ws");

        let msg = ws_connect_first_frame(&ws_url, &session_token).await;
        assert_stale_client_close_and_no_alert(msg, &alerts, "missing build").await;
    }

    #[tokio::test]
    async fn ws_handler_wrong_build_closes_stale_client_no_alert() {
        // A client that sends a `build` param that doesn't match the
        // server's BUILD_ID is a stale bundle (by definition), not a
        // malicious probe. Same Close(3001) path, same no-alert
        // guarantee. Viewport is absent but the build check fires first
        // regardless.
        let db = db::init_db_memory();
        let (state, alerts, _alert_handle) = test_state_with_capturing_alerter(&db);
        let (session_token, _) = setup_authenticated_user(&db).await;

        let (base_url, _shutdown) = spawn_test_server(state).await;
        let ws_url = http_to_ws_url(&base_url, "/app/test/ws?build=wrong-sha");

        let msg = ws_connect_first_frame(&ws_url, &session_token).await;
        assert_stale_client_close_and_no_alert(msg, &alerts, "wrong build").await;
    }

    #[tokio::test]
    async fn ws_handler_matching_build_missing_viewport_still_schema_violates() {
        // Guard against a regression where the build check silently
        // absorbs a legitimate SchemaViolation case. A client that
        // claims a matching build but omits `viewport` is a buggy or
        // malicious client — that's exactly the fail2ban signal we
        // want to keep.
        let db = db::init_db_memory();
        let (state, alerts, _alert_handle) = test_state_with_capturing_alerter(&db);
        let (session_token, _) = setup_authenticated_user(&db).await;

        let (base_url, _shutdown) = spawn_test_server(state).await;
        let url = format!(
            "{base_url}/app/test/ws?build={}",
            crate::test_support::TEST_BUILD_ID
        );

        // `ws_upgrade_status` is sufficient: the upgrade is refused
        // pre-handshake, so there is no Close frame to observe.
        let status = ws_upgrade_status(&url, Some(&session_token)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Give the async alert task time to drain the channel.
        for _ in 0..50 {
            if !alerts.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let captured = alerts.lock().unwrap().clone();
        assert_eq!(
            captured.len(),
            1,
            "expected exactly one SchemaViolation alert, got: {captured:?}"
        );
        let (title, body) = &captured[0];
        let combined = format!("{title} {body}");
        assert!(
            combined.contains("schema_violation") || combined.contains("viewport"),
            "alert does not look like a schema-violation-for-viewport: title={title:?} body={body:?}"
        );
    }

    #[tokio::test]
    async fn ws_handler_matching_build_and_viewport_succeeds() {
        // Happy path: build matches and viewport is present, so the
        // upgrade completes and the server's first frame is the
        // `Welcome` JSON text (not a 3001 Close).
        let db = db::init_db_memory();
        let state = test_state(&db);
        let (session_token, _) = setup_authenticated_user(&db).await;

        let (base_url, _shutdown) = spawn_test_server(state).await;
        let path = format!(
            "/app/test/ws?build={}&viewport=Compact",
            crate::test_support::TEST_BUILD_ID
        );
        let ws_url = http_to_ws_url(&base_url, &path);

        let msg = ws_connect_first_frame(&ws_url, &session_token).await;
        match msg {
            tokio_tungstenite::tungstenite::Message::Text(text) => {
                assert!(
                    text.contains("\"Welcome\""),
                    "first frame should be Welcome JSON, got: {text}"
                );
            }
            tokio_tungstenite::tungstenite::Message::Close(frame) => {
                panic!("unexpected Close frame on happy path: {frame:?}");
            }
            other => panic!("expected Welcome text frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ws_connect_records_session_event() {
        // Verifies that event_loop::handle_ws inserts a `ws_connect` usage
        // event row and that the row's device_id matches the device the
        // middleware auto-created for the connection.
        let db = db::init_db_memory();
        let state = test_state(&db);
        let (session_token, _) = setup_authenticated_user(&db).await;

        let (base_url, _shutdown) = spawn_test_server(state).await;
        let path = format!(
            "/app/test/ws?build={}&viewport=Compact",
            crate::test_support::TEST_BUILD_ID
        );
        let ws_url = http_to_ws_url(&base_url, &path);

        // Trigger the full event_loop path; record_ws_connect runs after
        // run_setup completes (event_loop.rs:171-181).
        ws_connect_first_frame(&ws_url, &session_token).await;

        // record_ws_connect runs on the server task after run_setup, which
        // is after the Welcome frame (first frame) has been sent. Poll until
        // the event row appears (up to 2 s) to avoid a fixed sleep that is
        // either too short under load or wastes time on fast machines.
        poll_until_db_count(
            &db,
            "SELECT COUNT(*) FROM usage_events WHERE event_type = 'ws_connect'",
            1,
            2000,
        )
        .await;

        let conn = db.lock().await;

        // Exactly one ws_connect event.
        let event_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_events WHERE event_type = 'ws_connect'",
                [],
                |row| row.get(0),
            )
            .expect("query usage_events ws_connect count");
        assert_eq!(event_count, 1, "expected exactly one ws_connect event row");

        // The event's device_id must match the single device the middleware
        // created for this connection. Query devices to get the auto-created id,
        // then assert it equals the event's device_id.
        let event_device_id: i64 = conn
            .query_row(
                "SELECT device_id FROM usage_events WHERE event_type = 'ws_connect'",
                [],
                |row| row.get(0),
            )
            .expect("query usage_events device_id");

        // Assert exactly one device was created (not just the first of several)
        // so a regression that creates multiple devices per connection is caught.
        let device_rows: Vec<i64> = {
            let mut stmt = conn
                .prepare("SELECT id FROM devices")
                .expect("prepare devices query");
            stmt.query_map([], |row| row.get(0))
                .expect("query devices")
                .map(|r| r.expect("devices row"))
                .collect()
        };
        assert_eq!(
            device_rows.len(),
            1,
            "expected exactly one device row, got: {device_rows:?}"
        );
        let created_device_id = device_rows[0];
        assert_eq!(
            event_device_id, created_device_id,
            "usage_events.device_id must match the device created by the middleware"
        );
    }

    #[tokio::test]
    async fn acceptance_two_tabs_two_devices() {
        // Verifies that two WS connections from the same user (each with no
        // device cookie) produce two distinct device rows and two distinct
        // usage_sessions rows. This mirrors the "two browser tabs" scenario
        // where each tab gets its own device identity.
        let db = db::init_db_memory();
        let state = test_state(&db);
        let (session_token, _) = setup_authenticated_user(&db).await;

        let (base_url, _shutdown) = spawn_test_server(state).await;
        let path = format!(
            "/app/test/ws?build={}&viewport=Compact",
            crate::test_support::TEST_BUILD_ID
        );
        let ws_url = http_to_ws_url(&base_url, &path);

        // Two connections with no device cookie → middleware calls
        // resolve_or_create_device(conn, None, user_id, ua) twice,
        // creating two separate device rows.
        ws_connect_first_frame(&ws_url, &session_token).await;
        ws_connect_first_frame(&ws_url, &session_token).await;

        // Give both record_ws_connect calls time to complete. Poll until
        // both session rows appear (up to 2 s) for deterministic behaviour
        // across CI environments.
        poll_until_db_count(&db, "SELECT COUNT(*) FROM usage_sessions", 2, 2000).await;

        let conn = db.lock().await;

        // Two sessions with distinct device_ids.
        let session_rows: Vec<i64> = {
            let mut stmt = conn
                .prepare("SELECT device_id FROM usage_sessions")
                .expect("prepare usage_sessions query");
            stmt.query_map([], |row| row.get(0))
                .expect("query usage_sessions")
                .map(|r| r.expect("usage_sessions row"))
                .collect()
        };
        assert_eq!(
            session_rows.len(),
            2,
            "expected 2 usage_sessions rows, got: {session_rows:?}"
        );
        assert_ne!(
            session_rows[0], session_rows[1],
            "two tabs must produce distinct device_id values in usage_sessions"
        );

        // Also assert that record_ws_connect wrote two ws_connect usage_events
        // rows (one per device) — verifying the direct write, not just the
        // session side-effect.
        let event_rows: Vec<i64> = {
            let mut stmt = conn
                .prepare(
                    "SELECT device_id FROM usage_events WHERE event_type = 'ws_connect' \
                     ORDER BY device_id",
                )
                .expect("prepare usage_events query");
            stmt.query_map([], |row| row.get(0))
                .expect("query usage_events")
                .map(|r| r.expect("usage_events row"))
                .collect()
        };
        assert_eq!(
            event_rows.len(),
            2,
            "expected 2 ws_connect usage_events rows, got: {event_rows:?}"
        );
        assert_ne!(
            event_rows[0], event_rows[1],
            "two tabs must produce distinct device_id values in usage_events"
        );
    }
}

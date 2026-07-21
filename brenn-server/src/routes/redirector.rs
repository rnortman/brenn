//! Redirector route: `GET /r/{nonce}?to={path}`
//!
//! Issues a per-click-unique URL that resolves to a validated `/app/` destination
//! via an HTTP 303 redirect. The per-click nonce defeats Firefox Android's
//! task-restore behavior so notification taps always open a fresh Brenn URL
//! rather than the browser's last-active tab.
//!
//! The route is registered behind `require_auth` (see `main.rs`). An
//! unauthenticated hit gets 303→`/auth/login` from the middleware before
//! this handler is reached; the deep link is lost (same behavior as any
//! protected `/app/` route today; no regression).

use std::net::IpAddr;

use axum::Extension;
use axum::extract::{Path, RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::Response;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};

use crate::client_ip::ClientIp;
use crate::path_validate::validate_app_path;
use crate::state::AppState;

/// `GET /r/{nonce}?to={path}` — validate and 303-redirect to the target `/app/` path.
///
/// Uses `RawQuery` instead of `Query<T>` so that malformed/unknown query
/// parameters reach the handler body and trigger a `SchemaViolation` security
/// event (fail2ban-relevant). With `Query<T>` + `deny_unknown_fields`, axum's
/// extractor would 400 before the handler runs, silently swallowing the event.
pub async fn redirect(
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
    Path(nonce): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Result<Response, (StatusCode, &'static str)> {
    // Parse and validate `to=` from the raw query string. Any deviation from
    // exactly one `to` parameter with a valid `/app/` value is a security event.
    let to_param = parse_and_validate_query(ip, &state, raw_query.as_deref())?;

    let canonical_to = match validate_nonce_and_to(ip, &state, &nonce, &to_param) {
        Ok(canonical) => canonical,
        Err(msg) => return Err((StatusCode::BAD_REQUEST, msg)),
    };

    // 303 See Other: browser follows the redirect transparently, no inline
    // script, no CSP interaction. `no-store` prevents caching of the
    // per-click-unique URL; `same-origin` retains referrer for same-origin hops.
    Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(header::LOCATION, canonical_to)
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::REFERRER_POLICY, "same-origin")
        .body(axum::body::Body::empty())
        .map_err(|e| {
            // `validate_app_path` guarantees `canonical_to` is ASCII
            // (url::Url-canonical path/query/fragment). If this fires,
            // that invariant was violated — a bug, not a user error.
            tracing::error!(
                error = %e,
                "BUG: redirector response builder failed; \
                 canonical_to violated HeaderValue ASCII contract"
            );
            (StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        })
}

/// Parse the raw query string and extract the `to=` parameter.
///
/// Fires a `SchemaViolation` security event and returns 400 on any deviation:
/// missing `to`, unknown extra parameters, or duplicate `to` keys.
fn parse_and_validate_query(
    ip: IpAddr,
    state: &AppState,
    raw_query: Option<&str>,
) -> Result<String, (StatusCode, &'static str)> {
    let qs = raw_query.unwrap_or("");
    let mut to_value: Option<String> = None;
    for (key, value) in url::form_urlencoded::parse(qs.as_bytes()) {
        if key == "to" {
            if to_value.is_some() {
                // Duplicate `to` key.
                log_and_alert_security_event(
                    &state.alert_dispatcher,
                    SecurityEventType::SchemaViolation,
                    ip,
                    &format!("redirector: duplicate `to` param in query `{qs}`"),
                );
                return Err((StatusCode::BAD_REQUEST, "invalid query"));
            }
            to_value = Some(value.into_owned());
        } else {
            // Unknown query parameter — fail2ban-relevant.
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                &format!("redirector: unknown query param `{key}` in query `{qs}`"),
            );
            return Err((StatusCode::BAD_REQUEST, "invalid query"));
        }
    }
    match to_value {
        Some(v) => Ok(v),
        None => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                &format!("redirector: missing `to` param in query `{qs}`"),
            );
            Err((StatusCode::BAD_REQUEST, "missing to"))
        }
    }
}

/// Validate the nonce character class and the `to` path.
///
/// Returns the canonicalized `to` path on success, or logs a security event
/// and returns an error message on any violation.
fn validate_nonce_and_to<'a>(
    ip: IpAddr,
    state: &AppState,
    nonce: &str,
    to: &str,
) -> Result<String, &'a str> {
    // Nonce must be 8–64 hex-with-hyphen characters.
    if !is_valid_nonce(nonce) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::SchemaViolation,
            ip,
            &format!("redirector: invalid nonce `{nonce}`"),
        );
        return Err("invalid nonce");
    }

    // `to` must be a same-origin `/app/` path. Return the canonical form so
    // the caller does not need to re-validate.
    validate_app_path(to).map_err(|reason| {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::SchemaViolation,
            ip,
            &format!("redirector: invalid `to` param `{to}`: {reason}"),
        );
        "invalid to"
    })
}

/// Returns true iff `nonce` matches `[0-9a-f-]{8,64}`.
fn is_valid_nonce(nonce: &str) -> bool {
    let len = nonce.len();
    if !(8..=64).contains(&len) {
        return false;
    }
    // Single pass: exact character class [0-9a-f-] (lowercase hex + hyphen only).
    nonce
        .bytes()
        .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'-'))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::Request;
    use axum::routing::get as route_get;
    use axum::{Router, middleware as axum_mw};
    use brenn_lib::db;
    use brenn_lib::obs::alerting::make_capturing_alerter;
    use http_body_util::BodyExt;
    use indexmap::IndexMap;
    use tower::ServiceExt;

    use super::*;
    use crate::active_bridge::ActiveBridges;
    use crate::client_ip::{TrustedProxyHops, resolve_client_ip};
    use crate::state::{PendingUploads, WakeLocks};

    /// Returns `(state, captured_alerts, drainer_handle)`.
    ///
    /// Tests that assert on alerts must call `handle.await.unwrap()` after
    /// `state` has been consumed — `state` is moved into `test_router(state)`,
    /// which is consumed by `http_get` via `oneshot`, so no explicit
    /// `drop(state)` is needed before `handle.await`. Tests that hold `state`
    /// directly outside a consumed router must call `drop(state)` explicitly
    /// before `handle.await` to close the alert mpsc channel.
    ///
    /// Tests that make no alert assertions may bind the handle as `_handle`:
    /// dropping a `JoinHandle` does not abort the task (tokio semantics), so
    /// the drainer runs to completion in the background without blocking the
    /// test.
    #[allow(clippy::type_complexity)]
    fn test_state_with_alerter() -> (
        AppState,
        Arc<std::sync::Mutex<Vec<(String, String)>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let (alert_dispatcher, buf, handle) = make_capturing_alerter();
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = db::init_db(&tmp.path().join("brenn.sqlite"));
        // `db` holds a live SQLite connection to `tmp`'s path; forget prevents
        // Drop from deleting the directory while the connection is open.
        std::mem::forget(tmp);
        let state = AppState {
            build_id: crate::test_support::TEST_BUILD_ID,
            db,
            alert_dispatcher,
            active_bridges: ActiveBridges::new(),
            secure_cookies: false,
            log_dir: std::path::PathBuf::from("/tmp"),
            mcp_script_path: std::path::PathBuf::from("/tmp/mcp.py"),
            apps: Arc::new(IndexMap::new()),
            bridge_notify_tx: tokio::sync::broadcast::channel(1).0,
            pending_uploads: PendingUploads::default(),
            static_dir: std::path::PathBuf::from("/tmp"),
            surface_dist_dir: std::path::PathBuf::from("/tmp"),
            cached_models: Default::default(),
            tool_registry: Default::default(),
            tools: std::sync::Arc::new(crate::tool_registry::ToolRegistry::new(vec![])),
            tool_server_origin: std::sync::Arc::from("test-origin"),
            wake_locks: WakeLocks::default(),
            server_shutting_down: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            repo_sync_sender: None,
            messenger: None,
            pwa_push: None,
            mqtt: None,
            mqtt_event_router: None,
            webhook: None,
            automation_engine: None,
            usage_session_gap_secs: 1800,
            surfaces: Arc::new(std::collections::HashMap::new()),
            surface_registry: Default::default(),
            surface_heartbeat_secs: 1,
            replay_components: Arc::new(std::collections::HashMap::new()),
            replay_locks: Arc::new(std::collections::HashMap::new()),
            test_wake_bridge: Default::default(),
        };
        (state, buf, handle)
    }

    /// Build a minimal test router for the redirector (no auth middleware —
    /// handler unit tests call the handler directly through a single-route
    /// router to avoid needing a real session).
    fn test_router(state: AppState) -> Router {
        Router::new()
            .route("/r/{nonce}", route_get(redirect))
            .with_state(state)
            .layer(axum_mw::from_fn(resolve_client_ip))
            .layer(axum::Extension(TrustedProxyHops(0)))
            .layer(MockConnectInfo(std::net::SocketAddr::from((
                [127, 0, 0, 1],
                9999,
            ))))
    }

    async fn http_get(router: Router, uri: &str) -> axum::http::Response<Body> {
        router
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    // --- Test 1: valid nonce + valid to → 303 to target ---

    #[tokio::test]
    async fn redirector_valid_request_returns_303_to_target() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/app/graf/c/42",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        let location = resp
            .headers()
            .get("location")
            .expect("303 must carry Location header")
            .to_str()
            .unwrap();
        assert_eq!(location, "/app/graf/c/42");
        // Empty body — no inline script, no HTML.
        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(body_bytes.is_empty(), "303 body must be empty");
    }

    // --- Test 2: missing `to` → 400 + security event ---

    #[tokio::test]
    async fn redirector_missing_to_returns_400_and_security_event() {
        let (state, alerts, handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(router, "/r/550e8400-e29b-41d4-a716-446655440000").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // Handler now manually parses the query string, so missing `to` fires a
        // security event (fail2ban-relevant) rather than being silently rejected
        // by the extractor before the handler runs.
        // `http_get` consumes router (and thus state+dispatcher) via `oneshot`;
        // `handle.await` drains queued alerts before we lock.
        handle.await.unwrap();
        let captured = alerts.lock().unwrap().clone();
        assert!(
            !captured.is_empty(),
            "missing `to` should trigger security event"
        );
    }

    // --- Test 3: `to` containing `..` → 400 + security event ---

    #[tokio::test]
    async fn redirector_to_with_dotdot_returns_400_and_security_event() {
        let (state, alerts, handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/app/../etc",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // `http_get` consumes router/state/dispatcher via `oneshot`;
        // `handle.await` drains queued alerts before we lock.
        handle.await.unwrap();
        let captured = alerts.lock().unwrap().clone();
        assert!(
            !captured.is_empty(),
            "security event should have been captured"
        );
    }

    // --- Test 4: `to` containing absolute URL → 400 ---

    #[tokio::test]
    async fn redirector_to_absolute_url_returns_400() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=https://evil/",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // --- Test 5: `to` with non-`/app/` prefix → 400 ---

    #[tokio::test]
    async fn redirector_to_non_app_prefix_returns_400() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(router, "/r/550e8400-e29b-41d4-a716-446655440000?to=/health").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // --- Test 6: unknown extra query param → 400 + security event ---

    #[tokio::test]
    async fn redirector_unknown_query_param_returns_400_and_security_event() {
        let (state, alerts, handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/app/x/c/9&x=y",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // `http_get` consumes router/state/dispatcher via `oneshot`;
        // `handle.await` drains queued alerts before we lock.
        handle.await.unwrap();
        let captured = alerts.lock().unwrap().clone();
        assert!(
            !captured.is_empty(),
            "unknown query param should trigger security event"
        );
    }

    // --- Test 7: malformed nonce (outside character class) → 400 ---

    #[tokio::test]
    async fn redirector_invalid_nonce_returns_400() {
        let (state, alerts, handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(router, "/r/zzz?to=/app/x/c/9").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // `http_get` consumes router/state/dispatcher via `oneshot`;
        // `handle.await` drains queued alerts before we lock.
        handle.await.unwrap();
        let captured = alerts.lock().unwrap().clone();
        assert!(
            !captured.is_empty(),
            "nonce violation should trigger security event"
        );
    }

    // --- Test 8: canonical query + fragment preserved verbatim in Location header ---

    #[tokio::test]
    async fn redirector_canonical_query_and_fragment_in_location() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        // `to` value must be percent-encoded when embedded in the outer query string.
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/app/x/c/9%3Ffoo%3Dbar%23z",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        let location = resp
            .headers()
            .get("location")
            .expect("303 must carry Location header")
            .to_str()
            .unwrap();
        // The already-canonical input (path/query/fragment) round-trips verbatim
        // through `validate_app_path` → Location header. `axum`'s HeaderValue
        // accepts `?`, `#`, `=` as ASCII. Note: non-canonical inputs (e.g. empty
        // fragments, redundant `&&`) would be re-encoded rather than preserved
        // byte-for-byte; this test only exercises the canonical-input path.
        assert_eq!(location, "/app/x/c/9?foo=bar#z");
    }

    // --- Test 9: duplicate `to` keys → 400 + security event ---

    #[tokio::test]
    async fn redirector_duplicate_to_returns_400_and_security_event() {
        let (state, alerts, handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/app/x/c/1&to=/app/x/c/2",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        // `http_get` consumes router/state/dispatcher via `oneshot`;
        // `handle.await` drains queued alerts before we lock.
        handle.await.unwrap();
        let captured = alerts.lock().unwrap().clone();
        assert!(
            !captured.is_empty(),
            "duplicate `to` keys should trigger a SchemaViolation security event"
        );
    }

    // --- Test 11: nested redirector `to=/r/abc?to=/app/x/c/9` → 400 ---

    #[tokio::test]
    async fn redirector_nested_redirector_in_to_returns_400() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        // Percent-encode the `to` value so it's a valid query parameter.
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/r/abc%3Fto%3D/app/x/c/9",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // --- Test 12: validate_app_path precision edge cases ---

    #[tokio::test]
    async fn redirector_app_no_trailing_slash_returns_400() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(router, "/r/550e8400-e29b-41d4-a716-446655440000?to=/app").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn redirector_app_with_query_no_trailing_slash_returns_400() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/app%3Fx%3Dy",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn redirector_application_prefix_returns_400() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/application/foo",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // --- Response headers ---

    #[tokio::test]
    async fn redirector_sets_no_store_cache_control() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/app/graf/c/42",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        let cc = resp
            .headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(cc, "no-store");
    }

    #[tokio::test]
    async fn redirector_sets_referrer_policy() {
        let (state, _alerts, _handle) = test_state_with_alerter();
        let router = test_router(state);
        let resp = http_get(
            router,
            "/r/550e8400-e29b-41d4-a716-446655440000?to=/app/graf/c/42",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SEE_OTHER);
        let rp = resp
            .headers()
            .get("referrer-policy")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(rp, "same-origin");
    }

    // --- is_valid_nonce unit tests ---

    #[test]
    fn nonce_valid_uuid_with_hyphens() {
        assert!(is_valid_nonce("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn nonce_valid_compact_hex() {
        assert!(is_valid_nonce("550e8400e29b41d4a716446655440000"));
    }

    #[test]
    fn nonce_too_short() {
        assert!(!is_valid_nonce("abc1234")); // 7 chars
    }

    #[test]
    fn nonce_too_long() {
        assert!(!is_valid_nonce(&"a".repeat(65)));
    }

    #[test]
    fn nonce_uppercase_hex_rejected() {
        // UUID v4 with uppercase hex — must be rejected per character class [0-9a-f-].
        assert!(!is_valid_nonce("550E8400-E29B-41D4-A716-446655440000"));
    }

    #[test]
    fn nonce_non_hex_char_rejected() {
        assert!(!is_valid_nonce("zzz-zzz-zzz-zzz-zz"));
    }
}

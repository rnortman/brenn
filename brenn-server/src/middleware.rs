use axum::body::Body;
use axum::extract::State;
use axum::http::header::COOKIE;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use brenn_lib::auth::device::{Device, resolve_or_create_device};
use brenn_lib::auth::session::{Session, validate_session};
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use tracing::info;

use crate::client_ip::ClientIp;
use crate::state::AppState;

/// Auth middleware. Validates the session cookie and injects the Session and
/// Device into request extensions. Returns 302 → /login if not authenticated.
///
/// Device resolution runs only after authentication succeeds. Failed-auth
/// paths short-circuit before any device DB access.
pub async fn require_auth(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let client_ip = request
        .extensions()
        .get::<ClientIp>()
        .expect("ClientIp extension missing — resolve_client_ip middleware not applied")
        .0;

    let token = extract_session_cookie(&request);

    let has_token = token.is_some();
    if let Some(token) = token {
        let conn = state.db.lock().await;
        if let Some(session) = validate_session(&conn, &token) {
            // Resolve or create a device for this authenticated user.
            let device_token = extract_device_cookie(&request);
            let ua = request
                .headers()
                .get(axum::http::header::USER_AGENT)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let resolved =
                resolve_or_create_device(&conn, device_token.as_deref(), session.user.id, ua);
            let device: Device = brenn_lib::auth::device::load_device(&conn, resolved.id);
            drop(conn);

            // If a new device was created, set the cookie on the response.
            let new_cookie = resolved
                .new_token
                .map(|t| device_cookie(&t, state.secure_cookies));

            request.extensions_mut().insert(session);
            request.extensions_mut().insert(device);

            let mut response = next.run(request).await;
            if let Some(cookie_str) = new_cookie
                && let Ok(val) = axum::http::HeaderValue::from_str(&cookie_str)
            {
                response
                    .headers_mut()
                    .append(axum::http::header::SET_COOKIE, val);
            }
            return response;
        }
    }

    // Not authenticated. Log only if a token was provided (invalid/expired).
    // Missing cookie is normal (first visit) — not a security event.
    if has_token {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::AuthFailure,
            client_ip,
            "invalid or expired session cookie",
        );
    }

    Redirect::to("/auth/login").into_response()
}

// CSRF validation is done per-handler for form-based endpoints (the handler has
// access to the parsed form body). For future API endpoints using JSON bodies,
// an X-CSRF-Token header check can be added as middleware here.

/// Extract a named cookie value from the request.
///
/// Returns `None` if the `Cookie` header is absent, not valid UTF-8, or
/// does not contain the named cookie.
fn extract_named_cookie(request: &Request<Body>, name: &str) -> Option<String> {
    let cookie_header = request.headers().get(COOKIE)?;
    let cookie_str = cookie_header.to_str().ok()?;
    let prefix = format!("{name}=");
    for cookie in cookie_str.split(';') {
        let cookie = cookie.trim();
        if let Some(value) = cookie.strip_prefix(prefix.as_str())
            && !value.is_empty()
        {
            return Some(value.to_string());
        }
    }
    None
}

/// Extract the brenn_session cookie value from the request.
fn extract_session_cookie(request: &Request<Body>) -> Option<String> {
    extract_named_cookie(request, "brenn_session")
}

/// Extract the brenn_device cookie value from the request.
fn extract_device_cookie(request: &Request<Body>) -> Option<String> {
    extract_named_cookie(request, "brenn_device")
}

/// Build the Set-Cookie header for a session token.
///
/// When `secure` is true, the `Secure` flag is set (cookie only sent over HTTPS).
/// Production deployments behind TLS should set this to true.
pub fn session_cookie(token: &str, secure: bool) -> String {
    let secure_flag = if secure { "; Secure" } else { "" };
    format!("brenn_session={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=7776000{secure_flag}")
}

/// Build the Set-Cookie header that clears the session cookie.
pub fn clear_session_cookie() -> String {
    "brenn_session=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0".to_string()
}

/// Build the Set-Cookie header for a device token.
///
/// Max-Age is 10 years — device cookies outlive session cookies substantially.
/// Same HttpOnly/SameSite posture as the session cookie.
pub fn device_cookie(token: &str, secure: bool) -> String {
    let secure_flag = if secure { "; Secure" } else { "" };
    format!("brenn_device={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=315360000{secure_flag}")
}

/// Middleware that logs 413 Payload Too Large responses at `info` level with
/// session user id, request path, `Content-Length`, and client IP.
///
/// Must run inside `require_auth` so `Session` and `ClientIp` extensions are
/// already populated. Intended for use on the upload route via `.layer()`.
/// Not a fail2ban signal — oversized upload is a legitimate user error.
pub async fn log_upload_413(request: Request<Body>, next: Next) -> Response {
    let session =
        request.extensions().get::<Session>().cloned().expect(
            "log_upload_413: Session extension missing — must be applied inside require_auth",
        );
    let client_ip = request.extensions().get::<ClientIp>().copied().expect(
        "log_upload_413: ClientIp extension missing — resolve_client_ip middleware not applied",
    );
    let path = request.uri().path().to_owned();
    let content_length = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    let response = next.run(request).await;

    if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
        info!(
            user_id = session.user.id,
            path = %path,
            content_length = content_length,
            client_ip = %client_ip.0,
            "upload 413: payload too large"
        );
    }

    response
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use tower::Layer as _;
    use tower::ServiceExt as _;

    use super::*;

    use crate::test_support::http::inject_extensions;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn log_upload_413_passes_through_non_413() {
        // A 200 response must pass through unchanged and must NOT emit a log.
        let svc = axum::middleware::from_fn(log_upload_413).layer(tower::service_fn(
            |_req: Request<Body>| {
                let resp = StatusCode::OK.into_response();
                std::future::ready(Ok::<_, std::convert::Infallible>(resp))
            },
        ));

        let req = inject_extensions(
            Request::builder().uri("/test").body(Body::empty()).unwrap(),
            42,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        );

        let resp = svc.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            !logs_contain("upload 413"),
            "non-413 response must not emit a log"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn log_upload_413_logs_on_413_response() {
        // A 413 response must trigger the log line with user_id, path, client_ip.
        let svc = axum::middleware::from_fn(log_upload_413).layer(tower::service_fn(
            |_req: Request<Body>| {
                let resp = StatusCode::PAYLOAD_TOO_LARGE.into_response();
                std::future::ready(Ok::<_, std::convert::Infallible>(resp))
            },
        ));

        let req = inject_extensions(
            Request::builder()
                .uri("/app/test/upload")
                .header(axum::http::header::CONTENT_LENGTH, "9999999")
                .body(Body::empty())
                .unwrap(),
            99,
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
        );

        let resp = svc.oneshot(req).await.unwrap();
        // Response must still be 413 — middleware must not alter the status.
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            logs_contain("upload 413"),
            "413 response must emit the log line"
        );
        assert!(
            logs_contain("9999999"),
            "log must include the Content-Length value"
        );
        assert!(logs_contain("99"), "log must include user_id");
        assert!(
            logs_contain("/app/test/upload"),
            "log must include request path"
        );
        assert!(logs_contain("203.0.113.1"), "log must include client_ip");
    }

    #[test]
    fn session_cookie_without_secure_flag() {
        let cookie = session_cookie("abc123", false);
        assert!(cookie.starts_with("brenn_session=abc123;"));
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Lax"));
        assert!(cookie.contains("Max-Age=7776000"));
        assert!(!cookie.contains("Secure"));
    }

    #[test]
    fn session_cookie_with_secure_flag() {
        let cookie = session_cookie("abc123", true);
        assert!(cookie.starts_with("brenn_session=abc123;"));
        assert!(cookie.contains("; Secure"));
    }

    #[test]
    fn device_cookie_format() {
        let token = "a".repeat(64);
        let cookie = device_cookie(&token, false);
        assert!(cookie.starts_with(&format!("brenn_device={token};")));
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Lax"));
        assert!(cookie.contains("Max-Age=315360000"));
        assert!(!cookie.contains("Secure"));
    }

    #[test]
    fn device_cookie_with_secure_flag() {
        let token = "b".repeat(64);
        let cookie = device_cookie(&token, true);
        assert!(cookie.contains("; Secure"));
    }
}

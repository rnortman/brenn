use axum::Extension;
use axum::Form;
use axum::extract::{Query, State};
use axum::http::header::SET_COOKIE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use brenn_db::auth::password::{verify_password, verify_password_dummy};
use brenn_db::auth::session::create_session;
use brenn_db::auth::user::get_user_credentials;
use brenn_obs::alerting::AlertSeverity;
use brenn_obs::security::{SecurityEventType, log_and_alert_security_event};
use serde::Deserialize;
use std::net::IpAddr;
use tracing::info;

use crate::client_ip::ClientIp;
use crate::middleware::{cross_site_form_post, login_next_query, session_cookie};
use crate::path_validate::validate_login_next;
use crate::routes::html_escape;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct LoginQuery {
    pub error: Option<String>,
    /// Where to return after login, as `require_auth` minted it.
    pub next: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginForm {
    pub username: String,
    pub password: String,
    /// The login page's hidden `next`, echoed back.
    pub next: Option<String>,
}

/// The validated `next`, if one was sent. An invalid one is a schema
/// violation: the browser only ever sends back what the middleware minted.
fn validated_next(state: &AppState, ip: IpAddr, next: Option<&str>) -> Option<String> {
    let raw = next?;
    match validate_login_next(raw) {
        Ok(next) => Some(next),
        Err(reason) => {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::SchemaViolation,
                ip,
                &format!("login: invalid `next` {raw:?}: {reason}"),
            );
            None
        }
    }
}

/// Where a failed login sends the browser: the login page again, carrying
/// `next` when it validated.
fn failure_location(next: Option<&str>) -> String {
    match next {
        Some(next) => format!("/auth/login?error=1&{}", login_next_query(next)),
        None => "/auth/login?error=1".to_string(),
    }
}

/// GET /auth/login — render the login page.
pub async fn login_page(
    State(state): State<AppState>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    Query(query): Query<LoginQuery>,
) -> Html<String> {
    let error_msg = if query.error.is_some() {
        r#"<p class="error">Invalid username or password.</p>"#
    } else {
        ""
    };
    let next_input = validated_next(&state, ip, query.next.as_deref())
        .map(|next| {
            format!(
                r#"
            <input type="hidden" name="next" value="{}">"#,
                html_escape(&next)
            )
        })
        .unwrap_or_default();
    let build_id = state.build_id;

    Html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Brenn — Login</title>
    <link rel="stylesheet" href="/auth/static/auth.css">
</head>
<body>
    <main>
        <h1>Brenn</h1>
        {error_msg}
        <form method="post" action="/auth/login">
            <label for="username">Username</label>
            <input type="text" id="username" name="username" autocomplete="username" required autofocus>
            <label for="password">Password</label>
            <input type="password" id="password" name="password" autocomplete="current-password" required>{next_input}
            <button type="submit">Log in</button>
        </form>
        <!-- TODO(register-page-invite-oracle): /auth/register 404s whenever no unused
             invite code exists, so this link is a dead end in the steady state. -->
        <p class="link"><a href="/auth/register">Register</a></p>
    </main>
    <script type="module" src="/static/nav-on-message.js?v={build_id}"></script>
</body>
</html>"#
    ))
}

/// POST /auth/login — authenticate and create a session.
pub async fn login_submit(
    State(state): State<AppState>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> Response {
    if let Some(reason) = cross_site_form_post(&headers) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::SchemaViolation,
            ip,
            &format!("login: cross-site form post refused ({reason})"),
        );
        return StatusCode::FORBIDDEN.into_response();
    }
    let next = validated_next(&state, ip, form.next.as_deref());
    // Look up user, then RELEASE the lock before argon2 (which takes hundreds of ms).
    let user_info = {
        let conn = state.db.lock().await;
        get_user_credentials(&conn, &form.username)
    };

    match user_info {
        Some((user_id, password_hash)) => {
            // Argon2 verify — expensive, DB lock is NOT held.
            if verify_password(form.password.as_bytes(), &password_hash) {
                // Re-acquire lock for session creation.
                let conn = state.db.lock().await;
                let (token, _csrf) = create_session(&conn, user_id);
                let cookie = session_cookie(&token, state.secure_cookies);
                info!(username = %form.username, user_id = user_id, %ip, "login successful");
                state.alert_dispatcher.alert(
                    AlertSeverity::Info,
                    format!("Login: {}", form.username),
                    format!("IP: {ip}"),
                );
                (
                    StatusCode::SEE_OTHER,
                    [
                        (SET_COOKIE, cookie),
                        (
                            axum::http::header::LOCATION,
                            next.unwrap_or_else(|| "/".to_string()),
                        ),
                    ],
                )
                    .into_response()
            } else {
                log_and_alert_security_event(
                    &state.alert_dispatcher,
                    SecurityEventType::AuthFailure,
                    ip,
                    &format!("failed login for user: {}", form.username),
                );
                Redirect::to(&failure_location(next.as_deref())).into_response()
            }
        }
        None => {
            // User doesn't exist — run dummy verify for constant-time response.
            // DB lock is NOT held during this expensive operation.
            verify_password_dummy(form.password.as_bytes());
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::AuthFailure,
                ip,
                &format!("failed login for unknown user: {}", form.username),
            );
            Redirect::to(&failure_location(next.as_deref())).into_response()
        }
    }
}

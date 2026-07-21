use axum::Extension;
use axum::Form;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::http::header::SET_COOKIE;
use axum::response::{Html, IntoResponse, Redirect, Response};
use brenn_lib::auth::invite::{has_unused_invite_codes, use_invite_code, validate_invite_code};
use brenn_lib::auth::password::hash_password;
use brenn_lib::auth::session::create_session;
use brenn_lib::obs::alerting::AlertSeverity;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use serde::Deserialize;
use tracing::{info, warn};

use crate::client_ip::ClientIp;
use crate::middleware::session_cookie;
use crate::state::AppState;

/// Minimum password length. 12 characters — not a consumer SaaS with password fatigue.
const MIN_PASSWORD_LEN: usize = 12;
/// Maximum username length in characters.
const MAX_USERNAME_LEN: usize = 64;

#[derive(Deserialize)]
pub struct RegisterQuery {
    /// Error codes: "invite" (bad code), "username" (taken), "username_invalid" (bad format), "password" (too short)
    pub error: Option<String>,
}

#[derive(Deserialize)]
pub struct RegisterForm {
    pub username: String,
    pub password: String,
    pub invite_code: String,
}

/// GET /register — render the registration page.
/// Returns 404 if no unused invite codes exist.
pub async fn register_page(
    State(state): State<AppState>,
    Query(query): Query<RegisterQuery>,
) -> Response {
    let conn = state.db.lock().await;
    if !has_unused_invite_codes(&conn) {
        // No invite codes available — this is NOT a security event,
        // just a "nothing to do here" response.
        return StatusCode::NOT_FOUND.into_response();
    }
    drop(conn);

    let error_msg = match query.error.as_deref() {
        Some("invite") => r#"<p class="error">Invalid invite code.</p>"#,
        Some("username") => r#"<p class="error">Username already taken.</p>"#,
        Some("username_invalid") => {
            r#"<p class="error">Username must be 1-64 characters, using only letters, numbers, hyphens, and underscores.</p>"#
        }
        Some("password") => r#"<p class="error">Password must be at least 12 characters.</p>"#,
        _ => "",
    };
    let build_id = state.build_id;

    Html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Brenn — Register</title>
    <link rel="stylesheet" href="/auth/static/auth.css">
</head>
<body>
    <main>
        <h1>Brenn</h1>
        <h2>Register</h2>
        {error_msg}
        <form method="post" action="/auth/register">
            <label for="invite_code">Invite Code</label>
            <input type="text" id="invite_code" name="invite_code" required autofocus>
            <label for="username">Username</label>
            <input type="text" id="username" name="username" autocomplete="username" required>
            <label for="password">Password</label>
            <input type="password" id="password" name="password" autocomplete="new-password" required
                   minlength="{MIN_PASSWORD_LEN}">
            <button type="submit">Register</button>
        </form>
    </main>
    <script type="module" src="/static/nav-on-message.js?v={build_id}"></script>
</body>
</html>"#
    ))
    .into_response()
}

/// POST /register — create a user with a valid invite code.
pub async fn register_submit(
    State(state): State<AppState>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    Form(form): Form<RegisterForm>,
) -> Response {
    info!(username = %form.username, %ip, "registration attempt");
    state.alert_dispatcher.alert(
        AlertSeverity::Info,
        format!("Registration attempt: {}", form.username),
        format!("IP: {ip}"),
    );

    // Phase 1: Validate inputs while holding the lock.
    {
        let conn = state.db.lock().await;

        // Validate invite code.
        if !validate_invite_code(&conn, &form.invite_code) {
            log_and_alert_security_event(
                &state.alert_dispatcher,
                SecurityEventType::AuthFailure,
                ip,
                "invalid invite code on registration",
            );
            warn!(username = %form.username, ip = %ip, invite_code = %form.invite_code, "registration rejected: invalid invite code");
            return Redirect::to("/auth/register?error=invite").into_response();
        }

        // Validate username: non-empty, within length limit, alphanumeric + hyphens/underscores.
        let username_len = form.username.chars().count();
        if username_len == 0
            || username_len > MAX_USERNAME_LEN
            || !form
                .username
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            warn!(username = %form.username, ip = %ip, "registration rejected: invalid username format");
            state.alert_dispatcher.alert(
                AlertSeverity::Info,
                format!("Registration rejected: bad username '{}'", form.username),
                format!("IP: {}", ip),
            );
            return Redirect::to("/auth/register?error=username_invalid").into_response();
        }

        // Validate password length (character count, not byte length).
        if form.password.chars().count() < MIN_PASSWORD_LEN {
            warn!(username = %form.username, ip = %ip, "registration rejected: password too short");
            state.alert_dispatcher.alert(
                AlertSeverity::Info,
                format!(
                    "Registration rejected: short password for '{}'",
                    form.username
                ),
                format!("IP: {}", ip),
            );
            return Redirect::to("/auth/register?error=password").into_response();
        }

        // Check username availability — case-insensitively to prevent case-variant
        // duplicates (e.g., both "alice" and "Alice") which would cause nondeterministic
        // address resolution in pwa_push canonicalization and auth divergence.
        if brenn_lib::auth::user::get_user_by_username_nocase(&conn, &form.username).is_some() {
            warn!(username = %form.username, ip = %ip, "registration rejected: username taken");
            state.alert_dispatcher.alert(
                AlertSeverity::Info,
                format!("Registration rejected: username '{}' taken", form.username),
                format!("IP: {}", ip),
            );
            return Redirect::to("/auth/register?error=username").into_response();
        }
        // Lock released here.
    }

    // Phase 2: Hash password — expensive (Argon2id, hundreds of ms). DB lock NOT held.
    let password_hash = hash_password(form.password.as_bytes());

    // Phase 3: Re-acquire lock for writes. Handle race condition on username.
    let conn = state.db.lock().await;

    // Re-validate invite code (could have been used between phases).
    if !validate_invite_code(&conn, &form.invite_code) {
        warn!(username = %form.username, ip = %ip, "registration rejected: invite code used during password hash");
        return Redirect::to("/auth/register?error=invite").into_response();
    }

    let user_id = match brenn_lib::auth::user::try_create_user(
        &conn,
        &form.username,
        &password_hash,
    ) {
        Ok(id) => id,
        Err(_) => {
            warn!(username = %form.username, ip = %ip, "registration rejected: username race condition");
            return Redirect::to("/auth/register?error=username").into_response();
        }
    };

    // Mark invite code as used.
    use_invite_code(&conn, &form.invite_code, user_id);

    // Auto-login: create session.
    let (token, _csrf) = create_session(&conn, user_id);
    let cookie = session_cookie(&token, state.secure_cookies);

    info!(username = %form.username, user_id = user_id, ip = %ip, "user registered successfully");
    state.alert_dispatcher.alert(
        AlertSeverity::Info,
        format!("New user registered: {}", form.username),
        format!("IP: {}\nUser ID: {user_id}", ip),
    );

    (
        StatusCode::SEE_OTHER,
        [
            (SET_COOKIE, cookie),
            (axum::http::header::LOCATION, "/".to_string()),
        ],
    )
        .into_response()
}

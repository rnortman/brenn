use axum::Extension;
use axum::Form;
use axum::extract::{Query, State};
use axum::http::header::SET_COOKIE;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use brenn_db::auth::invite::{has_unused_invite_codes, use_invite_code, validate_invite_code};
use brenn_db::auth::password::hash_password;
use brenn_db::auth::session::create_session;
use brenn_obs::alerting::AlertSeverity;
use brenn_obs::security::{SecurityEventType, log_and_alert_security_event};
use serde::Deserialize;
use tracing::{info, warn};

use crate::client_ip::ClientIp;
use crate::middleware::{cross_site_form_post, session_cookie};
use crate::state::AppState;

/// Minimum password length. 12 characters — not a consumer SaaS with password fatigue.
const MIN_PASSWORD_LEN: usize = 12;

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
    // TODO(register-page-invite-oracle): this 404 varies with invite state, which
    // leaks whether an onboarding window is open and dead-ends the login page's
    // Register link. Serve the same status either way.
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
    headers: HeaderMap,
    Form(form): Form<RegisterForm>,
) -> Response {
    if let Some(reason) = cross_site_form_post(&headers) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::SchemaViolation,
            ip,
            &format!("register: cross-site form post refused ({reason})"),
        );
        return StatusCode::FORBIDDEN.into_response();
    }
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

        if !brenn_envelope::addressing::is_username(&form.username) {
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
        if brenn_db::auth::user::get_user_by_username_nocase(&conn, &form.username).is_some() {
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

    let user_id = match brenn_db::auth::user::try_create_user(&conn, &form.username, &password_hash)
    {
        Ok(id) => id,
        Err(_) => {
            warn!(username = %form.username, ip = %ip, "registration rejected: username race condition");
            return Redirect::to("/auth/register?error=username").into_response();
        }
    };

    // The new user's location channel, minted under the same lock so no
    // connection of theirs can report a fix before it exists. With no
    // messaging there is no bus to mint on; the next boot that has one mints
    // it from the users table.
    if let Some(messenger) = &state.messenger {
        messenger.provision_device_location_channel(&conn, &form.username);
    }

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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use axum::extract::{Form, State};
    use axum::http::HeaderMap;
    use axum::{Extension, http::StatusCode};

    use super::{RegisterForm, register_submit};
    use crate::client_ip::ClientIp;
    use crate::state::AppState;

    /// Registration mints the new user's location channel on the running
    /// messenger: a directory entry and a row in the server's database.
    #[tokio::test]
    async fn registration_mints_the_users_location_channel() {
        let db = crate::test_support::init_db_memory();
        let code = brenn_db::auth::invite::create_invite_code(&*db.lock().await);
        let messenger =
            brenn_messaging::testutils::empty_directory_messenger_over(db.clone(), "test-source");
        let mut state = AppState::for_test(db.clone(), None);
        state.messenger = Some(messenger.clone());

        let response = register_submit(
            State(state),
            Extension(ClientIp(IpAddr::V4(Ipv4Addr::LOCALHOST))),
            HeaderMap::new(),
            Form(RegisterForm {
                username: "alice".to_string(),
                password: "correct horse battery staple".to_string(),
                invite_code: code,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers()[axum::http::header::LOCATION],
            "/",
            "registration succeeded"
        );

        let address = brenn_lib::messaging::device_location_address("alice");
        let entry = messenger
            .directory()
            .resolve(&address)
            .expect("registration minted the channel");
        let rows: i64 = db
            .lock()
            .await
            .query_row(
                "SELECT COUNT(*) FROM messaging_channels WHERE uuid = ?1",
                rusqlite::params![entry.uuid.as_bytes().to_vec()],
                |row| row.get(0),
            )
            .expect("count the channel's rows");
        assert_eq!(rows, 1);
    }
}

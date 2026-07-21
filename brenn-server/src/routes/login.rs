use axum::Extension;
use axum::Form;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::http::header::SET_COOKIE;
use axum::response::{Html, IntoResponse, Redirect, Response};
use brenn_lib::auth::password::{verify_password, verify_password_dummy};
use brenn_lib::auth::session::create_session;
use brenn_lib::auth::user::get_user_credentials;
use brenn_lib::obs::alerting::AlertSeverity;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use serde::Deserialize;
use tracing::info;

use crate::client_ip::ClientIp;
use crate::middleware::session_cookie;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct LoginQuery {
    pub error: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginForm {
    pub username: String,
    pub password: String,
}

/// GET /auth/login — render the login page.
pub async fn login_page(
    State(state): State<AppState>,
    Query(query): Query<LoginQuery>,
) -> Html<String> {
    let error_msg = if query.error.is_some() {
        r#"<p class="error">Invalid username or password.</p>"#
    } else {
        ""
    };
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
            <input type="password" id="password" name="password" autocomplete="current-password" required>
            <button type="submit">Log in</button>
        </form>
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
    Form(form): Form<LoginForm>,
) -> Response {
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
                        (axum::http::header::LOCATION, "/".to_string()),
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
                Redirect::to("/auth/login?error=1").into_response()
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
            Redirect::to("/auth/login?error=1").into_response()
        }
    }
}

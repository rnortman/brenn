use axum::Form;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::SET_COOKIE;
use axum::response::{IntoResponse, Response};
use brenn_lib::auth::session::{Session, delete_session};
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use serde::Deserialize;

use crate::client_ip::ClientIp;
use crate::middleware::clear_session_cookie;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct LogoutForm {
    pub csrf_token: String,
}

/// POST /logout — destroy the session and clear the cookie.
/// Requires a valid CSRF token in the form body.
pub async fn logout(
    State(state): State<AppState>,
    axum::Extension(ClientIp(ip)): axum::Extension<ClientIp>,
    axum::Extension(session): axum::Extension<Session>,
    Form(form): Form<LogoutForm>,
) -> Response {
    // Validate CSRF token.
    if form.csrf_token != session.csrf_token {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::SchemaViolation,
            ip,
            "CSRF mismatch on logout",
        );
        return StatusCode::FORBIDDEN.into_response();
    }

    let conn = state.db.lock().await;
    delete_session(&conn, &session.token);

    (
        StatusCode::SEE_OTHER,
        [
            (SET_COOKIE, clear_session_cookie()),
            (axum::http::header::LOCATION, "/auth/login".to_string()),
        ],
    )
        .into_response()
}

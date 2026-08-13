//! The surface route: the door a browser comes through to reach a surface.
//!
//! `surface_ws_handler` fronts `GET /surface/{slug}/ws` — cookie auth, the
//! capacity gate, and the served-asset build check — and then hands the socket
//! to the generic attachment session (`brenn-attach-server`) with the surface's
//! boot-resolved authority (`brenn_surface_server::profile`) as its authority
//! half. `page.rs` serves the document that opens it.
//!
//! Everything a surface *is* — the boot lowering of config into runtimes, the
//! bindings and self-description documents, asset validation, the disconnected
//! stamp — lives in `brenn-surface-server`, a crate below. What is here is the
//! part that needs `AppState`.

pub mod page;

#[cfg(test)]
mod conformance_tests;
#[cfg(any(test, feature = "testutils"))]
pub mod test_fixtures;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use axum::Extension;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use brenn_attach_proto::max_client_frame_bytes;
use brenn_attach_server::profile::AttachProfile;
use brenn_attach_server::registry::{AttachSessionHandle, PUSH_QUEUE_FRAMES, RegisterRejection};
use brenn_attach_server::session::{
    AttachSessionParams, run_attach_session, sanitize_client_detail,
};
use brenn_db::auth::session::Session;
use brenn_obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_surface_server::{SurfaceRuntime, telemetry};
use tracing::warn;
use uuid::Uuid;

use crate::client_ip::ClientIp;
use crate::routes::ws::close_with_stale_client;
use crate::state::AppState;

/// Shared pre-serve authorization for the surface page and WS handlers: resolve
/// the slug and enforce the access check, emitting the same fail2ban security
/// events from both entry points. `is_ws` selects the endpoint-specific detail
/// strings only. Unknown slug → 404 + `UnrecognizedUrl` (probe signal, slug
/// sanitized); denied user → 403 + `AuthFailure`.
pub(crate) fn authorize_surface(
    state: &AppState,
    slug: &str,
    username: &str,
    ip: std::net::IpAddr,
    is_ws: bool,
) -> Result<Arc<SurfaceRuntime>, StatusCode> {
    let Some(runtime) = state.surfaces.get(slug).cloned() else {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::UnrecognizedUrl,
            ip,
            &format!(
                "/surface/{}{}",
                sanitize_client_detail(slug),
                if is_ws { "/ws" } else { "" }
            ),
        );
        return Err(StatusCode::NOT_FOUND);
    };

    if !runtime.resolved.user_has_access(username) {
        log_and_alert_security_event(
            &state.alert_dispatcher,
            SecurityEventType::AuthFailure,
            ip,
            &format!(
                "user {} denied {}access to surface {}",
                username,
                if is_ws { "WS " } else { "" },
                slug
            ),
        );
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(runtime)
}

/// Query parameters for the surface WS endpoint.
///
/// `build` is `Option` for the same handler-controls-classification reason as
/// the legacy `WsQuery`: a missing value is a stale first-party tab (close with
/// the stale code, no security event), not a probe.
#[derive(serde::Deserialize)]
pub struct SurfaceWsQuery {
    build: Option<String>,
}

// TODO(attach-upgrade-preamble): the register-then-upgrade block below is
// duplicated in `routes::remote`, down to the ordering invariant.
/// `GET /surface/{slug}/ws` — upgrade to the surface WebSocket.
///
/// Auth middleware has already validated the session and injected `Session` /
/// `ClientIp`. Pre-upgrade checks run in the order access → capacity → handshake
/// so an unauthorized user sees `403` (and never learns attach counts), and a
/// full surface never consumes an upgraded socket.
pub async fn surface_ws_handler(
    Path(slug): Path<String>,
    Query(query): Query<SurfaceWsQuery>,
    ws: WebSocketUpgrade,
    Extension(session): Extension<Session>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    State(state): State<AppState>,
) -> Result<Response, StatusCode> {
    let runtime = authorize_surface(&state, &slug, &session.user.username, ip, true)?;

    // Register the slot before upgrading so the check has no
    // check-then-register race and a full surface never upgrades the socket.
    let session_id = Uuid::new_v4();
    let (push_tx, push_rx) = tokio::sync::mpsc::channel(PUSH_QUEUE_FRAMES);
    let active_channels = Arc::new(Mutex::new(HashSet::new()));
    let drain_notify = Arc::new(tokio::sync::Notify::new());
    let handle = AttachSessionHandle {
        session_id,
        account: session.user.username.clone(),
        push_tx,
        active_channels: active_channels.clone(),
        drain_notify: drain_notify.clone(),
    };
    let caps = runtime.profile.session_caps();
    let guard = match state.attach_registry.try_register(&slug, handle, caps) {
        Ok(guard) => guard,
        Err(RegisterRejection::AttacherFull { current }) => {
            // Not a security event: a user with many tabs is not fail2ban signal.
            warn!(
                surface = %slug,
                user = %session.user.username,
                ip = %ip,
                count = current,
                "surface session cap reached; rejecting with 503"
            );
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        Err(RegisterRejection::AccountCapExceeded { account_current }) => {
            // Not a security event either: a legitimate user with many devices
            // or tabs can trip this, and banning that IP would lock out an
            // authenticated user. The distinct message + user attribution turns
            // "surface is mysteriously full" into a one-grep answer.
            warn!(
                surface = %slug,
                user = %session.user.username,
                ip = %ip,
                user_count = account_current,
                "per-user surface session cap reached; rejecting with 503"
            );
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Missing or mismatched build is a stale first-party tab, not a probe:
    // accept the upgrade, then close with the stale code. No security event.
    let build_id = state.build_id;
    match query.build.as_deref() {
        Some(v) if v == build_id => {}
        other => {
            let client_build = other.unwrap_or("<missing>").to_string();
            drop(guard);
            return Ok(ws.on_upgrade(move |socket| async move {
                close_with_stale_client(socket, &client_build, build_id).await;
            }));
        }
    }

    let cap = max_client_frame_bytes(runtime.max_body_bytes);
    let account = session.user.username;
    let heartbeat_secs = state.attach_heartbeat_secs;
    let alert_dispatcher = state.alert_dispatcher.clone();
    let registry = state.attach_registry.clone();
    Ok(ws
        .max_message_size(cap)
        .max_frame_size(cap)
        .on_upgrade(move |socket| async move {
            let outcome = run_attach_session(AttachSessionParams {
                profile: runtime.profile.clone(),
                messenger: runtime.messenger().clone(),
                policy: runtime.policy.clone(),
                registry,
                guard,
                session_id,
                account,
                ip,
                max_body_bytes: runtime.max_body_bytes,
                heartbeat_secs,
                store_incarnation: runtime.store_incarnation(),
                ident: build_id.to_string(),
                alert_dispatcher,
                push_rx,
                active_channels,
                drain_notify,
                socket,
            })
            .await;
            if outcome.last_detach {
                telemetry::publish_terminal_disconnected_stamp(&runtime, session_id).await;
            }
        }))
}

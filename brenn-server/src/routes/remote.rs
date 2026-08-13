//! The `remote` route: the upgrade handler for an authenticated native daemon.
//!
//! The half of the remote stack that needs `AppState`; boot-resolved runtimes
//! and credential comparison live in `brenn_remote_server`.

#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod test_fixtures;
#[cfg(test)]
mod ws_tests;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use axum::Extension;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use brenn_attach_proto::max_client_frame_bytes;
use brenn_attach_server::profile::AttachProfile;
use brenn_attach_server::registry::{AttachSessionHandle, PUSH_QUEUE_FRAMES, RegisterRejection};
use brenn_attach_server::session::{AttachSessionParams, run_attach_session};
use brenn_remote_server::authenticate_remote;
use tracing::warn;
use uuid::Uuid;

use crate::client_ip::ClientIp;
use crate::state::AppState;

// TODO(attach-upgrade-preamble): the register-then-upgrade block below is
// duplicated in `routes::surface`, down to the ordering invariant.
/// `GET /remote/{slug}/ws` — upgrade to the remote WebSocket.
///
/// Registered outside the cookie-auth group and inside `resolve_client_ip`: the
/// only injector of the account string is `require_auth`, so this handler
/// supplies its own — `remote:<slug>`, the profile's principal, held for log
/// attribution and as the per-account cap grain (the two grains collapse for a
/// route whose account *is* its attacher).
///
/// The session slot is registered before the upgrade, so the capacity check has
/// no check-then-register race and a full remote never consumes an upgraded
/// socket. There is deliberately **no build-ID handshake**: a daemon deploys on
/// its own schedule; the remote's only skew gate is the protocol's own version
/// negotiation.
pub async fn remote_ws_handler(
    Path(slug): Path<String>,
    ws: WebSocketUpgrade,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Response, StatusCode> {
    let runtime =
        authenticate_remote(&state.remotes, &state.alert_dispatcher, &slug, &headers, ip)?;

    let session_id = Uuid::new_v4();
    let (push_tx, push_rx) = tokio::sync::mpsc::channel(PUSH_QUEUE_FRAMES);
    let active_channels = Arc::new(Mutex::new(HashSet::new()));
    let drain_notify = Arc::new(tokio::sync::Notify::new());
    let account = runtime.profile.attacher().as_str().to_string();
    let handle = AttachSessionHandle {
        session_id,
        account: account.clone(),
        push_tx,
        active_channels: active_channels.clone(),
        drain_notify: drain_notify.clone(),
    };
    let caps = runtime.profile.session_caps();
    let guard = match state
        .attach_registry
        .try_register(&runtime.registry_key, handle, caps)
    {
        Ok(guard) => guard,
        // Neither rejection is a security event. A remote at its cap is either
        // an operator running more consumers than they configured for or a
        // netsplit whose corpse has not yet been reaped by the heartbeat
        // watchdog; banning the pod's IP for that would turn a transient into an
        // outage. The two arms answer identically because the grains collapse —
        // the account behind a remote attachment is the remote — and are kept
        // apart only so the log names which cap the registry tripped.
        Err(RegisterRejection::AttacherFull { current }) => {
            warn!(
                remote = %runtime.slug,
                ip = %ip,
                count = current,
                cap = caps.per_attacher,
                "remote session cap reached; rejecting with 503"
            );
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        Err(RegisterRejection::AccountCapExceeded { account_current }) => {
            warn!(
                remote = %runtime.slug,
                ip = %ip,
                count = account_current,
                cap = caps.per_account,
                "remote per-account session cap reached; rejecting with 503"
            );
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let cap = max_client_frame_bytes(runtime.max_body_bytes);
    let heartbeat_secs = state.attach_heartbeat_secs;
    let ident = state.build_id.to_string();
    let alert_dispatcher = state.alert_dispatcher.clone();
    let registry = state.attach_registry.clone();
    Ok(ws
        .max_message_size(cap)
        .max_frame_size(cap)
        .on_upgrade(move |socket| async move {
            // The outcome is dropped: `last_detach` exists for a route with a
            // terminal document to write, and a remote has none — a daemon's
            // absence is not a fact the bus publishes on its behalf.
            let _outcome = run_attach_session(AttachSessionParams {
                profile: runtime.profile.clone(),
                messenger: runtime.messenger.clone(),
                policy: runtime.policy.clone(),
                registry,
                guard,
                session_id,
                account,
                ip,
                max_body_bytes: runtime.max_body_bytes,
                heartbeat_secs,
                store_incarnation: runtime.store_incarnation(),
                ident,
                alert_dispatcher,
                push_rx,
                active_channels,
                drain_notify,
                socket,
            })
            .await;
        }))
}

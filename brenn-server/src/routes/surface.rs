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
use axum::response::{IntoResponse, Response};
use brenn_attach_proto::max_client_frame_bytes;
use brenn_attach_server::profile::AttachProfile;
use brenn_attach_server::registry::{
    AttachSessionGuard, AttachSessionHandle, CloseReason, PUSH_QUEUE_FRAMES, RegisterRejection,
};
use brenn_attach_server::session::{
    AttachSessionParams, run_attach_session, sanitize_client_detail,
};
use brenn_db::auth::session::Session;
use brenn_obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_surface_server::{SurfaceRuntime, telemetry};
use tracing::warn;
use uuid::Uuid;

use crate::client_ip::ClientIp;
use crate::routes::ws::{
    SURFACE_RECONFIGURED_CLOSE_CODE, SURFACE_RETIRED_CLOSE_CODE, close_with_stale_client,
};
use crate::state::{AppState, SurfaceCell, SurfaceLookup};

/// Why a reload is closing a surface's live sessions.
///
/// The two cases differ in what the page should do next, which is the whole
/// content of the close code: a reconfigured surface still exists and its page
/// reloads into the new runtime, a retired one does not and its page stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SurfaceCloseReason {
    /// The surface is gone from the deployment document.
    Retired,
    /// The surface is being replaced: its resolved value, one of its channels,
    /// or the bytes of its kind moved.
    Reconfigured,
}

impl SurfaceCloseReason {
    /// The transport-level close this reason is sent as. The text is peer-facing
    /// and fixed here; nothing an attacher supplied ever reaches it.
    pub fn close(self) -> CloseReason {
        match self {
            Self::Retired => CloseReason::new(SURFACE_RETIRED_CLOSE_CODE, "surface retired"),
            Self::Reconfigured => {
                CloseReason::new(SURFACE_RECONFIGURED_CLOSE_CODE, "surface reconfigured")
            }
        }
    }
}

/// Why a surface door turned a request away, and the response that says so.
///
/// A named enum rather than a bare status code because `Reconfiguring` carries
/// a `Retry-After` header, and because the page door's form of it carries a
/// body that comes back on its own.
pub enum SurfaceDenial {
    /// No such surface. A probe: the security event is emitted where the
    /// lookup happens.
    NotFound,
    /// A reload is swapping this surface right now. `navigation` is set for the
    /// page door, whose caller is a browser following a URL rather than a
    /// connector that retries on its own.
    Reconfiguring { navigation: bool },
    /// The authenticated user may not reach this surface.
    Forbidden,
    /// The surface, or this account's share of it, is at its session cap.
    Full,
}

impl IntoResponse for SurfaceDenial {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND.into_response(),
            Self::Reconfiguring { navigation } => {
                let retry = [(
                    axum::http::header::RETRY_AFTER,
                    RECONFIGURING_RETRY_AFTER_SECS.to_string(),
                )];
                if navigation {
                    // Through the shared page builder, so the swap-window
                    // document carries the same content type and `no-store`
                    // policy every other page this server emits does.
                    let mut response = crate::routes::app::page_html(reconfiguring_page());
                    *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                    response.headers_mut().insert(
                        axum::http::header::RETRY_AFTER,
                        axum::http::HeaderValue::from(RECONFIGURING_RETRY_AFTER_SECS),
                    );
                    response
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, retry).into_response()
                }
            }
            Self::Forbidden => StatusCode::FORBIDDEN.into_response(),
            Self::Full => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        }
    }
}

/// How long a page told to come back later should wait, in seconds. A reload's
/// surface swap is three map writes and however long the retiring sessions take
/// to leave, so one second is the right order; the kernel's own connector
/// backoff is what handles a swap that runs longer.
const RECONFIGURING_RETRY_AFTER_SECS: u32 = 1;

/// The body of the page door's 503, whose refresh delay is
/// [`RECONFIGURING_RETRY_AFTER_SECS`] itself rather than a second spelling of
/// it.
///
/// `Retry-After` is advisory and no browser acts on it, so a page that reloaded
/// itself into the swap window would sit on an error document until someone
/// pressed reload. The meta refresh is what makes the swap window invisible to
/// a page that reloaded a few milliseconds early. The only interpolation is the
/// delay: no script, and nothing about the request reaches the markup.
fn reconfiguring_page() -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="{RECONFIGURING_RETRY_AFTER_SECS}">
<title>Reconfiguring</title>
</head>
<body><p>This surface is being reconfigured; this page comes back on its own.</p></body>
</html>
"#
    )
}

/// Shared pre-serve authorization for the surface page and WS handlers: resolve
/// the slug and enforce the access check, emitting the same fail2ban security
/// events from both entry points. `is_ws` selects the endpoint-specific detail
/// strings only. Unknown slug → 404 + `UnrecognizedUrl` (probe signal, slug
/// sanitized); slug mid-swap → 503 + `Retry-After`, no security event; denied
/// user → 403 + `AuthFailure`.
///
/// The mid-swap answer is why there is a `Reconfiguring` denial at all: a page
/// that arrives during a reload's retire→start window is a legitimate page, and
/// telling it "not here, and you are a probe" would both lie and feed fail2ban
/// with first-party traffic.
pub(crate) fn authorize_surface(
    state: &AppState,
    slug: &str,
    username: &str,
    ip: std::net::IpAddr,
    is_ws: bool,
) -> Result<Arc<SurfaceRuntime>, SurfaceDenial> {
    let runtime = match state.surfaces.lookup(slug) {
        SurfaceLookup::Ready(runtime) => runtime,
        SurfaceLookup::Reconfiguring => {
            warn!(
                surface = %slug,
                user = %username,
                "surface is mid-reload; answering 503 with a retry hint"
            );
            return Err(SurfaceDenial::Reconfiguring { navigation: !is_ws });
        }
        SurfaceLookup::Unknown => {
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
            return Err(SurfaceDenial::NotFound);
        }
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
        return Err(SurfaceDenial::Forbidden);
    }

    Ok(runtime)
}

/// The door's second look at the surface table, taken once the registry slot is
/// held.
///
/// [`authorize_surface`] resolves the runtime and nothing re-consults the table
/// afterwards, so a slug can be withdrawn between that lookup and the
/// registration — the intervening work is a uuid, two channels and a couple of
/// `Arc`s, all preemptible. A reload marks the table before it waits for the
/// surface to go quiet, so a registration that outran the wait sees the mark
/// here and is turned away, and one that did not is closed by the reload's
/// re-issued `close_all`. The two halves meet with no gap, which is what keeps
/// a session from owing a terminal stamp against a registration the commit walk
/// has already retired.
///
/// The guard travels through the call so a refusal releases the slot here
/// rather than leaving that to a caller to remember. The `Unknown` arm emits no
/// security event: this is a page that lost a race, not a probe.
fn hold_slot_if_still_served(
    surfaces: &SurfaceCell,
    slug: &str,
    runtime: &Arc<SurfaceRuntime>,
    guard: AttachSessionGuard,
) -> Result<AttachSessionGuard, SurfaceDenial> {
    match surfaces.lookup(slug) {
        SurfaceLookup::Ready(current) if Arc::ptr_eq(&current, runtime) => Ok(guard),
        SurfaceLookup::Ready(_) | SurfaceLookup::Reconfiguring => {
            drop(guard);
            warn!(
                surface = %slug,
                "surface swapped while a session was attaching; answering 503 with a retry hint"
            );
            Err(SurfaceDenial::Reconfiguring { navigation: false })
        }
        SurfaceLookup::Unknown => {
            drop(guard);
            warn!(
                surface = %slug,
                "surface retired while a session was attaching; answering 404"
            );
            Err(SurfaceDenial::NotFound)
        }
    }
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
) -> Result<Response, SurfaceDenial> {
    let runtime = authorize_surface(&state, &slug, &session.user.username, ip, true)?;

    // Register the slot before upgrading so the check has no
    // check-then-register race and a full surface never upgrades the socket.
    let session_id = Uuid::new_v4();
    let (push_tx, push_rx) = tokio::sync::mpsc::channel(PUSH_QUEUE_FRAMES);
    let active_channels = Arc::new(Mutex::new(HashSet::new()));
    let drain_notify = Arc::new(tokio::sync::Notify::new());
    // The sender rides in the registry handle, so whoever holds the registry can
    // close this session; the receiver goes to the session task.
    let (close_tx, close_rx) = tokio::sync::watch::channel(None);
    let handle = AttachSessionHandle {
        session_id,
        account: session.user.username.clone(),
        push_tx,
        active_channels: active_channels.clone(),
        drain_notify: drain_notify.clone(),
        close: close_tx,
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
            return Err(SurfaceDenial::Full);
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
            return Err(SurfaceDenial::Full);
        }
    };

    let guard = hold_slot_if_still_served(&state.surfaces, &slug, &runtime, guard)?;

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

    // Taken while the session still holds its registry slot, so an attacher
    // this route owes a terminal stamp is never momentarily silent: the slot is
    // released inside the session task, and a reload waiting for the surface to
    // go quiet would otherwise proceed to retire the registration this publish
    // resolves against.
    let drain = state.attach_registry.drain_ticket(&slug);
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
                close_rx,
                socket,
            })
            .await;
            if outcome.last_detach {
                telemetry::publish_terminal_disconnected_stamp(&runtime, session_id).await;
            }
            drop(drain);
        }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use brenn_attach_server::registry::SessionCaps;
    use brenn_surface_server::fixtures_config::{SurfaceFixture, description_params};
    use brenn_surface_server::test_fixtures::{TEST_MAX_BODY_BYTES, install_surface_runtimes};

    /// A state serving one `deskbar` surface, beside the runtime the door would
    /// have resolved for it.
    fn deskbar_state() -> (brenn_db::Db, AppState, Arc<SurfaceRuntime>) {
        let db = crate::test_support::init_db_memory();
        let state = crate::test_support::state::test_state(&db);
        state.surfaces.set_runtimes(deskbar_runtimes());
        let runtime = ready(&state.surfaces);
        (db, state, runtime)
    }

    fn deskbar_runtimes() -> std::collections::HashMap<String, Arc<SurfaceRuntime>> {
        install_surface_runtimes(
            vec![
                SurfaceFixture::new("deskbar", "echo-stub")
                    .subscribe("ephemeral:dev-stub", "echo-stub", "messages")
                    .build(),
            ],
            Some(brenn_messaging::testutils::empty_directory_messenger(
                "test",
            )),
            TEST_MAX_BODY_BYTES,
            None,
            description_params(),
        )
    }

    fn ready(surfaces: &SurfaceCell) -> Arc<SurfaceRuntime> {
        match surfaces.lookup("deskbar") {
            SurfaceLookup::Ready(runtime) => runtime,
            _ => panic!("the fixture installed the surface"),
        }
    }

    /// The registry slot the door takes before it upgrades, minted the way
    /// `surface_ws_handler` mints it.
    fn take_a_slot(state: &AppState) -> AttachSessionGuard {
        let (push_tx, _push_rx) = tokio::sync::mpsc::channel(PUSH_QUEUE_FRAMES);
        let (close_tx, _close_rx) = tokio::sync::watch::channel(None);
        let handle = AttachSessionHandle {
            session_id: Uuid::new_v4(),
            account: "dev".to_string(),
            push_tx,
            active_channels: Arc::new(Mutex::new(HashSet::new())),
            drain_notify: Arc::new(tokio::sync::Notify::new()),
            close: close_tx,
        };
        state
            .attach_registry
            .try_register(
                "deskbar",
                handle,
                SessionCaps {
                    per_attacher: 4,
                    per_account: 4,
                },
            )
            .expect("the fixture registry is empty")
    }

    /// The window the re-check exists for: a reload marks the table and only
    /// then waits for the surface to go quiet, so a door that registered after
    /// the mark can outrun that wait. Attaching anyway would leave a session on
    /// the old wiring and, on the retire path, owe a terminal stamp against a
    /// registration the commit walk is about to take away — a `MissingSender`
    /// panic mid-reload.
    #[tokio::test]
    async fn a_surface_retired_after_the_registration_is_turned_away() {
        let (_db, state, runtime) = deskbar_state();
        let guard = take_a_slot(&state);

        state.surfaces.retire("deskbar");
        let denial = match hold_slot_if_still_served(&state.surfaces, "deskbar", &runtime, guard) {
            Err(denial) => denial,
            Ok(_) => panic!("a retired slug is not served"),
        };

        assert_eq!(denial.into_response().status(), StatusCode::NOT_FOUND);
        assert_eq!(
            state.attach_registry.count("deskbar"),
            0,
            "the refusal releases the slot it was handed"
        );
        assert!(
            state.attach_registry.is_quiet("deskbar"),
            "a refused registration owes no terminal stamp, so the reload's wait is not held open"
        );
    }

    #[tokio::test]
    async fn a_surface_marked_mid_swap_after_the_registration_is_told_to_come_back() {
        let (_db, state, runtime) = deskbar_state();
        let guard = take_a_slot(&state);

        state.surfaces.begin_reconfigure("deskbar");
        let denial = match hold_slot_if_still_served(&state.surfaces, "deskbar", &runtime, guard) {
            Err(denial) => denial,
            Ok(_) => panic!("a slug mid-swap is not served"),
        };

        let response = denial.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get("retry-after").unwrap(),
            RECONFIGURING_RETRY_AFTER_SECS.to_string().as_str(),
            "the connector is a kernel, not a navigation: a header and no document"
        );
        assert_eq!(state.attach_registry.count("deskbar"), 0);
    }

    /// The swap can also be *finished* by the time the re-check runs, in which
    /// case the table answers `Ready` — with a different runtime. Attaching the
    /// session to the runtime this request resolved before the swap would put a
    /// page on wiring the reload has already unfolded, so the identity of the
    /// runtime is what the check compares, not merely the slug's presence.
    #[tokio::test]
    async fn a_surface_replaced_after_the_registration_does_not_attach_to_the_old_runtime() {
        let (_db, state, runtime) = deskbar_state();
        let guard = take_a_slot(&state);

        state.surfaces.set_runtimes(deskbar_runtimes());
        assert!(
            !Arc::ptr_eq(&ready(&state.surfaces), &runtime),
            "the second install is a different runtime for the same slug"
        );
        let denial = match hold_slot_if_still_served(&state.surfaces, "deskbar", &runtime, guard) {
            Err(denial) => denial,
            Ok(_) => panic!("the resolved runtime is no longer the one being served"),
        };

        assert_eq!(
            denial.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(state.attach_registry.count("deskbar"), 0);
    }

    #[tokio::test]
    async fn an_untouched_surface_keeps_the_slot_it_registered() {
        let (_db, state, runtime) = deskbar_state();
        let guard = take_a_slot(&state);

        let guard = match hold_slot_if_still_served(&state.surfaces, "deskbar", &runtime, guard) {
            Ok(guard) => guard,
            Err(_) => panic!("nothing moved under this registration"),
        };

        assert_eq!(state.attach_registry.count("deskbar"), 1);
        drop(guard);
        assert_eq!(state.attach_registry.count("deskbar"), 0);
    }

    /// The join between the two halves of the close contract: the reason a
    /// reload names and the code the kernel classifies. Swapping the two arms
    /// compiles and passes every test on either side of the join, and makes a
    /// retired surface's page reload forever against a 404.
    #[test]
    fn each_close_reason_carries_its_own_code_and_text() {
        assert_eq!(
            SurfaceCloseReason::Retired.close(),
            CloseReason::new(SURFACE_RETIRED_CLOSE_CODE, "surface retired"),
        );
        assert_eq!(
            SurfaceCloseReason::Reconfigured.close(),
            CloseReason::new(SURFACE_RECONFIGURED_CLOSE_CODE, "surface reconfigured"),
        );
        assert_ne!(
            SurfaceCloseReason::Retired.close(),
            SurfaceCloseReason::Reconfigured.close(),
        );
    }
}

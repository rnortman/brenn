//! Inbound webhook handler — serves `POST <endpoint.mount>` for each configured
//! endpoint.
//!
//! One handler function is shared across all configured endpoints. Each route is
//! registered at the literal mount path (e.g. `/webhooks/phonebuddy`); the endpoint
//! slug is injected per-route via an axum `Extension` layer so the handler can
//! look up the resolved `ResolvedWebhookEndpoint` from the `WebhookService`.
//!
//! Mounted on the pre-auth utility-routes layer. Body size limiting is
//! applied per-endpoint via `DefaultBodyLimit`.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::SystemTime;

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::webhook::signature::{VerifiedRequest, WebhookRejection, verify_request};
use brenn_wasm::{CheckInput, Header, ReplayComponent, ReplayError};

use crate::client_ip::ClientIp;
use crate::state::AppState;

/// Newtype wrapper for the per-route endpoint slug injected via `Extension`.
#[derive(Clone)]
pub struct EndpointSlug(pub String);

/// Inbound webhook handler. Validates auth per the endpoint's configured
/// `SignatureScheme`, then delivers the raw body to the owning app's
/// singleton conversation via the `WebhookEventRouter`.
///
/// Registered as `POST <endpoint.mount>` for each configured endpoint.
/// The endpoint slug is injected per-route via `Extension(EndpointSlug(...))`.
pub async fn receive(
    State(state): State<AppState>,
    Extension(EndpointSlug(endpoint_slug)): Extension<EndpointSlug>,
    Extension(ClientIp(ip)): Extension<ClientIp>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let dispatcher = &state.alert_dispatcher;

    // Routes are registered only when the service exists; None here is an
    // invariant violation — panic rather than silently returning 500.
    let webhook_svc = state.webhook.as_ref().unwrap_or_else(|| {
        panic!(
            "inbound webhook handler reached but WebhookService is None; routing invariant violated"
        )
    });

    let endpoint = match webhook_svc.endpoint_by_slug(&endpoint_slug) {
        Some(ep) => ep,
        None => {
            // A registered route's endpoint slug is missing from the service
            // index — startup sequencing bug, not an attacker scan. Log as a
            // structured error (not fail2ban) so the operator sees the config
            // mismatch, and return 404 so the sender does not assume success.
            tracing::error!(
                endpoint = %endpoint_slug,
                "webhook handler reached for slug not in WebhookService index; \
                 routing invariant violated — route registered but endpoint missing from service"
            );
            return StatusCode::NOT_FOUND.into_response();
        }
    };

    // Capture received_at once — shared by verify_request (skew check) and
    // the replay component (replay skew check and nonce ordering key).
    // Requirements §4.2.1: the two timestamps must be the same instant.
    let received_at = SystemTime::now();

    // Replay-component lookup: one map access, result used twice below
    // (gates the UTF-8 guard and the replay seam).
    let replay_component: Option<Arc<ReplayComponent>> =
        state.replay_components.get(&endpoint_slug).map(Arc::clone);

    // Header UTF-8 guard — gated on replay_component being present.
    // Non-UTF-8 header values cannot be marshaled across the WIT boundary
    // (which requires valid UTF-8 strings). Reject early rather than panic
    // inside the component. Unbound endpoints skip the guard entirely.
    //
    // This guard fires BEFORE signature verification, so any unauthenticated
    // sender can trigger it. Using log-only (fail2ban signal) here rather than
    // log_and_alert_security_event to avoid burning the phone-alert rate-limit
    // budget on pre-auth noise. Post-auth paths that reach the replay component
    // use log_and_alert_security_event as appropriate.
    if replay_component.is_some() {
        for (name, value) in headers.iter() {
            if value.to_str().is_err() {
                let detail = format!(
                    "non-UTF-8 header value on replay-protected endpoint; header={}, endpoint={}",
                    name.as_str(),
                    endpoint_slug,
                );
                brenn_lib::obs::security::log_security_event(
                    SecurityEventType::SchemaViolation,
                    ip,
                    &detail,
                );
                return (StatusCode::BAD_REQUEST, r#"{"error":"schema"}"#).into_response();
            }
        }
    }

    let content_type = headers.get(axum::http::header::CONTENT_TYPE);
    let result = verify_request(&endpoint, content_type, &headers, body, received_at);

    match result {
        Err(rejection) => {
            let (status, body_str) = rejection_response(&rejection);
            log_webhook_rejection(dispatcher, ip, &endpoint_slug, &rejection);
            (status, body_str).into_response()
        }
        Ok(VerifiedRequest { key_id, body }) => {
            // Replay-protection seam. Only runs when a component is bound for
            // this endpoint.
            if let Some(component) = replay_component {
                // Build CheckInput. UTF-8 validity of header values guaranteed
                // above (the guard returned early for any non-UTF-8 value).
                let received_at_ms: u64 = received_at
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_else(|_| {
                        tracing::error!(
                            endpoint = %endpoint_slug,
                            key_id = %key_id,
                            "SystemTime before UNIX_EPOCH — clock misconfigured"
                        );
                        panic!(
                            "SystemTime before UNIX_EPOCH — clock misconfigured \
                             (endpoint={endpoint_slug})"
                        )
                    })
                    .as_millis()
                    .try_into()
                    .expect("received_at_ms overflows u64 — not plausible before year 584556019");
                let check_input = CheckInput {
                    headers: headers
                        .iter()
                        .map(|(n, v)| Header {
                            name: n.as_str().to_string(),
                            // `.unwrap()` safe: guard above ensures UTF-8 validity.
                            value: v.to_str().unwrap().to_string(),
                        })
                        .collect(),
                    body: body.as_bytes().to_vec(),
                    received_at: received_at_ms,
                    key_id: key_id.clone(),
                    endpoint_slug: endpoint_slug.clone(),
                };

                // Serialize replay checks for this endpoint behind a per-endpoint
                // tokio Mutex. The underlying ReplayComponent uses an AtomicBool CAS
                // guard that fails-immediately (→ panic → 500) on concurrent calls.
                // Holding this lock across spawn_blocking ensures at most one
                // in-flight check per endpoint at any time. Lock is acquired async
                // (no executor blocking); the blocking work runs inside the guard.
                let _replay_lock = state
                    .replay_locks
                    .get(&endpoint_slug)
                    .cloned()
                    .unwrap_or_else(|| {
                        panic!(
                            "replay_lock missing for endpoint={} — replay_components and \
                             replay_locks must be populated together at startup",
                            endpoint_slug
                        )
                    })
                    .lock_owned()
                    .await;

                // Spawn-blocking: `ReplayComponent::check` drives wasmtime +
                // SQLite synchronously. Must not block the async executor.
                // Closes `wasm-drop-wasi` tokio sub-item.
                let join = tokio::task::spawn_blocking(move || component.check(&check_input));
                let (verdict, quota_hit) = join.await.unwrap_or_else(|je| {
                    // JoinError::is_panic() → re-propagate the panic so the
                    // CatchPanicLayer above turns it into a 500 + error log.
                    // Cancellation cannot occur here: no select! races the join.
                    if je.is_panic() {
                        // Log endpoint identity before re-propagating: the panic
                        // payload is a generic trap string (no slug). This is the
                        // design §3.5(a) diagnosability point — on a first-request
                        // anonymous trap it tells the operator which endpoint to
                        // inspect (likely: replay-generic paired with a skew-less
                        // scheme → brenn.max-skew-secs not injected).
                        tracing::error!(
                            endpoint = %endpoint_slug,
                            key_id = %key_id,
                            "replay check task panicked (guest trap?) — re-propagating; \
                             if this is the first request on this endpoint, suspect \
                             missing/invalid brenn.max-skew-secs (component paired with \
                             a skew-less scheme)"
                        );
                        std::panic::resume_unwind(je.into_panic());
                    }
                    panic!(
                        "spawn_blocking task was cancelled for endpoint={endpoint_slug} \
                         key_id={key_id} — unexpected; investigate runtime shutdown or select! race"
                    )
                });

                // Host-quota hit: fire a Warning phone alert (distinct from the
                // guest-abuse 429 path in route_replay_error). This is operator
                // signal — the store has reached its configured size cap — not a
                // fail2ban signal. The log was already emitted host-side at the
                // SQLite layer (§2.E layer 1). Alert fires regardless of verdict
                // so the two signals remain separable.
                if quota_hit {
                    let body = format!(
                        "endpoint={endpoint_slug} key_id={key_id} \
                         — WASM store reached its host-enforced size cap (SQLITE_FULL). \
                         The over-cap write was rejected; the store is still readable and \
                         recoverable via the guest's prune/delete path."
                    );
                    dispatcher.alert(
                        brenn_lib::obs::alerting::AlertSeverity::Warning,
                        "WASM store quota exceeded".to_string(),
                        body,
                    );
                }

                match verdict {
                    Ok(()) => {
                        // Accept — fall through to deliver_inbound below.
                        tracing::debug!(
                            endpoint = %endpoint_slug,
                            key_id = %key_id,
                            "replay check accepted"
                        );
                    }
                    Err(e) => {
                        return route_replay_error(
                            &e,
                            &endpoint_slug,
                            &key_id,
                            ip,
                            dispatcher,
                            quota_hit,
                        );
                    }
                }
            }

            // Deliver to the owning app's singleton conversation.
            // `set_router` is called before the listener starts; None here
            // means a startup sequencing bug — panic rather than silently
            // dropping an authenticated payload.
            let router = webhook_svc
                .router()
                .unwrap_or_else(|| panic!("WebhookService router not set at request time; startup sequencing invariant violated"));

            let owner = &endpoint.owner;

            match router
                .deliver_inbound(
                    &endpoint_slug,
                    owner,
                    &key_id,
                    headers,
                    ip,
                    received_at,
                    body,
                    endpoint.urgency,
                )
                .await
            {
                Ok(()) => StatusCode::NO_CONTENT.into_response(),
                Err(e) => {
                    tracing::error!(
                        endpoint = %endpoint_slug,
                        owner = %owner,
                        error = %e,
                        "deliver_inbound failed; returning 500 to webhook sender"
                    );
                    // Return a JSON body consistent with all other rejection paths so
                    // the CLI's stderr diagnostic includes detail rather than a bare status.
                    // (errhandling-5)
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        [("content-type", "application/json")],
                        r#"{"error":"internal"}"#,
                    )
                        .into_response()
                }
            }
        }
    }
}

/// Route a `ReplayError` to the appropriate HTTP response.
///
/// Extracted from the `match verdict` block in `receive` so the typed arms can
/// be unit-tested directly with synthetic `&ReplayError` values (tests 3 and 4)
/// without WASM execution.
///
/// The `Ok(())` fall-through to `deliver_inbound` stays in the caller;
/// this function handles only the `Err(_)` cases.
///
/// `quota_hit`: set when the 429 was caused by the host-enforced store size cap
/// (not guest logic). When true, the `TooManyRequests` arm skips the fail2ban
/// `ReplayCapHit` security event — the operator has already been alerted via the
/// quota Warning path, and banning a legitimate sender's IP for a storage-cap
/// condition would be incorrect.
fn route_replay_error(
    err: &ReplayError,
    endpoint_slug: &str,
    key_id: &str,
    ip: IpAddr,
    dispatcher: &AlertDispatcher,
    quota_hit: bool,
) -> Response {
    match err {
        ReplayError::TimestampOutOfWindow => {
            let detail = format!("replay skew endpoint={} key_id={}", endpoint_slug, key_id);
            log_and_alert_security_event(dispatcher, SecurityEventType::ReplaySkew, ip, &detail);
            /* stable replay reject body strings */
            (StatusCode::CONFLICT, r#"{"error":"replay-skew"}"#).into_response()
        }
        ReplayError::Duplicate => {
            let detail = format!(
                "replay duplicate endpoint={} key_id={}",
                endpoint_slug, key_id
            );
            log_and_alert_security_event(
                dispatcher,
                SecurityEventType::ReplayDuplicate,
                ip,
                &detail,
            );
            /* stable replay reject body strings */
            (StatusCode::CONFLICT, r#"{"error":"replay-duplicate"}"#).into_response()
        }
        ReplayError::MonotonicityViolation => {
            let detail = format!(
                "replay monotonicity endpoint={} key_id={}",
                endpoint_slug, key_id
            );
            log_and_alert_security_event(
                dispatcher,
                SecurityEventType::ReplayMonotonicity,
                ip,
                &detail,
            );
            /* stable replay reject body strings */
            (StatusCode::CONFLICT, r#"{"error":"replay-monotonicity"}"#).into_response()
        }
        ReplayError::TooManyRequests => {
            if quota_hit {
                // Host-quota hit: the store has reached its configured size cap.
                // The operator has already been alerted via the Warning path
                // (§2.E layer 2). Do NOT fire ReplayCapHit here — that would
                // ban the sender's IP via fail2ban for a storage-capacity
                // condition, not for abuse. Return 429 without a security event.
                tracing::warn!(
                    endpoint = %endpoint_slug,
                    key_id = %key_id,
                    "replay check: TooManyRequests from host store quota (no fail2ban)"
                );
            } else {
                // Guest CAP hit: abuse signal → fail2ban lane.
                let detail = format!(
                    "replay cap-hit endpoint={} key_id={}",
                    endpoint_slug, key_id
                );
                log_and_alert_security_event(
                    dispatcher,
                    SecurityEventType::ReplayCapHit,
                    ip,
                    &detail,
                );
            }
            /* stable replay reject body strings */
            (
                StatusCode::TOO_MANY_REQUESTS,
                r#"{"error":"replay-cap-hit"}"#,
            )
                .into_response()
        }
        ReplayError::MalformedInput(diag) => {
            // Malformed input: log diagnostic (never echoed in HTTP response —
            // closes `wasm-malformed-input-http`). String content carries no
            // routing semantics — cap-hit is now a typed variant (`TooManyRequests`).
            let detail = format!(
                "replay malformed-input endpoint={} key_id={} diag={}",
                endpoint_slug, key_id, diag
            );
            log_and_alert_security_event(
                dispatcher,
                SecurityEventType::ReplayMalformed,
                ip,
                &detail,
            );
            /* stable replay reject body strings */
            (StatusCode::BAD_REQUEST, r#"{"error":"replay-malformed"}"#).into_response()
        }
    }
}

/// Map a `WebhookRejection` to an HTTP status + response body string.
fn rejection_response(rejection: &WebhookRejection) -> (StatusCode, &'static str) {
    match rejection {
        WebhookRejection::WrongContentType => (StatusCode::UNSUPPORTED_MEDIA_TYPE, ""),
        WebhookRejection::BodyNotUtf8 => (StatusCode::BAD_REQUEST, r#"{"error":"schema"}"#),
        WebhookRejection::MissingOrMalformedSignatureHeader
        | WebhookRejection::MissingOrMalformedKeyIdHeader
        | WebhookRejection::UnknownKeyId
        | WebhookRejection::HmacMismatch
        | WebhookRejection::TimestampOutOfWindow => {
            (StatusCode::UNAUTHORIZED, r#"{"error":"auth"}"#)
        }
    }
}

/// Emit a `log_and_alert_security_event` for a webhook rejection.
///
/// `WrongContentType` and `BodyNotUtf8` are schema violations (fail2ban lane:
/// `SchemaViolation`). All auth variants use `AuthFailure`. This distinction
/// lets fail2ban apply different thresholds for structural probes vs auth
/// failures (requirements v3 §6.5).
fn log_webhook_rejection(
    dispatcher: &AlertDispatcher,
    ip: IpAddr,
    endpoint_slug: &str,
    rejection: &WebhookRejection,
) {
    let detail = format!("webhook[{endpoint_slug}]: {rejection:?}");
    let event_type = match rejection {
        WebhookRejection::WrongContentType | WebhookRejection::BodyNotUtf8 => {
            SecurityEventType::SchemaViolation
        }
        WebhookRejection::MissingOrMalformedSignatureHeader
        | WebhookRejection::MissingOrMalformedKeyIdHeader
        | WebhookRejection::UnknownKeyId
        | WebhookRejection::HmacMismatch
        | WebhookRejection::TimestampOutOfWindow => SecurityEventType::AuthFailure,
    };
    log_and_alert_security_event(dispatcher, event_type, ip, &detail);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::{IpAddr, SocketAddr};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    use axum::Router;
    use axum::body::Body;
    use axum::extract::DefaultBodyLimit;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::{HeaderMap, Request, StatusCode};
    use axum::middleware as axum_mw;
    use axum::routing::post;
    use brenn_lib::messaging::Urgency;
    use brenn_lib::obs::alerting::{AlertDispatcher, make_capturing_alerter};
    use brenn_lib::webhook::config::ResolvedWebhookEndpoint;
    use brenn_lib::webhook::service::{WebhookEventRouter, WebhookService};
    use brenn_lib::webhook::signature::{HexFormat, SignatureAlgorithm, SignatureScheme};
    use tower::ServiceExt;

    use super::*;
    use crate::client_ip::{TrustedProxyHops, resolve_client_ip};

    // -----------------------------------------------------------------------
    // Captured delivery record
    // -----------------------------------------------------------------------

    #[derive(Debug, Clone)]
    struct Delivery {
        endpoint_slug: String,
        owning_app_slug: String,
        key_id: String,
        headers: Vec<(String, String)>,
        client_ip: IpAddr,
        raw_body: String,
        urgency: Urgency,
    }

    /// Mock `WebhookEventRouter` that records every `deliver_inbound` call.
    struct CapturingRouter {
        deliveries: Mutex<Vec<Delivery>>,
    }

    impl CapturingRouter {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                deliveries: Mutex::new(Vec::new()),
            })
        }

        fn drain(&self) -> Vec<Delivery> {
            self.deliveries.lock().unwrap().drain(..).collect()
        }
    }

    #[async_trait::async_trait]
    impl WebhookEventRouter for CapturingRouter {
        async fn deliver_inbound(
            &self,
            endpoint_slug: &str,
            owner: &brenn_lib::webhook::config::WebhookOwner,
            key_id: &str,
            headers: HeaderMap,
            client_ip: IpAddr,
            _received_at: std::time::SystemTime,
            raw_body: String,
            urgency: Urgency,
        ) -> Result<(), String> {
            let headers_vec: Vec<(String, String)> = headers
                .iter()
                .filter_map(|(n, v)| {
                    v.to_str()
                        .ok()
                        .map(|s| (n.as_str().to_owned(), s.to_owned()))
                })
                .collect();
            self.deliveries.lock().unwrap().push(Delivery {
                endpoint_slug: endpoint_slug.to_owned(),
                owning_app_slug: owner.slug().to_owned(),
                key_id: key_id.to_owned(),
                headers: headers_vec,
                client_ip,
                raw_body,
                urgency,
            });
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // Test fixtures
    // -----------------------------------------------------------------------

    const TEST_SLUG: &str = "phonebuddy";
    const TEST_APP_SLUG: &str = "pa-alice";
    const TEST_KEY_ID: &str = "primary";
    const TEST_SECRET: &[u8] = b"super-secret-hmac-key";
    const TEST_MOUNT: &str = "/webhooks/phonebuddy";

    /// Build an `HmacRawBody` `ResolvedWebhookEndpoint` for the phonebuddy shape.
    fn phonebuddy_endpoint() -> Arc<ResolvedWebhookEndpoint> {
        let mut keys = HashMap::new();
        keys.insert(TEST_KEY_ID.to_string(), TEST_SECRET.to_vec());
        Arc::new(ResolvedWebhookEndpoint {
            slug: TEST_SLUG.to_string(),
            mount: TEST_MOUNT.to_string(),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacRawBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                header: "x-phonebuddy-signature".parse().unwrap(),
                format: HexFormat::V1Hex,
                key_id_header: Some("x-phonebuddy-key-id".parse().unwrap()),
                keys,
            },
            owner: brenn_lib::webhook::config::WebhookOwner::App(Arc::from(TEST_APP_SLUG)),
            urgency: brenn_lib::messaging::Urgency::Normal,
            replay_protection: None,
        })
    }

    /// Compute `v1=<hex>` HMAC-SHA256 over `body` using `TEST_SECRET`.
    fn sign_v1hex(body: &[u8]) -> String {
        format!(
            "v1={}",
            brenn_lib::webhook::signature::hmac_sha256_hex(TEST_SECRET, body)
        )
    }

    /// Build a minimal test axum router for the inbound webhook handler.
    /// The `AppState` is constructed with `webhook` populated from `svc`.
    fn test_router(svc: Arc<WebhookService>, capture: Arc<CapturingRouter>) -> Router {
        let router_trait: Arc<dyn WebhookEventRouter> = capture;
        svc.set_router(router_trait);

        let db = brenn_lib::db::init_db_memory();
        let (alert_dispatcher, _handle) = AlertDispatcher::noop();
        let mut state = crate::state::AppState::for_test(db, None);
        state.alert_dispatcher = alert_dispatcher;
        state.webhook = Some(svc.clone());

        let slug = TEST_SLUG.to_string();
        let ceiling = 1024 * 1024usize;

        Router::new()
            .route(
                TEST_MOUNT,
                post(receive).layer(
                    tower::ServiceBuilder::new()
                        .layer(axum::Extension(EndpointSlug(slug)))
                        .layer(DefaultBodyLimit::max(ceiling)),
                ),
            )
            .with_state(state)
            .layer(axum_mw::from_fn(resolve_client_ip))
            .layer(axum::Extension(TrustedProxyHops(0)))
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))))
    }

    /// Build a `WebhookService` containing only the phonebuddy endpoint.
    fn phonebuddy_service() -> Arc<WebhookService> {
        WebhookService::new(vec![(TEST_SLUG.to_string(), phonebuddy_endpoint())])
    }

    /// POST to `TEST_MOUNT` with the given body, content-type, signature, and
    /// key-id headers. `None` means the header is omitted.
    async fn post_to_endpoint(
        router: Router,
        body: Vec<u8>,
        content_type: Option<&str>,
        signature: Option<&str>,
        key_id: Option<&str>,
    ) -> axum::response::Response {
        let mut builder = Request::builder().method("POST").uri(TEST_MOUNT);
        if let Some(ct) = content_type {
            builder = builder.header("content-type", ct);
        }
        if let Some(sig) = signature {
            builder = builder.header("x-phonebuddy-signature", sig);
        }
        if let Some(kid) = key_id {
            builder = builder.header("x-phonebuddy-key-id", kid);
        }
        let req = builder.body(Body::from(body)).unwrap();
        router.oneshot(req).await.unwrap()
    }

    // -----------------------------------------------------------------------
    // Acceptance §5.4 happy path
    // -----------------------------------------------------------------------

    /// A valid signed POST returns 204 and the raw body reaches `deliver_inbound`.
    #[tokio::test]
    async fn valid_signed_post_returns_204_and_delivers() {
        let capture = CapturingRouter::new();
        let svc = phonebuddy_service();
        let router = test_router(svc, Arc::clone(&capture));

        let body = br#"{"kind":"ping","client_id":"c1","nonce":"abc"}"#;
        let sig = sign_v1hex(body);

        let resp = post_to_endpoint(
            router,
            body.to_vec(),
            Some("application/json"),
            Some(&sig),
            Some(TEST_KEY_ID),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::NO_CONTENT, "expected 204");

        let deliveries = capture.drain();
        assert_eq!(deliveries.len(), 1, "expected exactly one delivery");
        let d = &deliveries[0];
        assert_eq!(d.endpoint_slug, TEST_SLUG);
        assert_eq!(d.owning_app_slug, TEST_APP_SLUG);
        assert_eq!(d.key_id, TEST_KEY_ID);
        assert_eq!(d.raw_body, std::str::from_utf8(body).unwrap());
        assert_eq!(d.urgency, Urgency::Normal);
        // Headers and client_ip are now threaded through.
        assert!(
            !d.headers.is_empty(),
            "headers must be threaded through to deliver_inbound"
        );
        assert_eq!(
            d.client_ip,
            IpAddr::from([127, 0, 0, 1]),
            "client_ip must be threaded through"
        );
    }

    // -----------------------------------------------------------------------
    // §5 failure matrix: Content-Type / UTF-8
    // -----------------------------------------------------------------------

    /// Wrong Content-Type returns 415; no delivery.
    #[tokio::test]
    async fn wrong_content_type_returns_415() {
        let capture = CapturingRouter::new();
        let svc = phonebuddy_service();
        let router = test_router(svc, Arc::clone(&capture));

        let body = b"{}";
        let sig = sign_v1hex(body);
        let resp = post_to_endpoint(
            router,
            body.to_vec(),
            Some("text/plain"),
            Some(&sig),
            Some(TEST_KEY_ID),
        )
        .await;

        assert_eq!(
            resp.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "expected 415"
        );
        assert!(capture.drain().is_empty());
    }

    /// Non-UTF-8 body returns 400 with `{"error":"schema"}`; no delivery.
    #[tokio::test]
    async fn non_utf8_body_returns_400_schema() {
        let capture = CapturingRouter::new();
        let svc = phonebuddy_service();
        let router = test_router(svc, Arc::clone(&capture));

        let body = vec![0xff, 0xfe, 0xfd];
        let sig = sign_v1hex(&body);
        let resp = post_to_endpoint(
            router,
            body,
            Some("application/json"),
            Some(&sig),
            Some(TEST_KEY_ID),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "expected 400");
        let resp_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(resp_bytes.as_ref(), br#"{"error":"schema"}"#);
        assert!(capture.drain().is_empty());
    }

    // -----------------------------------------------------------------------
    // §5 failure matrix: signature / key-id auth
    // -----------------------------------------------------------------------

    /// Missing signature header returns 401 with `{"error":"auth"}`; no delivery.
    #[tokio::test]
    async fn missing_signature_header_returns_401() {
        let capture = CapturingRouter::new();
        let svc = phonebuddy_service();
        let router = test_router(svc, Arc::clone(&capture));

        let body = b"{}";
        let resp = post_to_endpoint(
            router,
            body.to_vec(),
            Some("application/json"),
            None, // omit sig header
            Some(TEST_KEY_ID),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "expected 401");
        let resp_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(resp_bytes.as_ref(), br#"{"error":"auth"}"#);
        assert!(capture.drain().is_empty());
    }

    /// Malformed signature header (wrong prefix for `v1-hex`) returns 401.
    #[tokio::test]
    async fn malformed_signature_header_returns_401() {
        let capture = CapturingRouter::new();
        let svc = phonebuddy_service();
        let router = test_router(svc, Arc::clone(&capture));

        let body = b"{}";
        // Send raw hex without the "v1=" prefix — malformed for V1Hex format.
        let bad_sig = hex::encode(b"not_a_real_mac_at_all_but_looks_hexlike");
        let resp = post_to_endpoint(
            router,
            body.to_vec(),
            Some("application/json"),
            Some(&bad_sig),
            Some(TEST_KEY_ID),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "expected 401");
        let resp_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(resp_bytes.as_ref(), br#"{"error":"auth"}"#);
        assert!(capture.drain().is_empty());
    }

    /// Unknown `key_id` returns 401; timing-parity dummy-key path executes.
    #[tokio::test]
    async fn unknown_key_id_returns_401() {
        let capture = CapturingRouter::new();
        let svc = phonebuddy_service();
        let router = test_router(svc, Arc::clone(&capture));

        let body = b"{}";
        let sig = sign_v1hex(body);
        let resp = post_to_endpoint(
            router,
            body.to_vec(),
            Some("application/json"),
            Some(&sig),
            Some("unknown-key"), // not in endpoint table
        )
        .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "expected 401");
        let resp_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(resp_bytes.as_ref(), br#"{"error":"auth"}"#);
        assert!(capture.drain().is_empty());
    }

    /// HMAC mismatch (correct key-id but wrong secret) returns 401.
    #[tokio::test]
    async fn hmac_mismatch_returns_401() {
        let capture = CapturingRouter::new();
        let svc = phonebuddy_service();
        let router = test_router(svc, Arc::clone(&capture));

        let body = b"{}";
        // Sign with a different key so the HMAC doesn't match.
        let bad_sig = format!(
            "v1={}",
            brenn_lib::webhook::signature::hmac_sha256_hex(b"wrong-key", body)
        );

        let resp = post_to_endpoint(
            router,
            body.to_vec(),
            Some("application/json"),
            Some(&bad_sig),
            Some(TEST_KEY_ID),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "expected 401");
        let resp_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(resp_bytes.as_ref(), br#"{"error":"auth"}"#);
        assert!(capture.drain().is_empty());
    }

    /// Missing key-id header when `key_id_header` is configured returns 401.
    #[tokio::test]
    async fn missing_key_id_header_returns_401() {
        let capture = CapturingRouter::new();
        let svc = phonebuddy_service();
        let router = test_router(svc, Arc::clone(&capture));

        let body = b"{}";
        let sig = sign_v1hex(body);
        // Omit the key-id header entirely.
        let resp = post_to_endpoint(
            router,
            body.to_vec(),
            Some("application/json"),
            Some(&sig),
            None, // omit key-id header
        )
        .await;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "expected 401");
        let resp_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(resp_bytes.as_ref(), br#"{"error":"auth"}"#);
        assert!(capture.drain().is_empty());
    }

    // -----------------------------------------------------------------------
    // §5.6 Timing-parity sanity: unknown key-id and HMAC mismatch both yield
    // 401 + same body (verified by code inspection above; this test confirms
    // neither leaks information via different response bodies).
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn unknown_key_id_and_hmac_mismatch_produce_identical_responses() {
        let body = b"{}";
        let sig = sign_v1hex(body);

        let capture1 = CapturingRouter::new();
        let router1 = test_router(phonebuddy_service(), Arc::clone(&capture1));
        let resp_unknown = post_to_endpoint(
            router1,
            body.to_vec(),
            Some("application/json"),
            Some(&sig),
            Some("no-such-key"),
        )
        .await;

        let capture2 = CapturingRouter::new();
        let router2 = test_router(phonebuddy_service(), capture2);
        let bad_sig = format!(
            "v1={}",
            brenn_lib::webhook::signature::hmac_sha256_hex(b"bad", body)
        );
        let resp_mismatch = post_to_endpoint(
            router2,
            body.to_vec(),
            Some("application/json"),
            Some(&bad_sig),
            Some(TEST_KEY_ID),
        )
        .await;

        assert_eq!(
            resp_unknown.status(),
            resp_mismatch.status(),
            "status must be identical"
        );
        let body_unknown = axum::body::to_bytes(resp_unknown.into_body(), 1024)
            .await
            .unwrap();
        let body_mismatch = axum::body::to_bytes(resp_mismatch.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(
            body_unknown, body_mismatch,
            "response body must be identical"
        );
    }

    // -----------------------------------------------------------------------
    // §5 acceptance: timestamp-out-of-window for HmacTimestampedBody → 401
    // -----------------------------------------------------------------------

    /// For `HmacTimestampedBody`, a timestamp outside `max_skew_secs` returns 401.
    #[tokio::test]
    async fn timestamp_out_of_window_returns_401() {
        let mut keys = HashMap::new();
        keys.insert(TEST_KEY_ID.to_string(), TEST_SECRET.to_vec());

        let endpoint = Arc::new(ResolvedWebhookEndpoint {
            slug: "slack-ep".to_string(),
            mount: "/webhooks/slack".to_string(),
            description: None,
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacTimestampedBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                sig_header: "x-slack-signature".parse().unwrap(),
                sig_format: HexFormat::V0Hex,
                timestamp_header: "x-slack-request-timestamp".parse().unwrap(),
                template_prefix: "v0:".to_string(),
                template_mid: ":".to_string(),
                template_suffix: String::new(),
                t_before_body: true,
                max_skew_secs: 300,
                key_id_header: None,
                keys,
            },
            owner: brenn_lib::webhook::config::WebhookOwner::App(Arc::from(TEST_APP_SLUG)),
            urgency: brenn_lib::messaging::Urgency::Normal,
            replay_protection: None,
        });

        let svc = WebhookService::new(vec![("slack-ep".to_string(), endpoint)]);
        let capture = CapturingRouter::new();
        svc.set_router(Arc::clone(&capture) as Arc<dyn WebhookEventRouter>);

        let db = brenn_lib::db::init_db_memory();
        let (alert_dispatcher, _handle) = AlertDispatcher::noop();
        let mut state = crate::state::AppState::for_test(db, None);
        state.alert_dispatcher = alert_dispatcher;
        state.webhook = Some(svc.clone());

        let slug = "slack-ep".to_string();
        let router = Router::new()
            .route(
                "/webhooks/slack",
                post(receive).layer(
                    tower::ServiceBuilder::new()
                        .layer(axum::Extension(EndpointSlug(slug)))
                        .layer(DefaultBodyLimit::max(1024 * 1024)),
                ),
            )
            .with_state(state)
            .layer(axum_mw::from_fn(resolve_client_ip))
            .layer(axum::Extension(TrustedProxyHops(0)))
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

        // Build a stale timestamp (600 seconds ago — outside max_skew_secs=300).
        let stale_t = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .checked_sub(Duration::from_secs(600))
            .unwrap()
            .as_secs();
        let body = br#"{"text":"hello"}"#;
        // Sign canonically (v0:<t>:<body>) — valid sig, but stale timestamp.
        let canonical = format!("v0:{}:{}", stale_t, std::str::from_utf8(body).unwrap());
        let sig = format!(
            "v0={}",
            brenn_lib::webhook::signature::hmac_sha256_hex(TEST_SECRET, canonical.as_bytes())
        );

        let req = Request::builder()
            .method("POST")
            .uri("/webhooks/slack")
            .header("content-type", "application/json")
            .header("x-slack-request-timestamp", stale_t.to_string())
            .header("x-slack-signature", sig)
            .body(Body::from(body.as_ref()))
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "expected 401 for stale timestamp"
        );
        let resp_bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(resp_bytes.as_ref(), br#"{"error":"auth"}"#);
        assert!(capture.drain().is_empty());
    }

    // -----------------------------------------------------------------------
    // §5 deliver_inbound failure → 500
    // -----------------------------------------------------------------------

    /// A `WebhookEventRouter` implementation that always returns `Err(...)`.
    struct FailingRouter;

    #[async_trait::async_trait]
    impl WebhookEventRouter for FailingRouter {
        async fn deliver_inbound(
            &self,
            _endpoint_slug: &str,
            _owner: &brenn_lib::webhook::config::WebhookOwner,
            _key_id: &str,
            _headers: HeaderMap,
            _client_ip: IpAddr,
            _received_at: std::time::SystemTime,
            _raw_body: String,
            _urgency: Urgency,
        ) -> Result<(), String> {
            Err("injected delivery failure".to_string())
        }
    }

    /// When `deliver_inbound` returns `Err`, the handler returns 500 so the
    /// sender knows to retry rather than treating the drop as success.
    #[tokio::test]
    async fn deliver_inbound_failure_returns_500() {
        let svc = phonebuddy_service();
        let failing: Arc<dyn WebhookEventRouter> = Arc::new(FailingRouter);
        svc.set_router(failing);

        let db = brenn_lib::db::init_db_memory();
        let (alert_dispatcher, _handle) = AlertDispatcher::noop();
        let mut state = crate::state::AppState::for_test(db, None);
        state.alert_dispatcher = alert_dispatcher;
        state.webhook = Some(svc.clone());

        let router = Router::new()
            .route(
                TEST_MOUNT,
                post(receive).layer(
                    tower::ServiceBuilder::new()
                        .layer(axum::Extension(EndpointSlug(TEST_SLUG.to_string())))
                        .layer(DefaultBodyLimit::max(1024 * 1024)),
                ),
            )
            .with_state(state)
            .layer(axum_mw::from_fn(resolve_client_ip))
            .layer(axum::Extension(TrustedProxyHops(0)))
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

        let body = br#"{"kind":"ping"}"#;
        let sig = sign_v1hex(body);
        let resp = post_to_endpoint(
            router,
            body.to_vec(),
            Some("application/json"),
            Some(&sig),
            Some(TEST_KEY_ID),
        )
        .await;

        assert_eq!(
            resp.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "expected 500 when deliver_inbound returns Err"
        );
    }

    // -----------------------------------------------------------------------
    // Body size limit (DefaultBodyLimit)
    // -----------------------------------------------------------------------

    /// Build a test router with a tiny body-size ceiling to exercise the
    /// `DefaultBodyLimit` layer. Bodies exceeding the ceiling return 413
    /// before the handler is called.
    fn test_router_with_ceiling(ceiling: usize, capture: Arc<CapturingRouter>) -> Router {
        let router_trait: Arc<dyn WebhookEventRouter> = capture;
        let svc = phonebuddy_service();
        svc.set_router(router_trait);

        let db = brenn_lib::db::init_db_memory();
        let (alert_dispatcher, _handle) = AlertDispatcher::noop();
        let mut state = crate::state::AppState::for_test(db, None);
        state.alert_dispatcher = alert_dispatcher;
        state.webhook = Some(svc.clone());

        Router::new()
            .route(
                TEST_MOUNT,
                post(receive).layer(
                    tower::ServiceBuilder::new()
                        .layer(axum::Extension(EndpointSlug(TEST_SLUG.to_string())))
                        .layer(DefaultBodyLimit::max(ceiling)),
                ),
            )
            .with_state(state)
            .layer(axum_mw::from_fn(resolve_client_ip))
            .layer(axum::Extension(TrustedProxyHops(0)))
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))))
    }

    /// A body one byte over the configured ceiling returns 413; handler is not called.
    #[tokio::test]
    async fn oversized_body_returns_413() {
        const CEILING: usize = 10;
        let capture = CapturingRouter::new();
        let router = test_router_with_ceiling(CEILING, Arc::clone(&capture));

        // Exactly one byte over the ceiling.
        let body = vec![b'x'; CEILING + 1];
        let sig = sign_v1hex(&body);
        let resp = post_to_endpoint(
            router,
            body,
            Some("application/json"),
            Some(&sig),
            Some(TEST_KEY_ID),
        )
        .await;

        assert_eq!(
            resp.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "expected 413 for oversized body"
        );
        assert!(
            capture.drain().is_empty(),
            "oversized body must not reach deliver_inbound"
        );
    }

    // -----------------------------------------------------------------------
    // End-to-end: real HTTP POST → real WebhookEventRouterImpl → real Messenger
    // -----------------------------------------------------------------------

    /// E2E: real HTTP POST with HMAC signature → real axum handler (no mock router)
    /// → real `WebhookEventRouterImpl` → real `Messenger::publish_transport_ingress`
    /// → subscribing conversation drains the message → asserts:
    ///   1. HTTP status 204.
    ///   2. Drained as `IngressOrBus::Bus(MessageEnvelope)` (not `Ingress`).
    ///   3. Rendered text starts with `[Webhook message]` (not `[Event]`).
    ///   4. `WebhookEnvelope` JSON is present with credential header masked.
    #[tokio::test]
    async fn e2e_real_post_real_router_drains_as_webhook_envelope() {
        use crate::webhook_router::WebhookEventRouterImpl;
        use brenn_lib::messaging::ParticipantId;
        use brenn_lib::messaging::format::{WEBHOOK_SINGLE_HEADING, format_messaging_event_single};
        use brenn_lib::messaging::{IngressOrBus, WebhookEnvelope};

        // Use TEST_APP_SLUG ("pa-alice") — phonebuddy_endpoint() hardcodes it as owning_app_slug.
        let (mut state, db, _user_id) = crate::test_support::state::test_state_with_user_and_app(
            TEST_APP_SLUG,
            vec!["alice".to_string()],
        );

        // Wire a real Messenger with the webhook: channel and TEST_APP_SLUG as subscriber.
        let messenger = {
            use brenn_lib::messaging::{
                ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry,
                SubscriberEntryKind, WEBHOOK_ADDRESS_PREFIX,
                config::{
                    Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel,
                    ResolvedMessagingConfig, ResolvedSubscription, Sink,
                },
                db::upsert_channels,
                webhook_channel_uuid_from_slug,
            };
            use indexmap::IndexMap;

            let channel_uuid = webhook_channel_uuid_from_slug(TEST_SLUG);
            let address = format!("{WEBHOOK_ADDRESS_PREFIX}{TEST_SLUG}");
            let entry = ChannelEntry {
                uuid: channel_uuid,
                address: address.clone(),
                description: None,
                resolved_channel: ResolvedChannel {
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    standing_retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    sink: Sink::Drop,
                    wake_min: brenn_lib::messaging::WakeMin::Normal,
                },
                subscribers: vec![SubscriberEntry {
                    kind: SubscriberEntryKind::App(TEST_APP_SLUG.to_string()),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: Some(brenn_lib::messaging::WakeMin::Normal),
                }],
                transport_type: ChannelScheme::Webhook,
                mount: Some(TEST_MOUNT.to_string()),
            };
            {
                let conn = db.try_lock().expect("db lock for channel upsert");
                upsert_channels(&conn, std::slice::from_ref(&entry));
            }
            let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));

            let mut app_cfg = crate::test_support::app_config::default_test_app_config(
                TEST_APP_SLUG,
                TEST_APP_SLUG,
            );
            app_cfg.allowed_users = vec!["alice".to_string()];
            // Delivery-time ACL gate (design §2.2 Point A): cover the webhook channel.
            app_cfg.policy =
                crate::test_support::app_config::delivery_policy_for_addresses([address.as_str()]);
            app_cfg.messaging = Some(ResolvedMessagingConfig {
                send_budget: 100,
                subscriptions: vec![ResolvedSubscription {
                    channel_uuid,
                    channel_address: address.clone(),
                    push_depth: Depth::Unbounded,
                    retain_depth: Depth::Unbounded,
                    noise: NoiseLevel::Silent,
                    wake_min: brenn_lib::messaging::WakeMin::Normal,
                }],
            });
            let mut apps_raw: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
            apps_raw.insert(TEST_APP_SLUG.to_string(), app_cfg);

            brenn_lib::messaging::Messenger::new(
                db.clone(),
                directory,
                Arc::from("e2e-test"),
                Arc::new(apps_raw),
                Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                    as Arc<dyn brenn_lib::messaging::WakeRouter>,
                MessagingGlobalConfig::default(),
            )
        };
        // Save a reference for draining after the request.
        let messenger_ref = Arc::clone(&messenger);
        state.messenger = Some(messenger);

        // Build the phonebuddy endpoint service (HmacRawBody, credential header =
        // "x-phonebuddy-signature", identifier header = "x-phonebuddy-key-id").
        let svc = phonebuddy_service();

        // Wire the REAL WebhookEventRouterImpl — no mock.
        // The axum router and the WebhookEventRouterImpl each need an AppState.
        // They share the same underlying Db; clone state for the axum layer.
        let (alert_dispatcher, _handle) = AlertDispatcher::noop();
        state.alert_dispatcher = alert_dispatcher;
        state.webhook = Some(svc.clone());

        // Clone for the axum router (the handler reads state.webhook to find the router).
        let axum_state = state.clone();

        let real_impl = Arc::new(WebhookEventRouterImpl::new());
        svc.set_router(
            Arc::clone(&real_impl) as Arc<dyn brenn_lib::webhook::service::WebhookEventRouter>
        );

        // Fill in state on the WebhookEventRouterImpl BEFORE any request.
        real_impl.set_state(state);

        // Build the axum router exactly like the seam tests do.
        let axum_router = Router::new()
            .route(
                TEST_MOUNT,
                axum::routing::post(receive).layer(
                    tower::ServiceBuilder::new()
                        .layer(axum::Extension(EndpointSlug(TEST_SLUG.to_string())))
                        .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024)),
                ),
            )
            .with_state(axum_state)
            .layer(axum_mw::from_fn(resolve_client_ip))
            .layer(axum::Extension(TrustedProxyHops(0)))
            .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

        // Build a properly HMAC-signed POST body.
        let body = br#"{"kind":"ping","client_id":"c1"}"#;
        let sig = sign_v1hex(body);

        let req = Request::builder()
            .method("POST")
            .uri(TEST_MOUNT)
            .header("content-type", "application/json")
            .header("x-phonebuddy-signature", &sig)
            .header("x-phonebuddy-key-id", TEST_KEY_ID)
            .body(Body::from(body.as_ref()))
            .unwrap();

        let resp = axum_router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT, "expected 204");

        // Drain pending pushes for the conversation created by resolve_push_targets.
        // The subscriber is "myapp" / user "alice" — conversation was get_or_created
        // during publish. We query the DB directly for the conversation id.
        let conversation_id: i64 = {
            let conn = db.lock().await;
            conn.query_row(
                "SELECT id FROM conversations WHERE app_slug = ?1 LIMIT 1",
                rusqlite::params![TEST_APP_SLUG],
                |r| r.get(0),
            )
            .expect("conversation must have been created by resolve_push_targets")
        };
        let subscriber = ParticipantId::for_conversation(conversation_id);

        let pushes: Vec<(i64, IngressOrBus)> = messenger_ref.load_pending_pushes(&subscriber).await;

        assert_eq!(pushes.len(), 1, "exactly one pending push for subscriber");
        let (_push_id, payload) = &pushes[0];

        // Assert the payload decoded as Bus(MessageEnvelope), not Ingress(Event).
        let envelope = match payload {
            IngressOrBus::Bus(env) => env,
            IngressOrBus::Ingress(_) => {
                panic!(
                    "expected IngressOrBus::Bus but got Ingress — webhook must not use the [Event] path"
                );
            }
        };

        // Assert channel address is the exact webhook:<slug> channel.
        assert_eq!(
            envelope.channel,
            format!("webhook:{}", TEST_SLUG),
            "channel must be webhook:<slug> with the correct slug"
        );

        // Assert rendering uses [Webhook message] heading, not [Event] / [Brenn message].
        let rendered = format_messaging_event_single(envelope);
        assert!(
            rendered.starts_with(WEBHOOK_SINGLE_HEADING),
            "rendered text must start with {WEBHOOK_SINGLE_HEADING:?}; got: {:?}",
            &rendered[..rendered.len().min(80)]
        );

        // Assert the envelope body is valid WebhookEnvelope JSON with credential
        // header masked and endpoint_slug correct.
        let wh: WebhookEnvelope = serde_json::from_str(&envelope.body)
            .expect("envelope.body must be valid WebhookEnvelope JSON");
        assert_eq!(wh.endpoint_slug, TEST_SLUG);

        // Assert key_id threading from VerifiedRequest into WebhookEnvelope.
        assert_eq!(
            wh.key_id, TEST_KEY_ID,
            "WebhookEnvelope.key_id must match the verified key"
        );

        // Assert raw body captured verbatim into WebhookEnvelope.body.
        assert_eq!(
            wh.body,
            std::str::from_utf8(body).unwrap(),
            "WebhookEnvelope.body must be the raw request body"
        );

        // Assert client_ip captured (one of the previously-dropped fields this slice fixes).
        // MockConnectInfo is 127.0.0.1:9999; the IP is extracted without port (IpAddr::to_string).
        assert_eq!(
            wh.client_ip, "127.0.0.1",
            "WebhookEnvelope.client_ip must be the MockConnectInfo IP (port stripped)"
        );

        // Assert received_at is a plausible recent timestamp (non-zero, within last 60s).
        let now = chrono::Utc::now();
        let age = now - wh.received_at;
        assert!(
            age >= chrono::Duration::zero() && age < chrono::Duration::seconds(60),
            "WebhookEnvelope.received_at must be a recent timestamp, got {:?}",
            wh.received_at
        );

        // "x-phonebuddy-signature" is the HmacRawBody credential header — must be masked.
        let sig_entry = wh
            .headers
            .iter()
            .find(|(n, _)| n == "x-phonebuddy-signature")
            .expect("x-phonebuddy-signature must be present in WebhookEnvelope.headers");
        assert_eq!(
            sig_entry.1, "[redacted]",
            "credential header value must be masked in WebhookEnvelope"
        );

        // "x-phonebuddy-key-id" is the identifier header — must survive verbatim.
        let kid_entry = wh
            .headers
            .iter()
            .find(|(n, _)| n == "x-phonebuddy-key-id")
            .expect("x-phonebuddy-key-id must be present in WebhookEnvelope.headers");
        assert_eq!(
            kid_entry.1, TEST_KEY_ID,
            "identifier header must not be masked"
        );
    }

    // -----------------------------------------------------------------------
    // §4.3 Replay-protection tests
    //
    // All tests in this section use a real `ReplayComponent` loaded against a
    // tempfile SQLite store. Tests exercise the full HTTP stack with replay
    // protection enabled on the test endpoint.
    // -----------------------------------------------------------------------

    mod replay {
        use super::*;
        use brenn_cal::ms_to_sent_at;
        use brenn_wasm::ReplayComponent;
        use tempfile::NamedTempFile;
        use tower_http::catch_panic::CatchPanicLayer;

        const REPLAY_ARTIFACT_PATH: &str = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../brenn-wasm/target/components/brenn_replay.wasm"
        );

        const FAULT_ARTIFACT_PATH: &str = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../brenn-wasm/target/components/brenn_replay_fault_test.wasm"
        );

        // ±5-minute skew window in ms, matching the component constant.
        // Used in documentation and edge-case reasoning; test helpers use wall-clock now.
        #[allow(dead_code)]
        const SKEW_WINDOW_MS: u64 = 5 * 60 * 1000;

        fn replay_artifact() -> std::path::PathBuf {
            std::path::PathBuf::from(REPLAY_ARTIFACT_PATH)
        }

        fn fault_artifact() -> std::path::PathBuf {
            std::path::PathBuf::from(FAULT_ARTIFACT_PATH)
        }

        /// Build a minimal valid phonebuddy envelope body as JSON bytes.
        fn envelope(client_id: &str, sent_at: &str, nonce: &str) -> Vec<u8> {
            format!(
                r#"{{"schema_version":"1","kind":"test","client_id":"{client_id}","sent_at":"{sent_at}","nonce":"{nonce}","seq":1,"payload":{{}}}}"#
            )
            .into_bytes()
        }

        /// Build a nonce padded to 8 characters from an integer index.
        fn nonce(i: u64) -> String {
            format!("nonce{i:04}")
        }

        /// Build the test router with replay protection enabled on the phonebuddy
        /// endpoint. Returns (Router, Arc<CapturingRouter>).
        /// The NamedTempFile passed in must be kept alive for the duration of the test.
        fn replay_test_router(db: &NamedTempFile) -> (Router, Arc<CapturingRouter>) {
            let component = Arc::new(ReplayComponent::load(
                "phonebuddy",
                &replay_artifact(),
                db.path(),
                brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
                std::collections::HashMap::new(),
            ));

            let capture = CapturingRouter::new();
            let svc = phonebuddy_service();
            let router_trait: Arc<dyn WebhookEventRouter> = Arc::clone(&capture) as _;
            svc.set_router(router_trait);

            let db2 = brenn_lib::db::init_db_memory();
            let (alert_dispatcher, _handle) = AlertDispatcher::noop();
            let mut state = crate::state::AppState::for_test(db2, None);
            state.alert_dispatcher = alert_dispatcher;
            state.webhook = Some(svc.clone());
            state.replay_components = Arc::new({
                let mut map = HashMap::new();
                map.insert(TEST_SLUG.to_string(), component);
                map
            });
            state.replay_locks = Arc::new({
                let mut map = HashMap::new();
                map.insert(TEST_SLUG.to_string(), Arc::new(tokio::sync::Mutex::new(())));
                map
            });

            let router = Router::new()
                .route(
                    TEST_MOUNT,
                    post(receive).layer(
                        tower::ServiceBuilder::new()
                            .layer(axum::Extension(EndpointSlug(TEST_SLUG.to_string())))
                            .layer(DefaultBodyLimit::max(1024 * 1024)),
                    ),
                )
                .with_state(state)
                .layer(axum_mw::from_fn(resolve_client_ip))
                .layer(axum::Extension(TrustedProxyHops(0)))
                .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

            (router, capture)
        }

        /// Like replay_test_router but also wraps with CatchPanicLayer so
        /// handler panics produce 500 rather than a connection drop.
        /// Takes a pre-built component (caller owns the NamedTempFile).
        fn replay_test_router_with_catch_panic(
            component: Arc<ReplayComponent>,
        ) -> (Router, Arc<CapturingRouter>) {
            let capture = CapturingRouter::new();
            let svc = phonebuddy_service();
            let router_trait: Arc<dyn WebhookEventRouter> = Arc::clone(&capture) as _;
            svc.set_router(router_trait);

            let db2 = brenn_lib::db::init_db_memory();
            let (alert_dispatcher, _handle) = AlertDispatcher::noop();
            let mut state = crate::state::AppState::for_test(db2, None);
            state.alert_dispatcher = alert_dispatcher;
            state.webhook = Some(svc.clone());
            state.replay_components = Arc::new({
                let mut map = HashMap::new();
                map.insert(TEST_SLUG.to_string(), component);
                map
            });
            state.replay_locks = Arc::new({
                let mut map = HashMap::new();
                map.insert(TEST_SLUG.to_string(), Arc::new(tokio::sync::Mutex::new(())));
                map
            });

            let router = Router::new()
                .route(
                    TEST_MOUNT,
                    post(receive).layer(
                        tower::ServiceBuilder::new()
                            .layer(axum::Extension(EndpointSlug(TEST_SLUG.to_string())))
                            .layer(DefaultBodyLimit::max(1024 * 1024)),
                    ),
                )
                .with_state(state)
                // CatchPanicLayer must be outside the route handler to catch panics
                // propagated via resume_unwind from spawn_blocking join errors.
                .layer(CatchPanicLayer::new())
                .layer(axum_mw::from_fn(resolve_client_ip))
                .layer(axum::Extension(TrustedProxyHops(0)))
                .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

            (router, capture)
        }

        /// POST a signed envelope to the replay-protected endpoint with the given
        /// received_at timestamp. Returns the axum Response.
        async fn post_envelope(
            router: Router,
            body: Vec<u8>,
            received_at_header: Option<&str>,
        ) -> axum::response::Response {
            let sig = sign_v1hex(&body);
            let mut builder = Request::builder()
                .method("POST")
                .uri(TEST_MOUNT)
                .header("content-type", "application/json")
                .header("x-phonebuddy-signature", &sig)
                .header("x-phonebuddy-key-id", TEST_KEY_ID);
            // received_at_header is used for tests that need to inject a custom
            // timestamp header; most tests omit it.
            if let Some(h) = received_at_header {
                builder = builder.header("x-test-received-at", h);
            }
            let req = builder.body(Body::from(body)).unwrap();
            router.oneshot(req).await.unwrap()
        }

        /// Current wall-clock ms, used as base for timestamp-sensitive tests.
        fn now_ms() -> u64 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        }

        // ── AC-accept (HTTP path) ──────────────────────────────────────────────

        /// A valid new envelope is accepted: 204 + delivery dispatched.
        #[tokio::test]
        async fn replay_accept_returns_204_and_delivers() {
            let db = NamedTempFile::new().unwrap();
            let (router, capture) = replay_test_router(&db);
            // sent_at must be within ±5 min of the handler's received_at (SystemTime::now()).
            let t = now_ms();
            let body = envelope("client1", &ms_to_sent_at(t), &nonce(1));
            let resp = post_envelope(router, body.clone(), None).await;
            assert_eq!(resp.status(), StatusCode::NO_CONTENT, "expected 204");
            let deliveries = capture.drain();
            assert_eq!(deliveries.len(), 1, "expected one delivery");
            assert_eq!(deliveries[0].raw_body, std::str::from_utf8(&body).unwrap());
        }

        // ── AC-duplicate ──────────────────────────────────────────────────────

        /// Replaying the same nonce returns 409 `{"error":"replay-duplicate"}`.
        #[tokio::test]
        async fn replay_duplicate_returns_409() {
            let db = NamedTempFile::new().unwrap();
            let (router, _capture) = replay_test_router(&db);
            let t = now_ms();
            let body1 = envelope("client1", &ms_to_sent_at(t), &nonce(1));
            let t2 = t + 1;
            let body2 = envelope("client1", &ms_to_sent_at(t2), &nonce(1)); // same nonce

            // First accept.
            let r1 = post_envelope(router.clone(), body1, None).await;
            assert_eq!(
                r1.status(),
                StatusCode::NO_CONTENT,
                "first accept must be 204"
            );

            // Replay with same nonce (monotonicity passes via later sent_at).
            let r2 = post_envelope(router, body2, None).await;
            assert_eq!(
                r2.status(),
                StatusCode::CONFLICT,
                "expected 409 for duplicate"
            );
            let bytes = axum::body::to_bytes(r2.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-duplicate"}"#);
        }

        // ── AC-skew-past ──────────────────────────────────────────────────────

        /// `sent_at` more than SKEW_WINDOW_MS before `received_at` returns 409 skew.
        #[tokio::test]
        async fn replay_skew_past_returns_409() {
            let db = NamedTempFile::new().unwrap();
            let (router, _capture) = replay_test_router(&db);
            // Use a fixed baseline far enough from epoch to be well-formed.
            let t: u64 = 1_748_000_000_000;
            // sent_at is SKEW_WINDOW_MS + 1 ms before received_at, which is t.
            // We can't control received_at from the HTTP layer (it's SystemTime::now()),
            // so we set sent_at to a time far in the past (well outside ±5 min).
            let stale_sent_at = ms_to_sent_at(1_000_000_000); // year 2001 — far past
            let body = envelope("client1", &stale_sent_at, &nonce(1));
            let _ = t; // unused; just for documentation
            let resp = post_envelope(router, body, None).await;
            assert_eq!(
                resp.status(),
                StatusCode::CONFLICT,
                "expected 409 for past skew"
            );
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-skew"}"#);
        }

        // ── AC-skew-future ────────────────────────────────────────────────────

        /// `sent_at` more than SKEW_WINDOW_MS after `received_at` returns 409 skew.
        #[tokio::test]
        async fn replay_skew_future_returns_409() {
            let db = NamedTempFile::new().unwrap();
            let (router, _capture) = replay_test_router(&db);
            // sent_at far in the future (year 2100) — well outside ±5 min.
            let future_sent_at = ms_to_sent_at(4_102_444_800_000); // 2100-01-01
            let body = envelope("client1", &future_sent_at, &nonce(1));
            let resp = post_envelope(router, body, None).await;
            assert_eq!(
                resp.status(),
                StatusCode::CONFLICT,
                "expected 409 for future skew"
            );
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-skew"}"#);
        }

        // ── AC-monotonicity ───────────────────────────────────────────────────

        /// A `sent_at` earlier than the previously accepted one returns 409 monotonicity.
        #[tokio::test]
        async fn replay_monotonicity_strict_returns_409() {
            let db = NamedTempFile::new().unwrap();
            let (router, _capture) = replay_test_router(&db);
            // Both timestamps must be within ±5 min of wall-clock now.
            // Use UNIX_EPOCH + some offset; but they need to be near now for the
            // skew check to pass. Best approach: use SystemTime::now as base.
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let t1 = now_ms;
            let t2 = now_ms + 1;
            let body1 = envelope("client1", &ms_to_sent_at(t2), &nonce(1)); // accept with t2
            let body2 = envelope("client1", &ms_to_sent_at(t1), &nonce(2)); // then send t1 < t2

            let r1 = post_envelope(router.clone(), body1, None).await;
            assert_eq!(
                r1.status(),
                StatusCode::NO_CONTENT,
                "first accept must be 204"
            );

            let r2 = post_envelope(router, body2, None).await;
            assert_eq!(
                r2.status(),
                StatusCode::CONFLICT,
                "expected 409 for monotonicity violation"
            );
            let bytes = axum::body::to_bytes(r2.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-monotonicity"}"#);
        }

        // ── AC-monotonicity-equal ─────────────────────────────────────────────

        /// Equal `sent_at` also returns 409 monotonicity (strict >).
        #[tokio::test]
        async fn replay_monotonicity_equal_returns_409() {
            let db = NamedTempFile::new().unwrap();
            let (router, _capture) = replay_test_router(&db);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let t = now_ms;
            let body1 = envelope("client1", &ms_to_sent_at(t), &nonce(1));
            let body2 = envelope("client1", &ms_to_sent_at(t), &nonce(2)); // same sent_at

            let r1 = post_envelope(router.clone(), body1, None).await;
            assert_eq!(
                r1.status(),
                StatusCode::NO_CONTENT,
                "first accept must be 204"
            );

            let r2 = post_envelope(router, body2, None).await;
            assert_eq!(
                r2.status(),
                StatusCode::CONFLICT,
                "expected 409 for equal sent_at"
            );
            let bytes = axum::body::to_bytes(r2.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-monotonicity"}"#);
        }

        // ── AC-malformed-envelope ─────────────────────────────────────────────

        /// A well-signed but malformed envelope returns 400 with fixed body.
        /// The diagnostic is NOT echoed.
        #[tokio::test]
        async fn replay_malformed_envelope_returns_400_with_fixed_body() {
            let db = NamedTempFile::new().unwrap();
            let (router, _capture) = replay_test_router(&db);
            // Missing required fields — parses as JSON but fails envelope validation.
            let body = br#"{"schema_version":"1","kind":"test"}"#.to_vec();
            let resp = post_envelope(router, body, None).await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "expected 400");
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-malformed"}"#);
        }

        // ── AC-nonce-cap-fail-closed (HTTP surface) ───────────────────────────

        /// 1025th non-expired accept from one client_id returns 429 + security event.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn replay_cap_hit_returns_429_and_emits_security_event() {
            let db = NamedTempFile::new().unwrap();
            let (router, _capture) = replay_test_router(&db);
            // Use wall-clock now as base; all 1024 accepts use sent_at within
            // ±5 min (ts_ms near now, strictly monotonic per accept).
            let base_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // Fill the cap: 1024 accepts with distinct nonces, strictly monotonic sent_at.
            for i in 0u64..1024 {
                let t = base_ms + i;
                let body = envelope("cap-client", &ms_to_sent_at(t), &nonce(i + 1));
                let resp = post_envelope(router.clone(), body, None).await;
                assert_eq!(
                    resp.status(),
                    StatusCode::NO_CONTENT,
                    "accept {i} must be 204"
                );
            }

            // 1025th non-expired accept with a fresh nonce and strictly-greater sent_at.
            let t_cap = base_ms + 1024;
            let body_cap = envelope("cap-client", &ms_to_sent_at(t_cap), &nonce(1025));
            let resp_cap = post_envelope(router, body_cap, None).await;
            assert_eq!(
                resp_cap.status(),
                StatusCode::TOO_MANY_REQUESTS,
                "expected 429 on cap-hit"
            );
            let bytes = axum::body::to_bytes(resp_cap.into_body(), 1024)
                .await
                .unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-cap-hit"}"#);

            // Cap-hit must emit security_event = true with event_type replay_cap_hit.
            assert!(
                logs_contain("security_event=true"),
                "cap-hit must emit security_event=true"
            );
            assert!(
                logs_contain("replay_cap_hit"),
                "cap-hit must emit event_type=replay_cap_hit"
            );
        }

        // ── route_replay_error unit tests (typed arm regression guards) ───────

        /// Regression guard: `MalformedInput("cap-hit")` must route to 400, not 429,
        /// and must NOT emit a ReplayCapHit security event. Guards against future
        /// re-introduction of string-content routing for cap-hit.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn route_replay_error_malformed_cap_hit_string_is_400_no_cap_hit_event() {
            let (dispatcher, _handle) = AlertDispatcher::noop();
            let ip: IpAddr = "127.0.0.1".parse().unwrap();
            let resp = route_replay_error(
                &ReplayError::MalformedInput("cap-hit".to_string()),
                "phonebuddy",
                "primary",
                ip,
                &dispatcher,
                false, // quota_hit: not a quota hit
            );
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "expected 400");
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-malformed"}"#);
            // The detail string carries the diag and is included in the security event.
            assert!(
                logs_contain("replay malformed-input"),
                "MalformedInput arm must include 'replay malformed-input' in detail"
            );
            // Typed variant for malformed must be emitted.
            assert!(
                logs_contain("event_type=replay_malformed"),
                "MalformedInput arm must emit event_type=replay_malformed"
            );
            // String content of MalformedInput must not trigger cap-hit event.
            assert!(
                !logs_contain("replay_cap_hit"),
                "MalformedInput('cap-hit') must not emit replay_cap_hit event"
            );
        }

        /// Regression guard: `TooManyRequests` typed variant must route to 429,
        /// emit ReplayCapHit security event with the expected detail string, AND
        /// fire the phone-alert side of `log_and_alert_security_event`.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn route_replay_error_cap_hit_typed_routes_to_429_and_security_event() {
            let (dispatcher, captured, handle) = make_capturing_alerter();
            let ip: IpAddr = "127.0.0.1".parse().unwrap();
            // quota_hit=false: guest CAP hit, should fire fail2ban security event.
            let resp = route_replay_error(
                &ReplayError::TooManyRequests,
                "phonebuddy",
                "primary",
                ip,
                &dispatcher,
                false, // quota_hit: guest CAP, not host quota
            );
            assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS, "expected 429");
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-cap-hit"}"#);
            assert!(
                logs_contain("security_event=true"),
                "TooManyRequests (guest CAP) must emit security_event=true"
            );
            assert!(
                logs_contain("replay_cap_hit"),
                "TooManyRequests (guest CAP) must emit event_type=replay_cap_hit"
            );
            assert!(
                logs_contain("replay cap-hit endpoint=phonebuddy key_id=primary"),
                "TooManyRequests (guest CAP) must emit expected detail string"
            );
            // Drop dispatcher so the background task drains and exits, then
            // wait for it before inspecting captured alerts.
            drop(dispatcher);
            handle.await.unwrap();
            let alerts = captured.lock().unwrap();
            assert_eq!(
                alerts.len(),
                1,
                "expected exactly one phone alert for guest cap-hit"
            );
            assert!(
                alerts[0].0.contains("replay_cap_hit"),
                "alert title must reference replay_cap_hit event type, got: {:?}",
                alerts[0].0,
            );
        }

        /// `TooManyRequests` with `quota_hit=true` must return 429 but NOT fire
        /// the `ReplayCapHit` fail2ban security event — it is a host storage-cap
        /// condition, not abuse. Operator has been alerted via the Warning path.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn route_replay_error_host_quota_429_no_fail2ban() {
            let (dispatcher, captured, handle) = make_capturing_alerter();
            let ip: IpAddr = "127.0.0.1".parse().unwrap();
            let resp = route_replay_error(
                &ReplayError::TooManyRequests,
                "phonebuddy",
                "primary",
                ip,
                &dispatcher,
                true, // quota_hit: host store quota, NOT guest CAP
            );
            assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS, "expected 429");
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-cap-hit"}"#);
            // Must NOT emit the fail2ban security event.
            assert!(
                !logs_contain("security_event=true"),
                "TooManyRequests from host quota must NOT emit security_event=true"
            );
            assert!(
                !logs_contain("replay_cap_hit"),
                "TooManyRequests from host quota must NOT emit event_type=replay_cap_hit"
            );
            drop(dispatcher);
            handle.await.unwrap();
            // No alert from route_replay_error (operator alert comes from the quota Warning path).
            let alerts = captured.lock().unwrap();
            assert_eq!(
                alerts.len(),
                0,
                "route_replay_error must not fire an alert for host-quota TooManyRequests"
            );
        }

        /// `TimestampOutOfWindow` must route to 409, emit `replay_skew` security event,
        /// and fire exactly one alert with the expected event type.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn route_replay_error_skew_routes_to_409_and_security_event() {
            let (dispatcher, captured, handle) = make_capturing_alerter();
            let ip: IpAddr = "127.0.0.1".parse().unwrap();
            let resp = route_replay_error(
                &ReplayError::TimestampOutOfWindow,
                "phonebuddy",
                "primary",
                ip,
                &dispatcher,
                false,
            );
            assert_eq!(resp.status(), StatusCode::CONFLICT, "expected 409");
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-skew"}"#);
            assert!(
                logs_contain("security_event=true"),
                "TimestampOutOfWindow must emit security_event=true"
            );
            assert!(
                logs_contain("replay_skew"),
                "TimestampOutOfWindow must emit event_type=replay_skew"
            );
            drop(dispatcher);
            handle.await.unwrap();
            let alerts = captured.lock().unwrap();
            assert_eq!(alerts.len(), 1, "expected exactly one alert for skew");
            assert!(
                alerts[0].0.contains("replay_skew"),
                "alert title must reference replay_skew, got: {:?}",
                alerts[0].0,
            );
        }

        /// `MonotonicityViolation` must route to 409, emit `replay_monotonicity` security
        /// event, and fire exactly one alert with the expected event type.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn route_replay_error_monotonicity_routes_to_409_and_security_event() {
            let (dispatcher, captured, handle) = make_capturing_alerter();
            let ip: IpAddr = "127.0.0.1".parse().unwrap();
            let resp = route_replay_error(
                &ReplayError::MonotonicityViolation,
                "phonebuddy",
                "primary",
                ip,
                &dispatcher,
                false,
            );
            assert_eq!(resp.status(), StatusCode::CONFLICT, "expected 409");
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-monotonicity"}"#);
            assert!(
                logs_contain("security_event=true"),
                "MonotonicityViolation must emit security_event=true"
            );
            assert!(
                logs_contain("replay_monotonicity"),
                "MonotonicityViolation must emit event_type=replay_monotonicity"
            );
            drop(dispatcher);
            handle.await.unwrap();
            let alerts = captured.lock().unwrap();
            assert_eq!(
                alerts.len(),
                1,
                "expected exactly one alert for monotonicity"
            );
            assert!(
                alerts[0].0.contains("replay_monotonicity"),
                "alert title must reference replay_monotonicity, got: {:?}",
                alerts[0].0,
            );
        }

        /// `MalformedInput` must route to 400, emit `replay_malformed` security event,
        /// include the diagnostic in the detail field (not the HTTP body), and fire
        /// exactly one alert.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn route_replay_error_malformed_routes_to_400_and_security_event() {
            let (dispatcher, captured, handle) = make_capturing_alerter();
            let ip: IpAddr = "127.0.0.1".parse().unwrap();
            let resp = route_replay_error(
                &ReplayError::MalformedInput("test diag".to_string()),
                "phonebuddy",
                "primary",
                ip,
                &dispatcher,
                false,
            );
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "expected 400");
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-malformed"}"#);
            // Diagnostic is in the log detail, not in the HTTP body.
            assert!(
                logs_contain("test diag"),
                "MalformedInput diagnostic must appear in log detail"
            );
            assert!(
                !String::from_utf8_lossy(bytes.as_ref()).contains("test diag"),
                "MalformedInput diagnostic must NOT appear in HTTP response body"
            );
            assert!(
                logs_contain("security_event=true"),
                "MalformedInput must emit security_event=true"
            );
            assert!(
                logs_contain("replay_malformed"),
                "MalformedInput must emit event_type=replay_malformed"
            );
            drop(dispatcher);
            handle.await.unwrap();
            let alerts = captured.lock().unwrap();
            assert_eq!(alerts.len(), 1, "expected exactly one alert for malformed");
            assert!(
                alerts[0].0.contains("replay_malformed"),
                "alert title must reference replay_malformed, got: {:?}",
                alerts[0].0,
            );
        }

        // ── AC-cap-hit-fail2ban-surfacing (emission-policy invariant) ─────────

        /// A duplicate-rejection MUST emit security_event=true and fire an alert.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn replay_duplicate_emits_security_event_and_alert() {
            let db = NamedTempFile::new().unwrap();
            let (router, _capture) = replay_test_router(&db);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let t1 = now_ms;
            let t2 = now_ms + 1;
            // First accept.
            let body1 = envelope("client1", &ms_to_sent_at(t1), &nonce(1));
            let r1 = post_envelope(router.clone(), body1, None).await;
            assert_eq!(r1.status(), StatusCode::NO_CONTENT);

            // Duplicate (same nonce, monotonicity passes via later sent_at).
            let body2 = envelope("client1", &ms_to_sent_at(t2), &nonce(1));
            let r2 = post_envelope(router, body2, None).await;
            assert_eq!(
                r2.status(),
                StatusCode::CONFLICT,
                "expected 409 for duplicate"
            );

            // Duplicate rejection MUST emit security_event=true with replay_duplicate.
            assert!(
                logs_contain("security_event=true"),
                "duplicate rejection must emit security_event=true"
            );
            assert!(
                logs_contain("replay_duplicate"),
                "duplicate rejection must emit event_type=replay_duplicate"
            );
        }

        /// `Duplicate` must fire exactly one alert with `replay_duplicate` in the title.
        /// The integration test `replay_duplicate_emits_security_event_and_alert` drives the
        /// full router (which uses `AlertDispatcher::noop`) so it cannot assert alert dispatch;
        /// this unit test covers the alert-dispatch half by calling `route_replay_error` directly
        /// with a `CapturingAlerter`, mirroring `route_replay_error_skew_*` etc.
        #[tokio::test]
        #[tracing_test::traced_test]
        async fn route_replay_error_duplicate_routes_to_409_and_security_event() {
            let (dispatcher, captured, handle) = make_capturing_alerter();
            let ip: IpAddr = "127.0.0.1".parse().unwrap();
            let resp = route_replay_error(
                &ReplayError::Duplicate,
                "phonebuddy",
                "primary",
                ip,
                &dispatcher,
                false,
            );
            assert_eq!(resp.status(), StatusCode::CONFLICT, "expected 409");
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"replay-duplicate"}"#);
            assert!(
                logs_contain("security_event=true"),
                "Duplicate must emit security_event=true"
            );
            assert!(
                logs_contain("replay_duplicate"),
                "Duplicate must emit event_type=replay_duplicate"
            );
            drop(dispatcher);
            handle.await.unwrap();
            let alerts = captured.lock().unwrap();
            assert_eq!(alerts.len(), 1, "expected exactly one alert for duplicate");
            assert!(
                alerts[0].0.contains("replay_duplicate"),
                "alert title must reference replay_duplicate, got: {:?}",
                alerts[0].0,
            );
        }

        // ── AC-header-non-utf8 ────────────────────────────────────────────────

        /// Non-UTF-8 header value on a replay-protected endpoint returns 400 schema.
        #[tokio::test]
        async fn replay_non_utf8_header_returns_400_schema() {
            let db = NamedTempFile::new().unwrap();
            let (router, _capture) = replay_test_router(&db);
            let body = envelope("client1", &ms_to_sent_at(1_748_000_000_000), &nonce(1));
            let sig = sign_v1hex(&body);
            // Inject a non-UTF-8 header value via a raw header with bytes 0xff.
            let req = Request::builder()
                .method("POST")
                .uri(TEST_MOUNT)
                .header("content-type", "application/json")
                .header("x-phonebuddy-signature", &sig)
                .header("x-phonebuddy-key-id", TEST_KEY_ID)
                .header(
                    "x-custom",
                    axum::http::HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
                )
                .body(Body::from(body))
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "expected 400 for non-UTF-8 header"
            );
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"schema"}"#);
        }

        // ── AC-unbound-endpoint-unchanged ─────────────────────────────────────

        /// An endpoint without replay protection continues to work normally.
        #[tokio::test]
        async fn unbound_endpoint_unchanged() {
            // Use the plain phonebuddy_service/test_router (no replay component).
            let capture = CapturingRouter::new();
            let svc = phonebuddy_service();
            let router = test_router(svc, Arc::clone(&capture));

            let body = br#"{"schema_version":"1","kind":"test","client_id":"c1","sent_at":"2026-01-01T00:00:00.000Z","nonce":"nonce001","seq":1,"payload":{}}"#;
            let sig = sign_v1hex(body);
            let resp = post_to_endpoint(
                router,
                body.to_vec(),
                Some("application/json"),
                Some(&sig),
                Some(TEST_KEY_ID),
            )
            .await;
            assert_eq!(
                resp.status(),
                StatusCode::NO_CONTENT,
                "unbound endpoint must return 204"
            );
            assert_eq!(
                capture.drain().len(),
                1,
                "unbound endpoint must deliver normally"
            );
        }

        // ── AC-startup-fail-loud ──────────────────────────────────────────────

        /// `ReplayComponent::load` panics if the component artifact does not exist.
        #[test]
        #[should_panic(expected = "failed to load WASM component")]
        fn startup_panics_on_missing_component_artifact() {
            let db = NamedTempFile::new().unwrap();
            ReplayComponent::load(
                "phonebuddy",
                std::path::Path::new("/nonexistent/path/to/missing.wasm"),
                db.path(),
                brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
                std::collections::HashMap::new(),
            );
        }

        /// Two `ReplayComponent` instances sharing one store path panic at startup.
        #[test]
        #[should_panic(expected = "already open")]
        fn startup_panics_on_duplicate_store_path() {
            let db = NamedTempFile::new().unwrap();
            let _c1 = ReplayComponent::load(
                "phonebuddy",
                &replay_artifact(),
                db.path(),
                brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
                std::collections::HashMap::new(),
            );
            // Second load with same store path must panic — KvStore::open's
            // process-global dedup guard fires.
            let _c2 = ReplayComponent::load(
                "phonebuddy",
                &replay_artifact(),
                db.path(),
                brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
                std::collections::HashMap::new(),
            );
        }

        // ── AC-restart-durability (HTTP path) ─────────────────────────────────

        /// SQLite persistence: accept, drop the router (drops ReplayComponent),
        /// rebuild with same store path, replay → 409.
        #[tokio::test]
        async fn replay_state_persists_across_appstate_rebuild() {
            let db = NamedTempFile::new().unwrap();
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let t1 = now_ms;
            let t2 = now_ms + 1;

            // First router: accept an envelope.
            {
                let (router, _capture) = replay_test_router(&db);
                let body = envelope("persist-client", &ms_to_sent_at(t1), &nonce(1));
                let resp = post_envelope(router, body, None).await;
                assert_eq!(
                    resp.status(),
                    StatusCode::NO_CONTENT,
                    "initial accept must be 204"
                );
            }
            // Router dropped here; ReplayComponent and its KvStore Arc dropped.
            // The KvStore deregisters the path from OPEN_PATHS.

            // Second router: rebuild against the same store path.
            {
                let (router2, _capture2) = replay_test_router(&db);
                // Replay the same nonce with a strictly-later sent_at so
                // monotonicity passes but duplicate check rejects.
                let body_replay = envelope("persist-client", &ms_to_sent_at(t2), &nonce(1));
                let resp2 = post_envelope(router2, body_replay, None).await;
                assert_eq!(
                    resp2.status(),
                    StatusCode::CONFLICT,
                    "replay after rebuild must return 409 (nonce persisted on disk)"
                );
                let bytes = axum::body::to_bytes(resp2.into_body(), 1024).await.unwrap();
                assert_eq!(bytes.as_ref(), br#"{"error":"replay-duplicate"}"#);
            }
        }

        // ── AC-trap-panics-host + §3.3 CatchPanicLayer ───────────────────────

        /// A component trap propagated via resume_unwind returns 500 (not conn drop).
        /// Uses the fault-test component with TRAP sentinel (`x-brenn-fault-test: TRAP`
        /// header) to force a wasm `unreachable` trap inside the guest.
        #[tokio::test]
        async fn component_panic_returns_500_via_catch_panic_layer() {
            let db = NamedTempFile::new().unwrap();
            // Load the fault-test component (dispatches on x-brenn-fault-test header).
            let component = Arc::new(ReplayComponent::load(
                "phonebuddy",
                &fault_artifact(),
                db.path(),
                brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
                std::collections::HashMap::new(),
            ));
            let (router, _capture) = replay_test_router_with_catch_panic(component);

            // The TRAP sentinel is keyed on the `x-brenn-fault-test: TRAP` header.
            // The body is arbitrary but must be valid UTF-8 (UTF-8 guard passes
            // because replay protection is active). Use an empty JSON object.
            let trap_body = b"{}".to_vec();
            let sig = sign_v1hex(&trap_body);
            let req = Request::builder()
                .method("POST")
                .uri(TEST_MOUNT)
                .header("content-type", "application/json")
                .header("x-phonebuddy-signature", &sig)
                .header("x-phonebuddy-key-id", TEST_KEY_ID)
                // Fault-test sentinel — causes wasm `unreachable` inside the guest.
                .header("x-brenn-fault-test", "TRAP")
                .body(Body::from(trap_body))
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "component trap must produce 500 via CatchPanicLayer"
            );
        }

        // ── AC-quota-hit-alert: handler fires Warning alert on host-quota hit ──

        /// Host-quota hit: when the store reaches its cap and a check hits SQLITE_FULL,
        /// the handler fires a Warning-severity phone alert (§2.E layer 2 / AC-7 alert half).
        ///
        /// Drives the store to cap via HTTP requests, then verifies that the handler
        /// fires a Warning alert titled "WASM store quota exceeded". The store is
        /// driven to cap through repeated POST requests (not `fill_to_cap_for_testing`).
        /// The guest's `put` hits SQLITE_FULL → QuotaExceeded → TooManyRequests (429) +
        /// quota_hit flag → handler fires alert.
        /// The alert signal is captured via `make_capturing_alerter_with_severity`.
        #[tokio::test]
        async fn quota_hit_fires_warning_alert() {
            use brenn_cal::ms_to_sent_at;
            use brenn_lib::obs::alerting::{AlertSeverity, make_capturing_alerter_with_severity};

            // 24 pages = 96 KiB. Small enough to fill quickly (the replay component
            // inserts 2 rows per request: one in last_ns, one in the nonce namespace).
            // This bypasses the config-layer floor (16 pages / 64 KiB) and passes
            // max_page_count directly to KvStore::open.
            const TINY_CAP_PAGES: u32 = 24;

            let db = NamedTempFile::new().unwrap();
            let component = Arc::new(ReplayComponent::load(
                "phonebuddy",
                &replay_artifact(),
                db.path(),
                TINY_CAP_PAGES,
                std::collections::HashMap::new(),
            ));

            let (dispatcher, captured, handle) = make_capturing_alerter_with_severity();
            // Clone the dispatcher so we can drop our copy to drain the channel later.
            // The clone and state.alert_dispatcher share the same underlying mpsc::Sender.
            let dispatcher_for_drain = dispatcher.clone();

            let capture = CapturingRouter::new();
            let svc = phonebuddy_service();
            svc.set_router(Arc::clone(&capture) as Arc<dyn WebhookEventRouter>);

            let db2 = brenn_lib::db::init_db_memory();
            let mut state = crate::state::AppState::for_test(db2, None);
            state.alert_dispatcher = dispatcher;
            state.webhook = Some(svc.clone());
            state.replay_components = Arc::new({
                let mut map = HashMap::new();
                map.insert(TEST_SLUG.to_string(), component);
                map
            });
            state.replay_locks = Arc::new({
                let mut map = HashMap::new();
                map.insert(TEST_SLUG.to_string(), Arc::new(tokio::sync::Mutex::new(())));
                map
            });

            let base_router = Router::new()
                .route(
                    TEST_MOUNT,
                    post(receive).layer(
                        tower::ServiceBuilder::new()
                            .layer(axum::Extension(EndpointSlug(TEST_SLUG.to_string())))
                            .layer(DefaultBodyLimit::max(1024 * 1024)),
                    ),
                )
                .with_state(state)
                .layer(axum_mw::from_fn(resolve_client_ip))
                .layer(axum::Extension(TrustedProxyHops(0)))
                .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

            // Send requests with unique nonces until the store reaches its cap (429).
            // Each accepted request inserts 2 rows (last_ns + nonce namespace), so with
            // 24 pages the cap is reached after a small number of requests.
            let base_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let mut quota_hit_resp = None;
            for i in 0u64..500 {
                // Use distinct client_id + nonce per iteration so each request inserts
                // new rows (no duplicate rejection) until the store fills.
                let body = envelope(
                    &format!("quota-client-{i}"),
                    &ms_to_sent_at(base_ms + i),
                    &nonce(i),
                );
                let resp = post_envelope(base_router.clone(), body, None).await;
                if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                    quota_hit_resp = Some(resp);
                    break;
                }
                assert_eq!(
                    resp.status(),
                    StatusCode::NO_CONTENT,
                    "expected 204 for request {i} before quota is reached; got {:?}",
                    resp.status()
                );
            }
            assert!(
                quota_hit_resp.is_some(),
                "expected at least one 429 (TooManyRequests) before 500 requests with \
                 TINY_CAP_PAGES={TINY_CAP_PAGES}"
            );

            // Drop base_router to release the AppState (and its AlertDispatcher Sender clone).
            // Then drop our dispatcher_for_drain clone. Once all Sender clones drop, the
            // background task exits and the handle resolves.
            drop(base_router);
            drop(dispatcher_for_drain);
            handle.await.unwrap();

            let alerts = captured.lock().unwrap();
            // Alert must be Warning-level with the expected title.
            let quota_alert = alerts.iter().find(|(sev, title, _body)| {
                matches!(sev, AlertSeverity::Warning) && title.contains("WASM store quota exceeded")
            });
            assert!(
                quota_alert.is_some(),
                "expected a Warning alert with title containing 'WASM store quota exceeded'; \
                 got {} alerts: {alerts:?}",
                alerts.len()
            );
            // Alert body must include the endpoint slug so the operator can identify
            // which store is at cap.
            let (_, _, body) = quota_alert.unwrap();
            assert!(
                body.contains(TEST_SLUG),
                "quota alert body must contain endpoint slug {TEST_SLUG:?}; got body: {body:?}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // §6 B — replay-generic component tests (HmacTimestampedBody + text/plain)
    //
    // Loads the real `brenn_replay_generic.wasm` component. Each test uses a
    // distinct NamedTempFile store path (OPEN_PATHS process-global guard).
    // The endpoint uses:
    //   content_type = "text/plain"
    //   scheme = HmacTimestampedBody, sig_header = "x-brenn-push-signature",
    //   sig_format = V1Hex, timestamp_header = "x-brenn-push-timestamp",
    //   template = "{t}.{body}" (prefix="", mid=".", suffix="", t_before_body=true)
    //   max_skew_secs = 300, key_id_header = None
    // -----------------------------------------------------------------------
    mod replay_generic {
        use super::*;
        use brenn_wasm::ReplayComponent;
        use tempfile::NamedTempFile;

        const GENERIC_ARTIFACT_PATH: &str = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../brenn-wasm/target/components/brenn_replay_generic.wasm"
        );
        /// Slug and mount for the push-test endpoint in host-seam tests.
        const PUSH_SLUG: &str = "push-test";
        const PUSH_MOUNT: &str = "/webhooks/push-test";
        const PUSH_APP_SLUG: &str = "push-test-app";

        fn generic_artifact() -> std::path::PathBuf {
            std::path::PathBuf::from(GENERIC_ARTIFACT_PATH)
        }

        /// Build the `HmacTimestampedBody` + `text/plain` endpoint matching the §4 TOML recipe.
        /// Template = "{t}.{body}" → prefix="", mid=".", suffix="", t_before_body=true.
        fn push_endpoint() -> Arc<ResolvedWebhookEndpoint> {
            let mut keys = HashMap::new();
            keys.insert(TEST_KEY_ID.to_string(), TEST_SECRET.to_vec());
            Arc::new(ResolvedWebhookEndpoint {
                slug: PUSH_SLUG.to_string(),
                mount: PUSH_MOUNT.to_string(),
                description: None,
                transport_ceiling_bytes: 1024 * 1024,
                content_type: "text/plain".to_string(),
                scheme: SignatureScheme::HmacTimestampedBody {
                    algorithm: SignatureAlgorithm::HmacSha256,
                    sig_header: "x-brenn-push-signature".parse().unwrap(),
                    sig_format: HexFormat::V1Hex,
                    timestamp_header: "x-brenn-push-timestamp".parse().unwrap(),
                    template_prefix: String::new(),
                    template_mid: ".".to_string(),
                    template_suffix: String::new(),
                    t_before_body: true,
                    max_skew_secs: 300,
                    key_id_header: None,
                    keys,
                },
                owner: brenn_lib::webhook::config::WebhookOwner::App(Arc::from(PUSH_APP_SLUG)),
                urgency: brenn_lib::messaging::Urgency::Normal,
                replay_protection: None, // set via replay_components map in state
            })
        }

        /// Build a signed (t_str, sig) pair for the push endpoint.
        /// Canonical form: `t_str || "." || body` (matches {t}.{body} template).
        /// Timestamp is current unix seconds unless `override_t_secs` is given.
        fn sign_push(body: &[u8], override_t_secs: Option<i64>) -> (String, String) {
            let t_secs = override_t_secs.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64
            });
            let t_str = t_secs.to_string();
            let mut canonical = t_str.as_bytes().to_vec();
            canonical.push(b'.');
            canonical.extend_from_slice(body);
            let hex = brenn_lib::webhook::signature::hmac_sha256_hex(TEST_SECRET, &canonical);
            let sig = format!("v1={hex}");
            (t_str, sig)
        }

        /// Config map for the push-test endpoint: injects `brenn.max-skew-secs`
        /// matching `push_endpoint()`'s `max_skew_secs: 300`.
        fn push_replay_config() -> std::collections::HashMap<String, String> {
            let mut m = std::collections::HashMap::new();
            m.insert("brenn.max-skew-secs".to_string(), "300".to_string());
            m
        }

        /// Build a test router for the push-test endpoint with the generic replay component.
        fn generic_replay_router(db: &NamedTempFile) -> (Router, Arc<CapturingRouter>) {
            let component = Arc::new(ReplayComponent::load(
                "push-test",
                &generic_artifact(),
                db.path(),
                brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
                push_replay_config(),
            ));
            let capture = CapturingRouter::new();
            let svc = WebhookService::new(vec![(PUSH_SLUG.to_string(), push_endpoint())]);
            svc.set_router(Arc::clone(&capture) as Arc<dyn WebhookEventRouter>);

            let db2 = brenn_lib::db::init_db_memory();
            let (alert_dispatcher, _handle) = AlertDispatcher::noop();
            let mut state = crate::state::AppState::for_test(db2, None);
            state.alert_dispatcher = alert_dispatcher;
            state.webhook = Some(svc.clone());
            state.replay_components = Arc::new({
                let mut map = HashMap::new();
                map.insert(PUSH_SLUG.to_string(), component);
                map
            });
            state.replay_locks = Arc::new({
                let mut map = HashMap::new();
                map.insert(PUSH_SLUG.to_string(), Arc::new(tokio::sync::Mutex::new(())));
                map
            });

            let router = Router::new()
                .route(
                    PUSH_MOUNT,
                    post(receive).layer(
                        tower::ServiceBuilder::new()
                            .layer(axum::Extension(EndpointSlug(PUSH_SLUG.to_string())))
                            .layer(DefaultBodyLimit::max(1024 * 1024)),
                    ),
                )
                .with_state(state)
                .layer(axum_mw::from_fn(resolve_client_ip))
                .layer(axum::Extension(TrustedProxyHops(0)))
                .layer(MockConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9999))));

            (router, capture)
        }

        /// POST a plain-text push to the generic-replay-protected endpoint.
        async fn post_push(
            router: Router,
            body: &[u8],
            t_secs: Option<i64>,
        ) -> axum::response::Response {
            let (t_str, sig) = sign_push(body, t_secs);
            let req = Request::builder()
                .method("POST")
                .uri(PUSH_MOUNT)
                .header("content-type", "text/plain")
                .header("x-brenn-push-timestamp", &t_str)
                .header("x-brenn-push-signature", &sig)
                .body(Body::from(body.to_vec()))
                .unwrap();
            router.oneshot(req).await.unwrap()
        }

        /// POST with an explicit pre-built (t_str, sig) pair (for verbatim-replay tests).
        async fn post_push_with_sig(
            router: Router,
            body: &[u8],
            t_str: &str,
            sig: &str,
            extra_header: Option<(&str, &str)>,
        ) -> axum::response::Response {
            let mut builder = Request::builder()
                .method("POST")
                .uri(PUSH_MOUNT)
                .header("content-type", "text/plain")
                .header("x-brenn-push-timestamp", t_str)
                .header("x-brenn-push-signature", sig);
            if let Some((name, value)) = extra_header {
                builder = builder.header(name, value);
            }
            let req = builder.body(Body::from(body.to_vec())).unwrap();
            router.oneshot(req).await.unwrap()
        }

        // ── Fresh push → 204 ─────────────────────────────────────────────────

        /// A fresh plain-text push returns 204 and delivers the body verbatim (design §6 B).
        #[tokio::test]
        async fn generic_fresh_push_returns_204_and_delivers() {
            let db = NamedTempFile::new().unwrap();
            let (router, capture) = generic_replay_router(&db);
            let body = b"hello from cron";
            let resp = post_push(router, body, None).await;
            assert_eq!(resp.status(), StatusCode::NO_CONTENT, "expected 204");
            let deliveries = capture.drain();
            assert_eq!(deliveries.len(), 1, "expected one delivery");
            assert_eq!(deliveries[0].raw_body, "hello from cron");
        }

        // ── Verbatim replay → 409 (acceptance 2a) ────────────────────────────

        /// Verbatim replay of the same (t, body, signature) triple returns 409 duplicate.
        /// Uses two routers against the same store to simulate a captured-request resend.
        #[tokio::test]
        async fn generic_verbatim_replay_returns_409() {
            let db = NamedTempFile::new().unwrap();
            let body = b"captured message";
            let (t_str, sig) = sign_push(body, None);

            // First send: accept.
            {
                let (router1, _) = generic_replay_router(&db);
                let resp = post_push_with_sig(router1, body, &t_str, &sig, None).await;
                assert_eq!(
                    resp.status(),
                    StatusCode::NO_CONTENT,
                    "first send must be 204"
                );
            }
            // router1 dropped; KvStore deregisters from OPEN_PATHS.

            // Second send: verbatim replay → 409.
            {
                let (router2, _) = generic_replay_router(&db);
                let resp = post_push_with_sig(router2, body, &t_str, &sig, None).await;
                assert_eq!(
                    resp.status(),
                    StatusCode::CONFLICT,
                    "verbatim replay must return 409"
                );
                let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
                assert_eq!(bytes.as_ref(), br#"{"error":"replay-duplicate"}"#);
            }
        }

        // ── Swapped-unsigned-input replay → 409 (acceptance 2b) ─────────────

        /// Adding an extra unsigned header to a captured (t, body, sig) triple does NOT
        /// produce a fresh dedup identity — the resend is still 409 duplicate (design §2.3,
        /// B6). The dedup key is the signature, which is unchanged.
        #[tokio::test]
        async fn generic_swapped_unsigned_input_replay_returns_409() {
            let db = NamedTempFile::new().unwrap();
            let body = b"captured for swap test";
            let (t_str, sig) = sign_push(body, None);

            // First send: accept (no extra header).
            {
                let (router1, _) = generic_replay_router(&db);
                let resp = post_push_with_sig(router1, body, &t_str, &sig, None).await;
                assert_eq!(
                    resp.status(),
                    StatusCode::NO_CONTENT,
                    "first send must be 204"
                );
            }

            // Replay with an extra unsigned header — same (t, body, sig) → same dedup key.
            {
                let (router2, _) = generic_replay_router(&db);
                let resp = post_push_with_sig(
                    router2,
                    body,
                    &t_str,
                    &sig,
                    Some(("x-extra-unsigned", "new-value")),
                )
                .await;
                assert_eq!(
                    resp.status(),
                    StatusCode::CONFLICT,
                    "swapped-unsigned-input replay must still return 409"
                );
                let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
                assert_eq!(bytes.as_ref(), br#"{"error":"replay-duplicate"}"#);
            }
        }

        // ── Missing signature header → MalformedInput (design §3.2 step 1 guard) ──

        /// Defensive guard: component returns MalformedInput when the signature header
        /// is absent from CheckInput. This path is unreachable via real requests
        /// (signature layer 401s first), but the guard's correctness is verified here
        /// at the component level (design §6 B, fourth bullet; scope-3).
        #[test]
        fn generic_missing_sig_header_returns_malformed_input() {
            let db = NamedTempFile::new().unwrap();
            let component = ReplayComponent::load(
                "push-test",
                &generic_artifact(),
                db.path(),
                brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
                push_replay_config(),
            );
            let input = CheckInput {
                headers: vec![
                    // Signature header intentionally absent; only timestamp present.
                    Header {
                        name: "x-brenn-push-timestamp".to_string(),
                        value: "1749200000".to_string(),
                    },
                ],
                body: b"some body".to_vec(),
                received_at: 1_749_200_000_000,
                key_id: String::new(),
                endpoint_slug: "push-test".to_string(),
            };
            let (result, _quota_hit) = component.check(&input);
            assert!(
                matches!(result, Err(ReplayError::MalformedInput(_))),
                "missing sig header must return MalformedInput; got: {result:?}"
            );
        }

        // ── Arbitrary / non-JSON body (design §6 B, fifth bullet; acceptance 5) ──

        /// Component treats the body as opaque: non-JSON, multi-byte UTF-8 body
        /// does not affect the dedup identity (acceptance 5: never assumes JSON).
        /// The design §6 B example `b"\x01plain text"` is valid UTF-8 (control char).
        #[tokio::test]
        async fn generic_non_json_body_accepted() {
            let db = NamedTempFile::new().unwrap();
            let (router, _) = generic_replay_router(&db);
            // Valid UTF-8 non-JSON body: control char prefix + multibyte chars.
            // \x01 is a valid UTF-8 byte (U+0001 START OF HEADING).
            let body: &[u8] = b"\x01plain text \xe2\x80\x94 an em-dash";
            let resp = post_push(router, body, None).await;
            assert_eq!(
                resp.status(),
                StatusCode::NO_CONTENT,
                "non-JSON body must be accepted (204); got {:?}",
                resp.status()
            );
        }

        /// Empty body → 204 on fresh send (design §1 verified: server accepts empty body;
        /// design §6 B fifth bullet explicitly requests this case).
        #[tokio::test]
        async fn generic_empty_body_accepted() {
            let db = NamedTempFile::new().unwrap();
            let (router, _) = generic_replay_router(&db);
            let resp = post_push(router, b"", None).await;
            assert_eq!(
                resp.status(),
                StatusCode::NO_CONTENT,
                "empty body must be accepted (204); got {:?}",
                resp.status()
            );
        }

        // ── Stale timestamp → 401 (signature-layer, before component) ────────

        /// A stale timestamp (outside max_skew_secs=300) returns 401 at the signature
        /// layer before the component runs — acceptance 2 stale-timestamp sub-case.
        #[tokio::test]
        async fn generic_stale_timestamp_returns_401() {
            let db = NamedTempFile::new().unwrap();
            let (router, _) = generic_replay_router(&db);
            // 600 seconds ago — outside the 300s window.
            let stale_t = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - 600;
            let body = b"stale push";
            let resp = post_push(router, body, Some(stale_t)).await;
            assert_eq!(
                resp.status(),
                StatusCode::UNAUTHORIZED,
                "stale timestamp must return 401"
            );
            let bytes = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
            assert_eq!(bytes.as_ref(), br#"{"error":"auth"}"#);
        }
    }
}

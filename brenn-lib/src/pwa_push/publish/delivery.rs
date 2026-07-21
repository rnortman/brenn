//! Single-subscription delivery and publish-wide fan-out collection.
//!
//! Holds `PwaPushService::deliver_to_subscription` (the per-subscription
//! encrypt + POST), the `DeliveryOutcome` enum it returns, the
//! `collect_with_publish_cap` fan-out drainer, and the publish-wide cap
//! constant/helpers. Split out of `publish/mod.rs` per design §2.1.

use std::collections::HashMap;
use std::time::Duration;

use base64ct::{Base64UrlUnpadded, Encoding as _};
use http::header::{HeaderName, HeaderValue};
use uuid::Uuid;
use web_push_native::p256::PublicKey;
use web_push_native::{Auth, WebPushBuilder};

use crate::obs::security::log_and_alert_ssrf_attempt;
use crate::pwa_push::db::SubscriptionRow;
use crate::pwa_push::endpoint_validator::{RejectReason, validate_endpoint};
use crate::pwa_push::payload::PushPayload;
use crate::pwa_push::vapid::build_vapid_authorization;

use super::{PwaPushService, Urgency};

/// Publish-wide fanout timeout. All spawned delivery tasks must complete
/// within this duration; tasks still in flight are aborted and counted as
/// `failed`. Must be greater than the per-POST timeout (8s) for
/// defense-in-depth.
const PUBLISH_WIDE_CAP: Duration = Duration::from_secs(10);

/// Returns the effective publish-wide cap.
///
/// In tests, can be overridden via `PUBLISH_WIDE_CAP_OVERRIDE` to exercise
/// timeout behavior without waiting the full 10 s.
pub(in crate::pwa_push) fn publish_wide_cap() -> Duration {
    #[cfg(test)]
    {
        let ms = PUBLISH_WIDE_CAP_OVERRIDE_MS.load(std::sync::atomic::Ordering::Relaxed);
        if ms != u64::MAX {
            return Duration::from_millis(ms);
        }
    }
    PUBLISH_WIDE_CAP
}

/// Override for `publish_wide_cap()` in tests, stored as milliseconds.
/// `u64::MAX` means "not overridden". Each test sets atomically; no
/// first-writer-wins problem unlike `OnceLock`.
#[cfg(test)]
pub(in crate::pwa_push) static PUBLISH_WIDE_CAP_OVERRIDE_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(u64::MAX);

impl PwaPushService {
    /// Attempt to deliver a single push to one subscription.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::pwa_push) async fn deliver_to_subscription(
        &self,
        sub: &SubscriptionRow,
        title: &str,
        body: &str,
        tag: Option<&str>,
        data: &Option<serde_json::Map<String, serde_json::Value>>,
        ttl: u32,
        urgency: Urgency,
        topic: Option<&str>,
    ) -> DeliveryOutcome {
        // Defense-in-depth: re-validate the endpoint before opening any
        // outbound connection. Guards against rows persisted before this
        // validation existed and against DNS-rebinding attacks (see module
        // doc in endpoint_validator.rs).
        //
        // Delivery is server-initiated, so there is no meaningful client IP.
        // IpAddr::from([0,0,0,0]) is the sentinel for server-originated events;
        // the actual context is in `detail` below.
        if let Err(reason) = validate_endpoint(&sub.endpoint, &self.config.endpoint_policy) {
            let endpoint_preview = crate::pwa_push::endpoint_preview(&sub.endpoint);
            let detail = format!(
                "delivery-time endpoint reject: reason={reason} \
                 endpoint_prefix={endpoint_preview} \
                 subscription_id={} user_id={} device_id={}",
                sub.id, sub.user_id, sub.device_id,
            );
            // Delivery is server-originated; there is no client IP. Use 127.0.0.2 as
            // a clearly-non-routable sentinel distinct from 0.0.0.0 (which fail2ban
            // could misinterpret as a wildcard interface ban target).
            log_and_alert_ssrf_attempt(
                &self.alert_dispatcher,
                std::net::IpAddr::from([127u8, 0, 0, 2]),
                &detail,
            );
            return DeliveryOutcome::InvalidEndpoint(reason);
        }

        // Decode subscription keys.
        let p256dh_bytes = match Base64UrlUnpadded::decode_vec(&sub.p256dh_b64url) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    subscription_id = sub.id,
                    "PushSend: failed to decode p256dh from DB: {e}"
                );
                return DeliveryOutcome::Failed("p256dh decode error".to_string());
            }
        };
        let auth_bytes = match Base64UrlUnpadded::decode_vec(&sub.auth_b64url) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    subscription_id = sub.id,
                    "PushSend: failed to decode auth from DB: {e}"
                );
                return DeliveryOutcome::Failed("auth decode error".to_string());
            }
        };

        let ua_public = match PublicKey::from_sec1_bytes(&p256dh_bytes) {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!(
                    subscription_id = sub.id,
                    "PushSend: failed to parse p256dh public key: {e}"
                );
                return DeliveryOutcome::Failed("p256dh key parse error".to_string());
            }
        };

        if auth_bytes.len() != 16 {
            tracing::warn!(
                subscription_id = sub.id,
                auth_len = auth_bytes.len(),
                "PushSend: auth bytes wrong length (expected 16)"
            );
            return DeliveryOutcome::Failed("auth length error".to_string());
        }
        let ua_auth = Auth::clone_from_slice(&auth_bytes);

        let endpoint: http::Uri = match sub.endpoint.parse() {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(
                    subscription_id = sub.id,
                    "PushSend: endpoint unparseable as URI: {e}"
                );
                return DeliveryOutcome::Failed("endpoint URI parse error".to_string());
            }
        };

        // Build the payload with the actual user_id for this subscription.
        // Clone data here (once per actual delivery attempt) rather than at spawn time.
        let payload = PushPayload {
            title: title.to_string(),
            body: body.to_string(),
            icon: None,
            badge: None,
            tag: tag.map(|s| s.to_string()),
            data: data.clone(),
            user_id: sub.user_id,
        };
        let payload_bytes =
            serde_json::to_vec(&payload).expect("PushPayload serialization is infallible");

        // Build the web push request.
        let valid_duration = Duration::from_secs(u64::from(ttl));
        let builder = WebPushBuilder::new(endpoint.clone(), ua_public, ua_auth)
            .with_valid_duration(valid_duration);

        let mut http_req = match builder.build(payload_bytes) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    subscription_id = sub.id,
                    "PushSend: web-push-native build failed: {e}"
                );
                return DeliveryOutcome::Failed(format!("build error: {e}"));
            }
        };

        // Inject VAPID Authorization header (RFC 8292 ES256 JWT).
        // web-push-native's build() without vapid omits the Authorization header;
        // we build it in-tree with p256/ecdsa and inject it here.
        let auth_value = build_vapid_authorization(
            &self.config.vapid,
            &endpoint,
            &self.config.subject,
            valid_duration,
        );
        http_req.headers_mut().insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&auth_value)
                .expect("VAPID Authorization header value must be valid ASCII"),
        );

        // Inject Urgency and Topic headers (web-push-native doesn't expose them).
        if urgency != Urgency::Normal {
            http_req.headers_mut().insert(
                HeaderName::from_static("urgency"),
                HeaderValue::from_str(urgency.as_str())
                    .expect("urgency header value is always valid ASCII"),
            );
        }
        if let Some(t) = topic {
            match HeaderValue::from_str(t) {
                Ok(v) => {
                    http_req
                        .headers_mut()
                        .insert(HeaderName::from_static("topic"), v);
                }
                Err(e) => {
                    // The topic was pre-validated by the MCP intercept layer
                    // (URL-safe base64 chars, ≤32 chars). Reaching this branch
                    // is an invariant violation — the only valid chars produce
                    // valid header values.
                    panic!(
                        "PushSend: topic header value invalid despite pre-validation: {e}; topic={topic:?}"
                    );
                }
            }
        }

        // Convert to reqwest::Request and POST.
        let reqwest_req = match reqwest::Request::try_from(http_req) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    subscription_id = sub.id,
                    "PushSend: failed to convert http::Request to reqwest::Request: {e}"
                );
                return DeliveryOutcome::Failed(format!("request conversion error: {e}"));
            }
        };

        let timeout_duration = Duration::from_secs(8);
        let result =
            tokio::time::timeout(timeout_duration, self.http_client.execute(reqwest_req)).await;

        let ep_preview = crate::pwa_push::endpoint_preview(&sub.endpoint);
        match result {
            Err(_elapsed) => {
                tracing::warn!(
                    subscription_id = sub.id,
                    endpoint = %ep_preview,
                    "PushSend: per-endpoint timeout (8s)"
                );
                DeliveryOutcome::Failed("timeout".to_string())
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    subscription_id = sub.id,
                    endpoint = %ep_preview,
                    "PushSend: HTTP error: {e}"
                );
                DeliveryOutcome::Failed(format!("HTTP client error: {e}"))
            }
            Ok(Ok(resp)) => {
                let status = resp.status();
                tracing::debug!(
                    subscription_id = sub.id,
                    status = status.as_u16(),
                    "PushSend: push service response"
                );
                match status.as_u16() {
                    201 => DeliveryOutcome::Delivered,
                    410 | 404 => {
                        tracing::info!(
                            subscription_id = sub.id,
                            status = status.as_u16(),
                            endpoint = %ep_preview,
                            "PushSend: subscription gone; deleting row"
                        );
                        DeliveryOutcome::Gone
                    }
                    413 => {
                        // Should not happen with our 3993-byte cap. Log at error.
                        tracing::error!(
                            subscription_id = sub.id,
                            "PushSend: push service returned 413 (payload too large) \
                             — off-by-one in size accounting?"
                        );
                        DeliveryOutcome::Failed("413 payload too large".to_string())
                    }
                    other => {
                        tracing::warn!(
                            subscription_id = sub.id,
                            status = other,
                            endpoint = %ep_preview,
                            "PushSend: push service error; subscription retained"
                        );
                        DeliveryOutcome::Failed(format!("push service status {other}"))
                    }
                }
            }
        }
    }
}

/// Outcome of a single `deliver_to_subscription` call.
#[derive(Debug)]
pub(in crate::pwa_push) enum DeliveryOutcome {
    /// 201 received.
    Delivered,
    /// 410 or 404 received — subscription is gone, delete row.
    Gone,
    /// Any other failure (4xx/5xx/network/timeout).
    Failed(String),
    /// Endpoint failed SSRF validation — subscription row must be deleted and a
    /// security event fired. Treated as permanent (not retried).
    InvalidEndpoint(RejectReason),
}

/// Drain a `JoinSet<(i64, DeliveryOutcome)>` with a publish-wide timeout cap.
///
/// On normal completion (all tasks finish before `cap`), returns the collected
/// outcomes. On timeout, calls `abort_all`, drains remaining tasks (completed
/// tasks contribute real outcomes; aborted tasks contribute
/// `Failed("publish-wide timeout")`), emits one `warn!` with the aborted
/// count, and returns the accumulated outcomes.
///
/// Policy table for `JoinError`:
/// - `is_panic()` → `Failed("task panic")`; warn-log with `subscription_id`
///   and panic payload.
/// - `is_cancelled()` → `Failed("publish-wide timeout")`.
/// - any other → panic (unreachable; indicates a tokio invariant change).
pub(in crate::pwa_push) async fn collect_with_publish_cap(
    join_set: &mut tokio::task::JoinSet<(i64, DeliveryOutcome)>,
    id_to_sub: &HashMap<tokio::task::Id, i64>,
    message_uuid: Uuid,
    cap: Duration,
) -> Vec<(i64, DeliveryOutcome)> {
    /// Apply the JoinError policy table and push the outcome.
    fn handle_join_error(
        e: tokio::task::JoinError,
        id_to_sub: &HashMap<tokio::task::Id, i64>,
        outcomes: &mut Vec<(i64, DeliveryOutcome)>,
    ) {
        let task_id = e.id();
        let sub_id = *id_to_sub
            .get(&task_id)
            .expect("JoinError id missing from id_to_sub — tokio invariant violated");
        if e.is_panic() {
            let payload = e.into_panic();
            let msg = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(|s| s.as_str()))
                .unwrap_or("<non-string panic payload>");
            tracing::warn!(
                subscription_id = sub_id,
                panic_payload = msg,
                "PushSend: delivery task panicked; counting as failed"
            );
            outcomes.push((sub_id, DeliveryOutcome::Failed("task panic".to_string())));
        } else if e.is_cancelled() {
            outcomes.push((
                sub_id,
                DeliveryOutcome::Failed("publish-wide timeout".to_string()),
            ));
        } else {
            panic!("unexpected JoinError variant (not panic, not cancelled): {e:?}");
        }
    }

    let mut outcomes: Vec<(i64, DeliveryOutcome)> = Vec::new();

    let drain_result = tokio::time::timeout(cap, async {
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((sub_id, outcome)) => outcomes.push((sub_id, outcome)),
                Err(e) => handle_join_error(e, id_to_sub, &mut outcomes),
            }
        }
    })
    .await;

    if drain_result.is_err() {
        // Outer cap fired: abort remaining tasks, then drain.
        join_set.abort_all();
        // Count tasks aborted by the cap. Includes both cancelled tasks and any
        // that panic during the post-abort drain window; individual panic warn-logs
        // are also emitted by handle_join_error for each panic case.
        let mut aborted_count = 0usize;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((sub_id, outcome)) => outcomes.push((sub_id, outcome)),
                Err(e) => {
                    aborted_count += 1;
                    handle_join_error(e, id_to_sub, &mut outcomes);
                }
            }
        }
        tracing::warn!(
            %message_uuid,
            aborted_count,
            "PushSend: publish-wide cap fired; aborted in-flight deliveries"
        );
    }

    outcomes
}

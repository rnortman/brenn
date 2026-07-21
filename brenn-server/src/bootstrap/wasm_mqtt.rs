//! Constructor for the WASM MQTT-egress publish callback.
//!
//! This is the brenn-lib-free seam the WASM host calls for `mqtt:publish`. It
//! closes over the consumer's `AppPolicy`, the consumer slug (the
//! connector-namespace key), and the (optional) `MqttService`, owns the
//! `mqtt:<client>:<topic>` parse, and bridges the async `enforce_and_publish`
//! into the synchronous `MqttPublishFn`. `brenn-wasm` never sees a `brenn-lib`
//! type — the closure maps `MqttEgressError` into the `brenn-wasm`-local
//! `MqttPublishOutcome`.

use std::sync::Arc;

use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_lib::mqtt::MqttService;
use brenn_lib::obs::alerting::AlertDispatcher;
use brenn_lib::obs::security::{
    DenialKind, DenialOrigin, SecurityEventType, signal_publish_denial,
};

/// Build the synchronous MQTT-egress callback for a WASM consumer holding the
/// `Mqtt` grant.
///
/// `svc` is `Some` whenever at least one `[[mqtt_client]]` is *referenced* — by an
/// ingress channel or by any `mqtt_publish`/`mqtt_subscribe` ACL matcher (see
/// `bootstrap/mqtt::referenced_clients`); otherwise it is `None`, in which case the
/// closure returns `NoConnector` — fail-closed. A consumer's own `mqtt_publish`
/// matcher itself brings the service up.
///
/// The returned closure bridges async → sync via `block_on`, which is correct
/// **only** because the WASM guest invocation runs on a `spawn_blocking`
/// (blocking-pool) thread — see `brenn/src/wasm_dispatch` — not a tokio async
/// worker; `block_on` panics on a worker thread but is supported on a
/// blocking-pool thread. This coupling to `spawn_blocking` is structural — the
/// closure is linked only into the guest-host dispatch path — and is enforced by
/// `block_on`'s own runtime panic rather than an explicit assert. Callers
/// (including tests) must invoke the returned fn from a blocking-pool thread.
pub(crate) fn make_wasm_mqtt_publish_fn(
    policy: brenn_lib::access::AppPolicy,
    slug: String,
    svc: Option<Arc<MqttService>>,
    alerts: AlertDispatcher,
) -> brenn_wasm::MqttPublishFn {
    Arc::new(
        move |client: String,
              topic: String,
              payload: Vec<u8>,
              content_type: Option<String>,
              qos: u8,
              retain: bool|
              -> brenn_wasm::MqttPublishOutcome {
            use brenn_lib::mqtt::egress::{MqttEgressError, SendBudget, enforce_and_publish};
            // Input validation owned by the caller: build and parse
            // `mqtt:<client>:<topic>` (rejects wildcards in publish
            // context). A parse failure is `InvalidPayload`.
            let raw = format!("mqtt:{client}:{topic}");
            let addr = match brenn_lib::mqtt::address::parse_topic_name(&raw) {
                Ok(a) => a,
                Err(e) => {
                    return brenn_wasm::MqttPublishOutcome::InvalidPayload(e.to_string());
                }
            };
            // No MQTT service configured on this server ⇒ fail-closed. This arm is
            // reachable only when no `[[mqtt_client]]` is referenced by any ingress
            // channel or ACL matcher (see `referenced_clients`); a consumer's own
            // `mqtt_publish` matcher would itself bring the service up, so a
            // consumer reaching here holds the `mqtt` grant with no publish matcher,
            // or the server runs no MQTT at all. Same guest variant as
            // `do_mqtt_publish`'s service-absent arm.
            let Some(ref svc) = svc else {
                tracing::warn!(
                    slug = %slug,
                    "wasm mqtt-publish: no-connector — MQTT service not configured on this server (no [[mqtt_client]] referenced by any ingress channel or ACL matcher)"
                );
                return brenn_wasm::MqttPublishOutcome::NoConnector;
            };
            // Bridge async → sync via `block_on` — safe only on a
            // blocking-pool thread; see this fn's doc for the full
            // constraint.
            //
            // WASM has no conversation budget; its per-activation quota is enforced
            // upstream at the host-fn boundary. That quota bounds one activation
            // only. Cross-activation pacing — which bounds a self-echo/runaway loop
            // where a consumer republishes into a bridge filter it also subscribes
            // to — lives upstream at the activation gate in `brenn/src/wasm_dispatch`
            // (`ActivationPacer`), not here: one activation-level knob paces every
            // egress path this component has (MQTT, bus, webhooks), so the host fn
            // itself intentionally stays limiter-free (mqtt-wasm-republish-pacing
            // design §1; docs/security-posture.md §8.1).
            let result = tokio::runtime::Handle::current().block_on(enforce_and_publish(
                svc,
                &policy,
                &addr,
                payload,
                content_type,
                qos,
                retain,
                SendBudget::None,
            ));
            match result {
                Ok(()) => brenn_wasm::MqttPublishOutcome::Ok,
                Err(MqttEgressError::AclDenied { .. }) => {
                    // An operator policy actively blocked a WASM egress publish
                    // to a client outside the consumer's `mqtt_publish` ACL — a
                    // security-relevant event, not guest-input error. Signal it
                    // the same way the LLM caller does: an app/component-
                    // attributed security-log line (every occurrence) plus a
                    // once-per-process phone alert via the shared helper. `kind` is
                    // always `acl_denied` here: the `Mqtt` grant gates whether this
                    // callback is even linked, so a no-grant guest cannot reach this
                    // path. `client` is guest-supplied — the helper sanitizes it; the
                    // trusted `mqtt:` prefix makes the `address=mqtt:<client>` detail
                    // self-describing at the ACL's per-client granularity.
                    signal_publish_denial(
                        &alerts,
                        SecurityEventType::MqttPublishDenied,
                        DenialOrigin::Component { slug: &slug },
                        DenialKind::AclDenied,
                        &format!("mqtt:{client}"),
                    );
                    brenn_wasm::MqttPublishOutcome::NotPermitted
                }
                // Structurally impossible: the WASM path always
                // passes `SendBudget::None`, for which
                // `enforce_and_publish` compiles out the budget
                // gate. Reaching this arm is a host wiring/invariant
                // violation (e.g. a future refactor passing a
                // budget-gating variant on the WASM path), NOT a
                // guest-input error — so panic per the project
                // posture ("panic if anything unexpected happens").
                // The `MqttPublishFn` no-panic contract governs
                // guest-supplied inputs; a host invariant break must
                // crash loudly rather than masquerade as a transient
                // `broker` error the guest may retry forever.
                Err(MqttEgressError::BudgetExhausted) => {
                    unreachable!(
                        "BudgetExhausted returned on the WASM MQTT path \
                             (SendBudget::None): enforce_and_publish contract \
                             violation — the budget gate must never fire when no \
                             budget is supplied"
                    )
                }
                Err(MqttEgressError::Broker(e)) => {
                    // The guest (untrusted out-of-tree) gets only a stable
                    // coarse kind — the full `Display` embeds host-internal
                    // topology (connector/app slugs, raw rumqttc last-error
                    // text) it was never granted. The full detail lands
                    // host-side here, where the on-call engineer needs it. The
                    // `Display` carries broker-derived external text, so it is
                    // sanitized per the log-integrity posture even though it is
                    // not guest-supplied; `client` is guest-supplied.
                    let safe_client = sanitize_untrusted_str(&client, MAX_LOGGED_UNTRUSTED_BYTES);
                    tracing::warn!(
                        slug = %slug,
                        client = %safe_client,
                        detail = %sanitize_untrusted_str(&e.to_string(), MAX_LOGGED_UNTRUSTED_BYTES),
                        "wasm mqtt-publish: broker error (guest sees coarse kind only)"
                    );
                    brenn_wasm::MqttPublishOutcome::Broker(e.coarse_kind().to_string())
                }
                Err(MqttEgressError::BrokerRejected { reason }) => {
                    brenn_wasm::MqttPublishOutcome::BrokerRejected(reason)
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_lib::access::acl::{AclSet, MqttClientMatcher};
    use brenn_lib::access::{AppCapability, AppPolicy, GrantSet};
    use brenn_lib::mqtt::state::MqttClientHandle;
    use brenn_lib::obs::alerting::{make_capturing_alerter, noop_alert_dispatcher};
    use brenn_wasm::MqttPublishOutcome;

    /// A no-op `AlertDispatcher` for the tests that do not assert on the denial
    /// signal. The drainer handle is dropped — it exits when the last clone
    /// drops (tokio semantics).
    fn noop_dispatcher() -> AlertDispatcher {
        noop_alert_dispatcher().0
    }

    /// A policy granting `MqttPublish` with a `mqtt_publish` matcher for each of
    /// `clients`. Hand-rolled literal-field construction because
    /// `AppPolicy::with_grants` is `#[cfg(test)]`-gated inside `brenn-lib` and so
    /// invisible to the `brenn` crate.
    fn policy_allowing(clients: &[&str]) -> AppPolicy {
        let mut grants = GrantSet::default();
        grants.insert(AppCapability::MqttPublish);
        let mut acls = AclSet::default();
        for c in clients {
            acls.mqtt_publish.push(MqttClientMatcher {
                client: (*c).to_string(),
            });
        }
        AppPolicy {
            grants,
            acls,
            tool_grants: Default::default(),
        }
    }

    /// An `MqttService` holding a single (disconnected) session for `client`. The
    /// session has no live `AsyncClient`, so any publish that reaches the broker
    /// layer fails with `NotConnected`.
    async fn service_with_client(client: &str) -> Arc<MqttService> {
        let svc = MqttService::new();
        let (tx, _rx) = tokio::sync::watch::channel(false);
        let config = Arc::new(crate::test_support::mqtt::test_client_config(client));
        let handle = MqttClientHandle::new(config, vec![], tx);
        svc.add_client(handle).await;
        svc
    }

    /// Invoke the callback from a blocking-pool thread — required because it
    /// `block_on`s and panics on a tokio async worker (see the fn's doc).
    async fn call(f: &brenn_wasm::MqttPublishFn, client: &str, topic: &str) -> MqttPublishOutcome {
        let f = f.clone();
        let client = client.to_string();
        let topic = topic.to_string();
        tokio::task::spawn_blocking(move || f(client, topic, b"x".to_vec(), None, 1, false))
            .await
            .expect("spawn_blocking join failed")
    }

    /// A wildcard in a publish topic is rejected by `parse_topic_name` before the
    /// service is consulted (service is `None` here, which would also surface as
    /// `NoConnector` if the parse arm were skipped).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_address_maps_to_invalid_payload() {
        let f = make_wasm_mqtt_publish_fn(
            policy_allowing(&["home"]),
            "slug".to_string(),
            None,
            noop_dispatcher(),
        );
        let out = call(&f, "home", "cmd/#").await;
        assert!(
            matches!(out, MqttPublishOutcome::InvalidPayload(_)),
            "expected InvalidPayload, got {out:?}"
        );
    }

    /// `svc: None` (no MQTT service configured on the server) ⇒ the fail-closed
    /// infrastructure arm returns `NoConnector`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_service_maps_to_no_connector() {
        let f = make_wasm_mqtt_publish_fn(
            policy_allowing(&["home"]),
            "slug".to_string(),
            None,
            noop_dispatcher(),
        );
        let out = call(&f, "home", "cmd/light").await;
        assert!(
            matches!(out, MqttPublishOutcome::NoConnector),
            "expected NoConnector, got {out:?}"
        );
    }

    /// Client not covered by the policy's `mqtt_publish` ACL ⇒ `NotPermitted`
    /// (the ACL deny short-circuits before connector resolution).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_not_in_policy_maps_to_not_permitted() {
        let svc = service_with_client("home").await;
        let f = make_wasm_mqtt_publish_fn(
            policy_allowing(&["home"]),
            "slug".to_string(),
            Some(svc),
            noop_dispatcher(),
        );
        let out = call(&f, "office", "cmd/light").await;
        assert!(
            matches!(out, MqttPublishOutcome::NotPermitted),
            "expected NotPermitted, got {out:?}"
        );
    }

    /// Fully authorized with a resolvable-but-disconnected session ⇒ reaches the
    /// broker layer and surfaces `Broker(...)` (proves ACL + session lookup passed).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolved_disconnected_session_maps_to_broker() {
        let svc = service_with_client("home").await;
        let f = make_wasm_mqtt_publish_fn(
            policy_allowing(&["home"]),
            "slug".to_string(),
            Some(svc),
            noop_dispatcher(),
        );
        let out = call(&f, "home", "cmd/light").await;
        // The guest receives only the coarse kind, never the leaky `Display`:
        // a disconnected connector surfaces `NotConnected`, whose coarse kind is
        // `"not connected"`. Pinning the exact payload guards the no-leak
        // contract — the connector/app/client slugs must not appear.
        match out {
            MqttPublishOutcome::Broker(kind) => {
                // Exact equality pins the no-leak contract: the coarse kind
                // carries no connector/app/client slug and no `last_error` text.
                // (The thorough leak sweep lives in `error.rs::coarse_kind_*`.)
                assert_eq!(kind, "not connected", "guest must see the coarse kind only");
            }
            other => panic!("expected Broker, got {other:?}"),
        }
    }

    /// An ACL-denied WASM egress publish emits one deduped phone alert —
    /// two denials to the same client fire a single `(origin, slug, kind)` alert
    /// (dedup key `component:{slug}:{kind}`). A
    /// non-denial outcome (a permitted publish reaching the broker layer) signals
    /// nothing. (The alert is captured off-thread via the drainer; the
    /// per-occurrence security-log line is emitted from a `spawn_blocking`
    /// thread outside any traced span, so it is not asserted here.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acl_denied_signals_once_others_dont() {
        let (dispatcher, captured, handle) = make_capturing_alerter();
        let svc = service_with_client("home").await;
        // ACL allows `home`; publish to `office` (not in the ACL) is denied.
        let f = make_wasm_mqtt_publish_fn(
            policy_allowing(&["home"]),
            "slug".to_string(),
            Some(svc.clone()),
            dispatcher.clone(),
        );
        // Two denials to the same client: the security log fires per occurrence,
        // the phone alert only once per (origin, slug, kind) — here
        // `component:{slug}:{kind}`.
        assert!(matches!(
            call(&f, "office", "cmd/light").await,
            MqttPublishOutcome::NotPermitted
        ));
        assert!(matches!(
            call(&f, "office", "cmd/light").await,
            MqttPublishOutcome::NotPermitted
        ));
        // A permitted publish reaches the broker layer (disconnected session →
        // Broker) and must NOT signal (not a policy denial).
        assert!(matches!(
            call(&f, "home", "cmd/light").await,
            MqttPublishOutcome::Broker(_)
        ));

        drop(f);
        drop(dispatcher);
        handle.await.expect("alert drainer join");
        let alerts = captured.lock().unwrap();
        assert_eq!(
            alerts.len(),
            1,
            "exactly one deduped alert for the ACL denial: {alerts:?}"
        );
        assert_eq!(alerts[0].0, "Security: mqtt_publish_denied");
        assert!(
            alerts[0].1.contains("kind=acl_denied") && alerts[0].1.contains("address=mqtt:office"),
            "alert body must carry the sanitized kind + address: {}",
            alerts[0].1
        );
    }
}

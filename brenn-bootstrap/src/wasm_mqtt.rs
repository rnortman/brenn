//! Constructor for the WASM MQTT-egress publisher.
//!
//! This is the brenn-lib-free seam the WASM host calls for `mqtt:publish`. It
//! closes over the consumer's `AppPolicy`, the consumer slug (the
//! connector-namespace key), and the `MqttService`, owns the
//! `mqtt:<client>:<topic>` parse, and bridges the async `enforce_and_publish`
//! into the synchronous `MqttPublisher`. `brenn-wasm` never sees a `brenn-lib`
//! type — the publisher maps `MqttEgressError` into the `brenn-wasm`-local
//! `MqttPublishOutcome`.

use std::sync::Arc;

use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_mqtt::MqttService;
use brenn_obs::alerting::AlertDispatcher;
use brenn_obs::security::{DenialKind, DenialOrigin, SecurityEventType, signal_publish_denial};

/// What a guest is told of a publish larger than the broker's maximum packet
/// size: no broker-derived number.
const PACKET_TOO_LARGE_TEXT: &str = "the packet exceeds the broker's maximum packet size";

// `PUBLISH_WAIT` bounds one call; the wait budget is spent by accumulated
// time per client. One call that times out spends the whole budget only while
// the budget is no larger than the bound.
const _: () = assert!(
    brenn_wasm::PROCESSOR_MQTT_WAIT_BUDGET.as_millis() <= brenn_mqtt::PUBLISH_WAIT.as_millis()
);

/// Build the synchronous MQTT-egress publisher for a WASM consumer holding the
/// `Mqtt` grant.
///
/// A matcher's client is validated as declared and a declared client has a
/// broker session registered before anything holding that matcher runs, so a
/// consumer with a publish matcher always finds one.
///
/// `publish` bridges async → sync via `block_on`, which is correct **only**
/// because the WASM guest invocation runs on a `spawn_blocking` (blocking-pool)
/// thread — see the `brenn-wasm-dispatch` crate — not a tokio async worker;
/// `block_on` panics on a worker thread but is supported on a blocking-pool
/// thread. This coupling to `spawn_blocking` is structural — the publisher is
/// linked only into the guest-host dispatch path — and is enforced by
/// `block_on`'s own runtime panic rather than an explicit assert. Callers
/// (including tests) must invoke `publish` from a blocking-pool thread.
pub(crate) fn make_wasm_mqtt_publish_fn(
    policy: brenn_lib::access::AppPolicy,
    slug: String,
    svc: Arc<MqttService>,
    alerts: AlertDispatcher,
) -> brenn_wasm::MqttPublishFn {
    Arc::new(WasmMqttPublisher {
        policy,
        slug,
        svc,
        alerts,
    })
}

/// The in-tree [`brenn_wasm::MqttPublisher`]: the `mqtt:<client>:<topic>`
/// parse, and the shared enforcement pipeline under the consumer's policy.
struct WasmMqttPublisher {
    policy: brenn_lib::access::AppPolicy,
    /// The consumer's slug, the connector-namespace key.
    slug: String,
    svc: Arc<MqttService>,
    alerts: AlertDispatcher,
}

impl brenn_wasm::MqttPublisher for WasmMqttPublisher {
    /// Build and parse `mqtt:<client>:<topic>`, rejecting wildcards in publish
    /// context. The parse splits at the first `:`, so `("home:a", "b")` and
    /// `("home", "a:b")` are one target.
    fn resolve(&self, client: &str, topic: &str) -> Result<brenn_wasm::MqttTarget, String> {
        let raw = format!("mqtt:{client}:{topic}");
        brenn_lib::mqtt::address::parse_topic_name(&raw)
            .map(|addr| brenn_wasm::MqttTarget {
                client: addr.client,
                topic: addr.topic,
            })
            .map_err(|e| e.to_string())
    }

    fn publish(
        &self,
        target: brenn_wasm::MqttTarget,
        payload: Vec<u8>,
        props: brenn_wasm::MqttPublishProps,
    ) -> brenn_wasm::MqttPublishOutcome {
        use brenn_mqtt::egress::{MqttEgressError, SendBudget, enforce_and_publish};
        let Self {
            policy,
            slug,
            svc,
            alerts,
        } = self;
        let addr = brenn_lib::mqtt::address::MqttAddress {
            client: target.client,
            topic: target.topic,
        };
        let client = &addr.client;
        // `block_on` is safe only on a blocking-pool thread; see
        // `make_wasm_mqtt_publish_fn`'s doc.
        //
        // No limiter here by design: pacing belongs at the activation level,
        // which covers every egress path a component has, so this fn relies on
        // the caller to bound call volume.
        let brenn_wasm::MqttPublishProps {
            content_type,
            qos,
            retain,
            message_expiry_s,
        } = props;
        let props = brenn_mqtt::PublishProps {
            content_type,
            qos,
            retain,
            message_expiry_s,
        };
        let result = tokio::runtime::Handle::current().block_on(enforce_and_publish(
            svc,
            policy,
            &addr,
            payload,
            props,
            SendBudget::None,
        ));
        match result {
            Ok(()) => brenn_wasm::MqttPublishOutcome::Ok,
            Err(MqttEgressError::AclDenied { .. }) => {
                // An ACL denial is a security event, not a guest-input error.
                // `kind` is always `acl_denied`: without the `Mqtt` grant this
                // callback is never linked. `client` is guest-derived and must
                // be sanitized by `signal_publish_denial`.
                signal_publish_denial(
                    alerts,
                    SecurityEventType::MqttPublishDenied,
                    DenialOrigin::Component { slug },
                    DenialKind::AclDenied,
                    &format!("mqtt:{client}"),
                );
                brenn_wasm::MqttPublishOutcome::NotPermitted
            }
            // Unreachable: this path always passes `SendBudget::None`. Reaching
            // it is a host invariant violation, not guest input, so it panics
            // rather than surface as a `broker` error the guest may retry.
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
                let safe_client = sanitize_untrusted_str(client, MAX_LOGGED_UNTRUSTED_BYTES);
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
            Err(MqttEgressError::PacketTooLarge {
                packet_bytes,
                broker_max,
            }) => {
                // The guest gets a fixed text: the broker's maximum is
                // broker-derived detail it was never granted. The numbers
                // land host-side. `invalid-payload` tells it not to retry.
                let safe_client = sanitize_untrusted_str(client, MAX_LOGGED_UNTRUSTED_BYTES);
                tracing::warn!(
                    slug = %slug,
                    client = %safe_client,
                    packet_bytes,
                    broker_max,
                    "wasm mqtt-publish: packet exceeds the broker's maximum packet size"
                );
                brenn_wasm::MqttPublishOutcome::InvalidPayload(PACKET_TOO_LARGE_TEXT.to_string())
            }
            // Structurally impossible on this path, unlike the LLM one: a
            // consumer whose ACL names a client is retired — task joined —
            // before a reload's commit stops any session, and the planner
            // refuses a matcher on an undeclared client, so no live
            // consumer can hold an authority the registry has moved past.
            // A miss here means a consumer outlived its retirement.
            Err(MqttEgressError::ClientNotRegistered { client }) => {
                unreachable!(
                    "wasm mqtt-publish: no session registered for client {client:?} while \
                         consumer {slug:?} is live — a consumer naming a client is retired \
                         before any session is stopped (host bug)"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_envelope::grants::AppCapability;
    use brenn_lib::access::acl::{AclSet, MqttClientMatcher};
    use brenn_lib::access::{AppPolicy, GrantSet};
    use brenn_mqtt::state::MqttClientHandle;
    use brenn_obs::alerting::{make_capturing_alerter, noop_alert_dispatcher};
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
        let config = Arc::new(brenn_server::test_support::mqtt::test_client_config(client));
        let handle = MqttClientHandle::new(config, vec![], tx);
        svc.add_client(handle);
        svc
    }

    /// Resolve and publish `payload` from a blocking-pool thread, as
    /// `do_mqtt_publish` does — required because `publish` `block_on`s and
    /// panics on a tokio async worker (see the fn's doc). A `resolve` error is
    /// the `invalid-payload` the host would return.
    async fn call_with(
        f: &brenn_wasm::MqttPublishFn,
        client: &str,
        topic: &str,
        payload: Vec<u8>,
        props: brenn_wasm::MqttPublishProps,
    ) -> MqttPublishOutcome {
        let target = match f.resolve(client, topic) {
            Ok(target) => target,
            Err(reason) => return MqttPublishOutcome::InvalidPayload(reason),
        };
        let f = f.clone();
        tokio::task::spawn_blocking(move || f.publish(target, payload, props))
            .await
            .expect("spawn_blocking join failed")
    }

    /// [`call_with`] a one-byte QoS 1 publish.
    async fn call(f: &brenn_wasm::MqttPublishFn, client: &str, topic: &str) -> MqttPublishOutcome {
        let props = brenn_wasm::MqttPublishProps {
            qos: 1,
            ..Default::default()
        };
        call_with(f, client, topic, b"x".to_vec(), props).await
    }

    /// The parse splits the joined address at its first `:`, so the same
    /// publish spelled two ways is one target — the key the host charges per
    /// client. An address outside the grammar is refused.
    #[tokio::test]
    async fn resolve_returns_the_session_the_publish_reaches() {
        let f = make_wasm_mqtt_publish_fn(
            policy_allowing(&["home"]),
            "slug".to_string(),
            MqttService::new(),
            noop_dispatcher(),
        );
        let expected = brenn_wasm::MqttTarget {
            client: "home".to_string(),
            topic: "a:b".to_string(),
        };
        assert_eq!(f.resolve("home", "a:b"), Ok(expected.clone()));
        assert_eq!(f.resolve("home:a", "b"), Ok(expected));
        assert!(
            f.resolve("ho me", "t").is_err(),
            "client outside the slug grammar"
        );
        assert!(
            f.resolve("home", "cmd/#").is_err(),
            "wildcard in a publish topic"
        );
    }

    /// A wildcard in a publish topic is rejected by `parse_topic_name` before the
    /// service is consulted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_address_maps_to_invalid_payload() {
        let f = make_wasm_mqtt_publish_fn(
            policy_allowing(&["home"]),
            "slug".to_string(),
            MqttService::new(),
            noop_dispatcher(),
        );
        let out = call(&f, "home", "cmd/#").await;
        assert!(
            matches!(out, MqttPublishOutcome::InvalidPayload(_)),
            "expected InvalidPayload, got {out:?}"
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
            svc,
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
            svc,
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

    /// A publish larger than the broker's advertised maximum is
    /// `InvalidPayload` with the fixed text: the guest learns that it will
    /// never fit and nothing of the broker's numbers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn packet_over_the_broker_maximum_maps_to_invalid_payload() {
        let svc = service_with_client("home").await;
        let handle = svc.get_client("home").expect("registered above");
        let _eventloop = brenn_mqtt::test_support::install_unpolled_client(&handle, Some(8)).await;
        let f = make_wasm_mqtt_publish_fn(
            policy_allowing(&["home"]),
            "slug".to_string(),
            svc,
            noop_dispatcher(),
        );
        let out = call(&f, "home", "cmd/light").await;
        match out {
            MqttPublishOutcome::InvalidPayload(text) => assert_eq!(text, PACKET_TOO_LARGE_TEXT),
            other => panic!("expected InvalidPayload, got {other:?}"),
        }
    }

    /// `outbound_payload_cap_bytes` bounds `MqttSend` alone: a component
    /// publish larger than it is submitted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_outbound_bound_is_not_on_the_component_path() {
        let svc = MqttService::new();
        let (tx, _rx) = tokio::sync::watch::channel(false);
        let mut config = brenn_server::test_support::mqtt::test_client_config("home");
        config.identity.outbound_payload_cap_bytes = 8;
        let handle = MqttClientHandle::new(Arc::new(config), vec![], tx);
        let _eventloop = brenn_mqtt::test_support::install_unpolled_client(&handle, None).await;
        svc.add_client(handle);
        let f = make_wasm_mqtt_publish_fn(
            policy_allowing(&["home"]),
            "slug".to_string(),
            svc,
            noop_dispatcher(),
        );
        let out = call_with(
            &f,
            "home",
            "cmd/light",
            vec![0x5a; 64],
            brenn_wasm::MqttPublishProps::default(),
        )
        .await;
        assert!(
            matches!(out, MqttPublishOutcome::Ok),
            "expected Ok, got {out:?}"
        );
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
            svc.clone(),
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

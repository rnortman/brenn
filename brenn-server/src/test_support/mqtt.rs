//! Shared MQTT test fixtures for the brenn binary crate.
//!
//! `#[cfg(test)]` items are invisible cross-crate, so brenn-lib has its own
//! equivalent (`brenn_lib::mqtt::test_support`); this one serves the binary
//! crate's tests.

use std::sync::Arc;

use brenn_lib::messaging::Urgency;
use brenn_lib::mqtt::config::{MqttClientConfig, MqttClientIdentity, TlsVersionMin};

/// A resolved client config pointing at an unroutable localhost port, so any
/// background supervisor an activation test spawns gets an immediate
/// connection-refused and sits in its backoff loop without touching a real
/// network. Callers mutate fields before `Arc::new` when a test needs a
/// variation (e.g. a non-default `urgency`/`qos`).
pub fn test_client_config(slug: &str) -> MqttClientConfig {
    MqttClientConfig {
        identity: MqttClientIdentity {
            slug: slug.to_string(),
            host: "127.0.0.1".to_string(),
            port: 1,
            username: None,
            tls_version_min: TlsVersionMin::Tls12,
            keepalive_secs: None,
            inbound_payload_cap_bytes: 4096,
            outbound_payload_cap_bytes: 4 * 1024 * 1024,
            last_will: None,
            reconnect_backoff_initial_secs: 1,
            reconnect_backoff_max_secs: 60,
            qos: 1,
            urgency: Urgency::Normal,
            session_expiry_secs: 0,
            stop_wait_secs: 0,
        },
        password: None,
        ca_cert_pem: None,
    }
}

/// Register on `svc` a session for `slug`, built from [`test_client_config`]
/// with an outbound bound of `outbound_payload_cap_bytes`. The session has no
/// broker connection.
pub fn register_test_client(
    svc: &brenn_mqtt::MqttService,
    slug: &str,
    outbound_payload_cap_bytes: u32,
) {
    let mut config = test_client_config(slug);
    config.identity.outbound_payload_cap_bytes = outbound_payload_cap_bytes;
    let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
    svc.add_client(brenn_mqtt::MqttClientHandle::new(
        Arc::new(config),
        vec![],
        stop_tx,
    ));
}

/// Run the `mqtt:` runtime subscribe the `MessageSubscribe` tool runs — the
/// ACL gate, the configured-client guard, the durable row, the route and the
/// broker SUBSCRIBE, in that order — over a messenger, MQTT service and
/// router the caller booted, and return the status word the tool would
/// answer. Panics if the subscribe is refused.
pub async fn subscribe_dynamic(
    messenger: &brenn_messaging::Messenger,
    mqtt_svc: &brenn_mqtt::MqttService,
    router: &crate::mqtt_router::MqttEventRouterImpl,
    app_slug: &str,
    address: &str,
    params: brenn_messaging::subscribe::DynamicSubscribeParams,
) -> &'static str {
    crate::mqtt_subscribe::activate_dynamic_subscribe(
        messenger, mqtt_svc, router, app_slug, address, params,
    )
    .await
    .unwrap_or_else(|e| panic!("subscribe_dynamic: {app_slug:?} on {address:?} refused: {e}"))
    .status_str()
}

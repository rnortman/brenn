//! Shared MQTT test fixtures for the brenn binary crate.
//!
//! `#[cfg(test)]` items are invisible cross-crate, so brenn-lib has its own
//! equivalent (`brenn_lib::mqtt::test_support`); this one serves the binary
//! crate's tests.

use brenn_lib::messaging::Urgency;
use brenn_lib::mqtt::config::{MqttClientConfig, TlsVersionMin};

/// A resolved client config pointing at an unroutable localhost port, so any
/// background supervisor an activation test spawns gets an immediate
/// connection-refused and sits in its backoff loop without touching a real
/// network. Callers mutate fields before `Arc::new` when a test needs a
/// variation (e.g. a non-default `urgency`/`qos`).
pub(crate) fn test_client_config(slug: &str) -> MqttClientConfig {
    MqttClientConfig {
        slug: slug.to_string(),
        host: "127.0.0.1".to_string(),
        port: 1,
        username: None,
        password: None,
        ca_cert_pem: None,
        tls_version_min: TlsVersionMin::Tls12,
        keepalive_secs: None,
        inbound_payload_cap_bytes: 4096,
        last_will: None,
        reconnect_backoff_initial_secs: 1,
        reconnect_backoff_max_secs: 60,
        qos: 1,
        urgency: Urgency::Normal,
        session_expiry_secs: 0,
    }
}

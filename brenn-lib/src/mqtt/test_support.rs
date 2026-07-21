//! Shared test fixtures for the mqtt module tree.
//!
//! `#[cfg(test)]` items are invisible cross-crate, so this helper serves only
//! brenn-lib's own unit tests (the binary crate and the integration tests have
//! their own equivalents).

use crate::messaging::Urgency;
use crate::mqtt::config::{MqttClientConfig, TlsVersionMin};

/// Minimal resolved client config for unit tests. Fields the mqtt paths do not
/// read in a given test are filled with defaults; callers mutate fields before
/// `Arc::new` when a test needs a variation.
///
/// `port` is the unroutable `1` (matching the brenn-crate twin): should a future
/// brenn-lib unit test ever build a handle from this fixture and spawn a
/// supervisor, it gets an immediate connection-refused instead of silently
/// dialing a real mosquitto on the dev machine's default 1883. Integration tests
/// that need a live broker use their own fixture (`tests/common/mod.rs`), which
/// takes an explicit port.
pub(crate) fn test_client_config(slug: &str) -> MqttClientConfig {
    MqttClientConfig {
        slug: slug.to_string(),
        host: "127.0.0.1".to_string(),
        port: 1,
        username: None,
        password: None,
        ca_cert_pem: None,
        tls_version_min: TlsVersionMin::Tls12,
        keepalive_secs: Some(30),
        inbound_payload_cap_bytes: 4 * 1024 * 1024,
        last_will: None,
        reconnect_backoff_initial_secs: 1,
        reconnect_backoff_max_secs: 60,
        qos: 1,
        urgency: Urgency::Normal,
        session_expiry_secs: 0,
    }
}

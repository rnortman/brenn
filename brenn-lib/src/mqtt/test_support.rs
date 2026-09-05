//! Shared test fixtures for the mqtt config types.
//!
//! Behind `testutils` rather than `#[cfg(test)]`: `brenn-mqtt`'s tests build
//! sessions from the same resolved client config, and a `#[cfg(test)]` item is
//! invisible cross-crate. The release configs clear the feature.

use crate::messaging::Urgency;
use crate::mqtt::config::{MqttClientConfig, MqttClientIdentity, TlsVersionMin};

/// Minimal resolved client config for unit tests. Fields the mqtt paths do not
/// read in a given test are filled with defaults; callers mutate fields before
/// `Arc::new` when a test needs a variation.
///
/// `port` is the unroutable `1` (matching the brenn-server twin): a test that
/// builds a handle from this fixture and spawns a supervisor gets an immediate
/// connection-refused instead of silently dialing a real mosquitto on the dev
/// machine's default 1883. Integration tests that need a live broker use their
/// own fixture (`brenn-mqtt/tests/common/mod.rs`), which takes an explicit port.
pub fn test_client_config(slug: &str) -> MqttClientConfig {
    MqttClientConfig {
        identity: test_client_identity(slug),
        password: None,
        ca_cert_pem: None,
    }
}

/// The non-secret half of [`test_client_config`], for the passes that take an
/// identity map rather than a resolved-client map.
pub fn test_client_identity(slug: &str) -> MqttClientIdentity {
    MqttClientIdentity {
        slug: slug.to_string(),
        host: "127.0.0.1".to_string(),
        port: 1,
        username: None,
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

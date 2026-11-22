//! MQTT addressing and configuration.
//!
//! This is the half of the MQTT transport that code below the transport reads:
//! `config` deserializes the `[[mqtt_client]]` blocks the configuration
//! aggregate holds, and `access`, `config::resolve` and `messaging` parse and
//! validate MQTT addresses. The transport runtime — sessions, supervisors,
//! payload classification, egress enforcement — lives in `brenn-mqtt`, one
//! crate above.
//!
//! # Module layout
//!
//! - `config`  — raw + resolved config types; `resolve_clients`.
//! - `address` — `MqttAddress` + `parse_mqtt_address` / `parse_topic_filter` / `parse_topic_name`.
//! - `error`   — `MqttError` with LLM-facing `Display` strings; shared by both halves.
//! - `outbound` — `MqttPublishOptions` and `MqttClientDirectory`, what a bus
//!   publish to an MQTT topic is admitted against.
//! - [`PUBLISH_WAIT`] — the publish bound `brenn-mqtt` enforces and the
//!   conversation's tool text states.

pub mod address;
pub mod config;
pub mod error;
pub mod outbound;
#[cfg(any(test, feature = "testutils"))]
pub mod test_support;

pub use address::{MqttAddress, parse_mqtt_address, parse_topic_filter, parse_topic_name};
pub use error::MqttError;
pub use outbound::{MQTT_OUTBOUND_DEFAULT_LIFETIME, MqttClientDirectory, MqttPublishOptions};

/// The longest one publish waits on its MQTT session, measured from entry: for
/// the client lock, for room on the request channel at QoS 0, and for the
/// broker's acknowledgement at QoS 1/2. Enforced by `brenn-mqtt`'s
/// `publish_on_handle`; stated to conversations in the `MqttSend` description.
pub const PUBLISH_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

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

pub mod address;
pub mod config;
pub mod error;
#[cfg(any(test, feature = "testutils"))]
pub mod test_support;

pub use address::{MqttAddress, parse_mqtt_address, parse_topic_filter, parse_topic_name};
pub use error::MqttError;

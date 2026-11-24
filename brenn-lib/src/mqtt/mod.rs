//! MQTT addressing and configuration.
//!
//! This is the half of the MQTT transport that code below the transport reads:
//! `config` deserializes the `[[mqtt_client]]` blocks the configuration
//! aggregate holds, and `access`, `config::resolve` and `messaging` parse and
//! validate MQTT addresses. `access` and the inbound router match topic filters
//! through `topic_filter`. The transport runtime — sessions, supervisors,
//! payload classification, the egress reader — lives in `brenn-mqtt`, one
//! crate above.
//!
//! # Module layout
//!
//! - `config`  — raw + resolved config types; `resolve_clients`.
//! - `address` — `MqttAddress` + `parse_mqtt_address` / `parse_topic_filter` / `parse_topic_name`
//!   / `topic_name_has_wildcard`.
//! - `error`   — `MqttError` with LLM-facing `Display` strings; shared by both halves.
//! - `outbound` — `MqttPublishOptions`, `MqttLifetime` and `MqttClientDirectory`, what a bus
//!   publish to an MQTT topic is admitted against.
//! - `topic_filter` — `covers`, the one MQTT filter-against-filter match, `$` first-level rule
//!   included; decides the subscribe ACL, the publish ACL and inbound routing.
//! - `subscription_id` — `subscription_identifier`, the MQTT 5 Subscription Identifier a
//!   filter is subscribed with, derived from the filter string.

pub mod address;
pub mod config;
pub mod error;
pub mod outbound;
pub mod subscription_id;
#[cfg(any(test, feature = "testutils"))]
pub mod test_support;
pub mod topic_filter;

pub use address::{MqttAddress, parse_mqtt_address, parse_topic_filter, parse_topic_name};
pub use error::MqttError;
pub use outbound::{MqttClientDirectory, MqttLifetime, MqttPublishOptions};
pub use subscription_id::{MAX_SUBSCRIPTION_IDENTIFIER, subscription_identifier};

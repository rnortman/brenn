//! MQTT transport for Brenn.
//!
//! The `MqttSend` virtual tool lets Claude-Code-driven apps publish to
//! operator-configured MQTT brokers. MQTT ingress discovery + health is served
//! by the transport-agnostic `MessageChannelList`, and ad-hoc retained reads by
//! `MessageSubscribe` (pull-only) + `MessageChannelGet` — the old
//! `MqttSubscriptionList` and `MqttGetRetained` tools were removed.
//!
//! # Module layout
//!
//! - `config`     — raw + resolved config types; `resolve_clients`.
//! - `address`    — `MqttAddress` + `parse_mqtt_address` / `parse_topic_filter` / `parse_topic_name`.
//! - `error`      — `MqttError` with LLM-facing `Display` strings.
//! - `payload`    — `InboundPayload` / `OutboundPayload`; `classify_inbound` / `decode_outbound_body`.
//! - `state`      — `MqttClientHandle`, `SupervisorState`, `IngressSubscription`.
//! - `egress`     — shared capability/ACL/budget enforcement + broker publish (`enforce_and_publish`).
//! - `service`    — `MqttService`, `MqttEventRouter` trait.
//! - `connection` — unified per-client supervisor (`spawn_client_supervisor`) plus
//!   subscription helpers (`union_subscriptions`, `assert_ingress_subscription`).

pub mod address;
pub mod config;
pub mod connection;
pub mod egress;
pub mod error;
pub mod payload;
pub mod service;
pub mod state;
#[cfg(test)]
pub(crate) mod test_support;

pub use address::{MqttAddress, parse_mqtt_address, parse_topic_filter, parse_topic_name};
pub use connection::{spawn_client_supervisor, union_subscriptions};
pub use egress::{MqttEgressError, SendBudget, enforce_and_publish};
pub use error::MqttError;
pub use payload::{InboundPayload, OutboundPayload, classify_inbound, decode_outbound_body};
pub use service::{MqttEventRouter, MqttService};
pub use state::{ConnectorHealthLabel, IngressSubscription, MqttClientHandle};

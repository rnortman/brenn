//! MQTT transport runtime for Brenn.
//!
//! The `MqttSend` virtual tool lets Claude-Code-driven apps publish to
//! operator-configured MQTT brokers. MQTT ingress discovery + health is served
//! by the transport-agnostic `MessageChannelList`, and ad-hoc retained reads by
//! `MessageSubscribe` (pull-only) + `MessageChannelGet`.
//!
//! # Layering
//!
//! Everything on the wire lives here; the addressing and configuration half
//! stays in `brenn_lib::mqtt`, because `access`, `config::resolve` and
//! `messaging` parse MQTT addresses and the configuration aggregate holds the
//! `[[mqtt_client]]` blocks. Nothing in brenn-lib reaches back into this crate.
//!
//! # Module layout
//!
//! - `payload`    — `InboundPayload` / `OutboundPayload`; `classify_inbound` / `decode_outbound_body`.
//! - `state`      — `MqttClientHandle`, `SupervisorState`, `IngressSubscription`.
//! - `egress`     — shared capability/ACL/budget enforcement + broker publish (`enforce_and_publish`).
//! - `service`    — `MqttService`, `MqttEventRouter` trait.
//! - `connection` — unified per-client supervisor (`spawn_client_supervisor`) plus
//!   subscription helpers (`union_subscriptions`, `assert_ingress_subscription`).

pub mod connection;
pub mod egress;
pub mod payload;
pub mod service;
pub mod state;

pub use connection::{spawn_client_supervisor, union_subscriptions};
pub use egress::{MqttEgressError, SendBudget, enforce_and_publish};
pub use payload::{InboundPayload, OutboundPayload, classify_inbound, decode_outbound_body};
pub use service::{MqttEventRouter, MqttService};
pub use state::{ConnectorHealthLabel, IngressSubscription, MqttClientHandle};

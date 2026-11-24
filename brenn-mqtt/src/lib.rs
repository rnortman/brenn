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
//! - `payload`    — `InboundPayload` / `OutboundPayload`; `classify_inbound` / `decode_outbound`.
//! - `state`      — `MqttClientHandle`, `SupervisorState`, `IngressSubscription`.
//! - `service`    — `MqttService`, `MqttEventRouter` trait.
//! - `reader`     — the per-client egress reader (`start_egress_reader`) and the
//!   `EgressBus` view of a client's egress channel it reads through.
//! - `connection` — unified per-client supervisor (`spawn_client_supervisor`) plus
//!   subscription helpers (`union_subscriptions`, `try_assert_ingress_subscription`).
//! - `test_support` — a `mosquitto` harness and its throwaway TLS material, for
//!   this crate's integration suite and for the crates above that drive ingress
//!   through a real broker. Behind the `testutils` feature.

pub mod connection;
pub mod payload;
pub mod reader;
pub mod service;
pub mod state;
#[cfg(feature = "testutils")]
pub mod test_support;

pub use connection::{
    ArrivingFilters, register_and_spawn, spawn_client_supervisor, union_subscriptions,
};
pub use payload::{
    InboundClass, InboundPayload, OutboundArg, OutboundPayload, classify_inbound, decode_outbound,
};
pub use reader::{
    EgressBus, EgressItem, EgressMessage, EgressMeta, EgressReport, UnknownCause,
    start_egress_reader,
};
pub use service::{
    InboundRouting, IngressSubscribeOutcome, IngressUnsubscribeOutcome, MqttEventRouter,
    MqttService,
};
pub use state::{
    ConnectorHealthLabel, IngressSubscription, IngressSubscriptionSet, MqttClientHandle,
    ResolvedIdentifiers, SubAckOutcome,
};

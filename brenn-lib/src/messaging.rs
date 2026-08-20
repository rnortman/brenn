//! The addressing and configuration vocabulary of intra-Brenn messaging.
//!
//! Channels are globally declared; apps publish / subscribe via per-app
//! config + a small set of MCP virtual tools.
//!
//! What lives here is the half of that subsystem this crate's own production
//! code reads: the address protocol and derived channel UUIDs
//! ([`addressing`]), the channel/subscriber directory ([`directory`]), the
//! config types and their resolution ([`config`], [`remote`]), the
//! participant identities ([`identity`]) and the name/size gates
//! ([`gates`]). `config::validate_and_resolve`, `access`, `mqtt`, `webhook`
//! and `tools` all reach one of these.
//!
//! The runtime — `Messenger`, its stores and the messaging tables — is the
//! `brenn-messaging` crate, one layer up. Nothing here reaches it.

pub mod addressing;
pub mod config;
pub mod directory;
pub mod gates;
pub mod identity;
pub mod remote;

#[cfg(any(test, feature = "testutils"))]
pub mod test_support;

pub use addressing::{
    AUTO_CHANNEL_SEGMENT, auto_channel_cid, auto_channel_name, canonical_address,
    canonicalize_channel_address, chat_channel_uuid_from_address, durable_auto_channel_uuid,
    ephemeral_channel_uuid_from_name, is_auto_channel_name, is_reserved_channel_name,
    is_unreserved_char, local_channel_uuid_from_name, mqtt_channel_uuid_from_address,
    nondurable_channel_uuid, tool_channel_uuid_from_address, webhook_channel_uuid_from_slug,
};
pub use config::{
    ChannelConfigRaw, Depth, MessagingConfigRaw, MessagingGlobalConfig, MessagingSubscriptionRaw,
    NoiseLevel, ResolvedChannel, ResolvedMessagingConfig, ResolvedSubscription, Sink,
    WasmConsumerConfigRaw, WasmConsumerSubscriptionRaw, WasmInputPort,
};
pub use directory::{
    ChannelEntry, DynamicSubscriptionRow, MessagingDirectory, SubscriberEntry, SubscriberEntryKind,
    SubscriberRegistration, WakeEconomics, WakeMin,
};
pub use identity::{AttachKind, AttachScope, ParticipantId, SubscriberKind};
pub use remote::{RemoteConfigRaw, RemoteDepths, RemoteGrant, RemoteToken, ResolvedRemote};

// The wire contract between the Brenn host and WASM guest components lives in
// `brenn-envelope` so guests can depend on that lightweight crate without
// pulling in all of brenn-lib's host dependencies. Re-exported here at the
// paths every host caller already uses.
pub use brenn_envelope::{
    BRENN_ADDRESS_PREFIX, ChannelScheme, EPHEMERAL_ADDRESS_PREFIX, Impetus, LOCAL_ADDRESS_PREFIX,
    MQTT_ADDRESS_PREFIX, MessageEnvelope, MqttEnvelope, MqttPayloadBody, PWA_PUSH_ADDRESS_PREFIX,
    Urgency, WEBHOOK_ADDRESS_PREFIX, WebhookEnvelope, utc_from_epoch_ms,
};

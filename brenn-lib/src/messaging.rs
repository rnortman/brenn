//! The addressing and configuration vocabulary of intra-Brenn messaging.
//!
//! Channels are globally declared; apps publish / subscribe via per-app
//! config + a small set of MCP virtual tools.
//!
//! What lives here is the half of that subsystem this crate's own production
//! code reads: the address protocol and derived channel UUIDs (re-exported
//! from `brenn_envelope::addressing`), the channel/subscriber directory
//! ([`directory`]), the
//! config types and their resolution ([`config`], [`remote`]), the
//! participant identities ([`identity`]) and the name/size gates
//! ([`gates`]). `config::validate_and_resolve`, `access`, `mqtt`, `webhook`
//! and `tools` all reach one of these.
//!
//! The runtime — `Messenger`, its stores and the messaging tables — is the
//! `brenn-messaging` crate, one layer up. Nothing here reaches it.

pub mod config;
pub mod directory;
pub mod gates;
pub mod identity;
pub mod remote;
pub mod tombstone;

#[cfg(any(test, feature = "testutils"))]
pub mod test_support;

pub use brenn_envelope::addressing::{
    APP_TOOL_RESULTS_NAMESPACE, AUTO_CHANNEL_SEGMENT, DEVICE_LOCATION_PREFIX, EGRESS_NAMESPACE,
    MQTT_EGRESS_PREFIX, TOOL_RESULTS_NAMESPACE, auto_channel_cid, auto_channel_name,
    canonical_address, canonicalize_channel_address, chat_channel_uuid_from_address,
    device_location_address, device_location_channel_uuid, device_location_user,
    durable_auto_channel_uuid, egress_channel_uuid, ends_at_matcher_boundary,
    ends_at_tuning_boundary, ephemeral_channel_uuid_from_name, in_a_tool_namespace,
    in_app_tool_results_namespace, in_the_device_location_family, in_the_egress_namespace,
    is_auto_channel_name, is_device_location_prefix, is_reserved_channel, is_reserved_channel_name,
    is_unreserved_char, is_unreserved_name, local_channel_uuid_from_name, matcher_boundary_list,
    mqtt_channel_uuid_from_address, mqtt_egress_address, mqtt_egress_client,
    nondurable_channel_uuid, tool_channel_uuid_from_address, tuning_boundary_list,
    webhook_channel_uuid_from_slug,
};
pub use brenn_envelope::channel_model::ChannelBlockRole;

/// The address of `app_slug`'s result inbox, `brenn:app-tool-results/<app-slug>`:
/// where a tool result that carries attachments to one of the app's
/// conversations rests.
pub fn app_tool_result_inbox_address(app_slug: &str) -> String {
    canonical_address(&format!("{APP_TOOL_RESULTS_NAMESPACE}{app_slug}"))
}
pub use brenn_envelope::grants::{
    AttachGrant, ComponentGrant, ComponentHost, EntityKind, Plane, bindable_schemes,
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
pub use remote::{RemoteConfigRaw, RemoteDepths, RemoteToken, ResolvedRemote};
pub use tombstone::{Lookup, TombstonedRegistry};

// The wire contract between the Brenn host and WASM guest components lives in
// `brenn-envelope` so guests can depend on that lightweight crate without
// pulling in all of brenn-lib's host dependencies. Re-exported here at the
// paths every host caller already uses.
pub use brenn_envelope::{
    AttachmentName, Attachments, BRENN_ADDRESS_PREFIX, BlobId, BlobRef, BodyError, ChannelScheme,
    EPHEMERAL_ADDRESS_PREFIX, EnvelopeShape, Impetus, LOCAL_ADDRESS_PREFIX, MQTT_ADDRESS_PREFIX,
    MessageBody, MessageEnvelope, MqttEnvelope, MqttExpiry, MqttPayloadBody, MqttPublishTransport,
    MqttTransport, PWA_PUSH_ADDRESS_PREFIX, ShapedEnvelope, TextShape, TransportMeta, Urgency,
    WEBHOOK_ADDRESS_PREFIX, WebhookEnvelope, WebhookTransport, check_content_type,
    is_text_content_type, serialize_text_shape, utc_from_epoch_ms, write_attachment_metadata,
    write_attachment_refs,
};

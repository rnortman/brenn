//! The MQTT outbound channel, `brenn:egress/mqtt`, and the
//! `system:mqtt-outbound` participant declared as its subscriber.
//!
//! Every admitted publish to an `mqtt:<client>:<topic>` address is committed
//! to this one durable channel, with its MQTT options as the envelope's
//! `MqttPublish` transport metadata. Nothing reads the channel or delivers
//! from it.
//!
//! The entry and the spec are derived from nothing but the system-channel
//! tuning, so every plan mints the same two, whether or not any MQTT client is
//! declared: a reload that adds the first client then finds the participant
//! already there.

use crate::config::{MessagingGlobalConfig, SystemChannelTuning, resolve_system_channel};
use crate::system::SystemParticipantSpec;
use crate::{ChannelEntry, ChannelScheme, MQTT_OUTBOUND_ADDRESS, egress_channel_uuid};

/// System-participant component name; the identity is `system:mqtt-outbound`.
pub const MQTT_OUTBOUND_COMPONENT: &str = "mqtt-outbound";

/// The directory entry for `brenn:egress/mqtt`: durable `brenn:`, its uuid
/// derived from the address, depths and the forced `non_text` from the
/// system-channel resolver, no subscriber pre-set.
pub fn mqtt_outbound_entry(
    tuning: &SystemChannelTuning,
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let address = MQTT_OUTBOUND_ADDRESS.to_string();
    let resolved_channel = resolve_system_channel(&address, tuning, defaults);
    ChannelEntry {
        uuid: egress_channel_uuid(&address),
        resolved_channel,
        description: Some(
            "MQTT publishes admitted for delivery, each bound for the client and topic in its \
             transport metadata"
                .to_string(),
        ),
        address,
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// The `system:mqtt-outbound` participant: subscribed to `brenn:egress/mqtt`
/// and nothing else, publishing nothing.
// TODO(mqtt-egress-unwired): boot attaches no position and runs no reader for it.
pub fn mqtt_outbound_spec() -> SystemParticipantSpec {
    SystemParticipantSpec::subscribe_only_durable(
        MQTT_OUTBOUND_COMPONENT,
        &[MQTT_OUTBOUND_ADDRESS.to_string()],
    )
}

#[cfg(test)]
mod tests {
    use brenn_envelope::grants::AppCapability;

    use super::*;
    use crate::config::{Depth, EGRESS_DEFAULT_RETAIN_DEPTH};

    #[test]
    fn the_entry_is_durable_brenn_with_the_family_window_and_non_text() {
        let entry = mqtt_outbound_entry(
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );
        assert_eq!(entry.address, "brenn:egress/mqtt");
        assert_eq!(entry.transport_type, ChannelScheme::Brenn);
        assert!(entry.capabilities().durable);
        assert_eq!(entry.uuid, egress_channel_uuid("brenn:egress/mqtt"));
        assert_eq!(
            entry.resolved_channel.retain_depth,
            EGRESS_DEFAULT_RETAIN_DEPTH
        );
        assert_eq!(entry.resolved_channel.retain_depth, Depth::Bounded(256));
        assert!(entry.resolved_channel.non_text);
        assert!(entry.subscribers.is_empty());
        assert!(entry.mount.is_none());
    }

    #[test]
    fn the_spec_subscribes_to_the_channel_and_publishes_nowhere() {
        let spec = mqtt_outbound_spec();
        assert_eq!(spec.component, "mqtt-outbound");
        assert_eq!(spec.subscriptions, vec!["brenn:egress/mqtt".to_string()]);
        assert!(spec.policy.grants.has(AppCapability::MessagingSubscribe));
        assert!(!spec.policy.grants.has(AppCapability::MessagingPublish));
        assert!(spec.policy.acls.brenn_publish.is_empty());
    }
}

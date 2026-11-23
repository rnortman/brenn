//! The MQTT egress channels, one `brenn:egress/mqtt/<client>` per configured
//! MQTT client, and the `system:mqtt-outbound` participant declared as their
//! subscriber.
//!
//! Every admitted publish to an `mqtt:<client>:<topic>` address is committed
//! to `<client>`'s durable egress channel, with its MQTT options as the
//! envelope's `MqttPublish` transport metadata.
//!
//! The participant is derived in every plan, with no client too, and its
//! policy is one prefix matcher over the whole family, so it is the same for
//! every client set: a reload that adds the first client finds the
//! participant already there, and only its subscriptions move.

use brenn_lib::access::acl::ChannelMatcher;

use crate::config::{
    MessagingGlobalConfig, NoiseLevel, SystemChannelTuning, resolve_system_channel,
};
use crate::system::SystemParticipantSpec;
use crate::{
    ChannelEntry, ChannelScheme, MQTT_EGRESS_PREFIX, ParticipantId, egress_channel_uuid,
    mqtt_egress_address,
};
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;

/// System-participant component name; the identity is `system:mqtt-outbound`.
pub const MQTT_OUTBOUND_COMPONENT: &str = "mqtt-outbound";

/// The egress reader's identity, under which it holds its position on every
/// egress channel and against which an MQTT commit is checked.
pub fn mqtt_outbound_participant() -> ParticipantId {
    ParticipantId::for_system(MQTT_OUTBOUND_COMPONENT)
}

/// The participant's overflow rung, on every egress channel it subscribes to:
/// a message the window drops before the reader reached it is counted and
/// raised to the operator by the bus.
pub const MQTT_OUTBOUND_NOISE: NoiseLevel = NoiseLevel::Alarm;

/// The directory entry for `client`'s egress channel,
/// `brenn:egress/mqtt/<client>`: durable `brenn:`, its uuid derived from the
/// address, depths and the forced `non_text` from the system-channel
/// resolver, no subscriber pre-set.
pub fn mqtt_egress_entry(
    client: &str,
    tuning: &SystemChannelTuning,
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let address = mqtt_egress_address(client);
    let resolved_channel = resolve_system_channel(&address, tuning, defaults);
    ChannelEntry {
        uuid: egress_channel_uuid(&address),
        resolved_channel,
        description: Some(format!(
            "MQTT publishes admitted for delivery through the client `{client}`, each bound for \
             the topic in its transport metadata"
        )),
        address,
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// The `system:mqtt-outbound` participant: subscribed to the egress channel of
/// each of `clients`, in the order given, publishing nothing. Its policy is
/// `MessagingSubscribe` and one `brenn_subscribe` prefix matcher over the
/// egress family, whatever the clients. Its overflow rung is
/// [`MQTT_OUTBOUND_NOISE`].
pub fn mqtt_outbound_spec<'a>(clients: impl IntoIterator<Item = &'a str>) -> SystemParticipantSpec {
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Prefix(MQTT_EGRESS_PREFIX.to_string()));
    SystemParticipantSpec {
        component: MQTT_OUTBOUND_COMPONENT,
        policy,
        subscriptions: clients.into_iter().map(mqtt_egress_address).collect(),
        noise: MQTT_OUTBOUND_NOISE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Depth, EGRESS_DEFAULT_RETAIN_DEPTH};

    #[test]
    fn the_entry_is_durable_brenn_with_the_family_window_and_non_text() {
        let entry = mqtt_egress_entry(
            "home",
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );
        assert_eq!(entry.address, "brenn:egress/mqtt/home");
        assert_eq!(entry.transport_type, ChannelScheme::Brenn);
        assert!(entry.capabilities().durable);
        assert_eq!(entry.uuid, egress_channel_uuid("brenn:egress/mqtt/home"));
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
    fn the_spec_subscribes_to_each_client_channel_under_one_prefix_policy() {
        let spec = mqtt_outbound_spec(["home", "away"]);
        assert_eq!(spec.component, "mqtt-outbound");
        assert_eq!(
            spec.subscriptions,
            vec![
                "brenn:egress/mqtt/home".to_string(),
                "brenn:egress/mqtt/away".to_string()
            ]
        );
        assert_eq!(spec.noise, NoiseLevel::Alarm);
        assert!(spec.policy.grants.has(AppCapability::MessagingSubscribe));
        assert!(!spec.policy.grants.has(AppCapability::MessagingPublish));
        assert!(spec.policy.acls.brenn_publish.is_empty());
        assert_eq!(
            spec.policy.acls.brenn_subscribe,
            vec![ChannelMatcher::Prefix("egress/mqtt/".to_string())]
        );
        assert!(spec.policy.allows_brenn_delivery("egress/mqtt/home"));
    }

    #[test]
    fn the_policy_is_the_same_for_every_client_set() {
        let none = mqtt_outbound_spec([]);
        assert!(none.subscriptions.is_empty());
        assert_eq!(none.policy, mqtt_outbound_spec(["home"]).policy);
        assert_eq!(none.noise, NoiseLevel::Alarm);
    }
}

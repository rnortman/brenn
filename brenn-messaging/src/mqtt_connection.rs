//! The per-client MQTT connection-state channels,
//! `brenn:mqtt-connection.<client>`, and the `system:mqtt-connection`
//! participant that writes them.
//!
//! One durable channel per declared `mqtt_client`, whose newest message is the
//! client's connection state. Its depth and send rate are the host's: no
//! `[[channel]]` block reaches the family, and no access policy may be granted
//! a publish into it, so `system:mqtt-connection` is its only possible writer.
//!
//! The writer is publish-only and code-built: it holds `MessagingPublish` on
//! the family prefix and nothing else, and reads nothing from config, so it is
//! the same in every plan whatever the client set.

use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;

use crate::config::{
    MQTT_CONNECTION_SEND_RATE, MessagingGlobalConfig, NoiseLevel, SystemChannelTuning,
    resolve_system_channel,
};
use crate::system::SystemParticipantSpec;
use crate::{
    ChannelEntry, ChannelScheme, MQTT_CONNECTION_PREFIX, mqtt_connection_address,
    mqtt_connection_channel_uuid,
};

/// System-participant component name; the identity is
/// `system:mqtt-connection`.
pub const MQTT_CONNECTION_COMPONENT: &str = "mqtt-connection";

/// The `system:mqtt-connection` participant: publish on every client's
/// connection-state channel, subscribe on nothing.
///
/// The matcher is the bare family prefix, which ends at the `.` boundary and so
/// reaches `mqtt-connection.<client>` and nothing beside it.
// TODO(mqtt-connection-state-ops-config): the production config grants no reader yet.
pub fn mqtt_connection_spec() -> SystemParticipantSpec {
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingPublish);
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Prefix(MQTT_CONNECTION_PREFIX.to_string()));
    SystemParticipantSpec {
        component: MQTT_CONNECTION_COMPONENT,
        policy,
        subscriptions: vec![],
        noise: NoiseLevel::Silent,
    }
}

/// The directory entry for `client`'s connection-state channel: durable
/// `brenn:`, its uuid derived from the address, depths from the system-channel
/// resolver, text-only, the send rate [`MQTT_CONNECTION_SEND_RATE`], no
/// subscriber pre-set.
pub fn mqtt_connection_entry(
    client: &str,
    tuning: &SystemChannelTuning,
    defaults: &MessagingGlobalConfig,
) -> ChannelEntry {
    let address = mqtt_connection_address(client);
    let mut resolved_channel = resolve_system_channel(&address, tuning, defaults);
    resolved_channel.send_rate = MQTT_CONNECTION_SEND_RATE;
    ChannelEntry {
        uuid: mqtt_connection_channel_uuid(&address),
        resolved_channel,
        description: Some(format!(
            "Connection state of the MQTT client `{client}`: the newest message is the state"
        )),
        address,
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MQTT_CONNECTION_DEFAULT_RETAIN_DEPTH, SendRate};
    use crate::{Depth, MQTT_EGRESS_PREFIX};

    fn bare_covered(spec: &SystemParticipantSpec, bare: &str) -> bool {
        spec.policy
            .acls
            .brenn_publish
            .iter()
            .any(|m| m.matches(bare))
    }

    #[test]
    fn the_writer_publishes_on_the_family_and_nothing_beside_it() {
        let spec = mqtt_connection_spec();
        assert_eq!(spec.component, MQTT_CONNECTION_COMPONENT);
        assert!(spec.subscriptions.is_empty());
        assert!(spec.policy.grants.has(AppCapability::MessagingPublish));
        assert!(!spec.policy.grants.has(AppCapability::MessagingSubscribe));
        assert!(bare_covered(&spec, "mqtt-connection.home"));
        assert!(bare_covered(&spec, "mqtt-connection.home.v2"));
        assert!(!bare_covered(&spec, "mqtt-connectionx"));
        assert!(!bare_covered(&spec, "mqtt-connection"));
        assert!(!bare_covered(&spec, &format!("{MQTT_EGRESS_PREFIX}home")));
    }

    #[test]
    fn the_spec_reads_nothing_from_config() {
        assert_eq!(mqtt_connection_spec(), mqtt_connection_spec());
    }

    #[test]
    fn the_entry_is_the_clients_durable_text_channel_at_the_family_depth() {
        let entry = mqtt_connection_entry(
            "home",
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );
        assert_eq!(entry.address, "brenn:mqtt-connection.home");
        assert_eq!(
            entry.uuid,
            mqtt_connection_channel_uuid("brenn:mqtt-connection.home")
        );
        assert_eq!(entry.transport_type, ChannelScheme::Brenn);
        assert!(entry.capabilities().durable);
        assert!(!entry.resolved_channel.non_text);
        assert!(entry.subscribers.is_empty());
        assert_eq!(entry.resolved_channel.push_depth, Depth::Bounded(1));
        assert_eq!(
            entry.resolved_channel.retain_depth,
            MQTT_CONNECTION_DEFAULT_RETAIN_DEPTH
        );
        assert_eq!(
            entry.resolved_channel.standing_retain_depth,
            MQTT_CONNECTION_DEFAULT_RETAIN_DEPTH
        );
        assert_eq!(MQTT_CONNECTION_DEFAULT_RETAIN_DEPTH, Depth::Bounded(8));
    }

    /// The operator's `default_send_rate` does not reach the family: the
    /// host's record of a connection must not be made false by a rate.
    #[test]
    fn the_entry_takes_the_host_send_rate() {
        let restrictive = SendRate {
            burst: 1,
            refill_interval_secs: 3600,
            refill: 1,
        };
        let defaults = MessagingGlobalConfig {
            default_send_rate: restrictive,
            ..MessagingGlobalConfig::default()
        };
        let entry = mqtt_connection_entry("home", &SystemChannelTuning::default(), &defaults);
        assert_eq!(entry.resolved_channel.send_rate, MQTT_CONNECTION_SEND_RATE);
    }
}

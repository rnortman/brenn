//! What a bus publish to an MQTT topic carries besides its payload, and the
//! view of the declared clients the publish path reads.

use std::time::Duration;

/// The MQTT options of one outbound publish, as its publisher stated them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttPublishOptions {
    /// 0, 1 or 2.
    pub qos: u8,
    pub retain: bool,
    /// The MQTT 5 message expiry to send, in seconds; at least 1 when stated.
    /// It also sets the message's lifetime inside Brenn.
    pub message_expiry_s: Option<u32>,
    /// MQTT 5 Content Type to send; absent sends none.
    pub content_type: Option<String>,
}

impl MqttPublishOptions {
    /// The message's lifetime, from which the admission computes its
    /// `expires_at`: the stated expiry, or [`MQTT_OUTBOUND_DEFAULT_LIFETIME`].
    pub fn lifetime(&self) -> Duration {
        match self.message_expiry_s {
            Some(s) => Duration::from_secs(u64::from(s)),
            None => MQTT_OUTBOUND_DEFAULT_LIFETIME,
        }
    }
}

/// Lifetime of an outbound message whose publisher stated no expiry.
pub const MQTT_OUTBOUND_DEFAULT_LIFETIME: Duration = super::PUBLISH_WAIT;

/// The declared MQTT clients, as the publish path needs them.
pub trait MqttClientDirectory: Send + Sync {
    /// The client's `outbound_payload_cap_bytes`, or `None` when no such client
    /// is registered.
    fn outbound_payload_cap(&self, client: &str) -> Option<u32>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(expiry: Option<u32>) -> MqttPublishOptions {
        MqttPublishOptions {
            qos: 0,
            retain: false,
            message_expiry_s: expiry,
            content_type: None,
        }
    }

    #[test]
    fn lifetime_is_the_stated_expiry_or_the_default() {
        assert_eq!(options(Some(90)).lifetime(), Duration::from_secs(90));
        assert_eq!(options(None).lifetime(), MQTT_OUTBOUND_DEFAULT_LIFETIME);
        assert_eq!(MQTT_OUTBOUND_DEFAULT_LIFETIME, Duration::from_secs(10));
    }
}

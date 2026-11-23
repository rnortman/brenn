//! What a bus publish to an MQTT topic carries besides its payload, and the
//! view of the declared clients the publish path reads.

use std::num::NonZeroU32;

/// How long an outbound MQTT message is worth delivering, counted from its
/// commit, or from its release time when it was parked. What is left of it at hand-off is sent as the MQTT 5 Message
/// Expiry Interval; `Forever` sends none. There is no default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttLifetime {
    Seconds(NonZeroU32),
    Forever,
}

/// The MQTT options of one outbound publish, as its publisher stated them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttPublishOptions {
    /// 0, 1 or 2.
    pub qos: u8,
    pub retain: bool,
    pub lifetime: MqttLifetime,
    /// MQTT 5 Content Type to send; absent sends none.
    pub content_type: Option<String>,
    /// The `brenn:` channel the host publishes this message's delivery
    /// statuses to, as stated; absent asks for none.
    pub status_to: Option<String>,
    /// The reply address as stated, a full `mqtt:<client>:<topic>`, sent as
    /// the MQTT 5 Response Topic; absent sends none.
    pub reply_to: Option<String>,
}

/// The declared MQTT clients, as the publish path needs them.
pub trait MqttClientDirectory: Send + Sync {
    /// The client's `outbound_payload_cap_bytes`, or `None` when no such client
    /// is registered.
    fn outbound_payload_cap(&self, client: &str) -> Option<u32>;
}

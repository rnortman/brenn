//! The `mqtt` interface's admission seam over the bus.
//!
//! A component's MQTT publish is decided at the call by
//! [`Messenger::admit_mqtt_from_wasm`] — the same answer an agent's publish
//! gets — and committed with the activation's other publishes by
//! [`Messenger::publish_from_wasm`]. Here rather than beside the host's other
//! seams so this crate's suites drive the production body instead of a copy.

use std::num::NonZeroU32;
use std::sync::Arc;

use brenn_lib::access::AppPolicy;
use brenn_lib::mqtt::{MqttLifetime, MqttPublishOptions};
use brenn_messaging::{Messenger, MqttRefusal, PublishResult};
use brenn_obs::alerting::AlertDispatcher;
use brenn_obs::security::{DenialKind, DenialOrigin, SecurityEventType, signal_publish_denial};
use brenn_wasm::{MqttAdmission, MqttPublishProps, MqttTarget};

/// The admission seam for a consumer holding the `mqtt` grant under `policy`.
///
/// `resolve` parses `mqtt:<client>:<topic>` and refuses a wildcard; `admit`
/// asks the bus. An ACL denial raises the publish-denial security signal,
/// attributed to the component, once per denied call.
///
/// # Panics
///
/// `admit` panics on any bus answer a component's publish cannot meet on a
/// sound host: the policy lacking the publish grant this seam is built for, a
/// client whose session or egress channel is gone while a consumer naming it
/// runs, or an option or shape the seam never states.
#[must_use]
pub fn mqtt_publisher_over(
    messenger: &Arc<Messenger>,
    policy: AppPolicy,
    slug: String,
    alerts: AlertDispatcher,
) -> brenn_wasm::MqttPublishFn {
    Arc::new(BusMqttPublisher {
        messenger: Arc::clone(messenger),
        policy,
        slug,
        alerts,
    })
}

/// The bus options of a component's publish: `mqtt-publish` is lifetime
/// forever, `mqtt-publish-expiring` a lifetime of its seconds.
///
/// # Panics
///
/// On a zero expiry, which the import refuses before the seam is reached.
pub(crate) fn mqtt_options(props: &MqttPublishProps) -> MqttPublishOptions {
    let lifetime = match props.message_expiry_s {
        None => MqttLifetime::Forever,
        Some(s) => MqttLifetime::Seconds(
            NonZeroU32::new(s).expect("a zero message expiry is refused at the mqtt import"),
        ),
    };
    MqttPublishOptions {
        qos: props.qos,
        retain: props.retain,
        lifetime,
        content_type: props.content_type.clone(),
        status_to: None,
    }
}

struct BusMqttPublisher {
    messenger: Arc<Messenger>,
    policy: AppPolicy,
    /// The consumer's slug, the attribution of a denial.
    slug: String,
    alerts: AlertDispatcher,
}

impl brenn_wasm::MqttPublisher for BusMqttPublisher {
    /// Build and parse `mqtt:<client>:<topic>`, rejecting wildcards in publish
    /// context. The parse splits at the first `:`, so `("home:a", "b")` and
    /// `("home", "a:b")` are one target.
    fn resolve(&self, client: &str, topic: &str) -> Result<MqttTarget, String> {
        let raw = format!("mqtt:{client}:{topic}");
        brenn_lib::mqtt::address::parse_topic_name(&raw)
            .map(|addr| MqttTarget {
                client: addr.client,
                topic: addr.topic,
            })
            .map_err(|e| e.to_string())
    }

    fn admit(
        &self,
        target: &MqttTarget,
        payload_len: usize,
        props: &MqttPublishProps,
    ) -> MqttAdmission {
        let len = u64::try_from(payload_len).expect("a payload length fits in u64");
        let answer = self.messenger.admit_mqtt_from_wasm(
            &self.policy,
            &target.client,
            &target.topic,
            &mqtt_options(props),
            len,
        );
        match answer {
            Ok(()) => MqttAdmission::Admitted,
            Err(PublishResult::AclDenied(address)) => {
                // `address` is guest-derived; `signal_publish_denial` sanitizes it.
                signal_publish_denial(
                    &self.alerts,
                    SecurityEventType::MqttPublishDenied,
                    DenialOrigin::Component { slug: &self.slug },
                    DenialKind::AclDenied,
                    &address,
                );
                MqttAdmission::NotPermitted
            }
            Err(PublishResult::MqttRefused(MqttRefusal::InvalidQos { qos })) => {
                MqttAdmission::InvalidPayload(format!("invalid qos {qos}: must be 0, 1, or 2"))
            }
            Err(PublishResult::MqttRefused(MqttRefusal::InvalidContentType { reason })) => {
                MqttAdmission::InvalidPayload(format!("invalid content type: {reason}"))
            }
            Err(PublishResult::MqttRefused(MqttRefusal::PayloadOverClientCap { len, cap })) => {
                MqttAdmission::InvalidPayload(format!(
                    "payload {len} bytes exceeds the client's outbound cap of {cap}"
                ))
            }
            Err(PublishResult::MissingSender) => panic!(
                "consumer {:?}: mqtt admission without the mqtt publish grant — the seam is \
                 built only for a consumer holding `mqtt` (host bug)",
                self.slug
            ),
            Err(PublishResult::UnknownChannel(address)) => panic!(
                "wasm mqtt-publish: no session or egress channel for {address:?} while consumer \
                 {:?} is live — a consumer naming a client is retired before the client is \
                 removed (host bug)",
                self.slug
            ),
            Err(other) => panic!(
                "consumer {:?}: mqtt admission answered {other:?} for an address built from \
                 `resolve`'s own output with no stated option and a stated length (host bug)",
                self.slug
            ),
        }
    }
}

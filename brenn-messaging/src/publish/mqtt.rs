//! The front door for a bus publish to an MQTT topic.
//!
//! An admitted publish to `mqtt:<client>:<topic>` is committed to the one
//! outbound channel, `brenn:egress/mqtt`, with the payload as its body (or its
//! one attachment) and the MQTT options as `MqttPublish` transport metadata.
//! [`Messenger::admit_mqtt`] is the one decision on whether it may be; the
//! common publish gates (body cap, send rate, send budget, commit) follow it
//! inside `publish_gated`.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use brenn_envelope::grants::AppCapability;
use brenn_envelope::{MessageBody, check_content_type};
use brenn_lib::access::AppPolicy;
use brenn_lib::mqtt::{MqttClientDirectory, MqttPublishOptions, parse_topic_name};
use brenn_messaging_store::blob::HeldAttachments;

use super::{PublishOrigin, PublishPrincipal, PublishResult};
use crate::{ChannelEntry, MQTT_OUTBOUND_ADDRESS, Messenger, MqttPublishTransport, Urgency};

/// Why the MQTT admission refused a publish on its options or payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttRefusal {
    /// A release time was stated. An MQTT publish is not deferred.
    DeliverAfter,
    /// A `reply_to` was stated, which a broker cannot carry.
    ReplyTo,
    /// A `delivery_deadline` was stated, which a broker cannot honour.
    DeliveryDeadline,
    /// QoS above 2.
    InvalidQos { qos: u8 },
    /// A stated message expiry of 0.
    InvalidExpiry,
    /// A stated content type that is not a well-formed media type.
    InvalidContentType { reason: String },
    /// Neither the body alone nor exactly one attachment beside an empty body.
    PayloadShape,
    /// The payload is over the client's `outbound_payload_cap_bytes`.
    PayloadOverClientCap { len: u64, cap: u32 },
}

/// The payload of a conversation's MQTT publish.
#[derive(Debug)]
pub enum MqttPayload {
    /// The bytes themselves, carried as the message body.
    Inline(Vec<u8>),
    /// Held attachments, carried beside an empty body; the admission accepts
    /// exactly one.
    Attached(HeldAttachments),
}

/// A committed MQTT publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttCommitted {
    pub message_id: Uuid,
    /// The conversation's send budget left after this publish; `None` for an
    /// origin with no budget.
    pub remaining_budget: Option<u32>,
}

/// An admitted MQTT publish: the outbound channel to commit to, and what the
/// stored transport metadata is built from.
pub(crate) struct MqttAdmitted {
    pub(crate) channel: Arc<ChannelEntry>,
    client: String,
    topic: String,
    options: MqttPublishOptions,
}

impl MqttAdmitted {
    /// The transport metadata for a message committed at `committed_at`.
    pub(crate) fn transport(&self, committed_at: DateTime<Utc>) -> MqttPublishTransport {
        let lifetime = chrono::Duration::from_std(self.options.lifetime())
            .expect("an MQTT lifetime of at most u32::MAX seconds fits a chrono duration");
        MqttPublishTransport {
            client_slug: self.client.clone(),
            topic: self.topic.clone(),
            qos: self.options.qos,
            retain: self.options.retain,
            message_expiry_s: self.options.message_expiry_s,
            content_type: self.options.content_type.clone(),
            expires_at: committed_at + lifetime,
        }
    }
}

impl Messenger {
    /// Install the table of declared MQTT clients the admission reads. Called
    /// once, at boot, after the MQTT service is built; the table itself is kept
    /// current by reload. A messenger with none installed knows no client.
    ///
    /// # Panics
    ///
    /// Panics on a second call: two client tables would be two answers to
    /// "is this client registered".
    pub fn install_mqtt_clients(&self, clients: Arc<dyn MqttClientDirectory>) {
        if self.mqtt_clients.set(clients).is_err() {
            panic!("Messenger::install_mqtt_clients called twice");
        }
    }

    /// Decide whether `policy` may publish `body` and `attachments` to the
    /// MQTT topic address `addr` with `options`. Spends nothing.
    ///
    /// The checks run in a fixed order, and the order is part of the answer: a
    /// publisher refused on its grant or ACL learns nothing of the client.
    /// `publish_gated` answers steps 1 and 2 itself, in this order, before it
    /// calls here; they are answered again for a caller that arrives with a
    /// policy and no layer-1 gate of its own.
    ///
    /// 1. `addr` is a well-formed `mqtt:` topic name → `MalformedAddress`.
    /// 2. the policy holds `MqttPublish` → `MissingSender`.
    /// 3. `allows_mqtt_publish(client, topic)` → `AclDenied`.
    /// 4. the client is registered → `UnknownChannel`.
    /// 5. no `deliver_after`, `reply_to` or `delivery_deadline` → `MqttRefused`.
    /// 6. QoS ≤ 2, a stated expiry ≥ 1, a stated content type well formed.
    /// 7. the payload is the body, or one attachment beside an empty body.
    /// 8. the payload's length is within the client's outbound cap.
    ///
    /// # Panics
    ///
    /// Panics when the client is registered and the directory holds no
    /// `brenn:egress/mqtt`: every messaging plan mints it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn admit_mqtt(
        &self,
        policy: &AppPolicy,
        addr: &str,
        options: &MqttPublishOptions,
        body: &MessageBody,
        attachments: &HeldAttachments,
        reply_to: Option<&str>,
        deliver_after: Option<DateTime<Utc>>,
        delivery_deadline: Option<DateTime<Utc>>,
    ) -> Result<MqttAdmitted, PublishResult> {
        let refuse = |r: MqttRefusal| Err(PublishResult::MqttRefused(r));

        let Ok(parsed) = parse_topic_name(addr) else {
            return Err(PublishResult::MalformedAddress(addr.to_string()));
        };
        if !policy.has_grant(AppCapability::MqttPublish) {
            return Err(PublishResult::MissingSender);
        }
        if !policy.allows_mqtt_publish(&parsed.client, &parsed.topic) {
            return Err(PublishResult::AclDenied(addr.to_string()));
        }
        let Some(cap) = self
            .mqtt_clients
            .get()
            .and_then(|clients| clients.outbound_payload_cap(&parsed.client))
        else {
            return Err(PublishResult::UnknownChannel(addr.to_string()));
        };

        // A stated option the broker cannot carry is refused, never silently dropped.
        if deliver_after.is_some() {
            return refuse(MqttRefusal::DeliverAfter);
        }
        if reply_to.is_some() {
            return refuse(MqttRefusal::ReplyTo);
        }
        if delivery_deadline.is_some() {
            return refuse(MqttRefusal::DeliveryDeadline);
        }

        if options.qos > 2 {
            return refuse(MqttRefusal::InvalidQos { qos: options.qos });
        }
        if options.message_expiry_s == Some(0) {
            return refuse(MqttRefusal::InvalidExpiry);
        }
        if let Some(ct) = options.content_type.as_deref()
            && let Err(e) = check_content_type(ct)
        {
            return refuse(MqttRefusal::InvalidContentType {
                reason: e.to_string(),
            });
        }

        let len = if attachments.is_empty() {
            body.as_bytes().len() as u64
        } else {
            let refs = attachments.refs();
            match refs.iter().next() {
                Some((_, blob)) if refs.len() == 1 && body.as_bytes().is_empty() => blob.size,
                _ => return refuse(MqttRefusal::PayloadShape),
            }
        };
        if len > u64::from(cap) {
            return refuse(MqttRefusal::PayloadOverClientCap { len, cap });
        }

        let channel = self
            .directory
            .resolve(MQTT_OUTBOUND_ADDRESS)
            .unwrap_or_else(|| {
                panic!(
                    "admit_mqtt: {MQTT_OUTBOUND_ADDRESS} is not in the directory — every messaging \
                 plan mints it"
                )
            });
        Ok(MqttAdmitted {
            channel,
            client: parsed.client,
            topic: parsed.topic,
            options: options.clone(),
        })
    }

    /// Publish `payload` to the MQTT topic address `addr` on behalf of an app's
    /// conversation, through the common publish gates with the MQTT admission
    /// at layer 2. Returns at commit. Nothing reads `brenn:egress/mqtt`, so a
    /// committed message reaches no broker.
    ///
    /// An inline payload becomes a body typed by `options.content_type` when
    /// its bytes are valid for that type, and an `application/octet-stream`
    /// body of the same bytes otherwise, so text reads as text to an observer
    /// of the channel and other bytes are still carried exactly. The MQTT
    /// Content Type sent is `options.content_type`, never the body's type. An
    /// attached payload rides beside an empty body.
    ///
    /// # Panics
    ///
    /// Panics on a `System` origin: an app's publish is priced by its
    /// conversation.
    // TODO(mqtt-egress-unwired): no production caller; it must gain none before a reader exists.
    pub async fn publish_mqtt(
        &self,
        origin: PublishOrigin,
        sender_app_slug: &str,
        addr: &str,
        payload: MqttPayload,
        options: &MqttPublishOptions,
    ) -> Result<MqttCommitted, PublishResult> {
        assert!(
            !matches!(origin, PublishOrigin::System),
            "Messenger::publish_mqtt called with PublishOrigin::System — an app's MQTT \
             publish is attributed to a conversation"
        );
        let (body, attachments) = match payload {
            MqttPayload::Inline(bytes) => {
                // A content type the admission will refuse also fails here; the
                // fallback body only carries the publish as far as that refusal.
                let body = MessageBody::from_parts(options.content_type.as_deref(), &bytes)
                    .unwrap_or_else(|_| {
                        MessageBody::from_parts(Some("application/octet-stream"), &bytes)
                            .expect("application/octet-stream is a well-formed binary type")
                    });
                (body, HeldAttachments::none())
            }
            MqttPayload::Attached(attachments) => (MessageBody::text(""), attachments),
        };
        let result = self
            .publish_core(
                origin,
                PublishPrincipal::App {
                    slug: sender_app_slug,
                },
                addr,
                body,
                Urgency::Normal,
                None,
                None,
                None,
                None,
                attachments,
                Some(options),
            )
            .await;
        match result {
            PublishResult::Ok {
                message_id,
                remaining_budget,
                ..
            } => Ok(MqttCommitted {
                message_id,
                remaining_budget,
            }),
            refused => Err(refused),
        }
    }
}

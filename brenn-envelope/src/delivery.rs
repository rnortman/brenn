//! Delivery status bodies: what the host publishes to a sender's `status_to`
//! channel about a message it handed off asynchronously.
//!
//! One body per event: `handed` when the message goes to its transport, then
//! one outcome. Every body is a JSON object carrying `"v":
//! DELIVERY_STATUS_VERSION`, stamped by [`encode`] and checked by [`decode`]:
//!
//! ```json
//! {"v":1,"message_id":"<uuid>","kind":"rejected","reason":"Not authorized",
//!  "mqtt":{"client":"ha","topic":"lights/k/set","qos":1,"reason_code":135}}
//! ```
//!
//! The top level is general: what happened to the message, not which
//! transport it happened on. The transport's own detail rides in its member
//! (`mqtt`), which a status need not carry. Kinds are closed: a new kind moves
//! the version. Causes are open: an unknown cause word decodes to `Other`, so
//! a transport adds causes without a version move. Unknown fields are ignored.
//!
//! Guest-safe: plain serde types, no clock, no I/O.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Schema version stamped into every body. Moves when a kind is added or a
/// body is reshaped so that a reader cannot absorb it.
pub const DELIVERY_STATUS_VERSION: u32 = 1;

/// The `sender` of every delivery status. A reader believes only envelopes
/// from this sender: a status channel is by rule one the original sender may
/// publish to itself, so anything else on it may be the sender's own. The
/// sender says the host wrote a status, not whose message it concerns: every
/// publisher that may write a channel may name it, so a reader matches
/// `message_id` against the ids its own publishes returned.
pub const DELIVERY_STATUS_SENDER: &str = "system:delivery-status";

/// The cap a `rejected` status's `reason` is sanitized to, before the
/// sanitizer's truncation marker.
pub const DELIVERY_REASON_MAX_BYTES: usize = 256;

/// Room [`max_encoded_len`] leaves past [`DELIVERY_REASON_MAX_BYTES`] for the
/// truncation marker a sanitizer appends to a reason it cut short.
pub const DELIVERY_REASON_MARKER_ALLOWANCE: usize = 16;

/// One event in the life of a message a sender asked to hear about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryStatus {
    /// The id the publish returned.
    pub message_id: Uuid,
    #[serde(flatten)]
    pub kind: DeliveryKind,
    /// The MQTT detail of a message published to a broker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mqtt: Option<MqttDelivery>,
}

/// What happened to the message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeliveryKind {
    /// Given to the transport. Not an outcome: one of the others follows, or
    /// nobody learns what became of it.
    Handed,
    /// The far end acknowledged it, or, on a transport mode with no
    /// acknowledgement (MQTT QoS 0), it was written to the connection.
    Delivered,
    /// The far end, or the host on its behalf, refused it, and no copy of it
    /// that the transport accepted went unanswered.
    Rejected { reason: String },
    /// Its lifetime ended and it was never sent: the transport never
    /// accepted it, whether or not a `handed` came first.
    Expired,
    /// It was not delivered, and a copy of it that the transport accepted
    /// went unanswered: it may have been sent, will not be sent again, and
    /// nobody knows whether it arrived.
    Unknown { cause: DeliveryUnknownCause },
    /// The host gave it up and it was never sent: the transport never
    /// accepted it, whether or not a `handed` came first.
    Dropped { cause: DeliveryDropCause },
}

/// Why nobody knows whether an `unknown` message arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryUnknownCause {
    /// The connection was lost with the message in flight.
    ConnectionLost,
    /// The message was accepted for sending, its connection ended before the
    /// far end answered, and its lifetime ended while it waited to be sent
    /// again.
    LifetimeEnded,
    /// The message was accepted for sending, its connection ended before the
    /// far end answered, and when the host went to send it again the send was
    /// refused: by the far end, or by the host because the message no longer
    /// fits what the far end accepts.
    ResendRefused,
    /// The transport's session stopped, or its client began to be removed,
    /// before the far end answered, with the message in flight or waiting to
    /// be sent again.
    Stopped,
    /// A cause this build does not know.
    #[serde(other)]
    Other,
}

/// Why a `dropped` message was given up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryDropCause {
    /// The transport's session stopped, or its client began to be removed,
    /// while the host held a message the transport had never accepted.
    Stopped,
    /// The transport it was bound for was removed.
    Removed,
    /// What it carried left the host's retention before it was sent.
    LeftRetention,
    /// A cause this build does not know.
    #[serde(other)]
    Other,
}

/// The MQTT detail of a status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqttDelivery {
    /// The declared client the message was published through.
    pub client: String,
    pub topic: String,
    pub qos: u8,
    /// The broker's MQTT 5 reason code when a broker refused the message: on
    /// `rejected`, and on `unknown` with cause `resend_refused`; absent on any
    /// other status, and on a refusal the host made itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<u8>,
}

/// Why a body could not be decoded: the shared versioned codec's error.
pub type DeliveryDecodeError = crate::versioned::VersionedDecodeError;

/// Serialize a status, stamping `"v": DELIVERY_STATUS_VERSION`.
pub fn encode(status: &DeliveryStatus) -> String {
    crate::versioned::encode(DELIVERY_STATUS_VERSION, status)
}

/// Parse a status, requiring `v` equal to [`DELIVERY_STATUS_VERSION`].
pub fn decode(body: &str) -> Result<DeliveryStatus, DeliveryDecodeError> {
    crate::versioned::decode(DELIVERY_STATUS_VERSION, body)
}

/// The length of the longest status [`encode`] can produce about a message
/// published through `client` to `topic`: over every kind, with a reason of
/// [`DELIVERY_REASON_MAX_BYTES`] plus [`DELIVERY_REASON_MARKER_ALLOWANCE`]
/// bytes of the character JSON escapes widest among those a sanitized reason
/// holds.
pub fn max_encoded_len(client: &str, topic: &str) -> usize {
    let reason = "\\".repeat(DELIVERY_REASON_MAX_BYTES + DELIVERY_REASON_MARKER_ALLOWANCE);
    let kinds = [
        DeliveryKind::Handed,
        DeliveryKind::Delivered,
        DeliveryKind::Rejected { reason },
        DeliveryKind::Expired,
        DeliveryKind::Unknown {
            cause: DeliveryUnknownCause::ConnectionLost,
        },
        DeliveryKind::Dropped {
            cause: DeliveryDropCause::LeftRetention,
        },
    ];
    kinds
        .into_iter()
        .map(|kind| {
            encode(&DeliveryStatus {
                message_id: Uuid::max(),
                kind,
                mqtt: Some(MqttDelivery {
                    client: client.to_owned(),
                    topic: topic.to_owned(),
                    qos: 2,
                    reason_code: Some(u8::MAX),
                }),
            })
            .len()
        })
        .max()
        .expect("the kind list is not empty")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn id() -> Uuid {
        Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef)
    }

    fn mqtt(reason_code: Option<u8>) -> Option<MqttDelivery> {
        Some(MqttDelivery {
            client: "ha".to_string(),
            topic: "lights/k/set".to_string(),
            qos: 1,
            reason_code,
        })
    }

    fn every_kind() -> Vec<DeliveryKind> {
        vec![
            DeliveryKind::Handed,
            DeliveryKind::Delivered,
            DeliveryKind::Rejected {
                reason: "Not authorized".to_string(),
            },
            DeliveryKind::Expired,
            DeliveryKind::Unknown {
                cause: DeliveryUnknownCause::ConnectionLost,
            },
            DeliveryKind::Unknown {
                cause: DeliveryUnknownCause::LifetimeEnded,
            },
            DeliveryKind::Unknown {
                cause: DeliveryUnknownCause::ResendRefused,
            },
            DeliveryKind::Unknown {
                cause: DeliveryUnknownCause::Stopped,
            },
            DeliveryKind::Dropped {
                cause: DeliveryDropCause::Stopped,
            },
            DeliveryKind::Dropped {
                cause: DeliveryDropCause::Removed,
            },
            DeliveryKind::Dropped {
                cause: DeliveryDropCause::LeftRetention,
            },
        ]
    }

    /// The literals out-of-tree readers decode by. A rename here breaks every
    /// one of them, so the wire form of each kind is pinned whole.
    #[test]
    fn each_kind_has_the_general_fields_at_the_top_and_mqtt_beneath() {
        let wire = |kind: DeliveryKind, code: Option<u8>| -> Value {
            serde_json::from_str(&encode(&DeliveryStatus {
                message_id: id(),
                kind,
                mqtt: mqtt(code),
            }))
            .unwrap()
        };
        let member = json!({"client": "ha", "topic": "lights/k/set", "qos": 1});
        let id = id().to_string();
        let cases = [
            (DeliveryKind::Handed, json!({"kind": "handed"})),
            (DeliveryKind::Delivered, json!({"kind": "delivered"})),
            (DeliveryKind::Expired, json!({"kind": "expired"})),
            (
                DeliveryKind::Unknown {
                    cause: DeliveryUnknownCause::ConnectionLost,
                },
                json!({"kind": "unknown", "cause": "connection_lost"}),
            ),
            (
                DeliveryKind::Unknown {
                    cause: DeliveryUnknownCause::LifetimeEnded,
                },
                json!({"kind": "unknown", "cause": "lifetime_ended"}),
            ),
            (
                DeliveryKind::Unknown {
                    cause: DeliveryUnknownCause::ResendRefused,
                },
                json!({"kind": "unknown", "cause": "resend_refused"}),
            ),
            (
                DeliveryKind::Unknown {
                    cause: DeliveryUnknownCause::Stopped,
                },
                json!({"kind": "unknown", "cause": "stopped"}),
            ),
            (
                DeliveryKind::Dropped {
                    cause: DeliveryDropCause::Stopped,
                },
                json!({"kind": "dropped", "cause": "stopped"}),
            ),
            (
                DeliveryKind::Dropped {
                    cause: DeliveryDropCause::Removed,
                },
                json!({"kind": "dropped", "cause": "removed"}),
            ),
            (
                DeliveryKind::Dropped {
                    cause: DeliveryDropCause::LeftRetention,
                },
                json!({"kind": "dropped", "cause": "left_retention"}),
            ),
        ];
        for (kind, fields) in cases {
            let mut expected = json!({"v": 1, "message_id": id, "mqtt": member});
            for (k, v) in fields.as_object().unwrap() {
                expected[k] = v.clone();
            }
            assert_eq!(wire(kind, None), expected);
        }

        assert_eq!(
            wire(
                DeliveryKind::Rejected {
                    reason: "Not authorized".to_string()
                },
                Some(135)
            ),
            json!({
                "v": 1, "message_id": id, "kind": "rejected", "reason": "Not authorized",
                "mqtt": {"client": "ha", "topic": "lights/k/set", "qos": 1, "reason_code": 135},
            })
        );
        assert_eq!(
            wire(
                DeliveryKind::Unknown {
                    cause: DeliveryUnknownCause::ResendRefused,
                },
                Some(135)
            ),
            json!({
                "v": 1, "message_id": id, "kind": "unknown", "cause": "resend_refused",
                "mqtt": {"client": "ha", "topic": "lights/k/set", "qos": 1, "reason_code": 135},
            })
        );
    }

    #[test]
    fn every_kind_round_trips() {
        for kind in every_kind() {
            for member in [None, mqtt(None), mqtt(Some(151))] {
                let status = DeliveryStatus {
                    message_id: id(),
                    kind: kind.clone(),
                    mqtt: member,
                };
                assert_eq!(decode(&encode(&status)), Ok(status));
            }
        }
    }

    #[test]
    fn a_body_without_mqtt_decodes_and_omits_it() {
        let status = DeliveryStatus {
            message_id: id(),
            kind: DeliveryKind::Delivered,
            mqtt: None,
        };
        let value: Value = serde_json::from_str(&encode(&status)).unwrap();
        assert!(value.get("mqtt").is_none());
        assert_eq!(
            decode(&format!(
                r#"{{"v":1,"message_id":"{}","kind":"delivered"}}"#,
                id()
            )),
            Ok(status)
        );
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let body = format!(
            r#"{{"v":1,"message_id":"{}","kind":"expired","added_later":3,
                "mqtt":{{"client":"ha","topic":"t","qos":0,"also_later":true}}}}"#,
            id()
        );
        let status = decode(&body).unwrap();
        assert_eq!(status.kind, DeliveryKind::Expired);
        assert_eq!(status.mqtt.unwrap().topic, "t");
    }

    #[test]
    fn an_unknown_cause_is_other_and_an_unknown_kind_fails() {
        let with = |rest: &str| format!(r#"{{"v":1,"message_id":"{}",{rest}}}"#, id());
        assert_eq!(
            decode(&with(r#""kind":"unknown","cause":"cosmic_ray""#))
                .unwrap()
                .kind,
            DeliveryKind::Unknown {
                cause: DeliveryUnknownCause::Other
            }
        );
        assert_eq!(
            decode(&with(r#""kind":"dropped","cause":"evicted""#))
                .unwrap()
                .kind,
            DeliveryKind::Dropped {
                cause: DeliveryDropCause::Other
            }
        );
        assert!(matches!(
            decode(&with(r#""kind":"teleported""#)),
            Err(DeliveryDecodeError::Payload(_))
        ));
    }

    #[test]
    fn another_version_fails() {
        assert_eq!(
            decode(&format!(
                r#"{{"v":2,"message_id":"{}","kind":"handed"}}"#,
                id()
            )),
            Err(DeliveryDecodeError::UnsupportedVersion(2))
        );
    }

    #[test]
    fn no_encoding_exceeds_max_encoded_len() {
        let topics = [
            "t".to_string(),
            "lights/k/set".to_string(),
            "quote\"and\\slash/\u{1}".to_string(),
            "x".repeat(4096),
        ];
        // A reason as a sanitizer leaves it: escapes are backslashes, the
        // widest a JSON string makes, then the truncation marker.
        let widest_reason = format!("{}…(truncated)", "\\".repeat(DELIVERY_REASON_MAX_BYTES));
        assert!(
            widest_reason.len() <= DELIVERY_REASON_MAX_BYTES + DELIVERY_REASON_MARKER_ALLOWANCE
        );
        for topic in &topics {
            let cap = max_encoded_len("ha", topic);
            let mut kinds = every_kind();
            kinds.push(DeliveryKind::Rejected {
                reason: widest_reason.clone(),
            });
            for kind in kinds {
                let len = encode(&DeliveryStatus {
                    message_id: id(),
                    kind,
                    mqtt: Some(MqttDelivery {
                        client: "ha".to_string(),
                        topic: topic.clone(),
                        qos: 2,
                        reason_code: Some(255),
                    }),
                })
                .len();
                assert!(len <= cap, "{len} > {cap} for topic {topic:?}");
            }
        }
        // The bound is tight: the widest reason at the allowance reaches it.
        let at_bound = encode(&DeliveryStatus {
            message_id: Uuid::max(),
            kind: DeliveryKind::Rejected {
                reason: "\\".repeat(DELIVERY_REASON_MAX_BYTES + DELIVERY_REASON_MARKER_ALLOWANCE),
            },
            mqtt: Some(MqttDelivery {
                client: "ha".to_string(),
                topic: "t".to_string(),
                qos: 2,
                reason_code: Some(255),
            }),
        });
        assert_eq!(at_bound.len(), max_encoded_len("ha", "t"));
    }
}

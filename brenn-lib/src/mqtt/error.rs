//! MQTT error types with LLM-facing string representations.

use std::fmt;
use std::time::Duration;

/// Errors surfaced from the MQTT subsystem to tool intercepts and the LLM.
///
/// Each variant maps 1:1 to one of the LLM-facing strings. The `Display` impl
/// produces that string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttError {
    /// The client session is not currently connected. `last_error` is the most
    /// recent failure reason, if any. App attribution stays at the call sites,
    /// which have it.
    NotConnected {
        client_slug: String,
        last_error: Option<String>,
    },
    /// The broker rejected a publish (PUBACK / PUBCOMP with failure reason code).
    BrokerRejected {
        topic: String,
        client_slug: String,
        reason: String,
    },
    /// The address does not start with `mqtt:` — direct to other tool.
    WrongProtocol { address: String },
    /// Wildcard (`+` or `#`) in a publish address.
    WildcardNotAllowed { address: String },
    /// `body_base64` is not a string, or not valid base64.
    BadBase64 { detail: String },
    /// `body` is a number, boolean or null.
    BadBodyShape,
    /// `content_type` is not a string, or is a string the bus's content-type
    /// rule refuses.
    BadBodyContentType { detail: String },
    /// `body` is an object with a top-level `binary_base64` key: the retired
    /// binary form, refused rather than published as JSON.
    RetiredBinaryBody,
    /// The MQTT address string is syntactically invalid.
    AddressInvalid { address: String, detail: String },
    /// The publish, encoded, is larger than the Maximum Packet Size the
    /// broker advertised for the current connection. Refused before submit.
    PacketTooLarge {
        client_slug: String,
        packet_bytes: usize,
        broker_max: u32,
    },
    /// The publish did not complete within the host's bound on the wait for
    /// the session: room on its request channel at QoS 0, or the broker's
    /// acknowledgement at QoS 1/2. The packet may already have been handed to
    /// the client library, so it may still be delivered.
    TimedOut {
        client_slug: String,
        waited: Duration,
    },
}

impl MqttError {
    /// A stable, coarse classification for guest-facing surfaces where the full
    /// `Display` must not leak host-internal topology.
    ///
    /// The `NotConnected` `Display` embeds the client slug and `last_error` — raw
    /// rumqttc connection-failure text (broker-derived TLS alert / OS error
    /// detail). An untrusted out-of-tree WASM guest that probes publishes during a
    /// broker outage must not harvest that; it gets only this coarse kind, while
    /// the full `Display` is logged host-side.
    ///
    /// The match is total via the catch-all so a future variant defaults to the
    /// safe `"publish failed"` kind rather than silently leaking its `Display`.
    pub fn coarse_kind(&self) -> &'static str {
        match self {
            MqttError::NotConnected { .. } => "not connected",
            MqttError::TimedOut { .. } => "timed out",
            _ => "publish failed",
        }
    }
}

impl fmt::Display for MqttError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MqttError::NotConnected {
                client_slug,
                last_error,
            } => {
                let reason = last_error.as_deref().unwrap_or("unknown");
                write!(
                    f,
                    "client '{client_slug}' is not connected — last error: {reason}",
                )
            }
            MqttError::BrokerRejected {
                topic,
                client_slug,
                reason,
            } => {
                write!(
                    f,
                    "publish to '{topic}' on client '{client_slug}' rejected by broker: {reason}",
                )
            }
            MqttError::WrongProtocol { address } => {
                write!(
                    f,
                    "address '{address}' is not an mqtt: address; use BrennSend/PwaPushSend",
                )
            }
            MqttError::WildcardNotAllowed { address } => {
                write!(
                    f,
                    "address '{address}' contains a wildcard; MqttSend requires a topic name",
                )
            }
            MqttError::BadBase64 { detail } => {
                write!(f, "invalid `body_base64` argument: {detail}")
            }
            MqttError::BadBodyShape => {
                write!(
                    f,
                    "invalid `body` argument: must be a string, an object or an array; a number, \
                     boolean or null is given as its JSON text in a string",
                )
            }
            MqttError::BadBodyContentType { detail } => {
                write!(f, "invalid `content_type` argument: {detail}")
            }
            MqttError::RetiredBinaryBody => {
                write!(
                    f,
                    "`body` is an object with a `binary_base64` key: bytes are given in the \
                     top-level `body_base64` argument, with `content_type` beside it. An object \
                     `body` is published as JSON text; a JSON payload that itself has a \
                     top-level `binary_base64` key is given as its text in `body_base64`",
                )
            }
            MqttError::AddressInvalid { address, detail } => {
                write!(f, "invalid mqtt address '{address}': {detail}")
            }
            MqttError::PacketTooLarge {
                client_slug,
                packet_bytes,
                broker_max,
            } => {
                write!(
                    f,
                    "publish on client '{client_slug}' is a {packet_bytes}-byte MQTT packet; \
                     the broker accepts at most {broker_max} bytes",
                )
            }
            MqttError::TimedOut {
                client_slug,
                waited,
            } => {
                write!(
                    f,
                    "the publish on MQTT client '{client_slug}' did not complete within {} s: \
                     the broker did not answer. The message may still be delivered; \
                     sending it again can deliver it twice.",
                    waited.as_secs(),
                )
            }
        }
    }
}

impl std::error::Error for MqttError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// `coarse_kind` for `NotConnected` must reveal none of the client slug nor
    /// the raw `last_error` text — that is the whole point of the coarsening.
    #[test]
    fn coarse_kind_not_connected_leaks_nothing() {
        let e = MqttError::NotConnected {
            client_slug: "internal-client".to_string(),
            last_error: Some(
                "I/O: tls handshake: alert: unknown ca (broker.internal:8883)".to_string(),
            ),
        };
        let coarse = e.coarse_kind();
        assert_eq!(coarse, "not connected");
        for leak in ["internal-client", "tls", "broker.internal", "8883"] {
            assert!(
                !coarse.contains(leak),
                "coarse kind must not leak {leak:?}: {coarse}"
            );
        }
    }

    #[test]
    fn coarse_kind_other_variants() {
        assert_eq!(
            MqttError::BrokerRejected {
                topic: "t".to_string(),
                client_slug: "c".to_string(),
                reason: "0x87".to_string(),
            }
            .coarse_kind(),
            "publish failed"
        );
    }

    #[test]
    fn packet_too_large_names_the_client_and_both_sizes() {
        let e = MqttError::PacketTooLarge {
            client_slug: "home".to_string(),
            packet_bytes: 1025,
            broker_max: 1024,
        };
        assert_eq!(
            e.to_string(),
            "publish on client 'home' is a 1025-byte MQTT packet; \
             the broker accepts at most 1024 bytes"
        );
        assert_eq!(e.coarse_kind(), "publish failed");
    }

    #[test]
    fn timed_out_names_the_client_and_the_wait_and_its_coarse_kind_leaks_neither() {
        let e = MqttError::TimedOut {
            client_slug: "home".to_string(),
            waited: Duration::from_secs(10),
        };
        assert_eq!(
            e.to_string(),
            "the publish on MQTT client 'home' did not complete within 10 s: \
             the broker did not answer. The message may still be delivered; \
             sending it again can deliver it twice."
        );
        assert_eq!(e.coarse_kind(), "timed out");
        assert!(!e.coarse_kind().contains("home"));
    }
}

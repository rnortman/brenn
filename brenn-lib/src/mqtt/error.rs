//! MQTT error types with LLM-facing string representations.

use std::fmt;

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
    /// Payload base64 is malformed.
    BadBase64 { detail: String },
    /// Payload shape is unrecognised (not string, not `{binary_base64, ...}`).
    BadBodyShape,
    /// The MQTT address string is syntactically invalid.
    AddressInvalid { address: String, detail: String },
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
                write!(f, "invalid binary_base64 in body: {detail}")
            }
            MqttError::BadBodyShape => {
                write!(
                    f,
                    "invalid `body` argument: must be a string or \
                     {{ binary_base64: string, content_type?: string }}",
                )
            }
            MqttError::AddressInvalid { address, detail } => {
                write!(f, "invalid mqtt address '{address}': {detail}")
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
}

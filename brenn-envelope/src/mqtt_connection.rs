//! MQTT connection-state bodies: what the host publishes to a declared
//! client's `brenn:mqtt-connection.<client>` channel.
//!
//! Every message is the client's whole state and subsumes the one before, so
//! the channel's newest message alone answers "is it connected" and "did the
//! session before this process end uncleanly, and when was that found". Every
//! body is a JSON object carrying `"v": MQTT_CONNECTION_VERSION`, stamped by
//! [`encode`] and checked by [`decode`]:
//!
//! ```json
//! {"v":1,"client":"ha","connection":"disconnected","last_error":"connection refused",
//!  "since_ms":1795600000000,
//!  "previous_session":{"end":"unclean","found_at_ms":1795599990000}}
//! ```
//!
//! The `connection` words and the `end` words are closed: a new one moves the
//! version. Unknown fields are ignored.
//!
//! Guest-safe: plain serde types, no clock, no I/O.

use serde::{Deserialize, Serialize};

use crate::versioned::TRUNCATION_MARKER_ALLOWANCE;

/// Schema version stamped into every body.
pub const MQTT_CONNECTION_VERSION: u32 = 1;

/// The `sender` of every connection-state message. A reader believes only
/// envelopes from this sender.
pub const MQTT_CONNECTION_SENDER: &str = "system:mqtt-connection";

/// Cap on `last_error` and `reason`, before the sanitizer's truncation marker.
pub const MQTT_CONNECTION_TEXT_MAX_BYTES: usize = 256;

/// One declared client's connection state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqttConnectionState {
    /// The declared `mqtt_client` this state is of.
    pub client: String,
    #[serde(flatten)]
    pub connection: Connection,
    /// When `connection` last changed kind, Unix milliseconds. A change of
    /// `last_error` alone does not move it.
    pub since_ms: i64,
    /// How the session before this process ended, fixed when this process
    /// opened the client and repeated on every message.
    pub previous_session: PreviousSession,
}

/// Whether the client is connected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "connection", rename_all = "snake_case")]
pub enum Connection {
    Connected,
    /// Not connected; the client is connecting or will retry.
    Disconnected {
        /// The broker's or the transport's words for the last failure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_error: Option<String>,
    },
    /// Stopped for good after an authoritative failure.
    Failed {
        /// The broker's or the transport's words for the failure.
        reason: String,
    },
    /// The session has stopped and hands nothing more: the server is shutting
    /// down, or the client was removed.
    Stopped,
}

/// The session immediately before this process, as this process found it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreviousSession {
    pub end: PreviousEnd,
    /// When this process opened the client and read the record, Unix
    /// milliseconds: at boot, or at the reload that added the client. Every
    /// hand-off of this process is later; every session that handed anything
    /// earlier has ended.
    pub found_at_ms: i64,
}

/// How the session immediately before this process ended. Speaks of that one
/// session only: a crash followed by a clean restart reads `clean`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreviousEnd {
    /// No record says how it ended, or no earlier session of this client
    /// handed anything this process would report.
    None,
    /// Its record ends in a host-written `stopped`.
    Clean,
    /// Its record does not end in `stopped`: the process ended without
    /// stopping the session, and senders were told.
    Unclean,
}

/// Why a body could not be decoded: the shared versioned codec's error.
pub type MqttConnectionDecodeError = crate::versioned::VersionedDecodeError;

/// Serialize a state, stamping `"v": MQTT_CONNECTION_VERSION`.
pub fn encode(state: &MqttConnectionState) -> String {
    crate::versioned::encode(MQTT_CONNECTION_VERSION, state)
}

/// Parse a state, requiring `v` equal to [`MQTT_CONNECTION_VERSION`].
pub fn decode(body: &str) -> Result<MqttConnectionState, MqttConnectionDecodeError> {
    crate::versioned::decode(MQTT_CONNECTION_VERSION, body)
}

/// The length of the longest body [`encode`] can produce for `client`: over
/// every `connection` word, with text of [`MQTT_CONNECTION_TEXT_MAX_BYTES`]
/// plus [`TRUNCATION_MARKER_ALLOWANCE`] bytes of the character JSON
/// escapes widest among those sanitized text holds, and the widest
/// timestamps.
pub fn max_encoded_len(client: &str) -> usize {
    let text = "\\".repeat(MQTT_CONNECTION_TEXT_MAX_BYTES + TRUNCATION_MARKER_ALLOWANCE);
    let connections = [
        Connection::Connected,
        Connection::Disconnected {
            last_error: Some(text.clone()),
        },
        Connection::Failed { reason: text },
        Connection::Stopped,
    ];
    connections
        .into_iter()
        .map(|connection| {
            encode(&MqttConnectionState {
                client: client.to_owned(),
                connection,
                since_ms: i64::MIN,
                previous_session: PreviousSession {
                    end: PreviousEnd::Unclean,
                    found_at_ms: i64::MIN,
                },
            })
            .len()
        })
        .max()
        .expect("the connection list is not empty")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn state(connection: Connection, end: PreviousEnd) -> MqttConnectionState {
        MqttConnectionState {
            client: "ha".to_string(),
            connection,
            since_ms: 1_795_600_000_000,
            previous_session: PreviousSession {
                end,
                found_at_ms: 1_795_599_990_000,
            },
        }
    }

    fn every_connection() -> Vec<Connection> {
        vec![
            Connection::Connected,
            Connection::Disconnected { last_error: None },
            Connection::Disconnected {
                last_error: Some("connection refused".to_string()),
            },
            Connection::Failed {
                reason: "Not authorized".to_string(),
            },
            Connection::Stopped,
        ]
    }

    /// The literals out-of-tree readers decode by. A rename here breaks every
    /// one of them, so the wire form of each word is pinned whole.
    #[test]
    fn each_connection_word_has_its_pinned_wire_form() {
        let wire = |connection: Connection, end: PreviousEnd| -> Value {
            serde_json::from_str(&encode(&state(connection, end))).unwrap()
        };
        let with = |fields: Value, end: &str| -> Value {
            let mut expected = json!({
                "v": 1, "client": "ha", "since_ms": 1_795_600_000_000_i64,
                "previous_session": {"end": end, "found_at_ms": 1_795_599_990_000_i64},
            });
            for (k, v) in fields.as_object().unwrap() {
                expected[k] = v.clone();
            }
            expected
        };
        assert_eq!(
            wire(Connection::Connected, PreviousEnd::None),
            with(json!({"connection": "connected"}), "none")
        );
        assert_eq!(
            wire(
                Connection::Disconnected { last_error: None },
                PreviousEnd::Clean
            ),
            with(json!({"connection": "disconnected"}), "clean")
        );
        assert_eq!(
            wire(
                Connection::Disconnected {
                    last_error: Some("connection refused".to_string())
                },
                PreviousEnd::Unclean
            ),
            with(
                json!({"connection": "disconnected", "last_error": "connection refused"}),
                "unclean"
            )
        );
        assert_eq!(
            wire(
                Connection::Failed {
                    reason: "Not authorized".to_string()
                },
                PreviousEnd::None
            ),
            with(
                json!({"connection": "failed", "reason": "Not authorized"}),
                "none"
            )
        );
        assert_eq!(
            wire(Connection::Stopped, PreviousEnd::Clean),
            with(json!({"connection": "stopped"}), "clean")
        );
    }

    #[test]
    fn every_state_round_trips() {
        for connection in every_connection() {
            for end in [PreviousEnd::None, PreviousEnd::Clean, PreviousEnd::Unclean] {
                let s = state(connection.clone(), end);
                assert_eq!(decode(&encode(&s)), Ok(s));
            }
        }
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let body = r#"{"v":1,"client":"ha","connection":"connected","added_later":3,
            "since_ms":5,"previous_session":{"end":"clean","found_at_ms":4,"also":true}}"#;
        assert_eq!(
            decode(body),
            Ok(MqttConnectionState {
                client: "ha".to_string(),
                connection: Connection::Connected,
                since_ms: 5,
                previous_session: PreviousSession {
                    end: PreviousEnd::Clean,
                    found_at_ms: 4,
                },
            })
        );
    }

    #[test]
    fn an_unknown_word_fails() {
        let body = |connection: &str, end: &str| {
            format!(
                r#"{{"v":1,"client":"ha","connection":"{connection}","since_ms":5,
                    "previous_session":{{"end":"{end}","found_at_ms":4}}}}"#
            )
        };
        assert!(decode(&body("stopped", "clean")).is_ok());
        assert!(matches!(
            decode(&body("teleported", "clean")),
            Err(MqttConnectionDecodeError::Payload(_))
        ));
        assert!(matches!(
            decode(&body("stopped", "abandoned")),
            Err(MqttConnectionDecodeError::Payload(_))
        ));
    }

    #[test]
    fn another_version_fails() {
        let body = encode(&state(Connection::Stopped, PreviousEnd::Clean))
            .replacen(r#""v":1"#, r#""v":2"#, 1);
        assert_eq!(
            decode(&body),
            Err(MqttConnectionDecodeError::UnsupportedVersion(2))
        );
    }

    #[test]
    fn no_encoding_exceeds_max_encoded_len() {
        // Text as a sanitizer leaves it: escapes are backslashes, the widest a
        // JSON string makes, then the truncation marker.
        let widest = format!(
            "{}…(truncated)",
            "\\".repeat(MQTT_CONNECTION_TEXT_MAX_BYTES)
        );
        assert!(widest.len() <= MQTT_CONNECTION_TEXT_MAX_BYTES + TRUNCATION_MARKER_ALLOWANCE);
        for client in ["ha", "home.v2", &"c".repeat(128)] {
            let cap = max_encoded_len(client);
            let mut connections = every_connection();
            connections.push(Connection::Disconnected {
                last_error: Some(widest.clone()),
            });
            connections.push(Connection::Failed {
                reason: widest.clone(),
            });
            for connection in connections {
                for (since_ms, found_at_ms) in [(0, 0), (i64::MAX, i64::MAX), (i64::MIN, i64::MIN)]
                {
                    let len = encode(&MqttConnectionState {
                        client: client.to_string(),
                        connection: connection.clone(),
                        since_ms,
                        previous_session: PreviousSession {
                            end: PreviousEnd::Unclean,
                            found_at_ms,
                        },
                    })
                    .len();
                    assert!(len <= cap, "{len} > {cap} for client {client:?}");
                }
            }
        }
    }
}

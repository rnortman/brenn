//! MQTT error types with LLM-facing string representations.

use std::fmt;

/// Errors surfaced from the MQTT subsystem to tool intercepts and the LLM.
///
/// Each variant maps 1:1 to one of the LLM-facing strings. The `Display` impl
/// produces that string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttError {
    /// The address does not start with `mqtt:` — direct to other tool.
    WrongProtocol { address: String },
    /// Wildcard (`+` or `#`) in a publish address.
    WildcardNotAllowed { address: String },
    /// `body_base64` is not a string, or not valid base64.
    BadBase64 { detail: String },
    /// `body` is JSON null.
    NullBody,
    /// `content_type` is not a string, or is a string the bus's content-type
    /// rule refuses.
    BadBodyContentType { detail: String },
    /// `body` is an object with a top-level `binary_base64` key: the retired
    /// binary form, refused rather than published as JSON.
    RetiredBinaryBody,
    /// The MQTT address string is syntactically invalid.
    AddressInvalid { address: String, detail: String },
}

impl fmt::Display for MqttError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            MqttError::NullBody => {
                write!(
                    f,
                    "`body` is null: an argument that is not used is left out, not given as \
                     null. To publish the text `null`, give its base64, `bnVsbA==`, in \
                     `body_base64`",
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
        }
    }
}

impl std::error::Error for MqttError {}

//! Webhook error types.

use std::fmt;

/// Errors from the webhook subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookError {
    /// The address does not start with `webhook:` — direct to other tool.
    WrongProtocol { address: String },
    /// The webhook address string is syntactically invalid.
    AddressInvalid { address: String, detail: String },
    /// A config or runtime error in the webhook subsystem.
    Internal { detail: String },
}

impl fmt::Display for WebhookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WebhookError::WrongProtocol { address } => {
                write!(
                    f,
                    "address {address:?} is not a webhook: address; use BrennSend/MqttSend/PwaPushSend",
                )
            }
            WebhookError::AddressInvalid { address, detail } => {
                write!(f, "invalid webhook address {address:?}: {detail}")
            }
            WebhookError::Internal { detail } => {
                write!(f, "webhook internal error: {detail}")
            }
        }
    }
}

impl std::error::Error for WebhookError {}

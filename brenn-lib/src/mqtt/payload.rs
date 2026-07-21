//! MQTT payload classification and encoding helpers.
//!
//! Outbound: converts a `serde_json::Value` body from `MqttSend` into raw bytes
//! + optional Content Type.
//!
//! Inbound: classifies raw bytes + optional Content Type into `InboundPayload`.

use base64ct::{Base64, Encoding as _};
use serde_json::Value;

use crate::mqtt::error::MqttError;

/// The decoded form of an inbound MQTT publish payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundPayload {
    /// Valid UTF-8 payload with no binary-indicating Content Type.
    Text(String),
    /// Binary payload or text with an explicit non-text Content Type.
    Binary {
        /// Base64-encoded bytes (standard alphabet with padding).
        base64: String,
        /// MQTT 5 Content Type property, if present.
        content_type: Option<String>,
    },
}

/// Classify an inbound publish into `InboundPayload`.
///
/// Decision rule (deterministic):
/// - If bytes are valid UTF-8 **and** (no `content_type` present **or**
///   `content_type.starts_with("text/")`) → `InboundPayload::Text`.
/// - Otherwise → `InboundPayload::Binary`.
pub fn classify_inbound(bytes: &[u8], content_type: Option<&str>) -> InboundPayload {
    let is_text = match std::str::from_utf8(bytes) {
        Err(_) => false,
        Ok(_) => {
            // Valid UTF-8. Now check content type.
            match content_type {
                None => true,
                Some(ct) => ct.starts_with("text/"),
            }
        }
    };

    if is_text {
        // SAFETY: we just validated it is valid UTF-8.
        InboundPayload::Text(std::str::from_utf8(bytes).unwrap().to_string())
    } else {
        InboundPayload::Binary {
            base64: Base64::encode_string(bytes),
            content_type: content_type.map(str::to_string),
        }
    }
}

/// Decoded outbound payload ready for `client.publish`.
#[derive(Debug)]
pub struct OutboundPayload {
    /// Raw bytes to publish.
    pub bytes: Vec<u8>,
    /// MQTT 5 `Content Type` publish property, if applicable.
    pub content_type: Option<String>,
}

/// Decode `MqttSend.body` into `OutboundPayload`.
///
/// Accepted forms:
/// - `Value::String(s)` → UTF-8 bytes, no Content Type.
/// - `Value::Object` with `binary_base64: string` (and optional `content_type: string`)
///   → base64-decoded bytes, Content Type forwarded.
///
/// Returns `Err(MqttError)` on bad base64 or unexpected shape.
pub fn decode_outbound_body(body: &Value) -> Result<OutboundPayload, MqttError> {
    match body {
        Value::String(s) => Ok(OutboundPayload {
            bytes: s.as_bytes().to_vec(),
            content_type: None,
        }),
        Value::Object(map) => {
            let b64 = map
                .get("binary_base64")
                .and_then(Value::as_str)
                .ok_or(MqttError::BadBodyShape)?;

            let bytes = Base64::decode_vec(b64).map_err(|e| MqttError::BadBase64 {
                detail: e.to_string(),
            })?;

            let content_type = map
                .get("content_type")
                .and_then(Value::as_str)
                .map(str::to_string);

            Ok(OutboundPayload {
                bytes,
                content_type,
            })
        }
        _ => Err(MqttError::BadBodyShape),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- classify_inbound ---

    #[test]
    fn valid_utf8_no_content_type_is_text() {
        let result = classify_inbound(b"hello world", None);
        assert_eq!(result, InboundPayload::Text("hello world".to_string()));
    }

    #[test]
    fn valid_utf8_text_content_type_is_text() {
        let result = classify_inbound(b"hello", Some("text/plain"));
        assert_eq!(result, InboundPayload::Text("hello".to_string()));
    }

    #[test]
    fn valid_utf8_text_html_content_type_is_text() {
        let result = classify_inbound(b"<h1>hi</h1>", Some("text/html; charset=utf-8"));
        assert_eq!(result, InboundPayload::Text("<h1>hi</h1>".to_string()),);
    }

    #[test]
    fn valid_utf8_application_json_content_type_is_binary() {
        // content_type doesn't start with "text/" → Binary path.
        let result = classify_inbound(b"{}", Some("application/json"));
        // Verify both the variant and the content_type passthrough (test-3).
        match result {
            InboundPayload::Binary { content_type, .. } => {
                assert_eq!(
                    content_type.as_deref(),
                    Some("application/json"),
                    "content_type must be passed through to the Binary variant"
                );
            }
            InboundPayload::Text(_) => panic!("expected Binary, got Text"),
        }
    }

    #[test]
    fn invalid_utf8_no_content_type_is_binary() {
        let result = classify_inbound(&[0xFF, 0xFE], None);
        assert!(matches!(result, InboundPayload::Binary { .. }));
    }

    #[test]
    fn invalid_utf8_text_content_type_still_binary() {
        // UTF-8 check fails; conjunctive rule → Binary.
        let result = classify_inbound(&[0xFF, 0xFE], Some("text/plain"));
        assert!(matches!(result, InboundPayload::Binary { .. }));
    }

    #[test]
    fn empty_bytes_no_content_type_is_text() {
        let result = classify_inbound(b"", None);
        assert_eq!(result, InboundPayload::Text(String::new()));
    }

    #[test]
    fn binary_content_type_produces_correct_base64() {
        let bytes = b"\x00\x01\x02";
        let result = classify_inbound(bytes, Some("application/octet-stream"));
        match result {
            InboundPayload::Binary {
                base64,
                content_type,
            } => {
                let decoded = Base64::decode_vec(&base64).unwrap();
                assert_eq!(decoded, bytes);
                assert_eq!(content_type.as_deref(), Some("application/octet-stream"));
            }
            InboundPayload::Text(_) => panic!("expected Binary"),
        }
    }

    // --- decode_outbound_body ---

    #[test]
    fn string_body_produces_utf8_bytes() {
        let body = json!("hello");
        let out = decode_outbound_body(&body).unwrap();
        assert_eq!(out.bytes, b"hello");
        assert!(out.content_type.is_none());
    }

    #[test]
    fn binary_base64_body_decoded_correctly() {
        // "AAEC" is base64 for [0x00, 0x01, 0x02].
        let body = json!({ "binary_base64": "AAEC" });
        let out = decode_outbound_body(&body).unwrap();
        assert_eq!(out.bytes, &[0x00, 0x01, 0x02]);
        assert!(out.content_type.is_none());
    }

    #[test]
    fn binary_base64_body_with_content_type() {
        let body = json!({ "binary_base64": "AAEC", "content_type": "application/octet-stream" });
        let out = decode_outbound_body(&body).unwrap();
        assert_eq!(out.bytes, &[0x00, 0x01, 0x02]);
        assert_eq!(
            out.content_type.as_deref(),
            Some("application/octet-stream")
        );
    }

    #[test]
    fn bad_base64_returns_error() {
        let body = json!({ "binary_base64": "not-valid-base64!!!" });
        let err = decode_outbound_body(&body).unwrap_err();
        assert!(matches!(err, MqttError::BadBase64 { .. }));
    }

    #[test]
    fn null_body_returns_bad_shape() {
        let body = json!(null);
        let err = decode_outbound_body(&body).unwrap_err();
        assert!(matches!(err, MqttError::BadBodyShape));
    }

    #[test]
    fn number_body_returns_bad_shape() {
        let body = json!(42);
        let err = decode_outbound_body(&body).unwrap_err();
        assert!(matches!(err, MqttError::BadBodyShape));
    }

    #[test]
    fn object_without_binary_base64_returns_bad_shape() {
        let body = json!({ "content_type": "text/plain" });
        let err = decode_outbound_body(&body).unwrap_err();
        assert!(matches!(err, MqttError::BadBodyShape));
    }
}

//! MQTT payload classification and encoding helpers.
//!
//! Outbound: converts a `serde_json::Value` body from `MqttSend` into raw bytes
//! + optional Content Type.
//!
//! Inbound: `InboundPayload` carries the broker's bytes and Content Type;
//! `classify_inbound` reads them for the default ingress shape.

use base64ct::{Base64, Encoding as _};
use bytes::Bytes;
use serde_json::Value;

use brenn_lib::mqtt::error::MqttError;

/// An inbound MQTT publish payload as the broker delivered it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundPayload {
    /// The payload bytes, shared with the broker packet rather than copied. A
    /// bus body built from them is a copy of its own.
    pub bytes: Bytes,
    /// MQTT 5 Content Type property, if present.
    pub content_type: Option<String>,
}

impl InboundPayload {
    pub fn new(bytes: impl Into<Bytes>, content_type: Option<String>) -> Self {
        Self {
            bytes: bytes.into(),
            content_type,
        }
    }
}

/// The default ingress shape's reading of an inbound payload; see
/// [`classify_inbound`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundClass<'a> {
    /// Valid UTF-8 with no binary-indicating Content Type.
    Text(&'a str),
    /// Not UTF-8, or text with a Content Type not starting with `text/`.
    Binary,
}

/// Classify an inbound payload for the default (`MqttEnvelope`) ingress shape.
///
/// Decision rule (deterministic):
/// - If bytes are valid UTF-8 **and** (no `content_type` present **or**
///   `content_type.starts_with("text/")`) → [`InboundClass::Text`].
/// - Otherwise → [`InboundClass::Binary`].
///
/// The raw ingress shape does not use this rule; the bus's own content-type
/// rule types its body.
// TODO(mqtt-ingress-text-rule): this rule is narrower than the bus's
// `is_text_content_type` (no `application/json`, case-sensitive).
pub fn classify_inbound<'a>(bytes: &'a [u8], content_type: Option<&str>) -> InboundClass<'a> {
    match std::str::from_utf8(bytes) {
        Ok(text) if content_type.is_none_or(|ct| ct.starts_with("text/")) => {
            InboundClass::Text(text)
        }
        _ => InboundClass::Binary,
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

// TODO(mqtt-send-binary-base64): take a binary payload as an attachment of
// the call, or document `binary_base64` as the contract.
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
        assert_eq!(result, InboundClass::Text("hello world"));
    }

    #[test]
    fn valid_utf8_text_content_type_is_text() {
        let result = classify_inbound(b"hello", Some("text/plain"));
        assert_eq!(result, InboundClass::Text("hello"));
    }

    #[test]
    fn valid_utf8_text_html_content_type_is_text() {
        let result = classify_inbound(b"<h1>hi</h1>", Some("text/html; charset=utf-8"));
        assert_eq!(result, InboundClass::Text("<h1>hi</h1>"));
    }

    #[test]
    fn valid_utf8_application_json_content_type_is_binary() {
        // content_type doesn't start with "text/" → Binary path.
        let result = classify_inbound(b"{}", Some("application/json"));
        assert_eq!(result, InboundClass::Binary);
    }

    #[test]
    fn invalid_utf8_no_content_type_is_binary() {
        let result = classify_inbound(&[0xFF, 0xFE], None);
        assert_eq!(result, InboundClass::Binary);
    }

    #[test]
    fn invalid_utf8_text_content_type_still_binary() {
        // UTF-8 check fails; conjunctive rule → Binary.
        let result = classify_inbound(&[0xFF, 0xFE], Some("text/plain"));
        assert_eq!(result, InboundClass::Binary);
    }

    #[test]
    fn empty_bytes_no_content_type_is_text() {
        let result = classify_inbound(b"", None);
        assert_eq!(result, InboundClass::Text(""));
    }

    #[test]
    fn binary_content_type_is_binary() {
        let result = classify_inbound(b"\x00\x01\x02", Some("application/octet-stream"));
        assert_eq!(result, InboundClass::Binary);
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

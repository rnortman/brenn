//! MQTT payload classification and encoding helpers.
//!
//! Outbound: converts `MqttSend`'s `body` or `body_base64` argument, with its
//! `content_type`, into raw bytes + optional Content Type.
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

/// The one inline payload argument an `MqttSend` call gives.
#[derive(Debug, Clone, Copy)]
pub enum OutboundArg<'a> {
    /// `body`: a string, an object or an array.
    Body(&'a Value),
    /// `body_base64`: the base64 text of the bytes to publish.
    Base64(&'a Value),
}

/// Decode an `MqttSend` inline payload argument and its `content_type` into
/// `OutboundPayload`.
///
/// - `body` as a string → its UTF-8 bytes, verbatim.
/// - `body` as an object or array → its JSON text from `serde_json::to_vec`:
///   compact, with no promise about key order or number spelling. An object
///   with a top-level `binary_base64` key is refused whatever else it holds,
///   so a caller of the retired binary object form never publishes base64
///   text as JSON.
/// - `body` as a number, boolean or null → refused.
/// - `body_base64` → the decoded bytes.
///
/// `content_type`, when given, must be a string the bus's content-type rule
/// admits ([`brenn_envelope::body::check_content_type`]) and is forwarded as
/// the MQTT 5 Content Type with either argument. None is added otherwise.
pub fn decode_outbound(
    arg: OutboundArg<'_>,
    content_type: Option<&Value>,
) -> Result<OutboundPayload, MqttError> {
    let bytes = match arg {
        OutboundArg::Body(Value::String(s)) => s.as_bytes().to_vec(),
        OutboundArg::Body(Value::Object(map)) if map.contains_key("binary_base64") => {
            return Err(MqttError::RetiredBinaryBody);
        }
        OutboundArg::Body(value @ (Value::Object(_) | Value::Array(_))) => {
            serde_json::to_vec(value).expect("a serde_json::Value serializes")
        }
        OutboundArg::Body(_) => return Err(MqttError::BadBodyShape),
        OutboundArg::Base64(Value::String(b64)) => {
            Base64::decode_vec(b64).map_err(|e| MqttError::BadBase64 {
                detail: e.to_string(),
            })?
        }
        OutboundArg::Base64(_) => {
            return Err(MqttError::BadBase64 {
                detail: "must be a string".to_string(),
            });
        }
    };

    let content_type = match content_type {
        None => None,
        Some(Value::String(ct)) => {
            brenn_envelope::body::check_content_type(ct).map_err(|e| {
                MqttError::BadBodyContentType {
                    detail: e.to_string(),
                }
            })?;
            Some(ct.clone())
        }
        Some(_) => {
            return Err(MqttError::BadBodyContentType {
                detail: "must be a string".to_string(),
            });
        }
    };

    Ok(OutboundPayload {
        bytes,
        content_type,
    })
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

    // --- decode_outbound ---

    fn body(value: &Value) -> Result<OutboundPayload, MqttError> {
        decode_outbound(OutboundArg::Body(value), None)
    }

    #[test]
    fn string_body_produces_its_utf8_bytes_verbatim() {
        let out = body(&json!("hello")).unwrap();
        assert_eq!(out.bytes, b"hello");
        assert!(out.content_type.is_none());
    }

    #[test]
    fn string_body_holding_json_text_keeps_its_spelling() {
        let text = r#"{ "b": 1.50,  "a": true }"#;
        let out = body(&json!(text)).unwrap();
        assert_eq!(out.bytes, text.as_bytes());
        assert!(out.content_type.is_none());
    }

    #[test]
    fn object_body_publishes_json_text_of_the_same_value() {
        let value = json!({ "action": "turn_on", "level": [1, 2] });
        let out = body(&value).unwrap();
        let parsed: Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(parsed, value);
        assert!(out.content_type.is_none());
    }

    #[test]
    fn array_body_publishes_json_text_of_the_same_value() {
        let value = json!([{ "a": 1 }, "two", null]);
        let out = body(&value).unwrap();
        let parsed: Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(parsed, value);
    }

    #[test]
    fn scalar_bodies_are_refused() {
        for value in [json!(null), json!(42), json!(1.5), json!(true)] {
            assert_eq!(
                body(&value).unwrap_err(),
                MqttError::BadBodyShape,
                "{value}"
            );
        }
    }

    #[test]
    fn the_retired_binary_object_is_refused_whatever_else_it_holds() {
        for value in [
            json!({ "binary_base64": "AAEC" }),
            json!({ "binary_base64": "AAEC", "content_type": "image/png" }),
            json!({ "binary_base64": 7, "other": "x" }),
        ] {
            assert_eq!(
                body(&value).unwrap_err(),
                MqttError::RetiredBinaryBody,
                "{value}"
            );
        }
    }

    #[test]
    fn a_nested_binary_base64_key_is_ordinary_json() {
        let value = json!({ "frame": { "binary_base64": "AAEC" } });
        let out = body(&value).unwrap();
        let parsed: Value = serde_json::from_slice(&out.bytes).unwrap();
        assert_eq!(parsed, value);
    }

    #[test]
    fn body_base64_is_decoded() {
        // "AAEC" is base64 for [0x00, 0x01, 0x02].
        let out = decode_outbound(OutboundArg::Base64(&json!("AAEC")), None).unwrap();
        assert_eq!(out.bytes, &[0x00, 0x01, 0x02]);
        assert!(out.content_type.is_none());
    }

    #[test]
    fn body_base64_with_content_type() {
        let out = decode_outbound(
            OutboundArg::Base64(&json!("AAEC")),
            Some(&json!("application/octet-stream")),
        )
        .unwrap();
        assert_eq!(out.bytes, &[0x00, 0x01, 0x02]);
        assert_eq!(
            out.content_type.as_deref(),
            Some("application/octet-stream")
        );
    }

    #[test]
    fn body_with_content_type() {
        let out = decode_outbound(
            OutboundArg::Body(&json!({ "on": true })),
            Some(&json!("application/json")),
        )
        .unwrap();
        assert_eq!(out.bytes, br#"{"on":true}"#);
        assert_eq!(out.content_type.as_deref(), Some("application/json"));
    }

    #[test]
    fn bad_base64_returns_error() {
        let err =
            decode_outbound(OutboundArg::Base64(&json!("not-valid-base64!!!")), None).unwrap_err();
        assert!(matches!(err, MqttError::BadBase64 { .. }));
        assert!(
            err.to_string()
                .starts_with("invalid `body_base64` argument: "),
            "{err}"
        );
    }

    #[test]
    fn non_string_body_base64_is_refused() {
        let err = decode_outbound(OutboundArg::Base64(&json!({ "x": 1 })), None).unwrap_err();
        assert_eq!(
            err,
            MqttError::BadBase64 {
                detail: "must be a string".to_string()
            }
        );
    }

    #[test]
    fn non_string_content_type_is_refused() {
        let err =
            decode_outbound(OutboundArg::Base64(&json!("AAEC")), Some(&json!(7))).unwrap_err();
        assert_eq!(
            err,
            MqttError::BadBodyContentType {
                detail: "must be a string".to_string()
            }
        );
        assert_eq!(
            err.to_string(),
            "invalid `content_type` argument: must be a string"
        );
    }

    #[test]
    fn content_type_the_bus_refuses_is_refused() {
        let err = decode_outbound(
            OutboundArg::Body(&json!("on")),
            Some(&json!("not a media type")),
        )
        .unwrap_err();
        let MqttError::BadBodyContentType { detail } = &err else {
            panic!("expected BadBodyContentType, got {err:?}");
        };
        assert!(detail.contains("\"not a media type\""), "{detail}");
        assert!(
            err.to_string()
                .starts_with("invalid `content_type` argument: invalid content type"),
            "{err}"
        );
    }
}

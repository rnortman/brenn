//! The arguments of an MQTT publish, as `MqttSend` and an automation's
//! `mqtt_publish` action both take them. This module is the one reader of
//! [`KEYS`] in a tool's input; each `Err` is the text the caller answers with.

use std::num::NonZeroU32;

use brenn_lib::mqtt::MqttLifetime;
use brenn_lib::mqtt::address::{MqttAddress, parse_topic_name};
use brenn_mqtt::payload::{OutboundArg, OutboundPayload, decode_outbound};

use crate::messaging_intercept;

/// Every argument name an MQTT publish takes.
pub(crate) const KEYS: [&str; 9] = [
    "to",
    "body",
    "body_base64",
    "content_type",
    "attachment",
    "qos",
    "retain",
    "lifetime_s",
    "status_to",
];

/// How `lifetime_s` is given, for every rejection of it.
const LIFETIME_FORMS: &str =
    "give a whole number of seconds from 1 to 4294967295, or the string \"forever\"";

/// One MQTT publish's arguments, parsed.
#[derive(Debug)]
pub(crate) struct MqttPublishArgs<'a> {
    /// The target address as given.
    pub(crate) to: &'a str,
    pub(crate) address: MqttAddress,
    pub(crate) payload: ArgPayload<'a>,
    pub(crate) qos: u8,
    pub(crate) retain: bool,
    pub(crate) lifetime: MqttLifetime,
    pub(crate) status_to: Option<String>,
}

/// The payload as the arguments give it.
#[derive(Debug)]
pub(crate) enum ArgPayload<'a> {
    /// `body` or `body_base64`, decoded, with the `content_type` given.
    Inline(OutboundPayload),
    /// An `attachment` reference, not yet admitted.
    Attachment { message_id: &'a str, name: &'a str },
}

/// The `to` argument: present, a string and not empty.
pub(crate) fn target(args: &serde_json::Value) -> Result<&str, String> {
    match args.get("to").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => Ok(s),
        _ => Err("missing or empty `to` argument".to_string()),
    }
}

/// Every argument of an MQTT publish, in this order: `to`, the address, the
/// payload, `qos`, `retain`, `lifetime_s`, `status_to`. The first refusal is
/// returned.
pub(crate) fn parse(args: &serde_json::Value) -> Result<MqttPublishArgs<'_>, String> {
    let to = target(args)?;
    let address = parse_topic_name(to).map_err(|e| e.to_string())?;
    let payload = payload(args)?;
    let qos = match args.get("qos") {
        None => 1,
        Some(v) => match v.as_u64().and_then(|n| u8::try_from(n).ok()) {
            Some(qos) if qos <= 2 => qos,
            _ => return Err("invalid `qos`: must be 0, 1, or 2".to_string()),
        },
    };
    let retain = match args.get("retain") {
        None => false,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(_) => return Err("invalid `retain`: must be true or false".to_string()),
    };
    let lifetime = lifetime(args)?;
    let status_to = match args.get("status_to") {
        None => None,
        Some(serde_json::Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(_) => {
            return Err(
                "invalid `status_to` argument: give a `brenn:<channel>` address as a string"
                    .to_string(),
            );
        }
    };
    Ok(MqttPublishArgs {
        to,
        address,
        payload,
        qos,
        retain,
        lifetime,
        status_to,
    })
}

/// The one payload argument of `body`, `body_base64` and `attachment`, with
/// `content_type`.
fn payload(args: &serde_json::Value) -> Result<ArgPayload<'_>, String> {
    let content_type = args.get("content_type");
    match (
        args.get("body"),
        args.get("body_base64"),
        args.get("attachment"),
    ) {
        (Some(body), None, None) => decode_outbound(OutboundArg::Body(body), content_type)
            .map(ArgPayload::Inline)
            .map_err(|e| e.to_string()),
        (None, Some(b64), None) => decode_outbound(OutboundArg::Base64(b64), content_type)
            .map(ArgPayload::Inline)
            .map_err(|e| e.to_string()),
        (None, None, Some(reference)) => {
            if content_type.is_some() {
                return Err(
                    "`content_type` is given with `attachment`: an attachment publishes its \
                     own recorded content type; drop `content_type`"
                        .to_string(),
                );
            }
            let Some((message_id, name)) = messaging_intercept::attachment_reference(reference)
            else {
                return Err(format!(
                    "`attachment` must be {}",
                    messaging_intercept::ATTACHMENT_REFERENCE_SHAPE
                ));
            };
            Ok(ArgPayload::Attachment { message_id, name })
        }
        (None, None, None) => Err(
            "missing payload: give exactly one of `body`, `body_base64` and `attachment`"
                .to_string(),
        ),
        _ => Err(
            "more than one payload argument is given: give exactly one of `body`, \
                  `body_base64` and `attachment`"
                .to_string(),
        ),
    }
}

/// The required `lifetime_s` argument: whole seconds from 1 to `u32::MAX`, or
/// `"forever"`. The `Err` text names both forms.
fn lifetime(args: &serde_json::Value) -> Result<MqttLifetime, String> {
    let Some(value) = args.get("lifetime_s") else {
        return Err(format!("missing `lifetime_s` argument: {LIFETIME_FORMS}"));
    };
    let lifetime = match value {
        serde_json::Value::String(s) if s == "forever" => Some(MqttLifetime::Forever),
        serde_json::Value::Number(n) => n
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .and_then(NonZeroU32::new)
            .map(MqttLifetime::Seconds),
        _ => None,
    };
    lifetime.ok_or_else(|| format!("invalid `lifetime_s` argument: {LIFETIME_FORMS}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A complete argument set on `mqtt:home:cmd/light` with `fields` laid over
    /// it; a `null` field removes the key.
    fn args(fields: serde_json::Value) -> serde_json::Value {
        let mut v = json!({ "to": "mqtt:home:cmd/light", "body": "on", "lifetime_s": 60 });
        for (k, field) in fields.as_object().unwrap() {
            if field.is_null() {
                v.as_object_mut().unwrap().remove(k);
            } else {
                v[k] = field.clone();
            }
        }
        v
    }

    fn refusal(fields: serde_json::Value) -> String {
        parse(&args(fields)).expect_err("refused")
    }

    #[test]
    fn target_refuses_a_missing_non_string_or_empty_to() {
        for input in [json!({}), json!({ "to": 7 }), json!({ "to": "" })] {
            assert_eq!(
                target(&input),
                Err("missing or empty `to` argument".to_string()),
                "{input}"
            );
        }
        assert_eq!(target(&json!({ "to": "brenn:x" })), Ok("brenn:x"));
    }

    #[test]
    fn parse_reads_to_itself_and_refuses_it_first() {
        assert_eq!(
            refusal(json!({ "to": null, "qos": 9 })),
            "missing or empty `to` argument"
        );
    }

    #[test]
    fn defaults_are_qos_1_unretained_with_no_status_to() {
        let input = args(json!({}));
        let parsed = parse(&input).unwrap();
        assert_eq!(parsed.to, "mqtt:home:cmd/light");
        assert_eq!(
            (
                parsed.address.client.as_str(),
                parsed.address.topic.as_str()
            ),
            ("home", "cmd/light")
        );
        assert_eq!((parsed.qos, parsed.retain), (1, false));
        assert_eq!(
            parsed.lifetime,
            MqttLifetime::Seconds(NonZeroU32::new(60).unwrap())
        );
        assert_eq!(parsed.status_to, None);
        let ArgPayload::Inline(p) = parsed.payload else {
            panic!("inline payload expected");
        };
        assert_eq!((p.bytes.as_slice(), p.content_type), (&b"on"[..], None));
    }

    #[test]
    fn each_payload_form_parses() {
        let cases: [(serde_json::Value, &[u8], Option<&str>); 4] = [
            (json!({ "body": "on" }), b"on", None),
            (
                json!({ "body": { "state": "ON" }, "content_type": "application/json" }),
                br#"{"state":"ON"}"#,
                Some("application/json"),
            ),
            (json!({ "body": [1, 2] }), b"[1,2]", None),
            (
                json!({ "body": null, "body_base64": "/wA=" }),
                &[0xff, 0x00],
                None,
            ),
        ];
        for (fields, bytes, content_type) in cases {
            let ArgPayload::Inline(p) = parse(&args(fields.clone())).unwrap().payload else {
                panic!("{fields}: inline payload expected");
            };
            assert_eq!(p.bytes, bytes, "{fields}");
            assert_eq!(p.content_type.as_deref(), content_type, "{fields}");
        }
        let reference =
            json!({ "body": null, "attachment": { "message_id": "m-1", "name": "a.bin" } });
        match parse(&args(reference.clone())) {
            Ok(MqttPublishArgs {
                payload: ArgPayload::Attachment { message_id, name },
                ..
            }) => assert_eq!((message_id, name), ("m-1", "a.bin")),
            other => panic!("{reference}: {other:?}"),
        }
    }

    #[test]
    fn payload_shape_errors_keep_their_texts() {
        assert_eq!(
            refusal(json!({ "body": null })),
            "missing payload: give exactly one of `body`, `body_base64` and `attachment`"
        );
        assert_eq!(
            refusal(json!({ "body_base64": "AA==" })),
            "more than one payload argument is given: give exactly one of `body`, \
             `body_base64` and `attachment`"
        );
        assert_eq!(
            refusal(json!({
                "body": null,
                "attachment": { "message_id": "m-1", "name": "a.bin" },
                "content_type": "text/plain",
            })),
            "`content_type` is given with `attachment`: an attachment publishes its own \
             recorded content type; drop `content_type`"
        );
        assert!(
            refusal(json!({ "body": null, "attachment": 3 })).starts_with("`attachment` must be "),
        );
        assert_eq!(
            refusal(json!({ "body": 5 })),
            "invalid `body` argument: must be a string, an object or an array; a number, \
             boolean or null is given as its JSON text in a string"
        );
        let bad_base64 = refusal(json!({ "body": null, "body_base64": "!!" }));
        assert!(
            bad_base64.starts_with("invalid `body_base64` argument: "),
            "{bad_base64}"
        );
        assert_eq!(
            refusal(json!({ "content_type": 5 })),
            "invalid `content_type` argument: must be a string"
        );
    }

    #[test]
    fn qos_retain_lifetime_and_status_to() {
        let input = args(json!({
            "qos": 2,
            "retain": true,
            "lifetime_s": "forever",
            "status_to": "brenn:light-status",
        }));
        let parsed = parse(&input).unwrap();
        assert_eq!((parsed.qos, parsed.retain), (2, true));
        assert_eq!(parsed.lifetime, MqttLifetime::Forever);
        assert_eq!(parsed.status_to.as_deref(), Some("brenn:light-status"));

        for qos in [json!(3), json!(258), json!(-1), json!("1")] {
            assert_eq!(
                refusal(json!({ "qos": qos })),
                "invalid `qos`: must be 0, 1, or 2"
            );
        }
        for retain in [json!("true"), json!(1), json!({})] {
            assert_eq!(
                refusal(json!({ "retain": retain })),
                "invalid `retain`: must be true or false"
            );
        }
        assert_eq!(
            refusal(json!({ "lifetime_s": null })),
            "missing `lifetime_s` argument: give a whole number of seconds from 1 to \
             4294967295, or the string \"forever\""
        );
        for lifetime in [json!(0), json!(1.5), json!(4_294_967_296_u64), json!("60")] {
            assert_eq!(
                refusal(json!({ "lifetime_s": lifetime })),
                "invalid `lifetime_s` argument: give a whole number of seconds from 1 to \
                 4294967295, or the string \"forever\""
            );
        }
        for status_to in [json!(""), json!(7)] {
            assert_eq!(
                refusal(json!({ "status_to": status_to })),
                "invalid `status_to` argument: give a `brenn:<channel>` address as a string"
            );
        }
    }

    #[test]
    fn a_non_mqtt_address_is_refused_by_the_address_parser() {
        let err = refusal(json!({ "to": "brenn:channel" }));
        assert_eq!(
            err,
            parse_topic_name("brenn:channel").unwrap_err().to_string()
        );
    }
}

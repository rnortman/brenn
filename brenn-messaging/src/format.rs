//! Pure LLM-facing text formatters for messaging.
//!
//! These are used in two places:
//! - immediate single-message delivery (active bridge)
//! - multi-message wake drain
//!
//! The browser-facing HTML formatter lives in the binary crate
//! (`brenn/src/tools/messaging.rs`) because it depends on
//! `brenn::markdown::render_markdown`.

use serde::Serialize;
use serde::ser::SerializeStruct;

use super::{ChannelScheme, MessageEnvelope};
use brenn_lib::messaging::{BodyShape, MessageBody};

/// Heading line that introduces the messaging sub-block in a wake drain.
pub const MESSAGING_BATCH_HEADING: &str = "[Brenn messages]";

/// Heading line that introduces a single immediate-delivery message.
pub const MESSAGING_SINGLE_HEADING: &str = "[Brenn message]";

/// Heading line for a batch of `webhook:` transport messages.
pub const WEBHOOK_BATCH_HEADING: &str = "[Webhook messages]";

/// Heading line for a single immediate `webhook:` transport message.
pub const WEBHOOK_SINGLE_HEADING: &str = "[Webhook message]";

/// Heading line for a batch of `mqtt:` transport messages.
pub const MQTT_BATCH_HEADING: &str = "[MQTT messages]";

/// Heading line for a single immediate `mqtt:` transport message.
pub const MQTT_SINGLE_HEADING: &str = "[MQTT message]";

/// Separator between the heading line and the JSON body in messaging event text.
pub const MESSAGING_HEADING_SEPARATOR: &str = "\n\n";

pub fn single_heading(env: &MessageEnvelope) -> &'static str {
    match env.envelope_type {
        ChannelScheme::Webhook => WEBHOOK_SINGLE_HEADING,
        ChannelScheme::Mqtt => MQTT_SINGLE_HEADING,
        _ => MESSAGING_SINGLE_HEADING,
    }
}

pub fn batch_heading(envelopes: &[MessageEnvelope]) -> &'static str {
    let heading = match envelopes.first() {
        Some(env) if env.envelope_type == ChannelScheme::Webhook => WEBHOOK_BATCH_HEADING,
        Some(env) if env.envelope_type == ChannelScheme::Mqtt => MQTT_BATCH_HEADING,
        _ => MESSAGING_BATCH_HEADING,
    };
    // Assert that all envelopes in the batch share the same transport type.
    // Mixed-transport batches are not currently possible (a subscriber is
    // subscribed to channels of one transport type at a time), so a mismatch
    // indicates an invariant violation — fail fast rather than silently
    // mislabel the LLM-facing output.
    debug_assert!(
        envelopes
            .iter()
            .all(|e| e.envelope_type == envelopes.first().unwrap().envelope_type),
        "batch_heading: mixed-transport batch detected — all envelopes must share the same \
         envelope_type; got: {:?}",
        envelopes
            .iter()
            .map(|e| e.envelope_type)
            .collect::<Vec<_>>()
    );
    heading
}

/// The conversation shape: how a conversation is shown a message.
///
/// A text body is its text, followed by `content_type` when it has one. A
/// non-text body is never shown as bytes: `body` is the placeholder
/// `{"binary":true,"content_type":…,"size":…}`, which no text body can forge
/// because a text body is always a JSON string.
pub struct ConversationShape;

impl BodyShape for ConversationShape {
    fn serialize_body<S: SerializeStruct>(body: &MessageBody, out: &mut S) -> Result<(), S::Error> {
        match body.as_text() {
            Some(text) => {
                out.serialize_field("body", text)?;
                if let Some(content_type) = body.content_type() {
                    out.serialize_field("content_type", content_type)?;
                }
                Ok(())
            }
            // TODO(llm-sees-binary-content): a non-text body reaches a
            // conversation only as this metadata placeholder.
            None => out.serialize_field(
                "body",
                &BinaryPlaceholder {
                    binary: true,
                    content_type: body
                        .content_type()
                        .expect("a non-text body always has a content type"),
                    size: body.len(),
                },
            ),
        }
    }
}

#[derive(Serialize)]
struct BinaryPlaceholder<'a> {
    binary: bool,
    content_type: &'a str,
    size: usize,
}

/// A run of envelopes serialized as a JSON array in the conversation shape.
pub struct ConversationEnvelopes<'a>(pub &'a [MessageEnvelope]);

impl Serialize for ConversationEnvelopes<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|e| e.shaped::<ConversationShape>()))
    }
}

/// Format a single envelope as the LLM-facing single-message text.
/// Used by the immediate dispatch path when the target bridge is active.
///
/// Output shape:
///
/// ```text
/// [Brenn message]
///
/// {"message_id":"...","channel":"brenn:...", ...}
/// ```
pub fn format_messaging_event_single(env: &MessageEnvelope) -> String {
    let heading = single_heading(env);
    let json = serde_json::to_string(&env.shaped::<ConversationShape>())
        .expect("the conversation shape serializes every body");
    format!("{heading}{MESSAGING_HEADING_SEPARATOR}{json}")
}

/// Format a list of envelopes as the LLM-facing batch sub-block. Returns
/// `None` if the list is empty (so the drain path can skip emission).
///
/// Output shape:
///
/// ```text
/// [Brenn messages]
///
/// [{...}, {...}]
/// ```
///
/// Single-message drains use the batch heading with a one-element JSON array —
/// keeping the LLM-facing structure uniform regardless of message count, per the
/// MVP requirement for single-batch multi-message delivery.
pub fn format_messaging_event_batch(envelopes: &[MessageEnvelope]) -> Option<String> {
    if envelopes.is_empty() {
        return None;
    }
    let heading = batch_heading(envelopes);
    let json = serde_json::to_string(&ConversationEnvelopes(envelopes))
        .expect("the conversation shape serializes every body");
    Some(format!("{heading}{MESSAGING_HEADING_SEPARATOR}{json}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Urgency;
    use chrono::Utc;
    use uuid::Uuid;

    fn env(body: &str) -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::new_v4(),
            source: "src".into(),
            channel: "brenn:test".into(),
            sender: "alice".into(),
            publish_ts: Utc::now(),
            body: body.into(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            impetus: None,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Brenn,
        }
    }

    #[test]
    fn single_event_has_heading() {
        let text = format_messaging_event_single(&env("hi"));
        assert!(text.starts_with(MESSAGING_SINGLE_HEADING));
        // JSON contains body.
        assert!(text.contains("\"body\":\"hi\""));
    }

    #[test]
    fn batch_empty_returns_none() {
        assert!(format_messaging_event_batch(&[]).is_none());
    }

    #[test]
    fn batch_with_two_messages() {
        let text = format_messaging_event_batch(&[env("first"), env("second")]).unwrap();
        assert!(text.starts_with(MESSAGING_BATCH_HEADING));
        // Compact JSON array, both bodies present.
        assert!(text.contains("\"first\""));
        assert!(text.contains("\"second\""));
        // Array form, not pretty-printed.
        assert!(text.contains("[{"));
    }

    #[test]
    fn batch_single_message_uses_array_form() {
        let text = format_messaging_event_batch(&[env("only")]).unwrap();
        assert!(text.contains("[{"));
    }

    fn webhook_env(body: &str) -> MessageEnvelope {
        MessageEnvelope {
            channel: "webhook:my-ep".into(),
            envelope_type: ChannelScheme::Webhook,
            ..env(body)
        }
    }

    /// A webhook-channel envelope renders with the `[Webhook message]` heading
    /// (not the brenn heading). Regression guard: a prefix-check removal or
    /// string typo would produce `[Brenn message]` silently.
    #[test]
    fn webhook_single_uses_webhook_heading() {
        let text = format_messaging_event_single(&webhook_env("payload"));
        assert!(
            text.starts_with(WEBHOOK_SINGLE_HEADING),
            "expected `{WEBHOOK_SINGLE_HEADING}` prefix, got: {:?}",
            &text[..text.len().min(60)]
        );
        assert!(!text.starts_with(MESSAGING_SINGLE_HEADING));
        assert!(text.contains("\"body\":\"payload\""));
    }

    /// A webhook-channel batch renders with the `[Webhook messages]` heading.
    #[test]
    fn webhook_batch_uses_webhook_heading() {
        let text = format_messaging_event_batch(&[webhook_env("a"), webhook_env("b")]).unwrap();
        assert!(
            text.starts_with(WEBHOOK_BATCH_HEADING),
            "expected `{WEBHOOK_BATCH_HEADING}` prefix, got: {:?}",
            &text[..text.len().min(60)]
        );
        assert!(!text.starts_with(MESSAGING_BATCH_HEADING));
    }

    fn mqtt_env(body: &str) -> MessageEnvelope {
        MessageEnvelope {
            channel: "mqtt:ha-sensors".into(),
            envelope_type: ChannelScheme::Mqtt,
            ..env(body)
        }
    }

    /// An mqtt-channel envelope renders with the `[MQTT message]` heading (not
    /// the brenn heading). Regression guard: a missing match arm would silently
    /// produce `[Brenn message]`.
    #[test]
    fn mqtt_single_uses_mqtt_heading() {
        let text = format_messaging_event_single(&mqtt_env("payload"));
        assert!(
            text.starts_with(MQTT_SINGLE_HEADING),
            "expected `{MQTT_SINGLE_HEADING}` prefix, got: {:?}",
            &text[..text.len().min(60)]
        );
        assert!(!text.starts_with(MESSAGING_SINGLE_HEADING));
        assert!(text.contains("\"body\":\"payload\""));
    }

    /// An mqtt-channel batch renders with the `[MQTT messages]` heading.
    #[test]
    fn mqtt_batch_uses_mqtt_heading() {
        let text = format_messaging_event_batch(&[mqtt_env("a"), mqtt_env("b")]).unwrap();
        assert!(
            text.starts_with(MQTT_BATCH_HEADING),
            "expected `{MQTT_BATCH_HEADING}` prefix, got: {:?}",
            &text[..text.len().min(60)]
        );
        assert!(!text.starts_with(MESSAGING_BATCH_HEADING));
    }

    fn fixed(body: MessageBody) -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::nil(),
            publish_ts: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            body,
            ..env("")
        }
    }

    const FIXED_HEAD: &str = r#"{"message_id":"00000000-0000-0000-0000-000000000000","source":"src","channel":"brenn:test","sender":"alice","publish_ts":"2023-11-14T22:13:20Z","#;
    const FIXED_TAIL: &str = r#""urgency":"normal","envelope_type":"brenn"}"#;

    /// An untyped text body is shown exactly as the text shape writes it, by
    /// both formatters.
    #[test]
    fn an_untyped_text_body_is_the_text_shape() {
        let e = fixed(MessageBody::text("hi"));
        let json = format!(r#"{FIXED_HEAD}"body":"hi",{FIXED_TAIL}"#);
        assert_eq!(e.to_envelope_json(), json);
        assert_eq!(
            format_messaging_event_single(&e),
            format!("{MESSAGING_SINGLE_HEADING}\n\n{json}")
        );
        assert_eq!(
            format_messaging_event_batch(std::slice::from_ref(&e)).unwrap(),
            format!("{MESSAGING_BATCH_HEADING}\n\n[{json}]")
        );
    }

    #[test]
    fn a_typed_text_body_carries_its_content_type() {
        let body = MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap();
        let text = format_messaging_event_single(&fixed(body));
        assert!(
            text.ends_with(&format!(
                r##"{FIXED_HEAD}"body":"# hi","content_type":"text/markdown",{FIXED_TAIL}"##
            )),
            "{text}"
        );
    }

    #[test]
    fn a_non_text_body_is_a_placeholder_without_its_bytes() {
        let body = MessageBody::from_parts(Some("image/jpeg"), &[0xff, 0xd8, b'Z', b'Q']).unwrap();
        let batch = format_messaging_event_batch(&[fixed(body)]).unwrap();
        assert!(
            batch.ends_with(&format!(
                r#"[{FIXED_HEAD}"body":{{"binary":true,"content_type":"image/jpeg","size":4}},{FIXED_TAIL}]"#
            )),
            "{batch}"
        );
        assert!(!batch.contains("ZQ"), "{batch}");
    }

    /// A text body whose text is the placeholder's JSON stays a JSON string.
    #[test]
    fn a_text_body_cannot_forge_the_placeholder() {
        let forged = r#"{"binary":true,"content_type":"image/jpeg","size":4}"#;
        let text = format_messaging_event_single(&fixed(MessageBody::text(forged)));
        let json: serde_json::Value =
            serde_json::from_str(text.strip_prefix(MESSAGING_SINGLE_HEADING).unwrap().trim())
                .unwrap();
        assert_eq!(json["body"], serde_json::Value::String(forged.to_string()));
    }
}

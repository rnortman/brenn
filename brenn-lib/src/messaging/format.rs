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

use super::{ChannelScheme, MessageEnvelope};

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
    let json = serde_json::to_string(env)
        .expect("MessageEnvelope serialization cannot fail (all fields owned)");
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
    // Compact (non-pretty) JSON. The LLM gets canonical envelope JSON.
    #[derive(Serialize)]
    struct Wrapper<'a>(&'a [MessageEnvelope]);
    let json = serde_json::to_string(&Wrapper(envelopes))
        .expect("MessageEnvelope batch serialization cannot fail");
    Some(format!("{heading}{MESSAGING_HEADING_SEPARATOR}{json}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messaging::Urgency;
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
}

//! The serialized shapes of a [`MessageEnvelope`].
//!
//! An envelope is written to several contracts that agree on every field but
//! the body: the `envelope-json` a component is handed and attach v4 frames
//! (the text shape, here), what a conversation is shown, and the eviction
//! archive. Each contract is a [`BodyShape`]; [`ShapedEnvelope`] writes the
//! envelope's fields once, in one order, and asks the shape for the entries
//! that stand at the body's position. A new envelope field is therefore added
//! in one place and appears in every shape; the serializer and the reader
//! both name every field, so a field missing from either does not compile.

use std::marker::PhantomData;

use serde::ser::{Error as _, SerializeStruct};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{ChannelScheme, Impetus, MessageBody, MessageEnvelope, Urgency};

/// The entries a contract writes at the body's position.
pub trait BodyShape {
    /// Write `body`'s entries into the envelope's struct. Entries the shape
    /// writes follow `sender`/`publish_ts` and precede `reply_to`.
    fn serialize_body<S: SerializeStruct>(body: &MessageBody, out: &mut S) -> Result<(), S::Error>;
}

/// An envelope serialized in shape `B`.
pub struct ShapedEnvelope<'a, B: BodyShape> {
    envelope: &'a MessageEnvelope,
    shape: PhantomData<B>,
}

impl MessageEnvelope {
    /// This envelope, to be serialized in shape `B`.
    pub fn shaped<B: BodyShape>(&self) -> ShapedEnvelope<'_, B> {
        ShapedEnvelope {
            envelope: self,
            shape: PhantomData,
        }
    }
}

impl<B: BodyShape> Serialize for ShapedEnvelope<'_, B> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Destructured without `..`, so a field added to the envelope does not
        // compile until it is written here.
        let MessageEnvelope {
            message_id,
            source,
            channel,
            sender,
            publish_ts,
            body,
            reply_to,
            delivery_deadline,
            deliver_after,
            impetus,
            urgency,
            envelope_type,
        } = self.envelope;
        // Every field, plus one for a shape that writes a second body entry;
        // a length hint only, which JSON ignores.
        let mut out = serializer.serialize_struct("MessageEnvelope", 13)?;
        out.serialize_field("message_id", message_id)?;
        out.serialize_field("source", source)?;
        out.serialize_field("channel", channel)?;
        out.serialize_field("sender", sender)?;
        out.serialize_field("publish_ts", publish_ts)?;
        B::serialize_body(body, &mut out)?;
        optional_field(&mut out, "reply_to", reply_to)?;
        optional_field(&mut out, "delivery_deadline", delivery_deadline)?;
        optional_field(&mut out, "deliver_after", deliver_after)?;
        optional_field(&mut out, "impetus", impetus)?;
        out.serialize_field("urgency", urgency)?;
        out.serialize_field("envelope_type", envelope_type)?;
        out.end()
    }
}

fn optional_field<S: SerializeStruct, T: Serialize>(
    out: &mut S,
    key: &'static str,
    value: &Option<T>,
) -> Result<(), S::Error> {
    match value {
        Some(v) => out.serialize_field(key, v),
        None => out.skip_field(key),
    }
}

/// The text shape: `"body":"…"`, and nothing else at the body's position.
///
/// The shape of the 0.1.0 `envelope-json` and of attach v4 frames. A typed
/// text body is written as exactly its text, without its content type; a
/// non-text body has no representation here and fails to serialize.
pub struct TextShape;

impl BodyShape for TextShape {
    fn serialize_body<S: SerializeStruct>(body: &MessageBody, out: &mut S) -> Result<(), S::Error> {
        match body.as_text() {
            Some(text) => out.serialize_field("body", text),
            None => Err(S::Error::custom(format_args!(
                "the text shape cannot carry a non-text body ({})",
                body.summary()
            ))),
        }
    }
}

/// The header shape: nothing at the body's position.
///
/// The `header` of a 0.2.0 `envelope`, which carries the body beside it as a
/// content type and bytes. Every body serializes, whatever it holds.
pub struct HeaderShape;

impl BodyShape for HeaderShape {
    fn serialize_body<S: SerializeStruct>(_: &MessageBody, _: &mut S) -> Result<(), S::Error> {
        Ok(())
    }
}

/// `serialize_with` function writing an envelope field in the text shape.
pub fn serialize_text_shape<S: Serializer>(
    envelope: &MessageEnvelope,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    envelope.shaped::<TextShape>().serialize(serializer)
}

impl MessageEnvelope {
    /// This envelope as the canonical JSON text a component is handed: the
    /// `envelope-json` of `processor.wit`, in the text shape.
    ///
    /// The one lowering, called by every host that windows envelopes for a
    /// guest, so the text a component reads is the same at every placement.
    /// A typed text body is written as exactly its text: this shape has no
    /// place for the content type.
    ///
    /// # Panics
    ///
    /// If the body is not text (see [`MessageEnvelope::require_text_body`]).
    pub fn to_envelope_json(&self) -> String {
        self.require_text_body("0.1.0 envelope-json");
        serde_json::to_string(&self.shaped::<TextShape>())
            .expect("an envelope with a text body serializes to JSON")
    }

    /// This envelope without its body, as the JSON text a 0.2.0 component is
    /// handed as an `envelope`'s `header`.
    ///
    /// Every envelope field but the body, in the order of every other shape,
    /// so a field added to the envelope appears here with no further edit.
    /// The object is open: a guest ignores a key it does not know.
    pub fn to_header_json(&self) -> String {
        serde_json::to_string(&self.shaped::<HeaderShape>())
            .expect("an envelope header serializes to JSON")
    }

    /// The body's text, for a carrier whose contract is a string.
    ///
    /// Nothing outside the host can publish a non-text body, so a non-text
    /// body reaching a string carrier is a host bug. A change that opens a
    /// publish path for non-text bodies to a guest, a page, a remote daemon or
    /// a conversation must, in the same change, keep such bodies away from
    /// these carriers (a channel declaring that it may carry them, and string
    /// carriers refused on such a channel); otherwise this panic becomes
    /// reachable by a guest.
    ///
    /// # Panics
    ///
    /// If the body is not text, naming the carrier, the message id, the
    /// channel, the content type and the size.
    pub fn require_text_body(&self, carrier: &'static str) -> &str {
        match self.body.as_text() {
            Some(text) => text,
            None => panic!(
                "{carrier} cannot carry a non-text body: message {} on {} ({})",
                self.message_id,
                self.channel,
                self.body.summary()
            ),
        }
    }
}

/// The text shape as read: `body` is a JSON string, read as an untyped text
/// body. A `content_type` or `body_base64` key belongs to another shape and is
/// refused, so a line in another shape never parses into an envelope that has
/// lost its body's type.
#[derive(Deserialize)]
struct TextShapeIn {
    message_id: uuid::Uuid,
    source: String,
    channel: String,
    sender: String,
    publish_ts: chrono::DateTime<chrono::Utc>,
    body: String,
    #[serde(default, deserialize_with = "refuse_content_type")]
    #[allow(dead_code)]
    content_type: (),
    #[serde(default, deserialize_with = "refuse_body_base64")]
    #[allow(dead_code)]
    body_base64: (),
    reply_to: Option<String>,
    delivery_deadline: Option<chrono::DateTime<chrono::Utc>>,
    deliver_after: Option<chrono::DateTime<chrono::Utc>>,
    impetus: Option<Impetus>,
    urgency: Urgency,
    envelope_type: ChannelScheme,
}

fn refuse_content_type<'de, D: Deserializer<'de>>(_: D) -> Result<(), D::Error> {
    Err(serde::de::Error::custom(
        "an envelope in the text shape carries no `content_type`",
    ))
}

fn refuse_body_base64<'de, D: Deserializer<'de>>(_: D) -> Result<(), D::Error> {
    Err(serde::de::Error::custom(
        "an envelope in the text shape carries no `body_base64`",
    ))
}

impl<'de> Deserialize<'de> for MessageEnvelope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let w = TextShapeIn::deserialize(deserializer)?;
        Ok(MessageEnvelope {
            message_id: w.message_id,
            source: w.source,
            channel: w.channel,
            sender: w.sender,
            publish_ts: w.publish_ts,
            body: MessageBody::text(w.body),
            reply_to: w.reply_to,
            delivery_deadline: w.delivery_deadline,
            deliver_after: w.deliver_after,
            impetus: w.impetus,
            urgency: w.urgency,
            envelope_type: w.envelope_type,
        })
    }
}

/// An envelope's fields without its body, as read from the header shape: the
/// `header` of a 0.2.0 `envelope` and of an attach v5 row.
///
/// The header shape is written from the envelope itself, as
/// [`HeaderShape`]. This type is its reader, and the object is open on
/// purpose: every key it does not know is ignored — including `body`, which
/// this shape does not hold — so a writer may add keys a reader built before
/// them skips. A missing optional field reads as absent; a missing required
/// one is refused.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EnvelopeHeader {
    message_id: uuid::Uuid,
    source: String,
    channel: String,
    sender: String,
    publish_ts: chrono::DateTime<chrono::Utc>,
    reply_to: Option<String>,
    delivery_deadline: Option<chrono::DateTime<chrono::Utc>>,
    deliver_after: Option<chrono::DateTime<chrono::Utc>>,
    impetus: Option<Impetus>,
    urgency: Urgency,
    envelope_type: ChannelScheme,
}

impl MessageEnvelope {
    /// This envelope's fields without its body, cloned.
    pub fn header(&self) -> EnvelopeHeader {
        // Destructured without `..`, so a field added to the envelope does not
        // compile until the header holds it.
        let MessageEnvelope {
            message_id,
            source,
            channel,
            sender,
            publish_ts,
            body: _,
            reply_to,
            delivery_deadline,
            deliver_after,
            impetus,
            urgency,
            envelope_type,
        } = self;
        EnvelopeHeader {
            message_id: *message_id,
            source: source.clone(),
            channel: channel.clone(),
            sender: sender.clone(),
            publish_ts: *publish_ts,
            reply_to: reply_to.clone(),
            delivery_deadline: *delivery_deadline,
            deliver_after: *deliver_after,
            impetus: *impetus,
            urgency: *urgency,
            envelope_type: *envelope_type,
        }
    }

    /// The envelope made of `header`'s fields and `body`.
    pub fn from_header(header: EnvelopeHeader, body: MessageBody) -> MessageEnvelope {
        let EnvelopeHeader {
            message_id,
            source,
            channel,
            sender,
            publish_ts,
            reply_to,
            delivery_deadline,
            deliver_after,
            impetus,
            urgency,
            envelope_type,
        } = header;
        MessageEnvelope {
            message_id,
            source,
            channel,
            sender,
            publish_ts,
            body,
            reply_to,
            delivery_deadline,
            deliver_after,
            impetus,
            urgency,
            envelope_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use uuid::Uuid;

    fn envelope(body: MessageBody) -> MessageEnvelope {
        MessageEnvelope {
            message_id: Uuid::nil(),
            source: "src".to_string(),
            channel: "brenn:chan".to_string(),
            sender: "alice".to_string(),
            publish_ts: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            body,
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            impetus: None,
            urgency: Urgency::Normal,
            envelope_type: ChannelScheme::Brenn,
        }
    }

    fn binary() -> MessageBody {
        MessageBody::from_parts(Some("image/jpeg"), &[0xff, 0xd8, 0xff]).unwrap()
    }

    /// The text shape's bytes for an untyped body, pinned literally: the
    /// 0.1.0 `envelope-json` and attach v4 frames are this text.
    #[test]
    fn the_text_shape_is_byte_identical_to_the_derived_form() {
        let mut env = envelope(MessageBody::text("hi \"there\""));
        env.reply_to = Some("r".to_string());
        env.impetus = Some(Impetus::Replenish);
        assert_eq!(
            env.to_envelope_json(),
            concat!(
                r#"{"message_id":"00000000-0000-0000-0000-000000000000","source":"src","#,
                r#""channel":"brenn:chan","sender":"alice","publish_ts":"2023-11-14T22:13:20Z","#,
                r#""body":"hi \"there\"","reply_to":"r","impetus":"replenish","#,
                r#""urgency":"normal","envelope_type":"brenn"}"#
            )
        );
    }

    #[test]
    fn a_typed_text_body_serializes_as_its_bare_text() {
        let typed = MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap();
        assert_eq!(
            envelope(typed).to_envelope_json(),
            envelope(MessageBody::text("# hi")).to_envelope_json()
        );
    }

    #[test]
    #[should_panic(
        expected = "0.1.0 envelope-json cannot carry a non-text body: message 00000000-0000-0000-0000-000000000000 on brenn:chan (image/jpeg, 3 bytes)"
    )]
    fn to_envelope_json_panics_on_a_non_text_body() {
        envelope(binary()).to_envelope_json();
    }

    #[test]
    fn the_text_shape_serializer_refuses_a_non_text_body() {
        let env = envelope(binary());
        let err = serde_json::to_string(&env.shaped::<TextShape>()).unwrap_err();
        assert!(err.to_string().contains("non-text body"), "{err}");
    }

    #[test]
    fn serialize_with_writes_the_text_shape() {
        #[derive(Serialize)]
        struct Row<'a> {
            #[serde(serialize_with = "serialize_text_shape")]
            envelope: &'a MessageEnvelope,
        }
        let env = envelope(MessageBody::text("x"));
        let row = serde_json::to_value(Row { envelope: &env }).unwrap();
        assert_eq!(
            row["envelope"],
            serde_json::from_str::<serde_json::Value>(&env.to_envelope_json()).unwrap()
        );
    }

    #[test]
    fn todays_json_reads_as_an_untyped_text_body() {
        let text = envelope(MessageBody::text("hello")).to_envelope_json();
        let parsed: MessageEnvelope = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed.body, MessageBody::text("hello"));
        assert_eq!(parsed.body.content_type(), None);
    }

    /// Every field, each set to a value that is not its default, survives
    /// the text shape: a field the serializer or the reader drops fails here.
    #[test]
    fn a_fully_populated_envelope_round_trips_through_the_text_shape() {
        let at = |secs| DateTime::<Utc>::from_timestamp(secs, 0).unwrap();
        let env = MessageEnvelope {
            message_id: Uuid::from_u128(0x0123_4567_89ab_cdef),
            source: "src".to_string(),
            channel: "ephemeral:chan".to_string(),
            sender: "alice".to_string(),
            publish_ts: at(1_700_000_000),
            body: MessageBody::text("hello"),
            reply_to: Some("brenn:replies".to_string()),
            delivery_deadline: Some(at(1_700_000_100)),
            deliver_after: Some(at(1_700_000_050)),
            impetus: Some(Impetus::Replenish),
            urgency: Urgency::High,
            envelope_type: ChannelScheme::Ephemeral,
        };
        let parsed: MessageEnvelope = serde_json::from_str(&env.to_envelope_json()).unwrap();
        assert_eq!(parsed, env);
    }

    #[test]
    fn another_shapes_keys_are_refused() {
        let mut v: serde_json::Value =
            serde_json::from_str(&envelope(MessageBody::text("x")).to_envelope_json()).unwrap();
        for key in ["content_type", "body_base64"] {
            let mut with = v.clone();
            with[key] = json!("text/markdown");
            let err = serde_json::from_value::<MessageEnvelope>(with).unwrap_err();
            assert!(err.to_string().contains(key), "{key}: {err}");
        }
        v["content_type"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<MessageEnvelope>(v).is_err());
    }

    /// The header is every key of the text shape but `body`, in the same
    /// order, and no body key at all, whatever the body holds.
    #[test]
    fn the_header_is_every_envelope_key_and_no_body() {
        let at = |secs| DateTime::<Utc>::from_timestamp(secs, 0).unwrap();
        let typed = MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap();
        for body in [MessageBody::text("hello"), typed, binary()] {
            let mut env = envelope(body);
            env.reply_to = Some("brenn:replies".to_string());
            env.delivery_deadline = Some(at(1_700_000_100));
            env.deliver_after = Some(at(1_700_000_050));
            env.impetus = Some(Impetus::Replenish);
            assert_eq!(
                env.to_header_json(),
                concat!(
                    r#"{"message_id":"00000000-0000-0000-0000-000000000000","source":"src","#,
                    r#""channel":"brenn:chan","sender":"alice","publish_ts":"2023-11-14T22:13:20Z","#,
                    r#""reply_to":"brenn:replies","delivery_deadline":"2023-11-14T22:15:00Z","#,
                    r#""deliver_after":"2023-11-14T22:14:10Z","impetus":"replenish","#,
                    r#""urgency":"normal","envelope_type":"brenn"}"#
                ),
                "{}",
                env.body.summary()
            );
        }
    }

    /// Absent optional fields are absent from the header, as from the text
    /// shape: the two differ by the `body` key alone.
    #[test]
    fn the_header_is_the_text_shape_less_its_body() {
        let env = envelope(MessageBody::text("x"));
        let mut text: serde_json::Value = serde_json::from_str(&env.to_envelope_json()).unwrap();
        text.as_object_mut().unwrap().remove("body");
        let header: serde_json::Value = serde_json::from_str(&env.to_header_json()).unwrap();
        assert_eq!(header, text);
    }

    fn fully_populated(body: MessageBody) -> MessageEnvelope {
        let at = |secs| DateTime::<Utc>::from_timestamp(secs, 0).unwrap();
        MessageEnvelope {
            message_id: Uuid::from_u128(0x0123_4567_89ab_cdef),
            source: "src".to_string(),
            channel: "ephemeral:chan".to_string(),
            sender: "alice".to_string(),
            publish_ts: at(1_700_000_000),
            body,
            reply_to: Some("brenn:replies".to_string()),
            delivery_deadline: Some(at(1_700_000_100)),
            deliver_after: Some(at(1_700_000_050)),
            impetus: Some(Impetus::Replenish),
            urgency: Urgency::High,
            envelope_type: ChannelScheme::Ephemeral,
        }
    }

    /// Every field survives the header shape, whatever the body; the body
    /// rejoins the header whole.
    #[test]
    fn an_envelope_round_trips_through_its_header_and_body() {
        let typed = MessageBody::from_parts(Some("text/markdown"), b"# hi").unwrap();
        for env in [
            fully_populated(binary()),
            fully_populated(typed),
            envelope(MessageBody::text("x")),
        ] {
            let header: EnvelopeHeader = serde_json::from_str(&env.to_header_json()).unwrap();
            assert_eq!(header, env.header());
            assert_eq!(MessageEnvelope::from_header(header, env.body.clone()), env);
        }
    }

    /// Absent optional fields read as absent; a key the reader does not know,
    /// `body` included, is skipped.
    #[test]
    fn the_header_reader_ignores_unknown_keys() {
        let env = envelope(MessageBody::text("x"));
        let mut v: serde_json::Value = serde_json::from_str(&env.to_header_json()).unwrap();
        v["blob"] = json!({"id": "abc", "len": 7});
        v["body"] = json!("ignored");
        v["content_type"] = json!("image/jpeg");
        let header: EnvelopeHeader = serde_json::from_value(v).unwrap();
        assert_eq!(header, env.header());
    }

    #[test]
    fn the_header_reader_refuses_a_missing_required_field() {
        let env = envelope(MessageBody::text("x"));
        for key in [
            "message_id",
            "source",
            "channel",
            "sender",
            "publish_ts",
            "urgency",
            "envelope_type",
        ] {
            let mut v: serde_json::Value = serde_json::from_str(&env.to_header_json()).unwrap();
            v.as_object_mut().unwrap().remove(key);
            let err = serde_json::from_value::<EnvelopeHeader>(v).unwrap_err();
            assert!(err.to_string().contains(key), "{key}: {err}");
        }
    }

    #[test]
    fn require_text_body_returns_the_text() {
        let env = envelope(MessageBody::text("abc"));
        assert_eq!(env.require_text_body("test carrier"), "abc");
    }

    #[test]
    #[should_panic(
        expected = "test carrier cannot carry a non-text body: message 00000000-0000-0000-0000-000000000000 on brenn:chan (image/jpeg, 3 bytes)"
    )]
    fn require_text_body_panics_naming_the_message() {
        envelope(binary()).require_text_body("test carrier");
    }
}

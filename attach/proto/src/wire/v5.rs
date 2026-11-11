//! Protocol version 5: the four body-carrying frames travel as binary
//! messages, every other frame as one JSON text message.
//!
//! `Publish`, `PublishBatch`, `Deliver` and `DeferredView` are binary in every
//! case, even with no body in them; every other kind is text. The kind fixes
//! the message type, so a text message of a binary kind and a binary message
//! of a text kind are both malformed.
//!
//! A binary message is
//!
//! ```text
//! +----------------+----------------------+---------------------------+
//! | H : u32, BE    | header : H bytes     | bodies : rest of message  |
//! |                | UTF-8 JSON, strict   | concatenated, no padding  |
//! +----------------+----------------------+---------------------------+
//! ```
//!
//! The header is the frame as a JSON object, tagged and strict as a text
//! frame is, with each body replaced by a descriptor
//! `{"len":N,"content_type":"…"}` (`content_type` absent for an untyped body).
//! The body section holds each described body's bytes in frame order: the one
//! body of a `Publish`; a batch's `publishes` in order, then the bodies of its
//! `deferred_ops` edits in order; a `Deliver`'s rows in order; a
//! `DeferredView`'s entries in order. A delivered row's envelope is
//! `{"header":{…},"body":{…}}`, the header being the envelope in the header
//! shape; that header object is the one open object in the protocol, whose
//! unknown keys a reader ignores.
//!
//! Every disagreement between a header and its body section — a prefix
//! shorter than four bytes, `H` past the end, a `len` past what remains,
//! bytes left after the last body — is the one rule `frame header and bodies
//! disagree`. The peer's numbers are only ever compared with what remains,
//! never summed, so decoding never panics and never wraps on either pointer
//! width. Every body is rebuilt through `MessageBody::from_parts`, whose
//! refusal is the rule `malformed body`.
//!
//! The batch law charges each operation's channel address and content type as
//! the header spends them (their JSON-escaped length) and each body raw, as it
//! travels; the frame cap is the body cap plus fixed allowances, with no
//! escaping factor.

use std::borrow::Cow;

use brenn_envelope::{EnvelopeHeader, HeaderShape, MessageBody};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use super::{DecodeError, TypeTag, WireMessage};
use crate::{
    AlertSeverity, BATCH_OP_OVERHEAD_BYTES, BatchDeferredOp, BatchEntry, BatchIllegal, ClientFrame,
    Cursor, DeferredOpKind, DeferredViewEntry, DeliverRow, GapInfo, MAX_BATCH_OPS, MessageEnvelope,
    PUBLISH_FRAME_OVERHEAD_BYTES, PublishBatchOutcome, PublishOutcome, ServerFrame,
    SubscribeOutcome, Urgency, VersionRange,
};

#[cfg(test)]
mod tests;

const DISAGREE: &str = "frame header and bodies disagree";
const MALFORMED_BODY: &str = "malformed body";
const TEXT_OF_BINARY_KIND: &str = "text frame of a binary kind";
const BINARY_OF_TEXT_KIND: &str = "binary frame of a text kind";
const UNPARSEABLE_CLIENT: &str = "unparseable client frame";
const UNPARSEABLE_SERVER: &str = "unparseable server frame";

const CLIENT_BINARY_KINDS: [&str; 2] = ["Publish", "PublishBatch"];
const CLIENT_TEXT_KINDS: [&str; 4] = ["Hello", "Subscribe", "Unsubscribe", "Alert"];
const SERVER_BINARY_KINDS: [&str; 2] = ["Deliver", "DeferredView"];
const SERVER_TEXT_KINDS: [&str; 6] = [
    "Hello",
    "Welcome",
    "Heartbeat",
    "SubscribeResult",
    "PublishResult",
    "PublishBatchResult",
];

// ---------------------------------------------------------------------------
// The v5 schema. Borrowed when written, owned when read.
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum ClientText<'a> {
    Hello {
        versions: VersionRange,
        ident: Cow<'a, str>,
    },
    Subscribe {
        channel: Cow<'a, str>,
        push_depth: u64,
        retain_depth: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resume: Option<Cow<'a, Cursor>>,
    },
    Unsubscribe {
        channel: Cow<'a, str>,
    },
    Alert {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<Cow<'a, str>>,
        severity: AlertSeverity,
        title: Cow<'a, str>,
        body: Cow<'a, str>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum ClientBinary<'a> {
    Publish {
        channel: Cow<'a, str>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<Cow<'a, str>>,
        body: Desc<'a>,
        urgency: Urgency,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<u64>,
    },
    PublishBatch {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<Cow<'a, str>>,
        correlation: u64,
        publishes: Vec<Entry<'a>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        deferred_ops: Vec<Op<'a>>,
    },
}

/// A body's place in the header: its length in the body section and its
/// content type.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Desc<'a> {
    len: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<Cow<'a, str>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry<'a> {
    channel: Cow<'a, str>,
    body: Desc<'a>,
    urgency: Urgency,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deliver_after: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Op<'a> {
    channel: Cow<'a, str>,
    message_id: Uuid,
    op: OpKind<'a>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
enum OpKind<'a> {
    Cancel,
    Edit {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<Desc<'a>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deliver_after: Option<u64>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum ServerText<'a> {
    Hello {
        versions: VersionRange,
        ident: Cow<'a, str>,
    },
    Welcome {
        version: u32,
        participant_id: Cow<'a, str>,
        session_id: Cow<'a, str>,
        heartbeat_secs: u32,
        max_body_bytes: u64,
        max_frame_bytes: u64,
        alert_granted: bool,
    },
    Heartbeat,
    SubscribeResult {
        channel: Cow<'a, str>,
        outcome: SubscribeOutcome,
        replay_count: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gap: Option<GapInfo>,
    },
    PublishResult {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        correlation: Option<u64>,
        outcome: Outcome,
    },
    PublishBatchResult {
        correlation: u64,
        outcome: BatchOutcome,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum ServerBinary<'a> {
    Deliver {
        channel: Cow<'a, str>,
        rows: Vec<Row<'a>>,
    },
    DeferredView {
        channel: Cow<'a, str>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<Cow<'a, str>>,
        entries: Vec<ViewEntry<'a>>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row<'a> {
    envelope: RowEnvelope<'a>,
    seq: u64,
    cursor: Cow<'a, Cursor>,
    dropped: u64,
}

/// `{header, body}`: strict itself, while `header` ignores unknown keys.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RowEnvelope<'a> {
    header: Header<'a>,
    body: Desc<'a>,
}

/// A row's `header`: written from the delivered envelope in the header shape,
/// borrowing it; read as the envelope's fields.
enum Header<'a> {
    Out(&'a MessageEnvelope),
    In(EnvelopeHeader),
}

impl Header<'_> {
    fn into_fields(self) -> EnvelopeHeader {
        match self {
            Header::Out(envelope) => envelope.header(),
            Header::In(header) => header,
        }
    }
}

impl Serialize for Header<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Header::Out(envelope) => envelope.shaped::<HeaderShape>().serialize(serializer),
            Header::In(_) => unreachable!("attach v5: a header read off the wire is never written"),
        }
    }
}

impl<'de> Deserialize<'de> for Header<'_> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        EnvelopeHeader::deserialize(deserializer).map(Header::In)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewEntry<'a> {
    message_id: Uuid,
    body: Desc<'a>,
    deliver_after: u64,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Outcome {
    Ok,
    RateLimited,
    BodyTooLarge { len: u64, max: u64 },
    Failed,
    TextOnlyChannel,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum BatchOutcome {
    Ok,
    RateLimited,
    TextOnlyChannel,
}

/// v5 writes every body as its bytes.
pub(super) const CARRIES_NON_TEXT_BODIES: bool = true;

/// v5 writes every body.
pub(super) fn carries(_body: &MessageBody) -> bool {
    true
}

/// The body section being written: each body's bytes, in the order their
/// descriptors are made.
#[derive(Default)]
struct BodiesOut<'a>(Vec<&'a [u8]>);

impl<'a> BodiesOut<'a> {
    fn describe(&mut self, body: &'a MessageBody) -> Desc<'a> {
        self.0.push(body.as_bytes());
        Desc {
            len: u64::try_from(body.len()).expect("a body length fits u64"),
            content_type: body.content_type().map(Cow::Borrowed),
        }
    }

    fn len(&self) -> usize {
        self.0.iter().map(|bytes| bytes.len()).sum()
    }
}

fn borrowed(s: &str) -> Cow<'_, str> {
    Cow::Borrowed(s)
}

fn owned(s: Cow<'_, str>) -> String {
    s.into_owned()
}

/// The prefix, the header and the bodies, as one binary message.
fn binary(header: &impl Serialize, bodies: &BodiesOut<'_>) -> WireMessage {
    let mut out = Vec::with_capacity(4 + 256 + bodies.len());
    out.extend_from_slice(&[0; 4]);
    serde_json::to_writer(&mut out, header).expect("attach v5: a frame header serializes to JSON");
    let header_len =
        u32::try_from(out.len() - 4).expect("attach v5: a frame header is under 4 GiB");
    out[..4].copy_from_slice(&header_len.to_be_bytes());
    for bytes in &bodies.0 {
        out.extend_from_slice(bytes);
    }
    WireMessage::Binary(out)
}

fn text(frame: &impl Serialize) -> WireMessage {
    WireMessage::Text(serde_json::to_string(frame).expect("attach v5: a text frame serializes"))
}

fn batch_out<'a>(
    attribution: Option<&'a str>,
    correlation: u64,
    publishes: &'a [BatchEntry],
    deferred_ops: &'a [BatchDeferredOp],
    bodies: &mut BodiesOut<'a>,
) -> ClientBinary<'a> {
    let publishes = publishes
        .iter()
        .map(|entry| Entry {
            channel: borrowed(&entry.channel),
            body: bodies.describe(&entry.body),
            urgency: entry.urgency,
            deliver_after: entry.deliver_after,
        })
        .collect();
    let deferred_ops = deferred_ops
        .iter()
        .map(|op| Op {
            channel: borrowed(&op.channel),
            message_id: op.message_id,
            op: match &op.op {
                DeferredOpKind::Cancel => OpKind::Cancel,
                DeferredOpKind::Edit {
                    body,
                    deliver_after,
                } => OpKind::Edit {
                    body: body.as_ref().map(|body| bodies.describe(body)),
                    deliver_after: *deliver_after,
                },
            },
        })
        .collect();
    ClientBinary::PublishBatch {
        attribution: attribution.map(borrowed),
        correlation,
        publishes,
        deferred_ops,
    }
}

pub(super) fn encode_client(frame: &ClientFrame) -> WireMessage {
    let mut bodies = BodiesOut::default();
    let header = match frame {
        ClientFrame::Hello { versions, ident } => {
            return text(&ClientText::Hello {
                versions: *versions,
                ident: borrowed(ident),
            });
        }
        ClientFrame::Subscribe {
            channel,
            push_depth,
            retain_depth,
            resume,
        } => {
            return text(&ClientText::Subscribe {
                channel: borrowed(channel),
                push_depth: *push_depth,
                retain_depth: *retain_depth,
                resume: resume.as_ref().map(Cow::Borrowed),
            });
        }
        ClientFrame::Unsubscribe { channel } => {
            return text(&ClientText::Unsubscribe {
                channel: borrowed(channel),
            });
        }
        ClientFrame::Alert {
            attribution,
            severity,
            title,
            body,
        } => {
            return text(&ClientText::Alert {
                attribution: attribution.as_deref().map(borrowed),
                severity: *severity,
                title: borrowed(title),
                body: borrowed(body),
            });
        }
        ClientFrame::Publish {
            channel,
            attribution,
            body,
            urgency,
            correlation,
        } => ClientBinary::Publish {
            channel: borrowed(channel),
            attribution: attribution.as_deref().map(borrowed),
            body: bodies.describe(body),
            urgency: *urgency,
            correlation: *correlation,
        },
        ClientFrame::PublishBatch {
            attribution,
            correlation,
            publishes,
            deferred_ops,
        } => batch_out(
            attribution.as_deref(),
            *correlation,
            publishes,
            deferred_ops,
            &mut bodies,
        ),
    };
    binary(&header, &bodies)
}

fn outcome_out(outcome: PublishOutcome) -> Outcome {
    match outcome {
        PublishOutcome::Ok => Outcome::Ok,
        PublishOutcome::RateLimited => Outcome::RateLimited,
        PublishOutcome::BodyTooLarge { len, max } => Outcome::BodyTooLarge { len, max },
        PublishOutcome::Failed => Outcome::Failed,
        PublishOutcome::TextOnlyChannel => Outcome::TextOnlyChannel,
    }
}

fn batch_outcome_out(outcome: PublishBatchOutcome) -> BatchOutcome {
    match outcome {
        PublishBatchOutcome::Ok => BatchOutcome::Ok,
        PublishBatchOutcome::RateLimited => BatchOutcome::RateLimited,
        PublishBatchOutcome::TextOnlyChannel => BatchOutcome::TextOnlyChannel,
    }
}

pub(super) fn encode_server(frame: &ServerFrame) -> WireMessage {
    let mut bodies = BodiesOut::default();
    let header = match frame {
        ServerFrame::Hello { versions, ident } => {
            return text(&ServerText::Hello {
                versions: *versions,
                ident: borrowed(ident),
            });
        }
        ServerFrame::Welcome {
            version,
            participant_id,
            session_id,
            heartbeat_secs,
            max_body_bytes,
            max_frame_bytes,
            alert_granted,
        } => {
            return text(&ServerText::Welcome {
                version: *version,
                participant_id: borrowed(participant_id),
                session_id: borrowed(session_id),
                heartbeat_secs: *heartbeat_secs,
                max_body_bytes: *max_body_bytes,
                max_frame_bytes: *max_frame_bytes,
                alert_granted: *alert_granted,
            });
        }
        ServerFrame::Heartbeat => return text(&ServerText::Heartbeat),
        ServerFrame::SubscribeResult {
            channel,
            outcome,
            replay_count,
            gap,
        } => {
            return text(&ServerText::SubscribeResult {
                channel: borrowed(channel),
                outcome: *outcome,
                replay_count: *replay_count,
                gap: *gap,
            });
        }
        ServerFrame::PublishResult {
            correlation,
            outcome,
        } => {
            return text(&ServerText::PublishResult {
                correlation: *correlation,
                outcome: outcome_out(*outcome),
            });
        }
        ServerFrame::PublishBatchResult {
            correlation,
            outcome,
        } => {
            return text(&ServerText::PublishBatchResult {
                correlation: *correlation,
                outcome: batch_outcome_out(*outcome),
            });
        }
        ServerFrame::Deliver { channel, rows } => ServerBinary::Deliver {
            channel: borrowed(channel),
            rows: rows
                .iter()
                .map(|row| Row {
                    envelope: RowEnvelope {
                        header: Header::Out(&row.envelope),
                        body: bodies.describe(&row.envelope.body),
                    },
                    seq: row.seq,
                    cursor: Cow::Borrowed(&row.cursor),
                    dropped: row.dropped,
                })
                .collect(),
        },
        ServerFrame::DeferredView {
            channel,
            attribution,
            entries,
        } => ServerBinary::DeferredView {
            channel: borrowed(channel),
            attribution: attribution.as_deref().map(borrowed),
            entries: entries
                .iter()
                .map(|entry| ViewEntry {
                    message_id: entry.message_id,
                    body: bodies.describe(&entry.body),
                    deliver_after: entry.deliver_after,
                })
                .collect(),
        },
    };
    binary(&header, &bodies)
}

/// A binary message cut into its header and its body section.
fn split(bytes: &[u8]) -> Result<(&[u8], &[u8]), DecodeError> {
    let Some((prefix, rest)) = bytes.split_first_chunk::<4>() else {
        return Err(DecodeError::new(DISAGREE));
    };
    let header_len = u32::from_be_bytes(*prefix);
    let header_len = usize::try_from(header_len)
        .ok()
        .filter(|len| *len <= rest.len())
        .ok_or_else(|| DecodeError::new(DISAGREE))?;
    Ok(rest.split_at(header_len))
}

/// The body section being read: bodies are cut from the front in frame order.
struct BodiesIn<'b> {
    rest: &'b [u8],
}

impl BodiesIn<'_> {
    /// The next body, as `desc` describes it.
    ///
    /// `desc.len` is the peer's number: it is compared, as a `u64`, with what
    /// remains before it is narrowed to an index.
    fn take(&mut self, desc: Desc<'_>) -> Result<MessageBody, DecodeError> {
        let remaining = u64::try_from(self.rest.len()).expect("a slice length fits u64");
        if desc.len > remaining {
            return Err(DecodeError::new(DISAGREE));
        }
        let len = usize::try_from(desc.len).map_err(|_| DecodeError::new(DISAGREE))?;
        let (bytes, rest) = self.rest.split_at(len);
        self.rest = rest;
        MessageBody::from_parts(desc.content_type.as_deref(), bytes)
            .map_err(|err| DecodeError::with_detail(MALFORMED_BODY, err))
    }

    /// Every byte of the section is accounted for.
    fn finish(self) -> Result<(), DecodeError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::new(DISAGREE))
        }
    }
}

/// The `type` tag of a text message or a binary header, `None` when there is
/// no readable tag.
fn tag_of(json: &[u8]) -> Option<String> {
    serde_json::from_slice::<TypeTag>(json)
        .ok()
        .map(|TypeTag { tag }| tag)
}

/// A text message's strict parse. Only a message that fails it is read for
/// its tag: the text schema holds no binary kind, so a binary kind sent as
/// text always fails, and is then named by its own rule.
fn parse_text<'de, T: Deserialize<'de>>(
    text: &'de str,
    binary_kinds: &[&str],
    unparseable: &'static str,
) -> Result<T, DecodeError> {
    serde_json::from_str(text).map_err(|err| {
        if tag_of(text.as_bytes()).is_some_and(|tag| binary_kinds.contains(&tag.as_str())) {
            DecodeError::new(TEXT_OF_BINARY_KIND)
        } else {
            DecodeError::with_detail(unparseable, err)
        }
    })
}

/// A binary header's strict parse. Only a header that fails it is read for
/// its tag: the binary schema holds no text kind, so a text kind sent as
/// binary always fails, and is then named by its own rule.
fn parse_header<'de, T: Deserialize<'de>>(
    header: &'de [u8],
    text_kinds: &[&str],
    unparseable: &'static str,
) -> Result<T, DecodeError> {
    serde_json::from_slice(header).map_err(|err| {
        if tag_of(header).is_some_and(|tag| text_kinds.contains(&tag.as_str())) {
            DecodeError::new(BINARY_OF_TEXT_KIND)
        } else {
            DecodeError::with_detail(unparseable, err)
        }
    })
}

fn client_text_in(frame: ClientText<'_>) -> ClientFrame {
    match frame {
        ClientText::Hello { versions, ident } => ClientFrame::Hello {
            versions,
            ident: owned(ident),
        },
        ClientText::Subscribe {
            channel,
            push_depth,
            retain_depth,
            resume,
        } => ClientFrame::Subscribe {
            channel: owned(channel),
            push_depth,
            retain_depth,
            resume: resume.map(Cow::into_owned),
        },
        ClientText::Unsubscribe { channel } => ClientFrame::Unsubscribe {
            channel: owned(channel),
        },
        ClientText::Alert {
            attribution,
            severity,
            title,
            body,
        } => ClientFrame::Alert {
            attribution: attribution.map(owned),
            severity,
            title: owned(title),
            body: owned(body),
        },
    }
}

fn client_binary_in(
    frame: ClientBinary<'_>,
    bodies: &mut BodiesIn<'_>,
) -> Result<ClientFrame, DecodeError> {
    Ok(match frame {
        ClientBinary::Publish {
            channel,
            attribution,
            body,
            urgency,
            correlation,
        } => ClientFrame::Publish {
            channel: owned(channel),
            attribution: attribution.map(owned),
            body: bodies.take(body)?,
            urgency,
            correlation,
        },
        ClientBinary::PublishBatch {
            attribution,
            correlation,
            publishes,
            deferred_ops,
        } => {
            let publishes = publishes
                .into_iter()
                .map(|entry| {
                    Ok(BatchEntry {
                        channel: owned(entry.channel),
                        body: bodies.take(entry.body)?,
                        urgency: entry.urgency,
                        deliver_after: entry.deliver_after,
                    })
                })
                .collect::<Result<_, DecodeError>>()?;
            let deferred_ops = deferred_ops
                .into_iter()
                .map(|op| {
                    Ok(BatchDeferredOp {
                        channel: owned(op.channel),
                        message_id: op.message_id,
                        op: match op.op {
                            OpKind::Cancel => DeferredOpKind::Cancel,
                            OpKind::Edit {
                                body,
                                deliver_after,
                            } => DeferredOpKind::Edit {
                                body: body.map(|desc| bodies.take(desc)).transpose()?,
                                deliver_after,
                            },
                        },
                    })
                })
                .collect::<Result<_, DecodeError>>()?;
            ClientFrame::PublishBatch {
                attribution: attribution.map(owned),
                correlation,
                publishes,
                deferred_ops,
            }
        }
    })
}

fn server_text_in(frame: ServerText<'_>) -> ServerFrame {
    match frame {
        ServerText::Hello { versions, ident } => ServerFrame::Hello {
            versions,
            ident: owned(ident),
        },
        ServerText::Welcome {
            version,
            participant_id,
            session_id,
            heartbeat_secs,
            max_body_bytes,
            max_frame_bytes,
            alert_granted,
        } => ServerFrame::Welcome {
            version,
            participant_id: owned(participant_id),
            session_id: owned(session_id),
            heartbeat_secs,
            max_body_bytes,
            max_frame_bytes,
            alert_granted,
        },
        ServerText::Heartbeat => ServerFrame::Heartbeat,
        ServerText::SubscribeResult {
            channel,
            outcome,
            replay_count,
            gap,
        } => ServerFrame::SubscribeResult {
            channel: owned(channel),
            outcome,
            replay_count,
            gap,
        },
        ServerText::PublishResult {
            correlation,
            outcome,
        } => ServerFrame::PublishResult {
            correlation,
            outcome: match outcome {
                Outcome::Ok => PublishOutcome::Ok,
                Outcome::RateLimited => PublishOutcome::RateLimited,
                Outcome::BodyTooLarge { len, max } => PublishOutcome::BodyTooLarge { len, max },
                Outcome::Failed => PublishOutcome::Failed,
                Outcome::TextOnlyChannel => PublishOutcome::TextOnlyChannel,
            },
        },
        ServerText::PublishBatchResult {
            correlation,
            outcome,
        } => ServerFrame::PublishBatchResult {
            correlation,
            outcome: match outcome {
                BatchOutcome::Ok => PublishBatchOutcome::Ok,
                BatchOutcome::RateLimited => PublishBatchOutcome::RateLimited,
                BatchOutcome::TextOnlyChannel => PublishBatchOutcome::TextOnlyChannel,
            },
        },
    }
}

fn server_binary_in(
    frame: ServerBinary<'_>,
    bodies: &mut BodiesIn<'_>,
) -> Result<ServerFrame, DecodeError> {
    Ok(match frame {
        ServerBinary::Deliver { channel, rows } => ServerFrame::Deliver {
            channel: owned(channel),
            rows: rows
                .into_iter()
                .map(|row| {
                    let RowEnvelope { header, body } = row.envelope;
                    Ok(DeliverRow {
                        envelope: MessageEnvelope::from_header(
                            header.into_fields(),
                            bodies.take(body)?,
                        ),
                        seq: row.seq,
                        cursor: row.cursor.into_owned(),
                        dropped: row.dropped,
                    })
                })
                .collect::<Result<_, DecodeError>>()?,
        },
        ServerBinary::DeferredView {
            channel,
            attribution,
            entries,
        } => ServerFrame::DeferredView {
            channel: owned(channel),
            attribution: attribution.map(owned),
            entries: entries
                .into_iter()
                .map(|entry| {
                    Ok(DeferredViewEntry {
                        message_id: entry.message_id,
                        body: bodies.take(entry.body)?,
                        deliver_after: entry.deliver_after,
                    })
                })
                .collect::<Result<_, DecodeError>>()?,
        },
    })
}

pub(super) fn decode_client(msg: WireMessage) -> Result<ClientFrame, DecodeError> {
    match msg {
        WireMessage::Text(text) => {
            parse_text::<ClientText<'_>>(&text, &CLIENT_BINARY_KINDS, UNPARSEABLE_CLIENT)
                .map(client_text_in)
        }
        WireMessage::Binary(bytes) => {
            let (header, section) = split(&bytes)?;
            let frame =
                parse_header::<ClientBinary<'_>>(header, &CLIENT_TEXT_KINDS, UNPARSEABLE_CLIENT)?;
            let mut bodies = BodiesIn { rest: section };
            let frame = client_binary_in(frame, &mut bodies)?;
            bodies.finish()?;
            Ok(frame)
        }
    }
}

pub(super) fn decode_server(msg: WireMessage) -> Result<ServerFrame, DecodeError> {
    match msg {
        WireMessage::Text(text) => {
            parse_text::<ServerText<'_>>(&text, &SERVER_BINARY_KINDS, UNPARSEABLE_SERVER)
                .map(server_text_in)
        }
        WireMessage::Binary(bytes) => {
            let (header, section) = split(&bytes)?;
            let frame =
                parse_header::<ServerBinary<'_>>(header, &SERVER_TEXT_KINDS, UNPARSEABLE_SERVER)?;
            let mut bodies = BodiesIn { rest: section };
            let frame = server_binary_in(frame, &mut bodies)?;
            bodies.finish()?;
            Ok(frame)
        }
    }
}

// ---------------------------------------------------------------------------
// The batch law and the frame cap
// ---------------------------------------------------------------------------

/// The length of `s` written as the contents of a JSON string, as
/// `serde_json` escapes it: `"` and `\` and the five short-escaped controls
/// take two bytes, every other control six, every other byte one.
fn escaped_len(s: &str) -> usize {
    s.bytes()
        .map(|byte| match byte {
            b'"' | b'\\' | 0x08 | 0x0c | b'\n' | b'\r' | b'\t' => 2,
            0x00..=0x1f => 6,
            _ => 1,
        })
        .sum()
}

/// What one operation's channel and optional body charge: the channel and the
/// content type escaped, the body raw.
fn op_charge(channel: &str, body: Option<&MessageBody>) -> usize {
    escaped_len(channel)
        + body.map_or(0, |body| {
            body.content_type().map_or(0, escaped_len) + body.len()
        })
}

pub(super) fn batch_charged_bytes(
    publishes: &[BatchEntry],
    deferred_ops: &[BatchDeferredOp],
) -> usize {
    let entries: usize = publishes
        .iter()
        .map(|entry| op_charge(&entry.channel, Some(&entry.body)))
        .sum();
    let ops: usize = deferred_ops
        .iter()
        .map(|op| {
            let body = match &op.op {
                DeferredOpKind::Cancel => None,
                DeferredOpKind::Edit { body, .. } => body.as_ref(),
            };
            op_charge(&op.channel, body)
        })
        .sum();
    entries + ops
}

pub(super) fn check_batch_legality(
    publishes: &[BatchEntry],
    deferred_ops: &[BatchDeferredOp],
    max_body_bytes: usize,
) -> Result<(), BatchIllegal> {
    super::check_law(
        publishes.len() + deferred_ops.len(),
        batch_charged_bytes(publishes, deferred_ops),
        max_body_bytes,
    )
}

/// The charged budget, plus a fixed allowance per batch operation for the
/// fields no budget charges, plus the base overhead a single publish is sized
/// with, which also covers the four-byte prefix.
pub(super) fn max_client_frame_bytes(max_body_bytes: usize) -> usize {
    // Checked, as v4's is: the value gates a violation and must equal what a
    // 32-bit client derives.
    max_body_bytes
        .checked_add(MAX_BATCH_OPS * BATCH_OP_OVERHEAD_BYTES)
        .and_then(|sum| sum.checked_add(PUBLISH_FRAME_OVERHEAD_BYTES))
        .expect("max_body_bytes too large: WS frame-cap derivation overflowed usize")
}

pub(super) fn batch_frame_bytes(
    attribution: Option<&str>,
    publishes: &[BatchEntry],
    deferred_ops: &[BatchDeferredOp],
) -> usize {
    let mut bodies = BodiesOut::default();
    let header = batch_out(attribution, u64::MAX, publishes, deferred_ops, &mut bodies);
    let header_len = serde_json::to_vec(&header)
        .expect("attach v5: a PublishBatch header serializes to JSON")
        .len();
    4 + header_len + bodies.len()
}

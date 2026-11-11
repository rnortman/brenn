//! Protocol version 4: every frame is one JSON text message, and every body is
//! a JSON string.
//!
//! The schema below is v4's own, kept as private serde types and converted to
//! and from the version-neutral frames of the crate root. The two frame enums are
//! tagged by `"type"`, the inner enums (outcomes, gap reasons, deferred-op
//! kinds) by `"kind"`; every frame and payload is `deny_unknown_fields`. The
//! one boundary of that strictness: a variant with no fields has no field list
//! for serde's internally-tagged representation to check trailing keys
//! against, so junk beside such a tag parses — there is nothing there an
//! extra key could be a misspelling of.
//!
//! A binary message is never a v4 frame. A client body is untyped text; a
//! server writes a typed text body as its text, without the type; nothing
//! writes a non-text body, and neither `TextOnlyChannel` outcome exists. The
//! batch law charges each operation's channel address plus its body, and the
//! frame cap allows for the worst-case JSON escaping of those charged bytes.

use std::borrow::Cow;

use brenn_envelope::MessageBody;
use serde::{Deserialize, Serialize, Serializer};
use uuid::Uuid;

use super::{DecodeError, WireMessage};
use crate::{
    AlertSeverity, BATCH_OP_OVERHEAD_BYTES, BatchDeferredOp, BatchEntry, BatchIllegal, ClientFrame,
    Cursor, DeferredOpKind, DeferredViewEntry, DeliverRow, GapInfo, MAX_BATCH_OPS, MessageEnvelope,
    PUBLISH_FRAME_OVERHEAD_BYTES, PublishBatchOutcome, PublishOutcome, ServerFrame,
    SubscribeOutcome, Urgency, VersionRange,
};

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod schema_tests;

/// Worst-case JSON string expansion: one source byte serializes as at most six
/// (`\u00XX` for a control character).
const JSON_ESCAPE_EXPANSION: usize = 6;

// ---------------------------------------------------------------------------
// The v4 schema. Borrowed when written, owned when read.
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum Client<'a> {
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
    Publish {
        channel: Cow<'a, str>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<Cow<'a, str>>,
        body: Cow<'a, str>,
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
    Alert {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<Cow<'a, str>>,
        severity: AlertSeverity,
        title: Cow<'a, str>,
        body: Cow<'a, str>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry<'a> {
    channel: Cow<'a, str>,
    body: Cow<'a, str>,
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
        body: Option<Cow<'a, str>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deliver_after: Option<u64>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum Server<'a> {
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
    Deliver {
        channel: Cow<'a, str>,
        rows: Vec<Row<'a>>,
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
    /// Written in the text shape: the body as its text.
    #[serde(serialize_with = "text_shape")]
    envelope: Cow<'a, MessageEnvelope>,
    seq: u64,
    cursor: Cow<'a, Cursor>,
    dropped: u64,
}

/// `serialize_with` for the row's envelope, whatever holds it.
fn text_shape<S: Serializer, E: std::ops::Deref<Target = MessageEnvelope>>(
    envelope: &E,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    brenn_envelope::serialize_text_shape(envelope, serializer)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewEntry<'a> {
    message_id: Uuid,
    body: Cow<'a, str>,
    deliver_after: u64,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Outcome {
    Ok,
    RateLimited,
    BodyTooLarge { len: u64, max: u64 },
    Failed,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum BatchOutcome {
    Ok,
    RateLimited,
}

/// v4 writes every body as a JSON string.
pub(super) const CARRIES_NON_TEXT_BODIES: bool = false;

/// Whether v4 can write `body` in a client frame: untyped text only.
pub(super) fn carries(body: &MessageBody) -> bool {
    body.is_text() && body.content_type().is_none()
}

/// A client body as v4 writes it.
///
/// # Panics
///
/// On a body that is not untyped text: an embedder asks
/// [`Codec::carries`](super::Codec::carries) before it publishes.
fn client_body(body: &MessageBody) -> Cow<'_, str> {
    match body.as_text() {
        Some(text) if body.content_type().is_none() => Cow::Borrowed(text),
        _ => panic!(
            "attach v4 cannot carry a client body that is not untyped text ({})",
            body.summary()
        ),
    }
}

/// A server body as v4 writes it: a typed text body as its text.
///
/// # Panics
///
/// On a non-text body: a session on v4 is never handed one to write.
fn server_body<'a>(body: &'a MessageBody, carrier: &str) -> Cow<'a, str> {
    match body.as_text() {
        Some(text) => Cow::Borrowed(text),
        None => panic!(
            "attach v4 {carrier} cannot carry a non-text body ({})",
            body.summary()
        ),
    }
}

fn read_body(text: Cow<'_, str>) -> MessageBody {
    MessageBody::text(text.into_owned())
}

fn borrowed(s: &str) -> Cow<'_, str> {
    Cow::Borrowed(s)
}

fn owned(s: Cow<'_, str>) -> String {
    s.into_owned()
}

fn entry_out(entry: &BatchEntry) -> Entry<'_> {
    Entry {
        channel: borrowed(&entry.channel),
        body: client_body(&entry.body),
        urgency: entry.urgency,
        deliver_after: entry.deliver_after,
    }
}

fn op_out(op: &BatchDeferredOp) -> Op<'_> {
    Op {
        channel: borrowed(&op.channel),
        message_id: op.message_id,
        op: match &op.op {
            DeferredOpKind::Cancel => OpKind::Cancel,
            DeferredOpKind::Edit {
                body,
                deliver_after,
            } => OpKind::Edit {
                body: body.as_ref().map(client_body),
                deliver_after: *deliver_after,
            },
        },
    }
}

fn batch_out<'a>(
    attribution: Option<&'a str>,
    correlation: u64,
    publishes: &'a [BatchEntry],
    deferred_ops: &'a [BatchDeferredOp],
) -> Client<'a> {
    Client::PublishBatch {
        attribution: attribution.map(borrowed),
        correlation,
        publishes: publishes.iter().map(entry_out).collect(),
        deferred_ops: deferred_ops.iter().map(op_out).collect(),
    }
}

fn client_out(frame: &ClientFrame) -> Client<'_> {
    match frame {
        ClientFrame::Hello { versions, ident } => Client::Hello {
            versions: *versions,
            ident: borrowed(ident),
        },
        ClientFrame::Subscribe {
            channel,
            push_depth,
            retain_depth,
            resume,
        } => Client::Subscribe {
            channel: borrowed(channel),
            push_depth: *push_depth,
            retain_depth: *retain_depth,
            resume: resume.as_ref().map(Cow::Borrowed),
        },
        ClientFrame::Unsubscribe { channel } => Client::Unsubscribe {
            channel: borrowed(channel),
        },
        ClientFrame::Publish {
            channel,
            attribution,
            body,
            urgency,
            correlation,
        } => Client::Publish {
            channel: borrowed(channel),
            attribution: attribution.as_deref().map(borrowed),
            body: client_body(body),
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
        ),
        ClientFrame::Alert {
            attribution,
            severity,
            title,
            body,
        } => Client::Alert {
            attribution: attribution.as_deref().map(borrowed),
            severity: *severity,
            title: borrowed(title),
            body: borrowed(body),
        },
    }
}

fn outcome_out(outcome: PublishOutcome) -> Outcome {
    match outcome {
        PublishOutcome::Ok => Outcome::Ok,
        PublishOutcome::RateLimited => Outcome::RateLimited,
        PublishOutcome::BodyTooLarge { len, max } => Outcome::BodyTooLarge { len, max },
        PublishOutcome::Failed => Outcome::Failed,
        PublishOutcome::TextOnlyChannel => {
            panic!("attach v4 cannot write a TextOnlyChannel publish outcome")
        }
    }
}

fn batch_outcome_out(outcome: PublishBatchOutcome) -> BatchOutcome {
    match outcome {
        PublishBatchOutcome::Ok => BatchOutcome::Ok,
        PublishBatchOutcome::RateLimited => BatchOutcome::RateLimited,
        PublishBatchOutcome::TextOnlyChannel => {
            panic!("attach v4 cannot write a TextOnlyChannel batch outcome")
        }
    }
}

fn row_out(row: &DeliverRow) -> Row<'_> {
    row.envelope.require_text_body("attach v4 Deliver");
    Row {
        envelope: Cow::Borrowed(&row.envelope),
        seq: row.seq,
        cursor: Cow::Borrowed(&row.cursor),
        dropped: row.dropped,
    }
}

fn server_out(frame: &ServerFrame) -> Server<'_> {
    match frame {
        ServerFrame::Hello { versions, ident } => Server::Hello {
            versions: *versions,
            ident: borrowed(ident),
        },
        ServerFrame::Welcome {
            version,
            participant_id,
            session_id,
            heartbeat_secs,
            max_body_bytes,
            max_frame_bytes,
            alert_granted,
        } => Server::Welcome {
            version: *version,
            participant_id: borrowed(participant_id),
            session_id: borrowed(session_id),
            heartbeat_secs: *heartbeat_secs,
            max_body_bytes: *max_body_bytes,
            max_frame_bytes: *max_frame_bytes,
            alert_granted: *alert_granted,
        },
        ServerFrame::Heartbeat => Server::Heartbeat,
        ServerFrame::SubscribeResult {
            channel,
            outcome,
            replay_count,
            gap,
        } => Server::SubscribeResult {
            channel: borrowed(channel),
            outcome: *outcome,
            replay_count: *replay_count,
            gap: *gap,
        },
        ServerFrame::Deliver { channel, rows } => Server::Deliver {
            channel: borrowed(channel),
            rows: rows.iter().map(row_out).collect(),
        },
        ServerFrame::PublishResult {
            correlation,
            outcome,
        } => Server::PublishResult {
            correlation: *correlation,
            outcome: outcome_out(*outcome),
        },
        ServerFrame::PublishBatchResult {
            correlation,
            outcome,
        } => Server::PublishBatchResult {
            correlation: *correlation,
            outcome: batch_outcome_out(*outcome),
        },
        ServerFrame::DeferredView {
            channel,
            attribution,
            entries,
        } => Server::DeferredView {
            channel: borrowed(channel),
            attribution: attribution.as_deref().map(borrowed),
            entries: entries
                .iter()
                .map(|entry| ViewEntry {
                    message_id: entry.message_id,
                    body: server_body(&entry.body, "DeferredView"),
                    deliver_after: entry.deliver_after,
                })
                .collect(),
        },
    }
}

fn client_in(frame: Client<'_>) -> ClientFrame {
    match frame {
        Client::Hello { versions, ident } => ClientFrame::Hello {
            versions,
            ident: owned(ident),
        },
        Client::Subscribe {
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
        Client::Unsubscribe { channel } => ClientFrame::Unsubscribe {
            channel: owned(channel),
        },
        Client::Publish {
            channel,
            attribution,
            body,
            urgency,
            correlation,
        } => ClientFrame::Publish {
            channel: owned(channel),
            attribution: attribution.map(owned),
            body: read_body(body),
            urgency,
            correlation,
        },
        Client::PublishBatch {
            attribution,
            correlation,
            publishes,
            deferred_ops,
        } => ClientFrame::PublishBatch {
            attribution: attribution.map(owned),
            correlation,
            publishes: publishes
                .into_iter()
                .map(|entry| BatchEntry {
                    channel: owned(entry.channel),
                    body: read_body(entry.body),
                    urgency: entry.urgency,
                    deliver_after: entry.deliver_after,
                })
                .collect(),
            deferred_ops: deferred_ops
                .into_iter()
                .map(|op| BatchDeferredOp {
                    channel: owned(op.channel),
                    message_id: op.message_id,
                    op: match op.op {
                        OpKind::Cancel => DeferredOpKind::Cancel,
                        OpKind::Edit {
                            body,
                            deliver_after,
                        } => DeferredOpKind::Edit {
                            body: body.map(read_body),
                            deliver_after,
                        },
                    },
                })
                .collect(),
        },
        Client::Alert {
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

fn server_in(frame: Server<'_>) -> ServerFrame {
    match frame {
        Server::Hello { versions, ident } => ServerFrame::Hello {
            versions,
            ident: owned(ident),
        },
        Server::Welcome {
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
        Server::Heartbeat => ServerFrame::Heartbeat,
        Server::SubscribeResult {
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
        Server::Deliver { channel, rows } => ServerFrame::Deliver {
            channel: owned(channel),
            rows: rows
                .into_iter()
                .map(|row| DeliverRow {
                    envelope: row.envelope.into_owned(),
                    seq: row.seq,
                    cursor: row.cursor.into_owned(),
                    dropped: row.dropped,
                })
                .collect(),
        },
        Server::PublishResult {
            correlation,
            outcome,
        } => ServerFrame::PublishResult {
            correlation,
            outcome: match outcome {
                Outcome::Ok => PublishOutcome::Ok,
                Outcome::RateLimited => PublishOutcome::RateLimited,
                Outcome::BodyTooLarge { len, max } => PublishOutcome::BodyTooLarge { len, max },
                Outcome::Failed => PublishOutcome::Failed,
            },
        },
        Server::PublishBatchResult {
            correlation,
            outcome,
        } => ServerFrame::PublishBatchResult {
            correlation,
            outcome: match outcome {
                BatchOutcome::Ok => PublishBatchOutcome::Ok,
                BatchOutcome::RateLimited => PublishBatchOutcome::RateLimited,
            },
        },
        Server::DeferredView {
            channel,
            attribution,
            entries,
        } => ServerFrame::DeferredView {
            channel: owned(channel),
            attribution: attribution.map(owned),
            entries: entries
                .into_iter()
                .map(|entry| DeferredViewEntry {
                    message_id: entry.message_id,
                    body: read_body(entry.body),
                    deliver_after: entry.deliver_after,
                })
                .collect(),
        },
    }
}

pub(super) fn encode_client(frame: &ClientFrame) -> WireMessage {
    WireMessage::Text(
        serde_json::to_string(&client_out(frame))
            .expect("attach v4: a client frame serializes to JSON"),
    )
}

pub(super) fn encode_server(frame: &ServerFrame) -> WireMessage {
    WireMessage::Text(
        serde_json::to_string(&server_out(frame))
            .expect("attach v4: a server frame serializes to JSON"),
    )
}

pub(super) fn decode_client(msg: WireMessage) -> Result<ClientFrame, DecodeError> {
    match msg {
        WireMessage::Text(text) => serde_json::from_str::<Client<'_>>(&text)
            .map(client_in)
            .map_err(|err| DecodeError::with_detail("unparseable client frame", err)),
        WireMessage::Binary(_) => Err(DecodeError::new("binary frame")),
    }
}

pub(super) fn decode_server(msg: WireMessage) -> Result<ServerFrame, DecodeError> {
    match msg {
        WireMessage::Text(text) => serde_json::from_str::<Server<'_>>(&text)
            .map(server_in)
            .map_err(|err| DecodeError::with_detail("unparseable server frame", err)),
        WireMessage::Binary(_) => Err(DecodeError::new("unexpected binary frame from peer")),
    }
}

/// Every variable-length string the two lists carry: each operation's channel
/// address plus each body it publishes or writes, as raw bytes.
pub(super) fn batch_charged_bytes(
    publishes: &[BatchEntry],
    deferred_ops: &[BatchDeferredOp],
) -> usize {
    let entries: usize = publishes
        .iter()
        .map(|entry| entry.channel.len() + entry.body.len())
        .sum();
    let ops: usize = deferred_ops
        .iter()
        .map(|op| {
            op.channel.len()
                + match &op.op {
                    DeferredOpKind::Cancel => 0,
                    DeferredOpKind::Edit { body, .. } => body.as_ref().map_or(0, MessageBody::len),
                }
        })
        .sum();
    entries + ops
}

/// The escaped charged budget, plus a fixed allowance per batch operation for
/// the fields no budget charges, plus the base overhead a single publish is
/// sized with.
pub(super) fn max_client_frame_bytes(max_body_bytes: usize) -> usize {
    // Checked, not wrapping: this value gates a fail2ban decision (an oversized
    // frame is a protocol violation), and it must equal the number a 32-bit wasm
    // client honours. An operator `max_body_bytes` large enough to overflow is a
    // config contradiction — fail fast rather than derive a wrong cap.
    max_body_bytes
        .checked_mul(JSON_ESCAPE_EXPANSION)
        .and_then(|scaled| scaled.checked_add(MAX_BATCH_OPS * BATCH_OP_OVERHEAD_BYTES))
        .and_then(|scaled| scaled.checked_add(PUBLISH_FRAME_OVERHEAD_BYTES))
        .expect("max_body_bytes too large: WS frame-cap derivation overflowed usize")
}

pub(super) fn batch_frame_bytes(
    attribution: Option<&str>,
    publishes: &[BatchEntry],
    deferred_ops: &[BatchDeferredOp],
) -> usize {
    serde_json::to_string(&batch_out(attribution, u64::MAX, publishes, deferred_ops))
        .expect("attach v4: a PublishBatch frame serializes to JSON")
        .len()
}

/// The two clauses over v4's count and charge.
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

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::super::Codec;
    use crate::wire::WireMessage;
    use crate::{
        BATCH_OP_OVERHEAD_BYTES, BatchDeferredOp, BatchEntry, BatchIllegal, ClientFrame,
        DeferredOpKind, MAX_BATCH_OPS, PUBLISH_FRAME_OVERHEAD_BYTES, ServerFrame, Urgency,
    };

    fn v4() -> Codec {
        Codec::negotiated(4)
    }

    #[test]
    fn the_frame_cap_clears_the_worst_case_escaping_of_a_legal_body() {
        assert_eq!(
            v4().max_client_frame_bytes(64 * 1024),
            64 * 1024 * 6 + MAX_BATCH_OPS * BATCH_OP_OVERHEAD_BYTES + PUBLISH_FRAME_OVERHEAD_BYTES
        );
        assert_eq!(v4().max_client_frame_bytes(64 * 1024), 532_480);
    }

    #[test]
    #[should_panic(expected = "WS frame-cap derivation overflowed")]
    fn an_unrepresentable_body_cap_panics_rather_than_deriving_a_wrong_frame_cap() {
        v4().max_client_frame_bytes(usize::MAX);
    }

    /// One batch entry charging `charge` bytes across its channel and body, both
    /// filled with the control character that escapes six-for-one.
    fn worst_case_entry(charge: usize) -> BatchEntry {
        let channel = "\u{1}".repeat(charge / 2);
        BatchEntry {
            channel,
            body: "\u{1}".repeat(charge - charge / 2).into(),
            urgency: Urgency::VeryLow,
            deliver_after: Some(u64::MAX),
        }
    }

    /// One control op charging `charge` bytes, in its widest shape: an edit that
    /// rewrites both halves, so it carries a body, a release time and a uuid.
    fn worst_case_op(charge: usize) -> BatchDeferredOp {
        BatchDeferredOp {
            channel: "\u{1}".repeat(charge / 2),
            message_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
            op: DeferredOpKind::Edit {
                body: Some("\u{1}".repeat(charge - charge / 2).into()),
                deliver_after: Some(u64::MAX),
            },
        }
    }

    /// The law and the derivation are one contract: the widest batch legality
    /// admits must serialize under the cap the peer reads with. Anything else
    /// puts a conforming attacher back where the two contracts contradicted —
    /// composing a legal flush its peer takes for tampering.
    ///
    /// The op mix is swept as well as the body cap, because the two halves of the
    /// derivation answer to different inputs. The cap varies the *charged* bytes,
    /// which the escape allowance covers; the mix varies the *fixed* bytes, which
    /// `BATCH_OP_OVERHEAD_BYTES` covers — and the two op shapes do not weigh the
    /// same there, since an edit carries a uuid, a kind tag and a release time an
    /// entry does not. All-ops is the maximizing shape, so a fixture that only
    /// ever ran the even mix would leave the overhead constant free to shrink
    /// under the widest legal batch.
    #[test]
    fn the_frame_cap_bounds_the_worst_legal_batch() {
        let attribution = "instance-that-a-document-declared";
        for max_body_bytes in [1_024_usize, 64 * 1024, 1024 * 1024] {
            let charge = max_body_bytes / MAX_BATCH_OPS;
            for entry_count in [MAX_BATCH_OPS, MAX_BATCH_OPS / 2, 0] {
                let op_count = MAX_BATCH_OPS - entry_count;
                let publishes: Vec<BatchEntry> =
                    (0..entry_count).map(|_| worst_case_entry(charge)).collect();
                let deferred_ops: Vec<BatchDeferredOp> =
                    (0..op_count).map(|_| worst_case_op(charge)).collect();
                v4().check_batch_legality(&publishes, &deferred_ops, max_body_bytes)
                    .expect("the batch is built to the law");
                let measured = v4().batch_frame_bytes(Some(attribution), &publishes, &deferred_ops);
                let frame = ClientFrame::PublishBatch {
                    attribution: Some(attribution.to_string()),
                    correlation: u64::MAX,
                    publishes,
                    deferred_ops,
                };
                let WireMessage::Text(text) = v4().encode_client(&frame) else {
                    panic!("a v4 frame is text");
                };
                assert_eq!(measured, text.len());
                let cap = v4().max_client_frame_bytes(max_body_bytes);
                assert!(
                    text.len() <= cap,
                    "{entry_count} entries + {op_count} ops at a {max_body_bytes}-byte cap \
                     serialize to {} bytes, over the {cap}-byte cap",
                    text.len()
                );
            }
        }
    }

    /// Both clauses, at and one past their boundaries.
    #[test]
    fn batch_legality_holds_at_its_boundaries() {
        let entry = |charge| worst_case_entry(charge);
        // Charged bytes: the budget is the body cap, spent across the batch.
        assert_eq!(v4().check_batch_legality(&[entry(64)], &[], 64), Ok(()));
        assert_eq!(
            v4().check_batch_legality(&[entry(64), entry(1)], &[], 64),
            Err(BatchIllegal::TooManyBytes { bytes: 65, cap: 64 })
        );
        // A channel address is charged like a body: it is client-composed, so a
        // rule that ignored it would bound nothing.
        assert_eq!(
            v4().batch_charged_bytes(
                &[BatchEntry {
                    channel: "ephemeral:x".to_string(),
                    body: "hello".into(),
                    urgency: Urgency::Normal,
                    deliver_after: None,
                }],
                &[]
            ),
            "ephemeral:x".len() + "hello".len()
        );
        // A cancel op charges its channel and nothing else.
        assert_eq!(
            v4().batch_charged_bytes(
                &[],
                &[BatchDeferredOp {
                    channel: "ephemeral:x".to_string(),
                    message_id: Uuid::nil(),
                    op: DeferredOpKind::Cancel,
                }]
            ),
            "ephemeral:x".len()
        );
        // Op count: publishes and ops share one ceiling, because they share one
        // frame.
        let empty = worst_case_entry(0);
        let full: Vec<BatchEntry> = (0..MAX_BATCH_OPS).map(|_| empty.clone()).collect();
        assert_eq!(v4().check_batch_legality(&full, &[], 1), Ok(()));
        assert_eq!(
            v4().check_batch_legality(&full[..MAX_BATCH_OPS - 1], &[worst_case_op(0)], 1),
            Ok(())
        );
        assert_eq!(
            v4().check_batch_legality(&full, &[worst_case_op(0)], 1),
            Err(BatchIllegal::TooManyOps {
                ops: MAX_BATCH_OPS + 1,
                cap: MAX_BATCH_OPS,
            })
        );
    }

    #[test]
    fn a_binary_message_is_never_a_v4_frame() {
        let client = v4()
            .decode_client(WireMessage::Binary(vec![b'{', b'}']))
            .unwrap_err();
        assert_eq!(client.rule(), "binary frame");
        assert_eq!(client.to_string(), "binary frame");
        let server = v4()
            .decode_server(WireMessage::Binary(Vec::new()))
            .unwrap_err();
        assert_eq!(server.to_string(), "unexpected binary frame from peer");
    }

    /// The rule is what a server's violation names; serde's message rides
    /// beside it for a client's own diagnostics.
    #[test]
    fn an_unparseable_text_names_its_rule_and_keeps_serdes_message() {
        let text = r#"{"type":"Subscribe","channel":"ephemeral:demo","push_depth":1}"#;
        let client = v4()
            .decode_client(WireMessage::Text(text.to_string()))
            .unwrap_err();
        assert_eq!(client.rule(), "unparseable client frame");
        assert!(
            client.detail().unwrap().contains("retain_depth"),
            "{client}"
        );
        let server = v4()
            .decode_server(WireMessage::Text(r#"{"type":"Heartbeat","x":"#.to_string()))
            .unwrap_err();
        assert_eq!(server.rule(), "unparseable server frame");
        assert!(
            server.to_string().starts_with("unparseable server frame: "),
            "{server}"
        );
    }

    /// The handshake frame decodes under the codec as well as through the
    /// shell, so a handler can still answer a second `Hello` as such.
    #[test]
    fn hello_still_decodes_under_the_codec() {
        let text = r#"{"type":"Hello","versions":{"min":4,"max":4},"ident":"x"}"#;
        assert!(matches!(
            v4().decode_client(WireMessage::Text(text.to_string())),
            Ok(ClientFrame::Hello { .. })
        ));
        assert!(matches!(
            v4().decode_server(WireMessage::Text(text.to_string())),
            Ok(ServerFrame::Hello { .. })
        ));
    }
}

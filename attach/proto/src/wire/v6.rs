//! Protocol version 6: v5 plus blob transfer.
//!
//! v6 has v5's binary layout and v5's decoder discipline, and reuses v5's
//! machinery for both: the prefix and header split, the body-section cursor,
//! the descriptors, and the shapes of every frame that did not change. It
//! holds its own schema, so v5's strictness is untouched: a v5 session's
//! decoder has no blob kind and no `attachments` field.
//!
//! What v6 adds to v5:
//!
//! - Text client frames `BlobCreate {handle, content_type, size}`,
//!   `BlobSeal {handle}`, `BlobOpen {handle, channel, message_id, name}`,
//!   `BlobRead {correlation, handle, offset, len}` and `BlobClose {handle}`.
//! - A binary client frame `BlobWrite`, whose header is
//!   `{"type":"BlobWrite","handle":H,"offset":O,"len":N}` and whose body
//!   section is exactly the `N` bytes written.
//! - `attachments` on `Publish` and on each `PublishBatch` entry: a list of
//!   `{"name":"…","handle":H}`, absent when empty. A list, not an object, so
//!   a repeated name is representable and refused rather than merged.
//! - Text server frames `BlobCreateResult`, `BlobSealResult` and
//!   `BlobOpenResult`, each `{handle, outcome}`.
//! - A binary server frame `BlobReadResult {correlation, outcome}`, whose
//!   `Ok` outcome is `{"kind":"Ok","len":N}` with exactly `N` bytes in the
//!   body section; every other outcome has an empty body section.
//! - `blobs` on `Welcome`, absent when the session has no blob door, and
//!   `AttachmentNotHeld` on both publish outcomes.
//!
//! A frame carrying none of the new fields is written as v5 writes it.
//!
//! **Every tagged variant is a struct variant.** serde's `deny_unknown_fields`
//! does not refuse an extra key beside the tag of an internally tagged unit
//! variant; a field-less struct variant writes the same `{"kind":"Ok"}` and
//! refuses `{"kind":"Ok","x":0}`. So v6 holds its own copy of every tagged
//! shape it shares with v5 or with the in-memory frames — the deferred op,
//! `Heartbeat`, and every outcome and gap reason — with each unit variant
//! written `{}`.
//!
//! **Decoding.** A `BlobWrite` or `BlobReadResult` `len` is the peer's number,
//! compared as a `u64` with the bytes remaining and narrowed only then, under
//! the rule `frame header and bodies disagree`, as a body descriptor's is.
//! Offsets and sizes are `u64` and are never summed here. An attachment name
//! outside the grammar, in `BlobOpen` or in an `attachments` list, is the
//! rule `malformed attachment name`.
//!
//! **The batch law** charges what v5 charges, plus, per attachment, its name's
//! length and [`ATTACHMENT_OVERHEAD_BYTES`]. **The frame cap** is
//!
//! ```text
//! max(max_body_bytes + MAX_BATCH_OPS × BATCH_OP_OVERHEAD_BYTES, BLOB_CHUNK_BYTES)
//!     + PUBLISH_FRAME_OVERHEAD_BYTES
//!     + MAX_ATTACHMENTS_PER_MESSAGE × (MAX_ATTACHMENT_NAME_LEN + ATTACHMENT_OVERHEAD_BYTES)
//! ```
//!
//! 274,432 bytes at a 64 KiB body cap. The last term covers a single
//! `Publish`'s attachments, which no budget charges; a batch's attachments
//! spend the batch's body budget.

use std::borrow::Cow;

use brenn_envelope::MessageBody;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::v5::{
    self, BodiesIn, BodiesOut, Desc, Row, UNPARSEABLE_CLIENT, UNPARSEABLE_SERVER, ViewEntry,
    binary, borrowed, owned, parse_header, parse_text, split, text,
};
use super::{DecodeError, WireMessage};
use crate::{
    ATTACHMENT_OVERHEAD_BYTES, AlertSeverity, AttachmentName, BATCH_OP_OVERHEAD_BYTES,
    BLOB_CHUNK_BYTES, BatchDeferredOp, BatchEntry, BatchIllegal, BlobCreateOutcome, BlobFacts,
    BlobHandle, BlobOpenOutcome, BlobReadOutcome, BlobSealOutcome, ClientFrame, Cursor,
    DeferredOpKind, GapInfo, GapReason, MAX_ATTACHMENT_NAME_LEN, MAX_ATTACHMENTS_PER_MESSAGE,
    MAX_BATCH_OPS, PUBLISH_FRAME_OVERHEAD_BYTES, PublishAttachment, PublishBatchOutcome,
    PublishOutcome, ServerFrame, SubscribeOutcome, Urgency, VersionRange,
};

#[cfg(test)]
mod tests;

const MALFORMED_ATTACHMENT_NAME: &str = "malformed attachment name";

const CLIENT_BINARY_KINDS: [&str; 3] = ["Publish", "PublishBatch", "BlobWrite"];
const CLIENT_TEXT_KINDS: [&str; 9] = [
    "Hello",
    "Subscribe",
    "Unsubscribe",
    "Alert",
    "BlobCreate",
    "BlobSeal",
    "BlobOpen",
    "BlobRead",
    "BlobClose",
];
const SERVER_BINARY_KINDS: [&str; 3] = ["Deliver", "DeferredView", "BlobReadResult"];
const SERVER_TEXT_KINDS: [&str; 9] = [
    "Hello",
    "Welcome",
    "Heartbeat",
    "SubscribeResult",
    "PublishResult",
    "PublishBatchResult",
    "BlobCreateResult",
    "BlobSealResult",
    "BlobOpenResult",
];

// ---------------------------------------------------------------------------
// The v6 schema. Borrowed when written, owned when read.
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
    BlobCreate {
        handle: BlobHandle,
        content_type: Cow<'a, str>,
        size: u64,
    },
    BlobSeal {
        handle: BlobHandle,
    },
    BlobOpen {
        handle: BlobHandle,
        channel: Cow<'a, str>,
        message_id: Uuid,
        name: Cow<'a, str>,
    },
    BlobRead {
        correlation: u64,
        handle: BlobHandle,
        offset: u64,
        len: u64,
    },
    BlobClose {
        handle: BlobHandle,
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
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<Attachment<'a>>,
    },
    PublishBatch {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attribution: Option<Cow<'a, str>>,
        correlation: u64,
        publishes: Vec<Entry<'a>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        deferred_ops: Vec<Op<'a>>,
    },
    BlobWrite {
        handle: BlobHandle,
        offset: u64,
        len: u64,
    },
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
    Cancel {},
    Edit {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        body: Option<Desc<'a>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deliver_after: Option<u64>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry<'a> {
    channel: Cow<'a, str>,
    body: Desc<'a>,
    urgency: Urgency,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deliver_after: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    attachments: Vec<Attachment<'a>>,
}

/// One named handle of a publish. The name is read as a string and built
/// through the grammar, which the envelope's own deserializer does not check.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attachment<'a> {
    name: Cow<'a, str>,
    handle: BlobHandle,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        blobs: Option<BlobFacts>,
    },
    Heartbeat {},
    SubscribeResult {
        channel: Cow<'a, str>,
        outcome: SubscribeKind,
        replay_count: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gap: Option<Gap>,
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
    BlobCreateResult {
        handle: BlobHandle,
        outcome: CreateOutcome,
    },
    BlobSealResult {
        handle: BlobHandle,
        outcome: SealOutcome,
    },
    BlobOpenResult {
        handle: BlobHandle,
        outcome: OpenOutcome<'a>,
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
    BlobReadResult {
        correlation: u64,
        outcome: ReadOutcome,
    },
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Outcome {
    Ok {},
    RateLimited {},
    BodyTooLarge { len: u64, max: u64 },
    Failed {},
    TextOnlyChannel {},
    AttachmentNotHeld {},
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum BatchOutcome {
    Ok {},
    RateLimited {},
    TextOnlyChannel {},
    AttachmentNotHeld {},
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum SubscribeKind {
    Ok {},
    Unavailable {},
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Gap {
    reason: GapKind,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum GapKind {
    EpochChanged {},
    BeyondRetained {},
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum CreateOutcome {
    Ok {},
    BlobTooLarge { size: u64, max: u64 },
    BudgetExhausted {},
    HandleLimit {},
    BytesHeldLimit {},
    StorageFailed {},
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum SealOutcome {
    Ok {},
    Incomplete { offset: u64, len: u64 },
    UnknownHandle {},
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum OpenOutcome<'a> {
    Ok {
        size: u64,
        content_type: Cow<'a, str>,
    },
    NotRetained {},
    NoSuchAttachment {},
    ToolChannel {},
    HandleLimit {},
    BytesHeldLimit {
        size: u64,
    },
}

/// A read's outcome in the header: `Ok` states the length of the bytes in
/// the body section.
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum ReadOutcome {
    Ok { len: u64 },
    OutOfRange { size: u64 },
    UnknownHandle {},
    NotSealed {},
    TooManyReads {},
    StorageFailed {},
}

/// v6 writes every body as its bytes.
pub(super) const CARRIES_NON_TEXT_BODIES: bool = true;

/// v6 writes every body.
pub(super) fn carries(_body: &MessageBody) -> bool {
    true
}

fn len_of(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).expect("a byte length fits u64")
}

fn attachments_out(attachments: &[PublishAttachment]) -> Vec<Attachment<'_>> {
    attachments
        .iter()
        .map(|attachment| Attachment {
            name: borrowed(attachment.name.as_str()),
            handle: attachment.handle,
        })
        .collect()
}

fn attachment_name(name: Cow<'_, str>) -> Result<AttachmentName, DecodeError> {
    AttachmentName::new(name.into_owned())
        .map_err(|err| DecodeError::with_detail(MALFORMED_ATTACHMENT_NAME, err))
}

fn attachments_in(attachments: Vec<Attachment<'_>>) -> Result<Vec<PublishAttachment>, DecodeError> {
    attachments
        .into_iter()
        .map(|attachment| {
            Ok(PublishAttachment::new(
                attachment_name(attachment.name)?,
                attachment.handle,
            ))
        })
        .collect()
}

/// A batch's control ops read from the header shape, their bodies cut from
/// the section in order.
fn ops_in(
    ops: Vec<Op<'_>>,
    bodies: &mut BodiesIn<'_>,
) -> Result<Vec<BatchDeferredOp>, DecodeError> {
    ops.into_iter()
        .map(|op| {
            Ok(BatchDeferredOp {
                channel: owned(op.channel),
                message_id: op.message_id,
                op: match op.op {
                    OpKind::Cancel {} => DeferredOpKind::Cancel,
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
        .collect()
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
            attachments: attachments_out(&entry.attachments),
        })
        .collect();
    ClientBinary::PublishBatch {
        attribution: attribution.map(borrowed),
        correlation,
        publishes,
        deferred_ops: ops_out(deferred_ops, bodies),
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
        ClientFrame::BlobCreate {
            handle,
            content_type,
            size,
        } => {
            return text(&ClientText::BlobCreate {
                handle: *handle,
                content_type: borrowed(content_type),
                size: *size,
            });
        }
        ClientFrame::BlobSeal { handle } => {
            return text(&ClientText::BlobSeal { handle: *handle });
        }
        ClientFrame::BlobOpen {
            handle,
            channel,
            message_id,
            name,
        } => {
            return text(&ClientText::BlobOpen {
                handle: *handle,
                channel: borrowed(channel),
                message_id: *message_id,
                name: borrowed(name.as_str()),
            });
        }
        ClientFrame::BlobRead {
            correlation,
            handle,
            offset,
            len,
        } => {
            return text(&ClientText::BlobRead {
                correlation: *correlation,
                handle: *handle,
                offset: *offset,
                len: *len,
            });
        }
        ClientFrame::BlobClose { handle } => {
            return text(&ClientText::BlobClose { handle: *handle });
        }
        ClientFrame::Publish {
            channel,
            attribution,
            body,
            urgency,
            correlation,
            attachments,
        } => ClientBinary::Publish {
            channel: borrowed(channel),
            attribution: attribution.as_deref().map(borrowed),
            body: bodies.describe(body),
            urgency: *urgency,
            correlation: *correlation,
            attachments: attachments_out(attachments),
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
        ClientFrame::BlobWrite {
            handle,
            offset,
            bytes,
        } => {
            bodies.raw(bytes);
            ClientBinary::BlobWrite {
                handle: *handle,
                offset: *offset,
                len: len_of(bytes),
            }
        }
    };
    binary(&header, &bodies)
}

fn outcome_out(outcome: PublishOutcome) -> Outcome {
    match outcome {
        PublishOutcome::Ok => Outcome::Ok {},
        PublishOutcome::RateLimited => Outcome::RateLimited {},
        PublishOutcome::BodyTooLarge { len, max } => Outcome::BodyTooLarge { len, max },
        PublishOutcome::Failed => Outcome::Failed {},
        PublishOutcome::TextOnlyChannel => Outcome::TextOnlyChannel {},
        PublishOutcome::AttachmentNotHeld => Outcome::AttachmentNotHeld {},
    }
}

fn batch_outcome_out(outcome: PublishBatchOutcome) -> BatchOutcome {
    match outcome {
        PublishBatchOutcome::Ok => BatchOutcome::Ok {},
        PublishBatchOutcome::RateLimited => BatchOutcome::RateLimited {},
        PublishBatchOutcome::TextOnlyChannel => BatchOutcome::TextOnlyChannel {},
        PublishBatchOutcome::AttachmentNotHeld => BatchOutcome::AttachmentNotHeld {},
    }
}

fn subscribe_outcome_out(outcome: SubscribeOutcome) -> SubscribeKind {
    match outcome {
        SubscribeOutcome::Ok => SubscribeKind::Ok {},
        SubscribeOutcome::Unavailable => SubscribeKind::Unavailable {},
    }
}

fn gap_out(gap: GapInfo) -> Gap {
    Gap {
        reason: match gap.reason {
            GapReason::EpochChanged => GapKind::EpochChanged {},
            GapReason::BeyondRetained => GapKind::BeyondRetained {},
        },
    }
}

fn create_outcome_out(outcome: BlobCreateOutcome) -> CreateOutcome {
    match outcome {
        BlobCreateOutcome::Ok => CreateOutcome::Ok {},
        BlobCreateOutcome::BlobTooLarge { size, max } => CreateOutcome::BlobTooLarge { size, max },
        BlobCreateOutcome::BudgetExhausted => CreateOutcome::BudgetExhausted {},
        BlobCreateOutcome::HandleLimit => CreateOutcome::HandleLimit {},
        BlobCreateOutcome::BytesHeldLimit => CreateOutcome::BytesHeldLimit {},
        BlobCreateOutcome::StorageFailed => CreateOutcome::StorageFailed {},
    }
}

fn seal_outcome_out(outcome: BlobSealOutcome) -> SealOutcome {
    match outcome {
        BlobSealOutcome::Ok => SealOutcome::Ok {},
        BlobSealOutcome::Incomplete { offset, len } => SealOutcome::Incomplete { offset, len },
        BlobSealOutcome::UnknownHandle => SealOutcome::UnknownHandle {},
    }
}

fn open_outcome_out(outcome: &BlobOpenOutcome) -> OpenOutcome<'_> {
    match outcome {
        BlobOpenOutcome::Ok { size, content_type } => OpenOutcome::Ok {
            size: *size,
            content_type: borrowed(content_type),
        },
        BlobOpenOutcome::NotRetained => OpenOutcome::NotRetained {},
        BlobOpenOutcome::NoSuchAttachment => OpenOutcome::NoSuchAttachment {},
        BlobOpenOutcome::ToolChannel => OpenOutcome::ToolChannel {},
        BlobOpenOutcome::HandleLimit => OpenOutcome::HandleLimit {},
        BlobOpenOutcome::BytesHeldLimit { size } => OpenOutcome::BytesHeldLimit { size: *size },
    }
}

/// A batch's control ops in the header shape, their bodies described in
/// order.
fn ops_out<'a>(deferred_ops: &'a [BatchDeferredOp], bodies: &mut BodiesOut<'a>) -> Vec<Op<'a>> {
    deferred_ops
        .iter()
        .map(|op| Op {
            channel: borrowed(&op.channel),
            message_id: op.message_id,
            op: match &op.op {
                DeferredOpKind::Cancel => OpKind::Cancel {},
                DeferredOpKind::Edit {
                    body,
                    deliver_after,
                } => OpKind::Edit {
                    body: body.as_ref().map(|body| bodies.describe(body)),
                    deliver_after: *deliver_after,
                },
            },
        })
        .collect()
}

/// A read's outcome in the header, its bytes (if any) appended to the body
/// section.
fn read_outcome_out<'a>(outcome: &'a BlobReadOutcome, bodies: &mut BodiesOut<'a>) -> ReadOutcome {
    match outcome {
        BlobReadOutcome::Ok { bytes } => {
            bodies.raw(bytes);
            ReadOutcome::Ok { len: len_of(bytes) }
        }
        BlobReadOutcome::OutOfRange { size } => ReadOutcome::OutOfRange { size: *size },
        BlobReadOutcome::UnknownHandle => ReadOutcome::UnknownHandle {},
        BlobReadOutcome::NotSealed => ReadOutcome::NotSealed {},
        BlobReadOutcome::TooManyReads => ReadOutcome::TooManyReads {},
        BlobReadOutcome::StorageFailed => ReadOutcome::StorageFailed {},
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
            blobs,
        } => {
            return text(&ServerText::Welcome {
                version: *version,
                participant_id: borrowed(participant_id),
                session_id: borrowed(session_id),
                heartbeat_secs: *heartbeat_secs,
                max_body_bytes: *max_body_bytes,
                max_frame_bytes: *max_frame_bytes,
                alert_granted: *alert_granted,
                blobs: *blobs,
            });
        }
        ServerFrame::Heartbeat => return text(&ServerText::Heartbeat {}),
        ServerFrame::SubscribeResult {
            channel,
            outcome,
            replay_count,
            gap,
        } => {
            return text(&ServerText::SubscribeResult {
                channel: borrowed(channel),
                outcome: subscribe_outcome_out(*outcome),
                replay_count: *replay_count,
                gap: gap.map(gap_out),
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
        ServerFrame::BlobCreateResult { handle, outcome } => {
            return text(&ServerText::BlobCreateResult {
                handle: *handle,
                outcome: create_outcome_out(*outcome),
            });
        }
        ServerFrame::BlobSealResult { handle, outcome } => {
            return text(&ServerText::BlobSealResult {
                handle: *handle,
                outcome: seal_outcome_out(*outcome),
            });
        }
        ServerFrame::BlobOpenResult { handle, outcome } => {
            return text(&ServerText::BlobOpenResult {
                handle: *handle,
                outcome: open_outcome_out(outcome),
            });
        }
        ServerFrame::Deliver { channel, rows } => ServerBinary::Deliver {
            channel: borrowed(channel),
            rows: v5::rows_out(rows, &mut bodies),
        },
        ServerFrame::DeferredView {
            channel,
            attribution,
            entries,
        } => ServerBinary::DeferredView {
            channel: borrowed(channel),
            attribution: attribution.as_deref().map(borrowed),
            entries: v5::view_out(entries, &mut bodies),
        },
        ServerFrame::BlobReadResult {
            correlation,
            outcome,
        } => ServerBinary::BlobReadResult {
            correlation: *correlation,
            outcome: read_outcome_out(outcome, &mut bodies),
        },
    };
    binary(&header, &bodies)
}

fn client_text_in(frame: ClientText<'_>) -> Result<ClientFrame, DecodeError> {
    Ok(match frame {
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
        ClientText::BlobCreate {
            handle,
            content_type,
            size,
        } => ClientFrame::BlobCreate {
            handle,
            content_type: owned(content_type),
            size,
        },
        ClientText::BlobSeal { handle } => ClientFrame::BlobSeal { handle },
        ClientText::BlobOpen {
            handle,
            channel,
            message_id,
            name,
        } => ClientFrame::BlobOpen {
            handle,
            channel: owned(channel),
            message_id,
            name: attachment_name(name)?,
        },
        ClientText::BlobRead {
            correlation,
            handle,
            offset,
            len,
        } => ClientFrame::BlobRead {
            correlation,
            handle,
            offset,
            len,
        },
        ClientText::BlobClose { handle } => ClientFrame::BlobClose { handle },
    })
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
            attachments,
        } => ClientFrame::Publish {
            channel: owned(channel),
            attribution: attribution.map(owned),
            body: bodies.take(body)?,
            urgency,
            correlation,
            attachments: attachments_in(attachments)?,
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
                        attachments: attachments_in(entry.attachments)?,
                    })
                })
                .collect::<Result<_, DecodeError>>()?;
            let deferred_ops = ops_in(deferred_ops, bodies)?;
            ClientFrame::PublishBatch {
                attribution: attribution.map(owned),
                correlation,
                publishes,
                deferred_ops,
            }
        }
        ClientBinary::BlobWrite {
            handle,
            offset,
            len,
        } => ClientFrame::BlobWrite {
            handle,
            offset,
            bytes: bodies.take_raw(len)?.to_vec(),
        },
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
            blobs,
        } => ServerFrame::Welcome {
            version,
            participant_id: owned(participant_id),
            session_id: owned(session_id),
            heartbeat_secs,
            max_body_bytes,
            max_frame_bytes,
            alert_granted,
            blobs,
        },
        ServerText::Heartbeat {} => ServerFrame::Heartbeat,
        ServerText::SubscribeResult {
            channel,
            outcome,
            replay_count,
            gap,
        } => ServerFrame::SubscribeResult {
            channel: owned(channel),
            outcome: match outcome {
                SubscribeKind::Ok {} => SubscribeOutcome::Ok,
                SubscribeKind::Unavailable {} => SubscribeOutcome::Unavailable,
            },
            replay_count,
            gap: gap.map(|gap| GapInfo {
                reason: match gap.reason {
                    GapKind::EpochChanged {} => GapReason::EpochChanged,
                    GapKind::BeyondRetained {} => GapReason::BeyondRetained,
                },
            }),
        },
        ServerText::PublishResult {
            correlation,
            outcome,
        } => ServerFrame::PublishResult {
            correlation,
            outcome: match outcome {
                Outcome::Ok {} => PublishOutcome::Ok,
                Outcome::RateLimited {} => PublishOutcome::RateLimited,
                Outcome::BodyTooLarge { len, max } => PublishOutcome::BodyTooLarge { len, max },
                Outcome::Failed {} => PublishOutcome::Failed,
                Outcome::TextOnlyChannel {} => PublishOutcome::TextOnlyChannel,
                Outcome::AttachmentNotHeld {} => PublishOutcome::AttachmentNotHeld,
            },
        },
        ServerText::PublishBatchResult {
            correlation,
            outcome,
        } => ServerFrame::PublishBatchResult {
            correlation,
            outcome: match outcome {
                BatchOutcome::Ok {} => PublishBatchOutcome::Ok,
                BatchOutcome::RateLimited {} => PublishBatchOutcome::RateLimited,
                BatchOutcome::TextOnlyChannel {} => PublishBatchOutcome::TextOnlyChannel,
                BatchOutcome::AttachmentNotHeld {} => PublishBatchOutcome::AttachmentNotHeld,
            },
        },
        ServerText::BlobCreateResult { handle, outcome } => ServerFrame::BlobCreateResult {
            handle,
            outcome: match outcome {
                CreateOutcome::Ok {} => BlobCreateOutcome::Ok,
                CreateOutcome::BlobTooLarge { size, max } => {
                    BlobCreateOutcome::BlobTooLarge { size, max }
                }
                CreateOutcome::BudgetExhausted {} => BlobCreateOutcome::BudgetExhausted,
                CreateOutcome::HandleLimit {} => BlobCreateOutcome::HandleLimit,
                CreateOutcome::BytesHeldLimit {} => BlobCreateOutcome::BytesHeldLimit,
                CreateOutcome::StorageFailed {} => BlobCreateOutcome::StorageFailed,
            },
        },
        ServerText::BlobSealResult { handle, outcome } => ServerFrame::BlobSealResult {
            handle,
            outcome: match outcome {
                SealOutcome::Ok {} => BlobSealOutcome::Ok,
                SealOutcome::Incomplete { offset, len } => {
                    BlobSealOutcome::Incomplete { offset, len }
                }
                SealOutcome::UnknownHandle {} => BlobSealOutcome::UnknownHandle,
            },
        },
        ServerText::BlobOpenResult { handle, outcome } => ServerFrame::BlobOpenResult {
            handle,
            outcome: match outcome {
                OpenOutcome::Ok { size, content_type } => BlobOpenOutcome::Ok {
                    size,
                    content_type: owned(content_type),
                },
                OpenOutcome::NotRetained {} => BlobOpenOutcome::NotRetained,
                OpenOutcome::NoSuchAttachment {} => BlobOpenOutcome::NoSuchAttachment,
                OpenOutcome::ToolChannel {} => BlobOpenOutcome::ToolChannel,
                OpenOutcome::HandleLimit {} => BlobOpenOutcome::HandleLimit,
                OpenOutcome::BytesHeldLimit { size } => BlobOpenOutcome::BytesHeldLimit { size },
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
            rows: v5::rows_in(rows, bodies)?,
        },
        ServerBinary::DeferredView {
            channel,
            attribution,
            entries,
        } => ServerFrame::DeferredView {
            channel: owned(channel),
            attribution: attribution.map(owned),
            entries: v5::view_in(entries, bodies)?,
        },
        ServerBinary::BlobReadResult {
            correlation,
            outcome,
        } => ServerFrame::BlobReadResult {
            correlation,
            outcome: match outcome {
                ReadOutcome::Ok { len } => BlobReadOutcome::Ok {
                    bytes: bodies.take_raw(len)?.to_vec(),
                },
                ReadOutcome::OutOfRange { size } => BlobReadOutcome::OutOfRange { size },
                ReadOutcome::UnknownHandle {} => BlobReadOutcome::UnknownHandle,
                ReadOutcome::NotSealed {} => BlobReadOutcome::NotSealed,
                ReadOutcome::TooManyReads {} => BlobReadOutcome::TooManyReads,
                ReadOutcome::StorageFailed {} => BlobReadOutcome::StorageFailed,
            },
        },
    })
}

pub(super) fn decode_client(msg: WireMessage) -> Result<ClientFrame, DecodeError> {
    match msg {
        WireMessage::Text(text) => {
            parse_text::<ClientText<'_>>(&text, &CLIENT_BINARY_KINDS, UNPARSEABLE_CLIENT)
                .and_then(client_text_in)
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

/// v5's charge: the channel and the content type escaped, the body raw.
pub(super) fn op_charge(channel: &str, body: Option<&MessageBody>) -> usize {
    v5::op_charge(channel, body)
}

/// Each attachment's name, which the grammar keeps free of anything JSON
/// escapes, plus the fixed allowance for its keys and handle.
pub(super) fn attachments_charge(attachments: &[PublishAttachment]) -> usize {
    attachments
        .iter()
        .map(|attachment| attachment.name.as_str().len() + ATTACHMENT_OVERHEAD_BYTES)
        .sum()
}

pub(super) fn batch_charged_bytes(
    publishes: &[BatchEntry],
    deferred_ops: &[BatchDeferredOp],
) -> usize {
    super::sum_op_charges(publishes, deferred_ops, op_charge, attachments_charge)
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

/// v5's derivation, raised to the chunk bound when that is larger, plus the
/// worst single `Publish`'s attachments.
pub(super) fn max_client_frame_bytes(max_body_bytes: usize) -> usize {
    // Checked, as v4's and v5's are: the value gates a violation and must
    // equal what a 32-bit client derives.
    max_body_bytes
        .checked_add(MAX_BATCH_OPS * BATCH_OP_OVERHEAD_BYTES)
        .map(|batch| batch.max(BLOB_CHUNK_BYTES))
        .and_then(|sum| sum.checked_add(PUBLISH_FRAME_OVERHEAD_BYTES))
        .and_then(|sum| {
            sum.checked_add(
                MAX_ATTACHMENTS_PER_MESSAGE * (MAX_ATTACHMENT_NAME_LEN + ATTACHMENT_OVERHEAD_BYTES),
            )
        })
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
        .expect("attach v6: a PublishBatch header serializes to JSON")
        .len();
    4 + header_len + bodies.len()
}

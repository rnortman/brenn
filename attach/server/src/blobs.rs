//! The attachment's blob plane: the session's table of blob handles and the
//! frames that act on it.
//!
//! A handle is a client-minted name for one entry of the table — an upload
//! being written, or a held blob (a sealed upload, or an opened attachment of
//! a message the session may read). The table is the session's hold on every
//! blob it names: nothing here is timed and nothing outlives the attachment,
//! because the table is dropped with the session and a dropped writer deletes
//! its bytes and a dropped hold is one holder fewer.
//!
//! Every authority question goes to the profile — whether the attachment has
//! the door at all, whether it may create, which channels it may read — and
//! every bound is a typed answer advertised in `Welcome`. File I/O runs on the
//! blocking pool, never on the session's task.
//!
//! The rule the handlers keep: a frame valid in the state its predecessors
//! would have left is an answer or a no-op in the state a refused predecessor
//! leaves, never a violation, so a client may send a whole sequence without
//! waiting for any answer.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use brenn_attach_proto::{
    AttachmentName, BLOB_CHUNK_BYTES, BlobCreateOutcome, BlobFacts, BlobHandle, BlobOpenOutcome,
    BlobReadOutcome, BlobSealOutcome, MAX_ATTACHMENTS_PER_MESSAGE, PublishAttachment, ServerFrame,
    carries_blob_transfer,
};
use brenn_envelope::check_content_type;
use brenn_messaging::{AdmittedPart, Messenger, NamedPart, PartRefusal, ToolChannelAttachments};
use brenn_messaging_store::blob::{
    AttachmentError, BlobError, BlobHold, BlobStore, BlobWriter, HeldAttachments,
    check_attachment_names,
};
use tracing::{debug, error, warn};
use uuid::Uuid;

use super::profile::{AttachProfile, BlobGrant};
use super::session::{
    AttachSessionCtx, FrameOutcome, SessionCounters, sanitize_client_detail, send_frame,
    send_read_answer,
};

/// The most handles one session's table holds, uploads and held blobs
/// together: one message's attachments twice over.
pub const MAX_BLOB_HANDLES: u32 = 64;

/// The most read answers carrying bytes one session has between the disk and
/// the socket at once. With [`BLOB_CHUNK_BYTES`] it bounds what a session that
/// asks for reads and does not drain its socket pins in the writer queue.
pub const MAX_INFLIGHT_BLOB_READS: u32 = 4;

/// The blob bytes one session may hold, in units of the store's per-blob cap.
/// Bounds how much of the pool one session keeps charged: a create reserves
/// its whole size before a byte is sent, and an open hold keeps its blob
/// charged after the message naming it is evicted.
pub const BYTES_HELD_BLOBS: u64 = 4;

/// One attachment's blob door: the grant, the store, and the facts `Welcome`
/// advertised. Exists only when the negotiated version carries blob transfer,
/// the profile grants the door and the host has a blob store.
pub struct BlobDoor {
    grant: BlobGrant,
    store: Arc<BlobStore>,
    facts: BlobFacts,
}

impl BlobDoor {
    /// The door an attachment speaking `version` has, or `None`.
    pub fn for_session(
        version: u32,
        profile: &dyn AttachProfile,
        messenger: &Messenger,
    ) -> Option<BlobDoor> {
        if !carries_blob_transfer(version) {
            return None;
        }
        let grant = profile.blob_grant()?;
        let store = Arc::clone(messenger.blob_store()?);
        let max_blob_bytes = store.limits().max_blob_bytes;
        let facts = BlobFacts {
            max_blob_bytes,
            max_chunk_bytes: BLOB_CHUNK_BYTES as u64,
            max_handles: MAX_BLOB_HANDLES,
            max_bytes_held: max_blob_bytes.saturating_mul(BYTES_HELD_BLOBS),
            max_inflight_reads: MAX_INFLIGHT_BLOB_READS,
            upload: grant.upload,
        };
        Some(BlobDoor {
            grant,
            store,
            facts,
        })
    }

    /// What `Welcome` advertises for this door.
    pub fn facts(&self) -> BlobFacts {
        self.facts
    }
}

/// One entry of the table.
enum BlobSlot {
    /// An upload being written, of its declared `size`.
    Writer { writer: BlobWriter, size: u64 },
    /// A held blob: a sealed upload or an opened attachment.
    Held { hold: BlobHold },
}

impl BlobSlot {
    /// What this slot counts against the bytes-held bound: a writer's
    /// declared size, a held blob's size.
    fn bytes_held(&self) -> u64 {
        match self {
            BlobSlot::Writer { size, .. } => *size,
            BlobSlot::Held { hold } => hold.reference().size,
        }
    }
}

/// The session's blob table, and the read answers in flight.
#[derive(Default)]
pub struct BlobTable {
    slots: HashMap<BlobHandle, BlobSlot>,
    /// Read answers carrying bytes between the disk and the socket. Shared
    /// with every [`ReadGuard`] the writer queue carries.
    reads_in_flight: Arc<AtomicU32>,
}

impl BlobTable {
    /// The holds a publish's attachments name, or `None` if any handle is not
    /// a held blob of this session. Each hold is a clone: the message's own
    /// from here on, while the session's handle stays until closed.
    ///
    /// # Panics
    ///
    /// On a list [`check_attachment_shape`] would refuse.
    pub fn held_attachments(&self, attachments: &[PublishAttachment]) -> Option<HeldAttachments> {
        if attachments.is_empty() {
            return Some(HeldAttachments::none());
        }
        let mut entries: Vec<(AttachmentName, BlobHold)> = Vec::with_capacity(attachments.len());
        for attachment in attachments {
            match self.slots.get(&attachment.handle) {
                Some(BlobSlot::Held { hold, .. }) => {
                    entries.push((attachment.name.clone(), hold.clone()));
                }
                Some(BlobSlot::Writer { .. }) | None => return None,
            }
        }
        Some(
            HeldAttachments::new(entries)
                .expect("a publish's attachments were shape-checked before their holds"),
        )
    }

    /// Whether every handle `attachments` names is a held blob of this
    /// session. Takes no hold.
    pub fn holds_all(&self, attachments: &[PublishAttachment]) -> bool {
        attachments.iter().all(|attachment| {
            matches!(
                self.slots.get(&attachment.handle),
                Some(BlobSlot::Held { .. })
            )
        })
    }

    /// The session's bytes held: the sum of every slot's
    /// [`BlobSlot::bytes_held`], counted per handle, so one blob under two
    /// handles counts twice. Computed from the slots, at most
    /// `MAX_BLOB_HANDLES` of them.
    fn bytes_held(&self) -> u64 {
        self.slots
            .values()
            .map(BlobSlot::bytes_held)
            .fold(0, |sum, held| {
                sum.checked_add(held)
                    .expect("a session's bytes held, each admitted under the bound, fit a u64")
            })
    }

    /// Whether holding `size` more bytes would pass `max_bytes_held`.
    fn over_bytes_held(&self, size: u64, facts: &BlobFacts) -> bool {
        !facts.admits_bytes_held(self.bytes_held(), size)
    }

    /// Hand the whole table to the blocking pool to drop: a dropped writer
    /// unlinks its file, as can a dropped hold. A table with no slot holds
    /// neither, so it drops here without the hop.
    pub async fn drop_off_task(self) {
        if self.slots.is_empty() {
            return;
        }
        tokio::task::spawn_blocking(move || drop(self))
            .await
            .expect("dropping a session's blob table panicked");
    }
}

/// One read answer carrying bytes, counted against
/// [`MAX_INFLIGHT_BLOB_READS`] until the writer has handed it to the socket
/// or dropped it with its queue.
pub struct ReadGuard {
    in_flight: Arc<AtomicU32>,
}

impl ReadGuard {
    fn take(in_flight: &Arc<AtomicU32>) -> ReadGuard {
        in_flight.fetch_add(1, Ordering::SeqCst);
        ReadGuard {
            in_flight: Arc::clone(in_flight),
        }
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Per-session blob counters, folded into the detach line.
#[derive(Debug, Default)]
pub struct BlobCounters {
    pub uploads_sealed: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub opens: u64,
    /// Answers other than `Ok`, by frame and outcome.
    pub refused: std::collections::BTreeMap<BlobRefusal, u64>,
    /// `BlobWrite`s naming a handle not in the table, dropped.
    pub writes_dropped: u64,
    /// `BlobClose`s naming a handle not in the table.
    pub closes_noop: u64,
    /// `Publish` and `PublishBatch` frames answered `AttachmentNotHeld`.
    pub publish_not_held: u64,
    /// Store I/O errors; the first in a session is logged at `error`.
    pub io_errors: u64,
}

/// A blob frame's answer other than `Ok`, by frame and outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BlobRefusal {
    CreateBlobTooLarge,
    CreateBudgetExhausted,
    CreateHandleLimit,
    CreateBytesHeldLimit,
    CreateStorageFailed,
    SealIncomplete,
    SealUnknownHandle,
    OpenNotRetained,
    OpenNoSuchAttachment,
    OpenToolChannel,
    OpenHandleLimit,
    OpenBytesHeldLimit,
    ReadUnknownHandle,
    ReadNotSealed,
    ReadOutOfRange,
    ReadTooManyReads,
    ReadStorageFailed,
}

impl BlobRefusal {
    fn of_create(outcome: BlobCreateOutcome) -> Option<Self> {
        match outcome {
            BlobCreateOutcome::Ok => None,
            BlobCreateOutcome::BlobTooLarge { .. } => Some(Self::CreateBlobTooLarge),
            BlobCreateOutcome::BudgetExhausted => Some(Self::CreateBudgetExhausted),
            BlobCreateOutcome::HandleLimit => Some(Self::CreateHandleLimit),
            BlobCreateOutcome::BytesHeldLimit => Some(Self::CreateBytesHeldLimit),
            BlobCreateOutcome::StorageFailed => Some(Self::CreateStorageFailed),
        }
    }

    fn of_seal(outcome: BlobSealOutcome) -> Option<Self> {
        match outcome {
            BlobSealOutcome::Ok => None,
            BlobSealOutcome::Incomplete { .. } => Some(Self::SealIncomplete),
            BlobSealOutcome::UnknownHandle => Some(Self::SealUnknownHandle),
        }
    }

    fn of_open(outcome: &BlobOpenOutcome) -> Option<Self> {
        match outcome {
            BlobOpenOutcome::Ok { .. } => None,
            BlobOpenOutcome::NotRetained => Some(Self::OpenNotRetained),
            BlobOpenOutcome::NoSuchAttachment => Some(Self::OpenNoSuchAttachment),
            BlobOpenOutcome::ToolChannel => Some(Self::OpenToolChannel),
            BlobOpenOutcome::HandleLimit => Some(Self::OpenHandleLimit),
            BlobOpenOutcome::BytesHeldLimit { .. } => Some(Self::OpenBytesHeldLimit),
        }
    }

    fn of_read(outcome: &BlobReadOutcome) -> Option<Self> {
        match outcome {
            BlobReadOutcome::Ok { .. } => None,
            BlobReadOutcome::OutOfRange { .. } => Some(Self::ReadOutOfRange),
            BlobReadOutcome::UnknownHandle => Some(Self::ReadUnknownHandle),
            BlobReadOutcome::NotSealed => Some(Self::ReadNotSealed),
            BlobReadOutcome::TooManyReads => Some(Self::ReadTooManyReads),
            BlobReadOutcome::StorageFailed => Some(Self::ReadStorageFailed),
        }
    }

    /// Whether this is a per-session bound, whose first refusal in a session
    /// is logged at `warn`.
    fn is_bound(self) -> bool {
        match self {
            Self::CreateHandleLimit
            | Self::CreateBytesHeldLimit
            | Self::OpenHandleLimit
            | Self::OpenBytesHeldLimit
            | Self::ReadTooManyReads => true,
            Self::CreateBlobTooLarge
            | Self::CreateBudgetExhausted
            | Self::CreateStorageFailed
            | Self::SealIncomplete
            | Self::SealUnknownHandle
            | Self::OpenNotRetained
            | Self::OpenNoSuchAttachment
            | Self::OpenToolChannel
            | Self::ReadUnknownHandle
            | Self::ReadNotSealed
            | Self::ReadOutOfRange
            | Self::ReadStorageFailed => false,
        }
    }
}

impl BlobCounters {
    /// Count the refusal `refusal` is, if it is one; warn on the first of a
    /// per-session bound.
    fn refuse(&mut self, refusal: Option<BlobRefusal>) {
        let Some(refusal) = refusal else {
            return;
        };
        let count = self.refused.entry(refusal).or_default();
        *count += 1;
        if *count == 1 && refusal.is_bound() {
            warn!(?refusal, "attach blob bound reached in this session");
        }
    }

    /// Count one store I/O error, logging the first in the session.
    fn io_error(&mut self, frame: &str, err: &BlobError) {
        if self.io_errors == 0 {
            error!(frame, error = %err, "attach blob store I/O failed");
        }
        self.io_errors += 1;
    }
}

/// The door, or the violation a blob frame without one is.
fn door_or_violate<'a>(
    ctx: &'a AttachSessionCtx,
    frame: &str,
) -> Result<&'a BlobDoor, FrameOutcome> {
    ctx.blob_door()
        .ok_or_else(|| ctx.violation(format!("{frame} without a blob door")))
}

/// A new handle that is already in the table: every client mints a handle once.
fn live_handle(ctx: &AttachSessionCtx, frame: &str, handle: BlobHandle) -> FrameOutcome {
    ctx.violation(format!("{frame} names live blob handle {}", handle.0))
}

/// Handle a `BlobCreate` frame: reserve `size` for a new upload.
pub async fn handle_blob_create(
    ctx: &AttachSessionCtx,
    table: &mut BlobTable,
    handle: BlobHandle,
    content_type: &str,
    size: u64,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let door = match door_or_violate(ctx, "BlobCreate") {
        Ok(door) => door,
        Err(violation) => return violation,
    };
    if !door.grant.upload {
        return ctx.violation("BlobCreate on an attachment that may not upload");
    }
    if table.slots.contains_key(&handle) {
        return live_handle(ctx, "BlobCreate", handle);
    }
    if check_content_type(content_type).is_err() {
        return ctx.violation(format!(
            "BlobCreate with malformed content type {}",
            sanitize_client_detail(content_type)
        ));
    }

    let facts = door.facts;
    let refusal = if table.slots.len() >= MAX_BLOB_HANDLES as usize {
        Some(BlobCreateOutcome::HandleLimit)
    } else if size > facts.max_blob_bytes {
        Some(BlobCreateOutcome::BlobTooLarge {
            size,
            max: facts.max_blob_bytes,
        })
    } else if table.over_bytes_held(size, &facts) {
        Some(BlobCreateOutcome::BytesHeldLimit)
    } else {
        None
    };
    let outcome = match refusal {
        Some(outcome) => outcome,
        None => {
            let store = Arc::clone(&door.store);
            let content_type = content_type.to_string();
            let created = tokio::task::spawn_blocking(move || {
                let mut writer = store.create(&content_type)?;
                writer.reserve(size)?;
                Ok::<_, BlobError>(writer)
            })
            .await
            .expect("blob create on the blocking pool panicked");
            match created {
                Ok(writer) => {
                    table
                        .slots
                        .insert(handle, BlobSlot::Writer { writer, size });
                    BlobCreateOutcome::Ok
                }
                Err(BlobError::BudgetExhausted { .. }) => BlobCreateOutcome::BudgetExhausted,
                Err(err @ BlobError::Io(_)) => {
                    counters.blobs.io_error("BlobCreate", &err);
                    BlobCreateOutcome::StorageFailed
                }
                Err(other) => panic!(
                    "attach BlobCreate: the store refused a create the handler checked: {other}"
                ),
            }
        }
    };
    counters.blobs.refuse(BlobRefusal::of_create(outcome));
    send_frame(
        &ctx.tx,
        ServerFrame::BlobCreateResult { handle, outcome },
        counters,
    )
    .await
}

/// Handle a `BlobWrite` frame: write `bytes` at `offset` of an upload.
/// Unanswered.
pub async fn handle_blob_write(
    ctx: &AttachSessionCtx,
    table: &mut BlobTable,
    handle: BlobHandle,
    offset: u64,
    bytes: Vec<u8>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let door = match door_or_violate(ctx, "BlobWrite") {
        Ok(door) => door,
        Err(violation) => return violation,
    };
    // No create precedes a write on a door without upload, so the write is
    // pipelined behind nothing.
    if !door.grant.upload {
        return ctx.violation("BlobWrite on an attachment that may not upload");
    }
    let len = bytes.len() as u64;
    if len == 0 || len > door.facts.max_chunk_bytes {
        return ctx.violation(format!(
            "BlobWrite of {len} bytes, outside 1..={}",
            door.facts.max_chunk_bytes
        ));
    }
    let size = match table.slots.get(&handle) {
        None => {
            // A write pipelined behind a refused create.
            counters.blobs.writes_dropped += 1;
            return FrameOutcome::Continue;
        }
        Some(BlobSlot::Held { .. }) => {
            return ctx.violation(format!("BlobWrite to sealed blob handle {}", handle.0));
        }
        Some(BlobSlot::Writer { size, .. }) => *size,
    };
    if offset.checked_add(len).is_none_or(|end| end > size) {
        return ctx.violation(format!(
            "BlobWrite of {len} bytes at offset {offset} passes the declared size {size}"
        ));
    }

    let Some(BlobSlot::Writer { mut writer, size }) = table.slots.remove(&handle) else {
        unreachable!("the slot was a writer above");
    };
    let (writer, written) = tokio::task::spawn_blocking(move || {
        let written = writer.write(offset, &bytes);
        (writer, written)
    })
    .await
    .expect("blob write on the blocking pool panicked");
    table
        .slots
        .insert(handle, BlobSlot::Writer { writer, size });
    match written {
        Ok(()) => {
            counters.blobs.bytes_written += len;
            FrameOutcome::Continue
        }
        // The range stays unwritten and the seal answers `Incomplete`.
        Err(err @ BlobError::Io(_)) => {
            counters.blobs.io_error("BlobWrite", &err);
            FrameOutcome::Continue
        }
        Err(BlobError::TooFragmented) => ctx.violation(format!(
            "BlobWrite would leave blob handle {} in too many disjoint ranges",
            handle.0
        )),
        Err(other) => panic!(
            "attach BlobWrite: the store refused a write inside the reserved extent: {other}"
        ),
    }
}

/// Handle a `BlobSeal` frame: make an upload a held blob.
pub async fn handle_blob_seal(
    ctx: &AttachSessionCtx,
    table: &mut BlobTable,
    handle: BlobHandle,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let door = match door_or_violate(ctx, "BlobSeal") {
        Ok(door) => door,
        Err(violation) => return violation,
    };
    // No create precedes a seal on a door without upload, so the seal is
    // pipelined behind nothing.
    if !door.grant.upload {
        return ctx.violation("BlobSeal on an attachment that may not upload");
    }
    let outcome = match table.slots.remove(&handle) {
        // A seal pipelined behind a refused create.
        None => BlobSealOutcome::UnknownHandle,
        Some(held @ BlobSlot::Held { .. }) => {
            table.slots.insert(handle, held);
            return ctx.violation(format!("BlobSeal of sealed blob handle {}", handle.0));
        }
        // The seal touches the store's index only, so it runs here.
        Some(BlobSlot::Writer { writer, size }) => match writer.seal() {
            Ok(hold) => {
                assert_eq!(
                    hold.reference().size,
                    size,
                    "a sealed blob's size is its declared size"
                );
                table.slots.insert(handle, BlobSlot::Held { hold });
                counters.blobs.uploads_sealed += 1;
                BlobSealOutcome::Ok
            }
            Err(refused) => {
                table.slots.insert(
                    handle,
                    BlobSlot::Writer {
                        writer: refused.writer,
                        size,
                    },
                );
                BlobSealOutcome::Incomplete {
                    offset: refused.gap.start,
                    len: refused.gap.end - refused.gap.start,
                }
            }
        },
    };
    counters.blobs.refuse(BlobRefusal::of_seal(outcome));
    send_frame(
        &ctx.tx,
        ServerFrame::BlobSealResult { handle, outcome },
        counters,
    )
    .await
}

/// One inbound `BlobOpen`'s fields.
pub struct BlobOpenRequest<'a> {
    pub handle: BlobHandle,
    pub channel: &'a str,
    pub message_id: Uuid,
    pub name: &'a AttachmentName,
}

/// Handle a `BlobOpen` frame: take a hold on an attachment of a retained
/// message on a channel this attachment may subscribe.
pub async fn handle_blob_open(
    ctx: &AttachSessionCtx,
    table: &mut BlobTable,
    request: BlobOpenRequest<'_>,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let BlobOpenRequest {
        handle,
        channel,
        message_id,
        name,
    } = request;
    let facts = match door_or_violate(ctx, "BlobOpen") {
        Ok(door) => door.facts,
        Err(violation) => return violation,
    };
    if table.slots.contains_key(&handle) {
        return live_handle(ctx, "BlobOpen", handle);
    }
    // The same words a `Subscribe` there earns: outside the grants, "not
    // yours" and "no such channel" stay one answer.
    // TODO(remote-blob-open-without-delivery): admits any message in the
    // channel's window, delivered to this session or not.
    if ctx.profile.subscribable(channel).is_none() {
        return ctx.violation(format!(
            "BlobOpen on unsubscribable channel {}",
            sanitize_client_detail(channel)
        ));
    }

    let outcome = if table.slots.len() >= MAX_BLOB_HANDLES as usize {
        BlobOpenOutcome::HandleLimit
    } else {
        let admitted = ctx
            .messenger
            .admit_retained_part(
                channel,
                message_id,
                &NamedPart::Attachment(name.clone()),
                ToolChannelAttachments::Refused,
            )
            .await;
        match admitted {
            Ok(AdmittedPart::Attachment(hold)) => {
                let reference = hold.reference();
                let size = reference.size;
                if table.over_bytes_held(size, &facts) {
                    // The hold can be the blob's last, whose drop unlinks a
                    // ring blob's file.
                    tokio::task::spawn_blocking(move || drop(hold))
                        .await
                        .expect("dropping a refused open's hold on the blocking pool panicked");
                    BlobOpenOutcome::BytesHeldLimit { size }
                } else {
                    table.slots.insert(handle, BlobSlot::Held { hold });
                    counters.blobs.opens += 1;
                    BlobOpenOutcome::Ok {
                        size,
                        content_type: reference.content_type,
                    }
                }
            }
            Ok(AdmittedPart::Body(_)) => unreachable!("an attachment name admits an attachment"),
            Err(PartRefusal::ToolChannelAttachment) => {
                // The refusal needs only the channel name, so a client can
                // provoke it at frame rate: `warn` on the session's first,
                // `debug` after; the detach line carries the count.
                let channel = sanitize_client_detail(channel);
                if counters
                    .blobs
                    .refused
                    .contains_key(&BlobRefusal::OpenToolChannel)
                {
                    debug!(
                        %channel,
                        %message_id,
                        "attach BlobOpen refused: an attachment on a tool-substrate channel"
                    );
                } else {
                    warn!(
                        %channel,
                        %message_id,
                        "attach BlobOpen refused: an attachment on a tool-substrate channel"
                    );
                }
                BlobOpenOutcome::ToolChannel
            }
            Err(PartRefusal::ChannelGone | PartRefusal::NotRetained) => {
                debug!(
                    channel = %sanitize_client_detail(channel),
                    %message_id,
                    "attach BlobOpen: message not retained"
                );
                BlobOpenOutcome::NotRetained
            }
            Err(PartRefusal::NoSuchAttachment { .. }) => {
                debug!(
                    channel = %sanitize_client_detail(channel),
                    %message_id,
                    "attach BlobOpen: no such attachment"
                );
                BlobOpenOutcome::NoSuchAttachment
            }
        }
    };
    counters.blobs.refuse(BlobRefusal::of_open(&outcome));
    send_frame(
        &ctx.tx,
        ServerFrame::BlobOpenResult { handle, outcome },
        counters,
    )
    .await
}

/// One inbound `BlobRead`'s fields.
pub struct BlobReadRequest {
    pub correlation: u64,
    pub handle: BlobHandle,
    pub offset: u64,
    pub len: u64,
}

/// Handle a `BlobRead` frame: read a byte range of a held blob.
pub async fn handle_blob_read(
    ctx: &AttachSessionCtx,
    table: &mut BlobTable,
    request: BlobReadRequest,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    let BlobReadRequest {
        correlation,
        handle,
        offset,
        len,
    } = request;
    let door = match door_or_violate(ctx, "BlobRead") {
        Ok(door) => door,
        Err(violation) => return violation,
    };
    if len > door.facts.max_chunk_bytes {
        return ctx.violation(format!(
            "BlobRead of {len} bytes, over {}",
            door.facts.max_chunk_bytes
        ));
    }
    let refused = |outcome: BlobReadOutcome, counters: &mut SessionCounters| {
        counters.blobs.refuse(BlobRefusal::of_read(&outcome));
        ServerFrame::BlobReadResult {
            correlation,
            outcome,
        }
    };
    let frame = match table.slots.get(&handle) {
        None => refused(BlobReadOutcome::UnknownHandle, counters),
        // A read pipelined behind a seal the host failed to complete.
        Some(BlobSlot::Writer { .. }) => refused(BlobReadOutcome::NotSealed, counters),
        Some(BlobSlot::Held { hold, .. }) => {
            let size = hold.reference().size;
            if offset.checked_add(len).is_none_or(|end| end > size) {
                refused(BlobReadOutcome::OutOfRange { size }, counters)
            } else if table.reads_in_flight.load(Ordering::SeqCst) >= MAX_INFLIGHT_BLOB_READS {
                refused(BlobReadOutcome::TooManyReads, counters)
            } else {
                let guard = ReadGuard::take(&table.reads_in_flight);
                let hold = hold.clone();
                let len = usize::try_from(len).expect("a read within the chunk bound fits a usize");
                let read = tokio::task::spawn_blocking(move || hold.read(offset, len))
                    .await
                    .expect("blob read on the blocking pool panicked");
                match read {
                    Ok(bytes) => {
                        counters.blobs.bytes_read += bytes.len() as u64;
                        let frame = ServerFrame::BlobReadResult {
                            correlation,
                            outcome: BlobReadOutcome::Ok { bytes },
                        };
                        return send_read_answer(&ctx.tx, frame, guard, counters).await;
                    }
                    Err(err @ BlobError::Io(_)) => {
                        counters.blobs.io_error("BlobRead", &err);
                        refused(BlobReadOutcome::StorageFailed, counters)
                    }
                    Err(other) => {
                        panic!("attach BlobRead: the store refused a checked read: {other}")
                    }
                }
            }
        }
    };
    send_frame(&ctx.tx, frame, counters).await
}

/// Handle a `BlobClose` frame: give a handle up. Unanswered.
pub async fn handle_blob_close(
    ctx: &AttachSessionCtx,
    table: &mut BlobTable,
    handle: BlobHandle,
    counters: &mut SessionCounters,
) -> FrameOutcome {
    if let Err(violation) = door_or_violate(ctx, "BlobClose") {
        return violation;
    }
    let Some(slot) = table.slots.remove(&handle) else {
        // A close pipelined behind a refused create or open.
        counters.blobs.closes_noop += 1;
        return FrameOutcome::Continue;
    };
    // Dropping a writer unlinks its file, as can dropping the last hold.
    tokio::task::spawn_blocking(move || drop(slot))
        .await
        .expect("dropping a blob slot on the blocking pool panicked");
    FrameOutcome::Continue
}

/// The violation a publish's attachment list is, if it is one: a list on an
/// attachment without the door, under an attribution the profile does not let
/// attach on `channel`, over the per-message bound, or naming one name twice.
/// Names outside the grammar never decode.
pub fn check_attachment_shape(
    ctx: &AttachSessionCtx,
    attribution: Option<&str>,
    channel: &str,
    attachments: &[PublishAttachment],
    frame: &str,
) -> Result<(), FrameOutcome> {
    if attachments.is_empty() {
        return Ok(());
    }
    door_or_violate(ctx, frame)?;
    if !ctx.profile.attribution_may_attach(attribution, channel) {
        return Err(ctx.violation(format!(
            "{frame} names attachments on channel {} under an attribution that may not attach \
             there",
            sanitize_client_detail(channel)
        )));
    }
    check_attachment_names(attachments.iter().map(|attachment| &attachment.name)).map_err(|err| {
        match err {
            AttachmentError::TooMany { count } => ctx.violation(format!(
                "{frame} names {count} attachments, over {MAX_ATTACHMENTS_PER_MESSAGE}"
            )),
            AttachmentError::DuplicateName(name) => ctx.violation(format!(
                "{frame} names attachment {} twice",
                sanitize_client_detail(name.as_str())
            )),
        }
    })
}

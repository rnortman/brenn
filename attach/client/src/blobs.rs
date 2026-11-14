//! The blob plane of an attachment: the handles this attacher has opened or
//! created on the peer's blob table, the answers the peer still owes on each,
//! and the byte ranges an upload or a read moves.
//!
//! Sans-I/O and generic over the embedder's tag `T`, as
//! [`PendingPublishes`](crate::publish::PendingPublishes) is. The embedder owns
//! the file or buffer bytes come from and go to; the plane owns handles, read
//! correlations, chunking against the advertised bounds, the in-flight read
//! window and the outcome types. No I/O, no clock, no `tokio`.
//!
//! The plane sends frames before their predecessors are answered, so it keeps,
//! per handle, the answers the server still owes, and a handle's record lasts
//! until the last of them has arrived. What the embedder may do with a handle,
//! without waiting:
//!
//! | State | Entered by | The embedder may |
//! |---|---|---|
//! | uploading | [`BlobPlane::create`] | `next_chunk`, `write_chunk`; `close` |
//! | sealing | the `write_chunk` (or empty `create`) that returned `BlobSeal` | name it in a publish; `close` |
//! | opening | [`BlobPlane::open`] | name it in a publish; `close` |
//! | held | the [`BlobEvent::Sealed`] or [`BlobEvent::Opened`] event | `read`; name it in a publish; `close` |
//! | closing | `close`, while answers are owed | nothing takes effect: `next_chunk` answers `None`, `write_chunk` returns no frames, `read` answers [`ReadError::Gone`], `close` returns an empty step |
//! | gone | `close` with nothing owed; the last owed answer of a closing handle; `UploadRefused`; `OpenRefused`; `on_detached` | nothing takes effect: `next_chunk` answers `None`, `write_chunk` returns no frames, `read` answers [`ReadError::Gone`], `close` returns an empty step |
//!
//! **One rule for every owed answer**, on a live handle and on a closing one:
//! an owed answer yields an event exactly when the operation it answers has
//! not had its outcome reported; otherwise it is absorbed. So a late read
//! answer on a closed handle is absorbed (its read already ended `Closed`), and
//! a late seal or open answer is reported as its real event.
//!
//! An answer the plane is not owed — a handle or correlation it never minted
//! on this attachment, a second answer to one request, a chunk of the wrong
//! length — is an `Err` the embedder turns into a fatal on its connection.
//!
//! **One rule for a call that names a handle** (`next_chunk`, `write_chunk`,
//! `read`, `close`). A handle this plane never minted is an embedder bug and
//! panics. A handle that is closing or gone is absorbed, whatever retired it —
//! the embedder's `close`, a refusal, the plane's own close on `Incomplete`, or
//! a detach — and whether or not an attachment is live now: the call sends
//! nothing, emits no event and changes no state. On a live handle the calls
//! its state does not allow are embedder bugs and panic: a `write_chunk` that
//! is not the range `next_chunk` named or on a handle that wants no chunk, a
//! `read` on a handle that is not held. None of the four checks the blob door;
//! a live handle exists only while its attachment has one. The embedder's own
//! repeated `close`, or its `write_chunk` or `read` after its own `close`, is
//! not detected: it is absorbed like any call on a gone handle.
//
// TODO(attach-client-blob-conveniences): no one-call form moves a whole blob
// to or from a file; the embedder drives the chunk loop itself.

use std::collections::{BTreeMap, VecDeque};

use brenn_attach_proto::{
    AttachmentName, BlobCreateOutcome, BlobFacts, BlobHandle, BlobOpenOutcome, BlobReadOutcome,
    BlobSealOutcome, BodyError, ClientFrame, ServerFrame,
};
use brenn_envelope::check_content_type;
use uuid::Uuid;

use crate::conn::AttachmentFacts;

/// What one call or answer produced: frames to send, in order, and events for
/// the embedder.
#[derive(Debug, PartialEq, Eq)]
pub struct BlobStep<T> {
    pub frames: Vec<ClientFrame>,
    pub events: Vec<BlobEvent<T>>,
}

impl<T> BlobStep<T> {
    fn new() -> Self {
        Self {
            frames: Vec::new(),
            events: Vec::new(),
        }
    }
}

/// Names one [`BlobPlane::read`] until it ends with [`BlobEvent::ReadDone`] or
/// [`BlobEvent::ReadRefused`]. Never reused by the plane that minted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadId(pub u64);

/// The next range an upload wants, as [`BlobPlane::next_chunk`] names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkRange {
    pub offset: u64,
    pub len: usize,
}

/// Something the embedder must know about a blob operation.
#[derive(Debug, PartialEq, Eq)]
pub enum BlobEvent<T> {
    /// The upload is sealed: the handle is a held blob.
    Sealed { tag: T, handle: BlobHandle },
    /// The upload is over and `handle` is gone. A refused create,
    /// `Incomplete` and `Closed` made nothing. With `Detached` after the seal
    /// was sent the server may have sealed the blob; the session's hold died
    /// with the session, so the blob lives only if a publish naming it was
    /// applied before the connection broke.
    UploadRefused {
        tag: T,
        handle: BlobHandle,
        reason: UploadRefusal,
    },
    /// The open succeeded: the handle is a held blob of `size` bytes.
    Opened {
        tag: T,
        handle: BlobHandle,
        size: u64,
        content_type: String,
    },
    /// The open was refused and `handle` is gone.
    OpenRefused {
        tag: T,
        handle: BlobHandle,
        reason: OpenRefusal,
    },
    /// The bytes at `offset` of the blob, part of `read`. Within one read the
    /// chunks arrive in ascending offset order.
    Chunk {
        read: ReadId,
        handle: BlobHandle,
        offset: u64,
        bytes: Vec<u8>,
    },
    /// Every chunk of `read` was delivered.
    ReadDone {
        read: ReadId,
        tag: T,
        handle: BlobHandle,
    },
    /// `read` ended before its last chunk; the rest of it is abandoned.
    ReadRefused {
        read: ReadId,
        tag: T,
        handle: BlobHandle,
        reason: ReadRefusal,
    },
}

/// Why an upload ended without a held blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadRefusal {
    /// The create was refused: `size` is over the host's per-blob cap `max`.
    BlobTooLarge { size: u64, max: u64 },
    /// The create was refused: the host's blob pool cannot reserve the size.
    BudgetExhausted,
    /// The create was refused: the session's table is full.
    HandleLimit,
    /// The create was refused: the session's bytes held would pass
    /// `max_bytes_held`.
    BytesHeldLimit,
    /// The create was refused: the host could not create the blob's file.
    StorageFailed,
    /// The seal found `[offset, offset + len)` unwritten, a host-side write
    /// failure. The plane closed the handle; upload again.
    Incomplete { offset: u64, len: u64 },
    /// The embedder closed the handle before the upload was sealed.
    Closed,
    /// The attachment ended first.
    Detached,
}

/// Why an open ended without a held blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenRefusal {
    /// The message is not in the channel's window now, is parked, or the
    /// channel is gone. A reference read from a replay store meets this once
    /// its message has rolled out.
    NotRetained,
    /// The message carries no attachment of that name.
    NoSuchAttachment,
    /// The channel is of the tool-substrate family, whose attachments this
    /// door never admits.
    ToolChannel,
    /// The session's table is full.
    HandleLimit,
    /// The session's bytes held plus the blob's `size` would pass
    /// `max_bytes_held`. Close a held handle and open again, or give the
    /// attachment up when `size` alone exceeds `max_bytes_held`.
    BytesHeldLimit { size: u64 },
    /// The attachment ended first.
    Detached,
}

/// Why a read ended before its last chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRefusal {
    /// The server found the range past the blob's `size`.
    OutOfRange { size: u64 },
    /// The server found no such handle in the session's table.
    UnknownHandle,
    /// The server found the handle an unsealed upload.
    NotSealed,
    /// The server had its bound of read answers in flight.
    TooManyReads,
    /// The host could not read the blob's file.
    StorageFailed,
    /// The embedder closed the handle.
    Closed,
    /// The attachment ended first.
    Detached,
}

/// Why [`BlobPlane::create`] sent nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateError {
    /// The content type is malformed.
    ContentType(BodyError),
    /// `size` is over the attachment's `max_blob_bytes`, `max`.
    BlobTooLarge { size: u64, max: u64 },
    /// The plane holds `max_handles` live handles.
    HandleLimit,
    /// The plane's bytes held plus `size` would pass `max_bytes_held`.
    BytesHeldLimit,
}

/// Why [`BlobPlane::open`] sent nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenError {
    /// The plane holds `max_handles` live handles.
    HandleLimit,
}

/// Why [`BlobPlane::read`] sent nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadError {
    /// The range passes the blob's `size`.
    OutOfRange { size: u64 },
    /// The handle is closing or gone: closed, refused, or forgotten at a
    /// detach.
    Gone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Uploading,
    Sealing,
    Opening,
    Held,
}

/// One handle the plane minted on this attachment and has not forgotten.
struct Record<T> {
    phase: Phase,
    /// The embedder may no longer name this handle: it was closed, or its
    /// upload or open was refused. The record stays only for owed answers.
    retired: bool,
    /// The create's or open's tag, while its outcome is unreported.
    tag: Option<T>,
    /// The declared size of an upload, or the size an open reported; 0 while
    /// opening.
    size: u64,
    /// The next byte offset an upload wants.
    next_offset: u64,
    create_owed: bool,
    seal_owed: bool,
    open_owed: bool,
    /// Read answers in flight on this handle.
    reads_owed: usize,
}

impl<T> Record<T> {
    fn owes(&self) -> bool {
        self.create_owed || self.seal_owed || self.open_owed || self.reads_owed > 0
    }

    /// What this handle counts against `max_bytes_held`: a writer its declared
    /// size, a held blob its size, an opening or retired handle nothing.
    fn bytes_held(&self) -> u64 {
        if self.retired {
            return 0;
        }
        match self.phase {
            Phase::Uploading | Phase::Sealing | Phase::Held => self.size,
            Phase::Opening => 0,
        }
    }
}

/// One read the embedder asked for that has not ended.
struct Read<T> {
    tag: T,
    handle: BlobHandle,
    /// The next offset not yet requested.
    next_offset: u64,
    end: u64,
    /// Chunk requests sent and unanswered.
    in_flight: usize,
}

/// One `BlobRead` on the wire.
struct ReadRequest {
    read: ReadId,
    handle: BlobHandle,
    offset: u64,
    len: u64,
}

/// The blob plane of one attacher, across its attachments.
///
/// Handles and read correlations come from counters that live as long as the
/// plane and never repeat, across attachments too, so a handle from a dead
/// attachment can never name a live blob.
pub struct BlobPlane<T> {
    /// The live attachment's blob door; `None` while detached or doorless.
    facts: Option<BlobFacts>,
    next_handle: u64,
    next_correlation: u64,
    next_read: u64,
    records: BTreeMap<BlobHandle, Record<T>>,
    reads: BTreeMap<ReadId, Read<T>>,
    /// Every `BlobRead` sent and unanswered, by correlation. Counts against
    /// `max_inflight_reads` until its answer arrives, ended read or not.
    requests: BTreeMap<u64, ReadRequest>,
    /// Reads with ranges not yet requested, oldest first.
    waiting: VecDeque<ReadId>,
}

impl<T> Default for BlobPlane<T> {
    fn default() -> Self {
        Self {
            facts: None,
            next_handle: 1,
            next_correlation: 1,
            next_read: 1,
            records: BTreeMap::new(),
            reads: BTreeMap::new(),
            requests: BTreeMap::new(),
            waiting: VecDeque::new(),
        }
    }
}

impl<T> BlobPlane<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// A new attachment is live; its blob door is `facts.blobs`.
    ///
    /// # Panics
    ///
    /// When the previous attachment was not ended with
    /// [`on_detached`](Self::on_detached).
    pub fn on_attached(&mut self, facts: &AttachmentFacts) {
        assert!(
            self.records.is_empty() && self.requests.is_empty(),
            "attach client: blob plane attached without on_detached for the previous attachment"
        );
        self.facts = facts.blobs;
    }

    /// The attachment ended. Every upload, open and read still outstanding,
    /// and every outcome a closing handle was still owed, fails with
    /// `Detached`, in handle order and then read order; every handle and owed
    /// answer is forgotten.
    pub fn on_detached(&mut self) -> Vec<BlobEvent<T>> {
        let mut events = Vec::new();
        for (handle, record) in std::mem::take(&mut self.records) {
            let Some(tag) = record.tag else { continue };
            events.push(match record.phase {
                Phase::Uploading | Phase::Sealing => BlobEvent::UploadRefused {
                    tag,
                    handle,
                    reason: UploadRefusal::Detached,
                },
                Phase::Opening => BlobEvent::OpenRefused {
                    tag,
                    handle,
                    reason: OpenRefusal::Detached,
                },
                Phase::Held => unreachable!("a held handle has no unreported outcome"),
            });
        }
        for (read, r) in std::mem::take(&mut self.reads) {
            events.push(BlobEvent::ReadRefused {
                read,
                tag: r.tag,
                handle: r.handle,
                reason: ReadRefusal::Detached,
            });
        }
        self.requests.clear();
        self.waiting.clear();
        self.facts = None;
        events
    }

    /// Start an upload of `size` bytes of `content_type`. The frames are
    /// `BlobCreate`, and for an empty blob `BlobSeal` behind it.
    ///
    /// Refused locally, with no frame, when the content type is malformed or
    /// `size` passes `max_blob_bytes`, `max_handles` or `max_bytes_held`
    /// counted over the plane's handles. The server is the authority on the
    /// bounds; a create that passes here may still be refused there.
    ///
    /// # Panics
    ///
    /// With no blob door, or a door whose `upload` is false.
    pub fn create(
        &mut self,
        tag: T,
        content_type: &str,
        size: u64,
    ) -> Result<(BlobHandle, Vec<ClientFrame>), CreateError> {
        let facts = self.door("create");
        assert!(
            facts.upload,
            "attach client: blob create on an attachment whose blob door grants no upload"
        );
        check_content_type(content_type).map_err(CreateError::ContentType)?;
        if size > facts.max_blob_bytes {
            return Err(CreateError::BlobTooLarge {
                size,
                max: facts.max_blob_bytes,
            });
        }
        if self.live_handles() >= u64::from(facts.max_handles) {
            return Err(CreateError::HandleLimit);
        }
        if !facts.admits_bytes_held(self.bytes_held(), size) {
            return Err(CreateError::BytesHeldLimit);
        }
        let handle = self.mint_handle();
        let mut frames = vec![ClientFrame::BlobCreate {
            handle,
            content_type: content_type.to_string(),
            size,
        }];
        let empty = size == 0;
        if empty {
            frames.push(ClientFrame::BlobSeal { handle });
        }
        self.records.insert(
            handle,
            Record {
                phase: if empty {
                    Phase::Sealing
                } else {
                    Phase::Uploading
                },
                retired: false,
                tag: Some(tag),
                size,
                next_offset: 0,
                create_owed: true,
                seal_owed: empty,
                open_owed: false,
                reads_owed: 0,
            },
        );
        Ok((handle, frames))
    }

    /// The range the upload `handle` wants next, at most `max_chunk_bytes`
    /// long, in ascending order; `None` once every range was handed over, or
    /// when the upload is over — refused, closed or forgotten at a detach.
    ///
    /// # Panics
    ///
    /// On a handle this plane never minted.
    pub fn next_chunk(&self, handle: BlobHandle) -> Option<ChunkRange> {
        let record = self.live(handle, "next_chunk")?;
        if record.phase != Phase::Uploading {
            return None;
        }
        let facts = self
            .facts
            .expect("attach client: a live blob handle has a blob door");
        let len = (record.size - record.next_offset).min(facts.max_chunk_bytes);
        Some(ChunkRange {
            offset: record.next_offset,
            len: usize::try_from(len)
                .expect("attach client: a chunk is bounded by max_chunk_bytes"),
        })
    }

    /// Write the range [`next_chunk`](Self::next_chunk) named. The frames are
    /// `BlobWrite`, and with the last chunk `BlobSeal` behind it.
    ///
    /// An `UploadRefused` — a refused create, or a detach — can arrive between
    /// `next_chunk` and `write_chunk`. The handle is then gone and the write is
    /// absorbed: no frames, the bytes discarded. The same holds on a handle
    /// that is closing or gone for any other reason.
    ///
    /// # Panics
    ///
    /// On a handle this plane never minted; on a live handle when `bytes` is
    /// not exactly the range `next_chunk` names, or the upload wants none
    /// (sealing, opening, held).
    pub fn write_chunk(&mut self, handle: BlobHandle, bytes: Vec<u8>) -> Vec<ClientFrame> {
        if self.live(handle, "write_chunk").is_none() {
            return Vec::new();
        }
        let range = self.next_chunk(handle).unwrap_or_else(|| {
            panic!(
                "attach client: write_chunk on blob handle {} that wants no chunk",
                handle.0
            )
        });
        assert_eq!(
            bytes.len(),
            range.len,
            "attach client: write_chunk on blob handle {} with {} bytes where the range at {} is {}",
            handle.0,
            bytes.len(),
            range.offset,
            range.len
        );
        let record = self.records.get_mut(&handle).expect("uploading record");
        record.next_offset += bytes.len() as u64;
        let mut frames = vec![ClientFrame::BlobWrite {
            handle,
            offset: range.offset,
            bytes,
        }];
        if record.next_offset == record.size {
            record.phase = Phase::Sealing;
            record.seal_owed = true;
            frames.push(ClientFrame::BlobSeal { handle });
        }
        frames
    }

    /// Open the attachment `name` of the message `message_id` on `channel`.
    /// Refused locally, with no frame, when the plane holds `max_handles` live
    /// handles; there is no local byte check, since the size is unknown until
    /// the answer.
    ///
    /// # Panics
    ///
    /// With no blob door.
    pub fn open(
        &mut self,
        tag: T,
        channel: &str,
        message_id: Uuid,
        name: AttachmentName,
    ) -> Result<(BlobHandle, ClientFrame), OpenError> {
        let facts = self.door("open");
        if self.live_handles() >= u64::from(facts.max_handles) {
            return Err(OpenError::HandleLimit);
        }
        let handle = self.mint_handle();
        self.records.insert(
            handle,
            Record {
                phase: Phase::Opening,
                retired: false,
                tag: Some(tag),
                size: 0,
                next_offset: 0,
                create_owed: false,
                seal_owed: false,
                open_owed: true,
                reads_owed: 0,
            },
        );
        let frame = ClientFrame::BlobOpen {
            handle,
            channel: channel.to_string(),
            message_id,
            name,
        };
        Ok((handle, frame))
    }

    /// Read `[offset, offset + len)` of the held blob `handle`. The plane cuts
    /// it into chunk requests of at most `max_chunk_bytes`, sends as many as
    /// the `max_inflight_reads` window across the plane allows and holds the
    /// rest until answers free the window. A zero-length read is one empty
    /// chunk.
    ///
    /// On a handle that is closing or gone — closed, refused, or forgotten at a
    /// detach, which the plane reports no event for on a held handle — the
    /// answer is [`ReadError::Gone`], with no frame. Either `Err` consumes
    /// `tag`.
    ///
    /// # Panics
    ///
    /// On a handle this plane never minted, or on a live handle that is not
    /// held.
    pub fn read(
        &mut self,
        handle: BlobHandle,
        tag: T,
        offset: u64,
        len: u64,
    ) -> Result<(ReadId, Vec<ClientFrame>), ReadError> {
        let Some(record) = self.live(handle, "read") else {
            return Err(ReadError::Gone);
        };
        assert_eq!(
            record.phase,
            Phase::Held,
            "attach client: read on blob handle {} that is not held",
            handle.0
        );
        let size = record.size;
        match offset.checked_add(len) {
            Some(end) if end <= size => {
                let read = ReadId(self.next_read);
                self.next_read += 1;
                self.reads.insert(
                    read,
                    Read {
                        tag,
                        handle,
                        next_offset: offset,
                        end,
                        in_flight: 0,
                    },
                );
                self.waiting.push_back(read);
                Ok((read, self.pump()))
            }
            _ => Err(ReadError::OutOfRange { size }),
        }
    }

    /// Give `handle` up. The frame is `BlobClose`; the step also ends what the
    /// plane has not finished putting on the wire: an upload whose seal was not
    /// sent ends `UploadRefused { reason: Closed }`, and every read on the
    /// handle ends `ReadRefused { reason: Closed }`. An upload whose seal is on
    /// the wire and an unanswered open keep their real outcome, reported when
    /// the server's answer arrives.
    ///
    /// On a handle that is closing or gone the step is empty: no `BlobClose`,
    /// no event. So a close on every handle `OutboxSteps::released` names is
    /// safe, including one the plane retired itself — a refusal, its own
    /// close on `Incomplete`, a detach — or the embedder already closed.
    ///
    /// # Panics
    ///
    /// On a handle this plane never minted.
    pub fn close(&mut self, handle: BlobHandle) -> BlobStep<T> {
        let mut step = BlobStep::new();
        if self.live(handle, "close").is_none() {
            return step;
        }
        step.frames.push(ClientFrame::BlobClose { handle });
        let record = self
            .records
            .get_mut(&handle)
            .expect("a live handle has a record");
        record.retired = true;
        if record.phase == Phase::Uploading {
            let tag = record.tag.take().expect("an uploading handle's tag");
            step.events.push(BlobEvent::UploadRefused {
                tag,
                handle,
                reason: UploadRefusal::Closed,
            });
        }
        let ended: Vec<ReadId> = self
            .reads
            .iter()
            .filter(|(_, r)| r.handle == handle)
            .map(|(id, _)| *id)
            .collect();
        for read in ended {
            let r = self.reads.remove(&read).expect("read listed above");
            step.events.push(BlobEvent::ReadRefused {
                read,
                tag: r.tag,
                handle,
                reason: ReadRefusal::Closed,
            });
        }
        self.waiting.retain(|read| self.reads.contains_key(read));
        self.forget_if_done(handle);
        step
    }

    /// Settle one blob answer: `BlobCreateResult`, `BlobSealResult`,
    /// `BlobOpenResult` or `BlobReadResult`.
    ///
    /// # Panics
    ///
    /// On any other frame: the embedder routes only these here.
    pub fn on_frame(&mut self, frame: ServerFrame) -> Result<BlobStep<T>, String> {
        match frame {
            ServerFrame::BlobCreateResult { handle, outcome } => self.on_create(handle, outcome),
            ServerFrame::BlobSealResult { handle, outcome } => self.on_seal(handle, outcome),
            ServerFrame::BlobOpenResult { handle, outcome } => self.on_open(handle, outcome),
            ServerFrame::BlobReadResult {
                correlation,
                outcome,
            } => self.on_read(correlation, outcome),
            other => panic!("attach client: blob plane handed a non-blob frame: {other:?}"),
        }
    }

    fn on_create(
        &mut self,
        handle: BlobHandle,
        outcome: BlobCreateOutcome,
    ) -> Result<BlobStep<T>, String> {
        let record = self.owed(handle, |r| r.create_owed, "BlobCreateResult")?;
        record.create_owed = false;
        let reason = match outcome {
            BlobCreateOutcome::Ok => {
                self.forget_if_done(handle);
                return Ok(BlobStep::new());
            }
            BlobCreateOutcome::BlobTooLarge { size, max } => {
                UploadRefusal::BlobTooLarge { size, max }
            }
            BlobCreateOutcome::BudgetExhausted => UploadRefusal::BudgetExhausted,
            BlobCreateOutcome::HandleLimit => UploadRefusal::HandleLimit,
            BlobCreateOutcome::BytesHeldLimit => UploadRefusal::BytesHeldLimit,
            BlobCreateOutcome::StorageFailed => UploadRefusal::StorageFailed,
        };
        let mut step = BlobStep::new();
        record.retired = true;
        if let Some(tag) = record.tag.take() {
            step.events.push(BlobEvent::UploadRefused {
                tag,
                handle,
                reason,
            });
        }
        self.forget_if_done(handle);
        Ok(step)
    }

    fn on_seal(
        &mut self,
        handle: BlobHandle,
        outcome: BlobSealOutcome,
    ) -> Result<BlobStep<T>, String> {
        let record = self.owed(handle, |r| r.seal_owed, "BlobSealResult")?;
        record.seal_owed = false;
        let mut step = BlobStep::new();
        match (record.tag.take(), outcome) {
            // The create was refused and its upload already reported.
            (None, BlobSealOutcome::UnknownHandle) => {}
            (None, outcome) => {
                return Err(format!(
                    "BlobSealResult {outcome:?} for blob handle {} whose create was refused",
                    handle.0
                ));
            }
            (Some(tag), BlobSealOutcome::Ok) => {
                if !record.retired {
                    record.phase = Phase::Held;
                }
                step.events.push(BlobEvent::Sealed { tag, handle });
            }
            (Some(tag), BlobSealOutcome::Incomplete { offset, len }) => {
                if !record.retired {
                    record.retired = true;
                    step.frames.push(ClientFrame::BlobClose { handle });
                }
                step.events.push(BlobEvent::UploadRefused {
                    tag,
                    handle,
                    reason: UploadRefusal::Incomplete { offset, len },
                });
            }
            (Some(_), BlobSealOutcome::UnknownHandle) => {
                return Err(format!(
                    "BlobSealResult UnknownHandle for blob handle {} whose create was accepted",
                    handle.0
                ));
            }
        }
        self.forget_if_done(handle);
        Ok(step)
    }

    fn on_open(
        &mut self,
        handle: BlobHandle,
        outcome: BlobOpenOutcome,
    ) -> Result<BlobStep<T>, String> {
        let record = self.owed(handle, |r| r.open_owed, "BlobOpenResult")?;
        record.open_owed = false;
        let tag = record.tag.take().expect("an opening handle's tag");
        let reason = match outcome {
            BlobOpenOutcome::Ok { size, content_type } => {
                if !record.retired {
                    record.phase = Phase::Held;
                    record.size = size;
                }
                self.forget_if_done(handle);
                return Ok(BlobStep {
                    frames: Vec::new(),
                    events: vec![BlobEvent::Opened {
                        tag,
                        handle,
                        size,
                        content_type,
                    }],
                });
            }
            BlobOpenOutcome::NotRetained => OpenRefusal::NotRetained,
            BlobOpenOutcome::NoSuchAttachment => OpenRefusal::NoSuchAttachment,
            BlobOpenOutcome::ToolChannel => OpenRefusal::ToolChannel,
            BlobOpenOutcome::HandleLimit => OpenRefusal::HandleLimit,
            BlobOpenOutcome::BytesHeldLimit { size } => OpenRefusal::BytesHeldLimit { size },
        };
        record.retired = true;
        self.forget_if_done(handle);
        Ok(BlobStep {
            frames: Vec::new(),
            events: vec![BlobEvent::OpenRefused {
                tag,
                handle,
                reason,
            }],
        })
    }

    fn on_read(
        &mut self,
        correlation: u64,
        outcome: BlobReadOutcome,
    ) -> Result<BlobStep<T>, String> {
        let request = self.requests.remove(&correlation).ok_or_else(|| {
            format!("BlobReadResult for correlation {correlation} this attachment is not owed")
        })?;
        if let BlobReadOutcome::Ok { bytes } = &outcome
            && bytes.len() as u64 != request.len
        {
            return Err(format!(
                "BlobReadResult for correlation {correlation} carries {} bytes where {} were asked",
                bytes.len(),
                request.len
            ));
        }
        let handle = request.handle;
        let record = self
            .records
            .get_mut(&handle)
            .expect("a handle with a read in flight has a record");
        record.reads_owed -= 1;
        let mut step = BlobStep::new();
        if let Some(read) = self.reads.get_mut(&request.read) {
            read.in_flight -= 1;
            let reason = match outcome {
                BlobReadOutcome::Ok { bytes } => {
                    step.events.push(BlobEvent::Chunk {
                        read: request.read,
                        handle,
                        offset: request.offset,
                        bytes,
                    });
                    None
                }
                BlobReadOutcome::OutOfRange { size } => Some(ReadRefusal::OutOfRange { size }),
                BlobReadOutcome::UnknownHandle => Some(ReadRefusal::UnknownHandle),
                BlobReadOutcome::NotSealed => Some(ReadRefusal::NotSealed),
                BlobReadOutcome::TooManyReads => Some(ReadRefusal::TooManyReads),
                BlobReadOutcome::StorageFailed => Some(ReadRefusal::StorageFailed),
            };
            let done = read.in_flight == 0 && read.next_offset == read.end;
            if reason.is_some() || done {
                let r = self
                    .reads
                    .remove(&request.read)
                    .expect("read looked up above");
                step.events.push(match reason {
                    Some(reason) => BlobEvent::ReadRefused {
                        read: request.read,
                        tag: r.tag,
                        handle,
                        reason,
                    },
                    None => BlobEvent::ReadDone {
                        read: request.read,
                        tag: r.tag,
                        handle,
                    },
                });
                self.waiting.retain(|read| *read != request.read);
            }
        }
        self.forget_if_done(handle);
        step.frames = self.pump();
        Ok(step)
    }

    /// Send waiting chunk requests while the in-flight window has room.
    fn pump(&mut self) -> Vec<ClientFrame> {
        let facts = self.facts.expect("reads are pumped only with a blob door");
        let window = usize::try_from(facts.max_inflight_reads)
            .expect("attach client: max_inflight_reads fits usize");
        let mut frames = Vec::new();
        while self.requests.len() < window {
            let Some(&read_id) = self.waiting.front() else {
                break;
            };
            let read = self
                .reads
                .get_mut(&read_id)
                .expect("a waiting read is live");
            let len = (read.end - read.next_offset).min(facts.max_chunk_bytes);
            let offset = read.next_offset;
            read.next_offset += len;
            read.in_flight += 1;
            let handle = read.handle;
            // A zero-length read is one empty request; afterwards its range is
            // exhausted like any other.
            if read.next_offset == read.end {
                self.waiting.pop_front();
            }
            let correlation = self.next_correlation;
            self.next_correlation += 1;
            self.records
                .get_mut(&handle)
                .expect("a read's handle has a record")
                .reads_owed += 1;
            self.requests.insert(
                correlation,
                ReadRequest {
                    read: read_id,
                    handle,
                    offset,
                    len,
                },
            );
            frames.push(ClientFrame::BlobRead {
                correlation,
                handle,
                offset,
                len,
            });
        }
        frames
    }

    /// The live door, or a panic naming the call.
    fn door(&self, call: &str) -> BlobFacts {
        self.facts.unwrap_or_else(|| {
            panic!("attach client: blob {call} while the live attachment has no blob door")
        })
    }

    fn mint_handle(&mut self) -> BlobHandle {
        let handle = BlobHandle(self.next_handle);
        self.next_handle += 1;
        handle
    }

    /// The record of a live handle, or `None` for one that is closing or gone.
    /// Panics, naming the call, on a handle this plane never minted.
    fn live(&self, handle: BlobHandle, call: &str) -> Option<&Record<T>> {
        assert!(
            handle.0 > 0 && handle.0 < self.next_handle,
            "attach client: {call} on blob handle {} this plane never minted",
            handle.0
        );
        self.records.get(&handle).filter(|record| !record.retired)
    }

    /// The record an answer is owed on, or the error naming the answer.
    fn owed(
        &mut self,
        handle: BlobHandle,
        owes: impl Fn(&Record<T>) -> bool,
        answer: &str,
    ) -> Result<&mut Record<T>, String> {
        match self.records.get_mut(&handle) {
            Some(record) if owes(record) => Ok(record),
            _ => Err(format!(
                "{answer} for blob handle {} this attachment is not owed",
                handle.0
            )),
        }
    }

    /// Drop a retired record once nothing more is owed on it.
    fn forget_if_done(&mut self, handle: BlobHandle) {
        if let Some(record) = self.records.get(&handle)
            && record.retired
            && !record.owes()
        {
            self.records.remove(&handle);
        }
    }

    /// Handles the server's table will hold when it handles the next frame
    /// this plane sends: every live handle. A retired one's `BlobClose` or
    /// refusal is ahead of that frame.
    fn live_handles(&self) -> u64 {
        self.records.values().filter(|r| !r.retired).count() as u64
    }

    /// The plane's bytes held: the sum, over its handles, of what each state
    /// counts. A function of the handle states, not a running total.
    fn bytes_held(&self) -> u64 {
        self.records
            .values()
            .map(Record::bytes_held)
            .fold(0, u64::saturating_add)
    }
}

#[cfg(test)]
mod tests;

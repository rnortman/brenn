//! Attachments: what a request or an answer carries, the handles a step holds
//! on blobs, the [`Blobs`] interface a host serves a step, and the [`Step`] a
//! component's `plan` and `advance` receive.
//!
//! A [`Blob`] or [`Writer`] is a slot in a table the host adapter keeps for
//! one step, not a blob id: a component names a blob only by a handle it was
//! given. The loop calls [`Blobs::release`] once each step's outcome has been
//! carried out, so every handle a step took is gone before the next step
//! runs.

use serde::{Deserialize, Serialize};

use crate::call::{Failure, internal};

/// One attachment as a message's header names it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attached {
    pub name: String,
    pub size: u64,
    pub content_type: String,
}

/// A sealed blob the current step holds. Valid until the step ends.
#[derive(Debug, PartialEq, Eq)]
pub struct Blob {
    slot: u32,
    size: u64,
    content_type: String,
}

impl Blob {
    /// The handle a [`Blobs`] implementation hands out for its table's slot
    /// `slot`. A component never builds one.
    pub fn new(slot: u32, size: u64, content_type: String) -> Blob {
        Blob {
            slot,
            size,
            content_type,
        }
    }

    /// The host adapter's table slot.
    pub fn slot(&self) -> u32 {
        self.slot
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn content_type(&self) -> &str {
        &self.content_type
    }
}

/// A blob being written in the current step. Left unsealed at the step's
/// end, its bytes are deleted.
#[derive(Debug, PartialEq, Eq)]
pub struct Writer {
    slot: u32,
}

impl Writer {
    /// The handle a [`Blobs`] implementation hands out for its table's slot
    /// `slot`. A component never builds one.
    pub fn new(slot: u32) -> Writer {
        Writer { slot }
    }

    /// The host adapter's table slot.
    pub fn slot(&self) -> u32 {
        self.slot
    }
}

/// The host's per-call bounds, in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobLimits {
    pub max_read_bytes: u32,
    pub max_write_bytes: u32,
}

/// The host serves this component no blobs: it is served by the glue's
/// `serve`, which holds no `blobs` grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unavailable;

const UNAVAILABLE: &str = "this provider is served by `serve`, which serves no blobs; a provider \
                           that handles attachments is served by `serve_with_blobs` and granted \
                           `blobs`";

/// The per-activation bound a `Quota` refusal names.
const QUOTA: &str = "the activation's bound on live blob handles or blob calls is reached";

/// Why an attachment gave no handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenError {
    /// The request or answer carries no attachment of that name.
    NotAttached,
    /// The request's message is no longer in the provider's request window.
    LeftWindow,
    /// The message names the blob and nothing holds it any longer.
    NotFound,
    Quota,
    Unavailable,
}

/// Why a read returned no bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadError {
    /// The range passes the blob's size. Never a short read.
    OutOfRange,
    /// The length is over [`BlobLimits::max_read_bytes`].
    TooLarge,
    Quota,
    Storage(String),
    Unavailable,
}

/// Why no writer was created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateError {
    InvalidContentType(String),
    /// The host's blob pool is full.
    BudgetExhausted,
    Quota,
    Storage(String),
    Unavailable,
}

/// Why a write wrote nothing. A refused write changes nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteError {
    /// The bytes are longer than [`BlobLimits::max_write_bytes`].
    TooLarge,
    /// The write would take the blob past the host's largest blob.
    BlobTooLarge,
    /// The host's blob pool is full.
    BudgetExhausted,
    TooFragmented,
    /// The activation has made as many blob calls, or written as many bytes,
    /// as the host allows.
    Quota,
    Storage(String),
    Unavailable,
}

/// Why a writer did not seal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SealError {
    /// The first range below the highest byte written that no write covered.
    Gap {
        offset: u64,
        len: u64,
    },
    Quota,
    Unavailable,
}

macro_rules! from_unavailable {
    ($($error:ident),*) => {$(
        impl From<Unavailable> for $error {
            fn from(_: Unavailable) -> $error {
                $error::Unavailable
            }
        }
    )*};
}

from_unavailable!(OpenError, ReadError, CreateError, WriteError, SealError);

impl From<Unavailable> for Failure {
    fn from(_: Unavailable) -> Failure {
        internal(UNAVAILABLE)
    }
}

impl From<OpenError> for Failure {
    fn from(error: OpenError) -> Failure {
        internal(match error {
            OpenError::NotAttached => "blob open: no attachment of that name".to_string(),
            OpenError::LeftWindow => {
                "blob open: the request is no longer in this provider's request window".to_string()
            }
            OpenError::NotFound => "blob open: the attachment's blob is no longer held".to_string(),
            OpenError::Quota => format!("blob open: {QUOTA}"),
            OpenError::Unavailable => UNAVAILABLE.to_string(),
        })
    }
}

impl From<ReadError> for Failure {
    fn from(error: ReadError) -> Failure {
        internal(match error {
            ReadError::OutOfRange => "blob read: the range passes the blob's end".to_string(),
            ReadError::TooLarge => {
                "blob read: the length is over the host's largest read (max_read_bytes)".to_string()
            }
            ReadError::Quota => format!("blob read: {QUOTA}"),
            ReadError::Storage(detail) => format!("blob read: storage failed: {detail}"),
            ReadError::Unavailable => UNAVAILABLE.to_string(),
        })
    }
}

impl From<CreateError> for Failure {
    fn from(error: CreateError) -> Failure {
        internal(match error {
            CreateError::InvalidContentType(detail) => {
                format!("blob create: the content type is not a media type: {detail}")
            }
            CreateError::BudgetExhausted => {
                "blob create: the host's blob store is full (blob_budget_bytes)".to_string()
            }
            CreateError::Quota => format!("blob create: {QUOTA}"),
            CreateError::Storage(detail) => format!("blob create: storage failed: {detail}"),
            CreateError::Unavailable => UNAVAILABLE.to_string(),
        })
    }
}

impl From<WriteError> for Failure {
    fn from(error: WriteError) -> Failure {
        internal(match error {
            WriteError::TooLarge => {
                "blob write: the bytes are over the host's largest write (max_write_bytes)"
                    .to_string()
            }
            WriteError::BlobTooLarge => {
                "blob write: the blob would exceed the host's largest blob (max_blob_bytes)"
                    .to_string()
            }
            WriteError::BudgetExhausted => {
                "blob write: the host's blob store is full (blob_budget_bytes)".to_string()
            }
            WriteError::TooFragmented => {
                "blob write: the blob has more disjoint written ranges than the host tracks"
                    .to_string()
            }
            WriteError::Quota => "blob write: the activation's bound on blob calls or on bytes \
                                  written is reached"
                .to_string(),
            WriteError::Storage(detail) => format!("blob write: storage failed: {detail}"),
            WriteError::Unavailable => UNAVAILABLE.to_string(),
        })
    }
}

impl From<SealError> for Failure {
    fn from(error: SealError) -> Failure {
        internal(match error {
            SealError::Gap { offset, len } => {
                format!("blob seal: {len} bytes at offset {offset} were never written")
            }
            SealError::Quota => format!("blob seal: {QUOTA}"),
            SealError::Unavailable => UNAVAILABLE.to_string(),
        })
    }
}

/// What a host serves a step: the `blobs` interface, by handle.
pub trait Blobs {
    fn limits(&self) -> Result<BlobLimits, Unavailable>;

    // TODO(kit-request-attachment-outlives-window): open a pending request's
    // attachment in any activation while the request is pending.
    /// A handle to attachment `name` of pending request `id`, found by its
    /// message's place in this activation's request window.
    /// [`OpenError::LeftWindow`] when the message has left the window.
    fn open_request(&self, id: &str, name: &str) -> Result<Blob, OpenError>;

    /// A handle to attachment `name` of the result correlated by `call_id`,
    /// delivered new in this activation.
    fn open_result(&self, call_id: &str, name: &str) -> Result<Blob, OpenError>;

    /// Exactly the bytes `[offset, offset + len)`.
    fn read(&self, blob: &Blob, offset: u64, len: u32) -> Result<Vec<u8>, ReadError>;

    fn create(&self, content_type: &str) -> Result<Writer, CreateError>;

    fn write(&self, writer: &Writer, offset: u64, bytes: &[u8]) -> Result<(), WriteError>;

    fn seal(&self, writer: Writer) -> Result<Blob, SealError>;

    /// Let go of every handle the step took.
    fn release(&self);
}

/// What a step of a request may do with attachments: open the request's and
/// the answer's, read and build blobs, and attach blobs to the request's
/// completion.
pub struct Step<'a> {
    blobs: &'a dyn Blobs,
    id: String,
    call_id: Option<String>,
    request: Vec<Attached>,
    answer: Vec<Attached>,
    attached: Vec<(String, Blob)>,
}

impl<'a> Step<'a> {
    /// The step that plans request `id`'s first call.
    pub fn planning(blobs: &'a dyn Blobs, id: &str, request: Vec<Attached>) -> Step<'a> {
        Step {
            blobs,
            id: id.to_string(),
            call_id: None,
            request,
            answer: Vec::new(),
            attached: Vec::new(),
        }
    }

    /// The step that advances request `id` on the answer to call `call_id`,
    /// which carries `answer`.
    pub fn answering(
        blobs: &'a dyn Blobs,
        id: &str,
        call_id: &str,
        request: Vec<Attached>,
        answer: Vec<Attached>,
    ) -> Step<'a> {
        Step {
            blobs,
            id: id.to_string(),
            call_id: Some(call_id.to_string()),
            request,
            answer,
            attached: Vec::new(),
        }
    }

    /// The attachments the request carries.
    pub fn request_attachments(&self) -> &[Attached] {
        &self.request
    }

    /// The request's attachment `name`.
    pub fn open_request(&self, name: &str) -> Result<Blob, OpenError> {
        if !self.request.iter().any(|attached| attached.name == name) {
            return Err(OpenError::NotAttached);
        }
        self.blobs.open_request(&self.id, name)
    }

    /// The attachments the answer carries; empty while planning.
    pub fn answer_attachments(&self) -> &[Attached] {
        &self.answer
    }

    /// The answer's attachment `name`.
    pub fn open_answer(&self, name: &str) -> Result<Blob, OpenError> {
        let Some(call_id) = &self.call_id else {
            return Err(OpenError::NotAttached);
        };
        if !self.answer.iter().any(|attached| attached.name == name) {
            return Err(OpenError::NotAttached);
        }
        self.blobs.open_result(call_id, name)
    }

    pub fn limits(&self) -> Result<BlobLimits, Unavailable> {
        self.blobs.limits()
    }

    /// Exactly the bytes `[offset, offset + len)` of `blob`.
    pub fn read(&self, blob: &Blob, offset: u64, len: u32) -> Result<Vec<u8>, ReadError> {
        self.blobs.read(blob, offset, len)
    }

    /// The blob in pieces of `max_read_bytes`, in order. The first error ends
    /// the iteration.
    pub fn pieces<'b>(
        &'b self,
        blob: &'b Blob,
    ) -> impl Iterator<Item = Result<Vec<u8>, ReadError>> + 'b {
        let mut offset = 0u64;
        let mut piece: Option<u64> = None;
        let mut ended = false;
        std::iter::from_fn(move || {
            if ended || offset >= blob.size() {
                return None;
            }
            let size = match piece {
                Some(size) => size,
                None => match self.limits() {
                    Ok(limits) => *piece.insert(u64::from(limits.max_read_bytes.max(1))),
                    Err(unavailable) => {
                        ended = true;
                        return Some(Err(unavailable.into()));
                    }
                },
            };
            let len = (blob.size() - offset).min(size);
            let read = self.read(
                blob,
                offset,
                u32::try_from(len).expect("a piece is at most max_read_bytes"),
            );
            match read {
                Ok(_) => offset += len,
                Err(_) => ended = true,
            }
            Some(read)
        })
    }

    /// A blob written front to back, buffered to `max_write_bytes` per call.
    pub fn append(&self, content_type: &str) -> Result<Append<'_>, CreateError> {
        let limits = self.limits()?;
        let writer = self.blobs.create(content_type)?;
        Ok(Append {
            blobs: self.blobs,
            writer,
            offset: 0,
            buffer: Vec::new(),
            piece: usize::try_from(limits.max_write_bytes.max(1))
                .expect("a u32 fits the target's usize"),
        })
    }

    /// Name `blob` as attachment `name` of this request's completion.
    pub fn attach(&mut self, name: &str, blob: Blob) {
        self.attached.push((name.to_string(), blob));
    }

    /// What the step attached, taken out.
    pub(crate) fn take_attached(&mut self) -> Vec<(String, Blob)> {
        std::mem::take(&mut self.attached)
    }
}

/// A blob being written front to back.
pub struct Append<'s> {
    blobs: &'s dyn Blobs,
    writer: Writer,
    offset: u64,
    buffer: Vec<u8>,
    piece: usize,
}

impl Append<'_> {
    // TODO(blob-append-write-copies-whole-argument): write full pieces
    // straight from the argument and buffer only the tail.
    /// Append `bytes`, writing every full piece the buffer holds. After a
    /// refused write the bytes not yet written stay buffered.
    pub fn write(&mut self, bytes: &[u8]) -> Result<(), WriteError> {
        self.buffer.extend_from_slice(bytes);
        let mut at = 0;
        let flushed = loop {
            if self.buffer.len() - at < self.piece {
                break Ok(());
            }
            let piece = &self.buffer[at..at + self.piece];
            if let Err(error) = self.blobs.write(&self.writer, self.offset, piece) {
                break Err(error);
            }
            self.offset += self.piece as u64;
            at += self.piece;
        };
        self.buffer.drain(..at);
        flushed
    }

    // TODO(blob-append-finish-erases-write-error): report a refused write or
    // seal as a typed error.
    /// Write what the buffer holds and seal.
    pub fn finish(self) -> Result<Blob, Failure> {
        let mut offset = self.offset;
        for piece in self.buffer.chunks(self.piece) {
            self.blobs.write(&self.writer, offset, piece)?;
            offset += piece.len() as u64;
        }
        Ok(self.blobs.seal(self.writer)?)
    }
}

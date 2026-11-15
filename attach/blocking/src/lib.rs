//! `brenn-attach-blocking` — a blocking, thread-safe attacher to the brenn bus.
//!
//! [`BlockingClient::attach`] starts one background thread that owns the
//! attachment: it dials, holds the connection live, reconnects with backoff,
//! resubscribes held channels, and answers each call. Any thread calls in;
//! every method takes `&self`.
//!
//! - [`connector`]: the dial, with TLS and a typed [`DialFailure`].
//! - [`supervisor`]: the loop over the attach client's driver and planes.
//! - [`handle`]: the blocking face, [`BlockingClient`].
//!
//! The client offers attach protocol version 6 exactly, so every body and
//! blob is carried and a server that does not speak 6 ends the attachment with
//! [`EndCause::Incompatible`].
//!
//! **A client that holds a subscription must read.** Subscription events wait
//! in a buffer of [`EVENT_CAPACITY`]; while it is full the supervisor stops
//! reading the socket, the server drops for this attacher, and the loss
//! reaches the reader as `dropped` or `gap` once it reads again. A publish
//! made while the buffer is full is not answered until the reader drains it.
//!
//! **A blob lasts until it is closed or its attachment drops.** A [`Blob`]
//! names a handle on the live attachment's blob table; nothing held survives
//! a reconnect. Blob bytes go to the caller that asked and never enter the
//! event buffer, but a blob answer, like a publish's, is not taken while the
//! buffer is full: a client that moves blobs while it holds a subscription
//! must read too.
//!
//! Nothing here logs or installs a `tracing` subscriber: every fact a caller
//! needs leaves as a value.

use std::fmt;
use std::io;
use std::path::PathBuf;

use brenn_attach_client::blobs;
use brenn_attach_client::conn::DetachReason;
use brenn_attach_proto::{GapInfo, VersionRange};
use brenn_envelope::MessageEnvelope;

pub mod connector;
pub mod handle;
pub mod supervisor;

pub use brenn_attach_client::conn::AttachmentFacts;
pub use brenn_attach_client::publish::PublishRequest;
pub use brenn_attach_client::subs::{ResumePolicy, SubscriptionDepths};
pub use brenn_attach_proto::{AttachmentName, BlobFacts, BlobHandle, BodyError, PublishAttachment};
pub use connector::BlockingConnector;
pub use handle::{AttachParams, BlockingClient};
pub use uuid::Uuid;

/// The subscription events the buffer holds before the supervisor stops
/// reading the socket. Soft by one frame: a delivery frame is taken whole.
pub const EVENT_CAPACITY: usize = 256;

/// The one protocol version this client offers.
pub const PROTOCOL: VersionRange = VersionRange::exactly(6);

/// A blob this client holds on its live attachment: an upload it sealed or an
/// attachment it opened. A name, not an owner: cloning it copies the name, and
/// dropping it releases nothing; [`BlockingClient::close_blob`] does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    /// The [`BlockingClient`] that holds it; no other may name it.
    pub(crate) client: u64,
    pub(crate) handle: BlobHandle,
    pub(crate) size: u64,
    pub(crate) content_type: String,
}

impl Blob {
    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn content_type(&self) -> &str {
        &self.content_type
    }
}

/// Why an upload made no blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadRefusal {
    /// `size` is over the per-blob cap `max`.
    BlobTooLarge { size: u64, max: u64 },
    /// The host's blob pool cannot reserve the size.
    BudgetExhausted,
    /// The session's table of handles is full: close a blob.
    HandleLimit,
    /// The session's bytes held would pass `max_bytes_held`: close a blob.
    BytesHeldLimit,
    /// The host could not create the blob's file.
    StorageFailed,
    /// `[offset, offset + len)` was not written: a host-side write failure.
    /// Upload again.
    Incomplete { offset: u64, len: u64 },
}

/// Why an open made no blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenRefusal {
    /// The message is not in the channel's window now, is parked, or the
    /// channel is gone.
    NotRetained,
    /// The message carries no attachment of that name.
    NoSuchAttachment,
    /// The channel is of the tool-substrate family, whose attachments are
    /// never opened over the bus.
    ToolChannel,
    /// The session's table of handles is full: close a blob.
    HandleLimit,
    /// The session's bytes held plus the blob's `size` would pass
    /// `max_bytes_held`.
    BytesHeldLimit { size: u64 },
}

/// Why a read ended before its last byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRefusal {
    /// The range passes the blob's `size`.
    OutOfRange { size: u64 },
    /// The server found no such handle in the session's table.
    UnknownHandle,
    /// The server found the handle an unsealed upload.
    NotSealed,
    /// The server had its bound of read answers in flight.
    TooManyReads,
    /// The host could not read the blob's file.
    StorageFailed,
    /// The blob was closed during the read.
    Closed,
}

/// How a client-crate refusal reaches a caller: as a refusal, or as the end
/// of what the refused operation stood on.
pub(crate) enum Settled<R> {
    Refused(R),
    /// The caller closed or abandoned the operation.
    Closed,
    /// The attachment ended first.
    Detached,
}

impl UploadRefusal {
    pub(crate) fn settle(refusal: blobs::UploadRefusal) -> Settled<Self> {
        Settled::Refused(match refusal {
            blobs::UploadRefusal::BlobTooLarge { size, max } => {
                UploadRefusal::BlobTooLarge { size, max }
            }
            blobs::UploadRefusal::BudgetExhausted => UploadRefusal::BudgetExhausted,
            blobs::UploadRefusal::HandleLimit => UploadRefusal::HandleLimit,
            blobs::UploadRefusal::BytesHeldLimit => UploadRefusal::BytesHeldLimit,
            blobs::UploadRefusal::StorageFailed => UploadRefusal::StorageFailed,
            blobs::UploadRefusal::Incomplete { offset, len } => {
                UploadRefusal::Incomplete { offset, len }
            }
            blobs::UploadRefusal::Closed => return Settled::Closed,
            blobs::UploadRefusal::Detached => return Settled::Detached,
        })
    }
}

impl OpenRefusal {
    pub(crate) fn settle(refusal: blobs::OpenRefusal) -> Settled<Self> {
        Settled::Refused(match refusal {
            blobs::OpenRefusal::NotRetained => OpenRefusal::NotRetained,
            blobs::OpenRefusal::NoSuchAttachment => OpenRefusal::NoSuchAttachment,
            blobs::OpenRefusal::ToolChannel => OpenRefusal::ToolChannel,
            blobs::OpenRefusal::HandleLimit => OpenRefusal::HandleLimit,
            blobs::OpenRefusal::BytesHeldLimit { size } => OpenRefusal::BytesHeldLimit { size },
            blobs::OpenRefusal::Detached => return Settled::Detached,
        })
    }
}

impl ReadRefusal {
    pub(crate) fn settle(refusal: blobs::ReadRefusal) -> Settled<Self> {
        Settled::Refused(match refusal {
            blobs::ReadRefusal::OutOfRange { size } => ReadRefusal::OutOfRange { size },
            blobs::ReadRefusal::UnknownHandle => ReadRefusal::UnknownHandle,
            blobs::ReadRefusal::NotSealed => ReadRefusal::NotSealed,
            blobs::ReadRefusal::TooManyReads => ReadRefusal::TooManyReads,
            blobs::ReadRefusal::StorageFailed => ReadRefusal::StorageFailed,
            blobs::ReadRefusal::Closed => ReadRefusal::Closed,
            blobs::ReadRefusal::Detached => return Settled::Detached,
        })
    }
}

/// What a local file failure was, kept as values so [`CallError`] stays
/// `Clone` and `Eq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileError {
    pub kind: io::ErrorKind,
    /// The operating system's error number, when the failure has one.
    pub os_error: Option<i32>,
    pub detail: String,
}

impl From<&io::Error> for FileError {
    fn from(error: &io::Error) -> Self {
        FileError {
            kind: error.kind(),
            os_error: error.raw_os_error(),
            detail: error.to_string(),
        }
    }
}

/// Why one dial reached no attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialFailure {
    /// Name resolution or the TCP connect failed.
    Io { detail: String },
    /// The TLS handshake failed, an untrusted certificate included.
    Tls { detail: String },
    /// The server answered the websocket upgrade with this HTTP status.
    UpgradeRejected { status: u16 },
    /// Any other websocket handshake failure.
    Handshake { detail: String },
    /// The attempt outlived the connect deadline.
    TimedOut,
}

impl fmt::Display for DialFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DialFailure::Io { detail } => write!(f, "connect failed: {detail}"),
            DialFailure::Tls { detail } => write!(f, "TLS handshake failed: {detail}"),
            DialFailure::UpgradeRejected { status } => {
                write!(f, "the server rejected the upgrade with HTTP {status}")
            }
            DialFailure::Handshake { detail } => write!(f, "websocket handshake failed: {detail}"),
            DialFailure::TimedOut => f.write_str("the attempt outlived the connect deadline"),
        }
    }
}

/// The server's answer to a publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The message was published.
    Ok,
    /// The server took the frame, but the publish failed on a path it reports
    /// rather than stops on.
    Failed,
    /// The sender's server-side budget refused the publish.
    RateLimited,
    /// The body is `len` bytes; the server takes at most `max`.
    BodyTooLarge { len: u64, max: u64 },
    /// The body is not text and the channel carries text only. Nothing was
    /// stored.
    TextOnlyChannel,
    /// A blob the publish names was closed, refused or never sealed. Nothing
    /// was stored.
    AttachmentNotHeld,
}

/// A detach reason as text for a person: the peer's close code and reason,
/// or the stack's, or the liveness window passing in silence.
pub fn describe_detach(reason: &DetachReason) -> String {
    match reason {
        DetachReason::LivenessTimeout => {
            "no frame from the server within the liveness window".to_string()
        }
        DetachReason::TransportClosed {
            code: Some(code),
            reason,
        } => format!("closed with code {code}: {reason}"),
        DetachReason::TransportClosed { code: None, reason } => reason.clone(),
    }
}

/// Why the attachment ended for good. Sticky: once the event buffer drains,
/// every read and every call reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndCause {
    /// The client was closed.
    Closed,
    /// A server frame could not be reconciled with the protocol.
    Fatal { detail: String },
    /// The two ends have no protocol version in common.
    Incompatible {
        ours: VersionRange,
        theirs: VersionRange,
    },
    /// This many consecutive attachments sent something and were answered
    /// nothing: the server is refusing what this client sends.
    Futile { attachments: u32 },
    /// The server rejected the credential on the upgrade (HTTP 401 or 403).
    CredentialRejected { status: u16 },
    /// The supervisor thread panicked.
    Panicked { message: String },
}

impl fmt::Display for EndCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EndCause::Closed => f.write_str("the client was closed"),
            EndCause::Fatal { detail } => write!(f, "attachment protocol error: {detail}"),
            EndCause::Incompatible { ours, theirs } => write!(
                f,
                "no protocol version in common: this client speaks {}..={}, the server {}..={}",
                ours.min, ours.max, theirs.min, theirs.max
            ),
            EndCause::Futile { attachments } => write!(
                f,
                "{attachments} consecutive attachments were answered nothing after sending; \
                 the server is refusing what this client sends"
            ),
            EndCause::CredentialRejected { status } => {
                write!(f, "the server rejected the credential with HTTP {status}")
            }
            EndCause::Panicked { message } => write!(f, "the supervisor panicked: {message}"),
        }
    }
}

/// Why a call produced no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// No live attachment carried the call.
    NotAttached,
    /// The attachment dropped before the server answered. A publish may or may
    /// not have landed. A subscribe or publish the server does not allow looks
    /// like this: the server closes the socket with no code.
    ConnectionLost {
        reason: DetachReason,
    },
    /// No answer within the call's timeout; the attachment may still be up.
    Timeout,
    /// The caller's interrupt check asked to abandon the wait.
    Interrupted,
    /// The attachment is over.
    Ended(EndCause),
    /// The channel is already held by this client, or its subscribe is
    /// unanswered.
    AlreadySubscribed,
    /// The channel is not held by this client.
    NotSubscribed,
    /// A `local:` channel is confined to its attacher and never crosses the wire.
    LocalChannel,
    /// A subscription with both depths zero asks for nothing.
    NoDepth,
    /// The live attachment has no blob door: the host keeps no blob store.
    /// Answers an upload, an open, and a publish that names attachments.
    NoBlobDoor,
    /// The blob door grants no upload: the remote has no publish authority.
    UploadNotGranted,
    /// The [`Blob`] was made by another client.
    ForeignBlob,
    /// The blob's handle is closed, or its attachment has ended.
    BlobGone,
    /// The content type is malformed.
    ContentType(BodyError),
    UploadRefused(UploadRefusal),
    OpenRefused(OpenRefusal),
    ReadRefused(ReadRefusal),
    /// A local file could not be opened, read, created or written.
    File {
        path: PathBuf,
        error: FileError,
    },
    /// This crate broke its own contract with the attach client; neither the
    /// caller nor the server is at fault, and a retry meets it again.
    Fault {
        detail: String,
    },
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::NotAttached => f.write_str("no live attachment"),
            CallError::ConnectionLost { reason } => {
                write!(
                    f,
                    "the attachment dropped before the answer: {}",
                    describe_detach(reason)
                )
            }
            CallError::Timeout => f.write_str("no answer within the timeout"),
            CallError::Interrupted => f.write_str("the wait was interrupted"),
            CallError::Ended(cause) => write!(f, "the attachment is over: {cause}"),
            CallError::AlreadySubscribed => f.write_str("the channel is already subscribed"),
            CallError::NotSubscribed => f.write_str("the channel is not subscribed"),
            CallError::LocalChannel => f.write_str("a local: channel never crosses the wire"),
            CallError::NoDepth => f.write_str("a subscription needs a nonzero depth"),
            CallError::NoBlobDoor => f.write_str("the attachment has no blob door"),
            CallError::UploadNotGranted => {
                f.write_str("the blob door grants no upload: this remote may not publish")
            }
            CallError::ForeignBlob => f.write_str("the blob belongs to another client"),
            CallError::BlobGone => {
                f.write_str("the blob is closed, or the attachment that held it has ended")
            }
            CallError::ContentType(error) => write!(f, "{error}"),
            CallError::UploadRefused(refusal) => write!(f, "the upload was refused: {refusal:?}"),
            CallError::OpenRefused(refusal) => write!(f, "the open was refused: {refusal:?}"),
            CallError::ReadRefused(refusal) => write!(f, "the read was refused: {refusal:?}"),
            CallError::File { path, error } => {
                write!(f, "{}: {}", path.display(), error.detail)
            }
            CallError::Fault { detail } => {
                write!(f, "a fault in brenn-attach-blocking itself: {detail}")
            }
        }
    }
}

impl std::error::Error for CallError {}

/// How the server settled a subscribe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeAnswer {
    /// The subscription is open; `replay_count` retained messages follow, and
    /// `gap` says the resume claim could not be covered.
    Subscribed {
        replay_count: u32,
        gap: Option<GapInfo>,
    },
    /// The channel is inside the grant but not there now. Nothing is held.
    Unavailable,
}

/// One message delivered on a held channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub channel: String,
    pub envelope: MessageEnvelope,
    /// The span sequence, restarting at 1 with each subscription span.
    pub seq: u64,
    /// Messages lost on the channel before this one.
    pub dropped: u64,
}

/// What a reader is shown.
///
/// The first three are subscription facts, kept in wire order. The last two
/// are connection state, coalesced: the reader is shown the latest state it has
/// not seen, once the subscription events are drained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Delivery(Box<Delivery>),
    /// A held channel was re-sent at a new attachment and the server opened it.
    Resubscribed {
        channel: String,
        replay_count: u32,
        gap: Option<GapInfo>,
    },
    /// A held channel was answered `Unavailable` on resubscribe; the hold is
    /// gone.
    SubscriptionDropped {
        channel: String,
    },
    /// The attachment is live again.
    Attached(AttachmentFacts),
    /// The attachment dropped; reconnection is under way.
    Detached {
        reason: DetachReason,
        last_dial_failure: Option<DialFailure>,
    },
}

impl Event {
    /// The channel a subscription event concerns; `None` for connection state.
    pub fn channel(&self) -> Option<&str> {
        match self {
            Event::Delivery(delivery) => Some(&delivery.channel),
            Event::Resubscribed { channel, .. } | Event::SubscriptionDropped { channel } => {
                Some(channel)
            }
            Event::Attached(_) | Event::Detached { .. } => None,
        }
    }
}

/// Where the attachment stands now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Attached(AttachmentFacts),
    /// No live attachment. `reason` is the last drop's; `last_dial_failure`
    /// is the latest dial's since.
    Detached {
        reason: DetachReason,
        last_dial_failure: Option<DialFailure>,
    },
    Ended(EndCause),
}

/// Why `recv` returned no event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// The attachment is over and every buffered event has been read.
    Ended(EndCause),
    /// The caller's interrupt check asked to abandon the wait.
    Interrupted,
}

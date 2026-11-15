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
//! The client offers attach protocol version 5 exactly, so every body is
//! carried and a server that does not speak 5 ends the attachment with
//! [`EndCause::Incompatible`].
//!
//! **A client that holds a subscription must read.** Subscription events wait
//! in a buffer of [`EVENT_CAPACITY`]; while it is full the supervisor stops
//! reading the socket, the server drops for this attacher, and the loss
//! reaches the reader as `dropped` or `gap` once it reads again. A publish
//! made while the buffer is full is not answered until the reader drains it.
//!
//! Nothing here logs or installs a `tracing` subscriber: every fact a caller
//! needs leaves as a value.

use std::fmt;

use brenn_attach_client::conn::DetachReason;
use brenn_attach_proto::{GapInfo, VersionRange};
use brenn_envelope::MessageEnvelope;

pub mod connector;
pub mod handle;
pub mod supervisor;

pub use brenn_attach_client::conn::AttachmentFacts;
pub use brenn_attach_client::publish::PublishRequest;
pub use brenn_attach_client::subs::{ResumePolicy, SubscriptionDepths};
pub use connector::BlockingConnector;
pub use handle::{AttachParams, BlockingClient};

/// The subscription events the buffer holds before the supervisor stops
/// reading the socket. Soft by one frame: a delivery frame is taken whole.
pub const EVENT_CAPACITY: usize = 256;

/// The one protocol version this client offers.
// TODO(python-binding-no-blob-transfer): v6 adds blob transfer, which this
// client does not offer; widening the range is that work.
pub const PROTOCOL: VersionRange = VersionRange::exactly(5);

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

/// The server's answer to a publish: the wire's outcomes but
/// `AttachmentNotHeld`, which answers a publish naming a blob, and ends the
/// attachment here, since this client names none.
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
    ConnectionLost { reason: DetachReason },
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

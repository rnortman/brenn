//! `brenn-attach-client` — the attacher-generic half of a bus attachment.
//!
//! An *attacher* is anything that attaches to Brenn's message bus over the
//! websocket attachment protocol: the browser surface kernel today, a native
//! daemon tomorrow. Everything in this crate is common to both — it names no
//! component, instance, port, DOM node, or pixel, and depends on no surface
//! crate. That dependency direction is the purity proof: the compiler refuses a
//! surface concept that leaks in here.
//!
//! What lives here today is the I/O boundary, the time currency the sans-I/O
//! layers above it are driven with, and those layers themselves: [`transport`]
//! (the connector/connection traits and the native + browser implementations),
//! [`Millis`] (the monotonic stamp every input carries), [`conn`] (the
//! connection lifecycle — connect, version handshake, liveness, backoff),
//! [`subs`] (the wire subscription plane — refcounted per-channel
//! subscriptions, resume cursors, and span continuity), [`store`] (what the
//! attachment keeps of each channel, and the windows its readers are served
//! from), [`publish`] (outstanding publishes, the per-registrant atomic-flush
//! outboxes, and the parked-set mirror), [`blobs`] (the handles on the peer's
//! blob table and the chunks an upload or a read moves), and [`router`] (the attacher's own
//! authority over the confined channels that never cross the wire, including
//! what is scheduled onto them). [`driver`] is where those layers meet the
//! outside: it owns the connector, the live transport and the armed deadlines,
//! and executes what they answer — but not the embedder's loop, whose select
//! arms and their bias are the embedder's own.
//!
//! Authentication is the connector's business, not the protocol's: the native
//! connector injects either a cookie header or an `Authorization: Bearer` one
//! depending on which credential it was constructed with, and the browser
//! connector relies on the same-origin cookie the browser attaches. The choice
//! is invisible to every layer below the seam.
//!
//! # TLS: this crate compiles no backend
//!
//! The native transport takes `tokio-tungstenite` with no TLS feature enabled,
//! and `tokio-tungstenite` ships none by default. A `wss://` dial through an
//! unmodified consumer therefore fails with "TLS support not compiled in"
//! before any TLS handshake is attempted — neither the upgrade request nor the
//! credential it carries ever reaches the wire.
//! [`transport::native`] asserts that refusal; every other test here dials
//! `ws://localhost` and never meets it.
//!
//! That is deliberate. Trust store, crypto provider, and link policy are
//! binary-level choices, not a library's, and cargo's feature unification
//! already lets the binary state them in one line. **A native consumer that
//! dials `wss://` must enable a backend on its own `tokio-tungstenite`
//! dependency**, version-unified with the one here:
//!
//! ```toml
//! tokio-tungstenite = { version = "0.29", features = ["native-tls"] }
//! ```
//!
//! No code in the consumer needs to call it — naming the feature is the whole
//! act. Prefer `native-tls` unless the binary has a reason not to: it reads the
//! platform trust store, so an operator-installed CA for a LAN origin works.
//! The rustls features carry a caveat — `tokio-tungstenite` takes rustls with
//! no crypto-provider feature, and rustls 0.23 panics on the first handshake
//! unless the binary installs a provider itself.
//!
//! # For a daemon author: bodies and protocol versions
//!
//! **The offered range.** [`ConnConfig::versions`](conn::ConnConfig::versions)
//! is the range of attach protocol versions the connection offers; it must be
//! non-empty and lie within [`brenn_attach_proto::SUPPORTED_VERSIONS`], or
//! [`Connection::start`](conn::Connection::start) panics. Version 4 carries
//! untyped text only; version 5 carries any body, bytes and content type;
//! version 6 is version 5 plus blob transfer (see "Blobs" below). A
//! daemon that needs bytes offers `VersionRange::exactly(5)`: a server that
//! cannot speak it is then a terminal [`ConnEvent::Incompatible`](conn::ConnEvent::Incompatible)
//! rather than a v4 attachment that cannot carry what the daemon publishes. A
//! daemon that publishes only text may offer both and take whichever the
//! server agrees.
//!
//! **Asking before publishing.** [`AttachmentFacts::carries`](conn::AttachmentFacts::carries)
//! answers whether the live attachment's version can write a body. Handing the
//! planes a body it answers false for is an embedder bug and panics at the
//! encoder, naming the version and the body's content type and size. A flush
//! queued while detached is checked by the plane when the next attachment
//! comes up, and dropped whole if that attachment cannot carry it.
//!
//! **Bodies.** Every body is a [`MessageBody`]: [`MessageBody::text`] for
//! untyped text, [`MessageBody::from_parts`] for anything else. A body is text
//! when it is untyped or its content type is textual — `text/*`,
//! `application/json` or `*+json`, with no charset other than UTF-8 or
//! US-ASCII — and a text body's bytes must be UTF-8; `from_parts` refuses a
//! malformed content type and a text body that is not UTF-8. A content type
//! is the daemon's assertion and is never checked against the bytes.
//!
//! **Outcomes a daemon must handle.** A single publish is answered with a
//! [`PublishOutcome`](brenn_attach_proto::PublishOutcome): besides `Ok` and
//! `Failed`, `RateLimited` (a server-side budget refused it; retry later),
//! `BodyTooLarge` (over the server's body cap) and `TextOnlyChannel` (a
//! non-text body offered to a channel declared to carry only text; it can
//! never succeed and nothing was stored). On version 6 a publish naming blob
//! handles may also be answered `AttachmentNotHeld`. A batch is answered
//! `Ok`, `RateLimited`, `TextOnlyChannel` or, on version 6,
//! `AttachmentNotHeld`; the outboxes hand a refused flush
//! back as lost with its outcome in [`publish::BatchAnswer`]. A subscribe
//! answered `Unavailable` may mean the channel is declared `non_text` and
//! this attachment is on version 4.
//!
//! **What is delivered.** A delivered row holds a [`MessageEnvelope`]: on
//! version 5 its body is exactly the bytes and content type that were
//! published; on version 4 it is the text, without a content type. A
//! [`DeferredViewEntry`](brenn_attach_proto::DeferredViewEntry) holds its body
//! the same way. A version 4 attachment's deferred view omits entries it
//! cannot represent: a non-text entry parked by a version 5 session of the
//! same remote is not in it.
//!
//! # For a daemon author: blobs
//!
//! A delivered envelope names its attachments by reference
//! ([`MessageEnvelope::attachments`]) on every version. Version 6 adds the
//! bytes: a session can upload a blob and name it in a publish, and open an
//! attachment of a message it was delivered and read it.
//!
//! **Getting the door.** Offer `VersionRange::exactly(6)`, and read
//! [`AttachmentFacts::blobs`](conn::AttachmentFacts::blobs): `Some` says the
//! attachment speaks version 6 *and* the server opened the blob door, with
//! the bounds it enforces; `upload` says whether this session may create
//! blobs, which it may only with publish authority. Asking the
//! [`BlobPlane`](blobs::BlobPlane) to `create` or `open` when it is `None`, or
//! to `create` when `upload` is false, is an embedder bug and panics. A daemon
//! that offers a range instead of `exactly(6)` reads `AttachmentFacts::blobs`
//! again at every attachment, and before it hands over a flush that names a
//! handle: a flush composed before a reconnect can meet an attachment without
//! a door, and is then the daemon's to refuse
//! ([`Outboxes::refuse_flush`](publish::Outboxes::refuse_flush)), as a body
//! the new version cannot write is. Hand
//! the plane every attachment's start and end
//! ([`on_attached`](blobs::BlobPlane::on_attached),
//! [`on_detached`](blobs::BlobPlane::on_detached)) and every blob answer
//! ([`on_frame`](blobs::BlobPlane::on_frame)); send the frames each step
//! returns, in order. Nothing need wait for an answer: the plane keeps what
//! the server still owes.
//!
//! **Uploading.** [`create`](blobs::BlobPlane::create) with the size and
//! content type mints a handle; then, while
//! [`next_chunk`](blobs::BlobPlane::next_chunk) names a range, read those
//! bytes from the file or buffer and hand them to
//! [`write_chunk`](blobs::BlobPlane::write_chunk). The last chunk sends the
//! seal. `Sealed` says the handle is a held blob; `UploadRefused` says it is
//! gone and nothing was made — except `Detached` after the seal was sent,
//! when the server may have sealed the blob: the session's hold died with
//! the session, so the blob lives only if a publish naming it was applied
//! before the connection broke. A chunk handed to `write_chunk` after the
//! refusal is discarded. The handle may be named in a publish once the
//! seal is sent, without waiting for `Sealed`: the server handles a session's
//! frames in order, so it has sealed the blob before it reads the publish.
//!
//! **Downloading.** [`open`](blobs::BlobPlane::open) names an attachment by
//! the channel, the message id and the attachment name of a delivered
//! envelope; `Opened` gives its size and content type. Then
//! [`read`](blobs::BlobPlane::read) any range of it: `Chunk` events arrive in
//! ascending offset order, then `ReadDone`. The plane cuts the range at
//! `max_chunk_bytes` and keeps at most `max_inflight_reads` requests on the
//! wire.
//!
//! **Forwarding.** Open the attachment and name the handle in a publish, with
//! no read: the new message names the same blob, and no byte moves. Every
//! forwarded attachment is held at once, so a message whose attachments
//! together exceed `max_bytes_held` cannot be forwarded whole in one publish.
//!
//! **Handles.** A handle is this attachment's: it lives until it is closed or
//! the attachment ends, and a daemon that reconnects uploads or opens again —
//! the plane's `on_detached` fails everything outstanding with `Detached`.
//! Publishing does not close a handle; the blob a published message names
//! lives as long as the message is retained, whatever becomes of the handle.
//! What the embedder may do with a handle, by state:
//!
//! | State | Entered by | The embedder may |
//! |---|---|---|
//! | uploading | `create` | `next_chunk`, `write_chunk`; `close` |
//! | sealing | the `write_chunk` (or empty `create`) that returned `BlobSeal` | name it in a publish; `close` |
//! | opening | `open` | name it in a publish; `close` |
//! | held | the `Sealed` or `Opened` event | `read`; name it in a publish; `close` |
//! | closing | `close`, while answers are owed | nothing takes effect: `next_chunk` answers `None`, `write_chunk` returns no frames, `read` answers `ReadError::Gone`, `close` returns an empty step |
//! | gone | `close` with nothing owed; the last owed answer of a closing handle; `UploadRefused`; `OpenRefused`; `on_detached` | nothing takes effect: `next_chunk` answers `None`, `write_chunk` returns no frames, `read` answers `ReadError::Gone`, `close` returns an empty step |
//!
//! [`close`](blobs::BlobPlane::close) sends `BlobClose` and, in the same step,
//! ends what the plane has not finished putting on the wire: an upload whose
//! seal was not sent ends `UploadRefused { reason: Closed }`, and every read on
//! the handle ends `ReadRefused { reason: Closed }`. An upload whose seal is on
//! the wire and an unanswered open keep their real outcome. The rule for every
//! owed answer, on a live handle and on a closing one: an owed answer yields
//! an event exactly when the operation it answers has not had its outcome
//! reported; otherwise it is absorbed. So a late read answer on a closed
//! handle is absorbed, and a late seal or open answer is reported as its real
//! event.
//!
//! A call on a handle the plane has retired — by the daemon's `close`, a
//! refusal, the plane's own close on `Incomplete`, or a detach — is absorbed:
//! it sends nothing, reports nothing and changes nothing. A handle the plane
//! never minted panics. A repeated `close` is therefore not detected.
//! `UploadRefused` and `OpenRefused` name the handle that is gone, and a daemon
//! drops its own bookkeeping of that handle at the event.
//!
//! **When to close a handle a publish names.** Behind the publish, with no
//! wait, to publish or drop: the server reads the publish first, so it is
//! applied or refused on its own merits. On the publish's answer, to keep the
//! retry: a `RateLimited` publish can be sent again only while the handle is
//! held. For a flush handed to the [`publish::Outboxes`], when
//! [`OutboxSteps::released`](publish::OutboxSteps::released) names the handle:
//! a flush can wait behind another, or be re-parked after `RateLimited`, long
//! after it was composed, and a handle named by several flushes is released
//! once per naming. [`FlushBatch::handles`](publish::FlushBatch::handles)
//! names the handles of the flushes `deregister` returns. Closing on every
//! release is safe, including one naming a handle the plane already retired.
//! A flush naming a handle that is parked at a detach, or handed over while
//! detached, is dropped whole and reported in `dropped`; one in flight at the
//! detach has no reported outcome — it may have been applied — and a daemon
//! that must know reads the channel. At a detach every handle is gone and a
//! daemon drops its own bookkeeping of them. A flush composed before a detach
//! and handed over after the next attachment is up is sent and answered
//! `AttachmentNotHeld` when that attachment has a door; when it has none, see
//! *Getting the door*. Closing early is not an error the crate detects: the
//! publish is answered `AttachmentNotHeld`.
//!
//! **Outcomes a blob daemon must handle**, each as the plane's event. Every
//! `UploadRefused` and `OpenRefused` names the handle that is gone beside the
//! tag:
//!
//! - `UploadRefused`: `BlobTooLarge` (over `max_blob_bytes`; never storable),
//!   `BudgetExhausted` (the host's pool is full; retry later), `HandleLimit`
//!   (close a handle first), `BytesHeldLimit` (close a held handle first),
//!   `StorageFailed` and `Incomplete` (a host-side failure; upload again),
//!   `Closed` and `Detached` (the daemon's own doing, or the connection's; a
//!   `Detached` upload whose seal was sent may have been sealed, and its blob
//!   lives only if a publish naming it was applied).
//! - `OpenRefused`: `NotRetained` (the message is not in the channel's
//!   window: rolled out, or still parked), `NoSuchAttachment` (the
//!   message names no attachment of that name), `ToolChannel` (attachments on
//!   the tool-substrate channels are never opened over attach),
//!   `HandleLimit`, `BytesHeldLimit { size }` (close a held handle and open
//!   again, or give the attachment up when `size` exceeds `max_bytes_held`),
//!   `Detached`.
//! - `ReadRefused`: `Closed` and `Detached`, and, from a server that disagrees
//!   with the plane, `OutOfRange`, `UnknownHandle`, `NotSealed`,
//!   `TooManyReads` and `StorageFailed` — the read is over either way.
//! - `AttachmentNotHeld` on a publish: a handle it names was closed, refused
//!   or never sealed; nothing was stored, and no retry can succeed.
//!
//! [`create`](blobs::BlobPlane::create), [`open`](blobs::BlobPlane::open) and
//! [`read`](blobs::BlobPlane::read) also refuse locally, sending nothing, when
//! the call cannot succeed against the advertised bounds. The server is the
//! authority on the bounds; a call that passes locally may still be refused
//! there. `read` answers `ReadError::Gone`, sending nothing, on a handle that is
//! closing or gone.
//!
//! **The replay store holds references, not blobs.** A reference read from a
//! channel's replay store can outlive its blob, and opening it answers
//! `NotRetained`.
//!
//! # What a consumer that binds this crate may rely on
//!
//! The crate is not published; it reaches a consumer by pin, and every
//! statement here is about source compatibility at a pin bump.
//!
//! - **Reading is additive.** A delivered [`MessageEnvelope`], a
//!   `DeferredViewEntry`, [`AttachmentFacts`](conn::AttachmentFacts),
//!   [`BlobFacts`](brenn_attach_proto::BlobFacts) and the reports of the
//!   planes ([`OutboxSteps`](publish::OutboxSteps),
//!   [`Discarded`](publish::Discarded), [`BlobStep`](blobs::BlobStep)) may
//!   gain fields; code that reads fields keeps compiling.
//! - **Building requests is additive through the constructors.** Build a
//!   [`PublishRequest`](publish::PublishRequest) with `PublishRequest::new`,
//!   `.attribution(..)` and `.attachments(..)`, and a
//!   [`BatchEntry`](brenn_attach_proto::BatchEntry) with `BatchEntry::new`,
//!   `.deliver_after(..)` and `.attachments(..)`. A struct literal compiles
//!   today and breaks when a field is added.
//! - **A new outcome or event variant is a compile break, deliberately.** The
//!   enums stay exhaustive: a refusal a daemon does not handle is a bug.
//! - **A later protocol version does not change these types' meaning.** The
//!   connection speaks only the range it is configured to offer; a consumer
//!   that stays on `exactly(5)` after a pin bump is delivered what it was
//!   before.
//! - **The blob API is covered by these rules.** The
//!   [`BlobPlane`](blobs::BlobPlane)'s methods, [`BlobStep`](blobs::BlobStep),
//!   [`BlobEvent`](blobs::BlobEvent), the tag and handle each of its outcome
//!   events carries, and its refusal reasons,
//!   [`BlobHandle`](brenn_attach_proto::BlobHandle),
//!   [`PublishAttachment`](brenn_attach_proto::PublishAttachment), the
//!   `.attachments(..)` builders, the `released` reports and the handle-state
//!   table above, and the version 6 wire they drive is settled.
//! - **Nothing is promised** about a one-call helper that moves a whole blob
//!   to or from a file. None exists; the daemon drives the chunk loop.

pub mod blobs;
pub mod conn;
pub mod driver;
pub mod publish;
pub mod router;
pub mod store;
pub mod subs;
pub mod transport;

/// A monotonic timestamp in milliseconds, supplied by the driver on every input
/// to a sans-I/O layer above this crate.
///
/// wasm32 has no working `std::time::Instant`, so the driver reads the clock
/// (`performance.now()` on wasm, `tokio::time::Instant` natively — see
/// [`transport::clock`]) and the layers above only ever compare these values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Millis(pub u64);

impl Millis {
    /// This instant advanced by `ms` milliseconds, saturating at `u64::MAX`.
    ///
    /// Saturating rather than wrapping: a deadline computed past the end of the
    /// representable range must stay in the future, never wrap into the past.
    pub fn saturating_add_ms(self, ms: u64) -> Millis {
        Millis(self.0.saturating_add(ms))
    }
}

pub use transport::{TransportConnection, TransportConnector, TransportError, TransportEvent};

pub use brenn_attach_proto::{BodyError, Codec, MessageBody, MessageEnvelope, VersionRange};

#[cfg(not(target_arch = "wasm32"))]
pub use transport::native::{
    NativeConnection, NativeConnector, insert_bearer_token, insert_session_cookie,
};

// Signature types of the two header helpers, re-exported so out-of-tree
// attachers can name them without guessing this crate's tungstenite pin. Their
// doc comments state the semver coupling to that pin.
#[cfg(not(target_arch = "wasm32"))]
pub use tokio_tungstenite::tungstenite::http::{HeaderMap, header::InvalidHeaderValue};

#[cfg(target_arch = "wasm32")]
pub use transport::websys::{WebSysConnection, WebSysConnector};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturating_add_ms_advances() {
        assert_eq!(Millis(10).saturating_add_ms(5), Millis(15));
    }

    #[test]
    fn saturating_add_ms_saturates_rather_than_wrapping() {
        assert_eq!(Millis(u64::MAX - 1).saturating_add_ms(10), Millis(u64::MAX));
    }
}

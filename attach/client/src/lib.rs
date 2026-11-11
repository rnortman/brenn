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
//! outboxes, and the parked-set mirror), and [`router`] (the attacher's own
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
//! untyped text only; version 5 carries any body, bytes and content type. A
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
//! never succeed and nothing was stored). A batch is answered
//! `Ok`, `RateLimited` or `TextOnlyChannel`; the outboxes hand a refused flush
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
//! # What a consumer that binds this crate may rely on
//!
//! The crate is not published; it reaches a consumer by pin, and every
//! statement here is about source compatibility at a pin bump.
//!
//! - **Reading is additive.** A delivered [`MessageEnvelope`], a
//!   `DeferredViewEntry` and [`AttachmentFacts`](conn::AttachmentFacts) may
//!   gain fields; code that reads fields keeps compiling. A blob reference on
//!   a delivered envelope would arrive this way.
//! - **Building requests is additive through the constructors.** Build a
//!   [`PublishRequest`](publish::PublishRequest) with `PublishRequest::new`
//!   and `.attribution(..)`, and a
//!   [`BatchEntry`](brenn_attach_proto::BatchEntry) with `BatchEntry::new` and
//!   `.deliver_after(..)`. A struct literal compiles today and breaks when a
//!   field is added.
//! - **A new outcome or event variant is a compile break, deliberately.** The
//!   enums stay exhaustive: a refusal a daemon does not handle is a bug.
//! - **A later protocol version does not change these types' meaning.** The
//!   connection speaks only the range it is configured to offer; a consumer
//!   that stays on `exactly(5)` after a pin bump is delivered what it was
//!   before.
//! - **Nothing is promised** about an API that will request, send or receive
//!   blob bytes.

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

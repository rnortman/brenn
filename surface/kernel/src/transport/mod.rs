//! Transport abstraction: the boundary between the sans-I/O core/driver and
//! the concrete WebSocket implementations.
//!
//! The driver is generic over these traits, so no `dyn` is needed and the
//! futures returned by the async methods need no `Send` bound: native tests
//! spawn the driver on a current-thread runtime and wasm runs single-threaded.
//! That is why `async_fn_in_trait` is allowed here rather than reaching for a
//! boxed-future workaround.
//!
//! Only text/binary/close/failure reach the core. Server liveness pings/pongs
//! are handled entirely inside the transport (the browser WebSocket API cannot
//! observe them), so they never cross this boundary and both transports age
//! identically under the client liveness rule.

use std::fmt;

pub mod clock;
// `pub(crate)`: the seed source is deliberately non-cryptographic
// (`Math.random` / `RandomState` hash), so it must not be reachable by
// out-of-tree kernels that build against this crate — an inviting `entropy::seed`
// is exactly what a future author would grab for a nonce or token. The only
// caller is `handle::new`, in-crate.
pub(crate) mod entropy;
pub mod timer;

#[cfg(not(target_arch = "wasm32"))]
pub mod native;

#[cfg(target_arch = "wasm32")]
pub mod websys;

/// Opens transport connections. Authentication is the connector's business:
/// the native connector is constructed with the session cookie and injects it
/// into the handshake; the browser connector relies on the automatically
/// attached same-origin cookie and carries nothing.
#[allow(async_fn_in_trait)]
pub trait TransportConnector {
    type Conn: TransportConnection;

    /// Open a connection to `url`. A connect failure is a normal, retryable
    /// outcome (backoff), not a panic.
    async fn connect(&mut self, url: &str) -> Result<Self::Conn, TransportError>;
}

/// A live transport connection. Implementations never panic on peer behavior:
/// every peer-triggered outcome is a typed [`TransportEvent`].
#[allow(async_fn_in_trait)]
pub trait TransportConnection {
    /// Send a text frame. An error means the connection is gone; the driver
    /// treats it as a transport failure.
    async fn send_text(&mut self, text: String) -> Result<(), TransportError>;

    /// Await the next transport-level event. Resolves to [`TransportEvent`];
    /// never panics regardless of what the peer sends.
    async fn next_event(&mut self) -> TransportEvent;

    /// Initiate an orderly close. Best-effort; returns once the close has been
    /// requested.
    async fn close(&mut self);
}

/// A transport-level event surfaced to the core. Pings/pongs are deliberately
/// absent (see module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportEvent {
    /// A text frame. The core parses it as a server protocol frame.
    Text(String),
    /// A binary frame. The server never sends binary; the core treats this as
    /// a fatal protocol error.
    Binary(Vec<u8>),
    /// The peer closed the connection, with an optional WS close code and the
    /// close reason string.
    Closed { code: Option<u16>, reason: String },
    /// The connection failed at the transport level (I/O error, handshake
    /// abort, decode error). Carries a human-readable description.
    Failed(String),
}

/// A transport-level failure returned from [`TransportConnector::connect`] or
/// [`TransportConnection::send_text`]. Carries a human-readable description;
/// the core does not branch on its contents, only on success vs failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportError {
    description: String,
}

impl TransportError {
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
        }
    }

    pub fn description(&self) -> &str {
        &self.description
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.description)
    }
}

impl std::error::Error for TransportError {}

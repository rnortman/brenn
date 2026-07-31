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
//! connector injects a cookie header, the browser connector relies on the
//! same-origin cookie the browser attaches, and a future bearer-token connector
//! plugs into the same seam without either of them noticing.

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

#[cfg(not(target_arch = "wasm32"))]
pub use transport::native::{NativeConnection, NativeConnector, insert_session_cookie};

// Signature types of `insert_session_cookie`, re-exported so out-of-tree
// attachers can name them without guessing this crate's tungstenite pin. The
// helper's doc comment states the semver coupling to that pin.
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

//! The message-queue mechanics both hosts share.
//!
//! A non-durable channel is, mechanically, three things: a bounded drop-oldest
//! window of recent messages, one position per subscriber into that window, and
//! a set of messages parked until their release time arrives. None of the three
//! needs a database, a runtime, or a clock — so all three live here, in a crate
//! that compiles for the backend and for wasm32 alike, and each host wraps them
//! in whatever identity, locking, and wake machinery it has.
//!
//! How the three compose is itself channel semantics, not host machinery — an
//! append charges the cursors it outran, a release enters retention like any
//! arrival, an attach primes from the current tail — so the composition lives
//! here too, as [`RingCore`]. A host that re-composed the primitives itself
//! would be writing a second copy of the channel model.
//!
//! Time enters as a parameter: release times are epoch-millisecond `u64`s
//! supplied by the caller. Epoch identity is a generic parameter, since the only
//! thing done with it is comparison.

mod cursor;
mod deferred;
mod ring;
mod store;

pub use cursor::{Advance, SubscriberCursor, Window, new_boundary, retention_frontier};
pub use deferred::{Deferred, DeferredId, DeferredSet, NoSuchDeferred, QuotaExceeded, ReleaseTime};
pub use ring::{Append, GapReason, Replay, ReplayDecision, Resume, Retained, RetainedRing};
pub use store::{
    AppendReport, Attached, CursorOverflow, OwnedDeferred, Priming, ReleaseReport, RingCore,
};

#[cfg(test)]
mod invariant_tests;

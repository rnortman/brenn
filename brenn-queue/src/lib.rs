//! The message-queue mechanics both hosts share.
//!
//! A non-durable channel is, mechanically, three things: a bounded drop-oldest
//! window of recent messages, one position per subscriber into that window, and
//! a set of messages parked until their release time arrives. None of the three
//! needs a database, a runtime, or a clock — so all three live here, in a crate
//! that compiles for the backend and for wasm32 alike, and each host wraps them
//! in whatever identity, locking, and wake machinery it has.
//!
//! Time enters as a parameter: release times are epoch-millisecond `u64`s
//! supplied by the caller. Epoch identity is a generic parameter, since the only
//! thing done with it is comparison.

mod cursor;
mod deferred;
mod ring;

pub use cursor::{Advance, SubscriberCursor, Window, new_boundary, retention_frontier};
pub use deferred::{Deferred, DeferredId, DeferredSet, NoSuchDeferred, QuotaExceeded, ReleaseTime};
pub use ring::{Append, GapReason, Replay, ReplayDecision, Resume, Retained, RetainedRing};

#[cfg(test)]
mod invariant_tests;

//! The messaging persistence layer: the messaging tables, the per-channel
//! retention stores, and the ingress event shape they hold.
//!
//! What lives here is everything the `Messenger` engine keeps its state in,
//! and nothing of the engine itself:
//!
//! - [`db`] — the messaging tables, their DDL set, and the row-level
//!   operations over them. It owns `run_messaging_migrations` and composes the
//!   slice at or below this crate in `run_slice_migrations`.
//! - [`store`] — the retention stores a channel's messages live in between
//!   publish and delivery, database-backed or in-memory, behind one contract.
//! - [`ingress`] — the persistent shape of an ingress message and the
//!   repo-sync collapsing helpers over it.
//!
//! Nothing here names `Messenger`, so an edit to the engine leaves this
//! crate's tests cached.
//!
//! The addressing and config vocabulary all three speak is
//! `brenn_lib::messaging`, below.

pub mod db;
pub mod ingress;
pub mod store;

#[cfg(any(test, feature = "testutils"))]
pub mod testutils;

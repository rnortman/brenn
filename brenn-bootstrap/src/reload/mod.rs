//! Converging a running process to a document it did not boot from.
//!
//! Reload is decided in two levels, and this module is both of them plus what
//! the process reports afterwards:
//!
//! - [`compare`] is level 1, over the raw documents. Everything outside the
//!   three convergible blocks — `channels`, `links`, `wasm_consumers` — must be
//!   equal, and a difference anywhere else is a refusal naming the section.
//! - [`delta`] is level 2, over the two lowered plans. It says which channel
//!   entries and which consumers moved, and refuses the moves that cannot be
//!   made without restarting.
//! - [`driver`] is those two asked about *this* process: it holds the baseline
//!   the running system is the projection of, re-reads the tree on disk, and
//!   turns the answers into an outcome.
//! - [`commit`] is what applies an outcome that may be applied: the five
//!   ordered steps that walk the running directory, registrations, bindings and
//!   tasks to the plan the driver prepared.
//! - [`doors`] is how a reload is asked for: the request channel's own drain
//!   loop and `SIGUSR1`, both feeding one serialized driver task.
//! - [`status`] projects a level-2 delta into the outcome document's shape. The
//!   document itself is `brenn_messaging::config_reload`, beside the address it
//!   is published to.
//!
//! Both comparisons are computations: they read no host state, mutate nothing,
//! and answer the same way however many times they are asked. Everything
//! fallible happens before [`commit`] runs, which is what lets commit panic on
//! anything that does not go through.
//!
//! The correctness rule everything here serves: after a successful reload the
//! process is in the state a fresh boot of the new document would have
//! produced. A change that cannot be brought to that state is refused rather
//! than approximated.
//!
//! Refusals come in two grammars, and they mean opposite things. A refusal from
//! either level ends in [`NEEDS_RESTART`]: the document is good and the process
//! cannot walk to it, so bouncing the service applies it. A refusal carrying a
//! compile diagnostic or one of boot's own environment asserts means the
//! document or the host is wrong, and a restart makes it worse — that text ends
//! the way boot ends it. Both land in the same list of lines.

pub(crate) mod commit;
pub(crate) mod compare;
pub(crate) mod delta;
pub(crate) mod doors;
pub(crate) mod driver;
pub(crate) mod status;

/// The correctness rule above, checked against a running process, and the
/// cases that need one: they live beside the driver's own tests rather than
/// inside them because their subject is the process, not the verdict.
#[cfg(test)]
mod oracle_tests;

/// The tail every level-1 and level-2 refusal ends with: what the operator does
/// about it.
///
/// One string rather than a per-site phrasing, because the operator's whole
/// decision procedure is "did this reload apply, and if not do I restart" — and
/// the answer to the second half is always yes for a refusal at either level.
/// It is deliberately *not* appended to a compile diagnostic or an environment
/// assert, where restarting is the wrong move.
pub(crate) const NEEDS_RESTART: &str = "this change needs a restart";

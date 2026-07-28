//! Activation delivery: the per-instance scheduler state.
//!
//! This is the backend's delivery model rebuilt page-side. The kernel is the
//! wasmtime-equivalent host: it batches deliveries into activations, windows
//! every bound input port (retained context ++ new, split by `new_from`),
//! advances each window's cursor at activation start, and serializes invocations
//! per instance. It mirrors `brenn-server`'s `drain_step`, because a component's
//! delivery semantics must not change with its hosting.
//!
//! What a binding is owed is **not** here. It lives in its channel's store
//! ([`super::store::SurfaceChannelStore`]) as a cursor — a position, not a queue
//! of copies. The messages exist once, retained by the channel, which is what
//! makes a loss *this binding's* accountable drop and what keeps a message the
//! window could not present as new still readable as context: no gap event, no
//! replay choreography, and attach is a delivery point on every class.
//!
//! What is left here is the scheduler: whether an instance is running, whether
//! it is terminal, its sink carryover, and its wire outbox. None of that is
//! retention.

use std::collections::{HashMap, VecDeque};

/// One activation-registered instance's scheduler state.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RegisteredInstance {
    /// The wire channels this instance holds a subscription reference on, one
    /// entry **per input binding** — so two ports of one instance on one channel
    /// hold two references on the one subscription they share, exactly as two
    /// attached ports would.
    ///
    /// A registered instance is a subscriber like any other: nothing else opens
    /// its subscriptions, since it has no attached ports to do it. Holding the
    /// references here (against the same refcounts the attach path uses) is what
    /// makes the existing reconnect machinery — `resubscribe_survivors`, the
    /// resume tokens, the deferred-`Unsubscribe` edge — apply to it unchanged.
    ///
    /// Depth-0 bindings are included: a depth-0 port still sees its channel, and
    /// on a wire channel seeing it means subscribing to it. That is the stated
    /// per-hosting cost of depth 0 on a surface — it buys the store's diet at
    /// the price of the channel's full publish volume over the link.
    ///
    /// Confined channels are absent: they have no subscription, no refcount, and
    /// no resume token, because no server is in the loop.
    pub subs: Vec<String>,
    /// Whether an activation is in flight. Invocations are serialized per
    /// instance: anything arriving during a handler coalesces into the next
    /// activation rather than overlapping this one.
    pub in_flight: bool,
    /// Whether the instance is terminal (a trap, or a `fatal`-rung overflow).
    /// Delivery stops and its cursors are detached; the stores themselves are
    /// not touched — they are the channel's, and a failed instance never
    /// activates, so what it leaves behind is inert rather than corrupt.
    pub failed: bool,
    /// Activations whose entry returned err, lifetime. An err is a failed
    /// activation, not a death.
    pub activation_failures: u64,
    /// Per-output-port millitokens carried between activations. Clamped to the
    /// port's `capacity_mt` when the next activation is seeded — the clamp is
    /// the *seeding* host's job, since only it knows an activation is starting.
    pub carry_mt: HashMap<String, u64>,
    /// The instance's ordered wire outbox: flushes waiting to go out, oldest
    /// first, bounded by the instance's `parked_batch_depth`.
    ///
    /// A flush enters here whenever it cannot go straight out — the link is
    /// down, an earlier flush is still unanswered, or the server refused the
    /// head and it is waiting for a retry. One queue, because order among an
    /// instance's own flushes is total: a newer batch that overtook a waiting
    /// one would reorder publishes the component already had ok'd.
    pub parked: VecDeque<ParkedBatch>,
    /// The correlation of this instance's unanswered `PublishBatch`, if any.
    ///
    /// At most one flush per instance is on the wire at a time. That is what
    /// makes the outbox ordered under refusal: a second flush sent while the
    /// first is unanswered would already be applied when the first came back
    /// `RateLimited` and re-parked for retry.
    pub batch_in_flight: Option<u64>,
    /// Whole parked batches dropped at the cap, lifetime.
    pub parked_dropped: u64,
    /// Flushes the server's send-budget backstop refused, lifetime. Non-zero
    /// means the kernel-side budget and the server's disagree — the kernel is the
    /// binding constraint for any non-malicious surface, so this counter is the
    /// evidence that something is wrong rather than an expected cost.
    pub rate_limited_batches: u64,
    /// Lifetime drops observed on each input binding whose resolved noise is
    /// `Metered` or louder, keyed by port. The `metered` rung of the loudness
    /// ladder: every drop charged against the binding — an eviction that outran
    /// its cursor, a still-retained span its advance passed unserved, or a
    /// server-reported loss upstream of the page — is counted here. `Silent`
    /// bindings never appear. This is kernel-internal observability, distinct
    /// from `InstanceCounters.drops` (which counts the legacy dialect path,
    /// rung-blind); the two never feed each other.
    pub metered_drops: HashMap<String, u64>,
    /// Deferred publishes whose schedule was dropped because the channel's
    /// deferred set was full, lifetime.
    ///
    /// A refusal to schedule, never a lost message body: the flush had no error
    /// channel back to the component, so this counter is the only account of a
    /// timer the component believes it set. Non-zero means a component schedules
    /// more parked future than its channel's depth allows.
    pub deferred_dropped: u64,
    /// Control ops (cancel / edit) that found their message already released,
    /// lifetime.
    ///
    /// The benign drain-vs-release race, which a conforming component can always
    /// lose: it read a schedule, the release time arrived, and its op reached a
    /// message that is now an ordinary published one. Counted rather than reported
    /// for the same reason `deferred_dropped` is — the component had already
    /// returned — and worth counting because a component whose ops *always* race is
    /// scheduling too close to its own activation rate.
    pub deferred_races: u64,
}

impl RegisteredInstance {
    pub(crate) fn new() -> Self {
        Self {
            subs: Vec::new(),
            in_flight: false,
            failed: false,
            activation_failures: 0,
            carry_mt: HashMap::new(),
            parked: VecDeque::new(),
            batch_in_flight: None,
            parked_dropped: 0,
            rate_limited_batches: 0,
            metered_drops: HashMap::new(),
            deferred_dropped: 0,
            deferred_races: 0,
        }
    }

    /// Whether an activation may be dispatched at all — the half of readiness
    /// that is about the instance rather than about what its bindings are owed.
    /// The owed half is a question for the channels' stores, which the core
    /// holds.
    pub(crate) fn runnable(&self) -> bool {
        !self.in_flight && !self.failed
    }
}

/// One activation's wire-bound flush, held in the instance's outbox.
///
/// Parked whole or not at all: the batch is the atom the server applies in one
/// transaction, so a partial send would break the guarantee it exists to carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParkedBatch {
    /// The durable + ephemeral entries of one flush, in call order — already in
    /// the shape the frame carries, so nothing reinterprets them on the way out.
    pub entries: Vec<brenn_surface_proto::BatchEntry>,
    /// The flush's control ops against transportable channels, in call order and
    /// already in the frame's shape. Held with the entries because the server
    /// applies both halves of one batch together, ops first.
    pub ops: Vec<brenn_surface_proto::BatchDeferredOp>,
}

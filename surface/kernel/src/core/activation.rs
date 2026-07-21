//! Activation delivery: the retained rings, the pending queues, and the
//! per-instance scheduler state.
//!
//! This is the backend's delivery model rebuilt page-side. The kernel is the
//! wasmtime-equivalent host: it batches deliveries into activations, windows
//! every bound input port (retained context ++ new, split by `new_from`), acks at
//! activation start, and serializes invocations per instance. The structures
//! below mirror `brenn-server`'s `drain_step` exactly, because a component's
//! delivery semantics must not change with its hosting.
//!
//! Two structures do two jobs that a single queue cannot:
//!
//! - The **pending queue** ([`PendingQueue`]) is the port's *delivery
//!   obligation*: new envelopes, bounded by `push_depth`, drop-oldest and counted
//!   on overflow. It answers "why did this component wake, and what is new?"
//! - The **retained ring** ([`RetainedRing`]) is the subscription's *view*: the
//!   channel's recent messages, bounded by `retain_depth`, fed by every delivery
//!   **before and independently of** any pending queue.
//!
//! That independence is the whole recovery model. A message evicted from a
//! pending queue is still in the ring, so it is still visible as retained context
//! in the same or any later activation retention covers — no gap event, no
//! replay choreography. Overflow retires the delivery obligation; it never
//! retires the message body.

use std::collections::{HashMap, VecDeque};

use brenn_envelope::MessageEnvelope;
use brenn_surface_proto::LocalPos;
use uuid::Uuid;

/// One subscription's retained ring: the most recent messages on the channel,
/// bounded by depth.
///
/// Shared by the `local:` router (whose per-channel ring is both its retention
/// and its context source) and by wire subscriptions, which is the point — the
/// two classes retain by one rule, so a binding's context depth means the same
/// thing whichever side of the wire its channel lives on.
///
/// Page-lifetime: reconciled at `Welcome` but never cleared by a reconnect. Only
/// a page reload discards a ring, and that mints a new `local_epoch` too. A
/// reconnect-discard would manufacture a loss the link never caused.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RetainedRing {
    /// Retained depth. `0` retains nothing — a legal, meaningful value on any
    /// binding, and the contract-fixed depth of the toast plane, where replaying
    /// a stale event to a late subscriber is wrong.
    depth: u64,
    /// The retained window, oldest first, bounded by `depth`.
    entries: VecDeque<(MessageEnvelope, LocalPos)>,
    /// The next page-local seq to assign, dense and ascending per channel. Never
    /// reset while the ring lives: the ring is page-lifetime, so a seq is unique
    /// for the page's life. This is the sole authority for a channel's [`LocalPos`]
    /// seq — the `local:` router draws from it for the channels it owns, and a
    /// wire subscription draws from it for the page-local position it stamps on
    /// ring entries and port events (never the server's span seq, which restarts
    /// at each `SubscribeResult` and would collide within one page epoch).
    seq_next: u64,
}

impl RetainedRing {
    pub(crate) fn new(depth: u64) -> Self {
        Self {
            depth,
            entries: VecDeque::new(),
            seq_next: 0,
        }
    }

    /// Draw the next page-local seq for this channel.
    pub(crate) fn next_seq(&mut self) -> u64 {
        let seq = self.seq_next;
        self.seq_next += 1;
        seq
    }

    /// The resolved depth. Test-only: nothing in the kernel asks a ring how deep
    /// it is — the ring enforces its own bound and every reader states the depth
    /// it wants. The fold that computes it is worth asserting directly, though,
    /// rather than only inferring it from what the ring happens to hold.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn depth(&self) -> u64 {
        self.depth
    }

    /// Retune the ring to a new depth, trimming in place if it shrank. The
    /// `Welcome` reconcile path: an operator changed a `retain_depth` and the
    /// ring's existing contents are still the honest recent history.
    pub(crate) fn set_depth(&mut self, depth: u64) {
        self.depth = depth;
        self.trim();
    }

    /// Feed one delivered envelope.
    ///
    /// **Idempotent by `message_id`:** an envelope already in the ring is not
    /// appended again. This is load-bearing, not hygiene. Rings survive reconnect
    /// while several reconnect paths legitimately re-deliver envelopes the ring
    /// already holds — a durable fresh-attach full-ring replay, a fresh replay
    /// after a gap past the ring, an ephemeral epoch-change replay. Without the
    /// check, one window's context could carry the same message twice, a shape
    /// the backend's distinct-row context read can never produce, so the two
    /// hostings would disagree about what "seen" means.
    ///
    /// The membership scan is over at most `depth` entries, which is a config
    /// bound on page memory — a handful, not a data structure problem.
    ///
    /// **Owns the [`LocalPos`]:** a redelivered envelope keeps the pos its ring
    /// entry already carries, and a fresh seq is drawn only on an actual insert.
    /// The returned pos is the one the caller fans out to ports, so one envelope
    /// carries a single stable pos across every redelivery — a list-style port
    /// can suppress a re-replayed window by pos equality, and the seq stays dense
    /// because a deduped push burns none.
    pub(crate) fn push(&mut self, envelope: &MessageEnvelope, epoch: Uuid) -> LocalPos {
        if let Some((_, pos)) = self
            .entries
            .iter()
            .find(|(e, _)| e.message_id == envelope.message_id)
        {
            return *pos;
        }
        let pos = LocalPos {
            epoch,
            seq: self.next_seq(),
        };
        if self.depth == 0 {
            return pos;
        }
        self.entries.push_back((envelope.clone(), pos));
        self.trim();
        pos
    }

    /// The most recent `depth` entries, oldest first — one binding's context
    /// window, read at *that binding's own* depth out of a ring folded to the max
    /// over the instance's bindings on the channel.
    pub(crate) fn recent(&self, depth: u64) -> impl Iterator<Item = &(MessageEnvelope, LocalPos)> {
        let take = usize::try_from(depth)
            .unwrap_or(usize::MAX)
            .min(self.entries.len());
        self.entries.iter().skip(self.entries.len() - take)
    }

    /// Every retained entry, oldest first — the whole ring.
    ///
    /// Test-only: window assembly reads `recent` at a binding's own depth, so
    /// nothing in the delivery path wants the undepthed view. Tests assert what
    /// the ring actually holds, which is the one question `recent` cannot answer
    /// without being told a depth to trust.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn entries(&self) -> impl Iterator<Item = &(MessageEnvelope, LocalPos)> {
        self.entries.iter()
    }

    /// Drop the oldest entries until the ring is within its depth. Depth is a
    /// `u64` from config while `len` is a `usize`; comparing in `u64` keeps the
    /// check exact on any target (wasm32 is 32-bit, where a `usize` cast of a
    /// large depth would wrap).
    fn trim(&mut self) {
        while self.entries.len() as u64 > self.depth {
            self.entries.pop_front();
        }
    }
}

/// One `local:` channel's page-local router state.
///
/// The ring is the same [`RetainedRing`] a wire subscription keeps — `local:` is
/// a delivery class, not a different retention model. The ring owns the dense seq
/// counter the router assigns from ([`RetainedRing::next_seq`]); page-local
/// traffic has no server to assign positions, so the router is the sole
/// authority, and there is no wire state here at all (no subscription, no
/// refcount, no resume token).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LocalRing {
    pub ring: RetainedRing,
}

impl LocalRing {
    pub(crate) fn new(depth: u64) -> Self {
        Self {
            ring: RetainedRing::new(depth),
        }
    }
}

/// One binding's pending queue: the new envelopes its port has not yet been
/// activated for, plus the drop bookkeeping the window's `dropped` delta comes
/// from.
///
/// A drop is recovered by retention rather than announced: overflow bounds the
/// queue and bumps the drop counter, but the lost body stays readable as
/// context wherever the retained ring still covers it, so there is nothing to
/// coalesce or terminate in the stream.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PendingQueue {
    /// New envelopes awaiting an activation, oldest first, bounded by
    /// `push_depth`.
    queue: VecDeque<MessageEnvelope>,
    /// This binding's `push_depth`. Proven `>= 1` and `usize`-representable by
    /// `on_welcome`; a depth-0 binding has no pending queue at all.
    capacity: usize,
    /// Monotone lifetime drop count for this binding: page-side evictions plus
    /// its share of every server-reported subscription drop. Never reset — the
    /// window reports a delta against it.
    count: u64,
    /// The `count` as of the last ack. The window's `dropped` is
    /// `count - last_seen_at_ack`, advanced at ack time, exactly as `drain_step`
    /// advances the backend's.
    last_seen_at_ack: u64,
}

impl PendingQueue {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            capacity,
            count: 0,
            last_seen_at_ack: 0,
        }
    }

    pub(crate) fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        self.trim();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Push one new envelope, evicting the oldest and counting the drop on
    /// overflow.
    ///
    /// Drop-oldest is the bus's model on every class, durable included: the
    /// freshest messages are the ones a component woken late can still act on.
    /// The evicted body is not lost — the subscription's ring still holds it
    /// wherever `retain_depth` covers it, so it reappears as context.
    pub(crate) fn push(&mut self, envelope: MessageEnvelope) {
        self.queue.push_back(envelope);
        self.trim();
    }

    /// Count `n` messages the server reported dropped on this binding's
    /// subscription before they ever reached the page. Every binding on the
    /// subscription takes the full count: each of them missed those messages.
    pub(crate) fn count_server_drops(&mut self, n: u64) {
        self.count = self.count.saturating_add(n);
    }

    /// Drain the queue for an activation and snapshot the drop delta — the ack.
    ///
    /// Ack-at-activation-**start**, backend parity: the messages are consumed the
    /// moment the activation is assembled, before the entry runs. An err or a
    /// trap therefore consumes them too, and they reappear only as retained
    /// context. That is the doctrine, not an accident: a redelivery-on-failure
    /// rule would be a retry loop with no bound and no consent.
    pub(crate) fn ack(&mut self) -> (Vec<MessageEnvelope>, u64) {
        let dropped = self.count - self.last_seen_at_ack;
        self.last_seen_at_ack = self.count;
        (self.queue.drain(..).collect(), dropped)
    }

    fn trim(&mut self) {
        while self.queue.len() > self.capacity {
            self.queue.pop_front();
            self.count = self.count.saturating_add(1);
        }
    }
}

/// One activation-registered instance's scheduler state.
///
/// An instance is registered or legacy-attached, never both (the core panics on
/// either crossing). While the two delivery models coexist this separation is
/// what keeps them from racing on one binding; Phase 2 deletes the other one and
/// the question stops existing.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RegisteredInstance {
    /// One pending queue per bound input port with `push_depth >= 1`, keyed by
    /// port name. A depth-0 binding has no entry: it never activates the
    /// instance and never delivers new envelopes — it windows as pure context
    /// when a sibling port does the waking.
    pub queues: HashMap<String, PendingQueue>,
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
    /// per-hosting cost of depth 0 on a surface — it buys the ring's diet at the
    /// price of the channel's full publish volume over the link.
    ///
    /// `local:` channels are absent: they have no subscription, no refcount, and
    /// no resume token, because no server is in the loop.
    pub subs: Vec<String>,
    /// Whether an activation is in flight. Invocations are serialized per
    /// instance: anything arriving during a handler coalesces into the next
    /// activation rather than overlapping this one.
    pub in_flight: bool,
    /// Whether the instance is terminal (a trap). Delivery stops; its pending
    /// queues are dropped. Its rings are **not** — they are per-subscription and
    /// page-lifetime, and a failed instance never activates, so they are inert
    /// and harmless rather than something to clean up.
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
    /// ladder: every drop the binding's window reports at activation assembly —
    /// from either origin (a kernel-side pending-queue overflow or a
    /// server-reported delta, both folded into the one ack delta) — is counted
    /// here. `Silent` bindings never appear. This is kernel-internal
    /// observability, distinct from `InstanceCounters.drops` (which counts the
    /// legacy dialect path, rung-blind); the two never feed each other.
    pub metered_drops: HashMap<String, u64>,
}

impl RegisteredInstance {
    pub(crate) fn new() -> Self {
        Self {
            queues: HashMap::new(),
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
        }
    }

    /// Whether any pending queue holds a new envelope.
    pub(crate) fn pending(&self) -> bool {
        self.queues.values().any(|q| !q.is_empty())
    }

    /// Whether an activation may be dispatched right now.
    pub(crate) fn ready(&self) -> bool {
        self.pending() && !self.in_flight && !self.failed
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
}

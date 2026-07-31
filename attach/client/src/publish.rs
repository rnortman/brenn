//! The publish plane of an attachment: what this attacher has sent that the peer
//! has not answered yet, what it could not send yet, and what the peer says it
//! has parked.
//!
//! Sans-I/O and channel-addressed. Three independent pieces, because they have
//! three different lifetimes and the embedder holds them separately:
//!
//! - [`PendingPublishes`] — the correlation table of single publishes. One
//!   connection wide: a correlation names a frame on *this* attachment, so a
//!   detach fails every outstanding one rather than carrying it across.
//! - [`Outboxes`] — the per-registrant atomic-flush queues. Longer-lived than any
//!   connection: a flush that the activation model already ok'd is owed the wire,
//!   so it waits in its registrant's outbox across a detach, a refusal, and a
//!   reconnect, up to a stated bound.
//! - [`DeferredViews`] — the mirror of what the peer says each sub-identity has
//!   parked. Pushed by the authority, cleared at every attach.
//!
//! A *registrant* is whatever the embedder flushes activations for — a component
//! instance in the browser surface, whatever a native attacher schedules work
//! for. This layer knows only that it has an opaque key, a sub-identity to
//! publish under, and an outbox depth, all stated at registration.
//!
//! What is deliberately not here: the buffering an activation does before it
//! flushes (the embedder's budget and call-order machinery), which channel a
//! flush's entries were allowed to name, and what an outcome *means* to the
//! caller that asked for it. This layer takes a composed flush and answers where
//! it went.

use std::collections::{BTreeMap, HashMap, VecDeque};

use brenn_attach_proto::{
    BatchDeferredOp, BatchEntry, ClientFrame, DeferredViewEntry, PublishBatchOutcome,
};
use brenn_envelope::Urgency;

use crate::Millis;

/// How long the attacher waits before re-offering a refused outbox head.
///
/// A constant, not config. The peer's backstop refill is what decides when the
/// head is admitted; this only decides how promptly the attacher notices, and a
/// 1s probe against a refill measured in seconds is idle-cheap — the timer is
/// disarmed whenever no outbox is blocked. A knob here would be a number nobody
/// can state a requirement for.
pub const RETRY_INTERVAL_MS: u64 = 1_000;

/// One immediate publish, as the attacher hands it over.
///
/// A struct rather than a parameter list: `channel` and `body` are both `String`
/// and a transposition would typecheck and publish the address into the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRequest {
    pub channel: String,
    /// The sub-identity sending, or `None` for the attacher itself. Opaque here:
    /// the peer validates it against the set its own configuration declares.
    pub attribution: Option<String>,
    pub body: String,
    /// Concrete on the wire, so the attacher resolves any per-channel default it
    /// configures before it gets here — the same value it would stamp onto an
    /// envelope it routes itself.
    pub urgency: Urgency,
}

/// Compose one [`ClientFrame::Publish`]. `correlation` of `None` asks for no
/// answer, which is the fire-and-forget shape.
pub fn publish_frame(req: PublishRequest, correlation: Option<u64>) -> ClientFrame {
    ClientFrame::Publish {
        channel: req.channel,
        attribution: req.attribution,
        body: req.body,
        urgency: req.urgency,
        correlation,
    }
}

/// Single publishes sent on this attachment and awaiting their
/// [`ServerFrame::PublishResult`], keyed by correlation and valued by whatever
/// the embedder needs to route the answer back.
///
/// Correlations are the embedder's to assign — it is the layer with callers to
/// answer — and must be unique per attachment. A collision is a bug in that
/// layer, not a race, so it panics rather than silently overwriting a routing
/// entry and misrouting two answers.
///
/// [`ServerFrame::PublishResult`]: brenn_attach_proto::ServerFrame::PublishResult
pub struct PendingPublishes<T> {
    pending: HashMap<u64, T>,
}

impl<T> Default for PendingPublishes<T> {
    fn default() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }
}

impl<T> PendingPublishes<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `tag` against `correlation` and compose the frame that asks for an
    /// answer.
    pub fn send(&mut self, correlation: u64, tag: T, req: PublishRequest) -> ClientFrame {
        let prev = self.pending.insert(correlation, tag);
        assert!(
            prev.is_none(),
            "attach client: duplicate pending publish correlation {correlation}"
        );
        publish_frame(req, Some(correlation))
    }

    /// Settle one `PublishResult`, answering the tag its publish was sent with.
    ///
    /// A result with no correlation, or one for a correlation this attachment
    /// never sent or already settled, is unreconcilable — the correlation space
    /// is the attacher's own — so it is an error the embedder turns into a fatal
    /// on its connection rather than a tolerated echo.
    ///
    /// The outcome itself is not interpreted here: whether a refusal is surfaced
    /// to a caller or swallowed (an error report whose own failure must not
    /// produce another error report) is the embedder's rule, and the tag is what
    /// it decides on.
    pub fn on_result(&mut self, correlation: Option<u64>) -> Result<T, String> {
        let Some(correlation) = correlation else {
            return Err("PublishResult with no correlation".to_string());
        };
        self.pending
            .remove(&correlation)
            .ok_or_else(|| format!("PublishResult with unknown correlation: {correlation}"))
    }

    /// Drain every outstanding publish — the transport went away, so no answer is
    /// coming for any of them. Ordered by correlation, so the embedder's failure
    /// events are deterministic.
    pub fn fail_all(&mut self) -> Vec<(u64, T)> {
        let mut pending: Vec<(u64, T)> = std::mem::take(&mut self.pending).into_iter().collect();
        pending.sort_by_key(|(correlation, _)| *correlation);
        pending
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// One activation's whole flush: the two lists a [`ClientFrame::PublishBatch`]
/// carries, in call order and already in the frame's shape.
///
/// Held verbatim while it waits, so a refused batch is re-offered rather than
/// reconstructed. It travels whole or not at all — the batch is the atom the
/// peer applies in one transaction, so half of one is not a smaller version of
/// it but a different, wrong thing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushBatch {
    pub entries: Vec<BatchEntry>,
    /// Control ops against messages this sender already parked. Applied by the
    /// peer *before* the entries, which is why they travel with them.
    pub ops: Vec<BatchDeferredOp>,
}

impl FlushBatch {
    /// A flush carrying only ops is still a flush, so a batch is empty only when
    /// both lists are.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.ops.is_empty()
    }
}

/// What to do with the retry timer. Absent — `None` in
/// [`OutboxSteps::retry_wakeup`] — leaves it exactly as it is.
///
/// Leaving it alone is a distinct answer from arming it. Re-arming an
/// already-armed timer on every input would let unrelated traffic push a blocked
/// head's deadline out indefinitely, starving the retry it exists for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerChange {
    /// Fire once at this deadline.
    Arm(Millis),
    /// Cancel the armed deadline; nothing is waiting on it.
    Disarm,
}

/// What an outbox operation produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxSteps<K> {
    /// Frames the embedder must send, in order.
    pub frames: Vec<ClientFrame>,
    /// One entry per whole flush dropped, naming the registrant that lost it, in
    /// the order they were dropped. A drop is never silent: the embedder counts
    /// it, announces it, or both.
    pub dropped: Vec<K>,
    /// The retry timer's instruction, when it changed.
    pub retry_wakeup: Option<TimerChange>,
}

impl<K> Default for OutboxSteps<K> {
    fn default() -> Self {
        Self {
            frames: Vec::new(),
            dropped: Vec::new(),
            retry_wakeup: None,
        }
    }
}

/// What one `PublishBatchResult` settled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchAnswer<K> {
    pub steps: OutboxSteps<K>,
    /// Whose flush it was.
    pub registrant: K,
    /// The flush the peer refused after the registrant that composed it was
    /// gone — deregistered, or deregistered and registered again under the same
    /// key: ok'd entries that were never applied and have no outbox left to wait
    /// in. `None` in every other case, including an ordinary refusal (which is
    /// re-parked) and any answer for the registration that sent it.
    pub lost: Option<FlushBatch>,
}

/// One registrant's outbox.
struct Outbox {
    /// Which registration under this key opened it. A key deregistered and
    /// registered again names a *different* registrant with the same spelling —
    /// its own attribution, its own queue — so an answer minted for the previous
    /// one must not be applied here.
    generation: u64,
    /// The sub-identity its flushes publish under, stated at registration and
    /// stamped onto every frame this outbox emits. The attacher never spells an
    /// identity — this is a string the peer's configuration already names.
    attribution: Option<String>,
    /// The cap on `parked`, in whole flushes.
    depth: u64,
    /// Flushes waiting to go out, oldest first. One queue, because order among a
    /// registrant's own flushes is total: a newer batch overtaking a waiting one
    /// would reorder publishes the embedder already ok'd.
    parked: VecDeque<FlushBatch>,
    /// The correlation of this registrant's unanswered `PublishBatch`, if any.
    ///
    /// At most one flush per registrant is on the wire at a time. That is what
    /// makes the outbox ordered under refusal: a second flush sent while the
    /// first was unanswered would already be applied when the first came back
    /// refused and went to the head of the queue for retry.
    in_flight: Option<u64>,
    /// Whole flushes dropped, lifetime — at the cap or on a re-validation the
    /// embedder refused.
    dropped: u64,
    /// Flushes the peer's budget backstop refused, lifetime. Non-zero means the
    /// attacher's own limiter and the peer's disagree; the attacher is the
    /// primary limiter, so this is evidence of a problem rather than an expected
    /// cost.
    rate_limited: u64,
}

/// A `PublishBatch` on the wire, awaiting its answer.
struct PendingBatch<K> {
    /// Whose flush it is.
    key: K,
    /// Which registration under that key composed it. Checked against the
    /// outbox's own generation when the answer lands: the key alone would name
    /// a successor registration that never sent this batch.
    generation: u64,
    /// The frame's payload, kept so a refusal can re-park the flush verbatim
    /// rather than reconstruct it.
    batch: FlushBatch,
}

/// The attachment's atomic-flush outboxes, one per registrant.
///
/// The embedder drives it: [`register`](Outboxes::register) /
/// [`deregister`](Outboxes::deregister) as its registrants come and go,
/// [`flush`](Outboxes::flush) as their activations complete,
/// [`on_attached`](Outboxes::on_attached) / [`on_detached`](Outboxes::on_detached)
/// as the connection under it comes and goes, and
/// [`on_batch_result`](Outboxes::on_batch_result) /
/// [`on_retry_tick`](Outboxes::on_retry_tick) as the peer and the clock answer.
///
/// Queueing is not an error path. The activation already returned ok, so the
/// guarantee is "flushed, not discarded" — up to a stated bound. Registrants run
/// while detached (page-local delivery and timers need no wire), so the outbox is
/// a queue like every other and takes the same overflow model: bounded per
/// registrant, drop-oldest at the cap, counted.
pub struct Outboxes<K: Ord> {
    /// Ordered, so every sweep over the registrants emits frames in one
    /// deterministic order and no registrant can starve a sibling.
    registrants: BTreeMap<K, Outbox>,
    /// Sent batches awaiting their `PublishBatchResult`, keyed by correlation.
    ///
    /// A separate correlation space from [`PendingPublishes`]: they are different
    /// frames with different answers, and these correlations are the attacher's
    /// own — a batch is only ever produced by a flush, never by a caller.
    pending: HashMap<u64, PendingBatch<K>>,
    next_correlation: u64,
    /// Stamped onto each outbox at registration, so two registrations under one
    /// key are distinguishable for as long as the attachment lives.
    next_generation: u64,
    /// Whether an attachment is live. Off it there is no wire to carry a flush,
    /// so everything queues.
    live: bool,
    retry_armed: bool,
}

impl<K: Clone + Ord> Default for Outboxes<K> {
    fn default() -> Self {
        Self {
            registrants: BTreeMap::new(),
            pending: HashMap::new(),
            next_correlation: 0,
            next_generation: 0,
            live: false,
            retry_armed: false,
        }
    }
}

impl<K: Clone + Ord> Outboxes<K> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open an outbox for `key`, publishing under `attribution` and holding at
    /// most `depth` whole flushes.
    ///
    /// Registering a key twice, or at depth zero, is an embedder bug: the second
    /// registration would silently discard the first's queue, and an outbox that
    /// can hold nothing drops every flush it is ever handed while reporting each
    /// one as an overflow.
    pub fn register(&mut self, key: K, attribution: Option<String>, depth: u64) {
        assert!(
            depth > 0,
            "attach client: an outbox depth of zero holds no flush"
        );
        let generation = self.next_generation;
        self.next_generation += 1;
        let prev = self.registrants.insert(
            key,
            Outbox {
                generation,
                attribution,
                depth,
                parked: VecDeque::new(),
                in_flight: None,
                dropped: 0,
                rate_limited: 0,
            },
        );
        assert!(
            prev.is_none(),
            "attach client: registrant already has an outbox"
        );
    }

    /// Close `key`'s outbox, answering what died with it: flushes the embedder
    /// ok'd and nobody is left to send.
    ///
    /// Deregistering a key that holds no outbox is an embedder bug and panics,
    /// like every other bookkeeping mismatch here: the real registrant is still
    /// queueing under the key that was meant, and an empty answer reads as
    /// "nothing was owed" instead of naming the mistake where it was made.
    ///
    /// The correlation of an in-flight batch is deliberately *not* forgotten —
    /// the peer will still answer it, and that answer must be reconcilable. It
    /// comes back as a [`BatchAnswer`] with no live registrant.
    pub fn deregister(&mut self, key: &K) -> Vec<FlushBatch> {
        let outbox = self
            .registrants
            .remove(key)
            .expect("attach client: deregister of an unregistered registrant");
        outbox.parked.into_iter().collect()
    }

    pub fn is_registered(&self, key: &K) -> bool {
        self.registrants.contains_key(key)
    }

    /// Offer one completed activation's flush to `key`'s outbox.
    ///
    /// Sent straight out only when the wire is free for this registrant: the
    /// attachment is live, nothing of its own is queued, and none of its own
    /// flushes is unanswered. Otherwise it queues, and the outbox drains in
    /// order.
    pub fn flush(&mut self, key: &K, batch: FlushBatch, now: Millis) -> OutboxSteps<K> {
        assert!(
            !batch.is_empty(),
            "attach client: a flush with neither entries nor ops is not a batch"
        );
        let mut steps = if self.wire_free_for(key) {
            OutboxSteps {
                frames: vec![self.batch_frame(key, batch)],
                ..OutboxSteps::default()
            }
        } else {
            self.park(key, batch, false)
        };
        steps.retry_wakeup = self.retry_wakeup(now);
        steps
    }

    /// The attachment came up: re-validate every queued flush, then start the
    /// outboxes draining, oldest first.
    ///
    /// A queued flush was composed under the *previous* connection's contract,
    /// and a reconnect can hand the attacher a different one. `survives` is the
    /// embedder's re-check against the new one — every gate the peer answers with
    /// a protocol violation rather than an outcome belongs in it, because taking
    /// a protocol death for honestly replaying what was buffered under the old
    /// contract is not a trade worth making. A flush that fails it is dropped
    /// whole, counted like a cap drop.
    ///
    /// Only the surviving head of each outbox goes out here: the queue is ordered
    /// and carries at most one flush on the wire, so the rest leave as each
    /// result comes back.
    pub fn on_attached(
        &mut self,
        now: Millis,
        survives: impl Fn(&K, &FlushBatch) -> bool,
    ) -> OutboxSteps<K> {
        self.live = true;
        let mut steps = OutboxSteps::default();
        let keys: Vec<K> = self.registrants.keys().cloned().collect();
        for key in keys {
            let parked: Vec<FlushBatch> = self
                .registrants
                .get_mut(&key)
                .expect("attach client: key from this map")
                .parked
                .drain(..)
                .collect();
            for batch in parked {
                let kept = survives(&key, &batch);
                let outbox = self
                    .registrants
                    .get_mut(&key)
                    .expect("attach client: key from this map");
                if kept {
                    outbox.parked.push_back(batch);
                } else {
                    outbox.dropped += 1;
                    steps.dropped.push(key.clone());
                }
            }
            steps.frames.extend(self.pump(&key));
        }
        steps.retry_wakeup = self.retry_wakeup(now);
        steps
    }

    /// The attachment went away.
    ///
    /// Outstanding batches die with the connection that carried them: there is
    /// nothing to answer and nothing to retry, and a batch the peer may or may not
    /// have applied is exactly the case a resend would double-apply. The queued
    /// flushes themselves survive — they were never sent and are still owed the
    /// wire — and each registrant's in-flight marker clears with the frame it
    /// named, so the next attachment starts free.
    pub fn on_detached(&mut self) -> OutboxSteps<K> {
        self.live = false;
        self.pending.clear();
        for outbox in self.registrants.values_mut() {
            outbox.in_flight = None;
        }
        OutboxSteps {
            retry_wakeup: self.disarm_retry(),
            ..OutboxSteps::default()
        }
    }

    /// Settle one `PublishBatchResult`.
    ///
    /// A result for a correlation this attachment never sent, or already settled,
    /// is unreconcilable — the correlation space is the attacher's own and
    /// monotone — so it is an error the embedder turns into a fatal rather than a
    /// tolerated echo.
    ///
    /// A refused flush is **not** discarded. The activation returned ok, so the
    /// guarantee is "flushed, not discarded, up to a stated bound" in the refusal
    /// case exactly as in the detach case — and a refusal is not even a failure:
    /// the peer's backstop meters the wire rate, and being metered is what a
    /// backstop is for. The flush goes back to the head of its outbox (it is the
    /// oldest un-applied one) and is retried on the timer. Nothing retries
    /// forever without evidence: a head the peer keeps refusing converges to the
    /// outbox cap, and from there to counted drops.
    ///
    /// The answer is matched to the *registration* that sent it, not merely to
    /// its key. A key deregistered and registered again while its batch was
    /// unanswered — a restarting registrant — leaves a successor whose outbox
    /// this answer says nothing about: applying it there would clear a marker
    /// naming a live frame (putting two of that registrant's flushes on the wire
    /// at once) and re-park a dead registration's entries under the successor's
    /// attribution.
    pub fn on_batch_result(
        &mut self,
        correlation: u64,
        outcome: PublishBatchOutcome,
        now: Millis,
    ) -> Result<BatchAnswer<K>, String> {
        let Some(PendingBatch {
            key,
            generation,
            batch,
        }) = self.pending.remove(&correlation)
        else {
            return Err(format!(
                "PublishBatchResult for unknown correlation {correlation}"
            ));
        };
        let sender_is_live = self
            .registrants
            .get(&key)
            .is_some_and(|outbox| outbox.generation == generation);
        if !sender_is_live {
            // The registrant that sent it went away under the outstanding frame;
            // its outbox went with it. An `Ok` was applied peer-side regardless,
            // but a refusal drops ok'd entries that were never applied anywhere.
            let lost = match outcome {
                PublishBatchOutcome::Ok => None,
                PublishBatchOutcome::RateLimited => Some(batch),
            };
            return Ok(BatchAnswer {
                steps: OutboxSteps::default(),
                registrant: key,
                lost,
            });
        }
        let outbox = self
            .registrants
            .get_mut(&key)
            .expect("attach client: the registration just checked live");
        outbox.in_flight = None;
        let mut steps = OutboxSteps::default();
        match outcome {
            // The wire is free for this registrant again: anything that queued
            // behind the frame goes out now rather than waiting a tick.
            PublishBatchOutcome::Ok => steps.frames.extend(self.pump(&key)),
            PublishBatchOutcome::RateLimited => {
                outbox.rate_limited += 1;
                steps.dropped.extend(self.park(&key, batch, true).dropped);
            }
        }
        steps.retry_wakeup = self.retry_wakeup(now);
        Ok(BatchAnswer {
            steps,
            registrant: key,
            lost: None,
        })
    }

    /// The retry timer fired: offer every blocked outbox's head once more.
    ///
    /// One head per registrant per tick — the head is the oldest un-applied
    /// flush, and anything behind it must not overtake it. Registrants are
    /// independent, so a starved one never blocks a sibling.
    ///
    /// A tick always leaves the plane unblocked, so the fired timer is never
    /// re-armed here: every registrant ends the sweep with either an empty queue
    /// or a flush on the wire, and a queue behind an unanswered flush is pumped
    /// by that flush's own result. What re-arms the timer is the next refusal.
    pub fn on_retry_tick(&mut self, now: Millis) -> OutboxSteps<K> {
        let mut steps = OutboxSteps::default();
        let keys: Vec<K> = self.registrants.keys().cloned().collect();
        for key in keys {
            steps.frames.extend(self.pump(&key));
        }
        debug_assert!(!self.blocked(), "a retry tick leaves no outbox blocked");
        steps.retry_wakeup = self.retry_wakeup(now);
        steps
    }

    /// How many flushes `key` has queued.
    pub fn parked_len(&self, key: &K) -> usize {
        self.registrants
            .get(key)
            .map_or(0, |outbox| outbox.parked.len())
    }

    /// Whole flushes `key` has lost, lifetime.
    pub fn dropped_count(&self, key: &K) -> u64 {
        self.registrants.get(key).map_or(0, |outbox| outbox.dropped)
    }

    /// Flushes of `key`'s the peer's budget refused, lifetime.
    pub fn rate_limited_count(&self, key: &K) -> u64 {
        self.registrants
            .get(key)
            .map_or(0, |outbox| outbox.rate_limited)
    }

    /// Whether a flush for `key` may go straight to the wire.
    fn wire_free_for(&self, key: &K) -> bool {
        if !self.live {
            return false;
        }
        let outbox = self
            .registrants
            .get(key)
            .expect("attach client: a flush implies a registered registrant");
        outbox.parked.is_empty() && outbox.in_flight.is_none()
    }

    /// Put a flush in `key`'s outbox — at the back for a new one, at the head for
    /// a refused one being retried — and enforce the cap.
    ///
    /// Overflow is drop-oldest, whole, counted. A refused head re-parked into a
    /// full outbox is therefore itself the drop: it *is* the oldest, and a queue
    /// at its cap with a head the peer keeps refusing is the mis-provisioned
    /// backstop converging on counted drops rather than on unbounded memory or
    /// silent discard.
    fn park(&mut self, key: &K, batch: FlushBatch, at_head: bool) -> OutboxSteps<K> {
        let outbox = self
            .registrants
            .get_mut(key)
            .expect("attach client: parking a flush for an unregistered registrant");
        if at_head {
            outbox.parked.push_front(batch);
        } else {
            outbox.parked.push_back(batch);
        }
        let mut steps = OutboxSteps::default();
        while outbox.parked.len() as u64 > outbox.depth {
            outbox.parked.pop_front();
            outbox.dropped += 1;
            steps.dropped.push(key.clone());
        }
        steps
    }

    /// Send `key`'s outbox head if the wire is free for it. The one place a
    /// queued flush leaves the attacher.
    fn pump(&mut self, key: &K) -> Vec<ClientFrame> {
        if !self.live {
            return Vec::new();
        }
        let outbox = self
            .registrants
            .get_mut(key)
            .expect("attach client: pumping an unregistered registrant");
        if outbox.in_flight.is_some() {
            return Vec::new();
        }
        let Some(batch) = outbox.parked.pop_front() else {
            return Vec::new();
        };
        vec![self.batch_frame(key, batch)]
    }

    /// Compose one `PublishBatch` frame and record it as outstanding — both in
    /// the correlation table (which answers "whose result is this?") and on the
    /// outbox (which answers "is this registrant's wire free?").
    fn batch_frame(&mut self, key: &K, batch: FlushBatch) -> ClientFrame {
        let correlation = self.next_correlation;
        self.next_correlation += 1;
        let outbox = self
            .registrants
            .get_mut(key)
            .expect("attach client: sending a flush for an unregistered registrant");
        assert!(
            outbox.in_flight.is_none(),
            "attach client: registrant already has a flush on the wire"
        );
        outbox.in_flight = Some(correlation);
        let attribution = outbox.attribution.clone();
        let generation = outbox.generation;
        let frame = ClientFrame::PublishBatch {
            attribution,
            correlation,
            publishes: batch.entries.clone(),
            deferred_ops: batch.ops.clone(),
        };
        self.pending.insert(
            correlation,
            PendingBatch {
                key: key.clone(),
                generation,
                batch,
            },
        );
        frame
    }

    /// Arm or disarm the retry timer from the outbox state, emitting only on the
    /// blocked↔unblocked transition.
    fn retry_wakeup(&mut self, now: Millis) -> Option<TimerChange> {
        let blocked = self.blocked();
        if blocked == self.retry_armed {
            return None;
        }
        self.retry_armed = blocked;
        if blocked {
            Some(TimerChange::Arm(now.saturating_add_ms(RETRY_INTERVAL_MS)))
        } else {
            Some(TimerChange::Disarm)
        }
    }

    /// Some registrant has a queued flush that a tick could actually send: the
    /// attachment is live, the flush is at its outbox's head, and nothing of that
    /// registrant's is on the wire.
    ///
    /// A head merely queued behind an unanswered flush is deliberately *not*
    /// blocked. Its own result pumps the queue the moment it lands, so a timer
    /// armed for it would only wake to sweep every registrant and do nothing —
    /// idle work at exactly the loaded moment that produced the queue.
    fn blocked(&self) -> bool {
        self.live
            && self
                .registrants
                .values()
                .any(|outbox| outbox.in_flight.is_none() && !outbox.parked.is_empty())
    }

    /// Disarm the retry timer on the way out of a live attachment, if it was
    /// armed.
    fn disarm_retry(&mut self) -> Option<TimerChange> {
        if !self.retry_armed {
            return None;
        }
        self.retry_armed = false;
        Some(TimerChange::Disarm)
    }
}

/// The peer's view of what each sub-identity has parked on each channel.
///
/// The attacher cannot maintain this itself: parked entries on a durable channel
/// outlive the attachment, releases happen on the peer's clock, and every
/// attachment of one principal shares its parked set. So the authority pushes a
/// full snapshot per `(channel, attribution)` whenever one changes, and this is
/// the mirror.
///
/// Last-writer-wins by construction: a snapshot replaces whatever was held.
/// [`clear`](DeferredViews::clear) at every attach, because the peer re-seeds
/// only the *nonempty* sets — a set with no frame is empty, and a stale mirror
/// would show messages nobody holds.
#[derive(Default)]
pub struct DeferredViews {
    views: BTreeMap<(String, Option<String>), Vec<DeferredViewEntry>>,
}

impl DeferredViews {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take one `DeferredView` snapshot.
    pub fn on_view(
        &mut self,
        channel: String,
        attribution: Option<String>,
        entries: Vec<DeferredViewEntry>,
    ) {
        self.views.insert((channel, attribution), entries);
    }

    /// What `attribution` has parked on `channel`, soonest release first. Empty
    /// for a set the peer has said nothing about, which is what an empty set
    /// looks like.
    pub fn get(&self, channel: &str, attribution: Option<&str>) -> &[DeferredViewEntry] {
        self.views
            .get(&(channel.to_string(), attribution.map(str::to_string)))
            .map_or(&[], Vec::as_slice)
    }

    /// Drop every mirror. Called at each attach, ahead of the peer's re-seeding.
    pub fn clear(&mut self) {
        self.views.clear();
    }

    /// How many `(channel, attribution)` sets are mirrored.
    pub fn len(&self) -> usize {
        self.views.len()
    }

    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }
}

#[cfg(test)]
mod tests;

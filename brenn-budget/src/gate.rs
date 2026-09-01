//! The per-activation gate: one activation's counters, buckets, and the
//! canonical order in which a guest's publish and control-op calls are checked.
//!
//! Every host that mints activations runs the same checks in the same sequence,
//! because "the same reason" for refusing a component's call includes *which
//! check wins when two would fire*. That order lives here, once, so a new rule
//! cannot land on one hosting and be forgotten on the other.
//!
//! # What the gate owns, and what it does not
//!
//! The gate owns everything that is pure arithmetic over one activation: the
//! shared call count, the buffered-entry count, the buffered-op count, the byte
//! aggregate, and the per-sink millitoken buckets. It owns no host data — no
//! port table, no ACL, no deferred snapshot — so a host keeps those checks and
//! runs them at the positions documented below. It counts nothing for
//! observability either: a host that wants per-sink suppression counts reads
//! [`GateRefusal::SinkExhausted`] off the verdict and tallies its own.
//!
//! # Publish order
//!
//! 1. [`ActivationGate::charge_call`] — the shared call ceiling, first, so a
//!    component looping on rejections pays for them. A refused call costs no
//!    bytes and no buffer slot, so without this the rejection path is free.
//! 2. **Host:** port → binding lookup (an unknown port is not-permitted), and
//!    on the backend the output-channel ACL. Both are reads of host-owned maps.
//! 3. [`ActivationGate::admit_publish`] — body size against the host-supplied
//!    body cap, release-time representability, the sink bucket, the buffered
//!    entry ceiling, then the byte aggregate. Charges the bucket, the byte
//!    aggregate, and the entry count only on acceptance, so a publish refused
//!    by a later check has spent nothing.
//!
//! A publish to a port the component declares but its deployer did not wire has
//! no sink, and takes [`ActivationGate::admit_publish_without_sink`] at step 3
//! instead: the same checks in the same order, minus the bucket.
//!
//! # Control-op order
//!
//! 1. [`check_deliver_after`] — a fact about the argument, not about any
//!    budget, so it answers before anything is charged. It is therefore the one
//!    refusal in either order that draws no call slot: unlike the publish path,
//!    where the same check sits inside [`ActivationGate::admit_publish`] and so
//!    costs a slot, a repr-refused op is uncounted. That is affordable because
//!    the check reads its argument and returns — no host map, no buffer, no
//!    log — so a component looping on it buys only the CPU its own activation
//!    deadline already bounds.
//! 2. [`ActivationGate::charge_call`] — the same ceiling publishes draw on, so
//!    every op that gets as far as a host lookup is counted whether or not it
//!    is accepted.
//! 3. **Host:** port → binding lookup, then the index into the deferred window
//!    this activation delivered. Both are reads of host-owned data.
//! 4. [`ActivationGate::admit_op`] — the buffered-op ceiling, then an edit
//!    body's size against the same cap a published body answers to, then that
//!    body's charge against the same byte aggregate. Charges only on
//!    acceptance.
//!
//! An edit body is a body: it is capped and it is charged. The op ceiling stays
//! a counter of its own — with edit bytes in the aggregate, a bounded count of
//! bounded-byte ops is harmless.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use crate::{
    MAX_PUBLISH_BYTES_PER_ACTIVATION, MAX_PUBLISH_CALLS_PER_ACTIVATION,
    MAX_PUBLISHES_PER_ACTIVATION, MILLITOKENS_PER_PUBLISH,
};

/// Why the gate refused a call.
///
/// One vocabulary, shared: each host maps these onto its own guest-facing error
/// type (the WIT-generated enums on the backend, the contract's verbatim enums
/// on the page). The mapping is per call site, because the same refusal is not
/// always the same guest error — an oversize published body is an invalid
/// payload, while an oversize *edit* body is a quota refusal, on both hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateRefusal {
    /// The activation's total call ceiling (accepted + refused) is spent.
    CallCap { cap: usize },
    /// The body exceeds the host-supplied per-message cap.
    BodyTooLarge { len: usize, max: usize },
    /// The requested release time is not a timestamp every authority can carry.
    UnrepresentableDeliverAfter { ms: u64 },
    /// The sink's millitoken bucket cannot pay for this publish.
    SinkExhausted,
    /// The activation's buffered-publish ceiling is reached.
    EntryCap { cap: usize },
    /// The activation's aggregate body-byte ceiling would be exceeded.
    ByteCap { cap: usize },
    /// The activation's buffered-control-op ceiling is reached.
    OpCap { cap: usize },
}

/// Whether a caller-supplied epoch-ms release time is a timestamp every parking
/// authority in the system can carry.
///
/// Delegates to [`brenn_envelope::utc_from_epoch_ms`], the single bound every
/// deliver-after gate shares, so no two gates can disagree about which times
/// exist. `None` — no release time at all — is trivially representable.
pub fn check_deliver_after(deliver_after: Option<u64>) -> Result<(), GateRefusal> {
    match deliver_after {
        Some(ms) if brenn_envelope::utc_from_epoch_ms(ms).is_none() => {
            Err(GateRefusal::UnrepresentableDeliverAfter { ms })
        }
        _ => Ok(()),
    }
}

/// One publish as the gate needs to judge it: which sink pays, how big the body
/// is, when it wants to be released, and how many buffered entries the host
/// holds outside the gate's own count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishCheck<'a, K> {
    /// The sink whose bucket pays. Must have a seeded bucket — a bound port
    /// always does, so a miss is a broken host invariant and panics.
    pub sink: &'a K,
    pub body_len: usize,
    pub deliver_after: Option<u64>,
    /// Buffered entries the host counts against the same ceiling but the gate
    /// does not hold. Zero where the gate's own count is the whole of it.
    pub entry_addend: usize,
}

/// One activation's counters and buckets, enforcing the shared check order.
///
/// Generic over the sink key so each host names its own sinks: the page keys by
/// output port, the backend by port *or* MQTT client slug.
///
/// Not `Default`: a gate only ever exists seeded for exactly one activation, and
/// an unseeded one would enforce nothing while looking like it enforced
/// everything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationGate<K: Eq + Hash> {
    /// The per-message body cap, from the host's own config. Applies to a
    /// published body and to an edit's replacement body alike.
    max_body_bytes: usize,
    /// Remaining millitokens per sink, seeded by [`crate::seed_sink_budget`] and
    /// charged [`MILLITOKENS_PER_PUBLISH`] per accepted publish. Whatever
    /// survives is the next activation's carryover.
    sinks: HashMap<K, u64>,
    /// Every publish and control call this activation, accepted or refused.
    /// Refusals count: a rejected call is otherwise free to a component that
    /// loops on one, and free is what makes it a flood.
    calls: usize,
    /// Accepted publishes.
    entries: usize,
    /// Accepted publishes that had no sink to pay and no entry to buffer — the
    /// subset of `entries` a host dropped rather than queued. Held here because
    /// the sink-less admission is the only way to produce one, so the gate is
    /// the one place that cannot fall out of step with the ceiling it charges.
    dropped: usize,
    /// Accepted control ops.
    ops: usize,
    /// Accepted body bytes — published bodies and edit bodies together.
    bytes: usize,
}

impl<K: Eq + Hash + Clone + Debug> ActivationGate<K> {
    /// Seed a gate for one activation. `sinks` is already
    /// [`crate::seed_sink_budget`]-folded by the caller (carry clamped, fill and
    /// input grant added) — the gate spends budgets, it does not compute them.
    pub fn new(max_body_bytes: usize, sinks: HashMap<K, u64>) -> Self {
        Self {
            max_body_bytes,
            sinks,
            calls: 0,
            entries: 0,
            dropped: 0,
            ops: 0,
            bytes: 0,
        }
    }

    /// The per-message body cap this gate enforces.
    pub fn max_body_bytes(&self) -> usize {
        self.max_body_bytes
    }

    /// Count this call against the activation's ceiling — step 1 of both orders.
    ///
    /// Counts before it checks, so the call that trips the cap is itself
    /// counted: the ceiling is on calls made, not on calls admitted.
    pub fn charge_call(&mut self) -> Result<(), GateRefusal> {
        self.calls += 1;
        if self.calls > MAX_PUBLISH_CALLS_PER_ACTIVATION {
            return Err(GateRefusal::CallCap {
                cap: MAX_PUBLISH_CALLS_PER_ACTIVATION,
            });
        }
        Ok(())
    }

    /// Judge one publish and, on acceptance, charge everything it costs.
    ///
    /// The host has already counted the call and resolved the port; what is left
    /// is arithmetic. Order: body cap, release-time representability, sink
    /// bucket (the tightest, most specific gate), buffered-entry ceiling, byte
    /// aggregate (the outer backstops, bounding the host's own memory against a
    /// component whose buckets are generous).
    ///
    /// # Panics
    ///
    /// If `check.sink` has no seeded bucket. Every sink a host admits a publish
    /// for is seeded from the same map the port resolution read, so a miss is a
    /// broken host invariant, not a component's problem.
    pub fn admit_publish(&mut self, check: PublishCheck<'_, K>) -> Result<(), GateRefusal> {
        if check.body_len > self.max_body_bytes {
            return Err(GateRefusal::BodyTooLarge {
                len: check.body_len,
                max: self.max_body_bytes,
            });
        }
        check_deliver_after(check.deliver_after)?;
        // The bucket is resolved once and held across the ceilings, so the check
        // and the debit are the same entry by construction. The ceilings below are
        // therefore written as field reads rather than calls through `&self`,
        // which the live bucket borrow rules out.
        let Some(remaining) = self.sinks.get_mut(check.sink) else {
            panic!("activation gate: no sink budget for {:?}", check.sink)
        };
        if *remaining < MILLITOKENS_PER_PUBLISH {
            return Err(GateRefusal::SinkExhausted);
        }
        if self.entries + check.entry_addend >= MAX_PUBLISHES_PER_ACTIVATION {
            return Err(GateRefusal::EntryCap {
                cap: MAX_PUBLISHES_PER_ACTIVATION,
            });
        }
        if self.bytes + check.body_len > MAX_PUBLISH_BYTES_PER_ACTIVATION {
            return Err(GateRefusal::ByteCap {
                cap: MAX_PUBLISH_BYTES_PER_ACTIVATION,
            });
        }
        *remaining -= MILLITOKENS_PER_PUBLISH;
        self.bytes += check.body_len;
        self.entries += 1;
        Ok(())
    }

    /// Judge one publish that has no sink, and on acceptance charge everything a
    /// sunk publish would charge except the bucket.
    ///
    /// A component may declare an output port its deployer chose not to wire.
    /// Publishing there is legal and the message is dropped, so there is no
    /// bucket to pay — but every check that does not need one still has to
    /// answer, in the same order and with the same verdicts, or the guest could
    /// read its own wiring out of the difference between the two paths. Order and
    /// charges are [`Self::admit_publish`]'s with the bucket step removed: body
    /// cap, release-time representability, buffered-entry ceiling, byte
    /// aggregate.
    ///
    /// The entry count is charged even though nothing is buffered: a dropped
    /// publish must consume the same ceiling a delivered one does, or the ceiling
    /// itself becomes the channel that reports wiring.
    pub fn admit_publish_without_sink(
        &mut self,
        body_len: usize,
        deliver_after: Option<u64>,
        entry_addend: usize,
    ) -> Result<(), GateRefusal> {
        if body_len > self.max_body_bytes {
            return Err(GateRefusal::BodyTooLarge {
                len: body_len,
                max: self.max_body_bytes,
            });
        }
        check_deliver_after(deliver_after)?;
        if self.entries + entry_addend >= MAX_PUBLISHES_PER_ACTIVATION {
            return Err(GateRefusal::EntryCap {
                cap: MAX_PUBLISHES_PER_ACTIVATION,
            });
        }
        if self.bytes + body_len > MAX_PUBLISH_BYTES_PER_ACTIVATION {
            return Err(GateRefusal::ByteCap {
                cap: MAX_PUBLISH_BYTES_PER_ACTIVATION,
            });
        }
        self.bytes += body_len;
        self.entries += 1;
        self.dropped += 1;
        Ok(())
    }

    /// Whether the buffered-entry ceiling is reached, counting `addend` host-held
    /// entries alongside the gate's own.
    ///
    /// Public for host paths that buffer entries outside the gate's own count yet
    /// must answer to the same ceiling.
    pub fn entries_at_ceiling(&self, addend: usize) -> bool {
        self.entries + addend >= MAX_PUBLISHES_PER_ACTIVATION
    }

    /// Judge one control op and, on acceptance, charge what it costs.
    ///
    /// The host has already checked representability, counted the call, resolved
    /// the port, and resolved the index. Order: the buffered-op ceiling, then
    /// `edit_body_len` (`Some` only for an edit that replaces the body) against
    /// the per-message cap, then against the activation's byte aggregate.
    pub fn admit_op(&mut self, edit_body_len: Option<usize>) -> Result<(), GateRefusal> {
        if self.ops >= MAX_PUBLISHES_PER_ACTIVATION {
            return Err(GateRefusal::OpCap {
                cap: MAX_PUBLISHES_PER_ACTIVATION,
            });
        }
        if let Some(len) = edit_body_len {
            if len > self.max_body_bytes {
                return Err(GateRefusal::BodyTooLarge {
                    len,
                    max: self.max_body_bytes,
                });
            }
            if self.bytes + len > MAX_PUBLISH_BYTES_PER_ACTIVATION {
                return Err(GateRefusal::ByteCap {
                    cap: MAX_PUBLISH_BYTES_PER_ACTIVATION,
                });
            }
            self.bytes += len;
        }
        self.ops += 1;
        Ok(())
    }

    /// Charge one sink for a synchronous, unbuffered send that reaches its
    /// destination inside the call — charges on attempt rather than on
    /// acceptance.
    ///
    /// A sink with no bucket is unmetered here, unlike [`Self::admit_publish`]:
    /// the caller's own ACL handling governs unbudgeted sinks.
    pub fn charge_sink_on_attempt(&mut self, sink: &K) -> Result<(), GateRefusal> {
        let Some(remaining) = self.sinks.get_mut(sink) else {
            return Ok(());
        };
        if *remaining < MILLITOKENS_PER_PUBLISH {
            return Err(GateRefusal::SinkExhausted);
        }
        *remaining -= MILLITOKENS_PER_PUBLISH;
        Ok(())
    }

    /// Calls made this activation, accepted or refused.
    pub fn calls(&self) -> usize {
        self.calls
    }

    /// Publishes accepted this activation.
    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Publishes accepted with no sink, and so dropped rather than buffered —
    /// the part of [`Self::entries`] a host's own buffer does not hold. A host
    /// reconciles its buffer against the ceiling as
    /// `entries() == buffered + dropped()`.
    pub fn dropped(&self) -> usize {
        self.dropped
    }

    /// Control ops accepted this activation.
    pub fn ops(&self) -> usize {
        self.ops
    }

    /// Body bytes accepted this activation — published and edited alike.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Remaining per-sink millitokens — the next activation's carryover.
    pub fn sinks(&self) -> &HashMap<K, u64> {
        &self.sinks
    }

    /// Remaining per-sink millitokens, consuming the gate.
    ///
    /// Carryover survives an err or a trap: what a component *spent* is a fact
    /// about the activation that happened, and a failure does not un-spend it.
    pub fn into_sinks(self) -> HashMap<K, u64> {
        self.sinks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PORT: &str = "out";

    fn gate(max_body_bytes: usize, sink_mt: u64) -> ActivationGate<&'static str> {
        let mut sinks = HashMap::new();
        sinks.insert(PORT, sink_mt);
        ActivationGate::new(max_body_bytes, sinks)
    }

    fn publish(body_len: usize) -> PublishCheck<'static, &'static str> {
        PublishCheck {
            sink: &PORT,
            body_len,
            deliver_after: None,
            entry_addend: 0,
        }
    }

    #[test]
    fn an_accepted_publish_charges_bucket_bytes_and_entry() {
        let mut g = gate(1024, 3 * MILLITOKENS_PER_PUBLISH);
        g.charge_call().expect("first call is under the ceiling");
        g.admit_publish(publish(10)).expect("every check passes");
        assert_eq!(g.entries(), 1);
        assert_eq!(g.bytes(), 10);
        assert_eq!(g.calls(), 1);
        assert_eq!(g.sinks()[PORT], 2 * MILLITOKENS_PER_PUBLISH);
    }

    /// The call ceiling counts the call that trips it, and every call after —
    /// the whole point is that refusals are not free.
    #[test]
    fn the_call_ceiling_counts_refusals() {
        let mut g = gate(1024, u64::MAX);
        for _ in 0..MAX_PUBLISH_CALLS_PER_ACTIVATION {
            g.charge_call().expect("under the ceiling");
        }
        assert_eq!(
            g.charge_call(),
            Err(GateRefusal::CallCap {
                cap: MAX_PUBLISH_CALLS_PER_ACTIVATION
            })
        );
        assert_eq!(g.calls(), MAX_PUBLISH_CALLS_PER_ACTIVATION + 1);
        assert_eq!(
            g.charge_call(),
            Err(GateRefusal::CallCap {
                cap: MAX_PUBLISH_CALLS_PER_ACTIVATION
            })
        );
        assert_eq!(g.calls(), MAX_PUBLISH_CALLS_PER_ACTIVATION + 2);
    }

    /// The body cap answers before the release time, before the bucket, and
    /// before the aggregates: an oversize body is a fact about the argument, and
    /// it must not spend a token on its way to being refused.
    #[test]
    fn the_body_cap_wins_over_every_later_check() {
        let mut g = gate(8, 0);
        let check = PublishCheck {
            body_len: 9,
            deliver_after: Some(u64::MAX),
            ..publish(9)
        };
        assert_eq!(
            g.admit_publish(check),
            Err(GateRefusal::BodyTooLarge { len: 9, max: 8 })
        );
        assert_eq!(g.bytes(), 0);
        assert_eq!(g.entries(), 0);
    }

    /// Representability answers before the bucket: an unrepresentable release
    /// time is refused on an exhausted sink without being counted as a
    /// suppression.
    #[test]
    fn representability_wins_over_the_bucket() {
        let mut g = gate(1024, 0);
        let check = PublishCheck {
            deliver_after: Some(u64::MAX),
            ..publish(4)
        };
        assert_eq!(
            g.admit_publish(check),
            Err(GateRefusal::UnrepresentableDeliverAfter { ms: u64::MAX })
        );
    }

    /// A representable release time passes the check that an unrepresentable one
    /// fails, so the gate is not simply refusing every deferral.
    #[test]
    fn a_representable_release_time_passes() {
        assert_eq!(check_deliver_after(None), Ok(()));
        assert_eq!(check_deliver_after(Some(0)), Ok(()));
        assert_eq!(check_deliver_after(Some(1_800_000_000_000)), Ok(()));
        assert!(check_deliver_after(Some(u64::MAX)).is_err());
    }

    /// The bucket answers before the per-activation ceilings — the tightest gate
    /// first.
    #[test]
    fn the_bucket_wins_over_the_ceilings() {
        let mut g = gate(1024, MILLITOKENS_PER_PUBLISH - 1);
        let check = PublishCheck {
            entry_addend: MAX_PUBLISHES_PER_ACTIVATION,
            ..publish(4)
        };
        assert_eq!(g.admit_publish(check), Err(GateRefusal::SinkExhausted));
        assert_eq!(g.entries(), 0);
        assert_eq!(g.bytes(), 0);
    }

    /// The entry ceiling counts the host's own buffered entries through the
    /// addend, and answers before the byte aggregate.
    #[test]
    fn the_entry_ceiling_counts_the_hosts_addend() {
        let mut g = gate(1024, u64::MAX);
        let check = PublishCheck {
            entry_addend: MAX_PUBLISHES_PER_ACTIVATION,
            ..publish(4)
        };
        assert_eq!(
            g.admit_publish(check),
            Err(GateRefusal::EntryCap {
                cap: MAX_PUBLISHES_PER_ACTIVATION
            })
        );
        assert!(!g.entries_at_ceiling(0));
        assert!(g.entries_at_ceiling(MAX_PUBLISHES_PER_ACTIVATION));
        // An accepted publish and the host's addend count against the one ceiling.
        g.admit_publish(publish(4)).expect("no addend, empty gate");
        assert!(g.entries_at_ceiling(MAX_PUBLISHES_PER_ACTIVATION - 1));
    }

    #[test]
    fn the_byte_aggregate_refuses_last_and_charges_nothing() {
        let mut g = gate(MAX_PUBLISH_BYTES_PER_ACTIVATION, u64::MAX);
        g.admit_publish(publish(MAX_PUBLISH_BYTES_PER_ACTIVATION - 1))
            .expect("the first body fits the aggregate");
        let before = g.sinks()[PORT];
        assert_eq!(
            g.admit_publish(publish(2)),
            Err(GateRefusal::ByteCap {
                cap: MAX_PUBLISH_BYTES_PER_ACTIVATION
            })
        );
        assert_eq!(g.sinks()[PORT], before);
        assert_eq!(g.entries(), 1);
    }

    #[test]
    fn a_cancel_charges_only_an_op_slot() {
        let mut g = gate(8, u64::MAX);
        g.admit_op(None).expect("a cancel has no body to weigh");
        assert_eq!(g.ops(), 1);
        assert_eq!(g.bytes(), 0);
        assert_eq!(g.entries(), 0);
    }

    #[test]
    fn the_op_ceiling_wins_over_the_edit_body_rules() {
        let mut g = gate(8, u64::MAX);
        for _ in 0..MAX_PUBLISHES_PER_ACTIVATION {
            g.admit_op(None).expect("under the op ceiling");
        }
        assert_eq!(
            g.admit_op(Some(9)),
            Err(GateRefusal::OpCap {
                cap: MAX_PUBLISHES_PER_ACTIVATION
            })
        );
    }

    /// An edit body answers to the same per-message cap a published body does.
    #[test]
    fn an_oversize_edit_body_is_refused() {
        let mut g = gate(8, u64::MAX);
        assert_eq!(
            g.admit_op(Some(9)),
            Err(GateRefusal::BodyTooLarge { len: 9, max: 8 })
        );
        assert_eq!(g.ops(), 0);
        assert_eq!(g.bytes(), 0);
        g.admit_op(Some(8)).expect("a body exactly at the cap fits");
        assert_eq!(g.bytes(), 8);
    }

    /// An edit body is charged to the one aggregate publishes draw on: a fat
    /// edit makes a later publish that would fit alone refuse.
    #[test]
    fn a_fat_edit_consumes_the_publish_aggregate() {
        let mut g = gate(MAX_PUBLISH_BYTES_PER_ACTIVATION, u64::MAX);
        g.admit_op(Some(MAX_PUBLISH_BYTES_PER_ACTIVATION - 1))
            .expect("the edit body fits the aggregate on its own");
        assert_eq!(g.bytes(), MAX_PUBLISH_BYTES_PER_ACTIVATION - 1);
        assert_eq!(
            g.admit_publish(publish(2)),
            Err(GateRefusal::ByteCap {
                cap: MAX_PUBLISH_BYTES_PER_ACTIVATION
            })
        );
        // And the converse: a fat publish makes a later edit refuse.
        let mut g = gate(MAX_PUBLISH_BYTES_PER_ACTIVATION, u64::MAX);
        g.admit_publish(publish(MAX_PUBLISH_BYTES_PER_ACTIVATION - 1))
            .expect("the published body fits the aggregate on its own");
        assert_eq!(
            g.admit_op(Some(2)),
            Err(GateRefusal::ByteCap {
                cap: MAX_PUBLISH_BYTES_PER_ACTIVATION
            })
        );
        assert_eq!(g.ops(), 0);
    }

    /// An edit refused by the aggregate has not drawn an op slot, so the two
    /// counters cannot drift from what was accepted.
    #[test]
    fn a_refused_edit_draws_no_op_slot() {
        let mut g = gate(MAX_PUBLISH_BYTES_PER_ACTIVATION, u64::MAX);
        g.admit_publish(publish(MAX_PUBLISH_BYTES_PER_ACTIVATION))
            .expect("the aggregate is exactly spent");
        assert!(g.admit_op(Some(1)).is_err());
        assert_eq!(g.ops(), 0);
        assert_eq!(g.bytes(), MAX_PUBLISH_BYTES_PER_ACTIVATION);
    }

    /// The op ceiling and the entry ceiling are two counters, not one shared
    /// slot pool: a component that spent its publishes can still cancel a
    /// schedule, and one that cancelled 256 schedules can still publish. Folding
    /// them together would change what a conforming component observes at the
    /// WIT seam on both hostings.
    #[test]
    fn the_op_and_entry_ceilings_are_independent() {
        let mut g = gate(1024, u64::MAX);
        for _ in 0..MAX_PUBLISHES_PER_ACTIVATION {
            g.admit_op(None).expect("under the op ceiling");
        }
        assert_eq!(g.ops(), MAX_PUBLISHES_PER_ACTIVATION);
        g.admit_publish(publish(4))
            .expect("a full op count leaves the publish path answering on its own terms");

        let mut g = gate(1024, u64::MAX);
        for _ in 0..MAX_PUBLISHES_PER_ACTIVATION {
            g.admit_publish(publish(1))
                .expect("under the entry ceiling");
        }
        assert!(g.entries_at_ceiling(0));
        assert_eq!(
            g.admit_publish(publish(1)),
            Err(GateRefusal::EntryCap {
                cap: MAX_PUBLISHES_PER_ACTIVATION
            })
        );
        g.admit_op(None)
            .expect("a spent entry ceiling leaves the op path answering on its own terms");
        assert_eq!(g.ops(), 1);
    }

    #[test]
    fn the_attempt_charge_spends_and_tolerates_an_unbudgeted_sink() {
        let mut g = gate(1024, MILLITOKENS_PER_PUBLISH);
        g.charge_sink_on_attempt(&PORT)
            .expect("the bucket pays once");
        assert_eq!(g.sinks()[PORT], 0);
        assert_eq!(
            g.charge_sink_on_attempt(&PORT),
            Err(GateRefusal::SinkExhausted)
        );
        // A sink with no bucket is outside the metering, not refused by it.
        g.charge_sink_on_attempt(&"unbudgeted")
            .expect("an unmetered sink is not refused by the gate");
    }

    /// Carryover is what the buckets have left, whatever the activation's
    /// outcome was.
    #[test]
    fn carry_is_the_remaining_buckets() {
        let mut g = gate(1024, 5 * MILLITOKENS_PER_PUBLISH);
        g.admit_publish(publish(1)).expect("accepted");
        g.admit_publish(publish(1)).expect("accepted");
        let carry = g.into_sinks();
        assert_eq!(carry[PORT], 3 * MILLITOKENS_PER_PUBLISH);
    }

    /// The sink-less path answers every check the sunk path answers except the
    /// bucket, with the same verdicts in the same order — that identity is what
    /// keeps a component from reading its own wiring out of an error code.
    #[test]
    fn the_sinkless_path_matches_the_sunk_path_check_for_check() {
        // Body cap.
        let mut g = gate(8, u64::MAX);
        assert_eq!(
            g.admit_publish_without_sink(9, Some(u64::MAX), 0),
            Err(GateRefusal::BodyTooLarge { len: 9, max: 8 })
        );
        // Representability, after the body cap.
        assert_eq!(
            g.admit_publish_without_sink(8, Some(u64::MAX), 0),
            Err(GateRefusal::UnrepresentableDeliverAfter { ms: u64::MAX })
        );
        // Entry ceiling, counting the host's addend.
        assert_eq!(
            g.admit_publish_without_sink(1, None, MAX_PUBLISHES_PER_ACTIVATION),
            Err(GateRefusal::EntryCap {
                cap: MAX_PUBLISHES_PER_ACTIVATION
            })
        );
        // Byte aggregate, last.
        let mut g = gate(MAX_PUBLISH_BYTES_PER_ACTIVATION, u64::MAX);
        g.admit_publish_without_sink(MAX_PUBLISH_BYTES_PER_ACTIVATION - 1, None, 0)
            .expect("the first body fits the aggregate");
        assert_eq!(
            g.admit_publish_without_sink(2, None, 0),
            Err(GateRefusal::ByteCap {
                cap: MAX_PUBLISH_BYTES_PER_ACTIVATION
            })
        );
        // Nothing refused was charged.
        assert_eq!(g.entries(), 1);
        assert_eq!(g.bytes(), MAX_PUBLISH_BYTES_PER_ACTIVATION - 1);
    }

    /// An accepted sink-less publish charges the entry and the bytes but spends
    /// no tokens — there is no bucket to spend them from, and the two ceilings
    /// are exactly what a dropped message must still answer to.
    #[test]
    fn an_accepted_sinkless_publish_charges_the_ceilings_and_no_bucket() {
        let mut g = gate(1024, 3 * MILLITOKENS_PER_PUBLISH);
        g.charge_call().expect("first call is under the ceiling");
        g.admit_publish_without_sink(10, None, 0)
            .expect("every sink-less check passes");
        assert_eq!(g.entries(), 1);
        assert_eq!(g.bytes(), 10);
        assert_eq!(g.calls(), 1);
        assert_eq!(g.sinks()[PORT], 3 * MILLITOKENS_PER_PUBLISH);
    }

    /// `dropped()` counts the sink-less admissions and only those, so a host can
    /// reconcile its buffer against the ceiling without keeping a count of its
    /// own. A refused sink-less publish charges nothing, here included.
    #[test]
    fn the_gate_counts_the_publishes_it_admitted_with_no_sink() {
        let mut g = gate(8, 3 * MILLITOKENS_PER_PUBLISH);
        g.admit_publish(publish(1)).expect("the bucket pays");
        assert_eq!(g.dropped(), 0);
        g.admit_publish_without_sink(4, None, 0)
            .expect("every sink-less check passes");
        g.admit_publish_without_sink(4, None, 0)
            .expect("every sink-less check passes");
        assert_eq!(
            g.admit_publish_without_sink(9, None, 0),
            Err(GateRefusal::BodyTooLarge { len: 9, max: 8 })
        );
        assert_eq!(g.dropped(), 2);
        assert_eq!(g.entries(), 3);
    }

    /// The gate holds no sink table entry for an unwired port and does not need
    /// one: the sink-less path never looks, so it cannot panic the way
    /// [`ActivationGate::admit_publish`] does on an unseeded key.
    #[test]
    fn the_sinkless_path_needs_no_seeded_bucket() {
        let mut g = ActivationGate::<&'static str>::new(1024, HashMap::new());
        g.admit_publish_without_sink(4, None, 0)
            .expect("no bucket is consulted");
        assert_eq!(g.entries(), 1);
    }

    /// A publish naming a sink the gate was never seeded with is a broken host
    /// invariant, not a component's problem.
    #[test]
    #[should_panic(expected = "no sink budget for")]
    fn an_unseeded_sink_panics() {
        let mut g = ActivationGate::<&'static str>::new(1024, HashMap::new());
        let _ = g.admit_publish(publish(1));
    }
}

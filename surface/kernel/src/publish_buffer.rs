//! The per-activation publish buffer — the kernel's half of flush-on-ok.
//!
//! One activation gets one [`PublishBuffer`]. The core seeds it at dispatch with
//! everything a verdict needs (the instance's output bindings, their resolved
//! sink budgets plus this activation's input grant, the body cap) and hands it to
//! the entry, which publishes into it synchronously. It is the **sole quota
//! authority for the duration of the handler**: every answer is inline, so the
//! entry never waits on the driver re-entering the core mid-call, and there is no
//! quota race to lose — the buffer is single-threaded by construction.
//!
//! Nothing here touches the router or the wire. Buffered entries go somewhere
//! only when the activation completes, and only if it returned ok; on err or trap
//! the whole buffer is discarded. That is the flush rule, and keeping the buffer
//! ignorant of both destinations is what makes it impossible to leak a publish
//! out of a failed activation.
//!
//! The budget arithmetic and the order the checks fire in are not here: they are
//! [`brenn_budget::ActivationGate`], which both hosts run their guests' calls
//! through; so is the classification of its verdicts, which arrives here as
//! [`brenn_budget::RefusalKind`]. What is here is the page's own half — the port
//! table, the deferred snapshot, the buffered entries — plus the last step onto
//! the contract's error vocabulary.

use std::collections::HashMap;

use brenn_budget::{
    ActivationGate, GateRefusal, PublishCheck, RefusalKind, check_deliver_after,
    defer_refusal_kind, publish_refusal_kind,
};
use brenn_envelope::Urgency;
use brenn_surface_contract::{DeferError, PublishError};
use uuid::Uuid;

use brenn_attach_client::store::DeferOp;

use crate::bindings::channel_is_transportable;

/// A gate verdict as the component sees it on the publish path.
///
/// The classification is [`brenn_budget::publish_refusal_kind`], shared with the
/// backend host.
fn publish_error(refusal: GateRefusal) -> PublishError {
    match publish_refusal_kind(refusal) {
        RefusalKind::InvalidPayload(_) => PublishError::InvalidPayload,
        RefusalKind::QuotaExceeded => PublishError::QuotaExceeded,
        RefusalKind::InvalidDeliverAfter => {
            unreachable!("publish_refusal_kind never answers invalid-deliver-after")
        }
    }
}

/// A gate verdict as the component sees it on the control-op path.
///
/// The classification is [`brenn_budget::defer_refusal_kind`], shared with the
/// backend host.
fn defer_error(refusal: GateRefusal) -> DeferError {
    match defer_refusal_kind(refusal) {
        RefusalKind::InvalidDeliverAfter => DeferError::InvalidDeliverAfter,
        RefusalKind::QuotaExceeded => DeferError::QuotaExceeded,
        RefusalKind::InvalidPayload(_) => {
            unreachable!("defer_refusal_kind never answers invalid-payload")
        }
    }
}

/// One publish accepted into the buffer, in call order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BufferedPublish {
    /// The instance's own output port name.
    pub port: String,
    /// The channel it resolved to, captured at buffer time from the bindings the
    /// activation was seeded with. Captured rather than re-resolved at flush:
    /// the resolution that authorized the publish is the one that must route it,
    /// and a `Welcome` can land between the two.
    pub channel: String,
    pub body: String,
    /// The resolved urgency: the caller's override, else the port's configured
    /// default. Resolved here for `local:` entries (the router applies no default
    /// of its own); wire entries carry the raw override on the frame and let the
    /// server apply the default it owns — see [`PublishBuffer::take`].
    pub urgency: Urgency,
    /// Whether the caller stated an urgency. The wire frame carries the override
    /// or nothing, so the server's own resolved default keeps winning for a
    /// silent caller — a client echoing back an advertised default could
    /// override the operator with a stale one.
    pub urgency_override: Option<Urgency>,
    /// The caller's requested release time, epoch milliseconds UTC; `None` for an
    /// ordinary publish.
    ///
    /// Whether a value in the past parks or publishes immediately is decided at
    /// flush, against the clock read there.
    pub deliver_after: Option<u64>,
}

/// One accepted control op against a message this instance already parked, in
/// call order.
///
/// The message is named by identity, not by the index the component used: the
/// index is resolved against this activation's deferred snapshot at buffer time,
/// which is the only moment the two are known to mean the same thing. A message
/// that releases before the flush is then a lookup that finds nothing — the benign
/// race — rather than an index that silently addresses a different message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BufferedDeferOp {
    /// The instance's own output port name.
    pub port: String,
    /// The channel it resolved to, captured at buffer time for the same reason
    /// [`BufferedPublish::channel`] is: the resolution that authorized the op is
    /// the one that must apply it.
    pub channel: String,
    /// The parked message's identity, from the snapshot the component read.
    pub message_id: Uuid,
    pub kind: DeferOp,
}

/// One ok activation's buffered work, consuming the buffer: what it published,
/// what it did to its own schedule, and the budget it did not spend.
pub(crate) struct BufferedFlush {
    pub publishes: Vec<BufferedPublish>,
    /// Applied ahead of `publishes`: an op names a message parked by an earlier
    /// activation, so applying it first keeps this activation's own publishes out
    /// of its way.
    pub defer_ops: Vec<BufferedDeferOp>,
    pub carry: HashMap<String, u64>,
}

/// One output binding as the buffer needs it: where the port goes, what it costs
/// to send there, and what the port defaults to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputSpec {
    pub channel: String,
    pub default_urgency: Urgency,
}

/// The per-activation publish buffer and its budgets.
///
/// Not `Copy`/`Default`: a buffer only ever exists seeded, for exactly one
/// activation, and a buffer nobody seeded would silently enforce nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBuffer {
    /// This instance's bound outputs, keyed by port. An unknown key is
    /// `not-permitted` — the component named a port its config does not give it.
    outputs: HashMap<String, OutputSpec>,
    /// The shared per-activation gate: the counters, the per-port millitoken
    /// buckets, and the check order. Every budget answer this buffer gives comes
    /// from here, so a rule cannot hold on one hosting and not the other.
    gate: ActivationGate<String>,
    /// The identities of this instance's parked messages, per output port, in the
    /// order the activation's deferred windows presented them. An index a
    /// component names is resolved through this and nothing else: it is exactly
    /// what the component was shown, so a resolution against it cannot address a
    /// message the component never saw.
    ///
    /// An absent port has no parked messages, so every index is out of range
    /// there. A transportable port's ids come from the backend's pushed view, a
    /// confined port's from the page's own deferred set; the buffer does not
    /// distinguish them, because an op is applied at its channel's authority and
    /// the identity is what travels either way.
    deferred: HashMap<String, Vec<Uuid>>,
    /// Accepted publishes, in call order.
    entries: Vec<BufferedPublish>,
    /// Accepted control ops, in call order.
    defer_ops: Vec<BufferedDeferOp>,
}

impl PublishBuffer {
    /// Seed a buffer for one activation. `sink_mt` is already
    /// `seed_sink_budget`-folded by the caller (carry clamped, fill and input
    /// grant added) — this type spends budgets, it does not compute them.
    ///
    /// `max_body_bytes` is the surface's publish-body cap from `Welcome`, applied
    /// to every class, `local:` included: a component's body-size contract must
    /// not change because an operator rebound its output port, and the cap is
    /// what bounds the router's rings, which are page memory. A cap wider than
    /// the page's address space saturates to `usize::MAX` — the activation's own
    /// byte aggregate is the binding limit long before that.
    pub(crate) fn new(
        outputs: HashMap<String, OutputSpec>,
        sink_mt: HashMap<String, u64>,
        max_body_bytes: u64,
        deferred: HashMap<String, Vec<Uuid>>,
    ) -> Self {
        Self {
            outputs,
            gate: ActivationGate::new(
                usize::try_from(max_body_bytes).unwrap_or(usize::MAX),
                sink_mt,
            ),
            deferred,
            entries: Vec::new(),
            defer_ops: Vec::new(),
        }
    }

    /// What this activation's *transportable* work has already spent of the wire
    /// batch budget: every channel address and body it will carry, charged
    /// exactly as `brenn_attach_proto::batch_charged_bytes` charges the frame it
    /// composes into. Summed from the buffered work rather than accumulated, so
    /// the two cannot drift.
    ///
    /// Confined publishes are not charged: they never join a batch.
    fn batch_charge(&self) -> usize {
        let entries: usize = self
            .entries
            .iter()
            .filter(|entry| channel_is_transportable(&entry.channel))
            .map(|entry| entry.channel.len() + entry.body.len())
            .sum();
        let ops: usize = self
            .defer_ops
            .iter()
            .filter(|op| channel_is_transportable(&op.channel))
            .map(|op| {
                op.channel.len()
                    + match &op.kind {
                        DeferOp::Edit {
                            body: Some(body), ..
                        } => body.len(),
                        _ => 0,
                    }
            })
            .sum();
        entries + ops
    }

    /// Whether adding `charge` more charged bytes on `channel` would overrun the
    /// wire batch budget.
    ///
    /// The whole flush travels as one `PublishBatch`, and the protocol's legality
    /// law gives that frame one `max_body_bytes` budget across all of it. Without
    /// this check an activation the per-body and per-activation gates admit
    /// composes a flush the law refuses, and the refusal lands at the wire — the
    /// entire atomic flush dropped against a counter after every publish in it
    /// was answered ok. Asked here so the answer reaches the component that can
    /// act on it.
    ///
    /// Asked only of a transportable channel, and only of a body already inside
    /// the per-body cap, so the gate's `BodyTooLarge` still names an oversize
    /// argument rather than a budget.
    fn overruns_batch_budget(&self, channel: &str, charge: usize) -> bool {
        channel_is_transportable(channel)
            && self.batch_charge() + charge > self.gate.max_body_bytes()
    }

    /// Publish `body` from this instance's output `port`, at the port's
    /// configured default urgency. Buffered, not sent: it reaches the router or
    /// the wire only if this activation returns ok.
    pub fn publish(&mut self, port: &str, body: String) -> Result<(), PublishError> {
        self.publish_inner(port, body, None, None)
    }

    /// Publish held until `deliver_after` (epoch milliseconds UTC), at the port's
    /// configured default urgency.
    ///
    /// Buffered like every other publish, so an err or trap schedules nothing.
    /// Whether the release time has already passed is not this type's question:
    /// the buffer holds the caller's number and the flush compares it against the
    /// clock it reads there.
    pub fn publish_deferred(
        &mut self,
        port: &str,
        body: String,
        deliver_after: u64,
    ) -> Result<(), PublishError> {
        self.publish_inner(port, body, None, Some(deliver_after))
    }

    /// Publish at an explicit urgency, overriding the port's configured default
    /// for this one message.
    pub fn publish_with_urgency(
        &mut self,
        port: &str,
        body: String,
        urgency: Urgency,
    ) -> Result<(), PublishError> {
        self.publish_inner(port, body, Some(urgency), None)
    }

    /// The publish path both entry points share.
    ///
    /// The gate's publish order, with the page's one host-data check at its
    /// documented position: the call ceiling first (so a component looping on
    /// rejections pays for them), then the port binding — `not-permitted`, the
    /// only refusal this buffer decides on its own — then everything the gate
    /// judges, which it charges only on acceptance.
    fn publish_inner(
        &mut self,
        port: &str,
        body: String,
        urgency_override: Option<Urgency>,
        deliver_after: Option<u64>,
    ) -> Result<(), PublishError> {
        self.gate.charge_call().map_err(publish_error)?;
        // The binding's own key is the sink key, so nothing is allocated to ask
        // the gate: a refused publish copies neither the port name nor the
        // channel, and only an accepted one owns them.
        let Some((sink, spec)) = self.outputs.get_key_value(port) else {
            return Err(PublishError::NotPermitted);
        };
        let urgency = urgency_override.unwrap_or(spec.default_urgency);
        // The wire batch budget, before the gate charges anything: the gate
        // charges on acceptance and `take` asserts its count against this
        // buffer's, so a refusal after it would leave the two disagreeing.
        let charge = spec.channel.len() + body.len();
        if body.len() <= self.gate.max_body_bytes()
            && self.overruns_batch_budget(&spec.channel, charge)
        {
            return Err(PublishError::QuotaExceeded);
        }
        // A bound port always has a seeded bucket — the core seeds one per entry
        // of the same `outputs` map this resolved against — so the gate's miss
        // panic is a broken kernel invariant, not a component's problem.
        self.gate
            .admit_publish(PublishCheck {
                sink,
                body_len: body.len(),
                deliver_after,
                entry_addend: 0,
            })
            .map_err(publish_error)?;
        self.entries.push(BufferedPublish {
            port: port.to_string(),
            channel: spec.channel.clone(),
            body,
            urgency,
            urgency_override,
            deliver_after,
        });
        Ok(())
    }

    /// Cancel one message this instance has parked on `port`'s channel, named by
    /// its `index` into the deferred window this activation was handed.
    ///
    /// Buffered like a publish, so an err or trap cancels nothing.
    pub fn defer_cancel(&mut self, port: &str, index: u32) -> Result<(), DeferError> {
        self.defer_inner(port, index, DeferOp::Cancel)
    }

    /// Rewrite one message this instance has parked on `port`'s channel — its
    /// body, its release time, or both; `None` leaves that half alone.
    ///
    /// A release time already past does not publish here: the edit is applied at
    /// flush and the message releases at the next release pass, which is the same
    /// answer a `publish_deferred` with a past time gets.
    pub fn defer_edit(
        &mut self,
        port: &str,
        index: u32,
        body: Option<String>,
        deliver_after: Option<u64>,
    ) -> Result<(), DeferError> {
        self.defer_inner(
            port,
            index,
            DeferOp::Edit {
                body,
                deliver_after,
            },
        )
    }

    /// The control-op path both entry points share.
    ///
    /// The gate's control-op order, with the page's two host-data checks at their
    /// documented positions: representability first (a fact about the argument,
    /// not about any budget, and so the one refusal here that draws no call
    /// slot), then the shared call count, then the port binding and the snapshot
    /// index — the two answers only this buffer holds — then the gate's op
    /// ceiling and edit-body rules.
    ///
    /// An edit body is weighed exactly as a published body is, against the same
    /// cap and the same per-activation aggregate. The page needs that as more than
    /// hygiene: a page edit travels to the server as a control op, where an
    /// oversize body is a protocol violation that kills the connection, so
    /// refusing it here keeps a conforming kernel from ever wiring one.
    fn defer_inner(&mut self, port: &str, index: u32, kind: DeferOp) -> Result<(), DeferError> {
        let edit_deliver_after = match &kind {
            DeferOp::Edit { deliver_after, .. } => *deliver_after,
            DeferOp::Cancel => None,
        };
        check_deliver_after(edit_deliver_after).map_err(defer_error)?;
        self.gate.charge_call().map_err(defer_error)?;
        let Some(spec) = self.outputs.get(port) else {
            return Err(DeferError::NotPermitted);
        };
        let Some(&message_id) = self
            .deferred
            .get(port)
            .and_then(|ids| ids.get(index as usize))
        else {
            return Err(DeferError::OutOfRange);
        };
        let edit_body_len = match &kind {
            DeferOp::Edit {
                body: Some(body), ..
            } => Some(body.len()),
            _ => None,
        };
        // The wire batch budget, as on the publish path: an op names a channel
        // and an edit carries a body, and the legality law charges both.
        let charge = spec.channel.len() + edit_body_len.unwrap_or(0);
        if edit_body_len.is_none_or(|len| len <= self.gate.max_body_bytes())
            && self.overruns_batch_budget(&spec.channel, charge)
        {
            return Err(DeferError::QuotaExceeded);
        }
        self.gate.admit_op(edit_body_len).map_err(defer_error)?;
        self.defer_ops.push(BufferedDeferOp {
            port: port.to_string(),
            channel: spec.channel.clone(),
            message_id,
            kind,
        });
        Ok(())
    }

    /// How many publishes were accepted. Read by the driver to mint one envelope
    /// stamp per entry before it hands the buffer back to the core. Control ops
    /// mint no envelope — they act on messages already minted — so they are not
    /// counted here.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    /// The buffered publishes and control ops in call order, plus the leftover
    /// per-port millitokens, consuming the buffer. Called by the core on an ok
    /// completion; on err or trap the buffer is simply dropped, work and all.
    ///
    /// Carryover returns even though the work does not: what a component *spent*
    /// is a fact about the activation that happened, and an err does not un-spend
    /// it. The core clamps the carry to `capacity_mt` when it seeds the next
    /// activation.
    pub(crate) fn take(self) -> BufferedFlush {
        // The gate counts the entry ceiling, this vector holds the entries: a push
        // that skipped `admit_publish` would let the two disagree and the ceiling
        // would bound nothing.
        assert_eq!(
            self.gate.entries(),
            self.entries.len(),
            "every buffered publish must have been admitted by the gate"
        );
        BufferedFlush {
            publishes: self.entries,
            defer_ops: self.defer_ops,
            carry: self.gate.into_sinks(),
        }
    }

    /// The leftover per-port millitokens without the entries — the err/trap path,
    /// where the buffer is discarded but the spending still happened.
    pub(crate) fn into_carry(self) -> HashMap<String, u64> {
        self.gate.into_sinks()
    }
}

#[cfg(test)]
mod tests {
    use brenn_attach_proto::{BatchEntry, check_batch_legality};

    use super::*;

    const WIRE: &str = "brenn:site.bar.out";
    const LOCAL: &str = "local:page/notes";

    fn spec(channel: &str) -> OutputSpec {
        OutputSpec {
            channel: channel.to_string(),
            default_urgency: Urgency::Normal,
        }
    }

    /// A buffer bound to one wire port and one confined one, with sink budgets
    /// wide enough that only the rules under test can refuse anything.
    fn buffer(max_body_bytes: u64) -> PublishBuffer {
        let outputs = HashMap::from([
            ("out".to_string(), spec(WIRE)),
            ("notes".to_string(), spec(LOCAL)),
        ]);
        let sink_mt = HashMap::from([
            ("out".to_string(), 1_000_000),
            ("notes".to_string(), 1_000_000),
        ]);
        PublishBuffer::new(outputs, sink_mt, max_body_bytes, HashMap::new())
    }

    /// The whole point of the buffer-time rule: entries each well inside the body
    /// cap that together overrun the batch budget are refused *here*, where the
    /// component gets an answer, instead of at the wire where the entire atomic
    /// flush would be dropped against a counter after every publish was told ok.
    #[test]
    fn a_publish_that_would_overrun_the_batch_budget_is_refused_at_the_call() {
        let mut buf = buffer(64);
        assert_eq!(buf.publish("out", "a".repeat(20)), Ok(()));
        assert_eq!(
            buf.publish("out", "b".repeat(20)),
            Err(PublishError::QuotaExceeded),
            "18 channel bytes + 20 body bytes, twice, is 76 against a 64-byte budget"
        );
        assert_eq!(buf.len(), 1, "the refused publish buffers nothing");
    }

    /// What the buffer accepted must compose a batch the protocol's law admits.
    /// This is the contract the rule exists to hold; asserting it against the law
    /// itself is what keeps the two from drifting.
    #[test]
    fn everything_the_buffer_accepts_composes_a_legal_batch() {
        let cap = 128;
        let mut buf = buffer(cap as u64);
        // Publish until refused, then check the survivors against the law.
        for i in 0..64 {
            if buf.publish("out", format!("body-{i}")).is_err() {
                break;
            }
        }
        assert!(buf.len() > 1, "the fixture must admit more than one entry");
        let flushed = buf.take();
        let entries: Vec<BatchEntry> = flushed
            .publishes
            .into_iter()
            .map(|p| BatchEntry {
                channel: p.channel,
                body: p.body,
                urgency: p.urgency,
                deliver_after: p.deliver_after,
            })
            .collect();
        assert_eq!(check_batch_legality(&entries, &[], cap), Ok(()));
    }

    /// A body at exactly the advertised cap cannot survive a batch, because the
    /// channel address is charged beside it. The component learns that at the
    /// call rather than from a drop counter.
    #[test]
    fn a_body_at_the_cap_on_a_wire_port_is_refused_for_the_channel_bytes() {
        let mut buf = buffer(64);
        assert_eq!(
            buf.publish("out", "a".repeat(64)),
            Err(PublishError::QuotaExceeded)
        );
    }

    /// An oversize body is still the gate's answer, not the budget's: it is a
    /// fact about the argument, and `invalid-payload` says so where
    /// `quota-exceeded` would tell the component to retry later.
    #[test]
    fn an_oversize_body_is_still_an_invalid_payload() {
        let mut buf = buffer(64);
        assert_eq!(
            buf.publish("out", "a".repeat(65)),
            Err(PublishError::InvalidPayload)
        );
    }

    /// A confined publish never joins a batch, so it is charged nothing and
    /// refuses nothing.
    #[test]
    fn confined_publishes_are_not_charged_against_the_batch_budget() {
        let mut buf = buffer(64);
        for i in 0..8 {
            assert_eq!(buf.publish("notes", format!("note-{i}")), Ok(()));
        }
        assert_eq!(buf.len(), 8);
        // And the wire port still has its whole budget.
        assert_eq!(buf.publish("out", "a".repeat(40)), Ok(()));
    }

    /// A control op names a channel and an edit carries a body; the law charges
    /// both, so this gate must too.
    #[test]
    fn control_ops_spend_the_same_budget_as_publishes() {
        let mut buf = PublishBuffer::new(
            HashMap::from([("out".to_string(), spec(WIRE))]),
            HashMap::from([("out".to_string(), 1_000_000)]),
            64,
            HashMap::from([(
                "out".to_string(),
                vec![Uuid::from_u128(1), Uuid::from_u128(2)],
            )]),
        );
        assert_eq!(buf.defer_cancel("out", 0), Ok(()));
        assert_eq!(
            buf.defer_edit("out", 1, Some("x".repeat(30)), None),
            Err(DeferError::QuotaExceeded),
            "18 + 18 + 30 charged bytes against a 64-byte budget"
        );
    }
}

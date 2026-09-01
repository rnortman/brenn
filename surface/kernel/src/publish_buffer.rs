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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use brenn_budget::{
    ActivationGate, GateRefusal, PublishCheck, RefusalKind, check_deliver_after,
    defer_refusal_kind, publish_refusal_kind,
};
use brenn_envelope::Urgency;
use brenn_surface_contract::{DeferError, PublishError};
use uuid::Uuid;

use brenn_attach_client::store::DeferOp;

use crate::bindings::channel_is_transportable;
use crate::outbound::truncate_report_field;

/// The byte cap on a guest-supplied port name reaching a diagnostic. The name is
/// component-controlled and the diagnostic reaches the console and a log frame,
/// so it is bounded here rather than at the reader.
const MAX_PORT_NAME_REPORT_BYTES: usize = 256;

/// A port call the buffer did not accept.
///
/// Two outcomes with different consequences, which is why they are one type and
/// not one error code. A [`Refused`](Self::Refused) call is answered in the
/// contract's own vocabulary and the component carries on. An
/// [`Undeclared`](Self::Undeclared) call is the component publishing to a name
/// its specification does not contain — the artifact is hash-bound to that
/// specification, so the call is not an error the component gets to read, and the
/// seam that made it ends the activation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortFault<E> {
    /// The contract's error vocabulary, as the component sees it.
    Refused(E),
    /// The offending port name, already capped and debug-escaped for a
    /// diagnostic. Rendered into a trap reason by the seam, which is what knows
    /// the instance it is speaking for.
    Undeclared(String),
}

/// A refused or violating publish. See [`PortFault`].
pub type PublishFault = PortFault<PublishError>;

/// A refused or violating deferred-message control op. See [`PortFault`].
pub type DeferFault = PortFault<DeferError>;

/// Cap and debug-escape a component-supplied port name for a diagnostic.
///
/// `{:?}` quotes and escapes, so control characters and ANSI sequences reach the
/// console and the log frame as text; the crate's one truncation policy bounds
/// the length.
fn port_for_report(port: &str) -> String {
    truncate_report_field(format!("{port:?}"), MAX_PORT_NAME_REPORT_BYTES)
}

/// A gate verdict as the component sees it on the publish path.
///
/// The classification is [`brenn_budget::publish_refusal_kind`], shared with the
/// backend host.
fn publish_error(refusal: GateRefusal) -> PublishFault {
    PortFault::Refused(match publish_refusal_kind(refusal) {
        RefusalKind::InvalidPayload(_) => PublishError::InvalidPayload,
        RefusalKind::QuotaExceeded => PublishError::QuotaExceeded,
        RefusalKind::InvalidDeliverAfter => {
            unreachable!("publish_refusal_kind never answers invalid-deliver-after")
        }
    })
}

/// A gate verdict as the component sees it on the control-op path.
///
/// The classification is [`brenn_budget::defer_refusal_kind`], shared with the
/// backend host.
fn defer_error(refusal: GateRefusal) -> DeferFault {
    PortFault::Refused(match defer_refusal_kind(refusal) {
        RefusalKind::InvalidDeliverAfter => DeferError::InvalidDeliverAfter,
        RefusalKind::QuotaExceeded => DeferError::QuotaExceeded,
        RefusalKind::InvalidPayload(_) => {
            unreachable!("defer_refusal_kind never answers invalid-payload")
        }
    })
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

/// What became of a publish the buffer accepted.
///
/// The component sees one answer for both — wiring is the deployer's business
/// and a component does not read topology out of its own publish results — so
/// this exists for the host side of the seam, which counts the two apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Queued for the flush: the port is bound and the message has a sink.
    Buffered,
    /// Accepted and discarded: the port is declared and nobody wired it.
    Dropped,
}

impl Admission {
    /// Read the verdict off the buffer's drop count either side of one publish.
    pub fn of(before: usize, after: usize) -> Self {
        if after > before {
            Self::Dropped
        } else {
            Self::Buffered
        }
    }
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
    /// This instance's bound outputs, keyed by port. An unknown key is not
    /// automatically a refusal: it is judged against
    /// [`declared_out_ports`](Self::declared_out_ports) first.
    outputs: HashMap<String, OutputSpec>,
    /// Every port name this instance's specification declares it may publish to,
    /// wired or not. A name in here that `outputs` does not hold is a declared
    /// port the deployer left unwired — the publish succeeds and the message is
    /// dropped. A name outside it is the component contradicting its own
    /// specification.
    declared_out_ports: Arc<BTreeSet<String>>,
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
        declared_out_ports: Arc<BTreeSet<String>>,
        sink_mt: HashMap<String, u64>,
        max_body_bytes: u64,
        deferred: HashMap<String, Vec<Uuid>>,
    ) -> Self {
        Self {
            outputs,
            declared_out_ports,
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

    /// Publishes this activation that were accepted and dropped for want of a
    /// sink — a declared port the deployer left unwired. Counted by the gate,
    /// which is the only thing that can admit one; the caller of a publish reads
    /// this across the call to learn whether its own message was the drop, since
    /// the component is deliberately told nothing.
    pub fn dropped(&self) -> usize {
        self.gate.dropped()
    }

    /// Publish `body` from this instance's output `port`, at the port's
    /// configured default urgency. Buffered, not sent: it reaches the router or
    /// the wire only if this activation returns ok.
    pub fn publish(&mut self, port: &str, body: String) -> Result<(), PublishFault> {
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
    ) -> Result<(), PublishFault> {
        self.publish_inner(port, body, None, Some(deliver_after))
    }

    /// Publish at an explicit urgency, overriding the port's configured default
    /// for this one message.
    pub fn publish_with_urgency(
        &mut self,
        port: &str,
        body: String,
        urgency: Urgency,
    ) -> Result<(), PublishFault> {
        self.publish_inner(port, body, Some(urgency), None)
    }

    /// The publish path both entry points share.
    ///
    /// The gate's publish order, with the page's one host-data check at its
    /// documented position: the call ceiling first (so a component looping on
    /// rejections pays for them), then the port — three-way, per
    /// [`Self::publish_unwired`] — then everything the gate judges, which it
    /// charges only on acceptance.
    fn publish_inner(
        &mut self,
        port: &str,
        body: String,
        urgency_override: Option<Urgency>,
        deliver_after: Option<u64>,
    ) -> Result<(), PublishFault> {
        self.gate.charge_call().map_err(publish_error)?;
        // The binding's own key is the sink key, so nothing is allocated to ask
        // the gate: a refused publish copies neither the port name nor the
        // channel, and only an accepted one owns them.
        let Some((sink, spec)) = self.outputs.get_key_value(port) else {
            return self.publish_unwired(port, &body, deliver_after);
        };
        let urgency = urgency_override.unwrap_or(spec.default_urgency);
        // The wire batch budget, before the gate charges anything: the gate
        // charges on acceptance and `take` asserts its count against this
        // buffer's, so a refusal after it would leave the two disagreeing.
        let charge = spec.channel.len() + body.len();
        if body.len() <= self.gate.max_body_bytes()
            && self.overruns_batch_budget(&spec.channel, charge)
        {
            return Err(PortFault::Refused(PublishError::QuotaExceeded));
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

    /// The publish path for a port with no binding: either a declared port the
    /// deployer left unwired, or a name outside the specification entirely.
    ///
    /// A declared port nobody wired is a sink with no channel, and publishing to
    /// nowhere is as legal here as publishing to a channel with no subscribers.
    /// The component sees `Ok` and the message is dropped: wiring is the
    /// deployer's business, and a component does not get to read deployment
    /// topology out of error codes.
    ///
    /// Not a short-circuit, for that same reason. Everything a bound publish
    /// validates that does not need a sink still runs, in the same order and
    /// charging the same activation-wide counters — the body cap, `deliver-after`
    /// representability, the buffered-entry ceiling and the byte aggregate. Only
    /// what exists to serve a sink is skipped: the per-sink bucket, the wire
    /// batch budget, the buffer and the carry. Answering `Ok` where a bound port
    /// answers `invalid-payload` would hand the component its own wiring back.
    fn publish_unwired(
        &mut self,
        port: &str,
        body: &str,
        deliver_after: Option<u64>,
    ) -> Result<(), PublishFault> {
        if !self.declared_out_ports.contains(port) {
            return Err(PortFault::Undeclared(port_for_report(port)));
        }
        self.gate
            .admit_publish_without_sink(body.len(), deliver_after, 0)
            .map_err(publish_error)?;
        Ok(())
    }

    /// Cancel one message this instance has parked on `port`'s channel, named by
    /// its `index` into the deferred window this activation was handed.
    ///
    /// Buffered like a publish, so an err or trap cancels nothing.
    pub fn defer_cancel(&mut self, port: &str, index: u32) -> Result<(), DeferFault> {
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
    ) -> Result<(), DeferFault> {
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
    /// slot), then the shared call count, then the port and the snapshot
    /// index — the two answers only this buffer holds — then the gate's op
    /// ceiling and edit-body rules.
    ///
    /// The port is judged three ways as it is on the publish path, but the
    /// declared-unwired arm needs no drop semantics of its own: a port with no
    /// binding has no deferred window, so every index is out of range there and
    /// the ordinary index refusal is the honest answer.
    ///
    /// An edit body is weighed exactly as a published body is, against the same
    /// cap and the same per-activation aggregate. The page needs that as more than
    /// hygiene: a page edit travels to the server as a control op, where an
    /// oversize body is a protocol violation that kills the connection, so
    /// refusing it here keeps a conforming kernel from ever wiring one.
    fn defer_inner(&mut self, port: &str, index: u32, kind: DeferOp) -> Result<(), DeferFault> {
        let edit_deliver_after = match &kind {
            DeferOp::Edit { deliver_after, .. } => *deliver_after,
            DeferOp::Cancel => None,
        };
        check_deliver_after(edit_deliver_after).map_err(defer_error)?;
        self.gate.charge_call().map_err(defer_error)?;
        let Some(spec) = self.outputs.get(port) else {
            if !self.declared_out_ports.contains(port) {
                return Err(PortFault::Undeclared(port_for_report(port)));
            }
            return Err(PortFault::Refused(DeferError::OutOfRange));
        };
        let Some(&message_id) = self
            .deferred
            .get(port)
            .and_then(|ids| ids.get(index as usize))
        else {
            return Err(PortFault::Refused(DeferError::OutOfRange));
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
            return Err(PortFault::Refused(DeferError::QuotaExceeded));
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
        // The gate counts the entry ceiling, this vector holds the entries, and a
        // publish dropped for want of a sink is counted by the gate while holding
        // no entry: a push that skipped the gate would let the two disagree and
        // the ceiling would bound nothing.
        assert_eq!(
            self.gate.entries(),
            self.entries.len() + self.gate.dropped(),
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

    /// A declared but deliberately unwired port: in the vocabulary, absent from
    /// the bound-output table.
    const UNWIRED: &str = "spare";
    /// A name the fixture's vocabulary does not contain at all.
    const UNDECLARED: &str = "ghost";

    /// The fixture's declared vocabulary: its two bound ports plus [`UNWIRED`].
    fn vocabulary() -> Arc<BTreeSet<String>> {
        Arc::new(
            ["out", "notes", UNWIRED]
                .into_iter()
                .map(str::to_string)
                .collect(),
        )
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
        PublishBuffer::new(
            outputs,
            vocabulary(),
            sink_mt,
            max_body_bytes,
            HashMap::new(),
        )
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
            Err(PortFault::Refused(PublishError::QuotaExceeded)),
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
            Err(PortFault::Refused(PublishError::QuotaExceeded))
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
            Err(PortFault::Refused(PublishError::InvalidPayload))
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

    /// The middle arm of the three-way rule: a port the specification declares
    /// and the deployer did not wire is a sink with no channel. The component is
    /// told ok and the message goes nowhere.
    #[test]
    fn a_declared_unwired_port_drops_the_message_and_answers_ok() {
        let mut buf = buffer(1_024);
        assert_eq!(buf.publish(UNWIRED, "hi".into()), Ok(()));
        assert_eq!(buf.len(), 0, "a drop buffers nothing");
        let flush = buf.take();
        assert!(flush.publishes.is_empty());
        assert!(
            !flush.carry.contains_key(UNWIRED),
            "a port with no sink carries nothing"
        );
    }

    /// The drop path is not a short-circuit: every check that does not need a
    /// sink answers exactly as it would on the bound port beside it, or the
    /// component reads its own wiring out of the difference.
    #[test]
    fn the_drop_path_validates_exactly_as_the_bound_path_does() {
        let mut bound = buffer(64);
        let mut unwired = buffer(64);
        // Pinned as concrete verdicts, not merely as equal ones: two paths that
        // both regressed to `Ok` would agree with each other and lose the
        // guarantee.
        assert_eq!(
            unwired.publish(UNWIRED, "a".repeat(65)),
            Err(PortFault::Refused(PublishError::InvalidPayload)),
            "an oversize body is invalid-payload on the unwired port"
        );
        assert_eq!(
            bound.publish("out", "a".repeat(65)),
            Err(PortFault::Refused(PublishError::InvalidPayload)),
            "and the same on the bound port beside it"
        );
        assert_eq!(
            unwired.publish_deferred(UNWIRED, "hi".into(), u64::MAX),
            Err(PortFault::Refused(PublishError::InvalidPayload)),
            "an unrepresentable release time is an invalid payload on the unwired port"
        );
        assert_eq!(
            bound.publish_deferred("out", "hi".into(), u64::MAX),
            Err(PortFault::Refused(PublishError::InvalidPayload)),
            "and the same on the bound port beside it"
        );
    }

    /// A deferred publish to an unwired declared port takes the same drop as the
    /// immediate one: ok to the component, nothing buffered, and no park left
    /// scheduled against a port that has no channel to release onto.
    #[test]
    fn a_declared_unwired_port_drops_a_deferred_publish_and_answers_ok() {
        let mut buf = buffer(1_024);
        assert_eq!(buf.publish_deferred(UNWIRED, "hi".into(), 60_000), Ok(()));
        assert_eq!(buf.len(), 0, "a drop buffers nothing");
        assert_eq!(buf.dropped(), 1);
        let flush = buf.take();
        assert!(flush.publishes.is_empty(), "nothing to release");
        assert!(flush.defer_ops.is_empty(), "and no control op either");
        assert!(!flush.carry.contains_key(UNWIRED));
    }

    /// A dropped publish charges the activation's entry ceiling, so the ceiling
    /// cannot become the channel that reports wiring.
    #[test]
    fn a_dropped_publish_spends_the_entry_ceiling() {
        let mut buf = buffer(1_024);
        for _ in 0..brenn_budget::MAX_PUBLISHES_PER_ACTIVATION {
            assert_eq!(buf.publish(UNWIRED, "hi".into()), Ok(()));
        }
        assert_eq!(
            buf.publish("notes", "hi".into()),
            Err(PortFault::Refused(PublishError::QuotaExceeded)),
            "the drops filled the ceiling a bound publish answers to"
        );
    }

    /// The count the port seam reads across a publish to tell a drop from a
    /// queued message — the component is told the same thing either way, so
    /// nothing else can distinguish them. A refused sink-less publish is not a
    /// drop: nothing was accepted.
    #[test]
    fn the_buffer_counts_the_publishes_it_dropped() {
        let mut buf = buffer(64);
        assert_eq!(buf.dropped(), 0);
        assert_eq!(buf.publish("out", "hi".into()), Ok(()));
        assert_eq!(buf.dropped(), 0, "a bound publish is queued, not dropped");
        assert_eq!(buf.publish(UNWIRED, "hi".into()), Ok(()));
        assert_eq!(buf.dropped(), 1);
        assert_eq!(
            buf.publish(UNWIRED, "a".repeat(65)),
            Err(PortFault::Refused(PublishError::InvalidPayload)),
            "an oversize body is refused before anything is dropped"
        );
        assert_eq!(buf.dropped(), 1);
        assert_eq!(
            Admission::of(0, buf.dropped()),
            Admission::Dropped,
            "the seam reads the verdict off the count"
        );
        assert_eq!(Admission::of(1, buf.dropped()), Admission::Buffered);
    }

    /// The third arm: a name outside the vocabulary is the component
    /// contradicting the specification its artifact is hash-bound to. Not an
    /// error code — the seam ends the activation for it.
    #[test]
    fn an_undeclared_port_is_a_violation_not_a_refusal() {
        let mut buf = buffer(1_024);
        assert_eq!(
            buf.publish(UNDECLARED, "hi".into()),
            Err(PortFault::Undeclared("\"ghost\"".to_string()))
        );
        assert_eq!(
            buf.publish_deferred(UNDECLARED, "hi".into(), 1),
            Err(PortFault::Undeclared("\"ghost\"".to_string()))
        );
        assert_eq!(buf.len(), 0);
    }

    /// The port name reaches a diagnostic, so it is capped and escaped there and
    /// not at the reader.
    #[test]
    fn a_hostile_port_name_is_capped_and_escaped_in_the_diagnostic() {
        let mut buf = buffer(1_024);
        let hostile = format!("\u{1b}[31m{}", "x".repeat(MAX_PORT_NAME_REPORT_BYTES * 2));
        let Err(PortFault::Undeclared(report)) = buf.publish(&hostile, "hi".into()) else {
            panic!("an undeclared port is a violation");
        };
        assert!(
            report.len() <= MAX_PORT_NAME_REPORT_BYTES,
            "{}",
            report.len()
        );
        assert!(!report.contains('\u{1b}'), "the escape is not literal");
    }

    /// The call ceiling is charged before the port is looked at, so a component
    /// looping on violations pays for the loop.
    #[test]
    fn the_call_ceiling_answers_before_the_vocabulary_does() {
        let mut buf = buffer(1_024);
        for _ in 0..brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION {
            let _ = buf.publish(UNDECLARED, "hi".into());
        }
        assert_eq!(
            buf.publish(UNDECLARED, "hi".into()),
            Err(PortFault::Refused(PublishError::QuotaExceeded))
        );
    }

    /// A declared port nobody wired has no deferred window, so every index into
    /// it is out of range — the honest answer, and the same one a bound port with
    /// an empty window gives.
    #[test]
    fn a_control_op_on_a_declared_unwired_port_is_out_of_range() {
        let mut buf = buffer(1_024);
        assert_eq!(
            buf.defer_cancel(UNWIRED, 0),
            Err(PortFault::Refused(DeferError::OutOfRange))
        );
        assert_eq!(
            buf.defer_edit(UNWIRED, 0, Some("x".into()), None),
            Err(PortFault::Refused(DeferError::OutOfRange))
        );
    }

    /// A control op naming a port outside the vocabulary is the same violation a
    /// publish to it is.
    #[test]
    fn a_control_op_on_an_undeclared_port_is_a_violation() {
        let mut buf = buffer(1_024);
        assert_eq!(
            buf.defer_cancel(UNDECLARED, 0),
            Err(PortFault::Undeclared("\"ghost\"".to_string()))
        );
    }

    /// A control op names a channel and an edit carries a body; the law charges
    /// both, so this gate must too.
    #[test]
    fn control_ops_spend_the_same_budget_as_publishes() {
        let mut buf = PublishBuffer::new(
            HashMap::from([("out".to_string(), spec(WIRE))]),
            vocabulary(),
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
            Err(PortFault::Refused(DeferError::QuotaExceeded)),
            "18 + 18 + 30 charged bytes against a 64-byte budget"
        );
    }
}

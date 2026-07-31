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
//! through. What is here is the page's own half — the port table, the deferred
//! snapshot, the buffered entries — plus the mapping from the gate's verdicts
//! onto the contract's error vocabulary.

use std::collections::HashMap;

use brenn_budget::{ActivationGate, GateRefusal, PublishCheck, check_deliver_after};
use brenn_envelope::Urgency;
use brenn_surface_contract::{DeferError, PublishError};
use uuid::Uuid;

use brenn_attach_client::store::DeferOp;

/// A gate verdict as the component sees it on the publish path.
///
/// An oversize body and an impossible release time are facts about the
/// argument — `invalid-payload`; everything else is a budget — `quota-exceeded`.
fn publish_error(refusal: GateRefusal) -> PublishError {
    match refusal {
        GateRefusal::BodyTooLarge { .. } | GateRefusal::UnrepresentableDeliverAfter { .. } => {
            PublishError::InvalidPayload
        }
        GateRefusal::CallCap { .. }
        | GateRefusal::SinkExhausted
        | GateRefusal::EntryCap { .. }
        | GateRefusal::ByteCap { .. }
        | GateRefusal::OpCap { .. } => PublishError::QuotaExceeded,
    }
}

/// A gate verdict as the component sees it on the control-op path.
///
/// An impossible release time has its own variant here — the op is otherwise
/// well-formed, and collapsing it into a quota refusal would tell the component
/// to retry later. An oversize edit body *is* a quota refusal, because the body
/// is charged against the activation's aggregate like any other.
fn defer_error(refusal: GateRefusal) -> DeferError {
    match refusal {
        GateRefusal::UnrepresentableDeliverAfter { .. } => DeferError::InvalidDeliverAfter,
        GateRefusal::CallCap { .. }
        | GateRefusal::BodyTooLarge { .. }
        | GateRefusal::SinkExhausted
        | GateRefusal::EntryCap { .. }
        | GateRefusal::ByteCap { .. }
        | GateRefusal::OpCap { .. } => DeferError::QuotaExceeded,
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

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
//! [`brenn_budget::PublishRefusalKind`] and [`brenn_budget::DeferRefusalKind`].
//! What is here is the page's own half — the port table, the deferred snapshot,
//! the buffered entries — plus the last step onto the contract's error
//! vocabulary.

#[cfg(test)]
mod attachment_tests;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use brenn_budget::{
    ActivationGate, DeferRefusalKind, GateRefusal, PublishCheck, PublishRefusalKind,
    check_deliver_after, defer_refusal_kind, publish_refusal_kind,
};
use brenn_envelope::{
    AttachmentListError, AttachmentName, GuestBody, MessageBody, Urgency, check_attachment_list,
};
use brenn_surface_contract::{CallError, DeferError, PublishError};
use uuid::Uuid;

use brenn_attach_client::Codec;
use brenn_attach_client::store::DeferOp;
use brenn_attach_proto::{BlobFacts, BlobHandle, PublishAttachment};
use brenn_surface_schema::SURFACE_ATTACH_VERSION;

use crate::bindings::{channel_carries, channel_is_transportable};
use crate::blob_scope::{Blob, BlobScope, UnknownHandle, blobs_bytes, distinct_blobs, door_holds};
use crate::outbound::truncate_report_field;

/// The most distinct blobs one activation's wire-bound publishes name, together.
/// The door's `max_handles` bounds it further.
pub const SURFACE_MAX_FLUSH_BLOBS: usize = 32;

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

/// What one buffered operation on a transportable `channel` charges against
/// the wire batch budget, by the codec of the version the surface route serves.
fn wire_charge(channel: &str, body: Option<&MessageBody>) -> usize {
    Codec::negotiated(SURFACE_ATTACH_VERSION).op_charged_bytes(channel, body)
}

/// What a publish's attachments charge against the wire batch budget, by the
/// same codec. The charge reads only the names, so the handle each attachment
/// will carry on the wire is not yet needed and a placeholder stands in.
fn attachments_charge(attachments: &[BufferedAttachment]) -> usize {
    let placeholders: Vec<PublishAttachment> = attachments
        .iter()
        .map(|attachment| PublishAttachment::new(attachment.name.clone(), BlobHandle(0)))
        .collect();
    Codec::negotiated(SURFACE_ATTACH_VERSION).attachments_charged_bytes(&placeholders)
}

/// `blobs.publish`'s `publish-error`: a `ports.publish` refusal, or one of the
/// attachment list's own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobsPublishError {
    Plain(PublishError),
    InvalidAttachmentName(String),
    DuplicateAttachmentName(String),
    TooManyAttachments,
}

/// A refused or violating `blobs.publish`. See [`PortFault`].
pub type BlobsPublishFault = PortFault<BlobsPublishError>;

/// Whether the caller of [`PublishBuffer::publish_attached`] holds every grant
/// the call needs, as the core judged it. The buffer does not judge grants; it
/// answers this verdict at its place in the check order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantVerdict {
    Held,
    Missing,
}

impl GrantVerdict {
    /// The verdict of a grant judgement: `Held` iff it refused nothing.
    pub fn of<R>(grants: &Result<(), R>) -> Self {
        match grants {
            Ok(()) => Self::Held,
            Err(_) => Self::Missing,
        }
    }
}

/// The answer to a `blobs.publish` call: `None` when no activation of the
/// caller is in flight, else the buffer's answer.
pub type BlobsPublishAnswer = Option<Result<Result<Admission, BlobsPublishFault>, UnknownHandle>>;

/// The grant refusal a `blobs.publish` call reports, given the judgement it was
/// made under and its answer.
///
/// A missing grant is reported when the call was answered `not-permitted`, by
/// the buffer or because no activation of the caller is in flight; it is not
/// reported when the call was answered `quota-exceeded`, because the charge came
/// first and the grant was never judged. A call that held every grant reports
/// nothing.
///
/// # Panics
///
/// On any other answer under a missing grant: [`PublishBuffer::publish_attached`]
/// answers a missing verdict `not-permitted` or `quota-exceeded` and nothing else.
pub fn ungranted_report<'a, R>(
    instance: &str,
    grants: &'a Result<(), R>,
    answer: &BlobsPublishAnswer,
) -> Option<&'a R> {
    let Err(refusal) = grants else {
        return None;
    };
    match answer {
        Some(Ok(Err(PortFault::Refused(BlobsPublishError::Plain(PublishError::NotPermitted)))))
        | None => Some(refusal),
        Some(Ok(Err(PortFault::Refused(BlobsPublishError::Plain(
            PublishError::QuotaExceeded,
        ))))) => None,
        other => panic!(
            "surface kernel: the buffer answered '{instance}''s ungranted blobs.publish \
             {other:?}; it answers a missing grant `not-permitted` or `quota-exceeded`"
        ),
    }
}

/// A `ports.publish` fault as `blobs.publish` answers it.
fn plain_fault(fault: PublishFault) -> BlobsPublishFault {
    match fault {
        PortFault::Refused(err) => PortFault::Refused(BlobsPublishError::Plain(err)),
        PortFault::Undeclared(port) => PortFault::Undeclared(port),
    }
}

/// The attachment list's names, checked as every host checks them.
fn check_attachment_names(
    attachments: &[(String, u32)],
) -> Result<Vec<AttachmentName>, BlobsPublishError> {
    check_attachment_list(attachments.iter().map(|(name, _)| name.as_str())).map_err(|refusal| {
        match refusal {
            AttachmentListError::TooMany => BlobsPublishError::TooManyAttachments,
            AttachmentListError::InvalidName(why) => BlobsPublishError::InvalidAttachmentName(why),
            AttachmentListError::DuplicateName(name) => {
                BlobsPublishError::DuplicateAttachmentName(name)
            }
        }
    })
}

/// The blobs `names` name, each resolved through `scope`; a handle the scope
/// does not hold as a blob is the caller's trap.
fn resolve_attachments(
    scope: &BlobScope,
    names: Vec<AttachmentName>,
    attachments: &[(String, u32)],
) -> Result<Vec<BufferedAttachment>, UnknownHandle> {
    names
        .into_iter()
        .zip(attachments)
        .map(|(name, &(_, handle))| {
            Ok(BufferedAttachment {
                name,
                blob: scope.blob(handle)?.clone(),
            })
        })
        .collect()
}

/// A gate verdict as the component sees it on the publish path.
///
/// The classification is [`brenn_budget::publish_refusal_kind`], shared with the
/// backend host.
fn publish_error(refusal: GateRefusal) -> PublishFault {
    PortFault::Refused(match publish_refusal_kind(refusal) {
        PublishRefusalKind::InvalidPayload(fault) => {
            PublishError::InvalidPayload(fault.to_string())
        }
        PublishRefusalKind::QuotaExceeded => PublishError::QuotaExceeded,
    })
}

/// A component body built into the body the buffer holds, or the refusal its
/// malformation draws, spelled by `invalid_body`.
fn build_body<E>(
    body: GuestBody,
    invalid_body: impl FnOnce(String) -> E,
) -> Result<MessageBody, PortFault<E>> {
    body.build()
        .map_err(|e| PortFault::Refused(invalid_body(e.to_string())))
}

/// A gate verdict as the component sees it on the peer-call path.
///
/// The classification is [`brenn_budget::publish_refusal_kind`], shared with the
/// backend host and with the publish path above: a call's payload becomes the one
/// envelope in the callee's window, so it is judged — and spelled to the guest —
/// exactly as a published body is.
fn call_error(refusal: GateRefusal) -> CallError {
    match publish_refusal_kind(refusal) {
        PublishRefusalKind::InvalidPayload(fault) => CallError::InvalidPayload(fault.to_string()),
        PublishRefusalKind::QuotaExceeded => CallError::QuotaExceeded,
    }
}

/// A gate verdict as the component sees it on the control-op path.
///
/// The classification is [`brenn_budget::defer_refusal_kind`], shared with the
/// backend host.
fn defer_error(refusal: GateRefusal) -> DeferFault {
    PortFault::Refused(match defer_refusal_kind(refusal) {
        DeferRefusalKind::InvalidDeliverAfter => DeferError::InvalidDeliverAfter,
        DeferRefusalKind::QuotaExceeded => DeferError::QuotaExceeded,
    })
}

/// One attachment of a buffered publish: the message's name for it and the
/// blob its handle held at the call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedAttachment {
    pub name: AttachmentName,
    pub blob: Blob,
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
    pub body: MessageBody,
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
    /// The attachments, in call order; empty for a `ports` publish. Holding
    /// them holds their blobs until the flush.
    pub attachments: Vec<BufferedAttachment>,
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

/// A control op as the component asked for it, its edit body unbuilt.
enum DeferInput {
    Cancel,
    Edit {
        body: Option<GuestBody>,
        deliver_after: Option<u64>,
    },
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
/// to send there, what the port defaults to, and whether its channel is
/// declared to carry non-text bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputSpec {
    pub channel: String,
    pub default_urgency: Urgency,
    /// The channel's `non_text` declaration in the wiring the buffer was seeded
    /// under ([`crate::bindings::AppliedBindings::carries_non_text`]).
    pub non_text: bool,
}

/// The per-activation publish buffer and its budgets.
///
/// Not `Copy`/`Default`: a buffer only ever exists seeded, for exactly one
/// activation, and a buffer nobody seeded would silently enforce nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBuffer {
    /// The instance whose activation this buffer serves, for its log lines.
    instance: String,
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
    /// Accepted publishes, `ports` and `blobs` alike, in call order.
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
        instance: &str,
        outputs: HashMap<String, OutputSpec>,
        declared_out_ports: Arc<BTreeSet<String>>,
        sink_mt: HashMap<String, u64>,
        max_body_bytes: u64,
        deferred: HashMap<String, Vec<Uuid>>,
    ) -> Self {
        Self {
            instance: instance.to_owned(),
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
    /// batch budget: every operation it will carry, charged by the codec of the
    /// version the surface route serves, exactly as that codec's
    /// `batch_charged_bytes` charges the frame it composes into. Summed from the
    /// buffered work rather than accumulated, so the two cannot drift. The
    /// route's version rather than an attachment's: a buffer exists while
    /// detached, and the route serves exactly one.
    ///
    /// Confined publishes are not charged: they never join a batch.
    fn batch_charge(&self) -> usize {
        let entries: usize = self
            .entries
            .iter()
            .filter(|entry| channel_is_transportable(&entry.channel))
            .map(|entry| {
                wire_charge(&entry.channel, Some(&entry.body))
                    + attachments_charge(&entry.attachments)
            })
            .sum();
        let ops: usize = self
            .defer_ops
            .iter()
            .filter(|op| channel_is_transportable(&op.channel))
            .map(|op| wire_charge(&op.channel, op.kind.body()))
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

    /// Admit one synchronous call to a peer against this activation's budgets.
    ///
    /// A call is weighed exactly as a publish is, and against the same counters:
    /// the per-activation call ceiling first — charged whether or not the call
    /// is admitted, so a component looping on refusals pays for them — then the
    /// payload against the publish-body cap, because the payload becomes the one
    /// envelope in the callee's fabricated window and a window entry is a
    /// published body wherever it came from.
    ///
    /// One counter over both bounds the *number* of calls, not their cost: a
    /// call is one whole activation of the peer, uncoalesced and with the peer's
    /// own ceilings, where N publishes onto a channel coalesce into at most one
    /// wake. The caller's ceiling bounds how many peers it raises, each peer's
    /// own ceilings bound what that peer then does, and the document's acyclic
    /// call graph bounds the depth. The page has no pacer, so that product is
    /// the whole bound on one chain's main-thread time.
    ///
    /// What this does *not* judge is the port: whether the caller declared it,
    /// and whether the deployer wired it, are facts about the document rather
    /// than about the budget, and they are decided at the seam on either side of
    /// this call — the first before it (a trap), the second after it (`unwired`).
    /// The order — charge, build, cap — must match the backend's call path, so
    /// a call is refused for the same reason on either host. A call has no
    /// channel, so no carriage rule applies.
    ///
    /// Answers the built request body.
    pub fn admit_call(&mut self, body: GuestBody) -> Result<MessageBody, CallError> {
        self.gate.charge_call().map_err(call_error)?;
        let body = body
            .build()
            .map_err(|err| CallError::InvalidBody(err.to_string()))?;
        let cap = self.gate.max_body_bytes();
        if !brenn_activation::sync::within_body_cap(body.as_bytes(), cap) {
            return Err(call_error(GateRefusal::BodyTooLarge {
                len: body.len(),
                max: cap,
            }));
        }
        Ok(body)
    }

    /// Publish `body` from this instance's output `port`, at the port's
    /// configured default urgency. Buffered, not sent: it reaches the router or
    /// the wire only if this activation returns ok.
    pub fn publish(&mut self, port: &str, body: GuestBody) -> Result<(), PublishFault> {
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
        body: GuestBody,
        deliver_after: u64,
    ) -> Result<(), PublishFault> {
        self.publish_inner(port, body, None, Some(deliver_after))
    }

    /// Publish at an explicit urgency, overriding the port's configured default
    /// for this one message.
    pub fn publish_with_urgency(
        &mut self,
        port: &str,
        body: GuestBody,
        urgency: Urgency,
    ) -> Result<(), PublishFault> {
        self.publish_inner(port, body, Some(urgency), None)
    }

    /// The publish path the `ports` entry points share: [`Self::publish_attached`]
    /// with no attachments and the grants held — the `ports` exports answer
    /// their grant before they reach the buffer.
    ///
    /// The gate's publish order, with the page's host-data checks at their
    /// documented positions: the call ceiling first (so a component looping on
    /// rejections pays for them), then the port — three-way, per
    /// [`Self::publish_unwired`] — then the body (`invalid-body`), then whether
    /// the channel may carry it ([`channel_carries`]), then everything the gate
    /// judges, which it charges only on acceptance.
    fn publish_inner(
        &mut self,
        port: &str,
        body: GuestBody,
        urgency_override: Option<Urgency>,
        deliver_after: Option<u64>,
    ) -> Result<(), PublishFault> {
        let no_handles = BlobScope::empty();
        match self.publish_attached(
            &no_handles,
            GrantVerdict::Held,
            None,
            port,
            body,
            &[],
            urgency_override,
            deliver_after,
        ) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(PortFault::Refused(BlobsPublishError::Plain(err)))) => {
                Err(PortFault::Refused(err))
            }
            Ok(Err(PortFault::Undeclared(port))) => Err(PortFault::Undeclared(port)),
            Ok(Err(PortFault::Refused(refusal))) => {
                unreachable!("an empty attachment list drew {refusal:?}")
            }
            Err(handle) => unreachable!("an empty attachment list named handle {handle:?}"),
        }
    }

    /// `blobs.publish`: publish `body` from `port` carrying the blobs
    /// `attachments` names by `scope`'s handles. An empty list is
    /// `ports.publish`, and both share one buffer, so the two interfaces keep
    /// one call order.
    ///
    /// `grants` is the core's verdict on the caller's grants, and `door` the
    /// blob door the last `Welcome` advertised. The checks run in the
    /// interface's order, with the page's additions:
    ///
    /// 1. the call budget shared with `ports`;
    /// 2. the grants — `plain(not-permitted)` on [`GrantVerdict::Missing`],
    ///    buffering nothing and reading nothing below;
    /// 3. the port — undeclared traps; declared and unwired runs every check
    ///    below that needs no channel and drops the message;
    /// 4. a non-empty list on a wire-bound port without a door that uploads is
    ///    `plain(not-permitted)`;
    /// 5. the body — `invalid-body`, then `text-only-channel`;
    /// 6. the attachment names — `too-many-attachments`,
    ///    `invalid-attachment-name`, `duplicate-attachment-name`;
    /// 7. each handle is a blob of `scope`, or the caller traps
    ///    (the outer `Err`);
    /// 8. the size cap and budgets: the body cap, the wire batch budget with
    ///    each attachment's charge, on a wire-bound port the activation's
    ///    distinct blobs against [`SURFACE_MAX_FLUSH_BLOBS`] and the door's
    ///    `max_handles` and `max_bytes_held`, then the gate's own budgets.
    ///
    /// So a caller without a grant answers `plain(not-permitted)` until the
    /// activation's call ceiling, and `plain(quota-exceeded)` from the call
    /// that passes it, each refusal drawing on the count every publish of the
    /// activation draws on.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_attached(
        &mut self,
        scope: &BlobScope,
        grants: GrantVerdict,
        door: Option<&BlobFacts>,
        port: &str,
        body: GuestBody,
        attachments: &[(String, u32)],
        urgency_override: Option<Urgency>,
        deliver_after: Option<u64>,
    ) -> Result<Result<(), BlobsPublishFault>, UnknownHandle> {
        if let Err(refusal) = self.gate.charge_call() {
            return Ok(Err(plain_fault(publish_error(refusal))));
        }
        if grants == GrantVerdict::Missing {
            return Ok(Err(PortFault::Refused(BlobsPublishError::Plain(
                PublishError::NotPermitted,
            ))));
        }
        // The binding's own key is the sink key, so nothing is allocated to ask
        // the gate: a refused publish copies neither the port name nor the
        // channel, and only an accepted one owns them.
        let Some((sink, spec)) = self.outputs.get_key_value(port) else {
            return self.publish_unwired(scope, port, body, attachments, deliver_after);
        };
        let wire_bound = channel_is_transportable(&spec.channel);
        if wire_bound && !attachments.is_empty() && !door.is_some_and(|door| door.upload) {
            return Ok(Err(PortFault::Refused(BlobsPublishError::Plain(
                PublishError::NotPermitted,
            ))));
        }
        let body = match build_body(body, PublishError::InvalidBody) {
            Ok(body) => body,
            Err(fault) => return Ok(Err(plain_fault(fault))),
        };
        if let Err(fault) = self.judge_carriage(port, spec, &body, PublishError::TextOnlyChannel) {
            return Ok(Err(plain_fault(fault)));
        }
        let names = match check_attachment_names(attachments) {
            Ok(names) => names,
            Err(refusal) => return Ok(Err(PortFault::Refused(refusal))),
        };
        let attachments = resolve_attachments(scope, names, attachments)?;
        let urgency = urgency_override.unwrap_or(spec.default_urgency);
        // The wire batch budget, before the gate charges anything: the gate
        // charges on acceptance and `take` asserts its count against this
        // buffer's, so a refusal after it would leave the two disagreeing.
        // Both asked only of a body inside the cap, so the gate's
        // `BodyTooLarge` still names an oversize argument rather than a budget.
        let charge = wire_charge(&spec.channel, Some(&body)) + attachments_charge(&attachments);
        let within_cap = body.len() <= self.gate.max_body_bytes();
        let over_flush_blobs = wire_bound
            && !attachments.is_empty()
            && !self.flush_blobs_fit(
                door.expect("checked above: a wire-bound attachment publish has a door"),
                &attachments,
            );
        if within_cap && (self.overruns_batch_budget(&spec.channel, charge) || over_flush_blobs) {
            return Ok(Err(PortFault::Refused(BlobsPublishError::Plain(
                PublishError::QuotaExceeded,
            ))));
        }
        // A bound port always has a seeded bucket — the core seeds one per entry
        // of the same `outputs` map this resolved against — so the gate's miss
        // panic is a broken kernel invariant, not a component's problem.
        if let Err(refusal) = self.gate.admit_publish(PublishCheck {
            sink,
            body_len: body.len(),
            deliver_after,
            entry_addend: 0,
        }) {
            return Ok(Err(plain_fault(publish_error(refusal))));
        }
        let channel = spec.channel.clone();
        self.entries.push(BufferedPublish {
            port: port.to_string(),
            channel,
            body,
            urgency,
            urgency_override,
            deliver_after,
            attachments,
        });
        Ok(Ok(()))
    }

    /// Whether this activation's wire-bound publishes, with `more` added, name
    /// no more distinct blobs than [`SURFACE_MAX_FLUSH_BLOBS`] and `door`'s
    /// `max_handles`, and no more of their bytes together than `door`'s
    /// `max_bytes_held`. The flush holds one server handle per distinct blob.
    fn flush_blobs_fit(&self, door: &BlobFacts, more: &[BufferedAttachment]) -> bool {
        let named = self
            .entries
            .iter()
            .filter(|entry| channel_is_transportable(&entry.channel))
            .flat_map(|entry| &entry.attachments)
            .chain(more)
            .map(|attachment| &attachment.blob);
        let distinct = distinct_blobs(named);
        distinct.len() <= SURFACE_MAX_FLUSH_BLOBS
            && door_holds(door, distinct.len(), blobs_bytes(distinct.iter().copied()))
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
    /// The body is built, so a malformed one draws `invalid-body` here too; with
    /// no channel there is nothing to judge its carriage against. The
    /// attachment names and handles are checked as a bound port checks them;
    /// the door and the per-flush blob bounds are a wire channel's and are
    /// not asked. The attachments are dropped with the message.
    fn publish_unwired(
        &mut self,
        scope: &BlobScope,
        port: &str,
        body: GuestBody,
        attachments: &[(String, u32)],
        deliver_after: Option<u64>,
    ) -> Result<Result<(), BlobsPublishFault>, UnknownHandle> {
        if !self.declared_out_ports.contains(port) {
            return Ok(Err(PortFault::Undeclared(port_for_report(port))));
        }
        let body = match build_body(body, PublishError::InvalidBody) {
            Ok(body) => body,
            Err(fault) => return Ok(Err(plain_fault(fault))),
        };
        let names = match check_attachment_names(attachments) {
            Ok(names) => names,
            Err(refusal) => return Ok(Err(PortFault::Refused(refusal))),
        };
        resolve_attachments(scope, names, attachments)?;
        if let Err(refusal) = self
            .gate
            .admit_publish_without_sink(body.len(), deliver_after, 0)
        {
            return Ok(Err(plain_fault(publish_error(refusal))));
        }
        Ok(Ok(()))
    }

    /// Cancel one message this instance has parked on `port`'s channel, named by
    /// its `index` into the deferred window this activation was handed.
    ///
    /// Buffered like a publish, so an err or trap cancels nothing.
    pub fn defer_cancel(&mut self, port: &str, index: u32) -> Result<(), DeferFault> {
        self.defer_inner(port, index, DeferInput::Cancel)
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
        body: Option<GuestBody>,
        deliver_after: Option<u64>,
    ) -> Result<(), DeferFault> {
        self.defer_inner(
            port,
            index,
            DeferInput::Edit {
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
    /// index — the two answers only this buffer holds — then an edit body's
    /// build (`invalid-body`) and carriage ([`channel_carries`]), then the
    /// gate's op ceiling and edit-body rules.
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
    fn defer_inner(&mut self, port: &str, index: u32, input: DeferInput) -> Result<(), DeferFault> {
        let edit_deliver_after = match &input {
            DeferInput::Edit { deliver_after, .. } => *deliver_after,
            DeferInput::Cancel => None,
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
        let kind = match input {
            DeferInput::Cancel => DeferOp::Cancel,
            DeferInput::Edit {
                body,
                deliver_after,
            } => {
                let body = body
                    .map(|body| build_body(body, DeferError::InvalidBody))
                    .transpose()?;
                if let Some(body) = &body {
                    self.judge_carriage(port, spec, body, DeferError::TextOnlyChannel)?;
                }
                DeferOp::Edit {
                    body,
                    deliver_after,
                }
            }
        };
        let edit_body = kind.body();
        let edit_body_len = edit_body.map(MessageBody::len);
        // The wire batch budget, as on the publish path: an op names a channel
        // and an edit carries a body, and the legality law charges both.
        let charge = wire_charge(&spec.channel, edit_body);
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

    /// Whether `spec`'s channel may carry `body` ([`channel_carries`]), as the
    /// refusal the publish and edit paths both answer: `text_only` for a
    /// non-text body on a channel not declared to carry one. Each refusal is
    /// logged.
    fn judge_carriage<E>(
        &self,
        port: &str,
        spec: &OutputSpec,
        body: &MessageBody,
        text_only: E,
    ) -> Result<(), PortFault<E>> {
        if channel_carries(spec.non_text, body) {
            return Ok(());
        }
        // One line per body refused for its carriage. The per-activation call
        // ceiling bounds how many one activation can write.
        tracing::info!(
            instance = %self.instance,
            port = %port_for_report(port),
            channel = %spec.channel,
            body = %body.summary(),
            "surface client: refused a non-text body on a channel declared text-only"
        );
        Err(PortFault::Refused(text_only))
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
    use brenn_attach_proto::{BatchDeferredOp, BatchEntry};

    use super::*;

    const WIRE: &str = "brenn:site.bar.out";

    fn text(body: impl Into<String>) -> GuestBody {
        GuestBody::Text(body.into())
    }

    fn parts(content_type: Option<&str>, bytes: &[u8]) -> GuestBody {
        GuestBody::Parts {
            content_type: content_type.map(str::to_owned),
            bytes: bytes.to_vec(),
        }
    }

    const LOCAL: &str = "local:page/notes";

    fn spec(channel: &str) -> OutputSpec {
        OutputSpec {
            channel: channel.to_string(),
            default_urgency: Urgency::Normal,
            non_text: false,
        }
    }

    /// A port whose channel the wiring declares `non_text`.
    fn declared_spec(channel: &str) -> OutputSpec {
        OutputSpec {
            non_text: true,
            ..spec(channel)
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
            "keeper",
            outputs,
            vocabulary(),
            sink_mt,
            max_body_bytes,
            HashMap::new(),
        )
    }

    #[test]
    fn a_call_payload_is_capped_exactly_as_a_published_body_is() {
        let mut buf = buffer(8);
        assert_eq!(
            buf.admit_call(GuestBody::Text("a".repeat(8))).map(|_| ()),
            Ok(()),
            "the cap is inclusive"
        );
        assert_eq!(
            buf.admit_call(GuestBody::Text("a".repeat(9))).map(|_| ()),
            Err(CallError::InvalidPayload(
                "payload 9 bytes exceeds max 8".to_string()
            ))
        );
    }

    #[test]
    fn a_call_body_is_built_after_the_charge_and_before_the_cap() {
        let mut buf = buffer(4);
        let malformed = GuestBody::Parts {
            content_type: None,
            bytes: vec![0xff],
        };
        assert!(
            matches!(buf.admit_call(malformed), Err(CallError::InvalidBody(_))),
            "an untyped body that is not UTF-8 does not build"
        );
        let oversize_malformed = GuestBody::Parts {
            content_type: Some("text/csv".to_string()),
            bytes: vec![0xff; 9],
        };
        assert!(
            matches!(
                buf.admit_call(oversize_malformed),
                Err(CallError::InvalidBody(_))
            ),
            "the build is judged before the cap"
        );
        let bytes = GuestBody::Parts {
            content_type: Some("application/octet-stream".to_string()),
            bytes: vec![0xff, 0x00],
        };
        let built = buf
            .admit_call(bytes)
            .expect("a well-formed body is admitted");
        assert_eq!(built.as_bytes(), &[0xff, 0x00]);
        assert_eq!(built.content_type(), Some("application/octet-stream"));
        assert!(!built.is_text());
    }

    #[test]
    fn a_call_spends_the_budget_a_publish_would_have_spent() {
        // One activation, one ceiling: calls and publishes draw on the same
        // counter, so the number of the two together is what is bounded.
        let mut buf = buffer(64);
        let mut calls = 0;
        loop {
            match buf.admit_call(GuestBody::Text("x".to_string())) {
                Ok(_) => calls += 1,
                Err(CallError::QuotaExceeded) => break,
                Err(other) => panic!("unexpected call refusal: {other:?}"),
            }
            assert!(calls < 100_000, "the call ceiling is not bounding anything");
        }
        assert!(calls > 0, "the fixture must admit at least one call");
        assert_eq!(
            buf.publish("notes", text("y")),
            Err(PortFault::Refused(PublishError::QuotaExceeded)),
            "the calls spent the publish family's own ceiling"
        );
    }

    /// The whole point of the buffer-time rule: entries each well inside the body
    /// cap that together overrun the batch budget are refused *here*, where the
    /// component gets an answer, instead of at the wire where the entire atomic
    /// flush would be dropped against a counter after every publish was told ok.
    #[test]
    fn a_publish_that_would_overrun_the_batch_budget_is_refused_at_the_call() {
        let mut buf = buffer(64);
        assert_eq!(buf.publish("out", text("a".repeat(20))), Ok(()));
        assert_eq!(
            buf.publish("out", text("b".repeat(20))),
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
        let cap = 160;
        let mut buf = declared_buffer_capped(cap as u64);
        // Publish until refused, then check the survivors against the law:
        // untyped text, typed text, a content type the header escapes, and
        // non-text bytes on the declared transportable port.
        for i in 0..64 {
            let (port, body) = match i % 4 {
                0 => ("out", text(format!("body-{i}"))),
                1 => ("out", parts(Some("text/csv"), format!("a,{i}").as_bytes())),
                2 => (
                    "out",
                    parts(Some("text/plain; charset=\"utf-8\""), b"quoted"),
                ),
                _ => ("bout", parts(Some("image/jpeg"), &[0xff, 0xd8, 0xff])),
            };
            if buf.publish(port, body).is_err() {
                break;
            }
        }
        assert!(buf.len() > 3, "the fixture must admit every kind of body");
        let flushed = buf.take();
        let entries: Vec<BatchEntry> = flushed
            .publishes
            .into_iter()
            .map(|p| BatchEntry {
                channel: p.channel,
                body: p.body,
                urgency: p.urgency,
                deliver_after: p.deliver_after,
                attachments: Vec::new(),
            })
            .collect();
        assert_eq!(
            Codec::negotiated(SURFACE_ATTACH_VERSION).check_batch_legality(&entries, &[], cap),
            Ok(())
        );
    }

    /// A body at exactly the advertised cap cannot survive a batch, because the
    /// channel address is charged beside it. The component learns that at the
    /// call rather than from a drop counter.
    #[test]
    fn a_body_at_the_cap_on_a_wire_port_is_refused_for_the_channel_bytes() {
        let mut buf = buffer(64);
        assert_eq!(
            buf.publish("out", text("a".repeat(64))),
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
            buf.publish("out", text("a".repeat(65))),
            Err(PortFault::Refused(PublishError::InvalidPayload(
                "payload 65 bytes exceeds max 64".to_string()
            ))),
            "the refusal carries the gate's account"
        );
    }

    /// A confined publish never joins a batch, so it is charged nothing and
    /// refuses nothing.
    #[test]
    fn confined_publishes_are_not_charged_against_the_batch_budget() {
        let mut buf = buffer(64);
        for i in 0..8 {
            assert_eq!(buf.publish("notes", text(format!("note-{i}"))), Ok(()));
        }
        assert_eq!(buf.len(), 8);
        // And the wire port still has its whole budget.
        assert_eq!(buf.publish("out", text("a".repeat(40))), Ok(()));
    }

    /// The middle arm of the three-way rule: a port the specification declares
    /// and the deployer did not wire is a sink with no channel. The component is
    /// told ok and the message goes nowhere.
    #[test]
    fn a_declared_unwired_port_drops_the_message_and_answers_ok() {
        let mut buf = buffer(1_024);
        assert_eq!(buf.publish(UNWIRED, text("hi")), Ok(()));
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
            unwired.publish(UNWIRED, text("a".repeat(65))),
            Err(PortFault::Refused(PublishError::InvalidPayload(
                "payload 65 bytes exceeds max 64".to_string()
            ))),
            "an oversize body is invalid-payload on the unwired port"
        );
        assert_eq!(
            bound.publish("out", text("a".repeat(65))),
            Err(PortFault::Refused(PublishError::InvalidPayload(
                "payload 65 bytes exceeds max 64".to_string()
            ))),
            "and the same on the bound port beside it"
        );
        assert_eq!(
            unwired.publish_deferred(UNWIRED, text("hi"), u64::MAX),
            Err(PortFault::Refused(PublishError::InvalidPayload(format!(
                "deliver_after {} ms is not a representable timestamp",
                u64::MAX
            )))),
            "an unrepresentable release time is an invalid payload on the unwired port"
        );
        assert_eq!(
            bound.publish_deferred("out", text("hi"), u64::MAX),
            Err(PortFault::Refused(PublishError::InvalidPayload(format!(
                "deliver_after {} ms is not a representable timestamp",
                u64::MAX
            )))),
            "and the same on the bound port beside it"
        );
    }

    /// A deferred publish to an unwired declared port takes the same drop as the
    /// immediate one: ok to the component, nothing buffered, and no park left
    /// scheduled against a port that has no channel to release onto.
    #[test]
    fn a_declared_unwired_port_drops_a_deferred_publish_and_answers_ok() {
        let mut buf = buffer(1_024);
        assert_eq!(buf.publish_deferred(UNWIRED, text("hi"), 60_000), Ok(()));
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
            assert_eq!(buf.publish(UNWIRED, text("hi")), Ok(()));
        }
        assert_eq!(
            buf.publish("notes", text("hi")),
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
        assert_eq!(buf.publish("out", text("hi")), Ok(()));
        assert_eq!(buf.dropped(), 0, "a bound publish is queued, not dropped");
        assert_eq!(buf.publish(UNWIRED, text("hi")), Ok(()));
        assert_eq!(buf.dropped(), 1);
        assert_eq!(
            buf.publish(UNWIRED, text("a".repeat(65))),
            Err(PortFault::Refused(PublishError::InvalidPayload(
                "payload 65 bytes exceeds max 64".to_string()
            ))),
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
            buf.publish(UNDECLARED, text("hi")),
            Err(PortFault::Undeclared("\"ghost\"".to_string()))
        );
        assert_eq!(
            buf.publish_deferred(UNDECLARED, text("hi"), 1),
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
        let Err(PortFault::Undeclared(report)) = buf.publish(&hostile, text("hi")) else {
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
            let _ = buf.publish(UNDECLARED, text("hi"));
        }
        assert_eq!(
            buf.publish(UNDECLARED, text("hi")),
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
            buf.defer_edit(UNWIRED, 0, Some(text("x")), None),
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
            "keeper",
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
            buf.defer_edit("out", 1, Some(text("x".repeat(30))), None),
            Err(PortFault::Refused(DeferError::QuotaExceeded)),
            "18 + 18 + 30 charged bytes against a 64-byte budget"
        );
    }

    /// The buffer's running charge is the codec's charge of the batch the
    /// flush composes into, publishes and control ops together — a cancel, an
    /// edit with a body and one without — and a confined publish adds nothing.
    #[test]
    fn the_running_charge_is_the_codecs_charge_of_the_composed_batch() {
        let mut buf = PublishBuffer::new(
            "keeper",
            HashMap::from([
                ("out".to_string(), spec(WIRE)),
                ("notes".to_string(), spec(LOCAL)),
            ]),
            vocabulary(),
            HashMap::from([
                ("out".to_string(), 1_000_000),
                ("notes".to_string(), 1_000_000),
            ]),
            1_024,
            HashMap::from([(
                "out".to_string(),
                vec![Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)],
            )]),
        );
        assert_eq!(buf.publish("out", text("first")), Ok(()));
        assert_eq!(buf.publish("notes", text("confined")), Ok(()));
        assert_eq!(buf.publish("out", text("\"quoted\"")), Ok(()));
        assert_eq!(buf.defer_cancel("out", 0), Ok(()));
        assert_eq!(buf.defer_edit("out", 1, Some(text("edited")), None), Ok(()));
        assert_eq!(buf.defer_edit("out", 2, None, Some(60_000)), Ok(()));
        let charge = buf.batch_charge();

        let flushed = buf.take();
        let entries: Vec<BatchEntry> = flushed
            .publishes
            .into_iter()
            .filter(|p| channel_is_transportable(&p.channel))
            .map(|p| BatchEntry {
                channel: p.channel,
                body: p.body,
                urgency: p.urgency,
                deliver_after: p.deliver_after,
                attachments: Vec::new(),
            })
            .collect();
        assert_eq!(entries.len(), 2);
        let ops: Vec<BatchDeferredOp> = flushed
            .defer_ops
            .into_iter()
            .map(crate::flush::wire_op)
            .collect();
        assert_eq!(ops.len(), 3);
        assert_eq!(
            charge,
            Codec::negotiated(SURFACE_ATTACH_VERSION).batch_charged_bytes(&entries, &ops)
        );
    }

    /// A buffer whose two bound ports each hold one parked message.
    fn buffer_with_parked() -> PublishBuffer {
        PublishBuffer::new(
            "keeper",
            HashMap::from([
                ("out".to_string(), spec(WIRE)),
                ("notes".to_string(), spec(LOCAL)),
            ]),
            vocabulary(),
            HashMap::from([
                ("out".to_string(), 1_000_000),
                ("notes".to_string(), 1_000_000),
            ]),
            1_024,
            HashMap::from([
                ("out".to_string(), vec![Uuid::from_u128(1)]),
                ("notes".to_string(), vec![Uuid::from_u128(2)]),
            ]),
        )
    }

    const NOT_UTF8: &str = "untyped body is not valid UTF-8";

    #[test]
    fn a_malformed_body_is_invalid_body_on_every_port_after_the_port_check() {
        let mut buf = buffer(1_024);
        let malformed = || parts(None, &[0xff]);
        assert_eq!(
            buf.publish(UNDECLARED, malformed()),
            Err(PortFault::Undeclared("\"ghost\"".to_string())),
            "the port is judged before the body"
        );
        for port in ["out", "notes", UNWIRED] {
            assert_eq!(
                buf.publish(port, malformed()),
                Err(PortFault::Refused(PublishError::InvalidBody(
                    NOT_UTF8.to_owned()
                ))),
                "{port}"
            );
        }
        assert_eq!(
            buf.publish("notes", parts(Some("text/"), b"x")),
            Err(PortFault::Refused(PublishError::InvalidBody(
                "invalid content type \"text/\": missing subtype".to_owned()
            )))
        );
        assert_eq!(buf.len(), 0, "a refused body buffers nothing");
        assert_eq!(buf.dropped(), 0, "and drops nothing");
    }

    #[test]
    fn a_non_text_body_is_refused_on_every_undeclared_bound_channel() {
        let mut buf = buffer(1_024);
        for port in ["out", "notes"] {
            assert_eq!(
                buf.publish(port, parts(Some("image/jpeg"), &[0xff, 0xd8])),
                Err(PortFault::Refused(PublishError::TextOnlyChannel)),
                "{port}"
            );
            assert_eq!(
                buf.publish_deferred(port, parts(Some("image/jpeg"), &[0xff]), 60_000),
                Err(PortFault::Refused(PublishError::TextOnlyChannel)),
                "{port}, deferred"
            );
        }
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn typed_text_keeps_its_type_on_every_bound_channel() {
        let mut buf = buffer(1_024);
        assert_eq!(buf.publish("out", parts(Some("text/csv"), b"a,b")), Ok(()));
        assert_eq!(
            buf.publish_with_urgency("out", parts(Some("text/csv"), b"a,b"), Urgency::High),
            Ok(())
        );
        assert_eq!(
            buf.publish("notes", parts(Some("text/csv"), b"a,b")),
            Ok(())
        );
        assert_eq!(
            buf.publish("out", parts(None, b"plain")),
            Ok(()),
            "untyped parts are untyped text"
        );
        let flushed = buf.take().publishes;
        assert_eq!(flushed.len(), 4);
        for p in &flushed[..3] {
            assert_eq!(p.body.content_type(), Some("text/csv"));
            assert_eq!(p.body.as_text(), Some("a,b"));
        }
        assert_eq!(flushed[0].channel, WIRE);
        assert_eq!(flushed[3].body, MessageBody::text("plain"));
    }

    #[test]
    fn the_body_is_judged_before_its_carriage_and_both_before_the_cap() {
        let mut buf = buffer(8);
        let oversize = vec![0xff; 9];
        assert_eq!(
            buf.publish("notes", parts(None, &oversize)),
            Err(PortFault::Refused(PublishError::InvalidBody(
                NOT_UTF8.to_owned()
            ))),
            "malformed before oversize"
        );
        assert_eq!(
            buf.publish("notes", parts(Some("image/png"), &oversize)),
            Err(PortFault::Refused(PublishError::TextOnlyChannel)),
            "uncarriable before oversize"
        );
        assert_eq!(
            buf.publish("out", parts(Some("image/png"), &oversize)),
            Err(PortFault::Refused(PublishError::TextOnlyChannel)),
            "uncarriable on the wire before oversize"
        );
    }

    #[test]
    fn a_declared_unwired_port_builds_the_body_and_judges_no_carriage() {
        let mut buf = buffer(1_024);
        assert_eq!(
            buf.publish(UNWIRED, parts(Some("image/jpeg"), &[0xff])),
            Ok(()),
            "a sink with no channel has nothing to judge carriage against"
        );
        assert_eq!(buf.dropped(), 1);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn a_refused_body_spends_its_call() {
        let mut buf = buffer(1_024);
        for _ in 0..brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION {
            assert_eq!(
                buf.publish("notes", parts(Some("image/jpeg"), &[0xff])),
                Err(PortFault::Refused(PublishError::TextOnlyChannel))
            );
        }
        assert_eq!(
            buf.publish("notes", text("hi")),
            Err(PortFault::Refused(PublishError::QuotaExceeded))
        );
    }

    #[test]
    fn an_edit_body_is_built_and_judged_after_the_index() {
        let mut buf = buffer_with_parked();
        assert_eq!(
            buf.defer_edit("notes", 1, Some(parts(None, &[0xff])), None),
            Err(PortFault::Refused(DeferError::OutOfRange)),
            "the index is judged before the body"
        );
        assert_eq!(
            buf.defer_edit("notes", 0, Some(parts(None, &[0xff])), None),
            Err(PortFault::Refused(DeferError::InvalidBody(
                NOT_UTF8.to_owned()
            )))
        );
        assert_eq!(
            buf.defer_edit("notes", 0, Some(parts(Some("image/jpeg"), &[0xff])), None),
            Err(PortFault::Refused(DeferError::TextOnlyChannel))
        );
        assert_eq!(
            buf.defer_edit("out", 0, Some(parts(Some("image/jpeg"), &[0xff])), None),
            Err(PortFault::Refused(DeferError::TextOnlyChannel))
        );
        assert_eq!(
            buf.defer_edit("notes", 0, Some(parts(Some("text/csv"), b"a,b")), None),
            Ok(())
        );
        assert_eq!(
            buf.defer_edit("out", 0, Some(parts(Some("text/csv"), b"a,b")), None),
            Ok(())
        );
        assert_eq!(buf.defer_edit("out", 0, None, Some(60_000)), Ok(()));
        let ops = buf.take().defer_ops;
        assert_eq!(ops.len(), 3, "only the accepted edits are buffered");
        let DeferOp::Edit {
            body: Some(body), ..
        } = &ops[0].kind
        else {
            panic!("the first accepted op is the typed edit: {:?}", ops[0].kind);
        };
        assert_eq!(body.content_type(), Some("text/csv"));
    }

    const LOCAL_BLOB: &str = "local:page/blob";
    const WIRE_BLOB: &str = "ephemeral:site.bar.frames";

    /// A buffer with one port on a page-local channel declared `non_text`
    /// (`blob`), one on a transportable channel declared `non_text` (`bout`),
    /// one on an undeclared page-local channel (`notes`) and one on an
    /// undeclared transportable channel (`out`). Each port holds one parked
    /// message.
    fn declared_buffer() -> PublishBuffer {
        declared_buffer_capped(1_024)
    }

    /// [`declared_buffer`] under the body cap `max_body_bytes`.
    fn declared_buffer_capped(max_body_bytes: u64) -> PublishBuffer {
        let ports = ["blob", "bout", "notes", "out"];
        PublishBuffer::new(
            "keeper",
            HashMap::from([
                ("blob".to_string(), declared_spec(LOCAL_BLOB)),
                ("bout".to_string(), declared_spec(WIRE_BLOB)),
                ("notes".to_string(), spec(LOCAL)),
                ("out".to_string(), spec(WIRE)),
            ]),
            Arc::new(ports.into_iter().map(str::to_string).collect()),
            ports
                .into_iter()
                .map(|p| (p.to_string(), 1_000_000))
                .collect(),
            max_body_bytes,
            ports
                .into_iter()
                .zip(1u128..)
                .map(|(p, id)| (p.to_string(), vec![Uuid::from_u128(id)]))
                .collect(),
        )
    }

    #[test]
    fn a_declared_port_of_either_class_carries_a_non_text_body() {
        for (port, channel) in [("blob", LOCAL_BLOB), ("bout", WIRE_BLOB)] {
            let mut buf = declared_buffer();
            let jpeg = || parts(Some("image/jpeg"), &[0xff, 0xd8, 0xff]);
            assert_eq!(buf.publish(port, jpeg()), Ok(()), "{port}");
            assert_eq!(buf.publish_deferred(port, jpeg(), 60_000), Ok(()));
            assert_eq!(
                buf.publish_with_urgency(port, jpeg(), Urgency::High),
                Ok(())
            );
            assert_eq!(
                buf.defer_edit(port, 0, Some(parts(None, b"plain")), None),
                Ok(()),
                "text is carried on a declared channel too"
            );
            assert_eq!(buf.defer_edit(port, 0, Some(jpeg()), None), Ok(()));
            let flushed = buf.take();
            assert_eq!(flushed.publishes.len(), 3);
            for p in &flushed.publishes {
                assert_eq!(p.channel, channel);
                assert_eq!(p.body.content_type(), Some("image/jpeg"));
                assert_eq!(p.body.as_bytes(), &[0xff, 0xd8, 0xff]);
            }
            assert_eq!(flushed.publishes[1].deliver_after, Some(60_000));
            let DeferOp::Edit {
                body: Some(body), ..
            } = &flushed.defer_ops[1].kind
            else {
                panic!("the second op is the non-text edit");
            };
            assert!(!body.is_text());
        }
    }

    #[test]
    fn an_undeclared_port_of_either_class_refuses_a_non_text_body_each_way() {
        let mut buf = declared_buffer();
        let jpeg = || parts(Some("image/jpeg"), &[0xff, 0xd8]);
        for port in ["notes", "out"] {
            assert_eq!(
                buf.publish(port, jpeg()),
                Err(PortFault::Refused(PublishError::TextOnlyChannel)),
                "{port}"
            );
            assert_eq!(
                buf.publish_deferred(port, jpeg(), 60_000),
                Err(PortFault::Refused(PublishError::TextOnlyChannel)),
                "{port}, deferred"
            );
            assert_eq!(
                buf.defer_edit(port, 0, Some(jpeg()), None),
                Err(PortFault::Refused(DeferError::TextOnlyChannel)),
                "{port}, edit"
            );
        }
        let flushed = buf.take();
        assert!(flushed.publishes.is_empty());
        assert!(flushed.defer_ops.is_empty());
    }
}

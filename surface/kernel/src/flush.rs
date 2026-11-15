//! What happens when an activation returns: the buffer committed, or discarded,
//! or the component taken terminal.
//!
//! An activation's publishes and control ops are buffered while it runs and reach
//! nothing until it completes — that is the flush rule, and it is why a component
//! that returns err or traps cannot leak a publish. This module is the one place
//! that rule is enacted, in three shapes:
//!
//! - **Ok** — [`flush_ok`] commits the buffer. Confined entries and confined ops
//!   are applied here and now, through the router; transportable entries and ops
//!   become one wire flush, offered to the instance's stage, which hands a plain
//!   one straight to its outbox and holds one naming attachments until their
//!   blobs are held on the live attachment.
//! - **Err** — [`discard_err`] drops the work and returns the budget. What a
//!   component *spent* is a fact about the activation that ran, and an err does
//!   not un-spend it.
//! - **Trap, or a fatal-rung overflow** — [`kill`] takes the instance terminal:
//!   its positions go, its queued flushes go, and it is delivered nothing more.
//!
//! # Both classes commit, in different places
//!
//! Call order is preserved *within* each class — the router routes its entries in
//! order, the frame carries its entries in order — but the two commit at
//! different authorities (one in this page, one at the peer), so their relative
//! order is not guaranteed. That is contract text, not an artifact of this code.
//!
//! Control ops apply ahead of the same activation's publishes on both sides: an
//! op names a message an *earlier* activation parked, and applying it first keeps
//! this activation's own publishes out of its way. The transportable half rides
//! the batch frame, whose ops the peer applies before its entries, for the same
//! reason.
//!
//! # A captured resolution, not a fresh one
//!
//! Every buffered entry carries the channel and the urgency its port resolved to
//! **at buffer time**, and this module writes those rather than resolving the port
//! again. The resolution that authorized the publish is the one that must route
//! it: a second bindings document could otherwise send a component's ok'd publish
//! somewhere it was never authorized to write when it wrote it.
//!
//! A captured resolution the wiring has since dropped is not written at all. The
//! transportable half is offered against the contract in force, and a flush that
//! contract refuses is dropped whole — the peer would answer it with a protocol
//! close rather than an outcome, which is not a price worth paying for honestly
//! replaying what an older document authorized.
//!
//! # The answers leave as data
//!
//! A flush produces frames to send, drops to announce, and plane refusals to
//! report; a kill produces a first-time flag and a discarded count. None of it is
//! enacted here — which frame an announcement becomes, and how a killed instance
//! is surfaced, are the caller's, the same seam every surface-side module answers
//! across. The confined release deadline is not answered either: a park moved it,
//! and the caller states it through `LocalRouter::release_wakeup` after every
//! input, which is the one arrangement that cannot be forgotten at a new site.

#[cfg(test)]
mod tests;

use brenn_attach_client::Millis;
use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::publish::{Discarded, OutboxSteps, TimerChange};
use brenn_attach_client::router::{
    DeferOpAnswer, DeferOpRequest, LocalRouter, MessageStamp, Origin, PlanePolicy, RouteOutcome,
    RouteRequest,
};
use brenn_attach_client::store::DeferOp;
use brenn_attach_proto::{BatchDeferredOp, ClientFrame, DeferredOpKind};
use brenn_envelope::{Attachments, MessageBody};

use crate::activation::{DropVerdicts, Schedules};
use crate::bindings::{AppliedBindings, channel_carries, channel_is_transportable};
use crate::blob_plane::PagePlane;
use crate::outbound::SurfaceOutbound;
use crate::page::HandOver;
use crate::page_blobs::PageBlobs;
use crate::publish_buffer::{BufferedAttachment, BufferedDeferOp, BufferedPublish, PublishBuffer};
use crate::registry::{Registrations, SurfaceStores};
use crate::uploads::{Lost, Stage, StagedEntry, StagedFlush};

/// Everything a completion writes to: the wiring it resolves against, the two
/// deferral authorities and the retention behind them, the outboxes, and the
/// tables that account for the instance.
///
/// One bundle rather than a positional list, for the reason
/// [`crate::activation::ActivationCtx`] is one: the members travel together, and
/// a completion touches more of the page than an assembly does.
pub struct FlushCtx<'a, P> {
    /// The wiring in force. Read to resolve the noise of a binding a confined
    /// append overflowed, and to assert that every confined body coming to rest
    /// is one its channel's declaration admits — every address a flush writes
    /// was captured when the component published.
    pub bindings: &'a AppliedBindings,
    /// The attachment's contract, or `None` while detached. What a transportable
    /// flush is judged against before it is offered the wire: the buffer captured
    /// its channels and its body cap when the component published, and both can
    /// have moved since.
    pub facts: Option<&'a AttachmentFacts>,
    pub stores: &'a mut SurfaceStores,
    /// What each attachment of a page-local message names. A confined publish's
    /// attachments are recorded here as it commits.
    pub page_blobs: &'a mut PageBlobs,
    pub router: &'a mut LocalRouter<P>,
    pub outbound: &'a mut SurfaceOutbound,
    /// Where a wire flush naming attachments waits for its blobs to be held.
    pub stage: &'a mut Stage,
    /// The page's blob plane, which the stage mints handles on and a kill
    /// closes them on.
    pub plane: &'a mut PagePlane,
    pub registrations: &'a mut Registrations,
    pub schedules: &'a mut Schedules,
    /// The wall clock this completion is judged at, epoch milliseconds UTC — the
    /// currency a release time is stated in, so a park and a control op resolve
    /// against the same instant the activation's own schedule was shown at.
    pub now_ms: u64,
    /// The driver's monotonic reading, for the outbox's retry deadline.
    pub now: Millis,
}

/// A plane's refusal of one buffered write.
///
/// The publisher is not told: it got its synchronous answer at buffer time and
/// has already returned. What this is for is the diagnostic the caller raises —
/// the page wrote something a plane's own rules rejected, which is a bug in the
/// page or in the component, either way worth surfacing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneRefusal {
    /// The instance's own output port, so the report names what a component
    /// author would recognize.
    pub port: String,
    pub channel: String,
    /// The plane's own words.
    pub reason: String,
}

/// What one ok completion produced.
#[derive(Debug, Default, PartialEq)]
pub struct FlushReport {
    /// The outbox's answer for the transportable half: frames to send, whole
    /// flushes lost — at the cap, or refused against the contract in force — and
    /// the retry timer's instruction. Default —
    /// nothing to send, nothing lost, timer unchanged — when the flush had no
    /// transportable half at all.
    pub steps: OutboxSteps<String>,
    /// The loudness ladder's verdicts for readers a confined append evicted. An
    /// instance's own publish can overflow a *sibling's* position, which is why
    /// each announcement names its own instance.
    pub drops: DropVerdicts,
    /// Plane refusals, in call order.
    pub refusals: Vec<PlaneRefusal>,
    /// Attachment flushes the stage lost while taking this one: its depth
    /// overflowed, or a head it moved can never reach the wire.
    pub lost: Vec<Lost>,
}

/// What a kill took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Killed {
    /// Whether this call was the transition. `false` for an instance already
    /// terminal, so a caller reports a failure once.
    pub first: bool,
    /// Whole flushes discarded from its outbox and its stage.
    pub discarded: u64,
    /// The `BlobClose` frames for the handles the discarded flushes held.
    pub frames: Vec<ClientFrame>,
    /// The retry timer's instruction, when it changed.
    pub retry_wakeup: Option<TimerChange>,
}

/// Commit one ok activation's buffer.
///
/// `stamps` are the envelope identities the driver minted, one per buffered
/// publish. Stamped for every entry rather than only the confined ones because
/// only this layer knows which is which, and a transportable entry discards its
/// stamp — the peer mints the authoritative envelope.
///
/// # Panics
///
/// If `stamps` does not match the buffered publishes one for one, if the instance
/// holds no scheduler state, or if a confined publish reaches a page with no
/// identity yet. The last is unreachable by construction: an activation implies
/// wiring, wiring arrives on an attachment, and the attachment's principal
/// outlives it — and an unattributable confined envelope is exactly what the
/// identity model exists to prevent.
///
/// A completion for an instance that deregistered mid-flight is the **caller's**
/// to absorb before it gets here: it holds the activation entry, and nothing in
/// this layer can order the two.
pub fn flush_ok<P: PlanePolicy>(
    ctx: &mut FlushCtx<'_, P>,
    instance: &str,
    buffer: PublishBuffer,
    stamps: Vec<MessageStamp>,
) -> FlushReport {
    let flushed = buffer.take();
    assert_eq!(
        flushed.publishes.len(),
        stamps.len(),
        "surface client: one envelope stamp per buffered publish"
    );
    ctx.schedules.finish_ok(instance, flushed.carry);
    let mut report = FlushReport::default();
    let (wire_ops, confined_ops): (Vec<BufferedDeferOp>, Vec<BufferedDeferOp>) = flushed
        .defer_ops
        .into_iter()
        .partition(|op| channel_is_transportable(&op.channel));
    apply_confined_ops(ctx, instance, confined_ops, &mut report);
    let entries = route_publishes(ctx, instance, flushed.publishes, stamps, &mut report);
    let flush = StagedFlush {
        entries,
        ops: wire_ops.into_iter().map(wire_op).collect(),
    };
    if !flush.entries.is_empty() || !flush.ops.is_empty() {
        let depth = ctx
            .bindings
            .component(instance)
            .unwrap_or_else(|| {
                panic!("surface client: {instance} flushed with no component entry in force")
            })
            .parked_batch_depth;
        HandOver {
            stage: ctx.stage,
            plane: ctx.plane,
            outbound: ctx.outbound,
            bindings: ctx.bindings,
            facts: ctx.facts,
            now: ctx.now,
        }
        .drive(&mut report.steps, &mut report.lost, |stage, plane, gate| {
            stage.push(instance, flush, depth, plane, gate)
        });
    }
    report
}

/// Discard one failed activation's buffer, returning its budget.
///
/// # Panics
///
/// If the instance holds no scheduler state, as [`flush_ok`] does.
pub fn discard_err<P>(ctx: &mut FlushCtx<'_, P>, instance: &str, buffer: PublishBuffer) {
    ctx.schedules.finish_err(instance, buffer.into_carry());
}

/// Take `instance` terminal: nothing more is delivered to it, and the flushes it
/// has queued die with it.
///
/// Its channels' stores stay, and so do its subscription references: a channel
/// does not stop retaining because one of its readers died, and a terminal
/// instance releasing a channel its siblings read would turn one component's
/// death into their outage.
///
/// # Panics
///
/// If the instance is not registered, or holds no scheduler state.
pub fn kill<P>(ctx: &mut FlushCtx<'_, P>, instance: &str) -> Killed {
    if !ctx.registrations.fail(instance, ctx.stores) {
        return Killed {
            first: false,
            discarded: 0,
            frames: Vec::new(),
            retry_wakeup: None,
        };
    }
    ctx.schedules.finish_terminal(instance);
    let Discarded {
        flushes,
        released,
        retry_wakeup,
    } = ctx.outbound.discard_parked(instance, ctx.now);
    let (mut frames, staged) = ctx.stage.discard(instance, ctx.plane);
    frames.extend(close_released(ctx.plane, released));
    Killed {
        first: true,
        discarded: flushes + staged,
        frames,
        retry_wakeup,
    }
}

/// Close each handle a finished flush held, once however many attachments
/// named it.
pub(crate) fn close_released(
    plane: &mut PagePlane,
    mut released: Vec<brenn_attach_proto::BlobHandle>,
) -> Vec<ClientFrame> {
    released.sort_unstable();
    released.dedup();
    released
        .into_iter()
        .flat_map(|handle| plane.close(handle))
        .collect()
}

/// Apply one activation's control ops against confined channels, in call order.
///
/// Each op names its message by the identity the component's own deferred window
/// carried, resolved when the component made the call, so the only thing that can
/// have changed by now is whether that message is still parked. Gone is the
/// benign race a conforming component can always lose — it had already returned
/// by the time the race was resolvable — and is counted rather than reported.
///
/// An edit's replacement body runs the plane's guard inside the router, exactly as
/// a publish's body does; a refused edit changes nothing, schedule included.
fn apply_confined_ops<P: PlanePolicy>(
    ctx: &mut FlushCtx<'_, P>,
    instance: &str,
    ops: Vec<BufferedDeferOp>,
    report: &mut FlushReport,
) {
    for BufferedDeferOp {
        port,
        channel,
        message_id,
        kind,
    } in ops
    {
        if let DeferOp::Edit {
            body: Some(body), ..
        } = &kind
        {
            assert_carried_in_force(ctx.bindings, instance, &port, &channel, body);
        }
        let answer = ctx.router.apply_op(
            ctx.stores,
            DeferOpRequest {
                channel: &channel,
                origin: Origin::Sub(instance),
                message_id,
                op: kind,
                now: ctx.now_ms,
            },
        );
        match answer {
            DeferOpAnswer::Applied => {}
            DeferOpAnswer::NotParked { deliver_after } => {
                // The two fields are what separates a component that lost a
                // benign race from one that acted on an entry its own deferred
                // window showed as already due, which the doctrine forbids and
                // nothing else here counts.
                tracing::info!(
                    instance,
                    %port,
                    %channel,
                    deliver_after,
                    now = ctx.now_ms,
                    "surface client: deferred control op is a no-op — the target is past its \
                     release time at flush"
                );
                ctx.schedules.count_deferred_race(instance);
            }
            DeferOpAnswer::Refused { reason } => report.refusals.push(PlaneRefusal {
                port,
                channel,
                reason,
            }),
        }
    }
}

/// Route the confined entries of one activation's buffer and collect the
/// transportable ones for its wire flush, in call order, each attachment still
/// the blob its handle held.
fn route_publishes<P: PlanePolicy>(
    ctx: &mut FlushCtx<'_, P>,
    instance: &str,
    publishes: Vec<BufferedPublish>,
    stamps: Vec<MessageStamp>,
    report: &mut FlushReport,
) -> Vec<StagedEntry> {
    let mut entries = Vec::new();
    for (publish, stamp) in publishes.into_iter().zip(stamps) {
        let BufferedPublish {
            port,
            channel,
            body,
            urgency,
            // The wire carries a concrete urgency, so the resolved value goes out
            // and the raw override has no reader on this path.
            urgency_override: _,
            deliver_after,
            attachments,
        } = publish;
        if channel_is_transportable(&channel) {
            // The stamp is discarded: the peer mints the authoritative envelope.
            // The release time rides verbatim — this channel's deferral authority
            // is the peer, so the page states the time and the peer decides
            // park-vs-immediate against its own clock.
            entries.push(StagedEntry {
                channel,
                body,
                urgency,
                deliver_after,
                attachments,
            });
            continue;
        }
        // The page is the authority here, so this commits now: minted, retained,
        // and by that single append every reader bound to the channel woken. It
        // never touches the wire, so a down link is no reason to delay it.
        assert_carried_in_force(ctx.bindings, instance, &port, &channel, &body);
        let attachments = commit_attachments(ctx.page_blobs, &attachments);
        let outcome = ctx.router.route_attached(
            ctx.stores,
            RouteRequest {
                channel: &channel,
                origin: Origin::Sub(instance),
                body,
                stamp,
                urgency,
                deliver_after,
            },
            attachments,
        );
        match outcome {
            RouteOutcome::Routed { overflow } => {
                report.drops.merge(
                    ctx.schedules
                        .charge_overflow(ctx.bindings, &channel, overflow),
                );
            }
            // Nothing is retained and nobody is woken until the release pass takes
            // it, which the caller's release timer drives.
            RouteOutcome::Parked { .. } => {}
            RouteOutcome::ScheduleDropped { cap } => {
                tracing::warn!(
                    instance,
                    %port,
                    %channel,
                    cap,
                    "surface client: deferred publish dropped — the channel's deferred set is full"
                );
                ctx.schedules.count_deferred_drop(instance);
            }
            RouteOutcome::Refused { reason } => report.refusals.push(PlaneRefusal {
                port,
                channel,
                reason,
            }),
            RouteOutcome::NoIdentity => panic!(
                "surface client: {instance} published on {channel} before the page had an identity"
            ),
        }
    }
    entries
}

/// Record each attachment of one confined publish in the page's blob table and
/// answer the references its envelope carries.
///
/// An entry the commit does not end up naming — the plane refused the body or
/// the schedule was dropped — is removed by the table's sweep at the end of the
/// turn.
fn commit_attachments(
    page_blobs: &mut PageBlobs,
    attachments: &[BufferedAttachment],
) -> Attachments {
    Attachments::from_map(
        attachments
            .iter()
            .map(|attachment| (attachment.name.clone(), page_blobs.commit(&attachment.blob)))
            .collect(),
    )
}

/// Assert that the wiring in force lets `channel` carry `body`, for a confined
/// body about to come to rest in a store.
///
/// The buffer judged the body's carriage when the component published, under
/// the document its activation was seeded with. An activation is assembled,
/// run and committed in one synchronous stretch and a document is applied only
/// between stretches, so that document is the one in force here and this never
/// fires. A driver that broke the stretch would otherwise leave a non-text body
/// in a store its channel's declaration no longer admits, in front of readers
/// that may only represent text.
///
/// # Panics
///
/// If the wiring in force refuses the body.
fn assert_carried_in_force(
    bindings: &AppliedBindings,
    instance: &str,
    port: &str,
    channel: &str,
    body: &MessageBody,
) {
    assert!(
        channel_carries(bindings.carries_non_text(channel), body),
        "surface client: {instance}'s port {port} committing {} to {channel}, which the wiring \
         in force declares text-only: the buffer was judged under a bindings document other \
         than the one in force",
        body.summary()
    );
}

/// One buffered control op as the batch frame carries it.
pub(crate) fn wire_op(op: BufferedDeferOp) -> BatchDeferredOp {
    let BufferedDeferOp {
        channel,
        message_id,
        kind,
        port: _,
    } = op;
    BatchDeferredOp {
        channel,
        message_id,
        op: match kind {
            DeferOp::Cancel => DeferredOpKind::Cancel,
            DeferOp::Edit {
                body,
                deliver_after,
            } => DeferredOpKind::Edit {
                body,
                deliver_after,
            },
        },
    }
}

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
//!   become one `PublishBatch` offered to the instance's outbox.
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
use brenn_attach_client::publish::{Discarded, FlushBatch, OutboxSteps, TimerChange};
use brenn_attach_client::router::{
    DeferOpAnswer, DeferOpRequest, LocalRouter, MessageStamp, Origin, PlanePolicy, RouteOutcome,
    RouteRequest,
};
use brenn_attach_client::store::DeferOp;
use brenn_attach_proto::{BatchDeferredOp, BatchEntry, DeferredOpKind};

use crate::activation::{DropVerdicts, Schedules};
use crate::bindings::AppliedBindings;
use crate::core::channel_is_transportable;
use crate::outbound::SurfaceOutbound;
use crate::publish_buffer::{BufferedDeferOp, BufferedPublish, PublishBuffer};
use crate::registry::{Registrations, SurfaceStores};

/// Everything a completion writes to: the wiring it resolves against, the two
/// deferral authorities and the retention behind them, the outboxes, and the
/// tables that account for the instance.
///
/// One bundle rather than a positional list, for the reason
/// [`crate::activation::ActivationCtx`] is one: the members travel together, and
/// a completion touches more of the page than an assembly does.
pub struct FlushCtx<'a, P> {
    /// The wiring in force. Read only to resolve the noise of a binding a
    /// confined append overflowed — every address a flush writes was captured
    /// when the component published.
    pub bindings: &'a AppliedBindings,
    /// The attachment's contract, or `None` while detached. What a transportable
    /// flush is judged against before it is offered the wire: the buffer captured
    /// its channels and its body cap when the component published, and both can
    /// have moved since.
    pub facts: Option<&'a AttachmentFacts>,
    pub stores: &'a mut SurfaceStores,
    pub router: &'a mut LocalRouter<P>,
    pub outbound: &'a mut SurfaceOutbound,
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
}

/// What a kill took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Killed {
    /// Whether this call was the transition. `false` for an instance already
    /// terminal, so a caller reports a failure once.
    pub first: bool,
    /// Whole flushes discarded from its outbox.
    pub discarded: u64,
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
    let batch = FlushBatch {
        entries,
        ops: wire_ops.into_iter().map(wire_op).collect(),
    };
    if !batch.is_empty() {
        report.steps = ctx
            .outbound
            .flush(ctx.bindings, ctx.facts, instance, batch, ctx.now);
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
            retry_wakeup: None,
        };
    }
    ctx.schedules.finish_terminal(instance);
    let Discarded {
        flushes,
        retry_wakeup,
    } = ctx.outbound.discard_parked(instance, ctx.now);
    Killed {
        first: true,
        discarded: flushes,
        retry_wakeup,
    }
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
            DeferOpAnswer::NotParked => {
                tracing::info!(
                    instance,
                    %port,
                    %channel,
                    "surface client: deferred control op is a no-op — the message released between \
                     the activation's snapshot and the flush"
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
/// transportable ones for its batch, in call order.
fn route_publishes<P: PlanePolicy>(
    ctx: &mut FlushCtx<'_, P>,
    instance: &str,
    publishes: Vec<BufferedPublish>,
    stamps: Vec<MessageStamp>,
    report: &mut FlushReport,
) -> Vec<BatchEntry> {
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
        } = publish;
        if channel_is_transportable(&channel) {
            // The stamp is discarded: the peer mints the authoritative envelope.
            // The release time rides verbatim — this channel's deferral authority
            // is the peer, so the page states the time and the peer decides
            // park-vs-immediate against its own clock.
            entries.push(BatchEntry {
                channel,
                body,
                urgency,
                deliver_after,
            });
            continue;
        }
        // The page is the authority here, so this commits now: minted, retained,
        // and by that single append every reader bound to the channel woken. It
        // never touches the wire, so a down link is no reason to delay it.
        let outcome = ctx.router.route(
            ctx.stores,
            RouteRequest {
                channel: &channel,
                origin: Origin::Sub(instance),
                body,
                stamp,
                urgency,
                deliver_after,
            },
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

/// One buffered control op as the batch frame carries it.
fn wire_op(op: BufferedDeferOp) -> BatchDeferredOp {
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

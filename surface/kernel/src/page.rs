//! Everything one surface page holds, and what an attachment's two phases do to
//! it.
//!
//! The layers below this one each own one question — where the wiring comes from,
//! what the page retains, what it subscribes, what it writes, how an activation is
//! scheduled, what its confined planes admit. None of them can be driven alone: a
//! bindings document reaches the stores, the registration table, the subscription
//! plane, the outboxes and the plane policy in one pass, in an order that matters.
//! [`SurfacePage`] is the aggregate those passes run over, and its methods are the
//! two edges of an attachment plus the one that puts a document in force:
//!
//! 1. [`on_attached`](SurfacePage::on_attached) — phase 1. The page learns who it
//!    is on this connection and subscribes the channel its wiring is retained on.
//!    Nothing else moves: what it *may* subscribe is what the document about to
//!    arrive says.
//! 2. [`apply_config`](SurfacePage::apply_config) — phase 2. The document is
//!    applied and everything the page holds is reconciled against it, in one
//!    order: plane policy, stores, positions and subscriptions, outboxes, then the
//!    flushes that were waiting for a wire.
//! 3. [`on_detached`](SurfacePage::on_detached) — the attachment went away. The
//!    wiring stays (it is what the page is still running on, and what the next
//!    document is compared against); everything per-connection goes.
//!
//! Nothing here reads a clock, opens a socket, or decides what a loss means. Every
//! pass answers what it cost — frames to send, verdicts for the loudness ladder,
//! flushes that died, callers owed an answer — and the layer above enacts it. That
//! is the same seam every surface-side module already answers across, applied to
//! the aggregate of them.

#[cfg(test)]
mod tests;

use brenn_attach_client::Millis;
use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_client::publish::{DeferredViews, OutboxSteps};
use brenn_attach_client::router::LocalRouter;
use brenn_attach_client::subs::{SubscribeAck, Subscriptions};
use brenn_attach_proto::{BlobFacts, ClientFrame};
use brenn_envelope::surface_sub_identity;
use uuid::Uuid;

use crate::activation::{DropVerdicts, Schedules};
use crate::bindings::AppliedBindings;
use crate::blob_plane::PagePlane;
use crate::blob_scope::HandleNumbers;
use crate::connect::{ConfigApplied, SurfaceConnect};
use crate::fetch::Fetch;
use crate::outbound::{LostFlush, PageGate, PublishAnswer, SurfaceOutbound};
use crate::page_blobs::PageBlobs;
use crate::page_bytes::MadeLedger;
use crate::planes::SurfacePlanes;
use crate::registry::{Registrations, StoreReconcile, SurfaceStores, new_stores, reconcile_stores};
use crate::uploads::{Lost, Stage, StageGate, StageStep};

/// One page's whole state: the attachment it has, the wiring in force, and the
/// six tables that wiring is enacted over.
///
/// The fields are public because the passes above this layer bundle borrows of
/// them — an activation's assembly and a completion's flush each take a context
/// struct over a disjoint subset — and a private field with an accessor per pass
/// would be the same access spelled twice.
pub struct SurfacePage {
    /// The attachment's facts, the config channel's custody, and the wiring in
    /// force.
    pub connect: SurfaceConnect,
    /// What the page retains, per channel, for both classes.
    pub stores: SurfaceStores,
    /// Which component instances are registered, and what each holds because of
    /// it.
    pub registrations: Registrations,
    /// The wire subscriptions the page holds open.
    pub subs: Subscriptions,
    /// What the page writes: single publishes awaiting an answer, and the
    /// per-instance flush outboxes.
    pub outbound: SurfaceOutbound,
    /// Per-instance scheduler state: what is in flight, and every counter an
    /// activation accrues.
    pub schedules: Schedules,
    /// The page's own authority over its confined channels, carrying the surface's
    /// plane policy.
    pub router: LocalRouter<SurfacePlanes>,
    /// The peer's mirror of what each of the page's senders has parked on a
    /// transportable channel.
    pub views: DeferredViews,
    /// What the page fetches of delivered attachments: the arrival holds, the
    /// attachment cache, and the blob plane. Fetched bytes and held rows outlive
    /// a detach; fetches in flight do not.
    pub fetch: Fetch,
    /// The page's one blob plane, which the fetcher and the upload stage
    /// share: the handles of the live attachment and the size behind each.
    pub plane: PagePlane,
    /// What each attachment of a page-local message names. Outlives a detach,
    /// as the page-local stores do.
    pub page_blobs: PageBlobs,
    /// Attachment flushes held per instance until the blobs they name are
    /// held on the live attachment. Outlives a detach, as the outboxes do.
    pub stage: Stage,
    /// The publish-body cap the most recent attachment stated, `0` before the
    /// first one.
    ///
    /// Retained across a detach, unlike the rest of the attachment's facts: the
    /// page keeps activating the components that read its confined planes while
    /// the link is down — chrome drawing the reconnect banner is the case that
    /// matters — and a component's body-size contract must not change because the
    /// link dropped. What a *transportable* flush is judged against is the live
    /// contract, which is why the outbox reads it from the facts instead.
    pub body_cap: u64,
    /// The page's count of page-made blob bytes held, which every writer and
    /// sealed blob debits while it lives. Bounded by `PAGE_MADE_BLOB_BYTES`.
    pub made: MadeLedger,
    /// The blob door the most recent attachment advertised, `None` before the
    /// first one or when it advertised none.
    ///
    /// Retained across a detach, as the body cap is: a component's `create`,
    /// `write` and `publish` are judged against it while the link is down, and
    /// what it publishes then is staged and judged against the live door before
    /// it is uploaded.
    pub blob_door: Option<BlobFacts>,
    /// The one source every activation's blob scope mints its handle numbers
    /// from, so a number names one writer or blob in the page's life.
    pub handle_numbers: HandleNumbers,
}

/// What a detach left to answer for.
#[derive(Debug, Default, PartialEq)]
pub struct Detached {
    /// Callers whose publish went out on the connection that just died. Nothing is
    /// coming for them, so each is answered `ConnectionLost`.
    pub answers: Vec<PublishAnswer>,
    /// The outboxes' answer: the retry timer disarmed, and no frames — a queued
    /// flush is still owed the wire and waits for the next attachment.
    pub steps: OutboxSteps<String>,
    /// Attachment flushes whose batch was on the wire, unanswered: the server
    /// may have applied them, so they are never sent again.
    pub lost: Vec<Lost>,
}

/// What putting a bindings document in force cost and produced.
#[derive(Debug, Default, PartialEq)]
pub struct Configured {
    /// Whether this is the first document of the current attachment — phase 2
    /// proper, after which the page is connected.
    pub first_of_attachment: bool,
    /// Whether the wiring differs from what was in force before it: the caller's
    /// cue to reload the page.
    pub wiring_changed: bool,
    /// Frames the reconcile composed, in order — the subscriptions this document
    /// closes, then the ones it opens.
    pub frames: Vec<ClientFrame>,
    /// The outboxes' answer once the wire is theirs: the surviving heads to send,
    /// the flushes this document's wiring dropped, and the retry timer.
    pub steps: OutboxSteps<String>,
    /// The loudness ladder's verdicts for positions a depth shrink retired out
    /// from under. An operator lowering a depth is as accountable a cause of loss
    /// as a burst.
    pub drops: DropVerdicts,
    /// Flushes that died with an outbox this document closed — an instance no
    /// longer registered, whose queue nobody is left to answer for. Each names the
    /// instance whose writes vanished.
    pub lost_flushes: Vec<LostFlush>,
    /// Attachment flushes the stage lost to this document: they no longer pass
    /// the checks a held flush is judged by, or the door can never take them.
    pub staged_lost: Vec<Lost>,
}

impl SurfacePage {
    /// A page at its first instant: reserved control planes seeded at their
    /// contract depths, no attachment, no wiring, nothing registered.
    ///
    /// `config_channel` is the address the page's bindings document is retained
    /// on, from the page's boot identity. `epoch` stamps the page's own stores, so
    /// a cursor minted against them cannot be mistaken for one from a previous
    /// page.
    ///
    /// # Panics
    ///
    /// If `config_channel` is empty or does not cross the wire — see
    /// [`SurfaceConnect::new`].
    pub fn new(config_channel: String, epoch: Uuid) -> Self {
        Self {
            connect: SurfaceConnect::new(config_channel),
            stores: new_stores(epoch),
            registrations: Registrations::new(),
            subs: Subscriptions::new(),
            outbound: SurfaceOutbound::new(),
            schedules: Schedules::new(),
            router: LocalRouter::new(SurfacePlanes::new()),
            views: DeferredViews::new(),
            fetch: Fetch::new(),
            plane: PagePlane::new(),
            page_blobs: PageBlobs::new(),
            stage: Stage::new(),
            body_cap: 0,
            made: MadeLedger::new(),
            blob_door: None,
            handle_numbers: HandleNumbers::new(),
        }
    }

    /// Phase 1: the attachment is live.
    ///
    /// Five things, and deliberately nothing else. The page takes the
    /// attachment's identity, which is what every confined envelope it mints is
    /// attributed to, and its body cap, which every activation's buffer enforces.
    /// It drops every deferred-view mirror: the peer re-seeds only
    /// the *nonempty* sets, immediately behind its `Welcome`, so an unmentioned
    /// pair means an empty set — and a retained mirror would show a schedule that
    /// released while the page was away. It hands the attachment's blob door to
    /// the page's blob plane; nothing is fetched until a pass runs. And it subscribes the
    /// config channel, which is the one frame this phase sends.
    ///
    /// The wiring from the previous attachment is untouched. The page is still
    /// running on it, and it is what the document about to arrive is compared
    /// against.
    ///
    /// # Panics
    ///
    /// If an attachment is already live — see
    /// [`SurfaceConnect::on_attached`].
    pub fn on_attached(&mut self, facts: AttachmentFacts) -> Vec<ClientFrame> {
        self.router.set_principal(facts.participant_id.clone());
        self.body_cap = facts.max_body_bytes;
        self.blob_door = facts.blobs().copied();
        self.views.clear();
        self.plane.on_attached(&facts);
        self.connect.on_attached(facts, &mut self.subs)
    }

    /// The attachment went away.
    ///
    /// Everything that belonged to the connection goes: the config channel's
    /// subscription, the wire state of every other one, the publishes that were
    /// awaiting an answer, the batches and attachment fetches that were in
    /// flight. Everything that belongs to the *page* stays — the wiring, the
    /// stores and their positions, the resume cursors, the registrations, every
    /// queued flush, the arrival holds and the fetched attachment bytes.
    ///
    /// Tolerates a detach with no attachment behind it: a connection lost while
    /// negotiating reports one, and the page never got a phase 1.
    pub fn on_detached(&mut self) -> Detached {
        self.connect.on_detached(&mut self.subs);
        self.plane.on_detached();
        self.fetch.on_detached();
        let answers = self.outbound.fail_pending();
        let mut steps = self.outbound.on_detached();
        // The stage takes the outbox's drop of a parked attachment flush back
        // before it is announced: that flush returns to staged.
        let StageStep {
            frames,
            handed,
            lost,
        } = self.stage.on_detached(&mut steps.dropped);
        assert!(
            frames.is_empty() && handed.is_empty(),
            "surface client: a detach composed stage frames or handed a flush over"
        );
        Detached {
            answers,
            steps,
            lost,
        }
    }

    /// Intake the config channel's own `SubscribeResult`.
    ///
    /// `Err` names a broken peer invariant for the caller to go fatal on — see
    /// [`SurfaceConnect::on_config_ack`].
    pub fn on_config_ack(&self, ack: &SubscribeAck) -> Result<(), String> {
        self.connect.on_config_ack(ack)
    }

    /// Phase 2: apply a bindings document and reconcile the page against it.
    ///
    /// `Err` names what makes the body unusable, for the caller to reload on: the
    /// body is peer skew, never a broken attachment. Nothing is half-applied:
    /// the document is parsed and checked whole before any table is touched.
    ///
    /// The order is the one the passes require of each other:
    ///
    /// 1. **Plane policy**, so a confined publish is judged against the wiring
    ///    that declares its writers rather than the previous one's.
    /// 2. **Stores**, before any position can be taken in one. A shrink retires
    ///    messages out from under lagging positions, which is charged here — and
    ///    a `fatal` rung fires before the position reconcile treats the instance
    ///    as the terminal one it now is.
    /// 3. **Positions and subscriptions**, before a single `Subscribe` goes out,
    ///    so the set the page opens is exactly the set this document authorizes.
    /// 4. **Outboxes**, opening one for every registered instance the document
    ///    declares — which is what carries a registration made before the page's
    ///    first document into a page that has one.
    /// 5. **The queued flushes**, re-validated against this attachment's contract
    ///    and offered the wire. They are older than anything the reconnected page
    ///    will produce, and the activations that made them already returned ok.
    ///
    /// Runs whole for a second document mid-attachment too. A healthy peer sends
    /// only one, but the input is well-formed, and reconciling against it is the
    /// same conservative answer a reconnect gets — the caller reloads on
    /// `wiring_changed` regardless, and until it does the page must agree with the
    /// document it was last told about.
    ///
    /// # Panics
    ///
    /// If no attachment is live — the document arrives on a subscription that only
    /// exists while attached.
    pub fn apply_config(&mut self, body: &str, now: Millis) -> Result<Configured, String> {
        let ConfigApplied {
            first_of_attachment,
            wiring_changed,
        } = self.connect.on_config_deliver(body)?;
        // Field-wise, because every pass below reads the wiring out of `connect`
        // while mutating one of its siblings.
        let Self {
            connect,
            stores,
            registrations,
            subs,
            outbound,
            schedules,
            router,
            views: _,
            fetch: _,
            plane,
            page_blobs: _,
            stage,
            body_cap: _,
            made: _,
            blob_door: _,
            handle_numbers: _,
        } = self;
        let bindings = connect
            .bindings()
            .expect("surface client: the document just applied is the wiring in force");
        let facts = connect
            .facts()
            .expect("surface client: phase 2 runs on a live attachment");
        router.policy_mut().apply(bindings);
        let StoreReconcile {
            retired,
            lost_schedules,
        } = reconcile_stores(bindings, stores);
        let mut drops = DropVerdicts::default();
        for (channel, overflow) in retired {
            drops.merge(schedules.charge_overflow(bindings, &channel, overflow));
        }
        charge_lost_schedules(
            &lost_schedules,
            router.principal(),
            registrations,
            schedules,
        );
        let mut frames = registrations.reconcile(bindings, stores, subs);
        // After the positions exist: a mount that was registered before the page's
        // first document is owed its guaranteed activation from the moment the
        // wiring it windows against is in force.
        schedules.wire_mounts();
        frames.extend(subs.resubscribe_survivors());
        let lost_flushes = outbound.reconcile(bindings, registrations.instances());
        let mut steps = outbound.on_attached(bindings, facts, now);
        // Held flushes are judged by this document before anything moves, then
        // the stage may start: the queued outbox flushes above went first.
        let mut staged_lost = Vec::new();
        HandOver {
            stage,
            plane,
            outbound,
            bindings,
            facts: Some(facts),
            now,
        }
        .drive(&mut steps, &mut staged_lost, |stage, plane, gate| {
            let mut step =
                stage.revalidate(plane, |instance, flush| gate.survives(instance, flush));
            step.absorb(stage.on_ready(plane, gate));
            step
        });
        Ok(Configured {
            first_of_attachment,
            wiring_changed,
            frames,
            steps,
            drops,
            lost_flushes,
            staged_lost,
        })
    }

    /// The wiring in force, or `None` before the page's first document.
    pub fn bindings(&self) -> Option<&AppliedBindings> {
        self.connect.bindings()
    }
}

/// One move of the stage and the hand-over of what it answers, judged
/// throughout by one wiring and one contract: the stage is asked with the gate
/// built from these fields, and the outboxes are offered its flushes under the
/// same ones.
pub struct HandOver<'a> {
    pub stage: &'a mut Stage,
    pub plane: &'a mut PagePlane,
    pub outbound: &'a mut SurfaceOutbound,
    pub bindings: &'a AppliedBindings,
    pub facts: Option<&'a AttachmentFacts>,
    pub now: Millis,
}

impl HandOver<'_> {
    /// Run `step` — one stage operation, given the stage, the plane and the
    /// gate — and enact what it answers with [`hand_over`].
    pub fn drive(
        self,
        steps: &mut OutboxSteps<String>,
        lost: &mut Vec<Lost>,
        step: impl FnOnce(&mut Stage, &mut PagePlane, PageGate<'_>) -> StageStep,
    ) {
        let Ok(()) = self.try_drive(steps, lost, |stage, plane, gate| {
            Ok::<_, std::convert::Infallible>(step(stage, plane, gate))
        });
    }

    /// [`drive`](Self::drive) for a stage operation that can fail; on `Err`
    /// nothing is handed over.
    pub fn try_drive<E>(
        self,
        steps: &mut OutboxSteps<String>,
        lost: &mut Vec<Lost>,
        step: impl FnOnce(&mut Stage, &mut PagePlane, PageGate<'_>) -> Result<StageStep, E>,
    ) -> Result<(), E> {
        let gate = page_gate(self.bindings, self.facts, self.outbound);
        let step = step(self.stage, self.plane, gate)?;
        hand_over(self, step, steps, lost);
        Ok(())
    }
}

/// The gate the stage is judged by under `bindings` and `facts`.
fn page_gate<'g>(
    bindings: &'g AppliedBindings,
    facts: Option<&'g AttachmentFacts>,
    outbound: &'g SurfaceOutbound,
) -> PageGate<'g> {
    PageGate {
        bindings,
        facts,
        outbound,
    }
}

/// Enact one stage step: its frames are sent ahead of what the outboxes
/// compose, its losses are collected, and each flush it handed over is offered
/// to its instance's outbox through [`SurfaceOutbound::flush`].
///
/// # Panics
///
/// If the outbox refuses an attachment flush as it is offered. The stage
/// judged the flush by the same `survives` under the same wiring and contract
/// — every document re-judges each flush not yet handed, and a contract comes
/// only with an attachment, whose document is applied before the stage moves
/// — measuring each handle at its widest; and the instance's outbox was idle,
/// with every later flush of the instance staged behind this one.
fn hand_over(
    ctx: HandOver<'_>,
    step: StageStep,
    steps: &mut OutboxSteps<String>,
    lost: &mut Vec<Lost>,
) {
    let HandOver {
        stage: _,
        plane: _,
        outbound,
        bindings,
        facts,
        now,
    } = ctx;
    let StageStep {
        frames,
        handed,
        lost: lost_here,
    } = step;
    steps.frames.extend(frames);
    lost.extend(lost_here);
    for (instance, batch) in handed {
        let names_handles = batch.handles().next().is_some();
        let offered = outbound.flush(bindings, facts, &instance, batch, now);
        assert!(
            !(names_handles && offered.dropped.contains(&instance)),
            "surface client: the outbox refused {instance}'s attachment flush, which the stage \
             judged under the same wiring and contract"
        );
        merge_steps(steps, offered);
    }
}

/// Fold a later outbox answer into an earlier one of the same turn.
pub fn merge_steps(into: &mut OutboxSteps<String>, later: OutboxSteps<String>) {
    let OutboxSteps {
        frames,
        dropped,
        released,
        retry_wakeup,
    } = later;
    into.frames.extend(frames);
    into.dropped.extend(dropped);
    into.released.extend(released);
    if retry_wakeup.is_some() {
        into.retry_wakeup = retry_wakeup;
    }
}

/// Charge each schedule a dropped confined store was holding to the instance that
/// parked it.
///
/// A store's deferred set is keyed by *sender*, which is where the page's own
/// identity grammar comes back in: an instance's schedules are parked under
/// `<principal>#<instance>`, so the charge is a lookup of the registered instance
/// whose sub-identity the sender spells. A sender no registered instance answers
/// for is left uncounted — its schedules are as lost either way, and there is
/// nobody's counter to put them on.
///
/// The loss itself is never silent: [`reconcile_stores`] warns per channel, since
/// a dropped schedule is the only account of a timer a component believes it set.
fn charge_lost_schedules(
    lost: &[(String, Vec<String>)],
    principal: Option<&str>,
    registrations: &Registrations,
    schedules: &mut Schedules,
) {
    // No identity yet means no attachment has completed, so nothing has been
    // parked under one either.
    let Some(principal) = principal else {
        return;
    };
    for (_, senders) in lost {
        for sender in senders {
            let charged = registrations
                .instances()
                .find(|instance| surface_sub_identity(principal, instance) == *sender);
            if let Some(instance) = charged {
                schedules.count_deferred_drop(instance);
            }
        }
    }
}

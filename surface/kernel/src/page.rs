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
use brenn_attach_client::publish::{DeferredViews, FlushBatch, OutboxSteps};
use brenn_attach_client::router::LocalRouter;
use brenn_attach_client::subs::{SubscribeAck, Subscriptions};
use brenn_attach_proto::ClientFrame;
use brenn_envelope::surface_sub_identity;
use uuid::Uuid;

use crate::activation::{DropVerdicts, Schedules};
use crate::bindings::AppliedBindings;
use crate::connect::{ConfigApplied, SurfaceConnect};
use crate::outbound::{PublishAnswer, SurfaceOutbound};
use crate::planes::SurfacePlanes;
use crate::registry::{Registrations, StoreReconcile, SurfaceStores, new_stores, reconcile_stores};

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
    /// longer registered, whose queue nobody is left to answer for.
    pub lost_flushes: Vec<FlushBatch>,
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
        }
    }

    /// Phase 1: the attachment is live.
    ///
    /// Three things, and deliberately nothing else. The page takes the
    /// attachment's identity, which is what every confined envelope it mints is
    /// attributed to. It drops every deferred-view mirror: the peer re-seeds only
    /// the *nonempty* sets, immediately behind its `Welcome`, so an unmentioned
    /// pair means an empty set — and a retained mirror would show a schedule that
    /// released while the page was away. And it subscribes the config channel,
    /// which is the one frame this phase sends.
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
        self.views.clear();
        self.connect.on_attached(facts, &mut self.subs)
    }

    /// The attachment went away.
    ///
    /// Everything that belonged to the connection goes: the config channel's
    /// subscription, the wire state of every other one, the publishes that were
    /// awaiting an answer, and the batches that were in flight. Everything that
    /// belongs to the *page* stays — the wiring, the stores and their positions,
    /// the resume cursors, the registrations, and every queued flush.
    ///
    /// Tolerates a detach with no attachment behind it: a connection lost while
    /// negotiating reports one, and the page never got a phase 1.
    pub fn on_detached(&mut self) -> Detached {
        self.connect.on_detached(&mut self.subs);
        Detached {
            answers: self.outbound.fail_pending(),
            steps: self.outbound.on_detached(),
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
    /// `Err` names what makes the body unusable, for the caller to go fatal on.
    /// Nothing is half-applied: the document is parsed and checked whole before
    /// any table is touched.
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
        frames.extend(subs.resubscribe_survivors());
        let lost_flushes = outbound.reconcile(bindings, registrations.instances());
        let steps = outbound.on_attached(bindings, facts, now);
        Ok(Configured {
            first_of_attachment,
            wiring_changed,
            frames,
            steps,
            drops,
            lost_flushes,
        })
    }

    /// The wiring in force, or `None` before the page's first document.
    pub fn bindings(&self) -> Option<&AppliedBindings> {
        self.connect.bindings()
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

//! The outward half of a turn: what the page does on its own account, between
//! the frames that arrive.
//!
//! [`crate::inbound`] is the half a peer drives. This is the half nothing drives
//! but the page itself: a component is ready, an activation returned, a schedule
//! matured, a refused flush is due another attempt. Each is one pass over the
//! aggregate [`SurfacePage`] holds, and each answers what it produced rather than
//! enacting it — the same seam every surface-side module answers across.
//!
//! # The four passes
//!
//! - [`dispatch`] picks the next ready instance and assembles its activation.
//! - [`on_activation_done`] takes the completion back: the buffer committed,
//!   discarded, or the instance taken terminal.
//! - [`on_release_due`] takes every confined channel's matured schedules into
//!   retention, which wakes their readers exactly as an arrival does.
//! - [`on_retry_tick`] offers every blocked outbox's head one more attempt.
//!
//! # Two deadlines, stated after every input
//!
//! Neither timer is armed at the site that moved it. A park at flush, a release
//! sweep, a control op and a discarded store all change when the next confined
//! message comes due, and a flush, a batch result and a detach all change whether
//! anything is waiting on the wire — so both are restated from the state itself,
//! through [`release_wakeup`] and the `retry_wakeup` every pass here carries.
//! One restatement over the whole page cannot be forgotten at a new site the way
//! per-site arming can.
//!
//! # What the page says out loud
//!
//! The loudness ladder's verdicts and an outbox's drops are data everywhere they
//! are raised. [`drop_notices`] and [`parked_drop_notices`] are where they become
//! the two things a page can actually say: an `Alert` frame for the operator, and
//! a toast on the page's own confined plane. Composed here, routed by the caller
//! — a toast is a confined publish, and minting one needs a stamp only the layer
//! that reads clocks can supply.

#[cfg(test)]
mod tests;

use brenn_attach_client::Millis;
use brenn_attach_client::publish::OutboxSteps;
use brenn_attach_client::router::{MessageStamp, ReleaseTimer, ReleasedChannel};
use brenn_attach_proto::{AlertSeverity, ClientFrame, MAX_ALERT_BODY_BYTES, MAX_ALERT_TITLE_BYTES};
use brenn_surface_schema::{
    CONTROL_PLANE_VERSION, LOCAL_TOAST_CHANNEL, ToastBody, ToastSeverity, ToastSource,
};

use crate::activation::{ActivationCtx, DropVerdicts, ReadyActivation};
use crate::core::{ActivationOutcome, truncate_report_field};
use crate::flush::{self, FlushCtx, Killed, PlaneRefusal};
use crate::page::SurfacePage;
use crate::planes::SurfacePlanes;
use crate::publish_buffer::PublishBuffer;

/// The instance the next [`dispatch`] would assemble for, or `None` when nothing
/// is ready — the wake question, at the grain a caller budgets a turn in.
///
/// Asking does not advance the rotation: this answers the same instance until an
/// assembly takes it.
pub fn ready(page: &SurfacePage) -> Option<&str> {
    page.schedules.ready(&page.registrations, &page.stores)
}

/// Assemble one activation for the next ready instance, or `None` when nothing is
/// ready.
///
/// `now_ms` is the wall clock the activation is read at, epoch milliseconds UTC:
/// it is what the component is shown as its `now`, and the cutoff its deferred
/// windows are scoped by.
///
/// The instance is in flight when this returns, so the caller owes exactly one
/// [`on_activation_done`] for it. Two consequences the caller must honour:
///
/// - A [`ReadyActivation::drops`] carrying a `fatal` verdict means the instance's
///   own binding overflowed past the rung that kills it. The caller [`kill`]s it
///   and does **not** invoke the entry — the assembly happened, so the buffer
///   exists, and discarding it is what the kill's own account of the flush is.
/// - The body cap the buffer enforces is the last attachment's, retained across a
///   detach. A page keeps activating the components reading its confined planes
///   while the link is down, and a component's body-size contract must not change
///   because the link dropped.
///
/// # Panics
///
/// If no bindings document is in force. A ready instance holds a position in a
/// store, which only the reconcile of a document creates.
pub fn dispatch(page: &mut SurfacePage, now_ms: u64) -> Option<ReadyActivation> {
    let SurfacePage {
        connect,
        stores,
        registrations,
        subs: _,
        outbound: _,
        schedules,
        router,
        views,
        body_cap,
    } = page;
    let instance = schedules.ready(registrations, stores)?.to_string();
    let generation = registrations
        .generation(&instance)
        .expect("surface client: a ready instance is registered");
    let bindings = connect
        .bindings()
        .expect("surface client: a ready activation implies a document in force");
    let mut ctx = ActivationCtx {
        bindings,
        stores,
        router,
        views,
        max_body_bytes: *body_cap,
        now_ms,
    };
    Some(schedules.assemble(&instance, generation, &mut ctx))
}

/// One activation's completion, as the caller hands it back.
///
/// A struct rather than a parameter list: the four travel together and the caller
/// is the invocation boundary, which is the one place that can put them in the
/// wrong order.
pub struct Completed {
    pub instance: String,
    /// The [`ReadyActivation::generation`] the assembly handed out — which mount
    /// of that instance ran.
    pub generation: u64,
    pub outcome: ActivationOutcome,
    /// The buffer this activation was seeded with, as the entry left it.
    pub buffer: PublishBuffer,
    /// One envelope stamp per buffered publish, minted at the driver. A
    /// transportable entry discards its stamp; the peer mints the authoritative
    /// envelope.
    pub stamps: Vec<MessageStamp>,
}

/// What one completion produced.
///
/// Carries the instance and the outcome back out, so the caller that folds this
/// into events reads them off the answer rather than holding its own copies
/// alongside the [`Completed`] it moved: a disagreement between the two would name
/// the wrong component on the failure while the kill inside landed on the right
/// one, and nothing would typecheck it wrong.
#[derive(Debug, PartialEq, Eq)]
pub struct Completion {
    /// Whose activation it was.
    pub instance: String,
    /// How the entry finished — what names the diagnostic the caller raises.
    pub outcome: ActivationOutcome,
    /// The transportable half's answer: frames to send, whole flushes lost, and
    /// the retry timer's instruction.
    pub steps: OutboxSteps<String>,
    /// The loudness ladder's verdicts for positions a confined append evicted. An
    /// instance's own publish can overflow a *sibling's* position, which is why
    /// each announcement names its own instance.
    pub drops: DropVerdicts,
    /// Plane refusals, in call order — the page wrote a body one of its own
    /// planes rejected, which is worth surfacing either way.
    pub refusals: Vec<PlaneRefusal>,
    /// Set when this completion took the instance terminal: a trap.
    pub killed: Option<Killed>,
    /// Whether the completion was absorbed because the page no longer activates the
    /// mount that produced it — it deregistered, it was replaced by a fresh
    /// registration under the same id, or it is already terminal. Nothing happened and
    /// nothing is owed — in particular the caller reports no failure for a trap it
    /// can no longer attribute, and no publish of a dead instance's reaches
    /// anything.
    pub absorbed: bool,
}

impl Completion {
    /// A completion that produced nothing, for the instance and outcome it names.
    /// The shape every leg starts from, since each produces only some of the rest.
    pub(crate) fn nothing(instance: String, outcome: ActivationOutcome) -> Self {
        Self {
            instance,
            outcome,
            steps: OutboxSteps::default(),
            drops: DropVerdicts::default(),
            refusals: Vec::new(),
            killed: None,
            absorbed: false,
        }
    }
}

/// Take one activation's completion.
///
/// Three shapes, and the two states that are not shapes at all. A completion whose
/// mount is **gone** has nowhere to land — no budget to return to, no outbox to
/// flush into — and that covers both the instance that deregistered mid-flight and
/// the one that deregistered and registered again, which is a different component
/// with the same spelling and must not inherit its predecessor's buffer, carry or
/// in-flight marker. That is why the completion is matched on the *generation* it
/// was assembled under rather than on the id alone. A completion whose instance was
/// **killed** mid-flight has nowhere it *may* land: the `fatal` rung is charged at
/// an arrival, at a depth shrink and at a sibling's confined append, all of which
/// can name an instance whose activation is already running, and the kill's own
/// account of the flush is that it is gone. Committing the buffer afterwards would
/// route a dead instance's confined publishes, wake its siblings, and put its batch
/// on the wire under a terminal attribution — after the platform half was told it
/// failed. Both are absorbed.
///
/// `now` is the driver's monotonic reading, for the outbox's retry deadline;
/// `now_ms` is its wall-clock reading, in the currency a release time is stated
/// in, so a control op resolves against the same instant the activation's own
/// schedule was shown at.
///
/// # Panics
///
/// If no bindings document is in force, if `stamps` does not match the buffered
/// publishes one for one, or if the instance holds no scheduler state.
pub fn on_activation_done(
    page: &mut SurfacePage,
    done: Completed,
    now: Millis,
    now_ms: u64,
) -> Completion {
    let Completed {
        instance,
        generation,
        outcome,
        buffer,
        stamps,
    } = done;
    if page.registrations.generation(&instance) != Some(generation)
        || page.registrations.is_failed(&instance)
    {
        // No budget is returned and no cap credited: a terminal instance has no next
        // activation to spend either on. The count is the whole account of what was
        // discarded here, matching the kill's posture over the parked queue.
        tracing::debug!(
            instance,
            publishes = buffer.len(),
            "surface client: absorbed the completion of an instance the page no longer activates"
        );
        return Completion {
            absorbed: true,
            ..Completion::nothing(instance, outcome)
        };
    }
    let mut ctx = flush_ctx(page, now, now_ms);
    let flushed = match &outcome {
        ActivationOutcome::Ok => {
            let report = flush::flush_ok(&mut ctx, &instance, buffer, stamps);
            Flushed {
                steps: report.steps,
                drops: report.drops,
                refusals: report.refusals,
                killed: None,
            }
        }
        ActivationOutcome::Err(_) => {
            flush::discard_err(&mut ctx, &instance, buffer);
            Flushed::default()
        }
        ActivationOutcome::Trap(_) => Flushed {
            killed: Some(flush::kill(&mut ctx, &instance)),
            ..Flushed::default()
        },
    };
    let Flushed {
        steps,
        drops,
        refusals,
        killed,
    } = flushed;
    Completion {
        steps,
        drops,
        refusals,
        killed,
        ..Completion::nothing(instance, outcome)
    }
}

/// What one completion's flush leg produced, before the identity goes back on it.
#[derive(Default)]
struct Flushed {
    steps: OutboxSteps<String>,
    drops: DropVerdicts,
    refusals: Vec<PlaneRefusal>,
    killed: Option<Killed>,
}

/// Take `instance` terminal.
///
/// The caller's move on a `fatal`-rung verdict, which every pass that charges the
/// ladder can raise: an assembly's own window, an arrival, a depth shrink, a
/// confined append. A trap reaches the same place through
/// [`on_activation_done`].
///
/// `now_ms` completes the completion context and a kill resolves nothing against
/// it; `now` is what the outbox's retry deadline is stated against.
///
/// # Panics
///
/// If the instance is not registered, holds no scheduler state, or if no bindings
/// document is in force.
pub fn kill(page: &mut SurfacePage, instance: &str, now: Millis, now_ms: u64) -> Killed {
    flush::kill(&mut flush_ctx(page, now, now_ms), instance)
}

/// What one release pass took into retention.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Released {
    /// The confined channels swept, in address order. Empty when the fire found
    /// nothing due, which is not an error: a wall clock that stepped back, or a
    /// timer that fired early, releases nothing.
    pub channels: Vec<String>,
    /// How many messages entered retention across every channel swept.
    pub released: usize,
    /// The ladder's verdicts for positions a release evicted. A release is an
    /// ordinary arrival, so it is as accountable a cause of loss as any other.
    pub drops: DropVerdicts,
}

/// Release every confined channel's matured schedules.
///
/// Each release is an ordinary arrival on its channel — a fresh tail seq, the same
/// eviction charges, every bound reader woken by it — exactly as an immediate
/// confined publish is, and it is the first moment the message is observable, so
/// it is where the plane policy records what it tracks. Channels are swept in
/// address order, so a page releasing on several at one fire enacts its loud rungs
/// reproducibly.
///
/// Honoured in every state, terminal ones included: the confined planes and their
/// readers outlive an attachment, and a fire absorbed without releasing would
/// leave the armed deadline permanently due.
///
/// # Panics
///
/// If something released and no bindings document is in force. Only an
/// activation's flush parks on a confined channel, and an activation implies a
/// document.
pub fn on_release_due(page: &mut SurfacePage, now_ms: u64) -> Released {
    let SurfacePage {
        connect,
        stores,
        schedules,
        router,
        ..
    } = page;
    let swept = router.release_due(stores, now_ms);
    if swept.is_empty() {
        return Released::default();
    }
    let bindings = connect
        .bindings()
        .expect("surface client: a parked message implies a document in force");
    let mut answer = Released::default();
    for ReleasedChannel {
        channel,
        released,
        overflow,
    } in swept
    {
        tracing::debug!(
            %channel,
            released = released.len(),
            "surface client: released parked messages into retention"
        );
        answer.released += released.len();
        answer
            .drops
            .merge(schedules.charge_overflow(bindings, &channel, overflow));
        answer.channels.push(channel);
    }
    answer
}

/// State the release deadline, if the soonest confined message moved.
///
/// Called after every input rather than at each site that could move one — see
/// the module's timer note. `None` means the armed deadline is still the right
/// one and the caller leaves its timer alone.
pub fn release_wakeup(page: &mut SurfacePage) -> Option<ReleaseTimer> {
    page.router.release_wakeup(&page.stores)
}

/// The retry timer fired: offer every blocked outbox's head once more.
///
/// One head per instance per fire — the head is the oldest un-applied flush and
/// nothing behind it may overtake it — and instances are independent, so a
/// starved one never blocks a sibling.
pub fn on_retry_tick(page: &mut SurfacePage, now: Millis) -> OutboxSteps<String> {
    page.outbound.on_retry_tick(now)
}

/// A confined control-plane publish the page composed, for the caller to route.
///
/// Not routed here: minting a confined envelope needs a stamp, which only the
/// layer that reads clocks and entropy can supply, and routing one can itself
/// evict a reader's position — which is another ladder charge, at the caller's
/// own pass rather than nested inside this one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPublish {
    pub channel: String,
    pub body: String,
}

/// What a set of loud-rung verdicts asks the page to say.
///
/// One alert and one toast per announcement, in announcement order. The
/// announcement is already coalesced — one per binding per window — so this adds
/// no further folding.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DropNotices {
    /// `Alert` frames for the operator, to send while the link is up. The grant is
    /// the server's to enforce; boot proves it granted for any `alarm`-or-louder
    /// binding, so a conforming page composes these unconditionally.
    pub alerts: Vec<ClientFrame>,
    /// Toasts on the page's own confined plane, which works whether or not the
    /// link does.
    pub toasts: Vec<ControlPublish>,
}

/// Compose what the loud rungs of one verdict set say.
///
/// Both halves carry [`crate::activation::DropAnnouncement::describe`]'s sentence,
/// so the operator reads the same words wherever the loss reaches them — the kill
/// reason included, which is the caller's to compose from the same method.
///
/// A `fatal` verdict adds nothing here. It is in `announce` too whenever it has a
/// delta to announce, and the kill it asks for carries its own account.
pub fn drop_notices(verdicts: &DropVerdicts) -> DropNotices {
    let mut notices = DropNotices::default();
    for announcement in &verdicts.announce {
        let text = announcement.describe();
        notices.alerts.push(ClientFrame::Alert {
            severity: AlertSeverity::Warning,
            title: truncate_report_field(
                format!("surface input overflow on {}", announcement.instance),
                MAX_ALERT_TITLE_BYTES,
            ),
            body: truncate_report_field(text.clone(), MAX_ALERT_BODY_BYTES),
        });
        notices.toasts.push(toast(text));
    }
    notices
}

/// Announce every whole flush an outbox lost, one toast each, in the order they
/// were dropped.
///
/// The sentence names no single cause because there are three, and the answer does
/// not distinguish them: a queue that reached its cap behind a head the link or the
/// peer would not take, a just-composed flush the wiring in force no longer admits,
/// and a queued one the attachment that came up re-validated away. Naming the
/// likeliest of them would send an operator's investigation at the link during a
/// rate-limit storm or a config change, which are the two the page is demonstrably
/// online for.
///
/// The toast plane and not an alert: half of these happen while the link is down,
/// and an alert composed against a dead link is a message nobody will read written
/// to a socket that is gone. The plane works either way, and the per-instance
/// counter carries the total for anyone who reconnects and asks.
pub fn parked_drop_notices(dropped: &[String]) -> Vec<ControlPublish> {
    dropped
        .iter()
        .map(|instance| {
            toast(format!(
                "{instance}: a queued publish batch was dropped — the outbox overflowed, or its \
                 wiring changed under it"
            ))
        })
        .collect()
}

/// One warning toast, as the page's own confined plane carries it.
fn toast(text: String) -> ControlPublish {
    ControlPublish {
        channel: LOCAL_TOAST_CHANNEL.to_string(),
        body: serde_json::to_string(&ToastBody {
            v: CONTROL_PLANE_VERSION,
            severity: ToastSeverity::Warning,
            text,
            source: ToastSource::Kernel,
        })
        .expect("surface client: a toast body serializes"),
    }
}

/// The completion context over the page's own tables.
///
/// # Panics
///
/// If no bindings document is in force — every completion resolves the noise of a
/// binding its own append could have overflowed.
fn flush_ctx(page: &mut SurfacePage, now: Millis, now_ms: u64) -> FlushCtx<'_, SurfacePlanes> {
    let SurfacePage {
        connect,
        stores,
        registrations,
        subs: _,
        outbound,
        schedules,
        router,
        views: _,
        body_cap: _,
    } = page;
    let bindings = connect
        .bindings()
        .expect("surface client: an activation's completion implies a document in force");
    FlushCtx {
        bindings,
        facts: connect.facts(),
        stores,
        router,
        outbound,
        registrations,
        schedules,
        now_ms,
        now,
    }
}

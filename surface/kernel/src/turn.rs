//! One input, one turn: what reaches the page from outside, and the table that
//! routes each of it.
//!
//! Everything below this module answers a question asked of one table. Something
//! has to decide *which* question one arriving thing asks — a routed frame is the
//! inbound pass, a matured schedule is the release pass, an entry that registered
//! is the registration table and the scheduler at once — and that is [`on_input`].
//! One [`Input`] in, one ordered [`Effect`] list out, with every pass's answer
//! folded through [`Reactions`] on the way.
//!
//! # What an input is
//!
//! [`Input`] is what happens *to* the page: the connection's own events, the
//! server frames it hands back, the two auxiliary deadlines, the activation
//! entries' lifecycle, and what the platform half asks for. It is `Eq` — no
//! variant carries a float, the viewport's device-pixel ratio included.
//!
//! # No clock, no socket, no envelope minted
//!
//! `now` and `now_ms` are read by the caller and handed in. That is the same
//! sans-I/O seam the layers below keep, applied to the layer that drives them, so
//! a whole turn is reproducible against fixed inputs.
//!
//! # The two passes that are not inputs
//!
//! [`dispatch`] is the exception, and it is one by necessity: assembling an
//! activation answers with the activation itself, and the caller — which holds the
//! entry closure the page deliberately does not — is the only party that can
//! invoke it. It is otherwise an ordinary turn, effects and release restatement
//! alike, and the completion comes back through [`Input::ActivationDone`].
//!
//! [`dispatch_sync`] is the same exception for the same reason, with the subject
//! named by the caller rather than picked by the rotation: a component's own stack
//! is blocked on the answer, so the request cannot be queued as an input and
//! answered later.
//!
//! # Nothing is enacted
//!
//! An effect is a request: send this frame, arm this deadline, emit this event,
//! take the attachment fatal. The layer that owns the driver performs them, which
//! is what keeps the page itself drivable from a test with no socket at all.

#[cfg(test)]
mod tests;

use brenn_attach_client::Millis;
use brenn_attach_client::conn::ConnEvent;
use brenn_attach_client::router::MessageStamp;
use brenn_attach_proto::ServerFrame;

use crate::activation::ReadyActivation;
use crate::command::{self, Command};
use crate::inbound;
use crate::outward::{self, Completed, SyncRefusal};
use crate::page::SurfacePage;
use crate::session::{Effect, Reactions};

/// Something that happened to the page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// The attachment's own lifecycle: it came up, it went away, or it ended
    /// terminally.
    Conn(ConnEvent),
    /// A server frame belonging to a plane above the connection. Never a `Hello`,
    /// `Welcome` or `Heartbeat` — see [`inbound::on_server_frame`].
    Frame(ServerFrame),
    /// Something the platform half asked the page to do.
    Command(Command),
    /// An instance registered its activation entry. The entry is a callback that
    /// does not cross this boundary, so this carries only the identity the page
    /// needs to give it positions, subscriptions, an outbox, and scheduler state.
    ActivationRegistered { instance: String },
    /// An instance's activation entry was withdrawn. Its positions, its
    /// subscription references and its outbox go with it; the channels' stores do
    /// not.
    ActivationDeregistered { instance: String },
    /// An invoked activation entry returned.
    ActivationDone(Completed),
    /// The outbox retry deadline fired: every blocked head is owed another offer.
    RetryDue,
    /// The confined release deadline fired: whatever is due enters retention now.
    ///
    /// Carries no instant of its own — the `now_ms` every input is fed with is the
    /// wall clock read at the fire, which is what a release is judged against. A
    /// fire that finds nothing due (a timer that fired early, a clock that stepped
    /// back) releases nothing and is not an error.
    ReleaseDue,
    /// A precondition the page cannot check for itself failed on the host side —
    /// today, a device clock reading before the Unix epoch. Terminal, through the
    /// ordinary fatal path.
    HostFatal { detail: String },
}

/// Take one input and answer the whole turn it produced.
///
/// `now` is the driver's monotonic reading, which every deadline in the page is
/// stated against; `now_ms` is its wall-clock reading in epoch milliseconds, the
/// currency a release time is named in. Both are read once per input by the
/// caller, so every pass a turn runs resolves against the same instant.
///
/// The confined-release deadline is restated at the end of every turn, whatever
/// the input was — a park, a sweep, a control op and a discarded store all move
/// it, and one restatement over the whole page cannot be forgotten at a new site
/// the way per-site arming can.
pub fn on_input(page: &mut SurfacePage, input: Input, now: Millis, now_ms: u64) -> Vec<Effect> {
    let mut reactions = Reactions::new();
    route(page, input, &mut reactions, now, now_ms);
    reactions.end_turn(page);
    reactions.into_effects()
}

/// Assemble the next ready instance's activation, and answer the turn that took.
///
/// The one pass a caller asks for rather than one an [`Input`] brings: an assembly
/// hands back the activation itself, which no `Effect` list can carry — the caller
/// owns the entry the page must not hold, and calling it is the whole point of
/// asking. Everything else about the pass is an ordinary turn: the window's
/// loudness verdicts are folded like any other pass's, and the release deadline is
/// restated at the end of it like any other turn's.
///
/// `None` means one of two things, and the caller treats them alike — it invokes
/// nothing:
///
/// - nothing was ready, or
/// - the assembly's own window overflowed past the rung that kills, and the
///   instance went terminal before its entry could run. The assembly happened and
///   its buffer exists; discarding it unread is what the kill's account of that
///   flush is. The kill and its announcement are in the effects.
///
/// A returned activation leaves its instance **in flight**, so the caller owes
/// exactly one [`Input::ActivationDone`] for it, carrying the generation the
/// assembly was made under.
///
/// # Panics
///
/// If an instance is ready with no bindings document in force. A position is
/// created only by the reconcile of a document.
pub fn dispatch(
    page: &mut SurfacePage,
    now: Millis,
    now_ms: u64,
) -> (Option<ReadyActivation>, Vec<Effect>) {
    let mut reactions = Reactions::new();
    let ready = outward::dispatch(page, now_ms).and_then(|mut ready| {
        // Taken off the assembly rather than read from it: the verdicts are the
        // page's to enact, and what the caller receives should carry nothing it is
        // still owed anything for.
        let drops = std::mem::take(&mut ready.drops);
        reactions.verdicts(page, drops, now, now_ms);
        (!page.registrations.is_failed(&ready.instance)).then_some(ready)
    });
    reactions.end_turn(page);
    (ready, reactions.into_effects())
}

/// What a sync-call request produced, for the caller blocked on the answer.
///
/// Boxed on the one arm that carries anything: the other two are a byte apiece,
/// and the answer is moved through the door's whole stack on every request.
#[derive(Debug, Clone, PartialEq)]
pub enum SyncDispatch {
    /// Assembled; the instance is **in flight**, so the caller invokes the entry
    /// and owes exactly one [`Input::ActivationDone`] for it.
    Ready(Box<ReadyActivation>),
    /// The assembly's own window overflowed past the rung that kills, and the
    /// instance went terminal before its entry could run. Nothing is in flight and
    /// no completion is owed; the kill and its announcement are in the effects.
    ///
    /// The caller is blocked on an answer, and the instance is dead without its
    /// entry having run: the same observable outcome as an entry that trapped, so
    /// the caller answers it the same way.
    Killed,
    /// The request was not admissible, which is always a bug — see [`SyncRefusal`].
    /// Nothing was assembled, nothing is in flight, and no completion is owed.
    Refused(SyncRefusal),
}

/// Assemble a sync-call activation for a **named** instance, and answer the turn
/// that took.
///
/// The second pass a caller asks for rather than one an [`Input`] brings, and for
/// a sharper version of [`dispatch`]'s reason: the caller is blocked on the
/// answer, so the request cannot be queued and answered later. An ordinary turn
/// in every other respect — verdicts folded, release deadline restated.
///
/// `port` names the sync port the request arrives on and `body` is its payload,
/// both the component's own; `instance` is the identity the caller resolved,
/// never one the component claimed. `stamp` mints the request envelope.
///
/// # Panics
///
/// If the instance is registered with no bindings document in force — the gates
/// answer that as a refusal rather than reaching an assembly.
pub fn dispatch_sync(
    page: &mut SurfacePage,
    instance: &str,
    port: &str,
    body: String,
    stamp: MessageStamp,
    now: Millis,
    now_ms: u64,
) -> (SyncDispatch, Vec<Effect>) {
    let mut reactions = Reactions::new();
    let answer = match outward::dispatch_sync(page, instance, port, body, stamp, now_ms) {
        Ok(mut ready) => {
            // The verdicts are the page's to enact, and one of them may be the
            // kill that means there is no entry to run.
            let drops = std::mem::take(&mut ready.drops);
            reactions.verdicts(page, drops, now, now_ms);
            if page.registrations.is_failed(&ready.instance) {
                SyncDispatch::Killed
            } else {
                SyncDispatch::Ready(Box::new(ready))
            }
        }
        Err(refusal) => {
            tracing::warn!("surface client: {}", refusal.describe(instance, port));
            SyncDispatch::Refused(refusal)
        }
    };
    reactions.end_turn(page);
    (answer, reactions.into_effects())
}

/// The input's own pass, ahead of the release restatement every turn ends with.
fn route(
    page: &mut SurfacePage,
    input: Input,
    reactions: &mut Reactions,
    now: Millis,
    now_ms: u64,
) {
    match input {
        Input::Conn(event) => reactions.conn_event(page, event),
        Input::Frame(frame) => match inbound::on_server_frame(page, frame, now) {
            Ok(inbound) => reactions.inbound(page, inbound, now, now_ms),
            // A peer contract the page cannot reconcile. It takes its whole
            // configuration from this peer on faith, so there is nothing to carry
            // on from.
            Err(detail) => reactions.go_fatal(detail),
        },
        Input::Command(command) => {
            let outcome = command::on_command(page, command);
            reactions.command(page, outcome, now, now_ms);
        }
        Input::ActivationRegistered { instance } => on_registered(page, &instance, reactions),
        Input::ActivationDeregistered { instance } => on_deregistered(page, &instance, reactions),
        Input::ActivationDone(done) => {
            let completion = outward::on_activation_done(page, done, now, now_ms);
            reactions.completion(page, completion, now, now_ms);
        }
        Input::RetryDue => {
            let steps = outward::on_retry_tick(page, now);
            reactions.steps(page, steps);
        }
        Input::ReleaseDue => {
            let released = outward::on_release_due(page, now_ms);
            reactions.released(page, released, now, now_ms);
        }
        Input::HostFatal { detail } => reactions.go_fatal(detail),
    }
}

/// An instance's activation entry is live: give it everything the wiring says it
/// holds.
///
/// Registration is admitted before the page's first document — a component can
/// mount while the wiring is still in flight — and the reconcile that document
/// runs is what wires it in. What is *not* deferred is the scheduler state, which
/// every later pass over this instance requires. The mount's guaranteed
/// activation follows the wiring: owed from here when a document is already in
/// force, otherwise from the one that lands.
///
/// # Panics
///
/// If the instance is already registered, or already scheduled. Both are the
/// fail-fast backstop behind the caller's own registration gate: a second entry
/// for one instance would silently orphan the first one's positions.
fn on_registered(page: &mut SurfacePage, instance: &str, reactions: &mut Reactions) {
    let frames = {
        let SurfacePage {
            connect,
            stores,
            registrations,
            subs,
            outbound,
            schedules,
            ..
        } = page;
        let wiring = connect.bindings();
        let frames = registrations.register(instance, wiring, stores, subs);
        schedules.track(instance, wiring.is_some());
        // An instance no document declares has no depth to open an outbox at, and
        // no channel it could publish on either.
        if let Some(bindings) = wiring
            && bindings.is_declared_instance(instance)
        {
            outbound.register(instance, bindings);
        }
        frames
    };
    reactions.frames(page, frames);
}

/// An instance's activation entry was withdrawn.
///
/// # Panics
///
/// If the instance is not registered, or holds no scheduler state.
fn on_deregistered(page: &mut SurfacePage, instance: &str, reactions: &mut Reactions) {
    let (frames, lost) = {
        let SurfacePage {
            stores,
            registrations,
            subs,
            outbound,
            schedules,
            ..
        } = page;
        let frames = registrations.deregister(instance, stores, subs);
        schedules.forget(instance);
        (frames, outbound.deregister(instance))
    };
    // The entries were ok'd by the activations that wrote them and never applied,
    // so the loss is real — and nobody is left to answer for it, which is why it
    // is a log line rather than an event.
    for batch in lost {
        tracing::warn!(
            instance,
            entries = batch.entries.len(),
            ops = batch.ops.len(),
            "surface client: a queued flush died with the outbox of an unmounted instance"
        );
    }
    reactions.frames(page, frames);
}

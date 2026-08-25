//! The synchronous side door onto a running page.
//!
//! [`crate::runner`] is the page's ordinary driver: it waits, it turns, it
//! enacts, and every activation it invokes happens on its own task, one macrotask
//! apart. That is the right shape for everything a message can cause — and the
//! wrong shape for the one thing a *gesture* causes, because a gesture handler's
//! caller is the browser, blocked inside `dispatchEvent`, holding a user-activation
//! token that expires the moment the stack unwinds. A request queued to the runner
//! cannot answer before that happens.
//!
//! So the door exists: a second caller of [`crate::turn`], on the requester's own
//! stack, running assembly → invoke → completion with nothing awaited in between.
//! [`turn::dispatch_sync`] was built for exactly this — a pass a caller *asks*
//! for, because the activation itself is the answer — and everything below it is
//! the same code the loop runs.
//!
//! # Why only a `dom` component has one
//!
//! The gesture is the whole reason: a user-activation token exists because a
//! browser event fired on an element. A headless instance has no element and no
//! gesture, so there is nothing for a synchronous pass to preserve. DOM-forced,
//! not an ABI difference the kernel chose to keep — every other privileged entry
//! is one router serving both.
//!
//! # What the door does not do
//!
//! It enacts nothing. Both of its turns' effects go back to the loop over a
//! channel and are performed there, in arrival order, interleaved with nothing.
//! That is what keeps frame order equal to page order with two callers driving one
//! page: the door mutates the page and queues the consequences; the loop remains
//! the only thing that writes a socket, arms a deadline or emits an event.
//!
//! # Re-entrancy is a borrow
//!
//! An entry is on the stack iff an activation is in flight, and an activation in
//! flight iff the page cell is borrowed — the run's activation pass holds its
//! borrow across the invocation for precisely this reason. So a request that
//! arrives from inside somebody's entry finds the cell borrowed and is refused,
//! and the check costs a `try_borrow_mut`. The page answers the same question
//! itself ([`crate::outward::SyncRefusal::ReEntrant`]); this is the layer that can
//! answer it without borrowing.

use std::cell::RefCell;

use futures_channel::mpsc;

use brenn_attach_client::driver::{flush_stamps, new_stamp};
use brenn_attach_client::transport::clock::{Clock, epoch_ms, wall_now};
use brenn_surface_contract::{ActivationError, SyncStatus};

use crate::activation::{ActivationOutcome, ReadyActivation};
use crate::front::InFlightSlot;
use crate::outward::{Completed, SyncRefusal};
use crate::page::SurfacePage;
use crate::runner::{SharedEntries, SharedPage, invoke_shared};
use crate::session::Effect;
use crate::turn::{self, Input, SyncDispatch};

/// How one sync-call request finished, in the kernel's own vocabulary.
///
/// The contract's [`SyncStatus`] is this minus everything a caller inside the
/// kernel still wants: the reply on ok, the component's account on err, and which
/// refusal it was for the breadcrumb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAnswer {
    /// The entry returned ok, with the reply it answered its caller with (or
    /// `None` if it answered without one). Its buffer flushed.
    Ok(Option<String>),
    /// The entry returned err, carrying its own account. The buffer was discarded
    /// and a failure counted; the instance keeps running.
    Err(ActivationError),
    /// The instance is terminal without having answered: its entry trapped, or the
    /// assembly's own loud-rung verdict killed it before the entry could run. Both
    /// are one fact for the requester — stop.
    Trap,
    /// Nothing was assembled and no entry ran. Always a bug; the refusal says
    /// whose.
    Refused(SyncRefusal),
}

impl SyncAnswer {
    /// The contract status this answer is written onto the request's detail as.
    pub fn status(&self) -> SyncStatus {
        match self {
            Self::Ok(_) => SyncStatus::Ok,
            Self::Err(_) => SyncStatus::Err,
            Self::Trap => SyncStatus::Trap,
            Self::Refused(_) => SyncStatus::Refused,
        }
    }
}

/// The seam a `brenn-activation-sync` request runs through.
///
/// Holds exactly what a whole activation needs and nothing else: the page to turn,
/// the entries to call, the in-flight slot a buffered publish routes through, and
/// the way back to the loop for the effects. Taken from the runner before it is
/// spawned ([`crate::runner::SurfaceRunner::sync_door`]) and held for the page's
/// life.
pub struct SyncDoor {
    page: SharedPage,
    entries: SharedEntries,
    in_flight: InFlightSlot,
    /// Behind a cell because a bounded sender needs `&mut` to offer and a request
    /// arrives on a shared reference. Uncontended: one browser thread, and the
    /// borrow spans one `try_send`.
    effects_tx: RefCell<mpsc::Sender<Vec<Effect>>>,
    /// The door's own monotonic reading, which the browser's clock makes free to
    /// take: it is `performance.now()` against the document's fixed origin, so a
    /// clock built here and the driver's clock answer the same numbers. Every
    /// deadline the page states is compared against both.
    clock: Clock,
}

impl SyncDoor {
    pub(crate) fn new(
        page: SharedPage,
        entries: SharedEntries,
        in_flight: InFlightSlot,
        effects_tx: mpsc::Sender<Vec<Effect>>,
    ) -> Self {
        Self {
            page,
            entries,
            in_flight,
            effects_tx: RefCell::new(effects_tx),
            clock: Clock::new(),
        }
    }

    /// Run one sync-call request to completion and answer it.
    ///
    /// `instance` is the identity the kernel resolved from the retargeted event
    /// target, never one the component claimed; `port` and `body` are the
    /// component's own. Nothing here awaits, so the whole activation — assembly,
    /// the entry, the flush fold — happens before the `dispatchEvent` that caused
    /// it returns.
    ///
    /// # Panics
    ///
    /// If the device clock reads before the Unix epoch, or if the run has ended
    /// and the effects this turn produced have nowhere to go. Both are states a
    /// conforming page never reaches, and the second is the one that matters: the
    /// page is already mutated by the time the effects are offered, so dropping
    /// them would leave a page whose state nothing on screen or on the wire
    /// reflects.
    pub fn request(&self, instance: &str, port: &str, body: String) -> SyncAnswer {
        // The borrow *is* the re-entrancy question: the run's activation pass holds
        // it across the entry call, so a request from inside an entry finds it
        // taken. Nothing else holds it across an await.
        let Ok(mut page) = self.page.try_borrow_mut() else {
            return SyncAnswer::Refused(SyncRefusal::ReEntrant);
        };
        // One reading for the whole stretch, as the loop's own activation pass
        // takes: assembly, the request envelope's `publish_ts` and the completion
        // are one commit and must agree about when now was.
        let now = self.clock.now();
        let now_ms = epoch_ms(wall_now());
        let (dispatch, mut effects) =
            turn::dispatch_sync(&mut page, instance, port, body, new_stamp(), now, now_ms);
        let answer = match dispatch {
            SyncDispatch::Refused(refusal) => SyncAnswer::Refused(refusal),
            // Terminal before its entry ran: nothing is in flight and no completion
            // is owed, so the kill's own effects are all that is left to hand back.
            SyncDispatch::Killed => SyncAnswer::Trap,
            SyncDispatch::Ready(ready) => self.run(&mut page, *ready, &mut effects, now, now_ms),
        };
        // Released before the effects are offered: `try_send` is not a turn, but
        // holding a page borrow past the work that needs it is how the next hazard
        // gets written.
        drop(page);
        // Every admitted request hands its effects over, empty list included,
        // because the send is also the loop's wake. The loop arms its activations
        // arm from a readiness snapshot taken before it parked, and a sync
        // activation can make some *other* instance ready — a flush onto a confined
        // channel that instance reads — while producing no effect at all: no frame,
        // no verdict, no moved release deadline. Skipping the send there parks the
        // loop on a stale answer until unrelated traffic arrives, which on a
        // detached page is never.
        //
        // A refusal turns nothing, so it owes no wake — and it is the one path a
        // non-conforming caller can drive at its own rate, which is why it is not
        // allowed to send one. What it is *not* allowed to do is drop an effect:
        // the wake is skipped on the answer, the hand-back on the list being empty,
        // so a refusal that ever did state something still states it.
        if !matches!(answer, SyncAnswer::Refused(_)) || !effects.is_empty() {
            self.enact(effects);
        }
        answer
    }

    /// Invoke one assembled sync activation and fold its completion, appending the
    /// completion turn's effects to what the assembly already asked for.
    fn run(
        &self,
        page: &mut SurfacePage,
        ready: ReadyActivation,
        effects: &mut Vec<Effect>,
        now: brenn_attach_client::Millis,
        now_ms: u64,
    ) -> SyncAnswer {
        let ReadyActivation {
            instance,
            generation,
            activation,
            buffer,
            drops: _,
        } = ready;
        let (outcome, buffer) = invoke_shared(
            &self.entries,
            &self.in_flight,
            &instance,
            &activation,
            buffer,
        );
        // Read before the outcome is moved into the completion: the requester's
        // answer is this fact, and the completion is what the page does about it.
        let answer = match &outcome {
            ActivationOutcome::Ok(reply) => SyncAnswer::Ok(reply.clone()),
            ActivationOutcome::Err(err) => SyncAnswer::Err(err.clone()),
            ActivationOutcome::Trap(_) => SyncAnswer::Trap,
        };
        // One stamp per buffered publish, minted here for the reason the loop mints
        // its own: this is an edge that reads clocks and entropy, and the page
        // reads neither.
        let stamps = flush_stamps(buffer.len());
        effects.extend(turn::on_input(
            page,
            Input::ActivationDone(Completed {
                instance,
                generation,
                outcome,
                buffer,
                stamps,
            }),
            now,
            now_ms,
        ));
        answer
    }

    /// Hand a turn's effects to the loop, which is the only thing that performs
    /// any of them — and, by the same send, tell it to look again at what is ready.
    ///
    /// An empty list is sent like any other: serving it enacts nothing, and the
    /// wake is the point.
    fn enact(&self, effects: Vec<Effect>) {
        let mut sender = self.effects_tx.borrow_mut();
        match sender.try_send(effects) {
            Ok(()) => {}
            Err(err) if err.is_full() => panic!(
                "surface kernel: the sync door's effects channel is full (the run stopped enacting)"
            ),
            Err(_) => panic!("surface kernel: the run is over (sync door effects channel closed)"),
        }
    }
}

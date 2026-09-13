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
//! # Its second caller: a component calling a peer
//!
//! [`SyncDoor::call`] is the same pass for the `calls.call` import. A gesture is
//! DOM-forced — a user-activation token exists because a browser event fired on
//! an element, so a headless instance has no gesture and nothing to preserve —
//! but a *call* is not: any instance the document wires may raise one, and it
//! needs the synchronous pass for the other reason, that the reply is the value
//! the guest is blocked on. The two differ in who asks and in what the request is
//! judged against (a chain), and in nothing below that.
//!
//! # What the door does not do
//!
//! It enacts nothing. A gesture's two turns' effects go back to the loop over a
//! channel and are performed there, in arrival order, interleaved with nothing;
//! a call's go onto the caller's in-flight frame and are drained by the driver
//! that is already running. That is what keeps frame order equal to page order
//! with two callers driving one page: the door mutates the page and queues the
//! consequences; the loop remains the only thing that writes a socket, arms a
//! deadline or emits an event.
//!
//! # Re-entrancy is the page's own fact
//!
//! An entry is on the stack iff an activation is in flight, and the page holds
//! that fact in its scheduler. A gesture's request carries an **empty chain**, so
//! [`crate::outward::dispatch_sync`] judges it by the page-wide rule: anything in
//! flight means it came from inside somebody's entry — programmatically, since one
//! JS thread means a genuine gesture never can — and it is refused.
//!
//! A component calling a peer turns the page from inside its own entry, so no
//! driver holds a borrow across an invocation. One fact, checked in one place.

use std::cell::RefCell;

use futures_channel::mpsc;

use brenn_attach_client::driver::{flush_stamps, new_stamp};
use brenn_attach_client::transport::clock::{Clock, epoch_ms, wall_now};

use crate::activation::ReadyActivation;
use crate::front::InFlightSlot;
use crate::outward::{Completed, SyncCall};
use crate::runner::{SharedEntries, SharedPage, invoke_shared};
use crate::session::Effect;
use crate::turn;

/// How one sync-call request finished. The facility's vocabulary, shared with
/// the backend host: the reply on ok, the component's own account on err, and
/// which refusal it was for the breadcrumb.
pub use brenn_activation::sync::SyncAnswer;

/// The seam a sync-call request runs through — a gesture's
/// ([`request`](Self::request)) or a component's ([`call`](Self::call)).
///
/// Holds exactly what a whole activation needs and nothing else: the page to turn,
/// the entries to call, the in-flight stack a buffered publish routes through and
/// a nested call's effects ride on, and the way back to the loop for a gesture's
/// effects. Taken from the runner before it is
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
        // One reading for the whole stretch, as the loop's own activation pass
        // takes: assembly, the request envelope's `publish_ts` and the completion
        // are one commit and must agree about when now was.
        let now = self.clock.now();
        let now_ms = epoch_ms(wall_now());
        // Empty chain: a gesture is nobody's callee, so the page-wide re-entrancy
        // rule is the one that judges it.
        let (dispatch, mut effects) = turn::dispatch_sync(
            &mut self.page.borrow_mut(),
            SyncCall {
                instance,
                port,
                body: &body,
                chain: &[],
            },
            new_stamp(),
            now,
            now_ms,
        );
        // A refusal and a kill are answers already — the kill's own effects are
        // all that is left to hand back, since nothing is in flight and no
        // completion is owed. Both mappings are the facility's, not this door's.
        let answer = match dispatch.ready_or_answer() {
            Err(answer) => answer,
            Ok(ready) => self.run(ready, &mut effects, now, now_ms),
        };
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
    ///
    /// Borrows the page for the completion and never across the entry call: an
    /// entry may call a peer, and that call turns this page from inside this
    /// stack.
    fn run(
        &self,
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
        let (outcome, buffer, called) = invoke_shared(
            &self.entries,
            &self.in_flight,
            &instance,
            &activation,
            buffer,
        );
        // What this activation's own callees asked for, ahead of what it asks for
        // itself: they turned the page first.
        effects.extend(called);
        // One stamp per buffered publish, minted here for the reason the loop mints
        // its own: this is an edge that reads clocks and entropy, and the page
        // reads neither.
        let stamps = flush_stamps(buffer.len());
        // The answer is read off the completion and never off the outcome handed
        // in — see `turn::answer_for`.
        let (ruled, done_effects) = turn::complete(
            &mut self.page.borrow_mut(),
            Completed {
                instance,
                generation,
                outcome,
                buffer,
                stamps,
            },
            now,
            now_ms,
        );
        effects.extend(done_effects);
        turn::answer_for(ruled)
    }

    /// Run one component's `calls.call` to its peer and answer it.
    ///
    /// The door's second caller, and the reason the door is not only the
    /// gesture's: a call is the same request the gesture raises — assembly,
    /// entry, completion, all on the requester's own stack — differing only in
    /// who asks and what the request is judged against. A gesture's caller is the
    /// browser and its chain is empty; a call's caller is an activation of this
    /// page, so its chain is the in-flight stack and the target is looked up in
    /// the document rather than named by the requester.
    ///
    /// `None` is a declared `call` port the deployer left unbound — nobody to
    /// ask, which the seam answers `unwired`. The port's *declaredness* is not
    /// judged here: that is the caller's own specification and the seam traps on
    /// it before asking the door anything.
    ///
    /// Nothing is enacted and nothing is sent. Both turns' effects are stashed on
    /// the caller's in-flight frame, for the reason
    /// [`crate::front::InFlightPublish::effects`] gives.
    ///
    /// TODO(surface-wasm-test-in-ci): this body is browser-only and is driven by
    /// no runner. What it composes is pinned natively — the gate in
    /// [`crate::turn`], the stack in [`crate::front::InFlightStack`], the answer
    /// in [`crate::calls::call_answer`] — but the composition is not.
    ///
    /// # Panics
    ///
    /// If no activation is on the stack. A call reaching the door outside one is
    /// refused at the seam (`not-permitted`), so arriving here means the seam's
    /// admission was bypassed.
    pub fn call(&self, caller: &str, port: &str, payload: String) -> Option<SyncAnswer> {
        let (target_instance, target_port) = {
            let page = self.page.borrow();
            let target = page.connect.bindings()?.call_target(caller, port)?;
            (target.target_instance.clone(), target.target_port.clone())
        };
        // The stack is the chain: every instance whose entry is on it is above
        // this request, outermost first, and the caller is its top. Snapshotted
        // before the dispatch so the borrow does not span a turn.
        let chain: Vec<String> = self.in_flight.borrow().chain();
        let now = self.clock.now();
        let now_ms = epoch_ms(wall_now());
        let (dispatch, mut effects) = turn::dispatch_sync(
            &mut self.page.borrow_mut(),
            SyncCall {
                instance: &target_instance,
                port: &target_port,
                body: &payload,
                chain: &chain,
            },
            new_stamp(),
            now,
            now_ms,
        );
        let answer = match dispatch.ready_or_answer() {
            Err(answer) => answer,
            Ok(ready) => self.run(ready, &mut effects, now, now_ms),
        };
        self.stash(effects);
        Some(answer)
    }

    /// Park a nested turn's effects on the caller's in-flight frame.
    fn stash(&self, effects: Vec<Effect>) {
        self.in_flight.borrow_mut().stash(effects);
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

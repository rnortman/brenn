//! The loop that runs a page against a peer: the one layer of the surface kernel
//! that owns a socket, reads clocks, and mints envelope identities.
//!
//! [`turn::on_input`] is a pure function of a [`SurfacePage`] — one input in, one
//! ordered [`Effect`] list out — and [`AttachDriver`] is its mirror image, pure
//! I/O that owns the connector, the live transport and the armed deadlines and
//! knows nothing above the connection itself. Neither can do the other's half of
//! a turn. The runner is what joins them: it waits on whichever of them can speak
//! next, turns what it hears into one [`Input`], and performs what the turn
//! answers with.
//!
//! # The three non-deterministic readings
//!
//! A turn is fed a monotonic instant and a wall-clock one, and a confined publish
//! is fed an envelope identity. All three are read here, at the edge, and handed
//! to the layers below as data — which is what keeps every one of them drivable
//! from a test with no socket and no clock at all.
//!
//! # The bias
//!
//! The wait is biased ahead of the platform half's commands. It bundles the
//! socket (biased first within it) with the connection's liveness deadline, the
//! outbox retry and the confined release, and each of those fires once and is
//! answered once — so a queued command waits at most one turn behind one of them.
//! The starvation a bias order guards against is a *stream*, and no deadline
//! produces one.
//!
//! One state is outside that bundle: a connect attempt in flight. The attempt
//! owns the driver for its whole duration — it is the one thing the driver cannot
//! hand back half-done — so the outbox retry and the confined release cannot fire
//! until it settles, up to a connect timeout when the attempt hangs rather than
//! refuses. The accepted trade: both deadlines release whatever is due *at the
//! fire* rather than what was due when they were armed, so a stalled attempt costs
//! a confined release its punctuality and never its contents.
//!
//! # Terminal is not the end of the loop
//!
//! When the attachment ends for good the run does not return: the platform half
//! folds the death one event-loop hop later and states its own terminal
//! link-state on a confined plane, which is still mounted for chrome to draw the
//! banner from. So the loop hands off to a drain that keeps routing exactly that
//! and absorbs the rest, and returns only once the platform half is gone.

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use futures_channel::mpsc;
use futures_util::{FutureExt, StreamExt, future, pin_mut, select_biased};

use brenn_attach_client::TransportConnector;
use brenn_attach_client::conn::ConnConfig;
use brenn_attach_client::driver::{AttachDriver, DriverStep, IoEvent};
use brenn_attach_client::transport::clock::{checked_epoch_ms, epoch_ms, wall_now};

use crate::command::Command;
use crate::handle::ActivationEntry;
use crate::page::SurfacePage;
use crate::session::{Effect, Event};
use crate::turn::{self, Input};

/// What the platform half asks of a running page over the control channel.
///
/// The lifecycle plane: mounts, unmounts, the kernel's own control-plane
/// statements, and the shutdown. Each is low-rate and kernel-produced, which is
/// why they share one channel and take a fail-fast bound on it.
pub enum RunnerCommand {
    /// Register `instance`'s activation entry. The entry is a callback and
    /// lives in the runner, not the page.
    RegisterActivation {
        instance: String,
        entry: ActivationEntry,
    },
    /// Withdraw `instance`'s activation entry.
    DeregisterActivation { instance: String },
    /// State one of the kernel's own confined control planes — link-state,
    /// surface-state, theme, toast. The stamp is minted here; the page mints no
    /// identity of its own.
    PublishControl { channel: String, body: String },
    /// Orderly shutdown: the attachment closes, every caller awaiting a publish
    /// is answered, and nothing reconnects.
    Close,
}

/// A page, the attachment under it, and the loop that drives both.
///
/// Built with [`SurfaceRunner::new`] and run to completion with
/// [`SurfaceRunner::run`]; the caller spawns it (`tokio::spawn` natively,
/// `spawn_local` on wasm).
pub struct SurfaceRunner<C: TransportConnector> {
    driver: AttachDriver<C>,
    page: SurfacePage,
    /// Registered activation entries, keyed by instance.
    ///
    /// Runner-side rather than page-side because an entry is a callback and the
    /// page is pure data: a `Box<dyn Fn>` in there would make the page's inputs
    /// unclonable, undebuggable and uncomparable. The page holds the *identity*
    /// of a registered instance and every scheduling decision about it; this map
    /// holds the one thing it cannot.
    // TODO(runner-activation-dispatch): nothing invokes these yet — the pass that
    // assembles a ready instance's activation, calls its entry and hands the
    // completion back is not wired into the loop.
    entries: HashMap<String, ActivationEntry>,
    /// The platform half's event sink. Bounded; an overflow is a
    /// platform-half-not-draining bug (this traffic is low-rate by construction)
    /// and panics.
    events_tx: mpsc::Sender<Event>,
    control_rx: mpsc::Receiver<RunnerCommand>,
    /// Set once the control channel closes — the platform half dropped every
    /// sender. The loop stops selecting it and carries on: the run's life is tied
    /// to the attachment and the event sink, not to who can command it.
    control_closed: bool,
    /// Whether the device clock can timestamp at all. False only after the boot
    /// check refused it, which is terminal — the turns that follow the diagnosis
    /// resolve nothing against a clock, and reading a broken one would panic
    /// instead of reporting.
    clock_usable: bool,
}

impl<C: TransportConnector> SurfaceRunner<C> {
    /// Build the runner over a page and a connection config. Nothing has happened
    /// yet: the first connect attempt is [`SurfaceRunner::run`]'s first act.
    ///
    /// The page carries the boot identity the attachment is about to be measured
    /// against — the channel its wiring is retained on, and the epoch its own
    /// stores are stamped with — because both are the caller's to resolve.
    pub fn new(
        page: SurfacePage,
        config: ConnConfig,
        connector: C,
        events_tx: mpsc::Sender<Event>,
        control_rx: mpsc::Receiver<RunnerCommand>,
    ) -> Self {
        Self {
            driver: AttachDriver::new(config, connector),
            page,
            entries: HashMap::new(),
            events_tx,
            control_rx,
            control_closed: false,
            clock_usable: true,
        }
    }

    /// Run to completion: connect, serve turns until the attachment is terminal,
    /// then drain (see the module doc). Returns only when the platform half is
    /// gone.
    ///
    /// Hands the page back. The page outlives the attachment — its rings, its
    /// retained control planes and its registrations are all still there — so the
    /// run gives it up rather than dropping it on the floor.
    pub async fn run(self) -> SurfacePage {
        let wall = wall_now();
        self.run_from(wall).await
    }

    /// [`run`](Self::run) from a wall-clock reading the caller already took, which
    /// is the reading the boot clock check is made against.
    ///
    /// Split out because that check is the one branch with no other way in: a
    /// device clock reading before the Unix epoch is not a state a test can put a
    /// machine into.
    pub(crate) async fn run_from(mut self, wall: DateTime<Utc>) -> SurfacePage {
        // The device clock is checked once, here, before anything depends on a
        // reading of it: every envelope this page mints, every activation's `now`
        // and every schedule comparison is read from it, so one that reads before
        // the Unix epoch cannot be worked around, only refused. Refusing it takes
        // the ordinary fatal path, which names the cause, where carrying on would
        // trade it for an undiagnosed panic at the first publish. The connect is
        // never attempted: a page that cannot timestamp has no business attaching.
        match checked_epoch_ms(wall) {
            Ok(_) => {
                let step = self.driver.start().await;
                self.absorb(step).await;
            }
            Err(detail) => {
                self.clock_usable = false;
                self.turn(Input::HostFatal { detail }).await;
            }
        }
        while !self.driver.is_terminal() && !self.host_gone() {
            match self.driver.take_pending_connect() {
                Some(url) => self.run_connect(url).await,
                None => self.run_select().await,
            }
        }
        if !self.host_gone() {
            self.run_terminal_drain().await;
        }
        self.page
    }

    /// Whether the platform half has left. One predicate, read live off the sink
    /// rather than cached at the emit that first noticed: a receiver dropped
    /// between two emits is gone from that moment, and a stale copy of the answer
    /// is one more thing a later arm could consult and get wrong.
    fn host_gone(&self) -> bool {
        self.events_tx.is_closed()
    }

    /// Run one connect attempt, buffering control commands meanwhile.
    ///
    /// The attempt is the one thing the driver cannot hand back half-done —
    /// dropping its future abandons it — so it is pinned across the whole race
    /// and nothing here cancels it. Commands that arrive in the interim cannot be
    /// fed to a page whose driver is mid-attempt, so they are buffered and
    /// applied in order once it settles; buffering is what keeps a slow trickle
    /// of mounts during a stalled handshake from filling the control channel to
    /// its panic bound.
    async fn run_connect(&mut self, url: String) {
        let mut buffered: Vec<RunnerCommand> = Vec::new();
        let mut control_open = !self.control_closed;
        let settled = {
            let Self {
                driver, control_rx, ..
            } = &mut *self;
            let connect = driver.connect(&url).fuse();
            pin_mut!(connect);
            loop {
                // Copied into the control future so the arm below may clear the
                // flag without clashing with that future's borrow.
                let open = control_open;
                let settled = {
                    let control = async {
                        if open {
                            control_rx.next().await
                        } else {
                            future::pending::<Option<RunnerCommand>>().await
                        }
                    }
                    .fuse();
                    pin_mut!(control);
                    select_biased! {
                        input = connect => Some(input),
                        command = control => {
                            match command {
                                Some(command) => buffered.push(command),
                                // The platform half dropped: stop selecting a
                                // closed channel so the loop does not spin on a
                                // ready `None`.
                                None => control_open = false,
                            }
                            None
                        }
                    }
                };
                if let Some(settled) = settled {
                    break settled;
                }
            }
        };
        if !control_open {
            self.control_closed = true;
        }
        let step = self.driver.on_input(settled).await;
        self.absorb(step).await;
        for command in buffered {
            self.control(command).await;
        }
    }

    /// Wait for the next thing that concerns the page and serve it.
    async fn run_select(&mut self) {
        let woke = {
            let Self {
                driver,
                control_rx,
                control_closed,
                ..
            } = &mut *self;
            let io = driver.wait().fuse();
            let control = async {
                if *control_closed {
                    future::pending::<Option<RunnerCommand>>().await
                } else {
                    control_rx.next().await
                }
            }
            .fuse();
            pin_mut!(io, control);
            select_biased! {
                event = io => Woke::Io(event),
                command = control => Woke::Control(command),
            }
        };
        match woke {
            Woke::Io(IoEvent::Conn(input)) => {
                let step = self.driver.on_input(input).await;
                self.absorb(step).await;
            }
            Woke::Io(IoEvent::RetryDue) => self.turn(Input::RetryDue).await,
            // The wall clock read at the fire, not the deadline that armed it: a
            // timer fires late (a throttled background tab) or early (a clock
            // step), and what releases is what is due now.
            Woke::Io(IoEvent::ReleaseDue { now_ms }) => {
                self.turn_at(Input::ReleaseDue, now_ms).await;
            }
            Woke::Control(Some(command)) => self.control(command).await,
            Woke::Control(None) => self.control_closed = true,
        }
    }

    /// Serve the control channel after the attachment has ended.
    ///
    /// Only the kernel's own confined planes are still worth stating — the page's
    /// rings outlive the attachment and their readers are still mounted — so a
    /// `PublishControl` routes and everything else is absorbed: there is no wire
    /// for a mount to subscribe on, and nothing left to close.
    // TODO(runner-drain-host-departure): terminal disarms every deadline and drops
    // the transport, so the wait below has no wake source but the control channel
    // — a platform half that drops the event receiver while holding an idle
    // control sender parks this drain, and the page and the task leak with it.
    // Everywhere else the run re-reads `host_gone` on a bounded cadence.
    async fn run_terminal_drain(&mut self) {
        loop {
            // Nothing consumes what a routed publish would produce, and no
            // command can arrive: either way the run is over.
            if self.host_gone() || self.control_closed {
                return;
            }
            let command = {
                let Self { control_rx, .. } = &mut *self;
                control_rx.next().await
            };
            match command {
                None => self.control_closed = true,
                Some(RunnerCommand::PublishControl { channel, body }) => {
                    self.publish_control(channel, body).await;
                }
                // Named rather than silently swallowed: during an incident a
                // mount that did nothing reads as a call that never happened
                // unless the log says it arrived late.
                Some(RunnerCommand::RegisterActivation { instance, .. }) => {
                    tracing::debug!(
                        %instance,
                        "surface runner: absorbed a mount after the attachment ended"
                    );
                }
                Some(RunnerCommand::DeregisterActivation { instance }) => {
                    tracing::debug!(
                        %instance,
                        "surface runner: absorbed an unmount after the attachment ended"
                    );
                }
                Some(RunnerCommand::Close) => {
                    tracing::debug!("surface runner: absorbed a close after the attachment ended");
                }
            }
        }
    }

    /// Serve one control command: resolve what the page cannot hold — a callback,
    /// a minted identity — and feed it the turn.
    async fn control(&mut self, command: RunnerCommand) {
        match command {
            RunnerCommand::RegisterActivation { instance, entry } => {
                // Stored before the page is told, so an activation dispatched on
                // the strength of this registration always finds its entry. The
                // page panics on a double registration, so a silent overwrite
                // here is unreachable.
                self.entries.insert(instance.clone(), entry);
                self.turn(Input::ActivationRegistered { instance }).await;
            }
            RunnerCommand::DeregisterActivation { instance } => {
                self.entries.remove(&instance);
                self.turn(Input::ActivationDeregistered { instance }).await;
            }
            RunnerCommand::PublishControl { channel, body } => {
                self.publish_control(channel, body).await;
            }
            RunnerCommand::Close => self.turn(Input::Command(Command::Close)).await,
        }
    }

    /// State one of the kernel's own confined planes, under a freshly minted
    /// envelope identity.
    async fn publish_control(&mut self, channel: String, body: String) {
        let stamp = self.driver.new_stamp();
        self.turn(Input::Command(Command::PublishControl {
            channel,
            body,
            stamp,
        }))
        .await;
    }

    /// Run one turn at the wall clock read now.
    async fn turn(&mut self, input: Input) {
        let now_ms = self.now_ms();
        self.turn_at(input, now_ms).await;
    }

    /// Run one turn at a wall-clock instant the caller already read.
    async fn turn_at(&mut self, input: Input, now_ms: u64) {
        let effects = self.compute(input, now_ms);
        self.execute(effects).await;
    }

    /// The pure half of a turn. Separate from [`SurfaceRunner::execute`] because
    /// executing an effect can itself produce a turn — a write that fails detaches
    /// the page — and the two must compose into one queue rather than nest.
    fn compute(&mut self, input: Input, now_ms: u64) -> Vec<Effect> {
        turn::on_input(&mut self.page, input, self.driver.now(), now_ms)
    }

    /// Feed the page everything one driver step produced, and perform what it
    /// answers.
    async fn absorb(&mut self, step: DriverStep) {
        let mut queue = VecDeque::new();
        self.fold_step(step, &mut queue);
        self.execute(Vec::from(queue)).await;
    }

    /// Turn a driver step's events and routed frame into turns, appending what
    /// they ask for to `queue`.
    fn fold_step(&mut self, step: DriverStep, queue: &mut VecDeque<Effect>) {
        let DriverStep { events, routed } = step;
        for event in events {
            let now_ms = self.now_ms();
            queue.extend(self.compute(Input::Conn(event), now_ms));
        }
        if let Some(frame) = routed {
            let now_ms = self.now_ms();
            queue.extend(self.compute(Input::Frame(frame), now_ms));
        }
    }

    /// Perform a turn's effects in order, draining to quiescence: an effect that
    /// feeds the page back — a lost transport, a control publish it composed —
    /// appends what that turn asks for to the same queue.
    async fn execute(&mut self, effects: Vec<Effect>) {
        let mut queue: VecDeque<Effect> = effects.into();
        // Set once this batch has no wire left. Every later frame in it was
        // composed against the attachment that just ended, so it is dropped
        // rather than written to a corpse.
        let mut wire_lost = false;
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::SendFrame(frame) => {
                    if wire_lost {
                        tracing::debug!(
                            "surface runner: dropping a frame composed against the lost attachment"
                        );
                        continue;
                    }
                    // The whole run of consecutive frames goes in one call: the
                    // driver writes them in order and drops the rest of the run
                    // itself once a write loses the transport, which is the same
                    // rule `wire_lost` applies to what follows in this queue. A
                    // reconnect's resubscription and a document's application
                    // both produce such runs, one frame per channel.
                    let mut frames = vec![frame];
                    while let Some(Effect::SendFrame(_)) = queue.front() {
                        let Some(Effect::SendFrame(next)) = queue.pop_front() else {
                            unreachable!("surface runner: the queue front was just read as a frame")
                        };
                        frames.push(next);
                    }
                    let step = self.driver.send(frames).await;
                    wire_lost = !self.driver.is_active();
                    self.fold_step(step, &mut queue);
                }
                Effect::EmitEvent(event) => self.emit(event),
                Effect::PublishControl { channel, body } => {
                    let stamp = self.driver.new_stamp();
                    let now_ms = self.now_ms();
                    queue.extend(self.compute(
                        Input::Command(Command::PublishControl {
                            channel,
                            body,
                            stamp,
                        }),
                        now_ms,
                    ));
                }
                Effect::SetRetryWakeup(change) => self.driver.set_retry_wakeup(Some(change)),
                Effect::SetReleaseWakeup(change) => self.driver.set_release_wakeup(Some(change)),
                Effect::GoFatal { detail } => {
                    let step = self.driver.host_fatal(detail).await;
                    wire_lost = true;
                    self.fold_step(step, &mut queue);
                }
                Effect::Close => {
                    let step = self.driver.close().await;
                    wire_lost = true;
                    self.fold_step(step, &mut queue);
                }
            }
        }
    }

    /// Hand one event to the platform half.
    ///
    /// A full sink is a platform-half-not-draining bug and panics; a dropped
    /// receiver means it has left, and the run winds down — which
    /// [`SurfaceRunner::host_gone`] reads off the sink itself, so nothing is
    /// recorded here.
    fn emit(&mut self, event: Event) {
        match self.events_tx.try_send(event) {
            Ok(()) => {}
            Err(err) if err.is_full() => {
                panic!("surface runner: event sink overflow (the platform half is not draining)")
            }
            Err(_) => {}
        }
    }

    /// The wall clock in epoch milliseconds, the currency a release time is
    /// stated in.
    ///
    /// Answers zero on a device clock the boot check refused. Nothing resolves
    /// against it after that point — the only turns left are the fatal's own —
    /// and reading the refused clock would panic where the page is already on its
    /// way to terminal for exactly that reason.
    fn now_ms(&self) -> u64 {
        if self.clock_usable {
            epoch_ms(wall_now())
        } else {
            0
        }
    }
}

/// Which arm of the live select fired.
enum Woke {
    Io(IoEvent),
    Control(Option<RunnerCommand>),
}

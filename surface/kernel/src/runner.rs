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
//! The sync door's effects come first among the queued things, ahead even of
//! control: they are the tail of a turn the page has *already* taken, so the page
//! state they were composed against is older than anything still queued. Enacting
//! them first is what makes frame order equal page order — which is why the wait
//! winning on the socket does not simply proceed: the driver's event is about to
//! become a *newer* turn, so whatever the door queued is drained and enacted
//! first. Out of order it is not only frames that invert. A turn states the
//! confined-release deadline only when it changed, so enacting an older statement
//! last leaves the timer armed at an instant the page does not think it stated,
//! and no later turn restates it.
//!
//! Within the front door's own four channels the order is then the one their
//! contracts imply: control first, because a mount is what every later delivery
//! depends on and its channel is the one with a panic bound under it; then the
//! publishes, which are answered; then the two best-effort planes, which are
//! dropped when there is nowhere to put them. Neither a publish backlog nor an
//! alert flood can starve an unmount that way.
//!
//! Activations are dead last, and that is the whole point. A component that
//! republishes onto a confined channel it reads is ready again the instant its own
//! flush commits, so its arm is permanently ready — anything above it must win
//! every turn or that one component starves the page. Below the deadlines too: a
//! wake the page armed is a promise to something, and a spinning component is a
//! promise to nobody.
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
//! — and keeps activating the readers it wakes, or the banner would be routed to
//! a page that never draws it — absorbs the rest, and returns only once the
//! platform half is gone.

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use std::task::Poll;

use chrono::{DateTime, Utc};
use futures_channel::mpsc;
use futures_util::{FutureExt, StreamExt, future, pin_mut, select_biased};

use brenn_attach_client::Millis;
use brenn_attach_client::TransportConnector;
use brenn_attach_client::conn::ConnConfig;
use brenn_attach_client::driver::{AttachDriver, DriverStep, IoEvent};
use brenn_attach_client::transport::clock::{checked_epoch_ms, epoch_ms, wall_now};
use brenn_surface_contract::Activation;

use crate::activation::{ActivationOutcome, ReadyActivation};
use crate::command::Command;
use crate::front::{
    ActivationEntry, AlertCommand, EFFECTS_CHANNEL_CAPACITY, FrontChannels, PublishCommand,
    PublishSlot, ReportCommand, SurfaceGate, TelemetryCommand,
};
use crate::outbound::PortPublish;
use crate::outward::{self, Completed};
use crate::page::SurfacePage;
use crate::publish_buffer::PublishBuffer;
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

/// The page, at the ownership each target's seams require.
///
/// The loop is not the only thing that drives a page on wasm: a component's
/// `brenn-activation-sync` request runs a whole turn on the browser's own stack,
/// synchronously, because its caller is blocked on the answer. So the browser
/// build shares the cell with [`crate::sync_door::SyncDoor`]; natively there is
/// no such seam and the run owns it outright (an `Rc` would not be `Send`, which
/// the multi-threaded executor requires).
///
/// The cell is a `RefCell` on both targets, so every access spells the same. The
/// borrow flag is load-bearing on wasm: an activation pass holds it across the
/// entry call, which is exactly the fact the door reads to answer a re-entrant
/// request.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type SharedPage = RefCell<SurfacePage>;

/// The browser's page cell. See the native definition above.
#[cfg(target_arch = "wasm32")]
pub(crate) type SharedPage = std::rc::Rc<RefCell<SurfacePage>>;

/// The registered entries, at the ownership each target's seams require — the
/// twin of [`SharedPage`], shared with the sync door for the same reason.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type SharedEntries = RefCell<HashMap<String, ActivationEntry>>;

/// The browser's entry table. See the native definition above.
#[cfg(target_arch = "wasm32")]
pub(crate) type SharedEntries = std::rc::Rc<RefCell<HashMap<String, ActivationEntry>>>;

/// A page, the attachment under it, and the loop that drives both.
///
/// Built with [`SurfaceRunner::new`] and run to completion with
/// [`SurfaceRunner::run`]; the caller spawns it (`tokio::spawn` natively,
/// `spawn_local` on wasm).
pub struct SurfaceRunner<C: TransportConnector> {
    driver: AttachDriver<C>,
    page: SharedPage,
    /// Registered activation entries, keyed by instance.
    ///
    /// Runner-side rather than page-side because an entry is a callback and the
    /// page is pure data: a `Box<dyn Fn>` in there would make the page's inputs
    /// unclonable, undebuggable and uncomparable. The page holds the *identity*
    /// of a registered instance and every scheduling decision about it; this map
    /// holds the one thing it cannot.
    entries: SharedEntries,
    /// The in-flight activation's buffer, shared with the platform half's front
    /// door. Filled for exactly the duration of an entry invocation; a `dom`
    /// component's publish reaches it from a DOM listener, which is a code path
    /// with no way to the buffer on this stack.
    #[cfg(target_arch = "wasm32")]
    in_flight: crate::front::InFlightSlot,
    /// The platform half's event sink. Bounded; an overflow is a
    /// platform-half-not-draining bug (this traffic is low-rate by construction)
    /// and panics.
    events_tx: mpsc::Sender<Event>,
    control_rx: mpsc::Receiver<RunnerCommand>,
    /// Set once the control channel closes — the platform half dropped every
    /// sender. The loop stops selecting it and carries on: the run's life is tied
    /// to the attachment and the event sink, not to who can command it.
    control_closed: bool,
    /// The components' publishes and the log path's reports, which share one
    /// channel because a report is an ordinary publish on an ordinary channel.
    publish_rx: mpsc::Receiver<PublishSlot>,
    publish_closed: bool,
    alert_rx: mpsc::Receiver<AlertCommand>,
    alert_closed: bool,
    telemetry_rx: mpsc::Receiver<TelemetryCommand>,
    telemetry_closed: bool,
    /// The effects of turns the sync door already ran on the page, waiting to be
    /// enacted. Every effect this run performs goes through the loop below,
    /// whichever turn produced it, which is what keeps frame order equal to page
    /// order.
    effects_rx: mpsc::Receiver<Vec<Effect>>,
    /// Held for the run's whole life so the channel above never closes: on wasm
    /// it is cloned into the sync door, and natively there is no door to write to
    /// it and the arm simply never fires.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        expect(
            dead_code,
            reason = "held to keep the effects channel open; the sync door that writes to it is \
                      the browser build's seam"
        )
    )]
    effects_tx: mpsc::Sender<Vec<Effect>>,
    /// The handle's publish pre-check, refreshed here at every edge that moves
    /// what it answers. Shared with the platform half, which only reads it.
    gate: Arc<Mutex<SurfaceGate>>,
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
    ///
    /// `front` is the receiving half of the front door: the four command channels
    /// the platform half's handle writes to, its event sink, the publish gate this
    /// run keeps current, and (on wasm) the in-flight buffer slot it fills.
    pub fn new(page: SurfacePage, config: ConnConfig, connector: C, front: FrontChannels) -> Self {
        let FrontChannels {
            events_tx,
            control_rx,
            publish_rx,
            alert_rx,
            telemetry_rx,
            #[cfg(target_arch = "wasm32")]
            in_flight,
            gate,
        } = front;
        #[cfg(not(target_arch = "wasm32"))]
        let page = RefCell::new(page);
        #[cfg(target_arch = "wasm32")]
        let page = std::rc::Rc::new(RefCell::new(page));
        #[cfg(not(target_arch = "wasm32"))]
        let entries = RefCell::new(HashMap::new());
        #[cfg(target_arch = "wasm32")]
        let entries = std::rc::Rc::new(RefCell::new(HashMap::new()));
        let (effects_tx, effects_rx) = mpsc::channel(EFFECTS_CHANNEL_CAPACITY);
        Self {
            driver: AttachDriver::new(config, connector),
            page,
            entries,
            #[cfg(target_arch = "wasm32")]
            in_flight,
            events_tx,
            control_rx,
            control_closed: false,
            publish_rx,
            publish_closed: false,
            alert_rx,
            alert_closed: false,
            telemetry_rx,
            telemetry_closed: false,
            effects_rx,
            effects_tx,
            gate,
            clock_usable: true,
        }
    }

    /// The browser's synchronous side door onto this run's page.
    ///
    /// Hands out the cells a `brenn-activation-sync` request needs to assemble,
    /// invoke and complete an activation on the requester's own stack — plus the
    /// channel its two turns' effects come back through, so that the loop stays
    /// the only thing that enacts anything.
    ///
    /// Taken before the run is spawned; the door outlives it, which is why the
    /// browser build's [`SurfaceRunner::run`] hands nothing back.
    #[cfg(target_arch = "wasm32")]
    pub fn sync_door(&self) -> crate::sync_door::SyncDoor {
        crate::sync_door::SyncDoor::new(
            std::rc::Rc::clone(&self.page),
            std::rc::Rc::clone(&self.entries),
            self.in_flight.clone(),
            self.effects_tx.clone(),
        )
    }

    /// Run to completion: connect, serve turns until the attachment is terminal,
    /// then drain (see the module doc). Returns only when the platform half is
    /// gone.
    ///
    /// Hands the page back. The page outlives the attachment — its rings, its
    /// retained control planes and its registrations are all still there — so the
    /// run gives it up rather than dropping it on the floor.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn run(mut self) -> SurfacePage {
        let wall = wall_now();
        self.drive(wall).await;
        self.page.into_inner()
    }

    /// The browser's run, which hands nothing back: the sync door holds the page
    /// cell for the document's life, so there is no sole owner left to give it to.
    /// Nothing in the browser wanted it — the run only returns once the platform
    /// half is gone, and the page goes with the document.
    #[cfg(target_arch = "wasm32")]
    pub async fn run(mut self) {
        let wall = wall_now();
        self.drive(wall).await;
    }

    /// [`run`](Self::run) from a wall-clock reading the caller already took, which
    /// is the reading the boot clock check is made against.
    ///
    /// Split out because that check is the one branch with no other way in: a
    /// device clock reading before the Unix epoch is not a state a test can put a
    /// machine into — so the native suite is its only caller.
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) async fn run_from(mut self, wall: DateTime<Utc>) -> SurfacePage {
        self.drive(wall).await;
        self.page.into_inner()
    }

    /// Everything a run does, from the boot clock check to the terminal drain.
    async fn drive(&mut self, wall: DateTime<Utc>) {
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
            // After the turn, not inside it: everything that turn delivered is
            // already in the positions an assembly windows, which is what makes
            // one turn's deliveries one activation rather than N.
            self.activation_pass().await;
        }
        if !self.host_gone() {
            self.run_terminal_drain().await;
        }
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
    ///
    /// The sync door's effects are drained the same way and for the same reason:
    /// the door admits a request whenever the page is wired, so a person clicking
    /// at a stalled handshake produces one hand-back per gesture, and that channel
    /// panics when it fills. They are enacted before the settled attempt is folded,
    /// which is the order the page took the turns in.
    ///
    /// The other three channels are not served here at all, and need not be: a
    /// publish backlog answers its own callers `Busy` at the handle, and the two
    /// best-effort planes drop what they cannot hold. Only control and the door's
    /// effects panic when they fill, so only those two are drained.
    async fn run_connect(&mut self, url: String) {
        let mut buffered: Vec<RunnerCommand> = Vec::new();
        let mut buffered_effects: Vec<Vec<Effect>> = Vec::new();
        let mut control_open = !self.control_closed;
        let settled = {
            let Self {
                driver,
                control_rx,
                effects_rx,
                ..
            } = &mut *self;
            let connect = driver.connect(&url).fuse();
            pin_mut!(connect);
            loop {
                // Copied into the control future so the arm below may clear the
                // flag without clashing with that future's borrow.
                let open = control_open;
                let settled = {
                    let control = recv_open(control_rx, !open).fuse();
                    // The door's sender lives as long as the run, so this arm
                    // never spins on a ready `None`.
                    let effects = recv_open(effects_rx, false).fuse();
                    pin_mut!(control, effects);
                    select_biased! {
                        input = connect => Some(input),
                        effects = effects => {
                            if let Some(effects) = effects {
                                buffered_effects.push(effects);
                            }
                            None
                        }
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
        // Before the attempt's own turn: the door took these turns while the
        // attempt was in flight, so the page state behind them is the older one.
        for effects in buffered_effects {
            self.execute(effects).await;
        }
        self.absorb(step).await;
        for command in buffered {
            self.control(command).await;
        }
    }

    /// Wait for the next thing that concerns the page and serve it.
    async fn run_select(&mut self) {
        // Read before the arms are built rather than inside one: the answer is a
        // borrow of the page cell, and the arm below holds its future across an
        // await that the sync door can run inside of.
        let has_ready = self.has_ready();
        let woke = {
            let Self {
                driver,
                control_rx,
                control_closed,
                publish_rx,
                publish_closed,
                alert_rx,
                alert_closed,
                telemetry_rx,
                telemetry_closed,
                effects_rx,
                ..
            } = &mut *self;
            let io = driver.wait().fuse();
            // The door's sender lives as long as the run, so this channel never
            // closes and the arm never spins on a ready `None`.
            let effects = recv_open(effects_rx, false).fuse();
            let control = recv_open(control_rx, *control_closed).fuse();
            let publish = recv_open(publish_rx, *publish_closed).fuse();
            let alert = recv_open(alert_rx, *alert_closed).fuse();
            let telemetry = recv_open(telemetry_rx, *telemetry_closed).fuse();
            // Ready while an instance is dispatchable, pending forever otherwise,
            // so work the previous pass's budget left over is picked up here rather
            // than waiting on an unrelated frame or deadline.
            //
            // The yield is not a detail. A component in a publish cycle keeps this
            // arm ready forever, and an arm that answers ready on its first poll
            // means this select — and so the whole run — never once returns
            // `Pending`. The executor would never get control back, and the
            // executor is the only thing that can hand the socket a frame to make
            // the transport arm ready in the first place: on wasm it is the JS
            // event loop the WebSocket callbacks run on. Biasing anything above
            // this arm buys nothing if the frame can never arrive. Which is also
            // why the browser's yield is a task hop and not a microtask one — see
            // `yield_now`.
            let activations = async {
                if has_ready {
                    yield_now().await;
                } else {
                    future::pending::<()>().await;
                }
            }
            .fuse();
            pin_mut!(io, effects, control, publish, alert, telemetry, activations);
            select_biased! {
                event = io => Woke::Io(event),
                effects = effects => Woke::Front(FrontWoke::Effects(effects)),
                command = control => Woke::Front(FrontWoke::Control(command)),
                command = publish => Woke::Front(FrontWoke::Publish(command)),
                command = alert => Woke::Front(FrontWoke::Alert(command)),
                command = telemetry => Woke::Front(FrontWoke::Telemetry(command)),
                () = activations => Woke::Activations,
            }
        };
        // The bias puts the door's effects above the front door's channels, but
        // the socket sits above both — and what it woke with is about to become a
        // turn *newer* than anything the door already took. So its queue is
        // emptied first, or an older turn's effects would be enacted after a
        // younger turn's.
        if matches!(woke, Woke::Io(_)) {
            self.drain_effects().await;
        }
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
            Woke::Front(front) => self.serve(front).await,
            // Nothing to serve: the readiness and the positions behind it are the
            // page's already, and the pass at the end of the loop is what takes
            // them. This arm exists only to be woken by.
            Woke::Activations => {}
        }
    }

    /// Enact everything the sync door has already queued, without waiting for
    /// more. Empty lists cost nothing: the send is a wake, and the pass at the end
    /// of the loop is what takes the readiness it announces.
    async fn drain_effects(&mut self) {
        while let Ok(effects) = self.effects_rx.try_recv() {
            self.execute(effects).await;
        }
    }

    /// Serve one thing the platform half asked for, or record that the channel it
    /// would have come on has gone.
    ///
    /// A closed channel is not the end of the run: the run's life is tied to the
    /// attachment and the event sink, not to who can still command it. Each flag
    /// only stops the loop selecting a channel that would answer `None` forever.
    async fn serve(&mut self, woke: FrontWoke) {
        match woke {
            FrontWoke::Effects(Some(effects)) => self.execute(effects).await,
            // Unreachable while the run holds its own sender, and harmless if it
            // ever were not: there is nothing left to enact.
            FrontWoke::Effects(None) => {}
            FrontWoke::Control(Some(command)) => self.control(command).await,
            FrontWoke::Control(None) => self.control_closed = true,
            FrontWoke::Publish(Some(slot)) => self.publish(slot).await,
            FrontWoke::Publish(None) => self.publish_closed = true,
            FrontWoke::Alert(Some(alert)) => self.turn(Input::Command(alert_command(alert))).await,
            FrontWoke::Alert(None) => self.alert_closed = true,
            FrontWoke::Telemetry(Some(document)) => {
                self.turn(Input::Command(telemetry_command(document))).await;
            }
            FrontWoke::Telemetry(None) => self.telemetry_closed = true,
        }
    }

    /// Serve one thing off the publish channel: a component's publish on one of
    /// its own ports, or a report from the kernel's log path.
    ///
    /// A publish is minted an envelope identity whatever its port turns out to
    /// bind. Only the page holds the wiring that says which class the port falls
    /// on, and this is the layer that reads entropy — so the stamp is minted here
    /// and spent there, or discarded when the peer is the one that will mint the
    /// authoritative envelope.
    async fn publish(&mut self, slot: PublishSlot) {
        let command = match slot {
            PublishSlot::Publish(PublishCommand {
                correlation,
                instance,
                port,
                body,
                urgency,
            }) => Command::Publish {
                publish: PortPublish {
                    instance,
                    port,
                    body,
                    urgency,
                    correlation,
                },
                stamp: self.driver.new_stamp(),
            },
            PublishSlot::Report(ReportCommand {
                level,
                source,
                message,
                subject,
            }) => Command::Report {
                level,
                source,
                message,
                subject,
            },
        };
        self.turn(Input::Command(command)).await;
    }

    /// Invoke the activations the page has ready — one pass over the ready set,
    /// not a drain to exhaustion.
    ///
    /// **The bound is load-bearing.** An ok flush carrying a confined entry routes
    /// synchronously, which wakes every reader bound to that channel — the
    /// publisher itself included, if it reads what it writes — and the input grant
    /// is deliberately 1:1 solvent, so a component that republishes what it
    /// consumes never runs out of budget to do it with. Draining until the page has
    /// nothing ready would then never return: the run would stop reading the
    /// socket, stop serving deadlines and stop activating every other instance —
    /// one buggy component hanging the page, which is precisely what the kernel's
    /// containment story is supposed to bound. So a pass gets one activation per
    /// registered instance, which the page's rotating pick makes a fair one, and
    /// whatever is still ready comes back through the select's own activations arm.
    /// A publish cycle is a livelock the page survives, not a hang it does not.
    ///
    /// Invocation is synchronous: wasm is single-threaded and the flush rule needs
    /// a return value, not a future. So a pass cannot be re-entered mid-entry, and
    /// the in-flight buffer cannot be observed by anything but the entry itself.
    async fn activation_pass(&mut self) {
        let mut budget = self.page.borrow().registrations.len();
        while budget > 0 && self.has_ready() {
            budget -= 1;
            let now = self.driver.now();
            let now_ms = self.now_ms();
            let effects = self.run_activation(now, now_ms);
            self.execute(effects).await;
        }
    }

    /// Whether any instance is dispatchable right now.
    fn has_ready(&self) -> bool {
        outward::ready(&self.page.borrow()).is_some()
    }

    /// Assemble, invoke and complete one activation in **one synchronous stretch**,
    /// answering both turns' effects for the caller to enact.
    ///
    /// The stretch is the point. Effects are requests — a frame to write, a
    /// deadline to restate, an event for the platform half — and every one of them
    /// is performed asynchronously anyway, so nothing is lost by deferring them
    /// past the completion. What is *gained* is the dichotomy the browser's sync
    /// door needs: an entry is on the stack iff an activation is in flight. Enact
    /// the assembly's effects in between and the page holds an instance in flight
    /// across an await with nobody's entry running, and a genuine user gesture
    /// landing in that window would be refused as re-entrant.
    ///
    /// One clock reading covers the whole stretch, for the same reason a flush's
    /// stamps share one: it is one commit, and its parts must agree about when now
    /// was.
    fn run_activation(&self, now: Millis, now_ms: u64) -> Vec<Effect> {
        // Held across the invocation deliberately: on wasm this borrow *is* the
        // "an entry is on the stack" fact the sync door reads.
        let mut page = self.page.borrow_mut();
        let (ready, mut effects) = turn::dispatch(&mut page, now, now_ms);
        // The window's own loud rungs — an `alarm` binding's alert and toast, a
        // `fatal` binding's kill — ride in these effects, and the kill is why there
        // may be no entry to run.
        let Some(ready) = ready else { return effects };
        let ReadyActivation {
            instance,
            generation,
            activation,
            buffer,
            drops: _,
        } = ready;
        let (outcome, buffer) = self.invoke(&instance, &activation, buffer);
        // One stamp per buffered publish: the page is the router for a flush's
        // confined entries and reads no entropy of its own. Minted for every
        // entry whatever its class, for the same reason a single publish is —
        // only the page resolves which channel a port names.
        let stamps = self.driver.flush_stamps(buffer.len());
        effects.extend(turn::on_input(
            &mut page,
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
        effects
    }

    /// Call one instance's entry and classify how it finished.
    fn invoke(
        &self,
        instance: &str,
        activation: &Activation,
        buffer: PublishBuffer,
    ) -> (ActivationOutcome, PublishBuffer) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let entries = self.entries.borrow();
            let Some(entry) = entries.get(instance) else {
                return (missing_entry_trap(), buffer);
            };
            invoke_native(entry, activation, buffer)
        }
        #[cfg(target_arch = "wasm32")]
        {
            invoke_shared(&self.entries, &self.in_flight, instance, activation, buffer)
        }
    }

    /// Serve the front door after the attachment has ended.
    ///
    /// Only the kernel's own confined planes are still worth stating — the page's
    /// rings outlive the attachment and their readers are still mounted — so a
    /// `PublishControl` routes and everything else is absorbed: there is no wire
    /// for a mount to subscribe on, nothing left to close, and no peer to answer
    /// an alert or take a document.
    ///
    /// Routing is only half of what a mounted reader needs, so each turn here is
    /// followed by the same bounded activation pass the live loop takes: a routed
    /// body that marks a reader ready draws nothing until that reader's entry
    /// runs, and the banner chrome draws the page's own death from arrives on
    /// exactly this path. The pass's precondition holds — the wiring in force
    /// outlives the attachment, and a ready instance implies it — and a publish
    /// composed while detached reaches no socket, so the drain writes nothing.
    /// Unlike the live loop there is no arm to come back through: what a pass's
    /// budget leaves over waits for the next thing the front door says, which
    /// keeps a component in a publish cycle from spinning a page that is already
    /// winding down.
    ///
    /// A publish is the one thing that is neither absorbed nor answered here: it
    /// takes the ordinary turn, exactly as it would have one turn earlier. Its
    /// caller was handed a correlation and is owed a disposition, and only the page
    /// knows which class the port falls on — a confined one still routes and still
    /// wakes its readers, and a transportable one is refused `NotConnected` by the
    /// same predicate the gate answers with. Answering the whole channel from here
    /// would refuse page-local work the class is offline-correct for, and would be
    /// a second copy of a disposition the page already owns.
    // TODO(runner-drain-host-departure): terminal disarms every deadline and drops
    // the transport, so the wait below has no wake source but the front door's own
    // channels — a platform half that drops the event receiver while holding idle
    // senders parks this drain, and the page and the task leak with it. Everywhere
    // else the run re-reads `host_gone` on a bounded cadence.
    // TODO(terminal-drain-release-deadline): the same missing io arm means
    // `Input::ReleaseDue` never fires here, so a confined message parked before
    // terminal never releases and every component's tick chain stops. Detached
    // pages are unaffected; terminal ones freeze.
    async fn run_terminal_drain(&mut self) {
        loop {
            // Nothing consumes what a routed publish would produce, and no
            // command can arrive: either way the run is over.
            if self.host_gone() || self.front_closed() {
                return;
            }
            let woke = {
                let Self {
                    control_rx,
                    control_closed,
                    publish_rx,
                    publish_closed,
                    alert_rx,
                    alert_closed,
                    telemetry_rx,
                    telemetry_closed,
                    effects_rx,
                    ..
                } = &mut *self;
                let effects = recv_open(effects_rx, false).fuse();
                let control = recv_open(control_rx, *control_closed).fuse();
                let publish = recv_open(publish_rx, *publish_closed).fuse();
                let alert = recv_open(alert_rx, *alert_closed).fuse();
                let telemetry = recv_open(telemetry_rx, *telemetry_closed).fuse();
                pin_mut!(effects, control, publish, alert, telemetry);
                select_biased! {
                    effects = effects => FrontWoke::Effects(effects),
                    command = control => FrontWoke::Control(command),
                    command = publish => FrontWoke::Publish(command),
                    command = alert => FrontWoke::Alert(command),
                    command = telemetry => FrontWoke::Telemetry(command),
                }
            };
            self.absorb_front(woke).await;
            self.activation_pass().await;
        }
    }

    /// Dispose of one thing the platform half asked for after the attachment
    /// ended.
    async fn absorb_front(&mut self, woke: FrontWoke) {
        match woke {
            // Enacted, not absorbed: the page already took the turn that produced
            // these, and a mounted reader still draws what its confined planes
            // say. The transportable half is dropped by `execute` itself, which
            // knows the wire is gone.
            FrontWoke::Effects(Some(effects)) => self.execute(effects).await,
            FrontWoke::Effects(None) => {}
            FrontWoke::Control(None) => self.control_closed = true,
            FrontWoke::Control(Some(RunnerCommand::PublishControl { channel, body })) => {
                self.publish_control(channel, body).await;
            }
            // Named rather than silently swallowed: during an incident a mount
            // that did nothing reads as a call that never happened unless the log
            // says it arrived late.
            FrontWoke::Control(Some(RunnerCommand::RegisterActivation { instance, .. })) => {
                tracing::debug!(
                    %instance,
                    "surface runner: absorbed a mount after the attachment ended"
                );
            }
            FrontWoke::Control(Some(RunnerCommand::DeregisterActivation { instance })) => {
                tracing::debug!(
                    %instance,
                    "surface runner: absorbed an unmount after the attachment ended"
                );
            }
            FrontWoke::Control(Some(RunnerCommand::Close)) => {
                tracing::debug!("surface runner: absorbed a close after the attachment ended");
            }
            FrontWoke::Publish(None) => self.publish_closed = true,
            FrontWoke::Publish(Some(slot @ PublishSlot::Publish(_))) => self.publish(slot).await,
            // A report is owed nobody an answer, here least of all: the loop a
            // report about a failed report opens is what the log path's swallow
            // closes.
            FrontWoke::Publish(Some(PublishSlot::Report(report))) => {
                tracing::debug!(
                    level = ?report.level,
                    "surface runner: absorbed a report after the attachment ended"
                );
            }
            FrontWoke::Alert(None) => self.alert_closed = true,
            FrontWoke::Alert(Some(_)) => {
                tracing::debug!("surface runner: absorbed an alert after the attachment ended");
            }
            FrontWoke::Telemetry(None) => self.telemetry_closed = true,
            FrontWoke::Telemetry(Some(_)) => {}
        }
    }

    /// Whether every channel of the front door has closed: the platform half
    /// dropped its handle, so nothing can ask this run for anything again.
    ///
    /// The effects channel is not one of them and deliberately never closes — the
    /// run holds its own sender — so it is not asked about here: this predicate is
    /// about who can still command the run, and nobody commands through effects.
    fn front_closed(&self) -> bool {
        self.control_closed && self.publish_closed && self.alert_closed && self.telemetry_closed
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
                self.entries.borrow_mut().insert(instance.clone(), entry);
                self.turn(Input::ActivationRegistered { instance }).await;
            }
            RunnerCommand::DeregisterActivation { instance } => {
                self.entries.borrow_mut().remove(&instance);
                self.turn(Input::ActivationDeregistered { instance }).await;
            }
            RunnerCommand::PublishControl { channel, body } => {
                self.publish_control(channel, body).await;
            }
            RunnerCommand::Close => {
                self.turn(Input::Command(Command::Close)).await;
                // The one edge that ends an attachment without the connection
                // reporting anything: the page detached itself, so the gate must
                // stop admitting what there is no longer a wire for.
                self.refresh_gate();
            }
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
        let now = self.driver.now();
        turn::on_input(&mut self.page.borrow_mut(), input, now, now_ms)
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
            // Every one of the connection's own events is an edge of the
            // attachment — it came up, it went away, it ended — and each moves
            // what a publish is judged against: the body cap a new attachment
            // states, and whether there is a configured one at all.
            self.refresh_gate();
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
                Effect::EmitEvent(event) => {
                    // The two events a document produces are the other thing that
                    // moves the gate: the bound ports it names and the report
                    // floor it states are read straight off the wiring in force.
                    let wiring = matches!(event, Event::Connected { .. } | Event::WiringChanged);
                    self.emit(event);
                    if wiring {
                        self.refresh_gate();
                    }
                }
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

    /// Re-take the handle's publish gate from the page.
    ///
    /// Called at the edges that move what it answers and nowhere else: every one
    /// of the connection's own events, the two events a document in force
    /// produces, and the close the platform half asks for. Refreshing on every
    /// turn instead would re-copy the bound-port table on every delivery, which is
    /// the cost the gate exists to avoid — and nothing between those edges changes
    /// a single answer it gives.
    fn refresh_gate(&self) {
        self.gate
            .lock()
            .expect("surface runner: the publish gate mutex is poisoned")
            .refresh(&self.page.borrow());
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

/// The outcome for an activation whose entry is gone: a **trap**.
///
/// It cannot happen — assembly and invocation are one synchronous stretch, and a
/// deregistration arrives on the control channel — but the page marked the
/// instance in flight and is owed a completion, or it never activates again; and
/// an activation whose entry vanished did not return ok. The page absorbs this
/// one anyway (the mount is gone, so the generation no longer matches), which is
/// what makes reporting it as a trap free of consequence beyond the log.
fn missing_entry_trap() -> ActivationOutcome {
    ActivationOutcome::Trap(
        "activation entry deregistered between assembly and invocation".to_string(),
    )
}

/// The browser's invocation against the shared entry table, so the run and the
/// sync door reach a component's entry exactly the same way.
#[cfg(target_arch = "wasm32")]
pub(crate) fn invoke_shared(
    entries: &SharedEntries,
    slot: &crate::front::InFlightSlot,
    instance: &str,
    activation: &Activation,
    buffer: PublishBuffer,
) -> (ActivationOutcome, PublishBuffer) {
    let entries = entries.borrow();
    match entries.get(instance) {
        Some(entry) => invoke_wasm(entry, activation, slot, instance, buffer),
        None => (missing_entry_trap(), buffer),
    }
}

/// The native build's invocation.
///
/// A panic is a **trap**, not an err: the two are different facts with different
/// consequences — an err keeps the instance running, a trap is terminal for it —
/// and only the invocation boundary can tell them apart. `catch_unwind` is the
/// native equivalent of the JS exception a wasm host observes, which is the same
/// discrimination the backend's wasmtime host gets for free.
///
/// Both failure arms carry the component's own account of what happened. The
/// kernel never parses it, but it is the only answer an operator has to "failed
/// *how*?", and this boundary is the only place it exists.
///
/// It is also where the reply is judged against the activation that earned it: a
/// `Result` cannot say "ok, but only if you asked me something", so the check
/// belongs at the call, where both halves are in hand.
///
/// Takes the buffer by value and hands it back so both builds' invocations share
/// one call shape; the wasm build cannot pass `&mut` (see below).
#[cfg(not(target_arch = "wasm32"))]
fn invoke_native(
    entry: &ActivationEntry,
    activation: &Activation,
    mut buffer: PublishBuffer,
) -> (ActivationOutcome, PublishBuffer) {
    // `AssertUnwindSafe`: the buffer may be left half-filled by a panicking entry,
    // which is exactly the state the trap path is built for — the page discards it
    // whole. Nothing observes a partially-published buffer.
    let called = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        entry(activation, &mut buffer)
    }));
    let outcome = match called {
        // A reply on an activation that asked no question is a trap, not an ok:
        // the entry answered something other than what it was handed, so its
        // buffer was built under a misapprehension and must not be flushed. The
        // browser's return-value classifier rules the same way on the same fact;
        // this is the other boundary the same rule has to hold at.
        Ok(Ok(Some(reply))) if activation.sync.is_none() => ActivationOutcome::Trap(format!(
            "activation entry replied {reply:?} to an activation with no sync port"
        )),
        Ok(Ok(reply)) => ActivationOutcome::Ok(reply),
        Ok(Err(err)) => ActivationOutcome::Err(err),
        Err(payload) => ActivationOutcome::Trap(unwind_message(payload)),
    };
    (outcome, buffer)
}

/// The panic message out of a `catch_unwind` payload.
///
/// `panic!` produces a `String` (formatted) or a `&'static str` (literal), which
/// covers every panic a component can raise through the ordinary macro. A payload
/// of any other type came from `panic_any` and carries no text to recover, so it
/// is named as such rather than guessed at — the message is diagnostic, and
/// inventing detail for it would be worse than admitting there is none.
#[cfg(not(target_arch = "wasm32"))]
fn unwind_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        "activation entry panicked with a non-string payload".to_string()
    }
}

/// The wasm build's invocation.
///
/// `catch_unwind` cannot observe a wasm panic, so the outcome discrimination this
/// build gets comes from the other side: the entry wraps a JS function, and a
/// thrown exception is a trap where a returned string is an err. The wrapper does
/// that classification and hands back an [`ActivationOutcome`] already.
///
/// The buffer travels through the shared in-flight slot rather than as an argument:
/// a `dom` entry publishes by dispatching an event, which surfaces on the kernel's
/// root listener — a code path that cannot reach the buffer on this stack.
/// Installed for exactly the duration of the call and taken back on return, so no
/// publish can find a buffer outside an activation.
#[cfg(target_arch = "wasm32")]
fn invoke_wasm(
    entry: &ActivationEntry,
    activation: &Activation,
    slot: &crate::front::InFlightSlot,
    instance: &str,
    buffer: PublishBuffer,
) -> (ActivationOutcome, PublishBuffer) {
    *slot.borrow_mut() = Some(crate::front::InFlightPublish {
        instance: instance.to_string(),
        buffer,
    });
    let outcome = entry(activation);
    // Taken back unconditionally: the entry returned (a trap here is a JS exception
    // the wrapper already caught and classified), so leaving the buffer installed
    // would let a publish made after it join an activation that is over.
    let in_flight = slot
        .borrow_mut()
        .take()
        .expect("surface runner: the in-flight buffer vanished during an activation");
    (outcome, in_flight.buffer)
}

/// Hand the executor control once, then resolve.
///
/// `tokio::task::yield_now`'s job, written out because this loop takes no tokio
/// dependency. Waking the task before parking re-queues it behind whatever else
/// the reactor has ready, which is the whole of a yield on a native executor.
#[cfg(not(target_arch = "wasm32"))]
async fn yield_now() {
    let mut yielded = false;
    future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            // Reschedule before parking: this is a yield, not a wait. Nothing else
            // will ever wake us.
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// Hand the browser control once, then resolve — a **macrotask** hop, not a
/// microtask one.
///
/// The distinction is the whole of the containment story on this target. Under
/// `spawn_local` a wake re-queues the task with `queueMicrotask`, and the HTML
/// event loop drains the microtask queue — including microtasks enqueued during
/// the drain — before it returns to tasks. A permanently-ready activations arm
/// waking itself therefore chains microtask to microtask forever: the WebSocket's
/// `message` callback, every `setTimeout` the driver armed and the next paint are
/// all tasks, and none of them would ever run again. A component in a publish
/// cycle would hang the tab rather than livelock inside it.
///
/// `setTimeout(0)` is a task, so the chain breaks at every hop and the page keeps
/// being served.
#[cfg(target_arch = "wasm32")]
async fn yield_now() {
    brenn_attach_client::transport::timer::sleep(std::time::Duration::ZERO).await;
}

/// Receive from a channel, or wait forever once its senders have gone.
///
/// A closed `futures` receiver answers `None` on every poll, so a select arm over
/// one spins the whole loop. Every arm over a front-door channel therefore carries
/// the flag that says it has already answered `None` once.
async fn recv_open<T>(rx: &mut mpsc::Receiver<T>, closed: bool) -> Option<T> {
    if closed {
        future::pending::<Option<T>>().await
    } else {
        rx.next().await
    }
}

/// One alert as the page's command vocabulary states it.
fn alert_command(alert: AlertCommand) -> Command {
    let AlertCommand {
        severity,
        title,
        body,
    } = alert;
    Command::Alert {
        severity,
        title,
        body,
    }
}

/// One of the surface's own documents as the page's command vocabulary states it.
fn telemetry_command(document: TelemetryCommand) -> Command {
    match document {
        TelemetryCommand::Geometry {
            width,
            height,
            device_pixel_ratio,
        } => Command::Geometry {
            width,
            height,
            device_pixel_ratio,
        },
        TelemetryCommand::Status {
            instances,
            uptime_secs,
            counters,
        } => Command::Status {
            instances,
            uptime_secs,
            counters,
        },
    }
}

/// Which arm of the live select fired.
enum Woke {
    Io(IoEvent),
    Front(FrontWoke),
    /// An instance is dispatchable; the pass at the end of the loop takes it.
    Activations,
}

/// Which of the front door's channels spoke — or the sync door's effects — and
/// whether it has closed.
///
/// One vocabulary for both selects — the live loop's and the terminal drain's —
/// because what arrives is the same either way; what differs is only what becomes
/// of it.
enum FrontWoke {
    /// The effects of a turn the sync door already ran on the page — not something
    /// the platform half asked for.
    Effects(Option<Vec<Effect>>),
    Control(Option<RunnerCommand>),
    Publish(Option<PublishSlot>),
    Alert(Option<AlertCommand>),
    Telemetry(Option<TelemetryCommand>),
}

/// The browser half of the yield contract, executed rather than reasoned about.
///
/// The native starvation tests in `runner/tests.rs` pin that the loop yields
/// between passes and rotates fairly, but they run under tokio and say nothing
/// about the JS event loop. What remains is the property `yield_now` exists
/// for: the hop reaches the browser's **task** queue, not just its microtask
/// queue — the difference between a publish cycle costing the page a turn and
/// hanging the tab.
///
/// Lives in `runner.rs` rather than beside the native suite because `yield_now`
/// is private to this module.
#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_tests {
    use super::yield_now;
    use std::cell::Cell;
    use std::rc::Rc;
    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    /// A zero-delay timer armed *before* the yield must have run by the time the
    /// yield resolves.
    ///
    /// Determinism is the HTML timer queue's, not real time's: same-delay timers
    /// fire in registration order, and `yield_now` arms its own zero-delay timer
    /// only when it is first polled — i.e. after this one. So if the await ever
    /// reaches a task, it reaches this task first. A microtask-chained
    /// implementation resolves at the microtask checkpoint, before the event loop
    /// returns to tasks at all, and leaves the flag false.
    #[wasm_bindgen_test]
    async fn yield_now_reaches_the_browser_task_queue() {
        let window = web_sys::window().expect("browser test page has a Window");
        let fired = Rc::new(Cell::new(false));
        let flag = Rc::clone(&fired);
        // Held alive across the await: a dropped `Closure` is invalidated, and the
        // callback would be gone before the timer fired.
        let callback = Closure::once(move || flag.set(true));
        window
            .set_timeout_with_callback_and_timeout_and_arguments_0(
                callback.as_ref().unchecked_ref(),
                0,
            )
            .expect("setTimeout is available in the browser");

        assert!(!fired.get(), "a timer callback cannot run synchronously");
        yield_now().await;
        assert!(
            fired.get(),
            "yield_now resolved without the event loop reaching its task queue"
        );

        drop(callback);
    }
}

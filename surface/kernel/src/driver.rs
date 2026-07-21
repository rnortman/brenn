//! The async driver — the one small loop that turns the sans-I/O [`ClientCore`]
//! into a running client.
//!
//! The driver owns the core, a transport connector, and the live connection. It
//! `select`s over transport events and the core's armed timer, feeds each as a
//! core [`Input`] stamped with the monotonic clock, and executes the returned
//! [`Effect`]s in order: opening and closing transports, writing client frames,
//! and emitting control-plane [`Event`]s to the kernel's EventStream.
//!
//! Every `cfg`-gated concern (the transport, the timer, the clock) lives behind
//! a trait or a shim, so this loop compiles unchanged for wasm and native. It
//! uses only `futures-util` primitives — no tokio types — so the same code runs
//! under `spawn_local` in the browser and `tokio::spawn` in tests.
//!
//! This layer owns the connection lifecycle (connect / backoff / handshake /
//! liveness / fatal) and the command plane: it selects the handle's control,
//! publish, and (best-effort) log channels alongside the transport and timer,
//! registers each attached port's fan-out queue (dropping it on detach), feeds the
//! core the corresponding command, and refreshes the handle's [`PublishGate`] on
//! every connection-state transition. Control is biased ahead of publish and
//! log, so neither a publish backlog nor a log flood starves attach/detach.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use futures_channel::mpsc;
use futures_util::{FutureExt, StreamExt, future, pin_mut, select_biased};

use brenn_surface_contract::Activation;

use crate::core::{
    ActivationOutcome, ClientCore, Command, CoreConfig, Effect, Event, Input, MessageStamp,
    PublishBuffer, PublishStatus, ReadyActivation,
};
use crate::handle::{
    ActivationEntry, AlertCommand, DriverChannels, HandleCommand, PublishCommand, PublishGate,
    TelemetryCommand,
};
use crate::transport::clock::{Clock, wall_now};
use crate::transport::timer;
use crate::transport::{TransportConnection, TransportConnector, TransportEvent};

/// Read the non-deterministic values an envelope needs and hand them to the core
/// as data — the same move as stamping `Clock::now()` on every input, and for the
/// same reason: the core is sans-I/O and can read neither source itself.
///
/// The core consumes this only when it routes the publish on a `local:` channel,
/// where it is the router and must mint the envelope; a wire publish discards it
/// and takes the server's authoritative envelope. See `Command::Publish::stamp`
/// for why that is stamped unconditionally rather than resolved here.
fn new_stamp() -> MessageStamp {
    MessageStamp {
        message_id: uuid::Uuid::new_v4(),
        publish_ts: wall_now(),
    }
}

/// The async driver, generic over the transport connector.
///
/// Construct with [`Driver::new`] and run to completion with [`Driver::run`];
/// the caller spawns it (`tokio::spawn` natively, `spawn_local` on wasm). The
/// run future completes when the core reaches its terminal `Fatal` state or the
/// EventStream receiver is dropped (the kernel has gone away).
pub struct Driver<C: TransportConnector> {
    core: ClientCore,
    connector: C,
    /// The live connection, present once a connect succeeds (`AwaitingWelcome`
    /// and `Active`); `None` while connecting, backing off, or terminal.
    conn: Option<C::Conn>,
    /// The EventStream producer. Bounded; an overflow is a kernel-not-draining
    /// bug (control-plane traffic is low-rate by construction) and panics.
    events_tx: mpsc::Sender<Event>,
    /// Registered activation entries, keyed by instance.
    ///
    /// Driver-side, not core-side, because an entry is a callback and the core is
    /// pure data: a `Box<dyn Fn>` in the core would make its inputs
    /// unclonable, undebuggable, and uncomparable, and the core is a state
    /// machine tests drive by feeding values and asserting on values. The core
    /// holds the *identity* of a registered instance and every scheduling
    /// decision about it; this map holds the one thing it cannot.
    entries: HashMap<String, ActivationEntry>,
    /// The handle's control command channel. Commands are selected alongside the
    /// transport and timer while a connection is live or backing off; during a
    /// connect attempt they are drained into a local buffer and applied once the
    /// attempt resolves, so a stalled handshake cannot let the channel fill to its
    /// panic bound.
    control_rx: mpsc::Receiver<HandleCommand>,
    /// Set once the control channel closes (the handle and all clones dropped):
    /// the driver stops selecting it. The driver's life is tied to the connection
    /// and the EventStream, not the handle, so this is not terminal.
    control_closed: bool,
    /// The handle's publish command channel, selected after control so a publish
    /// backlog never starves attach/detach. Each command becomes a
    /// `Command::Publish` fed to the core.
    publish_rx: mpsc::Receiver<PublishCommand>,
    /// Set once the publish channel closes; the driver stops selecting it. Not
    /// terminal, for the same reason as `control_closed`.
    publish_closed: bool,
    /// The handle's alert channel, selected after publish (best-effort, lowest
    /// priority). Each command becomes a `Command::Alert` fed to
    /// the core, which sends the frame only while `Active`.
    alert_rx: mpsc::Receiver<AlertCommand>,
    /// Set once the alert channel closes; the driver stops selecting it. Not
    /// terminal, for the same reason as `control_closed`.
    alert_closed: bool,
    /// The handle's telemetry channel, selected after alert (best-effort). Each
    /// command becomes a `Command::SendGeometry`/`SendStatus` fed to the core,
    /// which sends the frame only while `Active`.
    telemetry_rx: mpsc::Receiver<TelemetryCommand>,
    /// Set once the telemetry channel closes; the driver stops selecting it. Not
    /// terminal, for the same reason as `control_closed`.
    telemetry_closed: bool,
    /// The handle's publish pre-validation snapshot, refreshed here on every
    /// connection-state transition (Active on `Welcome`, not-Active on any
    /// teardown). The handle reads it to reject a doomed publish synchronously.
    gate: Arc<Mutex<PublishGate>>,
    /// The in-flight activation's buffer, shared with the handle. Filled here for
    /// exactly the duration of an entry invocation; the handle's buffered-publish
    /// route reads it from inside that call.
    #[cfg(target_arch = "wasm32")]
    in_flight: crate::handle::InFlightSlot,
    clock: Clock,
    /// The core's most recently requested wakeup deadline; the timer is armed
    /// from it each loop iteration. `None` disarms (only in the terminal state).
    wakeup: Option<crate::Millis>,
    /// The core's most recently requested outbox-retry deadline, armed on its own
    /// select arm. Independent of `wakeup`: the liveness schedule and the retry
    /// schedule are separate promises, and one deadline could only keep one.
    /// `None` disarms — the ordinary state, since a retry is only owed while some
    /// instance's outbox is blocked.
    retry_wakeup: Option<crate::Millis>,
    /// A URL the core asked to connect to, awaiting the connect race in the run
    /// loop. Set by [`Effect::Connect`], consumed at the top of the loop.
    pending_connect: Option<String>,
    /// The connect-on-spawn effects, executed at the start of [`Driver::run`]
    /// (effect execution is async, so it cannot run in the constructor).
    initial: Vec<Effect>,
    /// Set once the core emits `Event::Fatal` or the EventStream receiver drops;
    /// the run loop then exits.
    terminal: bool,
}

/// Call one activation entry and classify how it finished.
///
/// A panic is a **trap**, not an err: the two are different facts with different
/// consequences (an err keeps the instance running; a trap is terminal for it),
/// and only the invocation boundary can tell them apart. `catch_unwind` is the
/// native equivalent of the JS exception a wasm host observes, which is the same
/// discrimination the backend's wasmtime host gets for free.
///
/// Both failure arms carry the component's own account of what happened. The
/// kernel never parses it, but it is the only answer an operator has to "failed
/// *how*?", and this boundary is the only place it exists.
/// Takes the buffer by value and hands it back so both builds' invocations share
/// one call shape; the wasm build cannot pass `&mut` (see its definition below).
#[cfg(not(target_arch = "wasm32"))]
fn invoke(
    entry: &ActivationEntry,
    activation: &Activation,
    mut buffer: PublishBuffer,
) -> (ActivationOutcome, PublishBuffer) {
    // `AssertUnwindSafe`: the buffer may be left half-filled by a panicking
    // entry, which is exactly the state the trap path is built for — the core
    // discards it whole. Nothing observes a partially-published buffer.
    let called = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        entry(activation, &mut buffer)
    }));
    let outcome = match called {
        Ok(Ok(())) => ActivationOutcome::Ok,
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
/// The buffer travels through the shared in-flight slot rather than as an
/// argument: a `dom` entry publishes by dispatching an event, which surfaces on
/// the kernel's root listener — a code path that cannot reach the driver's stack.
/// Installed for exactly the duration of the call and taken back on return, so no
/// publish can find a buffer outside an activation.
// TODO(buffered-publish-routing-test): the slot install/take-back here is
// wasm-only and untested — the client crate has no browser-test harness yet.
#[cfg(target_arch = "wasm32")]
fn invoke(
    entry: &ActivationEntry,
    activation: &Activation,
    slot: &crate::handle::InFlightSlot,
    instance: &str,
    buffer: PublishBuffer,
) -> (ActivationOutcome, PublishBuffer) {
    *slot.borrow_mut() = Some(crate::handle::InFlightPublish {
        instance: instance.to_string(),
        buffer,
    });
    let outcome = entry(activation);
    // Taken back unconditionally: the entry returned (a trap here is a JS
    // exception the wrapper already caught and classified), so leaving the buffer
    // installed would make the next gesture publish look buffered.
    let in_flight = slot
        .borrow_mut()
        .take()
        .expect("surface driver: the in-flight buffer vanished during an activation");
    (outcome, in_flight.buffer)
}

impl<C: TransportConnector> Driver<C> {
    /// Build the driver and the core, starting the connect-on-spawn sequence.
    /// `events_tx` is the EventStream producer the kernel drains; `control_rx`
    /// receives the handle's commands.
    pub(crate) fn new(
        config: CoreConfig,
        connector: C,
        events_tx: mpsc::Sender<Event>,
        channels: DriverChannels,
        gate: Arc<Mutex<PublishGate>>,
        #[cfg(target_arch = "wasm32")] in_flight: crate::handle::InFlightSlot,
    ) -> Self {
        let DriverChannels {
            control_rx,
            publish_rx,
            alert_rx,
            telemetry_rx,
        } = channels;
        let clock = Clock::new();
        let (core, initial) = ClientCore::new(config, clock.now());
        Self {
            core,
            connector,
            conn: None,
            events_tx,
            entries: HashMap::new(),
            control_rx,
            control_closed: false,
            publish_rx,
            publish_closed: false,
            alert_rx,
            alert_closed: false,
            telemetry_rx,
            telemetry_closed: false,
            gate,
            #[cfg(target_arch = "wasm32")]
            in_flight,
            clock,
            wakeup: None,
            retry_wakeup: None,
            pending_connect: None,
            initial,
            terminal: false,
        }
    }

    /// Run the loop to completion. Executes the connect-on-spawn effects, then
    /// repeatedly connects (when the core asks) or waits on transport/timer
    /// events until the terminal state is reached. On the terminal transition it
    /// does its wind-down duties (fail queued publishes, close any live
    /// transport) and then enters the terminal drain loop rather than returning:
    /// the kernel folds the death event one event-loop hop later and issues its
    /// own terminal link-state `PublishControl`, which must still find a live
    /// driver to route onto the page-local router. It returns only when the
    /// kernel is gone (see [`Driver::run_terminal_drain`]).
    pub async fn run(mut self) {
        let initial = std::mem::take(&mut self.initial);
        self.execute(initial).await;
        while !self.terminal {
            match self.pending_connect.take() {
                Some(url) => self.run_connect(url).await,
                None => self.run_select().await,
            }
        }
        self.fail_queued_publishes();
        if let Some(mut conn) = self.conn.take() {
            conn.close().await;
        }
        self.run_terminal_drain().await;
    }

    /// Serve the handle's command channels after the core has gone terminal.
    ///
    /// The core's terminal state machine is authoritative: a `PublishControl`
    /// (the kernel's own `fatal`/`reloading` link-state publish) routes onto the
    /// page-local router, which outlives the terminal transition and is still
    /// mounted for chrome to draw the banner from; everything else is absorbed.
    /// A straggler component publish is answered `ConnectionLost`, matching
    /// [`Driver::fail_queued_publishes`] — in practice the handle's disconnected
    /// gate rejects publishes before they reach here, so these are rare races.
    ///
    /// No transport arm (there is none post-terminal) and no timers: the core
    /// serves nothing but command routing now. The loop exits only when the
    /// kernel is gone — the EventStream receiver dropped, or every command sender
    /// dropped. In production neither happens for the remaining page lifetime of
    /// a terminal state, so the driver task idles; that is intentional.
    async fn run_terminal_drain(&mut self) {
        loop {
            // The kernel event loop dropped the EventStream: nothing consumes a
            // routed publish and nothing will issue further commands.
            if self.events_tx.is_closed() {
                return;
            }
            // The handle and all its clones dropped: no command can arrive.
            if self.control_closed
                && self.publish_closed
                && self.alert_closed
                && self.telemetry_closed
            {
                return;
            }
            let outcome = {
                let Self {
                    control_rx,
                    control_closed,
                    publish_rx,
                    publish_closed,
                    alert_rx,
                    alert_closed,
                    telemetry_rx,
                    telemetry_closed,
                    ..
                } = &mut *self;
                let control = async {
                    if *control_closed {
                        future::pending::<Option<HandleCommand>>().await
                    } else {
                        control_rx.next().await
                    }
                }
                .fuse();
                let publish = async {
                    if *publish_closed {
                        future::pending::<Option<PublishCommand>>().await
                    } else {
                        publish_rx.next().await
                    }
                }
                .fuse();
                let alert = async {
                    if *alert_closed {
                        future::pending::<Option<AlertCommand>>().await
                    } else {
                        alert_rx.next().await
                    }
                }
                .fuse();
                let telemetry = async {
                    if *telemetry_closed {
                        future::pending::<Option<TelemetryCommand>>().await
                    } else {
                        telemetry_rx.next().await
                    }
                }
                .fuse();
                pin_mut!(control, publish, alert, telemetry);
                select_biased! {
                    command = control => TerminalOutcome::Control(command),
                    command = publish => TerminalOutcome::Publish(command),
                    command = alert => TerminalOutcome::Alert(command),
                    command = telemetry => TerminalOutcome::Telemetry(command),
                }
            };
            match outcome {
                TerminalOutcome::Control(Some(command)) => {
                    // Routes the terminal link-state PublishControl onto the
                    // page-local router; the terminal catch-all absorbs the rest.
                    let input = self.register_and_command(command);
                    let now = self.clock.now();
                    let effects = self.core.on_input(input, now);
                    self.execute(effects).await;
                }
                TerminalOutcome::Control(None) => self.control_closed = true,
                TerminalOutcome::Publish(Some(command)) => {
                    // A straggler that beat the handle's disconnected gate: the
                    // caller is owed a result, answered as ConnectionLost.
                    self.emit(Event::PublishResult {
                        instance: command.instance,
                        port: command.port,
                        correlation: command.correlation,
                        status: PublishStatus::ConnectionLost,
                    });
                }
                TerminalOutcome::Publish(None) => self.publish_closed = true,
                // Best-effort planes: absorbed in the terminal state, exactly as
                // the core's terminal catch-all would.
                TerminalOutcome::Alert(Some(_)) => {}
                TerminalOutcome::Alert(None) => self.alert_closed = true,
                TerminalOutcome::Telemetry(Some(_)) => {}
                TerminalOutcome::Telemetry(None) => self.telemetry_closed = true,
            }
        }
    }

    /// Answer every publish still sitting in the publish channel when the driver
    /// goes terminal. The handle accepted each (returned its correlation to the
    /// caller) but it never reached the core, so the core's own
    /// `fail_pending_publishes` cannot see it; without this drain the caller would
    /// wait forever for the `Event::PublishResult` the contract promises. Each is
    /// answered `ConnectionLost`, matching the core's disposition for publishes it
    /// did hold. Non-blocking: `try_recv` yields the queued commands and stops at
    /// the first empty/closed poll.
    fn fail_queued_publishes(&mut self) {
        while let Ok(command) = self.publish_rx.try_recv() {
            self.emit(Event::PublishResult {
                instance: command.instance,
                port: command.port,
                correlation: command.correlation,
                status: PublishStatus::ConnectionLost,
            });
        }
    }

    /// Race a connect attempt against the core's handshake timer, draining the
    /// handle's control channel meanwhile. No live transport exists during a
    /// connect, so only the connect result and a timeout tick can end the race;
    /// commands that arrive in the interim are buffered (not fed to the core yet,
    /// since that borrows `self` while the connect future holds `connector`) and
    /// applied once the race resolves. Buffering keeps a slow trickle of attaches
    /// during a stalled handshake from filling the control channel to its panic
    /// bound, and receiving a command never cancels the in-flight connect (its
    /// future stays pinned across iterations).
    async fn run_connect(&mut self, url: String) {
        let mut buffered: Vec<HandleCommand> = Vec::new();
        let mut control_open = !self.control_closed;
        let mut opened: Option<C::Conn> = None;
        let mut failed = false;
        {
            let Self {
                connector,
                clock,
                wakeup,
                control_rx,
                ..
            } = &mut *self;
            let connect = connector.connect(&url).fuse();
            let timer = sleep_until(clock, *wakeup).fuse();
            pin_mut!(connect, timer);
            loop {
                // Copy the flag into the control future so mutating `control_open`
                // in the arm below does not clash with the future's borrow.
                let co = control_open;
                let done = {
                    let control = async {
                        if co {
                            control_rx.next().await
                        } else {
                            future::pending::<Option<HandleCommand>>().await
                        }
                    }
                    .fuse();
                    pin_mut!(control);
                    select_biased! {
                        result = connect => {
                            match result {
                                Ok(conn) => opened = Some(conn),
                                Err(err) => {
                                    // Retryable (backoff), but log why so a
                                    // persistent connect-loop (bad DNS, TLS,
                                    // refused) is diagnosable.
                                    tracing::debug!(error = %err, "surface driver: connect attempt failed");
                                    failed = true;
                                }
                            }
                            true
                        }
                        command = control => {
                            match command {
                                Some(cmd) => buffered.push(cmd),
                                // The handle dropped: stop selecting the closed
                                // channel so the loop does not spin on a ready None.
                                None => control_open = false,
                            }
                            false
                        }
                        () = timer => true,
                    }
                };
                if done {
                    break;
                }
            }
        }
        if !control_open {
            self.control_closed = true;
        }
        let now = self.clock.now();
        let effects = if let Some(conn) = opened {
            self.conn = Some(conn);
            self.core.on_input(Input::Opened, now)
        } else if failed {
            self.core.on_input(Input::ConnectFailed, now)
        } else {
            self.core.on_input(Input::Tick, now)
        };
        self.execute(effects).await;
        // Apply commands that arrived during the connect, in order, now that the
        // race resolved and `self` is free. Pre-`Welcome` the core parks them.
        for command in std::mem::take(&mut buffered) {
            // A close buffered during the connect terminates the driver once its
            // teardown effects are executed (same as the live-select path).
            if matches!(command, HandleCommand::Close) {
                self.terminal = true;
            }
            let input = self.register_and_command(command);
            let now = self.clock.now();
            let effects = self.core.on_input(input, now);
            self.execute(effects).await;
        }
    }

    /// Wait on the live transport, the handle's control channel, and the core's
    /// armed timer, feeding whichever fires to the core and executing its
    /// effects.
    async fn run_select(&mut self) {
        let outcome = {
            let Self {
                core,
                conn,
                clock,
                wakeup,
                retry_wakeup,
                control_rx,
                control_closed,
                publish_rx,
                publish_closed,
                alert_rx,
                alert_closed,
                telemetry_rx,
                telemetry_closed,
                ..
            } = &mut *self;
            let transport = async {
                match conn.as_mut() {
                    Some(conn) => conn.next_event().await,
                    // No transport (backoff): only the timer or a command can fire.
                    None => future::pending::<TransportEvent>().await,
                }
            }
            .fuse();
            let control = async {
                if *control_closed {
                    future::pending::<Option<HandleCommand>>().await
                } else {
                    control_rx.next().await
                }
            }
            .fuse();
            let publish = async {
                if *publish_closed {
                    future::pending::<Option<PublishCommand>>().await
                } else {
                    publish_rx.next().await
                }
            }
            .fuse();
            let alert = async {
                if *alert_closed {
                    future::pending::<Option<AlertCommand>>().await
                } else {
                    alert_rx.next().await
                }
            }
            .fuse();
            let telemetry = async {
                if *telemetry_closed {
                    future::pending::<Option<TelemetryCommand>>().await
                } else {
                    telemetry_rx.next().await
                }
            }
            .fuse();
            let timer = sleep_until(clock, *wakeup).fuse();
            let retry = sleep_until(clock, *retry_wakeup).fuse();
            // Ready when an instance is dispatchable, pending forever otherwise.
            // This is what makes `drain_activations`' bounded pass safe: whatever
            // the pass left ready comes back here rather than waiting on an
            // unrelated frame or tick to carry it.
            //
            // The `yield_now` is not a detail. A component in a publish cycle
            // keeps this arm ready forever, and an arm that answers ready on its
            // first poll means this select — and so the driver's whole future —
            // never once returns `Pending`. The executor would never get control
            // back, and the executor is the only thing that can hand the socket a
            // frame to make the transport arm ready in the first place: on wasm it
            // is the JS event loop, which the WebSocket callbacks run on. Biasing
            // the transport above this arm buys nothing if the frame can never
            // arrive. So the arm yields once per turn — that gives the executor
            // the page back, and the poll that follows finds the transport ready.
            let activations = async {
                if core.has_ready_activation() {
                    yield_now().await;
                } else {
                    future::pending::<()>().await
                }
            }
            .fuse();
            pin_mut!(
                transport,
                control,
                publish,
                alert,
                telemetry,
                timer,
                retry,
                activations
            );
            // Control is biased ahead of the publish and best-effort alert /
            // telemetry channels so neither a publish backlog nor an alert or
            // telemetry flood starves attach/detach; the best-effort planes are
            // next. Activations are dead last, and that is the whole point: a
            // component in a publish cycle keeps this arm permanently ready, so
            // anything above it must win every turn or the cycle starves the
            // page. Below the timer too — a wake the core armed is a promise to
            // something, and a spinning component is a promise to nobody.
            select_biased! {
                event = transport => SelectOutcome::Transport(event),
                command = control => SelectOutcome::Control(command),
                command = publish => SelectOutcome::Publish(command),
                command = alert => SelectOutcome::Alert(command),
                command = telemetry => SelectOutcome::Telemetry(command),
                () = timer => SelectOutcome::Tick,
                () = retry => SelectOutcome::RetryTick,
                () = activations => SelectOutcome::Activations,
            }
        };
        let now = self.clock.now();
        let input = match outcome {
            SelectOutcome::Transport(TransportEvent::Text(text)) => Input::TextFrame(text),
            SelectOutcome::Transport(TransportEvent::Binary(_)) => Input::BinaryFrame,
            SelectOutcome::Transport(TransportEvent::Closed { code, reason }) => {
                // The peer closed; drop the transport before the core reacts so
                // no stale connection is reused. The close code and reason carry
                // through so the core can recognize a stale-build close (3001).
                // Log so a server-initiated error close (e.g. 1011 + diagnostic
                // reason) is not silently swallowed by the backoff response.
                tracing::debug!(?code, reason = %reason, "surface driver: transport closed");
                self.conn = None;
                self.mark_gate_disconnected();
                Input::Disconnected { code, reason }
            }
            SelectOutcome::Transport(TransportEvent::Failed(description)) => {
                // A transport-level failure carries no close code. Retryable, but
                // log the description so a persistent failure loop is diagnosable.
                tracing::debug!(%description, "surface driver: transport failed");
                self.conn = None;
                self.mark_gate_disconnected();
                Input::Disconnected {
                    code: None,
                    reason: String::new(),
                }
            }
            SelectOutcome::Control(Some(command)) => {
                // An orderly close makes the driver terminal: the CloseTransport
                // effect the core returns is executed by the common tail below,
                // then the run loop exits (no reconnect). The transport is closed
                // before the loop's exit path, so its final close is a no-op.
                if matches!(command, HandleCommand::Close) {
                    self.terminal = true;
                }
                self.register_and_command(command)
            }
            SelectOutcome::Control(None) => {
                // The handle and all its clones dropped: no more commands. The
                // driver lives on for the connection and the EventStream; stop
                // selecting this arm and continue.
                self.control_closed = true;
                return;
            }
            SelectOutcome::Publish(Some(command)) => Input::Command(Command::Publish {
                correlation: command.correlation,
                instance: command.instance,
                port: command.port,
                body: command.body,
                subject_instance: command.subject_instance,
                urgency: command.urgency,
                stamp: new_stamp(),
            }),
            SelectOutcome::Publish(None) => {
                // The handle dropped: stop selecting the closed publish channel.
                // Not terminal, for the same reason as the control channel.
                self.publish_closed = true;
                return;
            }
            SelectOutcome::Alert(Some(command)) => Input::Command(Command::Alert {
                severity: command.severity,
                title: command.title,
                body: command.body,
            }),
            SelectOutcome::Alert(None) => {
                // The handle dropped: stop selecting the closed alert channel.
                // Not terminal, for the same reason as the control channel.
                self.alert_closed = true;
                return;
            }
            SelectOutcome::Telemetry(Some(command)) => Input::Command(match command {
                TelemetryCommand::Geometry {
                    width,
                    height,
                    device_pixel_ratio,
                } => Command::SendGeometry {
                    width,
                    height,
                    device_pixel_ratio,
                },
                TelemetryCommand::Status {
                    instances,
                    uptime_secs,
                    counters,
                } => Command::SendStatus {
                    instances,
                    uptime_secs,
                    counters,
                },
            }),
            SelectOutcome::Telemetry(None) => {
                // The handle dropped: stop selecting the closed telemetry channel.
                // Not terminal, for the same reason as the control channel.
                self.telemetry_closed = true;
                return;
            }
            SelectOutcome::Tick => Input::Tick,
            SelectOutcome::RetryTick => Input::RetryTick,
            // No input to feed: the core already holds the readiness and the
            // activation, put there by whatever turn delivered the message. This
            // arm exists only so that work the previous turn's bounded pass left
            // ready is picked up without waiting on an unrelated frame or tick.
            SelectOutcome::Activations => {
                self.drain_activations().await;
                return;
            }
        };
        let effects = self.core.on_input(input, now);
        self.execute(effects).await;
        self.drain_activations().await;
    }

    /// Invoke the activations the core has ready — one pass over the ready set,
    /// not a drain to exhaustion.
    ///
    /// The dispatch point, run after the input's effects are executed so
    /// everything that input delivered is already in its pending queues — that is
    /// what makes a turn's deliveries coalesce into one activation rather than
    /// N.
    ///
    /// Invocation is synchronous: wasm is single-threaded, and the flush rule
    /// needs a return value, not a future. So the loop cannot be re-entered
    /// mid-entry, and `in_flight` cannot be observed by anything but the core
    /// itself — the serialization is structural on both sides.
    ///
    /// **The pass is bounded, and that bound is load-bearing.** An ok flush
    /// carrying a `local:` entry routes synchronously, which pushes the envelope
    /// into the pending queue of every registered instance bound to that channel
    /// — the publisher itself included. That instance is ready again before this
    /// function's next loop check, and the input grant is deliberately 1:1
    /// solvent, so a component that republishes what it consumes never runs out
    /// of budget to do it with. Draining "until the core has none" would then
    /// never return: the driver task would stop reading the socket, stop firing
    /// timers, and stop activating every other instance — one buggy component
    /// hanging the page, which is precisely what the kernel's containment story
    /// is supposed to bound.
    ///
    /// So a turn gets one activation per registered instance (the core's rotating
    /// pick makes that a fair pass), and anything still ready is picked up by the
    /// select loop's `activations` arm, which sits below the transport, the
    /// commands, and the timer in the bias order. A publish cycle is therefore a
    /// livelock the page survives — frames still arrive, timers still fire,
    /// siblings still run — rather than a hang it does not.
    async fn drain_activations(&mut self) {
        let mut budget = self.core.registered_count();
        while budget > 0
            && let Some(ready) = self.core.take_ready_activation()
        {
            budget -= 1;
            let ReadyActivation {
                instance,
                activation,
                buffer,
                effects,
            } = ready;
            // Loud-rung effects enacted at window assembly (an `alarm` binding's
            // alert/toast, a `fatal` binding's kill) run before the entry does.
            // A `fatal` overflow killed the instance during assembly: its buffer
            // is discarded and the entry is not invoked — there is nothing left to
            // deliver to.
            self.execute(effects).await;
            if self.core.is_failed(&instance) {
                continue;
            }
            let (outcome, buffer) = match self.entries.get(&instance) {
                #[cfg(not(target_arch = "wasm32"))]
                Some(entry) => invoke(entry, &activation, buffer),
                #[cfg(target_arch = "wasm32")]
                Some(entry) => invoke(entry, &activation, &self.in_flight, &instance, buffer),
                // The instance deregistered between assembly and here. It cannot
                // happen — assembly and invocation are one synchronous stretch —
                // but the core owns `in_flight` and must be told, or the instance
                // never activates again. Reported as a trap: an activation whose
                // entry vanished did not return ok, and treating it as an err
                // would leave a phantom instance being delivered forever.
                None => (
                    ActivationOutcome::Trap(
                        "activation entry deregistered between assembly and invocation".to_string(),
                    ),
                    buffer,
                ),
            };
            // One stamp per buffered publish: the core is the router for the
            // `local:` entries of a flush and reads no entropy itself. Minted for
            // every entry, local or not, for the same reason a single publish is
            // stamped unconditionally — only the core resolves locality.
            let stamps = (0..buffer.len()).map(|_| new_stamp()).collect();
            let now = self.clock.now();
            let effects = self.core.on_input(
                Input::ActivationDone {
                    instance,
                    outcome,
                    buffer,
                    stamps,
                },
                now,
            );
            self.execute(effects).await;
        }
    }

    /// Register a command's carried port queue and map it to the core [`Input`].
    /// Shared by the live select loop and the post-connect buffered drain. An
    /// Attach that resolves to an absent binding pushes `BindingRemoved` straight
    /// back to this port, so its queue must be registered before the core reacts.
    fn register_and_command(&mut self, command: HandleCommand) -> Input {
        match command {
            HandleCommand::PublishControl { channel, body } => {
                Input::Command(Command::PublishControl {
                    channel,
                    body,
                    stamp: new_stamp(),
                })
            }
            HandleCommand::RegisterActivation { instance, entry } => {
                // Stored before the core is told, so an activation the core
                // dispatches on the strength of this registration always finds
                // its entry. The core panics on a double registration, so a
                // silent overwrite here is unreachable.
                self.entries.insert(instance.clone(), entry);
                Input::ActivationRegistered { instance }
            }
            HandleCommand::DeregisterActivation { instance } => {
                self.entries.remove(&instance);
                Input::ActivationDeregistered { instance }
            }
            HandleCommand::Close => Input::Command(Command::Close),
        }
    }

    /// Execute the core's effects in order. Effects that feed the core back
    /// (a failed `SendFrame`, a `PublishControl` the core minted) append the
    /// resulting effects to the same queue, so a single call drains to
    /// quiescence.
    async fn execute(&mut self, effects: Vec<Effect>) {
        let mut queue: VecDeque<Effect> = effects.into();
        // Set when a send fails partway through this batch: the transport is gone,
        // so any later `SendFrame` still queued from the same batch was computed
        // against the now-dead connection and is skipped rather than panicked on.
        let mut transport_lost = false;
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::Connect { url } => self.pending_connect = Some(url),
                Effect::CloseTransport => {
                    if let Some(mut conn) = self.conn.take() {
                        conn.close().await;
                    }
                    // A core-initiated teardown (handshake/liveness timeout, or a
                    // fatal frame): the connection is no longer Active, so a
                    // publish issued after this must be rejected locally.
                    self.mark_gate_disconnected();
                }
                Effect::SetWakeup(deadline) => self.wakeup = deadline,
                Effect::SetRetryWakeup(deadline) => self.retry_wakeup = deadline,
                Effect::SendFrame(frame) => match self.conn.as_mut() {
                    Some(conn) => {
                        let text = serde_json::to_string(&frame)
                            .expect("surface driver: ClientFrame serializes to JSON");
                        if let Err(err) = conn.send_text(text).await {
                            // The send failed: the connection is gone. Feed the
                            // core a disconnect so it backs off, exactly as a
                            // transport-close event would, and mark the rest of
                            // this batch's sends stale.
                            tracing::debug!(%err, "surface driver: send failed, disconnecting");
                            self.conn = None;
                            self.mark_gate_disconnected();
                            transport_lost = true;
                            let now = self.clock.now();
                            queue.extend(self.core.on_input(
                                Input::Disconnected {
                                    code: None,
                                    reason: String::new(),
                                },
                                now,
                            ));
                        }
                    }
                    // A SendFrame after an in-batch send failure targets the dead
                    // transport — drop it; the core already backed off and will
                    // re-derive the wire set on the next connect.
                    None if transport_lost => {
                        tracing::debug!(
                            "surface driver: dropping stale SendFrame after mid-batch transport loss"
                        );
                    }
                    // Otherwise the core emitted SendFrame with no live transport
                    // (a core/driver contract break, not peer input) — fail fast.
                    None => panic!("surface driver: SendFrame with no live transport"),
                },
                Effect::EmitEvent(event) => self.emit(event),
                Effect::PublishControl { channel, body } => {
                    // The core decided to toast; it cannot mint the envelope
                    // (no clock, no entropy), so it says so and the stamp is read
                    // here — the same edge, and the same `new_stamp`, as every
                    // other envelope the router mints. Feeding the core back
                    // inside `execute` is the established shape: the resulting
                    // fan-out effects join this batch and it drains to
                    // quiescence.
                    let now = self.clock.now();
                    queue.extend(self.core.on_input(
                        Input::Command(Command::PublishControl {
                            channel,
                            body,
                            stamp: new_stamp(),
                        }),
                        now,
                    ));
                }
            }
        }
    }

    /// Emit one control-plane event to the EventStream. A full channel is a
    /// kernel-not-draining bug (control traffic is low-rate by construction) and
    /// panics; a dropped receiver means the kernel is gone, so the loop winds
    /// down. `Fatal` and `ReloadRequired` are terminal regardless — after either,
    /// the pre-terminal loop hands off to the terminal drain loop.
    fn emit(&mut self, event: Event) {
        // The Connected event carries this connection's outputs table and body
        // cap: seed the handle's publish gate from it so a publish is validated
        // against the current bindings. Teardown-driven not-Active transitions are
        // handled at the conn=None / CloseTransport sites, not here, so the gate
        // resets even before a peer close's Disconnected event is processed.
        if let Event::Connected {
            bindings,
            max_body_bytes,
            error_report_floor,
            ..
        } = &event
        {
            // Seed the gate only if the connection that produced this Connected is
            // still live. A mid-batch send failure earlier in this same effect
            // batch can have already torn the transport down (`conn` cleared, gate
            // marked disconnected) while the batch's trailing Connected is still
            // queued; re-marking the gate connected here would let publishes slip
            // past it onto a dead connection and be answered, per publish, with an
            // async NotConnected result — a component-rate flood the EventStream is
            // not built to absorb.
            if self.conn.is_some() {
                self.gate
                    .lock()
                    .expect("surface driver: publish gate mutex poisoned")
                    .on_welcome(bindings, *max_body_bytes, *error_report_floor);
            }
        }
        let terminal = matches!(event, Event::Fatal { .. } | Event::ReloadRequired { .. });
        match self.events_tx.try_send(event) {
            Ok(()) => {}
            Err(err) if err.is_full() => {
                panic!("surface driver: EventStream overflow (kernel not draining)")
            }
            Err(_) => self.terminal = true,
        }
        if terminal {
            self.terminal = true;
        }
    }

    /// Mark the handle's publish gate no longer `Active`. Called at every
    /// connection teardown — peer close/failure, a core-initiated `CloseTransport`,
    /// and a mid-batch send failure — so a publish after the connection drops is
    /// rejected locally rather than shipped to a dead transport. Idempotent.
    fn mark_gate_disconnected(&self) {
        self.gate
            .lock()
            .expect("surface driver: publish gate mutex poisoned")
            .on_disconnected();
    }
}

/// Which command channel fired in the post-terminal drain select.
enum TerminalOutcome {
    Control(Option<HandleCommand>),
    Publish(Option<PublishCommand>),
    Alert(Option<AlertCommand>),
    Telemetry(Option<TelemetryCommand>),
}

/// Which arm of the live-connection select fired.
enum SelectOutcome {
    Transport(TransportEvent),
    Control(Option<HandleCommand>),
    Publish(Option<PublishCommand>),
    Alert(Option<AlertCommand>),
    Telemetry(Option<TelemetryCommand>),
    Tick,
    /// The outbox-retry deadline fired.
    RetryTick,
    /// An instance is dispatchable and nothing above it in the bias order had
    /// work. Carries nothing: the core holds the readiness and the activation.
    Activations,
}

/// Hand the executor control once, then resolve.
///
/// `tokio::task::yield_now`'s job, written out because this loop takes no tokio
/// dependency — it runs under `spawn_local` in the browser, where the executor is
/// the JS event loop and yielding to it is the only way anything else on the page
/// (a WebSocket callback, a timer, a paint) ever runs.
async fn yield_now() {
    let mut yielded = false;
    future::poll_fn(move |cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            // Reschedule before parking: this is a yield, not a wait. Nothing
            // else will ever wake us.
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

/// Sleep until the core's armed deadline (relative to the monotonic clock), or
/// forever when disarmed. Recomputed each loop iteration, so a re-armed deadline
/// takes effect on the next pass.
async fn sleep_until(clock: &Clock, wakeup: Option<crate::Millis>) {
    match wakeup {
        Some(deadline) => {
            let delay = deadline.0.saturating_sub(clock.now().0);
            timer::sleep(Duration::from_millis(delay)).await;
        }
        None => future::pending::<()>().await,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use brenn_surface_proto::{
        AlertSeverity, Binding, ClientFrame, InstanceReport, InstanceState, LogLevel,
        OutputBinding, StatusCounters, Urgency,
    };

    use crate::test_support::cfg;
    use futures_util::StreamExt;

    use crate::transport::TransportError;

    /// The invocation boundary classifies what an entry did *and* recovers what
    /// it said. A panic's message is the operator's only account of a trap — the
    /// component cannot return one — so losing it here loses it forever.
    #[test]
    fn invoke_recovers_the_message_from_every_outcome() {
        let activation = Activation { ports: Vec::new() };
        let buffer = || PublishBuffer::new(Default::default(), Default::default(), 1024);

        let ok: ActivationEntry = Box::new(|_, _| Ok(()));
        assert_eq!(invoke(&ok, &activation, buffer()).0, ActivationOutcome::Ok);

        let returns_err: ActivationEntry = Box::new(|_, _| {
            Err(brenn_surface_contract::ActivationError {
                message: "component said no".into(),
            })
        });
        assert!(matches!(
            invoke(&returns_err, &activation, buffer()).0,
            ActivationOutcome::Err(e) if e.message == "component said no"
        ));

        // A formatted panic — a `String` payload. The hook is silenced: these
        // panics are the subject of the test, not a failure of it.
        let prior = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));

        let formatted: ActivationEntry = Box::new(|_, _| panic!("row {} is bad", 42));
        assert!(matches!(
            invoke(&formatted, &activation, buffer()).0,
            ActivationOutcome::Trap(m) if m == "row 42 is bad"
        ));

        // A literal panic — a `&'static str` payload, the other arm.
        let literal: ActivationEntry = Box::new(|_, _| panic!("flat out broken"));
        assert!(matches!(
            invoke(&literal, &activation, buffer()).0,
            ActivationOutcome::Trap(m) if m == "flat out broken"
        ));

        // `panic_any` with no text to recover: named as such, never guessed at.
        let opaque: ActivationEntry = Box::new(|_, _| std::panic::panic_any(7u8));
        assert!(matches!(
            invoke(&opaque, &activation, buffer()).0,
            ActivationOutcome::Trap(m) if m.contains("non-string payload")
        ));

        std::panic::set_hook(prior);
    }

    // ── config + frame helpers ────────────────────────────────────────────

    /// The queue depth every binding in these tests carries. The overflow test
    /// below deliberately delivers one more than this.
    const TEST_PUSH_DEPTH: u64 = 8;
    /// No context window: these tests are about the driver's frame plumbing,
    /// not window assembly.
    const TEST_RETAIN_DEPTH: u64 = 0;

    fn welcome_frame() -> String {
        crate::test_support::welcome_frame(
            vec![Binding {
                channel: "ephemeral:demo".into(),
                instance: "protobar".into(),
                port: "messages".into(),
                push_depth: TEST_PUSH_DEPTH,
                retain_depth: TEST_RETAIN_DEPTH,
                noise: brenn_surface_proto::NoiseLevel::Silent,
            }],
            vec![],
        )
    }

    /// The standard `Welcome` with the error-report floor advertised at `warn`,
    /// so `handle.report` publishes warn/error reports to the reserved
    /// `#brenn`/`error-reports` port.
    fn welcome_frame_reports() -> String {
        crate::test_support::welcome_frame_reports(vec![Binding {
            channel: "ephemeral:demo".into(),
            instance: "protobar".into(),
            port: "messages".into(),
            push_depth: TEST_PUSH_DEPTH,
            retain_depth: TEST_RETAIN_DEPTH,
            noise: brenn_surface_proto::NoiseLevel::Silent,
        }])
    }

    /// A `Welcome` that grants the alert plane (`alert_granted: true`); the
    /// shared `welcome_frame` fixture is ungranted, so the core drops alerts on
    /// it. Used by the alert-send test, which needs a granted surface.
    fn welcome_frame_alert_granted() -> String {
        brenn_surface_test_fixtures::welcome_frame(brenn_surface_test_fixtures::WelcomeParams {
            subscriptions: vec![Binding {
                channel: "ephemeral:demo".into(),
                instance: "protobar".into(),
                port: "messages".into(),
                push_depth: TEST_PUSH_DEPTH,
                retain_depth: TEST_RETAIN_DEPTH,
                noise: brenn_surface_proto::NoiseLevel::Silent,
            }],
            components: vec!["protobar"],
            ..Default::default()
        })
    }

    /// A Welcome binding two ephemeral channels to the same instance, so a
    /// reconnect resubscribes both in one batch (two `SendFrame`s).
    fn welcome_two() -> String {
        crate::test_support::welcome_frame(
            vec![
                Binding {
                    channel: "ephemeral:demo".into(),
                    instance: "protobar".into(),
                    port: "messages".into(),
                    push_depth: TEST_PUSH_DEPTH,
                    retain_depth: TEST_RETAIN_DEPTH,
                    noise: brenn_surface_proto::NoiseLevel::Silent,
                },
                Binding {
                    channel: "ephemeral:demo2".into(),
                    instance: "protobar".into(),
                    port: "other".into(),
                    push_depth: TEST_PUSH_DEPTH,
                    retain_depth: TEST_RETAIN_DEPTH,
                    noise: brenn_surface_proto::NoiseLevel::Silent,
                },
            ],
            vec![],
        )
    }

    /// A Welcome binding one ephemeral output `(protobar, out)`, so a publish to
    /// that pair passes the gate and the core's authoritative check.
    fn welcome_with_output() -> String {
        crate::test_support::welcome_frame(
            vec![],
            vec![OutputBinding {
                channel: "ephemeral:demo".into(),
                instance: "protobar".into(),
                port: "out".into(),
                urgency: Urgency::Normal,
                fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            }],
        )
    }

    /// A Welcome binding both a subscription `(protobar, messages)` →
    /// `ephemeral:demo` and an output `(protobar, out)` → `ephemeral:demo`, so a
    /// test can exercise attach (control channel) and publish (publish channel)
    /// on one connection.
    fn welcome_sub_and_output() -> String {
        crate::test_support::welcome_frame(
            vec![Binding {
                channel: "ephemeral:demo".into(),
                instance: "protobar".into(),
                port: "messages".into(),
                push_depth: TEST_PUSH_DEPTH,
                retain_depth: TEST_RETAIN_DEPTH,
                noise: brenn_surface_proto::NoiseLevel::Silent,
            }],
            vec![OutputBinding {
                channel: "ephemeral:demo".into(),
                instance: "protobar".into(),
                port: "out".into(),
                urgency: Urgency::Normal,
                fill_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
                capacity_mt: brenn_budget::MILLITOKENS_PER_PUBLISH,
            }],
        )
    }

    /// An `Ok` `PublishResult` for `correlation`.
    fn publish_result_ok(correlation: u64) -> String {
        brenn_surface_test_fixtures::publish_result_ok(correlation)
    }

    /// Count `Subscribe` frames seen so far.
    fn subscribe_count(controls: &Controls) -> usize {
        controls
            .sent()
            .iter()
            .filter(|frame| {
                matches!(
                    serde_json::from_str::<ClientFrame>(frame),
                    Ok(ClientFrame::Subscribe { .. })
                )
            })
            .count()
    }

    // ── mock transport ────────────────────────────────────────────────────

    enum Plan {
        Fail,
        Succeed {
            incoming: mpsc::UnboundedReceiver<TransportEvent>,
            closed: Arc<AtomicBool>,
            fail_sends: Arc<AtomicUsize>,
        },
        /// Connect resolves only once `release` fires, modelling an in-flight
        /// (stalled) handshake.
        Stall {
            release: futures_channel::oneshot::Receiver<()>,
            incoming: mpsc::UnboundedReceiver<TransportEvent>,
            closed: Arc<AtomicBool>,
        },
    }

    /// Scripts connect outcomes and captures what the driver sent and closed.
    #[derive(Clone)]
    struct Controls {
        plans: Arc<Mutex<VecDeque<Plan>>>,
        sent: Arc<Mutex<Vec<String>>>,
        connect_count: Arc<AtomicUsize>,
    }

    impl Controls {
        fn new() -> Self {
            Self {
                plans: Arc::new(Mutex::new(VecDeque::new())),
                sent: Arc::new(Mutex::new(Vec::new())),
                connect_count: Arc::new(AtomicUsize::new(0)),
            }
        }

        /// Queue a connect that succeeds; returns the sender for pushing
        /// transport events into that connection and its close flag.
        fn succeed(&self) -> (mpsc::UnboundedSender<TransportEvent>, Arc<AtomicBool>) {
            self.succeed_failing_sends(0)
        }

        /// Queue a connect that succeeds but whose first `n` `send_text` calls
        /// fail (modelling a transport that drops mid-batch during a send).
        fn succeed_failing_sends(
            &self,
            n: usize,
        ) -> (mpsc::UnboundedSender<TransportEvent>, Arc<AtomicBool>) {
            let (tx, rx) = mpsc::unbounded();
            let closed = Arc::new(AtomicBool::new(false));
            self.plans.lock().unwrap().push_back(Plan::Succeed {
                incoming: rx,
                closed: closed.clone(),
                fail_sends: Arc::new(AtomicUsize::new(n)),
            });
            (tx, closed)
        }

        /// Queue a connect that stalls until the returned oneshot is fired, then
        /// succeeds. Returns the release trigger, the event sender, and the close
        /// flag.
        fn stall_then_succeed(
            &self,
        ) -> (
            futures_channel::oneshot::Sender<()>,
            mpsc::UnboundedSender<TransportEvent>,
            Arc<AtomicBool>,
        ) {
            let (release_tx, release_rx) = futures_channel::oneshot::channel();
            let (tx, rx) = mpsc::unbounded();
            let closed = Arc::new(AtomicBool::new(false));
            self.plans.lock().unwrap().push_back(Plan::Stall {
                release: release_rx,
                incoming: rx,
                closed: closed.clone(),
            });
            (release_tx, tx, closed)
        }

        /// Queue a connect that fails (a normal retryable outcome).
        fn fail(&self) {
            self.plans.lock().unwrap().push_back(Plan::Fail);
        }

        fn connector(&self) -> MockConnector {
            MockConnector {
                plans: self.plans.clone(),
                sent: self.sent.clone(),
                connect_count: self.connect_count.clone(),
            }
        }

        fn connect_count(&self) -> usize {
            self.connect_count.load(Ordering::SeqCst)
        }

        fn sent(&self) -> Vec<String> {
            self.sent.lock().unwrap().clone()
        }
    }

    struct MockConnector {
        plans: Arc<Mutex<VecDeque<Plan>>>,
        sent: Arc<Mutex<Vec<String>>>,
        connect_count: Arc<AtomicUsize>,
    }

    impl TransportConnector for MockConnector {
        type Conn = MockConnection;

        async fn connect(&mut self, _url: &str) -> Result<MockConnection, TransportError> {
            self.connect_count.fetch_add(1, Ordering::SeqCst);
            let plan = self.plans.lock().unwrap().pop_front();
            match plan {
                Some(Plan::Succeed {
                    incoming,
                    closed,
                    fail_sends,
                }) => Ok(MockConnection {
                    incoming,
                    sent: self.sent.clone(),
                    closed,
                    fail_sends,
                }),
                Some(Plan::Stall {
                    release,
                    incoming,
                    closed,
                }) => {
                    // Hold the connect open until the test releases it.
                    let _ = release.await;
                    Ok(MockConnection {
                        incoming,
                        sent: self.sent.clone(),
                        closed,
                        fail_sends: Arc::new(AtomicUsize::new(0)),
                    })
                }
                // A scripted failure, or the script ran dry — either way a
                // retryable connect error, never a panic.
                Some(Plan::Fail) | None => Err(TransportError::new("mock connect refused")),
            }
        }
    }

    struct MockConnection {
        incoming: mpsc::UnboundedReceiver<TransportEvent>,
        sent: Arc<Mutex<Vec<String>>>,
        closed: Arc<AtomicBool>,
        fail_sends: Arc<AtomicUsize>,
    }

    impl TransportConnection for MockConnection {
        async fn send_text(&mut self, text: String) -> Result<(), TransportError> {
            if self.fail_sends.load(Ordering::SeqCst) > 0 {
                self.fail_sends.fetch_sub(1, Ordering::SeqCst);
                return Err(TransportError::new("mock send failed"));
            }
            self.sent.lock().unwrap().push(text);
            Ok(())
        }

        async fn next_event(&mut self) -> TransportEvent {
            match self.incoming.next().await {
                Some(event) => event,
                // The test dropped the sender: model it as a peer close.
                None => TransportEvent::Closed {
                    code: None,
                    reason: String::new(),
                },
            }
        }

        async fn close(&mut self) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    /// Spawn a driver on the current-thread runtime; returns its EventStream
    /// receiver and the task handle. These connection-lifecycle tests issue no
    /// commands, so the control sender is dropped — the driver's control arm goes
    /// idle without affecting its lifetime.
    fn spawn(controls: &Controls) -> (mpsc::Receiver<Event>, tokio::task::JoinHandle<()>) {
        let (events_tx, events_rx) = mpsc::channel(256);
        let (_control_tx, control_rx) = mpsc::channel(4);
        let (_publish_tx, publish_rx) = mpsc::channel(4);
        let (_alert_tx, alert_rx) = mpsc::channel(4);
        let (_telemetry_tx, telemetry_rx) = mpsc::channel(4);
        let gate = Arc::new(Mutex::new(PublishGate::default()));
        let driver = Driver::new(
            cfg(),
            controls.connector(),
            events_tx,
            DriverChannels {
                control_rx,
                publish_rx,
                alert_rx,
                telemetry_rx,
            },
            gate,
        );
        let handle = tokio::spawn(driver.run());
        (events_rx, handle)
    }

    fn client_cfg() -> crate::ClientConfig {
        crate::ClientConfig {
            url: "wss://host/surface/deskbar/ws".into(),
            build_id: "buildxyz".into(),
            initial_backoff: Duration::from_secs(3),
            max_backoff: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(15),
            liveness_multiplier: 3,
        }
    }

    /// Await the next event, failing (rather than hanging) if none arrives; the
    /// bound is virtual time, so it costs nothing under `start_paused`.
    async fn next_event<S: futures_util::Stream<Item = Event> + Unpin>(events: &mut S) -> Event {
        tokio::time::timeout(Duration::from_secs(3_600), events.next())
            .await
            .expect("an event within the (virtual) bound")
            .expect("the EventStream is not closed")
    }

    /// Poll until `cond` holds, advancing virtual time between checks; fails
    /// rather than hanging if it never does.
    async fn wait_until(mut cond: impl FnMut() -> bool) {
        for _ in 0..10_000 {
            if cond() {
                return;
            }
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        panic!("condition never held within the bound");
    }

    // ── tests ─────────────────────────────────────────────────────────────

    #[tokio::test(start_paused = true)]
    async fn connects_and_emits_connected() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (mut events, handle) = spawn(&controls);

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();

        match next_event(&mut events).await {
            Event::Connected {
                participant_id,
                bindings,
                ..
            } => {
                assert_eq!(participant_id, "surface:deskbar");
                assert_eq!(bindings.subscriptions[0].channel, "ephemeral:demo");
            }
            other => panic!("expected Connected, got {other:?}"),
        }
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn connect_failure_backs_off_then_reconnects() {
        let controls = Controls::new();
        controls.fail(); // attempt 1 fails
        let (server, _closed) = controls.succeed(); // attempt 2 succeeds
        let (mut events, handle) = spawn(&controls);

        // Buffered until the second connect takes this connection's receiver.
        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();

        // The 3s backoff auto-advances under paused time, then the second
        // connect succeeds and the buffered Welcome yields Connected.
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));
        assert_eq!(controls.connect_count(), 2);
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn handshake_timeout_closes_and_reconnects() {
        let controls = Controls::new();
        let (_server1, closed1) = controls.succeed(); // opens, never sends Welcome
        let (_server2, _closed2) = controls.succeed();
        let (_events, handle) = spawn(&controls);

        // The 15s handshake deadline fires: the driver closes the dead
        // connection and, after backoff, reconnects.
        wait_until(|| closed1.load(Ordering::SeqCst) && controls.connect_count() >= 2).await;
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn liveness_timeout_disconnects_and_reconnects() {
        let controls = Controls::new();
        let (server, closed) = controls.succeed();
        let (_server2, _closed2) = controls.succeed();
        let (mut events, handle) = spawn(&controls);

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // No further frames: after 3 × 20s of silence the liveness deadline
        // fires. The driver surfaces the reason, closes, and reconnects.
        match next_event(&mut events).await {
            Event::Disconnected { reason } => {
                assert!(matches!(
                    reason,
                    crate::core::DisconnectReason::LivenessTimeout
                ));
            }
            other => panic!("expected Disconnected, got {other:?}"),
        }
        wait_until(|| closed.load(Ordering::SeqCst) && controls.connect_count() >= 2).await;
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn server_close_reconnects() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (_server2, _closed2) = controls.succeed();
        let (mut events, handle) = spawn(&controls);

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // A clean peer close drops the connection; the core surfaces
        // Disconnected { TransportClosed } so the kernel can show "Reconnecting…",
        // then the driver backs off and reconnects.
        server
            .unbounded_send(TransportEvent::Closed {
                code: Some(1000),
                reason: "bye".into(),
            })
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Disconnected {
                reason: crate::core::DisconnectReason::TransportClosed,
            }
        ));
        wait_until(|| controls.connect_count() >= 2).await;
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_frame_closes_and_stops() {
        let controls = Controls::new();
        let (server, closed) = controls.succeed();
        let (mut events, handle) = spawn(&controls);

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // A second Welcome is a fatal protocol error.
        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        match next_event(&mut events).await {
            Event::Fatal { detail } => assert!(detail.contains("second Welcome"), "{detail}"),
            other => panic!("expected Fatal, got {other:?}"),
        }

        // Terminal: the driver closed the transport and the run future ends on
        // its own, with no reconnect.
        assert!(closed.load(Ordering::SeqCst));
        tokio::time::timeout(Duration::from_secs(3_600), handle)
            .await
            .expect("the driver task ends after Fatal")
            .expect("the driver task did not panic");
        assert_eq!(controls.connect_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_build_close_reloads_and_stops() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (mut events, handle) = spawn(&controls);

        // The server closes pre-Welcome with the stale-build code: this client is
        // older than the build now served. The driver surfaces ReloadRequired,
        // which is terminal — no reconnect.
        server
            .unbounded_send(TransportEvent::Closed {
                code: Some(brenn_surface_proto::STALE_BUILD_CLOSE_CODE),
                reason: "server-build-42".into(),
            })
            .unwrap();
        match next_event(&mut events).await {
            Event::ReloadRequired { server_build } => assert_eq!(server_build, "server-build-42"),
            other => panic!("expected ReloadRequired, got {other:?}"),
        }
        // Terminal: the run future ends on its own with no second connect.
        tokio::time::timeout(Duration::from_secs(3_600), handle)
            .await
            .expect("the driver task ends after ReloadRequired")
            .expect("the driver task did not panic");
        assert_eq!(controls.connect_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_control_publish_after_fatal_does_not_panic() {
        // The escalated regression: post-terminal the driver used to complete and
        // drop its control receiver, so the kernel's own terminal link-state
        // publish_control — issued one event-loop hop after Fatal — hit a closed
        // channel and panicked ("driver is gone"). The driver must now outlive the
        // terminal transition and route the control publish through it.
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // A second Welcome is a fatal protocol error → the core goes terminal.
        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(next_event(&mut events).await, Event::Fatal { .. }));

        // The kernel folds Fatal and publishes the terminal link state through the
        // still-open control channel. Pre-fix this panicked.
        handle.publish_control(
            "local:brenn/link-state",
            r#"{"v":1,"state":"fatal"}"#.into(),
        );

        // Give the driver turns to route it; it must neither panic nor complete.
        for _ in 0..20 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        assert!(
            !task.is_finished(),
            "the driver survives the terminal control publish and idles"
        );

        // It winds down once the kernel (all command senders) is gone.
        drop(handle);
        tokio::time::timeout(Duration::from_secs(3_600), task)
            .await
            .expect("the driver task ends once the handle drops")
            .expect("the driver task did not panic on the terminal control publish");
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_control_publish_after_reload_required_does_not_panic() {
        // Same seam as the Fatal case, on the stale-build reload path: the
        // kernel's `reloading` link-state publish must route rather than panic
        // before the ordered reload replaces the page.
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Closed {
                code: Some(brenn_surface_proto::STALE_BUILD_CLOSE_CODE),
                reason: "server-build-42".into(),
            })
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::ReloadRequired { .. }
        ));

        handle.publish_control(
            "local:brenn/link-state",
            r#"{"v":1,"state":"reloading"}"#.into(),
        );

        for _ in 0..20 {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        assert!(
            !task.is_finished(),
            "the driver survives the terminal control publish and idles"
        );

        drop(handle);
        tokio::time::timeout(Duration::from_secs(3_600), task)
            .await
            .expect("the driver task ends once the handle drops")
            .expect("the driver task did not panic on the terminal control publish");
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_drain_exits_when_event_receiver_dropped() {
        // The drain loop's second exit condition: the EventStream receiver drops
        // (the kernel event loop is gone) even while the handle is still alive.
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));
        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(next_event(&mut events).await, Event::Fatal { .. }));

        // Drop the EventStream receiver: the kernel event loop is gone. A control
        // publish wakes the idle drain loop, which sees the closed EventStream on
        // its next pass and winds down — with the handle still alive.
        drop(events);
        handle.publish_control(
            "local:brenn/link-state",
            r#"{"v":1,"state":"fatal"}"#.into(),
        );

        tokio::time::timeout(Duration::from_secs(3_600), task)
            .await
            .expect("the driver winds down when the EventStream receiver drops")
            .expect("the driver task did not panic");
        drop(handle);
    }

    /// An entry that does nothing, for tests about the plumbing around it rather
    /// than what it does.
    fn noop_entry() -> ActivationEntry {
        Box::new(|_, _| Ok(()))
    }

    #[tokio::test(start_paused = true)]
    async fn registration_while_active_subscribes() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // The instance's binding resolves to ephemeral:demo; being Active, the
        // core subscribes. A registered instance is a subscriber like any other,
        // and nothing else opens its subscriptions. The registration flowed
        // handle → control channel → driver → core.
        handle.register_activation("protobar", noop_entry());
        wait_until(|| {
            controls.sent().iter().any(|frame| {
                matches!(
                    serde_json::from_str::<ClientFrame>(frame),
                    Ok(ClientFrame::Subscribe { channel, .. }) if channel == "ephemeral:demo"
                )
            })
        })
        .await;
        task.abort();
    }

    /// A `fatal` overflow kills the instance during window assembly, and the
    /// driver must honour that for the activation *already in hand*: the entry is
    /// not invoked and the assembled buffer is discarded.
    ///
    /// The core-side tests prove `is_failed` and that no *further* activation is
    /// produced; neither reaches this. Drop the `is_failed` guard in
    /// `drain_activations` and a fatally-overflowed instance still runs its entry
    /// once and flushes — the exact "does the wrong thing" outcome the kill
    /// exists to prevent — with the rest of the suite green.
    #[tokio::test(start_paused = true)]
    async fn a_fatally_overflowed_instance_never_reaches_its_entry() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        let welcome = brenn_surface_test_fixtures::welcome_frame(
            brenn_surface_test_fixtures::WelcomeParams {
                subscriptions: vec![Binding {
                    channel: "ephemeral:demo".into(),
                    instance: "protobar".into(),
                    port: "messages".into(),
                    push_depth: 1,
                    retain_depth: 1,
                    noise: brenn_surface_proto::NoiseLevel::Fatal,
                }],
                components: vec!["protobar"],
                alert_granted: true,
                ..Default::default()
            },
        );

        let activations = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&activations);
        handle.register_activation(
            "protobar",
            Box::new(move |_activation, _buffer| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        );

        server
            .unbounded_send(TransportEvent::Text(welcome))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));
        server
            .unbounded_send(TransportEvent::Text(
                brenn_surface_test_fixtures::subscribe_result_ok("ephemeral:demo", "protobar"),
            ))
            .unwrap();

        // One delivery carrying a server-reported drop: the delta alone trips the
        // fatal rung, so the kill lands on the very activation this frame builds.
        server
            .unbounded_send(TransportEvent::Text(
                brenn_surface_test_fixtures::deliver_frame(
                    "ephemeral:demo",
                    "protobar",
                    "m1",
                    1,
                    brenn_surface_test_fixtures::wire_cursor("c1"),
                    3,
                ),
            ))
            .unwrap();

        // The kill is observable on the event stream; everything else rides the
        // same turn, so once it lands the entry has had its chance.
        loop {
            match next_event(&mut events).await {
                Event::InstanceFailed { instance, reason } => {
                    assert_eq!(instance, "protobar");
                    assert!(reason.contains("fatal"), "{reason}");
                    break;
                }
                _ => continue,
            }
        }
        assert_eq!(
            activations.load(Ordering::SeqCst),
            0,
            "the entry of a fatally-overflowed instance is never invoked"
        );

        drop(handle);
        task.abort();
    }

    /// A component whose entry republishes onto a `local:` channel one of its own
    /// bindings reads must not hang the driver.
    ///
    /// The cycle is real and unbreakable from below: the flush routes through the
    /// router synchronously, which pushes the envelope straight back into the
    /// publisher's own pending queue, and the input grant is 1:1 solvent by
    /// design, so the budget never runs out. Nothing terminates it. What must
    /// hold is that the driver keeps *serving the page* while it spins — the
    /// bounded dispatch pass and the yielding select arm exist for this and
    /// nothing else.
    ///
    /// The proof is that a frame sent after the cycle starts is still read and
    /// acted on. Under a drain-to-exhaustion loop this test hangs forever, which
    /// is exactly what it is here to prevent; it also relies on that read to end
    /// the spin, since the component itself never will.
    #[tokio::test(start_paused = true)]
    async fn a_component_republishing_its_own_input_does_not_hang_the_driver() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        // `protobar` reads local:loop on `in` and writes it on `loop`: a cycle of
        // one. The budget is generous, so nothing below is a bucket running dry
        // standing in for the fix.
        let generous = 1_000_000 * brenn_budget::MILLITOKENS_PER_PUBLISH;
        let welcome = brenn_surface_test_fixtures::welcome_frame(
            brenn_surface_test_fixtures::WelcomeParams {
                subscriptions: vec![Binding {
                    channel: "local:loop".into(),
                    instance: "protobar".into(),
                    port: "in".into(),
                    push_depth: 8,
                    retain_depth: 0,
                    noise: brenn_surface_proto::NoiseLevel::Silent,
                }],
                outputs: vec![OutputBinding {
                    channel: "local:loop".into(),
                    instance: "protobar".into(),
                    port: "loop".into(),
                    urgency: Urgency::Normal,
                    fill_mt: generous,
                    capacity_mt: generous,
                }],
                components: vec!["protobar"],
                local_channels: vec![brenn_surface_proto::LocalChannel {
                    channel: "local:loop".into(),
                    ring_depth: 1,
                }],
                ..Default::default()
            },
        );

        let activations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&activations);
        handle.register_activation(
            "protobar",
            Box::new(move |_activation, buffer| {
                counter.fetch_add(1, Ordering::SeqCst);
                // Feed itself, forever.
                buffer.publish("loop", "again".into()).ok();
                Ok(())
            }),
        );

        server
            .unbounded_send(TransportEvent::Text(welcome.clone()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // Kick the cycle off with one publish from outside it.
        handle
            .publish("protobar", "loop", "kick".into())
            .expect("the local output is bound and the gate is open");
        wait_until(|| activations.load(Ordering::SeqCst) > 4).await;

        // The component is now spinning. A second Welcome is a fatal protocol
        // error — so if this frame is read at all, the driver is still reading the
        // socket while the cycle runs, and it says so by going terminal.
        server
            .unbounded_send(TransportEvent::Text(welcome))
            .unwrap();
        loop {
            // The kick's own PublishResult rides this stream too; it is not what
            // is being asked about.
            match next_event(&mut events).await {
                Event::Fatal { detail } => {
                    assert!(detail.contains("second Welcome"), "{detail}");
                    break;
                }
                Event::PublishResult { .. } => continue,
                other => panic!("expected Fatal, got {other:?}"),
            }
        }
        // Terminal now hands off to the drain loop; dropping the handle drops
        // every command sender, so the driver winds down.
        drop(handle);
        tokio::time::timeout(Duration::from_secs(3_600), task)
            .await
            .expect("the driver task ends: it was never wedged in the cycle")
            .expect("the driver task did not panic");
    }

    /// A refused flush is retried on the driver's own timer — the arm that makes
    /// the core's `SetRetryWakeup` a real wakeup rather than a stated intention.
    ///
    /// The kernel-side half of §4 is only worth anything if the tick arrives with
    /// no other input to carry it: the whole point is a page that is otherwise
    /// idle, whose refused batch must still go out once the server's backstop
    /// refills. So this drives the clock and nothing else.
    #[tokio::test(start_paused = true)]
    async fn a_rate_limited_flush_is_retried_on_the_drivers_timer() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        let generous = 1_000_000 * brenn_budget::MILLITOKENS_PER_PUBLISH;
        let welcome = brenn_surface_test_fixtures::welcome_frame(
            brenn_surface_test_fixtures::WelcomeParams {
                subscriptions: vec![Binding {
                    channel: "local:kick".into(),
                    instance: "protobar".into(),
                    port: "in".into(),
                    push_depth: 8,
                    retain_depth: 0,
                    noise: brenn_surface_proto::NoiseLevel::Silent,
                }],
                outputs: vec![
                    OutputBinding {
                        channel: "ephemeral:sink".into(),
                        instance: "protobar".into(),
                        port: "out".into(),
                        urgency: Urgency::Normal,
                        fill_mt: generous,
                        capacity_mt: generous,
                    },
                    // How the test seeds the one activation, from outside it.
                    OutputBinding {
                        channel: "local:kick".into(),
                        instance: "protobar".into(),
                        port: "kick".into(),
                        urgency: Urgency::Normal,
                        fill_mt: generous,
                        capacity_mt: generous,
                    },
                ],
                components: vec!["protobar"],
                local_channels: vec![brenn_surface_proto::LocalChannel {
                    channel: "local:kick".into(),
                    ring_depth: 1,
                }],
                ..Default::default()
            },
        );
        // One activation, one wire flush, then nothing: the entry must not feed
        // itself, or an activation rather than the timer could carry the retry.
        handle.register_activation(
            "protobar",
            Box::new(move |_activation, buffer| {
                buffer.publish("out", "owed".into()).ok();
                Ok(())
            }),
        );
        server
            .unbounded_send(TransportEvent::Text(welcome))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));
        handle
            .publish("protobar", "kick", "kick".into())
            .expect("the local output is bound and the gate is open");

        let sent_batches = || {
            controls
                .sent()
                .iter()
                .filter_map(|frame| match serde_json::from_str::<ClientFrame>(frame) {
                    Ok(ClientFrame::PublishBatch { correlation, .. }) => Some(correlation),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        wait_until(|| !sent_batches().is_empty()).await;
        let first = sent_batches()[0];

        // Refuse it. Nothing else will ever be fed to this driver.
        server
            .unbounded_send(TransportEvent::Text(
                serde_json::to_string(&brenn_surface_proto::ServerFrame::PublishBatchResult {
                    correlation: first,
                    outcome: brenn_surface_proto::PublishBatchOutcome::RateLimited,
                })
                .unwrap(),
            ))
            .unwrap();

        // Only the clock moves from here. The retried frame is the proof the
        // timer arm fired and carried the head back to the wire.
        wait_until(|| sent_batches().len() > 1).await;
        let retried = controls
            .sent()
            .iter()
            .filter_map(|frame| match serde_json::from_str::<ClientFrame>(frame) {
                Ok(ClientFrame::PublishBatch { publishes, .. }) => Some(publishes),
                _ => None,
            })
            .next_back()
            .expect("a second batch went out");
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].body, "owed", "the same flush, whole");
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn report_while_active_publishes_to_reserved_port() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        // Floor advertised: a warn/error report becomes a reserved-port publish.
        server
            .unbounded_send(TransportEvent::Text(welcome_frame_reports()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // report flows handle → publish channel → driver → core → wire as a
        // `Publish` to `#brenn`/`error-reports` carrying the flat body.
        handle.report(LogLevel::Warn, "component:x", "boom", None);
        wait_until(|| {
            controls
                .sent()
                .iter()
                .any(|frame| match serde_json::from_str::<ClientFrame>(frame) {
                    Ok(ClientFrame::Publish {
                        instance,
                        port,
                        body,
                        ..
                    }) if instance == "#brenn" && port == "error-reports" => {
                        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                        v["source"] == "component:x"
                            && v["message"] == "boom"
                            && v["level"] == "warn"
                    }
                    _ => false,
                })
        })
        .await;
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn report_below_floor_sends_nothing() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_frame_reports()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // Below the `warn` floor: no frame ever reaches the wire.
        handle.report(LogLevel::Info, "component:x", "chatter", None);
        // Advance virtual time so any (erroneous) queued publish would have been
        // sent, then assert nothing was.
        for _ in 0..50 {
            tokio::time::advance(Duration::from_millis(20)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            !controls.sent().iter().any(|frame| matches!(
                serde_json::from_str::<ClientFrame>(frame),
                Ok(ClientFrame::Publish { instance, .. }) if instance == "#brenn"
            )),
            "a below-floor report must not publish, sent: {:?}",
            controls.sent()
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn alert_while_active_sends_an_alert_frame() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_frame_alert_granted()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // alert flows handle → alert channel → driver → core → wire.
        handle.alert(AlertSeverity::Warning, "component panic", "why");
        wait_until(|| {
            controls.sent().iter().any(|frame| {
                matches!(
                    serde_json::from_str::<ClientFrame>(frame),
                    Ok(ClientFrame::Alert { title, body, .. })
                        if title == "component panic" && body == "why"
                )
            })
        })
        .await;
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn send_geometry_while_active_sends_a_geometry_frame() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        // Connected carries the telemetry cadence, and the core sends telemetry
        // frames while Active.
        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        match next_event(&mut events).await {
            Event::Connected {
                surface_description,
                ..
            } => assert_eq!(
                surface_description,
                brenn_surface_proto::SurfaceDescription {
                    status_interval_secs: 60,
                }
            ),
            other => panic!("expected Connected, got {other:?}"),
        }

        // send_geometry flows handle → telemetry channel → driver → core → wire.
        handle.send_geometry(1920, 515, 2.0);
        wait_until(|| {
            controls.sent().iter().any(|frame| {
                matches!(
                    serde_json::from_str::<ClientFrame>(frame),
                    Ok(ClientFrame::Geometry {
                        width,
                        height,
                        device_pixel_ratio,
                    }) if width == 1920 && height == 515 && device_pixel_ratio == 2.0
                )
            })
        })
        .await;
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn send_status_while_active_sends_a_status_frame() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // send_status flows handle → telemetry channel → driver → core → wire.
        handle.send_status(
            vec![InstanceReport {
                instance: "protobar".into(),
                kind: "protobar".into(),
                state: InstanceState::Mounted,
                reason: None,
                ports_attached: 1,
            }],
            42,
            StatusCounters {
                deliveries: 3,
                publishes: 1,
                errors: 0,
                instances: Default::default(),
            },
        );
        wait_until(|| {
            controls.sent().iter().any(|frame| {
                matches!(
                    serde_json::from_str::<ClientFrame>(frame),
                    Ok(ClientFrame::Status {
                        uptime_secs,
                        ref instances,
                        ..
                    }) if uptime_secs == 42 && instances.len() == 1
                )
            })
        })
        .await;
        task.abort();
    }

    /// Count `Publish` frames whose body matches `body`.
    fn publish_count(controls: &Controls, body: &str) -> usize {
        controls
            .sent()
            .iter()
            .filter(|frame| {
                matches!(
                    serde_json::from_str::<ClientFrame>(frame),
                    Ok(ClientFrame::Publish { body: b, .. }) if b == body
                )
            })
            .count()
    }

    #[tokio::test(start_paused = true)]
    async fn publish_before_connected_is_rejected_not_connected() {
        let controls = Controls::new();
        let (_server, _closed) = controls.succeed();
        let (handle, _events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        // No Welcome yet: the gate is default (disconnected), so a publish is
        // rejected locally with no wire frame.
        assert_eq!(
            handle.publish("protobar", "out", "hi".into()),
            Err(crate::PublishReject::NotConnected)
        );
        assert!(
            controls.sent().is_empty(),
            "a rejected publish must send no frame, sent: {:?}",
            controls.sent()
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn publish_while_active_sends_frame_and_result_routes_back() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_with_output()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // The gate is now connected with (protobar, out) bound: the publish passes
        // the gate, flows handle → publish channel → driver → core → wire.
        let correlation = handle
            .publish("protobar", "out", "hello".into())
            .expect("publish accepted");
        wait_until(|| publish_count(&controls, "hello") >= 1).await;

        // The server acks it; the result routes back on the EventStream by
        // correlation.
        server
            .unbounded_send(TransportEvent::Text(publish_result_ok(correlation)))
            .unwrap();
        match next_event(&mut events).await {
            Event::PublishResult {
                correlation: c,
                status,
                ..
            } => {
                assert_eq!(c, correlation);
                assert_eq!(status, crate::core::PublishStatus::Ok);
            }
            other => panic!("expected PublishResult, got {other:?}"),
        }
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn publish_to_unbound_pair_is_rejected_locally() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_with_output()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // Connected, but (protobar, nope) is not a bound output: the gate rejects
        // it synchronously and nothing reaches the wire.
        assert_eq!(
            handle.publish("protobar", "nope", "hi".into()),
            Err(crate::PublishReject::UnboundPort)
        );
        assert!(
            controls.sent().is_empty(),
            "an unbound publish must send no frame, sent: {:?}",
            controls.sent()
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn publish_burst_returns_busy_without_starving_control() {
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_sub_and_output()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // Flood publishes without yielding, so the driver task never drains the
        // publish channel. The gate accepts each (connected + bound), so the
        // channel fills to its bound and further publishes return Busy — never a
        // panic, unlike a full control channel.
        let mut busy_seen = false;
        for _ in 0..(crate::handle::PUBLISH_CHANNEL_CAPACITY + 50) {
            if handle.publish("protobar", "out", "x".into()) == Err(crate::PublishReject::Busy) {
                busy_seen = true;
                break;
            }
        }
        assert!(
            busy_seen,
            "a synchronous publish burst must eventually be Busy"
        );
        // Control is a separate channel: a registration still works despite the
        // publish backlog. Yield so the driver drains and subscribes.
        handle.register_activation("protobar", noop_entry());
        wait_until(|| {
            controls.sent().iter().any(|frame| {
                matches!(
                    serde_json::from_str::<ClientFrame>(frame),
                    Ok(ClientFrame::Subscribe { channel, .. }) if channel == "ephemeral:demo"
                )
            })
        })
        .await;
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn send_failure_mid_batch_disconnects_without_panicking() {
        // A reconnect whose Welcome resubscribes two channels emits two Subscribe
        // frames in one batch. If the first send fails, the driver must feed the
        // core a disconnect and drop the stale second Subscribe — not panic on it.
        let controls = Controls::new();
        let (server1, _c1) = controls.succeed();
        let (_server2, _c2) = controls.succeed_failing_sends(1); // first resubscribe send fails
        let (_server3, _c3) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server1
            .unbounded_send(TransportEvent::Text(welcome_two()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // Register the instance: its two bindings each subscribe on the live
        // connection.
        handle.register_activation("protobar", noop_entry());
        wait_until(|| subscribe_count(&controls) >= 2).await;

        // Drop connection 1. The buffered welcomes on 2 and 3 drive the reconnect:
        // connection 2's first resubscribe send fails (would panic pre-fix on the
        // second queued Subscribe); the driver backs off and reaches connection 3.
        _server2
            .unbounded_send(TransportEvent::Text(welcome_two()))
            .unwrap();
        _server3
            .unbounded_send(TransportEvent::Text(welcome_two()))
            .unwrap();
        server1
            .unbounded_send(TransportEvent::Closed {
                code: Some(1000),
                reason: "bye".into(),
            })
            .unwrap();

        // Connection 3 resubscribes both channels (2 on conn1 + 0 on the failed
        // conn2 + 2 here = 4), proving the driver recovered rather than panicked.
        wait_until(|| controls.connect_count() >= 3 && subscribe_count(&controls) >= 4).await;
        assert!(
            !task.is_finished(),
            "the driver must not have panicked on the mid-batch send failure"
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn commands_during_stalled_connect_do_not_fill_control_channel() {
        // A trickle of registrations during a stalled handshake must be drained
        // (not accumulated to the control channel's panic bound) and applied once
        // the connect resolves. Pre-fix, run_connect did not select the control
        // channel, so > CONTROL_CHANNEL_CAPACITY registrations here would panic.
        let controls = Controls::new();
        let (release, server, _closed) = controls.stall_then_succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        // The driver is parked inside run_connect awaiting the stalled connect.
        // Issue far more registrations than the control channel could buffer,
        // yielding so the driver drains between them.
        for i in 0..200u32 {
            handle.register_activation(&format!("ghost{i}"), noop_entry());
            tokio::task::yield_now().await;
        }

        // Resolve the connect and deliver the Welcome; the client reaches Connected
        // with no panic despite the burst during the stall.
        release.send(()).unwrap();
        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));
        assert!(!task.is_finished(), "the driver must not have panicked");
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn close_shuts_down_and_ends_the_run() {
        let controls = Controls::new();
        let (server, closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // The kernel requests an orderly shutdown: the driver closes the transport
        // and the run future ends on its own, with no reconnect.
        handle.close();
        // The kernel drops the handle after an orderly close; that drops every
        // command sender, ending the terminal drain loop.
        drop(handle);
        tokio::time::timeout(Duration::from_secs(3_600), task)
            .await
            .expect("the driver task ends after close")
            .expect("the driver task did not panic");
        assert!(closed.load(Ordering::SeqCst));
        assert_eq!(controls.connect_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn close_answers_channel_queued_publishes_connection_lost() {
        // A publish the handle accepted (correlation returned) but that is still
        // sitting in the publish channel when Close makes the driver terminal must
        // still get exactly one result. Control is biased ahead of publish, so a
        // Close issued after the publish overtakes it and the loop exits with the
        // publish still queued — the driver must drain and fail it.
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_with_output()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // Accept the publish (gate is connected + bound), then close before the
        // driver drains the publish channel.
        let correlation = handle
            .publish("protobar", "out", "hi".into())
            .expect("publish accepted");
        handle.close();

        match next_event(&mut events).await {
            Event::PublishResult {
                correlation: c,
                status,
                ..
            } => {
                assert_eq!(c, correlation);
                assert_eq!(status, crate::core::PublishStatus::ConnectionLost);
            }
            other => panic!("expected PublishResult ConnectionLost, got {other:?}"),
        }
        // Dropping the handle drops the command senders, ending the drain loop.
        drop(handle);
        tokio::time::timeout(Duration::from_secs(3_600), task)
            .await
            .expect("the driver task ends after close")
            .expect("the driver task did not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn mid_batch_send_failure_leaves_publish_gate_disconnected() {
        // A reconnect whose Welcome resubscribes a survivor emits
        // [Subscribe, Connected]. If the Subscribe send fails, the transport is
        // gone and the gate is marked disconnected — but the batch's trailing
        // Connected still executes. It must NOT re-seed the gate as connected: a
        // publish issued during the dead window has to be rejected locally, not
        // queued to be answered per-publish while disconnected.
        let controls = Controls::new();
        let (server1, _c1) = controls.succeed();
        let (_server2, _c2) = controls.succeed_failing_sends(1); // resubscribe send fails
        let (_server3, _c3) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server1
            .unbounded_send(TransportEvent::Text(welcome_sub_and_output()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // Register the instance so the reconnect has a survivor to resubscribe
        // (its Subscribe leads the reconnect batch).
        handle.register_activation("protobar", noop_entry());
        wait_until(|| subscribe_count(&controls) >= 1).await;

        // Drop conn1; conn2's Welcome resubscribes but its first send fails.
        _server2
            .unbounded_send(TransportEvent::Text(welcome_sub_and_output()))
            .unwrap();
        server1
            .unbounded_send(TransportEvent::Closed {
                code: Some(1000),
                reason: "bye".into(),
            })
            .unwrap();

        // conn1's clean close emits Disconnected { TransportClosed } first (the
        // driver pipes every EmitEvent to the events channel in order).
        assert!(matches!(
            next_event(&mut events).await,
            Event::Disconnected {
                reason: crate::core::DisconnectReason::TransportClosed,
            }
        ));
        // The second Connected still fires (from the now-dead conn2). Once it has,
        // the gate must be disconnected: a publish to the bound output is rejected
        // locally rather than accepted onto a dead connection. (conn2's failed
        // resubscribe send feeds a second Input::Disconnected, emitting another
        // Disconnected after this Connected; it sits harmlessly unconsumed.)
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));
        assert_eq!(
            handle.publish("protobar", "out", "hi".into()),
            Err(crate::PublishReject::NotConnected)
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn report_flood_does_not_crash_or_starve_control() {
        // report's producers are the (untrusted) components. A synchronous flood
        // far past the publish channel's bound must silently drop (the reserved
        // report publish is fire-and-forget, swallowing the `Busy` reject) —
        // never panic like a full control channel — and must not starve control:
        // attach still works afterward.
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_frame_reports()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // The driver never drains between these synchronous calls, so the publish
        // channel fills and the surplus drops — no panic.
        for i in 0..(crate::handle::PUBLISH_CHANNEL_CAPACITY * 4) {
            handle.report(LogLevel::Warn, "component:x", &format!("boom {i}"), None);
        }
        // Control is a separate channel: registration still subscribes.
        handle.register_activation("protobar", noop_entry());
        wait_until(|| subscribe_count(&controls) >= 1).await;
        assert!(
            !task.is_finished(),
            "a report flood must not crash the driver"
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn alert_flood_does_not_crash_or_starve_control() {
        // alert's producers include the (untrusted) components. A synchronous
        // flood far past the alert channel's bound must silently drop — never
        // panic like a full control channel — and must not starve control:
        // attach still works afterward.
        let controls = Controls::new();
        let (server, _closed) = controls.succeed();
        let (handle, mut events, driver) = crate::new(client_cfg(), controls.connector());
        let task = tokio::spawn(driver.run());

        server
            .unbounded_send(TransportEvent::Text(welcome_frame()))
            .unwrap();
        assert!(matches!(
            next_event(&mut events).await,
            Event::Connected { .. }
        ));

        // The driver never drains between these synchronous calls, so the channel
        // fills and the surplus drops — no panic.
        for i in 0..(crate::handle::ALERT_CHANNEL_CAPACITY * 4) {
            handle.alert(AlertSeverity::Warning, &format!("boom {i}"), "why");
        }
        // Control is a separate channel: registration still subscribes.
        handle.register_activation("protobar", noop_entry());
        wait_until(|| subscribe_count(&controls) >= 1).await;
        assert!(
            !task.is_finished(),
            "an alert flood must not crash the driver"
        );
        task.abort();
    }

    #[test]
    #[should_panic(expected = "control command channel full")]
    fn control_channel_full_panics() {
        // Nothing drains the control channel (the driver is never polled), so a
        // large synchronous registration burst fills it and the handle panics.
        let controls = Controls::new();
        let (handle, _events, _driver) = crate::new(client_cfg(), controls.connector());
        for i in 0..1_000u32 {
            handle.register_activation(&format!("comp{i}"), noop_entry());
        }
    }

    #[test]
    #[should_panic(expected = "driver is gone")]
    fn control_channel_closed_panics() {
        // Dropping the driver closes the control receiver; the next command panics.
        let controls = Controls::new();
        let (handle, _events, driver) = crate::new(client_cfg(), controls.connector());
        drop(driver);
        handle.register_activation("comp", noop_entry());
    }

    #[test]
    #[should_panic(expected = "EventStream overflow")]
    fn eventstream_overflow_panics() {
        // A tiny, un-drained event channel: once full, the driver's emit panics
        // (the kernel-not-draining fail-fast contract).
        let controls = Controls::new();
        let (events_tx, _events_rx) = mpsc::channel(1);
        let (_control_tx, control_rx) = mpsc::channel(4);
        let (_publish_tx, publish_rx) = mpsc::channel(4);
        let (_alert_tx, alert_rx) = mpsc::channel(4);
        let (_telemetry_tx, telemetry_rx) = mpsc::channel(4);
        let gate = Arc::new(Mutex::new(PublishGate::default()));
        let mut driver = Driver::new(
            cfg(),
            controls.connector(),
            events_tx,
            DriverChannels {
                control_rx,
                publish_rx,
                alert_rx,
                telemetry_rx,
            },
            gate,
        );
        for _ in 0..100 {
            driver.emit(Event::Disconnected {
                reason: crate::core::DisconnectReason::LivenessTimeout,
            });
        }
    }
}

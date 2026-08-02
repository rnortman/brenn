//! The I/O half of an attachment: the one layer of this crate that opens
//! sockets, writes bytes, and reads clocks and entropy.
//!
//! Everything else here is sans-I/O — [`crate::conn`] answers with effects,
//! [`crate::subs`], [`crate::publish`] and [`crate::router`] answer with frames
//! and timer instructions — and none of it can execute any of that. The
//! [`AttachDriver`] is what does: it owns the connector, the live transport, the
//! monotonic clock, and the three deadlines the layers above arm, and it turns
//! [`ConnEffect`]s into the connects, closes and writes they name.
//!
//! **It does not own the loop.** An embedder has select arms of its own — a
//! command channel, a work queue, whatever it is built around — and their bias
//! against the transport is the embedder's decision, not this crate's. So the
//! driver offers the two halves of a turn separately: [`AttachDriver::wait`]
//! resolves when something happened, and the feed/send/close entry points act on
//! it. That split is also what makes the wait safe to drop: an embedder's
//! `select` may abandon a pending `wait`, and nothing has been consumed yet when
//! it does.
//!
//! The connect attempt is the one exception to "wait, then act", because a
//! connect is itself a long await: [`AttachDriver::connect`] runs the attempt,
//! and an embedder that races it against its own arms must pin it across those
//! turns — dropping it abandons the attempt.

use std::collections::VecDeque;
use std::time::Duration;

use brenn_attach_proto::{ClientFrame, ServerFrame};
use brenn_queue::ReleaseTime;
use futures_util::{FutureExt, future, pin_mut, select_biased};

use uuid::Uuid;

use crate::conn::{ConnConfig, ConnEffect, ConnEvent, ConnInput, ConnState, ConnStep, Connection};
use crate::publish::TimerChange;
use crate::router::{MessageStamp, ReleaseTimer};
use crate::transport::clock::{Clock, epoch_ms, wall_now};
use crate::transport::timer;
use crate::{Millis, TransportConnection, TransportConnector, TransportEvent};

/// What woke the driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IoEvent {
    /// The connection layer's business — a frame, a transport loss, or its armed
    /// deadline. Fed straight back through [`AttachDriver::on_input`]; the driver
    /// has already dropped a transport that ended, but nothing above has been
    /// told yet.
    Conn(ConnInput),
    /// The outbox retry deadline fired: some registrant's head flush is owed
    /// another offer ([`crate::publish::Outboxes::on_retry_tick`]).
    RetryDue,
    /// The release deadline fired: something parked on a confined channel is due
    /// ([`crate::router::LocalRouter::release_due`]).
    ///
    /// Carries the wall clock read at the fire rather than the deadline that
    /// armed it. A timer fires late (a throttled background tab) or early (a
    /// clock step), and what releases is what is due *now*.
    ReleaseDue { now_ms: ReleaseTime },
}

/// What one input, send, or shutdown produced for the embedder.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DriverStep {
    /// Connection events, in order. The embedder acts on each: an `Attached`
    /// resubscribes and reflushes, a `Detached` tears per-connection state down,
    /// a terminal event ends the attachment.
    pub events: Vec<ConnEvent>,
    /// A server frame belonging to a plane above the connection — a
    /// `SubscribeResult`, `Deliver`, publish result, or deferred view. Never a
    /// `Hello`, `Welcome` or `Heartbeat`: those are the connection's own.
    pub routed: Option<ServerFrame>,
}

/// The attachment's I/O owner, generic over the transport connector.
///
/// Holds the [`Connection`] state machine, so every effect it produces is
/// executed at one place and an embedder never sees a `ConnEffect` at all.
pub struct AttachDriver<C: TransportConnector> {
    conn: Connection,
    connector: C,
    /// The live transport, present from a successful connect until the socket
    /// ends or is closed; `None` while connecting, backing off, or terminal.
    transport: Option<C::Conn>,
    clock: Clock,
    /// The connection's armed deadline — handshake, backoff, or liveness,
    /// whichever the state owns. `None` disarms.
    wakeup: Option<Millis>,
    /// The outbox retry deadline, armed on its own arm. Independent of `wakeup`:
    /// the liveness schedule and the retry schedule are separate promises, and
    /// one deadline could only keep one. `None` — the ordinary state — disarms.
    retry_wakeup: Option<Millis>,
    /// The confined-release deadline, wall-clock epoch milliseconds. A wall-clock
    /// deadline rather than a [`Millis`] one because a release time is an instant
    /// a publisher named; the delay is recomputed against the wall clock on every
    /// pass, so a clock that steps is followed rather than fought.
    release_wakeup: Option<ReleaseTime>,
    /// A URL the connection asked to open, awaiting the embedder's connect turn.
    pending_connect: Option<String>,
    /// The first attempt's effects, held until [`AttachDriver::start`] executes
    /// them — effect execution is async and a constructor is not.
    start_effects: Vec<ConnEffect>,
}

/// A stamp for one locally minted envelope: a fresh identity and the wall-clock
/// instant it carries.
///
/// Free rather than only a method, because the driver is not the embedder's only
/// I/O edge: a browser embedder that mints an envelope on a synchronous stack of
/// its own has no driver in reach and must still read entropy and a clock in one
/// place that the sans-I/O layers below do not.
pub fn new_stamp() -> MessageStamp {
    MessageStamp {
        message_id: Uuid::new_v4(),
        publish_ts: wall_now(),
    }
}

/// `count` stamps for one flush, all carrying the same wall-clock instant: a
/// flush is one commit, and its entries must agree about when now was.
pub fn flush_stamps(count: usize) -> Vec<MessageStamp> {
    let publish_ts = wall_now();
    (0..count)
        .map(|_| MessageStamp {
            message_id: Uuid::new_v4(),
            publish_ts,
        })
        .collect()
}

impl<C: TransportConnector> AttachDriver<C> {
    /// Build the driver and its connection. Nothing has happened yet: the first
    /// attempt's effects wait for [`AttachDriver::start`].
    pub fn new(config: ConnConfig, connector: C) -> Self {
        let clock = Clock::new();
        let (conn, start_effects) = Connection::start(config, clock.now());
        Self {
            conn,
            connector,
            transport: None,
            clock,
            wakeup: None,
            retry_wakeup: None,
            release_wakeup: None,
            pending_connect: None,
            start_effects,
        }
    }

    /// Execute the first attempt's effects: the connect the embedder then runs,
    /// and the handshake deadline guarding it.
    ///
    /// Call this exactly once, before the first [`AttachDriver::wait`]. An
    /// embedder that has already diagnosed a host precondition failure calls
    /// [`AttachDriver::host_fatal`] instead and never starts — a client that
    /// cannot honour the contract has no business connecting.
    pub async fn start(&mut self) -> DriverStep {
        let effects = std::mem::take(&mut self.start_effects);
        DriverStep {
            events: self.execute(effects).await,
            routed: None,
        }
    }

    /// The monotonic instant every layer above is driven with.
    pub fn now(&self) -> Millis {
        self.clock.now()
    }

    /// A stamp for one confined publish: the identity and the wall-clock instant
    /// its envelope carries.
    ///
    /// Minted here because both readings are I/O — entropy and a clock — and the
    /// layers that compose the publish are sans-I/O. Only for confined publishes,
    /// where the attacher mints the envelope.
    pub fn new_stamp(&self) -> MessageStamp {
        new_stamp()
    }

    /// `count` stamps for one flush, all carrying the same wall-clock instant.
    ///
    /// One clock reading: a flush is one commit, and its entries must agree about
    /// when now was.
    pub fn flush_stamps(&self, count: usize) -> Vec<MessageStamp> {
        flush_stamps(count)
    }

    /// The connection's current lifecycle state.
    pub fn state(&self) -> ConnState {
        self.conn.state()
    }

    /// Whether frames may be sent right now. The planes above ask before handing
    /// anything to [`AttachDriver::send`]: off `Active` there is no wire.
    pub fn is_active(&self) -> bool {
        self.conn.state() == ConnState::Active
    }

    /// Whether the attachment is over. Terminal is terminal: no reconnect, and
    /// the embedder's loop winds down.
    pub fn is_terminal(&self) -> bool {
        self.conn.state() == ConnState::Terminal
    }

    /// The negotiated transport version, once the handshake reached one.
    pub fn version(&self) -> Option<u32> {
        self.conn.version()
    }

    /// Take the URL the connection asked to open, if it has asked. The embedder's
    /// loop checks this each turn: a URL here means the next turn is a connect
    /// ([`AttachDriver::connect`]) rather than a [`AttachDriver::wait`].
    pub fn take_pending_connect(&mut self) -> Option<String> {
        self.pending_connect.take()
    }

    /// Arm, re-arm, or disarm the outbox retry timer from what
    /// [`crate::publish::Outboxes`] answered. `None` — the plane's "unchanged" —
    /// leaves the armed deadline exactly as it is, which is what keeps unrelated
    /// traffic from pushing a blocked head's deadline out indefinitely.
    pub fn set_retry_wakeup(&mut self, change: Option<TimerChange>) {
        match change {
            Some(TimerChange::Arm(at)) => self.retry_wakeup = Some(at),
            Some(TimerChange::Disarm) => self.retry_wakeup = None,
            None => {}
        }
    }

    /// Arm, re-arm, or disarm the confined-release timer from what
    /// [`crate::router::LocalRouter::release_wakeup`] answered. `None` leaves it
    /// alone, as above.
    pub fn set_release_wakeup(&mut self, change: Option<ReleaseTimer>) {
        match change {
            Some(ReleaseTimer::Arm(at)) => self.release_wakeup = Some(at),
            Some(ReleaseTimer::Disarm) => self.release_wakeup = None,
            None => {}
        }
    }

    /// Wait for the next thing that concerns the attachment: a transport event,
    /// the connection's deadline, the retry deadline, or the release deadline.
    ///
    /// Resolves to exactly one of them and consumes nothing above the transport:
    /// an embedder's `select` may drop a pending call and lose only the wait.
    /// What it resolves to must be acted on — the transport of an event that
    /// ended it has already been dropped here, and nothing above has been told.
    ///
    /// Pends forever when there is no transport and no armed deadline. That is
    /// the terminal state's shape by construction — reaching terminal disarms all
    /// three deadlines and drops the transport — so an embedder winding down may
    /// keep calling this without being handed timer events for an attachment that
    /// is over.
    pub async fn wait(&mut self) -> IoEvent {
        let woke = {
            let Self {
                transport,
                clock,
                wakeup,
                retry_wakeup,
                release_wakeup,
                ..
            } = &mut *self;
            let socket = async {
                match transport.as_mut() {
                    Some(transport) => transport.next_event().await,
                    // Connecting, backing off, or terminal: only a timer can fire.
                    None => future::pending::<TransportEvent>().await,
                }
            }
            .fuse();
            let tick = sleep_until(clock, *wakeup).fuse();
            let retry = sleep_until(clock, *retry_wakeup).fuse();
            let release = sleep_until_release(*release_wakeup).fuse();
            pin_mut!(socket, tick, retry, release);
            // The transport is biased first: an arrived frame is evidence about
            // the very deadline the liveness tick would act on, so reading it
            // first turns a tick that would have reaped a live attachment into
            // one that re-arms.
            select_biased! {
                event = socket => Woke::Socket(event),
                () = tick => Woke::Tick,
                () = retry => Woke::Retry,
                () = release => Woke::Release,
            }
        };
        match woke {
            Woke::Socket(TransportEvent::Text(text)) => IoEvent::Conn(ConnInput::TextFrame(text)),
            Woke::Socket(TransportEvent::Binary(_)) => IoEvent::Conn(ConnInput::BinaryFrame),
            Woke::Socket(TransportEvent::Closed { code, reason }) => {
                // The transport goes before the connection reacts, so no dead
                // socket can be written to or read again. The code and reason ride
                // the `Detached` event this input produces, so a server-initiated
                // error close reaches the embedder rather than vanishing into the
                // backoff.
                tracing::debug!(?code, reason = %reason, "attach driver: transport closed");
                self.transport = None;
                IoEvent::Conn(ConnInput::Disconnected { code, reason })
            }
            Woke::Socket(TransportEvent::Failed(description)) => {
                // A transport-level failure carries no close code; its
                // description is the only diagnosis, so it passes through as
                // the reason.
                tracing::debug!(%description, "attach driver: transport failed");
                self.transport = None;
                IoEvent::Conn(ConnInput::Disconnected {
                    code: None,
                    reason: description,
                })
            }
            Woke::Tick => IoEvent::Conn(ConnInput::Tick),
            Woke::Retry => IoEvent::RetryDue,
            Woke::Release => IoEvent::ReleaseDue {
                now_ms: epoch_ms(wall_now()),
            },
        }
    }

    /// Run one connect attempt against `url`, racing it against the connection's
    /// armed handshake deadline.
    ///
    /// Answers [`ConnInput::Opened`] (the transport is now the driver's),
    /// [`ConnInput::ConnectFailed`], or [`ConnInput::Tick`] when the deadline won
    /// — each fed straight back through [`AttachDriver::on_input`].
    ///
    /// **Pin this across an embedder's own select arms.** Unlike
    /// [`AttachDriver::wait`], dropping this future abandons the attempt: an
    /// embedder that lets an unrelated command cancel it would restart the
    /// connect on every command and never finish one under load.
    pub async fn connect(&mut self, url: &str) -> ConnInput {
        let raced = {
            let Self {
                connector,
                clock,
                wakeup,
                ..
            } = &mut *self;
            let connect = connector.connect(url).fuse();
            let timer = sleep_until(clock, *wakeup).fuse();
            pin_mut!(connect, timer);
            select_biased! {
                result = connect => Some(result),
                () = timer => None,
            }
        };
        match raced {
            Some(Ok(transport)) => {
                self.transport = Some(transport);
                ConnInput::Opened
            }
            Some(Err(err)) => {
                // Retryable — the connection backs off — and the one loss no event
                // carries: a failed attempt reaches the embedder as a silent
                // return to backoff. So it is warned rather than debugged, since
                // otherwise a persistent connect loop (bad DNS, TLS, refused) is
                // invisible at default verbosity. The backoff bounds the rate.
                tracing::warn!(error = %err, "attach driver: connect attempt failed");
                ConnInput::ConnectFailed
            }
            None => ConnInput::Tick,
        }
    }

    /// Feed the connection one input and execute what it answers.
    pub async fn on_input(&mut self, input: ConnInput) -> DriverStep {
        let now = self.clock.now();
        let step = self.conn.on_input(input, now);
        self.run(step).await
    }

    /// Take the attachment down over a precondition the connection cannot check
    /// for itself — a host clock that cannot timestamp, a document that will not
    /// validate. Terminal, and it discards an unexecuted start: a driver that
    /// goes fatal before starting never connects.
    pub async fn host_fatal(&mut self, detail: String) -> DriverStep {
        self.start_effects.clear();
        let effects = self.conn.go_fatal(detail);
        DriverStep {
            events: self.execute(effects).await,
            routed: None,
        }
    }

    /// Shut the attachment down at the embedder's request. Terminal and silent —
    /// the embedder asked, so it needs no event telling it what it did.
    pub async fn close(&mut self) -> DriverStep {
        self.start_effects.clear();
        let effects = self.conn.close();
        DriverStep {
            events: self.execute(effects).await,
            routed: None,
        }
    }

    /// Write frames the planes above produced, in order.
    ///
    /// A write failure is a lost transport: the connection is told, backs off,
    /// and the rest of this batch — computed against the connection that just
    /// died — is dropped rather than written to a corpse.
    ///
    /// # Panics
    ///
    /// With no live transport and no failure in this batch to explain it. The
    /// planes emit only while the attachment is live, so a frame arriving here
    /// off `Active` is an embedder that did not ask [`AttachDriver::is_active`].
    pub async fn send(&mut self, frames: Vec<ClientFrame>) -> DriverStep {
        let effects = frames.into_iter().map(ConnEffect::SendFrame).collect();
        DriverStep {
            events: self.execute(effects).await,
            routed: None,
        }
    }

    /// Execute a connection step: its effects, then its routed frame.
    async fn run(&mut self, step: ConnStep) -> DriverStep {
        let ConnStep { effects, routed } = step;
        DriverStep {
            events: self.execute(effects).await,
            routed,
        }
    }

    /// Execute effects in order, draining to quiescence: a failed write feeds the
    /// connection back and its effects join the same queue.
    async fn execute(&mut self, effects: Vec<ConnEffect>) -> Vec<ConnEvent> {
        let mut queue: VecDeque<ConnEffect> = effects.into();
        let mut events = Vec::new();
        // Set when a write fails partway through this batch: every later
        // `SendFrame` in it was computed against the now-dead transport.
        let mut transport_lost = false;
        while let Some(effect) = queue.pop_front() {
            match effect {
                ConnEffect::Connect { url } => self.pending_connect = Some(url),
                ConnEffect::CloseTransport => {
                    if let Some(mut transport) = self.transport.take() {
                        transport.close().await;
                    }
                    // A connect the connection asked for but the embedder has
                    // not run yet is abandoned with the transport: a close or a
                    // fatal must not be followed by an attempt it predates.
                    self.pending_connect = None;
                }
                ConnEffect::SetWakeup(deadline) => self.wakeup = deadline,
                ConnEffect::SendFrame(frame) => match self.transport.as_mut() {
                    Some(transport) => {
                        let text = serde_json::to_string(&frame)
                            .expect("attach driver: a client frame serializes to JSON");
                        if let Err(err) = transport.send_text(text).await {
                            tracing::debug!(%err, "attach driver: write failed, disconnecting");
                            self.transport = None;
                            transport_lost = true;
                            let now = self.clock.now();
                            let step = self.conn.on_input(
                                ConnInput::Disconnected {
                                    code: None,
                                    reason: format!("write failed: {err}"),
                                },
                                now,
                            );
                            debug_assert!(
                                step.routed.is_none(),
                                "attach driver: a disconnect routes no frame"
                            );
                            queue.extend(step.effects);
                        }
                    }
                    None if transport_lost => {
                        tracing::debug!(
                            "attach driver: dropping a frame computed against the lost transport"
                        );
                    }
                    None => panic!("attach driver: a frame to send with no live transport"),
                },
                ConnEffect::Emit(event) => events.push(event),
            }
        }
        if self.conn.state() == ConnState::Terminal {
            // The connection disarms its own deadline on the way out, but the
            // outbox and release deadlines are the planes' and outlive the step
            // that ended the attachment. Nothing will ever answer them now, and a
            // release deadline already past would resolve every `wait` at once —
            // so the terminal state's shape holds only if they go too.
            self.retry_wakeup = None;
            self.release_wakeup = None;
        }
        events
    }
}

/// Which arm of the wait fired.
enum Woke {
    Socket(TransportEvent),
    Tick,
    Retry,
    Release,
}

/// Sleep to a monotonic deadline, or forever when disarmed. Recomputed each
/// call, so a re-armed deadline takes effect on the next turn.
async fn sleep_until(clock: &Clock, deadline: Option<Millis>) {
    match deadline {
        Some(deadline) => {
            let delay = deadline.0.saturating_sub(clock.now().0);
            timer::sleep(Duration::from_millis(delay)).await;
        }
        None => future::pending::<()>().await,
    }
}

/// Sleep to a wall-clock release deadline, or forever when disarmed.
///
/// The delay is measured from the wall clock read now, so a clock that steps is
/// followed: the release time a publisher named is a wall-clock instant, and a
/// host whose clock jumps forward owes its parked messages sooner. A deadline
/// already past sleeps not at all, which is correct — the sweep releases
/// everything due at the fire, so the next armed deadline is in the future of
/// the same read.
async fn sleep_until_release(deadline: Option<ReleaseTime>) {
    match deadline {
        Some(deadline) => {
            let delay = deadline.saturating_sub(epoch_ms(wall_now()));
            timer::sleep(Duration::from_millis(delay)).await;
        }
        None => future::pending::<()>().await,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

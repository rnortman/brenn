//! The loop that owns the attachment: the only code that touches the driver
//! and the planes.
//!
//! It runs on a tokio current-thread runtime on its own OS thread. Each turn
//! either runs the connect the driver asked for, answering commands beside it
//! without touching the attempt, or selects, commands first, between the next
//! command and the driver's wait. The wait arm is enabled only while the event
//! buffer has room, so a reader that stops reading stalls its own socket.
//!
//! Beyond absorbing the driver's steps, it decides:
//!
//! - A statement in flight at a detach is lost, not re-sent: a first subscribe
//!   still unanswered is released and its caller told `ConnectionLost`, as is
//!   every outstanding publish.
//! - A held subscription survives a detach and is re-sent at the next
//!   attachment with its cursor; its settlement reaches the reader as an event.
//! - An unsubscribe is honoured in every state and never needs an attachment.
//! - Three consecutive attachments that sent something and were answered
//!   nothing end the attachment.
//! - A dial whose upgrade is answered 401 or 403 ends the attachment.
//!
//! No blocking and no Python concern lives here; the blocking face is
//! [`crate::handle`].
// TODO(attach-blocking-second-loop-and-dial): a second loop over the attach
// client beside brenn-pod's `brenn-bridge`; a fix to one does not reach the
// other.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use brenn_attach_client::TransportConnector;
use brenn_attach_client::conn::{AttachmentFacts, ConnConfig, ConnEvent, ConnInput, DetachReason};
use brenn_attach_client::driver::{AttachDriver, DriverStep, IoEvent};
use brenn_attach_client::publish::{PendingPublishes, PublishRequest};
use brenn_attach_client::subs::{
    DeliverDisposition, ResumePolicy, SubscribeSettlement, SubscriptionDepths, Subscriptions,
};
use brenn_attach_proto::{ClientFrame, PublishOutcome as WireOutcome, ServerFrame};
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::connector::{DialCell, take_failure};
use crate::{
    CallError, Delivery, DialFailure, EVENT_CAPACITY, EndCause, Event, PROTOCOL, PublishOutcome,
    Status, SubscribeAnswer,
};

/// Consecutive attachments that spoke and were answered nothing before the
/// attachment ends.
const MAX_FUTILE_ATTACHMENTS: u32 = 3;

/// Inbound silence, in multiples of the server's heartbeat, that marks the
/// attachment dead.
const LIVENESS_MULTIPLIER: u32 = 3;

/// The reconnect schedule.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Timings {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub connect_timeout: Duration,
}

impl Timings {
    pub(crate) const DEFAULT: Timings = Timings {
        initial_backoff: Duration::from_millis(500),
        max_backoff: Duration::from_secs(30),
        connect_timeout: Duration::from_secs(15),
    };
}

pub(crate) fn conn_config(url: String, ident: String, timings: Timings, seed: u64) -> ConnConfig {
    ConnConfig {
        url,
        ident,
        versions: PROTOCOL,
        initial_backoff: timings.initial_backoff,
        max_backoff: timings.max_backoff,
        connect_timeout: timings.connect_timeout,
        liveness_multiplier: LIVENESS_MULTIPLIER,
        backoff_jitter_seed: seed,
        terminal_close_codes: Vec::new(),
    }
}

/// A statement from a caller's thread to the supervisor.
pub(crate) enum Command {
    Publish {
        id: u64,
        request: PublishRequest,
        reply: Sender<Result<PublishOutcome, CallError>>,
    },
    Subscribe {
        id: u64,
        channel: String,
        depths: SubscriptionDepths,
        resume: ResumePolicy,
        reply: Sender<Result<SubscribeAnswer, CallError>>,
    },
    Unsubscribe {
        channel: String,
        reply: Sender<Result<(), CallError>>,
    },
    /// The caller of request `id` stopped waiting: forget a publish's answer,
    /// release a subscribe whether pending or acknowledged.
    Cancel {
        id: u64,
    },
    Shutdown,
}

/// What the supervisor and the callers' threads share.
pub(crate) struct Shared {
    state: Mutex<SharedState>,
    /// Signalled whenever an event, the status or the end changes.
    pub(crate) changed: Condvar,
    /// Notified when a reader frees room in the event buffer.
    pub(crate) room: Notify,
}

pub(crate) struct SharedState {
    /// Subscription events, in wire order.
    pub(crate) buffer: VecDeque<Event>,
    /// The latest connection state the reader has not seen.
    pub(crate) signal: Option<Event>,
    pub(crate) standing: Standing,
    /// The most recent attachment's facts.
    pub(crate) facts: Option<AttachmentFacts>,
}

/// Where the attachment stands, the time before the first attachment
/// included.
pub(crate) enum Standing {
    /// No attachment yet; `last_dial_failure` is the latest dial's.
    Attaching {
        last_dial_failure: Option<DialFailure>,
    },
    /// What a caller is shown.
    Status(Status),
}

impl SharedState {
    /// Why the attachment ended, once it has.
    pub(crate) fn end(&self) -> Option<&EndCause> {
        match &self.standing {
            Standing::Status(Status::Ended(cause)) => Some(cause),
            Standing::Attaching { .. } | Standing::Status(_) => None,
        }
    }

    /// Show `status`, unless the attachment has ended: the end is sticky.
    fn show(&mut self, status: Status) {
        if self.end().is_none() {
            self.standing = Standing::Status(status);
        }
    }
}

impl Shared {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(SharedState {
                buffer: VecDeque::new(),
                signal: None,
                standing: Standing::Attaching {
                    last_dial_failure: None,
                },
                facts: None,
            }),
            changed: Condvar::new(),
            room: Notify::new(),
        }
    }

    /// The state, whether or not a thread panicked holding it: every write
    /// leaves it consistent.
    pub(crate) fn lock(&self) -> MutexGuard<'_, SharedState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn update(&self, change: impl FnOnce(&mut SharedState)) {
        change(&mut self.lock());
        self.changed.notify_all();
    }

    /// Record the end, unless one is recorded already. A connection state the
    /// reader has not seen is dropped: once the buffer drains, a read reports
    /// the end.
    pub(crate) fn end(&self, cause: EndCause) {
        self.update(|state| {
            if state.end().is_none() {
                state.standing = Standing::Status(Status::Ended(cause));
                state.signal = None;
            }
        });
    }
}

/// Which request a hold belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Hold {
    /// The first subscribe is on the wire, unanswered.
    Requested(u64),
    /// The server acknowledged it.
    Held(u64),
}

/// Everything the loop owns but the driver and the command receiver.
pub(crate) struct Core {
    shared: Arc<Shared>,
    dial: DialCell,
    subs: Subscriptions,
    /// Correlations are request ids: unique per client, so per attachment.
    pending: PendingPublishes<()>,
    publish_waiters: HashMap<u64, Sender<Result<PublishOutcome, CallError>>>,
    holds: BTreeMap<String, Hold>,
    subscribe_waiters: HashMap<u64, Sender<Result<SubscribeAnswer, CallError>>>,
    attached_once: bool,
    futile: u32,
    spoke: bool,
    answered: bool,
    end: Option<EndCause>,
}

impl Core {
    pub(crate) fn new(shared: Arc<Shared>, dial: DialCell) -> Self {
        Self {
            shared,
            dial,
            subs: Subscriptions::new(),
            pending: PendingPublishes::new(),
            publish_waiters: HashMap::new(),
            holds: BTreeMap::new(),
            subscribe_waiters: HashMap::new(),
            attached_once: false,
            futile: 0,
            spoke: false,
            answered: false,
            end: None,
        }
    }

    fn finish(&mut self, cause: EndCause) {
        if self.end.is_none() {
            self.end = Some(cause);
        }
    }

    fn has_room(&self) -> bool {
        self.shared.lock().buffer.len() < EVENT_CAPACITY
    }

    fn push(&self, event: Event) {
        self.shared.update(|state| state.buffer.push_back(event));
    }

    /// Act on one command other than `Shutdown`, answering with the frames to
    /// send. With `active` false nothing is sent and nothing may be.
    fn on_command(&mut self, command: Command, active: bool) -> Vec<ClientFrame> {
        match command {
            Command::Publish { id, request, reply } => {
                if !active {
                    let _ = reply.send(Err(CallError::NotAttached));
                    return Vec::new();
                }
                self.publish_waiters.insert(id, reply);
                vec![self.pending.send(id, (), request)]
            }
            Command::Subscribe {
                id,
                channel,
                depths,
                resume,
                reply,
            } => {
                if self.holds.contains_key(&channel) {
                    let _ = reply.send(Err(CallError::AlreadySubscribed));
                    return Vec::new();
                }
                if !active {
                    let _ = reply.send(Err(CallError::NotAttached));
                    return Vec::new();
                }
                self.holds.insert(channel.clone(), Hold::Requested(id));
                self.subscribe_waiters.insert(id, reply);
                self.subs.acquire(&channel, depths, resume)
            }
            Command::Unsubscribe { channel, reply } => {
                if !matches!(self.holds.get(&channel), Some(Hold::Held(_))) {
                    let _ = reply.send(Err(CallError::NotSubscribed));
                    return Vec::new();
                }
                let frames = self.release(&channel);
                let _ = reply.send(Ok(()));
                frames
            }
            Command::Cancel { id } => {
                self.publish_waiters.remove(&id);
                self.subscribe_waiters.remove(&id);
                let channel = self.holds.iter().find_map(|(channel, hold)| {
                    matches!(hold, Hold::Requested(held) | Hold::Held(held) if *held == id)
                        .then(|| channel.clone())
                });
                match channel {
                    Some(channel) => self.release(&channel),
                    None => Vec::new(),
                }
            }
            Command::Shutdown => unreachable!("the loop handles Shutdown itself"),
        }
    }

    /// Drop the hold on `channel` and every buffered event of it.
    fn release(&mut self, channel: &str) -> Vec<ClientFrame> {
        self.holds.remove(channel);
        self.shared.update(|state| {
            state
                .buffer
                .retain(|event| event.channel() != Some(channel))
        });
        self.subs.release(channel)
    }

    /// Record a connect attempt's outcome as the latest dial's failure, `None`
    /// for an attempt that opened. Answers whether it ended the attachment.
    fn on_dial(&mut self, input: &ConnInput) -> bool {
        let failure = match input {
            ConnInput::Opened => None,
            ConnInput::Tick => Some(DialFailure::TimedOut),
            _ => take_failure(&self.dial),
        };
        if let Some(DialFailure::UpgradeRejected {
            status: status @ (401 | 403),
        }) = failure
        {
            self.finish(EndCause::CredentialRejected { status });
            return true;
        }
        self.shared.update(|state| {
            if let Standing::Attaching { last_dial_failure }
            | Standing::Status(Status::Detached {
                last_dial_failure, ..
            }) = &mut state.standing
            {
                *last_dial_failure = failure.clone();
            }
            if let Some(Event::Detached {
                last_dial_failure, ..
            }) = &mut state.signal
            {
                *last_dial_failure = failure;
            }
        });
        false
    }

    async fn on_io<C: TransportConnector>(&mut self, driver: &mut AttachDriver<C>, io: IoEvent) {
        let step = match io {
            IoEvent::Conn(input) => driver.on_input(input).await,
            // No outbox and no confined channel: neither deadline is ever armed.
            other @ (IoEvent::RetryDue | IoEvent::ReleaseDue { .. }) => {
                driver
                    .host_fatal(format!("an unarmed deadline fired: {other:?}"))
                    .await
            }
        };
        self.absorb(driver, step).await;
    }

    /// Turn one driver step into events, answers and the frames they owe.
    async fn absorb<C: TransportConnector>(
        &mut self,
        driver: &mut AttachDriver<C>,
        step: DriverStep,
    ) {
        let mut queue = VecDeque::from([step]);
        while let Some(step) = queue.pop_front() {
            for event in step.events {
                let frames = self.on_conn_event(event);
                if let Some(next) = self.emit(driver, frames).await {
                    queue.push_back(next);
                }
            }
            let Some(frame) = step.routed else { continue };
            self.answered = true;
            match self.route(frame) {
                Ok(frames) => {
                    if let Some(next) = self.emit(driver, frames).await {
                        queue.push_back(next);
                    }
                }
                Err(detail) => queue.push_back(driver.host_fatal(detail).await),
            }
        }
    }

    fn on_conn_event(&mut self, event: ConnEvent) -> Vec<ClientFrame> {
        match event {
            ConnEvent::Attached(facts) => {
                self.spoke = false;
                self.answered = false;
                let signal = self.attached_once;
                self.attached_once = true;
                self.shared.update(|state| {
                    state.show(Status::Attached(facts.clone()));
                    state.facts = Some(facts.clone());
                    if signal {
                        state.signal = Some(Event::Attached(facts));
                    }
                });
                self.subs.on_attached()
            }
            ConnEvent::Detached { reason } => {
                self.on_detached(reason);
                Vec::new()
            }
            ConnEvent::Fatal { detail } => {
                self.finish(EndCause::Fatal { detail });
                Vec::new()
            }
            ConnEvent::Incompatible { ours, theirs } => {
                self.finish(EndCause::Incompatible { ours, theirs });
                Vec::new()
            }
            // This client declares no terminal close code.
            ConnEvent::PeerClosedTerminal { code, reason } => {
                self.finish(EndCause::Fatal {
                    detail: format!("a terminal close nobody declared: {code} {reason}"),
                });
                Vec::new()
            }
        }
    }

    fn on_detached(&mut self, reason: DetachReason) {
        // A socket that closes before the first `Welcome` is a failed attempt,
        // not a detach: nothing was attached, nothing is outstanding, and the
        // standing stays `Attaching`.
        if !self.attached_once {
            return;
        }
        // A first subscribe unanswered at the drop is not repeated at the next
        // attachment: the drop may be the server's answer to it.
        // TODO(bridge-violation-close-code): the server closes with no code on
        // a statement outside the grant, so this cannot be told from a drop.
        let requested: Vec<(String, u64)> = self
            .holds
            .iter()
            .filter_map(|(channel, hold)| match hold {
                Hold::Requested(id) => Some((channel.clone(), *id)),
                Hold::Held(_) => None,
            })
            .collect();
        for (channel, id) in requested {
            self.holds.remove(&channel);
            self.subs.release(&channel);
            if let Some(reply) = self.subscribe_waiters.remove(&id) {
                let _ = reply.send(Err(CallError::ConnectionLost {
                    reason: reason.clone(),
                }));
            }
        }
        self.subs.on_detached();
        for (id, ()) in self.pending.fail_all() {
            if let Some(reply) = self.publish_waiters.remove(&id) {
                let _ = reply.send(Err(CallError::ConnectionLost {
                    reason: reason.clone(),
                }));
            }
        }
        if self.spoke && !self.answered {
            self.futile += 1;
        } else {
            self.futile = 0;
        }
        self.spoke = false;
        self.answered = false;
        self.shared.update(|state| {
            state.show(Status::Detached {
                reason: reason.clone(),
                last_dial_failure: None,
            });
            state.signal = Some(Event::Detached {
                reason,
                last_dial_failure: None,
            });
        });
        if self.futile >= MAX_FUTILE_ATTACHMENTS {
            self.finish(EndCause::Futile {
                attachments: self.futile,
            });
        }
    }

    /// Route one server frame into the plane that owns it. An `Err` is the
    /// peer breaking the protocol.
    fn route(&mut self, frame: ServerFrame) -> Result<Vec<ClientFrame>, String> {
        match frame {
            ServerFrame::SubscribeResult {
                channel,
                outcome,
                replay_count,
                gap,
            } => {
                let settlement =
                    self.subs
                        .on_subscribe_result(&channel, outcome, replay_count, gap)?;
                let hold = self.holds.get(&channel).copied();
                match settlement {
                    SubscribeSettlement::Opened(ack) => {
                        if ack.live {
                            match hold {
                                Some(Hold::Requested(id)) => {
                                    self.holds.insert(channel, Hold::Held(id));
                                    self.answer_subscribe(
                                        id,
                                        SubscribeAnswer::Subscribed {
                                            replay_count: ack.replay_count,
                                            gap: ack.gap,
                                        },
                                    );
                                }
                                Some(Hold::Held(_)) => self.push(Event::Resubscribed {
                                    channel,
                                    replay_count: ack.replay_count,
                                    gap: ack.gap,
                                }),
                                None => {}
                            }
                        }
                        Ok(ack.frames)
                    }
                    SubscribeSettlement::Unavailable => {
                        self.holds.remove(&channel);
                        match hold {
                            Some(Hold::Requested(id)) => {
                                self.answer_subscribe(id, SubscribeAnswer::Unavailable);
                            }
                            Some(Hold::Held(_)) => {
                                self.push(Event::SubscriptionDropped { channel });
                            }
                            None => {}
                        }
                        Ok(Vec::new())
                    }
                }
            }
            ServerFrame::Deliver { channel, rows } => {
                match self.subs.on_deliver(&channel, &rows)? {
                    DeliverDisposition::Accept { .. } => {
                        self.shared.update(|state| {
                            state.buffer.extend(rows.into_iter().map(|row| {
                                Event::Delivery(Box::new(Delivery {
                                    channel: channel.clone(),
                                    envelope: row.envelope,
                                    seq: row.seq,
                                    dropped: row.dropped,
                                }))
                            }))
                        });
                    }
                    // A pass from a span already left, in flight across the
                    // `Unsubscribe`.
                    DeliverDisposition::Discard { .. } => {}
                }
                Ok(Vec::new())
            }
            ServerFrame::PublishResult {
                correlation,
                outcome,
            } => {
                let id = correlation.ok_or("PublishResult with no correlation")?;
                self.pending.on_result(Some(id))?;
                let outcome = match outcome {
                    WireOutcome::Ok => PublishOutcome::Ok,
                    WireOutcome::Failed => PublishOutcome::Failed,
                    WireOutcome::RateLimited => PublishOutcome::RateLimited,
                    WireOutcome::BodyTooLarge { len, max } => {
                        PublishOutcome::BodyTooLarge { len, max }
                    }
                    WireOutcome::TextOnlyChannel => PublishOutcome::TextOnlyChannel,
                    WireOutcome::AttachmentNotHeld => {
                        return Err(format!(
                            "AttachmentNotHeld for publish {id}, which named no attachment"
                        ));
                    }
                };
                // A caller that timed out has gone; its answer is dropped.
                if let Some(reply) = self.publish_waiters.remove(&id) {
                    let _ = reply.send(Ok(outcome));
                }
                Ok(Vec::new())
            }
            // Sent unasked; this client parks nothing.
            ServerFrame::DeferredView { .. } => Ok(Vec::new()),
            ServerFrame::PublishBatchResult { correlation, .. } => Err(format!(
                "a batch result (correlation {correlation}) for a client that sends no batches"
            )),
            frame @ (ServerFrame::BlobCreateResult { .. }
            | ServerFrame::BlobSealResult { .. }
            | ServerFrame::BlobOpenResult { .. }
            | ServerFrame::BlobReadResult { .. }) => Err(format!(
                "a blob answer for a client that opens no blob: {frame:?}"
            )),
            frame @ (ServerFrame::Hello { .. }
            | ServerFrame::Welcome { .. }
            | ServerFrame::Heartbeat) => Err(format!(
                "a connection frame reached a plane above the connection: {frame:?}"
            )),
        }
    }

    fn answer_subscribe(&mut self, id: u64, answer: SubscribeAnswer) {
        if let Some(reply) = self.subscribe_waiters.remove(&id) {
            let _ = reply.send(Ok(answer));
        }
    }

    /// Write frames, or drop them when there is no wire: a held subscription
    /// is re-stated at the next attachment.
    async fn emit<C: TransportConnector>(
        &mut self,
        driver: &mut AttachDriver<C>,
        frames: Vec<ClientFrame>,
    ) -> Option<DriverStep> {
        if frames.is_empty() || !driver.is_active() {
            return None;
        }
        self.spoke = true;
        Some(driver.send(frames).await)
    }

    /// Answer every waiting caller with the end and publish it.
    fn wind_down(&mut self, cause: EndCause) {
        for (_, reply) in self.publish_waiters.drain() {
            let _ = reply.send(Err(CallError::Ended(cause.clone())));
        }
        for (_, reply) in self.subscribe_waiters.drain() {
            let _ = reply.send(Err(CallError::Ended(cause.clone())));
        }
        self.shared.end(cause);
    }
}

/// Drive the attachment until it ends, then record why.
pub(crate) async fn run<C: TransportConnector>(
    mut driver: AttachDriver<C>,
    mut core: Core,
    mut commands: UnboundedReceiver<Command>,
) {
    let step = driver.start().await;
    core.absorb(&mut driver, step).await;
    while core.end.is_none() {
        if let Some(url) = driver.take_pending_connect() {
            let input = {
                let connect = driver.connect(&url);
                tokio::pin!(connect);
                loop {
                    tokio::select! {
                        biased;
                        command = commands.recv() => match command {
                            Some(Command::Shutdown) | None => break None,
                            Some(command) => {
                                let frames = core.on_command(command, false);
                                debug_assert!(frames.is_empty(), "a frame with no attachment");
                            }
                        },
                        input = &mut connect => break Some(input),
                    }
                }
            };
            match input {
                None => core.finish(EndCause::Closed),
                Some(input) => {
                    if !core.on_dial(&input) {
                        let step = driver.on_input(input).await;
                        core.absorb(&mut driver, step).await;
                    }
                }
            }
            continue;
        }
        let shared = core.shared.clone();
        let room = core.has_room();
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(Command::Shutdown) | None => core.finish(EndCause::Closed),
                Some(command) => {
                    let frames = core.on_command(command, driver.is_active());
                    if let Some(step) = core.emit(&mut driver, frames).await {
                        core.absorb(&mut driver, step).await;
                    }
                }
            },
            () = shared.room.notified(), if !room => {}
            io = driver.wait(), if room => core.on_io(&mut driver, io).await,
        }
    }
    if !driver.is_terminal() {
        driver.close().await;
    }
    let cause = core
        .end
        .take()
        .expect("the loop exits only once an end is set");
    core.wind_down(cause);
}

#[cfg(test)]
mod page_tests {
    use super::*;

    #[test]
    fn the_python_binding_page_states_the_supervisors_figures() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/python-binding.md");
        let page = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display()));
        let page = page.split_whitespace().collect::<Vec<_>>().join(" ");
        let Timings {
            initial_backoff,
            max_backoff,
            connect_timeout,
        } = Timings::DEFAULT;
        let (initial, max, connect) = (
            initial_backoff.as_secs_f64(),
            max_backoff.as_secs_f64(),
            connect_timeout.as_secs_f64(),
        );
        for figure in [
            format!("({initial} s doubling to {max} s, each dial bounded at {connect} s)"),
            format!("outlived its {connect} s bound"),
            format!("buffer of {EVENT_CAPACITY}."),
            format!("| {MAX_FUTILE_ATTACHMENTS} attachments in a row"),
        ] {
            assert!(
                page.contains(&figure),
                "docs/python-binding.md does not say {figure:?}; update the page"
            );
        }
    }
}

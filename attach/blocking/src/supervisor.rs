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
//! - The blob plane is told of every attachment and every detach, and a call
//!   it would panic on is refused first: no door, no upload grant. A blob
//!   operation outstanding at a detach is answered `ConnectionLost`; a
//!   cancelled upload or open is closed, a cancelled read is left to finish.
//!
//! No blocking and no Python concern lives here; the blocking face is
//! [`crate::handle`].
// TODO(attach-blocking-second-loop-and-dial): a second loop over the attach
// client beside brenn-pod's `brenn-bridge`; a fix to one does not reach the
// other.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use brenn_attach_client::TransportConnector;
use brenn_attach_client::blobs::{
    BlobEvent, BlobPlane, BlobStep, ChunkRange, CreateError, OpenError, ReadError, ReadId,
};
use brenn_attach_client::conn::{AttachmentFacts, ConnConfig, ConnEvent, ConnInput, DetachReason};
use brenn_attach_client::driver::{AttachDriver, DriverStep, IoEvent};
use brenn_attach_client::publish::{PendingPublishes, PublishRequest};
use brenn_attach_client::subs::{
    DeliverDisposition, ResumePolicy, SubscribeSettlement, SubscriptionDepths, Subscriptions,
};
use brenn_attach_proto::{
    AttachmentName, BlobFacts, BlobHandle, ClientFrame, PublishOutcome as WireOutcome, ServerFrame,
};
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

use crate::connector::{DialCell, take_failure};
use crate::{
    Blob, CallError, Delivery, DialFailure, EVENT_CAPACITY, EndCause, Event, OpenRefusal, PROTOCOL,
    PublishOutcome, ReadRefusal, Settled, Status, SubscribeAnswer, UploadRefusal,
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
    /// Start an upload of `size` bytes. Every step of it is answered on
    /// `reply`: each range to send next, then the sealed blob.
    BlobCreate {
        id: u64,
        content_type: String,
        size: u64,
        reply: Sender<Result<UploadStep, CallError>>,
    },
    /// The bytes of the range upload `id` last named.
    BlobChunk {
        id: u64,
        bytes: Vec<u8>,
    },
    BlobOpen {
        id: u64,
        channel: String,
        message_id: Uuid,
        name: AttachmentName,
        reply: Sender<Result<Blob, CallError>>,
    },
    BlobRead {
        id: u64,
        handle: BlobHandle,
        offset: u64,
        len: u64,
        reply: Sender<Result<Vec<u8>, CallError>>,
    },
    BlobClose {
        handle: BlobHandle,
        reply: Sender<Result<(), CallError>>,
    },
    /// The caller of request `id` stopped waiting: forget a publish's answer,
    /// release a subscribe whether pending or acknowledged, forget a read's
    /// bytes. The request's reply channel is dropped once this is taken.
    Cancel {
        id: u64,
    },
    /// The caller of upload or open `id` stopped waiting and handed back its
    /// reply channel: the handle is closed, and so is every blob an answer
    /// already in the channel names. The caller does not wait for this.
    Abandon {
        id: u64,
        answer: Abandoned,
    },
    Shutdown,
}

/// One answer to an upload.
#[derive(Debug)]
pub(crate) enum UploadStep {
    /// Send this range next.
    Chunk(ChunkRange),
    Sealed(Blob),
}

/// The reply channel of an upload or open whose caller gave up. The
/// supervisor is its only sender, so once the waiter is gone every answer it
/// ever sent is in the channel.
pub(crate) enum Abandoned {
    Upload(Receiver<Result<UploadStep, CallError>>),
    Open(Receiver<Result<Blob, CallError>>),
}

impl Abandoned {
    /// The handles of the blobs the answers in the channel made.
    fn held(self) -> Vec<BlobHandle> {
        match self {
            Abandoned::Upload(answer) => answer
                .try_iter()
                .filter_map(|step| match step {
                    Ok(UploadStep::Sealed(blob)) => Some(blob.handle),
                    Ok(UploadStep::Chunk(_)) | Err(_) => None,
                })
                .collect(),
            Abandoned::Open(answer) => answer
                .try_iter()
                .filter_map(|opened| opened.ok().map(|blob| blob.handle))
                .collect(),
        }
    }
}

struct UploadWaiter {
    handle: BlobHandle,
    content_type: String,
    size: u64,
    reply: Sender<Result<UploadStep, CallError>>,
}

struct OpenWaiter {
    handle: BlobHandle,
    reply: Sender<Result<Blob, CallError>>,
}

struct ReadWaiter {
    id: u64,
    bytes: Vec<u8>,
    reply: Sender<Result<Vec<u8>, CallError>>,
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
    /// The client whose blobs this plane holds.
    client: u64,
    /// Tags are request ids.
    plane: BlobPlane<u64>,
    /// The live attachment's blob door, written with the plane's: the plane
    /// panics on a doorless or upload-less call, so the door is checked here
    /// first. A copy because the plane offers no accessor for its own.
    door: Option<BlobFacts>,
    uploads: HashMap<u64, UploadWaiter>,
    opens: HashMap<u64, OpenWaiter>,
    reads: HashMap<ReadId, ReadWaiter>,
    /// Uploads to tell their next range once this turn's frames are written,
    /// so one chunk per upload is in flight.
    next_chunks: Vec<u64>,
    attached_once: bool,
    futile: u32,
    spoke: bool,
    answered: bool,
    end: Option<EndCause>,
}

impl Core {
    pub(crate) fn new(shared: Arc<Shared>, dial: DialCell, client: u64) -> Self {
        Self {
            shared,
            dial,
            subs: Subscriptions::new(),
            pending: PendingPublishes::new(),
            publish_waiters: HashMap::new(),
            holds: BTreeMap::new(),
            subscribe_waiters: HashMap::new(),
            client,
            plane: BlobPlane::new(),
            door: None,
            uploads: HashMap::new(),
            opens: HashMap::new(),
            reads: HashMap::new(),
            next_chunks: Vec::new(),
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
                // The server takes a publish naming attachments with no door
                // as a violation.
                if !request.attachments.is_empty() && self.door.is_none() {
                    let _ = reply.send(Err(CallError::NoBlobDoor));
                    return Vec::new();
                }
                self.publish_waiters.insert(id, reply);
                vec![self.pending.send(id, (), request)]
            }
            Command::BlobCreate {
                id,
                content_type,
                size,
                reply,
            } => {
                let refusal = match self.door {
                    _ if !active => Some(CallError::NotAttached),
                    None => Some(CallError::NoBlobDoor),
                    Some(door) if !door.upload => Some(CallError::UploadNotGranted),
                    Some(_) => None,
                };
                if let Some(refusal) = refusal {
                    let _ = reply.send(Err(refusal));
                    return Vec::new();
                }
                let (handle, frames) = match self.plane.create(id, &content_type, size) {
                    Ok(created) => created,
                    Err(error) => {
                        let _ = reply.send(Err(match error {
                            CreateError::ContentType(error) => CallError::ContentType(error),
                            CreateError::BlobTooLarge { size, max } => {
                                CallError::UploadRefused(UploadRefusal::BlobTooLarge { size, max })
                            }
                            CreateError::HandleLimit => {
                                CallError::UploadRefused(UploadRefusal::HandleLimit)
                            }
                            CreateError::BytesHeldLimit => {
                                CallError::UploadRefused(UploadRefusal::BytesHeldLimit)
                            }
                        }));
                        return Vec::new();
                    }
                };
                self.uploads.insert(
                    id,
                    UploadWaiter {
                        handle,
                        content_type,
                        size,
                        reply,
                    },
                );
                self.next_chunks.push(id);
                frames
            }
            Command::BlobChunk { id, bytes } => {
                // Refused, cancelled or detached meanwhile: nothing to write.
                let Some(handle) = self.uploads.get(&id).map(|upload| upload.handle) else {
                    return Vec::new();
                };
                let error = match self.plane.next_chunk(handle) {
                    _ if !active => CallError::NotAttached,
                    Some(range) if range.len == bytes.len() => {
                        self.next_chunks.push(id);
                        return self.plane.write_chunk(handle, bytes);
                    }
                    // This crate's own handle code sent a chunk the plane did
                    // not name: end the upload rather than panic.
                    Some(range) => CallError::Fault {
                        detail: format!(
                            "a chunk of {} bytes for the range of {} at {}",
                            bytes.len(),
                            range.len,
                            range.offset
                        ),
                    },
                    None => CallError::Fault {
                        detail: format!("a chunk of {} bytes past the last range", bytes.len()),
                    },
                };
                let waiter = self.uploads.remove(&id).expect("looked up above");
                let _ = waiter.reply.send(Err(error));
                let step = self.plane.close(handle);
                self.settle(step.events, None);
                step.frames
            }
            Command::BlobOpen {
                id,
                channel,
                message_id,
                name,
                reply,
            } => {
                if !active {
                    let _ = reply.send(Err(CallError::NotAttached));
                    return Vec::new();
                }
                if self.door.is_none() {
                    let _ = reply.send(Err(CallError::NoBlobDoor));
                    return Vec::new();
                }
                match self.plane.open(id, &channel, message_id, name) {
                    Ok((handle, frame)) => {
                        self.opens.insert(id, OpenWaiter { handle, reply });
                        vec![frame]
                    }
                    Err(OpenError::HandleLimit) => {
                        let _ = reply.send(Err(CallError::OpenRefused(OpenRefusal::HandleLimit)));
                        Vec::new()
                    }
                }
            }
            Command::BlobRead {
                id,
                handle,
                offset,
                len,
                reply,
            } => {
                // Every handle the plane held is forgotten at a detach.
                if !active {
                    let _ = reply.send(Err(CallError::BlobGone));
                    return Vec::new();
                }
                match self.plane.read(handle, id, offset, len) {
                    Ok((read, frames)) => {
                        let bytes = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
                        self.reads.insert(read, ReadWaiter { id, bytes, reply });
                        frames
                    }
                    Err(error) => {
                        let _ = reply.send(Err(match error {
                            ReadError::OutOfRange { size } => {
                                CallError::ReadRefused(ReadRefusal::OutOfRange { size })
                            }
                            ReadError::Gone => CallError::BlobGone,
                        }));
                        Vec::new()
                    }
                }
            }
            Command::BlobClose { handle, reply } => {
                let step = self.plane.close(handle);
                self.settle(step.events, None);
                let _ = reply.send(Ok(()));
                step.frames
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
                self.reads.retain(|_, read| read.id != id);
                let channel = self.holds.iter().find_map(|(channel, hold)| {
                    matches!(hold, Hold::Requested(held) | Hold::Held(held) if *held == id)
                        .then(|| channel.clone())
                });
                match channel {
                    Some(channel) => self.release(&channel),
                    None => Vec::new(),
                }
            }
            Command::Abandon { id, answer } => {
                // The waiter goes first, so the close's event, or a seal or
                // open answer already on the wire, finds none; with it goes
                // the channel's last sender.
                let pending = match (self.uploads.remove(&id), self.opens.remove(&id)) {
                    (Some(upload), _) => Some(upload.handle),
                    (None, Some(open)) => Some(open.handle),
                    (None, None) => None,
                };
                let mut frames = Vec::new();
                for handle in pending.into_iter().chain(answer.held()) {
                    let step = self.plane.close(handle);
                    self.settle(step.events, None);
                    frames.extend(step.frames);
                }
                frames
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
                self.plane.on_attached(&facts);
                self.door = facts.blobs;
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
        let events = self.plane.on_detached();
        self.door = None;
        self.settle(events, Some(&reason));
        assert!(
            self.uploads.is_empty() && self.opens.is_empty() && self.reads.is_empty(),
            "the plane's detach reports every outstanding blob operation"
        );
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
                    WireOutcome::AttachmentNotHeld => PublishOutcome::AttachmentNotHeld,
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
            | ServerFrame::BlobReadResult { .. }) => {
                let BlobStep { frames, events } = self.plane.on_frame(frame)?;
                self.settle(events, None);
                Ok(frames)
            }
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

    /// Answer the waiters the plane's events name. `lost` is the detach's
    /// reason when the events are the plane's `on_detached`. An event whose
    /// waiter is gone, its caller having given up, is dropped.
    fn settle(&mut self, events: Vec<BlobEvent<u64>>, lost: Option<&DetachReason>) {
        for event in events {
            match event {
                BlobEvent::Sealed { tag, handle } => {
                    if let Some(upload) = self.uploads.remove(&tag) {
                        let blob = Blob {
                            client: self.client,
                            handle,
                            size: upload.size,
                            content_type: upload.content_type,
                        };
                        let _ = upload.reply.send(Ok(UploadStep::Sealed(blob)));
                    }
                }
                BlobEvent::UploadRefused { tag, reason, .. } => {
                    if let Some(upload) = self.uploads.remove(&tag) {
                        let error = refused(
                            UploadRefusal::settle(reason),
                            CallError::UploadRefused,
                            lost,
                        );
                        let _ = upload.reply.send(Err(error));
                    }
                }
                BlobEvent::Opened {
                    tag,
                    handle,
                    size,
                    content_type,
                } => {
                    if let Some(open) = self.opens.remove(&tag) {
                        let blob = Blob {
                            client: self.client,
                            handle,
                            size,
                            content_type,
                        };
                        let _ = open.reply.send(Ok(blob));
                    }
                }
                BlobEvent::OpenRefused { tag, reason, .. } => {
                    if let Some(open) = self.opens.remove(&tag) {
                        let error =
                            refused(OpenRefusal::settle(reason), CallError::OpenRefused, lost);
                        let _ = open.reply.send(Err(error));
                    }
                }
                BlobEvent::Chunk { read, bytes, .. } => {
                    if let Some(waiter) = self.reads.get_mut(&read) {
                        waiter.bytes.extend_from_slice(&bytes);
                    }
                }
                BlobEvent::ReadDone { read, .. } => {
                    if let Some(waiter) = self.reads.remove(&read) {
                        let _ = waiter.reply.send(Ok(waiter.bytes));
                    }
                }
                BlobEvent::ReadRefused { read, reason, .. } => {
                    if let Some(waiter) = self.reads.remove(&read) {
                        let error =
                            refused(ReadRefusal::settle(reason), CallError::ReadRefused, lost);
                        let _ = waiter.reply.send(Err(error));
                    }
                }
            }
        }
    }

    /// Tell each upload whose chunk this turn wrote the range it sends next;
    /// an upload whose seal is on the wire is told nothing until it settles.
    fn tell_next_chunks(&mut self) {
        for id in std::mem::take(&mut self.next_chunks) {
            let Some(upload) = self.uploads.get(&id) else {
                continue;
            };
            if let Some(range) = self.plane.next_chunk(upload.handle) {
                let _ = upload.reply.send(Ok(UploadStep::Chunk(range)));
            }
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
        for (_, upload) in self.uploads.drain() {
            let _ = upload.reply.send(Err(CallError::Ended(cause.clone())));
        }
        for (_, open) in self.opens.drain() {
            let _ = open.reply.send(Err(CallError::Ended(cause.clone())));
        }
        for (_, read) in self.reads.drain() {
            let _ = read.reply.send(Err(CallError::Ended(cause.clone())));
        }
        self.shared.end(cause);
    }
}

/// The error a refused blob operation answers its caller. `lost` is the
/// detach's reason when the refusal is the plane's `on_detached`.
fn refused<R>(
    settled: Settled<R>,
    wrap: fn(R) -> CallError,
    lost: Option<&DetachReason>,
) -> CallError {
    match settled {
        Settled::Refused(refusal) => wrap(refusal),
        Settled::Closed => CallError::BlobGone,
        Settled::Detached => CallError::ConnectionLost {
            reason: lost
                .expect("the plane reports a detach only from on_detached")
                .clone(),
        },
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
                                core.tell_next_chunks();
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
                    core.tell_next_chunks();
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

//! The blocking face: [`BlockingClient`], `Send + Sync`, every method `&self`.
//!
//! Every wait is enforced on the caller's thread, on the reply channel, and is
//! taken in slices of at most [`WAIT_SLICE`]: between slices the caller's
//! `interrupted` check runs, so a host whose signals must be checked on the
//! waiting thread can abandon any wait. An abandoned or timed-out call cancels
//! its request: the supervisor forgets a publish's answer and releases a
//! subscribe whether it is still pending or has since been acknowledged.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use brenn_attach_client::TransportConnector;
use brenn_attach_client::driver::AttachDriver;
use brenn_envelope::is_local_channel;
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use tokio::sync::mpsc::UnboundedSender;

use crate::connector::{BlockingConnector, DialCell, Trust, upgrade_request};
use crate::supervisor::{Command, Core, Shared, Standing, Timings, conn_config, run};
use crate::{
    AttachmentFacts, CallError, DialFailure, EVENT_CAPACITY, EndCause, Event, PublishOutcome,
    PublishRequest, RecvError, ResumePolicy, Status, SubscribeAnswer, SubscriptionDepths,
};

/// The longest a blocking call waits before running its `interrupted` check.
pub const WAIT_SLICE: Duration = Duration::from_millis(100);

/// What [`BlockingClient::attach`] needs.
pub struct AttachParams {
    /// The full `ws://` or `wss://` URL of the server's remote route.
    pub url: String,
    /// The remote's bearer token.
    pub token: String,
    /// A build identifier for the server's logs.
    pub ident: String,
    /// PEM certificates to trust for `wss://`, and nothing else; `None` trusts
    /// the platform's store.
    pub ca_pem: Option<Vec<u8>>,
    /// How long to wait for the first attachment.
    pub timeout: Duration,
}

/// Why [`BlockingClient::attach`] returned no client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachError {
    /// An argument was refused before anything was dialled.
    Invalid { detail: String },
    /// No attachment within the timeout; the most recent dial's failure, if
    /// one was seen.
    Timeout { last_failure: Option<DialFailure> },
    /// The first attachment ended for good.
    Ended(EndCause),
    /// The caller's interrupt check asked to abandon the wait.
    Interrupted,
}

impl std::fmt::Display for AttachError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttachError::Invalid { detail } => write!(f, "invalid attach argument: {detail}"),
            AttachError::Timeout {
                last_failure: Some(failure),
            } => write!(f, "no attachment within the timeout; last dial: {failure}"),
            AttachError::Timeout { last_failure: None } => {
                f.write_str("no attachment within the timeout")
            }
            AttachError::Ended(cause) => write!(f, "the attachment ended: {cause}"),
            AttachError::Interrupted => f.write_str("the wait was interrupted"),
        }
    }
}

impl std::error::Error for AttachError {}

/// A live attachment, supervised by a background thread.
pub struct BlockingClient {
    commands: UnboundedSender<Command>,
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
    next_id: AtomicU64,
}

impl BlockingClient {
    /// Validate `params`, start the supervisor, and block until the first
    /// attachment, a terminal end, `params.timeout`, or `interrupted` answers
    /// true. On every path but success the supervisor is shut down.
    pub fn attach(
        params: AttachParams,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<BlockingClient, AttachError> {
        let invalid = |detail: String| AttachError::Invalid { detail };
        upgrade_request(&params.url, &params.token).map_err(invalid)?;
        let trust = match &params.ca_pem {
            None => Trust::Platform,
            Some(pem) => {
                let certs = CertificateDer::pem_slice_iter(pem)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| invalid(format!("ca_pem: {err}")))?;
                if certs.is_empty() {
                    return Err(invalid("ca_pem holds no certificate".to_string()));
                }
                Trust::Roots(certs)
            }
        };
        let dial = DialCell::default();
        let connector = BlockingConnector::new(params.token, trust, dial.clone());
        let client = Self::spawn(params.url, params.ident, connector, dial, Timings::DEFAULT);
        client.await_attachment(params.timeout, interrupted)?;
        Ok(client)
    }

    /// Start a supervisor over any connector; `dial` is where the connector
    /// leaves each failed attempt's [`DialFailure`].
    pub(crate) fn spawn<C: TransportConnector + Send + 'static>(
        url: String,
        ident: String,
        connector: C,
        dial: DialCell,
        timings: Timings,
    ) -> BlockingClient {
        let shared = Arc::new(Shared::new());
        let (commands, receiver) = tokio::sync::mpsc::unbounded_channel();
        let seed = RandomState::new().build_hasher().finish();
        let config = conn_config(url, ident, timings, seed);
        let thread_shared = shared.clone();
        let thread = std::thread::Builder::new()
            .name("brenn-attach".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("a current-thread runtime builds");
                    let driver = AttachDriver::new(config, connector);
                    let core = Core::new(thread_shared.clone(), dial);
                    runtime.block_on(run(driver, core, receiver));
                }));
                if let Err(payload) = outcome {
                    let message = payload
                        .downcast_ref::<&str>()
                        .map(|text| text.to_string())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "a panic with no message".to_string());
                    thread_shared.end(EndCause::Panicked { message });
                }
            })
            .expect("the supervisor thread spawns");
        BlockingClient {
            commands,
            shared,
            thread: Mutex::new(Some(thread)),
            next_id: AtomicU64::new(1),
        }
    }

    fn await_attachment(
        &self,
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<(), AttachError> {
        let deadline = Instant::now() + timeout;
        loop {
            {
                let state = self.shared.lock();
                match &state.standing {
                    // A drop the caller has not yet seen still follows an
                    // attachment.
                    Standing::Status(Status::Attached(_) | Status::Detached { .. }) => {
                        return Ok(());
                    }
                    Standing::Status(Status::Ended(cause)) => {
                        let cause = cause.clone();
                        drop(state);
                        self.close();
                        return Err(AttachError::Ended(cause));
                    }
                    Standing::Attaching { last_dial_failure } => {
                        let now = Instant::now();
                        if now >= deadline {
                            let last_failure = last_dial_failure.clone();
                            drop(state);
                            self.close();
                            return Err(AttachError::Timeout { last_failure });
                        }
                        let wait = WAIT_SLICE.min(deadline - now);
                        let _ = self.shared.changed.wait_timeout(state, wait);
                    }
                }
            }
            if interrupted() {
                self.close();
                return Err(AttachError::Interrupted);
            }
        }
    }

    /// The most recent attachment's facts.
    pub fn facts(&self) -> AttachmentFacts {
        self.shared
            .lock()
            .facts
            .clone()
            .expect("a client exists only once it has attached")
    }

    /// Where the attachment stands now. Never blocks on the supervisor.
    pub fn status(&self) -> Status {
        match &self.shared.lock().standing {
            Standing::Status(status) => status.clone(),
            Standing::Attaching { .. } => {
                unreachable!("a client exists only once it has attached")
            }
        }
    }

    /// Publish one message and wait for the server's outcome.
    pub fn publish(
        &self,
        request: PublishRequest,
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<PublishOutcome, CallError> {
        if is_local_channel(&request.channel) {
            return Err(CallError::LocalChannel);
        }
        let id = self.next_id();
        let (reply, answer) = mpsc::channel();
        self.send(Command::Publish { id, request, reply })?;
        self.await_reply(answer, Some(id), Some(timeout), interrupted)
    }

    /// Hold `channel` and wait for the server's settlement.
    pub fn subscribe(
        &self,
        channel: &str,
        depths: SubscriptionDepths,
        resume: ResumePolicy,
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<SubscribeAnswer, CallError> {
        if is_local_channel(channel) {
            return Err(CallError::LocalChannel);
        }
        if depths.push_depth == 0 && depths.retain_depth == 0 {
            return Err(CallError::NoDepth);
        }
        let id = self.next_id();
        let (reply, answer) = mpsc::channel();
        self.send(Command::Subscribe {
            id,
            channel: channel.to_string(),
            depths,
            resume,
            reply,
        })?;
        self.await_reply(answer, Some(id), Some(timeout), interrupted)
    }

    /// Drop the hold on `channel`, in any state; no read yields an event of it
    /// afterwards until it is subscribed again. Fails only with
    /// [`CallError::NotSubscribed`] or [`CallError::Ended`] (or
    /// [`CallError::Interrupted`], when `interrupted` asked; the hold is then
    /// dropped or not, as the supervisor reached it).
    pub fn unsubscribe(
        &self,
        channel: &str,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<(), CallError> {
        let (reply, answer) = mpsc::channel();
        self.send(Command::Unsubscribe {
            channel: channel.to_string(),
            reply,
        })?;
        self.await_reply(answer, None, None, interrupted)
    }

    /// The next event: subscription events first, in wire order, then the
    /// latest connection state not yet seen. `Ok(None)` when `timeout` passed;
    /// `None` waits indefinitely.
    pub fn recv(
        &self,
        timeout: Option<Duration>,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<Option<Event>, RecvError> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        loop {
            {
                let mut state = self.shared.lock();
                if let Some(event) = state.buffer.pop_front() {
                    if state.buffer.len() < EVENT_CAPACITY {
                        self.shared.room.notify_one();
                    }
                    return Ok(Some(event));
                }
                if let Some(signal) = state.signal.take() {
                    return Ok(Some(signal));
                }
                if let Some(cause) = state.end() {
                    return Err(RecvError::Ended(cause.clone()));
                }
                let wait = match deadline {
                    None => WAIT_SLICE,
                    Some(deadline) => {
                        let now = Instant::now();
                        if now >= deadline {
                            return Ok(None);
                        }
                        WAIT_SLICE.min(deadline - now)
                    }
                };
                let _ = self.shared.changed.wait_timeout(state, wait);
            }
            if interrupted() {
                return Err(RecvError::Interrupted);
            }
        }
    }

    /// End the attachment and join the supervisor thread. Idempotent.
    pub fn close(&self) {
        let _ = self.commands.send(Command::Shutdown);
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(thread) = thread {
            // A panic has already been recorded as the end.
            let _ = thread.join();
        }
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn send(&self, command: Command) -> Result<(), CallError> {
        if let Some(cause) = self.shared.lock().end().cloned() {
            return Err(CallError::Ended(cause));
        }
        self.commands
            .send(command)
            .map_err(|_| CallError::Ended(self.await_end()))
    }

    /// Wait out a reply, cancelling request `id` on a timeout or interrupt.
    fn await_reply<T>(
        &self,
        answer: Receiver<Result<T, CallError>>,
        id: Option<u64>,
        timeout: Option<Duration>,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<T, CallError> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let cancel = |error: CallError| {
            if let Some(id) = id {
                let _ = self.commands.send(Command::Cancel { id });
            }
            Err(error)
        };
        loop {
            let wait = match deadline {
                None => WAIT_SLICE,
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return cancel(CallError::Timeout);
                    }
                    WAIT_SLICE.min(deadline - now)
                }
            };
            match answer.recv_timeout(wait) {
                Ok(result) => return result,
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(CallError::Ended(self.await_end()));
                }
                Err(RecvTimeoutError::Timeout) => {
                    if interrupted() {
                        return cancel(CallError::Interrupted);
                    }
                }
            }
        }
    }

    /// The end, once the supervisor has recorded it. A dropped reply channel
    /// means the supervisor is ending or has panicked; either records the
    /// end before its thread exits.
    fn await_end(&self) -> EndCause {
        let mut state = self.shared.lock();
        loop {
            if let Some(cause) = state.end() {
                return cause.clone();
            }
            state = self
                .shared
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

impl Drop for BlockingClient {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests;

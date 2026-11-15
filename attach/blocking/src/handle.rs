//! The blocking face: [`BlockingClient`], `Send + Sync`, every method `&self`.
//!
//! Every wait is enforced on the caller's thread, on the reply channel, and is
//! taken in slices of at most [`WAIT_SLICE`]: between slices the caller's
//! `interrupted` check runs, so a host whose signals must be checked on the
//! waiting thread can abandon any wait. An abandoned or timed-out call cancels
//! its request: the supervisor forgets a publish's answer and releases a
//! subscribe whether it is still pending or has since been acknowledged. An
//! abandoned upload or open is closed, and so is a blob its answer made
//! before the cancel reached the supervisor. An abandoned read leaves its
//! blob held; at most one window of it is fetched and dropped.
//!
//! Blob bytes are read from and written to files on the caller's thread, one
//! chunk or one window at a time, never on the supervisor's.

use std::collections::hash_map::RandomState;
use std::fs::{self, File, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use brenn_attach_client::TransportConnector;
use brenn_attach_client::blobs::ChunkRange;
use brenn_attach_client::driver::AttachDriver;
use brenn_envelope::{check_content_type, is_local_channel};
use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use crate::connector::{BlockingConnector, DialCell, Trust, upgrade_request};
use crate::supervisor::{
    Abandoned, Command, Core, Shared, Standing, Timings, UploadStep, conn_config, run,
};
use crate::{
    AttachmentFacts, AttachmentName, Blob, CallError, DialFailure, EVENT_CAPACITY, EndCause, Event,
    FileError, PublishAttachment, PublishOutcome, PublishRequest, ReadRefusal, RecvError,
    ResumePolicy, Status, SubscribeAnswer, SubscriptionDepths,
};

/// The longest a blocking call waits before running its `interrupted` check.
pub const WAIT_SLICE: Duration = Duration::from_millis(100);

/// Each client's identity, so a [`Blob`] is never handed to another.
static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);

/// Why a wait ended without an answer it can return as is.
enum Gave {
    /// The answer was an error.
    Answer(CallError),
    /// The caller's deadline or interrupt: the request must be cancelled.
    Up(CallError),
}

fn file_error(path: &Path) -> impl Fn(io::Error) -> CallError + '_ {
    move |error| CallError::File {
        path: path.to_path_buf(),
        error: FileError::from(&error),
    }
}

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
    /// This client's identity, carried by every [`Blob`] it makes.
    id: u64,
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
        let id = NEXT_CLIENT.fetch_add(1, Ordering::Relaxed);
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
                    let core = Core::new(thread_shared.clone(), dial, id);
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
            id,
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
        self.await_reply(
            answer,
            Some(id),
            Some(Instant::now() + timeout),
            interrupted,
        )
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
        self.await_reply(
            answer,
            Some(id),
            Some(Instant::now() + timeout),
            interrupted,
        )
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

    /// Upload `bytes` as a blob of `content_type` and wait until it is sealed.
    /// `timeout` bounds the whole upload.
    pub fn upload_bytes(
        &self,
        content_type: &str,
        bytes: &[u8],
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<Blob, CallError> {
        let chunk = |range: ChunkRange| {
            let start = usize::try_from(range.offset).expect("a range within the bytes");
            Ok(bytes[start..start + range.len].to_vec())
        };
        self.upload(
            content_type,
            bytes.len() as u64,
            chunk,
            timeout,
            interrupted,
        )
    }

    /// Upload the regular file at `path`, as long as it is when opened, a
    /// chunk at a time. Any other kind of file (a pipe, a device, a
    /// directory) and a file that yields fewer bytes than its length end the
    /// upload with [`CallError::File`]. `timeout` bounds the whole upload.
    pub fn upload_file(
        &self,
        content_type: &str,
        path: &Path,
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<Blob, CallError> {
        let failed = file_error(path);
        // The length of anything but a regular file is not what it yields,
        // and the open of a FIFO with no writer blocks: both are refused,
        // the FIFO before its open.
        let regular = |metadata: fs::Metadata| {
            if metadata.is_file() {
                Ok(metadata.len())
            } else {
                Err(failed(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "not a regular file",
                )))
            }
        };
        regular(fs::metadata(path).map_err(&failed)?)?;
        let mut file = File::open(path).map_err(&failed)?;
        let size = regular(file.metadata().map_err(&failed)?)?;
        let chunk = |range: ChunkRange| {
            let mut bytes = vec![0; range.len];
            file.seek(SeekFrom::Start(range.offset)).map_err(&failed)?;
            file.read_exact(&mut bytes).map_err(&failed)?;
            Ok(bytes)
        };
        self.upload(content_type, size, chunk, timeout, interrupted)
    }

    /// One upload: the create, then each range the supervisor names, read by
    /// `chunk` here and sent once the previous chunk is written.
    fn upload(
        &self,
        content_type: &str,
        size: u64,
        mut chunk: impl FnMut(ChunkRange) -> Result<Vec<u8>, CallError>,
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<Blob, CallError> {
        check_content_type(content_type).map_err(CallError::ContentType)?;
        let deadline = Instant::now() + timeout;
        let id = self.next_id();
        let (reply, answer) = mpsc::channel();
        self.send(Command::BlobCreate {
            id,
            content_type: content_type.to_string(),
            size,
            reply,
        })?;
        let error = loop {
            match self.wait(&answer, Some(deadline), interrupted) {
                Ok(UploadStep::Sealed(blob)) => return Ok(blob),
                Ok(UploadStep::Chunk(range)) => match chunk(range) {
                    Ok(bytes) => self.send(Command::BlobChunk { id, bytes })?,
                    Err(error) => break error,
                },
                Err(Gave::Answer(error)) => return Err(error),
                Err(Gave::Up(error)) => break error,
            }
        };
        Err(self.abandon(id, Abandoned::Upload(answer), error))
    }

    /// Open the attachment `name` of the message `message_id` on `channel`
    /// and wait until it is held.
    pub fn open_blob(
        &self,
        channel: &str,
        message_id: Uuid,
        name: AttachmentName,
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<Blob, CallError> {
        if is_local_channel(channel) {
            return Err(CallError::LocalChannel);
        }
        let id = self.next_id();
        let (reply, answer) = mpsc::channel();
        self.send(Command::BlobOpen {
            id,
            channel: channel.to_string(),
            message_id,
            name,
            reply,
        })?;
        match self.wait(&answer, Some(Instant::now() + timeout), interrupted) {
            Ok(blob) => Ok(blob),
            Err(Gave::Answer(error)) => Err(error),
            Err(Gave::Up(error)) => Err(self.abandon(id, Abandoned::Open(answer), error)),
        }
    }

    /// The bytes `[offset, offset + len)` of `blob`. `timeout` bounds the
    /// whole read.
    pub fn read_blob(
        &self,
        blob: &Blob,
        offset: u64,
        len: u64,
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<Vec<u8>, CallError> {
        let mut bytes = Vec::new();
        self.read_windows(blob, offset, len, timeout, interrupted, |window| {
            // Sized at the first window, once the range has been checked.
            if bytes.is_empty() {
                bytes = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
            }
            bytes.extend_from_slice(&window);
            Ok(())
        })?;
        Ok(bytes)
    }

    /// Write the whole of `blob` to a new file at `path`, a window at a time.
    /// An existing file is [`CallError::File`]; on any failure the file is
    /// removed. `timeout` bounds the whole read.
    pub fn read_blob_to_file(
        &self,
        blob: &Blob,
        path: &Path,
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<(), CallError> {
        self.own(blob)?;
        let failed = file_error(path);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(&failed)?;
        let written = self.read_windows(blob, 0, blob.size, timeout, interrupted, |window| {
            file.write_all(&window).map_err(&failed)
        });
        if written.is_err() {
            drop(file);
            let _ = std::fs::remove_file(path);
        }
        written
    }

    /// Ask for `[offset, offset + len)` of `blob` one window at a time,
    /// handing each to `sink` as it arrives. A window is the door's
    /// `max_chunk_bytes` times `max_inflight_reads`: it bounds what the
    /// supervisor buffers for this caller and what an abandoned read wastes.
    fn read_windows(
        &self,
        blob: &Blob,
        offset: u64,
        len: u64,
        timeout: Duration,
        interrupted: &mut dyn FnMut() -> bool,
        mut sink: impl FnMut(Vec<u8>) -> Result<(), CallError>,
    ) -> Result<(), CallError> {
        self.own(blob)?;
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= blob.size)
            .ok_or(CallError::ReadRefused(ReadRefusal::OutOfRange {
                size: blob.size,
            }))?;
        let deadline = Instant::now() + timeout;
        let mut at = offset;
        loop {
            let len = (end - at).min(self.window()?);
            let id = self.next_id();
            let (reply, answer) = mpsc::channel();
            self.send(Command::BlobRead {
                id,
                handle: blob.handle,
                offset: at,
                len,
                reply,
            })?;
            sink(self.await_reply(answer, Some(id), Some(deadline), interrupted)?)?;
            at += len;
            if at == end {
                return Ok(());
            }
        }
    }

    /// The read window of the most recent attachment's door. With no door,
    /// no blob of this client is held.
    fn window(&self) -> Result<u64, CallError> {
        let door = self
            .shared
            .lock()
            .facts
            .as_ref()
            .and_then(|facts| facts.blobs)
            .ok_or(CallError::BlobGone)?;
        Ok(door
            .max_chunk_bytes
            .saturating_mul(u64::from(door.max_inflight_reads))
            .max(1))
    }

    /// Give `blob` up, in any state. A blob already closed, or gone with its
    /// attachment, closes without error.
    pub fn close_blob(
        &self,
        blob: &Blob,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<(), CallError> {
        self.own(blob)?;
        let (reply, answer) = mpsc::channel();
        let closed = self
            .send(Command::BlobClose {
                handle: blob.handle,
                reply,
            })
            .and_then(|()| self.await_reply(answer, None, None, interrupted));
        match closed {
            Err(CallError::Ended(_)) => Ok(()),
            other => other,
        }
    }

    /// `blob` named `name`, for [`PublishRequest::attachments`]; another
    /// client's blob is [`CallError::ForeignBlob`].
    pub fn attachment(
        &self,
        blob: &Blob,
        name: AttachmentName,
    ) -> Result<PublishAttachment, CallError> {
        self.own(blob)?;
        Ok(PublishAttachment::new(name, blob.handle))
    }

    fn own(&self, blob: &Blob) -> Result<(), CallError> {
        if blob.client == self.id {
            Ok(())
        } else {
            Err(CallError::ForeignBlob)
        }
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
        deadline: Option<Instant>,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<T, CallError> {
        match self.wait(&answer, deadline, interrupted) {
            Ok(value) => Ok(value),
            Err(Gave::Answer(error)) => Err(error),
            Err(Gave::Up(error)) => {
                if let Some(id) = id {
                    let _ = self.commands.send(Command::Cancel { id });
                }
                Err(error)
            }
        }
    }

    /// Wait for one reply until `deadline` or until `interrupted` asks.
    fn wait<T>(
        &self,
        answer: &Receiver<Result<T, CallError>>,
        deadline: Option<Instant>,
        interrupted: &mut dyn FnMut() -> bool,
    ) -> Result<T, Gave> {
        loop {
            let wait = match deadline {
                None => WAIT_SLICE,
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(Gave::Up(CallError::Timeout));
                    }
                    WAIT_SLICE.min(deadline - now)
                }
            };
            match answer.recv_timeout(wait) {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(error)) => return Err(Gave::Answer(error)),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(Gave::Answer(CallError::Ended(self.await_end())));
                }
                Err(RecvTimeoutError::Timeout) => {
                    if interrupted() {
                        return Err(Gave::Up(CallError::Interrupted));
                    }
                }
            }
        }
    }

    /// Hand upload or open `id` and its reply channel back to the supervisor,
    /// which closes its handle and any blob an answer already sent made, and
    /// return at once. Answers `error`.
    fn abandon(&self, id: u64, answer: Abandoned, error: CallError) -> CallError {
        let _ = self.commands.send(Command::Abandon { id, answer });
        error
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

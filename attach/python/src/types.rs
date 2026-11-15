//! The module's value classes, and the conversion of every blocking-crate and
//! client-crate value onto them.
//!
//! Every conversion from an enum is a `match` with no wildcard arm, so a
//! variant added upstream fails this crate's build.

use brenn_attach_blocking::{
    AttachmentFacts as Facts, BlobFacts, Delivery as BlockingDelivery,
    DialFailure as BlockingDialFailure, Event as BlockingEvent, OpenRefusal,
    PublishOutcome as Outcome, ReadRefusal, SubscribeAnswer, UploadRefusal, describe_detach,
};
use brenn_attach_client::conn::DetachReason;
use brenn_attach_proto::{GapInfo, GapReason};
use brenn_envelope::{MessageBody, Urgency as EnvelopeUrgency};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

/// What one attachment's `Welcome` stated.
#[pyclass(module = "brenn_bus", name = "AttachmentFacts", frozen, eq, get_all)]
#[derive(PartialEq)]
pub struct PyFacts {
    protocol_version: u32,
    participant_id: String,
    session_id: String,
    max_body_bytes: u64,
    blobs: Option<PyBlobFacts>,
}

#[pymethods]
impl PyFacts {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(
            py,
            [
                "protocol_version",
                "participant_id",
                "session_id",
                "max_body_bytes",
                "blobs",
            ],
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "AttachmentFacts(protocol_version={}, participant_id={:?}, session_id={:?}, \
             max_body_bytes={}, blobs={})",
            self.protocol_version,
            self.participant_id,
            self.session_id,
            self.max_body_bytes,
            self.blobs
                .as_ref()
                .map_or_else(|| "None".to_string(), PyBlobFacts::__repr__)
        )
    }
}

pub fn facts(facts: &Facts) -> PyFacts {
    PyFacts {
        protocol_version: facts.version,
        participant_id: facts.participant_id.clone(),
        session_id: facts.session_id.clone(),
        max_body_bytes: facts.max_body_bytes,
        blobs: facts.blobs.as_ref().map(blob_facts),
    }
}

/// An attachment's blob door: the host's bounds on this session's blobs, and
/// whether it may upload.
#[pyclass(
    module = "brenn_bus",
    name = "BlobFacts",
    frozen,
    eq,
    get_all,
    from_py_object
)]
#[derive(Clone, PartialEq)]
pub struct PyBlobFacts {
    max_blob_bytes: u64,
    max_chunk_bytes: u64,
    max_handles: u32,
    max_bytes_held: u64,
    max_inflight_reads: u32,
    upload: bool,
}

#[pymethods]
impl PyBlobFacts {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(
            py,
            [
                "max_blob_bytes",
                "max_chunk_bytes",
                "max_handles",
                "max_bytes_held",
                "max_inflight_reads",
                "upload",
            ],
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "BlobFacts(max_blob_bytes={}, max_chunk_bytes={}, max_handles={}, \
             max_bytes_held={}, max_inflight_reads={}, upload={})",
            self.max_blob_bytes,
            self.max_chunk_bytes,
            self.max_handles,
            self.max_bytes_held,
            self.max_inflight_reads,
            if self.upload { "True" } else { "False" }
        )
    }
}

fn blob_facts(door: &BlobFacts) -> PyBlobFacts {
    let BlobFacts {
        max_blob_bytes,
        max_chunk_bytes,
        max_handles,
        max_bytes_held,
        max_inflight_reads,
        upload,
    } = *door;
    PyBlobFacts {
        max_blob_bytes,
        max_chunk_bytes,
        max_handles,
        max_bytes_held,
        max_inflight_reads,
        upload,
    }
}

/// A message's urgency.
#[pyclass(module = "brenn_bus", name = "Urgency", eq, eq_int, from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyUrgency {
    #[pyo3(name = "VERY_LOW")]
    VeryLow,
    #[pyo3(name = "LOW")]
    Low,
    #[pyo3(name = "NORMAL")]
    Normal,
    #[pyo3(name = "HIGH")]
    High,
}

impl From<PyUrgency> for EnvelopeUrgency {
    fn from(urgency: PyUrgency) -> Self {
        match urgency {
            PyUrgency::VeryLow => EnvelopeUrgency::VeryLow,
            PyUrgency::Low => EnvelopeUrgency::Low,
            PyUrgency::Normal => EnvelopeUrgency::Normal,
            PyUrgency::High => EnvelopeUrgency::High,
        }
    }
}

impl From<EnvelopeUrgency> for PyUrgency {
    fn from(urgency: EnvelopeUrgency) -> Self {
        match urgency {
            EnvelopeUrgency::VeryLow => PyUrgency::VeryLow,
            EnvelopeUrgency::Low => PyUrgency::Low,
            EnvelopeUrgency::Normal => PyUrgency::Normal,
            EnvelopeUrgency::High => PyUrgency::High,
        }
    }
}

/// A message body: UTF-8 text, or bytes with a content type.
#[pyclass(module = "brenn_bus", name = "Body", frozen, eq)]
#[derive(PartialEq)]
pub struct PyBody {
    pub body: MessageBody,
}

#[pymethods]
impl PyBody {
    /// Bytes with a content type, by the envelope's rules: untyped bytes, or
    /// bytes typed with a text type, must be UTF-8 text; any other type makes
    /// a non-text body.
    #[new]
    #[pyo3(signature = (data, content_type = None))]
    fn new(data: &[u8], content_type: Option<&str>) -> PyResult<Self> {
        MessageBody::from_parts(content_type, data)
            .map(|body| PyBody { body })
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }

    /// Untyped UTF-8 text.
    #[staticmethod]
    fn text(text: &str) -> Self {
        PyBody {
            body: MessageBody::text(text),
        }
    }

    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.body.as_bytes())
    }

    #[getter]
    fn content_type(&self) -> Option<&str> {
        self.body.content_type()
    }

    #[getter]
    fn is_text(&self) -> bool {
        self.body.is_text()
    }

    fn as_text(&self) -> Option<&str> {
        self.body.as_text()
    }

    fn __repr__(&self) -> String {
        format!("Body({})", self.body.summary())
    }
}

/// Why a resume claim could not be covered.
#[pyclass(module = "brenn_bus", name = "Gap", eq, eq_int, skip_from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyGap {
    #[pyo3(name = "EPOCH_CHANGED")]
    EpochChanged,
    #[pyo3(name = "BEYOND_RETAINED")]
    BeyondRetained,
}

fn gap(gap: &Option<GapInfo>) -> Option<PyGap> {
    gap.as_ref().map(|info| match info.reason {
        GapReason::EpochChanged => PyGap::EpochChanged,
        GapReason::BeyondRetained => PyGap::BeyondRetained,
    })
}

/// How an attachment dropped.
#[pyclass(
    module = "brenn_bus",
    name = "LossKind",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyLossKind {
    #[pyo3(name = "LIVENESS_TIMEOUT")]
    LivenessTimeout,
    #[pyo3(name = "TRANSPORT_CLOSED")]
    TransportClosed,
}

/// Why an attachment dropped. `code` is the peer's close code, or None;
/// `reason` is text to show, never to parse.
#[pyclass(module = "brenn_bus", name = "Loss", frozen, eq, get_all)]
#[derive(PartialEq)]
pub struct PyLoss {
    kind: PyLossKind,
    code: Option<u16>,
    reason: String,
}

#[pymethods]
impl PyLoss {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(py, ["kind", "code", "reason"])
    }

    fn __repr__(&self) -> String {
        let kind = match self.kind {
            PyLossKind::LivenessTimeout => "LIVENESS_TIMEOUT",
            PyLossKind::TransportClosed => "TRANSPORT_CLOSED",
        };
        format!(
            "Loss(kind=LossKind.{kind}, code={}, reason={:?})",
            self.code
                .map_or_else(|| "None".to_string(), |code| code.to_string()),
            self.reason
        )
    }
}

pub fn loss(reason: &DetachReason) -> PyLoss {
    match reason {
        DetachReason::LivenessTimeout => PyLoss {
            kind: PyLossKind::LivenessTimeout,
            code: None,
            reason: describe_detach(reason),
        },
        DetachReason::TransportClosed { code, reason } => PyLoss {
            kind: PyLossKind::TransportClosed,
            code: *code,
            reason: reason.clone(),
        },
    }
}

/// What one dial failed on.
#[pyclass(
    module = "brenn_bus",
    name = "DialFailureKind",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyDialFailureKind {
    #[pyo3(name = "IO")]
    Io,
    #[pyo3(name = "TLS")]
    Tls,
    #[pyo3(name = "UPGRADE_REJECTED")]
    UpgradeRejected,
    #[pyo3(name = "HANDSHAKE")]
    Handshake,
    #[pyo3(name = "TIMED_OUT")]
    TimedOut,
}

/// Why one dial reached no attachment. `status` is set for
/// `UPGRADE_REJECTED`, `detail` for `IO`, `TLS` and `HANDSHAKE`.
#[pyclass(module = "brenn_bus", name = "DialFailure", frozen, eq, get_all)]
#[derive(PartialEq)]
pub struct PyDialFailure {
    kind: PyDialFailureKind,
    status: Option<u16>,
    detail: Option<String>,
}

#[pymethods]
impl PyDialFailure {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(py, ["kind", "status", "detail"])
    }

    fn __repr__(&self) -> String {
        let kind = match self.kind {
            PyDialFailureKind::Io => "IO",
            PyDialFailureKind::Tls => "TLS",
            PyDialFailureKind::UpgradeRejected => "UPGRADE_REJECTED",
            PyDialFailureKind::Handshake => "HANDSHAKE",
            PyDialFailureKind::TimedOut => "TIMED_OUT",
        };
        format!(
            "DialFailure(kind=DialFailureKind.{kind}, status={}, detail={})",
            self.status
                .map_or_else(|| "None".to_string(), |status| status.to_string()),
            self.detail
                .as_deref()
                .map_or_else(|| "None".to_string(), |detail| format!("{detail:?}"))
        )
    }
}

pub fn dial_failure(failure: &BlockingDialFailure) -> PyDialFailure {
    let (kind, status, detail) = match failure {
        BlockingDialFailure::Io { detail } => (PyDialFailureKind::Io, None, Some(detail.clone())),
        BlockingDialFailure::Tls { detail } => (PyDialFailureKind::Tls, None, Some(detail.clone())),
        BlockingDialFailure::UpgradeRejected { status } => {
            (PyDialFailureKind::UpgradeRejected, Some(*status), None)
        }
        BlockingDialFailure::Handshake { detail } => {
            (PyDialFailureKind::Handshake, None, Some(detail.clone()))
        }
        BlockingDialFailure::TimedOut => (PyDialFailureKind::TimedOut, None, None),
    };
    PyDialFailure {
        kind,
        status,
        detail,
    }
}

/// The base of the server's answers to a publish.
#[pyclass(module = "brenn_bus", name = "PublishOutcome", frozen, subclass)]
pub struct PyPublishOutcome;

/// The publish was stored.
#[pyclass(module = "brenn_bus", name = "Ok", frozen, extends = PyPublishOutcome)]
pub struct PyOk;

#[pymethods]
impl PyOk {
    fn __repr__(&self) -> &'static str {
        "Ok()"
    }
}

/// The publish failed on a path the server declares non-fatal.
#[pyclass(module = "brenn_bus", name = "Failed", frozen, extends = PyPublishOutcome)]
pub struct PyFailed;

#[pymethods]
impl PyFailed {
    fn __repr__(&self) -> &'static str {
        "Failed()"
    }
}

/// The sender's server-side budget refused the publish.
#[pyclass(module = "brenn_bus", name = "RateLimited", frozen, extends = PyPublishOutcome)]
pub struct PyRateLimited;

#[pymethods]
impl PyRateLimited {
    fn __repr__(&self) -> &'static str {
        "RateLimited()"
    }
}

/// The body is `len` bytes and the server takes at most `max`.
#[pyclass(module = "brenn_bus", name = "BodyTooLarge", frozen, get_all, extends = PyPublishOutcome)]
pub struct PyBodyTooLarge {
    len: u64,
    max: u64,
}

#[pymethods]
impl PyBodyTooLarge {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(py, ["len", "max"])
    }

    fn __repr__(&self) -> String {
        format!("BodyTooLarge(len={}, max={})", self.len, self.max)
    }
}

/// The body is not text and the channel carries only text. Nothing was stored.
#[pyclass(module = "brenn_bus", name = "TextOnlyChannel", frozen, extends = PyPublishOutcome)]
pub struct PyTextOnlyChannel;

#[pymethods]
impl PyTextOnlyChannel {
    fn __repr__(&self) -> &'static str {
        "TextOnlyChannel()"
    }
}

/// A blob the publish names was closed, refused or never sealed. Nothing was
/// stored.
#[pyclass(module = "brenn_bus", name = "AttachmentNotHeld", frozen, extends = PyPublishOutcome)]
pub struct PyAttachmentNotHeld;

#[pymethods]
impl PyAttachmentNotHeld {
    fn __repr__(&self) -> &'static str {
        "AttachmentNotHeld()"
    }
}

pub fn publish_outcome(py: Python<'_>, outcome: Outcome) -> PyResult<Py<PyAny>> {
    let base = PyClassInitializer::from(PyPublishOutcome);
    let made = match outcome {
        Outcome::Ok => Bound::new(py, base.add_subclass(PyOk))?.into_any(),
        Outcome::Failed => Bound::new(py, base.add_subclass(PyFailed))?.into_any(),
        Outcome::RateLimited => Bound::new(py, base.add_subclass(PyRateLimited))?.into_any(),
        Outcome::BodyTooLarge { len, max } => {
            Bound::new(py, base.add_subclass(PyBodyTooLarge { len, max }))?.into_any()
        }
        Outcome::TextOnlyChannel => {
            Bound::new(py, base.add_subclass(PyTextOnlyChannel))?.into_any()
        }
        Outcome::AttachmentNotHeld => {
            Bound::new(py, base.add_subclass(PyAttachmentNotHeld))?.into_any()
        }
    };
    Ok(made.unbind())
}

/// The subscription is open: `replay_count` retained messages follow, and
/// `gap` says the resume claim could not be covered.
#[pyclass(module = "brenn_bus", name = "Subscribed", frozen, eq, get_all)]
#[derive(PartialEq)]
pub struct PySubscribed {
    replay_count: u32,
    gap: Option<PyGap>,
}

#[pymethods]
impl PySubscribed {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(py, ["replay_count", "gap"])
    }

    fn __repr__(&self) -> String {
        format!(
            "Subscribed(replay_count={}, gap={})",
            self.replay_count,
            gap_repr(self.gap)
        )
    }
}

/// The channel is inside the grant but not there now. Nothing is held.
#[pyclass(module = "brenn_bus", name = "Unavailable", frozen, eq)]
#[derive(PartialEq)]
pub struct PyUnavailable;

#[pymethods]
impl PyUnavailable {
    fn __repr__(&self) -> &'static str {
        "Unavailable()"
    }
}

fn gap_repr(gap: Option<PyGap>) -> &'static str {
    match gap {
        None => "None",
        Some(PyGap::EpochChanged) => "Gap.EPOCH_CHANGED",
        Some(PyGap::BeyondRetained) => "Gap.BEYOND_RETAINED",
    }
}

pub fn subscribe_answer(py: Python<'_>, answer: SubscribeAnswer) -> PyResult<Py<PyAny>> {
    Ok(match answer {
        SubscribeAnswer::Subscribed {
            replay_count,
            gap: g,
        } => Bound::new(
            py,
            PySubscribed {
                replay_count,
                gap: gap(&g),
            },
        )?
        .into_any()
        .unbind(),
        SubscribeAnswer::Unavailable => Bound::new(py, PyUnavailable)?.into_any().unbind(),
    })
}

/// The base of everything `Client.recv` returns.
#[pyclass(module = "brenn_bus", name = "Event", frozen, subclass)]
pub struct PyEvent;

/// One message delivered on a held channel. `seq` restarts at 1 with each
/// subscription span; `dropped` is how many messages the channel lost before
/// this one.
#[pyclass(module = "brenn_bus", name = "Delivery", frozen, get_all, extends = PyEvent)]
pub struct PyDelivery {
    channel: String,
    message_id: String,
    sender: String,
    publish_ts: Py<PyAny>,
    reply_to: Option<String>,
    urgency: PyUrgency,
    body: Py<PyBody>,
    seq: u64,
    dropped: u64,
    attachments: Py<PyTuple>,
}

#[pymethods]
impl PyDelivery {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(
            py,
            [
                "channel",
                "message_id",
                "sender",
                "publish_ts",
                "reply_to",
                "urgency",
                "body",
                "seq",
                "dropped",
                "attachments",
            ],
        )
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let names = self
            .attachments
            .bind(py)
            .iter()
            .map(|attachment| {
                Ok(format!(
                    "{:?}",
                    attachment.cast::<PyAttachment>()?.get().name
                ))
            })
            .collect::<PyResult<Vec<String>>>()?;
        Ok(format!(
            "Delivery(channel={:?}, message_id={:?}, sender={:?}, seq={}, dropped={}, body={}, \
             attachments=[{}])",
            self.channel,
            self.message_id,
            self.sender,
            self.seq,
            self.dropped,
            self.body.get().__repr__(),
            names.join(", "),
        ))
    }
}

/// One attachment a delivered message names: its `name` in the message, and
/// the blob's `size` in bytes and `content_type`. Open it with
/// `Client.open_attachment`.
#[pyclass(module = "brenn_bus", name = "Attachment", frozen, eq, get_all)]
#[derive(PartialEq)]
pub struct PyAttachment {
    name: String,
    size: u64,
    content_type: String,
}

#[pymethods]
impl PyAttachment {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(py, ["name", "size", "content_type"])
    }

    fn __repr__(&self) -> String {
        format!(
            "Attachment(name={:?}, size={}, content_type={:?})",
            self.name, self.size, self.content_type
        )
    }
}

/// Why an upload made no blob.
#[pyclass(
    module = "brenn_bus",
    name = "UploadRefusal",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyUploadRefusal {
    #[pyo3(name = "BLOB_TOO_LARGE")]
    BlobTooLarge,
    #[pyo3(name = "BUDGET_EXHAUSTED")]
    BudgetExhausted,
    #[pyo3(name = "HANDLE_LIMIT")]
    HandleLimit,
    #[pyo3(name = "BYTES_HELD_LIMIT")]
    BytesHeldLimit,
    #[pyo3(name = "STORAGE_FAILED")]
    StorageFailed,
    #[pyo3(name = "INCOMPLETE")]
    Incomplete,
}

/// An upload refusal's member and the values it carries: `size`, `max`,
/// `offset`, `len`, each None unless the refusal carries it.
pub struct UploadFields {
    pub reason: PyUploadRefusal,
    pub size: Option<u64>,
    pub max: Option<u64>,
    pub offset: Option<u64>,
    pub len: Option<u64>,
}

pub fn upload_refusal(refusal: UploadRefusal) -> UploadFields {
    let bare = |reason| UploadFields {
        reason,
        size: None,
        max: None,
        offset: None,
        len: None,
    };
    match refusal {
        UploadRefusal::BlobTooLarge { size, max } => UploadFields {
            size: Some(size),
            max: Some(max),
            ..bare(PyUploadRefusal::BlobTooLarge)
        },
        UploadRefusal::BudgetExhausted => bare(PyUploadRefusal::BudgetExhausted),
        UploadRefusal::HandleLimit => bare(PyUploadRefusal::HandleLimit),
        UploadRefusal::BytesHeldLimit => bare(PyUploadRefusal::BytesHeldLimit),
        UploadRefusal::StorageFailed => bare(PyUploadRefusal::StorageFailed),
        UploadRefusal::Incomplete { offset, len } => UploadFields {
            offset: Some(offset),
            len: Some(len),
            ..bare(PyUploadRefusal::Incomplete)
        },
    }
}

/// Why an open made no blob.
#[pyclass(
    module = "brenn_bus",
    name = "OpenRefusal",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyOpenRefusal {
    #[pyo3(name = "NOT_RETAINED")]
    NotRetained,
    #[pyo3(name = "NO_SUCH_ATTACHMENT")]
    NoSuchAttachment,
    #[pyo3(name = "TOOL_CHANNEL")]
    ToolChannel,
    #[pyo3(name = "HANDLE_LIMIT")]
    HandleLimit,
    #[pyo3(name = "BYTES_HELD_LIMIT")]
    BytesHeldLimit,
}

/// An open refusal's member and the blob's `size`, set for `BYTES_HELD_LIMIT`.
pub fn open_refusal(refusal: OpenRefusal) -> (PyOpenRefusal, Option<u64>) {
    match refusal {
        OpenRefusal::NotRetained => (PyOpenRefusal::NotRetained, None),
        OpenRefusal::NoSuchAttachment => (PyOpenRefusal::NoSuchAttachment, None),
        OpenRefusal::ToolChannel => (PyOpenRefusal::ToolChannel, None),
        OpenRefusal::HandleLimit => (PyOpenRefusal::HandleLimit, None),
        OpenRefusal::BytesHeldLimit { size } => (PyOpenRefusal::BytesHeldLimit, Some(size)),
    }
}

/// Why a read ended before its last byte.
#[pyclass(
    module = "brenn_bus",
    name = "ReadRefusal",
    eq,
    eq_int,
    skip_from_py_object
)]
#[derive(Clone, Copy, PartialEq)]
pub enum PyReadRefusal {
    #[pyo3(name = "OUT_OF_RANGE")]
    OutOfRange,
    #[pyo3(name = "UNKNOWN_HANDLE")]
    UnknownHandle,
    #[pyo3(name = "NOT_SEALED")]
    NotSealed,
    #[pyo3(name = "TOO_MANY_READS")]
    TooManyReads,
    #[pyo3(name = "STORAGE_FAILED")]
    StorageFailed,
    #[pyo3(name = "CLOSED")]
    Closed,
}

/// A read refusal's member and the blob's `size`, set for `OUT_OF_RANGE`.
pub fn read_refusal(refusal: ReadRefusal) -> (PyReadRefusal, Option<u64>) {
    match refusal {
        ReadRefusal::OutOfRange { size } => (PyReadRefusal::OutOfRange, Some(size)),
        ReadRefusal::UnknownHandle => (PyReadRefusal::UnknownHandle, None),
        ReadRefusal::NotSealed => (PyReadRefusal::NotSealed, None),
        ReadRefusal::TooManyReads => (PyReadRefusal::TooManyReads, None),
        ReadRefusal::StorageFailed => (PyReadRefusal::StorageFailed, None),
        ReadRefusal::Closed => (PyReadRefusal::Closed, None),
    }
}

/// A held channel was re-sent at a new attachment and the server opened it.
#[pyclass(module = "brenn_bus", name = "Resubscribed", frozen, get_all, extends = PyEvent)]
pub struct PyResubscribed {
    channel: String,
    replay_count: u32,
    gap: Option<PyGap>,
}

#[pymethods]
impl PyResubscribed {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(py, ["channel", "replay_count", "gap"])
    }

    fn __repr__(&self) -> String {
        format!(
            "Resubscribed(channel={:?}, replay_count={}, gap={})",
            self.channel,
            self.replay_count,
            gap_repr(self.gap)
        )
    }
}

/// A held channel was answered `Unavailable` on resubscribe; the hold is gone.
#[pyclass(module = "brenn_bus", name = "SubscriptionDropped", frozen, get_all, extends = PyEvent)]
pub struct PySubscriptionDropped {
    channel: String,
}

#[pymethods]
impl PySubscriptionDropped {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(py, ["channel"])
    }

    fn __repr__(&self) -> String {
        format!("SubscriptionDropped(channel={:?})", self.channel)
    }
}

/// The attachment is live; `facts` are its own.
#[pyclass(module = "brenn_bus", name = "Attached", frozen, get_all, extends = PyEvent)]
pub struct PyAttached {
    facts: Py<PyFacts>,
}

#[pymethods]
impl PyAttached {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(py, ["facts"])
    }

    fn __repr__(&self) -> String {
        format!("Attached({})", self.facts.get().__repr__())
    }
}

/// No live attachment; reconnection is under way. `loss` is the last drop's,
/// `last_dial_failure` the latest dial's since.
#[pyclass(module = "brenn_bus", name = "Detached", frozen, get_all, extends = PyEvent)]
pub struct PyDetached {
    loss: Py<PyLoss>,
    last_dial_failure: Option<Py<PyDialFailure>>,
}

#[pymethods]
impl PyDetached {
    #[classattr]
    fn __match_args__(py: Python<'_>) -> PyResult<Bound<'_, PyTuple>> {
        PyTuple::new(py, ["loss", "last_dial_failure"])
    }

    fn __repr__(&self) -> String {
        format!(
            "Detached(loss={}, last_dial_failure={})",
            self.loss.get().__repr__(),
            self.last_dial_failure
                .as_ref()
                .map_or_else(|| "None".to_string(), |failure| failure.get().__repr__())
        )
    }
}

pub fn attached(py: Python<'_>, f: &Facts) -> PyResult<Py<PyAny>> {
    let made = PyClassInitializer::from(PyEvent).add_subclass(PyAttached {
        facts: Py::new(py, facts(f))?,
    });
    Ok(Bound::new(py, made)?.into_any().unbind())
}

pub fn detached(
    py: Python<'_>,
    reason: &DetachReason,
    last_dial_failure: &Option<BlockingDialFailure>,
) -> PyResult<Py<PyAny>> {
    let last_dial_failure = match last_dial_failure {
        Some(failure) => Some(Py::new(py, dial_failure(failure))?),
        None => None,
    };
    let made = PyClassInitializer::from(PyEvent).add_subclass(PyDetached {
        loss: Py::new(py, loss(reason))?,
        last_dial_failure,
    });
    Ok(Bound::new(py, made)?.into_any().unbind())
}

fn delivery(py: Python<'_>, delivery: BlockingDelivery) -> PyResult<Py<PyAny>> {
    let BlockingDelivery {
        channel,
        envelope,
        seq,
        dropped,
    } = delivery;
    let attachments = PyTuple::new(
        py,
        envelope
            .attachments
            .iter()
            .map(|(name, blob)| {
                Py::new(
                    py,
                    PyAttachment {
                        name: name.as_str().to_string(),
                        size: blob.size,
                        content_type: blob.content_type.clone(),
                    },
                )
            })
            .collect::<PyResult<Vec<_>>>()?,
    )?
    .unbind();
    let made = PyClassInitializer::from(PyEvent).add_subclass(PyDelivery {
        channel,
        message_id: envelope.message_id.to_string(),
        sender: envelope.sender,
        publish_ts: envelope.publish_ts.into_pyobject(py)?.into_any().unbind(),
        reply_to: envelope.reply_to,
        urgency: envelope.urgency.into(),
        body: Py::new(
            py,
            PyBody {
                body: envelope.body,
            },
        )?,
        seq,
        dropped,
        attachments,
    });
    Ok(Bound::new(py, made)?.into_any().unbind())
}

pub fn event(py: Python<'_>, event: BlockingEvent) -> PyResult<Py<PyAny>> {
    let base = PyClassInitializer::from(PyEvent);
    Ok(match event {
        BlockingEvent::Delivery(boxed) => delivery(py, *boxed)?,
        BlockingEvent::Resubscribed {
            channel,
            replay_count,
            gap: g,
        } => Bound::new(
            py,
            base.add_subclass(PyResubscribed {
                channel,
                replay_count,
                gap: gap(&g),
            }),
        )?
        .into_any()
        .unbind(),
        BlockingEvent::SubscriptionDropped { channel } => {
            Bound::new(py, base.add_subclass(PySubscriptionDropped { channel }))?
                .into_any()
                .unbind()
        }
        BlockingEvent::Attached(f) => attached(py, &f)?,
        BlockingEvent::Detached {
            reason,
            last_dial_failure,
        } => detached(py, &reason, &last_dial_failure)?,
    })
}

/// Add every value class to the module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyFacts>()?;
    m.add_class::<PyBlobFacts>()?;
    m.add_class::<PyAttachment>()?;
    m.add_class::<PyUploadRefusal>()?;
    m.add_class::<PyOpenRefusal>()?;
    m.add_class::<PyReadRefusal>()?;
    m.add_class::<PyAttachmentNotHeld>()?;
    m.add_class::<PyUrgency>()?;
    m.add_class::<PyBody>()?;
    m.add_class::<PyGap>()?;
    m.add_class::<PyLossKind>()?;
    m.add_class::<PyLoss>()?;
    m.add_class::<PyDialFailureKind>()?;
    m.add_class::<PyDialFailure>()?;
    m.add_class::<PyPublishOutcome>()?;
    m.add_class::<PyOk>()?;
    m.add_class::<PyFailed>()?;
    m.add_class::<PyRateLimited>()?;
    m.add_class::<PyBodyTooLarge>()?;
    m.add_class::<PyTextOnlyChannel>()?;
    m.add_class::<PySubscribed>()?;
    m.add_class::<PyUnavailable>()?;
    m.add_class::<PyEvent>()?;
    m.add_class::<PyDelivery>()?;
    m.add_class::<PyResubscribed>()?;
    m.add_class::<PySubscriptionDropped>()?;
    m.add_class::<PyAttached>()?;
    m.add_class::<PyDetached>()?;
    Ok(())
}

#[cfg(test)]
mod tests;

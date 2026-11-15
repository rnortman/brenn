//! The module's exceptions, and the mapping of every blocking-crate failure
//! onto them.
//!
//! Attributes an exception carries (`last_failure`, `loss`, `status`, …) are
//! set on the instance when it is raised.

use brenn_attach_blocking::handle::AttachError;
use brenn_attach_blocking::{CallError, EndCause, RecvError};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};

use crate::types::{dial_failure, loss, open_refusal, read_refusal, upload_refusal};

create_exception!(
    brenn_bus,
    BusError,
    PyException,
    "The base of every error the bus raises."
);
create_exception!(
    brenn_bus,
    AttachTimeout,
    BusError,
    "No attachment within the timeout. `last_failure` is the most recent dial's DialFailure, or None."
);
create_exception!(
    brenn_bus,
    NotAttached,
    BusError,
    "A publish or subscribe was made with no live attachment."
);
create_exception!(
    brenn_bus,
    ConnectionLost,
    BusError,
    "The attachment dropped before the server answered; `loss` says why. A publish may or may not \
     have landed. A subscribe or publish the server does not allow looks like this, with \
     `loss.code` None."
);
create_exception!(
    brenn_bus,
    AnswerTimeout,
    BusError,
    "No answer within the timeout; the attachment may still be up."
);
create_exception!(
    brenn_bus,
    AttachmentEnded,
    BusError,
    "The attachment is over for good; every later call and read raises the same."
);
create_exception!(
    brenn_bus,
    CredentialRejected,
    AttachmentEnded,
    "The server rejected the credential on the upgrade with HTTP `status` (401 or 403)."
);
create_exception!(
    brenn_bus,
    IncompatibleVersions,
    AttachmentEnded,
    "The two ends share no protocol version; `ours` and `theirs` are (min, max) ranges."
);
create_exception!(
    brenn_bus,
    ProtocolError,
    AttachmentEnded,
    "A server frame could not be reconciled with the protocol; `detail` says how."
);
create_exception!(
    brenn_bus,
    Futile,
    AttachmentEnded,
    "`attachments` consecutive attachments sent something and were answered nothing: the server \
     is refusing what this client sends."
);
create_exception!(
    brenn_bus,
    ClientClosed,
    AttachmentEnded,
    "The client was closed."
);
create_exception!(
    brenn_bus,
    SupervisorPanicked,
    AttachmentEnded,
    "The thread that supervises the attachment panicked; `message` is the panic's."
);

create_exception!(
    brenn_bus,
    BlobsUnavailable,
    BusError,
    "The live attachment has no blob door: the server keeps no blob store."
);
create_exception!(
    brenn_bus,
    UploadNotGranted,
    BusError,
    "The blob door grants no upload: this remote may not publish, so may not upload."
);
create_exception!(
    brenn_bus,
    UploadRefused,
    BusError,
    "The upload made no blob. `reason` is an UploadRefusal; `size`, `max`, `offset` and `len` \
     are None unless the reason carries them."
);
create_exception!(
    brenn_bus,
    OpenRefused,
    BusError,
    "The open made no blob. `reason` is an OpenRefusal; `size` is set for BYTES_HELD_LIMIT."
);
create_exception!(
    brenn_bus,
    ReadRefused,
    BusError,
    "The read ended before its last byte. `reason` is a ReadRefusal; `size` is set for \
     OUT_OF_RANGE."
);
create_exception!(
    brenn_bus,
    BlobGone,
    BusError,
    "The blob was closed, or the attachment that held it has ended."
);

static ALREADY_SUBSCRIBED: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static NOT_SUBSCRIBED: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// `AlreadySubscribed`: a `BusError` and a `ValueError`.
pub fn already_subscribed(py: Python<'_>) -> PyResult<Bound<'_, PyType>> {
    caller_error_type(
        py,
        &ALREADY_SUBSCRIBED,
        "AlreadySubscribed",
        "The channel is already held by this client, or its subscribe is unanswered.",
    )
}

/// `NotSubscribed`: a `BusError` and a `ValueError`.
pub fn not_subscribed(py: Python<'_>) -> PyResult<Bound<'_, PyType>> {
    caller_error_type(
        py,
        &NOT_SUBSCRIBED,
        "NotSubscribed",
        "The channel is not held by this client.",
    )
}

/// A class deriving from both `BusError` and `ValueError`, made once per
/// interpreter by calling `type`: an exception made in Rust takes one base.
fn caller_error_type<'py>(
    py: Python<'py>,
    cell: &PyOnceLock<Py<PyType>>,
    name: &str,
    doc: &str,
) -> PyResult<Bound<'py, PyType>> {
    let ty = cell.get_or_try_init(py, || -> PyResult<Py<PyType>> {
        let bases = PyTuple::new(
            py,
            [
                py.get_type::<BusError>().into_any(),
                py.get_type::<PyValueError>().into_any(),
            ],
        )?;
        let namespace = PyDict::new(py);
        namespace.set_item("__module__", "brenn_bus")?;
        namespace.set_item("__doc__", doc)?;
        let made = py
            .get_type::<PyType>()
            .call1((name, bases, namespace))?
            .cast_into::<PyType>()?;
        Ok(made.unbind())
    })?;
    Ok(ty.bind(py).clone())
}

/// An instance of `ty` with `message` as its argument and `attrs` set on it.
fn raise(ty: &Bound<'_, PyType>, message: String, attrs: &[(&str, Bound<'_, PyAny>)]) -> PyErr {
    let value = match ty.call1((message,)) {
        Ok(value) => value,
        Err(err) => return err,
    };
    for (name, attr) in attrs {
        if let Err(err) = value.setattr(*name, attr) {
            return err;
        }
    }
    PyErr::from_value(value)
}

/// The exception for an attachment that is over.
pub fn ended(py: Python<'_>, cause: &EndCause) -> PyErr {
    let message = cause.to_string();
    let built = || -> PyResult<PyErr> {
        Ok(match cause {
            EndCause::Closed => raise(&py.get_type::<ClientClosed>(), message, &[]),
            EndCause::Fatal { detail } => raise(
                &py.get_type::<ProtocolError>(),
                message,
                &[("detail", detail.into_pyobject(py)?.into_any())],
            ),
            EndCause::Incompatible { ours, theirs } => raise(
                &py.get_type::<IncompatibleVersions>(),
                message,
                &[
                    ("ours", (ours.min, ours.max).into_pyobject(py)?.into_any()),
                    (
                        "theirs",
                        (theirs.min, theirs.max).into_pyobject(py)?.into_any(),
                    ),
                ],
            ),
            EndCause::Futile { attachments } => raise(
                &py.get_type::<Futile>(),
                message,
                &[("attachments", attachments.into_pyobject(py)?.into_any())],
            ),
            EndCause::CredentialRejected { status } => raise(
                &py.get_type::<CredentialRejected>(),
                message,
                &[("status", status.into_pyobject(py)?.into_any())],
            ),
            EndCause::Panicked { message: panic } => raise(
                &py.get_type::<SupervisorPanicked>(),
                message,
                &[("message", panic.into_pyobject(py)?.into_any())],
            ),
        })
    };
    built().unwrap_or_else(|err| err)
}

/// The exception for a call that produced no answer. `CallError::Interrupted`
/// never reaches here: the pending signal's own exception is raised instead.
pub fn call(py: Python<'_>, error: CallError) -> PyErr {
    let message = error.to_string();
    let built = || -> PyResult<PyErr> {
        Ok(match error {
            CallError::NotAttached => raise(&py.get_type::<NotAttached>(), message, &[]),
            CallError::ConnectionLost { reason } => raise(
                &py.get_type::<ConnectionLost>(),
                message,
                &[("loss", Bound::new(py, loss(&reason))?.into_any())],
            ),
            CallError::Timeout => raise(&py.get_type::<AnswerTimeout>(), message, &[]),
            CallError::Interrupted => {
                unreachable!("an interrupted call raises the pending signal's exception")
            }
            CallError::Ended(cause) => ended(py, &cause),
            CallError::AlreadySubscribed => raise(&already_subscribed(py)?, message, &[]),
            CallError::NotSubscribed => raise(&not_subscribed(py)?, message, &[]),
            CallError::LocalChannel
            | CallError::NoDepth
            | CallError::ForeignBlob
            | CallError::ContentType(_) => PyValueError::new_err(message),
            // TODO(test-blobs-unavailable-never-raised-from-binding): no test
            // raises `BlobsUnavailable` through the module.
            CallError::NoBlobDoor => raise(&py.get_type::<BlobsUnavailable>(), message, &[]),
            CallError::UploadNotGranted => raise(&py.get_type::<UploadNotGranted>(), message, &[]),
            CallError::BlobGone => raise(&py.get_type::<BlobGone>(), message, &[]),
            CallError::UploadRefused(refusal) => {
                let fields = upload_refusal(refusal);
                raise(
                    &py.get_type::<UploadRefused>(),
                    message,
                    &[
                        ("reason", Bound::new(py, fields.reason)?.into_any()),
                        ("size", fields.size.into_pyobject(py)?.into_any()),
                        ("max", fields.max.into_pyobject(py)?.into_any()),
                        ("offset", fields.offset.into_pyobject(py)?.into_any()),
                        ("len", fields.len.into_pyobject(py)?.into_any()),
                    ],
                )
            }
            CallError::OpenRefused(refusal) => {
                let (reason, size) = open_refusal(refusal);
                raise(
                    &py.get_type::<OpenRefused>(),
                    message,
                    &[
                        ("reason", Bound::new(py, reason)?.into_any()),
                        ("size", size.into_pyobject(py)?.into_any()),
                    ],
                )
            }
            CallError::ReadRefused(refusal) => {
                let (reason, size) = read_refusal(refusal);
                raise(
                    &py.get_type::<ReadRefused>(),
                    message,
                    &[
                        ("reason", Bound::new(py, reason)?.into_any()),
                        ("size", size.into_pyobject(py)?.into_any()),
                    ],
                )
            }
            // `OSError(errno, strerror, filename)` becomes the subclass the
            // number names (`FileExistsError`, `FileNotFoundError`, …); the
            // filename is a `str`, as `open` gives it.
            CallError::File { path, error } => {
                PyOSError::new_err((error.os_error, error.detail, path.into_os_string()))
            }
            CallError::Fault { .. } => PyRuntimeError::new_err(message),
        })
    };
    built().unwrap_or_else(|err| err)
}

/// The exception for an attach that returned no client.
pub fn attach(py: Python<'_>, error: AttachError) -> PyErr {
    let message = error.to_string();
    let built = || -> PyResult<PyErr> {
        Ok(match error {
            AttachError::Invalid { detail } => PyValueError::new_err(detail),
            AttachError::Timeout { last_failure } => {
                let last_failure = match &last_failure {
                    Some(failure) => Bound::new(py, dial_failure(failure))?.into_any(),
                    None => py.None().into_bound(py),
                };
                raise(
                    &py.get_type::<AttachTimeout>(),
                    message,
                    &[("last_failure", last_failure)],
                )
            }
            AttachError::Ended(cause) => ended(py, &cause),
            AttachError::Interrupted => {
                unreachable!("an interrupted attach raises the pending signal's exception")
            }
        })
    };
    built().unwrap_or_else(|err| err)
}

/// The exception for a read that returned no event.
pub fn recv(py: Python<'_>, error: RecvError) -> PyErr {
    match error {
        RecvError::Ended(cause) => ended(py, &cause),
        RecvError::Interrupted => {
            unreachable!("an interrupted read raises the pending signal's exception")
        }
    }
}

/// Add every exception class to the module.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("BusError", py.get_type::<BusError>())?;
    m.add("AttachTimeout", py.get_type::<AttachTimeout>())?;
    m.add("NotAttached", py.get_type::<NotAttached>())?;
    m.add("ConnectionLost", py.get_type::<ConnectionLost>())?;
    m.add("AnswerTimeout", py.get_type::<AnswerTimeout>())?;
    m.add("AttachmentEnded", py.get_type::<AttachmentEnded>())?;
    m.add("CredentialRejected", py.get_type::<CredentialRejected>())?;
    m.add(
        "IncompatibleVersions",
        py.get_type::<IncompatibleVersions>(),
    )?;
    m.add("ProtocolError", py.get_type::<ProtocolError>())?;
    m.add("Futile", py.get_type::<Futile>())?;
    m.add("ClientClosed", py.get_type::<ClientClosed>())?;
    m.add("SupervisorPanicked", py.get_type::<SupervisorPanicked>())?;
    m.add("BlobsUnavailable", py.get_type::<BlobsUnavailable>())?;
    m.add("UploadNotGranted", py.get_type::<UploadNotGranted>())?;
    m.add("UploadRefused", py.get_type::<UploadRefused>())?;
    m.add("OpenRefused", py.get_type::<OpenRefused>())?;
    m.add("ReadRefused", py.get_type::<ReadRefused>())?;
    m.add("BlobGone", py.get_type::<BlobGone>())?;
    m.add("AlreadySubscribed", already_subscribed(py)?)?;
    m.add("NotSubscribed", not_subscribed(py)?)?;
    Ok(())
}

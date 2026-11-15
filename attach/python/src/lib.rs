//! `brenn_bus`: the brenn bus attachment as a CPython extension module.
//!
//! A mapping of `brenn-attach-blocking` onto Python classes and exceptions,
//! and nothing else. Every blocking call releases the GIL and, on the main
//! thread, takes it back at least every 100 ms to run the interpreter's
//! signal check, so Ctrl-C interrupts any wait: the pending signal's
//! exception is raised, and a request the wait abandoned is cancelled exactly
//! as a timeout cancels it.

use std::fs::File;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use brenn_attach_blocking::{
    AttachParams, AttachmentName, Blob, BlockingClient, CallError, PublishRequest, ResumePolicy,
    Status, SubscriptionDepths, Uuid,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyMapping, PyTuple};

mod errors;
mod types;

use types::{PyBody, PyUrgency};

/// Run `call` with the GIL released, handing it an interrupt check that, on
/// the main thread, takes the GIL to run the interpreter's signal handlers.
/// CPython runs them on the main thread only, so on any other thread the
/// check answers false without taking the GIL. Returns the call's result, or
/// the exception a signal handler raised.
fn blocking<T: Send>(
    py: Python<'_>,
    call: impl FnOnce(&mut dyn FnMut() -> bool) -> T + Send,
) -> PyResult<T> {
    let main = on_main_thread(py)?;
    let (result, signal) = py.detach(|| {
        let mut signal: Option<PyErr> = None;
        let mut interrupted = || {
            if !main {
                return false;
            }
            Python::attach(|py| match py.check_signals() {
                Ok(()) => false,
                Err(err) => {
                    signal = Some(err);
                    true
                }
            })
        };
        let result = call(&mut interrupted);
        (result, signal)
    });
    match signal {
        Some(err) => Err(err),
        None => Ok(result),
    }
}

/// Whether the calling thread is the interpreter's main thread, the one
/// thread signal handlers run on. From 3.13, `threading.main_thread()` is that
/// thread whichever thread first imported `threading`.
fn on_main_thread(py: Python<'_>) -> PyResult<bool> {
    let threading = py.import("threading")?;
    let main = threading.call_method0("main_thread")?.getattr("ident")?;
    main.eq(threading.call_method0("get_ident")?)
}

/// A timeout in seconds, refused unless finite and not negative.
fn seconds(value: f64) -> PyResult<Duration> {
    Duration::try_from_secs_f64(value).map_err(|_| {
        PyValueError::new_err(format!(
            "timeout must be a finite, non-negative number of seconds, not {value}"
        ))
    })
}

/// A file argument that could not be read, named with its path.
fn unreadable(argument: &str, path: &Path, err: &std::io::Error) -> PyErr {
    PyValueError::new_err(format!("{argument} {}: {err}", path.display()))
}

/// A token file's credential: read once, one trailing newline stripped,
/// refused if unreadable, if empty, if any other whitespace surrounds the
/// token, or if readable by group or others.
fn read_token_file(path: &Path) -> PyResult<String> {
    let mut file = File::open(path).map_err(|err| unreadable("token_file", path, &err))?;
    let mode = file
        .metadata()
        .map_err(|err| unreadable("token_file", path, &err))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(PyValueError::new_err(format!(
            "token_file {} is readable by group or others (mode {:o}); it must be 0600 or \
             stricter",
            path.display(),
            mode & 0o777
        )));
    }
    let mut token = String::new();
    file.read_to_string(&mut token)
        .map_err(|err| match err.kind() {
            std::io::ErrorKind::InvalidData => {
                PyValueError::new_err(format!("token_file {} is not UTF-8", path.display()))
            }
            _ => unreadable("token_file", path, &err),
        })?;
    if token.ends_with('\n') {
        token.pop();
    }
    if token.is_empty() {
        return Err(PyValueError::new_err(format!(
            "token_file {} is empty",
            path.display()
        )));
    }
    // A reader of the same file that trims whitespace must see the same
    // credential this client sends, so only one final newline is tolerated.
    if token.trim() != token {
        return Err(PyValueError::new_err(format!(
            "token_file {} has whitespace around the token beyond one final newline",
            path.display()
        )));
    }
    Ok(token)
}

/// Attach to a brenn server's remote route at `url` and block until the
/// first attachment.
#[pyfunction]
#[pyo3(signature = (url, *, ident, timeout, token = None, token_file = None, ca_file = None))]
fn attach(
    py: Python<'_>,
    url: String,
    ident: String,
    timeout: f64,
    token: Option<String>,
    token_file: Option<PathBuf>,
    ca_file: Option<PathBuf>,
) -> PyResult<PyClient> {
    let timeout = seconds(timeout)?;
    let token = match (token, token_file) {
        (Some(token), None) => token,
        (None, Some(path)) => read_token_file(&path)?,
        (Some(_), Some(_)) | (None, None) => {
            return Err(PyValueError::new_err(
                "exactly one of token and token_file is required",
            ));
        }
    };
    let ca_pem = match ca_file {
        Some(path) => Some(std::fs::read(&path).map_err(|err| unreadable("ca_file", &path, &err))?),
        None => None,
    };
    let params = AttachParams {
        url,
        token,
        ident,
        ca_pem,
        timeout,
    };
    match blocking(py, |interrupted| {
        BlockingClient::attach(params, interrupted)
    })? {
        Ok(inner) => Ok(PyClient { inner }),
        Err(err) => Err(errors::attach(py, err)),
    }
}

/// One attachment to a brenn server, supervised by a background thread that
/// reconnects across drops. Every method may be called from any thread.
#[pyclass(module = "brenn_bus", name = "Client", frozen)]
struct PyClient {
    inner: BlockingClient,
}

#[pymethods]
impl PyClient {
    /// The most recent attachment's facts.
    fn facts(&self) -> types::PyFacts {
        types::facts(&self.inner.facts())
    }

    /// Where the attachment stands now: `Attached` or `Detached`. Raises the
    /// `AttachmentEnded` subclass once the attachment is over. Never blocks.
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.inner.status() {
            Status::Attached(facts) => types::attached(py, &facts),
            Status::Detached {
                reason,
                last_dial_failure,
            } => types::detached(py, &reason, &last_dial_failure),
            Status::Ended(cause) => Err(errors::ended(py, &cause)),
        }
    }

    /// Publish one message and wait for the server's outcome. `attachments`
    /// maps each attachment's name to a blob this client holds.
    #[pyo3(signature = (channel, body, *, timeout, urgency = PyUrgency::Normal, attachments = None))]
    fn publish(
        slf: &Bound<'_, Self>,
        channel: String,
        body: PyRef<'_, PyBody>,
        timeout: f64,
        urgency: PyUrgency,
        attachments: Option<&Bound<'_, PyMapping>>,
    ) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let timeout = seconds(timeout)?;
        let mut named = Vec::new();
        let inner = &slf.get().inner;
        if let Some(attachments) = attachments {
            for item in attachments.items()?.iter() {
                let (name, blob): (String, Bound<'_, PyBlob>) = item.extract()?;
                let attachment = inner
                    .attachment(&blob.get().blob, attachment_name(name)?)
                    .map_err(|err| errors::call(py, err))?;
                named.push(attachment);
            }
        }
        let request =
            PublishRequest::new(channel, body.body.clone(), urgency.into()).attachments(named);
        drop(body);
        match blocking(py, |interrupted| {
            inner.publish(request, timeout, interrupted)
        })? {
            Ok(outcome) => types::publish_outcome(py, outcome),
            Err(err) => Err(errors::call(py, err)),
        }
    }

    /// Upload `data` as a blob of `content_type` and wait until it is sealed.
    #[pyo3(signature = (data, *, content_type, timeout))]
    fn upload(
        slf: &Bound<'_, Self>,
        data: &[u8],
        content_type: &str,
        timeout: f64,
    ) -> PyResult<PyBlob> {
        let timeout = seconds(timeout)?;
        let inner = &slf.get().inner;
        let made = blocking(slf.py(), |interrupted| {
            inner.upload_bytes(content_type, data, timeout, interrupted)
        })?;
        slf.get().held(slf, made)
    }

    /// Upload the regular file at `path`, as long as it is when opened, and
    /// wait until it is sealed.
    #[pyo3(signature = (path, *, content_type, timeout))]
    fn upload_file(
        slf: &Bound<'_, Self>,
        path: PathBuf,
        content_type: &str,
        timeout: f64,
    ) -> PyResult<PyBlob> {
        let timeout = seconds(timeout)?;
        let inner = &slf.get().inner;
        let made = blocking(slf.py(), |interrupted| {
            inner.upload_file(content_type, &path, timeout, interrupted)
        })?;
        slf.get().held(slf, made)
    }

    /// Open the attachment `name` of the message `message_id` on `channel`
    /// and wait until it is held.
    #[pyo3(signature = (channel, message_id, name, *, timeout))]
    fn open_attachment(
        slf: &Bound<'_, Self>,
        channel: &str,
        message_id: &str,
        name: String,
        timeout: f64,
    ) -> PyResult<PyBlob> {
        let timeout = seconds(timeout)?;
        let message_id = Uuid::parse_str(message_id).map_err(|err| {
            PyValueError::new_err(format!("message_id {message_id:?} is not a UUID: {err}"))
        })?;
        let name = attachment_name(name)?;
        let inner = &slf.get().inner;
        let made = blocking(slf.py(), |interrupted| {
            inner.open_blob(channel, message_id, name, timeout, interrupted)
        })?;
        slf.get().held(slf, made)
    }

    /// The bytes `[offset, offset + length)` of `blob`.
    #[pyo3(signature = (blob, offset, length, *, timeout))]
    fn read_blob<'py>(
        &self,
        py: Python<'py>,
        blob: &Bound<'py, PyBlob>,
        offset: i64,
        length: i64,
        timeout: f64,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let timeout = seconds(timeout)?;
        let offset = not_negative("offset", offset)?;
        let length = not_negative("length", length)?;
        let blob = &blob.get().blob;
        match blocking(py, |interrupted| {
            self.inner
                .read_blob(blob, offset, length, timeout, interrupted)
        })? {
            Ok(bytes) => Ok(PyBytes::new(py, &bytes)),
            Err(err) => Err(errors::call(py, err)),
        }
    }

    /// Write the whole of `blob` to a new file at `path`; an existing file is
    /// refused, and on any failure the file is removed.
    #[pyo3(signature = (blob, path, *, timeout))]
    fn download_blob(
        &self,
        py: Python<'_>,
        blob: &Bound<'_, PyBlob>,
        path: PathBuf,
        timeout: f64,
    ) -> PyResult<()> {
        let timeout = seconds(timeout)?;
        let blob = &blob.get().blob;
        match blocking(py, |interrupted| {
            self.inner
                .read_blob_to_file(blob, &path, timeout, interrupted)
        })? {
            Ok(()) => Ok(()),
            Err(err) => Err(errors::call(py, err)),
        }
    }

    /// Hold `channel` and wait for the server's settlement: `Subscribed` or
    /// `Unavailable`.
    #[pyo3(signature = (channel, *, push_depth, retain_depth, timeout, cursorless = false))]
    fn subscribe(
        &self,
        py: Python<'_>,
        channel: String,
        push_depth: u64,
        retain_depth: u64,
        timeout: f64,
        cursorless: bool,
    ) -> PyResult<Py<PyAny>> {
        let timeout = seconds(timeout)?;
        let depths = SubscriptionDepths {
            push_depth,
            retain_depth,
        };
        let resume = if cursorless {
            ResumePolicy::Cursorless
        } else {
            ResumePolicy::Resume
        };
        match blocking(py, |interrupted| {
            self.inner
                .subscribe(&channel, depths, resume, timeout, interrupted)
        })? {
            Ok(answer) => types::subscribe_answer(py, answer),
            Err(err) => Err(errors::call(py, err)),
        }
    }

    /// Drop the hold on `channel`, in any state; no read yields an event of it
    /// afterwards until it is subscribed again.
    fn unsubscribe(&self, py: Python<'_>, channel: String) -> PyResult<()> {
        match blocking(py, |interrupted| {
            self.inner.unsubscribe(&channel, interrupted)
        })? {
            Ok(()) => Ok(()),
            Err(err) => Err(errors::call(py, err)),
        }
    }

    /// The next event, or None once `timeout` seconds have passed; a timeout
    /// of None waits indefinitely.
    #[pyo3(signature = (*, timeout))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Py<PyAny>>> {
        let timeout = timeout.map(seconds).transpose()?;
        match blocking(py, |interrupted| self.inner.recv(timeout, interrupted))? {
            Ok(Some(event)) => types::event(py, event).map(Some),
            Ok(None) => Ok(None),
            Err(err) => Err(errors::recv(py, err)),
        }
    }

    /// End the attachment and wait for its thread. Idempotent.
    fn close(&self, py: Python<'_>) {
        py.detach(|| self.inner.close());
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_args))]
    fn __exit__(&self, py: Python<'_>, _args: &Bound<'_, PyTuple>) -> bool {
        self.close(py);
        false
    }
}

impl PyClient {
    /// The Python face of a blob this client, `slf`, now holds.
    fn held(&self, slf: &Bound<'_, Self>, made: Result<Blob, CallError>) -> PyResult<PyBlob> {
        match made {
            Ok(blob) => Ok(PyBlob {
                client: slf.clone().unbind(),
                blob,
            }),
            Err(err) => Err(errors::call(slf.py(), err)),
        }
    }
}

/// A blob held on a client's live attachment: an upload it sealed or an
/// attachment it opened. It lasts until `close()`, the end of a `with` block,
/// or the attachment's drop; collecting it releases nothing.
#[pyclass(module = "brenn_bus", name = "Blob", frozen)]
struct PyBlob {
    client: Py<PyClient>,
    blob: Blob,
}

#[pymethods]
impl PyBlob {
    #[getter]
    fn size(&self) -> u64 {
        self.blob.size()
    }

    #[getter]
    fn content_type(&self) -> &str {
        self.blob.content_type()
    }

    /// Give the blob up. Closing a blob already closed, or gone with its
    /// attachment, succeeds.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let client = self.client.get();
        match blocking(py, |interrupted| {
            client.inner.close_blob(&self.blob, interrupted)
        })? {
            Ok(()) => Ok(()),
            Err(err) => Err(errors::call(py, err)),
        }
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_args))]
    fn __exit__(&self, py: Python<'_>, _args: &Bound<'_, PyTuple>) -> PyResult<bool> {
        self.close(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!(
            "Blob(size={}, content_type={:?})",
            self.blob.size(),
            self.blob.content_type()
        )
    }
}

/// An attachment name, refused unless it follows the bus's grammar.
fn attachment_name(name: String) -> PyResult<AttachmentName> {
    AttachmentName::new(name).map_err(|err| PyValueError::new_err(err.to_string()))
}

/// A byte offset or length, refused if negative.
fn not_negative(argument: &str, value: i64) -> PyResult<u64> {
    u64::try_from(value)
        .map_err(|_| PyValueError::new_err(format!("{argument} must not be negative, not {value}")))
}

impl Drop for PyClient {
    /// Close with the GIL released: closing joins the supervisor thread, and a
    /// collected client must not hold every other Python thread for that long.
    /// Where the interpreter cannot be attached to (shutdown, a GC traversal),
    /// the inner client's own drop closes it.
    fn drop(&mut self) {
        let inner = &self.inner;
        let _ = Python::try_attach(|py| py.detach(|| inner.close()));
    }
}

#[pymodule]
fn brenn_bus(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(attach, m)?)?;
    m.add_class::<PyClient>()?;
    m.add_class::<PyBlob>()?;
    types::register(m)?;
    errors::register(m)?;
    Ok(())
}

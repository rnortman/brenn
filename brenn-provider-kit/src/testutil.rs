//! Test fixtures for a step's inputs and outputs: `api-request`'s answer
//! envelope, `one-click-unsubscribe`'s answers, a nested call's text body, a
//! nested-call error, a Google error body and the error answer a step
//! returns; and `MemoryHost`, an in-memory host and blob store for a loop's
//! tests.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use base64ct::{Base64, Encoding};
use serde_json::{Value, json};

use crate::activation::{Host, Input, Level, Logic, Rows, serve};
use crate::answer::NestedError;
use crate::blob::{
    Attached, Blob, BlobLimits, Blobs, CreateError, OpenError, ReadError, SealError, Unavailable,
    WriteError, Writer,
};
use crate::call::{ApiCall, Body, Call, ErrorKind, Failure, Nested};

/// A nested call that failed with `kind` and `detail`.
pub fn nested(kind: &str, detail: &str) -> Result<Value, NestedError> {
    Err(NestedError {
        kind: kind.to_string(),
        detail: detail.to_string(),
    })
}

/// A whole JSON answer with `status` and `body`.
pub fn http(status: u64, body: &str) -> Result<Value, NestedError> {
    whole(
        status,
        json!({ "content-type": "application/json" }),
        body.as_bytes(),
        Carrier::Text,
    )
}

/// A whole answer with `status` and a body that is not text, as `api-request`
/// carries one: `body_base64`.
pub fn binary(status: u64, body: &[u8]) -> Result<Value, NestedError> {
    whole(
        status,
        json!({ "content-type": "application/octet-stream" }),
        body,
        Carrier::Base64,
    )
}

/// A whole answer with `status`, `headers` and `body`, carried as
/// `api-request` carries it: `body` when the bytes are UTF-8, `body_base64`
/// otherwise. Header names are lower-cased, as `api-request` returns them.
pub fn payload(status: u64, headers: &[(&str, &str)], body: &[u8]) -> Result<Value, NestedError> {
    let headers: serde_json::Map<String, Value> = headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), json!(value)))
        .collect();
    let carrier = match std::str::from_utf8(body) {
        Ok(_) => Carrier::Text,
        Err(_) => Carrier::Base64,
    };
    whole(status, Value::Object(headers), body, carrier)
}

/// The field an answer's body is carried in.
enum Carrier {
    /// `body`; the bytes must be UTF-8.
    Text,
    /// `body_base64`.
    Base64,
}

/// A whole, uncut answer with `status`, `headers` and `body` carried in
/// `carrier`.
fn whole(status: u64, headers: Value, body: &[u8], carrier: Carrier) -> Result<Value, NestedError> {
    let mut answer = json!({
        "status": status,
        "headers": headers,
        "bytes": body.len(),
        "truncated": false,
    });
    match carrier {
        Carrier::Text => {
            let text = std::str::from_utf8(body).expect("a text-carried body is UTF-8");
            answer["body"] = json!(text);
        }
        Carrier::Base64 => answer["body_base64"] = json!(Base64::encode_string(body)),
    }
    Ok(answer)
}

/// `api-request`'s answer to a call made with `response_attachment`: a 2xx
/// whose body of `bytes` bytes is the result's attachment `name`.
pub fn blob_answer(name: &str, bytes: u64) -> Result<Value, NestedError> {
    Ok(json!({
        "status": 200,
        "headers": { "content-type": "application/octet-stream" },
        "bytes": bytes,
        "attachment": name,
    }))
}

/// `api-request`'s refusal of an answer over the host's largest blob
/// `limit`, of `bytes` when the API declared a length.
pub fn too_large(limit: u64, bytes: Option<u64>) -> Result<Value, NestedError> {
    Ok(json!({
        "error": "response_too_large", "status": 200, "headers": {}, "bytes": bytes,
        "limit": limit,
    }))
}

/// `api-request`'s refusal of an answer for a full blob pool.
pub fn store_full() -> Result<Value, NestedError> {
    Ok(json!({ "error": "blob_store_full", "status": 200, "headers": {} }))
}

/// `one-click-unsubscribe`'s answer when the endpoint at `host` answered HTTP
/// `status`.
pub fn unsubscribe_status(host: &str, status: u64) -> Result<Value, NestedError> {
    Ok(json!({ "status": status, "host": host }))
}

/// `one-click-unsubscribe`'s answer when no response came from `host`.
pub fn unsubscribe_unreachable(host: &str) -> Result<Value, NestedError> {
    Ok(json!({ "error": "unreachable", "host": host }))
}

/// The `api-request` arguments of `request`.
///
/// # Panics
/// When `request` reaches another tool.
fn api_or_panic(request: &Nested) -> &ApiCall {
    request
        .api()
        .unwrap_or_else(|| panic!("the call reaches `{}`, not api-request", request.tool()))
}

/// The `api-request` arguments of `call`.
///
/// # Panics
/// When `call` reaches another tool.
pub fn api_of(call: &Call) -> &ApiCall {
    api_or_panic(&call.request)
}

/// The text body of a nested call.
///
/// # Panics
/// When the call carries no body or a byte body.
pub fn text_body(call: &ApiCall) -> &str {
    match &call.body {
        Some(Body::Text(text)) => text,
        other => panic!("the call carries no text body: {other:?}"),
    }
}

/// An answer `api-request` cut at the provider's read limit: `bytes` is the
/// size it states (a number, or `null` for unstated), `body` the prefix it kept.
pub fn truncated(status: u64, bytes: Value, body: &str) -> Result<Value, NestedError> {
    Ok(json!({
        "status": status, "headers": {}, "bytes": bytes, "truncated": true, "body": body,
    }))
}

/// A Google API error body, with a `details` entry nothing may copy out.
pub fn google(code: u64, status: &str, message: &str) -> String {
    json!({ "error": { "code": code, "status": status, "message": message, "details": [{ "secret": "x" }] } })
        .to_string()
}

/// The error answer of `kind` with `detail`.
pub fn failure(kind: ErrorKind, detail: &str) -> Failure {
    Failure {
        kind,
        detail: detail.to_string(),
    }
}

/// A nested call the loop issued.
#[derive(Clone, Debug, PartialEq)]
pub struct NestedCall {
    /// The pending id the call serves.
    pub id: String,
    pub request: Nested,
    pub call_id: String,
    /// The attachment the call carried as its body: its name and bytes.
    pub body: Option<(String, Vec<u8>)>,
    /// The content type of the body's blob.
    pub body_content_type: Option<String>,
}

impl NestedCall {
    /// The `api-request` arguments the call carried.
    ///
    /// # Panics
    /// When the call reached another tool.
    pub fn api(&self) -> &ApiCall {
        api_or_panic(&self.request)
    }
}

/// One attachment of a completion: its name, content type and bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedBlob {
    pub name: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

/// How a request was answered.
#[derive(Clone, Debug, PartialEq)]
pub enum Answered {
    Value(Value),
    Error(ErrorKind, String),
}

/// A request's answer, as the loop gave it.
#[derive(Clone, Debug, PartialEq)]
pub struct Completion {
    pub id: String,
    pub answer: Answered,
    /// The blobs the completion carried; empty on an error answer.
    pub attachments: Vec<CompletedBlob>,
}

/// A store's rows by namespace and key.
type Store = BTreeMap<(String, Vec<u8>), Vec<u8>>;

/// A sealed blob's content type and bytes.
#[derive(Clone)]
struct Sealed {
    content_type: String,
    bytes: Rc<Vec<u8>>,
}

/// A handle of the current step.
enum Slot {
    Blob(Sealed),
    Writer {
        content_type: String,
        bytes: Vec<u8>,
        /// The written ranges, `(start, end)`.
        written: Vec<(u64, u64)>,
    },
    /// A writer that sealed into another slot.
    Spent,
}

/// The in-memory blob store: seeded request and result attachments, the
/// current step's handles, the host's bounds and the scripted refusals.
struct BlobStore {
    /// `None`: serve no blobs, as the glue's `serve` serves none.
    limits: Option<BlobLimits>,
    max_blob_bytes: u64,
    max_handles: usize,
    call_budget: u32,
    write_budget: u64,
    calls: u32,
    written: u64,
    not_pending: bool,
    requests: BTreeMap<(String, String), Sealed>,
    results: BTreeMap<(String, String), Sealed>,
    slots: Vec<Slot>,
    peak_handles: usize,
    refuse_create: Option<CreateError>,
    refuse_write: Option<WriteError>,
}

impl Default for BlobStore {
    fn default() -> BlobStore {
        BlobStore {
            limits: Some(BlobLimits {
                max_read_bytes: 1 << 20,
                max_write_bytes: 1 << 20,
            }),
            max_blob_bytes: 64 << 20,
            max_handles: 64,
            call_budget: 4_096,
            write_budget: 64 << 20,
            calls: 0,
            written: 0,
            not_pending: false,
            requests: BTreeMap::new(),
            results: BTreeMap::new(),
            slots: Vec::new(),
            peak_handles: 0,
            refuse_create: None,
            refuse_write: None,
        }
    }
}

impl BlobStore {
    /// Count one blob call against the activation's budget; whether it is
    /// within it.
    fn count(&mut self) -> bool {
        self.calls += 1;
        self.calls <= self.call_budget
    }

    fn live(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| !matches!(slot, Slot::Spent))
            .count()
    }

    /// A new slot holding `slot`, unless the step holds as many handles as
    /// the host allows.
    fn take(&mut self, slot: Slot) -> Option<u32> {
        if self.live() >= self.max_handles {
            return None;
        }
        self.slots.push(slot);
        self.peak_handles = self.peak_handles.max(self.live());
        Some(u32::try_from(self.slots.len() - 1).expect("slots fit a u32"))
    }

    fn open(&mut self, sealed: Option<Sealed>) -> Result<Blob, OpenError> {
        if !self.count() {
            return Err(OpenError::Quota);
        }
        let sealed = sealed.ok_or(OpenError::NotAttached)?;
        let size = sealed.bytes.len() as u64;
        let content_type = sealed.content_type.clone();
        let slot = self.take(Slot::Blob(sealed)).ok_or(OpenError::Quota)?;
        Ok(Blob::new(slot, size, content_type))
    }
}

/// The first gap below the highest byte `written` covers.
fn first_gap(written: &[(u64, u64)]) -> Option<(u64, u64)> {
    let mut ranges = written.to_vec();
    ranges.sort_unstable();
    let mut covered = 0;
    for (start, end) in ranges {
        if start > covered {
            return Some((covered, start - covered));
        }
        covered = covered.max(end);
    }
    None
}

/// An in-memory [`Host`]: a store whose activations commit on `Ok` and roll
/// back on `Err`, a script of refused calls and failed completions, and a
/// record of the calls, completions and log lines. Calls an activation issued
/// are dropped when it fails, as the host drops them; completions are not,
/// as the host sends them at once. Every request is pending until a test
/// ends it with [`MemoryHost::end_request`].
///
/// As [`Blobs`] it serves seeded request and result attachments and the
/// blobs a step builds, under per-call limits, per-activation write and call
/// budgets and 64 live handles, each refused as the host refuses it. Handles
/// live until [`Blobs::release`] or the activation's end, so a failed
/// activation's blobs survive only as a completion recorded them.
#[derive(Default)]
pub struct MemoryHost {
    store: RefCell<Store>,
    refuse: RefCell<Option<(usize, String)>>,
    fail_completes: Cell<bool>,
    calls: RefCell<Vec<NestedCall>>,
    completions: RefCell<Vec<Completion>>,
    log: RefCell<Vec<(Level, String)>>,
    blobs: RefCell<BlobStore>,
    ended: RefCell<BTreeSet<String>>,
}

impl MemoryHost {
    pub fn new() -> MemoryHost {
        MemoryHost::default()
    }

    /// Refuse the next `count` calls with `text`.
    pub fn refuse_next(&self, count: usize, text: &str) {
        *self.refuse.borrow_mut() = (count > 0).then(|| (count, text.to_string()));
    }

    /// Refuse every call from now on with `text`.
    pub fn refuse_all(&self, text: &str) {
        self.refuse_next(usize::MAX, text);
    }

    /// Accept every call from now on.
    pub fn accept_all(&self) {
        *self.refuse.borrow_mut() = None;
    }

    /// Whether every completion fails with a host error, failing the
    /// activation that attempts it.
    pub fn fail_completes(&self, fail: bool) {
        self.fail_completes.set(fail);
    }

    /// Run one activation of `L`'s loop in a transaction.
    pub fn activate<L: Logic>(&self, settings: &L::Settings, input: Input) -> Result<(), String> {
        let before = self.store.borrow().clone();
        let issued = self.calls.borrow().len();
        {
            let mut blobs = self.blobs.borrow_mut();
            blobs.calls = 0;
            blobs.written = 0;
            blobs.peak_handles = 0;
        }
        let outcome = serve::<MemoryHost, L>(self, settings, input);
        self.blobs.borrow_mut().slots.clear();
        if outcome.is_err() {
            *self.store.borrow_mut() = before;
            self.calls.borrow_mut().truncate(issued);
        }
        outcome
    }

    /// Hold `bytes` as request `id`'s attachment `name`; what the request's
    /// `attachments` list for it.
    pub fn seed_request(
        &self,
        id: &str,
        name: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> Attached {
        let (attached, sealed) = sealed(name, content_type, bytes);
        self.blobs
            .borrow_mut()
            .requests
            .insert((id.to_string(), name.to_string()), sealed);
        attached
    }

    /// Hold `bytes` as attachment `name` of the result correlated by
    /// `call_id`; what the result's `attachments` list for it.
    pub fn seed_result(
        &self,
        call_id: &str,
        name: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> Attached {
        let (attached, sealed) = sealed(name, content_type, bytes);
        self.blobs
            .borrow_mut()
            .results
            .insert((call_id.to_string(), name.to_string()), sealed);
        attached
    }

    /// Serve the per-call `limits`, or no blobs at all with `None`, as the
    /// glue's `serve` serves none.
    pub fn set_blob_limits(&self, limits: Option<BlobLimits>) {
        self.blobs.borrow_mut().limits = limits;
    }

    /// The largest blob a write may make.
    pub fn set_max_blob_bytes(&self, max: u64) {
        self.blobs.borrow_mut().max_blob_bytes = max;
    }

    /// The bytes an activation may write.
    pub fn set_write_budget(&self, bytes: u64) {
        self.blobs.borrow_mut().write_budget = bytes;
    }

    /// The blob calls an activation may make.
    pub fn set_blob_call_budget(&self, calls: u32) {
        self.blobs.borrow_mut().call_budget = calls;
    }

    /// Whether every request's attachment answers `NotPending`, as the host
    /// answers once a request has ended. Only the open is refused: a call
    /// is still accepted and a completion still recorded, so a test sees
    /// the failure the kit gives, which on the host is refused `not-pending`
    /// and dropped by the glue, reaching no caller.
    pub fn requests_not_pending(&self, not_pending: bool) {
        self.blobs.borrow_mut().not_pending = not_pending;
    }

    /// End request `id`: from now on [`Host::pending`] answers `false` for
    /// it. Calls and completions for it are still recorded, so a test sees
    /// what the loop attempted.
    pub fn end_request(&self, id: &str) {
        self.ended.borrow_mut().insert(id.to_string());
    }

    /// Refuse the next blob create with `error`.
    pub fn refuse_next_create(&self, error: CreateError) {
        self.blobs.borrow_mut().refuse_create = Some(error);
    }

    /// Refuse the next blob write with `error`.
    pub fn refuse_next_write(&self, error: WriteError) {
        self.blobs.borrow_mut().refuse_write = Some(error);
    }

    /// The most handles held at once in the last activation.
    pub fn peak_handles(&self) -> usize {
        self.blobs.borrow().peak_handles
    }

    /// Every row of `ns`, ascending by key.
    pub fn rows(&self, ns: &str) -> Rows {
        self.store
            .borrow()
            .iter()
            .filter(|((row_ns, _), _)| row_ns == ns)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect()
    }

    /// Write `value` under `key` in `ns`, outside any activation.
    pub fn insert(&self, ns: &str, key: &[u8], value: &[u8]) {
        self.store
            .borrow_mut()
            .insert((ns.to_string(), key.to_vec()), value.to_vec());
    }

    /// The calls issued since the last take.
    pub fn take_calls(&self) -> Vec<NestedCall> {
        std::mem::take(&mut *self.calls.borrow_mut())
    }

    /// The completions given since the last take.
    pub fn take_completions(&self) -> Vec<Completion> {
        std::mem::take(&mut *self.completions.borrow_mut())
    }

    /// The log lines written since the last take.
    pub fn take_log(&self) -> Vec<(Level, String)> {
        std::mem::take(&mut *self.log.borrow_mut())
    }

    fn answered(
        &self,
        id: &str,
        answer: Answered,
        attachments: Vec<CompletedBlob>,
    ) -> Result<(), String> {
        if self.fail_completes.get() {
            return Err(format!("complete {id}: host failure"));
        }
        self.completions.borrow_mut().push(Completion {
            id: id.to_string(),
            answer,
            attachments,
        });
        Ok(())
    }

    /// The sealed blob behind `blob`.
    ///
    /// # Panics
    /// When `blob` names no sealed blob of the current step: a handle used
    /// after its release.
    fn sealed_at(&self, blob: &Blob) -> Sealed {
        match self.blobs.borrow().slots.get(blob.slot() as usize) {
            Some(Slot::Blob(sealed)) => sealed.clone(),
            _ => panic!("blob handle {} is not live in this step", blob.slot()),
        }
    }
}

/// A seeded attachment's header entry and its blob.
fn sealed(name: &str, content_type: &str, bytes: Vec<u8>) -> (Attached, Sealed) {
    (
        Attached {
            name: name.to_string(),
            size: bytes.len() as u64,
            content_type: content_type.to_string(),
        },
        Sealed {
            content_type: content_type.to_string(),
            bytes: Rc::new(bytes),
        },
    )
}

impl Blobs for MemoryHost {
    fn limits(&self) -> Result<BlobLimits, Unavailable> {
        self.blobs.borrow().limits.ok_or(Unavailable)
    }

    fn open_request(&self, id: &str, name: &str) -> Result<Blob, OpenError> {
        self.limits()?;
        let mut blobs = self.blobs.borrow_mut();
        if blobs.not_pending {
            return Err(OpenError::NotPending);
        }
        let sealed = blobs
            .requests
            .get(&(id.to_string(), name.to_string()))
            .cloned();
        blobs.open(sealed)
    }

    fn open_result(&self, call_id: &str, name: &str) -> Result<Blob, OpenError> {
        self.limits()?;
        let mut blobs = self.blobs.borrow_mut();
        let sealed = blobs
            .results
            .get(&(call_id.to_string(), name.to_string()))
            .cloned();
        blobs.open(sealed)
    }

    fn read(&self, blob: &Blob, offset: u64, len: u32) -> Result<Vec<u8>, ReadError> {
        let limits = self.limits()?;
        if !self.blobs.borrow_mut().count() {
            return Err(ReadError::Quota);
        }
        if len > limits.max_read_bytes {
            return Err(ReadError::TooLarge);
        }
        let sealed = self.sealed_at(blob);
        let end = offset.saturating_add(u64::from(len));
        if end > sealed.bytes.len() as u64 {
            return Err(ReadError::OutOfRange);
        }
        Ok(sealed.bytes[offset as usize..end as usize].to_vec())
    }

    fn create(&self, content_type: &str) -> Result<Writer, CreateError> {
        self.limits()?;
        let mut blobs = self.blobs.borrow_mut();
        if !blobs.count() {
            return Err(CreateError::Quota);
        }
        if let Some(error) = blobs.refuse_create.take() {
            return Err(error);
        }
        let slot = blobs
            .take(Slot::Writer {
                content_type: content_type.to_string(),
                bytes: Vec::new(),
                written: Vec::new(),
            })
            .ok_or(CreateError::Quota)?;
        Ok(Writer::new(slot))
    }

    fn write(&self, writer: &Writer, offset: u64, bytes: &[u8]) -> Result<(), WriteError> {
        let limits = self.limits()?;
        let mut blobs = self.blobs.borrow_mut();
        if !blobs.count() {
            return Err(WriteError::Quota);
        }
        if let Some(error) = blobs.refuse_write.take() {
            return Err(error);
        }
        let len = bytes.len() as u64;
        if len > u64::from(limits.max_write_bytes) {
            return Err(WriteError::TooLarge);
        }
        let end = offset.saturating_add(len);
        if end > blobs.max_blob_bytes {
            return Err(WriteError::BlobTooLarge);
        }
        if blobs.written + len > blobs.write_budget {
            return Err(WriteError::Quota);
        }
        blobs.written += len;
        let Some(Slot::Writer {
            bytes: data,
            written,
            ..
        }) = blobs.slots.get_mut(writer.slot() as usize)
        else {
            panic!("writer handle {} is not live in this step", writer.slot());
        };
        if data.len() < end as usize {
            data.resize(end as usize, 0);
        }
        data[offset as usize..end as usize].copy_from_slice(bytes);
        written.push((offset, end));
        Ok(())
    }

    fn seal(&self, writer: Writer) -> Result<Blob, SealError> {
        self.limits()?;
        let mut blobs = self.blobs.borrow_mut();
        if !blobs.count() {
            return Err(SealError::Quota);
        }
        let slot = writer.slot() as usize;
        let Some(Slot::Writer {
            content_type,
            bytes,
            written,
        }) = blobs.slots.get_mut(slot)
        else {
            panic!("writer handle {slot} is not live in this step");
        };
        if let Some((offset, len)) = first_gap(written) {
            return Err(SealError::Gap { offset, len });
        }
        let sealed = Sealed {
            content_type: std::mem::take(content_type),
            bytes: Rc::new(std::mem::take(bytes)),
        };
        blobs.slots[slot] = Slot::Spent;
        let size = sealed.bytes.len() as u64;
        let content_type = sealed.content_type.clone();
        let blob = blobs.take(Slot::Blob(sealed)).ok_or(SealError::Quota)?;
        Ok(Blob::new(blob, size, content_type))
    }

    fn release(&self) {
        self.blobs.borrow_mut().slots.clear();
    }
}

impl Host for MemoryHost {
    type Error = String;

    fn fault(detail: String) -> String {
        detail
    }

    fn get(&self, ns: &str, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        Ok(self
            .store
            .borrow()
            .get(&(ns.to_string(), key.to_vec()))
            .cloned())
    }

    fn put(&self, ns: &str, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.insert(ns, key, value);
        Ok(())
    }

    fn delete(&self, ns: &str, key: &[u8]) -> Result<(), String> {
        self.store
            .borrow_mut()
            .remove(&(ns.to_string(), key.to_vec()));
        Ok(())
    }

    fn scan(&self, ns: &str) -> Result<Rows, String> {
        Ok(self.rows(ns))
    }

    fn call(
        &self,
        id: &str,
        call: &Nested,
        call_id: &str,
        body: Option<(&str, &Blob)>,
    ) -> Result<(), String> {
        assert!(
            body.is_none() || call.api().is_some(),
            "a call to {} carries a body",
            call.tool()
        );
        let mut refuse = self.refuse.borrow_mut();
        if let Some((count, text)) = refuse.as_mut() {
            let text = text.clone();
            *count -= 1;
            if *count == 0 {
                *refuse = None;
            }
            return Err(text);
        }
        drop(refuse);
        let body = body.map(|(name, blob)| (name.to_string(), self.sealed_at(blob)));
        self.calls.borrow_mut().push(NestedCall {
            id: id.to_string(),
            request: call.clone(),
            call_id: call_id.to_string(),
            body_content_type: body.as_ref().map(|(_, sealed)| sealed.content_type.clone()),
            body: body.map(|(name, sealed)| (name, sealed.bytes.to_vec())),
        });
        Ok(())
    }

    fn complete(
        &self,
        id: &str,
        value: &Value,
        attachments: &[(String, Blob)],
    ) -> Result<(), String> {
        let attachments = attachments
            .iter()
            .map(|(name, blob)| {
                let sealed = self.sealed_at(blob);
                CompletedBlob {
                    name: name.clone(),
                    content_type: sealed.content_type,
                    bytes: sealed.bytes.to_vec(),
                }
            })
            .collect();
        self.answered(id, Answered::Value(value.clone()), attachments)
    }

    fn complete_err(&self, id: &str, kind: ErrorKind, detail: &str) -> Result<(), String> {
        self.answered(id, Answered::Error(kind, detail.to_string()), Vec::new())
    }

    fn pending(&self, id: &str) -> bool {
        !self.ended.borrow().contains(id)
    }

    fn log(&self, level: Level, line: String) {
        self.log.borrow_mut().push((level, line));
    }
}

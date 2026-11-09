//! Test fixtures for a step's inputs and outputs: `api-request`'s answer
//! envelope, a nested call's text body, a nested-call error, a Google error
//! body and the error answer a step returns; and `MemoryHost`, an in-memory
//! host for a loop's tests.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

use base64ct::{Base64, Encoding};
use serde_json::{Value, json};

use crate::activation::{Host, Input, Level, Logic, Rows, serve};
use crate::answer::NestedError;
use crate::call::{ApiCall, Body, ErrorKind, Failure};

/// A nested call that failed with `kind` and `detail`.
pub fn nested(kind: &str, detail: &str) -> Result<Value, NestedError> {
    Err(NestedError {
        kind: kind.to_string(),
        detail: detail.to_string(),
    })
}

/// A whole JSON answer with `status` and `body`.
pub fn http(status: u64, body: &str) -> Result<Value, NestedError> {
    Ok(json!({
        "status": status,
        "headers": { "content-type": "application/json" },
        "bytes": body.len(),
        "truncated": false,
        "body": body,
    }))
}

/// A whole answer with `status` and a body that is not text, as `api-request`
/// carries one: `body_base64`.
pub fn binary(status: u64, body: &[u8]) -> Result<Value, NestedError> {
    Ok(json!({
        "status": status,
        "headers": { "content-type": "application/octet-stream" },
        "bytes": body.len(),
        "truncated": false,
        "body_base64": Base64::encode_string(body),
    }))
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
    pub call: ApiCall,
    pub call_id: String,
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
}

/// A store's rows by namespace and key.
type Store = BTreeMap<(String, Vec<u8>), Vec<u8>>;

/// An in-memory [`Host`]: a store whose activations commit on `Ok` and roll
/// back on `Err`, a script of refused calls and failed completions, and a
/// record of the calls, completions and log lines. Calls an activation issued
/// are dropped when it fails, as the host drops them; completions are not,
/// as the host sends them at once.
#[derive(Default)]
pub struct MemoryHost {
    store: RefCell<Store>,
    refuse: RefCell<Option<(usize, String)>>,
    fail_completes: Cell<bool>,
    calls: RefCell<Vec<NestedCall>>,
    completions: RefCell<Vec<Completion>>,
    log: RefCell<Vec<(Level, String)>>,
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
        let outcome = serve::<MemoryHost, L>(self, settings, input);
        if outcome.is_err() {
            *self.store.borrow_mut() = before;
            self.calls.borrow_mut().truncate(issued);
        }
        outcome
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

    fn answered(&self, id: &str, answer: Answered) -> Result<(), String> {
        if self.fail_completes.get() {
            return Err(format!("complete {id}: host failure"));
        }
        self.completions.borrow_mut().push(Completion {
            id: id.to_string(),
            answer,
        });
        Ok(())
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

    fn call(&self, id: &str, call: &ApiCall, call_id: &str) -> Result<(), String> {
        let mut refuse = self.refuse.borrow_mut();
        if let Some((count, text)) = refuse.as_mut() {
            let text = text.clone();
            *count -= 1;
            if *count == 0 {
                *refuse = None;
            }
            return Err(text);
        }
        self.calls.borrow_mut().push(NestedCall {
            id: id.to_string(),
            call: call.clone(),
            call_id: call_id.to_string(),
        });
        Ok(())
    }

    fn complete(&self, id: &str, value: &Value) -> Result<(), String> {
        self.answered(id, Answered::Value(value.clone()))
    }

    fn complete_err(&self, id: &str, kind: ErrorKind, detail: &str) -> Result<(), String> {
        self.answered(id, Answered::Error(kind, detail.to_string()))
    }

    fn log(&self, level: Level, line: String) {
        self.log.borrow_mut().push((level, line));
    }
}

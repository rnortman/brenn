//! Test fixtures for a step's inputs and outputs: `api-request`'s answer
//! envelope, a nested-call error, a Google error body and the error answer a
//! step returns.

use serde_json::{Value, json};

use crate::answer::NestedError;
use crate::call::{ErrorKind, Failure};

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

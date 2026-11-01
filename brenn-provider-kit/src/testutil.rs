//! Test fixtures for a step's inputs and outputs: `api-request`'s answer
//! envelope, `device-location`'s answer, a nested-call error, a Google error
//! body, the error answer a step returns, and a test's view of a step's
//! `api-request` arguments.

use serde_json::{Value, json};

use crate::answer::NestedError;
use crate::call::{ApiCall, Call, ErrorKind, Failure, Nested};

/// The `ApiCall` of a step's call, for a test's assertions; a location step
/// has none, which is the test's mistake.
pub fn api(call: &Call) -> &ApiCall {
    match &call.nested {
        Nested::ApiRequest(api) => api,
        other => panic!("step {} calls {}, not api-request", call.step, other.tool()),
    }
}

/// Stands in for `device-location`'s text on turning sharing on, which a
/// step passes through without reading.
pub const HOW_TO_ENABLE: &str = "<how to enable>";

/// `device-location`'s answer listing `devices` as `(device, latitude,
/// longitude, age_seconds, stale)`, in the order given, with every field the
/// tool writes.
pub fn device_location_answer(
    devices: &[(&str, f64, f64, u64, bool)],
) -> Result<Value, NestedError> {
    let devices: Vec<Value> = devices
        .iter()
        .map(|(device, latitude, longitude, age_seconds, stale)| {
            json!({
                "device": device,
                "latitude": latitude,
                "longitude": longitude,
                "accuracy_m": 12.0,
                "reported_at": "2026-11-01T12:00:00Z",
                "age_seconds": age_seconds,
                "stale": stale,
                "time_zone": "Europe/London",
                "local_time": "2026-11-01T12:00:30+00:00",
            })
        })
        .collect();
    Ok(json!({ "devices": devices, "stale_after_seconds": 900 }))
}

/// `device-location`'s answer when no device shares location.
pub fn no_devices() -> Result<Value, NestedError> {
    Ok(json!({ "devices": [], "stale_after_seconds": 900, "how_to_enable": HOW_TO_ENABLE }))
}

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::call::Query;

    fn bare() -> ApiCall {
        ApiCall {
            provider: "maps".to_string(),
            account: None,
            method: "GET".to_string(),
            host: "maps.example.com".to_string(),
            path: "/v1/places".to_string(),
            query: Query::new(),
            headers: BTreeMap::new(),
            body: None,
        }
    }

    #[test]
    fn api_is_the_steps_api_call() {
        assert_eq!(api(&Call::api(1, bare())), &bare());
    }

    #[test]
    #[should_panic(expected = "step 3 calls device-location, not api-request")]
    fn api_of_a_location_step_panics() {
        api(&Call::device_location(3));
    }
}

// The `provides` interface against a recording seam.
//
// Uses the `processor-provider-test` fixture, the one component that imports
// `brenn:processor/provides`. It serves every new request on `tool-requests`
// by the request's `mode` argument and completes served requests from the
// results on `tool-results` (see the fixture's own header for the modes).
//
// What is pinned here is the host's half of the contract: `complete` reaches
// the seam the moment the guest calls it and is not retracted by a later
// failure of the same activation, while `call-async-for` is buffered like
// `call-async` and reaches the bus only with an ok activation.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use brenn_wasm::{
    ComponentGrant, ProcessorActivation, ProcessorComponent, ProcessorLoadSpec, ProcessorOutcome,
    ProcessorPortWindow, ProcessorReceiveError, ProvidesCallError, ProvidesHost, QueuedToolRequest,
    ToolCallError, ToolHost, ToolHostFn,
};
use serde_json::{Value, json};

mod common;

const REQUESTS: &str = "tool-requests";
const RESULTS: &str = "tool-results";
const SLUG: &str = "provider";

fn component_path() -> std::path::PathBuf {
    common::artifact_path("brenn_processor_provider_test")
}

/// A seam over a fixed set of pending ids that records every call it admits.
#[derive(Default)]
struct RecordingProvidesHost {
    pending: Mutex<HashSet<String>>,
    /// Refuse every on-behalf-of call `denied`, pending or not, as an ACL miss
    /// on the nested call would.
    refuse_nested: bool,
    /// Refuse every completion with `invalid-outcome(<this>)`, leaving the id
    /// pending.
    refuse_outcome: Option<String>,
    /// `(id, outcome)` per admitted completion, in call order.
    completed: Mutex<Vec<(String, Value)>>,
    /// `(serving, tool, call_id)` per admitted on-behalf-of call.
    queued: Mutex<Vec<(String, String, String)>>,
}

impl RecordingProvidesHost {
    fn pending(ids: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(ids.iter().map(|id| id.to_string()).collect()),
            ..Self::default()
        })
    }

    fn refusing_nested(ids: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(ids.iter().map(|id| id.to_string()).collect()),
            refuse_nested: true,
            ..Self::default()
        })
    }

    fn refusing_outcomes(ids: &[&str], detail: &str) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(ids.iter().map(|id| id.to_string()).collect()),
            refuse_outcome: Some(detail.to_string()),
            ..Self::default()
        })
    }

    fn is_pending(&self, id: &str) -> bool {
        self.pending.lock().unwrap().contains(id)
    }

    fn completed(&self) -> Vec<(String, Value)> {
        self.completed.lock().unwrap().clone()
    }

    fn queued(&self) -> Vec<(String, String, String)> {
        self.queued.lock().unwrap().clone()
    }
}

impl ProvidesHost for RecordingProvidesHost {
    fn complete(&self, id: &str, outcome_json: &str) -> Result<(), ProvidesCallError> {
        if let Some(detail) = &self.refuse_outcome {
            return Err(ProvidesCallError::InvalidOutcome(detail.clone()));
        }
        if !self.pending.lock().unwrap().remove(id) {
            return Err(ProvidesCallError::NotPending);
        }
        let outcome: Value = serde_json::from_str(outcome_json)
            .map_err(|e| ProvidesCallError::InvalidOutcome(e.to_string()))?;
        self.completed
            .lock()
            .unwrap()
            .push((id.to_string(), outcome));
        Ok(())
    }

    fn queue_async_for(
        &self,
        serving: &str,
        tool: &str,
        args_json: &str,
        call_id: &str,
    ) -> Result<QueuedToolRequest, ToolCallError> {
        if !self.pending.lock().unwrap().contains(serving) {
            return Err(ToolCallError::Denied("not pending".to_string()));
        }
        if self.refuse_nested {
            return Err(ToolCallError::Denied("clause miss".to_string()));
        }
        self.queued.lock().unwrap().push((
            serving.to_string(),
            tool.to_string(),
            call_id.to_string(),
        ));
        let args: Value = serde_json::from_str(args_json).expect("fixture args are JSON");
        Ok(QueuedToolRequest {
            channel: format!("brenn:tools/{tool}"),
            reply_to: format!("brenn:tool-results/{SLUG}"),
            body_json: json!({
                "v": 1, "tool": tool, "call_id": call_id, "caller": SLUG,
                "args": args, "serving": serving,
            })
            .to_string(),
        })
    }
}

/// A plain `call-async` seam that records `(tool, call_id)` per admitted call.
#[derive(Default)]
struct RecordingToolHost {
    queued: Mutex<Vec<(String, String)>>,
}

impl ToolHost for RecordingToolHost {
    fn fast_call(&self, _tool: &str, _args_json: &str) -> Result<String, ToolCallError> {
        Err(ToolCallError::NotGranted)
    }

    fn queue_async(
        &self,
        tool: &str,
        args_json: &str,
        call_id: &str,
    ) -> Result<QueuedToolRequest, ToolCallError> {
        self.queued
            .lock()
            .unwrap()
            .push((tool.to_string(), call_id.to_string()));
        let args: Value = serde_json::from_str(args_json).expect("fixture args are JSON");
        Ok(QueuedToolRequest {
            channel: format!("brenn:tools/{tool}"),
            reply_to: format!("brenn:tool-results/{SLUG}"),
            body_json: json!({
                "v": 1, "tool": tool, "call_id": call_id, "caller": SLUG, "args": args,
            })
            .to_string(),
        })
    }
}

fn load(grants: &[ComponentGrant], host: Option<Arc<RecordingProvidesHost>>) -> ProcessorComponent {
    let tool_host = grants
        .contains(&ComponentGrant::Tools)
        .then(common::noop_tool_host);
    load_with(grants, host, tool_host)
}

fn load_with(
    grants: &[ComponentGrant],
    host: Option<Arc<RecordingProvidesHost>>,
    tool_host: Option<ToolHostFn>,
) -> ProcessorComponent {
    let grants: BTreeSet<ComponentGrant> = grants.iter().copied().collect();
    ProcessorComponent::load(ProcessorLoadSpec {
        input_amplification_mt: HashMap::from([
            (REQUESTS.to_string(), 1000),
            (RESULTS.to_string(), 1000),
        ]),
        grants,
        alerter: common::noop_alerter(),
        output_acl: common::allow_all(),
        tool_host,
        provides_host: host.map(|host| host as brenn_wasm::ProvidesHostFn),
        ..ProcessorLoadSpec::minimal(&component_path(), SLUG)
    })
}

fn provider(host: &Arc<RecordingProvidesHost>) -> ProcessorComponent {
    load(
        &[ComponentGrant::Tools, ComponentGrant::Provides],
        Some(host.clone()),
    )
}

fn envelope(port: &str, n: usize, body: &Value) -> String {
    json!({
        "message_id": format!("00000000-0000-0000-0000-{n:012}"),
        "source": "system:tool-executor",
        "channel": format!("brenn:{port}/{SLUG}"),
        "sender": "system:tool-executor",
        "publish_ts": "2026-01-01T00:00:00Z",
        "body": body.to_string(),
        "urgency": "normal",
        "envelope_type": "brenn",
    })
    .to_string()
}

fn request(id: &str, mode: &str) -> Value {
    json!({
        "v": 1, "id": id, "tool": "echo-for-user",
        "args": { "mode": mode, "request": { "url": "https://example.com/x" } },
    })
}

/// A window on `port` whose `bodies` are all new.
fn window(port: &str, bodies: &[Value]) -> ProcessorPortWindow {
    ProcessorPortWindow {
        port: port.to_string(),
        envelopes: bodies
            .iter()
            .enumerate()
            .map(|(n, body)| envelope(port, n, body))
            .collect(),
        new_from: 0,
        dropped: 0,
    }
}

fn activation_of(ports: Vec<ProcessorPortWindow>) -> ProcessorActivation {
    ProcessorActivation {
        ports,
        deferred: vec![],
        now: None,
        sync: None,
    }
}

/// One activation carrying `requests` new on `tool-requests` and `results` new
/// on `tool-results`.
fn activation(requests: &[Value], results: &[Value]) -> ProcessorActivation {
    activation_of(vec![window(REQUESTS, requests), window(RESULTS, results)])
}

#[test]
fn an_echo_request_is_completed_through_the_seam() {
    let host = RecordingProvidesHost::pending(&["a"]);
    let outcome = provider(&host).handle(activation(&[request("a", "echo")], &[]));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        host.completed(),
        [(
            "a".to_string(),
            json!({ "ok": request("a", "echo")["args"] })
        )]
    );
}

/// A completion is the caller's answer, not a buffered side effect: the guest
/// failing later in the same activation does not take it back.
#[test]
fn a_completion_survives_the_activation_failing_after_it() {
    let host = RecordingProvidesHost::pending(&["a", "b"]);
    let outcome = provider(&host).handle(activation(
        &[request("a", "echo"), request("b", "fail")],
        &[],
    ));
    assert!(matches!(outcome, ProcessorOutcome::Err(_)), "{outcome:?}");
    let completed: Vec<String> = host.completed().into_iter().map(|(id, _)| id).collect();
    assert_eq!(completed, ["a"]);
}

#[test]
fn call_async_for_rides_the_ok_flush_as_a_tool_request() {
    let host = RecordingProvidesHost::pending(&["a"]);
    let outcome = provider(&host).handle(activation(&[request("a", "api")], &[]));
    let ProcessorOutcome::Ok { publishes, .. } = outcome else {
        panic!("an api-mode activation returns ok, got {outcome:?}");
    };
    assert_eq!(
        host.queued(),
        [("a".to_string(), "api-request".to_string(), "a".to_string())]
    );
    assert_eq!(publishes.len(), 1, "{publishes:?}");
    assert_eq!(publishes[0].channel_address, "brenn:tools/api-request");
    assert_eq!(
        publishes[0].reply_to.as_deref(),
        Some("brenn:tool-results/provider")
    );
    let body: Value = serde_json::from_str(&publishes[0].payload).unwrap();
    assert_eq!(body["serving"], "a");
    assert!(
        host.completed().is_empty(),
        "nothing completes before the result"
    );
}

/// The on-behalf-of call is transactional like `call-async`: queued at the
/// seam, then dropped with the failed activation's buffer, so the next ok
/// activation of the same component flushes no trace of it.
#[test]
fn call_async_for_is_discarded_with_a_failed_activation() {
    let host = RecordingProvidesHost::pending(&["a", "b", "c"]);
    let component = provider(&host);
    let outcome = component.handle(activation(
        &[request("a", "api"), request("b", "fail")],
        &[],
    ));
    assert!(matches!(outcome, ProcessorOutcome::Err(_)), "{outcome:?}");
    assert_eq!(host.queued().len(), 1, "the call reached the seam");

    let outcome = component.handle(activation(&[request("c", "echo")], &[]));
    let ProcessorOutcome::Ok { publishes, .. } = outcome else {
        panic!("the echo activation returns ok, got {outcome:?}");
    };
    assert!(publishes.is_empty(), "{publishes:?}");
}

/// A request delivered after its record is gone (a restart, an expiry) is
/// refused at `call-async-for`; completing it then draws `not-pending`, so
/// the provider drops it and still serves the live request beside it.
#[test]
fn an_api_request_no_longer_pending_is_dropped_without_failing_the_activation() {
    let host = RecordingProvidesHost::pending(&["live"]);
    let outcome = provider(&host).handle(activation(
        &[request("stale", "api"), request("live", "echo")],
        &[],
    ));
    let ProcessorOutcome::Ok { publishes, .. } = outcome else {
        panic!("a stale request must not fail the activation, got {outcome:?}");
    };
    assert!(publishes.is_empty(), "{publishes:?}");
    assert!(host.queued().is_empty(), "the refused call is not queued");
    let completed: Vec<String> = host.completed().into_iter().map(|(id, _)| id).collect();
    assert_eq!(completed, ["live"]);
}

/// A live request whose nested call is refused is answered with the refusal
/// at once, not left to the ceiling.
#[test]
fn a_refused_nested_call_completes_its_request_with_the_refusal() {
    let host = RecordingProvidesHost::refusing_nested(&["a"]);
    let outcome = provider(&host).handle(activation(&[request("a", "api")], &[]));
    let ProcessorOutcome::Ok { publishes, .. } = outcome else {
        panic!("a refused nested call is answered, not raised, got {outcome:?}");
    };
    assert!(publishes.is_empty(), "{publishes:?}");
    let completed = host.completed();
    assert_eq!(completed.len(), 1, "{completed:?}");
    assert_eq!(completed[0].0, "a");
    assert_eq!(completed[0].1["err"]["kind"], "internal");
    let detail = completed[0].1["err"]["detail"].as_str().unwrap();
    assert!(
        detail.contains("call-async api-request: denied: clause miss"),
        "{detail}"
    );
}

#[test]
fn a_result_naming_its_request_completes_it() {
    let host = RecordingProvidesHost::pending(&["a"]);
    let result = json!({
        "v": 1, "tool": "api-request", "call_id": "a",
        "outcome": { "ok": { "status": 200 } }, "serving": "a",
    });
    let outcome = provider(&host).handle(activation(&[], &[result]));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        host.completed(),
        [("a".to_string(), json!({ "ok": { "status": 200 } }))]
    );
}

/// A result for a request the host no longer holds is dropped by the guest
/// through `Error::NotPending`, and the activation still succeeds.
#[test]
fn a_result_for_a_request_no_longer_pending_is_dropped() {
    let host = RecordingProvidesHost::pending(&[]);
    let result = json!({
        "v": 1, "tool": "api-request", "call_id": "gone",
        "outcome": { "err": { "kind": "internal", "detail": "x" } }, "serving": "gone",
    });
    let outcome = provider(&host).handle(activation(&[], &[result]));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "{outcome:?}"
    );
    assert!(host.completed().is_empty());
}

/// The SDK parses version 1 of both host-built bodies only; any other version
/// is a host/guest skew and fails the activation as a malformed envelope.
#[test]
fn a_request_or_result_of_another_version_fails_the_activation() {
    let mut request = request("a", "echo");
    request["v"] = json!(2);
    let result = json!({
        "v": 2, "tool": "api-request", "call_id": "a",
        "outcome": { "ok": { "status": 200 } }, "serving": "a",
    });
    for (requests, results, what) in [
        (vec![request], vec![], "tool request"),
        (vec![], vec![result], "tool result"),
    ] {
        let host = RecordingProvidesHost::pending(&["a"]);
        let outcome = provider(&host).handle(activation(&requests, &results));
        let ProcessorOutcome::Err(ProcessorReceiveError::MalformedEnvelope(detail)) = outcome
        else {
            panic!("a version-2 {what} must fail the activation, got {outcome:?}");
        };
        assert!(
            detail.contains(&format!("{what} body: version 2 is not 1")),
            "{detail}"
        );
        assert!(host.completed().is_empty());
    }
}

/// `invalid-outcome` reaches the guest with the host's detail, as a failure
/// distinct from `not-pending`; the fixture's echo mode returns it.
#[test]
fn an_invalid_outcome_refusal_reaches_the_guest_with_its_detail() {
    let host = RecordingProvidesHost::refusing_outcomes(&["a"], "value over cap");
    let outcome = provider(&host).handle(activation(&[request("a", "echo")], &[]));
    let ProcessorOutcome::Err(ProcessorReceiveError::ProcessingFailed(detail)) = outcome else {
        panic!("the echo mode returns the refusal, got {outcome:?}");
    };
    assert_eq!(detail, "complete a: invalid-outcome: value over cap");
    assert!(host.is_pending("a"));
}

/// A plain `call-async` from a provider reaches the ordinary tool seam, not
/// the on-behalf-of one, and its request names no `serving`.
#[test]
fn a_plain_call_async_from_a_provider_acts_for_no_one() {
    let host = RecordingProvidesHost::pending(&["a"]);
    let tools = Arc::new(RecordingToolHost::default());
    let component = load_with(
        &[ComponentGrant::Tools, ComponentGrant::Provides],
        Some(host.clone()),
        Some(tools.clone() as ToolHostFn),
    );
    let outcome = component.handle(activation(&[request("a", "plain")], &[]));
    let ProcessorOutcome::Ok { publishes, .. } = outcome else {
        panic!("a plain-mode activation returns ok, got {outcome:?}");
    };
    assert_eq!(
        *tools.queued.lock().unwrap(),
        [("api-request".to_string(), "a".to_string())]
    );
    assert!(host.queued().is_empty());
    assert_eq!(publishes.len(), 1, "{publishes:?}");
    let body: Value = serde_json::from_str(&publishes[0].payload).unwrap();
    assert!(body.get("serving").is_none(), "{body}");
    assert_eq!(
        host.completed(),
        [("a".to_string(), json!({ "ok": { "queued": true } }))]
    );
}

#[test]
fn complete_err_writes_the_kind_token_and_detail() {
    let host = RecordingProvidesHost::pending(&["a"]);
    let outcome = provider(&host).handle(activation(&[request("a", "bogus")], &[]));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        host.completed(),
        [(
            "a".to_string(),
            json!({ "err": { "kind": "invalid_args", "detail": "unknown mode Some(\"bogus\")" } })
        )]
    );
}

/// Fields the host adds to a version-1 request body do not stop the SDK
/// parsing it.
#[test]
fn a_request_carrying_an_unknown_field_is_served() {
    let host = RecordingProvidesHost::pending(&["a"]);
    let mut body = request("a", "echo");
    body["added_later"] = json!("x");
    let outcome = provider(&host).handle(activation(&[body], &[]));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "{outcome:?}"
    );
    let completed: Vec<String> = host.completed().into_iter().map(|(id, _)| id).collect();
    assert_eq!(completed, ["a"]);
}

/// `tool_requests()` and `tool_results()` refuse an activation without their
/// reserved port as a malformed envelope naming the port.
#[test]
fn a_missing_reserved_port_fails_the_activation() {
    for (ports, missing) in [
        (vec![window(RESULTS, &[])], REQUESTS),
        (vec![window(REQUESTS, &[])], RESULTS),
    ] {
        let host = RecordingProvidesHost::pending(&[]);
        let outcome = provider(&host).handle(activation_of(ports));
        let ProcessorOutcome::Err(ProcessorReceiveError::MalformedEnvelope(detail)) = outcome
        else {
            panic!("an activation without `{missing}` must fail, got {outcome:?}");
        };
        assert_eq!(detail, format!("no `{missing}` port in this activation"));
    }
}

#[test]
fn completing_an_id_not_pending_is_refused_not_pending() {
    let host = RecordingProvidesHost::pending(&["a"]);
    let outcome = provider(&host).handle(activation(&[request("a", "stale")], &[]));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "{outcome:?}"
    );
    let completed = host.completed();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].0, "a");
    let drew = completed[0].1["ok"]["stale"].as_str().unwrap();
    assert!(drew.ends_with("not-pending"), "{drew}");
}

#[test]
#[should_panic(expected = "requires ungranted capability \"provides\"")]
fn a_provides_import_without_the_grant_is_refused_at_load() {
    load(&[ComponentGrant::Tools], None);
}

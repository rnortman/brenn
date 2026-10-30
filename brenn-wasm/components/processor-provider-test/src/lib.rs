// Provided-tool fixture for the `brenn:processor` world: the one component that
// imports `brenn:processor/provides`.
//
// It serves every new request on its `tool-requests` port, choosing what to do
// by the request's `mode` argument:
//
//   "echo"  — complete at once with the request's arguments.
//   "api"   — issue `call-async-for(<id>, "api-request", <args.request>)` and
//             complete later, on the result activation, with that result. A
//             refused call is completed at once with an `internal` error
//             naming the refusal; a request no longer pending draws
//             `not-pending` there and is dropped.
//   "plain" — issue an ordinary `call-async("api-request", <args.request>)`,
//             which acts for no one, and complete at once with
//             `{"queued": true}`.
//   "stale" — complete a made-up id, then complete the real request with the
//             error text that drew and whether `drop_not_pending` dropped a
//             second such completion: `{"stale": "<diagnostic>", "dropped": true}`.
//   "fail"  — return `Err` from the activation, completing nothing.
//   other   — complete with an `invalid_args` error.
//
// Both reserved ports are bound, so both appear in every activation; a
// missing one fails the activation.
//
// On the `tool-results` port, each result that names the request it serves
// completes that request: its value on success, an `internal` error carrying
// the tool's error otherwise. A result whose request is no longer pending is
// dropped, as a provider must after an expiry or a restart.

use brenn_guest::provides::{self, OutcomeErrorKind, PendingId, ToolRequest, drop_not_pending};
use brenn_guest::serde_json::{Value, json};
use brenn_guest::tools::{self, ToolResult};
use brenn_guest::{Activation, Error, Processor};

/// The tool every nested call reaches.
const NESTED_TOOL: &str = "api-request";

struct ProcessorProviderTest;

impl Processor for ProcessorProviderTest {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        for request in activation.tool_requests()? {
            serve(&request)?;
        }
        for result in activation.tool_results()? {
            answer(result)?;
        }
        Ok(None)
    }
}

fn serve(request: &ToolRequest) -> Result<(), Error> {
    let nested = request.args.get("request").cloned().unwrap_or(Value::Null);
    match request.args.get("mode").and_then(Value::as_str) {
        Some("echo") => provides::complete(&request.id, &request.args),
        Some("api") => {
            match provides::call_async_for(&request.id, NESTED_TOOL, &nested, request.id.as_str()) {
                Ok(()) => Ok(()),
                Err(refused) => drop_not_pending(provides::complete_err(
                    &request.id,
                    OutcomeErrorKind::Internal,
                    &refused.to_string(),
                )),
            }
        }
        Some("plain") => {
            tools::call_async_json(NESTED_TOOL, &nested, request.id.as_str())?;
            provides::complete(&request.id, &json!({ "queued": true }))
        }
        Some("stale") => {
            let made_up = PendingId::from("0".repeat(32));
            let drew = match provides::complete(&made_up, &json!({})) {
                Err(Error::NotPending(diagnostic)) => diagnostic,
                other => format!("unexpected: {other:?}"),
            };
            let dropped = drop_not_pending(provides::complete(&made_up, &json!({}))).is_ok();
            provides::complete(&request.id, &json!({ "stale": drew, "dropped": dropped }))
        }
        // Through `drop_not_pending`, which passes every error but `NotPending`.
        Some("fail") => drop_not_pending(Err(Error::failed("fail mode"))),
        other => provides::complete_err(
            &request.id,
            OutcomeErrorKind::InvalidArgs,
            &format!("unknown mode {other:?}"),
        ),
    }
}

fn answer(result: ToolResult) -> Result<(), Error> {
    let Some(id) = result.serving else {
        return Ok(());
    };
    let completed = match result.outcome {
        Ok(value) => provides::complete(&id, &value),
        Err(err) => provides::complete_err(
            &id,
            OutcomeErrorKind::Internal,
            &format!("{}: {}", err.kind, err.detail),
        ),
    };
    drop_not_pending(completed)
}

brenn_guest::export_processor!(ProcessorProviderTest);

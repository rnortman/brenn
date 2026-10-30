//! Google Maps Platform tools (brenn:processor world).
//!
//! Each activation reads the instance's `config`, opens one store transaction,
//! sweeps in-flight rows no result can still arrive for, serves every new
//! request on `tool-requests`, answers every new result on `tool-results`, and
//! commits before returning, so the nested calls it buffered flush against
//! committed state.
//!
//! Serving a request validates its arguments and issues one nested
//! `api-request` for the caller's user through `call_async_for`, recording the
//! request's state in the store under the call's id. Answering a result reads
//! that state back, deletes it, and completes the request from
//! `api-request`'s answer. A request's own problem — bad arguments, Brenn's
//! refusal, Google's error — is answered with `complete_err`; only a
//! structural fault (a bad `config`, a store failure, a malformed envelope)
//! fails the activation. A completion the host no longer holds is dropped.

mod logic;
mod spec;

use brenn_guest::provides::{OutcomeErrorKind, PendingId, ToolRequest, drop_not_pending};
use brenn_guest::serde_json::Value;
use brenn_guest::tools::ToolResult;
use brenn_guest::{Activation, Error, Processor};

use crate::logic::{Call, ErrorKind, InFlight, NestedError, Next, Settings};
use crate::spec::{Provided, config, log, provides, store};

/// The store namespace of in-flight rows, keyed by call id.
const INFLIGHT_NS: &str = "inflight";
/// The tool every nested call reaches.
const NESTED_TOOL: &str = "api-request";

struct GoogleMapsTools;

impl Processor for GoogleMapsTools {
    fn receive(activation: Activation) -> Result<Option<String>, Error> {
        let settings = Settings::from_lookup(config::get).map_err(Error::failed)?;
        let now_ms = activation
            .now()
            .expect("backend activation carries a host-stamped now");
        let tx = store::begin()?;
        sweep(&tx, now_ms)?;
        for request in activation.tool_requests()? {
            serve(&tx, &settings, now_ms, &request)?;
        }
        for result in activation.tool_results()? {
            answer(&tx, result)?;
        }
        tx.commit()?;
        Ok(None)
    }
}

/// Delete every in-flight row older than the TTL: its request was answered at
/// the host's ceiling or lost to a restart, and no result for it will come.
/// The sweep reads each row's age alone, so a row written by another build
/// still expires.
fn sweep(tx: &store::Transaction, now_ms: u64) -> Result<(), Error> {
    for (key, bytes) in tx.scan(INFLIGHT_NS, b"", None, 0)? {
        if logic::row_expired(&bytes, now_ms).map_err(Error::failed)? {
            log::debug(format!("swept in-flight {}", String::from_utf8_lossy(&key)));
            tx.delete(INFLIGHT_NS, &key)?;
        }
    }
    Ok(())
}

fn serve(
    tx: &store::Transaction,
    settings: &Settings,
    now_ms: u64,
    request: &ToolRequest,
) -> Result<(), Error> {
    let tool = Provided::of(request)?;
    match logic::plan(tool, &request.args, settings) {
        Err(reason) => drop_not_pending(provides::complete_err(
            &request.id,
            OutcomeErrorKind::InvalidArgs,
            &reason,
        )),
        Ok(call) => issue(tx, &request.id, tool, &request.args, &call, now_ms),
    }
}

/// Issue `call` for request `id` and record its state, or answer the request
/// with Brenn's refusal when the call cannot be queued.
fn issue(
    tx: &store::Transaction,
    id: &PendingId,
    tool: Provided,
    args: &Value,
    call: &Call,
    now_ms: u64,
) -> Result<(), Error> {
    let state = InFlight::new(id.as_str(), tool, args, call, now_ms);
    let call_id = state.call_id();
    match provides::call_async_for(id, NESTED_TOOL, &call.api, &call_id) {
        Ok(()) => {
            log::info(format!(
                "{} step {} {} {}{}",
                tool.name(),
                call.step,
                call.api.method,
                call.api.host,
                call.api.path
            ));
            tx.put(INFLIGHT_NS, call_id.as_bytes(), &state.encode())
        }
        Err(refused) => drop_not_pending(provides::complete_err(
            id,
            OutcomeErrorKind::Internal,
            &logic::not_issued(&refused.to_string()),
        )),
    }
}

fn answer(tx: &store::Transaction, result: ToolResult) -> Result<(), Error> {
    // Every call this component makes serves a request.
    let Some(id) = result.serving else {
        log::debug(format!(
            "ignored result {} serving no request",
            result.call_id
        ));
        return Ok(());
    };
    let row = tx.get(INFLIGHT_NS, result.call_id.as_bytes())?;
    let Some(state) =
        logic::claim(id.as_str(), &result.call_id, row.as_deref()).map_err(Error::failed)?
    else {
        log::debug(format!(
            "result for {} with no in-flight state: duplicate or stale",
            result.call_id
        ));
        return Ok(());
    };
    tx.delete(INFLIGHT_NS, result.call_id.as_bytes())?;
    let outcome = result.outcome.map_err(|error| NestedError {
        kind: error.kind,
        detail: error.detail,
    });
    log::info(format!(
        "{} step {} result {}",
        state.tool.name(),
        state.step,
        logic::result_summary(&outcome)
    ));
    match logic::advance(&state, outcome) {
        Next::Complete(value) => drop_not_pending(provides::complete(&id, &value)),
        Next::Fail(failure) => drop_not_pending(provides::complete_err(
            &id,
            outcome_kind(failure.kind),
            &failure.detail,
        )),
    }
}

fn outcome_kind(kind: ErrorKind) -> OutcomeErrorKind {
    match kind {
        ErrorKind::Denied => OutcomeErrorKind::Denied,
        ErrorKind::InvalidArgs => OutcomeErrorKind::InvalidArgs,
        ErrorKind::Internal => OutcomeErrorKind::Internal,
    }
}

brenn_guest::export_processor!(GoogleMapsTools);

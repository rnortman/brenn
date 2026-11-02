//! `brenn-provider-kit`'s activation loop, served through the `brenn-guest`
//! SDK.
//!
//! A provider component's `Processor::receive` reads its settings and calls
//! [`serve`] with its [`Logic`]; everything else — the sweep, the results, the
//! backlog, the new requests, the call budget — is the kit's loop, run against
//! one store transaction that commits when the loop returns `Ok`, so the
//! nested calls it buffered flush against committed state. An `Err` drops the
//! transaction, which rolls back every row the activation wrote.
//!
//! The component's class needs the `provides`, `store` and `log` grants and a
//! `tool-results` inbox; nested calls reach `api-request` on behalf of the
//! request's user. A completion the host no longer holds is dropped.

use brenn_guest::provides::{self, OutcomeErrorKind, PendingId, drop_not_pending};
use brenn_guest::serde_json::Value;
use brenn_guest::{Activation, Error, log, store};
use brenn_provider_kit::activation::NESTED_TOOL;
use brenn_provider_kit::{
    ApiCall, Delivered, ErrorKind, Host, Input, Level, Logic, NestedError, Request, Rows,
};

/// Serve one activation of a provider built on `brenn-provider-kit`: one store
/// transaction, the kit's loop, commit.
pub fn serve<L: Logic>(activation: &Activation, settings: &L::Settings) -> Result<(), Error> {
    let now_ms = activation
        .now()
        .expect("backend activation carries a host-stamped now");
    let requests = activation
        .tool_requests()?
        .into_iter()
        .map(|request| Request {
            id: request.id.as_str().to_owned(),
            tool: request.tool,
            args: request.args,
        })
        .collect();
    let results = activation
        .tool_results()?
        .into_iter()
        .map(|result| Delivered {
            call_id: result.call_id,
            serving: result.serving.map(|id| id.as_str().to_owned()),
            outcome: result.outcome.map_err(|error| NestedError {
                kind: error.kind,
                detail: error.detail,
            }),
        })
        .collect();
    let tx = store::begin()?;
    brenn_provider_kit::serve::<_, L>(
        &GuestHost(&tx),
        settings,
        Input {
            now_ms,
            requests,
            results,
        },
    )?;
    tx.commit()
}

/// The kit's [`Host`] over one store transaction and the SDK's `provides` and
/// `log` calls.
struct GuestHost<'a>(&'a store::Transaction);

impl Host for GuestHost<'_> {
    type Error = Error;

    fn fault(detail: String) -> Error {
        Error::failed(detail)
    }

    fn get(&self, ns: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.0.get(ns, key)
    }

    fn put(&self, ns: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.0.put(ns, key, value)
    }

    fn delete(&self, ns: &str, key: &[u8]) -> Result<(), Error> {
        self.0.delete(ns, key)
    }

    fn scan(&self, ns: &str) -> Result<Rows, Error> {
        self.0.scan(ns, b"", None, 0)
    }

    fn call(&self, id: &str, call: &ApiCall, call_id: &str) -> Result<(), String> {
        provides::call_async_for(&pending(id), NESTED_TOOL, call, call_id)
            .map_err(|refused| refused.to_string())
    }

    fn complete(&self, id: &str, value: &Value) -> Result<(), Error> {
        drop_not_pending(provides::complete(&pending(id), value))
    }

    fn complete_err(&self, id: &str, kind: ErrorKind, detail: &str) -> Result<(), Error> {
        drop_not_pending(provides::complete_err(
            &pending(id),
            outcome_kind(kind),
            detail,
        ))
    }

    fn log(&self, level: Level, line: String) {
        match level {
            Level::Debug => log::debug(line),
            Level::Info => log::info(line),
            Level::Warn => log::warn(line),
        }
    }
}

fn pending(id: &str) -> PendingId {
    PendingId::from(id.to_owned())
}

fn outcome_kind(kind: ErrorKind) -> OutcomeErrorKind {
    match kind {
        ErrorKind::Denied => OutcomeErrorKind::Denied,
        ErrorKind::InvalidArgs => OutcomeErrorKind::InvalidArgs,
        ErrorKind::Internal => OutcomeErrorKind::Internal,
    }
}

// The one seam between the SDK and the generated bindings.
//
// Every generated path, and every record of the contract's `bus`, `ports` and
// `calls` interfaces, is named here and nowhere else in the SDK: the rest of
// the crate calls the aliases and conversions below, so a change of contract
// costs `bindings.rs` and this file. The bindings are the byte-carrying
// contract's: a body crosses as its content type and bytes, unchanged.
//
// `pub` and `#[doc(hidden)]` because `export_processor!` expands in the
// component's crate and names `receive` and the two raw types; nothing here is
// API.

use crate::bindings;
use crate::{
    Activation, Body, BodyRef, DeferredEntry, DeferredWindow, Error, Message, PortWindow,
    Processor, Urgency, calls::CallError,
};

pub(crate) use bindings::brenn::processor0_1_0::{
    alert, config, dom, log, mqtt, page_dom, provides, store, tools,
};
use bindings::brenn::processor0_2_0::{bus, calls as raw_calls, ports};

/// The activation record the export receives.
pub type RawActivation = bus::Activation;

/// The export's answer: the reply, or the guest's error.
pub type RawReply = Result<Option<bus::BodyResult>, bus::ReceiveError>;

/// The body of the export shim `export_processor!` expands to: build the
/// [`Activation`], run `P::receive`, lower the answer.
pub fn receive<P: Processor>(raw: RawActivation) -> RawReply {
    let activation = activation(raw).map_err(lower_error)?;
    let reply = P::receive(activation).map_err(lower_error)?;
    Ok(reply.map(|body| {
        let (content_type, bytes) = body.into_parts();
        bus::BodyResult {
            content_type,
            bytes,
        }
    }))
}

fn lower_error(e: Error) -> bus::ReceiveError {
    match e {
        Error::MalformedEnvelope(msg) => bus::ReceiveError::MalformedEnvelope(msg),
        // The host has one failure arm for the rest: the distinction is the
        // guest's, drawn so a component can decide, and the diagnostic text
        // already names what failed.
        Error::ProcessingFailed(msg)
        | Error::QuotaExceeded(msg)
        | Error::NotPending(msg)
        | Error::TextOnlyChannel(msg)
        | Error::InvalidBody(msg) => bus::ReceiveError::ProcessingFailed(msg),
    }
}

/// A host body as the SDK's, moved.
fn body(raw: bus::BodyResult) -> Body {
    Body::from_parts(raw.content_type, raw.bytes)
}

/// The host's borrowed body for an SDK one: no copy.
fn raw_body<'a>(body: BodyRef<'a>) -> bus::BodyParam<'a> {
    bus::BodyParam {
        content_type: body.content_type(),
        bytes: body.as_bytes(),
    }
}

/// Validate and convert the activation record.
///
/// `Err(ProcessingFailed)` when a window's `new_from` exceeds its length (a
/// host contract violation), so every [`PortWindow`] accessor may slice
/// unconditionally.
fn activation(raw: RawActivation) -> Result<Activation, Error> {
    let mut windows = Vec::with_capacity(raw.ports.len());
    for pw in raw.ports {
        let new_from = pw.new_from as usize;
        let len = pw.envelopes.len();
        if new_from > len {
            return Err(Error::failed(format!(
                "host invariant violation: new_from {new_from} > {len} on port {}",
                pw.port
            )));
        }
        let messages = pw
            .envelopes
            .into_iter()
            .map(|e| Message::new(e.header, body(e.body)))
            .collect();
        windows.push(PortWindow {
            port: pw.port,
            dropped: pw.dropped,
            messages,
            new_from,
        });
    }
    let deferred = raw
        .deferred
        .into_iter()
        .map(|dw| DeferredWindow {
            port: dw.port,
            entries: dw
                .entries
                .into_iter()
                .map(|e| DeferredEntry {
                    index: e.index,
                    body: body(e.body),
                    deliver_after: e.deliver_after,
                })
                .collect(),
        })
        .collect();
    Ok(Activation {
        windows,
        deferred,
        now: raw.now,
        sync: raw.sync,
    })
}

/// The fix named in every non-text refusal.
const NON_TEXT_FIX: &str = "declare `non_text = true` on the channel, or on the binding where the channel stays on the page";

/// A `PublishError` as the SDK's `Error`, with a per-port diagnostic.
fn publish_error(port: &str, body: BodyRef<'_>, e: ports::PublishError) -> Error {
    use ports::PublishError;
    match e {
        PublishError::NotPermitted => {
            Error::ProcessingFailed(format!("publish to {port}: not-permitted"))
        }
        PublishError::InvalidPayload(m) => {
            Error::ProcessingFailed(format!("publish to {port}: invalid-payload: {m}"))
        }
        PublishError::QuotaExceeded => {
            Error::QuotaExceeded(format!("publish to {port}: quota-exceeded"))
        }
        PublishError::InvalidBody(m) => Error::InvalidBody(format!(
            "publish to {port}: invalid-body ({}): {m}",
            body.describe()
        )),
        PublishError::TextOnlyChannel => Error::TextOnlyChannel(format!(
            "publish to {port}: text-only-channel: a non-text body ({}) on a channel not \
             declared non_text; {NON_TEXT_FIX}",
            body.describe()
        )),
    }
}

/// A `DeferError` as the SDK's `Error`, with a per-port diagnostic.
fn defer_error(port: &str, body: Option<BodyRef<'_>>, e: ports::DeferError) -> Error {
    use ports::DeferError;
    let describe = || body.map_or_else(|| String::from("no body"), |b| b.describe());
    let word = match e {
        DeferError::NotPermitted => "not-permitted",
        DeferError::OutOfRange => "out-of-range",
        DeferError::QuotaExceeded => {
            return Error::QuotaExceeded(format!("defer control on {port}: quota-exceeded"));
        }
        DeferError::InvalidDeliverAfter => "invalid-deliver-after",
        DeferError::InvalidBody(m) => {
            return Error::InvalidBody(format!(
                "defer edit on {port}: invalid-body ({}): {m}",
                describe()
            ));
        }
        DeferError::TextOnlyChannel => {
            return Error::TextOnlyChannel(format!(
                "defer edit on {port}: text-only-channel: a non-text body ({}) on a channel \
                 not declared non_text; {NON_TEXT_FIX}",
                describe()
            ));
        }
    };
    Error::ProcessingFailed(format!("defer control on {port}: {word}"))
}

/// `brenn_envelope::Urgency` to the contract's, exhaustively: variant drift
/// on either side fails compilation.
fn urgency(u: Urgency) -> ports::Urgency {
    match u {
        Urgency::VeryLow => ports::Urgency::VeryLow,
        Urgency::Low => ports::Urgency::Low,
        Urgency::Normal => ports::Urgency::Normal,
        Urgency::High => ports::Urgency::High,
    }
}

pub(crate) fn publish(port: &str, body: BodyRef<'_>) -> Result<(), Error> {
    ports::publish(port, raw_body(body)).map_err(|e| publish_error(port, body, e))
}

pub(crate) fn publish_with_urgency(
    port: &str,
    body: BodyRef<'_>,
    level: Urgency,
) -> Result<(), Error> {
    ports::publish_with_urgency(port, raw_body(body), urgency(level))
        .map_err(|e| publish_error(port, body, e))
}

pub(crate) fn publish_deferred(
    port: &str,
    body: BodyRef<'_>,
    deliver_after: u64,
) -> Result<(), Error> {
    ports::publish_deferred(port, raw_body(body), deliver_after)
        .map_err(|e| publish_error(port, body, e))
}

pub(crate) fn defer_cancel(port: &str, index: u32) -> Result<(), Error> {
    ports::defer_cancel(port, index).map_err(|e| defer_error(port, None, e))
}

pub(crate) fn defer_edit(
    port: &str,
    index: u32,
    body: Option<BodyRef<'_>>,
    deliver_after: Option<u64>,
) -> Result<(), Error> {
    ports::defer_edit(port, index, body.map(raw_body), deliver_after)
        .map_err(|e| defer_error(port, body, e))
}

pub(crate) fn call(port: &str, request: BodyRef<'_>) -> Result<Option<Body>, CallError> {
    match raw_calls::call(port, raw_body(request)) {
        Ok(reply) => Ok(reply.map(body)),
        Err(e) => Err(match e {
            raw_calls::CallError::NotPermitted => CallError::NotPermitted,
            raw_calls::CallError::Unwired => CallError::Unwired,
            raw_calls::CallError::Refused => CallError::Refused,
            raw_calls::CallError::Failed => CallError::Failed,
            raw_calls::CallError::InvalidPayload(m) => CallError::InvalidPayload(m),
            raw_calls::CallError::InvalidBody(m) => CallError::InvalidBody(m),
            raw_calls::CallError::TextOnlyPeer => CallError::TextOnlyPeer,
            raw_calls::CallError::QuotaExceeded => CallError::QuotaExceeded,
        }),
    }
}

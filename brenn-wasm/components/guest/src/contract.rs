// The one seam between the SDK and the generated bindings.
//
// Every generated path, and every record of the contract's `bus`, `ports`,
// `calls` and `blobs` interfaces, is named here and nowhere else in the SDK:
// the rest of the crate calls the aliases and conversions below, so a change
// of contract costs `bindings.rs` and this file. The bindings are the
// byte-carrying contract's: a body crosses as its content type and bytes,
// unchanged.
//
// `pub` and `#[doc(hidden)]` because `export_processor!` expands in the
// component's crate and names `receive` and the two raw types; nothing here is
// API.

use crate::bindings;
use crate::{
    Activation, Body, BodyRef, DeferredEntry, DeferredWindow, Error, Message, PortWindow,
    Processor, PublishAttachmentsError, PublishOptions, Urgency,
    blobs::{BlobLimits, CreateError, Gap, OpenError, ReadError, SealError, WriteError},
    calls::CallError,
    provides::CompleteAttachmentsError,
    tools::CallAttachmentsError,
};

pub(crate) use bindings::brenn::processor0_1_0::{
    alert, config, dom, log, mqtt, page_dom, provides, store, tools,
};
use bindings::brenn::processor0_2_0::{bus, calls as raw_calls, ports};
use bindings::brenn::processor0_3_0::blobs as raw_blobs;

/// A `blob` handle as the host lowered it.
pub(crate) type RawBlob = raw_blobs::Blob;

/// A `writer` handle as the host lowered it.
pub(crate) type RawWriter = raw_blobs::Writer;

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
        // A canonical-ABI list's length is a `u32`, so every index fits.
        let port: std::sync::Arc<str> = pw.port.as_str().into();
        let messages = pw
            .envelopes
            .into_iter()
            .zip(0u32..)
            .map(|(e, index)| Message::new(e.header, body(e.body), port.clone(), index))
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
    publish_refusal(&format!("publish to {port}"), Some(body), e)
}

/// A publish refusal as the SDK's `Error`: `call` names the call, and the
/// body is described when it is known. The one wording of the refusals
/// `ports.publish` and `blobs.publish` share.
fn publish_refusal(call: &str, body: Option<BodyRef<'_>>, e: ports::PublishError) -> Error {
    use ports::PublishError;
    let described = body.map_or_else(String::new, |b| format!(" ({})", b.describe()));
    match e {
        PublishError::NotPermitted => Error::ProcessingFailed(format!("{call}: not-permitted")),
        PublishError::InvalidPayload(m) => {
            Error::ProcessingFailed(format!("{call}: invalid-payload: {m}"))
        }
        PublishError::QuotaExceeded => Error::QuotaExceeded(format!("{call}: quota-exceeded")),
        PublishError::InvalidBody(m) => {
            Error::InvalidBody(format!("{call}: invalid-body{described}: {m}"))
        }
        PublishError::TextOnlyChannel => Error::TextOnlyChannel(format!(
            "{call}: text-only-channel: a non-text body{described} on a channel not declared \
             non_text; {NON_TEXT_FIX}"
        )),
    }
}

/// A [`PublishAttachmentsError`] as the SDK's `Error`. The five refusals it
/// shares with `ports.publish` read as that call's do.
pub(crate) fn publish_attachments_error(e: PublishAttachmentsError) -> Error {
    const CALL: &str = "publish with attachments";
    let shared = match e {
        PublishAttachmentsError::NotPermitted => ports::PublishError::NotPermitted,
        PublishAttachmentsError::InvalidPayload(m) => ports::PublishError::InvalidPayload(m),
        PublishAttachmentsError::InvalidBody(m) => ports::PublishError::InvalidBody(m),
        PublishAttachmentsError::TextOnlyChannel => ports::PublishError::TextOnlyChannel,
        PublishAttachmentsError::QuotaExceeded => ports::PublishError::QuotaExceeded,
        PublishAttachmentsError::InvalidAttachmentName(n) => {
            return Error::ProcessingFailed(format!("{CALL}: invalid-attachment-name: {n:?}"));
        }
        PublishAttachmentsError::DuplicateAttachmentName(n) => {
            return Error::ProcessingFailed(format!("{CALL}: duplicate-attachment-name: {n:?}"));
        }
        PublishAttachmentsError::TooManyAttachments => {
            return Error::ProcessingFailed(format!("{CALL}: too-many-attachments"));
        }
    };
    publish_refusal(CALL, None, shared)
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

pub(crate) fn blob_limits() -> BlobLimits {
    let raw = raw_blobs::limits();
    BlobLimits {
        max_read_bytes: raw.max_read_bytes,
        max_write_bytes: raw.max_write_bytes,
    }
}

pub(crate) fn blob_open(port: &str, index: u32, name: &str) -> Result<RawBlob, OpenError> {
    raw_blobs::open(port, index, name).map_err(|e| match e {
        raw_blobs::OpenError::NotPermitted => OpenError::NotPermitted,
        raw_blobs::OpenError::NotFound => OpenError::NotFound,
        raw_blobs::OpenError::QuotaExceeded => OpenError::QuotaExceeded,
    })
}

pub(crate) fn blob_read(blob: &RawBlob, offset: u64, len: u32) -> Result<Vec<u8>, ReadError> {
    blob.read(offset, len).map_err(|e| match e {
        raw_blobs::ReadError::OutOfRange => ReadError::OutOfRange,
        raw_blobs::ReadError::TooLarge => ReadError::TooLarge,
        raw_blobs::ReadError::StorageFailed(m) => ReadError::StorageFailed(m),
        raw_blobs::ReadError::QuotaExceeded => ReadError::QuotaExceeded,
        raw_blobs::ReadError::Unavailable => ReadError::Unavailable,
    })
}

pub(crate) fn blob_create(content_type: &str) -> Result<RawWriter, CreateError> {
    raw_blobs::create(content_type).map_err(|e| match e {
        raw_blobs::CreateError::InvalidContentType(m) => CreateError::InvalidContentType(m),
        raw_blobs::CreateError::BudgetExhausted => CreateError::BudgetExhausted,
        raw_blobs::CreateError::StorageFailed(m) => CreateError::StorageFailed(m),
        raw_blobs::CreateError::QuotaExceeded => CreateError::QuotaExceeded,
    })
}

pub(crate) fn blob_write(writer: &RawWriter, offset: u64, bytes: &[u8]) -> Result<(), WriteError> {
    writer.write(offset, bytes).map_err(|e| match e {
        raw_blobs::WriteError::TooLarge => WriteError::TooLarge,
        raw_blobs::WriteError::BlobTooLarge => WriteError::BlobTooLarge,
        raw_blobs::WriteError::BudgetExhausted => WriteError::BudgetExhausted,
        raw_blobs::WriteError::TooFragmented => WriteError::TooFragmented,
        raw_blobs::WriteError::StorageFailed(m) => WriteError::StorageFailed(m),
        raw_blobs::WriteError::QuotaExceeded => WriteError::QuotaExceeded,
    })
}

pub(crate) fn blob_seal(writer: &RawWriter) -> Result<RawBlob, SealError> {
    writer.seal().map_err(|e| match e {
        raw_blobs::SealError::Gap(g) => SealError::Gap(Gap {
            offset: g.offset,
            len: g.len,
        }),
        raw_blobs::SealError::QuotaExceeded => SealError::QuotaExceeded,
    })
}

/// `brenn_envelope::Urgency` to the `blobs` interface's own, exhaustively.
fn blobs_urgency(u: Urgency) -> raw_blobs::Urgency {
    match u {
        Urgency::VeryLow => raw_blobs::Urgency::VeryLow,
        Urgency::Low => raw_blobs::Urgency::Low,
        Urgency::Normal => raw_blobs::Urgency::Normal,
        Urgency::High => raw_blobs::Urgency::High,
    }
}

pub(crate) fn publish_with_attachments(
    port: &str,
    body: BodyRef<'_>,
    attachments: &[(&str, &RawBlob)],
    options: PublishOptions,
) -> Result<(), PublishAttachmentsError> {
    let options = raw_blobs::PublishOptions {
        urgency: options.urgency.map(blobs_urgency),
        deliver_after: options.deliver_after,
    };
    raw_blobs::publish(port, raw_body(body), attachments, options).map_err(|e| match e {
        raw_blobs::PublishError::Plain(plain) => match plain {
            ports::PublishError::NotPermitted => PublishAttachmentsError::NotPermitted,
            ports::PublishError::InvalidPayload(m) => PublishAttachmentsError::InvalidPayload(m),
            ports::PublishError::InvalidBody(m) => PublishAttachmentsError::InvalidBody(m),
            ports::PublishError::TextOnlyChannel => PublishAttachmentsError::TextOnlyChannel,
            ports::PublishError::QuotaExceeded => PublishAttachmentsError::QuotaExceeded,
        },
        raw_blobs::PublishError::InvalidAttachmentName(m) => {
            PublishAttachmentsError::InvalidAttachmentName(m)
        }
        raw_blobs::PublishError::DuplicateAttachmentName(m) => {
            PublishAttachmentsError::DuplicateAttachmentName(m)
        }
        raw_blobs::PublishError::TooManyAttachments => PublishAttachmentsError::TooManyAttachments,
    })
}

/// The `blobs` interface's `call-error` as the SDK's, exhaustively; the
/// plain form's `tool-error` it wraps is flattened into the same enum.
fn call_attachments_error(e: raw_blobs::CallError) -> CallAttachmentsError {
    use raw_blobs::CallError as Raw;
    use tools::ToolError as Plain;
    match e {
        Raw::Plain(Plain::NotGranted) => CallAttachmentsError::NotGranted,
        Raw::Plain(Plain::Denied(m)) => CallAttachmentsError::Denied(m),
        Raw::Plain(Plain::InvalidArgs(m)) => CallAttachmentsError::InvalidArgs(m),
        Raw::Plain(Plain::RateLimited) => CallAttachmentsError::RateLimited,
        Raw::Plain(Plain::WrongClass) => CallAttachmentsError::WrongClass,
        Raw::Plain(Plain::Internal(m)) => CallAttachmentsError::Internal(m),
        Raw::InvalidAttachmentName(m) => CallAttachmentsError::InvalidAttachmentName(m),
        Raw::DuplicateAttachmentName(m) => CallAttachmentsError::DuplicateAttachmentName(m),
        Raw::TooManyAttachments => CallAttachmentsError::TooManyAttachments,
        Raw::AttachmentsNotAccepted => CallAttachmentsError::AttachmentsNotAccepted,
    }
}

pub(crate) fn call_async_with_attachments(
    tool: &str,
    args_json: &str,
    call_id: &str,
    attachments: &[(&str, &RawBlob)],
) -> Result<(), CallAttachmentsError> {
    raw_blobs::call_async(tool, args_json, call_id, attachments).map_err(call_attachments_error)
}

pub(crate) fn call_async_for_with_attachments(
    serving: &str,
    tool: &str,
    args_json: &str,
    call_id: &str,
    attachments: &[(&str, &RawBlob)],
) -> Result<(), CallAttachmentsError> {
    raw_blobs::call_async_for(serving, tool, args_json, call_id, attachments)
        .map_err(call_attachments_error)
}

pub(crate) fn complete_with_attachments(
    id: &str,
    outcome_json: &str,
    attachments: &[(&str, &RawBlob)],
) -> Result<(), CompleteAttachmentsError> {
    use provides::CompleteError as Plain;
    use raw_blobs::CompleteError as Raw;
    raw_blobs::complete(id, outcome_json, attachments).map_err(|e| match e {
        Raw::NotPermitted => CompleteAttachmentsError::NotPermitted,
        Raw::Plain(Plain::NotPending) => CompleteAttachmentsError::NotPending,
        Raw::Plain(Plain::InvalidOutcome(m)) => CompleteAttachmentsError::InvalidOutcome(m),
        Raw::InvalidAttachmentName(m) => CompleteAttachmentsError::InvalidAttachmentName(m),
        Raw::DuplicateAttachmentName(m) => CompleteAttachmentsError::DuplicateAttachmentName(m),
        Raw::TooManyAttachments => CompleteAttachmentsError::TooManyAttachments,
    })
}

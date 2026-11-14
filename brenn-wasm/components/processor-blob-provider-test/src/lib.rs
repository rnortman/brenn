// Blob-carrying provided-tool fixture: serves `blob-echo`, which accepts
// attachments, through `blobs.complete` and `blobs.call-async-for`.
//
// Every new request on `tool-requests` is served by its `mode` argument. Bytes
// cross the value as lowercase hex.
//
//   "read"          — read `len` bytes at `offset` of the handed attachment
//                     `name` and complete with `{"range": <hex>, "size",
//                     "content_type"}`, carrying nothing. `offset` defaults
//                     to 0, `len` to the rest of the blob.
//   "make"          — make a blob of `content_type` (default
//                     `application/octet-stream`) holding `hex`, and complete
//                     with `{"made": <size>}` carrying it under `as` (default
//                     `made`). With `read` (an object of `name`, `offset`,
//                     `len`) the value also carries that range as `range`.
//   "return-handed" — complete with `{"returned": [<names>]}`, carrying every
//                     handed attachment under its own name.
//   "ok-then-err"   — as "make", then return `Err` from the activation.
//   "ok-then-trap"  — as "make", then panic.
//   "refusals"      — make one blob, try four completions the host refuses
//                     (33 attachments, a malformed name, a duplicate name, an
//                     `err` outcome with an attachment), and complete plainly
//                     with the refusal each drew: `{"too_many", "bad_name",
//                     "duplicate", "err_with_attachments"}`.
//   "nested"        — call `tool` with `args` (default `{}`) through
//                     `call_async_for` with attachments, handing every
//                     attachment it was handed; the
//                     result completes this request, below. A refused call is
//                     completed at once with an `internal` error naming it.
//   "hold"          — complete nothing: the request stays pending until its
//                     deadline.
//   "complete-late" — make a blob of `content_type` holding `hex`, as "make",
//                     and complete the request whose id
//                     is `target` with it, then complete this one with
//                     `{"late": "ok" | <refusal>}`.
//
// An unknown mode, or a required argument that is absent, mistyped or
// malformed, completes the request with an `invalid_args` error naming it.
//
// On `tool-results`, a result that names the request it serves completes it:
// its value and every attachment it carries, or an `internal` error with the
// tool's error. A result whose request is no longer pending is dropped.

use brenn_guest::bindings::brenn::processor0_1_0::provides as provides_raw;
use brenn_guest::bindings::brenn::processor0_3_0::blobs as raw;
use brenn_guest::blobs::{self, Blob};
use brenn_guest::provides::{
    self, CompleteAttachmentsError, OutcomeErrorKind, PendingId, ToolRequest, drop_not_pending,
};
use brenn_guest::serde_json::{Value, json};
use brenn_guest::tools::ToolResult;
use brenn_guest::{Activation, Body, Error, Processor, log};

struct ProcessorBlobProviderTest;

impl Processor for ProcessorBlobProviderTest {
    fn receive(activation: Activation) -> Result<Option<Body>, Error> {
        for request in activation.tool_requests()? {
            log::info(format_args!(
                "blob-provider-test: {} attachments, mode {:?}",
                request.attachments.len(),
                request.args.get("mode")
            ));
            serve(&request)?;
        }
        for result in activation.tool_results()? {
            answer(&result)?;
        }
        Ok(None)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// An argument a mode needs that is absent, mistyped or malformed. The
/// request is completed with an `invalid_args` error naming it.
struct BadArg(String);

fn unhex(text: &str) -> Result<Vec<u8>, BadArg> {
    if !text.len().is_multiple_of(2) {
        return Err(BadArg(format!("odd hex length {}", text.len())));
    }
    (0..text.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&text[i..i + 2], 16).map_err(|e| BadArg(format!("hex at {i}: {e}")))
        })
        .collect()
}

fn str_arg<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(Value::as_str)
}

fn need_str<'a>(args: &'a Value, key: &str) -> Result<&'a str, BadArg> {
    str_arg(args, key).ok_or_else(|| BadArg(format!("no string `{key}`")))
}

/// `key` when present, which must then be a string.
fn opt_str<'a>(args: &'a Value, key: &str) -> Result<Option<&'a str>, BadArg> {
    match args.get(key) {
        None => Ok(None),
        Some(v) => v
            .as_str()
            .map(Some)
            .ok_or_else(|| BadArg(format!("`{key}` is not a string"))),
    }
}

/// `key` when present, which must then be an unsigned integer.
fn opt_u64(args: &Value, key: &str) -> Result<Option<u64>, BadArg> {
    match args.get(key) {
        None => Ok(None),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| BadArg(format!("`{key}` is not an unsigned integer"))),
    }
}

/// A byte range of the handed attachment `name`; `offset` defaults to the
/// start and `len` to the rest of the blob.
struct Range<'a> {
    name: &'a str,
    offset: u64,
    len: Option<u64>,
}

fn range_spec(spec: &Value) -> Result<Range<'_>, BadArg> {
    Ok(Range {
        name: need_str(spec, "name")?,
        offset: opt_u64(spec, "offset")?.unwrap_or(0),
        len: opt_u64(spec, "len")?,
    })
}

/// A blob to make: `hex` is required, `content_type` defaults to
/// `application/octet-stream`.
struct Made<'a> {
    content_type: &'a str,
    bytes: Vec<u8>,
}

fn made_spec(args: &Value) -> Result<Made<'_>, BadArg> {
    Ok(Made {
        content_type: opt_str(args, "content_type")?.unwrap_or("application/octet-stream"),
        bytes: unhex(need_str(args, "hex")?)?,
    })
}

/// Mode "make": the blob, the name it is carried under (default `made`) and
/// an optional range of a handed attachment to report as well.
struct MadeAnswer<'a> {
    made: Made<'a>,
    name: &'a str,
    read: Option<Range<'a>>,
}

fn made_answer(args: &Value) -> Result<MadeAnswer<'_>, BadArg> {
    Ok(MadeAnswer {
        made: made_spec(args)?,
        name: opt_str(args, "as")?.unwrap_or("made"),
        read: args.get("read").map(range_spec).transpose()?,
    })
}

/// A request's mode with the arguments it needs, all checked.
enum Mode<'a> {
    Read(Range<'a>),
    Make(MadeAnswer<'a>),
    ReturnHanded,
    OkThenErr(MadeAnswer<'a>),
    OkThenTrap(MadeAnswer<'a>),
    Refusals,
    /// `args` absent calls the tool with no arguments.
    Nested {
        tool: &'a str,
        args: Value,
    },
    Hold,
    CompleteLate {
        target: &'a str,
        made: Made<'a>,
    },
}

fn mode(args: &Value) -> Result<Mode<'_>, BadArg> {
    Ok(match need_str(args, "mode")? {
        "read" => Mode::Read(range_spec(args)?),
        "make" => Mode::Make(made_answer(args)?),
        "return-handed" => Mode::ReturnHanded,
        "ok-then-err" => Mode::OkThenErr(made_answer(args)?),
        "ok-then-trap" => Mode::OkThenTrap(made_answer(args)?),
        "refusals" => Mode::Refusals,
        "nested" => Mode::Nested {
            tool: need_str(args, "tool")?,
            args: args.get("args").cloned().unwrap_or(json!({})),
        },
        "hold" => Mode::Hold,
        "complete-late" => Mode::CompleteLate {
            target: need_str(args, "target")?,
            made: made_spec(args)?,
        },
        other => return Err(BadArg(format!("unknown mode {other:?}"))),
    })
}

fn make(made: &Made) -> Result<Blob, Error> {
    let writer = blobs::create(made.content_type)?;
    writer.write_all(&made.bytes)?;
    Ok(writer.seal()?)
}

/// `{"range", "size", "content_type"}` of a handed attachment.
fn read_range(request: &ToolRequest, range: &Range) -> Result<Value, Error> {
    let blob = request.open(range.name)?;
    let len = match range.len {
        Some(len) => len,
        None => blob.size().checked_sub(range.offset).ok_or_else(|| {
            Error::failed(format!("offset {} past size {}", range.offset, blob.size()))
        })?,
    };
    let len = u32::try_from(len).map_err(|_| Error::failed("read length over u32"))?;
    Ok(json!({
        "range": hex(&blob.read(range.offset, len)?),
        "size": blob.size(),
        "content_type": blob.content_type(),
    }))
}

/// Complete the request with a blob it makes, as mode "make".
fn complete_made(request: &ToolRequest, answer: &MadeAnswer) -> Result<(), Error> {
    let blob = make(&answer.made)?;
    let mut value = json!({ "made": blob.size() });
    if let Some(range) = &answer.read {
        value["range"] = read_range(request, range)?["range"].clone();
    }
    Ok(provides::complete_with_attachments(
        &request.id,
        &value,
        &[(answer.name, &blob)],
    )?)
}

fn complete_word(refused: Result<(), CompleteAttachmentsError>) -> String {
    match refused {
        Ok(()) => "ok".to_string(),
        Err(CompleteAttachmentsError::NotPermitted) => "not-permitted".to_string(),
        Err(CompleteAttachmentsError::NotPending) => "not-pending".to_string(),
        Err(CompleteAttachmentsError::InvalidOutcome(_)) => "invalid-outcome".to_string(),
        Err(CompleteAttachmentsError::InvalidAttachmentName(_)) => {
            "invalid-attachment-name".to_string()
        }
        Err(CompleteAttachmentsError::DuplicateAttachmentName(_)) => {
            "duplicate-attachment-name".to_string()
        }
        Err(CompleteAttachmentsError::TooManyAttachments) => "too-many-attachments".to_string(),
    }
}

/// The word for an `err` completion given attachments, through the raw
/// binding: the SDK's form answers only with a value.
fn err_with_attachments(id: &PendingId) -> Result<String, Error> {
    let writer = raw::create("application/octet-stream")
        .map_err(|e| Error::failed(format!("raw create: {e:?}")))?;
    writer
        .write(0, b"x")
        .map_err(|e| Error::failed(format!("raw write: {e:?}")))?;
    let blob = writer
        .seal()
        .map_err(|e| Error::failed(format!("raw seal: {e:?}")))?;
    let outcome = json!({ "err": { "kind": "internal", "detail": "refused" } }).to_string();
    Ok(
        match raw::complete(id.as_str(), &outcome, &[("x", &blob)]) {
            Ok(()) => "ok".to_string(),
            Err(raw::CompleteError::Plain(provides_raw::CompleteError::InvalidOutcome(_))) => {
                "invalid-outcome".to_string()
            }
            Err(other) => format!("unexpected: {other:?}"),
        },
    )
}

fn refusals(request: &ToolRequest) -> Result<(), Error> {
    let id = &request.id;
    let blob = make(&Made {
        content_type: "application/octet-stream",
        bytes: vec![0],
    })?;
    let names: Vec<String> = (0..33).map(|i| format!("a{i}")).collect();
    let many: Vec<(&str, &Blob)> = names.iter().map(|n| (n.as_str(), &blob)).collect();
    let too_many = complete_word(provides::complete_with_attachments(id, &json!({}), &many));
    let bad_name = complete_word(provides::complete_with_attachments(
        id,
        &json!({}),
        &[(".bad", &blob)],
    ));
    let duplicate = complete_word(provides::complete_with_attachments(
        id,
        &json!({}),
        &[("a", &blob), ("a", &blob)],
    ));
    let err_with_attachments = err_with_attachments(id)?;
    provides::complete(
        id,
        &json!({
            "too_many": too_many,
            "bad_name": bad_name,
            "duplicate": duplicate,
            "err_with_attachments": err_with_attachments,
        }),
    )
}

/// Every handed attachment, opened, under its own name.
fn handed(request: &ToolRequest) -> Result<Vec<(String, Blob)>, Error> {
    request
        .attachments
        .iter()
        .map(|a| {
            let name = a.name().as_str().to_string();
            let blob = request.open(&name)?;
            Ok((name, blob))
        })
        .collect()
}

fn as_pairs(blobs: &[(String, Blob)]) -> Vec<(&str, &Blob)> {
    blobs.iter().map(|(n, b)| (n.as_str(), b)).collect()
}

fn serve(request: &ToolRequest) -> Result<(), Error> {
    let mode = match mode(&request.args) {
        Ok(mode) => mode,
        Err(BadArg(detail)) => {
            return provides::complete_err(&request.id, OutcomeErrorKind::InvalidArgs, &detail);
        }
    };
    match mode {
        Mode::Read(range) => {
            let value = read_range(request, &range)?;
            provides::complete(&request.id, &value)
        }
        Mode::Make(answer) => complete_made(request, &answer),
        Mode::ReturnHanded => {
            let blobs = handed(request)?;
            let names: Vec<&str> = blobs.iter().map(|(n, _)| n.as_str()).collect();
            Ok(provides::complete_with_attachments(
                &request.id,
                &json!({ "returned": names }),
                &as_pairs(&blobs),
            )?)
        }
        Mode::OkThenErr(answer) => {
            complete_made(request, &answer)?;
            Err(Error::failed("ok-then-err mode"))
        }
        Mode::OkThenTrap(answer) => {
            complete_made(request, &answer)?;
            panic!("ok-then-trap mode");
        }
        Mode::Refusals => refusals(request),
        Mode::Nested { tool, args } => {
            let blobs = handed(request)?;
            match provides::call_async_for_with_attachments(
                &request.id,
                tool,
                &args,
                request.id.as_str(),
                &as_pairs(&blobs),
            ) {
                Ok(()) => Ok(()),
                Err(refused) => drop_not_pending(provides::complete_err(
                    &request.id,
                    OutcomeErrorKind::Internal,
                    &format!("{refused:?}"),
                )),
            }
        }
        Mode::Hold => Ok(()),
        Mode::CompleteLate { target, made } => {
            let blob = make(&made)?;
            let late = complete_word(provides::complete_with_attachments(
                &PendingId::from(target.to_string()),
                &json!({ "late": true }),
                &[("made", &blob)],
            ));
            provides::complete(&request.id, &json!({ "late": late }))
        }
    }
}

fn answer(result: &ToolResult) -> Result<(), Error> {
    let Some(id) = &result.serving else {
        return Ok(());
    };
    match &result.outcome {
        Ok(value) => {
            let blobs: Vec<(String, Blob)> = result
                .attachments
                .iter()
                .map(|a| {
                    let name = a.name().as_str().to_string();
                    let blob = result.open(&name)?;
                    Ok((name, blob))
                })
                .collect::<Result<_, Error>>()?;
            match provides::complete_with_attachments(id, value, &as_pairs(&blobs)) {
                Err(CompleteAttachmentsError::NotPending) => Ok(()),
                other => Ok(other?),
            }
        }
        Err(err) => drop_not_pending(provides::complete_err(
            id,
            OutcomeErrorKind::Internal,
            &format!("{}: {}", err.kind, err.detail),
        )),
    }
}

brenn_guest::export_processor!(ProcessorBlobProviderTest);

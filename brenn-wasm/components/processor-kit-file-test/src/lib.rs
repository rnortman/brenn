//! Provider-kit file-transfer fixture (brenn:processor world), served by
//! `brenn-provider-kit-guest`'s `serve_with_blobs`, so its runs go through the
//! guest adapter's real `blobs` calls.
//!
//! - `kit-file-put` — `PUT <path>` with the request's attachment `file` as
//!   the body, forwarded by name; with `head: true`, a `HEAD <path>` first, so
//!   the `PUT` is issued by the activation that receives the `HEAD`'s answer
//!   and its body is opened from the request's window context. Completes with
//!   `{"status", "bytes"}`, the status of the `PUT` and the size of the file
//!   sent.
//! - `kit-file-get` — `GET <path>` asking for the body as an attachment, and
//!   completes with `{"status", "bytes", "attachment": "file"}`, the answer's
//!   blob forwarded as the completion's attachment `file`.
//! - `kit-body-put` — `PUT <path>` with a body the component builds when the
//!   call is issued (`Body::Built`): `text` repeated `times` times, then the
//!   request's attachment `file`, read in pieces, when it carries one —
//!   written as two lowercase hexadecimal digits a byte when `hex` is true.
//!   Completes with `{"status", "bytes"}`, the status of the `PUT` and the
//!   body's length, computed from the arguments and the attachment's header.
//!
//! The instance's `config` names `provider` and `host`; every call names the
//! caller's user and no account.

mod spec;

use brenn_guest::{Activation, Body as GuestBody, Error, Processor};
use brenn_provider_kit::{
    ApiCall, Attached, Blob, BlobAnswer, Body, Call, Failure, InFlight, Logic, NestedError, Next,
    PayloadAnswer, Queued, Step, Tool, capped, host, integer, internal, no_such_step, object,
    read_api_blob, read_api_payload, required, stored,
};
use serde_json::{Value, json};

use crate::spec::{Provided, config};

/// The request attachment `kit-file-put` sends, and the completion
/// attachment `kit-file-get` answers.
const FILE: &str = "file";
/// The API name the kit's refusals name.
const API: &str = "the stub";
/// Every request's first call: the `PUT`, the `HEAD` or the `GET`.
const FIRST_STEP: u8 = 0;
/// `kit-file-put`'s `PUT` after its `HEAD`.
const PUT_AFTER_HEAD_STEP: u8 = 1;
/// The content type of the body `kit-body-put` builds.
const BUILT_TYPE: &str = "application/octet-stream";
/// The longest `text` `kit-body-put` takes, in characters.
const TEXT_MAX_CHARS: usize = 393_216;
/// The most times `kit-body-put` repeats its `text`.
const TIMES_MAX: u64 = 4096;
/// The digits `kit-body-put` writes its attachment in when `hex` is true.
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

impl Tool for Provided {
    fn name(self) -> &'static str {
        Provided::name(self)
    }

    fn from_name(name: &str) -> Option<Provided> {
        Provided::from_name(name)
    }
}

/// The instance's `config`.
struct Settings {
    provider: String,
    host: String,
}

impl Settings {
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Settings, String> {
        Ok(Settings {
            provider: required(&get, "provider")?,
            host: host(&get, "host")?,
        })
    }
}

/// A request's arguments: the path, and whether `kit-file-put` sends a `HEAD`
/// first.
struct Args {
    path: String,
    head: bool,
}

impl Args {
    fn parse(args: &Value) -> Result<Args, String> {
        let map = object(args, &["path", "head"])?;
        let path = path(map)?;
        let head = brenn_provider_kit::flag(map, "head")?;
        Ok(Args { path, head })
    }
}

/// The arguments' `path`: a string starting with `/`.
fn path(map: &serde_json::Map<String, Value>) -> Result<String, String> {
    match map.get("path") {
        Some(Value::String(path)) if path.starts_with('/') => Ok(path.clone()),
        _ => Err("`path` must be a string starting with `/`".to_string()),
    }
}

/// `kit-body-put`'s arguments: the path, the text its body repeats, and
/// whether the attachment is written in hexadecimal.
struct BodyArgs {
    path: String,
    text: String,
    times: u64,
    hex: bool,
}

impl BodyArgs {
    fn parse(args: &Value) -> Result<BodyArgs, String> {
        let map = object(args, &["path", "text", "times", "hex"])?;
        let path = path(map)?;
        let text = capped(map, "text", TEXT_MAX_CHARS)?
            .ok_or("`text` is required")?
            .to_string();
        let times = integer(map, "times", 1, TIMES_MAX)?.ok_or("`times` is required")?;
        let hex = brenn_provider_kit::flag(map, "hex")?;
        Ok(BodyArgs {
            path,
            text,
            times,
            hex,
        })
    }

    /// The length of the body built from these arguments and the request's
    /// `attachments`.
    fn body_len(&self, attachments: &[Attached]) -> u64 {
        let file = attachments
            .iter()
            .find(|attached| attached.name == FILE)
            .map_or(0, |attached| attached.size);
        let file = if self.hex { file * 2 } else { file };
        self.text.len() as u64 * self.times + file
    }
}

fn api_call(settings: &Settings, method: &str, path: &str, body: Option<Body>) -> ApiCall {
    ApiCall {
        provider: settings.provider.clone(),
        account: None,
        method: method.to_string(),
        host: settings.host.clone(),
        path: path.to_string(),
        query: Default::default(),
        headers: Default::default(),
        body,
        response_attachment: None,
    }
}

fn put(settings: &Settings, path: &str, step: u8) -> Call {
    Call {
        step,
        api: api_call(
            settings,
            "PUT",
            path,
            Some(Body::Attachment(FILE.to_string())),
        ),
    }
}

struct KitFiles;

impl Logic for KitFiles {
    type Tool = Provided;
    type Settings = Settings;

    fn plan(
        tool: Provided,
        args: &Value,
        step: &Step<'_>,
        settings: &Settings,
    ) -> Result<Call, String> {
        match tool {
            Provided::KitFilePut => {
                let args = Args::parse(args)?;
                if !step
                    .request_attachments()
                    .iter()
                    .any(|attached| attached.name == FILE)
                {
                    return Err(format!("attach the file to send as `{FILE}`"));
                }
                if args.head {
                    Ok(Call {
                        step: FIRST_STEP,
                        api: api_call(settings, "HEAD", &args.path, None),
                    })
                } else {
                    Ok(put(settings, &args.path, FIRST_STEP))
                }
            }
            Provided::KitBodyPut => {
                let args = BodyArgs::parse(args)?;
                Ok(Call {
                    step: FIRST_STEP,
                    api: api_call(settings, "PUT", &args.path, Some(Body::Built)),
                })
            }
            Provided::KitFileGet => {
                let args = Args::parse(args)?;
                if args.head {
                    return Err("`head` is an argument of KitFilePut".to_string());
                }
                Ok(Call {
                    step: FIRST_STEP,
                    api: api_call(settings, "GET", &args.path, None).answer_as_attachment(),
                })
            }
        }
    }

    fn advance(
        state: &InFlight<Provided>,
        outcome: Result<Value, NestedError>,
        step: &mut Step<'_>,
        settings: &Settings,
    ) -> Next {
        let next = match state.tool {
            Provided::KitFilePut => put_answered(state, outcome, settings),
            Provided::KitFileGet if state.step == FIRST_STEP => got(state, outcome, step),
            Provided::KitFileGet => Err(no_such_step(state.tool, state.step)),
            Provided::KitBodyPut if state.step == FIRST_STEP => body_put(state, outcome),
            Provided::KitBodyPut => Err(no_such_step(state.tool, state.step)),
        };
        next.unwrap_or_else(Next::Fail)
    }

    fn body(
        call: &Queued<Provided>,
        step: &Step<'_>,
        _settings: &Settings,
    ) -> Result<Blob, Failure> {
        if !matches!(call.tool, Provided::KitBodyPut) || call.call.step != FIRST_STEP {
            return Err(no_such_step(call.tool, call.call.step));
        }
        let args = call.stored(BodyArgs::parse)?;
        let mut body = step.append(BUILT_TYPE)?;
        for _ in 0..args.times {
            body.write(args.text.as_bytes())?;
        }
        if call
            .attachments
            .iter()
            .any(|attached| attached.name == FILE)
        {
            let file = step.open_request(FILE)?;
            let mut hex = Vec::new();
            for piece in step.pieces(&file) {
                let piece = piece?;
                if args.hex {
                    hex.clear();
                    hex.extend(piece.iter().flat_map(|byte| {
                        [
                            HEX_DIGITS[usize::from(byte >> 4)],
                            HEX_DIGITS[usize::from(byte & 0xf)],
                        ]
                    }));
                    body.write(&hex)?;
                } else {
                    body.write(&piece)?;
                }
            }
        }
        body.finish()
    }
}

/// `kit-body-put`'s `PUT` answered: the request completes.
fn body_put(
    state: &InFlight<Provided>,
    outcome: Result<Value, NestedError>,
) -> Result<Next, Failure> {
    let args = stored(state, BodyArgs::parse)?;
    let status = match read_api_payload(outcome, &state.target, API)? {
        PayloadAnswer::Payload(payload) => payload.status,
        PayloadAnswer::Relay(value) => return Ok(Next::Complete(value)),
        PayloadAnswer::Unreadable(why) => return Err(why.failure(API, None)),
        PayloadAnswer::Error { status, .. } => {
            return Err(internal(format!("{API} answered HTTP {status}")));
        }
    };
    Ok(Next::Complete(json!({
        "status": status,
        "bytes": args.body_len(&state.attachments),
    })))
}

/// `kit-file-put`'s `HEAD` answered: the `PUT` follows. Its `PUT` answered:
/// the request completes.
fn put_answered(
    state: &InFlight<Provided>,
    outcome: Result<Value, NestedError>,
    settings: &Settings,
) -> Result<Next, Failure> {
    let args = stored(state, Args::parse)?;
    let status = match read_api_payload(outcome, &state.target, API)? {
        PayloadAnswer::Payload(payload) => payload.status,
        PayloadAnswer::Relay(value) => return Ok(Next::Complete(value)),
        PayloadAnswer::Unreadable(why) => return Err(why.failure(API, None)),
        PayloadAnswer::Error { status, .. } => {
            return Err(internal(format!("{API} answered HTTP {status}")));
        }
    };
    match (args.head, state.step) {
        (true, FIRST_STEP) => {
            return Ok(Next::Call {
                call: put(settings, &args.path, PUT_AFTER_HEAD_STEP),
                derived: Default::default(),
            });
        }
        (false, FIRST_STEP) | (true, PUT_AFTER_HEAD_STEP) => {}
        _ => return Err(no_such_step(state.tool, state.step)),
    }
    let bytes = state
        .attachments
        .iter()
        .find(|attached| attached.name == FILE)
        .map(|attached| attached.size)
        .ok_or_else(|| internal(format!("in-flight row carries no `{FILE}`")))?;
    Ok(Next::Complete(json!({ "status": status, "bytes": bytes })))
}

/// `kit-file-get`'s `GET` answered: the answer's blob is the completion's
/// attachment.
fn got(
    state: &InFlight<Provided>,
    outcome: Result<Value, NestedError>,
    step: &mut Step<'_>,
) -> Result<Next, Failure> {
    let (status, bytes, attachment) = match read_api_blob(outcome, &state.target, API)? {
        BlobAnswer::Blob {
            status,
            bytes,
            attachment,
            ..
        } => (status, bytes, attachment),
        BlobAnswer::Relay(value) => return Ok(Next::Complete(value)),
        BlobAnswer::Unreadable(why) => return Err(why.failure(API, None)),
        BlobAnswer::Error { status, .. } => {
            return Err(internal(format!("{API} answered HTTP {status}")));
        }
        BlobAnswer::TooLarge { limit, .. } => {
            return Err(internal(format!(
                "the answer exceeds the host's largest blob ({limit} bytes)"
            )));
        }
        BlobAnswer::StoreFull => return Err(internal("the host's blob store is full")),
    };
    let blob = step.open_answer(&attachment)?;
    step.attach(FILE, blob);
    Ok(Next::Complete(json!({
        "status": status,
        "bytes": bytes,
        "attachment": FILE,
    })))
}

struct ProcessorKitFileTest;

impl Processor for ProcessorKitFileTest {
    fn receive(activation: Activation) -> Result<Option<GuestBody>, Error> {
        let settings = Settings::from_lookup(config::get).map_err(Error::failed)?;
        brenn_provider_kit_guest::serve_with_blobs::<KitFiles>(&activation, &settings)?;
        Ok(None)
    }
}

brenn_guest::export_processor!(ProcessorKitFileTest);

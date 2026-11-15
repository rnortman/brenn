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
//!
//! The instance's `config` names `provider` and `host`; every call names the
//! caller's user and no account.

mod spec;

use brenn_guest::{Activation, Body as GuestBody, Error, Processor};
use brenn_provider_kit::{
    ApiCall, BlobAnswer, Body, Call, Failure, InFlight, Logic, NestedError, Next, PayloadAnswer,
    Step, Tool, host, internal, no_such_step, object, read_api_blob, read_api_payload, required,
    stored,
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
        let path = match map.get("path") {
            Some(Value::String(path)) if path.starts_with('/') => path.clone(),
            _ => return Err("`path` must be a string starting with `/`".to_string()),
        };
        let head = brenn_provider_kit::flag(map, "head")?;
        Ok(Args { path, head })
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
        let args = Args::parse(args)?;
        match tool {
            Provided::KitFilePut => {
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
            Provided::KitFileGet => {
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
        };
        next.unwrap_or_else(Next::Fail)
    }
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

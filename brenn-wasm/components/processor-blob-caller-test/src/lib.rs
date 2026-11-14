// Blob-carrying tool caller fixture. Every tool call it makes goes through
// `blobs.call-async` and every publish through `blobs.publish`: it calls no
// function of `tools` or `ports`, and imports both only for the error types
// those two functions reach.
//
// Each new message on `in` is a JSON trigger:
//
//   {"tool": <name>, "args": <object>, "call_id": <id>,
//    "attach": {<name the tool sees>: {"from": <attachment of this message>}
//                                   | {"make": {"content_type", "hex"}}},
//    "trap": <bool>}
//
// `tool` and `call_id` are required, as is `hex` in a `make`; `args` defaults
// to `{}`, `attach` to none, `content_type` to `application/octet-stream` and
// `trap` to false. A trigger with a required field absent, or any field
// mistyped or malformed, makes no call and publishes `{"bad_trigger": <what>}`
// on `out`.
//
// It opens or makes each blob, calls the tool carrying them, and publishes
// nothing for a call the host queued. A refused call publishes
// `{"call_id", "refused": <word>}` on `out`. With `"trap": true` it panics
// after queueing the call, so nothing is flushed.
//
// Each result on `tool-results` is read back and reported on `out`:
//
//   {"call_id", "tool", "ok": <value> | "err": {"kind", "detail"},
//    "attachments": [{"name", "size", "content_type", "hex"}]}
//
// Bytes cross the JSON as lowercase hex. Reports carry no blob.

use brenn_guest::blobs::{self, Blob};
use brenn_guest::serde_json::{Value, json};
use brenn_guest::tools::{self, CallAttachmentsError, ToolResult};
use brenn_guest::{Activation, Body, Error, Message, Processor, PublishOptions, log};

struct ProcessorBlobCallerTest;

impl Processor for ProcessorBlobCallerTest {
    fn receive(activation: Activation) -> Result<Option<Body>, Error> {
        let triggers: Vec<&Message> = activation
            .port_windows()
            .filter(|w| w.port() == "in")
            .flat_map(|w| w.new_messages())
            .collect();
        for trigger in triggers {
            call(trigger)?;
        }
        for result in activation.tool_results()? {
            report(&result)?;
        }
        Ok(None)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A trigger field that is absent, mistyped or malformed.
struct BadTrigger(String);

fn unhex(text: &str) -> Result<Vec<u8>, BadTrigger> {
    if !text.len().is_multiple_of(2) {
        return Err(BadTrigger(format!("odd hex length {}", text.len())));
    }
    (0..text.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&text[i..i + 2], 16)
                .map_err(|e| BadTrigger(format!("hex at {i}: {e}")))
        })
        .collect()
}

fn need_str<'a>(spec: &'a Value, key: &str) -> Result<&'a str, BadTrigger> {
    spec.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| BadTrigger(format!("no string `{key}`")))
}

/// Publish `value` on `out` through `blobs.publish`, carrying nothing.
fn publish_out(value: &Value) -> Result<(), Error> {
    Ok(brenn_guest::publish_with_attachments(
        "out",
        value.to_string().as_str(),
        &[],
        PublishOptions::default(),
    )?)
}

/// Where an attachment of the call comes from.
enum Source<'a> {
    /// The trigger message's attachment of this name.
    From(&'a str),
    /// A blob made of these bytes.
    Make {
        content_type: &'a str,
        bytes: Vec<u8>,
    },
}

/// A trigger with every field checked.
struct Trigger<'a> {
    tool: &'a str,
    call_id: &'a str,
    args: Value,
    attach: Vec<(&'a str, Source<'a>)>,
    trap: bool,
}

fn source<'a>(name: &str, spec: &'a Value) -> Result<Source<'a>, BadTrigger> {
    if let Some(from) = spec.get("from") {
        let from = from
            .as_str()
            .ok_or_else(|| BadTrigger(format!("attach `{name}`: `from` is not a string")))?;
        return Ok(Source::From(from));
    }
    let made = spec
        .get("make")
        .ok_or_else(|| BadTrigger(format!("attach `{name}` names neither `from` nor `make`")))?;
    let content_type = match made.get("content_type") {
        None => "application/octet-stream",
        Some(v) => v.as_str().ok_or_else(|| {
            BadTrigger(format!("attach `{name}`: `content_type` is not a string"))
        })?,
    };
    let bytes = need_str(made, "hex")
        .and_then(unhex)
        .map_err(|BadTrigger(e)| BadTrigger(format!("attach `{name}`: {e}")))?;
    Ok(Source::Make {
        content_type,
        bytes,
    })
}

fn parse(spec: &Value) -> Result<Trigger<'_>, BadTrigger> {
    let mut attach = Vec::new();
    match spec.get("attach") {
        None => {}
        Some(Value::Object(entries)) => {
            for (name, source_spec) in entries {
                attach.push((name.as_str(), source(name, source_spec)?));
            }
        }
        Some(_) => return Err(BadTrigger("`attach` is not an object".to_string())),
    }
    let trap = match spec.get("trap") {
        None => false,
        Some(v) => v
            .as_bool()
            .ok_or_else(|| BadTrigger("`trap` is not a bool".to_string()))?,
    };
    Ok(Trigger {
        tool: need_str(spec, "tool")?,
        call_id: need_str(spec, "call_id")?,
        args: spec.get("args").cloned().unwrap_or(json!({})),
        attach,
        trap,
    })
}

/// The blob a checked `attach` entry names.
fn open_source(trigger: &Message, source: &Source) -> Result<Blob, Error> {
    match source {
        Source::From(name) => Ok(blobs::open(trigger, name)?),
        Source::Make {
            content_type,
            bytes,
        } => {
            let writer = blobs::create(content_type)?;
            writer.write_all(bytes)?;
            Ok(writer.seal()?)
        }
    }
}

fn refusal_word(e: &CallAttachmentsError) -> &'static str {
    match e {
        CallAttachmentsError::NotGranted => "not-granted",
        CallAttachmentsError::Denied(_) => "denied",
        CallAttachmentsError::InvalidArgs(_) => "invalid-args",
        CallAttachmentsError::RateLimited => "rate-limited",
        CallAttachmentsError::WrongClass => "wrong-class",
        CallAttachmentsError::Internal(_) => "internal",
        CallAttachmentsError::InvalidAttachmentName(_) => "invalid-attachment-name",
        CallAttachmentsError::DuplicateAttachmentName(_) => "duplicate-attachment-name",
        CallAttachmentsError::TooManyAttachments => "too-many-attachments",
        CallAttachmentsError::AttachmentsNotAccepted => "attachments-not-accepted",
    }
}

fn call(trigger: &Message) -> Result<(), Error> {
    let spec: Value = trigger.body().json()?;
    let parsed = match parse(&spec) {
        Ok(parsed) => parsed,
        Err(BadTrigger(what)) => return publish_out(&json!({ "bad_trigger": what })),
    };
    let mut blobs: Vec<(&str, Blob)> = Vec::new();
    for (name, source) in &parsed.attach {
        blobs.push((name, open_source(trigger, source)?));
    }
    let pairs: Vec<(&str, &Blob)> = blobs.iter().map(|(n, b)| (*n, b)).collect();
    let (tool, call_id) = (parsed.tool, parsed.call_id);
    log::info(format_args!(
        "blob-caller-test: {tool} with {} attachments",
        pairs.len()
    ));
    if let Err(refused) =
        tools::call_async_with_attachments_json(tool, &parsed.args, call_id, &pairs)
    {
        return publish_out(&json!({
            "call_id": call_id,
            "refused": refusal_word(&refused),
        }));
    }
    if parsed.trap {
        panic!("trap after queueing {call_id}");
    }
    Ok(())
}

fn report(result: &ToolResult) -> Result<(), Error> {
    let mut attachments = Vec::new();
    for a in &result.attachments {
        let blob = result.open(a.name().as_str())?;
        attachments.push(json!({
            "name": a.name().as_str(),
            "size": blob.size(),
            "content_type": blob.content_type(),
            "hex": hex(&blob.read_all()?),
        }));
    }
    let mut value = json!({
        "call_id": result.call_id,
        "tool": result.tool,
        "attachments": attachments,
    });
    match &result.outcome {
        Ok(ok) => value["ok"] = ok.clone(),
        Err(err) => value["err"] = json!({ "kind": err.kind, "detail": err.detail }),
    }
    publish_out(&value)
}

brenn_guest::export_processor!(ProcessorBlobCallerTest);

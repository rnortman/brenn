// The guest SDK's binary-state fixture: a component built on `brenn-guest`
// whose retained state is bytes, kept through `RetainedState` and a
// hand-written `StateCodec`, and the worked example of that facility.
//
// The state is a step counter and the bytes after it. `WorkloadCodec` encodes
// it as the step, little-endian, followed by the bytes, and publishes that as
// `application/octet-stream` — bit for bit the body the raw fixture
// `processor-page-bytes-test` holds for its `state` workload, so one oracle
// checks both. Its `decode` refuses a body under four bytes. A state of this
// codec needs its port's channel (or, on a page, its binding) declared
// `non_text`.
//
// `receive` reads its command lines off `in`, then is one
// `RetainedState::around_on` on `state`: under `StateLoss::Recover` when a line
// is `state-recover`, `StateLoss::FailActivation` otherwise.
//
// `around_on` stores what its body leaves, and on an activation that found no
// state that is stored unconditionally — `Workload::default()` included, when
// no line created a state. This state's default is not a state: stored, it
// would decode as a four-byte one and the next `state` would refuse it. So
// `around_on` runs only on an activation with command lines and either a stored
// state on `state` or a `state`/`state-recover` line to create one. Any other
// activation — a page's mount, a `feed` delivery, a `noop` before any state —
// leaves the state port alone. Commands are untyped text lines read from the
// bodies of the new messages on `in`:
//
//   noop                    change nothing; an unchanged state is not stored
//   state <raw-len>         with no state yet, create `raw-len` bytes, byte i
//                           being (i * 31 + 7) mod 256 — every byte value, so
//                           not UTF-8 — the first four of them the step; with
//                           one, require it to be `raw-len` bytes and add one
//                           to the step. Result
//                           `state:<len>:<FNV-1a 64 of the encoded body, hex>`.
//   state-recover <raw-len> the same, the activation run under `Recover`
//   fail                    end the activation with an error
//
// A line that is none of these ends the activation with an error naming it.
//
// Every activation that returns ok publishes one JSON report on `report`, as
// untyped text, in the raw fixtures' report shape:
//
//   windows   the `feed` window, when the instance has one: its port, new_from,
//             dropped, and each message as the SDK reads it — its header
//             (parsed), content type, bytes in hex, three verdicts, each the
//             value or the word `malformed`: `text_body` (through
//             `Message::envelope`), `as_text` and `json` (on `Message::body`),
//             and the four ingress accessors `mqtt_transport`,
//             `webhook_transport`, `mqtt_body` and `webhook_body`, each
//             `{"ok": <value>}` or `{"err": "<error kind>"}`
//   streams   always empty
//   deferred  always empty
//   results   one word per command run
//   sync      the activation's sync port, or null

mod spec;

use brenn_guest::serde_json::{self, Value, json};
use brenn_guest::{
    Activation, Body, Error, Message, MessageEnvelope, MessageEnvelopeExt, Processor,
    RetainedState, StateCodec, StateLoss, publish,
};

use crate::spec::port::{FEED, IN, REPORT, STATE};

/// The content type the state is published under.
const STATE_CONTENT_TYPE: &str = "application/octet-stream";
/// The width of the step counter at the head of the encoded state.
const STEP_BYTES: usize = 4;
/// What a verdict reads when the SDK refused the body.
const MALFORMED: &str = "malformed";

/// The binary state: a step counter and the bytes after it.
#[derive(Default)]
struct Workload {
    /// Whether this value holds a state — decoded from a stored body, or
    /// created by `state` — rather than the default no state has replaced. Not
    /// encoded: every body that decodes is held, which is why the default is
    /// never stored.
    held: bool,
    step: u32,
    rest: Vec<u8>,
}

/// The step, little-endian, then the bytes; `application/octet-stream`.
struct WorkloadCodec;

impl StateCodec<Workload> for WorkloadCodec {
    const CONTENT_TYPE: Option<&'static str> = Some(STATE_CONTENT_TYPE);

    fn encode(value: &Workload) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::with_capacity(STEP_BYTES + value.rest.len());
        bytes.extend_from_slice(&value.step.to_le_bytes());
        bytes.extend_from_slice(&value.rest);
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Workload, Error> {
        let Some((head, rest)) = bytes.split_first_chunk::<STEP_BYTES>() else {
            return Err(Error::malformed(format!(
                "workload state is {} bytes, shorter than its {STEP_BYTES}-byte step",
                bytes.len()
            )));
        };
        Ok(Workload {
            held: true,
            step: u32::from_le_bytes(*head),
            rest: rest.to_vec(),
        })
    }
}

struct SdkBytesTest;

impl Processor for SdkBytesTest {
    fn receive(activation: Activation) -> Result<Option<Body>, Error> {
        let mut commands = Vec::new();
        let mut windows = Vec::new();
        let mut stored = false;
        for window in activation.port_windows() {
            if window.port() == STATE {
                stored = !window.context_messages().is_empty() || !window.new_messages().is_empty();
            } else if window.port() == IN {
                for message in window.new_messages() {
                    commands.extend(message.body().as_text()?.lines().map(str::to_owned));
                }
            } else if window.port() == FEED {
                windows.push(json!({
                    "port": window.port(),
                    "new_from": window.context_messages().len(),
                    "dropped": window.dropped(),
                    "envelopes": window
                        .context_messages()
                        .iter()
                        .chain(window.new_messages())
                        .map(report_message)
                        .collect::<Result<Vec<_>, Error>>()?,
                }));
            }
        }
        commands.retain(|line| !line.trim().is_empty());
        let sync = activation.sync().map(str::to_owned);
        let loss = if commands
            .iter()
            .any(|line| verb(line) == Some("state-recover"))
        {
            StateLoss::Recover
        } else {
            StateLoss::FailActivation
        };
        let creates = commands
            .iter()
            .any(|line| matches!(verb(line), Some("state" | "state-recover")));

        let results = if commands.is_empty() {
            Vec::new()
        } else if !stored && !creates {
            let mut scratch = Workload::default();
            commands
                .iter()
                .map(|line| run(line, &mut scratch))
                .collect::<Result<Vec<_>, Error>>()?
        } else {
            RetainedState::<Workload, WorkloadCodec>::around_on(
                activation,
                STATE,
                loss,
                |_, workload| {
                    commands
                        .iter()
                        .map(|line| run(line, workload))
                        .collect::<Result<Vec<_>, Error>>()
                },
            )?
        };

        let report = json!({
            "windows": windows,
            "streams": [],
            "deferred": [],
            "results": results,
            "sync": sync,
        });
        publish(REPORT, &report.to_string())?;
        Ok(None)
    }
}

/// One `feed` message as the report shows it.
fn report_message(message: &Message) -> Result<Value, Error> {
    let header: Value = serde_json::from_str(message.header_json())
        .map_err(|e| Error::malformed(format!("feed header is not JSON: {e}")))?;
    let body = message.body();
    let envelope = message.envelope();
    let text_body = envelope
        .as_ref()
        .ok()
        .and_then(|envelope| envelope.text_body().ok().map(str::to_owned));
    let accessor = |read: &dyn Fn(&MessageEnvelope) -> Result<Value, Error>| match &envelope {
        Ok(envelope) => read(envelope).unwrap_or_else(|e| json!({ "err": error_kind(&e) })),
        Err(e) => json!({ "err": error_kind(e) }),
    };
    Ok(json!({
        "header": header,
        "content_type": body.content_type(),
        "hex": hex(body.as_bytes()),
        "text_body": text_body.map_or_else(|| Value::from(MALFORMED), Value::from),
        "as_text": body.as_text().map_or_else(|_| Value::from(MALFORMED), Value::from),
        "json": body.json::<Value>().unwrap_or_else(|_| Value::from(MALFORMED)),
        "mqtt_transport": accessor(&|e| e.mqtt_transport().map(ok_json)),
        "webhook_transport": accessor(&|e| e.webhook_transport().map(ok_json)),
        "mqtt_body": accessor(&|e| e.mqtt_body().map(|v| ok_json(&v))),
        "webhook_body": accessor(&|e| e.webhook_body().map(|v| ok_json(&v))),
    }))
}

/// An accessor's `Ok` value as the report records it.
fn ok_json<T: brenn_guest::serde::Serialize>(value: &T) -> Value {
    json!({ "ok": value })
}

/// The kind of an SDK error, as the report records it.
fn error_kind(error: &Error) -> &'static str {
    match error {
        Error::MalformedEnvelope(_) => "malformed-envelope",
        Error::ProcessingFailed(_) => "processing-failed",
        Error::QuotaExceeded(_) => "quota-exceeded",
        Error::NotPending(_) => "not-pending",
        Error::TextOnlyChannel(_) => "text-only-channel",
        Error::InvalidBody(_) => "invalid-body",
    }
}

fn verb(line: &str) -> Option<&str> {
    line.split_whitespace().next()
}

/// Run one command line against the state; the result is the word the report
/// records for it.
fn run(line: &str, workload: &mut Workload) -> Result<String, Error> {
    let words: Vec<&str> = line.split_whitespace().collect();
    let bad = || Error::failed(format!("unknown command {line:?}"));
    match words.as_slice() {
        ["noop"] => Ok(String::from("ok")),
        ["state" | "state-recover", len] => state(len.parse().map_err(|_| bad())?, workload),
        ["fail"] => Err(Error::failed("asked to fail")),
        _ => Err(bad()),
    }
}

/// Create the workload or advance its step.
fn state(raw_len: usize, workload: &mut Workload) -> Result<String, Error> {
    if raw_len < STEP_BYTES {
        return Err(Error::failed(format!(
            "state: raw-len {raw_len} is under {STEP_BYTES}"
        )));
    }
    if workload.held {
        let len = STEP_BYTES + workload.rest.len();
        if len != raw_len {
            return Err(Error::failed(format!(
                "state: stored length {len} is not {raw_len}"
            )));
        }
        workload.step = workload.step.wrapping_add(1);
    } else {
        let mut head = [0u8; STEP_BYTES];
        for (i, byte) in head.iter_mut().enumerate() {
            *byte = pattern(i);
        }
        *workload = Workload {
            held: true,
            step: u32::from_le_bytes(head),
            rest: (STEP_BYTES..raw_len).map(pattern).collect(),
        };
    }
    // The hash of the encoded body, taken over its two halves rather than over
    // a second copy of the state.
    let hash = fnv1a64_from(
        fnv1a64_from(FNV_OFFSET, &workload.step.to_le_bytes()),
        &workload.rest,
    );
    Ok(format!(
        "state:{}:{hash:016x}",
        STEP_BYTES + workload.rest.len()
    ))
}

// `pattern` and FNV-1a 64 below are copies of the state workload's in
// `processor-page-test` and `processor-page-bytes-test`, and `hex` of theirs and
// `processor-bytes-test`'s; an edit to one is an edit to all. The workload and
// its hash must stay identical across them, since one oracle checks every
// fixture's state body. `brenn-wasm/tests/sdk_bytes.rs` and
// `frontend/src/page-rig/state.ts` keep their own copies on purpose: an oracle
// sharing the fixture's code would check nothing.

/// Byte `i` of the workload: (i * 31 + 7) mod 256.
fn pattern(i: usize) -> u8 {
    ((i * 31 + 7) % 256) as u8
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from(DIGITS[usize::from(b >> 4)]));
        out.push(char::from(DIGITS[usize::from(b & 0xf)]));
    }
    out
}

/// FNV-1a 64's offset basis.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a 64 continued from `hash` over `bytes`.
fn fnv1a64_from(mut hash: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

brenn_guest::export_processor!(SdkBytesTest);

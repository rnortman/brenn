// The page host's test fixture for the `brenn:processor@0.2.0` world: bodies
// cross the boundary as bytes with a content type. The twin of
// `processor-page-test`, differing from it only where the contracts do.
//
// Every activation that returns ok publishes one JSON report on `report`, as
// untyped text: the page's wire carries untyped text only, and the rig binds
// `report` to a wire channel, so a typed report would be refused.
//
//   windows   each port window other than `in` and `state`: its port, new_from,
//             dropped, and each envelope as its header (parsed), content type
//             and bytes in hex
//   deferred  each deferred window: its port, and each entry's index, content
//             type, bytes in hex and deliver_after
//   results   one word per command run
//   sync      the activation's sync port, or null
//
// `in` carries the commands and is not reported. `state` carries the workload
// below, megabytes on a measuring page, and is shown only through the checksum
// the `state` command reports.
//
// Commands are untyped text lines read from the bodies of the new envelopes on
// `in`. A content type is `-` for none; bytes are hex, `_` for none.
//
//   noop
//   publish <port> <content-type> <hex>
//   defer <port> <content-type> <hex> <deliver-after-ms>
//   edit <port> <index> <content-type> <hex>
//   cancel <port> <index>
//   call <port> <content-type> <hex>
//                                   the result is `ok:none`,
//                                   `ok:<content-type>:<hex>` or the
//                                   call-error's word
//   reply <content-type> <hex>      the sync reply
//   state <raw-len>                 the UTF-8 state workload
//   fail                            end the activation with an error
//
// A publish, defer-op or call result is `ok` or the error variant's word; a
// variant that carries a diagnostic is recorded as `<word>:<diagnostic>`. A
// line that is none of these ends the activation with an error naming it.
//
// A sync-call activation whose request carries a content type runs no
// commands: its reply is the request's body, unchanged. An untyped request is
// command lines, so a caller on either contract can ask for any reply with
// `reply`. The request window is reported like any other window.
//
// `state <raw-len>` reads the newest envelope in the `state` window. With none,
// it creates `raw-len` bytes of printable ASCII: an eight-digit hex counter
// `00000000`, then byte i being 32 + (i * 31 + 7) mod 95. With one, it checks
// the body's length is `raw-len` and adds one to the counter. Either way it
// publishes the bytes on `state` as `text/plain` and reports
// `state:<len>:<FNV-1a 64 of the bytes, hex>`. A `raw-len` under eight is an
// error.

// The generated `receive` lowering takes one flattened argument per canonical-ABI
// field, which is more than the lint's threshold. Nothing there is hand-written.
#![allow(clippy::too_many_arguments)]

wit_bindgen::generate!({
    world: "brenn:processor/processor@0.2.0",
    path: ["../../wit/processor.wit", "../../wit/processor-0.2.wit"],
    generate_all,
});

use brenn::processor0_2_0::calls::{CallError, call};
use brenn::processor0_2_0::ports::{
    DeferError, PublishError, defer_cancel, defer_edit, publish, publish_deferred,
};
use serde_json::{Value, json};

struct PageBytesTest;

/// The input port commands are read from.
const COMMAND_PORT: &str = "in";
/// The io port the state workload reads and writes.
const STATE_PORT: &str = "state";
/// The output port the report is published on.
const REPORT_PORT: &str = "report";
/// The content type the state workload publishes under.
const STATE_CONTENT_TYPE: &str = "text/plain";
/// The width of the state workload's hex counter.
const COUNTER_DIGITS: usize = 8;

impl Guest for PageBytesTest {
    fn receive(a: Activation) -> Result<Option<Body>, ReceiveError> {
        let mut windows = Vec::new();
        let mut commands: Vec<String> = Vec::new();
        let mut reply: Option<Body> = None;
        let mut state_window: Option<&Body> = None;
        for w in &a.ports {
            let is_sync = a.sync.as_deref() == Some(w.port.as_str());
            if w.port == COMMAND_PORT {
                for e in w.envelopes.iter().skip(w.new_from as usize) {
                    commands.extend(command_text(&e.body)?.lines().map(str::to_owned));
                }
                continue;
            }
            if w.port == STATE_PORT {
                state_window = w.envelopes.last().map(|e| &e.body);
                continue;
            }
            if is_sync {
                let request = w.envelopes.last().ok_or_else(|| {
                    ReceiveError::MalformedEnvelope(String::from("sync window is empty"))
                })?;
                if request.body.content_type.is_some() {
                    reply = Some(request.body.clone());
                } else {
                    commands.extend(command_text(&request.body)?.lines().map(str::to_owned));
                }
            }
            let envelopes = w
                .envelopes
                .iter()
                .map(|e| {
                    let header: Value = serde_json::from_str(&e.header).map_err(|_| {
                        ReceiveError::MalformedEnvelope(String::from("header is not JSON"))
                    })?;
                    Ok(json!({
                        "header": header,
                        "content_type": e.body.content_type,
                        "hex": hex(&e.body.bytes),
                    }))
                })
                .collect::<Result<Vec<_>, ReceiveError>>()?;
            windows.push(json!({
                "port": w.port,
                "new_from": w.new_from,
                "dropped": w.dropped,
                "envelopes": envelopes,
            }));
        }

        let deferred: Vec<Value> = a
            .deferred
            .iter()
            .map(|dw| {
                json!({
                    "port": dw.port,
                    "entries": dw.entries.iter().map(|e| json!({
                        "index": e.index,
                        "content_type": e.body.content_type,
                        "hex": hex(&e.body.bytes),
                        "deliver_after": e.deliver_after,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();

        let mut results = Vec::new();
        for line in commands.iter().filter(|l| !l.trim().is_empty()) {
            results.push(run(line, state_window, &mut reply)?);
        }

        let report = json!({
            "windows": windows,
            "deferred": deferred,
            "results": results,
            "sync": a.sync,
        });
        publish(
            REPORT_PORT,
            &Body {
                content_type: None,
                bytes: report.to_string().into_bytes(),
            },
        )
        .map_err(|e| ReceiveError::ProcessingFailed(format!("report: {}", publish_word(&e))))?;
        Ok(reply)
    }
}

/// Run one command line; the result is the word the report records for it.
fn run(
    line: &str,
    state_window: Option<&Body>,
    reply: &mut Option<Body>,
) -> Result<String, ReceiveError> {
    let words: Vec<&str> = line.split_whitespace().collect();
    let bad = || ReceiveError::ProcessingFailed(format!("unknown command {line:?}"));
    Ok(match words.as_slice() {
        ["noop"] => String::from("ok"),
        ["publish", port, ct, bytes] => word(
            publish(port, &body(ct, bytes).ok_or_else(bad)?),
            publish_word,
        ),
        ["defer", port, ct, bytes, at] => word(
            publish_deferred(
                port,
                &body(ct, bytes).ok_or_else(bad)?,
                at.parse().map_err(|_| bad())?,
            ),
            publish_word,
        ),
        ["edit", port, index, ct, bytes] => word(
            defer_edit(
                port,
                index.parse().map_err(|_| bad())?,
                Some(&body(ct, bytes).ok_or_else(bad)?),
                None,
            ),
            defer_word,
        ),
        ["cancel", port, index] => word(
            defer_cancel(port, index.parse().map_err(|_| bad())?),
            defer_word,
        ),
        ["call", port, ct, bytes] => match call(port, &body(ct, bytes).ok_or_else(bad)?) {
            Ok(None) => String::from("ok:none"),
            Ok(Some(reply)) => format!(
                "ok:{}:{}",
                reply.content_type.as_deref().unwrap_or("-"),
                hex(&reply.bytes)
            ),
            Err(e) => call_word(&e),
        },
        ["reply", ct, bytes] => {
            *reply = Some(body(ct, bytes).ok_or_else(bad)?);
            String::from("ok")
        }
        ["state", len] => state(len.parse().map_err(|_| bad())?, state_window)?,
        ["fail"] => {
            return Err(ReceiveError::ProcessingFailed(String::from(
                "asked to fail",
            )));
        }
        _ => return Err(bad()),
    })
}

/// The state workload: create or advance the stored state and publish it back.
fn state(raw_len: usize, stored: Option<&Body>) -> Result<String, ReceiveError> {
    let failed = |what: &str| ReceiveError::ProcessingFailed(format!("state: {what}"));
    if raw_len < COUNTER_DIGITS {
        return Err(failed(&format!(
            "raw-len {raw_len} is under {COUNTER_DIGITS}"
        )));
    }
    let bytes = match stored {
        None => {
            let mut bytes: Vec<u8> = (0..raw_len)
                .map(|i| 32 + ((i * 31 + 7) % 95) as u8)
                .collect();
            bytes[..COUNTER_DIGITS].fill(b'0');
            bytes
        }
        Some(body) => {
            if body.bytes.len() != raw_len {
                return Err(failed(&format!(
                    "stored length {} is not {raw_len}",
                    body.bytes.len()
                )));
            }
            let mut bytes = body.bytes.clone();
            let counter = std::str::from_utf8(&bytes[..COUNTER_DIGITS])
                .ok()
                .and_then(|digits| u32::from_str_radix(digits, 16).ok())
                .ok_or_else(|| failed("the counter is not hex"))?;
            let next = format!("{:08x}", counter.wrapping_add(1));
            bytes[..COUNTER_DIGITS].copy_from_slice(next.as_bytes());
            bytes
        }
    };
    let result = format!("state:{}:{:016x}", bytes.len(), fnv1a64(&bytes));
    publish(
        STATE_PORT,
        &Body {
            content_type: Some(String::from(STATE_CONTENT_TYPE)),
            bytes,
        },
    )
    .map_err(|e| failed(&publish_word(&e)))?;
    Ok(result)
}

/// A command body's text. Commands are untyped UTF-8; anything else is a
/// malformed command, not a command to skip.
fn command_text(body: &Body) -> Result<&str, ReceiveError> {
    std::str::from_utf8(&body.bytes)
        .map_err(|_| ReceiveError::MalformedEnvelope(String::from("command is not UTF-8")))
}

fn word<E>(result: Result<(), E>, name: fn(&E) -> String) -> String {
    match result {
        Ok(()) => String::from("ok"),
        Err(e) => name(&e),
    }
}

/// `word`, or `word:diagnostic` for a variant that carries one.
fn with_reason(word: &str, reason: &str) -> String {
    format!("{word}:{reason}")
}

fn publish_word(e: &PublishError) -> String {
    match e {
        PublishError::NotPermitted => String::from("not-permitted"),
        PublishError::InvalidPayload(reason) => with_reason("invalid-payload", reason),
        PublishError::InvalidBody(reason) => with_reason("invalid-body", reason),
        PublishError::TextOnlyChannel => String::from("text-only-channel"),
        PublishError::QuotaExceeded => String::from("quota-exceeded"),
    }
}

fn defer_word(e: &DeferError) -> String {
    match e {
        DeferError::NotPermitted => String::from("not-permitted"),
        DeferError::OutOfRange => String::from("out-of-range"),
        DeferError::QuotaExceeded => String::from("quota-exceeded"),
        DeferError::InvalidDeliverAfter => String::from("invalid-deliver-after"),
        DeferError::InvalidBody(reason) => with_reason("invalid-body", reason),
        DeferError::TextOnlyChannel => String::from("text-only-channel"),
    }
}

fn call_word(e: &CallError) -> String {
    match e {
        CallError::NotPermitted => String::from("not-permitted"),
        CallError::Unwired => String::from("unwired"),
        CallError::Refused => String::from("refused"),
        CallError::Failed => String::from("failed"),
        CallError::InvalidPayload(reason) => with_reason("invalid-payload", reason),
        CallError::InvalidBody(reason) => with_reason("invalid-body", reason),
        CallError::TextOnlyPeer => String::from("text-only-peer"),
        CallError::QuotaExceeded => String::from("quota-exceeded"),
    }
}

/// A body from a command's content-type and hex words.
fn body(ct: &str, bytes: &str) -> Option<Body> {
    Some(Body {
        content_type: (ct != "-").then(|| ct.to_owned()),
        bytes: unhex(bytes)?,
    })
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s == "_" {
        return Some(Vec::new());
    }
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
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

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

export!(PageBytesTest);

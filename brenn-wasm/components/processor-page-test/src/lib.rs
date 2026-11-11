// The page host's test fixture for the `brenn:processor@0.1.0` world: bodies
// cross the boundary as text inside JSON envelopes.
//
// Every activation that returns ok publishes one JSON report on `report`:
//
//   windows   each port window other than `in` and `state`: its port, new_from,
//             dropped, and each envelope as the hex of the UTF-8 bytes of the
//             string the guest was handed, unparsed
//   deferred  each deferred window: its port, and each entry's index, payload
//             in hex and deliver_after
//   results   one word per command run
//   sync      the activation's sync port, or null
//
// `in` carries the commands and is not reported. `state` carries the workload
// below, megabytes on a measuring page, and is shown only through the checksum
// the `state` command reports.
//
// Commands are text lines read from the bodies of the new envelopes on `in`.
// Bodies are hex of UTF-8, `_` for the empty body.
//
//   noop
//   publish <port> <hex>
//   publish-fill <port> <len>       publish <len> bytes of 'x'
//   defer <port> <hex> <deliver-after-ms>
//   edit <port> <index> <hex>
//   cancel <port> <index>
//   call <port> <hex>               the result is `ok:none`, `ok:<hex>` or the
//                                   call-error's word
//   state <raw-len>                 the base64 state workload
//   fail                            end the activation with an error
//
// A publish or defer-op result is `ok` or the error variant's word. A line that
// is none of these, or hex that is not UTF-8, ends the activation with an error
// naming it.
//
// A sync-call activation runs no commands: its reply is the request's body,
// unchanged, and its request window is reported like any other window.
//
// `state <raw-len>` reads the newest envelope in the `state` window. With none,
// it creates `raw-len` bytes, byte i being (i * 31 + 7) mod 256. With one, it
// parses the envelope, parses its body `{"v":1,"state":"<base64>"}`, decodes
// the base64 and adds one to the little-endian u32 in the first four bytes.
// Either way it publishes `{"v":1,"state":"<base64>"}` on `state` and reports
// `state:<len>:<FNV-1a 64 of the raw bytes, hex>`. A stored state whose
// decoded length is not `raw-len` is an error.

// The generated `receive` lowering takes one flattened argument per canonical-ABI
// field, which is more than the lint's threshold. Nothing there is hand-written.
#![allow(clippy::too_many_arguments)]

wit_bindgen::generate!({
    world: "brenn:processor/processor@0.1.0",
    path: "../../wit/processor.wit",
});

use brenn::processor::calls::{CallError, call};
use brenn::processor::ports::{
    DeferError, PublishError, defer_cancel, defer_edit, publish, publish_deferred,
};
use serde_json::{Value, json};

struct PageTest;

/// The input port commands are read from.
const COMMAND_PORT: &str = "in";
/// The io port the state workload reads and writes.
const STATE_PORT: &str = "state";
/// The output port the report is published on.
const REPORT_PORT: &str = "report";

impl Guest for PageTest {
    fn receive(a: Activation) -> Result<Option<String>, ReceiveError> {
        let mut windows = Vec::new();
        let mut commands: Vec<String> = Vec::new();
        let mut reply: Option<String> = None;
        let mut state_window: Option<&String> = None;
        for w in &a.ports {
            let is_sync = a.sync.as_deref() == Some(w.port.as_str());
            if w.port == COMMAND_PORT {
                for e in w.envelopes.iter().skip(w.new_from as usize) {
                    commands.extend(envelope_body(e)?.lines().map(str::to_owned));
                }
                continue;
            }
            if w.port == STATE_PORT {
                state_window = w.envelopes.last();
                continue;
            }
            if is_sync {
                let request = w.envelopes.last().ok_or_else(|| {
                    ReceiveError::MalformedEnvelope(String::from("sync window is empty"))
                })?;
                reply = Some(envelope_body(request)?);
            }
            windows.push(json!({
                "port": w.port,
                "new_from": w.new_from,
                "dropped": w.dropped,
                "envelopes": w.envelopes.iter().map(|e| hex(e.as_bytes())).collect::<Vec<_>>(),
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
                        "hex": hex(e.payload.as_bytes()),
                        "deliver_after": e.deliver_after,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect();

        let mut results = Vec::new();
        if a.sync.is_none() {
            for line in commands.iter().filter(|l| !l.trim().is_empty()) {
                results.push(run(line, state_window)?);
            }
        }

        let report = json!({
            "windows": windows,
            "deferred": deferred,
            "results": results,
            "sync": a.sync,
        });
        publish(REPORT_PORT, &report.to_string())
            .map_err(|e| ReceiveError::ProcessingFailed(format!("report: {}", publish_word(&e))))?;
        Ok(reply)
    }
}

/// Run one command line; the result is the word the report records for it.
fn run(line: &str, state_window: Option<&String>) -> Result<String, ReceiveError> {
    let words: Vec<&str> = line.split_whitespace().collect();
    let bad = || ReceiveError::ProcessingFailed(format!("unknown command {line:?}"));
    Ok(match words.as_slice() {
        ["noop"] => String::from("ok"),
        ["publish", port, body] => word(publish(port, &text(body).ok_or_else(bad)?), publish_word),
        ["publish-fill", port, len] => {
            let len: usize = len.parse().map_err(|_| bad())?;
            word(publish(port, &"x".repeat(len)), publish_word)
        }
        ["defer", port, body, at] => word(
            publish_deferred(
                port,
                &text(body).ok_or_else(bad)?,
                at.parse().map_err(|_| bad())?,
            ),
            publish_word,
        ),
        ["edit", port, index, body] => word(
            defer_edit(
                port,
                index.parse().map_err(|_| bad())?,
                Some(&text(body).ok_or_else(bad)?),
                None,
            ),
            defer_word,
        ),
        ["cancel", port, index] => word(
            defer_cancel(port, index.parse().map_err(|_| bad())?),
            defer_word,
        ),
        ["call", port, body] => match call(port, &text(body).ok_or_else(bad)?) {
            Ok(None) => String::from("ok:none"),
            Ok(Some(reply)) => format!("ok:{}", hex(reply.as_bytes())),
            Err(e) => call_word(&e),
        },
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
fn state(raw_len: usize, stored: Option<&String>) -> Result<String, ReceiveError> {
    let failed = |what: &str| ReceiveError::ProcessingFailed(format!("state: {what}"));
    let bytes = match stored {
        None => (0..raw_len).map(|i| ((i * 31 + 7) % 256) as u8).collect(),
        Some(envelope) => {
            let body = envelope_body(envelope)?;
            let doc: Value = serde_json::from_str(&body).map_err(|_| failed("body is not JSON"))?;
            let encoded = doc
                .get("state")
                .and_then(Value::as_str)
                .ok_or_else(|| failed("body has no state"))?;
            let mut bytes = base64_decode(encoded).ok_or_else(|| failed("bad base64"))?;
            if bytes.len() != raw_len {
                return Err(failed(&format!(
                    "stored length {} is not {raw_len}",
                    bytes.len()
                )));
            }
            let head: [u8; 4] = bytes
                .get(..4)
                .and_then(|h| h.try_into().ok())
                .ok_or_else(|| failed("state is shorter than four bytes"))?;
            let counter = u32::from_le_bytes(head).wrapping_add(1);
            bytes[..4].copy_from_slice(&counter.to_le_bytes());
            bytes
        }
    };
    let body = format!("{{\"v\":1,\"state\":\"{}\"}}", base64_encode(&bytes));
    publish(STATE_PORT, &body).map_err(|e| failed(&publish_word(&e)))?;
    Ok(format!("state:{}:{:016x}", bytes.len(), fnv1a64(&bytes)))
}

/// The `body` of a 0.1.0 envelope, which is text on this contract.
fn envelope_body(envelope: &str) -> Result<String, ReceiveError> {
    let malformed = |what: &str| ReceiveError::MalformedEnvelope(String::from(what));
    let value: Value =
        serde_json::from_str(envelope).map_err(|_| malformed("envelope is not JSON"))?;
    value
        .get("body")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| malformed("envelope has no text body"))
}

fn word<E>(result: Result<(), E>, name: fn(&E) -> String) -> String {
    match result {
        Ok(()) => String::from("ok"),
        Err(e) => name(&e),
    }
}

fn publish_word(e: &PublishError) -> String {
    String::from(match e {
        PublishError::NotPermitted => "not-permitted",
        PublishError::InvalidPayload(_) => "invalid-payload",
        PublishError::QuotaExceeded => "quota-exceeded",
    })
}

fn defer_word(e: &DeferError) -> String {
    String::from(match e {
        DeferError::NotPermitted => "not-permitted",
        DeferError::OutOfRange => "out-of-range",
        DeferError::QuotaExceeded => "quota-exceeded",
        DeferError::InvalidDeliverAfter => "invalid-deliver-after",
    })
}

fn call_word(e: &CallError) -> String {
    String::from(match e {
        CallError::NotPermitted => "not-permitted",
        CallError::Unwired => "unwired",
        CallError::Refused => "refused",
        CallError::Failed => "failed",
        CallError::InvalidPayload(_) => "invalid-payload",
        CallError::QuotaExceeded => "quota-exceeded",
    })
}

/// A command's body word as text: hex of UTF-8, `_` for empty.
fn text(s: &str) -> Option<String> {
    String::from_utf8(unhex(s)?).ok()
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

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Marks a byte outside the alphabet in [`BASE64_DECODE`].
const INVALID: u8 = 0xff;

/// Each byte's six-bit value in [`BASE64`], or [`INVALID`].
const BASE64_DECODE: [u8; 256] = {
    let mut table = [INVALID; 256];
    let mut i = 0;
    while i < 64 {
        table[BASE64[i] as usize] = i as u8;
        i += 1;
    }
    table
};

/// Standard-alphabet base64 with padding.
fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for (i, shift) in [18u32, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(char::from(BASE64[((n >> shift) & 0x3f) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The inverse of [`base64_encode`]; `None` on anything it would not produce.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.as_bytes();
    if !s.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for (q, quad) in s.chunks(4).enumerate() {
        let last = q + 1 == s.len() / 4;
        let pad = quad.iter().rev().take_while(|&&c| c == b'=').count();
        if pad > 2 || (pad > 0 && !last) {
            return None;
        }
        let mut n: u32 = 0;
        for &c in &quad[..4 - pad] {
            let v = BASE64_DECODE[usize::from(c)];
            if v == INVALID {
                return None;
            }
            n = (n << 6) | u32::from(v);
        }
        n <<= 6 * pad as u32;
        let decoded = [(n >> 16) as u8, (n >> 8) as u8, n as u8];
        out.extend_from_slice(&decoded[..3 - pad]);
    }
    Some(out)
}

export!(PageTest);

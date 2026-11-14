// Test fixture for the `brenn:processor@0.2.0` world: bodies cross the
// boundary as bytes with a content type.
//
// Every activation publishes one JSON report on `report`: each window
// envelope it was handed outside the command windows (its header, content
// type and bytes in hex), its deferred view, the sync port when the
// activation is a sync call, and the result of every command it ran.
//
// Commands are untyped text, one per line, read from the new envelopes on `in`
// and from the request of a sync-call activation. A content type is `-` for
// none; bytes are hex, `_` for none. A sync request that carries a content type
// is not commands: it is the reply, echoed unchanged.
//
//   publish <port> <content-type> <hex>
//   defer <port> <content-type> <hex> <deliver-after-ms>
//   edit <port> <index> <content-type> <hex>
//   cancel <port> <index>
//   call <port> <content-type> <hex>
//                                   call the peer wired to <port>; the result
//                                   is `ok:none`, `ok:<content-type>:<hex>` or
//                                   the call-error's word
//   reply <content-type> <hex>      the sync reply
//   fail                            end the activation with an error
//
// A line that is none of these ends the activation with an error naming it.

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

struct BytesTest;

/// The input port commands are read from.
const COMMAND_PORT: &str = "in";
/// The output port the report is published on.
const REPORT_PORT: &str = "report";

impl Guest for BytesTest {
    fn receive(a: Activation) -> Result<Option<Body>, ReceiveError> {
        let mut report = String::from("{\"windows\":[");
        let mut commands: Vec<String> = Vec::new();
        let mut echo: Option<Body> = None;
        for (i, w) in a.ports.iter().enumerate() {
            if i > 0 {
                report.push(',');
            }
            report.push_str(&format!(
                "{{\"port\":{},\"new_from\":{},\"dropped\":{},\"envelopes\":[",
                json_str(&w.port),
                w.new_from,
                w.dropped
            ));
            let is_sync = a.sync.as_deref() == Some(w.port.as_str());
            let is_command = w.port == COMMAND_PORT || is_sync;
            // Command windows are not echoed: a command carrying a body at the
            // cap would make the report itself oversize.
            let echoed = if is_command {
                &[][..]
            } else {
                &w.envelopes[..]
            };
            for (j, e) in echoed.iter().enumerate() {
                if j > 0 {
                    report.push(',');
                }
                report.push_str(&format!(
                    "{{\"header\":{},\"content_type\":{},\"hex\":\"{}\"}}",
                    e.header,
                    json_opt(e.body.content_type.as_deref()),
                    hex(&e.body.bytes)
                ));
            }
            report.push_str("]}");
            if is_command {
                let from = if is_sync { 0 } else { w.new_from as usize };
                for e in w.envelopes.iter().skip(from) {
                    if is_sync && e.body.content_type.is_some() {
                        echo = Some(e.body.clone());
                        continue;
                    }
                    let text = std::str::from_utf8(&e.body.bytes).map_err(|_| {
                        ReceiveError::MalformedEnvelope(String::from("command is not UTF-8"))
                    })?;
                    commands.extend(text.lines().map(str::to_owned));
                }
            }
        }
        report.push_str("],\"deferred\":[");
        for (i, dw) in a.deferred.iter().enumerate() {
            if i > 0 {
                report.push(',');
            }
            report.push_str(&format!("{{\"port\":{},\"entries\":[", json_str(&dw.port)));
            for (j, e) in dw.entries.iter().enumerate() {
                if j > 0 {
                    report.push(',');
                }
                report.push_str(&format!(
                    "{{\"index\":{},\"content_type\":{},\"hex\":\"{}\",\"deliver_after\":{}}}",
                    e.index,
                    json_opt(e.body.content_type.as_deref()),
                    hex(&e.body.bytes),
                    e.deliver_after
                ));
            }
            report.push_str("]}");
        }
        report.push_str("],\"results\":[");

        let mut reply = echo;
        for (i, line) in commands.iter().filter(|l| !l.trim().is_empty()).enumerate() {
            if i > 0 {
                report.push(',');
            }
            let result = run(line, &mut reply)?;
            report.push_str(&json_str(&result));
        }
        report.push_str(&format!("],\"sync\":{}}}", json_opt(a.sync.as_deref())));

        publish(
            REPORT_PORT,
            &Body {
                content_type: Some(String::from("application/json")),
                bytes: report.into_bytes(),
            },
        )
        .map_err(|e| ReceiveError::ProcessingFailed(format!("report: {}", publish_word(&e))))?;
        Ok(reply)
    }
}

/// Run one command line; the result is the word the report records for it.
fn run(line: &str, reply: &mut Option<Body>) -> Result<String, ReceiveError> {
    let words: Vec<&str> = line.split_whitespace().collect();
    let bad = || ReceiveError::ProcessingFailed(format!("unknown command {line:?}"));
    Ok(match words.as_slice() {
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
        ["fail"] => {
            return Err(ReceiveError::ProcessingFailed(String::from(
                "asked to fail",
            )));
        }
        _ => return Err(bad()),
    })
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
        PublishError::InvalidBody(_) => "invalid-body",
        PublishError::TextOnlyChannel => "text-only-channel",
        PublishError::QuotaExceeded => "quota-exceeded",
    })
}

fn defer_word(e: &DeferError) -> String {
    String::from(match e {
        DeferError::NotPermitted => "not-permitted",
        DeferError::OutOfRange => "out-of-range",
        DeferError::QuotaExceeded => "quota-exceeded",
        DeferError::InvalidDeliverAfter => "invalid-deliver-after",
        DeferError::InvalidBody(_) => "invalid-body",
        DeferError::TextOnlyChannel => "text-only-channel",
    })
}

fn call_word(e: &CallError) -> String {
    String::from(match e {
        CallError::NotPermitted => "not-permitted",
        CallError::Unwired => "unwired",
        CallError::Refused => "refused",
        CallError::Failed => "failed",
        CallError::InvalidPayload(_) => "invalid-payload",
        CallError::InvalidBody(_) => "invalid-body",
        CallError::TextOnlyPeer => "text-only-peer",
        CallError::QuotaExceeded => "quota-exceeded",
    })
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
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn json_opt(s: Option<&str>) -> String {
    s.map_or_else(|| String::from("null"), json_str)
}

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

export!(BytesTest);

// The `brenn:processor@0.2.0` contract on the backend host, against the
// processor-bytes-test fixture: what a 0.2.0 component is handed, what its
// publishes, deferred publishes and edits become, how its bodies are refused,
// and how its sync reply is judged. Beside it, a 0.1.0 artifact in the same
// process.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use brenn_activation::sync::{SyncAnswer, SyncRefusal};
use brenn_activation::{Activation, DeferredEntry, DeferredWindow, PortWindow};
use brenn_envelope::{MessageBody, MessageEnvelope};
use brenn_wasm::{
    ComponentGrant, HostActivation, OutputPortSpec, ProcessorCallTarget, ProcessorComponent,
    ProcessorContract, ProcessorDeferredOp, ProcessorLoadSpec, ProcessorOutcome, ProcessorPublish,
    SyncCallerFn,
};
use serde_json::Value;

mod common;

/// Every window port the fixture's specification declares as an input.
const WINDOW_PORTS: [&str; 4] = ["in", "feed", "state", "timer"];

/// A bound output port on `channel`, which may carry non-text bodies iff
/// `non_text`.
fn port(channel: &str, non_text: bool) -> OutputPortSpec {
    OutputPortSpec {
        non_text,
        ..common::out_spec(channel)
    }
}

/// The bytes fixture with `out` bound as given, `report` bound to a text-only
/// channel, and `state` and `timer` declared but unwired; its `peer` call port
/// reaches a peer that answers with no reply.
fn load_bytes(out: Option<OutputPortSpec>, max_payload_bytes: usize) -> ProcessorComponent {
    load_bytes_calling(
        out,
        max_payload_bytes,
        Arc::new(|_, _, _, _| SyncAnswer::Ok(None)),
    )
}

/// [`load_bytes`] with `peer` reaching `sync_caller`.
fn load_bytes_calling(
    out: Option<OutputPortSpec>,
    max_payload_bytes: usize,
    sync_caller: SyncCallerFn,
) -> ProcessorComponent {
    let mut output_ports = HashMap::from([("report".to_string(), port("brenn:report", false))]);
    if let Some(out) = out {
        output_ports.insert("out".to_string(), out);
    }
    ProcessorComponent::load(ProcessorLoadSpec {
        output_ports,
        declared_out_ports: ["out", "report", "state", "timer"]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<_>>(),
        input_amplification_mt: WINDOW_PORTS
            .into_iter()
            .map(|p| (p.to_string(), 1_000))
            .collect(),
        grants: [ComponentGrant::Ports, ComponentGrant::Calls]
            .into_iter()
            .collect(),
        max_payload_bytes,
        declared_call_ports: BTreeSet::from(["peer".to_string()]),
        calls: HashMap::from([(
            "peer".to_string(),
            ProcessorCallTarget {
                target_slug: "callee".to_string(),
                target_port: "answer".to_string(),
            },
        )]),
        sync_caller: Some(sync_caller),
        ..ProcessorLoadSpec::minimal(
            &common::artifact_path("brenn_processor_bytes_test"),
            "bytes-test",
        )
    })
}

/// An envelope on `channel` carrying `body`.
fn envelope(channel: &str, body: MessageBody) -> Arc<MessageEnvelope> {
    let mut e: MessageEnvelope = serde_json::from_value(serde_json::json!({
        "message_id": "00000000-0000-0000-0000-0000000000b1",
        "source": "test",
        "channel": channel,
        "sender": "test-sender",
        "publish_ts": "2026-01-01T00:00:00Z",
        "body": "",
        "urgency": "normal",
        "envelope_type": "brenn",
    }))
    .expect("a test envelope deserializes");
    e.body = body;
    Arc::new(e)
}

fn body(content_type: Option<&str>, bytes: &[u8]) -> MessageBody {
    MessageBody::from_parts(content_type, bytes).expect("a test body is well-formed")
}

fn window(port: &str, envelopes: Vec<Arc<MessageEnvelope>>) -> PortWindow<Arc<MessageEnvelope>> {
    PortWindow {
        port: port.to_string(),
        envelopes,
        new_from: 0,
        dropped: 0,
    }
}

/// An activation whose `in` window carries `commands` as one new text message.
fn commands(commands: &str) -> HostActivation {
    Activation {
        ports: vec![window(
            "in",
            vec![envelope("brenn:in", MessageBody::text(commands))],
        )],
        deferred: vec![],
        now: Some(1_000),
        sync: None,
    }
}

/// The publishes and deferred ops of an ok outcome, and its parsed report.
fn ok(outcome: ProcessorOutcome) -> (Vec<ProcessorPublish>, Vec<ProcessorDeferredOp>, Value) {
    let ProcessorOutcome::Ok {
        mut publishes,
        deferred_ops,
        reply: _,
    } = outcome
    else {
        panic!("expected an ok outcome, got {outcome:?}");
    };
    let at = publishes
        .iter()
        .position(|p| p.port == "report")
        .expect("every activation publishes a report");
    let report = publishes.remove(at);
    assert_eq!(report.body.content_type(), Some("application/json"));
    let report = serde_json::from_str(report.body.as_text().expect("the report is text"))
        .expect("the report is JSON");
    (publishes, deferred_ops, report)
}

fn results(report: &Value) -> Vec<&str> {
    report["results"]
        .as_array()
        .expect("results")
        .iter()
        .map(|r| r.as_str().expect("a result word"))
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Bytes that are not UTF-8 (a JPEG start-of-image marker).
const NOT_UTF8: &[u8] = &[0xff, 0xd8, 0xff, 0x00];

#[test]
fn a_0_1_and_a_0_2_artifact_load_and_run_side_by_side() {
    let v1 = common::load_processor_noop("brenn_processor_exhaust", "string-contract");
    let v2 = load_bytes(Some(port("brenn:out", true)), 1024 * 1024);
    assert_eq!(v1.contract(), ProcessorContract::V0_1);
    assert_eq!(v2.contract(), ProcessorContract::V0_2);

    let (_, _, report) = ok(v2.handle(commands("")));
    assert_eq!(report["windows"][0]["port"], "in");
    // The 0.1.0 artifact runs in the same process, through its own lowering;
    // with nothing new it answers ok.
    let outcome = v1.handle(Activation {
        ports: vec![],
        deferred: vec![],
        now: None,
        sync: None,
    });
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "the 0.1.0 artifact runs beside a 0.2.0 one: {outcome:?}"
    );
}

#[test]
fn a_0_2_activation_delivers_header_content_type_and_bytes_exactly() {
    let component = load_bytes(None, 1024 * 1024);
    let feed = vec![
        envelope("brenn:feed", MessageBody::text("plain")),
        envelope("brenn:feed", body(Some("text/markdown"), b"# title")),
        envelope("brenn:feed", body(Some("image/jpeg"), NOT_UTF8)),
    ];
    let parked = envelope(
        "brenn:out",
        body(Some("application/octet-stream"), NOT_UTF8),
    );
    let mut activation = commands("");
    activation.ports.push(window("feed", feed.clone()));
    activation.deferred.push(DeferredWindow {
        port: "out".to_string(),
        entries: vec![DeferredEntry {
            index: 0,
            payload: parked,
            deliver_after: 5_000,
        }],
    });
    let (_, _, report) = ok(component.handle(activation));

    let delivered = report["windows"][1]["envelopes"]
        .as_array()
        .expect("the feed window");
    assert_eq!(report["windows"][1]["port"], "feed");
    assert_eq!(delivered.len(), feed.len());
    for (sent, got) in feed.iter().zip(delivered) {
        assert_eq!(got["content_type"].as_str(), sent.body.content_type());
        assert_eq!(got["hex"], hex(sent.body.as_bytes()));
        let header: Value = serde_json::from_str(&sent.to_header_json()).unwrap();
        assert_eq!(
            got["header"], header,
            "the header is the envelope less its body"
        );
        assert!(got["header"].get("body").is_none());
    }

    let entry = &report["deferred"][0]["entries"][0];
    assert_eq!(report["deferred"][0]["port"], "out");
    assert_eq!(entry["index"], 0);
    assert_eq!(entry["content_type"], "application/octet-stream");
    assert_eq!(entry["hex"], hex(NOT_UTF8));
    assert_eq!(entry["deliver_after"], 5_000);
}

#[test]
fn a_0_2_publish_deferred_publish_and_edit_carry_the_body_given() {
    let component = load_bytes(Some(port("brenn:out", true)), 1024 * 1024);
    let mut activation = commands(&format!(
        "publish out image/jpeg {h}\n\
         defer out application/octet-stream {h} 9000\n\
         edit out 0 image/png {h}\n\
         publish out text/markdown 2a",
        h = hex(NOT_UTF8)
    ));
    activation.deferred.push(DeferredWindow {
        port: "out".to_string(),
        entries: vec![DeferredEntry {
            index: 0,
            payload: envelope("brenn:out", MessageBody::text("parked")),
            deliver_after: 5_000,
        }],
    });
    let (publishes, ops, report) = ok(component.handle(activation));
    assert_eq!(results(&report), ["ok", "ok", "ok", "ok"]);

    assert_eq!(publishes.len(), 3);
    assert_eq!(publishes[0].port, "out");
    assert_eq!(publishes[0].channel_address, "brenn:out");
    assert_eq!(publishes[0].body, body(Some("image/jpeg"), NOT_UTF8));
    assert_eq!(publishes[0].deliver_after, None);
    assert_eq!(
        publishes[1].body,
        body(Some("application/octet-stream"), NOT_UTF8)
    );
    assert_eq!(publishes[1].deliver_after, Some(9_000));
    assert_eq!(publishes[2].body, body(Some("text/markdown"), b"*"));
    assert!(publishes[2].body.is_text());

    assert_eq!(
        ops,
        [ProcessorDeferredOp::Edit {
            port: "out".to_string(),
            index: 0,
            body: Some(body(Some("image/png"), NOT_UTF8)),
            deliver_after: None,
        }]
    );
}

#[test]
fn a_malformed_body_is_invalid_body() {
    let component = load_bytes(Some(port("brenn:out", true)), 1024 * 1024);
    let (publishes, _, report) = ok(component.handle(commands(
        "publish out noslash 00\n\
         publish out - ff\n\
         publish out text/plain ff\n\
         publish out application/json ff",
    )));
    assert_eq!(
        results(&report),
        [
            "invalid-body",
            "invalid-body",
            "invalid-body",
            "invalid-body"
        ]
    );
    assert!(publishes.is_empty(), "nothing is buffered");
}

#[test]
fn a_non_text_body_on_a_text_only_port_is_refused_and_typed_text_is_not() {
    let component = load_bytes(Some(port("brenn:out", false)), 1024 * 1024);
    let mut activation = commands(&format!(
        "publish out image/jpeg {h}\n\
         defer out image/jpeg {h} 9000\n\
         publish out text/plain;charset=iso-8859-1 41\n\
         edit out 0 image/png {h}\n\
         publish out text/markdown 2a",
        h = hex(NOT_UTF8)
    ));
    activation.deferred.push(DeferredWindow {
        port: "out".to_string(),
        entries: vec![DeferredEntry {
            index: 0,
            payload: envelope("brenn:out", MessageBody::text("parked")),
            deliver_after: 5_000,
        }],
    });
    let (publishes, ops, report) = ok(component.handle(activation));
    assert_eq!(
        results(&report),
        [
            "text-only-channel",
            "text-only-channel",
            "text-only-channel",
            "text-only-channel",
            "ok"
        ]
    );
    assert!(ops.is_empty(), "the refused edit buffers nothing");
    assert_eq!(publishes.len(), 1, "only the typed text is buffered");
    assert_eq!(publishes[0].body, body(Some("text/markdown"), b"*"));
}

fn text_only_refusal_warns(lines: &[&str]) -> Vec<String> {
    lines
        .iter()
        .filter(|l| l.contains("non-text bodies refused"))
        .map(|l| l.to_string())
        .collect()
}

#[test]
#[tracing_test::traced_test]
fn text_only_refusals_reach_the_operator_as_one_warn_per_activation() {
    let component = load_bytes(Some(port("brenn:out", false)), 1024 * 1024);
    let (publishes, _, report) = ok(component.handle(commands(&format!(
        "publish out image/jpeg {h}\n\
         publish out application/octet-stream {h}\n\
         defer out image/jpeg {h} 9000",
        h = hex(NOT_UTF8)
    ))));
    assert_eq!(
        results(&report),
        [
            "text-only-channel",
            "text-only-channel",
            "text-only-channel"
        ]
    );
    assert!(publishes.is_empty(), "nothing is buffered");
    logs_assert(|lines| {
        let warns = text_only_refusal_warns(lines);
        match warns.as_slice() {
            [line]
                if line.contains("WARN")
                    && line.contains("bytes-test")
                    && line.contains(r#"{("out", "brenn:out"): 3}"#) =>
            {
                Ok(())
            }
            _ => Err(format!(
                "expected one warn naming the slug and ({{out, brenn:out}}: 3), got {warns:?}"
            )),
        }
    });
}

#[test]
#[tracing_test::traced_test]
fn a_non_text_publish_on_a_non_text_port_writes_no_refusal_warn() {
    let component = load_bytes(Some(port("brenn:out", true)), 1024 * 1024);
    let (publishes, _, report) = ok(component.handle(commands(&format!(
        "publish out image/jpeg {}",
        hex(NOT_UTF8)
    ))));
    assert_eq!(results(&report), ["ok"]);
    assert_eq!(publishes.len(), 1);
    logs_assert(|lines| {
        let warns = text_only_refusal_warns(lines);
        if warns.is_empty() {
            Ok(())
        } else {
            Err(format!("expected no refusal warn, got {warns:?}"))
        }
    });
}

#[test]
fn the_body_cap_counts_raw_bytes() {
    let cap = 4096;
    let component = load_bytes(Some(port("brenn:out", true)), cap);
    let (publishes, _, report) = ok(component.handle(commands(&format!(
        "publish out application/octet-stream {}\npublish out application/octet-stream {}",
        hex(&vec![0xffu8; cap + 1]),
        hex(&vec![0xffu8; cap]),
    ))));
    assert_eq!(results(&report), ["invalid-payload", "ok"]);
    assert_eq!(publishes.len(), 1);
    assert_eq!(publishes[0].body.len(), cap);
}

#[test]
fn a_declared_unwired_port_checks_the_body_but_has_no_channel_rule() {
    let component = load_bytes(Some(port("brenn:out", false)), 1024 * 1024);
    let (publishes, _, report) = ok(component.handle(commands(&format!(
        "publish state noslash 00\npublish state image/jpeg {}",
        hex(NOT_UTF8)
    ))));
    assert_eq!(results(&report), ["invalid-body", "ok"]);
    assert!(
        publishes.is_empty(),
        "an unwired port drops what it accepts"
    );
}

#[test]
fn a_malformed_body_on_an_undeclared_port_traps() {
    let component = load_bytes(Some(port("brenn:out", true)), 1024 * 1024);
    for command in ["publish nowhere noslash 00", "edit nowhere 0 noslash 00"] {
        let outcome = component.handle(commands(command));
        let ProcessorOutcome::Trap(why) = outcome else {
            panic!("{command:?}: expected a trap, got {outcome:?}");
        };
        assert!(why.contains("not declared"), "{command:?}: {why}");
    }
}

#[test]
fn a_malformed_edit_body_out_of_range_is_out_of_range() {
    let component = load_bytes(Some(port("brenn:out", true)), 1024 * 1024);
    let (_, ops, report) = ok(component.handle(commands("edit out 3 noslash 00")));
    assert_eq!(results(&report), ["out-of-range"]);
    assert!(ops.is_empty());
}

/// A sync-call activation asking the fixture to reply with `command`'s body.
fn sync_call(command: &str) -> HostActivation {
    Activation {
        ports: vec![window(
            "ask",
            vec![envelope("brenn:ask", MessageBody::text(command))],
        )],
        deferred: vec![],
        now: Some(1_000),
        sync: Some("ask".to_string()),
    }
}

#[test]
fn a_typed_text_reply_is_the_answer_with_its_type() {
    let component = load_bytes(Some(port("brenn:out", true)), 1024 * 1024);
    let ProcessorOutcome::Ok { reply, .. } = component.handle(sync_call("reply text/markdown 2a"))
    else {
        panic!("a text reply is ok");
    };
    assert_eq!(reply, Some(body(Some("text/markdown"), b"*")));
}

/// A reply is judged against the callee's own contract: a non-text one ends
/// the activation ok and its buffer flushes, whoever asked.
#[test]
fn a_non_text_reply_ends_the_callee_ok_with_its_buffer_flushed() {
    let component = load_bytes(Some(port("brenn:out", true)), 1024 * 1024);
    let outcome = component.handle(sync_call(&format!(
        "publish out - 6869\nreply image/png {}",
        hex(NOT_UTF8)
    )));
    let ProcessorOutcome::Ok {
        reply, publishes, ..
    } = outcome
    else {
        panic!("a non-text reply is ok, got {outcome:?}");
    };
    assert_eq!(reply, Some(body(Some("image/png"), NOT_UTF8)));
    assert!(
        publishes.iter().any(|p| p.port == "out"),
        "the buffer flushes"
    );
}

#[test]
fn a_malformed_reply_traps() {
    let component = load_bytes(Some(port("brenn:out", true)), 1024 * 1024);
    let outcome = component.handle(sync_call("publish out - 6869\nreply noslash 00"));
    let ProcessorOutcome::Trap(why) = outcome else {
        panic!("expected a trap, got {outcome:?}");
    };
    assert!(why.contains("invalid body"), "{why}");
}

#[test]
fn a_reply_over_the_cap_traps() {
    let cap = 4096;
    let component = load_bytes(Some(port("brenn:out", true)), cap);
    let outcome = component.handle(sync_call(&format!("reply - {}", hex(&vec![b'a'; cap + 1]))));
    let ProcessorOutcome::Trap(why) = outcome else {
        panic!("expected a trap, got {outcome:?}");
    };
    assert!(why.contains("reply cap"), "{why}");
}

/// What the seam was handed: the target and the request body.
type Asked = Arc<Mutex<Vec<(String, String, MessageBody)>>>;

/// A seam recording every call and answering each with `answer`.
fn recording(answer: SyncAnswer) -> (SyncCallerFn, Asked) {
    let asked: Asked = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&asked);
    let caller: SyncCallerFn = Arc::new(move |slug, port, body, _chain| {
        record
            .lock()
            .unwrap()
            .push((slug.to_string(), port.to_string(), body));
        answer.clone()
    });
    (caller, asked)
}

#[test]
fn a_0_2_call_hands_the_peer_bytes_and_returns_the_reply() {
    let (caller, asked) = recording(SyncAnswer::Ok(Some(body(Some("image/png"), NOT_UTF8))));
    let component = load_bytes_calling(None, 1024 * 1024, caller);
    let (_, _, report) = ok(component.handle(commands("call peer application/x-frame ff00")));
    assert_eq!(
        results(&report),
        [format!("ok:image/png:{}", hex(NOT_UTF8)).as_str()]
    );
    let asked = asked.lock().unwrap();
    assert_eq!(
        asked.as_slice(),
        [(
            "callee".to_string(),
            "answer".to_string(),
            body(Some("application/x-frame"), &[0xff, 0x00]),
        )]
    );
}

#[test]
fn a_0_2_call_reads_untyped_text_and_no_reply() {
    let (caller, _) = recording(SyncAnswer::Ok(Some(MessageBody::text("hi"))));
    let component = load_bytes_calling(None, 1024 * 1024, caller);
    let (_, _, report) = ok(component.handle(commands("call peer - 00")));
    assert_eq!(results(&report), ["ok:-:6869"]);
    let component = load_bytes(None, 1024 * 1024);
    let (_, _, report) = ok(component.handle(commands("call peer - 00")));
    assert_eq!(results(&report), ["ok:none"]);
}

#[test]
fn a_0_2_call_draws_the_body_refusals_and_text_only_peer() {
    let (caller, asked) = recording(SyncAnswer::Refused(SyncRefusal::TextOnlyTarget));
    let component = load_bytes_calling(None, 4096, caller);
    let (_, _, report) = ok(component.handle(commands(&format!(
        "call peer noslash 00\ncall peer - ff\ncall peer - {}\ncall peer image/png ff",
        hex(&[b'a'; 4097])
    ))));
    assert_eq!(
        results(&report),
        [
            "invalid-body",
            "invalid-body",
            "invalid-payload",
            "text-only-peer"
        ]
    );
    assert_eq!(
        asked.lock().unwrap().len(),
        1,
        "only the well-formed body under the cap reaches the seam"
    );
}

#[test]
fn a_malformed_body_on_an_undeclared_call_port_traps() {
    let component = load_bytes(None, 1024 * 1024);
    let outcome = component.handle(commands("call stranger noslash 00"));
    let ProcessorOutcome::Trap(why) = outcome else {
        panic!("expected a trap, got {outcome:?}");
    };
    assert!(why.contains("stranger"), "{why}");
}

/// A component built against a `brenn:processor@0.2.0` whose `bus.envelope`
/// is not the host's imports only names the host accepts, so the name check
/// passes it as a 0.2.0 component; the load refuses it on the type of its
/// `receive` export, before anything can run.
#[test]
fn a_component_built_against_a_changed_bus_is_refused_on_the_type() {
    let path = common::artifact_path("brenn_processor_stale_contract_test");
    let Err(refused) = std::panic::catch_unwind(|| {
        ProcessorComponent::load(ProcessorLoadSpec::minimal(&path, "stale"))
    }) else {
        panic!("the load must refuse the artifact");
    };
    let why = refused
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| refused.downcast_ref::<&str>().copied())
        .expect("the refusal is a message");
    assert!(
        why.contains("export type-check failed")
            && why.contains("type-checking export func `receive`"),
        "{why}"
    );
}

// The `brenn:processor@0.1.0` contract on the backend host, against the
// processor-page-test fixture: what a 0.1.0 component is handed, what each
// `ports@0.1.0` function's call becomes, how each refusal reaches it through
// the 0.1.0 spelling, and how its sync reply and its call are carried.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use brenn_activation::sync::SyncAnswer;
use brenn_activation::{Activation, DeferredEntry, DeferredWindow};
use brenn_envelope::MessageBody;
use brenn_wasm::{
    ComponentGrant, HostActivation, OutputAclFn, OutputPortSpec, ProcessorCallTarget,
    ProcessorComponent, ProcessorContract, ProcessorDeferredOp, ProcessorLoadSpec,
    ProcessorOutcome, ProcessorPublish, ProcessorUrgency, SinkBudget, SyncCallerFn,
};
use serde_json::Value;

mod common;

/// Every window port the fixture's specification declares as an input.
const WINDOW_PORTS: [&str; 5] = ["in", "feed", "memo", "state", "timer"];

/// How one test loads the fixture; [`Rig::default`] binds `out` and `report`
/// with generous budgets, permits every channel and answers every call with no
/// reply.
struct Rig {
    out: OutputPortSpec,
    output_acl: OutputAclFn,
    amplification_mt: u64,
    max_payload_bytes: usize,
    sync_caller: SyncCallerFn,
}

impl Default for Rig {
    fn default() -> Self {
        Rig {
            out: common::out_spec("brenn:out"),
            output_acl: Arc::new(|_| true),
            amplification_mt: 1_000,
            max_payload_bytes: 1024 * 1024,
            sync_caller: Arc::new(|_, _, _, _| SyncAnswer::Ok(None)),
        }
    }
}

impl Rig {
    fn load(self) -> ProcessorComponent {
        let component = ProcessorComponent::load(ProcessorLoadSpec {
            output_ports: HashMap::from([
                ("out".to_string(), self.out),
                ("report".to_string(), common::out_spec("brenn:report")),
            ]),
            declared_out_ports: ["memo", "out", "report", "state", "timer"]
                .into_iter()
                .map(String::from)
                .collect::<BTreeSet<_>>(),
            input_amplification_mt: WINDOW_PORTS
                .into_iter()
                .map(|p| (p.to_string(), self.amplification_mt))
                .collect(),
            grants: [ComponentGrant::Ports, ComponentGrant::Calls]
                .into_iter()
                .collect(),
            max_payload_bytes: self.max_payload_bytes,
            output_acl: self.output_acl,
            declared_call_ports: BTreeSet::from(["peer".to_string()]),
            calls: HashMap::from([(
                "peer".to_string(),
                ProcessorCallTarget {
                    target_slug: "callee".to_string(),
                    target_port: "answer".to_string(),
                },
            )]),
            sync_caller: Some(self.sync_caller),
            ..ProcessorLoadSpec::minimal(
                &common::artifact_path("brenn_processor_page_test"),
                "string-test",
            )
        });
        assert_eq!(component.contract(), ProcessorContract::V0_1);
        component
    }
}

/// An activation whose `in` window carries `commands` as one new text message.
fn commands(commands: &str) -> HostActivation {
    Activation {
        ports: vec![common::window(
            "in",
            vec![common::envelope(
                "brenn:in",
                "a0",
                MessageBody::text(commands),
            )],
            0,
        )],
        deferred: vec![],
        now: Some(1_000),
        sync: None,
    }
}

/// `activation` with one text message parked on `out` at index 0.
fn with_parked(mut activation: HostActivation) -> HostActivation {
    activation.deferred.push(DeferredWindow {
        port: "out".to_string(),
        entries: vec![DeferredEntry {
            index: 0,
            payload: common::envelope("brenn:out", "d0", MessageBody::text("parked")),
            deliver_after: 5_000,
        }],
    });
    activation
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
    assert_eq!(
        report.body.content_type(),
        None,
        "a 0.1.0 publish is untyped"
    );
    let report = serde_json::from_str(report.body.as_text().expect("the report is text"))
        .expect("the report is JSON");
    (publishes, deferred_ops, report)
}

#[test]
fn a_0_1_activation_hands_over_envelope_json_payload_text_and_the_clock() {
    let component = Rig::default().load();
    let feed = vec![
        common::envelope("brenn:feed", "b1", MessageBody::text("seen before")),
        common::envelope("brenn:feed", "b2", MessageBody::text("plain")),
        common::envelope(
            "brenn:feed",
            "b3",
            MessageBody::from_parts(Some("text/markdown"), b"# title").unwrap(),
        ),
    ];
    let mut activation = with_parked(commands("noop"));
    activation
        .ports
        .push(common::window("feed", feed.clone(), 1));
    let (publishes, ops, report) = ok(component.handle(activation));
    assert!(publishes.is_empty() && ops.is_empty());
    assert_eq!(common::results(&report), ["ok"]);

    let reported = &report["windows"][0];
    assert_eq!(reported["port"], "feed");
    assert_eq!(reported["new_from"], 1);
    assert_eq!(reported["dropped"], 0);
    let handed: Vec<String> = feed
        .iter()
        .map(|e| common::hex(e.to_envelope_json().as_bytes()))
        .collect();
    assert_eq!(
        reported["envelopes"],
        serde_json::json!(handed),
        "each envelope is the text-shape envelope JSON"
    );
    // The typed text reaches the 0.1.0 reader as its text, its type dropped.
    let typed: Value = serde_json::from_str(&feed[2].to_envelope_json()).unwrap();
    assert_eq!(typed["body"], "# title");
    assert!(typed.get("content_type").is_none(), "{typed}");

    let parked = &report["deferred"][0];
    assert_eq!(parked["port"], "out");
    assert_eq!(parked["entries"][0]["index"], 0);
    assert_eq!(parked["entries"][0]["hex"], common::hex(b"parked"));
    assert_eq!(parked["entries"][0]["deliver_after"], 5_000);
    assert_eq!(report["now"], 1_000);
    assert_eq!(report["sync"], Value::Null);
}

#[test]
fn an_activation_without_a_clock_reports_none() {
    let component = Rig::default().load();
    let mut activation = commands("noop");
    activation.now = None;
    let (_, _, report) = ok(component.handle(activation));
    assert_eq!(report["now"], Value::Null);
}

#[test]
fn publish_buffers_untyped_text_at_the_ports_default_urgency() {
    let component = Rig {
        out: OutputPortSpec {
            default_urgency: ProcessorUrgency::Low,
            ..common::out_spec("brenn:out")
        },
        ..Rig::default()
    }
    .load();
    let (publishes, _, report) = ok(component.handle(commands(&format!(
        "publish out {}\npublish out _",
        common::hex("héllo".as_bytes())
    ))));
    assert_eq!(common::results(&report), ["ok", "ok"]);
    assert_eq!(publishes.len(), 2);
    assert_eq!(publishes[0].channel_address, "brenn:out");
    assert_eq!(publishes[0].body, MessageBody::text("héllo"));
    assert_eq!(publishes[0].urgency, ProcessorUrgency::Low);
    assert_eq!(publishes[0].deliver_after, None);
    assert_eq!(publishes[1].body, MessageBody::text(""));
}

#[test]
fn publish_with_urgency_carries_each_level() {
    let component = Rig::default().load();
    let (publishes, _, report) = ok(component.handle(commands(
        "publish-urgent out very-low 61\n\
         publish-urgent out low 62\n\
         publish-urgent out normal 63\n\
         publish-urgent out high 64",
    )));
    assert_eq!(common::results(&report), ["ok", "ok", "ok", "ok"]);
    let got: Vec<(ProcessorUrgency, &str)> = publishes
        .iter()
        .map(|p| (p.urgency, p.body.as_text().unwrap()))
        .collect();
    assert_eq!(
        got,
        [
            (ProcessorUrgency::VeryLow, "a"),
            (ProcessorUrgency::Low, "b"),
            (ProcessorUrgency::Normal, "c"),
            (ProcessorUrgency::High, "d"),
        ]
    );
}

#[test]
fn publish_deferred_buffers_the_release_time() {
    let component = Rig::default().load();
    let (publishes, _, report) = ok(component.handle(commands("defer out 6c61746572 9000")));
    assert_eq!(common::results(&report), ["ok"]);
    assert_eq!(publishes.len(), 1);
    assert_eq!(publishes[0].body, MessageBody::text("later"));
    assert_eq!(publishes[0].deliver_after, Some(9_000));
}

#[test]
fn defer_cancel_buffers_the_cancel() {
    let component = Rig::default().load();
    let (_, ops, report) = ok(component.handle(with_parked(commands("cancel out 0"))));
    assert_eq!(common::results(&report), ["ok"]);
    assert_eq!(
        ops,
        [ProcessorDeferredOp::Cancel {
            port: "out".to_string(),
            index: 0,
        }]
    );
}

#[test]
fn defer_edit_changes_the_body_or_the_release_time_alone() {
    let component = Rig::default().load();
    let (_, ops, report) = ok(component.handle(with_parked(commands(
        "edit out 0 6e6577\nretime out 0 12000",
    ))));
    assert_eq!(common::results(&report), ["ok", "ok"]);
    assert_eq!(
        ops,
        [
            ProcessorDeferredOp::Edit {
                port: "out".to_string(),
                index: 0,
                body: Some(MessageBody::text("new")),
                deliver_after: None,
            },
            ProcessorDeferredOp::Edit {
                port: "out".to_string(),
                index: 0,
                body: None,
                deliver_after: Some(12_000),
            },
        ]
    );
}

#[test]
fn a_channel_outside_the_acl_is_not_permitted() {
    let component = Rig {
        output_acl: Arc::new(|channel| channel != "brenn:out"),
        ..Rig::default()
    }
    .load();
    let (publishes, _, report) = ok(component.handle(commands(
        "publish out 61\npublish-urgent out high 61\ndefer out 61 9000",
    )));
    assert_eq!(
        common::results(&report),
        ["not-permitted", "not-permitted", "not-permitted"]
    );
    assert!(publishes.is_empty());
}

#[test]
fn a_body_over_the_cap_and_an_unrepresentable_publish_time_are_invalid_payload() {
    let component = Rig {
        max_payload_bytes: 4096,
        ..Rig::default()
    }
    .load();
    let (publishes, _, report) = ok(component.handle(commands(&format!(
        "publish-fill out 4097\npublish-fill out 4096\ndefer out 61 {}",
        u64::MAX
    ))));
    assert_eq!(
        common::results(&report),
        ["invalid-payload", "ok", "invalid-payload"]
    );
    assert_eq!(publishes.len(), 1);
    assert_eq!(publishes[0].body.len(), 4096);
}

#[test]
fn an_exhausted_sink_is_quota_exceeded() {
    let component = Rig {
        out: OutputPortSpec {
            budget: SinkBudget {
                fill_mt: 0,
                capacity_mt: 0,
            },
            ..common::out_spec("brenn:out")
        },
        amplification_mt: 0,
        ..Rig::default()
    }
    .load();
    let (publishes, _, report) = ok(component.handle(commands("publish out 61")));
    assert_eq!(common::results(&report), ["quota-exceeded"]);
    assert!(publishes.is_empty());
}

#[test]
fn a_defer_op_past_the_window_is_out_of_range() {
    let component = Rig::default().load();
    let (_, ops, report) = ok(component.handle(with_parked(commands(
        "cancel out 1\nedit out 3 61\nretime timer 0 9000",
    ))));
    assert_eq!(
        common::results(&report),
        ["out-of-range", "out-of-range", "out-of-range"]
    );
    assert!(ops.is_empty());
}

#[test]
fn an_unrepresentable_edit_time_is_invalid_deliver_after() {
    let component = Rig::default().load();
    let (_, ops, report) =
        ok(component.handle(with_parked(commands(&format!("retime out 0 {}", u64::MAX)))));
    assert_eq!(common::results(&report), ["invalid-deliver-after"]);
    assert!(ops.is_empty());
}

#[test]
fn a_sync_activation_reports_the_request_and_replies_with_its_text() {
    let component = Rig::default().load();
    let request = common::envelope("brenn:ask", "c1", MessageBody::text("ask me"));
    let outcome = component.handle(Activation {
        ports: vec![common::window("ask", vec![Arc::clone(&request)], 0)],
        deferred: vec![],
        now: Some(1_000),
        sync: Some("ask".to_string()),
    });
    let ProcessorOutcome::Ok { reply, .. } = &outcome else {
        panic!("a sync activation answers ok, got {outcome:?}");
    };
    assert_eq!(reply.as_ref(), Some(&MessageBody::text("ask me")));
    let (_, _, report) = ok(outcome);
    assert_eq!(report["sync"], "ask");
    assert_eq!(report["windows"][0]["port"], "ask");
    assert_eq!(
        report["windows"][0]["envelopes"][0],
        common::hex(request.to_envelope_json().as_bytes())
    );
}

#[test]
fn a_0_1_call_hands_the_peer_text_and_reads_its_reply() {
    let asked: Arc<Mutex<Vec<(String, String, MessageBody)>>> = Arc::default();
    let record = Arc::clone(&asked);
    let component = Rig {
        sync_caller: Arc::new(move |slug, port, body, _chain| {
            record
                .lock()
                .unwrap()
                .push((slug.to_string(), port.to_string(), body));
            SyncAnswer::Ok(Some(
                MessageBody::from_parts(Some("text/markdown"), b"*").unwrap(),
            ))
        }),
        ..Rig::default()
    }
    .load();
    let (_, _, report) = ok(component.handle(commands("call peer 6869")));
    assert_eq!(
        common::results(&report),
        [format!("ok:{}", common::hex(b"*")).as_str()],
        "a typed text reply reaches the 0.1.0 caller as its text"
    );
    assert_eq!(
        asked.lock().unwrap().as_slice(),
        [(
            "callee".to_string(),
            "answer".to_string(),
            MessageBody::text("hi"),
        )]
    );
}

/// Two real 0.1.0 ends: the caller's request reaches the callee's sync
/// activation as the text it sent, and the callee's reply reaches the caller
/// as the text it replied.
#[test]
fn a_0_1_caller_and_a_0_1_callee_exchange_text() {
    let callee = Arc::new(Rig::default().load());
    let served = Arc::clone(&callee);
    let caller = Rig {
        sync_caller: Arc::new(move |slug, port, body, _chain| {
            assert_eq!((slug, port), ("callee", "answer"));
            let outcome = served.handle(Activation {
                ports: vec![common::window(
                    "ask",
                    vec![common::envelope("brenn:ask", "e1", body)],
                    0,
                )],
                deferred: vec![],
                now: Some(2_000),
                sync: Some("ask".to_string()),
            });
            let ProcessorOutcome::Ok { reply, .. } = outcome else {
                panic!("the callee answers ok, got {outcome:?}");
            };
            SyncAnswer::Ok(reply)
        }),
        ..Rig::default()
    }
    .load();
    let (_, _, report) = ok(caller.handle(commands(&format!(
        "call peer {}",
        common::hex("ping ✓".as_bytes())
    ))));
    assert_eq!(
        common::results(&report),
        [format!("ok:{}", common::hex("ping ✓".as_bytes())).as_str()]
    );
}

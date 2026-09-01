// The transplant test, wasmtime half.
//
// One artifact — `brenn_processor_transplant.wasm` — driven through the
// scripted activation sequence in the fixture's `transplant.json` and reduced
// to that file's canonical transcript.
//
// The surface half — driving the *same* artifact, transpiled, through the *same*
// script and asserting transcript equality — lives in
// `frontend/src/processor-transplant.test.ts`. Equality of the two transcripts is
// the executable form of the invariant: any component runs on any host that can
// satisfy its imports, and the component cannot tell which host it got. Both
// halves read `transplant.json`, so a change to the script or its expected
// transcript is answered by both hosts or by neither.
//
// Wire class: the script is `brenn:`-bound throughout. That is an owner scoping
// decision, not doctrine — backend WASM consumers cannot bind `ephemeral:`
// channels yet (a registry fork, never a decision), and closing that gap is its
// own design and implementation effort. The `ephemeral:`-bound variant of this
// fixture is that effort's standing obligation and extends this criterion with
// no further ratification. Nothing in the surface half is class-aware, so the
// deferral costs the criterion nothing beyond coverage of the backend hosting.

mod common;

use brenn_wasm::{
    ComponentGrant, ProcessorActivation, ProcessorComponent, ProcessorDeferredEntry,
    ProcessorDeferredOp, ProcessorDeferredWindow, ProcessorLoadSpec, ProcessorOutcome,
    ProcessorPortWindow, store::DEFAULT_MAX_PAGE_COUNT,
};
use std::collections::HashMap;

const OUT_CHANNEL: &str = "brenn:transplant-out";

fn script() -> serde_json::Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("components/processor-transplant/transplant.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read transplant script {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("transplant script is valid JSON")
}

fn artifact() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/components/brenn_processor_transplant.wasm")
}

/// Expand an `(id, body)` pair into the canonical `MessageEnvelope` JSON, using
/// the script's fixed non-identifying fields. The surface half performs the
/// identical expansion — the script names the identity, both harnesses supply
/// the same frame around it.
fn envelope(template: &serde_json::Value, pair: &serde_json::Value) -> String {
    let mut env = template.clone();
    let obj = env.as_object_mut().expect("envelope_template is an object");
    obj.insert("message_id".to_string(), pair["id"].clone());
    obj.insert("body".to_string(), pair["body"].clone());
    serde_json::to_string(&env).expect("envelope serializes")
}

fn activation(template: &serde_json::Value, spec: &serde_json::Value) -> ProcessorActivation {
    let ports = spec["ports"]
        .as_array()
        .expect("activation has a ports array")
        .iter()
        .map(|port| ProcessorPortWindow {
            port: port["port"].as_str().expect("port name").to_string(),
            envelopes: port["envelopes"]
                .as_array()
                .expect("envelopes array")
                .iter()
                .map(|pair| envelope(template, pair))
                .collect(),
            new_from: port["new_from"].as_u64().expect("new_from") as u32,
            dropped: port["dropped"].as_u64().expect("dropped"),
        })
        .collect();
    let deferred = spec["deferred"]
        .as_array()
        .expect("activation has a deferred array")
        .iter()
        .map(|window| ProcessorDeferredWindow {
            port: window["port"]
                .as_str()
                .expect("deferred window port")
                .to_string(),
            entries: window["entries"]
                .as_array()
                .expect("deferred entries array")
                .iter()
                .map(|entry| ProcessorDeferredEntry {
                    index: entry["index"].as_u64().expect("entry index") as u32,
                    payload: entry["payload"]
                        .as_str()
                        .expect("entry payload")
                        .to_string(),
                    deliver_after: entry["deliver_after"]
                        .as_u64()
                        .expect("entry deliver_after"),
                })
                .collect(),
        })
        .collect();
    // Named on every activation, null for a host with no UTC wall clock — absent
    // and null must not be the same thing to a script reader.
    let now = match spec
        .get("now")
        .expect("activation names now (null for a clockless host)")
    {
        serde_json::Value::Null => None,
        value => Some(value.as_u64().expect("now is epoch milliseconds")),
    };
    ProcessorActivation {
        ports,
        deferred,
        now,
        sync: None,
    }
}

/// Reduce one buffered control op to its canonical transcript form. An object
/// rather than a joined string: an edit's body is guest-chosen text that may
/// contain any separator, and `null` distinguishes "leave this half alone" from
/// "set it to the empty string".
fn op_entry(op: &ProcessorDeferredOp) -> serde_json::Value {
    match op {
        ProcessorDeferredOp::Cancel { port, index } => {
            serde_json::json!({ "op": "cancel", "port": port, "index": index })
        }
        ProcessorDeferredOp::Edit {
            port,
            index,
            payload,
            deliver_after,
        } => serde_json::json!({
            "op": "edit",
            "port": port,
            "index": index,
            "payload": payload,
            "deliver_after": deliver_after,
        }),
    }
}

/// Reduce one activation's outcome to the canonical transcript entry: the flush
/// outcome plus, in call order, the immediate publishes, the deferred publishes,
/// and the control ops that actually reached the host.
///
/// This host carries immediate and deferred publishes in one buffer,
/// discriminated by `deliver_after`; the transcript splits them so the release
/// instant a deferred publish carries is asserted and an immediate publish that
/// silently acquired one cannot hide.
fn transcript_entry(outcome: ProcessorOutcome) -> serde_json::Value {
    match outcome {
        ProcessorOutcome::Ok {
            publishes,
            deferred_ops,
        } => {
            let mut immediate: Vec<serde_json::Value> = Vec::new();
            let mut deferred: Vec<serde_json::Value> = Vec::new();
            for p in &publishes {
                assert_eq!(
                    p.channel_address, OUT_CHANNEL,
                    "the fixture publishes only to its one bound output port"
                );
                match p.deliver_after {
                    None => immediate.push(serde_json::Value::String(p.payload.clone())),
                    Some(when) => deferred
                        .push(serde_json::json!({ "body": p.payload, "deliver_after": when })),
                }
            }
            let ops: Vec<serde_json::Value> = deferred_ops.iter().map(op_entry).collect();
            serde_json::json!({
                "outcome": "ok",
                "publishes": immediate,
                "deferred_publishes": deferred,
                "ops": ops,
            })
        }
        // Err and trap both discard the buffer, so neither carries publishes,
        // schedules, or ops. The distinction that survives into the transcript is
        // the outcome tag; the terminal consequence of a trap is the host's, not
        // the guest's.
        ProcessorOutcome::Err(_) => serde_json::json!({
            "outcome": "err", "publishes": [], "deferred_publishes": [], "ops": [],
        }),
        ProcessorOutcome::Trap(_) => serde_json::json!({
            "outcome": "trap", "publishes": [], "deferred_publishes": [], "ops": [],
        }),
    }
}

fn load(config: HashMap<String, String>) -> ProcessorComponent {
    let mut output_ports = HashMap::new();
    output_ports.insert("out".to_string(), common::out_spec(OUT_CHANNEL));
    // The transpilable profile, exactly: ports + log + config. No store, mqtt,
    // or tools — importing any of those would make the artifact backend-only
    // and its surface declaration a boot panic.
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &artifact(),
        slug: "transplant",
        declared_out_ports: output_ports.keys().cloned().collect(),
        output_ports,
        input_amplification_mt: HashMap::from([
            ("in".to_string(), 1000u64),
            ("ctx".to_string(), 1000u64),
        ]),
        mqtt_sinks: HashMap::new(),
        config,
        grants: [
            ComponentGrant::Ports,
            ComponentGrant::Log,
            ComponentGrant::Config,
        ]
        .into_iter()
        .collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: common::noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

/// Drive the whole script against one instance and return the transcript.
fn run_script(script: &serde_json::Value) -> Vec<serde_json::Value> {
    let config: HashMap<String, String> = script["config"]
        .as_object()
        .expect("config map")
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.as_str().expect("config value is a string").to_string(),
            )
        })
        .collect();
    let component = load(config);
    let template = &script["envelope_template"];

    script["activations"]
        .as_array()
        .expect("activations array")
        .iter()
        .map(|spec| transcript_entry(component.handle(activation(template, spec))))
        .collect()
}

#[test]
fn transplant_script_produces_the_canonical_transcript() {
    let script = script();
    let actual = run_script(&script);
    let expected = script["transcript"].as_array().expect("transcript array");
    assert_eq!(
        &actual, expected,
        "the wasmtime hosting's transcript must equal the canonical one \
         (regenerate deliberately, never to make this pass)"
    );
}

#[test]
fn instance_survives_err_and_dies_on_trap() {
    // The transcript's shape is itself the contract for err vs trap: an err
    // activation flushes nothing yet is followed by an ok activation, and the
    // trap activation flushes nothing and is last. Asserted separately from the
    // equality above so a regenerated transcript cannot quietly lose it.
    // Properties, not the literal sequence: extending the fixture with a
    // legitimate activation must not break this pin, or the cheapest repair is to
    // paste the new sequence back in — which re-derives the assertion from the
    // fixture and dissolves the independence. Same pin, same rationale, as the
    // surface half's.
    let script = script();
    let transcript = run_script(&script);
    let outcomes: Vec<&str> = transcript
        .iter()
        .map(|e| e["outcome"].as_str().unwrap())
        .collect();
    let err_at = outcomes
        .iter()
        .position(|o| *o == "err")
        .expect("the script exercises the err sentinel");
    assert_eq!(
        outcomes.iter().position(|o| *o == "trap"),
        Some(outcomes.len() - 1),
        "the trap activation is terminal, so it is last and it is the only one"
    );
    assert!(
        outcomes[err_at + 1..].contains(&"ok"),
        "the instance must keep delivering after an err"
    );
    for (i, entry) in transcript.iter().enumerate() {
        // Every buffered class is discarded together: publishes, deferred
        // schedules, and control ops.
        let flushed: usize = ["publishes", "deferred_publishes", "ops"]
            .iter()
            .map(|key| entry[*key].as_array().unwrap().len())
            .sum();
        if outcomes[i] == "ok" {
            assert!(flushed > 0, "ok activation {i} flushed nothing");
        } else {
            assert_eq!(
                flushed, 0,
                "activation {i} was {} yet flushed {flushed} buffered items; \
                 the buffer must be discarded",
                outcomes[i],
            );
        }
    }
}

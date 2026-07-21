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
    Capability, ProcessorActivation, ProcessorComponent, ProcessorLoadSpec, ProcessorOutcome,
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
    ProcessorActivation { ports }
}

/// Reduce one activation's outcome to the canonical transcript entry: the flush
/// outcome plus, in call order, the publishes that actually reached the sink.
fn transcript_entry(outcome: ProcessorOutcome) -> serde_json::Value {
    match outcome {
        ProcessorOutcome::Ok(publishes) => {
            let payloads: Vec<serde_json::Value> = publishes
                .iter()
                .map(|p| {
                    assert_eq!(
                        p.channel_address, OUT_CHANNEL,
                        "the fixture publishes only to its one bound output port"
                    );
                    serde_json::Value::String(p.payload.clone())
                })
                .collect();
            serde_json::json!({ "outcome": "ok", "publishes": payloads })
        }
        // Err and trap both discard the buffer, so neither carries publishes.
        // The distinction that survives into the transcript is the outcome tag;
        // the terminal consequence of a trap is the host's, not the guest's.
        ProcessorOutcome::Err(_) => serde_json::json!({ "outcome": "err", "publishes": [] }),
        ProcessorOutcome::Trap(_) => serde_json::json!({ "outcome": "trap", "publishes": [] }),
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
        output_ports,
        input_amplification_mt: HashMap::from([
            ("in".to_string(), 1000u64),
            ("ctx".to_string(), 1000u64),
        ]),
        mqtt_sinks: HashMap::new(),
        config,
        grants: [Capability::Ports, Capability::Log, Capability::Config]
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
    // The transcript's shape is itself the contract for err vs trap: the err
    // activation flushes nothing yet is followed by an ok activation, and the
    // trap activation flushes nothing and is last. Asserted separately from
    // the equality above so a regenerated transcript cannot quietly lose it.
    let script = script();
    let transcript = run_script(&script);
    let outcomes: Vec<&str> = transcript
        .iter()
        .map(|e| e["outcome"].as_str().unwrap())
        .collect();
    assert_eq!(outcomes, ["ok", "ok", "err", "ok", "trap"]);
    for (i, entry) in transcript.iter().enumerate() {
        let publishes = entry["publishes"].as_array().unwrap();
        if outcomes[i] == "ok" {
            assert!(!publishes.is_empty(), "ok activation {i} flushed nothing");
        } else {
            assert!(
                publishes.is_empty(),
                "activation {i} was {} yet flushed {} publishes; \
                 the buffer must be discarded",
                outcomes[i],
                publishes.len()
            );
        }
    }
}

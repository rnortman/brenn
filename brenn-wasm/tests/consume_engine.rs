// Integration tests for `ProcessorComponent` against the processor-demo and
// processor-exhaust fixtures.
//
// All tests load the WASM fixtures via `ProcessorComponent::load` and drive
// `handle` directly, including resource limits and the fuel/memory exhaustion
// bound.

use std::collections::HashMap;
use std::sync::Arc;

use brenn_wasm::{
    ComponentGrant, PROCESSOR_FUEL_MINIMUM, PROCESSOR_FUEL_PER_ENVELOPE, PROCESSOR_MAX_INSTANCES,
    PROCESSOR_MAX_MEMORIES, PROCESSOR_MAX_MEMORY_BYTES, PROCESSOR_MAX_TABLE_ELEMENTS,
    PROCESSOR_MAX_TABLES, ProcessorActivation, ProcessorComponent, ProcessorLoadSpec,
    ProcessorOutcome, ProcessorPortWindow, ProcessorUrgency, store::DEFAULT_MAX_PAGE_COUNT,
};
use tracing_test::traced_test;

mod common;

fn noop_alerter() -> Arc<dyn brenn_wasm::ProcessorAlerter> {
    common::noop_alerter()
}

/// Path to a built WASM fixture component by artifact basename (without `.wasm`).
/// E.g. `component_path("brenn_processor_demo")`.
fn component_path(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/components")
        .join(format!("{name}.wasm"))
}

/// Load a fixture component with no output ports. Panics on load failure (correct
/// for tests: a broken fixture is a test-infrastructure bug, not a runtime error).
fn load_component(name: &str, slug: &str) -> ProcessorComponent {
    // exhaust / mem_exhaust import only `types` — no capability grants needed.
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path(name),
        slug,
        declared_out_ports: std::collections::BTreeSet::new(),
        output_ports: HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants: std::collections::BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

fn load_dual() -> ProcessorComponent {
    let mut ports = HashMap::new();
    ports.insert("out1".to_string(), common::out_spec("brenn:channel-out1"));
    ports.insert("out2".to_string(), common::out_spec("brenn:channel-out2"));
    // processor-dual imports: types + ports
    let grants = [ComponentGrant::Ports].into_iter().collect();
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_dual"),
        slug: "dual",
        declared_out_ports: ports.keys().cloned().collect(),
        output_ports: ports,
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants,
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

fn load_demo(output_ports: HashMap<String, brenn_wasm::OutputPortSpec>) -> ProcessorComponent {
    // processor-demo imports: types + ports
    let grants = [ComponentGrant::Ports].into_iter().collect();
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_demo"),
        slug: "demo",
        declared_out_ports: output_ports.keys().cloned().collect(),
        output_ports,
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants,
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

fn load_demo_with_out() -> ProcessorComponent {
    let mut ports = HashMap::new();
    ports.insert("out".to_string(), common::out_spec("brenn:test-out"));
    load_demo(ports)
}

/// The demo publishing to `"out"`, which the component declares and the deployer
/// did not wire.
fn load_demo_declaring_out_unwired() -> ProcessorComponent {
    let grants = [ComponentGrant::Ports].into_iter().collect();
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_demo"),
        slug: "demo",
        declared_out_ports: ["out".to_string()].into_iter().collect(),
        output_ports: HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants,
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

fn load_demo_no_ports() -> ProcessorComponent {
    load_demo(HashMap::new())
}

fn load_multiport() -> ProcessorComponent {
    let mut ports = HashMap::new();
    ports.insert("out".to_string(), common::out_spec("brenn:multiport-out"));
    // processor-multiport imports: types + ports
    let grants = [ComponentGrant::Ports].into_iter().collect();
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_multiport"),
        slug: "multiport",
        declared_out_ports: ports.keys().cloned().collect(),
        output_ports: ports,
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants,
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

fn load_exhaust() -> ProcessorComponent {
    load_component("brenn_processor_exhaust", "exhaust")
}

fn load_mem_exhaust() -> ProcessorComponent {
    load_component("brenn_processor_mem_exhaust", "mem-exhaust")
}

/// Load a component that needs the `Store` grant (e.g. processor-store-rt),
/// with its store opened — the pair a running consumer is.
fn load_store_component(name: &str, slug: &str) -> (ProcessorComponent, tempfile::NamedTempFile) {
    let db = tempfile::NamedTempFile::new().unwrap();
    let comp = load_store_component_at(name, slug, db.path());
    // The store is opened at the start rather than at the load, and this
    // fixture is both.
    comp.open_store();
    (comp, db)
}

/// The same load without the open: what a host does when it instantiates a
/// replacement for a consumer that is still running.
fn load_store_component_at(name: &str, slug: &str, store: &std::path::Path) -> ProcessorComponent {
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path(name),
        slug,
        declared_out_ports: std::collections::BTreeSet::new(),
        output_ports: HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants: [ComponentGrant::Store].into_iter().collect(),
        store_path: Some(store),
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

/// Loading does not take the store file. A config reload instantiates a changed
/// consumer's replacement *before* it retires the running one, and a store file
/// admits exactly one holder — so if the load opened it, every reload of a
/// store-bearing consumer would be refused for a conflict the operator did not
/// cause.
#[test]
fn a_replacement_for_a_running_consumer_loads_against_a_held_store() {
    let db = tempfile::NamedTempFile::new().unwrap();
    let running = load_store_component_at("brenn_processor_store_rt", "store-rt", db.path());
    running.open_store();

    let replacement = load_store_component_at("brenn_processor_store_rt", "store-rt", db.path());

    // Only the open contends, and only while the old instance is alive.
    drop(running);
    replacement.open_store();
}

/// The other half: two instances holding one store file at once is a host
/// wiring error, and the guard that says so is still there.
#[test]
#[should_panic(expected = "KvStore path already open")]
fn two_open_stores_on_one_file_is_refused() {
    let db = tempfile::NamedTempFile::new().unwrap();
    let running = load_store_component_at("brenn_processor_store_rt", "store-rt", db.path());
    running.open_store();
    let replacement = load_store_component_at("brenn_processor_store_rt", "store-rt", db.path());
    replacement.open_store();
}

/// Minimal valid MessageEnvelope JSON for the demo component.
fn envelope_json(channel: &str, body: &str) -> String {
    format!(
        r#"{{"message_id":"00000000-0000-0000-0000-000000000001","source":"test","channel":"{channel}","sender":"test-sender","publish_ts":"2026-01-01T00:00:00Z","body":"{body}","urgency":"normal","envelope_type":"brenn"}}"#
    )
}

fn single_port_activation(
    port: &str,
    envelopes: Vec<String>,
    new_from: u32,
) -> ProcessorActivation {
    ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: port.to_string(),
            envelopes,
            new_from,
            dropped: 0,
        }],
        deferred: vec![],
        now: None,
        sync: None,
    }
}

// ── load ──────────────────────────────────────────────────────────────────────

#[test]
fn load_valid_component_succeeds() {
    let _comp = load_demo_no_ports();
}

#[test]
#[should_panic]
fn load_missing_path_panics() {
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: std::path::Path::new("/nonexistent/processor.wasm"),
        slug: "test",
        declared_out_ports: std::collections::BTreeSet::new(),
        output_ports: HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants: std::collections::BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
}

// ── Ok path ───────────────────────────────────────────────────────────────────

#[test]
fn handle_ok_brenn_envelope_returns_ok_no_publish() {
    // Non-webhook envelopes: demo does not call publish → Ok, empty buffer.
    let comp = load_demo_with_out();
    let activation = single_port_activation("in", vec![envelope_json("brenn:test", "hello")], 0);
    assert!(
        matches!(comp.handle(activation), ProcessorOutcome::Ok { publishes, .. } if publishes.is_empty())
    );
}

#[test]
fn handle_ok_with_context_prefix() {
    let comp = load_demo_with_out();
    let activation = single_port_activation(
        "in",
        vec![
            envelope_json("brenn:test", "ctx-1"),
            envelope_json("brenn:test", "ctx-2"),
            envelope_json("brenn:test", "new-1"),
        ],
        2, // new_from=2: first two are context
    );
    assert!(matches!(
        comp.handle(activation),
        ProcessorOutcome::Ok { .. }
    ));
}

#[test]
fn handle_no_new_envelopes_does_not_fail() {
    // new_from == envelopes.len() — nothing new; guest iterates empty slice → Ok.
    let comp = load_demo_with_out();
    let activation = single_port_activation(
        "in",
        vec![envelope_json("brenn:test", "ctx-only")],
        1, // new_from == len → no new
    );
    assert!(matches!(
        comp.handle(activation),
        ProcessorOutcome::Ok { .. }
    ));
}

#[test]
#[should_panic(expected = "cannot be lowered into the processor world")]
fn handle_panics_on_a_sync_call_activation() {
    // The processor world has no sync vocabulary, so a sync-call activation
    // reaching this host is a caller error, not a shape to silently degrade
    // into an async one.
    let comp = load_demo_with_out();
    let mut activation =
        single_port_activation("in", vec![envelope_json("brenn:test", "hello")], 0);
    activation.sync = Some("press".to_string());
    comp.handle(activation);
}

// ── Webhook publish path ──────────────────────────────────────────────────────

fn webhook_envelope(inner_body: &str) -> String {
    // body field is a JSON string containing the WebhookEnvelope JSON.
    // The outer envelope is envelope_type="webhook" with body = JSON(WebhookEnvelope).
    let inner_json = serde_json::json!({
        "headers": [],
        "key_id": "test-key",
        "client_ip": "127.0.0.1",
        "received_at": "2026-01-01T00:00:00Z",
        "body": inner_body,
        "endpoint_slug": "test-endpoint"
    })
    .to_string();
    // Escape the inner JSON for embedding in the outer envelope body string.
    let escaped = serde_json::to_string(&inner_json).unwrap();
    format!(
        r#"{{"message_id":"00000000-0000-0000-0000-000000000002","source":"test","channel":"webhook:test","sender":"test-sender","publish_ts":"2026-01-01T00:00:00Z","body":{escaped},"urgency":"normal","envelope_type":"webhook"}}"#
    )
}

#[test]
fn handle_webhook_envelope_publishes_inner_body() {
    let comp = load_demo_with_out();
    let activation = single_port_activation("in", vec![webhook_envelope("hello-from-webhook")], 0);
    match comp.handle(activation) {
        ProcessorOutcome::Ok { publishes, .. } => {
            assert_eq!(publishes.len(), 1);
            assert_eq!(publishes[0].port, "out");
            assert_eq!(publishes[0].channel_address, "brenn:test-out");
            assert_eq!(publishes[0].payload, "hello-from-webhook");
        }
        other => panic!("expected Ok with one publish, got {other:?}"),
    }
}

/// A bound output port outside the declared vocabulary would make the three-way
/// rule incoherent — the port would deliver and simultaneously be a contract
/// violation. Config resolution refuses it; the loader refuses it again, because
/// a hand-built load spec never passes through that refusal.
#[test]
#[should_panic(expected = "is bound but is not in the component's declared port vocabulary")]
fn loading_a_bound_port_outside_the_declared_vocabulary_panics() {
    let mut ports = HashMap::new();
    ports.insert("out".to_string(), common::out_spec("brenn:test-out"));
    let _ = ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_demo"),
        slug: "demo-undeclared-binding",
        declared_out_ports: std::collections::BTreeSet::new(),
        output_ports: ports,
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
}

// ── the three-way publish rule, end to end ───────────────────────────────────

/// The demo declares `"out"` and the deployer left it unwired: the publish
/// succeeds, the guest returns ok, and no message reaches the host. The
/// component cannot tell this activation from one whose channel had no
/// subscribers.
#[test]
fn handle_webhook_declared_unwired_port_succeeds_and_delivers_nothing() {
    let comp = load_demo_declaring_out_unwired();
    let activation = single_port_activation("in", vec![webhook_envelope("some-payload")], 0);
    match comp.handle(activation) {
        ProcessorOutcome::Ok { publishes, .. } => {
            assert!(
                publishes.is_empty(),
                "a publish to an unwired port delivers nothing, got {publishes:?}"
            );
        }
        other => panic!("expected Ok with no publish, got {other:?}"),
    }
}

/// The demo publishes to `"out"` while declaring no ports at all — the artifact
/// contradicting its own hash-bound specification. The activation traps, and the
/// diagnostic names the component, the port and the reason.
#[test]
fn handle_webhook_undeclared_port_traps_the_activation() {
    let comp = load_demo_no_ports();
    let activation = single_port_activation("in", vec![webhook_envelope("some-payload")], 0);
    match comp.handle(activation) {
        ProcessorOutcome::Trap(diagnostic) => {
            assert!(
                diagnostic.contains("demo")
                    && diagnostic.contains("out")
                    && diagnostic.contains("not declared"),
                "the trap diagnostic must be attributable, got {diagnostic:?}"
            );
        }
        other => panic!("expected a trap for an undeclared port, got {other:?}"),
    }
}

// ── output-ACL deny / allow (end-to-end through handle) ───────────────────────

/// Load the demo component with `out` bound to `brenn:secret` and a caller-supplied
/// output ACL, so the publish-ACL gate can be driven through `handle()`.
fn load_demo_out_with_acl(channel: &str, acl: brenn_wasm::OutputAclFn) -> ProcessorComponent {
    let mut ports = HashMap::new();
    ports.insert("out".to_string(), common::out_spec(channel));
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_demo"),
        slug: "demo-acl",
        declared_out_ports: ports.keys().cloned().collect(),
        output_ports: ports,
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: acl,
        mqtt_publish: None,
        tool_host: None,
    })
}

/// Bound port whose channel is outside the output ACL: `do_publish` returns
/// `NotPermitted`, the demo maps it to `ProcessingFailed`, and nothing is buffered.
/// The guest-visible result is identical to a bound port whose channel is denied;
/// wiring itself is never observable.
#[test]
fn handle_webhook_bound_port_outside_output_acl_returns_processing_failed() {
    // Deny exactly the bound channel.
    let acl: brenn_wasm::OutputAclFn = Arc::new(|addr: &str| addr != "brenn:secret");
    let comp = load_demo_out_with_acl("brenn:secret", acl);
    let activation = single_port_activation("in", vec![webhook_envelope("denied-payload")], 0);
    // `ProcessorOutcome::Err` carries no publish list, so an Err outcome already
    // proves nothing reached the host's publish buffer (only `Ok(publishes)`
    // carries buffered messages). The buffer-empty / zero-bytes invariant at the
    // `do_publish` level is pinned directly by the unit test
    // `do_publish_acl_deny_returns_not_permitted_and_buffers_nothing` in `lib.rs`.
    assert!(
        matches!(comp.handle(activation), ProcessorOutcome::Err(_)),
        "a bound port whose channel is outside the output ACL must surface as Err(processing-failed) with no publish"
    );
}

/// Positive pair: a bound port whose channel the ACL permits publishes normally.
/// Proves the gate does not reject an in-allowlist channel.
#[test]
fn handle_webhook_bound_port_inside_output_acl_publishes() {
    // Allow exactly the bound channel.
    let acl: brenn_wasm::OutputAclFn = Arc::new(|addr: &str| addr == "brenn:secret");
    let comp = load_demo_out_with_acl("brenn:secret", acl);
    let activation = single_port_activation("in", vec![webhook_envelope("allowed-payload")], 0);
    match comp.handle(activation) {
        ProcessorOutcome::Ok { publishes, .. } => {
            assert_eq!(
                publishes.len(),
                1,
                "in-ACL channel must publish exactly once"
            );
            assert_eq!(publishes[0].channel_address, "brenn:secret");
            assert_eq!(publishes[0].payload, "allowed-payload");
        }
        other => panic!("expected Ok with one publish for in-ACL channel, got {other:?}"),
    }
}

// ── invalid-payload publish ───────────────────────────────────────────────────

#[test]
fn handle_oversized_payload_returns_processing_failed() {
    let mut ports = HashMap::new();
    ports.insert("out".to_string(), common::out_spec("brenn:test-out"));
    // Set tiny max_payload_bytes so webhook body triggers invalid-payload.
    // processor-demo imports: types + ports
    let comp = ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_demo"),
        slug: "demo-tiny",
        declared_out_ports: ports.keys().cloned().collect(),
        output_ports: ports,
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1, // 1 byte cap
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
    let activation = single_port_activation(
        "in",
        vec![webhook_envelope("ab")], // 2 bytes, over cap
        0,
    );
    assert!(
        matches!(comp.handle(activation), ProcessorOutcome::Err(_)),
        "oversize payload must surface as Err(processing-failed)"
    );
}

// ── Err paths ─────────────────────────────────────────────────────────────────

#[test]
fn handle_malformed_json_returns_err() {
    let comp = load_demo_with_out();
    let activation = single_port_activation("in", vec!["not valid json".into()], 0);
    assert!(matches!(comp.handle(activation), ProcessorOutcome::Err(_)));
}

#[test]
fn handle_missing_channel_field_returns_processing_failed() {
    let comp = load_demo_with_out();
    // Valid JSON but no `channel` field → ProcessingFailed.
    let activation = single_port_activation(
        "in",
        vec![r#"{"message_id":"abc","body":"hello"}"#.into()],
        0,
    );
    assert!(matches!(comp.handle(activation), ProcessorOutcome::Err(_)));
}

// ── Trap path ─────────────────────────────────────────────────────────────────

#[test]
fn handle_sentinel_body_traps_not_panics() {
    // The demo component traps on body == "__trap__".
    // Verifies that ProcessorComponent::handle returns Trap (not panic).
    let comp = load_demo_with_out();
    let activation = single_port_activation("in", vec![envelope_json("brenn:test", "__trap__")], 0);
    assert!(matches!(comp.handle(activation), ProcessorOutcome::Trap(_)));
}

#[test]
fn trap_discards_publish_buffer() {
    // Activation: first envelope is webhook (publishes), second is sentinel (traps).
    // Buffer must be discarded — no publishes returned.
    let comp = load_demo_with_out();
    let activation = single_port_activation(
        "in",
        vec![
            webhook_envelope("first"),
            envelope_json("brenn:test", "__trap__"),
        ],
        0,
    );
    assert!(
        matches!(comp.handle(activation), ProcessorOutcome::Trap(_)),
        "sentinel after publish must trap and discard the buffer"
    );
}

// ── Multi-port publish ────────────────────────────────────────────────────────

#[test]
fn two_webhook_envelopes_produce_two_buffered_publishes() {
    // Two webhook envelopes in one activation → two buffered publishes, each
    // with the correct resolved channel address and payload.
    let mut ports = HashMap::new();
    ports.insert("out".to_string(), common::out_spec("brenn:channel-a"));
    // processor-demo imports: types + ports
    let comp = ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_demo"),
        slug: "demo-multi",
        declared_out_ports: ports.keys().cloned().collect(),
        output_ports: ports,
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
    // Two webhook envelopes → two publishes, both resolved to "brenn:channel-a".
    let activation = single_port_activation(
        "in",
        vec![webhook_envelope("body-1"), webhook_envelope("body-2")],
        0,
    );
    match comp.handle(activation) {
        ProcessorOutcome::Ok { publishes, .. } => {
            assert_eq!(publishes.len(), 2, "expected 2 publishes");
            assert_eq!(publishes[0].channel_address, "brenn:channel-a");
            assert_eq!(publishes[0].payload, "body-1");
            assert_eq!(publishes[1].channel_address, "brenn:channel-a");
            assert_eq!(publishes[1].payload, "body-2");
        }
        other => panic!("expected Ok with 2 publishes, got {other:?}"),
    }
}

// ── per-sink publish token buckets, end-to-end through handle (design §2.3,
//    §3.4, §6) ────────────────────────────────────────────────────────────────

/// Load the demo with `out` bound and explicit per-sink budget / input
/// amplification (millitokens), so the token-bucket seeding, enforcement, and
/// carryover glue can be driven through `handle()`.
fn load_demo_budgeted(
    out_addr: &str,
    amp_mt: u64,
    fill_mt: u64,
    capacity_mt: u64,
) -> ProcessorComponent {
    let mut ports = HashMap::new();
    ports.insert(
        "out".to_string(),
        brenn_wasm::OutputPortSpec {
            channel_address: out_addr.to_string(),
            default_urgency: ProcessorUrgency::Normal,
            budget: brenn_wasm::SinkBudget {
                fill_mt,
                capacity_mt,
            },
        },
    );
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path("brenn_processor_demo"),
        slug: "demo-budget",
        declared_out_ports: ports.keys().cloned().collect(),
        output_ports: ports,
        input_amplification_mt: HashMap::from([("in".to_string(), amp_mt)]),
        mqtt_sinks: HashMap::new(),
        grants: [ComponentGrant::Ports].into_iter().collect(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        config: HashMap::new(),
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

/// A zero budget (fill 0, amplification 0) rejects the single publish: the demo's
/// `publish` returns `quota-exceeded`, which it maps to `ProcessingFailed`.
#[test]
fn per_sink_budget_zero_rejects_publish() {
    let comp = load_demo_budgeted("brenn:test-out", 0, 0, 0);
    let activation = single_port_activation("in", vec![webhook_envelope("body")], 0);
    assert!(
        matches!(comp.handle(activation), ProcessorOutcome::Err(_)),
        "a zero per-sink budget must reject the publish and surface as Err"
    );
}

/// Fractional amplification accumulates across activations via carryover: at
/// amplification 0.5 (500 mt) with fill 0, one new envelope grants only 500 mt —
/// below the 1000 mt per publish — so activation 1 is rejected (Err), but the
/// unconsumed 500 mt carries over, so activation 2 (another +500) reaches 1000
/// and publishes (Ok). Exercises unconsumed-budget seeding and writeback
/// end-to-end; the capacity clamp is covered by
/// `per_sink_capacity_clamp_bounds_carryover` and `brenn-budget`'s
/// `seed_clamps_carry_then_adds`.
#[test]
fn per_sink_carryover_accumulates_fractional_amplification() {
    // capacity generous so the 500 mt carry is not clamped away.
    let comp = load_demo_budgeted("brenn:test-out", 500, 0, 1_000_000);
    let a1 = single_port_activation("in", vec![webhook_envelope("first")], 0);
    assert!(
        matches!(comp.handle(a1), ProcessorOutcome::Err(_)),
        "activation 1 grants only 500 mt (< 1000) — publish rejected"
    );
    let a2 = single_port_activation("in", vec![webhook_envelope("second")], 0);
    match comp.handle(a2) {
        ProcessorOutcome::Ok { publishes, .. } => {
            assert_eq!(
                publishes.len(),
                1,
                "carryover reaches 1000 mt ⇒ one publish"
            );
            assert_eq!(publishes[0].payload, "second");
        }
        other => panic!("activation 2 should publish via accumulated carryover, got {other:?}"),
    }
}

/// No refund on failure: an activation that consumes a token and then fails must
/// NOT restore the consumed token to carryover (anti-retry-amplification).
///
/// amplification 0.5 (500 mt), fill 0. Activation 1 delivers two new envelopes:
/// grant = 2 × 500 = 1000 mt = exactly one token. The demo publishes the first
/// envelope (charging the token → budget 0), then the second publish hits
/// `quota-exceeded` and the demo maps it to `ProcessingFailed` (Err) — the
/// activation fails after consuming one token. Writeback leaves carry = 0.
///
/// Activation 2 delivers one new envelope: grant = 500 mt. If the consumed token
/// had been refunded, carry would be 1000 and budget 1500 ⇒ the publish would
/// succeed (Ok). Because it is NOT refunded, carry is 0, budget is 500 (< 1000),
/// and the publish is rejected (Err). Asserting activation 2 is Err proves the
/// token stayed consumed across the failed activation.
#[test]
fn per_sink_no_refund_of_consumed_tokens_on_failure() {
    let comp = load_demo_budgeted("brenn:test-out", 500, 0, 1_000_000);
    let a1 = single_port_activation(
        "in",
        vec![webhook_envelope("first"), webhook_envelope("second")],
        0,
    );
    assert!(
        matches!(comp.handle(a1), ProcessorOutcome::Err(_)),
        "activation 1 grants exactly one token; the 2nd publish exhausts it and fails the activation"
    );
    let a2 = single_port_activation("in", vec![webhook_envelope("third")], 0);
    assert!(
        matches!(comp.handle(a2), ProcessorOutcome::Err(_)),
        "the token consumed in activation 1 must not be refunded — activation 2 has only \
         500 mt (< 1000) and must be rejected; an Ok here would mean the token was resurrected"
    );
}

/// The capacity clamp is applied at the next activation start and genuinely bounds
/// carryover end-to-end. fill 1000, amplification 0, capacity 1000 (one token).
///
/// Two idle activations (zero new envelopes) accumulate fill into carry:
/// 0 → 1000 → 2000. The clamp caps the carried-over contribution at 1000, so at the
/// third activation the seeded budget is `min(carry, 1000) + fill = 2000`, NOT the
/// unclamped `carry + fill = 3000`. Driving three new envelopes through that third
/// activation lets the demo publish two (2000 mt) and rejects the third — an Err.
/// Without the clamp the budget would be 3000, all three would publish, and the
/// activation would be Ok. Asserting Err proves the clamp is live at its call site
/// and applied at activation start.
#[test]
fn per_sink_capacity_clamp_bounds_carryover() {
    let comp = load_demo_budgeted("brenn:test-out", 0, 1000, 1000);
    // Two idle activations: one context-only envelope each (new_from == len ⇒ 0 new).
    for body in ["idle-1", "idle-2"] {
        let idle = single_port_activation("in", vec![webhook_envelope(body)], 1);
        assert!(
            matches!(comp.handle(idle), ProcessorOutcome::Ok { .. }),
            "an idle activation publishes nothing and only accumulates fill into carry"
        );
    }
    // Third activation: three new envelopes. Seeded budget is clamped to
    // min(carry=2000, cap=1000) + fill=1000 = 2000 ⇒ two publishes, third rejected.
    let spend = single_port_activation(
        "in",
        vec![
            webhook_envelope("s1"),
            webhook_envelope("s2"),
            webhook_envelope("s3"),
        ],
        0,
    );
    assert!(
        matches!(comp.handle(spend), ProcessorOutcome::Err(_)),
        "clamped budget (2000 mt) allows two publishes; the third is rejected — without the \
         clamp the budget would be 3000 and all three would succeed"
    );
}

/// A suppressed publish emits exactly one post-activation `warn` naming the slug
/// and the per-sink dropped counts.
#[test]
#[traced_test]
fn per_sink_suppression_emits_post_activation_warn() {
    let comp = load_demo_budgeted("brenn:test-out", 0, 0, 0);
    let activation = single_port_activation("in", vec![webhook_envelope("body")], 0);
    let _ = comp.handle(activation);
    assert!(
        logs_contain("wasm publish budget exceeded"),
        "the post-activation warn must fire on a suppressed publish"
    );
    assert!(logs_contain("demo-budget"), "the warn must carry the slug");
    assert!(
        logs_contain("Port(\"out\")"),
        "the warn must name the suppressed port sink via its SinkKey debug form"
    );
}

/// Multi-port routing: one envelope → publish to "out1" and "out2" in the same
/// activation → each publish resolves to its own distinct channel_address.
///
/// Regression gate for all-publishes-to-first-channel keying bugs in
/// `ports::Host::publish` (design §2.2: port→channel resolution happens at
/// buffer time, one `output_ports` map lookup per call).
#[test]
fn multi_port_publishes_routed_independently() {
    let comp = load_dual();
    // One brenn envelope → processor-dual publishes its JSON to "out1" then "out2".
    let payload = envelope_json("brenn:source", "hello");
    let activation = single_port_activation("in", vec![payload.clone()], 0);
    match comp.handle(activation) {
        ProcessorOutcome::Ok { publishes, .. } => {
            assert_eq!(
                publishes.len(),
                2,
                "expected 2 publishes (one per output port)"
            );
            // out1 publish must resolve to brenn:channel-out1, not out2's address.
            assert_eq!(
                publishes[0].channel_address, "brenn:channel-out1",
                "first publish must resolve to out1's channel"
            );
            assert_eq!(publishes[0].port, "out1");
            assert_eq!(publishes[0].payload, payload);
            // out2 publish must resolve to brenn:channel-out2, not out1's address.
            assert_eq!(
                publishes[1].channel_address, "brenn:channel-out2",
                "second publish must resolve to out2's channel"
            );
            assert_eq!(publishes[1].port, "out2");
            assert_eq!(publishes[1].payload, payload);
        }
        other => panic!("expected Ok with 2 publishes, got {other:?}"),
    }
}

// ── The synchronous channel, on a host that mints none ───────────────────────

/// This host always lowers `sync: none`, because a headless component has no
/// cause that could be synchronous. The world carries the field anyway — one
/// world, two hostings — so the guest can always answer, and the rule that only
/// a sync-call activation may be answered has to hold here as well as in the
/// page. It holds as a trap: reading an unasked-for ok would flush a buffer the
/// component built while confused about why it was running.
///
/// The fixture publishes before it replies, so the discard is what is under
/// test and not just the outcome variant: an arm that flushed and then trapped,
/// or a `some` match placed below the flush, would put that publish on the bus.
/// `Trap` carries no publishes, so the outcome type is the assertion — which is
/// only worth anything with a non-empty buffer behind it.
#[test]
fn a_reply_to_an_activation_that_asked_nothing_traps() {
    let comp = load_multiport();
    let activation = ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![envelope_json("brenn:test", "__reply__")],
            new_from: 0,
            dropped: 0,
        }],
        deferred: vec![],
        now: None,
        sync: None,
    };
    match comp.handle(activation) {
        ProcessorOutcome::Trap(message) => assert!(
            message.contains("asked nothing"),
            "trap must name the cause: {message}"
        ),
        other => panic!("an unasked-for reply must trap and flush nothing, got {other:?}"),
    }
}

/// The other half of the rule: `ok(none)` is the only ok shape this host reads,
/// and an ordinary activation reaching an ordinary guest produces it.
#[test]
fn an_ordinary_activation_returns_the_only_ok_this_host_reads() {
    let comp = load_multiport();
    let activation = ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![envelope_json("brenn:test", "plain")],
            new_from: 0,
            dropped: 0,
        }],
        deferred: vec![],
        now: None,
        sync: None,
    };
    match comp.handle(activation) {
        ProcessorOutcome::Ok { publishes, .. } => {
            assert_eq!(publishes.len(), 1, "multiport publishes its summary")
        }
        other => panic!("an ordinary activation must return ok, got {other:?}"),
    }
}

/// A sync-call activation is a shape this host cannot honestly deliver: there is
/// no element, no gesture, and no caller waiting on a stack. It is refused where
/// it is built rather than degraded into an asynchronous one.
#[test]
#[should_panic(expected = "cannot be lowered into the processor world")]
fn lowering_a_sync_call_activation_is_refused() {
    let comp = load_multiport();
    let _ = comp.handle(ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![envelope_json("brenn:test", "click")],
            new_from: 0,
            dropped: 0,
        }],
        deferred: vec![],
        now: None,
        sync: Some("in".to_string()),
    });
}

// ── Context/new split via context_envelopes() ────────────────────────────────

/// Exercises `context_envelopes()` through observable WASM output using the
/// processor-multiport fixture. The fixture counts context envelopes via
/// `context_envelopes().count()` and serialises the count as `context_count`
/// in its summary JSON. Two context envelopes + one new envelope are supplied;
/// the asserted summary must show `context_count == 2` and `new_from == 2`.
///
/// Regression gate: a `[..new_from]` ↔ `[new_from..]` transposition in
/// `context_envelopes()` would yield `context_count == 1` (the new slice
/// contains one envelope), not 2, causing this test to fail.
#[test]
fn context_envelopes_count_matches_new_from() {
    let comp = load_multiport();
    // Two context envelopes, one new envelope; new_from == 2.
    let activation = ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![
                envelope_json("brenn:test", "ctx-a"),
                envelope_json("brenn:test", "ctx-b"),
                envelope_json("brenn:test", "new-1"),
            ],
            new_from: 2,
            dropped: 0,
        }],
        deferred: vec![],
        now: None,
        sync: None,
    };
    match comp.handle(activation) {
        ProcessorOutcome::Ok { publishes, .. } => {
            assert_eq!(
                publishes.len(),
                1,
                "multiport publishes exactly one summary"
            );
            let summary: serde_json::Value =
                serde_json::from_str(&publishes[0].payload).expect("summary must be valid JSON");
            let port_summary = &summary[0];
            assert_eq!(
                port_summary["context_count"],
                serde_json::Value::Number(2.into()),
                "context_count must equal new_from (2); a slice-index transposition would produce 1"
            );
            assert_eq!(
                port_summary["new_from"],
                serde_json::Value::Number(2.into()),
                "new_from must be 2"
            );
            assert_eq!(
                port_summary["len"],
                serde_json::Value::Number(3.into()),
                "total envelope count must be 3"
            );
        }
        other => panic!("expected Ok with one summary publish, got {other:?}"),
    }
}

// ── Typed urgency publish ─────────────────────────────────────────────────────

/// Driving the `__urgency__:<level>` directive through processor-dual calls
/// `publish_with_urgency` on the guest side, which the host resolves to the
/// named WIT `urgency` enum variant. The resulting publish must carry
/// `ProcessorUrgency::High`, overriding the port's configured default
/// (`ProcessorUrgency::Normal`). Closes the design §4 gap: no fixture
/// exercised `publish-with-urgency` end-to-end before this test.
#[test]
fn publish_with_urgency_overrides_port_default() {
    let comp = load_dual();
    // The "__urgency__:high" directive body causes the fixture to call
    // publish_with_urgency("out1", "urgency-marker", Urgency::High).
    let activation = single_port_activation(
        "in",
        vec![envelope_json("brenn:test", "__urgency__:high")],
        0,
    );
    match comp.handle(activation) {
        ProcessorOutcome::Ok { publishes, .. } => {
            assert_eq!(
                publishes.len(),
                1,
                "directive path publishes exactly one message to out1"
            );
            assert_eq!(publishes[0].port, "out1");
            assert_eq!(publishes[0].payload, "urgency-marker");
            assert_eq!(
                publishes[0].urgency,
                ProcessorUrgency::High,
                "publish_with_urgency must override port default (Normal) with High"
            );
        }
        other => panic!("expected Ok with one publish, got {other:?}"),
    }
}

/// Complete urgency-level coverage for `publish_with_urgency`: VeryLow, Low, Normal, High.
///
/// The High case is already in `publish_with_urgency_overrides_port_default`; these three
/// cover the remaining variants of the exhaustive `urgency_to_wit` match in brenn-guest.
/// A transposition (e.g. VeryLow ↔ Normal) would be invisible without this coverage
/// because Normal is also the port default.
#[test]
fn publish_with_urgency_all_levels() {
    use ProcessorUrgency::{High, Low, Normal, VeryLow};
    let cases: &[(&str, ProcessorUrgency)] = &[
        ("very-low", VeryLow),
        ("low", Low),
        ("normal", Normal),
        ("high", High),
    ];
    for (level_str, expected_urgency) in cases {
        let comp = load_dual();
        let activation = single_port_activation(
            "in",
            vec![envelope_json(
                "brenn:test",
                &format!("__urgency__:{level_str}"),
            )],
            0,
        );
        match comp.handle(activation) {
            ProcessorOutcome::Ok { publishes, .. } => {
                assert_eq!(
                    publishes.len(),
                    1,
                    "urgency directive must publish exactly one message; level={level_str}"
                );
                assert_eq!(
                    publishes[0].urgency, *expected_urgency,
                    "urgency level {level_str} must map to {expected_urgency:?}"
                );
            }
            other => panic!("expected Ok for urgency level {level_str}, got {other:?}"),
        }
    }
}

/// Host-invariant violation: `new_from > envelopes.len()` must produce a labeled
/// `ProcessingFailed` error, not a trap or panic. `build_activation` validates this
/// before user code runs; the explicit error string reaches the host's alert path intact.
///
/// Regression gate: if the range check is accidentally removed or inverted, all four
/// `PortWindow` accessors would slice out-of-bounds → opaque trap, undetected otherwise.
#[test]
fn host_invariant_violation_new_from_exceeds_len_returns_labeled_error() {
    let comp = load_demo_with_out();
    // new_from=2 but only one envelope → host invariant violation.
    let activation = ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![envelope_json("brenn:test", "only-one")],
            new_from: 2,
            dropped: 0,
        }],
        deferred: vec![],
        now: None,
        sync: None,
    };
    match comp.handle(activation) {
        ProcessorOutcome::Err(e) => {
            let diag = format!("{e:?}");
            assert!(
                diag.contains("host invariant violation"),
                "error must contain 'host invariant violation'; got: {diag}"
            );
        }
        other => panic!("expected Err(processing-failed) for invariant violation, got {other:?}"),
    }
}

// ── Fuel / memory exhaustion ──────────────────────────────────────────────────

/// A fuel-exhausting guest produces `ProcessorOutcome::Trap` — host does not stall.
///
/// The message assert ("all fuel consumed") also verifies that the `{e:#}` anyhow
/// alternate-format is intact for the Err arm in `invoke`: if this were reverted to
/// `e.to_string()` the root cause would be dropped and the message would start with
/// "error while executing at wasm backtrace:" without the fuel phrase, failing here.
#[test]
fn fuel_exhausting_guest_traps_not_panics() {
    let comp = load_exhaust();
    let activation = single_port_activation("in", vec![envelope_json("brenn:test", "spin")], 0);
    let outcome = comp.handle(activation);
    match &outcome {
        ProcessorOutcome::Trap(msg) => {
            assert!(
                msg.contains("all fuel consumed"),
                "fuel-trap message must contain wasmtime fuel-exhaustion phrase; got: {msg}"
            );
        }
        other => panic!("fuel-exhausting guest must produce Trap, got {other:?}"),
    }
}

/// The fuel budget for a full-size window does not spuriously trap the demo.
#[test]
fn full_size_window_does_not_spuriously_trap_demo() {
    let comp = load_demo_with_out();
    let envelopes: Vec<String> = (0..FULL_WINDOW)
        .map(|i| envelope_json("brenn:ch", &format!("msg-{i}")))
        .collect();
    let activation = single_port_activation("in", envelopes, 0);
    let outcome = comp.handle(activation);
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "demo component must succeed on a full-size window. got {outcome:?}"
    );
}

const FULL_WINDOW: usize = 8;

/// A full-size window costs well under the fuel a real activation is given.
///
/// Drives the window on a quarter of the production budget and requires success, so
/// the assertion is about *headroom*: a codegen change (a wasmtime bump is the usual
/// cause) that quadruples instructions-retired per envelope fails here, while a
/// did-it-trap assertion at the full budget would stay green until the margin was
/// gone entirely.
#[test]
fn full_size_window_leaves_fuel_headroom() {
    let comp = load_demo_with_out();
    let production_budget =
        PROCESSOR_FUEL_MINIMUM.max((FULL_WINDOW as u64) * PROCESSOR_FUEL_PER_ENVELOPE);
    let envelopes: Vec<String> = (0..FULL_WINDOW)
        .map(|i| envelope_json("brenn:ch", &format!("msg-{i}")))
        .collect();
    let activation = single_port_activation("in", envelopes, 0);
    // Epoch deadline far beyond the ticker's reach for this activation, so fuel is the only
    // limit under test.
    let outcome = comp.handle_with_limits(
        activation,
        common::GENEROUS_EPOCH_TICKS,
        production_budget / 4,
    );
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "a full-size window must complete on a quarter of the production fuel budget \
         ({} of {production_budget}); got {outcome:?}",
        production_budget / 4
    );
}

/// The exhaust component returns Ok when given a pure-context window (no new envelopes).
#[test]
fn exhaust_component_ok_on_pure_context_window() {
    let comp = load_exhaust();
    let activation = single_port_activation(
        "in",
        vec![envelope_json("brenn:test", "ctx")],
        1, // new_from == envelopes.len() → no new entries → Ok
    );
    assert!(matches!(
        comp.handle(activation),
        ProcessorOutcome::Ok { .. }
    ));
}

// ── Memory cap runtime tests ──────────────────────────────────────────────────

/// A guest that attempts to grow past the 16 MiB memory cap traps deterministically.
///
/// The fixture (`processor-mem-exhaust`) uses `Vec::try_reserve_exact` — a safe,
/// fallible allocator call — in 1 MiB chunks. With `trap_on_grow_failure = true` the
/// host raises a trap inside `memory.grow` before `try_reserve_exact` can return `Err`,
/// so the graceful-guest arm in the fixture is unreachable and the outcome is
/// `ProcessorOutcome::Trap`. The message assert pins the grow-failure root cause
/// specifically (not fuel/epoch), proving it is the limiter that fired.
///
/// Fuel cannot trip first: the cap is crossed after ~17 iterations of simple
/// allocator work (1 MiB chunk × 17 ≈ 17 MiB > 16 MiB cap), which costs far
/// fewer than `PROCESSOR_FUEL_MINIMUM` (50 M) instructions. If `PROCESSOR_FUEL_MINIMUM`
/// were ever lowered below the cost of ~17 chunk iterations, the fuel limiter could
/// trip first and the message assert ("forcing trap when growing memory") would fail,
/// making the regression visible rather than silent.
///
/// Regression gate: reverting `trap_on_grow_failure` to `false` causes the fixture's
/// `try_reserve_exact` to return `Err` and the fixture to return `Ok(())`, making this
/// test fail.
#[test]
fn memory_cap_excess_traps_deterministically() {
    let comp = load_mem_exhaust();
    let activation = single_port_activation(
        "in",
        vec![envelope_json("brenn:test", "trigger")],
        0, // new_from=0 → the single envelope is new → fixture attempts memory exhaust
    );
    let outcome = comp.handle(activation);
    match &outcome {
        ProcessorOutcome::Trap(msg) => {
            // Assert the exact phrase `StoreLimits::memory_growing` raises, not just a
            // substring, so a {e:#}→e.to_string() revert would cause this to fail
            // (the backtrace outermost context does not repeat the root-cause phrase).
            assert!(
                msg.contains("forcing trap when growing memory"),
                "trap message must contain the limiter root-cause phrase; got: {msg}"
            );
        }
        other => panic!("expected ProcessorOutcome::Trap from memory cap excess, got {other:?}"),
    }
}

/// The mem-exhaust fixture does NOT trap on a pure-context window (no new envelopes).
///
/// Confirms that the `trap_on_grow_failure` flag does not spuriously trap a guest
/// that stays under the 16 MiB cap.
#[test]
fn mem_exhaust_component_ok_on_pure_context_window() {
    let comp = load_mem_exhaust();
    let activation = single_port_activation(
        "in",
        vec![envelope_json("brenn:test", "ctx")],
        1, // new_from == envelopes.len() → no new entries → Ok
    );
    assert!(matches!(
        comp.handle(activation),
        ProcessorOutcome::Ok { .. }
    ));
}

// ── Memory limits ─────────────────────────────────────────────────────────────

#[test]
fn processor_max_memory_bytes_is_sensible() {
    const { assert!(PROCESSOR_MAX_MEMORY_BYTES >= 1024 * 1024) };
    const { assert!(PROCESSOR_MAX_MEMORY_BYTES <= 256 * 1024 * 1024) };
}

#[test]
fn processor_resource_cap_constants_are_sensible() {
    const { assert!(PROCESSOR_MAX_TABLE_ELEMENTS >= 1024) };
    const { assert!(PROCESSOR_MAX_TABLE_ELEMENTS <= 1_000_000) };
    const { assert!(PROCESSOR_MAX_INSTANCES >= 1) };
    const { assert!(PROCESSOR_MAX_INSTANCES <= 64) };
    const { assert!(PROCESSOR_MAX_TABLES >= 1) };
    const { assert!(PROCESSOR_MAX_TABLES <= 256) };
    const { assert!(PROCESSOR_MAX_MEMORIES >= 1) };
    const { assert!(PROCESSOR_MAX_MEMORIES <= 64) };
    // The aggregate linear-memory worst case is memories × per-memory size; hold it
    // to 512 MiB so raising either factor alone cannot quietly multiply the bound.
    const { assert!(PROCESSOR_MAX_MEMORIES * PROCESSOR_MAX_MEMORY_BYTES <= 512 * 1024 * 1024) };
}

/// wasmtime applies `table_elements` to each table separately, and an over-cap grow
/// is a trap rather than a refused grow the guest could paper over.
///
/// Both properties are asserted against wasmtime's own limiter — the same value
/// `processor_store_limits` hands every activation — so a wasmtime release that
/// made the ceiling store-wide, or stopped trapping, fails here instead of leaving
/// a comment claiming semantics the engine no longer has. Per-table also means the
/// store-wide worst case is `PROCESSOR_MAX_TABLE_ELEMENTS * PROCESSOR_MAX_TABLES`.
///
/// This drives the limiter directly; `table_cap_excess_traps_through_a_guest`
/// drives the same cap through guest code.
#[test]
fn table_element_cap_is_per_table_and_traps_on_excess() {
    use wasmtime::ResourceLimiter;

    let mut limits = brenn_wasm::processor_store_limits();
    assert!(
        limits
            .table_growing(0, PROCESSOR_MAX_TABLE_ELEMENTS, None)
            .expect("growing exactly to the cap must be permitted, not a trap"),
        "growing exactly to the cap must be permitted"
    );

    let err = limits
        .table_growing(0, PROCESSOR_MAX_TABLE_ELEMENTS + 1, None)
        .expect_err("growing past the cap must trap, not return a refused grow");
    let diag = format!("{err:#}");
    assert!(
        diag.contains("forcing trap when growing table to"),
        "over-cap table growth must raise the limiter's trap phrase; got: {diag}"
    );

    // Per-table, not store-wide: a second table may reach the same ceiling after the
    // first one already has. A store-wide ceiling would refuse this.
    assert!(
        limits
            .table_growing(0, PROCESSOR_MAX_TABLE_ELEMENTS, None)
            .expect("a second table growing to the cap must be permitted"),
        "the cap must apply per table, not to the store's running total"
    );
}

/// The instance, table-count and memory-count caps reach the limiter wasmtime
/// consults.
///
/// `ResourceLimiter::instances`/`tables`/`memories` are what the engine calls at
/// instantiation; asserting them here means dropping any of them from
/// `processor_store_limits` fails rather than silently reverting to wasmtime's
/// defaults (10,000 of each).
///
/// The memory-count assertion is belt-and-braces by construction: multi-memory is
/// outside the guest feature envelope, so a guest gets at most one memory per
/// module instance and the instance cap is the reachable bound. It is asserted
/// anyway because the aggregate linear-memory bound is `memories × memory_size`,
/// and that product should rest on a stated cap rather than on a second policy
/// (the envelope) that a later change could widen.
#[test]
fn instance_table_and_memory_count_caps_reach_the_limiter() {
    use wasmtime::ResourceLimiter;

    let limits = brenn_wasm::processor_store_limits();
    assert_eq!(
        limits.instances(),
        PROCESSOR_MAX_INSTANCES,
        "the instance cap must reach wasmtime's limiter"
    );
    assert_eq!(
        limits.tables(),
        PROCESSOR_MAX_TABLES,
        "the table-count cap must reach wasmtime's limiter"
    );
    assert_eq!(
        limits.memories(),
        PROCESSOR_MAX_MEMORIES,
        "the memory-count cap must reach wasmtime's limiter"
    );
}

// ── Guest-driven structural growth ────────────────────────────────────────────
//
// The Rust guests this build produces emit no table-growth instructions at all
// (`wasm32-unknown-unknown` LLVM never grows a table), so the only way to drive a
// guest across `PROCESSOR_MAX_TABLE_ELEMENTS` is a hand-written WAT component.
//
// The fixture's canonical-ABI surface is minimized on purpose: the lift
// declarations match the processor world's `receive` exactly, because that is
// what the load-time typecheck demands, but the core function body accepts the
// lowered parameters opaquely and never decodes them. The subject under test is
// the limiter trap, not envelope handling. It is driven with an empty activation
// so the host has almost nothing to lower into guest memory.

/// A processor-world component in WAT whose `receive` core body is `body`.
///
/// Three parts, all of them forced by the load path rather than chosen:
///
/// - The `brenn:processor/types` instance type, imported and aliased. An exported
///   function's type may only name types the component itself imports or exports,
///   so a structurally-identical set of anonymous component-level types is
///   rejected at validation ("func not valid to be used as export"). This mirrors
///   what `wit-component` emits for the Rust fixtures, and the import is
///   type-only — it needs no grant.
/// - The core module: the memory and bump `realloc` the canonical ABI needs in
///   order to lower an activation, plus `receive` whose core signature is the
///   flattened form of `func(activation) -> result<option<string>, receive-error>`
///   — the record's four fields flatten to (list ptr, list len) twice, (option
///   discriminant, u64), and (option discriminant, string ptr, string len); the
///   result exceeds one flat value so it comes back as a pointer.
/// - The lift, whose declared type must match the world's `receive` exactly.
///
/// The core body accepts those lowered parameters opaquely and never decodes
/// them; the subject under test is the store limiter, not envelope handling.
fn wat_processor_component(body: &str) -> String {
    wat_processor_component_with(body, "")
}

/// As `wat_processor_component`, plus `extra_core_defs` spliced in after the main
/// core instance — for fixtures that need additional core instances.
fn wat_processor_component_with(body: &str, extra_core_defs: &str) -> String {
    format!(
        r#"(component
  (type $types-ty
    (instance
      (type $envelope-json string)
      (export "envelope-json" (type $envelope-json-e (eq $envelope-json)))
      (type $envelopes (list $envelope-json-e))
      (type $port-window-t (record
        (field "port" string)
        (field "envelopes" $envelopes)
        (field "new-from" u32)
        (field "dropped" u32)))
      (export "port-window" (type $port-window-e (eq $port-window-t)))
      (type $deferred-entry-t (record
        (field "index" u32)
        (field "payload" string)
        (field "deliver-after" u64)))
      (export "deferred-entry" (type $deferred-entry-e (eq $deferred-entry-t)))
      (type $entries (list $deferred-entry-e))
      (type $deferred-window-t (record
        (field "port" string)
        (field "entries" $entries)))
      (export "deferred-window" (type $deferred-window-e (eq $deferred-window-t)))
      (type $ports-list (list $port-window-e))
      (type $deferred-list (list $deferred-window-e))
      (type $now (option u64))
      (type $sync (option string))
      (type $activation-t (record
        (field "ports" $ports-list)
        (field "deferred" $deferred-list)
        (field "now" $now)
        (field "sync" $sync)))
      (export "activation" (type $activation-e (eq $activation-t)))
      (type $receive-error-t (variant
        (case "malformed-envelope" string)
        (case "processing-failed" string)))
      (export "receive-error" (type $receive-error-e (eq $receive-error-t)))
    )
  )
  (import "brenn:processor/types@0.1.0" (instance $types (type $types-ty)))
  (alias export $types "activation" (type $activation))
  (alias export $types "receive-error" (type $receive-error))
  (core module $m
    (memory (export "memory") 1)
    (table $t 0 funcref)
    (global $bump (mut i32) (i32.const 1024))
    (func (export "realloc") (param i32 i32 i32 i32) (result i32)
      (local $ret i32)
      (local.set $ret (global.get $bump))
      (global.set $bump (i32.add (global.get $bump) (local.get 3)))
      (local.get $ret))
    (func (export "receive")
      (param i32) (param i32) (param i32) (param i32) (param i32) (param i64)
      (param i32) (param i32) (param i32)
      (result i32)
      {body}
      (i32.const 0))
  )
  (core instance $i (instantiate $m))
  {extra_core_defs}
  (alias core export $i "memory" (core memory $mem))
  (alias core export $i "realloc" (core func $realloc))
  (alias core export $i "receive" (core func $cf))
  (type $rt (result (option string) (error $receive-error)))
  (type $ft (func (param "a" $activation) (result $rt)))
  (func $lifted (type $ft)
    (canon lift (core func $cf) (memory $mem) (realloc $realloc) string-encoding=utf8))
  (export "receive" (func $lifted))
)"#
    )
}

/// Load a WAT processor component with no grants and drive it with an empty
/// activation.
fn run_wat_processor(slug: &str, wat_src: &str) -> ProcessorOutcome {
    use std::io::Write;

    let wasm_bytes = wat::parse_str(wat_src)
        .unwrap_or_else(|e| panic!("WAT fixture for {slug} must assemble: {e}"));
    let mut wasm_file = tempfile::NamedTempFile::new().expect("tempfile");
    wasm_file.write_all(&wasm_bytes).expect("write wasm");
    wasm_file.flush().expect("flush wasm");

    let comp = ProcessorComponent::load(ProcessorLoadSpec {
        component_path: wasm_file.path(),
        slug,
        declared_out_ports: std::collections::BTreeSet::new(),
        output_ports: HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        config: HashMap::new(),
        grants: std::collections::BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    });
    comp.handle(ProcessorActivation {
        ports: vec![],
        deferred: vec![],
        now: None,
        sync: None,
    })
}

/// A guest that grows a table past `PROCESSOR_MAX_TABLE_ELEMENTS` traps, and the
/// trap surfaces as `ProcessorOutcome::Trap` carrying the limiter's phrase.
///
/// This is the table twin of `memory_cap_excess_traps_deterministically`: the
/// limiter-value tests prove wasmtime enforces the number, this proves the store
/// actually consults the limiter for tables when guest code grows one. Reverting
/// `trap_on_grow_failure` turns the `table.grow` into a `-1` the guest ignores and
/// the activation into `Ok`, failing this test.
#[test]
fn table_cap_excess_traps_through_a_guest() {
    let wat_src = wat_processor_component(&format!(
        "(drop (table.grow $t (ref.null func) (i32.const {})))",
        PROCESSOR_MAX_TABLE_ELEMENTS + 1
    ));
    let outcome = run_wat_processor("wat-table-grow", &wat_src);
    match &outcome {
        ProcessorOutcome::Trap(msg) => assert!(
            msg.contains("forcing trap when growing table to"),
            "trap message must carry the limiter's table-growth phrase; got: {msg}"
        ),
        other => panic!("expected Trap from an over-cap table.grow, got {other:?}"),
    }
}

/// A guest that stays under the cap does not trap, so the test above is pinning
/// the cap rather than any `table.grow` at all.
#[test]
fn table_growth_within_the_cap_does_not_trap() {
    let wat_src = wat_processor_component(&format!(
        "(drop (table.grow $t (ref.null func) (i32.const {})))",
        PROCESSOR_MAX_TABLE_ELEMENTS
    ));
    let outcome = run_wat_processor("wat-table-grow-ok", &wat_src);
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "growing exactly to the cap must not trap; got {outcome:?}"
    );
}

/// A component whose core instantiations exceed `PROCESSOR_MAX_INSTANCES` fails
/// inside `processor_pre.instantiate` and surfaces as `ProcessorOutcome::Trap`.
///
/// The filler module declares nothing, so this crosses the instance cap without
/// also crossing the memory or table caps — the trap is attributable to the
/// instance limiter alone. Instantiation happens per activation, not at load, so
/// the component loads fine and fails when driven.
#[test]
fn instance_cap_excess_traps_at_instantiation() {
    let filler_instances: String = (0..=PROCESSOR_MAX_INSTANCES)
        .map(|n| format!("(core instance $f{n} (instantiate $filler))\n  "))
        .collect();
    let wat_src = wat_processor_component_with(
        "(nop)",
        &format!("(core module $filler)\n  {filler_instances}"),
    );
    let outcome = run_wat_processor("wat-instance-cap", &wat_src);
    match &outcome {
        ProcessorOutcome::Trap(msg) => assert!(
            msg.contains("instantiation failed") && msg.contains("instance"),
            "trap must name the instantiation failure and the instance limit; got: {msg}"
        ),
        other => panic!("expected Trap from exceeding the instance cap, got {other:?}"),
    }
}

// ── Store-through-processor ───────────────────────────────────────────────────

/// Guest-exercised store linker-wiring coverage: exercises all six guest-callable
/// store operations (begin, put, get, scan, delete, rollback, commit) from WASM
/// guest code through the wasmtime linker to the host implementation.
///
/// The fixture (processor-store-rt) chains these operations in a single activation:
///   put+commit → get+commit (assert value) → scan+commit (assert result) →
///   delete+commit → get+commit (assert absent) → put+(RAII rollback) → get+commit (assert absent)
///
/// The final put is rolled back via the brenn-guest RAII Transaction::drop (no explicit
/// rollback call in the fixture), proving the guard works on the non-error path too.
///
/// An Err return means the linker, ABI lift/lower, or host implementation failed.
/// The fixture fails loudly with labeled ProcessingFailed messages in all such cases.
///
/// Post-activation: the namespace must be empty (delete committed, RAII rollback
/// reverted the final put). Verified via kv_store_for_testing to confirm persistence.
#[test]
fn store_guest_round_trip_begin_put_commit_get() {
    let (comp, _db) = load_store_component("brenn_processor_store_rt", "store-rt");
    let activation = single_port_activation("in", vec![], 0);
    let outcome = comp.handle(activation);
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "store round-trip must succeed; got {outcome:?}"
    );

    // Post-activation: fixture ends with delete committed and RAII rollback reverting
    // the final put — namespace must be empty.
    let kv = comp.kv_store_for_testing();
    let pairs = kv.scan_for_testing("test-ns");
    assert_eq!(
        pairs.len(),
        0,
        "namespace must be empty after fixture (delete committed, RAII rollback reverted final put); got {pairs:?}"
    );
}

/// RAII rollback test: a live transaction held only by the brenn-guest guard rolls back
/// on Err return without an explicit `rollback()` call.
///
/// The `__raii_rollback__` sentinel causes the fixture to: begin a transaction, write
/// a key, then return Err without rolling back. The RAII guard in Transaction::drop
/// calls rollback on the way out. Proves the brenn-guest guard eliminates the
/// leaked-tx trap footgun (the original fixture had explicit rollback calls on every
/// error path; now none are needed).
///
/// Asserts: outcome is Err (not Trap) — the labeled diagnostic survives, confirming
/// the guard's rollback() does NOT trigger the host's "rollback-after-commit" trap.
/// Asserts: the sentinel key is absent — rollback was effective.
#[test]
fn store_raii_rollback_on_err_return_no_trap() {
    let (comp, _db) = load_store_component("brenn_processor_store_rt", "store-rt");
    let activation = single_port_activation(
        "in",
        vec![envelope_json("brenn:test", "__raii_rollback__")],
        0,
    );
    let outcome = comp.handle(activation);
    match &outcome {
        ProcessorOutcome::Err(e) => {
            let diag = format!("{e:?}");
            assert!(
                diag.contains("RAII rollback"),
                "expected RAII rollback diagnostic; got: {diag}"
            );
        }
        other => panic!("expected Err(processing-failed) not a trap; got {other:?}"),
    }

    // The key must be absent — rollback was effective.
    let kv = comp.kv_store_for_testing();
    let pairs = kv.scan_for_testing("test-ns");
    assert_eq!(
        pairs.len(),
        0,
        "raii-key must be absent after RAII rollback; got {pairs:?}"
    );
}

// ── Ordering/context semantics ────────────────────────────────────────────────

#[test]
fn guest_observes_correct_new_from_and_ordering() {
    // new_from=1: first entry is context, second is new.
    let comp = load_demo_with_out();
    let activation = single_port_activation(
        "in",
        vec![
            envelope_json("brenn:test", "ctx"),
            envelope_json("brenn:test", "new-entry"),
        ],
        1,
    );
    assert!(matches!(
        comp.handle(activation),
        ProcessorOutcome::Ok { .. }
    ));
}

// ── Quota-exceeded ────────────────────────────────────────────────────────────

/// Publishing one more message than `MAX_PUBLISHES_PER_ACTIVATION` (256)
/// must return `Err(processing-failed)` — the demo component maps `QuotaExceeded`
/// to `ProcessingFailed`. Uses webhook-typed envelopes so each triggers one publish.
///
/// This exercises the count-cap branch in `ports::Host::publish` (design §2.2).
#[test]
fn quota_exceeded_count_cap_returns_err() {
    use brenn_budget::MAX_PUBLISHES_PER_ACTIVATION;

    let comp = load_demo_with_out();
    // N = cap + 1 webhook envelopes; each causes one publish call. The 257th call
    // returns QuotaExceeded → demo maps to ProcessingFailed → guest returns Err.
    let n = MAX_PUBLISHES_PER_ACTIVATION + 1;
    let envelopes: Vec<String> = (0..n).map(|i| webhook_envelope(&format!("p{i}"))).collect();
    let activation = single_port_activation("in", envelopes, 0);
    assert!(
        matches!(comp.handle(activation), ProcessorOutcome::Err(_)),
        "publishing past the count cap must yield Err(processing-failed)"
    );
}

// ── Epoch deadline ────────────────────────────────────────────────────────────

/// A spinning guest (processor-exhaust) traps when the epoch deadline fires,
/// even with generous fuel. The test uses `handle_with_limits` with a 1-tick
/// deadline; the per-component epoch ticker thread fires within ≈ 100 ms and
/// trips the deadline.
///
/// This exercises the epoch interrupt path (design §2.2, behavior 13).
#[test]
fn epoch_deadline_spins_guest_traps() {
    let comp = load_exhaust();
    // Epoch deadline of 1 tick: the ticker thread (100 ms interval) will increment
    // the epoch within the test window, causing the spinning guest to trap.
    // Fuel is set very high so the fuel cap does NOT trip first.
    let activation = single_port_activation("in", vec![envelope_json("brenn:test", "spin")], 0);
    let outcome = comp.handle_with_limits(
        activation,
        1,            // epoch_deadline: 1 tick — trips fast
        u64::MAX / 2, // fuel: effectively unlimited
    );
    assert!(
        matches!(outcome, ProcessorOutcome::Trap(_)),
        "spinning guest must trap on epoch deadline, got {outcome:?}"
    );
}

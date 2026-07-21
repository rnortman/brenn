// Shared test helpers for brenn-wasm integration tests.
//
// Each integration test file that uses `mod common;` gets its own compiled
// copy of this module (that is normal for Rust integration tests; there is
// no linker-level sharing, but duplication here is fine).
//
// Not every helper is used by every test binary; suppress the resulting
// dead-code warnings at the module level.
#![allow(dead_code)]

use brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT;
use brenn_wasm::{
    GuestAlertSeverity, KvStore, MqttPublishFn, MqttPublishOutcome, OutputAclFn, OutputPortSpec,
    ProcessorAlerter, ProcessorLoadSpec, ProcessorUrgency, QueuedToolRequest, ReplayComponent,
    SinkBudget, ToolCallError, ToolHost, ToolHostFn,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;

/// Capturing `ProcessorAlerter` for integration tests.
///
/// Collects all `(severity, title, body)` triples for post-invocation assertion.
pub struct CapturingAlerter {
    pub calls: Mutex<Vec<(GuestAlertSeverity, String, String)>>,
}

impl CapturingAlerter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
        })
    }
}

impl ProcessorAlerter for CapturingAlerter {
    fn alert(&self, severity: GuestAlertSeverity, title: &str, body: &str) {
        self.calls
            .lock()
            .unwrap()
            .push((severity, title.to_string(), body.to_string()));
    }
}

/// No-op `ProcessorAlerter` for tests that don't exercise the alert path.
pub struct NoopAlerter;

impl ProcessorAlerter for NoopAlerter {
    fn alert(&self, _severity: GuestAlertSeverity, _title: &str, _body: &str) {}
}

/// Allow-all output ACL for tests that don't exercise the publish-ACL gate.
/// Imposes no publish policy — every bound port publishes unconditionally.
pub fn allow_all() -> OutputAclFn {
    Arc::new(|_| true)
}

/// No-op alerter handle for tests that don't exercise the alert path. Wraps
/// `NoopAlerter` so call sites don't each re-declare an identical helper.
pub fn noop_alerter() -> Arc<dyn ProcessorAlerter> {
    Arc::new(NoopAlerter)
}

/// MQTT-publish callback that always reports the publish reached the broker.
///
/// For tests that grant `Mqtt` but do not exercise the egress path (e.g. the
/// superset-grants load test) — the `load` invariant requires a `Some` callback
/// whenever the `Mqtt` grant is present.
pub fn ok_mqtt_publish() -> MqttPublishFn {
    Arc::new(|_client, _topic, _payload, _content_type, _qos, _retain| MqttPublishOutcome::Ok)
}

/// `ToolHost` that reports every tool as unknown/ungranted.
///
/// For tests that grant `Tools` but do not exercise the invocation path (e.g. the
/// superset-grants load test) — the `load` invariant requires a `Some` tool_host
/// whenever the `Tools` grant is present.
pub struct NotGrantedToolHost;

impl ToolHost for NotGrantedToolHost {
    fn fast_call(&self, _tool: &str, _args_json: &str) -> Result<String, ToolCallError> {
        Err(ToolCallError::NotGranted)
    }
    fn queue_async(
        &self,
        _tool: &str,
        _args_json: &str,
        _call_id: &str,
    ) -> Result<QueuedToolRequest, ToolCallError> {
        Err(ToolCallError::NotGranted)
    }
}

/// No-op tool-host handle for tests that grant `Tools` but do not invoke a tool.
pub fn noop_tool_host() -> ToolHostFn {
    Arc::new(NotGrantedToolHost)
}

/// Publish amplification map covering the input port every processor fixture
/// drives (`"in"`), at the default 1.0 (= 1000 millitokens). Extra keys are
/// harmless — the grant only sums over ports present in an activation window.
pub fn amp_in() -> HashMap<String, u64> {
    HashMap::from([("in".to_string(), 1000u64)])
}

/// A per-sink budget large enough it never trips first — for tests exercising
/// the global 256/512/byte backstops or non-budget behavior.
pub fn generous_budget() -> SinkBudget {
    SinkBudget {
        fill_mt: 1_000_000_000,
        capacity_mt: 1_000_000_000,
    }
}

/// A bound output port (default urgency, generous per-sink budget).
pub fn out_spec(channel_address: &str) -> OutputPortSpec {
    OutputPortSpec {
        channel_address: channel_address.to_string(),
        default_urgency: ProcessorUrgency::Normal,
        budget: generous_budget(),
    }
}

/// Load a no-capability processor fixture component with no output ports and a no-op alerter.
///
/// Grants no capabilities. Callers that need capabilities should construct
/// `ProcessorLoadSpec` directly.
pub fn load_processor_noop(name: &str, slug: &str) -> brenn_wasm::ProcessorComponent {
    brenn_wasm::ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/components")
            .join(format!("{name}.wasm")),
        slug,
        output_ports: HashMap::new(),
        input_amplification_mt: amp_in(),
        mqtt_sinks: HashMap::new(),
        config: HashMap::new(),
        grants: std::collections::BTreeSet::new(),
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: Arc::new(NoopAlerter),
        output_acl: allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

/// Open a temporary store and return both the NamedTempFile (keep alive) and
/// the component loaded from `artifact`.
///
/// The caller must keep the `NamedTempFile` alive for the duration of the
/// test; dropping it deletes the backing file.
pub fn open_component(artifact: &Path) -> (NamedTempFile, ReplayComponent) {
    let db = NamedTempFile::new().unwrap();
    let component = ReplayComponent::load(
        "test-replay",
        artifact,
        db.path(),
        DEFAULT_MAX_PAGE_COUNT,
        HashMap::new(),
    );
    (db, component)
}

/// Assert that `store` holds exactly `expected_count` entries in `ns`, and
/// that none of those entries has `needle` at byte offset `key_prefix_len`
/// onward.
///
/// `key_prefix_len` is the number of leading bytes in each key that precede
/// the dedup-identity bytes (e.g. 8 for a `received_at_ms` u64 prefix, 9 for
/// a 1-byte kind tag + u64 prefix).
pub fn assert_scan_count_and_absent(
    store: &KvStore,
    ns: &str,
    expected_count: usize,
    key_prefix_len: usize,
    needle: &[u8],
) {
    let entries = store.scan_for_testing(ns);
    assert_eq!(
        entries.len(),
        expected_count,
        "store namespace {ns:?} must hold exactly {expected_count} entries; got {}",
        entries.len()
    );
    let needle_present = entries
        .iter()
        .any(|(k, _)| k.len() >= key_prefix_len && &k[key_prefix_len..] == needle);
    assert!(
        !needle_present,
        "needle {needle:?} must be absent from store namespace {ns:?}"
    );
}

// Integration tests for the processor-config fixture component.
//
// Verifies the config WIT round-trip through ProcessorComponent::handle:
//   - present key: the published payload equals the configured value.
//   - absent key: the published payload equals "absent".
//   - get_parsed ok: parsed key value published as "ok:<n>".
//   - get_parsed absent: publishes "absent".
//   - get_parsed failure: labeled ProcessingFailed error string published.
//
// The fixture supports both directive-driven invocations (via envelope body
// JSON) and the legacy empty-activation path for backward compatibility.

mod common;

use brenn_wasm::{
    Capability, ProcessorActivation, ProcessorComponent, ProcessorLoadSpec, ProcessorOutcome,
    ProcessorPortWindow,
};
use std::collections::HashMap;
use std::sync::Arc;

const CONFIG_ARTIFACT_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/target/components/brenn_processor_config.wasm"
);

fn config_artifact() -> std::path::PathBuf {
    std::path::PathBuf::from(CONFIG_ARTIFACT_PATH)
}

/// Build a ProcessorComponent with an "out" port bound to "brenn:config-test-out"
/// and the given config map.
///
/// This fixture uses no store (`store_path: None`) — no tempfile is returned.
fn load_config_component(config: HashMap<String, String>) -> ProcessorComponent {
    let mut ports = HashMap::new();
    ports.insert("out".to_string(), common::out_spec("brenn:config-test-out"));
    // processor-config imports: types + ports + config
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &config_artifact(),
        slug: "config-test",
        output_ports: ports,
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: HashMap::new(),
        config,
        grants: [Capability::Ports, Capability::Config]
            .into_iter()
            .collect(),
        store_path: None,
        max_page_count: brenn_wasm::store::DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: Arc::new(common::NoopAlerter),
        output_acl: common::allow_all(),
        mqtt_publish: None,
        tool_host: None,
    })
}

/// Build an empty activation (no port windows — hits the legacy path in the fixture).
fn empty_activation() -> ProcessorActivation {
    ProcessorActivation { ports: vec![] }
}

/// Build a directive envelope JSON string.
fn directive_envelope(directive: serde_json::Value) -> String {
    let body_str = directive.to_string();
    let body_escaped = serde_json::to_string(&body_str).unwrap();
    format!(
        r#"{{"message_id":"00000000-0000-0000-0000-000000000001","source":"test","channel":"brenn:test","sender":"test-sender","publish_ts":"2026-01-01T00:00:00Z","body":{body_escaped},"urgency":"normal","envelope_type":"brenn"}}"#
    )
}

/// Build a single-envelope activation on port "in".
fn single_activation(envelope: String) -> ProcessorActivation {
    ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![envelope],
            new_from: 0,
            dropped: 0,
        }],
    }
}

// ── Present key ───────────────────────────────────────────────────────────────
//
// Self-defeating property: if config::get always returns None, the component
// publishes "absent" instead of the configured value, and the assertion fails.

#[test]
fn config_present_key_published_as_payload() {
    let mut config = HashMap::new();
    config.insert("test-key".to_string(), "hello-from-config".to_string());
    let comp = load_config_component(config);

    let outcome = comp.handle(empty_activation());
    let publishes = match outcome {
        ProcessorOutcome::Ok(p) => p,
        other => panic!("expected Ok, got: {other:?}"),
    };
    assert_eq!(
        publishes.len(),
        1,
        "expected exactly one publish; got: {publishes:?}"
    );
    assert_eq!(publishes[0].port, "out", "publish must be on port 'out'");
    assert_eq!(
        publishes[0].payload, "hello-from-config",
        "published payload must equal the configured value"
    );
    assert_eq!(
        publishes[0].channel_address, "brenn:config-test-out",
        "channel address must be the resolved port address"
    );
}

// ── Absent key ────────────────────────────────────────────────────────────────
//
// Self-defeating property: if config::get returns Some(_) for absent keys, or if
// the absent branch is unreachable, the payload would not be "absent".

#[test]
fn config_absent_key_publishes_absent() {
    // Empty config — "test-key" is not set.
    let comp = load_config_component(HashMap::new());

    let outcome = comp.handle(empty_activation());
    let publishes = match outcome {
        ProcessorOutcome::Ok(p) => p,
        other => panic!("expected Ok, got: {other:?}"),
    };
    assert_eq!(
        publishes.len(),
        1,
        "expected exactly one publish; got: {publishes:?}"
    );
    assert_eq!(
        publishes[0].payload, "absent",
        "absent key must publish the literal 'absent'"
    );
}

// ── get_parsed: present, parseable key ───────────────────────────────────────
//
// Self-defeating property: if get_parsed returns None for a present key, or fails
// to parse a valid u32, the payload would not be "ok:42".

#[test]
fn config_get_parsed_present_ok() {
    let mut config = HashMap::new();
    config.insert("parsed-key".to_string(), "42".to_string());
    let comp = load_config_component(config);

    let envelope =
        directive_envelope(serde_json::json!({"cmd": "get_parsed", "key": "parsed-key"}));
    let outcome = comp.handle(single_activation(envelope));
    let publishes = match outcome {
        ProcessorOutcome::Ok(p) => p,
        other => panic!("expected Ok, got: {other:?}"),
    };
    assert_eq!(publishes.len(), 1, "expected exactly one publish");
    assert_eq!(
        publishes[0].payload, "ok:42",
        "parsed u32 must be published as 'ok:42'"
    );
}

// ── get_parsed: absent key ────────────────────────────────────────────────────
//
// Self-defeating property: if get_parsed returns Some for an absent key, the
// payload would not be "absent".

#[test]
fn config_get_parsed_absent_key_publishes_absent() {
    let comp = load_config_component(HashMap::new());

    let envelope =
        directive_envelope(serde_json::json!({"cmd": "get_parsed", "key": "parsed-key"}));
    let outcome = comp.handle(single_activation(envelope));
    let publishes = match outcome {
        ProcessorOutcome::Ok(p) => p,
        other => panic!("expected Ok, got: {other:?}"),
    };
    assert_eq!(publishes.len(), 1, "expected exactly one publish");
    assert_eq!(
        publishes[0].payload, "absent",
        "absent key must publish 'absent'"
    );
}

// ── get_parsed: parse failure ─────────────────────────────────────────────────
//
// Self-defeating property: if get_parsed silently ignores parse errors or returns
// Ok on an unparseable value, the activation would not return ProcessingFailed.
//
// Design §2.2: get_parsed returns Err(ProcessingFailed("config {key}: {e}")) on
// parse failure.

#[test]
fn config_get_parsed_bad_value_returns_labeled_error() {
    let mut config = HashMap::new();
    config.insert("parsed-key".to_string(), "not-a-number".to_string());
    let comp = load_config_component(config);

    let envelope =
        directive_envelope(serde_json::json!({"cmd": "get_parsed", "key": "parsed-key"}));
    let outcome = comp.handle(single_activation(envelope));
    // The fixture propagates the error from get_parsed, so the activation returns Err.
    match &outcome {
        ProcessorOutcome::Err(e) => {
            let diag = format!("{e:?}");
            assert!(
                diag.contains("config parsed-key:"),
                "error must be labeled 'config parsed-key:'; got: {diag}"
            );
        }
        other => panic!("expected Err(processing-failed), got: {other:?}"),
    }
}

// ── config::require: present key ──────────────────────────────────────────────
//
// Self-defeating property: if require() returns Err on a present key, or fails
// to return the value, the test would not see "ok:<value>".

#[test]
fn config_require_present_ok() {
    let mut config = HashMap::new();
    config.insert("req-key".to_string(), "required-value".to_string());
    let comp = load_config_component(config);

    let envelope = directive_envelope(serde_json::json!({"cmd": "require", "key": "req-key"}));
    let outcome = comp.handle(single_activation(envelope));
    let publishes = match outcome {
        ProcessorOutcome::Ok(p) => p,
        other => panic!("expected Ok, got: {other:?}"),
    };
    assert_eq!(publishes.len(), 1, "expected exactly one publish");
    assert_eq!(
        publishes[0].payload, "ok:required-value",
        "require on present key must publish 'ok:<value>'"
    );
}

// ── config::require: absent key → labeled ProcessingFailed ───────────────────
//
// Self-defeating property: if require() silently returns Ok(None) or the wrong
// error label for an absent key, the test catches the regression.
// Design §2.2: require absent-key → Err(ProcessingFailed("config {key}: missing")).

#[test]
fn config_require_absent_key_returns_labeled_error() {
    // Empty config — "req-key" is not set.
    let comp = load_config_component(HashMap::new());

    let envelope = directive_envelope(serde_json::json!({"cmd": "require", "key": "req-key"}));
    let outcome = comp.handle(single_activation(envelope));
    match &outcome {
        ProcessorOutcome::Err(e) => {
            let diag = format!("{e:?}");
            assert!(
                diag.contains("config req-key: missing"),
                "absent require must produce 'config req-key: missing'; got: {diag}"
            );
        }
        other => panic!("expected Err(processing-failed), got: {other:?}"),
    }
}

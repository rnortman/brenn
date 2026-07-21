// Integration tests for the WASM MQTT-egress path (design §4 "WASM egress",
// first acceptance test: guest calls `mqtt-publish` naming an authorized client,
// the host callback reports a broker error, and the guest propagates it back).
//
// Uses the `processor-mqtt-test` raw-bindings fixture component: on any
// activation with ≥1 new envelope, the component calls
// `mqtt_publish("test-client", "test/topic", payload, None, 0, false)` and
// maps the returned `MqttPublishError` to
// `Err(ReceiveError::ProcessingFailed("ErrorVariant:detail"))`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION;
use brenn_wasm::{
    Capability, MqttPublishFn, MqttPublishOutcome, ProcessorActivation, ProcessorComponent,
    ProcessorLoadSpec, ProcessorOutcome, ProcessorPortWindow, store::DEFAULT_MAX_PAGE_COUNT,
};

mod common;

fn component_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/components/brenn_processor_mqtt_test.wasm")
}

fn make_envelope() -> String {
    r#"{"message_id":"00000000-0000-0000-0000-000000000001","source":"test","channel":"brenn:test","sender":"test-sender","publish_ts":"2026-01-01T00:00:00Z","body":"ping","urgency":"normal","envelope_type":"brenn"}"#.to_string()
}

fn single_activation() -> ProcessorActivation {
    ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![make_envelope()],
            new_from: 0,
            dropped: 0,
        }],
    }
}

/// An envelope whose body carries the `TRAP_AFTER_PUBLISH` sentinel the fixture
/// keys on to take the publish-once-then-panic path (design §3.1 / §4 final
/// acceptance test).
fn trap_after_publish_envelope() -> String {
    r#"{"message_id":"00000000-0000-0000-0000-000000000002","source":"test","channel":"brenn:test","sender":"test-sender","publish_ts":"2026-01-01T00:00:00Z","body":"TRAP_AFTER_PUBLISH","urgency":"normal","envelope_type":"brenn"}"#.to_string()
}

fn trap_after_publish_activation() -> ProcessorActivation {
    ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![trap_after_publish_envelope()],
            new_from: 0,
            dropped: 0,
        }],
    }
}

/// Load the mqtt-test fixture with a given `mqtt_publish` callback.
fn load_mqtt_test(mqtt_publish: MqttPublishFn) -> ProcessorComponent {
    let grants: BTreeSet<Capability> = [Capability::Mqtt].into_iter().collect();
    ProcessorComponent::load(ProcessorLoadSpec {
        component_path: &component_path(),
        slug: "mqtt-test",
        output_ports: std::collections::HashMap::new(),
        input_amplification_mt: common::amp_in(),
        mqtt_sinks: std::collections::HashMap::new(),
        config: std::collections::HashMap::new(),
        grants,
        store_path: None,
        max_page_count: DEFAULT_MAX_PAGE_COUNT,
        max_payload_bytes: 1024 * 1024,
        alerter: common::noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: Some(mqtt_publish),
        tool_host: None,
    })
}

// ── test: authorized client reaches broker layer ──────────────────────────────

/// Guest calls `mqtt-publish`; host callback returns `Broker("test-broker-error")`;
/// guest propagates it as `ProcessingFailed("Broker:test-broker-error")`.
///
/// This is the §4 "WASM egress" first acceptance test: proves the full host path
/// (linked `mqtt` interface → `do_mqtt_publish` → bootstrap closure) is wired
/// end-to-end through a real WASM activation and that the guest receives and
/// propagates the broker outcome inline.
#[test]
fn mqtt_publish_broker_error_propagates_to_guest() {
    let callback: MqttPublishFn =
        Arc::new(|_client, _topic, _payload, _content_type, _qos, _retain| {
            MqttPublishOutcome::Broker("test-broker-error".to_string())
        });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(single_activation());

    match outcome {
        ProcessorOutcome::Err(re) => {
            let msg = re.to_string();
            assert!(
                msg.contains("Broker:test-broker-error"),
                "expected ProcessingFailed containing 'Broker:test-broker-error', got: {msg:?}"
            );
        }
        other => panic!("expected ProcessorOutcome::Err; got {other:?}"),
    }
}

// ── test: ACL deny → not-permitted (design §4 "WASM egress", 2nd acceptance test) ──

/// Guest calls `mqtt-publish` for a client its `mqtt_publish` ACL does not
/// authorize; the host callback (which in production is the `bootstrap` closure
/// over `enforce_and_publish`, returning `MqttEgressError::AclDenied` →
/// `MqttPublishOutcome::NotPermitted` for an unlisted client) returns
/// `NotPermitted`; the guest receives `not-permitted` and propagates it as
/// `ProcessingFailed("NotPermitted:")`.
///
/// This is the §4 "WASM egress" second acceptance test. The host `warn!` carrying
/// the consumer slug on this path (design §3.3 — an ACL deny on the WASM path must
/// surface server-side, parity with the LLM intercept) fires in `do_mqtt_publish`
/// before the WIT mapping; it is unit-pinned by
/// `do_mqtt_publish_maps_each_outcome` (lib tests). Here we assert the observable
/// end-to-end seam: a real guest activation receives the `not-permitted` variant.
#[test]
fn mqtt_publish_not_permitted_propagates_to_guest() {
    let callback: MqttPublishFn =
        Arc::new(|_client, _topic, _payload, _content_type, _qos, _retain| {
            MqttPublishOutcome::NotPermitted
        });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(single_activation());

    match outcome {
        ProcessorOutcome::Err(re) => {
            let msg = re.to_string();
            assert!(
                msg.contains("NotPermitted:"),
                "expected ProcessingFailed containing 'NotPermitted:', got: {msg:?}"
            );
        }
        other => panic!("expected ProcessorOutcome::Err; got {other:?}"),
    }
}

// ── test: no connector → no-connector (design §4 "WASM egress", 3rd acceptance test) ──

/// Guest calls `mqtt-publish` for a client it is authorized to reach but for
/// which no egress connector is configured; the host callback (in production the
/// `bootstrap` closure over `enforce_and_publish`, returning
/// `MqttEgressError::NoConnector` → `MqttPublishOutcome::NoConnector` when
/// `resolve_by_app_client` finds none) returns `NoConnector`; the guest receives
/// `no-connector` and propagates it as `ProcessingFailed("NoConnector:")`.
///
/// This is the §4 "WASM egress" third acceptance test. It is distinct from the
/// `not-permitted` case (§3.5: connector presence is connection definition, never
/// authorization — ACL is checked before connector resolution): here ACL passes
/// and the failure is the absence of a connector for the target client. Reuses
/// the `processor-mqtt-test` fixture + `load_mqtt_test` helper.
#[test]
fn mqtt_publish_no_connector_propagates_to_guest() {
    let callback: MqttPublishFn =
        Arc::new(|_client, _topic, _payload, _content_type, _qos, _retain| {
            MqttPublishOutcome::NoConnector
        });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(single_activation());

    match outcome {
        ProcessorOutcome::Err(re) => {
            let msg = re.to_string();
            assert!(
                msg.contains("NoConnector:"),
                "expected ProcessingFailed containing 'NoConnector:', got: {msg:?}"
            );
        }
        other => panic!("expected ProcessorOutcome::Err; got {other:?}"),
    }
}

// ── test: malformed/wildcard topic → invalid-payload (design §4 "WASM egress", 4th acceptance test) ──

/// Guest calls `mqtt-publish` with a wildcard/malformed topic; the host callback
/// (in production the `bootstrap` closure, which builds `mqtt:<client>:<topic>`
/// from the guest-supplied client + topic and runs `parse_topic_name` on it,
/// mapping a parse/wildcard failure to `MqttPublishOutcome::InvalidPayload(reason)`
/// — §2.5) returns `InvalidPayload`; the guest receives `invalid-payload(reason)`
/// and propagates it as `ProcessingFailed("InvalidPayload:{reason}")`.
///
/// This is the §4 "WASM egress" fourth acceptance test. Address validation is the
/// caller's responsibility (§2.2: `parse_topic_name` stays at the call site; the
/// shared `enforce_and_publish` receives an already-validated `MqttAddress`), so on
/// the WASM path the validation lives in the bootstrap closure ahead of
/// `enforce_and_publish` and a failure never reaches the broker. The unit-level
/// outcome→WIT mapping for `InvalidPayload` is pinned by
/// `do_mqtt_publish_maps_each_outcome` (lib tests, increment 6); this test pins the
/// observable end-to-end seam (the variant carries its reason string through to the
/// guest). Reuses the `processor-mqtt-test` fixture + `load_mqtt_test` helper.
#[test]
fn mqtt_publish_invalid_payload_propagates_to_guest() {
    let callback: MqttPublishFn =
        Arc::new(|_client, _topic, _payload, _content_type, _qos, _retain| {
            MqttPublishOutcome::InvalidPayload("wildcard not allowed in publish topic".to_string())
        });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(single_activation());

    match outcome {
        ProcessorOutcome::Err(re) => {
            let msg = re.to_string();
            assert!(
                msg.contains("InvalidPayload:wildcard not allowed in publish topic"),
                "expected ProcessingFailed containing the InvalidPayload reason, got: {msg:?}"
            );
        }
        other => panic!("expected ProcessorOutcome::Err; got {other:?}"),
    }
}

// ── test: trap after a successful publish does not roll back the broker send (design §4 "WASM egress", 6th acceptance test) ──

/// A guest activation that publishes once via `mqtt-publish` (which the host
/// callback fulfils with `Ok`) and then traps. The broker send is synchronous and
/// direct — it has *already happened* by the time the trap aborts the activation,
/// and it is NOT rolled back (§3.1). This is the cardinal semantic difference from
/// `ports.publish`, whose buffered publishes are discarded when the activation
/// returns `Err`/`Trap`.
///
/// This is the §4 "WASM egress" sixth (final) acceptance test. The broker side
/// effect is not retractable, so it cannot be observed *after* the trap; we pin it
/// the only realizable way — a shared counter inside the host `MqttPublishFn`
/// records how many times the callback (the broker round-trip) was invoked.
/// Assertions: the activation traps (`ProcessorOutcome::Trap`), AND the callback
/// fired exactly once. The publish committed to the broker before the trap; the
/// trap did not undo it.
#[test]
fn mqtt_publish_then_trap_does_not_roll_back_broker_send() {
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_cb = Arc::clone(&calls);
    let callback: MqttPublishFn = Arc::new(
        move |_client, _topic, _payload, _content_type, _qos, _retain| {
            calls_cb.fetch_add(1, Ordering::SeqCst);
            MqttPublishOutcome::Ok
        },
    );
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(trap_after_publish_activation());

    match outcome {
        ProcessorOutcome::Trap(_) => {
            // The publish (broker round-trip) committed before the trap and was
            // not rolled back: the callback ran exactly once despite the trap.
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "mqtt-publish callback must fire exactly once even though the \
                 activation later traps — the synchronous broker send is not \
                 retracted (§3.1)"
            );
        }
        other => panic!("expected ProcessorOutcome::Trap after publish; got {other:?}"),
    }
}

// ── test: shared per-activation call-count cap → quota-exceeded (design §4 "WASM egress", 5th acceptance test) ──

/// A guest that publishes via `mqtt-publish` enough times in a single activation
/// to exhaust the host's per-activation call-count budget
/// (`MAX_PUBLISH_CALLS_PER_ACTIVATION`, 512 — §2.5) receives
/// `quota-exceeded`, even when the wired callback would otherwise return `Ok`.
///
/// This is the §4 "WASM egress" fifth acceptance test. The `mqtt-publish` surface
/// shares the same `publish_call_count` budget as `ports.publish` so a hostile
/// guest cannot flood across the combined surface (design §2.5). The budget gate
/// runs *before* the callback inside `do_mqtt_publish` (the host returns
/// `quota-exceeded` on the over-cap call without invoking the callback), so the
/// `Ok`-returning callback here never produces the rejection — the host's cap does.
///
/// The fixture publishes in a bounded loop (`PUBLISH_ATTEMPTS` = cap + 1), stopping
/// at the first error. With an always-`Ok` callback the loop runs until call 513
/// trips the cap, at which point the host returns `quota-exceeded` and the guest
/// propagates `ProcessingFailed("QuotaExceeded:")`.
///
/// The unit-level mapping (the cap is consumed across `ports.publish` +
/// `mqtt-publish`, and an over-cap `mqtt-publish` returns `quota-exceeded`) is
/// pinned by the increment-6 lib tests
/// (`do_mqtt_publish_shares_call_budget_with_do_publish`,
/// `do_mqtt_publish_call_budget_boundary_mqtt_only`); this test pins the observable
/// end-to-end seam via a real WASM activation.
#[test]
fn mqtt_publish_quota_exceeded_propagates_to_guest() {
    // The cap is what bounds a real activation; assert the fixture loop is built to
    // exceed it (guards against the fixture and the host cap drifting apart).
    const {
        assert!(
            MAX_PUBLISH_CALLS_PER_ACTIVATION < 513,
            "fixture PUBLISH_ATTEMPTS (513) must exceed the host per-activation call cap"
        )
    };

    // Count the callback (real broker round-trip) invocations. The host gate runs
    // *before* the callback, so the cap-many calls succeed and the over-cap call is
    // rejected without ever reaching the callback. A correct fixture therefore drives
    // exactly `MAX_PUBLISH_CALLS_PER_ACTIVATION` callbacks before the cap
    // trips. Asserting this (not merely "the outcome is QuotaExceeded") catches a
    // fixture bug where the loop exits early and the rejection comes from some other
    // cause — the substring check alone would still pass in that case (test-4).
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_cb = Arc::clone(&calls);
    let callback: MqttPublishFn = Arc::new(
        move |_client, _topic, _payload, _content_type, _qos, _retain| {
            calls_cb.fetch_add(1, Ordering::SeqCst);
            MqttPublishOutcome::Ok
        },
    );
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(single_activation());

    match outcome {
        ProcessorOutcome::Err(re) => {
            let msg = re.to_string();
            assert!(
                msg.contains("QuotaExceeded:"),
                "expected ProcessingFailed containing 'QuotaExceeded:', got: {msg:?}"
            );
        }
        other => panic!("expected ProcessorOutcome::Err; got {other:?}"),
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        MAX_PUBLISH_CALLS_PER_ACTIVATION,
        "the fixture loop must drive exactly the cap-many successful callbacks before \
         the over-cap call trips quota-exceeded — proving the cap (not an early fixture \
         exit) produced the rejection (test-4)"
    );
}

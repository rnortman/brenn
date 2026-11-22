// Integration tests for the WASM MQTT-egress path (first test: guest calls
// `mqtt-publish` naming an authorized client,
// the host callback reports a broker error, and the guest propagates it back).
//
// Uses the `processor-mqtt-test` SDK fixture component: on any activation
// with ≥1 new envelope, the component publishes the first new message's body
// bytes with `mqtt::try_publish("test-client", "test/topic", payload, None, 0,
// false)` and maps the returned `PublishError` to a processing failure
// `"ErrorVariant:detail"`.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use brenn_activation::{ProcessorActivation, ProcessorPortWindow};
use brenn_budget::MAX_PUBLISH_CALLS_PER_ACTIVATION;
use brenn_wasm::{
    ComponentGrant, MqttPublishFn, MqttPublishOutcome, MqttPublishProps, MqttPublisher, MqttTarget,
    ProcessorComponent, ProcessorLoadSpec, ProcessorOutcome,
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
        deferred: vec![],
        now: None,
        sync: None,
    }
}

/// An envelope whose body carries the `TRAP_AFTER_PUBLISH` sentinel the fixture
/// keys on to take the publish-once-then-panic path.
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
        deferred: vec![],
        now: None,
        sync: None,
    }
}

/// Load the mqtt-test fixture with a given `mqtt_publish` callback, and `out`
/// bound so the hold path's buffered publishes appear in
/// `ProcessorOutcome::Ok`.
fn load_mqtt_test(mqtt_publish: MqttPublishFn) -> ProcessorComponent {
    let grants: BTreeSet<ComponentGrant> = [ComponentGrant::Mqtt, ComponentGrant::Ports]
        .into_iter()
        .collect();
    ProcessorComponent::load(ProcessorLoadSpec {
        input_amplification_mt: common::amp_in(),
        output_ports: HashMap::from([("out".to_string(), common::out_spec("brenn:out"))]),
        declared_out_ports: BTreeSet::from(["out".to_string()]),
        grants,
        alerter: common::noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_publish: Some(mqtt_publish),
        ..ProcessorLoadSpec::minimal(&component_path(), "mqtt-test")
    })
}

// ── test: authorized client reaches broker layer ──────────────────────────────

/// Guest calls `mqtt-publish`; host callback returns `Broker("test-broker-error")`;
/// guest propagates it as `ProcessingFailed("Broker:test-broker-error")`.
///
/// Proves the full host path
/// (linked `mqtt` interface → `do_mqtt_publish` → bootstrap closure) is wired
/// end-to-end through a real WASM activation and that the guest receives and
/// propagates the broker outcome inline.
#[test]
fn mqtt_publish_broker_error_propagates_to_guest() {
    let callback: MqttPublishFn = common::passthrough_mqtt(|_client, _topic, _payload, _props| {
        MqttPublishOutcome::Broker("test-broker-error".to_string())
    });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(single_activation()));

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

// ── test: ACL deny → not-permitted ──

/// Guest calls `mqtt-publish` for a client its `mqtt_publish` ACL does not
/// authorize; the host callback (which in production is the `bootstrap` closure
/// over `enforce_and_publish`, returning `MqttEgressError::AclDenied` →
/// `MqttPublishOutcome::NotPermitted` for an unlisted client) returns
/// `NotPermitted`; the guest receives `not-permitted` and propagates it as
/// `ProcessingFailed("NotPermitted:")`.
///
/// The host `warn!` carrying
/// the consumer slug on this path (an ACL deny on the WASM path must
/// surface server-side, parity with the LLM intercept) fires in `do_mqtt_publish`
/// before the WIT mapping; it is unit-pinned by
/// `do_mqtt_publish_maps_each_outcome` (lib tests). Here we assert the observable
/// end-to-end seam: a real guest activation receives the `not-permitted` variant.
#[test]
fn mqtt_publish_not_permitted_propagates_to_guest() {
    let callback: MqttPublishFn = common::passthrough_mqtt(|_client, _topic, _payload, _props| {
        MqttPublishOutcome::NotPermitted
    });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(single_activation()));

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

// ── test: no connector → no-connector ──

/// Guest calls `mqtt-publish` for a client it is authorized to reach but for
/// which no egress connector is configured; the host callback (in production the
/// `bootstrap` closure over `enforce_and_publish`, returning
/// `MqttEgressError::NoConnector` → `MqttPublishOutcome::NoConnector` when
/// `resolve_by_app_client` finds none) returns `NoConnector`; the guest receives
/// `no-connector` and propagates it as `ProcessingFailed("NoConnector:")`.
///
/// It is distinct from the
/// `not-permitted` case (connector presence is connection definition, never
/// authorization — ACL is checked before connector resolution): here ACL passes
/// and the failure is the absence of a connector for the target client. Reuses
/// the `processor-mqtt-test` fixture + `load_mqtt_test` helper.
#[test]
fn mqtt_publish_no_connector_propagates_to_guest() {
    let callback: MqttPublishFn = common::passthrough_mqtt(|_client, _topic, _payload, _props| {
        MqttPublishOutcome::NoConnector
    });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(single_activation()));

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

// ── test: malformed/wildcard topic → invalid-payload ──

/// Guest calls `mqtt-publish` with a wildcard/malformed topic; the host callback
/// (in production the `bootstrap` closure, which builds `mqtt:<client>:<topic>`
/// from the guest-supplied client + topic and runs `parse_topic_name` on it,
/// mapping a parse/wildcard failure to `MqttPublishOutcome::InvalidPayload(reason)`)
/// returns `InvalidPayload`; the guest receives `invalid-payload(reason)`
/// and propagates it as `ProcessingFailed("InvalidPayload:{reason}")`.
///
/// Address validation is the
/// caller's responsibility (`parse_topic_name` stays at the call site; the
/// shared `enforce_and_publish` receives an already-validated `MqttAddress`), so on
/// the WASM path the validation lives in the bootstrap closure ahead of
/// `enforce_and_publish` and a failure never reaches the broker. The unit-level
/// outcome→WIT mapping for `InvalidPayload` is pinned by
/// `do_mqtt_publish_maps_each_outcome` (lib tests, increment 6); this test pins the
/// observable end-to-end seam (the variant carries its reason string through to the
/// guest). Reuses the `processor-mqtt-test` fixture + `load_mqtt_test` helper.
#[test]
fn mqtt_publish_invalid_payload_propagates_to_guest() {
    let callback: MqttPublishFn = common::passthrough_mqtt(|_client, _topic, _payload, _props| {
        MqttPublishOutcome::InvalidPayload("wildcard not allowed in publish topic".to_string())
    });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(single_activation()));

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

// ── test: trap after a successful publish does not roll back the broker send ──

/// A guest activation that publishes once via `mqtt-publish` (which the host
/// callback fulfils with `Ok`) and then traps. The broker send is synchronous and
/// direct — it has *already happened* by the time the trap aborts the activation,
/// and it is NOT rolled back. This is the cardinal semantic difference from
/// `ports.publish`, whose buffered publishes are discarded when the activation
/// returns `Err`/`Trap`.
///
/// The broker side
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
    let callback: MqttPublishFn =
        common::passthrough_mqtt(move |_client, _topic, _payload, _props| {
            calls_cb.fetch_add(1, Ordering::SeqCst);
            MqttPublishOutcome::Ok
        });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(trap_after_publish_activation()));

    match outcome {
        ProcessorOutcome::Trap(_) => {
            // The publish (broker round-trip) committed before the trap and was
            // not rolled back: the callback ran exactly once despite the trap.
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "mqtt-publish callback must fire exactly once even though the \
                 activation later traps — the synchronous broker send is not \
                 retracted"
            );
        }
        other => panic!("expected ProcessorOutcome::Trap after publish; got {other:?}"),
    }
}

// ── test: shared per-activation call-count cap → quota-exceeded ──

/// A guest that publishes via `mqtt-publish` enough times in a single activation
/// to exhaust the host's per-activation call-count budget
/// (`MAX_PUBLISH_CALLS_PER_ACTIVATION`, 512) receives
/// `quota-exceeded`, even when the wired callback would otherwise return `Ok`.
///
/// The `mqtt-publish` surface
/// shares the same `publish_call_count` budget as `ports.publish` so a hostile
/// guest cannot flood across the combined surface. The budget gate
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
    let callback: MqttPublishFn =
        common::passthrough_mqtt(move |_client, _topic, _payload, _props| {
            calls_cb.fetch_add(1, Ordering::SeqCst);
            MqttPublishOutcome::Ok
        });
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(single_activation()));

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

// ── the guest SDK's own mqtt surface ─────────────────────────────────────────

/// A single envelope as JSON, with the given `body`.
fn envelope_with_body(body: &str) -> String {
    format!(
        r#"{{"message_id":"00000000-0000-0000-0000-000000000003","source":"test","channel":"brenn:test","sender":"test-sender","publish_ts":"2026-01-01T00:00:00Z","body":"{body}","urgency":"normal","envelope_type":"brenn"}}"#
    )
}

fn activation_with_body(body: &str) -> ProcessorActivation {
    ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![envelope_with_body(body)],
            new_from: 0,
            dropped: 0,
        }],
        deferred: vec![],
        now: None,
        sync: None,
    }
}

/// Every argument one `mqtt-publish` call reached the host with.
type PublishArgs = (String, String, Vec<u8>, MqttPublishProps);

/// A callback that records its arguments and reports `outcome`.
///
/// The properties are `_props` in every other test here, so nothing
/// reads what a guest sends it. These paths are about exactly that: the SDK
/// wrappers choose the content type and encode the payload, and a wrong choice
/// is invisible in the outcome.
fn recording_callback(
    outcome: MqttPublishOutcome,
) -> (MqttPublishFn, Arc<std::sync::Mutex<Vec<PublishArgs>>>) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_cb = Arc::clone(&seen);
    let callback: MqttPublishFn = common::passthrough_mqtt(move |client, topic, payload, props| {
        seen_cb
            .lock()
            .expect("the recording callback's lock is never poisoned")
            .push((client, topic, payload, props));
        outcome.clone()
    });
    (callback, seen)
}

/// A callback whose outcome is read from a shared cell at call time, so one loaded
/// component can be driven through several outcomes.
///
/// `ProcessorComponent::load` cranelift-compiles the guest, which dominates the runtime of
/// this suite; a loop that varies only the outcome must not pay for it per iteration.
fn switchable_callback() -> (MqttPublishFn, Arc<std::sync::Mutex<MqttPublishOutcome>>) {
    let cell = Arc::new(std::sync::Mutex::new(MqttPublishOutcome::Ok));
    let cell_cb = Arc::clone(&cell);
    let callback: MqttPublishFn =
        common::passthrough_mqtt(move |_client, _topic, _payload, _props| {
            cell_cb
                .lock()
                .expect("the switchable callback's lock is never poisoned")
                .clone()
        });
    (callback, cell)
}

/// `mqtt::publish_json` sends `application/json` and a body that parses back to
/// the value the guest handed it.
///
/// A wrapper that sent the `Debug` form, or labelled it `text/plain`, would be
/// invisible to every other test: the outcome is `Ok` either way and no other
/// assertion reads the content type.
#[test]
fn publish_json_sends_json_under_the_json_content_type() {
    let (callback, seen) = recording_callback(MqttPublishOutcome::Ok);
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(activation_with_body(
        "PUBLISH_JSON",
    )));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "expected Ok; got {outcome:?}"
    );

    let calls = seen.lock().expect("not poisoned");
    assert_eq!(calls.len(), 1, "{calls:?}");
    let (client, topic, payload, props) = &calls[0];
    assert_eq!(client, "test-client");
    assert_eq!(topic, "test/topic");
    assert_eq!(
        *props,
        MqttPublishProps {
            content_type: Some("application/json".to_string()),
            ..Default::default()
        }
    );
    // The fixture publishes the body it was handed, so the payload is that
    // body as a JSON string — which is the point: it came back out of
    // `serde_json`, not out of a `Debug` rendering.
    let decoded: String =
        serde_json::from_slice(payload).expect("publish_json sends JSON, not a Debug rendering");
    assert_eq!(decoded, "PUBLISH_JSON");
}

/// `mqtt::publish_text` sends the bytes as given, under `text/plain`.
#[test]
fn publish_text_sends_the_body_under_the_text_content_type() {
    let (callback, seen) = recording_callback(MqttPublishOutcome::Ok);
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(activation_with_body(
        "PUBLISH_TEXT",
    )));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "expected Ok; got {outcome:?}"
    );

    let calls = seen.lock().expect("not poisoned");
    assert_eq!(calls.len(), 1, "{calls:?}");
    let (_, _, payload, props) = &calls[0];
    assert_eq!(props.content_type.as_deref(), Some("text/plain"));
    assert_eq!(props.message_expiry_s, None);
    assert_eq!(
        payload.as_slice(),
        b"PUBLISH_TEXT",
        "the text wrapper sends the bytes it was given, unencoded"
    );
}

/// `mqtt::publish_text_expiring` reaches the host through
/// `mqtt-publish-expiring`: the expiry arrives beside the text content type,
/// QoS and retain flag the guest chose.
#[test]
fn publish_text_expiring_sends_the_expiry() {
    let (callback, seen) = recording_callback(MqttPublishOutcome::Ok);
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(activation_with_body(
        "PUBLISH_TEXT_EXPIRING",
    )));
    assert!(
        matches!(outcome, ProcessorOutcome::Ok { .. }),
        "expected Ok; got {outcome:?}"
    );

    let calls = seen.lock().expect("not poisoned");
    assert_eq!(calls.len(), 1, "{calls:?}");
    let (client, topic, payload, props) = &calls[0];
    assert_eq!(client, "test-client");
    assert_eq!(topic, "test/topic");
    assert_eq!(
        *props,
        MqttPublishProps {
            content_type: Some("text/plain".to_string()),
            qos: 1,
            retain: true,
            message_expiry_s: Some(90),
        }
    );
    assert_eq!(payload.as_slice(), b"PUBLISH_TEXT_EXPIRING");
}

/// A zero expiry from a guest is refused by the host as `invalid-payload`
/// before the callback runs.
#[test]
fn publish_expiring_with_zero_expiry_is_invalid_payload() {
    let (callback, seen) = recording_callback(MqttPublishOutcome::Ok);
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(activation_with_body(
        "PUBLISH_ZERO_EXPIRY",
    )));
    match outcome {
        ProcessorOutcome::Err(re) => {
            let msg = re.to_string();
            assert!(
                msg.contains("InvalidPayload:") && msg.contains("message-expiry-s"),
                "unexpected error: {msg}"
            );
        }
        other => panic!("expected ProcessorOutcome::Err; got {other:?}"),
    }
    assert!(
        seen.lock().expect("not poisoned").is_empty(),
        "the callback must not be reached"
    );
}

/// Every publish failure the guest SDK can meet, classified and rendered.
///
/// `is_transient` is the expensive one to get wrong in the retryable direction:
/// the bus has no backpressure, so a guest that believes a permanent rejection
/// may be retried retries it on every activation, forever. Only a broker
/// disconnect is retryable, and this walks the whole variant set to say so.
#[test]
fn the_sdk_classifies_and_renders_every_publish_failure() {
    let (callback, outcome_cell) = switchable_callback();
    let comp = load_mqtt_test(callback);

    for (outcome, expected) in [
        (
            MqttPublishOutcome::NotPermitted,
            "transient=false mqtt publish test-client/test/topic: not-permitted",
        ),
        (
            MqttPublishOutcome::NoConnector,
            "transient=false mqtt publish test-client/test/topic: no-connector",
        ),
        (
            MqttPublishOutcome::InvalidPayload("too big".to_string()),
            "transient=false mqtt publish test-client/test/topic: invalid-payload: too big",
        ),
        (
            MqttPublishOutcome::BrokerRejected("topic refused".to_string()),
            "transient=false mqtt publish test-client/test/topic: broker-rejected: topic refused",
        ),
        (
            MqttPublishOutcome::Broker("disconnected".to_string()),
            "transient=true mqtt publish test-client/test/topic: broker: disconnected",
        ),
    ] {
        *outcome_cell
            .lock()
            .expect("the switchable callback's lock is never poisoned") = outcome.clone();

        let result = comp.handle(common::host_activation(activation_with_body("CLASSIFY")));

        match result {
            ProcessorOutcome::Err(re) => {
                let msg = re.to_string();
                assert!(
                    msg.contains(expected),
                    "{outcome:?}: expected {expected:?}, got {msg:?}"
                );
            }
            other => panic!("{outcome:?}: expected ProcessorOutcome::Err; got {other:?}"),
        }
    }
}

/// The one failure no callback can produce: the host's own per-activation call
/// cap. It is permanent for this activation, and the SDK must not offer it as
/// retryable.
#[test]
fn the_sdk_classifies_a_quota_exhaustion_as_permanent() {
    let (callback, _seen) = recording_callback(MqttPublishOutcome::Ok);
    let comp = load_mqtt_test(callback);

    let outcome = comp.handle(common::host_activation(activation_with_body("CLASSIFY")));

    match outcome {
        ProcessorOutcome::Err(re) => {
            let msg = re.to_string();
            assert!(
                msg.contains("transient=false mqtt publish test-client/test/topic: quota-exceeded"),
                "{msg:?}"
            );
        }
        other => panic!("expected ProcessorOutcome::Err; got {other:?}"),
    }
}

// ── the hold: time stopped in mqtt-publish ───────────────────────────────────

/// What the hold publisher does on each call.
#[derive(Clone)]
struct HoldScript {
    /// Map every client string to `home`, as the host's parser maps the
    /// fixture's `home:<i>` spellings.
    all_to_home: bool,
    sleep: Duration,
    outcome: MqttPublishOutcome,
}

/// A publisher whose behaviour is read from a shared script at call time, so
/// one loaded component serves every hold case: loading compiles the guest,
/// which dominates this suite's runtime.
struct HoldPublisher {
    script: Mutex<HoldScript>,
    published: AtomicUsize,
}

impl HoldPublisher {
    fn script(&self) -> HoldScript {
        self.script
            .lock()
            .expect("the hold script's lock is never poisoned")
            .clone()
    }
}

impl MqttPublisher for HoldPublisher {
    fn resolve(&self, client: &str, topic: &str) -> Result<MqttTarget, String> {
        let client = if self.script().all_to_home {
            "home".to_string()
        } else {
            client.to_string()
        };
        Ok(MqttTarget {
            client,
            topic: topic.to_string(),
        })
    }

    fn publish(
        &self,
        _target: MqttTarget,
        _payload: Vec<u8>,
        _props: MqttPublishProps,
    ) -> MqttPublishOutcome {
        let script = self.script();
        std::thread::sleep(script.sleep);
        self.published.fetch_add(1, Ordering::SeqCst);
        script.outcome
    }
}

/// Fuel no hold case comes near: every case is about wall time.
const HOLD_FUEL: u64 = 1 << 50;

/// The fixture's `HOLD_CALLS`.
const HOLD_CALLS: usize = 20;

/// The scheduling allowance on top of a computed wall-time bound.
const SLACK: Duration = Duration::from_millis(400);

/// One epoch tick: the deadline every hold case runs under.
const ONE_TICK: u64 = 1;

/// What one hold activation produced.
struct Held {
    outcome: ProcessorOutcome,
    wall: Duration,
    published: usize,
}

impl Held {
    /// The two buffered publishes, flushed: `before`, then the tally.
    fn tally(&self) -> &str {
        let ProcessorOutcome::Ok { publishes, .. } = &self.outcome else {
            panic!("expected ProcessorOutcome::Ok; got {:?}", self.outcome);
        };
        let bodies: Vec<&str> = publishes
            .iter()
            .map(|p| p.body.as_text().expect("the fixture publishes text"))
            .collect();
        assert_eq!(bodies.len(), 2, "both buffered publishes flush: {bodies:?}");
        assert_eq!(bodies[0], "before");
        bodies[1]
    }
}

fn run_hold(
    comp: &ProcessorComponent,
    publisher: &HoldPublisher,
    sentinel: &str,
    script: HoldScript,
    budget: Duration,
) -> Held {
    *publisher
        .script
        .lock()
        .expect("the hold script's lock is never poisoned") = script;
    publisher.published.store(0, Ordering::SeqCst);
    let started = Instant::now();
    let outcome = comp.handle_with_mqtt_wait_budget(
        common::host_activation(activation_with_body(sentinel)),
        ONE_TICK,
        HOLD_FUEL,
        budget,
    );
    Held {
        outcome,
        wall: started.elapsed(),
        published: publisher.published.load(Ordering::SeqCst),
    }
}

/// Time stopped in `mqtt-publish` is not charged to the epoch deadline, and the
/// per-client wait budget bounds it instead.
///
/// Every case runs under a one-tick (100 ms) deadline, which each outlasts in
/// wall time: an activation charged for its broker waits would trap and flush
/// nothing.
#[test]
fn the_hold_in_mqtt_publish_is_uncharged_and_bounded_per_client() {
    let publisher = Arc::new(HoldPublisher {
        script: Mutex::new(HoldScript {
            all_to_home: false,
            sleep: Duration::ZERO,
            outcome: MqttPublishOutcome::Ok,
        }),
        published: AtomicUsize::new(0),
    });
    let comp = load_mqtt_test(Arc::clone(&publisher) as MqttPublishFn);
    let spent = |n: usize| {
        format!(
            "ok={n} broker={} other=0 kinds=wait budget spent",
            HOLD_CALLS - n
        )
    };

    // Held past the whole deadline, the guest receives the error and runs on.
    let held = run_hold(
        &comp,
        &publisher,
        "HOLD",
        HoldScript {
            all_to_home: false,
            sleep: Duration::from_millis(300),
            outcome: MqttPublishOutcome::Broker("timed out".to_string()),
        },
        Duration::from_secs(1),
    );
    let tally = held.tally();
    assert!(
        tally.starts_with(&format!("ok=0 broker={HOLD_CALLS} other=0 kinds=timed out")),
        "a broker timeout reaches the guest: {tally}"
    );

    // Each call ok and slow, together several times past the deadline.
    let held = run_hold(
        &comp,
        &publisher,
        "HOLD",
        HoldScript {
            all_to_home: false,
            sleep: Duration::from_millis(50),
            outcome: MqttPublishOutcome::Ok,
        },
        Duration::from_secs(5),
    );
    assert_eq!(
        held.tally(),
        format!("ok={HOLD_CALLS} broker=0 other=0 kinds="),
        "{held_wall:?} of slow successes is not charged",
        held_wall = held.wall
    );
    assert_eq!(held.published, HOLD_CALLS);

    // The worst case when no call times out: the budget is crossed during some
    // call, that call finishes, and every later one is refused at once. A sleep
    // is never shorter than asked and may overrun, so the count is a range.
    let budget = Duration::from_millis(250);
    let call = Duration::from_millis(100);
    let held = run_hold(
        &comp,
        &publisher,
        "HOLD",
        HoldScript {
            all_to_home: false,
            sleep: call,
            outcome: MqttPublishOutcome::Ok,
        },
        budget,
    );
    let n = held.published;
    assert!((1..=3).contains(&n), "publish ran {n} times");
    assert_eq!(held.tally(), spent(n));
    assert!(
        held.wall < budget + call + SLACK,
        "held {:?} for a {budget:?} budget",
        held.wall
    );

    // The worst case when a call times out: one call spends the whole budget.
    let held = run_hold(
        &comp,
        &publisher,
        "HOLD",
        HoldScript {
            all_to_home: false,
            sleep: Duration::from_millis(300),
            outcome: MqttPublishOutcome::Broker("timed out".to_string()),
        },
        Duration::from_millis(300),
    );
    assert_eq!(held.published, 1, "the one timed-out call spent the budget");
    assert_eq!(
        held.tally(),
        format!("ok=0 broker={HOLD_CALLS} other=0 kinds=timed out|wait budget spent")
    );

    // Respelling the client buys no time: twenty client strings, one session,
    // one budget.
    let held = run_hold(
        &comp,
        &publisher,
        "HOLD_SPELL",
        HoldScript {
            all_to_home: true,
            sleep: call,
            outcome: MqttPublishOutcome::Ok,
        },
        budget,
    );
    let n = held.published;
    assert!(
        (1..=3).contains(&n),
        "publish ran {n} times across {HOLD_CALLS} spellings of one client"
    );
    assert_eq!(held.tally(), spent(n));
    assert!(
        held.wall < budget + call + SLACK,
        "held {:?} for a {budget:?} budget",
        held.wall
    );

    // A guest that burns its own running time still traps.
    let held = run_hold(
        &comp,
        &publisher,
        "HOLD_SPIN",
        HoldScript {
            all_to_home: false,
            sleep: Duration::ZERO,
            outcome: MqttPublishOutcome::Ok,
        },
        budget,
    );
    assert!(
        matches!(held.outcome, ProcessorOutcome::Trap(_)),
        "a spinning guest traps on its own time; got {:?}",
        held.outcome
    );
}

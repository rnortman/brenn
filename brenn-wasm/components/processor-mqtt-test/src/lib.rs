// MQTT-egress test fixture for the `brenn:processor` world (design §4 "WASM egress").
//
// This component calls `mqtt-publish` in a loop on every activation that contains
// at least one new envelope, using the envelope body as the MQTT payload.  It
// surfaces the host-reported outcome back to the test runner via a typed
// `ReceiveError`:
//
//   - Host returns `Ok`  → keep publishing (up to a bounded cap), then return
//     `Ok(())` if every call succeeded.
//   - Host returns an error variant → stop immediately and return
//     `Err(ReceiveError::ProcessingFailed(format!("{error_name}:{detail}")))`.
//
// The bounded loop (PUBLISH_ATTEMPTS, one past the host's shared per-activation
// call-count cap of 512) lets one fixture serve two shapes of test:
//
//   - Single-call error tests wire a callback that errors on the *first* call;
//     the loop stops at iteration 1 and reports that variant. Behaviour is
//     identical to a single publish.
//   - The quota-exceeded test wires a callback that always returns `Ok`; the loop
//     drives enough `mqtt-publish` calls that the host's own per-activation
//     call-count budget (`PROCESSOR_MAX_PUBLISH_CALLS_PER_ACTIVATION`) trips and
//     the host returns `quota-exceeded` *before* invoking the callback — proving
//     the cap is enforced on the synchronous MQTT surface end-to-end.
//
// A third shape (design §3.1 / §4 "WASM egress", trap-after-publish-no-rollback):
// when the first new envelope's body contains the sentinel `TRAP_AFTER_PUBLISH`,
// the fixture calls `mqtt-publish` exactly once (expecting `Ok`) and then panics,
// which the host converts to `ProcessorOutcome::Trap`. Because MQTT egress is
// synchronous and direct-to-broker (NOT the buffered `ports.publish` path), the
// publish has already reached the broker by the time the trap aborts the
// activation — and is NOT rolled back. The test observes this via a shared
// counter in the host callback: the callback is invoked once even though the
// activation traps.
//
// Three further shapes route through the guest SDK (`brenn_guest::mqtt`) rather
// than the raw import, keyed on their own body sentinels: `PUBLISH_JSON` and
// `PUBLISH_TEXT` send once through the wrappers that choose a content type, so
// the host reads what the SDK chose off the callback's arguments; `CLASSIFY`
// runs the bounded loop through the SDK and reports how it classified and
// rendered the first failure.
//
// The test wires the `mqtt_publish` host callback to return a specific
// `MqttPublishOutcome` variant, then asserts on the `ProcessorOutcome` the host
// sees.  This proves the full host path (linker → `do_mqtt_publish` → bootstrap
// closure → `enforce_and_publish` or stub) is wired end-to-end.
//
// Addressing constants used by the fixture:
//   client : "test-client"
//   topic  : "test/topic"
//   qos    : 0  (fire-and-forget; no broker round-trip delay in tests)
//   retain : false

#[allow(dead_code, clippy::all)]
mod bindings;

use bindings::Guest;
use bindings::brenn::processor::mqtt::{MqttPublishError, mqtt_publish};
use bindings::brenn::processor::types::{Activation, ReceiveError};

struct ProcessorMqttTest;

/// Bounded publish-loop ceiling: one past the host's shared per-activation
/// call-count cap (`PROCESSOR_MAX_PUBLISH_CALLS_PER_ACTIVATION` = 512). An
/// always-`Ok` callback drives the loop until the host's own budget trips and
/// returns `quota-exceeded`; the `+ 1` guarantees the loop reaches the
/// over-cap call rather than exiting clean one short. Single-call error tests
/// never reach iteration 2 (they error on the first call).
const PUBLISH_ATTEMPTS: usize = 513;

/// Sentinel substring in the envelope body that selects the trap-after-publish
/// path (design §3.1 / §4 "WASM egress" final acceptance test): publish exactly
/// once, then panic so the host reports `ProcessorOutcome::Trap`.
const TRAP_AFTER_PUBLISH: &str = "TRAP_AFTER_PUBLISH";

/// Sentinel selecting `brenn_guest::mqtt::publish_json`: serialize the body and
/// send it, so the host callback observes the content type and the payload the
/// SDK chose rather than one this fixture spelled.
const PUBLISH_JSON: &str = "PUBLISH_JSON";

/// Sentinel selecting `brenn_guest::mqtt::publish_text`.
const PUBLISH_TEXT: &str = "PUBLISH_TEXT";

/// Sentinel selecting the classification path: publish through the SDK until
/// something fails, then report how the SDK classified and rendered that
/// failure. The host picks which failure by what its callback returns — and by
/// letting the loop run to the per-activation call cap, which produces the one
/// variant no callback can.
const CLASSIFY: &str = "CLASSIFY";

/// The message inside a guest-SDK error, for reporting it through this
/// fixture's own `ReceiveError` (the SDK's bindings and this crate's are
/// separate generations of the same WIT, so the types do not convert).
fn rendered(e: brenn_guest::Error) -> String {
    match e {
        brenn_guest::Error::MalformedEnvelope(m) => m,
        brenn_guest::Error::ProcessingFailed(m) => m,
    }
}

impl Guest for ProcessorMqttTest {
    fn receive(a: Activation) -> Result<(), ReceiveError> {
        // Only act when there is at least one new envelope.
        let has_new = a
            .ports
            .iter()
            .any(|pw| (pw.new_from as usize) < pw.envelopes.len());
        if !has_new {
            return Ok(());
        }

        // The first new envelope, as a string (for the sentinel check) and bytes
        // (as the MQTT payload).
        let first_new: Option<&String> = a
            .ports
            .iter()
            .find(|pw| (pw.new_from as usize) < pw.envelopes.len())
            .and_then(|pw| pw.envelopes.get(pw.new_from as usize));
        let payload: Vec<u8> = first_new.map(|s| s.as_bytes().to_vec()).unwrap_or_default();

        // Trap-after-publish path: publish ONCE (expecting Ok), then panic. The
        // panic becomes a wasm trap → `ProcessorOutcome::Trap`. The synchronous
        // MQTT publish has already gone to the broker by this point and is not
        // retracted by the trap (§3.1); the test pins this via a host-callback
        // counter that records exactly one invocation despite the trap.
        //
        // SDK wrapper here exercises `brenn_guest::mqtt` end to end; the loop
        // below stays on the raw import because it is about the host's own
        // error variants.
        if first_new.is_some_and(|s| s.contains(TRAP_AFTER_PUBLISH)) {
            match brenn_guest::mqtt::publish("test-client", "test/topic", &payload, None, 0, false)
            {
                Ok(()) => panic!("trap-after-publish: publish succeeded, now trapping"),
                Err(_) => {
                    // The trap-after-publish test wires an always-Ok callback, so
                    // this branch should be unreachable; surface it explicitly
                    // rather than silently swallowing.
                    return Err(ReceiveError::ProcessingFailed(
                        "trap-after-publish: unexpected publish error".to_string(),
                    ));
                }
            }
        }

        // SDK content-type paths. Each sends exactly once and returns Ok, so
        // the host test reads the callback's captured arguments rather than an
        // outcome variant: what is under test is the payload and the content
        // type the SDK chose, neither of which any other test observes.
        if first_new.is_some_and(|s| s.contains(PUBLISH_JSON)) {
            let body = first_new.expect("the sentinel came from a body").as_str();
            return match brenn_guest::mqtt::publish_json(
                "test-client",
                "test/topic",
                &body,
                0,
                false,
            ) {
                Ok(()) => Ok(()),
                Err(e) => Err(ReceiveError::ProcessingFailed(rendered(e))),
            };
        }
        if first_new.is_some_and(|s| s.contains(PUBLISH_TEXT)) {
            let body = first_new.expect("the sentinel came from a body").as_str();
            return match brenn_guest::mqtt::publish_text(
                "test-client",
                "test/topic",
                body,
                0,
                false,
            ) {
                Ok(()) => Ok(()),
                Err(e) => Err(ReceiveError::ProcessingFailed(rendered(e))),
            };
        }

        // Classification path: the same bounded loop, through the SDK. The
        // first failure is reported as the SDK saw it — whether a later
        // activation may retry it, and the diagnostic the SDK renders — so the
        // host test pins `is_transient` and the error rendering against a real
        // host outcome instead of trusting the guest crate's own reading of its
        // variants.
        if first_new.is_some_and(|s| s.contains(CLASSIFY)) {
            for _ in 0..PUBLISH_ATTEMPTS {
                let Err(e) = brenn_guest::mqtt::try_publish(
                    "test-client",
                    "test/topic",
                    &payload,
                    None,
                    0,
                    false,
                ) else {
                    continue;
                };
                let transient = brenn_guest::mqtt::is_transient(&e);
                // A second call for the rendered form: the flattening wrapper
                // keeps no variant, and the callback answers the same way
                // twice.
                let message = match brenn_guest::mqtt::publish(
                    "test-client",
                    "test/topic",
                    &payload,
                    None,
                    0,
                    false,
                ) {
                    Ok(()) => String::from("second call unexpectedly succeeded"),
                    Err(e) => rendered(e),
                };
                return Err(ReceiveError::ProcessingFailed(format!(
                    "transient={transient} {message}"
                )));
            }
            return Ok(());
        }

        // Publish repeatedly, stopping at the first error. A callback that errors
        // on the first call reports that variant (single-call tests); an always-Ok
        // callback runs until the host's per-activation call-count cap returns
        // `quota-exceeded` (the quota test).
        for _ in 0..PUBLISH_ATTEMPTS {
            match mqtt_publish("test-client", "test/topic", &payload, None, 0, false) {
                Ok(()) => continue,
                Err(e) => {
                    // Encode the error name + detail into ProcessingFailed so the
                    // test can assert on the specific variant the host returned.
                    let msg = match e {
                        MqttPublishError::NotPermitted => "NotPermitted:".to_string(),
                        MqttPublishError::NoConnector => "NoConnector:".to_string(),
                        MqttPublishError::InvalidPayload(s) => format!("InvalidPayload:{s}"),
                        MqttPublishError::QuotaExceeded => "QuotaExceeded:".to_string(),
                        MqttPublishError::Broker(s) => format!("Broker:{s}"),
                        MqttPublishError::BrokerRejected(s) => format!("BrokerRejected:{s}"),
                    };
                    return Err(ReceiveError::ProcessingFailed(msg));
                }
            }
        }
        // Every publish succeeded (always-Ok callback that somehow never tripped
        // the cap — should not happen given PUBLISH_ATTEMPTS > the cap).
        Ok(())
    }
}

bindings::export!(ProcessorMqttTest with_types_in bindings);

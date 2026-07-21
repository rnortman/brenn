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

use bindings::brenn::processor::mqtt::{MqttPublishError, mqtt_publish};
use bindings::brenn::processor::types::{Activation, ReceiveError};
use bindings::Guest;

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
        if first_new.is_some_and(|s| s.contains(TRAP_AFTER_PUBLISH)) {
            match mqtt_publish("test-client", "test/topic", &payload, None, 0, false) {
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

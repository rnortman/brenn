// MQTT-egress test fixture, built on the guest SDK.
//
// This component publishes over MQTT in a loop on every activation that
// contains at least one new message, using that message's body bytes as the
// MQTT payload. It surfaces the host-reported outcome back to the test runner
// through its error:
//
//   - Host returns `Ok`  → keep publishing (up to a bounded cap), then return
//     `Ok(None)` if every call succeeded.
//   - Host returns an error variant → stop immediately and fail the activation
//     with `"{error_name}:{detail}"`.
//
// The loop goes through `brenn_guest::mqtt::try_publish`, which keeps the
// host's own variant. The bounded loop (PUBLISH_ATTEMPTS, one past the host's
// shared per-activation call-count cap of 512) lets one fixture serve two
// shapes of test:
//
//   - Single-call error tests wire a callback that errors on the *first* call;
//     the loop stops at iteration 1 and reports that variant. Behaviour is
//     identical to a single publish.
//   - The quota-exceeded test wires a callback that always returns `Ok`; the loop
//     drives enough MQTT publishes that the host's own per-activation
//     call-count budget (`PROCESSOR_MAX_PUBLISH_CALLS_PER_ACTIVATION`) trips and
//     the host returns `quota-exceeded` *before* invoking the callback — proving
//     the cap is enforced on the synchronous MQTT surface end-to-end.
//
// A third shape:
// when the first new message's body contains the sentinel `TRAP_AFTER_PUBLISH`,
// the fixture publishes exactly once (expecting `Ok`) and then panics, which
// the host converts to `ProcessorOutcome::Trap`. Because MQTT egress is
// synchronous and direct-to-broker (NOT the buffered `ports.publish` path), the
// publish has already reached the broker by the time the trap aborts the
// activation — and is NOT rolled back. The test observes this via a shared
// counter in the host callback: the callback is invoked once even though the
// activation traps.
//
// Three further shapes are keyed on their own body sentinels: `PUBLISH_JSON`
// and `PUBLISH_TEXT` send the body once through the wrappers that choose a
// content type, so the host reads what the SDK chose off the callback's
// arguments (`PUBLISH_TEXT_EXPIRING` and `PUBLISH_ZERO_EXPIRY` do the same
// through the expiring wrappers, the second against the host's zero-expiry
// refusal); `CLASSIFY` runs the bounded loop and reports how the SDK
// classified and rendered the first failure.
//
// Addressing constants used by the fixture:
//   client : "test-client"
//   topic  : "test/topic"
//   qos    : 0  (fire-and-forget; no broker round-trip delay in tests)
//   retain : false

use brenn_guest::mqtt::{self, PublishError};
use brenn_guest::{Activation, Body, Error, Processor};

struct ProcessorMqttTest;

/// Bounded publish-loop ceiling: one past the host's shared per-activation
/// call-count cap (`PROCESSOR_MAX_PUBLISH_CALLS_PER_ACTIVATION` = 512). An
/// always-`Ok` callback drives the loop until the host's own budget trips and
/// returns `quota-exceeded`; the `+ 1` guarantees the loop reaches the
/// over-cap call rather than exiting clean one short. Single-call error tests
/// never reach iteration 2 (they error on the first call).
const PUBLISH_ATTEMPTS: usize = 513;

/// Sentinel substring in the body that selects the trap-after-publish path:
/// publish exactly once, then panic so the host reports
/// `ProcessorOutcome::Trap`.
const TRAP_AFTER_PUBLISH: &str = "TRAP_AFTER_PUBLISH";

/// Sentinel selecting `brenn_guest::mqtt::publish_json`: serialize the body
/// text and send it, so the host callback observes the content type and the
/// payload the SDK chose rather than one this fixture spelled.
const PUBLISH_JSON: &str = "PUBLISH_JSON";

/// Sentinel selecting `brenn_guest::mqtt::publish_text`.
const PUBLISH_TEXT: &str = "PUBLISH_TEXT";

/// Sentinel selecting `brenn_guest::mqtt::publish_text_expiring`, retained,
/// QoS 1, with a 90-second message expiry. Checked before `PUBLISH_TEXT`, which
/// it contains.
const PUBLISH_TEXT_EXPIRING: &str = "PUBLISH_TEXT_EXPIRING";

/// Sentinel selecting `brenn_guest::mqtt::try_publish_expiring` with a zero
/// expiry, which the host refuses; the failure is reported by its rendering.
const PUBLISH_ZERO_EXPIRY: &str = "PUBLISH_ZERO_EXPIRY";

/// Sentinel selecting the classification path: publish through the SDK until
/// something fails, then report how the SDK classified and rendered that
/// failure. The host picks which failure by what its callback returns — and by
/// letting the loop run to the per-activation call cap, which produces the one
/// variant no callback can.
const CLASSIFY: &str = "CLASSIFY";

impl Processor for ProcessorMqttTest {
    fn receive(a: Activation) -> Result<Option<Body>, Error> {
        let Some(first_new) = a.port_windows().find_map(|w| w.new_messages().first()) else {
            return Ok(None);
        };
        let payload = first_new.body().as_bytes();
        // The sentinels are text; a non-text body carries none.
        let text = first_new.body().as_text().ok();
        let has = |sentinel: &str| text.is_some_and(|t| t.contains(sentinel));

        // Trap-after-publish path: publish ONCE (expecting Ok), then panic. The
        // panic becomes a wasm trap → `ProcessorOutcome::Trap`. The synchronous
        // MQTT publish has already gone to the broker by this point and is not
        // retracted by the trap; the test pins this via a host-callback
        // counter that records exactly one invocation despite the trap.
        if has(TRAP_AFTER_PUBLISH) {
            return match mqtt::publish("test-client", "test/topic", payload, None, 0, false) {
                Ok(()) => panic!("trap-after-publish: publish succeeded, now trapping"),
                // The trap-after-publish test wires an always-Ok callback, so
                // this branch should be unreachable; surface it explicitly
                // rather than silently swallowing.
                Err(_) => Err(Error::failed(
                    "trap-after-publish: unexpected publish error",
                )),
            };
        }

        // SDK content-type paths. Each sends exactly once and returns Ok, so
        // the host test reads the callback's captured arguments rather than an
        // outcome variant: what is under test is the payload and the content
        // type the SDK chose, neither of which any other test observes.
        if has(PUBLISH_JSON) {
            let body = text.expect("the sentinel came from a text body");
            mqtt::publish_json("test-client", "test/topic", &body, 0, false)?;
            return Ok(None);
        }
        if has(PUBLISH_TEXT_EXPIRING) {
            let body = text.expect("the sentinel came from a text body");
            mqtt::publish_text_expiring("test-client", "test/topic", body, 1, true, 90)?;
            return Ok(None);
        }
        if has(PUBLISH_ZERO_EXPIRY) {
            return match mqtt::try_publish_expiring(
                "test-client",
                "test/topic",
                payload,
                None,
                1,
                true,
                0,
            ) {
                Ok(()) => Ok(None),
                Err(PublishError::InvalidPayload(m)) => {
                    Err(Error::failed(format!("InvalidPayload:{m}")))
                }
                Err(e) => Err(Error::failed(format!("other:{e:?}"))),
            };
        }
        if has(PUBLISH_TEXT) {
            let body = text.expect("the sentinel came from a text body");
            mqtt::publish_text("test-client", "test/topic", body, 0, false)?;
            return Ok(None);
        }

        // Classification path: the same bounded loop. The first failure is
        // reported as the SDK saw it — whether a later activation may retry
        // it, and the diagnostic the SDK renders — so the host test pins
        // `is_transient` and the error rendering against a real host outcome
        // instead of trusting the guest crate's own reading of its variants.
        if has(CLASSIFY) {
            for _ in 0..PUBLISH_ATTEMPTS {
                let Err(e) =
                    mqtt::try_publish("test-client", "test/topic", payload, None, 0, false)
                else {
                    continue;
                };
                let transient = mqtt::is_transient(&e);
                // A second call for the rendered form: the flattening wrapper
                // keeps no variant, and the callback answers the same way
                // twice.
                let message =
                    match mqtt::publish("test-client", "test/topic", payload, None, 0, false) {
                        Ok(()) => String::from("second call unexpectedly succeeded"),
                        Err(e) => e.to_string(),
                    };
                return Err(Error::failed(format!("transient={transient} {message}")));
            }
            return Ok(None);
        }

        // Publish repeatedly, stopping at the first error. A callback that errors
        // on the first call reports that variant (single-call tests); an always-Ok
        // callback runs until the host's per-activation call-count cap returns
        // `quota-exceeded` (the quota test).
        for _ in 0..PUBLISH_ATTEMPTS {
            if let Err(e) = mqtt::try_publish("test-client", "test/topic", payload, None, 0, false)
            {
                // Encode the error name + detail so the test can assert on the
                // specific variant the host returned.
                let msg = match e {
                    PublishError::NotPermitted => "NotPermitted:".to_string(),
                    PublishError::NoConnector => "NoConnector:".to_string(),
                    PublishError::InvalidPayload(s) => format!("InvalidPayload:{s}"),
                    PublishError::QuotaExceeded => "QuotaExceeded:".to_string(),
                    PublishError::Broker(s) => format!("Broker:{s}"),
                    PublishError::BrokerRejected(s) => format!("BrokerRejected:{s}"),
                };
                return Err(Error::failed(msg));
            }
        }
        // Every publish succeeded (always-Ok callback that somehow never tripped
        // the cap — should not happen given PUBLISH_ATTEMPTS > the cap).
        Ok(None)
    }
}

brenn_guest::export_processor!(ProcessorMqttTest);

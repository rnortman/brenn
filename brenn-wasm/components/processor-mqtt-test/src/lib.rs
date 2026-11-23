// MQTT-egress test fixture, built on the guest SDK.
//
// On every activation that contains at least one new message, this component
// makes MQTT publishes chosen by sentinels in that message's body, and reports
// what the host answered through its own outcome. An MQTT publish is buffered
// with the activation: the host commits it with the activation's other
// publishes when the guest returns ok and discards it on an err or trap, so a
// host test reads what was published off `ProcessorOutcome::Ok`.
//
// With no sentinel the fixture publishes the body in a bounded loop
// (`PUBLISH_ATTEMPTS`, one past the host's per-activation call cap) and stops
// at the first refusal, failing the activation with `"{variant}:{detail}"`. A
// host that refuses the first call yields that variant; one that admits every
// call yields `quota-exceeded` from the host's own ceilings.
//
// Sentinel paths:
//
//   - `TRAP_AFTER_PUBLISH` / `ERR_AFTER_PUBLISH`: publish once, then panic or
//     return an error, so the host's discard of the buffered publish shows.
//   - `PUBLISH_JSON`, `PUBLISH_TEXT`, `PUBLISH_TEXT_EXPIRING`: send the body
//     once through the SDK wrapper that chooses the content type.
//     `PUBLISH_ZERO_EXPIRY` sends a zero expiry, which the host refuses.
//   - `CLASSIFY`: the bounded loop, reporting how the SDK classified and
//     rendered the first refusal.
//   - `INTERLEAVE`: buffer a `ports.publish` on `out`, make `INTERLEAVE_CALLS`
//     MQTT publishes whatever each returns, then buffer the tally on `out` —
//     the flush shows call order across the two interfaces.
//     `INTERLEAVE_SPELL` does the same with call `i` naming client `home:<i>`.
//   - `PUBLISH_TO`: one publish whose every argument is in the body (see
//     `publish_to`).
//
// Fixed addressing on every path but `INTERLEAVE_SPELL` and `PUBLISH_TO`:
//   client : "test-client"
//   topic  : "test/topic"
//   qos    : 0, except the expiring path
//   retain : false, except the expiring path

use brenn_guest::mqtt::{self, PublishError};
use brenn_guest::{Activation, Body, Error, Processor};

struct ProcessorMqttTest;

/// Bounded publish-loop ceiling: one past the host's shared per-activation
/// call-count cap (512), so a loop the host keeps admitting always reaches one
/// of the host's own ceilings rather than exiting clean.
const PUBLISH_ATTEMPTS: usize = 513;

/// Sentinel selecting publish-once-then-panic.
const TRAP_AFTER_PUBLISH: &str = "TRAP_AFTER_PUBLISH";

/// Sentinel selecting publish-once-then-return-an-error.
const ERR_AFTER_PUBLISH: &str = "ERR_AFTER_PUBLISH";

/// Sentinel selecting `brenn_guest::mqtt::publish_json`: serialize the body
/// text and send it, so the host observes the content type and the payload the
/// SDK chose rather than one this fixture spelled.
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
/// something is refused, then report how the SDK classified and rendered that
/// refusal.
const CLASSIFY: &str = "CLASSIFY";

/// Sentinel selecting the interleave path with a fixed client string.
const INTERLEAVE: &str = "INTERLEAVE";

/// Sentinel selecting the interleave path with a different client string per
/// call, all of them spellings of `home` under the host's address parser.
/// Checked before `INTERLEAVE`, which it contains.
const INTERLEAVE_SPELL: &str = "INTERLEAVE_SPELL";

/// Sentinel selecting the parameterised single publish.
const PUBLISH_TO: &str = "PUBLISH_TO";

/// How many `mqtt-publish` calls the interleave path makes.
const INTERLEAVE_CALLS: usize = 20;

/// The interleave path: buffer a publish on `out`, make `INTERLEAVE_CALLS` MQTT
/// publishes whatever each returns, then buffer the tally on `out`:
/// `ok=<n> refused=<n> kinds=<distinct refusals, |-separated>`.
fn interleave(spell: bool) -> Result<Option<Body>, Error> {
    brenn_guest::publish("out", "before")?;
    let (mut ok, mut refused) = (0usize, 0usize);
    let mut kinds: Vec<String> = Vec::new();
    for i in 0..INTERLEAVE_CALLS {
        let client = if spell {
            format!("home:{i}")
        } else {
            String::from("test-client")
        };
        let payload = format!("{i}");
        match mqtt::try_publish(&client, "test/topic", payload.as_bytes(), None, 1, false) {
            Ok(()) => ok += 1,
            Err(e) => {
                refused += 1;
                let kind = format!("{e:?}");
                if !kinds.contains(&kind) {
                    kinds.push(kind);
                }
            }
        }
    }
    let tally = format!("ok={ok} refused={refused} kinds={}", kinds.join("|"));
    brenn_guest::publish("out", &tally)?;
    Ok(None)
}

/// The parameterised publish. The body is the sentinel line, then one field per
/// line: client, topic, QoS, retain (`0`/`1`), message expiry in seconds or `-`
/// (`-` selects `mqtt-publish`, a number `mqtt-publish-expiring`), content type
/// or `-`, and the payload as the remainder (which may span lines). Returns ok
/// when the host admits the publish and its refusal's rendering otherwise.
fn publish_to(text: &str) -> Result<Option<Body>, Error> {
    let mut lines = text.splitn(8, '\n');
    let _sentinel = lines.next();
    let mut field = |name: &str| {
        lines
            .next()
            .unwrap_or_else(|| panic!("PUBLISH_TO: missing field {name}"))
    };
    let client = field("client");
    let topic = field("topic");
    let qos: u8 = field("qos").parse().expect("PUBLISH_TO: qos is a number");
    let retain = match field("retain") {
        "0" => false,
        "1" => true,
        other => panic!("PUBLISH_TO: retain must be 0 or 1, got {other:?}"),
    };
    let expiry = match field("expiry") {
        "-" => None,
        n => Some(n.parse::<u32>().expect("PUBLISH_TO: expiry is a number")),
    };
    let content_type = match field("content type") {
        "-" => None,
        ct => Some(ct),
    };
    let payload = lines.next().unwrap_or("");
    let outcome = match expiry {
        None => mqtt::try_publish(client, topic, payload.as_bytes(), content_type, qos, retain),
        Some(expiry) => mqtt::try_publish_expiring(
            client,
            topic,
            payload.as_bytes(),
            content_type,
            qos,
            retain,
            expiry,
        ),
    };
    match outcome {
        Ok(()) => Ok(None),
        Err(e) => Err(Error::failed(format!("{e:?}"))),
    }
}

impl Processor for ProcessorMqttTest {
    fn receive(a: Activation) -> Result<Option<Body>, Error> {
        let Some(first_new) = a.port_windows().find_map(|w| w.new_messages().first()) else {
            return Ok(None);
        };
        let payload = first_new.body().as_bytes();
        // The sentinels are text; a non-text body carries none.
        let text = first_new.body().as_text().ok();
        let has = |sentinel: &str| text.is_some_and(|t| t.contains(sentinel));

        if has(PUBLISH_TO) {
            return publish_to(text.expect("the sentinel came from a text body"));
        }
        if has(INTERLEAVE_SPELL) {
            return interleave(true);
        }
        if has(INTERLEAVE) {
            return interleave(false);
        }

        // Publish once, then fail: the host discards the buffered publish with
        // the activation.
        if has(TRAP_AFTER_PUBLISH) {
            mqtt::publish("test-client", "test/topic", payload, None, 0, false)?;
            panic!("trap-after-publish: publish admitted, now trapping");
        }
        if has(ERR_AFTER_PUBLISH) {
            mqtt::publish("test-client", "test/topic", payload, None, 0, false)?;
            return Err(Error::failed(
                "err-after-publish: publish admitted, now failing",
            ));
        }

        // SDK content-type paths. Each sends exactly once and returns Ok, so
        // the host test reads the buffered publish rather than an outcome
        // variant: what is under test is the payload and the content type the
        // SDK chose.
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

        // Classification path: the same bounded loop. The first refusal is
        // reported as the SDK saw it — whether a later activation may retry
        // it, and the diagnostic the SDK renders — so the host test pins
        // `is_transient` and the error rendering against a real host answer
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
                // keeps no variant, and the host answers the same way twice.
                let message =
                    match mqtt::publish("test-client", "test/topic", payload, None, 0, false) {
                        Ok(()) => String::from("second call unexpectedly succeeded"),
                        Err(e) => e.to_string(),
                    };
                return Err(Error::failed(format!("transient={transient} {message}")));
            }
            return Ok(None);
        }

        // Publish repeatedly, stopping at the first refusal. A host that refuses
        // the first call reports that variant; one that admits every call runs
        // until one of its per-activation ceilings returns `quota-exceeded`.
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
        // Every publish admitted: unreachable on a host whose call cap is under
        // `PUBLISH_ATTEMPTS`.
        Ok(None)
    }
}

brenn_guest::export_processor!(ProcessorMqttTest);

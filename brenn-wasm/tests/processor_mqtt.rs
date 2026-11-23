// Integration tests for the WASM MQTT-egress path: the `processor-mqtt-test`
// SDK fixture, a real activation, and a scripted `MqttPublisher`.
//
// An admitted MQTT publish is buffered with the activation and comes back in
// `ProcessorOutcome::Ok`'s `publishes` as an entry with `mqtt` set, aimed at
// the client's egress channel; an activation that errs or traps returns none.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use brenn_activation::{ProcessorActivation, ProcessorPortWindow};
use brenn_budget::{MAX_PUBLISH_CALLS_PER_ACTIVATION, MAX_PUBLISHES_PER_ACTIVATION};
use brenn_envelope::addressing::mqtt_egress_address;
use brenn_wasm::{
    ComponentGrant, MqttAdmission, MqttPublishFn, MqttPublishProps, MqttPublisher, MqttTarget,
    ProcessorComponent, ProcessorLoadSpec, ProcessorMqttPublish, ProcessorOutcome,
    ProcessorPublish, SinkBudget,
};

mod common;

fn component_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/components/brenn_processor_mqtt_test.wasm")
}

/// One activation whose only new message has `body` as its text.
fn activation_with_body(body: &str) -> ProcessorActivation {
    let envelope = serde_json::json!({
        "message_id": "00000000-0000-0000-0000-000000000003",
        "source": "test",
        "channel": "brenn:test",
        "sender": "test-sender",
        "publish_ts": "2026-01-01T00:00:00Z",
        "body": body,
        "urgency": "normal",
        "envelope_type": "brenn",
    });
    ProcessorActivation {
        ports: vec![ProcessorPortWindow {
            port: "in".to_string(),
            envelopes: vec![envelope.to_string()],
            new_from: 0,
            dropped: 0,
        }],
        deferred: vec![],
        now: None,
        sync: None,
    }
}

/// What one `admit` call saw.
type SeenAdmit = (MqttTarget, usize, MqttPublishProps);

/// A publisher whose admission is read from a shared cell at call time, so one
/// loaded component serves several answers: loading compiles the guest, which
/// dominates this suite's runtime.
///
/// `resolve` maps every `home:<suffix>` client string to `home`, as the host's
/// parser joins and splits the address, and leaves every other one unchanged.
struct ScriptedPublisher {
    admission: Mutex<MqttAdmission>,
    seen: Mutex<Vec<SeenAdmit>>,
}

impl ScriptedPublisher {
    fn new(admission: MqttAdmission) -> Arc<Self> {
        Arc::new(Self {
            admission: Mutex::new(admission),
            seen: Mutex::new(Vec::new()),
        })
    }

    fn answer(&self, admission: MqttAdmission) {
        *self.admission.lock().expect("not poisoned") = admission;
        self.seen.lock().expect("not poisoned").clear();
    }

    fn seen(&self) -> Vec<SeenAdmit> {
        self.seen.lock().expect("not poisoned").clone()
    }
}

impl MqttPublisher for ScriptedPublisher {
    fn resolve(&self, client: &str, topic: &str) -> Result<MqttTarget, String> {
        let client = if client.starts_with("home:") {
            "home"
        } else {
            client
        };
        Ok(MqttTarget {
            client: client.to_string(),
            topic: topic.to_string(),
        })
    }

    fn admit(
        &self,
        target: &MqttTarget,
        payload_len: usize,
        props: &MqttPublishProps,
    ) -> MqttAdmission {
        self.seen
            .lock()
            .expect("not poisoned")
            .push((target.clone(), payload_len, props.clone()));
        self.admission.lock().expect("not poisoned").clone()
    }
}

/// Load the fixture over `publisher`, with `out` bound so the interleave
/// path's port publishes flush, and `mqtt_sinks` as the clients' buckets.
fn load_mqtt_test(
    publisher: &Arc<ScriptedPublisher>,
    mqtt_sinks: HashMap<String, SinkBudget>,
) -> ProcessorComponent {
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
        mqtt_sinks,
        mqtt_publish: Some(Arc::clone(publisher) as MqttPublishFn),
        ..ProcessorLoadSpec::minimal(&component_path(), "mqtt-test")
    })
}

/// The publishes of an ok activation.
fn ok_publishes(outcome: ProcessorOutcome) -> Vec<ProcessorPublish> {
    match outcome {
        ProcessorOutcome::Ok { publishes, .. } => publishes,
        other => panic!("expected ProcessorOutcome::Ok; got {other:?}"),
    }
}

/// The rendering of an activation the guest failed.
fn failure(outcome: ProcessorOutcome) -> String {
    match outcome {
        ProcessorOutcome::Err(re) => re.to_string(),
        other => panic!("expected ProcessorOutcome::Err; got {other:?}"),
    }
}

/// A `PUBLISH_TO` body: the sentinel, then client, topic, QoS, retain, expiry,
/// content type and payload, one per line.
fn publish_to(
    client: &str,
    topic: &str,
    qos: u8,
    retain: bool,
    expiry: Option<u32>,
    content_type: Option<&str>,
    payload: &str,
) -> String {
    format!(
        "PUBLISH_TO\n{client}\n{topic}\n{qos}\n{}\n{}\n{}\n{payload}",
        u8::from(retain),
        expiry.map_or("-".to_string(), |n| n.to_string()),
        content_type.unwrap_or("-"),
    )
}

/// The one buffered publish of an activation, which must be an MQTT entry.
fn only_mqtt_entry(publishes: &[ProcessorPublish]) -> (&ProcessorPublish, &ProcessorMqttPublish) {
    assert_eq!(publishes.len(), 1, "{publishes:?}");
    let entry = &publishes[0];
    (entry, entry.mqtt.as_ref().expect("an mqtt entry"))
}

// ── what an admitted publish buffers ─────────────────────────────────────────

/// `mqtt-publish` and `mqtt-publish-expiring`, each from an activation that
/// returns ok, yield one entry with `mqtt` set: the client's egress channel,
/// the resolved target, the guest's properties, and the payload as the body.
#[test]
fn each_function_buffers_one_mqtt_entry() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());

    let outcome = comp.handle(common::host_activation(activation_with_body(&publish_to(
        "home",
        "sensors/temp",
        1,
        true,
        None,
        Some("application/json"),
        r#"{"c":21}"#,
    ))));
    let publishes = ok_publishes(outcome);
    let (entry, mqtt) = only_mqtt_entry(&publishes);
    let props = MqttPublishProps {
        content_type: Some("application/json".to_string()),
        qos: 1,
        retain: true,
        message_expiry_s: None,
    };
    let target = MqttTarget {
        client: "home".to_string(),
        topic: "sensors/temp".to_string(),
    };
    assert_eq!(
        *mqtt,
        ProcessorMqttPublish {
            target: target.clone(),
            props: props.clone(),
        }
    );
    assert_eq!(entry.channel_address, mqtt_egress_address("home"));
    assert_eq!(entry.body.as_bytes(), br#"{"c":21}"#);
    assert_eq!(entry.reply_to, None);
    assert_eq!(entry.deliver_after, None);
    assert_eq!(publisher.seen(), vec![(target, 8, props)]);

    publisher.answer(MqttAdmission::Admitted);
    let outcome = comp.handle(common::host_activation(activation_with_body(&publish_to(
        "office",
        "lights/state",
        2,
        false,
        Some(90),
        None,
        "on",
    ))));
    let publishes = ok_publishes(outcome);
    let (entry, mqtt) = only_mqtt_entry(&publishes);
    assert_eq!(mqtt.target.client, "office");
    assert_eq!(mqtt.target.topic, "lights/state");
    assert_eq!(
        mqtt.props,
        MqttPublishProps {
            content_type: None,
            qos: 2,
            retain: false,
            message_expiry_s: Some(90),
        }
    );
    assert_eq!(entry.channel_address, mqtt_egress_address("office"));
    assert_eq!(entry.body.as_text(), Some("on"));
}

/// An empty payload buffers an empty body.
#[test]
fn an_empty_payload_buffers_an_empty_body() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());
    let outcome = comp.handle(common::host_activation(activation_with_body(&publish_to(
        "home",
        "retained/t",
        1,
        true,
        None,
        None,
        "",
    ))));
    let publishes = ok_publishes(outcome);
    let (entry, mqtt) = only_mqtt_entry(&publishes);
    assert!(entry.body.is_empty());
    assert!(mqtt.props.retain);
}

/// MQTT entries flush in call order with the activation's port publishes.
#[test]
fn mqtt_entries_keep_call_order_with_port_publishes() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());
    let publishes =
        ok_publishes(comp.handle(common::host_activation(activation_with_body("INTERLEAVE"))));

    assert_eq!(publishes.len(), INTERLEAVE_CALLS + 2, "{publishes:?}");
    assert_eq!(publishes[0].channel_address, "brenn:out");
    assert_eq!(publishes[0].body.as_text(), Some("before"));
    let egress = mqtt_egress_address("test-client");
    for (i, entry) in publishes[1..=INTERLEAVE_CALLS].iter().enumerate() {
        assert!(entry.mqtt.is_some(), "entry {i}");
        assert_eq!(entry.channel_address, egress);
        assert_eq!(entry.body.as_text(), Some(i.to_string().as_str()));
    }
    let last = &publishes[INTERLEAVE_CALLS + 1];
    assert_eq!(last.channel_address, "brenn:out");
    assert_eq!(
        last.body.as_text(),
        Some(format!("ok={INTERLEAVE_CALLS} refused=0 kinds=").as_str())
    );
}

/// The fixture's `INTERLEAVE_CALLS`.
const INTERLEAVE_CALLS: usize = 20;

/// A publish admitted before the guest returns `err`, or before it traps, is
/// discarded with the activation: the outcome carries no publishes, though the
/// host admitted the call.
#[test]
fn a_failed_activation_discards_its_mqtt_publishes() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());

    let msg = failure(comp.handle(common::host_activation(activation_with_body(
        "ERR_AFTER_PUBLISH",
    ))));
    assert!(msg.contains("err-after-publish"), "{msg:?}");
    assert_eq!(publisher.seen().len(), 1, "the publish was admitted");

    publisher.answer(MqttAdmission::Admitted);
    let outcome = comp.handle(common::host_activation(activation_with_body(
        "TRAP_AFTER_PUBLISH",
    )));
    assert!(
        matches!(outcome, ProcessorOutcome::Trap(_)),
        "expected a trap; got {outcome:?}"
    );
    assert_eq!(publisher.seen().len(), 1, "the publish was admitted");
}

/// The admission's refusals reach the guest as their variants, with the
/// reason text of an `invalid-payload`.
#[test]
fn admission_refusals_reach_the_guest() {
    let publisher = ScriptedPublisher::new(MqttAdmission::NotPermitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());
    let body = publish_to("home", "t", 0, false, None, None, "x");

    let msg = failure(comp.handle(common::host_activation(activation_with_body(&body))));
    assert!(msg.contains("NotPermitted"), "{msg:?}");

    publisher.answer(MqttAdmission::InvalidPayload(
        "payload 9 bytes exceeds the client's cap of 4".to_string(),
    ));
    let msg = failure(comp.handle(common::host_activation(activation_with_body(&body))));
    assert!(
        msg.contains("InvalidPayload") && msg.contains("exceeds the client's cap of 4"),
        "{msg:?}"
    );
}

/// A zero expiry from a guest is refused as `invalid-payload` before the
/// admission runs.
#[test]
fn publish_expiring_with_zero_expiry_is_invalid_payload() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());

    let msg = failure(comp.handle(common::host_activation(activation_with_body(
        "PUBLISH_ZERO_EXPIRY",
    ))));
    assert!(
        msg.contains("InvalidPayload:") && msg.contains("message-expiry-s"),
        "unexpected error: {msg}"
    );
    assert!(publisher.seen().is_empty(), "the admission was not reached");
}

/// A loop the host keeps admitting ends at the 257th call, refused by the
/// buffered-publish ceiling, well short of the 512-call ceiling.
#[test]
fn the_bounded_loop_ends_at_the_buffered_publish_ceiling() {
    const {
        assert!(
            MAX_PUBLISHES_PER_ACTIVATION < MAX_PUBLISH_CALLS_PER_ACTIVATION,
            "the buffered-publish ceiling is the first one an admitting loop meets"
        )
    };
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());

    let msg = failure(comp.handle(common::host_activation(activation_with_body("ping"))));
    assert!(msg.contains("QuotaExceeded:"), "{msg:?}");
    assert_eq!(
        publisher.seen().len(),
        MAX_PUBLISHES_PER_ACTIVATION + 1,
        "every call up to and including the refused one passed the admission"
    );
}

/// Spellings that resolve to one client share its one token bucket: twenty
/// client strings, one session, five tokens.
#[test]
fn spellings_of_one_client_share_one_bucket() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    // No carry between activations; one activation's grant is the input
    // grant (1000) plus the fill (4000): five publishes.
    let comp = load_mqtt_test(
        &publisher,
        HashMap::from([(
            "home".to_string(),
            SinkBudget {
                fill_mt: 4000,
                capacity_mt: 0,
            },
        )]),
    );
    let publishes = ok_publishes(comp.handle(common::host_activation(activation_with_body(
        "INTERLEAVE_SPELL",
    ))));
    let tally = publishes
        .last()
        .and_then(|p| p.body.as_text())
        .expect("the tally");
    assert_eq!(
        tally,
        format!(
            "ok=5 refused={} kinds=MqttPublishError::QuotaExceeded",
            INTERLEAVE_CALLS - 5
        )
    );
    let homes = publishes
        .iter()
        .filter(|p| p.mqtt.as_ref().is_some_and(|m| m.target.client == "home"))
        .count();
    assert_eq!(homes, 5);
}

// ── the guest SDK's own mqtt surface ─────────────────────────────────────────

/// `mqtt::publish_json` sends `application/json` and a body that parses back to
/// the value the guest handed it.
#[test]
fn publish_json_sends_json_under_the_json_content_type() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());

    let publishes = ok_publishes(comp.handle(common::host_activation(activation_with_body(
        "PUBLISH_JSON",
    ))));
    let (entry, mqtt) = only_mqtt_entry(&publishes);
    assert_eq!(mqtt.target.client, "test-client");
    assert_eq!(mqtt.target.topic, "test/topic");
    assert_eq!(
        mqtt.props,
        MqttPublishProps {
            content_type: Some("application/json".to_string()),
            ..Default::default()
        }
    );
    // The fixture publishes the body it was handed, so the payload is that
    // body as a JSON string.
    let decoded: String = serde_json::from_slice(entry.body.as_bytes())
        .expect("publish_json sends JSON, not a Debug rendering");
    assert_eq!(decoded, "PUBLISH_JSON");
}

/// `mqtt::publish_text` sends the bytes as given, under `text/plain`.
#[test]
fn publish_text_sends_the_body_under_the_text_content_type() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());

    let publishes = ok_publishes(comp.handle(common::host_activation(activation_with_body(
        "PUBLISH_TEXT",
    ))));
    let (entry, mqtt) = only_mqtt_entry(&publishes);
    assert_eq!(mqtt.props.content_type.as_deref(), Some("text/plain"));
    assert_eq!(mqtt.props.message_expiry_s, None);
    assert_eq!(
        entry.body.as_bytes(),
        b"PUBLISH_TEXT",
        "the text wrapper sends the bytes it was given, unencoded"
    );
}

/// `mqtt::publish_text_expiring` reaches the host through
/// `mqtt-publish-expiring`: the expiry arrives beside the text content type,
/// QoS and retain flag the guest chose.
#[test]
fn publish_text_expiring_sends_the_expiry() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());

    let publishes = ok_publishes(comp.handle(common::host_activation(activation_with_body(
        "PUBLISH_TEXT_EXPIRING",
    ))));
    let (entry, mqtt) = only_mqtt_entry(&publishes);
    assert_eq!(
        mqtt.props,
        MqttPublishProps {
            content_type: Some("text/plain".to_string()),
            qos: 1,
            retain: true,
            message_expiry_s: Some(90),
        }
    );
    assert_eq!(entry.body.as_bytes(), b"PUBLISH_TEXT_EXPIRING");
}

/// Every refusal this host returns, classified and rendered by the SDK. None
/// is retryable: the bus has no backpressure, so a guest that believed a
/// permanent refusal retryable would retry it on every activation, forever.
#[test]
fn the_sdk_classifies_and_renders_every_refusal() {
    let publisher = ScriptedPublisher::new(MqttAdmission::Admitted);
    let comp = load_mqtt_test(&publisher, HashMap::new());

    for (admission, expected) in [
        (
            MqttAdmission::NotPermitted,
            "transient=false mqtt publish test-client/test/topic: not-permitted",
        ),
        (
            MqttAdmission::InvalidPayload("too big".to_string()),
            "transient=false mqtt publish test-client/test/topic: invalid-payload: too big",
        ),
        // Admitted throughout: the loop meets the host's own ceiling.
        (
            MqttAdmission::Admitted,
            "transient=false mqtt publish test-client/test/topic: quota-exceeded",
        ),
    ] {
        publisher.answer(admission.clone());
        let msg = failure(comp.handle(common::host_activation(activation_with_body("CLASSIFY"))));
        assert!(
            msg.contains(expected),
            "{admission:?}: expected {expected:?}, got {msg:?}"
        );
    }
}

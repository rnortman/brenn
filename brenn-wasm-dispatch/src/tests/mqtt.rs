//! The `mqtt` interface under a real `Messenger`: `processor-mqtt-test`
//! publishes through the production admission seam
//! ([`crate::mqtt_publisher_over`]) and the dispatcher's flush, and the rows it
//! commits are read back off the client's egress channel.
//!
//! Every new text message on the consumer's `in` channel selects one of the
//! fixture's paths. A refusal the guest turns into an `err` reaches the
//! operator as the dispatcher's activation-failed alert, whose diagnostic is
//! the guest's rendering of the variant.

use super::non_text::{activate, attach_subscribers, durable, unattached_messenger};
use super::*;

use brenn_envelope::grants::AppCapability;
use brenn_envelope::{MqttExpiry, MqttPublishTransport, TransportMeta};
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::MqttPubMatcher;
use brenn_lib::mqtt::MqttClientDirectory;
use brenn_messaging::Messenger;
use brenn_messaging::mqtt_outbound::mqtt_egress_entry;
use brenn_obs::alerting::{AlertSeverity, make_capturing_alerter_with_severity};
use std::collections::HashMap;

const MQTT_WASM: &str = "brenn-wasm/target/components/brenn_processor_mqtt_test.wasm";

/// The client every fixed-address path of the fixture names.
const CLIENT: &str = "test-client";
/// The topic every fixed-address path of the fixture names.
const TOPIC: &str = "test/topic";
/// The client's `outbound_payload_cap_bytes`.
/// Above the length of every sentinel the fixed-address paths publish.
const CLIENT_CAP: u32 = 32;
/// A topic the consumer's ACL does not admit.
const DENIED_TOPIC: &str = "locks/front/set";
/// How many MQTT publishes the fixture's interleave path makes.
const INTERLEAVE_CALLS: usize = 20;
/// The title of the security signal a denied publish raises.
const DENIAL_TITLE: &str = "Security: mqtt_publish_denied";

struct StubClients;

impl MqttClientDirectory for StubClients {
    fn outbound_payload_cap(&self, client: &str) -> Option<u32> {
        (client == CLIENT).then_some(CLIENT_CAP)
    }
}

/// The consumer's policy: the MQTT publish grant, and an ACL admitting
/// [`TOPIC`] and `allowed/#` on [`CLIENT`] and nothing else.
fn policy() -> AppPolicy {
    let mut policy = AppPolicy::with_grants(&[AppCapability::MqttPublish]);
    for filter in [TOPIC, "allowed/#"] {
        policy.acls.mqtt_publish.push(MqttPubMatcher {
            client: CLIENT.to_string(),
            topic_filter: filter.to_string(),
        });
    }
    policy
}

/// One fixture consumer over a messenger holding its script channel, a
/// durable `out` channel and [`CLIENT`]'s egress channel, with the client
/// table installed. The seam and the dispatcher share one capturing alerter.
struct Rig {
    messenger: Arc<Messenger>,
    script: ChannelEntry,
    out: ChannelEntry,
    egress: String,
    cfg: WasmConsumerConfig,
    alerts: Arc<std::sync::Mutex<Vec<(AlertSeverity, String, String)>>>,
    alert_task: tokio::task::JoinHandle<()>,
}

async fn rig(slug: &str) -> Rig {
    let script = durable(
        &format!("brenn:mq-in-{slug}"),
        false,
        &[(slug, Depth::Unbounded, Depth::Unbounded)],
    );
    let out = durable(&format!("brenn:mq-out-{slug}"), false, &[]);
    let egress_entry = mqtt_egress_entry(
        CLIENT,
        &brenn_lib::messaging::config::SystemChannelTuning::default(),
        &MessagingGlobalConfig::default(),
    );
    let egress = egress_entry.address.clone();
    let entries = [script.clone(), out.clone(), egress_entry];
    let messenger = attach_subscribers(
        unattached_messenger(&entries, HashMap::new()).await,
        &entries,
    )
    .await;
    messenger.install_mqtt_clients(Arc::new(StubClients));
    brenn_messaging::testutils::attach_mqtt_egress_reader(&messenger, CLIENT).await;

    let (alert_dispatcher, alerts, alert_task) = make_capturing_alerter_with_severity();
    let outputs = vec![output("out", &out)];
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        declared_out_ports: ["out".to_string()].into_iter().collect(),
        output_ports: output_specs(&outputs),
        input_amplification_mt: test_amp_map(),
        grants: [ComponentGrant::Ports, ComponentGrant::Mqtt]
            .into_iter()
            .collect(),
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: Some(crate::mqtt_publisher_over(
            &messenger,
            policy(),
            slug.to_string(),
            alert_dispatcher.clone(),
        )),
        ..ProcessorLoadSpec::minimal(std::path::Path::new(MQTT_WASM), slug)
    }));
    let mut cfg = consumer(
        slug,
        component,
        &messenger,
        vec![input(slug, "in", &script)],
        outputs,
    );
    cfg.alert_dispatcher = alert_dispatcher;
    Rig {
        messenger,
        script,
        out,
        egress,
        cfg,
        alerts,
        alert_task,
    }
}

impl Rig {
    /// Hand the consumer `text` and activate it once.
    async fn run(&self, text: &str) {
        testutils::insert_bus_message(&self.messenger, &self.script, text, ChannelScheme::Brenn)
            .await;
        activate(&self.cfg).await;
    }

    /// Every row on [`CLIENT`]'s egress channel, oldest first.
    async fn egress_rows(&self) -> Vec<brenn_envelope::MessageEnvelope> {
        rows(&self.messenger, &self.egress).await
    }

    /// Drop the consumer, and with it every holder of the alerter, and return
    /// every alert raised.
    async fn alerts(self) -> Vec<(AlertSeverity, String, String)> {
        drop(self.cfg);
        tokio::time::timeout(std::time::Duration::from_secs(10), self.alert_task)
            .await
            .expect("the alert task ends once the consumer is dropped")
            .expect("the alert task does not panic");
        self.alerts.lock().expect("alerts lock").clone()
    }
}

async fn rows(messenger: &Messenger, address: &str) -> Vec<brenn_envelope::MessageEnvelope> {
    testutils::retained_on(messenger, address)
        .await
        .into_iter()
        .map(|(_, envelope)| envelope)
        .collect()
}

fn sender(slug: &str) -> String {
    ParticipantId::for_wasm(slug).as_str().to_owned()
}

fn transport(envelope: &brenn_envelope::MessageEnvelope) -> &MqttPublishTransport {
    match &envelope.transport {
        Some(TransportMeta::MqttPublish(t)) => t,
        other => panic!("an egress row carries MqttPublish, got {other:?}"),
    }
}

/// The fixture's parameterised publish: one call with these arguments.
/// `expiry` `None` is `mqtt-publish`, `Some` `mqtt-publish-expiring`.
fn publish_to(
    topic: &str,
    qos: u8,
    retain: bool,
    expiry: Option<u32>,
    content_type: Option<&str>,
    payload: &str,
) -> String {
    format!(
        "PUBLISH_TO\n{CLIENT}\n{topic}\n{qos}\n{}\n{}\n{}\n{payload}",
        u8::from(retain),
        expiry.map_or_else(|| "-".to_string(), |s| s.to_string()),
        content_type.unwrap_or("-"),
    )
}

/// The diagnostics of every activation-failed alert in `alerts`.
fn failures(alerts: &[(AlertSeverity, String, String)]) -> Vec<&str> {
    alerts
        .iter()
        .filter(|(_, title, _)| title.contains("activation failed"))
        .map(|(_, _, body)| body.as_str())
        .collect()
}

/// An activation that interleaves `ports.publish` with `mqtt-publish` commits
/// the MQTT publishes as the component's rows on the client's egress channel,
/// each never expiring and carrying its options, and every row of the flush —
/// bus and egress — in call order with strictly increasing timestamps.
#[tokio::test]
async fn an_activations_mqtt_publishes_commit_to_the_egress_channel_in_call_order() {
    let slug = "mq-interleave";
    let rig = rig(slug).await;
    rig.run("INTERLEAVE").await;

    let out = rows(&rig.messenger, &rig.out.address).await;
    let egress = rig.egress_rows().await;
    assert_eq!(out.len(), 2, "the two port publishes");
    assert_eq!(out[0].body.as_text().expect("text"), "before");
    assert_eq!(
        out[1].body.as_text().expect("text"),
        format!("ok={INTERLEAVE_CALLS} refused=0 kinds=")
    );
    assert_eq!(egress.len(), INTERLEAVE_CALLS, "every admitted publish");
    for (i, row) in egress.iter().enumerate() {
        assert_eq!(row.sender, sender(slug));
        assert_eq!(row.channel, rig.egress);
        assert_eq!(
            row.body.as_text().expect("an untyped payload is text"),
            i.to_string()
        );
        assert_eq!(
            *transport(row),
            MqttPublishTransport {
                client_slug: CLIENT.to_string(),
                topic: TOPIC.to_string(),
                qos: 1,
                retain: false,
                content_type: None,
                expires: MqttExpiry::Never,
                status_to: None,
            }
        );
    }
    let stamps: Vec<_> = std::iter::once(&out[0])
        .chain(egress.iter())
        .chain(std::iter::once(&out[1]))
        .map(|row| row.publish_ts)
        .collect();
    assert!(
        stamps.windows(2).all(|w| w[0] < w[1]),
        "strictly increasing in call order: {stamps:?}"
    );
    assert!(failures(&rig.alerts().await).is_empty());
}

/// `mqtt-publish-expiring` commits a row whose lifetime ends its seconds after
/// the row's own commit time, with the stated QoS, retain flag and content
/// type, and a body typed by that content type.
#[tokio::test]
async fn an_expiring_publish_expires_its_seconds_after_commit() {
    let slug = "mq-expiring";
    let rig = rig(slug).await;
    rig.run(&publish_to(
        "allowed/state",
        2,
        true,
        Some(90),
        Some("application/json"),
        r#"{"on":true}"#,
    ))
    .await;

    let egress = rig.egress_rows().await;
    let [row] = egress.as_slice() else {
        panic!("one row, got {}", egress.len());
    };
    assert_eq!(row.sender, sender(slug));
    assert_eq!(row.body.content_type(), Some("application/json"));
    assert_eq!(row.body.as_text().expect("JSON is text"), r#"{"on":true}"#);
    assert_eq!(
        *transport(row),
        MqttPublishTransport {
            client_slug: CLIENT.to_string(),
            topic: "allowed/state".to_string(),
            qos: 2,
            retain: true,
            content_type: Some("application/json".to_string()),
            expires: MqttExpiry::At(row.publish_ts + chrono::Duration::seconds(90)),
            status_to: None,
        }
    );
    assert!(failures(&rig.alerts().await).is_empty());
}

/// An empty payload commits an empty body, which the reader publishes as zero
/// bytes: how a retained message is cleared.
#[tokio::test]
async fn an_empty_payload_commits_an_empty_body() {
    let rig = rig("mq-empty").await;
    rig.run(&publish_to("allowed/state", 1, true, None, None, ""))
        .await;

    let egress = rig.egress_rows().await;
    let [row] = egress.as_slice() else {
        panic!("one row, got {}", egress.len());
    };
    assert_eq!(row.body.as_text().expect("an empty body is text"), "");
    assert!(transport(row).retain);
    assert!(failures(&rig.alerts().await).is_empty());
}

/// A publish the guest made before returning `err`, or before trapping, is
/// discarded with the activation: nothing reaches the egress channel.
#[tokio::test]
async fn a_failed_activations_mqtt_publishes_are_discarded() {
    for (slug, sentinel, title, diagnostic) in [
        (
            "mq-err",
            "ERR_AFTER_PUBLISH",
            "activation failed (err)",
            "err-after-publish: publish admitted",
        ),
        (
            "mq-trap",
            "TRAP_AFTER_PUBLISH",
            "activation trapped",
            "wasm trap",
        ),
    ] {
        let rig = rig(slug).await;
        rig.run(sentinel).await;
        assert!(rig.egress_rows().await.is_empty(), "{sentinel}");
        let alerts = rig.alerts().await;
        assert!(
            alerts
                .iter()
                .any(|(_, t, body)| t.contains(title) && body.contains(diagnostic)),
            "{sentinel}: the activation failed as scripted: {alerts:?}"
        );
    }
}

/// A topic outside the consumer's ACL is `not-permitted` to the guest, raises
/// exactly one publish-denial signal attributed to the component, and commits
/// nothing.
#[tokio::test]
async fn a_topic_outside_the_acl_is_not_permitted_and_signalled() {
    let slug = "mq-denied";
    let rig = rig(slug).await;
    rig.run(&publish_to(DENIED_TOPIC, 0, false, None, None, "x"))
        .await;
    assert!(rig.egress_rows().await.is_empty());

    let alerts = rig.alerts().await;
    let failures = failures(&alerts);
    assert_eq!(failures.len(), 1, "{alerts:?}");
    assert!(failures[0].contains("NotPermitted"), "{}", failures[0]);
    let denials: Vec<_> = alerts
        .iter()
        .filter(|(_, title, _)| title == DENIAL_TITLE)
        .collect();
    assert_eq!(denials.len(), 1, "{alerts:?}");
    let (severity, _, body) = denials[0];
    assert!(matches!(severity, AlertSeverity::Warning));
    assert!(
        body.contains(&format!("component {slug} denied publish"))
            && body.contains(&format!("mqtt:{CLIENT}:{DENIED_TOPIC}")),
        "{body}"
    );
}

/// The admission's payload refusals are `invalid-payload` to the guest and
/// commit nothing: a malformed content type, and a payload over the client's
/// `outbound_payload_cap_bytes` (one byte under it is admitted).
#[tokio::test]
async fn the_admissions_payload_refusals_are_invalid_payload() {
    let cap = usize::try_from(CLIENT_CAP).expect("fits");
    let at_cap = "x".repeat(cap);
    let over_cap = "x".repeat(cap + 1);
    let rig = rig("mq-invalid").await;
    for (script, expect) in [
        (
            publish_to(TOPIC, 0, false, None, Some("text/"), "x"),
            "invalid content type",
        ),
        (
            publish_to(TOPIC, 0, false, None, None, &over_cap),
            "over the client's cap",
        ),
    ] {
        rig.run(&script).await;
        assert!(rig.egress_rows().await.is_empty(), "{expect}");
    }
    rig.run(&publish_to(TOPIC, 0, false, None, None, &at_cap))
        .await;
    let egress = rig.egress_rows().await;
    assert_eq!(egress.len(), 1, "a payload at the cap is admitted");
    assert_eq!(egress[0].body.as_text().expect("text"), at_cap);

    let alerts = rig.alerts().await;
    let failures = failures(&alerts);
    assert_eq!(failures.len(), 2, "{alerts:?}");
    assert!(
        failures[0].contains("InvalidPayload") && failures[0].contains("invalid content type"),
        "{}",
        failures[0]
    );
    assert!(
        failures[1].contains("InvalidPayload")
            && failures[1].contains(&format!("payload {} bytes exceeds", cap + 1)),
        "{}",
        failures[1]
    );
}

/// Ask the production seam, built over `rig`'s messenger under `policy`, to
/// admit one byte to `client`/`topic`.
fn admit_at_seam(rig: &Rig, policy: AppPolicy, client: &str, topic: &str) {
    let seam = crate::mqtt_publisher_over(
        &rig.messenger,
        policy,
        "mq-seam".to_string(),
        rig.cfg.alert_dispatcher.clone(),
    );
    let props = brenn_wasm::MqttPublishProps {
        content_type: None,
        qos: 0,
        retain: false,
        message_expiry_s: None,
    };
    let answer = seam.admit(
        &brenn_wasm::MqttTarget {
            client: client.to_string(),
            topic: topic.to_string(),
        },
        1,
        &props,
    );
    panic!("the seam answered {answer:?} instead of panicking");
}

#[tokio::test]
#[should_panic(expected = "mqtt admission without the mqtt publish grant")]
async fn the_seam_panics_on_a_policy_without_the_grant() {
    let rig = rig("mq-no-grant").await;
    let mut policy = policy();
    policy.grants = brenn_lib::access::GrantSet::default();
    admit_at_seam(&rig, policy, CLIENT, TOPIC);
}

#[tokio::test]
#[should_panic(expected = "no session or egress channel for \"mqtt:absent:test/topic\"")]
async fn the_seam_panics_on_a_client_with_no_session_or_channel() {
    let rig = rig("mq-absent").await;
    let mut policy = policy();
    policy.acls.mqtt_publish.push(MqttPubMatcher {
        client: "absent".to_string(),
        topic_filter: TOPIC.to_string(),
    });
    admit_at_seam(&rig, policy, "absent", TOPIC);
}

/// A target `resolve` would never build reaches the catch-all arm.
#[tokio::test]
#[should_panic(expected = "mqtt admission answered MalformedAddress")]
async fn the_seam_panics_on_an_answer_it_never_meets() {
    let rig = rig("mq-wildcard").await;
    admit_at_seam(&rig, policy(), CLIENT, "test/+");
}

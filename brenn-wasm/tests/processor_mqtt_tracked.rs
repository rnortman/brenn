// Integration tests for `mqtt-publish-tracked` and the guest SDK's status and
// connection-state readers: the `processor-mqtt-tracked-test` fixture, a real
// activation, and a scripted `MqttPublisher`.
//
// The fixture's ports are `in inbound`, `out out`, `io delivery`, `optional out
// spare`, `optional in broker` and `io state`. Here `out`, `delivery` and
// `state` are wired and `spare` is declared and left unwired.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use brenn_activation::{ProcessorActivation, ProcessorPortWindow};
use brenn_envelope::addressing::mqtt_egress_address;
use brenn_envelope::delivery::{
    self, DELIVERY_STATUS_SENDER, DeliveryDropCause, DeliveryKind, DeliveryStatus,
    DeliveryUnknownCause, MqttDelivery, MqttRestarted, StatusBody, TransportRestarted,
};
use brenn_envelope::mqtt_connection::{
    self, Connection, MQTT_CONNECTION_SENDER, MqttConnectionState, PreviousEnd, PreviousSession,
};
use brenn_wasm::{
    ComponentGrant, MqttAdmission, MqttPublishFn, MqttPublishLifetime, MqttPublishProps,
    MqttTarget, ProcessorComponent, ProcessorLoadSpec, ProcessorOutcome, ProcessorPublish,
    StatusChannelRefusal,
};
use uuid::Uuid;

mod common;

use common::{ScriptedPublisher, failure, ok_publishes};

/// The channel the deployer wired to `delivery`.
const STATUS_CHANNEL: &str = "brenn:light-status";

/// The publish time every test envelope carries, and its Unix milliseconds.
const PUBLISH_TS: &str = "2026-01-01T00:00:00Z";
const PUBLISH_MS: i64 = 1_767_225_600_000;

fn component_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target/components/brenn_processor_mqtt_tracked_test.wasm")
}

fn admitted(response_topic: Option<&str>) -> MqttAdmission {
    MqttAdmission::Admitted {
        response_topic: response_topic.map(str::to_string),
    }
}

fn load(publisher: &Arc<ScriptedPublisher>) -> ProcessorComponent {
    let grants: BTreeSet<ComponentGrant> = [ComponentGrant::Mqtt, ComponentGrant::Ports]
        .into_iter()
        .collect();
    let amplification = ["inbound", "delivery", "broker", "state"]
        .into_iter()
        .map(|port| (port.to_string(), 1000u64))
        .collect();
    ProcessorComponent::load(ProcessorLoadSpec {
        input_amplification_mt: amplification,
        output_ports: HashMap::from([
            ("out".to_string(), common::out_spec("brenn:out")),
            ("delivery".to_string(), common::out_spec(STATUS_CHANNEL)),
            ("state".to_string(), common::out_spec("brenn:tracked-state")),
        ]),
        declared_out_ports: ["out", "delivery", "spare", "state"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        grants,
        alerter: common::noop_alerter(),
        output_acl: common::allow_all(),
        mqtt_sinks: HashMap::new(),
        mqtt_publish: Some(Arc::clone(publisher) as MqttPublishFn),
        ..ProcessorLoadSpec::minimal(&component_path(), "mqtt-tracked-test")
    })
}

/// One envelope's JSON, as the host hands it over.
fn envelope(sender: &str, body: &str) -> String {
    serde_json::json!({
        "message_id": Uuid::new_v4().to_string(),
        "source": "test",
        "channel": "brenn:test",
        "sender": sender,
        "publish_ts": PUBLISH_TS,
        "body": body,
        "urgency": "normal",
        "envelope_type": "brenn",
    })
    .to_string()
}

/// A window on `port` whose envelopes are all new.
fn new_window(port: &str, envelopes: Vec<String>) -> ProcessorPortWindow {
    ProcessorPortWindow {
        port: port.to_string(),
        envelopes,
        new_from: 0,
        dropped: 0,
    }
}

/// A window on `port` whose envelopes are all context.
fn context_window(port: &str, envelopes: Vec<String>) -> ProcessorPortWindow {
    let new_from = u32::try_from(envelopes.len()).expect("a test window is small");
    ProcessorPortWindow {
        port: port.to_string(),
        envelopes,
        new_from,
        dropped: 0,
    }
}

fn activation(ports: Vec<ProcessorPortWindow>) -> ProcessorActivation {
    ProcessorActivation {
        ports,
        deferred: vec![],
        now: None,
        sync: None,
    }
}

/// An activation whose only new message is `body` on `inbound`.
fn inbound(body: &str) -> ProcessorActivation {
    activation(vec![new_window(
        "inbound",
        vec![envelope("test-sender", body)],
    )])
}

/// The fields of one tracked publish, as the fixture reads them.
struct Tracked<'a> {
    client: &'a str,
    topic: &'a str,
    qos: u8,
    retain: bool,
    lifetime: &'a str,
    content_type: Option<&'a str>,
    status_port: Option<&'a str>,
    reply_topic: Option<&'a str>,
    payload: &'a str,
}

impl Tracked<'_> {
    fn plain() -> Self {
        Tracked {
            client: "home",
            topic: "lights/kitchen/set",
            qos: 1,
            retain: false,
            lifetime: "forever",
            content_type: None,
            status_port: None,
            reply_topic: None,
            payload: "on",
        }
    }

    fn body(&self, sentinel: &str) -> String {
        format!(
            "{sentinel}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            self.client,
            self.topic,
            self.qos,
            u8::from(self.retain),
            self.lifetime,
            self.content_type.unwrap_or("-"),
            self.status_port.unwrap_or("-"),
            self.reply_topic.unwrap_or("-"),
            self.payload,
        )
    }
}

/// The publishes of an admitted tracked call: the MQTT entry, then
/// `published <id>` on `out` and the id on `state`. Returns the entry and the
/// id the guest was given.
fn admitted_publish(publishes: &[ProcessorPublish]) -> (&ProcessorPublish, Uuid) {
    assert_eq!(publishes.len(), 3, "{publishes:?}");
    let entry = &publishes[0];
    assert!(entry.mqtt.is_some(), "{entry:?}");
    let reported = publishes[1]
        .body
        .as_text()
        .and_then(|t| t.strip_prefix("published "))
        .unwrap_or_else(|| panic!("the reported id: {:?}", publishes[1]));
    assert_eq!(publishes[1].channel_address, "brenn:out");
    let id = Uuid::parse_str(reported).expect("the guest's id is a UUID");
    assert_eq!(publishes[2].channel_address, "brenn:tracked-state");
    assert_eq!(publishes[2].body.as_text(), Some(id.to_string().as_str()));
    (entry, id)
}

// ── the call ─────────────────────────────────────────────────────────────────

/// A publish with a lifetime in seconds, a wired status port and a reply topic
/// buffers one MQTT entry under the id the guest is given, with every option
/// carried to the seam and the admission's response topic on the entry.
#[test]
fn a_tracked_publish_returns_the_id_its_entry_is_buffered_under() {
    let publisher = ScriptedPublisher::new(admitted(Some("lights/replies")));
    let comp = load(&publisher);
    let body = Tracked {
        lifetime: "90",
        content_type: Some("text/plain"),
        status_port: Some("delivery"),
        reply_topic: Some("lights/replies"),
        retain: true,
        ..Tracked::plain()
    }
    .body("TRACKED");

    let publishes = ok_publishes(comp.handle(common::host_activation(inbound(&body))));
    let (entry, id) = admitted_publish(&publishes);
    let mqtt = entry.mqtt.as_ref().expect("an mqtt entry");
    let props = MqttPublishProps {
        content_type: Some("text/plain".to_string()),
        qos: 1,
        retain: true,
        lifetime: MqttPublishLifetime::Seconds(90),
        status_to: Some(STATUS_CHANNEL.to_string()),
        reply_topic: Some("lights/replies".to_string()),
    };
    assert_eq!(mqtt.message_id, id);
    assert_eq!(mqtt.props, props);
    assert_eq!(mqtt.response_topic.as_deref(), Some("lights/replies"));
    assert_eq!(entry.channel_address, mqtt_egress_address("home"));
    assert_eq!(entry.body.as_text(), Some("on"));
    let target = MqttTarget {
        client: "home".to_string(),
        topic: "lights/kitchen/set".to_string(),
    };
    assert_eq!(publisher.seen(), vec![(target, 2, props)]);
}

/// `forever` with no status port and no reply topic states none of them, and
/// every call mints a fresh id.
#[test]
fn a_forever_publish_states_no_status_channel_and_ids_are_distinct() {
    let publisher = ScriptedPublisher::new(admitted(None));
    let comp = load(&publisher);
    let body = Tracked::plain().body("TRACKED");

    let first = ok_publishes(comp.handle(common::host_activation(inbound(&body))));
    let (entry, first_id) = admitted_publish(&first);
    let mqtt = entry.mqtt.as_ref().expect("an mqtt entry");
    assert_eq!(mqtt.props.lifetime, MqttPublishLifetime::Forever);
    assert_eq!(mqtt.props.status_to, None);
    assert_eq!(mqtt.props.reply_topic, None);
    assert_eq!(mqtt.response_topic, None);

    let second = ok_publishes(comp.handle(common::host_activation(inbound(&body))));
    let (_, second_id) = admitted_publish(&second);
    assert_ne!(first_id, second_id);
}

/// A declared port the deployer left unwired admits the publish with no
/// status channel.
#[test]
fn an_unwired_status_port_admits_with_no_status_channel() {
    let publisher = ScriptedPublisher::new(admitted(None));
    let comp = load(&publisher);
    let body = Tracked {
        status_port: Some("spare"),
        ..Tracked::plain()
    }
    .body("TRACKED");

    let publishes = ok_publishes(comp.handle(common::host_activation(inbound(&body))));
    let (entry, _) = admitted_publish(&publishes);
    assert_eq!(entry.mqtt.as_ref().expect("mqtt").props.status_to, None);
    assert_eq!(publisher.seen().len(), 1);
}

/// A port the specification does not declare `out` or `io` — an `in`-only
/// port among them — traps the activation, and the seam is never asked.
#[test]
fn a_status_port_not_declared_out_or_io_traps() {
    for port in ["inbound", "no-such-port"] {
        let publisher = ScriptedPublisher::new(admitted(None));
        let comp = load(&publisher);
        let body = Tracked {
            status_port: Some(port),
            ..Tracked::plain()
        }
        .body("TRACKED");
        let outcome = comp.handle(common::host_activation(inbound(&body)));
        assert!(
            matches!(outcome, ProcessorOutcome::Trap(_)),
            "{port}: expected a trap; got {outcome:?}"
        );
        assert!(publisher.seen().is_empty(), "{port}: the seam was asked");
    }
}

/// A lifetime of zero seconds, which the SDK cannot state, is refused by the
/// host before the seam runs.
#[test]
fn a_zero_lifetime_is_refused_before_the_seam() {
    let publisher = ScriptedPublisher::new(admitted(None));
    let comp = load(&publisher);
    let body = Tracked {
        lifetime: "0",
        ..Tracked::plain()
    }
    .body("TRACKED");

    let msg = failure(comp.handle(common::host_activation(inbound(&body))));
    assert!(
        msg.contains("mqtt publish: invalid-publish: lifetime seconds must be at least 1"),
        "{msg:?}"
    );
    assert!(publisher.seen().is_empty());
}

/// A tracked publish admitted before the guest fails the activation is
/// discarded with it: the id the guest was given names nothing.
#[test]
fn a_failed_activation_discards_the_tracked_publish() {
    let publisher = ScriptedPublisher::new(admitted(None));
    let comp = load(&publisher);
    let body = Tracked::plain().body("TRACKED_THEN_ERR");

    let msg = failure(comp.handle(common::host_activation(inbound(&body))));
    assert!(msg.contains("tracked-then-err"), "{msg:?}");
    assert_eq!(publisher.seen().len(), 1, "the publish was admitted");
}

/// Each refusal reaches the guest as its own case, rendered by the SDK.
#[test]
fn each_refusal_reaches_the_guest_as_its_case() {
    let publisher = ScriptedPublisher::new(admitted(None));
    let comp = load(&publisher);
    let body = Tracked {
        status_port: Some("delivery"),
        reply_topic: Some("lights/replies"),
        ..Tracked::plain()
    }
    .body("TRACKED");

    for (admission, expected) in [
        (MqttAdmission::NotPermitted, "mqtt publish: not-permitted"),
        (
            MqttAdmission::InvalidPayload("invalid reply topic: wildcard".to_string()),
            "mqtt publish: invalid-publish: invalid reply topic: wildcard",
        ),
        (
            MqttAdmission::ReplyTopicNotPermitted,
            "mqtt publish: reply-topic-not-permitted",
        ),
        (
            MqttAdmission::StatusChannelRefused {
                cause: StatusChannelRefusal::NotPlainBrenn,
            },
            "mqtt publish: status-port-unusable",
        ),
    ] {
        publisher.answer(admission.clone());
        let msg = failure(comp.handle(common::host_activation(inbound(&body))));
        assert!(
            msg.contains(expected),
            "{admission:?}: expected {expected:?}, got {msg:?}"
        );
        let cause = StatusChannelRefusal::NotPlainBrenn.to_string();
        assert!(
            !msg.contains(&cause),
            "the operator's cause reached the guest: {msg:?}"
        );
    }
}

// ── the readers ──────────────────────────────────────────────────────────────

fn status(message_id: Uuid, kind: DeliveryKind) -> String {
    delivery::encode(&StatusBody::Message(DeliveryStatus {
        message_id,
        kind,
        mqtt: Some(MqttDelivery {
            client: "home".to_string(),
            topic: "lights/kitchen/set".to_string(),
            qos: 1,
            reason_code: None,
        }),
    }))
}

fn connection_state(found_at_ms: i64) -> String {
    mqtt_connection::encode(&MqttConnectionState {
        client: "home".to_string(),
        connection: Connection::Connected,
        since_ms: found_at_ms,
        previous_session: PreviousSession {
            end: PreviousEnd::Unclean,
            found_at_ms,
        },
    })
}

/// The lines the fixture buffered on `out`.
fn lines(publishes: &[ProcessorPublish]) -> Vec<String> {
    publishes
        .iter()
        .map(|p| {
            assert_eq!(p.channel_address, "brenn:out", "{p:?}");
            p.body.as_text().expect("a text line").to_string()
        })
        .collect()
}

/// `delivery::read` on every status kind, a `restarted`, a message from
/// another sender and an undecodable status; statuses matched against the
/// recorded ids; `mqtt::connection::read` on a state and on another sender's
/// message; and a `handed` judged against a state found after it.
#[test]
fn the_readers_classify_and_match_every_kind_of_message() {
    let publisher = ScriptedPublisher::new(admitted(None));
    let comp = load(&publisher);
    let mine = Uuid::new_v4();
    let theirs = Uuid::new_v4();

    let statuses = vec![
        envelope(DELIVERY_STATUS_SENDER, &status(mine, DeliveryKind::Handed)),
        envelope(
            DELIVERY_STATUS_SENDER,
            &status(mine, DeliveryKind::Delivered),
        ),
        envelope(
            DELIVERY_STATUS_SENDER,
            &status(
                theirs,
                DeliveryKind::Rejected {
                    reason: "Not authorized".to_string(),
                },
            ),
        ),
        envelope(DELIVERY_STATUS_SENDER, &status(mine, DeliveryKind::Expired)),
        envelope(
            DELIVERY_STATUS_SENDER,
            &status(
                mine,
                DeliveryKind::Unknown {
                    cause: DeliveryUnknownCause::ConnectionLost,
                },
            ),
        ),
        envelope(
            DELIVERY_STATUS_SENDER,
            &status(
                mine,
                DeliveryKind::Dropped {
                    cause: DeliveryDropCause::Stopped,
                },
            ),
        ),
        envelope(
            DELIVERY_STATUS_SENDER,
            &delivery::encode(&StatusBody::Restarted(TransportRestarted {
                found_at_ms: 42,
                mqtt: Some(MqttRestarted {
                    client: "home".to_string(),
                }),
            })),
        ),
        // The component's own traffic on its status channel, which carries a
        // status-shaped body: the sender decides, not the body.
        envelope(
            "wasm:mqtt-tracked-test",
            &status(mine, DeliveryKind::Delivered),
        ),
        envelope(DELIVERY_STATUS_SENDER, r#"{"v":99,"kind":"handed"}"#),
    ];
    let broker = vec![
        envelope("app:alice@example.com", &connection_state(7)),
        envelope(MQTT_CONNECTION_SENDER, &connection_state(PUBLISH_MS + 1)),
    ];
    let state = vec![envelope("wasm:mqtt-tracked-test", &mine.to_string())];

    let publishes = ok_publishes(comp.handle(common::host_activation(activation(vec![
        new_window("delivery", statuses),
        context_window("broker", broker),
        context_window("state", state),
    ]))));
    let got = lines(&publishes);
    let expected = [
        format!("status matched=true kind=handed id={mine} never=true"),
        format!("status matched=true kind=delivered id={mine}"),
        format!("status matched=false kind=rejected id={theirs}"),
        format!("status matched=true kind=expired id={mine}"),
        format!("status matched=true kind=unknown id={mine}"),
        format!("status matched=true kind=dropped id={mine}"),
        "restarted client=home found_at_ms=42".to_string(),
        "other".to_string(),
    ];
    assert_eq!(got[..expected.len()], expected, "{got:#?}");
    assert!(
        got[expected.len()].starts_with("error delivery status: "),
        "{got:#?}"
    );
    assert_eq!(
        got[expected.len() + 1..],
        [
            "connection none".to_string(),
            format!("connection client=home found_at_ms={}", PUBLISH_MS + 1),
        ],
        "{got:#?}"
    );
    assert!(
        publisher.seen().is_empty(),
        "reading publishes nothing to MQTT"
    );
}

/// A `handed` published after the newest state's `found_at_ms` may still be in
/// flight; with no state on `broker` there is nothing to judge against.
#[test]
fn a_handed_after_the_state_was_found_may_still_be_in_flight() {
    let publisher = ScriptedPublisher::new(admitted(None));
    let comp = load(&publisher);
    let id = Uuid::new_v4();
    let handed = || {
        new_window(
            "delivery",
            vec![envelope(
                DELIVERY_STATUS_SENDER,
                &status(id, DeliveryKind::Handed),
            )],
        )
    };

    for (found_at_ms, never) in [(PUBLISH_MS, "false"), (PUBLISH_MS - 1, "false")] {
        let publishes = ok_publishes(comp.handle(common::host_activation(activation(vec![
            handed(),
            context_window(
                "broker",
                vec![envelope(
                    MQTT_CONNECTION_SENDER,
                    &connection_state(found_at_ms),
                )],
            ),
        ]))));
        assert_eq!(
            lines(&publishes)[0],
            format!("status matched=false kind=handed id={id} never={never}")
        );
    }

    let publishes = ok_publishes(comp.handle(common::host_activation(activation(vec![handed()]))));
    assert_eq!(
        lines(&publishes),
        [format!("status matched=false kind=handed id={id} never=-")]
    );
}

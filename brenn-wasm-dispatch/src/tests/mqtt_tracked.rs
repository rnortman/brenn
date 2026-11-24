//! `mqtt-publish-tracked` under a real `Messenger`: `processor-mqtt-tracked-test`
//! publishes through the production admission seam
//! ([`crate::mqtt_publisher_over`]) and the dispatcher's flush; the row it
//! commits is read back off the client's egress channel, and a status about
//! that row, published as the egress reader publishes it, reaches the
//! component's `delivery` port and is matched there.

use super::mqtt::{rows, transport};
use super::non_text::{activate, attach_subscribers, durable, unattached_messenger};
use super::*;

use brenn_envelope::delivery::{DELIVERY_STATUS_SENDER, DeliveryKind, DeliveryStatus, StatusBody};
use brenn_envelope::grants::AppCapability;
use brenn_envelope::{MqttExpiry, MqttPublishTransport};
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::{ChannelMatcher, MqttMatcher};
use brenn_lib::messaging::SubscriberRegistration;
use brenn_lib::mqtt::MqttClientDirectory;
use brenn_messaging::Messenger;
use brenn_messaging::mqtt_outbound::mqtt_egress_entry;
use brenn_messaging::publish::{DeliveryStatusPublish, DeliveryStatusRefusal};
use brenn_obs::alerting::{AlertSeverity, make_capturing_alerter_with_severity};
use std::collections::HashMap;

const TRACKED_WASM: &str = "brenn-wasm/target/components/brenn_processor_mqtt_tracked_test.wasm";

/// The client every publish names.
const CLIENT: &str = "test-client";
/// The topic every publish names.
const TOPIC: &str = "lights/kitchen/set";
/// The reply topic, admitted by the consumer's MQTT publish ACL.
const REPLY_TOPIC: &str = "lights/replies";

struct StubClients;

impl MqttClientDirectory for StubClients {
    fn outbound_payload_cap(&self, client: &str) -> Option<u32> {
        (client == CLIENT).then_some(1024)
    }
}

fn bare(address: &str) -> &str {
    address
        .strip_prefix("brenn:")
        .unwrap_or_else(|| panic!("{address} is a brenn: address"))
}

/// The consumer's one policy, handed to the seam and registered as its
/// subscriber policy: subscribe on its inputs, publish on its outputs, and
/// [`TOPIC`] and [`REPLY_TOPIC`] on [`CLIENT`].
fn policy(inputs: &[&ChannelEntry], outputs: &[&ChannelEntry]) -> AppPolicy {
    let mut policy = delivery_policy_for_addresses(inputs.iter().map(|e| e.address.as_str()));
    policy.grants.insert(AppCapability::MqttPublish);
    policy.grants.insert(AppCapability::MessagingPublish);
    for filter in [TOPIC, REPLY_TOPIC] {
        policy.acls.mqtt_publish.push(MqttMatcher {
            client: CLIENT.to_string(),
            topic_filter: filter.to_string(),
        });
    }
    for entry in outputs {
        policy
            .acls
            .brenn_publish
            .push(ChannelMatcher::Exact(bare(&entry.address).to_string()));
    }
    policy
}

/// One fixture consumer whose `delivery` port is `io` on a status channel and
/// whose `state` port is `io` on a channel recording its ids.
struct Rig {
    slug: String,
    messenger: Arc<Messenger>,
    inbound: ChannelEntry,
    out: ChannelEntry,
    status: ChannelEntry,
    egress: String,
    cfg: WasmConsumerConfig,
    alerts: Arc<std::sync::Mutex<Vec<(AlertSeverity, String, String)>>>,
    alert_task: tokio::task::JoinHandle<()>,
}

async fn rig(slug: &str) -> Rig {
    let read = &[(slug, Depth::Unbounded, Depth::Unbounded)];
    let inbound = durable(&format!("brenn:mqt-in-{slug}"), false, read);
    let out = durable(&format!("brenn:mqt-out-{slug}"), false, &[]);
    let status = durable(&format!("brenn:mqt-status-{slug}"), false, read);
    let state = durable(&format!("brenn:mqt-state-{slug}"), false, read);
    let egress_entry = mqtt_egress_entry(
        CLIENT,
        &brenn_lib::messaging::config::SystemChannelTuning::default(),
        &MessagingGlobalConfig::default(),
    );
    let egress = egress_entry.address.clone();
    let entries = [
        inbound.clone(),
        out.clone(),
        status.clone(),
        state.clone(),
        egress_entry,
    ];
    let policy = policy(&[&inbound, &status, &state], &[&out, &status, &state]);
    let messenger = attach_subscribers(
        unattached_messenger(&entries, HashMap::new()).await,
        &entries,
    )
    .await;
    messenger.replace_subscriber_registration(
        &SubscriberEntryKind::Wasm(slug.to_string()),
        SubscriberRegistration {
            policy: Arc::new(policy.clone()),
            wake: brenn_lib::messaging::WakeEconomics::Eager,
        },
    );
    messenger.install_mqtt_clients(Arc::new(StubClients));
    brenn_messaging::testutils::attach_mqtt_egress_reader(&messenger, CLIENT).await;

    let (alert_dispatcher, alerts, alert_task) = make_capturing_alerter_with_severity();
    let outputs = vec![
        output("out", &out),
        output("delivery", &status),
        output("state", &state),
    ];
    let component = Arc::new(ProcessorComponent::load(ProcessorLoadSpec {
        declared_out_ports: ["out", "delivery", "spare", "state"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        output_ports: output_specs(&outputs),
        input_amplification_mt: ["inbound", "delivery", "broker", "state"]
            .into_iter()
            .map(|port| (port.to_string(), 1000u64))
            .collect(),
        grants: [ComponentGrant::Ports, ComponentGrant::Mqtt]
            .into_iter()
            .collect(),
        alerter: noop_proc_alerter(),
        output_acl: allow_all(),
        mqtt_publish: Some(crate::mqtt_publisher_over(
            &messenger,
            policy,
            slug.to_string(),
            alert_dispatcher.clone(),
        )),
        ..ProcessorLoadSpec::minimal(std::path::Path::new(TRACKED_WASM), slug)
    }));
    let mut cfg = consumer(
        slug,
        component,
        &messenger,
        vec![
            input(slug, "inbound", &inbound),
            input(slug, "delivery", &status),
            input(slug, "state", &state),
        ],
        outputs,
    );
    cfg.alert_dispatcher = alert_dispatcher;
    Rig {
        slug: slug.to_string(),
        messenger,
        inbound,
        out,
        status,
        egress,
        cfg,
        alerts,
        alert_task,
    }
}

impl Rig {
    /// Hand the consumer `text` on `inbound` and activate it once.
    async fn run(&self, text: &str) {
        testutils::insert_bus_message(&self.messenger, &self.inbound, text, ChannelScheme::Brenn)
            .await;
        activate(&self.cfg).await;
    }

    /// A tracked publish to [`TOPIC`] with this lifetime and status port, and
    /// [`REPLY_TOPIC`] as its reply topic.
    async fn publish(&self, lifetime: &str, status_port: &str) {
        self.run(&format!(
            "TRACKED\n{CLIENT}\n{TOPIC}\n1\n0\n{lifetime}\n-\n{status_port}\n{REPLY_TOPIC}\non"
        ))
        .await;
    }

    async fn egress_rows(&self) -> Vec<brenn_envelope::MessageEnvelope> {
        rows(&self.messenger, &self.egress).await
    }

    async fn out_lines(&self) -> Vec<String> {
        rows(&self.messenger, &self.out.address)
            .await
            .into_iter()
            .map(|row| row.body.as_text().expect("out carries text").to_string())
            .collect()
    }

    /// The id the component published on `out` for its admitted publish.
    async fn published_id(&self) -> Uuid {
        let lines = self.out_lines().await;
        let ids: Vec<Uuid> = lines
            .iter()
            .filter_map(|line| line.strip_prefix("published "))
            .map(|id| Uuid::parse_str(id).expect("the component publishes a UUID"))
            .collect();
        let [id] = ids.as_slice() else {
            panic!("one published id on out, got {lines:?}");
        };
        *id
    }

    /// Publish a status about `message_id` to the status channel, as the
    /// egress reader does: on the authority of the row's stored sender.
    async fn report(&self, message_id: Uuid, kind: DeliveryKind) -> DeliveryStatusPublish {
        self.messenger
            .publish_delivery_status(
                ParticipantId::for_wasm(&self.slug).as_str(),
                &self.status.address,
                &StatusBody::Message(DeliveryStatus {
                    message_id,
                    kind,
                    mqtt: None,
                }),
            )
            .await
    }

    async fn alerts(self) -> Vec<(AlertSeverity, String, String)> {
        drop(self.cfg);
        tokio::time::timeout(std::time::Duration::from_secs(10), self.alert_task)
            .await
            .expect("the alert task ends once the consumer is dropped")
            .expect("the alert task does not panic");
        self.alerts.lock().expect("alerts lock").clone()
    }
}

fn failures(alerts: &[(AlertSeverity, String, String)]) -> Vec<&str> {
    alerts
        .iter()
        .filter(|(_, title, _)| title.contains("activation failed") || title.contains("trapped"))
        .map(|(_, _, body)| body.as_str())
        .collect()
}

/// A tracked publish commits one egress row under the id the component was
/// returned, carrying the wired status channel, the reply topic as its
/// response topic, and the expiry of each lifetime form.
#[tokio::test]
async fn a_tracked_publish_commits_under_the_returned_id_with_every_option() {
    for (slug, lifetime) in [("mqt-seconds", "90"), ("mqt-forever", "forever")] {
        let rig = rig(slug).await;
        rig.publish(lifetime, "delivery").await;

        let id = rig.published_id().await;
        let egress = rig.egress_rows().await;
        let [row] = egress.as_slice() else {
            panic!("{slug}: one row, got {}", egress.len());
        };
        assert_eq!(row.message_id, id, "{slug}: the row is the returned id");
        assert_eq!(row.sender, ParticipantId::for_wasm(slug).as_str());
        assert_eq!(row.body.as_text().expect("text"), "on");
        let expires = match lifetime {
            "forever" => MqttExpiry::Never,
            _ => MqttExpiry::At(row.publish_ts + chrono::Duration::seconds(90)),
        };
        assert_eq!(
            *transport(row),
            MqttPublishTransport {
                client_slug: CLIENT.to_string(),
                topic: TOPIC.to_string(),
                qos: 1,
                retain: false,
                content_type: None,
                expires,
                status_to: Some(rig.status.address.clone()),
                response_topic: Some(REPLY_TOPIC.to_string()),
            },
            "{slug}"
        );
        assert!(failures(&rig.alerts().await).is_empty(), "{slug}");
    }
}

/// An undeclared status port traps the activation, which commits nothing.
#[tokio::test]
async fn an_undeclared_status_port_traps_and_commits_nothing() {
    let rig = rig("mqt-undeclared").await;
    rig.publish("forever", "nonesuch").await;
    assert!(rig.egress_rows().await.is_empty());
    assert!(rig.out_lines().await.is_empty());
    let alerts = rig.alerts().await;
    assert!(
        alerts.iter().any(|(_, title, _)| title.contains("trapped")),
        "the activation trapped: {alerts:?}"
    );
}

/// The end of the path inside one process: statuses about the committed row,
/// published on the component's authority as the egress reader publishes
/// them, land on its status channel from the status sender, and its next
/// activation reads them off `delivery` and matches both to its publish. A
/// status about an id it never issued is read and not matched.
#[tokio::test]
async fn statuses_about_the_row_reach_the_component_and_match() {
    let rig = rig("mqt-match").await;
    rig.publish("forever", "delivery").await;
    let id = rig.published_id().await;

    for kind in [DeliveryKind::Handed, DeliveryKind::Delivered] {
        assert_eq!(rig.report(id, kind).await, DeliveryStatusPublish::Published);
    }
    let stranger = Uuid::from_u128(7);
    assert_eq!(
        rig.report(stranger, DeliveryKind::Delivered).await,
        DeliveryStatusPublish::Published
    );
    let statuses = rows(&rig.messenger, &rig.status.address).await;
    assert_eq!(statuses.len(), 3);
    assert!(statuses.iter().all(|s| s.sender == DELIVERY_STATUS_SENDER));

    activate(&rig.cfg).await;
    let lines = rig.out_lines().await;
    assert_eq!(
        lines[1..],
        [
            format!("status matched=true kind=handed id={id} never=-"),
            format!("status matched=true kind=delivered id={id}"),
            format!("status matched=false kind=delivered id={stranger}"),
        ],
        "{lines:?}"
    );
    assert!(failures(&rig.alerts().await).is_empty());
}

/// Once the consumer is retired, a status about its message has no authority
/// to land on and is refused, committing nothing.
#[tokio::test]
async fn a_status_for_a_retired_consumer_is_sender_gone() {
    let rig = rig("mqt-retired").await;
    rig.publish("forever", "delivery").await;
    let id = rig.published_id().await;
    rig.messenger
        .retire_subscriber_registration(&SubscriberEntryKind::Wasm(rig.slug.clone()));
    assert_eq!(
        rig.report(id, DeliveryKind::Handed).await,
        DeliveryStatusPublish::Refused(DeliveryStatusRefusal::SenderGone)
    );
    assert!(rows(&rig.messenger, &rig.status.address).await.is_empty());
}

//! An app's or a component's MQTT publish from commit to broker: the admission
//! commits it to the client's egress channel, the client's reader hands it to
//! the session, and `mosquitto` receives it. What the broker received is read off its own
//! log (`Received PUBLISH from <session>`), which is the one account of what
//! brenn sent that does not depend on a subscriber being connected at the
//! instant it was sent.

mod connection_state;
mod stop;

use std::time::Duration;

use rumqttc::mqttbytes::QoS;
use rumqttc::{AsyncClient, MqttOptions, Transport};
use tokio::sync::mpsc;
use uuid::Uuid;

use brenn_envelope::delivery::{self, DELIVERY_STATUS_SENDER, DeliveryKind, DeliveryStatus};
use brenn_lib::messaging::config::Depth;
use brenn_lib::mqtt::{MqttLifetime, MqttPublishOptions};
use brenn_messaging::config_reload::Outcome;
use brenn_messaging::{MqttPayload, PublishOrigin};
use brenn_messaging_store::db::load_channel_retained_window_seq;
use brenn_mqtt::broker_gate;
use brenn_mqtt::state::ConnectorHealthLabel;
use brenn_mqtt::test_support::{
    BrokerHarness, certs, log_publishes_from, log_records_publish_from, session_client_id,
    wait_for_health,
};
use brenn_obs::alerting::AlertSeverity;

use super::super::driver::TriggerSource;
use super::super::driver::tests::{
    BootFixture, Booted, Tree, boot_with, document, install_package_from, seat_user, staged_module,
};
use super::super::mqtt_egress_tests::{egress_cursor, egress_uuid, shut_down};
use super::{BROKER_WAIT_SECS, trusted_ca};

/// The agent every document here declares, holding publish authority on each
/// declared client's topics under [`TOPIC_PREFIX`].
const SENDER: &str = "mqtt-sender";

/// The topic prefix the broker's ACL admits, under which every case here
/// publishes.
const TOPIC_PREFIX: &str = "brenn/itest/egress";

/// A port nothing listens on: bound and released, so a client dialing it is
/// refused and keeps retrying.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("an ephemeral port");
    let port = listener.local_addr().expect("its address").port();
    drop(listener);
    port
}

/// One `mqtt_client` per `(slug, port)`, all trusting `ca_file` and retrying
/// every second, and the sender agent with publish authority on each one's
/// topics.
///
/// The one-second backoff is what lets a case wait on a reconnect inside the
/// file's broker wait rather than the default backoff's minute.
fn egress_document(clients: &[(&str, u16)], ca_file: &std::path::Path) -> String {
    egress_document_with(clients, ca_file, "")
}

/// The consumer every component case declares: the `processor-mqtt-test`
/// fixture, holding the `mqtt` grant, woken by each message on `work`.
const COMPONENT: &str = "mqtt-component";

/// A topic Brenn's ACL admits for [`COMPONENT`] and the harness broker's ACL
/// does not, which admits `brenn/itest/#` only.
const BROKER_REFUSED_TOPIC: &str = "brenn/outside/refused";

/// [`egress_document`] over the one client `ha` at `port`, with [`COMPONENT`]
/// beside the sender: its Brenn ACL admits `ha`'s topics under
/// [`TOPIC_PREFIX`] and [`BROKER_REFUSED_TOPIC`], and its `out` port, which the
/// fixture's interleave path writes, is bound to a sink so its `ports` grant
/// reaches something. The sender may also publish on `work`, which is how a
/// case wakes the component.
fn component_document(port: u16, ca_file: &std::path::Path) -> String {
    egress_document_with(
        &[("ha", port)],
        ca_file,
        &format!(
            r#"channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{packaged}component MqttTest {{
    abi = processor;
    requires = [ports, mqtt];
    in inbound;
    out out;
}}
{packaged}
new {COMPONENT}: MqttTest {{
    grants = [ports, mqtt];
    acl publish [topic_filter "mqtt:ha:{TOPIC_PREFIX}/#", topic_filter "mqtt:ha:{BROKER_REFUSED_TOPIC}", exact sink];
    in inbound <- work {{ push_depth = 4; }}
    out out -> sink;
}}
"#,
            packaged = brenn_lib::config::PACKAGED,
        ),
    )
}

/// [`egress_document`] with `extra` after the sender.
fn egress_document_with(clients: &[(&str, u16)], ca_file: &std::path::Path, extra: &str) -> String {
    let blocks: String = clients
        .iter()
        .map(|(slug, port)| {
            format!(
                "mqtt_client {slug} {{\n    url = \"mqtts://127.0.0.1:{port}\";\n    \
                 ca_file = \"{ca}\";\n    qos = 1;\n    reconnect_backoff_initial_secs = 1;\n    \
                 reconnect_backoff_max_secs = 1;\n}}\n",
                ca = ca_file.display(),
            )
        })
        .collect();
    let acl: String = clients
        .iter()
        .map(|(slug, _)| format!("topic_filter \"mqtt:{slug}:{TOPIC_PREFIX}/#\""))
        .collect::<Vec<_>>()
        .join(", ");
    document(&format!(
        r#"{blocks}
agent Sender() {{
    working_dir = ".";
    grants = [publish];
    send_budget = 1000000;
    acl publish [{acl}, exact work];
}}

new {SENDER}: Sender();
{extra}"#
    ))
}

/// Boot `tree` with live sessions and a dispatcher, over `db` when given.
///
/// The dispatcher is what wakes a reader on a commit: the participant's
/// delivery binding fires on its pass, and the relay turns that into every
/// reader's wake.
///
/// It returns without waiting for any session to connect: cases here declare
/// clients on a closed port or a stopped broker, which never reach
/// `Connected`.
async fn boot_dispatching(tree: &Tree, db: Option<brenn_db::Db>) -> Booted {
    boot_with(
        tree,
        BootFixture {
            db,
            mqtt_live: true,
            dispatcher: true,
            ..BootFixture::default()
        },
    )
    .await
}

/// [`boot_dispatching`] over a fresh database with the fixture's package
/// installed under `components`, for a document from [`component_document`].
async fn boot_component(tree: &Tree, components: &std::path::Path) -> Booted {
    install_package_from(
        components,
        &staged_module(tree),
        "brenn_processor_mqtt_test.wasm",
    );
    boot_with(
        tree,
        BootFixture {
            components_roots: vec![components.to_path_buf()],
            mqtt_live: true,
            dispatcher: true,
            ..BootFixture::default()
        },
    )
    .await
}

/// A user and a conversation of the sender agent for the publishes to be
/// attributed to, as `MqttSend` attributes its own.
async fn sender_conversation(booted: &Booted) -> i64 {
    let user = seat_user(booted, "alice").await;
    let conn = booted.db.lock().await;
    brenn_db::conversation::create_conversation(&conn, user, SENDER, false)
}

/// `MqttSend`'s options: QoS 1, the stated retain flag and content type, and
/// a lifetime.
fn options(retain: bool, content_type: Option<&str>, lifetime: MqttLifetime) -> MqttPublishOptions {
    MqttPublishOptions {
        qos: 1,
        retain,
        lifetime,
        content_type: content_type.map(str::to_string),
        status_to: None,
        reply_to: None,
    }
}

fn seconds(n: u32) -> MqttLifetime {
    MqttLifetime::Seconds(std::num::NonZeroU32::new(n).expect("a non-zero lifetime"))
}

/// Publish `payload` on `topic` through `client` as the sender agent, as
/// `MqttSend` does, and return the committed message's id.
async fn send(
    booted: &Booted,
    conversation: i64,
    client: &str,
    topic: &str,
    payload: &[u8],
    options: &MqttPublishOptions,
) -> Uuid {
    booted
        .messenger
        .publish_mqtt(
            PublishOrigin::Conversation { id: conversation },
            SENDER,
            &format!("mqtt:{client}:{topic}"),
            MqttPayload::Inline(payload.to_vec()),
            options,
            None,
        )
        .await
        .unwrap_or_else(|refused| panic!("the publish to {client} was refused: {refused:?}"))
        .message_id
}

/// Wake [`COMPONENT`] with the fixture's parameterised publish: one call
/// through `ha` with these arguments and no message expiry.
async fn wake_component(
    booted: &Booted,
    conversation: i64,
    topic: &str,
    retain: bool,
    content_type: &str,
    payload: &str,
) {
    let body = format!(
        "PUBLISH_TO\nha\n{topic}\n1\n{}\n-\n{content_type}\n{payload}",
        u8::from(retain)
    );
    let published = booted
        .messenger
        .publish(
            PublishOrigin::Conversation { id: conversation },
            SENDER,
            "brenn:work",
            body.into(),
            brenn_messaging::Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(published, brenn_messaging::PublishResult::Ok { .. }),
        "{published:?}"
    );
}

/// The rows on `client`'s egress channel, oldest first, once there are
/// `count`.
async fn await_egress_rows(
    booted: &Booted,
    client: &str,
    count: usize,
) -> Vec<brenn_lib::messaging::MessageEnvelope> {
    let uuid = egress_uuid(&booted.messenger, client);
    brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async {
            let rows: Vec<_> = {
                let conn = booted.db.lock().await;
                load_channel_retained_window_seq(&conn, uuid, Depth::Unbounded)
                    .into_iter()
                    .map(|(_, envelope)| envelope)
                    .collect()
            };
            (rows.len() >= count).then_some(rows)
        },
        || async { format!("{client}'s egress channel never held {count} rows") },
    )
    .await
}

/// Every alert raised so far whose title says a consumer's activation failed.
fn activation_failures(booted: &Booted) -> Vec<(AlertSeverity, String, String)> {
    booted
        .captured
        .lock()
        .expect("alert capture")
        .iter()
        .filter(|(_, t, _)| t.contains("activation failed") || t.contains("activation trapped"))
        .cloned()
        .collect()
}

/// Wait until the broker has received a PUBLISH on each of `topics` from
/// `client`'s session, reading the log from `since`.
async fn await_received(harness: &BrokerHarness, since: usize, client: &str, topics: &[&str]) {
    let session = session_client_id(client);
    harness
        .wait_for_log(
            since,
            BROKER_WAIT_SECS,
            &format!("the broker never received {topics:?} from {session}"),
            |log| {
                topics
                    .iter()
                    .all(|topic| log_records_publish_from(log, &session, topic))
            },
        )
        .await;
}

/// A subscriber on `topic` that hands over each PUBLISH packet it receives,
/// returned once its SUBACK has arrived so nothing published afterwards can
/// race ahead of the subscription.
async fn subscriber(
    port: u16,
    topic: &str,
) -> mpsc::UnboundedReceiver<rumqttc::mqttbytes::v5::Publish> {
    let mut opts = MqttOptions::new(
        format!("egress-witness-{}", Uuid::new_v4().simple()),
        ("127.0.0.1", port),
    );
    opts.set_clean_start(true);
    opts.set_transport(Transport::tls(certs::ca_pem_bytes(), None, None));
    let (client, mut eventloop) = AsyncClient::builder(opts).capacity(16).build();
    client
        .subscribe(topic, QoS::AtLeastOnce)
        .await
        .expect("the subscribe is queued");
    brenn_mqtt::test_support::client::drain_until_incoming(
        &mut eventloop,
        |packet| matches!(packet, rumqttc::Incoming::SubAck(_)),
        "the witness's SubAck",
    )
    .await;
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let _client = client;
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Incoming::Publish(publish))) => {
                    if tx.send(publish).is_err() {
                        return;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("egress witness: eventloop error: {e}");
                    return;
                }
            }
        }
    });
    rx
}

async fn next_publish(
    rx: &mut mpsc::UnboundedReceiver<rumqttc::mqttbytes::v5::Publish>,
    what: &str,
) -> rumqttc::mqttbytes::v5::Publish {
    tokio::time::timeout(Duration::from_secs(BROKER_WAIT_SECS), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing arrived within {BROKER_WAIT_SECS}s"))
        .unwrap_or_else(|| panic!("{what}: the witness stopped"))
}

/// The retained sequences on `client`'s egress channel, oldest first.
async fn retained_seqs(booted: &Booted, client: &str) -> Vec<i64> {
    let uuid = egress_uuid(&booted.messenger, client);
    let conn = booted.db.lock().await;
    load_channel_retained_window_seq(&conn, uuid, Depth::Unbounded)
        .into_iter()
        .map(|(seq, _)| seq)
        .collect()
}

/// The egress alerts raised so far whose title names `kind` and `client`.
fn egress_alerts(
    booted: &Booted,
    kind: &str,
    client: &str,
) -> Vec<(AlertSeverity, String, String)> {
    let title = format!("MQTT egress: {kind} ({client})");
    booted
        .captured
        .lock()
        .expect("alert capture")
        .iter()
        .filter(|(_, t, _)| t.starts_with(&title))
        .cloned()
        .collect()
}

/// With two clients, one at the broker and one at a port nothing listens on:
/// a publish through the first arrives with `MqttSend`'s topic, QoS, retain
/// flag, content type and remaining lifetime as its Message Expiry Interval;
/// the other's publish stays on its own channel, unhanded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publish_arrives_with_its_options_and_an_unreachable_clients_stays_on_its_channel() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&egress_document(
        &[("ha", harness.port), ("down", closed_port())],
        &ca_file,
    ));
    let booted = boot_dispatching(&tree, None).await;
    let service = booted.mqtt.0.clone();
    wait_for_health(
        &service,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the reachable client never connected",
    )
    .await;
    let conversation = sender_conversation(&booted).await;

    let topic = format!("{TOPIC_PREFIX}/options");
    let stuck = format!("{TOPIC_PREFIX}/stuck");
    let mut live = subscriber(harness.port, &topic).await;

    send(
        &booted,
        conversation,
        "down",
        &stuck,
        b"never",
        &options(false, None, MqttLifetime::Forever),
    )
    .await;
    send(
        &booted,
        conversation,
        "ha",
        &topic,
        b"on",
        &options(true, Some("text/plain"), seconds(60)),
    )
    .await;

    let publish = next_publish(&mut live, "the publish through ha").await;
    assert_eq!(&publish.topic[..], topic.as_bytes());
    assert_eq!(&publish.payload[..], b"on");
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    let props = publish.properties.expect("the publish carried properties");
    assert_eq!(props.content_type.as_deref(), Some("text/plain"));
    let expiry = props
        .message_expiry_interval
        .expect("a sixty-second lifetime is sent as a Message Expiry Interval");
    assert!(
        (50..=60).contains(&expiry),
        "expiry {expiry} is not what remained of 60s"
    );

    // Retain is not visible on a live delivery; a subscriber arriving now is
    // handed the broker's retained copy only if the publish carried it.
    let mut late = subscriber(harness.port, &topic).await;
    let retained = next_publish(&mut late, "the retained copy").await;
    assert!(retained.retain, "the broker kept no retained copy");
    assert_eq!(&retained.payload[..], b"on");

    // The unreachable client's message is on its own channel, its position
    // still owes it, and no session sent it anywhere.
    let stuck_seqs = retained_seqs(&booted, "down").await;
    assert_eq!(stuck_seqs.len(), 1, "{stuck_seqs:?}");
    assert_eq!(retained_seqs(&booted, "ha").await.len(), 1);
    let owed = egress_cursor(&booted.messenger, egress_uuid(&booted.messenger, "down"))
        .await
        .expect("the unreachable client's position")
        .next_owed_seq;
    assert!(
        owed <= stuck_seqs[0],
        "the unreachable client's position moved past a message nothing sent",
    );
    assert!(!log_records_publish_from(
        &harness.log(),
        &session_client_id("down"),
        &stuck
    ));

    booted.stop_mqtt();
}

/// A message committed while its client's broker is down waits on its
/// channel. When the broker returns, one whose lifetime still holds is
/// published; one whose lifetime passed is reported expired to the operator
/// and never published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_message_committed_while_the_broker_is_down_is_delivered_or_expires() {
    broker_gate!();

    let mut harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&egress_document(&[("ha", harness.port)], &ca_file));
    let booted = boot_dispatching(&tree, None).await;
    let service = booted.mqtt.0.clone();
    wait_for_health(
        &service,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the client never connected",
    )
    .await;
    let conversation = sender_conversation(&booted).await;

    harness.stop();
    wait_for_health(
        &service,
        "ha",
        &[ConnectorHealthLabel::Disconnected],
        BROKER_WAIT_SECS,
        "the client never noticed the broker go",
    )
    .await;

    let lapsed = format!("{TOPIC_PREFIX}/lapsed");
    let kept = format!("{TOPIC_PREFIX}/kept");
    let lapsed_id = send(
        &booted,
        conversation,
        "ha",
        &lapsed,
        b"too late",
        &options(false, None, seconds(1)),
    )
    .await;
    send(
        &booted,
        conversation,
        "ha",
        &kept,
        b"in time",
        &options(false, None, seconds(600)),
    )
    .await;
    // Past the short lifetime, with the broker still away.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let since = harness.log_len();
    harness.restart();
    await_received(&harness, since, "ha", &[&kept]).await;

    // The reader judged the lapsed message before it handed the kept one, so
    // its report is in before the kept one reached the broker.
    let expired = brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async {
            let found = egress_alerts(&booted, "expired", "ha");
            (!found.is_empty()).then_some(found)
        },
        || async { "no expired alert for ha".to_string() },
    )
    .await;
    assert_eq!(expired.len(), 1, "{expired:?}");
    assert!(
        expired[0].2.contains(&lapsed_id.to_string()),
        "the alert names the lapsed message: {expired:?}",
    );
    assert!(
        !log_records_publish_from(&harness.log(), &session_client_id("ha"), &lapsed),
        "a message whose lifetime passed was published",
    );

    booted.stop_mqtt();
}

/// A second boot over the same database hands off exactly what the first did
/// not: a message the first process delivered is not published again, and the
/// ones committed while its broker was away are.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_hands_off_exactly_what_was_not_handed() {
    broker_gate!();

    let mut harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&egress_document(&[("ha", harness.port)], &ca_file));
    let db = brenn_server::test_support::init_db_memory();

    let first = boot_dispatching(&tree, Some(db.clone())).await;
    let service = first.mqtt.0.clone();
    wait_for_health(
        &service,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the first process never connected",
    )
    .await;
    let conversation = sender_conversation(&first).await;
    let handed = format!("{TOPIC_PREFIX}/handed");
    let owed = [
        format!("{TOPIC_PREFIX}/owed-1"),
        format!("{TOPIC_PREFIX}/owed-2"),
    ];
    send(
        &first,
        conversation,
        "ha",
        &handed,
        b"once",
        &options(false, None, MqttLifetime::Forever),
    )
    .await;
    await_received(&harness, 0, "ha", &[&handed]).await;

    harness.stop();
    wait_for_health(
        &service,
        "ha",
        &[ConnectorHealthLabel::Disconnected],
        BROKER_WAIT_SECS,
        "the first process never noticed the broker go",
    )
    .await;
    for topic in &owed {
        send(
            &first,
            conversation,
            "ha",
            topic,
            b"later",
            &options(false, None, MqttLifetime::Forever),
        )
        .await;
    }
    shut_down(first).await;

    harness.restart();
    let second = boot_dispatching(&tree, Some(db)).await;
    await_received(&harness, 0, "ha", &[&owed[0], &owed[1]]).await;
    let log = harness.log();
    let session = session_client_id("ha");
    assert_eq!(
        log_publishes_from(&log, &session, &handed),
        1,
        "the message the first process handed was published again",
    );
    for topic in &owed {
        assert_eq!(log_publishes_from(&log, &session, topic), 1, "{topic}");
    }

    second.stop_mqtt();
}

/// Reload: a client the reload adds publishes its first message to its
/// broker, and messages committed for a client before a reload changes it are
/// delivered by the successor session from the same position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_added_client_publishes_and_a_changed_clients_successor_delivers_what_was_owed() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    // `ha` starts at a port nothing listens on; the reload moves it to the
    // broker and adds `spare` there.
    let tree = Tree::holding(&egress_document(&[("ha", closed_port())], &ca_file));
    let mut booted = boot_dispatching(&tree, None).await;
    let conversation = sender_conversation(&booted).await;

    let owed = [
        format!("{TOPIC_PREFIX}/changed-1"),
        format!("{TOPIC_PREFIX}/changed-2"),
    ];
    for topic in &owed {
        send(
            &booted,
            conversation,
            "ha",
            topic,
            b"owed",
            &options(false, None, seconds(600)),
        )
        .await;
    }

    tree.write(&egress_document(
        &[("ha", harness.port), ("spare", harness.port)],
        &ca_file,
    ));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec!["ha".to_string()]);
    assert_eq!(status.delta.mqtt_clients_added, vec!["spare".to_string()]);

    await_received(&harness, 0, "ha", &[&owed[0], &owed[1]]).await;

    let added = format!("{TOPIC_PREFIX}/added");
    send(
        &booted,
        conversation,
        "spare",
        &added,
        b"first",
        &options(false, None, seconds(600)),
    )
    .await;
    await_received(&harness, 0, "spare", &[&added]).await;

    booted.stop_mqtt();
}

/// A component's publish reaches the broker through the client's reader, with
/// the topic, payload, QoS, retain flag and content type it was made with, and
/// its row on the egress channel is the component's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_components_publish_arrives_at_the_broker() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&component_document(harness.port, &ca_file));
    let booted = boot_component(&tree, components.path()).await;
    wait_for_health(
        &booted.mqtt.0,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the client never connected",
    )
    .await;
    let conversation = sender_conversation(&booted).await;
    let topic = format!("{TOPIC_PREFIX}/component");
    let mut live = subscriber(harness.port, &topic).await;

    wake_component(&booted, conversation, &topic, true, "text/plain", "lit").await;

    let publish = next_publish(&mut live, "the component's publish").await;
    assert_eq!(&publish.topic[..], topic.as_bytes());
    assert_eq!(&publish.payload[..], b"lit");
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    let props = publish.properties.expect("the publish carried properties");
    assert_eq!(props.content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        props.message_expiry_interval, None,
        "mqtt-publish sends no Message Expiry Interval"
    );
    let mut late = subscriber(harness.port, &topic).await;
    let retained = next_publish(&mut late, "the retained copy").await;
    assert!(retained.retain, "the broker kept no retained copy");
    assert_eq!(&retained.payload[..], b"lit");

    let rows = await_egress_rows(&booted, "ha", 1).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        rows[0].sender,
        brenn_lib::messaging::ParticipantId::for_wasm(COMPONENT).as_str()
    );
    assert!(activation_failures(&booted).is_empty());

    booted.stop_mqtt();
}

/// With the client at a port nothing listens on, the component's call is
/// admitted and the activation commits without waiting on a broker: its row
/// is on the channel and nothing failed. A reload that moves the client to
/// the live broker delivers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_components_publish_to_an_unreachable_client_waits_on_the_channel_until_a_reload() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&component_document(closed_port(), &ca_file));
    let mut booted = boot_component(&tree, components.path()).await;
    let conversation = sender_conversation(&booted).await;
    let topic = format!("{TOPIC_PREFIX}/component-owed");

    wake_component(&booted, conversation, &topic, false, "-", "owed").await;
    let rows = await_egress_rows(&booted, "ha", 1).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(
        activation_failures(&booted).is_empty(),
        "the call was admitted and the activation committed: {:?}",
        activation_failures(&booted)
    );

    tree.write(&component_document(harness.port, &ca_file));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec!["ha".to_string()]);

    await_received(&harness, 0, "ha", &[&topic]).await;

    booted.stop_mqtt();
}

/// A topic Brenn admits and the broker refuses, published by a component at
/// QoS 1, is reported to the operator as rejected with the component named
/// as its sender. (A QoS 0 message counts as delivered once written, so its
/// refusal is never reported.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_refusal_of_a_components_publish_names_the_component() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&component_document(harness.port, &ca_file));
    let booted = boot_component(&tree, components.path()).await;
    wait_for_health(
        &booted.mqtt.0,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the client never connected",
    )
    .await;
    let conversation = sender_conversation(&booted).await;

    wake_component(
        &booted,
        conversation,
        BROKER_REFUSED_TOPIC,
        false,
        "-",
        "refused",
    )
    .await;

    let rejected = brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async {
            let found = egress_alerts(&booted, "rejected", "ha");
            (!found.is_empty()).then_some(found)
        },
        || async { "no rejected alert for ha".to_string() },
    )
    .await;
    assert_eq!(rejected.len(), 1, "{rejected:?}");
    let sender = brenn_lib::messaging::ParticipantId::for_wasm(COMPONENT);
    assert!(
        rejected[0].2.contains(&format!("from {}", sender.as_str()))
            && rejected[0].2.contains(BROKER_REFUSED_TOPIC),
        "the alert names the component and the topic: {rejected:?}",
    );
    assert!(activation_failures(&booted).is_empty());

    booted.stop_mqtt();
}

/// The status channel every `status_to` case names.
const STATUS: &str = "brenn:status";

/// The one client `ha` at `port`, retrying every second, backing off at most
/// `backoff_max` seconds, and waiting a second for the broker at a stop, since
/// some cases stop it with the broker frozen; the status channel [`STATUS`] when `status` is
/// given, with `status` as extra lines of its block (a `send_rate`, or empty);
/// and the sender agent, which may publish to `ha`'s topics under
/// [`TOPIC_PREFIX`], to [`BROKER_REFUSED_TOPIC`], and to the status channel
/// when it is declared.
fn status_document(
    port: u16,
    ca_file: &std::path::Path,
    status: Option<&str>,
    backoff_max: u32,
) -> String {
    let status_acl = if status.is_some() {
        ", exact status"
    } else {
        ""
    };
    let client = client_block("ha", port, ca_file, backoff_max, 1);
    let channel = status.map_or_else(String::new, status_channel);
    document(&format!(
        r#"{client}
{channel}
agent Sender() {{
    working_dir = ".";
    grants = [publish];
    send_budget = 1000000;
    acl publish [topic_filter "mqtt:ha:{TOPIC_PREFIX}/#", topic_filter "mqtt:ha:{BROKER_REFUSED_TOPIC}"{status_acl}];
}}

new {SENDER}: Sender();
"#,
    ))
}

/// The `mqtt_client` block for `slug` at `port` with QoS 1, retrying every
/// second, backing off at most `backoff_max` seconds and waiting
/// `stop_wait_secs` for the broker at a stop.
fn client_block(
    slug: &str,
    port: u16,
    ca_file: &std::path::Path,
    backoff_max: u32,
    stop_wait_secs: u32,
) -> String {
    format!(
        "mqtt_client {slug} {{\n    url = \"mqtts://127.0.0.1:{port}\";\n    \
         ca_file = \"{ca}\";\n    qos = 1;\n    reconnect_backoff_initial_secs = 1;\n    \
         reconnect_backoff_max_secs = {backoff_max};\n    \
         stop_wait_secs = {stop_wait_secs};\n}}\n",
        ca = ca_file.display(),
    )
}

/// The status channel [`STATUS`], with `extra` as extra lines of its block.
fn status_channel(extra: &str) -> String {
    format!(
        "channel status at \"{STATUS}\" {{\n    push_depth = 1;\n    retain_depth = 64;\n    \
         standing_retain_depth = 64;\n{extra}}}\n"
    )
}

/// [`options`] at QoS 1 with a minute's lifetime, naming `status_to`.
fn telling(status_to: Option<&str>, lifetime: MqttLifetime) -> MqttPublishOptions {
    MqttPublishOptions {
        status_to: status_to.map(str::to_string),
        ..options(false, None, lifetime)
    }
}

/// The statuses on [`STATUS`], oldest first, each checked to be from
/// `system:delivery-status`.
async fn statuses(booted: &Booted) -> Vec<DeliveryStatus> {
    let uuid = booted
        .messenger
        .directory()
        .resolve(STATUS)
        .expect("the status channel is declared")
        .uuid;
    let conn = booted.db.lock().await;
    load_channel_retained_window_seq(&conn, uuid, Depth::Unbounded)
        .into_iter()
        .map(|(_, envelope)| {
            assert_eq!(envelope.sender, DELIVERY_STATUS_SENDER, "{envelope:?}");
            match delivery::decode(envelope.body.as_text().expect("a status is text"))
                .expect("a v1 status")
            {
                delivery::StatusBody::Message(status) => status,
                other => panic!("expected a message status, got {other:?}"),
            }
        })
        .collect()
}

/// The statuses on [`STATUS`] once there are at least `count`.
async fn await_statuses(booted: &Booted, count: usize) -> Vec<DeliveryStatus> {
    brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async {
            let found = statuses(booted).await;
            (found.len() >= count).then_some(found)
        },
        || async { format!("{STATUS} never held {count} statuses") },
    )
    .await
}

/// Each status's `(message id, kind)`, in order.
fn kinds(found: &[DeliveryStatus]) -> Vec<(Uuid, DeliveryKind)> {
    found
        .iter()
        .map(|status| (status.message_id, status.kind.clone()))
        .collect()
}

/// Wait until exactly `count` rejected alerts have been raised for client
/// `ha`, and answer them.
async fn await_rejected_alerts(
    booted: &Booted,
    count: usize,
) -> Vec<(AlertSeverity, String, String)> {
    brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async {
            let found = egress_alerts(booted, "rejected", "ha");
            (found.len() >= count).then_some(found)
        },
        || async { format!("fewer than {count} rejected alerts") },
    )
    .await
}

/// A publish naming a `status_to` is told `handed` then `delivered`, and one
/// the broker refuses `handed` then `rejected` with the broker's reason. A
/// broker's refusal alerts the operator whether or not its sender was told:
/// the untold one first, then, after a delivery closed its episode, the told
/// one, each opening its own episode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_status_to_hears_handed_then_its_outcome_and_a_broker_rejection_alerts_told_or_not() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&status_document(harness.port, &ca_file, Some(""), 1));
    let booted = boot_dispatching(&tree, None).await;
    wait_for_health(
        &booted.mqtt.0,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the client never connected",
    )
    .await;
    let conversation = sender_conversation(&booted).await;
    let topic = format!("{TOPIC_PREFIX}/told");

    let untold = send(
        &booted,
        conversation,
        "ha",
        BROKER_REFUSED_TOPIC,
        b"no",
        &telling(None, seconds(60)),
    )
    .await;
    let alerted = await_rejected_alerts(&booted, 1).await;
    assert_eq!(alerted.len(), 1, "{alerted:?}");
    assert!(alerted[0].2.contains(&untold.to_string()), "{alerted:?}");
    assert!(
        statuses(&booted).await.is_empty(),
        "an untold publish publishes no status"
    );

    let delivered = send(
        &booted,
        conversation,
        "ha",
        &topic,
        b"on",
        &telling(Some(STATUS), seconds(60)),
    )
    .await;
    let found = await_statuses(&booted, 2).await;
    assert_eq!(
        kinds(&found),
        vec![
            (delivered, DeliveryKind::Handed),
            (delivered, DeliveryKind::Delivered)
        ],
    );
    let mqtt = found[1]
        .mqtt
        .as_ref()
        .expect("an MQTT status names its publish");
    assert_eq!(
        (
            mqtt.client.as_str(),
            mqtt.topic.as_str(),
            mqtt.qos,
            mqtt.reason_code
        ),
        ("ha", topic.as_str(), 1, None),
    );

    let told = send(
        &booted,
        conversation,
        "ha",
        BROKER_REFUSED_TOPIC,
        b"no",
        &telling(Some(STATUS), seconds(60)),
    )
    .await;
    let found = await_statuses(&booted, 4).await;
    assert_eq!(
        (found[2].message_id, &found[2].kind),
        (told, &DeliveryKind::Handed)
    );
    assert_eq!(found[3].message_id, told);
    let DeliveryKind::Rejected { reason } = &found[3].kind else {
        panic!("the broker's refusal is told as rejected: {:?}", found[3]);
    };
    assert!(
        !reason.is_empty(),
        "the rejection carries the broker's reason"
    );
    let code = found[3]
        .mqtt
        .as_ref()
        .and_then(|mqtt| mqtt.reason_code)
        .expect("a broker rejection carries its reason code");
    assert!(code >= 0x80, "a refusal's reason code is a failure: {code}");
    let alerted = await_rejected_alerts(&booted, 2).await;
    assert_eq!(
        alerted.len(),
        2,
        "the delivery closed the first episode with nothing withheld: {alerted:?}"
    );
    assert!(alerted[1].2.contains(&told.to_string()), "{alerted:?}");
    assert!(
        alerted[1]
            .2
            .contains("Its sender was told on its status channel."),
        "{alerted:?}"
    );
    assert_eq!(statuses(&booted).await.len(), 4);

    booted.stop_mqtt();
}

/// A message whose lifetime ends while the broker is down is told nothing
/// until its client connects, then `expired`, with no `handed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lifetime_that_ends_while_the_broker_is_down_is_told_expired_at_reconnect() {
    broker_gate!();

    let mut harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&status_document(harness.port, &ca_file, Some(""), 1));
    let booted = boot_dispatching(&tree, None).await;
    let service = booted.mqtt.0.clone();
    wait_for_health(
        &service,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the client never connected",
    )
    .await;
    let conversation = sender_conversation(&booted).await;

    harness.stop();
    wait_for_health(
        &service,
        "ha",
        &[ConnectorHealthLabel::Disconnected],
        BROKER_WAIT_SECS,
        "the client never noticed the broker go",
    )
    .await;

    let lapsed = send(
        &booted,
        conversation,
        "ha",
        &format!("{TOPIC_PREFIX}/lapsed-told"),
        b"too late",
        &telling(Some(STATUS), seconds(1)),
    )
    .await;
    let kept = format!("{TOPIC_PREFIX}/kept-after-lapse");
    send(
        &booted,
        conversation,
        "ha",
        &kept,
        b"in time",
        &telling(None, seconds(600)),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        statuses(&booted).await.is_empty(),
        "nothing is told while the client has no connection",
    );

    let since = harness.log_len();
    harness.restart();
    await_received(&harness, since, "ha", &[&kept]).await;
    let found = await_statuses(&booted, 1).await;
    assert_eq!(kinds(&found), vec![(lapsed, DeliveryKind::Expired)]);
    assert!(egress_alerts(&booted, "expired", "ha").is_empty());

    booted.stop_mqtt();
}

/// A status channel whose send rate admits one status: the first publish's
/// `handed` is published and every later status refused. The refused
/// statuses' messages still go out, the refusal is alerted, and nothing
/// panics.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_status_holds_nothing_back_and_is_alerted() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&status_document(
        harness.port,
        &ca_file,
        Some("    send_rate = { burst = 1, refill_interval_secs = 3600, refill = 1 };\n"),
        1,
    ));
    let booted = boot_dispatching(&tree, None).await;
    wait_for_health(
        &booted.mqtt.0,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the client never connected",
    )
    .await;
    let conversation = sender_conversation(&booted).await;
    let (first_topic, second_topic, third_topic) = (
        format!("{TOPIC_PREFIX}/rate-first"),
        format!("{TOPIC_PREFIX}/rate-second"),
        format!("{TOPIC_PREFIX}/rate-third"),
    );

    let since = harness.log_len();
    let first = send(
        &booted,
        conversation,
        "ha",
        &first_topic,
        b"1",
        &telling(Some(STATUS), seconds(60)),
    )
    .await;
    send(
        &booted,
        conversation,
        "ha",
        &second_topic,
        b"2",
        &telling(Some(STATUS), seconds(60)),
    )
    .await;
    await_received(&harness, since, "ha", &[&first_topic, &second_topic]).await;

    let refused = brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async {
            let found: Vec<_> = egress_alerts(&booted, "status not published", "ha")
                .into_iter()
                .filter(|(_, title, _)| !title.ends_with("summary"))
                .collect();
            (!found.is_empty()).then_some(found)
        },
        || async { "no status-not-published alert for ha".to_string() },
    )
    .await;
    // TODO(mqtt-refused-status-alert-test-sees-two-alerts): once failed here under full `make check`, two alerts.
    assert_eq!(refused.len(), 1, "one alert opens the episode: {refused:?}");
    assert!(refused[0].2.contains(STATUS), "{refused:?}");
    assert_eq!(
        kinds(&statuses(&booted).await),
        vec![(first, DeliveryKind::Handed)],
        "only the first status fits the send rate",
    );

    // The reader carries on: a later publish is delivered.
    let since = harness.log_len();
    send(
        &booted,
        conversation,
        "ha",
        &third_topic,
        b"3",
        &telling(None, seconds(60)),
    )
    .await;
    await_received(&harness, since, "ha", &[&third_topic]).await;

    booted.stop_mqtt();
}

/// A changed client is stopped after the reload's directory swap, so the
/// statuses of its stop see the new directory and policies: a message in
/// flight when a reload changes its client and removes its status channel
/// (and the sender's right to it) has its `unknown` status refused, and both
/// the refusal and the untold failure are alerted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_clients_stop_statuses_to_a_channel_the_reload_removes_are_refused() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&status_document(harness.port, &ca_file, Some(""), 1));
    let mut booted = boot_dispatching(&tree, None).await;
    wait_for_health(
        &booted.mqtt.0,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the client never connected",
    )
    .await;
    let conversation = sender_conversation(&booted).await;

    // A frozen broker acknowledges nothing, so the publish stays in flight.
    harness.pause();
    let in_flight = send(
        &booted,
        conversation,
        "ha",
        &format!("{TOPIC_PREFIX}/in-flight"),
        b"pending",
        &telling(Some(STATUS), seconds(600)),
    )
    .await;
    assert_eq!(
        kinds(&await_statuses(&booted, 1).await),
        vec![(in_flight, DeliveryKind::Handed)],
    );

    tree.write(&status_document(harness.port, &ca_file, None, 2));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec!["ha".to_string()]);
    assert!(booted.messenger.directory().resolve(STATUS).is_none());

    let refused = egress_alerts(&booted, "status not published", "ha");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert!(
        refused[0].2.contains(&in_flight.to_string()) && refused[0].2.contains("\"unknown\""),
        "{refused:?}"
    );
    let unknown = egress_alerts(&booted, "outcome unknown", "ha");
    assert_eq!(
        unknown.len(),
        1,
        "an untold failure is alerted: {unknown:?}"
    );
    assert!(unknown[0].2.contains(&in_flight.to_string()), "{unknown:?}");

    harness.resume();
    booted.stop_mqtt();
}

/// Publish as [`send`] does, parked until `release_at`.
async fn send_at(
    booted: &Booted,
    conversation: i64,
    topic: &str,
    options: &MqttPublishOptions,
    release_at: chrono::DateTime<chrono::Utc>,
) -> Uuid {
    booted
        .messenger
        .publish_mqtt(
            PublishOrigin::Conversation { id: conversation },
            SENDER,
            &format!("mqtt:ha:{topic}"),
            MqttPayload::Inline(b"later".to_vec()),
            options,
            Some(release_at),
        )
        .await
        .unwrap_or_else(|refused| panic!("the deferred publish was refused: {refused:?}"))
        .message_id
}

/// A whole-second instant `secs` from now: the database keeps release times
/// at second granularity.
fn whole_seconds_from_now(secs: i64) -> chrono::DateTime<chrono::Utc> {
    let at = chrono::Utc::now() + chrono::Duration::seconds(secs);
    chrono::DateTime::from_timestamp(at.timestamp(), 0).expect("representable")
}

/// A deferred publish naming a `status_to` is told nothing and reaches no
/// broker before its release time; at release the dispatcher moves it into
/// the channel, and it is told `handed` then `delivered` and received.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deferred_publish_is_sent_and_told_only_after_its_release() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&status_document(harness.port, &ca_file, Some(""), 1));
    let booted = boot_dispatching(&tree, None).await;
    wait_for_health(
        &booted.mqtt.0,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the client never connected",
    )
    .await;
    let conversation = sender_conversation(&booted).await;
    let topic = format!("{TOPIC_PREFIX}/deferred-told");

    let since = harness.log_len();
    let release_at = whole_seconds_from_now(3);
    let parked = send_at(
        &booted,
        conversation,
        &topic,
        &telling(Some(STATUS), seconds(60)),
        release_at,
    )
    .await;
    assert!(
        retained_seqs(&booted, "ha").await.is_empty(),
        "a parked publish holds no place on the channel"
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(
        statuses(&booted).await.is_empty(),
        "nothing told before release"
    );
    assert!(
        !log_records_publish_from(&harness.log()[since..], &session_client_id("ha"), &topic),
        "nothing sent before release"
    );

    await_received(&harness, since, "ha", &[&topic]).await;
    assert!(chrono::Utc::now() >= release_at, "sent at or after release");
    let found = await_statuses(&booted, 2).await;
    assert_eq!(
        kinds(&found),
        vec![
            (parked, DeliveryKind::Handed),
            (parked, DeliveryKind::Delivered)
        ],
    );

    booted.stop_mqtt();
}

/// A publish parked by one process is released by the next one over the same
/// database: one whose lifetime is still running is sent, and one whose
/// lifetime, counted from its release time, ended while no process ran is
/// told `expired` with no `handed` and not sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_releases_what_was_parked_and_expires_what_outlived_its_lifetime() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&status_document(harness.port, &ca_file, Some(""), 1));
    let db = brenn_server::test_support::init_db_memory();

    let first = boot_dispatching(&tree, Some(db.clone())).await;
    let conversation = sender_conversation(&first).await;
    let kept = format!("{TOPIC_PREFIX}/parked-kept");
    let lapsed = format!("{TOPIC_PREFIX}/parked-lapsed");
    let release_at = whole_seconds_from_now(4);
    send_at(
        &first,
        conversation,
        &kept,
        &telling(None, MqttLifetime::Forever),
        release_at,
    )
    .await;
    let lapsed_id = send_at(
        &first,
        conversation,
        &lapsed,
        &telling(Some(STATUS), seconds(1)),
        release_at,
    )
    .await;
    shut_down(first).await;
    let wait = release_at + chrono::Duration::seconds(2) - chrono::Utc::now();
    tokio::time::sleep(wait.to_std().unwrap_or_default()).await;

    let since = harness.log_len();
    let second = boot_dispatching(&tree, Some(db)).await;
    await_received(&harness, since, "ha", &[&kept]).await;
    let found = await_statuses(&second, 1).await;
    assert_eq!(kinds(&found), vec![(lapsed_id, DeliveryKind::Expired)]);
    assert!(
        !log_records_publish_from(&harness.log()[since..], &session_client_id("ha"), &lapsed),
        "the lapsed publish was not sent"
    );

    second.stop_mqtt();
}

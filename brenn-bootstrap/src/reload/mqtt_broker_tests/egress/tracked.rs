//! A component's tracked publish against the real broker: the
//! `processor-mqtt-tracked-test` fixture states a lifetime, a reply topic and
//! its `delivery` port as its status port; the packet the broker hands on
//! carries the Response Topic and the lifetime's Message Expiry Interval; the
//! egress reader's statuses land on the channel wired to `delivery` under the
//! component's authority, and the component matches them to the id its call
//! returned.

use rumqttc::mqttbytes::QoS;
use uuid::Uuid;

use brenn_envelope::delivery::DeliveryKind;
use brenn_lib::messaging::ParticipantId;
use brenn_lib::messaging::config::Depth;
use brenn_messaging::PublishOrigin;
use brenn_messaging_store::db::load_channel_retained_window_seq;
use brenn_mqtt::broker_gate;
use brenn_mqtt::state::ConnectorHealthLabel;
use brenn_mqtt::test_support::{BrokerHarness, wait_for_health};

use super::super::super::driver::tests::{
    BootFixture, Booted, Tree, boot_with, install_package_from, staged_module,
};
use super::super::{BROKER_WAIT_SECS, trusted_ca};
use super::{
    BROKER_REFUSED_TOPIC, SENDER, TOPIC_PREFIX, activation_failures, await_egress_rows,
    await_rejected_alerts, await_statuses, egress_document_with, kinds, next_publish,
    sender_conversation, status_channel, subscriber,
};

/// The consumer of the tracked fixture.
const TRACKED: &str = "tracked-component";

/// The channel the fixture's `out` port writes, which the cases read its
/// report lines off.
const TRACKED_OUT: &str = "brenn:tracked-out";

/// The reply topic every case names, inside the component's ACL.
fn reply_topic() -> String {
    format!("{TOPIC_PREFIX}/replies")
}

/// [`egress_document_with`] over the one client `ha` at `port`, with the
/// status channel [`STATUS`] and [`TRACKED`] beside the sender. Its ACL admits
/// `ha`'s topics under [`TOPIC_PREFIX`] and [`BROKER_REFUSED_TOPIC`] and its
/// three output channels; `delivery` is `io` on [`STATUS`], `state` is `io` on
/// a channel recording its ids, and `broker` reads `ha`'s connection state.
fn tracked_document(port: u16, ca_file: &std::path::Path) -> String {
    egress_document_with(
        &[("ha", port)],
        ca_file,
        &format!(
            r#"{status}
channel tracked_out at "{TRACKED_OUT}" {{
    push_depth = 1;
    retain_depth = 64;
    standing_retain_depth = 64;
}}
channel tracked_state at "brenn:tracked-state" {{
    push_depth = 1;
    retain_depth = 64;
    standing_retain_depth = 64;
}}
{packaged}component TrackedTest {{
    abi = processor;
    requires = [ports, mqtt];
    in inbound;
    out out;
    io delivery;
    optional out spare;
    optional in broker;
    io state;
}}
{packaged}
new {TRACKED}: TrackedTest {{
    grants = [ports, mqtt];
    acl publish [topic_filter "mqtt:ha:{TOPIC_PREFIX}/#", topic_filter "mqtt:ha:{BROKER_REFUSED_TOPIC}", exact tracked_out, exact status, exact tracked_state];
    in inbound <- work {{ push_depth = 4; }}
    out out -> tracked_out;
    io delivery <-> status {{ push_depth = 8; retain_depth = 64; }}
    io state <-> tracked_state {{ push_depth = 0; retain_depth = 64; }}
    in broker <- "brenn:mqtt-connection.ha" {{ push_depth = 0; retain_depth = 1; }}
}}
"#,
            status = status_channel(""),
            packaged = brenn_lib::config::PACKAGED,
        ),
    )
}

/// Boot `tree` over a fresh database with the tracked fixture's package
/// installed under `components`, and wait for `ha` to connect.
async fn boot_tracked(tree: &Tree, components: &std::path::Path) -> Booted {
    install_package_from(
        components,
        &staged_module(tree),
        "brenn_processor_mqtt_tracked_test.wasm",
    );
    let booted = boot_with(
        tree,
        BootFixture {
            components_roots: vec![components.to_path_buf()],
            mqtt_live: true,
            dispatcher: true,
            ..BootFixture::default()
        },
    )
    .await;
    wait_for_health(
        &booted.mqtt.0,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the client never connected",
    )
    .await;
    booted
}

/// Wake [`TRACKED`] with one tracked publish through `ha`: QoS 1, no retain,
/// no content type, the reply topic, and this lifetime and status port.
async fn wake_tracked(
    booted: &Booted,
    conversation: i64,
    topic: &str,
    lifetime: &str,
    status_port: &str,
    payload: &str,
) {
    let body = format!(
        "TRACKED\nha\n{topic}\n1\n0\n{lifetime}\n-\n{status_port}\n{}\n{payload}",
        reply_topic()
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

/// The component's report lines on [`TRACKED_OUT`], oldest first.
async fn out_lines(booted: &Booted) -> Vec<String> {
    let uuid = booted
        .messenger
        .directory()
        .resolve(TRACKED_OUT)
        .expect("the out channel is declared")
        .uuid;
    let conn = booted.db.lock().await;
    load_channel_retained_window_seq(&conn, uuid, Depth::Unbounded)
        .into_iter()
        .map(|(_, envelope)| {
            envelope
                .body
                .as_text()
                .expect("a report line is text")
                .to_string()
        })
        .collect()
}

/// The component's `published` ids and `status` lines once it has written
/// `ids` of the first and `count` of the second.
async fn await_report(booted: &Booted, ids: usize, count: usize) -> (Vec<Uuid>, Vec<String>) {
    brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async {
            let lines = out_lines(booted).await;
            let published: Vec<Uuid> = lines
                .iter()
                .filter_map(|line| line.strip_prefix("published "))
                .map(|id| Uuid::parse_str(id).expect("the component publishes a UUID"))
                .collect();
            let statuses: Vec<String> = lines
                .into_iter()
                .filter(|line| line.starts_with("status "))
                .collect();
            (published.len() >= ids && statuses.len() >= count).then_some((published, statuses))
        },
        || async {
            format!(
                "{TRACKED} never reported {ids} ids and {count} statuses: {:?}",
                out_lines(booted).await
            )
        },
    )
    .await
}

/// The packet a tracked publish puts on the wire carries the reply topic as
/// its Response Topic, and what remains of a stated lifetime as its Message
/// Expiry Interval; a `forever` publish carries no expiry. The egress rows are
/// the component's, under the ids its calls returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tracked_publish_arrives_with_its_response_topic_and_lifetime() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&tracked_document(harness.port, &ca_file));
    let booted = boot_tracked(&tree, components.path()).await;
    let conversation = sender_conversation(&booted).await;
    let topic = format!("{TOPIC_PREFIX}/tracked");
    let mut live = subscriber(harness.port, &topic).await;

    wake_tracked(&booted, conversation, &topic, "60", "-", "minute").await;
    let publish = next_publish(&mut live, "the publish with a lifetime").await;
    assert_eq!(&publish.topic[..], topic.as_bytes());
    assert_eq!(&publish.payload[..], b"minute");
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    let props = publish.properties.expect("the publish carried properties");
    assert_eq!(
        props.response_topic.as_deref(),
        Some(reply_topic().as_str())
    );
    let expiry = props
        .message_expiry_interval
        .expect("a sixty-second lifetime is sent as a Message Expiry Interval");
    assert!(
        (50..=60).contains(&expiry),
        "expiry {expiry} is not what remained of 60s"
    );

    wake_tracked(&booted, conversation, &topic, "forever", "-", "always").await;
    let publish = next_publish(&mut live, "the forever publish").await;
    assert_eq!(&publish.payload[..], b"always");
    let props = publish.properties.expect("the publish carried properties");
    assert_eq!(
        props.response_topic.as_deref(),
        Some(reply_topic().as_str())
    );
    assert_eq!(
        props.message_expiry_interval, None,
        "a forever publish sends no Message Expiry Interval"
    );

    let (ids, _) = await_report(&booted, 2, 0).await;
    let rows = await_egress_rows(&booted, "ha", 2).await;
    assert_eq!(
        rows.iter().map(|row| row.message_id).collect::<Vec<_>>(),
        ids,
        "each row is under the id its call returned"
    );
    for row in &rows {
        assert_eq!(row.sender, ParticipantId::for_wasm(TRACKED).as_str());
    }
    assert!(activation_failures(&booted).is_empty());

    booted.stop_mqtt();
}

/// The end-to-end match: a publish naming `delivery` as its status port is
/// told `handed` then `delivered` on the channel `delivery` is wired to, both
/// carrying the id the call returned, and the component's next activations
/// read both off `delivery` and match them to its publish. The `handed`
/// postdates the client's connection state, so it is not one whose outcome
/// will never come.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_component_hears_handed_then_delivered_and_matches_both() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&tracked_document(harness.port, &ca_file));
    let booted = boot_tracked(&tree, components.path()).await;
    let conversation = sender_conversation(&booted).await;
    let topic = format!("{TOPIC_PREFIX}/matched");

    wake_tracked(&booted, conversation, &topic, "60", "delivery", "on").await;

    let (ids, lines) = await_report(&booted, 1, 2).await;
    let [id] = ids.as_slice() else {
        panic!("one publish, got {ids:?}");
    };
    let found = await_statuses(&booted, 2).await;
    assert_eq!(
        kinds(&found),
        vec![(*id, DeliveryKind::Handed), (*id, DeliveryKind::Delivered)],
    );
    assert_eq!(
        lines,
        vec![
            format!("status matched=true kind=handed id={id} never=false"),
            format!("status matched=true kind=delivered id={id}"),
        ],
    );
    assert!(activation_failures(&booted).is_empty());

    booted.stop_mqtt();
}

/// A topic the broker refuses, published with a status port, is told to the
/// component as `handed` then `rejected` and matched, and is still alerted to
/// the operator, saying its sender was told.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_refusal_is_told_to_the_component_and_still_alerted() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&tracked_document(harness.port, &ca_file));
    let booted = boot_tracked(&tree, components.path()).await;
    let conversation = sender_conversation(&booted).await;

    wake_tracked(
        &booted,
        conversation,
        BROKER_REFUSED_TOPIC,
        "forever",
        "delivery",
        "no",
    )
    .await;

    let (ids, lines) = await_report(&booted, 1, 2).await;
    let [id] = ids.as_slice() else {
        panic!("one publish, got {ids:?}");
    };
    assert_eq!(
        lines[0],
        format!("status matched=true kind=handed id={id} never=false")
    );
    assert_eq!(
        lines[1],
        format!("status matched=true kind=rejected id={id}")
    );

    let alerted = await_rejected_alerts(&booted, 1).await;
    assert_eq!(alerted.len(), 1, "{alerted:?}");
    let sender = ParticipantId::for_wasm(TRACKED);
    assert!(
        alerted[0].2.contains(&id.to_string())
            && alerted[0].2.contains(&format!("from {}", sender.as_str()))
            && alerted[0]
                .2
                .contains("Its sender was told on its status channel."),
        "the alert names the message and the component, and says it was told: {alerted:?}",
    );
    assert!(activation_failures(&booted).is_empty());

    booted.stop_mqtt();
}

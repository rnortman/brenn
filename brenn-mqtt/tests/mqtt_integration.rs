// MQTT integration test suite.
//
// Gate: `brenn_mqtt::broker_gate!`, which lives beside the broker harness so
// every suite that needs a broker makes the run-or-skip decision one way.
//
// Run: bazel test //brenn-mqtt:mqtt_integration

mod common;

use std::sync::Arc;
use std::time::Duration;

use brenn_envelope::{MqttExpiry, MqttPublishTransport};
use brenn_lib::mqtt::config::TlsVersionMin;
use brenn_messaging_store::store::MessageSeq;
use brenn_mqtt::broker_gate;
use brenn_mqtt::payload::InboundPayload;
use brenn_mqtt::reader::{
    Abandoned, DropCause, EgressBus, EgressItem, EgressMessage, EgressMeta, EgressReport, Sent,
    UnknownCause, start_egress_reader,
};
use brenn_mqtt::service::MqttEventRouter;
use brenn_mqtt::service::{IngressSubscribeOutcome, IngressUnsubscribeOutcome};
use brenn_mqtt::state::ConnectorHealthLabel;
use brenn_mqtt::state::IngressSubscription;
use brenn_mqtt::test_support::client::wait_for_filter_acked;
use brenn_mqtt::{ArrivingFilters, MqttClientHandle, MqttService, register_and_spawn};
use common::{
    AUTH_CREDENTIALS, BrokerHarness, CapturingRouter, DEFAULT_ACL, DEFAULT_CONF_TEMPLATE,
    SpawnedClient, TcpRelay, await_puback, broker_auth, broker_tls13, certs,
    direct_publisher_acked, direct_subscriber, direct_subscriber_every, direct_subscriber_publish,
    log_records_disconnect, log_records_disconnect_after, log_records_disconnect_before_reconnect,
    log_records_publish_to_subscriber, log_records_unsubscribe, recv_delivery, session_client_id,
    spawn_client, spawn_client_tls13, spawn_client_with_config, subscribe_live_confirmed,
    test_client_config, wait_for_health,
};
use rumqttc::mqttbytes::QoS;

// ---------------------------------------------------------------------------
// QoS 1 publish reaches an external subscriber (text)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_qos1_text_delivers() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { handle, .. } = spawn_client("test2", &harness, ca.clone(), vec![]).await;

    // Witness delivery on a separate client id.
    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/test2/state").await;

    let mut egress = Egress::start(&handle);
    let mut message = egress.message("brenn/itest/test2/state", 1, b"online");
    message.publish.retain = true;
    egress.deliver(message).await;

    let payload = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no message within 2s after QoS 1 publish")
        .expect("channel closed");
    assert_eq!(payload, b"online", "unexpected payload: {payload:?}");
}

// ---------------------------------------------------------------------------
// QoS 1 publish (binary + content-type) reaches an external subscriber
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_binary_content_type_delivers() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { handle, .. } = spawn_client("test3", &harness, ca.clone(), vec![]).await;

    let topic = "brenn/itest/test3/bin";
    let mut deliver_rx = direct_subscriber_publish(harness.port, ca, topic).await;

    let payload_bytes: Vec<u8> = vec![0xFF, 0xFE, 0xFD];
    let mut egress = Egress::start(&handle);
    let mut message = egress.message(topic, 1, &payload_bytes);
    message.publish.content_type = Some("application/octet-stream".to_string());
    egress.deliver(message).await;

    let received = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no binary message within 2s")
        .expect("channel closed");
    assert_eq!(received.payload.to_vec(), payload_bytes);
    assert_eq!(
        received
            .properties
            .as_ref()
            .and_then(|p| p.content_type.as_deref()),
        Some("application/octet-stream")
    );
}

// ---------------------------------------------------------------------------
// Message expiry reaches the subscriber, with and without a content type
// ---------------------------------------------------------------------------

/// Publish on `slug` a retained message with 60 s of lifetime left and the
/// given content type, and return the properties of the PUBLISH a separate
/// subscriber receives.
async fn publish_expiring_and_receive(
    slug: &str,
    content_type: Option<String>,
) -> rumqttc::mqttbytes::v5::PublishProperties {
    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { handle, .. } = spawn_client(slug, &harness, ca.clone(), vec![]).await;
    let topic = format!("brenn/itest/{slug}/expiring");
    let mut deliver_rx = direct_subscriber_publish(harness.port, ca, &topic).await;

    let mut egress = Egress::start(&handle);
    let mut message = egress.message(&topic, 1, b"alive");
    message.publish.retain = true;
    message.publish.content_type = content_type;
    message.publish.expires = MqttExpiry::At(chrono::Utc::now() + chrono::TimeDelta::seconds(60));
    egress.deliver(message).await;

    let publish = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no message within 2s after expiring publish")
        .expect("channel closed");
    assert_eq!(&publish.payload[..], b"alive");
    publish
        .properties
        .expect("expiring publish arrived without properties")
}

/// The broker forwards the remaining lifetime, so the received interval is at
/// most the one sent and, two seconds into a 60 s lifetime, well above zero.
fn assert_expiry_near_60(props: &rumqttc::mqttbytes::v5::PublishProperties) {
    let expiry = props
        .message_expiry_interval
        .expect("message_expiry_interval missing on the received publish");
    assert!(
        (55..=60).contains(&expiry),
        "message_expiry_interval {expiry} not in 55..=60"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_message_expiry_without_content_type() {
    broker_gate!();

    let props = publish_expiring_and_receive("exp1", None).await;
    assert_expiry_near_60(&props);
    assert_eq!(props.content_type, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_message_expiry_with_content_type() {
    broker_gate!();

    let props = publish_expiring_and_receive("exp2", Some("application/json".to_string())).await;
    assert_expiry_near_60(&props);
    assert_eq!(props.content_type.as_deref(), Some("application/json"));
}

// ---------------------------------------------------------------------------
// QoS 0 publish: reported delivered + external delivery confirmation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos0_publish_is_delivered() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { handle, .. } = spawn_client("test8", &harness, ca.clone(), vec![]).await;

    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/test8/topic").await;

    let mut egress = Egress::start(&handle);
    let message = egress.message("brenn/itest/test8/topic", 0, b"qos0-payload");
    egress.deliver(message).await;

    let payload = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no message within 2s after QoS 0 publish")
        .expect("channel closed");
    assert_eq!(payload, b"qos0-payload", "unexpected payload: {payload:?}");
}

// ---------------------------------------------------------------------------
// The broker's Maximum Packet Size: refused before submit, session kept
// ---------------------------------------------------------------------------

/// The maximum the broker of `broker_max_packet_*` advertises in its CONNACK.
const BROKER_MAX_PACKET: u32 = 1024;

/// The payload length that makes a property-less PUBLISH on `topic` exactly
/// [`BROKER_MAX_PACKET`] bytes: fixed header 1, remaining length 2 (it is over
/// 127), topic 2 + its length, packet identifier 2 at QoS 1 and 2, empty
/// properties 1.
fn payload_filling_the_maximum(topic: &str, qos: u8) -> usize {
    let pkid = if qos == 0 { 0 } else { 2 };
    BROKER_MAX_PACKET as usize - (1 + 2 + 2 + topic.len() + pkid + 1)
}

/// Against a broker that advertises a Maximum Packet Size, at QoS 0 and at
/// QoS 1: a message whose packet is exactly the maximum is delivered — the
/// size checked is the size encoded — and one a byte over is reported
/// rejected without touching the session, which stays connected, is not
/// dropped at the broker, and delivers the next in-limit message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_max_packet_size_refuses_over_and_delivers_exact() {
    broker_gate!();

    let template = format!("{DEFAULT_CONF_TEMPLATE}max_packet_size {BROKER_MAX_PACKET}\n");
    let harness = BrokerHarness::start_with(&template, &[("acl", DEFAULT_ACL.as_bytes())]);
    let ca = certs::ca_pem_bytes();
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client("maxpkt", &harness, ca.clone(), vec![]).await;
    let since = harness.log_len();
    let mut egress = Egress::start(&handle);

    for qos in [0u8, 1] {
        let exact_topic = format!("brenn/itest/maxpkt/exact{qos}");
        let exact_len = payload_filling_the_maximum(&exact_topic, qos);
        let mut deliver_rx = direct_subscriber(harness.port, ca.clone(), &exact_topic).await;
        let message = egress.message(&exact_topic, qos, &vec![0u8; exact_len]);
        egress.deliver(message).await;
        let payload = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("QoS {qos}: the exact-maximum publish was not delivered"))
            .expect("channel closed");
        assert_eq!(payload.len(), exact_len);

        let over_topic = format!("brenn/itest/maxpkt/over{qos}");
        let over_len = payload_filling_the_maximum(&over_topic, qos) + 1;
        let message = egress.message(&over_topic, qos, &vec![0u8; over_len]);
        let id = message.id;
        match egress.send(message).await {
            EgressReport::Rejected {
                message,
                reason,
                code,
                earlier,
            } => {
                assert_eq!(earlier, Sent::Never);
                assert_eq!(message.id, id);
                assert_eq!(
                    code, None,
                    "the host's own rejection carries no reason code"
                );
                assert_eq!(
                    reason,
                    format!(
                        "the PUBLISH packet is {} bytes and the broker's Maximum Packet Size \
                         is {BROKER_MAX_PACKET} bytes",
                        BROKER_MAX_PACKET + 1
                    ),
                    "QoS {qos}: a packet one byte over the maximum",
                );
            }
            other => panic!("QoS {qos}: expected the oversize message rejected, got {other:?}"),
        }
    }

    let after_topic = "brenn/itest/maxpkt/after";
    let mut deliver_rx = direct_subscriber(harness.port, ca, after_topic).await;
    let message = egress.message(after_topic, 1, &[0u8; 16]);
    egress.deliver(message).await;
    tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("the in-limit publish after the refusals was not delivered")
        .expect("channel closed");

    let (label, error) = svc.ingress_health(&client_slug).await;
    assert_eq!(
        label,
        ConnectorHealthLabel::Connected,
        "the session survived the refusals: {error:?}"
    );
    let client_id = session_client_id(&client_slug);
    let received = format!("Received PUBLISH from {client_id} (");
    let quoted = format!("'{after_topic}'");
    let window = harness
        .wait_for_log(since, 2, "the broker logs the in-limit publish", |log| {
            log.lines()
                .any(|line| line.contains(&received) && line.contains(&quoted))
        })
        .await;
    assert!(
        !window.lines().any(|line| line.contains(&client_id)
            && (line.to_lowercase().contains("disconnect")
                || line.contains("closed its connection"))),
        "the broker recorded a disconnect of {client_id}:\n{window}"
    );
}

// ---------------------------------------------------------------------------
// TLS 1.3 connect + publish/delivery roundtrip
//
// A session that incorrectly used TLS 1.2 would fail to connect to the TLS-1.3-
// only broker, so a passing publish + external delivery confirms the
// `build_tls_transport` TLS-1.3 branch is exercised.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls13_connect_publish_delivers() {
    broker_gate!();

    let harness = broker_tls13();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { handle, .. } =
        spawn_client_tls13("tls13", &harness, ca.clone(), vec![]).await;

    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/tls13/state").await;

    let mut egress = Egress::start(&handle);
    let message = egress.message("brenn/itest/tls13/state", 1, b"tls13-hello");
    egress.deliver(message).await;

    let payload = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no message within 2s over TLS 1.3")
        .expect("channel closed");
    assert_eq!(payload, b"tls13-hello", "unexpected payload: {payload:?}");
}

// ---------------------------------------------------------------------------
// AC-stop-state: stopped session reports Disconnected (broker alive)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_reports_disconnected_broker_alive() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();

    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client("test11", &harness, ca, vec![]).await;

    // Stop the session while the broker is still alive — exercises the event-loop
    // stop arm in supervisor_body, which must write terminal state.
    handle.stop();

    let final_label = wait_for_health(
        &svc,
        &client_slug,
        &[
            ConnectorHealthLabel::Disconnected,
            ConnectorHealthLabel::Failed,
        ],
        3,
        "session did not become Disconnected within 3s after stop (broker alive)",
    )
    .await;

    assert_eq!(
        final_label,
        ConnectorHealthLabel::Disconnected,
        "stopped session (broker alive) should report Disconnected, got {final_label:?}"
    );
}

// ---------------------------------------------------------------------------
// Echo pin (the load-bearing one): a publish to a self-subscribed filter on the
// same session is delivered back as an ingress message — nolocal is not set.
// Exactly one delivery with the published payload.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_publish_echoes_back_on_same_session() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        mut rx,
    } = spawn_client("echo", &harness, ca.clone(), vec![]).await;

    // A direct publisher drives the retained-barrier handshake that confirms the
    // subscription is live before we publish through the brenn session.
    let (pubc, mut ack_rx) = direct_publisher_acked(harness.port, ca).await;
    let echo_topic = "brenn/itest/echo/t";
    subscribe_live_confirmed(&svc, &client_slug, echo_topic, &pubc, &mut ack_rx, &mut rx).await;

    // Publish via the SAME session. With nolocal removed the broker delivers it
    // back to this session, and the router surfaces it as an ingress message.
    let mut egress = Egress::start(&handle);
    let message = egress.message(echo_topic, 1, b"echoed");
    egress.deliver(message).await;

    let msg = recv_delivery(&mut rx, "echo").await;
    assert_eq!(msg.topic, echo_topic);
    assert_eq!(msg.client, client_slug);
    assert!(
        msg.payload == InboundPayload::new("echoed", None),
        "echo payload mismatch: {:?}",
        msg.payload
    );

    // Exactly one delivery — no duplicate echo.
    match tokio::time::timeout(Duration::from_millis(300), rx.recv()).await {
        Err(_) => {}
        Ok(Some(m)) => panic!("unexpected second delivery on {:?}", m.topic),
        Ok(None) => panic!("router receiver closed"),
    }
}

// ---------------------------------------------------------------------------
// Ingress live subscribe / unsubscribe (dynamic mqtt: subscribe path)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingress_subscribe_filter_live_delivers() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();

    // 1. Store a retained message on the static topic BEFORE the session connects,
    //    so the supervisor's initial re-assert loop delivers it via OnEverySubscribe.
    let (pubc, mut ack_rx) = direct_publisher_acked(harness.port, ca.clone()).await;
    let static_topic = "brenn/itest/ingsub/static";
    pubc.publish(
        static_topic.to_string(),
        QoS::AtLeastOnce,
        true,
        b"static-retained".to_vec(),
    )
    .await
    .expect("static retained publish failed");
    await_puback(&mut ack_rx, "static retained").await;

    // 2. Spawn the session with the static subscription.
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        mut rx,
    } = spawn_client(
        "ingsub",
        &harness,
        ca.clone(),
        vec![(static_topic.to_string(), 1)],
    )
    .await;

    // 3. The static topic's retained payload arrives — pins the initial re-assert
    //    loop + OnEverySubscribe for static subscriptions.
    let msg = recv_delivery(&mut rx, "static retained").await;
    assert_eq!(msg.topic, static_topic);
    assert!(
        msg.payload == InboundPayload::new("static-retained", None),
        "static retained payload mismatch: {:?}",
        msg.payload
    );

    // 4. Dynamic live subscribe, confirmed via the retained barrier.
    let dyn_topic = "brenn/itest/ingsub/dyn";
    let outcome =
        subscribe_live_confirmed(&svc, &client_slug, dyn_topic, &pubc, &mut ack_rx, &mut rx).await;
    assert_eq!(outcome, IngressSubscribeOutcome::SubscribedLive);

    // 5. A non-retained live publish is delivered at QoS 1.
    pubc.publish(
        dyn_topic.to_string(),
        QoS::AtLeastOnce,
        false,
        b"live-1".to_vec(),
    )
    .await
    .expect("live-1 publish failed");
    await_puback(&mut ack_rx, "live-1").await;
    let msg = recv_delivery(&mut rx, "live-1").await;
    assert_eq!(msg.topic, dyn_topic);
    assert_eq!(msg.qos, 1, "granted QoS must be >= 1 (behavioral)");
    assert!(
        msg.payload == InboundPayload::new("live-1", None),
        "live payload mismatch: {:?}",
        msg.payload
    );

    // 6. Idempotent live re-subscribe: still SubscribedLive, and OnEverySubscribe
    //    redelivers the retained barrier (live-1 was not retained).
    let outcome2 = svc
        .subscribe_filter(&client_slug, dyn_topic.to_string(), 1)
        .await;
    assert_eq!(outcome2, Some(IngressSubscribeOutcome::SubscribedLive));
    let msg = recv_delivery(&mut rx, "re-subscribe retained barrier").await;
    assert_eq!(msg.topic, dyn_topic);
    assert!(
        msg.payload.bytes.starts_with(b"__barrier__"),
        "re-subscribe should redeliver the retained barrier, got {:?}",
        msg.payload
    );

    // 7. Exactly two subscriptions (static + dyn), and the dyn filter's QoS is 1.
    assert_eq!(handle.subscriptions.read().await.len(), 2);
    assert_eq!(
        svc.ingress_filter_qos(&client_slug, dyn_topic).await,
        Some(1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingress_unsubscribe_filter_live_stops_delivery() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();

    let SpawnedClient {
        svc,
        client_slug,
        handle,
        mut rx,
    } = spawn_client("ingunsub", &harness, ca.clone(), vec![]).await;
    let (pubc, mut ack_rx) = direct_publisher_acked(harness.port, ca.clone()).await;

    let topic_a = "brenn/itest/ingunsub/a"; // target
    let topic_b = "brenn/itest/ingunsub/b"; // control

    // 2. Subscribe both, confirmed live.
    assert_eq!(
        subscribe_live_confirmed(&svc, &client_slug, topic_a, &pubc, &mut ack_rx, &mut rx).await,
        IngressSubscribeOutcome::SubscribedLive
    );
    assert_eq!(
        subscribe_live_confirmed(&svc, &client_slug, topic_b, &pubc, &mut ack_rx, &mut rx).await,
        IngressSubscribeOutcome::SubscribedLive
    );

    // 3. Unsubscribe the target.
    let outcome = svc.unsubscribe_filter(&client_slug, topic_a).await;
    assert_eq!(outcome, Some(IngressUnsubscribeOutcome::UnsubscribedLive));
    assert!(
        handle
            .subscriptions
            .read()
            .await
            .iter()
            .all(|s| s.topic_filter != topic_a),
        "unsubscribed filter must be gone from the reconnect set"
    );

    // 4. Unsubscribe barrier: UNSUBSCRIBE(a) and this SUBSCRIBE(c) travel the same
    //    connection in order, so c's barrier delivery proves the broker fully
    //    processed the unsubscribe.
    let topic_c = "brenn/itest/ingunsub/c";
    assert_eq!(
        subscribe_live_confirmed(&svc, &client_slug, topic_c, &pubc, &mut ack_rx, &mut rx).await,
        IngressSubscribeOutcome::SubscribedLive
    );

    // 5. Publish to a (unsubscribed) then b (subscribed), same connection, QoS 1 →
    //    the broker preserves order. The next delivery must be b/control.
    pubc.publish(
        topic_a.to_string(),
        QoS::AtLeastOnce,
        false,
        b"after-unsub".to_vec(),
    )
    .await
    .expect("publish to a failed");
    pubc.publish(
        topic_b.to_string(),
        QoS::AtLeastOnce,
        false,
        b"control".to_vec(),
    )
    .await
    .expect("publish to b failed");
    await_puback(&mut ack_rx, "after-unsub").await;
    await_puback(&mut ack_rx, "control").await;

    let msg = recv_delivery(&mut rx, "control delivery").await;
    assert_eq!(
        msg.topic, topic_b,
        "a was unsubscribed; the first delivery must be the control on b"
    );
    assert!(
        msg.payload == InboundPayload::new("control", None),
        "control payload mismatch: {:?}",
        msg.payload
    );

    // A short follow-up window yields nothing — a's message never arrives.
    match tokio::time::timeout(Duration::from_millis(300), rx.recv()).await {
        Err(_) => {}
        Ok(Some(msg)) => panic!(
            "unexpected extra delivery on {:?} after unsubscribe",
            msg.topic
        ),
        Ok(None) => panic!("router receiver closed"),
    }
}

/// **The broker-log matchers, read against a real broker's wording.**
///
/// The unit cases beside `log_records_unsubscribe` and
/// `log_records_publish_to_subscriber` parse a literal transcript, so both sides
/// of that pairing are written in this repository: a `mosquitto` release that
/// renames a line, drops the tab before a listed filter or re-spells
/// `Sending PUBLISH to` leaves them green and turns every broker-side wait in
/// the tree into a ten-second timeout attributed to whatever the wait was
/// about. This case puts the same two matchers on a log an actual broker wrote,
/// so that rename fails here, where the diagnostic is the wording.
///
/// Both discriminations the matchers make are asserted against the real thing,
/// not only the recognition: a filter the broker has merely granted must not
/// read as withdrawn — its SUBSCRIBE lists it in the same tab-indented shape —
/// and a send to this session must not read as a send to a client id that is a
/// strict prefix of it, which is the one form of that discrimination whose
/// wrong answer is reachable, because the prefix is in the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_matchers_read_a_live_brokers_wording() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();

    let SpawnedClient {
        svc,
        client_slug,
        mut rx,
        ..
    } = spawn_client("logfmt", &harness, ca.clone(), vec![]).await;
    let (pubc, mut ack_rx) = direct_publisher_acked(harness.port, ca).await;

    let withdrawn = "brenn/itest/logfmt/withdrawn";
    let kept = "brenn/itest/logfmt/kept";

    // Both filters granted, and their retained barriers delivered — so the log
    // holds this session's SUBSCRIBEs and a `Sending PUBLISH` to it on each
    // topic before anything is withdrawn.
    assert_eq!(
        subscribe_live_confirmed(&svc, &client_slug, withdrawn, &pubc, &mut ack_rx, &mut rx).await,
        IngressSubscribeOutcome::SubscribedLive
    );
    assert_eq!(
        subscribe_live_confirmed(&svc, &client_slug, kept, &pubc, &mut ack_rx, &mut rx).await,
        IngressSubscribeOutcome::SubscribedLive
    );

    let subscriber = session_client_id(&client_slug);
    let granted = harness
        .wait_for_log(
            0,
            5,
            &format!("the broker never recorded sending the barrier to {subscriber} on {kept}"),
            |log| log_records_publish_to_subscriber(log, &subscriber, kept),
        )
        .await;
    for filter in [withdrawn, kept] {
        assert!(
            !log_records_unsubscribe(&granted, &subscriber, filter),
            "{filter} was only ever subscribed, and its SUBSCRIBE listed it in the shape an \
             UNSUBSCRIBE uses: {granted}",
        );
    }
    // A strict prefix of the subscriber's id, so the substring is in the log
    // and only the bracket that opens the packet's fields keeps it out. An id
    // absent from the log would pass against any matcher that read the
    // subscriber at all, including one that ignored it.
    let prefix_of_subscriber = &subscriber[..subscriber.len() - 1];
    assert!(
        !log_records_publish_to_subscriber(&granted, prefix_of_subscriber, kept),
        "a send to {subscriber} read as a send to {prefix_of_subscriber}: {granted}",
    );

    // What the broker appends from here is the withdrawal's own record.
    let before_withdrawal = harness.log_len();
    assert_eq!(
        svc.unsubscribe_filter(&client_slug, withdrawn).await,
        Some(IngressUnsubscribeOutcome::UnsubscribedLive)
    );

    let withdrawn_log = harness
        .wait_for_log(
            before_withdrawal,
            5,
            &format!("the broker never recorded the UNSUBSCRIBE for {withdrawn}"),
            |log| log_records_unsubscribe(log, &subscriber, withdrawn),
        )
        .await;
    assert!(
        !log_records_unsubscribe(&withdrawn_log, &subscriber, kept),
        "the filter still subscribed read as withdrawn: {withdrawn_log}",
    );
}

/// The wording pin for `log_records_disconnect`, the other broker-log matcher
/// a reload case reads its claim off.
///
/// Held the same way as the two above: against a broker that is really running,
/// so a `mosquitto` release that re-spells the line fails here rather than
/// turning a reload case's ten-second wait into a report of a regression that
/// did not happen.
///
/// The negative half is what makes it a matcher and not a substring: a live
/// session's id must not read as disconnected while it is still connected, and
/// a strict prefix of the stopped session's id must not read as it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_disconnect_matcher_reads_a_live_brokers_wording() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();

    // Opened before anything connects, for the ordering matcher's negative
    // side below.
    let start = harness.log_len();
    let stopped = spawn_client("logfmt-stop", &harness, ca.clone(), vec![]).await;
    let staying = spawn_client("logfmt-stay", &harness, ca, vec![]).await;
    let stopped_id = session_client_id(&stopped.client_slug);
    let staying_id = session_client_id(&staying.client_slug);

    let before = harness.log_len();
    stopped.handle.stop_and_join().await;

    let log = harness
        .wait_for_log(
            before,
            5,
            &format!("the broker never recorded a DISCONNECT from {stopped_id}"),
            |log| log_records_disconnect(log, &stopped_id),
        )
        .await;
    assert!(
        !log_records_disconnect(&log, &staying_id),
        "the session still connected read as disconnected: {log}",
    );
    let prefix = &stopped_id[..stopped_id.len() - 1];
    assert!(
        !log_records_disconnect(&log, prefix),
        "a DISCONNECT from {stopped_id} read as one from {prefix}: {log}",
    );

    // The ordering matcher a restart case reads its claim off carries a second
    // wording — the line naming the id a session connected under — so it is
    // pinned here too, on both sides. Over a window opened after both sessions
    // connected, the stopped session's DISCONNECT is the first thing in it
    // under that id.
    assert!(
        log_records_disconnect_before_reconnect(&log, &stopped_id),
        "the DISCONNECT is in the window and no connect under {stopped_id} follows it: {log}",
    );
    // Over a window opened before it connected, it is not: the connect line
    // comes first, which is the shape a client-id takeover would leave in a
    // restart case's window and the reason that window is opened where it is.
    let wide = harness
        .wait_for_log(
            start,
            5,
            &format!("the broker never recorded a DISCONNECT from {stopped_id}"),
            |log| log_records_disconnect(log, &stopped_id),
        )
        .await;
    assert!(
        !log_records_disconnect_before_reconnect(&wide, &stopped_id),
        "a session's own connect line read as a successor's: {wide}",
    );

    staying.handle.stop();
}

// ===========================================================================
// Reconnect / auth, through a real broker: (1) a mid-session drop re-asserts
// subscription filters; (2) bad credentials drive the session to Failed with
// the reason surfaced.
// ===========================================================================

// ---------------------------------------------------------------------------
// Mid-session drop (broker stays up): reconnect re-asserts the filter.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_reasserts_filters() {
    broker_gate!();

    let broker = BrokerHarness::start();
    let relay = TcpRelay::start(broker.port).await;
    let ca = certs::ca_pem_bytes();

    // Retained sentinel stored at the broker (via a DIRECT connection, not the
    // relay) before the session ever subscribes. The subscribed filter is a
    // wildcard covering it.
    let sub_filter = "brenn/itest/reconn/sub/#";
    let sentinel_topic = "brenn/itest/reconn/sub/probe";
    let (pubc, mut ack_rx) = direct_publisher_acked(broker.port, ca.clone()).await;
    pubc.publish(
        sentinel_topic.to_string(),
        QoS::AtLeastOnce,
        true,
        b"sentinel".to_vec(),
    )
    .await
    .expect("sentinel publish failed");
    await_puback(&mut ack_rx, "sentinel").await;

    // Session dials the RELAY, not the broker directly.
    let config = Arc::new(test_client_config(
        "testbroker-reconn",
        relay.port,
        ca,
        TlsVersionMin::Tls12,
    ));
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        mut rx,
    } = spawn_client_with_config(config, vec![(sub_filter.to_string(), 1)]).await;

    wait_for_health(
        &svc,
        &client_slug,
        &[ConnectorHealthLabel::Connected],
        5,
        "session never reached Connected through the relay",
    )
    .await;

    // The initial re-assert's OnEverySubscribe retained delivery proves the
    // pre-drop subscription is live.
    let msg = recv_delivery(&mut rx, "pre-drop sentinel").await;
    assert_eq!(msg.topic, sentinel_topic);
    assert!(
        msg.payload == InboundPayload::new("sentinel", None),
        "pre-drop sentinel payload mismatch: {:?}",
        msg.payload
    );

    // Sever: the event loop errors and the supervisor reconnects.
    relay.sever().await;

    // Reconnect through the relay (listener still up, new conns Forward).
    wait_for_health(
        &svc,
        &client_slug,
        &[ConnectorHealthLabel::Connected],
        10,
        "session never reconnected through the relay after sever",
    )
    .await;

    // The sentinel is redelivered ONLY because the supervisor re-asserted the
    // filter with a new SUBSCRIBE (OnEverySubscribe) — a direct witness of the
    // reconnect re-assert, end-to-end through broker → relay → session → router.
    let msg = recv_delivery(&mut rx, "post-reconnect sentinel").await;
    assert_eq!(msg.topic, sentinel_topic);
    assert!(
        msg.payload == InboundPayload::new("sentinel", None),
        "post-reconnect sentinel payload mismatch: {:?}",
        msg.payload
    );

    handle.stop();
}

// ---------------------------------------------------------------------------
// A session behind a relay the test can stall and sever.
// ---------------------------------------------------------------------------

/// A session for `slug` dialling a relay in front of a fresh broker, Connected,
/// with a direct subscriber on `topic` that bypasses the relay.
struct RelayedSession {
    _broker: BrokerHarness,
    relay: TcpRelay,
    spawned: SpawnedClient,
    deliver_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
}

async fn relayed_session(slug: &str, topic: &str) -> RelayedSession {
    let broker = BrokerHarness::start();
    let relay = TcpRelay::start(broker.port).await;
    let ca = certs::ca_pem_bytes();
    let deliver_rx = direct_subscriber_every(broker.port, ca.clone(), topic).await;
    let config = Arc::new(test_client_config(
        slug,
        relay.port,
        ca,
        TlsVersionMin::Tls12,
    ));
    let spawned = spawn_client_with_config(config, vec![]).await;
    wait_for_health(
        &spawned.svc,
        &spawned.client_slug,
        &[ConnectorHealthLabel::Connected],
        5,
        "session never reached Connected through the relay",
    )
    .await;
    RelayedSession {
        _broker: broker,
        relay,
        spawned,
        deliver_rx,
    }
}

/// The next payload the direct subscriber receives, within 5 s.
async fn next_delivery(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    what: &str,
) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("{what}: no delivery within 5s"))
        .expect("delivery channel closed")
}

/// The direct subscriber receives nothing more within 500 ms.
async fn no_more_deliveries(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>, what: &str) {
    match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
        Err(_) => {}
        Ok(Some(p)) => panic!("{what}: unexpected delivery {p:?}"),
        Ok(None) => panic!("{what}: delivery channel closed"),
    }
}

/// Forwards each inbound payload, except that the first payload `panic`
/// panics the supervisor body delivering it.
struct PanicOnceRouter {
    tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    panicked: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl MqttEventRouter for PanicOnceRouter {
    async fn deliver_inbound(
        &self,
        _client_slug: &str,
        _topic: &str,
        payload: InboundPayload,
        _qos: u8,
    ) {
        if payload.bytes.as_ref() == b"panic"
            && !self
                .panicked
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("supervisor body panic injected by the test router");
        }
        // The receiver outlives every delivery the test awaits; a retained
        // redelivery during teardown may find it gone.
        self.tx.send(payload.bytes.to_vec()).ok();
    }
}

/// A message handed on the connection of a supervisor body that panics is
/// delivered: acknowledged before the panic, or resent on the respawned
/// body's connection. The next message is resolved by its own acknowledgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_on_a_panicked_bodys_connection_is_delivered() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let trigger_topic = "brenn/itest/respawn/trigger";
    let publish_topic = "brenn/itest/respawn/out";

    // A retained message on the trigger topic, delivered when the session's
    // subscription goes live.
    let (pubc, mut ack_rx) = direct_publisher_acked(harness.port, ca.clone()).await;
    pubc.publish(
        trigger_topic.to_string(),
        QoS::AtLeastOnce,
        true,
        b"ready".to_vec(),
    )
    .await
    .expect("retained ready publish failed");
    await_puback(&mut ack_rx, "retained ready").await;
    let mut deliver_rx = direct_subscriber_every(harness.port, ca.clone(), publish_topic).await;

    let (tx, mut inbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let router: Arc<dyn MqttEventRouter> = Arc::new(PanicOnceRouter {
        tx,
        panicked: std::sync::atomic::AtomicBool::new(false),
    });
    let svc = MqttService::new();
    svc.set_router(router.clone()).await;
    let config = Arc::new(test_client_config(
        "testbroker-respawn",
        harness.port,
        ca,
        TlsVersionMin::Tls12,
    ));
    let handle = register_and_spawn(
        &svc,
        config,
        ArrivingFilters::Declared(vec![IngressSubscription {
            topic_filter: trigger_topic.to_string(),
            qos: 1,
            sub_id: 1,
        }]),
        router,
    )
    .await;
    let ready = tokio::time::timeout(Duration::from_secs(5), inbound_rx.recv())
        .await
        .expect("retained ready not delivered within 5s")
        .expect("router channel closed");
    assert_eq!(ready, b"ready");
    let mut egress = Egress::start(&handle);

    pubc.publish(
        trigger_topic.to_string(),
        QoS::AtLeastOnce,
        false,
        b"panic".to_vec(),
    )
    .await
    .expect("panic trigger publish failed");
    await_puback(&mut ack_rx, "panic trigger").await;

    // Handed while the body delivering the trigger panics: on the dying
    // connection, or on its successor once the watchdog respawns the body.
    // The respawn is delayed, so the report wait allows for that delay.
    let racing = egress.message(publish_topic, 1, b"racing");
    egress.deliver(racing).await;
    let after = egress.message(publish_topic, 1, b"after");
    egress.deliver(after).await;

    // QoS 1 may deliver the racing message twice when its acknowledgement was
    // lost with the connection; the message after it arrives exactly once.
    let mut received = Vec::new();
    while received.last().map(Vec::as_slice) != Some(b"after".as_slice()) {
        received.push(next_delivery(&mut deliver_rx, "racing, then after").await);
    }
    let (after, racing) = received.split_last().expect("at least the message after");
    assert_eq!(after, b"after");
    assert!(
        !racing.is_empty() && racing.iter().all(|p| p == b"racing"),
        "the racing message arrives at least once before the next: {received:?}"
    );
    no_more_deliveries(&mut deliver_rx, "after the message after").await;

    handle.stop();
}

// ---------------------------------------------------------------------------
// More filters than the session's request channel holds (64): the supervisor
// sends the connect-time re-assert from inside its poll loop, so the overflow
// goes out as the event loop frees room instead of wedging the session.
// ---------------------------------------------------------------------------

/// How many static filters the large-set cases spawn with: comfortably more
/// than the 64 requests the session's request channel holds.
const MANY_FILTERS: usize = 100;

fn many_filters() -> Vec<(String, u8)> {
    (0..MANY_FILTERS)
        .map(|i| (format!("brenn/itest/many/{i}/#"), 1))
        .collect()
}

/// Every one of a 100-filter set is granted at the broker, and delivery works on
/// the last one. A supervisor that waited for room on its own request channel
/// before polling would never send the 65th SUBSCRIBE, so the last filter's
/// grant wait fails at its cap instead of the case hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_with_more_filters_than_the_request_channel_holds_asserts_all() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let filters = many_filters();

    let SpawnedClient {
        svc,
        client_slug,
        handle,
        mut rx,
    } = spawn_client("many", &harness, ca.clone(), filters.clone()).await;

    let last = &filters[MANY_FILTERS - 1].0;
    wait_for_filter_acked(
        &svc,
        &client_slug,
        last,
        10,
        "the last of the static filters was never granted",
    )
    .await;
    for (filter, _) in &filters {
        assert_eq!(
            svc.subscription_acked(&client_slug, filter).await,
            Some(true),
            "{filter} was not granted on the session",
        );
    }

    let (pubc, mut ack_rx) = direct_publisher_acked(harness.port, ca).await;
    let probe = format!("brenn/itest/many/{}/probe", MANY_FILTERS - 1);
    pubc.publish(probe.clone(), QoS::AtLeastOnce, false, b"probe".to_vec())
        .await
        .expect("probe publish failed");
    await_puback(&mut ack_rx, "probe").await;
    let msg = recv_delivery(&mut rx, "probe on the last filter").await;
    assert_eq!(msg.topic, probe);

    handle.stop();
}

/// A live subscribe issued while the connect-time re-assert is still filling
/// the request channel is asserted too. It finds either a free slot (sent
/// directly) or a full channel (handed to the supervisor); either way it must
/// reach the broker without the caller or the supervisor waiting on each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_subscribe_during_a_large_reassert_is_asserted() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let filters = many_filters();

    let SpawnedClient {
        svc,
        client_slug,
        handle,
        mut rx,
    } = spawn_client("many-live", &harness, ca.clone(), filters.clone()).await;

    let live = "brenn/itest/many/live/#";
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        svc.subscribe_filter(&client_slug, live.to_string(), 1),
    )
    .await
    .expect("a live subscribe never waits on the request channel");
    assert_eq!(outcome, Some(IngressSubscribeOutcome::SubscribedLive));

    wait_for_filter_acked(
        &svc,
        &client_slug,
        live,
        10,
        "the live filter was never granted",
    )
    .await;
    wait_for_filter_acked(
        &svc,
        &client_slug,
        &filters[MANY_FILTERS - 1].0,
        10,
        "the last of the static filters was never granted",
    )
    .await;

    let (pubc, mut ack_rx) = direct_publisher_acked(harness.port, ca).await;
    let probe = "brenn/itest/many/live/probe";
    pubc.publish(
        probe.to_string(),
        QoS::AtLeastOnce,
        false,
        b"probe".to_vec(),
    )
    .await
    .expect("probe publish failed");
    await_puback(&mut ack_rx, "probe").await;
    let msg = recv_delivery(&mut rx, "probe on the live filter").await;
    assert_eq!(msg.topic, probe);

    handle.stop();
}

/// A stop that lands while the connect-time re-assert may still have the
/// request channel full returns without waiting on that channel and leaves
/// the session terminal: the DISCONNECT goes through the library's immediate
/// channel, so a full request channel is not in its way. Whether the channel
/// is full at the instant of the stop is down to timing; the case holds on
/// correct code either way.
///
/// The broker log is not read: the stop can close the socket with SUBACKs for
/// the just-sent SUBSCRIBEs still unread, which resets the connection and
/// loses the DISCONNECT at the broker.
// TODO(mqtt-disconnect-lost-to-reset): assert `log_records_disconnect` for
// this session once the stop path no longer loses its DISCONNECT to a reset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_right_after_a_large_connect_ends_the_session() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();

    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client("many-stop", &harness, ca, many_filters()).await;

    // The session has nothing in flight, so the stop waits on no bound; this
    // limit is the case's own failure limit.
    tokio::time::timeout(Duration::from_secs(7), handle.stop_and_join())
        .await
        .expect("the session did not stop");

    wait_for_health(
        &svc,
        &client_slug,
        &[ConnectorHealthLabel::Disconnected],
        1,
        "a stopped session did not read Disconnected",
    )
    .await;
}

/// A subscribe handed to the supervisor through the deferral queue is sent by
/// the supervisor at its next poll return. The case puts the entry on the queue
/// itself, so the deferred path is taken on every run rather than when a live
/// subscribe happens to meet a full request channel, and then wakes the
/// session's poll with a delivery on its static filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deferred_subscribe_is_sent_at_the_next_poll() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let wake = "brenn/itest/deferred/wake/#";

    let SpawnedClient {
        svc,
        client_slug,
        handle,
        mut rx,
    } = spawn_client(
        "deferred-drain",
        &harness,
        ca.clone(),
        vec![(wake.to_string(), 1)],
    )
    .await;
    wait_for_filter_acked(
        &svc,
        &client_slug,
        wake,
        10,
        "the static filter was never granted",
    )
    .await;

    // Registered first, as `subscribe_filter` does before it defers: the SUBACK
    // arm records a grant only for a filter in the session's set.
    let handed = "brenn/itest/deferred/handed/#";
    let sub = handle.add_subscription(handed.to_string(), 1).await;
    handle
        .deferred_subscribes
        .lock()
        .expect("deferred_subscribes poisoned")
        .push_back(sub);

    let (pubc, mut ack_rx) = direct_publisher_acked(harness.port, ca).await;
    let probe = "brenn/itest/deferred/wake/probe";
    pubc.publish(probe.to_string(), QoS::AtLeastOnce, false, b"wake".to_vec())
        .await
        .expect("wake publish failed");
    await_puback(&mut ack_rx, "wake").await;
    let msg = recv_delivery(&mut rx, "wake on the static filter").await;
    assert_eq!(msg.topic, probe);

    // The keepalive is 30s, so a grant inside this cap came from the drain that
    // follows the delivery's poll return.
    wait_for_filter_acked(
        &svc,
        &client_slug,
        handed,
        10,
        "the deferred filter was never sent",
    )
    .await;
    assert!(
        handle
            .deferred_subscribes
            .lock()
            .expect("deferred_subscribes poisoned")
            .is_empty()
    );

    handle.stop();
}

/// A static filter the re-assert cannot send is logged and passed over: every
/// filter behind it is still asserted on the session. The bad filter sits at
/// the front, where a re-assert that stopped on it would assert nothing.
/// Config resolution never lets such a filter through; the harness does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsendable_static_filter_does_not_stall_the_reassert() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let valid: Vec<String> = (0..10)
        .map(|i| format!("brenn/itest/badfront/{i}/#"))
        .collect();
    let mut filters = vec![("brenn/itest/#/bad".to_string(), 1)];
    filters.extend(valid.iter().map(|f| (f.clone(), 1)));

    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client("bad-front", &harness, ca, filters).await;

    wait_for_filter_acked(
        &svc,
        &client_slug,
        &valid[valid.len() - 1],
        10,
        "the last valid filter was never granted",
    )
    .await;
    for filter in &valid {
        assert_eq!(
            svc.subscription_acked(&client_slug, filter).await,
            Some(true),
            "{filter} was not granted on the session",
        );
    }

    handle.stop();
}

// ---------------------------------------------------------------------------
// Auth control: correct credentials connect. Without this, the bad-credentials
// test below could pass vacuously against a broker that rejects everyone (e.g.
// an unreadable passwd hash); this isolates auth as the cause of that failure.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_good_credentials_connects() {
    broker_gate!();

    let broker = broker_auth();
    let ca = certs::ca_pem_bytes();

    let mut config =
        test_client_config("testbroker-authgood", broker.port, ca, TlsVersionMin::Tls12);
    config.identity.username = Some(AUTH_CREDENTIALS.0.to_string());
    config.password = Some(AUTH_CREDENTIALS.1.to_string());
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client_with_config(Arc::new(config), vec![]).await;

    wait_for_health(
        &svc,
        &client_slug,
        &[ConnectorHealthLabel::Connected],
        5,
        "session with valid credentials never reached Connected against the auth broker",
    )
    .await;

    handle.stop();
}

// ---------------------------------------------------------------------------
// Auth failure: bad credentials drive the session to Failed with the reason
// surfaced via ingress_filter_status — the first integration coverage of the
// authoritative-connect-error → Failed path through a real broker.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_bad_credentials_drives_failed_with_reason() {
    broker_gate!();

    let broker = broker_auth();
    let ca = certs::ca_pem_bytes();

    let auth_filter = "brenn/itest/auth/#";
    let mut config =
        test_client_config("testbroker-authbad", broker.port, ca, TlsVersionMin::Tls12);
    config.identity.username = Some(AUTH_CREDENTIALS.0.to_string());
    config.password = Some("wrong-password".to_string());
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client_with_config(Arc::new(config), vec![(auth_filter.to_string(), 1)]).await;

    // Mosquitto answers the CONNACK with a bad-user/password or not-authorized
    // reason code; both map to authoritative, driving the session to Failed.
    wait_for_health(
        &svc,
        &client_slug,
        &[ConnectorHealthLabel::Failed],
        5,
        "session with bad credentials never reached Failed against the auth broker",
    )
    .await;

    let (qos, label, reason) = svc.ingress_filter_status(&client_slug, auth_filter).await;
    assert_eq!(
        qos,
        Some(1),
        "the configured filter's QoS must still be reported"
    );
    assert_eq!(label, ConnectorHealthLabel::Failed);
    // Deliberately no substring match: the pin is "Failed with a reason surfaced".
    // The exact text is rumqttc's Display over mosquitto's chosen return code
    // (0x86 vs 0x87 varies by version); the Failed label already proves an
    // authoritative broker rejection, and the control test proves it was not TLS.
    assert!(
        reason.is_some_and(|r| !r.is_empty()),
        "Failed session must surface a non-empty reason"
    );

    handle.stop();
}

// ---------------------------------------------------------------------------
// A config rejected before the log opens is reported through stderr
// ---------------------------------------------------------------------------

/// mosquitto reads its whole config before it opens `log_dest file`, so a
/// directive it does not know makes it exit with no log at all and the reason
/// on stderr only. The harness's start-up panic must carry that stderr.
///
/// Only the directive's name is asserted — it is this test's own word, and a
/// config parser rejecting an unknown directive names it — so neither the
/// broker's wording nor its exit code is pinned here.
#[test]
fn a_config_the_broker_rejects_is_reported_with_its_stderr() {
    broker_gate!();

    const DIRECTIVE: &str = "brenn_itest_bogus_directive";
    let template = format!("{DEFAULT_CONF_TEMPLATE}{DIRECTIVE} 1\n");

    let payload = std::thread::spawn(move || {
        BrokerHarness::start_with(&template, &[("acl", DEFAULT_ACL.as_bytes())])
    })
    .join()
    .err()
    .expect("the harness started a broker on a config naming an unknown directive");
    let message = payload
        .downcast_ref::<String>()
        .expect("the harness's start-up panic carries a formatted message");

    // The broker exits before its log is open, so the directive's name can
    // reach the panic only through the broker's stderr.
    assert!(
        message.contains(DIRECTIVE),
        "the start-up panic does not carry the broker's stderr naming the rejected directive:\n{message}"
    );
}

// ---------------------------------------------------------------------------
// The egress reader against a live broker, over an in-memory bus
// ---------------------------------------------------------------------------

/// An in-memory egress channel with one position, forwarding every report but
/// `Drained` to the test.
struct MemBus {
    entries: std::sync::Mutex<(Vec<EgressItem>, u64)>,
    wake: tokio::sync::watch::Sender<u64>,
    reports: tokio::sync::mpsc::UnboundedSender<EgressReport>,
}

impl MemBus {
    fn holding(
        messages: Vec<EgressMessage>,
    ) -> (
        Arc<Self>,
        tokio::sync::mpsc::UnboundedReceiver<EgressReport>,
    ) {
        let (reports, rx) = tokio::sync::mpsc::unbounded_channel();
        let bus = Arc::new(Self {
            entries: std::sync::Mutex::new((
                messages.into_iter().map(EgressItem::Message).collect(),
                1,
            )),
            wake: tokio::sync::watch::channel(0).0,
            reports,
        });
        (bus, rx)
    }
}

impl MemBus {
    /// Append `message` to the channel and raise the wake, as a commit does.
    fn append(&self, message: EgressMessage) {
        self.entries
            .lock()
            .unwrap()
            .0
            .push(EgressItem::Message(message));
        self.wake.send_modify(|n| *n += 1);
    }
}

/// A client's egress reader over an initially empty [`MemBus`], for a test that
/// publishes through the session one message at a time. One handle takes one
/// reader, so a test that publishes more than once appends to the same bus.
struct Egress {
    client_slug: String,
    bus: Arc<MemBus>,
    reports: tokio::sync::mpsc::UnboundedReceiver<EgressReport>,
    next_seq: u64,
}

impl Egress {
    fn start(handle: &Arc<MqttClientHandle>) -> Self {
        let (bus, reports) = MemBus::holding(vec![]);
        start_egress_reader(handle, bus.clone());
        Self {
            client_slug: handle.config.identity.slug.clone(),
            bus,
            reports,
            next_seq: 1,
        }
    }

    /// The next message for `topic` at `qos`: not retained, no content type,
    /// no expiry.
    fn message(&mut self, topic: &str, qos: u8, payload: &[u8]) -> EgressMessage {
        let message = egress_message(self.next_seq, &self.client_slug, topic, qos, payload);
        self.next_seq += 1;
        message
    }

    /// Commit `message` to the channel and return the reader's next report.
    async fn send(&mut self, message: EgressMessage) -> EgressReport {
        self.bus.append(message);
        next_report(&mut self.reports, 15, "the egress report").await
    }

    /// The next message for `topic` at `qos`, naming a `status_to` so the
    /// reader reports it handed before it gives it to the library.
    fn tracked(&mut self, topic: &str, qos: u8, payload: &[u8]) -> EgressMessage {
        let mut message = self.message(topic, qos, payload);
        message.publish.status_to = Some("brenn:itest-status".to_string());
        message
    }

    /// Commit `message`, which names a `status_to`, and return its id once the
    /// library holds it: a stop requested from here finds it in flight.
    ///
    /// Its `Handed` report comes before the reader submits it, and a stop that
    /// lands between the two leaves it held, never sent. So a QoS 0 sentinel on
    /// `sentinel_topic` is committed with it: the reader submits the two in
    /// order in one pass, and the sentinel's delivery, which needs only the
    /// write, says the message before it was accepted. The broker is expected
    /// to be paused, so nothing answers the message itself.
    async fn hand(&mut self, message: EgressMessage, sentinel_topic: &str) -> uuid::Uuid {
        let id = message.id;
        let sentinel = self.message(sentinel_topic, 0, b"sentinel");
        let sentinel_id = sentinel.id;
        self.bus.append(message);
        self.bus.append(sentinel);
        match next_report(&mut self.reports, 15, "the handed message").await {
            EgressReport::Handed(meta) if meta.id == id => {}
            other => panic!("expected {id} handed, got {other:?}"),
        }
        match next_report(&mut self.reports, 15, "the sentinel").await {
            EgressReport::Delivered(meta) if meta.id == sentinel_id => id,
            other => panic!("expected the sentinel {sentinel_id} delivered, got {other:?}"),
        }
    }

    /// The report the reader has already made, if any: for after the reader
    /// has exited, when every report it will make has been made.
    fn made(&mut self) -> Option<EgressReport> {
        match self.reports.try_recv() {
            Ok(report) => Some(report),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => None,
            Err(e) => panic!("report channel: {e}"),
        }
    }

    /// Commit `message` and assert the reader reports it delivered.
    async fn deliver(&mut self, message: EgressMessage) {
        let id = message.id;
        let report = self.send(message).await;
        assert!(
            matches!(&report, EgressReport::Delivered(meta) if meta.id == id),
            "expected {id} delivered, got {report:?}"
        );
    }
}

#[async_trait::async_trait]
impl EgressBus for MemBus {
    async fn unhanded(&self, limit: usize) -> Vec<EgressItem> {
        let entries = self.entries.lock().unwrap();
        let (items, next_owed) = &*entries;
        items
            .iter()
            .filter(|item| match item {
                EgressItem::Message(message) => message.seq.0 >= *next_owed,
                EgressItem::PayloadGone { seq, .. } => seq.0 >= *next_owed,
            })
            .take(limit)
            .cloned()
            .collect()
    }

    async fn advance(&self, seq: MessageSeq) {
        self.entries.lock().unwrap().1 = seq.0 + 1;
    }

    async fn abandon(&self) -> Abandoned {
        let mut entries = self.entries.lock().unwrap();
        let (items, next_owed) = &mut *entries;
        let unhanded: Vec<EgressMeta> = items
            .iter()
            .filter_map(|item| match item {
                EgressItem::Message(message) => (message.seq.0 >= *next_owed)
                    .then(|| EgressMeta::new(message.id, &message.sender, &message.publish)),
                EgressItem::PayloadGone { seq, meta } => {
                    (seq.0 >= *next_owed).then(|| meta.clone())
                }
            })
            .collect();
        if let Some(last) = items.last() {
            let last = match last {
                EgressItem::Message(message) => message.seq.0,
                EgressItem::PayloadGone { seq, .. } => seq.0,
            };
            *next_owed = (*next_owed).max(last + 1);
        }
        Abandoned {
            count: unhanded.len() as u64,
            named: unhanded
                .into_iter()
                .filter(|meta| meta.status_to.is_some())
                .collect(),
        }
    }

    fn wakes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.wake.subscribe()
    }

    fn window_depth(&self) -> Option<usize> {
        Some(256)
    }

    async fn report(&self, report: EgressReport) {
        if report != EgressReport::Drained {
            self.reports.send(report).ok();
        }
    }
}

fn egress_message(seq: u64, client: &str, topic: &str, qos: u8, payload: &[u8]) -> EgressMessage {
    EgressMessage {
        seq: MessageSeq(seq),
        id: uuid::Uuid::from_u128(u128::from(seq)),
        sender: "app:alice".to_string(),
        publish: MqttPublishTransport {
            client_slug: client.to_string(),
            topic: topic.to_string(),
            qos,
            retain: false,
            content_type: None,
            expires: MqttExpiry::Never,
            status_to: None,
        },
        payload: bytes::Bytes::copy_from_slice(payload),
    }
}

/// The next report the reader makes, within `secs`.
async fn next_report(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<EgressReport>,
    secs: u64,
    what: &str,
) -> EgressReport {
    tokio::time::timeout(Duration::from_secs(secs), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("{what}: no report within {secs}s"))
        .expect("report channel closed")
}

/// What the reader hands arrives as stated: topic, QoS, payload, content
/// type, the remaining lifetime as the Message Expiry Interval, and the
/// retain flag (read back from a subscriber that arrives afterwards).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reader_publishes_with_the_messages_options() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient {
        handle,
        client_slug,
        ..
    } = spawn_client("rdropts", &harness, ca.clone(), vec![]).await;
    let topic = "brenn/itest/rdropts/state";
    let mut live = direct_subscriber_publish(harness.port, ca.clone(), topic).await;

    let mut message = egress_message(1, &client_slug, topic, 1, b"on");
    message.publish.retain = true;
    message.publish.content_type = Some("text/plain".to_string());
    message.publish.expires = MqttExpiry::At(chrono::Utc::now() + chrono::TimeDelta::seconds(60));
    let id = message.id;
    let (bus, mut reports) = MemBus::holding(vec![message]);
    start_egress_reader(&handle, bus);

    let report = next_report(&mut reports, 5, "the delivery").await;
    assert!(
        matches!(&report, EgressReport::Delivered(meta) if meta.id == id),
        "{report:?}"
    );
    let publish = tokio::time::timeout(Duration::from_secs(2), live.recv())
        .await
        .expect("no live delivery within 2s")
        .expect("channel closed");
    assert_eq!(&publish.payload[..], b"on");
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    let props = publish
        .properties
        .expect("the publish carried no properties");
    assert_eq!(props.content_type.as_deref(), Some("text/plain"));
    assert_expiry_near_60(&props);

    let mut late = direct_subscriber_publish(harness.port, ca, topic).await;
    let retained = tokio::time::timeout(Duration::from_secs(2), late.recv())
        .await
        .expect("no retained message for a later subscriber within 2s")
        .expect("channel closed");
    assert!(retained.retain, "the broker kept the message as retained");
    assert_eq!(&retained.payload[..], b"on");

    handle.stop();
}

/// A topic the broker's ACL denies is reported rejected with the broker's
/// reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reader_reports_a_broker_denied_topic_rejected() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient {
        handle,
        client_slug,
        ..
    } = spawn_client("rdrdeny", &harness, ca, vec![]).await;
    let message = egress_message(1, &client_slug, "forbidden/rdrdeny/topic", 1, b"no");
    let id = message.id;
    let (bus, mut reports) = MemBus::holding(vec![message]);
    start_egress_reader(&handle, bus);

    match next_report(&mut reports, 5, "the rejection").await {
        EgressReport::Rejected {
            message,
            reason,
            code,
            earlier,
        } => {
            assert_eq!(earlier, Sent::Never);
            assert_eq!(message.id, id);
            assert!(!reason.is_empty(), "the rejection carries a reason");
            assert!(
                code.is_some(),
                "the broker's rejection carries its reason code"
            );
        }
        other => panic!("expected a rejection, got {other:?}"),
    }

    handle.stop();
}

/// A connection lost with a QoS 1 and a QoS 2 message unanswered: the QoS 2
/// one is reported unknown and never resent, the QoS 1 one goes out again on
/// the next connection and is delivered once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reader_resends_qos1_and_reports_qos2_unknown_across_a_loss() {
    broker_gate!();

    let topic = "brenn/itest/rdrloss/t";
    let mut session = relayed_session("testbroker-rdrloss", topic).await;
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = &session.spawned;
    let once = egress_message(1, client_slug, topic, 1, b"at-least-once");
    let twice = egress_message(2, client_slug, topic, 2, b"exactly-once");
    let (once_id, twice_id) = (once.id, twice.id);
    let (bus, mut reports) = MemBus::holding(vec![once, twice]);

    // Nothing the client writes from here reaches the broker.
    session.relay.stall().await;
    start_egress_reader(handle, bus);
    no_more_deliveries(&mut session.deliver_rx, "while the relay is stalled").await;
    session.relay.sever().await;

    match next_report(&mut reports, 10, "the lost QoS 2 message").await {
        EgressReport::Unknown { message, cause } => {
            assert_eq!(message.id, twice_id);
            assert_eq!(cause, UnknownCause::ConnectionLost);
        }
        other => panic!("expected the QoS 2 message unknown, got {other:?}"),
    }
    wait_for_health(
        svc,
        client_slug,
        &[ConnectorHealthLabel::Connected],
        10,
        "session never reconnected through the relay after the sever",
    )
    .await;
    match next_report(&mut reports, 10, "the resent QoS 1 message").await {
        EgressReport::Delivered(meta) => assert_eq!(meta.id, once_id),
        other => panic!("expected the QoS 1 message delivered, got {other:?}"),
    }
    assert_eq!(
        next_delivery(&mut session.deliver_rx, "the resent QoS 1 message").await,
        b"at-least-once"
    );
    no_more_deliveries(&mut session.deliver_rx, "after the resent message").await;

    session.spawned.handle.stop();
}

// ---------------------------------------------------------------------------
// A stop waits, up to the client's `stop_wait_secs`, for what is in flight
// ---------------------------------------------------------------------------

/// How long a case holds the broker paused after the stop before it resumes
/// it: long enough that a stop which did not wait has visibly returned.
const PAUSED_AFTER_STOP: Duration = Duration::from_millis(1500);

/// How far past its bound a stop the broker never answers may return: the
/// reports' commits, the DISCONNECT's write and the scheduler.
const STOP_SLACK: Duration = Duration::from_secs(2);

/// A `stop_and_join` of `handle` running on its own task, so the case can
/// watch whether it has returned.
fn stopping(handle: &Arc<MqttClientHandle>) -> tokio::task::JoinHandle<()> {
    let handle = handle.clone();
    tokio::spawn(async move { handle.stop_and_join().await })
}

/// Whether `handle`'s session has ended: a joined session's `stop_and_join`
/// returns without waiting on anything.
async fn session_ended(handle: &MqttClientHandle) -> bool {
    tokio::time::timeout(Duration::ZERO, handle.stop_and_join())
        .await
        .is_ok()
}

/// A message at `qos` in flight with the broker paused, a stop requested, and
/// the broker resumed inside the bound: the stop has not returned while the
/// broker was silent; the message is reported delivered, not unknown; and the
/// broker's log has the acknowledgement that closes the exchange, `ack`,
/// before the session's DISCONNECT.
///
/// The session has no subscription filters and a keep-alive far longer than
/// the pause, so at the close the broker has nothing further to send it — no
/// SUBACK, no inbound publish, no PINGRESP — and the DISCONNECT is not lost
/// to a reset.
async fn an_answer_after_the_stop_precedes_the_disconnect(slug: &str, qos: u8, ack: &str) {
    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient {
        client_slug,
        handle,
        ..
    } = spawn_client(slug, &harness, ca, vec![]).await;
    assert!(
        handle.stop_wait() > PAUSED_AFTER_STOP + STOP_SLACK,
        "the case resumes the broker inside the bound"
    );
    let client_id = session_client_id(&client_slug);
    let topic = format!("brenn/itest/{slug}/t");
    let mut egress = Egress::start(&handle);

    let since = harness.log_len();
    harness.pause();
    let message = egress.tracked(&topic, qos, b"answer-me");
    let id = egress
        .hand(message, &format!("brenn/itest/{slug}/sentinel"))
        .await;
    let stop = stopping(&handle);
    tokio::time::sleep(PAUSED_AFTER_STOP).await;
    assert!(
        !stop.is_finished(),
        "the stop returned with the broker paused and QoS {qos} message {id} unanswered"
    );
    harness.resume();
    tokio::time::timeout(STOP_SLACK * 2, stop)
        .await
        .expect("the stop did not return once the broker answered")
        .expect("the stop panicked");

    match egress.made() {
        Some(EgressReport::Delivered(meta)) if meta.id == id => {}
        other => panic!("expected QoS {qos} message {id} delivered, got {other:?}"),
    }
    assert_eq!(egress.made(), None, "a report after the delivery");

    let log = harness
        .wait_for_log(
            since,
            5,
            &format!("the broker never recorded {ack} to {client_id} before its DISCONNECT"),
            |log| log_records_disconnect_after(log, &client_id, ack),
        )
        .await;
    // The matcher's negative sides against the same live wording: a strict
    // prefix of the id is not the session, and the other QoS's closing packet
    // is not this one's.
    let prefix = &client_id[..client_id.len() - 1];
    assert!(
        !log_records_disconnect_after(&log, prefix, ack),
        "{client_id}'s exchange read as {prefix}'s: {log}"
    );
    let other = if ack == "PUBACK" { "PUBCOMP" } else { "PUBACK" };
    assert!(
        !log_records_disconnect_after(&log, &client_id, other),
        "a QoS {qos} exchange read as closed by {other}: {log}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_answer_after_the_stop_precedes_the_disconnect_qos1() {
    broker_gate!();
    an_answer_after_the_stop_precedes_the_disconnect("stopq1", 1, "PUBACK").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_answer_after_the_stop_precedes_the_disconnect_qos2() {
    broker_gate!();
    an_answer_after_the_stop_precedes_the_disconnect("stopq2", 2, "PUBCOMP").await;
}

/// The broker left paused: the stop returns at the bound — not before it, and
/// not long after — and the message is reported unknown because the session
/// stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_the_broker_never_answers_ends_at_the_bound() {
    broker_gate!();

    let bound = 2;
    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let mut config = test_client_config(
        "testbroker-stopsilent",
        harness.port,
        ca,
        TlsVersionMin::Tls12,
    );
    config.identity.stop_wait_secs = bound;
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client_with_config(Arc::new(config), vec![]).await;
    wait_for_health(
        &svc,
        &client_slug,
        &[ConnectorHealthLabel::Connected],
        5,
        "session never reached Connected",
    )
    .await;
    let mut egress = Egress::start(&handle);

    harness.pause();
    let message = egress.tracked("brenn/itest/stopsilent/t", 1, b"unanswered");
    let id = egress
        .hand(message, "brenn/itest/stopsilent/sentinel")
        .await;
    let began = tokio::time::Instant::now();
    tokio::time::timeout(handle.stop_wait() + STOP_SLACK, handle.stop_and_join())
        .await
        .expect("the stop outlasted its bound");
    let waited = began.elapsed();
    assert!(
        waited >= handle.stop_wait(),
        "the stop returned after {waited:?}, inside its {bound} s bound, with nothing answered"
    );

    match egress.made() {
        Some(EgressReport::Unknown { message, cause }) => {
            assert_eq!(message.id, id);
            assert_eq!(cause, UnknownCause::SessionStopped);
        }
        other => panic!("expected {id} unknown, got {other:?}"),
    }
    assert_eq!(egress.made(), None, "a report after the unknown");
    harness.resume();
}

/// A session with nothing in flight stops at once, far inside its bound, and
/// the broker records its DISCONNECT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_stop_does_not_wait_and_says_goodbye() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient {
        client_slug,
        handle,
        ..
    } = spawn_client("stopidle", &harness, ca, vec![]).await;
    let mut egress = Egress::start(&handle);
    let message = egress.message("brenn/itest/stopidle/t", 1, b"answered");
    egress.deliver(message).await;
    let client_id = session_client_id(&client_slug);

    let since = harness.log_len();
    let began = tokio::time::Instant::now();
    handle.stop_and_join().await;
    let waited = began.elapsed();
    assert!(
        waited < STOP_SLACK && waited * 4 < handle.stop_wait(),
        "a stop with nothing in flight took {waited:?}"
    );
    harness
        .wait_for_log(
            since,
            5,
            &format!("the broker never recorded a DISCONNECT from {client_id}"),
            |log| log_records_disconnect(log, &client_id),
        )
        .await;
}

/// Process shutdown's two calls over one service with two sessions, each with
/// a message in flight on the paused broker: `join_all` does not return while
/// the broker is silent; once it answers, `join_all` returns in about one
/// resume, with both readers' deliveries already reported and both
/// supervisors ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_all_waits_for_every_sessions_answers() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let svc = MqttService::new();
    let (router, _deliveries) = CapturingRouter::new();
    let router: Arc<dyn MqttEventRouter> = Arc::new(router);
    svc.set_router(router.clone()).await;
    let mut sessions = Vec::new();
    for slug in ["testbroker-joinone", "testbroker-jointwo"] {
        let config = Arc::new(test_client_config(
            slug,
            harness.port,
            ca.clone(),
            TlsVersionMin::Tls12,
        ));
        let handle = register_and_spawn(
            &svc,
            config,
            ArrivingFilters::Declared(vec![]),
            router.clone(),
        )
        .await;
        wait_for_health(
            &svc,
            slug,
            &[ConnectorHealthLabel::Connected],
            5,
            "session never reached Connected",
        )
        .await;
        sessions.push((handle.clone(), Egress::start(&handle)));
    }

    harness.pause();
    let mut ids = Vec::new();
    for (_, egress) in &mut sessions {
        let message = egress.tracked("brenn/itest/joinall/t", 1, b"answer-me");
        ids.push(egress.hand(message, "brenn/itest/joinall/sentinel").await);
    }
    assert_eq!(svc.stop_all(), 2);
    let joining = {
        let svc = svc.clone();
        tokio::spawn(async move { svc.join_all().await })
    };
    tokio::time::sleep(PAUSED_AFTER_STOP).await;
    assert!(
        !joining.is_finished(),
        "join_all returned with the broker paused and both messages unanswered"
    );
    harness.resume();
    let resumed = tokio::time::Instant::now();
    let joined = tokio::time::timeout(STOP_SLACK * 2, joining)
        .await
        .expect("join_all did not return once the broker answered")
        .expect("join_all panicked");
    assert_eq!(joined, 2);
    let after = resumed.elapsed();
    assert!(
        after < STOP_SLACK,
        "join_all took {after:?} after the broker resumed"
    );

    for ((handle, egress), id) in sessions.iter_mut().zip(ids) {
        match egress.made() {
            Some(EgressReport::Delivered(meta)) if meta.id == id => {}
            other => panic!("expected {id} delivered by the time join_all returned, got {other:?}"),
        }
        assert!(
            session_ended(handle).await,
            "{}'s supervisor outlived join_all",
            handle.config.identity.slug
        );
    }
}

/// A message the broker sends the session while the stop is waiting for the
/// answer to its own publish is routed, not discarded.
///
/// The session publishes on a filter it subscribes to, so the broker's
/// forward of that publish back to it is written ahead of the acknowledgement
/// on the same connection, and the reader is still waiting when it arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_inbound_publish_during_the_wait_is_routed() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let topic = "brenn/itest/stopinbound/t";
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        mut rx,
    } = spawn_client("stopinbound", &harness, ca, vec![(topic.to_string(), 1)]).await;
    wait_for_filter_acked(
        &svc,
        &client_slug,
        topic,
        5,
        "the static filter was never granted",
    )
    .await;
    let mut egress = Egress::start(&handle);

    harness.pause();
    let message = egress.tracked(topic, 1, b"echo");
    let id = egress
        .hand(message, "brenn/itest/stopinbound/sentinel")
        .await;
    let stop = stopping(&handle);
    tokio::time::sleep(PAUSED_AFTER_STOP).await;
    harness.resume();
    tokio::time::timeout(STOP_SLACK * 2, stop)
        .await
        .expect("the stop did not return once the broker answered")
        .expect("the stop panicked");

    match egress.made() {
        Some(EgressReport::Delivered(meta)) if meta.id == id => {}
        other => panic!("expected {id} delivered, got {other:?}"),
    }
    let echoed = recv_delivery(&mut rx, "the publish forwarded during the stop").await;
    assert_eq!(echoed.client, client_slug);
    assert_eq!(echoed.topic, topic);
    assert_eq!(echoed.payload, InboundPayload::new(b"echo".to_vec(), None));
}

/// A retire waits for the answer to what is in flight like a stop does, then
/// abandons what it never handed — reporting a message that names a
/// `status_to` dropped because the client was removed — and leaves the
/// session running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retire_waits_for_the_answer_then_abandons() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client("retire", &harness, ca, vec![]).await;
    let topic = "brenn/itest/retire/t";
    let mut egress = Egress::start(&handle);

    harness.pause();
    let in_flight = egress.tracked(topic, 1, b"answer-me");
    let in_flight = egress.hand(in_flight, "brenn/itest/retire/sentinel").await;
    let retiring = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.retire_egress_reader().await })
    };
    tokio::time::sleep(PAUSED_AFTER_STOP).await;
    // Committed after the retire: the reader, waiting, never hands it.
    let unhanded = egress.tracked(topic, 1, b"never-sent");
    let unhanded_id = unhanded.id;
    egress.bus.append(unhanded);
    assert!(
        !retiring.is_finished(),
        "the retire returned with the broker paused and {in_flight} unanswered"
    );
    harness.resume();
    tokio::time::timeout(STOP_SLACK * 2, retiring)
        .await
        .expect("the retire did not return once the broker answered")
        .expect("the retire panicked");

    match egress.made() {
        Some(EgressReport::Delivered(meta)) if meta.id == in_flight => {}
        other => panic!("expected {in_flight} delivered, got {other:?}"),
    }
    match egress.made() {
        Some(EgressReport::DroppedMessage {
            message,
            cause,
            sent,
        }) => {
            assert_eq!(message.id, unhanded_id);
            assert_eq!(cause, DropCause::ClientRemoved);
            assert_eq!(sent, Sent::Never);
        }
        other => panic!("expected {unhanded_id} dropped, got {other:?}"),
    }
    assert_eq!(egress.made(), None, "a report after the abandon");
    assert_eq!(
        svc.ingress_health(&client_slug).await.0,
        ConnectorHealthLabel::Connected,
        "the retire ended the session"
    );
    handle.stop_and_join().await;
}

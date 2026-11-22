// MQTT integration test suite.
//
// Gate: `brenn_mqtt::broker_gate!`, which lives beside the broker harness so
// every suite that needs a broker makes the run-or-skip decision one way.
//
// Run: bazel test //brenn-mqtt:mqtt_integration

mod common;

use std::sync::Arc;
use std::time::Duration;

use brenn_envelope::grants::AppCapability;
use brenn_lib::access::acl::{AclSet, MqttClientMatcher};
use brenn_lib::access::{AppPolicy, GrantSet};
use brenn_lib::mqtt::address::MqttAddress;
use brenn_lib::mqtt::config::TlsVersionMin;
use brenn_mqtt::broker_gate;
use brenn_mqtt::connection::SUPERSEDED_BY_NEW_CONNECTION;
use brenn_mqtt::egress::{MqttEgressError, SendBudget, enforce_and_publish};
use brenn_mqtt::payload::InboundPayload;
use brenn_mqtt::service::MqttEventRouter;
use brenn_mqtt::service::{IngressSubscribeOutcome, IngressUnsubscribeOutcome};
use brenn_mqtt::state::ConnectorHealthLabel;
use brenn_mqtt::state::{IngressSubscription, PendingPublish};
use brenn_mqtt::test_support::client::wait_for_filter_acked;
use brenn_mqtt::{ArrivingFilters, MqttService, register_and_spawn};
use brenn_mqtt::{PUBLISH_WAIT, PublishProps};
use common::{
    AUTH_CREDENTIALS, BrokerHarness, DEFAULT_ACL, DEFAULT_CONF_TEMPLATE, SpawnedClient, TcpRelay,
    await_puback, broker_auth, broker_tls13, certs, direct_publisher_acked, direct_subscriber,
    direct_subscriber_every, direct_subscriber_publish, log_records_disconnect,
    log_records_disconnect_before_reconnect, log_records_publish_to_subscriber,
    log_records_unsubscribe, recv_delivery, session_client_id, spawn_client, spawn_client_tls13,
    spawn_client_with_config, subscribe_live_confirmed, test_client_config, wait_for_health,
};
use rumqttc::mqttbytes::QoS;

/// A WASM-egress `AppPolicy`: the `MqttPublish` grant plus a `mqtt_publish`
/// matcher for `client`. Hand-rolled literal-field construction because
/// `AppPolicy::with_grants` is `#[cfg(test)]`-gated inside `brenn-lib` and so
/// invisible to integration tests in `tests/`.
fn wasm_egress_policy(client: &str) -> AppPolicy {
    let mut grants = GrantSet::default();
    grants.insert(AppCapability::MqttPublish);
    let mut acls = AclSet::default();
    acls.mqtt_publish.push(MqttClientMatcher {
        client: client.to_string(),
    });
    AppPolicy {
        grants,
        acls,
        tool_grants: Default::default(),
    }
}

// ---------------------------------------------------------------------------
// QoS 1 publish reaches an external subscriber (text)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_qos1_text_delivers() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { svc, handle, .. } =
        spawn_client("test2", &harness, ca.clone(), vec![]).await;

    // Witness delivery on a separate client id.
    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/test2/state").await;

    let outcome = svc
        .publish_on_handle(
            &handle,
            "brenn/itest/test2/state".to_string(),
            b"online".to_vec(),
            PublishProps {
                qos: 1,
                retain: true,
                ..Default::default()
            },
        )
        .await
        .expect("publish retained failed");
    assert!(
        matches!(outcome, brenn_mqtt::state::PubackOutcome::Success),
        "qos1 publish outcome: {outcome:?}"
    );

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
    let SpawnedClient { svc, handle, .. } =
        spawn_client("test3", &harness, ca.clone(), vec![]).await;

    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/test3/bin").await;

    let payload_bytes: Vec<u8> = vec![0xFF, 0xFE, 0xFD];
    svc.publish_on_handle(
        &handle,
        "brenn/itest/test3/bin".to_string(),
        payload_bytes.clone(),
        PublishProps {
            content_type: Some("application/octet-stream".to_string()),
            qos: 1,
            ..Default::default()
        },
    )
    .await
    .expect("publish binary failed");

    let payload = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no binary message within 2s")
        .expect("channel closed");
    assert_eq!(payload, payload_bytes, "unexpected payload: {payload:?}");
}

// ---------------------------------------------------------------------------
// Message expiry reaches the subscriber, with and without a content type
// ---------------------------------------------------------------------------

/// Publish on `slug` with `message_expiry_s = 60` and the given content type,
/// and return the properties of the PUBLISH a separate subscriber receives.
async fn publish_expiring_and_receive(
    slug: &str,
    content_type: Option<String>,
) -> rumqttc::mqttbytes::v5::PublishProperties {
    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { svc, handle, .. } = spawn_client(slug, &harness, ca.clone(), vec![]).await;
    let topic = format!("brenn/itest/{slug}/expiring");
    let mut deliver_rx = direct_subscriber_publish(harness.port, ca, &topic).await;

    let outcome = svc
        .publish_on_handle(
            &handle,
            topic,
            b"alive".to_vec(),
            PublishProps {
                content_type,
                qos: 1,
                retain: true,
                message_expiry_s: Some(60),
            },
        )
        .await
        .expect("expiring publish failed");
    assert!(
        matches!(outcome, brenn_mqtt::state::PubackOutcome::Success),
        "expiring publish outcome: {outcome:?}"
    );

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
// Disconnect returns NotConnected promptly
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_returns_not_connected_promptly() {
    broker_gate!();

    let mut harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();

    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client("test6", &harness, ca, vec![]).await;

    // Kill the broker, then signal the supervisor to stop so it does not
    // accumulate unnecessary reconnect attempts during teardown.
    harness.stop();
    handle.stop();

    wait_for_health(
        &svc,
        &client_slug,
        &[
            ConnectorHealthLabel::Disconnected,
            ConnectorHealthLabel::Failed,
        ],
        3,
        "session did not become Disconnected within 3s after broker kill",
    )
    .await;

    let start = std::time::Instant::now();
    let result = svc
        .publish_on_handle(
            &handle,
            "brenn/itest/test6/probe".to_string(),
            b"x".to_vec(),
            PublishProps {
                qos: 1,
                ..Default::default()
            },
        )
        .await;
    let elapsed = start.elapsed();

    assert!(
        matches!(result, Err(brenn_lib::mqtt::MqttError::NotConnected { .. })),
        "expected NotConnected, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "publish took too long: {elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// QoS 0 publish: PubackOutcome::Success + external delivery confirmation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos0_publish_returns_success_and_delivers() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { svc, handle, .. } =
        spawn_client("test8", &harness, ca.clone(), vec![]).await;

    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/test8/topic").await;

    let result = svc
        .publish_on_handle(
            &handle,
            "brenn/itest/test8/topic".to_string(),
            b"qos0-payload".to_vec(),
            PublishProps::default(),
        )
        .await;
    assert!(
        matches!(result, Ok(brenn_mqtt::state::PubackOutcome::Success)),
        "qos0 publish result: {result:?}"
    );

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

/// Publish `len` zero bytes on `topic` at `qos`, with no properties.
async fn publish_zeros(
    svc: &brenn_mqtt::MqttService,
    handle: &Arc<brenn_mqtt::MqttClientHandle>,
    topic: &str,
    len: usize,
    qos: u8,
) -> Result<brenn_mqtt::state::PubackOutcome, brenn_lib::mqtt::MqttError> {
    svc.publish_on_handle(
        handle,
        topic.to_string(),
        vec![0u8; len],
        PublishProps {
            qos,
            ..Default::default()
        },
    )
    .await
}

/// The payload length that makes a property-less PUBLISH on `topic` exactly
/// [`BROKER_MAX_PACKET`] bytes: fixed header 1, remaining length 2 (it is over
/// 127), topic 2 + its length, packet identifier 2 at QoS 1 and 2, empty
/// properties 1.
fn payload_filling_the_maximum(topic: &str, qos: u8) -> usize {
    let pkid = if qos == 0 { 0 } else { 2 };
    BROKER_MAX_PACKET as usize - (1 + 2 + 2 + topic.len() + pkid + 1)
}

/// Against a broker that advertises a Maximum Packet Size, at QoS 0 and at
/// QoS 1: a publish whose packet is exactly the maximum is delivered — the
/// size checked is the size encoded — and one a byte over is `PacketTooLarge`
/// without touching the session, which stays connected, is not dropped at
/// the broker, and delivers the next in-limit publish.
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

    for qos in [0u8, 1] {
        let exact_topic = format!("brenn/itest/maxpkt/exact{qos}");
        let exact_len = payload_filling_the_maximum(&exact_topic, qos);
        let mut deliver_rx = direct_subscriber(harness.port, ca.clone(), &exact_topic).await;
        let result = publish_zeros(&svc, &handle, &exact_topic, exact_len, qos).await;
        assert!(
            matches!(result, Ok(brenn_mqtt::state::PubackOutcome::Success)),
            "QoS {qos}: a packet exactly at the maximum: {result:?}"
        );
        let payload = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("QoS {qos}: the exact-maximum publish was not delivered"))
            .expect("channel closed");
        assert_eq!(payload.len(), exact_len);

        let over_topic = format!("brenn/itest/maxpkt/over{qos}");
        let over_len = payload_filling_the_maximum(&over_topic, qos) + 1;
        let result = publish_zeros(&svc, &handle, &over_topic, over_len, qos).await;
        assert_eq!(
            result.err(),
            Some(brenn_lib::mqtt::MqttError::PacketTooLarge {
                client_slug: client_slug.clone(),
                packet_bytes: BROKER_MAX_PACKET as usize + 1,
                broker_max: BROKER_MAX_PACKET,
            }),
            "QoS {qos}: a packet one byte over the maximum",
        );
    }

    let after_topic = "brenn/itest/maxpkt/after";
    let mut deliver_rx = direct_subscriber(harness.port, ca, after_topic).await;
    let result = publish_zeros(&svc, &handle, after_topic, 16, 1).await;
    assert!(
        matches!(result, Ok(brenn_mqtt::state::PubackOutcome::Success)),
        "the in-limit publish after the refusals: {result:?}"
    );
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

/// The two egress halves, as the intercept calls them, put a binary payload
/// and its Content Type on the wire unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorize_then_publish_delivers_bytes_and_content_type() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient {
        svc, client_slug, ..
    } = spawn_client("halves", &harness, ca.clone(), vec![]).await;
    let topic = "brenn/itest/halves/bin";
    let mut deliver_rx = direct_subscriber_publish(harness.port, ca, topic).await;

    let policy = wasm_egress_policy(&client_slug);
    let addr = MqttAddress {
        client: client_slug.clone(),
        topic: topic.to_string(),
    };
    let payload: Vec<u8> = vec![0x89, b'P', b'N', b'G', 0x00, 0xFF];
    let authorized = brenn_mqtt::egress::authorize(&svc, &policy, &addr)
        .unwrap_or_else(|e| panic!("authorize: {e:?}"));
    assert_eq!(authorized.client().slug, client_slug);
    authorized
        .publish(
            &svc,
            payload.clone(),
            PublishProps {
                content_type: Some("image/png".to_string()),
                qos: 1,
                ..Default::default()
            },
            SendBudget::None,
        )
        .await
        .unwrap_or_else(|e| panic!("publish: {e:?}"));

    let received = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no message within 2s")
        .expect("channel closed");
    assert_eq!(received.payload.to_vec(), payload);
    assert_eq!(
        received
            .properties
            .as_ref()
            .and_then(|p| p.content_type.as_deref()),
        Some("image/png")
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
    let SpawnedClient { svc, handle, .. } =
        spawn_client_tls13("tls13", &harness, ca.clone(), vec![]).await;

    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/tls13/state").await;

    svc.publish_on_handle(
        &handle,
        "brenn/itest/tls13/state".to_string(),
        b"tls13-hello".to_vec(),
        PublishProps {
            qos: 1,
            ..Default::default()
        },
    )
    .await
    .expect("tls13 publish failed");

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
    svc.publish_on_handle(
        &handle,
        echo_topic.to_string(),
        b"echoed".to_vec(),
        PublishProps {
            qos: 1,
            ..Default::default()
        },
    )
    .await
    .expect("self-publish failed");

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

// ===========================================================================
// WASM egress — the acceptance layer for the guest→host→broker publish path.
//
// These drive `enforce_and_publish` with `SendBudget::None` (the exact call
// shape the WASM `mqtt:publish` closure uses) against the live broker. The
// app/client slugs follow `spawn_client`'s registration convention
// (`itest-<name>` / `testbroker-<name>`).
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_egress_publish_reaches_broker() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { svc, .. } = spawn_client("wasmpub", &harness, ca.clone(), vec![]).await;

    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/wasmpub/topic").await;

    let policy = wasm_egress_policy("testbroker-wasmpub");
    let addr = MqttAddress {
        client: "testbroker-wasmpub".to_string(),
        topic: "brenn/itest/wasmpub/topic".to_string(),
    };
    let result = enforce_and_publish(
        &svc,
        &policy,
        &addr,
        b"wasm-egress".to_vec(),
        PublishProps {
            qos: 1,
            ..Default::default()
        },
        SendBudget::None,
    )
    .await;
    assert!(matches!(result, Ok(())), "expected Ok, got {result:?}");

    let payload = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no message within 2s after permitted WASM egress publish")
        .expect("delivery channel closed");
    assert_eq!(payload, b"wasm-egress", "unexpected payload: {payload:?}");
}

// `outbound_payload_cap_bytes` bounds `MqttSend` alone: the component's
// entry, `enforce_and_publish` with no budget, delivers a payload larger than it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_egress_is_not_held_to_the_outbound_bound() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let mut config = test_client_config(
        "testbroker-wasmbig",
        harness.port,
        ca.clone(),
        TlsVersionMin::Tls12,
    );
    config.identity.outbound_payload_cap_bytes = 8;
    let SpawnedClient {
        svc, client_slug, ..
    } = spawn_client_with_config(Arc::new(config), vec![]).await;
    wait_for_health(
        &svc,
        &client_slug,
        &[ConnectorHealthLabel::Connected],
        5,
        "session never reached Connected",
    )
    .await;

    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/wasmbig/topic").await;

    let payload = vec![0x5a_u8; 64];
    let result = enforce_and_publish(
        &svc,
        &wasm_egress_policy("testbroker-wasmbig"),
        &MqttAddress {
            client: "testbroker-wasmbig".to_string(),
            topic: "brenn/itest/wasmbig/topic".to_string(),
        },
        payload.clone(),
        PublishProps {
            qos: 1,
            ..Default::default()
        },
        SendBudget::None,
    )
    .await;
    assert!(matches!(result, Ok(())), "expected Ok, got {result:?}");

    let delivered = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no message within 2s after a component publish over the outbound bound")
        .expect("delivery channel closed");
    assert_eq!(delivered, payload);
}

// broker-rejected publish maps to BrokerRejected with a non-empty reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_egress_broker_rejected_maps_reason() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { svc, .. } = spawn_client("wasmrej", &harness, ca, vec![]).await;

    let policy = wasm_egress_policy("testbroker-wasmrej");
    // Topic OUTSIDE `brenn/itest/#`: brenn-side ACL permits it (client-scoped),
    // the broker rejects it.
    let addr = MqttAddress {
        client: "testbroker-wasmrej".to_string(),
        topic: "forbidden/wasmrej/topic".to_string(),
    };

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        enforce_and_publish(
            &svc,
            &policy,
            &addr,
            b"denied".to_vec(),
            PublishProps {
                qos: 1,
                ..Default::default()
            },
            SendBudget::None,
        ),
    )
    .await
    .expect(
        "publish to a broker-denied topic hung with no PUBACK within 5s — the broker may be \
         silently dropping the denied publish rather than rejecting it",
    );

    match result {
        Err(MqttEgressError::BrokerRejected { reason }) => {
            assert!(
                !reason.is_empty(),
                "BrokerRejected reason must be non-empty"
            );
        }
        Ok(()) => panic!(
            "broker ACL not enforced: a QoS-1 publish to a topic outside `brenn/itest/#` succeeded"
        ),
        other => panic!("expected BrokerRejected, got {other:?}"),
    }
}

// brenn-side ACL deny (client not in the policy's mqtt_publish matcher).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_egress_acl_denied() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { svc, .. } = spawn_client("wasmacl", &harness, ca, vec![]).await;

    // Grant MqttPublish, but only for a different client — the target client is
    // denied at layer 2 before any session lookup or broker contact.
    let policy = wasm_egress_policy("some-other-client");
    let addr = MqttAddress {
        client: "testbroker-wasmacl".to_string(),
        topic: "brenn/itest/wasmacl/topic".to_string(),
    };
    let result = enforce_and_publish(
        &svc,
        &policy,
        &addr,
        b"x".to_vec(),
        PublishProps {
            qos: 1,
            ..Default::default()
        },
        SendBudget::None,
    )
    .await;
    assert!(
        matches!(result, Err(MqttEgressError::AclDenied { ref client }) if client == "testbroker-wasmacl"),
        "expected AclDenied for testbroker-wasmacl, got {result:?}"
    );
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
// Reconnect / auth (mqtt-unify-reconnect-test) — the two design-mandated pins
// for the unify-sessions refactor's core behaviors, exercised through a real
// broker: (1) a mid-session drop re-asserts subscription filters AND fails a
// pending publish cleanly; (2) bad credentials drive the session to Failed with
// the reason surfaced.
// ===========================================================================

// ---------------------------------------------------------------------------
// Mid-session drop (broker stays up): reconnect re-asserts the filter AND a
// pre-disconnect pending publish fails NotConnected on the one shared session.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_reasserts_filters_and_fails_pending_publish() {
    broker_gate!();

    let broker = BrokerHarness::start();
    let relay = TcpRelay::start(broker.port).await;
    let ca = certs::ca_pem_bytes();

    // Retained sentinel stored at the broker (via a DIRECT connection, not the
    // relay) before the session ever subscribes. The subscribed filter is a
    // wildcard covering it; the stalled publish goes OUTSIDE that filter so no
    // self-echo can confuse the delivery assertions.
    let sub_filter = "brenn/itest/reconn/sub/#";
    let sentinel_topic = "brenn/itest/reconn/sub/probe";
    let pub_topic = "brenn/itest/reconn/pub/probe";
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

    // Stall the relay, then start a QoS-1 publish OUTSIDE the subscribed filter.
    // Its PUBLISH bytes leave the client but are never forwarded, so no PubAck can
    // arrive: the call parks awaiting its oneshot with the waiter registered.
    relay.stall().await;
    let svc_task = svc.clone();
    let handle_task = handle.clone();
    let pub_task = tokio::spawn(async move {
        svc_task
            .publish_on_handle(
                &handle_task,
                pub_topic.to_string(),
                b"stalled".to_vec(),
                PublishProps {
                    qos: 1,
                    ..Default::default()
                },
            )
            .await
    });

    // Event witness: the waiter must be registered (pending or inflight) BEFORE the
    // sever, so the disconnect provably fails a real pre-disconnect pending publish
    // rather than resolving via the client-is-None fast path.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let registered = !handle.pending_publishes.lock().unwrap().is_empty()
            || !handle.inflight_publishes.lock().await.is_empty();
        if registered {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "stalled publish never registered a pending/inflight waiter within 3s"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Sever: the eventloop errors, the supervisor's disconnect path runs
    // fail_all_publishes, resolving the parked waiter with NotConnected.
    relay.sever().await;
    let result = pub_task.await.expect("publish task panicked");
    assert!(
        matches!(result, Err(brenn_lib::mqtt::MqttError::NotConnected { .. })),
        "pre-disconnect pending publish must fail NotConnected, got {result:?}"
    );

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
// The publish wait: a broker that does not answer releases the caller after
// `PUBLISH_WAIT`. The sessions keep `keepalive_secs: Some(30)`, so keepalive
// cannot end a wait before 30 s and the bound is what is observed.
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

/// A component's publish: `enforce_and_publish` with no send budget.
async fn component_publish(
    session: &RelayedSession,
    topic: &str,
    payload: &[u8],
) -> Result<(), MqttEgressError> {
    let SpawnedClient {
        svc, client_slug, ..
    } = &session.spawned;
    enforce_and_publish(
        svc,
        &wasm_egress_policy(client_slug),
        &MqttAddress {
            client: client_slug.clone(),
            topic: topic.to_string(),
        },
        payload.to_vec(),
        PublishProps {
            qos: 1,
            ..Default::default()
        },
        SendBudget::None,
    )
    .await
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

async fn assert_no_publish_waiters(handle: &brenn_mqtt::MqttClientHandle) {
    assert!(handle.pending_publishes.lock().unwrap().is_empty());
    assert!(handle.inflight_publishes.lock().await.is_empty());
}

fn assert_timed_out(result: &Result<(), MqttEgressError>, what: &str) {
    assert!(
        matches!(
            result,
            Err(MqttEgressError::Broker(
                brenn_lib::mqtt::MqttError::TimedOut { .. }
            ))
        ),
        "{what}: expected Broker(TimedOut), got {result:?}"
    );
}

/// A silent connection: the component's call and the conversation's call both
/// return `TimedOut` after the bound, well before keepalive would notice, and
/// the session stays Connected. Once the connection forwards again both
/// messages are delivered, though their callers were told of a timeout, each
/// late acknowledgement removes its own entry, and a third publish is
/// resolved by its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_connection_times_out_both_callers_within_the_bound() {
    broker_gate!();

    let topic = "brenn/itest/silent/t";
    let mut session = relayed_session("testbroker-silent", topic).await;
    let db = brenn_messaging_store::db::init_db_memory();
    {
        let conn = db.lock().await;
        brenn_messaging_store::testutils::ensure_user_and_conv(&conn, 1);
    }

    session.relay.stall().await;
    let start = std::time::Instant::now();
    let SpawnedClient {
        svc, client_slug, ..
    } = &session.spawned;
    let policy = wasm_egress_policy(client_slug);
    let addr = MqttAddress {
        client: client_slug.clone(),
        topic: topic.to_string(),
    };
    let conversation = async {
        brenn_mqtt::egress::authorize(svc, &policy, &addr)
            .expect("authorized")
            .publish(
                svc,
                b"conversation".to_vec(),
                PublishProps {
                    qos: 1,
                    ..Default::default()
                },
                SendBudget::Conversation {
                    db: &db,
                    conversation_id: 1,
                    default_budget: 5,
                },
            )
            .await
    };
    let (component, conversation) = tokio::join!(
        component_publish(&session, topic, b"component"),
        conversation
    );
    let elapsed = start.elapsed();
    assert_timed_out(&component, "component publish");
    assert_timed_out(&conversation, "conversation publish");
    assert!(elapsed >= PUBLISH_WAIT, "returned after {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(25),
        "returned after {elapsed:?}"
    );
    assert_eq!(
        svc.ingress_health(client_slug).await.0,
        ConnectorHealthLabel::Connected
    );
    assert_eq!(
        brenn_messaging_store::db::read_send_budget(&db.lock().await as &rusqlite::Connection, 1),
        Some(4),
        "the conversation's send budget is spent and not refunded"
    );

    session.relay.resume().await;
    let mut stalled = vec![
        next_delivery(&mut session.deliver_rx, "first stalled message").await,
        next_delivery(&mut session.deliver_rx, "second stalled message").await,
    ];
    stalled.sort();
    assert_eq!(
        stalled,
        vec![b"component".to_vec(), b"conversation".to_vec()]
    );

    let third = component_publish(&session, topic, b"third").await;
    assert!(matches!(third, Ok(())), "third publish: {third:?}");
    assert_eq!(
        next_delivery(&mut session.deliver_rx, "third message").await,
        b"third"
    );
    no_more_deliveries(&mut session.deliver_rx, "after the third message").await;
    assert_no_publish_waiters(&session.spawned.handle).await;

    session.spawned.handle.stop();
}

/// An acknowledgement that arrives late but within the bound completes the
/// publish normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_acknowledgement_within_the_bound_completes_the_publish() {
    broker_gate!();

    let topic = "brenn/itest/late/t";
    let mut session = relayed_session("testbroker-late", topic).await;

    session.relay.stall().await;
    let publish = component_publish(&session, topic, b"late");
    let resume = async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        session.relay.resume().await;
    };
    let (result, ()) = tokio::join!(publish, resume);
    assert!(matches!(result, Ok(())), "late publish: {result:?}");
    assert_eq!(
        next_delivery(&mut session.deliver_rx, "late message").await,
        b"late"
    );
    no_more_deliveries(&mut session.deliver_rx, "after the late message").await;
    assert_no_publish_waiters(&session.spawned.handle).await;

    session.spawned.handle.stop();
}

/// A timed-out publish is not resent when the session reconnects: the client
/// side keeps nothing across connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timed_out_publish_is_not_resent_after_a_reconnect() {
    broker_gate!();

    let topic = "brenn/itest/noresend/t";
    let mut session = relayed_session("testbroker-noresend", topic).await;

    session.relay.stall().await;
    let first = component_publish(&session, topic, b"first").await;
    assert_timed_out(&first, "stalled publish");
    session.relay.sever().await;

    let SpawnedClient {
        svc, client_slug, ..
    } = &session.spawned;
    // The supervisor backs off at least a second before it redials, so the
    // Disconnected state is there to be seen.
    wait_for_health(
        svc,
        client_slug,
        &[ConnectorHealthLabel::Disconnected],
        10,
        "session never noticed the sever",
    )
    .await;
    wait_for_health(
        svc,
        client_slug,
        &[ConnectorHealthLabel::Connected],
        10,
        "session never reconnected through the relay after sever",
    )
    .await;

    let second = component_publish(&session, topic, b"second").await;
    assert!(matches!(second, Ok(())), "second publish: {second:?}");
    assert_eq!(
        next_delivery(&mut session.deliver_rx, "second message").await,
        b"second"
    );
    no_more_deliveries(&mut session.deliver_rx, "after the second message").await;
    assert_no_publish_waiters(&session.spawned.handle).await;

    session.spawned.handle.stop();
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

/// A supervisor body that panics while connected leaves its publish waiters
/// behind. The watchdog's respawned body fails every one of them as it
/// installs its new connection, and the first publish on that connection is
/// resolved by its own acknowledgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_respawned_supervisor_fails_the_dead_bodys_publish_waiters() {
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
    let client_slug = config.identity.slug.clone();
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

    // What a body that panics mid-session leaves: waiters in both maps whose
    // packets went on its event loop.
    let mut stale = Vec::new();
    {
        let mut pending = handle.pending_publishes.lock().unwrap();
        for _ in 0..2 {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            pending.push_back(PendingPublish { ack_tx });
            stale.push(ack_rx);
        }
    }
    let (ack_tx, inflight_rx) = tokio::sync::oneshot::channel();
    handle
        .inflight_publishes
        .lock()
        .await
        .insert(1, PendingPublish { ack_tx });
    stale.push(inflight_rx);

    pubc.publish(
        trigger_topic.to_string(),
        QoS::AtLeastOnce,
        false,
        b"panic".to_vec(),
    )
    .await
    .expect("panic trigger publish failed");
    await_puback(&mut ack_rx, "panic trigger").await;

    // The watchdog delays its respawn, so this wait must allow for that delay.
    for rx in stale {
        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(Err(brenn_lib::mqtt::MqttError::NotConnected {
                last_error: Some(e),
                ..
            }))) => assert_eq!(e, SUPERSEDED_BY_NEW_CONNECTION),
            other => panic!("stale waiter resolved to {other:?}"),
        }
    }
    wait_for_health(
        &svc,
        &client_slug,
        &[ConnectorHealthLabel::Connected],
        5,
        "the respawned body never reached Connected",
    )
    .await;

    let outcome = svc
        .publish_on_handle(
            &handle,
            publish_topic.to_string(),
            b"after".to_vec(),
            PublishProps {
                qos: 1,
                ..Default::default()
            },
        )
        .await
        .expect("publish on the new connection failed");
    assert!(
        matches!(outcome, brenn_mqtt::state::PubackOutcome::Success),
        "publish outcome: {outcome:?}"
    );
    assert_eq!(next_delivery(&mut deliver_rx, "after").await, b"after");
    assert_no_publish_waiters(&handle).await;

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
/// request channel full queues its DISCONNECT without waiting for room, so
/// `stop_and_join` returns within the drain cap and the session is left
/// terminal. Whether the channel is full at the instant of the stop is down to
/// timing; the case holds on correct code either way.
///
/// The broker log is not read: the stop can close the socket with SUBACKs for
/// the just-sent SUBSCRIBEs still unread, which resets the connection and
/// loses the DISCONNECT at the broker.
// TODO(mqtt-disconnect-lost-to-reset): assert `log_records_disconnect` for
// this session once the stop path no longer loses its DISCONNECT to a reset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_right_after_a_large_connect_returns_within_the_drain_cap() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let ca = certs::ca_pem_bytes();

    let SpawnedClient {
        svc,
        client_slug,
        handle,
        ..
    } = spawn_client("many-stop", &harness, ca, many_filters()).await;

    // The supervisor's DISCONNECT drain is capped at 5s; 2s of slack on top.
    tokio::time::timeout(Duration::from_secs(7), handle.stop_and_join())
        .await
        .expect("the supervisor did not stop within its drain cap");

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

// MQTT integration test suite.
//
// Gate: set `BRENN_MQTT_INTEGRATION=1` in the environment.
// Without it, all tests early-return (compile and link, but no broker is spawned).
//
// Run: BRENN_MQTT_INTEGRATION=1 cargo test -p brenn-lib --test mqtt_integration

mod common;

use std::sync::Arc;
use std::time::Duration;

use brenn_lib::access::acl::{AclSet, MqttClientMatcher};
use brenn_lib::access::{AppCapability, AppPolicy, GrantSet};
use brenn_lib::mqtt::address::MqttAddress;
use brenn_lib::mqtt::config::TlsVersionMin;
use brenn_lib::mqtt::egress::{MqttEgressError, SendBudget, enforce_and_publish};
use brenn_lib::mqtt::payload::InboundPayload;
use brenn_lib::mqtt::service::{IngressSubscribeOutcome, IngressUnsubscribeOutcome};
use brenn_lib::mqtt::state::ConnectorHealthLabel;
use common::{
    BrokerHarness, SpawnedClient, TcpRelay, await_puback, certs, direct_publisher_acked,
    direct_subscriber, recv_delivery, spawn_client, spawn_client_tls13, spawn_client_with_config,
    subscribe_live_confirmed, test_client_config, wait_for_health,
};
use rumqttc::mqttbytes::QoS;

macro_rules! integration_gate {
    () => {
        if std::env::var("BRENN_MQTT_INTEGRATION").is_err() {
            eprintln!("skipping: set BRENN_MQTT_INTEGRATION=1 to run");
            return;
        }
    };
}

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
    integration_gate!();

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
            None,
            1,
            true,
        )
        .await
        .expect("publish retained failed");
    assert!(
        matches!(outcome, brenn_lib::mqtt::state::PubackOutcome::Success),
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
    integration_gate!();

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
        Some("application/octet-stream".to_string()),
        1,
        false,
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
// Disconnect returns NotConnected promptly
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_returns_not_connected_promptly() {
    integration_gate!();

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
            None,
            1,
            false,
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
    integration_gate!();

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
            None,
            0,
            false,
        )
        .await;
    assert!(
        matches!(result, Ok(brenn_lib::mqtt::state::PubackOutcome::Success)),
        "qos0 publish result: {result:?}"
    );

    let payload = tokio::time::timeout(Duration::from_secs(2), deliver_rx.recv())
        .await
        .expect("no message within 2s after QoS 0 publish")
        .expect("channel closed");
    assert_eq!(payload, b"qos0-payload", "unexpected payload: {payload:?}");
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
    integration_gate!();

    let harness = BrokerHarness::start_tls13();
    let ca = certs::ca_pem_bytes();
    let SpawnedClient { svc, handle, .. } =
        spawn_client_tls13("tls13", &harness, ca.clone(), vec![]).await;

    let mut deliver_rx = direct_subscriber(harness.port, ca, "brenn/itest/tls13/state").await;

    svc.publish_on_handle(
        &handle,
        "brenn/itest/tls13/state".to_string(),
        b"tls13-hello".to_vec(),
        None,
        1,
        false,
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
    integration_gate!();

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
// same session is delivered back as an ingress message (nolocal removed, design
// §3.3). Exactly one delivery with the published payload.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_publish_echoes_back_on_same_session() {
    integration_gate!();

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
        None,
        1,
        false,
    )
    .await
    .expect("self-publish failed");

    let msg = recv_delivery(&mut rx, "echo").await;
    assert_eq!(msg.topic, echo_topic);
    assert_eq!(msg.client, client_slug);
    assert!(
        matches!(&msg.payload, InboundPayload::Text(t) if t == "echoed"),
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
    integration_gate!();

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
        None,
        1,
        false,
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

// broker-rejected publish maps to BrokerRejected with a non-empty reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_egress_broker_rejected_maps_reason() {
    integration_gate!();

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
            None,
            1,
            false,
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
    integration_gate!();

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
        None,
        1,
        false,
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
    integration_gate!();

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
        matches!(&msg.payload, InboundPayload::Text(t) if t == "static-retained"),
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
        matches!(&msg.payload, InboundPayload::Text(t) if t == "live-1"),
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
        matches!(&msg.payload, InboundPayload::Text(t) if t.starts_with("__barrier__")),
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
    integration_gate!();

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
        matches!(&msg.payload, InboundPayload::Text(t) if t == "control"),
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
    integration_gate!();

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
        matches!(&msg.payload, InboundPayload::Text(t) if t == "sentinel"),
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
                None,
                1,
                false,
            )
            .await
    });

    // Event witness: the waiter must be registered (pending or inflight) BEFORE the
    // sever, so the disconnect provably fails a real pre-disconnect pending publish
    // rather than resolving via the client-is-None fast path.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let registered = !handle.pending_publishes.lock().await.is_empty()
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
        matches!(&msg.payload, InboundPayload::Text(t) if t == "sentinel"),
        "post-reconnect sentinel payload mismatch: {:?}",
        msg.payload
    );

    handle.stop();
}

// ---------------------------------------------------------------------------
// Auth control: correct credentials connect. Without this, the bad-credentials
// test below could pass vacuously against a broker that rejects everyone (e.g.
// an unreadable passwd hash); this isolates auth as the cause of that failure.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auth_good_credentials_connects() {
    integration_gate!();

    let broker = BrokerHarness::start_auth();
    let ca = certs::ca_pem_bytes();

    let mut config =
        test_client_config("testbroker-authgood", broker.port, ca, TlsVersionMin::Tls12);
    config.username = Some("brenn-itest".to_string());
    config.password = Some("brenn-itest-password".to_string());
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
    integration_gate!();

    let broker = BrokerHarness::start_auth();
    let ca = certs::ca_pem_bytes();

    let auth_filter = "brenn/itest/auth/#";
    let mut config =
        test_client_config("testbroker-authbad", broker.port, ca, TlsVersionMin::Tls12);
    config.username = Some("brenn-itest".to_string());
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

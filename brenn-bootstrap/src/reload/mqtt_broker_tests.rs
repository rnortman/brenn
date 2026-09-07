//! Reload's MQTT convergence, against a broker that is really running.
//!
//! Every other MQTT reload case in this crate proves the *plan*: which filters
//! the walk decides to SUBSCRIBE, which routes it adds, and in what order. None
//! of them can prove the one thing an operator cares about — that a topic the
//! reload subscribed to is a topic the process then receives on. The handles
//! those fixtures register have no connection, so every SUBSCRIBE defers and no
//! packet ever moves.
//!
//! These cases boot the real subsystem against `mosquitto` on loopback, reload
//! a document that grows an ingress binding, and then publish on the new topic
//! from outside brenn.
//!
//! `mosquitto` is a system binary and the only non-hermetic dependency in the
//! build, so this module is filtered out of the hermetic test target and run by
//! one of its own. The gate that decides between running and skipping is
//! `brenn_mqtt::broker_gate!`, beside the harness it gates.

use brenn_mqtt::broker_gate;
use brenn_mqtt::state::ConnectorHealthLabel;
use brenn_mqtt::test_support::{BrokerHarness, await_puback, certs, direct_publisher_acked};
use rumqttc::mqttbytes::QoS;

use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Booted, Tree, bodies_on, boot_with, cursor_of, document, install_package,
    staged_module,
};
use brenn_messaging::config_reload::Outcome;

/// The topic prefix the broker's ACL admits.
const TOPIC_PREFIX: &str = "brenn/itest/reload";

/// A document declaring one broker at `port`, trusting `ca_file`, and one
/// consumer bound to one `mqtt:` channel per topic.
///
/// The same shape as the plan-only fixture's document, with the broker the
/// harness actually started in place of a port nothing listens on.
fn document_over_the_broker(port: u16, ca_file: &std::path::Path, topics: &[&str]) -> String {
    let ports: String = (0..topics.len())
        .map(|index| format!("    in inbound{index};\n"))
        .collect();
    let bindings: String = topics
        .iter()
        .enumerate()
        .map(|(index, topic)| {
            format!(
                "    in inbound{index} <- \"mqtt:ha:{topic}\" {{ push_depth = 4; \
                 retain_depth = 4; }}\n"
            )
        })
        .collect();
    let ca = ca_file.display();
    document(&format!(
        r#"mqtt_client ha {{
    url = "mqtts://127.0.0.1:{port}";
    ca_file = "{ca}";
    qos = 1;
}}

channel sink at "brenn:sink" {{
    push_depth = 1;
    retain_depth = 4;
    standing_retain_depth = 4;
}}
{}component Demo {{
    abi = processor;
    requires = [ports];
{ports}    out digest;
}}
{}
new sifter: Demo {{
    grants = [ports];
{bindings}    out digest -> sink;
}}
"#,
        brenn_lib::config::PACKAGED,
        brenn_lib::config::PACKAGED,
    ))
}

/// Boot against `harness` with `topics` bound, and wait for the session to
/// reach the broker.
async fn boot_connected(
    harness: &BrokerHarness,
    ca_file: &std::path::Path,
    components: &std::path::Path,
    topics: &[&str],
) -> (Tree, Booted) {
    let tree = Tree::holding(&document_over_the_broker(harness.port, ca_file, topics));
    install_package(components, &staged_module(&tree));
    let booted = boot_with(
        &tree,
        BootFixture {
            components_roots: vec![components.to_path_buf()],
            mqtt_live: true,
            dispatcher: true,
            ..BootFixture::default()
        },
    )
    .await;
    let (service, _) = booted.mqtt.clone().expect("the fixture stood one up");
    brenn_mqtt::test_support::wait_for_health(
        &service,
        "ha",
        &[ConnectorHealthLabel::Connected],
        10,
        "the booted session never reached the broker",
    )
    .await;
    (tree, booted)
}

/// The whole point of converging `mqtt:` ingress: a binding that arrives by
/// reload is a binding the process receives on, with no restart.
///
/// The assertions walk the path end to end — the SUBSCRIBE was answered live
/// rather than deferred, the broker's copy of the message reached the channel
/// the reload minted, and the consumer wired to that channel moved its cursor
/// past it. A reload that added the route but not the filter, or the filter but
/// not the route, fails at a different one of the three.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_binding_added_by_reload_receives_from_the_broker() {
    broker_gate!();

    let kept = format!("{TOPIC_PREFIX}/kept");
    let arrived = format!("{TOPIC_PREFIX}/arrived");
    let address = format!("mqtt:ha:{arrived}");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");

    let (tree, mut booted) = boot_connected(&harness, &ca_file, components.path(), &[&kept]).await;
    let (service, router) = booted.mqtt.clone().expect("the fixture stood one up");
    assert_eq!(service.ingress_filter_qos("ha", &arrived).await, None);

    // The reload: one more binding on the same client, so no session moves and
    // rule 6 has nothing to say.
    tree.write(&document_over_the_broker(
        harness.port,
        &ca_file,
        &[&kept, &arrived],
    ));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
    assert!(
        status.delta.mqtt_deferred.is_empty(),
        "a connected session answers the SUBSCRIBE live: {:?}",
        status.delta.mqtt_deferred,
    );
    let arrived_uuid = booted
        .messenger
        .directory()
        .resolve(&address)
        .expect("the reload minted the entry")
        .uuid;
    assert!(router.route_uuids().contains(&arrived_uuid));

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(arrived.clone(), QoS::AtLeastOnce, false, b"after".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the arriving topic's publish").await;

    // The body is the ingress envelope the router builds, so the assertion is
    // on its parts rather than on the payload text alone: a message that
    // reached this channel from any other topic would pass a bare
    // payload comparison.
    let bodies = booted.bodies_until(&address, 1).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    let envelope: serde_json::Value =
        serde_json::from_str(&bodies[0]).expect("the ingress envelope is JSON");
    assert_eq!(envelope["client_slug"], "ha");
    assert_eq!(envelope["topic"], arrived.as_str());
    assert_eq!(
        envelope["payload"]["text"], "after",
        "the message the broker took reached the channel the reload minted: {bodies:?}",
    );

    // And the consumer bound to it saw it: its cursor is past the message the
    // channel now holds, which only a dispatch through the delivery binding the
    // walk installed can do.
    assert!(
        poll_cursor_advanced(&booted, &address).await,
        "the consumer wired to the arriving channel never moved its cursor",
    );

    assert!(
        bodies_on(&booted.messenger, &format!("mqtt:ha:{kept}"))
            .await
            .is_empty()
    );

    booted.stop_mqtt();
}

/// The other direction, and the one no plan-level fixture can prove: a binding
/// the reload dropped is a binding the process no longer receives on.
///
/// The plan-only cases assert that the walk *decided* to UNSUBSCRIBE and to
/// remove the route. Neither can see a broker that kept sending, so this case
/// removes a live binding and then publishes on the removed topic behind a
/// barrier — a publish on a topic that stayed subscribed, over the same
/// connection, so ordering makes the barrier arriving the proof that the
/// removed topic's message would have arrived too if the subscription were
/// still there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_binding_removed_by_reload_stops_receiving_from_the_broker() {
    broker_gate!();

    let kept = format!("{TOPIC_PREFIX}/removal-kept");
    let dropped = format!("{TOPIC_PREFIX}/removal-dropped");
    let kept_address = format!("mqtt:ha:{kept}");
    let dropped_address = format!("mqtt:ha:{dropped}");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");

    let (tree, mut booted) =
        boot_connected(&harness, &ca_file, components.path(), &[&kept, &dropped]).await;
    let (service, router) = booted.mqtt.clone().expect("the fixture stood one up");
    assert!(service.ingress_filter_qos("ha", &dropped).await.is_some());
    let dropped_uuid = booted
        .messenger
        .directory()
        .resolve(&dropped_address)
        .expect("boot minted the entry")
        .uuid;
    assert!(router.route_uuids().contains(&dropped_uuid));

    // The reload: the same client, one binding fewer.
    tree.write(&document_over_the_broker(harness.port, &ca_file, &[&kept]));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(
        status.delta.mqtt_unsubscribed,
        vec![dropped_address.clone()]
    );
    assert_eq!(
        service.ingress_filter_qos("ha", &dropped).await,
        None,
        "the filter must leave the set the supervisor re-asserts on reconnect",
    );
    assert!(
        !router.route_uuids().contains(&dropped_uuid),
        "the ingress route outlived the channel it delivered to",
    );
    assert!(
        booted
            .messenger
            .directory()
            .resolve(&dropped_address)
            .is_none()
    );
    assert!(
        service.ingress_filter_qos("ha", &kept).await.is_some(),
        "the untouched binding is collateral of nothing",
    );

    // The barrier: the removed topic first, the surviving one second, on one
    // connection. Once the second has landed the first has been answered too.
    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(dropped.clone(), QoS::AtLeastOnce, false, b"gone".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the publish on the removed topic").await;
    publisher
        .publish(kept.clone(), QoS::AtLeastOnce, false, b"here".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the publish on the surviving topic").await;

    let bodies = booted.bodies_until(&kept_address, 1).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    assert!(
        booted
            .messenger
            .directory()
            .resolve(&dropped_address)
            .is_none(),
        "nothing re-minted the entry the reload removed",
    );
    assert!(
        !router.route_uuids().contains(&dropped_uuid),
        "an inbound publish on the removed topic re-created its route",
    );

    booted.stop_mqtt();
}

/// **The oracle over an ingress binding arriving, and over one leaving.**
///
/// The enumerated cases above check what somebody thought to check: the filter,
/// the route, the envelope, the cursor. This one compares the whole process
/// against a fresh boot of the document it reloaded onto, so a piece of MQTT
/// state the walk forgot to move — or moved and should not have — shows up
/// without anyone having named it in advance.
///
/// It runs here rather than beside the other transitions because the fresh-boot
/// side has to be boot's. Under the plan-only fixture the filter set and the
/// route table are transcribed from the plan, which would make the comparison a
/// comparison against the plan; only `mqtt_live` builds them the way
/// `start_mqtt` and `wire_mqtt_state` do, and that needs a broker.
///
/// The two processes never overlap on the broker: the comparison helper stops
/// the reload side's supervisors once its snapshot is taken and before the
/// fresh side boots, so neither takes the other's session over. The reload side
/// waits for connection health inside the arrival, which is strictly before the
/// fresh side exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_mqtt_binding_matches_a_fresh_boot() {
    broker_gate!();

    let kept = format!("{TOPIC_PREFIX}/oracle-kept");
    let moved = format!("{TOPIC_PREFIX}/oracle-moved");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");

    // The binding arrives.
    compare_over_the_broker(&harness, &ca_file, &[&kept], &[&kept, &moved], 2).await;
    // And the binding leaves.
    compare_over_the_broker(&harness, &ca_file, &[&kept, &moved], &[&kept], 1).await;
}

/// Boot `before` over `harness`, reload onto `after`, and compare the reloaded
/// process against a fresh boot of `after`.
///
/// `filters` is how many the reloaded session must hold, which is what keeps a
/// transition that did nothing from passing the comparison trivially.
async fn compare_over_the_broker(
    harness: &BrokerHarness,
    ca_file: &std::path::Path,
    before: &[&str],
    after: &[&str],
    filters: usize,
) {
    let components = tempfile::tempdir().expect("a components root");
    let tree = Tree::holding(&document_over_the_broker(harness.port, ca_file, before));
    install_package(components.path(), &staged_module(&tree));

    let fixture = |db| BootFixture {
        db: Some(db),
        components_roots: vec![components.path().to_path_buf()],
        mqtt_live: true,
        ..BootFixture::default()
    };

    super::oracle_tests::a_reload_matches_a_fresh_boot(
        &tree,
        fixture,
        async |booted| {
            let (service, _) = booted.mqtt.clone().expect("the fixture stood one up");
            brenn_mqtt::test_support::wait_for_health(
                &service,
                "ha",
                &[ConnectorHealthLabel::Connected],
                10,
                "the booted session never reached the broker",
            )
            .await;

            tree.write(&document_over_the_broker(harness.port, ca_file, after));
            install_package(components.path(), &staged_module(&tree));
            booted.driver.reload(TriggerSource::Signal).await;
            let status = booted.last_status().await;
            assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
        },
        |reloaded| {
            assert_eq!(
                reloaded.mqtt_filters().len(),
                filters,
                "{:?}",
                reloaded.mqtt_filters(),
            );
            // The route table beside the filter set: one ingress channel per
            // declared filter, and a field left empty on both sides compares
            // nothing.
            assert_eq!(
                reloaded.mqtt_routes().len(),
                filters,
                "{:?}",
                reloaded.mqtt_routes(),
            );
        },
    )
    .await;
}

/// Poll the consumer's cursor on `address` until it owes nothing behind the
/// first message, capped at 10s.
async fn poll_cursor_advanced(booted: &Booted, address: &str) -> bool {
    let participant = brenn_lib::messaging::ParticipantId::for_wasm("sifter");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let cursor = cursor_of(&booted.messenger, address, &participant).await;
        if cursor.is_some_and(|row| row.next_owed_seq > 1) {
            return true;
        }
        if std::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

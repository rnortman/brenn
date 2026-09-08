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
use brenn_mqtt::test_support::{
    BrokerHarness, await_puback, certs, direct_publisher_acked, log_records_publish_to_subscriber,
    log_records_unsubscribe, session_client_id,
};
use rumqttc::mqttbytes::QoS;

use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Booted, READER, Tree, bodies_on, boot_with, conversation_of, cursor_of, document,
    document_push_subscribing_acl, document_subscribing_acl, dynamic_rows, insert_dynamic_mqtt_row,
    install_package, live_entry_of_reader, seat_user, staged_module, staged_module_opt,
};
use brenn_messaging::config_reload::Outcome;

/// The topic prefix the broker's ACL admits.
const TOPIC_PREFIX: &str = "brenn/itest/reload";

/// The `mqtt_client` declaration every document in this module stands on.
///
/// One copy for every shape: the idle-broker test boots a document with no
/// binding and reloads onto a bound one, and a second copy that drifted in
/// `url` or `qos` would make it converge onto a different client declaration
/// than it booted — which the `mqtt_clients` level-1 refusal would answer, in a
/// test whose whole point is the applied path.
fn client_block(port: u16, ca_file: &std::path::Path) -> String {
    format!(
        r#"mqtt_client ha {{
    url = "mqtts://127.0.0.1:{port}";
    ca_file = "{ca}";
    qos = 1;
}}
"#,
        ca = ca_file.display(),
    )
}

/// A document declaring one broker at `port`, trusting `ca_file`, and one
/// consumer bound to one `mqtt:` channel per topic.
///
/// The same shape as the plan-only fixture's document, with the broker the
/// harness actually started in place of a port nothing listens on.
///
/// With `topics` empty it is the broker alone: no component and no consumer,
/// because a consumer with an output port and no subscriptions is a boot-time
/// assert. That is the shape a site declares before its first component ships,
/// so a reload from it adds the component, the consumer and its `mqtt:` binding
/// together.
fn document_over_the_broker(port: u16, ca_file: &std::path::Path, topics: &[&str]) -> String {
    let mut body = client_block(port, ca_file);
    if topics.is_empty() {
        return document(&body);
    }
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
    body.push_str(&format!(
        r#"
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
    ));
    document(&body)
}

/// The one broker client every document in this file declares.
const CLIENT: &str = "ha";

/// How long any wait in this file gives the broker to be observed reaching a
/// state — a health label, a granted filter, or a line in the broker's own log.
/// One number, because every wait here is the same wait against the same local
/// broker.
const BROKER_WAIT_SECS: u64 = 10;

/// Boot against `harness` with `topics` bound, and wait for the session to
/// reach the broker.
async fn boot_connected(
    harness: &BrokerHarness,
    ca_file: &std::path::Path,
    components: &std::path::Path,
    topics: &[&str],
) -> (Tree, Booted) {
    let tree = Tree::holding(&document_over_the_broker(harness.port, ca_file, topics));
    // A broker-only document declares no component and so stages no module,
    // and there is then no package to install; the reload that brings the first
    // component installs it. Read off the tree rather than off `topics`, so the
    // builder stays the only thing that knows which documents stage one.
    if let Some(module) = staged_module_opt(&tree) {
        install_package(components, &module);
    }
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
        CLIENT,
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the booted session never reached the broker",
    )
    .await;
    (tree, booted)
}

/// The half of holding the reload's ingress answer to account that both
/// direction barriers share: a filter the walk could neither assert nor
/// withdraw is a failure whichever direction it was moving.
fn assert_no_failed_filters(status: &brenn_messaging::config_reload::ReloadStatus) {
    assert!(
        status.delta.mqtt_failed.is_empty(),
        "a filter the walk could not assert is a failure in every case: {:?}",
        status.delta.mqtt_failed,
    );
}

/// Hold the reload's ingress answer to account, then wait until the broker has
/// granted the filter on the client's current session.
///
/// `subscribe_filter` answers `DeferredDisconnected` whenever the client cell is
/// empty, and the supervisor empties it on every event-loop error before it
/// reconnects. `boot_connected` waits for `Connected`, but the test then writes
/// a document, installs a package and runs a whole reload before the SUBSCRIBE
/// goes out — a drop anywhere in that span is a deferral no case here can
/// prevent. So the assertion is not on the instant: a failure is always a
/// failure, and the deferred list may name this address and nothing else.
///
/// **What the wait proves.** The grant wait runs on both paths, deferred or
/// live: a SUBSCRIBE that went out during the reload races the publisher's
/// connect exactly as a re-asserted one does, and a wait on the session's state
/// would prove neither had been answered. `wait_for_filter_acked` returning
/// means the broker granted this session's SUBSCRIBE for the filter at that
/// instant. It does not hold the session up afterwards: a drop between the
/// grant and the publish clears the grant with the client and the publish lands
/// on a broker holding no matching filter — clean session, no retain. That
/// window is the milliseconds between this call and the publish reaching the
/// broker, and it is the residual no wait on the test's side can close, because
/// the test cannot stop the broker dropping a session. [`note_if_filter_lost`]
/// is the line that says so on a run where it fires.
///
/// Because this accepts either the deferred or the live answer, the
/// *distinction* between them is pinned where it is deterministic:
/// `subscribe_filter_live_client_reports_subscribed_live` and
/// `subscribe_filter_disconnected_defers_but_registers` in `brenn-mqtt`'s
/// service tests. A regression that turned every live SUBSCRIBE into a deferral
/// fails there.
async fn await_filter_at_broker(
    service: &brenn_mqtt::MqttService,
    status: &brenn_messaging::config_reload::ReloadStatus,
    client: &str,
    address: &str,
    topic_filter: &str,
) {
    assert_no_failed_filters(status);
    if !status.delta.mqtt_deferred.is_empty() {
        assert_eq!(
            status.delta.mqtt_deferred,
            vec![address.to_string()],
            "nothing but this address may have deferred",
        );
        eprintln!(
            "mqtt_live: the SUBSCRIBE for {address} deferred on a session drop; waiting for the \
             broker to grant it after the supervisor re-asserts it",
        );
    }
    brenn_mqtt::test_support::wait_for_filter_acked(
        service,
        client,
        topic_filter,
        BROKER_WAIT_SECS,
        &format!("the broker never granted a subscription for {topic_filter}"),
    )
    .await;
}

/// Note, without asserting, that the broker no longer holds a grant for
/// `topic_filter` — the residual [`await_filter_at_broker`] cannot close.
///
/// Called after the publish and before the body wait: a session drop in that
/// span means the publish was taken by a broker holding no filter for it, so
/// the body never arrives and `poll_until`'s panic would otherwise say nothing
/// about why. It can also fire on a run that passes, when the drop follows the
/// broker routing the publish, which is why this is a note and not an
/// assertion.
///
/// # Panics
///
/// If `client` is not a registered client — the caller named a client the
/// document does not declare, as `wait_for_filter_acked` does.
async fn note_if_filter_lost(service: &brenn_mqtt::MqttService, client: &str, topic_filter: &str) {
    let acked = service
        .subscription_acked(client, topic_filter)
        .await
        .unwrap_or_else(|| panic!("note_if_filter_lost: no session registered for {client:?}"));
    if !acked {
        let (label, error) = service.ingress_health(client).await;
        eprintln!(
            "mqtt_live: the grant for {topic_filter} was lost between the wait and the publish \
             (health now: {label:?} {error:?}); the publish above was taken by a broker holding \
             no filter for it, so the body wait below will time out for that reason",
        );
    }
}

/// Hold the reload's ingress answer to account, then wait until the broker's own
/// log records it processing brenn's UNSUBSCRIBE for `topic_filter`.
///
/// The removal direction's counterpart of [`await_filter_at_broker`], and it
/// treats a deferral as a failure where the arrival side tolerates one. The
/// asymmetry is the packets': a deferred SUBSCRIBE is re-asserted by the
/// supervisor's reconnect, so the arrival claim can still be proven after a
/// wait, whereas a deferred UNSUBSCRIBE is never sent — the filter only leaves
/// the reconnect-survival set — and the reconnect re-asserts subscriptions and
/// unsubscribes nothing. The session is persistent (`clean_start(false)` with a
/// session expiry), so the broker keeps the filter across the reconnect and the
/// claim is *false* for that run rather than merely unobserved. Waiting ten
/// seconds for a line that cannot appear, and then reporting a regression, is
/// the failure mode this asserts its way out of.
///
/// Why the broker's log at all: the UNSUBACK is not attributed to its filter
/// in-process, so brenn's own state cannot answer whether the broker acted.
/// At `log_type all` the broker records the UNSUBSCRIBE it received and the
/// filter it applied, which is its own account of the fact.
///
/// `since` is a `harness.log_len()` taken before the reload, so what satisfies
/// the wait is a record the reload wrote. Read over the whole log the wait
/// would also accept an earlier withdrawal of the same filter — by this
/// session before the reload, or by a fixture's own first process — and a
/// reload that issued no UNSUBSCRIBE at all would pass green, which is the
/// regression these cases exist to catch.
async fn await_unsubscribe_at_broker(
    harness: &BrokerHarness,
    status: &brenn_messaging::config_reload::ReloadStatus,
    since: usize,
    address: &str,
    topic_filter: &str,
) {
    assert_no_failed_filters(status);
    assert!(
        !status.delta.mqtt_deferred.contains(&address.to_string()),
        "the session dropped during the reload, so the UNSUBSCRIBE for {address} was never sent \
         and this run cannot make the case's claim; this is not a regression in \
         unsubscribe_filter: {:?}",
        status.delta.mqtt_deferred,
    );
    harness
        .wait_for_log(
            since,
            BROKER_WAIT_SECS,
            &format!(
                "the broker never recorded brenn's UNSUBSCRIBE for {topic_filter}; a session \
                 drop after the reload is one other explanation, and a broker whose log wording \
                 log_records_unsubscribe no longer parses is another — \
                 the_matchers_read_a_live_brokers_wording in brenn-mqtt's integration suite is \
                 the case that tells them apart"
            ),
            |log| log_records_unsubscribe(log, &session_client_id(CLIENT), topic_filter),
        )
        .await;
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
    assert_eq!(service.ingress_filter_qos(CLIENT, &arrived).await, None);

    // The reload: one more binding on the same client's existing session.
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
    await_filter_at_broker(&service, &status, CLIENT, &address, &arrived).await;
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
    note_if_filter_lost(&service, CLIENT, &arrived).await;

    // The body is the ingress envelope the router builds, so the assertion is
    // on its parts rather than on the payload text alone: a message that
    // reached this channel from any other topic would pass a bare
    // payload comparison.
    let bodies = booted.bodies_until(&address, 1).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    let envelope: serde_json::Value =
        serde_json::from_str(&bodies[0]).expect("the ingress envelope is JSON");
    assert_eq!(envelope["client_slug"], CLIENT);
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

/// The other direction, over a live session: a binding the reload dropped is a
/// binding the process no longer *routes*, and an inbound publish on the removed
/// topic re-creates neither its route nor its channel.
///
/// The plan-only cases assert that the walk decided to UNSUBSCRIBE and to remove
/// the route over a fixture. This one removes a binding on a session that is
/// connected to a real broker, then publishes on the removed topic behind a
/// barrier — a publish on a topic that stayed subscribed, over the same
/// connection, so ordering makes the barrier's arrival proof that the removed
/// topic's message has been dealt with too — and asserts that nothing re-minted
/// the entry or the route for it.
///
/// **What the broker's own log is read for.** Two of the facts this case claims
/// are invisible from brenn's side: that the broker processed the UNSUBSCRIBE,
/// and that it then sent nothing on the removed topic. Once the channel and the
/// route are gone a delivery the broker kept making is discarded inside the
/// router with nothing in reach to observe it by. So the case waits on the
/// broker's `Received UNSUBSCRIBE` record before it publishes on the removed
/// topic after the reload, and afterwards reads the log for a `Sending PUBLISH`
/// on each topic. That the
/// UNSUBSCRIBE is *issued* is pinned in `brenn-mqtt`'s `unsubscribe_filter`
/// service tests; the broker's action on it is pinned here.
///
/// **The pair that makes stopping mean something.** Before the reload the case
/// publishes on the removed topic and holds the broker to having sent it to
/// this session — so the negative assertion afterwards is "was receiving, now
/// is not" and not "never was". Without it every world in which the
/// subscription never reached the broker at all — a narrowed broker ACL, a boot
/// that stopped asserting ingress filters — satisfies the negative assertion
/// while proving nothing. The two broker-log observations are windowed on
/// either side of the reload (`log_len` before it), so the delivery the case
/// arranged is not what the post-reload read finds.
///
/// The positive anchor precedes the negative assertion because it is what makes
/// the log current: the surviving topic's body arrived, so the broker sent it,
/// and waiting for that line to be in the window means the snapshot the next
/// assertion reads is one the broker has already written its decision into. The
/// one assumption about `mosquitto` this rests on is per-subscriber ordering —
/// both topics are delivered to the same subscriber session, the publisher
/// awaited the PUBACK for the removed topic before publishing the surviving
/// one, so the removed topic entered the broker's handling first, and a client's
/// outbound PUBLISHes are written through one FIFO queue per session. A
/// `Sending PUBLISH` for the removed topic, if one ever exists, is therefore
/// logged before the one for the surviving topic, whether the delivery was
/// written inside the PUBLISH handler or queued behind the inflight window. The
/// PUBACK to the publisher is not what orders it. Anchoring on the broker's
/// `Received PUBLISH` line for the removed topic instead would order nothing:
/// that line is written at the top of the handler, before any delivery is
/// queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_binding_removed_by_reload_stops_receiving_from_the_broker() {
    broker_gate!();

    let kept = format!("{TOPIC_PREFIX}/removal-kept");
    let dropped = format!("{TOPIC_PREFIX}/removal-dropped");
    let kept_address = format!("mqtt:ha:{kept}");
    let dropped_address = format!("mqtt:ha:{dropped}");
    // The negative assertion below is about what the broker sent to *this*
    // session; the publisher is on the same broker.
    let subscriber = session_client_id(CLIENT);

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");

    let (tree, mut booted) =
        boot_connected(&harness, &ca_file, components.path(), &[&kept, &dropped]).await;
    let (service, router) = booted.mqtt.clone().expect("the fixture stood one up");
    assert!(service.ingress_filter_qos(CLIENT, &dropped).await.is_some());
    let dropped_uuid = booted
        .messenger
        .directory()
        .resolve(&dropped_address)
        .expect("boot minted the entry")
        .uuid;
    assert!(router.route_uuids().contains(&dropped_uuid));

    // The "was receiving" half of the pair: the broker granted the filter, and
    // a publish on the removed topic reached the channel and this session.
    brenn_mqtt::test_support::wait_for_filter_acked(
        &service,
        CLIENT,
        &dropped,
        BROKER_WAIT_SECS,
        &format!("the broker never granted the booted subscription for {dropped}"),
    )
    .await;
    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(dropped.clone(), QoS::AtLeastOnce, false, b"before".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the publish on the topic still bound").await;
    note_if_filter_lost(&service, CLIENT, &dropped).await;
    let before = booted.bodies_until(&dropped_address, 1).await;
    assert_eq!(before.len(), 1, "{before:?}");
    harness
        .wait_for_log(
            0,
            BROKER_WAIT_SECS,
            &format!("the broker never recorded sending {dropped} to {subscriber}"),
            |log| log_records_publish_to_subscriber(log, &subscriber, &dropped),
        )
        .await;

    // Everything the broker records from here is the reload's and the publishes
    // that follow it, so the pre-reload delivery above cannot satisfy the
    // observations below.
    let before_reload = harness.log_len();

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
        service.ingress_filter_qos(CLIENT, &dropped).await,
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
        service.ingress_filter_qos(CLIENT, &kept).await.is_some(),
        "the untouched binding is collateral of nothing",
    );

    // The broker's barrier: the publish below is issued only once the broker
    // has processed the UNSUBSCRIBE, so it reaches a broker that has already
    // answered it. The connection itself was opened before the reload, for the
    // pre-reload half of the pair.
    await_unsubscribe_at_broker(&harness, &status, before_reload, &dropped_address, &dropped).await;

    // The barrier: the removed topic first, the surviving one second, on the
    // one connection. Once the second has landed the first has been answered
    // too.
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

    let log = harness
        .wait_for_log(
            before_reload,
            BROKER_WAIT_SECS,
            "the broker never recorded sending the surviving topic's publish",
            |log| log_records_publish_to_subscriber(log, &subscriber, &kept),
        )
        .await;
    assert!(
        !log_records_publish_to_subscriber(&log, &subscriber, &dropped),
        "the broker still delivered on the removed topic — the UNSUBSCRIBE did not take effect at \
         the broker: {log}",
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
                CLIENT,
                &[ConnectorHealthLabel::Connected],
                BROKER_WAIT_SECS,
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

/// **End-to-end idle-broker deploy.** A site declares a broker and binds
/// nothing through it; a later reload brings the first component, the first
/// consumer and the first `mqtt:` binding at once, and the process receives on
/// the topic without a restart.
///
/// The plan-level version is
/// `driver::tests::the_first_binding_on_a_broker_only_document_converges`;
/// this one is against a broker that is really listening, so the SUBSCRIBE is
/// answered live and the message is the broker's own copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_binding_on_an_idle_broker_receives_from_the_broker() {
    broker_gate!();

    let topic = format!("{TOPIC_PREFIX}/idle-first");
    let address = format!("mqtt:ha:{topic}");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");

    let (tree, mut booted) = boot_connected(&harness, &ca_file, components.path(), &[]).await;
    let (service, router) = booted.mqtt.clone().expect("a declared client gets one");
    assert_eq!(service.client_slugs(), vec![CLIENT.to_string()]);
    assert_eq!(service.ingress_filter_qos(CLIENT, &topic).await, None);
    assert!(router.route_uuids().is_empty());

    tree.write(&document_over_the_broker(harness.port, &ca_file, &[&topic]));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
    await_filter_at_broker(&service, &status, CLIENT, &address, &topic).await;
    let uuid = booted
        .messenger
        .directory()
        .resolve(&address)
        .expect("the reload minted the entry")
        .uuid;
    assert_eq!(router.route_uuids(), vec![uuid]);

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(topic.clone(), QoS::AtLeastOnce, false, b"first".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the first topic's publish").await;
    note_if_filter_lost(&service, CLIENT, &topic).await;

    let bodies = booted.bodies_until(&address, 1).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    let envelope: serde_json::Value =
        serde_json::from_str(&bodies[0]).expect("the ingress envelope is JSON");
    assert_eq!(envelope["client_slug"], CLIENT);
    assert_eq!(envelope["topic"], topic.as_str());
    assert_eq!(envelope["payload"]["text"], "first");

    assert!(
        poll_cursor_advanced(&booted, &address).await,
        "the consumer the reload brought never moved its cursor",
    );

    booted.stop_mqtt();
}

// ── an agent's ingress, on the wire ─────────────────────────────────────────

/// The broker declaration with the reader agent as a singleton owned by
/// `alice`, subscribing push-enabled to one `mqtt:` channel per topic.
///
/// No component and no consumer: the agent is the binding's only subscriber,
/// which is what makes the filter, the route and the position the agent's own
/// rather than a consumer's it happens to share a channel with.
fn document_agent_over_the_broker(port: u16, ca_file: &std::path::Path, topics: &[&str]) -> String {
    let addresses: Vec<String> = topics
        .iter()
        .map(|topic| format!("mqtt:ha:{topic}"))
        .collect();
    let addresses: Vec<&str> = addresses.iter().map(String::as_str).collect();
    document_push_subscribing_acl(
        &client_block(port, ca_file),
        &["alice"],
        &[],
        &addresses,
        &[],
    )
}

/// The broker declaration with the reader agent's subscribe ACL widened to the
/// topic filters `addresses` names, and no static subscription on any of them:
/// what a dynamic row on such a channel needs to stay kept.
fn covering_over_the_broker(port: u16, ca_file: &std::path::Path, addresses: &[&str]) -> String {
    let clauses: Vec<String> = addresses
        .iter()
        .map(|address| format!("topic_filter \"{address}\""))
        .collect();
    let clauses: Vec<&str> = clauses.iter().map(String::as_str).collect();
    document_subscribing_acl(&client_block(port, ca_file), &[], &clauses)
}

/// Boot `tree` over the live subsystem, optionally over a store a previous boot
/// left, and wait for the session to reach the broker.
async fn boot_live(tree: &Tree, db: Option<brenn_db::Db>) -> Booted {
    let booted = boot_with(
        tree,
        BootFixture {
            db,
            mqtt_live: true,
            ..BootFixture::default()
        },
    )
    .await;
    let (service, _) = booted.mqtt.clone().expect("the fixture stood one up");
    brenn_mqtt::test_support::wait_for_health(
        &service,
        CLIENT,
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the booted session never reached the broker",
    )
    .await;
    booted
}

/// The motivating shape on the ingress side: an `mqtt_subscription` added to an
/// agent by reload is a topic the agent's conversation is positioned on and
/// receives on, with no restart.
///
/// Four things have to move together, and the assertions take them in order:
/// the broker filter, the router's route, the agent's subscriber entry in the
/// live directory, and its conversation's position on the channel. Then the
/// broker's own copy of a message on that topic, which is the only proof that
/// the filter was answered rather than merely recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_binding_added_by_reload_receives_from_the_broker() {
    broker_gate!();

    let topic = format!("{TOPIC_PREFIX}/agent-added");
    let address = format!("mqtt:ha:{topic}");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");

    let tree = Tree::holding(&document_agent_over_the_broker(harness.port, &ca_file, &[]));
    let mut booted = boot_live(&tree, None).await;
    seat_user(&booted.db, "alice").await;
    let (service, router) = booted.mqtt.clone().expect("the fixture stood one up");
    assert_eq!(service.ingress_filter_qos(CLIENT, &topic).await, None);

    tree.write(&document_agent_over_the_broker(
        harness.port,
        &ca_file,
        &[&topic],
    ));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
    assert_eq!(
        status.delta.subscriptions_added,
        vec![format!("{READER} {address}")],
    );
    await_filter_at_broker(&service, &status, CLIENT, &address, &topic).await;
    assert!(
        service.ingress_filter_qos(CLIENT, &topic).await.is_some(),
        "the agent's filter is one the supervisor holds",
    );
    let uuid = booted
        .messenger
        .directory()
        .resolve(&address)
        .expect("the reload minted the entry")
        .uuid;
    assert!(router.route_uuids().contains(&uuid));
    assert!(
        live_entry_of_reader(&booted, &address).is_some(),
        "the agent is folded onto the channel its document now names",
    );
    let conversation = conversation_of(&booted, READER)
        .await
        .expect("the attach minted the agent's singleton conversation");
    assert!(
        cursor_of(
            &booted.messenger,
            &address,
            &brenn_lib::messaging::ParticipantId::for_conversation(conversation),
        )
        .await
        .is_some(),
        "and gave it a position on the topic it now reads",
    );

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(topic.clone(), QoS::AtLeastOnce, false, b"agent".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the agent topic's publish").await;
    note_if_filter_lost(&service, CLIENT, &topic).await;

    let bodies = booted.bodies_until(&address, 1).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    let envelope: serde_json::Value =
        serde_json::from_str(&bodies[0]).expect("the ingress envelope is JSON");
    assert_eq!(envelope["client_slug"], CLIENT);
    assert_eq!(envelope["topic"], topic.as_str());
    assert_eq!(envelope["payload"]["text"], "agent");

    booted.stop_mqtt();
}

/// A process holding one durable dynamic `mqtt:` row for the reader on `topic`,
/// booted over `document`.
///
/// Two boots over one store, because the state under test is one a runtime
/// `MessageSubscribe` leaves and a restart then meets: the first boot declares
/// the channel — which is what puts its `messaging_channels` row in the store,
/// the row boot's reconstruction reads a dormant subscription's channel back
/// out of — and mints the dynamic row; the second is the process the reload
/// runs against. The first stands no supervisor up, so the two never contend
/// for the broker's session.
async fn boot_over_a_dynamic_row(
    port: u16,
    ca_file: &std::path::Path,
    topic: &str,
    document: &str,
) -> (Tree, Booted) {
    let address = format!("mqtt:ha:{topic}");
    let db = brenn_server::test_support::init_db_memory();
    let components = tempfile::tempdir().expect("a components root");
    let declaring = Tree::holding(&document_over_the_broker(port, ca_file, &[topic]));
    install_package(components.path(), &staged_module(&declaring));
    let first = boot_with(
        &declaring,
        BootFixture {
            db: Some(db.clone()),
            components_roots: vec![components.path().to_path_buf()],
            mqtt: true,
            ..BootFixture::default()
        },
    )
    .await;
    // QoS 1, the `qos` the document's client declares and the one a
    // `MessageSubscribe` through it would have stored.
    insert_dynamic_mqtt_row(&first, &address, false, 1).await;
    drop(first);

    let tree = Tree::holding(document);
    let booted = boot_live(&tree, Some(db)).await;
    (tree, booted)
}

/// A dormant dynamic `mqtt:` subscription the candidate authorizes again is a
/// topic the process receives on: the filter is asserted at the broker, the
/// route is added, and the broker's own copy of a message on it reaches the
/// channel.
///
/// The plan-level revive case proves the entry came back at the row's own
/// depths. Only a real broker can say whether the filter behind it did.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revived_dynamic_binding_receives_from_the_broker() {
    broker_gate!();

    let topic = format!("{TOPIC_PREFIX}/dynamic-revived");
    let address = format!("mqtt:ha:{topic}");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");

    // Booted with the ACL narrow, so the row is dormant and its filter is one
    // this process has never asserted.
    let narrow = document_over_the_broker(harness.port, &ca_file, &[]);
    let (tree, mut booted) = boot_over_a_dynamic_row(harness.port, &ca_file, &topic, &narrow).await;
    let (service, router) = booted.mqtt.clone().expect("the fixture stood one up");
    assert_eq!(service.ingress_filter_qos(CLIENT, &topic).await, None);
    assert!(live_entry_of_reader(&booted, &address).is_none());

    tree.write(&covering_over_the_broker(
        harness.port,
        &ca_file,
        &[&address],
    ));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(
        status.delta.dynamic_revived,
        vec![format!("{READER} {address}")],
    );
    assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
    await_filter_at_broker(&service, &status, CLIENT, &address, &topic).await;
    assert!(
        service.ingress_filter_qos(CLIENT, &topic).await.is_some(),
        "the revived row's filter is one the supervisor now holds",
    );
    let uuid = booted
        .messenger
        .directory()
        .resolve(&address)
        .expect("boot reconstructed the dormant row's channel")
        .uuid;
    assert!(router.route_uuids().contains(&uuid));

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(topic.clone(), QoS::AtLeastOnce, false, b"revived".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the revived topic's publish").await;
    note_if_filter_lost(&service, CLIENT, &topic).await;

    let bodies = booted.bodies_until(&address, 1).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    let envelope: serde_json::Value =
        serde_json::from_str(&bodies[0]).expect("the ingress envelope is JSON");
    assert_eq!(envelope["topic"], topic.as_str());
    assert_eq!(envelope["payload"]["text"], "revived");

    booted.stop_mqtt();
}

/// The other direction: an ACL narrowed under a live dynamic `mqtt:`
/// subscription takes the filter out of the set the supervisor re-asserts and
/// the route out of the table, and keeps the row for the day the ACL returns.
///
/// The row is the filter's only subscriber, which is the case worth checking —
/// a filter another subscriber still needs is one the walk must leave alone,
/// and the plan-level cases cover that split.
///
/// What the name claims — that the filter left the *broker* — is read off the
/// broker's own log, which records the UNSUBSCRIBE it processed, over what the
/// broker appended after the reload: the fixture's first process holds the same
/// topic, so a read over the whole log is one fixture change away from being
/// satisfied by a line this reload did not write. The case publishes nothing
/// and so needs no negative delivery assertion: the record of the withdrawal is
/// the whole claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_dynamic_binding_leaves_the_brokers_filter_set() {
    broker_gate!();

    let topic = format!("{TOPIC_PREFIX}/dynamic-revoked");
    let address = format!("mqtt:ha:{topic}");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");

    // Booted with the ACL wide, so the boot merge folds the row and the rig
    // re-activates its filter and route.
    let narrow = document_over_the_broker(harness.port, &ca_file, &[]);
    let (tree, mut booted) = boot_over_a_dynamic_row(
        harness.port,
        &ca_file,
        &topic,
        &covering_over_the_broker(harness.port, &ca_file, &[&address]),
    )
    .await;
    let (service, router) = booted.mqtt.clone().expect("the fixture stood one up");
    assert!(
        service.ingress_filter_qos(CLIENT, &topic).await.is_some(),
        "the kept row's filter is asserted at boot",
    );
    let uuid = booted
        .messenger
        .directory()
        .resolve(&address)
        .expect("boot reconstructed the row's channel")
        .uuid;
    assert!(router.route_uuids().contains(&uuid));

    // The fixture's first process holds the same topic; the window is what
    // keeps anything it recorded out of the wait below.
    let before_reload = harness.log_len();
    tree.write(&narrow);
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(
        status.delta.dynamic_revoked,
        vec![format!("{READER} {address}")],
    );
    assert_eq!(status.delta.mqtt_unsubscribed, vec![address.clone()]);
    await_unsubscribe_at_broker(&harness, &status, before_reload, &address, &topic).await;
    assert_eq!(
        service.ingress_filter_qos(CLIENT, &topic).await,
        None,
        "the filter must leave the set the supervisor re-asserts on reconnect",
    );
    assert!(
        !router.route_uuids().contains(&uuid),
        "the ingress route outlived the subscription it delivered for",
    );
    assert!(live_entry_of_reader(&booted, &address).is_none());
    assert_eq!(
        dynamic_rows(&booted).await.len(),
        1,
        "the durable row is kept, so the subscription resumes if the ACL comes back",
    );

    booted.stop_mqtt();
}

/// **The oracle over an agent's `mqtt_subscription` arriving, and over one
/// leaving.**
///
/// The same argument as [`an_mqtt_binding_matches_a_fresh_boot`], on the agent
/// side: the reload's filter set, route table, directory entry, conversation
/// and cursor rows against a fresh boot's, so a piece of state the walk moved
/// and should not have — or left where a boot would not — shows up unnamed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_mqtt_subscription_matches_a_fresh_boot() {
    broker_gate!();

    let topic = format!("{TOPIC_PREFIX}/agent-oracle");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");

    compare_agent_over_the_broker(&harness, &ca_file, &[], &[&topic], 1).await;
    compare_agent_over_the_broker(&harness, &ca_file, &[&topic], &[], 0).await;
}

/// Boot the agent document over `before`, reload onto `after`, and compare the
/// reloaded process against a fresh boot of `after`.
///
/// The owner's user row is seeded before the database copy: both sides have to
/// start from it, since the attach the transition runs resolves the agent's
/// conversation through it.
async fn compare_agent_over_the_broker(
    harness: &BrokerHarness,
    ca_file: &std::path::Path,
    before: &[&str],
    after: &[&str],
    filters: usize,
) {
    let tree = Tree::holding(&document_agent_over_the_broker(
        harness.port,
        ca_file,
        before,
    ));

    let fixture = |db| BootFixture {
        db: Some(db),
        mqtt_live: true,
        ..BootFixture::default()
    };

    super::oracle_tests::a_seeded_reload_matches_a_fresh_boot(
        &tree,
        fixture,
        async |booted| {
            seat_user(&booted.db, "alice").await;
        },
        async |booted| {
            let (service, _) = booted.mqtt.clone().expect("the fixture stood one up");
            brenn_mqtt::test_support::wait_for_health(
                &service,
                CLIENT,
                &[ConnectorHealthLabel::Connected],
                BROKER_WAIT_SECS,
                "the booted session never reached the broker",
            )
            .await;

            tree.write(&document_agent_over_the_broker(
                harness.port,
                ca_file,
                after,
            ));
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

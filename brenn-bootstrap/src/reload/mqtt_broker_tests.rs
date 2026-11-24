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
//! These cases need loopback and a `large` time budget the in-process suite
//! should not carry, so this module is filtered out of the crate's main test
//! target and run by one of its own, which also declares the broker
//! (`//bazel/tools:mosquitto`) as an input. The gate that decides between
//! running and skipping is `brenn_mqtt::broker_gate!`, beside the harness it
//! gates.

mod egress;

use std::sync::Arc;

use brenn_mqtt::broker_gate;
use brenn_mqtt::state::ConnectorHealthLabel;
use brenn_mqtt::test_support::{
    AUTH_CREDENTIALS, BrokerHarness, ROTATED_CREDENTIALS, await_puback, certs, connection_epoch,
    direct_publisher_acked, direct_publisher_acked_as, log_records_connack, log_records_connect,
    log_records_disconnect, log_records_publish_to_subscriber, log_records_subscribe_at,
    log_records_unsubscribe, session_client_id,
};
use rumqttc::mqttbytes::QoS;

use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Booted, READER, Tree, bodies_on, boot_with, conversation_of, cursor_of, document,
    document_push_subscribing_acl, document_subscribing_acl, document_with_agent,
    dynamic_reader_agent, dynamic_rows, egress_entry_noise, insert_dynamic_row, install_package,
    live_entry_of_reader, runtime_subscribe, seat_user, staged_module, staged_module_opt,
};
use brenn_messaging::config_reload::Outcome;

/// The topic prefix the broker's ACL admits.
const TOPIC_PREFIX: &str = "brenn/itest/reload";

/// The `mqtt_client` declaration every document in this module stands on.
///
/// One copy for every shape: the idle-broker test boots a document with no
/// binding and reloads onto a bound one, and a second copy that drifted in
/// `url` or `qos` would restart the client's supervisor mid-case — a change
/// these tests are not about, in the middle of the applied path they are.
fn client_block(port: u16, ca_file: &std::path::Path) -> String {
    client_block_at(port, ca_file, 1)
}

/// [`client_block`] at `qos`, for the cases about a `qos` edit.
fn client_block_at(port: u16, ca_file: &std::path::Path, qos: u8) -> String {
    format!(
        r#"mqtt_client ha {{
    url = "mqtts://127.0.0.1:{port}";
    ca_file = "{ca}";
    qos = {qos};
}}
"#,
        ca = ca_file.display(),
    )
}

/// The `mqtt_client` declaration of the kept-session cases: [`client_block`]
/// asking the broker to keep the session for a minute after a connection
/// ends, and retrying a lost broker at least every two seconds.
fn kept_client_block(port: u16, ca_file: &std::path::Path) -> String {
    format!(
        r#"mqtt_client ha {{
    url = "mqtts://127.0.0.1:{port}";
    ca_file = "{ca}";
    qos = 1;
    session_expiry_secs = 60;
    reconnect_backoff_max_secs = 2;
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
    document_over_a_declared_client(&client_block(port, ca_file), topics)
}

/// [`document_over_the_broker`] over an arbitrary `mqtt_client` block, for the
/// cases that vary the declaration itself rather than what is bound through it.
///
/// With `block` empty it is the document with no broker at all — no client, no
/// component and no consumer — which is what a reload that brings the first
/// client starts from.
fn document_over_a_declared_client(block: &str, topics: &[&str]) -> String {
    let mut body = block.to_string();
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
    let booted = boot_connected_on(&tree, components).await;
    (tree, booted)
}

/// [`boot_connected`] over a tree a case built itself — one whose client block
/// carries credentials, or whose secret files had to exist before the document
/// naming them.
async fn boot_connected_on(tree: &Tree, components: &std::path::Path) -> Booted {
    boot_connected_over(tree, components, None, None).await
}

/// [`boot_connected_on`] over a store a previous boot left, when `db` names
/// one, and with a look at the broker between building the MQTT sessions and
/// wiring them, when `before_mqtt_wiring` is one.
async fn boot_connected_over(
    tree: &Tree,
    components: &std::path::Path,
    db: Option<brenn_db::Db>,
    before_mqtt_wiring: Option<Box<dyn FnOnce() + Send>>,
) -> Booted {
    let booted = boot_declared_over(tree, components, db, before_mqtt_wiring).await;
    let (service, _) = booted.mqtt.clone();
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

/// Boot `tree` over the live subsystem without waiting on any session: the
/// shape for a document that declares no client at all.
async fn boot_declared(tree: &Tree, components: &std::path::Path) -> Booted {
    boot_declared_over(tree, components, None, None).await
}

/// [`boot_declared`] over the store `db` names, if any, running
/// `before_mqtt_wiring`, if any, where `BootFixture::before_mqtt_wiring` says.
async fn boot_declared_over(
    tree: &Tree,
    components: &std::path::Path,
    db: Option<brenn_db::Db>,
    before_mqtt_wiring: Option<Box<dyn FnOnce() + Send>>,
) -> Booted {
    // A broker-only document declares no component and so stages no module,
    // and there is then no package to install; the reload that brings the first
    // component installs it. Read off the tree rather than off `topics`, so the
    // builder stays the only thing that knows which documents stage one.
    if let Some(module) = staged_module_opt(tree) {
        install_package(components, &module);
    }
    boot_with(
        tree,
        BootFixture {
            db,
            components_roots: vec![components.to_path_buf()],
            mqtt_live: true,
            before_mqtt_wiring,
            dispatcher: true,
            ..BootFixture::default()
        },
    )
    .await
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
/// tolerates a deferral as that one does: a withdrawal deferred on a session
/// drop stays asserted, and the reconnect that finds the broker's session
/// sends its UNSUBSCRIBE, so the claim can still be proven after a wait. A
/// reconnect that finds no session sends none, and the broker then holds no
/// filter either; that is the residual this wait cannot close.
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
    if status
        .delta
        .mqtt_unsubscribe_deferred
        .contains(&address.to_string())
    {
        eprintln!(
            "mqtt_live: the UNSUBSCRIBE for {address} deferred on a session drop; waiting for \
             the reconnect that finds the session to send it",
        );
    }
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
    let (service, router) = booted.mqtt.clone();
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
    let (service, router) = booted.mqtt.clone();
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
/// `build_mqtt` and `wire_and_spawn` do, and that needs a broker.
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
            let (service, _) = booted.mqtt.clone();
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
    let (service, router) = booted.mqtt.clone();
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
    let (service, _) = booted.mqtt.clone();
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
    seat_user(&booted, "alice").await;
    let (service, router) = booted.mqtt.clone();
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
            ..BootFixture::default()
        },
    )
    .await;
    insert_dynamic_row(&first, &address, false).await;
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
    let (service, router) = booted.mqtt.clone();
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
    let (service, router) = booted.mqtt.clone();
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
            seat_user(booted, "alice").await;
        },
        async |booted| {
            let (service, _) = booted.mqtt.clone();
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

// ===========================================================================
// The `[[mqtt_client]]` block itself, against a broker that is really running.
//
// Every other client case in this crate proves the *registry*: which handle the
// commit built, which subscriptions it carried, in what order relative to the
// agent swap. None can prove the packets — the handles those fixtures register
// hold no connection, so a client the reload added never dials and one it
// stopped never says goodbye. These four cases are the packet half: the first
// binding on a client this reload declared receives, a stopped client's
// DISCONNECT reaches the broker, an edited credential re-authenticates and
// keeps receiving, and a wrong one applies and then fails.
// ===========================================================================

/// [`client_block`] with the credentials a password-authenticating broker
/// requires: the account name in the document, its password in a file the
/// document points at.
fn credentialed_client_block(
    port: u16,
    ca_file: &std::path::Path,
    username: &str,
    password_file: &std::path::Path,
) -> String {
    format!(
        r#"mqtt_client ha {{
    url = "mqtts://127.0.0.1:{port}";
    ca_file = "{ca}";
    username = "{username}";
    password_file = "{password}";
    qos = 1;
}}
"#,
        ca = ca_file.display(),
        password = password_file.display(),
    )
}

/// Write `ca_file` under a fresh directory and hand back both, so a case's
/// document can name the CA the harness's broker presents.
fn trusted_ca() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("a directory for the CA");
    let path = dir.path().join("ca.pem");
    std::fs::write(&path, certs::ca_pem()).expect("the CA is writable");
    (dir, path)
}

/// **The deploy story the facility exists for.** A site with no broker at all
/// declares one, and the first binding through it receives, with no restart.
///
/// The plan-level version is
/// `driver::tests::an_added_client_is_registered_and_its_first_binding_defers`,
/// which is where the deferral is pinned; against a fixture handle that is all
/// it can say. Here the supervisor the commit spawned dials a broker that is
/// listening, so the case follows the same filter to the end: the broker grants
/// it, an outside publisher publishes on the topic, and the message reaches the
/// channel the reload minted.
///
/// The filters of a client this reload added are normally `mqtt_deferred` — the
/// supervisor was spawned a few commit steps before the incoming step ran and
/// its client cell is usually still empty — but not deterministically: the
/// steps between the two write channels to the database, swap the agents and
/// join every other restarting session, and a localhost broker can answer the
/// CONNECT inside that span. So the filter is held to being a *move* that the
/// broker has not refused, either reported live or deferred, and the walk below
/// follows it to the broker's own grant, which is the claim that matters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_added_by_reload_connects_and_its_first_binding_receives() {
    broker_gate!();

    let arrived = format!("{TOPIC_PREFIX}/client-added");
    let address = format!("mqtt:ha:{arrived}");

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");

    // No broker in the document at all: the registry is empty and nothing has
    // ever dialed.
    let tree = Tree::holding(&document_over_a_declared_client("", &[]));
    let mut booted = boot_declared(&tree, components.path()).await;
    let (service, router) = booted.mqtt.clone();
    assert!(
        service.client_slugs().is_empty(),
        "{:?}",
        service.client_slugs(),
    );

    tree.write(&document_over_the_broker(
        harness.port,
        &ca_file,
        &[&arrived],
    ));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_added, vec![CLIENT.to_string()]);
    assert!(status.delta.mqtt_clients_removed.is_empty());
    assert!(status.delta.mqtt_clients_changed.is_empty());
    assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
    assert!(
        status.delta.mqtt_deferred == vec![address.clone()]
            || status.delta.mqtt_deferred.is_empty(),
        "the filter is either registered for the supervisor's first connect or was sent live on \
         a connection it had already made, and nothing else may have deferred: {:?}",
        status.delta.mqtt_deferred,
    );
    assert_no_failed_filters(&status);

    // The supervisor the commit spawned dials on its own; from here the case is
    // the same walk as an added binding on a client that was already up.
    brenn_mqtt::test_support::wait_for_health(
        &service,
        CLIENT,
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the session the reload registered never reached the broker",
    )
    .await;
    brenn_mqtt::test_support::wait_for_filter_acked(
        &service,
        CLIENT,
        &arrived,
        BROKER_WAIT_SECS,
        &format!("the broker never granted a subscription for {arrived}"),
    )
    .await;
    let arrived_uuid = booted
        .messenger
        .directory()
        .resolve(&address)
        .expect("the reload minted the entry")
        .uuid;
    assert!(router.route_uuids().contains(&arrived_uuid));
    assert_eq!(
        egress_entry_noise(&booted.messenger, CLIENT),
        Some(brenn_lib::messaging::config::NoiseLevel::Alarm),
        "the first client's egress channel carries the egress participant's entry",
    );

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(arrived.clone(), QoS::AtLeastOnce, false, b"first".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the arriving topic's publish").await;
    note_if_filter_lost(&service, CLIENT, &arrived).await;

    let bodies = booted.bodies_until(&address, 1).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    let envelope: serde_json::Value =
        serde_json::from_str(&bodies[0]).expect("the ingress envelope is JSON");
    assert_eq!(envelope["client_slug"], CLIENT);
    assert_eq!(envelope["topic"], arrived.as_str());
    assert_eq!(envelope["payload"]["text"], "first", "{bodies:?}");

    booted.stop_mqtt();
}

/// A client the candidate no longer declares says goodbye to the broker.
///
/// The registry half is pinned over a fixture handle by
/// `driver::tests::a_removed_client_is_stopped_and_contributes_no_filter_moves`.
/// What only a broker can answer is whether the stop was *orderly*: the commit
/// signals the supervisor and joins it, and the supervisor's stop arm drains a
/// DISCONNECT under its own timeout before the task exits. A join that returned
/// on a supervisor which dropped its connection instead is invisible in
/// process — brenn discards the session's state as it goes — and is what
/// [`log_records_disconnect`] reads off the broker's own account.
///
/// The filter lists are held empty on the way past: a stopped client's filters
/// leave with its session, and reporting them as withdrawn — or as deferred,
/// which a session about to be stopped can never make good — would misdescribe
/// what happened. Its channel is gone from the directory, which is the half an
/// operator reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_removed_by_reload_disconnects() {
    broker_gate!();

    let topic = format!("{TOPIC_PREFIX}/client-removed");
    let address = format!("mqtt:ha:{topic}");

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");

    let (tree, mut booted) = boot_connected(&harness, &ca_file, components.path(), &[&topic]).await;
    let (service, _router) = booted.mqtt.clone();
    let session = session_client_id(CLIENT);

    // What the broker records from here is this reload's own account.
    let before = harness.log_len();
    tree.write(&document_over_a_declared_client("", &[]));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_removed, vec![CLIENT.to_string()]);
    assert!(status.delta.mqtt_clients_added.is_empty());
    assert!(status.delta.mqtt_clients_changed.is_empty());
    assert!(
        status.delta.mqtt_unsubscribed.is_empty() && status.delta.mqtt_deferred.is_empty(),
        "a stopped client's filters left with its session and are no reload's moves: {:?} {:?}",
        status.delta.mqtt_unsubscribed,
        status.delta.mqtt_deferred,
    );
    assert_no_failed_filters(&status);
    assert!(status.delta.channels_removed.contains(&address));
    assert!(
        service.client_slugs().is_empty(),
        "{:?}",
        service.client_slugs(),
    );

    harness
        .wait_for_log(
            before,
            BROKER_WAIT_SECS,
            &format!(
                "the broker never recorded a DISCONNECT from {session}; a supervisor that dropped \
                 its socket instead of draining one is the regression this reads for, and a \
                 broker whose wording log_records_disconnect no longer parses is the other \
                 explanation — the_disconnect_matcher_reads_a_live_brokers_wording in brenn-mqtt's \
                 integration suite is the case that tells them apart"
            ),
            |log| log_records_disconnect(log, &session),
        )
        .await;

    booted.stop_mqtt();
}

/// An edited credential is a supervisor restart that re-authenticates and keeps
/// receiving, with no restart of the process.
///
/// Against a broker with `allow_anonymous false`, so reaching `Connected` at
/// all is the broker's own answer that the account and the password the
/// document now names were accepted. The filter is not a move — the successor
/// carries the predecessor's list — so what proves the re-assert is the
/// broker's grant on the *new* session: the successor starts with the filter
/// registered and ungranted, and the supervisor re-asserts it on its first
/// connect.
///
/// Deviation from a pure password rotation: the account name moves with the
/// password, because a mosquitto password file holds one entry per user and
/// both sides of the change have to be a login the broker really accepts. That
/// a rotation of the password *bytes alone* — no document edit — restarts the
/// client is pinned where it needs no broker,
/// `driver::tests::a_rotated_broker_password_restarts_the_client_without_a_document_edit`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_whose_credentials_changed_reconnects_and_keeps_receiving() {
    broker_gate!();

    let topic = format!("{TOPIC_PREFIX}/credentials-changed");
    let address = format!("mqtt:ha:{topic}");

    let harness = BrokerHarness::start_auth();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");

    // The secret is written before the document that names it: it has to be
    // readable at boot as well as at the reload.
    let tree = Tree::new();
    let first = tree.secret("broker.pw", AUTH_CREDENTIALS.1);
    tree.write(&document_over_a_declared_client(
        &credentialed_client_block(harness.port, &ca_file, AUTH_CREDENTIALS.0, &first),
        &[&topic],
    ));
    let mut booted = boot_connected_on(&tree, components.path()).await;
    let (service, _router) = booted.mqtt.clone();
    let before = service.get_client(CLIENT).expect("a declared client");

    // Opened before the reload, so the first line in it naming this session's
    // client id is the restart's, not the boot's.
    let log_from = harness.log_len();
    let rotated = tree.secret("broker.rotated.pw", ROTATED_CREDENTIALS.1);
    tree.write(&document_over_a_declared_client(
        &credentialed_client_block(harness.port, &ca_file, ROTATED_CREDENTIALS.0, &rotated),
        &[&topic],
    ));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec![CLIENT.to_string()]);
    assert!(status.delta.mqtt_clients_added.is_empty());
    assert!(status.delta.mqtt_clients_removed.is_empty());
    assert!(
        status.delta.mqtt_subscribed.is_empty() && status.delta.mqtt_unsubscribed.is_empty(),
        "a carried filter is not a move: {:?} {:?}",
        status.delta.mqtt_subscribed,
        status.delta.mqtt_unsubscribed,
    );
    assert_no_failed_filters(&status);

    let after = service
        .get_client(CLIENT)
        .expect("the slug is never absent");
    assert!(
        !Arc::ptr_eq(&before, &after),
        "the changed client kept its predecessor's handle",
    );
    assert_eq!(
        after.config.identity.username.as_deref(),
        Some(ROTATED_CREDENTIALS.0),
    );
    assert_eq!(
        after.config.password.as_deref(),
        Some(ROTATED_CREDENTIALS.1)
    );

    brenn_mqtt::test_support::wait_for_health(
        &service,
        CLIENT,
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the restarted session never re-authenticated against the broker",
    )
    .await;
    brenn_mqtt::test_support::wait_for_filter_acked(
        &service,
        CLIENT,
        &topic,
        BROKER_WAIT_SECS,
        &format!("the restarted session never re-asserted {topic} at the broker"),
    )
    .await;

    // The handover's own claim, which no in-process state can make: the
    // predecessor's DISCONNECT reached the broker before the successor's
    // CONNECT.
    let session_id = brenn_mqtt::test_support::session_client_id(CLIENT);
    let window = harness
        .wait_for_log(
            log_from,
            BROKER_WAIT_SECS,
            &format!("the broker never recorded a DISCONNECT from {session_id}"),
            |log| brenn_mqtt::test_support::log_records_disconnect(log, &session_id),
        )
        .await;
    assert!(
        brenn_mqtt::test_support::log_records_disconnect_before_reconnect(&window, &session_id),
        "the successor connected under {session_id} before the predecessor's DISCONNECT was \
         drained, which is a client-id takeover: {window}",
    );

    // The auth broker admits nobody anonymously, so the case's own publisher
    // logs in as the account brenn just left.
    let (publisher, mut acks) =
        direct_publisher_acked_as(harness.port, certs::ca_pem_bytes(), Some(AUTH_CREDENTIALS))
            .await;
    publisher
        .publish(topic.clone(), QoS::AtLeastOnce, false, b"after".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the publish after the credential change").await;
    note_if_filter_lost(&service, CLIENT, &topic).await;

    let bodies = booted.bodies_until(&address, 1).await;
    assert_eq!(bodies.len(), 1, "{bodies:?}");
    let envelope: serde_json::Value =
        serde_json::from_str(&bodies[0]).expect("the ingress envelope is JSON");
    assert_eq!(envelope["payload"]["text"], "after", "{bodies:?}");

    booted.stop_mqtt();
}

/// A credential the broker rejects is an `applied` reload and then a `Failed`
/// supervisor — not a refusal.
///
/// Prepare cannot dial the broker: that would make every reload wait on a
/// network round trip and turn a slow broker into a refused deploy. So the
/// document is accepted, the client is restarted with it, and the answer
/// arrives from the broker afterwards, exactly as it would for a fresh boot of
/// the same document. The rejection is authoritative — mosquitto answers the
/// CONNACK with a bad-credentials reason code — so the supervisor stops
/// retrying, the session's health is `Failed`, and the router raises a
/// `Critical` alert naming the client.
///
/// No document edit: the rotation is the password file's bytes alone, which is
/// the shape an operator's own rotation takes and the one the status body reads
/// as a changed client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_whose_new_credentials_are_wrong_reaches_failed_after_an_applied_reload() {
    broker_gate!();

    let topic = format!("{TOPIC_PREFIX}/credentials-wrong");
    let address = format!("mqtt:ha:{topic}");

    let harness = BrokerHarness::start_auth();
    let (_ca_dir, ca_file) = trusted_ca();
    let components = tempfile::tempdir().expect("a components root");

    let tree = Tree::new();
    let password = tree.secret("broker.pw", AUTH_CREDENTIALS.1);
    tree.write(&document_over_a_declared_client(
        &credentialed_client_block(harness.port, &ca_file, AUTH_CREDENTIALS.0, &password),
        &[&topic],
    ));
    let mut booted = boot_connected_on(&tree, components.path()).await;
    let (service, _router) = booted.mqtt.clone();

    std::fs::write(&password, "not-this-accounts-password").expect("the secret file is writable");
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec![CLIENT.to_string()]);
    assert!(
        status.delta.mqtt_failed.is_empty(),
        "the supervisor has not had time to fail when the incoming step runs: {:?}",
        status.delta.mqtt_failed,
    );

    brenn_mqtt::test_support::wait_for_health(
        &service,
        CLIENT,
        &[ConnectorHealthLabel::Failed],
        BROKER_WAIT_SECS,
        "the restarted session never reached Failed against the broker that rejected it",
    )
    .await;
    // The filter is registered on the handle and would be asserted by a process
    // whose credentials work, which is why the reload applied.
    assert_eq!(service.ingress_filter_qos(CLIENT, &topic).await, Some(1));
    assert!(
        booted.messenger.directory().resolve(&address).is_some(),
        "the ingress channel is the document's whatever the broker thinks of the credentials",
    );

    booted.stop_mqtt();
}

// ── which channels an inbound message reaches ──────────────────────────────

/// The `topic` field of each ingress envelope in `bodies`, in order.
fn topics_of(bodies: &[String]) -> Vec<String> {
    bodies
        .iter()
        .map(|body| {
            let envelope: serde_json::Value =
                serde_json::from_str(body).expect("the ingress envelope is JSON");
            envelope["topic"]
                .as_str()
                .expect("the envelope names its topic")
                .to_string()
        })
        .collect()
}

/// Poll until `address` holds at least `wanted` rows, and answer with their
/// bodies. The timeout diagnostic names the client's connection epoch and
/// health, because a reconnect is what a count-claiming case cannot survive.
async fn rows_until(booted: &Booted, address: &str, wanted: usize, what: &str) -> Vec<String> {
    let (service, _) = booted.mqtt.clone();
    let messenger = &booted.messenger;
    brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async {
            let bodies = bodies_on(messenger, address).await;
            (bodies.len() >= wanted).then_some(bodies)
        },
        || async {
            let held = bodies_on(messenger, address).await.len();
            let epoch = connection_epoch(&service, CLIENT).await;
            let (label, error) = service.ingress_health(CLIENT).await;
            format!(
                "{what}: {address} holds {held} of {wanted} rows (connection epoch {epoch}, \
                 health {label:?} {error:?})"
            )
        },
    )
    .await
}

/// A message on a topic two filters of one client cover is one row in each
/// filter's channel — not one row per covering filter in each.
///
/// The broker sends this session one copy per matching subscription, each
/// carrying that subscription's identifier, and each copy goes to its own
/// subscription's channel only. The barrier is covered by the wide filter
/// alone, and the broker writes one session's copies in order, so once the
/// barrier is in the wide channel both copies of the first message have been
/// routed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_filters_put_one_row_in_each_channel() {
    broker_gate!();

    let wide = format!("{TOPIC_PREFIX}/ov/#");
    let narrow = format!("{TOPIC_PREFIX}/ov/b");
    let barrier = format!("{TOPIC_PREFIX}/ov/end");
    let wide_address = format!("mqtt:{CLIENT}:{wide}");
    let narrow_address = format!("mqtt:{CLIENT}:{narrow}");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");

    let (_tree, mut booted) =
        boot_connected(&harness, &ca_file, components.path(), &[&wide, &narrow]).await;
    let (service, _router) = booted.mqtt.clone();
    for filter in [&wide, &narrow] {
        brenn_mqtt::test_support::wait_for_filter_acked(
            &service,
            CLIENT,
            filter,
            BROKER_WAIT_SECS,
            &format!("the broker never granted {filter}"),
        )
        .await;
    }

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(narrow.clone(), QoS::AtLeastOnce, false, b"once".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the overlapped topic's publish").await;
    publisher
        .publish(barrier.clone(), QoS::AtLeastOnce, false, b"end".to_vec())
        .await
        .expect("the broker took the barrier");
    await_puback(&mut acks, "the barrier's publish").await;
    note_if_filter_lost(&service, CLIENT, &wide).await;

    let wide_bodies = booted.bodies_until(&wide_address, 2).await;
    assert_eq!(
        topics_of(&wide_bodies),
        vec![narrow.clone(), barrier.clone()],
        "the wide channel holds the message once and the barrier once: {wide_bodies:?}",
    );
    let narrow_bodies = bodies_on(&booted.messenger, &narrow_address).await;
    assert_eq!(
        topics_of(&narrow_bodies),
        vec![narrow.clone()],
        "the narrow channel holds the message once: {narrow_bodies:?}",
    );

    booted.stop_mqtt();
}

/// How many retained messages the burst case stores before brenn subscribes.
const BURST: usize = 150;

/// The broker declaration with the reader agent holding `wide` pull-only as a
/// static subscription, and granted what a runtime `mqtt:` subscribe under it
/// needs: the `dynamic_subscribe` grant beside `subscribe`, and a `topic_filter`
/// clause covering every filter `wide` covers.
fn document_dynamic_under(port: u16, ca_file: &std::path::Path, wide: &str) -> String {
    let address = format!("mqtt:{CLIENT}:{wide}");
    document_with_agent(
        &client_block(port, ca_file),
        &dynamic_reader_agent(
            &address,
            &format!("    subscribe \"{address}\" {{ push_depth = 0; retain_depth = 4; }}\n"),
            &[READER],
        ),
    )
}

/// A runtime subscribe to a narrower filter receives every retained message the
/// broker holds under it, each exactly once, and none of them reaches the wider
/// channel the same client already holds.
///
/// 150 retained messages are stored before brenn connects; the wide filter is
/// a static binding, the narrow one is added the way `MessageSubscribe` adds
/// it. `"subscribed"` is answered before the replay is in the channel, so the
/// case records how many rows were there at the return and how long the rest
/// took, and prints both.
///
/// A reconnect is not tolerated: every reconnect asserts every held filter
/// again, the broker replays each one's retained messages into its channel a
/// second time, and the exact counts below stop being a statement about
/// routing. The epoch assertion comes first and says so.
///
/// TODO(mqtt-retained-replay-shortfall-cause-unconfirmed): what produced the
/// reported shortfall of retained values on a new subscription is not
/// established; this case shows none on brenn's inbound path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_narrow_subscription_receives_every_retained_message_once() {
    broker_gate!();

    let wide = format!("{TOPIC_PREFIX}/burst/#");
    let narrow = format!("{TOPIC_PREFIX}/burst/light/+/state");
    let barrier = format!("{TOPIC_PREFIX}/burst/light/end/state");
    let wide_address = format!("mqtt:{CLIENT}:{wide}");
    let narrow_address = format!("mqtt:{CLIENT}:{narrow}");
    let retained: Vec<String> = (0..BURST)
        .map(|i| format!("{TOPIC_PREFIX}/burst/light/{i}/state"))
        .collect();

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    for (i, topic) in retained.iter().enumerate() {
        publisher
            .publish(
                topic.clone(),
                QoS::AtLeastOnce,
                true,
                format!("value-{i}").into_bytes(),
            )
            .await
            .expect("the broker took the retained publish");
        await_puback(&mut acks, "a retained publish").await;
    }

    let tree = Tree::holding(&document_dynamic_under(harness.port, &ca_file, &wide));
    let booted = boot_live(&tree, None).await;
    let (service, _) = booted.mqtt.clone();
    rows_until(
        &booted,
        &wide_address,
        BURST,
        "the wide filter's retained replay",
    )
    .await;

    let status = runtime_subscribe(&booted, READER, &narrow_address).await;
    let returned = std::time::Instant::now();
    let rows_at_return = bodies_on(&booted.messenger, &narrow_address).await.len();
    let (label, error) = service.ingress_health(CLIENT).await;
    assert_eq!(
        status,
        "subscribed",
        "the session was not live when the narrow filter was added (health {label:?} {error:?}, \
         connection epoch {})",
        connection_epoch(&service, CLIENT).await,
    );

    rows_until(
        &booted,
        &narrow_address,
        BURST,
        "the narrow filter's retained replay",
    )
    .await;
    let replay_time = returned.elapsed();
    eprintln!(
        "retained burst: {rows_at_return} of {BURST} rows in the narrow channel when the \
         subscribe returned; the {BURST}th arrived {replay_time:?} after the return",
    );

    publisher
        .publish(barrier.clone(), QoS::AtLeastOnce, false, b"end".to_vec())
        .await
        .expect("the broker took the barrier");
    await_puback(&mut acks, "the barrier's publish").await;
    let wide_bodies = rows_until(&booted, &wide_address, BURST + 1, "the barrier, wide").await;
    let narrow_bodies =
        rows_until(&booted, &narrow_address, BURST + 1, "the barrier, narrow").await;

    assert_eq!(
        connection_epoch(&service, CLIENT).await,
        1,
        "the session reconnected during the case; each reconnect replays every filter's retained \
         messages into its channel again, so the counts below are not this case's claim on this \
         run",
    );

    let mut expected: Vec<String> = retained.clone();
    expected.push(barrier.clone());
    expected.sort();
    let mut narrow_topics = topics_of(&narrow_bodies);
    narrow_topics.sort();
    assert_eq!(
        narrow_topics, expected,
        "the narrow channel holds each retained topic exactly once, and the barrier",
    );
    let mut wide_topics = topics_of(&wide_bodies);
    wide_topics.sort();
    assert_eq!(
        wide_topics, expected,
        "the wide channel holds its own replay and the barrier, and none of the copies the broker \
         sent for the narrow subscription",
    );

    assert!(
        !harness
            .log()
            .contains("Outgoing messages are being dropped"),
        "the broker dropped messages for a client, so a shortfall is the harness's limits: {}",
        harness.log(),
    );

    booted.stop_mqtt();
}

// ── a resumed session and a reload's QoS move, at the broker ───────────────

/// A message the broker queued for a filter while Brenn was stopped reaches
/// that filter's channel in the next process, whatever order that process
/// built its subscription set in.
///
/// The client keeps its session across the stop (`session_expiry_secs`), so
/// the broker queues the down-time message under the identifier the first
/// process subscribed `rs/#` with and delivers it on reconnect, before it reads
/// the second process's SUBSCRIBEs. The second process binds `rs/b` before
/// `rs/#`; a rule that numbered filters in the order a process meets them would
/// give `rs/b` the number `rs/#` had, and the message would land in the `rs/b`
/// channel and not in the `rs/#` one. The case proves the session was resumed
/// by itself: without it the down-time message never arrives.
///
/// The second process is booted the way `run_server` boots, and the case looks
/// in between its two MQTT steps: once the client directory and the egress
/// readers are in place and before the router has its state, no supervisor is
/// spawned (the fixture asserts that on every live boot) and the broker has
/// accepted no connection from the client, so the backlog it holds has not
/// started to flow. After the wiring, the backlog reaches its channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resumed_sessions_backlog_reaches_the_channel_it_was_queued_for() {
    broker_gate!();

    let wide = format!("{TOPIC_PREFIX}/rs/#");
    let narrow = format!("{TOPIC_PREFIX}/rs/b");
    let barrier = format!("{TOPIC_PREFIX}/rs/end");
    let wide_address = format!("mqtt:{CLIENT}:{wide}");
    let narrow_address = format!("mqtt:{CLIENT}:{narrow}");

    let harness = Arc::new(BrokerHarness::start());
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let block = kept_client_block(harness.port, &ca_file);

    let first_components = tempfile::tempdir().expect("a components root");
    let first_tree = Tree::holding(&document_over_a_declared_client(&block, &[&wide]));
    let first = boot_connected_on(&first_tree, first_components.path()).await;
    let (first_service, _) = first.mqtt.clone();
    brenn_mqtt::test_support::wait_for_filter_acked(
        &first_service,
        CLIENT,
        &wide,
        BROKER_WAIT_SECS,
        &format!("the first process's {wide} was never granted"),
    )
    .await;
    let since_first = harness.log_len();
    super::mqtt_egress_tests::shut_down(first).await;

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(narrow.clone(), QoS::AtLeastOnce, false, b"down".to_vec())
        .await
        .expect("the broker took the down-time publish");
    await_puback(&mut acks, "the down-time publish").await;

    let second_components = tempfile::tempdir().expect("a components root");
    let second_tree = Tree::holding(&document_over_a_declared_client(&block, &[&narrow, &wide]));
    let since_second = harness.log_len();
    let looked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let look = {
        let harness = Arc::clone(&harness);
        let looked = Arc::clone(&looked);
        let session_id = session_client_id(CLIENT);
        move || {
            let window = harness.log()[since_second..].to_string();
            assert!(
                !log_records_connect(&window, &session_id),
                "the broker accepted a connection from {session_id} before the router had its \
                 state: {window}",
            );
            looked.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    };
    let second = boot_connected_over(
        &second_tree,
        second_components.path(),
        None,
        Some(Box::new(look)),
    )
    .await;
    assert!(
        looked.load(std::sync::atomic::Ordering::SeqCst),
        "the boot never reached the place between building and wiring its MQTT sessions",
    );
    let (service, _) = second.mqtt.clone();
    for filter in [&narrow, &wide] {
        brenn_mqtt::test_support::wait_for_filter_acked(
            &service,
            CLIENT,
            filter,
            BROKER_WAIT_SECS,
            &format!("the second process's {filter} was never granted"),
        )
        .await;
    }
    publisher
        .publish(barrier.clone(), QoS::AtLeastOnce, false, b"end".to_vec())
        .await
        .expect("the broker took the barrier");
    await_puback(&mut acks, "the barrier's publish").await;
    note_if_filter_lost(&service, CLIENT, &wide).await;

    let wide_bodies = rows_until(&second, &wide_address, 2, "the resumed backlog").await;
    let session_id = session_client_id(CLIENT);
    let window = harness.log()[since_first..].to_string();
    assert!(
        brenn_mqtt::test_support::log_records_disconnect_before_reconnect(&window, &session_id),
        "the second process connected under {session_id} before the first one's DISCONNECT, so \
         the broker took the session over and the counts below are not this case's claim: \
         {window}",
    );
    assert_eq!(
        topics_of(&wide_bodies),
        vec![narrow.clone(), barrier.clone()],
        "the wide channel holds the down-time message once and the barrier once: {wide_bodies:?}",
    );
    let narrow_bodies = bodies_on(&second.messenger, &narrow_address).await;
    assert!(
        narrow_bodies.is_empty(),
        "the down-time message was queued for the wide filter only: {narrow_bodies:?}",
    );

    second.stop_mqtt();
}

/// The document of [`a_reloads_qos_move_sends_no_unsubscribe`]: the consumer
/// bound to `topic`, on a client at `qos`.
fn bound_at_qos(port: u16, ca_file: &std::path::Path, topic: &str, qos: u8) -> String {
    document_over_a_declared_client(&client_block_at(port, ca_file, qos), &[topic])
}

/// A reload that moves a held filter's QoS sends the broker a SUBSCRIBE at the
/// new QoS and no UNSUBSCRIBE.
///
/// A static binding's QoS is its client's, so the move is an edit to the
/// client's `qos`, which also restarts the client. The reload's outgoing step,
/// where a remove-and-add would withdraw the filter, runs before the restart,
/// on the predecessor's connection; the broker logs an UNSUBSCRIBE sent there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reloads_qos_move_sends_no_unsubscribe() {
    broker_gate!();

    let topic = format!("{TOPIC_PREFIX}/qos-move");
    let address = format!("mqtt:{CLIENT}:{topic}");

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");

    let tree = Tree::holding(&bound_at_qos(harness.port, &ca_file, &topic, 0));
    let mut booted = boot_connected_on(&tree, components.path()).await;
    let (service, _router) = booted.mqtt.clone();
    assert_eq!(service.ingress_filter_qos(CLIENT, &topic).await, Some(0));
    brenn_mqtt::test_support::wait_for_filter_acked(
        &service,
        CLIENT,
        &topic,
        BROKER_WAIT_SECS,
        &format!("the broker never granted {topic}"),
    )
    .await;
    let session_id = session_client_id(CLIENT);
    assert!(
        log_records_subscribe_at(&harness.log(), &session_id, &topic, 0),
        "boot subscribed {topic} at the client's qos 0: {}",
        harness.log(),
    );

    let before_reload = harness.log_len();
    tree.write(&bound_at_qos(harness.port, &ca_file, &topic, 1));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec![CLIENT.to_string()]);
    assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
    assert!(
        status.delta.mqtt_unsubscribed.is_empty(),
        "a qos move is not a withdrawal: {:?}",
        status.delta.mqtt_unsubscribed,
    );
    assert_no_failed_filters(&status);

    brenn_mqtt::test_support::wait_for_filter_acked(
        &service,
        CLIENT,
        &topic,
        BROKER_WAIT_SECS,
        &format!("the successor's {topic} was never granted at the new qos"),
    )
    .await;
    assert_eq!(service.ingress_filter_qos(CLIENT, &topic).await, Some(1));
    harness
        .wait_for_log(
            before_reload,
            BROKER_WAIT_SECS,
            &format!(
                "the broker never recorded a SUBSCRIBE for {topic} at qos 1 from {session_id}"
            ),
            |log| log_records_subscribe_at(log, &session_id, &topic, 1),
        )
        .await;

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(topic.clone(), QoS::AtLeastOnce, false, b"moved".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the moved topic's publish").await;
    let window = harness
        .wait_for_log(
            before_reload,
            BROKER_WAIT_SECS,
            &format!("the broker never recorded sending {topic} to {session_id}"),
            |log| log_records_publish_to_subscriber(log, &session_id, &topic),
        )
        .await;
    assert!(
        !log_records_unsubscribe(&window, &session_id, &topic),
        "the reload's qos move sent an UNSUBSCRIBE for {topic}: {window}",
    );

    let bodies = booted.bodies_until(&address, 1).await;
    assert_eq!(
        topics_of(&bodies),
        vec![topic.clone()],
        "the channel holds the message once: {bodies:?}",
    );
    assert_eq!(
        qos_of(&bodies[0]),
        1,
        "delivered at the new qos: {bodies:?}"
    );

    booted.stop_mqtt();
}

/// The QoS the broker delivered an ingress envelope's message at.
fn qos_of(body: &str) -> u64 {
    let envelope: serde_json::Value =
        serde_json::from_str(body).expect("the ingress envelope is JSON");
    envelope["qos"]
        .as_u64()
        .expect("the envelope names its delivered qos")
}

/// The broker declaration at `qos` with two reader agents, each granted what a
/// runtime `mqtt:` subscribe needs — `dynamic_subscribe` and a `topic_filter`
/// clause covering `covered` — and no binding or static subscription on any
/// `mqtt:` channel.
fn document_dynamic_only(
    port: u16,
    ca_file: &std::path::Path,
    covered: &str,
    qos: u8,
    second: &str,
) -> String {
    document_with_agent(
        &client_block_at(port, ca_file, qos),
        &dynamic_reader_agent(covered, "", &[READER, second]),
    )
}

/// A reload that edits the client's `qos` moves a filter held only by a
/// runtime subscription, at the broker: a SUBSCRIBE at the new QoS and no
/// UNSUBSCRIBE, and a message published at QoS 1 is delivered at QoS 0 before
/// and at QoS 1 after. A second app subscribing the same address afterwards
/// sends the filter at the new QoS too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reloads_qos_move_resubscribes_a_dynamic_only_filter() {
    broker_gate!();

    const SECOND: &str = "second-reader";
    let topic = format!("{TOPIC_PREFIX}/qos-dyn");
    let address = format!("mqtt:{CLIENT}:{topic}");
    let session_id = session_client_id(CLIENT);

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");
    let at = |qos| document_dynamic_only(harness.port, &ca_file, &address, qos, SECOND);

    let tree = Tree::holding(&at(0));
    let mut booted = boot_connected_on(&tree, components.path()).await;
    let (service, _) = booted.mqtt.clone();
    let status = runtime_subscribe(&booted, READER, &address).await;
    assert_eq!(
        status, "subscribed",
        "the session was live for the runtime subscribe"
    );
    brenn_mqtt::test_support::wait_for_filter_acked(
        &service,
        CLIENT,
        &topic,
        BROKER_WAIT_SECS,
        &format!("the broker never granted {topic}"),
    )
    .await;
    assert!(
        log_records_subscribe_at(&harness.log(), &session_id, &topic, 0),
        "the runtime subscribe sent {topic} at the client's qos 0: {}",
        harness.log(),
    );

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    publisher
        .publish(topic.clone(), QoS::AtLeastOnce, false, b"before".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the publish before the reload").await;
    let before = rows_until(&booted, &address, 1, "the message before the reload").await;
    assert_eq!(
        qos_of(&before[0]),
        0,
        "delivered at the old qos: {before:?}"
    );

    let before_reload = harness.log_len();
    tree.write(&at(1));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec![CLIENT.to_string()]);
    assert_eq!(status.delta.mqtt_subscribed, vec![address.clone()]);
    assert!(
        status.delta.mqtt_unsubscribed.is_empty(),
        "a qos move is not a withdrawal: {:?}",
        status.delta.mqtt_unsubscribed,
    );
    assert_no_failed_filters(&status);

    brenn_mqtt::test_support::wait_for_filter_acked(
        &service,
        CLIENT,
        &topic,
        BROKER_WAIT_SECS,
        &format!("the successor's {topic} was never granted"),
    )
    .await;
    let window = harness
        .wait_for_log(
            before_reload,
            BROKER_WAIT_SECS,
            &format!(
                "the broker never recorded a SUBSCRIBE for {topic} at qos 1 from {session_id}"
            ),
            |log| log_records_subscribe_at(log, &session_id, &topic, 1),
        )
        .await;
    assert!(
        !log_records_unsubscribe(&window, &session_id, &topic),
        "the reload's qos move sent an UNSUBSCRIBE for {topic}: {window}",
    );

    publisher
        .publish(topic.clone(), QoS::AtLeastOnce, false, b"after".to_vec())
        .await
        .expect("the broker took the publish");
    await_puback(&mut acks, "the publish after the reload").await;
    note_if_filter_lost(&service, CLIENT, &topic).await;
    let after = rows_until(&booted, &address, 2, "the message after the reload").await;
    assert_eq!(qos_of(&after[1]), 1, "delivered at the new qos: {after:?}");

    let before_second = harness.log_len();
    let status = runtime_subscribe(&booted, SECOND, &address).await;
    assert_eq!(
        status, "subscribed",
        "the successor was live for the second app"
    );
    let window = harness
        .wait_for_log(
            before_second,
            BROKER_WAIT_SECS,
            &format!("the second app's subscribe sent no SUBSCRIBE for {topic} at qos 1"),
            |log| log_records_subscribe_at(log, &session_id, &topic, 1),
        )
        .await;
    assert!(
        !log_records_subscribe_at(&window, &session_id, &topic, 0),
        "a SUBSCRIBE after the qos edit went out at qos 0: {window}",
    );

    booted.stop_mqtt();
}

// ── a kept session's retained replay and withdrawals, at the broker ────────

/// The payload text of each ingress envelope in `bodies` on `topic`, in order.
fn texts_on(bodies: &[String], topic: &str) -> Vec<String> {
    bodies
        .iter()
        .filter_map(|body| {
            let envelope: serde_json::Value =
                serde_json::from_str(body).expect("the ingress envelope is JSON");
            (envelope["topic"] == topic).then(|| {
                envelope["payload"]["text"]
                    .as_str()
                    .expect("the envelope carries a text payload")
                    .to_string()
            })
        })
        .collect()
}

/// Publish a live `end` on `barrier`, and poll until `address` holds a row
/// on that topic carrying it; answer with every row the channel then holds.
///
/// A barrier published after the broker has logged sending every message the
/// case waits on is written to the session behind them, and one connection's
/// deliveries are routed in the order they arrive, so once the barrier's row
/// is in the channel every message before it has been compared and stored or
/// skipped.
async fn rows_through_barrier(
    booted: &Booted,
    publisher: &rumqttc::AsyncClient,
    acks: &mut tokio::sync::mpsc::UnboundedReceiver<()>,
    address: &str,
    barrier: &str,
) -> Vec<String> {
    publisher
        .publish(
            barrier.to_string(),
            QoS::AtLeastOnce,
            false,
            b"end".to_vec(),
        )
        .await
        .expect("the broker took the barrier");
    await_puback(acks, "the barrier's publish").await;
    let messenger = &booted.messenger;
    brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async {
            let bodies = bodies_on(messenger, address).await;
            texts_on(&bodies, barrier)
                .iter()
                .any(|text| text == "end")
                .then_some(bodies)
        },
        || async {
            let bodies = bodies_on(messenger, address).await;
            format!("the barrier on {barrier} never reached {address}: {bodies:?}")
        },
    )
    .await
}

/// A process that resumes a session the broker kept brings its channel up to
/// the broker's retained values, and adds no copy of one it already holds.
///
/// Three retained topics under one filter, held by a first process; then, while
/// no process is connected, one topic changes at QoS 0, which the session does
/// not queue, one changes at QoS 1, which it does, and one does not change. A
/// second process boots over the first one's store, resumes the session, and
/// subscribes again, so the broker replays all three.
///
/// - The QoS 0 change reaches the channel through the replay alone: its newest
///   row is the new value.
/// - The unchanged topic is replayed — the broker's log shows the SUBSCRIBE and
///   the PUBLISH — and the channel holds its value once: the row is absent
///   because it was compared, not because it was never sent.
/// - The QoS 1 change arrives twice, queued and replayed, in an order the
///   broker does not promise: its newest row is the new value, held once or
///   twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resumed_session_brings_the_brokers_retained_values_and_no_copy() {
    broker_gate!();

    let wide = format!("{TOPIC_PREFIX}/kr/#");
    let changed = format!("{TOPIC_PREFIX}/kr/changed");
    let same = format!("{TOPIC_PREFIX}/kr/same");
    let queued = format!("{TOPIC_PREFIX}/kr/queued");
    let barrier = format!("{TOPIC_PREFIX}/kr/end");
    // Outside the filter: a QoS 1 publish behind the QoS 0 one, whose PUBACK
    // says the broker has taken the QoS 0 one too.
    let sync = format!("{TOPIC_PREFIX}/kr-sync");
    let address = format!("mqtt:{CLIENT}:{wide}");
    let session_id = session_client_id(CLIENT);

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let tree = Tree::holding(&document_over_a_declared_client(
        &kept_client_block(harness.port, &ca_file),
        &[&wide],
    ));

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    for (topic, value) in [(&changed, "v1"), (&same, "s"), (&queued, "q1")] {
        publisher
            .publish(
                topic.clone(),
                QoS::AtLeastOnce,
                true,
                value.as_bytes().to_vec(),
            )
            .await
            .expect("the broker took the retained publish");
        await_puback(&mut acks, "a retained publish").await;
    }

    let db = brenn_server::test_support::init_db_memory();
    let first_components = tempfile::tempdir().expect("a components root");
    let first = boot_connected_over(&tree, first_components.path(), Some(db.clone()), None).await;
    rows_until(&first, &address, 3, "the first process's replay").await;
    let since_first = harness.log_len();
    super::mqtt_egress_tests::shut_down(first).await;

    publisher
        .publish(changed.clone(), QoS::AtMostOnce, true, b"v2".to_vec())
        .await
        .expect("the broker took the QoS 0 retained publish");
    publisher
        .publish(sync.clone(), QoS::AtLeastOnce, false, b"sync".to_vec())
        .await
        .expect("the broker took the sync publish");
    await_puback(&mut acks, "the publish behind the QoS 0 one").await;
    publisher
        .publish(queued.clone(), QoS::AtLeastOnce, true, b"q2".to_vec())
        .await
        .expect("the broker took the QoS 1 retained publish");
    await_puback(&mut acks, "the queued retained publish").await;

    let since_second = harness.log_len();
    let second_components = tempfile::tempdir().expect("a components root");
    let second = boot_connected_over(&tree, second_components.path(), Some(db), None).await;
    let window = harness
        .wait_for_log(
            since_second,
            BROKER_WAIT_SECS,
            "the second process never resumed the session, subscribed and had every retained \
             topic replayed",
            |log| {
                log_records_connack(log, &session_id, true)
                    && log_records_subscribe_at(log, &session_id, &wide, 1)
                    && [&changed, &same, &queued]
                        .iter()
                        .all(|topic| log_records_publish_to_subscriber(log, &session_id, topic))
            },
        )
        .await;
    assert!(
        brenn_mqtt::test_support::log_records_disconnect_before_reconnect(
            &harness.log()[since_first..],
            &session_id
        ),
        "the second process connected under {session_id} before the first one's DISCONNECT, so \
         the broker took the session over: {window}",
    );

    let bodies = rows_through_barrier(&second, &publisher, &mut acks, &address, &barrier).await;
    let changed_texts = texts_on(&bodies, &changed);
    assert_eq!(
        changed_texts.last().map(String::as_str),
        Some("v2"),
        "the value changed at QoS 0 reached the channel through the replay: {bodies:?}",
    );
    assert_eq!(
        texts_on(&bodies, &same),
        vec!["s".to_string()],
        "the unchanged value was replayed and is held once: {bodies:?}",
    );
    let queued_texts = texts_on(&bodies, &queued);
    assert_eq!(
        queued_texts.last().map(String::as_str),
        Some("q2"),
        "the value changed at QoS 1 is the newest row for its topic: {bodies:?}",
    );
    let new_copies = queued_texts.iter().filter(|text| *text == "q2").count();
    assert!(
        (1..=2).contains(&new_copies),
        "the queued value and its replay leave it once or twice, never more: {bodies:?}",
    );

    second.stop_mqtt();
}

/// The broker declaration with the session kept, and two reader agents each
/// granted a runtime `mqtt:` subscribe on `covered`, holding nothing static.
fn kept_dynamic_only(port: u16, ca_file: &std::path::Path, covered: &str, second: &str) -> String {
    document_with_agent(
        &kept_client_block(port, ca_file),
        &dynamic_reader_agent(covered, "", &[READER, second]),
    )
}

/// A filter added to a kept session takes its retained messages once, and a
/// second subscriber joining the filter — a second SUBSCRIBE, and a second
/// replay at the broker — adds no row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_subscriber_to_a_held_filter_adds_no_replayed_row() {
    broker_gate!();

    const SECOND: &str = "second-reader";
    let filter = format!("{TOPIC_PREFIX}/kd/+");
    let barrier = format!("{TOPIC_PREFIX}/kd/end");
    let retained: Vec<String> = ["a", "b", "c"]
        .iter()
        .map(|leaf| format!("{TOPIC_PREFIX}/kd/{leaf}"))
        .collect();
    let address = format!("mqtt:{CLIENT}:{filter}");
    let session_id = session_client_id(CLIENT);

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    for topic in &retained {
        publisher
            .publish(topic.clone(), QoS::AtLeastOnce, true, b"held".to_vec())
            .await
            .expect("the broker took the retained publish");
        await_puback(&mut acks, "a retained publish").await;
    }

    let tree = Tree::holding(&kept_dynamic_only(harness.port, &ca_file, &address, SECOND));
    let booted = boot_connected_on(&tree, components.path()).await;
    assert_eq!(
        runtime_subscribe(&booted, READER, &address).await,
        "subscribed",
        "the session was live for the first subscriber",
    );
    let first = rows_until(&booted, &address, retained.len(), "the first replay").await;
    let mut first_topics = topics_of(&first);
    first_topics.sort();
    assert_eq!(
        first_topics, retained,
        "each retained value once: {first:?}"
    );

    let before_second = harness.log_len();
    assert_eq!(
        runtime_subscribe(&booted, SECOND, &address).await,
        "subscribed",
        "the session was live for the second subscriber",
    );
    harness
        .wait_for_log(
            before_second,
            BROKER_WAIT_SECS,
            &format!("the second subscriber's SUBSCRIBE for {filter} was never replayed to"),
            |log| {
                log_records_subscribe_at(log, &session_id, &filter, 1)
                    && retained
                        .iter()
                        .all(|topic| log_records_publish_to_subscriber(log, &session_id, topic))
            },
        )
        .await;

    let bodies = rows_through_barrier(&booted, &publisher, &mut acks, &address, &barrier).await;
    let mut topics = topics_of(&bodies);
    topics.sort();
    let mut expected = retained.clone();
    expected.push(barrier.clone());
    expected.sort();
    assert_eq!(
        topics, expected,
        "the second replay added no row, and the barrier is the only new one: {bodies:?}",
    );

    booted.stop_mqtt();
}

/// A client whose session expiry is 0, restarted by a reload that changes its
/// declaration: the successor's CONNACK reports no session, every filter is
/// subscribed again, the broker replays each retained value, and each is in
/// its channel once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconnect_without_the_session_leaves_each_retained_value_once() {
    broker_gate!();

    let first = format!("{TOPIC_PREFIX}/ns/a");
    let second = format!("{TOPIC_PREFIX}/ns/b");
    let session_id = session_client_id(CLIENT);

    let harness = BrokerHarness::start();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");
    let block = client_block(harness.port, &ca_file);
    let changed_block = block.replace("qos = 1;", "qos = 1;\n    stop_wait_secs = 5;");
    assert_ne!(block, changed_block, "the reload changes the client");

    let (publisher, mut acks) = direct_publisher_acked(harness.port, certs::ca_pem_bytes()).await;
    for topic in [&first, &second] {
        publisher
            .publish(topic.clone(), QoS::AtLeastOnce, true, b"held".to_vec())
            .await
            .expect("the broker took the retained publish");
        await_puback(&mut acks, "a retained publish").await;
    }

    let tree = Tree::holding(&document_over_a_declared_client(&block, &[&first, &second]));
    let mut booted = boot_connected_on(&tree, components.path()).await;
    for topic in [&first, &second] {
        rows_until(
            &booted,
            &format!("mqtt:{CLIENT}:{topic}"),
            1,
            "the boot's replay",
        )
        .await;
    }

    let before_reload = harness.log_len();
    tree.write(&document_over_a_declared_client(
        &changed_block,
        &[&first, &second],
    ));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec![CLIENT.to_string()]);

    harness
        .wait_for_log(
            before_reload,
            BROKER_WAIT_SECS,
            "the successor never connected without a session, subscribed both filters and had \
             both retained values replayed",
            |log| {
                log_records_connack(log, &session_id, false)
                    && [&first, &second].iter().all(|topic| {
                        log_records_subscribe_at(log, &session_id, topic, 1)
                            && log_records_publish_to_subscriber(log, &session_id, topic)
                    })
            },
        )
        .await;
    let window = harness.log()[before_reload..].to_string();
    assert!(
        !log_records_connack(&window, &session_id, true),
        "a connection after the reload found a session at expiry 0: {window}",
    );

    for topic in [&first, &second] {
        let address = format!("mqtt:{CLIENT}:{topic}");
        let bodies = rows_through_barrier(&booted, &publisher, &mut acks, &address, topic).await;
        assert_eq!(
            texts_on(&bodies, topic),
            vec!["held".to_string(), "end".to_string()],
            "the retained value once, then the barrier: {bodies:?}",
        );
    }

    booted.stop_mqtt();
}

/// A reload that withdraws a filter while the client's broker is down reports
/// the withdrawal as deferred — in `mqtt_unsubscribe_deferred`, not in
/// `mqtt_deferred` — and the UNSUBSCRIBE reaches the broker on the connection
/// that resumes the session once the broker is back.
///
/// The broker keeps its sessions across the stop (persistence on, an orderly
/// shutdown), which is what a broker restarted under a kept session looks like
/// to a client: without it the session would be gone and there would be nothing
/// to withdraw.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_withdrawal_deferred_while_the_broker_is_down_is_sent_when_it_returns() {
    broker_gate!();

    let kept = format!("{TOPIC_PREFIX}/dw/kept");
    let dropped = format!("{TOPIC_PREFIX}/dw/dropped");
    let dropped_address = format!("mqtt:{CLIENT}:{dropped}");
    let session_id = session_client_id(CLIENT);

    let mut harness = BrokerHarness::start_persistent();
    let ca_dir = tempfile::tempdir().expect("a directory for the CA");
    let ca_file = ca_dir.path().join("ca.pem");
    std::fs::write(&ca_file, certs::ca_pem()).expect("the CA is writable");
    let components = tempfile::tempdir().expect("a components root");
    let block = kept_client_block(harness.port, &ca_file);

    let tree = Tree::holding(&document_over_a_declared_client(&block, &[&kept, &dropped]));
    let mut booted = boot_connected_on(&tree, components.path()).await;
    let (service, _) = booted.mqtt.clone();
    for filter in [&kept, &dropped] {
        brenn_mqtt::test_support::wait_for_filter_acked(
            &service,
            CLIENT,
            filter,
            BROKER_WAIT_SECS,
            &format!("the broker never granted {filter}"),
        )
        .await;
    }

    harness.stop_keeping_sessions();
    brenn_mqtt::test_support::wait_for_health(
        &service,
        CLIENT,
        &[ConnectorHealthLabel::Disconnected],
        BROKER_WAIT_SECS,
        "the client never saw its broker go",
    )
    .await;

    tree.write(&document_over_a_declared_client(&block, &[&kept]));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(
        status.delta.mqtt_unsubscribed,
        vec![dropped_address.clone()]
    );
    assert_eq!(
        status.delta.mqtt_unsubscribe_deferred,
        vec![dropped_address.clone()],
        "the withdrawal made with no connection is reported deferred",
    );
    assert!(
        status.delta.mqtt_deferred.is_empty(),
        "a deferred withdrawal is not a deferred assertion: {:?}",
        status.delta.mqtt_deferred,
    );
    assert_no_failed_filters(&status);

    let before_restart = harness.log_len();
    harness.restart();
    harness
        .wait_for_log(
            before_restart,
            3 * BROKER_WAIT_SECS,
            &format!(
                "the client never resumed its session and withdrew {dropped} once the broker was \
                 back"
            ),
            |log| {
                log_records_connack(log, &session_id, true)
                    && log_records_unsubscribe(log, &session_id, &dropped)
            },
        )
        .await;

    booted.stop_mqtt();
}

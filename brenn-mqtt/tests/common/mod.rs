// Shared harness for the MQTT integration suite.
//
// The broker itself, the throwaway TLS material and the raw publisher live in
// `brenn_mqtt::test_support`, which the crates above this one use too. What
// stays here is what only this suite needs: the capturing router, the TCP relay
// that drops a connection mid-session, and the `spawn_client` family built on
// them.
//
// All public items are re-exported at module root so test functions can
// `use common::*;` without multi-level path disambiguation.
//
// NOTE: the retained-message suite's `assert_eq!(retained.len(), 100)` is an
// exact count; it relies on each test starting with a fresh broker
// (persistence=false eliminates bleed). If the harness is ever refactored to a
// shared broker, verify that assertion still holds across test ordering.

pub mod relay;
pub mod router;

pub use brenn_mqtt::test_support::broker::{
    BrokerHarness, DEFAULT_ACL, log_records_publish_to_subscriber, log_records_unsubscribe,
};
pub use brenn_mqtt::test_support::certs;
pub use brenn_mqtt::test_support::client::{
    await_puback, direct_publisher_acked, drain_until_incoming, session_client_id, wait_for_health,
};
pub use brenn_mqtt::test_support::poll::poll_until;
pub use relay::TcpRelay;
pub use router::{CapturingRouter, DeliveredMessage};

use std::sync::Arc;

use brenn_lib::messaging::Urgency;
use brenn_lib::mqtt::config::{MqttClientConfig, MqttClientIdentity, TlsVersionMin};
use brenn_mqtt::service::IngressSubscribeOutcome;
use brenn_mqtt::state::{ConnectorHealthLabel, IngressSubscription, MqttClientHandle};
use brenn_mqtt::{InboundPayload, MqttEventRouter, MqttService, spawn_client_supervisor};
use rumqttc::{AsyncClient, MqttOptions, Transport};
use tokio::sync::mpsc;

/// Returns the absolute path to the `mqtt_assets` directory shipped alongside
/// this test crate. Asset files are resolved relative to `CARGO_MANIFEST_DIR`,
/// which the test target pins to this package and which resolves from the
/// runfiles root the test starts in.
pub fn mqtt_assets_dir() -> std::path::PathBuf {
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.join("tests").join("mqtt_assets")
}

fn conf_template(name: &str) -> String {
    std::fs::read_to_string(mqtt_assets_dir().join(name))
        .unwrap_or_else(|e| panic!("failed to read {name}: {e}"))
}

/// Spawn a broker that accepts TLS 1.3 connections only.
pub fn broker_tls13() -> BrokerHarness {
    BrokerHarness::start_with(
        &conf_template("mosquitto.conf.tls13.tmpl"),
        &[("acl", DEFAULT_ACL.as_bytes())],
    )
}

/// Spawn a broker that requires username/password authentication
/// (`allow_anonymous false` + a `password_file`). The checked-in `passwd` asset
/// holds one user (`brenn-itest` / `brenn-itest-password`).
pub fn broker_auth() -> BrokerHarness {
    let passwd = std::fs::read(mqtt_assets_dir().join("passwd")).expect("failed to read passwd");
    BrokerHarness::start_with(
        &conf_template("mosquitto.conf.auth.tmpl"),
        &[("acl", DEFAULT_ACL.as_bytes()), ("passwd", &passwd)],
    )
}

// ---------------------------------------------------------------------------
// spawn_client helper: the unified per-client session harness
// ---------------------------------------------------------------------------

/// Handle set returned by the unified spawn helpers: the service, the client
/// slug, the session handle (for `publish_on_handle` / stop / subscription-set
/// asserts), and the receiver every inbound delivery lands on.
pub struct SpawnedClient {
    pub svc: Arc<MqttService>,
    pub client_slug: String,
    pub handle: Arc<MqttClientHandle>,
    pub rx: mpsc::UnboundedReceiver<DeliveredMessage>,
}

/// Build `MqttService` + `MqttClientHandle` + the unified supervisor for
/// `test_name`, wiring a `CapturingRouter` for inbound deliveries. `static_subs`
/// are the client's union subscription set (assigned 1-based `sub_id`s in order),
/// re-asserted by the supervisor on connect.
///
/// Returns only after the session reaches `Connected` (poll every 25ms, cap 5s).
/// Connected ≠ subscriptions live — callers confirm liveness via
/// [`subscribe_live_confirmed`]'s retained barrier (or, for static subs, by
/// draining the retained payload the initial re-assert loop delivers).
pub async fn spawn_client(
    test_name: &str,
    broker: &BrokerHarness,
    ca_pem: Vec<u8>,
    static_subs: Vec<(String, u8)>,
) -> SpawnedClient {
    spawn_client_with_tls_version(test_name, broker, ca_pem, static_subs, TlsVersionMin::Tls12)
        .await
}

/// Like [`spawn_client`] but negotiates TLS 1.3 only (`tls_version_min = Tls13`).
pub async fn spawn_client_tls13(
    test_name: &str,
    broker: &BrokerHarness,
    ca_pem: Vec<u8>,
    static_subs: Vec<(String, u8)>,
) -> SpawnedClient {
    spawn_client_with_tls_version(test_name, broker, ca_pem, static_subs, TlsVersionMin::Tls13)
        .await
}

/// The `MqttClientConfig` the harness spawner uses, returned as a bare struct so
/// the caller can mutate the fields a given test needs to vary (bad credentials,
/// etc.) before wrapping it in `Arc`. `port` is whatever the client should dial —
/// the broker directly, or a [`TcpRelay`] port in front of it.
pub fn test_client_config(
    client_slug: &str,
    port: u16,
    ca_pem: Vec<u8>,
    tls_version_min: TlsVersionMin,
) -> MqttClientConfig {
    MqttClientConfig {
        identity: MqttClientIdentity {
            slug: client_slug.to_string(),
            host: BrokerHarness::HOST.to_string(),
            port,
            username: None,
            tls_version_min,
            keepalive_secs: Some(30),
            inbound_payload_cap_bytes: 4 * 1024 * 1024,
            last_will: None,
            reconnect_backoff_initial_secs: 1,
            reconnect_backoff_max_secs: 60,
            qos: 1,
            urgency: Urgency::Normal,
            session_expiry_secs: 0,
        },
        password: None,
        ca_cert_pem: Some(ca_pem),
    }
}

/// Poll `probe` until it reports `Connected`, capping at 5s and panicking with
/// `msg` on timeout. Shared by both harness spawners.
async fn wait_until_connected<F, Fut>(probe: F, msg: &str)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = ConnectorHealthLabel>,
{
    let probe = &probe;
    poll_until(
        5,
        || async move { (probe().await == ConnectorHealthLabel::Connected).then_some(()) },
        || async move { msg.to_string() },
    )
    .await
}

/// Await one delivery on `rx` (3s cap). Panics on a closed channel or timeout,
/// naming `what`. The strict 3s cap is the ingress suite's shared convention.
pub async fn recv_delivery(
    rx: &mut mpsc::UnboundedReceiver<DeliveredMessage>,
    what: &str,
) -> DeliveredMessage {
    match tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv()).await {
        Ok(Some(msg)) => msg,
        Ok(None) => panic!("{what}: router receiver closed"),
        Err(_) => panic!("{what}: not delivered within 3s"),
    }
}

async fn spawn_client_with_tls_version(
    test_name: &str,
    broker: &BrokerHarness,
    ca_pem: Vec<u8>,
    static_subs: Vec<(String, u8)>,
    tls_version_min: TlsVersionMin,
) -> SpawnedClient {
    let client_slug = format!("testbroker-{test_name}");
    let config = Arc::new(test_client_config(
        &client_slug,
        broker.port,
        ca_pem,
        tls_version_min,
    ));
    let spawned = spawn_client_with_config(config, static_subs).await;

    wait_until_connected(
        || async { spawned.svc.ingress_health(&spawned.client_slug).await.0 },
        "spawn_client: session never reached Connected within 5s",
    )
    .await;

    spawned
}

/// Build `MqttService` + `MqttClientHandle` + the unified supervisor for a
/// caller-supplied `config`, wiring a `CapturingRouter`. Unlike [`spawn_client`],
/// this does **not** wait for `Connected` — tests that expect a session to *never*
/// connect (bad credentials) or that drive their own connection lifecycle call
/// this directly and use [`wait_for_health`] for whatever state they expect. The
/// client slug is taken from `config.slug`.
pub async fn spawn_client_with_config(
    config: Arc<MqttClientConfig>,
    static_subs: Vec<(String, u8)>,
) -> SpawnedClient {
    let client_slug = config.identity.slug.clone();

    let subs: Vec<IngressSubscription> = static_subs
        .into_iter()
        .enumerate()
        .map(|(i, (topic_filter, qos))| IngressSubscription {
            topic_filter,
            qos,
            sub_id: (i + 1) as u32,
        })
        .collect();

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let handle = MqttClientHandle::new(config, subs, stop_tx);

    let svc = MqttService::new();
    svc.add_client(handle.clone()).await;

    let (router, rx) = CapturingRouter::new();
    let router_arc: Arc<dyn MqttEventRouter> = Arc::new(router);
    svc.set_router(router_arc.clone()).await;

    spawn_client_supervisor(handle.clone(), router_arc, stop_rx);

    SpawnedClient {
        svc,
        client_slug,
        handle,
        rx,
    }
}

fn uuid_v4_simple() -> String {
    // simple() produces the 32-hex-digit dashless form without an intermediate allocation.
    uuid::Uuid::new_v4().simple().to_string()
}

// ---------------------------------------------------------------------------
// direct_subscriber: a raw rumqttc client that witnesses the first delivery
// ---------------------------------------------------------------------------

/// Subscribe a direct rumqttc v5 client to `topic` and return a receiver that
/// yields the payload of the first `Publish` delivered on it.
///
/// The subscription is registered (SubAck received) before this function
/// returns, so a publish issued by the caller afterward cannot race ahead of it.
/// Use this to witness delivery independently of the brenn session, on a separate
/// client id.
///
/// If the eventloop errors before a `Publish` arrives (e.g. broker/TLS drop),
/// the error is printed to stderr — captured and shown by the test harness on
/// failure — so a delivery-timeout can be told apart from a transport fault
/// rather than surfacing only as a bare `recv` timeout.
///
/// # TEST-ONLY TLS note
/// Uses `Transport::tls(ca_pem, None, None)` with an IP-literal host. Acceptable
/// for loopback test brokers; MUST NOT be copied into production connection code.
pub async fn direct_subscriber(
    broker_port: u16,
    ca_pem: Vec<u8>,
    topic: &str,
) -> mpsc::Receiver<Vec<u8>> {
    let client_id = format!("brenn-direct-sub-{}", uuid_v4_simple());
    let mut opts = MqttOptions::new(client_id, ("127.0.0.1", broker_port));
    opts.set_clean_start(true);
    opts.set_transport(Transport::tls(ca_pem, None, None));
    let (client, mut eventloop) = AsyncClient::builder(opts).capacity(16).build();

    client
        .subscribe(topic, rumqttc::mqttbytes::QoS::AtLeastOnce)
        .await
        .expect("direct_subscriber: subscribe failed");

    // Drain the eventloop until SubAck so the subscription is registered before
    // the caller publishes, then hand off to a task collecting the first Publish.
    drain_until_incoming(
        &mut eventloop,
        |pkt| matches!(pkt, rumqttc::Incoming::SubAck(_)),
        "direct_subscriber: SubAck",
    )
    .await;

    let (deliver_tx, deliver_rx) = mpsc::channel::<Vec<u8>>(1);
    tokio::spawn(async move {
        // Keep the client alive so the eventloop keeps servicing the connection.
        let _client = client;
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Incoming::Publish(p))) => {
                    deliver_tx.send(p.payload.to_vec()).await.ok();
                    return; // one publish is all we need
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("direct_subscriber: eventloop error before Publish: {e}");
                    return;
                }
            }
        }
    });

    deliver_rx
}

// ---------------------------------------------------------------------------
// subscribe_live_confirmed: the retained barrier
// ---------------------------------------------------------------------------

/// Subscribe `client_slug` to `topic` and return only once the subscription is
/// proven live at the broker.
///
/// `subscribe_filter` returns as soon as the SUBSCRIBE is *queued* (no SUBACK
/// wait), so a publish issued right after could beat the SUBSCRIBE to the broker.
/// This closes the race with a retained barrier: publish a retained sentinel on
/// `topic` (QoS 1, PubAck-confirmed so it is stored before the SUBSCRIBE), then
/// subscribe. `OnEverySubscribe` makes the broker redeliver the retained sentinel
/// when it processes the SUBSCRIBE; its arrival at the router proves the
/// subscription is live and SUBACKed.
///
/// Drains `rx` until the barrier is delivered on `topic` (cap 3s). Any other
/// delivery panics — there is no legitimate source of noise in these tests.
/// Returns the `IngressSubscribeOutcome` for the caller to assert.
pub async fn subscribe_live_confirmed(
    svc: &MqttService,
    client_slug: &str,
    topic: &str,
    pub_client: &AsyncClient,
    ack_rx: &mut mpsc::UnboundedReceiver<()>,
    rx: &mut mpsc::UnboundedReceiver<DeliveredMessage>,
) -> IngressSubscribeOutcome {
    let barrier = format!("__barrier__{}_{topic}", uuid_v4_simple());

    // The barrier handshake relies on the next PubAck being *this* barrier's. A stale
    // ack left by an earlier un-awaited QoS-1 publish would let the SUBSCRIBE proceed
    // before the retained message is stored, silently reopening the race. Fail loudly
    // if the caller left the ack channel dirty.
    assert!(
        matches!(ack_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "subscribe_live_confirmed: PubAck channel not drained — await one ack per prior \
         QoS-1 publish before calling this helper"
    );

    // Retained barrier publish, PubAck-confirmed: stored before the SUBSCRIBE.
    pub_client
        .publish(
            topic.to_string(),
            rumqttc::mqttbytes::QoS::AtLeastOnce,
            true,
            barrier.as_bytes().to_vec(),
        )
        .await
        .expect("subscribe_live_confirmed: barrier publish failed");
    await_puback(ack_rx, "subscribe_live_confirmed: barrier").await;

    let outcome = svc
        .subscribe_filter(client_slug, topic.to_string(), 1)
        .await
        .expect("subscribe_live_confirmed: no ingress supervisor for client");

    // The barrier's redelivery via OnEverySubscribe proves the SUBSCRIBE is live.
    // It is the first (and only expected) delivery on `topic` — any other delivery
    // is noise, which has no legitimate source in these tests.
    let msg = recv_delivery(
        rx,
        &format!("subscribe_live_confirmed: barrier for {topic:?}"),
    )
    .await;
    assert!(
        msg.topic == topic
            && msg.client == client_slug
            && matches!(&msg.payload, InboundPayload::Text(t) if *t == barrier),
        "subscribe_live_confirmed: expected the barrier on {topic:?} for client \
         {client_slug:?}, got a delivery on {:?} for {:?}: {:?}",
        msg.topic,
        msg.client,
        msg.payload
    );

    outcome
}

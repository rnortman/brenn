// A kept MQTT session across a lost connection, through the real supervisor
// and the real egress reader.
//
// Each case runs one client against its own mosquitto through a
// `PacketRelay`, with a session expiry above zero so the broker keeps the
// session across the loss, stages one loss at a chosen packet, and judges the
// outcome by what the relay saw on the wire, what a peer subscribed directly
// at the broker received, and what the reader reported.
//
// Gate: `brenn_mqtt::broker_gate!`.
//
// Run: bazel test //brenn-mqtt:session_resume

// The shared harness serves the main broker suite too; this suite uses part
// of it.
#[allow(dead_code, unused_imports)]
mod common;
#[path = "common/packet_relay.rs"]
mod packet_relay;

use std::sync::Arc;
use std::time::Duration;

use brenn_envelope::MqttExpiry;
use brenn_lib::mqtt::config::TlsVersionMin;
use brenn_mqtt::ArrivingFilters;
use brenn_mqtt::broker_gate;
use brenn_mqtt::reader::{EgressMessage, EgressReport, Sent, UnknownCause};
use brenn_mqtt::service::{IngressSubscribeOutcome, IngressUnsubscribeOutcome, SessionFault};
use brenn_mqtt::state::ConnectorHealthLabel;
use brenn_mqtt::test_support::broker::{
    log_records_connack, log_records_subscribe_at, log_records_unsubscribe,
};
use brenn_mqtt::test_support::client::{session_client_id, wait_for_filter_acked};
use common::{
    BrokerHarness, DEFAULT_ACL, DEFAULT_CONF_TEMPLATE, Egress, SpawnedClient, await_puback, certs,
    direct_publisher_acked, direct_subscriber_every, spawn_client_arriving,
    spawn_client_with_config, test_client_config, wait_for_health,
};
use packet_relay::{
    CONNACK, CONNECT, Cut, DISCONNECT, Dir, Fate, PUBACK, PUBCOMP, PUBLISH, PUBREC, PUBREL, Packet,
    PacketRelay, SUBSCRIBE, UNSUBSCRIBE,
};
use tokio::sync::mpsc;

/// The session expiry every case's client asks for: far longer than any
/// staged outage.
const SESSION_EXPIRY_SECS: u32 = 300;

/// How long a staged outage lasts at least: over a second, so a publish the
/// reader built afresh would carry a smaller Message Expiry Interval than the
/// one first sent.
const OUTAGE: Duration = Duration::from_secs(2);

/// One client behind a relay, its egress reader, and a peer subscribed to the
/// case's topic directly at the broker.
struct Rig {
    broker: BrokerHarness,
    relay: PacketRelay,
    spawned: SpawnedClient,
    egress: Egress,
    peer: mpsc::UnboundedReceiver<Vec<u8>>,
    topic: String,
}

impl Rig {
    async fn start(name: &str) -> Self {
        Self::start_on(name, BrokerHarness::start(), vec![]).await
    }

    /// [`Rig::start`] with the client also subscribed to the case's inbound
    /// topic, for [`Rig::hold_supervisor`] and the inbound cases, returning
    /// once the broker has granted it.
    async fn start_inbound(name: &str) -> Self {
        let rig = Self::start_on(
            name,
            BrokerHarness::start(),
            vec![inbound_topic(&case_topic(name))],
        )
        .await;
        wait_for_filter_acked(
            &rig.spawned.svc,
            &rig.spawned.client_slug,
            &inbound_topic(&rig.topic),
            10,
            "the inbound filter was never acknowledged",
        )
        .await;
        rig
    }

    /// One client of `broker` behind a relay, subscribed to `filters`.
    async fn start_on(name: &str, broker: BrokerHarness, filters: Vec<String>) -> Self {
        let relay = PacketRelay::start(broker.port).await;
        let ca = certs::ca_pem_bytes();
        let mut config = test_client_config(
            &format!("resume-{name}"),
            relay.port,
            ca.clone(),
            TlsVersionMin::Tls12,
        );
        config.identity.session_expiry_secs = SESSION_EXPIRY_SECS;
        let topic = case_topic(name);
        let spawned = spawn_client_with_config(Arc::new(config), filters).await;
        wait_for_health(
            &spawned.svc,
            &spawned.client_slug,
            &[ConnectorHealthLabel::Connected],
            10,
            "the client never connected through the relay",
        )
        .await;
        let egress = Egress::start(&spawned.handle);
        let peer = direct_subscriber_every(broker.port, ca, &topic).await;
        Self {
            broker,
            relay,
            spawned,
            egress,
            peer,
            topic,
        }
    }

    /// The next message on the case's topic at `qos`, with `lifetime`.
    fn message(&mut self, qos: u8, payload: &[u8], lifetime: Duration) -> EgressMessage {
        let mut message = self.egress.message(&self.topic, qos, payload);
        message.publish.expires = MqttExpiry::At(
            chrono::Utc::now() + chrono::TimeDelta::from_std(lifetime).expect("a lifetime"),
        );
        message
    }

    /// [`Rig::message`], naming a `status_to` so the reader reports it handed
    /// before it gives it to the library.
    fn tracked(&mut self, qos: u8, payload: &[u8], lifetime: Duration) -> EgressMessage {
        let mut message = self.message(qos, payload, lifetime);
        message.publish.status_to = Some("brenn:itest-status".to_string());
        message
    }

    /// Hold the supervisor still: a message published at the broker on the
    /// case's inbound topic reaches the router, whose delivery blocks until
    /// the test releases the client's hold. Meanwhile the client cell keeps
    /// its client and nothing polls the event loop.
    async fn hold_supervisor(&self) {
        let inbound = inbound_topic(&self.topic);
        self.spawned.hold.arm(&inbound);
        let (publisher, mut acks) =
            direct_publisher_acked(self.broker.port, certs::ca_pem_bytes()).await;
        publisher
            .publish(
                inbound,
                rumqttc::mqttbytes::QoS::AtLeastOnce,
                false,
                b"hold".to_vec(),
            )
            .await
            .expect("the inbound publish was not queued");
        await_puback(&mut acks, "the inbound publish").await;
        self.spawned.hold.wait_held(10).await;
    }

    /// Commit `message`, which names a `status_to`, while the supervisor is
    /// held, and return once the reader has given it to the library.
    ///
    /// A second tracked message, on another topic, is committed behind it. The
    /// reader hands the two in order in one pass and reports the second
    /// handed only after the first was accepted; a refusal would have held the
    /// first and ended the pass. The second message is delivered like any
    /// other; its reports are the caller's to expect.
    async fn submit_while_held(&mut self, message: EgressMessage) -> uuid::Uuid {
        let id = message.id;
        let mut marker = self
            .egress
            .tracked(&format!("{}/marker", self.topic), 1, b"marker");
        marker.publish.expires = message.publish.expires;
        let marker_id = marker.id;
        self.egress.bus.append(message);
        self.egress.bus.append(marker);
        for want in [id, marker_id] {
            match self.egress.next(15, "a handed message").await {
                EgressReport::Handed(meta) if meta.id == want => {}
                other => panic!("expected {want} handed, got {other:?}"),
            }
        }
        marker_id
    }

    /// Stop the broker, with the relay refusing new connections.
    async fn stop_broker(&mut self) {
        self.relay.refuse(true).await;
        self.broker.stop();
    }

    /// Start the broker again after [`Rig::stop_broker`], with nothing of its
    /// sessions kept, stop discarding packets, subscribe a fresh peer, and let
    /// the relay take connections again.
    async fn restart_broker(&mut self) {
        self.broker.restart();
        self.relay.clear_drops();
        self.peer =
            direct_subscriber_every(self.broker.port, certs::ca_pem_bytes(), &self.topic).await;
        self.relay.refuse(false).await;
    }

    /// Wait up to fifteen seconds for the relay to have logged `count`
    /// entries matching `wanted`.
    async fn wait_count(&self, count: usize, what: &str, wanted: impl Fn(&Packet) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while self.relay.log().iter().filter(|p| wanted(p)).count() < count {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{what}: fewer than {count} within 15s; relay log:\n{}",
                self.relay.dump()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Publish `payload` at `qos` on the case's inbound topic, directly at the
    /// broker; a QoS 1 publish returns once the broker has it.
    async fn publish_inbound(&self, qos: u8, payload: &[u8]) {
        let (publisher, mut acks) =
            direct_publisher_acked(self.broker.port, certs::ca_pem_bytes()).await;
        let qos = match qos {
            0 => rumqttc::mqttbytes::QoS::AtMostOnce,
            1 => rumqttc::mqttbytes::QoS::AtLeastOnce,
            other => panic!("an inbound publish at QoS {other}"),
        };
        publisher
            .publish(inbound_topic(&self.topic), qos, false, payload.to_vec())
            .await
            .expect("the inbound publish was not queued");
        if qos == rumqttc::mqttbytes::QoS::AtLeastOnce {
            await_puback(&mut acks, "the inbound publish").await;
        } else {
            // A QoS 0 publish is written by the publisher's own loop; give
            // it the moment it needs before the publisher is dropped.
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// The next inbound delivery to the router, within ten seconds; asserts
    /// it is on the case's inbound topic and returns its payload.
    async fn delivered(&mut self, what: &str) -> Vec<u8> {
        let delivery = tokio::time::timeout(Duration::from_secs(10), self.spawned.rx.recv())
            .await
            .unwrap_or_else(|_| panic!("{what}: no delivery within 10s"))
            .expect("the router's channel closed");
        assert_eq!(delivery.topic, inbound_topic(&self.topic), "{what}");
        delivery.payload.bytes.to_vec()
    }

    /// Assert the router receives no delivery within a second.
    async fn no_more_deliveries(&mut self) {
        if let Ok(extra) =
            tokio::time::timeout(Duration::from_secs(1), self.spawned.rx.recv()).await
        {
            panic!("a delivery after the last expected one: {extra:?}");
        }
    }

    /// The next `count` reports, in the order made.
    async fn reports(&mut self, count: usize, what: &str) -> Vec<EgressReport> {
        let mut reports = Vec::with_capacity(count);
        for _ in 0..count {
            reports.push(self.egress.next(30, what).await);
        }
        reports
    }

    /// The PUBLISH packets on the case's topic that connection `conn` carried
    /// to the broker, in order.
    fn publishes_on(&self, conn: usize) -> Vec<Packet> {
        self.relay
            .forwarded(conn, Dir::ClientToBroker, PUBLISH)
            .into_iter()
            .filter(|p| p.topic.as_deref() == Some(&self.topic))
            .collect()
    }

    /// Assert connection `conn` carried PUBLISH packets on the case's topic
    /// whose payloads are `payloads`, in that order, and return them.
    fn assert_publishes(&self, conn: usize, payloads: &[&[u8]]) -> Vec<Packet> {
        let publishes = self.publishes_on(conn);
        assert!(
            publishes.len() == payloads.len()
                && publishes
                    .iter()
                    .zip(payloads)
                    .all(|(p, want)| p.bytes.ends_with(want)),
            "conn{conn}'s PUBLISHes are not {:?}; relay log:\n{}",
            payloads
                .iter()
                .map(|p| String::from_utf8_lossy(p))
                .collect::<Vec<_>>(),
            self.relay.dump()
        );
        publishes
    }

    async fn health(&self) -> ConnectorHealthLabel {
        self.spawned
            .svc
            .ingress_health(&self.spawned.client_slug)
            .await
            .0
    }

    /// Wait up to fifteen seconds for the supervisor to have reported `count`
    /// session faults, and return them.
    async fn wait_faults(&self, count: usize, what: &str) -> Vec<SessionFault> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let faults: Vec<SessionFault> = self
                .spawned
                .faults
                .lock()
                .unwrap()
                .iter()
                .map(|(client, fault)| {
                    assert_eq!(client, &self.spawned.client_slug, "the fault's client");
                    fault.clone()
                })
                .collect();
            if faults.len() >= count {
                return faults;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{what}: {} of {count} faults within 15s; relay log:\n{}",
                faults.len(),
                self.relay.dump()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The number of CONNECT packets the relay has seen.
    fn connects(&self) -> usize {
        self.relay
            .log()
            .iter()
            .filter(|p| p.is(Dir::ClientToBroker, CONNECT))
            .count()
    }

    /// Assert no CONNECT is added over `window`.
    async fn connects_stay_put(&self, window: Duration, what: &str) {
        let before = self.connects();
        tokio::time::sleep(window).await;
        assert_eq!(
            self.connects(),
            before,
            "{what}; relay log:\n{}",
            self.relay.dump()
        );
    }

    async fn wait_health(&self, label: ConnectorHealthLabel, what: &str) {
        wait_for_health(
            &self.spawned.svc,
            &self.spawned.client_slug,
            &[label],
            15,
            what,
        )
        .await;
    }

    /// Assert the peer receives `count` copies of `payload` and nothing more.
    async fn peer_receives(&mut self, count: usize, payload: &[u8]) {
        self.peer_receives_each(&vec![payload; count]).await;
    }

    /// Assert the peer receives `payloads`, in that order, and nothing more.
    async fn peer_receives_each(&mut self, payloads: &[&[u8]]) {
        let count = payloads.len();
        for (n, payload) in payloads.iter().enumerate() {
            let got = tokio::time::timeout(Duration::from_secs(10), self.peer.recv())
                .await
                .unwrap_or_else(|_| panic!("the peer received {n} of {count} messages"))
                .expect("the peer's channel closed");
            assert_eq!(&got, payload, "message {} at the peer", n + 1);
        }
        if let Ok(extra) = tokio::time::timeout(Duration::from_secs(1), self.peer.recv()).await {
            panic!("the peer received more than {count} messages: {extra:?}");
        }
    }

    /// Assert the reader makes no further report within a second.
    async fn no_more_reports(&mut self) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(self.egress.made(), None, "a report after the outcome");
    }

    /// The first connection after `conn` that the broker accepted.
    fn next_accepted(&self, conn: usize) -> usize {
        self.relay
            .log()
            .iter()
            .find(|p| p.conn > conn && p.is(Dir::BrokerToClient, CONNACK))
            .unwrap_or_else(|| {
                panic!(
                    "no connection after conn{conn} was accepted; relay log:\n{}",
                    self.relay.dump()
                )
            })
            .conn
    }

    /// The CONNECT and CONNACK of connection `conn`.
    fn handshake(&self, conn: usize) -> (Packet, Packet) {
        let connect = self.relay.forwarded(conn, Dir::ClientToBroker, CONNECT);
        let connack = self.relay.forwarded(conn, Dir::BrokerToClient, CONNACK);
        assert_eq!(
            connect.len(),
            1,
            "conn{conn}'s CONNECT; relay log:\n{}",
            self.relay.dump()
        );
        assert_eq!(
            connack.len(),
            1,
            "conn{conn}'s CONNACK; relay log:\n{}",
            self.relay.dump()
        );
        (connect[0].clone(), connack[0].clone())
    }

    /// Assert the connection after `lost` resumed the session: a CONNECT
    /// without Clean Start, answered with Session Present. Returns its number.
    fn assert_resumed_after(&self, lost: usize) -> usize {
        let resumed = self.next_accepted(lost);
        let (connect, connack) = self.handshake(resumed);
        assert_eq!(connect.clean_start, Some(false), "conn{resumed}'s CONNECT");
        assert_eq!(
            connack.session_present,
            Some(true),
            "conn{resumed}'s CONNACK; relay log:\n{}",
            self.relay.dump()
        );
        resumed
    }

    /// Assert the case's message was replayed by the MQTT library on the
    /// resumed connection, not resubmitted by Brenn, and return the number of
    /// the connection it was lost on.
    ///
    /// The resumed connection carries exactly one PUBLISH on the topic, and
    /// none follows on a later connection; it has the packet identifier and
    /// Message Expiry Interval of the first, and equals it byte for byte once
    /// the DUP bit of the fixed header is masked off in both. A resubmit by
    /// Brenn would be a second PUBLISH, under another packet identifier and
    /// with a smaller interval.
    ///
    /// The DUP bit is masked because the library replays the stored packet
    /// unchanged and does not set it, where MQTT 5 requires it on a
    /// re-delivery [MQTT-3.3.1-1]; a library that sets it must not fail this.
    fn assert_replayed(&self) -> usize {
        let publishes: Vec<Packet> = self
            .relay
            .log()
            .into_iter()
            .filter(|p| {
                p.is(Dir::ClientToBroker, PUBLISH) && p.topic.as_deref() == Some(&self.topic)
            })
            .collect();
        let first = publishes
            .first()
            .unwrap_or_else(|| panic!("no PUBLISH on the wire; relay log:\n{}", self.relay.dump()));
        let resumed = self.assert_resumed_after(first.conn);
        let later: Vec<&Packet> = publishes.iter().filter(|p| p.conn > first.conn).collect();
        assert!(
            later.len() == 1 && later[0].conn == resumed,
            "expected one PUBLISH after conn{}, on conn{resumed}; relay log:\n{}",
            first.conn,
            self.relay.dump()
        );
        let replay = later[0];
        assert!(first.expiry.is_some(), "the message carries a lifetime");
        assert_eq!(replay.pkid, first.pkid, "the replay's packet identifier");
        assert_eq!(
            replay.expiry, first.expiry,
            "the replay's Message Expiry Interval"
        );
        let undup = |bytes: &[u8]| {
            let mut bytes = bytes.to_vec();
            bytes[0] &= !0x08;
            bytes
        };
        assert_eq!(
            undup(&replay.bytes),
            undup(&first.bytes),
            "the replay is the first PUBLISH"
        );
        first.conn
    }

    /// Arm `cut`, commit one message at `qos`, hold the relay refusing for
    /// [`OUTAGE`] once the cut has fired, and wait for the reader's report.
    async fn lose_one(&mut self, qos: u8, cut: Cut) -> (uuid::Uuid, EgressReport) {
        self.lose_one_for(qos, cut, Duration::from_secs(60), OUTAGE)
            .await
    }

    /// [`Rig::lose_one`] with a message of `lifetime` and an outage of
    /// `outage`.
    async fn lose_one_for(
        &mut self,
        qos: u8,
        cut: Cut,
        lifetime: Duration,
        outage: Duration,
    ) -> (uuid::Uuid, EgressReport) {
        self.relay.refuse(true).await;
        self.relay.arm(cut);
        let message = self.message(qos, b"m1", lifetime);
        let id = message.id;
        self.egress.bus.append(message);
        self.relay
            .wait_for(10, "the armed cut", |p| {
                matches!(p.fate, Fate::CutBefore | Fate::CutAfter)
            })
            .await;
        tokio::time::sleep(outage).await;
        self.relay.refuse(false).await;
        let report = self.egress.next(20, "the message's outcome").await;
        (id, report)
    }
}

/// A PUBACK for `pkid` with the reason code and properties left out, which
/// MQTT 5 reads as success.
fn puback(pkid: u16) -> Vec<u8> {
    let [hi, lo] = pkid.to_be_bytes();
    vec![PUBACK << 4, 2, hi, lo]
}

/// The topic a case named `name` publishes on.
fn case_topic(name: &str) -> String {
    format!("brenn/itest/resume/{name}")
}

/// The topic a [`Rig::start_inbound`] client subscribes to beside `topic`.
fn inbound_topic(topic: &str) -> String {
    format!("{topic}/in")
}

/// The report about `id` among `reports`; panics unless there is exactly one.
fn report_for(reports: &[EgressReport], id: uuid::Uuid) -> &EgressReport {
    let about: Vec<&EgressReport> = reports
        .iter()
        .filter(|r| match r {
            EgressReport::Handed(meta) | EgressReport::Delivered(meta) => meta.id == id,
            EgressReport::Unknown { message, .. }
            | EgressReport::Expired { message, .. }
            | EgressReport::Rejected { message, .. } => message.id == id,
            _ => false,
        })
        .collect();
    assert_eq!(about.len(), 1, "reports about {id} in {reports:?}");
    about[0]
}

fn assert_unknown_lost(report: &EgressReport, id: uuid::Uuid) {
    assert!(
        matches!(
            report,
            EgressReport::Unknown { message, cause: UnknownCause::ConnectionLost } if message.id == id
        ),
        "expected {id} unknown, connection lost, got {report:?}"
    );
}

fn assert_delivered(report: &EgressReport, id: uuid::Uuid) {
    assert!(
        matches!(report, EgressReport::Delivered(meta) if meta.id == id),
        "expected {id} delivered, got {report:?}"
    );
}

// ---------------------------------------------------------------------------
// Outgoing QoS 1
// ---------------------------------------------------------------------------

/// The broker's PUBACK is lost: the library replays the PUBLISH on the
/// resumed session and the broker's answer to the replay delivers it; the
/// peer sees it twice, as QoS 1 allows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_qos1_puback_lost_is_replayed() {
    broker_gate!();
    let mut rig = Rig::start("o1a").await;
    let (id, report) = rig
        .lose_one(
            1,
            Cut {
                dir: Dir::BrokerToClient,
                ptype: PUBACK,
                after: false,
            },
        )
        .await;
    assert_delivered(&report, id);
    rig.assert_replayed();
    rig.peer_receives(2, b"m1").await;
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

/// The PUBLISH never reaches the broker: the replay is the only copy the
/// broker gets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_qos1_publish_lost_is_replayed() {
    broker_gate!();
    let mut rig = Rig::start("o1b").await;
    let (id, report) = rig
        .lose_one(
            1,
            Cut {
                dir: Dir::ClientToBroker,
                ptype: PUBLISH,
                after: false,
            },
        )
        .await;
    assert_delivered(&report, id);
    rig.assert_replayed();
    rig.peer_receives(1, b"m1").await;
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

// ---------------------------------------------------------------------------
// Outgoing QoS 2, lost at each stage of the exchange
// ---------------------------------------------------------------------------

/// Lose the exchange of one QoS 2 message at `cut`: the resumed session
/// completes it where it stopped, the peer receives it exactly once, and the
/// reader reports it delivered and nothing else.
async fn outgoing_qos2_row(name: &str, cut: Cut) {
    let mut rig = Rig::start(name).await;
    let (id, report) = rig.lose_one(2, cut).await;
    assert_delivered(&report, id);
    let lost = rig
        .relay
        .log()
        .iter()
        .find(|p| matches!(p.fate, Fate::CutBefore))
        .expect("the cut is logged")
        .conn;
    rig.assert_resumed_after(lost);
    rig.peer_receives(1, b"m1").await;
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

/// The broker's PUBREC is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_qos2_pubrec_lost_completes_once() {
    broker_gate!();
    outgoing_qos2_row(
        "o2a",
        Cut {
            dir: Dir::BrokerToClient,
            ptype: PUBREC,
            after: false,
        },
    )
    .await;
}

/// The PUBLISH never reaches the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_qos2_publish_lost_completes_once() {
    broker_gate!();
    outgoing_qos2_row(
        "o2b",
        Cut {
            dir: Dir::ClientToBroker,
            ptype: PUBLISH,
            after: false,
        },
    )
    .await;
}

/// The PUBREL reached the broker; its PUBCOMP is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_qos2_pubcomp_lost_completes_once() {
    broker_gate!();
    outgoing_qos2_row(
        "o3a",
        Cut {
            dir: Dir::BrokerToClient,
            ptype: PUBCOMP,
            after: false,
        },
    )
    .await;
}

/// The PUBREL never reaches the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outgoing_qos2_pubrel_lost_completes_once() {
    broker_gate!();
    outgoing_qos2_row(
        "o3b",
        Cut {
            dir: Dir::ClientToBroker,
            ptype: PUBREL,
            after: false,
        },
    )
    .await;
}

/// The accepted limit: a message already handed to the MQTT library is
/// replayed when the session resumes even after its lifetime has passed,
/// with the Message Expiry Interval it was first sent with.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replay_is_not_recalled_after_its_lifetime() {
    broker_gate!();
    let mut rig = Rig::start("limit").await;
    let lifetime = Duration::from_secs(3);
    let committed = tokio::time::Instant::now();
    let (id, report) = rig
        .lose_one_for(
            1,
            Cut {
                dir: Dir::ClientToBroker,
                ptype: PUBLISH,
                after: false,
            },
            lifetime,
            lifetime + OUTAGE,
        )
        .await;
    assert!(
        committed.elapsed() > lifetime,
        "the outcome came inside the lifetime"
    );
    assert_delivered(&report, id);
    rig.assert_replayed();
    rig.peer_receives(1, b"m1").await;
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

// ---------------------------------------------------------------------------
// A publish submitted as the connection fails
// ---------------------------------------------------------------------------

/// The supervisor is held still in the router while the relay severs its
/// connection, and the reader gives a message at `qos` to the library in that
/// gap, before the supervisor has seen the loss. The message reaches the wire
/// once, on the resumed connection, and is delivered once.
///
/// The message is on the request channel when the supervisor's next `poll`
/// fails, so the library's own sweep inside that `poll` takes it; this row
/// passes without the supervisor's `sweep_after_loss`, which covers a submit
/// between that `poll` returning and the client cell being emptied and is
/// pinned by the unit test `the_sweep_at_a_loss_puts_a_late_publish_with_the_session`.
async fn submitted_at_the_loss_row(name: &str, qos: u8) {
    let mut rig = Rig::start_inbound(name).await;
    rig.hold_supervisor().await;
    rig.relay.sever();
    let lost = rig
        .relay
        .wait_for(10, "the sever", |p| p.fate == Fate::Severed)
        .await
        .conn;
    let message = rig.tracked(qos, b"m1", Duration::from_secs(60));
    let id = message.id;
    let marker = rig.submit_while_held(message).await;
    rig.spawned.hold.release();

    let reports = rig.reports(2, "the outcomes").await;
    assert_delivered(report_for(&reports, id), id);
    assert_delivered(report_for(&reports, marker), marker);
    let resumed = rig.assert_resumed_after(lost);
    rig.assert_publishes(resumed, &[b"m1"]);
    let elsewhere: Vec<Packet> = rig
        .relay
        .log()
        .into_iter()
        .filter(|p| {
            p.is(Dir::ClientToBroker, PUBLISH)
                && p.topic.as_deref() == Some(&rig.topic)
                && p.conn != resumed
        })
        .collect();
    assert!(
        elsewhere.is_empty(),
        "a PUBLISH off the resumed conn{resumed}; relay log:\n{}",
        rig.relay.dump()
    );
    rig.peer_receives(1, b"m1").await;
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_qos1_publish_submitted_at_the_loss_is_sent_once() {
    broker_gate!();
    submitted_at_the_loss_row("atloss1", 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_qos2_publish_submitted_at_the_loss_is_sent_once() {
    broker_gate!();
    submitted_at_the_loss_row("atloss2", 2).await;
}

// ---------------------------------------------------------------------------
// The broker kept no session
// ---------------------------------------------------------------------------

/// With a QoS 2 exchange waiting for its PUBCOMP, another waiting for its
/// PUBREC and a QoS 1 message waiting for its PUBACK, the broker restarts
/// without its sessions. The reconnect finds no session: each QoS 2 message
/// is reported unknown, the QoS 1 message is sent again as a new publish and
/// delivered, and a message committed during the outage goes out behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_gone_resends_qos1_and_reports_qos2_unknown() {
    broker_gate!();
    let mut rig = Rig::start("gone").await;
    let lifetime = Duration::from_secs(60);

    rig.relay.drop_packets(Dir::BrokerToClient, PUBCOMP);
    let released = rig.message(2, b"m-released", lifetime);
    let released_id = released.id;
    rig.egress.bus.append(released);
    rig.relay
        .wait_for(10, "the dropped PUBCOMP", |p| {
            p.is(Dir::BrokerToClient, PUBCOMP) && p.fate == Fate::Dropped
        })
        .await;
    rig.relay.drop_packets(Dir::BrokerToClient, PUBREC);
    let received = rig.message(2, b"m-received", lifetime);
    let received_id = received.id;
    rig.egress.bus.append(received);
    rig.relay
        .wait_for(10, "the dropped PUBREC", |p| {
            p.is(Dir::BrokerToClient, PUBREC) && p.fate == Fate::Dropped
        })
        .await;
    rig.relay.drop_packets(Dir::BrokerToClient, PUBACK);
    let acked = rig.message(1, b"m-acked", lifetime);
    let acked_id = acked.id;
    rig.egress.bus.append(acked);
    let lost = rig
        .relay
        .wait_for(10, "the dropped PUBACK", |p| {
            p.is(Dir::BrokerToClient, PUBACK) && p.fate == Fate::Dropped
        })
        .await
        .conn;
    let first = rig.publishes_on(lost);
    let first_acked = first
        .iter()
        .find(|p| p.bytes.ends_with(b"m-acked"))
        .expect("the QoS 1 message's first PUBLISH")
        .clone();

    rig.stop_broker().await;
    rig.wait_health(
        ConnectorHealthLabel::Disconnected,
        "the client never saw the broker go",
    )
    .await;
    let later = rig.message(1, b"m-later", lifetime);
    let later_id = later.id;
    rig.egress.bus.append(later);
    tokio::time::sleep(OUTAGE).await;
    rig.restart_broker().await;

    let reports = rig.reports(4, "the outcomes").await;
    assert_unknown_lost(report_for(&reports, released_id), released_id);
    assert_unknown_lost(report_for(&reports, received_id), received_id);
    assert_delivered(report_for(&reports, acked_id), acked_id);
    assert_delivered(report_for(&reports, later_id), later_id);

    let reset = rig.next_accepted(lost);
    let (connect, connack) = rig.handshake(reset);
    assert_eq!(connect.clean_start, Some(false), "conn{reset}'s CONNECT");
    assert_eq!(
        connack.session_present,
        Some(false),
        "conn{reset}'s CONNACK; relay log:\n{}",
        rig.relay.dump()
    );
    let resent = rig.assert_publishes(reset, &[b"m-acked", b"m-later"]);
    assert!(
        resent[0].expiry < first_acked.expiry,
        "the QoS 1 message was not built afresh: {:?} then {:?}",
        first_acked.expiry,
        resent[0].expiry
    );
    rig.peer_receives_each(&[b"m-acked", b"m-later"]).await;
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

/// A QoS 1 message A waits for its PUBACK; the supervisor is held still while
/// the relay severs, and the reader gives a QoS 1 message B to the library in
/// that gap; the broker restarts without its sessions. Once the supervisor
/// is released and reconnects, A and B are sent again in channel order, and a
/// message C committed after the reconnect goes out behind them.
///
/// As in `submitted_at_the_loss_row`, B is taken by the library's own sweep inside the
/// failing `poll`; the supervisor's `sweep_after_loss` is not reached here and
/// is pinned by the unit test `the_sweep_at_a_loss_puts_a_late_publish_with_the_session`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_submitted_after_the_loss_keeps_its_place_on_a_session_reset() {
    broker_gate!();
    let mut rig = Rig::start_inbound("late").await;
    let lifetime = Duration::from_secs(60);

    rig.relay.drop_packets(Dir::BrokerToClient, PUBACK);
    let a = rig.message(1, b"m-a", lifetime);
    let a_id = a.id;
    rig.egress.bus.append(a);
    let lost = rig
        .relay
        .wait_for(10, "A's dropped PUBACK", |p| {
            p.is(Dir::BrokerToClient, PUBACK) && p.fate == Fate::Dropped
        })
        .await
        .conn;

    rig.hold_supervisor().await;
    rig.relay.refuse(true).await;
    rig.relay.sever();
    rig.relay
        .wait_for(10, "the sever", |p| p.fate == Fate::Severed)
        .await;
    let b = rig.tracked(1, b"m-b", lifetime);
    let b_id = b.id;
    let marker = rig.submit_while_held(b).await;
    rig.stop_broker().await;
    rig.restart_broker().await;
    rig.spawned.hold.release();

    let reset = rig
        .relay
        .wait_for(15, "a connection after the restart", |p| {
            p.conn > lost && p.is(Dir::BrokerToClient, CONNACK)
        })
        .await
        .conn;
    let c = rig.message(1, b"m-c", lifetime);
    let c_id = c.id;
    rig.egress.bus.append(c);

    let reports = rig.reports(4, "the outcomes").await;
    for id in [a_id, b_id, marker, c_id] {
        assert_delivered(report_for(&reports, id), id);
    }
    let (_, connack) = rig.handshake(reset);
    assert_eq!(
        connack.session_present,
        Some(false),
        "conn{reset}'s CONNACK; relay log:\n{}",
        rig.relay.dump()
    );
    rig.assert_publishes(reset, &[b"m-a", b"m-b", b"m-c"]);
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

// ---------------------------------------------------------------------------
// The in-flight bound across a resume
// ---------------------------------------------------------------------------

/// The Receive Maximum the bound case's broker states.
const BOUND: usize = 3;

/// With the broker's Receive Maximum of [`BOUND`] filled by QoS 1 messages
/// whose PUBACKs are lost, a resumed session carries all of them over: the
/// reader hands nothing new while the replays are unanswered, and the next
/// message goes out only after a replay is acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_flight_bound_holds_across_a_resume() {
    broker_gate!();
    let broker = BrokerHarness::start_with(
        &format!("{DEFAULT_CONF_TEMPLATE}max_inflight_messages {BOUND}\n"),
        &[("acl", DEFAULT_ACL.as_bytes())],
    );
    let mut rig = Rig::start_on("bound", broker, vec![]).await;
    let first: Vec<&[u8]> = vec![b"m-1", b"m-2", b"m-3"];
    let all: Vec<&[u8]> = vec![b"m-1", b"m-2", b"m-3", b"m-4", b"m-5"];
    assert_eq!(first.len(), BOUND);
    let dropped_puback = |conn: usize| {
        move |p: &Packet| {
            p.conn == conn && p.is(Dir::BrokerToClient, PUBACK) && p.fate == Fate::Dropped
        }
    };

    rig.relay.drop_packets(Dir::BrokerToClient, PUBACK);
    let mut ids = Vec::new();
    for payload in &all {
        let message = rig.message(1, payload, Duration::from_secs(60));
        ids.push(message.id);
        rig.egress.bus.append(message);
    }
    let lost = rig
        .relay
        .wait_for(10, "the first dropped PUBACK", |p| {
            p.is(Dir::BrokerToClient, PUBACK) && p.fate == Fate::Dropped
        })
        .await
        .conn;
    rig.wait_count(
        BOUND,
        "the first connection's dropped PUBACKs",
        dropped_puback(lost),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    rig.assert_publishes(lost, &first);

    rig.relay.sever();
    let resumed = rig
        .relay
        .wait_for(15, "the resumed connection", |p| {
            p.conn > lost && p.is(Dir::BrokerToClient, CONNACK)
        })
        .await
        .conn;
    rig.wait_count(
        BOUND,
        "the replays' dropped PUBACKs",
        dropped_puback(resumed),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rig.assert_resumed_after(lost), resumed);
    rig.assert_publishes(resumed, &first);
    assert_eq!(
        rig.egress.made(),
        None,
        "a report while the replays are unanswered"
    );

    rig.relay.clear_drops();
    rig.relay.sever();
    let reports = rig.reports(all.len(), "the outcomes").await;
    for id in &ids {
        assert_delivered(report_for(&reports, *id), *id);
    }
    let last = rig.assert_resumed_after(resumed);
    rig.assert_publishes(last, &all);
    let log = rig.relay.log();
    let first_ack = log
        .iter()
        .position(|p| {
            p.conn == last && p.is(Dir::BrokerToClient, PUBACK) && p.fate == Fate::Forwarded
        })
        .expect("a PUBACK on the last connection");
    let first_new = log
        .iter()
        .position(|p| {
            p.conn == last && p.is(Dir::ClientToBroker, PUBLISH) && p.bytes.ends_with(b"m-4")
        })
        .expect("the first new PUBLISH");
    assert!(
        first_ack < first_new,
        "a new PUBLISH before any replay was acknowledged; relay log:\n{}",
        rig.relay.dump()
    );
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

// ---------------------------------------------------------------------------
// Incoming
// ---------------------------------------------------------------------------

/// The client's PUBACK for an incoming QoS 1 message is lost: the resumed
/// session's broker sends it again, flagged DUP, and the router receives it
/// twice, as QoS 1 allows; a later message arrives once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incoming_qos1_whose_puback_was_lost_arrives_twice() {
    broker_gate!();
    let mut rig = Rig::start_inbound("in1").await;
    rig.relay.arm(Cut {
        dir: Dir::ClientToBroker,
        ptype: PUBACK,
        after: false,
    });
    rig.publish_inbound(1, b"m1").await;
    assert_eq!(rig.delivered("the first copy").await, b"m1");
    let lost = rig
        .relay
        .wait_for(10, "the cut PUBACK", |p| p.fate == Fate::CutBefore)
        .await
        .conn;
    assert_eq!(rig.delivered("the broker's second copy").await, b"m1");
    let resumed = rig.assert_resumed_after(lost);
    let inbound = inbound_topic(&rig.topic);
    let copies: Vec<Packet> = rig
        .relay
        .log()
        .into_iter()
        .filter(|p| p.is(Dir::BrokerToClient, PUBLISH) && p.topic.as_deref() == Some(&inbound))
        .collect();
    assert!(
        copies.len() == 2
            && copies[0].conn == lost
            && !copies[0].dup
            && copies[1].conn == resumed
            && copies[1].dup,
        "the copies on the wire; relay log:\n{}",
        rig.relay.dump()
    );

    rig.publish_inbound(1, b"m2").await;
    assert_eq!(rig.delivered("the later message").await, b"m2");
    rig.no_more_deliveries().await;
    rig.spawned.handle.stop();
}

/// With the supervisor held still, the relay forwards an incoming PUBLISH at
/// `qos` to the client and severs the connection behind it. Released, the
/// supervisor's next poll reads the PUBLISH and the end of the connection in
/// one go, so the library holds the message among the lost connection's
/// events. Returns the number of copies the router received, each checked to
/// be the message.
async fn incoming_among_the_lost_events_row(name: &str, qos: u8) -> usize {
    let mut rig = Rig::start_inbound(name).await;
    rig.hold_supervisor().await;
    rig.relay.arm(Cut {
        dir: Dir::BrokerToClient,
        ptype: PUBLISH,
        after: true,
    });
    rig.publish_inbound(qos, b"m1").await;
    let lost = rig
        .relay
        .wait_for(10, "the cut after the PUBLISH", |p| {
            p.fate == Fate::CutAfter
        })
        .await
        .conn;
    rig.spawned.hold.release();
    assert_eq!(rig.delivered("the held message").await, b"hold");
    assert_eq!(rig.delivered("the message").await, b"m1");
    // TODO(mqtt-lost-events-incoming-test-flaky): the message is sometimes
    // delivered from the live connection, before the loss, and this fails.
    let reconnected = rig
        .relay
        .log()
        .iter()
        .any(|p| p.conn > lost && p.is(Dir::BrokerToClient, CONNACK));
    assert!(
        reconnected,
        "the message was delivered before the client reconnected, not from the lost \
         connection's events; relay log:\n{}",
        rig.relay.dump()
    );
    let mut copies = 1;
    while let Ok(Some(extra)) =
        tokio::time::timeout(Duration::from_secs(1), rig.spawned.rx.recv()).await
    {
        assert_eq!(extra.payload.bytes.as_ref(), b"m1", "another copy");
        copies += 1;
    }
    rig.spawned.handle.stop();
    copies
}

/// A QoS 0 message among the lost connection's events reaches the router
/// exactly once, after the reconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incoming_qos0_among_the_lost_events_arrives_once() {
    broker_gate!();
    assert_eq!(incoming_among_the_lost_events_row("lost0", 0).await, 1);
}

/// A QoS 1 message among the lost connection's events reaches the router at
/// least once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incoming_qos1_among_the_lost_events_arrives() {
    broker_gate!();
    assert!(incoming_among_the_lost_events_row("lost1", 1).await >= 1);
}

// ---------------------------------------------------------------------------
// SUBSCRIBE attribution across a resume
// ---------------------------------------------------------------------------

/// The client holds several filters, one of which is refused. The connection
/// is cut at the first SUBSCRIBE of a re-assert, so the SUBSCRIBEs behind it
/// go to a dead connection and are never answered; the re-assert on the
/// resumed session repeats every filter under new packet identifiers. Every
/// granted filter then reads granted, and the refusal is recorded against the
/// refused filter and no other.
///
/// The library writes every queued SUBSCRIBE to the dead connection before it
/// reads the end of it, so the resumed session replays none, and this row does
/// not reach a replayed SUBSCRIBE binding its filter through the kept FIFO.
/// That binding is pinned by the unit test
/// `a_connack_keeps_the_subscribe_fifo_only_with_a_session`.
///
/// The relay refuses the filter: the suite's mosquitto does not apply its ACL
/// to a SUBSCRIBE and grants every filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_outcomes_are_attributed_across_a_resume() {
    broker_gate!();
    let base = case_topic("attr");
    let refused = "brenn/refused/attr".to_string();
    let mut filters: Vec<String> = (0..6).map(|i| format!("{base}/f{i}")).collect();
    filters.insert(3, refused.clone());
    let rig = Rig::start_on("attr", BrokerHarness::start(), filters.clone()).await;
    let granted: Vec<&String> = filters.iter().filter(|f| **f != refused).collect();

    let assert_attributed = async |what: &str| {
        for filter in &granted {
            wait_for_filter_acked(&rig.spawned.svc, &rig.spawned.client_slug, filter, 15, what)
                .await;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let reason = loop {
            if let Some(reason) = rig
                .spawned
                .svc
                .subscription_refusal(&rig.spawned.client_slug, &refused)
                .await
            {
                break reason;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{what}: no refusal recorded for {refused}; relay log:\n{}",
                rig.relay.dump()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(!reason.is_empty(), "{what}: the refusal's words");
        for filter in &granted {
            assert_eq!(
                rig.spawned
                    .svc
                    .subscription_refusal(&rig.spawned.client_slug, filter)
                    .await,
                None,
                "{what}: a refusal recorded against {filter}"
            );
        }
        assert_eq!(
            rig.spawned
                .svc
                .subscription_acked(&rig.spawned.client_slug, &refused)
                .await,
            Some(false),
            "{what}: the refused filter reads granted"
        );
    };
    rig.relay.refuse_filter(&refused);
    rig.relay.sever();
    rig.relay
        .wait_for(15, "a SUBACK refusing the filter", |p| {
            p.fate == Fate::Refused
        })
        .await;
    assert_attributed("after the first resume").await;

    rig.relay.arm(Cut {
        dir: Dir::ClientToBroker,
        ptype: SUBSCRIBE,
        after: false,
    });
    rig.relay.sever();
    let cut = rig
        .relay
        .wait_for(15, "the cut SUBSCRIBE", |p| p.fate == Fate::CutBefore)
        .await
        .conn;
    let resumed = rig
        .relay
        .wait_for(15, "the resumed connection", |p| {
            p.conn > cut && p.is(Dir::BrokerToClient, CONNACK)
        })
        .await
        .conn;
    assert_eq!(rig.assert_resumed_after(cut), resumed);
    assert_attributed("after the resume").await;
    rig.spawned.handle.stop();
}

// ---------------------------------------------------------------------------
// While disconnected
// ---------------------------------------------------------------------------

/// A message committed while the client is disconnected, whose lifetime ends
/// before the client reconnects, is never handed: it never reaches the wire
/// and is reported expired, never sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_is_handed_while_disconnected() {
    broker_gate!();
    let mut rig = Rig::start("nohand").await;
    rig.relay.refuse(true).await;
    rig.relay.sever();
    rig.wait_health(
        ConnectorHealthLabel::Disconnected,
        "the client never saw the loss",
    )
    .await;
    let message = rig.message(1, b"short-lived", Duration::from_secs(1));
    let id = message.id;
    rig.egress.bus.append(message);
    tokio::time::sleep(OUTAGE + Duration::from_secs(1)).await;
    rig.relay.refuse(false).await;

    match rig.egress.next(20, "the expiry").await {
        EgressReport::Expired { message, sent } => {
            assert_eq!(message.id, id);
            assert_eq!(sent, Sent::Never);
        }
        other => panic!("expected {id} expired, got {other:?}"),
    }
    assert!(
        !rig.relay
            .log()
            .iter()
            .any(|p| p.is(Dir::ClientToBroker, PUBLISH)),
        "a PUBLISH reached the wire; relay log:\n{}",
        rig.relay.dump()
    );
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

/// A stop while the client is disconnected, with a QoS 2 exchange in flight,
/// returns at once rather than at the stop bound, and the message is reported
/// unknown because its connection was lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_while_disconnected_returns_at_once() {
    broker_gate!();
    let mut rig = Rig::start("stopdown").await;
    rig.relay.drop_packets(Dir::BrokerToClient, PUBREC);
    let message = rig.message(2, b"in-flight", Duration::from_secs(60));
    let id = message.id;
    rig.egress.bus.append(message);
    rig.relay
        .wait_for(10, "the dropped PUBREC", |p| {
            p.is(Dir::BrokerToClient, PUBREC) && p.fate == Fate::Dropped
        })
        .await;
    rig.relay.refuse(true).await;
    rig.relay.sever();
    rig.wait_health(
        ConnectorHealthLabel::Disconnected,
        "the client never saw the loss",
    )
    .await;

    let handle = rig.spawned.handle.clone();
    let bound = handle.stop_wait();
    let started = tokio::time::Instant::now();
    tokio::time::timeout(bound, handle.stop_and_join())
        .await
        .expect("the stop did not return inside its bound");
    let took = started.elapsed();
    assert!(
        took < Duration::from_secs(3) && took * 3 < bound,
        "the stop took {took:?} of its {bound:?} bound"
    );

    match rig.egress.made() {
        Some(EgressReport::Unknown { message, cause }) => {
            assert_eq!(message.id, id);
            assert_eq!(cause, UnknownCause::ConnectionLost);
        }
        other => panic!("expected {id} unknown, got {other:?}"),
    }
    assert_eq!(rig.egress.made(), None, "a report after the outcome");
}

/// A stop that arrives while the supervisor is passing on the lost
/// connection's events, after the library has already made the new
/// connection, ends that connection with a DISCONNECT rather than dropping it,
/// so the broker does not publish the client's will.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_while_the_lost_events_are_passed_on_disconnects() {
    broker_gate!();
    let mut rig = Rig::start_inbound("stoplost").await;
    rig.hold_supervisor().await;
    rig.relay.arm(Cut {
        dir: Dir::BrokerToClient,
        ptype: PUBLISH,
        after: true,
    });
    rig.publish_inbound(0, b"m1").await;
    let lost = rig
        .relay
        .wait_for(10, "the cut after the PUBLISH", |p| {
            p.fate == Fate::CutAfter
        })
        .await
        .conn;
    rig.spawned.hold.arm(&inbound_topic(&rig.topic));
    rig.spawned.hold.release();
    assert_eq!(rig.delivered("the held message").await, b"hold");
    rig.spawned.hold.wait_held(10).await;
    let resumed = rig
        .relay
        .wait_for(10, "the CONNACK of the new connection", |p| {
            p.conn > lost && p.is(Dir::BrokerToClient, CONNACK)
        })
        .await
        .conn;

    let handle = rig.spawned.handle.clone();
    handle.stop();
    rig.spawned.hold.release();
    assert_eq!(rig.delivered("the message").await, b"m1");
    tokio::time::timeout(handle.stop_wait(), handle.stop_and_join())
        .await
        .expect("the stop did not return inside its bound");
    rig.relay
        .wait_for(10, "the DISCONNECT on the new connection", |p| {
            p.conn == resumed && p.is(Dir::ClientToBroker, DISCONNECT)
        })
        .await;
}

// ---------------------------------------------------------------------------
// A connection hung up during the TLS handshake
// ---------------------------------------------------------------------------

/// The relay accepts each dial and closes it before the TLS handshake ends:
/// the supervisor retries at its backoff rather than failing the client, and
/// the session resumes once the relay lets a dial through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hang_up_in_the_tls_handshake_is_retried() {
    broker_gate!();
    let mut rig = Rig::start("hangup").await;
    rig.relay.hang_up(true);
    rig.relay.arm(Cut {
        dir: Dir::ClientToBroker,
        ptype: PUBLISH,
        after: false,
    });
    let message = rig.message(1, b"m1", Duration::from_secs(60));
    let id = message.id;
    rig.egress.bus.append(message);
    let lost = rig
        .relay
        .wait_for(10, "the armed cut", |p| p.fate == Fate::CutBefore)
        .await
        .conn;
    let first = rig
        .relay
        .wait_for(15, "the first hung-up dial", |p| {
            p.fate == Fate::HungUp && p.conn > lost
        })
        .await
        .conn;
    rig.relay
        .wait_for(15, "a second hung-up dial, after a backoff", |p| {
            p.fate == Fate::HungUp && p.conn > first
        })
        .await;
    assert_ne!(
        rig.health().await,
        ConnectorHealthLabel::Failed,
        "the client failed at a hang-up"
    );
    rig.relay.hang_up(false);

    let report = rig.egress.next(30, "the message's outcome").await;
    assert_delivered(&report, id);
    rig.assert_replayed();
    rig.peer_receives(1, b"m1").await;
    rig.no_more_reports().await;
    assert_ne!(rig.health().await, ConnectorHealthLabel::Failed);
    rig.spawned.handle.stop();
}

// ---------------------------------------------------------------------------
// An unsolicited acknowledgement
// ---------------------------------------------------------------------------

/// With a QoS 1 message in flight, the relay injects a PUBACK for an
/// identifier the client does not hold: the supervisor reports the session
/// discarded, the next CONNECT is a clean start that finds no session, and the
/// in-flight message is sent again as a new publish and delivered. Returns
/// the number of the connection the clean start made.
async fn discard_once(rig: &mut Rig) -> usize {
    rig.relay.drop_packets(Dir::BrokerToClient, PUBACK);
    let message = rig.message(1, b"m1", Duration::from_secs(60));
    let id = message.id;
    rig.egress.bus.append(message);
    let dropped = rig
        .relay
        .wait_for(10, "the dropped PUBACK", |p| {
            p.is(Dir::BrokerToClient, PUBACK) && p.fate == Fate::Dropped
        })
        .await;
    rig.relay.clear_drops();
    let stray = dropped.pkid.expect("a PUBACK's identifier") + 1;
    rig.relay.inject(dropped.conn, puback(stray));

    let faults = rig.wait_faults(1, "the discarded session").await;
    assert_eq!(faults, [SessionFault::SessionDiscarded { pkid: stray }]);
    let report = rig.egress.next(20, "the message's outcome").await;
    assert_delivered(&report, id);

    let cured = rig.next_accepted(dropped.conn);
    let (connect, connack) = rig.handshake(cured);
    assert_eq!(connect.clean_start, Some(true), "conn{cured}'s CONNECT");
    assert_eq!(
        connack.session_present,
        Some(false),
        "conn{cured}'s CONNACK"
    );
    let resent: Vec<Packet> = rig
        .relay
        .forwarded(cured, Dir::ClientToBroker, PUBLISH)
        .into_iter()
        .filter(|p| p.topic.as_deref() == Some(&rig.topic))
        .collect();
    assert_eq!(
        resent.len(),
        1,
        "the in-flight message sent again on conn{cured}; relay log:\n{}",
        rig.relay.dump()
    );
    rig.wait_health(
        ConnectorHealthLabel::Connected,
        "the clean start never connected",
    )
    .await;
    cured
}

/// One unsolicited acknowledgement discards the session once; the connection
/// after the clean start resumes sessions again, and the client does not
/// reconnect on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsolicited_ack_discards_the_session_once() {
    broker_gate!();
    let mut rig = Rig::start("unsol1").await;
    let cured = discard_once(&mut rig).await;
    rig.peer_receives(2, b"m1").await;

    rig.relay.sever();
    rig.relay
        .wait_for(15, "a connection after the sever", |p| {
            p.conn > cured && p.is(Dir::BrokerToClient, CONNACK)
        })
        .await;
    rig.assert_resumed_after(cured);
    rig.wait_health(
        ConnectorHealthLabel::Connected,
        "never reconnected after the sever",
    )
    .await;
    rig.connects_stay_put(Duration::from_secs(3), "a CONNECT nobody provoked")
        .await;
    assert_eq!(
        rig.connects(),
        3,
        "CONNECTs: first, clean start, after the sever"
    );
    assert_eq!(rig.wait_faults(1, "the one fault").await.len(), 1);
    rig.no_more_reports().await;
    rig.spawned.handle.stop();
}

/// A second unsolicited acknowledgement, on the connection the clean start
/// made, stops the client for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_unsolicited_ack_gives_up() {
    broker_gate!();
    let mut rig = Rig::start("unsol2").await;
    let cured = discard_once(&mut rig).await;
    rig.relay.inject(cured, puback(1));

    let faults = rig.wait_faults(2, "the give-up").await;
    assert!(
        matches!(&faults[1], SessionFault::GaveUp { .. }),
        "expected the give-up, got {faults:?}"
    );
    assert_eq!(faults.len(), 2, "{faults:?}");
    rig.wait_health(ConnectorHealthLabel::Failed, "the client did not give up")
        .await;
    rig.connects_stay_put(Duration::from_secs(3), "a CONNECT after the give-up")
        .await;
    assert!(
        !rig.relay
            .log()
            .iter()
            .any(|p| p.conn > cured && p.is(Dir::ClientToBroker, CONNECT)),
        "a CONNECT after the give-up; relay log:\n{}",
        rig.relay.dump()
    );
}

// ---------------------------------------------------------------------------
// Sessions that end and start on purpose
// ---------------------------------------------------------------------------

impl Rig {
    /// The DISCONNECT the client sent on connection `conn`, once the relay
    /// has logged it: the stop returns when the packet is written, which can
    /// be before the relay has read it.
    async fn disconnect_on(&self, conn: usize) -> Packet {
        self.relay
            .wait_for(15, "the stop's DISCONNECT at the relay", |p| {
                p.conn == conn && p.is(Dir::ClientToBroker, DISCONNECT)
            })
            .await;
        let disconnects = self.relay.forwarded(conn, Dir::ClientToBroker, DISCONNECT);
        assert_eq!(
            disconnects.len(),
            1,
            "conn{conn}'s DISCONNECT; relay log:\n{}",
            self.relay.dump()
        );
        disconnects[0].clone()
    }

    /// Bring a new client up under the case's slug, through the relay, as
    /// `arriving` says, and return it with the number of the first connection
    /// after `after` that the broker accepted.
    async fn arrive_after(
        &self,
        after: usize,
        arriving: ArrivingFilters<'_>,
    ) -> (SpawnedClient, usize) {
        let arrived = spawn_client_arriving(self.spawned.handle.config.clone(), arriving).await;
        self.relay
            .wait_for(15, "the arriving client's CONNACK", |p| {
                p.conn > after && p.is(Dir::BrokerToClient, CONNACK)
            })
            .await;
        (arrived, self.next_accepted(after))
    }
}

/// A removed client's stop ends its session: its DISCONNECT carries Session
/// Expiry Interval 0, and a new client under the slug that asks to resume
/// finds no session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removed_clients_session_is_gone() {
    broker_gate!();
    let rig = Rig::start("end1").await;
    let first = rig.next_accepted(0);
    rig.spawned.handle.end_session_and_join().await;
    assert_eq!(
        rig.disconnect_on(first).await.session_expiry,
        Some(0),
        "the session-ending DISCONNECT; relay log:\n{}",
        rig.relay.dump()
    );

    let (next, conn) = rig
        .arrive_after(first, ArrivingFilters::Declared(Vec::new()))
        .await;
    let (connect, connack) = rig.handshake(conn);
    assert_eq!(
        connect.clean_start,
        Some(false),
        "the new client asks to resume"
    );
    assert_eq!(
        connack.session_present,
        Some(false),
        "the broker discarded the session; relay log:\n{}",
        rig.relay.dump()
    );
    next.handle.stop_and_join().await;
}

/// A stop that keeps the session sends the plain DISCONNECT, and a new client
/// under the slug gets the session back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kept_sessions_stop_leaves_it_to_resume() {
    broker_gate!();
    let rig = Rig::start("keep1").await;
    let first = rig.next_accepted(0);
    rig.spawned.handle.stop_and_join().await;
    assert_eq!(
        rig.disconnect_on(first).await.session_expiry,
        None,
        "the plain DISCONNECT; relay log:\n{}",
        rig.relay.dump()
    );

    let (next, conn) = rig
        .arrive_after(first, ArrivingFilters::Declared(Vec::new()))
        .await;
    let (connect, connack) = rig.handshake(conn);
    assert_eq!(
        connect.clean_start,
        Some(false),
        "the new client asks to resume"
    );
    assert_eq!(
        connack.session_present,
        Some(true),
        "the broker kept the session; relay log:\n{}",
        rig.relay.dump()
    );
    next.handle.stop_and_join().await;
}

/// A client a reload adds, on a slug whose session the broker kept with a
/// message queued in it, starts clean: its CONNECT has Clean Start, its
/// CONNACK reports no session, and the queued message is not sent to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reload_added_client_starts_clean() {
    broker_gate!();
    let rig = Rig::start_inbound("added1").await;
    let first = rig.next_accepted(0);
    rig.spawned.handle.stop_and_join().await;
    assert_eq!(rig.disconnect_on(first).await.session_expiry, None);
    rig.publish_inbound(1, b"queued").await;

    let (added, conn) = rig.arrive_after(first, ArrivingFilters::Added).await;
    let (connect, connack) = rig.handshake(conn);
    assert_eq!(
        connect.clean_start,
        Some(true),
        "the added client's CONNECT"
    );
    assert_eq!(
        connack.session_present,
        Some(false),
        "the added client's CONNACK; relay log:\n{}",
        rig.relay.dump()
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        rig.relay
            .forwarded(conn, Dir::BrokerToClient, PUBLISH)
            .is_empty(),
        "the old session's queue reached the added client; relay log:\n{}",
        rig.relay.dump()
    );
    added.handle.stop_and_join().await;
}

// ---------------------------------------------------------------------------
// Withdrawals made while the client is disconnected
// ---------------------------------------------------------------------------

impl Rig {
    /// Sever the client's connection with the relay refusing new ones, and
    /// wait for the supervisor to have seen the loss.
    async fn hold_off(&self) {
        self.relay.refuse(true).await;
        self.relay.sever();
        self.wait_health(
            ConnectorHealthLabel::Disconnected,
            "the client never saw its connection go",
        )
        .await;
    }

    /// The MQTT client id the case's session connects as.
    fn session_id(&self) -> String {
        session_client_id(&self.spawned.client_slug)
    }

    /// Withdraw the case's inbound filter from `client`, which has no
    /// connection, and assert it is answered as deferred.
    async fn withdraw_disconnected(&self, client: &SpawnedClient) {
        assert_eq!(
            client
                .svc
                .unsubscribe_filter(&client.client_slug, &inbound_topic(&self.topic))
                .await,
            Some(IngressUnsubscribeOutcome::DeferredDisconnected),
            "a withdrawal with no connection is deferred",
        );
    }

    /// Wait for the broker's log past `since` to record a CONNACK to the case's
    /// session reporting `session_present`, and whatever else `also` asks;
    /// answer with that window.
    async fn await_broker(
        &self,
        since: usize,
        session_present: bool,
        what: &str,
        also: impl Fn(&str) -> bool,
    ) -> String {
        let id = self.session_id();
        self.broker
            .wait_for_log(since, 15, what, |log| {
                log_records_connack(log, &id, session_present) && also(log)
            })
            .await
    }
}

/// A withdrawal made while the client is disconnected reaches the broker on
/// the connection that resumes the session, and the filter delivers no more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawal_while_disconnected_reaches_the_broker() {
    broker_gate!();
    let mut rig = Rig::start_inbound("wd1").await;
    let inbound = inbound_topic(&rig.topic);
    let lost = rig.next_accepted(0);
    rig.hold_off().await;
    rig.withdraw_disconnected(&rig.spawned).await;

    let since = rig.broker.log_len();
    rig.relay.refuse(false).await;
    let id = rig.session_id();
    rig.await_broker(
        since,
        true,
        &format!("the resumed connection never withdrew {inbound}"),
        |log| log_records_unsubscribe(log, &id, &inbound),
    )
    .await;
    let resumed = rig.assert_resumed_after(lost);
    assert_eq!(
        rig.relay
            .forwarded(resumed, Dir::ClientToBroker, UNSUBSCRIBE)
            .len(),
        1,
        "one UNSUBSCRIBE on the resumed connection; relay log:\n{}",
        rig.relay.dump()
    );

    rig.publish_inbound(1, b"after").await;
    rig.no_more_deliveries().await;
    rig.spawned.handle.stop_and_join().await;
}

/// A filter a new handle was registered with and withdrew before its first
/// connection — while an earlier client under the slug left a session holding
/// it — is withdrawn on that first connection, which finds the session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawal_before_the_first_connection_reaches_the_broker() {
    broker_gate!();
    let rig = Rig::start_inbound("wd2").await;
    let inbound = inbound_topic(&rig.topic);
    let first = rig.next_accepted(0);
    rig.spawned.handle.stop_and_join().await;

    rig.relay.refuse(true).await;
    let next =
        spawn_client_with_config(rig.spawned.handle.config.clone(), vec![inbound.clone()]).await;
    rig.withdraw_disconnected(&next).await;

    let since = rig.broker.log_len();
    rig.relay.refuse(false).await;
    let id = rig.session_id();
    rig.await_broker(
        since,
        true,
        &format!("the new handle's first connection never withdrew {inbound}"),
        |log| log_records_unsubscribe(log, &id, &inbound),
    )
    .await;
    let connected = rig.next_accepted(first);
    let (connect, _) = rig.handshake(connected);
    assert_eq!(
        connect.clean_start,
        Some(false),
        "the new handle asks to resume"
    );
    assert!(
        rig.relay
            .forwarded(connected, Dir::ClientToBroker, SUBSCRIBE)
            .is_empty(),
        "the withdrawn filter was subscribed again; relay log:\n{}",
        rig.relay.dump()
    );
    next.handle.stop_and_join().await;
}

/// A withdrawal made while disconnected, met by a connection whose CONNACK
/// reports no session, sends nothing: the broker holds nothing to withdraw.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_withdrawal_met_by_no_session_sends_nothing() {
    broker_gate!();
    let mut rig = Rig::start_inbound("wd3").await;
    let lost = rig.next_accepted(0);
    rig.stop_broker().await;
    rig.wait_health(
        ConnectorHealthLabel::Disconnected,
        "the client never saw its broker go",
    )
    .await;
    rig.withdraw_disconnected(&rig.spawned).await;

    let since = rig.broker.log_len();
    rig.restart_broker().await;
    rig.await_broker(
        since,
        false,
        "the client never reconnected to the restarted broker",
        |_| true,
    )
    .await;
    // The broker logs its CONNACK before the relay has passed it on.
    rig.relay
        .wait_for(15, "the reconnect's CONNACK at the relay", |p| {
            p.conn > lost && p.is(Dir::BrokerToClient, CONNACK)
        })
        .await;
    let reconnected = rig.next_accepted(lost);
    // Room for an UNSUBSCRIBE the settle would have sent behind the CONNACK.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        rig.relay
            .forwarded(reconnected, Dir::ClientToBroker, UNSUBSCRIBE)
            .is_empty(),
        "an UNSUBSCRIBE to a broker holding no session; relay log:\n{}",
        rig.relay.dump()
    );
    assert!(
        !log_records_unsubscribe(
            &rig.broker.log()[since..],
            &rig.session_id(),
            &inbound_topic(&rig.topic)
        ),
        "the broker received an UNSUBSCRIBE after the restart"
    );
    rig.spawned.handle.stop_and_join().await;
}

/// A filter withdrawn and subscribed again while disconnected is subscribed,
/// and not withdrawn, on the resumed connection, and delivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_filter_withdrawn_and_held_again_while_disconnected_is_only_subscribed() {
    broker_gate!();
    let mut rig = Rig::start_inbound("wd4").await;
    let inbound = inbound_topic(&rig.topic);
    let lost = rig.next_accepted(0);
    rig.hold_off().await;
    rig.withdraw_disconnected(&rig.spawned).await;
    assert_eq!(
        rig.spawned
            .svc
            .subscribe_filter(&rig.spawned.client_slug, inbound.clone())
            .await,
        Some(IngressSubscribeOutcome::DeferredDisconnected),
        "the filter held again with no connection",
    );

    let since = rig.broker.log_len();
    rig.relay.refuse(false).await;
    let id = rig.session_id();
    let window = rig
        .await_broker(
            since,
            true,
            &format!("the resumed connection never subscribed {inbound}"),
            |log| log_records_subscribe_at(log, &id, &inbound, 1),
        )
        .await;
    assert!(
        !log_records_unsubscribe(&window, &id, &inbound),
        "the filter held again was withdrawn: {window}"
    );
    let resumed = rig.assert_resumed_after(lost);
    assert!(
        rig.relay
            .forwarded(resumed, Dir::ClientToBroker, UNSUBSCRIBE)
            .is_empty(),
        "an UNSUBSCRIBE on the resumed connection; relay log:\n{}",
        rig.relay.dump()
    );
    wait_for_filter_acked(
        &rig.spawned.svc,
        &rig.spawned.client_slug,
        &inbound,
        10,
        "the filter held again was never granted",
    )
    .await;

    rig.publish_inbound(1, b"back").await;
    assert_eq!(rig.delivered("the filter held again").await, b"back");
    rig.spawned.handle.stop_and_join().await;
}

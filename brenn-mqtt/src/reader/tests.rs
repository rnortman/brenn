//! The reader against a scripted bus, over either an unpolled client (its
//! request channel observable, nothing ever answered) or a client polled
//! against an in-process broker that answers as each case tells it to.

use super::*;

use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;

use brenn_lib::mqtt::test_support::test_client_config;

use crate::connection::{install_connection, unpolled_client};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Unhanded(usize),
    Advance(u64),
    Abandon,
}

/// How long one bus call takes.
#[derive(Debug, Clone, Copy, Default)]
enum Pace {
    #[default]
    Instant,
    Takes(Duration),
    Never,
}

impl Pace {
    async fn wait(self) {
        match self {
            Self::Instant => {}
            Self::Takes(duration) => tokio::time::sleep(duration).await,
            Self::Never => std::future::pending().await,
        }
    }
}

#[derive(Default)]
struct BusState {
    entries: Vec<EgressItem>,
    /// The first sequence number the position still owes.
    next_owed: u64,
    calls: Vec<Call>,
    reports: Vec<EgressReport>,
    /// Calls and reports, in the order they completed.
    timeline: Vec<String>,
    advance_pace: Pace,
    report_pace: Pace,
}

struct ScriptBus {
    state: StdMutex<BusState>,
    wake: watch::Sender<u64>,
    depth: Option<usize>,
    /// An advance over this sequence number waits for the receiver.
    gate: tokio::sync::Mutex<Option<(u64, oneshot::Receiver<()>)>>,
}

fn item_seq(item: &EgressItem) -> u64 {
    match item {
        EgressItem::Message(message) => message.seq.0,
        EgressItem::PayloadGone { seq, .. } => seq.0,
    }
}

fn item_meta(item: &EgressItem) -> EgressMeta {
    match item {
        EgressItem::Message(message) => EgressMeta::of(message),
        EgressItem::PayloadGone { meta, .. } => meta.clone(),
    }
}

impl ScriptBus {
    fn new(depth: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            state: StdMutex::new(BusState {
                next_owed: 1,
                ..BusState::default()
            }),
            wake: watch::channel(0).0,
            depth,
            gate: tokio::sync::Mutex::new(None),
        })
    }

    /// Commit `item` without waking anyone.
    fn commit_quietly(&self, item: EgressItem) {
        let mut state = self.state.lock().unwrap();
        if let Some(last) = state.entries.last() {
            assert!(item_seq(last) < item_seq(&item), "commits ascend");
        }
        state.entries.push(item);
    }

    /// Commit `item` and wake the reader.
    fn commit(&self, item: EgressItem) {
        self.commit_quietly(item);
        self.wake.send_modify(|n| *n += 1);
    }

    fn calls(&self) -> Vec<Call> {
        self.state.lock().unwrap().calls.clone()
    }

    fn reads(&self) -> usize {
        self.calls()
            .iter()
            .filter(|call| matches!(call, Call::Unhanded(_)))
            .count()
    }

    fn advances(&self) -> Vec<u64> {
        self.calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Advance(seq) => Some(seq),
                _ => None,
            })
            .collect()
    }

    /// Every report but `Drained`, which a reader repeats on every idle pass.
    fn reports(&self) -> Vec<EgressReport> {
        self.state
            .lock()
            .unwrap()
            .reports
            .iter()
            .filter(|report| **report != EgressReport::Drained)
            .cloned()
            .collect()
    }

    /// [`Self::reports`] ordered by message id: notices resolve in any order.
    fn reports_by_id(&self) -> Vec<EgressReport> {
        let mut reports = self.reports();
        reports.sort_by_key(|report| match report {
            EgressReport::Handed(meta)
            | EgressReport::Delivered(meta)
            | EgressReport::PayloadGone(meta) => Some(meta.id),
            EgressReport::Expired { message, .. }
            | EgressReport::Rejected { message, .. }
            | EgressReport::Unknown { message, .. }
            | EgressReport::DroppedMessage { message, .. } => Some(message.id),
            EgressReport::Dropped { .. } | EgressReport::Drained => None,
        });
        reports
    }

    fn timeline(&self) -> Vec<String> {
        self.state.lock().unwrap().timeline.clone()
    }

    fn pace(&self, advance: Pace, report: Pace) {
        let mut state = self.state.lock().unwrap();
        state.advance_pace = advance;
        state.report_pace = report;
    }

    /// Every completed bus call, `Drained` reports included.
    fn completed_calls(&self) -> u64 {
        let state = self.state.lock().unwrap();
        (state.calls.len() + state.reports.len()) as u64
    }

    fn drained(&self) -> bool {
        self.state
            .lock()
            .unwrap()
            .reports
            .contains(&EgressReport::Drained)
    }
}

#[async_trait]
impl EgressBus for ScriptBus {
    async fn unhanded(&self, limit: usize) -> Vec<EgressItem> {
        let mut state = self.state.lock().unwrap();
        state.calls.push(Call::Unhanded(limit));
        let next_owed = state.next_owed;
        state
            .entries
            .iter()
            .filter(|item| item_seq(item) >= next_owed)
            .take(limit)
            .cloned()
            .collect()
    }

    async fn advance(&self, seq: MessageSeq) {
        let gate = {
            let mut gate = self.gate.lock().await;
            match gate.take() {
                Some((gated, rx)) if gated == seq.0 => Some(rx),
                other => {
                    *gate = other;
                    None
                }
            }
        };
        if let Some(rx) = gate {
            rx.await.unwrap();
        }
        let pace = self.state.lock().unwrap().advance_pace;
        pace.wait().await;
        let mut state = self.state.lock().unwrap();
        state.calls.push(Call::Advance(seq.0));
        state.timeline.push(format!("advance {}", seq.0));
        state.next_owed = seq.0 + 1;
    }

    async fn abandon(&self) -> Abandoned {
        let mut state = self.state.lock().unwrap();
        state.calls.push(Call::Abandon);
        let next_owed = state.next_owed;
        let unhanded: Vec<EgressMeta> = state
            .entries
            .iter()
            .filter(|item| item_seq(item) >= next_owed)
            .map(item_meta)
            .collect();
        if let Some(last) = state.entries.last() {
            state.next_owed = state.next_owed.max(item_seq(last) + 1);
        }
        Abandoned {
            count: unhanded.len() as u64,
            named: unhanded
                .into_iter()
                .filter(|meta| meta.status_to.is_some())
                .collect(),
        }
    }

    fn wakes(&self) -> watch::Receiver<u64> {
        self.wake.subscribe()
    }

    fn window_depth(&self) -> Option<usize> {
        self.depth
    }

    async fn report(&self, report: EgressReport) {
        let pace = self.state.lock().unwrap().report_pace;
        pace.wait().await;
        let mut state = self.state.lock().unwrap();
        if report != EgressReport::Drained {
            state
                .timeline
                .push(format!("report {}", report_label(&report)));
        }
        state.reports.push(report);
    }
}

fn message(seq: u64, qos: u8, expires: MqttExpiry) -> EgressMessage {
    EgressMessage {
        seq: MessageSeq(seq),
        id: Uuid::from_u128(u128::from(seq)),
        sender: "app:alice".to_string(),
        publish: MqttPublishTransport {
            client_slug: "home".to_string(),
            topic: format!("lights/{seq}"),
            qos,
            retain: false,
            content_type: None,
            expires,
            status_to: None,
        },
        payload: Bytes::from(format!("m{seq}")),
    }
}

const STATUS_TO: &str = "brenn:light-status";

/// [`message`], naming [`STATUS_TO`].
fn named(seq: u64, qos: u8, expires: MqttExpiry) -> EgressMessage {
    let mut message = message(seq, qos, expires);
    message.publish.status_to = Some(STATUS_TO.to_string());
    message
}

fn named_item(seq: u64, qos: u8) -> EgressItem {
    EgressItem::Message(named(seq, qos, MqttExpiry::Never))
}

fn named_meta(seq: u64, qos: u8) -> EgressMeta {
    EgressMeta::of(&named(seq, qos, MqttExpiry::Never))
}

/// A report's kind and the sequence number of the message it concerns.
fn report_label(report: &EgressReport) -> String {
    let seq = |meta: &EgressMeta| meta.id.as_u128();
    match report {
        EgressReport::Handed(meta) => format!("handed {}", seq(meta)),
        EgressReport::Delivered(meta) => format!("delivered {}", seq(meta)),
        EgressReport::Rejected { message, .. } => format!("rejected {}", seq(message)),
        EgressReport::Expired { message, .. } => format!("expired {}", seq(message)),
        EgressReport::Unknown { message, .. } => format!("unknown {}", seq(message)),
        EgressReport::Dropped { count, .. } => format!("dropped x{count}"),
        EgressReport::DroppedMessage { message, .. } => format!("dropped {}", seq(message)),
        EgressReport::PayloadGone(meta) => format!("gone {}", seq(meta)),
        EgressReport::Drained => "drained".to_string(),
    }
}

fn item(seq: u64, qos: u8) -> EgressItem {
    EgressItem::Message(message(seq, qos, MqttExpiry::Never))
}

fn meta(seq: u64, qos: u8) -> EgressMeta {
    EgressMeta::of(&message(seq, qos, MqttExpiry::Never))
}

fn expired(message: EgressMeta, sent: Sent) -> EgressReport {
    EgressReport::Expired { message, sent }
}

fn make_handle() -> Arc<MqttClientHandle> {
    let (tx, _rx) = watch::channel(false);
    MqttClientHandle::new(Arc::new(test_client_config("home")), vec![], tx)
}

/// Record a supervisor that exits when the handle is stopped.
async fn fake_supervisor(handle: &Arc<MqttClientHandle>) {
    let mut stop = handle.stop_tx.subscribe();
    let join = tokio::spawn(async move {
        stop.wait_for(|stopped| *stopped).await.unwrap();
    });
    handle.set_supervisor(join).await;
}

/// Install an unpolled client with request-channel room for `capacity`.
async fn install_unpolled(
    handle: &MqttClientHandle,
    capacity: usize,
    max_packet_size: Option<u32>,
    receive_max: Option<u16>,
) -> rumqttc::EventLoop {
    let (client, eventloop) = unpolled_client(capacity);
    install_connection(handle, client, max_packet_size, receive_max).await;
    eventloop
}

/// Do what the supervisor does at a loss: clear the cell, then let the
/// connection's event loop go.
async fn lose_connection(handle: &MqttClientHandle, eventloop: rumqttc::EventLoop) {
    *handle.client.lock().await = None;
    drop(eventloop);
}

fn queued(eventloop: &mut rumqttc::EventLoop) -> usize {
    eventloop.clean();
    eventloop.pending_len()
}

async fn until(what: &str, mut reached: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !reached() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Long enough for a woken reader to have run its pass.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[derive(Clone)]
enum Answer {
    Success,
    Reject(&'static str),
    /// QoS 1: no PUBACK until [`FakeBroker::release`].
    Withhold,
}

struct FakeBroker {
    received: Arc<StdMutex<Vec<rumqttc::Publish>>>,
    release: mpsc::UnboundedSender<()>,
}

impl FakeBroker {
    fn received(&self) -> Vec<rumqttc::Publish> {
        self.received.lock().unwrap().clone()
    }

    fn topics(&self) -> Vec<String> {
        self.received()
            .iter()
            .map(|p| String::from_utf8(p.topic.to_vec()).unwrap())
            .collect()
    }

    /// Acknowledge every withheld QoS 1 publish with success.
    fn release(&self) {
        self.release.send(()).unwrap();
    }
}

async fn write_packet(stream: &mut DuplexStream, write: impl FnOnce(&mut BytesMut)) {
    let mut out = BytesMut::new();
    write(&mut out);
    stream.write_all(&out).await.unwrap();
}

async fn serve(
    mut stream: DuplexStream,
    received: Arc<StdMutex<Vec<rumqttc::Publish>>>,
    answer: Answer,
    mut release: mpsc::UnboundedReceiver<()>,
) {
    use rumqttc::{
        ConnAck, ConnectReturnCode, Packet, PingResp, PubAck, PubAckProperties, PubAckReason,
        PubComp, PubCompReason, PubRec, PubRecProperties, PubRecReason,
    };
    let mut buf = BytesMut::new();
    let mut withheld = Vec::new();
    loop {
        match Packet::read(&mut buf, None) {
            Ok(Packet::Connect(..)) => {
                let connack = ConnAck {
                    session_present: false,
                    code: ConnectReturnCode::Success,
                    properties: None,
                };
                write_packet(&mut stream, |out| {
                    connack.write(out).unwrap();
                })
                .await;
            }
            Ok(Packet::Publish(publish)) => {
                received.lock().unwrap().push(publish.clone());
                let pkid = publish.pkid;
                match (publish.qos, answer.clone()) {
                    (rumqttc::mqttbytes::QoS::AtMostOnce, _) => {}
                    (rumqttc::mqttbytes::QoS::AtLeastOnce, Answer::Withhold) => withheld.push(pkid),
                    (rumqttc::mqttbytes::QoS::AtLeastOnce, answer) => {
                        let ack = match answer {
                            Answer::Reject(reason) => PubAck {
                                pkid,
                                reason: PubAckReason::NotAuthorized,
                                properties: Some(PubAckProperties {
                                    reason_string: Some(reason.to_string()),
                                    user_properties: vec![],
                                }),
                            },
                            _ => PubAck {
                                pkid,
                                reason: PubAckReason::Success,
                                properties: None,
                            },
                        };
                        write_packet(&mut stream, |out| {
                            ack.write(out).unwrap();
                        })
                        .await;
                    }
                    (rumqttc::mqttbytes::QoS::ExactlyOnce, answer) => {
                        let rec = match answer {
                            Answer::Reject(reason) => PubRec {
                                pkid,
                                reason: PubRecReason::NotAuthorized,
                                properties: Some(PubRecProperties {
                                    reason_string: Some(reason.to_string()),
                                    user_properties: vec![],
                                }),
                            },
                            _ => PubRec {
                                pkid,
                                reason: PubRecReason::Success,
                                properties: None,
                            },
                        };
                        write_packet(&mut stream, |out| {
                            rec.write(out).unwrap();
                        })
                        .await;
                    }
                }
            }
            Ok(Packet::PubRel(rel)) => {
                let comp = PubComp {
                    pkid: rel.pkid,
                    reason: PubCompReason::Success,
                    properties: None,
                };
                write_packet(&mut stream, |out| {
                    comp.write(out).unwrap();
                })
                .await;
            }
            Ok(Packet::PingReq(_)) => {
                write_packet(&mut stream, |out| {
                    PingResp::write(out).unwrap();
                })
                .await;
            }
            Ok(Packet::Disconnect(_)) => return,
            Ok(other) => panic!("the in-process broker does not expect {other:?}"),
            Err(rumqttc::mqttbytes::Error::InsufficientBytes(_)) => {
                tokio::select! {
                    read = stream.read_buf(&mut buf) => {
                        if read.unwrap() == 0 {
                            return;
                        }
                    }
                    Some(()) = release.recv() => {
                        for pkid in withheld.drain(..) {
                            let ack = PubAck {
                                pkid,
                                reason: PubAckReason::Success,
                                properties: None,
                            };
                            write_packet(&mut stream, |out| {
                                ack.write(out).unwrap();
                            })
                            .await;
                        }
                    }
                }
            }
            Err(e) => panic!("the in-process broker could not read a packet: {e:?}"),
        }
    }
}

/// Connect `handle` to a fresh in-process broker that answers QoS 1/2 with
/// `answer`, with the connection stating `receive_max`.
async fn connect_fake(
    handle: &Arc<MqttClientHandle>,
    receive_max: Option<u16>,
    answer: Answer,
) -> FakeBroker {
    use rumqttc::{AsyncClient, Event, MqttOptions, Packet};

    let (client_end, broker_end) = tokio::io::duplex(1 << 16);
    let client_end = Arc::new(StdMutex::new(Some(client_end)));
    let mut opts = MqttOptions::new("test-reader", ("127.0.0.1", 1));
    opts.set_socket_connector(move |_host, _network| {
        let stream = client_end.lock().unwrap().take();
        async move {
            stream.ok_or_else(|| std::io::Error::other("the in-process broker accepts once"))
        }
    });
    let (client, mut eventloop) = AsyncClient::builder(opts).capacity(64).build();

    let received = Arc::new(StdMutex::new(Vec::new()));
    let (release_tx, release_rx) = mpsc::unbounded_channel();
    tokio::spawn(serve(broker_end, Arc::clone(&received), answer, release_rx));

    loop {
        match eventloop.poll().await.unwrap() {
            Event::Incoming(Packet::ConnAck(_)) => break,
            _ => continue,
        }
    }
    install_connection(handle, client, None, receive_max).await;
    let polled = Arc::clone(handle);
    tokio::spawn(async move {
        while let Ok(event) = eventloop.poll().await {
            if let Event::Outgoing(_) = event {
                polled.egress_wake.notify_one();
            }
        }
    });
    FakeBroker {
        received,
        release: release_tx,
    }
}

#[test]
fn expiry_interval_rounds_the_remainder_up_and_never_sends_none() {
    let now = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let at = |ms: i64| MqttExpiry::At(now + TimeDelta::milliseconds(ms));
    assert_eq!(expiry_interval(&MqttExpiry::Never, now), Some(None));
    assert_eq!(expiry_interval(&at(10_000), now), Some(Some(10)));
    assert_eq!(expiry_interval(&at(10_001), now), Some(Some(11)));
    assert_eq!(expiry_interval(&at(1), now), Some(Some(1)));
    assert_eq!(
        expiry_interval(&at(0), now),
        None,
        "at the instant: expired"
    );
    assert_eq!(expiry_interval(&at(-1), now), None);
    assert_eq!(
        expiry_interval(
            &MqttExpiry::At(now + TimeDelta::seconds(i64::from(u32::MAX) + 10)),
            now
        ),
        Some(Some(u32::MAX)),
    );
}

#[test]
fn publish_properties_carry_the_content_type_and_expiry() {
    assert_eq!(publish_properties(None, None), None);
    let props = publish_properties(None, Some(7)).unwrap();
    assert_eq!(props.message_expiry_interval, Some(7));
    assert_eq!(props.content_type, None);
    let props = publish_properties(Some("application/json".to_string()), None).unwrap();
    assert_eq!(props.content_type.as_deref(), Some("application/json"));
    assert_eq!(props.message_expiry_interval, None);
}

/// Messages go out in channel order with their topic, QoS, retain flag,
/// content type and remaining lifetime, and each is reported delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messages_go_out_in_channel_order_with_their_options() {
    let handle = make_handle();
    let broker = connect_fake(&handle, None, Answer::Success).await;
    let bus = ScriptBus::new(Some(256));
    let mut first = message(1, 0, MqttExpiry::Never);
    first.publish.retain = true;
    first.publish.content_type = Some("text/plain".to_string());
    bus.commit_quietly(EgressItem::Message(first));
    let lifetime = Utc::now() + TimeDelta::seconds(600);
    bus.commit_quietly(EgressItem::Message(message(2, 1, MqttExpiry::At(lifetime))));
    bus.commit_quietly(item(3, 2));
    start_egress_reader(&handle, bus.clone());

    until("three deliveries", || bus.reports().len() == 3).await;
    assert_eq!(
        bus.reports_by_id(),
        vec![
            EgressReport::Delivered(meta(1, 0)),
            EgressReport::Delivered(meta(2, 1)),
            EgressReport::Delivered(meta(3, 2)),
        ]
    );
    assert_eq!(broker.topics(), ["lights/1", "lights/2", "lights/3"]);
    let received = broker.received();
    assert!(received[0].retain);
    assert_eq!(received[0].qos, rumqttc::mqttbytes::QoS::AtMostOnce);
    let props = received[0].properties.as_ref().unwrap();
    assert_eq!(props.content_type.as_deref(), Some("text/plain"));
    assert_eq!(props.message_expiry_interval, None, "Never sends no expiry");
    let expiry = received[1]
        .properties
        .as_ref()
        .unwrap()
        .message_expiry_interval;
    assert!(
        matches!(expiry, Some(s) if (590..=600).contains(&s)),
        "the remaining lifetime is sent: {expiry:?}"
    );
    assert_eq!(received[1].qos, rumqttc::mqttbytes::QoS::AtLeastOnce);
    assert_eq!(received[2].qos, rumqttc::mqttbytes::QoS::ExactlyOnce);
    assert_eq!(received[2].properties, None);
    assert_eq!(received[2].payload, Bytes::from("m3"));
    until("drained", || bus.drained()).await;
}

/// A broker's failure answer is reported rejected with the broker's words
/// and its reason code.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_rejection_is_reported_with_its_reason() {
    let handle = make_handle();
    let _broker = connect_fake(&handle, None, Answer::Reject("topic denied")).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(item(1, 1));
    bus.commit_quietly(item(2, 2));
    start_egress_reader(&handle, bus.clone());

    until("two rejections", || bus.reports().len() == 2).await;
    assert_eq!(
        bus.reports_by_id(),
        vec![
            EgressReport::Rejected {
                message: meta(1, 1),
                reason: "topic denied".to_string(),
                code: Some(135),
                earlier: Sent::Never,
            },
            EgressReport::Rejected {
                message: meta(2, 2),
                reason: "topic denied".to_string(),
                code: Some(135),
                earlier: Sent::Never,
            },
        ]
    );
}

/// The position moves past a message before the library is given it: while
/// the advance is held up, the broker sees nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cursor_moves_before_the_hand_off() {
    let handle = make_handle();
    let broker = connect_fake(&handle, None, Answer::Success).await;
    let bus = ScriptBus::new(Some(256));
    let (open, gate) = oneshot::channel();
    *bus.gate.lock().await = Some((1, gate));
    bus.commit_quietly(item(1, 1));
    start_egress_reader(&handle, bus.clone());

    until("the read", || bus.reads() == 1).await;
    settle().await;
    assert!(
        broker.received().is_empty(),
        "nothing goes out before the advance"
    );
    assert!(bus.advances().is_empty());
    open.send(()).unwrap();
    until("the delivery", || bus.reports().len() == 1).await;
    assert_eq!(bus.advances(), [1]);
    assert_eq!(broker.topics(), ["lights/1"]);
}

/// The reader keeps at most `min(Receive Maximum, window depth)` outstanding,
/// and reads more as acknowledgements free room.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_flight_limit_is_the_smaller_of_receive_maximum_and_depth() {
    for (receive_max, depth, limit) in [
        (Some(2), Some(5), 2),
        (Some(10), Some(3), 3),
        (Some(4), None, 4),
    ] {
        let handle = make_handle();
        let broker = connect_fake(&handle, receive_max, Answer::Withhold).await;
        let bus = ScriptBus::new(depth);
        for seq in 1..=6 {
            bus.commit_quietly(item(seq, 1));
        }
        start_egress_reader(&handle, bus.clone());

        until("the first window", || broker.received().len() == limit).await;
        settle().await;
        assert_eq!(broker.received().len(), limit, "{receive_max:?}/{depth:?}");
        assert_eq!(bus.advances().len(), limit);
        assert!(
            bus.calls()
                .iter()
                .all(|call| !matches!(call, Call::Unhanded(n) if *n > limit)),
            "no read asks past the limit: {:?}",
            bus.calls()
        );

        broker.release();
        until("the next window", || {
            broker.received().len() == (2 * limit).min(6)
        })
        .await;
        assert_eq!(bus.reports().len(), limit);
        handle.stop();
    }
}

/// QoS 1 in flight at a loss is published again, first and in channel
/// order, on the next connection; nothing is handed while a notice of the
/// lost connection is unresolved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos1_lost_in_flight_goes_out_again_first_and_in_order() {
    let handle = make_handle();
    let mut old = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(item(1, 1));
    bus.commit_quietly(item(2, 1));
    start_egress_reader(&handle, bus.clone());
    until("two submits", || bus.advances().len() == 2).await;
    assert_eq!(queued(&mut old), 2);

    // A new connection while the old one's notices are still unresolved: the
    // old event loop is alive, so its publishes have no answer yet.
    let old_client = handle.client.lock().await.take().unwrap();
    let broker = connect_fake(&handle, None, Answer::Success).await;
    bus.commit(item(3, 1));
    settle().await;
    assert_eq!(
        bus.reads(),
        1,
        "nothing is read while an older epoch is unresolved"
    );
    assert!(broker.received().is_empty());

    drop(old_client);
    drop(old);
    until("three deliveries", || bus.reports().len() == 3).await;
    assert_eq!(broker.topics(), ["lights/1", "lights/2", "lights/3"]);
    assert_eq!(
        bus.advances(),
        [1, 2, 3],
        "a resubmit does not advance again"
    );
}

/// A connection installed while a pass is under way takes nothing from that
/// pass: the message the pass was about to submit is held, and goes out on
/// the new connection behind the lost connection's QoS 1 message, in channel
/// order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_installed_mid_pass_takes_nothing_from_it() {
    let handle = make_handle();
    let mut old = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let (open, gate) = oneshot::channel();
    *bus.gate.lock().await = Some((2, gate));
    for seq in 1..=3 {
        bus.commit_quietly(item(seq, 1));
    }
    start_egress_reader(&handle, bus.clone());
    until("the first submit", || queued(&mut old) == 1).await;
    settle().await;
    assert_eq!(bus.advances(), [1], "the pass waits on the second advance");

    // The connection is lost and a new one installed while the pass waits.
    // The old event loop is alive, so the first message's notice has no
    // answer yet.
    let old_client = handle.client.lock().await.take().unwrap();
    let broker = connect_fake(&handle, None, Answer::Success).await;
    open.send(()).unwrap();
    until("the second advance", || bus.advances().len() == 2).await;
    settle().await;
    assert!(
        broker.received().is_empty(),
        "nothing goes out on the new connection while the old one's notice is unresolved"
    );

    drop(old_client);
    drop(old);
    until("three deliveries", || bus.reports().len() == 3).await;
    assert_eq!(broker.topics(), ["lights/1", "lights/2", "lights/3"]);
    assert_eq!(bus.advances(), [1, 2, 3]);
}

/// A full read whose messages all expired leaves room, and the reader reads
/// on from the same wake until it reaches the live message behind them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backlog_of_expired_messages_is_walked_from_one_wake() {
    let handle = make_handle();
    let mut eventloop = install_unpolled(&handle, 64, None, Some(2)).await;
    let bus = ScriptBus::new(Some(256));
    let past = Utc::now() - TimeDelta::seconds(1);
    for seq in 1..=4 {
        bus.commit_quietly(EgressItem::Message(message(seq, 1, MqttExpiry::At(past))));
    }
    bus.commit_quietly(item(5, 1));
    start_egress_reader(&handle, bus.clone());
    until("the live submit", || queued(&mut eventloop) == 1).await;
    assert_eq!(bus.advances(), [1, 2, 3, 4, 5]);
    assert_eq!(
        bus.reports(),
        (1..=4)
            .map(|seq| expired(meta(seq, 1), Sent::Never))
            .collect::<Vec<_>>()
    );
    assert!(
        bus.calls()
            .iter()
            .all(|call| !matches!(call, Call::Unhanded(n) if *n > 2)),
        "every read keeps to the limit: {:?}",
        bus.calls()
    );
}

/// QoS 0 and QoS 2 in flight at a loss are reported unknown and not resent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qos0_and_qos2_lost_in_flight_are_unknown() {
    let handle = make_handle();
    let old = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(item(1, 0));
    bus.commit_quietly(item(2, 2));
    start_egress_reader(&handle, bus.clone());
    until("two submits", || bus.advances().len() == 2).await;

    lose_connection(&handle, old).await;
    until("two unknowns", || bus.reports().len() == 2).await;
    assert_eq!(
        bus.reports_by_id(),
        vec![
            EgressReport::Unknown {
                message: meta(1, 0),
                cause: UnknownCause::ConnectionLost
            },
            EgressReport::Unknown {
                message: meta(2, 2),
                cause: UnknownCause::ConnectionLost
            },
        ]
    );
    let broker = connect_fake(&handle, None, Answer::Success).await;
    settle().await;
    assert!(broker.received().is_empty(), "neither is resent");
    until("drained", || bus.drained()).await;
}

/// With no client the reader reads nothing; a channel wake only marks, and
/// the connection's install then reads once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_is_read_while_disconnected() {
    let handle = make_handle();
    let bus = ScriptBus::new(Some(256));
    start_egress_reader(&handle, bus.clone());
    bus.commit(item(1, 1));
    bus.commit(item(2, 1));
    settle().await;
    assert!(bus.calls().is_empty(), "{:?}", bus.calls());

    let mut eventloop = install_unpolled(&handle, 64, None, None).await;
    until("the hand-off", || bus.advances().len() == 2).await;
    assert_eq!(bus.reads(), 1);
    assert_eq!(queued(&mut eventloop), 2);
}

/// A full request channel holds the one message and leaves the rest of the
/// read in the channel. A room wake alone, with no channel wake, resubmits
/// the held message first and then reads the rest from the cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_submit_holds_one_message_and_a_room_wake_resumes() {
    let handle = make_handle();
    let mut eventloop = install_unpolled(&handle, 1, None, None).await;
    let bus = ScriptBus::new(Some(256));
    for seq in 1..=3 {
        bus.commit_quietly(item(seq, 1));
    }
    start_egress_reader(&handle, bus.clone());
    until("the first pass", || bus.advances().len() == 2).await;
    settle().await;
    assert_eq!(bus.advances(), [1, 2], "the third is not advanced over");
    assert_eq!(bus.reads(), 1);

    // Take the queued request off the channel, then wake for room.
    assert_eq!(queued(&mut eventloop), 1);
    handle.egress_wake.notify_one();
    until("the third advance", || bus.advances().len() == 3).await;
    assert_eq!(bus.reads(), 2, "the rest is read again from the cursor");
    assert_eq!(queued(&mut eventloop), 2, "the held message went out");

    handle.egress_wake.notify_one();
    until("the last submit", || queued(&mut eventloop) == 3).await;
    assert!(bus.reports().is_empty());
}

/// An expired message is advanced over, reported and never submitted, held
/// or read from the channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_message_is_reported_and_not_submitted() {
    let handle = make_handle();
    let mut eventloop = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let past = Utc::now() - TimeDelta::seconds(1);
    bus.commit_quietly(EgressItem::Message(message(1, 1, MqttExpiry::At(past))));
    bus.commit_quietly(item(2, 1));
    start_egress_reader(&handle, bus.clone());
    until("both advances", || bus.advances().len() == 2).await;
    assert_eq!(bus.reports(), vec![expired(meta(1, 1), Sent::Never)]);
    assert_eq!(
        queued(&mut eventloop),
        1,
        "only the live message is submitted"
    );
}

/// A held QoS 1 message whose lifetime passes before the reconnect is
/// reported expired, as possibly sent, and not resubmitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_message_whose_lifetime_passes_is_expired() {
    let handle = make_handle();
    let old = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let soon = Utc::now() + TimeDelta::milliseconds(300);
    bus.commit_quietly(EgressItem::Message(message(1, 1, MqttExpiry::At(soon))));
    start_egress_reader(&handle, bus.clone());
    until("the submit", || bus.advances().len() == 1).await;
    lose_connection(&handle, old).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut eventloop = install_unpolled(&handle, 64, None, None).await;
    until("the expiry", || bus.reports().len() == 1).await;
    assert_eq!(bus.reports(), vec![expired(meta(1, 1), Sent::Possibly)]);
    assert_eq!(queued(&mut eventloop), 0);
}

/// A packet over the broker's Maximum Packet Size is reported rejected, with
/// no earlier copy, and never given to the library, at every QoS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversize_packet_is_rejected_unsent() {
    for qos in 0..=2 {
        let handle = make_handle();
        let mut eventloop = install_unpolled(&handle, 64, Some(12), None).await;
        let bus = ScriptBus::new(Some(256));
        bus.commit_quietly(item(1, qos));
        start_egress_reader(&handle, bus.clone());
        until("the report", || bus.reports().len() == 1).await;
        let reports = bus.reports();
        let EgressReport::Rejected {
            message,
            reason,
            code,
            earlier,
        } = &reports[0]
        else {
            panic!("expected a rejection, got {reports:?}");
        };
        assert_eq!(*message, meta(1, qos));
        assert_eq!(
            *code, None,
            "the host's own rejection carries no reason code"
        );
        assert_eq!(*earlier, Sent::Never, "QoS {qos}");
        assert!(
            reason.contains("Maximum Packet Size is 12 bytes"),
            "{reason}"
        );
        assert_eq!(bus.advances(), [1]);
        assert_eq!(queued(&mut eventloop), 0);
        until("drained", || bus.drained()).await;
    }
}

/// A row whose blob left retention is advanced over; it is reported, after
/// the advance, only when it names a `status_to`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gone_payload_is_reported_only_when_it_names_a_status_to() {
    let handle = make_handle();
    let mut eventloop = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(EgressItem::PayloadGone {
        seq: MessageSeq(1),
        meta: meta(1, 1),
    });
    bus.commit_quietly(EgressItem::PayloadGone {
        seq: MessageSeq(2),
        meta: named_meta(2, 1),
    });
    bus.commit_quietly(item(3, 1));
    start_egress_reader(&handle, bus.clone());
    until("three advances", || bus.advances().len() == 3).await;
    settle().await;
    assert_eq!(
        bus.reports(),
        vec![EgressReport::PayloadGone(named_meta(2, 1))]
    );
    assert_eq!(
        bus.timeline()[..3],
        ["advance 1", "advance 2", "report gone 2"]
    );
    assert_eq!(queued(&mut eventloop), 1);
}

/// Stopping the session reports in flight as unknown and held as dropped,
/// leaves the unhanded in the channel, and joins the reader before the
/// supervisor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_reports_in_flight_unknown_and_held_dropped() {
    let handle = make_handle();
    fake_supervisor(&handle).await;
    let _eventloop = install_unpolled(&handle, 1, None, None).await;
    let bus = ScriptBus::new(Some(256));
    for seq in 1..=3 {
        bus.commit_quietly(item(seq, 1));
    }
    start_egress_reader(&handle, bus.clone());
    until("the first pass", || bus.advances().len() == 2).await;

    handle.stop_and_join().await;
    assert_eq!(
        bus.reports(),
        vec![
            EgressReport::Unknown {
                message: meta(1, 1),
                cause: UnknownCause::SessionStopped
            },
            EgressReport::Dropped {
                cause: DropCause::SessionStopped,
                count: 1
            },
        ]
    );
    assert!(!bus.calls().contains(&Call::Abandon));
    assert_eq!(bus.advances(), [1, 2], "the third stays behind the cursor");
}

/// Retiring the reader reports as a stop does, abandons the rest of the
/// channel, reports what that skipped, and leaves the session running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retire_abandons_the_channel_and_leaves_the_session() {
    let handle = make_handle();
    fake_supervisor(&handle).await;
    let _eventloop = install_unpolled(&handle, 1, None, None).await;
    let bus = ScriptBus::new(Some(256));
    for seq in 1..=4 {
        bus.commit_quietly(item(seq, 1));
    }
    start_egress_reader(&handle, bus.clone());
    until("the first pass", || bus.advances().len() == 2).await;

    handle.retire_egress_reader().await;
    assert_eq!(
        bus.reports(),
        vec![
            EgressReport::Unknown {
                message: meta(1, 1),
                cause: UnknownCause::SessionStopped
            },
            EgressReport::Dropped {
                cause: DropCause::SessionStopped,
                count: 1
            },
            EgressReport::Dropped {
                cause: DropCause::ClientRemoved,
                count: 2
            },
        ]
    );
    assert!(!*handle.stop_tx.borrow(), "the session is not stopped");
    assert!(handle.client.lock().await.is_some());

    // The supervisor alone is joined now.
    handle.stop_and_join().await;
}

/// A message naming a `status_to` is reported handed, and the report is
/// complete, before the position moves past it and before the library has
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handed_is_reported_before_the_advance_and_the_hand_off() {
    let handle = make_handle();
    let mut eventloop = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let (open, gate) = oneshot::channel();
    *bus.gate.lock().await = Some((1, gate));
    bus.commit_quietly(named_item(1, 1));
    start_egress_reader(&handle, bus.clone());

    until("the handed report", || bus.reports().len() == 1).await;
    settle().await;
    assert_eq!(bus.reports(), vec![EgressReport::Handed(named_meta(1, 1))]);
    assert!(bus.advances().is_empty());
    assert_eq!(queued(&mut eventloop), 0, "the library has nothing yet");

    open.send(()).unwrap();
    until("the submit", || queued(&mut eventloop) == 1).await;
    assert_eq!(bus.timeline(), ["report handed 1", "advance 1"]);
}

/// A message whose lifetime passed before it was first reached is advanced
/// over and reported expired with no handed; a message with no `status_to`
/// is never reported handed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_message_is_never_handed() {
    let handle = make_handle();
    let mut eventloop = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let past = Utc::now() - TimeDelta::seconds(1);
    bus.commit_quietly(EgressItem::Message(named(1, 1, MqttExpiry::At(past))));
    bus.commit_quietly(named_item(2, 1));
    bus.commit_quietly(item(3, 1));
    start_egress_reader(&handle, bus.clone());
    until("three submits and expiries", || bus.advances().len() == 3).await;
    assert_eq!(
        bus.timeline(),
        [
            "advance 1",
            "report expired 1",
            "report handed 2",
            "advance 2",
            "advance 3"
        ]
    );
    assert_eq!(queued(&mut eventloop), 2);
}

/// A QoS 1 message lost in flight and resubmitted is handed once and
/// delivered once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resubmit_is_not_handed_again() {
    let handle = make_handle();
    let old = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(named_item(1, 1));
    start_egress_reader(&handle, bus.clone());
    until("the submit", || bus.advances().len() == 1).await;

    let old_client = handle.client.lock().await.take().unwrap();
    let broker = connect_fake(&handle, None, Answer::Success).await;
    drop(old_client);
    drop(old);
    until("the delivery", || bus.reports().len() == 2).await;
    settle().await;
    assert_eq!(broker.topics(), ["lights/1"]);
    assert_eq!(
        bus.reports(),
        vec![
            EgressReport::Handed(named_meta(1, 1)),
            EgressReport::Delivered(named_meta(1, 1)),
        ]
    );
}

/// QoS 0 and 2 naming a `status_to` and lost in flight: handed, then
/// unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_qos0_or_qos2_is_handed_then_unknown() {
    let handle = make_handle();
    let old = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(named_item(1, 0));
    bus.commit_quietly(named_item(2, 2));
    start_egress_reader(&handle, bus.clone());
    until("two submits", || bus.advances().len() == 2).await;

    lose_connection(&handle, old).await;
    until("two unknowns", || bus.reports().len() == 4).await;
    let unknown = |seq, qos| EgressReport::Unknown {
        message: named_meta(seq, qos),
        cause: UnknownCause::ConnectionLost,
    };
    assert_eq!(
        bus.reports_by_id(),
        vec![
            EgressReport::Handed(named_meta(1, 0)),
            unknown(1, 0),
            EgressReport::Handed(named_meta(2, 2)),
            unknown(2, 2),
        ]
    );
}

/// A handed QoS 1 message the library accepted, held across a reconnect past
/// its lifetime: handed, then expired as possibly sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_message_past_its_lifetime_is_handed_then_expired() {
    let handle = make_handle();
    let old = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let soon = Utc::now() + TimeDelta::milliseconds(300);
    let sent = named(1, 1, MqttExpiry::At(soon));
    bus.commit_quietly(EgressItem::Message(sent.clone()));
    start_egress_reader(&handle, bus.clone());
    until("the submit", || bus.advances().len() == 1).await;
    lose_connection(&handle, old).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut eventloop = install_unpolled(&handle, 64, None, None).await;
    until("the expiry", || bus.reports().len() == 2).await;
    assert_eq!(
        bus.reports(),
        vec![
            EgressReport::Handed(EgressMeta::of(&sent)),
            expired(EgressMeta::of(&sent), Sent::Possibly),
        ]
    );
    assert_eq!(queued(&mut eventloop), 0);
}

/// An oversize packet naming a `status_to`: handed, then rejected by the
/// host with no reason code.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversize_packet_is_handed_then_rejected_without_a_code() {
    let handle = make_handle();
    let _eventloop = install_unpolled(&handle, 64, Some(16), None).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(named_item(1, 1));
    start_egress_reader(&handle, bus.clone());
    until("both reports", || bus.reports().len() == 2).await;
    let reports = bus.reports();
    assert_eq!(reports[0], EgressReport::Handed(named_meta(1, 1)));
    assert!(
        matches!(
            &reports[1],
            EgressReport::Rejected { message, code: None, earlier: Sent::Never, .. }
                if *message == named_meta(1, 1)
        ),
        "{reports:?}"
    );
}

/// Leave the reader with message 1 in flight on a fresh connection whose
/// request channel holds one, messages 2 and 3 held, and 4 and 5 unread.
/// 1, 3 and 4 name a `status_to`.
async fn outstanding_mix(handle: &Arc<MqttClientHandle>) -> (Arc<ScriptBus>, rumqttc::EventLoop) {
    fake_supervisor(handle).await;
    let old = install_unpolled(handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(named_item(1, 1));
    bus.commit_quietly(item(2, 1));
    bus.commit_quietly(named_item(3, 1));
    start_egress_reader(handle, bus.clone());
    until("three submits", || bus.advances().len() == 3).await;
    lose_connection(handle, old).await;
    settle().await;
    bus.commit_quietly(named_item(4, 1));
    bus.commit_quietly(item(5, 1));
    let eventloop = install_unpolled(handle, 1, None, None).await;
    settle().await;
    assert_eq!(bus.reads(), 1, "the unread stay unread");
    (bus, eventloop)
}

fn handed_1_and_3() -> Vec<EgressReport> {
    vec![
        EgressReport::Handed(named_meta(1, 1)),
        EgressReport::Handed(named_meta(3, 1)),
    ]
}

fn stopped_1_2_3() -> Vec<EgressReport> {
    vec![
        EgressReport::Unknown {
            message: named_meta(1, 1),
            cause: UnknownCause::SessionStopped,
        },
        EgressReport::Dropped {
            cause: DropCause::SessionStopped,
            count: 1,
        },
        EgressReport::DroppedMessage {
            message: named_meta(3, 1),
            cause: DropCause::SessionStopped,
            sent: Sent::Possibly,
        },
    ]
}

/// A stop reports the held messages naming a `status_to` one by one and
/// counts the rest, and leaves the unread on the channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_reports_each_held_message_naming_a_status_to() {
    let handle = make_handle();
    let (bus, _eventloop) = outstanding_mix(&handle).await;
    handle.stop_and_join().await;
    let mut expected = handed_1_and_3();
    expected.extend(stopped_1_2_3());
    assert_eq!(bus.reports(), expected);
    assert_eq!(bus.advances(), [1, 2, 3]);
    assert_eq!(
        handle.egress_reader_progress.load(Ordering::Relaxed),
        bus.completed_calls(),
        "the counter moves once per completed bus call"
    );
}

/// A retire reports as a stop does, then splits what it abandoned the same
/// way under `ClientRemoved`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retire_reports_each_abandoned_message_naming_a_status_to() {
    let handle = make_handle();
    let (bus, _eventloop) = outstanding_mix(&handle).await;
    handle.retire_egress_reader().await;
    let mut expected = handed_1_and_3();
    expected.extend(stopped_1_2_3());
    expected.extend([
        EgressReport::Dropped {
            cause: DropCause::ClientRemoved,
            count: 1,
        },
        EgressReport::DroppedMessage {
            message: named_meta(4, 1),
            cause: DropCause::ClientRemoved,
            sent: Sent::Never,
        },
    ]);
    assert_eq!(bus.reports(), expected);
    assert_eq!(
        handle.egress_reader_progress.load(Ordering::Relaxed),
        bus.completed_calls(),
        "the counter moves once per completed bus call"
    );
    handle.stop_and_join().await;
}

/// Start a reader over messages 1 and 2, each naming a `status_to` at `qos`
/// and living for `lifetime`, on a client whose request channel holds one:
/// message 1 is accepted and stays in flight, and message 2's first submit
/// is given back, so the reader holds it never sent.
async fn second_given_back(
    handle: &Arc<MqttClientHandle>,
    qos: u8,
    lifetime: TimeDelta,
) -> (Arc<ScriptBus>, rumqttc::EventLoop) {
    fake_supervisor(handle).await;
    let eventloop = install_unpolled(handle, 1, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let expires = MqttExpiry::At(Utc::now() + lifetime);
    bus.commit_quietly(EgressItem::Message(named(1, qos, expires)));
    bus.commit_quietly(EgressItem::Message(named(2, qos, expires)));
    start_egress_reader(handle, bus.clone());
    until("both handed", || bus.advances().len() == 2).await;
    settle().await;
    (bus, eventloop)
}

fn named_meta_at(seq: u64, bus: &ScriptBus) -> EgressMeta {
    let state = bus.state.lock().unwrap();
    let item = state
        .entries
        .iter()
        .find(|item| item_seq(item) == seq)
        .expect("the message was committed");
    item_meta(item)
}

/// A message whose first submit a full request channel gave back is held
/// never sent: past its lifetime it is handed, then expired as never sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_given_back_by_a_full_channel_expires_never_sent() {
    for qos in 0..=2 {
        let handle = make_handle();
        let (bus, _eventloop) = second_given_back(&handle, qos, TimeDelta::milliseconds(300)).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        handle.egress_wake.notify_one();
        until("the expiry", || bus.reports().len() == 3).await;
        let second = named_meta_at(2, &bus);
        assert_eq!(
            bus.reports(),
            vec![
                EgressReport::Handed(named_meta_at(1, &bus)),
                EgressReport::Handed(second.clone()),
                expired(second, Sent::Never),
            ],
            "QoS {qos}"
        );
        handle.stop_and_join().await;
    }
}

/// The same held message, stopped: dropped as never sent; the accepted one
/// in flight is unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_given_back_by_a_full_channel_is_dropped_never_sent_at_a_stop() {
    for qos in 0..=2 {
        let handle = make_handle();
        let (bus, _eventloop) = second_given_back(&handle, qos, TimeDelta::seconds(600)).await;
        handle.stop_and_join().await;
        let first = named_meta_at(1, &bus);
        let second = named_meta_at(2, &bus);
        assert_eq!(
            bus.reports(),
            vec![
                EgressReport::Handed(first.clone()),
                EgressReport::Handed(second.clone()),
                EgressReport::Unknown {
                    message: first,
                    cause: UnknownCause::SessionStopped,
                },
                EgressReport::DroppedMessage {
                    message: second,
                    cause: DropCause::SessionStopped,
                    sent: Sent::Never,
                },
            ],
            "QoS {qos}"
        );
    }
}

/// Start a reader over message 1, naming a `status_to` at `qos` and living
/// for `lifetime`, and take the client away while the pass waits on its
/// advance: its submit finds no client, so the reader holds it never sent.
async fn given_back_for_no_client(
    handle: &Arc<MqttClientHandle>,
    qos: u8,
    lifetime: TimeDelta,
) -> Arc<ScriptBus> {
    fake_supervisor(handle).await;
    let eventloop = install_unpolled(handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let (open, gate) = oneshot::channel();
    *bus.gate.lock().await = Some((1, gate));
    let expires = MqttExpiry::At(Utc::now() + lifetime);
    bus.commit_quietly(EgressItem::Message(named(1, qos, expires)));
    start_egress_reader(handle, bus.clone());
    until("the handed report", || bus.reports().len() == 1).await;
    lose_connection(handle, eventloop).await;
    open.send(()).unwrap();
    until("the advance", || bus.advances().len() == 1).await;
    settle().await;
    bus
}

/// A message whose submit found no client is held never sent: past its
/// lifetime at the next connection it is expired as never sent, and at a
/// stop it is dropped as never sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_given_back_for_no_client_is_never_sent() {
    for qos in 0..=2 {
        let handle = make_handle();
        let bus = given_back_for_no_client(&handle, qos, TimeDelta::milliseconds(300)).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let mut eventloop = install_unpolled(&handle, 64, None, None).await;
        until("the expiry", || bus.reports().len() == 2).await;
        let only = named_meta_at(1, &bus);
        assert_eq!(
            bus.reports(),
            vec![
                EgressReport::Handed(only.clone()),
                expired(only, Sent::Never),
            ],
            "QoS {qos}"
        );
        assert_eq!(queued(&mut eventloop), 0);
        handle.stop_and_join().await;

        let handle = make_handle();
        let bus = given_back_for_no_client(&handle, qos, TimeDelta::seconds(600)).await;
        handle.stop_and_join().await;
        let only = named_meta_at(1, &bus);
        assert_eq!(
            bus.reports(),
            vec![
                EgressReport::Handed(only.clone()),
                EgressReport::DroppedMessage {
                    message: only,
                    cause: DropCause::SessionStopped,
                    sent: Sent::Never,
                },
            ],
            "QoS {qos}"
        );
    }
}

/// A QoS 1 message the library accepted stays possibly sent when a later
/// resubmit is given back: lost, given back by a full request channel on the
/// next connection, then past its lifetime, it is expired as possibly sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn possibly_sent_survives_a_resubmit_given_back() {
    let handle = make_handle();
    let old = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let soon = MqttExpiry::At(Utc::now() + TimeDelta::milliseconds(600));
    bus.commit_quietly(item(1, 1));
    bus.commit_quietly(EgressItem::Message(named(2, 1, soon)));
    start_egress_reader(&handle, bus.clone());
    until("both submits", || bus.advances().len() == 2).await;
    lose_connection(&handle, old).await;
    settle().await;

    // Room for one: message 1 is resubmitted, message 2 is given back.
    let mut eventloop = install_unpolled(&handle, 1, None, None).await;
    settle().await;
    assert_eq!(queued(&mut eventloop), 1, "only message 1 went out again");
    assert_eq!(bus.reports().len(), 1, "message 2 is still held");

    tokio::time::sleep(Duration::from_millis(600)).await;
    handle.egress_wake.notify_one();
    until("the expiry", || bus.reports().len() == 2).await;
    let second = named_meta_at(2, &bus);
    assert_eq!(
        bus.reports(),
        vec![
            EgressReport::Handed(second.clone()),
            expired(second, Sent::Possibly),
        ]
    );
}

/// [`named`] with a payload of `bytes` bytes.
fn named_sized(seq: u64, qos: u8, expires: MqttExpiry, bytes: usize) -> EgressMessage {
    let mut message = named(seq, qos, expires);
    message.payload = Bytes::from(vec![b'x'; bytes]);
    message
}

/// Leave the reader holding message 1, a QoS 1 message naming a `status_to`
/// that the library accepted on a connection now lost, and nothing else.
async fn possibly_sent_held(
    handle: &Arc<MqttClientHandle>,
    first: EgressMessage,
) -> Arc<ScriptBus> {
    fake_supervisor(handle).await;
    let old = install_unpolled(handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(EgressItem::Message(first));
    start_egress_reader(handle, bus.clone());
    until("the submit", || bus.advances().len() == 1).await;
    lose_connection(handle, old).await;
    settle().await;
    bus
}

/// A QoS 1 message accepted before its connection was lost, whose resend is
/// over the next connection's Maximum Packet Size, is rejected with an
/// earlier copy and is done; the message behind it goes out in the same pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversize_resend_is_rejected_with_an_earlier_copy() {
    let handle = make_handle();
    let big = named_sized(1, 1, MqttExpiry::Never, 200);
    let bus = possibly_sent_held(&handle, big.clone()).await;
    bus.commit(item(2, 1));
    settle().await;
    assert_eq!(bus.reads(), 1, "nothing is read while disconnected");

    let mut eventloop = install_unpolled(&handle, 64, Some(100), None).await;
    until("the rejection", || bus.reports().len() == 2).await;
    let reports = bus.reports();
    assert_eq!(reports[0], EgressReport::Handed(EgressMeta::of(&big)));
    let EgressReport::Rejected {
        message,
        reason,
        code,
        earlier,
    } = &reports[1]
    else {
        panic!("expected a rejection, got {reports:?}");
    };
    assert_eq!(*message, EgressMeta::of(&big));
    assert_eq!(*code, None);
    assert_eq!(*earlier, Sent::Possibly);
    assert!(
        reason.contains("Maximum Packet Size is 100 bytes"),
        "{reason}"
    );
    until("the message behind it", || queued(&mut eventloop) == 1).await;
    assert_eq!(bus.advances(), [1, 2]);

    handle.stop_and_join().await;
    assert_eq!(
        bus.reports()[2..],
        [EgressReport::Unknown {
            message: meta(2, 1),
            cause: UnknownCause::SessionStopped,
        }],
        "only message 2 was in flight, and nothing was held"
    );
}

/// A QoS 1 message accepted before its connection was lost, resent and
/// refused by the broker, is rejected with the broker's code and an earlier
/// copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resend_the_broker_refuses_is_rejected_with_an_earlier_copy() {
    let handle = make_handle();
    let first = named(1, 1, MqttExpiry::Never);
    let bus = possibly_sent_held(&handle, first.clone()).await;
    let broker = connect_fake(&handle, None, Answer::Reject("topic denied")).await;
    until("the rejection", || bus.reports().len() == 2).await;
    assert_eq!(
        bus.reports(),
        vec![
            EgressReport::Handed(EgressMeta::of(&first)),
            EgressReport::Rejected {
                message: EgressMeta::of(&first),
                reason: "topic denied".to_string(),
                code: Some(135),
                earlier: Sent::Possibly,
            },
        ]
    );
    assert_eq!(broker.topics(), ["lights/1"]);
    handle.stop_and_join().await;
}

/// A possibly-sent QoS 1 message whose resend a full request channel gave
/// back keeps its earlier copy: accepted on a later connection and refused by
/// the broker, it is rejected with an earlier copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_earlier_copy_survives_a_resend_given_back() {
    let handle = make_handle();
    fake_supervisor(&handle).await;
    let old = install_unpolled(&handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(item(1, 1));
    bus.commit_quietly(named_item(2, 1));
    start_egress_reader(&handle, bus.clone());
    until("both submits", || bus.advances().len() == 2).await;
    lose_connection(&handle, old).await;
    settle().await;

    // Room for one: message 1 is resent, message 2 is given back.
    let mut second = install_unpolled(&handle, 1, None, None).await;
    settle().await;
    assert_eq!(queued(&mut second), 1, "only message 1 went out again");
    lose_connection(&handle, second).await;
    settle().await;

    let _broker = connect_fake(&handle, None, Answer::Reject("topic denied")).await;
    until("both rejections", || bus.reports().len() == 3).await;
    assert_eq!(
        bus.reports_by_id(),
        vec![
            EgressReport::Rejected {
                message: meta(1, 1),
                reason: "topic denied".to_string(),
                code: Some(135),
                earlier: Sent::Possibly,
            },
            EgressReport::Handed(named_meta(2, 1)),
            EgressReport::Rejected {
                message: named_meta(2, 1),
                reason: "topic denied".to_string(),
                code: Some(135),
                earlier: Sent::Possibly,
            },
        ]
    );
    handle.stop_and_join().await;
}

/// A QoS 1 message accepted, lost, resent and accepted, then lost again and
/// held past its lifetime, is expired as possibly sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resend_lost_again_and_past_its_lifetime_is_expired_possibly_sent() {
    let handle = make_handle();
    let soon = MqttExpiry::At(Utc::now() + TimeDelta::milliseconds(800));
    let first = named(1, 1, soon);
    let bus = possibly_sent_held(&handle, first.clone()).await;
    let mut second = install_unpolled(&handle, 64, None, None).await;
    until("the resend", || queued(&mut second) == 1).await;
    lose_connection(&handle, second).await;
    tokio::time::sleep(Duration::from_millis(900)).await;

    let mut third = install_unpolled(&handle, 64, None, None).await;
    until("the expiry", || bus.reports().len() == 2).await;
    assert_eq!(
        bus.reports(),
        vec![
            EgressReport::Handed(EgressMeta::of(&first)),
            expired(EgressMeta::of(&first), Sent::Possibly),
        ]
    );
    assert_eq!(queued(&mut third), 0);
    handle.stop_and_join().await;
}

/// A message whose first submit found no client was never accepted: its
/// next submit, refused by the host's size check or by the broker, is
/// rejected with no earlier copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_given_back_then_refused_has_no_earlier_copy() {
    let handle = make_handle();
    let bus = given_back_for_no_client(&handle, 1, TimeDelta::seconds(600)).await;
    let _eventloop = install_unpolled(&handle, 64, Some(16), None).await;
    until("the rejection", || bus.reports().len() == 2).await;
    assert!(
        matches!(
            &bus.reports()[1],
            EgressReport::Rejected {
                code: None,
                earlier: Sent::Never,
                ..
            }
        ),
        "{:?}",
        bus.reports()
    );
    handle.stop_and_join().await;

    let handle = make_handle();
    let bus = given_back_for_no_client(&handle, 1, TimeDelta::seconds(600)).await;
    let _broker = connect_fake(&handle, None, Answer::Reject("topic denied")).await;
    until("the rejection", || bus.reports().len() == 2).await;
    let only = named_meta_at(1, &bus);
    assert_eq!(
        bus.reports()[1],
        EgressReport::Rejected {
            message: only,
            reason: "topic denied".to_string(),
            code: Some(135),
            earlier: Sent::Never,
        }
    );
    handle.stop_and_join().await;
}

/// What a slow bus call costs in the watchdog tests: four of them fill one
/// join window.
const SLOW: Duration = Duration::from_secs(5);

/// Messages enough that their reports outlast the join window many times.
const BACKLOG: u64 = 30;

/// A retire whose exit owes far more slow reports than one join window
/// holds joins: the reader keeps completing bus calls.
#[tokio::test(start_paused = true)]
async fn a_long_retire_joins_while_the_reader_progresses() {
    let handle = make_handle();
    let bus = ScriptBus::new(Some(256));
    for seq in 1..=BACKLOG {
        bus.commit_quietly(named_item(seq, 1));
    }
    bus.pace(Pace::Instant, Pace::Takes(SLOW));
    start_egress_reader(&handle, bus.clone());
    handle.retire_egress_reader().await;
    assert_eq!(bus.reports().len() as u64, BACKLOG);
    assert!(
        bus.reports().iter().all(|report| matches!(
            report,
            EgressReport::DroppedMessage {
                cause: DropCause::ClientRemoved,
                sent: Sent::Never,
                ..
            }
        )),
        "every abandoned message was never read, so never sent"
    );
    assert_eq!(
        handle.egress_reader_progress.load(Ordering::Relaxed),
        bus.completed_calls()
    );
}

/// A retire whose report never returns panics the join after one window.
#[tokio::test(start_paused = true)]
#[should_panic(expected = "neither exited nor completed one bus call within 20s")]
async fn a_retire_stuck_on_a_report_panics_the_join() {
    let handle = make_handle();
    let bus = ScriptBus::new(Some(256));
    bus.commit_quietly(named_item(1, 1));
    bus.pace(Pace::Instant, Pace::Never);
    start_egress_reader(&handle, bus.clone());
    handle.retire_egress_reader().await;
}

/// Commit [`BACKLOG`] QoS 1 messages naming no `status_to` and wait until the
/// reader has them all in flight.
async fn all_in_flight(handle: &Arc<MqttClientHandle>) -> (Arc<ScriptBus>, rumqttc::EventLoop) {
    fake_supervisor(handle).await;
    let eventloop = install_unpolled(handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    for seq in 1..=BACKLOG {
        bus.commit_quietly(item(seq, 1));
    }
    start_egress_reader(handle, bus.clone());
    until("every submit", || bus.advances().len() as u64 == BACKLOG).await;
    (bus, eventloop)
}

/// A stop whose exit owes far more slow reports than one join window holds
/// joins.
#[tokio::test(start_paused = true)]
async fn a_long_stop_joins_while_the_reader_progresses() {
    let handle = make_handle();
    let (bus, _eventloop) = all_in_flight(&handle).await;
    bus.pace(Pace::Instant, Pace::Takes(SLOW));
    handle.stop_and_join().await;
    assert_eq!(bus.reports().len() as u64, BACKLOG);
}

/// A stop whose report never returns panics the join after one window.
#[tokio::test(start_paused = true)]
#[should_panic(expected = "neither exited nor completed one bus call within 20s")]
async fn a_stop_stuck_on_a_report_panics_the_join() {
    let handle = make_handle();
    let (bus, _eventloop) = all_in_flight(&handle).await;
    bus.pace(Pace::Instant, Pace::Never);
    handle.stop_and_join().await;
}

/// Start a reader on a connected client over [`BACKLOG`] expired messages
/// naming a `status_to`, every advance and report paced by `pace`, and wait
/// until its pass over them has begun.
async fn expired_backlog(
    handle: &Arc<MqttClientHandle>,
    advance: Pace,
) -> (Arc<ScriptBus>, rumqttc::EventLoop) {
    fake_supervisor(handle).await;
    let eventloop = install_unpolled(handle, 64, None, None).await;
    let bus = ScriptBus::new(Some(256));
    let past = Utc::now() - TimeDelta::seconds(1);
    for seq in 1..=BACKLOG {
        bus.commit_quietly(EgressItem::Message(named(seq, 1, MqttExpiry::At(past))));
    }
    bus.pace(advance, Pace::Takes(SLOW));
    start_egress_reader(handle, bus.clone());
    until("the pass's read", || bus.reads() == 1).await;
    (bus, eventloop)
}

fn assert_all_expired(bus: &ScriptBus) {
    let reports = bus.reports();
    assert_eq!(reports.len() as u64, BACKLOG, "{reports:?}");
    assert!(
        reports.iter().all(|report| matches!(
            report,
            EgressReport::Expired {
                sent: Sent::Never,
                ..
            }
        )),
        "the pass gives its messages their own outcome: {reports:?}"
    );
}

/// A retire signalled during a pass that alone outlasts the join window many
/// times joins, and every message of the pass is reported expired.
#[tokio::test(start_paused = true)]
async fn a_retire_during_a_long_pass_joins() {
    let handle = make_handle();
    let (bus, _eventloop) = expired_backlog(&handle, Pace::Takes(SLOW)).await;
    handle.retire_egress_reader().await;
    assert_all_expired(&bus);
}

/// The same pass under a stop.
#[tokio::test(start_paused = true)]
async fn a_stop_during_a_long_pass_joins() {
    let handle = make_handle();
    let (bus, _eventloop) = expired_backlog(&handle, Pace::Takes(SLOW)).await;
    handle.stop_and_join().await;
    assert_all_expired(&bus);
}

/// A pass stuck on an advance panics the join after one window.
#[tokio::test(start_paused = true)]
#[should_panic(expected = "neither exited nor completed one bus call within 20s")]
async fn a_pass_stuck_on_an_advance_panics_the_join() {
    let handle = make_handle();
    let (_bus, _eventloop) = expired_backlog(&handle, Pace::Never).await;
    handle.retire_egress_reader().await;
}

#[tokio::test]
#[should_panic(expected = "start_egress_reader called twice")]
async fn a_second_reader_on_one_handle_panics() {
    let handle = make_handle();
    start_egress_reader(&handle, ScriptBus::new(Some(256)));
    start_egress_reader(&handle, ScriptBus::new(Some(256)));
}

#[tokio::test]
#[should_panic(expected = "no egress reader running")]
async fn retiring_with_no_reader_panics() {
    make_handle().retire_egress_reader().await;
}

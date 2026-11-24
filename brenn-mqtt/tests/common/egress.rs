//! The egress reader's side of a broker test: an in-memory egress channel the
//! reader reads, and the reports it makes collected for the test.

use std::sync::Arc;
use std::time::Duration;

use brenn_envelope::{MqttExpiry, MqttPublishTransport};
use brenn_messaging_store::store::MessageSeq;
use brenn_mqtt::MqttClientHandle;
use brenn_mqtt::reader::{EgressBus, EgressItem, EgressMessage, EgressReport, start_egress_reader};

/// An in-memory egress channel with one position, forwarding every report but
/// `Drained` to the test.
pub struct MemBus {
    entries: std::sync::Mutex<(Vec<EgressItem>, u64)>,
    wake: tokio::sync::watch::Sender<u64>,
    reports: tokio::sync::mpsc::UnboundedSender<EgressReport>,
}

impl MemBus {
    pub fn holding(
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
    pub fn append(&self, message: EgressMessage) {
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
pub struct Egress {
    client_slug: String,
    pub bus: Arc<MemBus>,
    reports: tokio::sync::mpsc::UnboundedReceiver<EgressReport>,
    next_seq: u64,
}

impl Egress {
    pub fn start(handle: &Arc<MqttClientHandle>) -> Self {
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
    pub fn message(&mut self, topic: &str, qos: u8, payload: &[u8]) -> EgressMessage {
        let message = egress_message(self.next_seq, &self.client_slug, topic, qos, payload);
        self.next_seq += 1;
        message
    }

    /// Commit `message` to the channel and return the reader's next report.
    pub async fn send(&mut self, message: EgressMessage) -> EgressReport {
        self.bus.append(message);
        self.next(15, "the egress report").await
    }

    /// The reader's next report, within `secs`.
    pub async fn next(&mut self, secs: u64, what: &str) -> EgressReport {
        next_report(&mut self.reports, secs, what).await
    }

    /// The next message for `topic` at `qos`, naming a `status_to` so the
    /// reader reports it handed before it gives it to the library.
    pub fn tracked(&mut self, topic: &str, qos: u8, payload: &[u8]) -> EgressMessage {
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
    pub async fn hand(&mut self, message: EgressMessage, sentinel_topic: &str) -> uuid::Uuid {
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
    pub fn made(&mut self) -> Option<EgressReport> {
        match self.reports.try_recv() {
            Ok(report) => Some(report),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => None,
            Err(e) => panic!("report channel: {e}"),
        }
    }

    /// Commit `message` and assert the reader reports it delivered.
    pub async fn deliver(&mut self, message: EgressMessage) {
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

pub fn egress_message(
    seq: u64,
    client: &str,
    topic: &str,
    qos: u8,
    payload: &[u8],
) -> EgressMessage {
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
            response_topic: None,
        },
        payload: bytes::Bytes::copy_from_slice(payload),
    }
}

/// The next report the reader makes, within `secs`.
pub async fn next_report(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<EgressReport>,
    secs: u64,
    what: &str,
) -> EgressReport {
    tokio::time::timeout(Duration::from_secs(secs), rx.recv())
        .await
        .unwrap_or_else(|_| panic!("{what}: no report within {secs}s"))
        .expect("report channel closed")
}

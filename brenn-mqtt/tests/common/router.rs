use std::sync::{Arc, Mutex};

use brenn_mqtt::InboundPayload;
use brenn_mqtt::service::{InboundRouting, MqttEventRouter, SessionFault};
use tokio::sync::{mpsc, watch};

/// A delivered MQTT message as seen by the test.
///
/// Mirrors the bridge-model `MqttEventRouter::deliver_inbound` signature
/// (`client_slug`, `topic`, `payload`, `qos`, `routing`). `qos` is the delivery
/// QoS of the inbound PUBLISH — the ingress live-subscribe tests assert it to
/// pin the broker-granted QoS behaviorally. `routing` is the subscriptions the
/// supervisor resolved the copy's Subscription Identifiers to.
#[derive(Debug, Clone)]
pub struct DeliveredMessage {
    pub client: String,
    pub topic: String,
    pub payload: InboundPayload,
    pub qos: u8,
    pub routing: InboundRouting,
}

/// Every session fault a router was told of, with the client it named, in
/// order.
pub type Faults = Arc<Mutex<Vec<(String, SessionFault)>>>;

/// A hold on the delivery of one topic, armed once per delivery: the
/// router's `deliver_inbound` for the armed topic blocks until the test
/// releases it, so the supervisor that called it polls nothing meanwhile.
pub struct Hold {
    topic: Mutex<Option<String>>,
    /// How many deliveries have blocked here.
    held: watch::Sender<u64>,
    /// How many of those have been let return.
    released: watch::Sender<u64>,
}

#[allow(dead_code, reason = "held by the kept-session suite only")]
impl Hold {
    fn new() -> Self {
        Self {
            topic: Mutex::new(None),
            held: watch::channel(0).0,
            released: watch::channel(0).0,
        }
    }

    /// Block the next delivery on `topic`.
    pub fn arm(&self, topic: &str) {
        let mut armed = self.topic.lock().unwrap();
        assert!(armed.is_none(), "Hold: armed twice");
        *armed = Some(topic.to_string());
    }

    /// Wait up to `secs` for a delivery to be blocked and not yet released.
    pub async fn wait_held(&self, secs: u64) {
        let released = *self.released.borrow();
        tokio::time::timeout(
            std::time::Duration::from_secs(secs),
            self.held.subscribe().wait_for(|held| *held > released),
        )
        .await
        .unwrap_or_else(|_| panic!("Hold: no delivery on the armed topic within {secs}s"))
        .expect("Hold: the sender lives in the hold");
    }

    /// Let the blocked delivery return.
    pub fn release(&self) {
        let held = *self.held.borrow();
        assert!(
            held > *self.released.borrow(),
            "Hold: released before anything was held"
        );
        self.released.send_replace(held);
    }

    /// Block here if `topic` is the armed one, until [`Hold::release`].
    async fn pass(&self, topic: &str) {
        {
            let mut armed = self.topic.lock().unwrap();
            if armed.as_deref() != Some(topic) {
                return;
            }
            *armed = None;
        }
        let mut released = self.released.subscribe();
        let mut this = 0;
        self.held.send_modify(|held| {
            *held += 1;
            this = *held;
        });
        released
            .wait_for(|released| *released >= this)
            .await
            .expect("Hold: the sender lives in the hold");
    }
}

/// Test implementation of [`MqttEventRouter`] that pushes every delivery onto
/// an unbounded channel and records every session fault. Tests hold the
/// matching `UnboundedReceiver` and assert via `recv().await` wrapped in
/// `tokio::time::timeout`. A delivery on a topic armed on its [`Hold`] blocks
/// until released.
pub struct CapturingRouter {
    tx: mpsc::UnboundedSender<DeliveredMessage>,
    faults: Faults,
    hold: Arc<Hold>,
}

impl CapturingRouter {
    /// Create a router + the receiver used by tests.
    pub fn new() -> (Self, mpsc::UnboundedReceiver<DeliveredMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                tx,
                faults: Faults::default(),
                hold: Arc::new(Hold::new()),
            },
            rx,
        )
    }

    /// The faults this router records.
    pub fn faults(&self) -> Faults {
        self.faults.clone()
    }

    /// The hold this router's deliveries pass through.
    pub fn hold(&self) -> Arc<Hold> {
        self.hold.clone()
    }
}

#[async_trait::async_trait]
impl MqttEventRouter for CapturingRouter {
    fn ready(&self) -> bool {
        true
    }

    async fn deliver_inbound(
        &self,
        client_slug: &str,
        topic: &str,
        payload: InboundPayload,
        qos: u8,
        _retained: bool,
        routing: InboundRouting,
    ) {
        self.hold.pass(topic).await;
        // This runs inside the detached ingress-supervisor task, so a panic here is
        // swallowed by the runtime rather than failing a test. Receiver-drop at
        // teardown (the broker may deliver a retained message between the test's last
        // assert and dropping `rx`) is benign; ignore the send result. Tests enforce
        // "every delivery is observed" via their own recv-based assertions.
        let _ = self.tx.send(DeliveredMessage {
            client: client_slug.to_string(),
            topic: topic.to_string(),
            payload,
            qos,
            routing,
        });
    }

    async fn session_fault(&self, client_slug: &str, fault: SessionFault) {
        self.faults
            .lock()
            .unwrap()
            .push((client_slug.to_string(), fault));
    }
}

use brenn_lib::mqtt::InboundPayload;
use brenn_lib::mqtt::service::MqttEventRouter;
use tokio::sync::mpsc;

/// A delivered MQTT message as seen by the test.
///
/// Mirrors the bridge-model `MqttEventRouter::deliver_inbound` signature
/// (`client_slug`, `topic`, `payload`, `qos`). `qos` is the delivery QoS of the
/// inbound PUBLISH — the ingress live-subscribe tests assert it to pin the
/// broker-granted QoS behaviorally.
#[derive(Debug, Clone)]
pub struct DeliveredMessage {
    pub client: String,
    pub topic: String,
    pub payload: InboundPayload,
    pub qos: u8,
}

/// Test implementation of [`MqttEventRouter`] that pushes every delivery onto
/// an unbounded channel. Tests hold the matching `UnboundedReceiver` and assert
/// via `recv().await` wrapped in `tokio::time::timeout`.
pub struct CapturingRouter {
    tx: mpsc::UnboundedSender<DeliveredMessage>,
}

impl CapturingRouter {
    /// Create a router + the receiver used by tests.
    pub fn new() -> (Self, mpsc::UnboundedReceiver<DeliveredMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }
}

#[async_trait::async_trait]
impl MqttEventRouter for CapturingRouter {
    async fn deliver_inbound(
        &self,
        client_slug: &str,
        topic: &str,
        payload: InboundPayload,
        qos: u8,
    ) {
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
        });
    }
}

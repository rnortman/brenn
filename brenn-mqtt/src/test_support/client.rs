//! Client-side helpers for a test that drives a real broker: a health poll and
//! a raw publisher that confirms its own PubAcks.
//!
//! These sit beside the broker harness because every suite that spawns one
//! needs the same two things — a way to wait for a brenn session to reach a
//! state, and a way to put a message on a topic from outside brenn and know the
//! broker took it.

use std::sync::atomic::{AtomicU64, Ordering};

use rumqttc::{AsyncClient, MqttOptions, Transport};
use tokio::sync::mpsc;

use crate::{MqttService, state::ConnectorHealthLabel};

/// Poll `svc.ingress_health(client_slug)` at 25ms intervals until the label is
/// in `accepted`, then return that label. Panics with `msg` if no accepted label
/// is seen within `timeout_secs`.
pub async fn wait_for_health(
    svc: &MqttService,
    client_slug: &str,
    accepted: &[ConnectorHealthLabel],
    timeout_secs: u64,
    msg: &str,
) -> ConnectorHealthLabel {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        let (label, _) = svc.ingress_health(client_slug).await;
        if accepted.contains(&label) {
            return label;
        }
        assert!(std::time::Instant::now() < deadline, "{msg}");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// A raw rumqttc client plus a receiver that yields one item per PubAck.
///
/// The connection is established (ConnAck received) before this returns, so a
/// publish issued afterwards cannot race the connect. Await one PubAck per QoS-1
/// publish with [`await_puback`] to know the broker processed it before
/// asserting on anything downstream.
///
/// # TEST-ONLY TLS note
///
/// Uses `Transport::tls(ca_pem, None, None)` with an IP-literal host. Acceptable
/// for loopback test brokers; MUST NOT be copied into production connection
/// code.
pub async fn direct_publisher_acked(
    broker_port: u16,
    ca_pem: Vec<u8>,
) -> (AsyncClient, mpsc::UnboundedReceiver<()>) {
    let client_id = format!("brenn-direct-acked-{}", unique_suffix());
    let mut opts = MqttOptions::new(client_id, ("127.0.0.1", broker_port));
    opts.set_clean_start(true);
    opts.set_transport(Transport::tls(ca_pem, None, None));
    let (client, mut eventloop) = AsyncClient::builder(opts).capacity(64).build();

    drain_until_incoming(
        &mut eventloop,
        |pkt| matches!(pkt, rumqttc::Incoming::ConnAck(_)),
        "direct_publisher_acked: ConnAck",
    )
    .await;

    let (ack_tx, ack_rx) = mpsc::unbounded_channel::<()>();
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Incoming::PubAck(_))) => {
                    // Receiver-dropped at teardown is benign; ignore the send
                    // result.
                    ack_tx.send(()).ok();
                }
                Ok(_) => {}
                // A mid-test transport drop stops PubAcks; surface it so a
                // downstream barrier timeout can be told apart from a code
                // regression.
                Err(e) => {
                    eprintln!("direct_publisher_acked: eventloop error: {e}");
                    return;
                }
            }
        }
    });

    (client, ack_rx)
}

/// Await one PubAck on `ack_rx` (3s cap). Panics on a closed channel or timeout,
/// naming `what`.
pub async fn await_puback(ack_rx: &mut mpsc::UnboundedReceiver<()>, what: &str) {
    match tokio::time::timeout(std::time::Duration::from_secs(3), ack_rx.recv()).await {
        Ok(Some(())) => {}
        Ok(None) => panic!("{what}: PubAck channel closed"),
        Err(_) => panic!("{what}: PubAck not received within 3s"),
    }
}

/// Poll `eventloop` until an incoming packet satisfies `wanted`, discarding
/// every other event. Panics on eventloop error or after 5s; `what` names the
/// awaited packet for diagnostics.
pub async fn drain_until_incoming<F>(eventloop: &mut rumqttc::EventLoop, mut wanted: F, what: &str)
where
    F: FnMut(&rumqttc::Incoming) -> bool,
{
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, eventloop.poll()).await {
            Ok(Ok(rumqttc::Event::Incoming(pkt))) if wanted(&pkt) => return,
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => panic!("{what}: eventloop error before {what}: {e}"),
            Err(_) => panic!("{what}: not received within 5s"),
        }
    }
}

/// A client-id suffix unique within this process and across concurrent
/// processes on one broker.
///
/// A broker takes over the session of a reconnecting client id, so two clients
/// sharing one would silently evict each other.
fn unique_suffix() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

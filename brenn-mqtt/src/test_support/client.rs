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

use crate::{MqttService, state::ConnectorHealthLabel, test_support::poll};

/// The MQTT client id a brenn session for `client_slug` connects with.
///
/// What the broker writes beside every packet it sends that session, so a test
/// reading the broker's own log can name the subscriber it means instead of
/// transcribing the format.
pub fn session_client_id(client_slug: &str) -> String {
    crate::connection::client_id_of_slug(client_slug)
}

/// [`poll::poll_until`] with the client's current health appended to `msg` on
/// the failing path: every wait here is a wait on a session, and the health
/// label is what says whether it was even connected.
async fn poll_health<T, F, Fut>(
    svc: &MqttService,
    client_slug: &str,
    timeout_secs: u64,
    msg: &str,
    state: F,
) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    poll::poll_until(timeout_secs, state, || async {
        let (label, error) = svc.ingress_health(client_slug).await;
        format!("{msg} (health while waiting: {label:?} {error:?})")
    })
    .await
}

/// Poll `svc.ingress_health(client_slug)` until the label is
/// in `accepted`, then return that label. Panics with `msg` if no accepted label
/// is seen within `timeout_secs`.
pub async fn wait_for_health(
    svc: &MqttService,
    client_slug: &str,
    accepted: &[ConnectorHealthLabel],
    timeout_secs: u64,
    msg: &str,
) -> ConnectorHealthLabel {
    poll_health(svc, client_slug, timeout_secs, msg, || async {
        let (label, _) = svc.ingress_health(client_slug).await;
        accepted.contains(&label).then_some(label)
    })
    .await
}

/// Poll `svc.subscription_acked(client_slug, topic_filter)` until the broker
/// has granted the filter on the current session. Panics with
/// `msg` if no grant is seen within `timeout_secs`.
///
/// This is the barrier a test crosses before publishing from outside brenn: it
/// proves the filter is at the broker, where a health wait proves only that the
/// session reconnected and the SUBSCRIBE went out. It does not hold the session
/// up afterwards — a drop between the grant and the publish clears the grant
/// with the client.
///
/// A broker refusal ends the wait at once, carrying the SUBACK's reason: the
/// broker has answered, and polling out a ten-second deadline for an answer
/// already given reads as a lost session instead of the refusal it is.
///
/// # Panics
///
/// If `client_slug` is not a registered client: the caller named a client the
/// document does not declare. If the broker refused the filter.
pub async fn wait_for_filter_acked(
    svc: &MqttService,
    client_slug: &str,
    topic_filter: &str,
    timeout_secs: u64,
    msg: &str,
) {
    poll_health(svc, client_slug, timeout_secs, msg, || async {
        let acked = svc
            .subscription_acked(client_slug, topic_filter)
            .await
            .unwrap_or_else(|| {
                panic!("wait_for_filter_acked: no session registered for client {client_slug:?}")
            });
        if !acked && let Some(reason) = svc.subscription_refusal(client_slug, topic_filter).await {
            panic!("{msg}: the broker refused the subscription for {topic_filter}: {reason}");
        }
        acked.then_some(())
    })
    .await
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
    direct_publisher_acked_as(broker_port, ca_pem, None).await
}

/// [`direct_publisher_acked`] logging in as `credentials`, for a broker that
/// rejects anonymous clients.
///
/// The publisher is the case's stand-in for whatever is really on the topic, so
/// it needs an account of its own on a password-authenticating broker;
/// `brenn_mqtt::test_support::broker`'s two are both admitted by the shipped
/// ACL.
pub async fn direct_publisher_acked_as(
    broker_port: u16,
    ca_pem: Vec<u8>,
    credentials: Option<(&str, &str)>,
) -> (AsyncClient, mpsc::UnboundedReceiver<()>) {
    let client_id = format!("brenn-direct-acked-{}", unique_suffix());
    let mut opts = MqttOptions::new(client_id, ("127.0.0.1", broker_port));
    opts.set_clean_start(true);
    opts.set_transport(Transport::tls(ca_pem, None, None));
    if let Some((username, password)) = credentials {
        opts.set_credentials(username.to_string(), password.to_string());
    }
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

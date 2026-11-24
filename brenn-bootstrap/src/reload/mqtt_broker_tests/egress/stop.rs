//! A session's stop against a broker that is slow to answer: what the egress
//! reader has in flight when a stop begins is waited for, up to the client's
//! `stop_wait_secs`, and reported as what it was. Each case freezes the broker
//! with a publish in flight, starts a stop — process shutdown, a reload that
//! changes a client, a reload that removes one — and thaws it, or not, inside
//! the bound.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use brenn_envelope::delivery::{DeliveryKind, DeliveryUnknownCause};
use brenn_messaging::config_reload::Outcome;
use brenn_mqtt::broker_gate;
use brenn_mqtt::state::ConnectorHealthLabel;
use brenn_mqtt::test_support::{BrokerHarness, wait_for_health};
use uuid::Uuid;

use super::super::super::driver::TriggerSource;
use super::super::super::driver::tests::{Booted, Tree, document};
use super::super::{BROKER_WAIT_SECS, trusted_ca};
use super::{
    SENDER, STATUS, TOPIC_PREFIX, await_statuses, boot_dispatching, client_block, kinds, seconds,
    send, sender_conversation, status_channel, statuses, telling,
};
use crate::shutdown::{ShutdownHandle, serve_until_shutdown};

/// One `mqtt_client` per `(slug, backoff_max)` at `port`, each retrying every
/// second, backing off at most `backoff_max` seconds and waiting
/// `stop_wait_secs` for the broker at a stop; the status channel [`STATUS`];
/// and the sender agent, which may publish to every declared client's topics
/// under [`TOPIC_PREFIX`] and to the status channel.
///
/// A client's `backoff_max` is what a case moves to make a reload change it.
fn stop_document(
    clients: &[(&str, u32)],
    port: u16,
    ca_file: &std::path::Path,
    stop_wait_secs: u32,
) -> String {
    let blocks: String = clients
        .iter()
        .map(|&(slug, backoff_max)| client_block(slug, port, ca_file, backoff_max, stop_wait_secs))
        .collect();
    let acl: String = clients
        .iter()
        .map(|(slug, _)| format!("topic_filter \"mqtt:{slug}:{TOPIC_PREFIX}/#\", "))
        .collect();
    let channel = status_channel("");
    document(&format!(
        r#"{blocks}
{channel}
agent Sender() {{
    working_dir = ".";
    grants = [publish];
    send_budget = 1000000;
    acl publish [{acl}exact status];
}}

new {SENDER}: Sender();
"#
    ))
}

/// Boot `document` with live sessions and wait for each of `clients` to
/// connect.
async fn boot_connected(tree: &Tree, clients: &[&str]) -> Booted {
    let booted = boot_dispatching(tree, None).await;
    for client in clients {
        wait_for_health(
            &booted.mqtt.0,
            client,
            &[ConnectorHealthLabel::Connected],
            BROKER_WAIT_SECS,
            "a client never connected",
        )
        .await;
    }
    booted
}

/// Freeze `harness` and publish one message naming [`STATUS`] through each of
/// `clients`; return the messages' ids once each has been told `handed`.
async fn in_flight_on_a_paused_broker(
    booted: &Booted,
    harness: &BrokerHarness,
    clients: &[&str],
) -> Vec<Uuid> {
    let conversation = sender_conversation(booted).await;
    harness.pause();
    let mut sent = Vec::new();
    for client in clients {
        sent.push(
            send(
                booted,
                conversation,
                client,
                &format!("{TOPIC_PREFIX}/in-flight-{client}"),
                b"pending",
                &telling(Some(STATUS), seconds(600)),
            )
            .await,
        );
    }
    let handed = await_statuses(booted, clients.len()).await;
    for id in &sent {
        assert!(
            kinds(&handed).contains(&(*id, DeliveryKind::Handed)),
            "{id} was handed: {handed:?}",
        );
    }
    sent
}

/// The kinds told for `id` on [`STATUS`], in order.
async fn told(booted: &Booted, id: Uuid) -> Vec<DeliveryKind> {
    statuses(booted)
        .await
        .into_iter()
        .filter(|status| status.message_id == id)
        .map(|status| status.kind)
        .collect()
}

/// The process-shutdown path, from the signal to the join's return, with only
/// the OS signal stood in for: two clients each with a publish in flight on a
/// frozen broker. The function does not return while the broker is frozen;
/// once it is thawed inside the bound the function returns, and at that
/// moment both senders have been told `handed` then `delivered` and both
/// sessions have been joined. A client a reload adds after the signal is born
/// stopped and does not hold the return.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_shutdown_waits_for_every_sessions_answers_before_it_returns() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&stop_document(
        &[("ha", 1), ("hb", 1)],
        harness.port,
        &ca_file,
        30,
    ));
    let booted = boot_connected(&tree, &["ha", "hb"]).await;
    let sent = in_flight_on_a_paused_broker(&booted, &harness, &["ha", "hb"]).await;

    let service = booted.mqtt.0.clone();
    let handle = ShutdownHandle {
        active_bridges: booted.active_bridges.clone(),
        server_shutting_down: Arc::new(AtomicBool::new(false)),
        mqtt: service.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback listener");
    let (signal_tx, signal_rx) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(serve_until_shutdown(
        listener,
        axum::Router::new(),
        handle,
        async move {
            signal_rx.await.expect("the case fires the signal");
        },
    ));
    signal_tx
        .send(())
        .expect("the server is waiting on the signal");

    let ha = service.get_client("ha").expect("ha is registered");
    brenn_mqtt::test_support::poll_until(
        BROKER_WAIT_SECS,
        || async { (*ha.stop_tx.borrow()).then_some(()) },
        || async { "the signal never stopped ha".to_string() },
    )
    .await;
    let mut late_config = (*ha.config).clone();
    late_config.identity.slug = "late".to_string();
    let late = brenn_mqtt::register_and_spawn(
        &service,
        Arc::new(late_config),
        brenn_mqtt::ArrivingFilters::Declared(Vec::new()),
        booted.mqtt.1.clone(),
    )
    .await;
    assert!(
        *late.stop_tx.borrow(),
        "a client added after the signal is born stopped"
    );

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        !serving.is_finished(),
        "the shutdown returned while the broker was frozen with publishes in flight",
    );

    harness.resume();
    tokio::time::timeout(Duration::from_secs(BROKER_WAIT_SECS), serving)
        .await
        .expect("the shutdown returned once the broker answered")
        .expect("the serve task");

    for id in &sent {
        // TODO(mqtt-egress-shutdown-answers-test-flaky): once failed here on a broker-suite run, [Handed, Dropped { cause: Stopped }].
        assert_eq!(
            told(&booted, *id).await,
            vec![DeliveryKind::Handed, DeliveryKind::Delivered],
            "{id}'s outcome was committed before the shutdown returned",
        );
    }
    for slug in ["ha", "hb"] {
        let session = service.get_client(slug).expect("still registered");
        tokio::time::timeout(Duration::ZERO, session.stop_and_join())
            .await
            .unwrap_or_else(|_| panic!("{slug}'s session was joined by the shutdown"));
    }
}

/// A reload that changes a client whose publish is in flight on a frozen
/// broker waits for the answer: the broker stays frozen past five seconds, is
/// thawed inside the client's bound, and the
/// sender is told `delivered`, never `unknown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reload_that_changes_a_client_waits_for_its_in_flight_answer() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&stop_document(&[("ha", 1)], harness.port, &ca_file, 30));
    let mut booted = boot_connected(&tree, &["ha"]).await;
    let sent = in_flight_on_a_paused_broker(&booted, &harness, &["ha"]).await;

    tree.write(&stop_document(&[("ha", 2)], harness.port, &ca_file, 30));
    let frozen_for = Duration::from_secs(6);
    let started = Instant::now();
    tokio::join!(booted.driver.reload(TriggerSource::Signal), async {
        tokio::time::sleep(frozen_for).await;
        harness.resume();
    });
    assert!(
        started.elapsed() >= frozen_for,
        "the reload waited for the broker"
    );
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec!["ha".to_string()]);
    assert_eq!(
        told(&booted, sent[0]).await,
        vec![DeliveryKind::Handed, DeliveryKind::Delivered],
    );

    booted.stop_mqtt();
}

/// A reload that removes a client whose publish is in flight on a frozen
/// broker waits, in the retire, for the answer, and the sender is told
/// `delivered`, never `unknown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reload_that_removes_a_client_waits_for_its_in_flight_answer() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&stop_document(
        &[("ha", 1), ("hb", 1)],
        harness.port,
        &ca_file,
        30,
    ));
    let mut booted = boot_connected(&tree, &["ha", "hb"]).await;
    let sent = in_flight_on_a_paused_broker(&booted, &harness, &["hb"]).await;

    tree.write(&stop_document(&[("ha", 1)], harness.port, &ca_file, 30));
    tokio::join!(booted.driver.reload(TriggerSource::Signal), async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        harness.resume();
    });
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_removed, vec!["hb".to_string()]);
    assert_eq!(
        told(&booted, sent[0]).await,
        vec![DeliveryKind::Handed, DeliveryKind::Delivered],
    );

    booted.stop_mqtt();
}

/// Two removed clients, each with a publish in flight on a broker that stays
/// frozen: their retires wait together, so the reload takes about one bound,
/// not two, and each sender is told `unknown`, `stopped`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_clients_wait_for_the_broker_together() {
    broker_gate!();

    let bound = Duration::from_secs(3);
    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let doc = |clients: &[(&str, u32)]| {
        stop_document(
            clients,
            harness.port,
            &ca_file,
            u32::try_from(bound.as_secs()).expect("a small bound"),
        )
    };
    let tree = Tree::holding(&doc(&[("ha", 1), ("hb", 1), ("hc", 1)]));
    let mut booted = boot_connected(&tree, &["ha", "hb", "hc"]).await;
    let sent = in_flight_on_a_paused_broker(&booted, &harness, &["hb", "hc"]).await;

    tree.write(&doc(&[("ha", 1)]));
    let started = Instant::now();
    booted.driver.reload(TriggerSource::Signal).await;
    let took = started.elapsed();
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert!(
        took >= bound,
        "the retire waited the bound for the frozen broker: {took:?}"
    );
    assert!(
        took < bound * 2,
        "the two retires waited together, not one after the other: {took:?}",
    );
    for id in sent {
        assert_eq!(
            told(&booted, id).await,
            vec![
                DeliveryKind::Handed,
                DeliveryKind::Unknown {
                    cause: DeliveryUnknownCause::Stopped
                }
            ],
        );
    }

    harness.resume();
    booted.stop_mqtt();
}

//! A client's connection-state channel against a real broker: it follows the
//! session through a connect, a lost broker, a reconnect and a stop; it reads
//! `failed` with the broker's reason when the broker refuses the client for
//! good; and a second process over the first's database tells a sender of an
//! unclean end, and says nothing after a clean one.

use uuid::Uuid;

use brenn_envelope::delivery::{
    self, DELIVERY_STATUS_SENDER, DeliveryKind, DeliveryStatus, DeliveryUnknownCause,
    MqttRestarted, StatusBody, TransportRestarted,
};
use brenn_envelope::mqtt_connection::{Connection, MqttConnectionState, PreviousEnd};
use brenn_lib::messaging::config::Depth;
use brenn_messaging_store::db::load_channel_retained_window_seq;
use brenn_mqtt::broker_gate;
use brenn_mqtt::state::ConnectorHealthLabel;
use brenn_mqtt::test_support::{BrokerHarness, poll_until, wait_for_health};
use brenn_obs::alerting::AlertSeverity;

use super::super::super::driver::tests::{Booted, Tree};
use super::super::super::mqtt_egress_tests::{connection_states, newest_state, shut_down};
use super::super::{BROKER_WAIT_SECS, trusted_ca};
use super::{
    STATUS, TOPIC_PREFIX, boot_dispatching, egress_document, seconds, send, sender_conversation,
    status_document, telling,
};

/// `client`'s newest connection state once `want` holds of it.
async fn await_state(
    booted: &Booted,
    client: &str,
    what: &str,
    want: impl Fn(&MqttConnectionState) -> bool,
) -> MqttConnectionState {
    poll_until(
        BROKER_WAIT_SECS,
        || async {
            let states = connection_states(&booted.messenger, client).await;
            states.last().filter(|state| want(state)).cloned()
        },
        || async {
            format!(
                "{client}'s state never became {what}: {:?}",
                connection_states(&booted.messenger, client).await
            )
        },
    )
    .await
}

/// Every body on [`STATUS`], oldest first, each from `system:delivery-status`.
async fn status_bodies(booted: &Booted) -> Vec<StatusBody> {
    let uuid = booted
        .messenger
        .directory()
        .resolve(STATUS)
        .expect("the status channel is declared")
        .uuid;
    let conn = booted.db.lock().await;
    load_channel_retained_window_seq(&conn, uuid, Depth::Unbounded)
        .into_iter()
        .map(|(_, envelope)| {
            assert_eq!(envelope.sender, DELIVERY_STATUS_SENDER, "{envelope:?}");
            delivery::decode(envelope.body.as_text().expect("a status is text"))
                .expect("a v1 status")
        })
        .collect()
}

/// The bodies on [`STATUS`] once there are at least `count`.
async fn await_status_bodies(booted: &Booted, count: usize) -> Vec<StatusBody> {
    poll_until(
        BROKER_WAIT_SECS,
        || async {
            let found = status_bodies(booted).await;
            (found.len() >= count).then_some(found)
        },
        || async { format!("{STATUS} never held {count} statuses") },
    )
    .await
}

/// The message status `body` is, as `(message id, kind)`.
fn message(body: &StatusBody) -> (Uuid, DeliveryKind) {
    let StatusBody::Message(DeliveryStatus {
        message_id, kind, ..
    }) = body
    else {
        panic!("expected a message status, got {body:?}");
    };
    (*message_id, kind.clone())
}

/// The connection state follows the session: `connected` once the client
/// connects — a client nothing is bound through, as this one is —,
/// `disconnected` with the transport's error once the broker goes, `connected`
/// again when it returns, and `stopped` last after the shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_connection_state_follows_the_session_through_a_lost_broker_and_a_stop() {
    broker_gate!();

    let mut harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&egress_document(&[("ha", harness.port)], &ca_file));
    let booted = boot_dispatching(&tree, None).await;

    let connected = await_state(&booted, "ha", "connected", |state| {
        state.connection == Connection::Connected
    })
    .await;
    assert_eq!(connected.client, "ha");
    assert_eq!(connected.previous_session.end, PreviousEnd::None);

    harness.stop();
    let lost = await_state(&booted, "ha", "disconnected with an error", |state| {
        matches!(&state.connection, Connection::Disconnected { last_error: Some(e) } if !e.is_empty())
    })
    .await;
    assert!(lost.since_ms >= connected.since_ms);
    assert_eq!(lost.previous_session, connected.previous_session);

    harness.restart();
    let back = await_state(&booted, "ha", "connected again", |state| {
        state.connection == Connection::Connected
    })
    .await;
    assert!(back.since_ms >= lost.since_ms);

    let messenger = std::sync::Arc::clone(&booted.messenger);
    shut_down(booted).await;
    let last = newest_state(&messenger, "ha").await;
    assert_eq!(last.connection, Connection::Stopped);
    assert_eq!(last.previous_session, connected.previous_session);
}

/// A client the broker refuses for good reads `failed` with the broker's
/// reason, and one `Critical` alert names it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_the_broker_refuses_reads_failed_and_alerts_once() {
    broker_gate!();

    let harness = BrokerHarness::start_auth();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&egress_document(&[("ha", harness.port)], &ca_file));
    let booted = boot_dispatching(&tree, None).await;
    wait_for_health(
        &booted.mqtt.0,
        "ha",
        &[ConnectorHealthLabel::Failed],
        BROKER_WAIT_SECS,
        "the client the broker refuses never failed",
    )
    .await;

    let failed = await_state(&booted, "ha", "failed", |state| {
        matches!(&state.connection, Connection::Failed { .. })
    })
    .await;
    let Connection::Failed { reason } = &failed.connection else {
        unreachable!("the wait answered a failed state");
    };
    assert!(
        !reason.is_empty(),
        "the failure carries the broker's reason"
    );

    let alerts = booted.alerts().await;
    let stopped: Vec<_> = alerts
        .iter()
        .filter(|(_, title, _)| title == "MQTT client stopped: ha")
        .collect();
    assert_eq!(stopped.len(), 1, "{alerts:?}");
    assert!(
        matches!(stopped[0].0, AlertSeverity::Critical),
        "{stopped:?}"
    );
    booted.stop_mqtt();
}

/// What the first process of a restart case left: the message it had
/// handed, unanswered, naming [`STATUS`].
struct FirstProcess {
    handed: Uuid,
    /// The sender's conversation, which the database keeps for the next
    /// process.
    conversation: i64,
}

/// Boot over `db`, wait for the client, freeze `harness` and publish one
/// QoS 1 message naming [`STATUS`], so it is handed and unanswered. Then end
/// the process: through the server's shutdown when `clean`, otherwise by
/// ending its runtime with nothing stopped, as a kill does — every task of
/// the process ends at its next await and none outlives it.
fn first_process(
    tree: &Tree,
    db: &brenn_db::Db,
    harness: &BrokerHarness,
    clean: bool,
) -> FirstProcess {
    tokio::task::block_in_place(|| {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(4)
                        .enable_all()
                        .build()
                        .expect("a runtime for the first process");
                    let (booted, handed, conversation) = runtime.block_on(async {
                        let booted = boot_dispatching(tree, Some(db.clone())).await;
                        wait_for_health(
                            &booted.mqtt.0,
                            "ha",
                            &[ConnectorHealthLabel::Connected],
                            BROKER_WAIT_SECS,
                            "the first process never connected",
                        )
                        .await;
                        let conversation = sender_conversation(&booted).await;
                        // A frozen broker acknowledges nothing, so the publish
                        // stays in flight.
                        harness.pause();
                        let handed = send(
                            &booted,
                            conversation,
                            "ha",
                            &format!("{TOPIC_PREFIX}/in-flight"),
                            b"pending",
                            &telling(Some(STATUS), seconds(600)),
                        )
                        .await;
                        assert_eq!(
                            await_status_bodies(&booted, 1)
                                .await
                                .iter()
                                .map(message)
                                .collect::<Vec<_>>(),
                            [(handed, DeliveryKind::Handed)],
                        );
                        (booted, handed, conversation)
                    });
                    if clean {
                        runtime.block_on(shut_down(booted));
                        drop(runtime);
                    } else {
                        drop(runtime);
                        drop(booted);
                    }
                    FirstProcess {
                        handed,
                        conversation,
                    }
                })
                .join()
                .expect("the first process's thread")
        })
    })
}

/// A process killed with a message handed and unanswered: the next process
/// tells the status channel `restarted` once, after the `handed` and before
/// anything of its own, and its state says the previous session ended
/// uncleanly at the marker's instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unclean_restart_tells_the_sender_before_anything_new_is_handed() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&status_document(harness.port, &ca_file, Some(""), 1));
    let db = brenn_server::test_support::init_db_memory();

    let first = first_process(&tree, &db, &harness, false);

    let second = boot_dispatching(&tree, Some(db)).await;
    let state = newest_state(&second.messenger, "ha").await;
    assert_eq!(state.previous_session.end, PreviousEnd::Unclean);
    let marker = StatusBody::Restarted(TransportRestarted {
        found_at_ms: state.previous_session.found_at_ms,
        mqtt: Some(MqttRestarted {
            client: "ha".to_string(),
        }),
    });
    let found = status_bodies(&second).await;
    assert_eq!(found.len(), 2, "{found:?}");
    assert_eq!(message(&found[0]), (first.handed, DeliveryKind::Handed));
    assert_eq!(found[1], marker);

    harness.resume();
    wait_for_health(
        &second.mqtt.0,
        "ha",
        &[ConnectorHealthLabel::Connected],
        BROKER_WAIT_SECS,
        "the second process never connected",
    )
    .await;
    let fresh = send(
        &second,
        first.conversation,
        "ha",
        &format!("{TOPIC_PREFIX}/fresh"),
        b"now",
        &telling(Some(STATUS), seconds(60)),
    )
    .await;
    let found = await_status_bodies(&second, 4).await;
    assert_eq!(
        found[1], marker,
        "the marker stays ahead of the new process"
    );
    assert_eq!(
        found[2..].iter().map(message).collect::<Vec<_>>(),
        [
            (fresh, DeliveryKind::Handed),
            (fresh, DeliveryKind::Delivered)
        ],
    );
    assert_eq!(
        found
            .iter()
            .filter(|body| matches!(body, StatusBody::Restarted(_)))
            .count(),
        1,
        "{found:?}"
    );
    shut_down(second).await;
}

/// A process stopped through the shutdown with a message in flight tells
/// its sender `unknown`, `stopped`; the next process reads its record clean
/// and tells nobody.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clean_restart_tells_the_sender_nothing_more() {
    broker_gate!();

    let harness = BrokerHarness::start();
    let (_ca_dir, ca_file) = trusted_ca();
    let tree = Tree::holding(&status_document(harness.port, &ca_file, Some(""), 1));
    let db = brenn_server::test_support::init_db_memory();

    let first = first_process(&tree, &db, &harness, true);
    harness.resume();

    let second = boot_dispatching(&tree, Some(db)).await;
    let state = newest_state(&second.messenger, "ha").await;
    assert_eq!(state.previous_session.end, PreviousEnd::Clean);
    assert_eq!(
        status_bodies(&second)
            .await
            .iter()
            .map(message)
            .collect::<Vec<_>>(),
        [
            (first.handed, DeliveryKind::Handed),
            (
                first.handed,
                DeliveryKind::Unknown {
                    cause: DeliveryUnknownCause::Stopped
                }
            ),
        ],
        "the stop told the outcome and no marker follows"
    );
    shut_down(second).await;
}

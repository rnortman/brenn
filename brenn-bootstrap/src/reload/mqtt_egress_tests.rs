//! Each client's egress position, reader and connection-state record as boot,
//! shutdown and reload leave them: added, removed, changed, a slug that
//! returns after its client was removed, and a process that ended without its
//! stop. The rig's sessions never connect, so a reader here hands nothing
//! off and what it is owed stays on its channel.

use std::sync::Arc;

use chrono::Utc;
use uuid::Uuid;

use brenn_envelope::delivery::{
    self, DELIVERY_STATUS_SENDER, DeliveryDropCause, DeliveryKind, DeliveryStatus, MqttRestarted,
    StatusBody, TransportRestarted,
};
use brenn_envelope::mqtt_connection::{
    self, Connection, MQTT_CONNECTION_SENDER, MqttConnectionState, PreviousEnd,
};
use brenn_envelope::{
    ChannelScheme, MessageBody, MqttExpiry, MqttPublishTransport, TransportMeta, Urgency,
};
use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{
    ParticipantId, mqtt_connection_address, mqtt_connection_channel_uuid, mqtt_egress_address,
};
use brenn_messaging::Messenger;
use brenn_messaging::config_reload::Outcome;
use brenn_messaging::mqtt_outbound::{MQTT_OUTBOUND_NOISE, mqtt_outbound_participant};
use brenn_messaging_store::blob::HeldAttachments;
use brenn_messaging_store::db::{
    SubscriberCursorRow, load_channel_retained_window_seq, load_subscriber_cursor, utc_to_ns,
};
use brenn_messaging_store::store::NewMessage;
use brenn_obs::alerting::AlertSeverity;

use super::driver::TriggerSource;
use super::driver::tests::{
    BootFixture, Booted, Tree, boot_with, document_with_clients, egress_entry_noise,
    install_package, staged_module,
};

pub(crate) fn participant() -> ParticipantId {
    mqtt_outbound_participant()
}

/// The uuid of `client`'s egress channel in the live directory.
pub(crate) fn egress_uuid(messenger: &Messenger, client: &str) -> Uuid {
    messenger
        .directory()
        .resolve(&mqtt_egress_address(client))
        .unwrap_or_else(|| panic!("{client}'s egress channel is in the directory"))
        .uuid
}

/// The participant's cursor row on the channel `uuid`, whether or not the
/// directory still holds the channel.
pub(crate) async fn egress_cursor(
    messenger: &Messenger,
    uuid: Uuid,
) -> Option<SubscriberCursorRow> {
    let conn = messenger.db().lock().await;
    load_subscriber_cursor(&conn, uuid, &participant())
}

/// Commit `n` messages to `client`'s egress channel, as the admission would.
async fn commit(messenger: &Messenger, client: &str, n: usize) {
    commit_as(messenger, client, n, "app:sifter", None).await;
}

/// Commit `n` messages from `sender` naming `status_to` to `client`'s egress
/// channel, as the admission would, and return their ids in order.
async fn commit_as(
    messenger: &Messenger,
    client: &str,
    n: usize,
    sender: &str,
    status_to: Option<&str>,
) -> Vec<Uuid> {
    let store = messenger.store_for_address(&mqtt_egress_address(client));
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let appended = store
            .append(NewMessage {
                source: "test".to_string(),
                sender: sender.to_string(),
                body: MessageBody::from_parts(
                    Some("application/octet-stream"),
                    format!("m{i}").as_bytes(),
                )
                .expect("a small body"),
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Brenn,
                transport: Some(TransportMeta::MqttPublish(MqttPublishTransport {
                    client_slug: client.to_string(),
                    topic: "lights/kitchen/set".to_string(),
                    qos: 1,
                    retain: false,
                    content_type: None,
                    expires: MqttExpiry::Never,
                    status_to: status_to.map(str::to_string),
                    response_topic: None,
                })),
                reply_to: None,
                delivery_deadline: None,
                impetus: None,
                publish_ts_ns: utc_to_ns(Utc::now()),
                attachments: HeldAttachments::none(),
            })
            .await;
        ids.push(appended.committed.message_uuid);
    }
    ids
}

/// A fixture boot over `tree` and, when given, an existing database.
async fn boot_over(tree: &Tree, components: &std::path::Path, db: Option<brenn_db::Db>) -> Booted {
    boot_with(
        tree,
        BootFixture {
            db,
            components_roots: vec![components.to_path_buf()],
            ..BootFixture::default()
        },
    )
    .await
}

/// Stop every session, join its reader and write each client's `stopped`, as
/// the server's shutdown does, so nothing this process started still holds
/// its messenger when the next boot opens the same stores.
pub(crate) async fn shut_down(booted: Booted) {
    let service = booted.mqtt.0.clone();
    service.stop_all();
    crate::shutdown::join_mqtt(&service, Some(&booted.mqtt_connection)).await;
    drop(booted);
}

/// An added client's channel arrives with the participant's position and
/// subscriber entry on it, and a reader runs on its session. Boot's clients
/// have theirs from the start. The commit's closing wiring cross-check passed,
/// or the reload would not have returned.
#[tokio::test(flavor = "multi_thread")]
async fn an_added_client_gets_a_position_an_entry_and_a_running_reader() {
    let one = [("ha", 8883u16, None)];
    let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
    let tree = Tree::holding(&document_with_clients(&one, &[("ha", "home/state")]));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_over(&tree, components.path(), None).await;
    let service = booted.mqtt.0.clone();
    let ha = service.get_client("ha").expect("a declared client");
    assert!(
        ha.egress_reader_running(),
        "boot starts a booted client's reader"
    );
    let ha_uuid = egress_uuid(&booted.messenger, "ha");
    assert!(egress_cursor(&booted.messenger, ha_uuid).await.is_some());

    tree.write(&document_with_clients(&two, &[("ha", "home/state")]));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_added, vec!["spare".to_string()]);
    let spare = service.get_client("spare").expect("the added client");
    assert!(
        spare.egress_reader_running(),
        "the added client has no reader"
    );
    let spare_uuid = egress_uuid(&booted.messenger, "spare");
    assert!(
        egress_cursor(&booted.messenger, spare_uuid).await.is_some(),
        "the added client's channel has no position",
    );
    assert_eq!(
        egress_entry_noise(&booted.messenger, "spare"),
        Some(brenn_lib::messaging::config::NoiseLevel::Alarm),
    );
    assert!(
        ha.egress_reader_running(),
        "an unmoved client keeps its reader"
    );
    booted.stop_mqtt();
}

/// A removed client's reader is retired before its channel leaves: the close
/// raises what it was owed to the operator by count, its position and
/// subscriber entry go, and the channel leaves the directory.
#[tokio::test(flavor = "multi_thread")]
async fn a_removed_clients_reader_is_retired_and_what_it_was_owed_is_alerted() {
    let one = [("ha", 8883u16, None)];
    let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
    let tree = Tree::holding(&document_with_clients(&two, &[("ha", "home/state")]));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_over(&tree, components.path(), None).await;
    let service = booted.mqtt.0.clone();
    let spare = service.get_client("spare").expect("a declared client");
    let spare_uuid = egress_uuid(&booted.messenger, "spare");
    commit(&booted.messenger, "spare", 3).await;

    tree.write(&document_with_clients(&one, &[("ha", "home/state")]));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_removed, vec!["spare".to_string()]);
    assert!(
        !spare.egress_reader_running(),
        "the removed client's reader runs"
    );
    assert!(
        egress_cursor(&booted.messenger, spare_uuid).await.is_none(),
        "the removed client's position outlived it",
    );
    assert!(
        booted
            .messenger
            .directory()
            .resolve(&mqtt_egress_address("spare"))
            .is_none(),
    );
    let alerts = booted.alerts().await;
    let dropped: Vec<_> = alerts
        .iter()
        .filter(|(severity, title, _)| {
            matches!(severity, AlertSeverity::Warning) && title.contains("spare")
        })
        .collect();
    assert_eq!(dropped.len(), 1, "{alerts:?}");
    assert!(
        dropped[0].1.contains("dropped") || dropped[0].2.contains("dropped"),
        "{dropped:?}",
    );
    assert!(
        dropped[0]
            .2
            .contains("\"spare\": 3 message(s) dropped unsent"),
        "the count is not in {dropped:?}"
    );
    assert!(
        service
            .get_client("ha")
            .expect("the kept client")
            .egress_reader_running()
    );
}

/// A changed client's successor session gets a reader on the same channel and
/// the same position: nothing is skipped or replayed across the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_clients_successor_resumes_from_the_same_position() {
    let tree = Tree::holding(&document_with_clients(
        &[("ha", 8883u16, None)],
        &[("ha", "home/state")],
    ));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_over(&tree, components.path(), None).await;
    let service = booted.mqtt.0.clone();
    let before = service.get_client("ha").expect("a declared client");
    let uuid = egress_uuid(&booted.messenger, "ha");
    commit(&booted.messenger, "ha", 2).await;
    let owed = egress_cursor(&booted.messenger, uuid)
        .await
        .expect("boot's position")
        .next_owed_seq;

    tree.write(&document_with_clients(
        &[("ha", 8885u16, None)],
        &[("ha", "home/state")],
    ));
    booted.driver.reload(TriggerSource::Signal).await;

    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec!["ha".to_string()]);
    let after = service.get_client("ha").expect("the slug is never absent");
    assert!(!Arc::ptr_eq(&before, &after));
    assert!(
        !before.egress_reader_running(),
        "the predecessor's reader runs"
    );
    assert!(after.egress_reader_running(), "the successor has no reader");
    assert_eq!(egress_uuid(&booted.messenger, "ha"), uuid);
    assert_eq!(
        egress_cursor(&booted.messenger, uuid)
            .await
            .expect("the position survives")
            .next_owed_seq,
        owed,
    );
    let alerts = booted.captured.lock().expect("alert capture").clone();
    assert!(
        alerts.is_empty(),
        "nothing was outstanding, so nothing is reported: {alerts:?}",
    );
    booted.stop_mqtt();
}

/// A restart resumes a client's position; a client removed between boots
/// loses it, and when its slug returns the new position skips what the
/// channel kept from before.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_removed_and_added_again_does_not_publish_what_was_retained() {
    let with_ha = document_with_clients(&[("ha", 8883u16, None)], &[("ha", "home/state")]);
    let without_ha = document_with_clients(&[("spare", 8884u16, None)], &[("spare", "home/state")]);
    let tree = Tree::holding(&with_ha);
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let db = brenn_server::test_support::init_db_memory();

    let first = boot_over(&tree, components.path(), Some(db.clone())).await;
    let uuid = egress_uuid(&first.messenger, "ha");
    let owed = egress_cursor(&first.messenger, uuid)
        .await
        .expect("boot's position")
        .next_owed_seq;
    commit(&first.messenger, "ha", 2).await;
    shut_down(first).await;

    let restarted = boot_over(&tree, components.path(), Some(db.clone())).await;
    assert_eq!(
        egress_cursor(&restarted.messenger, uuid)
            .await
            .expect("the position survives a restart")
            .next_owed_seq,
        owed,
        "a restart resumes where the reader was",
    );
    shut_down(restarted).await;

    tree.write(&without_ha);
    install_package(components.path(), &staged_module(&tree));
    let removed = boot_over(&tree, components.path(), Some(db.clone())).await;
    assert!(
        egress_cursor(&removed.messenger, uuid).await.is_none(),
        "boot keeps a position no subscriber justifies",
    );
    shut_down(removed).await;

    tree.write(&with_ha);
    install_package(components.path(), &staged_module(&tree));
    let returned = boot_over(&tree, components.path(), Some(db)).await;
    assert_eq!(egress_uuid(&returned.messenger, "ha"), uuid);
    assert_eq!(
        egress_cursor(&returned.messenger, uuid)
            .await
            .expect("the returning client's position")
            .next_owed_seq,
        owed + 2,
        "the new position skips both messages the channel kept",
    );
    shut_down(returned).await;
}

/// A client that never connects, with an egress window of 2: five commits and
/// one eviction pass raise one overflow alert naming the client's channel
/// with the three the window dropped, and the participant's drop tally reads 3.
#[tokio::test(flavor = "multi_thread")]
async fn an_outage_longer_than_the_window_is_alerted_by_the_eviction_pass() {
    let address = mqtt_egress_address("ha");
    let mut document = document_with_clients(&[("ha", 8883u16, None)], &[("ha", "home/state")]);
    document.push_str(&format!(
        "\nchannel at \"{address}\" {{\n    push_depth = 2;\n    retain_depth = 2;\n    \
         standing_retain_depth = 2;\n}}\n"
    ));
    let tree = Tree::holding(&document);
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let booted = boot_over(&tree, components.path(), None).await;
    commit(&booted.messenger, "ha", 5).await;

    crate::shutdown::bus_gc_pass(&booted.db, &booted.messenger, None).await;

    let alerts = booted.alerts().await;
    let overflow: Vec<_> = alerts
        .iter()
        .filter(|(_, title, body)| title.contains(&address) || body.contains(&address))
        .collect();
    assert_eq!(overflow.len(), 1, "{alerts:?}");
    assert!(
        matches!(overflow[0].0, AlertSeverity::Warning),
        "{overflow:?}"
    );
    assert!(
        overflow[0]
            .2
            .contains(": 3 message(s) passed the subscriber's position unread"),
        "the count is not in {overflow:?}",
    );
    assert_eq!(booted.messenger.drop_counter(&address, &participant()), 3);
    booted.stop_mqtt();
}

/// The status channel the `status_to` cases name.
const STATUS: &str = "brenn:status";

/// The agent whose publishes name [`STATUS`].
const STATUS_SENDER: &str = "mqtt-sender";

/// [`document_with_clients`] over `clients`, with [`STATUS`] deep and fast
/// enough for a full egress window's statuses, and [`STATUS_SENDER`], which may
/// publish to it.
fn status_document(clients: &[(&str, u16, Option<&std::path::Path>)]) -> String {
    let mut document = document_with_clients(clients, &[("ha", "home/state")]);
    document.push_str(&format!(
        r#"
channel status at "{STATUS}" {{
    push_depth = 1;
    retain_depth = 512;
    standing_retain_depth = 512;
    send_rate = {{ burst = 1000, refill_interval_secs = 1, refill = 1000 }};
}}

agent Sender() {{
    working_dir = ".";
    grants = [publish];
    acl publish [exact status];
}}

new {STATUS_SENDER}: Sender();
"#
    ));
    document
}

/// The statuses on [`STATUS`], oldest first, each from
/// `system:delivery-status`.
async fn statuses(messenger: &Messenger) -> Vec<DeliveryStatus> {
    let uuid = messenger
        .directory()
        .resolve(STATUS)
        .expect("the status channel is declared")
        .uuid;
    let conn = messenger.db().lock().await;
    load_channel_retained_window_seq(&conn, uuid, Depth::Unbounded)
        .into_iter()
        .map(|(_, envelope)| {
            assert_eq!(envelope.sender, DELIVERY_STATUS_SENDER, "{envelope:?}");
            match delivery::decode(envelope.body.as_text().expect("a status is text"))
                .expect("a v1 status")
            {
                delivery::StatusBody::Message(status) => status,
                other => panic!("expected a message status, got {other:?}"),
            }
        })
        .collect()
}

/// Boot with `ha` and `spare`, commit `named` messages naming [`STATUS`] and
/// `unnamed` naming none to `spare`, and reload with `spare` removed. Returns
/// the booted process and the named messages' ids.
async fn remove_spare_owing(named: usize, unnamed: usize) -> (Booted, Vec<Uuid>) {
    let (booted, ids, _) = remove_spare(named, unnamed, false).await;
    (booted, ids)
}

/// [`remove_spare_owing`], first stopping `spare`'s session and waiting for
/// its reader to exit when `stopped_first`, as a shutdown that races the
/// reload leaves it. Also returns `spare`'s egress channel uuid.
async fn remove_spare(
    named: usize,
    unnamed: usize,
    stopped_first: bool,
) -> (Booted, Vec<Uuid>, Uuid) {
    let one = [("ha", 8883u16, None)];
    let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
    let tree = Tree::holding(&status_document(&two));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_over(&tree, components.path(), None).await;
    let uuid = egress_uuid(&booted.messenger, "spare");
    let sender = ParticipantId::for_app(STATUS_SENDER, booted.messenger.source());
    let ids = commit_as(
        &booted.messenger,
        "spare",
        named,
        sender.as_str(),
        Some(STATUS),
    )
    .await;
    commit_as(&booted.messenger, "spare", unnamed, sender.as_str(), None).await;
    if stopped_first {
        let spare = booted
            .mqtt
            .0
            .get_client("spare")
            .expect("a declared client");
        spare.stop_and_join().await;
        assert!(!spare.egress_reader_running());
    }

    tree.write(&status_document(&one));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_removed, vec!["spare".to_string()]);
    (booted, ids, uuid)
}

/// Assert `found` tells each of `ids`, in order, `dropped`, `removed`.
fn assert_all_removed(found: &[DeliveryStatus], ids: &[Uuid]) {
    let told: Vec<_> = found
        .iter()
        .map(|status| (status.message_id, status.kind.clone()))
        .collect();
    let removed: Vec<_> = ids
        .iter()
        .map(|id| {
            (
                *id,
                DeliveryKind::Dropped {
                    cause: DeliveryDropCause::Removed,
                },
            )
        })
        .collect();
    assert_eq!(told, removed);
}

/// A removed client's unread messages that name a `status_to` are each told
/// `dropped`, `removed` there; the operator's dropped alert counts only the
/// rest.
#[tokio::test(flavor = "multi_thread")]
async fn a_removed_clients_unread_messages_naming_a_status_to_are_told_removed() {
    let (booted, ids) = remove_spare_owing(3, 2).await;
    assert_all_removed(&statuses(&booted.messenger).await, &ids);
    let alerts = booted.alerts().await;
    let dropped: Vec<_> = alerts
        .iter()
        .filter(|(_, title, _)| title.contains("dropped") && title.contains("spare"))
        .collect();
    assert_eq!(dropped.len(), 1, "{alerts:?}");
    assert!(
        dropped[0]
            .2
            .contains("\"spare\": 2 message(s) dropped unsent"),
        "only the untold messages are counted: {dropped:?}"
    );
    booted.stop_mqtt();
}

/// A removed client whose channel holds a full default window, every message
/// naming a `status_to`, has every one told, and the reload completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_window_of_messages_naming_a_status_to_is_told_and_the_reload_completes() {
    let (booted, ids) = remove_spare_owing(256, 0).await;
    assert_eq!(ids.len(), 256);
    assert_all_removed(&statuses(&booted.messenger).await, &ids);
    let alerts = booted.alerts().await;
    assert!(
        !alerts
            .iter()
            .any(|(_, title, _)| title.contains("dropped") && title.contains("spare")),
        "every dropped message was told, so none is alerted: {alerts:?}"
    );
    booted.stop_mqtt();
}

/// A removal whose reader had already ended for the session's stop reports
/// the channel's unread messages all the same: each naming a `status_to` is
/// told `dropped`, `removed`, the operator's alert counts the rest, and the
/// position goes.
#[tokio::test(flavor = "multi_thread")]
async fn a_removal_after_the_readers_stop_reports_what_the_channel_held() {
    let (booted, ids, uuid) = remove_spare(2, 3, true).await;
    assert_all_removed(&statuses(&booted.messenger).await, &ids);
    let alerts = booted.alerts().await;
    let dropped: Vec<_> = alerts
        .iter()
        .filter(|(_, title, _)| title.contains("dropped") && title.contains("spare"))
        .collect();
    assert_eq!(dropped.len(), 1, "{alerts:?}");
    assert!(
        dropped[0]
            .2
            .contains("\"spare\": 3 message(s) dropped unsent"),
        "the untold messages are counted: {dropped:?}"
    );
    assert!(
        dropped[0].2.contains("the client was removed"),
        "{dropped:?}"
    );
    assert!(
        egress_cursor(&booted.messenger, uuid).await.is_none(),
        "the removed client's position outlived it",
    );
    booted.stop_mqtt();
}

/// Every connection state on `client`'s state channel, oldest first, each
/// checked to come from the host's writer. Read by the channel's uuid, so a
/// removed client's channel reads too.
pub(crate) async fn connection_states(
    messenger: &Messenger,
    client: &str,
) -> Vec<MqttConnectionState> {
    let uuid = mqtt_connection_channel_uuid(&mqtt_connection_address(client));
    let conn = messenger.db().lock().await;
    load_channel_retained_window_seq(&conn, uuid, Depth::Unbounded)
        .into_iter()
        .map(|(_, envelope)| {
            assert_eq!(envelope.sender, MQTT_CONNECTION_SENDER, "{envelope:?}");
            mqtt_connection::decode(envelope.body.as_text().expect("a state is text"))
                .expect("a v1 state")
        })
        .collect()
}

/// The newest connection state of `client`.
pub(crate) async fn newest_state(messenger: &Messenger, client: &str) -> MqttConnectionState {
    connection_states(messenger, client)
        .await
        .pop()
        .unwrap_or_else(|| panic!("{client}'s state channel holds nothing"))
}

/// Every body on [`STATUS`], oldest first, each from `system:delivery-status`.
async fn status_bodies(messenger: &Messenger) -> Vec<StatusBody> {
    let uuid = messenger
        .directory()
        .resolve(STATUS)
        .expect("the status channel is declared")
        .uuid;
    let conn = messenger.db().lock().await;
    load_channel_retained_window_seq(&conn, uuid, Depth::Unbounded)
        .into_iter()
        .map(|(_, envelope)| {
            assert_eq!(envelope.sender, DELIVERY_STATUS_SENDER, "{envelope:?}");
            delivery::decode(envelope.body.as_text().expect("a status is text"))
                .expect("a v1 status")
        })
        .collect()
}

/// Commit `n` messages naming [`STATUS`] to `ha`'s egress channel and move
/// the participant's position past every message the channel holds, as a
/// reader that handed them would. The rig's sessions never connect, so the
/// reader itself hands nothing.
async fn hand_named(messenger: &Messenger, n: usize) {
    let sender = ParticipantId::for_app(STATUS_SENDER, messenger.source());
    commit_as(messenger, "ha", n, sender.as_str(), Some(STATUS)).await;
    let address = mqtt_egress_address("ha");
    let window = messenger
        .store_for_address(&address)
        .window(&participant(), Depth::Bounded(256), Depth::Bounded(0))
        .await
        .expect("boot attached the position");
    let new = window.new_entries();
    let (Some((first, _)), Some((last, _))) = (new.first(), new.last()) else {
        return;
    };
    messenger
        .advance_subscriber(&address, &participant(), *last, *first, MQTT_OUTBOUND_NOISE)
        .await
        .expect("boot attached the position");
}

/// What a process that ended without its stop saw at boot: the bodies on
/// [`STATUS`] and `ha`'s newest connection state.
struct Crashed {
    statuses: Vec<StatusBody>,
    state: MqttConnectionState,
}

/// Boot over `db` on a runtime of its own, hand `n` messages naming
/// [`STATUS`], and end that runtime with nothing stopped, as a kill does:
/// every task of the process ends at its next await and none outlives it.
fn boot_and_crash(
    tree: &Tree,
    components: &std::path::Path,
    db: &brenn_db::Db,
    n: usize,
) -> Crashed {
    tokio::task::block_in_place(|| {
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .expect("a runtime for the first process");
                    let (booted, crashed) = runtime.block_on(async {
                        let booted = boot_over(tree, components, Some(db.clone())).await;
                        let crashed = Crashed {
                            statuses: status_bodies(&booted.messenger).await,
                            state: newest_state(&booted.messenger, "ha").await,
                        };
                        hand_named(&booted.messenger, n).await;
                        (booted, crashed)
                    });
                    drop(runtime);
                    drop(booted);
                    crashed
                })
                .join()
                .expect("the crashed process's thread")
        })
    })
}

fn marker(found_at_ms: i64) -> StatusBody {
    StatusBody::Restarted(TransportRestarted {
        found_at_ms,
        mqtt: Some(MqttRestarted {
            client: "ha".to_string(),
        }),
    })
}

/// A first boot finds no record; a process ended through the shutdown leaves
/// `stopped` as the newest state, and the next boot reads it as clean and
/// tells nobody.
#[tokio::test(flavor = "multi_thread")]
async fn a_clean_shutdown_is_read_clean_at_the_next_boot() {
    let tree = Tree::holding(&status_document(&[("ha", 8883u16, None)]));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let db = brenn_server::test_support::init_db_memory();

    let first = boot_over(&tree, components.path(), Some(db.clone())).await;
    let state = newest_state(&first.messenger, "ha").await;
    assert_eq!(state.previous_session.end, PreviousEnd::None);
    assert_eq!(
        connection_states(&first.messenger, "ha").await.len(),
        1,
        "one state at boot"
    );
    hand_named(&first.messenger, 2).await;
    let messenger = Arc::clone(&first.messenger);
    shut_down(first).await;
    assert_eq!(
        newest_state(&messenger, "ha").await.connection,
        Connection::Stopped
    );

    let second = boot_over(&tree, components.path(), Some(db)).await;
    let state = newest_state(&second.messenger, "ha").await;
    assert_eq!(state.previous_session.end, PreviousEnd::Clean);
    assert!(status_bodies(&second.messenger).await.is_empty());
    assert!(
        !second
            .alerts()
            .await
            .iter()
            .any(|(_, title, _)| title.contains("restarted uncleanly")),
    );
    shut_down(second).await;
}

/// A process that ended without its stop leaves a record that is not
/// `stopped`: the next boot tells the status channel once, before anything
/// of its own, and says so on the state channel with the marker's instant. A
/// boot after a second unclean end tells it again. After a clean restart the
/// record reads clean and nobody is told, and the new `found_at_ms` is later
/// than every message the crashed process handed — the comparison a sender
/// resolves an unanswered `handed` by.
#[tokio::test(flavor = "multi_thread")]
async fn an_unclean_end_tells_each_status_channel_and_a_clean_restart_does_not() {
    let tree = Tree::holding(&status_document(&[("ha", 8883u16, None)]));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let db = brenn_server::test_support::init_db_memory();

    let first = boot_and_crash(&tree, components.path(), &db, 2);
    assert!(first.statuses.is_empty());
    assert_eq!(first.state.previous_session.end, PreviousEnd::None);
    let handed_by = Utc::now().timestamp_millis();

    let second = boot_and_crash(&tree, components.path(), &db, 0);
    let found = second.state.previous_session.found_at_ms;
    assert_eq!(second.state.previous_session.end, PreviousEnd::Unclean);
    assert_eq!(second.statuses, [marker(found)]);

    let third = boot_over(&tree, components.path(), Some(db.clone())).await;
    let state = newest_state(&third.messenger, "ha").await;
    assert_eq!(state.previous_session.end, PreviousEnd::Unclean);
    assert_eq!(
        status_bodies(&third.messenger).await,
        [marker(found), marker(state.previous_session.found_at_ms)],
        "the messages behind the position are still there to tell"
    );
    let alerts = third.alerts().await;
    assert!(
        alerts.iter().any(|(severity, title, body)| {
            matches!(severity, AlertSeverity::Warning)
                && title == "MQTT client restarted uncleanly: ha"
                && body.contains("1 status channel(s)")
        }),
        "{alerts:?}"
    );
    shut_down(third).await;

    let fourth = boot_over(&tree, components.path(), Some(db)).await;
    let state = newest_state(&fourth.messenger, "ha").await;
    assert_eq!(state.previous_session.end, PreviousEnd::Clean);
    assert_eq!(
        status_bodies(&fourth.messenger).await.len(),
        2,
        "no new marker"
    );
    assert!(state.previous_session.found_at_ms > handed_by);
    shut_down(fourth).await;
}

/// A client a reload adds gets a state channel holding one state with no
/// previous session; a client a reload removes ends on `stopped` and its
/// channel leaves the directory; the slug added back again starts with no
/// previous session.
#[tokio::test(flavor = "multi_thread")]
async fn a_reload_begins_an_added_client_and_finishes_a_removed_one() {
    let one = [("ha", 8883u16, None)];
    let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
    let tree = Tree::holding(&status_document(&one));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_over(&tree, components.path(), None).await;

    tree.write(&status_document(&two));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    let added = connection_states(&booted.messenger, "spare").await;
    let [state] = added.as_slice() else {
        panic!("one state for the added client, got {added:?}");
    };
    assert_eq!(state.previous_session.end, PreviousEnd::None);
    assert_ne!(state.connection, Connection::Stopped);

    tree.write(&status_document(&one));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_removed, vec!["spare".to_string()]);
    assert_eq!(
        newest_state(&booted.messenger, "spare").await.connection,
        Connection::Stopped
    );
    assert!(
        booted
            .messenger
            .directory()
            .resolve(&mqtt_connection_address("spare"))
            .is_none()
    );

    tree.write(&status_document(&two));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    let state = newest_state(&booted.messenger, "spare").await;
    assert_eq!(state.previous_session.end, PreviousEnd::None);
    assert_ne!(state.connection, Connection::Stopped);
    booted.stop_mqtt();
}

/// A reload that replaces a client's session writes no `stopped`, and every
/// state carries the previous session the client was opened with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_client_keeps_its_previous_session_and_is_never_stopped() {
    let tree = Tree::holding(&status_document(&[("ha", 8883u16, None)]));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_over(&tree, components.path(), None).await;
    let previous = newest_state(&booted.messenger, "ha").await.previous_session;

    tree.write(&status_document(&[("ha", 8885u16, None)]));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);
    assert_eq!(status.delta.mqtt_clients_changed, vec!["ha".to_string()]);

    let states = connection_states(&booted.messenger, "ha").await;
    assert!(
        states
            .iter()
            .all(|state| state.connection != Connection::Stopped
                && state.previous_session == previous),
        "{states:?}"
    );
    booted.stop_mqtt();
}

/// A process that ended without its stop, over a state channel that holds
/// nothing — the first boot of a version that writes the record, over a
/// database an earlier one wrote — is read as no previous session: the record
/// says nothing, so nobody is told, though the position resumes with handed
/// messages behind it.
#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_position_with_an_empty_state_channel_reads_none() {
    let tree = Tree::holding(&status_document(&[("ha", 8883u16, None)]));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let db = brenn_server::test_support::init_db_memory();

    boot_and_crash(&tree, components.path(), &db, 2);
    {
        let uuid = mqtt_connection_channel_uuid(&mqtt_connection_address("ha"));
        let conn = db.lock().await;
        let deleted = conn
            .execute(
                "DELETE FROM messaging_messages WHERE channel_uuid = ?1",
                [uuid.as_bytes().to_vec()],
            )
            .expect("the state rows delete");
        assert_eq!(deleted, 1, "the crashed process wrote one state");
    }

    let second = boot_over(&tree, components.path(), Some(db)).await;
    let states = connection_states(&second.messenger, "ha").await;
    let [state] = states.as_slice() else {
        panic!("one state from the second process, got {states:?}");
    };
    assert_eq!(state.previous_session.end, PreviousEnd::None);
    assert!(status_bodies(&second.messenger).await.is_empty());
    assert!(
        !second
            .alerts()
            .await
            .iter()
            .any(|(_, title, _)| title.contains("restarted uncleanly")),
    );
    shut_down(second).await;
}

/// [`status_document`] for `clients`, with a consumer whose `in` port reads
/// `spare`'s connection state when `watching` holds.
fn watching_document(clients: &[(&str, u16, Option<&std::path::Path>)], watching: bool) -> String {
    use brenn_lib::config::PACKAGED;
    let mut document = status_document(clients);
    if watching {
        document.push_str(&format!(
            r#"{PACKAGED}component StateWatch {{
    abi = processor;
    requires = [ports];
    in state;
    out digest;
}}
{PACKAGED}
new state-watch: StateWatch {{
    grants = [ports];
    in state <- "brenn:mqtt-connection.spare" {{ push_depth = 1; retain_depth = 1; }}
    out digest -> sink;
}}
"#
        ));
    }
    document
}

/// A reload that adds a client and, in the same document, a component whose
/// `in` port reads that client's connection state: the channel is minted, its
/// first state is written, and the component is a subscriber of it.
#[tokio::test(flavor = "multi_thread")]
async fn a_component_added_with_its_client_reads_the_clients_state() {
    let one = [("ha", 8883u16, None)];
    let two = [("ha", 8883u16, None), ("spare", 8884u16, None)];
    let tree = Tree::holding(&watching_document(&one, false));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_over(&tree, components.path(), None).await;

    tree.write(&watching_document(&two, true));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Applied, "{:?}", status.refusals);

    let entry = booted
        .messenger
        .directory()
        .resolve(&mqtt_connection_address("spare"))
        .expect("the reload minted spare's state channel");
    assert!(
        entry.subscribers.iter().any(|sub| matches!(
            &sub.kind,
            brenn_lib::messaging::SubscriberEntryKind::Wasm(slug) if slug == "state-watch"
        )),
        "the component reads spare's state: {:?}",
        entry.subscribers
    );
    let states = connection_states(&booted.messenger, "spare").await;
    let [state] = states.as_slice() else {
        panic!("one state for the added client, got {states:?}");
    };
    assert_eq!(state.previous_session.end, PreviousEnd::None);
    booted.stop_mqtt();
}

/// Clients `ha` and, when `spare` holds, `spare`, with the fixture's reader
/// agent granted a runtime subscribe on `spare`'s connection state.
fn runtime_watcher_document(spare: bool) -> String {
    use super::driver::tests::{clients_and_bindings, document_with_agent};
    let mut clients = vec![("ha", 8883u16, None)];
    if spare {
        clients.push(("spare", 8884u16, None));
    }
    document_with_agent(
        &clients_and_bindings(&clients, &[("ha", "home/state")], 1),
        r#"
agent Reader() {
    working_dir = ".";
    grants = [subscribe, publish, dynamic_subscribe];
    send_budget = 1000000;
    acl subscribe [exact reload_outcomes, prefix "brenn:surface.", prefix "ephemeral:surface.", exact "brenn:mqtt-connection.spare"];
    acl publish [exact reload_requests, exact work];
}

new some-reader: Reader();
"#,
    )
}

/// An agent's runtime subscription on a client's connection state holds the
/// channel: a reload that removes the client is refused and needs a restart,
/// and the client and its channel are left as they were.
#[tokio::test(flavor = "multi_thread")]
async fn a_runtime_subscription_on_a_removed_clients_state_needs_a_restart() {
    let tree = Tree::holding(&runtime_watcher_document(true));
    let components = tempfile::tempdir().expect("a components root");
    install_package(components.path(), &staged_module(&tree));
    let mut booted = boot_over(&tree, components.path(), None).await;

    let address = mqtt_connection_address("spare");
    assert_eq!(
        super::driver::tests::runtime_subscribe(&booted, "some-reader", &address).await,
        "subscribed"
    );

    tree.write(&runtime_watcher_document(false));
    install_package(components.path(), &staged_module(&tree));
    booted.driver.reload(TriggerSource::Signal).await;
    let status = booted.last_status().await;
    assert_eq!(status.outcome, Outcome::Refused);
    assert!(
        status
            .refusals
            .iter()
            .any(|refusal| refusal.contains(&address) && refusal.contains(super::NEEDS_RESTART)),
        "{:?}",
        status.refusals
    );
    assert!(booted.messenger.directory().resolve(&address).is_some());
    assert_ne!(
        newest_state(&booted.messenger, "spare").await.connection,
        Connection::Stopped
    );
    booted.stop_mqtt();
}

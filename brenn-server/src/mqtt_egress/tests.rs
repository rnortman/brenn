//! The hub over a real `Messenger` and durable egress channel: positions,
//! the head read and its payloads, the overflow the bus raises under the
//! participant's rung, and the report sink's logging, counts, alerts and
//! episodes.

use std::sync::{Arc, Mutex};

use chrono::Utc;
use uuid::Uuid;

use brenn_envelope::{
    ChannelScheme, MessageBody, MessageEnvelope, MqttExpiry, MqttPublishTransport, TransportMeta,
    Urgency,
};
use brenn_lib::messaging::config::{Depth, MessagingGlobalConfig, SystemChannelTuning};
use brenn_lib::messaging::{MessagingDirectory, SubscriberEntryKind};
use brenn_messaging::mqtt_outbound::{mqtt_egress_entry, mqtt_outbound_spec};
use brenn_messaging::system::fold_spec_subscriptions;
use brenn_messaging::testutils::{TempBlobDir, held, install_blob_store_at, seal_blob};
use brenn_messaging::{DeliveryShape, Messenger, WakeRouter, default_delivery_shape};
use brenn_messaging_store::blob::HeldAttachments;
use brenn_messaging_store::db::{bus_gc_evict_channel, init_db_memory, upsert_channels, utc_to_ns};
use brenn_messaging_store::store::NewMessage;
use brenn_obs::alerting::make_capturing_alerter;

use super::*;

const CLIENT: &str = "home";
const ADDRESS: &str = "brenn:egress/mqtt/home";
/// The egress channel's window in these cases.
const WINDOW: u64 = 4;

/// Records every overflow alarm the bus raises.
#[derive(Default)]
struct AlarmRouter {
    alarms: Mutex<Vec<(String, String, u64)>>,
}

impl AlarmRouter {
    fn alarms(&self) -> Vec<(String, String, u64)> {
        self.alarms.lock().unwrap().clone()
    }
}

#[async_trait]
impl WakeRouter for AlarmRouter {
    async fn deliver(
        &self,
        _key: &SubscriberEntryKind,
        _envelope: &Arc<MessageEnvelope>,
        _retained_seq: i64,
    ) -> Result<bool, String> {
        Ok(false)
    }

    async fn deliver_ingress(
        &self,
        _key: &SubscriberEntryKind,
        _subscriber: &ParticipantId,
        _event: &brenn_messaging_store::ingress::Event,
    ) -> Result<bool, String> {
        Ok(false)
    }

    fn spawn_eager_wake(&self, _key: &SubscriberEntryKind, _subscriber: &ParticipantId) {}

    fn delivery_shape(&self, key: &SubscriberEntryKind) -> DeliveryShape {
        default_delivery_shape(key)
    }

    fn alarm(&self, channel: &str, subscriber: &ParticipantId, count: u64) {
        self.alarms.lock().unwrap().push((
            channel.to_string(),
            subscriber.as_str().to_string(),
            count,
        ));
    }
}

struct Fixture {
    messenger: Arc<Messenger>,
    router: Arc<AlarmRouter>,
    hub: Arc<MqttEgressHub>,
    alerts: AlertDispatcher,
    captured: Arc<Mutex<Vec<(String, String)>>>,
    _blobs: TempBlobDir,
}

impl Fixture {
    async fn new() -> Self {
        let mut entry = mqtt_egress_entry(
            CLIENT,
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );
        entry.resolved_channel.retain_depth = Depth::Bounded(WINDOW);
        entry.resolved_channel.standing_retain_depth = Depth::Bounded(WINDOW);
        let mut entries = vec![entry];
        fold_spec_subscriptions(&mut entries, &[mqtt_outbound_spec([CLIENT])]);
        let db = init_db_memory();
        upsert_channels(&*db.lock().await, &entries);
        let router = Arc::new(AlarmRouter::default());
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from("test"),
            Arc::new(indexmap::IndexMap::new()),
            Arc::clone(&router) as Arc<dyn WakeRouter>,
            MessagingGlobalConfig::default(),
        );
        let blobs = TempBlobDir::new();
        let messenger = install_blob_store_at(messenger, blobs.path()).await;
        let (alerts, captured, _drainer) = make_capturing_alerter();
        let hub = MqttEgressHub::new(Arc::clone(&messenger), alerts.clone());
        Self {
            messenger,
            router,
            hub,
            alerts,
            captured,
            _blobs: blobs,
        }
    }

    /// Commit one message to the client's channel, as the admission would,
    /// with `body` as its payload and `attachments` beside it.
    async fn commit(&self, body: &[u8], attachments: HeldAttachments) -> Uuid {
        let committed = self
            .messenger
            .store_for_address(ADDRESS)
            .append(NewMessage {
                source: "test".to_string(),
                sender: "app:pa-bob".to_string(),
                body: MessageBody::from_parts(Some("application/octet-stream"), body).unwrap(),
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Brenn,
                transport: Some(TransportMeta::MqttPublish(transport("lights/kitchen/set"))),
                reply_to: None,
                delivery_deadline: None,
                impetus: None,
                publish_ts_ns: utc_to_ns(Utc::now()),
                attachments,
            })
            .await;
        committed.committed.message_uuid
    }

    async fn commit_n(&self, n: usize) -> Vec<Uuid> {
        let mut ids = Vec::new();
        for i in 0..n {
            ids.push(
                self.commit(format!("m{i}").as_bytes(), HeldAttachments::none())
                    .await,
            );
        }
        ids
    }

    /// Every alert raised so far, as `(title, body)`.
    async fn alerts(&self) -> Vec<(String, String)> {
        self.alerts.flush().await;
        self.captured.lock().unwrap().clone()
    }

    /// One eviction pass over the client's channel, enacted as the hourly
    /// pass enacts it.
    async fn evict(&self) {
        let entry = self.messenger.directory().resolve(ADDRESS).unwrap();
        let eviction = {
            let conn = self.messenger.db().lock().await;
            bus_gc_evict_channel(
                &conn,
                entry.uuid,
                &entry.address,
                entry.transport_type,
                entry.reap_frontier().expect("a bounded window"),
                entry.resolved_channel.sink,
                None,
            )
        };
        self.messenger
            .enact_overflow_for_channel(ADDRESS, &eviction.overflow);
    }
}

fn transport(topic: &str) -> MqttPublishTransport {
    MqttPublishTransport {
        client_slug: CLIENT.to_string(),
        topic: topic.to_string(),
        qos: 1,
        retain: true,
        content_type: Some("text/plain".to_string()),
        expires: MqttExpiry::Never,
    }
}

fn ids(items: &[EgressItem]) -> Vec<Uuid> {
    items
        .iter()
        .map(|item| match item {
            EgressItem::Message(message) => message.id,
            EgressItem::PayloadGone { id, .. } => *id,
        })
        .collect()
}

fn message(item: &EgressItem) -> &EgressMessage {
    match item {
        EgressItem::Message(message) => message,
        other => panic!("expected a message, got {other:?}"),
    }
}

fn meta(topic: &str) -> EgressMeta {
    EgressMeta {
        id: Uuid::new_v4(),
        sender: "app:pa-bob".to_string(),
        topic: topic.to_string(),
        qos: 1,
    }
}

fn participant() -> String {
    format!("system:{MQTT_OUTBOUND_COMPONENT}")
}

#[tokio::test]
async fn a_fresh_position_skips_what_the_channel_already_retains() {
    let f = Fixture::new().await;
    f.commit_n(2).await;

    let bus = f.hub.open(CLIENT).await;
    assert!(
        bus.unhanded(10).await.is_empty(),
        "messages committed before the position existed are not handed"
    );
    let later = f.commit_n(1).await;
    assert_eq!(ids(&bus.unhanded(10).await), later);
    assert!(f.router.alarms().is_empty(), "the skip outran nothing");
}

#[tokio::test]
async fn an_existing_position_resumes_where_it_was() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    let committed = f.commit_n(3).await;
    let head = bus.unhanded(1).await;
    assert_eq!(ids(&head), committed[..1]);
    bus.advance(message(&head[0]).seq).await;

    let restarted = MqttEgressHub::new(Arc::clone(&f.messenger), f.alerts.clone());
    let bus = restarted.open(CLIENT).await;
    assert_eq!(
        ids(&bus.unhanded(10).await),
        committed[1..],
        "exactly what was not handed"
    );
}

#[tokio::test]
async fn the_head_read_hands_the_oldest_unhanded_with_their_payloads() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    let inline = f.commit(b"on", HeldAttachments::none()).await;
    let blob = seal_blob(&f.messenger, b"a payload carried as a blob");
    let attached = f.commit(b"", held(&[("payload", &blob)])).await;
    drop(blob);
    let third = f.commit(b"off", HeldAttachments::none()).await;

    let head = bus.unhanded(2).await;
    assert_eq!(
        ids(&head),
        vec![inline, attached],
        "oldest first, at most the limit"
    );
    let first = message(&head[0]);
    assert_eq!(first.payload.as_ref(), b"on");
    assert_eq!(first.sender, "app:pa-bob");
    assert_eq!(first.publish, transport("lights/kitchen/set"));
    assert_eq!(
        message(&head[1]).payload.as_ref(),
        b"a payload carried as a blob"
    );
    assert_eq!(
        ids(&bus.unhanded(10).await),
        vec![inline, attached, third],
        "the read moved nothing"
    );
}

#[tokio::test]
async fn a_message_whose_blob_nothing_holds_is_payload_gone() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    f.commit_n(1).await;
    let head = f
        .messenger
        .store_for_address(ADDRESS)
        .unseen_head(&f.hub.participant, Depth::Bounded(WINDOW), 1)
        .await
        .unwrap();
    let (seq, envelope) = &head[0];
    let gone = seal_blob(&f.messenger, b"evicted with its row");
    let mut envelope = MessageEnvelope::clone(envelope);
    envelope.attachments = held(&[("payload", &gone)]).refs().clone();
    drop(gone);

    let egress = ClientEgress {
        hub: Arc::clone(&f.hub),
        client: CLIENT.to_string(),
        address: ADDRESS.to_string(),
        push_depth: Depth::Bounded(WINDOW),
    };
    assert_eq!(
        egress.item(*seq, &envelope).await,
        EgressItem::PayloadGone {
            seq: *seq,
            id: envelope.message_id
        }
    );
    drop(bus);
}

#[tokio::test]
#[should_panic(expected = "not MQTT publish metadata")]
async fn a_message_without_publish_metadata_is_a_panic() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    f.commit_n(1).await;
    let head = f
        .messenger
        .store_for_address(ADDRESS)
        .unseen_head(&f.hub.participant, Depth::Bounded(WINDOW), 1)
        .await
        .unwrap();
    let mut envelope = MessageEnvelope::clone(&head[0].1);
    envelope.transport = None;
    let egress = ClientEgress {
        hub: Arc::clone(&f.hub),
        client: CLIENT.to_string(),
        address: ADDRESS.to_string(),
        push_depth: Depth::Bounded(WINDOW),
    };
    egress.item(head[0].0, &envelope).await;
    drop(bus);
}

#[tokio::test]
#[should_panic(expected = "carries 2 attachments")]
async fn a_message_with_two_attachments_is_a_panic() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    f.commit_n(1).await;
    let head = f
        .messenger
        .store_for_address(ADDRESS)
        .unseen_head(&f.hub.participant, Depth::Bounded(WINDOW), 1)
        .await
        .unwrap();
    let first = seal_blob(&f.messenger, b"one payload");
    let second = seal_blob(&f.messenger, b"another payload");
    let mut envelope = MessageEnvelope::clone(&head[0].1);
    envelope.attachments = held(&[("payload", &first), ("extra", &second)])
        .refs()
        .clone();
    let egress = ClientEgress {
        hub: Arc::clone(&f.hub),
        client: CLIENT.to_string(),
        address: ADDRESS.to_string(),
        push_depth: Depth::Bounded(WINDOW),
    };
    egress.item(head[0].0, &envelope).await;
    drop((bus, first, second));
}

/// The reader's advance over the head read's first entry, past messages the
/// window dropped before the reader reached them, raises the bus's alarm for
/// them under the participant's rung, and the hub reads the tally back.
#[tokio::test]
async fn an_advance_past_an_outrun_window_raises_the_overflow() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    let committed = f.commit_n(WINDOW as usize + 2).await;

    let head = bus.unhanded(1).await;
    assert_eq!(
        ids(&head),
        committed[2..3],
        "the oldest the window still serves"
    );
    bus.advance(message(&head[0]).seq).await;

    assert_eq!(
        f.router.alarms(),
        vec![(ADDRESS.to_string(), participant(), 2)]
    );
    assert_eq!(f.hub.counts(CLIENT).overflow, 2);
    let next = bus.unhanded(1).await;
    bus.advance(message(&next[0]).seq).await;
    assert_eq!(f.router.alarms().len(), 1, "the next advance lost nothing");
}

/// The eviction pass reports the outrun span against the position with no
/// advance by anyone, and the reader's later advance raises nothing more.
#[tokio::test]
async fn an_eviction_raises_the_overflow_once() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    f.commit_n(WINDOW as usize + 3).await;

    f.evict().await;
    assert_eq!(
        f.router.alarms(),
        vec![(ADDRESS.to_string(), participant(), 3)]
    );
    assert_eq!(f.hub.counts(CLIENT).overflow, 3);

    let head = bus.unhanded(1).await;
    bus.advance(message(&head[0]).seq).await;
    assert_eq!(f.router.alarms().len(), 1, "no span is raised twice");
    assert_eq!(f.hub.counts(CLIENT).overflow, 3);

    f.commit_n(WINDOW as usize + 1).await;
    f.evict().await;
    let head = bus.unhanded(1).await;
    bus.advance(message(&head[0]).seq).await;
    let alarms = f.router.alarms();
    assert_eq!(
        alarms.len(),
        2,
        "the second span is raised once: {alarms:?}"
    );
    assert_eq!(
        f.hub.counts(CLIENT).overflow,
        3 + alarms[1].2,
        "every raised span is in the tally"
    );
}

/// `abandon` moves past everything retained and counts only what the window
/// still served; the outrun part is the bus's, raised under the rung.
#[tokio::test]
async fn abandon_counts_the_served_and_the_bus_raises_the_outrun() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    f.commit_n(WINDOW as usize + 1).await;

    assert_eq!(bus.abandon().await, WINDOW);
    assert_eq!(
        f.router.alarms(),
        vec![(ADDRESS.to_string(), participant(), 1)]
    );
    assert!(bus.unhanded(10).await.is_empty());
    assert_eq!(bus.abandon().await, 0, "nothing left");
}

#[tokio::test]
async fn close_detaches_the_position_and_forgets_the_counts() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    bus.report(EgressReport::Delivered(meta("t")));
    assert_eq!(f.hub.counts(CLIENT).delivered, 1);

    f.hub.close(CLIENT).await;
    assert!(
        f.messenger
            .store_for_address(ADDRESS)
            .unseen_head(&f.hub.participant, Depth::Bounded(WINDOW), 1)
            .await
            .is_none(),
        "no position"
    );
    assert_eq!(f.hub.counts(CLIENT), EgressCounts::default());
}

#[tokio::test]
async fn wake_all_wakes_every_reader() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    let mut first = bus.wakes();
    let mut second = bus.wakes();
    f.hub.wake_all();
    first.changed().await.unwrap();
    second.changed().await.unwrap();
}

#[tokio::test]
#[tracing_test::traced_test]
async fn every_failure_kind_is_logged_counted_and_alerted() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    bus.report(EgressReport::Delivered(meta("lights/a")));
    bus.report(EgressReport::Rejected {
        message: meta("lights/b"),
        reason: "not authorized\n".to_string(),
    });
    bus.report(EgressReport::Expired(meta("lights/c")));
    bus.report(EgressReport::Unknown {
        message: meta("lights/d"),
        cause: UnknownCause::ConnectionLost,
    });
    bus.report(EgressReport::Dropped {
        cause: DropCause::ClientRemoved,
        count: 5,
    });

    assert_eq!(
        f.hub.counts(CLIENT),
        EgressCounts {
            delivered: 1,
            rejected: 1,
            expired: 1,
            unknown: 1,
            dropped: 5,
            overflow: 0,
        }
    );
    let alerts = f.alerts().await;
    let titles: Vec<&str> = alerts.iter().map(|(title, _)| title.as_str()).collect();
    assert_eq!(
        titles,
        vec![
            "MQTT egress: rejected (home)",
            "MQTT egress: expired (home)",
            "MQTT egress: outcome unknown (home)",
            "MQTT egress: dropped (home)",
        ],
        "one alert per failure, none for a delivery"
    );
    assert!(alerts[0].1.contains("lights/b"), "{}", alerts[0].1);
    assert!(
        alerts[0].1.contains("not authorized\\n"),
        "the broker's reason, sanitized: {}",
        alerts[0].1
    );
    assert!(alerts[3].1.contains("5 message(s)"), "{}", alerts[3].1);
    assert!(logs_contain("MQTT egress: message not delivered"));
    assert!(logs_contain("MQTT egress: messages dropped unsent"));
}

/// The first failure of a kind alerts and opens its episode; repeats are
/// withheld; a delivery or a drained channel closes every episode with one
/// summary for what it withheld; the next failure opens a new one.
#[tokio::test]
async fn an_episode_alerts_once_and_closes_with_a_summary() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    for _ in 0..3 {
        bus.report(EgressReport::Expired(meta("lights/a")));
    }
    bus.report(EgressReport::Rejected {
        message: meta("lights/b"),
        reason: "quota".to_string(),
    });
    assert_eq!(f.alerts().await.len(), 2, "the first of each kind");
    assert_eq!(f.hub.counts(CLIENT).expired, 3, "every one is counted");

    bus.report(EgressReport::Drained);
    let alerts = f.alerts().await;
    assert_eq!(
        alerts.len(),
        3,
        "one summary, for the kind that withheld: {alerts:?}"
    );
    assert_eq!(alerts[2].0, "MQTT egress: expired (home), summary");
    assert!(alerts[2].1.contains("2 more message(s)"), "{}", alerts[2].1);

    bus.report(EgressReport::Drained);
    bus.report(EgressReport::Expired(meta("lights/a")));
    assert_eq!(f.alerts().await.len(), 4, "a new episode alerts again");

    bus.report(EgressReport::Dropped {
        cause: DropCause::SessionStopped,
        count: 2,
    });
    bus.report(EgressReport::Dropped {
        cause: DropCause::SessionStopped,
        count: 3,
    });
    bus.report(EgressReport::Delivered(meta("lights/a")));
    let alerts = f.alerts().await;
    assert_eq!(alerts.len(), 6, "{alerts:?}");
    assert_eq!(alerts[5].0, "MQTT egress: dropped (home), summary");
    assert!(alerts[5].1.contains("3 more message(s)"), "{}", alerts[5].1);
}

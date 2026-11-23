//! The hub over a real `Messenger` and durable egress channel: positions,
//! the head read and its payloads, the overflow the bus raises under the
//! participant's rung, the report sink's logging, counts, alerts and
//! episodes, and the delivery statuses it publishes.

use std::sync::{Arc, Mutex};

use chrono::Utc;
use uuid::Uuid;

use brenn_envelope::delivery::{self, DELIVERY_REASON_MARKER_ALLOWANCE, DELIVERY_STATUS_SENDER};
use brenn_envelope::grants::AppCapability;
use brenn_envelope::{
    ChannelScheme, MessageBody, MessageEnvelope, MqttExpiry, MqttPublishTransport, TransportMeta,
    Urgency,
};
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::messaging::config::{
    Depth, MessagingGlobalConfig, ResolvedMessagingConfig, SendRate, SystemChannelTuning,
};
use brenn_lib::messaging::test_support::{test_app_config, test_channel_entry};
use brenn_lib::messaging::{MessagingDirectory, SubscriberEntryKind};
use brenn_messaging::mqtt_outbound::{mqtt_egress_entry, mqtt_outbound_spec};
use brenn_messaging::system::fold_spec_subscriptions;
use brenn_messaging::testutils::{TempBlobDir, held, install_blob_store_at, seal_blob};
use brenn_messaging::{DeliveryShape, Messenger, WakeRouter, default_delivery_shape};
use brenn_messaging_store::blob::HeldAttachments;
use brenn_messaging_store::db::{
    bus_gc_evict_channel, init_db_memory, load_channel_retained_tail, upsert_channels, utc_to_ns,
};
use brenn_messaging_store::store::NewMessage;
use brenn_obs::alerting::make_capturing_alerter;

use super::*;

const CLIENT: &str = "home";
const ADDRESS: &str = "brenn:egress/mqtt/home";
/// The egress channel's window in these cases.
const WINDOW: u64 = 4;
/// The server source the fixture's apps run under.
const SOURCE: &str = "test";
/// The app whose status channel is declared.
const BOB: &str = "pa-bob";
const BOB_STATUS: &str = "brenn:bob-status";
/// The app whose status channel is not declared, as after a reload removed it.
const CAROL: &str = "pa-carol";
const CAROL_STATUS: &str = "brenn:carol-status";

/// An app with `MessagingPublish` and a `brenn:` publish ACL covering
/// exactly `status`.
fn status_sender(slug: &str, status: &str) -> brenn_lib::config::AppConfig {
    let mut cfg = test_app_config(
        slug,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec!["bob".to_string()],
    );
    let policy = cfg.policy_mut();
    policy.grants.insert(AppCapability::MessagingPublish);
    policy.acls.brenn_publish.push(ChannelMatcher::Exact(
        status.strip_prefix("brenn:").unwrap().to_string(),
    ));
    cfg
}

fn identity(slug: &str) -> String {
    ParticipantId::for_app(slug, SOURCE).as_str().to_string()
}

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
        Self::with_status_rate(SendRate::default()).await
    }

    /// The fixture, with `send_rate` on Bob's status channel.
    async fn with_status_rate(send_rate: SendRate) -> Self {
        let mut entry = mqtt_egress_entry(
            CLIENT,
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        );
        entry.resolved_channel.retain_depth = Depth::Bounded(WINDOW);
        entry.resolved_channel.standing_retain_depth = Depth::Bounded(WINDOW);
        let mut status = test_channel_entry(BOB_STATUS.strip_prefix("brenn:").unwrap(), vec![]);
        status.resolved_channel.send_rate = send_rate;
        let mut entries = vec![entry, status];
        fold_spec_subscriptions(&mut entries, &[mqtt_outbound_spec([CLIENT])]);
        let mut apps = indexmap::IndexMap::new();
        apps.insert(BOB.to_string(), status_sender(BOB, BOB_STATUS));
        apps.insert(CAROL.to_string(), status_sender(CAROL, CAROL_STATUS));
        let db = init_db_memory();
        upsert_channels(&*db.lock().await, &entries);
        let router = Arc::new(AlarmRouter::default());
        let messenger = Messenger::new(
            db,
            Arc::new(MessagingDirectory::with_entries(entries)),
            Arc::from(SOURCE),
            Arc::new(apps),
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
        self.append(
            "app:pa-bob",
            body,
            attachments,
            transport("lights/kitchen/set"),
        )
        .await
    }

    /// Commit one message from `sender` published as `publish`.
    async fn commit_publish_as(
        &self,
        sender: &str,
        body: &[u8],
        publish: MqttPublishTransport,
    ) -> Uuid {
        self.append(sender, body, HeldAttachments::none(), publish)
            .await
    }

    async fn append(
        &self,
        sender: &str,
        body: &[u8],
        attachments: HeldAttachments,
        publish: MqttPublishTransport,
    ) -> Uuid {
        let committed = self
            .messenger
            .store_for_address(ADDRESS)
            .append(NewMessage {
                source: "test".to_string(),
                sender: sender.to_string(),
                body: MessageBody::from_parts(Some("application/octet-stream"), body).unwrap(),
                urgency: Urgency::Normal,
                envelope_type: ChannelScheme::Brenn,
                transport: Some(TransportMeta::MqttPublish(publish)),
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

    /// Every status on Bob's status channel, oldest first, each checked to
    /// come from the status sender.
    async fn statuses(&self) -> Vec<delivery::DeliveryStatus> {
        let entry = self.messenger.directory().resolve(BOB_STATUS).unwrap();
        let conn = self.messenger.db().lock().await;
        load_channel_retained_tail(&conn, entry.uuid, Depth::Unbounded)
            .into_iter()
            .map(|(_, envelope)| {
                assert_eq!(envelope.sender, DELIVERY_STATUS_SENDER);
                delivery::decode(envelope.body.as_text().unwrap()).unwrap()
            })
            .collect()
    }

    /// Alert titles raised so far.
    async fn titles(&self) -> Vec<String> {
        self.alerts()
            .await
            .into_iter()
            .map(|(title, _)| title)
            .collect()
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
        status_to: None,
    }
}

fn ids(items: &[EgressItem]) -> Vec<Uuid> {
    items
        .iter()
        .map(|item| match item {
            EgressItem::Message(message) => message.id,
            EgressItem::PayloadGone { meta, .. } => meta.id,
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
        status_to: None,
    }
}

/// A message from Bob naming his status channel.
fn bob(topic: &str) -> EgressMeta {
    EgressMeta {
        sender: identity(BOB),
        status_to: Some(BOB_STATUS.to_string()),
        ..meta(topic)
    }
}

/// A message from Carol naming her undeclared status channel.
fn carol(topic: &str) -> EgressMeta {
    EgressMeta {
        sender: identity(CAROL),
        status_to: Some(CAROL_STATUS.to_string()),
        ..meta(topic)
    }
}

fn expired_never(message: EgressMeta) -> EgressReport {
    EgressReport::Expired {
        message,
        sent: Sent::Never,
    }
}

fn titled(titles: &[String], title: &str) -> usize {
    titles.iter().filter(|t| t.as_str() == title).count()
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
            meta: EgressMeta::new(
                envelope.message_id,
                &envelope.sender,
                &transport("lights/kitchen/set")
            ),
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

    assert_eq!(bus.abandon().await.count, WINDOW);
    assert_eq!(
        f.router.alarms(),
        vec![(ADDRESS.to_string(), participant(), 1)]
    );
    assert!(bus.unhanded(10).await.is_empty());
    assert_eq!(bus.abandon().await.count, 0, "nothing left");
}

#[tokio::test]
async fn close_detaches_the_position_and_forgets_the_counts() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    bus.report(EgressReport::Delivered(meta("t"))).await;
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
    bus.report(EgressReport::Delivered(meta("lights/a"))).await;
    bus.report(EgressReport::Rejected {
        message: meta("lights/b"),
        reason: "not authorized\n".to_string(),
        code: Some(135),
        earlier: Sent::Never,
    })
    .await;
    bus.report(expired_never(meta("lights/c"))).await;
    bus.report(EgressReport::Unknown {
        message: meta("lights/d"),
        cause: UnknownCause::ConnectionLost,
    })
    .await;
    bus.report(EgressReport::Dropped {
        cause: DropCause::ClientRemoved,
        count: 5,
    })
    .await;

    assert_eq!(
        f.hub.counts(CLIENT),
        EgressCounts {
            delivered: 1,
            rejected: 1,
            expired: 1,
            unknown: 1,
            dropped: 5,
            status_unpublished: 0,
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
        bus.report(expired_never(meta("lights/a"))).await;
    }
    bus.report(EgressReport::Rejected {
        message: meta("lights/b"),
        reason: "quota".to_string(),
        code: Some(151),
        earlier: Sent::Never,
    })
    .await;
    assert_eq!(f.alerts().await.len(), 2, "the first of each kind");
    assert_eq!(f.hub.counts(CLIENT).expired, 3, "every one is counted");

    bus.report(EgressReport::Drained).await;
    let alerts = f.alerts().await;
    assert_eq!(
        alerts.len(),
        3,
        "one summary, for the kind that withheld: {alerts:?}"
    );
    assert_eq!(alerts[2].0, "MQTT egress: expired (home), summary");
    assert!(alerts[2].1.contains("2 more message(s)"), "{}", alerts[2].1);

    bus.report(EgressReport::Drained).await;
    bus.report(expired_never(meta("lights/a"))).await;
    assert_eq!(f.alerts().await.len(), 4, "a new episode alerts again");

    bus.report(EgressReport::Dropped {
        cause: DropCause::SessionStopped,
        count: 2,
    })
    .await;
    bus.report(EgressReport::Dropped {
        cause: DropCause::SessionStopped,
        count: 3,
    })
    .await;
    bus.report(EgressReport::Delivered(meta("lights/a"))).await;
    let alerts = f.alerts().await;
    assert_eq!(alerts.len(), 6, "{alerts:?}");
    assert_eq!(alerts[5].0, "MQTT egress: dropped (home), summary");
    assert!(alerts[5].1.contains("3 more message(s)"), "{}", alerts[5].1);
}
/// The sanitizer's truncation marker fits in the room a status's size bound
/// leaves for it, so the admission's size check holds for every reason.
#[test]
fn the_truncation_marker_fits_the_reason_allowance() {
    assert!(brenn_common::TRUNCATION_MARKER.len() <= DELIVERY_REASON_MARKER_ALLOWANCE);
}

/// The operator is told a refused status's kind by the same word the encoded
/// status carries as its `kind`.
#[test]
fn the_kind_label_is_the_encoded_kind() {
    use delivery::{DeliveryDropCause as D, DeliveryKind as K, DeliveryUnknownCause as U};
    let kinds = [
        K::Handed,
        K::Delivered,
        K::Rejected {
            reason: "no".to_string(),
        },
        K::Expired,
        K::Unknown {
            cause: U::ConnectionLost,
        },
        K::Unknown {
            cause: U::LifetimeEnded,
        },
        K::Unknown {
            cause: U::ResendRefused,
        },
        K::Dropped { cause: D::Removed },
    ];
    for kind in kinds {
        let label = kind_label(&kind);
        let body = delivery::encode(&delivery::DeliveryStatus {
            message_id: Uuid::nil(),
            kind,
            mqtt: None,
        });
        let json: serde_json::Value = serde_json::from_str(&body).expect("a status is JSON");
        assert_eq!(json["kind"], label, "{body}");
    }
}

/// Every report about a message naming a `status_to` lands its status on that
/// channel, from the status sender, mapped as the table says; the reports that
/// concern no such message publish nothing.
#[tokio::test]
async fn each_report_lands_its_status_by_the_mapping() {
    use delivery::{DeliveryDropCause as D, DeliveryKind as K, DeliveryUnknownCause as U};
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    let message = bob("lights/kitchen/set");
    let reports = [
        (EgressReport::Handed(message.clone()), K::Handed, None),
        (EgressReport::Delivered(message.clone()), K::Delivered, None),
        (
            EgressReport::Rejected {
                message: message.clone(),
                reason: "Not authorized\n".to_string(),
                code: Some(135),
                earlier: Sent::Never,
            },
            K::Rejected {
                reason: "Not authorized\\n".to_string(),
            },
            Some(135),
        ),
        (
            EgressReport::Rejected {
                message: message.clone(),
                reason: "too big".to_string(),
                code: None,
                earlier: Sent::Never,
            },
            K::Rejected {
                reason: "too big".to_string(),
            },
            None,
        ),
        (
            EgressReport::Rejected {
                message: message.clone(),
                reason: "Not authorized".to_string(),
                code: Some(135),
                earlier: Sent::Possibly,
            },
            K::Unknown {
                cause: U::ResendRefused,
            },
            Some(135),
        ),
        (
            EgressReport::Rejected {
                message: message.clone(),
                reason: "too big".to_string(),
                code: None,
                earlier: Sent::Possibly,
            },
            K::Unknown {
                cause: U::ResendRefused,
            },
            None,
        ),
        (expired_never(message.clone()), K::Expired, None),
        (
            EgressReport::Expired {
                message: message.clone(),
                sent: Sent::Possibly,
            },
            K::Unknown {
                cause: U::LifetimeEnded,
            },
            None,
        ),
        (
            EgressReport::Unknown {
                message: message.clone(),
                cause: UnknownCause::ConnectionLost,
            },
            K::Unknown {
                cause: U::ConnectionLost,
            },
            None,
        ),
        (
            EgressReport::Unknown {
                message: message.clone(),
                cause: UnknownCause::SessionStopped,
            },
            K::Unknown { cause: U::Stopped },
            None,
        ),
        (
            EgressReport::DroppedMessage {
                message: message.clone(),
                cause: DropCause::SessionStopped,
                sent: Sent::Never,
            },
            K::Dropped { cause: D::Stopped },
            None,
        ),
        (
            EgressReport::DroppedMessage {
                message: message.clone(),
                cause: DropCause::SessionStopped,
                sent: Sent::Possibly,
            },
            K::Unknown { cause: U::Stopped },
            None,
        ),
        (
            EgressReport::DroppedMessage {
                message: message.clone(),
                cause: DropCause::ClientRemoved,
                sent: Sent::Never,
            },
            K::Dropped { cause: D::Removed },
            None,
        ),
        (
            EgressReport::PayloadGone(message.clone()),
            K::Dropped {
                cause: D::LeftRetention,
            },
            None,
        ),
    ];
    let mut expected = Vec::new();
    for (report, kind, reason_code) in reports {
        bus.report(report).await;
        expected.push(delivery::DeliveryStatus {
            message_id: message.id,
            kind,
            mqtt: Some(delivery::MqttDelivery {
                client: CLIENT.to_string(),
                topic: message.topic.clone(),
                qos: message.qos,
                reason_code,
            }),
        });
    }
    bus.report(EgressReport::Dropped {
        cause: DropCause::ClientRemoved,
        count: 2,
    })
    .await;
    bus.report(EgressReport::Drained).await;
    bus.report(expired_never(meta("lights/kitchen/set"))).await;
    assert_eq!(f.statuses().await, expected);
    assert_eq!(f.hub.counts(CLIENT).status_unpublished, 0);
}

/// A message reported removed that the library may have sent is a host bug:
/// the reader reports removed only messages it never read.
#[test]
#[should_panic(expected = "is reported removed and possibly sent")]
fn a_removed_message_possibly_sent_panics() {
    delivery_status(
        CLIENT,
        &EgressReport::DroppedMessage {
            message: bob("lights/a"),
            cause: DropCause::ClientRemoved,
            sent: Sent::Possibly,
        },
    );
}

/// Every report the reader can make about a message naming a `status_to`,
/// over every reader's cause, both `Sent` values and both rejection codes.
fn every_report(message: &EgressMeta) -> Vec<EgressReport> {
    let mut reports = vec![
        EgressReport::Handed(message.clone()),
        EgressReport::Delivered(message.clone()),
        EgressReport::PayloadGone(message.clone()),
        EgressReport::Drained,
    ];
    for cause in [UnknownCause::ConnectionLost, UnknownCause::SessionStopped] {
        reports.push(EgressReport::Unknown {
            message: message.clone(),
            cause,
        });
    }
    for sent in [Sent::Never, Sent::Possibly] {
        for code in [None, Some(135)] {
            reports.push(EgressReport::Rejected {
                message: message.clone(),
                reason: "no".to_string(),
                code,
                earlier: sent,
            });
        }
        reports.push(EgressReport::Expired {
            message: message.clone(),
            sent,
        });
        for cause in [DropCause::SessionStopped, DropCause::ClientRemoved] {
            if cause == DropCause::ClientRemoved && sent == Sent::Possibly {
                continue;
            }
            reports.push(EgressReport::DroppedMessage {
                message: message.clone(),
                cause,
                sent,
            });
            reports.push(EgressReport::Dropped { cause, count: 2 });
        }
    }
    reports
}

/// Whether `report` ends a message that has an unanswered copy: one the
/// library accepted whose notice ended with its connection or was pending at
/// a stop.
fn unanswered(report: &EgressReport) -> bool {
    match report {
        EgressReport::Handed(_)
        | EgressReport::Delivered(_)
        | EgressReport::PayloadGone(_)
        | EgressReport::Drained
        | EgressReport::Dropped { .. } => false,
        EgressReport::Unknown { .. } => true,
        EgressReport::Rejected { earlier, .. } => *earlier == Sent::Possibly,
        EgressReport::Expired { sent, .. } | EgressReport::DroppedMessage { sent, .. } => {
            *sent == Sent::Possibly
        }
    }
}

/// A sender is told `unknown` exactly when the message ended with a copy the
/// library accepted and the broker never answered: never `rejected`,
/// `expired` or `dropped` for such a message, and never `unknown` otherwise.
#[test]
fn a_status_is_unknown_exactly_when_an_accepted_copy_went_unanswered() {
    use delivery::DeliveryKind as K;
    let message = bob("lights/a");
    for report in every_report(&message) {
        let Some((_, status)) = delivery_status(CLIENT, &report) else {
            continue;
        };
        assert_eq!(
            matches!(status.kind, K::Unknown { .. }),
            unanswered(&report),
            "{report:?} told {:?}",
            status.kind
        );
    }
}

/// A refused resend's `unknown` status carries the broker's reason code when
/// the broker answered, and never the reason string.
#[test]
fn a_refused_resend_carries_no_reason_member() {
    for code in [None, Some(135)] {
        let report = EgressReport::Rejected {
            message: bob("lights/a"),
            reason: "Not authorized".to_string(),
            code,
            earlier: Sent::Possibly,
        };
        let (_, status) = delivery_status(CLIENT, &report).expect("bob names a status_to");
        let body: serde_json::Value =
            serde_json::from_str(&delivery::encode(&status)).expect("a status is JSON");
        assert_eq!(body["kind"], "unknown", "{body}");
        assert_eq!(body["cause"], "resend_refused", "{body}");
        assert!(body.get("reason").is_none(), "{body}");
        assert_eq!(
            body["mqtt"].get("reason_code").and_then(|c| c.as_u64()),
            code.map(u64::from),
            "{body}"
        );
    }
}

/// The operator's side ignores `sent`: an expired or dropped message the
/// library may have sent, with no `status_to` and with its status refused,
/// is counted, logged and alerted exactly as one it never sent.
#[tokio::test]
async fn the_operator_is_told_alike_whether_or_not_the_message_was_sent() {
    let untold = meta("lights/a");
    let refused = carol("lights/b");
    let mut seen = Vec::new();
    for sent in [Sent::Never, Sent::Possibly] {
        let f = Fixture::new().await;
        let bus = f.hub.open(CLIENT).await;
        for message in [&untold, &refused] {
            bus.report(EgressReport::Expired {
                message: message.clone(),
                sent,
            })
            .await;
            bus.report(EgressReport::Drained).await;
        }
        bus.report(EgressReport::DroppedMessage {
            message: refused.clone(),
            cause: DropCause::SessionStopped,
            sent,
        })
        .await;
        bus.report(EgressReport::Drained).await;
        seen.push((f.hub.counts(CLIENT), f.alerts().await));
    }
    let (never, possibly) = (&seen[0], &seen[1]);
    assert_eq!(never.0.expired, 2);
    assert_eq!(never.0.dropped, 1);
    let titles: Vec<&str> = never.1.iter().map(|(title, _)| title.as_str()).collect();
    assert_eq!(
        titles,
        [
            "MQTT egress: expired (home)",
            "MQTT egress: status not published (home)",
            "MQTT egress: expired (home)",
            "MQTT egress: status not published (home)",
            "MQTT egress: dropped (home)",
        ]
    );
    assert_eq!(never.0, possibly.0, "the same counts");
    assert_eq!(
        never.1[0], possibly.1[0],
        "the same expired alert, title and body"
    );
    assert_eq!(never.1[2], possibly.1[2]);
    assert_eq!(never.1[4], possibly.1[4], "the same dropped alert");
    assert!(
        possibly.1[3].1.contains("\"unknown\" status"),
        "the refused status is named by its own kind: {}",
        possibly.1[3].1
    );
}

/// The operator's side ignores `earlier`: a rejection of a resend, with no
/// `status_to`, told, and with its status refused, is counted, logged and
/// alerted exactly as a rejection of a first copy, for a broker's refusal and
/// the host's oversize refusal alike. Told, a broker's refusal alerts and says
/// the sender was told; an oversize one raises nothing.
#[tokio::test]
#[tracing_test::traced_test]
async fn the_operator_is_told_alike_whether_or_not_a_rejection_was_a_resend() {
    let untold = meta("lights/a");
    let told = bob("lights/b");
    let refused = carol("lights/c");
    let mut seen = Vec::new();
    for earlier in [Sent::Never, Sent::Possibly] {
        let f = Fixture::new().await;
        let bus = f.hub.open(CLIENT).await;
        for code in [Some(135), None] {
            for message in [&untold, &told, &refused] {
                bus.report(EgressReport::Rejected {
                    message: message.clone(),
                    reason: "Not authorized".to_string(),
                    code,
                    earlier,
                })
                .await;
                bus.report(EgressReport::Drained).await;
            }
        }
        seen.push((f.hub.counts(CLIENT), f.alerts().await, f.statuses().await));
    }
    let (never, possibly) = (&seen[0], &seen[1]);
    assert_eq!(never.0.rejected, 6);
    assert_eq!(never.0, possibly.0, "the same counts");
    let titles: Vec<&str> = never.1.iter().map(|(title, _)| title.as_str()).collect();
    assert_eq!(
        titles,
        [
            "MQTT egress: rejected (home)",
            "MQTT egress: rejected (home)",
            "MQTT egress: status not published (home)",
            "MQTT egress: rejected (home)",
            "MQTT egress: rejected (home)",
            "MQTT egress: status not published (home)",
            "MQTT egress: rejected (home)",
        ],
        "a broker's refusal alerts untold, told and refused; an oversize one untold and refused"
    );
    assert_eq!(never.1.len(), possibly.1.len());
    for (index, (a, b)) in never.1.iter().zip(&possibly.1).enumerate() {
        if a.0 == UNPUBLISHED {
            assert!(a.1.contains("\"rejected\" status"), "{}", a.1);
            assert!(
                b.1.contains("\"unknown\" status"),
                "the refused status is named by its own kind: {}",
                b.1
            );
        } else {
            assert_eq!(a, b, "alert {index}: the same title and body");
        }
    }
    assert!(
        never.1[1]
            .1
            .ends_with("Its sender was told on its status channel."),
        "{}",
        never.1[1].1
    );
    let kinds = |statuses: &[delivery::DeliveryStatus]| -> Vec<String> {
        statuses
            .iter()
            .map(|status| kind_label(&status.kind).to_string())
            .collect()
    };
    assert_eq!(kinds(&never.2), ["rejected", "rejected"]);
    assert_eq!(kinds(&possibly.2), ["unknown", "unknown"]);
    logs_assert(|lines| {
        let rejected = lines
            .iter()
            .filter(|line| line.contains("kind=\"rejected\""))
            .count();
        if rejected == 12 {
            Ok(())
        } else {
            Err(format!("{rejected} rejected log lines, expected 12"))
        }
    });
}

/// A broker's rejection alerts the operator whether or not its sender was
/// told, under the rejected episode, which withholds a repeat until a
/// delivery closes it with a summary. The host's own oversize rejection, and
/// the other failures, told, raise nothing; untold or refused, they alert.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_broker_rejection_alerts_told_or_not() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    let rejected = |message: EgressMeta, code| EgressReport::Rejected {
        message,
        reason: "Not authorized".to_string(),
        code,
        earlier: Sent::Never,
    };
    let told = bob("lights/a");
    bus.report(rejected(told.clone(), Some(135))).await;
    let alerts = f.alerts().await;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0].0, "MQTT egress: rejected (home)");
    assert!(
        alerts[0].1.contains(&told.id.to_string()),
        "{}",
        alerts[0].1
    );
    assert!(
        alerts[0]
            .1
            .ends_with("Its sender was told on its status channel."),
        "{}",
        alerts[0].1
    );
    assert!(logs_contain("told=true"));
    assert_eq!(f.statuses().await.len(), 1, "and its sender was told");

    bus.report(rejected(bob("lights/b"), Some(135))).await;
    assert_eq!(f.alerts().await.len(), 1, "the repeat is withheld");
    bus.report(EgressReport::Delivered(meta("lights/c"))).await;
    let titles = f.titles().await;
    assert_eq!(
        titles,
        [
            "MQTT egress: rejected (home)",
            "MQTT egress: rejected (home), summary",
        ]
    );

    // Told, none of these alerts.
    bus.report(rejected(bob("lights/d"), None)).await;
    bus.report(expired_never(bob("lights/e"))).await;
    bus.report(EgressReport::Unknown {
        message: bob("lights/f"),
        cause: UnknownCause::ConnectionLost,
    })
    .await;
    bus.report(EgressReport::DroppedMessage {
        message: bob("lights/g"),
        cause: DropCause::SessionStopped,
        sent: Sent::Never,
    })
    .await;
    assert_eq!(f.alerts().await.len(), 2, "{:?}", f.titles().await);
    assert_eq!(f.statuses().await.len(), 6);

    // Untold, each alerts, an oversize rejection and a broker's alike.
    bus.report(rejected(meta("lights/h"), None)).await;
    bus.report(EgressReport::Drained).await;
    bus.report(rejected(meta("lights/i"), Some(135))).await;
    bus.report(EgressReport::Drained).await;
    // Refused, the same.
    bus.report(rejected(carol("lights/j"), Some(135))).await;
    let titles = f.titles().await;
    assert_eq!(
        titles[2..],
        [
            "MQTT egress: rejected (home)",
            "MQTT egress: rejected (home)",
            "MQTT egress: status not published (home)",
            "MQTT egress: rejected (home)",
        ]
    );
    let refused = f.alerts().await;
    assert!(
        !refused[5].1.contains("Its sender was told"),
        "{}",
        refused[5].1
    );
}

/// When `report(Handed)` returns the status is committed and the reader's
/// position has not moved.
#[tokio::test]
async fn handed_is_committed_before_the_position_moves() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    let publish = MqttPublishTransport {
        status_to: Some(BOB_STATUS.to_string()),
        ..transport("lights/kitchen/set")
    };
    let id = f.commit_publish_as(&identity(BOB), b"on", publish).await;
    let head = bus.unhanded(1).await;
    let handed = message(&head[0]);
    bus.report(EgressReport::Handed(EgressMeta::new(
        handed.id,
        &handed.sender,
        &handed.publish,
    )))
    .await;

    let statuses = f.statuses().await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].message_id, id);
    assert_eq!(statuses[0].kind, delivery::DeliveryKind::Handed);
    assert_eq!(
        ids(&bus.unhanded(10).await),
        vec![id],
        "the cursor is unmoved"
    );
}

/// A failure with no `status_to` alerts; the same failure told on its
/// `status_to` is logged and counted and does not; the same failure whose
/// status was refused alerts, and so does the refusal.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_told_failure_raises_nothing_and_a_refused_one_alerts() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    bus.report(expired_never(bob("lights/a"))).await;
    assert!(f.alerts().await.is_empty(), "told, so not raised");
    assert_eq!(f.hub.counts(CLIENT).expired, 1, "but counted");
    assert!(logs_contain("MQTT egress: message not delivered"));

    bus.report(expired_never(meta("lights/b"))).await;
    assert_eq!(f.titles().await, ["MQTT egress: expired (home)"]);
    bus.report(EgressReport::Drained).await;

    bus.report(expired_never(carol("lights/c"))).await;
    let alerts = f.alerts().await;
    assert_eq!(
        alerts[1..]
            .iter()
            .map(|(title, _)| title.as_str())
            .collect::<Vec<_>>(),
        [
            "MQTT egress: status not published (home)",
            "MQTT egress: expired (home)",
        ]
    );
    assert!(
        alerts[1].1.contains("\"expired\" status"),
        "{}",
        alerts[1].1
    );
    assert!(alerts[1].1.contains(CAROL_STATUS), "{}", alerts[1].1);
    assert!(
        alerts[1].1.contains("no longer declared"),
        "{}",
        alerts[1].1
    );
    assert_eq!(f.hub.counts(CLIENT).status_unpublished, 1);
    assert!(logs_contain(
        "MQTT egress: a delivery status was not published"
    ));
}

/// The `told` field of every dropped log line, in the order they were logged.
fn dropped_lines_told(lines: &[&str]) -> Vec<String> {
    lines
        .iter()
        .filter(|line| line.contains("kind=\"dropped\""))
        .map(|line| {
            line.split_whitespace()
                .find(|field| field.starts_with("told="))
                .unwrap_or_else(|| panic!("a dropped log line with no told field: {line}"))
                .to_string()
        })
        .collect()
}

/// Asserts the dropped log lines carry exactly `expected` `told` fields.
fn assert_dropped_told(lines: &[&str], expected: &[&str]) -> Result<(), String> {
    let told = dropped_lines_told(lines);
    if told == expected {
        Ok(())
    } else {
        Err(format!(
            "dropped log lines told {told:?}, expected {expected:?}"
        ))
    }
}

/// A dropped message told on its `status_to` is counted, logged `told=true`
/// and raises nothing; one whose status was refused is logged `told=false`
/// and alerts as a drop of one; a counted drop is logged `told=false`.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_dropped_message_alerts_only_when_its_status_is_refused() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    bus.report(EgressReport::DroppedMessage {
        message: bob("lights/a"),
        cause: DropCause::ClientRemoved,
        sent: Sent::Never,
    })
    .await;
    assert!(f.alerts().await.is_empty());
    assert_eq!(f.hub.counts(CLIENT).dropped, 1);
    logs_assert(|lines| assert_dropped_told(lines, &["told=true"]));

    bus.report(EgressReport::DroppedMessage {
        message: carol("lights/b"),
        cause: DropCause::ClientRemoved,
        sent: Sent::Never,
    })
    .await;
    let alerts = f.alerts().await;
    assert_eq!(
        alerts
            .iter()
            .map(|(title, _)| title.as_str())
            .collect::<Vec<_>>(),
        [
            "MQTT egress: status not published (home)",
            "MQTT egress: dropped (home)",
        ]
    );
    assert!(alerts[1].1.contains("1 message(s)"), "{}", alerts[1].1);
    assert!(
        alerts[1].1.contains("the client was removed"),
        "{}",
        alerts[1].1
    );
    assert_eq!(f.hub.counts(CLIENT).dropped, 2);
    logs_assert(|lines| assert_dropped_told(lines, &["told=true", "told=false"]));

    bus.report(EgressReport::Dropped {
        cause: DropCause::SessionStopped,
        count: 3,
    })
    .await;
    assert_eq!(f.hub.counts(CLIENT).dropped, 5);
    logs_assert(|lines| assert_dropped_told(lines, &["told=true", "told=false", "told=false"]));
}

const UNPUBLISHED: &str = "MQTT egress: status not published (home)";
const UNPUBLISHED_SUMMARY: &str = "MQTT egress: status not published (home), summary";

/// Exactly one "status not published" alert before `Drained`, and exactly
/// one summary at it.
async fn assert_one_alert_then_one_summary(f: &Fixture, bus: &Arc<dyn EgressBus>) {
    let before = f.titles().await;
    assert_eq!(titled(&before, UNPUBLISHED), 1, "{before:?}");
    assert_eq!(titled(&before, UNPUBLISHED_SUMMARY), 0, "{before:?}");
    bus.report(EgressReport::Drained).await;
    let after = f.titles().await;
    assert_eq!(titled(&after, UNPUBLISHED), 1, "{after:?}");
    assert_eq!(titled(&after, UNPUBLISHED_SUMMARY), 1, "{after:?}");
}

/// A throttled status channel: once its burst is spent, published and
/// refused statuses alternate as the bucket refills. One alert, one summary.
#[tokio::test(start_paused = true)]
async fn a_throttled_status_channel_alerts_once_per_run() {
    let f = Fixture::with_status_rate(SendRate {
        burst: 2,
        refill_interval_secs: 1,
        refill: 1,
    })
    .await;
    let bus = f.hub.open(CLIENT).await;
    for _ in 0..2 {
        bus.report(EgressReport::Handed(bob("lights/a"))).await;
    }
    for _ in 0..3 {
        bus.report(EgressReport::Handed(bob("lights/a"))).await;
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        bus.report(EgressReport::Handed(bob("lights/a"))).await;
    }
    assert_eq!(f.statuses().await.len(), 5, "the burst and one per refill");
    assert_eq!(f.hub.counts(CLIENT).status_unpublished, 3);
    assert_one_alert_then_one_summary(&f, &bus).await;
}

/// Two senders on one client, one status channel gone and one healthy, their
/// reports interleaved. One alert, one summary.
#[tokio::test]
async fn interleaved_senders_alert_once_per_run() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    for _ in 0..3 {
        bus.report(EgressReport::Handed(carol("lights/a"))).await;
        bus.report(EgressReport::Handed(bob("lights/b"))).await;
        bus.report(EgressReport::Delivered(bob("lights/b"))).await;
    }
    assert_eq!(f.statuses().await.len(), 6);
    assert_eq!(f.hub.counts(CLIENT).status_unpublished, 3);
    assert_one_alert_then_one_summary(&f, &bus).await;
}

/// Deliveries interleaved with refusals close an open `rejected` episode and
/// leave the "status not published" one open.
#[tokio::test]
async fn a_delivery_leaves_the_status_episode_open() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    for _ in 0..3 {
        bus.report(EgressReport::Rejected {
            message: carol("lights/a"),
            reason: "denied".to_string(),
            code: Some(135),
            earlier: Sent::Never,
        })
        .await;
        bus.report(EgressReport::Delivered(bob("lights/b"))).await;
    }
    let titles = f.titles().await;
    assert_eq!(
        titled(&titles, "MQTT egress: rejected (home)"),
        3,
        "each delivery closed the rejected episode: {titles:?}"
    );
    assert_one_alert_then_one_summary(&f, &bus).await;
}

/// `abandon` answers the metadata of the skipped messages that name a
/// `status_to`, beside the count of all it skipped.
#[tokio::test]
async fn abandon_names_the_messages_with_a_status_to() {
    let f = Fixture::new().await;
    let bus = f.hub.open(CLIENT).await;
    f.commit_n(1).await;
    let publish = MqttPublishTransport {
        status_to: Some(BOB_STATUS.to_string()),
        ..transport("lights/hall/set")
    };
    let named = f
        .commit_publish_as(&identity(BOB), b"on", publish.clone())
        .await;
    f.commit_n(1).await;

    let abandoned = bus.abandon().await;
    assert_eq!(abandoned.count, 3);
    assert_eq!(
        abandoned.named,
        vec![EgressMeta::new(named, &identity(BOB), &publish)]
    );
}

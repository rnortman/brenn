//! `Messenger::publish_delivery_status`: a status lands as the status sender
//! on the original sender's current authority, draws no budget and its own
//! send-rate bucket, and each refusal a reload or an operator's setting can
//! cause is answered, never panicked on.

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::config::{Depth, MessagingGlobalConfig, ResolvedMessagingConfig, SendRate};
use crate::db::{init_db_memory, load_channel_retained_tail, read_send_budget, upsert_channels};
use crate::testutils::test_channel_entry;
use crate::{ChannelEntry, MessagingDirectory, WakeRouter};
use brenn_envelope::delivery::{
    self, DELIVERY_STATUS_SENDER, DeliveryDropCause, DeliveryKind, DeliveryStatus, MqttDelivery,
    MqttRestarted, StatusBody, TransportRestarted,
};
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::acl::ChannelMatcher;
use indexmap::IndexMap;
use std::sync::Arc;
use uuid::Uuid;

const SOURCE: &str = "test-source";
const APP: &str = "pa-bob";
const STATUS: &str = "brenn:light-status";

/// The app whose message a status concerns: `MessagingPublish` and a
/// `brenn:` publish ACL covering exactly the status channel.
fn sender_app() -> brenn_lib::config::AppConfig {
    let mut cfg = test_app_config(
        APP,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec!["bob".to_string()],
    );
    let policy = cfg.policy_mut();
    policy.grants.insert(AppCapability::MessagingPublish);
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Exact("light-status".to_string()));
    cfg
}

fn sender() -> String {
    ParticipantId::for_app(APP, SOURCE).as_str().to_owned()
}

struct Fixture {
    m: Arc<Messenger>,
    channel: ChannelEntry,
}

async fn fixture_with(send_rate: SendRate, max_body_bytes: usize) -> Fixture {
    let db = init_db_memory();
    let mut channel = test_channel_entry("light-status", vec![]);
    channel.resolved_channel.send_rate = send_rate;
    upsert_channels(&*db.lock().await, std::slice::from_ref(&channel));
    let mut apps: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
    apps.insert(APP.to_string(), sender_app());
    let m = Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(vec![channel.clone()])),
        Arc::from(SOURCE),
        Arc::new(apps),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig {
            max_body_bytes,
            ..MessagingGlobalConfig::default()
        },
    );
    Fixture { m, channel }
}

async fn fixture() -> Fixture {
    fixture_with(
        SendRate::default(),
        MessagingGlobalConfig::default().max_body_bytes,
    )
    .await
}

fn status(kind: DeliveryKind) -> StatusBody {
    StatusBody::Message(DeliveryStatus {
        message_id: Uuid::from_u128(7),
        kind,
        mqtt: Some(MqttDelivery {
            client: "home".to_string(),
            topic: "lights/kitchen/set".to_string(),
            qos: 1,
            reason_code: None,
        }),
    })
}

impl Fixture {
    async fn rows(&self) -> Vec<crate::MessageEnvelope> {
        let conn = self.m.db.lock().await;
        load_channel_retained_tail(&conn, self.channel.uuid, Depth::Unbounded)
            .into_iter()
            .map(|(_, envelope)| envelope)
            .collect()
    }

    fn rate_drawn_by(&self, who: &str) -> bool {
        self.m
            .send_rate_buckets
            .lock()
            .unwrap()
            .contains_key(&(who.to_owned(), self.channel.uuid))
    }

    async fn publish(&self, kind: DeliveryKind) -> DeliveryStatusPublish {
        self.m
            .publish_delivery_status(&sender(), STATUS, &status(kind))
            .await
    }

    async fn assert_refused(&self, why: DeliveryStatusRefusal) {
        assert_eq!(
            self.publish(DeliveryKind::Handed).await,
            DeliveryStatusPublish::Refused(why)
        );
        assert!(self.rows().await.is_empty(), "nothing committed");
    }

    fn edit_app(&self, edit: impl FnOnce(&mut IndexMap<String, brenn_lib::config::AppConfig>)) {
        let mut apps = (*self.m.apps.load()).clone();
        edit(&mut apps);
        self.m.apps.store(Arc::new(apps));
    }
}

/// The status is on the channel as untyped text from the status sender, and
/// decodes to what was published. It draws a send-rate token under the
/// status sender, none under the app, and touches no send budget.
#[tokio::test]
async fn a_published_status_is_on_the_channel_from_the_status_sender() {
    let f = fixture().await;
    let dropped = DeliveryKind::Dropped {
        cause: DeliveryDropCause::Removed,
    };
    assert_eq!(
        f.publish(DeliveryKind::Handed).await,
        DeliveryStatusPublish::Published
    );
    assert_eq!(
        f.publish(dropped.clone()).await,
        DeliveryStatusPublish::Published
    );
    let rows = f.rows().await;
    let [handed, outcome] = rows.as_slice() else {
        panic!("two rows, got {}", rows.len());
    };
    for (row, kind) in [(handed, DeliveryKind::Handed), (outcome, dropped)] {
        assert_eq!(row.sender, DELIVERY_STATUS_SENDER);
        assert_eq!(row.channel, STATUS);
        assert_eq!(row.body.content_type(), None, "untyped text");
        assert!(row.reply_to.is_none());
        assert!(row.transport.is_none());
        let text = row.body.as_text().expect("a status is text");
        assert_eq!(delivery::decode(text), Ok(status(kind)));
    }
    assert!(f.rate_drawn_by(DELIVERY_STATUS_SENDER));
    assert!(!f.rate_drawn_by(&sender()));
    assert_eq!(read_send_budget(&*f.m.db.lock().await, 1), None);
}

/// The marker, about a transport rather than a message, lands the same way.
#[tokio::test]
async fn a_restarted_status_lands_from_the_status_sender() {
    let f = fixture().await;
    let marker = StatusBody::Restarted(TransportRestarted {
        found_at_ms: 1_795_599_990_000,
        mqtt: Some(MqttRestarted {
            client: "home".to_string(),
        }),
    });
    assert_eq!(
        f.m.publish_delivery_status(&sender(), STATUS, &marker)
            .await,
        DeliveryStatusPublish::Published
    );
    let rows = f.rows().await;
    let [row] = rows.as_slice() else {
        panic!("one row, got {}", rows.len());
    };
    assert_eq!(row.sender, DELIVERY_STATUS_SENDER);
    let text = row.body.as_text().expect("a status is text");
    assert_eq!(delivery::decode(text), Ok(marker));
}

#[tokio::test]
async fn a_channel_gone_from_the_directory_is_refused() {
    let f = fixture().await;
    assert_eq!(
        f.m.publish_delivery_status(&sender(), "brenn:nowhere", &status(DeliveryKind::Handed))
            .await,
        DeliveryStatusPublish::Refused(DeliveryStatusRefusal::ChannelGone)
    );
}

#[tokio::test]
async fn a_removed_app_is_sender_gone() {
    let f = fixture().await;
    f.edit_app(|apps| {
        apps.shift_remove(APP);
    });
    f.assert_refused(DeliveryStatusRefusal::SenderGone).await;
}

#[tokio::test]
async fn an_app_without_the_publish_grant_is_sender_gone() {
    let f = fixture().await;
    f.edit_app(|apps| {
        let policy = apps.get_mut(APP).unwrap().policy_mut();
        policy.grants = brenn_lib::access::GrantSet::default();
        policy.grants.insert(AppCapability::MessagingSubscribe);
    });
    f.assert_refused(DeliveryStatusRefusal::SenderGone).await;
}

/// The stored sender is matched against each app's identity on this server;
/// the same slug from another source is no app here.
#[tokio::test]
async fn a_sender_from_another_source_is_sender_gone() {
    let f = fixture().await;
    let elsewhere = ParticipantId::for_app(APP, "elsewhere");
    assert_eq!(
        f.m.publish_delivery_status(elsewhere.as_str(), STATUS, &status(DeliveryKind::Handed))
            .await,
        DeliveryStatusPublish::Refused(DeliveryStatusRefusal::SenderGone)
    );
    assert!(f.rows().await.is_empty());
}

#[tokio::test]
async fn an_acl_tightened_after_admission_is_sender_not_permitted() {
    let f = fixture().await;
    f.edit_app(|apps| {
        apps.get_mut(APP).unwrap().policy_mut().acls.brenn_publish =
            vec![ChannelMatcher::Exact("records".to_string())];
    });
    f.assert_refused(DeliveryStatusRefusal::SenderNotPermitted)
        .await;
}

/// The bucket is the status sender's on the channel: a burst of one admits
/// one status and refuses the next.
#[tokio::test]
async fn the_channels_send_rate_refuses_the_excess() {
    let f = fixture_with(
        SendRate {
            burst: 1,
            refill_interval_secs: 3600,
            refill: 1,
        },
        MessagingGlobalConfig::default().max_body_bytes,
    )
    .await;
    assert_eq!(
        f.publish(DeliveryKind::Handed).await,
        DeliveryStatusPublish::Published
    );
    assert_eq!(
        f.publish(DeliveryKind::Delivered).await,
        DeliveryStatusPublish::Refused(DeliveryStatusRefusal::RateLimited)
    );
    assert_eq!(f.rows().await.len(), 1);
    assert!(!f.rate_drawn_by(&sender()));
}

#[tokio::test]
async fn a_status_over_the_body_cap_is_refused() {
    let f = fixture_with(SendRate::default(), 64).await;
    let len = delivery::encode(&status(DeliveryKind::Handed)).len();
    f.assert_refused(DeliveryStatusRefusal::BodyTooLarge { len, max: 64 })
        .await;
}

#[tokio::test]
#[should_panic(expected = "MalformedAddress")]
async fn a_malformed_brenn_address_panics() {
    let f = fixture().await;
    f.m.publish_delivery_status(&sender(), "brenn:bad name", &status(DeliveryKind::Handed))
        .await;
}

#[tokio::test]
#[should_panic(expected = "is not a brenn: address")]
async fn an_address_on_another_scheme_panics() {
    let f = fixture().await;
    f.m.publish_delivery_status(&sender(), "ephemeral:ticks", &status(DeliveryKind::Handed))
        .await;
}

const CONSUMER: &str = "lights";

/// A WASM consumer's registered policy: `MessagingPublish` and a `brenn:`
/// publish ACL covering `channels`.
fn consumer_registration(channels: &[&str]) -> brenn_lib::messaging::SubscriberRegistration {
    let mut policy = brenn_lib::access::AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingPublish);
    policy.acls.brenn_publish = channels
        .iter()
        .map(|c| ChannelMatcher::Exact((*c).to_string()))
        .collect();
    brenn_lib::messaging::SubscriberRegistration {
        policy: Arc::new(policy),
        wake: brenn_lib::messaging::WakeEconomics::Eager,
    }
}

fn consumer_kind() -> SubscriberEntryKind {
    SubscriberEntryKind::Wasm(CONSUMER.to_string())
}

fn consumer_sender() -> String {
    ParticipantId::for_wasm(CONSUMER).as_str().to_owned()
}

impl Fixture {
    fn register_consumer(&self, channels: &[&str]) {
        self.m
            .register_subscriber_registration(consumer_kind(), consumer_registration(channels));
    }

    async fn publish_for_consumer(&self) -> DeliveryStatusPublish {
        self.m
            .publish_delivery_status(&consumer_sender(), STATUS, &status(DeliveryKind::Handed))
            .await
    }
}

/// A status about a component's message lands under the live consumer's
/// registered policy, from the status sender, drawing the status sender's
/// rate and nothing of the consumer's.
#[tokio::test]
async fn a_consumers_status_lands_under_its_registered_policy() {
    let f = fixture().await;
    f.register_consumer(&["light-status"]);
    assert_eq!(
        f.publish_for_consumer().await,
        DeliveryStatusPublish::Published
    );
    let rows = f.rows().await;
    let [row] = rows.as_slice() else {
        panic!("one row, got {}", rows.len());
    };
    assert_eq!(row.sender, DELIVERY_STATUS_SENDER);
    let text = row.body.as_text().expect("a status is text");
    assert_eq!(delivery::decode(text), Ok(status(DeliveryKind::Handed)));
    assert!(f.rate_drawn_by(DELIVERY_STATUS_SENDER));
    assert!(!f.rate_drawn_by(&consumer_sender()));
}

/// The app path is unchanged when a consumer is also registered.
#[tokio::test]
async fn an_app_sender_lands_beside_a_registered_consumer() {
    let f = fixture().await;
    f.register_consumer(&[]);
    assert_eq!(
        f.publish(DeliveryKind::Handed).await,
        DeliveryStatusPublish::Published
    );
}

#[tokio::test]
async fn a_consumer_never_registered_is_sender_gone() {
    let f = fixture().await;
    assert_eq!(
        f.publish_for_consumer().await,
        DeliveryStatusPublish::Refused(DeliveryStatusRefusal::SenderGone)
    );
    assert!(f.rows().await.is_empty());
}

#[tokio::test]
async fn a_retired_consumer_is_sender_gone() {
    let f = fixture().await;
    f.register_consumer(&["light-status"]);
    f.m.retire_subscriber_registration(&consumer_kind());
    assert_eq!(
        f.publish_for_consumer().await,
        DeliveryStatusPublish::Refused(DeliveryStatusRefusal::SenderGone)
    );
    assert!(f.rows().await.is_empty());
}

#[tokio::test]
async fn a_consumer_without_the_publish_grant_is_sender_gone() {
    let f = fixture().await;
    let mut registration = consumer_registration(&["light-status"]);
    let mut policy = (*registration.policy).clone();
    policy.grants = brenn_lib::access::GrantSet::default();
    policy.grants.insert(AppCapability::MessagingSubscribe);
    registration.policy = Arc::new(policy);
    f.m.register_subscriber_registration(consumer_kind(), registration);
    assert_eq!(
        f.publish_for_consumer().await,
        DeliveryStatusPublish::Refused(DeliveryStatusRefusal::SenderGone)
    );
}

/// The consumer's policy is read as it stands at report time: rewired so
/// its ACL no longer covers the channel, the status is refused.
#[tokio::test]
async fn a_consumer_rewired_off_the_channel_is_sender_not_permitted() {
    let f = fixture().await;
    f.register_consumer(&["light-status"]);
    f.m.replace_subscriber_registration(&consumer_kind(), consumer_registration(&["records"]));
    assert_eq!(
        f.publish_for_consumer().await,
        DeliveryStatusPublish::Refused(DeliveryStatusRefusal::SenderNotPermitted)
    );
    assert!(f.rows().await.is_empty());
}

/// Only a WASM consumer's identity resolves through the registry: a
/// system component registered under the same name is not a status sender.
#[tokio::test]
async fn a_registered_non_wasm_subscriber_is_not_a_status_sender() {
    let f = fixture().await;
    f.m.register_subscriber_registration(
        SubscriberEntryKind::System(CONSUMER.to_string()),
        consumer_registration(&["light-status"]),
    );
    assert_eq!(
        f.m.publish_delivery_status(
            ParticipantId::for_system(CONSUMER).as_str(),
            STATUS,
            &status(DeliveryKind::Handed),
        )
        .await,
        DeliveryStatusPublish::Refused(DeliveryStatusRefusal::SenderGone)
    );
}

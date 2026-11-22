//! The MQTT admission and `Messenger::publish_mqtt`: each refusal in its
//! order, spending nothing; a committed publish's row on its client's egress
//! channel; the blob carrier for an inline payload over the body cap; the live
//! feed of an egress channel; and the general entries' refusal of an `mqtt:`
//! address.

use super::super::*;
use super::{CountingRouter, test_app_config};
use crate::config::{
    Depth, MessagingGlobalConfig, NoiseLevel, ResolvedMessagingConfig, SystemChannelTuning,
};
use crate::db::{init_db_memory, load_channel_retained_tail, read_send_budget, upsert_channels};
use crate::mqtt_outbound::mqtt_egress_entry;
use crate::testutils::{
    TempBlobDir, covering_system_publisher, install_blob_store_at, seal_blob,
    surface_registrations, test_channel_entry,
};
use crate::{
    AttachmentName, ChannelEntry, MessageEnvelope, MessagingDirectory, MqttExpiry,
    MqttPublishTransport, SubscriberEntry, SubscriberEntryKind, TransportMeta, WakeRouter,
};
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::acl::{ChannelMatcher, MqttPubMatcher};
use brenn_lib::mqtt::{MqttClientDirectory, MqttLifetime, MqttPublishOptions};
use chrono::{Duration, Utc};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

const SOURCE: &str = "test-source";
const APP: &str = "pa-bob";
const SYSTEM: &str = "egress-prober";
const CONVERSATION: i64 = 1;
const CLIENT: &str = "home";
/// A second declared client with its own egress channel.
const OTHER: &str = "away";
/// A client the client table knows and the directory holds no channel for.
const UNCHANNELLED: &str = "loft";
const CLIENT_CAP: u32 = 128 * 1024;
const ADDR: &str = "mqtt:home:lights/kitchen/set";
const OTHER_ADDR: &str = "mqtt:away:lights/kitchen/set";
/// A page subscribed to `home`'s egress channel, fed live.
const PAGE: &str = "egress-watcher";
const EGRESS_HOME: &str = "brenn:egress/mqtt/home";
const ORIGIN: PublishOrigin = PublishOrigin::Conversation { id: CONVERSATION };

struct StubClients(HashMap<String, u32>);

impl MqttClientDirectory for StubClients {
    fn outbound_payload_cap(&self, client: &str) -> Option<u32> {
        self.0.get(client).copied()
    }
}

struct Fixture {
    m: Arc<Messenger>,
    router: Arc<CountingRouter>,
    _blobs: Option<TempBlobDir>,
    /// `home`'s egress channel.
    outbound: Arc<ChannelEntry>,
    /// `away`'s egress channel.
    other: Arc<ChannelEntry>,
    ingress: ChannelEntry,
}

/// What the fixture's app holds: the MQTT grant, a topic ACL admitting
/// `lights/+/set` on `home`, `away` and `loft` and nothing else, and a
/// `brenn:` publish ACL covering every name.
fn publisher() -> brenn_lib::config::AppConfig {
    let mut cfg = test_app_config(
        APP,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec!["bob".to_string()],
    );
    let policy = cfg.policy_mut();
    policy.grants.insert(AppCapability::MqttPublish);
    for client in [CLIENT, OTHER, UNCHANNELLED] {
        policy.acls.mqtt_publish.push(MqttPubMatcher {
            client: client.to_string(),
            topic_filter: "lights/+/set".to_string(),
        });
    }
    policy.grants.insert(AppCapability::MessagingPublish);
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Prefix(String::new()));
    cfg
}

/// `home`'s and `away`'s egress channels, an ingress channel at the exact
/// address the tests publish to, the clients `home`, `away` and `loft`
/// registered at [`CLIENT_CAP`] (`loft` with no egress channel), a blob store,
/// a system publisher whose ACL covers every `brenn:` name, and a page fed
/// live from `home`'s egress channel.
async fn fixture_with(install_clients: bool, install_blobs: bool) -> Fixture {
    let db = init_db_memory();
    let egress = |client: &str| {
        mqtt_egress_entry(
            client,
            &SystemChannelTuning::default(),
            &MessagingGlobalConfig::default(),
        )
    };
    let mut outbound = egress(CLIENT);
    outbound.subscribers.push(SubscriberEntry {
        kind: SubscriberEntryKind::Surface(PAGE.to_string()),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    });
    let other = egress(OTHER);
    let mut ingress = test_channel_entry("unused", vec![]);
    ingress.address = ADDR.to_string();
    ingress.transport_type = crate::ChannelScheme::Mqtt;
    ingress.resolved_channel.non_text = true;
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'bob', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (1, 1, 'active', 'pa-bob', '2024-01-01', '2024-01-01')",
            [],
        )
        .unwrap();
        upsert_channels(&conn, &[outbound.clone(), other.clone(), ingress.clone()]);
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![
        outbound.clone(),
        other.clone(),
        ingress.clone(),
    ]));
    let mut apps: IndexMap<String, brenn_lib::config::AppConfig> = IndexMap::new();
    apps.insert(APP.to_string(), publisher());
    let mut registrations = covering_system_publisher(SYSTEM);
    registrations.extend(surface_registrations(
        [(
            PAGE.to_string(),
            brenn_lib::messaging::test_support::brenn_delivery_policy(ChannelMatcher::Prefix(
                String::new(),
            )),
        )]
        .into(),
    ));
    let router = Arc::new(CountingRouter::default());
    let m = Messenger::new(
        db,
        directory,
        Arc::from(SOURCE),
        Arc::new(apps),
        Arc::clone(&router) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(registrations);
    let (m, blobs) = if install_blobs {
        let blobs = TempBlobDir::new();
        (install_blob_store_at(m, blobs.path()).await, Some(blobs))
    } else {
        (m, None)
    };
    if install_clients {
        m.install_mqtt_clients(Arc::new(StubClients(HashMap::from([
            (CLIENT.to_string(), CLIENT_CAP),
            (OTHER.to_string(), CLIENT_CAP),
            (UNCHANNELLED.to_string(), CLIENT_CAP),
        ]))));
    }
    let resolve = |client: &str| {
        m.directory
            .resolve(&crate::mqtt_egress_address(client))
            .expect("the fixture's directory holds the client's egress channel")
    };
    let (outbound, other) = (resolve(CLIENT), resolve(OTHER));
    Fixture {
        m,
        router,
        _blobs: blobs,
        outbound,
        other,
        ingress,
    }
}

async fn fixture() -> Fixture {
    fixture_with(true, true).await
}

fn seconds(n: u32) -> MqttLifetime {
    MqttLifetime::Seconds(NonZeroU32::new(n).unwrap())
}

fn options() -> MqttPublishOptions {
    MqttPublishOptions {
        qos: 1,
        retain: false,
        lifetime: seconds(60),
        content_type: None,
    }
}

fn sender() -> String {
    ParticipantId::for_app(APP, SOURCE).as_str().to_owned()
}

impl Fixture {
    async fn rows(&self, channel: &ChannelEntry) -> Vec<MessageEnvelope> {
        let conn = self.m.db.lock().await;
        load_channel_retained_tail(&conn, channel.uuid, Depth::Unbounded)
            .into_iter()
            .map(|(_, envelope)| envelope)
            .collect()
    }

    async fn budget(&self) -> Option<u32> {
        read_send_budget(&*self.m.db.lock().await, CONVERSATION)
    }

    fn rate_drawn_on(&self, channel: &ChannelEntry) -> bool {
        self.m
            .send_rate_buckets
            .lock()
            .unwrap()
            .contains_key(&(sender(), channel.uuid))
    }

    fn rate_drawn(&self) -> bool {
        self.rate_drawn_on(&self.outbound)
    }

    /// The blob store's sealed and unsealed counts.
    fn blobs(&self) -> (usize, usize) {
        let usage = self.m.blob_store().expect("a blob store").usage();
        (usage.sealed, usage.unsealed)
    }

    /// A refusal stored nothing on any channel, drew no rate token, spent no
    /// budget and wrote no blob.
    async fn assert_spent_nothing(&self) {
        assert!(self.rows(&self.outbound).await.is_empty(), "nothing stored");
        assert!(self.rows(&self.other).await.is_empty(), "nothing stored");
        assert!(!self.rate_drawn_on(&self.other), "no rate token drawn");
        if self.m.blob_store().is_some() {
            assert_eq!(self.blobs(), (0, 0), "no blob written");
        }
        assert!(
            self.rows(&self.ingress).await.is_empty(),
            "nothing on ingress"
        );
        assert!(!self.rate_drawn(), "no rate token drawn");
        assert_eq!(self.budget().await, None, "no budget row touched");
    }

    async fn send(
        &self,
        addr: &str,
        payload: MqttPayload,
        options: &MqttPublishOptions,
    ) -> Result<MqttCommitted, PublishResult> {
        self.m
            .publish_mqtt(ORIGIN, APP, addr, payload, options)
            .await
    }

    async fn send_inline(&self, bytes: &[u8]) -> Result<MqttCommitted, PublishResult> {
        self.send(ADDR, MqttPayload::Inline(bytes.to_vec()), &options())
            .await
    }

    /// The MQTT admission under the fixture app's policy with every option
    /// argument a bus publish can carry.
    fn admit(
        &self,
        body: &[u8],
        reply_to: Option<&str>,
        deliver_after: Option<chrono::DateTime<Utc>>,
        delivery_deadline: Option<chrono::DateTime<Utc>>,
    ) -> Result<(), PublishResult> {
        let apps = self.m.apps.load();
        let policy = Arc::clone(&apps.get(APP).unwrap().policy);
        self.m
            .admit_mqtt(
                &policy,
                ADDR,
                &options(),
                Some(body.len() as u64),
                reply_to,
                deliver_after,
                delivery_deadline,
            )
            .map(|_| ())
    }
}

fn held(f: &Fixture, entries: &[(&str, &[u8])]) -> HeldAttachments {
    HeldAttachments::new(
        entries
            .iter()
            .map(|(name, bytes)| (AttachmentName::new(*name).unwrap(), seal_blob(&f.m, bytes))),
    )
    .unwrap()
}

fn expect_refused(result: Result<MqttCommitted, PublishResult>) -> PublishResult {
    match result {
        Ok(committed) => panic!("expected a refusal, got {committed:?}"),
        Err(refusal) => refusal,
    }
}

fn mqtt_transport(envelope: &MessageEnvelope) -> &MqttPublishTransport {
    match envelope.transport.as_ref() {
        Some(TransportMeta::MqttPublish(t)) => t,
        other => panic!("expected MqttPublish metadata, got {other:?}"),
    }
}

// ── Refusals, in the admission's order ──────────────────────────────────

#[tokio::test]
async fn a_wildcard_or_malformed_address_is_malformed() {
    let f = fixture().await;
    for addr in [
        "mqtt:home:lights/+/set",
        "mqtt:home:lights/#",
        "mqtt:home",
        "mqtt:bad slug:x",
    ] {
        let refusal = expect_refused(
            f.send(addr, MqttPayload::Inline(b"on".to_vec()), &options())
                .await,
        );
        assert!(
            matches!(&refusal, PublishResult::MalformedAddress(a) if a == addr),
            "{addr}: {refusal:?}"
        );
    }
    f.assert_spent_nothing().await;
}

#[tokio::test]
async fn a_non_mqtt_address_on_the_mqtt_entry_is_malformed() {
    let f = fixture().await;
    for addr in ["brenn:records", "brenn:egress/mqtt/home", "ephemeral:ticks"] {
        let refusal = expect_refused(
            f.send(addr, MqttPayload::Inline(b"on".to_vec()), &options())
                .await,
        );
        assert!(
            matches!(refusal, PublishResult::MalformedAddress(_)),
            "{addr}: {refusal:?}"
        );
    }
    f.assert_spent_nothing().await;
}

#[tokio::test]
async fn a_publisher_without_the_grant_is_missing_sender() {
    let f = fixture().await;
    let apps = {
        let mut apps = (*f.m.apps.load()).clone();
        // Every grant but `MqttPublish`; the topic ACL stays.
        let policy = apps.get_mut(APP).unwrap().policy_mut();
        policy.grants = brenn_lib::access::GrantSet::default();
        policy.grants.insert(AppCapability::MessagingPublish);
        Arc::new(apps)
    };
    f.m.apps.store(apps);
    let refusal = expect_refused(f.send_inline(b"on").await);
    assert!(
        matches!(refusal, PublishResult::MissingSender),
        "{refusal:?}"
    );
    f.assert_spent_nothing().await;
}

/// The ACL is `allows_mqtt_publish`'s: the same client, one topic inside the
/// filter and one outside it.
#[tokio::test]
async fn the_topic_acl_decides() {
    let f = fixture().await;
    let refusal = expect_refused(
        f.send(
            "mqtt:home:lights/kitchen/get",
            MqttPayload::Inline(b"on".to_vec()),
            &options(),
        )
        .await,
    );
    assert!(
        matches!(&refusal, PublishResult::AclDenied(a) if a == "mqtt:home:lights/kitchen/get"),
        "{refusal:?}"
    );
    f.assert_spent_nothing().await;
    assert!(f.send_inline(b"on").await.is_ok());
}

#[tokio::test]
async fn an_unregistered_client_is_unknown() {
    let f = fixture().await;
    let apps = {
        let mut apps = (*f.m.apps.load()).clone();
        apps.get_mut(APP)
            .unwrap()
            .policy_mut()
            .acls
            .mqtt_publish
            .push(MqttPubMatcher {
                client: "attic".to_string(),
                topic_filter: "#".to_string(),
            });
        Arc::new(apps)
    };
    f.m.apps.store(apps);
    let refusal = expect_refused(
        f.send(
            "mqtt:attic:fan/set",
            MqttPayload::Inline(b"on".to_vec()),
            &options(),
        )
        .await,
    );
    assert!(
        matches!(&refusal, PublishResult::UnknownChannel(a) if a == "mqtt:attic:fan/set"),
        "{refusal:?}"
    );
    f.assert_spent_nothing().await;
}

/// A client the table knows whose egress channel is not in the directory, as
/// while a reload adds or removes it, is unknown too, and nothing panics.
#[tokio::test]
async fn a_known_client_with_no_egress_channel_is_unknown() {
    let f = fixture().await;
    let addr = "mqtt:loft:lights/kitchen/set";
    let refusal = expect_refused(
        f.send(addr, MqttPayload::Inline(b"on".to_vec()), &options())
            .await,
    );
    assert!(
        matches!(&refusal, PublishResult::UnknownChannel(a) if a == addr),
        "{refusal:?}"
    );
    f.assert_spent_nothing().await;
}

#[tokio::test]
async fn a_messenger_with_no_client_table_knows_no_client() {
    let f = fixture_with(false, true).await;
    let refusal = expect_refused(f.send_inline(b"on").await);
    assert!(
        matches!(refusal, PublishResult::UnknownChannel(_)),
        "{refusal:?}"
    );
    f.assert_spent_nothing().await;
}

/// `deliver_after` is refused whether it is past or future, and the two other
/// options a broker cannot carry are refused on the same ground.
#[tokio::test]
async fn the_bus_options_are_refused_whenever_stated() {
    let f = fixture().await;
    let future = Utc::now() + Duration::hours(1);
    let past = Utc::now() - Duration::hours(1);
    let cases: [(Result<(), PublishResult>, MqttRefusal); 4] = [
        (
            f.admit(b"on", None, Some(future), None),
            MqttRefusal::DeliverAfter,
        ),
        (
            f.admit(b"on", None, Some(past), None),
            MqttRefusal::DeliverAfter,
        ),
        (
            f.admit(b"on", Some("brenn:replies"), None, None),
            MqttRefusal::ReplyTo,
        ),
        (
            f.admit(b"on", None, None, Some(future)),
            MqttRefusal::DeliveryDeadline,
        ),
    ];
    for (result, expected) in cases {
        match result {
            Err(PublishResult::MqttRefused(r)) => assert_eq!(r, expected),
            other => panic!("expected MqttRefused({expected:?}), got {other:?}"),
        }
    }
    assert!(f.admit(b"on", None, None, None).is_ok());
}

#[tokio::test]
async fn invalid_options_are_refused() {
    let f = fixture().await;
    let bad_qos = MqttPublishOptions {
        qos: 3,
        ..options()
    };
    let refusal = expect_refused(
        f.send(ADDR, MqttPayload::Inline(b"on".to_vec()), &bad_qos)
            .await,
    );
    assert!(
        matches!(
            &refusal,
            PublishResult::MqttRefused(MqttRefusal::InvalidQos { qos: 3 })
        ),
        "{refusal:?}"
    );
    let bad_type = MqttPublishOptions {
        content_type: Some("not a type".to_string()),
        ..options()
    };
    let refusal = expect_refused(
        f.send(ADDR, MqttPayload::Inline(b"on".to_vec()), &bad_type)
            .await,
    );
    assert!(
        matches!(
            refusal,
            PublishResult::MqttRefused(MqttRefusal::InvalidContentType { .. })
        ),
        "{refusal:?}"
    );
    f.assert_spent_nothing().await;
}

#[tokio::test]
async fn the_payload_is_the_body_or_one_attachment_beside_an_empty_body() {
    let f = fixture().await;
    let two = held(&f, &[("a", b"one"), ("b", b"two")]);
    let refusal = expect_refused(f.send(ADDR, MqttPayload::Attached(two), &options()).await);
    assert!(
        matches!(
            refusal,
            PublishResult::MqttRefused(MqttRefusal::PayloadShape)
        ),
        "{refusal:?}"
    );

    // A body beside an attachment reaches the admission only through
    // `publish_core`, which every entry shares.
    let one = held(&f, &[("a", b"one")]);
    let result =
        f.m.publish_core(
            ORIGIN,
            PublishPrincipal::App { slug: APP },
            ADDR,
            "beside".into(),
            Urgency::Normal,
            None,
            None,
            None,
            None,
            one,
            Some(super::super::mqtt::MqttTarget {
                options: &options(),
                over_cap: None,
            }),
        )
        .await;
    assert!(
        matches!(
            result,
            PublishResult::MqttRefused(MqttRefusal::PayloadShape)
        ),
        "{result:?}"
    );
    f.assert_spent_nothing().await;
}

#[tokio::test]
async fn a_payload_over_the_client_cap_is_refused_inline_and_attached() {
    let f = fixture().await;
    let over = vec![b'x'; CLIENT_CAP as usize + 1];
    let refusal = expect_refused(f.send_inline(&over).await);
    assert!(
        matches!(
            refusal,
            PublishResult::MqttRefused(MqttRefusal::PayloadOverClientCap { len, cap })
                if len == u64::from(CLIENT_CAP) + 1 && cap == CLIENT_CAP
        ),
        "{refusal:?}"
    );
    let attached = held(&f, &[("payload", &over)]);
    let refusal = expect_refused(
        f.send(ADDR, MqttPayload::Attached(attached), &options())
            .await,
    );
    assert!(
        matches!(
            refusal,
            PublishResult::MqttRefused(MqttRefusal::PayloadOverClientCap { .. })
        ),
        "{refusal:?}"
    );
    f.assert_spent_nothing().await;
}

/// An inline payload at the bus's body cap is the body; one byte over it,
/// and under the client cap, rides as one blob attachment named `payload`,
/// typed by the stated content type, beside an empty body.
#[tokio::test]
async fn an_inline_payload_over_the_body_cap_rides_as_a_blob() {
    let f = fixture().await;
    let max = f.m.max_body_bytes();
    assert!(
        max < CLIENT_CAP as usize,
        "the fixture's cap is above the body cap"
    );
    f.send_inline(&vec![b'x'; max]).await.expect("committed");
    let over: Vec<u8> = (0..=max).map(|i| (i % 251) as u8).collect();
    let typed = MqttPublishOptions {
        content_type: Some("application/cbor".to_string()),
        ..options()
    };
    f.send(ADDR, MqttPayload::Inline(over.clone()), &typed)
        .await
        .expect("committed");
    f.send(ADDR, MqttPayload::Inline(over.clone()), &options())
        .await
        .expect("committed");

    let rows = f.rows(&f.outbound).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].body.as_bytes().len(), max);
    assert!(rows[0].attachments.is_empty());
    for (row, content_type) in [
        (&rows[1], "application/cbor"),
        (&rows[2], "application/octet-stream"),
    ] {
        assert!(row.body.as_bytes().is_empty());
        let name = AttachmentName::new("payload").unwrap();
        let blob = row
            .attachments
            .get(&name)
            .expect("one `payload` attachment");
        assert_eq!(row.attachments.len(), 1);
        assert_eq!(blob.size, over.len() as u64);
        assert_eq!(blob.content_type, content_type);
        let held =
            f.m.hold_attachments(&row.attachments)
                .await
                .expect("a retained row's blob is held");
        let hold = held.hold(&name).unwrap();
        assert_eq!(hold.read(0, over.len()).unwrap(), over);
    }
    // The MQTT Content Type sent is the publisher's, never the blob's.
    assert_eq!(mqtt_transport(&rows[2]).content_type, None);
    assert_eq!(f.blobs(), (2, 0));
}

/// A publish the admission refuses writes no blob, whatever the refusal.
#[tokio::test]
async fn a_refused_publish_over_the_body_cap_writes_no_blob() {
    let f = fixture().await;
    let over_body = vec![b'x'; f.m.max_body_bytes() + 1];
    let over_client = vec![b'x'; CLIENT_CAP as usize + 1];
    for (addr, bytes) in [
        ("mqtt:home:door/set", &over_body),
        ("mqtt:attic:lights/kitchen/set", &over_body),
        ("mqtt:home:lights/+/set", &over_body),
        (ADDR, &over_client),
    ] {
        let refusal = expect_refused(
            f.send(addr, MqttPayload::Inline(bytes.clone()), &options())
                .await,
        );
        assert!(
            matches!(
                refusal,
                PublishResult::AclDenied(_)
                    | PublishResult::MalformedAddress(_)
                    | PublishResult::MqttRefused(MqttRefusal::PayloadOverClientCap { .. })
            ),
            "{addr}: {refusal:?}"
        );
    }
    f.assert_spent_nothing().await;
}

/// The blob is written after the send rate: a rate-limited publish over the
/// body cap writes no blob and spends no budget.
#[tokio::test]
async fn a_rate_limited_publish_over_the_body_cap_writes_no_blob() {
    let f = fixture().await;
    while f.m.draw_send_rate(&sender(), &f.outbound) {}
    let over = vec![b'x'; f.m.max_body_bytes() + 1];
    let refusal = expect_refused(f.send_inline(&over).await);
    assert!(matches!(refusal, PublishResult::RateLimited), "{refusal:?}");
    assert_eq!(f.blobs(), (0, 0), "no blob written");
    assert_eq!(f.budget().await, None, "no budget row touched");
    assert!(f.rows(&f.outbound).await.is_empty(), "nothing stored");
}

/// The blob is written after the send budget: a publish over the body cap from
/// a conversation with no budget left writes no blob.
#[tokio::test]
async fn a_budget_exhausted_publish_over_the_body_cap_writes_no_blob() {
    let f = fixture().await;
    f.m.db
        .lock()
        .await
        .execute(
            "INSERT INTO messaging_send_budget (conversation_id, remaining, last_reset_at) \
             VALUES (?1, 0, '2024-01-01')",
            [CONVERSATION],
        )
        .unwrap();
    let over = vec![b'x'; f.m.max_body_bytes() + 1];
    let refusal = expect_refused(f.send_inline(&over).await);
    assert!(
        matches!(refusal, PublishResult::BudgetExhausted),
        "{refusal:?}"
    );
    assert_eq!(f.blobs(), (0, 0), "no blob written");
    assert!(f.rows(&f.outbound).await.is_empty(), "nothing stored");
}

/// A blob the store refuses lands no message, and the send budget drawn for
/// the publish is refunded.
#[tokio::test]
async fn a_refused_blob_refunds_the_send_budget() {
    let f = fixture().await;
    // Fill the test store's 4 MiB budget with four 1 MiB blobs.
    let filler: Vec<_> = (0..4)
        .map(|_| seal_blob(&f.m, &vec![0u8; 1 << 20]))
        .collect();
    let over = vec![b'x'; f.m.max_body_bytes() + 1];
    let refusal = expect_refused(f.send_inline(&over).await);
    assert!(
        matches!(
            refusal,
            PublishResult::MqttRefused(MqttRefusal::BlobRefused { .. })
        ),
        "{refusal:?}"
    );
    let full = f.m.apps.load().get(APP).unwrap().messaging_send_budget();
    assert_eq!(f.budget().await, Some(full), "the drawn budget is refunded");
    assert!(f.rows(&f.outbound).await.is_empty(), "nothing stored");
    assert_eq!(f.blobs(), (4, 0), "only the filler is held");
    drop(filler);
}

/// With no blob store, an inline payload over the body cap cannot be carried.
#[tokio::test]
async fn an_inline_payload_over_the_body_cap_needs_a_blob_store() {
    let f = fixture_with(true, false).await;
    let max = f.m.max_body_bytes();
    let refusal = expect_refused(f.send_inline(&vec![b'x'; max + 1]).await);
    assert!(
        matches!(
            refusal,
            PublishResult::MqttRefused(MqttRefusal::NeedsBlobStore { len, max_body })
                if len == max as u64 + 1 && max_body == max
        ),
        "{refusal:?}"
    );
    f.assert_spent_nothing().await;
}

/// A denied publisher learns nothing of the payload's size against the client.
#[tokio::test]
async fn an_acl_denial_precedes_the_payload_checks() {
    let f = fixture().await;
    let over = vec![b'x'; CLIENT_CAP as usize + 1];
    let refusal = expect_refused(
        f.send("mqtt:home:door/set", MqttPayload::Inline(over), &options())
            .await,
    );
    assert!(
        matches!(refusal, PublishResult::AclDenied(_)),
        "{refusal:?}"
    );
}

// ── A committed publish ─────────────────────────────────────────────────

#[tokio::test]
async fn a_committed_publish_is_a_brenn_row_on_its_clients_egress_channel() {
    let f = fixture().await;
    let opts = MqttPublishOptions {
        qos: 2,
        retain: true,
        lifetime: seconds(90),
        content_type: Some("application/json".to_string()),
    };
    let before = Utc::now();
    let committed = f
        .send(ADDR, MqttPayload::Inline(br#"{"on":true}"#.to_vec()), &opts)
        .await
        .expect("committed");

    let rows = f.rows(&f.outbound).await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.message_id, committed.message_id);
    assert_eq!(row.channel, "brenn:egress/mqtt/home");
    assert_eq!(row.envelope_type, crate::ChannelScheme::Brenn);
    assert_eq!(row.sender, sender());
    assert_eq!(row.body.as_text(), Some(r#"{"on":true}"#));
    assert_eq!(row.body.content_type(), Some("application/json"));
    let t = mqtt_transport(row);
    assert_eq!(
        *t,
        MqttPublishTransport {
            client_slug: CLIENT.to_string(),
            topic: "lights/kitchen/set".to_string(),
            qos: 2,
            retain: true,
            content_type: Some("application/json".to_string()),
            expires: MqttExpiry::At(row.publish_ts + Duration::seconds(90)),
        }
    );
    let MqttExpiry::At(at) = t.expires else {
        unreachable!()
    };
    assert!(at >= before + Duration::seconds(90));

    // One budget unit, drawn on this client's channel's rate bucket.
    assert_eq!(committed.remaining_budget, Some(99));
    assert_eq!(f.budget().await, Some(99));
    assert!(f.rate_drawn());
    // Nothing lands on another client's channel or on the ingress channel at
    // the same address.
    assert!(f.rows(&f.other).await.is_empty());
    assert!(!f.rate_drawn_on(&f.other));
    assert!(f.rows(&f.ingress).await.is_empty());
}

#[tokio::test]
async fn a_forever_lifetime_never_expires() {
    let f = fixture().await;
    let forever = MqttPublishOptions {
        lifetime: MqttLifetime::Forever,
        ..options()
    };
    f.send(ADDR, MqttPayload::Inline(b"on".to_vec()), &forever)
        .await
        .expect("committed");
    let row = &f.rows(&f.outbound).await[0];
    let t = mqtt_transport(row);
    assert_eq!(t.expires, MqttExpiry::Never);
    assert_eq!(t.content_type, None);
}

/// The send rate is drawn per client: a sender's bucket on one client's
/// channel is not its bucket on another's.
#[tokio::test]
async fn the_send_rate_is_drawn_per_client() {
    let f = fixture().await;
    f.send_inline(b"on").await.expect("committed");
    assert!(f.rate_drawn());
    assert!(!f.rate_drawn_on(&f.other));
    f.send(OTHER_ADDR, MqttPayload::Inline(b"on".to_vec()), &options())
        .await
        .expect("committed");
    assert!(f.rate_drawn_on(&f.other));
    assert_eq!(f.rows(&f.other).await.len(), 1);
    assert_eq!(f.rows(&f.outbound).await.len(), 1);
    assert_eq!(f.m.send_rate_buckets.lock().unwrap().len(), 2);
}

/// A live feed target on an egress channel is handed an envelope whose
/// transport is the retained row's.
#[tokio::test]
async fn the_live_feed_carries_the_retained_rows_transport() {
    let f = fixture().await;
    let committed = f.send_inline(b"on").await.expect("committed");
    let row = &f.rows(&f.outbound).await[0];
    let fed = f.router.fed.lock().await;
    let [envelope] = fed.as_slice() else {
        panic!("one live delivery, got {}", fed.len());
    };
    assert_eq!(envelope.message_id, committed.message_id);
    assert_eq!(envelope.channel, "brenn:egress/mqtt/home");
    assert!(envelope.transport.is_some());
    assert_eq!(envelope.transport, row.transport);
}

/// Text reads as text, typed text keeps its type, and bytes that are not valid
/// for their stated text type are carried exactly as octets.
#[tokio::test]
async fn the_body_is_typed_by_the_content_type_when_the_bytes_fit_it() {
    let f = fixture().await;
    let typed = |ct: Option<&str>| MqttPublishOptions {
        content_type: ct.map(str::to_string),
        ..options()
    };
    let not_utf8 = vec![0xff, 0xfe, 0x00];
    for (bytes, opts) in [
        (b"on".to_vec(), typed(None)),
        (b"on".to_vec(), typed(Some("text/plain"))),
        (not_utf8.clone(), typed(Some("text/plain"))),
    ] {
        f.send(ADDR, MqttPayload::Inline(bytes), &opts)
            .await
            .expect("committed");
    }
    let rows = f.rows(&f.outbound).await;
    assert_eq!(rows[0].body.as_text(), Some("on"));
    assert_eq!(rows[0].body.content_type(), None);
    assert_eq!(rows[1].body.as_text(), Some("on"));
    assert_eq!(rows[1].body.content_type(), Some("text/plain"));
    assert_eq!(rows[2].body.as_bytes(), not_utf8.as_slice());
    assert_eq!(
        rows[2].body.content_type(),
        Some("application/octet-stream")
    );
    // The MQTT Content Type sent is the publisher's, whatever the body says.
    assert_eq!(
        mqtt_transport(&rows[2]).content_type.as_deref(),
        Some("text/plain")
    );
}

#[tokio::test]
async fn an_attached_payload_rides_by_reference_beside_an_empty_body() {
    let f = fixture().await;
    let attached = held(&f, &[("payload", b"\x00\x01binary")]);
    let blob = attached.refs().iter().next().unwrap().1.clone();
    f.send(ADDR, MqttPayload::Attached(attached), &options())
        .await
        .expect("committed");
    let row = &f.rows(&f.outbound).await[0];
    assert!(row.body.as_bytes().is_empty());
    let refs: Vec<_> = row.attachments.iter().map(|(_, r)| r.clone()).collect();
    assert_eq!(refs, vec![blob]);
}

// ── The general entries ─────────────────────────────────────────────────

/// An `mqtt:` address on an entry with no MQTT options is refused at the shape
/// gate, under a policy that would pass every MQTT check.
#[tokio::test]
async fn the_general_entry_refuses_an_mqtt_address() {
    let f = fixture().await;
    let result =
        f.m.publish(
            ORIGIN,
            APP,
            ADDR,
            "on".into(),
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(&result, PublishResult::MalformedAddress(a) if a == ADDR),
        "{result:?}"
    );
    f.assert_spent_nothing().await;
}

/// Nobody writes an egress channel by address: not an app whose `brenn:`
/// ACL covers every name, and not a system component whose ACL does. Both are
/// refused at the shape gate, on the `/` in the name; a system publish skips
/// that gate only for the tool namespaces, and the egress namespace is not one.
#[tokio::test]
async fn an_egress_channel_is_not_a_publish_target() {
    let f = fixture().await;
    let app =
        f.m.publish(
            ORIGIN,
            APP,
            EGRESS_HOME,
            "on".into(),
            Urgency::Normal,
            None,
            None,
            None,
        )
        .await;
    assert!(
        matches!(&app, PublishResult::MalformedAddress(a) if a == EGRESS_HOME),
        "{app:?}"
    );
    let system =
        f.m.publish_from_system(SYSTEM, EGRESS_HOME, "on".into(), Urgency::Normal, None)
            .await;
    assert!(
        matches!(&system, PublishResult::MalformedAddress(a) if a == EGRESS_HOME),
        "{system:?}"
    );
    f.assert_spent_nothing().await;
}

#[tokio::test]
#[should_panic(expected = "install_mqtt_clients called twice")]
async fn the_client_table_is_installed_once() {
    let f = fixture().await;
    f.m.install_mqtt_clients(Arc::new(StubClients(HashMap::new())));
}

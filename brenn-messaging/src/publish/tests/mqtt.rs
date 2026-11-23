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
    StubMqttClients, TempBlobDir, attach_mqtt_egress_reader, covering_system_publisher,
    install_blob_store_at, seal_blob, surface_registrations, test_channel_entry,
};
use crate::{
    AttachmentName, ChannelEntry, MessageBody, MessageEnvelope, MessagingDirectory, MqttExpiry,
    MqttPublishTransport, SubscriberEntry, SubscriberEntryKind, TransportMeta, WakeRouter,
};
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::acl::{ChannelMatcher, MqttMatcher};
use brenn_lib::mqtt::{MqttLifetime, MqttPublishOptions};
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
/// A plain `brenn:` channel in the directory, named as a `status_to`.
const STATUS: &str = "brenn:light-status";
const ORIGIN: PublishOrigin = PublishOrigin::Conversation { id: CONVERSATION };

struct Fixture {
    m: Arc<Messenger>,
    router: Arc<CountingRouter>,
    _blobs: Option<TempBlobDir>,
    /// `home`'s egress channel.
    outbound: Arc<ChannelEntry>,
    /// `away`'s egress channel.
    other: Arc<ChannelEntry>,
    ingress: ChannelEntry,
    /// The channel at [`STATUS`].
    status: ChannelEntry,
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
        policy.acls.mqtt_publish.push(MqttMatcher {
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
    let status = test_channel_entry("light-status", vec![]);
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
        upsert_channels(
            &conn,
            &[
                outbound.clone(),
                other.clone(),
                ingress.clone(),
                status.clone(),
            ],
        );
    }
    let directory = Arc::new(MessagingDirectory::with_entries(vec![
        outbound.clone(),
        other.clone(),
        ingress.clone(),
        status.clone(),
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
        m.install_mqtt_clients(Arc::new(StubMqttClients(HashMap::from([
            (CLIENT.to_string(), CLIENT_CAP),
            (OTHER.to_string(), CLIENT_CAP),
            (UNCHANNELLED.to_string(), CLIENT_CAP),
        ]))));
    }
    for client in [CLIENT, OTHER] {
        attach_mqtt_egress_reader(&m, client).await;
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
        status,
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
        status_to: None,
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
        assert!(
            self.rows(&self.status).await.is_empty(),
            "nothing on the status channel"
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
            .push(MqttMatcher {
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
        status_to: None,
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
            status_to: None,
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
    f.m.install_mqtt_clients(Arc::new(StubMqttClients(HashMap::new())));
}

// ── A component's MQTT publish: admitted at the call, committed at the flush ──

const COMPONENT: &str = "usage-proxy";

fn component_sender() -> String {
    ParticipantId::for_wasm(COMPONENT).as_str().to_owned()
}

fn wasm_mqtt<'a>(
    egress: &'a str,
    client: &'a str,
    topic: &'a str,
    body: &[u8],
    options: MqttPublishOptions,
    none: &'a HeldAttachments,
) -> WasmPublish<'a> {
    WasmPublish {
        channel_address: egress,
        body: MessageBody::carrying(options.content_type.as_deref(), body),
        urgency: Urgency::Normal,
        reply_to: None,
        deliver_after: None,
        attachments: none,
        mqtt: Some(WasmMqttPublish {
            client,
            topic,
            options,
        }),
    }
}

impl Fixture {
    fn policy(&self) -> Arc<brenn_lib::access::AppPolicy> {
        let apps = self.m.apps.load();
        Arc::clone(&apps.get(APP).unwrap().policy)
    }
}

/// The component's entry point answers each case it can reach with the
/// admission's variant for it, and spends nothing deciding.
#[tokio::test]
async fn the_components_admission_answers_each_case() {
    type Expect = fn(&Result<(), PublishResult>) -> bool;
    let f = fixture().await;
    let policy = f.policy();
    let typed = |ct: &str| MqttPublishOptions {
        content_type: Some(ct.to_string()),
        ..options()
    };
    let kitchen = "lights/kitchen/set";
    let cases: Vec<(&str, &str, MqttPublishOptions, u64, Expect)> = vec![
        (CLIENT, kitchen, options(), 2, |r| r.is_ok()),
        (CLIENT, kitchen, typed("text/plain"), 2, |r| r.is_ok()),
        (
            CLIENT,
            "lights/+/set",
            options(),
            2,
            |r| matches!(r, Err(PublishResult::MalformedAddress(a)) if a == "mqtt:home:lights/+/set"),
        ),
        (
            CLIENT,
            "locks/front/set",
            options(),
            2,
            |r| matches!(r, Err(PublishResult::AclDenied(a)) if a == "mqtt:home:locks/front/set"),
        ),
        // The topic ACL is asked before the client table.
        (
            "nowhere",
            kitchen,
            options(),
            2,
            |r| matches!(r, Err(PublishResult::AclDenied(a)) if a == "mqtt:nowhere:lights/kitchen/set"),
        ),
        (
            UNCHANNELLED,
            kitchen,
            options(),
            2,
            |r| matches!(r, Err(PublishResult::UnknownChannel(a)) if a == "mqtt:loft:lights/kitchen/set"),
        ),
        (CLIENT, kitchen, typed("text/"), 2, |r| {
            matches!(
                r,
                Err(PublishResult::MqttRefused(
                    MqttRefusal::InvalidContentType { .. }
                ))
            )
        }),
        (
            CLIENT,
            kitchen,
            MqttPublishOptions {
                qos: 3,
                ..options()
            },
            2,
            |r| {
                matches!(
                    r,
                    Err(PublishResult::MqttRefused(MqttRefusal::InvalidQos {
                        qos: 3
                    }))
                )
            },
        ),
        (CLIENT, kitchen, options(), u64::from(CLIENT_CAP), |r| {
            r.is_ok()
        }),
        (CLIENT, kitchen, options(), u64::from(CLIENT_CAP) + 1, |r| {
            matches!(
                r,
                Err(PublishResult::MqttRefused(MqttRefusal::PayloadOverClientCap { len, cap }))
                    if *len == u64::from(CLIENT_CAP) + 1 && *cap == CLIENT_CAP
            )
        }),
    ];
    for (client, topic, opts, len, expect) in cases {
        let answer = f.m.admit_mqtt_from_wasm(&policy, client, topic, &opts, len);
        assert!(
            expect(&answer),
            "{client}:{topic} {opts:?} {len}: {answer:?}"
        );
    }
    f.assert_spent_nothing().await;
}

#[tokio::test]
async fn the_components_admission_needs_the_grant() {
    let f = fixture().await;
    let mut policy = (*f.policy()).clone();
    policy.grants = brenn_lib::access::GrantSet::default();
    policy.grants.insert(AppCapability::MessagingPublish);
    assert!(matches!(
        f.m.admit_mqtt_from_wasm(&policy, CLIENT, "lights/kitchen/set", &options(), 2),
        Err(PublishResult::MissingSender)
    ));
}

/// The app entry answers each case with the admission's own decision under the
/// app's current policy, and spends nothing deciding.
#[tokio::test]
async fn the_apps_admission_is_the_admissions_decision() {
    let f = fixture().await;
    let typed = |ct: &str| MqttPublishOptions {
        content_type: Some(ct.to_string()),
        ..options()
    };
    let cases: Vec<(&str, MqttPublishOptions, u64)> = vec![
        (ADDR, options(), 2),
        (ADDR, typed("application/json"), 2),
        ("mqtt:home:lights/+/set", options(), 2),
        ("brenn:light-status", options(), 2),
        ("mqtt:home:locks/front/set", options(), 2),
        ("mqtt:nowhere:lights/kitchen/set", options(), 2),
        ("mqtt:loft:lights/kitchen/set", options(), 2),
        (ADDR, typed("text/"), 2),
        (
            ADDR,
            MqttPublishOptions {
                qos: 3,
                ..options()
            },
            2,
        ),
        (ADDR, naming(STATUS), 2),
        (ADDR, naming("brenn:nowhere"), 2),
        (ADDR, naming("light-status"), 2),
        (ADDR, options(), u64::from(CLIENT_CAP)),
        (ADDR, options(), u64::from(CLIENT_CAP) + 1),
    ];
    let policy = f.policy();
    for (addr, opts, len) in cases {
        let answer = f.m.admit_mqtt_for_app(APP, addr, &opts, len);
        let expected =
            f.m.admit_mqtt(&policy, addr, &opts, Some(len), None, None, None)
                .map(|_| ());
        assert_eq!(
            format!("{answer:?}"),
            format!("{expected:?}"),
            "{addr} {opts:?} {len}"
        );
    }
    assert!(f.m.admit_mqtt_for_app(APP, ADDR, &options(), 2).is_ok());
    assert!(matches!(
        f.m.admit_mqtt_for_app(APP, "mqtt:home:locks/front/set", &options(), 2),
        Err(PublishResult::AclDenied(_))
    ));
    f.assert_spent_nothing().await;
}

/// The app entry reads the app table as it stands at the call, and an app the
/// table does not hold is `MissingSender`.
#[tokio::test]
async fn the_apps_admission_reads_the_current_policy_and_knows_no_stranger() {
    let f = fixture().await;
    assert!(matches!(
        f.m.admit_mqtt_for_app("stranger", ADDR, &options(), 2),
        Err(PublishResult::MissingSender)
    ));
    f.edit_policy(|policy| {
        policy.grants = brenn_lib::access::GrantSet::default();
        policy.grants.insert(AppCapability::MessagingPublish);
    });
    assert!(matches!(
        f.m.admit_mqtt_for_app(APP, ADDR, &options(), 2),
        Err(PublishResult::MissingSender)
    ));
    f.assert_spent_nothing().await;
}

/// A component's MQTT entry commits to its client's egress channel as the
/// component's row, with the transport an agent's publish of the same options
/// gets: an expiry counted from the row's own commit time. It draws no send
/// rate and no budget.
#[tokio::test]
async fn a_components_flushed_entry_is_a_row_with_the_agents_transport() {
    let f = fixture().await;
    let opts = MqttPublishOptions {
        qos: 2,
        retain: true,
        lifetime: seconds(90),
        content_type: Some("application/json".to_string()),
        status_to: None,
    };
    let none = HeldAttachments::none();
    f.m.publish_from_wasm(
        COMPONENT,
        &[wasm_mqtt(
            EGRESS_HOME,
            CLIENT,
            "lights/kitchen/set",
            br#"{"on":true}"#,
            opts.clone(),
            &none,
        )],
    )
    .await;
    assert!(!f.rate_drawn_on(&f.outbound), "no send rate drawn");
    assert_eq!(f.budget().await, None, "no budget drawn");
    f.send(ADDR, MqttPayload::Inline(br#"{"on":true}"#.to_vec()), &opts)
        .await
        .expect("the agent's publish commits");

    let rows = f.rows(&f.outbound).await;
    let [component, agent] = rows.as_slice() else {
        panic!("two rows, got {}", rows.len());
    };
    assert_eq!(component.sender, component_sender());
    assert_eq!(component.envelope_type, crate::ChannelScheme::Brenn);
    assert_eq!(component.channel, EGRESS_HOME);
    assert_eq!(component.body, agent.body);
    assert_eq!(component.body.content_type(), Some("application/json"));
    let expect = |row: &MessageEnvelope| MqttPublishTransport {
        client_slug: CLIENT.to_string(),
        topic: "lights/kitchen/set".to_string(),
        qos: 2,
        retain: true,
        content_type: Some("application/json".to_string()),
        expires: MqttExpiry::At(row.publish_ts + Duration::seconds(90)),
        status_to: None,
    };
    assert_eq!(*mqtt_transport(component), expect(component));
    assert_eq!(*mqtt_transport(agent), expect(agent));
}

#[tokio::test]
async fn a_components_forever_publish_never_expires_and_is_fed_its_transport() {
    let f = fixture().await;
    let none = HeldAttachments::none();
    let forever = MqttPublishOptions {
        lifetime: MqttLifetime::Forever,
        ..options()
    };
    f.m.publish_from_wasm(
        COMPONENT,
        &[wasm_mqtt(
            EGRESS_HOME,
            CLIENT,
            "lights/kitchen/set",
            b"",
            forever,
            &none,
        )],
    )
    .await;
    let row = &f.rows(&f.outbound).await[0];
    assert_eq!(mqtt_transport(row).expires, MqttExpiry::Never);
    assert_eq!(
        row.body,
        MessageBody::text(""),
        "an empty payload is an empty body"
    );
    let fed = f.router.fed.lock().await;
    let [envelope] = fed.as_slice() else {
        panic!("one live delivery, got {}", fed.len());
    };
    assert_eq!(envelope.message_id, row.message_id);
    assert!(envelope.transport.is_some());
    assert_eq!(envelope.transport, row.transport);
}

/// One flush commits its MQTT entries to their clients' channels in call
/// order, with strictly increasing timestamps across channels.
#[tokio::test]
async fn a_flush_commits_mqtt_entries_in_call_order() {
    let f = fixture().await;
    let none = HeldAttachments::none();
    let egress_away = crate::mqtt_egress_address(OTHER);
    f.m.publish_from_wasm(
        COMPONENT,
        &[
            wasm_mqtt(EGRESS_HOME, CLIENT, "lights/a/set", b"1", options(), &none),
            wasm_mqtt(&egress_away, OTHER, "lights/b/set", b"2", options(), &none),
            wasm_mqtt(EGRESS_HOME, CLIENT, "lights/c/set", b"3", options(), &none),
        ],
    )
    .await;
    let home = f.rows(&f.outbound).await;
    let away = f.rows(&f.other).await;
    assert_eq!(home.len(), 2);
    assert_eq!(away.len(), 1);
    assert_eq!(mqtt_transport(&home[0]).topic, "lights/a/set");
    assert_eq!(mqtt_transport(&away[0]).topic, "lights/b/set");
    assert_eq!(mqtt_transport(&home[1]).topic, "lights/c/set");
    assert!(home[0].publish_ts < away[0].publish_ts);
    assert!(away[0].publish_ts < home[1].publish_ts);
}

#[tokio::test]
#[should_panic(expected = "MQTT entry for client \"away\" aimed at")]
async fn a_flushed_mqtt_entry_aimed_at_another_clients_channel_panics() {
    let f = fixture().await;
    let none = HeldAttachments::none();
    f.m.publish_from_wasm(
        COMPONENT,
        &[wasm_mqtt(
            EGRESS_HOME,
            OTHER,
            "lights/a/set",
            b"1",
            options(),
            &none,
        )],
    )
    .await;
}

#[tokio::test]
#[should_panic(expected = "carries a reply_to, a release time or an attachment")]
async fn a_flushed_mqtt_entry_with_a_reply_to_panics() {
    let f = fixture().await;
    let none = HeldAttachments::none();
    let mut entry = wasm_mqtt(EGRESS_HOME, CLIENT, "lights/a/set", b"1", options(), &none);
    entry.reply_to = Some(EGRESS_HOME);
    f.m.publish_from_wasm(COMPONENT, &[entry]).await;
}

#[tokio::test]
#[should_panic(expected = "carries a reply_to, a release time or an attachment")]
async fn a_flushed_mqtt_entry_with_a_release_time_panics() {
    let f = fixture().await;
    let none = HeldAttachments::none();
    let mut entry = wasm_mqtt(EGRESS_HOME, CLIENT, "lights/a/set", b"1", options(), &none);
    entry.deliver_after = Some(Utc::now() + Duration::seconds(60));
    f.m.publish_from_wasm(COMPONENT, &[entry]).await;
}

#[tokio::test]
#[should_panic(expected = "carries a reply_to, a release time or an attachment")]
async fn a_flushed_mqtt_entry_with_an_attachment_panics() {
    let f = fixture().await;
    let attached = held(&f, &[("payload", b"1")]);
    let entry = wasm_mqtt(
        EGRESS_HOME,
        CLIENT,
        "lights/a/set",
        b"",
        options(),
        &attached,
    );
    f.m.publish_from_wasm(COMPONENT, &[entry]).await;
}

#[tokio::test]
#[should_panic(expected = "a non-MQTT entry aimed at the egress channel")]
async fn a_flushed_plain_entry_aimed_at_an_egress_channel_panics() {
    let f = fixture().await;
    let none = HeldAttachments::none();
    let mut entry = wasm_mqtt(EGRESS_HOME, CLIENT, "lights/a/set", b"1", options(), &none);
    entry.mqtt = None;
    f.m.publish_from_wasm(COMPONENT, &[entry]).await;
}

/// The invariants are checked before anything is written: a bad entry late in
/// a flush leaves the good ones before it uncommitted.
#[tokio::test]
async fn a_flush_with_a_bad_entry_commits_nothing() {
    let f = fixture().await;
    let m = Arc::clone(&f.m);
    let joined = tokio::spawn(async move {
        let none = HeldAttachments::none();
        let mut bad = wasm_mqtt(EGRESS_HOME, CLIENT, "lights/b/set", b"2", options(), &none);
        bad.mqtt = None;
        m.publish_from_wasm(
            COMPONENT,
            &[
                wasm_mqtt(EGRESS_HOME, CLIENT, "lights/a/set", b"1", options(), &none),
                bad,
            ],
        )
        .await;
    })
    .await;
    assert!(joined.unwrap_err().is_panic());
    assert!(f.rows(&f.outbound).await.is_empty());
}

#[tokio::test]
#[should_panic(expected = "names a status_to, which a component cannot")]
async fn a_flushed_mqtt_entry_naming_a_status_to_panics() {
    let f = fixture().await;
    let none = HeldAttachments::none();
    let naming = MqttPublishOptions {
        status_to: Some(STATUS.to_string()),
        ..options()
    };
    let entry = wasm_mqtt(EGRESS_HOME, CLIENT, "lights/a/set", b"1", naming, &none);
    f.m.publish_from_wasm(COMPONENT, &[entry]).await;
}

// ── status_to ───────────────────────────────────────────────────────────

fn naming(status_to: &str) -> MqttPublishOptions {
    MqttPublishOptions {
        status_to: Some(status_to.to_string()),
        ..options()
    }
}

/// A refusal is `StatusTo` with the address as stated and the given reason.
fn is_status_to(refusal: &PublishResult, address: &str, why: &StatusToRefusal) -> bool {
    matches!(
        refusal,
        PublishResult::MqttRefused(MqttRefusal::StatusTo { address: a, why: w })
            if a == address && w == why
    )
}

impl Fixture {
    /// Replace the fixture app's policy with `edit` applied to it.
    fn edit_policy(&self, edit: impl FnOnce(&mut brenn_lib::access::AppPolicy)) {
        let mut apps = (*self.m.apps.load()).clone();
        edit(apps.get_mut(APP).unwrap().policy_mut());
        self.m.apps.store(Arc::new(apps));
    }
}

#[tokio::test]
async fn a_malformed_status_to_is_refused() {
    let f = fixture().await;
    for address in [
        "light-status",
        "brenn:bad name",
        "ephemeral:ticks",
        "brenn:",
    ] {
        let refusal = expect_refused(
            f.send(ADDR, MqttPayload::Inline(b"on".to_vec()), &naming(address))
                .await,
        );
        assert!(
            is_status_to(&refusal, address, &StatusToRefusal::Malformed),
            "{address}: {refusal:?}"
        );
        assert_eq!(
            refusal.signal_kind(),
            Some(brenn_obs::security::DenialKind::MalformedAddress)
        );
    }
    f.assert_spent_nothing().await;
}

#[tokio::test]
async fn a_status_to_without_the_publish_grant_is_refused() {
    let f = fixture().await;
    f.edit_policy(|policy| {
        policy.grants = brenn_lib::access::GrantSet::default();
        policy.grants.insert(AppCapability::MqttPublish);
    });
    let refusal = expect_refused(
        f.send(ADDR, MqttPayload::Inline(b"on".to_vec()), &naming(STATUS))
            .await,
    );
    assert!(
        is_status_to(&refusal, STATUS, &StatusToRefusal::GrantAbsent),
        "{refusal:?}"
    );
    assert_eq!(
        refusal.signal_kind(),
        Some(brenn_obs::security::DenialKind::MissingSender)
    );
    f.assert_spent_nothing().await;
}

/// The right is the publish right: a channel the publisher may only read is
/// refused, and an address outside the ACL answers the same whether or not
/// the channel exists.
#[tokio::test]
async fn a_status_to_outside_the_publish_acl_is_refused_whether_or_not_it_exists() {
    let f = fixture().await;
    f.edit_policy(|policy| {
        policy.acls.brenn_publish = vec![ChannelMatcher::Exact("records".to_string())];
        policy
            .acls
            .brenn_subscribe
            .push(ChannelMatcher::Exact("light-status".to_string()));
    });
    assert!(
        f.policy().allows_brenn_delivery("light-status"),
        "the channel is readable"
    );
    for address in [STATUS, "brenn:nowhere"] {
        let refusal = expect_refused(
            f.send(ADDR, MqttPayload::Inline(b"on".to_vec()), &naming(address))
                .await,
        );
        assert!(
            is_status_to(&refusal, address, &StatusToRefusal::AclDenied),
            "{address}: {refusal:?}"
        );
        assert_eq!(
            refusal.signal_kind(),
            Some(brenn_obs::security::DenialKind::AclDenied)
        );
    }
    f.assert_spent_nothing().await;
}

#[tokio::test]
async fn a_status_to_not_in_the_directory_is_refused() {
    let f = fixture().await;
    let refusal = expect_refused(
        f.send(
            ADDR,
            MqttPayload::Inline(b"on".to_vec()),
            &naming("brenn:nowhere"),
        )
        .await,
    );
    assert!(
        is_status_to(&refusal, "brenn:nowhere", &StatusToRefusal::Unknown),
        "{refusal:?}"
    );
    assert_eq!(
        refusal.signal_kind(),
        Some(brenn_obs::security::DenialKind::UnknownChannel)
    );
    f.assert_spent_nothing().await;
}

/// A topic so long that a status about it could never fit the body cap is
/// refused at the publish, and an argument error carries no denial kind.
#[tokio::test]
async fn a_status_to_whose_statuses_cannot_fit_the_body_cap_is_refused() {
    let f = fixture().await;
    let max = f.m.max_body_bytes();
    let topic = format!("lights/{}/set", "k".repeat(max - 200));
    let addr = format!("mqtt:{CLIENT}:{topic}");
    let len = brenn_envelope::delivery::max_encoded_len(CLIENT, &topic);
    assert!(len > max, "the fixture topic's statuses are over the cap");
    let refusal = expect_refused(
        f.send(&addr, MqttPayload::Inline(b"on".to_vec()), &naming(STATUS))
            .await,
    );
    assert!(
        is_status_to(
            &refusal,
            STATUS,
            &StatusToRefusal::StatusOverBodyCap { len, max }
        ),
        "{refusal:?}"
    );
    assert_eq!(refusal.signal_kind(), None);
    f.assert_spent_nothing().await;

    // Without a `status_to` the same topic is admitted.
    f.send(&addr, MqttPayload::Inline(b"on".to_vec()), &options())
        .await
        .expect("committed");
}

/// The target's grant, ACL and client are answered before the `status_to`,
/// and the `status_to` before the QoS, content type and payload.
#[tokio::test]
async fn the_status_to_is_asked_after_the_target_and_before_the_arguments() {
    let f = fixture().await;
    let bad = naming("brenn:nowhere");
    let fixture = &f;
    let send = |addr: &'static str, opts: MqttPublishOptions| async move {
        expect_refused(
            fixture
                .send(addr, MqttPayload::Inline(b"on".to_vec()), &opts)
                .await,
        )
    };

    let refusal = send("mqtt:home:lights/kitchen/get", bad.clone()).await;
    assert!(
        matches!(refusal, PublishResult::AclDenied(_)),
        "{refusal:?}"
    );
    let refusal = send("mqtt:loft:lights/kitchen/set", bad.clone()).await;
    assert!(
        matches!(refusal, PublishResult::UnknownChannel(ref a) if a == "mqtt:loft:lights/kitchen/set"),
        "{refusal:?}"
    );

    for opts in [
        MqttPublishOptions {
            qos: 3,
            ..bad.clone()
        },
        MqttPublishOptions {
            content_type: Some("not a type".to_string()),
            ..bad.clone()
        },
    ] {
        let refusal = send(ADDR, opts).await;
        assert!(
            is_status_to(&refusal, "brenn:nowhere", &StatusToRefusal::Unknown),
            "{refusal:?}"
        );
    }
    let two = held(&f, &[("a", b"one"), ("b", b"two")]);
    let refusal = expect_refused(f.send(ADDR, MqttPayload::Attached(two), &bad).await);
    assert!(
        is_status_to(&refusal, "brenn:nowhere", &StatusToRefusal::Unknown),
        "{refusal:?}"
    );

    f.edit_policy(|policy| {
        policy.grants = brenn_lib::access::GrantSet::default();
        policy.grants.insert(AppCapability::MessagingPublish);
    });
    let refusal = send(ADDR, bad).await;
    assert!(
        matches!(refusal, PublishResult::MissingSender),
        "{refusal:?}"
    );
    f.assert_spent_nothing().await;
}

#[tokio::test]
async fn the_committed_row_carries_the_status_to() {
    let f = fixture().await;
    let committed = f
        .send(ADDR, MqttPayload::Inline(b"on".to_vec()), &naming(STATUS))
        .await
        .expect("committed");
    let rows = f.rows(&f.outbound).await;
    let [row] = rows.as_slice() else {
        panic!("one row, got {}", rows.len());
    };
    assert_eq!(row.message_id, committed.message_id);
    assert_eq!(mqtt_transport(row).status_to.as_deref(), Some(STATUS));
    assert!(
        f.rows(&f.status).await.is_empty(),
        "admission publishes no status"
    );
}

// ── A closed egress channel ─────────────────────────────────────────────

fn egress_reader() -> ParticipantId {
    crate::mqtt_outbound::mqtt_outbound_participant()
}

impl Fixture {
    /// Close `home`'s egress channel as a reload's removal does, leaving the
    /// client registered and the channel in the directory.
    async fn close_home(&self) {
        let taken = self
            .m
            .detach_subscriber_taking_unseen(
                EGRESS_HOME,
                &egress_reader(),
                self.outbound.resolved_channel.retain_depth,
                crate::mqtt_outbound::MQTT_OUTBOUND_NOISE,
            )
            .await;
        assert!(taken.is_some(), "the fixture's reader held a position");
    }

    fn full_budget(&self) -> u32 {
        self.m.apps.load().get(APP).unwrap().messaging_send_budget()
    }
}

/// A publish admitted and committed after the channel was closed, the client
/// still registered and the channel still in the directory, is answered as for
/// an unknown client: nothing is stored and the drawn budget is refunded. The
/// send-rate token it drew shows it passed admission.
#[tokio::test]
async fn a_commit_to_a_closed_egress_channel_is_unknown_and_refunded() {
    let f = fixture().await;
    f.close_home().await;
    let refusal = expect_refused(f.send_inline(b"on").await);
    assert!(
        matches!(&refusal, PublishResult::UnknownChannel(a) if a == ADDR),
        "{refusal:?}"
    );
    assert!(f.rows(&f.outbound).await.is_empty(), "nothing stored");
    assert!(f.rate_drawn(), "the publish passed admission");
    assert_eq!(f.budget().await, Some(f.full_budget()), "budget refunded");

    // The other client's channel is still in service.
    f.send(OTHER_ADDR, MqttPayload::Inline(b"on".to_vec()), &options())
        .await
        .expect("committed");
    assert_eq!(f.rows(&f.other).await.len(), 1);
}

/// A publish that passed admission while the channel was in service, and whose
/// commit comes after the close and after the channel left the directory, is
/// refused at the commit. The publish is held at its budget draw by the
/// database lock — admission takes no lock before it — and the close and the
/// directory removal happen through that held lock.
#[tokio::test]
async fn a_commit_admitted_before_the_close_and_landing_after_it_is_refused() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let f = fixture().await;
    let conn = f.m.db.lock().await;
    let mut publish = Box::pin(f.send_inline(b"on"));
    assert!(
        matches!(
            publish
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ),
        "the publish waits on the database lock"
    );
    assert!(
        f.rate_drawn(),
        "the publish passed admission before the close"
    );
    crate::db::delete_subscriber_cursor(&conn, f.outbound.uuid, &egress_reader());
    assert!(f.m.directory.remove_channel(&f.outbound.uuid));
    drop(conn);

    let refusal = expect_refused(publish.await);
    assert!(
        matches!(&refusal, PublishResult::UnknownChannel(a) if a == ADDR),
        "{refusal:?}"
    );
    assert!(f.rows(&f.outbound).await.is_empty(), "nothing stored");
    assert_eq!(f.budget().await, Some(f.full_budget()), "budget refunded");
}

/// Polls `fut` once with a waker that does nothing and asserts it is pending.
/// A future pending on the database lock holds its place in the lock's queue,
/// and a later `.await` finds the lock handed to it.
fn poll_pending<F: std::future::Future + ?Sized>(fut: std::pin::Pin<&mut F>, what: &str) {
    use std::task::{Context, Poll, Waker};
    assert!(
        matches!(
            fut.poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ),
        "{what}"
    );
}

/// A publish whose commit is queued on the database lock ahead of the close's
/// take is committed and is in what the take returns. The database lock hands
/// itself over in queue order, so the order is fixed: the publish is parked at
/// its commit by a holder queued behind its budget draw, and the take queues
/// after it.
#[tokio::test]
async fn a_commit_queued_before_the_take_is_in_what_the_take_returns() {
    let f = fixture().await;
    let held = f.m.db.lock().await;
    let mut publish = Box::pin(f.send_inline(b"on"));
    poll_pending(publish.as_mut(), "the publish waits at its budget draw");
    let mut holder = Box::pin(f.m.db.lock());
    poll_pending(holder.as_mut(), "the holder queues behind the publish");
    drop(held);
    poll_pending(
        publish.as_mut(),
        "the publish draws its budget and waits at its commit",
    );
    let held = holder.await;
    let reader = egress_reader();
    let mut take = Box::pin(f.m.detach_subscriber_taking_unseen(
        EGRESS_HOME,
        &reader,
        f.outbound.resolved_channel.retain_depth,
        crate::mqtt_outbound::MQTT_OUTBOUND_NOISE,
    ));
    poll_pending(take.as_mut(), "the take queues behind the commit");
    drop(held);

    let (committed, unseen) = tokio::join!(publish, take);
    let committed = committed.expect("committed ahead of the take");
    let unseen = unseen.expect("the fixture's reader held a position");
    assert_eq!(
        unseen
            .iter()
            .map(|(_, envelope)| envelope.message_id)
            .collect::<Vec<_>>(),
        vec![committed.message_id],
        "the take returns the commit for its report"
    );
}

/// A publish whose commit is queued on the database lock behind the close's
/// take is refused: the take reads what is unread and gives up the position in
/// one hold, so no commit lands between the two. The take queues behind the
/// publish's budget draw and ahead of its commit.
#[tokio::test]
async fn a_commit_queued_behind_the_take_is_refused_and_not_in_what_it_returns() {
    let f = fixture().await;
    let held = f.m.db.lock().await;
    let mut publish = Box::pin(f.send_inline(b"on"));
    poll_pending(publish.as_mut(), "the publish waits at its budget draw");
    assert!(
        f.rate_drawn(),
        "the publish passed admission before the close"
    );
    let reader = egress_reader();
    let mut take = Box::pin(f.m.detach_subscriber_taking_unseen(
        EGRESS_HOME,
        &reader,
        f.outbound.resolved_channel.retain_depth,
        crate::mqtt_outbound::MQTT_OUTBOUND_NOISE,
    ));
    poll_pending(take.as_mut(), "the take queues behind the budget draw");
    drop(held);
    poll_pending(
        publish.as_mut(),
        "the publish draws its budget and waits at its commit, behind the take",
    );

    let (unseen, published) = tokio::join!(take, publish);
    let unseen = unseen.expect("the fixture's reader held a position");
    assert!(unseen.is_empty(), "nothing was committed before the take");
    let refusal = expect_refused(published);
    assert!(
        matches!(&refusal, PublishResult::UnknownChannel(a) if a == ADDR),
        "{refusal:?}"
    );
    assert!(f.rows(&f.outbound).await.is_empty(), "nothing stored");
    assert_eq!(f.budget().await, Some(f.full_budget()), "budget refunded");
}

/// A payload over the body cap refused at the commit leaves no blob behind: the
/// refused append dropped the only hold on it.
#[tokio::test]
async fn an_over_cap_payload_refused_at_the_commit_leaves_no_blob() {
    let f = fixture().await;
    f.close_home().await;
    let over = vec![b'x'; f.m.max_body_bytes() + 1];
    let refusal = expect_refused(f.send_inline(&over).await);
    assert!(
        matches!(refusal, PublishResult::UnknownChannel(_)),
        "{refusal:?}"
    );
    assert_eq!(f.blobs(), (0, 0), "no blob left");
    assert!(f.rows(&f.outbound).await.is_empty(), "nothing stored");
    assert_eq!(f.budget().await, Some(f.full_budget()), "budget refunded");
}

#[tokio::test]
#[should_panic(expected = "whose egress reader holds no position on it")]
async fn a_flushed_mqtt_entry_to_a_closed_egress_channel_panics() {
    let f = fixture().await;
    f.close_home().await;
    let none = HeldAttachments::none();
    f.m.publish_from_wasm(
        COMPONENT,
        &[wasm_mqtt(
            EGRESS_HOME,
            CLIENT,
            "lights/kitchen/set",
            b"on",
            options(),
            &none,
        )],
    )
    .await;
}

/// The position is checked for each egress channel a flush writes to, not only
/// the first: an entry to a closed channel after one to a channel in service
/// still panics.
#[tokio::test]
#[should_panic(expected = "whose egress reader holds no position on it")]
async fn a_flushed_mqtt_entry_to_a_closed_channel_after_an_open_one_panics() {
    let f = fixture().await;
    f.close_home().await;
    let none = HeldAttachments::none();
    let egress_away = crate::mqtt_egress_address(OTHER);
    f.m.publish_from_wasm(
        COMPONENT,
        &[
            wasm_mqtt(&egress_away, OTHER, "lights/b/set", b"1", options(), &none),
            wasm_mqtt(EGRESS_HOME, CLIENT, "lights/a/set", b"2", options(), &none),
        ],
    )
    .await;
}

/// The take returns what the position had not been served, enacts the outrun
/// its window passed under the rung it is given, and forgets the tally with the
/// position.
#[tokio::test]
async fn detach_taking_unseen_enacts_the_outrun_and_forgets_the_tally() {
    let f = fixture().await;
    let probe = ParticipantId::for_system("probe");
    f.m.attach_subscriber(STATUS, "probe", &probe, Depth::Bounded(8))
        .await;
    for body in ["a", "b", "c"] {
        crate::testutils::insert_bus_message(&f.m, &f.status, body, crate::ChannelScheme::Brenn)
            .await;
    }
    let taken =
        f.m.detach_subscriber_taking_unseen(STATUS, &probe, Depth::Bounded(1), NoiseLevel::Alarm)
            .await
            .expect("the probe held a position");
    let bodies: Vec<_> = taken
        .iter()
        .map(|(_, envelope)| envelope.body.as_text().unwrap().to_owned())
        .collect();
    assert_eq!(bodies, vec!["c"]);
    assert_eq!(
        f.router.alarms.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a and b were outrun, under the alarm rung"
    );
    assert_eq!(f.m.drop_counter(STATUS, &probe), 0, "the tally went too");
    assert!(
        f.m.detach_subscriber_taking_unseen(STATUS, &probe, Depth::Bounded(1), NoiseLevel::Alarm)
            .await
            .is_none()
    );
}

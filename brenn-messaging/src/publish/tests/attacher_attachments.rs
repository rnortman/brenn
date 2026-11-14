//! A remote attacher's publishes with attachments: the single publish on
//! both stores, and the batch's sync before the lock, adoption with the row,
//! confirmation after the commit, the ring's holds, the fed envelopes, and
//! what an entry that publishes nothing leaves.

use super::super::*;
use super::CountingRouter;
use crate::config::{AttachSendBudget, Depth, MessagingGlobalConfig, NoiseLevel, SendRate};
use crate::db::{init_db_memory, upsert_channels};
use crate::store::RingStores;
use crate::test_support::test_channel_entry;
use crate::testutils::{
    TempBlobDir, ephemeral_channel_entry, install_blob_store_at, redeem_all, remote_registrations,
    surface_registrations,
};
use crate::{
    AttachScope, AttachmentName, ChannelEntry, MessagingDirectory, SubscriberEntry,
    SubscriberEntryKind, Urgency, WakeRouter,
};
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_messaging_store::blob::{BlobError, BlobHold, ReapReport};
use chrono::{DateTime, Duration, Utc};
use indexmap::IndexMap;
use std::sync::Arc;

const REMOTE: &str = "blob-daemon";
const PAGE: &str = "blob-watcher";

struct Fixture {
    m: Arc<Messenger>,
    router: Arc<CountingRouter>,
    blobs: TempBlobDir,
    /// Durable, unbounded, fed to [`PAGE`].
    durable: String,
    /// Durable with a retain depth of 1, so its deferred cap is one park.
    capped: String,
    /// A ring of depth 1, fed to [`PAGE`].
    ring: String,
}

fn page_subscriber() -> SubscriberEntry {
    SubscriberEntry {
        kind: SubscriberEntryKind::Surface(PAGE.to_string()),
        push_depth: Depth::Unbounded,
        retain_depth: Depth::Unbounded,
        noise: NoiseLevel::Silent,
        wake_min: None,
    }
}

/// A send rate the tests never meet.
fn unmetered(entry: &mut ChannelEntry) {
    entry.resolved_channel.send_rate = SendRate {
        burst: u32::MAX,
        refill_interval_secs: 1,
        refill: u32::MAX,
    };
}

/// Publishes onto both schemes everywhere.
fn remote_policy() -> AppPolicy {
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::MessagingPublish);
    policy.grants.insert(AppCapability::EphemeralPublish);
    policy
        .acls
        .brenn_publish
        .push(ChannelMatcher::Prefix(String::new()));
    policy
        .acls
        .ephemeral_publish
        .push(ChannelMatcher::Prefix(String::new()));
    policy
}

/// Reads both schemes everywhere.
fn page_policy() -> AppPolicy {
    let mut policy = brenn_lib::messaging::test_support::brenn_delivery_policy(
        ChannelMatcher::Prefix(String::new()),
    );
    policy.grants.insert(AppCapability::EphemeralSubscribe);
    policy
        .acls
        .ephemeral_subscribe
        .push(ChannelMatcher::Prefix(String::new()));
    policy
}

async fn fixture() -> Fixture {
    let mut durable = test_channel_entry("remote-blob-durable", vec![page_subscriber()]);
    unmetered(&mut durable);
    let mut capped = test_channel_entry("remote-blob-capped", vec![]);
    capped.resolved_channel.retain_depth = Depth::Bounded(1);
    capped.resolved_channel.standing_retain_depth = Depth::Bounded(1);
    unmetered(&mut capped);
    let mut ring = ephemeral_channel_entry("remote-blob-ring", 1);
    ring.subscribers = vec![page_subscriber()];
    unmetered(&mut ring);
    let (durable_addr, capped_addr, ring_addr) = (
        durable.address.clone(),
        capped.address.clone(),
        ring.address.clone(),
    );

    let db = init_db_memory();
    upsert_channels(&*db.lock().await, &[durable.clone(), capped.clone()]);
    let mut registrations = surface_registrations([(PAGE.to_string(), page_policy())].into());
    registrations.extend(remote_registrations(
        [(REMOTE.to_string(), remote_policy())].into(),
    ));
    let stores = Arc::new(RingStores::build(std::slice::from_ref(&ring)));
    let router = Arc::new(CountingRouter::default());
    let messenger = Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(vec![
            durable, capped, ring,
        ])),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::clone(&router) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(registrations)
    .with_ring_stores(stores)
    .with_attach_send_budgets(crate::attach_principal_budgets(
        AttachScope::remote(REMOTE),
        vec![(None, AttachSendBudget::default())],
    ));
    let blobs = TempBlobDir::new();
    let m = install_blob_store_at(messenger, blobs.path()).await;
    Fixture {
        m,
        router,
        blobs,
        durable: durable_addr,
        capped: capped_addr,
        ring: ring_addr,
    }
}

fn name(s: &str) -> AttachmentName {
    AttachmentName::new(s).unwrap()
}

fn held(entries: &[(&str, &BlobHold)]) -> HeldAttachments {
    brenn_messaging_store::testutils::held(entries)
}

fn entry<'a>(
    addr: &'a str,
    body: &str,
    ts: i64,
    deliver_after: Option<DateTime<Utc>>,
    attachments: &'a HeldAttachments,
) -> AttachBatchPublish<'a> {
    AttachBatchPublish {
        channel_address: addr,
        body: body.into(),
        urgency: Urgency::Normal,
        publish_ts_ns: 1_700_000_000_000_000_000 + ts,
        deliver_after,
        attachments,
    }
}

impl Fixture {
    fn seal(&self, bytes: &[u8]) -> BlobHold {
        crate::testutils::seal_blob(&self.m, bytes)
    }

    async fn publish(&self, addr: &str, attachments: HeldAttachments) -> PublishResult {
        self.m
            .publish_from_attacher(
                AttachScope::remote(REMOTE),
                None,
                addr,
                "single".into(),
                Urgency::Normal,
                attachments,
            )
            .await
    }

    async fn batch(&self, entries: &[AttachBatchPublish<'_>]) -> usize {
        self.m
            .publish_batch_from_attacher(
                AttachScope::remote(REMOTE),
                None,
                entries,
                MissingChannelPosture::Race,
            )
            .await
    }

    async fn newest(&self, addr: &str) -> Arc<crate::MessageEnvelope> {
        let entry = self.m.directory().resolve(addr).unwrap();
        self.m
            .store_for(&entry)
            .retained_tail(Depth::Bounded(1))
            .await
            .pop()
            .expect("the channel retains a message")
    }

    /// `(body, parked)` of every durable row, in publish order.
    async fn rows(&self) -> Vec<(String, bool)> {
        let conn = self.m.db().lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT body, deliver_after IS NOT NULL FROM messaging_messages \
                 ORDER BY publish_ts_ns",
            )
            .unwrap();
        stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, bool>(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    async fn delete_rows(&self) {
        let conn = self.m.db().lock().await;
        conn.execute("DELETE FROM messaging_messages", []).unwrap();
    }

    /// Publish one plain message onto the ring of depth 1, evicting what it
    /// held.
    async fn evict_ring(&self) {
        let none = HeldAttachments::none();
        assert_eq!(
            self.batch(&[entry(&self.ring, "evicts", 99, None, &none)])
                .await,
            0
        );
    }
}

/// A single publish names its blobs on a durable channel, where the row
/// alone keeps the blob once the caller lets go, and on a ring, where the
/// ring's holds keep it until eviction.
#[tokio::test]
async fn a_single_publish_stores_its_attachments_on_both_stores() {
    let f = fixture().await;
    for addr in [&f.durable, &f.ring] {
        let photo = f.seal(b"photo bytes");
        let id = photo.id().clone();
        let result = f.publish(addr, held(&[("photo", &photo)])).await;
        assert!(
            matches!(result, PublishResult::Ok { .. }),
            "{addr}: {result:?}"
        );
        drop(photo);

        let stored = f.newest(addr).await;
        assert_eq!(stored.attachments.get(&name("photo")).unwrap().id, id);
        assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"photo bytes");
        assert_eq!(f.m.reap_blobs().await, ReapReport::default(), "{addr}");
    }
    let fed = f.router.fed.lock().await;
    assert_eq!(fed.len(), 2);
    assert!(fed.iter().all(|e| e.attachments.len() == 1));
}

/// A durable batch entry's blob is adopted by its row and confirmed: once the
/// caller's holds drop the row alone keeps it, and it goes at the reap after
/// the row does.
#[tokio::test]
async fn a_durable_batch_entry_adopts_its_blobs() {
    let f = fixture().await;
    let photo = f.seal(b"photo bytes");
    let id = photo.id().clone();
    let attachments = held(&[("photo", &photo), ("again", &photo)]);
    assert_eq!(
        f.batch(&[entry(&f.durable, "named", 0, None, &attachments)])
            .await,
        0
    );
    drop((photo, attachments));

    let stored = f.newest(&f.durable).await;
    assert_eq!(stored.attachments.len(), 2);
    assert_eq!(stored.attachments.get(&name("photo")).unwrap().id, id);
    assert_eq!(stored.attachments.get(&name("again")).unwrap().id, id);
    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"photo bytes");
    assert_eq!(f.m.reap_blobs().await, ReapReport::default());

    f.delete_rows().await;
    assert_eq!(f.m.reap_blobs().await.blobs, 1);
    assert!(!f.blobs.holds_file(&id));
}

/// One blob on a ring entry and a durable entry of one batch: both messages
/// hold it, the ring's copy goes with its eviction and the row keeps it.
#[tokio::test]
async fn a_mixed_batch_holds_one_blob_from_both_stores() {
    let f = fixture().await;
    let shared = f.seal(b"shared");
    let id = shared.id().clone();
    let attachments = held(&[("shared", &shared)]);
    let none = HeldAttachments::none();
    assert_eq!(
        f.batch(&[
            entry(&f.ring, "on the ring", 0, None, &attachments),
            entry(&f.durable, "plain", 1, None, &none),
            entry(&f.durable, "on the row", 2, None, &attachments),
        ])
        .await,
        0
    );
    drop((shared, attachments));

    assert_eq!(
        f.rows().await,
        vec![
            ("plain".to_string(), false),
            ("on the row".to_string(), false)
        ]
    );
    let ring = f.newest(&f.ring).await;
    assert_eq!(ring.attachments.get(&name("shared")).unwrap().id, id);
    f.evict_ring().await;
    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"shared");

    f.delete_rows().await;
    assert_eq!(f.m.reap_blobs().await.blobs, 1);
    assert!(!f.blobs.holds_file(&id));
}

/// A ring-only entry's blob is neither synced nor adopted: the eviction of
/// the last message naming it deletes it, with no reap.
#[tokio::test]
async fn a_ring_batch_entry_holds_its_blob_until_eviction() {
    let f = fixture().await;
    let frame = f.seal(b"frame");
    let id = frame.id().clone();
    let attachments = held(&[("frame", &frame)]);
    assert_eq!(
        f.batch(&[entry(&f.ring, "one frame", 0, None, &attachments)])
            .await,
        0
    );
    drop((frame, attachments));
    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"frame");

    f.evict_ring().await;
    assert!(matches!(
        redeem_all(&f.m, &id).await,
        Err(BlobError::NotFound)
    ));
    assert!(!f.blobs.holds_file(&id));
}

/// Parked entries hold their blobs on both stores until release, and the
/// released messages carry the references.
#[tokio::test]
async fn a_parked_batch_entry_holds_its_blob_through_release() {
    let f = fixture().await;
    let release_at = Utc::now() + Duration::hours(1);
    let (dur, eph) = (f.seal(b"durable later"), f.seal(b"ring later"));
    let ids = (dur.id().clone(), eph.id().clone());
    let (a, b) = (held(&[("later", &dur)]), held(&[("later", &eph)]));
    assert_eq!(
        f.batch(&[
            entry(&f.durable, "parked row", 0, Some(release_at), &a),
            entry(&f.ring, "parked ring", 1, Some(release_at), &b),
        ])
        .await,
        0
    );
    drop((dur, eph, a, b));
    assert_eq!(f.rows().await, vec![("parked row".to_string(), true)]);
    assert_eq!(redeem_all(&f.m, &ids.0).await.unwrap(), b"durable later");
    assert_eq!(redeem_all(&f.m, &ids.1).await.unwrap(), b"ring later");
    assert_eq!(f.m.reap_blobs().await, ReapReport::default());
    assert!(
        f.router.fed.lock().await.is_empty(),
        "a parked message is not fed"
    );

    f.m.release_due_messages(release_at + Duration::seconds(1))
        .await;
    for (addr, id) in [(&f.durable, &ids.0), (&f.ring, &ids.1)] {
        let stored = f.newest(addr).await;
        assert_eq!(&stored.attachments.get(&name("later")).unwrap().id, id);
    }
}

/// An entry that publishes nothing — a park the deferred cap refuses, a
/// target gone from the directory — adopts nothing, and its blob goes as soon
/// as the caller lets go, while the entries around it are stored.
#[tokio::test]
async fn an_entry_that_publishes_nothing_adopts_nothing() {
    let f = fixture().await;
    let release_at = Utc::now() + Duration::hours(1);
    let (parked, refused, vanished, immediate) = (
        f.seal(b"parked"),
        f.seal(b"refused"),
        f.seal(b"vanished"),
        f.seal(b"now"),
    );
    let ids = [
        parked.id().clone(),
        refused.id().clone(),
        vanished.id().clone(),
        immediate.id().clone(),
    ];
    let (a, b, c, d) = (
        held(&[("p", &parked)]),
        held(&[("r", &refused)]),
        held(&[("v", &vanished)]),
        held(&[("i", &immediate)]),
    );
    let dropped = f
        .batch(&[
            entry(&f.capped, "first", 0, Some(release_at), &a),
            entry(&f.capped, "over the cap", 1, Some(release_at), &b),
            entry("brenn:remote-blob-gone", "nowhere", 2, None, &c),
            entry(&f.capped, "immediate", 3, None, &d),
        ])
        .await;
    assert_eq!(dropped, 2, "the refused park and the vanished target");
    drop((parked, refused, vanished, immediate, a, b, c, d));

    assert_eq!(
        f.rows().await,
        vec![
            ("first".to_string(), true),
            ("immediate".to_string(), false)
        ]
    );
    for id in [&ids[1], &ids[2]] {
        assert!(!f.blobs.holds_file(id), "nothing adopted {id:?}");
    }
    assert_eq!(redeem_all(&f.m, &ids[0]).await.unwrap(), b"parked");
    assert_eq!(redeem_all(&f.m, &ids[3]).await.unwrap(), b"now");
    assert_eq!(f.m.reap_blobs().await, ReapReport::default());
}

/// The envelope fed live carries the entry's references, on the durable arm
/// and on the ring arm.
#[tokio::test]
async fn fed_batch_envelopes_carry_the_attachments_on_both_stores() {
    let f = fixture().await;
    let (dur, eph) = (f.seal(b"durable"), f.seal(b"ring"));
    let (a, b) = (held(&[("d", &dur)]), held(&[("e", &eph)]));
    assert_eq!(
        f.batch(&[
            entry(&f.durable, "to the row", 0, None, &a),
            entry(&f.ring, "to the ring", 1, None, &b),
        ])
        .await,
        0
    );

    let fed = f.router.fed.lock().await;
    assert_eq!(fed.len(), 2);
    assert_eq!(fed[0].body.as_text(), Some("to the row"));
    assert_eq!(fed[0].attachments, *a.refs());
    assert_eq!(fed[1].body.as_text(), Some("to the ring"));
    assert_eq!(fed[1].attachments, *b.refs());
}

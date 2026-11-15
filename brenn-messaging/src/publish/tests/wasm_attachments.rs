//! `Messenger::publish_from_wasm` with attachments: the sync before the lock,
//! adoption with the row, confirmation after the commit, the ring's holds, the
//! fed envelopes, and what a refused park and a rolled-back batch leave.

use super::super::*;
use super::CountingRouter;
use crate::config::{Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink};
use crate::db::{init_db_memory, upsert_channels};
use crate::store::RingStores;
use crate::testutils::{
    TempBlobDir, ephemeral_channel_entry, install_blob_store_at, local_channel_entry, redeem_all,
};
use crate::{
    AttachmentName, ChannelEntry, ChannelScheme, MessagingDirectory, SubscriberEntry,
    SubscriberEntryKind, Urgency, WakeMin, WakeRouter, canonical_address,
};
use brenn_lib::access::acl::ChannelMatcher;
use brenn_messaging_store::blob::{BlobError, BlobHold, ReapReport};
use chrono::{DateTime, Duration, Utc};
use indexmap::IndexMap;
use std::sync::Arc;
use uuid::Uuid;

const SLUG: &str = "wasm-blob-producer";
const PAGE: &str = "blob-page";

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
    /// A `local:` ring of depth 1.
    local: String,
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

fn durable_entry(name: &str, retain: Depth, subscribers: Vec<SubscriberEntry>) -> ChannelEntry {
    ChannelEntry {
        uuid: Uuid::new_v4(),
        address: canonical_address(name),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: retain,
            standing_retain_depth: retain,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
            non_text: false,
            untyped_content_type: None,
        },
        subscribers,
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

async fn fixture() -> Fixture {
    let durable = durable_entry(
        "wasm-blob-durable",
        Depth::Unbounded,
        vec![page_subscriber()],
    );
    let capped = durable_entry("wasm-blob-capped", Depth::Bounded(1), vec![]);
    let mut ring = ephemeral_channel_entry("wasm-blob-ring", 1);
    ring.subscribers = vec![page_subscriber()];
    let local = local_channel_entry("wasm-blob-local", 1);
    let (durable_addr, capped_addr, ring_addr, local_addr) = (
        durable.address.clone(),
        capped.address.clone(),
        ring.address.clone(),
        local.address.clone(),
    );

    let db = init_db_memory();
    {
        let conn = db.lock().await;
        upsert_channels(&conn, &[durable.clone(), capped.clone()]);
    }
    let mut page_policy = brenn_lib::messaging::test_support::brenn_delivery_policy(
        ChannelMatcher::Prefix(String::new()),
    );
    page_policy
        .grants
        .insert(brenn_envelope::grants::AppCapability::EphemeralSubscribe);
    page_policy
        .acls
        .ephemeral_subscribe
        .push(ChannelMatcher::Prefix(String::new()));
    let stores = Arc::new(RingStores::build(&[ring.clone(), local.clone()]));
    let router = Arc::new(CountingRouter::default());
    let messenger = Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(vec![
            durable, capped, ring, local,
        ])),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::clone(&router) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(crate::testutils::surface_registrations(
        [(PAGE.to_string(), page_policy)].into(),
    ))
    .with_ring_stores(stores);
    let blobs = TempBlobDir::new();
    let m = install_blob_store_at(messenger, blobs.path()).await;
    Fixture {
        m,
        router,
        blobs,
        durable: durable_addr,
        capped: capped_addr,
        ring: ring_addr,
        local: local_addr,
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
    deliver_after: Option<DateTime<Utc>>,
    attachments: &'a HeldAttachments,
) -> WasmPublish<'a> {
    WasmPublish {
        channel_address: addr,
        body: body.into(),
        urgency: Urgency::Normal,
        reply_to: None,
        deliver_after,
        attachments,
    }
}

impl Fixture {
    fn seal(&self, bytes: &[u8]) -> BlobHold {
        crate::testutils::seal_blob(&self.m, bytes)
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
}

/// A durable entry's blob is adopted by its row and confirmed: once the
/// activation's holds drop, the row alone keeps it, the reap leaves it, and
/// it goes at the reap after the row does.
#[tokio::test]
async fn a_durable_wasm_publish_adopts_its_blobs_and_the_row_holds_them() {
    let f = fixture().await;
    let photo = f.seal(b"photo bytes");
    let id = photo.id().clone();
    let attachments = held(&[("photo", &photo), ("again", &photo)]);
    f.m.publish_from_wasm(SLUG, &[entry(&f.durable, "named", None, &attachments)])
        .await;
    drop((photo, attachments));

    let stored = f.newest(&f.durable).await;
    assert_eq!(stored.attachments.len(), 2);
    assert_eq!(stored.attachments.get(&name("photo")).unwrap().id, id);
    assert_eq!(stored.attachments.get(&name("again")).unwrap().id, id);
    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"photo bytes");
    assert_eq!(f.m.reap_blobs().await, ReapReport::default());

    f.delete_rows().await;
    assert!(matches!(
        redeem_all(&f.m, &id).await,
        Err(BlobError::NotFound)
    ));
    assert_eq!(
        f.m.reap_blobs().await,
        ReapReport {
            blobs: 1,
            bytes: 11
        }
    );
    assert!(!f.blobs.holds_file(&id));
}

/// A ring-only batch neither syncs nor adopts: the blob stays *Volatile*, so
/// the eviction of the last message naming it unlinks it with no reap.
#[tokio::test]
async fn a_ring_wasm_publish_holds_its_blob_in_the_ring_and_eviction_deletes_it() {
    let f = fixture().await;
    let frame = f.seal(b"frame");
    let id = frame.id().clone();
    let attachments = held(&[("frame", &frame)]);
    f.m.publish_from_wasm(SLUG, &[entry(&f.ring, "one frame", None, &attachments)])
        .await;
    drop((frame, attachments));

    let stored = f.newest(&f.ring).await;
    assert_eq!(stored.attachments.get(&name("frame")).unwrap().id, id);
    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"frame");

    let none = HeldAttachments::none();
    f.m.publish_from_wasm(SLUG, &[entry(&f.ring, "evicts", None, &none)])
        .await;
    assert!(matches!(
        redeem_all(&f.m, &id).await,
        Err(BlobError::NotFound)
    ));
    assert!(
        !f.blobs.holds_file(&id),
        "a blob no durable row ever named goes with its last holder"
    );
}

/// A `local:` channel takes attachments as an `ephemeral:` one does: the ring
/// entry holds the blob, a reader redeems it, and eviction deletes it.
#[tokio::test]
async fn a_local_wasm_publish_holds_its_blob_in_the_ring_and_a_reader_opens_it() {
    let f = fixture().await;
    let kept = f.seal(b"kept on the host");
    let id = kept.id().clone();
    let attachments = held(&[("kept", &kept), ("again", &kept)]);
    f.m.publish_from_wasm(SLUG, &[entry(&f.local, "kept", None, &attachments)])
        .await;
    drop((kept, attachments));

    let stored = f.newest(&f.local).await;
    assert_eq!(stored.attachments.len(), 2);
    assert_eq!(stored.attachments.get(&name("kept")).unwrap().id, id);
    assert_eq!(stored.attachments.get(&name("again")).unwrap().id, id);
    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"kept on the host");
    assert!(
        f.rows().await.is_empty(),
        "a `local:` publish writes no row"
    );

    let none = HeldAttachments::none();
    f.m.publish_from_wasm(SLUG, &[entry(&f.local, "evicts", None, &none)])
        .await;
    assert!(matches!(
        redeem_all(&f.m, &id).await,
        Err(BlobError::NotFound)
    ));
    assert!(!f.blobs.holds_file(&id));
}

/// One blob on a ring entry and a durable entry of the same batch: both
/// messages hold it, and it outlives the ring message's eviction by the row.
#[tokio::test]
async fn a_mixed_batch_holds_one_blob_from_both_stores() {
    let f = fixture().await;
    let shared = f.seal(b"shared");
    let id = shared.id().clone();
    let attachments = held(&[("shared", &shared)]);
    let none = HeldAttachments::none();
    f.m.publish_from_wasm(
        SLUG,
        &[
            entry(&f.ring, "on the ring", None, &attachments),
            entry(&f.durable, "plain", None, &none),
            entry(&f.durable, "on the row", None, &attachments),
        ],
    )
    .await;
    drop((shared, attachments));

    assert_eq!(
        f.rows().await,
        vec![
            ("plain".to_string(), false),
            ("on the row".to_string(), false)
        ]
    );
    assert_eq!(
        f.newest(&f.ring)
            .await
            .attachments
            .get(&name("shared"))
            .unwrap()
            .id,
        id
    );
    f.m.publish_from_wasm(SLUG, &[entry(&f.ring, "evicts", None, &none)])
        .await;
    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"shared");

    f.delete_rows().await;
    assert_eq!(f.m.reap_blobs().await.blobs, 1);
    assert!(!f.blobs.holds_file(&id));
}

/// A park the deferred cap refuses adopts nothing and is not confirmed, while
/// the parked and immediate entries around it are: the refused entry's blob
/// goes as soon as the activation lets go, the others stay held by their rows.
#[tokio::test]
async fn a_park_refused_at_the_cap_adopts_nothing_beside_inserted_entries() {
    let f = fixture().await;
    let release_at = Utc::now() + Duration::hours(1);
    let (parked, refused, immediate) = (f.seal(b"parked"), f.seal(b"refused"), f.seal(b"now"));
    let ids = (
        parked.id().clone(),
        refused.id().clone(),
        immediate.id().clone(),
    );
    let (a, b, c) = (
        held(&[("p", &parked)]),
        held(&[("r", &refused)]),
        held(&[("i", &immediate)]),
    );
    f.m.publish_from_wasm(
        SLUG,
        &[
            entry(&f.capped, "first", Some(release_at), &a),
            entry(&f.capped, "over the cap", Some(release_at), &b),
            entry(&f.capped, "immediate", None, &c),
        ],
    )
    .await;
    drop((parked, refused, immediate, a, b, c));

    assert_eq!(
        f.rows().await,
        vec![
            ("first".to_string(), true),
            ("immediate".to_string(), false)
        ]
    );
    assert_eq!(f.m.dropped_deferred_count(SLUG, &f.capped), 1);
    assert!(
        !f.blobs.holds_file(&ids.1),
        "the refused entry's blob was synced but never adopted, so it goes with its holds"
    );
    assert_eq!(redeem_all(&f.m, &ids.0).await.unwrap(), b"parked");
    assert_eq!(redeem_all(&f.m, &ids.2).await.unwrap(), b"now");
    assert_eq!(f.m.reap_blobs().await, ReapReport::default());
}

/// A parked durable entry's blob is held while parked and after release, and
/// the released message carries its reference.
#[tokio::test]
async fn a_parked_durable_wasm_publish_holds_its_blob_through_release() {
    let f = fixture().await;
    let release_at = Utc::now() + Duration::hours(1);
    let later = f.seal(b"later");
    let id = later.id().clone();
    let attachments = held(&[("later", &later)]);
    f.m.publish_from_wasm(
        SLUG,
        &[entry(&f.durable, "parked", Some(release_at), &attachments)],
    )
    .await;
    drop((later, attachments));
    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"later");
    assert!(
        f.router.fed.lock().await.is_empty(),
        "a parked message is not fed"
    );

    f.m.release_due_messages(release_at + Duration::seconds(1))
        .await;
    let stored = f.newest(&f.durable).await;
    assert_eq!(stored.body.as_text(), Some("parked"));
    assert_eq!(stored.attachments.get(&name("later")).unwrap().id, id);
    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"later");
}

/// The envelope fed live to a subscribing page carries the entry's
/// references, on the durable arm and on the ring arm.
#[tokio::test]
async fn fed_envelopes_carry_the_attachments_on_both_stores() {
    let f = fixture().await;
    let (dur, eph) = (f.seal(b"durable"), f.seal(b"ring"));
    let (a, b) = (held(&[("d", &dur)]), held(&[("e", &eph)]));
    f.m.publish_from_wasm(
        SLUG,
        &[
            entry(&f.durable, "to the row", None, &a),
            entry(&f.ring, "to the ring", None, &b),
        ],
    )
    .await;

    let fed = f.router.fed.lock().await;
    assert_eq!(fed.len(), 2);
    assert_eq!(fed[0].body.as_text(), Some("to the row"));
    assert_eq!(fed[0].attachments, *a.refs());
    assert_eq!(fed[1].body.as_text(), Some("to the ring"));
    assert_eq!(fed[1].attachments, *b.refs());
}

/// A batch that panics after adopting a blob rolls back: no row lands, the
/// blob is unredeemable at once, and the next reap deletes it.
#[tokio::test]
async fn a_rolled_back_batch_leaves_its_adopted_blobs_to_the_reap() {
    let f = fixture().await;
    let doomed = f.seal(b"doomed");
    let id = doomed.id().clone();
    let m = Arc::clone(&f.m);
    let durable = f.durable.clone();
    let attachments = held(&[("doomed", &doomed)]);
    drop(doomed);
    let flush = tokio::spawn(async move {
        let none = HeldAttachments::none();
        m.publish_from_wasm(
            SLUG,
            &[
                entry(&durable, "adopted then rolled back", None, &attachments),
                entry("brenn:not-a-channel", "panics", None, &none),
            ],
        )
        .await;
    });
    let err = flush
        .await
        .expect_err("the flush panics on the unknown channel");
    assert!(err.is_panic());

    assert!(f.rows().await.is_empty(), "the batch landed nothing");
    assert!(matches!(
        redeem_all(&f.m, &id).await,
        Err(BlobError::NotFound)
    ));
    assert_eq!(f.m.reap_blobs().await.blobs, 1);
    assert!(!f.blobs.holds_file(&id));
}

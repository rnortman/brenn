//! `Messenger::publish_from_system_with_attachments` onto both stores, and the
//! `Messenger`'s blob redemption and reap.

use super::super::*;
use super::CountingRouter;
use crate::config::{Depth, MessagingGlobalConfig, NoiseLevel, ResolvedChannel, Sink};
use crate::db::init_db_memory;
use crate::store::DeferralOutcome;
use crate::testutils::{
    TempBlobDir, blob_messenger, ephemeral_channel_entry, publish_attached, redeem_all, seal_blob,
};
use crate::{
    AttachmentName, BlobId, ChannelEntry, ChannelScheme, MessagingDirectory, ParticipantId,
    WakeMin, WakeRouter, canonical_address,
};
use brenn_messaging_store::blob::{BlobError, BlobHold, ReapReport};
use chrono::{Duration, Utc};
use indexmap::IndexMap;
use std::sync::Arc;
use uuid::Uuid;

const PUBLISHER: &str = "blob-producer";
const DURABLE: &str = "blob-durable";
const RING: &str = "blob-ring";

struct Fixture {
    m: Arc<Messenger>,
    blobs: TempBlobDir,
    durable: String,
    ring: String,
}

fn durable_entry() -> ChannelEntry {
    ChannelEntry {
        uuid: Uuid::new_v4(),
        address: canonical_address(DURABLE),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Unbounded,
            retain_depth: Depth::Unbounded,
            standing_retain_depth: Depth::Unbounded,
            noise: NoiseLevel::Silent,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
            non_text: false,
            untyped_content_type: None,
        },
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

/// A messenger with one durable channel, one ring channel of depth 1, a blob
/// store, and a system publisher allowed onto both.
async fn fixture() -> Fixture {
    let durable = durable_entry();
    let ring = ephemeral_channel_entry(RING, 1);
    let addresses = (durable.address.clone(), ring.address.clone());
    let blobs = TempBlobDir::new();
    let m = blob_messenger(
        init_db_memory(),
        vec![durable, ring],
        PUBLISHER,
        blobs.path(),
    )
    .await;
    Fixture {
        m,
        blobs,
        durable: addresses.0,
        ring: addresses.1,
    }
}

fn name(s: &str) -> AttachmentName {
    AttachmentName::new(s).unwrap()
}

impl Fixture {
    async fn publish(
        &self,
        addr: &str,
        body: &str,
        deliver_after: Option<chrono::DateTime<Utc>>,
        holds: &[(&str, &BlobHold)],
    ) -> Uuid {
        publish_attached(&self.m, PUBLISHER, addr, body, holds, deliver_after).await
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

    async fn delete_row(&self, uuid: Uuid) {
        let conn = self.m.db().lock().await;
        let deleted = conn
            .execute(
                "DELETE FROM messaging_messages WHERE uuid = ?1",
                [uuid.as_bytes().to_vec()],
            )
            .unwrap();
        assert_eq!(deleted, 1);
    }
}

// ── Into both stores ─────────────────────────────────────────────────────

/// A durable publish stores both references on the row, the row holds the
/// blobs once the producer lets go, and redemption settles that against the
/// database.
#[tokio::test]
async fn a_durable_publish_stores_its_attachments_and_its_row_holds_the_blobs() {
    let f = fixture().await;
    let photo = seal_blob(&f.m, b"photo bytes");
    let thumb = seal_blob(&f.m, b"thumb");
    let (photo_id, thumb_id) = (photo.id().clone(), thumb.id().clone());
    f.publish(
        &f.durable,
        "two attached",
        None,
        &[("photo", &photo), ("thumb", &thumb)],
    )
    .await;
    drop((photo, thumb));

    let stored = f.newest(&f.durable).await;
    assert_eq!(stored.attachments.len(), 2);
    assert_eq!(stored.attachments.get(&name("photo")).unwrap().id, photo_id);
    assert_eq!(stored.attachments.get(&name("thumb")).unwrap().size, 5);
    assert_eq!(redeem_all(&f.m, &photo_id).await.unwrap(), b"photo bytes");
    assert_eq!(redeem_all(&f.m, &thumb_id).await.unwrap(), b"thumb");
    assert_eq!(f.m.reap_blobs().await, ReapReport::default());
}

/// A ring publish holds its blob in the ring entry, and redeeming a blob only
/// ring messages name never takes the database lock: the test holds it for
/// the whole call.
#[tokio::test]
async fn a_ring_publish_holds_its_blob_and_redeems_without_the_database() {
    let f = fixture().await;
    let photo = seal_blob(&f.m, b"frame");
    let id = photo.id().clone();
    f.publish(&f.ring, "one frame", None, &[("photo", &photo)])
        .await;
    drop(photo);

    let stored = f.newest(&f.ring).await;
    assert_eq!(stored.attachments.get(&name("photo")).unwrap().id, id);
    {
        let _db = f.m.db().lock().await;
        let hold = tokio::time::timeout(std::time::Duration::from_secs(5), f.m.retain_blob(&id))
            .await
            .expect("a ring-held blob is redeemed without the database lock")
            .expect("the ring entry holds it");
        assert_eq!(hold.read(0, 5).unwrap(), b"frame");
    }

    f.publish(&f.ring, "evicts", None, &[]).await;
    assert!(matches!(
        f.m.retain_blob(&id).await,
        Err(BlobError::NotFound)
    ));
    assert!(!f.blobs.holds_file(&id));
}

/// A parked publish holds its blob on both stores, and the release delivers
/// the message with its references.
#[tokio::test]
async fn a_parked_publish_holds_its_blob_and_releases_with_it_on_both_stores() {
    let f = fixture().await;
    let release_at = Utc::now() + Duration::hours(1);
    for addr in [f.durable.clone(), f.ring.clone()] {
        let photo = seal_blob(&f.m, b"later");
        let id = photo.id().clone();
        f.publish(&addr, "parked", Some(release_at), &[("photo", &photo)])
            .await;
        drop(photo);
        assert_eq!(
            redeem_all(&f.m, &id).await.unwrap(),
            b"later",
            "{addr}: parked"
        );

        f.m.release_due_messages(release_at + Duration::seconds(1))
            .await;
        let stored = f.newest(&addr).await;
        assert_eq!(stored.body.as_text(), Some("parked"));
        assert_eq!(stored.attachments.get(&name("photo")).unwrap().id, id);
        assert_eq!(
            redeem_all(&f.m, &id).await.unwrap(),
            b"later",
            "{addr}: released"
        );
    }
}

// ── Redemption and the reap ──────────────────────────────────────────────

/// Once the last row naming a blob is deleted, the blob is unredeemable at
/// once though its bytes stay on disk until the reap.
#[tokio::test]
async fn a_blob_whose_last_row_is_deleted_is_unredeemable_before_the_reap() {
    let f = fixture().await;
    let photo = seal_blob(&f.m, b"gone soon");
    let id = photo.id().clone();
    let uuid = f
        .publish(&f.durable, "named", None, &[("photo", &photo)])
        .await;
    drop(photo);

    f.delete_row(uuid).await;
    assert!(matches!(
        redeem_all(&f.m, &id).await,
        Err(BlobError::NotFound)
    ));
    assert!(f.blobs.holds_file(&id), "the bytes wait for the reap");

    assert_eq!(f.m.reap_blobs().await, ReapReport { blobs: 1, bytes: 9 });
    assert!(!f.blobs.holds_file(&id));
}

/// A blob no row names any longer is still redeemable while host code holds
/// it, and the reap leaves it alone until the hold drops.
#[tokio::test]
async fn a_held_blob_is_redeemable_after_its_last_row_is_deleted() {
    let f = fixture().await;
    let photo = seal_blob(&f.m, b"held");
    let id = photo.id().clone();
    let uuid = f
        .publish(&f.durable, "named", None, &[("photo", &photo)])
        .await;
    f.delete_row(uuid).await;

    assert_eq!(redeem_all(&f.m, &id).await.unwrap(), b"held");
    assert_eq!(f.m.reap_blobs().await, ReapReport::default());
    drop(photo);
    assert_eq!(f.m.reap_blobs().await.blobs, 1);
    assert!(!f.blobs.holds_file(&id));
}

/// A durable parked message cancelled through the store deletes its blob
/// without anyone else calling the reap.
#[tokio::test]
async fn cancelling_a_durable_parked_message_deletes_its_blob() {
    let f = fixture().await;
    let photo = seal_blob(&f.m, b"never sent");
    let kept = seal_blob(&f.m, b"kept");
    let (id, kept_id) = (photo.id().clone(), kept.id().clone());
    let release_at = Utc::now() + Duration::hours(1);
    let uuid = f
        .publish(
            &f.durable,
            "cancel me",
            Some(release_at),
            &[("photo", &photo), ("kept", &kept)],
        )
        .await;
    f.publish(&f.durable, "keeps", None, &[("kept", &kept)])
        .await;
    drop((photo, kept));

    let sender = ParticipantId::for_system(PUBLISHER);
    assert_eq!(
        f.m.cancel_deferred_if_present(&f.durable, sender.as_str(), uuid, Utc::now())
            .await,
        Some(DeferralOutcome::Applied)
    );
    assert!(!f.blobs.holds_file(&id));
    assert!(matches!(
        redeem_all(&f.m, &id).await,
        Err(BlobError::NotFound)
    ));
    assert_eq!(redeem_all(&f.m, &kept_id).await.unwrap(), b"kept");
}

/// Without a blob store nothing is redeemable and a reap does nothing.
#[tokio::test]
async fn a_messenger_without_a_blob_store_redeems_nothing() {
    let m = Messenger::new(
        init_db_memory(),
        Arc::new(MessagingDirectory::with_entries(vec![])),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(CountingRouter::default()) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    );
    let id = BlobId::new("0".repeat(32));
    assert!(matches!(m.retain_blob(&id).await, Err(BlobError::NotFound)));
    assert_eq!(m.reap_blobs().await, ReapReport::default());
}

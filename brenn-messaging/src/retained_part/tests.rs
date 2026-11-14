//! The shared admission over a durable and a ring channel, asked directly:
//! each refusal with its cause, the tool-substrate rule both ways, and the
//! hold an admitted attachment gives.

use std::sync::Arc;

use chrono::{Duration, Utc};
use indexmap::IndexMap;

use super::*;
use crate::config::MessagingGlobalConfig;
use crate::db::{init_db_memory, upsert_channels};
use crate::query::NoopWakeRouter;
use crate::store::RingStores;
use crate::test_support::test_channel_entry;
use crate::testutils::{
    TempBlobDir, covering_system_publisher, ephemeral_channel_entry, install_blob_store_at,
    publish_attached, seal_blob,
};
use crate::{ChannelEntry, MessagingDirectory, WakeRouter};

const PUBLISHER: &str = "part-producer";
const RING_DEPTH: u64 = 2;

struct Fixture {
    m: Arc<Messenger>,
    // Declared after the messenger, so the directory outlives its store.
    _blobs: TempBlobDir,
    /// Unbounded standing window.
    durable: String,
    /// A standing window two messages deep.
    windowed: String,
    ring: String,
    /// A durable channel of the tool substrate.
    tool: String,
}

async fn fixture() -> Fixture {
    let durable = test_channel_entry("part-durable", vec![]);
    let mut windowed = test_channel_entry("part-windowed", vec![]);
    windowed.resolved_channel.retain_depth = Depth::Bounded(1);
    windowed.resolved_channel.standing_retain_depth = Depth::Bounded(2);
    let tool = test_channel_entry("tool-requests/part", vec![]);
    let ring = ephemeral_channel_entry("part-ring", RING_DEPTH);
    let addresses = [
        durable.address.clone(),
        windowed.address.clone(),
        ring.address.clone(),
        tool.address.clone(),
    ];
    let durables: Vec<ChannelEntry> = vec![durable, windowed, tool];
    let db = init_db_memory();
    upsert_channels(&*db.lock().await, &durables);
    let rings = vec![ring];
    let m = Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(
            durables.into_iter().chain(rings.iter().cloned()).collect(),
        )),
        Arc::from("test"),
        Arc::new(IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(covering_system_publisher(PUBLISHER))
    .with_ring_stores(Arc::new(RingStores::build(&rings)));
    let blobs = TempBlobDir::new();
    let m = install_blob_store_at(m, blobs.path()).await;
    let [durable, windowed, ring, tool] = addresses;
    Fixture {
        m,
        _blobs: blobs,
        durable,
        windowed,
        ring,
        tool,
    }
}

fn name(s: &str) -> AttachmentName {
    AttachmentName::new(s).unwrap()
}

fn attachment(s: &str) -> NamedPart {
    NamedPart::Attachment(name(s))
}

impl Fixture {
    /// Publish `body` onto `addr` carrying one fresh blob of `bytes` as
    /// `photo`, and drop the caller's hold on it.
    async fn publish_photo(&self, addr: &str, body: &str, bytes: &[u8]) -> Uuid {
        let photo = seal_blob(&self.m, bytes);
        publish_attached(&self.m, PUBLISHER, addr, body, &[("photo", &photo)], None).await
    }

    async fn publish_plain(&self, addr: &str, body: &str) -> Uuid {
        publish_attached(&self.m, PUBLISHER, addr, body, &[], None).await
    }

    async fn admit(
        &self,
        addr: &str,
        message_id: Uuid,
        part: &NamedPart,
    ) -> Result<AdmittedPart, PartRefusal> {
        self.m
            .admit_retained_part(addr, message_id, part, ToolChannelAttachments::Refused)
            .await
    }

    async fn admit_hold(&self, addr: &str, message_id: Uuid, part: &NamedPart) -> BlobHold {
        match self.admit(addr, message_id, part).await {
            Ok(AdmittedPart::Attachment(hold)) => hold,
            other => panic!("{addr}: expected an attachment, got {other:?}"),
        }
    }

    async fn unretain_row(&self, message_id: Uuid) {
        self.m
            .db()
            .lock()
            .await
            .execute(
                "UPDATE messaging_messages SET retained_seq = NULL WHERE uuid = ?1",
                [message_id.as_bytes().to_vec()],
            )
            .unwrap();
    }

    fn blob_count(&self) -> usize {
        self.m.blob_store().unwrap().usage().sealed
    }
}

fn read_all(hold: &BlobHold) -> Vec<u8> {
    let size = usize::try_from(hold.reference().size).unwrap();
    hold.read(0, size).unwrap()
}

/// On both stores an attachment the message carries is admitted as a hold on
/// its blob, and the body as the body.
#[tokio::test]
async fn an_attachment_and_a_body_are_admitted_from_either_store() {
    let f = fixture().await;
    for addr in [&f.durable, &f.ring] {
        let uuid = f.publish_photo(addr, "a photo", b"photo bytes").await;
        let hold = f.admit_hold(addr, uuid, &attachment("photo")).await;
        assert_eq!(read_all(&hold), b"photo bytes", "{addr}");
        match f.admit(addr, uuid, &NamedPart::Body).await {
            Ok(AdmittedPart::Body(body)) => assert_eq!(body.as_text(), Some("a photo")),
            other => panic!("{addr}: expected the body, got {other:?}"),
        }
    }
}

/// A name the message does not carry is refused with the names it does.
#[tokio::test]
async fn a_name_the_message_does_not_carry_is_refused_with_what_it_carries() {
    let f = fixture().await;
    for addr in [&f.durable, &f.ring] {
        let uuid = f.publish_photo(addr, "one", b"p").await;
        assert_eq!(
            f.admit(addr, uuid, &attachment("scan")).await.unwrap_err(),
            PartRefusal::NoSuchAttachment {
                carried: vec![name("photo")]
            },
            "{addr}"
        );
        let plain = f.publish_plain(addr, "none").await;
        assert_eq!(
            f.admit(addr, plain, &attachment("photo"))
                .await
                .unwrap_err(),
            PartRefusal::NoSuchAttachment { carried: vec![] },
            "{addr}"
        );
    }
}

/// A channel not in the directory is gone, whatever is named.
#[tokio::test]
async fn a_channel_not_in_the_directory_is_gone() {
    let f = fixture().await;
    for part in [attachment("photo"), NamedPart::Body] {
        assert_eq!(
            f.admit("brenn:part-gone", Uuid::new_v4(), &part)
                .await
                .unwrap_err(),
            PartRefusal::ChannelGone
        );
    }
}

/// An evicted ring message and a durable row out of retention are not
/// retained; nor is a message asked for on a channel it is not on.
#[tokio::test]
async fn an_evicted_or_misplaced_message_is_not_retained() {
    let f = fixture().await;
    let ring = f.publish_photo(&f.ring, "first", b"p").await;
    for i in 0..RING_DEPTH {
        f.publish_plain(&f.ring, &format!("evicts {i}")).await;
    }
    assert_eq!(
        f.admit(&f.ring, ring, &attachment("photo"))
            .await
            .unwrap_err(),
        PartRefusal::NotRetained
    );

    let durable = f.publish_photo(&f.durable, "row", b"p").await;
    assert_eq!(
        f.admit(&f.windowed, durable, &attachment("photo"))
            .await
            .unwrap_err(),
        PartRefusal::NotRetained,
        "the id names a row on another channel"
    );
    f.unretain_row(durable).await;
    assert_eq!(
        f.admit(&f.durable, durable, &NamedPart::Body)
            .await
            .unwrap_err(),
        PartRefusal::NotRetained
    );
}

/// The durable window is the channel's standing depth: a row past it, still
/// on disk, is not retained.
#[tokio::test]
async fn the_durable_window_is_the_standing_depth() {
    let f = fixture().await;
    let first = f.publish_photo(&f.windowed, "first", b"first").await;
    f.publish_plain(&f.windowed, "second").await;
    let hold = f.admit_hold(&f.windowed, first, &attachment("photo")).await;
    assert_eq!(read_all(&hold), b"first");
    f.publish_plain(&f.windowed, "third").await;
    assert_eq!(
        f.admit(&f.windowed, first, &attachment("photo"))
            .await
            .unwrap_err(),
        PartRefusal::NotRetained
    );
}

/// A parked message is not retained until it is released.
#[tokio::test]
async fn a_parked_message_is_not_retained_until_released() {
    let f = fixture().await;
    for addr in [&f.durable, &f.ring] {
        let photo = seal_blob(&f.m, b"later bytes");
        let at = Utc::now() + Duration::hours(1);
        let uuid = publish_attached(
            &f.m,
            PUBLISHER,
            addr,
            "later",
            &[("photo", &photo)],
            Some(at),
        )
        .await;
        drop(photo);
        assert_eq!(
            f.admit(addr, uuid, &attachment("photo")).await.unwrap_err(),
            PartRefusal::NotRetained,
            "{addr}"
        );
        let entry = f.m.directory().resolve(addr).unwrap();
        f.m.store_for(&entry)
            .release_due(Utc::now() + Duration::days(1))
            .await;
        let hold = f.admit_hold(addr, uuid, &attachment("photo")).await;
        assert_eq!(read_all(&hold), b"later bytes", "{addr}");
    }
}

/// On a tool-substrate channel an attachment is refused under `Refused`
/// before the message is looked at — an unknown name and an unretained row
/// get the same answer, and no hold is taken — and admitted under `Admitted`.
/// The body is admitted either way.
#[tokio::test]
async fn a_tool_channels_attachment_follows_the_callers_rule() {
    let f = fixture().await;
    assert_eq!(
        SystemChannelFamily::of(&f.tool),
        Some(SystemChannelFamily::Tool)
    );
    let uuid = f.publish_photo(&f.tool, "call", b"tool bytes").await;
    for part in [attachment("photo"), attachment("other")] {
        assert_eq!(
            f.admit(&f.tool, uuid, &part).await.unwrap_err(),
            PartRefusal::ToolChannelAttachment,
            "{part:?}"
        );
    }
    assert_eq!(
        f.admit(&f.tool, Uuid::new_v4(), &attachment("photo"))
            .await
            .unwrap_err(),
        PartRefusal::ToolChannelAttachment
    );
    assert!(matches!(
        f.admit(&f.tool, uuid, &NamedPart::Body).await,
        Ok(AdmittedPart::Body(_))
    ));
    match f
        .m
        .admit_retained_part(
            &f.tool,
            uuid,
            &attachment("photo"),
            ToolChannelAttachments::Admitted,
        )
        .await
    {
        Ok(AdmittedPart::Attachment(hold)) => assert_eq!(read_all(&hold), b"tool bytes"),
        other => panic!("admitted: {other:?}"),
    }

    f.unretain_row(uuid).await;
    f.m.db()
        .lock()
        .await
        .execute(
            "DELETE FROM messaging_messages WHERE uuid = ?1",
            [uuid.as_bytes().to_vec()],
        )
        .unwrap();
    f.m.reap_blobs().await;
    assert_eq!(f.blob_count(), 0, "the refusals held nothing");
}

/// An admitted hold keeps the bytes after the message that named them is
/// evicted, on both stores.
#[tokio::test]
async fn an_admitted_hold_outlives_the_messages_eviction() {
    let f = fixture().await;
    let uuid = f.publish_photo(&f.ring, "ring", b"kept bytes").await;
    let hold = f.admit_hold(&f.ring, uuid, &attachment("photo")).await;
    for i in 0..RING_DEPTH {
        f.publish_plain(&f.ring, &format!("evicts {i}")).await;
    }
    assert_eq!(read_all(&hold), b"kept bytes");

    let uuid = f
        .publish_photo(&f.durable, "durable", b"durable bytes")
        .await;
    let durable_hold = f.admit_hold(&f.durable, uuid, &attachment("photo")).await;
    f.m.db()
        .lock()
        .await
        .execute(
            "DELETE FROM messaging_messages WHERE uuid = ?1",
            [uuid.as_bytes().to_vec()],
        )
        .unwrap();
    f.m.reap_blobs().await;
    assert_eq!(read_all(&durable_hold), b"durable bytes");
    assert_eq!(f.blob_count(), 2);
    drop(hold);
    drop(durable_hold);
    f.m.reap_blobs().await;
    assert_eq!(f.blob_count(), 0);
}

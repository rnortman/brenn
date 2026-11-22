//! The ring half of a blob's lifetime: a ring entry holds every blob its
//! message names, and the blobs go when the ring's last copy of the entry
//! drops, wherever that happens.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use brenn_envelope::{AttachmentName, BlobId, BlobRef, ChannelScheme, MessageEnvelope, Urgency};
use brenn_lib::messaging::ChannelEntry;
use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
use brenn_lib::messaging::{WakeMin, nondurable_channel_uuid};
use brenn_queue::QuotaExceeded;

use crate::blob::{BlobHold, BlobLimits, BlobStore, BlobUsage, HeldAttachments, Retain};
use crate::db::utc_to_ns;
use crate::store::{
    DeferralOutcome, NewMessage, RetentionStore, RingNamed, RingStore, RingStores, no_holds,
};
use crate::testutils::held as attachments;

const ADDRESS: &str = "ephemeral:pictures";

const LIMITS: BlobLimits = BlobLimits {
    max_blob_bytes: 1 << 20,
    budget_bytes: 4 << 20,
};

struct Fixture {
    dir: tempfile::TempDir,
    blobs: Arc<BlobStore>,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let blobs = BlobStore::for_tests(dir.path(), LIMITS);
    Fixture { dir, blobs }
}

impl Fixture {
    fn sealed(&self, bytes: &[u8]) -> BlobHold {
        crate::testutils::seal_bytes(&self.blobs, "image/jpeg", bytes)
    }

    /// Whether `id` is still a live blob: its file on disk, its charge in the
    /// store, and a hold obtainable for it.
    fn alive(&self, id: &BlobId) -> bool {
        let file = crate::testutils::holds_file(self.dir.path(), id);
        let retained = match self.blobs.retain(id) {
            Retain::Held(_) => true,
            Retain::NotFound => false,
            Retain::Unsettled => panic!("a ring-only blob is never unsettled"),
        };
        assert_eq!(
            file, retained,
            "blob {id}: file on disk and index disagree about whether it lives"
        );
        retained
    }

    fn empty(&self) -> bool {
        self.blobs.usage()
            == BlobUsage {
                sealed: 0,
                unsealed: 0,
                charged_bytes: 0,
            }
    }
}

fn name(s: &str) -> AttachmentName {
    AttachmentName::new(s).unwrap()
}

fn message(body: &str, attachments: HeldAttachments) -> NewMessage {
    NewMessage {
        source: "node".to_string(),
        sender: "alice".to_string(),
        body: body.into(),
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Ephemeral,
        transport: None,
        reply_to: None,
        delivery_deadline: None,
        impetus: None,
        publish_ts_ns: utc_to_ns(now()),
        attachments,
    }
}

fn ring(depth: u64) -> RingStore {
    RingStore::new(Uuid::new_v4(), ADDRESS, Depth::Bounded(depth))
}

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}

fn later(secs: i64) -> DateTime<Utc> {
    now() + chrono::Duration::seconds(secs)
}

/// Publish a message naming `holds` under the given names and let the
/// producer's own holds go, so the ring is the only holder.
async fn append_naming(ring: &RingStore, body: &str, holds: &[(&str, &BlobHold)]) -> Uuid {
    RetentionStore::append(ring, message(body, attachments(holds)))
        .await
        .committed
        .message_uuid
}

async fn park_naming(
    ring: &RingStore,
    body: &str,
    holds: &[(&str, &BlobHold)],
    at: DateTime<Utc>,
) -> Result<Uuid, QuotaExceeded> {
    RetentionStore::park(ring, message(body, attachments(holds)), at)
        .await
        .map(|parked| parked.message_uuid)
}

// ── What the ring carries ────────────────────────────────────────────────

/// Every read path hands out the envelope with its references, so a reader
/// sees exactly what the producer named.
#[tokio::test]
async fn a_retained_message_carries_its_references() {
    let f = fixture();
    let a = f.sealed(b"aaaa");
    let b = f.sealed(b"bb");
    let ring = ring(4);
    append_naming(&ring, "two", &[("photo", &a), ("thumb", &b), ("again", &a)]).await;

    let tail = RetentionStore::retained_tail(&ring, Depth::Bounded(1)).await;
    let refs = &tail[0].attachments;
    assert_eq!(refs.len(), 3);
    assert_eq!(
        refs.get(&name("photo")),
        Some(&BlobRef {
            id: a.id().clone(),
            size: 4,
            content_type: "image/jpeg".to_string(),
        })
    );
    assert_eq!(refs.get(&name("again")).unwrap().id, *a.id());
    assert_eq!(refs.get(&name("thumb")).unwrap().size, 2);

    let replay = ring.replay(None);
    assert_eq!(replay.messages[0].message.attachments, *refs);
}

/// A message naming a blob the ring is not given a hold on is refused at the
/// mint: a held message never names a blob whose bytes may go.
#[test]
#[should_panic(expected = "which the message does not hold")]
fn a_ring_message_naming_an_unheld_blob_panics() {
    let f = fixture();
    let a = f.sealed(b"a");
    let refs = attachments(&[("photo", &a)]).refs().clone();
    let envelope = MessageEnvelope {
        message_id: Uuid::new_v4(),
        source: "node".to_string(),
        channel: ADDRESS.to_string(),
        sender: "alice".to_string(),
        publish_ts: now(),
        body: "x".into(),
        reply_to: None,
        delivery_deadline: None,
        deliver_after: None,
        impetus: None,
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Ephemeral,
        transport: None,
        attachments: refs,
    };
    ring(4).append(envelope, no_holds());
}

// ── Every way a ring message stops being held ────────────────────────────

/// Overflow drops the oldest entry, and with it the blob only that message
/// named; a blob a still-retained message names stays.
#[tokio::test]
async fn ring_overflow_deletes_the_blob_only_the_evicted_message_named() {
    let f = fixture();
    let only_first = f.sealed(b"first");
    let shared = f.sealed(b"shared");
    let (only_id, shared_id) = (only_first.id().clone(), shared.id().clone());
    let ring = ring(1);
    append_naming(&ring, "one", &[("a", &only_first), ("b", &shared)]).await;
    append_naming(&ring, "two", &[("b", &shared)]).await;
    drop((only_first, shared));

    assert!(!f.alive(&only_id), "the evicted message alone named it");
    assert!(f.alive(&shared_id), "the retained message still names it");

    append_naming(&ring, "three", &[]).await;
    assert!(!f.alive(&shared_id));
    assert!(f.empty());
}

/// A release enters retention like an append, so the overflow it causes
/// drops the evicted entry's blobs the same way.
#[tokio::test]
async fn an_overflow_caused_by_a_release_deletes_the_evicted_blob() {
    let f = fixture();
    let a = f.sealed(b"a");
    let id = a.id().clone();
    let ring = ring(1);
    append_naming(&ring, "retained", &[("a", &a)]).await;
    park_naming(&ring, "parked", &[], later(10)).await.unwrap();
    drop(a);
    assert!(f.alive(&id));

    let released = RetentionStore::release_due(&ring, later(10)).await;
    assert_eq!(released.released.len(), 1);
    assert!(!f.alive(&id));
    assert!(f.empty());
}

/// A depth-0 ring retains nothing, so the entry, and its blobs, go at once.
#[tokio::test]
async fn an_append_at_depth_zero_deletes_the_blob_at_once() {
    let f = fixture();
    let a = f.sealed(b"a");
    let id = a.id().clone();
    let ring = ring(0);
    append_naming(&ring, "gone", &[("a", &a)]).await;
    drop(a);
    assert!(!f.alive(&id));
    assert!(f.empty());
}

/// A park refused at the cap holds nothing.
#[tokio::test]
async fn a_park_refused_at_the_cap_releases_its_blobs() {
    let f = fixture();
    let a = f.sealed(b"a");
    let id = a.id().clone();
    let ring = ring(1);
    park_naming(&ring, "fills", &[], later(10)).await.unwrap();
    assert_eq!(
        park_naming(&ring, "refused", &[("a", &a)], later(10)).await,
        Err(QuotaExceeded { cap: 1 })
    );
    drop(a);
    assert!(!f.alive(&id));
}

/// A cancelled parked message is discarded, and its blobs with it.
#[tokio::test]
async fn a_cancelled_parked_ring_message_deletes_its_blob() {
    let f = fixture();
    let a = f.sealed(b"a");
    let kept = f.sealed(b"kept");
    let (id, kept_id) = (a.id().clone(), kept.id().clone());
    let ring = ring(4);
    let uuid = park_naming(&ring, "cancel me", &[("a", &a), ("k", &kept)], later(10))
        .await
        .unwrap();
    park_naming(&ring, "stays", &[("k", &kept)], later(20))
        .await
        .unwrap();
    drop((a, kept));

    assert_eq!(
        ring.cancel_deferred("alice", uuid, now()),
        DeferralOutcome::Applied
    );
    assert!(!f.alive(&id));
    assert!(f.alive(&kept_id), "the other parked message still names it");
}

/// A channel removed or retuned at reload is deregistered; its blobs go when
/// the last reference to its store drops, and not before.
#[tokio::test]
async fn a_deregistered_ring_releases_its_blobs_when_the_store_drops() {
    let f = fixture();
    let a = f.sealed(b"a");
    let b = f.sealed(b"b");
    let (a_id, b_id) = (a.id().clone(), b.id().clone());
    let (scheme, bare) = ChannelScheme::split(ADDRESS).unwrap();
    let entry = ChannelEntry {
        uuid: nondurable_channel_uuid(scheme, bare),
        address: ADDRESS.to_string(),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Bounded(4),
            retain_depth: Depth::Bounded(8),
            standing_retain_depth: Depth::Bounded(8),
            noise: NoiseLevel::Metered,
            sink: Sink::Drop,
            wake_min: WakeMin::Normal,
            non_text: false,
            untyped_content_type: None,
        },
        subscribers: vec![],
        transport_type: scheme,
        mount: None,
    };
    let stores = RingStores::build(std::slice::from_ref(&entry));
    let store = stores.get(&entry.uuid).unwrap();
    append_naming(&store, "retained", &[("a", &a)]).await;
    park_naming(&store, "parked", &[("b", &b)], later(10))
        .await
        .unwrap();
    drop((a, b));

    assert!(stores.deregister(&entry.uuid));
    assert!(
        f.alive(&a_id) && f.alive(&b_id),
        "a caller still holds the store"
    );
    drop(store);
    assert!(!f.alive(&a_id), "the retained message's blob");
    assert!(!f.alive(&b_id), "the parked message's blob");
    assert!(f.empty());
}

// ── What keeps a ring blob ──────────────────────────────────────────────

/// A parked message holds its blobs exactly as a retained one does, and the
/// release carries the holds into the window.
#[tokio::test]
async fn a_parked_ring_message_released_into_the_window_keeps_its_blob() {
    let f = fixture();
    let a = f.sealed(b"parked bytes");
    let id = a.id().clone();
    let ring = ring(4);
    park_naming(&ring, "later", &[("a", &a)], later(10))
        .await
        .unwrap();
    drop(a);
    assert!(f.alive(&id), "a parked message is a holder");

    let released = RetentionStore::release_due(&ring, later(10)).await;
    assert_eq!(
        released.released[0]
            .envelope
            .attachments
            .get(&name("a"))
            .unwrap()
            .id,
        id
    );
    assert!(f.alive(&id), "the released message is a holder");
    let Retain::Held(hold) = f.blobs.retain(&id) else {
        panic!("a retained message's blob is redeemable");
    };
    assert_eq!(hold.read(0, 6).unwrap(), b"parked");
}

/// An edit replaces the body and keeps the attachments, and the replaced
/// entry's drop does not release the blobs the edited one still names.
#[tokio::test]
async fn an_edited_parked_ring_message_keeps_its_attachments_and_blobs() {
    let f = fixture();
    let a = f.sealed(b"a");
    let id = a.id().clone();
    let ring = ring(4);
    let uuid = park_naming(&ring, "draft", &[("a", &a)], later(10))
        .await
        .unwrap();
    drop(a);

    assert_eq!(
        ring.edit_deferred("alice", uuid, Some("final".into()), Some(later(5)), now()),
        DeferralOutcome::Applied
    );
    assert!(f.alive(&id));
    let view = ring.deferred_for_sender("alice");
    assert_eq!(view[0].envelope.body.as_text(), Some("final"));
    assert_eq!(view[0].envelope.attachments.get(&name("a")).unwrap().id, id);

    assert_eq!(
        ring.cancel_deferred("alice", uuid, now()),
        DeferralOutcome::Applied
    );
    assert!(!f.alive(&id), "the edited entry held the blob, and only it");
}

/// A reader's copy of the envelope is not a holder: the blob goes with the
/// ring's entry even while a reader still has the envelope in hand.
#[tokio::test]
async fn a_readers_envelope_does_not_hold_the_blob() {
    let f = fixture();
    let a = f.sealed(b"a");
    let id = a.id().clone();
    let ring = ring(1);
    append_naming(&ring, "read me", &[("a", &a)]).await;
    drop(a);
    let in_hand = ring.retained_tail(1).remove(0).envelope;
    let window = ring.window(&reader(), 0, 1).unwrap();

    append_naming(&ring, "evicts", &[]).await;
    assert!(!f.alive(&id));
    assert_eq!(in_hand.attachments.get(&name("a")).unwrap().id, id);
    assert_eq!(window.entries.len(), 1);
}

fn reader() -> brenn_lib::messaging::ParticipantId {
    brenn_lib::messaging::ParticipantId::for_wasm("reader")
}

/// Host code holding a blob keeps it across the eviction of its last
/// message; dropping the hold then deletes it.
#[tokio::test]
async fn a_hold_keeps_a_ring_blob_across_its_last_messages_eviction() {
    let f = fixture();
    let hold = f.sealed(b"held");
    let id = hold.id().clone();
    let ring = ring(1);
    append_naming(&ring, "names it", &[("a", &hold)]).await;
    append_naming(&ring, "evicts", &[]).await;

    assert!(f.alive(&id));
    assert_eq!(hold.read(0, 4).unwrap(), b"held");
    drop(hold);
    assert!(!f.alive(&id));
    assert!(f.empty());
}

/// A sealed blob its producer drops without publishing is deleted.
#[test]
fn a_sealed_unpublished_blob_is_deleted_when_its_producer_drops_it() {
    let f = fixture();
    let hold = f.sealed(b"never sent");
    let id = hold.id().clone();
    drop(hold);
    assert!(!f.alive(&id));
    assert!(f.empty());
}

// ── Naming one retained message's part ──────────────────────────────────

/// Each of `retained_named`'s four answers.
#[tokio::test]
async fn retained_named_answers_by_message_and_name() {
    let f = fixture();
    let a = f.sealed(b"aaaa");
    let ring = ring(4);
    let uuid = append_naming(&ring, "carries a", &[("photo", &a)]).await;
    let parked = park_naming(&ring, "parked", &[("photo", &a)], later(10))
        .await
        .unwrap();

    match ring.retained_named(uuid, Some(&name("photo"))) {
        RingNamed::Attachment(envelope, hold) => {
            assert_eq!(envelope.message_id, uuid);
            assert_eq!(hold.id(), a.id());
            assert_eq!(hold.read(0, 4).unwrap(), b"aaaa");
        }
        other => panic!("expected the attachment, got {other:?}"),
    }
    match ring.retained_named(uuid, None) {
        RingNamed::Body(envelope) => assert_eq!(envelope.body.as_text(), Some("carries a")),
        other => panic!("expected the body, got {other:?}"),
    }
    match ring.retained_named(uuid, Some(&name("thumb"))) {
        RingNamed::NoSuchAttachment(envelope) => assert_eq!(envelope.message_id, uuid),
        other => panic!("expected no such attachment, got {other:?}"),
    }
    assert!(matches!(
        ring.retained_named(Uuid::new_v4(), None),
        RingNamed::NotRetained
    ));
    assert!(
        matches!(ring.retained_named(parked, None), RingNamed::NotRetained),
        "a parked message is not retained"
    );
}

/// An evicted message is not retained, whatever it named.
#[tokio::test]
async fn retained_named_refuses_an_evicted_message() {
    let f = fixture();
    let a = f.sealed(b"a");
    let ring = ring(1);
    let uuid = append_naming(&ring, "evicted", &[("photo", &a)]).await;
    append_naming(&ring, "evicts", &[]).await;
    assert!(matches!(
        ring.retained_named(uuid, Some(&name("photo"))),
        RingNamed::NotRetained
    ));
}

/// The hold handed out is on the one blob named: after the message is
/// evicted, it keeps that blob readable while the message's other blob is
/// unlinked and its charge returned.
#[tokio::test]
async fn a_named_hold_keeps_only_the_named_blob_past_eviction() {
    let f = fixture();
    let named = f.sealed(b"named bytes");
    let other = f.sealed(b"other");
    let (named_id, other_id) = (named.id().clone(), other.id().clone());
    let ring = ring(1);
    let uuid = append_naming(&ring, "two", &[("keep", &named), ("drop", &other)]).await;
    drop((named, other));

    let RingNamed::Attachment(_, hold) = ring.retained_named(uuid, Some(&name("keep"))) else {
        panic!("the retained message carries the name");
    };
    append_naming(&ring, "evicts", &[]).await;

    assert!(f.alive(&named_id), "the named hold keeps its blob");
    assert!(!f.alive(&other_id), "the other blob had only the entry");
    assert_eq!(f.blobs.usage().charged_bytes, 11);
    assert_eq!(hold.read(0, 11).unwrap(), b"named bytes");
    drop(hold);
    assert!(!f.alive(&named_id));
    assert!(f.empty());
}

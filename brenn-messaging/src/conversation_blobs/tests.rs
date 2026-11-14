//! The naming rule over a durable and a ring channel: the read record, each
//! refusal with its cause, the hold an admitted name gives, and
//! `publish_with_attachments`.

use std::sync::Arc;

use chrono::{Duration, Utc};
use indexmap::IndexMap;
use uuid::Uuid;

use super::*;
use crate::config::{MessagingGlobalConfig, ResolvedMessagingConfig};
use crate::conversations::ConversationDelivery;
use crate::db::{init_db_memory, upsert_channels};
use crate::query::{MessageQuery, NoopWakeRouter};
use crate::store::RingStores;
use crate::testutils::{
    TempBlobDir, covering_system_publisher, ephemeral_channel_entry, install_blob_store_at,
    publish_attached, seal_blob,
};
use crate::{
    ChannelEntry, MessagingDirectory, PublishOrigin, PublishResult, Urgency, WakeRouter,
    test_support::{test_app_config, test_channel_entry},
};
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_messaging_store::blob::{BlobHold, HeldAttachments};

const APP: &str = "reader-app";
const CONV: i64 = 7;
const PUBLISHER: &str = "blob-producer";
const DURABLE: &str = "blob-durable";
const WINDOWED: &str = "blob-windowed";
const HIDDEN: &str = "hidden";
const RING: &str = "blob-ring";
const RING_DEPTH: u64 = 2;

struct Fixture {
    m: Arc<Messenger>,
    // Declared after the messenger, so the directory outlives its store.
    _blobs: TempBlobDir,
    durable: String,
    windowed: String,
    hidden: String,
    ring: String,
}

/// Reads `brenn:blob-*` and every `ephemeral:` channel, and may publish onto
/// both schemes; `brenn:hidden` is outside its read grant.
fn app() -> brenn_lib::config::AppConfig {
    let mut cfg = test_app_config(
        APP,
        Some(ResolvedMessagingConfig {
            send_budget: 100,
            subscriptions: vec![],
        }),
        vec![],
    );
    let mut policy = AppPolicy::default();
    for grant in [
        AppCapability::MessagingSubscribe,
        AppCapability::MessagingPublish,
        AppCapability::EphemeralSubscribe,
        AppCapability::EphemeralPublish,
    ] {
        policy.grants.insert(grant);
    }
    let everything = ChannelMatcher::Prefix(String::new());
    policy
        .acls
        .brenn_subscribe
        .push(ChannelMatcher::Prefix("blob-".to_string()));
    policy.acls.brenn_publish.push(everything.clone());
    policy.acls.ephemeral_subscribe.push(everything.clone());
    policy.acls.ephemeral_publish.push(everything);
    *cfg.policy_mut() = policy;
    cfg
}

/// A messenger over three durable channels — one unbounded, one whose
/// standing window is two messages deep over a subscriber depth of one, one
/// the app may not read — and a ring of depth [`RING_DEPTH`], with a blob
/// store, a system publisher allowed everywhere, and [`APP`]'s conversation
/// [`CONV`].
async fn fixture() -> Fixture {
    let durable = test_channel_entry(DURABLE, vec![]);
    let mut windowed = test_channel_entry(WINDOWED, vec![]);
    windowed.resolved_channel.retain_depth = Depth::Bounded(1);
    windowed.resolved_channel.standing_retain_depth = Depth::Bounded(2);
    let hidden = test_channel_entry(HIDDEN, vec![]);
    let ring = ephemeral_channel_entry(RING, RING_DEPTH);
    let addresses = [
        durable.address.clone(),
        windowed.address.clone(),
        hidden.address.clone(),
        ring.address.clone(),
    ];
    let durables: Vec<ChannelEntry> = vec![durable, windowed, hidden];
    let db = init_db_memory();
    {
        let conn = db.lock().await;
        conn.execute(
            "INSERT INTO users (id, username, password_hash, created_at) \
             VALUES (1, 'alice', 'h', '2024-01-01')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO conversations (id, user_id, status, app_slug, created_at, updated_at) \
             VALUES (?1, 1, 'active', ?2, '2024-01-01', '2024-01-01')",
            rusqlite::params![CONV, APP],
        )
        .unwrap();
        upsert_channels(&conn, &durables);
    }
    let mut apps = IndexMap::new();
    apps.insert(APP.to_string(), app());
    let rings = vec![ring];
    let m = Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(
            durables.into_iter().chain(rings.iter().cloned()).collect(),
        )),
        Arc::from("test"),
        Arc::new(apps),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(covering_system_publisher(PUBLISHER))
    .with_ring_stores(Arc::new(RingStores::build(&rings)));
    let blobs = TempBlobDir::new();
    let m = install_blob_store_at(m, blobs.path()).await;
    let [durable, windowed, hidden, ring] = addresses;
    Fixture {
        m,
        _blobs: blobs,
        durable,
        windowed,
        hidden,
        ring,
    }
}

fn name(s: &str) -> AttachmentName {
    AttachmentName::new(s).unwrap()
}

fn attachment(s: &str) -> NamedPart {
    NamedPart::Attachment(name(s))
}

impl Fixture {
    async fn publish(&self, addr: &str, body: &str, holds: &[(&str, &BlobHold)]) -> Uuid {
        publish_attached(&self.m, PUBLISHER, addr, body, holds, None).await
    }

    /// Read `addr` as the conversation does through `MessageChannelGet`,
    /// recording what the read returns.
    async fn read(&self, addr: &str) {
        let envelopes = self
            .m
            .query(&MessageQuery {
                channel: addr.to_string(),
                limit: 100,
                before: None,
                after: None,
                sender: None,
                search: None,
                calling_app_slug: APP.to_string(),
            })
            .await
            .unwrap_or_else(|e| panic!("{addr}: the app reads it: {e:?}"));
        self.m.note_conversation_read(CONV, &envelopes);
    }

    async fn admit(&self, message_id: Uuid, part: &NamedPart) -> Result<AdmittedPart, NameRefusal> {
        self.m
            .admit_conversation_name(CONV, APP, message_id, part)
            .await
    }

    async fn admit_bytes(&self, message_id: Uuid, part: &NamedPart) -> Vec<u8> {
        match self.admit(message_id, part).await {
            Ok(AdmittedPart::Attachment(hold)) => {
                let size = usize::try_from(hold.reference().size).unwrap();
                hold.read(0, size).unwrap()
            }
            other => panic!("expected an attachment, got {other:?}"),
        }
    }

    async fn release_all(&self, addr: &str) {
        let entry = self.m.directory().resolve(addr).unwrap();
        self.m
            .store_for(&entry)
            .release_due(Utc::now() + Duration::days(1))
            .await;
    }

    fn blob_count(&self) -> usize {
        self.m.blob_store().unwrap().usage().sealed
    }
}

// ── The read record ─────────────────────────────────────────────────────

/// On both stores: a message never handed over is refused `not_read`; a read
/// admits it; a forget refuses it again; a second read admits it again.
#[tokio::test]
async fn a_name_is_admitted_only_after_the_conversation_read_its_message() {
    let f = fixture().await;
    for addr in [&f.durable, &f.ring] {
        let photo = seal_blob(&f.m, b"photo bytes");
        let uuid = f.publish(addr, "a photo", &[("photo", &photo)]).await;
        drop(photo);

        assert_eq!(
            f.admit(uuid, &attachment("photo")).await.unwrap_err(),
            NameRefusal::NotRead,
            "{addr}"
        );
        f.read(addr).await;
        assert_eq!(
            f.admit_bytes(uuid, &attachment("photo")).await,
            b"photo bytes"
        );

        f.m.forget_conversation_reads(CONV);
        assert_eq!(
            f.admit(uuid, &attachment("photo")).await.unwrap_err(),
            NameRefusal::NotRead,
            "{addr}: a forget clears the record"
        );
        f.read(addr).await;
        assert_eq!(
            f.admit_bytes(uuid, &attachment("photo")).await,
            b"photo bytes"
        );
    }
}

/// The record is per conversation: another conversation's read admits
/// nothing for this one.
#[tokio::test]
async fn the_record_is_per_conversation() {
    let f = fixture().await;
    let uuid = f.publish(&f.durable, "text", &[]).await;
    f.m.note_conversation_sent(CONV + 1, uuid, &f.durable);
    assert_eq!(
        f.admit(uuid, &NamedPart::Body).await.unwrap_err(),
        NameRefusal::NotRead
    );
}

/// What a push delivery hands over is recorded when its positions advance.
#[tokio::test]
async fn advance_conversation_records_the_batch() {
    let f = fixture().await;
    let uuid = f.publish(&f.ring, "pushed", &[]).await;
    let envelope =
        f.m.store_for_address(&f.ring)
            .retained_tail(Depth::Bounded(1))
            .await
            .pop()
            .unwrap();
    let mut delivery = ConversationDelivery::default();
    delivery.messages.push((*envelope).clone());
    f.m.advance_conversation(CONV, delivery).await;
    match f.admit(uuid, &NamedPart::Body).await {
        Ok(AdmittedPart::Body(body)) => assert_eq!(body.as_text(), Some("pushed")),
        other => panic!("expected the body, got {other:?}"),
    }
}

/// A conversation's own send is nameable by the id its answer carried.
#[tokio::test]
async fn a_send_is_recorded_by_its_id_and_address() {
    let f = fixture().await;
    let uuid = f.publish(&f.durable, "mine", &[]).await;
    f.m.note_conversation_sent(CONV, uuid, &f.durable);
    assert!(matches!(
        f.admit(uuid, &NamedPart::Body).await,
        Ok(AdmittedPart::Body(_))
    ));
}

/// Past [`READ_RECORD_MESSAGES`] the oldest message is dropped; recording
/// one again makes it the newest.
#[test]
fn the_message_cap_drops_the_oldest() {
    let reads = ConversationReads::default();
    let ids: Vec<Uuid> = (0..=READ_RECORD_MESSAGES).map(|_| Uuid::new_v4()).collect();
    reads.record(
        1,
        ids[..READ_RECORD_MESSAGES]
            .iter()
            .map(|id| (*id, "brenn:a")),
    );
    reads.record(1, [(ids[0], "brenn:a")]);
    reads.record(1, [(ids[READ_RECORD_MESSAGES], "brenn:a")]);
    assert_eq!(reads.channel_of(1, ids[0]).as_deref(), Some("brenn:a"));
    assert_eq!(reads.channel_of(1, ids[1]), None, "the oldest went");
    assert!(reads.channel_of(1, ids[2]).is_some());
    assert!(reads.channel_of(1, ids[READ_RECORD_MESSAGES]).is_some());
}

/// A batch that changes channel part-way records each message under its own
/// address, and a run on one channel shares one copy of it.
#[test]
fn a_mixed_batch_records_each_message_under_its_own_channel() {
    let reads = ConversationReads::default();
    let ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    let channels = ["brenn:a", "brenn:a", "brenn:b", "brenn:a"];
    reads.record(1, ids.iter().copied().zip(channels));
    for (id, channel) in ids.iter().zip(channels) {
        assert_eq!(reads.channel_of(1, *id).as_deref(), Some(channel));
    }
    let all = reads.lock();
    let record = &all[&1];
    assert!(Arc::ptr_eq(&record[&ids[0]], &record[&ids[1]]));
    assert!(!Arc::ptr_eq(&record[&ids[1]], &record[&ids[3]]));
}

/// Past [`READ_RECORD_CONVERSATIONS`] the least recently fed conversation is
/// dropped whole.
#[test]
fn the_conversation_cap_drops_the_least_recently_fed() {
    let reads = ConversationReads::default();
    let id = Uuid::new_v4();
    let count = i64::try_from(READ_RECORD_CONVERSATIONS).unwrap();
    for conversation in 0..count {
        reads.record(conversation, [(id, "brenn:a")]);
    }
    reads.record(0, [(Uuid::new_v4(), "brenn:a")]);
    reads.record(count, [(id, "brenn:a")]);
    assert!(reads.channel_of(0, id).is_some(), "fed again, so kept");
    assert_eq!(reads.channel_of(1, id), None, "least recently fed");
    assert!(reads.channel_of(2, id).is_some());
    assert!(reads.channel_of(count, id).is_some());
}

/// An empty read feeds nothing, so it does not refresh a conversation.
#[test]
fn an_empty_read_records_nothing() {
    let reads = ConversationReads::default();
    reads.record(1, std::iter::empty::<(Uuid, &str)>());
    assert!(reads.lock().is_empty());
}

// ── Refusals ────────────────────────────────────────────────────────────

/// A channel the app may not read refuses a message it was handed, and
/// names the channel.
#[tokio::test]
async fn a_channel_the_app_may_not_read_is_refused() {
    let f = fixture().await;
    let uuid = f.publish(&f.hidden, "secret", &[]).await;
    f.m.note_conversation_sent(CONV, uuid, &f.hidden);
    assert_eq!(
        f.admit(uuid, &NamedPart::Body).await.unwrap_err(),
        NameRefusal::ChannelNotReadable {
            channel: f.hidden.clone()
        }
    );
}

/// A channel gone from the directory is not readable.
#[tokio::test]
async fn a_channel_gone_from_the_directory_is_refused() {
    let f = fixture().await;
    let gone = "brenn:blob-gone";
    let uuid = Uuid::new_v4();
    f.m.note_conversation_sent(CONV, uuid, gone);
    assert_eq!(
        f.admit(uuid, &NamedPart::Body).await.unwrap_err(),
        NameRefusal::ChannelNotReadable {
            channel: gone.to_string()
        }
    );
}

/// A name the message does not carry is refused with the names it does.
#[tokio::test]
async fn a_name_the_message_does_not_carry_is_refused_with_what_it_carries() {
    let f = fixture().await;
    for addr in [&f.durable, &f.ring] {
        let photo = seal_blob(&f.m, b"p");
        let thumb = seal_blob(&f.m, b"t");
        let uuid = f
            .publish(addr, "two", &[("photo", &photo), ("thumb", &thumb)])
            .await;
        f.read(addr).await;
        assert_eq!(
            f.admit(uuid, &attachment("scan")).await.unwrap_err(),
            NameRefusal::NoSuchAttachment {
                carried: vec![name("photo"), name("thumb")]
            },
            "{addr}"
        );
    }
}

/// A ring message evicted after the read is not retained.
#[tokio::test]
async fn an_evicted_ring_message_is_refused() {
    let f = fixture().await;
    let photo = seal_blob(&f.m, b"p");
    let uuid = f.publish(&f.ring, "first", &[("photo", &photo)]).await;
    f.read(&f.ring).await;
    for i in 0..RING_DEPTH {
        f.publish(&f.ring, &format!("evicts {i}"), &[]).await;
    }
    assert_eq!(
        f.admit(uuid, &attachment("photo")).await.unwrap_err(),
        NameRefusal::NotRetained {
            channel: f.ring.clone(),
            attachment: true,
        }
    );
}

/// A durable row the reaper has taken is not retained.
#[tokio::test]
async fn an_evicted_durable_row_is_refused() {
    let f = fixture().await;
    let uuid = f.publish(&f.durable, "gone soon", &[]).await;
    f.read(&f.durable).await;
    {
        let conn = f.m.db().lock().await;
        conn.execute(
            "UPDATE messaging_messages SET retained_seq = NULL WHERE uuid = ?1",
            [uuid.as_bytes().to_vec()],
        )
        .unwrap();
    }
    assert_eq!(
        f.admit(uuid, &NamedPart::Body).await.unwrap_err(),
        NameRefusal::NotRetained {
            channel: f.durable.clone(),
            attachment: false,
        }
    );
}

/// A body that is not retained is refused without pointing at another
/// message: only an attachment can be carried by one.
#[test]
fn a_bodys_not_retained_text_names_no_other_message() {
    let refused = |attachment| {
        NameRefusal::NotRetained {
            channel: "brenn:somewhere".to_string(),
            attachment,
        }
        .to_string()
    };
    assert_eq!(
        refused(false),
        "the message is not retained on brenn:somewhere now"
    );
    assert!(refused(true).contains("another message carrying the same attachment"));
}

/// The durable window is the channel's standing depth, whatever the reaper
/// has done: a row inside it but beyond the subscriber depth is admitted, and
/// a row past it, still on disk, is refused.
#[tokio::test]
async fn the_durable_window_is_the_standing_depth() {
    let f = fixture().await;
    let first = f.publish(&f.windowed, "first", &[]).await;
    f.publish(&f.windowed, "second", &[]).await;
    f.m.note_conversation_sent(CONV, first, &f.windowed);
    match f.admit(first, &NamedPart::Body).await {
        Ok(AdmittedPart::Body(body)) => assert_eq!(body.as_text(), Some("first")),
        other => panic!("inside the standing window: {other:?}"),
    }

    f.publish(&f.windowed, "third", &[]).await;
    {
        let conn = f.m.db().lock().await;
        assert!(
            crate::db::retained_message_by_uuid(&conn, first).is_some(),
            "the reaper has not run"
        );
    }
    assert_eq!(
        f.admit(first, &NamedPart::Body).await.unwrap_err(),
        NameRefusal::NotRetained {
            channel: f.windowed.clone(),
            attachment: false,
        }
    );
}

/// A parked message is not retained until it is released, then is admitted
/// under the same id.
#[tokio::test]
async fn a_parked_message_is_refused_until_released() {
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
        f.m.note_conversation_sent(CONV, uuid, addr);
        assert_eq!(
            f.admit(uuid, &attachment("photo")).await.unwrap_err(),
            NameRefusal::NotRetained {
                channel: addr.clone(),
                attachment: true,
            },
            "{addr}"
        );
        f.release_all(addr).await;
        assert_eq!(
            f.admit_bytes(uuid, &attachment("photo")).await,
            b"later bytes"
        );
    }
}

/// Both conditions are asked of one message: an evicted message's
/// attachment is refused while another retained message names the same
/// blob, and admitted under that message once the conversation reads it.
#[tokio::test]
async fn an_evicted_messages_blob_is_admitted_only_under_a_message_read() {
    let f = fixture().await;
    let shared = seal_blob(&f.m, b"shared");
    let first = f.publish(&f.ring, "first", &[("photo", &shared)]).await;
    f.read(&f.ring).await;
    f.publish(&f.ring, "filler", &[]).await;
    let second = f.publish(&f.ring, "again", &[("photo", &shared)]).await;
    drop(shared);

    assert_eq!(
        f.admit(first, &attachment("photo")).await.unwrap_err(),
        NameRefusal::NotRetained {
            channel: f.ring.clone(),
            attachment: true,
        }
    );
    assert_eq!(
        f.admit(second, &attachment("photo")).await.unwrap_err(),
        NameRefusal::NotRead
    );
    f.read(&f.ring).await;
    assert_eq!(f.admit_bytes(second, &attachment("photo")).await, b"shared");
}

/// A body is admitted whatever its kind, from either store.
#[tokio::test]
async fn a_body_is_admitted_from_either_store() {
    let f = fixture().await;
    for addr in [&f.durable, &f.ring] {
        let uuid = f.publish(addr, "the body", &[]).await;
        f.read(addr).await;
        match f.admit(uuid, &NamedPart::Body).await {
            Ok(AdmittedPart::Body(body)) => assert_eq!(body.as_text(), Some("the body")),
            other => panic!("{addr}: expected the body, got {other:?}"),
        }
    }
}

/// An admitted hold keeps the bytes after the message that named them is
/// evicted, on both stores.
#[tokio::test]
async fn an_admitted_hold_outlives_the_messages_eviction() {
    let f = fixture().await;
    let photo = seal_blob(&f.m, b"kept bytes");
    let uuid = f.publish(&f.ring, "ring", &[("photo", &photo)]).await;
    drop(photo);
    f.read(&f.ring).await;
    let Ok(AdmittedPart::Attachment(hold)) = f.admit(uuid, &attachment("photo")).await else {
        panic!("the ring message is retained and read");
    };
    for i in 0..RING_DEPTH {
        f.publish(&f.ring, &format!("evicts {i}"), &[]).await;
    }
    assert_eq!(hold.read(0, 10).unwrap(), b"kept bytes");

    let scan = seal_blob(&f.m, b"durable bytes");
    let uuid = f.publish(&f.durable, "durable", &[("scan", &scan)]).await;
    drop(scan);
    f.read(&f.durable).await;
    let Ok(AdmittedPart::Attachment(durable_hold)) = f.admit(uuid, &attachment("scan")).await
    else {
        panic!("the durable message is retained and read");
    };
    {
        let conn = f.m.db().lock().await;
        conn.execute(
            "DELETE FROM messaging_messages WHERE uuid = ?1",
            [uuid.as_bytes().to_vec()],
        )
        .unwrap();
    }
    f.m.reap_blobs().await;
    assert_eq!(durable_hold.read(0, 13).unwrap(), b"durable bytes");
}

/// A forward names the same blob on both stores, immediately and parked,
/// and makes no new one.
#[tokio::test]
async fn publish_with_attachments_names_the_same_blob() {
    let f = fixture().await;
    let photo = seal_blob(&f.m, b"forwarded");
    let source = f.publish(&f.durable, "source", &[("photo", &photo)]).await;
    let photo_id = photo.id().clone();
    drop(photo);
    f.read(&f.durable).await;
    let before = f.blob_count();

    for addr in [&f.durable, &f.ring] {
        for deliver_after in [None, Some(Utc::now() + Duration::hours(1))] {
            let Ok(AdmittedPart::Attachment(hold)) = f.admit(source, &attachment("photo")).await
            else {
                panic!("the source is retained and read");
            };
            let held = HeldAttachments::new([(name("copy"), hold)]).unwrap();
            let result =
                f.m.publish_with_attachments(
                    PublishOrigin::Conversation { id: CONV },
                    APP,
                    addr,
                    "forward".into(),
                    Urgency::Normal,
                    None,
                    deliver_after,
                    None,
                    held,
                )
                .await;
            let PublishResult::Ok { message_id, .. } = result else {
                panic!("{addr}: the forward is published: {result:?}");
            };
            if deliver_after.is_some() {
                f.release_all(addr).await;
            }
            f.m.note_conversation_sent(CONV, message_id, addr);
            let Ok(AdmittedPart::Attachment(forwarded)) =
                f.admit(message_id, &attachment("copy")).await
            else {
                panic!("{addr}: the forward is retained and recorded");
            };
            assert_eq!(forwarded.id(), &photo_id, "{addr}: the same blob");
            assert_eq!(forwarded.read(0, 9).unwrap(), b"forwarded");
            assert_eq!(f.blob_count(), before, "{addr}: no blob was made");
        }
    }
}

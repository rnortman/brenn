//! Blob bytes over the remote route, walked by the same attacher.
//!
//! Every case here moves bytes through `brenn-attach-client`'s blob plane —
//! its chunking, its correlation, its read window — against the real server
//! on a messenger with a blob store, and asserts on what the attacher was
//! answered and on what the store holds. A frame the plane will not send goes
//! out through `send_raw`, and its answer comes back raw.

use std::sync::Arc;
use std::time::Duration;

use brenn_attach_conformance::{
    AttachmentName, BlobHandle, OpenRefusal, OpenedBlob, PublishAttachment, ReadRefusal,
    UploadRefusal,
};
use brenn_attach_proto::{
    BlobCreateOutcome, BlobOpenOutcome, BlobReadOutcome, ClientFrame, ServerFrame,
};
use brenn_messaging::Messenger;
use brenn_messaging::testutils::{
    TEST_BLOB_LIMITS, TempBlobDir, covering_system_publisher, publish_attached, seal_blob,
};
use brenn_messaging_store::blob::{BlobLimits, BlobStore, BlobUsage};
use uuid::Uuid;

use super::super::test_fixtures::remote_harness_extending_messenger;
use super::*;
use crate::messaging_router::WakeRouterImpl;

mod interop;

/// The host-side publisher every hosted blob is published under.
const HOST: &str = "camera-host";

/// A ring channel short enough that two messages evict the oldest.
const STREAM_8_NAME: &str = "chat.app.home.stream.8";
const STREAM_8: &str = "ephemeral:chat.app.home.stream.8";

/// A tool-substrate channel the rig's remote may subscribe to.
const TOOLS_BARE: &str = "tool-requests/scan";
const TOOLS: &str = "brenn:tool-requests/scan";

/// Over two chunk bounds: three `BlobWrite`s and three `BlobRead`s.
const SEVERAL_CHUNKS: usize = 600_000;

/// A raw frame's handle or correlation, far above any the plane mints.
const RAW: u64 = 1_000_000;

/// The fleet's channels plus `stream.8` and the tool channel.
fn blob_channels() -> Vec<ChannelEntry> {
    let mut channels = fleet_channels();
    channels.push(ephemeral_channel_entry(STREAM_8_NAME, 2));
    channels.push(test_channel_entry(TOOLS_BARE, vec![]));
    channels
}

/// [`loopback`] that may also subscribe to the tool channels.
fn loopback_with_tools(token_file: &Path) -> RemoteConfigRaw {
    let mut raw = loopback(token_file);
    raw.subscribe_acl
        .push(brenn_lib::config::remote_prefix_ceiling(
            "tool-requests/",
            8,
            8,
        ));
    raw
}

/// A remote that may subscribe as [`loopback`] does and publish nowhere.
fn subscribe_only(token_file: &Path) -> RemoteConfigRaw {
    RemoteConfigRaw {
        grants: vec![AttachGrant::Subscribe, AttachGrant::EphemeralSubscribe],
        publish_acl: vec![],
        ephemeral_publish_acl: vec![],
        ..loopback(token_file)
    }
}

/// A rig whose messenger has a blob store of `limits` and a host publisher.
struct BlobRig {
    rig: Rig,
    messenger: Arc<Messenger>,
    _dir: TempBlobDir,
}

async fn blob_rig(
    db: &brenn_db::Db,
    block: impl FnOnce(&Path) -> RemoteConfigRaw,
    versions: VersionRange,
    limits: BlobLimits,
) -> BlobRig {
    blob_rig_over(db, block, versions, limits, blob_channels(), |m, _| m).await
}

/// [`blob_rig`] over `channels`, with `extend` applied to the messenger after
/// the store and the host publisher.
async fn blob_rig_over(
    db: &brenn_db::Db,
    block: impl FnOnce(&Path) -> RemoteConfigRaw,
    versions: VersionRange,
    limits: BlobLimits,
    channels: Vec<ChannelEntry>,
    extend: impl FnOnce(Arc<Messenger>, &WakeRouterImpl) -> Arc<Messenger>,
) -> BlobRig {
    let dir = TempBlobDir::new();
    let store = {
        let conn = db.lock().await;
        BlobStore::open(dir.path(), limits, &conn)
    };
    let harness = remote_harness_extending_messenger(db, block, channels, move |m, router| {
        extend(
            m.with_blob_store(store)
                .with_subscriber_registrations(covering_system_publisher(HOST)),
            router,
        )
    })
    .await;
    let messenger = Arc::clone(harness.state.messenger.as_ref().expect("a messenger"));
    let (base, server) = spawn_test_server(harness.state.clone()).await;
    let relay = SeverableRelay::spawn(http_base_addr(&base)).await;
    let client = client_through(&relay, versions);
    BlobRig {
        rig: Rig {
            client,
            relay,
            harness,
            _server: server,
        },
        messenger,
        _dir: dir,
    }
}

impl BlobRig {
    /// A message on `channel` from the host carrying `bytes` as `photo`. The
    /// host keeps no hold: the message is the blob's only holder.
    async fn hosted(&self, channel: &str, bytes: &[u8]) -> Uuid {
        let hold = seal_blob(&self.messenger, bytes);
        publish_attached(
            &self.messenger,
            HOST,
            channel,
            "hosted",
            &[("photo", &hold)],
            None,
        )
        .await
    }

    /// A text message on `channel` from the host, to roll a window.
    async fn filler(&self, channel: &str) {
        publish_attached(&self.messenger, HOST, channel, "filler", &[], None).await;
    }

    fn usage(&self) -> BlobUsage {
        self.messenger.blob_store().expect("a blob store").usage()
    }

    /// Wait for the store to reach `want`: a session's table is dropped by its
    /// teardown, which runs after the socket closes and signals nothing.
    async fn until_usage(&self, want: BlobUsage) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while self.usage() != want {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the store holds {:?}, never {want:?}",
                self.usage()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

fn bytes_of(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn name(part: &str) -> AttachmentName {
    AttachmentName::new(part).expect("a valid attachment name")
}

/// One publish naming `handle` as `part`.
fn attaching(channel: &str, part: &str, handle: BlobHandle) -> PublishRequest {
    publish(channel, "attached").attachments(vec![PublishAttachment::new(name(part), handle)])
}

fn usage(sealed: usize, charged_bytes: u64) -> BlobUsage {
    BlobUsage {
        sealed,
        unsealed: 0,
        charged_bytes,
    }
}

/// The detach a violation earns, past whatever the planes failed with it.
async fn expect_blob_detach(client: &mut AttachClient) {
    loop {
        match client.next_observation().await {
            Observation::Detached { .. } => return,
            Observation::PublishLost { .. }
            | Observation::OpenRefused {
                reason: OpenRefusal::Detached,
                ..
            }
            | Observation::UploadRefused {
                reason: UploadRefusal::Detached,
                ..
            }
            | Observation::ReadRefused {
                reason: ReadRefusal::Detached,
                ..
            } => continue,
            other => panic!("expected the attachment to be detached, got {other:?}"),
        }
    }
}

/// **A host's blob, read whole and by range, on a ring and a durable channel.**
#[tokio::test]
async fn a_hosted_blob_of_several_chunks_is_read_whole_and_by_range_on_both_channel_kinds() {
    let db = crate::test_support::init_db_memory();
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), TEST_BLOB_LIMITS).await;
    let facts = b.rig.client.attach().await;
    let door = facts
        .blobs()
        .expect("a v6 remote with a store has the door");
    assert!(door.upload, "the remote may publish, so it may upload");

    for (channel, seed) in [(STREAM_7, 1), (OUT_7, 2)] {
        let data = bytes_of(SEVERAL_CHUNKS, seed);
        let message_id = b.hosted(channel, &data).await;
        let opened = b
            .rig
            .client
            .open(channel, message_id, "photo")
            .await
            .unwrap_or_else(|r| panic!("{channel}: open refused {r:?}"));
        assert_eq!(opened.size, SEVERAL_CHUNKS as u64);
        assert_eq!(opened.content_type, "application/octet-stream");
        assert_eq!(
            b.rig.client.read(opened.handle, 0, opened.size).await,
            Ok(data.clone()),
            "{channel}: the whole blob, across three chunks"
        );
        assert_eq!(
            b.rig.client.read(opened.handle, 262_000, 10_000).await,
            Ok(data[262_000..272_000].to_vec()),
            "{channel}: a range across a chunk boundary"
        );

        b.rig
            .client
            .send_raw(ClientFrame::BlobRead {
                correlation: RAW,
                handle: opened.handle,
                offset: opened.size,
                len: 1,
            })
            .await;
        assert_eq!(
            b.rig.client.next_raw_answer().await,
            ServerFrame::BlobReadResult {
                correlation: RAW,
                outcome: BlobReadOutcome::OutOfRange { size: opened.size },
            },
            "{channel}: a range past the size is refused, never short"
        );
        b.rig.client.close_blob(opened.handle).await;
    }
    assert!(b.rig.client.is_attached());
    assert!(b.rig.harness.captured().await.is_empty());
    b.rig.client.close().await;
}

/// **Uploaded by one session, read by another.**
#[tokio::test]
async fn an_upload_one_session_publishes_another_session_reads_byte_for_byte() {
    let db = crate::test_support::init_db_memory();
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), TEST_BLOB_LIMITS).await;
    let mut reader = b.rig.sibling(VersionRange::exactly(6));
    b.rig.client.attach().await;
    reader.attach().await;
    reader
        .subscribe(STREAM_7, DEPTHS, ResumePolicy::Cursorless)
        .await;

    let data = bytes_of(SEVERAL_CHUNKS, 7);
    let handle = b
        .rig
        .client
        .upload("image/jpeg", &data)
        .await
        .expect("the upload seals");
    assert_eq!(
        b.rig
            .client
            .publish(attaching(STREAM_7, "frame", handle))
            .await,
        PublishOutcome::Ok
    );
    b.rig.client.close_blob(handle).await;

    let delivery = reader.next_delivery(STREAM_7).await;
    let reference = delivery
        .attachments
        .get(&name("frame"))
        .expect("the delivery names the attachment");
    assert_eq!(reference.size, SEVERAL_CHUNKS as u64);
    assert_eq!(reference.content_type, "image/jpeg");
    let opened = reader
        .open(STREAM_7, delivery.message_id, "frame")
        .await
        .expect("the reader opens what it was delivered");
    assert_eq!(reader.read(opened.handle, 0, opened.size).await, Ok(data));
    reader.close_blob(opened.handle).await;

    reader.close().await;
    b.rig.client.close().await;
}

/// **Forwarding by reference copies no byte, in both forms.**
///
/// Opened off a message on a short ring, published onto a second channel by
/// handle, and closed — once after the publish's answer, once back to back.
/// The store holds one blob that two messages name; the first message's
/// eviction leaves it readable through the second.
#[tokio::test]
async fn a_forward_by_handle_names_one_blob_from_two_messages_in_both_forms() {
    let db = crate::test_support::init_db_memory();
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), TEST_BLOB_LIMITS).await;
    b.rig.client.attach().await;
    b.rig
        .client
        .subscribe(STREAM_7, DEPTHS, ResumePolicy::Cursorless)
        .await;

    for (round, pipelined) in [(0u8, false), (1, true)] {
        let data = bytes_of(1000, round);
        let before = b.usage().sealed;
        let hold = seal_blob(&b.messenger, &data);
        let blob = hold.id().clone();
        let source = publish_attached(
            &b.messenger,
            HOST,
            STREAM_8,
            "hosted",
            &[("photo", &hold)],
            None,
        )
        .await;
        drop(hold);

        let handle = if pipelined {
            let handle = b.rig.client.send_open(STREAM_8, source, "photo").await;
            let correlation = b
                .rig
                .client
                .send_publish(attaching(STREAM_7, "fwd", handle))
                .await;
            b.rig.client.close_blob(handle).await;
            let opened = b.rig.client.next_open_outcome(handle).await;
            assert_eq!(
                opened.map(|o| o.size),
                Ok(1000),
                "the plane reports the open after the close"
            );
            assert_eq!(
                b.rig.client.next_publish_outcome(correlation).await,
                PublishOutcome::Ok
            );
            handle
        } else {
            let OpenedBlob { handle, .. } = b
                .rig
                .client
                .open(STREAM_8, source, "photo")
                .await
                .expect("open");
            assert_eq!(
                b.rig
                    .client
                    .publish(attaching(STREAM_7, "fwd", handle))
                    .await,
                PublishOutcome::Ok
            );
            b.rig.client.close_blob(handle).await;
            handle
        };
        let forwarded = b.rig.client.next_delivery(STREAM_7).await;
        assert_eq!(
            forwarded
                .attachments
                .get(&name("fwd"))
                .map(|r| r.id.clone()),
            Some(blob.clone()),
            "the second message names the same blob: no byte was copied (handle {handle:?})"
        );
        assert_eq!(
            b.usage().sealed,
            before + 1,
            "the store holds one blob more than before the round"
        );

        b.filler(STREAM_8).await;
        b.filler(STREAM_8).await;
        assert_eq!(
            b.rig.client.open(STREAM_8, source, "photo").await,
            Err(OpenRefusal::NotRetained),
            "the first message is out of its window"
        );
        let opened = b
            .rig
            .client
            .open(STREAM_7, forwarded.message_id, "fwd")
            .await
            .expect("still readable through the second message");
        assert_eq!(b.rig.client.read(opened.handle, 0, 1000).await, Ok(data));
        b.rig.client.close_blob(opened.handle).await;
        assert!(b.rig.client.is_attached());
    }
    b.rig.client.close().await;
}

/// **A whole upload pipelined, and the same sequence refused at the pool.**
#[tokio::test]
async fn a_pipelined_upload_publishes_and_a_refused_one_is_answered_not_violated() {
    let db = crate::test_support::init_db_memory();
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), TEST_BLOB_LIMITS).await;
    let mut reader = b.rig.sibling(VersionRange::exactly(6));
    b.rig.client.attach().await;
    reader.attach().await;
    reader
        .subscribe(STREAM_7, DEPTHS, ResumePolicy::Cursorless)
        .await;

    let data = bytes_of(SEVERAL_CHUNKS, 3);
    let handle = b.rig.client.send_upload("image/jpeg", &data).await;
    let correlation = b
        .rig
        .client
        .send_publish(attaching(STREAM_7, "frame", handle))
        .await;
    b.rig.client.close_blob(handle).await;
    assert_eq!(
        b.rig.client.next_publish_outcome(correlation).await,
        PublishOutcome::Ok
    );
    assert_eq!(
        b.rig.client.next_upload_outcome(handle).await,
        Ok(()),
        "the plane reports the seal after the close"
    );
    let delivery = reader.next_delivery(STREAM_7).await;
    let opened = reader
        .open(STREAM_7, delivery.message_id, "frame")
        .await
        .expect("open");
    assert_eq!(reader.read(opened.handle, 0, opened.size).await, Ok(data));
    assert!(b.rig.client.is_attached());
    reader.close().await;
    b.rig.client.close().await;

    let db = crate::test_support::init_db_memory();
    let small_pool = BlobLimits {
        max_blob_bytes: 1 << 20,
        budget_bytes: 1000,
    };
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), small_pool).await;
    b.rig.client.attach().await;
    let handle = b
        .rig
        .client
        .send_upload("image/jpeg", &bytes_of(SEVERAL_CHUNKS, 4))
        .await;
    let correlation = b
        .rig
        .client
        .send_publish(attaching(STREAM_7, "frame", handle))
        .await;
    b.rig.client.close_blob(handle).await;
    assert_eq!(
        b.rig.client.next_upload_outcome(handle).await,
        Err(UploadRefusal::BudgetExhausted)
    );
    assert_eq!(
        b.rig.client.next_publish_outcome(correlation).await,
        PublishOutcome::AttachmentNotHeld
    );
    assert!(b.rig.client.is_attached(), "every refusal was an answer");
    assert!(b.rig.harness.captured().await.is_empty());
    assert_eq!(b.usage(), usage(0, 0));
    b.rig.client.close().await;
}

/// **A flush naming an upload the pool refused is lost, and closing on its
/// release is absorbed.**
///
/// The handle is named in a registrant's flush behind a pipelined upload and
/// is not closed before the answers.
#[tokio::test]
async fn a_flush_naming_a_refused_upload_is_lost_and_the_close_on_release_sends_nothing() {
    let db = crate::test_support::init_db_memory();
    let small_pool = BlobLimits {
        max_blob_bytes: 1 << 20,
        budget_bytes: 1000,
    };
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), small_pool).await;
    b.rig.client.attach().await;
    b.rig.client.register_outbox("cam", None, 4);
    let handle = b
        .rig
        .client
        .send_upload("image/jpeg", &bytes_of(SEVERAL_CHUNKS, 5))
        .await;
    b.rig
        .client
        .flush(
            "cam",
            FlushBatch {
                entries: vec![
                    BatchEntry::new(STREAM_7, "frame".into(), Urgency::Normal)
                        .attachments(vec![PublishAttachment::new(name("frame"), handle)]),
                ],
                ops: Vec::new(),
            },
        )
        .await;
    assert_eq!(
        b.rig.client.next_upload_outcome(handle).await,
        Err(UploadRefusal::BudgetExhausted)
    );
    assert_eq!(
        b.rig.client.next_batch_outcome().await,
        PublishBatchOutcome::AttachmentNotHeld
    );
    assert_eq!(b.rig.client.next_released().await, vec![handle]);
    assert!(
        !b.rig.client.close_blob(handle).await,
        "the plane retired the handle at the refusal, so the close sends nothing"
    );
    assert!(b.rig.client.is_attached(), "every refusal was an answer");
    assert!(b.rig.harness.captured().await.is_empty());
    assert_eq!(b.usage(), usage(0, 0));
    b.rig.client.close().await;
}

/// **A queued flush keeps its handle until the outbox releases it.**
#[tokio::test]
async fn a_flush_naming_a_handle_applies_when_closed_on_release_and_is_lost_when_closed_early() {
    let db = crate::test_support::init_db_memory();
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), TEST_BLOB_LIMITS).await;
    b.rig.client.attach().await;
    b.rig
        .client
        .subscribe(STREAM_7, DEPTHS, ResumePolicy::Cursorless)
        .await;
    b.rig.client.register_outbox("cam", None, 4);

    let text_flush = || FlushBatch {
        entries: vec![BatchEntry::new(STREAM_7, "ahead".into(), Urgency::Normal)],
        ops: Vec::new(),
    };
    let blob_flush = |handle| FlushBatch {
        entries: vec![
            BatchEntry::new(STREAM_7, "frame".into(), Urgency::Normal)
                .attachments(vec![PublishAttachment::new(name("frame"), handle)]),
        ],
        ops: Vec::new(),
    };

    let kept = b
        .rig
        .client
        .upload("image/jpeg", b"kept")
        .await
        .expect("seal");
    b.rig.client.flush("cam", text_flush()).await;
    b.rig.client.flush("cam", blob_flush(kept)).await;
    assert_eq!(
        b.rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok
    );
    assert_eq!(
        b.rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok
    );
    assert_eq!(b.rig.client.next_released().await, vec![kept]);
    assert!(
        b.rig.client.close_blob(kept).await,
        "a held handle is closed"
    );
    assert_eq!(b.rig.client.next_delivery(STREAM_7).await.text(), "ahead");
    let applied = b.rig.client.next_delivery(STREAM_7).await;
    assert!(applied.attachments.get(&name("frame")).is_some());

    let early = b
        .rig
        .client
        .upload("image/jpeg", b"early")
        .await
        .expect("seal");
    b.rig.client.flush("cam", text_flush()).await;
    b.rig.client.flush("cam", blob_flush(early)).await;
    b.rig.client.close_blob(early).await;
    assert_eq!(
        b.rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok
    );
    assert_eq!(
        b.rig.client.next_batch_outcome().await,
        PublishBatchOutcome::AttachmentNotHeld,
        "the parked flush reached the peer after the close"
    );
    assert_eq!(b.rig.client.next_released().await, vec![early]);
    assert!(
        !b.rig.client.close_blob(early).await,
        "closing on the second release sends nothing for a handle already closed"
    );
    assert!(b.rig.client.is_attached());
    assert!(b.rig.harness.captured().await.is_empty());
    b.rig.client.close().await;
}

/// **A batch with blobs on a durable and a ring entry applies whole, or not at
/// all.**
#[tokio::test]
async fn a_batch_carrying_blobs_on_both_channel_kinds_applies_whole_or_nothing() {
    let db = crate::test_support::init_db_memory();
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), TEST_BLOB_LIMITS).await;
    b.rig.client.attach().await;
    b.rig
        .client
        .subscribe(STREAM_7, DEPTHS, ResumePolicy::Cursorless)
        .await;
    b.rig
        .client
        .subscribe(OUT_7, DEPTHS, ResumePolicy::Cursorless)
        .await;
    b.rig.client.register_outbox("pair", None, 4);

    let durable = b
        .rig
        .client
        .upload("image/jpeg", b"durable")
        .await
        .expect("seal");
    let ring = b
        .rig
        .client
        .upload("image/jpeg", b"ring")
        .await
        .expect("seal");
    let pair = |durable, ring| FlushBatch {
        entries: vec![
            BatchEntry::new(OUT_7, "d".into(), Urgency::Normal)
                .attachments(vec![PublishAttachment::new(name("a"), durable)]),
            BatchEntry::new(STREAM_7, "r".into(), Urgency::Normal)
                .attachments(vec![PublishAttachment::new(name("b"), ring)]),
        ],
        ops: Vec::new(),
    };
    b.rig.client.flush("pair", pair(durable, ring)).await;
    assert_eq!(
        b.rig.client.next_batch_outcome().await,
        PublishBatchOutcome::Ok
    );
    let on_out = b.rig.client.next_delivery(OUT_7).await;
    let on_stream = b.rig.client.next_delivery(STREAM_7).await;
    assert_eq!(on_out.attachments.get(&name("a")).map(|r| r.size), Some(7));
    assert_eq!(
        on_stream.attachments.get(&name("b")).map(|r| r.size),
        Some(4)
    );

    b.rig.client.close_blob(ring).await;
    b.rig.client.flush("pair", pair(durable, ring)).await;
    assert_eq!(
        b.rig.client.next_batch_outcome().await,
        PublishBatchOutcome::AttachmentNotHeld
    );
    assert_eq!(
        brenn_messaging::testutils::retained_on(&b.messenger, OUT_7)
            .await
            .len(),
        1,
        "the durable entry beside the unheld one was not applied"
    );
    b.rig.client.close().await;
}

/// **What a session leaves in the store, read off the store.**
///
/// An upload sealed and never published and one never sealed are gone once
/// the session ends; one published outlives the session and goes with its
/// message.
#[tokio::test]
async fn a_session_leaves_only_what_it_published_and_that_goes_with_its_message() {
    let db = crate::test_support::init_db_memory();
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), TEST_BLOB_LIMITS).await;
    b.rig.client.attach().await;

    let published = b
        .rig
        .client
        .upload("image/jpeg", b"published")
        .await
        .expect("seal");
    assert_eq!(
        b.rig
            .client
            .publish(attaching(STREAM_8, "frame", published))
            .await,
        PublishOutcome::Ok
    );
    b.rig
        .client
        .upload("image/jpeg", b"sealed")
        .await
        .expect("seal");
    b.rig
        .client
        .send_raw(ClientFrame::BlobCreate {
            handle: BlobHandle(RAW),
            content_type: "image/jpeg".to_string(),
            size: 10,
        })
        .await;
    b.rig
        .client
        .send_raw(ClientFrame::BlobWrite {
            handle: BlobHandle(RAW),
            offset: 0,
            bytes: b"half".to_vec(),
        })
        .await;
    assert_eq!(
        b.rig.client.next_raw_answer().await,
        ServerFrame::BlobCreateResult {
            handle: BlobHandle(RAW),
            outcome: BlobCreateOutcome::Ok,
        }
    );
    assert_eq!(
        b.usage(),
        BlobUsage {
            sealed: 2,
            unsealed: 1,
            charged_bytes: 9 + 6 + 10,
        }
    );

    b.rig.client.close().await;
    b.until_usage(usage(1, 9)).await;

    b.filler(STREAM_8).await;
    b.filler(STREAM_8).await;
    assert_eq!(
        b.usage(),
        usage(0, 0),
        "a blob only a ring message named goes with the message"
    );
}

/// **The five cases of an attachment the remote may not have.**
#[tokio::test]
async fn opens_and_publishes_a_remote_may_not_make_are_answered_or_violations_as_ruled() {
    let db = crate::test_support::init_db_memory();
    let mut b = blob_rig(
        &db,
        loopback_with_tools,
        VersionRange::exactly(6),
        TEST_BLOB_LIMITS,
    )
    .await;
    b.rig.client.attach().await;

    let on_stream = b.hosted(STREAM_7, b"photo").await;
    assert_eq!(
        b.rig.client.open(STREAM_7, Uuid::new_v4(), "photo").await,
        Err(OpenRefusal::NotRetained)
    );
    let on_tools = b.hosted(TOOLS, b"tool").await;
    assert_eq!(
        b.rig.client.open(TOOLS, on_tools, "photo").await,
        Err(OpenRefusal::ToolChannel)
    );
    assert_eq!(
        b.rig.client.open(STREAM_7, on_stream, "other").await,
        Err(OpenRefusal::NoSuchAttachment)
    );
    assert!(b.rig.client.is_attached());
    assert!(
        b.rig.harness.captured().await.is_empty(),
        "an answered open is not fail2ban signal"
    );

    let held = b
        .rig
        .client
        .open(STREAM_7, on_stream, "photo")
        .await
        .expect("open");
    b.rig
        .client
        .send_publish(attaching(ROSTER, "photo", held.handle))
        .await;
    expect_blob_detach(&mut b.rig.client).await;
    expect_one_violation(
        &b.rig,
        "a publish with attachments onto an unpublishable channel",
    )
    .await;

    let mut stranger = b.rig.sibling(VersionRange::exactly(6));
    stranger.attach().await;
    stranger
        .send_open("brenn:chat.app.other.out.1", on_stream, "photo")
        .await;
    expect_blob_detach(&mut stranger).await;
    let events = b.rig.harness.captured().await;
    assert_eq!(
        events.len(),
        2,
        "the open outside subscribe authority is one more violation, got {events:?}"
    );
    assert!(events[1].0.contains("attach_protocol_violation"));
}

/// **Every advertised bound, refused with its answer.**
#[tokio::test]
async fn each_advertised_bound_is_refused_with_its_answer() {
    let db = crate::test_support::init_db_memory();
    let limits = BlobLimits {
        max_blob_bytes: 1000,
        budget_bytes: 1 << 20,
    };
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(6), limits).await;
    let facts = b.rig.client.attach().await;
    let door = *facts.blobs().expect("a door");
    assert_eq!(door.max_bytes_held, 4000);

    b.rig
        .client
        .send_raw(ClientFrame::BlobCreate {
            handle: BlobHandle(RAW),
            content_type: "image/jpeg".to_string(),
            size: 1001,
        })
        .await;
    assert_eq!(
        b.rig.client.next_raw_answer().await,
        ServerFrame::BlobCreateResult {
            handle: BlobHandle(RAW),
            outcome: BlobCreateOutcome::BlobTooLarge {
                size: 1001,
                max: 1000,
            },
        }
    );

    let mut uploads = Vec::new();
    for seed in 0..4 {
        uploads.push(
            b.rig
                .client
                .upload("image/jpeg", &bytes_of(1000, seed))
                .await
                .expect("four blobs fill the bound exactly"),
        );
    }
    b.rig
        .client
        .send_raw(ClientFrame::BlobCreate {
            handle: BlobHandle(RAW + 1),
            content_type: "image/jpeg".to_string(),
            size: 1,
        })
        .await;
    assert_eq!(
        b.rig.client.next_raw_answer().await,
        ServerFrame::BlobCreateResult {
            handle: BlobHandle(RAW + 1),
            outcome: BlobCreateOutcome::BytesHeldLimit,
        }
    );
    for handle in uploads {
        b.rig.client.close_blob(handle).await;
    }

    let tiny = b.hosted(STREAM_7, b"t").await;
    let mut handles = Vec::new();
    for _ in 0..door.max_handles {
        handles.push(
            b.rig
                .client
                .open(STREAM_7, tiny, "photo")
                .await
                .expect("an open under the handle bound")
                .handle,
        );
    }
    b.rig
        .client
        .send_raw(ClientFrame::BlobOpen {
            handle: BlobHandle(RAW + 2),
            channel: STREAM_7.to_string(),
            message_id: tiny,
            name: name("photo"),
        })
        .await;
    assert_eq!(
        b.rig.client.next_raw_answer().await,
        ServerFrame::BlobOpenResult {
            handle: BlobHandle(RAW + 2),
            outcome: BlobOpenOutcome::HandleLimit,
        },
        "the 65th handle"
    );
    for handle in handles {
        b.rig.client.close_blob(handle).await;
    }
    assert!(b.rig.client.is_attached());
    assert!(b.rig.harness.captured().await.is_empty());

    b.rig
        .client
        .send_raw(ClientFrame::BlobWrite {
            handle: BlobHandle(RAW + 3),
            offset: 0,
            bytes: vec![0; 300_000],
        })
        .await;
    expect_blob_detach(&mut b.rig.client).await;
    expect_one_violation(&b.rig, "a transfer frame over the v6 cap").await;
}

/// **Opens count against the bytes-held bound, for a remote that cannot
/// upload.**
#[tokio::test]
async fn a_subscribe_only_remotes_opens_are_refused_past_the_bytes_held_bound() {
    let db = crate::test_support::init_db_memory();
    let limits = BlobLimits {
        max_blob_bytes: 1000,
        budget_bytes: 1 << 20,
    };
    let mut b = blob_rig(&db, subscribe_only, VersionRange::exactly(6), limits).await;
    let facts = b.rig.client.attach().await;
    assert!(
        !facts.blobs().expect("a door").upload,
        "a remote that may publish nowhere may not upload"
    );

    let mut messages = Vec::new();
    for seed in 0..5 {
        messages.push(b.hosted(STREAM_7, &bytes_of(900, seed)).await);
    }
    let mut held = Vec::new();
    for message_id in &messages[..4] {
        held.push(
            b.rig
                .client
                .open(STREAM_7, *message_id, "photo")
                .await
                .expect("3,600 bytes held is under 4,000"),
        );
    }
    assert_eq!(
        b.rig.client.open(STREAM_7, messages[4], "photo").await,
        Err(OpenRefusal::BytesHeldLimit { size: 900 })
    );
    b.rig.client.close_blob(held[0].handle).await;
    let reopened = b
        .rig
        .client
        .open(STREAM_7, messages[4], "photo")
        .await
        .expect("a close frees the bytes for the next open");
    assert_eq!(
        b.rig.client.read(reopened.handle, 0, 900).await,
        Ok(bytes_of(900, 4))
    );
    assert!(b.rig.client.is_attached());
    assert!(b.rig.harness.captured().await.is_empty());
    b.rig.client.close().await;
}

/// **A v5 session on the same host is shown references and no door.**
#[tokio::test]
async fn a_v5_session_is_delivered_references_and_has_no_blob_door() {
    let db = crate::test_support::init_db_memory();
    let mut b = blob_rig(&db, loopback, VersionRange::exactly(5), TEST_BLOB_LIMITS).await;
    let facts = b.rig.client.attach().await;
    assert_eq!(facts.version, 5);
    assert_eq!(facts.blobs(), None, "v5 carries no blob transfer");
    b.rig
        .client
        .subscribe(STREAM_7, DEPTHS, ResumePolicy::Cursorless)
        .await;
    b.hosted(STREAM_7, b"seen").await;
    let delivery = b.rig.client.next_delivery(STREAM_7).await;
    assert_eq!(
        delivery.attachments.get(&name("photo")).map(|r| r.size),
        Some(4),
        "the reference arrives as metadata"
    );
    b.rig.client.close().await;
}

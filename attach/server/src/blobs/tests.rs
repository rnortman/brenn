use std::collections::{HashMap, HashSet};

use brenn_attach_proto::{
    BatchDeferredOp, BatchEntry, DeferredOpKind, MessageBody, PublishBatchOutcome, PublishOutcome,
    Urgency,
};
use brenn_envelope::BlobId;
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::access::acl::ChannelMatcher;
use brenn_lib::messaging::config::{
    AttachSendBudget, ChannelConfigRaw, Depth, MessagingGlobalConfig, build_channel_entries,
};
use brenn_lib::messaging::{AttachScope, MessagingDirectory, ParticipantId};
use brenn_lib::token_bucket::TokenBucket;
use brenn_messaging::query::NoopWakeRouter;
use brenn_messaging::testutils::{
    TEST_BLOB_LIMITS, TempBlobDir, covering_system_publisher, ephemeral_channel_entry,
    install_blob_store_with, publish_attached, surface_registrations,
};
use brenn_messaging::{WakeRouter, attach_principal_budgets};
use brenn_messaging_store::blob::BlobLimits;
use chrono::Utc;

use super::*;
use crate::profile::SubscriptionFacts;
use crate::publish::{PublishBatchRequest, PublishRequest, handle_publish, handle_publish_batch};
use crate::session::Outbound;
use crate::socket::welcome;
use crate::test_support::{AttachCtxBuilder, FrameRx, TestProfile, hosted_blob};

const CAM: &str = "ephemeral:cam";
/// `CAM`'s ring depth: this many later messages evict one.
const CAM_DEPTH: u64 = 8;
const MAIL: &str = "brenn:mail";
const TOOLS: &str = "brenn:tool-requests/scan";
const HOST: &str = "host";

/// A profile that may read and publish `CAM`, read `MAIL` and `TOOLS`, and
/// has the door with `upload`.
fn profile(upload: bool) -> TestProfile {
    let facts = SubscriptionFacts {
        push_depth: 8,
        retain_depth: 8,
    };
    TestProfile {
        subscribable: HashMap::from([
            (CAM.to_string(), facts),
            (MAIL.to_string(), facts),
            (TOOLS.to_string(), facts),
        ]),
        publishable: HashMap::from([(None, HashSet::from([CAM.to_string()]))]),
        blob_grant: Some(BlobGrant { upload }),
        ..TestProfile::new()
    }
}

/// One session's handler inputs over a messenger with the ring channel `CAM`,
/// the durable channel `MAIL` and, unless `limits` is `None`, a blob store of
/// those limits.
struct Rig {
    ctx: AttachSessionCtx,
    rx: FrameRx,
    table: BlobTable,
    counters: SessionCounters,
    messenger: Arc<Messenger>,
    dir: TempBlobDir,
}

/// The durable channel `MAIL`, its standing window unbounded.
fn mail_entry() -> brenn_lib::messaging::ChannelEntry {
    let raw = ChannelConfigRaw {
        send_rate: None,
        uuid: Some(Uuid::new_v4().to_string()),
        address: Some("mail".to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(Depth::Unbounded),
        retain_depth: Some(Depth::Unbounded),
        standing_retain_depth: Some(Depth::Unbounded),
        noise: None,
        sink: None,
        wake_min: None,
        non_text: false,
        untyped_content_type: None,
    };
    build_channel_entries(&[raw], &MessagingGlobalConfig::default())
        .pop()
        .expect("one channel entry")
}

async fn rig_with(profile: TestProfile, version: u32, limits: Option<BlobLimits>) -> Rig {
    let entry = ephemeral_channel_entry("cam", CAM_DEPTH);
    let mail = mail_entry();
    let db = brenn_messaging_store::db::init_db_memory();
    brenn_messaging_store::db::upsert_channels(&*db.lock().await, std::slice::from_ref(&mail));
    let mut policy = AppPolicy::default();
    policy.grants.insert(AppCapability::EphemeralPublish);
    policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact("cam".to_string())];
    let mut registrations = covering_system_publisher(HOST);
    registrations.extend(surface_registrations(HashMap::from([(
        crate::test_support::TEST_ATTACHER.to_string(),
        policy,
    )])));
    let budget = AttachSendBudget {
        burst: 64,
        refill: std::time::Duration::from_secs(3600),
    };
    let messenger = Messenger::new(
        db,
        Arc::new(MessagingDirectory::with_entries(vec![entry.clone(), mail])),
        Arc::from("test"),
        Arc::new(indexmap::IndexMap::new()),
        Arc::new(NoopWakeRouter) as Arc<dyn WakeRouter>,
        MessagingGlobalConfig::default(),
    )
    .with_subscriber_registrations(registrations)
    .with_ring_stores(Arc::new(brenn_messaging_store::store::RingStores::build(
        &[entry],
    )))
    .with_attach_send_budgets(attach_principal_budgets(
        AttachScope::surface(crate::test_support::TEST_ATTACHER),
        vec![(None, budget)],
    ));
    let (messenger, dir) = match limits {
        Some(limits) => install_blob_store_with(messenger, limits).await,
        None => (messenger, TempBlobDir::new()),
    };
    let (ctx, rx) = AttachCtxBuilder::new(profile)
        .version(version)
        .messenger(Arc::clone(&messenger))
        .build();
    Rig {
        ctx,
        rx,
        table: BlobTable::default(),
        counters: SessionCounters::default(),
        messenger,
        dir,
    }
}

async fn rig() -> Rig {
    rig_with(profile(true), 6, Some(TEST_BLOB_LIMITS)).await
}

fn name(s: &str) -> AttachmentName {
    AttachmentName::new(s).expect("attachment name")
}

fn violated(outcome: FrameOutcome) -> String {
    match outcome {
        FrameOutcome::Violation(detail) => detail,
        FrameOutcome::Continue => panic!("expected a violation, got Continue"),
        FrameOutcome::Disconnect => panic!("expected a violation, got Disconnect"),
    }
}

fn continued(outcome: FrameOutcome) {
    match outcome {
        FrameOutcome::Continue => {}
        FrameOutcome::Violation(detail) => panic!("unexpected violation: {detail}"),
        FrameOutcome::Disconnect => panic!("unexpected disconnect"),
    }
}

impl Rig {
    fn store(&self) -> &Arc<BlobStore> {
        self.messenger.blob_store().expect("a blob store")
    }

    /// How many answers of `refusal` this session counted.
    fn refused(&self, refusal: BlobRefusal) -> u64 {
        self.counters
            .blobs
            .refused
            .get(&refusal)
            .copied()
            .unwrap_or(0)
    }

    fn answer(&mut self) -> ServerFrame {
        self.rx.try_recv().expect("an answer")
    }

    fn no_answer(&mut self) {
        assert!(self.rx.try_recv().is_err(), "no answer expected");
    }

    async fn create(&mut self, handle: u64, size: u64) -> FrameOutcome {
        handle_blob_create(
            &self.ctx,
            &mut self.table,
            BlobHandle(handle),
            "image/jpeg",
            size,
            &mut self.counters,
        )
        .await
    }

    async fn create_answer(&mut self, handle: u64, size: u64) -> BlobCreateOutcome {
        continued(self.create(handle, size).await);
        match self.answer() {
            ServerFrame::BlobCreateResult { handle: h, outcome } => {
                assert_eq!(h, BlobHandle(handle));
                outcome
            }
            other => panic!("expected BlobCreateResult, got {other:?}"),
        }
    }

    async fn write(&mut self, handle: u64, offset: u64, bytes: &[u8]) -> FrameOutcome {
        handle_blob_write(
            &self.ctx,
            &mut self.table,
            BlobHandle(handle),
            offset,
            bytes.to_vec(),
            &mut self.counters,
        )
        .await
    }

    async fn seal(&mut self, handle: u64) -> FrameOutcome {
        handle_blob_seal(
            &self.ctx,
            &mut self.table,
            BlobHandle(handle),
            &mut self.counters,
        )
        .await
    }

    async fn seal_answer(&mut self, handle: u64) -> BlobSealOutcome {
        continued(self.seal(handle).await);
        match self.answer() {
            ServerFrame::BlobSealResult { outcome, .. } => outcome,
            other => panic!("expected BlobSealResult, got {other:?}"),
        }
    }

    async fn upload(&mut self, handle: u64, bytes: &[u8]) {
        assert_eq!(
            self.create_answer(handle, bytes.len() as u64).await,
            BlobCreateOutcome::Ok
        );
        continued(self.write(handle, 0, bytes).await);
        assert_eq!(self.seal_answer(handle).await, BlobSealOutcome::Ok);
    }

    async fn open(
        &mut self,
        handle: u64,
        channel: &str,
        message_id: Uuid,
        part: &str,
    ) -> FrameOutcome {
        let part = name(part);
        handle_blob_open(
            &self.ctx,
            &mut self.table,
            BlobOpenRequest {
                handle: BlobHandle(handle),
                channel,
                message_id,
                name: &part,
            },
            &mut self.counters,
        )
        .await
    }

    async fn open_answer(
        &mut self,
        handle: u64,
        channel: &str,
        id: Uuid,
        part: &str,
    ) -> BlobOpenOutcome {
        continued(self.open(handle, channel, id, part).await);
        match self.answer() {
            ServerFrame::BlobOpenResult { outcome, .. } => outcome,
            other => panic!("expected BlobOpenResult, got {other:?}"),
        }
    }

    async fn read(&mut self, handle: u64, offset: u64, len: u64) -> FrameOutcome {
        handle_blob_read(
            &self.ctx,
            &mut self.table,
            BlobReadRequest {
                correlation: 9,
                handle: BlobHandle(handle),
                offset,
                len,
            },
            &mut self.counters,
        )
        .await
    }

    async fn read_answer(&mut self, handle: u64, offset: u64, len: u64) -> BlobReadOutcome {
        continued(self.read(handle, offset, len).await);
        match self.answer() {
            ServerFrame::BlobReadResult {
                correlation,
                outcome,
            } => {
                assert_eq!(correlation, 9);
                outcome
            }
            other => panic!("expected BlobReadResult, got {other:?}"),
        }
    }

    async fn close(&mut self, handle: u64) {
        continued(
            handle_blob_close(
                &self.ctx,
                &mut self.table,
                BlobHandle(handle),
                &mut self.counters,
            )
            .await,
        );
    }

    async fn publish(&mut self, attachments: &[PublishAttachment]) -> PublishOutcome {
        let body = MessageBody::from("frame");
        let mut bucket = TokenBucket::new(64, std::time::Duration::from_secs(1), 64);
        let request = PublishRequest {
            channel: CAM,
            attribution: None,
            body: &body,
            urgency: Urgency::Normal,
            correlation: Some(1),
            attachments,
            blobs: &self.table,
        };
        continued(handle_publish(&self.ctx, &mut bucket, request, &mut self.counters).await);
        match self.answer() {
            ServerFrame::PublishResult { outcome, .. } => outcome,
            other => panic!("expected PublishResult, got {other:?}"),
        }
    }

    /// A message on `CAM` from the host carrying `bytes` as `photo`.
    async fn hosted(&self, bytes: &[u8]) -> Uuid {
        self.hosted_on(CAM, bytes).await.0
    }

    /// A message on `channel` from the host carrying `bytes` as `photo`, and
    /// the blob's id. The host keeps no hold.
    async fn hosted_on(&self, channel: &str, bytes: &[u8]) -> (Uuid, BlobId) {
        hosted_blob(&self.messenger, HOST, channel, bytes).await
    }

    /// The session's bytes held, by the table's own sum.
    fn bytes_held(&self) -> u64 {
        self.table.bytes_held()
    }
}

fn attach(part: &str, handle: u64) -> PublishAttachment {
    PublishAttachment::new(name(part), BlobHandle(handle))
}

#[tokio::test]
async fn the_door_opens_on_v6_with_a_grant_and_a_store_and_welcome_says_so() {
    let open = rig().await;
    let facts = open.ctx.blob_door().expect("a door").facts();
    assert_eq!(
        facts,
        BlobFacts {
            max_blob_bytes: TEST_BLOB_LIMITS.max_blob_bytes,
            max_chunk_bytes: BLOB_CHUNK_BYTES as u64,
            max_handles: MAX_BLOB_HANDLES,
            max_bytes_held: 4 * TEST_BLOB_LIMITS.max_blob_bytes,
            max_inflight_reads: MAX_INFLIGHT_BLOB_READS,
            upload: true,
        }
    );
    let ServerFrame::Welcome { blobs, .. } = welcome(&open.ctx, 6, 20) else {
        panic!("a Welcome");
    };
    assert_eq!(blobs, Some(facts));

    let no_grant = TestProfile {
        blob_grant: None,
        ..profile(true)
    };
    for mut closed in [
        rig_with(profile(true), 5, Some(TEST_BLOB_LIMITS)).await,
        rig_with(no_grant, 6, Some(TEST_BLOB_LIMITS)).await,
        rig_with(profile(true), 6, None).await,
    ] {
        assert!(closed.ctx.blob_door().is_none());
        let version = closed.ctx.version();
        let ServerFrame::Welcome { blobs, .. } = welcome(&closed.ctx, version, 20) else {
            panic!("a Welcome");
        };
        assert_eq!(blobs, None);
        let detail = violated(closed.create(1, 4).await);
        assert!(
            detail.contains("BlobCreate without a blob door"),
            "{detail}"
        );
        let detail = violated(closed.read(1, 0, 1).await);
        assert!(detail.contains("BlobRead without a blob door"), "{detail}");
    }
}

#[tokio::test]
async fn an_upload_is_published_read_back_and_outlives_its_session() {
    let mut rig = rig().await;
    let bytes: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
    assert_eq!(rig.create_answer(7, 1000).await, BlobCreateOutcome::Ok);
    // Out of order, as positional writes may be.
    continued(rig.write(7, 600, &bytes[600..]).await);
    continued(rig.write(7, 0, &bytes[..600]).await);
    rig.no_answer();
    assert_eq!(rig.seal_answer(7).await, BlobSealOutcome::Ok);
    assert_eq!(
        rig.read_answer(7, 100, 50).await,
        BlobReadOutcome::Ok {
            bytes: bytes[100..150].to_vec()
        }
    );
    assert_eq!(rig.publish(&[attach("frame", 7)]).await, PublishOutcome::Ok);
    // An upload never published, and one never sealed.
    rig.upload(8, b"stray").await;
    assert_eq!(rig.create_answer(9, 10).await, BlobCreateOutcome::Ok);
    assert_eq!(rig.store().usage().sealed, 2);
    assert_eq!(rig.store().usage().unsealed, 1);

    std::mem::take(&mut rig.table).drop_off_task().await;
    let usage = rig.store().usage();
    assert_eq!(
        (usage.sealed, usage.unsealed),
        (1, 0),
        "the published blob alone"
    );
    assert_eq!(usage.charged_bytes, 1000);
}

#[tokio::test]
async fn a_sequence_behind_a_refused_create_is_answered_and_never_violates() {
    let mut rig = rig().await;
    let too_big = TEST_BLOB_LIMITS.max_blob_bytes + 1;
    assert_eq!(
        rig.create_answer(3, too_big).await,
        BlobCreateOutcome::BlobTooLarge {
            size: too_big,
            max: TEST_BLOB_LIMITS.max_blob_bytes
        }
    );
    continued(rig.write(3, 0, b"dropped").await);
    rig.no_answer();
    assert_eq!(rig.seal_answer(3).await, BlobSealOutcome::UnknownHandle);
    assert_eq!(
        rig.read_answer(3, 0, 1).await,
        BlobReadOutcome::UnknownHandle
    );
    assert_eq!(
        rig.publish(&[attach("frame", 3)]).await,
        PublishOutcome::AttachmentNotHeld
    );
    rig.close(3).await;
    rig.no_answer();
    assert_eq!(rig.counters.blobs.writes_dropped, 1);
    assert_eq!(rig.counters.blobs.closes_noop, 1);
    assert_eq!(rig.counters.blobs.publish_not_held, 1);
}

// TODO(attach-blob-write-io-failure-test): the gap here is left by the
// client; no test fails a store write to reach the handler's I/O-error arm.
#[tokio::test]
async fn an_incomplete_seal_leaves_a_writer_that_reads_and_publishes_as_answers() {
    let mut rig = rig().await;
    assert_eq!(rig.create_answer(4, 10).await, BlobCreateOutcome::Ok);
    continued(rig.write(4, 0, b"12345").await);
    assert_eq!(
        rig.seal_answer(4).await,
        BlobSealOutcome::Incomplete { offset: 5, len: 5 }
    );
    assert_eq!(rig.read_answer(4, 0, 1).await, BlobReadOutcome::NotSealed);
    assert_eq!(
        rig.publish(&[attach("frame", 4)]).await,
        PublishOutcome::AttachmentNotHeld
    );
    // The writer stays open to be filled.
    continued(rig.write(4, 5, b"67890").await);
    assert_eq!(rig.seal_answer(4).await, BlobSealOutcome::Ok);
    rig.close(4).await;
    assert_eq!(
        rig.store().usage().sealed,
        0,
        "closing the only hold deletes it"
    );
}

#[tokio::test]
async fn frames_no_conforming_client_sends_are_violations() {
    let mut rig = rig().await;
    rig.upload(1, b"sealed").await;
    assert_eq!(rig.create_answer(2, 4).await, BlobCreateOutcome::Ok);

    let cases: Vec<(&str, FrameOutcome)> = vec![
        ("live blob handle 1", rig.create(1, 4).await),
        ("BlobWrite to sealed", rig.write(1, 0, b"x").await),
        ("passes the declared size", rig.write(2, 3, b"xy").await),
        (
            "passes the declared size",
            rig.write(2, u64::MAX, b"x").await,
        ),
        ("outside 1..=", rig.write(2, 0, b"").await),
        ("BlobSeal of sealed", rig.seal(1).await),
        (
            "live blob handle 2",
            rig.open(2, CAM, Uuid::new_v4(), "photo").await,
        ),
        (
            "BlobOpen on unsubscribable channel",
            rig.open(5, "ephemeral:elsewhere", Uuid::new_v4(), "photo")
                .await,
        ),
        (
            "over 262144",
            rig.read(1, 0, BLOB_CHUNK_BYTES as u64 + 1).await,
        ),
    ];
    for (expected, outcome) in cases {
        let detail = violated(outcome);
        assert!(detail.contains(expected), "{expected:?} not in {detail:?}");
    }
    let detail = violated(
        handle_blob_create(
            &rig.ctx,
            &mut rig.table,
            BlobHandle(6),
            "not a type",
            4,
            &mut rig.counters,
        )
        .await,
    );
    assert!(detail.contains("malformed content type"), "{detail}");

    let mut readonly = rig_with(profile(false), 6, Some(TEST_BLOB_LIMITS)).await;
    let detail = violated(readonly.create(1, 4).await);
    assert!(detail.contains("may not upload"), "{detail}");
}

#[tokio::test]
async fn an_open_reads_an_attachment_and_refuses_by_type() {
    let mut rig = rig().await;
    let bytes: Vec<u8> = (0..200u8).collect();
    let id = rig.hosted(&bytes).await;
    assert_eq!(
        rig.open_answer(1, CAM, id, "photo").await,
        BlobOpenOutcome::Ok {
            size: 200,
            content_type: "application/octet-stream".to_string()
        }
    );
    assert_eq!(
        rig.read_answer(1, 0, 200).await,
        BlobReadOutcome::Ok {
            bytes: bytes.clone()
        }
    );
    assert_eq!(
        rig.read_answer(1, 150, 50).await,
        BlobReadOutcome::Ok {
            bytes: bytes[150..].to_vec()
        }
    );
    assert_eq!(
        rig.read_answer(1, 200, 0).await,
        BlobReadOutcome::Ok { bytes: Vec::new() }
    );
    assert_eq!(
        rig.read_answer(1, 150, 51).await,
        BlobReadOutcome::OutOfRange { size: 200 }
    );
    assert_eq!(
        rig.read_answer(1, u64::MAX, 1).await,
        BlobReadOutcome::OutOfRange { size: 200 }
    );

    // Forwarding by handle: the same blob under a second message.
    assert_eq!(rig.publish(&[attach("copy", 1)]).await, PublishOutcome::Ok);
    assert_eq!(rig.store().usage().sealed, 1);

    assert_eq!(
        rig.open_answer(2, CAM, id, "absent").await,
        BlobOpenOutcome::NoSuchAttachment
    );
    assert_eq!(
        rig.open_answer(2, CAM, Uuid::new_v4(), "photo").await,
        BlobOpenOutcome::NotRetained
    );
    // Any id will do: the refusal reads only the channel.
    for message_id in [id, Uuid::new_v4()] {
        assert_eq!(
            rig.open_answer(2, TOOLS, message_id, "photo").await,
            BlobOpenOutcome::ToolChannel
        );
    }
    assert_eq!(rig.table.slots.len(), 1, "no refused open holds a handle");
    assert_eq!(rig.counters.blobs.opens, 1);
    assert_eq!(rig.refused(BlobRefusal::ReadOutOfRange), 2);
    assert_eq!(rig.refused(BlobRefusal::OpenNoSuchAttachment), 1);
    assert_eq!(rig.refused(BlobRefusal::OpenNotRetained), 1);
    assert_eq!(rig.refused(BlobRefusal::OpenToolChannel), 2);
    assert_eq!(rig.counters.blobs.refused.len(), 4, "nothing else counted");
}

#[tokio::test]
async fn the_per_session_bounds_are_typed_answers() {
    // The handle bound, by opens of one attachment.
    let mut rig = rig().await;
    let id = rig.hosted(b"x").await;
    for handle in 0..u64::from(MAX_BLOB_HANDLES) {
        assert!(matches!(
            rig.open_answer(handle, CAM, id, "photo").await,
            BlobOpenOutcome::Ok { .. }
        ));
    }
    assert_eq!(
        rig.open_answer(100, CAM, id, "photo").await,
        BlobOpenOutcome::HandleLimit
    );
    assert_eq!(
        rig.create_answer(100, 1).await,
        BlobCreateOutcome::HandleLimit
    );
    assert_eq!(rig.refused(BlobRefusal::OpenHandleLimit), 1);
    assert_eq!(rig.refused(BlobRefusal::CreateHandleLimit), 1);
    rig.close(0).await;
    assert!(matches!(
        rig.open_answer(100, CAM, id, "photo").await,
        BlobOpenOutcome::Ok { .. }
    ));
    assert_eq!(rig.counters.blobs.refused.len(), 2, "nothing else counted");

    // The bytes-held bound on uploads, before the store is asked.
    let mut rig = rig_with(
        profile(true),
        6,
        Some(BlobLimits {
            max_blob_bytes: 100,
            budget_bytes: 10_000,
        }),
    )
    .await;
    for handle in 0..4 {
        assert_eq!(rig.create_answer(handle, 100).await, BlobCreateOutcome::Ok);
    }
    assert_eq!(
        rig.create_answer(4, 1).await,
        BlobCreateOutcome::BytesHeldLimit
    );
    assert_eq!(rig.refused(BlobRefusal::CreateBytesHeldLimit), 1);
    rig.close(0).await;
    assert_eq!(rig.create_answer(4, 100).await, BlobCreateOutcome::Ok);
    assert_eq!(rig.counters.blobs.refused.len(), 1, "nothing else counted");

    // The store's pool.
    let mut rig = rig_with(
        profile(true),
        6,
        Some(BlobLimits {
            max_blob_bytes: 100,
            budget_bytes: 150,
        }),
    )
    .await;
    assert_eq!(rig.create_answer(0, 100).await, BlobCreateOutcome::Ok);
    assert_eq!(
        rig.create_answer(1, 100).await,
        BlobCreateOutcome::BudgetExhausted
    );
    assert_eq!(
        rig.store().usage().unsealed,
        1,
        "a refused create leaves no file"
    );
    assert_eq!(rig.refused(BlobRefusal::CreateBudgetExhausted), 1);
    assert_eq!(rig.counters.blobs.refused.len(), 1, "nothing else counted");
}

/// Limits under which four 100-byte blobs fill a session's bytes held, with
/// the pool far from full.
const SMALL_BLOBS: BlobLimits = BlobLimits {
    max_blob_bytes: 100,
    budget_bytes: 10_000,
};

#[tokio::test]
async fn opened_bytes_count_against_the_bytes_held_bound() {
    let mut rig = rig_with(profile(true), 6, Some(SMALL_BLOBS)).await;
    let photo = [7u8; 100];
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(rig.hosted(&photo).await);
    }
    for (handle, id) in (0..4).zip(&ids) {
        assert!(matches!(
            rig.open_answer(handle, CAM, *id, "photo").await,
            BlobOpenOutcome::Ok { size: 100, .. }
        ));
    }
    assert_eq!(rig.bytes_held(), 400);
    assert_eq!(
        rig.open_answer(4, CAM, ids[4], "photo").await,
        BlobOpenOutcome::BytesHeldLimit { size: 100 }
    );
    assert!(!rig.table.slots.contains_key(&BlobHandle(4)));
    assert_eq!(rig.refused(BlobRefusal::OpenBytesHeldLimit), 1);
    assert_eq!(rig.counters.blobs.opens, 4);

    // Frames pipelined behind the refused open are answers or no-ops.
    assert_eq!(
        rig.read_answer(4, 0, 1).await,
        BlobReadOutcome::UnknownHandle
    );
    assert_eq!(
        rig.publish(&[attach("frame", 4)]).await,
        PublishOutcome::AttachmentNotHeld
    );
    rig.close(4).await;
    rig.no_answer();
    assert_eq!(rig.counters.blobs.closes_noop, 1);

    // Opened bytes refuse a create, and a close frees them.
    assert_eq!(
        rig.create_answer(9, 1).await,
        BlobCreateOutcome::BytesHeldLimit
    );
    rig.close(0).await;
    assert_eq!(rig.bytes_held(), 300);
    assert!(matches!(
        rig.open_answer(4, CAM, ids[4], "photo").await,
        BlobOpenOutcome::Ok { .. }
    ));
    assert_eq!(rig.bytes_held(), 400);
    assert_eq!(rig.refused(BlobRefusal::CreateBytesHeldLimit), 1);
    assert_eq!(rig.counters.blobs.refused.len(), 3, "nothing else counted");

    // Uploaded bytes refuse an open; a seal leaves the sum unchanged; one blob
    // under two handles counts twice.
    let mut rig = rig_with(profile(true), 6, Some(SMALL_BLOBS)).await;
    let id = rig.hosted(&[1u8; 50]).await;
    assert_eq!(rig.create_answer(0, 100).await, BlobCreateOutcome::Ok);
    assert_eq!(rig.bytes_held(), 100);
    continued(rig.write(0, 0, &photo).await);
    assert_eq!(rig.seal_answer(0).await, BlobSealOutcome::Ok);
    assert_eq!(rig.bytes_held(), 100);
    for handle in [1, 2] {
        assert!(matches!(
            rig.open_answer(handle, CAM, id, "photo").await,
            BlobOpenOutcome::Ok { size: 50, .. }
        ));
    }
    assert_eq!(rig.bytes_held(), 200);
    for handle in [3, 5] {
        assert_eq!(rig.create_answer(handle, 100).await, BlobCreateOutcome::Ok);
    }
    assert_eq!(
        rig.open_answer(4, CAM, id, "photo").await,
        BlobOpenOutcome::BytesHeldLimit { size: 50 }
    );
    rig.close(3).await;
    assert_eq!(rig.bytes_held(), 300);
    assert!(matches!(
        rig.open_answer(4, CAM, id, "photo").await,
        BlobOpenOutcome::Ok { .. }
    ));
}

/// Open attachments on `channel` until one is refused, with `blobs` of 100
/// bytes each hosted there. Returns the refused blob's id.
async fn open_until_refused(rig: &mut Rig, channel: &str, blobs: usize) -> BlobId {
    let mut hosted = Vec::new();
    for _ in 0..blobs {
        hosted.push(rig.hosted_on(channel, &[5u8; 100]).await);
    }
    for (handle, (message_id, id)) in (0u64..).zip(hosted) {
        match rig.open_answer(handle, channel, message_id, "photo").await {
            BlobOpenOutcome::Ok { .. } => {}
            BlobOpenOutcome::BytesHeldLimit { size } => {
                assert_eq!(size, 100);
                assert_eq!(handle, 4, "four fill the bound");
                return id;
            }
            other => panic!("expected Ok or BytesHeldLimit, got {other:?}"),
        }
    }
    panic!("no open was refused");
}

/// The bound holds in the store: after every message is evicted, the pool
/// is charged exactly the blobs the session holds, and the refused open left
/// none. Ring blobs go with their last hold.
#[tokio::test]
async fn a_session_keeps_at_most_its_bound_charged_after_ring_eviction() {
    let mut rig = rig_with(profile(false), 6, Some(SMALL_BLOBS)).await;
    let refused = open_until_refused(&mut rig, CAM, 5).await;
    for i in 0..CAM_DEPTH {
        publish_attached(&rig.messenger, HOST, CAM, &format!("evicts {i}"), &[], None).await;
    }
    let usage = rig.store().usage();
    assert_eq!(usage.charged_bytes, 400);
    assert_eq!(usage.sealed, 4);
    assert!(!rig.dir.holds_file(&refused));

    for handle in 0..4 {
        rig.close(handle).await;
    }
    assert_eq!(rig.store().usage().charged_bytes, 0);
}

/// The same on a durable channel, read after the rows are gone and the reaper
/// has run: it deletes the refused blob and skips the held ones. Blobs a row
/// named stay charged after the session lets go, until the next reap.
#[tokio::test]
async fn a_session_keeps_at_most_its_bound_charged_after_durable_eviction() {
    let mut rig = rig_with(profile(false), 6, Some(SMALL_BLOBS)).await;
    let refused = open_until_refused(&mut rig, MAIL, 5).await;
    // `MAIL` is the rig's only durable channel.
    rig.messenger
        .db()
        .lock()
        .await
        .execute("DELETE FROM messaging_messages", [])
        .expect("deleting MAIL's rows");
    rig.messenger.reap_blobs().await;
    let usage = rig.store().usage();
    assert_eq!(usage.charged_bytes, 400);
    assert_eq!(usage.sealed, 4);
    assert!(!rig.dir.holds_file(&refused));

    for handle in 0..4 {
        rig.close(handle).await;
    }
    assert_eq!(rig.store().usage().charged_bytes, 400);
    rig.messenger.reap_blobs().await;
    assert_eq!(rig.store().usage().charged_bytes, 0);
}

#[tokio::test]
async fn read_answers_in_flight_are_bounded_until_the_writer_takes_them() {
    let mut rig = rig().await;
    rig.upload(1, b"0123456789").await;
    // Nothing drains the queue: each answer's guard stays in it.
    for _ in 0..MAX_INFLIGHT_BLOB_READS {
        continued(rig.read(1, 0, 4).await);
    }
    let mut queued: Vec<Outbound> = Vec::new();
    while let Ok(outbound) = rig.rx.0.try_recv() {
        queued.push(outbound);
    }
    assert_eq!(queued.len(), MAX_INFLIGHT_BLOB_READS as usize);
    assert_eq!(
        rig.read_answer(1, 0, 4).await,
        BlobReadOutcome::TooManyReads
    );
    assert_eq!(rig.refused(BlobRefusal::ReadTooManyReads), 1);
    // The writer writes them; their guards drop.
    drop(queued);
    assert_eq!(
        rig.read_answer(1, 0, 4).await,
        BlobReadOutcome::Ok {
            bytes: b"0123".to_vec()
        }
    );
}

#[tokio::test]
async fn publish_attachment_shape_is_checked_before_anything_is_drawn() {
    let mut rig = rig().await;
    rig.upload(1, b"one").await;
    let body = MessageBody::from("frame");
    let mut bucket = TokenBucket::new(1, std::time::Duration::from_secs(60), 1);
    let twice = [attach("a", 1), attach("a", 1)];
    let request = PublishRequest {
        channel: CAM,
        attribution: None,
        body: &body,
        urgency: Urgency::Normal,
        correlation: Some(1),
        attachments: &twice,
        blobs: &rig.table,
    };
    let detail = violated(handle_publish(&rig.ctx, &mut bucket, request, &mut rig.counters).await);
    assert!(detail.contains("names attachment a twice"), "{detail}");

    let many: Vec<PublishAttachment> = (0..=MAX_ATTACHMENTS_PER_MESSAGE)
        .map(|i| attach(&format!("n{i}"), 1))
        .collect();
    let entries = [BatchEntry::new(CAM, body.clone(), Urgency::Normal).attachments(many)];
    let request = PublishBatchRequest {
        attribution: None,
        correlation: 2,
        publishes: &entries,
        deferred_ops: &[],
        blobs: &rig.table,
    };
    let detail = violated(handle_publish_batch(&rig.ctx, request, &mut rig.counters).await);
    assert!(detail.contains("33 attachments, over 32"), "{detail}");

    // Without the door, any list is a violation.
    let mut closed = rig_with(profile(true), 5, Some(TEST_BLOB_LIMITS)).await;
    let request = PublishRequest {
        channel: CAM,
        attribution: None,
        body: &body,
        urgency: Urgency::Normal,
        correlation: Some(1),
        attachments: &[attach("a", 1)],
        blobs: &closed.table,
    };
    let detail =
        violated(handle_publish(&closed.ctx, &mut bucket, request, &mut closed.counters).await);
    assert!(detail.contains("Publish without a blob door"), "{detail}");
}

#[tokio::test]
async fn a_batch_names_held_blobs_or_applies_nothing() {
    let mut rig = rig().await;
    rig.upload(1, b"one").await;
    let entry = |body: &str, handle: u64| {
        BatchEntry::new(CAM, MessageBody::from(body), Urgency::Normal)
            .attachments(vec![attach("f", handle)])
    };
    let outcome = |rig: &mut Rig| match rig.answer() {
        ServerFrame::PublishBatchResult { outcome, .. } => outcome,
        other => panic!("expected PublishBatchResult, got {other:?}"),
    };
    let bodies = |rig: &Rig| {
        let messenger = Arc::clone(&rig.messenger);
        async move {
            messenger
                .store_for_address(CAM)
                .replay_from(None, Depth::Bounded(64))
                .await
                .messages
                .into_iter()
                .map(|row| row.message.body.as_text().unwrap().to_owned())
                .collect::<Vec<_>>()
        }
    };
    let sender = ParticipantId::for_surface(crate::test_support::TEST_ATTACHER);

    // A schedule for the refused batch's cancel to name.
    let later = (Utc::now() + chrono::Duration::hours(1)).timestamp_millis() as u64;
    let mut scheduled = BatchEntry::new(CAM, MessageBody::from("later"), Urgency::Normal);
    scheduled.deliver_after = Some(later);
    let request = PublishBatchRequest {
        attribution: None,
        correlation: 1,
        publishes: std::slice::from_ref(&scheduled),
        deferred_ops: &[],
        blobs: &rig.table,
    };
    continued(handle_publish_batch(&rig.ctx, request, &mut rig.counters).await);
    assert_eq!(outcome(&mut rig), PublishBatchOutcome::Ok);
    let parked = rig
        .messenger
        .deferred_view_for_sender(CAM, sender.as_str())
        .await;
    assert_eq!(parked.len(), 1);
    let parked_id = parked[0].message_uuid();

    // Entry 0 names a held handle, entry 1 does not; the cancel is valid.
    let refused = [entry("held", 1), entry("unheld", 2)];
    let ops = [BatchDeferredOp {
        channel: CAM.to_string(),
        message_id: parked_id,
        op: DeferredOpKind::Cancel,
    }];
    let request = PublishBatchRequest {
        attribution: None,
        correlation: 2,
        publishes: &refused,
        deferred_ops: &ops,
        blobs: &rig.table,
    };
    continued(handle_publish_batch(&rig.ctx, request, &mut rig.counters).await);
    assert_eq!(outcome(&mut rig), PublishBatchOutcome::AttachmentNotHeld);
    assert_eq!(rig.counters.blobs.publish_not_held, 1);
    assert!(bodies(&rig).await.is_empty(), "no entry was published");
    let parked = rig
        .messenger
        .deferred_view_for_sender(CAM, sender.as_str())
        .await;
    assert_eq!(parked.len(), 1, "the cancel was not applied");
    assert_eq!(parked[0].message_uuid(), parked_id);

    let applied = [entry("a", 1), entry("b", 1)];
    let request = PublishBatchRequest {
        attribution: None,
        correlation: 3,
        publishes: &applied,
        deferred_ops: &[],
        blobs: &rig.table,
    };
    continued(handle_publish_batch(&rig.ctx, request, &mut rig.counters).await);
    assert_eq!(outcome(&mut rig), PublishBatchOutcome::Ok);
    assert_eq!(bodies(&rig).await, vec!["a".to_string(), "b".to_string()]);

    // Two messages hold the one blob after the session lets go.
    std::mem::take(&mut rig.table).drop_off_task().await;
    assert_eq!(rig.store().usage().sealed, 1);
}

/// Where handle 1 stands before the frame under test.
#[derive(Clone, Copy, Debug)]
enum State {
    Absent,
    /// Created for four bytes, all of them written, not sealed.
    Writer,
    /// `abcd`, uploaded and sealed.
    Held,
}

/// A fresh rig with handle 1 in `state`.
async fn in_state(state: State) -> Rig {
    let mut rig = rig().await;
    match state {
        State::Absent => {}
        State::Writer => {
            assert_eq!(rig.create_answer(1, 4).await, BlobCreateOutcome::Ok);
            continued(rig.write(1, 0, b"abcd").await);
        }
        State::Held => rig.upload(1, b"abcd").await,
    }
    rig
}

/// Every cell of the handle-state table: what each frame does to a handle
/// that is absent, a writer or held. Each cell runs on a rig of its own.
#[tokio::test]
async fn each_cell_of_the_handle_state_table() {
    use State::{Absent, Held, Writer};

    // BlobCreate.
    assert_eq!(
        in_state(Absent).await.create_answer(1, 4).await,
        BlobCreateOutcome::Ok
    );
    for state in [Writer, Held] {
        let detail = violated(in_state(state).await.create(1, 4).await);
        assert!(detail.contains("live blob handle 1"), "{state:?}: {detail}");
    }

    // BlobOpen.
    let mut rig = in_state(Absent).await;
    let id = rig.hosted(b"photo").await;
    assert!(matches!(
        rig.open_answer(1, CAM, id, "photo").await,
        BlobOpenOutcome::Ok { size: 5, .. }
    ));
    for state in [Writer, Held] {
        let mut rig = in_state(state).await;
        let id = rig.hosted(b"photo").await;
        let detail = violated(rig.open(1, CAM, id, "photo").await);
        assert!(detail.contains("live blob handle 1"), "{state:?}: {detail}");
    }

    // BlobWrite: dropped and counted, written, a violation.
    let mut rig = in_state(Absent).await;
    continued(rig.write(1, 0, b"x").await);
    rig.no_answer();
    assert_eq!(rig.counters.blobs.writes_dropped, 1);
    let mut rig = in_state(Absent).await;
    assert_eq!(rig.create_answer(1, 4).await, BlobCreateOutcome::Ok);
    continued(rig.write(1, 0, b"abcd").await);
    rig.no_answer();
    assert_eq!(rig.seal_answer(1).await, BlobSealOutcome::Ok, "written");
    let detail = violated(in_state(Held).await.write(1, 0, b"x").await);
    assert!(detail.contains("BlobWrite to sealed"), "{detail}");

    // BlobSeal.
    assert_eq!(
        in_state(Absent).await.seal_answer(1).await,
        BlobSealOutcome::UnknownHandle
    );
    assert_eq!(
        in_state(Writer).await.seal_answer(1).await,
        BlobSealOutcome::Ok
    );
    let detail = violated(in_state(Held).await.seal(1).await);
    assert!(detail.contains("BlobSeal of sealed"), "{detail}");

    // BlobRead.
    assert_eq!(
        in_state(Absent).await.read_answer(1, 0, 1).await,
        BlobReadOutcome::UnknownHandle
    );
    assert_eq!(
        in_state(Writer).await.read_answer(1, 0, 1).await,
        BlobReadOutcome::NotSealed
    );
    assert_eq!(
        in_state(Held).await.read_answer(1, 1, 2).await,
        BlobReadOutcome::Ok {
            bytes: b"bc".to_vec()
        }
    );

    // A publish naming the handle.
    for state in [Absent, Writer] {
        assert_eq!(
            in_state(state).await.publish(&[attach("f", 1)]).await,
            PublishOutcome::AttachmentNotHeld,
            "{state:?}"
        );
    }
    assert_eq!(
        in_state(Held).await.publish(&[attach("f", 1)]).await,
        PublishOutcome::Ok
    );

    // BlobClose: a counted no-op, the bytes deleted, the hold dropped.
    let mut rig = in_state(Absent).await;
    rig.close(1).await;
    rig.no_answer();
    assert_eq!(rig.counters.blobs.closes_noop, 1);
    for state in [Writer, Held] {
        let mut rig = in_state(state).await;
        rig.close(1).await;
        rig.no_answer();
        assert_eq!(rig.counters.blobs.closes_noop, 0, "{state:?}");
        let usage = rig.store().usage();
        assert_eq!(
            (usage.sealed, usage.unsealed, usage.charged_bytes),
            (0, 0, 0),
            "{state:?}: nothing else held it"
        );
        assert_eq!(rig.bytes_held(), 0, "{state:?}");
    }
}

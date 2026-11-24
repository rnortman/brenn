//! The durable half of a blob's lifetime: adoption by `DbStore`, every
//! durable removal followed by a reap, redemption against the database, and
//! the reconciliation `BlobStore::open` makes at boot.

use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension};
use uuid::Uuid;

use super::helpers::*;
use crate::blob::{
    BlobError, BlobHold, BlobLimits, BlobStore, HeldAttachments, ReapReport, Retain,
};
use crate::db::*;
use crate::store::{DbStore, NewMessage, RetentionStore};
use crate::testutils::{held as attachments, holds_file, seal_bytes};
use brenn_db::Db;
use brenn_envelope::BlobId;
use brenn_lib::messaging::config::{Depth, Sink};
use brenn_lib::messaging::{ChannelEntry, ChannelScheme, MessageEnvelope, Urgency};

const LIMITS: BlobLimits = BlobLimits {
    max_blob_bytes: 1 << 20,
    budget_bytes: 4 << 20,
};

const ADDRESS: &str = "brenn:blobs";

struct Fixture {
    dir: tempfile::TempDir,
    db: Db,
    channel: Uuid,
    store: DbStore,
    blobs: Arc<BlobStore>,
}

fn blob_dir(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("db.blobs")
}

fn owner_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    blob_dir(dir).join("owner")
}

/// The blob files in the fixture's blob directory, sorted; the owner record is
/// not one.
fn blob_files(dir: &tempfile::TempDir) -> Vec<String> {
    crate::testutils::blob_files(&blob_dir(dir))
        .into_iter()
        .filter(|name| name != "owner")
        .collect()
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let db = init_db_memory();
    let channel = Uuid::new_v4();
    let blobs = {
        let conn = db.lock().await;
        upsert_channels(
            &conn,
            &[ChannelEntry {
                uuid: channel,
                address: ADDRESS.to_string(),
                description: None,
                resolved_channel: default_resolved_channel(),
                subscribers: vec![],
                transport_type: ChannelScheme::Brenn,
                mount: None,
            }],
        );
        BlobStore::open(&blob_dir(&dir), LIMITS, &conn)
    };
    let store = DbStore::new(db.clone(), channel, ADDRESS, Depth::Bounded(10));
    Fixture {
        dir,
        db,
        channel,
        store,
        blobs,
    }
}

fn message(body: &str, attachments: HeldAttachments) -> NewMessage {
    NewMessage {
        source: "node".to_string(),
        sender: "alice".to_string(),
        body: body.into(),
        urgency: Urgency::Normal,
        envelope_type: ChannelScheme::Brenn,
        transport: None,
        reply_to: None,
        delivery_deadline: None,
        impetus: None,
        publish_ts_ns: utc_to_ns(Utc::now()),
        attachments,
    }
}

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}

impl Fixture {
    fn sealed(&self, bytes: &[u8]) -> BlobHold {
        seal_bytes(&self.blobs, "application/octet-stream", bytes)
    }

    fn file_exists(&self, id: &BlobId) -> bool {
        holds_file(&blob_dir(&self.dir), id)
    }

    async fn reap(&self) -> ReapReport {
        let conn = self.db.lock().await;
        self.blobs.reap(&conn)
    }

    /// Redeem `id` the way host code does: the index first, the database only
    /// when the index cannot answer.
    async fn redeem(&self, id: &BlobId) -> Option<BlobHold> {
        match self.blobs.retain(id) {
            Retain::Held(hold) => Some(hold),
            Retain::NotFound => None,
            Retain::Unsettled => {
                let conn = self.db.lock().await;
                self.blobs.retain_settled(id, &conn).ok()
            }
        }
    }

    async fn refs(&self, id: &BlobId) -> Option<i64> {
        let conn = self.db.lock().await;
        conn.query_row(
            "SELECT refs FROM messaging_blobs WHERE id = ?1",
            [id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .expect("read refs")
    }

    async fn message_row_id(&self, uuid: Uuid) -> i64 {
        let conn = self.db.lock().await;
        conn.query_row(
            "SELECT id FROM messaging_messages WHERE uuid = ?1",
            [uuid.as_bytes().to_vec()],
            |row| row.get(0),
        )
        .unwrap()
    }

    async fn evict(&self, keep: u64, sink: Sink, archive: Option<&Path>) -> usize {
        let conn = self.db.lock().await;
        bus_gc_evict_channel(
            &conn,
            self.channel,
            ADDRESS,
            ChannelScheme::Brenn,
            keep,
            sink,
            archive,
        )
        .messages_evicted
    }

    /// Assert that `id` is gone: no file, no redemption, and the charge
    /// returned (the store's total equals `charged`).
    async fn assert_gone(&self, id: &BlobId, charged: u64) {
        assert!(!self.file_exists(id), "blob {id} still has a file");
        assert!(self.redeem(id).await.is_none(), "blob {id} still redeems");
        assert_eq!(self.blobs.usage().charged_bytes, charged);
    }

    async fn assert_readable(&self, id: &BlobId, bytes: &[u8]) {
        let hold = self.redeem(id).await.expect("blob redeems");
        assert_eq!(hold.read(0, bytes.len()).unwrap(), bytes);
    }

    /// Publish two messages: the first names `only` and `shared`, the second
    /// names `shared`. Returns the two blobs' ids with every hold dropped.
    async fn two_messages(&self, deliver_after: Option<DateTime<Utc>>) -> (BlobId, BlobId, Uuid) {
        let only = self.sealed(b"only");
        let shared = self.sealed(b"shared");
        let first = message("first", attachments(&[("a", &only), ("b", &shared)]));
        let first_uuid = match deliver_after {
            None => self.store.append(first).await.committed.message_uuid,
            Some(at) => self.store.park(first, at).await.unwrap().message_uuid,
        };
        self.store
            .append(message("second", attachments(&[("a", &shared)])))
            .await;
        (only.id().clone(), shared.id().clone(), first_uuid)
    }
}

#[tokio::test]
async fn a_durable_message_stores_and_reads_back_its_attachments() {
    let f = fixture().await;
    let photo = f.sealed(b"jpeg bytes");
    let note = f.sealed(b"text");
    let held = attachments(&[("photo", &photo), ("note", &note), ("again", &photo)]);
    let refs = held.refs().clone();
    f.store.append(message("see attached", held)).await;

    let tail = f.store.retained_tail(Depth::Unbounded).await;
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].attachments, refs);
    // One reference row per message and blob, however many names give it.
    assert_eq!(f.refs(photo.id()).await, Some(1));
    assert_eq!(f.refs(note.id()).await, Some(1));

    let (photo_id, note_id) = (photo.id().clone(), note.id().clone());
    drop((photo, note));
    // The rows hold the blobs; the index alone cannot say so.
    assert!(matches!(f.blobs.retain(&photo_id), Retain::Unsettled));
    f.assert_readable(&photo_id, b"jpeg bytes").await;
    f.assert_readable(&note_id, b"text").await;
}

#[tokio::test]
async fn every_retained_read_carries_the_attachments() {
    let f = fixture().await;
    let blob = f.sealed(b"x");
    let held = attachments(&[("x", &blob)]);
    let refs = held.refs().clone();
    let uuid = f
        .store
        .append(message("m", held))
        .await
        .committed
        .message_uuid;
    let conn = f.db.lock().await;

    let by_uuid = load_envelope_by_uuid(&conn, uuid).unwrap();
    assert_eq!(by_uuid.attachments, refs);
    let window = load_channel_retained_tail(&conn, f.channel, Depth::Unbounded);
    assert_eq!(window[0].1.attachments, refs);
    let sql = format!("{SELECT_ENVELOPE_BASE}WHERE m.uuid = ?1");
    let based: MessageEnvelope = conn
        .query_row(&sql, [uuid.as_bytes().to_vec()], row_to_envelope)
        .unwrap();
    assert_eq!(based.attachments, refs);
}

async fn eviction_deletes_only_what_the_evicted_message_alone_named(sink: Sink) {
    let f = fixture().await;
    let archive = f.dir.path().join("archive.jsonl");
    let (only, shared, _) = f.two_messages(None).await;
    let charged = f.blobs.usage().charged_bytes;

    let archive_path = (sink == Sink::Archive).then_some(archive.as_path());
    assert_eq!(f.evict(1, sink, archive_path).await, 1);
    // Unredeemable from the commit; the bytes wait for the reap.
    assert!(f.redeem(&only).await.is_none());
    assert!(f.file_exists(&only));
    assert_eq!(f.blobs.usage().charged_bytes, charged);

    assert_eq!(f.reap().await, ReapReport { blobs: 1, bytes: 4 });
    f.assert_gone(&only, charged - 4).await;
    f.assert_readable(&shared, b"shared").await;
    if sink == Sink::Archive {
        let line = std::fs::read_to_string(&archive).unwrap();
        assert!(line.contains(only.as_str()), "archive line: {line}");
    }
}

#[tokio::test]
async fn eviction_with_sink_drop_then_reap() {
    eviction_deletes_only_what_the_evicted_message_alone_named(Sink::Drop).await;
}

#[tokio::test]
async fn eviction_with_sink_archive_then_reap() {
    eviction_deletes_only_what_the_evicted_message_alone_named(Sink::Archive).await;
}

#[tokio::test]
async fn a_withdrawn_parked_message_releases_its_blobs() {
    let f = fixture().await;
    let (only, shared, first) = f.two_messages(Some(now() + Duration::days(1))).await;
    let charged = f.blobs.usage().charged_bytes;
    let id = f.message_row_id(first).await;
    assert!(withdraw_parked_message(&*f.db.lock().await, id, "alice"));
    f.reap().await;
    f.assert_gone(&only, charged - 4).await;
    f.assert_readable(&shared, b"shared").await;
}

#[tokio::test]
async fn a_parked_message_cancelled_through_the_store_releases_its_blobs() {
    let f = fixture().await;
    let (only, shared, first) = f.two_messages(Some(now() + Duration::days(1))).await;
    let charged = f.blobs.usage().charged_bytes;
    assert_eq!(
        f.store.cancel_deferred("alice", first, now()).await,
        crate::store::DeferralOutcome::Applied
    );
    f.reap().await;
    f.assert_gone(&only, charged - 4).await;
    f.assert_readable(&shared, b"shared").await;
}

#[tokio::test]
async fn a_delete_nobody_wrote_a_reap_for_is_collected_by_the_next_reap() {
    let f = fixture().await;
    let (only, shared, first) = f.two_messages(None).await;
    let charged = f.blobs.usage().charged_bytes;
    f.db.lock()
        .await
        .execute(
            "DELETE FROM messaging_messages WHERE uuid = ?1",
            [first.as_bytes().to_vec()],
        )
        .unwrap();
    assert_eq!(f.refs(&only).await, Some(0));
    assert_eq!(f.refs(&shared).await, Some(1));
    f.reap().await;
    f.assert_gone(&only, charged - 4).await;
    f.assert_readable(&shared, b"shared").await;
}

#[tokio::test]
async fn an_update_of_attachments_is_refused() {
    let f = fixture().await;
    let blob = f.sealed(b"x");
    f.store
        .append(message("m", attachments(&[("x", &blob)])))
        .await;
    let err =
        f.db.lock()
            .await
            .execute("UPDATE messaging_messages SET attachments = NULL", [])
            .unwrap_err();
    assert!(err.to_string().contains("written at insert only"), "{err}");
}

#[tokio::test]
#[should_panic(expected = "reap called with a transaction open")]
async fn a_reap_inside_a_transaction_panics() {
    let f = fixture().await;
    let conn = f.db.lock().await;
    let _tx = conn.unchecked_transaction().unwrap();
    f.blobs.reap(&conn);
}

#[tokio::test]
async fn release_and_both_durable_edit_paths_keep_attachments_and_blobs() {
    let f = fixture().await;
    let blob = f.sealed(b"parked");
    let held = attachments(&[("p", &blob)]);
    let refs = held.refs().clone();
    let id = blob.id().clone();
    drop(blob);
    let later = now() + Duration::seconds(60);
    let uuid = f
        .store
        .park(message("parked", held), later)
        .await
        .unwrap()
        .message_uuid;

    assert_eq!(
        f.store
            .edit_deferred("alice", uuid, Some("edited".into()), None, now())
            .await,
        crate::store::DeferralOutcome::Applied
    );
    let row = f.message_row_id(uuid).await;
    let body = "edited again".into();
    update_parked_message(
        &*f.db.lock().await,
        row,
        "alice",
        &EditFieldsApplied {
            body: Some(&body),
            reply_to_uuid: None,
            deliver_after: None,
            delivery_deadline: None,
            urgency: None,
        },
    );
    let view = f.store.deferred_for_sender("alice").await;
    assert_eq!(view[0].envelope.attachments, refs);
    f.reap().await;
    f.assert_readable(&id, b"parked").await;

    let released = f.store.release_due(later).await.released;
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].envelope.attachments, refs);
    f.reap().await;
    f.assert_readable(&id, b"parked").await;
    assert_eq!(f.refs(&id).await, Some(1));
}

#[tokio::test]
async fn a_hold_keeps_a_durable_blob_across_its_last_message_eviction() {
    let f = fixture().await;
    let blob = f.sealed(b"held");
    f.store
        .append(message("first", attachments(&[("h", &blob)])))
        .await;
    f.store
        .append(message("second", HeldAttachments::none()))
        .await;
    f.evict(1, Sink::Drop, None).await;
    assert_eq!(f.reap().await, ReapReport::default());
    // A blob no row names but a hold holds is still redeemed.
    let again = f.redeem(blob.id()).await.expect("held blob redeems");
    assert_eq!(again.read(0, 4).unwrap(), b"held");

    let id = blob.id().clone();
    drop((blob, again));
    assert!(
        f.file_exists(&id),
        "a durable blob's bytes wait for the reap"
    );
    assert!(f.redeem(&id).await.is_none());
    f.reap().await;
    f.assert_gone(&id, 0).await;
}

#[tokio::test]
async fn a_rolled_back_insert_leaves_a_blob_the_next_reap_collects() {
    let f = fixture().await;
    let blob = f.sealed(b"rolled back");
    let held = attachments(&[("r", &blob)]);
    held.make_durable();
    {
        let conn = f.db.lock().await;
        let tx = conn.unchecked_transaction().unwrap();
        insert_message_in_tx(
            &tx,
            None,
            f.channel,
            "node",
            "alice",
            &"m".into(),
            Urgency::Normal,
            ChannelScheme::Brenn,
            None,
            None,
            None,
            None,
            None,
            &held,
            0,
        );
        tx.rollback().unwrap();
    }
    assert_eq!(f.refs(blob.id()).await, None);
    // Still held: the rollback took nothing from the producer.
    f.assert_readable(blob.id(), b"rolled back").await;

    let id = blob.id().clone();
    drop((blob, held));
    assert!(f.file_exists(&id));
    assert!(matches!(f.blobs.retain(&id), Retain::Unsettled));
    assert!(f.redeem(&id).await.is_none());
    assert_eq!(f.reap().await.blobs, 1);
    f.assert_gone(&id, 0).await;
}

#[tokio::test]
async fn a_ring_held_blob_redeems_without_the_database_lock() {
    let f = fixture().await;
    let blob = f.sealed(b"ring");
    let _conn = f.db.lock().await;
    let Retain::Held(again) = f.blobs.retain(blob.id()) else {
        panic!("a held blob redeems from the index alone");
    };
    assert_eq!(again.read(0, 4).unwrap(), b"ring");
}

#[tokio::test]
#[should_panic(expected = "did not open against this database")]
async fn a_hold_from_another_store_cannot_be_adopted() {
    let f = fixture().await;
    let other_dir = tempfile::tempdir().unwrap();
    let other_conn = Connection::open_in_memory().unwrap();
    crate::db::run_slice_migrations(&other_conn);
    let other = BlobStore::open(other_dir.path(), LIMITS, &other_conn);
    let mut w = other.create("text/plain").unwrap();
    w.write(0, b"x").unwrap();
    let hold = w.seal().unwrap();
    f.store
        .append(message("m", attachments(&[("x", &hold)])))
        .await;
}

// ---------------------------------------------------------------------------
// Open: restart and the refusals
// ---------------------------------------------------------------------------

impl Fixture {
    /// Reopen the blob store over the same database and directory, as a boot
    /// after a crash would: nothing of the previous store is dropped, and its
    /// directory lock is released as the crashed process's death releases it.
    async fn restart(self) -> Fixture {
        self.restart_with(LIMITS).await
    }

    async fn restart_with(self, limits: BlobLimits) -> Fixture {
        self.blobs.release_dir_lock();
        std::mem::forget(self.blobs);
        let blobs = BlobStore::open(&blob_dir(&self.dir), limits, &*self.db.lock().await);
        Fixture { blobs, ..self }
    }

    async fn sql(&self, sql: &str) {
        self.db.lock().await.execute_batch(sql).unwrap();
    }
}

#[tokio::test]
async fn restart_keeps_durable_blobs_and_sweeps_everything_else() {
    let f = fixture().await;
    let durable = f.sealed(b"durable");
    let durable_id = durable.id().clone();
    f.store
        .append(message("m", attachments(&[("d", &durable)])))
        .await;
    let unpublished = f.sealed(b"unpublished");
    let unpublished_id = unpublished.id().clone();
    let mut unsealed = f.blobs.create("text/plain").unwrap();
    unsealed.write(0, b"half").unwrap();
    std::mem::forget((durable, unpublished, unsealed));
    assert_eq!(blob_files(&f.dir).len(), 3);

    let f = f.restart().await;
    f.assert_readable(&durable_id, b"durable").await;
    assert!(!f.file_exists(&unpublished_id));
    assert_eq!(blob_files(&f.dir), vec![durable_id.as_str().to_owned()]);
    assert!(
        owner_path(&f.dir).is_file(),
        "the sweep keeps the owner record"
    );
    assert_eq!(f.blobs.usage().charged_bytes, 7);
    assert_eq!(f.blobs.usage().sealed, 1);
}

#[tokio::test]
async fn restart_sweeps_a_blob_whose_last_row_went_before_a_reap() {
    let f = fixture().await;
    let (only, shared, first) = f.two_messages(None).await;
    f.sql(&format!(
        "DELETE FROM messaging_messages WHERE uuid = x'{}'",
        first.simple()
    ))
    .await;
    let f = f.restart().await;
    assert!(!f.file_exists(&only));
    assert_eq!(f.refs(&only).await, None);
    f.assert_readable(&shared, b"shared").await;
}

/// A fixture holding one durable message naming one blob, every hold dropped.
async fn one_durable_blob() -> (Fixture, BlobId) {
    let f = fixture().await;
    let blob = f.sealed(b"bytes");
    f.store
        .append(message("m", attachments(&[("b", &blob)])))
        .await;
    let id = blob.id().clone();
    drop(blob);
    (f, id)
}

#[tokio::test]
async fn a_store_opened_over_a_lowered_budget_refuses_creates_until_under_it() {
    let (f, id) = one_durable_blob().await;
    let f = f
        .restart_with(BlobLimits {
            max_blob_bytes: 4,
            budget_bytes: 4,
        })
        .await;
    assert_eq!(f.blobs.usage().charged_bytes, 5);
    assert!(matches!(
        f.blobs.create("text/plain"),
        Err(BlobError::BudgetExhausted { budget: 4 })
    ));
    assert_eq!(blob_files(&f.dir).len(), 1);

    f.sql("DELETE FROM messaging_messages").await;
    assert_eq!(f.reap().await, ReapReport { blobs: 1, bytes: 5 });
    assert!(!f.file_exists(&id));
    let mut w = f.blobs.create("text/plain").unwrap();
    w.write(0, b"four").unwrap();
    w.seal().unwrap();
}

#[tokio::test]
#[should_panic(expected = "has no file in the blob directory")]
async fn open_refuses_a_referenced_blob_with_no_file() {
    let (f, id) = one_durable_blob().await;
    std::fs::remove_file(blob_dir(&f.dir).join(id.as_str())).unwrap();
    f.restart().await;
}

#[tokio::test]
#[should_panic(expected = "has a file of 3 bytes; its row says 5")]
async fn open_refuses_a_referenced_blob_of_the_wrong_length() {
    let (f, id) = one_durable_blob().await;
    std::fs::write(blob_dir(&f.dir).join(id.as_str()), b"abc").unwrap();
    f.restart().await;
}

#[tokio::test]
#[should_panic(expected = "which is not a blob file")]
async fn open_refuses_an_unrecognised_directory_entry() {
    let (f, _) = one_durable_blob().await;
    std::fs::write(blob_dir(&f.dir).join("notes.txt"), b"").unwrap();
    f.restart().await;
}

#[tokio::test]
#[should_panic(expected = "records 2 references and 1 reference rows")]
async fn open_refuses_a_count_that_disagrees() {
    let (f, _) = one_durable_blob().await;
    f.sql("UPDATE messaging_blobs SET refs = 2").await;
    f.restart().await;
}

#[tokio::test]
#[should_panic(expected = "which has no reference row")]
async fn open_refuses_a_named_blob_with_no_reference_row() {
    let (f, _) = one_durable_blob().await;
    f.sql("DELETE FROM messaging_blob_refs").await;
    f.restart().await;
}

#[test]
#[should_panic(expected = "below FULL")]
fn open_refuses_synchronous_below_full() {
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open_in_memory().unwrap();
    crate::db::run_slice_migrations(&conn);
    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    BlobStore::open(&blob_dir(&dir), LIMITS, &conn);
}

/// The open sweep deletes every file its database does not name, so a second
/// store over a directory a live store holds is refused before it lists the
/// directory, and the live store's blob is still there.
#[tokio::test]
async fn open_refuses_a_directory_another_live_store_holds() {
    let (f, id) = one_durable_blob().await;
    let other_conn = Connection::open_in_memory().unwrap();
    crate::db::run_slice_migrations(&other_conn);
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        BlobStore::open(&blob_dir(&f.dir), LIMITS, &other_conn)
    }))
    .expect_err("a second store over a held directory must be refused");
    let message = refused
        .downcast_ref::<String>()
        .expect("the refusal is a formatted panic");
    assert!(
        message.contains("is held by another open blob store"),
        "{message}"
    );
    assert!(f.file_exists(&id), "the live store's blob was not swept");
}

/// The lock is the open store's: once it is dropped, the directory opens again.
#[tokio::test]
async fn a_dropped_store_releases_its_directory() {
    let f = fixture().await;
    let Fixture { dir, db, blobs, .. } = f;
    drop(blobs);
    BlobStore::open(&blob_dir(&dir), LIMITS, &*db.lock().await);
}

#[test]
fn open_creates_the_directory_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open_in_memory().unwrap();
    crate::db::run_slice_migrations(&conn);
    BlobStore::open(&blob_dir(&dir), LIMITS, &conn);
    let mode = std::fs::metadata(blob_dir(&dir))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o700);
}

// ---------------------------------------------------------------------------
// The owner record
// ---------------------------------------------------------------------------

fn migrated_conn() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    crate::db::run_slice_migrations(&conn);
    conn
}

fn generation_of(conn: &Connection) -> Uuid {
    read_store_identity(conn).generation
}

/// The panic message of `open`, which must refuse.
fn refusal(open: impl FnOnce() -> Arc<BlobStore>) -> String {
    let Err(refused) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(open)) else {
        panic!("the open must be refused");
    };
    refused
        .downcast_ref::<String>()
        .expect("the refusal is a formatted panic")
        .clone()
}

impl Fixture {
    /// Release the directory lock, as a crashed host's death does, so another
    /// open may be tried over the directory while the fixture is kept.
    fn crash(&self) {
        self.blobs.release_dir_lock();
    }

    async fn generation(&self) -> Uuid {
        generation_of(&*self.db.lock().await)
    }
}

#[tokio::test]
async fn a_fresh_directory_is_claimed_with_the_database_generation() {
    use std::os::unix::fs::PermissionsExt;
    let f = fixture().await;
    let record = std::fs::read_to_string(owner_path(&f.dir)).unwrap();
    assert_eq!(record, format!("{}\n", f.generation().await));
    let mode = std::fs::metadata(owner_path(&f.dir))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
    assert!(!blob_dir(&f.dir).join("owner.tmp").exists());
}

#[tokio::test]
async fn a_reopen_on_the_same_database_keeps_the_record_and_the_blobs() {
    let (f, id) = one_durable_blob().await;
    let record = std::fs::read(owner_path(&f.dir)).unwrap();
    let f = f.restart().await;
    assert_eq!(std::fs::read(owner_path(&f.dir)).unwrap(), record);
    f.assert_readable(&id, b"bytes").await;
}

/// The case the directory lock cannot cover: the owning host is down.
#[tokio::test]
async fn open_refuses_a_directory_another_database_owns_and_deletes_nothing() {
    let (f, id) = one_durable_blob().await;
    let ours = f.generation().await;
    f.crash();
    let other = migrated_conn();
    let theirs = generation_of(&other);
    let message = refusal(|| BlobStore::open(&blob_dir(&f.dir), LIMITS, &other));
    assert!(message.contains(&ours.to_string()), "{message}");
    assert!(message.contains(&theirs.to_string()), "{message}");
    assert!(
        message.contains("point messaging blob_dir elsewhere"),
        "{message}"
    );
    assert!(f.file_exists(&id), "the refusal swept nothing");
    assert_eq!(
        std::fs::read_to_string(owner_path(&f.dir)).unwrap(),
        format!("{ours}\n")
    );
}

#[tokio::test]
async fn open_refuses_blob_files_with_no_owner_record() {
    let (f, id) = one_durable_blob().await;
    std::fs::remove_file(owner_path(&f.dir)).unwrap();
    f.crash();
    let message = refusal(|| {
        let conn = f.db.try_lock().expect("the database is unlocked");
        BlobStore::open(&blob_dir(&f.dir), LIMITS, &conn)
    });
    assert!(
        message.contains("holds 1 blob files and no owner record"),
        "{message}"
    );
    assert!(f.file_exists(&id));
    assert!(!owner_path(&f.dir).exists(), "a refusal claims nothing");
}

/// Records that do not hold exactly one generation line, each refused over a
/// directory with a blob file and over one without.
#[tokio::test]
async fn open_refuses_a_malformed_owner_record() {
    let malformed = |generation: Uuid| -> Vec<String> {
        vec![
            String::new(),
            "\n".to_owned(),
            "not a generation\n".to_owned(),
            generation.to_string(),
            format!("{generation}\n{generation}\n"),
            format!("{}\n", generation.to_string().to_uppercase()),
            format!("{}\n", generation.simple()),
            format!("{generation}\n{}", "x".repeat(100)),
        ]
    };

    let (f, id) = one_durable_blob().await;
    f.crash();
    let generation = f.generation().await;
    for record in malformed(generation) {
        std::fs::write(owner_path(&f.dir), &record).unwrap();
        let message = refusal(|| {
            let conn = f.db.try_lock().expect("the database is unlocked");
            BlobStore::open(&blob_dir(&f.dir), LIMITS, &conn)
        });
        assert!(
            message.contains("does not hold one store generation"),
            "{record:?}: {message}"
        );
        assert!(f.file_exists(&id));
    }

    let dir = tempfile::tempdir().unwrap();
    let conn = migrated_conn();
    for record in malformed(generation_of(&conn)) {
        std::fs::write(dir.path().join("owner"), &record).unwrap();
        let message = refusal(|| BlobStore::open(dir.path(), LIMITS, &conn));
        assert!(
            message.contains("does not hold one store generation"),
            "{record:?}: {message}"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("owner")).unwrap(),
            record
        );
    }
}

#[test]
fn open_refuses_an_owner_record_that_is_not_a_regular_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("owner")).unwrap();
    let conn = migrated_conn();
    let message = refusal(|| BlobStore::open(dir.path(), LIMITS, &conn));
    assert!(message.contains("is not a regular file"), "{message}");
}

/// A restore, or a move, of the database with its own directory keeps them
/// equal: the generation is a row of the one and the record a file of the
/// other.
#[tokio::test]
async fn a_database_and_directory_copied_to_new_paths_boot() {
    let (f, id) = one_durable_blob().await;
    let moved = tempfile::tempdir().unwrap();
    let database = moved.path().join("restored.db");
    f.db.lock()
        .await
        .execute("VACUUM INTO ?1", [database.to_str().unwrap()])
        .unwrap();
    let directory = moved.path().join("restored.blobs");
    std::fs::create_dir(&directory).unwrap();
    for entry in std::fs::read_dir(blob_dir(&f.dir)).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), directory.join(entry.file_name())).unwrap();
    }

    let conn = Connection::open(&database).unwrap();
    conn.pragma_update(None, "synchronous", "FULL").unwrap();
    let blobs = BlobStore::open(&directory, LIMITS, &conn);
    let Retain::Unsettled = blobs.retain(&id) else {
        panic!("a durable blob is settled against the database");
    };
    let hold = blobs
        .retain_settled(&id, &conn)
        .expect("the restored blob redeems");
    assert_eq!(hold.read(0, 5).unwrap(), b"bytes");
}

#[test]
fn a_leftover_temporary_beside_no_record_is_removed_and_the_directory_claimed() {
    for leftover in [String::new(), format!("{}\n", Uuid::new_v4())] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("owner.tmp"), &leftover).unwrap();
        let conn = migrated_conn();
        BlobStore::open(dir.path(), LIMITS, &conn);
        assert!(!dir.path().join("owner.tmp").exists(), "{leftover:?}");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("owner")).unwrap(),
            format!("{}\n", generation_of(&conn))
        );
    }
}

#[tokio::test]
async fn a_leftover_temporary_beside_a_valid_record_is_removed() {
    let (f, id) = one_durable_blob().await;
    let tmp = blob_dir(&f.dir).join("owner.tmp");
    std::fs::write(&tmp, b"torn").unwrap();
    let f = f.restart().await;
    assert!(!tmp.exists());
    f.assert_readable(&id, b"bytes").await;
}

#[tokio::test]
async fn a_leftover_temporary_beside_a_foreign_record_survives_the_refusal() {
    let (f, _) = one_durable_blob().await;
    let tmp = blob_dir(&f.dir).join("owner.tmp");
    std::fs::write(&tmp, b"torn").unwrap();
    f.crash();
    let other = migrated_conn();
    refusal(|| BlobStore::open(&blob_dir(&f.dir), LIMITS, &other));
    assert_eq!(std::fs::read(&tmp).unwrap(), b"torn");
}

#[test]
fn a_temporary_that_is_not_a_regular_file_is_a_stranger() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("owner.tmp")).unwrap();
    let conn = migrated_conn();
    let message = refusal(|| BlobStore::open(dir.path(), LIMITS, &conn));
    assert!(message.contains("which is not a blob file"), "{message}");
    assert!(!dir.path().join("owner").exists());
}

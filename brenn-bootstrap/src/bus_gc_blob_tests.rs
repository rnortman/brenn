//! What one pass of the hourly bus GC does to the blobs its evictions leave
//! unheld: the eviction's trigger releases the rows' references and the pass's
//! closing reap deletes the bytes.

use std::sync::Arc;

use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
use brenn_lib::messaging::{BlobId, ChannelEntry, ChannelScheme, WakeMin, canonical_address};
use brenn_messaging::Messenger;
use brenn_messaging::testutils::{
    TempBlobDir, blob_messenger, publish_attached, redeem_all, seal_blob,
};
use brenn_messaging_store::blob::BlobError;
use brenn_server::test_support::init_db_memory;
use uuid::Uuid;

use crate::shutdown::bus_gc_pass;

const PUBLISHER: &str = "gc-blob-producer";

/// A durable channel keeping one message: every pass evicts all but the
/// newest.
fn keep_one(name: &str, sink: Sink) -> ChannelEntry {
    ChannelEntry {
        uuid: Uuid::new_v4(),
        address: canonical_address(name),
        description: None,
        resolved_channel: ResolvedChannel {
            send_rate: Default::default(),
            push_depth: Depth::Bounded(1),
            retain_depth: Depth::Bounded(1),
            standing_retain_depth: Depth::Bounded(1),
            noise: NoiseLevel::Silent,
            sink,
            wake_min: WakeMin::Normal,
            non_text: false,
            untyped_content_type: None,
        },
        subscribers: vec![],
        transport_type: ChannelScheme::Brenn,
        mount: None,
    }
}

fn charged(m: &Messenger) -> u64 {
    m.blob_store().unwrap().usage().charged_bytes
}

/// Two messages on a keep-one channel: the older names `only` and `shared`,
/// the newer names `shared`. Returns the two ids once every producer hold is
/// gone, so the rows are the blobs' only holders.
async fn two_messages(m: &Arc<Messenger>, addr: &str) -> (BlobId, BlobId) {
    let only = seal_blob(m, b"evicted with its message");
    let shared = seal_blob(m, b"still named");
    let ids = (only.id().clone(), shared.id().clone());
    let older = [("photo", &only), ("doc", &shared)];
    publish_attached(m, PUBLISHER, addr, "older", &older, None).await;
    publish_attached(m, PUBLISHER, addr, "newer", &[("doc", &shared)], None).await;
    drop((only, shared));
    ids
}

/// One eviction pass over `sink`: the blob only the evicted message named is
/// deleted, its charge returned and its redemption refused; the blob the kept
/// message also names stays readable.
async fn an_eviction_pass_deletes_only_the_unheld_blob(
    sink: Sink,
    archive: Option<&std::path::Path>,
) -> (BlobId, BlobId) {
    let blobs = TempBlobDir::new();
    let entry = keep_one("gc-blobs", sink);
    let addr = entry.address.clone();
    let db = init_db_memory();
    let m = blob_messenger(db.clone(), vec![entry], PUBLISHER, blobs.path()).await;
    let (only, shared) = two_messages(&m, &addr).await;
    let shared_size = u64::try_from(b"still named".len()).unwrap();

    let pass = bus_gc_pass(&db, &m, archive).await;

    assert_eq!(pass.messages_evicted, 1);
    assert_eq!(pass.reaped.blobs, 1);
    assert_eq!(
        pass.reaped.bytes,
        u64::try_from(b"evicted with its message".len()).unwrap()
    );
    assert!(
        !blobs.holds_file(&only),
        "the evicted message's blob is unlinked"
    );
    assert!(matches!(
        redeem_all(&m, &only).await,
        Err(BlobError::NotFound)
    ));
    assert_eq!(charged(&m), shared_size);
    assert_eq!(redeem_all(&m, &shared).await.unwrap(), b"still named");
    assert!(blobs.holds_file(&shared));

    let again = bus_gc_pass(&db, &m, archive).await;
    assert_eq!((again.messages_evicted, again.reaped.blobs), (0, 0));
    (only, shared)
}

#[tokio::test]
async fn an_eviction_pass_with_sink_drop_deletes_the_blob_only_the_evicted_message_named() {
    an_eviction_pass_deletes_only_the_unheld_blob(Sink::Drop, None).await;
}

/// The archive line keeps the evicted message's references; its bytes go with
/// the message.
#[tokio::test]
async fn an_eviction_pass_with_sink_archive_records_the_references_and_deletes_the_bytes() {
    let archive = tempfile::NamedTempFile::new().expect("archive file");
    let (only, shared) =
        an_eviction_pass_deletes_only_the_unheld_blob(Sink::Archive, Some(archive.path())).await;

    let line = std::fs::read_to_string(archive.path()).expect("read archive");
    let record: serde_json::Value = serde_json::from_str(line.trim()).expect("one JSON line");
    assert_eq!(record["body"], "older");
    assert_eq!(record["attachments"]["photo"]["id"], only.as_str());
    assert_eq!(record["attachments"]["doc"]["id"], shared.as_str());
    assert_eq!(
        record["attachments"]["photo"]["content_type"],
        "application/octet-stream"
    );
}

/// Rows of a durable channel the configuration no longer declares are never
/// evicted, so their blobs outlive every pass: a restart over the same
/// database and blob directory with the channel gone keeps both.
#[tokio::test]
async fn rows_of_a_channel_absent_from_the_directory_keep_their_blobs_across_a_pass() {
    let blobs = TempBlobDir::new();
    let db = init_db_memory();
    let dropped = keep_one("gc-dropped", Sink::Drop);
    let addr = dropped.address.clone();
    let first = blob_messenger(db.clone(), vec![dropped], PUBLISHER, blobs.path()).await;
    let (only, shared) = two_messages(&first, &addr).await;
    drop(first);

    let kept = keep_one("gc-kept", Sink::Drop);
    let m = blob_messenger(db.clone(), vec![kept], PUBLISHER, blobs.path()).await;
    let pass = bus_gc_pass(&db, &m, None).await;

    assert_eq!((pass.messages_evicted, pass.reaped.blobs), (0, 0));
    assert_eq!(
        redeem_all(&m, &only).await.unwrap(),
        b"evicted with its message"
    );
    assert_eq!(redeem_all(&m, &shared).await.unwrap(), b"still named");
}

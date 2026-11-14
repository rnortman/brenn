//! Attachments delivered to a 0.2.0 component: host code publishes a message
//! naming two blobs through `publish_from_system_with_attachments`, and a
//! `processor-bytes-test` consumer reports the header it was handed, which
//! carries both references in full. Both stores, immediate and parked then
//! released.

use super::non_text::{
    activate, attach_subscribers, bytes_consumer, durable, input, one_report, output,
    report_channel, unattached_messenger, window,
};
use super::*;

use brenn_lib::messaging::BlobRef;
use brenn_messaging::testutils::{
    TempBlobDir, covering_system_publisher, install_blob_store_at, publish_attached, seal_blob,
};
use brenn_messaging_store::blob::BlobHold;

/// The host component that publishes the attachments.
const PUBLISHER: &str = "att-producer";

/// The channel the consumer `slug` reads on its `feed` port: `brenn:` when
/// `durable_store`, `ephemeral:` otherwise.
fn feed_channel(name: &str, slug: &str, durable_store: bool) -> ChannelEntry {
    let unbounded = (Depth::Unbounded, Depth::Unbounded);
    if durable_store {
        return durable(
            &format!("brenn:{name}"),
            false,
            &[(slug, unbounded.0, unbounded.1)],
        );
    }
    let mut entry = testutils::ephemeral_channel_entry(name, 16);
    entry.subscribers = vec![SubscriberEntry {
        kind: SubscriberEntryKind::Wasm(slug.to_string()),
        push_depth: unbounded.0,
        retain_depth: unbounded.1,
        noise: NoiseLevel::Silent,
        wake_min: None,
    }];
    entry
}

struct Case {
    messenger: Arc<brenn_messaging::Messenger>,
    consumer: WasmConsumerConfig,
    feed: ChannelEntry,
    report: ChannelEntry,
    photo: BlobHold,
    notes: BlobHold,
    _blobs: TempBlobDir,
}

/// A consumer `slug` reading one feed channel on the given store, a blob store,
/// and two sealed blobs.
async fn case(slug: &str, durable_store: bool) -> Case {
    let feed = feed_channel(&format!("{slug}-feed"), slug, durable_store);
    let report = report_channel(&format!("brenn:{slug}-report"));
    let entries = [feed.clone(), report.clone()];
    let blobs = TempBlobDir::new();
    let messenger = install_blob_store_at(
        unattached_messenger(&entries, covering_system_publisher(PUBLISHER)).await,
        blobs.path(),
    )
    .await;
    let messenger = attach_subscribers(messenger, &entries).await;
    let consumer = bytes_consumer(
        slug,
        &messenger,
        vec![input(slug, "feed", &feed)],
        vec![output("report", &report)],
    );
    let photo = seal_blob(&messenger, b"jpeg bytes");
    let notes = seal_blob(&messenger, b"some notes");
    Case {
        messenger,
        consumer,
        feed,
        report,
        photo,
        notes,
        _blobs: blobs,
    }
}

impl Case {
    async fn publish(&self, deliver_after: Option<chrono::DateTime<chrono::Utc>>) {
        publish_attached(
            &self.messenger,
            PUBLISHER,
            &self.feed.address,
            "see attached",
            &[("photo", &self.photo), ("notes", &self.notes)],
            deliver_after,
        )
        .await;
    }

    /// The `attachments` key of the one header the consumer was handed on
    /// its feed at its next activation.
    async fn delivered_attachments(&self) -> serde_json::Value {
        activate(&self.consumer).await;
        let report = one_report(&self.messenger, &self.report.address).await;
        let feed = window(&report, "feed");
        assert_eq!(feed.len(), 1, "{}: {report}", self.feed.address);
        assert_eq!(feed[0]["hex"], hex(b"see attached"));
        feed[0]["header"]["attachments"].clone()
    }

    /// Both references, as the header writes them in full.
    fn expected(&self) -> serde_json::Value {
        let full = |r: BlobRef| {
            serde_json::json!({
                "id": r.id.as_str(),
                "size": r.size,
                "content_type": r.content_type,
            })
        };
        serde_json::json!({
            "notes": full(self.notes.reference()),
            "photo": full(self.photo.reference()),
        })
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A message published with two attachments reaches a 0.2.0 consumer on a
/// `brenn:` channel and on an `ephemeral:` channel with both references, id
/// included, in its header.
#[tokio::test]
async fn a_0_2_consumer_is_handed_both_references_on_either_store() {
    for (slug, durable_store) in [("att-durable", true), ("att-ring", false)] {
        let case = case(slug, durable_store).await;
        case.publish(None).await;
        assert_eq!(
            case.delivered_attachments().await,
            case.expected(),
            "{}",
            case.feed.address
        );
    }
}

/// The same publish parked and then released delivers both references, on
/// either store; nothing is owed before the release.
#[tokio::test]
async fn a_parked_publish_is_handed_its_references_on_release_on_either_store() {
    for (slug, durable_store) in [("att-park-durable", true), ("att-park-ring", false)] {
        let case = case(slug, durable_store).await;
        let release_at = chrono::Utc::now() + chrono::Duration::hours(1);
        case.publish(Some(release_at)).await;
        let owed =
            testutils::owed_everywhere(&case.messenger, &ParticipantId::for_wasm(slug)).await;
        assert!(
            owed.is_empty(),
            "{}: parked, owed {owed:?}",
            case.feed.address
        );

        case.messenger
            .release_due_messages(release_at + chrono::Duration::seconds(1))
            .await;
        assert_eq!(
            case.delivered_attachments().await,
            case.expected(),
            "{}",
            case.feed.address
        );
    }
}

//! The page's plane owner: the size behind each live handle, and answers split
//! by tag.

use brenn_attach_proto::{BlobCreateOutcome, BlobOpenOutcome};

use crate::test_support::pages;

use super::*;

fn door() -> BlobFacts {
    BlobFacts {
        max_blob_bytes: 100,
        max_chunk_bytes: 16,
        max_handles: 3,
        max_bytes_held: 150,
        max_inflight_reads: 2,
        upload: true,
    }
}

fn attached() -> PagePlane {
    let mut plane = PagePlane::new();
    plane.on_attached(&AttachmentFacts {
        blobs: Some(door()),
        ..pages::facts()
    });
    plane
}

fn photo() -> AttachmentName {
    AttachmentName::new("photo").unwrap()
}

#[test]
fn every_live_handle_counts_its_declared_or_delivered_size() {
    let mut plane = attached();
    assert!(plane.has_room(3, 150));
    let (made, _) = plane.create(UploadId(1), "text/plain", 60).unwrap();
    let (forwarded, _) = plane
        .open(
            PlaneTag::Upload(UploadId(2)),
            "brenn:in",
            Uuid::from_u128(1),
            photo(),
            50,
        )
        .unwrap();
    assert_eq!(plane.handles_in_use(), 2);
    assert_eq!(plane.bytes_in_use(), 110);
    assert!(plane.has_room(1, 40));
    assert!(!plane.has_room(1, 41), "bytes held");
    assert!(!plane.has_room(2, 0), "handles");
    let close = plane.close(made);
    assert!(matches!(close[..], [ClientFrame::BlobClose { .. }]));
    assert_eq!(plane.bytes_in_use(), 50);
    // A refusal gives the handle up.
    let answer = plane
        .on_frame(ServerFrame::BlobOpenResult {
            handle: forwarded,
            outcome: BlobOpenOutcome::NotRetained,
        })
        .unwrap();
    assert_eq!(answer.upload.len(), 1);
    assert_eq!(plane.handles_in_use(), 0);
}

#[test]
fn a_detach_forgets_every_handle_and_hands_on_no_event() {
    let mut plane = attached();
    plane.create(UploadId(1), "text/plain", 10).unwrap();
    plane.hold_fetches(true);
    plane.on_detached();
    assert_eq!(plane.handles_in_use(), 0);
    assert_eq!(plane.door(), None);
    assert!(!plane.fetches_held());
    assert!(!plane.has_room(0, 0), "no door, no room");
}

#[test]
fn answers_go_to_the_user_their_tag_names() {
    let mut plane = attached();
    let (upload, _) = plane.create(UploadId(7), "text/plain", 10).unwrap();
    let answer = plane
        .on_frame(ServerFrame::BlobCreateResult {
            handle: upload,
            outcome: BlobCreateOutcome::StorageFailed,
        })
        .unwrap();
    assert!(answer.fetch.is_empty());
    assert!(matches!(
        answer.upload[..],
        [BlobEvent::UploadRefused {
            tag: UploadId(7),
            ..
        }]
    ));
    let (fetch, _) = plane
        .open(
            PlaneTag::Fetch(FetchId::for_tests(3)),
            "brenn:in",
            Uuid::from_u128(1),
            photo(),
            20,
        )
        .unwrap();
    let answer = plane
        .on_frame(ServerFrame::BlobOpenResult {
            handle: fetch,
            outcome: BlobOpenOutcome::Ok {
                size: 20,
                content_type: "image/png".to_string(),
            },
        })
        .unwrap();
    assert!(answer.upload.is_empty());
    assert!(matches!(
        answer.fetch[..],
        [BlobEvent::Opened { tag, size: 20, .. }] if tag == FetchId::for_tests(3)
    ));
}

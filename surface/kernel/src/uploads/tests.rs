//! The stage driven against a real page plane, the server's blob answers
//! scripted frame by frame and the outbox's idleness stated by each test.

use brenn_attach_client::conn::AttachmentFacts;
use brenn_attach_proto::{
    AttachmentName, BlobCreateOutcome, BlobOpenOutcome, BlobSealOutcome, ServerFrame,
};
use brenn_envelope::{BlobId, BlobRef};
use uuid::Uuid;

use crate::blob_scope::{BlobScope, Remote, largest_made_blob};
use crate::page_bytes::MadeLedger;
use crate::test_support::pages;

use super::*;

const WIRE: &str = "brenn:site.bar.out";
const ORIGIN: &str = "brenn:site.bar.in";
const CHUNK: u64 = 4;

fn door() -> BlobFacts {
    BlobFacts {
        max_blob_bytes: 64,
        max_chunk_bytes: CHUNK,
        max_handles: 4,
        max_bytes_held: 128,
        max_inflight_reads: 2,
        upload: true,
    }
}

fn plane_with(door: BlobFacts) -> PagePlane {
    let mut plane = PagePlane::new();
    plane.on_attached(&AttachmentFacts {
        blobs: Some(door),
        ..pages::facts()
    });
    plane
}

fn ready(stage: &mut Stage, plane: &mut PagePlane) -> StageStep {
    stage.on_ready(plane, Idle(true))
}

fn name(name: &str) -> AttachmentName {
    AttachmentName::new(name).expect("a fixture attachment name")
}

/// Page bytes, made through a scope as a component makes them.
fn made(ledger: &MadeLedger, bytes: &[u8]) -> Blob {
    let mut scope = BlobScope::empty();
    let writer = scope.create(ledger, "application/octet-stream").unwrap();
    scope
        .write(writer, 0, bytes, largest_made_blob(None))
        .unwrap()
        .unwrap();
    let blob = scope.seal(writer).unwrap().unwrap();
    scope.blob(blob).unwrap().clone()
}

fn remote(size: u64) -> Blob {
    Blob::Remote(Remote {
        channel: ORIGIN.to_string(),
        message_id: Uuid::from_u128(9),
        name: name("photo"),
        blob: BlobRef {
            id: BlobId::new("b9".to_string()),
            size,
            content_type: "image/png".to_string(),
        },
    })
}

fn entry(body: &str, attachments: &[(&str, &Blob)]) -> StagedEntry {
    StagedEntry {
        channel: WIRE.to_string(),
        body: body.into(),
        urgency: Urgency::Normal,
        deliver_after: None,
        attachments: attachments
            .iter()
            .map(|(n, blob)| BufferedAttachment {
                name: name(n),
                blob: (*blob).clone(),
            })
            .collect(),
    }
}

fn flush(entries: Vec<StagedEntry>) -> StagedFlush {
    StagedFlush {
        entries,
        ops: Vec::new(),
    }
}

fn answer(stage: &mut Stage, plane: &mut PagePlane, frame: ServerFrame) -> StageStep {
    let answer = plane.on_frame(frame).unwrap();
    assert!(answer.fetch.is_empty());
    let mut step = stage.on_events(plane, answer.upload, Idle(true)).unwrap();
    step.frames.splice(0..0, answer.frames);
    step
}

/// Every chunk the stage composes for each upload in progress, until it
/// composes none.
fn drain(stage: &Stage, plane: &mut PagePlane) -> Vec<ClientFrame> {
    let mut frames = Vec::new();
    for handle in stage.uploading_handles() {
        while let Some(chunk) = stage.next_chunk_frames(handle, plane) {
            frames.extend(chunk);
        }
    }
    frames
}

fn creates(frames: &[ClientFrame]) -> Vec<(BlobHandle, u64)> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            ClientFrame::BlobCreate { handle, size, .. } => Some((*handle, *size)),
            _ => None,
        })
        .collect()
}

fn opens(frames: &[ClientFrame]) -> Vec<BlobHandle> {
    frames
        .iter()
        .filter_map(|frame| match frame {
            ClientFrame::BlobOpen { handle, .. } => Some(*handle),
            _ => None,
        })
        .collect()
}

fn created(handle: BlobHandle) -> ServerFrame {
    ServerFrame::BlobCreateResult {
        handle,
        outcome: BlobCreateOutcome::Ok,
    }
}

fn sealed(handle: BlobHandle) -> ServerFrame {
    ServerFrame::BlobSealResult {
        handle,
        outcome: BlobSealOutcome::Ok,
    }
}

fn opened(handle: BlobHandle, size: u64) -> ServerFrame {
    ServerFrame::BlobOpenResult {
        handle,
        outcome: BlobOpenOutcome::Ok {
            size,
            content_type: "image/png".to_string(),
        },
    }
}

/// Upload page bytes through to the handed batch: answers the create and the
/// seal, and answers the handle.
fn upload_to_handed(stage: &mut Stage, plane: &mut PagePlane, start: &StageStep) -> StageStep {
    let [(handle, _)] = creates(&start.frames)[..] else {
        panic!("one create: {:?}", start.frames)
    };
    drain(stage, plane);
    answer(stage, plane, created(handle));
    answer(stage, plane, sealed(handle))
}

fn attachment_handles(batch: &FlushBatch) -> Vec<Vec<(String, BlobHandle)>> {
    batch
        .entries
        .iter()
        .map(|entry| {
            entry
                .attachments
                .iter()
                .map(|a| (a.name.as_str().to_string(), a.handle))
                .collect()
        })
        .collect()
}

#[test]
fn a_plain_flush_with_nothing_staged_goes_straight_to_the_outbox() {
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    let step = stage.push(
        "p1",
        flush(vec![entry("hi", &[])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert_eq!(step.handed.len(), 1);
    assert_eq!(step.handed[0].0, "p1");
    assert!(step.frames.is_empty());
    assert!(!stage.holds("p1"));
}

#[test]
fn page_bytes_are_uploaded_chunk_by_chunk_and_the_batch_names_one_handle_per_blob() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"0123456789");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![
            entry("one", &[("a", &blob), ("b", &blob)]),
            entry("two", &[("c", &blob)]),
        ]),
        4,
        &mut plane,
        Idle(true),
    );
    let [(handle, 10)] = creates(&start.frames)[..] else {
        panic!("one create of ten bytes: {:?}", start.frames)
    };
    assert!(start.handed.is_empty(), "nothing is handed before the seal");
    // One chunk at a time, in order, the seal behind the last.
    let first = stage.next_chunk_frames(handle, &mut plane).unwrap();
    assert!(matches!(
        &first[..],
        [ClientFrame::BlobWrite { offset: 0, bytes, .. }] if bytes == b"0123"
    ));
    let rest = drain(&stage, &mut plane);
    assert_eq!(rest.len(), 3, "two writes and the seal: {rest:?}");
    assert!(matches!(rest[2], ClientFrame::BlobSeal { .. }));
    assert!(
        answer(&mut stage, &mut plane, created(handle))
            .handed
            .is_empty()
    );
    let step = answer(&mut stage, &mut plane, sealed(handle));
    let [(instance, batch)] = &step.handed[..] else {
        panic!("one batch handed: {step:?}")
    };
    assert_eq!(instance, "p1");
    assert_eq!(
        attachment_handles(batch),
        vec![
            vec![("a".to_string(), handle), ("b".to_string(), handle)],
            vec![("c".to_string(), handle)]
        ]
    );
    assert_eq!(plane.handles_in_use(), 1);
    assert_eq!(plane.bytes_in_use(), 10);
}

#[test]
fn a_forwarded_attachment_is_opened_and_nothing_is_written() {
    let forwarded = remote(40);
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("fwd", &[("photo", &forwarded)])]),
        4,
        &mut plane,
        Idle(true),
    );
    let [handle] = opens(&start.frames)[..] else {
        panic!("one open: {:?}", start.frames)
    };
    assert!(creates(&start.frames).is_empty());
    assert!(stage.next_chunk_frames(handle, &mut plane).is_none());
    assert_eq!(plane.bytes_in_use(), 40, "the reference's size is recorded");
    let step = answer(&mut stage, &mut plane, opened(handle, 40));
    assert_eq!(step.handed.len(), 1);
}

#[test]
fn an_open_answer_contradicting_the_reference_is_fatal() {
    let forwarded = remote(40);
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("fwd", &[("photo", &forwarded)])]),
        4,
        &mut plane,
        Idle(true),
    );
    let handle = opens(&start.frames)[0];
    let answer = plane.on_frame(opened(handle, 41)).unwrap();
    assert!(
        stage
            .on_events(&mut plane, answer.upload, Idle(true))
            .is_err()
    );
}

#[test]
fn later_flushes_wait_behind_an_attachment_flush_and_leave_in_completion_order() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"xy");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("first", &[("a", &blob)])]),
        4,
        &mut plane,
        Idle(true),
    );
    let behind = stage.push(
        "p1",
        flush(vec![entry("second", &[])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert!(
        behind.handed.is_empty(),
        "staged behind the attachment flush"
    );
    let other = stage.push(
        "p2",
        flush(vec![entry("other", &[])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert_eq!(other.handed.len(), 1, "another instance is not held back");
    let handed = upload_to_handed(&mut stage, &mut plane, &start);
    assert_eq!(handed.handed.len(), 1);
    // The outbox is busy with it, so nothing moves on a refusal.
    let refused = stage.on_batch_answer(
        "p1",
        PublishBatchOutcome::RateLimited,
        &mut plane,
        Idle(false),
    );
    assert!(refused.handed.is_empty());
    let done = stage.on_batch_answer("p1", PublishBatchOutcome::Ok, &mut plane, Idle(true));
    let [(instance, batch)] = &done.handed[..] else {
        panic!("the plain flush behind it: {done:?}")
    };
    assert_eq!(instance, "p1");
    assert_eq!(batch.entries[0].body.as_text(), Some("second"));
    assert!(!stage.holds("p1"));
}

#[test]
fn an_upload_starts_only_when_ready_and_the_outbox_is_idle() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"xy");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    let step = stage.push(
        "p1",
        flush(vec![entry("m", &[("a", &blob)])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert!(step.frames.is_empty(), "no document applied yet");
    let step = stage.on_ready(&mut plane, Idle(false));
    assert!(step.frames.is_empty(), "the outbox is busy");
    let step = stage.advance(&mut plane, Idle(true));
    assert_eq!(creates(&step.frames).len(), 1);
}

#[test]
fn a_flush_waiting_for_room_holds_fetches_back_until_it_starts() {
    let ledger = MadeLedger::new();
    let big = made(&ledger, &[7; 64]);
    let also_big = made(&ledger, &[8; 64]);
    let mut plane = plane_with(BlobFacts {
        max_bytes_held: 100,
        ..door()
    });
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let first = stage.push(
        "p1",
        flush(vec![entry("a", &[("a", &big)])]),
        4,
        &mut plane,
        Idle(true),
    );
    let second = stage.push(
        "p2",
        flush(vec![entry("b", &[("b", &also_big)])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert!(second.frames.is_empty(), "no room beside p1's upload");
    assert!(plane.fetches_held());
    upload_to_handed(&mut stage, &mut plane, &first);
    stage.on_batch_answer("p1", PublishBatchOutcome::Ok, &mut plane, Idle(true));
    let handle = creates(&first.frames)[0].0;
    // The outbox releases the handle; closing it makes room.
    let _close = plane.close(handle);
    let step = stage.advance(&mut plane, Idle(true));
    assert_eq!(creates(&step.frames).len(), 1);
    assert!(!plane.fetches_held());
}

/// An idle outbox and every flush passing, counting how often `survives` is
/// asked.
#[derive(Clone, Copy)]
struct Counting<'a>(&'a std::cell::Cell<u32>);

impl StageGate for Counting<'_> {
    fn idle(&self, _: &str) -> bool {
        true
    }

    fn survives(&self, _: &str, _: &StagedFlush) -> bool {
        self.0.set(self.0.get() + 1);
        true
    }
}

#[test]
fn a_head_waiting_for_room_is_judged_once_until_a_document_judges_it_again() {
    let ledger = MadeLedger::new();
    let big = made(&ledger, &[7; 64]);
    let also_big = made(&ledger, &[8; 64]);
    let mut plane = plane_with(BlobFacts {
        max_bytes_held: 100,
        ..door()
    });
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let first = stage.push(
        "p1",
        flush(vec![entry("a", &[("a", &big)])]),
        4,
        &mut plane,
        Idle(true),
    );
    let asked = std::cell::Cell::new(0);
    let gate = Counting(&asked);
    let waiting = stage.push(
        "p2",
        flush(vec![entry("b", &[("b", &also_big)])]),
        4,
        &mut plane,
        gate,
    );
    assert!(waiting.frames.is_empty(), "no room beside p1's upload");
    assert_eq!(asked.get(), 1);
    for _ in 0..3 {
        assert!(stage.advance(&mut plane, gate).frames.is_empty());
    }
    assert_eq!(asked.get(), 1, "a waiting head is not judged on every pass");

    // A document judges every held flush again.
    let revalidated =
        stage.revalidate(&mut plane, |instance, flush| gate.survives(instance, flush));
    assert!(revalidated.lost.is_empty());
    assert_eq!(asked.get(), 3, "p1's uploading flush and p2's waiting one");

    upload_to_handed(&mut stage, &mut plane, &first);
    stage.on_batch_answer("p1", PublishBatchOutcome::Ok, &mut plane, gate);
    let _close = plane.close(creates(&first.frames)[0].0);
    let step = stage.advance(&mut plane, gate);
    assert_eq!(
        creates(&step.frames).len(),
        1,
        "p2 starts once there is room"
    );
    assert_eq!(asked.get(), 3);
}

/// An idle outbox, refusing every flush whose first entry's body is `doomed`.
#[derive(Clone, Copy)]
struct Refusing;

impl StageGate for Refusing {
    fn idle(&self, _: &str) -> bool {
        true
    }

    fn survives(&self, _: &str, flush: &StagedFlush) -> bool {
        flush.entries[0].body.as_text() != Some("doomed")
    }
}

#[test]
fn a_head_the_gate_refuses_at_its_start_is_lost_and_the_flush_behind_it_starts() {
    let ledger = MadeLedger::new();
    let doomed = made(&ledger, b"xy");
    let kept = made(&ledger, b"abc");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    // Staged before the stage is ready, so the first judgement is the start
    // check's, not a document's.
    for (body, blob) in [("doomed", &doomed), ("kept", &kept)] {
        let step = stage.push(
            "p1",
            flush(vec![entry(body, &[("a", blob)])]),
            4,
            &mut plane,
            Refusing,
        );
        assert_eq!(step, StageStep::default());
    }

    let step = stage.on_ready(&mut plane, Refusing);

    assert_eq!(
        step.lost,
        vec![Lost {
            instance: "p1".to_string(),
            channels: vec![WIRE.to_string()],
            reason: LossReason::Revalidation,
        }]
    );
    let [(_, 3)] = creates(&step.frames)[..] else {
        panic!("only the kept flush's blob is created: {:?}", step.frames)
    };
    assert_eq!(
        step.frames.len(),
        1,
        "nothing is closed: nothing was minted"
    );
    assert_eq!(stage.lost_count("p1"), 1);
    assert!(stage.holds("p1"), "the kept flush is uploading");
    assert_eq!(plane.handles_in_use(), 1);
}

#[test]
fn a_refused_create_loses_the_flush_whole_and_closes_its_other_handles() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"abc");
    let forwarded = remote(8);
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("m", &[("a", &blob), ("photo", &forwarded)])]),
        4,
        &mut plane,
        Idle(true),
    );
    stage.push(
        "p1",
        flush(vec![entry("next", &[])]),
        4,
        &mut plane,
        Idle(true),
    );
    let created_handle = creates(&start.frames)[0].0;
    let step = answer(
        &mut stage,
        &mut plane,
        ServerFrame::BlobCreateResult {
            handle: created_handle,
            outcome: BlobCreateOutcome::BudgetExhausted,
        },
    );
    assert_eq!(
        step.lost,
        vec![Lost {
            instance: "p1".to_string(),
            channels: vec![WIRE.to_string()],
            reason: LossReason::Upload(UploadRefusal::BudgetExhausted),
        }]
    );
    assert_eq!(opens(&start.frames).len(), 1);
    assert!(
        step.frames
            .iter()
            .any(|frame| matches!(frame, ClientFrame::BlobClose { .. })),
        "the open handle is closed: {:?}",
        step.frames
    );
    assert_eq!(step.handed.len(), 1, "the next flush proceeds");
    assert_eq!(plane.handles_in_use(), 0);
    assert_eq!(stage.lost_count("p1"), 1);
}

#[test]
fn a_flush_the_door_can_never_take_is_lost() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, &[1; 65]);
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let step = stage.push(
        "p1",
        flush(vec![entry("m", &[("a", &blob)])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert_eq!(step.lost[0].reason, LossReason::DoorTooSmall);
    let small = made(&ledger, b"ok");
    let mut reader = plane_with(BlobFacts {
        upload: false,
        ..door()
    });
    let step = stage.push(
        "p1",
        flush(vec![entry("m", &[("a", &small)])]),
        4,
        &mut reader,
        Idle(true),
    );
    assert_eq!(step.lost[0].reason, LossReason::DoorTooSmall);
    assert!(step.frames.is_empty());
}

#[test]
fn a_detach_mid_upload_returns_the_flush_to_staged_and_it_uploads_again() {
    let ledger = MadeLedger::new();
    let one = made(&ledger, b"one");
    let two = made(&ledger, b"two");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("m", &[("a", &one), ("b", &two)])]),
        4,
        &mut plane,
        Idle(true),
    );
    let [(first, _), _] = creates(&start.frames)[..] else {
        panic!("two creates: {:?}", start.frames)
    };
    drain(&stage, &mut plane);
    answer(&mut stage, &mut plane, created(first));
    answer(&mut stage, &mut plane, sealed(first));
    plane.on_detached();
    let step = stage.on_detached(&mut Vec::new());
    assert!(step.lost.is_empty());
    assert_eq!(plane.handles_in_use(), 0);
    plane.on_attached(&AttachmentFacts {
        blobs: Some(door()),
        ..pages::facts()
    });
    let again = ready(&mut stage, &mut plane);
    assert_eq!(creates(&again.frames).len(), 2, "both are created again");
}

#[test]
fn a_detach_tells_a_parked_handed_flush_from_one_on_the_wire() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"z");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("m", &[("a", &blob)])]),
        4,
        &mut plane,
        Idle(true),
    );
    upload_to_handed(&mut stage, &mut plane, &start);
    // Re-parked after a refusal: the outbox drops it at the detach.
    let mut dropped = vec!["p1".to_string()];
    plane.on_detached();
    let step = stage.on_detached(&mut dropped);
    assert!(step.lost.is_empty());
    assert!(dropped.is_empty(), "the stage took the drop as its own");
    assert!(stage.holds("p1"));
    plane.on_attached(&AttachmentFacts {
        blobs: Some(door()),
        ..pages::facts()
    });
    let again = ready(&mut stage, &mut plane);
    upload_to_handed(&mut stage, &mut plane, &again);
    // Unanswered on the wire: lost, counted for the report.
    plane.on_detached();
    let step = stage.on_detached(&mut Vec::new());
    assert_eq!(step.lost[0].reason, LossReason::Unanswered);
    assert!(!stage.holds("p1"));
    assert_eq!(stage.take_unanswered().get("p1"), Some(&1));
    assert!(stage.take_unanswered().is_empty());
}

#[test]
fn over_its_depth_the_stage_loses_the_oldest_flush_not_yet_uploading() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"q");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("a", &[("a", &blob)])]),
        2,
        &mut plane,
        Idle(true),
    );
    assert_eq!(creates(&start.frames).len(), 1);
    // The uploading head is not counted: two flushes behind it are kept, as an
    // outbox of depth 2 keeps two parked behind the one on the wire.
    let b = stage.push(
        "p1",
        flush(vec![entry("b", &[])]),
        2,
        &mut plane,
        Idle(true),
    );
    let c = stage.push(
        "p1",
        flush(vec![entry("c", &[])]),
        2,
        &mut plane,
        Idle(true),
    );
    assert!(b.lost.is_empty() && c.lost.is_empty());
    let step = stage.push(
        "p1",
        flush(vec![entry("d", &[])]),
        2,
        &mut plane,
        Idle(true),
    );
    assert_eq!(step.lost.len(), 1);
    assert_eq!(step.lost[0].reason, LossReason::Overflow);
    let handed = upload_to_handed(&mut stage, &mut plane, &start);
    assert_eq!(handed.handed.len(), 1, "the uploading head survived");
    // Handed, the head is still not counted.
    let e = stage.push(
        "p1",
        flush(vec![entry("e", &[])]),
        2,
        &mut plane,
        Idle(true),
    );
    assert_eq!(e.lost.len(), 1, "c is the oldest staged now");
    let done = stage.on_batch_answer("p1", PublishBatchOutcome::Ok, &mut plane, Idle(true));
    let bodies: Vec<_> = done
        .handed
        .iter()
        .map(|(_, batch)| batch.entries[0].body.as_text())
        .collect();
    assert_eq!(bodies, vec![Some("d"), Some("e")]);
}

#[test]
fn a_head_waiting_for_room_goes_first_and_no_other_upload_overtakes_it() {
    let ledger = MadeLedger::new();
    let first = made(&ledger, &[1; 40]);
    let second = made(&ledger, &[2; 40]);
    let large = made(&ledger, &[3; 64]);
    let small = made(&ledger, &[4; 10]);
    let mut plane = plane_with(BlobFacts {
        max_bytes_held: 100,
        ..door()
    });
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "a1",
        flush(vec![entry("first", &[("a", &first)])]),
        4,
        &mut plane,
        Idle(true),
    );
    stage.push(
        "a1",
        flush(vec![entry("second", &[("a", &second)])]),
        4,
        &mut plane,
        Idle(true),
    );
    let waits = stage.push(
        "z1",
        flush(vec![entry("large", &[("l", &large)])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert!(waits.frames.is_empty(), "40 + 64 is over 100");
    assert!(plane.fetches_held());
    let overtakes = stage.push(
        "m1",
        flush(vec![entry("small", &[("s", &small)])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert!(
        overtakes.frames.is_empty(),
        "a small upload that fits does not overtake the waiting head"
    );
    upload_to_handed(&mut stage, &mut plane, &start);
    stage.on_batch_answer("a1", PublishBatchOutcome::Ok, &mut plane, Idle(true));
    let _close = plane.close(creates(&start.frames)[0].0);
    // a1 comes first in key order and its next flush fits the room freed, but
    // z1 has waited longer.
    let step = stage.advance(&mut plane, Idle(true));
    assert_eq!(
        creates(&step.frames)
            .iter()
            .map(|(_, size)| *size)
            .collect::<Vec<_>>(),
        vec![64]
    );
    assert!(plane.fetches_held(), "a1 now waits beside z1's upload");
}

#[test]
fn a_refused_open_loses_the_flush_whole_and_closes_its_other_handles() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"abc");
    let forwarded = remote(8);
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("m", &[("a", &blob), ("photo", &forwarded)])]),
        4,
        &mut plane,
        Idle(true),
    );
    stage.push(
        "p1",
        flush(vec![entry("next", &[])]),
        4,
        &mut plane,
        Idle(true),
    );
    let [created_handle] = creates(&start.frames)
        .iter()
        .map(|(h, _)| *h)
        .collect::<Vec<_>>()[..]
    else {
        panic!("one create: {:?}", start.frames)
    };
    let [open_handle] = opens(&start.frames)[..] else {
        panic!("one open: {:?}", start.frames)
    };
    let step = answer(
        &mut stage,
        &mut plane,
        ServerFrame::BlobOpenResult {
            handle: open_handle,
            outcome: BlobOpenOutcome::NotRetained,
        },
    );
    assert_eq!(
        step.lost,
        vec![Lost {
            instance: "p1".to_string(),
            channels: vec![WIRE.to_string()],
            reason: LossReason::Open(OpenRefusal::NotRetained),
        }]
    );
    assert!(
        step.frames.iter().any(|frame| matches!(
            frame,
            ClientFrame::BlobClose { handle } if *handle == created_handle
        )),
        "the create handle is closed: {:?}",
        step.frames
    );
    assert_eq!(step.handed.len(), 1, "the next flush proceeds");
    assert_eq!(step.handed[0].1.entries[0].body.as_text(), Some("next"));
    assert_eq!(plane.handles_in_use(), 0);
    assert_eq!(stage.lost_count("p1"), 1);
}

#[test]
fn revalidation_loses_a_refused_flush_and_closes_an_uploading_ones_handles() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"r");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("a", &[("a", &blob)])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert_eq!(creates(&start.frames).len(), 1);
    stage.push(
        "p2",
        flush(vec![entry("b", &[("b", &blob)])]),
        4,
        &mut plane,
        Idle(false),
    );
    let step = stage.revalidate(&mut plane, |_, _| false);
    assert_eq!(step.lost.len(), 2);
    assert!(
        step.lost
            .iter()
            .all(|lost| lost.reason == LossReason::Revalidation)
    );
    assert!(
        step.frames
            .iter()
            .any(|frame| matches!(frame, ClientFrame::BlobClose { .. }))
    );
    assert_eq!(plane.handles_in_use(), 0);
    assert!(!stage.holds("p1") && !stage.holds("p2"));
}

#[test]
fn revalidation_keeps_a_surviving_flush_and_leaves_a_handed_head_alone() {
    let ledger = MadeLedger::new();
    let one = made(&ledger, b"one");
    let two = made(&ledger, b"two");
    let three = made(&ledger, b"three");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let handed = stage.push(
        "p1",
        flush(vec![entry("one", &[("a", &one)])]),
        4,
        &mut plane,
        Idle(true),
    );
    upload_to_handed(&mut stage, &mut plane, &handed);
    let uploading = stage.push(
        "p2",
        flush(vec![entry("two", &[("b", &two)])]),
        4,
        &mut plane,
        Idle(true),
    );
    assert_eq!(creates(&uploading.frames).len(), 1);
    stage.push(
        "p3",
        flush(vec![entry("three", &[("c", &three)])]),
        4,
        &mut plane,
        Idle(false),
    );
    // Only p2 passes; p1 is handed, so it is not asked.
    let step = stage.revalidate(&mut plane, |instance, _| instance == "p2");
    assert_eq!(
        step.lost,
        vec![Lost {
            instance: "p3".to_string(),
            channels: vec![WIRE.to_string()],
            reason: LossReason::Revalidation,
        }]
    );
    assert!(step.frames.is_empty(), "no handle of p1 or p2 is closed");
    assert_eq!(plane.handles_in_use(), 2);
    assert!(stage.holds("p1") && stage.holds("p2"));
    stage.on_batch_answer("p1", PublishBatchOutcome::Ok, &mut plane, Idle(true));
    assert!(!stage.holds("p1"));
    let p2 = upload_to_handed(&mut stage, &mut plane, &uploading);
    assert_eq!(p2.handed.len(), 1);
}

fn handed_head(stage: &mut Stage, plane: &mut PagePlane, ledger: &MadeLedger) {
    let blob = made(ledger, b"h");
    let start = stage.push(
        "p1",
        flush(vec![entry("h", &[("a", &blob)])]),
        4,
        plane,
        Idle(true),
    );
    upload_to_handed(stage, plane, &start);
}

#[test]
#[should_panic(expected = "answered TextOnlyChannel")]
fn a_handed_head_answered_text_only_is_fatal() {
    let ledger = MadeLedger::new();
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    handed_head(&mut stage, &mut plane, &ledger);
    stage.on_batch_answer(
        "p1",
        PublishBatchOutcome::TextOnlyChannel,
        &mut plane,
        Idle(true),
    );
}

#[test]
#[should_panic(expected = "answered AttachmentNotHeld")]
fn a_handed_head_answered_attachment_not_held_is_fatal() {
    let ledger = MadeLedger::new();
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    handed_head(&mut stage, &mut plane, &ledger);
    stage.on_batch_answer(
        "p1",
        PublishBatchOutcome::AttachmentNotHeld,
        &mut plane,
        Idle(true),
    );
}

#[test]
fn page_bytes_are_freed_when_the_flush_is_finished_or_discarded() {
    let ledger = MadeLedger::new();
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let blob = made(&ledger, b"0123456789");
    let start = stage.push(
        "p1",
        flush(vec![entry("a", &[("a", &blob)])]),
        4,
        &mut plane,
        Idle(true),
    );
    drop(blob);
    assert_eq!(ledger.held(), 10, "the stage holds the only copy");
    upload_to_handed(&mut stage, &mut plane, &start);
    stage.on_batch_answer("p1", PublishBatchOutcome::Ok, &mut plane, Idle(true));
    assert_eq!(ledger.held(), 0);

    let blob = made(&ledger, b"abc");
    stage.push(
        "p1",
        flush(vec![entry("b", &[("b", &blob)])]),
        4,
        &mut plane,
        Idle(true),
    );
    drop(blob);
    let (frames, count) = stage.discard("p1", &mut plane);
    assert_eq!(count, 1);
    assert!(
        frames
            .iter()
            .any(|frame| matches!(frame, ClientFrame::BlobClose { .. }))
    );
    assert_eq!(ledger.held(), 0);
}

#[test]
fn an_upload_behind_a_plain_flush_handed_over_waits_for_that_flushs_answer() {
    let ledger = MadeLedger::new();
    let blob = made(&ledger, b"s");
    let mut plane = plane_with(door());
    let mut stage = Stage::new();
    ready(&mut stage, &mut plane);
    let start = stage.push(
        "p1",
        flush(vec![entry("a", &[("a", &blob)])]),
        4,
        &mut plane,
        Idle(true),
    );
    stage.push(
        "p1",
        flush(vec![entry("b", &[])]),
        4,
        &mut plane,
        Idle(true),
    );
    stage.push(
        "p1",
        flush(vec![entry("c", &[("c", &blob)])]),
        4,
        &mut plane,
        Idle(true),
    );
    upload_to_handed(&mut stage, &mut plane, &start);
    let done = stage.on_batch_answer("p1", PublishBatchOutcome::Ok, &mut plane, Idle(true));
    assert_eq!(done.handed.len(), 1, "the plain flush");
    assert!(creates(&done.frames).is_empty(), "c waits for b's answer");
    let next = stage.on_batch_answer("p1", PublishBatchOutcome::Ok, &mut plane, Idle(true));
    assert_eq!(creates(&next.frames).len(), 1);
}

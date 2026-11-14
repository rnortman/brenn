//! Blob plane tests: every input is a call or a scripted server answer, so the
//! plane is exercised without a socket or a server.

use super::*;

const CHANNEL: &str = "brenn:camera";

/// A door with small numbers: chunks of 4 bytes, blobs of at most 100, three
/// handles, 40 bytes held and two reads in flight.
fn door() -> BlobFacts {
    BlobFacts {
        max_blob_bytes: 100,
        max_chunk_bytes: 4,
        max_handles: 3,
        max_bytes_held: 40,
        max_inflight_reads: 2,
        upload: true,
    }
}

fn facts(blobs: Option<BlobFacts>) -> AttachmentFacts {
    AttachmentFacts {
        version: 6,
        participant_id: "remote:alice".to_string(),
        session_id: "sess-1".to_string(),
        heartbeat_secs: 20,
        max_body_bytes: 65_536,
        max_frame_bytes: 274_432,
        alert_granted: false,
        blobs,
    }
}

fn plane() -> BlobPlane<&'static str> {
    plane_with(door())
}

fn plane_with(door: BlobFacts) -> BlobPlane<&'static str> {
    let mut plane = BlobPlane::new();
    plane.on_attached(&facts(Some(door)));
    plane
}

fn name() -> AttachmentName {
    AttachmentName::new("photo").expect("a valid name")
}

fn create_answer(handle: BlobHandle, outcome: BlobCreateOutcome) -> ServerFrame {
    ServerFrame::BlobCreateResult { handle, outcome }
}

fn seal_answer(handle: BlobHandle, outcome: BlobSealOutcome) -> ServerFrame {
    ServerFrame::BlobSealResult { handle, outcome }
}

fn open_ok(handle: BlobHandle, size: u64) -> ServerFrame {
    ServerFrame::BlobOpenResult {
        handle,
        outcome: BlobOpenOutcome::Ok {
            size,
            content_type: "image/jpeg".to_string(),
        },
    }
}

fn read_ok(correlation: u64, bytes: &[u8]) -> ServerFrame {
    ServerFrame::BlobReadResult {
        correlation,
        outcome: BlobReadOutcome::Ok {
            bytes: bytes.to_vec(),
        },
    }
}

/// Feed every chunk the upload asks for, returning the frames in order.
fn write_all(plane: &mut BlobPlane<&'static str>, handle: BlobHandle) -> Vec<ClientFrame> {
    let mut frames = Vec::new();
    while let Some(range) = plane.next_chunk(handle) {
        frames.extend(plane.write_chunk(handle, vec![7; range.len]));
    }
    frames
}

/// A held blob of `size` bytes reached by an open.
fn opened(plane: &mut BlobPlane<&'static str>, tag: &'static str, size: u64) -> BlobHandle {
    let (handle, _) = plane.open(tag, CHANNEL, Uuid::nil(), name()).expect("open");
    let step = plane.on_frame(open_ok(handle, size)).expect("owed");
    assert!(matches!(step.events[..], [BlobEvent::Opened { .. }]));
    handle
}

/// The `(correlation, offset, len)` of every `BlobRead` among `frames`.
fn reads(frames: &[ClientFrame]) -> Vec<(u64, u64, u64)> {
    frames
        .iter()
        .map(|frame| match frame {
            ClientFrame::BlobRead {
                correlation,
                offset,
                len,
                ..
            } => (*correlation, *offset, *len),
            other => panic!("expected BlobRead, got {other:?}"),
        })
        .collect()
}

fn ok(step: Result<BlobStep<&'static str>, String>) -> BlobStep<&'static str> {
    step.expect("an owed answer")
}

#[test]
fn an_upload_is_chunked_at_the_advertised_bound() {
    for (size, lens) in [(8, vec![4, 4]), (9, vec![4, 4, 1]), (3, vec![3])] {
        let mut plane = plane();
        let (handle, frames) = plane.create("up", "image/jpeg", size).expect("create");
        assert_eq!(
            frames,
            vec![ClientFrame::BlobCreate {
                handle,
                content_type: "image/jpeg".to_string(),
                size,
            }]
        );
        let frames = write_all(&mut plane, handle);
        let (writes, seal) = frames.split_at(frames.len() - 1);
        let mut offset = 0;
        for (frame, len) in writes.iter().zip(&lens) {
            assert_eq!(
                frame,
                &ClientFrame::BlobWrite {
                    handle,
                    offset,
                    bytes: vec![7; *len],
                }
            );
            offset += *len as u64;
        }
        assert_eq!(writes.len(), lens.len(), "size {size}");
        assert_eq!(seal, [ClientFrame::BlobSeal { handle }]);
        assert_eq!(plane.next_chunk(handle), None);
        assert!(
            ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::Ok)))
                .events
                .is_empty()
        );
        assert_eq!(
            ok(plane.on_frame(seal_answer(handle, BlobSealOutcome::Ok))).events,
            vec![BlobEvent::Sealed { tag: "up", handle }]
        );
        // Held: a read is now allowed.
        assert!(plane.read(handle, "r", 0, size).is_ok());
    }
}

#[test]
fn an_empty_blob_is_created_and_sealed_in_one_call() {
    let mut plane = plane();
    let (handle, frames) = plane.create("empty", "text/plain", 0).expect("create");
    assert_eq!(
        frames,
        vec![
            ClientFrame::BlobCreate {
                handle,
                content_type: "text/plain".to_string(),
                size: 0,
            },
            ClientFrame::BlobSeal { handle },
        ]
    );
    assert_eq!(plane.next_chunk(handle), None);
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::Ok)));
    assert_eq!(
        ok(plane.on_frame(seal_answer(handle, BlobSealOutcome::Ok))).events,
        vec![BlobEvent::Sealed {
            tag: "empty",
            handle
        }]
    );
}

#[test]
fn create_is_refused_locally_without_a_frame() {
    let mut plane = plane();
    assert!(matches!(
        plane.create("a", "not a type", 1),
        Err(CreateError::ContentType(_))
    ));
    assert_eq!(
        plane.create("a", "image/png", 101),
        Err(CreateError::BlobTooLarge {
            size: 101,
            max: 100
        })
    );
    plane.create("a", "image/png", 20).expect("fits");
    assert_eq!(
        plane.create("b", "image/png", 21),
        Err(CreateError::BytesHeldLimit)
    );
    plane.create("b", "image/png", 20).expect("fits exactly");
    plane.create("c", "image/png", 0).expect("a third handle");
    assert_eq!(
        plane.create("d", "image/png", 0),
        Err(CreateError::HandleLimit)
    );
    assert_eq!(
        plane.open("d", CHANNEL, Uuid::nil(), name()),
        Err(OpenError::HandleLimit)
    );
}

#[test]
fn a_refused_create_ends_the_upload_and_absorbs_its_owed_seal_answer() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/jpeg", 6).expect("create");
    // Pipelined: the first chunk is fed before the create is answered.
    let frames = plane.write_chunk(handle, vec![1; 4]);
    assert_eq!(frames.len(), 1);
    let step = ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::BudgetExhausted)));
    assert_eq!(
        step.events,
        vec![BlobEvent::UploadRefused {
            tag: "up",
            handle,
            reason: UploadRefusal::BudgetExhausted
        }]
    );
    assert_eq!(plane.next_chunk(handle), None);
    assert_eq!(plane.bytes_held(), 0);
    // With the seal on the wire, its `UnknownHandle` is owed and absorbed.
    let (handle, _) = plane.create("up2", "image/jpeg", 2).expect("create");
    write_all(&mut plane, handle);
    let step = ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::HandleLimit)));
    assert_eq!(
        step.events,
        vec![BlobEvent::UploadRefused {
            tag: "up2",
            handle,
            reason: UploadRefusal::HandleLimit
        }]
    );
    let step = ok(plane.on_frame(seal_answer(handle, BlobSealOutcome::UnknownHandle)));
    assert!(step.events.is_empty() && step.frames.is_empty());
    assert!(
        plane.records.is_empty(),
        "the record is gone after its last answer"
    );
}

#[test]
fn a_write_after_a_refused_create_is_absorbed() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/jpeg", 6).expect("create");
    let range = plane.next_chunk(handle).expect("the first range");
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::StorageFailed)));
    assert!(plane.write_chunk(handle, vec![0; range.len]).is_empty());
    assert_eq!(plane.next_chunk(handle), None);
    assert!(plane.records.is_empty());
}

#[test]
fn a_write_after_a_detach_is_absorbed_detached_and_after_the_next_attach() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/jpeg", 6).expect("create");
    let range = plane.next_chunk(handle).expect("the first range");
    plane.on_detached();
    assert!(plane.write_chunk(handle, vec![0; range.len]).is_empty());
    assert_eq!(plane.next_chunk(handle), None);
    plane.on_attached(&facts(Some(door())));
    assert!(plane.write_chunk(handle, vec![0; range.len]).is_empty());
    assert_eq!(plane.next_chunk(handle), None);
    assert!(plane.records.is_empty());
}

#[test]
fn an_incomplete_seal_closes_the_handle_and_ends_the_upload() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/jpeg", 2).expect("create");
    write_all(&mut plane, handle);
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::Ok)));
    let step = ok(plane.on_frame(seal_answer(
        handle,
        BlobSealOutcome::Incomplete { offset: 0, len: 2 },
    )));
    assert_eq!(step.frames, vec![ClientFrame::BlobClose { handle }]);
    assert_eq!(
        step.events,
        vec![BlobEvent::UploadRefused {
            tag: "up",
            handle,
            reason: UploadRefusal::Incomplete { offset: 0, len: 2 }
        }]
    );
    assert!(plane.records.is_empty());
}

#[test]
fn a_read_keeps_the_window_and_delivers_chunks_in_order() {
    let mut plane = plane();
    let handle = opened(&mut plane, "open", 10);
    let (read, frames) = plane.read(handle, "whole", 0, 10).expect("read");
    assert_eq!(reads(&frames), vec![(1, 0, 4), (2, 4, 4)]);
    let step = ok(plane.on_frame(read_ok(1, &[0, 1, 2, 3])));
    assert_eq!(
        step.events,
        vec![BlobEvent::Chunk {
            read,
            handle,
            offset: 0,
            bytes: vec![0, 1, 2, 3]
        }]
    );
    assert_eq!(reads(&step.frames), vec![(3, 8, 2)]);
    let step = ok(plane.on_frame(read_ok(2, &[4, 5, 6, 7])));
    assert!(step.frames.is_empty(), "nothing left to request");
    let step = ok(plane.on_frame(read_ok(3, &[8, 9])));
    assert_eq!(
        step.events,
        vec![
            BlobEvent::Chunk {
                read,
                handle,
                offset: 8,
                bytes: vec![8, 9]
            },
            BlobEvent::ReadDone {
                read,
                tag: "whole",
                handle
            },
        ]
    );
}

#[test]
fn the_window_is_shared_across_reads_and_never_exceeded() {
    let mut plane = plane();
    let a = opened(&mut plane, "a", 8);
    let b = opened(&mut plane, "b", 4);
    let (_, frames) = plane.read(a, "ra", 0, 8).expect("read");
    assert_eq!(frames.len(), 2);
    let (rb, frames) = plane.read(b, "rb", 0, 4).expect("read");
    assert!(frames.is_empty(), "the window is full");
    let step = ok(plane.on_frame(read_ok(1, &[0; 4])));
    assert_eq!(reads(&step.frames), vec![(3, 0, 4)], "one slot freed");
    assert_eq!(plane.requests.len(), 2);
    let step = ok(plane.on_frame(read_ok(2, &[0; 4])));
    assert!(step.frames.is_empty(), "nothing waits");
    assert_eq!(plane.requests.len(), 1);
    let step = ok(plane.on_frame(read_ok(3, &[1; 4])));
    assert!(matches!(
        step.events[..],
        [BlobEvent::Chunk { .. }, BlobEvent::ReadDone { read, .. }] if read == rb
    ));
}

#[test]
fn a_zero_length_read_is_one_empty_chunk() {
    let mut plane = plane();
    let handle = opened(&mut plane, "open", 10);
    let (read, frames) = plane.read(handle, "none", 10, 0).expect("read at the end");
    assert_eq!(reads(&frames), vec![(1, 10, 0)]);
    let step = ok(plane.on_frame(read_ok(1, &[])));
    assert_eq!(step.events.len(), 2);
    assert!(matches!(step.events[1], BlobEvent::ReadDone { read: r, .. } if r == read));
}

#[test]
fn a_range_past_the_size_is_refused_locally() {
    let mut plane = plane();
    let handle = opened(&mut plane, "open", 10);
    assert_eq!(
        plane.read(handle, "r", 8, 3),
        Err(ReadError::OutOfRange { size: 10 })
    );
    assert_eq!(
        plane.read(handle, "r", u64::MAX, 1),
        Err(ReadError::OutOfRange { size: 10 })
    );
    assert!(plane.requests.is_empty());
}

#[test]
fn a_refused_read_abandons_the_rest_and_absorbs_its_answers_in_flight() {
    let mut plane = plane();
    let handle = opened(&mut plane, "open", 12);
    let (read, _) = plane.read(handle, "r", 0, 12).expect("read");
    let step = ok(plane.on_frame(ServerFrame::BlobReadResult {
        correlation: 1,
        outcome: BlobReadOutcome::StorageFailed,
    }));
    assert_eq!(
        step.events,
        vec![BlobEvent::ReadRefused {
            read,
            tag: "r",
            handle,
            reason: ReadRefusal::StorageFailed
        }]
    );
    assert!(step.frames.is_empty(), "the third chunk is never requested");
    let step = ok(plane.on_frame(read_ok(2, &[0; 4])));
    assert!(step.events.is_empty() && step.frames.is_empty());
    assert!(plane.requests.is_empty());
}

#[test]
fn every_open_refusal_is_its_event_and_the_handle_is_gone() {
    for (outcome, reason) in [
        (BlobOpenOutcome::NotRetained, OpenRefusal::NotRetained),
        (
            BlobOpenOutcome::NoSuchAttachment,
            OpenRefusal::NoSuchAttachment,
        ),
        (BlobOpenOutcome::ToolChannel, OpenRefusal::ToolChannel),
        (BlobOpenOutcome::HandleLimit, OpenRefusal::HandleLimit),
        (
            BlobOpenOutcome::BytesHeldLimit { size: 99 },
            OpenRefusal::BytesHeldLimit { size: 99 },
        ),
    ] {
        let mut plane = plane();
        let (handle, frame) = plane.open("o", CHANNEL, Uuid::nil(), name()).expect("open");
        assert_eq!(
            frame,
            ClientFrame::BlobOpen {
                handle,
                channel: CHANNEL.to_string(),
                message_id: Uuid::nil(),
                name: name(),
            }
        );
        let step = ok(plane.on_frame(ServerFrame::BlobOpenResult { handle, outcome }));
        assert_eq!(
            step.events,
            vec![BlobEvent::OpenRefused {
                tag: "o",
                handle,
                reason
            }]
        );
        assert!(plane.records.is_empty());
    }
}

#[test]
fn closing_an_unsealed_upload_ends_it_and_absorbs_the_late_create_answer() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/jpeg", 8).expect("create");
    plane.write_chunk(handle, vec![0; 4]);
    let step = plane.close(handle);
    assert_eq!(step.frames, vec![ClientFrame::BlobClose { handle }]);
    assert_eq!(
        step.events,
        vec![BlobEvent::UploadRefused {
            tag: "up",
            handle,
            reason: UploadRefusal::Closed
        }]
    );
    assert_eq!(plane.next_chunk(handle), None);
    let step = ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::Ok)));
    assert!(step.events.is_empty());
    assert!(plane.records.is_empty(), "gone after its last owed answer");
}

#[test]
fn closing_a_sealing_upload_keeps_its_real_outcome() {
    // Sealed after all.
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/jpeg", 2).expect("create");
    write_all(&mut plane, handle);
    assert!(plane.close(handle).events.is_empty());
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::Ok)));
    assert_eq!(
        ok(plane.on_frame(seal_answer(handle, BlobSealOutcome::Ok))).events,
        vec![BlobEvent::Sealed { tag: "up", handle }]
    );
    assert!(plane.records.is_empty());

    // The create refused: reported with its outcome, the seal's answer absorbed.
    let (handle, _) = plane.create("up", "image/jpeg", 2).expect("create");
    write_all(&mut plane, handle);
    plane.close(handle);
    assert_eq!(
        ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::BudgetExhausted))).events,
        vec![BlobEvent::UploadRefused {
            tag: "up",
            handle,
            reason: UploadRefusal::BudgetExhausted
        }]
    );
    assert!(
        ok(plane.on_frame(seal_answer(handle, BlobSealOutcome::UnknownHandle)))
            .events
            .is_empty()
    );
    assert!(plane.records.is_empty());
    assert_eq!(plane.bytes_held(), 0, "nothing underflows");

    // Incomplete: reported, and no second `BlobClose`.
    let (handle, _) = plane.create("up", "image/jpeg", 2).expect("create");
    write_all(&mut plane, handle);
    plane.close(handle);
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::Ok)));
    let step = ok(plane.on_frame(seal_answer(
        handle,
        BlobSealOutcome::Incomplete { offset: 1, len: 1 },
    )));
    assert!(step.frames.is_empty());
    assert_eq!(
        step.events,
        vec![BlobEvent::UploadRefused {
            tag: "up",
            handle,
            reason: UploadRefusal::Incomplete { offset: 1, len: 1 }
        }]
    );
}

#[test]
fn closing_an_unanswered_open_reports_the_late_answer() {
    let mut plane = plane();
    let (handle, _) = plane.open("o", CHANNEL, Uuid::nil(), name()).expect("open");
    let step = plane.close(handle);
    assert!(step.events.is_empty());
    assert_eq!(
        ok(plane.on_frame(open_ok(handle, 5))).events,
        vec![BlobEvent::Opened {
            tag: "o",
            handle,
            size: 5,
            content_type: "image/jpeg".to_string()
        }]
    );
    assert!(plane.records.is_empty());
}

#[test]
fn closing_a_held_handle_ends_its_reads_and_counts_late_answers_in_the_window() {
    let mut plane = plane();
    let a = opened(&mut plane, "a", 12);
    let b = opened(&mut plane, "b", 4);
    let (ra, _) = plane.read(a, "ra", 0, 12).expect("read");
    let step = plane.close(a);
    assert_eq!(step.frames, vec![ClientFrame::BlobClose { handle: a }]);
    assert_eq!(
        step.events,
        vec![BlobEvent::ReadRefused {
            read: ra,
            tag: "ra",
            handle: a,
            reason: ReadRefusal::Closed
        }]
    );
    // Both of a's requests still fill the window.
    let (_, frames) = plane.read(b, "rb", 0, 4).expect("read");
    assert!(frames.is_empty());
    let step = ok(plane.on_frame(read_ok(1, &[0; 4])));
    assert!(step.events.is_empty(), "a late chunk is absorbed");
    assert_eq!(reads(&step.frames), vec![(3, 0, 4)]);
    assert!(plane.records.contains_key(&a), "one answer still owed on a");
    ok(plane.on_frame(read_ok(2, &[0; 4])));
    assert!(
        !plane.records.contains_key(&a),
        "gone after its last answer"
    );
}

#[test]
fn closing_with_nothing_owed_forgets_the_handle_at_once() {
    let mut plane = plane();
    let handle = opened(&mut plane, "o", 3);
    plane.close(handle);
    assert!(plane.records.is_empty());
}

#[test]
fn an_answer_the_plane_is_not_owed_is_an_error() {
    let mut plane = plane();
    assert!(
        plane
            .on_frame(create_answer(BlobHandle(99), BlobCreateOutcome::Ok))
            .is_err()
    );
    assert!(plane.on_frame(open_ok(BlobHandle(99), 1)).is_err());
    assert!(plane.on_frame(read_ok(99, &[])).is_err());
    let (handle, _) = plane.create("up", "image/jpeg", 2).expect("create");
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::Ok)));
    assert!(
        plane
            .on_frame(create_answer(handle, BlobCreateOutcome::Ok))
            .is_err(),
        "a second answer to one create"
    );
    assert!(
        plane
            .on_frame(seal_answer(handle, BlobSealOutcome::Ok))
            .is_err(),
        "a seal never sent"
    );
    let held = opened(&mut plane, "o", 8);
    plane.read(held, "r", 0, 4).expect("read");
    let detail = plane.on_frame(read_ok(1, &[0; 3])).unwrap_err();
    assert!(detail.contains("3 bytes where 4"), "{detail}");
}

#[test]
fn on_detached_fails_everything_outstanding_and_handles_never_repeat() {
    let mut plane = plane();
    let (up, _) = plane.create("up", "image/jpeg", 8).expect("create");
    let (open, _) = plane
        .open("open", CHANNEL, Uuid::nil(), name())
        .expect("open");
    let held = {
        let (h, _) = plane.create("sealing", "image/jpeg", 2).expect("create");
        write_all(&mut plane, h);
        plane.close(h);
        h
    };
    let events = plane.on_detached();
    assert_eq!(
        events,
        vec![
            BlobEvent::UploadRefused {
                tag: "up",
                handle: up,
                reason: UploadRefusal::Detached
            },
            BlobEvent::OpenRefused {
                tag: "open",
                handle: open,
                reason: OpenRefusal::Detached
            },
            BlobEvent::UploadRefused {
                tag: "sealing",
                handle: held,
                reason: UploadRefusal::Detached
            },
        ]
    );
    assert_eq!(plane.bytes_held(), 0);
    plane.on_attached(&facts(Some(door())));
    let r = opened(&mut plane, "r", 8);
    let (read, _) = plane.read(r, "read", 0, 8).expect("read");
    assert!(r > up && r > open && r > held, "handles never repeat");
    assert_eq!(
        plane.on_detached(),
        vec![BlobEvent::ReadRefused {
            read,
            tag: "read",
            handle: r,
            reason: ReadRefusal::Detached
        }]
    );
}

#[test]
fn opened_sizes_fill_the_bound_and_close_frees_them() {
    let mut plane = plane();
    let big = opened(&mut plane, "big", 30);
    assert_eq!(
        plane.create("up", "image/png", 11),
        Err(CreateError::BytesHeldLimit)
    );
    plane.close(big);
    plane
        .create("up", "image/png", 40)
        .expect("the sum fell at close");
}

#[test]
fn an_opening_handle_counts_nothing_until_opened() {
    let mut plane = plane();
    let (handle, _) = plane.open("o", CHANNEL, Uuid::nil(), name()).expect("open");
    assert_eq!(plane.bytes_held(), 0);
    ok(plane.on_frame(open_ok(handle, 25)));
    assert_eq!(plane.bytes_held(), 25);
}

#[test]
fn a_closing_handle_counts_nothing_whatever_arrives_late() {
    // open, close, then `Opened`: reported, the sum unchanged.
    let mut plane = plane();
    let (handle, _) = plane.open("o", CHANNEL, Uuid::nil(), name()).expect("open");
    plane.close(handle);
    let step = ok(plane.on_frame(open_ok(handle, 30)));
    assert_eq!(step.events.len(), 1);
    assert_eq!(plane.bytes_held(), 0);
    plane
        .create("up", "image/png", 40)
        .expect("not refused locally");

    // create, close with the seal on the wire, then the create's refusal.
    let mut plane = plane_with(door());
    let (handle, _) = plane.create("up", "image/png", 2).expect("create");
    write_all(&mut plane, handle);
    plane.close(handle);
    assert_eq!(plane.bytes_held(), 0);
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::BytesHeldLimit)));
    ok(plane.on_frame(seal_answer(handle, BlobSealOutcome::UnknownHandle)));
    assert_eq!(plane.bytes_held(), 0);
}

#[test]
fn a_sealed_upload_counts_its_size_as_a_held_blob() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/png", 6).expect("create");
    assert_eq!(plane.bytes_held(), 6);
    write_all(&mut plane, handle);
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::Ok)));
    ok(plane.on_frame(seal_answer(handle, BlobSealOutcome::Ok)));
    assert_eq!(plane.bytes_held(), 6);
}

#[test]
#[should_panic(expected = "has no blob door")]
fn a_call_without_a_blob_door_panics() {
    let mut plane: BlobPlane<&str> = BlobPlane::new();
    plane.on_attached(&facts(None));
    let _ = plane.open("o", CHANNEL, Uuid::nil(), name());
}

#[test]
#[should_panic(expected = "grants no upload")]
fn create_without_upload_panics() {
    let mut plane = plane_with(BlobFacts {
        upload: false,
        ..door()
    });
    let _ = plane.create("up", "image/png", 1);
}

#[test]
#[should_panic(expected = "with 3 bytes where the range at 0 is 4")]
fn writing_bytes_that_are_not_the_named_range_panics() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/png", 8).expect("create");
    plane.write_chunk(handle, vec![0; 3]);
}

#[test]
#[should_panic(expected = "that is not held")]
fn reading_an_opening_handle_panics() {
    let mut plane = plane();
    let (handle, _) = plane.open("o", CHANNEL, Uuid::nil(), name()).expect("open");
    let _ = plane.read(handle, "r", 0, 0);
}

#[test]
#[should_panic(expected = "that is not held")]
fn reading_a_sealing_upload_panics() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/png", 0).expect("create");
    let _ = plane.read(handle, "r", 0, 0);
}

/// `close` on `handle` sends nothing, reports nothing and changes neither the
/// bytes held nor the live handles nor the records.
fn assert_close_absorbed(plane: &mut BlobPlane<&'static str>, handle: BlobHandle) {
    let held = plane.bytes_held();
    let live = plane.live_handles();
    let records: Vec<BlobHandle> = plane.records.keys().copied().collect();
    let step = plane.close(handle);
    assert!(step.frames.is_empty(), "no frame for handle {}", handle.0);
    assert!(step.events.is_empty(), "no event for handle {}", handle.0);
    assert_eq!(plane.bytes_held(), held);
    assert_eq!(plane.live_handles(), live);
    assert_eq!(plane.records.keys().copied().collect::<Vec<_>>(), records);
}

#[test]
fn closing_a_handle_a_refused_create_retired_is_absorbed() {
    let mut plane = plane();
    let keep = opened(&mut plane, "keep", 5);
    let (handle, _) = plane.create("up", "image/jpeg", 2).expect("create");
    write_all(&mut plane, handle);
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::BudgetExhausted)));
    // The seal's answer is still owed.
    assert!(plane.records.contains_key(&handle));
    assert_close_absorbed(&mut plane, handle);
    ok(plane.on_frame(seal_answer(handle, BlobSealOutcome::UnknownHandle)));
    assert!(!plane.records.contains_key(&handle));
    assert_close_absorbed(&mut plane, handle);
    assert!(
        plane.read(keep, "r", 0, 5).is_ok(),
        "the other handle lives"
    );
}

#[test]
fn closing_a_handle_closed_on_incomplete_sends_no_second_close() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/jpeg", 2).expect("create");
    write_all(&mut plane, handle);
    ok(plane.on_frame(create_answer(handle, BlobCreateOutcome::Ok)));
    let step = ok(plane.on_frame(seal_answer(
        handle,
        BlobSealOutcome::Incomplete { offset: 0, len: 2 },
    )));
    assert_eq!(step.frames, vec![ClientFrame::BlobClose { handle }]);
    assert_close_absorbed(&mut plane, handle);
}

#[test]
fn closing_a_handle_whose_open_was_refused_is_absorbed() {
    let mut plane = plane();
    let (handle, _) = plane.open("o", CHANNEL, Uuid::nil(), name()).expect("open");
    ok(plane.on_frame(ServerFrame::BlobOpenResult {
        handle,
        outcome: BlobOpenOutcome::NotRetained,
    }));
    assert_close_absorbed(&mut plane, handle);
}

#[test]
fn closing_a_handle_forgotten_at_a_detach_is_absorbed_detached_and_after_the_next_attach() {
    let mut plane = plane();
    let held = opened(&mut plane, "o", 5);
    let (up, _) = plane.create("up", "image/jpeg", 6).expect("create");
    plane.on_detached();
    assert_close_absorbed(&mut plane, held);
    assert_close_absorbed(&mut plane, up);
    plane.on_attached(&facts(Some(door())));
    let live = opened(&mut plane, "live", 7);
    assert_close_absorbed(&mut plane, held);
    assert_close_absorbed(&mut plane, up);
    assert_eq!(plane.bytes_held(), 7);
    assert!(plane.read(live, "r", 0, 7).is_ok());
}

#[test]
fn the_embedders_repeated_close_is_absorbed_closing_and_gone() {
    let mut plane = plane();
    // Closing: the open's answer is still owed.
    let (opening, _) = plane.open("o", CHANNEL, Uuid::nil(), name()).expect("open");
    assert_eq!(
        plane.close(opening).frames,
        vec![ClientFrame::BlobClose { handle: opening }]
    );
    assert!(plane.records.contains_key(&opening));
    assert_close_absorbed(&mut plane, opening);
    // Gone: nothing was owed.
    let held = opened(&mut plane, "h", 3);
    plane.close(held);
    assert!(!plane.records.contains_key(&held));
    assert_close_absorbed(&mut plane, held);
}

#[test]
fn reading_a_held_handle_forgotten_at_a_detach_is_gone_detached_and_after_the_next_attach() {
    let mut plane = plane();
    let handle = opened(&mut plane, "o", 8);
    plane.on_detached();
    assert_eq!(plane.read(handle, "r", 0, 4), Err(ReadError::Gone));
    plane.on_attached(&facts(Some(door())));
    assert_eq!(plane.read(handle, "r", 0, 4), Err(ReadError::Gone));
    assert!(plane.requests.is_empty() && plane.reads.is_empty());
}

#[test]
fn reading_a_refused_or_closed_handle_is_gone() {
    let mut plane = plane();
    let (refused, _) = plane.open("o", CHANNEL, Uuid::nil(), name()).expect("open");
    ok(plane.on_frame(ServerFrame::BlobOpenResult {
        handle: refused,
        outcome: BlobOpenOutcome::NotRetained,
    }));
    assert_eq!(plane.read(refused, "r", 0, 0), Err(ReadError::Gone));
    let closed = opened(&mut plane, "h", 4);
    plane.close(closed);
    assert_eq!(plane.read(closed, "r", 0, 4), Err(ReadError::Gone));
    assert!(plane.requests.is_empty() && plane.reads.is_empty());
}

#[test]
#[should_panic(expected = "next_chunk on blob handle 5 this plane never minted")]
fn next_chunk_on_a_handle_never_minted_panics() {
    let plane = plane();
    plane.next_chunk(BlobHandle(5));
}

#[test]
#[should_panic(expected = "write_chunk on blob handle 5 this plane never minted")]
fn write_chunk_on_a_handle_never_minted_panics() {
    let mut plane = plane();
    plane.write_chunk(BlobHandle(5), vec![0; 4]);
}

#[test]
#[should_panic(expected = "read on blob handle 0 this plane never minted")]
fn read_on_a_handle_never_minted_panics() {
    let mut plane = plane();
    let _ = plane.read(BlobHandle(0), "r", 0, 0);
}

#[test]
#[should_panic(expected = "close on blob handle 5 this plane never minted")]
fn close_on_a_handle_never_minted_panics() {
    let mut plane: BlobPlane<&str> = BlobPlane::new();
    plane.close(BlobHandle(5));
}

#[test]
#[should_panic(expected = "wants no chunk")]
fn writing_to_a_live_sealing_upload_panics() {
    let mut plane = plane();
    let (handle, _) = plane.create("up", "image/png", 4).expect("create");
    write_all(&mut plane, handle);
    plane.write_chunk(handle, vec![0; 4]);
}

#[test]
#[should_panic(expected = "without on_detached")]
fn attaching_twice_without_a_detach_panics() {
    let mut plane = plane();
    plane.open("o", CHANNEL, Uuid::nil(), name()).expect("open");
    plane.on_attached(&facts(Some(door())));
}

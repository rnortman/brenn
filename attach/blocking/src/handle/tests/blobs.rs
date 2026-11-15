//! Blob transfer against the scripted server: uploads, opens, reads, closes,
//! and what a detach, a reattachment and a cancel do to each.

use std::path::PathBuf;

use brenn_attach_proto::{BlobCreateOutcome, BlobOpenOutcome, BlobSealOutcome};

use super::*;
use crate::{OpenRefusal, PublishAttachment, UploadRefusal};

const TYPE: &str = "application/octet-stream";

fn name(text: &str) -> AttachmentName {
    AttachmentName::new(text).expect("a valid attachment name")
}

fn content(len: usize) -> Vec<u8> {
    (0..len).map(|n| n as u8).collect()
}

/// A path under the test's scratch directory that does not exist yet.
fn scratch(file: &str) -> PathBuf {
    let dir = std::env::var_os("TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let path = dir.join(format!("{}-{file}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

/// The next `BlobCreate`, skipping what an earlier refused upload still sent.
fn next_create(socket: &Socket) -> (BlobHandle, u64) {
    loop {
        if let ClientFrame::BlobCreate { handle, size, .. } = socket.next_frame() {
            return (handle, size);
        }
    }
}

/// Serve one upload of `expect`: answer the create, take each write in
/// order, at most one chunk long, and seal it.
fn serve_upload(socket: &Socket, expect: &[u8]) -> BlobHandle {
    let (handle, size) = next_create(socket);
    assert_eq!(size, expect.len() as u64);
    socket.send(ServerFrame::BlobCreateResult {
        handle,
        outcome: BlobCreateOutcome::Ok,
    });
    let mut written = Vec::new();
    loop {
        match socket.next_frame() {
            ClientFrame::BlobWrite {
                handle: to,
                offset,
                bytes,
            } => {
                assert_eq!((to, offset), (handle, written.len() as u64));
                assert!(bytes.len() as u64 <= door().max_chunk_bytes);
                written.extend(bytes);
            }
            ClientFrame::BlobSeal { handle: sealed } => {
                assert_eq!(sealed, handle);
                break;
            }
            other => panic!("expected a write or the seal, got {other:?}"),
        }
    }
    assert_eq!(written, expect);
    socket.send(ServerFrame::BlobSealResult {
        handle,
        outcome: BlobSealOutcome::Ok,
    });
    handle
}

fn uploaded(client: &BlockingClient, socket: &Socket, bytes: &[u8]) -> Blob {
    thread::scope(|scope| {
        let call = scope.spawn(|| client.upload_bytes(TYPE, bytes, PATIENCE, &mut never()));
        let handle = serve_upload(socket, bytes);
        let blob = call.join().unwrap().expect("the upload");
        assert_eq!(blob.handle, handle);
        blob
    })
}

/// Answer reads of `blob` holding `bytes` until `len` bytes were asked for;
/// answers the ranges asked, in order.
fn serve_reads(socket: &Socket, bytes: &[u8], len: u64) -> Vec<(u64, u64)> {
    let mut asked = Vec::new();
    while asked.iter().map(|(_, len)| len).sum::<u64>() < len {
        let ClientFrame::BlobRead {
            correlation,
            offset,
            len,
            ..
        } = socket.next_frame()
        else {
            panic!("expected a BlobRead");
        };
        asked.push((offset, len));
        let range = offset as usize..(offset + len) as usize;
        socket.send(ServerFrame::BlobReadResult {
            correlation,
            outcome: BlobReadOutcome::Ok {
                bytes: bytes[range].to_vec(),
            },
        });
    }
    asked
}

#[test]
fn an_upload_is_written_a_chunk_at_a_time_and_sealed() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let bytes = content(10);
    let blob = uploaded(&client, &socket, &bytes);
    assert_eq!((blob.size(), blob.content_type()), (10, TYPE));
}

#[test]
fn an_empty_upload_is_created_and_sealed_at_once() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let blob = uploaded(&client, &socket, &[]);
    assert_eq!(blob.size(), 0);
}

#[test]
fn an_upload_from_a_file_sends_the_files_bytes() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let bytes = content(9);
    let path = scratch("upload");
    std::fs::write(&path, &bytes).unwrap();
    let blob = thread::scope(|scope| {
        let call = scope.spawn(|| client.upload_file(TYPE, &path, PATIENCE, &mut never()));
        serve_upload(&socket, &bytes);
        call.join().unwrap()
    });
    assert_eq!(blob.map(|blob| blob.size()), Ok(9));

    let missing = scratch("missing");
    let Err(CallError::File { path, error }) =
        client.upload_file(TYPE, &missing, PATIENCE, &mut never())
    else {
        panic!("a missing file uploads nothing");
    };
    assert_eq!((path, error.kind), (missing, io::ErrorKind::NotFound));
    socket.no_frame();
}

#[test]
fn an_upload_from_anything_but_a_regular_file_sends_nothing() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let directory = scratch("directory");
    let _ = std::fs::remove_dir(&directory);
    std::fs::create_dir(&directory).unwrap();
    // A device whose length, zero, is not what it yields, and a directory.
    for file in [PathBuf::from("/dev/null"), directory] {
        let Err(CallError::File { path, error }) =
            client.upload_file(TYPE, &file, PATIENCE, &mut never())
        else {
            panic!("{} uploads nothing", file.display());
        };
        assert_eq!((path, error.kind), (file, io::ErrorKind::InvalidInput));
        assert_eq!(error.detail, "not a regular file");
    }
    socket.no_frame();
}

#[test]
fn each_create_refusal_reaches_the_caller() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    for (outcome, refusal) in [
        (
            BlobCreateOutcome::BlobTooLarge { size: 2, max: 1 },
            UploadRefusal::BlobTooLarge { size: 2, max: 1 },
        ),
        (
            BlobCreateOutcome::BudgetExhausted,
            UploadRefusal::BudgetExhausted,
        ),
        (BlobCreateOutcome::HandleLimit, UploadRefusal::HandleLimit),
        (
            BlobCreateOutcome::BytesHeldLimit,
            UploadRefusal::BytesHeldLimit,
        ),
        (
            BlobCreateOutcome::StorageFailed,
            UploadRefusal::StorageFailed,
        ),
    ] {
        let answer = thread::scope(|scope| {
            let call = scope.spawn(|| client.upload_bytes(TYPE, &[1, 2], PATIENCE, &mut never()));
            let (handle, _) = next_create(&socket);
            socket.send(ServerFrame::BlobCreateResult { handle, outcome });
            call.join().unwrap()
        });
        assert_eq!(answer, Err(CallError::UploadRefused(refusal)));
    }
}

#[test]
fn incomplete_at_the_seal_is_refused_and_the_handle_closed() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| client.upload_bytes(TYPE, &[1, 2], PATIENCE, &mut never()));
        let (handle, _) = next_create(&socket);
        socket.send(ServerFrame::BlobCreateResult {
            handle,
            outcome: BlobCreateOutcome::Ok,
        });
        assert!(matches!(socket.next_frame(), ClientFrame::BlobWrite { .. }));
        assert_eq!(socket.next_frame(), ClientFrame::BlobSeal { handle });
        socket.send(ServerFrame::BlobSealResult {
            handle,
            outcome: BlobSealOutcome::Incomplete { offset: 0, len: 2 },
        });
        assert_eq!(socket.next_frame(), ClientFrame::BlobClose { handle });
        call.join().unwrap()
    });
    assert_eq!(
        answer,
        Err(CallError::UploadRefused(UploadRefusal::Incomplete {
            offset: 0,
            len: 2
        }))
    );
}

#[test]
fn local_refusals_send_nothing() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    assert_eq!(
        client.upload_bytes(TYPE, &content(65), PATIENCE, &mut never()),
        Err(CallError::UploadRefused(UploadRefusal::BlobTooLarge {
            size: 65,
            max: 64
        }))
    );
    assert!(matches!(
        client.upload_bytes("not a type", &[1], PATIENCE, &mut never()),
        Err(CallError::ContentType(_))
    ));
    socket.no_frame();
}

#[test]
fn a_door_without_upload_and_no_door_refuse_before_the_wire() {
    let controls = Controls::new();
    let socket = controls.succeed();
    let client = controls.start(timings());
    socket.handshake_with(
        "sess-1",
        Some(BlobFacts {
            upload: false,
            ..door()
        }),
    );
    client.await_attachment(PATIENCE, &mut never()).unwrap();
    assert_eq!(
        client.upload_bytes(TYPE, &[1], PATIENCE, &mut never()),
        Err(CallError::UploadNotGranted)
    );
    socket.no_frame();

    let doorless = controls.succeed();
    drop_and_wait(&client, &socket);
    doorless.handshake_with("sess-2", None);
    wait_until("the doorless attachment", || {
        session(&client).as_deref() == Some("sess-2")
    });
    assert_eq!(
        client.upload_bytes(TYPE, &[1], PATIENCE, &mut never()),
        Err(CallError::NoBlobDoor)
    );
    assert_eq!(
        client.open_blob(CHANNEL, Uuid::nil(), name("a"), PATIENCE, &mut never()),
        Err(CallError::NoBlobDoor)
    );
    let request = publish_request(CHANNEL)
        .attachments(vec![PublishAttachment::new(name("a"), BlobHandle(1))]);
    assert_eq!(
        client.publish(request, PATIENCE, &mut never()),
        Err(CallError::NoBlobDoor)
    );
    doorless.no_frame();
}

#[test]
fn a_publish_naming_a_blob_carries_it_and_attachment_not_held_is_an_outcome() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let blob = uploaded(&client, &socket, &[7]);
    let attachment = client
        .attachment(&blob, name("photo"))
        .expect("its own blob");
    let request = publish_request(CHANNEL).attachments(vec![attachment]);
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| client.publish(request, PATIENCE, &mut never()));
        let ClientFrame::Publish {
            correlation,
            attachments,
            ..
        } = socket.next_frame()
        else {
            panic!("expected a Publish");
        };
        assert_eq!(
            attachments,
            vec![PublishAttachment::new(name("photo"), blob.handle)]
        );
        socket.send(ServerFrame::PublishResult {
            correlation,
            outcome: WireOutcome::AttachmentNotHeld,
        });
        call.join().unwrap()
    });
    assert_eq!(answer, Ok(PublishOutcome::AttachmentNotHeld));
    assert!(matches!(client.status(), Status::Attached(_)));
}

/// Open attachment `a` of a nil message on [`CHANNEL`], answered `outcome`.
fn open(
    client: &BlockingClient,
    socket: &Socket,
    outcome: BlobOpenOutcome,
) -> Result<Blob, CallError> {
    thread::scope(|scope| {
        let call = scope
            .spawn(|| client.open_blob(CHANNEL, Uuid::nil(), name("a"), PATIENCE, &mut never()));
        let ClientFrame::BlobOpen {
            handle,
            channel,
            name: opened,
            ..
        } = socket.next_frame()
        else {
            panic!("expected a BlobOpen");
        };
        assert_eq!((channel.as_str(), opened.as_str()), (CHANNEL, "a"));
        socket.send(ServerFrame::BlobOpenResult { handle, outcome });
        call.join().unwrap()
    })
}

#[test]
fn an_open_holds_the_blob_or_reaches_the_caller_refused() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let ok = BlobOpenOutcome::Ok {
        size: 12,
        content_type: "image/png".to_string(),
    };
    let blob = open(&client, &socket, ok).expect("the open");
    assert_eq!((blob.size(), blob.content_type()), (12, "image/png"));
    for (outcome, refusal) in [
        (BlobOpenOutcome::NotRetained, OpenRefusal::NotRetained),
        (
            BlobOpenOutcome::NoSuchAttachment,
            OpenRefusal::NoSuchAttachment,
        ),
        (BlobOpenOutcome::ToolChannel, OpenRefusal::ToolChannel),
        (BlobOpenOutcome::HandleLimit, OpenRefusal::HandleLimit),
        (
            BlobOpenOutcome::BytesHeldLimit { size: 9 },
            OpenRefusal::BytesHeldLimit { size: 9 },
        ),
    ] {
        assert_eq!(
            open(&client, &socket, outcome),
            Err(CallError::OpenRefused(refusal))
        );
    }
    assert_eq!(
        client.open_blob("local:x", Uuid::nil(), name("a"), PATIENCE, &mut never()),
        Err(CallError::LocalChannel)
    );
}

#[test]
fn a_read_across_windows_is_assembled_in_order() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let bytes = content(20);
    let blob = uploaded(&client, &socket, &bytes);
    let (read, asked) = thread::scope(|scope| {
        let call = scope.spawn(|| client.read_blob(&blob, 1, 17, PATIENCE, &mut never()));
        let asked = serve_reads(&socket, &bytes, 17);
        (call.join().unwrap(), asked)
    });
    assert_eq!(read, Ok(bytes[1..18].to_vec()));
    // Windows of eight, each cut into chunks of four.
    assert_eq!(asked, [(1, 4), (5, 4), (9, 4), (13, 4), (17, 1)]);
}

#[test]
fn a_read_is_refused_by_the_server_or_locally_past_the_end() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let blob = uploaded(&client, &socket, &[1, 2, 3]);
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| client.read_blob(&blob, 0, 3, PATIENCE, &mut never()));
        let ClientFrame::BlobRead { correlation, .. } = socket.next_frame() else {
            panic!("expected a BlobRead");
        };
        socket.send(ServerFrame::BlobReadResult {
            correlation,
            outcome: BlobReadOutcome::StorageFailed,
        });
        call.join().unwrap()
    });
    assert_eq!(
        answer,
        Err(CallError::ReadRefused(ReadRefusal::StorageFailed))
    );
    assert_eq!(
        client.read_blob(&blob, 2, 2, PATIENCE, &mut never()),
        Err(CallError::ReadRefused(ReadRefusal::OutOfRange { size: 3 }))
    );
    socket.no_frame();
}

#[test]
fn a_download_writes_a_new_file_and_leaves_none_on_failure() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let bytes = content(11);
    let blob = uploaded(&client, &socket, &bytes);
    let path = scratch("download");
    let written = thread::scope(|scope| {
        let call = scope.spawn(|| client.read_blob_to_file(&blob, &path, PATIENCE, &mut never()));
        serve_reads(&socket, &bytes, 11);
        call.join().unwrap()
    });
    assert_eq!(written, Ok(()));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);

    let Err(CallError::File { error, .. }) =
        client.read_blob_to_file(&blob, &path, PATIENCE, &mut never())
    else {
        panic!("an existing file is refused");
    };
    assert_eq!(error.kind, io::ErrorKind::AlreadyExists);
    socket.no_frame();

    let failed = scratch("failed");
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| client.read_blob_to_file(&blob, &failed, PATIENCE, &mut never()));
        let ClientFrame::BlobRead { correlation, .. } = socket.next_frame() else {
            panic!("expected a BlobRead");
        };
        socket.send(ServerFrame::BlobReadResult {
            correlation,
            outcome: BlobReadOutcome::UnknownHandle,
        });
        call.join().unwrap()
    });
    assert_eq!(
        answer,
        Err(CallError::ReadRefused(ReadRefusal::UnknownHandle))
    );
    assert!(!failed.exists(), "a failed download leaves no file");
}

#[test]
fn a_close_is_sent_once_and_ends_reads() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let blob = uploaded(&client, &socket, &content(8));
    let answer = thread::scope(|scope| {
        let call = scope.spawn(|| client.read_blob(&blob, 0, 8, PATIENCE, &mut never()));
        assert!(matches!(socket.next_frame(), ClientFrame::BlobRead { .. }));
        assert_eq!(client.close_blob(&blob, &mut never()), Ok(()));
        call.join().unwrap()
    });
    assert_eq!(answer, Err(CallError::ReadRefused(ReadRefusal::Closed)));
    // The second chunk request may be on the wire before the close.
    loop {
        match socket.next_frame() {
            ClientFrame::BlobRead { .. } => {}
            frame => {
                assert_eq!(
                    frame,
                    ClientFrame::BlobClose {
                        handle: blob.handle
                    }
                );
                break;
            }
        }
    }
    assert_eq!(client.close_blob(&blob, &mut never()), Ok(()));
    assert_eq!(
        client.read_blob(&blob, 0, 1, PATIENCE, &mut never()),
        Err(CallError::BlobGone)
    );
    socket.no_frame();
}

#[test]
fn a_detach_answers_every_blob_call_connection_lost() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let blob = uploaded(&client, &socket, &content(4));
    let lost = CallError::ConnectionLost { reason: gone() };
    thread::scope(|scope| {
        let upload = scope.spawn(|| client.upload_bytes(TYPE, &[1], PATIENCE, &mut never()));
        assert!(matches!(
            socket.next_frame(),
            ClientFrame::BlobCreate { .. }
        ));
        let opening = scope
            .spawn(|| client.open_blob(CHANNEL, Uuid::nil(), name("a"), PATIENCE, &mut never()));
        let reading = scope.spawn(|| client.read_blob(&blob, 0, 4, PATIENCE, &mut never()));
        let mut seen = (false, false);
        while seen != (true, true) {
            match socket.next_frame() {
                ClientFrame::BlobOpen { .. } => seen.0 = true,
                ClientFrame::BlobRead { .. } => seen.1 = true,
                _ => {}
            }
        }
        socket.drop_connection();
        assert_eq!(upload.join().unwrap(), Err(lost.clone()));
        assert_eq!(opening.join().unwrap(), Err(lost.clone()));
        assert_eq!(reading.join().unwrap(), Err(lost));
    });
}

#[test]
fn a_reattachment_forgets_held_blobs_and_takes_the_new_door() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let blob = uploaded(&client, &socket, &[1, 2]);
    let second = controls.succeed();
    drop_and_wait(&client, &socket);
    reattach(&client, &second, "sess-2");
    assert_eq!(
        client.read_blob(&blob, 0, 2, PATIENCE, &mut never()),
        Err(CallError::BlobGone)
    );
    let fresh = uploaded(&client, &second, &[3]);
    assert_ne!(fresh.handle, blob.handle);
    let ok = BlobOpenOutcome::Ok {
        size: 1,
        content_type: TYPE.to_string(),
    };
    assert!(open(&client, &second, ok).is_ok());

    let doorless = controls.succeed();
    drop_and_wait(&client, &second);
    doorless.handshake_with("sess-3", None);
    wait_until("the doorless attachment", || {
        session(&client).as_deref() == Some("sess-3")
    });
    assert_eq!(
        client.upload_bytes(TYPE, &[1], PATIENCE, &mut never()),
        Err(CallError::NoBlobDoor)
    );
    doorless.no_frame();
}

#[test]
fn a_socket_closed_before_the_first_welcome_leaves_uploads_working() {
    let controls = Controls::new();
    let first = controls.succeed();
    let second = controls.succeed();
    let client = controls.start(timings());
    first.hello(VersionRange::exactly(6));
    first.drop_connection();
    second.handshake("sess-2");
    client.await_attachment(PATIENCE, &mut never()).unwrap();
    uploaded(&client, &second, &[1, 2, 3]);
}

#[test]
fn a_cancelled_upload_or_open_is_closed_and_its_late_answer_absorbed() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let brief = Duration::from_millis(200);
    assert_eq!(
        client.upload_bytes(TYPE, &[1], brief, &mut never()),
        Err(CallError::Timeout)
    );
    let (handle, _) = next_create(&socket);
    assert!(matches!(socket.next_frame(), ClientFrame::BlobWrite { .. }));
    assert_eq!(socket.next_frame(), ClientFrame::BlobSeal { handle });
    assert_eq!(socket.next_frame(), ClientFrame::BlobClose { handle });
    socket.send(ServerFrame::BlobCreateResult {
        handle,
        outcome: BlobCreateOutcome::Ok,
    });
    socket.send(ServerFrame::BlobSealResult {
        handle,
        outcome: BlobSealOutcome::Ok,
    });

    assert_eq!(
        client.open_blob(CHANNEL, Uuid::nil(), name("a"), brief, &mut never()),
        Err(CallError::Timeout)
    );
    let ClientFrame::BlobOpen { handle, .. } = socket.next_frame() else {
        panic!("expected a BlobOpen");
    };
    assert_eq!(socket.next_frame(), ClientFrame::BlobClose { handle });
    socket.send(ServerFrame::BlobOpenResult {
        handle,
        outcome: BlobOpenOutcome::NotRetained,
    });
    thread::sleep(QUIET);
    assert!(matches!(client.status(), Status::Attached(_)));
    socket.no_frame();
}

#[test]
fn another_clients_blob_is_refused_before_the_supervisor() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let blob = uploaded(&client, &socket, &[1]);
    let others = Controls::new();
    let (other, other_socket) = others.attached();
    assert_eq!(
        other.read_blob(&blob, 0, 1, PATIENCE, &mut never()),
        Err(CallError::ForeignBlob)
    );
    assert_eq!(
        other.close_blob(&blob, &mut never()),
        Err(CallError::ForeignBlob)
    );
    assert_eq!(
        other.attachment(&blob, name("photo")),
        Err(CallError::ForeignBlob)
    );
    other_socket.no_frame();
}

/// The id of a request these tests make by hand, past any the client mints.
const BY_HAND: u64 = u64::MAX;

#[test]
fn an_abandoned_open_closes_a_blob_its_answer_already_made() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let (reply, answer) = std_mpsc::channel();
    client
        .send(Command::BlobOpen {
            id: BY_HAND,
            channel: CHANNEL.to_string(),
            message_id: Uuid::nil(),
            name: name("a"),
            reply,
        })
        .unwrap();
    let ClientFrame::BlobOpen { handle, .. } = socket.next_frame() else {
        panic!("expected a BlobOpen");
    };
    socket.send(ServerFrame::BlobOpenResult {
        handle,
        outcome: BlobOpenOutcome::Ok {
            size: 1,
            content_type: TYPE.to_string(),
        },
    });
    // The blob is answered into the channel, and nobody reads it.
    thread::sleep(QUIET);
    assert_eq!(
        client.abandon(BY_HAND, Abandoned::Open(answer), CallError::Timeout),
        CallError::Timeout
    );
    assert_eq!(socket.next_frame(), ClientFrame::BlobClose { handle });
    socket.no_frame();
}

#[test]
fn a_chunk_the_plane_did_not_name_is_a_fault_and_closes_the_upload() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    let (reply, answer) = std_mpsc::channel();
    client
        .send(Command::BlobCreate {
            id: BY_HAND,
            content_type: TYPE.to_string(),
            size: 8,
            reply,
        })
        .unwrap();
    let (handle, _) = next_create(&socket);
    let Ok(Ok(UploadStep::Chunk(range))) = answer.recv_timeout(PATIENCE) else {
        panic!("expected the first range");
    };
    client
        .send(Command::BlobChunk {
            id: BY_HAND,
            bytes: vec![0; range.len - 1],
        })
        .unwrap();
    let Ok(Err(CallError::Fault { .. })) = answer.recv_timeout(PATIENCE) else {
        panic!("expected a fault");
    };
    assert_eq!(socket.next_frame(), ClientFrame::BlobClose { handle });
    assert!(matches!(client.status(), Status::Attached(_)));
}

#[test]
fn a_full_buffer_holds_blob_answers_until_read_while_chunks_go_out() {
    let controls = Controls::new();
    let (client, socket) = controls.attached();
    hold(&client, &socket, CHANNEL);
    let full = (1..=EVENT_CAPACITY as u64)
        .map(|seq| row(CHANNEL, seq, 0))
        .collect();
    socket.send(deliver(CHANNEL, full));
    wait_until("a full buffer", || buffered(&client) == EVENT_CAPACITY);

    let bytes = content(10);
    thread::scope(|scope| {
        let call = scope.spawn(|| client.upload_bytes(TYPE, &bytes, PATIENCE, &mut never()));
        let (handle, size) = next_create(&socket);
        assert_eq!(size, 10);
        // The create is unanswered on the wire, yet every chunk and the seal
        // go out: commands are taken while the socket is not read.
        let mut written = Vec::new();
        loop {
            match socket.next_frame() {
                ClientFrame::BlobWrite { offset, bytes, .. } => {
                    assert_eq!(offset, written.len() as u64);
                    written.extend(bytes);
                }
                ClientFrame::BlobSeal { handle: sealed } => {
                    assert_eq!(sealed, handle);
                    break;
                }
                other => panic!("expected a write or the seal, got {other:?}"),
            }
        }
        assert_eq!(written, bytes);
        socket.send(ServerFrame::BlobCreateResult {
            handle,
            outcome: BlobCreateOutcome::Ok,
        });
        socket.send(ServerFrame::BlobSealResult {
            handle,
            outcome: BlobSealOutcome::Ok,
        });
        thread::sleep(QUIET);
        assert!(!call.is_finished(), "the seal's answer is not read");
        assert_eq!(buffered(&client), EVENT_CAPACITY);

        assert_eq!(delivery(recv_now(&client)).seq, 1);
        let blob = call
            .join()
            .unwrap()
            .expect("the upload, once the reader drained");
        assert_eq!((blob.handle, blob.size()), (handle, 10));
    });
}

//! `save` over a real tool socket: the bytes of an attachment or a body
//! byte-identical, each refusal before any byte, a save that outlives its
//! message's eviction, and the deadline and permit bounds.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use brenn_envelope::MessageBody;
use brenn_lib::runtime_dir::tool_socket_in;
use brenn_messaging::{PublishResult, Urgency};
use brenn_messaging_store::blob::BlobLimits;
use brenn_tool_proto::{Request, Response, SaveHeader, SavePart, WireError};
use serde_json::json;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;

use super::TRANSFER_DEADLINE;
use crate::active_bridge::test_support::test_bridge;
use crate::active_bridge::tool_socket::{MAX_IN_FLIGHT, REQUEST_READ_DEADLINE, ToolSocket};
use crate::messaging_intercept::blob_fixture::{
    Fixture, HIDDEN, PRODUCER, RING, RING_DEPTH, SRC, WATCHED, fixture, fixture_with_limits,
};

/// Room for a blob of several pieces and over the request line's cap.
pub(super) const LARGE: BlobLimits = BlobLimits {
    max_blob_bytes: 4 << 20,
    budget_bytes: 16 << 20,
};

/// A blob over 1 MiB and not a whole number of pieces.
pub(super) fn large_bytes() -> Vec<u8> {
    (0..(3 << 19) + 4321)
        .map(|i: u32| (i % 251) as u8)
        .collect()
}

pub(super) struct Socket {
    _root: tempfile::TempDir,
    pub(super) socket: ToolSocket,
}

impl Socket {
    pub(super) fn path(&self) -> PathBuf {
        tool_socket_in(self.socket.host_dir())
    }
}

pub(super) fn serve(f: &Fixture, transfer_deadline: std::time::Duration) -> Socket {
    let root = tempfile::tempdir().unwrap();
    let socket = ToolSocket::bind_with_deadlines(
        root.path(),
        "testapp",
        f.bridge.conversation_id,
        None,
        REQUEST_READ_DEADLINE,
        transfer_deadline,
    )
    .unwrap();
    socket.attach(Arc::downgrade(&f.bridge));
    Socket {
        _root: root,
        socket,
    }
}

fn save_request(message_id: impl ToString, part: SavePart) -> Request {
    Request::Save {
        message_id: message_id.to_string(),
        part,
    }
}

fn attachment(name: &str) -> SavePart {
    SavePart::Attachment(name.to_string())
}

/// Send `request` and read its response line; the reader is left at the
/// first byte after it.
async fn open(path: &Path, request: &Request) -> (Response, BufReader<UnixStream>) {
    let mut stream = UnixStream::connect(path).await.unwrap();
    let mut line = request.to_line().into_bytes();
    line.push(b'\n');
    stream.write_all(&line).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut out = Vec::new();
    reader.read_until(b'\n', &mut out).await.unwrap();
    assert_eq!(out.pop(), Some(b'\n'), "{}", String::from_utf8_lossy(&out));
    (Response::parse(&out).unwrap(), reader)
}

/// A whole save: its header and every byte up to the close.
async fn save(path: &Path, request: &Request) -> (SaveHeader, Vec<u8>) {
    let (response, mut reader) = open(path, request).await;
    let Response::Ok(result) = response else {
        panic!("save refused: {response:?}");
    };
    let header = SaveHeader::from_result(result).unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    (header, bytes)
}

/// A refused save: its tool error JSON, and that nothing follows the line.
async fn refused(path: &Path, request: &Request, reason: &str) -> serde_json::Value {
    let (response, mut reader) = open(path, request).await;
    let Response::Err(WireError::ToolError { output }) = response else {
        panic!("expected a tool error, got {response:?}");
    };
    assert_eq!(output["ok"], json!(false), "{output}");
    assert_eq!(output["reason"], json!(reason), "{output}");
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty(), "{} bytes followed a refusal", rest.len());
    output
}

pub(super) async fn publish_body(f: &Fixture, address: &str, body: MessageBody) -> Uuid {
    match f
        .m
        .publish_from_system_with_attachments(
            PRODUCER,
            address,
            body,
            Urgency::Normal,
            None,
            None,
            brenn_messaging_store::blob::HeldAttachments::none(),
        )
        .await
    {
        PublishResult::Ok { message_id, .. } => message_id,
        other => panic!("{address}: {other:?}"),
    }
}

/// Publish enough to the ring to evict everything it held.
pub(super) async fn flush_ring(f: &Fixture) {
    for _ in 0..RING_DEPTH {
        publish_body(f, RING, MessageBody::text("filler")).await;
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn an_attachment_over_a_piece_and_the_line_cap_is_saved_byte_identical() {
    let f = fixture_with_limits(LARGE).await;
    let bytes = large_bytes();
    let (message, _) = f.attached(SRC, &bytes).await;
    f.read(SRC).await;
    let s = serve(&f, TRANSFER_DEADLINE);

    let (header, saved) = save(&s.path(), &save_request(message, attachment("scan.pdf"))).await;
    assert_eq!(
        header,
        SaveHeader {
            size: bytes.len() as u64,
            content_type: Some("application/octet-stream".into()),
        }
    );
    assert!(saved == bytes, "the saved bytes differ from the blob's");
    assert!(logs_contain("tool socket transfer"));
    assert!(logs_contain("outcome=\"ok\""));
}

#[tokio::test]
async fn a_non_text_body_is_saved_byte_identical_with_its_type() {
    let f = fixture().await;
    let png = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0xff];
    let message = publish_body(
        &f,
        SRC,
        MessageBody::from_parts(Some("image/png"), &png).unwrap(),
    )
    .await;
    f.read(SRC).await;
    let s = serve(&f, TRANSFER_DEADLINE);

    let (header, saved) = save(&s.path(), &save_request(message, SavePart::Body)).await;
    assert_eq!(header.content_type.as_deref(), Some("image/png"));
    assert_eq!(header.size, png.len() as u64);
    assert_eq!(saved, png);
}

#[tokio::test]
async fn an_untyped_text_body_is_saved_with_no_content_type() {
    let f = fixture().await;
    let message = publish_body(&f, SRC, MessageBody::text("plain words")).await;
    f.read(SRC).await;
    let s = serve(&f, TRANSFER_DEADLINE);

    let (header, saved) = save(&s.path(), &save_request(message, SavePart::Body)).await;
    assert_eq!(header.content_type, None);
    assert_eq!(saved, b"plain words");
}

#[tokio::test]
async fn each_name_refusal_is_a_tool_error_before_any_byte() {
    let f = fixture().await;
    let (message, _) = f.attached(SRC, b"bytes").await;
    let s = serve(&f, TRANSFER_DEADLINE);
    let path = s.path();

    let text = refused(
        &path,
        &save_request(message, attachment("scan.pdf")),
        "not_read",
    )
    .await;
    assert!(
        text["error"]
            .as_str()
            .unwrap()
            .contains("MessageChannelGet"),
        "{text}"
    );
    f.read(SRC).await;
    refused(
        &path,
        &save_request("not-a-uuid", attachment("scan.pdf")),
        "malformed_message_id",
    )
    .await;
    refused(
        &path,
        &save_request(message, attachment("../etc/passwd")),
        "malformed_attachment_name",
    )
    .await;
    let missing = refused(
        &path,
        &save_request(message, attachment("other.pdf")),
        "no_such_attachment",
    )
    .await;
    assert!(
        missing["error"].as_str().unwrap().contains("scan.pdf"),
        "{missing}"
    );

    let (hidden, _) = f.attached(HIDDEN, b"secret").await;
    f.m.note_conversation_sent(f.bridge.conversation_id, hidden, HIDDEN);
    refused(
        &path,
        &save_request(hidden, attachment("scan.pdf")),
        "channel_not_readable",
    )
    .await;

    // The attachment saves once every refusal is past.
    let (_, saved) = save(&path, &save_request(message, attachment("scan.pdf"))).await;
    assert_eq!(saved, b"bytes");
}

/// A call's attachment on a watched tool channel is refused
/// `tool_channel_attachment` before any byte, and the call's body saves.
#[tokio::test]
async fn a_watched_tool_channels_attachment_is_refused_and_its_body_saved() {
    let f = fixture().await;
    let (message, _) = f.attached(WATCHED, b"handed bytes").await;
    f.read(WATCHED).await;
    let s = serve(&f, TRANSFER_DEADLINE);
    let path = s.path();

    let text = refused(
        &path,
        &save_request(message, attachment("scan.pdf")),
        "tool_channel_attachment",
    )
    .await;
    assert!(text["error"].as_str().unwrap().contains(WATCHED), "{text}");

    let (header, saved) = save(&path, &save_request(message, SavePart::Body)).await;
    assert_eq!(header.content_type, None);
    assert_eq!(saved, b"a scan");
}

#[tokio::test]
async fn a_message_evicted_before_the_save_is_refused_not_retained() {
    let f = fixture().await;
    let (message, _) = f.attached(RING, b"frame").await;
    f.read(RING).await;
    flush_ring(&f).await;
    let s = serve(&f, TRANSFER_DEADLINE);

    let text = refused(
        &s.path(),
        &save_request(message, attachment("scan.pdf")),
        "not_retained",
    )
    .await;
    assert!(text["error"].as_str().unwrap().contains(RING), "{text}");
}

/// Once admitted, the hold keeps the bytes: evicting the message mid-stream
/// does not cut the save short.
#[tokio::test]
async fn a_save_admitted_before_eviction_completes_from_its_hold() {
    let f = fixture_with_limits(LARGE).await;
    let bytes = large_bytes();
    let (message, blob) = f.attached(RING, &bytes).await;
    f.read(RING).await;
    let s = serve(&f, TRANSFER_DEADLINE);

    let (response, mut reader) =
        open(&s.path(), &save_request(message, attachment("scan.pdf"))).await;
    assert!(matches!(response, Response::Ok(_)), "{response:?}");
    flush_ring(&f).await;
    assert!(
        f.retained(RING)
            .await
            .iter()
            .all(|e| e.message_id != message),
        "the message is evicted"
    );
    let mut saved = Vec::new();
    reader.read_to_end(&mut saved).await.unwrap();
    assert!(saved == bytes, "the saved bytes differ from the blob's");
    drop(reader);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while f.blobs.holds_file(&blob) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the blob goes once the save's hold drops");
}

/// A save holds its connection's permit while its bytes move, and a client
/// that stops reading is cut off at the deadline and gives the permit back.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_stalled_save_ends_at_the_deadline_and_frees_its_permit() {
    let f = fixture_with_limits(LARGE).await;
    let bytes = large_bytes();
    let (message, _) = f.attached(SRC, &bytes).await;
    f.read(SRC).await;
    let s = serve(&f, std::time::Duration::from_millis(300));

    let (response, mut reader) =
        open(&s.path(), &save_request(message, attachment("scan.pdf"))).await;
    assert!(matches!(response, Response::Ok(_)), "{response:?}");
    assert_eq!(s.socket.available_permits(), MAX_IN_FLIGHT - 1);

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while s.socket.available_permits() < MAX_IN_FLIGHT {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the stalled save gives its permit back");
    let mut saved = Vec::new();
    // The server closes with bytes the client never read, which can reach the
    // client as a reset; either way the save is short.
    if let Err(e) = reader.read_to_end(&mut saved).await {
        assert_eq!(e.kind(), std::io::ErrorKind::ConnectionReset, "{e}");
    }
    assert!(saved.len() < bytes.len(), "the save was cut short");
    assert!(logs_contain("tool socket transfer ended early"));
    assert!(logs_contain("deadline"));
    assert!(!logs_contain("tool_socket_violation"));
}

#[tokio::test]
async fn a_bridge_without_a_messenger_answers_the_missing_messenger_error() {
    let root = tempfile::tempdir().unwrap();
    let (bridge, _e, _b, _a) = test_bridge().await;
    let socket = ToolSocket::bind(root.path(), "testapp", 42, None).unwrap();
    socket.attach(Arc::downgrade(&bridge));
    let (response, _) = open(
        &tool_socket_in(socket.host_dir()),
        &save_request(Uuid::new_v4(), SavePart::Body),
    )
    .await;
    let Response::Err(WireError::ToolError { output }) = response else {
        panic!("expected a tool error, got {response:?}");
    };
    assert!(
        output["error"]
            .as_str()
            .unwrap()
            .contains("messaging is not configured"),
        "{output}"
    );
}

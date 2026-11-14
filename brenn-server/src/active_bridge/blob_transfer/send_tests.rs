//! `send_files` over a real tool socket: files attached byte-identical beside
//! forwarded attachments, every refusal before the go-ahead, and a send that
//! leaves no blob when its publish is refused, its connection drops or its
//! deadline passes.

use std::path::Path;
use std::time::Duration;

use brenn_envelope::grants::AppCapability;
use brenn_messaging::AttachmentName;
use brenn_messaging_store::blob::{BlobLimits, BlobUsage};
use brenn_tool_proto::{FileDecl, Request, Response, SendFilesReply, WireError};
use serde_json::json;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;

use super::TRANSFER_DEADLINE;
use super::tests::{LARGE, Socket, large_bytes, serve};
use crate::active_bridge::test_fixtures::swap_single_app;
use crate::active_bridge::tool_socket::MAX_IN_FLIGHT;
use crate::messaging_intercept::blob_fixture::{
    DST, Fixture, HIDDEN, SRC, fixture, fixture_with_limits,
};

/// A fixture whose agent is offered `BrennSend`, which a `send_files` is
/// admitted as, served on a socket with `deadline`.
pub(super) async fn sending(limits: BlobLimits, deadline: Duration) -> (Fixture, Socket) {
    let f = fixture_with_limits(limits).await;
    offer_send(&f);
    let s = serve(&f, deadline);
    (f, s)
}

fn offer_send(f: &Fixture) {
    swap_single_app(&f.bridge.apps, "testapp", |app| {
        app.policy_mut()
            .grants
            .insert(AppCapability::MessagingPublish);
    });
}

fn decl(name: &str, content_type: &str, size: usize) -> FileDecl {
    FileDecl {
        name: name.into(),
        content_type: content_type.into(),
        size: size as u64,
    }
}

async fn read_line(reader: &mut BufReader<UnixStream>) -> Vec<u8> {
    let mut out = Vec::new();
    reader.read_until(b'\n', &mut out).await.unwrap();
    assert_eq!(out.pop(), Some(b'\n'), "{}", String::from_utf8_lossy(&out));
    out
}

/// Send the request line and read the server's first answer.
async fn open(
    path: &Path,
    input: serde_json::Value,
    files: Vec<FileDecl>,
) -> (SendFilesReply, BufReader<UnixStream>) {
    let mut stream = UnixStream::connect(path).await.unwrap();
    let mut line = Request::SendFiles { input, files }.to_line().into_bytes();
    line.push(b'\n');
    stream.write_all(&line).await.unwrap();
    let mut reader = BufReader::new(stream);
    let first = read_line(&mut reader).await;
    (SendFilesReply::parse(&first).unwrap(), reader)
}

/// A whole send: the go-ahead, every file's bytes, and the final answer.
async fn send_files(
    path: &Path,
    input: serde_json::Value,
    files: &[(&str, &str, &[u8])],
) -> Response {
    let decls = files
        .iter()
        .map(|(name, ty, bytes)| decl(name, ty, bytes.len()))
        .collect();
    let (reply, mut reader) = open(path, input, decls).await;
    assert_eq!(reply, SendFilesReply::GoAhead);
    for (_, _, bytes) in files {
        reader.get_mut().write_all(bytes).await.unwrap();
    }
    let last = read_line(&mut reader).await;
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty(), "{} bytes followed the answer", rest.len());
    Response::parse(&last).unwrap()
}

/// A send refused before the go-ahead: its error, and that nothing follows.
async fn refused_early(path: &Path, input: serde_json::Value, files: Vec<FileDecl>) -> WireError {
    let (reply, mut reader) = open(path, input, files).await;
    let SendFilesReply::Refused(error) = reply else {
        panic!("expected a refusal before the go-ahead");
    };
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty(), "{} bytes followed a refusal", rest.len());
    error
}

/// [`refused_early`] as a tool error of kind `reason`; its text.
async fn refused_with(
    path: &Path,
    input: serde_json::Value,
    files: Vec<FileDecl>,
    reason: &str,
) -> String {
    match refused_early(path, input, files).await {
        WireError::ToolError { output } => {
            assert_eq!(output["ok"], json!(false), "{output}");
            assert_eq!(output["reason"], json!(reason), "{output}");
            output["error"].as_str().unwrap().to_string()
        }
        other => panic!("expected a tool error of {reason}, got {other:?}"),
    }
}

fn to(address: &str) -> serde_json::Value {
    json!({ "to": address, "body": "with files" })
}

fn usage(f: &Fixture) -> BlobUsage {
    f.m.blob_store().unwrap().usage()
}

/// Wait until the store's usage is back to `before`: every blob the send
/// made is gone and its charge returned.
async fn usage_returns_to(f: &Fixture, before: BlobUsage) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while usage(f) != before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("usage stayed at {:?}, not {before:?}", usage(f)));
}

fn message_id(response: &Response) -> Uuid {
    let Response::Ok(result) = response else {
        panic!("send refused: {response:?}");
    };
    assert_eq!(result["ok"], json!(true), "{result}");
    result["message_id"].as_str().unwrap().parse().unwrap()
}

/// The bytes of attachment `name` of the retained message `id` on
/// `address`, read through the host's own redeem.
async fn read_back(f: &Fixture, address: &str, id: Uuid, name: &str) -> (String, Vec<u8>) {
    let stored = f.retained(address).await;
    let message = stored
        .iter()
        .find(|e| e.message_id == id)
        .expect("the sent message is retained");
    let reference = message
        .attachments
        .get(&AttachmentName::new(name).unwrap())
        .unwrap_or_else(|| panic!("the message names {name}"))
        .clone();
    let hold =
        f.m.retain_blob(&reference.id)
            .await
            .expect("the blob is held");
    let size = usize::try_from(reference.size).unwrap();
    (reference.content_type, hold.read(0, size).unwrap())
}

#[tokio::test]
#[tracing_test::traced_test]
async fn a_file_over_a_piece_and_the_line_cap_is_attached_byte_identical() {
    let (f, s) = sending(LARGE, TRANSFER_DEADLINE).await;
    let bytes = large_bytes();
    let response = send_files(
        &s.path(),
        to(DST),
        &[("scan.pdf", "application/pdf", &bytes)],
    )
    .await;
    let id = message_id(&response);
    let (content_type, read) = read_back(&f, DST, id, "scan.pdf").await;
    assert_eq!(content_type, "application/pdf");
    assert!(read == bytes, "the attached bytes differ from the file's");
    assert!(f.has_read(id), "the send is recorded for the conversation");
    assert!(logs_contain("op=\"send_files\""));
    assert!(logs_contain("outcome=\"ok\""));
}

/// One send carries a forwarded attachment beside two files, one of them
/// empty; the forward names the source's blob.
#[tokio::test]
async fn a_send_carries_forwarded_and_file_attachments_together() {
    let (f, s) = sending(LARGE, TRANSFER_DEADLINE).await;
    let (source, blob) = f.attached(SRC, b"%PDF forwarded").await;
    f.read(SRC).await;
    let input = json!({
        "to": DST,
        "body": "both",
        "attachments": { "fwd.pdf": { "message_id": source.to_string(), "name": "scan.pdf" } },
    });
    let response = send_files(
        &s.path(),
        input,
        &[
            ("notes.txt", "text/plain; charset=utf-8", b"some notes"),
            ("empty.bin", "application/octet-stream", b""),
        ],
    )
    .await;
    let id = message_id(&response);
    let stored = f.retained(DST).await;
    let message = stored.iter().find(|e| e.message_id == id).unwrap();
    assert_eq!(message.attachments.len(), 3);
    assert_eq!(
        message
            .attachments
            .get(&AttachmentName::new("fwd.pdf").unwrap())
            .unwrap()
            .id,
        blob
    );
    assert_eq!(
        read_back(&f, DST, id, "notes.txt").await,
        (
            "text/plain; charset=utf-8".to_string(),
            b"some notes".to_vec()
        )
    );
    assert_eq!(
        read_back(&f, DST, id, "empty.bin").await.1,
        Vec::<u8>::new()
    );
}

/// The ACL refuses the publish after the bytes crossed: the answer is the
/// send's own refusal and the blob is gone.
#[tokio::test]
async fn a_refused_publish_leaves_no_blob() {
    let (f, s) = sending(LARGE, TRANSFER_DEADLINE).await;
    let before = usage(&f);
    let response = send_files(
        &s.path(),
        to(HIDDEN),
        &[("scan.pdf", "application/pdf", b"never published")],
    )
    .await;
    let Response::Err(WireError::ToolError { output }) = response else {
        panic!("expected the publish's refusal, got {response:?}");
    };
    assert_eq!(output["ok"], json!(false), "{output}");
    assert!(output.get("reason").is_none(), "{output}");
    usage_returns_to(&f, before).await;
}

/// A client that goes away mid-upload leaves no blob, and its short upload
/// is not a protocol violation.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_connection_dropped_mid_upload_leaves_no_blob() {
    let (f, s) = sending(LARGE, TRANSFER_DEADLINE).await;
    let before = usage(&f);
    let bytes = large_bytes();
    let (reply, mut reader) = open(
        &s.path(),
        to(DST),
        vec![decl("scan.pdf", "application/pdf", bytes.len())],
    )
    .await;
    assert_eq!(reply, SendFilesReply::GoAhead);
    assert!(usage(&f).charged_bytes > before.charged_bytes, "reserved");
    reader
        .get_mut()
        .write_all(&bytes[..bytes.len() / 2])
        .await
        .unwrap();
    drop(reader);
    usage_returns_to(&f, before).await;
    assert!(f.retained(DST).await.is_empty(), "nothing was published");
    tokio::time::timeout(Duration::from_secs(10), async {
        while !logs_contain("tool socket transfer ended early") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the short upload is logged");
    assert!(!logs_contain("tool_socket_violation"));
}

/// A client that stops sending is cut off at the deadline with
/// `transfer_aborted`, gives its permit back, and leaves no blob.
#[tokio::test]
#[tracing_test::traced_test]
async fn a_stalled_upload_ends_at_the_deadline() {
    let (f, s) = sending(LARGE, Duration::from_millis(300)).await;
    let before = usage(&f);
    let (reply, mut reader) = open(
        &s.path(),
        to(DST),
        vec![decl("scan.pdf", "application/pdf", 1000)],
    )
    .await;
    assert_eq!(reply, SendFilesReply::GoAhead);
    assert_eq!(s.socket.available_permits(), MAX_IN_FLIGHT - 1);
    reader.get_mut().write_all(&[7; 10]).await.unwrap();
    let last = read_line(&mut reader).await;
    match Response::parse(&last).unwrap() {
        Response::Err(WireError::TransferAborted { detail }) => {
            assert!(detail.contains("deadline"), "{detail}");
            assert!(detail.contains("nothing was published"), "{detail}");
        }
        other => panic!("expected transfer_aborted, got {other:?}"),
    }
    usage_returns_to(&f, before).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while s.socket.available_permits() < MAX_IN_FLIGHT {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the stalled send gives its permit back");
    assert!(!logs_contain("tool_socket_violation"));
}

/// Every declaration-level refusal is a tool error with its reason, before
/// the go-ahead and before any blob exists.
#[tokio::test]
async fn each_declaration_refusal_comes_before_the_go_ahead() {
    let (f, s) = sending(LARGE, TRANSFER_DEADLINE).await;
    let path = s.path();
    let before = usage(&f);
    let pdf = |name: &str| decl(name, "application/pdf", 3);

    refused_with(&path, to(DST), vec![], "no_files").await;
    refused_with(
        &path,
        to(DST),
        vec![pdf("../etc/passwd")],
        "malformed_attachment_name",
    )
    .await;
    let text = refused_with(
        &path,
        to(DST),
        vec![decl("a.bin", "not a type", 3)],
        "malformed_content_type",
    )
    .await;
    assert!(text.contains("a.bin"), "{text}");
    let long = "x".repeat(64 * 1024);
    let text = refused_with(
        &path,
        to(DST),
        vec![decl("a.bin", &long, 3)],
        "malformed_content_type",
    )
    .await;
    assert!(
        text.len() < 2 * brenn_common::MAX_LOGGED_UNTRUSTED_BYTES,
        "{} bytes",
        text.len()
    );
    refused_with(
        &path,
        to(DST),
        vec![pdf("a.pdf"), pdf("a.pdf")],
        "duplicate_attachment_name",
    )
    .await;
    let many = (0..33).map(|i| pdf(&format!("f{i}.pdf"))).collect();
    let text = refused_with(&path, to(DST), many, "too_many_attachments").await;
    assert!(text.contains("33"), "{text}");

    let (source, _) = f.attached(SRC, b"forwarded").await;
    f.read(SRC).await;
    let after_source = usage(&f);
    let forwarding = |n: usize| {
        let mut input = to(DST);
        input["attachments"] =
            json!({ "a.pdf": { "message_id": source.to_string(), "name": "scan.pdf" } });
        let files = (0..n)
            .map(|i| pdf(&format!("f{i}.pdf")))
            .collect::<Vec<_>>();
        (input, files)
    };
    let (input, _) = forwarding(0);
    refused_with(
        &path,
        input,
        vec![pdf("a.pdf")],
        "duplicate_attachment_name",
    )
    .await;
    let (input, files) = forwarding(32);
    refused_with(&path, input, files, "too_many_attachments").await;
    let mut unread = to(DST);
    unread["attachments"] =
        json!({ "a.pdf": { "message_id": Uuid::new_v4().to_string(), "name": "scan.pdf" } });
    refused_with(&path, unread, vec![pdf("b.pdf")], "not_read").await;

    let max = LARGE.max_blob_bytes as usize;
    let text = refused_with(
        &path,
        to(DST),
        vec![decl("big.bin", "application/octet-stream", max + 1)],
        "blob_too_large",
    )
    .await;
    assert!(text.contains(&max.to_string()), "{text}");
    refused_with(
        &path,
        to(DST),
        vec![
            decl("a.bin", "application/octet-stream", max / 2 + 1),
            decl("b.bin", "application/octet-stream", max / 2 + 1),
        ],
        "send_files_too_large",
    )
    .await;
    assert_eq!(usage(&f), after_source, "no refused send left a blob");
    assert_eq!(after_source.sealed, before.sealed + 1);
}

/// The store's budget refuses a file before the go-ahead, not part-way
/// through its bytes.
#[tokio::test]
async fn a_send_over_the_budget_is_refused_before_the_go_ahead() {
    let limits = BlobLimits {
        max_blob_bytes: 1000,
        budget_bytes: 1500,
    };
    let (f, s) = sending(limits, TRANSFER_DEADLINE).await;
    f.attached(SRC, &[1; 1000]).await;
    let before = usage(&f);
    let text = refused_with(
        &s.path(),
        to(DST),
        vec![decl("a.bin", "application/octet-stream", 800)],
        "blob_budget_exhausted",
    )
    .await;
    assert!(
        text.contains("budget of 1500 bytes cannot take file a.bin now; nothing waits"),
        "{text}"
    );
    assert_eq!(usage(&f), before);
}

/// Admission as `BrennSend` answers as `call BrennSend` does: an input that
/// fails the schema is `invalid_args`, an argument `parse_send` rejects is a
/// reason-less tool error, and an agent without the tool hears
/// `unknown_tool`.
#[tokio::test]
async fn admission_answers_as_a_brenn_send_call() {
    let (_f, s) = sending(LARGE, TRANSFER_DEADLINE).await;
    let files = || vec![decl("a.txt", "text/plain", 1)];
    assert!(matches!(
        refused_early(&s.path(), json!({ "body": "no address" }), files()).await,
        WireError::InvalidArgs { .. }
    ));
    match refused_early(
        &s.path(),
        json!({ "to": DST, "body": "b", "deliver_after": "whenever" }),
        files(),
    )
    .await
    {
        WireError::ToolError { output } => {
            assert_eq!(output["ok"], json!(false), "{output}");
            assert!(output.get("reason").is_none(), "{output}");
        }
        other => panic!("expected the argument rejection, got {other:?}"),
    }

    let plain = fixture().await;
    let s = serve(&plain, TRANSFER_DEADLINE);
    assert!(matches!(
        refused_early(&s.path(), to(DST), files()).await,
        WireError::UnknownTool { .. }
    ));
}

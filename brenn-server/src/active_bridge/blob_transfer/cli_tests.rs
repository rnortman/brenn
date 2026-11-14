//! The real `brenn-tool` binary against a real tool socket: a file sent with
//! `send`, read back by the host, saved again with `save` and byte-identical;
//! and the exit code and kind of a name refusal, a file over the store's
//! per-blob cap, and an input that fails the `BrennSend` schema.

use std::path::Path;
use std::process::Stdio;

use brenn_messaging::AttachmentName;
use brenn_messaging::testutils::{TEST_BLOB_LIMITS, redeem_all};
use brenn_tool_proto::SOCKET_ENV;
use serde_json::{Value, json};
use uuid::Uuid;

use super::TRANSFER_DEADLINE;
use super::send_tests::sending;
use super::tests::{LARGE, large_bytes};
use crate::messaging_intercept::blob_fixture::DST;

/// The `brenn-tool` binary built with this server.
const BIN: &str = env!("BRENN_TOOL_BIN");

struct Run {
    code: Option<i32>,
    stdout: Value,
}

/// Run `brenn-tool` with `args` against the socket at `socket`, and parse the
/// one JSON value it prints.
async fn brenn_tool(socket: &Path, args: &[&str]) -> Run {
    let out = tokio::process::Command::new(BIN)
        .args(args)
        .env(SOCKET_ENV, socket)
        .stdin(Stdio::null())
        .output()
        .await
        .expect("spawn brenn-tool");
    let stdout = String::from_utf8(out.stdout).expect("stdout is UTF-8");
    let stdout = serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!(
            "stdout is not one JSON value ({e}): {stdout:?}; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        )
    });
    Run {
        code: out.status.code(),
        stdout,
    }
}

/// `brenn-tool send` of `input` with one file, `scan.pdf`, at `path`.
async fn send_one(socket: &Path, input: &Value, path: &Path) -> Run {
    brenn_tool(
        socket,
        &[
            "send",
            &input.to_string(),
            "--file",
            &format!("scan.pdf={}", path.display()),
            "--file-type",
            "scan.pdf=application/pdf",
        ],
    )
    .await
}

#[tokio::test]
async fn a_file_sent_by_the_cli_is_read_by_the_host_and_saved_byte_identical() {
    let (f, s) = sending(LARGE, TRANSFER_DEADLINE).await;
    let dir = tempfile::tempdir().unwrap();
    let original = dir.path().join("scan.pdf");
    let bytes = large_bytes();
    std::fs::write(&original, &bytes).unwrap();

    let sent = send_one(
        &s.path(),
        &json!({ "to": DST, "body": "with a file" }),
        &original,
    )
    .await;
    assert_eq!(sent.code, Some(0), "{}", sent.stdout);
    assert_eq!(sent.stdout["ok"], json!(true), "{}", sent.stdout);
    let message_id = sent.stdout["message_id"]
        .as_str()
        .expect("the send answers its message id")
        .to_string();

    let retained = f.retained(DST).await;
    let envelope = retained
        .iter()
        .find(|e| e.message_id.to_string() == message_id)
        .expect("the message is retained on its channel");
    let reference = envelope
        .attachments
        .get(&AttachmentName::new("scan.pdf").unwrap())
        .expect("the message carries the file");
    assert_eq!(redeem_all(&f.m, &reference.id).await.unwrap(), bytes);

    let copy = dir.path().join("copy.pdf");
    let saved = brenn_tool(
        &s.path(),
        &[
            "save",
            &message_id,
            "--attachment",
            "scan.pdf",
            "--out",
            &copy.display().to_string(),
        ],
    )
    .await;
    assert_eq!(saved.code, Some(0), "{}", saved.stdout);
    assert_eq!(saved.stdout["size"], json!(bytes.len()), "{}", saved.stdout);
    assert_eq!(
        saved.stdout["content_type"],
        json!("application/pdf"),
        "{}",
        saved.stdout
    );
    assert_eq!(std::fs::read(&copy).unwrap(), bytes);
}

#[tokio::test]
async fn a_name_refusal_exits_one_with_its_reason() {
    let (_f, s) = sending(LARGE, TRANSFER_DEADLINE).await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("never.pdf");
    let refused = brenn_tool(
        &s.path(),
        &[
            "save",
            &Uuid::new_v4().to_string(),
            "--attachment",
            "scan.pdf",
            "--out",
            &out.display().to_string(),
        ],
    )
    .await;
    assert_eq!(refused.code, Some(1), "{}", refused.stdout);
    assert_eq!(
        refused.stdout["reason"],
        json!("not_read"),
        "{}",
        refused.stdout
    );
    assert!(!out.exists(), "a refused save writes no file");
}

#[tokio::test]
async fn a_file_over_the_per_blob_cap_exits_one_with_blob_too_large() {
    let (f, s) = sending(TEST_BLOB_LIMITS, TRANSFER_DEADLINE).await;
    let dir = tempfile::tempdir().unwrap();
    let big = dir.path().join("big.pdf");
    let size = usize::try_from(TEST_BLOB_LIMITS.max_blob_bytes).unwrap() + 1;
    std::fs::write(&big, vec![0u8; size]).unwrap();

    let refused = send_one(&s.path(), &json!({ "to": DST, "body": "too big" }), &big).await;
    assert_eq!(refused.code, Some(1), "{}", refused.stdout);
    assert_eq!(
        refused.stdout["reason"],
        json!("blob_too_large"),
        "{}",
        refused.stdout
    );
    assert!(f.retained(DST).await.is_empty(), "nothing was published");
}

#[tokio::test]
async fn an_input_that_fails_the_schema_exits_two_with_invalid_args() {
    let (f, s) = sending(LARGE, TRANSFER_DEADLINE).await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("scan.pdf");
    std::fs::write(&file, b"%PDF-").unwrap();

    let refused = send_one(&s.path(), &json!({ "body": "no address" }), &file).await;
    assert_eq!(refused.code, Some(2), "{}", refused.stdout);
    assert_eq!(
        refused.stdout["error"],
        json!("invalid_args"),
        "{}",
        refused.stdout
    );
    assert!(f.retained(DST).await.is_empty(), "nothing was published");
}

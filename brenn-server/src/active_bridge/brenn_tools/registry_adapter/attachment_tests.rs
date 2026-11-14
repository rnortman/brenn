//! A conversation's registry tool call carrying attachments: handing a tool
//! blobs it names under the conversation naming rule, and receiving a
//! result's attachments through the app's result inbox.

use std::sync::Arc;

use brenn_lib::messaging::{AttachmentName, app_tool_result_inbox_address};
use brenn_tool_registry::testutil::{ATTACHMENT_ECHO, ATTACHMENT_ECHO_MCP, AttachmentEchoTool};
use brenn_tool_registry::{RegisteredTool, ToolRegistry};
use serde_json::json;
use uuid::Uuid;

use super::super::execute::ToolOutcome;
use super::execute_registry_tool;
use crate::messaging_intercept::blob_fixture::{
    DST, Fixture, RING, RING_DEPTH, SRC, WATCHED, fixture_with_rate_limited_tools,
    fixture_with_tools,
};

fn echo_registry() -> Arc<ToolRegistry> {
    Arc::new(ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(
        AttachmentEchoTool::new(),
    ))]))
}

async fn echo_fixture() -> Fixture {
    fixture_with_tools(echo_registry(), &[ATTACHMENT_ECHO], true).await
}

/// Call the echo for the conversation; `(ok, answer)`.
async fn call_echo(f: &Fixture, input: serde_json::Value) -> (bool, serde_json::Value) {
    match execute_registry_tool(&f.bridge, ATTACHMENT_ECHO_MCP, &input)
        .await
        .expect("a registry tool")
    {
        ToolOutcome::Ok(v) => (true, v),
        ToolOutcome::Err(v) => (false, v),
    }
}

fn hand(message_id: Uuid, name: &str) -> serde_json::Value {
    json!({ "doc": { "message_id": message_id.to_string(), "name": name } })
}

/// Assert `answer` is a naming refusal of kind `reason`.
fn refused(answer: &serde_json::Value, reason: &str) {
    assert_eq!(answer["ok"], json!(false), "expected a refusal: {answer}");
    assert_eq!(answer["reason"], json!(reason), "wrong reason: {answer}");
}

/// The rows on `testapp`'s result inbox.
async fn inbox_rows(f: &Fixture) -> i64 {
    let db = f.m.db().clone();
    let conn = db.lock().await;
    conn.query_row(
        "SELECT COUNT(*) FROM messaging_messages m \
         JOIN messaging_channels c ON m.channel_uuid = c.uuid WHERE c.address = ?1",
        [app_tool_result_inbox_address("testapp")],
        |row| row.get(0),
    )
    .expect("count the inbox")
}

/// A conversation hands the echo an attachment of a message it was shown;
/// the tool sees its own arguments without `attachments`, and the result's
/// attachments come back as an inbox message the conversation may name: the
/// output is `{"result", "message_id", "attachments"}` with no blob id, and a
/// `BrennSend` forwarding the result's attachment names the same blob.
#[tokio::test]
async fn a_handed_blob_reaches_the_tool_and_its_result_is_nameable() {
    let f = echo_fixture().await;
    let (source, blob) = f.attached(SRC, b"%PDF-1.7 bytes").await;
    f.read(SRC).await;
    let before = f.sealed();

    let (ok, answer) = call_echo(
        &f,
        json!({ "note": "n", "attachments": hand(source, "scan.pdf") }),
    )
    .await;
    assert!(ok, "the call refused: {answer}");
    assert_eq!(
        answer["result"],
        json!({
            "args": { "note": "n" },
            "received": { "doc": { "size": 14, "content_type": "application/octet-stream" } },
        })
    );
    assert_eq!(
        answer["attachments"],
        json!({ "doc": { "size": 14, "content_type": "application/octet-stream" } })
    );
    let text = answer.to_string();
    assert!(!text.contains(blob.as_str()), "a blob id leaked: {text}");
    let message_id: Uuid = answer["message_id"]
        .as_str()
        .expect("a message id")
        .parse()
        .expect("a uuid");
    assert!(f.has_read(message_id), "the result is in the read record");
    assert_eq!(inbox_rows(&f).await, 1);
    assert_eq!(f.sealed(), before, "no byte was copied");

    let sent = f
        .send(json!({ "to": DST, "body": "fwd", "attachments": {
            "copy.pdf": { "message_id": message_id.to_string(), "name": "doc" },
        }}))
        .await;
    assert_eq!(sent["ok"], json!(true), "forward refused: {sent}");
    let stored = f.retained(DST).await;
    assert_eq!(
        stored[0]
            .attachments
            .get(&AttachmentName::new("copy.pdf").unwrap())
            .map(|r| r.id.clone()),
        Some(blob),
        "the result names the blob the conversation handed over"
    );
}

/// A result with no attachments is the bare value, and nothing is published.
#[tokio::test]
async fn a_result_without_attachments_is_the_bare_value() {
    let f = echo_fixture().await;
    for input in [json!({}), json!({ "attachments": null })] {
        let (ok, answer) = call_echo(&f, input).await;
        assert!(ok, "{answer}");
        assert_eq!(answer, json!({ "args": {}, "received": {} }));
    }
    assert_eq!(inbox_rows(&f).await, 0);
}

/// A name the conversation was not handed, one whose message left its window,
/// and malformed entries are each refused with the naming refusal's reason, before the
/// tool runs: nothing reaches the inbox.
#[tokio::test]
async fn a_refused_name_refuses_the_call_before_the_tool() {
    let f = echo_fixture().await;
    let (unread, _) = f.attached(SRC, b"x").await;
    refused(
        &call_echo(&f, json!({ "attachments": hand(unread, "scan.pdf") }))
            .await
            .1,
        "not_read",
    );

    let (evicted, _) = f.attached(RING, b"x").await;
    f.read(RING).await;
    for _ in 0..RING_DEPTH {
        f.attached(RING, b"newer").await;
    }
    refused(
        &call_echo(&f, json!({ "attachments": hand(evicted, "scan.pdf") }))
            .await
            .1,
        "not_retained",
    );

    f.read(SRC).await;
    refused(
        &call_echo(
            &f,
            json!({ "attachments": { "doc": { "message_id": "nope", "name": "scan.pdf" } } }),
        )
        .await
        .1,
        "malformed_message_id",
    );
    refused(
        &call_echo(
            &f,
            json!({ "attachments": { "../doc": { "message_id": unread.to_string(), "name": "scan.pdf" } } }),
        )
        .await
        .1,
        "malformed_attachment_name",
    );
    let many: serde_json::Map<String, serde_json::Value> = (0..33)
        .map(|i| {
            (
                format!("a{i}"),
                json!({ "message_id": unread.to_string(), "name": "scan.pdf" }),
            )
        })
        .collect();
    refused(
        &call_echo(&f, json!({ "attachments": many })).await.1,
        "too_many_attachments",
    );
    let (ok, answer) = call_echo(&f, json!({ "attachments": "scan.pdf" })).await;
    assert!(!ok);
    assert_eq!(answer["ok"], json!(false), "{answer}");
    assert_eq!(inbox_rows(&f).await, 0, "the tool never ran");
}

/// Handing the tool an attachment of a call the conversation watched is
/// refused `tool_channel_attachment` before the tool and its rate bucket: the
/// one call the bucket allows is still there for a call that names nothing.
#[tokio::test]
async fn a_watched_tool_channels_attachment_is_refused_before_the_rate_bucket() {
    let f = fixture_with_rate_limited_tools(
        echo_registry(),
        &[ATTACHMENT_ECHO],
        true,
        Some(brenn_lib::tools::ResolvedRateLimit {
            burst: 1,
            sustained_per_minute: 1,
        }),
    )
    .await;
    let (watched, _) = f.attached(WATCHED, b"handed bytes").await;
    f.read(WATCHED).await;

    let (ok, answer) = call_echo(&f, json!({ "attachments": hand(watched, "scan.pdf") })).await;
    assert!(!ok);
    refused(&answer, "tool_channel_attachment");
    assert_eq!(inbox_rows(&f).await, 0, "the tool never ran");

    // A spent bucket would hold this call for the minute a token takes to
    // refill.
    let (ok, answer) =
        tokio::time::timeout(std::time::Duration::from_secs(30), call_echo(&f, json!({})))
            .await
            .expect("the bucket's one call was not spent: the call does not wait for a token");
    assert!(ok, "{answer}");
}

/// An app whose result inbox is gone (a reload removed its last asynchronous
/// grant) answers the call `internal`, and the result's holds drop.
#[tokio::test]
async fn a_result_with_no_inbox_to_rest_on_is_an_internal_error() {
    let f = fixture_with_tools(echo_registry(), &[ATTACHMENT_ECHO], false).await;
    let (source, _) = f.attached(SRC, b"bytes").await;
    f.read(SRC).await;
    let (ok, answer) = call_echo(&f, json!({ "attachments": hand(source, "scan.pdf") })).await;
    assert!(!ok);
    assert_eq!(answer["error_type"], json!("internal"), "{answer}");
    assert_eq!(
        answer["error"],
        json!(
            "the result's attachments could not be delivered: this app has no tool-result \
             channel now"
        )
    );
}

/// The hook path answers CC with the handed result's shape.
#[tokio::test]
async fn the_hook_path_answers_with_the_handed_result() {
    let f = echo_fixture().await;
    let (source, _) = f.attached(SRC, b"bytes").await;
    f.read(SRC).await;
    let short_name = ATTACHMENT_ECHO_MCP
        .strip_prefix(crate::active_bridge::mcp_constants::MCP_BRENN_PREFIX)
        .expect("a Brenn tool's MCP name");
    let parsed = f
        .bridge
        .test_post_tool_use(
            short_name,
            json!({ "attachments": hand(source, "scan.pdf") }),
        )
        .await;
    assert!(parsed["message_id"].is_string(), "{parsed}");
    assert_eq!(parsed["attachments"]["doc"]["size"], json!(5), "{parsed}");
}

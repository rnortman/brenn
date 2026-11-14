//! `content_type` on `BrennSend` and `BrennMessageEdit`: stored with the body,
//! shown to a reader, and refused unless it is a text type.

use serde_json::json;

use super::blob_fixture::{DST, fixture};
use super::*;

fn far_future() -> String {
    (chrono::Utc::now() + chrono::Duration::days(30)).to_rfc3339()
}

/// Assert `answer` is an argument rejection and return its text.
fn rejected(answer: &serde_json::Value) -> String {
    assert_eq!(answer["ok"], json!(false), "expected a rejection: {answer}");
    assert!(
        answer.get("reason").is_none(),
        "a reason-less rejection: {answer}"
    );
    answer["error"].as_str().expect("error text").to_string()
}

#[tokio::test]
async fn a_send_stores_its_content_type_and_a_reader_is_shown_it() {
    let f = fixture().await;
    let answer = f
        .send(json!({ "to": DST, "body": "{\"a\":1}", "content_type": "application/json" }))
        .await;
    assert_eq!(answer["ok"], json!(true), "send refused: {answer}");

    let stored = f.retained(DST).await;
    assert_eq!(stored[0].body.content_type(), Some("application/json"));
    assert_eq!(stored[0].body.as_text(), Some("{\"a\":1}"));

    let shown = f
        .call(
            MCP_MESSAGE_QUERY_CHANNEL_TOOL,
            json!({ "address": DST, "limit": 10 }),
        )
        .await;
    assert_eq!(
        shown[0]["content_type"],
        json!("application/json"),
        "{shown}"
    );
    assert_eq!(shown[0]["body"], json!("{\"a\":1}"), "{shown}");
}

#[tokio::test]
async fn a_send_without_a_content_type_is_untyped() {
    let f = fixture().await;
    let answer = f.send(json!({ "to": DST, "body": "# notes" })).await;
    assert_eq!(answer["ok"], json!(true), "send refused: {answer}");
    let stored = f.retained(DST).await;
    assert_eq!(stored[0].body.content_type(), None);
}

#[tokio::test]
async fn a_text_type_with_a_utf8_charset_is_accepted() {
    let f = fixture().await;
    let answer = f
        .send(json!({ "to": DST, "body": "a,b", "content_type": "text/csv; charset=utf-8" }))
        .await;
    assert_eq!(answer["ok"], json!(true), "send refused: {answer}");
    let stored = f.retained(DST).await;
    assert_eq!(
        stored[0].body.content_type(),
        Some("text/csv; charset=utf-8")
    );
}

#[tokio::test]
async fn a_malformed_content_type_is_rejected_by_name_and_publishes_nothing() {
    let f = fixture().await;
    let text = rejected(
        &f.send(json!({ "to": DST, "body": "x", "content_type": "not a type" }))
            .await,
    );
    assert!(text.contains("malformed `content_type`"), "{text}");
    assert!(text.contains("not a type"), "{text}");
    assert!(f.retained(DST).await.is_empty());
}

#[tokio::test]
async fn a_long_malformed_content_type_is_rejected_with_bounded_text() {
    let f = fixture().await;
    let id = parked(&f).await;
    let long = "x".repeat(64 * 1024);
    let sent = rejected(
        &f.send(json!({ "to": DST, "body": "x", "content_type": long }))
            .await,
    );
    let edited = rejected(
        &f.call(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": id, "body": "x", "content_type": long }),
        )
        .await,
    );
    for text in [sent, edited] {
        assert!(text.contains("malformed `content_type`"), "{text}");
        assert!(
            text.len() < 2 * brenn_common::MAX_LOGGED_UNTRUSTED_BYTES,
            "{} bytes",
            text.len()
        );
    }
}

#[tokio::test]
async fn a_non_text_content_type_is_rejected_and_points_at_attachments() {
    let f = fixture().await;
    for content_type in ["application/pdf", "image/png", "text/plain; charset=latin1"] {
        let text = rejected(
            &f.send(json!({ "to": DST, "body": "x", "content_type": content_type }))
                .await,
        );
        assert!(text.contains(content_type), "{text}");
        assert!(text.contains("not a text type"), "{text}");
        assert!(text.contains("as an attachment"), "{text}");
    }
    assert!(f.retained(DST).await.is_empty());
}

#[tokio::test]
async fn a_content_type_that_is_not_a_string_is_rejected() {
    let f = fixture().await;
    let text = rejected(
        &f.send(json!({ "to": DST, "body": "x", "content_type": 7 }))
            .await,
    );
    assert!(text.contains("`content_type` must be a string"), "{text}");
}

/// Park a message to `DST` and return its id.
async fn parked(f: &super::blob_fixture::Fixture) -> String {
    let answer = f
        .send(json!({ "to": DST, "body": "draft", "deliver_after": far_future() }))
        .await;
    assert_eq!(answer["ok"], json!(true), "send refused: {answer}");
    answer["message_id"]
        .as_str()
        .expect("message_id")
        .to_string()
}

#[tokio::test]
async fn an_edit_replaces_the_body_and_its_content_type_together() {
    let f = fixture().await;
    let id = parked(&f).await;

    let typed = f
        .call(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": id, "body": "<p>hi</p>", "content_type": "text/html" }),
        )
        .await;
    assert_eq!(typed["ok"], json!(true), "edit refused: {typed}");
    assert_eq!(typed["content_type"], json!("text/html"), "{typed}");
    let pending = f.m.list_pending("testapp", Some(DST)).await;
    assert_eq!(pending[0].body.content_type(), Some("text/html"));

    let untyped = f
        .call(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": id, "body": "plain again" }),
        )
        .await;
    assert_eq!(untyped["ok"], json!(true), "edit refused: {untyped}");
    assert!(untyped.get("content_type").is_none(), "{untyped}");
    let pending = f.m.list_pending("testapp", Some(DST)).await;
    assert_eq!(pending[0].body.content_type(), None);
    assert_eq!(pending[0].body.as_text(), Some("plain again"));
}

#[tokio::test]
async fn an_edit_refuses_a_content_type_without_a_body_or_not_text() {
    let f = fixture().await;
    let id = parked(&f).await;

    let text = rejected(
        &f.call(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": id, "content_type": "text/html" }),
        )
        .await,
    );
    assert!(text.contains("`content_type` without `body`"), "{text}");

    let text = rejected(
        &f.call(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": id, "body": "x", "content_type": "application/pdf" }),
        )
        .await,
    );
    assert!(text.contains("not a text type"), "{text}");

    let text = rejected(
        &f.call(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": id, "body": "x", "content_type": "bad" }),
        )
        .await,
    );
    assert!(text.contains("malformed `content_type`"), "{text}");

    let pending = f.m.list_pending("testapp", Some(DST)).await;
    assert_eq!(
        pending[0].body.as_text(),
        Some("draft"),
        "nothing was edited"
    );
    assert_eq!(pending[0].body.content_type(), None);
}

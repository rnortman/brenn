//! `BrennSend`'s forwarded attachments, each name refusal as the tool's error
//! JSON, and the tool answers that feed the conversation's read record.

use serde_json::json;
use uuid::Uuid;

use super::blob_fixture::{DST, HIDDEN, RING, RING_DEPTH, SRC, fixture};
use super::*;

fn forward(message_id: Uuid, name: &str) -> serde_json::Value {
    json!({ "copy.pdf": { "message_id": message_id.to_string(), "name": name } })
}

fn far_future() -> String {
    (chrono::Utc::now() + chrono::Duration::days(30)).to_rfc3339()
}

/// Assert `answer` is a refusal of kind `reason` and return its text.
fn refused(answer: &serde_json::Value, reason: &str) -> String {
    assert_eq!(answer["ok"], json!(false), "expected a refusal: {answer}");
    assert_eq!(answer["reason"], json!(reason), "wrong reason: {answer}");
    answer["error"].as_str().expect("error text").to_string()
}

#[tokio::test]
async fn a_forward_names_the_same_blob_and_creates_none() {
    let f = fixture().await;
    let (source, blob) = f.attached(SRC, b"%PDF-1.7 bytes").await;
    f.read(SRC).await;
    let before = f.sealed();

    let answer = f
        .send(json!({ "to": DST, "body": "fwd", "attachments": forward(source, "scan.pdf") }))
        .await;
    assert_eq!(answer["ok"], json!(true), "forward refused: {answer}");

    let stored = f.retained(DST).await;
    assert_eq!(stored.len(), 1);
    let reference = stored[0]
        .attachments
        .get(&AttachmentName::new("copy.pdf").unwrap())
        .expect("the new message names copy.pdf");
    assert_eq!(reference.id, blob, "the forward names the source's blob");
    assert_eq!(f.sealed(), before, "no blob was created");
}

#[tokio::test]
async fn a_forward_from_a_ring_channel_names_the_same_blob() {
    let f = fixture().await;
    let (source, blob) = f.attached(RING, b"frame").await;
    f.read(RING).await;

    let answer = f
        .send(json!({ "to": DST, "body": "fwd", "attachments": forward(source, "scan.pdf") }))
        .await;
    assert_eq!(answer["ok"], json!(true), "forward refused: {answer}");
    let stored = f.retained(DST).await;
    assert_eq!(
        stored[0]
            .attachments
            .get(&AttachmentName::new("copy.pdf").unwrap())
            .map(|r| r.id.clone()),
        Some(blob)
    );
}

#[tokio::test]
async fn a_parked_forward_carries_its_attachment_to_release() {
    let f = fixture().await;
    let (source, blob) = f.attached(SRC, b"later").await;
    f.read(SRC).await;

    let answer = f
        .send(json!({
            "to": DST,
            "body": "fwd",
            "deliver_after": far_future(),
            "attachments": forward(source, "scan.pdf"),
        }))
        .await;
    assert_eq!(
        answer["ok"],
        json!(true),
        "parked forward refused: {answer}"
    );
    let pending = f.m.list_pending("testapp", Some(DST)).await;
    assert_eq!(
        pending[0]
            .attachments
            .get(&AttachmentName::new("copy.pdf").unwrap())
            .map(|r| r.id.clone()),
        Some(blob)
    );
}

#[tokio::test]
async fn an_unread_message_is_refused_not_read_until_it_is_read() {
    let f = fixture().await;
    let (source, _) = f.attached(SRC, b"x").await;
    let input = json!({ "to": DST, "body": "fwd", "attachments": forward(source, "scan.pdf") });

    let text = refused(&f.send(input.clone()).await, "not_read");
    assert!(text.contains("MessageChannelGet"), "{text}");

    f.read(SRC).await;
    assert_eq!(f.send(input).await["ok"], json!(true));
}

#[tokio::test]
async fn each_malformed_part_of_a_name_is_refused_by_kind() {
    let f = fixture().await;
    let (source, _) = f.attached(SRC, b"x").await;
    f.read(SRC).await;

    let bad_id = json!({ "copy.pdf": { "message_id": "not-a-uuid", "name": "scan.pdf" } });
    let text = refused(
        &f.send(json!({ "to": DST, "body": "b", "attachments": bad_id }))
            .await,
        "malformed_message_id",
    );
    assert!(text.contains("not-a-uuid"), "{text}");

    let bad_new = json!({ ".hidden": { "message_id": source.to_string(), "name": "scan.pdf" } });
    refused(
        &f.send(json!({ "to": DST, "body": "b", "attachments": bad_new }))
            .await,
        "malformed_attachment_name",
    );

    refused(
        &f.send(json!({ "to": DST, "body": "b", "attachments": forward(source, "a/b") }))
            .await,
        "malformed_attachment_name",
    );
}

#[tokio::test]
async fn a_name_the_message_does_not_carry_lists_what_it_does() {
    let f = fixture().await;
    let (source, _) = f.attached(SRC, b"x").await;
    f.read(SRC).await;

    let text = refused(
        &f.send(json!({ "to": DST, "body": "b", "attachments": forward(source, "other.pdf") }))
            .await,
        "no_such_attachment",
    );
    assert!(text.contains("scan.pdf"), "{text}");
}

#[tokio::test]
async fn an_evicted_message_is_refused_not_retained() {
    let f = fixture().await;
    let (source, _) = f.attached(RING, b"x").await;
    f.read(RING).await;
    for _ in 0..RING_DEPTH {
        f.attached(RING, b"newer").await;
    }

    let text = refused(
        &f.send(json!({ "to": DST, "body": "b", "attachments": forward(source, "scan.pdf") }))
            .await,
        "not_retained",
    );
    assert!(text.contains(RING), "{text}");
}

#[tokio::test]
async fn a_channel_the_app_may_not_read_is_refused_by_name() {
    let f = fixture().await;
    let (source, _) = f.attached(HIDDEN, b"x").await;
    f.m.note_conversation_sent(f.bridge.conversation_id, source, HIDDEN);

    let text = refused(
        &f.send(json!({ "to": DST, "body": "b", "attachments": forward(source, "scan.pdf") }))
            .await,
        "channel_not_readable",
    );
    assert!(text.contains(HIDDEN), "{text}");
}

#[tokio::test]
async fn a_33rd_attachment_is_refused_by_count() {
    let f = fixture().await;
    let (source, _) = f.attached(SRC, b"x").await;
    f.read(SRC).await;
    let entries: serde_json::Map<String, serde_json::Value> = (0..=MAX_ATTACHMENTS_PER_MESSAGE)
        .map(|i| {
            (
                format!("copy{i}"),
                json!({ "message_id": source.to_string(), "name": "scan.pdf" }),
            )
        })
        .collect();

    let text = refused(
        &f.send(json!({ "to": DST, "body": "b", "attachments": entries }))
            .await,
        "too_many_attachments",
    );
    assert!(text.contains("33"), "{text}");
}

#[tokio::test]
async fn an_entry_of_the_wrong_shape_is_an_argument_rejection() {
    let f = fixture().await;
    for attachments in [
        json!("scan.pdf"),
        json!({ "copy.pdf": "scan.pdf" }),
        json!({ "copy.pdf": { "message_id": Uuid::nil().to_string() } }),
        json!({ "copy.pdf": { "message_id": Uuid::nil().to_string(), "name": "a", "x": 1 } }),
    ] {
        let answer = f
            .send(json!({ "to": DST, "body": "b", "attachments": attachments }))
            .await;
        assert_eq!(answer["ok"], json!(false), "{answer}");
        assert!(answer.get("reason").is_none(), "no reason kind: {answer}");
    }
}

#[tokio::test]
async fn a_refused_send_publishes_nothing_and_spends_no_budget() {
    let f = fixture().await;
    let (source, _) = f.attached(SRC, b"x").await;
    refused(
        &f.send(json!({ "to": DST, "body": "b", "attachments": forward(source, "scan.pdf") }))
            .await,
        "not_read",
    );
    assert!(f.retained(DST).await.is_empty(), "nothing was published");

    let answer = f.send(json!({ "to": DST, "body": "plain" })).await;
    assert_eq!(
        answer["remaining_budget"],
        json!(99u32),
        "only the successful send drew on the budget: {answer}"
    );
}

#[tokio::test]
async fn each_tool_answer_that_hands_over_a_message_records_it() {
    let f = fixture().await;
    let (read, _) = f.attached(SRC, b"x").await;
    assert!(!f.has_read(read));
    f.read(SRC).await;
    assert!(
        f.has_read(read),
        "MessageChannelGet records what it returns"
    );

    let answer = f
        .send(json!({ "to": DST, "body": "parked", "deliver_after": far_future() }))
        .await;
    let parked: Uuid = answer["message_id"].as_str().unwrap().parse().unwrap();
    assert!(f.has_read(parked), "a send's answer records the message");

    f.m.forget_conversation_reads(f.bridge.conversation_id);
    let listed = f
        .call(MCP_MESSAGE_PENDING_LIST_TOOL, json!({ "channel": DST }))
        .await;
    assert_eq!(listed["messages"].as_array().map(Vec::len), Some(1));
    assert!(f.has_read(parked), "BrennPendingList records what it lists");

    f.m.forget_conversation_reads(f.bridge.conversation_id);
    let edited = f
        .call(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": parked.to_string(), "body": "edited" }),
        )
        .await;
    assert_eq!(edited["ok"], json!(true), "{edited}");
    assert!(f.has_read(parked), "an edit's answer records the message");
}

#[tokio::test]
async fn a_parked_message_is_not_retained_until_released() {
    let f = fixture().await;
    let (source, _) = f.attached(SRC, b"x").await;
    f.read(SRC).await;
    let answer = f
        .send(json!({
            "to": DST,
            "body": "parked",
            "deliver_after": far_future(),
            "attachments": forward(source, "scan.pdf"),
        }))
        .await;
    let parked: Uuid = answer["message_id"].as_str().unwrap().parse().unwrap();

    let text = refused(
        &f.send(json!({ "to": DST, "body": "b", "attachments": forward(parked, "copy.pdf") }))
            .await,
        "not_retained",
    );
    assert!(text.contains(DST), "{text}");
}

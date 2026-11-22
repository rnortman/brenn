//! `MqttSend` over a messenger with a blob store: the `attachment` form, what
//! a call commits, each refusal of a reference before the budget, the client's
//! outbound bound on a blob, and an inline payload over the body cap riding as
//! a blob.

use serde_json::json;
use uuid::Uuid;

use base64ct::Encoding as _;

use super::*;
use crate::active_bridge::test_support::send_budget_row;
use crate::messaging_intercept::blob_fixture::{
    Fixture, HIDDEN, MQTT_CLIENT, PRODUCER, SRC, fixture_with_mqtt, fixture_with_mqtt_limits,
};
use brenn_envelope::{MessageEnvelope, TransportMeta};
use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{egress_channel_uuid, mqtt_egress_address};
use brenn_messaging::testutils::publish_attached;
use brenn_messaging_store::blob::BlobLimits;
use brenn_messaging_store::testutils::seal_bytes;

use crate::test_support::mqtt::register_test_client;

const TO: &str = "mqtt:home:cam/snapshot";

/// A message from the producer on `address` carrying `snap.png`, `bytes`
/// typed `image/png`.
async fn snapshot(f: &Fixture, address: &str, bytes: &[u8]) -> Uuid {
    let hold = seal_bytes(
        f.m.blob_store().expect("the fixture has a blob store"),
        "image/png",
        bytes,
    );
    publish_attached(
        &f.m,
        PRODUCER,
        address,
        "a snapshot",
        &[("snap.png", &hold)],
        None,
    )
    .await
}

fn reference(message_id: Uuid, name: &str) -> serde_json::Value {
    json!({ "message_id": message_id.to_string(), "name": name })
}

/// Run `MqttSend` with `input`, a lifetime of 60 s added when it states none.
async fn call(f: &Fixture, mut input: serde_json::Value) -> Option<ToolOutcome> {
    input
        .as_object_mut()
        .unwrap()
        .entry("lifetime_s")
        .or_insert_with(|| json!(60));
    execute_mqtt_tool(&f.bridge, MCP_MQTT_SEND_TOOL, &input).await
}

/// The rows committed to the client's egress channel, oldest first.
async fn committed(f: &Fixture) -> Vec<MessageEnvelope> {
    let conn = f.m.db().lock().await;
    brenn_messaging_store::db::load_channel_retained_tail(
        &conn,
        egress_channel_uuid(&mqtt_egress_address(MQTT_CLIENT)),
        Depth::Unbounded,
    )
    .into_iter()
    .map(|(_, envelope)| envelope)
    .collect()
}

/// Commit `input` and return its row, which carries exactly one attachment,
/// `name`, beside an empty body, and that attachment's bytes.
async fn commit_attached(
    f: &Fixture,
    input: serde_json::Value,
    name: &str,
) -> (MessageEnvelope, Vec<u8>) {
    match call(f, input.clone()).await {
        Some(ToolOutcome::Ok(v)) => assert_eq!(v["ok"], json!(true), "{v}"),
        other => panic!("expected {input} to commit, got {other:?}"),
    }
    let row = committed(f).await.pop().expect("a committed row");
    assert!(row.body.as_bytes().is_empty());
    assert_eq!(row.attachments.len(), 1);
    let name = brenn_envelope::AttachmentName::new(name).unwrap();
    let blob = row.attachments.get(&name).expect("the named attachment");
    let size = usize::try_from(blob.size).unwrap();
    let held =
        f.m.hold_attachments(&row.attachments)
            .await
            .expect("a retained row's blob is held");
    let bytes = held.hold(&name).unwrap().read(0, size).unwrap();
    (row, bytes)
}

fn transport(row: &MessageEnvelope) -> &brenn_envelope::MqttPublishTransport {
    match row.transport.as_ref() {
        Some(TransportMeta::MqttPublish(t)) => t,
        other => panic!("expected MqttPublish metadata, got {other:?}"),
    }
}

/// Assert `answer` is a refusal of kind `reason`, spent no budget, and return
/// its text.
async fn refused(f: &Fixture, answer: Option<ToolOutcome>, reason: &str) -> String {
    let Some(ToolOutcome::Err(v)) = answer else {
        panic!("expected a refusal, got {answer:?}");
    };
    assert_eq!(v["ok"], json!(false), "{v}");
    assert_eq!(v["reason"], json!(reason), "wrong reason: {v}");
    assert_eq!(
        send_budget_row(&f.bridge).await,
        None,
        "a refusal spends no budget"
    );
    v["error"].as_str().expect("error text").to_string()
}

/// Assert `answer` is an argument rejection, with no `reason`, that spent no
/// budget, and return its text.
async fn rejected(f: &Fixture, answer: Option<ToolOutcome>) -> String {
    let Some(ToolOutcome::Err(v)) = answer else {
        panic!("expected a rejection, got {answer:?}");
    };
    assert_eq!(v["ok"], json!(false), "{v}");
    assert!(v.get("reason").is_none(), "{v}");
    assert_eq!(
        send_budget_row(&f.bridge).await,
        None,
        "a rejection spends no budget"
    );
    v["error"].as_str().expect("error text").to_string()
}

#[tokio::test]
async fn an_attachment_commits_the_blob_by_reference_with_its_content_type() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"\x89PNG\r\n\x1a\n pixels").await;
    f.read(SRC).await;

    let (row, bytes) = commit_attached(
        &f,
        json!({
            "to": TO,
            "attachment": reference(source, "snap.png"),
            "qos": 2,
            "retain": true,
        }),
        "snap.png",
    )
    .await;
    assert_eq!(bytes, b"\x89PNG\r\n\x1a\n pixels");
    let t = transport(&row);
    assert_eq!(t.content_type.as_deref(), Some("image/png"));
    assert_eq!(
        (t.client_slug.as_str(), t.topic.as_str(), t.qos, t.retain),
        (MQTT_CLIENT, "cam/snapshot", 2, true)
    );
}

#[tokio::test]
async fn an_attachment_call_answers_at_commit_and_spends_one_budget_unit() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"pixels").await;
    f.read(SRC).await;

    let Some(ToolOutcome::Ok(v)) = call(
        &f,
        json!({ "to": TO, "attachment": reference(source, "snap.png"), "qos": 0 }),
    )
    .await
    else {
        panic!("expected a commit");
    };
    let row = committed(&f).await.pop().expect("a committed row");
    assert_eq!(v["message_id"], json!(row.message_id.to_string()), "{v}");
    assert_eq!(v["client_connected"], json!(false), "{v}");
    assert_eq!(
        send_budget_row(&f.bridge).await,
        Some(f.bridge.app_config_default_send_budget() - 1)
    );
}

#[tokio::test]
async fn an_attachment_of_zero_bytes_commits_an_empty_payload() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"").await;
    f.read(SRC).await;

    let (_, bytes) = commit_attached(
        &f,
        json!({ "to": TO, "attachment": reference(source, "snap.png") }),
        "snap.png",
    )
    .await;
    assert!(bytes.is_empty());
}

const MORE_THAN_ONE: &str = "more than one payload argument is given: give exactly one of \
                             `body`, `body_base64` and `attachment`";

#[tokio::test]
async fn two_payload_arguments_are_rejected() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"pixels").await;
    f.read(SRC).await;

    for input in [
        json!({ "to": TO, "body": "hi", "attachment": reference(source, "snap.png") }),
        json!({ "to": TO, "body_base64": "AAEC", "attachment": reference(source, "snap.png") }),
        json!({ "to": TO, "body": "hi", "body_base64": "AAEC" }),
        json!({
            "to": TO,
            "body": "hi",
            "body_base64": "AAEC",
            "attachment": reference(source, "snap.png"),
        }),
    ] {
        let text = rejected(&f, call(&f, input.clone()).await).await;
        assert_eq!(text, MORE_THAN_ONE, "{input}");
    }
}

#[tokio::test]
async fn no_payload_argument_is_rejected() {
    let f = fixture_with_mqtt(1024).await;
    let text = rejected(&f, call(&f, json!({ "to": TO })).await).await;
    assert_eq!(
        text,
        "missing payload: give exactly one of `body`, `body_base64` and `attachment`"
    );
}

#[tokio::test]
async fn content_type_with_an_attachment_is_rejected() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"pixels").await;
    f.read(SRC).await;

    let text = rejected(
        &f,
        call(
            &f,
            json!({
                "to": TO,
                "attachment": reference(source, "snap.png"),
                "content_type": "image/png",
            }),
        )
        .await,
    )
    .await;
    assert_eq!(
        text,
        "`content_type` is given with `attachment`: an attachment publishes its own recorded \
         content type; drop `content_type`"
    );
}

#[tokio::test]
async fn a_reference_with_a_third_field_is_rejected_naming_attachment() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"pixels").await;
    f.read(SRC).await;

    let text = rejected(
        &f,
        call(
            &f,
            json!({
                "to": TO,
                "attachment": {
                    "message_id": source.to_string(),
                    "name": "snap.png",
                    "as": "copy.png",
                },
            }),
        )
        .await,
    )
    .await;
    assert_eq!(
        text,
        "`attachment` must be {\"message_id\": \"<uuid>\", \"name\": \"<attachment name>\"}"
    );
    assert!(!text.contains("attachments"), "{text}");
}

#[tokio::test]
async fn a_malformed_message_id_is_refused_by_kind() {
    let f = fixture_with_mqtt(1024).await;
    let text = refused(
        &f,
        call(
            &f,
            json!({ "to": TO, "attachment": { "message_id": "not-a-uuid", "name": "snap.png" } }),
        )
        .await,
        "malformed_message_id",
    )
    .await;
    assert!(text.contains("not-a-uuid"), "{text}");
}

#[tokio::test]
async fn an_unread_message_is_refused_not_read() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"pixels").await;

    let text = refused(
        &f,
        call(
            &f,
            json!({ "to": TO, "attachment": reference(source, "snap.png") }),
        )
        .await,
        "not_read",
    )
    .await;
    assert!(text.starts_with("`attachment`: "), "{text}");
    assert!(text.contains("MessageChannelGet"), "{text}");
}

#[tokio::test]
async fn a_message_on_a_channel_the_app_may_not_read_is_refused() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, HIDDEN, b"pixels").await;
    f.m.note_conversation_sent(f.bridge.conversation_id, source, HIDDEN);

    let text = refused(
        &f,
        call(
            &f,
            json!({ "to": TO, "attachment": reference(source, "snap.png") }),
        )
        .await,
        "channel_not_readable",
    )
    .await;
    assert!(text.contains(HIDDEN), "{text}");
}

#[tokio::test]
async fn a_name_the_message_does_not_carry_is_refused() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"pixels").await;
    f.read(SRC).await;

    let text = refused(
        &f,
        call(
            &f,
            json!({ "to": TO, "attachment": reference(source, "other.png") }),
        )
        .await,
        "no_such_attachment",
    )
    .await;
    assert!(text.contains("snap.png"), "{text}");
}

#[tokio::test]
async fn an_attachment_over_the_bound_is_refused_before_the_budget_and_never_read() {
    let f = fixture_with_mqtt(4).await;
    let hold = seal_bytes(
        f.m.blob_store().expect("the fixture has a blob store"),
        "image/png",
        b"fives",
    );
    let source = publish_attached(
        &f.m,
        PRODUCER,
        SRC,
        "a snapshot",
        &[("snap.png", &hold)],
        None,
    )
    .await;
    // The recorded size stays 5; a read of the emptied file would panic.
    f.blobs.empty_file(hold.id());
    f.read(SRC).await;

    let text = refused(
        &f,
        call(
            &f,
            json!({ "to": TO, "attachment": reference(source, "snap.png") }),
        )
        .await,
        "payload_too_large",
    )
    .await;
    assert_eq!(
        text,
        "payload is 5 bytes; MQTT client `home` publishes at most 4 \
         (`outbound_payload_cap_bytes`)"
    );
}

#[tokio::test]
async fn an_attachment_exactly_at_the_bound_commits() {
    let f = fixture_with_mqtt(4).await;
    let source = snapshot(&f, SRC, b"four").await;
    f.read(SRC).await;

    let (_, bytes) = commit_attached(
        &f,
        json!({ "to": TO, "attachment": reference(source, "snap.png") }),
        "snap.png",
    )
    .await;
    assert_eq!(bytes, b"four");
}

#[tokio::test]
async fn an_acl_denial_with_an_oversize_attachment_answers_the_denial() {
    let f = fixture_with_mqtt(4).await;
    register_test_client(f.bridge.mqtt_service(), "office", 4);
    let source = snapshot(&f, SRC, b"far too long").await;
    f.read(SRC).await;

    assert_eq!(
        call(
            &f,
            json!({
                "to": "mqtt:office:cam/snapshot",
                "attachment": reference(source, "snap.png"),
            })
        )
        .await,
        Some(ToolOutcome::Err(json!({
            "ok": false,
            "error": "MQTT publish to `mqtt:office:cam/snapshot` is not allowed for this app: \
                      no `acl publish` entry admits that topic on client `office`; the \
                      operator adds `topic_filter \"mqtt:office:<filter>\"` to the app's \
                      `acl publish`",
        })))
    );
    assert_eq!(send_budget_row(&f.bridge).await, None);
}

#[tokio::test]
async fn an_inline_payload_over_the_body_cap_commits_as_one_payload_blob() {
    let f = fixture_with_mqtt(4 * 1024 * 1024).await;
    let max = f.m.max_body_bytes();
    let text = "x".repeat(max + 1);
    let (row, bytes) = commit_attached(
        &f,
        json!({ "to": TO, "body": text, "content_type": "text/plain" }),
        "payload",
    )
    .await;
    assert_eq!(bytes, text.as_bytes());
    assert_eq!(transport(&row).content_type.as_deref(), Some("text/plain"));
}

#[tokio::test]
async fn an_inline_payload_the_blob_store_refuses_is_unstorable() {
    let max_blob_bytes = 70 * 1024;
    let f = fixture_with_mqtt_limits(
        4 * 1024 * 1024,
        BlobLimits {
            max_blob_bytes,
            budget_bytes: 4 << 20,
        },
    )
    .await;
    let answer = call(
        &f,
        json!({ "to": TO, "body_base64": base64ct::Base64::encode_string(&vec![7u8; 80 * 1024]) }),
    )
    .await;
    let Some(ToolOutcome::Err(v)) = answer else {
        panic!("expected a refusal, got {answer:?}");
    };
    assert_eq!(v["reason"], json!("payload_unstorable"), "{v}");
    assert!(
        v["error"]
            .as_str()
            .unwrap()
            .starts_with("the blob store refused the payload: "),
        "{v}"
    );
    assert!(committed(&f).await.is_empty());
    assert_ne!(
        send_budget_row(&f.bridge).await,
        Some(f.bridge.app_config_default_send_budget() - 1),
        "a refused blob spends no budget"
    );
}

//! `MqttSend`'s `attachment` form: what a call publishes, each refusal of a
//! reference before the budget, and the client's outbound bound on a blob.

use serde_json::json;
use uuid::Uuid;

use super::*;
use crate::active_bridge::test_support::send_budget_row;
use crate::messaging_intercept::blob_fixture::{
    Fixture, HIDDEN, MQTT_CLIENT, PRODUCER, SRC, fixture_with_mqtt,
};
use brenn_messaging::testutils::publish_attached;
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

async fn call(f: &Fixture, input: serde_json::Value) -> Option<ToolOutcome> {
    execute_mqtt_tool(&f.bridge, MCP_MQTT_SEND_TOOL, &input).await
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
async fn an_attachment_publishes_the_blobs_bytes_and_content_type() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"\x89PNG\r\n\x1a\n pixels").await;
    f.read(SRC).await;

    let input = json!({
        "to": TO,
        "attachment": reference(source, "snap.png"),
        "qos": 2,
        "retain": true,
    });
    let p = match prepare(&f.bridge, MCP_MQTT_SEND_TOOL, &input).await {
        Ok(p) => p,
        Err(answer) => panic!("expected the call to be prepared, got {answer:?}"),
    };
    assert_eq!(p.payload, b"\x89PNG\r\n\x1a\n pixels");
    assert_eq!(
        p.props,
        PublishProps {
            content_type: Some("image/png".to_string()),
            qos: 2,
            retain: true,
            message_expiry_s: None,
        }
    );
    assert_eq!(p.authorized.client().slug, MQTT_CLIENT);
}

#[tokio::test]
async fn an_attachment_call_answers_ok_and_spends_one_budget_unit() {
    let f = fixture_with_mqtt(1024).await;
    let handle = f
        .bridge
        .mqtt_service()
        .get_client(MQTT_CLIENT)
        .expect("the fixture registers a session");
    let _eventloop = brenn_mqtt::test_support::install_unpolled_client(&handle, None).await;
    let source = snapshot(&f, SRC, b"pixels").await;
    f.read(SRC).await;

    assert_eq!(
        call(
            &f,
            json!({ "to": TO, "attachment": reference(source, "snap.png"), "qos": 0 })
        )
        .await,
        Some(ToolOutcome::Ok(json!({ "ok": true })))
    );
    assert_eq!(
        send_budget_row(&f.bridge).await,
        Some(f.bridge.app_config_default_send_budget() - 1)
    );
}

#[tokio::test]
async fn an_attachment_of_zero_bytes_publishes_an_empty_payload() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"").await;
    f.read(SRC).await;

    let input = json!({ "to": TO, "attachment": reference(source, "snap.png") });
    let p = match prepare(&f.bridge, MCP_MQTT_SEND_TOOL, &input).await {
        Ok(p) => p,
        Err(answer) => panic!("expected the call to be prepared, got {answer:?}"),
    };
    assert!(p.payload.is_empty());
}

#[tokio::test]
async fn body_and_attachment_together_are_rejected() {
    let f = fixture_with_mqtt(1024).await;
    let source = snapshot(&f, SRC, b"pixels").await;
    f.read(SRC).await;

    let text = rejected(
        &f,
        call(
            &f,
            json!({ "to": TO, "body": "hi", "attachment": reference(source, "snap.png") }),
        )
        .await,
    )
    .await;
    assert_eq!(
        text,
        "both `body` and `attachment` are given; give exactly one"
    );
}

#[tokio::test]
async fn neither_body_nor_attachment_is_rejected() {
    let f = fixture_with_mqtt(1024).await;
    let text = rejected(&f, call(&f, json!({ "to": TO })).await).await;
    assert_eq!(
        text,
        "missing payload: give exactly one of `body` and `attachment`"
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
async fn an_attachment_exactly_at_the_bound_is_prepared() {
    let f = fixture_with_mqtt(4).await;
    let source = snapshot(&f, SRC, b"four").await;
    f.read(SRC).await;

    let input = json!({ "to": TO, "attachment": reference(source, "snap.png") });
    let p = match prepare(&f.bridge, MCP_MQTT_SEND_TOOL, &input).await {
        Ok(p) => p,
        Err(answer) => panic!("expected the call to be prepared, got {answer:?}"),
    };
    assert_eq!(p.payload, b"four");
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
            "error": "MQTT publish to client `office` is not allowed for this app: \
                      add an \"mqtt_publish\" ACL matcher for that client",
        })))
    );
    assert_eq!(send_budget_row(&f.bridge).await, None);
}

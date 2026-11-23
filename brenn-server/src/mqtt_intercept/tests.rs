//! `MqttSend`: its arguments, what it commits to the client's egress channel,
//! its answer at commit, and each refusal the bus can give it.

use std::sync::Arc;

use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
use brenn_envelope::{MessageEnvelope, MqttExpiry, MqttPublishTransport, TransportMeta};
use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{egress_channel_uuid, mqtt_egress_address};
use serde_json::json;

use super::*;
use crate::active_bridge::test_fixtures::BrennStatusChannels;
use crate::active_bridge::test_support::{post_tool_use_req, pre_tool_use_req, send_budget_row};
use crate::test_support::mqtt::register_test_client;

/// The rows committed to `client`'s egress channel, oldest first.
async fn committed(bridge: &ActiveBridge, client: &str) -> Vec<MessageEnvelope> {
    let messenger = bridge.messenger().expect("an MQTT bridge has a messenger");
    let conn = messenger.db().lock().await;
    brenn_messaging_store::db::load_channel_retained_tail(
        &conn,
        egress_channel_uuid(&mqtt_egress_address(client)),
        Depth::Unbounded,
    )
    .into_iter()
    .map(|(_, envelope)| envelope)
    .collect()
}

/// The one row committed to `client`'s egress channel.
async fn the_row(bridge: &ActiveBridge, client: &str) -> MessageEnvelope {
    let mut rows = committed(bridge, client).await;
    assert_eq!(rows.len(), 1, "one row committed: {rows:?}");
    rows.remove(0)
}

fn transport(envelope: &MessageEnvelope) -> &MqttPublishTransport {
    match envelope.transport.as_ref() {
        Some(TransportMeta::MqttPublish(t)) => t,
        other => panic!("expected MqttPublish metadata, got {other:?}"),
    }
}

/// Run `MqttSend` with `input` through the PostToolUse hook and return the
/// answer it gives CC.
async fn through_hook(bridge: &ActiveBridge, input: serde_json::Value) -> serde_json::Value {
    let req = post_tool_use_req(MCP_MQTT_SEND_TOOL, input);
    match try_handle_mqtt_tool(bridge, &req).await {
        Some(MqttHandled::Respond(CcApprovalDecision::Continue {
            updated_output: Some(out),
        })) => serde_json::from_str(&out).unwrap(),
        other => panic!("unexpected: {other:?}"),
    }
}

async fn call(bridge: &ActiveBridge, input: serde_json::Value) -> Option<ToolOutcome> {
    execute_mqtt_tool(bridge, MCP_MQTT_SEND_TOOL, &input).await
}

/// Assert `answer` is a refusal of kind `reason` that committed nothing and
/// spent no budget, and return its text.
async fn refused(bridge: &ActiveBridge, answer: Option<ToolOutcome>, reason: &str) -> String {
    let Some(ToolOutcome::Err(v)) = answer else {
        panic!("expected a refusal, got {answer:?}");
    };
    assert_eq!(v["ok"], json!(false), "{v}");
    assert_eq!(v["reason"], json!(reason), "wrong reason: {v}");
    assert!(committed(bridge, "home").await.is_empty());
    assert_eq!(
        send_budget_row(bridge).await,
        None,
        "a refusal spends no budget"
    );
    v["error"].as_str().expect("error text").to_string()
}

/// Assert the call is an argument rejection with exactly `error` that
/// committed nothing and spent no budget.
async fn assert_rejected(bridge: &ActiveBridge, input: serde_json::Value, error: &str) {
    assert_eq!(
        call(bridge, input.clone()).await,
        Some(ToolOutcome::Err(json!({ "ok": false, "error": error }))),
        "{input}"
    );
    assert!(committed(bridge, "home").await.is_empty());
    assert_eq!(send_budget_row(bridge).await, None);
}

/// A bridge whose app may publish anywhere on `home`, with a registered,
/// unconnected session for `home` that publishes at most `bound` bytes a call.
async fn bridge_with_bound(bound: u32) -> Arc<ActiveBridge> {
    let bridge = ActiveBridge::test_new_for_mqtt_publish_acl(&["home"], &[]).await;
    register_test_client(bridge.mqtt_service(), "home", bound);
    bridge
}

/// Commit `input` (with `to` on `home` and a lifetime of 60 s unless given)
/// and return the row.
async fn commit(bridge: &ActiveBridge, mut input: serde_json::Value) -> MessageEnvelope {
    let fields = input.as_object_mut().unwrap();
    fields
        .entry("to")
        .or_insert_with(|| json!("mqtt:home:cmd/light"));
    fields.entry("lifetime_s").or_insert_with(|| json!(60));
    match call(bridge, input.clone()).await {
        Some(ToolOutcome::Ok(v)) => assert_eq!(v["ok"], json!(true), "{v}"),
        other => panic!("expected {input} to commit, got {other:?}"),
    }
    let rows = committed(bridge, "home").await;
    rows.last().expect("a committed row").clone()
}

#[tokio::test]
async fn pre_tool_use_send_auto_approves() {
    let bridge = ActiveBridge::test_new_for_pwa_push().await;
    let req = pre_tool_use_req(MCP_MQTT_SEND_TOOL);
    match try_handle_mqtt_tool(&bridge, &req).await {
        Some(MqttHandled::Respond(CcApprovalDecision::Allow {
            updated_input: None,
        })) => {}
        other => panic!("expected Allow, got {other:?}"),
    }
}

#[tokio::test]
async fn pre_tool_use_unrelated_returns_none() {
    let bridge = ActiveBridge::test_new_for_pwa_push().await;
    let req = pre_tool_use_req("mcp__brenn__SomethingElse");
    assert!(try_handle_mqtt_tool(&bridge, &req).await.is_none());
}

// ── Arguments ───────────────────────────────────────────────────────────

#[tokio::test]
async fn a_missing_to_is_rejected() {
    let bridge = ActiveBridge::test_new_for_pwa_push().await;
    let v = through_hook(&bridge, json!({ "body": "hello", "lifetime_s": 60 })).await;
    assert_eq!(v["ok"], json!(false));
    assert_eq!(v["error"], json!("missing or empty `to` argument"));
}

#[tokio::test]
async fn another_schemes_address_names_its_tool() {
    let bridge = ActiveBridge::test_new_for_pwa_push().await;
    for (to, tool) in [
        ("brenn:channel", "BrennSend"),
        ("pwa_push:alice", "PwaPushSend"),
    ] {
        let v = through_hook(&bridge, json!({ "to": to, "body": "hi", "lifetime_s": 60 })).await;
        assert_eq!(v["ok"], json!(false));
        let err = v["error"].as_str().unwrap();
        assert!(err.contains(tool), "error: {err}");
    }
}

#[tokio::test]
async fn a_wildcard_topic_is_rejected_naming_the_wildcard() {
    let bridge = ActiveBridge::test_new_with_mqtt_publish_acl().await;
    let v = through_hook(
        &bridge,
        json!({ "to": "mqtt:ha:home/+/state", "body": "hi", "lifetime_s": 60 }),
    )
    .await;
    assert_eq!(v["ok"], json!(false));
    let err = v["error"].as_str().unwrap();
    assert!(
        err.contains("wildcard") || err.contains('+') || err.contains('#'),
        "wildcard error must mention wildcard characters: {err}"
    );
}

#[tokio::test]
async fn an_invalid_qos_is_rejected() {
    let bridge = bridge_with_bound(1024).await;
    // 258 is 2 when truncated to a byte.
    for qos in [json!(3), json!(258), json!(-1), json!("1")] {
        assert_rejected(
            &bridge,
            json!({ "to": "mqtt:home:t", "body": "42", "qos": qos, "lifetime_s": 60 }),
            "invalid `qos`: must be 0, 1, or 2",
        )
        .await;
    }
}

#[tokio::test]
async fn neither_payload_form_is_rejected_naming_them() {
    let bridge = bridge_with_bound(1024).await;
    assert_rejected(
        &bridge,
        json!({ "to": "mqtt:home:t", "lifetime_s": 60 }),
        "missing payload: give exactly one of `body`, `body_base64` and `attachment`",
    )
    .await;
}

#[tokio::test]
async fn a_missing_or_malformed_lifetime_is_rejected_naming_both_forms() {
    let bridge = bridge_with_bound(1024).await;
    assert_rejected(
        &bridge,
        json!({ "to": "mqtt:home:t", "body": "on" }),
        "missing `lifetime_s` argument: give a whole number of seconds from 1 to 4294967295, \
         or the string \"forever\"",
    )
    .await;
    for value in [
        json!(0),
        json!(-5),
        json!(1.5),
        json!(4_294_967_296_u64),
        json!("never"),
        json!("60"),
        json!(true),
        json!(null),
    ] {
        assert_rejected(
            &bridge,
            json!({ "to": "mqtt:home:t", "body": "on", "lifetime_s": value }),
            "invalid `lifetime_s` argument: give a whole number of seconds from 1 to \
             4294967295, or the string \"forever\"",
        )
        .await;
    }
}

#[tokio::test]
async fn a_lifetime_in_seconds_expires_that_long_after_the_commit() {
    let bridge = bridge_with_bound(1024).await;
    let row = commit(&bridge, json!({ "body": "on", "lifetime_s": 90 })).await;
    assert_eq!(
        transport(&row).expires,
        MqttExpiry::At(row.publish_ts + chrono::Duration::seconds(90))
    );
    let row = commit(
        &bridge,
        json!({ "body": "on", "lifetime_s": 4_294_967_295_u64 }),
    )
    .await;
    assert_eq!(
        transport(&row).expires,
        MqttExpiry::At(row.publish_ts + chrono::Duration::seconds(4_294_967_295))
    );
}

#[tokio::test]
async fn a_forever_lifetime_never_expires() {
    let bridge = bridge_with_bound(1024).await;
    let row = commit(&bridge, json!({ "body": "on", "lifetime_s": "forever" })).await;
    assert_eq!(transport(&row).expires, MqttExpiry::Never);
}

// ── What a call commits ─────────────────────────────────────────────────

#[tokio::test]
async fn a_committed_call_answers_its_message_id_and_spends_one_budget_unit() {
    let bridge = bridge_with_bound(1024).await;
    let answer = call(
        &bridge,
        json!({
            "to": "mqtt:home:cmd/light",
            "body": "café",
            "qos": 2,
            "retain": true,
            "lifetime_s": 60,
        }),
    )
    .await;
    let row = the_row(&bridge, "home").await;
    let budget = bridge.app_config_default_send_budget() - 1;
    assert_eq!(
        answer,
        Some(ToolOutcome::Ok(json!({
            "ok": true,
            "message_id": row.message_id.to_string(),
            "remaining_budget": budget,
            "client_connected": false,
            "note": "MQTT client `home` is disconnected; the message is committed and waits \
                     for a connection within its lifetime",
        })))
    );
    assert_eq!(send_budget_row(&bridge).await, Some(budget));
    assert_eq!(row.channel, "brenn:egress/mqtt/home");
    assert_eq!(row.body.as_bytes(), "café".as_bytes());
    assert!(row.attachments.is_empty());
    let t = transport(&row);
    assert_eq!(
        (t.client_slug.as_str(), t.topic.as_str(), t.qos, t.retain),
        ("home", "cmd/light", 2, true)
    );
    assert_eq!(t.content_type, None);
}

#[tokio::test]
async fn a_call_on_a_connected_client_says_so_with_no_note() {
    let bridge = bridge_with_bound(1024).await;
    let handle = bridge.mqtt_service().get_client("home").unwrap();
    *handle.supervisor_state.write().await = brenn_mqtt::state::SupervisorState::Connected;
    let Some(ToolOutcome::Ok(v)) = call(
        &bridge,
        json!({ "to": "mqtt:home:t", "body": "on", "lifetime_s": 60 }),
    )
    .await
    else {
        panic!("expected a commit");
    };
    assert_eq!(v["client_connected"], json!(true), "{v}");
    assert!(v.get("note").is_none(), "{v}");
}

#[tokio::test]
async fn a_failed_client_is_named_in_the_note() {
    let bridge = bridge_with_bound(1024).await;
    let handle = bridge.mqtt_service().get_client("home").unwrap();
    *handle.supervisor_state.write().await = brenn_mqtt::state::SupervisorState::Failed {
        reason: "bad credentials".to_string(),
    };
    let Some(ToolOutcome::Ok(v)) = call(
        &bridge,
        json!({ "to": "mqtt:home:t", "body": "on", "lifetime_s": 60 }),
    )
    .await
    else {
        panic!("expected a commit");
    };
    assert_eq!(v["client_connected"], json!(false), "{v}");
    assert!(
        v["note"]
            .as_str()
            .unwrap()
            .starts_with("MQTT client `home` is failed;"),
        "{v}"
    );
}

#[tokio::test]
async fn a_text_body_holding_json_commits_those_exact_bytes_untyped() {
    let bridge = bridge_with_bound(1024).await;
    let text = r#"{ "action":  "turn_on", "level": 1.50 }"#;
    let row = commit(&bridge, json!({ "body": text })).await;
    assert_eq!(row.body.as_bytes(), text.as_bytes());
    assert_eq!(transport(&row).content_type, None);
}

#[tokio::test]
async fn an_object_or_array_body_commits_json_text_of_the_same_value() {
    let bridge = bridge_with_bound(1024).await;
    for value in [
        json!({ "action": "turn_on" }),
        json!([1, "two", { "three": 3 }]),
    ] {
        let row = commit(&bridge, json!({ "body": value.clone() })).await;
        let parsed: serde_json::Value = serde_json::from_slice(row.body.as_bytes()).unwrap();
        assert_eq!(parsed, value);
        assert_eq!(transport(&row).content_type, None);
    }
}

#[tokio::test]
async fn a_body_takes_a_content_type() {
    let bridge = bridge_with_bound(1024).await;
    let row = commit(
        &bridge,
        json!({ "body": { "on": true }, "content_type": "application/json" }),
    )
    .await;
    assert_eq!(row.body.as_bytes(), br#"{"on":true}"#);
    assert_eq!(
        transport(&row).content_type.as_deref(),
        Some("application/json")
    );
}

#[tokio::test]
async fn body_base64_commits_the_decoded_bytes_with_and_without_a_content_type() {
    let bridge = bridge_with_bound(1024).await;
    let row = commit(
        &bridge,
        json!({ "body_base64": "AAEC/w==", "content_type": "image/png" }),
    )
    .await;
    assert_eq!(row.body.as_bytes(), [0x00, 0x01, 0x02, 0xff]);
    assert_eq!(transport(&row).content_type.as_deref(), Some("image/png"));

    let row = commit(&bridge, json!({ "body_base64": "AAEC/w==" })).await;
    assert_eq!(row.body.as_bytes(), [0x00, 0x01, 0x02, 0xff]);
    assert_eq!(transport(&row).content_type, None);
}

#[tokio::test]
async fn json_text_with_a_binary_base64_key_commits_exactly_through_body_base64() {
    use base64ct::{Base64, Encoding as _};
    let bridge = bridge_with_bound(1024).await;
    let text = r#"{"binary_base64": "AAEC", "n": 1.50}"#;
    let row = commit(
        &bridge,
        json!({ "body_base64": Base64::encode_string(text.as_bytes()) }),
    )
    .await;
    assert_eq!(row.body.as_bytes(), text.as_bytes());
}

#[tokio::test]
async fn bad_base64_is_rejected() {
    let bridge = bridge_with_bound(1024).await;
    let input = json!({ "to": "mqtt:home:t", "body_base64": "not base64!!", "lifetime_s": 60 });
    let Some(ToolOutcome::Err(v)) = call(&bridge, input).await else {
        panic!("expected a rejection");
    };
    assert!(
        v["error"]
            .as_str()
            .unwrap()
            .starts_with("invalid `body_base64` argument: "),
        "{v}"
    );
    assert_eq!(send_budget_row(&bridge).await, None);
}

#[tokio::test]
async fn a_content_type_the_bus_refuses_is_rejected() {
    let bridge = bridge_with_bound(1024).await;
    let input = json!({
        "to": "mqtt:home:t",
        "body": "on",
        "content_type": "not a media type",
        "lifetime_s": 60,
    });
    let Some(ToolOutcome::Err(v)) = call(&bridge, input).await else {
        panic!("expected a rejection");
    };
    assert!(
        v["error"]
            .as_str()
            .unwrap()
            .starts_with("invalid `content_type` argument: invalid content type"),
        "{v}"
    );
    assert_eq!(send_budget_row(&bridge).await, None);
}

#[tokio::test]
async fn the_retired_binary_object_is_rejected_with_the_pointer_text() {
    let bridge = bridge_with_bound(1024).await;
    for body in [
        json!({ "binary_base64": "AAEC/w==", "content_type": "image/png" }),
        json!({ "binary_base64": "AAEC", "action": "turn_on" }),
    ] {
        assert_rejected(
            &bridge,
            json!({ "to": "mqtt:home:cam/snapshot", "body": body, "lifetime_s": 60 }),
            "`body` is an object with a `binary_base64` key: bytes are given in the \
             top-level `body_base64` argument, with `content_type` beside it. An object \
             `body` is published as JSON text; a JSON payload that itself has a top-level \
             `binary_base64` key is given as its text in `body_base64`",
        )
        .await;
    }
}

// ── The client's outbound bound and the body cap ────────────────────────

fn too_large(len: usize, bound: u32) -> String {
    format!(
        "payload is {len} bytes; MQTT client `home` publishes at most {bound} \
         (`outbound_payload_cap_bytes`)"
    )
}

#[tokio::test]
async fn a_payload_exactly_at_the_bound_commits() {
    let bridge = bridge_with_bound(4).await;
    let row = commit(&bridge, json!({ "body": "four" })).await;
    assert_eq!(row.body.as_bytes(), b"four");
}

#[tokio::test]
async fn a_payload_over_the_bound_is_refused_on_its_published_length() {
    // Eight base64 characters are five decoded bytes; `{"a":1}` is seven.
    for (bound, input, len) in [
        (4, json!({ "body": "fives" }), 5),
        (4, json!({ "body_base64": "AAECAwQ=" }), 5),
        (6, json!({ "body": { "a": 1 } }), 7),
    ] {
        let bridge = bridge_with_bound(bound).await;
        let mut input = input;
        input["to"] = json!("mqtt:home:cmd/light");
        input["lifetime_s"] = json!(60);
        let text = refused(&bridge, call(&bridge, input).await, "payload_too_large").await;
        assert_eq!(text, too_large(len, bound));
    }
}

#[tokio::test]
async fn an_acl_denial_answers_before_the_bound() {
    let bridge = bridge_with_bound(4).await;
    register_test_client(bridge.mqtt_service(), "office", 4);
    assert_eq!(
        call(
            &bridge,
            json!({ "to": "mqtt:office:cmd/light", "body": "far too long", "lifetime_s": 60 }),
        )
        .await,
        Some(ToolOutcome::Err(json!({
            "ok": false,
            "error": "MQTT publish to `mqtt:office:cmd/light` is not allowed for this app: \
                      no `acl publish` entry admits that topic on client `office`; the \
                      operator adds `topic_filter \"mqtt:office:<filter>\"` to the app's \
                      `acl publish`",
        })))
    );
    assert_eq!(send_budget_row(&bridge).await, None);
}

#[tokio::test]
async fn a_payload_over_the_body_cap_with_no_blob_store_is_unstorable() {
    let bridge = bridge_with_bound(4 * 1024 * 1024).await;
    let max_body = bridge.messenger().unwrap().max_body_bytes();
    let text = refused(
        &bridge,
        call(
            &bridge,
            json!({ "to": "mqtt:home:t", "body": "x".repeat(max_body + 1), "lifetime_s": 60 }),
        )
        .await,
        "payload_unstorable",
    )
    .await;
    assert_eq!(
        text,
        format!(
            "payload is {} bytes, over the bus's {max_body}-byte message body cap, and this \
             server has no blob store to carry it",
            max_body + 1
        )
    );
}

// ── The bus's other answers ─────────────────────────────────────────────

#[tokio::test]
#[tracing_test::traced_test]
async fn a_client_with_no_session_is_not_configured_and_not_a_security_event() {
    // The ACL names `ha`, and the registry holds no session for it: what an
    // in-flight call sees while a reload removes the client.
    let bridge = ActiveBridge::test_new_with_mqtt_publish_acl().await;
    let v = through_hook(
        &bridge,
        json!({ "to": "mqtt:ha:home/sensor/temp", "body": "42", "lifetime_s": 60 }),
    )
    .await;
    assert_eq!(
        v,
        json!({ "ok": false, "error": "MQTT client `ha` is not configured on this server" })
    );
    assert!(logs_contain(
        "MqttSend: the MQTT client is not configured on this server"
    ));
    assert!(!logs_contain("app security event"));
    assert_eq!(send_budget_row(&bridge).await, None);
}

#[tokio::test]
async fn an_exhausted_budget_commits_nothing() {
    let bridge = bridge_with_bound(1024).await;
    {
        let conn = bridge.db().lock().await;
        for _ in 0..bridge.app_config_default_send_budget() {
            brenn_messaging_store::db::decrement_send_budget(
                &conn,
                bridge.conversation_id,
                bridge.app_config_default_send_budget(),
            );
        }
    }
    assert_eq!(
        call(
            &bridge,
            json!({ "to": "mqtt:home:t", "body": "on", "lifetime_s": 60 })
        )
        .await,
        Some(ToolOutcome::Err(json!({
            "ok": false,
            "error": "budget exhausted: 0 remaining; ask the user to send a chat message to reset",
        })))
    );
    assert!(committed(&bridge, "home").await.is_empty());
}

#[tokio::test]
async fn a_publish_over_the_send_rate_is_rate_limited() {
    let global = brenn_lib::messaging::MessagingGlobalConfig {
        default_send_rate: brenn_lib::messaging::config::SendRate {
            burst: 1,
            refill_interval_secs: 3600,
            refill: 1,
        },
        ..Default::default()
    };
    let bridge =
        ActiveBridge::test_new_for_mqtt_publish_with_global(&[("home", "#")], &["home"], global)
            .await;
    let input = json!({ "to": "mqtt:home:t", "body": "on", "lifetime_s": 60 });
    assert!(matches!(
        call(&bridge, input.clone()).await,
        Some(ToolOutcome::Ok(_))
    ));
    assert_eq!(
        call(&bridge, input).await,
        Some(ToolOutcome::Err(json!({
            "ok": false,
            "error": "rate limited: too many publishes to this MQTT client; slow down",
        })))
    );
    assert_eq!(committed(&bridge, "home").await.len(), 1);
}

// ── Grant and ACL denials ───────────────────────────────────────────────

#[tokio::test]
#[tracing_test::traced_test]
async fn an_absent_grant_says_mqtt_publish_is_not_enabled() {
    let bridge = ActiveBridge::test_new_for_mqtt_no_grant().await;
    let v = through_hook(
        &bridge,
        json!({ "to": "mqtt:ha:home/sensor/temp", "body": "42", "lifetime_s": 60 }),
    )
    .await;
    assert_eq!(
        v,
        json!({
            "ok": false,
            "error": "MQTT publish is not enabled for this app: the operator adds an \
                      `acl publish` entry `topic_filter \"mqtt:<client>:<filter>\"`, \
                      with `publish` in the app's `grants`",
        })
    );
    assert!(
        logs_contain("app security event") && logs_contain("kind=grant_absent address=mqtt:ha"),
        "a grant-absent MQTT denial is a security event"
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn an_unlisted_client_is_denied_as_a_security_event() {
    // The ACL allows `home` only; `office` is denied whether or not it has a
    // session.
    for sessions in [&["home"][..], &["home", "office"][..]] {
        let bridge = ActiveBridge::test_new_for_mqtt_publish_acl(&["home"], sessions).await;
        let v = through_hook(
            &bridge,
            json!({ "to": "mqtt:office:cmd/light", "body": "on", "lifetime_s": 60 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false));
        let err = v["error"].as_str().unwrap();
        assert!(
            err.contains("acl publish") && err.contains("office"),
            "{err}"
        );
    }
    assert!(logs_contain("mqtt_publish_denied"));
    assert!(logs_contain(
        "kind=acl_denied address=mqtt:office:cmd/light"
    ));
}

/// The bridge, with `home` held for publishing to `a/+/set` only and a
/// session for `home`.
async fn bridge_publishing_to_a_set() -> Arc<ActiveBridge> {
    ActiveBridge::test_new_for_mqtt_publish_filters(&[("home", "a/+/set")], &["home"]).await
}

#[tokio::test]
async fn a_topic_the_filter_admits_commits() {
    let bridge = bridge_publishing_to_a_set().await;
    let row = commit(&bridge, json!({ "to": "mqtt:home:a/x/set", "body": "on" })).await;
    assert_eq!(transport(&row).topic, "a/x/set");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn a_topic_outside_the_filter_is_refused_with_the_topic_named() {
    let bridge = bridge_publishing_to_a_set().await;
    assert_eq!(
        call(
            &bridge,
            json!({ "to": "mqtt:home:a/x/get", "body": "on", "lifetime_s": 60 }),
        )
        .await,
        Some(ToolOutcome::Err(json!({
            "ok": false,
            "error": "MQTT publish to `mqtt:home:a/x/get` is not allowed for this app: \
                      no `acl publish` entry admits that topic on client `home`; the \
                      operator adds `topic_filter \"mqtt:home:<filter>\"` to the app's \
                      `acl publish`",
        })))
    );
    assert!(logs_contain("kind=acl_denied address=mqtt:home:a/x/get"));
    assert_eq!(send_budget_row(&bridge).await, None);
}

#[tokio::test]
#[tracing_test::traced_test]
async fn a_control_character_in_a_refused_topic_is_escaped() {
    let bridge = bridge_publishing_to_a_set().await;
    let answer = call(
        &bridge,
        json!({ "to": "mqtt:home:a/x\u{1b}[2J/get", "body": "on", "lifetime_s": 60 }),
    )
    .await;
    let Some(ToolOutcome::Err(v)) = answer else {
        panic!("expected a refusal, got {answer:?}");
    };
    let error = v["error"].as_str().expect("the refusal carries text");
    assert!(
        error.starts_with("MQTT publish to `mqtt:home:a/x\\u{1b}[2J/get` is not allowed"),
        "the echoed topic must be escaped: {error}"
    );
    assert!(!error.contains('\u{1b}'));
    assert!(logs_contain("kind=acl_denied address=mqtt:home:a/x"));
    assert!(!logs_contain("\u{1b}[2J"));
    assert_eq!(send_budget_row(&bridge).await, None);
}

#[tokio::test]
async fn execution_alone_emits_no_card() {
    let bridge = bridge_with_bound(1024).await;
    let mut rx = bridge.subscribe();

    assert_eq!(
        call(&bridge, json!({ "body": "on" })).await,
        Some(ToolOutcome::Err(json!({
            "ok": false,
            "error": "missing or empty `to` argument",
        })))
    );
    assert!(matches!(
        call(
            &bridge,
            json!({ "to": "mqtt:home:cmd/light", "body": "on", "lifetime_s": 60 })
        )
        .await,
        Some(ToolOutcome::Ok(_))
    ));
    assert_eq!(
        execute_mqtt_tool(&bridge, "mcp__brenn__BrennSend", &json!({})).await,
        None
    );

    while let Ok(msg) = rx.try_recv() {
        assert!(
            !matches!(msg, brenn_ws_types::WsServerMessage::ToolUseSummary { .. }),
            "execution alone must emit no chat card: {msg:?}"
        );
    }
}

// ── status_to ───────────────────────────────────────────────────────────

/// A bridge whose app may publish anywhere on `home` (with a session for it),
/// beside the declared `brenn:light-status` and `brenn:other-status`.
/// `publish` is the app's `brenn:` publish authority (see
/// [`BrennStatusChannels::publish`]); `global` the bus's configuration.
async fn bridge_with_status(
    publish: Option<&[&str]>,
    global: brenn_lib::messaging::MessagingGlobalConfig,
) -> Arc<ActiveBridge> {
    ActiveBridge::test_new_for_mqtt_publish_with_status(
        &[("home", "#")],
        &["home"],
        global,
        &BrennStatusChannels {
            declared: &["light-status", "other-status"],
            publish,
        },
    )
    .await
}

/// [`bridge_with_status`] where the app may publish to `light-status` and to
/// the undeclared `gone-status`.
async fn status_bridge() -> Arc<ActiveBridge> {
    bridge_with_status(
        Some(&["light-status", "gone-status"]),
        brenn_lib::messaging::MessagingGlobalConfig::default(),
    )
    .await
}

fn naming(status_to: serde_json::Value) -> serde_json::Value {
    json!({
        "to": "mqtt:home:cmd/light",
        "body": "on",
        "lifetime_s": 60,
        "status_to": status_to,
    })
}

/// Assert `input` is refused with exactly `error`, committing nothing and
/// spending no budget.
async fn assert_refused_with(bridge: &ActiveBridge, input: serde_json::Value, error: &str) {
    assert_eq!(
        call(bridge, input.clone()).await,
        Some(ToolOutcome::Err(json!({ "ok": false, "error": error }))),
        "{input}"
    );
    assert!(committed(bridge, "home").await.is_empty());
    assert_eq!(
        send_budget_row(bridge).await,
        None,
        "a refusal spends no budget"
    );
}

#[tokio::test]
async fn a_status_to_rides_on_the_committed_row() {
    let bridge = status_bridge().await;
    let row = commit(
        &bridge,
        json!({ "body": "on", "status_to": "brenn:light-status" }),
    )
    .await;
    assert_eq!(
        transport(&row).status_to.as_deref(),
        Some("brenn:light-status")
    );
    let row = commit(&bridge, json!({ "body": "off" })).await;
    assert_eq!(transport(&row).status_to, None);
}

#[tokio::test]
async fn a_status_to_that_is_not_a_nonempty_string_is_an_argument_error() {
    let bridge = status_bridge().await;
    for value in [
        json!(""),
        json!(7),
        json!(null),
        json!(["brenn:light-status"]),
    ] {
        assert_rejected(
            &bridge,
            naming(value),
            "invalid `status_to` argument: give a `brenn:<channel>` address as a string",
        )
        .await;
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn a_malformed_status_to_names_the_brenn_form_and_is_no_security_event() {
    let bridge = status_bridge().await;
    for address in ["light-status", "brenn:bad name", "ephemeral:light-status"] {
        assert_refused_with(
            &bridge,
            naming(json!(address)),
            &format!(
                "malformed `status_to` {address:?}: must be of the form \"brenn:<name>\" with \
                 name matching ^[A-Za-z0-9._~-]+$"
            ),
        )
        .await;
    }
    assert!(!logs_contain("brenn_publish_denied"));
}

#[tokio::test]
#[tracing_test::traced_test]
async fn a_status_to_without_the_publish_grant_is_refused_as_a_security_event() {
    let bridge =
        bridge_with_status(None, brenn_lib::messaging::MessagingGlobalConfig::default()).await;
    let v = through_hook(&bridge, naming(json!("brenn:light-status"))).await;
    assert_eq!(
        v,
        json!({
            "ok": false,
            "error": "`status_to` is refused: this app has no `publish` grant for `brenn:` channels",
        })
    );
    assert!(committed(&bridge, "home").await.is_empty());
    assert_eq!(send_budget_row(&bridge).await, None);
    assert!(logs_contain("brenn_publish_denied"));
    assert!(logs_contain("kind=grant_absent address=brenn:light-status"));
}

/// Outside the ACL and absent from the directory answer with one string, the
/// one a `BrennSend` denial gives; only the security log tells them apart.
#[tokio::test]
#[tracing_test::traced_test]
async fn an_out_of_acl_or_missing_status_to_answers_alike_and_signals_its_kind() {
    let bridge = status_bridge().await;
    for (address, kind) in [
        ("brenn:other-status", "acl_denied"),
        ("brenn:nowhere", "acl_denied"),
        ("brenn:gone-status", "unknown_channel"),
    ] {
        let v = through_hook(&bridge, naming(json!(address))).await;
        assert_eq!(
            v,
            json!({
                "ok": false,
                "error": format!("`status_to`: {}", brenn_channel_denied_msg(address)),
            }),
            "{address}"
        );
        assert!(
            logs_contain(&format!("kind={kind} address={address}")),
            "{address}"
        );
    }
    assert!(logs_contain("brenn_publish_denied"));
    assert!(committed(&bridge, "home").await.is_empty());
    assert_eq!(send_budget_row(&bridge).await, None);
}

#[tokio::test]
#[tracing_test::traced_test]
async fn a_topic_whose_status_could_not_fit_the_body_cap_is_refused() {
    let global = brenn_lib::messaging::MessagingGlobalConfig {
        max_body_bytes: 256,
        ..Default::default()
    };
    let bridge = bridge_with_status(Some(&["light-status"]), global).await;
    let len = brenn_envelope::delivery::max_encoded_len("home", "cmd/light");
    assert!(len > 256, "the fixture's cap is below the bound: {len}");
    assert_refused_with(
        &bridge,
        naming(json!("brenn:light-status")),
        &format!(
            "the topic is too long for a delivery status about it to be published (a status can \
             be {len} bytes, over the bus's 256-byte body cap): drop `status_to` or shorten the \
             topic"
        ),
    )
    .await;
    assert!(!logs_contain("brenn_publish_denied"));
}

/// A refusal of the target itself answers before any `status_to` refusal.
#[tokio::test]
async fn a_target_refusal_answers_before_the_status_to() {
    let bridge =
        bridge_with_status(None, brenn_lib::messaging::MessagingGlobalConfig::default()).await;
    let v = call(
        &bridge,
        json!({
            "to": "mqtt:office:cmd/light",
            "body": "on",
            "lifetime_s": 60,
            "status_to": "brenn:nowhere",
        }),
    )
    .await;
    let Some(ToolOutcome::Err(v)) = v else {
        panic!("expected a refusal, got {v:?}");
    };
    let error = v["error"].as_str().unwrap();
    assert!(
        error.starts_with("MQTT publish to `mqtt:office:cmd/light`"),
        "{error}"
    );
}

//! The `mqtt_publish` action through the automation tools: its arguments,
//! what a fire of it commits, and the admission's refusals at create.

use std::num::NonZeroU32;
use std::sync::Arc;

use brenn_lib::messaging::config::Depth;
use brenn_lib::messaging::{egress_channel_uuid, mqtt_egress_address};
use brenn_lib::mqtt::MqttLifetime;
use serde_json::json;

use super::*;
use crate::active_bridge::test_fixtures::BrennStatusChannels;

/// `parse_action`'s refusal of `action`.
fn action_err(action: serde_json::Value) -> String {
    parse_action(&json!({ "action": action })).expect_err("refused")
}

/// An `mqtt_publish` action on `mqtt:home:cmd/light` with `fields` laid over
/// it; a `null` field removes the key.
fn mqtt(fields: serde_json::Value) -> serde_json::Value {
    let mut action = json!({
        "kind": "mqtt_publish",
        "to": "mqtt:home:cmd/light",
        "body": "on",
        "lifetime_s": 60,
    });
    for (k, v) in fields.as_object().unwrap() {
        if v.is_null() {
            action.as_object_mut().unwrap().remove(k);
        } else {
            action[k] = v.clone();
        }
    }
    action
}

/// A `send_message` action on `brenn:test-channel` with `fields` laid over it.
fn send(fields: serde_json::Value) -> serde_json::Value {
    let mut action = json!({ "kind": "send_message", "to": "brenn:test-channel", "body": "hi" });
    for (k, v) in fields.as_object().unwrap() {
        if v.is_null() {
            action.as_object_mut().unwrap().remove(k);
        } else {
            action[k] = v.clone();
        }
    }
    action
}

#[test]
fn an_mqtt_publish_action_parses_every_field() {
    let action = parse_action(&json!({ "action": mqtt(json!({
        "body": { "state": "ON" },
        "content_type": "application/json",
        "qos": 2,
        "retain": true,
        "lifetime_s": "forever",
        "status_to": "brenn:light-status",
    })) }))
    .unwrap();
    assert_eq!(
        action,
        Action::MqttPublish(MqttPublishAction {
            to: "mqtt:home:cmd/light".to_string(),
            payload: MqttActionPayload::from_bytes(br#"{"state":"ON"}"#.to_vec()),
            qos: 2,
            retain: true,
            lifetime: MqttLifetime::Forever,
            content_type: Some("application/json".to_string()),
            status_to: Some("brenn:light-status".to_string()),
        })
    );
    let action =
        parse_action(&json!({ "action": mqtt(json!({ "body": null, "body_base64": "/wA=" })) }))
            .unwrap();
    assert_eq!(
        action,
        Action::MqttPublish(MqttPublishAction {
            to: "mqtt:home:cmd/light".to_string(),
            payload: MqttActionPayload::from_bytes(vec![0xff, 0x00]),
            qos: 1,
            retain: false,
            lifetime: MqttLifetime::Seconds(NonZeroU32::new(60).unwrap()),
            content_type: None,
            status_to: None,
        })
    );
}

#[test]
fn an_mqtt_publish_action_refuses_what_does_not_apply() {
    assert_eq!(
        action_err(mqtt(json!({ "lifetime_s": null }))),
        "action: missing `lifetime_s` argument: give a whole number of seconds from 1 to \
         4294967295, or the string \"forever\""
    );
    assert_eq!(
        action_err(mqtt(json!({
            "body": null,
            "attachment": { "message_id": "m-1", "name": "a.bin" },
        }))),
        "action.attachment is not available in an automation: a job holds its payload \
         itself; give body or body_base64"
    );
    for key in ["urgency", "reply_to", "delivery_deadline_secs"] {
        let err = action_err(mqtt(json!({ key: "x" })));
        assert!(
            err.starts_with(&format!(
                "action.{key} does not apply to an mqtt_publish action"
            )),
            "{key}: {err}"
        );
    }
    assert_eq!(
        action_err(mqtt(json!({ "to": null }))),
        "action: missing or empty `to` argument"
    );
    assert_eq!(
        action_err(mqtt(json!({ "colour": "red" }))),
        "action.colour is not a field of a mqtt_publish action; remove it"
    );
    assert_eq!(
        action_err(mqtt(json!({ "to": "brenn:test-channel" }))),
        "action.to is a brenn: address: use action kind \"send_message\""
    );
    assert_eq!(
        action_err(mqtt(json!({ "retain": "yes" }))),
        "action: invalid `retain`: must be true or false"
    );
    assert_eq!(
        action_err(json!("mqtt_publish")),
        "action must be an object"
    );
}

#[test]
fn a_send_message_action_refuses_keys_it_does_not_define() {
    for key in ["qos", "lifetime_s", "colour"] {
        assert_eq!(
            action_err(send(json!({ key: 1 }))),
            format!("action.{key} is not a field of a send_message action; remove it")
        );
    }
    assert_eq!(
        action_err(send(json!({ "body": { "state": "ON" } }))),
        "action.body must be a string for send_message"
    );
    assert_eq!(
        action_err(send(json!({ "body": null }))),
        "action.body is required"
    );
    assert_eq!(
        action_err(send(json!({ "to": "mqtt:home:cmd/light" }))),
        "action.to is an mqtt: address: use action kind \"mqtt_publish\""
    );
    assert!(
        action_err(send(json!({ "wake": "none", "colour": 1 }))).contains("use action.urgency"),
        "the wake refusal comes before the unknown-key one"
    );
}

// ── Through the tools ───────────────────────────────────────────────────

/// A bridge whose app may publish anywhere on `home`, with a session for
/// `home`, and an automation engine.
async fn mqtt_bridge() -> Arc<ActiveBridge> {
    ActiveBridge::test_new_for_mqtt_automation(
        &[("home", "#")],
        &["home"],
        &BrennStatusChannels::default(),
    )
    .await
}

/// The JSON answer to one call of `tool`, refusal or not.
async fn answer(bridge: &ActiveBridge, tool: &str, input: serde_json::Value) -> serde_json::Value {
    match execute_automation_tool(bridge, tool, &input).await {
        Some(ToolOutcome::Ok(v) | ToolOutcome::Err(v)) => v,
        other => panic!("{tool}: unexpected {other:?}"),
    }
}

/// Create a daily job firing `action` and return its id.
async fn create(bridge: &ActiveBridge, action: serde_json::Value) -> String {
    let v = answer(bridge, MCP_AUTO_CREATE_TOOL, create_input(action)).await;
    assert_eq!(v["ok"], json!(true), "{v}");
    v["id"].as_str().unwrap().to_string()
}

fn create_input(action: serde_json::Value) -> serde_json::Value {
    json!({
        "name": "kitchen light",
        "trigger": { "kind": "cron", "expr": "0 18 * * *", "tz": "UTC" },
        "action": action,
    })
}

/// The one job's listed action.
async fn listed_action(bridge: &ActiveBridge) -> serde_json::Value {
    let v = answer(bridge, MCP_AUTO_LIST_TOOL, json!({})).await;
    let jobs = v["jobs"].as_array().expect("jobs");
    assert_eq!(jobs.len(), 1, "{v}");
    jobs[0]["action"].clone()
}

/// Make every job due and fire it.
async fn fire_all(bridge: &ActiveBridge) {
    let engine = bridge.automation_engine().expect("an automation bridge");
    {
        let conn = bridge.db().lock().await;
        conn.execute(
            "UPDATE automation_jobs SET next_fire_at = ?1",
            [brenn_db::format_ts_for_db(
                chrono::Utc::now() - chrono::Duration::minutes(1),
            )],
        )
        .unwrap();
    }
    let due = engine.get_due_jobs().await;
    assert_eq!(due.len(), 1);
    for job in due {
        brenn_automation::fire::fire_one(engine, job).await;
    }
}

/// The bodies committed to `client`'s egress channel, oldest first.
async fn committed_bodies(bridge: &ActiveBridge, client: &str) -> Vec<Vec<u8>> {
    let messenger = bridge.messenger().expect("an MQTT bridge has a messenger");
    let conn = messenger.db().lock().await;
    brenn_messaging_store::db::load_channel_retained_tail(
        &conn,
        egress_channel_uuid(&mqtt_egress_address(client)),
        Depth::Unbounded,
    )
    .into_iter()
    .map(|(_, envelope)| envelope.body.as_bytes().to_vec())
    .collect()
}

#[tokio::test]
async fn each_payload_form_is_listed_and_fired_as_its_bytes() {
    let object = json!({ "state": "ON", "brightness": 120 });
    let object_text = serde_json::to_string(&object).unwrap();
    let cases: [(serde_json::Value, serde_json::Value, Vec<u8>); 3] = [
        (
            json!({ "body": object }),
            json!({ "body": object_text }),
            object_text.clone().into_bytes(),
        ),
        (
            json!({ "body": object_text }),
            json!({ "body": object_text }),
            object_text.clone().into_bytes(),
        ),
        (
            json!({ "body": null, "body_base64": "/wA=" }),
            json!({ "body_base64": "/wA=" }),
            vec![0xff, 0x00],
        ),
    ];
    for (given, shown, bytes) in cases {
        let bridge = mqtt_bridge().await;
        create(&bridge, mqtt(given.clone())).await;
        let mut expected = json!({
            "kind": "mqtt_publish",
            "to": "mqtt:home:cmd/light",
            "qos": 1,
            "retain": false,
            "lifetime_s": 60,
        });
        for (k, v) in shown.as_object().unwrap() {
            expected[k] = v.clone();
        }
        assert_eq!(listed_action(&bridge).await, expected, "{given}");
        fire_all(&bridge).await;
        assert_eq!(
            committed_bodies(&bridge, "home").await,
            vec![bytes],
            "{given}"
        );
    }
}

/// Every payload form and every lifetime form the stored action can hold is
/// accepted back by `AutoEdit` as listed.
#[tokio::test]
async fn a_listed_action_given_back_to_edit_stores_the_same_action() {
    for given in [
        json!({
            "body": { "state": "ON" },
            "content_type": "application/json",
            "qos": 0,
            "retain": true,
            "lifetime_s": "forever",
        }),
        json!({ "body": "ON", "qos": 2, "lifetime_s": 1 }),
        json!({ "body": "", "lifetime_s": 4294967295_u64 }),
        json!({ "body": null, "body_base64": "/wA=", "lifetime_s": 300 }),
        json!({ "body": null, "body_base64": "T04=", "lifetime_s": 300 }),
    ] {
        let bridge = mqtt_bridge().await;
        let id = create(&bridge, mqtt(given.clone())).await;
        let listed = listed_action(&bridge).await;
        let v = answer(
            &bridge,
            MCP_AUTO_EDIT_TOOL,
            json!({ "id": id, "action": listed.clone() }),
        )
        .await;
        assert_eq!(v["ok"], json!(true), "{given}: {v}");
        assert_eq!(listed_action(&bridge).await, listed, "{given}");
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn a_topic_outside_the_acl_answers_as_mqtt_send_does_and_is_signalled() {
    let bridge = ActiveBridge::test_new_for_mqtt_automation(
        &[("home", "lights/#")],
        &["home"],
        &BrennStatusChannels::default(),
    )
    .await;
    let v = answer(
        &bridge,
        MCP_AUTO_CREATE_TOOL,
        create_input(mqtt(json!({ "to": "mqtt:home:locks/front" }))),
    )
    .await;
    assert_eq!(
        v,
        json!({
            "ok": false,
            "error": "MQTT publish to `mqtt:home:locks/front` is not allowed for this app: no \
                      `acl publish` entry admits that topic on client `home`; the operator \
                      adds `topic_filter \"mqtt:home:<filter>\"` to the app's `acl publish`",
        })
    );
    assert!(logs_contain("mqtt_publish_denied"));
    assert!(logs_contain(
        "kind=acl_denied address=mqtt:home:locks/front"
    ));
    let list = answer(&bridge, MCP_AUTO_LIST_TOOL, json!({})).await;
    assert_eq!(list["jobs"], json!([]));
}

#[tokio::test]
#[tracing_test::traced_test]
async fn an_unconfigured_client_answers_not_configured_and_is_not_signalled() {
    let bridge = ActiveBridge::test_new_for_mqtt_automation(
        &[("home", "#"), ("away", "#")],
        &["home"],
        &BrennStatusChannels::default(),
    )
    .await;
    let v = answer(
        &bridge,
        MCP_AUTO_CREATE_TOOL,
        create_input(mqtt(json!({ "to": "mqtt:away:cmd/light" }))),
    )
    .await;
    assert_eq!(
        v,
        json!({ "ok": false, "error": "MQTT client `away` is not configured on this server" })
    );
    assert!(logs_contain(
        "the MQTT client is not configured on this server"
    ));
    assert!(logs_contain(&format!("tool=\"{MCP_AUTO_CREATE_TOOL}\"")));
    assert!(!logs_contain("MqttSend"));
    assert!(!logs_contain("app security event"));
    let list = answer(&bridge, MCP_AUTO_LIST_TOOL, json!({})).await;
    assert_eq!(list["jobs"], json!([]));
}

#[tokio::test]
#[tracing_test::traced_test]
async fn an_admission_argument_refusal_is_logged_as_an_automation_tool_rejection() {
    let bridge = mqtt_bridge().await;
    let v = answer(
        &bridge,
        MCP_AUTO_CREATE_TOOL,
        create_input(mqtt(json!({ "status_to": "not-a-brenn-address" }))),
    )
    .await;
    assert_eq!(v["ok"], json!(false), "{v}");
    assert!(
        v["error"]
            .as_str()
            .unwrap()
            .contains("malformed `status_to`"),
        "{v}"
    );
    assert!(logs_contain("automation tool rejected"));
    assert!(!logs_contain("mqtt tool rejected"));
}

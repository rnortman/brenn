//! What a job write costs: `AutoCreate`, and an `AutoEdit` that sets a job's
//! action or trigger or enables it, draw one unit of the calling
//! conversation's send budget; renaming, disabling, listing and deleting are
//! free; a write the engine refuses gives its unit back.

use serde_json::json;

use super::*;
use crate::active_bridge::test_support::send_budget_row;

const EXHAUSTED: &str =
    "budget exhausted: 0 remaining; ask the user to send a chat message to reset";

/// Set the calling conversation's pool to `remaining`.
async fn set_pool(bridge: &ActiveBridge, remaining: u32) {
    assert!(remaining <= bridge.app_config_default_send_budget());
    let conn = bridge.db().lock().await;
    brenn_messaging_store::db::reset_send_budget(&conn, bridge.conversation_id, remaining);
}

async fn pool(bridge: &ActiveBridge) -> u32 {
    send_budget_row(bridge).await.expect("a budget row")
}

async fn call(bridge: &ActiveBridge, tool: &str, input: serde_json::Value) -> ToolOutcome {
    execute_automation_tool(bridge, tool, &input)
        .await
        .expect("an automation tool")
}

fn create_input(expr: &str) -> serde_json::Value {
    json!({
        "name": "job",
        "trigger": { "kind": "cron", "expr": expr, "tz": "UTC" },
        "action": { "kind": "send_message", "to": "brenn:test-channel", "body": "hello" }
    })
}

fn assert_exhausted(outcome: &ToolOutcome) {
    assert_eq!(
        outcome,
        &ToolOutcome::Err(json!({ "ok": false, "error": EXHAUSTED }))
    );
}

/// A job written straight through the engine, so it draws nothing.
async fn seed_job(bridge: &ActiveBridge) -> String {
    let engine = bridge.automation_engine().unwrap();
    match engine
        .create(
            "testapp",
            CreateJob {
                name: "seeded".to_string(),
                trigger: Trigger::Cron(CronTrigger {
                    expr: "0 9 * * *".to_string(),
                    tz: "UTC".to_string(),
                    persistent: false,
                }),
                action: Action::SendMessage(SendMessageAction {
                    to: "brenn:test-channel".to_string(),
                    body: "original".to_string(),
                    urgency: brenn_lib::messaging::Urgency::Low,
                    reply_to: None,
                    delivery_deadline_secs: None,
                }),
                enabled: true,
            },
        )
        .await
    {
        CreateResult::Ok { id, .. } => id,
        other => panic!("seed create: {other:?}"),
    }
}

async fn only_job(bridge: &ActiveBridge) -> JobView {
    let engine = bridge.automation_engine().unwrap();
    let ListResult::Ok(mut jobs) = engine.list("testapp", false).await;
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    jobs.remove(0)
}

fn assert_ok(outcome: &ToolOutcome) {
    assert!(matches!(outcome, ToolOutcome::Ok(_)), "{outcome:?}");
}

#[tokio::test]
async fn a_create_draws_one_unit() {
    let bridge = ActiveBridge::test_new_with_automation().await;
    set_pool(&bridge, 5).await;
    assert_ok(&call(&bridge, MCP_AUTO_CREATE_TOOL, create_input("0 9 * * *")).await);
    assert_eq!(pool(&bridge).await, 4);
}

#[tokio::test]
async fn a_create_at_an_empty_pool_is_refused() {
    let bridge = ActiveBridge::test_new_with_automation().await;
    set_pool(&bridge, 0).await;
    assert_exhausted(&call(&bridge, MCP_AUTO_CREATE_TOOL, create_input("0 9 * * *")).await);
    let ListResult::Ok(jobs) = bridge
        .automation_engine()
        .unwrap()
        .list("testapp", false)
        .await;
    assert!(jobs.is_empty(), "{jobs:?}");
    assert_eq!(pool(&bridge).await, 0);
}

#[tokio::test]
async fn a_refused_create_costs_nothing() {
    let bridge =
        ActiveBridge::test_new_with_automation_config(brenn_automation::AutomationGlobalConfig {
            max_jobs_per_app: 1,
            ..brenn_automation::AutomationGlobalConfig::default()
        })
        .await;
    set_pool(&bridge, 5).await;

    let bad_cron = call(&bridge, MCP_AUTO_CREATE_TOOL, create_input("not a cron")).await;
    assert!(matches!(bad_cron, ToolOutcome::Err(_)), "{bad_cron:?}");
    assert_eq!(pool(&bridge).await, 5);

    assert_ok(&call(&bridge, MCP_AUTO_CREATE_TOOL, create_input("0 9 * * *")).await);
    assert_eq!(pool(&bridge).await, 4);
    let over_limit = call(&bridge, MCP_AUTO_CREATE_TOOL, create_input("0 10 * * *")).await;
    assert!(matches!(over_limit, ToolOutcome::Err(_)), "{over_limit:?}");
    assert_eq!(pool(&bridge).await, 4);
}

#[tokio::test]
async fn a_create_with_bad_arguments_draws_nothing() {
    let bridge = ActiveBridge::test_new_with_automation().await;
    set_pool(&bridge, 5).await;
    let out = call(
        &bridge,
        MCP_AUTO_CREATE_TOOL,
        json!({ "name": "job", "trigger": { "kind": "cron", "expr": "0 9 * * *", "tz": "UTC" } }),
    )
    .await;
    assert!(matches!(out, ToolOutcome::Err(_)), "{out:?}");
    assert_eq!(pool(&bridge).await, 5);
}

#[tokio::test]
async fn each_priced_edit_draws_one_unit() {
    let bridge = ActiveBridge::test_new_with_automation().await;
    let id = seed_job(&bridge).await;
    set_pool(&bridge, 5).await;
    for (field, edit) in [
        (
            "action",
            json!({ "id": id, "action": { "kind": "send_message", "to": "brenn:test-channel", "body": "new" } }),
        ),
        (
            "trigger",
            json!({ "id": id, "trigger": { "kind": "cron", "expr": "0 10 * * *", "tz": "UTC" } }),
        ),
        ("enabled: true", json!({ "id": id, "enabled": true })),
    ] {
        let before = pool(&bridge).await;
        assert_ok(&call(&bridge, MCP_AUTO_EDIT_TOOL, edit).await);
        assert_eq!(pool(&bridge).await, before - 1, "{field}");
    }
    assert_eq!(pool(&bridge).await, 2);
}

#[tokio::test]
async fn a_priced_edit_at_an_empty_pool_is_refused_and_changes_nothing() {
    let bridge = ActiveBridge::test_new_with_automation().await;
    let id = seed_job(&bridge).await;
    let before = only_job(&bridge).await;
    set_pool(&bridge, 0).await;

    assert_exhausted(
        &call(
            &bridge,
            MCP_AUTO_EDIT_TOOL,
            json!({ "id": id, "action": { "kind": "send_message", "to": "brenn:test-channel", "body": "steered" } }),
        )
        .await,
    );
    assert_eq!(only_job(&bridge).await.action, before.action);

    assert_exhausted(
        &call(
            &bridge,
            MCP_AUTO_EDIT_TOOL,
            json!({ "id": id, "trigger": { "kind": "cron", "expr": "* * * * *", "tz": "UTC" } }),
        )
        .await,
    );
    let after = only_job(&bridge).await;
    assert_eq!(
        serde_json::to_value(&after.trigger).unwrap(),
        serde_json::to_value(&before.trigger).unwrap()
    );
    assert_eq!(after.next_fire_at, before.next_fire_at);

    {
        let conn = bridge.db().lock().await;
        conn.execute(
            "UPDATE automation_jobs SET enabled = 0, consecutive_failures = 5",
            [],
        )
        .unwrap();
    }
    assert_exhausted(
        &call(
            &bridge,
            MCP_AUTO_EDIT_TOOL,
            json!({ "id": id, "enabled": true }),
        )
        .await,
    );
    let after = only_job(&bridge).await;
    assert!(!after.enabled);
    assert_eq!(after.consecutive_failures, 5);
    assert_eq!(pool(&bridge).await, 0);
}

#[tokio::test]
async fn a_refused_edit_costs_nothing() {
    let bridge = ActiveBridge::test_new_with_automation().await;
    let id = seed_job(&bridge).await;
    set_pool(&bridge, 5).await;

    let unknown = call(
        &bridge,
        MCP_AUTO_EDIT_TOOL,
        json!({ "id": "550e8400-e29b-41d4-a716-446655440000", "enabled": true }),
    )
    .await;
    assert_eq!(
        unknown,
        ToolOutcome::Err(json!({ "ok": false, "error": "job not found" }))
    );
    assert_eq!(pool(&bridge).await, 5);

    let denied = call(
        &bridge,
        MCP_AUTO_EDIT_TOOL,
        json!({ "id": id, "action": { "kind": "send_message", "to": "brenn:no-such-channel", "body": "hi" } }),
    )
    .await;
    assert!(matches!(denied, ToolOutcome::Err(_)), "{denied:?}");
    assert_eq!(pool(&bridge).await, 5);
}

#[tokio::test]
async fn free_calls_at_an_empty_pool_succeed_and_draw_nothing() {
    let bridge = ActiveBridge::test_new_with_automation().await;
    let id = seed_job(&bridge).await;
    set_pool(&bridge, 0).await;

    assert_ok(
        &call(
            &bridge,
            MCP_AUTO_EDIT_TOOL,
            json!({ "id": id, "name": "renamed" }),
        )
        .await,
    );
    assert_eq!(only_job(&bridge).await.name, "renamed");
    assert_ok(
        &call(
            &bridge,
            MCP_AUTO_EDIT_TOOL,
            json!({ "id": id, "enabled": false }),
        )
        .await,
    );
    assert!(!only_job(&bridge).await.enabled);
    assert_ok(&call(&bridge, MCP_AUTO_LIST_TOOL, json!({})).await);
    assert_ok(&call(&bridge, MCP_AUTO_DELETE_TOOL, json!({ "id": id })).await);
    let ListResult::Ok(jobs) = bridge
        .automation_engine()
        .unwrap()
        .list("testapp", false)
        .await;
    assert!(jobs.is_empty(), "{jobs:?}");
    assert_eq!(pool(&bridge).await, 0);
}

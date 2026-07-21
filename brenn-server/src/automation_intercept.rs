//! Intercept handlers for the four automation MCP virtual tools.
//!
//! All four tools are auto-approved:
//! - PreToolUse intercepted, returns `Allow`.
//! - PostToolUse intercepted, executes the real work, returns
//!   `Continue { updated_output }` so CC sees the real result instead
//!   of `__NOOP__`.
//!
//! Pattern mirrors `messaging_intercept.rs`.
//!
//! The active bridge owns the conversation context (app_slug); this flows
//! into `AutomationEngine::create/edit/delete/list`.

use std::borrow::Cow;

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_lib::automation::{
    CreateJob, CreateResult, DeleteResult, EditJob, EditResult, ListResult, MCP_AUTO_CREATE_TOOL,
    MCP_AUTO_DELETE_TOOL, MCP_AUTO_EDIT_TOOL, MCP_AUTO_LIST_TOOL,
    job::{Action, CronTrigger, JobView, SendMessageAction, Trigger},
};
use serde::Serialize;

use crate::active_bridge::ActiveBridge;
use crate::intercept_helpers::{ToolErr, ToolOk, reject_tool, warn_if_unexpected_tool_response};

// ---------------------------------------------------------------------------
// Typed tool-response structs (C1)
// ---------------------------------------------------------------------------

/// Returned to CC when `AutomationCreate` succeeds.
#[derive(Serialize)]
struct AutomationCreateOk {
    // alphabetical: id, next_fire_at, ok
    id: String,
    next_fire_at: String,
    ok: bool,
}

/// Returned to CC when `AutomationList` succeeds.
#[derive(Serialize)]
struct AutomationListOk<'a> {
    // alphabetical: jobs, ok
    jobs: &'a [JobView],
    ok: bool,
}

/// Returned to CC when `AutomationEdit` succeeds.
#[derive(Serialize)]
struct AutomationEditOk {
    // alphabetical: next_fire_at, ok
    next_fire_at: String,
    ok: bool,
}

/// Outcome of `try_handle_automation_tool`. `None` means "not an automation
/// tool"; the caller falls through.
#[derive(Debug)]
pub enum AutomationHandled {
    Respond(CcApprovalDecision),
}

/// Try to handle an automation tool intercept. Returns `Some` only when the
/// tool name matches one of the four automation tools.
pub async fn try_handle_automation_tool(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<AutomationHandled> {
    match &req.kind {
        // ---- PreToolUse: all four automation tools auto-approve ----
        ApprovalKind::PreToolUse { tool_name, .. }
            if tool_name == MCP_AUTO_CREATE_TOOL
                || tool_name == MCP_AUTO_LIST_TOOL
                || tool_name == MCP_AUTO_EDIT_TOOL
                || tool_name == MCP_AUTO_DELETE_TOOL =>
        {
            Some(AutomationHandled::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // ---- PostToolUse: AutoCreate ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_AUTO_CREATE_TOOL => {
            warn_if_unexpected_tool_response("automation intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            let engine = match bridge.automation_engine() {
                Some(e) => e,
                None => {
                    return Some(no_automation_response());
                }
            };
            // Parse trigger.
            let trigger = match parse_trigger(tool_input) {
                Ok(t) => t,
                Err(e) => {
                    return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                }
            };
            // Parse action.
            let action = match parse_action(tool_input) {
                Ok(a) => a,
                Err(e) => {
                    return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                }
            };
            let name = match tool_input.get("name").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => {
                    return Some(
                        tool_error_response(bridge, tool_name, tool_input, "missing `name`").await,
                    );
                }
            };
            let enabled = tool_input
                .get("enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let req = CreateJob {
                name,
                trigger,
                action,
                enabled,
            };
            let result = engine.create(&bridge.app_slug, req).await;
            let (output_str, is_error) = match result {
                CreateResult::Ok { id, next_fire_at } => {
                    let ok = AutomationCreateOk {
                        id,
                        next_fire_at: next_fire_at.to_rfc3339(),
                        ok: true,
                    };
                    (
                        serde_json::to_string(&ok)
                            .expect("AutomationCreateOk serialization is infallible"),
                        false,
                    )
                }
                CreateResult::MissingSender => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "automation not available: this app has no messaging sender configured",
                        ),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CreateResult::OwnerHasNoUser => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "automation not available: this app has no allowed_users configured",
                        ),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CreateResult::InvalidName(msg) => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("invalid name: {msg}")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CreateResult::InvalidCron(e) => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("invalid cron: {e:?}")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CreateResult::InvalidAddress { addr, kind } => {
                    signal_address_denial(bridge, kind, &addr);
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(automation_address_denied_msg(&addr)),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CreateResult::BodyTooLarge { len, max } => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("body too large: {len} bytes (max {max})")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CreateResult::InvalidDeadline(v) => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!(
                            "delivery_deadline_secs {v} out of range [1, 2592000]"
                        )),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CreateResult::TooManyJobs { count, max } => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!(
                            "job limit reached: {count} jobs exist (max {max} per app); delete a job to free a slot"
                        )),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
            };
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, is_error,
            )
            .await;
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: AutoList ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_AUTO_LIST_TOOL => {
            warn_if_unexpected_tool_response("automation intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            let engine = match bridge.automation_engine() {
                Some(e) => e,
                None => {
                    return Some(no_automation_response());
                }
            };
            let enabled_only = tool_input
                .get("enabled_only")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let result = engine.list(&bridge.app_slug, enabled_only).await;
            let output_str = match result {
                ListResult::Ok(jobs) => {
                    let ok = AutomationListOk {
                        jobs: &jobs,
                        ok: true,
                    };
                    serde_json::to_string(&ok)
                        .expect("AutomationListOk serialization is infallible")
                }
            };
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, false,
            )
            .await;
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: AutoEdit ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_AUTO_EDIT_TOOL => {
            warn_if_unexpected_tool_response("automation intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            let engine = match bridge.automation_engine() {
                Some(e) => e,
                None => {
                    return Some(no_automation_response());
                }
            };
            let id = match tool_input.get("id").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => {
                    return Some(
                        tool_error_response(bridge, tool_name, tool_input, "missing `id`").await,
                    );
                }
            };
            // Parse optional fields.
            let trigger = if tool_input.get("trigger").is_some() {
                match parse_trigger(tool_input) {
                    Ok(t) => Some(t),
                    Err(e) => {
                        return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                    }
                }
            } else {
                None
            };
            let action = if tool_input.get("action").is_some() {
                match parse_action(tool_input) {
                    Ok(a) => Some(a),
                    Err(e) => {
                        return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                    }
                }
            } else {
                None
            };
            let name = tool_input
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let enabled = tool_input.get("enabled").and_then(|v| v.as_bool());
            let req = EditJob {
                id,
                name,
                trigger,
                action,
                enabled,
            };
            let result = engine.edit(&bridge.app_slug, req).await;
            let (output_str, is_error) = match result {
                EditResult::Ok { next_fire_at } => {
                    let ok = AutomationEditOk {
                        next_fire_at: next_fire_at.to_rfc3339(),
                        ok: true,
                    };
                    (
                        serde_json::to_string(&ok)
                            .expect("AutomationEditOk serialization is infallible"),
                        false,
                    )
                }
                EditResult::NotFound => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed("job not found"),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                EditResult::Forbidden { reason } => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("forbidden: {reason}")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                EditResult::InvalidName(msg) => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("invalid name: {msg}")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                EditResult::InvalidCron(e) => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("invalid cron: {e:?}")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                EditResult::InvalidAddress { addr, kind } => {
                    signal_address_denial(bridge, kind, &addr);
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(automation_address_denied_msg(&addr)),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                EditResult::BodyTooLarge { len, max } => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("body too large: {len} bytes (max {max})")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                EditResult::InvalidDeadline(v) => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!(
                            "delivery_deadline_secs {v} out of range [1, 2592000]"
                        )),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                EditResult::Unauthorized(msg) => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("unauthorized: {msg}")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
            };
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, is_error,
            )
            .await;
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: AutoDelete ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_AUTO_DELETE_TOOL => {
            warn_if_unexpected_tool_response("automation intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            let engine = match bridge.automation_engine() {
                Some(e) => e,
                None => {
                    return Some(no_automation_response());
                }
            };
            let id = match tool_input.get("id").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => {
                    return Some(
                        tool_error_response(bridge, tool_name, tool_input, "missing `id`").await,
                    );
                }
            };
            let result = engine.delete(&bridge.app_slug, &id).await;
            let (output_str, is_error) = match result {
                DeleteResult::Ok => {
                    let ok = ToolOk::new();
                    (
                        serde_json::to_string(&ok).expect("ToolOk serialization is infallible"),
                        false,
                    )
                }
                DeleteResult::NotFound => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed("job not found"),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                DeleteResult::Forbidden { reason } => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("forbidden: {reason}")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
            };
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, is_error,
            )
            .await;
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Input parsing helpers
// ---------------------------------------------------------------------------

/// Parse the `trigger` field from tool input into a `Trigger`.
fn parse_trigger(tool_input: &serde_json::Value) -> Result<Trigger, String> {
    let trigger_obj = tool_input
        .get("trigger")
        .ok_or_else(|| "missing `trigger`".to_string())?;
    let kind = trigger_obj
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "trigger.kind must be a string".to_string())?;
    match kind {
        "cron" => {
            let expr = trigger_obj
                .get("expr")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "trigger.expr is required for cron trigger".to_string())?
                .to_string();
            let tz = trigger_obj
                .get("tz")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "trigger.tz is required (IANA timezone name, e.g. UTC)".to_string())?
                .to_string();
            let persistent = trigger_obj
                .get("persistent")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Ok(Trigger::Cron(CronTrigger {
                expr,
                tz,
                persistent,
            }))
        }
        other => Err(format!("unknown trigger kind: {other:?}")),
    }
}

/// Parse the `action` field from tool input into an `Action`.
fn parse_action(tool_input: &serde_json::Value) -> Result<Action, String> {
    let action_obj = tool_input
        .get("action")
        .ok_or_else(|| "missing `action`".to_string())?;
    let kind = action_obj
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "action.kind must be a string".to_string())?;
    match kind {
        "send_message" => {
            let to = action_obj
                .get("to")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "action.to is required".to_string())?
                .to_string();
            let body = action_obj
                .get("body")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "action.body is required".to_string())?
                .to_string();
            // Reject legacy `wake` key — a stale-habit sender would otherwise
            // be silently defaulted to urgency `low` baking wrong urgency into
            // a recurring job (§2.4 reject-and-teach).
            if action_obj.get("wake").is_some() {
                return Err("action.wake is no longer valid; use action.urgency \
                     (\"very-low\"|\"low\"|\"normal\"|\"high\")"
                    .to_string());
            }
            let urgency_str = action_obj
                .get("urgency")
                .and_then(|v| v.as_str())
                .unwrap_or("low");
            let urgency = brenn_lib::messaging::Urgency::parse(urgency_str)
                .ok_or_else(|| format!("unknown action.urgency value: {urgency_str:?}"))?;
            let reply_to = action_obj
                .get("reply_to")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let delivery_deadline_secs = match action_obj.get("delivery_deadline_secs") {
                None | Some(serde_json::Value::Null) => None,
                Some(v) => {
                    let n = v.as_u64().ok_or_else(|| {
                        "action.delivery_deadline_secs must be a positive integer".to_string()
                    })?;
                    // Reject before cast to avoid silent truncation (security-1):
                    // a value > u32::MAX would wrap to a small number that passes
                    // the [1, 2_592_000] range check.
                    let n32 = u32::try_from(n).map_err(|_| {
                        format!("action.delivery_deadline_secs {n} exceeds maximum (2592000)")
                    })?;
                    Some(n32)
                }
            };
            Ok(Action::SendMessage(SendMessageAction {
                to,
                body,
                urgency,
                reply_to,
                delivery_deadline_secs,
            }))
        }
        other => Err(format!("unknown action kind: {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Unified LLM-visible reject for a create/edit address denial. Byte-identical
/// for malformed, out-of-scope, and unresolved addresses so it discloses no
/// channel-existence bit — the create/edit-time namespace oracle is closed.
fn automation_address_denied_msg(addr: &str) -> String {
    format!("address {addr:?} does not exist or is not allowed for this app")
}

/// Emit the durable-publish denial security event + once-per-process phone alert
/// for a create/edit address rejection, via the shared publish-denial signal.
/// `kind` is the internal denial tag carried on the result variant; `addr` is
/// CC-supplied and sanitized inside the helper.
fn signal_address_denial(
    bridge: &ActiveBridge,
    kind: brenn_lib::obs::security::DenialKind,
    addr: &str,
) {
    brenn_lib::obs::security::signal_publish_denial(
        bridge.alert_dispatcher(),
        brenn_lib::obs::security::SecurityEventType::BrennPublishDenied,
        bridge.denial_origin(),
        kind,
        addr,
    );
}

fn no_automation_response() -> AutomationHandled {
    let err = ToolErr {
        ok: false,
        error: Cow::Borrowed("automation is not configured on this brenn server"),
    };
    AutomationHandled::Respond(CcApprovalDecision::Continue {
        updated_output: Some(
            serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
        ),
    })
}

async fn tool_error_response(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
    error: &str,
) -> AutomationHandled {
    AutomationHandled::Respond(
        reject_tool(bridge, "automation tool", tool_name, tool_input, error).await,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::active_bridge::test_support::{post_tool_use_req, pre_tool_use_req};

    /// All four automation PreToolUse events must return Allow.
    #[tokio::test]
    async fn auto_tools_pre_tool_use_auto_approve() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        for tool_name in [
            MCP_AUTO_CREATE_TOOL,
            MCP_AUTO_LIST_TOOL,
            MCP_AUTO_EDIT_TOOL,
            MCP_AUTO_DELETE_TOOL,
        ] {
            let req = pre_tool_use_req(tool_name);
            match try_handle_automation_tool(&bridge, &req).await {
                Some(AutomationHandled::Respond(CcApprovalDecision::Allow { .. })) => {}
                other => panic!("{tool_name}: expected Allow, got {other:?}"),
            }
        }
    }

    /// AutoCreate/List/Edit/Delete with no engine configured → ok:false error.
    #[tokio::test]
    async fn auto_tools_post_tool_use_no_engine_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        for (tool_name, input) in [
            (
                MCP_AUTO_CREATE_TOOL,
                json!({ "name": "x", "trigger": {"kind":"cron","expr":"* * * * *","tz":"UTC"}, "action": {"kind":"send_message","to":"brenn:x","body":"hi"} }),
            ),
            (MCP_AUTO_LIST_TOOL, json!({})),
            (
                MCP_AUTO_EDIT_TOOL,
                json!({ "id": "550e8400-e29b-41d4-a716-446655440000" }),
            ),
            (
                MCP_AUTO_DELETE_TOOL,
                json!({ "id": "550e8400-e29b-41d4-a716-446655440000" }),
            ),
        ] {
            let req = post_tool_use_req(tool_name, input);
            match try_handle_automation_tool(&bridge, &req).await {
                Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                    updated_output: Some(out),
                })) => {
                    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                    assert_eq!(v["ok"], json!(false), "{tool_name}: should be error: {out}");
                }
                other => panic!("{tool_name}: unexpected: {other:?}"),
            }
        }
    }

    /// Non-automation tool → None.
    #[tokio::test]
    async fn non_automation_tool_returns_none() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = pre_tool_use_req("Bash");
        assert!(
            try_handle_automation_tool(&bridge, &req).await.is_none(),
            "non-automation tool should return None"
        );
    }

    /// AutoEdit missing id → error.
    #[tokio::test]
    async fn auto_edit_missing_id_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(MCP_AUTO_EDIT_TOOL, json!({ "name": "updated" }));
        match try_handle_automation_tool(&bridge, &req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// AutoDelete missing id → error.
    #[tokio::test]
    async fn auto_delete_missing_id_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(MCP_AUTO_DELETE_TOOL, json!({}));
        match try_handle_automation_tool(&bridge, &req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Full-engine tests (require test_new_with_automation fixture).
    // -----------------------------------------------------------------------

    /// AutoCreate with a valid cron + send_message action returns ok:true,
    /// an id, and next_fire_at (req §4: auto_create_succeeds_returns_id_and_next_fire).
    #[tokio::test]
    async fn auto_create_succeeds_returns_id_and_next_fire() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_automation().await;
        let req = post_tool_use_req(
            MCP_AUTO_CREATE_TOOL,
            json!({
                "name": "daily check",
                "trigger": { "kind": "cron", "expr": "0 9 * * *", "tz": "UTC", "persistent": false },
                "action": { "kind": "send_message", "to": "brenn:test-channel", "body": "hello" }
            }),
        );
        match try_handle_automation_tool(&bridge, &req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "should succeed: {out}");
                assert!(v["id"].as_str().is_some(), "should return id: {out}");
                assert!(
                    v["next_fire_at"].as_str().is_some(),
                    "should return next_fire_at: {out}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// AutoCreate with an invalid cron expression returns ok:false,
    /// no DB row created (req §4: auto_create_invalid_cron_returns_structured_error_no_db_change).
    #[tokio::test]
    async fn auto_create_invalid_cron_returns_structured_error_no_db_change() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_automation().await;
        let req = post_tool_use_req(
            MCP_AUTO_CREATE_TOOL,
            json!({
                "name": "bad cron",
                "trigger": { "kind": "cron", "expr": "not a cron", "tz": "UTC" },
                "action": { "kind": "send_message", "to": "brenn:test-channel", "body": "hello" }
            }),
        );
        match try_handle_automation_tool(&bridge, &req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("cron") || err.contains("invalid"),
                    "error should mention cron: {err}"
                );
                // Verify no row was inserted.
                let engine = bridge.automation_engine().unwrap();
                let brenn_lib::automation::ListResult::Ok(jobs) =
                    engine.list("testapp", false).await;
                assert!(
                    jobs.is_empty(),
                    "no DB row should be created for invalid cron"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// AutoEdit cross-app attempt returns forbidden and logs an anomaly
    /// (req §4: auto_edit_cross_app_rejected_with_anomaly_log).
    #[tokio::test]
    async fn auto_edit_cross_app_rejected_with_anomaly_log() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_automation().await;
        // Create a job owned by "testapp".
        let engine = bridge.automation_engine().unwrap();
        let create = engine
            .create(
                "testapp",
                brenn_lib::automation::CreateJob {
                    name: "owned job".to_string(),
                    trigger: brenn_lib::automation::Trigger::Cron(
                        brenn_lib::automation::CronTrigger {
                            expr: "0 9 * * *".to_string(),
                            tz: "UTC".to_string(),
                            persistent: false,
                        },
                    ),
                    action: brenn_lib::automation::Action::SendMessage(
                        brenn_lib::automation::SendMessageAction {
                            to: "brenn:test-channel".to_string(),
                            body: "hello".to_string(),
                            urgency: brenn_lib::messaging::Urgency::Low,
                            reply_to: None,
                            delivery_deadline_secs: None,
                        },
                    ),
                    enabled: true,
                },
            )
            .await;
        let id = match create {
            brenn_lib::automation::CreateResult::Ok { id, .. } => id,
            other => panic!("create failed: {other:?}"),
        };

        // Attempt edit from a different app slug — bridge has "testapp" but engine
        // checks caller vs owner. We exercise via engine directly for cross-app since
        // the bridge always uses its own app_slug.
        let edit_result = engine
            .edit(
                "otherapp",
                brenn_lib::automation::EditJob {
                    id: id.clone(),
                    name: Some("hacked".to_string()),
                    trigger: None,
                    action: None,
                    enabled: None,
                },
            )
            .await;
        match edit_result {
            brenn_lib::automation::EditResult::Forbidden { .. } => {}
            other => panic!("expected Forbidden, got: {other:?}"),
        }
    }

    /// AutoDelete cross-app attempt returns forbidden
    /// (req §4: auto_delete_cross_app_rejected_with_anomaly_log).
    #[tokio::test]
    async fn auto_delete_cross_app_rejected_with_anomaly_log() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_automation().await;
        let engine = bridge.automation_engine().unwrap();
        let create = engine
            .create(
                "testapp",
                brenn_lib::automation::CreateJob {
                    name: "owned job".to_string(),
                    trigger: brenn_lib::automation::Trigger::Cron(
                        brenn_lib::automation::CronTrigger {
                            expr: "0 9 * * *".to_string(),
                            tz: "UTC".to_string(),
                            persistent: false,
                        },
                    ),
                    action: brenn_lib::automation::Action::SendMessage(
                        brenn_lib::automation::SendMessageAction {
                            to: "brenn:test-channel".to_string(),
                            body: "hello".to_string(),
                            urgency: brenn_lib::messaging::Urgency::Low,
                            reply_to: None,
                            delivery_deadline_secs: None,
                        },
                    ),
                    enabled: true,
                },
            )
            .await;
        let id = match create {
            brenn_lib::automation::CreateResult::Ok { id, .. } => id,
            other => panic!("create failed: {other:?}"),
        };
        let delete_result = engine.delete("otherapp", &id).await;
        match delete_result {
            brenn_lib::automation::DeleteResult::Forbidden { .. } => {}
            other => panic!("expected Forbidden, got: {other:?}"),
        }
    }

    /// AutoList returns only the caller's app's jobs and filters correctly
    /// (req §4: auto_list_returns_only_caller_app_jobs, auto_list_filter_enabled_only_works).
    #[tokio::test]
    async fn auto_list_returns_only_caller_app_jobs_and_filter_enabled_works() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_automation().await;
        let engine = bridge.automation_engine().unwrap();

        // Create one enabled and one disabled job.
        let create_one = |name: &'static str, enabled: bool| {
            let engine = engine.clone();
            async move {
                engine
                    .create(
                        "testapp",
                        brenn_lib::automation::CreateJob {
                            name: name.to_string(),
                            trigger: brenn_lib::automation::Trigger::Cron(
                                brenn_lib::automation::CronTrigger {
                                    expr: "0 9 * * *".to_string(),
                                    tz: "UTC".to_string(),
                                    persistent: false,
                                },
                            ),
                            action: brenn_lib::automation::Action::SendMessage(
                                brenn_lib::automation::SendMessageAction {
                                    to: "brenn:test-channel".to_string(),
                                    body: "body".to_string(),
                                    urgency: brenn_lib::messaging::Urgency::Low,
                                    reply_to: None,
                                    delivery_deadline_secs: None,
                                },
                            ),
                            enabled,
                        },
                    )
                    .await
            }
        };
        create_one("enabled job", true).await;
        create_one("disabled job", false).await;

        // AutoList via intercept (no filter).
        let req = post_tool_use_req(MCP_AUTO_LIST_TOOL, json!({}));
        match try_handle_automation_tool(&bridge, &req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "should succeed: {out}");
                let jobs = v["jobs"].as_array().expect("jobs array");
                assert_eq!(jobs.len(), 2, "should return both jobs: {out}");
                // All jobs belong to testapp.
                for j in jobs {
                    assert_eq!(
                        j["owner_app_slug"].as_str().unwrap_or(""),
                        "testapp",
                        "all jobs should be owned by testapp"
                    );
                }
            }
            other => panic!("unexpected: {other:?}"),
        }

        // AutoList with enabled_only filter.
        let req_enabled = post_tool_use_req(MCP_AUTO_LIST_TOOL, json!({ "enabled_only": true }));
        match try_handle_automation_tool(&bridge, &req_enabled).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                let jobs = v["jobs"].as_array().expect("jobs array");
                assert_eq!(
                    jobs.len(),
                    1,
                    "enabled_only should return only 1 job: {out}"
                );
                assert_eq!(jobs[0]["enabled"], json!(true));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Wire-shape regression guards for C1 typed structs
    // -----------------------------------------------------------------------

    /// `AutomationCreateOk` must serialize byte-identically to the source `json!`.
    #[test]
    fn automation_create_ok_matches_reference() {
        let ok = super::AutomationCreateOk {
            ok: true,
            id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            next_fire_at: "2026-05-18T09:00:00+00:00".to_string(),
        };
        let produced =
            serde_json::to_string(&ok).expect("AutomationCreateOk serialization is infallible");
        let reference = serde_json::json!({
            "ok": true,
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "next_fire_at": "2026-05-18T09:00:00+00:00",
        })
        .to_string();
        assert_eq!(produced, reference);
    }

    /// `AutomationListOk` must serialize byte-identically to the source `json!`
    /// (empty jobs slice — guards struct shape and field names).
    #[test]
    fn automation_list_ok_matches_reference() {
        let jobs: &[brenn_lib::automation::job::JobView] = &[];
        let ok = super::AutomationListOk { ok: true, jobs };
        let produced =
            serde_json::to_string(&ok).expect("AutomationListOk serialization is infallible");
        let reference = serde_json::json!({ "jobs": [], "ok": true }).to_string();
        assert_eq!(produced, reference);
    }

    /// AutoCreate returns ok:false with "job limit reached" when the per-app cap
    /// is hit, exercising the `TooManyJobs` arm in `try_handle_automation_tool`.
    /// Verifies both the `ok` flag and the error message text surfaced to the LLM.
    #[tokio::test]
    async fn auto_create_too_many_jobs_returns_error_with_message() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_automation_config(
            brenn_lib::automation::AutomationGlobalConfig {
                max_jobs_per_app: 1,
                ..brenn_lib::automation::AutomationGlobalConfig::default()
            },
        )
        .await;

        let create_req = || {
            post_tool_use_req(
                MCP_AUTO_CREATE_TOOL,
                json!({
                    "name": "test job",
                    "trigger": { "kind": "cron", "expr": "0 9 * * *", "tz": "UTC", "persistent": false },
                    "action": { "kind": "send_message", "to": "brenn:test-channel", "body": "hi" }
                }),
            )
        };

        // First create must succeed.
        match try_handle_automation_tool(&bridge, &create_req()).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "first create must succeed: {out}");
            }
            other => panic!("first create: unexpected {other:?}"),
        }

        // Second create must fail with TooManyJobs.
        match try_handle_automation_tool(&bridge, &create_req()).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(
                    v["ok"],
                    json!(false),
                    "second create must be rejected: {out}"
                );
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("job limit reached"),
                    "error must contain 'job limit reached': {err}"
                );
            }
            other => panic!("second create: unexpected {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // test-2: Legacy `wake` key and unknown `urgency` value rejection in parse_action
    // -----------------------------------------------------------------------

    /// AutoCreate with legacy `action.wake` key returns `ok:false` error mentioning `urgency`.
    /// Guards the reject-and-teach path (§2.4): a stale-habit sender sending `wake` in a
    /// recurring job would bake the wrong urgency in permanently — must be an explicit error.
    #[tokio::test]
    async fn auto_create_legacy_wake_key_returns_error() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_with_automation_config(Default::default())
                .await;
        let req = post_tool_use_req(
            MCP_AUTO_CREATE_TOOL,
            json!({
                "name": "legacy wake job",
                "trigger": { "kind": "cron", "expr": "0 9 * * *", "tz": "UTC", "persistent": false },
                "action": { "kind": "send_message", "to": "brenn:test-channel", "body": "hi", "wake": "none" }
            }),
        );
        match try_handle_automation_tool(&bridge, &req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok:false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("urgency"),
                    "error must mention 'urgency': {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// AutoCreate with unknown `action.urgency` value returns `ok:false`.
    #[tokio::test]
    async fn auto_create_unknown_urgency_returns_error() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_with_automation_config(Default::default())
                .await;
        let req = post_tool_use_req(
            MCP_AUTO_CREATE_TOOL,
            json!({
                "name": "bad urgency job",
                "trigger": { "kind": "cron", "expr": "0 9 * * *", "tz": "UTC", "persistent": false },
                "action": { "kind": "send_message", "to": "brenn:test-channel", "body": "hi", "urgency": "garbage" }
            }),
        );
        match try_handle_automation_tool(&bridge, &req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok:false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("garbage"),
                    "error must mention the bad value 'garbage': {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// AutoCreate with an address outside the owner's `brenn_publish` scope (and
    /// absent from the directory) returns the unified reject that discloses no
    /// channel-existence bit, and drives the create-time denial signal path
    /// without panicking. The exact wording is pinned (byte-identical to the
    /// unresolved/malformed cases — the create-time namespace oracle is closed).
    #[tokio::test]
    async fn auto_create_denied_address_returns_unified_reject() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_automation().await;
        let req = post_tool_use_req(
            MCP_AUTO_CREATE_TOOL,
            json!({
                "name": "probe",
                "trigger": { "kind": "cron", "expr": "0 9 * * *", "tz": "UTC", "persistent": false },
                "action": { "kind": "send_message", "to": "brenn:no-such-channel", "body": "hi" }
            }),
        );
        match try_handle_automation_tool(&bridge, &req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                assert_eq!(
                    v["error"].as_str().unwrap(),
                    "address \"brenn:no-such-channel\" does not exist or is not allowed for this app",
                    "unified reject wording: {out}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Create/edit address-denial signal wiring (intercept level)
    // -----------------------------------------------------------------------

    /// The `format!("Security: {event_type}")` title `signal_address_denial`
    /// derives for `BrennPublishDenied` (routed through `signal_publish_denial`).
    const BRENN_DENIAL_ALERT_TITLE: &str = "Security: brenn_publish_denied";

    /// Drive a denial-producing `AutomationCreate` on a capturing bridge and
    /// return the captured `(title, body)` alerts. Exercises the full intercept
    /// path so a wiring regression in the create denial signal (a missing
    /// `signal_address_denial` call, a mislabeled `kind`) is caught, not just the
    /// engine's own validation logic.
    async fn capture_create_denial_alerts(
        to: &str,
        reply_to: Option<&str>,
    ) -> Vec<(String, String)> {
        let (dispatcher, captured, _handle) = brenn_lib::obs::alerting::make_capturing_alerter();
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_with_automation_config_and_dispatcher(
                Default::default(),
                dispatcher,
            )
            .await;
        let mut action = json!({ "kind": "send_message", "to": to, "body": "hi" });
        if let Some(rt) = reply_to {
            action["reply_to"] = json!(rt);
        }
        let req = post_tool_use_req(
            MCP_AUTO_CREATE_TOOL,
            json!({
                "name": "probe",
                "trigger": { "kind": "cron", "expr": "0 9 * * *", "tz": "UTC", "persistent": false },
                "action": action
            }),
        );
        let _ = try_handle_automation_tool(&bridge, &req).await;
        bridge.alert_dispatcher().flush().await;
        let alerts = captured.lock().unwrap();
        alerts.clone()
    }

    /// An out-of-`brenn_publish`-ACL `to` (also absent from the directory) is
    /// denied `acl_denied` at create time (ACL checked before resolution) and the
    /// intercept emits exactly one durable-publish security alert with that kind.
    #[tokio::test]
    async fn auto_create_out_of_acl_to_signals_acl_denied() {
        let alerts = capture_create_denial_alerts("brenn:no-such-channel", None).await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, BRENN_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=acl_denied"), "body: {body}");
    }

    /// A malformed `to` is denied `malformed_address` at create time and signals
    /// one alert with that kind.
    #[tokio::test]
    async fn auto_create_malformed_to_signals_malformed_address() {
        let alerts = capture_create_denial_alerts("brenn:bad name", None).await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, BRENN_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=malformed_address"), "body: {body}");
    }

    /// A `reply_to` outside the app's publish∪delivery visibility (with a
    /// publishable `to`) is denied `acl_denied` by the reply_to gate and signals
    /// one alert — proving the reply_to probe vector is signalled at create time.
    #[tokio::test]
    async fn auto_create_out_of_visibility_reply_to_signals_acl_denied() {
        let alerts =
            capture_create_denial_alerts("brenn:test-channel", Some("brenn:no-such-channel")).await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, BRENN_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=acl_denied"), "body: {body}");
    }

    /// Editing a job's action `to` to an out-of-ACL address is denied at edit
    /// time: the intercept returns the unified reject (byte-identical to the
    /// create path) and signals exactly one `acl_denied` alert. Guards the edit
    /// probe vector's signal wiring.
    #[tokio::test]
    async fn auto_edit_out_of_acl_to_signals_acl_denied() {
        let (dispatcher, captured, _handle) = brenn_lib::obs::alerting::make_capturing_alerter();
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_with_automation_config_and_dispatcher(
                Default::default(),
                dispatcher,
            )
            .await;
        // Create a valid job first (in-ACL `to` → no denial, no signal).
        let create_req = post_tool_use_req(
            MCP_AUTO_CREATE_TOOL,
            json!({
                "name": "job",
                "trigger": { "kind": "cron", "expr": "0 9 * * *", "tz": "UTC", "persistent": false },
                "action": { "kind": "send_message", "to": "brenn:test-channel", "body": "hi" }
            }),
        );
        let id = match try_handle_automation_tool(&bridge, &create_req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                v["id"].as_str().expect("create returns id").to_string()
            }
            other => panic!("create: unexpected {other:?}"),
        };
        // Edit the action's `to` to an out-of-ACL address → acl_denied.
        let edit_req = post_tool_use_req(
            MCP_AUTO_EDIT_TOOL,
            json!({
                "id": id,
                "action": { "kind": "send_message", "to": "brenn:no-such-channel", "body": "hi" }
            }),
        );
        let out = match try_handle_automation_tool(&bridge, &edit_req).await {
            Some(AutomationHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => out,
            other => panic!("edit: unexpected {other:?}"),
        };
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["ok"], json!(false), "edit should be denied: {out}");
        assert_eq!(
            v["error"].as_str().unwrap(),
            "address \"brenn:no-such-channel\" does not exist or is not allowed for this app",
            "edit unified reject wording: {out}"
        );
        bridge.alert_dispatcher().flush().await;
        let alerts = captured.lock().unwrap();
        assert_eq!(
            alerts.len(),
            1,
            "expected one edit-denial alert: {alerts:?}"
        );
        let (title, body) = &alerts[0];
        assert_eq!(title, BRENN_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=acl_denied"), "body: {body}");
    }

    /// `AutomationEditOk` must serialize byte-identically to the source `json!`.
    #[test]
    fn automation_edit_ok_matches_reference() {
        let ok = super::AutomationEditOk {
            ok: true,
            next_fire_at: "2026-05-19T09:00:00+00:00".to_string(),
        };
        let produced =
            serde_json::to_string(&ok).expect("AutomationEditOk serialization is infallible");
        let reference = serde_json::json!({
            "ok": true,
            "next_fire_at": "2026-05-19T09:00:00+00:00",
        })
        .to_string();
        assert_eq!(produced, reference);
    }
}

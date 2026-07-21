//! Intercept handler for the MQTT MCP virtual tool.
//!
//! - `MqttSend` — PreToolUse Allow; PostToolUse runs `MqttService::publish`.
//!   Counts against the messaging send budget.
//!
//! Pattern mirrors `messaging_intercept.rs` and `pwa_push_intercept.rs`.

use std::borrow::Cow;

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_lib::access::AppCapability;
use brenn_lib::mqtt::address::parse_topic_name;
use brenn_lib::mqtt::egress::{MqttEgressError, SendBudget, enforce_and_publish};
use brenn_lib::mqtt::payload::decode_outbound_body;
use brenn_lib::obs::security::{DenialKind, SecurityEventType, signal_publish_denial};

use crate::active_bridge::ActiveBridge;
use crate::intercept_helpers::{ToolErr, ToolOk, reject_tool, warn_if_unexpected_tool_response};
use crate::tools::mqtt::MCP_MQTT_SEND_TOOL;

/// Outcome of `try_handle_mqtt_tool`. `None` means "not an MQTT tool".
#[derive(Debug)]
pub enum MqttHandled {
    /// Send this decision back to CC.
    Respond(CcApprovalDecision),
}

/// Try to handle an MQTT tool intercept.
/// Returns `Some` only when the tool name matches the MQTT tool.
pub async fn try_handle_mqtt_tool(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<MqttHandled> {
    match &req.kind {
        // ---- PreToolUse: MqttSend auto-approves ----
        ApprovalKind::PreToolUse { tool_name, .. } if tool_name == MCP_MQTT_SEND_TOOL => {
            Some(MqttHandled::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // ---- PostToolUse: MqttSend ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MQTT_SEND_TOOL => {
            warn_if_unexpected_tool_response("mqtt intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;

            // Extract required `to`.
            let to = match tool_input.get("to").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "missing or empty `to` argument",
                        )
                        .await,
                    );
                }
            };

            // Cross-protocol misuse guards.
            if brenn_lib::messaging::ChannelScheme::of(&to)
                == Some(brenn_lib::messaging::ChannelScheme::Brenn)
            {
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "MqttSend only accepts `mqtt:` addresses. \
                         Use BrennSend for `brenn:` addresses.",
                    )
                    .await,
                );
            }
            if brenn_lib::messaging::ChannelScheme::of(&to)
                == Some(brenn_lib::messaging::ChannelScheme::PwaPush)
            {
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "MqttSend only accepts `mqtt:` addresses. \
                         Use PwaPushSend for `pwa_push:` addresses.",
                    )
                    .await,
                );
            }
            if brenn_lib::messaging::ChannelScheme::of(&to)
                == Some(brenn_lib::messaging::ChannelScheme::Webhook)
            {
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "MqttSend only accepts `mqtt:` addresses. \
                         `webhook:` channels are inbound-only in this version.",
                    )
                    .await,
                );
            }

            // Parse and validate the address (wildcards rejected in publish
            // context). Input validation, not authorization — stays at the call
            // site so the shared `enforce_and_publish` receives an already-parsed,
            // already-wildcard-validated `MqttAddress` (design §2.2).
            let addr = match parse_topic_name(&to) {
                Ok(a) => a,
                Err(e) => {
                    return Some(
                        tool_error_response(bridge, tool_name, tool_input, &e.to_string()).await,
                    );
                }
            };

            // Extract `body` (caller-specific input handling, design §2.2).
            let body_value = match tool_input.get("body") {
                Some(v) => v,
                None => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "missing `body` argument",
                        )
                        .await,
                    );
                }
            };

            let outbound = match decode_outbound_body(body_value) {
                Ok(p) => p,
                Err(e) => {
                    return Some(
                        tool_error_response(bridge, tool_name, tool_input, &e.to_string()).await,
                    );
                }
            };

            let qos = tool_input
                .get("qos")
                .and_then(|v| v.as_u64())
                .map(|n| n as u8)
                .unwrap_or(1);
            if qos > 2 {
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "invalid `qos`: must be 0, 1, or 2",
                    )
                    .await,
                );
            }
            let retain = tool_input
                .get("retain")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            // The MQTT tool is only offered on a server with messaging configured,
            // so a missing Messenger (and thus a missing resolved policy for a live
            // app slug) at this point is a host wiring bug, not bad input — panic
            // rather than fail open (mirrors `subscribe_dynamic_activated`'s policy
            // lookup; design §2.3 / §3.7).
            let messenger = bridge.messenger().unwrap_or_else(|| {
                panic!(
                    "MqttSend intercept: Messenger required for the mqtt_publish \
                     enforcement but absent on ActiveBridge"
                )
            });
            let policy = messenger.app_policy(&bridge.app_slug).unwrap_or_else(|| {
                panic!(
                    "MqttSend intercept: no resolved AppPolicy for app {:?} \
                     — every resolved app carries a (possibly empty) policy",
                    bridge.app_slug
                )
            });

            // Require MQTT service. Server-global readiness, distinct from a
            // per-app grant/ACL denial (handled inside `enforce_and_publish`). A
            // per-client session miss after an ACL pass is a boot-prevented panic
            // invariant, not a runtime outcome (design §3.5).
            let svc = match bridge.mqtt_service() {
                Some(s) => s,
                None => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "MQTT is not configured on this server",
                        )
                        .await,
                    );
                }
            };

            // Shared enforcement (design §2.3): the grant + per-client ACL,
            // session lookup, and send-budget decrement collapse into one call,
            // preserving the "validate before budget" ordering inside
            // `enforce_and_publish`. JSON extraction, address parsing, body
            // decoding, and all response mapping stay here (design §2.2). The
            // shared function locks the DB only for the budget decrement and drops
            // it before the broker await, so this future stays `Send` (it runs in a
            // `tokio::spawn`ed CC event loop) — we pass the `Db` handle, not a held
            // connection guard.
            let outcome = enforce_and_publish(
                svc,
                policy,
                &addr,
                outbound.bytes,
                outbound.content_type,
                qos,
                retain,
                SendBudget::Conversation {
                    db: bridge.db(),
                    conversation_id: bridge.conversation_id,
                    default_budget: bridge.app_config_default_send_budget(),
                },
            )
            .await;

            let (outcome_value, is_error) = match outcome {
                Ok(()) => {
                    let ok = ToolOk::new();
                    (
                        serde_json::to_value(&ok).expect("ToolOk serialization is infallible"),
                        false,
                    )
                }
                Err(MqttEgressError::AclDenied { client }) => {
                    // Distinguish "no grant at all" from "grant held, client not in
                    // the mqtt_publish ACL" via a cheap secondary `has_grant` check
                    // to pick both the security-signal `kind` and the right
                    // operator-facing remedy string.
                    let has_grant = policy.has_grant(AppCapability::MqttPublish);
                    let kind = if has_grant {
                        DenialKind::AclDenied
                    } else {
                        DenialKind::GrantAbsent
                    };
                    // An operator policy actively blocked an LLM MQTT publish — a
                    // security-relevant event, not LLM input error. Signal it like
                    // the durable/ephemeral publish denials via the shared helper: an
                    // app-attributed security-log line per occurrence plus a
                    // once-per-process phone alert. Both the acl_denied and
                    // grant_absent kinds signal — an absent grant is still an
                    // operator-policy denial. The LLM-facing text alone never reaches
                    // the logs. `client` is CC-supplied — the helper sanitizes it; the
                    // trusted `mqtt:` prefix makes the `address=mqtt:<client>` detail
                    // self-describing at the ACL's per-client granularity.
                    signal_publish_denial(
                        bridge.alert_dispatcher(),
                        SecurityEventType::MqttPublishDenied,
                        bridge.denial_origin(),
                        kind,
                        &format!("mqtt:{client}"),
                    );
                    // The two remedy strings stay distinct: each reveals only the
                    // app's own policy state (grant vs allowlist), no namespace bit.
                    let error = if has_grant {
                        Cow::Owned(format!(
                            "MQTT publish to client `{client}` is not allowed for this app: \
                             add an \"mqtt_publish\" ACL matcher for that client"
                        ))
                    } else {
                        Cow::Borrowed(
                            "MQTT publish is not enabled for this app: \
                             add \"mqtt_publish\" to this app's grants",
                        )
                    };
                    let err = ToolErr { ok: false, error };
                    (
                        serde_json::to_value(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                Err(MqttEgressError::BudgetExhausted) => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "budget exhausted: 0 remaining; ask the user to send a chat message to reset",
                        ),
                    };
                    (
                        serde_json::to_value(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                Err(MqttEgressError::BrokerRejected { reason }) => {
                    // Surface a server-side signal for a persistent broker
                    // rejection (e.g. broker-side ACL) so it reaches on-call.
                    // `reason` is broker-derived external input, so it is sanitized.
                    // Not unit-guarded: no fixture seam can synthesize
                    // `PubackOutcome::BrokerRejected` short of a live-broker sim.
                    tracing::warn!(
                        app_slug = %bridge.app_slug,
                        conversation_id = bridge.conversation_id,
                        reason = %sanitize_untrusted_str(&reason, MAX_LOGGED_UNTRUSTED_BYTES),
                        "MqttSend: broker rejected publish"
                    );
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("broker rejected publish: {reason}")),
                    };
                    (
                        serde_json::to_value(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                Err(MqttEgressError::Broker(e)) => {
                    // The LLM keeps the full `Display` (the conversation is the
                    // operator's own; the TLS/OS detail is operationally useful in
                    // chat). But the host log needs it too, guaranteeing on-call
                    // visibility regardless. The `Display` embeds broker-derived
                    // external text, so it is sanitized per the log-integrity
                    // posture even though it is not CC-supplied.
                    tracing::warn!(
                        app_slug = %bridge.app_slug,
                        conversation_id = bridge.conversation_id,
                        detail = %sanitize_untrusted_str(&e.to_string(), MAX_LOGGED_UNTRUSTED_BYTES),
                        "MqttSend: broker error"
                    );
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(e.to_string()),
                    };
                    (
                        serde_json::to_value(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
            };
            let output_str = serde_json::to_string(&outcome_value)
                .expect("outcome_value serialization is infallible");

            // Inject `_outcome` for the chat-history badge.
            let mut enriched = tool_input.clone();
            if let serde_json::Value::Object(map) = &mut enriched {
                map.insert("_outcome".to_string(), outcome_value);
            }
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, &enriched, is_error,
            )
            .await;

            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        _ => None,
    }
}

/// Common shape for "tool input failed validation" responses.
async fn tool_error_response(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
    error: &str,
) -> MqttHandled {
    MqttHandled::Respond(reject_tool(bridge, "mqtt tool", tool_name, tool_input, error).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
    use serde_json::json;

    use crate::active_bridge::test_support::{post_tool_use_req, pre_tool_use_req};

    #[tokio::test]
    async fn pre_tool_use_send_auto_approves() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = pre_tool_use_req(MCP_MQTT_SEND_TOOL);
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            })) => {}
            other => panic!("expected Allow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pre_tool_use_unrelated_returns_none() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = pre_tool_use_req("mcp__brenn__SomethingElse");
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn mqtt_send_missing_to_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(MCP_MQTT_SEND_TOOL, json!({ "body": "hello" }));
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(
                    v["error"].as_str().unwrap().contains("to"),
                    "error: {}",
                    v["error"]
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mqtt_send_brenn_address_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "brenn:channel", "body": "hi" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(err.contains("BrennSend"), "error: {err}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mqtt_send_pwa_push_address_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "pwa_push:alice", "body": "hi" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(err.contains("PwaPushSend"), "error: {err}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mqtt_send_wildcard_rejected() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_mqtt_publish_acl().await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:ha:home/+/state", "body": "hi" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mqtt_send_no_service_returns_error() {
        // Use a bridge with an mqtt_publish ACL so the per-app gate passes, allowing
        // the server-global "MQTT not configured on this server" gate to fire.
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_mqtt_publish_acl().await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:ha:home/sensor/temp", "body": "42" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(err.contains("not configured"), "error: {err}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // test-4: wildcard error message should mention wildcards
    #[tokio::test]
    async fn mqtt_send_wildcard_error_mentions_wildcard() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_mqtt_publish_acl().await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:ha:home/+/state", "body": "hi" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("wildcard") || err.contains('+') || err.contains('#'),
                    "wildcard error must mention wildcard characters: {err}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // test-5: invalid qos (> 2) returns error
    #[tokio::test]
    async fn mqtt_send_invalid_qos_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_mqtt_publish_acl().await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:ha:home/sensor/temp", "body": "42", "qos": 3 }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(err.contains("qos"), "qos error must mention qos: {err}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // test-6: missing body returns error
    #[tokio::test]
    async fn mqtt_send_missing_body_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_mqtt_publish_acl().await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:ha:home/sensor/temp" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("body"),
                    "missing-body error must mention body: {err}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn mqtt_send_grant_absent_returns_not_enabled() {
        // Policy lacks the `MqttPublish` grant (test_new_for_mqtt_no_grant
        // fixture). A valid mqtt: address is denied by the shared enforcement;
        // the intercept's secondary `has_grant` check (false) selects the
        // grant-absent "not enabled" remedy string (design §2.3).
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_no_grant().await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:ha:home/sensor/temp", "body": "42" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("MQTT publish is not enabled"),
                    "error must indicate MQTT publish not enabled: {err}"
                );
                assert!(
                    err.contains("mqtt_publish"),
                    "error must point at the mqtt_publish grant: {err}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
        // The grant-absent branch signals an app-attributed security event with
        // kind=grant_absent — an absent grant is still an operator-policy denial.
        // The detail pins the canonical `address=mqtt:<client>` label (not the
        // pre-refactor `client=<client>`); the client here is `ha`.
        assert!(
            logs_contain("app security event") && logs_contain("kind=grant_absent address=mqtt:ha"),
            "grant-absent MQTT deny must emit a security event with \
             kind=grant_absent address=mqtt:ha"
        );
    }

    /// The LLM keeps the FULL broker-error `Display`, not the coarse kind the
    /// WASM guest gets. A disconnected session surfaces `NotConnected`, whose
    /// `Display` names the client and a `last error:` clause — neither of which the
    /// coarse `"not connected"` kind contains. Pins the deliberate asymmetry.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn mqtt_send_broker_error_retains_full_display_for_llm() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_publish_acl(&["home"], &["home"])
                .await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:home:cmd/light", "body": "on" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                // Full Display markers the coarse kind would strip.
                assert!(
                    err.contains("not connected") && err.contains("last error"),
                    "LLM must receive the full broker-error Display: {err}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
        // The LLM keeps the full Display in-chat, but on-call visibility depends
        // on the host-side warn firing regardless — guard it against regression.
        assert!(
            logs_contain("MqttSend: broker error"),
            "broker error must emit a host-side warn for on-call visibility"
        );
    }

    // -----------------------------------------------------------------------
    // Seam C — MQTT publish per-client ACL (design §2.4)
    // -----------------------------------------------------------------------

    /// Denied (tool error naming `mqtt_publish`) when the app holds `MqttPublish`
    /// (coarse grant gate passes) but `addr.client` is not in its `mqtt_publish`
    /// ACL. The denial must fire on the address's client before reaching connector
    /// resolution or the budget decrement.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn mqtt_send_acl_denies_unlisted_client() {
        // ACL allows `home`; a connector exists for `home`. The publish targets
        // `office`, which is in neither — denied by the ACL.
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_publish_acl(&["home"], &["home"])
                .await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:office:cmd/light", "body": "on" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("mqtt_publish"),
                    "ACL-denied error must point at the mqtt_publish allowlist: {err}"
                );
                assert!(
                    err.contains("office"),
                    "ACL-denied error should name the denied client: {err}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
        // An operator policy actively blocking a publish is a security-relevant
        // event that must surface server-side (the LLM-facing error text never
        // reaches the logs). The denial emits an app-attributed security event
        // (kind + sanitized client) plus a once-per-process phone alert.
        assert!(
            logs_contain("app security event"),
            "ACL deny on the LLM caller must emit an app security event"
        );
        assert!(
            logs_contain("mqtt_publish_denied"),
            "the security event must carry the mqtt_publish_denied event_type"
        );
        // Pin the canonical `kind=… address=mqtt:<client>` detail schema (not the
        // pre-refactor `client=<client>` label).
        assert!(
            logs_contain("kind=acl_denied address=mqtt:office"),
            "grant-held ACL deny must carry kind=acl_denied address=mqtt:office"
        );
    }

    /// Allowed: the client is in the `mqtt_publish` ACL **and** a connector exists,
    /// so the publish passes the Seam-C check and reaches `MqttService::publish`.
    /// Without a live broker the publish returns `NotConnected`, but reaching that
    /// error proves the ACL check passed and connector resolution succeeded (it is
    /// distinct from both the ACL-denied and the no-connector errors).
    #[tokio::test]
    async fn mqtt_send_acl_allows_listed_client_with_connector() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_publish_acl(&["home"], &["home"])
                .await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:home:cmd/light", "body": "on" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                // The publish reaches the broker layer (no live connection → error),
                // which proves the ACL allowed it and connector resolution succeeded.
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    !err.contains("mqtt_publish") && !err.contains("no connector"),
                    "an allowed publish with a connector must pass the ACL and connector \
                     resolution (reaching the broker layer), got: {err}"
                );
                // It reached the broker layer: a disconnected connector reports
                // NotConnected, confirming ACL + connector resolution both passed.
                assert!(
                    err.contains("not connected"),
                    "an allowed publish should reach the (disconnected) broker layer: {err}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// Visibility is not enforcement (design §2.6 invariant): the Seam-C ACL check
    /// fires for a client not in `mqtt_publish` **even when a connector for that
    /// client exists**. Connector presence (the legacy proto-ACL) never implies
    /// authorization — authorization is the explicit `mqtt_publish` ACL.
    #[tokio::test]
    async fn mqtt_send_acl_denies_even_with_connector_present() {
        // A connector exists for `home`, but the ACL allows no clients at all.
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_publish_acl(&[], &["home"]).await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:home:cmd/light", "body": "on" }),
        );
        let result = try_handle_mqtt_tool(&bridge, &req).await;
        match result {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("mqtt_publish"),
                    "ACL must deny despite a connector being present: {err}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // Note: the former "grant+ACL pass but no session for the client" case is now a
    // boot-prevented invariant — every ACL matcher's client is validated and a
    // session registered at boot, so a miss at the publish path panics rather than
    // returning an error. That panic is covered by the `enforce_and_publish`
    // session-miss unit test in `brenn-lib::mqtt::egress`.

    // -----------------------------------------------------------------------
    // Wire-shape regression guards for typed structs (B4)
    // -----------------------------------------------------------------------

    /// B4 success arm: `ToolOk` must serialize to `{"ok":true}`.
    /// (Shared struct tested fully in intercept_helpers; this guards the B4
    /// call site specifically.)
    #[test]
    fn mqtt_send_ok_matches_reference() {
        use crate::intercept_helpers::ToolOk;
        let ok = ToolOk::new();
        let produced = serde_json::to_string(&ok).expect("ToolOk serialization is infallible");
        let reference = serde_json::json!({ "ok": true }).to_string();
        assert_eq!(produced, reference);
    }
}

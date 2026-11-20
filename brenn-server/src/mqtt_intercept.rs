//! Intercept handler for the MQTT MCP virtual tool.
//!
//! - `MqttSend` — PreToolUse Allow; PostToolUse publishes a text body, a
//!   base64 body or an attachment of a message the conversation was shown.
//!   Counts against the messaging send budget.
//!
//! `execute_mqtt_tool` is the execution, independent of the hook.

use std::borrow::Cow;

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_envelope::grants::AppCapability;
use brenn_lib::access::AppPolicy;
use brenn_lib::mqtt::address::parse_topic_name;
use brenn_messaging_store::blob::{BlobError, BlobHold};
use brenn_mqtt::PublishProps;
use brenn_mqtt::egress::{Authorized, MqttEgressError, SendBudget, authorize};
use brenn_mqtt::payload::{OutboundPayload, decode_outbound_body};
use brenn_obs::security::{DenialKind, SecurityEventType, signal_publish_denial};

use crate::active_bridge::{ActiveBridge, ToolOutcome};
use crate::intercept_helpers::{
    PostCard, ToolOk, answer_post_tool_use, outcome_err, outcome_ok, outcome_rejection,
};
use crate::messaging_intercept;
use brenn_render::tools::mqtt::MCP_MQTT_SEND_TOOL;

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

        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MQTT_SEND_TOOL => Some(MqttHandled::Respond(
            answer_post_tool_use(
                bridge,
                "mqtt intercept",
                tool_name,
                tool_input,
                tool_use_id,
                tool_response,
                PostCard::SummaryWithOutcome,
            )
            .await,
        )),

        _ => None,
    }
}

/// Execute `MqttSend` for the agent. `None` when `mcp_name` is not the MQTT
/// tool.
pub(crate) async fn execute_mqtt_tool(
    bridge: &ActiveBridge,
    mcp_name: &str,
    input: &serde_json::Value,
) -> Option<ToolOutcome> {
    if mcp_name != MCP_MQTT_SEND_TOOL {
        return None;
    }
    Some(execute_send(bridge, mcp_name, input).await)
}

/// What one `MqttSend` call publishes, once every check that precedes the
/// budget has passed.
struct PreparedPublish {
    authorized: Authorized,
    payload: Vec<u8>,
    props: PublishProps,
}

/// `MqttSend`: [`prepare`], then the budget and the publish.
async fn execute_send(
    bridge: &ActiveBridge,
    tool_name: &str,
    input: &serde_json::Value,
) -> ToolOutcome {
    let PreparedPublish {
        authorized,
        payload,
        props,
    } = match prepare(bridge, tool_name, input).await {
        Ok(prepared) => prepared,
        Err(answer) => return answer,
    };
    let client = authorized.client().slug.clone();

    // Pass the `Db` handle, not a held guard: this future runs in a
    // `tokio::spawn`ed CC event loop and must stay `Send`.
    let outcome = authorized
        .publish(
            bridge.mqtt_service(),
            payload,
            props,
            SendBudget::Conversation {
                db: bridge.db(),
                conversation_id: bridge.conversation_id,
                default_budget: bridge.app_config_default_send_budget(),
            },
        )
        .await;

    match outcome {
        Ok(()) => outcome_ok(&ToolOk::new()),
        Err(MqttEgressError::BudgetExhausted) => outcome_err(Cow::Borrowed(
            "budget exhausted: 0 remaining; ask the user to send a chat message to reset",
        )),
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
            outcome_err(Cow::Owned(format!("broker rejected publish: {reason}")))
        }
        Err(MqttEgressError::PacketTooLarge {
            packet_bytes,
            broker_max,
        }) => {
            // Both numbers go to the conversation, which is the operator's
            // own, so it can tell the publish will not fit on any retry.
            tracing::warn!(
                app_slug = %bridge.app_slug,
                conversation_id = bridge.conversation_id,
                client = %sanitize_untrusted_str(&client, MAX_LOGGED_UNTRUSTED_BYTES),
                packet_bytes,
                broker_max,
                "MqttSend: publish exceeds the broker's maximum packet size"
            );
            outcome_err(Cow::Owned(format!(
                "publish is a {packet_bytes}-byte MQTT packet; the broker of MQTT client \
                 `{client}` accepts at most {broker_max} bytes"
            )))
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
            outcome_err(Cow::Owned(e.to_string()))
        }
        Err(
            e @ (MqttEgressError::AclDenied { .. } | MqttEgressError::ClientNotRegistered { .. }),
        ) => {
            unreachable!("Authorized::publish runs neither the ACL nor the session lookup: {e:?}")
        }
    }
}

/// Every step of `MqttSend` before the budget: the arguments, the admission
/// of an attachment reference, the ACL and the session lookup, the client's
/// outbound bound, and the read of an attachment's bytes. A refusal here returns
/// the call's answer and spends no budget.
async fn prepare(
    bridge: &ActiveBridge,
    tool_name: &str,
    input: &serde_json::Value,
) -> Result<PreparedPublish, ToolOutcome> {
    let to = match input.get("to").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return Err(reject(tool_name, "missing or empty `to` argument"));
        }
    };

    // Cross-protocol misuse guards.
    if brenn_lib::messaging::ChannelScheme::of(&to)
        == Some(brenn_lib::messaging::ChannelScheme::Brenn)
    {
        return Err(reject(
            tool_name,
            "MqttSend only accepts `mqtt:` addresses. \
                 Use BrennSend for `brenn:` addresses.",
        ));
    }
    if brenn_lib::messaging::ChannelScheme::of(&to)
        == Some(brenn_lib::messaging::ChannelScheme::PwaPush)
    {
        return Err(reject(
            tool_name,
            "MqttSend only accepts `mqtt:` addresses. \
                 Use PwaPushSend for `pwa_push:` addresses.",
        ));
    }
    if brenn_lib::messaging::ChannelScheme::of(&to)
        == Some(brenn_lib::messaging::ChannelScheme::Webhook)
    {
        return Err(reject(
            tool_name,
            "MqttSend only accepts `mqtt:` addresses. \
                 `webhook:` channels are inbound-only in this version.",
        ));
    }

    // Parse and validate the address (wildcards rejected in publish
    // context). `authorize` requires an already-parsed, wildcard-validated
    // `MqttAddress`.
    let addr = parse_topic_name(&to).map_err(|e| reject(tool_name, &e.to_string()))?;

    let form = match (input.get("body"), input.get("attachment")) {
        (Some(_), Some(_)) => {
            return Err(reject(
                tool_name,
                "both `body` and `attachment` are given; give exactly one",
            ));
        }
        (None, None) => {
            return Err(reject(
                tool_name,
                "missing payload: give exactly one of `body` and `attachment`",
            ));
        }
        (Some(body), None) => PayloadForm::Inline(
            decode_outbound_body(body).map_err(|e| reject(tool_name, &e.to_string()))?,
        ),
        (None, Some(reference)) => {
            let Some((message_id, name)) = messaging_intercept::attachment_reference(reference)
            else {
                return Err(reject(
                    tool_name,
                    &format!(
                        "`attachment` must be {}",
                        messaging_intercept::ATTACHMENT_REFERENCE_SHAPE
                    ),
                ));
            };
            PayloadForm::Attachment { message_id, name }
        }
    };

    let qos = input
        .get("qos")
        .and_then(|v| v.as_u64())
        .map(|n| n as u8)
        .unwrap_or(1);
    if qos > 2 {
        return Err(reject(tool_name, "invalid `qos`: must be 0, 1, or 2"));
    }
    let retain = input
        .get("retain")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // The MQTT tool is only offered on a server with messaging configured,
    // so a missing Messenger (and thus a missing resolved policy for a live
    // app slug) at this point is a host wiring bug, not bad input — panic
    // rather than fail open.
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

    // The hold is taken before the ACL; no byte is read until the bound
    // has passed.
    let form = match form {
        PayloadForm::Inline(outbound) => AdmittedForm::Inline(outbound),
        PayloadForm::Attachment { message_id, name } => AdmittedForm::Attachment(
            messaging_intercept::admit_attachment_reference(
                bridge,
                messenger,
                "`attachment`",
                message_id,
                name,
            )
            .await?,
        ),
    };

    // A per-app grant/ACL denial comes back from `authorize`, and so does a
    // per-client session miss after an ACL pass: the policy read above is a
    // snapshot, and a reload that stops a client swaps the authority naming
    // it before it stops the session, so an in-flight tool call can hold an
    // authority the registry has already moved past.
    let authorized = match authorize(bridge.mqtt_service(), &policy, &addr) {
        Ok(authorized) => authorized,
        Err(MqttEgressError::AclDenied { client }) => {
            return Err(answer_acl_denied(bridge, &policy, &client));
        }
        Err(MqttEgressError::ClientNotRegistered { client }) => {
            // The operator removed the client from the document while
            // this call was in flight. Not a security event and not a
            // host bug: the agent asked for something that was true when
            // it asked. `client` is CC-supplied, so the log line
            // sanitizes it; the LLM-facing text repeats what it sent.
            tracing::warn!(
                app_slug = %bridge.app_slug,
                conversation_id = bridge.conversation_id,
                client = %sanitize_untrusted_str(&client, MAX_LOGGED_UNTRUSTED_BYTES),
                "MqttSend: the client's session is no longer registered (removed by a reload)"
            );
            return Err(outcome_err(Cow::Owned(format!(
                "MQTT client `{client}` is no longer configured on this server"
            ))));
        }
        Err(
            e @ (MqttEgressError::BudgetExhausted
            | MqttEgressError::Broker(_)
            | MqttEgressError::BrokerRejected { .. }
            | MqttEgressError::PacketTooLarge { .. }),
        ) => unreachable!("authorize runs neither the budget nor the publish: {e:?}"),
    };

    // The bound is the session's, checked after the ACL so a denied app
    // learns nothing of the client's configuration, and before any blob byte
    // is read.
    let bound = authorized.client().outbound_payload_cap_bytes;
    let size = match &form {
        AdmittedForm::Inline(outbound) => outbound.bytes.len() as u64,
        AdmittedForm::Attachment(hold) => hold.reference().size,
    };
    if size > u64::from(bound) {
        return Err(messaging_intercept::refusal(
            "payload_too_large",
            format!(
                "payload is {size} bytes; MQTT client `{}` publishes at most {bound} \
                 (`outbound_payload_cap_bytes`)",
                authorized.client().slug,
            ),
        ));
    }

    let (payload, content_type) = match form {
        AdmittedForm::Inline(outbound) => (outbound.bytes, outbound.content_type),
        AdmittedForm::Attachment(hold) => {
            let content_type = hold.reference().content_type;
            (read_blob(bridge, hold).await?, Some(content_type))
        }
    };

    Ok(PreparedPublish {
        authorized,
        payload,
        props: PublishProps {
            content_type,
            qos,
            retain,
            message_expiry_s: None,
        },
    })
}

/// The payload form of one call, as its arguments give it.
enum PayloadForm<'a> {
    Inline(OutboundPayload),
    Attachment { message_id: &'a str, name: &'a str },
}

/// The payload form once an attachment reference is admitted.
enum AdmittedForm {
    Inline(OutboundPayload),
    Attachment(BlobHold),
}

/// The whole of a held blob, read on the blocking pool since the store's I/O
/// is synchronous. A store I/O error is the host's fault and is logged at
/// `warn`.
async fn read_blob(bridge: &ActiveBridge, hold: BlobHold) -> Result<Vec<u8>, ToolOutcome> {
    let reference = hold.reference();
    let len = usize::try_from(reference.size).expect("a blob within the bound fits in memory");
    let read = tokio::task::spawn_blocking(move || hold.read(0, len))
        .await
        .expect("a blob read task does not panic or get cancelled");
    match read {
        Ok(bytes) => Ok(bytes),
        Err(BlobError::Io(e)) => {
            tracing::warn!(
                app_slug = %bridge.app_slug,
                conversation_id = bridge.conversation_id,
                blob = %reference.id,
                error = %e,
                "MqttSend: blob store read failed"
            );
            Err(outcome_err(Cow::Owned(format!(
                "the blob store could not read: {e}"
            ))))
        }
        Err(e) => panic!(
            "BUG: a read of the whole of blob {} ({len} bytes) failed: {e}",
            reference.id
        ),
    }
}

/// The answer to an `MqttSend` the app's `mqtt_publish` grant or ACL denies,
/// with its security signal.
fn answer_acl_denied(bridge: &ActiveBridge, policy: &AppPolicy, client: &str) -> ToolOutcome {
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
    outcome_err(error)
}

/// A rejection of the call's arguments: warn-logged, `{"ok":false,"error":…}`.
fn reject(tool_name: &str, error: &str) -> ToolOutcome {
    outcome_rejection("mqtt tool", tool_name, error)
}

#[cfg(test)]
mod attachment_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
    use serde_json::json;

    use crate::active_bridge::test_support::{
        post_tool_use_req, pre_tool_use_req, send_budget_row,
    };
    use crate::test_support::mqtt::register_test_client;

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
    async fn mqtt_send_on_a_client_with_no_session_is_a_tool_error() {
        // An ACL naming a client the registry holds no session for is what an
        // in-flight tool call sees when a reload removes that client: the
        // policy this call is deciding against was resolved before the agent
        // swap, and the session is stopped after it. The agent asked for
        // something that was true when it asked, so it is told, not panicked
        // at.
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_mqtt_publish_acl().await;
        let req = post_tool_use_req(
            MCP_MQTT_SEND_TOOL,
            json!({ "to": "mqtt:ha:home/sensor/temp", "body": "42" }),
        );
        match try_handle_mqtt_tool(&bridge, &req).await {
            Some(MqttHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("no longer configured") && err.contains("ha"),
                    "the error names the client and why: {err}"
                );
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

    #[tokio::test]
    async fn mqtt_send_with_neither_payload_form_returns_error() {
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
                    err.contains("`body`") && err.contains("`attachment`"),
                    "the error must name both payload forms: {err}"
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
        // grant-absent "not enabled" remedy string.
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

    /// A publish over the broker's advertised maximum packet size is a tool
    /// error naming the packet's size, the client and the broker's limit, with
    /// a host-side warn.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn mqtt_send_over_the_broker_maximum_names_the_limit() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_publish_acl(&["home"], &["home"])
                .await;
        let handle = bridge
            .mqtt_service()
            .get_client("home")
            .expect("the fixture registers a session for `home`");
        let _eventloop = brenn_mqtt::test_support::install_unpolled_client(&handle, Some(8)).await;

        assert_eq!(
            execute_mqtt_tool(
                &bridge,
                MCP_MQTT_SEND_TOOL,
                &json!({ "to": "mqtt:home:cmd/light", "body": "on", "qos": 0 }),
            )
            .await,
            Some(ToolOutcome::Err(json!({
                "ok": false,
                "error": "publish is a 16-byte MQTT packet; the broker of MQTT client `home` \
                          accepts at most 8 bytes",
            })))
        );
        assert!(logs_contain(
            "MqttSend: publish exceeds the broker's maximum packet size"
        ));
    }

    /// A bridge whose app may publish to `home`, with a registered session for
    /// `home` that publishes at most `bound` bytes a call.
    async fn bridge_with_bound(bound: u32) -> std::sync::Arc<ActiveBridge> {
        let bridge = ActiveBridge::test_new_for_mqtt_publish_acl(&["home"], &[]).await;
        register_test_client(bridge.mqtt_service(), "home", bound);
        bridge
    }

    async fn prepared(bridge: &ActiveBridge, input: serde_json::Value) -> PreparedPublish {
        match prepare(bridge, MCP_MQTT_SEND_TOOL, &input).await {
            Ok(prepared) => prepared,
            Err(answer) => panic!("expected the call to be prepared, got {answer:?}"),
        }
    }

    fn payload_too_large(len: usize, bound: u32) -> Option<ToolOutcome> {
        Some(ToolOutcome::Err(json!({
            "ok": false,
            "reason": "payload_too_large",
            "error": format!(
                "payload is {len} bytes; MQTT client `home` publishes at most {bound} \
                 (`outbound_payload_cap_bytes`)"
            ),
        })))
    }

    #[tokio::test]
    async fn a_text_body_publishes_its_utf8_bytes_untyped() {
        let bridge = bridge_with_bound(1024).await;
        let p = prepared(
            &bridge,
            json!({ "to": "mqtt:home:cmd/light", "body": "café", "qos": 2, "retain": true }),
        )
        .await;
        assert_eq!(p.payload, "café".as_bytes());
        assert_eq!(
            p.props,
            PublishProps {
                content_type: None,
                qos: 2,
                retain: true,
                message_expiry_s: None,
            }
        );
        assert_eq!(p.authorized.client().slug, "home");
    }

    #[tokio::test]
    async fn a_base64_body_publishes_the_decoded_bytes_with_its_content_type() {
        let bridge = bridge_with_bound(1024).await;
        let p = prepared(
            &bridge,
            json!({
                "to": "mqtt:home:cam/snapshot",
                "body": { "binary_base64": "AAEC/w==", "content_type": "image/png" },
            }),
        )
        .await;
        assert_eq!(p.payload, [0x00, 0x01, 0x02, 0xff]);
        assert_eq!(
            p.props,
            PublishProps {
                content_type: Some("image/png".to_string()),
                qos: 1,
                retain: false,
                message_expiry_s: None,
            }
        );
    }

    #[tokio::test]
    async fn a_base64_body_with_an_unknown_key_is_rejected() {
        let bridge = bridge_with_bound(1024).await;
        assert_eq!(
            execute_mqtt_tool(
                &bridge,
                MCP_MQTT_SEND_TOOL,
                &json!({
                    "to": "mqtt:home:cam/snapshot",
                    "body": { "binary_base64": "AAEC", "contentType": "image/png" },
                }),
            )
            .await,
            Some(ToolOutcome::Err(json!({
                "ok": false,
                "error": "unknown key `contentType` in body: the object form takes \
                          `binary_base64` and `content_type` only",
            })))
        );
        assert_eq!(send_budget_row(&bridge).await, None);
    }

    #[tokio::test]
    async fn a_payload_exactly_at_the_bound_is_prepared() {
        let bridge = bridge_with_bound(4).await;
        let p = prepared(
            &bridge,
            json!({ "to": "mqtt:home:cmd/light", "body": "four" }),
        )
        .await;
        assert_eq!(p.payload, b"four");
    }

    #[tokio::test]
    async fn a_text_body_over_the_bound_is_refused_before_the_budget() {
        let bridge = bridge_with_bound(4).await;
        assert_eq!(
            execute_mqtt_tool(
                &bridge,
                MCP_MQTT_SEND_TOOL,
                &json!({ "to": "mqtt:home:cmd/light", "body": "fives" }),
            )
            .await,
            payload_too_large(5, 4)
        );
        assert_eq!(send_budget_row(&bridge).await, None);
    }

    #[tokio::test]
    async fn a_base64_body_over_the_bound_is_refused_on_its_decoded_length() {
        // Eight base64 characters, five decoded bytes: the bound is on what is
        // published.
        let bridge = bridge_with_bound(4).await;
        assert_eq!(
            execute_mqtt_tool(
                &bridge,
                MCP_MQTT_SEND_TOOL,
                &json!({ "to": "mqtt:home:cmd/light", "body": { "binary_base64": "AAECAwQ=" } }),
            )
            .await,
            payload_too_large(5, 4)
        );
        assert_eq!(send_budget_row(&bridge).await, None);
    }

    #[tokio::test]
    async fn an_acl_denial_answers_before_the_bound() {
        let bridge = bridge_with_bound(4).await;
        register_test_client(bridge.mqtt_service(), "office", 4);
        assert_eq!(
            execute_mqtt_tool(
                &bridge,
                MCP_MQTT_SEND_TOOL,
                &json!({ "to": "mqtt:office:cmd/light", "body": "far too long" }),
            )
            .await,
            Some(ToolOutcome::Err(json!({
                "ok": false,
                "error": "MQTT publish to client `office` is not allowed for this app: \
                          add an \"mqtt_publish\" ACL matcher for that client",
            })))
        );
        assert_eq!(send_budget_row(&bridge).await, None);
    }

    #[tokio::test]
    async fn a_published_call_answers_ok_and_spends_one_budget_unit() {
        let bridge = bridge_with_bound(1024).await;
        let handle = bridge
            .mqtt_service()
            .get_client("home")
            .expect("the bridge registers a session for `home`");
        let _eventloop = brenn_mqtt::test_support::install_unpolled_client(&handle, None).await;

        assert_eq!(
            execute_mqtt_tool(
                &bridge,
                MCP_MQTT_SEND_TOOL,
                &json!({ "to": "mqtt:home:cmd/light", "body": "on", "qos": 0 }),
            )
            .await,
            Some(ToolOutcome::Ok(json!({ "ok": true })))
        );
        assert_eq!(
            send_budget_row(&bridge).await,
            Some(bridge.app_config_default_send_budget() - 1)
        );
    }

    // -----------------------------------------------------------------------
    // MQTT publish per-client ACL
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
    /// so the publish passes the ACL check and reaches `MqttService::publish`.
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

    /// Visibility is not enforcement: the per-client ACL check
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

    #[tokio::test]
    async fn execute_mqtt_answers_err_outcomes_without_a_card() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_publish_acl(&["home"], &["home"])
                .await;
        let mut rx = bridge.subscribe();

        assert_eq!(
            execute_mqtt_tool(&bridge, MCP_MQTT_SEND_TOOL, &json!({ "body": "on" })).await,
            Some(ToolOutcome::Err(json!({
                "ok": false,
                "error": "missing or empty `to` argument",
            })))
        );
        // An allowed publish reaches the (disconnected) broker layer.
        match execute_mqtt_tool(
            &bridge,
            MCP_MQTT_SEND_TOOL,
            &json!({ "to": "mqtt:home:cmd/light", "body": "on" }),
        )
        .await
        {
            Some(ToolOutcome::Err(v)) => assert!(
                v["error"].as_str().unwrap().contains("not connected"),
                "{v}"
            ),
            other => panic!("expected the broker layer's error, got {other:?}"),
        }
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
}

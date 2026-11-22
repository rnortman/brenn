//! Intercept handler for the MQTT MCP virtual tool.
//!
//! - `MqttSend` — PreToolUse Allow; PostToolUse commits a text or JSON
//!   `body`, the bytes of `body_base64`, or an attachment of a message the
//!   conversation was shown, to the MQTT client's egress channel through
//!   [`brenn_messaging::Messenger::publish_mqtt`], and answers at commit.
//!   Counts against the messaging send budget.
//!
//! `execute_mqtt_tool` is the execution, independent of the hook.

use std::borrow::Cow;
use std::num::NonZeroU32;

use serde::Serialize;

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_envelope::AttachmentName;
use brenn_lib::mqtt::address::{MqttAddress, parse_topic_name};
use brenn_lib::mqtt::{MqttLifetime, MqttPublishOptions};
use brenn_messaging::{MqttCommitted, MqttPayload, MqttRefusal, PublishOrigin, PublishResult};
use brenn_messaging_store::blob::HeldAttachments;
use brenn_mqtt::ConnectorHealthLabel;
use brenn_mqtt::payload::{OutboundArg, decode_outbound};
use brenn_obs::security::{DenialKind, SecurityEventType, signal_publish_denial};

use crate::active_bridge::{ActiveBridge, ToolOutcome};
use crate::intercept_helpers::{
    PostCard, answer_post_tool_use, outcome_err, outcome_ok, outcome_rejection,
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

/// What one `MqttSend` call publishes, once its arguments are parsed and an
/// attachment reference is admitted.
struct PreparedPublish {
    to: String,
    address: MqttAddress,
    payload: MqttPayload,
    options: MqttPublishOptions,
}

/// The answer to a committed `MqttSend`.
#[derive(Serialize)]
struct MqttSendOk {
    // alphabetical: client_connected, message_id, note, ok, remaining_budget
    client_connected: bool,
    message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
    ok: bool,
    remaining_budget: u32,
}

/// How `lifetime_s` is given, for every rejection of it.
const LIFETIME_FORMS: &str =
    "give a whole number of seconds from 1 to 4294967295, or the string \"forever\"";

/// `MqttSend`: [`prepare`], then one commit through the bus's MQTT admission.
async fn execute_send(
    bridge: &ActiveBridge,
    tool_name: &str,
    input: &serde_json::Value,
) -> ToolOutcome {
    let PreparedPublish {
        to,
        address,
        payload,
        options,
    } = match prepare(bridge, tool_name, input).await {
        Ok(prepared) => prepared,
        Err(answer) => return answer,
    };
    let messenger = messenger(bridge);
    let result = messenger
        .publish_mqtt(
            PublishOrigin::Conversation {
                id: bridge.conversation_id,
            },
            &bridge.app_slug,
            &to,
            payload,
            &options,
        )
        .await;
    match result {
        Ok(committed) => answer_committed(bridge, &address.client, committed).await,
        Err(refused) => answer_refused(bridge, tool_name, &to, &address, refused),
    }
}

/// The answer to a committed publish, with whether its client is connected
/// now. A client the registry no longer holds reads as not connected.
async fn answer_committed(
    bridge: &ActiveBridge,
    client: &str,
    committed: MqttCommitted,
) -> ToolOutcome {
    let health = match bridge.mqtt_service().get_client(client) {
        Some(handle) => Some(handle.health_snapshot().await.0),
        None => None,
    };
    let client_connected = health == Some(ConnectorHealthLabel::Connected);
    let note = (!client_connected).then(|| {
        let state = health.map_or("no longer registered", ConnectorHealthLabel::wire_str);
        format!(
            "MQTT client `{client}` is {state}; the message is committed and waits for a \
             connection within its lifetime"
        )
    });
    outcome_ok(&MqttSendOk {
        client_connected,
        message_id: committed.message_id.to_string(),
        note,
        ok: true,
        remaining_budget: committed
            .remaining_budget
            .expect("a Conversation-origin publish returns Some(remaining_budget)"),
    })
}

/// The answer to a publish the bus refused.
fn answer_refused(
    bridge: &ActiveBridge,
    tool_name: &str,
    to: &str,
    address: &MqttAddress,
    refused: PublishResult,
) -> ToolOutcome {
    let client = &address.client;
    match refused {
        PublishResult::MalformedAddress(_) => match parse_topic_name(to) {
            Err(e) => reject(tool_name, &e.to_string()),
            Ok(_) => unreachable!(
                "the bus refused {to:?} as malformed after the intercept parsed it as a topic name"
            ),
        },
        PublishResult::MissingSender => answer_acl_denied(bridge, false, client, &address.topic),
        PublishResult::AclDenied(_) => answer_acl_denied(bridge, true, client, &address.topic),
        PublishResult::UnknownChannel(_) => {
            // The client is not configured, or a reload is adding or removing
            // it at this instant. Not a security event: the app's ACL names it.
            // `client` is CC-supplied, so the log line sanitizes it.
            tracing::warn!(
                app_slug = %bridge.app_slug,
                conversation_id = bridge.conversation_id,
                client = %sanitize_untrusted_str(client, MAX_LOGGED_UNTRUSTED_BYTES),
                "MqttSend: the MQTT client is not configured on this server"
            );
            outcome_err(Cow::Owned(format!(
                "MQTT client `{client}` is not configured on this server"
            )))
        }
        PublishResult::MqttRefused(MqttRefusal::InvalidQos { .. }) => {
            reject(tool_name, "invalid `qos`: must be 0, 1, or 2")
        }
        PublishResult::MqttRefused(MqttRefusal::InvalidContentType { reason }) => reject(
            tool_name,
            &format!("invalid `content_type` argument: {reason}"),
        ),
        PublishResult::MqttRefused(MqttRefusal::PayloadShape) => reject(
            tool_name,
            "the payload must be exactly one of `body`, `body_base64` and `attachment`",
        ),
        PublishResult::MqttRefused(MqttRefusal::PayloadOverClientCap { len, cap }) => {
            messaging_intercept::refusal(
                "payload_too_large",
                format!(
                    "payload is {len} bytes; MQTT client `{client}` publishes at most {cap} \
                     (`outbound_payload_cap_bytes`)"
                ),
            )
        }
        PublishResult::MqttRefused(MqttRefusal::NeedsBlobStore { len, max_body }) => {
            messaging_intercept::refusal(
                "payload_unstorable",
                format!(
                    "payload is {len} bytes, over the bus's {max_body}-byte message body cap, \
                     and this server has no blob store to carry it"
                ),
            )
        }
        PublishResult::MqttRefused(MqttRefusal::BlobRefused { reason }) => {
            messaging_intercept::refusal(
                "payload_unstorable",
                format!("the blob store refused the payload: {reason}"),
            )
        }
        PublishResult::RateLimited => outcome_err(Cow::Borrowed(
            "rate limited: too many publishes to this MQTT client; slow down",
        )),
        PublishResult::BudgetExhausted => outcome_err(Cow::Borrowed(
            "budget exhausted: 0 remaining; ask the user to send a chat message to reset",
        )),
        e @ (PublishResult::Ok { .. }
        | PublishResult::BodyTooLarge { .. }
        | PublishResult::DeferredQuotaExceeded { .. }
        | PublishResult::ImpetusUnauthorized
        | PublishResult::MqttRefused(
            MqttRefusal::DeliverAfter | MqttRefusal::ReplyTo | MqttRefusal::DeliveryDeadline,
        )) => unreachable!(
            "MqttSend states no release time, reply_to, deadline or impetus, and an inline \
             payload over the body cap rides as a blob, so the bus cannot answer {e:?}"
        ),
    }
}

/// The bridge's messenger. The MQTT tool is only offered on a server with
/// messaging configured, so a missing one is a host wiring bug.
fn messenger(bridge: &ActiveBridge) -> &std::sync::Arc<brenn_messaging::Messenger> {
    bridge.messenger().unwrap_or_else(|| {
        panic!(
            "MqttSend intercept: Messenger required for the MQTT publish but absent on ActiveBridge"
        )
    })
}

/// Every step of `MqttSend` before the bus: the arguments and the admission
/// of an attachment reference. A refusal here returns the call's answer and
/// spends no budget.
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

    let address = parse_topic_name(&to).map_err(|e| reject(tool_name, &e.to_string()))?;

    let content_type = input.get("content_type");
    let form = match (
        input.get("body"),
        input.get("body_base64"),
        input.get("attachment"),
    ) {
        (Some(body), None, None) => PayloadForm::Inline(
            decode_outbound(OutboundArg::Body(body), content_type)
                .map_err(|e| reject(tool_name, &e.to_string()))?,
        ),
        (None, Some(b64), None) => PayloadForm::Inline(
            decode_outbound(OutboundArg::Base64(b64), content_type)
                .map_err(|e| reject(tool_name, &e.to_string()))?,
        ),
        (None, None, Some(reference)) => {
            if content_type.is_some() {
                return Err(reject(
                    tool_name,
                    "`content_type` is given with `attachment`: an attachment publishes its \
                     own recorded content type; drop `content_type`",
                ));
            }
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
        (None, None, None) => {
            return Err(reject(
                tool_name,
                "missing payload: give exactly one of `body`, `body_base64` and `attachment`",
            ));
        }
        _ => {
            return Err(reject(
                tool_name,
                "more than one payload argument is given: give exactly one of `body`, \
                 `body_base64` and `attachment`",
            ));
        }
    };

    let qos = match input.get("qos") {
        None => 1,
        Some(v) => match v.as_u64().and_then(|n| u8::try_from(n).ok()) {
            Some(qos) if qos <= 2 => qos,
            _ => return Err(reject(tool_name, "invalid `qos`: must be 0, 1, or 2")),
        },
    };
    let retain = input
        .get("retain")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let lifetime = lifetime(input).map_err(|e| reject(tool_name, &e))?;

    let messenger = messenger(bridge);
    let (payload, content_type) = match form {
        PayloadForm::Inline(outbound) => {
            (MqttPayload::Inline(outbound.bytes), outbound.content_type)
        }
        PayloadForm::Attachment { message_id, name } => {
            let hold = messaging_intercept::admit_attachment_reference(
                bridge,
                messenger,
                "`attachment`",
                message_id,
                name,
            )
            .await?;
            let content_type = hold.reference().content_type;
            let name =
                AttachmentName::new(name).expect("an admitted attachment name is well formed");
            let held =
                HeldAttachments::new([(name, hold)]).expect("one attachment is within every bound");
            (MqttPayload::Attached(held), Some(content_type))
        }
    };

    Ok(PreparedPublish {
        to,
        address,
        payload,
        options: MqttPublishOptions {
            qos,
            retain,
            lifetime,
            content_type,
        },
    })
}

/// The required `lifetime_s` argument: whole seconds from 1 to `u32::MAX`, or
/// `"forever"`. The `Err` text names both forms.
fn lifetime(input: &serde_json::Value) -> Result<MqttLifetime, String> {
    let Some(value) = input.get("lifetime_s") else {
        return Err(format!("missing `lifetime_s` argument: {LIFETIME_FORMS}"));
    };
    let lifetime = match value {
        serde_json::Value::String(s) if s == "forever" => Some(MqttLifetime::Forever),
        serde_json::Value::Number(n) => n
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .and_then(NonZeroU32::new)
            .map(MqttLifetime::Seconds),
        _ => None,
    };
    lifetime.ok_or_else(|| format!("invalid `lifetime_s` argument: {LIFETIME_FORMS}"))
}

/// The payload form of one call, as its arguments give it.
enum PayloadForm<'a> {
    Inline(brenn_mqtt::payload::OutboundPayload),
    Attachment { message_id: &'a str, name: &'a str },
}

/// The answer to an `MqttSend` the app's `mqtt_publish` grant or ACL denies,
/// with its security signal.
/// `has_grant` tells "no grant at all" (`MissingSender`) from "grant held, no
/// `acl publish` entry admits the topic" (`AclDenied`), which picks both the
/// security-signal `kind` and the operator-facing remedy string.
fn answer_acl_denied(
    bridge: &ActiveBridge,
    has_grant: bool,
    client: &str,
    topic: &str,
) -> ToolOutcome {
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
    // the logs. `client` and `topic` are CC-supplied — the helper
    // sanitizes and caps the address; the trusted `mqtt:` prefix makes
    // the `address=mqtt:<client>:<topic>` detail self-describing.
    let address = format!("mqtt:{client}:{topic}");
    signal_publish_denial(
        bridge.alert_dispatcher(),
        SecurityEventType::MqttPublishDenied,
        bridge.denial_origin(),
        kind,
        &address,
    );
    // The two remedy strings stay distinct: each reveals only the
    // app's own policy state (grant vs allowlist), and neither names a
    // filter the app holds. A topic may carry control characters, so
    // the echoed address goes through the log sanitizer too.
    let error = if has_grant {
        let shown = sanitize_untrusted_str(&address, MAX_LOGGED_UNTRUSTED_BYTES);
        Cow::Owned(format!(
            "MQTT publish to `{shown}` is not allowed for this app: no `acl publish` entry \
             admits that topic on client `{client}`; the operator adds \
             `topic_filter \"mqtt:{client}:<filter>\"` to the app's `acl publish`"
        ))
    } else {
        Cow::Borrowed(
            "MQTT publish is not enabled for this app: the operator adds an \
             `acl publish` entry `topic_filter \"mqtt:<client>:<filter>\"`, \
             with `publish` in the app's `grants`",
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
mod tests;

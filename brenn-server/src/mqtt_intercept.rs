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

use serde::Serialize;

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_envelope::AttachmentName;
use brenn_lib::mqtt::MqttPublishOptions;
use brenn_lib::mqtt::address::{MqttAddress, parse_topic_name};
use brenn_messaging::{
    MqttCommitted, MqttPayload, MqttRefusal, PublishOrigin, PublishResult, ReplyToRefusal,
    StatusToRefusal,
};
use brenn_messaging_store::blob::HeldAttachments;
use brenn_mqtt::ConnectorHealthLabel;
use brenn_obs::security::{DenialKind, SecurityEventType, signal_publish_denial};

use crate::active_bridge::{ActiveBridge, ToolOutcome};
use crate::intercept_helpers::{
    PostCard, answer_post_tool_use, outcome_err, outcome_ok, outcome_rejection,
};
use crate::messaging_intercept::{self, CHANNEL_NAME_RULE, brenn_channel_denied_msg};
use crate::mqtt_publish_args::{self, ArgPayload, MqttPublishArgs};
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
    deliver_after: Option<chrono::DateTime<chrono::Utc>>,
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
        deliver_after,
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
            deliver_after,
        )
        .await;
    match result {
        Ok(committed) => answer_committed(bridge, &address.client, committed).await,
        Err(refused) => answer_refused(bridge, REJECTION_CONTEXT, tool_name, &to, refused),
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

/// The answer to an MQTT publish to `to` the bus refused, for `MqttSend` and
/// for an automation's MQTT action alike. An argument rejection is logged
/// under `rejection_context`, the calling tool family's category. Every
/// caller has parsed `to` as a topic name before publishing.
pub(crate) fn answer_refused(
    bridge: &ActiveBridge,
    rejection_context: &str,
    tool_name: &str,
    to: &str,
    refused: PublishResult,
) -> ToolOutcome {
    let reject = |error: &str| outcome_rejection(rejection_context, tool_name, error);
    let address = match parse_topic_name(to) {
        Ok(address) => address,
        Err(e) => match refused {
            PublishResult::MalformedAddress(_) => return reject(&e.to_string()),
            other => unreachable!(
                "the bus answered {other:?} for {to:?}, which the caller parsed as a topic name                  before publishing: {e}"
            ),
        },
    };
    let client = &address.client;
    match refused {
        PublishResult::MalformedAddress(_) => unreachable!(
            "the bus refused {to:?} as malformed after the intercept parsed it as a topic name"
        ),
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
                tool = tool_name,
                "the MQTT client is not configured on this server"
            );
            outcome_err(Cow::Owned(format!(
                "MQTT client `{client}` is not configured on this server"
            )))
        }
        PublishResult::MqttRefused(MqttRefusal::StatusTo { address, why }) => {
            answer_status_to_refused(bridge, rejection_context, tool_name, &address, why)
        }
        PublishResult::MqttRefused(MqttRefusal::ReplyTo { address, why }) => {
            answer_reply_to_refused(bridge, rejection_context, tool_name, &address, client, why)
        }
        PublishResult::MqttRefused(MqttRefusal::InvalidQos { .. }) => {
            reject("invalid `qos`: must be 0, 1, or 2")
        }
        PublishResult::MqttRefused(MqttRefusal::InvalidContentType { reason }) => {
            reject(&format!("invalid `content_type` argument: {reason}"))
        }
        PublishResult::MqttRefused(MqttRefusal::PayloadShape) => {
            reject("the payload must be exactly one of `body`, `body_base64` and `attachment`")
        }
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
        PublishResult::DeferredQuotaExceeded { cap } => outcome_err(Cow::Owned(format!(
            "deferred-message quota exceeded: MQTT client `{client}` already holds its cap of \
             {cap} scheduled messages; retry after some are released, or cancel one"
        ))),
        e @ (PublishResult::Ok { .. }
        | PublishResult::BodyTooLarge { .. }
        | PublishResult::ImpetusUnauthorized
        | PublishResult::MqttRefused(MqttRefusal::DeliveryDeadline)) => unreachable!(
            "MqttSend states no deadline or impetus, and an inline payload over the body cap \
             rides as a blob, so the bus cannot answer {e:?}"
        ),
    }
}

/// The answer to a `status_to` the MQTT admission refused. A refusal on the
/// app's `brenn:` publish authority raises the security signal a `BrennSend`
/// denial raises; "not in the ACL" and "does not exist" answer with one
/// string, so the answer is no existence oracle.
fn answer_status_to_refused(
    bridge: &ActiveBridge,
    rejection_context: &str,
    tool_name: &str,
    address: &str,
    why: StatusToRefusal,
) -> ToolOutcome {
    let signalled = match why {
        StatusToRefusal::GrantAbsent => Some(DenialKind::GrantAbsent),
        StatusToRefusal::AclDenied => Some(DenialKind::AclDenied),
        StatusToRefusal::Unknown => Some(DenialKind::UnknownChannel),
        StatusToRefusal::Malformed | StatusToRefusal::StatusOverBodyCap { .. } => None,
    };
    if let Some(kind) = signalled {
        signal_publish_denial(
            bridge.alert_dispatcher(),
            SecurityEventType::BrennPublishDenied,
            bridge.denial_origin(),
            kind,
            address,
        );
    }
    match why {
        StatusToRefusal::Malformed => {
            let shown = sanitize_untrusted_str(address, MAX_LOGGED_UNTRUSTED_BYTES);
            outcome_rejection(
                rejection_context,
                tool_name,
                &format!(
                    "malformed `status_to` {shown:?}: must be of the form \"brenn:<name>\" with \
                     {CHANNEL_NAME_RULE}"
                ),
            )
        }
        StatusToRefusal::GrantAbsent => outcome_err(Cow::Borrowed(
            "`status_to` is refused: this app has no `publish` grant for `brenn:` channels",
        )),
        StatusToRefusal::AclDenied | StatusToRefusal::Unknown => outcome_err(Cow::Owned(format!(
            "`status_to`: {}",
            brenn_channel_denied_msg(address)
        ))),
        StatusToRefusal::StatusOverBodyCap { len, max } => outcome_rejection(
            rejection_context,
            tool_name,
            &format!(
                "the topic is too long for a delivery status about it to be published (a status \
                 can be {len} bytes, over the bus's {max}-byte body cap): drop `status_to` or \
                 shorten the topic"
            ),
        ),
    }
}

/// The answer to a `reply_to` the MQTT admission refused. A reply topic the
/// app's MQTT publish ACL does not cover raises the signal a denied target
/// raises; a malformed address or one on another client is an argument
/// rejection and signals nothing.
fn answer_reply_to_refused(
    bridge: &ActiveBridge,
    rejection_context: &str,
    tool_name: &str,
    address: &str,
    client: &str,
    why: ReplyToRefusal,
) -> ToolOutcome {
    let shown = sanitize_untrusted_str(address, MAX_LOGGED_UNTRUSTED_BYTES);
    match why {
        ReplyToRefusal::Malformed { detail } => outcome_rejection(
            rejection_context,
            tool_name,
            &format!(
                "invalid `reply_to` {shown:?}: {detail}; give a concrete topic as \
                 `mqtt:<client>:<topic>`"
            ),
        ),
        ReplyToRefusal::OtherClient => outcome_rejection(
            rejection_context,
            tool_name,
            &format!(
                "`reply_to` must name a topic on the client the message is published through \
                 (`{client}`)"
            ),
        ),
        ReplyToRefusal::AclDenied => {
            signal_publish_denial(
                bridge.alert_dispatcher(),
                SecurityEventType::MqttPublishDenied,
                bridge.denial_origin(),
                DenialKind::AclDenied,
                address,
            );
            outcome_err(Cow::Owned(format!(
                "`reply_to` {shown:?} is not allowed for this app: no `acl publish` entry admits \
                 that topic on client `{client}`; a reply topic must be one this app may publish \
                 to"
            )))
        }
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
    let to = mqtt_publish_args::target(input).map_err(|e| reject(tool_name, &e))?;

    // Cross-protocol misuse guards.
    if brenn_lib::messaging::ChannelScheme::of(to)
        == Some(brenn_lib::messaging::ChannelScheme::Brenn)
    {
        return Err(reject(
            tool_name,
            "MqttSend only accepts `mqtt:` addresses. \
                 Use BrennSend for `brenn:` addresses.",
        ));
    }
    if brenn_lib::messaging::ChannelScheme::of(to)
        == Some(brenn_lib::messaging::ChannelScheme::PwaPush)
    {
        return Err(reject(
            tool_name,
            "MqttSend only accepts `mqtt:` addresses. \
                 Use PwaPushSend for `pwa_push:` addresses.",
        ));
    }
    if brenn_lib::messaging::ChannelScheme::of(to)
        == Some(brenn_lib::messaging::ChannelScheme::Webhook)
    {
        return Err(reject(
            tool_name,
            "MqttSend only accepts `mqtt:` addresses. \
                 `webhook:` channels are inbound-only in this version.",
        ));
    }

    let MqttPublishArgs {
        to,
        address,
        payload,
        qos,
        retain,
        lifetime,
        status_to,
        reply_to,
        deliver_after,
    } = mqtt_publish_args::parse(input).map_err(|e| reject(tool_name, &e))?;

    let messenger = messenger(bridge);
    let (payload, content_type) = match payload {
        ArgPayload::Inline(outbound) => {
            (MqttPayload::Inline(outbound.bytes), outbound.content_type)
        }
        ArgPayload::Attachment { message_id, name } => {
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
        to: to.to_string(),
        address,
        payload,
        options: MqttPublishOptions {
            qos,
            retain,
            lifetime,
            content_type,
            status_to,
            reply_to,
        },
        deliver_after,
    })
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
    outcome_rejection(REJECTION_CONTEXT, tool_name, error)
}

/// The log category of an `MqttSend` argument rejection.
const REJECTION_CONTEXT: &str = "mqtt tool";

#[cfg(test)]
mod attachment_tests;

#[cfg(test)]
mod tests;

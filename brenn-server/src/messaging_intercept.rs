//! Intercept handlers for the three messaging MCP virtual tools.
//!
//! All three tools are auto-approved:
//! - PreToolUse intercepted, returns `Allow`.
//! - PostToolUse intercepted, executes the real work, returns
//!   `Continue { updated_output }` so CC sees the real result instead
//!   of `__NOOP__`.
//! - `emit_tool_summary` is invoked from PostToolUse so the chat-history
//!   card renders.
//!
//! The active bridge owns the conversation context (sender_conversation_id,
//! sender_app_slug); these flow into `Messenger::publish` from the
//! intercept handlers.

use std::borrow::Cow;

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_common::{MAX_LOGGED_UNTRUSTED_BYTES, sanitize_untrusted_str};
use brenn_lib::messaging::{
    AnyPublishResult, CancelResult, ChannelListing, EditFields, EditResult, EphemeralPublishResult,
    MessageEnvelope, MessageQuery, PublishOrigin, PublishResult, QueryError, SubscriptionListing,
    Urgency,
};
use brenn_lib::obs::security::{DenialKind, SecurityEventType, signal_publish_denial};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::active_bridge::ActiveBridge;
use crate::intercept_helpers::{ToolErr, reject_tool, warn_if_unexpected_tool_response};
use crate::tools::messaging::{
    MCP_MESSAGE_CANCEL_TOOL, MCP_MESSAGE_EDIT_TOOL, MCP_MESSAGE_LIST_CHANNELS_TOOL,
    MCP_MESSAGE_PENDING_LIST_TOOL, MCP_MESSAGE_QUERY_CHANNEL_TOOL, MCP_MESSAGE_SEND_TOOL,
    MCP_MESSAGE_SUBSCRIBE_TOOL, MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL, MCP_MESSAGE_UNSUBSCRIBE_TOOL,
};

// ---------------------------------------------------------------------------
// Typed tool-response structs (C2)
// ---------------------------------------------------------------------------

/// Response for `MessageChannelList`.
///
/// Field order: channels — matches source `json!({ "channels": listing })`.
#[derive(Serialize)]
struct MessageChannelListResponse<'a> {
    channels: &'a [ChannelListing],
}

/// Response for `MessageSubscriptionList`.
///
/// Field order: subscriptions — matches the `{ subscriptions: [...] }` wrapper
/// (design §2.1), analogous to `MessageChannelListResponse`.
#[derive(Serialize)]
struct MessageSubscriptionListResponse<'a> {
    subscriptions: &'a [SubscriptionListing],
}

/// Success response for `BrennSend`.
///
/// Fields alphabetical: address, message_id, ok, remaining_budget.
/// `message_id` is `String` (from `Uuid::to_string()`); `address` borrowed from
/// `PublishResult::Ok { address, .. }`. `remaining_budget` is `u32` (matches upstream).
#[derive(Serialize)]
struct BrennSendOk<'a> {
    // alphabetical: address, message_id, ok, remaining_budget
    address: &'a str,
    message_id: String,
    ok: bool,
    remaining_budget: u32,
}

/// Success response for `BrennSend` to an `ephemeral:` channel.
///
/// Mirrors `BrennSendOk` minus `remaining_budget` — ephemeral channels carry no
/// per-app send budget. Fields alphabetical: address, message_id, ok.
#[derive(Serialize)]
struct EphemeralSendOk<'a> {
    address: &'a str,
    message_id: String,
    ok: bool,
}

/// Success response for `BrennMessageCancel`.
///
/// Fields alphabetical: cancelled, cancelled_pushes, message_id, ok.
/// `message_id` is `String` (from `Uuid::to_string()`). `cancelled_pushes` is `u32`.
#[derive(Serialize)]
struct BrennMessageCancelOk {
    // alphabetical: cancelled, cancelled_pushes, message_id, ok
    cancelled: bool,
    cancelled_pushes: u32,
    message_id: String,
    ok: bool,
}

/// Response for `BrennPendingList`.
///
/// Single field: messages.
#[derive(Serialize)]
struct BrennPendingListResponse<'a> {
    messages: &'a [MessageEnvelope],
}

/// Success response for `MessageSubscribe`.
///
/// Fields alphabetical: address, ok, status. `status` is the activation status
/// string (`"subscribed"`, `"subscribed_pending_reconnect"`, or
/// `"already_subscribed"`) so the LLM gets an honest live-vs-deferred-vs-noop
/// signal (design §3 / §2.4).
#[derive(Serialize)]
struct MessageSubscribeOk<'a> {
    // alphabetical: address, ok, status
    address: &'a str,
    ok: bool,
    status: &'static str,
}

/// Success response for `MessageUnsubscribe`.
///
/// Fields alphabetical: address, ok, status. `status` is the deactivation status
/// string (`"unsubscribed"`, `"unsubscribed_others_remain"`, or
/// `"unsubscribed_pending_reconnect"`) so the LLM gets an honest signal about
/// whether the broker subscription was actually torn down (design §3 / §2.4).
#[derive(Serialize)]
struct MessageUnsubscribeOk<'a> {
    // alphabetical: address, ok, status
    address: &'a str,
    ok: bool,
    status: &'static str,
}

/// Outcome of `try_handle_messaging_tool`. `None` means "not a messaging
/// tool"; the caller falls through.
#[derive(Debug)]
pub enum MessagingHandled {
    /// Send this decision back to CC.
    Respond(CcApprovalDecision),
}

/// Try to handle a messaging tool intercept. Returns `Some` only when the
/// tool name matches one of the three messaging tools.
///
/// The caller is `handle_brenn_tools` in `active_bridge.rs`. `emit_tool_summary`
/// is invoked here for PostToolUse intercepts so the chat-history card
/// fires alongside the substituted tool result.
pub async fn try_handle_messaging_tool(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<MessagingHandled> {
    match &req.kind {
        // ---- PreToolUse: all messaging tools auto-approve ----
        ApprovalKind::PreToolUse { tool_name, .. }
            if tool_name == MCP_MESSAGE_LIST_CHANNELS_TOOL
                || tool_name == MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL
                || tool_name == MCP_MESSAGE_SEND_TOOL
                || tool_name == MCP_MESSAGE_QUERY_CHANNEL_TOOL
                || tool_name == MCP_MESSAGE_SUBSCRIBE_TOOL
                || tool_name == MCP_MESSAGE_UNSUBSCRIBE_TOOL
                || tool_name == MCP_MESSAGE_PENDING_LIST_TOOL
                || tool_name == MCP_MESSAGE_CANCEL_TOOL
                || tool_name == MCP_MESSAGE_EDIT_TOOL =>
        {
            Some(MessagingHandled::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // ---- PostToolUse: MessageChannelList ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MESSAGE_LIST_CHANNELS_TOOL => {
            warn_if_unexpected_tool_response("messaging intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            let app_slug = &bridge.app_slug;
            // Start with the app's ACL-scoped brenn:/webhook: channels + mqtt:
            // pattern rows (design §2.2 — "what could THIS app subscribe to?", not
            // the old system-wide dump). Empty when messaging is not configured:
            // this is intentional — pwa_push-only deployments have no Messenger,
            // and the listing should still succeed (showing only pwa_push entries).
            // This is not a silent fallback: the LLM receives a real result that
            // reflects the actual configuration. Contrast with BrennSend, which
            // cannot proceed without a Messenger and returns an explicit error.
            let mut listing = match bridge.messenger() {
                Some(m) => m.list_accessible_channels(app_slug),
                None => vec![],
            };
            // Append pwa_push targets (already app-scoped; concrete existing
            // targets, hence AccessKind::Existing).
            if let Some(pwa_push_svc) = bridge.pwa_push_service() {
                let push_targets = pwa_push_svc.list_targets(app_slug).await;
                for target in push_targets {
                    listing.push(brenn_lib::messaging::ChannelListing {
                        protocol: brenn_lib::messaging::ChannelScheme::PwaPush,
                        address: target.address,
                        description: None,
                        access: brenn_lib::messaging::AccessKind::Existing,
                        details: Some(brenn_lib::messaging::ChannelDetails::PwaPush(
                            brenn_lib::messaging::PwaPushDetails {
                                user: target.user,
                                device: target.device,
                                last_seen_at: target.last_seen_at,
                            },
                        )),
                    });
                }
            } else if bridge.messenger().is_none() {
                // Both messenger and pwa_push_service are absent. The listing
                // will be empty, which looks like a valid "no channels" state
                // to the LLM. Log so an operator can distinguish a
                // misconfiguration from a correctly-empty deployment.
                tracing::warn!(
                    app_slug = %app_slug,
                    "MessageChannelList: neither Messenger nor PwaPushService configured; \
                     returning empty channel listing"
                );
            }
            // webhook: channels are now persisted in the directory and emitted
            // by list_channels() above — no runtime synthesis needed here.

            // Enrich mqtt: entries with runtime ingress health (design §2.5).
            // `list_channels()` emits MqttDetails with client/topic and the
            // health fields left `None` (Messenger has no MQTT dependency); fill
            // qos/health/last_error from MqttService here, exactly as the
            // pwa_push: targets are appended above. When mqtt_service() is None
            // (no MQTT runtime), the fields stay absent — an honest "MQTT runtime
            // not present" state; the channel still lists with client/topic.
            if let Some(mqtt_svc) = bridge.mqtt_service() {
                enrich_mqtt_listing(&mut listing, mqtt_svc).await;
            }

            let resp = MessageChannelListResponse { channels: &listing };
            let output_str = serde_json::to_string(&resp)
                .expect("MessageChannelListResponse serialization is infallible");
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, false,
            )
            .await;
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: MessageSubscriptionList ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL => {
            warn_if_unexpected_tool_response("messaging intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            let app_slug = &bridge.app_slug;
            // App-scoped: only THIS app's own subscriptions. Empty when messaging
            // is not configured — a pwa_push-only deployment has no Messenger, but
            // the listing must still succeed (showing only pwa_push targets, which
            // ARE subscriptions this app holds). Same honest-empty contract as the
            // MessageChannelList arm: a real result reflecting the actual config,
            // not a silent fallback.
            let mut listing = match bridge.messenger() {
                Some(m) => m.list_subscriptions(app_slug).await,
                None => vec![],
            };
            // Append pwa_push targets: a PWA-push registration IS a subscription
            // this app holds, and `list_targets(app_slug)` is already app-scoped.
            // These are always concrete registrations (no static-vs-dynamic
            // distinction), reported with `dynamic = false`.
            if let Some(pwa_push_svc) = bridge.pwa_push_service() {
                let push_targets = pwa_push_svc.list_targets(app_slug).await;
                for target in push_targets {
                    listing.push(SubscriptionListing {
                        protocol: brenn_lib::messaging::ChannelScheme::PwaPush,
                        address: target.address,
                        description: None,
                        dynamic: false,
                        push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                        retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                        noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                        wake_min: brenn_lib::messaging::WakeMin::Normal,
                        details: Some(brenn_lib::messaging::ChannelDetails::PwaPush(
                            brenn_lib::messaging::PwaPushDetails {
                                user: target.user,
                                device: target.device,
                                last_seen_at: target.last_seen_at,
                            },
                        )),
                    });
                }
            } else if bridge.messenger().is_none() {
                // Both messenger and pwa_push_service absent: the listing is empty,
                // which looks like a valid "no subscriptions" state to the LLM. Log
                // so an operator can distinguish a misconfiguration from a
                // correctly-empty deployment (parallels the MessageChannelList arm).
                tracing::warn!(
                    app_slug = %app_slug,
                    "MessageSubscriptionList: neither Messenger nor PwaPushService \
                     configured; returning empty subscription listing"
                );
            }

            // Enrich mqtt: entries with runtime ingress health, exactly as the
            // MessageChannelList arm does. `list_subscriptions` leaves the health
            // fields `None`; fill qos/health/last_error from MqttService here.
            if let Some(mqtt_svc) = bridge.mqtt_service() {
                // Reuse the shared per-entry primitive directly (reuse-1): the
                // SubscriptionListing rows carry the same `Option<ChannelDetails>`
                // as ChannelListing, so no per-type wrapper is needed.
                for entry in listing.iter_mut() {
                    enrich_mqtt_details(&mut entry.details, mqtt_svc).await;
                }
            }

            let resp = MessageSubscriptionListResponse {
                subscriptions: &listing,
            };
            let output_str = serde_json::to_string(&resp)
                .expect("MessageSubscriptionListResponse serialization is infallible");
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, false,
            )
            .await;
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: BrennSend ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MESSAGE_SEND_TOOL => {
            warn_if_unexpected_tool_response("messaging intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            // Extract `to` first so cross-protocol errors can fire before the
            // messenger check (cross-protocol misuse doesn't require a messenger).
            let to = match tool_input.get("to").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s,
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
            // Cross-protocol misuse: pwa_push: addresses must go to PwaPushSend.
            if brenn_lib::messaging::ChannelScheme::of(to)
                == Some(brenn_lib::messaging::ChannelScheme::PwaPush)
            {
                tracing::debug!(
                    tool = tool_name,
                    to,
                    "BrennSend called with pwa_push: address; redirecting LLM to PwaPushSend"
                );
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "BrennSend only accepts `brenn:` and `ephemeral:` addresses. \
                         Use PwaPushSend for `pwa_push:` addresses. \
                         Use MessageChannelList to discover available channels.",
                    )
                    .await,
                );
            }
            // Cross-protocol misuse: mqtt: addresses must go to MqttSend.
            if brenn_lib::messaging::ChannelScheme::of(to)
                == Some(brenn_lib::messaging::ChannelScheme::Mqtt)
            {
                tracing::debug!(
                    tool = tool_name,
                    to,
                    "BrennSend called with mqtt: address; redirecting LLM to MqttSend"
                );
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "BrennSend only accepts `brenn:` and `ephemeral:` addresses. \
                         Use MqttSend for `mqtt:` addresses. \
                         Use MessageChannelList to discover available channels.",
                    )
                    .await,
                );
            }
            // Cross-protocol misuse: webhook: addresses are inbound-only (no outbound send tool in MVP).
            if brenn_lib::messaging::ChannelScheme::of(to)
                == Some(brenn_lib::messaging::ChannelScheme::Webhook)
            {
                tracing::debug!(
                    tool = tool_name,
                    to,
                    "BrennSend called with webhook: address; webhook: is inbound-only in MVP"
                );
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "BrennSend only accepts `brenn:` and `ephemeral:` addresses. \
                         `webhook:` channels are inbound-only in this version. \
                         Use MessageChannelList to discover available channels.",
                    )
                    .await,
                );
            }
            let messenger = match bridge.messenger() {
                Some(m) => m,
                None => {
                    tracing::warn!(
                        tool = tool_name,
                        "BrennSend: no Messenger configured; returning error to LLM"
                    );
                    return Some(missing_messenger_response());
                }
            };
            let body = match tool_input.get("body").and_then(|v| v.as_str()) {
                Some(s) => s,
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
            // Reject legacy `wake` key — reject-and-teach so a stale-habit LLM
            // isn't silently downgraded to the `urgency` default (§2.4).
            if tool_input.get("wake").is_some() {
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "unknown field `wake`; use `urgency` (\"very-low\", \"low\", \"normal\", \"high\")",
                    )
                    .await,
                );
            }
            let urgency_str = tool_input
                .get("urgency")
                .and_then(|v| v.as_str())
                .unwrap_or("low");
            let urgency = match Urgency::parse(urgency_str) {
                Some(u) => u,
                None => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            &format!(
                                "unknown `urgency` value: {urgency_str:?}; \
                                 must be one of \"very-low\", \"low\", \"normal\", \"high\""
                            ),
                        )
                        .await,
                    );
                }
            };
            let reply_to = tool_input
                .get("reply_to")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let deliver_after = match parse_optional_rfc3339(tool_input.get("deliver_after")) {
                Ok(v) => v,
                Err(e) => {
                    return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                }
            };
            let delivery_deadline =
                match parse_optional_rfc3339(tool_input.get("delivery_deadline")) {
                    Ok(v) => v,
                    Err(e) => {
                        return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                    }
                };

            let result = messenger
                .publish_any(
                    PublishOrigin::Conversation {
                        id: bridge.conversation_id,
                    },
                    &bridge.app_slug,
                    to,
                    body,
                    urgency,
                    reply_to.as_deref(),
                    deliver_after,
                    delivery_deadline,
                )
                .await;

            // Shared error mapper: every failure arm produces the same
            // `ToolErr { ok: false, error } -> (Value, is_error=true)` shape, so it
            // lives in one place and cannot drift arm-to-arm.
            let mkerr = |msg: Cow<'static, str>| {
                (
                    serde_json::to_value(&ToolErr {
                        ok: false,
                        error: msg,
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                )
            };
            // `missing_sender` and `body_too_large` share identical wording across
            // the durable and ephemeral arms; build each once so the two cannot
            // drift.
            let missing_sender_msg =
                || Cow::Borrowed("messaging not configured for this app (sender missing)");
            let body_too_large_msg = |len: usize, max: usize| {
                Cow::Owned(format!("body too large: {len} bytes (max {max})"))
            };
            // One byte-identical message for both ephemeral UnknownChannel and
            // AclDenied — true in both arms, so the LLM-visible string reveals
            // nothing about which gate fired; the real `kind` lives only in the
            // server-side security log. The app corrects a typo'd address from its
            // own operator-configured allowlist, not from a listing tool (there is
            // no self-service discovery surface for ephemeral publish targets).
            let ephemeral_channel_denied_msg = |addr: &str| {
                Cow::Owned(format!(
                    "channel {addr:?} does not exist or is not in this app's \
                     ephemeral_publish allowlist"
                ))
            };
            let (outcome_value, is_error) = match result {
                AnyPublishResult::Durable(durable) => {
                    // One security signal per denial, mirroring the ephemeral arm:
                    // the log `kind` is derived from the result enum, address-bearing
                    // arms echo their carried address, and the rest fall back to the
                    // original target `to`. `Ok`/`BudgetExhausted` return no
                    // `signal_kind`, so they emit no signal.
                    if let Some(kind) = durable.signal_kind() {
                        signal_publish_denial(
                            bridge.alert_dispatcher(),
                            SecurityEventType::BrennPublishDenied,
                            bridge.denial_origin(),
                            kind,
                            durable.denied_address().unwrap_or(to),
                        );
                    }
                    match durable {
                        PublishResult::Ok {
                            message_id,
                            address,
                            remaining_budget,
                        } => {
                            let ok = BrennSendOk {
                                address: &address,
                                message_id: message_id.to_string(),
                                ok: true,
                                remaining_budget: remaining_budget.expect(
                                    "Conversation-origin publish returns Some(remaining_budget)",
                                ),
                            };
                            (
                                serde_json::to_value(&ok)
                                    .expect("BrennSendOk serialization is infallible"),
                                false,
                            )
                        }
                        PublishResult::BudgetExhausted => mkerr(Cow::Borrowed(
                            "budget exhausted: 0 remaining; ask the user to send a chat message to reset",
                        )),
                        PublishResult::UnknownChannel(addr) => {
                            mkerr(Cow::Owned(brenn_channel_denied_msg(&addr)))
                        }
                        PublishResult::MalformedAddress(addr) => mkerr(Cow::Owned(format!(
                            "malformed channel address {addr:?}: must be of the form \
                             \"brenn:<name>\" (or \"ephemeral:<name>\") with name \
                             matching ^[A-Za-z0-9._~-]+$"
                        ))),
                        PublishResult::MissingSender => mkerr(missing_sender_msg()),
                        PublishResult::AclDenied(addr) => {
                            mkerr(Cow::Owned(brenn_channel_denied_msg(&addr)))
                        }
                        PublishResult::BodyTooLarge { len, max } => {
                            mkerr(body_too_large_msg(len, max))
                        }
                    }
                }
                AnyPublishResult::Ephemeral(ephemeral) => {
                    // One security signal per denial, with the log `kind` field
                    // derived from the result enum so it stays in lockstep with
                    // the bus `publish_denied` counter key. Address-bearing arms
                    // echo their carried address; the rest fall back to the
                    // original target `to`. `RateLimited` (its own bus counter +
                    // warn) and `UnsupportedOption` (pure LLM input error) return
                    // no `signal_kind`, so they emit no signal here.
                    if let Some(kind) = ephemeral.signal_kind() {
                        signal_publish_denial(
                            bridge.alert_dispatcher(),
                            SecurityEventType::EphemeralPublishDenied,
                            bridge.denial_origin(),
                            kind,
                            ephemeral.denied_address().unwrap_or(to),
                        );
                    }
                    match ephemeral {
                        EphemeralPublishResult::Ok {
                            message_id,
                            address,
                            ..
                        } => {
                            let ok = EphemeralSendOk {
                                address: &address,
                                message_id: message_id.to_string(),
                                ok: true,
                            };
                            (
                                serde_json::to_value(&ok)
                                    .expect("EphemeralSendOk serialization is infallible"),
                                false,
                            )
                        }
                        EphemeralPublishResult::UnknownChannel(addr) => {
                            mkerr(ephemeral_channel_denied_msg(&addr))
                        }
                        EphemeralPublishResult::MalformedAddress(addr) => {
                            mkerr(Cow::Owned(format!(
                                "malformed channel address {addr:?}: must be of the form \
                                 \"ephemeral:<name>\" with name matching ^[A-Za-z0-9._~-]+$"
                            )))
                        }
                        EphemeralPublishResult::MissingSender => mkerr(missing_sender_msg()),
                        EphemeralPublishResult::AclDenied(addr) => {
                            mkerr(ephemeral_channel_denied_msg(&addr))
                        }
                        EphemeralPublishResult::RateLimited => mkerr(Cow::Borrowed(
                            "rate limited: too many ephemeral publishes; slow down",
                        )),
                        EphemeralPublishResult::BodyTooLarge { len, max } => {
                            mkerr(body_too_large_msg(len, max))
                        }
                        EphemeralPublishResult::UnsupportedOption { field } => mkerr(Cow::Owned(
                            format!("`{field}` is not supported on `ephemeral:` channels"),
                        )),
                    }
                }
            };
            let output_str = serde_json::to_string(&outcome_value)
                .expect("outcome_value serialization is infallible");

            // Plumb the publish outcome into `format_summary` via a
            // synthetic field on a cloned `tool_input`. Design §8.1
            // specifies a status badge ("delivered", "budget exhausted:
            // 0 remaining", etc.) on the sent-message card; the badge
            // text comes from this `_outcome` object.
            let mut enriched = tool_input.clone();
            if let serde_json::Value::Object(map) = &mut enriched {
                map.insert("_outcome".to_string(), outcome_value);
            }
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, &enriched, is_error,
            )
            .await;

            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: MessageChannelGet ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MESSAGE_QUERY_CHANNEL_TOOL => {
            warn_if_unexpected_tool_response("messaging intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            // Extract `address` first so cross-protocol errors fire before
            // the messenger check.
            let channel = match tool_input.get("address").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "missing or empty `address` argument",
                        )
                        .await,
                    );
                }
            };
            // Cross-protocol misuse: pwa_push: addresses must go to PwaPushChannelGet.
            if brenn_lib::messaging::ChannelScheme::of(&channel)
                == Some(brenn_lib::messaging::ChannelScheme::PwaPush)
            {
                tracing::debug!(
                    tool = tool_name,
                    address = %channel,
                    "MessageChannelGet called with pwa_push: address; redirecting LLM"
                );
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "MessageChannelGet does not accept `pwa_push:` addresses. \
                         Use PwaPushChannelGet for `pwa_push:` addresses. \
                         Use MessageChannelList to discover available channels.",
                    )
                    .await,
                );
            }
            let messenger = match bridge.messenger() {
                Some(m) => m,
                None => {
                    tracing::warn!(
                        tool = tool_name,
                        "MessageChannelGet: no Messenger configured; returning error to LLM"
                    );
                    return Some(missing_messenger_response());
                }
            };
            let limit = match tool_input.get("limit").and_then(|v| v.as_u64()) {
                Some(n) if (1..=500).contains(&n) => n as u32,
                _ => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "`limit` is required and must be between 1 and 500",
                        )
                        .await,
                    );
                }
            };
            let before = match parse_optional_rfc3339(tool_input.get("before")) {
                Ok(v) => v,
                Err(e) => {
                    return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                }
            };
            let after = match parse_optional_rfc3339(tool_input.get("after")) {
                Ok(v) => v,
                Err(e) => {
                    return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                }
            };
            let sender = tool_input
                .get("sender")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let search = tool_input
                .get("search")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let q = MessageQuery {
                channel,
                limit,
                before,
                after,
                sender,
                search,
                calling_app_slug: bridge.app_slug.clone(),
            };
            let outcome = messenger.query(&q).await;
            let (output_str, is_error) = match outcome {
                Ok(envelopes) => (
                    serde_json::to_string(&envelopes).expect("MessageEnvelope serialize"),
                    false,
                ),
                Err(QueryError::UnknownChannel(addr)) => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("unknown channel address {addr:?}")),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
                Err(QueryError::Fts(msg)) => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("FTS query failed: {msg}")),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
            };
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, is_error,
            )
            .await;
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: MessageSubscribe ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MESSAGE_SUBSCRIBE_TOOL => {
            warn_if_unexpected_tool_response("messaging intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            Some(handle_message_subscribe(bridge, tool_name, tool_input).await)
        }

        // ---- PostToolUse: MessageUnsubscribe ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MESSAGE_UNSUBSCRIBE_TOOL => {
            warn_if_unexpected_tool_response("messaging intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            Some(handle_message_unsubscribe(bridge, tool_name, tool_input).await)
        }

        // ---- PostToolUse: BrennPendingList ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MESSAGE_PENDING_LIST_TOOL => {
            warn_if_unexpected_tool_response("messaging intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            let messenger = match bridge.messenger() {
                Some(m) => m,
                None => {
                    tracing::warn!(
                        tool = tool_name,
                        "BrennPendingList: no Messenger configured; returning error to LLM"
                    );
                    return Some(missing_messenger_response());
                }
            };
            // Optional channel filter. Unknown / malformed addresses → empty result
            // per §2.11 (no tool error; log malformed for operator visibility).
            // Use `is_well_formed_address` (the canonical check) rather than a
            // weaker prefix+length check so all malformed shapes are logged.
            let channel_str = tool_input.get("channel").and_then(|v| v.as_str());
            if let Some(s) = channel_str
                && !brenn_lib::messaging::is_well_formed_address(s)
            {
                tracing::warn!(
                    tool = tool_name,
                    channel = %sanitize_untrusted_str(s, MAX_LOGGED_UNTRUSTED_BYTES),
                    "BrennPendingList: malformed channel address; returning empty list"
                );
                let empty: &[MessageEnvelope] = &[];
                let resp = BrennPendingListResponse { messages: empty };
                let output_str = serde_json::to_string(&resp)
                    .expect("BrennPendingListResponse serialization is infallible");
                crate::active_bridge::emit_tool_summary_for_intercept(
                    bridge, tool_name, tool_input, false,
                )
                .await;
                return Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                    updated_output: Some(output_str),
                }));
            }
            let envelopes = messenger.list_pending(&bridge.app_slug, channel_str).await;
            let resp = BrennPendingListResponse {
                messages: &envelopes,
            };
            let output_str = serde_json::to_string(&resp)
                .expect("BrennPendingListResponse serialization is infallible");
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, false,
            )
            .await;
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: BrennMessageCancel ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MESSAGE_CANCEL_TOOL => {
            warn_if_unexpected_tool_response("messaging intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            let message_uuid = match parse_message_uuid(bridge, tool_name, tool_input).await {
                Ok(u) => u,
                Err(e) => return Some(e),
            };
            let messenger = match bridge.messenger() {
                Some(m) => m,
                None => {
                    tracing::warn!(
                        tool = tool_name,
                        "BrennMessageCancel: no Messenger configured; returning error to LLM"
                    );
                    return Some(missing_messenger_response());
                }
            };
            let result = messenger.cancel(&bridge.app_slug, message_uuid).await;
            let (output_str, is_error) = match result {
                CancelResult::Ok {
                    message_id,
                    cancelled_pushes,
                } => {
                    let ok = BrennMessageCancelOk {
                        cancelled: true,
                        cancelled_pushes,
                        message_id: message_id.to_string(),
                        ok: true,
                    };
                    (
                        serde_json::to_string(&ok)
                            .expect("BrennMessageCancelOk serialization is infallible"),
                        false,
                    )
                }
                CancelResult::UnknownMessage => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("unknown message {message_uuid}")),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CancelResult::NotAuthorized => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "not authorized: caller's sender does not match this message's sender",
                        ),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CancelResult::AlreadyDelivered => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "all pending pushes for this message have already been delivered",
                        ),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CancelResult::NoPendingPushes => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "no pending pushes for this message (already cancelled or zero-target broadcast)",
                        ),
                    };
                    (
                        serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                CancelResult::MissingSender => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "messaging not configured for this app (sender missing)",
                        ),
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
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: BrennMessageEdit ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_MESSAGE_EDIT_TOOL => {
            warn_if_unexpected_tool_response("messaging intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
            let message_uuid = match parse_message_uuid(bridge, tool_name, tool_input).await {
                Ok(u) => u,
                Err(e) => return Some(e),
            };
            // Parse body — must be string if present.
            let body = match tool_input.get("body") {
                None => None,
                Some(serde_json::Value::String(s)) => Some(s.clone()),
                Some(_) => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "`body` must be a string",
                        )
                        .await,
                    );
                }
            };
            // Parse deliver_after: distinguish missing from explicit null.
            let obj = tool_input.as_object();
            let deliver_after = if let Some(obj) = obj
                && obj.contains_key("deliver_after")
            {
                // Key present — use parse_present_rfc3339 so the type enforces
                // that we've already confirmed the key exists.
                let v = obj
                    .get("deliver_after")
                    .expect("key just confirmed present");
                match parse_present_rfc3339(v) {
                    Ok(ts) => Some(ts), // Some(None) = clear; Some(Some(t)) = set
                    Err(e) => {
                        return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                    }
                }
            } else {
                None // field absent = leave unchanged
            };
            let delivery_deadline = if let Some(obj) = obj
                && obj.contains_key("delivery_deadline")
            {
                let v = obj
                    .get("delivery_deadline")
                    .expect("key just confirmed present");
                match parse_present_rfc3339(v) {
                    Ok(ts) => Some(ts),
                    Err(e) => {
                        return Some(tool_error_response(bridge, tool_name, tool_input, &e).await);
                    }
                }
            } else {
                None
            };
            // Reject legacy `wake` key — reject-and-teach so a stale-habit LLM
            // isn't silently downgraded to the `urgency` default (§2.4).
            if tool_input.get("wake").is_some() {
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "unknown field `wake`; use `urgency` (\"very-low\", \"low\", \"normal\", \"high\")",
                    )
                    .await,
                );
            }
            // Parse urgency — must be a ladder string if present.
            let urgency = match tool_input.get("urgency").and_then(|v| v.as_str()) {
                None => None,
                Some(s) => match Urgency::parse(s) {
                    Some(u) => Some(u),
                    None => {
                        return Some(
                            tool_error_response(
                                bridge,
                                tool_name,
                                tool_input,
                                &format!(
                                    "unknown `urgency` value: {s:?}; \
                                     must be one of \"very-low\", \"low\", \"normal\", \"high\""
                                ),
                            )
                            .await,
                        );
                    }
                },
            };
            // Parse reply_to: distinguish missing, null (clear), string (set).
            let reply_to = if obj.map(|o| o.contains_key("reply_to")).unwrap_or(false) {
                match tool_input.get("reply_to") {
                    Some(serde_json::Value::Null) => Some(None), // clear
                    Some(serde_json::Value::String(s)) => Some(Some(s.clone())), // set
                    None => unreachable!("contains_key returned true but get returned None"), // key present check above
                    Some(_) => {
                        return Some(
                            tool_error_response(
                                bridge,
                                tool_name,
                                tool_input,
                                "`reply_to` must be a string or null",
                            )
                            .await,
                        );
                    }
                }
            } else {
                None // absent = leave unchanged
            };
            // At least one mutable field required (intercept-side enforcement per §2.12).
            if body.is_none()
                && deliver_after.is_none()
                && delivery_deadline.is_none()
                && urgency.is_none()
                && reply_to.is_none()
            {
                let err = ToolErr {
                    ok: false,
                    error: Cow::Borrowed(
                        "no fields provided: at least one of body, deliver_after, \
                         delivery_deadline, urgency, reply_to must be specified",
                    ),
                };
                let output_str =
                    serde_json::to_string(&err).expect("ToolErr serialization is infallible");
                crate::active_bridge::emit_tool_summary_for_intercept(
                    bridge, tool_name, tool_input, true,
                )
                .await;
                return Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                    updated_output: Some(output_str),
                }));
            }
            let messenger = match bridge.messenger() {
                Some(m) => m,
                None => {
                    tracing::warn!(
                        tool = tool_name,
                        "BrennMessageEdit: no Messenger configured; returning error to LLM"
                    );
                    return Some(missing_messenger_response());
                }
            };
            let fields = EditFields {
                body,
                reply_to,
                deliver_after,
                delivery_deadline,
                urgency,
            };
            let result = messenger.edit(&bridge.app_slug, message_uuid, fields).await;
            // Close the reply_to existence oracle the same way the BrennSend
            // publish arm does: UnknownChannel and AclDenied render one
            // byte-identical `brenn_channel_denied_msg` so the LLM-visible text
            // never reveals whether a `brenn:` channel exists, and each reply_to
            // denial is recorded through the shared security-signal helper (the
            // existence bit lives only in the server-side security log's `kind`).
            if let Some((kind, addr)) = match &result {
                EditResult::MalformedAddress(a) => Some((DenialKind::MalformedAddress, a.as_str())),
                EditResult::UnknownChannel(a) => Some((DenialKind::UnknownChannel, a.as_str())),
                EditResult::AclDenied(a) => Some((DenialKind::AclDenied, a.as_str())),
                _ => None,
            } {
                signal_publish_denial(
                    bridge.alert_dispatcher(),
                    SecurityEventType::BrennPublishDenied,
                    bridge.denial_origin(),
                    kind,
                    addr,
                );
            }
            // Ok arm requires a serde_json::Value to inject "ok": true into the
            // envelope object; it serializes to String at the end. Error arms
            // serialize to String directly (no intermediate Value needed).
            let (output_str, is_error) = match result {
                EditResult::Ok { envelope } => {
                    let mut outcome_value =
                        serde_json::to_value(&envelope).expect("MessageEnvelope serialize");
                    // Add ok: true for consistency with BrennMessageCancel (correctness-2).
                    if let serde_json::Value::Object(ref mut map) = outcome_value {
                        map.insert("ok".to_string(), serde_json::Value::Bool(true));
                    }
                    (outcome_value.to_string(), false)
                }
                EditResult::UnknownMessage => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("unknown message {message_uuid}")),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
                EditResult::NotAuthorized => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "not authorized: caller's sender does not match this message's sender",
                        ),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
                EditResult::AlreadyDelivered => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "at least one push for this message has already been delivered; edit not allowed",
                        ),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
                EditResult::NoPendingPushes => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "no pending pushes for this message (already cancelled or zero-target broadcast)",
                        ),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
                EditResult::NoFieldsProvided => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "no fields provided: at least one of body, deliver_after, \
                             delivery_deadline, urgency, reply_to must be specified",
                        ),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
                EditResult::BodyTooLarge { len, max } => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("body too large: {len} bytes (max {max})")),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
                EditResult::UnknownChannel(addr) | EditResult::AclDenied(addr) => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Owned(brenn_channel_denied_msg(&addr)),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
                EditResult::MalformedAddress(addr) => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Owned(format!(
                            "malformed reply_to address {addr:?}: must be of the form \
                             \"brenn:<name>\" with name matching ^[A-Za-z0-9._~-]+$"
                        )),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
                EditResult::MissingSender => (
                    serde_json::to_string(&ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "messaging not configured for this app (sender missing)",
                        ),
                    })
                    .expect("ToolErr serialization is infallible"),
                    true,
                ),
            };
            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, is_error,
            )
            .await;
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        _ => None,
    }
}

/// Parse an RFC3339 timestamp from a present JSON value (key was already
/// confirmed to exist via `contains_key`). Accepts null or empty-string as
/// `Ok(None)` ("clear the field"). Returns a descriptive error on type
/// mismatch or malformed timestamp so the LLM can retry.
///
/// Use this instead of `parse_optional_rfc3339` at call sites that need the
/// absent/"leave alone" vs. present/"clear or set" distinction: the non-`Option`
/// parameter makes key-presence enforcement by the type rather than a comment.
fn parse_present_rfc3339(v: &serde_json::Value) -> Result<Option<DateTime<Utc>>, String> {
    match v {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) if s.is_empty() => Ok(None),
        serde_json::Value::String(s) => {
            let parsed = DateTime::parse_from_rfc3339(s)
                .map_err(|e| format!("invalid RFC3339 timestamp {s:?}: {e}"))?;
            Ok(Some(parsed.with_timezone(&Utc)))
        }
        other => Err(format!(
            "expected RFC3339 string for timestamp field, got JSON {}",
            json_variant_name(other),
        )),
    }
}

/// Parse an optional RFC3339 timestamp argument from tool input.
/// Returns `Ok(None)` for missing / null / empty-string fields. Returns
/// a descriptive error (with the JSON variant name on type mismatch,
/// review N2) for the LLM to retry against.
///
/// Prefer `parse_present_rfc3339` at call sites that have already done a
/// `contains_key` guard: it takes `&serde_json::Value` directly and makes
/// the key-presence contract explicit in the type.
fn parse_optional_rfc3339(v: Option<&serde_json::Value>) -> Result<Option<DateTime<Utc>>, String> {
    match v {
        None => Ok(None),
        Some(v) => parse_present_rfc3339(v),
    }
}

fn json_variant_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Parse `message_id` from `tool_input`. Returns `Ok(Uuid)` or `Err` with the
/// `MessagingHandled` error response already built (caller returns `Some(err)`).
async fn parse_message_uuid(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
) -> Result<uuid::Uuid, MessagingHandled> {
    let id_str = match tool_input.get("message_id").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => {
            return Err(tool_error_response(
                bridge,
                tool_name,
                tool_input,
                "missing `message_id` argument",
            )
            .await);
        }
    };
    match uuid::Uuid::parse_str(id_str) {
        Ok(u) => Ok(u),
        Err(_) => Err(tool_error_response(
            bridge,
            tool_name,
            tool_input,
            &format!("invalid UUID: {id_str:?}"),
        )
        .await),
    }
}

/// Fill the runtime ingress-health fields on each `mqtt:` entry of a channel
/// listing from `MqttService` (design §2.5 health enrichment).
///
/// `list_channels()` emits `MqttDetails` with `client`/`topic` set and the
/// runtime fields `None` (the messaging core has no MQTT dependency). This fills:
/// - `qos` — the broker SUBSCRIBE QoS the client's live ingress holds for this
///   exact topic filter (`None` if the filter is not currently subscribed).
/// - `health` — the stringified ingress connection-health label.
/// - `last_error` — the ingress connection error, if any.
///
/// `urgency` is intentionally left `None`: it lives in `[[mqtt_client]]` config
/// and the message-injection path, not in any `MqttService` ingress structure, so
/// surfacing it would require adding net-new ingress state (deferred with the
/// egress observability rework, design §7). Non-`mqtt:` entries are untouched.
async fn enrich_mqtt_listing(
    listing: &mut [ChannelListing],
    mqtt_svc: &brenn_lib::mqtt::MqttService,
) {
    for entry in listing.iter_mut() {
        enrich_mqtt_details(&mut entry.details, mqtt_svc).await;
    }
}

/// Fill the runtime ingress-health fields (`qos`/`health`/`last_error`) on a
/// single `mqtt:` `ChannelDetails`, leaving non-mqtt details untouched. Shared
/// by both `MessageChannelList` and `MessageSubscriptionList` enrichment so the
/// two tools report identical health labels for the same channel.
async fn enrich_mqtt_details(
    details: &mut Option<brenn_lib::messaging::ChannelDetails>,
    mqtt_svc: &brenn_lib::mqtt::MqttService,
) {
    use brenn_lib::messaging::ChannelDetails;
    let Some(ChannelDetails::Mqtt(details)) = details.as_mut() else {
        return;
    };
    // Resolve the session handle once per entry and read both the per-filter
    // QoS and the per-client health off it (efficiency-2), instead of two
    // separate `get_client` resolutions.
    let (qos, label, last_error) = mqtt_svc
        .ingress_filter_status(&details.client, &details.topic)
        .await;
    details.qos = qos;
    details.health = Some(label.wire_str().to_string());
    details.last_error = last_error;
}

/// Parse a `Depth` from a `MessageSubscribe` JSON field: a non-negative integer
/// → `Bounded(n)`, the string `"unbounded"` → `Unbounded`. Returns `Err` with a
/// tool-facing message for any other shape (missing, negative, wrong type).
///
/// The value-shape decode delegates to `Depth`'s own `Deserialize` (the
/// `DepthVisitor`, the single source of truth for the two wire shapes) rather than
/// re-implementing it (reuse-2). The required-field contract is preserved here: a
/// missing/`null` field yields a field-named "required" error (the intercept tests
/// assert the error names the field), and a present-but-malformed value yields a
/// field-named error so the LLM knows which argument was wrong.
fn parse_depth_field(
    value: Option<&serde_json::Value>,
    field: &str,
) -> Result<brenn_lib::messaging::config::Depth, String> {
    use brenn_lib::messaging::config::Depth;
    match value {
        None | Some(serde_json::Value::Null) => Err(format!(
            "`{field}` is required and must be a non-negative integer or \"unbounded\""
        )),
        Some(v) => serde_json::from_value::<Depth>(v.clone())
            .map_err(|_| format!("`{field}` must be a non-negative integer or \"unbounded\"")),
    }
}

/// Handle the `MessageSubscribe` PostToolUse intercept: parse + validate input,
/// call the runtime subscribe-activation wrapper, and map the typed outcome /
/// error to a tool-facing JSON result (design §2.4). Owner = `bridge.app_slug`.
async fn handle_message_subscribe(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
) -> MessagingHandled {
    use brenn_lib::messaging::subscribe::DynamicSubscribeParams;

    // `address` (required, non-empty).
    let address = match tool_input.get("address").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return tool_error_response(
                bridge,
                tool_name,
                tool_input,
                "missing or empty `address` argument",
            )
            .await;
        }
    };

    // `push_depth` / `retain_depth` (both required, no defaults — design §7 A).
    let push_depth = match parse_depth_field(tool_input.get("push_depth"), "push_depth") {
        Ok(d) => d,
        Err(e) => return tool_error_response(bridge, tool_name, tool_input, &e).await,
    };
    let retain_depth = match parse_depth_field(tool_input.get("retain_depth"), "retain_depth") {
        Ok(d) => d,
        Err(e) => return tool_error_response(bridge, tool_name, tool_input, &e).await,
    };

    // `noise` / `wake_min` (optional; inherit on omission). A present-but-invalid
    // value is a caller mistake → error (don't silently ignore).
    let noise = match tool_input.get("noise") {
        None => None,
        Some(serde_json::Value::Null) => None,
        Some(v) => match v
            .as_str()
            .and_then(brenn_lib::messaging::config::NoiseLevel::parse)
        {
            Some(n) => Some(n),
            None => {
                return tool_error_response(
                    bridge,
                    tool_name,
                    tool_input,
                    "`noise` must be one of \"silent\", \"metered\", \"alarm\"",
                )
                .await;
            }
        },
    };
    let wake_min = match tool_input.get("wake_min") {
        None => None,
        Some(serde_json::Value::Null) => None,
        Some(v) => match v.as_str().and_then(brenn_lib::messaging::WakeMin::parse) {
            Some(w) => Some(w),
            None => {
                return tool_error_response(
                    bridge,
                    tool_name,
                    tool_input,
                    "`wake_min` must be one of \"very-low\", \"low\", \"normal\", \"high\", \"never\"",
                )
                .await;
            }
        },
    };

    // `qos` (optional; MQTT-only — the wrapper/core rejects it on non-mqtt). A
    // present value must be 0/1/2.
    let qos = match tool_input.get("qos") {
        None => None,
        Some(serde_json::Value::Null) => None,
        Some(v) => match v.as_u64() {
            Some(n @ 0..=2) => Some(n as u8),
            _ => {
                return tool_error_response(
                    bridge,
                    tool_name,
                    tool_input,
                    "`qos` must be 0, 1, or 2",
                )
                .await;
            }
        },
    };

    let params = DynamicSubscribeParams {
        push_depth,
        retain_depth,
        noise,
        wake_min,
        qos,
    };

    let app_slug = bridge.app_slug.clone();
    let outcome =
        crate::mqtt_subscribe::subscribe_dynamic_activated(bridge, &app_slug, &address, params)
            .await;

    use crate::mqtt_subscribe::SubscribeActivation;
    let (output_str, is_error) = match outcome {
        Ok(activation) => {
            // A send failure still leaves a durable subscription + route; the
            // reconnect re-assert retries. The status string (pure mapping)
            // reports it as pending rather than a hard error so the LLM knows the
            // subscription exists; the warn is logged here at the call site.
            if let SubscribeActivation::MqttSendFailed(ref e) = activation {
                tracing::warn!(
                    tool = tool_name,
                    address = %sanitize_untrusted_str(&address, MAX_LOGGED_UNTRUSTED_BYTES),
                    error = %e,
                    "MessageSubscribe: broker SUBSCRIBE send failed; subscription durable, \
                     reconnect will retry"
                );
            }
            let status = activation.status_str();
            let resp = MessageSubscribeOk {
                address: &address,
                ok: true,
                status,
            };
            (
                serde_json::to_string(&resp)
                    .expect("MessageSubscribeOk serialization is infallible"),
                false,
            )
        }
        Err(e) => {
            // An app exceeding its grant is worth surfacing (CC anomaly =
            // log-and-surface), but NOT fail2ban signal: the request came from this
            // app's own LLM, not a network attacker (§3.4 / CLAUDE.md logging
            // posture). Two over-grant shapes warn here: a policy-denied subscribe,
            // and a retain_depth exceeding the channel's standing window (an app
            // requesting a read window beyond the operator's baseline). Other
            // subscribe errors (bad address, dormant row, resolver invariants) are
            // plain tool errors and do not warn.
            use crate::mqtt_subscribe::SubscribeActivateError;
            use brenn_lib::messaging::subscribe::RuntimeSubscribeError;
            let denial_reason = match &e {
                SubscribeActivateError::PolicyDenied { .. } => Some("access policy"),
                SubscribeActivateError::Core(
                    RuntimeSubscribeError::RetainDepthExceedsStanding { .. },
                ) => Some("retain depth exceeds standing"),
                _ => None,
            };
            if let Some(reason) = denial_reason {
                tracing::warn!(
                    tool = tool_name,
                    app = %app_slug,
                    address = %sanitize_untrusted_str(&address, MAX_LOGGED_UNTRUSTED_BYTES),
                    reason,
                    "MessageSubscribe denied — app exceeding its grant"
                );
            }
            let payload = ToolErr {
                ok: false,
                error: Cow::Owned(e.to_string()),
            };
            (
                serde_json::to_string(&payload).expect("ToolErr serialization is infallible"),
                true,
            )
        }
    };

    crate::active_bridge::emit_tool_summary_for_intercept(bridge, tool_name, tool_input, is_error)
        .await;
    MessagingHandled::Respond(CcApprovalDecision::Continue {
        updated_output: Some(output_str),
    })
}

/// Handle the `MessageUnsubscribe` PostToolUse intercept: parse + validate input,
/// call the runtime unsubscribe-activation wrapper, and map the typed outcome /
/// error to a tool-facing JSON result (design §2.4). Owner = `bridge.app_slug`.
async fn handle_message_unsubscribe(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
) -> MessagingHandled {
    // `address` (required, non-empty).
    let address = match tool_input.get("address").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => {
            return tool_error_response(
                bridge,
                tool_name,
                tool_input,
                "missing or empty `address` argument",
            )
            .await;
        }
    };

    let app_slug = bridge.app_slug.clone();
    let outcome =
        crate::mqtt_subscribe::unsubscribe_dynamic_activated(bridge, &app_slug, &address).await;

    use crate::mqtt_subscribe::UnsubscribeActivation;
    let (output_str, is_error) = match outcome {
        Ok(activation) => {
            // The caller's durable sub + directory subscriber are already gone and
            // the filter was dropped from the reconnect set even on a send failure;
            // only the live broker UNSUBSCRIBE *send* failed. The status string
            // (pure mapping) reports the removal as done; the warn is logged here.
            if let UnsubscribeActivation::MqttSendFailed(ref e) = activation {
                tracing::warn!(
                    tool = tool_name,
                    address = %sanitize_untrusted_str(&address, MAX_LOGGED_UNTRUSTED_BYTES),
                    error = %e,
                    "MessageUnsubscribe: broker UNSUBSCRIBE send failed; subscription already \
                     removed, reconnect will not re-subscribe"
                );
            }
            let status = activation.status_str();
            let resp = MessageUnsubscribeOk {
                address: &address,
                ok: true,
                status,
            };
            (
                serde_json::to_string(&resp)
                    .expect("MessageUnsubscribeOk serialization is infallible"),
                false,
            )
        }
        Err(e) => {
            let payload = ToolErr {
                ok: false,
                error: Cow::Owned(e.to_string()),
            };
            (
                serde_json::to_string(&payload).expect("ToolErr serialization is infallible"),
                true,
            )
        }
    };

    crate::active_bridge::emit_tool_summary_for_intercept(bridge, tool_name, tool_input, is_error)
        .await;
    MessagingHandled::Respond(CcApprovalDecision::Continue {
        updated_output: Some(output_str),
    })
}

async fn tool_error_response(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
    error: &str,
) -> MessagingHandled {
    MessagingHandled::Respond(
        reject_tool(bridge, "messaging tool", tool_name, tool_input, error).await,
    )
}

fn missing_messenger_response() -> MessagingHandled {
    // Server constructed without a Messenger (no channels configured).
    // Surface as a tool-result error rather than panic — the LLM can
    // gracefully back off.
    let err = ToolErr {
        ok: false,
        error: Cow::Borrowed("messaging is not configured on this brenn server"),
    };
    MessagingHandled::Respond(CcApprovalDecision::Continue {
        updated_output: Some(
            serde_json::to_string(&err).expect("ToolErr serialization is infallible"),
        ),
    })
}

/// The one LLM-visible reject for a durable (`brenn:`) publish/reply_to address
/// denial. Byte-identical across the `UnknownChannel` and `AclDenied` arms (and
/// those an out-of-visibility `reply_to` produces) so the string never reveals
/// whether a `brenn:` channel exists — the existence bit lives only in the
/// server-side security log's `kind` field. Single-sourced so the publish and
/// edit arms cannot drift and reopen the oracle.
pub(crate) fn brenn_channel_denied_msg(addr: &str) -> String {
    format!("channel {addr:?} does not exist or is not in this app's brenn_publish allowlist")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::active_bridge::test_support::{post_tool_use_req, pre_tool_use_req};
    use crate::intercept_helpers::is_noop_tool_response;

    /// The `format!("Security: {event_type}")` title `signal_publish_denial`
    /// derives for `EphemeralPublishDenied`. Pinned here to lock the exact
    /// alert-title bytes the ephemeral scheme produces.
    const EPHEMERAL_DENIAL_ALERT_TITLE: &str = "Security: ephemeral_publish_denied";

    // -----------------------------------------------------------------------
    // Ephemeral denial signalling
    // -----------------------------------------------------------------------

    /// Drive a `BrennSend` to `to` on the given bridge and return the tool-error
    /// string (panics if the outcome was not an error response).
    async fn ephemeral_send_error(bridge: &ActiveBridge, to: &str) -> String {
        let req = post_tool_use_req(MCP_MESSAGE_SEND_TOOL, json!({ "to": to, "body": "hi" }));
        match try_handle_messaging_tool(bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected error: {out}");
                v["error"].as_str().unwrap().to_string()
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn ephemeral_unknown_and_acl_denied_error_strings_identical() {
        // `locked` resolves but is outside the allowlist → AclDenied; `no-such`
        // does not resolve → UnknownChannel. Both must render the same
        // gate-independent template so neither can be used as an existence oracle.
        let messenger = ephemeral_intercept_messenger();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let acl_err = ephemeral_send_error(&bridge, "ephemeral:locked").await;
        let unknown_err = ephemeral_send_error(&bridge, "ephemeral:no-such").await;
        let expect_denied = |addr: &str| {
            format!(
                "channel {addr:?} does not exist or is not in this app's \
                 ephemeral_publish allowlist"
            )
        };
        assert_eq!(
            acl_err,
            expect_denied("ephemeral:locked"),
            "acl arm wording"
        );
        assert_eq!(
            unknown_err,
            expect_denied("ephemeral:no-such"),
            "unknown arm wording"
        );
        // The two differ only by the echoed address, never by which gate fired.
        assert_eq!(
            acl_err.replacen("ephemeral:locked", "ADDR", 1),
            unknown_err.replacen("ephemeral:no-such", "ADDR", 1),
            "oracle open: gate-dependent wording"
        );
    }

    #[tokio::test]
    async fn ephemeral_missing_sender_signals_event() {
        // No EphemeralPublish grant → MissingSender at the layer-1 gate; the
        // intercept must still emit the app security event + alert.
        let messenger = ephemeral_intercept_messenger_cfg(false, 65536);
        let (dispatcher, captured, _handle) = brenn_lib::obs::alerting::make_capturing_alerter();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger_and_dispatcher(
            messenger, dispatcher,
        )
        .await;
        let _ = ephemeral_send_error(&bridge, "ephemeral:protobar").await;
        bridge.alert_dispatcher().flush().await;
        let alerts = captured.lock().unwrap();
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, EPHEMERAL_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=missing_sender"), "body: {body}");
    }

    /// Drive a denial-producing `BrennSend` to `to` on a capturing bridge and
    /// return the captured `(title, body)` alerts. Exercises the full
    /// `try_handle_messaging_tool` path so a wiring regression at any ephemeral
    /// denial arm (a missing signal call, a mislabeled `kind`) is caught, not
    /// just the helper's own logic.
    async fn capture_denial_alerts(
        messenger: std::sync::Arc<brenn_lib::messaging::Messenger>,
        to: &str,
    ) -> Vec<(String, String)> {
        let (dispatcher, captured, _handle) = brenn_lib::obs::alerting::make_capturing_alerter();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger_and_dispatcher(
            messenger, dispatcher,
        )
        .await;
        let _ = ephemeral_send_error(&bridge, to).await;
        bridge.alert_dispatcher().flush().await;
        let alerts = captured.lock().unwrap();
        alerts.clone()
    }

    /// Each of the four bus-produced denial kinds, driven end-to-end, emits one
    /// alert whose body carries the matching `kind=` tag — pinning that every
    /// production match arm invokes the signal with the correct kind.
    #[tokio::test]
    async fn ephemeral_unknown_channel_signals_event() {
        let alerts =
            capture_denial_alerts(ephemeral_intercept_messenger(), "ephemeral:no-such").await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, EPHEMERAL_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=unknown_channel"), "body: {body}");
    }

    #[tokio::test]
    async fn ephemeral_acl_denied_signals_event() {
        let alerts =
            capture_denial_alerts(ephemeral_intercept_messenger(), "ephemeral:locked").await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, EPHEMERAL_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=acl_denied"), "body: {body}");
    }

    #[tokio::test]
    async fn ephemeral_malformed_address_signals_event() {
        let alerts =
            capture_denial_alerts(ephemeral_intercept_messenger(), "ephemeral:bad name").await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, EPHEMERAL_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=malformed_address"), "body: {body}");
    }

    #[tokio::test]
    async fn ephemeral_body_too_large_signals_event() {
        // `max_body_bytes = 1` makes the fixed 2-byte "hi" body oversize.
        let alerts = capture_denial_alerts(
            ephemeral_intercept_messenger_cfg(true, 1),
            "ephemeral:protobar",
        )
        .await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, EPHEMERAL_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=body_too_large"), "body: {body}");
    }

    // -----------------------------------------------------------------------
    // Durable denial signalling + oracle closure
    // -----------------------------------------------------------------------

    /// The `format!("Security: {event_type}")` title `signal_publish_denial`
    /// derives for `BrennPublishDenied`.
    const BRENN_DENIAL_ALERT_TITLE: &str = "Security: brenn_publish_denied";

    /// A no-subscriber durable `brenn:` channel entry — enough for the directory
    /// to resolve the target; every durable denial arm returns before subscribers
    /// are consulted.
    fn durable_channel(name: &str) -> brenn_lib::messaging::ChannelEntry {
        brenn_lib::messaging::testutils::test_channel_entry(name, vec![])
    }

    /// A `Messenger` whose directory holds `brenn:known` (publishable by
    /// `testapp`) and `brenn:locked` (resolvable but outside `testapp`'s
    /// `brenn_publish` ACL). When `grant` is set, `testapp` holds
    /// `MessagingPublish` scoped to `known` plus a universal `brenn_subscribe`
    /// matcher (so any `reply_to` is in visibility and an unresolved one is
    /// `UnknownChannel`, not the reply_to gate's `AclDenied`); when unset,
    /// `testapp` holds only `MessagingSubscribe`, so a publish is `MissingSender`.
    fn durable_intercept_messenger_cfg(
        grant: bool,
    ) -> std::sync::Arc<brenn_lib::messaging::Messenger> {
        use std::sync::Arc;

        let db = brenn_lib::db::init_db_memory();
        let dir = brenn_lib::messaging::MessagingDirectory::with_entries(vec![
            durable_channel("known"),
            durable_channel("locked"),
        ]);
        let mut apps = indexmap::IndexMap::new();
        let mut testapp_cfg =
            crate::test_support::app_config::default_test_app_config("testapp", "testapp");
        testapp_cfg.policy = brenn_lib::access::AppPolicy::default();
        if grant {
            testapp_cfg
                .policy
                .grants
                .insert(brenn_lib::access::AppCapability::MessagingPublish);
            testapp_cfg.policy.acls.brenn_publish.push(
                brenn_lib::access::acl::ChannelMatcher::Exact("known".to_string()),
            );
        }
        // A universal `brenn_subscribe` matcher keeps every `reply_to` in
        // visibility, so an unresolved one surfaces as `UnknownChannel` rather
        // than the reply_to gate's `AclDenied`.
        testapp_cfg
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::MessagingSubscribe);
        testapp_cfg
            .policy
            .acls
            .brenn_subscribe
            .push(brenn_lib::access::acl::ChannelMatcher::Prefix(String::new()));
        apps.insert("testapp".to_string(), testapp_cfg);
        brenn_lib::messaging::Messenger::new(
            db,
            Arc::new(dir),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            brenn_lib::messaging::MessagingGlobalConfig::default(),
        )
    }

    fn durable_intercept_messenger() -> std::sync::Arc<brenn_lib::messaging::Messenger> {
        durable_intercept_messenger_cfg(true)
    }

    /// Drive a `BrennSend` (optional `reply_to`) to `to` on the bridge and return
    /// the tool-error string (panics if the outcome was not an error response).
    async fn brenn_send_error(bridge: &ActiveBridge, to: &str, reply_to: Option<&str>) -> String {
        let input = match reply_to {
            Some(rt) => json!({ "to": to, "body": "hi", "reply_to": rt }),
            None => json!({ "to": to, "body": "hi" }),
        };
        let req = post_tool_use_req(MCP_MESSAGE_SEND_TOOL, input);
        match try_handle_messaging_tool(bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected error: {out}");
                v["error"].as_str().unwrap().to_string()
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// The durable UnknownChannel and AclDenied arms — including an
    /// UnknownChannel produced via `reply_to` — must render one byte-identical
    /// gate-blind template, so neither can be used as an existence oracle over
    /// the operator's `brenn:` namespace.
    #[tokio::test]
    async fn durable_unknown_and_acl_denied_error_strings_identical() {
        let messenger = durable_intercept_messenger();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        // `locked` resolves but is outside the publish ACL → AclDenied.
        let acl_err = brenn_send_error(&bridge, "brenn:locked", None).await;
        // `no-such` does not resolve → UnknownChannel (via the `to` path).
        let unknown_err = brenn_send_error(&bridge, "brenn:no-such", None).await;
        // A reply_to that is in visibility but unresolved → UnknownChannel (via
        // the reply_to path), with `to` a publishable channel.
        let reply_unknown_err =
            brenn_send_error(&bridge, "brenn:known", Some("brenn:reply-missing")).await;

        let expect_denied = |addr: &str| {
            format!(
                "channel {addr:?} does not exist or is not in this app's \
                 brenn_publish allowlist"
            )
        };
        assert_eq!(acl_err, expect_denied("brenn:locked"), "acl arm wording");
        assert_eq!(
            unknown_err,
            expect_denied("brenn:no-such"),
            "unknown arm wording"
        );
        assert_eq!(
            reply_unknown_err,
            expect_denied("brenn:reply-missing"),
            "reply_to unknown arm wording"
        );
        // The arms differ only by the echoed address, never by which gate fired.
        assert_eq!(
            acl_err.replacen("brenn:locked", "ADDR", 1),
            unknown_err.replacen("brenn:no-such", "ADDR", 1),
            "oracle open: gate-dependent wording"
        );
    }

    #[tokio::test]
    async fn durable_acl_denied_signals_event() {
        let alerts = capture_denial_alerts(durable_intercept_messenger(), "brenn:locked").await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, BRENN_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=acl_denied"), "body: {body}");
    }

    #[tokio::test]
    async fn durable_unknown_channel_signals_event() {
        let alerts = capture_denial_alerts(durable_intercept_messenger(), "brenn:no-such").await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, BRENN_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=unknown_channel"), "body: {body}");
    }

    #[tokio::test]
    async fn durable_malformed_address_signals_event() {
        let alerts = capture_denial_alerts(durable_intercept_messenger(), "brenn:bad name").await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, BRENN_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=malformed_address"), "body: {body}");
    }

    #[tokio::test]
    async fn durable_missing_sender_signals_event() {
        // `testapp` holds no `MessagingPublish` grant → MissingSender at layer-1;
        // the intercept must still emit the app security event + alert.
        let alerts =
            capture_denial_alerts(durable_intercept_messenger_cfg(false), "brenn:known").await;
        assert_eq!(alerts.len(), 1, "expected one alert: {alerts:?}");
        let (title, body) = &alerts[0];
        assert_eq!(title, BRENN_DENIAL_ALERT_TITLE);
        assert!(body.contains("kind=missing_sender"), "body: {body}");
    }

    // -----------------------------------------------------------------------
    // Cross-protocol error tests
    // -----------------------------------------------------------------------

    /// BrennSend with a pwa_push: address → is_error, mentions PwaPushSend and
    /// MessageChannelList. The check fires before the messenger lookup, so the
    /// no-messenger test bridge is sufficient.
    #[tokio::test]
    async fn brenn_send_with_pwa_push_address_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "pwa_push:alice", "body": "hello" }),
        );
        let result = try_handle_messaging_tool(&bridge, &req).await;
        match result {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("PwaPushSend"),
                    "error should mention PwaPushSend: {err}"
                );
                assert!(
                    err.contains("MessageChannelList"),
                    "error should mention MessageChannelList: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// test-7: BrennSend with an mqtt: address → is_error, mentions MqttSend (cross-protocol guard).
    #[tokio::test]
    async fn brenn_send_mqtt_address_redirects_to_mqtt_send() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "mqtt:ha:home/cmnd/tasmota/power", "body": "on" }),
        );
        let result = try_handle_messaging_tool(&bridge, &req).await;
        match result {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("MqttSend"),
                    "error should mention MqttSend: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// MessageChannelGet with a pwa_push: address → is_error, mentions
    /// PwaPushChannelGet and MessageChannelList.
    #[tokio::test]
    async fn message_channel_get_with_pwa_push_address_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_QUERY_CHANNEL_TOOL,
            json!({ "address": "pwa_push:alice", "limit": 10 }),
        );
        let result = try_handle_messaging_tool(&bridge, &req).await;
        match result {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("PwaPushChannelGet"),
                    "error should mention PwaPushChannelGet: {err}"
                );
                assert!(
                    err.contains("MessageChannelList"),
                    "error should mention MessageChannelList: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // MessageChannelList merge path (AC3)
    // -----------------------------------------------------------------------

    /// `MessageChannelList` PostToolUse with both a Messenger and a PwaPushService
    /// populates a combined listing containing at least one `brenn:` entry and at
    /// least one `pwa_push:` entry, each tagged with the correct `protocol` value.
    #[tokio::test]
    async fn message_channel_list_returns_combined_brenn_and_pwa_push_entries() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(MCP_MESSAGE_LIST_CHANNELS_TOOL, json!({}));
        let result = try_handle_messaging_tool(&bridge, &req).await;
        match result {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                let channels = v["channels"].as_array().expect("channels array");
                // At least one brenn: entry.
                let brenn_entries: Vec<_> = channels
                    .iter()
                    .filter(|e| e["protocol"].as_str() == Some("brenn"))
                    .collect();
                assert!(
                    !brenn_entries.is_empty(),
                    "expected at least one brenn: entry, got: {channels:?}"
                );
                let first_brenn = brenn_entries[0];
                assert!(
                    brenn_lib::messaging::ChannelScheme::of(
                        first_brenn["address"].as_str().unwrap()
                    ) == Some(brenn_lib::messaging::ChannelScheme::Brenn),
                    "brenn address should start with brenn:: {:?}",
                    first_brenn["address"]
                );
                assert!(
                    first_brenn["details"]["subscribers"].is_array(),
                    "brenn entry should have details.subscribers array: {:?}",
                    first_brenn
                );
                // At least one pwa_push: entry.
                let pwa_entries: Vec<_> = channels
                    .iter()
                    .filter(|e| e["protocol"].as_str() == Some("pwa_push"))
                    .collect();
                assert!(
                    !pwa_entries.is_empty(),
                    "expected at least one pwa_push: entry, got: {channels:?}"
                );
                let first_pwa = pwa_entries[0];
                let pwa_addr = first_pwa["address"].as_str().unwrap();
                assert!(
                    brenn_lib::messaging::ChannelScheme::of(pwa_addr)
                        == Some(brenn_lib::messaging::ChannelScheme::PwaPush),
                    "pwa_push address should start with pwa_push:: {pwa_addr:?}"
                );
                // Parse the address to verify it uses the canonical `@` delimiter
                // (not `:`) for device addresses, catching any format mismatch
                // between the address grammar and tool descriptions.
                brenn_lib::pwa_push::targets::parse_pwa_push_address(pwa_addr).unwrap_or_else(
                    |e| panic!("pwa_push address {pwa_addr:?} failed to parse: {e}"),
                );
                assert!(
                    !first_pwa["details"]["user"]
                        .as_str()
                        .unwrap_or("")
                        .is_empty(),
                    "pwa_push entry should have non-empty details.user: {:?}",
                    first_pwa
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `MessageChannelList` with an MqttService present enriches the `mqtt:`
    /// entry's `MqttDetails`: `protocol: mqtt`, `client`/`topic` from the address,
    /// `qos`/`health` filled from the service. `urgency` stays absent (not
    /// reachable from the ingress registry; design §2.5 deviation).
    #[tokio::test]
    async fn message_channel_list_enriches_mqtt_entry_with_ingress_health() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_mqtt_ingress_listing().await;
        let req = post_tool_use_req(MCP_MESSAGE_LIST_CHANNELS_TOOL, json!({}));
        let result = try_handle_messaging_tool(&bridge, &req).await;
        let Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
            updated_output: Some(out),
        })) = result
        else {
            panic!("unexpected result: {result:?}");
        };
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let channels = v["channels"].as_array().expect("channels array");
        let mqtt = channels
            .iter()
            .find(|e| e["protocol"].as_str() == Some("mqtt"))
            .unwrap_or_else(|| panic!("expected an mqtt: entry, got: {channels:?}"));
        assert_eq!(mqtt["address"], json!("mqtt:home:sensors/+/temp"));
        assert_eq!(mqtt["details"]["client"], json!("home"));
        assert_eq!(mqtt["details"]["topic"], json!("sensors/+/temp"));
        // Enriched from the service: qos from the subscribed filter, health from
        // the session's SupervisorState (never connected in a unit test →
        // disconnected, with the "unknown" last_error the Disconnected-without-a-
        // recorded-error state carries).
        assert_eq!(mqtt["details"]["qos"], json!(2), "enriched qos: {mqtt}");
        assert_eq!(
            mqtt["details"]["health"],
            json!("disconnected"),
            "enriched health: {mqtt}"
        );
        // urgency is not surfaced (no ingress urgency store; §2.5 deviation).
        assert!(
            mqtt["details"].get("urgency").is_none(),
            "urgency should be absent: {mqtt}"
        );
        assert_eq!(
            mqtt["details"]["last_error"],
            json!("unknown"),
            "a never-connected session surfaces the SupervisorState 'unknown' last_error: {mqtt}"
        );
    }

    /// `MessageChannelList` with no MqttService still lists the `mqtt:` channel
    /// with `client`/`topic`, but the runtime health fields stay absent (honest
    /// "MQTT runtime not present" — no qos/health/urgency/last_error keys).
    #[tokio::test]
    async fn message_channel_list_mqtt_entry_without_service_omits_runtime_fields() {
        use std::sync::Arc;
        let mqtt_address =
            brenn_lib::mqtt::config::parsed_address_canonical("home", "sensors/+/temp");
        let entry = brenn_lib::messaging::ChannelEntry {
            uuid: brenn_lib::messaging::mqtt_channel_uuid_from_address(&mqtt_address),
            address: mqtt_address.clone(),
            description: None,
            resolved_channel: brenn_lib::messaging::config::ResolvedChannel {
                push_depth: brenn_lib::messaging::config::Depth::Unbounded,
                retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                standing_retain_depth: brenn_lib::messaging::config::Depth::Unbounded,
                noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                sink: brenn_lib::messaging::config::Sink::Drop,
                wake_min: brenn_lib::messaging::WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: brenn_lib::messaging::ChannelScheme::Mqtt,
            mount: None,
        };
        let dir = brenn_lib::messaging::MessagingDirectory::with_entries(vec![entry]);
        // mqtt: rows are now ACL-sourced (design §2.2), so register "testapp" with
        // an mqtt_subscribe matcher covering (home, sensors/+/temp); that Pattern
        // row is what appears in the listing (the directory mqtt: entry is ignored).
        let mut messenger_apps = indexmap::IndexMap::new();
        let mut testapp_cfg =
            crate::test_support::app_config::default_test_app_config("testapp", "testapp");
        testapp_cfg
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::MqttSubscribe);
        testapp_cfg
            .policy
            .acls
            .mqtt_subscribe
            .push(brenn_lib::access::acl::MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "sensors/+/temp".to_string(),
            });
        messenger_apps.insert("testapp".to_string(), testapp_cfg);
        let messenger = brenn_lib::messaging::Messenger::new(
            brenn_lib::db::init_db_memory(),
            Arc::new(dir),
            Arc::from("test-source"),
            Arc::new(messenger_apps),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            brenn_lib::messaging::MessagingGlobalConfig::default(),
        );
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(MCP_MESSAGE_LIST_CHANNELS_TOOL, json!({}));
        let result = try_handle_messaging_tool(&bridge, &req).await;
        let Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
            updated_output: Some(out),
        })) = result
        else {
            panic!("unexpected result: {result:?}");
        };
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let channels = v["channels"].as_array().expect("channels array");
        let mqtt = channels
            .iter()
            .find(|e| e["protocol"].as_str() == Some("mqtt"))
            .unwrap_or_else(|| panic!("expected an mqtt: entry, got: {channels:?}"));
        // mqtt: rows are ACL-derived Pattern rows now.
        assert_eq!(mqtt["access"], json!("pattern"));
        assert_eq!(mqtt["details"]["client"], json!("home"));
        assert_eq!(mqtt["details"]["topic"], json!("sensors/+/temp"));
        // No service → no runtime fields enriched; they serialize away.
        for field in ["qos", "health", "urgency", "last_error"] {
            assert!(
                mqtt["details"].get(field).is_none(),
                "{field} should be absent with no MqttService: {mqtt}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // parse_optional_rfc3339 (review F16 minimum bar)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_optional_rfc3339_none_for_missing() {
        assert!(matches!(parse_optional_rfc3339(None), Ok(None)));
    }

    #[test]
    fn parse_optional_rfc3339_none_for_explicit_null() {
        let v = serde_json::Value::Null;
        assert!(matches!(parse_optional_rfc3339(Some(&v)), Ok(None)));
    }

    #[test]
    fn parse_optional_rfc3339_none_for_empty_string() {
        let v = json!("");
        assert!(matches!(parse_optional_rfc3339(Some(&v)), Ok(None)));
    }

    #[test]
    fn parse_optional_rfc3339_parses_well_formed_string() {
        let v = json!("2026-04-28T18:42:13Z");
        let result = parse_optional_rfc3339(Some(&v)).unwrap().unwrap();
        assert_eq!(result.to_rfc3339(), "2026-04-28T18:42:13+00:00");
    }

    #[test]
    fn parse_optional_rfc3339_rejects_malformed_string() {
        let v = json!("not-a-date");
        let err = parse_optional_rfc3339(Some(&v)).unwrap_err();
        assert!(err.contains("invalid RFC3339"));
        assert!(err.contains("not-a-date"));
    }

    #[test]
    fn parse_optional_rfc3339_rejects_non_string_with_variant_name() {
        let v = json!({"key": "value"});
        let err = parse_optional_rfc3339(Some(&v)).unwrap_err();
        assert!(
            err.contains("object"),
            "error should name the variant: {err}"
        );
    }

    #[test]
    fn parse_optional_rfc3339_rejects_number_with_variant_name() {
        let v = json!(42);
        let err = parse_optional_rfc3339(Some(&v)).unwrap_err();
        assert!(
            err.contains("number"),
            "error should name the variant: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // json_variant_name
    // -----------------------------------------------------------------------

    #[test]
    fn json_variant_name_covers_all_kinds() {
        assert_eq!(json_variant_name(&serde_json::Value::Null), "null");
        assert_eq!(json_variant_name(&json!(true)), "boolean");
        assert_eq!(json_variant_name(&json!(1)), "number");
        assert_eq!(json_variant_name(&json!("x")), "string");
        assert_eq!(json_variant_name(&json!([1, 2])), "array");
        assert_eq!(json_variant_name(&json!({"k": "v"})), "object");
    }

    // -----------------------------------------------------------------------
    // is_noop_tool_response (pure predicate behind
    // warn_if_unexpected_tool_response — review F27)
    // -----------------------------------------------------------------------

    #[test]
    fn is_noop_tool_response_accepts_canonical_shape() {
        let v = json!({"content": [{"type": "text", "text": "__NOOP__"}]});
        assert!(is_noop_tool_response(&v));
    }

    #[test]
    fn is_noop_tool_response_accepts_noop_with_extra_content_blocks() {
        // CC may append extra blocks; we still recognize the leading
        // text == "__NOOP__".
        let v = json!({
            "content": [
                {"type": "text", "text": "__NOOP__"},
                {"type": "text", "text": "trailing"},
            ]
        });
        assert!(is_noop_tool_response(&v));
    }

    #[test]
    fn is_noop_tool_response_rejects_empty_object() {
        assert!(!is_noop_tool_response(&json!({})));
    }

    #[test]
    fn is_noop_tool_response_rejects_non_array_content() {
        assert!(!is_noop_tool_response(&json!({"content": "not-an-array"})));
    }

    #[test]
    fn is_noop_tool_response_rejects_empty_content_array() {
        assert!(!is_noop_tool_response(&json!({"content": []})));
    }

    #[test]
    fn is_noop_tool_response_rejects_first_block_without_noop_text() {
        let v = json!({"content": [{"type": "text", "text": "real output"}]});
        assert!(!is_noop_tool_response(&v));
    }

    #[test]
    fn is_noop_tool_response_rejects_first_block_missing_text_field() {
        let v = json!({"content": [{"type": "text"}]});
        assert!(!is_noop_tool_response(&v));
    }

    // -----------------------------------------------------------------------
    // test-2: MessageChannelList no-messenger / pwa-only path (test-2)
    // -----------------------------------------------------------------------

    /// `MessageChannelList` with no Messenger but with a PwaPushService returns
    /// a successful (non-error) response containing pwa_push entries. Verifies
    /// the `None` arm does not trigger an early error response.
    #[tokio::test]
    async fn message_channel_list_pwa_only_succeeds() {
        // test_new_for_pwa_push has a PwaPushService but no Messenger.
        // No subscriptions are seeded, so the listing will be empty pwa_push —
        // but crucially the response must be ok:true, not an error.
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(MCP_MESSAGE_LIST_CHANNELS_TOOL, json!({}));
        let result = try_handle_messaging_tool(&bridge, &req).await;
        match result {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                // Must be a success shape (channels array), not an error.
                assert!(
                    v.get("ok") != Some(&json!(false)),
                    "pwa-only listing should not return an error shape: {out}"
                );
                assert!(v["channels"].is_array(), "expected channels array: {out}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // MessageSubscriptionList (design §2.1)
    // -----------------------------------------------------------------------

    /// `MessageSubscriptionList` PostToolUse returns `{ subscriptions: [...] }`
    /// scoped to the calling app: the combined-services bridge has `testapp`
    /// statically subscribed to `brenn:test-channel` (folded into the directory,
    /// no durable dynamic row), so the brenn: row appears with `dynamic = false`,
    /// and the app's pwa_push targets are appended.
    #[tokio::test]
    async fn message_subscription_list_returns_app_scoped_subscriptions() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL, json!({}));
        let result = try_handle_messaging_tool(&bridge, &req).await;
        match result {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                let subs = v["subscriptions"].as_array().expect("subscriptions array");
                // testapp's static brenn: subscription is present and flagged static.
                let brenn = subs
                    .iter()
                    .find(|s| s["protocol"].as_str() == Some("brenn"))
                    .expect("brenn: subscription present");
                assert!(
                    brenn_lib::messaging::ChannelScheme::of(brenn["address"].as_str().unwrap())
                        == Some(brenn_lib::messaging::ChannelScheme::Brenn),
                    "brenn address prefix: {:?}",
                    brenn["address"]
                );
                assert_eq!(
                    brenn["dynamic"],
                    json!(false),
                    "config-folded sub is static (dynamic=false): {brenn:?}"
                );
                // Per-subscriber params carry the fixture's known config-declared
                // values (test-3): testapp's static brenn: subscriber is folded with
                // Unbounded depths, so the serialized row must echo them exactly —
                // a presence-only check would pass even if the serializer zeroed the
                // per-subscriber params or substituted the channel-wide view.
                assert_eq!(
                    brenn["push_depth"],
                    json!("unbounded"),
                    "per-subscriber push_depth: {brenn:?}"
                );
                assert_eq!(
                    brenn["retain_depth"],
                    json!("unbounded"),
                    "per-subscriber retain_depth: {brenn:?}"
                );
                // pwa_push targets for the app are appended as subscriptions.
                let pwa: Vec<_> = subs
                    .iter()
                    .filter(|s| s["protocol"].as_str() == Some("pwa_push"))
                    .collect();
                assert!(
                    !pwa.is_empty(),
                    "expected at least one pwa_push subscription: {subs:?}"
                );
                assert_eq!(
                    pwa[0]["dynamic"],
                    json!(false),
                    "pwa_push registrations report dynamic=false"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// PreToolUse for `MessageSubscriptionList` auto-approves (Allow), not a
    /// user-visible prompt — it is a read-only inventory tool.
    #[tokio::test]
    async fn message_subscription_list_pre_tool_use_auto_approves() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = pre_tool_use_req(MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL);
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Allow { .. })) => {}
            other => panic!("expected Allow, got {other:?}"),
        }
    }

    /// `MessageSubscriptionList` with no Messenger but with a PwaPushService
    /// returns a success shape (`subscriptions` array), not an error — the same
    /// honest-empty contract as `MessageChannelList`.
    #[tokio::test]
    async fn message_subscription_list_pwa_only_succeeds() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(MCP_MESSAGE_SUBSCRIPTION_LIST_TOOL, json!({}));
        let result = try_handle_messaging_tool(&bridge, &req).await;
        match result {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert!(
                    v.get("ok") != Some(&json!(false)),
                    "pwa-only listing should not return an error shape: {out}"
                );
                assert!(
                    v["subscriptions"].is_array(),
                    "expected subscriptions array: {out}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // test-4: MessageChannelGet missing `address` field
    // -----------------------------------------------------------------------

    /// `MessageChannelGet` with no `address` field returns an error mentioning
    /// "address". Verifies that the renamed parameter (`channel` → `address`)
    /// is correctly validated.
    #[tokio::test]
    async fn message_channel_get_missing_address_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_QUERY_CHANNEL_TOOL,
            json!({ "limit": 10 }), // no address field
        );
        let result = try_handle_messaging_tool(&bridge, &req).await;
        match result {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("address"),
                    "error should mention 'address': {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // §4.4: BrennPendingList, BrennMessageCancel, BrennMessageEdit intercept tests
    // -----------------------------------------------------------------------

    /// §4.4: cancel_pre_tool_use_auto_approves — PreToolUse for all three new
    /// tools returns Allow (not a user-visible prompt).
    #[tokio::test]
    async fn new_tools_pre_tool_use_auto_approve() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        for tool_name in [
            MCP_MESSAGE_PENDING_LIST_TOOL,
            MCP_MESSAGE_CANCEL_TOOL,
            MCP_MESSAGE_EDIT_TOOL,
        ] {
            let req = pre_tool_use_req(tool_name);
            match try_handle_messaging_tool(&bridge, &req).await {
                Some(MessagingHandled::Respond(CcApprovalDecision::Allow { .. })) => {}
                other => panic!("{tool_name}: expected Allow, got {other:?}"),
            }
        }
    }

    /// §4.4: cancel_post_tool_use_with_no_messenger_returns_error.
    #[tokio::test]
    async fn cancel_post_tool_use_with_no_messenger_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_CANCEL_TOOL,
            json!({ "message_id": "550e8400-e29b-41d4-a716-446655440000" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// §4.4: cancel_post_tool_use_with_invalid_uuid_returns_error.
    #[tokio::test]
    async fn cancel_post_tool_use_with_invalid_uuid_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_CANCEL_TOOL,
            json!({ "message_id": "not-a-uuid" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("UUID") || err.contains("invalid"),
                    "error should mention UUID/invalid: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// §4.4: edit_post_tool_use_with_no_fields_returns_error.
    #[tokio::test]
    async fn edit_post_tool_use_with_no_fields_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": "550e8400-e29b-41d4-a716-446655440000" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("no fields"),
                    "error should mention 'no fields': {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// §4.4: edit_post_tool_use_with_no_messenger_returns_error (with a field).
    #[tokio::test]
    async fn edit_post_tool_use_with_no_messenger_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_EDIT_TOOL,
            json!({
                "message_id": "550e8400-e29b-41d4-a716-446655440000",
                "body": "new body"
            }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// §4.4: edit_post_tool_use_distinguishes_missing_vs_null_deliver_after —
    /// payload `{}` (plus message_id) has no `deliver_after`; `{deliver_after:null}` does.
    /// Verify the latter clears the schedule (passes Some(None)) while the former leaves it.
    ///
    /// Since we have no messenger here the calls return errors, but we can distinguish:
    /// the no-fields error fires before messenger lookup (absent), while null-deliver_after
    /// has a field and reaches the messenger-missing error.
    #[tokio::test]
    async fn edit_post_tool_use_distinguishes_missing_vs_null_deliver_after() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;

        // Missing deliver_after + only message_id → "no fields" error (fires before messenger).
        let req_missing = post_tool_use_req(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": "550e8400-e29b-41d4-a716-446655440000" }),
        );
        let out_missing = match try_handle_messaging_tool(&bridge, &req_missing).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => out,
            other => panic!("unexpected result: {other:?}"),
        };
        let v_missing: serde_json::Value = serde_json::from_str(&out_missing).unwrap();
        assert!(
            v_missing["error"]
                .as_str()
                .unwrap_or("")
                .contains("no fields"),
            "missing deliver_after should give no-fields error: {v_missing}"
        );

        // Explicit null deliver_after → has a field, reaches messenger (missing) error.
        let req_null = post_tool_use_req(
            MCP_MESSAGE_EDIT_TOOL,
            json!({
                "message_id": "550e8400-e29b-41d4-a716-446655440000",
                "deliver_after": null
            }),
        );
        let out_null = match try_handle_messaging_tool(&bridge, &req_null).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => out,
            other => panic!("unexpected result: {other:?}"),
        };
        let v_null: serde_json::Value = serde_json::from_str(&out_null).unwrap();
        // Should NOT be "no fields" — it has a field (deliver_after = null).
        assert!(
            !v_null["error"].as_str().unwrap_or("").contains("no fields"),
            "explicit null deliver_after should not give no-fields error: {v_null}"
        );
    }

    /// §4.4: pending_list_post_tool_use_returns_envelopes — messenger configured,
    /// no messages seeded → returns ok shape with empty messages array.
    #[tokio::test]
    async fn pending_list_post_tool_use_returns_envelopes() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(MCP_MESSAGE_PENDING_LIST_TOOL, json!({}));
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert!(v["messages"].is_array(), "expected messages array: {out}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// §4.4: pending_list_unknown_channel_filter_returns_empty — pin §2.11 contract.
    /// A well-formed but non-existent channel address returns empty messages, not error.
    #[tokio::test]
    async fn pending_list_unknown_channel_filter_returns_empty() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_PENDING_LIST_TOOL,
            json!({ "channel": "brenn:no-such-channel" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                let messages = v["messages"].as_array().expect("messages array");
                assert!(
                    messages.is_empty(),
                    "unknown channel should return empty list: {out}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// §4.4: malformed channel address in BrennPendingList returns empty (logged, no error).
    #[tokio::test]
    async fn pending_list_malformed_channel_filter_returns_empty() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_PENDING_LIST_TOOL,
            json!({ "channel": "not-a-brenn-address" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert!(
                    v.get("ok") != Some(&json!(false)),
                    "malformed channel should not return error shape: {out}"
                );
                let messages = v["messages"].as_array().expect("messages array");
                assert!(
                    messages.is_empty(),
                    "malformed channel should return empty list: {out}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// test-5: BrennPendingList with no messenger configured returns an error response.
    #[tokio::test]
    async fn pending_list_post_tool_use_with_no_messenger_returns_error() {
        // test_new_for_pwa_push creates a bridge with no Messenger configured.
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(MCP_MESSAGE_PENDING_LIST_TOOL, json!({}));
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(
                    v["ok"],
                    json!(false),
                    "no-messenger BrennPendingList should return error: {out}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// test-6: BrennMessageEdit with an invalid UUID returns an error response.
    #[tokio::test]
    async fn edit_post_tool_use_with_invalid_uuid_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": "not-a-uuid", "body": "new body" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(
                    v["ok"],
                    json!(false),
                    "invalid UUID should return error: {out}"
                );
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.to_lowercase().contains("uuid") || err.contains("invalid"),
                    "error should mention UUID: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// parse-message-uuid-missing-field-test (cancel arm): empty payload returns
    /// an error mentioning "message_id".
    #[tokio::test]
    async fn cancel_post_tool_use_with_missing_message_id_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(MCP_MESSAGE_CANCEL_TOOL, json!({}));
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("message_id"),
                    "error should mention 'message_id': {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// parse-message-uuid-missing-field-test (edit arm): payload with a body
    /// field but no message_id returns an error mentioning "message_id".
    #[tokio::test]
    async fn edit_post_tool_use_with_missing_message_id_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(MCP_MESSAGE_EDIT_TOOL, json!({ "body": "new body" }));
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("message_id"),
                    "error should mention 'message_id': {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Webhook channel listing and BrennSend cross-protocol guard (test-4)
    // -----------------------------------------------------------------------

    /// Build a minimal `WebhookService` with one endpoint owned by "testapp".
    #[allow(dead_code)]
    fn make_test_webhook_service() -> std::sync::Arc<brenn_lib::webhook::WebhookService> {
        use brenn_lib::webhook::config::ResolvedWebhookEndpoint;
        use brenn_lib::webhook::signature::{HexFormat, SignatureAlgorithm, SignatureScheme};
        use std::collections::HashMap;
        use std::sync::Arc;

        let mut keys = HashMap::new();
        keys.insert("primary".to_string(), b"test-secret".to_vec());
        let ep = Arc::new(ResolvedWebhookEndpoint {
            slug: "test-ep".to_string(),
            mount: "/webhooks/test-ep".to_string(),
            description: Some("test endpoint".to_string()),
            transport_ceiling_bytes: 1024 * 1024,
            content_type: "application/json".to_string(),
            scheme: SignatureScheme::HmacRawBody {
                algorithm: SignatureAlgorithm::HmacSha256,
                header: "x-sig".parse().unwrap(),
                format: HexFormat::V1Hex,
                key_id_header: None,
                keys,
            },
            owner: brenn_lib::webhook::config::WebhookOwner::App(Arc::from("testapp")),
            urgency: brenn_lib::messaging::Urgency::Normal,
            replay_protection: None,
        });
        brenn_lib::webhook::WebhookService::new(vec![("test-ep".to_string(), ep)])
    }

    /// `MessageChannelList` on a bridge whose Messenger directory contains a
    /// `webhook:` channel emits a `webhook` protocol entry with the correct
    /// address and `details.mount`.
    ///
    /// After this slice, webhook channel listings come from the persisted
    /// directory (not from runtime WebhookService synthesis). The test therefore
    /// builds a Messenger whose directory includes the `webhook:test-ep` entry.
    #[tokio::test]
    async fn message_channel_list_includes_webhook_endpoints() {
        use std::sync::Arc;

        use crate::tools::messaging::MCP_MESSAGE_LIST_CHANNELS_TOOL;
        use brenn_lib::messaging::config::{Depth, NoiseLevel, ResolvedChannel, Sink};
        use brenn_lib::messaging::{
            ChannelEntry, ChannelScheme, MessagingDirectory, Messenger, WakeMin,
            webhook_channel_uuid_from_slug,
        };

        let db = brenn_lib::db::init_db_memory();
        let slug = "test-ep";
        let uuid = webhook_channel_uuid_from_slug(slug);
        let entry = ChannelEntry {
            uuid,
            address: format!("webhook:{slug}"),
            description: Some("test endpoint".to_string()),
            resolved_channel: ResolvedChannel {
                push_depth: Depth::Unbounded,
                retain_depth: Depth::Unbounded,
                standing_retain_depth: Depth::Unbounded,
                noise: NoiseLevel::Silent,
                sink: Sink::Drop,
                wake_min: WakeMin::Normal,
            },
            subscribers: vec![],
            transport_type: ChannelScheme::Webhook,
            mount: Some("/webhooks/test-ep".to_string()),
        };
        {
            let conn = db.lock().await;
            brenn_lib::messaging::db::upsert_channels(&conn, std::slice::from_ref(&entry));
        }
        let directory = Arc::new(MessagingDirectory::with_entries(vec![entry]));
        // webhook: rows survive list_accessible_channels' ACL filter only when the
        // app's policy covers the endpoint (design §2.2): Webhook grant + a webhook
        // matcher for `test-ep`. Register "testapp" accordingly.
        let mut messenger_apps = indexmap::IndexMap::new();
        let mut testapp_cfg =
            crate::test_support::app_config::default_test_app_config("testapp", "testapp");
        testapp_cfg
            .policy
            .grants
            .insert(brenn_lib::access::AppCapability::Webhook);
        testapp_cfg
            .policy
            .acls
            .webhook
            .push(brenn_lib::access::acl::WebhookMatcher {
                endpoint: slug.to_string(),
            });
        messenger_apps.insert("testapp".to_string(), testapp_cfg);
        let messenger = Messenger::new(
            db.clone(),
            directory,
            Arc::from("https://test.example"),
            Arc::new(messenger_apps),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            brenn_lib::messaging::config::MessagingGlobalConfig::default(),
        );

        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(MCP_MESSAGE_LIST_CHANNELS_TOOL, json!({}));
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                let channels = v["channels"].as_array().expect("channels array");
                let wh = channels
                    .iter()
                    .find(|c| c["protocol"].as_str() == Some("webhook"))
                    .expect("expected a webhook channel entry");
                assert_eq!(
                    wh["address"].as_str(),
                    Some("webhook:test-ep"),
                    "address should be webhook:<slug>"
                );
                assert_eq!(
                    wh["details"]["mount"].as_str(),
                    Some("/webhooks/test-ep"),
                    "details.mount should be the HTTP mount path"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `BrennSend` with a `webhook:` address returns an error containing
    /// "inbound-only" (the guard fires before any messenger lookup).
    #[tokio::test]
    async fn brenn_send_with_webhook_address_returns_inbound_only_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "webhook:test-ep", "body": "hello" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.to_ascii_lowercase().contains("inbound-only"),
                    "error should mention inbound-only: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Happy-path tests: BrennSend success, missing-to, cancel/edit
    // -----------------------------------------------------------------------

    /// BrennSend with a valid `brenn:test-channel` address and a seeded `testapp`
    /// sender config returns `ok: true`, a valid UUID `message_id`, a `brenn:`
    /// address, and `remaining_budget == 99` (default 100 minus one send).
    #[tokio::test]
    async fn brenn_send_success_returns_ok() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "brenn:test-channel", "body": "hello" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "expected ok: true, got: {out}");
                let msg_id = v["message_id"]
                    .as_str()
                    .expect("message_id should be string");
                uuid::Uuid::parse_str(msg_id)
                    .unwrap_or_else(|_| panic!("message_id {msg_id:?} is not a valid UUID"));
                let addr = v["address"].as_str().expect("address should be string");
                assert_eq!(
                    addr, "brenn:test-channel",
                    "address should be canonical channel address: {addr:?}"
                );
                assert_eq!(
                    v["remaining_budget"],
                    json!(99u32),
                    "remaining_budget should be 99 after one send: {out}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// BrennSend to a well-formed but unknown `brenn:` channel returns
    /// `ok: false` with the unified gate-blind denial wording — deliberately the
    /// same string as the AclDenied arm so it names no gate (oracle closure).
    #[tokio::test]
    async fn brenn_send_unknown_channel_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "brenn:no-such-channel", "body": "hello" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("does not exist or is not in this app's brenn_publish allowlist"),
                    "error should carry the unified denial wording: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// BrennSend to a channel that resolves in the directory but is NOT covered
    /// by the app's `brenn_publish` ACL returns `ok: false` with an error naming
    /// the `brenn_publish` allowlist. Exercises the `PublishResult::AclDenied` arm
    /// and its LLM-facing serialization (design §2.2, Seam A) — the unit tests in
    /// `publish_core.rs` prove the variant is returned, but only this test covers
    /// the intercept-layer error string and `ok: false` shape. The fixture
    /// registers `brenn:locked-channel` (resolvable) but scopes `brenn_publish` to
    /// `Exact("test-channel")`, so `locked-channel` is granted-but-out-of-scope.
    #[tokio::test]
    async fn brenn_send_acl_denied_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "brenn:locked-channel", "body": "hello" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("brenn_publish") || err.to_lowercase().contains("allowlist"),
                    "error should name the brenn_publish allowlist: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// BrennSend with a missing `to` field returns `ok: false` with an error
    /// mentioning "to". Exercises the early-return before messenger lookup.
    #[tokio::test]
    async fn brenn_send_missing_to_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "body": "hello" }), // no `to` field
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("missing or empty"),
                    "error should mention 'missing or empty': {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Ephemeral `MessageSend`: the `publish_any`
    // dispatch routes `ephemeral:` targets to the bus and each outcome maps to
    // its documented LLM-facing teach-string. The dispatch-layer variant
    // production is proven in `publish/tests/dispatch_any.rs`; these tests cover
    // the intercept-layer serialization (`ok` shape + error strings).
    // -----------------------------------------------------------------------

    fn eph_entry(name: &str) -> brenn_lib::messaging::config::EphemeralChannelEntry {
        brenn_lib::messaging::testutils::ephemeral_channel_entry(name, 8, 16)
    }

    /// A `Messenger` with a two-channel ephemeral bus (`protobar` + `locked`).
    /// When `grant` is set, the `testapp` policy holds `EphemeralPublish` scoped
    /// to `protobar` only (`locked` is declared but outside the ACL, exercising
    /// the intercept `AclDenied` arm); when unset, `testapp` holds no grant,
    /// exercising the `MissingSender` arm. `max_body_bytes` bounds every publish.
    fn ephemeral_intercept_messenger_cfg(
        grant: bool,
        max_body_bytes: usize,
    ) -> std::sync::Arc<brenn_lib::messaging::Messenger> {
        use std::sync::Arc;

        let db = brenn_lib::db::init_db_memory();
        let dir = brenn_lib::messaging::MessagingDirectory::with_entries(vec![]);
        let mut apps = indexmap::IndexMap::new();
        let mut testapp_cfg =
            crate::test_support::app_config::default_test_app_config("testapp", "testapp");
        if grant {
            testapp_cfg
                .policy
                .grants
                .insert(brenn_lib::access::AppCapability::EphemeralPublish);
            testapp_cfg.policy.acls.ephemeral_publish.push(
                brenn_lib::access::acl::ChannelMatcher::Exact("protobar".to_string()),
            );
        }
        apps.insert("testapp".to_string(), testapp_cfg);
        let messenger = brenn_lib::messaging::Messenger::new(
            db,
            Arc::new(dir),
            Arc::from("test-source"),
            Arc::new(apps),
            Arc::new(brenn_lib::messaging::query::NoopWakeRouter)
                as Arc<dyn brenn_lib::messaging::WakeRouter>,
            brenn_lib::messaging::MessagingGlobalConfig::default(),
        );
        let bus = brenn_lib::messaging::EphemeralBus::new(
            vec![eph_entry("protobar"), eph_entry("locked")],
            Arc::from("test-source"),
            max_body_bytes,
        );
        messenger.with_ephemeral_bus(bus)
    }

    /// The common fixture: `testapp` holds the grant, generous body limit.
    fn ephemeral_intercept_messenger() -> std::sync::Arc<brenn_lib::messaging::Messenger> {
        ephemeral_intercept_messenger_cfg(true, 65536)
    }

    /// `MessageSend` to a granted `ephemeral:` channel returns `ok: true`, a
    /// valid UUID `message_id`, the canonical `ephemeral:` address, and — unlike
    /// durable — no `remaining_budget` field (ephemeral has no budget).
    #[tokio::test]
    async fn ephemeral_send_success_returns_ok_without_budget() {
        let messenger = ephemeral_intercept_messenger();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "ephemeral:protobar", "body": "hello" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "expected ok: true, got: {out}");
                let msg_id = v["message_id"]
                    .as_str()
                    .expect("message_id should be string");
                uuid::Uuid::parse_str(msg_id)
                    .unwrap_or_else(|_| panic!("message_id {msg_id:?} is not a valid UUID"));
                assert_eq!(
                    v["address"].as_str(),
                    Some("ephemeral:protobar"),
                    "address should be the canonical ephemeral address: {out}"
                );
                assert!(
                    v.get("remaining_budget").is_none(),
                    "ephemeral success carries no remaining_budget: {out}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `MessageSend` to an `ephemeral:` channel with a durable-only option
    /// (`reply_to`) is rejected with a teach-string naming the field and the
    /// `ephemeral:` scheme.
    #[tokio::test]
    async fn ephemeral_send_reply_to_returns_unsupported_option() {
        let messenger = ephemeral_intercept_messenger();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "ephemeral:protobar", "body": "hi", "reply_to": "brenn:x" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("reply_to") && err.contains("ephemeral:"),
                    "error should name reply_to and the ephemeral scheme: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `MessageSend` to a well-formed but undeclared `ephemeral:` channel returns
    /// `ok: false` with the unified denial teach-string (deliberately gate-blind:
    /// the same wording as AclDenied, so it names no gate).
    #[tokio::test]
    async fn ephemeral_send_unknown_channel_returns_error() {
        let messenger = ephemeral_intercept_messenger();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "ephemeral:no-such", "body": "hi" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains(
                        "does not exist or is not in this app's ephemeral_publish allowlist"
                    ),
                    "error should carry the unified denial wording: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `MessageSend` to a declared `ephemeral:` channel the app holds the grant
    /// for but which is outside its `ephemeral_publish` ACL returns `ok: false`
    /// with a teach-string naming the `ephemeral_publish` allowlist.
    #[tokio::test]
    async fn ephemeral_send_acl_denied_returns_error() {
        let messenger = ephemeral_intercept_messenger();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "ephemeral:locked", "body": "hi" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("ephemeral_publish") || err.to_lowercase().contains("allowlist"),
                    "error should name the ephemeral_publish allowlist: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `MessageSend` to a malformed `ephemeral:` address (disallowed characters)
    /// returns `ok: false` with a teach-string mentioning the `ephemeral:` form.
    #[tokio::test]
    async fn ephemeral_send_malformed_address_returns_error() {
        let messenger = ephemeral_intercept_messenger();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "ephemeral:bad name", "body": "hi" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.to_lowercase().contains("malformed") && err.contains("ephemeral:"),
                    "error should mention malformed and the ephemeral form: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `MessageSend` from an app holding no `EphemeralPublish` grant returns
    /// `ok: false` with the "messaging not configured" (sender missing)
    /// teach-string.
    #[tokio::test]
    async fn ephemeral_send_missing_sender_returns_error() {
        let messenger = ephemeral_intercept_messenger_cfg(false, 65536);
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "ephemeral:protobar", "body": "hi" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("sender missing") || err.contains("not configured"),
                    "error should name the missing sender: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `MessageSend` past the per-sender burst returns `ok: false` with the
    /// "rate limited" teach-string. The first `EPHEMERAL_SENDER_BURST` sends
    /// drain the bucket; the next one is rate-limited.
    #[tokio::test]
    async fn ephemeral_send_rate_limited_returns_error() {
        let messenger = ephemeral_intercept_messenger();
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "ephemeral:protobar", "body": "hi" }),
        );
        for _ in 0..brenn_lib::messaging::EPHEMERAL_SENDER_BURST {
            match try_handle_messaging_tool(&bridge, &req).await {
                Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                    updated_output: Some(out),
                })) => {
                    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                    assert_eq!(v["ok"], json!(true), "burst send should succeed: {out}");
                }
                other => panic!("unexpected result during burst: {other:?}"),
            }
        }
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.to_lowercase().contains("rate limited"),
                    "error should mention rate limiting: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `MessageSend` of a body exceeding the bus `max_body_bytes` returns
    /// `ok: false` with a teach-string interpolating the byte length and max.
    #[tokio::test]
    async fn ephemeral_send_body_too_large_returns_error() {
        let messenger = ephemeral_intercept_messenger_cfg(true, 4);
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_messenger(messenger).await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "ephemeral:protobar", "body": "way too big" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "expected ok: false, got: {out}");
                let err = v["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("body too large") && err.contains("max 4"),
                    "error should name the size and max: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// Seed-then-cancel: publish a message, extract `message_id`, cancel it.
    /// Exercises `CancelResult::Ok` arm and `BrennMessageCancelOk` serialization.
    #[tokio::test]
    async fn cancel_success_returns_ok() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;

        // Publish a message to get a real message_id.
        let send_req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "brenn:test-channel", "body": "cancel me" }),
        );
        let message_id = match try_handle_messaging_tool(&bridge, &send_req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "publish should succeed: {out}");
                v["message_id"].as_str().expect("message_id").to_string()
            }
            other => panic!("publish: unexpected result: {other:?}"),
        };

        // Cancel the message.
        let cancel_req =
            post_tool_use_req(MCP_MESSAGE_CANCEL_TOOL, json!({ "message_id": message_id }));
        match try_handle_messaging_tool(&bridge, &cancel_req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "cancel should succeed: {out}");
                assert_eq!(
                    v["cancelled"],
                    json!(true),
                    "cancelled should be true: {out}"
                );
                assert_eq!(
                    v["message_id"].as_str(),
                    Some(message_id.as_str()),
                    "cancelled message_id should match: {out}"
                );
                assert_eq!(
                    v["cancelled_pushes"],
                    json!(1u32),
                    "cancelled_pushes should be 1 (one pending push row, not yet delivered): {out}"
                );
            }
            other => panic!("cancel: unexpected result: {other:?}"),
        }
    }

    /// Seed-then-edit: publish a message, extract `message_id`, edit body.
    /// Exercises `EditResult::Ok` arm and `ok: true` injection.
    #[tokio::test]
    async fn edit_success_returns_ok_with_envelope() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;

        // Publish a message to get a real message_id.
        let send_req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "brenn:test-channel", "body": "original body" }),
        );
        let message_id = match try_handle_messaging_tool(&bridge, &send_req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "publish should succeed: {out}");
                v["message_id"].as_str().expect("message_id").to_string()
            }
            other => panic!("publish: unexpected result: {other:?}"),
        };

        // Edit the message body.
        let edit_req = post_tool_use_req(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": message_id, "body": "edited body" }),
        );
        match try_handle_messaging_tool(&bridge, &edit_req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "edit should succeed: {out}");
                assert_eq!(
                    v["body"].as_str(),
                    Some("edited body"),
                    "body should be updated: {out}"
                );
                assert_eq!(
                    v["message_id"].as_str(),
                    Some(message_id.as_str()),
                    "message_id should match: {out}"
                );
            }
            other => panic!("edit: unexpected result: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Wire-shape regression guards for C2 typed structs
    // -----------------------------------------------------------------------

    /// `BrennSendOk` must serialize byte-identically to the source `json!`.
    #[test]
    fn brenn_send_ok_matches_reference() {
        let ok = super::BrennSendOk {
            ok: true,
            message_id: "00000000-0000-0000-0000-000000000001".to_string(),
            address: "brenn:my-channel",
            remaining_budget: 99,
        };
        let produced = serde_json::to_string(&ok).expect("BrennSendOk serialization is infallible");
        let reference = serde_json::json!({
            "ok": true,
            "message_id": "00000000-0000-0000-0000-000000000001",
            "address": "brenn:my-channel",
            "remaining_budget": 99_u32,
        })
        .to_string();
        assert_eq!(produced, reference);
    }

    /// `BrennMessageCancelOk` must serialize byte-identically to the source `json!`.
    #[test]
    fn brenn_message_cancel_ok_matches_reference() {
        let ok = super::BrennMessageCancelOk {
            ok: true,
            cancelled: true,
            message_id: "00000000-0000-0000-0000-000000000002".to_string(),
            cancelled_pushes: 3,
        };
        let produced =
            serde_json::to_string(&ok).expect("BrennMessageCancelOk serialization is infallible");
        let reference = serde_json::json!({
            "ok": true,
            "cancelled": true,
            "message_id": "00000000-0000-0000-0000-000000000002",
            "cancelled_pushes": 3_u32,
        })
        .to_string();
        assert_eq!(produced, reference);
    }

    /// `MessageChannelListResponse` must serialize byte-identically to the source `json!`.
    #[test]
    fn message_channel_list_response_empty_matches_reference() {
        let resp = super::MessageChannelListResponse { channels: &[] };
        let produced = serde_json::to_string(&resp)
            .expect("MessageChannelListResponse serialization is infallible");
        let reference = serde_json::json!({ "channels": [] }).to_string();
        assert_eq!(produced, reference);
    }

    /// `BrennPendingListResponse` must serialize byte-identically (empty case).
    #[test]
    fn brenn_pending_list_response_empty_matches_reference() {
        let empty: &[brenn_lib::messaging::MessageEnvelope] = &[];
        let resp = super::BrennPendingListResponse { messages: empty };
        let produced = serde_json::to_string(&resp)
            .expect("BrennPendingListResponse serialization is infallible");
        let reference = serde_json::json!({ "messages": [] }).to_string();
        assert_eq!(produced, reference);
    }

    /// `BrennPendingListResponse` must serialize correctly with ≥1 envelope
    /// (non-empty case — guards `MessageEnvelope` serialization path through struct).
    /// Compared as `Value` (not byte-identical): `MessageEnvelope` fields
    /// serialize in declaration order while `json!` uses BTreeMap ordering.
    #[test]
    fn brenn_pending_list_response_nonempty_matches_reference() {
        use brenn_lib::messaging::{MessageEnvelope, Urgency};
        use uuid::Uuid;
        let message_id =
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid uuid");
        let publish_ts = chrono::DateTime::parse_from_rfc3339("2026-05-18T10:00:00+00:00")
            .expect("valid ts")
            .with_timezone(&chrono::Utc);
        let envelope = MessageEnvelope {
            message_id,
            source: "brenn".to_string(),
            channel: "brenn:test".to_string(),
            sender: "alice".to_string(),
            publish_ts,
            body: "hello".to_string(),
            reply_to: None,
            delivery_deadline: None,
            deliver_after: None,
            urgency: Urgency::Low,
            envelope_type: brenn_lib::messaging::ChannelScheme::Brenn,
        };
        let messages = vec![envelope.clone()];
        let resp = super::BrennPendingListResponse {
            messages: &messages,
        };
        let produced = serde_json::to_string(&resp)
            .expect("BrennPendingListResponse serialization is infallible");
        let produced_val: serde_json::Value =
            serde_json::from_str(&produced).expect("produced must be valid JSON");
        let reference = serde_json::json!({ "messages": [envelope] });
        assert_eq!(produced_val, reference);
        // Verify at least one message entry round-tripped.
        let arr = produced_val["messages"].as_array().expect("messages array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["message_id"], "550e8400-e29b-41d4-a716-446655440000");

        // test-3: string-level assertion that the wire JSON carries `urgency`, not `wake`.
        // If MessageEnvelope still had `#[serde(rename = "wake")]` on the field, the
        // struct-vs-struct comparison above would pass but this would fail.
        assert_eq!(
            arr[0]["urgency"],
            json!("low"),
            "urgency field must appear as kebab-case 'low' in envelope JSON: {:?}",
            arr[0]
        );
        assert!(
            arr[0].get("wake").is_none(),
            "legacy wake field must not appear in envelope JSON: {:?}",
            arr[0]
        );
    }

    // -----------------------------------------------------------------------
    // test-1: Legacy `wake` key and unknown `urgency` value rejection
    // -----------------------------------------------------------------------

    /// BrennSend with legacy `wake` key returns `ok:false` error mentioning `urgency`.
    /// Guards the reject-and-teach path (§2.4): a stale-habit LLM sending `wake` must
    /// get an explicit error rather than silent downgrade to the default urgency.
    #[tokio::test]
    async fn brenn_send_legacy_wake_key_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "brenn:test-channel", "body": "x", "wake": "immediate" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
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

    /// BrennSend with an unknown `urgency` value returns `ok:false` with an error
    /// mentioning the bad value.
    #[tokio::test]
    async fn brenn_send_unknown_urgency_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "brenn:test-channel", "body": "x", "urgency": "garbage" }),
        );
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
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

    /// BrennMessageEdit with legacy `wake` key returns `ok:false` error mentioning `urgency`.
    #[tokio::test]
    async fn brenn_edit_legacy_wake_key_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;

        // Seed a message first.
        let send_req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "brenn:test-channel", "body": "original" }),
        );
        let message_id = match try_handle_messaging_tool(&bridge, &send_req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                v["message_id"].as_str().expect("message_id").to_string()
            }
            other => panic!("publish: unexpected result: {other:?}"),
        };

        let edit_req = post_tool_use_req(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": message_id, "wake": "immediate" }),
        );
        match try_handle_messaging_tool(&bridge, &edit_req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
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

    /// BrennMessageEdit with unknown `urgency` value returns `ok:false`.
    #[tokio::test]
    async fn brenn_edit_unknown_urgency_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;

        // Seed a message first.
        let send_req = post_tool_use_req(
            MCP_MESSAGE_SEND_TOOL,
            json!({ "to": "brenn:test-channel", "body": "original" }),
        );
        let message_id = match try_handle_messaging_tool(&bridge, &send_req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                v["message_id"].as_str().expect("message_id").to_string()
            }
            other => panic!("publish: unexpected result: {other:?}"),
        };

        let edit_req = post_tool_use_req(
            MCP_MESSAGE_EDIT_TOOL,
            json!({ "message_id": message_id, "urgency": "garbage" }),
        );
        match try_handle_messaging_tool(&bridge, &edit_req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
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

    // -----------------------------------------------------------------------
    // MessageSubscribe intercept tests (design §2.4 / §5)
    // -----------------------------------------------------------------------

    /// Run the `MessageSubscribe` intercept and return the parsed JSON result.
    async fn subscribe_result(
        bridge: &crate::active_bridge::ActiveBridge,
        input: serde_json::Value,
    ) -> serde_json::Value {
        let req = post_tool_use_req(MCP_MESSAGE_SUBSCRIBE_TOOL, input);
        match try_handle_messaging_tool(bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => serde_json::from_str(&out).expect("valid JSON"),
            other => panic!("unexpected MessageSubscribe result: {other:?}"),
        }
    }

    /// PreToolUse for MessageSubscribe auto-approves (Allow).
    #[tokio::test]
    async fn message_subscribe_pre_tool_use_auto_approves() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let req = pre_tool_use_req(MCP_MESSAGE_SUBSCRIBE_TOOL);
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Allow { .. })) => {}
            other => panic!("expected Allow, got: {other:?}"),
        }
    }

    /// Subscribe to an existing `brenn:` channel → ok, status "subscribed", and
    /// the subscriber is folded into the directory.
    #[tokio::test]
    async fn message_subscribe_existing_brenn_channel_succeeds() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 5 }),
        )
        .await;
        assert_eq!(v["ok"], json!(true), "expected ok: {v}");
        assert_eq!(v["status"], json!("subscribed"));
        assert_eq!(v["address"], json!("brenn:test-channel"));

        let entry = bridge
            .messenger()
            .unwrap()
            .directory()
            .resolve("brenn:test-channel")
            .expect("channel present");
        assert!(
            entry.subscribers.iter().any(|s| matches!(
                &s.kind,
                brenn_lib::messaging::SubscriberEntryKind::App(slug) if slug == "testapp"
            )),
            "subscriber folded"
        );
    }

    /// Subscribe to a non-existent `brenn:` channel → ok:false (never auto-creates).
    #[tokio::test]
    async fn message_subscribe_nonexistent_brenn_channel_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:does-not-exist", "push_depth": 0, "retain_depth": 1 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
    }

    /// Subscribe to a new `mqtt:` filter on the configured `home` client →
    /// ok, channel created, route added, status pending_reconnect (client cell
    /// empty in the fixture).
    #[tokio::test]
    async fn message_subscribe_new_mqtt_filter_creates_and_activates() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "mqtt:home:sensors/+/temp";
        let v = subscribe_result(
            &bridge,
            json!({ "address": addr, "push_depth": 0, "retain_depth": 5 }),
        )
        .await;
        assert_eq!(v["ok"], json!(true), "expected ok: {v}");
        assert_eq!(v["status"], json!("subscribed_pending_reconnect"));
        assert!(
            bridge
                .messenger()
                .unwrap()
                .directory()
                .resolve(addr)
                .is_some(),
            "mqtt channel created"
        );
    }

    /// Subscribe to an `mqtt:` client the app *is* authorized for but which has
    /// no ingress supervisor → ok:false (the configured-client guard fires).
    ///
    /// The default `test_new_for_mqtt_subscribe` fixture's policy scopes its
    /// `mqtt_subscribe` matcher to the `home` client, so a subscribe naming
    /// `nope` would be `PolicyDenied` *before* the configured-client guard is
    /// ever reached — masking the guard this test is about. To actually exercise
    /// the `UnconfiguredMqttClient` path at the intercept (full tool-response)
    /// layer, build a bridge whose policy authorizes the `nope` client, so the
    /// ACL gate admits the request and the guard becomes the thing that denies.
    #[tokio::test]
    async fn message_subscribe_unconfigured_mqtt_client_errors() {
        let policy = crate::test_support::app_config::mqtt_acl_policy("nope", "#");
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy)
                .await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "mqtt:nope:sensors/x", "push_depth": 0, "retain_depth": 1 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        // The guard's message names the unconfigured client — distinguishing this
        // from a PolicyDenied response, which (by design §3.3) never echoes
        // whether the client exists.
        let err = v["error"].as_str().expect("error string present");
        assert!(
            err.contains("nope"),
            "expected the unconfigured-client error to name the client, got: {err}"
        );
        // No channel created for the unconfigured client.
        assert!(
            bridge
                .messenger()
                .unwrap()
                .directory()
                .resolve("mqtt:nope:sensors/x")
                .is_none()
        );
    }

    /// Subscribe to an `mqtt:` client the app is **not** authorized for →
    /// ok:false with the policy-denied message, and (per design §3.3) the error
    /// does **not** leak whether the client is configured. The default fixture's
    /// policy is scoped to the `home` client, so naming `nope` is denied by the
    /// ACL gate *before* the configured-client guard — the gate fires first and
    /// hides broker topology. This is the intercept-layer (full tool-response)
    /// companion to the activation-layer
    /// `subscribe_mqtt_unconfigured_client_errors_before_persist` ordering test.
    #[tokio::test]
    async fn message_subscribe_policy_denied_client_mismatch_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "mqtt:nope:sensors/x", "push_depth": 0, "retain_depth": 1 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        let err = v["error"].as_str().expect("error string present");
        assert!(
            err.contains("access policy"),
            "expected policy-denied message, got: {err}"
        );
        // The deny must not reveal whether `nope` is configured (no leak).
        assert!(
            !err.contains("configured") && !err.contains("ingress"),
            "policy-denied error must not leak broker topology, got: {err}"
        );
        // Nothing persisted on the deny path.
        assert!(
            bridge
                .messenger()
                .unwrap()
                .directory()
                .resolve("mqtt:nope:sensors/x")
                .is_none()
        );
    }

    /// The intercept-site observability contract (design §3.4): a `PolicyDenied`
    /// dynamic subscribe emits a WARN with `reason = "access policy"` (a
    /// CC-anomaly surface, **not** fail2ban signal), and a *different* subscribe
    /// error does **not** emit that same warn. This pins the `if let PolicyDenied
    /// { .. } = e` guard so a future change that either drops it (warning on
    /// every error) or suppresses it for `PolicyDenied` is caught.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn message_subscribe_policy_denied_emits_warn() {
        // (a) A policy-denied subscribe (client mismatch: fixture is scoped to
        // `home`, request names `nope`) emits the access-policy warn.
        let denied_bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let denied = subscribe_result(
            &denied_bridge,
            json!({ "address": "mqtt:nope:sensors/x", "push_depth": 0, "retain_depth": 1 }),
        )
        .await;
        assert_eq!(denied["ok"], json!(false), "expected denial: {denied}");
        assert!(
            logs_contain("reason=\"access policy\""),
            "PolicyDenied must emit a WARN with reason=\"access policy\""
        );

        // (b) A non-policy subscribe error (authorized client `nope`, but no
        // ingress supervisor → UnconfiguredMqttClient) must NOT add a second
        // access-policy warn. The fixture policy here authorizes `nope`, so the
        // ACL gate admits the request and the configured-client guard denies it
        // instead — a distinct error that is not a policy denial.
        let policy = crate::test_support::app_config::mqtt_acl_policy("nope", "#");
        let other_bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe_with_policy(policy)
                .await;
        let other = subscribe_result(
            &other_bridge,
            json!({ "address": "mqtt:nope:sensors/x", "push_depth": 0, "retain_depth": 1 }),
        )
        .await;
        assert_eq!(other["ok"], json!(false), "expected error: {other}");
        // `logs_assert` sees all events captured so far in this test; assert the
        // access-policy warn appears exactly once (from step (a) only), proving
        // the UnconfiguredMqttClient path did not emit it.
        logs_assert(|lines: &[&str]| {
            let count = lines
                .iter()
                .filter(|l| l.contains("reason=\"access policy\""))
                .count();
            if count == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected exactly one access-policy warn (PolicyDenied only), saw {count}"
                ))
            }
        });
    }

    /// A dynamic subscribe whose `retain_depth` exceeds the channel's standing
    /// window is an app requesting a read window beyond the operator's baseline —
    /// the same "app exceeding its grant" signal class as `PolicyDenied`. It emits
    /// the subscribe-denial WARN with `reason = "retain depth exceeds standing"`; a
    /// conforming subscribe (at/under standing) does not.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn message_subscribe_over_standing_emits_warn() {
        // `test-channel` here has a bounded standing_retain_depth of 2.
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe_bounded_standing()
                .await;

        // (a) Over-standing (retain 5 > standing 2) → error + retain-depth warn.
        let over = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 5 }),
        )
        .await;
        assert_eq!(over["ok"], json!(false), "expected denial: {over}");
        assert!(
            logs_contain("reason=\"retain depth exceeds standing\""),
            "over-standing subscribe must emit the retain-depth WARN"
        );

        // (b) A conforming subscribe (retain 2 == standing 2) succeeds and adds no
        // second retain-depth warn.
        let ok = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 2 }),
        )
        .await;
        assert_eq!(ok["ok"], json!(true), "conforming subscribe succeeds: {ok}");
        logs_assert(|lines: &[&str]| {
            let count = lines
                .iter()
                .filter(|l| l.contains("reason=\"retain depth exceeds standing\""))
                .count();
            if count == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected exactly one retain-depth warn (over-standing only), saw {count}"
                ))
            }
        });
    }

    /// A dynamic subscribe that hits a **dormant** durable row (a boot-merge
    /// `revoked` row: durable-only, no directory subscriber) is a plain tool error
    /// (`ok:false`) and — unlike an over-grant — emits **no** subscribe-denial WARN:
    /// dormant state is app-visible, not an app exceeding its grant. Pins the
    /// intercept `denial_reason` match's `_ => None` arm for `DormantSubscriptionExists`.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn message_subscribe_dormant_row_errors_without_warn() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        // Seed a dormant durable row on `brenn:test-channel`: durable-only, no
        // directory subscriber folded (the boot-merge revoked shape). Standing here
        // is Unbounded, so the cap does not trip — the dormant-row probe is what the
        // subscribe hits.
        let uuid = bridge
            .messenger()
            .unwrap()
            .directory()
            .resolve("brenn:test-channel")
            .expect("test-channel present")
            .uuid;
        {
            let conn = bridge.messenger().unwrap().db().lock().await;
            brenn_lib::messaging::db::insert_dynamic_subscription(
                &conn,
                &brenn_lib::messaging::db::DynamicSubscriptionRow {
                    channel_uuid: uuid,
                    app_slug: "testapp".to_string(),
                    push_depth: brenn_lib::messaging::config::Depth::Bounded(0),
                    retain_depth: brenn_lib::messaging::config::Depth::Bounded(5),
                    noise: brenn_lib::messaging::config::NoiseLevel::Silent,
                    wake_min: brenn_lib::messaging::WakeMin::Normal,
                    qos: None,
                    created_at: brenn_lib::db::format_ts_for_db(chrono::Utc::now()),
                },
            );
        }

        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 5 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "dormant row → tool error: {v}");
        logs_assert(|lines: &[&str]| {
            if lines.iter().any(|l| l.contains("MessageSubscribe denied")) {
                Err("dormant-row subscribe must not emit the subscribe-denial WARN".to_string())
            } else {
                Ok(())
            }
        });
    }

    /// Push-enabled (`push_depth > 0`) subscribe on a **non-singleton** app →
    /// ok:false, surfacing the singleton requirement. Exercises the
    /// intercept→core handoff of the shared resolver's push-enabled invariant
    /// (design §5): the resolver returns `PushEnabledRequiresSingleton`, the
    /// runtime wrapper wraps it as `RuntimeSubscribeError::Params`, and the
    /// intercept maps that to a tool error rather than a panic.
    #[tokio::test]
    async fn message_subscribe_push_enabled_non_singleton_errors() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe_non_singleton().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 5, "retain_depth": 5 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        assert!(
            v["error"].as_str().unwrap_or("").contains("singleton"),
            "error must cite the singleton requirement: {v}"
        );
        // No subscriber folded into the directory on the rejected push-enabled sub.
        let entry = bridge
            .messenger()
            .unwrap()
            .directory()
            .resolve("brenn:test-channel")
            .expect("channel present");
        assert!(
            !entry.subscribers.iter().any(|s| matches!(
                &s.kind,
                brenn_lib::messaging::SubscriberEntryKind::App(slug) if slug == "testapp"
            )),
            "no subscriber folded for a rejected push-enabled subscribe: {entry:?}"
        );
    }

    /// Subscribing to a channel the app already has a *static* (config) sub on →
    /// ok:false; static subs are config-managed and unshadowable at runtime. (The
    /// combined-services fixture statically subscribes `testapp` to
    /// `brenn:test-channel`.)
    #[tokio::test]
    async fn message_subscribe_over_static_subscription_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 5 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        assert!(
            v["error"].as_str().unwrap_or("").contains("static"),
            "error must explain the static-sub conflict: {v}"
        );
    }

    /// `qos` supplied for a `brenn:` address → ok:false (qos is mqtt-only).
    #[tokio::test]
    async fn message_subscribe_qos_on_brenn_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 1, "qos": 1 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
    }

    /// Missing required `push_depth` → ok:false with a field-naming error.
    #[tokio::test]
    async fn message_subscribe_missing_push_depth_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "retain_depth": 1 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        assert!(
            v["error"].as_str().unwrap_or("").contains("push_depth"),
            "error must name push_depth: {v}"
        );
    }

    /// Missing required `retain_depth` → ok:false with a field-naming error
    /// (test-3, the complement of the missing-`push_depth` test; both are required
    /// and exercise the same `parse_depth_field` path).
    #[tokio::test]
    async fn message_subscribe_missing_retain_depth_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        assert!(
            v["error"].as_str().unwrap_or("").contains("retain_depth"),
            "error must name retain_depth: {v}"
        );
    }

    /// A present-but-invalid `noise` value → ok:false naming the valid options
    /// (test-4): the intercept rejects an unknown enum string rather than silently
    /// ignoring a caller mistake.
    #[tokio::test]
    async fn message_subscribe_invalid_noise_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 1, "noise": "loud" }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        assert!(
            v["error"].as_str().unwrap_or("").contains("noise"),
            "error must name the noise field: {v}"
        );
    }

    /// A present-but-invalid `wake_min` value → ok:false naming the field (test-4).
    #[tokio::test]
    async fn message_subscribe_invalid_wake_min_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 1, "wake_min": "always" }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        assert!(
            v["error"].as_str().unwrap_or("").contains("wake_min"),
            "error must name the wake_min field: {v}"
        );
    }

    /// Re-subscribe with identical params → idempotent no-op, status
    /// "already_subscribed", and no second durable row.
    #[tokio::test]
    async fn message_subscribe_resubscribe_identical_is_noop() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let input = json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 5 });

        let first = subscribe_result(&bridge, input.clone()).await;
        assert_eq!(first["status"], json!("subscribed"));

        let second = subscribe_result(&bridge, input).await;
        assert_eq!(second["ok"], json!(true), "no-op is success: {second}");
        assert_eq!(second["status"], json!("already_subscribed"));

        let rows = {
            let conn = bridge.messenger().unwrap().db().lock().await;
            brenn_lib::messaging::db::load_dynamic_subscriptions(&conn)
        };
        assert_eq!(rows.len(), 1, "no duplicate durable row");
    }

    /// Re-subscribe with different params → ok:false (must MessageUnsubscribe
    /// first); the original row is unchanged.
    #[tokio::test]
    async fn message_subscribe_resubscribe_differs_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 5 }),
        )
        .await;

        let v = subscribe_result(
            &bridge,
            json!({ "address": "brenn:test-channel", "push_depth": 0, "retain_depth": 99 }),
        )
        .await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        assert!(
            v["error"]
                .as_str()
                .unwrap_or("")
                .contains("MessageUnsubscribe"),
            "error must point at MessageUnsubscribe: {v}"
        );
    }

    /// `MessageChannelGet` with an `mqtt:` address reads persisted history through
    /// the transport-blind `Messenger::query()` (design §2.4 / §5: "`mqtt:`
    /// resolves ingress channel, returns persisted history"). Subscribe to create
    /// the `mqtt:` channel, inject one persisted ingress message, then query it.
    #[tokio::test]
    async fn message_channel_get_mqtt_address_returns_history() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "mqtt:home:home/kitchen/state";

        // Subscribe creates the mqtt: channel (filter never in TOML).
        let sub = subscribe_result(
            &bridge,
            json!({ "address": addr, "push_depth": 0, "retain_depth": 5 }),
        )
        .await;
        assert_eq!(sub["ok"], json!(true), "subscribe must succeed: {sub}");

        // Inject one persisted ingress message on that channel.
        let messenger = bridge.messenger().unwrap();
        let channel = messenger
            .directory()
            .resolve(addr)
            .expect("mqtt channel created by subscribe");
        let body = r#"{"client_slug":"home","topic":"home/kitchen/state","payload":{"text":"22.5"},"received_at":"2023-11-14T22:13:20Z","qos":1}"#;
        messenger
            .publish_transport_ingress(
                channel,
                addr,
                "home",
                body,
                brenn_lib::messaging::Urgency::Normal,
            )
            .await;

        // Query via the renamed MessageChannelGet tool; success returns a JSON
        // array of envelopes (not a {ok:...} object).
        let req = post_tool_use_req(
            MCP_MESSAGE_QUERY_CHANNEL_TOOL,
            json!({ "address": addr, "limit": 10 }),
        );
        let out = match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => out,
            other => panic!("unexpected MessageChannelGet result: {other:?}"),
        };
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid JSON");
        let envelopes = v.as_array().unwrap_or_else(|| {
            panic!("successful mqtt: MessageChannelGet must return a JSON array: {out}")
        });
        assert_eq!(envelopes.len(), 1, "one persisted message returned: {out}");
    }

    /// MessageChannelGet on a channel outside the app's ACL surfaces the
    /// unknown-channel tool error — the read-ACL deny path end-to-end through the
    /// LLM tool entry point. The test policy grants only mqtt `home`, so a `brenn:`
    /// read is denied by the gate and masked as unknown-channel (no existence leak).
    #[tokio::test]
    async fn message_channel_get_outside_acl_returns_unknown_channel() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let req = post_tool_use_req(
            MCP_MESSAGE_QUERY_CHANNEL_TOOL,
            json!({ "address": "brenn:not-mine", "limit": 10 }),
        );
        let out = match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => out,
            other => panic!("unexpected MessageChannelGet result: {other:?}"),
        };
        let v: serde_json::Value = serde_json::from_str(&out).expect("valid JSON");
        assert_eq!(v["ok"], json!(false), "denied read must be an error: {out}");
        assert!(
            v["error"]
                .as_str()
                .unwrap_or_default()
                .contains("unknown channel"),
            "denied read must surface unknown-channel: {out}"
        );
    }

    // -----------------------------------------------------------------------
    // MessageUnsubscribe intercept tests (design §2.4 / §5)
    // -----------------------------------------------------------------------

    /// Run the `MessageUnsubscribe` intercept and return the parsed JSON result.
    async fn unsubscribe_result(
        bridge: &crate::active_bridge::ActiveBridge,
        input: serde_json::Value,
    ) -> serde_json::Value {
        let req = post_tool_use_req(MCP_MESSAGE_UNSUBSCRIBE_TOOL, input);
        match try_handle_messaging_tool(bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => serde_json::from_str(&out).expect("valid JSON"),
            other => panic!("unexpected MessageUnsubscribe result: {other:?}"),
        }
    }

    /// PreToolUse for MessageUnsubscribe auto-approves (Allow).
    #[tokio::test]
    async fn message_unsubscribe_pre_tool_use_auto_approves() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let req = pre_tool_use_req(MCP_MESSAGE_UNSUBSCRIBE_TOOL);
        match try_handle_messaging_tool(&bridge, &req).await {
            Some(MessagingHandled::Respond(CcApprovalDecision::Allow { .. })) => {}
            other => panic!("expected Allow, got: {other:?}"),
        }
    }

    /// Unsubscribe a dynamic `brenn:` sub this app created → ok, status
    /// "unsubscribed", and the durable row + directory subscriber are removed.
    #[tokio::test]
    async fn message_unsubscribe_own_dynamic_brenn_sub_succeeds() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let addr = "brenn:test-channel";

        // Create the dynamic sub via the subscribe tool, then remove it.
        subscribe_result(
            &bridge,
            json!({ "address": addr, "push_depth": 0, "retain_depth": 5 }),
        )
        .await;

        let v = unsubscribe_result(&bridge, json!({ "address": addr })).await;
        assert_eq!(v["ok"], json!(true), "expected ok: {v}");
        assert_eq!(v["status"], json!("unsubscribed"));
        assert_eq!(v["address"], json!(addr));

        // Durable row gone.
        let rows = {
            let conn = bridge.messenger().unwrap().db().lock().await;
            brenn_lib::messaging::db::load_dynamic_subscriptions(&conn)
        };
        assert!(rows.is_empty(), "durable row removed");

        // Directory subscriber folded out.
        let entry = bridge
            .messenger()
            .unwrap()
            .directory()
            .resolve(addr)
            .expect("channel still present");
        assert!(
            !entry.subscribers.iter().any(|s| matches!(
                &s.kind,
                brenn_lib::messaging::SubscriberEntryKind::App(slug) if slug == "testapp"
            )),
            "subscriber folded out"
        );
    }

    /// Unsubscribe a channel the app has no dynamic sub on → ok:false.
    #[tokio::test]
    async fn message_unsubscribe_not_subscribed_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = unsubscribe_result(&bridge, json!({ "address": "brenn:test-channel" })).await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
    }

    /// Unsubscribe a channel the app only has a *static* (config) sub on →
    /// ok:false; static subs are config-managed and cannot be removed at runtime.
    /// (The combined-services fixture statically subscribes `testapp` to
    /// `brenn:test-channel`.)
    #[tokio::test]
    async fn message_unsubscribe_static_only_sub_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let v = unsubscribe_result(&bridge, json!({ "address": "brenn:test-channel" })).await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");

        // The static subscriber is left intact (structurally unreachable by the
        // unsubscribe path — it carries no durable dynamic row).
        let entry = bridge
            .messenger()
            .unwrap()
            .directory()
            .resolve("brenn:test-channel")
            .expect("channel present");
        assert!(
            entry.subscribers.iter().any(|s| matches!(
                &s.kind,
                brenn_lib::messaging::SubscriberEntryKind::App(slug) if slug == "testapp"
            )),
            "static subscriber untouched"
        );
    }

    /// Missing required `address` → ok:false.
    #[tokio::test]
    async fn message_unsubscribe_missing_address_errors() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_mqtt_subscribe().await;
        let v = unsubscribe_result(&bridge, json!({})).await;
        assert_eq!(v["ok"], json!(false), "expected error: {v}");
        assert!(
            v["error"].as_str().unwrap_or("").contains("address"),
            "error must name address: {v}"
        );
    }
}

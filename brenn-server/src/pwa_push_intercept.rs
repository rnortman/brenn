//! Intercept handlers for pwa_push MCP virtual tools.
//!
//! Two tools are intercepted:
//! - `mcp__brenn__PwaPushSend` — auto-approved at PreToolUse; PostToolUse runs
//!   `PwaPushService::send()` and returns `Continue { updated_output }`.
//! - `mcp__brenn__PwaPushChannelGet` — auto-approved at PreToolUse; PostToolUse
//!   returns per-target detail for the named pwa_push address.
//!
//! Pattern mirrors `messaging_intercept.rs`.

use std::borrow::Cow;
use std::sync::Arc;

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_lib::pwa_push::publish::{GetTargetResult, PushSendResult, Urgency};
use serde::Serialize;

use crate::active_bridge::ActiveBridge;
use crate::intercept_helpers::{ToolErr, reject_tool, warn_if_unexpected_tool_response};
use crate::tools::pwa_push::{MCP_PUSH_LIST_TARGETS_TOOL, MCP_PUSH_SEND_TOOL};

// ---------------------------------------------------------------------------
// Typed tool-response structs (C3)
// ---------------------------------------------------------------------------

/// Success response for `PwaPushChannelGet`.
///
/// Fields alphabetical: address, device, last_seen_at, user — matches serde_json
/// BTreeMap sort order for byte-identical output vs the source `json!` macro.
/// No `ok` field (distinct shape from error).
/// `device` is `Option<&str>` because `PushTargetEntry::device` is `Option<String>`;
/// `serde_json` serializes `None` as `null` (key present), matching the `json!` macro behavior.
#[derive(Serialize)]
struct PwaPushChannelGetOk<'a> {
    // alphabetical: address, device, last_seen_at, user
    address: &'a str,
    device: Option<&'a str>,
    last_seen_at: &'a str,
    user: &'a str,
}

/// Success response for `PwaPushSend`.
///
/// Fields alphabetical: address, attempted, delivered, failed, failed_invalid_endpoint,
/// failed_stale_user, gone, message_id, ok, remaining_budget.
/// Numeric fields use `u32` to match `PushSendResult::Ok` field types.
#[derive(Serialize)]
struct PwaPushSendOk {
    // alphabetical
    address: String,
    attempted: u32,
    delivered: u32,
    failed: u32,
    failed_invalid_endpoint: u32,
    failed_stale_user: u32,
    gone: u32,
    message_id: String,
    ok: bool,
    remaining_budget: u32,
}

/// Outcome of `try_handle_pwa_push_tool`. `None` means "not a pwa_push tool".
#[derive(Debug)]
pub enum PwaPushHandled {
    /// Send this decision back to CC.
    Respond(CcApprovalDecision),
}

/// Try to handle a pwa_push tool intercept. Returns `Some` only when the
/// tool name matches `mcp__brenn__PwaPushSend` or `mcp__brenn__PwaPushChannelGet`.
pub async fn try_handle_pwa_push_tool(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<PwaPushHandled> {
    match &req.kind {
        // ---- PreToolUse: both tools auto-approve ----
        ApprovalKind::PreToolUse { tool_name, .. }
            if tool_name == MCP_PUSH_SEND_TOOL || tool_name == MCP_PUSH_LIST_TARGETS_TOOL =>
        {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // ---- PostToolUse: PwaPushChannelGet ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_PUSH_LIST_TARGETS_TOOL => {
            warn_if_unexpected_tool_response("pwa_push intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;

            // Extract required `address` — kept as `&str` through the cross-protocol
            // guards to avoid a clone that is immediately discarded on the redirect path.
            // Converted to `String` only when an `.await` forces ownership.
            let address = match tool_input.get("address").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s,
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

            // Cross-protocol misuse: brenn: addresses must go to MessageChannelGet.
            if brenn_lib::messaging::ChannelScheme::of(address)
                == Some(brenn_lib::messaging::ChannelScheme::Brenn)
            {
                tracing::debug!(
                    tool = tool_name,
                    address = %address,
                    "PwaPushChannelGet called with brenn: address; redirecting LLM"
                );
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "PwaPushChannelGet only accepts `pwa_push:` addresses. \
                         Use MessageChannelGet for `brenn:` addresses. \
                         Use MessageChannelList to discover available channels.",
                    )
                    .await,
                );
            }

            let svc = match bridge.pwa_push_service() {
                Some(s) => s,
                None => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "pwa_push is not configured on this server",
                        )
                        .await,
                    );
                }
            };

            // Parse address first; query only the requested target — O(1) for
            // device addresses, O(user-subs) for user fan-out — instead of
            // scanning all subscriptions server-wide.
            let parsed_addr = match brenn_lib::pwa_push::targets::parse_pwa_push_address(address) {
                Ok(a) => a,
                Err(e) => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            &format!("invalid pwa_push address: {e}"),
                        )
                        .await,
                    );
                }
            };

            let target = svc.get_target(&bridge.app_slug, &parsed_addr).await;
            let output_str = match target {
                GetTargetResult::Found(t) => {
                    let ok = PwaPushChannelGetOk {
                        address: &t.address,
                        device: t.device.as_deref(),
                        last_seen_at: &t.last_seen_at,
                        user: &t.user,
                    };
                    serde_json::to_string(&ok)
                        .expect("PwaPushChannelGetOk serialization is infallible")
                }
                GetTargetResult::Forbidden => {
                    tracing::warn!(
                        address = %address,
                        app = %bridge.app_slug,
                        "PwaPushChannelGet: access denied"
                    );
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "access denied; the user is not in this app's allowed_users list",
                        ),
                    };
                    serde_json::to_string(&err).expect("ToolErr serialization is infallible")
                }
                GetTargetResult::Disabled => {
                    tracing::debug!(
                        address = %address,
                        app = %bridge.app_slug,
                        "PwaPushChannelGet: pwa_push disabled for app"
                    );
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed("pwa_push is disabled for this app"),
                    };
                    serde_json::to_string(&err).expect("ToolErr serialization is infallible")
                }
                GetTargetResult::NotFound => {
                    tracing::debug!(
                        address = %address,
                        app = %bridge.app_slug,
                        "PwaPushChannelGet: no target found for address"
                    );
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "no pwa_push target found; use MessageChannelList to see available targets",
                        ),
                    };
                    serde_json::to_string(&err).expect("ToolErr serialization is infallible")
                }
            };

            crate::active_bridge::emit_tool_summary_for_intercept(
                bridge, tool_name, tool_input, false,
            )
            .await;
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        // ---- PostToolUse: PwaPushSend ----
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            tool_response,
            ..
        } if tool_name == MCP_PUSH_SEND_TOOL => {
            warn_if_unexpected_tool_response("pwa_push intercept", tool_name, tool_response);
            crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;

            // Validate all inputs first (service check comes after, so argument
            // errors surface cleanly regardless of server configuration).

            // Extract required `address` — kept as `&str` through all cross-protocol
            // guards; cloned to String only at the `svc.send(…).await` call site.
            let address = match tool_input.get("address").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s,
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

            // Cross-protocol misuse: brenn: addresses must go to BrennSend.
            if brenn_lib::messaging::ChannelScheme::of(address)
                == Some(brenn_lib::messaging::ChannelScheme::Brenn)
            {
                tracing::debug!(
                    tool = tool_name,
                    address = %address,
                    "PwaPushSend called with brenn: address; redirecting LLM to BrennSend"
                );
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "PwaPushSend only accepts `pwa_push:` addresses. \
                         Use BrennSend for `brenn:` addresses. \
                         Use MessageChannelList to discover available channels.",
                    )
                    .await,
                );
            }
            // Cross-protocol misuse: mqtt: addresses must go to MqttSend.
            if brenn_lib::messaging::ChannelScheme::of(address)
                == Some(brenn_lib::messaging::ChannelScheme::Mqtt)
            {
                tracing::debug!(
                    tool = tool_name,
                    address = %address,
                    "PwaPushSend called with mqtt: address; redirecting LLM to MqttSend"
                );
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "PwaPushSend only accepts `pwa_push:` addresses. \
                         Use MqttSend for `mqtt:` addresses. \
                         Use MessageChannelList to discover available channels.",
                    )
                    .await,
                );
            }
            // Cross-protocol misuse: webhook: addresses are inbound-only.
            if brenn_lib::messaging::ChannelScheme::of(address)
                == Some(brenn_lib::messaging::ChannelScheme::Webhook)
            {
                tracing::debug!(
                    tool = tool_name,
                    address = %address,
                    "PwaPushSend called with webhook: address; webhook: is inbound-only in MVP"
                );
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "PwaPushSend only accepts `pwa_push:` addresses. \
                         `webhook:` channels are inbound-only in this version. \
                         Use MessageChannelList to discover available channels.",
                    )
                    .await,
                );
            }

            // Extract required `body` (empty string is allowed per design §3 edge case 20).
            let body = match tool_input.get("body").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
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

            // Extract optional fields.
            let title = tool_input
                .get("title")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let ttl_seconds = tool_input
                .get("ttl_seconds")
                .and_then(|v| v.as_u64())
                .map(|n| n.min(u64::from(brenn_lib::pwa_push::publish::MAX_TTL_SECONDS)) as u32)
                .unwrap_or(86400);
            let urgency_str = tool_input
                .get("urgency")
                .and_then(|v| v.as_str())
                .unwrap_or("normal");
            let urgency = match Urgency::parse(urgency_str) {
                Some(u) => u,
                None => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "unknown `urgency` value; must be one of very-low, low, normal, high",
                        )
                        .await,
                    );
                }
            };
            let topic = tool_input
                .get("topic")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            // RFC 8030 §5.4: topic must be ≤ 32 chars, URL-safe base64 alphabet,
            // and non-empty (empty topic would inject a spurious `Topic: ` header).
            if topic.as_deref().is_some_and(|t| {
                t.is_empty()
                    || t.len() > 32
                    || !t
                        .chars()
                        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
            }) {
                return Some(
                    tool_error_response(
                        bridge,
                        tool_name,
                        tool_input,
                        "invalid `topic`: must be ≤ 32 chars of URL-safe base64 alphabet \
                         (A-Za-z0-9, -, _)",
                    )
                    .await,
                );
            }
            let tag = tool_input
                .get("tag")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            // Normalize and validate `data` / `data.url`.
            // The injected URL is always a redirector URL (/r/{uuid}?to={encoded_app_path})
            // so that Android Firefox's task router sees a per-click-unique navigation
            // rather than a back-stack restore. See design §2.2.
            let default_app_path = format!("/app/{}/c/{}", bridge.app_slug, bridge.conversation_id);
            let default_data_url = make_redirector_url(&default_app_path);
            let data = match tool_input.get("data") {
                // Absent or JSON null → inject default URL.
                None | Some(serde_json::Value::Null) => {
                    tracing::debug!(tool = tool_name, "data.url auto-populated (data absent)");
                    let mut map = serde_json::Map::new();
                    map.insert(
                        "url".to_string(),
                        serde_json::Value::String(default_data_url),
                    );
                    Some(map)
                }
                // Not an object → reject.
                Some(v) if !v.is_object() => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "invalid `data`: must be a JSON object",
                        )
                        .await,
                    );
                }
                // Object — check/inject `url` key. Validate type against the
                // original reference first; clone only on the success path.
                Some(serde_json::Value::Object(obj)) => {
                    // Reject before cloning if `url` is present but not a string.
                    if matches!(obj.get("url"), Some(v) if !v.is_string()) {
                        return Some(
                            tool_error_response(
                                bridge,
                                tool_name,
                                tool_input,
                                "invalid `data.url`: must be a string",
                            )
                            .await,
                        );
                    }
                    // Validation passed — clone now.
                    let mut map = obj.clone();
                    match map.get("url") {
                        // No `url` key → inject default, preserve other keys.
                        None => {
                            tracing::debug!(
                                tool = tool_name,
                                "data.url auto-populated (url key absent)"
                            );
                            map.insert(
                                "url".to_string(),
                                serde_json::Value::String(default_data_url),
                            );
                        }
                        // `url` is a string — validate format, require /app/ prefix,
                        // wrap in redirector.
                        Some(serde_json::Value::String(raw_url)) => {
                            match validate_and_wrap_data_url(raw_url) {
                                Ok(redirector_url) => {
                                    map.insert(
                                        "url".to_string(),
                                        serde_json::Value::String(redirector_url),
                                    );
                                }
                                Err(msg) => {
                                    return Some(
                                        tool_error_response(bridge, tool_name, tool_input, msg)
                                            .await,
                                    );
                                }
                            }
                        }
                        Some(v) => {
                            unreachable!("non-string url rejected before clone; value={v:?}")
                        }
                    }
                    Some(map)
                }
                Some(v) => unreachable!("non-object handled above; value={v:?}"),
            };

            // All inputs validated — now check the service is present.
            let svc = match bridge.pwa_push_service() {
                Some(s) => s,
                None => {
                    return Some(
                        tool_error_response(
                            bridge,
                            tool_name,
                            tool_input,
                            "pwa_push is not configured on this server",
                        )
                        .await,
                    );
                }
            };

            let result = Arc::clone(svc)
                .send(
                    bridge.conversation_id,
                    &bridge.app_slug,
                    address,
                    &body,
                    title.as_deref(),
                    ttl_seconds,
                    urgency,
                    topic.as_deref(),
                    tag.as_deref(),
                    data,
                )
                .await;

            let (outcome_value, is_error) = match result {
                PushSendResult::Ok {
                    message_uuid,
                    address: canonical_addr,
                    delivered,
                    gone,
                    failed,
                    failed_stale_user,
                    failed_invalid_endpoint,
                    remaining_budget,
                } => {
                    let ok = PwaPushSendOk {
                        address: canonical_addr,
                        attempted: delivered
                            + gone
                            + failed
                            + failed_stale_user
                            + failed_invalid_endpoint,
                        delivered,
                        failed,
                        failed_invalid_endpoint,
                        failed_stale_user,
                        gone,
                        message_id: message_uuid.to_string(),
                        ok: true,
                        remaining_budget,
                    };
                    (
                        serde_json::to_value(&ok)
                            .expect("PwaPushSendOk serialization is infallible"),
                        false,
                    )
                }
                PushSendResult::MissingSender => {
                    // Covers two producer branches in pwa_push::publish:
                    // (a) channel-disabled-via-config (!pwa_push_enabled()) and
                    // (b) unknown-app routing bug (apps.get() returned None).
                    // Both return the same variant; branch (b) also emits a
                    // tracing::warn! inside the library. If on-call sees this
                    // error with no obvious config reason, check the library-level
                    // warn! log for the routing-bug indicator.
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed("push is not enabled for this app"),
                    };
                    (
                        serde_json::to_value(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                PushSendResult::BodyTooLarge { len, max } => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!("body too large: {len} bytes (max {max})")),
                    };
                    (
                        serde_json::to_value(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                PushSendResult::BudgetExhausted => {
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
                PushSendResult::Forbidden { .. } => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Borrowed(
                            "forbidden: the target user is not in this app's allowed_users list",
                        ),
                    };
                    (
                        serde_json::to_value(&err).expect("ToolErr serialization is infallible"),
                        true,
                    )
                }
                PushSendResult::MalformedAddress(addr) => {
                    let err = ToolErr {
                        ok: false,
                        error: Cow::Owned(format!(
                            "malformed address {addr:?}: must be \
                             pwa_push:<user> or pwa_push:<user>@<device>"
                        )),
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

            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output_str),
            }))
        }

        _ => None,
    }
}

/// Construct a per-click-unique redirector URL for `data.url`.
///
/// Produces `/r/{uuid}?to={percent-encoded-app-path}` where the `to` parameter
/// is safe to round-trip through `serde_urlencoded` query extraction. The UUID
/// is a fresh v4 per call — no server-side tracking needed; uniqueness alone
/// defeats Android Firefox's task-restore behavior. See design §2.2.
fn make_redirector_url(app_path: &str) -> String {
    let nonce = uuid::Uuid::new_v4();
    let mut s = url::form_urlencoded::Serializer::new(String::new());
    s.append_pair("to", app_path);
    let qs = s.finish();
    format!("/r/{nonce}?{qs}")
}

/// Validate a caller-supplied `data.url`, require it to start with `/app/`,
/// and wrap it in a redirector URL.
///
/// Rejects:
/// - Anything [`crate::path_validate::validate_same_origin_path`] rejects.
/// - Paths that do not start with `/app/` (e.g. `/r/...`, `/health`).
///
/// Returns the redirector URL on success, or a static error string on failure.
fn validate_and_wrap_data_url(raw: &str) -> Result<String, &'static str> {
    let canonical = crate::path_validate::validate_app_path(raw)
        .map_err(|_| "invalid `data.url`: must be an `/app/` path, e.g. `/app/graf/c/42`")?;
    Ok(make_redirector_url(&canonical))
}

/// Common shape for "tool input failed validation" responses.
async fn tool_error_response(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
    error: &str,
) -> PwaPushHandled {
    PwaPushHandled::Respond(
        reject_tool(bridge, "pwa_push tool", tool_name, tool_input, error).await,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::active_bridge::test_support::{post_tool_use_req, pre_tool_use_req};

    // -----------------------------------------------------------------------
    // MockPwaPushSender — captures send() args for injection-assertion tests.
    // -----------------------------------------------------------------------

    /// Arguments captured by `MockPwaPushSender::send`.
    struct CapturedSendArgs {
        data: Option<serde_json::Map<String, serde_json::Value>>,
        address: String,
        body: String,
    }

    struct MockPwaPushSender {
        captured_send_args: Mutex<Option<CapturedSendArgs>>,
        endpoint_policy: brenn_lib::pwa_push::endpoint_validator::EndpointPolicy,
    }

    impl MockPwaPushSender {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                captured_send_args: Mutex::new(None),
                endpoint_policy: brenn_lib::pwa_push::endpoint_validator::EndpointPolicy::new(
                    vec![],
                    false,
                ),
            })
        }
    }

    #[async_trait::async_trait]
    impl brenn_lib::pwa_push::PwaPushSender for MockPwaPushSender {
        async fn send(
            self: Arc<Self>,
            _sender_conversation_id: i64,
            _sender_app_slug: &str,
            address: &str,
            body: &str,
            _title: Option<&str>,
            _ttl_seconds: u32,
            _urgency: brenn_lib::pwa_push::publish::Urgency,
            _topic: Option<&str>,
            _tag: Option<&str>,
            data: Option<serde_json::Map<String, serde_json::Value>>,
        ) -> brenn_lib::pwa_push::publish::PushSendResult {
            *self.captured_send_args.lock().await = Some(CapturedSendArgs {
                data,
                address: address.to_string(),
                body: body.to_string(),
            });
            brenn_lib::pwa_push::publish::PushSendResult::Ok {
                message_uuid: uuid::Uuid::nil(),
                address: address.to_string(),
                delivered: 0,
                gone: 0,
                failed: 0,
                failed_stale_user: 0,
                failed_invalid_endpoint: 0,
                remaining_budget: u32::MAX,
            }
        }

        async fn get_target(
            &self,
            _app_slug: &str,
            _parsed_addr: &brenn_lib::pwa_push::targets::PwaPushAddress,
        ) -> brenn_lib::pwa_push::publish::GetTargetResult {
            brenn_lib::pwa_push::publish::GetTargetResult::NotFound
        }

        async fn list_targets(
            &self,
            _app_slug: &str,
        ) -> Vec<brenn_lib::pwa_push::publish::PushTargetEntry> {
            vec![]
        }

        fn public_key_b64url(&self) -> &str {
            "mock-vapid-key"
        }

        fn endpoint_policy(&self) -> &brenn_lib::pwa_push::endpoint_validator::EndpointPolicy {
            &self.endpoint_policy
        }
    }

    /// Create a bridge with a `MockPwaPushSender` injected.
    async fn bridge_with_mock_push() -> (
        Arc<crate::active_bridge::ActiveBridge>,
        Arc<MockPwaPushSender>,
    ) {
        let mock = MockPwaPushSender::new();
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push_with_service(
            Arc::clone(&mock) as Arc<dyn brenn_lib::pwa_push::PwaPushSender>,
        )
        .await;
        (bridge, mock)
    }

    /// Parse the `to` query parameter from a redirector URL `/r/{uuid}?to=...`.
    /// Panics if the URL does not have the expected form or if the path segment
    /// after `/r/` is not a valid UUID.
    fn extract_to_param(redirector_url: &str) -> String {
        assert!(
            redirector_url.starts_with("/r/"),
            "redirector URL must start with /r/: {redirector_url}"
        );
        // Validate the UUID segment: everything between "/r/" and the "?".
        let after_r = &redirector_url[3..]; // skip "/r/"
        let uuid_end = after_r.find('?').expect("redirector URL must contain '?'");
        let uuid_str = &after_r[..uuid_end];
        uuid::Uuid::parse_str(uuid_str)
            .unwrap_or_else(|_| panic!("segment after /r/ must be a valid UUID, got: {uuid_str}"));
        let qs = &after_r[uuid_end + 1..];
        url::form_urlencoded::parse(qs.as_bytes())
            .find(|(k, _)| k == "to")
            .map(|(_, v)| v.into_owned())
            .expect("redirector URL must have 'to' query parameter")
    }

    #[tokio::test]
    async fn pre_tool_use_push_send_auto_approves() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = pre_tool_use_req(MCP_PUSH_SEND_TOOL);
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            })) => {}
            other => panic!("expected Allow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pre_tool_use_push_list_targets_auto_approves() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = pre_tool_use_req(MCP_PUSH_LIST_TARGETS_TOOL);
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            })) => {}
            other => panic!("expected Allow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pre_tool_use_unrelated_tool_returns_none() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = pre_tool_use_req("mcp__brenn__SomethingElse");
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        assert!(result.is_none(), "should return None for unrelated tool");
    }

    #[tokio::test]
    async fn post_tool_use_push_send_no_service_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        // Bridge has no pwa_push_service (None).
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({ "address": "pwa_push:test", "body": "hi" }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(
                    v["error"].as_str().unwrap().contains("not configured"),
                    "error: {}",
                    v["error"]
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn post_tool_use_push_send_missing_address_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({ "body": "hi" }), // no address
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(v["error"].as_str().unwrap().contains("address"));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn post_tool_use_push_send_missing_body_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({ "address": "pwa_push:test" }), // no body
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(v["error"].as_str().unwrap().contains("body"));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn post_tool_use_push_send_invalid_urgency_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "urgency": "ultra-high",
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(err.contains("urgency"));
                assert!(
                    !err.contains("ultra-high"),
                    "error must not echo urgency input: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn post_tool_use_push_send_invalid_topic_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "topic": "this topic is way too long to be valid for push services",
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(v["error"].as_str().unwrap().contains("topic"));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// PwaPushSend with a brenn: address → is_error, mentions BrennSend and
    /// MessageChannelList. The check fires before the service lookup.
    #[tokio::test]
    async fn pwa_push_send_with_brenn_address_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({ "address": "brenn:my-channel", "body": "hello" }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("BrennSend"),
                    "error should mention BrennSend: {err}"
                );
                assert!(
                    err.contains("MessageChannelList"),
                    "error should mention MessageChannelList: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// PwaPushChannelGet with a brenn: address → is_error, mentions MessageChannelGet
    /// and MessageChannelList.
    #[tokio::test]
    async fn pwa_push_channel_get_with_brenn_address_returns_error() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_LIST_TARGETS_TOOL,
            json!({ "address": "brenn:foo" }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be error: {out}");
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("MessageChannelGet"),
                    "error should mention MessageChannelGet: {err}"
                );
                assert!(
                    err.contains("MessageChannelList"),
                    "error should mention MessageChannelList: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // ---------------------------------------------------------------------------
    // data.url injection tests — use MockPwaPushSender to observe captured data
    // ---------------------------------------------------------------------------

    /// No `data` arg → injected `data["url"]` is a redirector wrapping `/app/testapp/c/<conv_id>`.
    #[tokio::test]
    async fn pwa_push_send_injects_default_url_when_data_absent() {
        let (bridge, mock) = bridge_with_mock_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({ "address": "pwa_push:test", "body": "hi" }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        // Must succeed (mock returns Ok).
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "expected ok from mock: {out}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        // Verify the captured data map and forwarded arguments.
        let guard = mock.captured_send_args.lock().await;
        let args = guard.as_ref().expect("send must have been called");
        assert_eq!(args.address, "pwa_push:test", "address must be forwarded");
        assert_eq!(args.body, "hi", "body must be forwarded");
        let data = args.data.as_ref().expect("data must be Some");
        let url = data
            .get("url")
            .and_then(|v| v.as_str())
            .expect("data must contain string url");
        let to = extract_to_param(url);
        let expected = format!("/app/testapp/c/{}", bridge.conversation_id);
        assert_eq!(to, expected, "decoded `to` must equal default app path");
    }

    /// `data: {{}}` (object, no url key) → default url injected, other keys preserved.
    #[tokio::test]
    async fn pwa_push_send_injects_default_url_when_data_omits_url() {
        let (bridge, mock) = bridge_with_mock_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({ "address": "pwa_push:test", "body": "hi", "data": { "foo": "bar" } }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "expected ok from mock: {out}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        let guard = mock.captured_send_args.lock().await;
        let args = guard.as_ref().expect("send must have been called");
        let data = args.data.as_ref().expect("data must be Some");
        // Injected url.
        let url = data
            .get("url")
            .and_then(|v| v.as_str())
            .expect("data must contain string url");
        assert!(url.starts_with("/r/"), "url must be redirector: {url}");
        let to = extract_to_param(url);
        let expected = format!("/app/testapp/c/{}", bridge.conversation_id);
        assert_eq!(to, expected, "decoded `to` must equal default app path");
        // Original key preserved.
        assert_eq!(
            data.get("foo").and_then(|v| v.as_str()),
            Some("bar"),
            "original data key 'foo' must be preserved"
        );
    }

    /// `data: null` (JSON null) → treated as absent → default url injected.
    #[tokio::test]
    async fn pwa_push_send_treats_null_data_as_absent() {
        let (bridge, mock) = bridge_with_mock_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({ "address": "pwa_push:test", "body": "hi", "data": null }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "expected ok from mock: {out}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        let guard = mock.captured_send_args.lock().await;
        let args = guard.as_ref().expect("send must have been called");
        let data = args.data.as_ref().expect("data must be Some");
        let url = data
            .get("url")
            .and_then(|v| v.as_str())
            .expect("data must contain string url");
        assert!(url.starts_with("/r/"), "url must be redirector: {url}");
        let to = extract_to_param(url);
        let expected = format!("/app/testapp/c/{}", bridge.conversation_id);
        assert_eq!(to, expected, "decoded `to` must equal default app path");
    }

    /// `data.url = "/app/x/c/9"` (valid app path) → caller url wrapped in redirector.
    #[tokio::test]
    async fn pwa_push_send_valid_app_data_url_reaches_service() {
        let (bridge, mock) = bridge_with_mock_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({ "address": "pwa_push:test", "body": "hi", "data": { "url": "/app/x/c/9" } }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "expected ok from mock: {out}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        let guard = mock.captured_send_args.lock().await;
        let args = guard.as_ref().expect("send must have been called");
        let data = args.data.as_ref().expect("data must be Some");
        let url = data
            .get("url")
            .and_then(|v| v.as_str())
            .expect("data must contain string url");
        assert!(url.starts_with("/r/"), "url must be redirector: {url}");
        let to = extract_to_param(url);
        assert_eq!(
            to, "/app/x/c/9",
            "decoded `to` must equal caller-supplied path"
        );
    }

    /// `data.url = "/r/abc-def?to=/app/x/c/9"` (caller-supplied redirector URL)
    /// → rejected with "must be an `/app/` path".
    #[tokio::test]
    async fn pwa_push_send_rejects_caller_supplied_redirector_url() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "data": { "url": "/r/abc-def?to=/app/x/c/9" }
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("/app/"),
                    "error should mention /app/ requirement: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `data.url = "/health"` (non-app same-origin path) → rejected with
    /// "must be an `/app/` path".
    #[tokio::test]
    async fn pwa_push_send_rejects_non_app_data_url() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "data": { "url": "/health" }
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("/app/"),
                    "error should mention /app/ requirement: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// Producer-side encoding: `data.url = "/app/x/c/9?foo=bar#z"` is wrapped
    /// in a redirector URL whose `to=` parameter percent-encodes `?`/`#` so
    /// the decoded `to` round-trips through `serde_urlencoded`.
    #[tokio::test]
    async fn pwa_push_send_wraps_data_url_with_query_and_fragment() {
        // make_redirector_url must percent-encode `?` and `#` inside `to`.
        let app_path = "/app/x/c/9?foo=bar#z";
        let redirector_url = make_redirector_url(app_path);

        // Verify the `to` query param round-trips: UUID segment is validated
        // and the decoded `to` equals the original app_path.
        let to = extract_to_param(&redirector_url);
        assert_eq!(to, app_path, "decoded `to` must equal original app_path");
    }

    /// `data.url = "https://evil.example/"` → tool error.
    #[tokio::test]
    async fn pwa_push_send_rejects_absolute_data_url() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "data": { "url": "https://evil.example/" }
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(
                    v["error"].as_str().unwrap().contains("data.url"),
                    "error should mention data.url: {v}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `data.url = "//evil.example/x"` (protocol-relative) → tool error.
    #[tokio::test]
    async fn pwa_push_send_rejects_protocol_relative_data_url() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "data": { "url": "//evil.example/x" }
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(
                    v["error"].as_str().unwrap().contains("data.url"),
                    "error should mention data.url: {v}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `data.url = 42` (non-string) → tool error.
    #[tokio::test]
    async fn pwa_push_send_rejects_non_string_data_url() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "data": { "url": 42 }
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(
                    v["error"].as_str().unwrap().contains("data.url"),
                    "error should mention data.url: {v}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `data = ["x"]` (array, not object) → tool error.
    #[tokio::test]
    async fn pwa_push_send_rejects_data_not_object() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "data": ["x"]
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(
                    v["error"].as_str().unwrap().contains("`data`"),
                    "error should mention data: {v}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `data = "foo"` (scalar string, not object) → tool error (complements array test).
    #[tokio::test]
    async fn pwa_push_send_rejects_data_scalar_string() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "data": "foo"
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(
                    v["error"].as_str().unwrap().contains("`data`"),
                    "error should mention data: {v}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    /// `data.url = "/app/graf/c/9?foo=bar#x"` → query and fragment preserved through full pipeline.
    #[tokio::test]
    async fn pwa_push_send_preserves_query_and_fragment_in_data_url() {
        let (_bridge, mock) = bridge_with_mock_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "data": { "url": "/app/graf/c/9?foo=bar#x" }
            }),
        );
        let result = try_handle_pwa_push_tool(&_bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(true), "expected ok from mock: {out}");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        let guard = mock.captured_send_args.lock().await;
        let args = guard.as_ref().expect("send must have been called");
        let data = args.data.as_ref().expect("data must be Some");
        let url = data
            .get("url")
            .and_then(|v| v.as_str())
            .expect("data must contain string url");
        let to = extract_to_param(url);
        assert_eq!(
            to, "/app/graf/c/9?foo=bar#x",
            "decoded `to` must preserve query and fragment"
        );
    }

    /// `data.url = "/app/../etc"` → dot-dot rejected.
    #[tokio::test]
    async fn pwa_push_send_rejects_dot_dot_in_data_url() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "pwa_push:test",
                "body": "hi",
                "data": { "url": "/app/../etc" }
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(
                    v["error"].as_str().unwrap().contains("data.url"),
                    "error should mention data.url: {v}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Wire-shape regression guards for C3 typed structs
    // -----------------------------------------------------------------------

    /// `PwaPushChannelGetOk` must serialize byte-identically to the source `json!`.
    #[test]
    fn pwa_push_channel_get_ok_matches_reference() {
        // With device name present.
        let ok = super::PwaPushChannelGetOk {
            address: "pwa_push:alice@laptop",
            user: "alice",
            device: Some("laptop"),
            last_seen_at: "2026-05-18T09:00:00+00:00",
        };
        let produced =
            serde_json::to_string(&ok).expect("PwaPushChannelGetOk serialization is infallible");
        let reference = serde_json::json!({
            "address": "pwa_push:alice@laptop",
            "user": "alice",
            "device": "laptop",
            "last_seen_at": "2026-05-18T09:00:00+00:00",
        })
        .to_string();
        assert_eq!(produced, reference);

        // With no device name (None → null).
        let ok2 = super::PwaPushChannelGetOk {
            address: "pwa_push:alice",
            user: "alice",
            device: None,
            last_seen_at: "2026-05-18T09:00:00+00:00",
        };
        let produced2 =
            serde_json::to_string(&ok2).expect("PwaPushChannelGetOk serialization is infallible");
        let reference2 = serde_json::json!({
            "address": "pwa_push:alice",
            "user": "alice",
            "device": null,
            "last_seen_at": "2026-05-18T09:00:00+00:00",
        })
        .to_string();
        assert_eq!(produced2, reference2);
    }

    /// `PwaPushSendOk` must serialize byte-identically to the source `json!`.
    #[test]
    fn pwa_push_send_ok_matches_reference() {
        let ok = super::PwaPushSendOk {
            ok: true,
            message_id: "00000000-0000-0000-0000-000000000003".to_string(),
            address: "pwa_push:alice".to_string(),
            attempted: 2,
            delivered: 1,
            gone: 0,
            failed: 0,
            failed_stale_user: 1,
            failed_invalid_endpoint: 0,
            remaining_budget: 50,
        };
        let produced =
            serde_json::to_string(&ok).expect("PwaPushSendOk serialization is infallible");
        let reference = serde_json::json!({
            "ok": true,
            "message_id": "00000000-0000-0000-0000-000000000003",
            "address": "pwa_push:alice",
            "attempted": 2_u32,
            "delivered": 1_u32,
            "gone": 0_u32,
            "failed": 0_u32,
            "failed_stale_user": 1_u32,
            "failed_invalid_endpoint": 0_u32,
            "remaining_budget": 50_u32,
        })
        .to_string();
        assert_eq!(produced, reference);
    }

    // Note: validate_data_url unit tests have moved to crate::path_validate::tests.
    // The function is now a thin wrapper; coverage lives in path_validate.rs.

    /// test-7: PwaPushSend with an mqtt: address → is_error, mentions MqttSend (cross-protocol guard).
    #[tokio::test]
    async fn pwa_push_send_mqtt_address_redirects_to_mqtt_send() {
        let bridge = crate::active_bridge::ActiveBridge::test_new_for_pwa_push().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({
                "address": "mqtt:ha:home/cmnd/tasmota/power",
                "title": "ignored",
                "body": "ignored",
                "data": { "url": "/app/" }
            }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
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

    /// PwaPushChannelGet with a valid pwa_push: address that has no matching
    /// subscription → is_error, error mentions MessageChannelList (test-1).
    #[tokio::test]
    async fn pwa_push_channel_get_address_not_found_returns_error() {
        // test_new_with_combined_services has a real PwaPushService + seeds
        // alice@laptop. "nobody" is not present, so the lookup returns not-found.
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        let req = post_tool_use_req(
            MCP_PUSH_LIST_TARGETS_TOOL,
            json!({ "address": "pwa_push:nobody" }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false), "should be not-found error: {out}");
                let err = v["error"].as_str().unwrap();
                assert!(
                    err.contains("MessageChannelList"),
                    "not-found error should mention MessageChannelList: {err}"
                );
                assert!(
                    !err.contains("nobody"),
                    "error must not echo the address: {err}"
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn pwa_push_channel_get_invalid_address_returns_error() {
        // A malformed address (not a brenn: prefix, not a valid pwa_push: address)
        // should return a parse error, not a not-found error.
        let bridge = crate::active_bridge::ActiveBridge::test_new_with_combined_services().await;
        for bad_address in &["pwa_push:", "not-valid-at-all", "pwa_push:@"] {
            let req = post_tool_use_req(
                MCP_PUSH_LIST_TARGETS_TOOL,
                json!({ "address": bad_address }),
            );
            let result = try_handle_pwa_push_tool(&bridge, &req).await;
            match result {
                Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                    updated_output: Some(out),
                })) => {
                    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                    assert_eq!(
                        v["ok"],
                        json!(false),
                        "invalid address should return error for {bad_address}: {out}"
                    );
                    let err = v["error"].as_str().unwrap();
                    assert!(
                        err.contains("invalid pwa_push address"),
                        "error should mention 'invalid pwa_push address' for {bad_address}: {err}"
                    );
                }
                other => panic!("unexpected result for address {bad_address}: {other:?}"),
            }
        }
    }

    /// `GetTargetResult::Forbidden` produces `{ ok: false, error: "access denied; ..." }`.
    /// The error string must NOT contain the queried address (no address echo).
    /// Covers both `pwa_push:<user>` and `pwa_push:<user>@<device>` address forms.
    /// Also asserts that the `tracing::warn!` audit log fires with the address.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn pwa_push_channel_get_forbidden_returns_access_denied() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_with_restricted_push_access().await;
        for address in &["pwa_push:alice", "pwa_push:alice@laptop"] {
            let req = post_tool_use_req(MCP_PUSH_LIST_TARGETS_TOOL, json!({ "address": address }));
            let result = try_handle_pwa_push_tool(&bridge, &req).await;
            match result {
                Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                    updated_output: Some(out),
                })) => {
                    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                    assert_eq!(
                        v["ok"],
                        json!(false),
                        "Forbidden should set ok=false for {address}: {out}"
                    );
                    let err = v["error"].as_str().unwrap();
                    assert!(
                        err.contains("access denied"),
                        "error should contain 'access denied' for {address}: {err}"
                    );
                    assert!(
                        !err.contains("alice"),
                        "error must not echo username for {address}: {err}"
                    );
                    assert!(
                        !err.contains("laptop"),
                        "error must not echo device name for {address}: {err}"
                    );
                }
                other => panic!("unexpected result for address {address}: {other:?}"),
            }
        }
        assert!(
            logs_contain("PwaPushChannelGet: access denied"),
            "tracing::warn! audit log must fire on Forbidden"
        );
    }

    /// `PushSendResult::Forbidden` produces `{ ok: false, error: "forbidden: ..." }`.
    /// The error string must NOT contain the queried address or device name (no address echo).
    /// Covers both `pwa_push:<user>` and `pwa_push:<user>@<device>` address forms.
    /// The library-level audit `warn!` must fire regardless of address form.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn post_tool_use_push_send_forbidden_returns_access_denied() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_with_restricted_push_access().await;
        for address in &["pwa_push:alice", "pwa_push:alice@laptop"] {
            let req = post_tool_use_req(
                MCP_PUSH_SEND_TOOL,
                json!({ "address": address, "body": "test" }),
            );
            let result = try_handle_pwa_push_tool(&bridge, &req).await;
            match result {
                Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                    updated_output: Some(out),
                })) => {
                    let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                    assert_eq!(
                        v["ok"],
                        json!(false),
                        "Forbidden should set ok=false for {address}: {out}"
                    );
                    let err = v["error"].as_str().unwrap();
                    assert!(
                        err.contains("forbidden"),
                        "error should contain 'forbidden' for {address}: {err}"
                    );
                    assert!(
                        !err.contains("alice"),
                        "error must not echo username for {address}: {err}"
                    );
                    assert!(
                        !err.contains("laptop"),
                        "error must not echo device name for {address}: {err}"
                    );
                }
                other => panic!("unexpected result for address {address}: {other:?}"),
            }
        }
        assert!(
            logs_contain("PushSend: user not in app allowed_users"),
            "library-level tracing::warn! audit log must fire on Forbidden"
        );
    }

    /// `PushSendResult::MissingSender` (channel-disabled branch) produces
    /// `{ ok: false, error: "push is not enabled for this app" }`.
    ///
    /// This test drives the full intercept path with a real `PwaPushService`
    /// whose apps map has the sender app present but `pwa_push_enabled = false`,
    /// so the service returns `MissingSender` from the channel-disabled branch
    /// and the intercept must serialize it to the expected wire shape.
    #[tokio::test]
    async fn post_tool_use_push_send_missing_sender_returns_error() {
        let bridge =
            crate::active_bridge::ActiveBridge::test_new_with_push_disabled_service().await;
        let req = post_tool_use_req(
            MCP_PUSH_SEND_TOOL,
            json!({ "address": "pwa_push:alice", "body": "hi" }),
        );
        let result = try_handle_pwa_push_tool(&bridge, &req).await;
        match result {
            Some(PwaPushHandled::Respond(CcApprovalDecision::Continue {
                updated_output: Some(out),
            })) => {
                let v: serde_json::Value = serde_json::from_str(&out).unwrap();
                assert_eq!(v["ok"], json!(false));
                assert!(
                    v["error"].as_str().unwrap().contains("push is not enabled"),
                    "error: {}",
                    v["error"]
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}

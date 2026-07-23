//! Shared helpers used by all PostToolUse intercept modules.
//!
//! Each intercept (messaging, pwa_push, automation, mqtt) follows the same
//! pattern for detecting the expected noop response, warning on deviation,
//! and emitting a tool-rejection response.  This module extracts the invariant
//! parts so they have a single implementation.

use std::borrow::Cow;

use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
use serde::Serialize;
use tracing::warn;

use crate::active_bridge::ActiveBridge;

// ---------------------------------------------------------------------------
// Shared tool-response types (D1 and all inline error arms)
// ---------------------------------------------------------------------------

/// `{"ok":true}` — minimal success response used by sites with no extra fields.
///
/// Used at: AutomationDelete success (C1), MqttSend success (B4).
///
/// The `ok` field is always `true`; construct via `ToolOk::new()` or `ToolOk::default()`.
#[derive(Serialize)]
pub struct ToolOk {
    // Private: always true. Use ToolOk::new() or ToolOk::default().
    ok: bool,
}

impl ToolOk {
    /// Construct a `ToolOk`; `ok` is hardcoded `true`.
    pub fn new() -> Self {
        Self { ok: true }
    }
}

impl Default for ToolOk {
    fn default() -> Self {
        Self::new()
    }
}

/// `{"ok":false,"error":"..."}` — shared error shape for all `{ok:false,error}` arms.
///
/// `error` is `Cow<'a, str>` so both borrowed (`&str`) and owned (`String`)
/// error messages are accepted without extra allocation:
/// - Borrowed: static strings, `error: &str` at `reject_tool` call site.
/// - Owned: `format!("…: {x}")` inline arms.
///
/// Fields alphabetical: error, ok — matches serde_json BTreeMap sort order
/// so byte-identical output vs the source `json!({ok:false,error:...})`.
#[derive(Serialize)]
pub struct ToolErr<'a> {
    // alphabetical: error, ok
    pub error: Cow<'a, str>,
    pub ok: bool,
}

/// Returns `true` if `tool_response` has the expected noop shape emitted by
/// `noop_mcp.py`: `{"content": [{"type": "text", "text": "__NOOP__"}]}`.
pub fn is_noop_tool_response(tool_response: &serde_json::Value) -> bool {
    tool_response
        .get("content")
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .and_then(|first| first.get("text"))
        .and_then(|t| t.as_str())
        == Some("__NOOP__")
}

/// Warn-log if `tool_response` deviates from the expected noop shape.
///
/// `context` identifies the caller (e.g. `"messaging intercept"`).  The
/// intercept still runs after the warning — a deviation signals a CC protocol
/// change that may need adapting, but is not fatal.
pub fn warn_if_unexpected_tool_response(
    context: &str,
    tool_name: &str,
    tool_response: &serde_json::Value,
) {
    // TODO(intercept-noop-shape): in production every `BrennSend` trips this
    // warning while the response *is* `__NOOP__` — the shape check above misses
    // whatever wrapping the live tool_response carries. Log noise on every send.
    if !is_noop_tool_response(tool_response) {
        warn!(
            tool = tool_name,
            response = %tool_response,
            "{context}: PostToolUse tool_response was not the expected `__NOOP__`; \
             continuing but this may indicate a CC protocol change",
        );
    }
}

/// Warn-log a tool rejection, emit the tool summary card, and return a
/// `CcApprovalDecision::Continue` response containing `{"ok":false,"error":"..."}`.
///
/// Callers wrap the returned decision in their module-specific handled type, e.g.
/// `MessagingHandled::Respond(decision)`.
///
/// `context` identifies the caller (e.g. `"messaging tool"`) for the warn log.
pub async fn reject_tool(
    bridge: &ActiveBridge,
    context: &str,
    tool_name: &str,
    tool_input: &serde_json::Value,
    error: &str,
) -> CcApprovalDecision {
    warn!(tool = tool_name, error, "{context} rejected");
    crate::active_bridge::emit_tool_summary_for_intercept(bridge, tool_name, tool_input, true)
        .await;
    let payload = ToolErr {
        ok: false,
        error: Cow::Borrowed(error),
    };
    CcApprovalDecision::Continue {
        updated_output: Some(
            serde_json::to_string(&payload).expect("ToolErr serialization is infallible"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active_bridge::ActiveBridge;

    /// `reject_tool` must return `CcApprovalDecision::Continue` with a JSON body
    /// containing `{"ok": false, "error": "<error message>"}`.  Tests the exact
    /// field names so a rename (`"error"` → `"err"` etc.) is caught immediately.
    #[tokio::test]
    async fn reject_tool_returns_ok_false_with_error_field() {
        let bridge = ActiveBridge::test_new_for_pwa_push().await;
        let decision = reject_tool(
            &bridge,
            "test context",
            "TestTool",
            &serde_json::json!({"param": "value"}),
            "something went wrong",
        )
        .await;
        match decision {
            CcApprovalDecision::Continue {
                updated_output: Some(json_str),
            } => {
                let v: serde_json::Value =
                    serde_json::from_str(&json_str).expect("output must be valid JSON");
                assert_eq!(v["ok"], serde_json::json!(false), "ok must be false");
                assert_eq!(
                    v["error"],
                    serde_json::json!("something went wrong"),
                    "error field must carry the error message"
                );
                // No extra fields should appear.
                assert_eq!(
                    v.as_object().map(|o| o.len()),
                    Some(2),
                    "output must have exactly two fields: ok and error"
                );
            }
            other => panic!("expected Continue with updated_output, got: {other:?}"),
        }
    }

    /// `ToolErr` must serialize to `{"ok":false,"error":"..."}` — byte-identical
    /// to the reference `json!` literals at every inline error arm.
    #[test]
    fn tool_err_matches_reference() {
        // Borrowed variant (static string error).
        let err = ToolErr {
            ok: false,
            error: std::borrow::Cow::Borrowed("something went wrong"),
        };
        let produced = serde_json::to_string(&err).expect("ToolErr serialization is infallible");
        let reference = serde_json::json!({
            "ok": false,
            "error": "something went wrong",
        })
        .to_string();
        assert_eq!(produced, reference);

        // Owned variant (format!-computed error).
        let detail = format!("invalid value: {}", 42);
        let err2 = ToolErr {
            ok: false,
            error: std::borrow::Cow::Owned(detail.clone()),
        };
        let produced2 = serde_json::to_string(&err2).expect("ToolErr serialization is infallible");
        let reference2 = serde_json::json!({
            "ok": false,
            "error": detail,
        })
        .to_string();
        assert_eq!(produced2, reference2);
    }

    /// `ToolOk` must serialize to `{"ok":true}` — byte-identical to the reference
    /// `json!({ "ok": true })` used at AutomationDelete success and MqttSend success.
    #[test]
    fn tool_ok_matches_reference() {
        let ok = ToolOk::new();
        let produced = serde_json::to_string(&ok).expect("ToolOk serialization is infallible");
        let reference = serde_json::json!({ "ok": true }).to_string();
        assert_eq!(produced, reference);
    }
}

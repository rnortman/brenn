//! Shared helpers used by all PostToolUse intercept modules.
//!
//! Every tool family that executes outside the hook detects the expected noop
//! response, warns on deviation, builds tool outcomes, and answers the
//! PostToolUse through these helpers.

use std::borrow::Cow;

use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
use serde::Serialize;
use tracing::warn;

use crate::active_bridge::{ActiveBridge, MCP_BRENN_PREFIX, ToolOutcome, execute_admitted};

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
/// - Borrowed: static strings, e.g. `outcome_rejection`'s `error: &str`.
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

/// `{"ok":false,"error":"..."}` as a JSON value.
fn tool_err_json(error: Cow<'_, str>) -> serde_json::Value {
    serde_json::to_value(ToolErr { ok: false, error }).expect("ToolErr serialization is infallible")
}

/// A successful call's response struct as the outcome.
pub(crate) fn outcome_ok<T: Serialize + ?Sized>(resp: &T) -> ToolOutcome {
    ToolOutcome::Ok(serde_json::to_value(resp).expect("tool response serialization is infallible"))
}

/// A failed call: `{"ok":false,"error":…}`.
pub(crate) fn outcome_err(error: Cow<'_, str>) -> ToolOutcome {
    ToolOutcome::Err(tool_err_json(error))
}

/// A rejection of the call's arguments: warn-logged, `{"ok":false,"error":…}`.
///
/// `context` identifies the caller (e.g. `"messaging tool"`) for the warn log.
pub(crate) fn outcome_rejection(context: &str, tool_name: &str, error: &str) -> ToolOutcome {
    warn!(tool = tool_name, error, "{context} rejected");
    ToolOutcome::Err(tool_err_json(Cow::Borrowed(error)))
}

/// The chat card a tool family shows for an answered PostToolUse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PostCard {
    /// The tool-use summary card, from the call's input.
    Summary,
    /// The summary card with the outcome JSON added to its input under
    /// `_outcome`, which the send cards read their status badge from.
    SummaryWithOutcome,
    /// No card.
    None,
}

/// Answer the PostToolUse of an admitted `mcp__brenn__` tool whose family
/// executes through `execute_admitted`: warn on an unexpected stub response,
/// mark the call handled, run it, emit the family's card with `is_error`
/// taken from the outcome, and substitute the outcome for the stub's
/// `__NOOP__`.
///
/// `context` identifies the family (e.g. `"automation intercept"`) for the
/// unexpected-response warning.
pub(crate) async fn answer_post_tool_use(
    bridge: &ActiveBridge,
    context: &str,
    tool_name: &str,
    tool_input: &serde_json::Value,
    tool_use_id: &str,
    tool_response: &serde_json::Value,
    card: PostCard,
) -> CcApprovalDecision {
    warn_if_unexpected_tool_response(context, tool_name, tool_response);
    crate::active_bridge::mark_tool_handled(bridge, tool_use_id).await;
    let short_name = tool_name
        .strip_prefix(MCP_BRENN_PREFIX)
        .expect("intercepted brenn tool names carry the brenn prefix");
    // Admitted at the handle_brenn_tools choke point.
    let outcome = execute_admitted(bridge, short_name, tool_input).await;
    let card_input = match card {
        PostCard::None => None,
        PostCard::Summary => Some(Cow::Borrowed(tool_input)),
        PostCard::SummaryWithOutcome => {
            let mut enriched = tool_input.clone();
            if let serde_json::Value::Object(map) = &mut enriched {
                map.insert("_outcome".to_string(), outcome.json().clone());
            }
            Some(Cow::Owned(enriched))
        }
    };
    if let Some(card_input) = card_input {
        crate::active_bridge::emit_tool_summary_for_intercept(
            bridge,
            tool_name,
            &card_input,
            outcome.is_error(),
        )
        .await;
    }
    outcome.into_continue()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A rejection is `{"ok": false, "error": "<error message>"}` and nothing
    /// else, so a rename (`"error"` → `"err"` etc.) is caught immediately.
    #[test]
    fn outcome_rejection_is_ok_false_with_error_field() {
        let outcome = outcome_rejection("test context", "TestTool", "something went wrong");
        assert_eq!(
            outcome,
            ToolOutcome::Err(serde_json::json!({"ok": false, "error": "something went wrong"}))
        );
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

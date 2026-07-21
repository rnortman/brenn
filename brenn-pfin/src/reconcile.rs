//! ReconcileTool — custom approval display for pfin's `reconcile` MCP tool.
//!
//! Renders a transaction card showing the date, description, splits table
//! (with color-coded amounts), and import reference. Wrapped in
//! `<brenn-tool-approve>` for standard approve/deny/always-allow buttons.

use brenn_lib::app::{AppTool, wrap_in_tool_approve};
use brenn_lib::util::html_escape;
use brenn_lib::ws_types::ToolResponseDecision;
use serde_json::Value;
use tracing::warn;

use crate::card;

/// The MCP tool name for pfin's `reconcile` tool (commits a reconciled
/// transaction). Mirrors `MCP_PROPOSE_RECONCILIATION_TOOL` / `MCP_BATCH_RECONCILE_TOOL`.
pub const MCP_RECONCILE_TOOL: &str = "mcp__pfin__reconcile";

/// Formats the `mcp__pfin__reconcile` tool for the approval dialog.
pub struct ReconcileTool;

impl AppTool for ReconcileTool {
    fn name(&self) -> &str {
        MCP_RECONCILE_TOOL
    }

    fn format_display(&self, tool_input: &Value) -> Option<String> {
        let card_html = match card::render_card(tool_input) {
            Some(c) => c,
            None => {
                warn!("mcp__pfin__reconcile: unexpected input shape, falling back to JSON display");
                return None;
            }
        };

        // Prepend the import header if backend injected _pending_import.
        let content = match tool_input.get("_pending_import") {
            Some(pending) => match card::render_import_header(pending) {
                Some(header) => format!("{header}{card_html}"),
                None => {
                    warn!(
                        "mcp__pfin__reconcile: _pending_import present but malformed, skipping header"
                    );
                    card_html
                }
            },
            None => card_html,
        };

        Some(wrap_in_tool_approve(self.name(), tool_input, &content))
    }

    fn format_summary(
        &self,
        tool_input: &Value,
        decision: &ToolResponseDecision,
    ) -> Option<String> {
        let txn = tool_input.get("transaction").or_else(|| {
            warn!("mcp__pfin__reconcile: missing transaction in summary, falling back to generic");
            None
        })?;
        let description = txn
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("\u{2014}");
        let date = txn.get("date").and_then(|v| v.as_str()).unwrap_or("");
        let split_count = txn
            .get("splits")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);

        let detail = if date.is_empty() {
            format!(
                "<span class=\"ts-file\">{desc}</span> \
                 <span class=\"ts-pattern\">{n} splits</span>",
                desc = html_escape(description),
                n = split_count,
            )
        } else {
            format!(
                "<span class=\"ts-file\">{desc}</span> \
                 <span class=\"ts-pattern\">{date} · {n} splits</span>",
                desc = html_escape(description),
                date = html_escape(date),
                n = split_count,
            )
        };

        let denied = matches!(decision, ToolResponseDecision::Deny { .. });
        if denied {
            Some(format!(
                "<span class=\"ts-denied\" title=\"Denied\">✘</span> {detail}"
            ))
        } else {
            Some(detail)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_input() -> Value {
        serde_json::json!({
            "import_id": "imp-abc-123-def-456",
            "transaction": {
                "date": "2025-03-28",
                "description": "Grocery Store",
                "splits": [
                    { "account": "Expenses:Food:Groceries", "amount": "-52.30", "memo": "weekly" },
                    { "account": "Assets:Checking", "amount": "52.30" }
                ]
            }
        })
    }

    fn allowed() -> ToolResponseDecision {
        ToolResponseDecision::Allow {
            updated_input: None,
        }
    }

    fn denied() -> ToolResponseDecision {
        ToolResponseDecision::Deny { reason: None }
    }

    // -- format_display: happy path --

    #[test]
    fn display_produces_card_wrapped_in_tool_approve() {
        let html = ReconcileTool.format_display(&valid_input()).unwrap();
        assert!(
            html.contains("pfin-reconcile"),
            "missing card class: {html}"
        );
        assert!(
            html.contains("<brenn-tool-approve"),
            "not wrapped in tool-approve: {html}"
        );
        assert!(
            html.contains("tool-name=\"mcp__pfin__reconcile\""),
            "missing tool-name attr: {html}"
        );
    }

    #[test]
    fn display_shows_date_and_description() {
        let html = ReconcileTool.format_display(&valid_input()).unwrap();
        assert!(html.contains("2025-03-28"), "missing date: {html}");
        assert!(html.contains("Grocery Store"), "missing desc: {html}");
    }

    #[test]
    fn display_shows_splits_with_correct_classes() {
        let html = ReconcileTool.format_display(&valid_input()).unwrap();
        assert!(
            html.contains("Expenses:Food:Groceries"),
            "missing debit account: {html}"
        );
        assert!(
            html.contains("Assets:Checking"),
            "missing credit account: {html}"
        );
        assert!(html.contains("-$52.30"), "missing debit amount: {html}");
        assert!(html.contains("$52.30"), "missing credit amount: {html}");
        assert!(html.contains("pfin-debit"), "missing debit class: {html}");
        assert!(html.contains("pfin-credit"), "missing credit class: {html}");
    }

    #[test]
    fn display_shows_memo_when_present() {
        let html = ReconcileTool.format_display(&valid_input()).unwrap();
        assert!(html.contains("weekly"), "missing memo: {html}");
    }

    #[test]
    fn display_truncates_long_import_id() {
        let html = ReconcileTool.format_display(&valid_input()).unwrap();
        assert!(html.contains("imp-abc-123-"), "missing import ref: {html}");
        assert!(!html.contains("def-456"), "should be truncated: {html}");
    }

    #[test]
    fn display_short_import_id_not_truncated() {
        let input = serde_json::json!({
            "import_id": "short",
            "transaction": {
                "date": "2025-01-01",
                "description": "X",
                "splits": [{ "account": "A", "amount": "1" }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(html.contains("short"), "missing short id: {html}");
    }

    // -- format_display: import header --

    fn input_with_import() -> Value {
        let mut input = valid_input();
        input.as_object_mut().unwrap().insert(
            "_pending_import".to_string(),
            serde_json::json!({
                "date": "2025-04-01T00:00:00+00:00",
                "amount": "-52.30",
                "payee": "KROGER #1234",
                "memo": "POS PURCHASE",
                "account": "Assets:Checking"
            }),
        );
        input
    }

    #[test]
    fn display_shows_import_header_when_present() {
        let html = ReconcileTool.format_display(&input_with_import()).unwrap();
        assert!(
            html.contains("pfin-import-header"),
            "missing import header: {html}"
        );
        assert!(html.contains("KROGER #1234"), "missing payee: {html}");
        assert!(html.contains("-$52.30"), "missing formatted amount: {html}");
        assert!(html.contains("2025-04-01"), "missing date: {html}");
        assert!(html.contains("Assets:Checking"), "missing account: {html}");
    }

    #[test]
    fn display_no_import_header_when_absent() {
        let html = ReconcileTool.format_display(&valid_input()).unwrap();
        assert!(
            !html.contains("pfin-import-header"),
            "should not have import header: {html}"
        );
    }

    #[test]
    fn display_import_header_malformed_skipped() {
        let mut input = valid_input();
        input.as_object_mut().unwrap().insert(
            "_pending_import".to_string(),
            serde_json::json!({ "memo": "just a memo" }),
        );
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(
            !html.contains("pfin-import-header"),
            "malformed pending_import should not produce header: {html}"
        );
        // Card should still render.
        assert!(
            html.contains("pfin-reconcile"),
            "card should still render: {html}"
        );
    }

    // -- format_display: create vs update --

    #[test]
    fn display_new_transaction_label() {
        let html = ReconcileTool.format_display(&valid_input()).unwrap();
        assert!(html.contains("New transaction"), "should say new: {html}");
        assert!(!html.contains("Update:"), "should not say update: {html}");
    }

    #[test]
    fn display_update_transaction_label_with_truncated_id() {
        let input = serde_json::json!({
            "import_id": "imp-123",
            "transaction": {
                "id": "txn-abcdef-1234-5678",
                "date": "2025-03-28",
                "description": "Updated",
                "splits": [
                    { "account": "A", "amount": "-10" },
                    { "account": "B", "amount": "10" }
                ]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(html.contains("Update:"), "should say update: {html}");
        assert!(
            html.contains("txn-abcd"),
            "should have truncated txn id: {html}"
        );
        assert!(
            !html.contains("New transaction"),
            "should not say new: {html}"
        );
    }

    #[test]
    fn display_update_short_id_not_truncated() {
        let input = serde_json::json!({
            "import_id": "imp",
            "transaction": {
                "id": "abc",
                "date": "2025-01-01",
                "description": "X",
                "splits": [{ "account": "A", "amount": "1" }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(html.contains("Update: abc"), "short id: {html}");
    }

    // -- format_display: escaping --

    #[test]
    fn display_escapes_description() {
        let input = serde_json::json!({
            "import_id": "imp-123",
            "transaction": {
                "date": "2025-03-28",
                "description": "<script>alert(1)</script>",
                "splits": [{ "account": "A", "amount": "10" }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(
            !html.contains("<script>alert"),
            "description not escaped: {html}"
        );
        assert!(
            html.contains("&lt;script&gt;"),
            "should have escaped form: {html}"
        );
    }

    #[test]
    fn display_escapes_account_name() {
        let input = serde_json::json!({
            "import_id": "imp-123",
            "transaction": {
                "date": "2025-01-01",
                "description": "Test",
                "splits": [{ "account": "A&B<C>", "amount": "10" }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(
            html.contains("A&amp;B&lt;C&gt;"),
            "account not escaped: {html}"
        );
    }

    #[test]
    fn display_escapes_memo() {
        let input = serde_json::json!({
            "import_id": "imp-123",
            "transaction": {
                "date": "2025-01-01",
                "description": "Test",
                "splits": [{ "account": "A", "amount": "10", "memo": "<b>bold</b>" }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(!html.contains("<b>bold</b>"), "memo not escaped: {html}");
    }

    #[test]
    fn display_escapes_transaction_id_in_update_label() {
        let input = serde_json::json!({
            "import_id": "imp",
            "transaction": {
                "id": "a&b<c>d",
                "date": "2025-01-01",
                "description": "X",
                "splits": [{ "account": "A", "amount": "1" }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(
            html.contains("a&amp;b&lt;c&gt;d"),
            "txn id not escaped correctly: {html}"
        );
        assert!(!html.contains("&amp;amp;"), "double-escaped: {html}");
    }

    // -- format_display: fallback to None on bad input --

    #[test]
    fn display_none_on_missing_import_id() {
        let input = serde_json::json!({
            "transaction": {
                "date": "2025-03-28",
                "description": "Test",
                "splits": [{ "account": "A", "amount": "10" }]
            }
        });
        assert!(ReconcileTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_missing_transaction() {
        let input = serde_json::json!({ "import_id": "imp-123" });
        assert!(ReconcileTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_missing_splits() {
        let input = serde_json::json!({
            "import_id": "imp-123",
            "transaction": { "date": "2025-01-01", "description": "Test" }
        });
        assert!(ReconcileTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_empty_splits() {
        let input = serde_json::json!({
            "import_id": "imp-123",
            "transaction": { "date": "2025-01-01", "description": "Test", "splits": [] }
        });
        assert!(ReconcileTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_splits_not_array() {
        let input = serde_json::json!({
            "import_id": "imp-123",
            "transaction": { "date": "2025-01-01", "description": "Test", "splits": "bad" }
        });
        assert!(ReconcileTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_import_id_not_string() {
        let input = serde_json::json!({
            "import_id": 42,
            "transaction": {
                "date": "2025-01-01",
                "description": "Test",
                "splits": [{ "account": "A", "amount": "10" }]
            }
        });
        assert!(ReconcileTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_completely_empty_input() {
        assert!(
            ReconcileTool
                .format_display(&serde_json::json!({}))
                .is_none()
        );
    }

    // -- format_display: graceful degradation on partial split data --

    #[test]
    fn display_split_missing_amount_shows_zero() {
        let input = serde_json::json!({
            "import_id": "imp",
            "transaction": {
                "date": "2025-01-01",
                "description": "Test",
                "splits": [{ "account": "A" }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(
            html.contains("$0"),
            "should show $0 for missing amount: {html}"
        );
    }

    #[test]
    fn display_split_amount_as_number_shows_zero() {
        let input = serde_json::json!({
            "import_id": "imp",
            "transaction": {
                "date": "2025-01-01",
                "description": "Test",
                "splits": [{ "account": "A", "amount": 42.50 }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(html.contains("$0"), "numeric amount degrades to $0: {html}");
    }

    // -- format_display: optional fields graceful defaults --

    #[test]
    fn display_missing_date_shows_em_dash() {
        let input = serde_json::json!({
            "import_id": "imp",
            "transaction": {
                "description": "No date",
                "splits": [{ "account": "A", "amount": "10" }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(
            html.contains("\u{2014}"),
            "should show em dash for missing date: {html}"
        );
    }

    #[test]
    fn display_missing_description_shows_em_dash() {
        let input = serde_json::json!({
            "import_id": "imp",
            "transaction": {
                "date": "2025-01-01",
                "splits": [{ "account": "A", "amount": "10" }]
            }
        });
        let html = ReconcileTool.format_display(&input).unwrap();
        assert!(
            html.contains("\u{2014}"),
            "should show em dash for missing desc: {html}"
        );
    }

    // -- format_summary --

    #[test]
    fn summary_allowed_shows_description_date_splits() {
        let html = ReconcileTool
            .format_summary(&valid_input(), &allowed())
            .unwrap();
        assert!(
            html.contains("Grocery Store"),
            "missing description: {html}"
        );
        assert!(html.contains("2025-03-28"), "missing date: {html}");
        assert!(html.contains("2 splits"), "missing split count: {html}");
        assert!(!html.contains("ts-denied"), "should not be denied: {html}");
    }

    #[test]
    fn summary_denied_shows_denied_indicator() {
        let html = ReconcileTool
            .format_summary(&valid_input(), &denied())
            .unwrap();
        assert!(
            html.contains("ts-denied"),
            "missing denied indicator: {html}"
        );
        assert!(
            html.contains("Grocery Store"),
            "missing description: {html}"
        );
    }

    #[test]
    fn summary_no_date_omits_date_separator() {
        let input = serde_json::json!({
            "import_id": "imp-123",
            "transaction": {
                "description": "No date txn",
                "splits": [
                    { "account": "A", "amount": "10" },
                    { "account": "B", "amount": "-10" }
                ]
            }
        });
        let html = ReconcileTool.format_summary(&input, &allowed()).unwrap();
        assert!(html.contains("No date txn"), "missing description: {html}");
        assert!(html.contains("2 splits"), "missing split count: {html}");
        assert!(
            !html.contains(" \u{b7} "),
            "should not have date separator: {html}"
        );
    }

    #[test]
    fn summary_none_on_missing_transaction() {
        let input = serde_json::json!({ "import_id": "imp-123" });
        assert!(ReconcileTool.format_summary(&input, &allowed()).is_none());
    }

    #[test]
    fn summary_none_on_empty_input() {
        assert!(
            ReconcileTool
                .format_summary(&serde_json::json!({}), &allowed())
                .is_none()
        );
    }

    #[test]
    fn summary_missing_splits_shows_zero_count() {
        let input = serde_json::json!({
            "import_id": "imp",
            "transaction": { "description": "No splits" }
        });
        let html = ReconcileTool.format_summary(&input, &allowed()).unwrap();
        assert!(html.contains("0 splits"), "should show 0 splits: {html}");
    }

    #[test]
    fn summary_escapes_description() {
        let input = serde_json::json!({
            "import_id": "imp",
            "transaction": {
                "description": "<b>bold</b>",
                "splits": [{ "account": "A", "amount": "1" }]
            }
        });
        let html = ReconcileTool.format_summary(&input, &allowed()).unwrap();
        assert!(
            !html.contains("<b>bold</b>"),
            "description not escaped: {html}"
        );
        assert!(
            html.contains("&lt;b&gt;"),
            "should have escaped form: {html}"
        );
    }

    #[test]
    fn summary_missing_description_uses_em_dash() {
        let input = serde_json::json!({
            "import_id": "imp",
            "transaction": {
                "date": "2025-01-01",
                "splits": [{ "account": "A", "amount": "1" }]
            }
        });
        let html = ReconcileTool.format_summary(&input, &allowed()).unwrap();
        assert!(html.contains("\u{2014}"), "should use em dash: {html}");
    }
}

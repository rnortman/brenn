//! BatchAssign — batch user assignment for pending imports.
//!
//! The LLM sends a batch of (import_id, optional notes, optional info) items
//! plus a single `user` (assignee) at the top level. The user accepts or
//! rejects each. Accepted items are assigned via `pf assign`. Rejected and
//! failed items are reported back to CC.
//!
//! Mirrors the architecture of `BatchReconcile` — same noop-MCP / PostToolUse
//! intercept / DB-persist / async-decision pattern — but assigns rather than
//! reconciles, so each row carries less information (no transaction).
//!
//! Two rendering modes: `render_assign_table` (desktop) and
//! `render_assign_swipe` (mobile). The bridge picks the right one based on
//! `ViewportClass` and passes the result through `StashProposal`.

use brenn_lib::app::AppTool;
use brenn_lib::subprocess::SubprocessExecContext;
use brenn_lib::util::html_escape;
use brenn_lib::ws_types::ToolResponseDecision;
use serde_json::Value;
use tracing::{info, warn};

use crate::batch::EnrichedBatchItem;
use crate::batch_render::{render_swipe_envelope, render_table_envelope};
use crate::card;

/// MCP tool name for BatchAssign (brenn's noop MCP server).
pub const MCP_BATCH_ASSIGN_TOOL: &str = "mcp__brenn__BatchAssign";

/// Registered AppTool for summary formatting. Display rendering is done
/// directly by `render_assign_table` / `render_assign_swipe` (called from
/// the bridge), NOT through `format_display`, because the bridge needs to
/// pick the viewport-appropriate variant.
pub struct BatchAssignTool;

impl AppTool for BatchAssignTool {
    fn name(&self) -> &str {
        MCP_BATCH_ASSIGN_TOOL
    }

    fn format_display(&self, _tool_input: &Value) -> Option<String> {
        // Bridge dispatches directly; None falls back to generic display.
        None
    }

    fn format_summary(
        &self,
        tool_input: &Value,
        decision: &ToolResponseDecision,
    ) -> Option<String> {
        let items = tool_input.get("items")?.as_array()?;
        let count = items.len();
        let user = tool_input
            .get("user")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        // user is LLM-supplied — escape before embedding in HTML.
        let user_escaped = html_escape(user);

        // TODO(summary-real-decision): currently emit_tool_summary always
        // passes Allow { updated_input: None }, so we can't show per-item
        // accept/reject counts. Show a generic summary for now.
        let detail = match decision {
            ToolResponseDecision::Deny { .. } => {
                format!(
                    "<span class=\"ts-denied\" title=\"Denied\">\u{2718}</span> \
                     <span class=\"ts-file\">{count} items \u{2192} @{user_escaped}</span> \
                     <span class=\"ts-pattern\">denied</span>",
                )
            }
            _ => {
                format!(
                    "<span class=\"ts-file\">{count} items \u{2192} @{user_escaped}</span> \
                     <span class=\"ts-pattern\">approved</span>",
                )
            }
        };

        Some(detail)
    }
}

/// Render the desktop table variant: `<brenn-pfin-batch-assign-table>`.
///
/// Each row shows date, payee + account, amount, and ✓/✗ buttons.
/// Header above the table reads "Assign N imports to <user>".
/// Returns `None` on empty input.
pub fn render_assign_table(items: &[EnrichedBatchItem], user: &str) -> Option<String> {
    if items.is_empty() {
        return None;
    }
    // Config block carries only `count` (no user-supplied data) — the
    // assignee text is in the visible header where we control escaping.
    let header = format!(
        "  <div class=\"pfin-batch-assign-header\">Assign {} import{} to <span class=\"pfin-batch-assign-user\">@{}</span></div>\n",
        items.len(),
        if items.len() == 1 { "" } else { "s" },
        html_escape(user),
    );
    render_table_envelope(
        "brenn-pfin-batch-assign-table",
        items,
        Some(&header),
        |html| {
            html.push_str("  <table class=\"pfin-batch-table pfin-batch-assign-table\">\n");
            html.push_str("    <thead><tr>\n");
            html.push_str("      <th class=\"pfin-batch-th-date\">Date</th>\n");
            html.push_str("      <th class=\"pfin-batch-th-payee\">Payee</th>\n");
            html.push_str("      <th class=\"pfin-batch-th-amount\">Amount</th>\n");
            html.push_str("      <th class=\"pfin-batch-th-actions\"></th>\n");
            html.push_str("    </tr></thead>\n");
            html.push_str("    <tbody>\n");
            for enriched in items {
                html.push_str(&render_table_row(enriched));
            }
            html.push_str("    </tbody>\n");
            html.push_str("  </table>\n");
        },
    )
}

/// Render the mobile swipe variant: `<brenn-pfin-batch-assign-swipe>`.
///
/// Stacked cards with swipe-to-accept/reject interaction.
/// Header above the cards reads "Assign N imports to <user>".
/// Returns `None` on empty input.
pub fn render_assign_swipe(items: &[EnrichedBatchItem], user: &str) -> Option<String> {
    if items.is_empty() {
        return None;
    }
    let header = format!(
        "  <div class=\"pfin-batch-assign-header\">Assign {} import{} to <span class=\"pfin-batch-assign-user\">@{}</span></div>\n",
        items.len(),
        if items.len() == 1 { "" } else { "s" },
        html_escape(user),
    );
    render_swipe_envelope(
        "brenn-pfin-batch-assign-swipe",
        items,
        Some(&header),
        |enriched, html| {
            html.push_str(&render_swipe_card(enriched));
        },
    )
}

/// Render a single table row for the desktop view.
fn render_table_row(enriched: &EnrichedBatchItem) -> String {
    let idx = enriched.original_index;
    let pending = &enriched.pending_import;

    let payee = pending.get("payee").and_then(|v| v.as_str()).unwrap_or("?");
    let amount_raw = pending
        .get("amount")
        .and_then(|v| v.as_str())
        .unwrap_or("0");
    let date = pending
        .get("date")
        .and_then(|v| v.as_str())
        .map(|d| d.get(..10).unwrap_or(d))
        .unwrap_or("");
    let import_account = pending.get("account").and_then(|v| v.as_str());

    let display_amount = card::format_amount(amount_raw);
    let amount_class = if amount_raw.trim().starts_with('-') {
        "pfin-debit"
    } else {
        "pfin-credit"
    };

    let notes = enriched
        .item
        .get("notes")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let info = enriched
        .item
        .get("info")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    let mut html = String::with_capacity(384);
    html.push_str(&format!(
        "    <tr class=\"pfin-batch-row pfin-batch-assign-row\" data-index=\"{idx}\">\n"
    ));

    // Date.
    html.push_str(&format!(
        "      <td class=\"pfin-batch-date\">{}</td>\n",
        html_escape(date)
    ));

    // Payee + account (and optional notes/info as sub-lines).
    html.push_str("      <td class=\"pfin-batch-payee\"><span class=\"pfin-batch-payee-name\">");
    html.push_str(&html_escape(payee));
    html.push_str("</span>");
    if let Some(acct) = import_account {
        html.push_str(&format!(
            "<div class=\"pfin-batch-assign-account\">{}</div>",
            html_escape(acct)
        ));
    }
    if let Some(notes_text) = notes {
        html.push_str(&format!(
            "<div class=\"pfin-batch-assign-notes\">+ note: {}</div>",
            html_escape(notes_text)
        ));
    }
    if let Some(info_text) = info {
        html.push_str(&format!(
            "<div class=\"pfin-batch-assign-info\">{}</div>",
            html_escape(info_text)
        ));
    }
    html.push_str("</td>\n");

    // Amount.
    html.push_str(&format!(
        "      <td class=\"pfin-batch-amount {amount_class}\">{}</td>\n",
        html_escape(&display_amount)
    ));

    // Action buttons.
    html.push_str(
        "      <td class=\"pfin-batch-actions\">\
         <button class=\"pfin-batch-accept\" title=\"Accept\">\u{2713}</button>\
         <button class=\"pfin-batch-reject\" title=\"Reject\">\u{2717}</button>\
         </td>\n",
    );

    html.push_str("    </tr>\n");
    html
}

/// Render a single swipe card for the mobile view.
fn render_swipe_card(enriched: &EnrichedBatchItem) -> String {
    let idx = enriched.original_index;
    let pending = &enriched.pending_import;

    let payee = pending.get("payee").and_then(|v| v.as_str()).unwrap_or("?");
    let amount_raw = pending
        .get("amount")
        .and_then(|v| v.as_str())
        .unwrap_or("0");
    let date = pending
        .get("date")
        .and_then(|v| v.as_str())
        .map(|d| d.get(..10).unwrap_or(d))
        .unwrap_or("");
    let import_account = pending.get("account").and_then(|v| v.as_str());

    let display_amount = card::format_amount(amount_raw);
    let amount_class = if amount_raw.trim().starts_with('-') {
        "pfin-debit"
    } else {
        "pfin-credit"
    };

    let notes = enriched
        .item
        .get("notes")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let info = enriched
        .item
        .get("info")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    let mut html = String::with_capacity(384);

    // Outer wrapper for swipe mechanics: reveal layer behind, card on top.
    html.push_str(&format!(
        "  <div class=\"pfin-batch-swipe-item\" data-index=\"{idx}\">\n"
    ));

    // Reveal layers (shown behind the card during swipe).
    html.push_str(
        "    <div class=\"pfin-batch-reveal pfin-batch-reveal-accept\">\
         <span class=\"pfin-batch-reveal-icon\">\u{2713}</span>\
         <span class=\"pfin-batch-reveal-label\">Accept</span></div>\n",
    );
    html.push_str(
        "    <div class=\"pfin-batch-reveal pfin-batch-reveal-reject\">\
         <span class=\"pfin-batch-reveal-icon\">\u{2717}</span>\
         <span class=\"pfin-batch-reveal-label\">Reject</span></div>\n",
    );

    // Card content.
    html.push_str("    <div class=\"pfin-batch-card pfin-batch-assign-card\">\n");

    // Header: payee + amount.
    html.push_str(&format!(
        "      <div class=\"pfin-batch-card-header\">\
         <span class=\"pfin-batch-payee-name\">{}</span>\
         <span class=\"pfin-batch-amount {amount_class}\">{}</span>\
         </div>\n",
        html_escape(payee),
        html_escape(&display_amount),
    ));

    // Sub line: date · account.
    if !date.is_empty() || import_account.is_some() {
        html.push_str("      <div class=\"pfin-batch-import-detail\">\n");
        if !date.is_empty() {
            html.push_str(&format!(
                "        <span class=\"pfin-batch-date\">{}</span>\n",
                html_escape(date)
            ));
        }
        if let Some(acct) = import_account {
            html.push_str(&format!(
                "        <span class=\"pfin-batch-assign-account\">{}</span>\n",
                html_escape(acct)
            ));
        }
        html.push_str("      </div>\n");
    }

    // Optional notes line.
    if let Some(notes_text) = notes {
        html.push_str(&format!(
            "      <div class=\"pfin-batch-assign-notes\">+ note: {}</div>\n",
            html_escape(notes_text)
        ));
    }

    // Optional info line.
    if let Some(info_text) = info {
        html.push_str(&format!(
            "      <div class=\"pfin-batch-assign-info\">{}</div>\n",
            html_escape(info_text)
        ));
    }

    html.push_str("    </div>\n"); // card
    html.push_str("  </div>\n"); // swipe-item

    html
}

/// Execute a batch of assignments for accepted items.
///
/// Runs sequentially to avoid pfin ledger conflicts (pfin's WAL serializes
/// writes anyway). Returns a structured JSON result for CC.
///
/// The assignee is read from `tool_input["user"]` (the LLM picks who to
/// assign to). Per-item `notes` come from `items[index].notes`.
pub async fn execute_batch_assign(
    tool_input: &Value,
    decisions: &[(usize, bool)],
    ctx: &SubprocessExecContext<'_>,
    enrichment_failures: &[(usize, String, String)], // (index, import_id, error)
) -> Value {
    let items = match tool_input.get("items").and_then(|v| v.as_array()) {
        Some(items) => items,
        None => {
            return serde_json::json!({
                "status": "error",
                "error": "missing items array in tool input",
            });
        }
    };

    let assignee = match tool_input.get("user").and_then(|v| v.as_str()) {
        Some(u) if !u.is_empty() => u,
        _ => {
            return serde_json::json!({
                "status": "error",
                "error": "missing user in tool input",
            });
        }
    };

    let total = items.len();
    let mut results: Vec<Value> = Vec::new();
    let mut accepted_count = 0usize;
    let mut rejected_count = 0usize;
    let mut failed_count = 0usize;

    for &(index, accepted) in decisions {
        let item = match items.get(index) {
            Some(i) => i,
            None => {
                warn!(index, "batch_assign decision index out of range");
                continue;
            }
        };
        let import_id = item
            .get("import_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        if !accepted {
            rejected_count += 1;
            results.push(serde_json::json!({
                "index": index,
                "import_id": import_id,
                "status": "rejected",
            }));
            continue;
        }

        // Per-item notes from the LLM (NOT from pending_import enrichment).
        let notes = item
            .get("notes")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        match crate::run_pfin_assign(import_id, assignee, notes, ctx).await {
            Ok(_output) => {
                accepted_count += 1;
                info!(index, import_id, assignee, "batch_assign item assigned");
                results.push(serde_json::json!({
                    "index": index,
                    "import_id": import_id,
                    "status": "assigned",
                }));
            }
            Err(e) => {
                failed_count += 1;
                warn!(index, import_id, error = %e, "batch_assign item assignment failed");
                results.push(serde_json::json!({
                    "index": index,
                    "import_id": import_id,
                    "status": "error",
                    "error": e,
                }));
            }
        }
    }

    let enrichment_failed_count = enrichment_failures.len();
    for (index, import_id, error) in enrichment_failures {
        results.push(serde_json::json!({
            "index": index,
            "import_id": import_id,
            "status": "enrichment_failed",
            "error": error,
        }));
    }

    let presented = total - enrichment_failed_count;

    serde_json::json!({
        "status": "batch_complete",
        "user": assignee,
        "total": total,
        "enrichment_failed": enrichment_failed_count,
        "presented": presented,
        "accepted": accepted_count,
        "rejected": rejected_count,
        "failed": failed_count,
        "results": results,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use serde_json::json;

    fn sample_enriched_item(index: usize) -> EnrichedBatchItem {
        EnrichedBatchItem {
            original_index: index,
            item: json!({
                "import_id": format!("imp-{index}"),
            }),
            pending_import: json!({
                "payee": "Whole Foods",
                "amount": "-52.30",
                "date": "2025-03-28T00:00:00Z",
                "account": "Assets:Checking"
            }),
        }
    }

    #[test]
    fn render_assign_table_produces_valid_html() {
        let items = vec![sample_enriched_item(0), sample_enriched_item(1)];
        let html = render_assign_table(&items, "wonder").unwrap();
        assert!(html.contains("<brenn-pfin-batch-assign-table>"));
        assert!(html.contains("</brenn-pfin-batch-assign-table>"));
        assert!(html.contains("Whole Foods"));
        assert!(html.contains("-$52.30"));
        assert!(html.contains("2025-03-28"));
        assert!(
            html.contains("Assets:Checking"),
            "should show import account"
        );
        assert!(
            html.contains("Assign 2 imports to"),
            "header should mention count and assignee"
        );
        assert!(html.contains("@wonder"), "header should show username");
        assert!(html.contains("data-index=\"0\""));
        assert!(html.contains("data-index=\"1\""));
        assert!(html.contains("pfin-batch-accept"));
        assert!(html.contains("pfin-batch-reject"));
        // Singular header should NOT appear with two items.
        assert!(!html.contains("Assign 2 import to"));
    }

    #[test]
    fn render_assign_table_singular_header() {
        let items = vec![sample_enriched_item(0)];
        let html = render_assign_table(&items, "wonder").unwrap();
        assert!(
            html.contains("Assign 1 import to"),
            "singular header for one item: {html}"
        );
    }

    #[test]
    fn render_assign_table_empty_returns_none() {
        assert!(render_assign_table(&[], "wonder").is_none());
    }

    #[test]
    fn render_assign_swipe_produces_valid_html() {
        let items = vec![sample_enriched_item(0)];
        let html = render_assign_swipe(&items, "wonder").unwrap();
        assert!(html.contains("<brenn-pfin-batch-assign-swipe>"));
        assert!(html.contains("</brenn-pfin-batch-assign-swipe>"));
        assert!(html.contains("Whole Foods"));
        assert!(html.contains("-$52.30"));
        assert!(html.contains("2025-03-28"));
        assert!(html.contains("Assets:Checking"));
        assert!(html.contains("@wonder"));
        assert!(html.contains("pfin-batch-card"));
        assert!(html.contains("pfin-batch-assign-card"));
        assert!(html.contains("pfin-batch-reveal-accept"));
        assert!(html.contains("pfin-batch-reveal-reject"));
        assert!(html.contains("Accept"));
        assert!(html.contains("Reject"));
    }

    #[test]
    fn render_assign_swipe_empty_returns_none() {
        assert!(render_assign_swipe(&[], "wonder").is_none());
    }

    #[test]
    fn render_shows_notes_when_present() {
        let mut item = sample_enriched_item(0);
        item.item["notes"] = json!("from John, see thread");
        let table = render_assign_table(&[item.clone()], "wonder").unwrap();
        assert!(table.contains("from John, see thread"));
        assert!(table.contains("pfin-batch-assign-notes"));

        let swipe = render_assign_swipe(&[item], "wonder").unwrap();
        assert!(swipe.contains("from John, see thread"));
        assert!(swipe.contains("pfin-batch-assign-notes"));
    }

    #[test]
    fn render_omits_notes_when_absent() {
        let item = sample_enriched_item(0);
        let table = render_assign_table(std::slice::from_ref(&item), "wonder").unwrap();
        assert!(
            !table.contains("pfin-batch-assign-notes"),
            "should not render notes element when absent"
        );
        let swipe = render_assign_swipe(&[item], "wonder").unwrap();
        assert!(
            !swipe.contains("pfin-batch-assign-notes"),
            "should not render notes element when absent"
        );
    }

    #[test]
    fn render_shows_info_when_present() {
        let mut item = sample_enriched_item(0);
        item.item["info"] = json!("matched email pattern");
        let table = render_assign_table(&[item.clone()], "wonder").unwrap();
        assert!(table.contains("matched email pattern"));
        assert!(table.contains("pfin-batch-assign-info"));

        let swipe = render_assign_swipe(&[item], "wonder").unwrap();
        assert!(swipe.contains("matched email pattern"));
        assert!(swipe.contains("pfin-batch-assign-info"));
    }

    #[test]
    fn render_omits_info_when_absent() {
        let item = sample_enriched_item(0);
        let table = render_assign_table(std::slice::from_ref(&item), "wonder").unwrap();
        assert!(
            !table.contains("pfin-batch-assign-info"),
            "should not render info element when absent"
        );
        let swipe = render_assign_swipe(&[item], "wonder").unwrap();
        assert!(
            !swipe.contains("pfin-batch-assign-info"),
            "should not render info element when absent"
        );
    }

    #[test]
    fn table_escapes_html_in_all_fields() {
        let item = EnrichedBatchItem {
            original_index: 0,
            item: json!({
                "import_id": "imp-0",
                "notes": "<i>note</i>",
                "info": "<img onerror=alert(1)>",
            }),
            pending_import: json!({
                "payee": "<script>alert(1)</script>",
                "amount": "-10",
                "account": "Evil<account>",
            }),
        };
        // Inject XSS in user as well.
        let xss_user = "<svg onload=alert(1)>";

        let table = render_assign_table(std::slice::from_ref(&item), xss_user).unwrap();
        assert!(
            !table.contains("<script>alert"),
            "payee not escaped in table"
        );
        assert!(!table.contains("<i>note"), "notes not escaped in table");
        assert!(!table.contains("<img onerror"), "info not escaped in table");
        assert!(
            !table.contains("<account>"),
            "import account not escaped in table"
        );
        assert!(!table.contains("<svg onload"), "user not escaped in table");
        assert!(table.contains("&lt;script&gt;"));
        assert!(table.contains("&lt;svg"));

        let swipe = render_assign_swipe(&[item], xss_user).unwrap();
        assert!(
            !swipe.contains("<script>alert"),
            "payee not escaped in swipe"
        );
        assert!(!swipe.contains("<i>note"), "notes not escaped in swipe");
        assert!(!swipe.contains("<img onerror"), "info not escaped in swipe");
        assert!(
            !swipe.contains("<account>"),
            "import account not escaped in swipe"
        );
        assert!(!swipe.contains("<svg onload"), "user not escaped in swipe");
    }

    #[test]
    fn summary_shows_item_count_and_user() {
        let input = json!({
            "user": "wonder",
            "items": [
                { "import_id": "a" },
                { "import_id": "b" },
            ]
        });
        let tool = BatchAssignTool;
        let decision = ToolResponseDecision::Allow {
            updated_input: None,
        };
        let summary = tool.format_summary(&input, &decision).unwrap();
        assert!(summary.contains("2 items"), "got: {summary}");
        assert!(summary.contains("wonder"), "got: {summary}");
        assert!(summary.contains("approved"), "got: {summary}");
    }

    #[test]
    fn summary_denied() {
        let input = json!({
            "user": "wonder",
            "items": [{ "import_id": "a" }]
        });
        let tool = BatchAssignTool;
        let decision = ToolResponseDecision::Deny { reason: None };
        let summary = tool.format_summary(&input, &decision).unwrap();
        assert!(summary.contains("denied"), "got: {summary}");
        assert!(summary.contains("\u{2718}"), "got: {summary}");
    }

    #[test]
    fn summary_escapes_user() {
        let input = json!({
            "user": "<script>x</script>",
            "items": [{ "import_id": "a" }]
        });
        let tool = BatchAssignTool;
        let decision = ToolResponseDecision::Allow {
            updated_input: None,
        };
        let summary = tool.format_summary(&input, &decision).unwrap();
        assert!(
            !summary.contains("<script>x"),
            "user not escaped in summary: {summary}"
        );
        assert!(summary.contains("&lt;script&gt;"));
    }

    #[test]
    fn date_truncated_to_ymd() {
        let mut item = sample_enriched_item(0);
        item.pending_import["date"] = json!("2025-03-28T12:34:56Z");
        let html = render_assign_table(&[item], "wonder").unwrap();
        assert!(html.contains("2025-03-28"));
        assert!(!html.contains("T12:34:56Z"));
    }

    #[tokio::test]
    async fn execute_batch_assign_handles_rejected_items() {
        let tool_input = json!({
            "user": "wonder",
            "items": [
                { "import_id": "imp-1" },
                { "import_id": "imp-2" },
            ]
        });
        // Both rejected — no pfin invocation needed.
        let decisions = vec![(0, false), (1, false)];
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "/nonexistent/pfin",
            env: &env,
            working_dir: std::path::Path::new("."),
            container_spawn: None,
        };
        let result = execute_batch_assign(&tool_input, &decisions, &ctx, &[]).await;
        assert_eq!(result["status"], "batch_complete");
        assert_eq!(result["user"], "wonder");
        assert_eq!(result["accepted"], 0);
        assert_eq!(result["rejected"], 2);
        assert_eq!(result["failed"], 0);
        let results = result["results"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["status"], "rejected");
        assert_eq!(results[1]["status"], "rejected");
    }

    #[tokio::test]
    async fn execute_batch_assign_handles_missing_items_array() {
        let tool_input = json!({ "user": "wonder" });
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "/nonexistent/pfin",
            env: &env,
            working_dir: std::path::Path::new("."),
            container_spawn: None,
        };
        let result = execute_batch_assign(&tool_input, &[], &ctx, &[]).await;
        assert_eq!(result["status"], "error");
        assert!(
            result["error"].as_str().unwrap().contains("missing items"),
            "got: {result}"
        );
    }

    #[tokio::test]
    async fn execute_batch_assign_handles_missing_user() {
        let tool_input = json!({
            "items": [{ "import_id": "imp-1" }]
        });
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "/nonexistent/pfin",
            env: &env,
            working_dir: std::path::Path::new("."),
            container_spawn: None,
        };
        let result = execute_batch_assign(&tool_input, &[], &ctx, &[]).await;
        assert_eq!(result["status"], "error");
        assert!(
            result["error"].as_str().unwrap().contains("missing user"),
            "got: {result}"
        );
    }

    #[tokio::test]
    async fn execute_batch_assign_handles_empty_user() {
        let tool_input = json!({
            "user": "",
            "items": [{ "import_id": "imp-1" }]
        });
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "/nonexistent/pfin",
            env: &env,
            working_dir: std::path::Path::new("."),
            container_spawn: None,
        };
        let result = execute_batch_assign(&tool_input, &[], &ctx, &[]).await;
        assert_eq!(result["status"], "error");
        assert!(
            result["error"].as_str().unwrap().contains("missing user"),
            "got: {result}"
        );
    }

    #[tokio::test]
    async fn execute_batch_assign_appends_enrichment_failures() {
        let tool_input = json!({
            "user": "wonder",
            "items": [
                { "import_id": "imp-1" },
                { "import_id": "imp-2" },
            ]
        });
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "/nonexistent/pfin",
            env: &env,
            working_dir: std::path::Path::new("."),
            container_spawn: None,
        };
        let enrichment_failures = vec![(1, "imp-2".to_string(), "pf show failed".to_string())];
        let result = execute_batch_assign(&tool_input, &[], &ctx, &enrichment_failures).await;
        assert_eq!(result["status"], "batch_complete");
        assert_eq!(result["enrichment_failed"], 1);
        let results = result["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["status"], "enrichment_failed");
        assert_eq!(results[0]["import_id"], "imp-2");
    }
}

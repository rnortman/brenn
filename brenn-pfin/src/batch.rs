//! BatchReconcile — batch accept/reject for high-confidence reconciliation
//! proposals.
//!
//! The LLM sends a batch of (import_id, transaction, optional info) items.
//! The user accepts or rejects each. Accepted items are reconciled via pfin
//! CLI. Rejected and failed items are reported back to CC.
//!
//! Two rendering modes: `render_batch_table` (desktop) and
//! `render_batch_swipe` (mobile). The bridge picks the right one based on
//! `ViewportClass` and passes the result through `StashProposal`.

use brenn_lib::app::AppTool;
use brenn_lib::subprocess::SubprocessExecContext;
use brenn_lib::util::html_escape;
use brenn_lib::ws_types::ToolResponseDecision;
use serde_json::Value;
use tracing::{info, warn};

use crate::batch_render::{render_swipe_envelope, render_table_envelope};
use crate::card;

/// MCP tool name for BatchReconcile (brenn's noop MCP server).
pub const MCP_BATCH_RECONCILE_TOOL: &str = "mcp__brenn__BatchReconcile";

/// Maximum items per batch. Enforced server-side.
pub const MAX_BATCH_ITEMS: usize = 50;

/// Registered AppTool for summary formatting. Display rendering is done
/// directly by `render_batch_table` / `render_batch_swipe` (called from
/// the bridge), NOT through `format_display`, because the bridge needs to
/// pick the viewport-appropriate variant.
pub struct BatchReconcileTool;

impl AppTool for BatchReconcileTool {
    fn name(&self) -> &str {
        MCP_BATCH_RECONCILE_TOOL
    }

    fn format_display(&self, _tool_input: &Value) -> Option<String> {
        // Rendering is done by render_batch_table / render_batch_swipe,
        // called directly from handle_brenn_tools. This intentionally
        // returns None so the generic fallback is available as a safety net.
        None
    }

    fn format_summary(
        &self,
        tool_input: &Value,
        decision: &ToolResponseDecision,
    ) -> Option<String> {
        let items = tool_input.get("items")?.as_array()?;
        let count = items.len();

        // TODO(summary-real-decision): currently emit_tool_summary always
        // passes Allow { updated_input: None }, so we can't show per-item
        // accept/reject counts. Show a generic summary for now.
        let detail = match decision {
            ToolResponseDecision::Deny { .. } => {
                format!(
                    "<span class=\"ts-denied\" title=\"Denied\">\u{2718}</span> \
                     <span class=\"ts-file\">{count} items</span> \
                     <span class=\"ts-pattern\">denied</span>",
                )
            }
            _ => {
                format!(
                    "<span class=\"ts-file\">{count} items</span> \
                     <span class=\"ts-pattern\">\u{2192} approved</span>",
                )
            }
        };

        Some(detail)
    }
}

/// An enriched batch item ready for rendering. Produced by the bridge after
/// parallel enrichment; items that failed enrichment are excluded.
#[derive(Clone)]
pub struct EnrichedBatchItem {
    /// Original index in the LLM's items array.
    pub original_index: usize,
    /// The full item from the LLM (import_id, transaction, optional info).
    pub item: Value,
    /// Enriched import details from `pf show`.
    pub pending_import: Value,
}

/// A non-import split extracted from the proposed transaction.
struct CounterpartySplit {
    account: String,
    amount: String,
    memo: Option<String>,
}

/// Render the desktop table variant: `<brenn-pfin-batch-table>`.
///
/// Each row shows payee, amount, categorization (description + all
/// counterparty splits with memos), and ✓/✗ buttons.
/// Returns `None` on empty input.
pub fn render_batch_table(items: &[EnrichedBatchItem]) -> Option<String> {
    render_table_envelope("brenn-pfin-batch-table", items, None, |html| {
        html.push_str("  <table class=\"pfin-batch-table\">\n");
        html.push_str("    <thead><tr>\n");
        html.push_str("      <th class=\"pfin-batch-th-payee\">Payee</th>\n");
        html.push_str("      <th class=\"pfin-batch-th-amount\">Amount</th>\n");
        html.push_str("      <th class=\"pfin-batch-th-category\">Categorization</th>\n");
        html.push_str("      <th class=\"pfin-batch-th-actions\"></th>\n");
        html.push_str("    </tr></thead>\n");
        html.push_str("    <tbody>\n");
        for enriched in items {
            html.push_str(&render_table_row(enriched));
        }
        html.push_str("    </tbody>\n");
        html.push_str("  </table>\n");
    })
}

/// Render the mobile swipe variant: `<brenn-pfin-batch-swipe>`.
///
/// Stacked cards with swipe-to-accept/reject interaction.
/// Returns `None` on empty input.
pub fn render_batch_swipe(items: &[EnrichedBatchItem]) -> Option<String> {
    render_swipe_envelope("brenn-pfin-batch-swipe", items, None, |enriched, html| {
        html.push_str(&render_swipe_card(enriched));
    })
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
    let import_memo = pending
        .get("memo")
        .and_then(|v| v.as_str())
        .filter(|m| !m.is_empty());

    let display_amount = card::format_amount(amount_raw);
    let amount_class = if amount_raw.trim().starts_with('-') {
        "pfin-debit"
    } else {
        "pfin-credit"
    };

    let description = enriched
        .item
        .get("transaction")
        .and_then(|t| t.get("description"))
        .and_then(|v| v.as_str());
    let info = enriched.item.get("info").and_then(|v| v.as_str());
    let splits = extract_counterparty_splits(&enriched.item, &enriched.pending_import);
    let show_split_amounts = splits.len() > 1;

    let mut html = String::with_capacity(512);
    html.push_str(&format!(
        "    <tr class=\"pfin-batch-row\" data-index=\"{idx}\">\n"
    ));

    // Import: payee, date, account, memo.
    html.push_str(&format!(
        "      <td class=\"pfin-batch-payee\">\
         <span class=\"pfin-batch-payee-name\">{}</span>",
        html_escape(payee)
    ));
    if !date.is_empty() {
        html.push_str(&format!(
            " <span class=\"pfin-batch-date\">{}</span>",
            html_escape(date)
        ));
    }
    if let Some(acct) = import_account {
        html.push_str(&format!(
            "<div class=\"pfin-batch-import-account\">{}</div>",
            html_escape(acct)
        ));
    }
    if let Some(memo) = import_memo {
        html.push_str(&format!(
            "<div class=\"pfin-batch-import-memo\">{}</div>",
            html_escape(memo)
        ));
    }
    html.push_str("</td>\n");

    // Import amount.
    html.push_str(&format!(
        "      <td class=\"pfin-batch-amount {amount_class}\">{}</td>\n",
        html_escape(&display_amount)
    ));

    // Categorization: description, counterparty splits, optional info.
    html.push_str("      <td class=\"pfin-batch-category\">");
    if let Some(desc) = description {
        html.push_str(&format!(
            "<div class=\"pfin-batch-desc\">{}</div>",
            html_escape(desc)
        ));
    }
    if !splits.is_empty() {
        html.push_str("<div class=\"pfin-batch-splits\">");
        for split in &splits {
            html.push_str(&format!(
                "<div class=\"pfin-batch-split\">\
                 <span class=\"pfin-batch-split-account\">{}</span>",
                html_escape(&split.account),
            ));
            if show_split_amounts {
                let split_amount = card::format_amount(&split.amount);
                let split_amount_class = if split.amount.trim().starts_with('-') {
                    "pfin-debit"
                } else {
                    "pfin-credit"
                };
                html.push_str(&format!(
                    "<span class=\"pfin-batch-split-amount {split_amount_class}\">{}</span>",
                    html_escape(&split_amount),
                ));
            }
            if let Some(memo) = &split.memo {
                html.push_str(&format!(
                    "<span class=\"pfin-batch-split-memo\">{}</span>",
                    html_escape(memo)
                ));
            }
            html.push_str("</div>");
        }
        html.push_str("</div>");
    }
    if let Some(info_text) = info {
        html.push_str(&format!(
            "<div class=\"pfin-batch-info\">{}</div>",
            html_escape(info_text)
        ));
    }
    html.push_str("</td>\n");

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
    let import_account = pending.get("account").and_then(|v| v.as_str());
    let import_memo = pending
        .get("memo")
        .and_then(|v| v.as_str())
        .filter(|m| !m.is_empty());

    let display_amount = card::format_amount(amount_raw);
    let amount_class = if amount_raw.trim().starts_with('-') {
        "pfin-debit"
    } else {
        "pfin-credit"
    };

    let description = enriched
        .item
        .get("transaction")
        .and_then(|t| t.get("description"))
        .and_then(|v| v.as_str());
    let info = enriched.item.get("info").and_then(|v| v.as_str());
    let splits = extract_counterparty_splits(&enriched.item, &enriched.pending_import);
    let show_split_amounts = splits.len() > 1;

    let mut html = String::with_capacity(512);

    // Outer wrapper for swipe mechanics: reveal layer behind, card on top.
    html.push_str(&format!(
        "  <div class=\"pfin-batch-swipe-item\" data-index=\"{idx}\">\n"
    ));

    // Reveal layer (shown behind the card during swipe).
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
    html.push_str("    <div class=\"pfin-batch-card\">\n");

    // Header: payee + amount from the bank import.
    html.push_str(&format!(
        "      <div class=\"pfin-batch-card-header\">\
         <span class=\"pfin-batch-payee-name\">{}</span>\
         <span class=\"pfin-batch-amount {amount_class}\">{}</span>\
         </div>\n",
        html_escape(payee),
        html_escape(&display_amount),
    ));

    // Import details: account, memo.
    if import_account.is_some() || import_memo.is_some() {
        html.push_str("      <div class=\"pfin-batch-import-detail\">\n");
        if let Some(acct) = import_account {
            html.push_str(&format!(
                "        <span class=\"pfin-batch-import-account\">{}</span>\n",
                html_escape(acct)
            ));
        }
        if let Some(memo) = import_memo {
            html.push_str(&format!(
                "        <span class=\"pfin-batch-import-memo\">{}</span>\n",
                html_escape(memo)
            ));
        }
        html.push_str("      </div>\n");
    }

    // Transaction description (if present).
    if let Some(desc) = description {
        html.push_str(&format!(
            "      <div class=\"pfin-batch-desc\">{}</div>\n",
            html_escape(desc)
        ));
    }

    // All counterparty splits.
    if !splits.is_empty() {
        html.push_str("      <div class=\"pfin-batch-splits\">\n");
        for split in &splits {
            html.push_str(&format!(
                "        <div class=\"pfin-batch-split\">\
                 <span class=\"pfin-batch-split-account\">{}</span>",
                html_escape(&split.account),
            ));
            if show_split_amounts {
                let split_amount = card::format_amount(&split.amount);
                let split_amount_class = if split.amount.trim().starts_with('-') {
                    "pfin-debit"
                } else {
                    "pfin-credit"
                };
                html.push_str(&format!(
                    "<span class=\"pfin-batch-split-amount {split_amount_class}\">{}</span>",
                    html_escape(&split_amount),
                ));
            }
            if let Some(memo) = &split.memo {
                html.push_str(&format!(
                    "<span class=\"pfin-batch-split-memo\">{}</span>",
                    html_escape(memo)
                ));
            }
            html.push_str("</div>\n");
        }
        html.push_str("      </div>\n");
    }

    // Optional info from CC.
    if let Some(info_text) = info {
        html.push_str(&format!(
            "      <div class=\"pfin-batch-info\">{}</div>\n",
            html_escape(info_text)
        ));
    }

    html.push_str("    </div>\n"); // card
    html.push_str("  </div>\n"); // swipe-item

    html
}

/// Extract all counterparty splits from a batch item's proposed transaction.
///
/// Uses the import's account name and absolute amount to identify and skip the
/// import's own split (same logic as `render_compact_proposal` in propose.rs).
/// Returns all remaining splits with account, amount, and optional memo.
fn extract_counterparty_splits(item: &Value, pending_import: &Value) -> Vec<CounterpartySplit> {
    let splits = match item
        .get("transaction")
        .and_then(|t| t.get("splits"))
        .and_then(|s| s.as_array())
    {
        Some(s) => s,
        None => return Vec::new(),
    };

    let import_identity: Option<(&str, &str)> =
        pending_import.get("account").and_then(|v| v.as_str()).zip(
            pending_import
                .get("amount")
                .and_then(|v| v.as_str())
                .map(|a| a.trim().trim_start_matches('-')),
        );

    let mut removed_import = false;
    let mut result = Vec::new();

    for split in splits {
        let split_acct = split.get("account").and_then(|v| v.as_str()).unwrap_or("?");
        let split_amt = split.get("amount").and_then(|v| v.as_str()).unwrap_or("0");
        let split_memo = split
            .get("memo")
            .and_then(|v| v.as_str())
            .filter(|m| !m.is_empty())
            .map(|m| m.to_string());

        // Skip the import's own split (first match only, by account + absolute amount).
        if !removed_import && let Some((import_acct, import_abs)) = import_identity {
            let split_abs = split_amt.trim().trim_start_matches('-');
            if split_acct == import_acct && split_abs == import_abs {
                removed_import = true;
                continue;
            }
        }

        result.push(CounterpartySplit {
            account: split_acct.to_string(),
            amount: split_amt.to_string(),
            memo: split_memo,
        });
    }

    result
}

/// Execute a batch of reconciliations for accepted items.
///
/// Runs sequentially to avoid pfin ledger conflicts. Returns a structured
/// JSON result for CC.
pub async fn execute_batch(
    tool_input: &Value,
    decisions: &[(usize, bool)],
    ctx: &SubprocessExecContext<'_>,
    username: &str,
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

    let total = items.len();
    let mut results: Vec<Value> = Vec::new();
    let mut accepted_count = 0usize;
    let mut rejected_count = 0usize;
    let mut failed_count = 0usize;

    // Process user decisions.
    for &(index, accepted) in decisions {
        let item = match items.get(index) {
            Some(i) => i,
            None => {
                warn!(index, "batch decision index out of range");
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

        // Execute reconciliation for this item.
        let transaction = item.get("transaction");
        if transaction.is_none() {
            failed_count += 1;
            results.push(serde_json::json!({
                "index": index,
                "import_id": import_id,
                "status": "error",
                "error": "missing transaction in item",
            }));
            continue;
        }

        let reconcile_input = serde_json::json!({
            "import_id": import_id,
            "transaction": transaction.unwrap(),
        });

        match crate::run_pfin_reconcile(&reconcile_input, ctx, username).await {
            Ok(_output) => {
                accepted_count += 1;
                info!(index, import_id, "batch item reconciled");
                results.push(serde_json::json!({
                    "index": index,
                    "import_id": import_id,
                    "status": "reconciled",
                }));
            }
            Err(e) => {
                failed_count += 1;
                warn!(index, import_id, error = %e, "batch item reconciliation failed");
                results.push(serde_json::json!({
                    "index": index,
                    "import_id": import_id,
                    "status": "error",
                    "error": e,
                }));
            }
        }
    }

    // Append enrichment failures.
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
    use super::*;
    use serde_json::json;

    fn sample_enriched_item(index: usize) -> EnrichedBatchItem {
        EnrichedBatchItem {
            original_index: index,
            item: json!({
                "import_id": format!("imp-{index}"),
                "transaction": {
                    "description": "Grocery Store",
                    "splits": [
                        { "account": "Expenses:Food:Groceries", "amount": "-52.30" },
                        { "account": "Assets:Checking", "amount": "52.30" }
                    ]
                }
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
    fn render_table_produces_valid_html() {
        let items = vec![sample_enriched_item(0), sample_enriched_item(1)];
        let html = render_batch_table(&items).unwrap();
        assert!(html.contains("<brenn-pfin-batch-table>"));
        assert!(html.contains("</brenn-pfin-batch-table>"));
        assert!(html.contains("Whole Foods"));
        assert!(html.contains("-$52.30"));
        assert!(
            html.contains("Grocery Store"),
            "should show transaction description"
        );
        assert!(
            html.contains("Expenses:Food:Groceries"),
            "should show counterparty account"
        );
        // Import account should be shown.
        assert!(
            html.contains("pfin-batch-import-account"),
            "should show import account"
        );
        // Single counterparty — should NOT show split amount (redundant with import).
        assert!(
            !html.contains("pfin-batch-split-amount"),
            "single-split should omit split amount"
        );
        assert!(html.contains("data-index=\"0\""));
        assert!(html.contains("data-index=\"1\""));
        assert!(html.contains("pfin-batch-accept"));
        assert!(html.contains("pfin-batch-reject"));
        assert!(!html.contains("pfin-batch-info"));
    }

    #[test]
    fn render_table_empty_returns_none() {
        assert!(render_batch_table(&[]).is_none());
    }

    #[test]
    fn render_swipe_produces_valid_html() {
        let items = vec![sample_enriched_item(0)];
        let html = render_batch_swipe(&items).unwrap();
        assert!(html.contains("<brenn-pfin-batch-swipe>"));
        assert!(html.contains("</brenn-pfin-batch-swipe>"));
        assert!(html.contains("Whole Foods"));
        assert!(
            html.contains("Grocery Store"),
            "should show transaction description"
        );
        assert!(
            html.contains("Expenses:Food:Groceries"),
            "should show counterparty account"
        );
        // Import account should be shown.
        assert!(
            html.contains("pfin-batch-import-account"),
            "should show import account"
        );
        // Single counterparty — should NOT show split amount.
        assert!(
            !html.contains("pfin-batch-split-amount"),
            "single-split should omit split amount"
        );
        assert!(html.contains("pfin-batch-card"));
        assert!(html.contains("pfin-batch-reveal-accept"));
        assert!(html.contains("pfin-batch-reveal-reject"));
        assert!(html.contains("Accept"));
        assert!(html.contains("Reject"));
        assert!(!html.contains("pfin-batch-info"));
    }

    #[test]
    fn render_swipe_empty_returns_none() {
        assert!(render_batch_swipe(&[]).is_none());
    }

    #[test]
    fn table_escapes_html_in_all_fields() {
        let item = EnrichedBatchItem {
            original_index: 0,
            item: json!({
                "import_id": "imp-0",
                "info": "<img onerror=alert(1)>",
                "transaction": {
                    "description": "<b>bold</b>",
                    "splits": [
                        { "account": "Evil<script>", "amount": "-10", "memo": "<i>memo</i>" },
                        { "account": "Assets:Checking", "amount": "10" }
                    ]
                }
            }),
            pending_import: json!({
                "payee": "<script>alert(1)</script>",
                "amount": "-10",
                "account": "Evil<account>",
                "memo": "<marquee>xss</marquee>"
            }),
        };
        let table = render_batch_table(std::slice::from_ref(&item)).unwrap();
        assert!(
            !table.contains("<script>alert"),
            "payee not escaped in table"
        );
        assert!(
            !table.contains("<b>bold"),
            "description not escaped in table"
        );
        assert!(
            !table.contains("<i>memo"),
            "split memo not escaped in table"
        );
        assert!(!table.contains("<img onerror"), "info not escaped in table");
        assert!(
            !table.contains("<marquee>"),
            "import memo not escaped in table"
        );
        assert!(
            !table.contains("<account>"),
            "import account not escaped in table"
        );
        assert!(table.contains("&lt;script&gt;"));
        assert!(table.contains("&lt;b&gt;"));

        let swipe = render_batch_swipe(&[item]).unwrap();
        assert!(
            !swipe.contains("<script>alert"),
            "payee not escaped in swipe"
        );
        assert!(
            !swipe.contains("<b>bold"),
            "description not escaped in swipe"
        );
        assert!(
            !swipe.contains("<i>memo"),
            "split memo not escaped in swipe"
        );
        assert!(!swipe.contains("<img onerror"), "info not escaped in swipe");
        assert!(
            !swipe.contains("<marquee>"),
            "import memo not escaped in swipe"
        );
        assert!(
            !swipe.contains("<account>"),
            "import account not escaped in swipe"
        );
    }

    #[test]
    fn counterparty_splits_skips_import_split() {
        let item = json!({
            "transaction": {
                "splits": [
                    { "account": "Assets:Checking", "amount": "50" },
                    { "account": "Expenses:Food", "amount": "-50" }
                ]
            }
        });
        let pending = json!({
            "account": "Assets:Checking",
            "amount": "-50"
        });
        let splits = extract_counterparty_splits(&item, &pending);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].account, "Expenses:Food");
        assert_eq!(splits[0].amount, "-50");
    }

    #[test]
    fn counterparty_splits_returns_all_without_import_info() {
        let item = json!({
            "transaction": {
                "splits": [
                    { "account": "Expenses:Food", "amount": "-50" },
                    { "account": "Assets:Checking", "amount": "50" }
                ]
            }
        });
        // No account/amount in pending_import → returns all splits.
        let pending = json!({ "payee": "Store" });
        let splits = extract_counterparty_splits(&item, &pending);
        assert_eq!(splits.len(), 2);
        assert_eq!(splits[0].account, "Expenses:Food");
        assert_eq!(splits[1].account, "Assets:Checking");
    }

    #[test]
    fn counterparty_splits_empty_on_no_splits() {
        let item = json!({ "transaction": { "splits": [] } });
        let pending = json!({});
        assert!(extract_counterparty_splits(&item, &pending).is_empty());
    }

    #[test]
    fn counterparty_splits_empty_on_missing_transaction() {
        let item = json!({ "import_id": "x" });
        let pending = json!({});
        assert!(extract_counterparty_splits(&item, &pending).is_empty());
    }

    #[test]
    fn counterparty_splits_empty_memo_treated_as_none() {
        let item = json!({
            "transaction": {
                "splits": [
                    { "account": "Expenses:Food", "amount": "-50", "memo": "" },
                ]
            }
        });
        let pending = json!({});
        let splits = extract_counterparty_splits(&item, &pending);
        assert_eq!(splits.len(), 1);
        assert!(splits[0].memo.is_none(), "empty memo should be None");
    }

    #[test]
    fn counterparty_splits_multi_split() {
        let item = json!({
            "transaction": {
                "splits": [
                    { "account": "Assets:Checking", "amount": "100" },
                    { "account": "Expenses:Food", "amount": "-80", "memo": "groceries" },
                    { "account": "Expenses:Household", "amount": "-20" }
                ]
            }
        });
        let pending = json!({
            "account": "Assets:Checking",
            "amount": "-100"
        });
        let splits = extract_counterparty_splits(&item, &pending);
        assert_eq!(splits.len(), 2, "should return both counterparty splits");
        assert_eq!(splits[0].account, "Expenses:Food");
        assert_eq!(splits[0].amount, "-80");
        assert_eq!(splits[0].memo.as_deref(), Some("groceries"));
        assert_eq!(splits[1].account, "Expenses:Household");
        assert_eq!(splits[1].amount, "-20");
        assert!(splits[1].memo.is_none());
    }

    #[test]
    fn render_shows_multi_split() {
        let item = EnrichedBatchItem {
            original_index: 0,
            item: json!({
                "import_id": "imp-0",
                "transaction": {
                    "description": "Mixed purchase",
                    "splits": [
                        { "account": "Assets:Checking", "amount": "100" },
                        { "account": "Expenses:Food", "amount": "-80", "memo": "groceries" },
                        { "account": "Expenses:Household", "amount": "-20" }
                    ]
                }
            }),
            pending_import: json!({
                "payee": "Store",
                "amount": "-100",
                "account": "Assets:Checking"
            }),
        };
        // Both table and swipe should show all counterparty splits with amounts and memos.
        let table = render_batch_table(std::slice::from_ref(&item)).unwrap();
        assert!(table.contains("Expenses:Food"), "table missing Food split");
        assert!(
            table.contains("Expenses:Household"),
            "table missing Household split"
        );
        assert!(table.contains("-$80"), "table missing Food amount");
        assert!(table.contains("-$20"), "table missing Household amount");
        assert!(table.contains("groceries"), "table missing memo");

        let swipe = render_batch_swipe(&[item]).unwrap();
        assert!(swipe.contains("Expenses:Food"), "swipe missing Food split");
        assert!(
            swipe.contains("Expenses:Household"),
            "swipe missing Household split"
        );
        assert!(swipe.contains("-$80"), "swipe missing Food amount");
        assert!(swipe.contains("-$20"), "swipe missing Household amount");
        assert!(swipe.contains("groceries"), "swipe missing memo");
    }

    #[test]
    fn render_shows_info_when_present() {
        let mut item = sample_enriched_item(0);
        item.item["info"] = json!("Matched by recurring pattern");
        let table = render_batch_table(&[item.clone()]).unwrap();
        assert!(table.contains("Matched by recurring pattern"));
        assert!(table.contains("pfin-batch-info"));

        let swipe = render_batch_swipe(&[item]).unwrap();
        assert!(swipe.contains("Matched by recurring pattern"));
        assert!(swipe.contains("pfin-batch-info"));
    }

    #[test]
    fn render_omits_description_when_absent() {
        let mut item = sample_enriched_item(0);
        // Remove description from transaction.
        item.item["transaction"] = json!({
            "splits": [
                { "account": "Expenses:Food", "amount": "-52.30" },
                { "account": "Assets:Checking", "amount": "52.30" }
            ]
        });
        let table = render_batch_table(&[item.clone()]).unwrap();
        assert!(
            !table.contains("pfin-batch-desc"),
            "should not render desc element when absent"
        );

        let swipe = render_batch_swipe(&[item]).unwrap();
        assert!(
            !swipe.contains("pfin-batch-desc"),
            "should not render desc element when absent"
        );
    }

    #[test]
    fn summary_shows_item_count() {
        let input = json!({
            "items": [
                { "import_id": "a", "transaction": { "splits": [] } },
                { "import_id": "b", "transaction": { "splits": [] } },
            ]
        });
        let tool = BatchReconcileTool;
        let decision = ToolResponseDecision::Allow {
            updated_input: None,
        };
        let summary = tool.format_summary(&input, &decision).unwrap();
        assert!(summary.contains("2 items"), "got: {summary}");
        assert!(summary.contains("approved"), "got: {summary}");
    }

    #[test]
    fn summary_denied() {
        let input = json!({
            "items": [
                { "import_id": "a", "transaction": { "splits": [] } },
            ]
        });
        let tool = BatchReconcileTool;
        let decision = ToolResponseDecision::Deny { reason: None };
        let summary = tool.format_summary(&input, &decision).unwrap();
        assert!(summary.contains("denied"), "got: {summary}");
        assert!(summary.contains("\u{2718}"), "got: {summary}");
    }

    #[test]
    fn date_truncated_to_ymd() {
        let mut item = sample_enriched_item(0);
        item.pending_import["date"] = json!("2025-03-28T12:34:56Z");
        let items = vec![item];
        let html = render_batch_table(&items).unwrap();
        assert!(html.contains("2025-03-28"));
        assert!(!html.contains("T12:34:56Z"));
    }
}

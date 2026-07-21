//! Shared transaction card rendering for pfin tools.
//!
//! Used by both `ReconcileTool` (single card with approve/deny) and
//! `ProposeReconciliationTool` (multiple selectable cards).

use brenn_lib::util::html_escape;

/// Render a transaction card from `reconcile`-shaped tool input
/// (`{ import_id, transaction }`). Includes the import reference footer.
///
/// Returns `None` if required fields are missing (caller should fall back to
/// JSON display).
pub fn render_card(tool_input: &serde_json::Value) -> Option<String> {
    let import_id = tool_input.get("import_id")?.as_str()?;
    let txn = tool_input.get("transaction")?;
    render_transaction_card(txn, Some(import_id))
}

/// Render a transaction card from a transaction object.
///
/// If `import_id` is Some, a footer with the truncated import ref is appended.
pub fn render_transaction_card(txn: &serde_json::Value, import_id: Option<&str>) -> Option<String> {
    let date = txn
        .get("date")
        .and_then(|v| v.as_str())
        .unwrap_or("\u{2014}");
    let description = txn
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("\u{2014}");
    let splits = txn.get("splits").and_then(|v| v.as_array())?;
    if splits.is_empty() {
        return None;
    }

    let op_label = match txn.get("id").and_then(|v| v.as_str()) {
        Some(id) => {
            let short = truncate(id, 8);
            format!("Update: {short}\u{2026}")
        }
        None => "New transaction".to_string(),
    };

    let mut html = String::with_capacity(512);
    html.push_str("<div class=\"pfin-reconcile\">\n");

    // Header: operation, date, description.
    html.push_str("  <div class=\"pfin-reconcile-header\">\n");
    html.push_str(&format!(
        "    <span class=\"pfin-reconcile-op\">{}</span>\n",
        html_escape(&op_label)
    ));
    html.push_str(&format!(
        "    <span class=\"pfin-reconcile-date\">{}</span>\n",
        html_escape(date)
    ));
    html.push_str(&format!(
        "    <span class=\"pfin-reconcile-desc\">{}</span>\n",
        html_escape(description)
    ));
    html.push_str("  </div>\n");

    // Splits table.
    html.push_str("  <table class=\"pfin-splits\">\n");
    for split in splits {
        let account = split.get("account").and_then(|v| v.as_str()).unwrap_or("?");
        let amount_str = split.get("amount").and_then(|v| v.as_str()).unwrap_or("0");
        let memo = split.get("memo").and_then(|v| v.as_str()).unwrap_or("");

        let amount_class = if amount_str.starts_with('-') {
            "pfin-debit"
        } else {
            "pfin-credit"
        };

        let display_amount = format_amount(amount_str);

        html.push_str("    <tr>\n");
        html.push_str(&format!(
            "      <td class=\"pfin-split-account\">{}</td>\n",
            html_escape(account)
        ));
        html.push_str(&format!(
            "      <td class=\"pfin-split-amount {}\">{}</td>\n",
            amount_class,
            html_escape(&display_amount)
        ));
        if !memo.is_empty() {
            html.push_str(&format!(
                "      <td class=\"pfin-split-memo\">{}</td>\n",
                html_escape(memo)
            ));
        } else {
            html.push_str("      <td class=\"pfin-split-memo\"></td>\n");
        }
        html.push_str("    </tr>\n");
    }
    html.push_str("  </table>\n");

    // Import reference (optional).
    if let Some(id) = import_id {
        let short_id = truncate(id, 12);
        html.push_str(&format!(
            "  <div class=\"pfin-reconcile-ref\">Import: {}</div>\n",
            html_escape(short_id)
        ));
    }

    html.push_str("</div>");

    Some(html)
}

/// Render the pending import header block.
///
/// Accepts the JSON output from `pf show <import_id> --json`. Shows the bank's
/// imported data (payee, amount, date, account, memo) so the user has context
/// when reviewing or choosing among reconciliation proposals.
pub fn render_import_header(pending: &serde_json::Value) -> Option<String> {
    let payee = pending.get("payee").and_then(|v| v.as_str())?;
    let amount_raw = pending.get("amount").and_then(|v| v.as_str())?;
    // pf show emits RFC3339 dates; extract just the YYYY-MM-DD portion.
    let date = pending
        .get("date")
        .and_then(|v| v.as_str())
        .map(|d| d.get(..10).unwrap_or(d));
    let account = pending.get("account").and_then(|v| v.as_str());
    let memo = pending.get("memo").and_then(|v| v.as_str());

    let display_amount = format_amount(amount_raw);
    let amount_class = if amount_raw.trim().starts_with('-') {
        "pfin-debit"
    } else {
        "pfin-credit"
    };

    let mut html = String::with_capacity(512);
    html.push_str("  <div class=\"pfin-import-header\">\n");
    html.push_str("    <div class=\"pfin-import-summary\">\n");
    html.push_str(&format!(
        "      <span class=\"pfin-import-payee\">{}</span>\n",
        html_escape(payee)
    ));
    html.push_str(&format!(
        "      <span class=\"pfin-import-amount {amount_class}\">{}</span>\n",
        html_escape(&display_amount)
    ));
    html.push_str("    </div>\n");

    // Detail line — only if there's at least one detail field.
    if date.is_some() || account.is_some() || memo.is_some() {
        html.push_str("    <div class=\"pfin-import-detail\">\n");
        if let Some(d) = date {
            html.push_str(&format!(
                "      <span class=\"pfin-import-date\">{}</span>\n",
                html_escape(d)
            ));
        }
        if let Some(a) = account {
            html.push_str(&format!(
                "      <span class=\"pfin-import-account\">{}</span>\n",
                html_escape(a)
            ));
        }
        if let Some(m) = memo {
            html.push_str(&format!(
                "      <span class=\"pfin-import-memo\">{}</span>\n",
                html_escape(m)
            ));
        }
        html.push_str("    </div>\n");
    }

    html.push_str("  </div>\n");
    Some(html)
}

/// Truncate a string to at most `max_chars` characters (UTF-8 safe).
pub fn truncate(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((byte_pos, _)) => &s[..byte_pos],
        None => s,
    }
}

/// Format an amount string for display. Adds $ prefix, handles negatives.
///
/// Input: string like "-52.30" or "52.30".
/// Output: "-$52.30" or "$52.30".
pub fn format_amount(amount: &str) -> String {
    let trimmed = amount.trim();
    if let Some(rest) = trimmed.strip_prefix('-') {
        format!("-${rest}")
    } else {
        format!("${trimmed}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_tool_input() -> serde_json::Value {
        json!({
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

    // -- render_card --

    #[test]
    fn render_card_produces_card_with_import_ref() {
        let html = render_card(&sample_tool_input()).unwrap();
        assert!(html.contains("pfin-reconcile"), "missing card class");
        assert!(html.contains("pfin-reconcile-ref"), "missing import ref");
        assert!(html.contains("imp-abc-123-"), "missing truncated import id");
    }

    #[test]
    fn render_card_none_on_missing_import_id() {
        let input = json!({
            "transaction": {
                "date": "2025-01-01",
                "description": "X",
                "splits": [{ "account": "A", "amount": "1" }]
            }
        });
        assert!(render_card(&input).is_none());
    }

    #[test]
    fn render_card_none_on_empty_splits() {
        let input = json!({
            "import_id": "imp",
            "transaction": { "date": "2025-01-01", "description": "X", "splits": [] }
        });
        assert!(render_card(&input).is_none());
    }

    // -- render_transaction_card --

    #[test]
    fn transaction_card_shows_date_desc_splits() {
        let txn = json!({
            "date": "2025-03-28",
            "description": "Grocery Store",
            "splits": [
                { "account": "Expenses:Food", "amount": "-52.30" },
                { "account": "Assets:Checking", "amount": "52.30" }
            ]
        });
        let html = render_transaction_card(&txn, None).unwrap();
        assert!(html.contains("2025-03-28"));
        assert!(html.contains("Grocery Store"));
        assert!(html.contains("Expenses:Food"));
        assert!(html.contains("-$52.30"));
        assert!(html.contains("pfin-debit"));
        assert!(html.contains("pfin-credit"));
    }

    #[test]
    fn transaction_card_with_import_id() {
        let txn = json!({
            "date": "2025-01-01",
            "description": "X",
            "splits": [{ "account": "A", "amount": "1" }]
        });
        let html = render_transaction_card(&txn, Some("imp-abc-123-def-456")).unwrap();
        assert!(html.contains("pfin-reconcile-ref"));
        assert!(html.contains("imp-abc-123-"));
    }

    #[test]
    fn transaction_card_escapes_html() {
        let txn = json!({
            "date": "2025-01-01",
            "description": "<script>alert(1)</script>",
            "splits": [{ "account": "A&B", "amount": "1" }]
        });
        let html = render_transaction_card(&txn, None).unwrap();
        assert!(!html.contains("<script>alert"));
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("A&amp;B"));
    }

    #[test]
    fn transaction_card_none_on_missing_splits() {
        let txn = json!({ "date": "2025-01-01", "description": "X" });
        assert!(render_transaction_card(&txn, None).is_none());
    }

    // -- truncate --

    #[test]
    fn truncate_ascii() {
        assert_eq!(truncate("abcdefghij", 8), "abcdefgh");
    }

    #[test]
    fn truncate_short_unchanged() {
        assert_eq!(truncate("abc", 8), "abc");
    }

    #[test]
    fn truncate_multibyte_safe() {
        assert_eq!(truncate("café", 3), "caf");
        assert_eq!(truncate("ééé", 2), "éé");
    }

    // -- format_amount --

    #[test]
    fn format_amount_positive() {
        assert_eq!(format_amount("52.30"), "$52.30");
    }

    #[test]
    fn format_amount_negative() {
        assert_eq!(format_amount("-52.30"), "-$52.30");
    }

    #[test]
    fn format_amount_zero() {
        assert_eq!(format_amount("0"), "$0");
    }

    #[test]
    fn format_amount_whitespace_trimmed() {
        assert_eq!(format_amount("  10.00  "), "$10.00");
        assert_eq!(format_amount("  -5  "), "-$5");
    }
}

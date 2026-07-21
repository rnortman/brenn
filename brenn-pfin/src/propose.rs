//! ProposeReconciliationTool — interactive multi-choice approval display for
//! brenn's `ProposeReconciliation` noop MCP tool.
//!
//! Renders multiple transaction cards as selectable proposals inside a
//! `<brenn-pfin-propose>` custom element. The user picks one (or denies with
//! feedback). NOT wrapped in `<brenn-tool-approve>` — the component handles
//! its own interaction.
//!
//! The tool is advertised by brenn's noop MCP server (not pfin's). CC calls
//! `mcp__brenn__ProposeReconciliation`; brenn intercepts via hooks.
//! `execute_selection()` runs the actual reconcile via the pfin CLI.

use brenn_lib::app::AppTool;
use brenn_lib::subprocess::SubprocessExecContext;
use brenn_lib::util::{html_escape, json_for_script_tag};
use brenn_lib::ws_types::ToolResponseDecision;
use serde_json::Value;
use tracing::{info, warn};

use crate::card;

/// MCP tool name for ProposeReconciliation (brenn's noop MCP server).
pub const MCP_PROPOSE_RECONCILIATION_TOOL: &str = "mcp__brenn__ProposeReconciliation";

/// Formats the `mcp__brenn__ProposeReconciliation` tool for the approval dialog.
pub struct ProposeReconciliationTool;

impl AppTool for ProposeReconciliationTool {
    fn name(&self) -> &str {
        MCP_PROPOSE_RECONCILIATION_TOOL
    }

    fn format_display(&self, tool_input: &Value) -> Option<String> {
        match render_proposals(tool_input) {
            Some(html) => Some(html),
            None => {
                warn!(
                    "ProposeReconciliation: unexpected input shape, \
                     falling back to JSON display"
                );
                None
            }
        }
    }

    fn format_summary(
        &self,
        tool_input: &Value,
        decision: &ToolResponseDecision,
    ) -> Option<String> {
        let proposals = tool_input.get("proposals")?.as_array()?;
        let count = proposals.len();

        let detail = match decision {
            ToolResponseDecision::Allow { updated_input } => {
                // Note: currently emit_tool_summary in active_bridge always passes
                // Allow { updated_input: None } because PostToolUse doesn't carry
                // the browser's decision. The selected_idx extraction below is
                // future-proofing — when the summary path eventually gets the real
                // decision, it'll show the selected label without changes here.
                let selected_idx = updated_input
                    .as_ref()
                    .and_then(|ui| ui.get("selected"))
                    .and_then(|v| v.as_u64());

                match selected_idx {
                    Some(idx) => {
                        let label = proposals
                            .get(idx as usize)
                            .and_then(|p| p.get("label"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("?");
                        format!(
                            "<span class=\"ts-file\">{count} options</span> \
                             <span class=\"ts-pattern\">\u{2192} {}</span>",
                            html_escape(label),
                        )
                    }
                    None => {
                        format!(
                            "<span class=\"ts-file\">{count} options</span> \
                             <span class=\"ts-pattern\">\u{2192} approved</span>",
                        )
                    }
                }
            }
            ToolResponseDecision::Deny { .. } => {
                format!(
                    "<span class=\"ts-denied\" title=\"Denied\">\u{2718}</span> \
                     <span class=\"ts-file\">{count} options</span> \
                     <span class=\"ts-pattern\">denied</span>",
                )
            }
        };

        Some(detail)
    }
}

/// Render the full `<brenn-pfin-propose>` HTML with proposal cards.
///
/// Returns `None` on unexpected input shape.
fn render_proposals(tool_input: &Value) -> Option<String> {
    let import_id = tool_input.get("import_id")?.as_str()?;
    let proposals = tool_input.get("proposals")?.as_array()?;

    if proposals.is_empty() || proposals.len() > 5 {
        warn!(
            "ProposeReconciliation: expected 1-5 proposals, got {}",
            proposals.len()
        );
        return None;
    }

    // Extract the import account + amount so we can identify and suppress the
    // import's own split from proposals. We match on both account name and
    // absolute amount to avoid suppressing the wrong split.
    let import_identity: Option<(&str, &str)> = tool_input.get("_pending_import").and_then(|p| {
        let acct = p.get("account")?.as_str()?;
        let amt = p.get("amount")?.as_str()?;
        Some((acct, amt))
    });

    // Embed minimal config in the script tag. The component counts DOM children
    // for proposal_count, so we only need import_id here.
    let config = serde_json::json!({ "import_id": import_id });
    let config_json = json_for_script_tag(&config);

    let mut html = String::with_capacity(2048);
    html.push_str("<brenn-pfin-propose>\n");
    html.push_str(&format!(
        "  <script type=\"application/json\">{config_json}</script>\n"
    ));

    // Render pending import header if backend injected it.
    // The `_pending_import` field is added by active_bridge after fetching
    // import details via `pf show`. It's not part of the MCP schema.
    if let Some(pending) = tool_input.get("_pending_import") {
        match card::render_import_header(pending) {
            Some(header) => html.push_str(&header),
            None => {
                warn!(
                    "ProposeReconciliation: _pending_import present but malformed, skipping header"
                );
            }
        }
    }

    for (i, proposal) in proposals.iter().enumerate() {
        let label = proposal
            .get("label")
            .and_then(|v| v.as_str())
            .unwrap_or("Option");
        let txn = match proposal.get("transaction") {
            Some(t) => t,
            None => {
                warn!("ProposeReconciliation: proposal {i} missing transaction");
                return None;
            }
        };

        let compact_html = match render_compact_proposal(txn, import_identity) {
            Some(h) => h,
            None => {
                warn!("ProposeReconciliation: proposal {i} has empty or missing splits");
                return None;
            }
        };
        let num = i + 1; // 1-indexed for display

        html.push_str(&format!(
            "  <div class=\"pfin-proposal\" data-index=\"{i}\">\n"
        ));
        html.push_str(&format!(
            "    <div class=\"pfin-proposal-label\">\
             <span class=\"pfin-proposal-number\">{num}</span> {}</div>\n",
            html_escape(label)
        ));
        html.push_str("    ");
        html.push_str(&compact_html);
        html.push('\n');
        html.push_str("  </div>\n");
    }

    // Shared import reference at the bottom.
    let short_id = card::truncate(import_id, 12);
    html.push_str(&format!(
        "  <div class=\"pfin-reconcile-ref\">Import: {}</div>\n",
        html_escape(short_id)
    ));

    html.push_str("</brenn-pfin-propose>");

    Some(html)
}

/// Render a compact proposal showing only the counterparty splits.
///
/// The import's own split is redundant with the header, so we identify and
/// remove it. We match by both account name and absolute amount (the import
/// amount is the negative of the split amount, or vice versa). If multiple
/// splits match, we remove only the first one.
///
/// For simple 2-way splits (the common case), the single remaining
/// counterparty amount is just the inverse of the import, so we show only
/// the account name. For 3+ counterparty splits, we show account + amount.
///
/// If `import_identity` is None (no `_pending_import`), all splits are shown
/// with amounts as a fallback.
fn render_compact_proposal(txn: &Value, import_identity: Option<(&str, &str)>) -> Option<String> {
    let splits = txn.get("splits").and_then(|v| v.as_array())?;
    if splits.is_empty() {
        return None;
    }

    // Identify and remove the import's own split. Match on account name and
    // absolute amount (the import records e.g. "-52.30" and the split has
    // "52.30", or vice versa — we compare absolute values).
    let counterparty_splits: Vec<&Value> = match import_identity {
        Some((import_acct, import_amt)) => {
            let import_abs = import_amt.trim().trim_start_matches('-');
            let mut removed_one = false;
            splits
                .iter()
                .filter(|s| {
                    if removed_one {
                        return true;
                    }
                    let acct = s.get("account").and_then(|v| v.as_str()).unwrap_or("");
                    let amt = s.get("amount").and_then(|v| v.as_str()).unwrap_or("");
                    let amt_abs = amt.trim().trim_start_matches('-');
                    if acct == import_acct && amt_abs == import_abs {
                        removed_one = true;
                        false
                    } else {
                        true
                    }
                })
                .collect()
        }
        None => splits.iter().collect(),
    };

    // Simple case: exactly one counterparty split — show just the account.
    // Complex case: multiple counterparty splits — show account + amount.
    let show_amounts = counterparty_splits.len() > 1;

    let mut html = String::with_capacity(256);
    html.push_str("<div class=\"pfin-proposal-splits\">\n");

    if counterparty_splits.is_empty() {
        html.push_str(
            "    <div class=\"pfin-proposal-split\">\
             <span class=\"pfin-split-account\">No other splits</span></div>\n",
        );
    }

    for split in &counterparty_splits {
        let account = split.get("account").and_then(|v| v.as_str()).unwrap_or("?");
        let memo = split.get("memo").and_then(|v| v.as_str()).unwrap_or("");

        html.push_str("    <div class=\"pfin-proposal-split\">");
        html.push_str(&format!(
            "<span class=\"pfin-split-account\">{}</span>",
            html_escape(account)
        ));

        if show_amounts {
            let amount_str = split.get("amount").and_then(|v| v.as_str()).unwrap_or("0");
            let amount_class = if amount_str.starts_with('-') {
                "pfin-debit"
            } else {
                "pfin-credit"
            };
            let display_amount = card::format_amount(amount_str);
            html.push_str(&format!(
                " <span class=\"pfin-split-amount {amount_class}\">{}</span>",
                html_escape(&display_amount)
            ));
        }

        if !memo.is_empty() {
            html.push_str(&format!(
                " <span class=\"pfin-split-memo\">{}</span>",
                html_escape(memo)
            ));
        }

        html.push_str("</div>\n");
    }

    html.push_str("  </div>");
    Some(html)
}

/// Fetch pending import details by shelling out to `pf show <import_id> --json`.
///
/// Returns the parsed JSON value on success. The caller injects this into the
/// tool input as `_pending_import` before calling `format_display`.
///
/// Container-aware: uses podman when `container_spawn` is `Some`.
pub async fn fetch_import_details(
    import_id: &str,
    ctx: &SubprocessExecContext<'_>,
) -> Result<Value, String> {
    let env_pairs: Vec<(&str, &str)> = ctx
        .env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let mut cmd = brenn_lib::subprocess::run_in_app_env(
        ctx.command,
        &["--json", "show", import_id],
        ctx.working_dir,
        ctx.container_spawn,
        &env_pairs,
        &[],
    );
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("failed to spawn pf show: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("pf show failed (exit {}): {stderr}", output.status));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim()).map_err(|e| format!("pf show returned invalid JSON: {e}"))
}

/// Execute reconciliation for a selected proposal via the pfin CLI.
///
/// Extracts `proposals[selected].transaction` and `import_id` from the tool
/// input, builds a `ReconcileInput` JSON payload, and pipes it to the pfin
/// binary's `reconcile` subcommand.
///
/// For containerized apps, the pfin command runs inside a podman container
/// using the provided `ContainerSpawnConfig`. For bare-process apps, the
/// pfin binary is spawned directly.
///
/// # Arguments
/// * `tool_input` — The full tool input from CC (has `import_id` and `proposals`).
/// * `selected` — Index of the chosen proposal.
/// * `ctx` — Subprocess execution context (command, env, working_dir, container_spawn).
/// * `username` — Brenn username, passed as `--user` to pfin.
///
/// # Returns
/// The stdout from the pfin reconcile command on success, or an error description.
pub async fn execute_selection(
    tool_input: &Value,
    selected: usize,
    ctx: &SubprocessExecContext<'_>,
    username: &str,
) -> Result<String, String> {
    let import_id = tool_input
        .get("import_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing import_id in tool input".to_string())?;

    let proposals = tool_input
        .get("proposals")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "missing proposals array in tool input".to_string())?;

    let proposal = proposals.get(selected).ok_or_else(|| {
        format!(
            "selected index {selected} out of range (have {})",
            proposals.len()
        )
    })?;

    let transaction = proposal
        .get("transaction")
        .ok_or_else(|| format!("proposal {selected} missing transaction"))?;

    let reconcile_input = serde_json::json!({
        "import_id": import_id,
        "transaction": transaction,
    });

    info!(
        pfin_command = ctx.command,
        selected, import_id, "executing reconciliation for selected proposal"
    );

    let result = crate::run_pfin_reconcile(&reconcile_input, ctx, username).await;

    if result.is_ok() {
        info!("pfin reconcile succeeded");
    }

    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use super::*;
    use serde_json::json;

    fn valid_input() -> Value {
        json!({
            "import_id": "imp-abc-123-def-456",
            "proposals": [
                {
                    "label": "Groceries",
                    "transaction": {
                        "date": "2025-03-28",
                        "description": "Grocery Store",
                        "splits": [
                            { "account": "Expenses:Food:Groceries", "amount": "-52.30" },
                            { "account": "Assets:Checking", "amount": "52.30" }
                        ]
                    }
                },
                {
                    "label": "Restaurant",
                    "transaction": {
                        "date": "2025-03-28",
                        "description": "Grocery Store",
                        "splits": [
                            { "account": "Expenses:Dining", "amount": "-52.30" },
                            { "account": "Assets:Checking", "amount": "52.30" }
                        ]
                    }
                }
            ]
        })
    }

    // -- format_display: happy path --

    #[test]
    fn display_produces_propose_element() {
        let html = ProposeReconciliationTool
            .format_display(&valid_input())
            .unwrap();
        assert!(
            html.contains("<brenn-pfin-propose>"),
            "missing propose element: {html}"
        );
        assert!(
            html.contains("</brenn-pfin-propose>"),
            "missing closing tag: {html}"
        );
    }

    #[test]
    fn display_not_wrapped_in_tool_approve() {
        let html = ProposeReconciliationTool
            .format_display(&valid_input())
            .unwrap();
        assert!(
            !html.contains("<brenn-tool-approve"),
            "should NOT be wrapped in tool-approve: {html}"
        );
    }

    #[test]
    fn display_shows_proposal_labels() {
        let html = ProposeReconciliationTool
            .format_display(&valid_input())
            .unwrap();
        assert!(html.contains("Groceries"), "missing label 1: {html}");
        assert!(html.contains("Restaurant"), "missing label 2: {html}");
    }

    #[test]
    fn display_shows_number_badges_in_correct_order() {
        let html = ProposeReconciliationTool
            .format_display(&valid_input())
            .unwrap();
        // Badge 1 should appear with "Groceries", badge 2 with "Restaurant".
        let badge1 = "pfin-proposal-number\">1</span> Groceries";
        let badge2 = "pfin-proposal-number\">2</span> Restaurant";
        assert!(
            html.contains(badge1),
            "missing or misplaced badge 1: {html}"
        );
        assert!(
            html.contains(badge2),
            "missing or misplaced badge 2: {html}"
        );
        // Badge 1 must precede badge 2 in the output.
        let pos1 = html.find(badge1).unwrap();
        let pos2 = html.find(badge2).unwrap();
        assert!(pos1 < pos2, "badge 1 should come before badge 2: {html}");
    }

    #[test]
    fn display_shows_correct_data_indices() {
        let html = ProposeReconciliationTool
            .format_display(&valid_input())
            .unwrap();
        assert!(html.contains("data-index=\"0\""), "missing index 0: {html}");
        assert!(html.contains("data-index=\"1\""), "missing index 1: {html}");
    }

    #[test]
    fn display_shows_transaction_cards() {
        let html = ProposeReconciliationTool
            .format_display(&valid_input())
            .unwrap();
        assert!(
            html.contains("Expenses:Food:Groceries"),
            "missing account from proposal 1: {html}"
        );
        assert!(
            html.contains("Expenses:Dining"),
            "missing account from proposal 2: {html}"
        );
    }

    #[test]
    fn display_shows_import_ref() {
        let html = ProposeReconciliationTool
            .format_display(&valid_input())
            .unwrap();
        assert!(
            html.contains("pfin-reconcile-ref"),
            "missing import ref: {html}"
        );
        assert!(
            html.contains("imp-abc-123-"),
            "missing truncated import id: {html}"
        );
    }

    #[test]
    fn display_embeds_config_json() {
        let html = ProposeReconciliationTool
            .format_display(&valid_input())
            .unwrap();
        assert!(
            html.contains("application/json"),
            "missing script tag: {html}"
        );
        assert!(
            html.contains("imp-abc-123-def-456"),
            "config should have full import_id: {html}"
        );
    }

    #[test]
    fn display_five_proposals_ok() {
        let mut input = valid_input();
        let proposal = input["proposals"][0].clone();
        let proposals = input["proposals"].as_array_mut().unwrap();
        while proposals.len() < 5 {
            proposals.push(proposal.clone());
        }
        assert!(ProposeReconciliationTool.format_display(&input).is_some());
    }

    // -- format_display: import header --

    /// Import header uses deliberately different values from the proposal
    /// transactions in `valid_input()` so `.contains()` tests can distinguish
    /// header rendering from card rendering.
    fn input_with_import() -> Value {
        let mut input = valid_input();
        input.as_object_mut().unwrap().insert(
            "_pending_import".to_string(),
            // Matches pf show JSON format: RFC3339 date, decimal amount string.
            json!({
                "date": "2025-04-01T00:00:00+00:00",
                "amount": "-99.77",
                "payee": "KROGER #1234",
                "memo": "POS PURCHASE",
                "account": "Assets:Savings"
            }),
        );
        input
    }

    #[test]
    fn display_shows_import_header_when_present() {
        let html = ProposeReconciliationTool
            .format_display(&input_with_import())
            .unwrap();
        assert!(
            html.contains("pfin-import-header"),
            "missing import header: {html}"
        );
        assert!(html.contains("KROGER #1234"), "missing payee: {html}");
        assert!(html.contains("-$99.77"), "missing formatted amount: {html}");
        assert!(
            html.contains("pfin-import-amount pfin-debit"),
            "missing debit class on import amount: {html}"
        );
    }

    #[test]
    fn display_import_header_shows_detail_fields() {
        let html = ProposeReconciliationTool
            .format_display(&input_with_import())
            .unwrap();
        assert!(
            html.contains("pfin-import-date"),
            "missing date field: {html}"
        );
        assert!(html.contains("2025-04-01"), "missing date value: {html}");
        assert!(html.contains("Assets:Savings"), "missing account: {html}");
        assert!(html.contains("POS PURCHASE"), "missing memo: {html}");
    }

    #[test]
    fn display_no_import_header_when_absent() {
        let html = ProposeReconciliationTool
            .format_display(&valid_input())
            .unwrap();
        assert!(
            !html.contains("pfin-import-header"),
            "should not have import header: {html}"
        );
    }

    #[test]
    fn display_import_header_minimal_fields() {
        let mut input = valid_input();
        input.as_object_mut().unwrap().insert(
            "_pending_import".to_string(),
            json!({ "amount": "100.00", "payee": "ACME" }),
        );
        let html = ProposeReconciliationTool.format_display(&input).unwrap();
        assert!(html.contains("ACME"), "missing payee: {html}");
        assert!(html.contains("$100.00"), "missing amount: {html}");
        assert!(html.contains("pfin-credit"), "missing credit class: {html}");
        // No detail line when no optional fields.
        assert!(
            !html.contains("pfin-import-detail"),
            "should not have detail line: {html}"
        );
    }

    #[test]
    fn display_import_header_escapes_html() {
        let mut input = valid_input();
        input.as_object_mut().unwrap().insert(
            "_pending_import".to_string(),
            json!({
                "amount": "1.00",
                "payee": "<script>alert(1)</script>",
                "memo": "A&B"
            }),
        );
        let html = ProposeReconciliationTool.format_display(&input).unwrap();
        assert!(!html.contains("<script>alert"), "payee not escaped: {html}");
        assert!(
            html.contains("&lt;script&gt;"),
            "should have escaped payee: {html}"
        );
        assert!(html.contains("A&amp;B"), "should have escaped memo: {html}");
    }

    #[test]
    fn display_import_header_skipped_when_malformed() {
        // pending_import present but missing required fields — should render
        // proposals without a header rather than showing garbage defaults.
        let mut input = valid_input();
        input.as_object_mut().unwrap().insert(
            "_pending_import".to_string(),
            json!({ "memo": "just a memo" }),
        );
        let html = ProposeReconciliationTool.format_display(&input).unwrap();
        assert!(
            !html.contains("pfin-import-header"),
            "malformed pending_import should not produce header: {html}"
        );
        // Proposals should still render fine.
        assert!(
            html.contains("Groceries"),
            "proposals should still render: {html}"
        );
    }

    // -- format_display: compact split filtering --

    /// Helper: input where the import account (Assets:Checking / 52.30) matches
    /// a split in each proposal, so it should be suppressed.
    fn input_with_matching_import() -> Value {
        json!({
            "import_id": "imp-match-test",
            "_pending_import": {
                "date": "2025-04-01T00:00:00+00:00",
                "amount": "-52.30",
                "payee": "GROCERY STORE",
                "account": "Assets:Checking"
            },
            "proposals": [
                {
                    "label": "Groceries",
                    "transaction": {
                        "date": "2025-03-28",
                        "description": "Grocery Store",
                        "splits": [
                            { "account": "Expenses:Food:Groceries", "amount": "-52.30" },
                            { "account": "Assets:Checking", "amount": "52.30" }
                        ]
                    }
                },
                {
                    "label": "Restaurant",
                    "transaction": {
                        "date": "2025-03-28",
                        "description": "Grocery Store",
                        "splits": [
                            { "account": "Expenses:Dining", "amount": "-52.30" },
                            { "account": "Assets:Checking", "amount": "52.30" }
                        ]
                    }
                }
            ]
        })
    }

    #[test]
    fn display_suppresses_import_account_split() {
        let html = ProposeReconciliationTool
            .format_display(&input_with_matching_import())
            .unwrap();
        // The counterparty accounts should be present.
        assert!(
            html.contains("Expenses:Food:Groceries"),
            "missing counterparty account: {html}"
        );
        assert!(
            html.contains("Expenses:Dining"),
            "missing counterparty account: {html}"
        );
        // The import account should NOT appear in the proposal splits
        // (it's only in the header).
        let header_end = html.find("pfin-import-header").unwrap();
        let after_header = &html[header_end..];
        // After the header, "Assets:Checking" should not appear in proposal splits.
        // It may appear in the header itself, so we check only the proposal area.
        let proposals_start = after_header.find("pfin-proposal").unwrap();
        let proposals_html = &after_header[proposals_start..];
        assert!(
            !proposals_html.contains("Assets:Checking"),
            "import account should be suppressed from proposals: {proposals_html}"
        );
    }

    #[test]
    fn display_shows_no_amounts_for_simple_two_way_split() {
        let html = ProposeReconciliationTool
            .format_display(&input_with_matching_import())
            .unwrap();
        // After filtering, each proposal has one counterparty split.
        // Single-split proposals should NOT show amounts.
        assert!(
            !html.contains("pfin-split-amount"),
            "should not show amounts for simple 2-way splits: {html}"
        );
    }

    #[test]
    fn display_shows_amounts_for_multi_way_split() {
        let input = json!({
            "import_id": "imp-multi",
            "_pending_import": {
                "amount": "-100.00",
                "payee": "STORE",
                "account": "Assets:Checking"
            },
            "proposals": [{
                "label": "Split purchase",
                "transaction": {
                    "date": "2025-01-01",
                    "description": "Store",
                    "splits": [
                        { "account": "Expenses:Food", "amount": "-60.00" },
                        { "account": "Expenses:Household", "amount": "-40.00" },
                        { "account": "Assets:Checking", "amount": "100.00" }
                    ]
                }
            }]
        });
        let html = ProposeReconciliationTool.format_display(&input).unwrap();
        // Two counterparty splits after filtering — should show amounts.
        assert!(
            html.contains("pfin-split-amount"),
            "should show amounts for multi-way splits: {html}"
        );
        assert!(html.contains("-$60.00"), "missing amount: {html}");
        assert!(html.contains("-$40.00"), "missing amount: {html}");
    }

    #[test]
    fn display_no_other_splits_when_all_match() {
        // A $0 transaction where the only split matches the import.
        let input = json!({
            "import_id": "imp-zero",
            "_pending_import": {
                "amount": "0.00",
                "payee": "ADJUSTMENT",
                "account": "Assets:Checking"
            },
            "proposals": [{
                "label": "Zero adjustment",
                "transaction": {
                    "date": "2025-01-01",
                    "description": "Adjustment",
                    "splits": [
                        { "account": "Assets:Checking", "amount": "0.00" }
                    ]
                }
            }]
        });
        let html = ProposeReconciliationTool.format_display(&input).unwrap();
        assert!(
            html.contains("No other splits"),
            "should show 'No other splits' placeholder: {html}"
        );
    }

    #[test]
    fn display_removes_only_first_matching_split() {
        // Two splits with same account and amount — only the first should be removed.
        let input = json!({
            "import_id": "imp-dup",
            "_pending_import": {
                "amount": "-50.00",
                "payee": "TRANSFER",
                "account": "Assets:Checking"
            },
            "proposals": [{
                "label": "Double entry",
                "transaction": {
                    "date": "2025-01-01",
                    "description": "Transfer",
                    "splits": [
                        { "account": "Assets:Checking", "amount": "50.00" },
                        { "account": "Assets:Checking", "amount": "50.00" },
                        { "account": "Expenses:Fees", "amount": "-100.00" }
                    ]
                }
            }]
        });
        let html = ProposeReconciliationTool.format_display(&input).unwrap();
        // Should still show one Assets:Checking (the second one wasn't removed)
        // plus Expenses:Fees = 2 counterparty splits, so amounts shown.
        assert!(
            html.contains("Assets:Checking"),
            "second matching split should remain: {html}"
        );
        assert!(
            html.contains("Expenses:Fees"),
            "non-matching split should remain: {html}"
        );
    }

    // -- format_display: escaping --

    #[test]
    fn display_escapes_label() {
        let input = json!({
            "import_id": "imp",
            "proposals": [{
                "label": "<script>alert(1)</script>",
                "transaction": {
                    "date": "2025-01-01",
                    "description": "X",
                    "splits": [{ "account": "A", "amount": "1" }]
                }
            }]
        });
        let html = ProposeReconciliationTool.format_display(&input).unwrap();
        assert!(!html.contains("<script>alert"), "label not escaped: {html}");
        assert!(
            html.contains("&lt;script&gt;"),
            "should have escaped form: {html}"
        );
    }

    // -- format_display: fallback to None on bad input --

    #[test]
    fn display_none_on_missing_import_id() {
        let input = json!({
            "proposals": [{
                "label": "X",
                "transaction": {
                    "date": "2025-01-01",
                    "description": "X",
                    "splits": [{ "account": "A", "amount": "1" }]
                }
            }]
        });
        assert!(ProposeReconciliationTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_missing_proposals() {
        let input = json!({ "import_id": "imp" });
        assert!(ProposeReconciliationTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_empty_proposals() {
        let input = json!({ "import_id": "imp", "proposals": [] });
        assert!(ProposeReconciliationTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_too_many_proposals() {
        let proposal = json!({
            "label": "X",
            "transaction": {
                "date": "2025-01-01",
                "description": "X",
                "splits": [{ "account": "A", "amount": "1" }]
            }
        });
        let input = json!({
            "import_id": "imp",
            "proposals": vec![proposal; 6]
        });
        assert!(ProposeReconciliationTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_proposal_missing_transaction() {
        let input = json!({
            "import_id": "imp",
            "proposals": [{ "label": "X" }]
        });
        assert!(ProposeReconciliationTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_proposal_empty_splits() {
        let input = json!({
            "import_id": "imp",
            "proposals": [{
                "label": "X",
                "transaction": {
                    "date": "2025-01-01",
                    "description": "X",
                    "splits": []
                }
            }]
        });
        assert!(ProposeReconciliationTool.format_display(&input).is_none());
    }

    #[test]
    fn display_none_on_empty_input() {
        assert!(
            ProposeReconciliationTool
                .format_display(&json!({}))
                .is_none()
        );
    }

    // -- format_summary --

    #[test]
    fn summary_allowed_with_selected_shows_label() {
        let decision = ToolResponseDecision::Allow {
            updated_input: Some(json!({ "selected": 0 })),
        };
        let html = ProposeReconciliationTool
            .format_summary(&valid_input(), &decision)
            .unwrap();
        assert!(html.contains("2 options"), "missing count: {html}");
        assert!(html.contains("Groceries"), "missing selected label: {html}");
        assert!(html.contains("\u{2192}"), "missing arrow: {html}");
    }

    #[test]
    fn summary_allowed_with_second_selected() {
        let decision = ToolResponseDecision::Allow {
            updated_input: Some(json!({ "selected": 1 })),
        };
        let html = ProposeReconciliationTool
            .format_summary(&valid_input(), &decision)
            .unwrap();
        assert!(
            html.contains("Restaurant"),
            "should show second label: {html}"
        );
    }

    #[test]
    fn summary_allowed_without_selected_shows_generic() {
        let decision = ToolResponseDecision::Allow {
            updated_input: None,
        };
        let html = ProposeReconciliationTool
            .format_summary(&valid_input(), &decision)
            .unwrap();
        assert!(html.contains("2 options"), "missing count: {html}");
        assert!(html.contains("approved"), "missing approved: {html}");
    }

    #[test]
    fn summary_denied() {
        let decision = ToolResponseDecision::Deny {
            reason: Some("bad options".into()),
        };
        let html = ProposeReconciliationTool
            .format_summary(&valid_input(), &decision)
            .unwrap();
        assert!(html.contains("ts-denied"), "missing denied class: {html}");
        assert!(html.contains("denied"), "missing denied text: {html}");
    }

    #[test]
    fn summary_none_on_missing_proposals() {
        let decision = ToolResponseDecision::Allow {
            updated_input: None,
        };
        assert!(
            ProposeReconciliationTool
                .format_summary(&json!({}), &decision)
                .is_none()
        );
    }

    #[test]
    fn summary_escapes_label() {
        let input = json!({
            "import_id": "imp",
            "proposals": [{
                "label": "<b>X</b>",
                "transaction": {
                    "date": "2025-01-01",
                    "description": "X",
                    "splits": [{ "account": "A", "amount": "1" }]
                }
            }]
        });
        let decision = ToolResponseDecision::Allow {
            updated_input: Some(json!({ "selected": 0 })),
        };
        let html = ProposeReconciliationTool
            .format_summary(&input, &decision)
            .unwrap();
        assert!(!html.contains("<b>X</b>"), "label not escaped: {html}");
        assert!(
            html.contains("&lt;b&gt;"),
            "should have escaped form: {html}"
        );
    }

    // -- execute_selection: input validation --

    #[tokio::test]
    async fn execute_selection_missing_import_id() {
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "pf",
            env: &env,
            working_dir: Path::new("/tmp"),
            container_spawn: None,
        };
        let input = json!({ "proposals": [{ "label": "X", "transaction": {} }] });
        let result = execute_selection(&input, 0, &ctx, "testuser").await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("missing import_id"),
            "should mention import_id"
        );
    }

    #[tokio::test]
    async fn execute_selection_missing_proposals() {
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "pf",
            env: &env,
            working_dir: Path::new("/tmp"),
            container_spawn: None,
        };
        let input = json!({ "import_id": "imp-123" });
        let result = execute_selection(&input, 0, &ctx, "testuser").await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("missing proposals"),
            "should mention proposals"
        );
    }

    #[tokio::test]
    async fn execute_selection_index_out_of_range() {
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "pf",
            env: &env,
            working_dir: Path::new("/tmp"),
            container_spawn: None,
        };
        let input = json!({
            "import_id": "imp-123",
            "proposals": [{ "label": "X", "transaction": {} }]
        });
        let result = execute_selection(&input, 5, &ctx, "testuser").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("out of range"),
            "should mention out of range: {err}"
        );
    }

    #[tokio::test]
    async fn execute_selection_missing_transaction() {
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "pf",
            env: &env,
            working_dir: Path::new("/tmp"),
            container_spawn: None,
        };
        let input = json!({
            "import_id": "imp-123",
            "proposals": [{ "label": "X" }]
        });
        let result = execute_selection(&input, 0, &ctx, "testuser").await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("missing transaction"),
            "should mention transaction"
        );
    }

    #[tokio::test]
    async fn execute_selection_bad_command() {
        let env = HashMap::new();
        let ctx = SubprocessExecContext {
            command: "/nonexistent/binary/pf",
            env: &env,
            working_dir: Path::new("/tmp"),
            container_spawn: None,
        };
        let input = json!({
            "import_id": "imp-123",
            "proposals": [{
                "label": "X",
                "transaction": { "splits": [{ "account": "A", "amount": "1" }] }
            }]
        });
        let result = execute_selection(&input, 0, &ctx, "testuser").await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("failed to spawn"),
            "should mention spawn failure"
        );
    }
}

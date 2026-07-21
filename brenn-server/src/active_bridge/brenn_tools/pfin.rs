//! pfin tool family: ProposeReconciliation, BatchReconcile, BatchAssign.
//! Includes pfin-specific batch-item enrichment, persisted-extra decoding,
//! and the render-dispatch override for pfin tool cards.

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_lib::approval_rules::ApprovalMatch;
use brenn_lib::integration::{IntegrationToolAction, ToolPhase};
use brenn_lib::subprocess::SubprocessExecContext;
use brenn_lib::ws_types::ViewportClass;
use tracing::{info, warn};

use super::super::ActiveBridge;
use super::super::mcp_constants::{
    MCP_BATCH_ASSIGN_TOOL, MCP_BATCH_RECONCILE_TOOL, MCP_PROPOSE_RECONCILIATION_TOOL,
};
use super::super::tool_summary::{HandleBrennToolResult, emit_tool_summary, mark_tool_handled};

/// Handle ProposeReconciliation, BatchReconcile, and BatchAssign Pre+Post
/// approval flows.
///
/// Decision logic (Pre `Allow`, BatchAssign `user` validation, tool-name
/// matching) is delegated to `PfinIntegration::intercept_tool`. This function
/// is translation glue: it calls `intercept_tool`, marks the tool handled at
/// the right moment, then dispatches enrichment orchestration for `Proceed`.
///
/// Returns `Some(...)` when the request is for one of these three tool names
/// and `None` otherwise — letting the dispatcher fall through.
pub(super) async fn handle(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<HandleBrennToolResult> {
    let (phase, tool_name, tool_input, tool_use_id) = match &req.kind {
        ApprovalKind::PreToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } => (ToolPhase::Pre, tool_name, tool_input, tool_use_id),
        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } => (ToolPhase::Post, tool_name, tool_input, tool_use_id),
        _ => return None,
    };

    // Look up the pfin integration. If pfin is not enabled for this app,
    // no pfin tools will appear — return None to let the dispatcher fall
    // through. In a correctly-configured deployment this guard is never hit
    // for pfin tool names, but it keeps the dispatcher safe in tests and
    // for non-pfin tools routed here before any name check.
    let integration = bridge
        .integrations
        .get(brenn_pfin::INTEGRATION_NAME)
        .cloned()?;

    // Delegate decision logic to PfinIntegration::intercept_tool.
    // Returns None if tool_name is not a pfin tool — fall through.
    //
    // For PostToolUse, `mark_tool_handled` runs *before* `intercept_tool`
    // (§5.3 invariant). The tool is already ours (guard above passed), so we
    // mark it handled unconditionally before dispatching. This ensures the
    // Reject early-return path also leaves the tool correctly marked, and
    // means the ordering invariant holds regardless of any future side effects
    // `intercept_tool` might acquire. PreToolUse uses Respond(Allow) which
    // does not go through mark_tool_handled.
    if phase == ToolPhase::Post {
        mark_tool_handled(bridge, tool_use_id).await;
    }

    let action = integration
        .intercept_tool(phase, tool_name, tool_input)
        .await?;

    match action {
        // --- PreToolUse: grant permission immediately ---
        IntegrationToolAction::GrantPermission => {
            debug_assert!(
                phase == ToolPhase::Pre,
                "GrantPermission is a PreToolUse-only action; returning it for Post \
                 phase is a logic error in intercept_tool"
            );
            info!("intercepting {tool_name} PreToolUse — granting permission");
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow {
                updated_input: None,
            }))
        }

        // --- PostToolUse validation failed ---
        IntegrationToolAction::Reject { message } => {
            warn!("{tool_name}: intercept_tool rejected: {message}");
            Some(HandleBrennToolResult::Respond(
                CcApprovalDecision::Continue {
                    updated_output: Some(
                        serde_json::json!({"status": "error", "error": message}).to_string(),
                    ),
                },
            ))
        }

        // --- PostToolUse: proceed with brenn-side enrichment orchestration ---
        IntegrationToolAction::Proceed => {
            info!("intercepting {tool_name} PostToolUse — async persist");
            if tool_name == MCP_PROPOSE_RECONCILIATION_TOOL {
                // Fetch pending import details from pfin to display as context.
                let enriched_input = bridge.enrich_with_import_details(tool_input.clone()).await;

                // Emit summary for chat history.
                emit_tool_summary(
                    bridge,
                    tool_name,
                    tool_input,
                    None,
                    Some(&ApprovalMatch::GlobalTool),
                    false,
                )
                .await;

                // Persist to DB and broadcast.
                Some(HandleBrennToolResult::PersistAndBroadcast {
                    tool_name: tool_name.clone(),
                    tool_input: enriched_input,
                    extra: None,
                })
            } else if tool_name == MCP_BATCH_RECONCILE_TOOL || tool_name == MCP_BATCH_ASSIGN_TOOL {
                // BatchReconcile or BatchAssign: shared enrichment scaffold.
                // No extra_validation needed — intercept_tool already validated.
                handle_batch_post_tool_use(bridge, tool_name, tool_input).await
            } else {
                // intercept_tool returned Proceed for a tool name not handled here.
                // This is an invariant violation: a new pfin tool was added to
                // intercept_tool without a corresponding dispatch arm here.
                unreachable!(
                    "intercept_tool returned Proceed for unrecognised pfin tool: {tool_name}"
                );
            }
        }
    }
}

/// Shared PostToolUse scaffold for BatchReconcile and BatchAssign.
///
/// Validates the `items` array, enforces the server-side cap, enriches items
/// in parallel, emits a tool summary, and returns a `PersistAndBroadcast`
/// result. Callers are expected to have run all input validation via
/// `intercept_tool` before dispatching here.
///
/// # Pre-condition
/// The caller must call `mark_tool_handled(bridge, tool_use_id).await` before
/// invoking this function. The `Respond` early-return paths here require the
/// tool to already be marked handled.
async fn handle_batch_post_tool_use(
    bridge: &ActiveBridge,
    tool_name: &str,
    tool_input: &serde_json::Value,
) -> Option<HandleBrennToolResult> {
    let items = match tool_input.get("items").and_then(|v| v.as_array()) {
        Some(items) => items,
        None => {
            warn!("{tool_name}: missing items array");
            return Some(HandleBrennToolResult::Respond(
                CcApprovalDecision::Continue {
                    updated_output: Some(
                        serde_json::json!({
                            "status": "error",
                            "error": "missing items array in tool input",
                        })
                        .to_string(),
                    ),
                },
            ));
        }
    };

    // Enforce server-side item cap.
    if items.len() > brenn_pfin::batch::MAX_BATCH_ITEMS {
        warn!(
            count = items.len(),
            max = brenn_pfin::batch::MAX_BATCH_ITEMS,
            "{tool_name}: too many items"
        );
        return Some(HandleBrennToolResult::Respond(
            CcApprovalDecision::Continue {
                updated_output: Some(
                    serde_json::json!({
                        "status": "error",
                        "error": format!(
                            "too many items: {} (max {})",
                            items.len(),
                            brenn_pfin::batch::MAX_BATCH_ITEMS,
                        ),
                    })
                    .to_string(),
                ),
            },
        ));
    }

    // Parallel enrichment with bounded concurrency.
    let (enriched, enrichment_failures) = enrich_batch_items(bridge, items).await;

    // If all items failed enrichment, return error immediately.
    if enriched.is_empty() {
        warn!("{tool_name}: all items failed enrichment");
        let mut result = serde_json::json!({
            "status": "error",
            "error": "all items failed enrichment",
            "total": items.len(),
            "enrichment_failed": enrichment_failures.len(),
        });
        let failures: Vec<_> = enrichment_failures
            .iter()
            .map(|(idx, id, err)| {
                serde_json::json!({
                    "index": idx,
                    "import_id": id,
                    "status": "enrichment_failed",
                    "error": err,
                })
            })
            .collect();
        result["results"] = serde_json::Value::Array(failures);
        return Some(HandleBrennToolResult::Respond(
            CcApprovalDecision::Continue {
                updated_output: Some(
                    serde_json::to_string(&result).expect("JSON serialization cannot fail"),
                ),
            },
        ));
    }

    // Emit summary for chat history.
    emit_tool_summary(
        bridge,
        tool_name,
        tool_input,
        None,
        Some(&ApprovalMatch::GlobalTool),
        false,
    )
    .await;

    // Serialize enriched items and enrichment failures for DB storage.
    // The enriched items are needed to render on replay without re-enriching.
    let enriched_json: Vec<_> = enriched
        .iter()
        .map(|e| {
            serde_json::json!({
                "original_index": e.original_index,
                "item": e.item,
                "pending_import": e.pending_import,
            })
        })
        .collect();
    let mut extra_obj = serde_json::json!({
        "enriched_items": enriched_json,
    });
    if !enrichment_failures.is_empty() {
        let failures: Vec<_> = enrichment_failures
            .iter()
            .map(|(idx, id, err)| {
                serde_json::json!({
                    "index": idx,
                    "import_id": id,
                    "error": err,
                })
            })
            .collect();
        extra_obj["enrichment_failures"] = serde_json::Value::Array(failures);
    }

    Some(HandleBrennToolResult::PersistAndBroadcast {
        tool_name: tool_name.to_string(),
        tool_input: tool_input.clone(),
        extra: Some(extra_obj.to_string()),
    })
}

/// Render a pending tool request for display in the browser.
///
/// This is the single render path for both live requests and replays. It dispatches
/// on `tool_name` to tool-specific renderers (catalog pattern). Adding a new
/// interactive tool means adding a new match arm here.
pub(crate) fn render_pending_tool_request(
    tool_registry: &std::collections::HashMap<String, std::sync::Arc<dyn brenn_lib::app::AppTool>>,
    tool_name: &str,
    tool_input: &serde_json::Value,
    extra: Option<&str>,
    viewport: ViewportClass,
) -> String {
    match tool_name {
        name if name == MCP_BATCH_RECONCILE_TOOL => {
            let enriched = decode_enriched_items(extra);

            let renderer = match viewport {
                ViewportClass::Wide => brenn_pfin::batch::render_batch_table,
                ViewportClass::Compact => brenn_pfin::batch::render_batch_swipe,
            };
            renderer(&enriched).unwrap_or_else(|| {
                crate::approval_formatter::format_tool_display(tool_registry, tool_name, tool_input)
            })
        }
        name if name == MCP_BATCH_ASSIGN_TOOL => {
            let enriched = decode_enriched_items(extra);
            let user = tool_input
                .get("user")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let result = match viewport {
                ViewportClass::Wide => {
                    brenn_pfin::batch_assign::render_assign_table(&enriched, user)
                }
                ViewportClass::Compact => {
                    brenn_pfin::batch_assign::render_assign_swipe(&enriched, user)
                }
            };
            result.unwrap_or_else(|| {
                crate::approval_formatter::format_tool_display(tool_registry, tool_name, tool_input)
            })
        }
        _ => {
            // ProposeReconciliation and any other tools: render via the tool registry.
            crate::approval_formatter::format_tool_display(tool_registry, tool_name, tool_input)
        }
    }
}

/// Reconstruct `EnrichedBatchItem`s from the persisted `extra` JSON.
///
/// Used by both `BatchReconcile` and `BatchAssign` render dispatches — the
/// shape is identical: `{ "enriched_items": [{ original_index, item,
/// pending_import }, ...] }`. Returns an empty vec when `extra` is `None` so
/// the caller falls back to the generic display.
///
/// This data was written by us — parse failures are broken invariants and panic.
fn decode_enriched_items(extra: Option<&str>) -> Vec<brenn_pfin::batch::EnrichedBatchItem> {
    extra
        .map(|e| {
            let parsed: serde_json::Value =
                serde_json::from_str(e).expect("stored extra must be valid JSON");
            let items = parsed
                .get("enriched_items")
                .and_then(|v| v.as_array())
                .expect("stored extra must contain enriched_items array");
            items
                .iter()
                .map(|obj| brenn_pfin::batch::EnrichedBatchItem {
                    original_index: obj
                        .get("original_index")
                        .and_then(|v| v.as_u64())
                        .expect("enriched item must have original_index")
                        as usize,
                    item: obj
                        .get("item")
                        .expect("enriched item must have item")
                        .clone(),
                    pending_import: obj
                        .get("pending_import")
                        .expect("enriched item must have pending_import")
                        .clone(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// Enrich batch items with import details in parallel, bounded by concurrency.
///
/// Returns (enriched_items, enrichment_failures). Failures are excluded from
/// the enriched list but tracked for inclusion in the tool result.
async fn enrich_batch_items(
    bridge: &ActiveBridge,
    items: &[serde_json::Value],
) -> (
    Vec<brenn_pfin::batch::EnrichedBatchItem>,
    Vec<(usize, String, String)>,
) {
    use futures::stream::StreamExt;

    let pfin_config = bridge
        .pfin_config()
        .expect("pfin enabled ⇒ config present; missing config is a startup bug");

    let mut enriched = Vec::with_capacity(items.len());
    let mut failures = Vec::new();

    // Clone config values upfront — closures below must be 'static (buffer_unordered),
    // so they cannot borrow from pfin_config or bridge. SubprocessExecContext borrows;
    // we clone the underlying values for the per-future closures.
    let pfin_command = pfin_config.command.clone();
    let pfin_env = pfin_config.env.clone();
    let container_spawn = bridge.container_spawn.clone();
    let working_dir = bridge.working_dir.clone();

    // Build owned futures, then use buffer_unordered(8) to limit concurrent
    // subprocess spawns. Collected into a Vec so the iterator is 'static.
    let futs: Vec<_> = items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let import_id = item
                .get("import_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let command = pfin_command.clone();
            let env = pfin_env.clone();
            let cs = container_spawn.clone();
            let wd = working_dir.clone();
            let item_clone = item.clone();

            async move {
                let ctx = SubprocessExecContext {
                    command: &command,
                    env: &env,
                    working_dir: &wd,
                    container_spawn: cs.as_ref(),
                };
                let result = brenn_pfin::fetch_import_details(&import_id, &ctx).await;
                (i, import_id, item_clone, result)
            }
        })
        .collect();

    let mut stream = futures::stream::iter(futs).buffer_unordered(8);

    while let Some((idx, import_id, item_val, result)) = stream.next().await {
        match result {
            Ok(details) => {
                enriched.push(brenn_pfin::batch::EnrichedBatchItem {
                    original_index: idx,
                    item: item_val,
                    pending_import: details,
                });
            }
            Err(e) => {
                warn!(index = idx, import_id = %import_id, error = %e, "batch enrichment failed");
                failures.push((idx, import_id, e));
            }
        }
    }

    // Sort enriched items by original index to preserve order.
    enriched.sort_by_key(|e| e.original_index);

    (enriched, failures)
}

#[cfg(test)]
mod tests {
    use brenn_cc::session::{
        ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest, SessionEvent,
    };
    use brenn_lib::ws_types::{CcState, ToolResponseDecision, ViewportClass, WsServerMessage};
    use tokio::sync::oneshot;

    use super::super::super::mcp_constants::{
        MCP_BATCH_ASSIGN_TOOL, MCP_BATCH_RECONCILE_TOOL, MCP_PROPOSE_RECONCILIATION_TOOL,
    };
    use super::super::super::test_fixtures::{TestBridgeConfig, pfin_test_integrations};
    use super::super::super::test_support::{recv_broadcast, test_bridge, test_bridge_with_config};
    use super::super::super::tool_summary::HandleBrennToolResult;
    use super::super::handle_brenn_tools;
    use super::render_pending_tool_request;

    // -----------------------------------------------------------------------
    // Dispatcher guard — pfin integration absent
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn pfin_absent_returns_none_for_pfin_tool_name() {
        // If the pfin integration is not in the bridge's integrations map,
        // handle() must return None (fall-through) even for a pfin tool name.
        // Without this guard, the subsequent pfin_config().expect(...) would
        // panic instead of falling through.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: std::collections::HashMap::new(), // no pfin
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_pfin_absent".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_PROPOSE_RECONCILIATION_TOOL.into(),
                tool_input: serde_json::json!({"import_id": "imp-1"}),
                tool_use_id: "t_pfin_absent".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        assert!(
            result.is_none(),
            "pfin tool on pfin-less bridge should fall through (None), got {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // ProposeReconciliation interception
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn propose_pre_tool_use_allows() {
        // PreToolUse for ProposeReconciliation should return Allow to grant
        // hook-level permission so CC skips the Permission prompt.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_propose_pre".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_PROPOSE_RECONCILIATION_TOOL.into(),
                tool_input: serde_json::json!({
                    "import_id": "imp-123",
                    "proposals": [{"label": "X", "transaction": {}}]
                }),
                tool_use_id: "t_propose_pre".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow { .. })) => {}
            other => panic!("PreToolUse for ProposeReconciliation should Allow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn propose_post_tool_use_persists_and_broadcasts() {
        // PostToolUse for ProposeReconciliation should persist to DB
        // and broadcast an ApprovalRequest to the browser.
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        // Pre-inject _pending_import so the PostToolUse handler skips the
        // pf show fetch (test bridge has no pfin binary).
        let tool_input = serde_json::json!({
            "import_id": "imp-456",
            "_pending_import": {
                "payee": "TEST STORE",
                "amount": "-50.00",
                "date": "2025-03-28T00:00:00+00:00",
                "account": "Assets:Checking"
            },
            "proposals": [
                {
                    "label": "Groceries",
                    "transaction": {
                        "date": "2025-03-28",
                        "description": "Store",
                        "splits": [
                            { "account": "Expenses:Food", "amount": "-50.00" },
                            { "account": "Assets:Checking", "amount": "50.00" }
                        ]
                    }
                }
            ]
        });

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_propose_post".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_PROPOSE_RECONCILIATION_TOOL.into(),
                tool_input: tool_input.clone(),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_propose_post".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Should broadcast ToolUseSummary (from emit_tool_summary).
        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(&msg, WsServerMessage::ToolUseSummary { .. }),
            "expected ToolUseSummary, got {msg:?}"
        );

        // Should broadcast ToolCardRequest with proposal UI.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::ToolCardRequest {
                request_id,
                tool_name,
                tool_input: broadcast_tool_input,
                formatted_display,
            } => {
                assert_eq!(request_id, "req_propose_post");
                assert_eq!(tool_name, MCP_PROPOSE_RECONCILIATION_TOOL);
                // tool_input should be the real value, not Null.
                assert_eq!(
                    broadcast_tool_input["import_id"], "imp-456",
                    "broadcast should include real tool_input"
                );
                // formatted_display should contain the proposal HTML.
                // Since the test bridge has no tool_registry entries, it
                // falls back to the generic JSON display.
                assert!(!formatted_display.is_empty());
            }
            other => panic!("expected ApprovalRequest, got {other:?}"),
        }

        // Should broadcast AwaitingApproval status.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(
                &msg,
                WsServerMessage::Status {
                    state: CcState::AwaitingApproval
                }
            ),
            "expected AwaitingApproval, got {msg:?}"
        );

        // CC should get an immediate Continue with compact request_id JSON.
        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Continue { updated_output } => {
                let output = updated_output.expect("should have updated_output");
                let parsed: serde_json::Value =
                    serde_json::from_str(&output).expect("should be valid JSON");
                assert_eq!(
                    parsed["request_id"], "req_propose_post",
                    "should include request_id: {output}"
                );
            }
            other => panic!("expected Continue with request_id, got {other:?}"),
        }

        // Should be persisted in DB, not in-memory pending_permissions.
        {
            let permissions = bridge.pending_permissions.lock().await;
            assert!(
                !permissions.contains_key("req_propose_post"),
                "should NOT be in pending_permissions (async tools go to DB)"
            );
        }
        {
            let conn = bridge.db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&conn, "req_propose_post");
            assert!(req.is_some(), "should be in pending_tool_requests DB table");
            let req = req.unwrap();
            assert_eq!(req.status, "pending");
            assert_eq!(req.tool_name, MCP_PROPOSE_RECONCILIATION_TOOL);
        }

        // Now simulate user denying the proposal.
        bridge
            .handle_tool_card_response(
                "req_propose_post",
                ToolResponseDecision::Deny {
                    reason: Some("wrong category".into()),
                },
            )
            .await;

        // Should broadcast ToolCardResolved.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(&msg, WsServerMessage::ToolCardResolved { .. }),
            "expected ToolCardResolved, got {msg:?}"
        );

        // DB should show denied.
        {
            let conn = bridge.db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&conn, "req_propose_post")
                .expect("request should exist");
            assert_eq!(req.status, "denied");
            let result: serde_json::Value =
                serde_json::from_str(req.result.as_deref().unwrap()).unwrap();
            assert_eq!(result["status"], "denied");
            assert!(
                result["reason"]
                    .as_str()
                    .unwrap()
                    .contains("wrong category"),
                "should include reason"
            );
        }
    }

    // NOTE: propose_allow_without_pfin_config is no longer tested — missing
    // pfin config is now an invariant violation (panic/unreachable).

    #[tokio::test]
    async fn propose_allow_without_selected_index_returns_error() {
        // Allow without { selected: N } should return a graceful error.
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_propose_no_idx".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_PROPOSE_RECONCILIATION_TOOL.into(),
                tool_input: serde_json::json!({
                    "import_id": "imp-abc",
                    "_pending_import": { "payee": "X", "amount": "1" },
                    "proposals": [{"label": "X", "transaction": {}}]
                }),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_propose_no_idx".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Drain broadcasts (ToolUseSummary, ApprovalRequest, AwaitingApproval).
        let _ = recv_broadcast(&mut broadcast_rx).await;
        let _ = recv_broadcast(&mut broadcast_rx).await;
        let _ = recv_broadcast(&mut broadcast_rx).await;

        // Allow without selected field.
        bridge
            .handle_tool_card_response(
                "req_propose_no_idx",
                ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // Drain the ToolCardResolved.
        let _ = recv_broadcast(&mut broadcast_rx).await;

        // DB result should indicate error about missing selection.
        let req = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::get_pending_tool_request(&conn, "req_propose_no_idx")
        };
        let req = req.expect("request should exist");
        let result: serde_json::Value =
            serde_json::from_str(req.result.as_deref().unwrap()).unwrap();
        assert!(
            result["error"]
                .as_str()
                .unwrap()
                .contains("no proposal was selected"),
            "should mention missing selection: {result}"
        );
    }

    // ---- BatchReconcile tests ----

    #[tokio::test]
    async fn batch_pre_tool_use_allows() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_batch_pre".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_BATCH_RECONCILE_TOOL.into(),
                tool_input: serde_json::json!({
                    "items": [{"import_id": "imp-1", "transaction": {}}]
                }),
                tool_use_id: "t_batch_pre".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow { .. })) => {}
            other => panic!("PreToolUse for BatchReconcile should Allow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_post_tool_use_rejects_too_many_items() {
        // More than MAX_BATCH_ITEMS should return an immediate error.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let items: Vec<serde_json::Value> = (0..51)
            .map(|i| {
                serde_json::json!({
                    "import_id": format!("imp-{i}"),
                    "transaction": { "splits": [] }
                })
            })
            .collect();

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_batch_too_many".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_BATCH_RECONCILE_TOOL.into(),
                tool_input: serde_json::json!({ "items": items }),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_batch_post".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                assert!(output.contains("too many items"), "got: {output}");
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_post_tool_use_rejects_missing_items() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_batch_no_items".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_BATCH_RECONCILE_TOOL.into(),
                tool_input: serde_json::json!({}),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_batch_post".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                assert!(
                    output.contains("missing items"),
                    "should mention missing items: {output}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_post_tool_use_all_enrichment_fails_returns_error() {
        // When all items fail enrichment (pfin binary unavailable),
        // should return an immediate error to CC without stashing.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_batch_all_fail".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_BATCH_RECONCILE_TOOL.into(),
                tool_input: serde_json::json!({
                    "items": [
                        { "import_id": "imp-1", "transaction": { "splits": [] } },
                        { "import_id": "imp-2", "transaction": { "splits": [] } },
                    ]
                }),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_batch_post".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                assert!(
                    output.contains("all items failed enrichment"),
                    "should report all enrichment failed: {output}"
                );
                assert!(
                    output.contains("enrichment_failed"),
                    "should include per-item failures: {output}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_deny_via_db_resolves_and_broadcasts() {
        // Simulate a batch request persisted to DB, then denied by the user.
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Insert a pending tool request directly into the DB.
        let tool_input = serde_json::json!({
            "items": [
                { "import_id": "imp-1", "transaction": { "splits": [] } },
            ]
        });
        {
            let conn = bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_batch_deny",
                bridge.conversation_id,
                MCP_BATCH_RECONCILE_TOOL,
                &serde_json::to_string(&tool_input).unwrap(),
                None,
            );
        }

        bridge
            .handle_tool_card_response(
                "req_batch_deny",
                ToolResponseDecision::Deny {
                    reason: Some("not now".into()),
                },
            )
            .await;

        // Should broadcast ToolCardResolved.
        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(msg, WsServerMessage::ToolCardResolved { .. }),
            "expected ToolCardResolved, got {msg:?}"
        );

        // Check DB: status should be 'denied'.
        let req = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::get_pending_tool_request(&conn, "req_batch_deny")
        };
        let req = req.expect("request should exist in DB");
        assert_eq!(req.status, "denied");
        let result: serde_json::Value =
            serde_json::from_str(req.result.as_deref().unwrap()).unwrap();
        assert_eq!(result["status"], "denied");
        assert!(
            result["reason"].as_str().unwrap().contains("not now"),
            "should include reason: {result}"
        );
    }

    #[tokio::test]
    async fn batch_allow_without_decisions_via_db() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let tool_input = serde_json::json!({
            "items": [
                { "import_id": "imp-1", "transaction": { "splits": [] } },
            ]
        });
        {
            let conn = bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_batch_no_decisions",
                bridge.conversation_id,
                MCP_BATCH_RECONCILE_TOOL,
                &serde_json::to_string(&tool_input).unwrap(),
                None,
            );
        }

        bridge
            .handle_tool_card_response(
                "req_batch_no_decisions",
                ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // Drain the ApprovalResolved broadcast.
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // Check DB result.
        let req = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::get_pending_tool_request(&conn, "req_batch_no_decisions")
        };
        let req = req.expect("request should exist in DB");
        assert_eq!(req.status, "completed");
        let result: serde_json::Value =
            serde_json::from_str(req.result.as_deref().unwrap()).unwrap();
        assert!(
            result["error"].as_str().unwrap().contains("no decisions"),
            "should mention missing decisions: {result}"
        );
    }

    #[tokio::test]
    async fn batch_deny_with_enrichment_failures_via_db() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let extra = serde_json::json!({
            "enrichment_failures": [
                { "index": 3, "import_id": "imp-bad", "error": "pf show failed" }
            ]
        });
        {
            let conn = bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_batch_deny_enrich",
                bridge.conversation_id,
                MCP_BATCH_RECONCILE_TOOL,
                &serde_json::json!({ "items": [] }).to_string(),
                Some(&extra.to_string()),
            );
        }

        bridge
            .handle_tool_card_response(
                "req_batch_deny_enrich",
                ToolResponseDecision::Deny { reason: None },
            )
            .await;

        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // Check DB result.
        let req = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::get_pending_tool_request(&conn, "req_batch_deny_enrich")
        };
        let req = req.expect("request should exist in DB");
        assert_eq!(req.status, "denied");
    }

    // --- render_pending_tool_request tests ---

    #[test]
    fn render_dispatch_propose_uses_tool_registry() {
        // ProposeReconciliation renders via the tool registry (format_tool_display).
        // With no registry entries, falls back to generic JSON display.
        let registry = std::collections::HashMap::new();
        let tool_input = serde_json::json!({
            "import_id": "imp-1",
            "proposals": [{
                "label": "Test",
                "transaction": {
                    "description": "Test txn",
                    "splits": [{"account": "A", "amount": "1.00"}]
                }
            }]
        });
        let html = render_pending_tool_request(
            &registry,
            MCP_PROPOSE_RECONCILIATION_TOOL,
            &tool_input,
            None,
            ViewportClass::Wide,
        );
        // Should produce non-empty HTML (fallback renders JSON).
        assert!(!html.is_empty());
    }

    #[test]
    fn render_dispatch_batch_wide_vs_compact() {
        let registry = std::collections::HashMap::new();
        let tool_input = serde_json::json!({"items": []});
        let extra = serde_json::json!({
            "enriched_items": [{
                "original_index": 0,
                "item": {"import_id": "imp-1", "transaction": {
                    "description": "Coffee",
                    "splits": [
                        {"account": "Expenses:Food", "amount": "-5.00"},
                        {"account": "Assets:Checking", "amount": "5.00"}
                    ]
                }},
                "pending_import": {"amount": "-5.00", "payee": "CAFE"}
            }]
        });
        let extra_str = extra.to_string();

        let wide = render_pending_tool_request(
            &registry,
            MCP_BATCH_RECONCILE_TOOL,
            &tool_input,
            Some(&extra_str),
            ViewportClass::Wide,
        );
        let compact = render_pending_tool_request(
            &registry,
            MCP_BATCH_RECONCILE_TOOL,
            &tool_input,
            Some(&extra_str),
            ViewportClass::Compact,
        );

        // Wide renders a table, compact renders swipe cards.
        assert!(
            wide.contains("brenn-pfin-batch-table"),
            "wide should render table: {wide}"
        );
        assert!(
            compact.contains("brenn-pfin-batch-swipe"),
            "compact should render swipe: {compact}"
        );
    }

    #[test]
    fn render_dispatch_batch_no_extra_falls_back() {
        // BatchReconcile with no extra (None) should fall back to generic display.
        let registry = std::collections::HashMap::new();
        let tool_input = serde_json::json!({"items": []});

        let html = render_pending_tool_request(
            &registry,
            MCP_BATCH_RECONCILE_TOOL,
            &tool_input,
            None,
            ViewportClass::Wide,
        );
        // Falls back to generic display (no enriched items → empty batch → fallback).
        assert!(!html.is_empty());
    }

    #[test]
    fn render_dispatch_unknown_tool_falls_back() {
        let registry = std::collections::HashMap::new();
        let tool_input = serde_json::json!({"foo": "bar"});

        let html = render_pending_tool_request(
            &registry,
            "mcp__brenn__SomeUnknownTool",
            &tool_input,
            None,
            ViewportClass::Wide,
        );
        // Should produce fallback HTML (JSON dump in brenn-tool-approve).
        assert!(!html.is_empty());
    }

    #[test]
    fn batch_extra_round_trip() {
        // Verify that enriched items stored in extra can be reconstructed.
        let enriched = [
            brenn_pfin::batch::EnrichedBatchItem {
                original_index: 0,
                item: serde_json::json!({"import_id": "imp-1"}),
                pending_import: serde_json::json!({"amount": "10.00", "payee": "SHOP"}),
            },
            brenn_pfin::batch::EnrichedBatchItem {
                original_index: 2,
                item: serde_json::json!({"import_id": "imp-3"}),
                pending_import: serde_json::json!({"amount": "20.00", "payee": "STORE"}),
            },
        ];

        // Serialize as the PostToolUse handler does.
        let enriched_json: Vec<_> = enriched
            .iter()
            .map(|e| {
                serde_json::json!({
                    "original_index": e.original_index,
                    "item": e.item,
                    "pending_import": e.pending_import,
                })
            })
            .collect();
        let extra = serde_json::json!({
            "enriched_items": enriched_json,
        });
        let extra_str = extra.to_string();

        // Deserialize as render_pending_tool_request does.
        let parsed: serde_json::Value = serde_json::from_str(&extra_str).unwrap();
        let items = parsed["enriched_items"].as_array().unwrap();
        assert_eq!(items.len(), 2);

        let item0 = &items[0];
        assert_eq!(item0["original_index"], 0);
        assert_eq!(item0["item"]["import_id"], "imp-1");
        assert_eq!(item0["pending_import"]["payee"], "SHOP");

        let item1 = &items[1];
        assert_eq!(item1["original_index"], 2);
        assert_eq!(item1["item"]["import_id"], "imp-3");
        assert_eq!(item1["pending_import"]["payee"], "STORE");
    }

    // ---- BatchAssign tests ----

    #[tokio::test]
    async fn batch_assign_pre_tool_use_allows() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_assign_pre".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: MCP_BATCH_ASSIGN_TOOL.into(),
                tool_input: serde_json::json!({
                    "user": "wonder",
                    "items": [{"import_id": "imp-1"}]
                }),
                tool_use_id: "t_assign_pre".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow { .. })) => {}
            other => panic!("PreToolUse for BatchAssign should Allow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_assign_post_tool_use_rejects_too_many_items() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let items: Vec<serde_json::Value> = (0..51)
            .map(|i| serde_json::json!({ "import_id": format!("imp-{i}") }))
            .collect();

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_assign_too_many".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_BATCH_ASSIGN_TOOL.into(),
                tool_input: serde_json::json!({ "user": "wonder", "items": items }),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_assign_post".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                assert!(output.contains("too many items"), "got: {output}");
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_assign_post_tool_use_rejects_missing_items() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_assign_no_items".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_BATCH_ASSIGN_TOOL.into(),
                tool_input: serde_json::json!({ "user": "wonder" }),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_assign_post".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                assert!(
                    output.contains("missing items"),
                    "should mention missing items: {output}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_assign_post_tool_use_rejects_missing_user() {
        // Confirm validation fires BEFORE enrichment. intercept_tool returns
        // Reject for a missing user, so the handler returns the error without
        // ever reaching enrich_batch_items.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_assign_no_user".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_BATCH_ASSIGN_TOOL.into(),
                tool_input: serde_json::json!({
                    "items": [{"import_id": "imp-1"}, {"import_id": "imp-2"}]
                }),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_assign_post".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                assert!(
                    output.contains("missing user"),
                    "should mention missing user: {output}"
                );
                assert!(
                    !output.contains("enrichment"),
                    "must short-circuit before enrichment: {output}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_assign_post_tool_use_rejects_empty_user() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_assign_empty_user".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_BATCH_ASSIGN_TOOL.into(),
                tool_input: serde_json::json!({
                    "user": "",
                    "items": [{"import_id": "imp-1"}, {"import_id": "imp-2"}]
                }),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_assign_post".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                assert!(
                    output.contains("non-empty"),
                    "should mention non-empty user requirement: {output}"
                );
                assert!(
                    !output.contains("enrichment"),
                    "must short-circuit before enrichment: {output}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_assign_post_tool_use_all_enrichment_fails_returns_error() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let (resp_tx, _) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_assign_all_fail".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_BATCH_ASSIGN_TOOL.into(),
                tool_input: serde_json::json!({
                    "user": "wonder",
                    "items": [
                        { "import_id": "imp-1" },
                        { "import_id": "imp-2" },
                    ]
                }),
                tool_response: serde_json::json!("__NOOP__"),
                tool_use_id: "t_assign_post".into(),
            },
            response_tx: resp_tx,
        };

        let result = handle_brenn_tools(&bridge, &req).await;
        match result {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                assert!(
                    output.contains("all items failed enrichment"),
                    "should report all enrichment failed: {output}"
                );
                assert!(
                    output.contains("enrichment_failed"),
                    "should include per-item failures: {output}"
                );
            }
            other => panic!("expected Continue with error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_assign_deny_via_db_resolves_and_broadcasts() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let tool_input = serde_json::json!({
            "user": "wonder",
            "items": [{ "import_id": "imp-1" }]
        });
        {
            let conn = bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_assign_deny",
                bridge.conversation_id,
                MCP_BATCH_ASSIGN_TOOL,
                &serde_json::to_string(&tool_input).unwrap(),
                None,
            );
        }

        bridge
            .handle_tool_card_response(
                "req_assign_deny",
                ToolResponseDecision::Deny {
                    reason: Some("not now".into()),
                },
            )
            .await;

        let msg = recv_broadcast(&mut broadcast_rx).await;
        assert!(
            matches!(msg, WsServerMessage::ToolCardResolved { .. }),
            "expected ToolCardResolved, got {msg:?}"
        );

        let req = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::get_pending_tool_request(&conn, "req_assign_deny")
        };
        let req = req.expect("request should exist in DB");
        assert_eq!(req.status, "denied");
        let result: serde_json::Value =
            serde_json::from_str(req.result.as_deref().unwrap()).unwrap();
        assert_eq!(result["status"], "denied");
        assert!(
            result["reason"].as_str().unwrap().contains("not now"),
            "should include reason: {result}"
        );
    }

    #[tokio::test]
    async fn batch_assign_allow_without_decisions_via_db() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let tool_input = serde_json::json!({
            "user": "wonder",
            "items": [{ "import_id": "imp-1" }]
        });
        {
            let conn = bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_assign_no_decisions",
                bridge.conversation_id,
                MCP_BATCH_ASSIGN_TOOL,
                &serde_json::to_string(&tool_input).unwrap(),
                None,
            );
        }

        bridge
            .handle_tool_card_response(
                "req_assign_no_decisions",
                ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        let _msg = recv_broadcast(&mut broadcast_rx).await;

        let req = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::get_pending_tool_request(&conn, "req_assign_no_decisions")
        };
        let req = req.expect("request should exist in DB");
        assert_eq!(req.status, "completed");
        let result: serde_json::Value =
            serde_json::from_str(req.result.as_deref().unwrap()).unwrap();
        assert!(
            result["error"].as_str().unwrap().contains("no decisions"),
            "should mention missing decisions: {result}"
        );
    }

    #[tokio::test]
    async fn batch_assign_deny_with_enrichment_failures_via_db() {
        let (bridge, _event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let extra = serde_json::json!({
            "enrichment_failures": [
                { "index": 3, "import_id": "imp-bad", "error": "pf show failed" }
            ]
        });
        {
            let conn = bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_assign_deny_enrich",
                bridge.conversation_id,
                MCP_BATCH_ASSIGN_TOOL,
                &serde_json::json!({ "user": "wonder", "items": [] }).to_string(),
                Some(&extra.to_string()),
            );
        }

        bridge
            .handle_tool_card_response(
                "req_assign_deny_enrich",
                ToolResponseDecision::Deny { reason: None },
            )
            .await;

        let _msg = recv_broadcast(&mut broadcast_rx).await;

        let req = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::get_pending_tool_request(&conn, "req_assign_deny_enrich")
        };
        let req = req.expect("request should exist in DB");
        assert_eq!(req.status, "denied");
    }

    #[test]
    fn render_dispatch_batch_assign_wide_vs_compact() {
        let registry = std::collections::HashMap::new();
        // tool_input must include `user` so the renderer has the assignee.
        let tool_input = serde_json::json!({"user": "wonder", "items": []});
        let extra = serde_json::json!({
            "enriched_items": [{
                "original_index": 0,
                "item": {"import_id": "imp-1"},
                "pending_import": {"amount": "-5.00", "payee": "CAFE", "account": "Assets:Checking"}
            }]
        });
        let extra_str = extra.to_string();

        let wide = render_pending_tool_request(
            &registry,
            MCP_BATCH_ASSIGN_TOOL,
            &tool_input,
            Some(&extra_str),
            ViewportClass::Wide,
        );
        let compact = render_pending_tool_request(
            &registry,
            MCP_BATCH_ASSIGN_TOOL,
            &tool_input,
            Some(&extra_str),
            ViewportClass::Compact,
        );

        assert!(
            wide.contains("brenn-pfin-batch-assign-table"),
            "wide should render assign table: {wide}"
        );
        assert!(
            wide.contains("@wonder"),
            "wide should show assignee: {wide}"
        );
        assert!(
            compact.contains("brenn-pfin-batch-assign-swipe"),
            "compact should render assign swipe: {compact}"
        );
        assert!(
            compact.contains("@wonder"),
            "compact should show assignee: {compact}"
        );
    }

    #[test]
    fn render_dispatch_batch_assign_no_extra_falls_back() {
        let registry = std::collections::HashMap::new();
        let tool_input = serde_json::json!({"user": "wonder", "items": []});

        let html = render_pending_tool_request(
            &registry,
            MCP_BATCH_ASSIGN_TOOL,
            &tool_input,
            None,
            ViewportClass::Wide,
        );
        // No enriched items → renderer returns None → fallback engages.
        assert!(!html.is_empty());
    }

    #[test]
    fn batch_assign_extra_round_trip() {
        // Same shape as batch_extra_round_trip — verify enriched items can be
        // round-tripped via the extra JSON.
        let enriched = [
            brenn_pfin::batch::EnrichedBatchItem {
                original_index: 0,
                item: serde_json::json!({"import_id": "imp-1", "notes": "from John"}),
                pending_import: serde_json::json!({"amount": "10.00", "payee": "SHOP"}),
            },
            brenn_pfin::batch::EnrichedBatchItem {
                original_index: 2,
                item: serde_json::json!({"import_id": "imp-3"}),
                pending_import: serde_json::json!({"amount": "20.00", "payee": "STORE"}),
            },
        ];

        let enriched_json: Vec<_> = enriched
            .iter()
            .map(|e| {
                serde_json::json!({
                    "original_index": e.original_index,
                    "item": e.item,
                    "pending_import": e.pending_import,
                })
            })
            .collect();
        let extra = serde_json::json!({
            "enriched_items": enriched_json,
        });
        let extra_str = extra.to_string();

        let parsed: serde_json::Value = serde_json::from_str(&extra_str).unwrap();
        let items = parsed["enriched_items"].as_array().unwrap();
        assert_eq!(items.len(), 2);

        let item0 = &items[0];
        assert_eq!(item0["original_index"], 0);
        assert_eq!(item0["item"]["import_id"], "imp-1");
        assert_eq!(item0["item"]["notes"], "from John");
        assert_eq!(item0["pending_import"]["payee"], "SHOP");

        let item1 = &items[1];
        assert_eq!(item1["original_index"], 2);
        assert_eq!(item1["item"]["import_id"], "imp-3");
        assert_eq!(item1["pending_import"]["payee"], "STORE");
    }

    // -----------------------------------------------------------------------
    // enrich_with_import_details invariant — panics when pfin not enabled
    // -----------------------------------------------------------------------

    #[tokio::test]
    #[should_panic(expected = "pfin enabled ⇒ config present")]
    async fn enrich_with_import_details_panics_without_pfin_integration() {
        // The new invariant: enrich_with_import_details panics when pfin is not
        // in bridge.integrations (previously it silently skipped enrichment).
        // This test documents that the panic is intentional and catches any
        // future refactor that silently re-introduces the graceful-skip path.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: std::collections::HashMap::new(), // no pfin
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        // A tool_input with import_id but no _pending_import hits the pfin_config()
        // path and panics because pfin is not enabled on this bridge.
        let _ = bridge
            .enrich_with_import_details(serde_json::json!({"import_id": "imp-1"}))
            .await;
    }
}

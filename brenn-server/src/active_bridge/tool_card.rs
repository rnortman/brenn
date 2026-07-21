//! Async tool response routing (browser → bridge → CC injection): tool-card response handling, per-tool-family execute actions, and undelivered-result replay on session resume.

use std::net::{IpAddr, Ipv4Addr};

use brenn_lib::obs::security::{SecurityEventType, log_and_alert_security_event};
use brenn_lib::ws_types::{ToolResponseDecision, WsServerMessage};
use tracing::{info, warn};

use super::ActiveBridge;
use super::mcp_constants::*;

impl ActiveBridge {
    /// Handle a tool card response from a browser tab (asynchronous — CC already continued).
    pub async fn handle_tool_card_response(
        &self,
        request_id: &str,
        decision: ToolResponseDecision,
    ) {
        self.handle_async_tool_response(request_id, &decision).await;
    }

    /// Handle a user response to an async interactive tool request (DB-backed).
    async fn handle_async_tool_response(&self, request_id: &str, decision: &ToolResponseDecision) {
        // Determine the claim status from the decision. This is used as the
        // intermediate status while the action executes. If the process crashes
        // between claim and final update, this status persists — so it should
        // be directionally correct (deny → "denied", allow → "completed").
        let claim_status = match decision {
            ToolResponseDecision::Deny { .. } => "denied",
            _ => "completed",
        };

        // Atomically claim the request to prevent races (e.g., two browser tabs
        // clicking simultaneously). Only the winner proceeds to execute the action.
        let pending = {
            let conn = self.db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&conn, request_id);
            match req {
                // Ownership guard: the row belongs to a different conversation than
                // this bridge is attached to. Authority is re-derived from the
                // session-attached `self.conversation_id`, never from the
                // browser-supplied `request_id` (security-posture §6 B1). Reject
                // before the atomic claim so probing an id cannot mark a victim's
                // row resolved as a side effect; the owner's pending request is left
                // untouched. A request_id for an unattached conversation is a
                // security signal.
                Some(p) if p.conversation_id != self.conversation_id => {
                    log_and_alert_security_event(
                        &self.alert_dispatcher,
                        SecurityEventType::SchemaViolation,
                        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                        &format!(
                            "tool card response for request_id {request_id} \
                             (status={}) in conversation {} but submitted from bridge \
                             attached to conversation {} (user {})",
                            p.status, p.conversation_id, self.conversation_id, self.user_id
                        ),
                    );
                    return;
                }
                Some(p) if p.status == "pending" => {
                    // Claim it: resolve with a placeholder to prevent double-execution.
                    // We'll update with the real result after execution.
                    if !brenn_lib::db::resolve_pending_tool_request(
                        &conn,
                        request_id,
                        claim_status,
                        Some("{}"),
                    ) {
                        warn!("async tool request {request_id} claimed by another handler (race)");
                        return;
                    }
                    p
                }
                Some(_) => {
                    // Already resolved and owned by this bridge: a benign
                    // double-click/double-tab race. Log only; not a security
                    // signal (would penalize normal double-tap UX).
                    warn!("async tool request {request_id} already resolved");
                    return;
                }
                None => {
                    // Unknown request_id: matches no row. A correct browser only
                    // ever returns a request_id the backend sent it, so this is the
                    // cross-user brute-force MISS case (design path 2) and exactly
                    // the signal fail2ban needs. Upgrade from a bare warn! to a
                    // SchemaViolation security signal.
                    log_and_alert_security_event(
                        &self.alert_dispatcher,
                        SecurityEventType::SchemaViolation,
                        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                        &format!(
                            "tool card response for unknown request_id {request_id} \
                             (bridge attached to conversation {}, user {})",
                            self.conversation_id, self.user_id
                        ),
                    );
                    return;
                }
            }
        };

        let tool_input: serde_json::Value = serde_json::from_str(&pending.tool_input)
            .expect("stored tool_input must be valid JSON");

        let extra: Option<serde_json::Value> = pending
            .extra
            .as_ref()
            .map(|e| serde_json::from_str(e).expect("stored extra must be valid JSON"));

        // Execute the action and build result. The request is already claimed in DB,
        // so no other handler can execute concurrently.
        let (new_status, result_json) = match decision {
            ToolResponseDecision::Allow { .. } => match pending.tool_name.as_str() {
                name if name == MCP_PROPOSE_RECONCILIATION_TOOL => {
                    self.execute_proposal_action(request_id, &tool_input, decision)
                        .await
                }
                name if name == MCP_BATCH_RECONCILE_TOOL => {
                    let enrichment_failures = decode_enrichment_failures(extra.as_ref());
                    self.execute_batch_action(
                        request_id,
                        &tool_input,
                        decision,
                        &enrichment_failures,
                    )
                    .await
                }
                name if name == MCP_BATCH_ASSIGN_TOOL => {
                    let enrichment_failures = decode_enrichment_failures(extra.as_ref());
                    self.execute_batch_assign_action(
                        request_id,
                        &tool_input,
                        decision,
                        &enrichment_failures,
                    )
                    .await
                }
                _ => {
                    warn!(tool = %pending.tool_name, "unknown async tool");
                    (
                        "completed",
                        serde_json::json!({
                            "status": "error",
                            "error": format!("unknown tool: {}", pending.tool_name),
                        }),
                    )
                }
            },
            ToolResponseDecision::Deny { reason } => (
                "denied",
                serde_json::json!({
                    "status": "denied",
                    "reason": reason.as_deref().unwrap_or("no reason given"),
                }),
            ),
        };

        let result_str =
            serde_json::to_string(&result_json).expect("JSON serialization cannot fail");

        // Update with the real result and final status.
        {
            let conn = self.db.lock().await;
            brenn_lib::db::update_pending_tool_result(&conn, request_id, new_status, &result_str);
        }

        self.broadcast(WsServerMessage::ToolCardResolved {
            request_id: request_id.to_string(),
            decision: decision.clone(),
        });
        self.inject_tool_result_to_cc(request_id, &pending.tool_name, &result_str)
            .await;
    }

    /// Execute a ProposeReconciliation action. Returns (status, result_json).
    async fn execute_proposal_action(
        &self,
        request_id: &str,
        tool_input: &serde_json::Value,
        decision: &ToolResponseDecision,
    ) -> (&'static str, serde_json::Value) {
        let updated_input = match decision {
            ToolResponseDecision::Allow { updated_input } => updated_input.as_ref(),
            _ => None,
        };
        let selected = updated_input
            .and_then(|ui| ui.get("selected"))
            .and_then(|v| v.as_u64());

        match selected {
            Some(idx) => {
                let username = self.get_username().await;
                let pfin_config = self
                    .pfin_config()
                    .expect("pfin enabled ⇒ config present; missing config is a startup bug");
                let selected_proposal = tool_input
                    .get("proposals")
                    .and_then(|p| p.as_array())
                    .and_then(|arr| arr.get(idx as usize));
                let selected_label = selected_proposal
                    .and_then(|p| p.get("label"))
                    .and_then(|l| l.as_str())
                    .unwrap_or("unknown");
                let import_details = tool_input.get("_pending_import");

                let ctx = self.pfin_exec_ctx(pfin_config);
                match brenn_pfin::execute_selection(tool_input, idx as usize, &ctx, &username).await
                {
                    Ok(output) => {
                        let mut result = serde_json::json!({
                            "status": "reconciled",
                            "selected_index": idx,
                            "selected_label": selected_label,
                        });
                        if let Some(proposal) = selected_proposal {
                            result["selected_proposal"] = proposal.clone();
                        }
                        if let Some(import) = import_details {
                            result["pending_import"] = import.clone();
                        }
                        let trimmed = output.trim();
                        if !trimmed.is_empty() {
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(trimmed) {
                                result["pfin_output"] = parsed;
                            } else {
                                result["pfin_output"] =
                                    serde_json::Value::String(trimmed.to_string());
                            }
                        }
                        ("completed", result)
                    }
                    Err(e) => {
                        warn!(request_id, "reconcile execution failed: {e}");
                        (
                            "completed",
                            serde_json::json!({
                                "status": "error",
                                "error": format!("Reconciliation failed: {e}"),
                            }),
                        )
                    }
                }
            }
            None => {
                warn!(request_id, "proposal approved without selected index");
                (
                    "completed",
                    serde_json::json!({
                        "status": "error",
                        "error": "User approved but no proposal was selected.",
                    }),
                )
            }
        }
    }

    /// Execute a BatchReconcile action. Returns (status, result_json).
    async fn execute_batch_action(
        &self,
        request_id: &str,
        tool_input: &serde_json::Value,
        decision: &ToolResponseDecision,
        enrichment_failures: &[(usize, String, String)],
    ) -> (&'static str, serde_json::Value) {
        let Some(decisions) = parse_batch_decisions(decision, request_id, "batch") else {
            return no_decisions_result(request_id, "batch");
        };

        let username = self.get_username().await;
        let pfin_config = self
            .pfin_config()
            .expect("pfin enabled ⇒ config present; missing config is a startup bug");
        let ctx = self.pfin_exec_ctx(pfin_config);
        let result = brenn_pfin::batch::execute_batch(
            tool_input,
            &decisions,
            &ctx,
            &username,
            enrichment_failures,
        )
        .await;
        ("completed", result)
    }

    /// Execute a BatchAssign action. Returns (status, result_json).
    ///
    /// Unlike `execute_batch_action`, the assignee is read from
    /// `tool_input["user"]` rather than from the brenn session — the LLM
    /// picks who to assign to.
    async fn execute_batch_assign_action(
        &self,
        request_id: &str,
        tool_input: &serde_json::Value,
        decision: &ToolResponseDecision,
        enrichment_failures: &[(usize, String, String)],
    ) -> (&'static str, serde_json::Value) {
        let Some(decisions) = parse_batch_decisions(decision, request_id, "batch_assign") else {
            return no_decisions_result(request_id, "batch_assign");
        };

        let pfin_config = self
            .pfin_config()
            .expect("pfin enabled ⇒ config present; missing config is a startup bug");
        let ctx = self.pfin_exec_ctx(pfin_config);
        let result = brenn_pfin::batch_assign::execute_batch_assign(
            tool_input,
            &decisions,
            &ctx,
            enrichment_failures,
        )
        .await;
        ("completed", result)
    }

    /// Inject a tool result as a user message to CC.
    async fn inject_tool_result_to_cc(&self, request_id: &str, tool_name: &str, result: &str) {
        // Send compact JSON directly — no wrapper text. CC correlates via request_id
        // in the JSON. The tool descriptions explain the async response pattern.
        let message = result.to_string();
        tracing::debug!(request_id, tool_name, "injecting tool result to CC");

        let sent = {
            let session = self.session.lock().await;
            if let Some(ref session) = *session {
                if session.is_alive() {
                    // Mark CC as not-idle after the send succeeds — this message
                    // will start a new CC turn. Prevents drain_on_idle from killing
                    // CC between inject and turn start. Set busy only on Ok so a
                    // failed send does not leave cc_idle=false with no turn running.
                    match session.send_message(&message).await {
                        Ok(()) => {
                            self.set_cc_busy("inject_tool_result");
                            info!(request_id, "injected tool result to CC");
                            true
                        }
                        Err(e) => {
                            warn!(request_id, "failed to inject tool result to CC: {e}");
                            false
                        }
                    }
                } else {
                    false
                }
            } else {
                false
            }
        }; // session lock released here

        if sent {
            let conn = self.db.lock().await;
            brenn_lib::db::mark_delivered_to_cc(&conn, request_id);
        } else {
            info!(
                request_id,
                "CC not running, tool result persisted for later delivery"
            );
        }
    }

    /// Inject all undelivered tool results to CC. Called on CC session resume.
    pub async fn deliver_pending_results(&self) {
        let undelivered = {
            let conn = self.db.lock().await;
            brenn_lib::db::get_undelivered_results(&conn, self.conversation_id)
        };

        if undelivered.is_empty() {
            return;
        }

        info!(
            count = undelivered.len(),
            "delivering pending tool results to CC"
        );

        for req in &undelivered {
            let result = req
                .result
                .as_deref()
                .unwrap_or_else(|| panic!("resolved request {} has no result", req.request_id));
            self.inject_tool_result_to_cc(&req.request_id, &req.tool_name, result)
                .await;
        }
    }
}

/// Decode persisted enrichment failures from the `extra` JSON shape.
///
/// Both `BatchReconcile` and `BatchAssign` write the same `extra.enrichment_failures`
/// shape (`[{ index, import_id, error }, ...]`). Used in the ApprovalResponse
/// dispatch (handle_async_tool_response) to feed per-item failures back to
/// the per-tool execute helper.
fn decode_enrichment_failures(extra: Option<&serde_json::Value>) -> Vec<(usize, String, String)> {
    extra
        .and_then(|e| e.get("enrichment_failures"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let idx = item.get("index")?.as_u64()? as usize;
                    let id = item.get("import_id")?.as_str()?.to_string();
                    let err = item.get("error")?.as_str()?.to_string();
                    Some((idx, id, err))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// Parse the `decisions` array from a batch tool's user response.
///
/// Both `BatchReconcile` and `BatchAssign` ship `{ decisions: [{ index, accepted }, ...] }`
/// from the frontend; this helper unifies extraction + the malformed-entry
/// warning. Returns `None` when the user approved without a `decisions`
/// array — the caller should respond to CC with a no-decisions error
/// (use `no_decisions_result`).
///
/// `tool_label` is included in the warn log for debuggability.
fn parse_batch_decisions(
    decision: &ToolResponseDecision,
    request_id: &str,
    tool_label: &str,
) -> Option<Vec<(usize, bool)>> {
    let updated_input = match decision {
        ToolResponseDecision::Allow { updated_input } => updated_input.as_ref(),
        _ => None,
    };
    let decisions_arr = updated_input
        .and_then(|ui| ui.get("decisions"))
        .and_then(|v| v.as_array())?;

    let decisions: Vec<(usize, bool)> = decisions_arr
        .iter()
        .filter_map(|d| {
            let index = d.get("index")?.as_u64()? as usize;
            let accepted = d.get("accepted")?.as_bool()?;
            Some((index, accepted))
        })
        .collect();

    if decisions.len() != decisions_arr.len() {
        warn!(
            request_id,
            tool = tool_label,
            expected = decisions_arr.len(),
            parsed = decisions.len(),
            "batch decisions array contained malformed entries"
        );
    }

    Some(decisions)
}

/// Build the (status, result_json) tuple for a batch tool that the user
/// approved without a `decisions` array. Shared by `execute_batch_action`
/// and `execute_batch_assign_action`.
fn no_decisions_result(request_id: &str, tool_label: &str) -> (&'static str, serde_json::Value) {
    warn!(
        request_id,
        tool = tool_label,
        "batch approved without decisions array"
    );
    (
        "completed",
        serde_json::json!({
            "status": "error",
            "error": "User approved but no decisions were provided.",
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use brenn_cc::session::{
        ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest, SessionEvent,
    };
    use std::sync::atomic::Ordering;
    use tokio::sync::oneshot;

    use super::super::test_fixtures::{TestBridgeConfig, pfin_test_integrations};
    use super::super::test_support::{
        await_fence, drain_broadcast, event_fence, recv_broadcast, test_bridge,
        test_bridge_with_config, test_bridge_with_failing_session,
    };

    #[tokio::test]
    async fn async_tool_claim_prevents_double_execution() {
        // Two concurrent handle_tool_card_response calls on the same request_id should
        // result in only one execution (the loser gets "already resolved").
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
                "req_race",
                bridge.conversation_id,
                MCP_BATCH_RECONCILE_TOOL,
                &serde_json::to_string(&tool_input).unwrap(),
                None,
            );
        }

        // First approval should succeed.
        bridge
            .handle_tool_card_response(
                "req_race",
                ToolResponseDecision::Deny {
                    reason: Some("first".into()),
                },
            )
            .await;

        // Drain broadcast.
        let _msg = recv_broadcast(&mut broadcast_rx).await;

        // Second approval on same request_id should be a no-op (already claimed).
        bridge
            .handle_tool_card_response(
                "req_race",
                ToolResponseDecision::Deny {
                    reason: Some("second".into()),
                },
            )
            .await;

        // Verify the first result stuck.
        let req = {
            let conn = bridge.db.lock().await;
            brenn_lib::db::get_pending_tool_request(&conn, "req_race")
        };
        let req = req.expect("request should exist");
        assert_eq!(req.status, "denied");
        let result: serde_json::Value =
            serde_json::from_str(req.result.as_deref().unwrap()).unwrap();
        assert!(
            result["reason"].as_str().unwrap().contains("first"),
            "first handler's result should persist: {result}"
        );
    }

    #[tokio::test]
    async fn compact_return_value_is_minimal_json() {
        // Verify the PostToolUse Continue response is just {"request_id":"..."}.
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge_with_config(
            TestBridgeConfig {
                integrations: pfin_test_integrations(),
                ..Default::default()
            },
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
        )
        .await;

        let tool_input = serde_json::json!({
            "import_id": "imp-789",
            "_pending_import": { "amount": "50.00", "payee": "TEST" },
            "proposals": [{
                "label": "Match",
                "transaction": {
                    "description": "Test",
                    "splits": [{"account": "A", "amount": "50.00"}]
                }
            }]
        });

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ApprovalRequired(ApprovalRequest {
                request_id: "req_compact".into(),
                kind: ApprovalKind::PostToolUse {
                    callback_id: "cb-1".into(),
                    tool_name: MCP_PROPOSE_RECONCILIATION_TOOL.to_string(),
                    tool_input: tool_input.clone(),
                    tool_response: serde_json::json!("__NOOP__"),
                    tool_use_id: "tu-1".into(),
                },
                response_tx: resp_tx,
            }))
            .await
            .unwrap();

        // Drain broadcasts (ToolUseSummary, ApprovalRequest, AwaitingApproval).
        await_fence(fence).await;
        drain_broadcast(&mut broadcast_rx);

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Continue { updated_output } => {
                let output = updated_output.expect("should have output");
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                // Should only have request_id, nothing else.
                assert_eq!(parsed["request_id"], "req_compact");
                assert!(
                    parsed.get("status").is_none(),
                    "should not have status field: {parsed}"
                );
                assert!(
                    parsed.get("message").is_none(),
                    "should not have message field: {parsed}"
                );
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Cross-type response tests: verify that sending a response to the wrong
    // handler is a harmless no-op (the split eliminated the fallthrough).
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tool_card_response_for_unknown_request_id_is_noop() {
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge().await;

        // No DB entry for this request_id.
        bridge
            .handle_tool_card_response(
                "req_nonexistent",
                ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // Should not panic — just logs a warning.
    }

    #[tokio::test]
    async fn tool_card_response_for_permission_request_id_is_noop() {
        // A permission request is in pending_permissions (in-memory), but a
        // ToolCardResponse arrives for it (wrong handler). Should be a no-op —
        // the handler only checks the DB, not pending_permissions.
        let (bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Create a pending permission via the normal flow.
        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_cross_perm".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_cross".into(),
                input: serde_json::json!({"command": "echo hi"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Drain the PermissionRequest + Status broadcasts.
        let _ = recv_broadcast(&mut broadcast_rx).await;
        let _ = recv_broadcast(&mut broadcast_rx).await;

        // Send a ToolCardResponse for this permission's request_id.
        bridge
            .handle_tool_card_response(
                "req_cross_perm",
                ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // The pending permission should still be in-memory (untouched).
        {
            let permissions = bridge.pending_permissions.lock().await;
            assert!(
                permissions.contains_key("req_cross_perm"),
                "pending permission should not have been consumed"
            );
        }

        // No ToolCardResolved broadcast (request not in DB).
        // handle_tool_card_response is a direct call; any broadcast it would emit
        // is already complete by the time .await returns. No fence needed.
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(
            msgs.is_empty(),
            "should not broadcast anything, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn approval_cancelled_for_tool_card_request_id_is_harmless() {
        // CC sends ApprovalCancelled only for synchronous permissions, never
        // for async tool cards. But if a cancel arrives for a tool card's
        // request_id, it should be harmless: the pending_permissions lookup
        // finds nothing, and the broadcast goes out as PermissionCancelled.
        // The tool card in the DB is unaffected.
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        // Insert a tool card request in the DB.
        {
            let conn = _bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_tc_cancel",
                _bridge.conversation_id,
                "mcp__brenn__ProposeReconciliation",
                r#"{"proposals":[]}"#,
                None,
            );
        }

        // Send an ApprovalCancelled event for the tool card's request_id.
        event_tx
            .send(SessionEvent::ApprovalCancelled {
                request_id: "req_tc_cancel".into(),
            })
            .await
            .unwrap();

        // Should broadcast PermissionCancelled (harmless — frontend ignores
        // cancel messages for request_ids it doesn't recognize as permissions).
        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::PermissionCancelled { request_id } => {
                assert_eq!(request_id, "req_tc_cancel");
            }
            other => panic!("expected PermissionCancelled, got {other:?}"),
        }

        // The DB tool card should be unaffected (still pending).
        {
            let conn = _bridge.db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&conn, "req_tc_cancel")
                .expect("DB entry should still exist");
            assert_eq!(
                req.status, "pending",
                "tool card should be unaffected by cancel"
            );
        }
    }

    /// H1355 primary regression (design test 1): a tool-card response whose
    /// request_id is pending in conversation B, submitted from a bridge attached
    /// to conversation A, must be rejected before the claim. Row B stays
    /// `pending` (not claimed/resolved), no `ToolCardResolved` broadcast occurs,
    /// no CC injection occurs, and a `SchemaViolation` security event fires.
    #[tokio::test]
    async fn cross_conversation_tool_card_response_rejected() {
        let (dispatcher, captured, handle) =
            brenn_lib::obs::alerting::make_capturing_alerter_with_severity();
        let (bridge, event_tx, mut broadcast_rx, _ab) =
            super::super::test_support::test_bridge_with_dispatcher(dispatcher).await;

        // Create a SECOND conversation (B) in the same DB and insert a pending
        // tool request bound to it. The bridge is attached to conversation A
        // (bridge.conversation_id), so B is a foreign owner.
        let conv_b = {
            let conn = bridge.db.lock().await;
            brenn_lib::conversation::create_conversation(&conn, bridge.user_id, "test", false)
        };
        assert_ne!(conv_b, bridge.conversation_id, "B must differ from A");
        let conv_a = bridge.conversation_id;
        let user_id = bridge.user_id;

        let tool_input = serde_json::json!({
            "items": [ { "import_id": "imp-1", "transaction": { "splits": [] } } ]
        });
        {
            let conn = bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_cross_conv",
                conv_b,
                MCP_BATCH_RECONCILE_TOOL,
                &serde_json::to_string(&tool_input).unwrap(),
                None,
            );
        }

        // Bridge A receives a response for B's request_id.
        bridge
            .handle_tool_card_response(
                "req_cross_conv",
                ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        // Row B must be untouched (still pending, no result).
        {
            let conn = bridge.db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&conn, "req_cross_conv")
                .expect("B's row must still exist");
            assert_eq!(
                req.status, "pending",
                "B's row must NOT be claimed/resolved"
            );
            assert!(req.result.is_none(), "B's row must have no result");
        }

        // No ToolCardResolved broadcast (and no other broadcast) occurred.
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(
            msgs.is_empty(),
            "cross-conversation reject must not broadcast anything, got: {msgs:?}"
        );

        // Deterministically drain both dispatcher clones before asserting.
        super::super::test_support::drop_and_drain_alerts(event_tx, bridge, handle).await;

        // Exactly one alert, and it is a Warning-severity SchemaViolation whose
        // detail carries the owning + attached conversation ids and the user id.
        let captured = captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "cross-conversation reject must emit exactly one alert, got: {captured:?}"
        );
        let (severity, title, body) = &captured[0];
        assert!(
            matches!(severity, brenn_lib::obs::alerting::AlertSeverity::Warning),
            "alert severity must be Warning, got: {severity:?}"
        );
        assert!(
            title.contains("schema_violation"),
            "alert title must be a schema_violation, got: {title}"
        );
        assert!(
            body.contains(&conv_b.to_string())
                && body.contains(&conv_a.to_string())
                && body.contains(&user_id.to_string()),
            "alert body must carry owning + attached conversation ids and user id, got: {body}"
        );
    }

    /// H1355 (design test 3): after an attacker probe on bridge A is rejected,
    /// the rightful owner (bridge B) can still resolve the request — the reject
    /// left the victim's row fully usable.
    #[tokio::test]
    async fn owner_can_resolve_after_cross_conversation_probe() {
        // Shared registry + shared DB so two bridges coexist. Build A normally,
        // then build B on the same DB attached to a second conversation.
        let (bridge_a, _event_tx_a, mut broadcast_rx_a, active_bridges) =
            super::super::test_support::test_bridge().await;

        let conv_b = {
            let conn = bridge_a.db.lock().await;
            brenn_lib::conversation::create_conversation(&conn, bridge_a.user_id, "test", false)
        };
        let (tx_b, mut broadcast_rx_b) = tokio::sync::broadcast::channel(64);
        let bridge_b = ActiveBridge::inject_for_test_full(
            bridge_a.user_id,
            conv_b,
            "test",
            bridge_a.db.clone(),
            tx_b,
            brenn_lib::obs::alerting::noop_alert_dispatcher().0,
            super::super::test_fixtures::TestBridgeConfig {
                active_bridges: Some(active_bridges.clone()),
                ..Default::default()
            },
        );

        let tool_input = serde_json::json!({
            "items": [ { "import_id": "imp-1", "transaction": { "splits": [] } } ]
        });
        {
            let conn = bridge_a.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_owner",
                conv_b,
                MCP_BATCH_RECONCILE_TOOL,
                &serde_json::to_string(&tool_input).unwrap(),
                None,
            );
        }

        // Attacker probe on bridge A — rejected, row untouched.
        bridge_a
            .handle_tool_card_response(
                "req_owner",
                ToolResponseDecision::Deny {
                    reason: Some("attacker".into()),
                },
            )
            .await;
        let a_msgs = drain_broadcast(&mut broadcast_rx_a);
        assert!(
            a_msgs.is_empty(),
            "probe must not broadcast on A: {a_msgs:?}"
        );
        {
            let conn = bridge_a.db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&conn, "req_owner").unwrap();
            assert_eq!(req.status, "pending", "row must survive the probe");
        }

        // Owner resolves on bridge B — row resolves, broadcast happens.
        bridge_b
            .handle_tool_card_response(
                "req_owner",
                ToolResponseDecision::Deny {
                    reason: Some("owner".into()),
                },
            )
            .await;

        let resolved = recv_broadcast(&mut broadcast_rx_b).await;
        match &resolved {
            WsServerMessage::ToolCardResolved { request_id, .. } => {
                assert_eq!(request_id, "req_owner");
            }
            other => panic!("expected ToolCardResolved on owner bridge, got {other:?}"),
        }
        {
            let conn = bridge_a.db.lock().await;
            let req = brenn_lib::db::get_pending_tool_request(&conn, "req_owner").unwrap();
            assert_eq!(req.status, "denied", "owner's resolve must stick");
            let result: serde_json::Value =
                serde_json::from_str(req.result.as_deref().unwrap()).unwrap();
            assert!(
                result["reason"].as_str().unwrap().contains("owner"),
                "owner's result must persist: {result}"
            );
        }
    }

    /// H1355 (design test 4): an unknown request_id (present in no row) emits a
    /// SchemaViolation security signal and executes nothing.
    #[tokio::test]
    async fn unknown_tool_card_request_id_emits_security_signal() {
        let (dispatcher, captured, handle) =
            brenn_lib::obs::alerting::make_capturing_alerter_with_severity();
        let (bridge, event_tx, mut broadcast_rx, _ab) =
            super::super::test_support::test_bridge_with_dispatcher(dispatcher).await;
        let conv_a = bridge.conversation_id;
        let user_id = bridge.user_id;

        bridge
            .handle_tool_card_response(
                "req_does_not_exist",
                ToolResponseDecision::Allow {
                    updated_input: None,
                },
            )
            .await;

        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(msgs.is_empty(), "unknown id must not broadcast: {msgs:?}");

        super::super::test_support::drop_and_drain_alerts(event_tx, bridge, handle).await;

        let captured = captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "unknown request_id must emit exactly one alert, got: {captured:?}"
        );
        let (severity, title, body) = &captured[0];
        assert!(
            matches!(severity, brenn_lib::obs::alerting::AlertSeverity::Warning),
            "alert severity must be Warning, got: {severity:?}"
        );
        assert!(
            title.contains("schema_violation"),
            "alert title must be a schema_violation, got: {title}"
        );
        assert!(
            body.contains("req_does_not_exist")
                && body.contains(&conv_a.to_string())
                && body.contains(&user_id.to_string()),
            "alert body must carry the request id, attached conversation id, and user id, got: {body}"
        );
    }

    /// H1355 (design test 5): an already-resolved id owned by THIS conversation
    /// must NOT emit a security signal — benign double-click must stay benign.
    #[tokio::test]
    async fn already_resolved_tool_card_does_not_emit_security_signal() {
        let (dispatcher, alert_count, handle) = brenn_lib::obs::alerting::make_counting_alerter();
        let (bridge, event_tx, mut broadcast_rx, _ab) =
            super::super::test_support::test_bridge_with_dispatcher(dispatcher).await;

        let tool_input = serde_json::json!({
            "items": [ { "import_id": "imp-1", "transaction": { "splits": [] } } ]
        });
        {
            let conn = bridge.db.lock().await;
            brenn_lib::db::insert_pending_tool_request(
                &conn,
                "req_dbl",
                bridge.conversation_id,
                MCP_BATCH_RECONCILE_TOOL,
                &serde_json::to_string(&tool_input).unwrap(),
                None,
            );
        }

        // First response: resolves the row (owned by this conversation).
        bridge
            .handle_tool_card_response(
                "req_dbl",
                ToolResponseDecision::Deny {
                    reason: Some("first".into()),
                },
            )
            .await;
        let _ = recv_broadcast(&mut broadcast_rx).await; // ToolCardResolved

        // Second response on the same (now resolved) id: benign double-click.
        bridge
            .handle_tool_card_response(
                "req_dbl",
                ToolResponseDecision::Deny {
                    reason: Some("second".into()),
                },
            )
            .await;

        // Deterministically drain both dispatcher clones, then assert no alert
        // ever fired — a `drop_and_drain_alerts` drain is a strong "all alerts
        // that were going to fire have now fired" guarantee, unlike a timed sleep.
        super::super::test_support::drop_and_drain_alerts(event_tx, bridge, handle).await;
        assert_eq!(
            alert_count.load(Ordering::SeqCst),
            0,
            "already-resolved double-click must NOT emit a security signal"
        );
    }

    #[tokio::test]
    async fn inject_tool_result_does_not_set_busy_when_send_fails() {
        // Covers the `set_cc_busy("inject_tool_result")` call inside the Ok(()) arm at
        // tool_card.rs:317-318 — must not fire when send_message returns Err.
        // The dummy session is alive (is_alive()=true) but has a closed channel,
        // hitting the exact race the guard protects against.
        let (bridge, _event_tx, _broadcast_rx, _ab) = test_bridge_with_failing_session().await;
        bridge.cc_idle.store(true, Ordering::SeqCst);
        bridge
            .inject_tool_result_to_cc("req_test", "test_tool", "{}")
            .await;
        assert!(
            bridge.cc_idle.load(Ordering::SeqCst),
            "cc_idle must remain true; set_cc_busy must not fire on Err from send_message"
        );
    }
}

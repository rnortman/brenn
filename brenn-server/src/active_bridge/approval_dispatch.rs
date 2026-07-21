//! Top-level approval-request dispatcher: routes ApprovalRequest events from CC to browser, AlwaysAllow rules, auto-approval for read-only tools, and PreToolUse intercept paths.

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_lib::ws_types::{CcState, WsServerMessage};
use tracing::{info, warn};

use super::mcp_constants::{MCP_EXPORT_USAGE_TOOL, MCP_RECONCILE_TOOL};
use super::permission_sync::PendingPermission;
use super::tool_summary::{ApprovalOutcome, HandleBrennToolResult};
use super::{ActiveBridge, handle_brenn_tools, render_pending_tool_request};

pub(super) async fn handle_approval_required(bridge: &ActiveBridge, req: ApprovalRequest) {
    let request_id = req.request_id.clone();

    // --- Brenn noop MCP tool interception (DisplayFile, ProposeReconciliation) ---
    if let Some(result) = handle_brenn_tools(bridge, &req).await {
        match result {
            HandleBrennToolResult::Respond(decision) => {
                if req.response_tx.send(decision).is_err() {
                    warn!("brenn tool response for {request_id} dropped — CC may have moved on");
                }
                return;
            }
            HandleBrennToolResult::PersistAndBroadcast {
                tool_name,
                tool_input,
                extra,
            } => {
                let tool_input_str =
                    serde_json::to_string(&tool_input).expect("JSON serialization cannot fail");

                // Persist to DB.
                {
                    let conn = bridge.db.lock().await;
                    brenn_lib::db::insert_pending_tool_request(
                        &conn,
                        &request_id,
                        bridge.conversation_id,
                        &tool_name,
                        &tool_input_str,
                        extra.as_deref(),
                    );
                }

                // Render and broadcast using the current viewport.
                let viewport = bridge.get_viewport_class();
                let formatted_display = render_pending_tool_request(
                    &bridge.tool_registry,
                    &tool_name,
                    &tool_input,
                    extra.as_deref(),
                    viewport,
                );

                bridge.broadcast(WsServerMessage::ToolCardRequest {
                    request_id: request_id.clone(),
                    tool_name: tool_name.clone(),
                    tool_input: tool_input.clone(),
                    formatted_display,
                });
                bridge.broadcast(WsServerMessage::Status {
                    state: CcState::AwaitingApproval,
                });

                // Return immediately to CC — don't block.
                let decision = CcApprovalDecision::Continue {
                    updated_output: Some(serde_json::json!({"request_id": request_id}).to_string()),
                };
                if req.response_tx.send(decision).is_err() {
                    warn!("async tool response for {request_id} dropped — CC may have moved on");
                }
                return;
            }
        }
    }

    // --- Hook events: observe only, no permission decisions ---
    //
    // Hooks observe tool use; they don't gate it. Tool-use summary emission
    // is handled by the ToolResult event handler, not here.
    //
    // PreToolUse: respond with "no opinion" so CC falls through to its normal
    //   permission flow (--permission-prompt-tool). Do NOT send Allow here —
    //   that would tell CC the hook granted permission, skipping the prompt.
    // PostToolUse: just respond Continue. CC skips PostToolUse on tool errors,
    //   so summary emission lives in the ToolResult handler instead.

    if let ApprovalKind::PreToolUse { tool_name, .. } = &req.kind {
        info!(tool = %tool_name, "PreToolUse hook — continuing with no permission opinion");
        let decision = CcApprovalDecision::Continue {
            updated_output: None,
        };
        if req.response_tx.send(decision).is_err() {
            warn!("PreToolUse continue for {request_id} dropped");
        }
        return;
    }

    if let ApprovalKind::PostToolUse { tool_name, .. } = &req.kind {
        // Summary emission moved to the ToolResult handler — it always fires,
        // whereas CC skips PostToolUse when a tool returns an error.
        // We still need to respond Continue so CC doesn't time out.
        info!(tool = %tool_name, "PostToolUse hook — continuing (summary emitted from ToolResult)");
        let decision = CcApprovalDecision::Continue {
            updated_output: None,
        };
        if req.response_tx.send(decision).is_err() {
            warn!("PostToolUse continue for {request_id} dropped");
        }
        return;
    }

    if let ApprovalKind::OtherHook { .. } = &req.kind {
        let decision = CcApprovalDecision::Continue {
            updated_output: None,
        };
        if req.response_tx.send(decision).is_err() {
            warn!("OtherHook continue for {request_id} dropped");
        }
        return;
    }

    // --- Permission requests: route to browser for user approval ---
    //
    // Auto-approve read-only tools that CC doesn't handle internally.
    // Everything else goes to the browser as an approval dialog.

    let (tool_name, original_input, tool_use_id) = match &req.kind {
        ApprovalKind::Permission {
            tool_name,
            input,
            tool_use_id,
        } => (tool_name.clone(), input.clone(), tool_use_id.clone()),
        _ => unreachable!("all non-Permission kinds handled above"),
    };

    let approval_match = bridge
        .approval_rules
        .check(tool_name.as_str(), &original_input)
        .await;

    if approval_match.is_approved() {
        info!(
            tool = %tool_name,
            reason = %approval_match.description(),
            "auto-approving tool"
        );
        // Store approval info so the ToolResult handler can show which rule
        // auto-approved this tool in the detail view.
        {
            let mut outcomes = bridge.approval_outcomes.lock().await;
            outcomes.insert(tool_use_id, ApprovalOutcome { approval_match });
        }
        let decision = CcApprovalDecision::Allow {
            updated_input: Some(original_input),
        };
        if req.response_tx.send(decision).is_err() {
            warn!("auto-approve for {request_id} dropped");
        }
        return;
    }

    // Enrich display_input with call-site-computed metadata. Each tool gets
    // one enrichment pass; async enrichers run before sync ones. All
    // enrichment works on the display clone only — original_input is never
    // mutated and is what gets stored and sent to CC.
    let display_input = match tool_name.as_str() {
        n if n == MCP_RECONCILE_TOOL => {
            // Inject pending import details so the approval display can show
            // the import header as context.
            //
            // Invariant: `mcp__pfin__reconcile` is only callable by CC when
            // `[app.mcp_servers.pfin]` is configured. `enrich_with_import_details`
            // calls `pfin_config().expect(...)`, which panics if the pfin
            // *integration* is not enabled on this app. These two stanzas must
            // co-occur; a config carrying `[app.mcp_servers.pfin]` without
            // `integrations = ["pfin"]` is a misconfiguration that will panic
            // here at runtime. This is intentional (fail-fast on misconfig).
            bridge
                .enrich_with_import_details(original_input.clone())
                .await
        }
        n if n == MCP_EXPORT_USAGE_TOOL => {
            // Annotate with git-sync state so the approval prompt warns when
            // the destination is in an auto-push repo.
            bridge.annotate_git_sync(original_input.clone())
        }
        _ => original_input.clone(),
    };

    // Stash the pending permission; `display_input` is stored so a
    // late-attaching tab can re-render the dialog from the same input.
    {
        let mut permissions = bridge.pending_permissions.lock().await;
        permissions.insert(
            request_id.clone(),
            PendingPermission {
                tx: req.response_tx,
                original_input: original_input.clone(),
                tool_use_id: tool_use_id.clone(),
                tool_name: tool_name.clone(),
                display_input: display_input.clone(),
            },
        );
    }

    let formatted_display = crate::approval_formatter::format_tool_display(
        &bridge.tool_registry,
        &tool_name,
        &display_input,
    );

    bridge.broadcast(WsServerMessage::PermissionRequest {
        request_id,
        tool_name,
        tool_input: original_input,
        formatted_display,
    });
    bridge.broadcast(WsServerMessage::Status {
        state: CcState::AwaitingApproval,
    });
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        await_fence, drain_broadcast, event_fence, recv_broadcast, test_bridge,
    };
    use brenn_cc::session::{
        ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest, SessionEvent,
    };
    use brenn_lib::ws_types::{CcState, WsServerMessage};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn approval_auto_approves_read_only_tools() {
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_1".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Read".into(),
                tool_use_id: "t1".into(),
                input: serde_json::json!({"file_path": "/tmp/foo"}),
            },
            response_tx: resp_tx,
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Should have sent Allow decision back to CC (auto-approved).
        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Allow { .. }),
            "should allow read-only tool"
        );

        // Should NOT broadcast anything to WS — auto-approved, no summary.
        // Summaries come from PostToolUse hooks, not from Permission auto-approve.
        await_fence(fence).await;
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(
            msgs.is_empty(),
            "auto-approved Permission should not broadcast, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn approval_auto_approve_includes_original_input() {
        // Regression: auto-approved Permission must echo original input back to CC.
        // CC protocol requires updated_input on Allow.
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let original_input = serde_json::json!({"file_path": "/tmp/foo"});
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_auto_input".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Read".into(),
                tool_use_id: "t_auto".into(),
                input: original_input.clone(),
            },
            response_tx: resp_tx,
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let decision = resp_rx.await.unwrap();
        match decision {
            CcApprovalDecision::Allow { updated_input } => {
                assert_eq!(
                    updated_input,
                    Some(original_input),
                    "auto-approve must echo original input"
                );
            }
            other => panic!("expected Allow, got {other:?}"),
        }

        // Drain so the test doesn't leak.
        await_fence(fence).await;
        drain_broadcast(&mut broadcast_rx);
    }

    #[tokio::test]
    async fn approval_routes_non_readonly_to_browser() {
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_2".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Write".into(),
                tool_use_id: "t2".into(),
                input: serde_json::json!({"file_path": "/tmp/foo", "content": "bar"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        // Should broadcast PermissionRequest + Status(AwaitingApproval).
        let msg1 = recv_broadcast(&mut broadcast_rx).await;
        let msg2 = recv_broadcast(&mut broadcast_rx).await;

        match &msg1 {
            WsServerMessage::PermissionRequest {
                request_id,
                tool_name,
                ..
            } => {
                assert_eq!(request_id, "req_2");
                assert_eq!(tool_name, "Write");
            }
            other => panic!("expected PermissionRequest, got {other:?}"),
        }
        match &msg2 {
            WsServerMessage::Status { state } => assert_eq!(*state, CcState::AwaitingApproval),
            other => panic!("expected Status, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pre_tool_use_hook_sends_continue_not_allow() {
        // Regression: PreToolUse hooks must send Continue (no permission opinion),
        // NOT Allow. Sending Allow tells CC to skip the permission prompt entirely,
        // which was the root cause of all tools being auto-approved.
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_pre".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: "Bash".into(),
                tool_input: serde_json::json!({"command": "rm -rf /"}),
                tool_use_id: "t_pre".into(),
            },
            response_tx: resp_tx,
        };
        let fence = event_fence(&_bridge);
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Continue { .. }),
            "PreToolUse hook must send Continue (no permission opinion), got {decision:?}"
        );

        // PreToolUse hooks should not broadcast anything.
        await_fence(fence).await;
        let msgs = drain_broadcast(&mut broadcast_rx);
        assert!(
            msgs.is_empty(),
            "PreToolUse hook should not broadcast, got: {msgs:?}"
        );
    }

    #[tokio::test]
    async fn pre_tool_use_hook_for_read_only_tool_also_sends_continue() {
        // Even read-only tools get Continue from PreToolUse — the auto-approve
        // happens at the Permission level, not the hook level.
        let (_bridge, event_tx, _broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_pre_read".into(),
            kind: ApprovalKind::PreToolUse {
                callback_id: "brenn_pre_tool_0".into(),
                tool_name: "Read".into(),
                tool_input: serde_json::json!({"file_path": "/tmp/foo"}),
                tool_use_id: "t_pre_read".into(),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let decision = resp_rx.await.unwrap();
        assert!(
            matches!(decision, CcApprovalDecision::Continue { .. }),
            "PreToolUse for Read should also send Continue, got {decision:?}"
        );
    }

    #[tokio::test]
    async fn ask_user_question_uses_custom_component() {
        // The test bridge has an empty tool_registry, so AskUserQuestion falls
        // through to the JSON fallback wrapped in brenn-tool-approve. To test the
        // AppTool path, we'd need to register AskUserQuestionTool in the test bridge.
        // Here we just verify the WS message carries the original tool_input unchanged
        // (no _rendered augmentation — that's now in formatted_display via the AppTool).
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_auq".into(),
            kind: ApprovalKind::Permission {
                tool_name: "AskUserQuestion".into(),
                tool_use_id: "t_auq".into(),
                input: serde_json::json!({
                    "questions": [{
                        "question": "Which **library**?",
                        "header": "Library",
                        "multiSelect": false,
                        "options": [
                            {"label": "date-fns", "description": "Lightweight"},
                            {"label": "dayjs", "description": "Small footprint"}
                        ]
                    }]
                }),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::PermissionRequest {
                tool_name,
                tool_input,
                ..
            } => {
                assert_eq!(tool_name, "AskUserQuestion");
                // tool_input should be the original, without _rendered augmentation.
                assert!(
                    tool_input.get("_rendered").is_none(),
                    "tool_input should NOT have _rendered field: {tool_input}"
                );
                assert!(tool_input["questions"].is_array());
                assert_eq!(
                    tool_input["questions"][0]["question"].as_str().unwrap(),
                    "Which **library**?"
                );
            }
            other => panic!("expected ApprovalRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn approval_request_embeds_default_patterns_in_html() {
        let (_bridge, event_tx, mut broadcast_rx, _ab) = test_bridge().await;

        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_dp".into(),
            kind: ApprovalKind::Permission {
                tool_name: "Bash".into(),
                tool_use_id: "t_dp".into(),
                input: serde_json::json!({"command": "git status --porcelain"}),
            },
            response_tx: resp_tx,
        };
        event_tx
            .send(SessionEvent::ApprovalRequired(req))
            .await
            .unwrap();

        let msg = recv_broadcast(&mut broadcast_rx).await;
        match &msg {
            WsServerMessage::PermissionRequest {
                formatted_display, ..
            } => {
                assert!(
                    formatted_display.contains("brenn-tool-approve"),
                    "should contain tool-approve component: {formatted_display}"
                );
                assert!(
                    formatted_display.contains("default_patterns"),
                    "should embed default patterns in HTML: {formatted_display}"
                );
                assert!(
                    formatted_display.contains("git status"),
                    "should contain pattern: {formatted_display}"
                );
            }
            other => panic!("expected ApprovalRequest, got {other:?}"),
        }
    }
}

//! LLM adapter for the first-class tool registry.
//!
//! Placed first in `handle_brenn_tools`: if CC's tool name resolves to a
//! registered tool, this handler owns it. PreToolUse gates on the grant plus
//! the descriptor's `auto_approve`; PostToolUse runs the tool through
//! `invoke_for_llm` (the same grant table, rate-limit bucket, ACL check, and
//! per-tool concurrency guard the bus executor uses) and returns its output.
//!
//! Tools CC never had declared to it (ungranted) are anomalous — the adapter
//! denies them with a warn log (defense in depth; the declaration source only
//! ever emits granted tools).

use brenn_cc::session::{ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest};
use brenn_lib::approval_rules::ApprovalMatch;
use brenn_lib::messaging::ParticipantId;
use serde_json::json;
use tracing::{info, warn};

use super::super::ActiveBridge;
use super::super::tool_summary::{HandleBrennToolResult, emit_tool_summary, mark_tool_handled};
use crate::tool_registry::ToolError;

/// Handle PreToolUse + PostToolUse for any registry tool. Returns `None` when
/// the request's tool name does not resolve to a registered tool (letting the
/// dispatcher fall through to the legacy per-family handlers).
pub(super) async fn handle(
    bridge: &ActiveBridge,
    req: &ApprovalRequest,
) -> Option<HandleBrennToolResult> {
    match &req.kind {
        ApprovalKind::PreToolUse { tool_name, .. } => {
            // Copy the canonical name + auto_approve out so the immutable borrow
            // of the registry ends before we touch `bridge.tool_grants`.
            let (canonical, auto_approve) = {
                let tool = bridge.tools.get_by_mcp_name(tool_name)?;
                let desc = tool.descriptor();
                (desc.name, desc.auto_approve)
            };

            if !bridge.tool_grants.contains_key(canonical) {
                // Declared tools are granted tools, so CC calling an ungranted
                // one is anomalous — surface it, deny it.
                warn!(
                    tool = %tool_name,
                    app = %bridge.app_slug,
                    "registry_adapter: denying ungranted registry tool call",
                );
                return Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny {
                    reason: format!("Tool {canonical} is not granted to this app."),
                }));
            }

            if auto_approve {
                info!(tool = %tool_name, "registry_adapter: auto-approving granted tool");
                Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow {
                    updated_input: None,
                }))
            } else {
                // Granted but not auto-approve: fall through to the normal
                // Permission flow (user approval UI) by returning None.
                None
            }
        }

        ApprovalKind::PostToolUse {
            tool_name,
            tool_input,
            tool_use_id,
            ..
        } => {
            let canonical = bridge.tools.get_by_mcp_name(tool_name)?.descriptor().name;

            let Some(grant) = bridge.tool_grants.get(canonical).cloned() else {
                // Ungranted at execution (Pre already denied; belt-and-suspenders).
                warn!(
                    tool = %tool_name,
                    app = %bridge.app_slug,
                    "registry_adapter: PostToolUse for ungranted tool",
                );
                mark_tool_handled(bridge, tool_use_id).await;
                return Some(HandleBrennToolResult::Respond(
                    CcApprovalDecision::Continue {
                        updated_output: Some(
                            tool_error_to_json(canonical, &ToolError::NotGranted).to_string(),
                        ),
                    },
                ));
            };

            mark_tool_handled(bridge, tool_use_id).await;

            let caller = ParticipantId::for_app(&bridge.app_slug, &bridge.server_origin);
            let outcome = bridge
                .tools
                .invoke_for_llm(
                    tool_name,
                    caller,
                    grant,
                    Some(bridge.conversation_id),
                    tool_input.clone(),
                    &bridge.alert_dispatcher,
                )
                .await;

            let output = match outcome {
                Ok(value) => value,
                Err(e) => tool_error_to_json(canonical, &e),
            };

            emit_tool_summary(
                bridge,
                tool_name,
                tool_input,
                None,
                Some(&ApprovalMatch::GlobalTool),
                false,
            )
            .await;

            Some(HandleBrennToolResult::Respond(
                CcApprovalDecision::Continue {
                    updated_output: Some(output.to_string()),
                },
            ))
        }

        _ => None,
    }
}

/// Render a tool error as the JSON body CC receives in place of tool output.
fn tool_error_to_json(tool: &str, err: &ToolError) -> serde_json::Value {
    let (kind, detail) = match err {
        ToolError::NotGranted => (
            "not_granted".to_string(),
            format!("tool {tool} not granted"),
        ),
        ToolError::Denied(resource) => (
            "denied".to_string(),
            format!("access to {resource:?} is outside this app's grant"),
        ),
        ToolError::InvalidArgs(msg) => ("invalid_args".to_string(), msg.clone()),
        ToolError::RateLimited => (
            "rate_limited".to_string(),
            format!("rate limit exceeded for tool {tool}"),
        ),
        ToolError::WrongClass => (
            "wrong_class".to_string(),
            format!("tool {tool} was called with the wrong class"),
        ),
        ToolError::Internal(msg) => ("internal".to_string(), msg.clone()),
    };
    json!({ "error": detail, "error_type": kind })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use brenn_cc::session::{
        ApprovalDecision as CcApprovalDecision, ApprovalKind, ApprovalRequest,
    };
    use brenn_lib::db::init_db_memory;
    use brenn_lib::tools::{AclClause, ResolvedToolGrant};
    use tokio::sync::{broadcast, oneshot};

    use super::super::super::ActiveBridge;
    use super::super::super::test_fixtures::TestBridgeConfig;
    use super::super::HandleBrennToolResult;
    use super::super::handle_brenn_tools;
    use crate::tool_registry::{
        AclDenied, DEFAULT_FAST_BUDGET, FastTool, GitRepoPullTool, Idempotency, RegisteredTool,
        ToolClass, ToolCtx, ToolDescriptor, ToolError, ToolRegistry,
    };

    const MCP_PULL: &str = "mcp__brenn__GitRepoPull";
    const MCP_MANUAL: &str = "mcp__brenn__ManualTool";

    /// A fast tool that is *not* auto-approve, so a granted call to it must fall
    /// through the adapter to the normal Permission flow. `execute` is never
    /// reached by these PreToolUse tests.
    struct ManualTool {
        descriptor: ToolDescriptor,
    }

    impl ManualTool {
        fn new() -> Self {
            Self {
                descriptor: ToolDescriptor {
                    name: "manual-tool",
                    mcp_name: MCP_MANUAL,
                    description: "requires user approval",
                    input_schema: serde_json::json!({ "type": "object" }),
                    class: ToolClass::Fast {
                        budget: DEFAULT_FAST_BUDGET,
                    },
                    acl_keys: &[],
                    idempotency: Idempotency::Natural,
                    auto_approve: false,
                },
            }
        }
    }

    impl FastTool for ManualTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }
        fn check_acl(
            &self,
            _args: &serde_json::Value,
            _acl: &[AclClause],
        ) -> Result<(), AclDenied> {
            Ok(())
        }
        fn execute(
            &self,
            _ctx: &ToolCtx,
            _args: serde_json::Value,
        ) -> Result<serde_json::Value, ToolError> {
            Ok(serde_json::json!({}))
        }
    }

    fn clause(repo: &str) -> AclClause {
        AclClause::new(BTreeMap::from([("repo".to_string(), repo.to_string())]))
    }

    /// A registry holding only git-repo-pull, wired to an empty clone index (no
    /// clone to resolve — execute returns per-repo "unknown", which is fine for
    /// exercising the adapter's Pre/Post routing, not the pull itself).
    fn registry() -> Arc<ToolRegistry> {
        let tool = GitRepoPullTool::new(
            Arc::new(Default::default()),
            Arc::new(Default::default()),
            None,
        );
        Arc::new(ToolRegistry::new(vec![RegisteredTool::Async(Arc::new(
            tool,
        ))]))
    }

    async fn bridge_with(
        tools: Arc<ToolRegistry>,
        grants: BTreeMap<String, ResolvedToolGrant>,
    ) -> Arc<ActiveBridge> {
        let db = init_db_memory();
        let (uid, cid) = {
            let conn = db.lock().await;
            let uid = brenn_lib::auth::user::create_user(&conn, "adapt-user", "$argon2id$fake");
            let cid = brenn_lib::conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let (tx, _rx) = broadcast::channel(16);
        let (alert, _h) = brenn_lib::obs::alerting::noop_alert_dispatcher();
        ActiveBridge::inject_for_test_full(
            uid,
            cid,
            "testapp",
            db,
            tx,
            alert,
            TestBridgeConfig {
                tools: Some(tools),
                tool_grants: grants,
                ..Default::default()
            },
        )
    }

    fn pre_req(tool: &str) -> (ApprovalRequest, oneshot::Receiver<CcApprovalDecision>) {
        let (resp_tx, resp_rx) = oneshot::channel();
        (
            ApprovalRequest {
                request_id: "req".into(),
                kind: ApprovalKind::PreToolUse {
                    callback_id: "brenn_pre_tool_0".into(),
                    tool_name: tool.into(),
                    tool_input: serde_json::json!({"repos": ["brenn"]}),
                    tool_use_id: "tu".into(),
                },
                response_tx: resp_tx,
            },
            resp_rx,
        )
    }

    #[tokio::test]
    async fn pre_tool_use_auto_approves_granted_tool() {
        let grants = BTreeMap::from([(
            "git-repo-pull".to_string(),
            ResolvedToolGrant {
                acl: vec![clause("brenn")],
                rate_limit: None,
            },
        )]);
        let bridge = bridge_with(registry(), grants).await;
        let (req, _rx) = pre_req(MCP_PULL);
        match handle_brenn_tools(&bridge, &req).await {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Allow { .. })) => {}
            other => panic!("granted auto-approve tool should Allow, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pre_tool_use_denies_ungranted_tool() {
        // Registry holds git-repo-pull, but the app has no grant for it.
        let bridge = bridge_with(registry(), BTreeMap::new()).await;
        let (req, _rx) = pre_req(MCP_PULL);
        match handle_brenn_tools(&bridge, &req).await {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Deny { reason })) => {
                assert!(reason.contains("git-repo-pull"), "reason: {reason}");
            }
            other => panic!("ungranted tool should Deny, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pre_tool_use_falls_through_for_granted_non_auto_approve_tool() {
        // A granted registry tool with `auto_approve: false` must not be
        // auto-allowed nor denied — the adapter returns None so the normal
        // Permission (user-approval) flow runs.
        let registry = Arc::new(ToolRegistry::new(vec![RegisteredTool::Fast(Arc::new(
            ManualTool::new(),
        ))]));
        let grants = BTreeMap::from([(
            "manual-tool".to_string(),
            ResolvedToolGrant {
                acl: vec![],
                rate_limit: None,
            },
        )]);
        let bridge = bridge_with(registry, grants).await;
        let (req, _rx) = pre_req(MCP_MANUAL);
        assert!(
            handle_brenn_tools(&bridge, &req).await.is_none(),
            "granted non-auto-approve tool must fall through to Permission flow",
        );
    }

    #[tokio::test]
    async fn pre_tool_use_ignores_non_registry_tool() {
        // A tool the registry does not know falls through (None).
        let bridge = bridge_with(registry(), BTreeMap::new()).await;
        let (req, _rx) = pre_req("Bash");
        assert!(
            handle_brenn_tools(&bridge, &req).await.is_none(),
            "non-registry tool must fall through",
        );
    }

    #[tokio::test]
    async fn post_tool_use_runs_tool_and_returns_output() {
        // Granted; the empty clone index yields a per-repo "unknown" outcome —
        // enough to prove the adapter invoked the tool and returned its output.
        let grants = BTreeMap::from([(
            "git-repo-pull".to_string(),
            ResolvedToolGrant {
                acl: vec![clause("brenn")],
                rate_limit: None,
            },
        )]);
        let bridge = bridge_with(registry(), grants).await;
        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_post".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_PULL.into(),
                tool_input: serde_json::json!({"repos": ["brenn"]}),
                tool_use_id: "tu_post".into(),
                tool_response: serde_json::Value::Null,
            },
            response_tx: resp_tx,
        };
        match handle_brenn_tools(&bridge, &req).await {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                let repos = parsed["repos"].as_array().expect("repos array");
                assert_eq!(repos.len(), 1);
                assert_eq!(repos[0]["error_type"], "unknown");
            }
            other => panic!("PostToolUse should Continue with output, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn post_tool_use_denied_when_acl_rejects_slug() {
        // Grant admits "brenn"; the call names "secret" → ACL denial surfaced as
        // an error body (not a pull).
        let grants = BTreeMap::from([(
            "git-repo-pull".to_string(),
            ResolvedToolGrant {
                acl: vec![clause("brenn")],
                rate_limit: None,
            },
        )]);
        let bridge = bridge_with(registry(), grants).await;
        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_denied".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_PULL.into(),
                tool_input: serde_json::json!({"repos": ["secret"]}),
                tool_use_id: "tu_denied".into(),
                tool_response: serde_json::Value::Null,
            },
            response_tx: resp_tx,
        };
        match handle_brenn_tools(&bridge, &req).await {
            Some(HandleBrennToolResult::Respond(CcApprovalDecision::Continue {
                updated_output: Some(output),
            })) => {
                let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(parsed["error_type"], "denied");
            }
            other => panic!("ACL-denied call should Continue with error body, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn post_tool_use_emits_summary_card() {
        use brenn_lib::ws_types::WsServerMessage;

        // The registry adapter must preserve the summary-card UX GitRepoPull had
        // on the legacy intercept: PostToolUse emits a ToolUseSummary broadcast.
        let grants = BTreeMap::from([(
            "git-repo-pull".to_string(),
            ResolvedToolGrant {
                acl: vec![clause("brenn")],
                rate_limit: None,
            },
        )]);
        let bridge = bridge_with(registry(), grants).await;
        let mut rx = bridge.subscribe();
        let (resp_tx, _resp_rx) = oneshot::channel();
        let req = ApprovalRequest {
            request_id: "req_card".into(),
            kind: ApprovalKind::PostToolUse {
                callback_id: "brenn_post_tool_0".into(),
                tool_name: MCP_PULL.into(),
                tool_input: serde_json::json!({"repos": ["brenn"]}),
                tool_use_id: "tu_card".into(),
                tool_response: serde_json::Value::Null,
            },
            response_tx: resp_tx,
        };
        handle_brenn_tools(&bridge, &req).await;

        let mut saw_summary = false;
        while let Ok(msg) = rx.try_recv() {
            if let WsServerMessage::ToolUseSummary { tool_name, .. } = msg
                && tool_name == MCP_PULL
            {
                saw_summary = true;
            }
        }
        assert!(
            saw_summary,
            "PostToolUse must emit a ToolUseSummary card for the pull",
        );
    }
}

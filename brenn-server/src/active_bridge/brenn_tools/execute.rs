//! Admission and execution of a virtual tool call, independent of the entry
//! point it arrived on.
//!
//! `admit_tool_call` is the one place the server looks a tool name up in the
//! agent's catalog and validates the call's input against the tool's schema.
//! `execute_admitted` runs an admitted call through the dispatch table of the
//! tool families that execute outside the hook.

use brenn_cc::session::ApprovalDecision as CcApprovalDecision;
use brenn_lib::integration::CliAccess;
use brenn_tool_proto::WireError;
use serde_json::json;

use super::super::ActiveBridge;
use super::super::cc_spawn_config::find_virtual_tool;
use super::super::mcp_constants::MCP_BRENN_PREFIX;
use super::{device, git, registry_adapter, timezone};

/// What a tool produced. `Err` carries the JSON the LLM receives for a failed
/// call, in the family's own error shape.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ToolOutcome {
    Ok(serde_json::Value),
    Err(serde_json::Value),
}

impl ToolOutcome {
    pub(crate) fn is_error(&self) -> bool {
        matches!(self, ToolOutcome::Err(_))
    }

    /// The tool's output JSON, success or failure.
    pub(crate) fn json(&self) -> &serde_json::Value {
        match self {
            ToolOutcome::Ok(v) | ToolOutcome::Err(v) => v,
        }
    }

    /// The PostToolUse answer that substitutes this outcome for the stub's
    /// `__NOOP__`.
    pub(crate) fn into_continue(self) -> CcApprovalDecision {
        CcApprovalDecision::Continue {
            updated_output: Some(self.json().to_string()),
        }
    }
}

/// Why a call was refused before any tool work ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExecuteError {
    /// Socket origin only: the name is in the catalog but is not a command.
    NotAvailable(CliAccess),
    /// The name is not in this agent's catalog (unknown and ungranted are one
    /// answer).
    UnknownTool,
    /// The input failed schema validation.
    InvalidArgs(String),
}

impl ExecuteError {
    /// The human-readable detail of the refusal, naming the tool.
    pub(crate) fn detail(&self, short_name: &str) -> String {
        match self {
            ExecuteError::NotAvailable(access) => format!("{short_name}: {access}"),
            ExecuteError::UnknownTool => format!("{short_name} is not a tool this agent has"),
            ExecuteError::InvalidArgs(msg) => format!("invalid arguments for {short_name}: {msg}"),
        }
    }

    /// The refusal as the socket answers it. Its `kind` is also the hook
    /// path's `error`, so both origins speak one vocabulary.
    pub(crate) fn to_wire(&self, short_name: &str) -> WireError {
        let detail = self.detail(short_name);
        match self {
            ExecuteError::NotAvailable(_) => WireError::NotAvailable { detail },
            ExecuteError::UnknownTool => WireError::UnknownTool { detail },
            ExecuteError::InvalidArgs(_) => WireError::InvalidArgs { detail },
        }
    }

    /// `{"error":"<kind>","detail":…}` — the hook path's answer, in the shape
    /// the tool families use for their own rejections.
    pub(crate) fn to_json(&self, short_name: &str) -> serde_json::Value {
        json!({ "error": self.to_wire(short_name).kind(), "detail": self.detail(short_name) })
    }
}

/// Who is calling. Only the socket is refused on `CliAccess`; a hook call's
/// PreToolUse leg has already decided whether the tool may run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallOrigin {
    Hook,
    Socket,
}

/// Admit a call: look `short_name` (the `mcp__brenn__`-less name) up in the
/// agent's live catalog, refuse non-command tools for the socket origin, and
/// validate `input` against the tool's schema.
///
/// The catalog is computed per call from the agent's current config and the
/// tool registry, so a reload's grant changes decide the next call.
pub(crate) fn admit_tool_call(
    bridge: &ActiveBridge,
    origin: CallOrigin,
    short_name: &str,
    input: &serde_json::Value,
) -> Result<(), ExecuteError> {
    let def = find_virtual_tool(&bridge.app_config(), &bridge.tools, short_name)
        .ok_or(ExecuteError::UnknownTool)?;
    if origin == CallOrigin::Socket && def.cli != CliAccess::Available {
        return Err(ExecuteError::NotAvailable(def.cli));
    }
    brenn_lib::tool_input::validate_tool_input(short_name, &def.input_schema, input)
        .map_err(ExecuteError::InvalidArgs)
}

/// The dispatch table of the families that execute outside the hook, in
/// `handle_brenn_tools` order, registry first. `None`: no family owns the name.
async fn dispatch(
    bridge: &ActiveBridge,
    short_name: &str,
    input: &serde_json::Value,
) -> Option<ToolOutcome> {
    let mcp_name = format!("{MCP_BRENN_PREFIX}{short_name}");
    if let Some(outcome) = registry_adapter::execute_registry_tool(bridge, &mcp_name, input).await {
        return Some(outcome);
    }
    if let Some(outcome) =
        crate::messaging_intercept::execute_messaging_tool(bridge, &mcp_name, input).await
    {
        return Some(outcome);
    }
    if let Some(outcome) =
        crate::automation_intercept::execute_automation_tool(bridge, &mcp_name, input).await
    {
        return Some(outcome);
    }
    if let Some(outcome) =
        crate::pwa_push_intercept::execute_pwa_push_tool(bridge, &mcp_name, input).await
    {
        return Some(outcome);
    }
    if let Some(outcome) = crate::mqtt_intercept::execute_mqtt_tool(bridge, &mcp_name, input).await
    {
        return Some(outcome);
    }
    if let Some(outcome) = device::execute_device_tool(bridge, &mcp_name, input).await {
        return Some(outcome);
    }
    if let Some(outcome) = git::execute_git_read_tool(bridge, &mcp_name, input).await {
        return Some(outcome);
    }
    timezone::execute_timezone_tool(bridge, &mcp_name, input).await
}

/// Run a call `admit_tool_call` has admitted.
///
/// Panics if no family executes the name: a catalog entry without an executor
/// is a bug, not an outcome to hand the caller.
pub(crate) async fn execute_admitted(
    bridge: &ActiveBridge,
    short_name: &str,
    input: &serde_json::Value,
) -> ToolOutcome {
    if let Some(outcome) = dispatch(bridge, short_name, input).await {
        return outcome;
    }
    match find_virtual_tool(&bridge.app_config(), &bridge.tools, short_name) {
        Some(def) => panic!(
            "BUG: tool {short_name} is in the catalog as `cli = {}` but no family executes it",
            def.cli
        ),
        None => {
            panic!("BUG: execute_admitted called for {short_name}, which is not in the catalog")
        }
    }
}

/// Admit a call, then run it: the socket's entry point.
pub(crate) async fn execute_tool(
    bridge: &ActiveBridge,
    origin: CallOrigin,
    short_name: &str,
    input: serde_json::Value,
) -> Result<ToolOutcome, ExecuteError> {
    admit_tool_call(bridge, origin, short_name, &input)?;
    Ok(execute_admitted(bridge, short_name, &input).await)
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use brenn_envelope::grants::AppCapability;
    use brenn_lib::tools::ResolvedToolGrant;
    use serde_json::json;

    use super::*;
    use crate::active_bridge::test_fixtures::{
        TestBridgeConfig, pfin_test_integrations, single_app_table,
    };
    use crate::active_bridge::test_support::test_bridge;
    use crate::active_bridge::virtual_tools;

    /// A bridge whose agent is offered every virtual tool there is: all three
    /// send grants, a read-write mount under `mount_dir`, pfin, and a granted
    /// `git-repo-pull`. `repo_sync_sender` is wired when given.
    pub(in crate::active_bridge::brenn_tools) async fn full_catalog_bridge(
        mount_dir: &std::path::Path,
        repo_sync_sender: Option<brenn_git::sync::SyncTriggerSender>,
    ) -> Arc<ActiveBridge> {
        let mounts = vec![brenn_lib::config::ResolvedMount {
            slug: "test-repo".to_string(),
            host_path: mount_dir.to_path_buf(),
            container_path: None,
            access: brenn_lib::config::AccessLevel::ReadWrite,
            auto_pull: false,
            is_working_dir: false,
            primary: false,
        }];
        let integrations = pfin_test_integrations();
        let apps = single_app_table("testapp", |app| {
            app.messaging = None;
            app.mounts = mounts.clone();
            app.integrations = integrations.clone();
            let policy = app.policy_mut();
            for cap in [
                AppCapability::MessagingPublish,
                AppCapability::PwaPush,
                AppCapability::MqttPublish,
            ] {
                policy.grants.insert(cap);
            }
            policy.tool_grants = BTreeMap::from([(
                "git-repo-pull".to_string(),
                ResolvedToolGrant {
                    acl: vec![],
                    rate_limit: None,
                },
            )]);
        });
        let db = crate::test_support::init_db_memory();
        let (uid, cid) = {
            let conn = db.lock().await;
            let uid = brenn_db::auth::user::create_user(&conn, "catalog-user", "$argon2id$fake");
            let cid = brenn_db::conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let (tx, _rx) = tokio::sync::broadcast::channel(64);
        let (alert, _h) = brenn_obs::alerting::noop_alert_dispatcher();
        ActiveBridge::inject_for_test_full(
            uid,
            cid,
            "testapp",
            db,
            tx,
            alert,
            TestBridgeConfig {
                apps: Some(apps),
                mounts,
                integrations,
                repo_sync_sender,
                tools: Some(Arc::new(brenn_tool_registry::testutil::git_repo_pull_only())),
                ..Default::default()
            },
        )
    }

    /// Every tool is classified, and the set of commands is exactly this list:
    /// a new tool is classified consciously, not by default.
    #[tokio::test]
    async fn available_tools_are_exactly_the_command_list() {
        let dir = tempfile::tempdir().unwrap();
        let bridge = full_catalog_bridge(dir.path(), None).await;
        let catalog = virtual_tools(&bridge.app_config(), &bridge.tools);
        let mut available: Vec<&str> = catalog
            .iter()
            .filter(|d| d.cli == CliAccess::Available)
            .map(|d| d.name.as_str())
            .collect();
        available.sort_unstable();
        let mut expected = vec![
            "MessageChannelList",
            "MessageSubscriptionList",
            "MessageChannelGet",
            "BrennPendingList",
            "BrennMessageCancel",
            "BrennMessageEdit",
            "BrennSend",
            "MessageSubscribe",
            "MessageUnsubscribe",
            "PwaPushSend",
            "PwaPushChannelGet",
            "MqttSend",
            "AutoCreate",
            "AutoList",
            "AutoEdit",
            "AutoDelete",
            "DeviceList",
            "DeviceGet",
            "DeviceAssignSlug",
            "SetUserTimezone",
            "GitListRepos",
            "GitRepoStatus",
            "GitRepoPull",
        ];
        expected.sort_unstable();
        assert_eq!(available, expected);

        let mut refused: Vec<(&str, CliAccess)> = catalog
            .iter()
            .filter(|d| d.cli != CliAccess::Available)
            .map(|d| (d.name.as_str(), d.cli))
            .collect();
        refused.sort_unstable_by_key(|(n, _)| *n);
        assert_eq!(
            refused,
            vec![
                ("BatchAssign", CliAccess::ConversationBound),
                ("BatchReconcile", CliAccess::ConversationBound),
                ("DisplayFile", CliAccess::ConversationBound),
                ("ExportUsage", CliAccess::RequiresApproval),
                ("GitRepoCommitAndPush", CliAccess::RequiresApproval),
                ("GitRepoRun", CliAccess::RequiresApproval),
                ("ProposeReconciliation", CliAccess::ConversationBound),
                ("RequestCompaction", CliAccess::ConversationBound),
            ]
        );
    }

    #[tokio::test]
    async fn socket_refuses_git_repo_run() {
        let dir = tempfile::tempdir().unwrap();
        let bridge = full_catalog_bridge(dir.path(), None).await;
        let input = json!({"repo": "test-repo", "args": ["status"]});
        assert_eq!(
            admit_tool_call(&bridge, CallOrigin::Socket, "GitRepoRun", &input),
            Err(ExecuteError::NotAvailable(CliAccess::RequiresApproval))
        );
        // The hook path's PreToolUse leg decided; admission only validates.
        assert_eq!(
            admit_tool_call(&bridge, CallOrigin::Hook, "GitRepoRun", &input),
            Ok(())
        );
        assert_eq!(
            admit_tool_call(&bridge, CallOrigin::Hook, "GitRepoRun", &json!({"repo": 1}))
                .unwrap_err()
                .to_wire("GitRepoRun")
                .kind(),
            "invalid_args"
        );
    }

    #[tokio::test]
    async fn socket_refuses_non_command_tools() {
        let (bridge, _e, _b, _a) = test_bridge().await;
        let r = |name| admit_tool_call(&bridge, CallOrigin::Socket, name, &json!({}));
        assert_eq!(
            r("DisplayFile"),
            Err(ExecuteError::NotAvailable(CliAccess::ConversationBound))
        );
        assert_eq!(
            r("RequestCompaction"),
            Err(ExecuteError::NotAvailable(CliAccess::ConversationBound))
        );
        assert_eq!(
            r("ExportUsage"),
            Err(ExecuteError::NotAvailable(CliAccess::RequiresApproval))
        );
    }

    #[tokio::test]
    async fn hook_admits_non_command_tools_and_validates_identically() {
        let (bridge, _e, _b, _a) = test_bridge().await;
        let hook = |input: &serde_json::Value| {
            admit_tool_call(&bridge, CallOrigin::Hook, "DisplayFile", input)
        };
        assert_eq!(hook(&json!({"file_path": "a.md"})), Ok(()));
        let hook_err = hook(&json!({})).unwrap_err();
        assert!(
            matches!(hook_err, ExecuteError::InvalidArgs(_)),
            "{hook_err:?}"
        );

        let socket_err =
            admit_tool_call(&bridge, CallOrigin::Socket, "MessageChannelGet", &json!({}))
                .unwrap_err();
        let hook_err = admit_tool_call(&bridge, CallOrigin::Hook, "MessageChannelGet", &json!({}))
            .unwrap_err();
        assert_eq!(socket_err, hook_err);
        assert!(
            matches!(socket_err, ExecuteError::InvalidArgs(_)),
            "{socket_err:?}"
        );
    }

    #[tokio::test]
    async fn socket_admits_a_valid_command() {
        let (bridge, _e, _b, _a) = test_bridge().await;
        assert_eq!(
            admit_tool_call(
                &bridge,
                CallOrigin::Socket,
                "MessageChannelList",
                &json!({})
            ),
            Ok(())
        );
    }

    #[tokio::test]
    async fn unknown_and_ungranted_names_are_one_answer() {
        let (bridge, _e, _b, _a) = test_bridge().await;
        for origin in [CallOrigin::Hook, CallOrigin::Socket] {
            assert_eq!(
                admit_tool_call(&bridge, origin, "NoSuchTool", &json!({})),
                Err(ExecuteError::UnknownTool)
            );
        }

        // A registered tool the agent holds no grant for is absent from its
        // catalog.
        let tools = std::sync::Arc::new(brenn_tool_registry::testutil::git_repo_pull_only());
        let ungranted = registry_bridge(tools.clone(), BTreeMap::new()).await;
        assert_eq!(
            admit_tool_call(
                &ungranted,
                CallOrigin::Socket,
                "GitRepoPull",
                &json!({"repos": []})
            ),
            Err(ExecuteError::UnknownTool)
        );
        let granted = registry_bridge(
            tools,
            BTreeMap::from([(
                "git-repo-pull".to_string(),
                ResolvedToolGrant {
                    acl: vec![],
                    rate_limit: None,
                },
            )]),
        )
        .await;
        assert_eq!(
            admit_tool_call(
                &granted,
                CallOrigin::Socket,
                "GitRepoPull",
                &json!({"repos": []})
            ),
            Ok(())
        );
    }

    #[test]
    fn error_json_shape() {
        let e = ExecuteError::InvalidArgs("missing required argument(s): [\"to\"]".into());
        assert_eq!(
            e.to_json("BrennSend"),
            json!({
                "error": "invalid_args",
                "detail": "invalid arguments for BrennSend: missing required argument(s): [\"to\"]",
            })
        );
        assert_eq!(
            ExecuteError::NotAvailable(CliAccess::RequiresApproval).to_json("GitRepoRun"),
            json!({"error": "not_available", "detail": "GitRepoRun: requires_approval"})
        );
    }

    #[test]
    fn tool_outcome_answers_cc_with_its_json() {
        let ok = ToolOutcome::Ok(json!({"ok": true}));
        let err = ToolOutcome::Err(json!({"error": "x"}));
        assert!(!ok.is_error());
        assert!(err.is_error());
        match err.into_continue() {
            CcApprovalDecision::Continue {
                updated_output: Some(out),
            } => assert_eq!(out, r#"{"error":"x"}"#),
            other => panic!("expected Continue with output, got {other:?}"),
        }
    }

    #[tokio::test]
    #[should_panic(
        expected = "BUG: tool DisplayFile is in the catalog as `cli = conversation_bound` but no family executes it"
    )]
    async fn execute_admitted_panics_for_a_catalog_tool_no_family_executes() {
        let (bridge, _e, _b, _a) = test_bridge().await;
        let input = json!({"file_path": "a.md"});
        admit_tool_call(&bridge, CallOrigin::Hook, "DisplayFile", &input).unwrap();
        execute_admitted(&bridge, "DisplayFile", &input).await;
    }

    #[tokio::test]
    async fn registry_errors_keep_the_registry_shape() {
        let tools = std::sync::Arc::new(brenn_tool_registry::testutil::git_repo_pull_only());
        let bridge = registry_bridge(
            tools,
            BTreeMap::from([(
                "git-repo-pull".to_string(),
                ResolvedToolGrant {
                    acl: vec![brenn_lib::tools::AclClause::new(BTreeMap::from([(
                        "repo".to_string(),
                        "brenn".to_string(),
                    )]))],
                    rate_limit: None,
                },
            )]),
        )
        .await;
        // Through the socket's entry point, so the registry's own ACL is what
        // refuses a command.
        let call = |repo: &str| {
            execute_tool(
                &bridge,
                CallOrigin::Socket,
                "GitRepoPull",
                json!({"repos": [repo]}),
            )
        };
        match call("secret").await {
            Ok(ToolOutcome::Err(v)) => assert_eq!(v["error_type"], "denied", "{v}"),
            other => panic!("expected a denial, got {other:?}"),
        }
        match call("brenn").await {
            Ok(ToolOutcome::Ok(v)) => assert!(v["repos"].is_array(), "{v}"),
            other => panic!("expected the pull's report, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn messaging_names_dispatch_to_the_messaging_family() {
        let (bridge, _e, _b, _a) = test_bridge().await;
        match execute_admitted(&bridge, "MessageChannelList", &json!({})).await {
            ToolOutcome::Ok(v) => assert!(v["channels"].is_array(), "{v}"),
            other => panic!("expected a listing, got {other:?}"),
        }
    }

    /// Every command has an executor: `dispatch` itself, below admission,
    /// answers each `Available` name. `{}` is refused by each family's own
    /// parsing or is a read, so the probe has no effects. The bridge has no
    /// messenger, automation engine, MQTT client or push service.
    #[tokio::test]
    async fn every_command_has_an_executor() {
        let dir = tempfile::tempdir().unwrap();
        let bridge = full_catalog_bridge(dir.path(), None).await;
        assert!(bridge.messenger().is_none());
        assert!(bridge.automation_engine().is_none());
        assert!(bridge.pwa_push_service().is_none());
        let catalog = virtual_tools(&bridge.app_config(), &bridge.tools);
        let mut probed = 0;
        for def in catalog.iter().filter(|d| d.cli == CliAccess::Available) {
            assert!(
                dispatch(&bridge, &def.name, &json!({})).await.is_some(),
                "{} is a command but no family executes it",
                def.name
            );
            probed += 1;
        }
        assert_eq!(probed, 23);
    }

    /// The tools that are not commands stay hook-only: no family on the
    /// dispatch table executes them.
    #[tokio::test]
    async fn non_commands_are_not_on_the_dispatch_table() {
        let dir = tempfile::tempdir().unwrap();
        let bridge = full_catalog_bridge(dir.path(), None).await;
        let catalog = virtual_tools(&bridge.app_config(), &bridge.tools);
        for def in catalog.iter().filter(|d| d.cli != CliAccess::Available) {
            assert!(
                dispatch(&bridge, &def.name, &json!({})).await.is_none(),
                "{} is not a command but a family executes it",
                def.name
            );
        }
    }

    async fn registry_bridge(
        tools: std::sync::Arc<brenn_tool_registry::ToolRegistry>,
        grants: BTreeMap<String, ResolvedToolGrant>,
    ) -> std::sync::Arc<ActiveBridge> {
        use crate::active_bridge::test_fixtures::TestBridgeConfig;
        let db = crate::test_support::init_db_memory();
        let (uid, cid) = {
            let conn = db.lock().await;
            let uid = brenn_db::auth::user::create_user(&conn, "admit-user", "$argon2id$fake");
            let cid = brenn_db::conversation::create_conversation(&conn, uid, "test", false);
            (uid, cid)
        };
        let (tx, _rx) = tokio::sync::broadcast::channel(16);
        let (alert, _h) = brenn_obs::alerting::noop_alert_dispatcher();
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
}

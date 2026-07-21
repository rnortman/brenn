//! pfin (personal finance) integration for Brenn.
//!
//! Registers pfin MCP tools in the global tool registry with appropriate
//! auto-approve flags and custom approval formatters.

pub mod batch;
pub mod batch_assign;
pub(crate) mod batch_render;
pub(crate) mod card;
mod propose;
mod reconcile;

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use async_trait::async_trait;
use brenn_lib::app::{AppTool, AutoApproveTool};
use brenn_lib::config::AppConfig;
use brenn_lib::integration::{Integration, IntegrationFactory, IntegrationToolAction, ToolPhase};
use brenn_lib::subprocess::SubprocessExecContext;

pub use batch::{BatchReconcileTool, MCP_BATCH_RECONCILE_TOOL};
pub use batch_assign::{BatchAssignTool, MCP_BATCH_ASSIGN_TOOL};
pub use propose::{
    MCP_PROPOSE_RECONCILIATION_TOOL, ProposeReconciliationTool, execute_selection,
    fetch_import_details,
};
pub use reconcile::{MCP_RECONCILE_TOOL, ReconcileTool};

/// Run `pfin --json reconcile --user <username>` with the given reconcile
/// input piped to stdin. Handles both containerized (podman) and bare
/// process execution.
///
/// Returns pfin's stdout on success, or an error description.
pub(crate) async fn run_pfin_reconcile(
    reconcile_input: &serde_json::Value,
    ctx: &SubprocessExecContext<'_>,
    username: &str,
) -> Result<String, String> {
    let stdin_payload = serde_json::to_string(reconcile_input)
        .map_err(|e| format!("failed to serialize reconcile input: {e}"))?;

    let env_pairs: Vec<(&str, &str)> = ctx
        .env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let mut cmd = brenn_lib::subprocess::run_in_app_env(
        ctx.command,
        &["--json", "reconcile", "--user", username],
        ctx.working_dir,
        ctx.container_spawn,
        &env_pairs,
        &["-i"],
    );
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn pfin: {e}"))?;

    {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child
            .stdin
            .take()
            .expect("stdin must be available when Stdio::piped()");
        stdin
            .write_all(stdin_payload.as_bytes())
            .await
            .map_err(|e| format!("failed to write to pfin stdin: {e}"))?;
    }

    let output = child
        .wait_with_output()
        .await
        .map_err(|e| format!("failed to wait for pfin: {e}"))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        Err(format!(
            "pfin reconcile failed (exit {}): stdout={stdout}, stderr={stderr}",
            output.status
        ))
    }
}

/// Run `pfin --json assign <import_id> <assignee_username> [--notes <text>]`.
///
/// Unlike `run_pfin_reconcile`, `pf assign` takes positional args (no stdin
/// payload), so no stdin is piped and no `-i` extra container arg is passed.
///
/// Returns pfin's stdout on success, or an error description.
pub(crate) async fn run_pfin_assign(
    import_id: &str,
    assignee_username: &str,
    notes: Option<&str>,
    ctx: &SubprocessExecContext<'_>,
) -> Result<String, String> {
    let env_pairs: Vec<(&str, &str)> = ctx
        .env
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let mut args: Vec<&str> = vec!["--json", "assign", import_id, assignee_username];
    if let Some(notes_text) = notes {
        args.push("--notes");
        args.push(notes_text);
    }

    let mut cmd = brenn_lib::subprocess::run_in_app_env(
        ctx.command,
        &args,
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
        .map_err(|e| format!("failed to spawn pfin: {e}"))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        Err(format!(
            "pfin assign failed (exit {}): stdout={stdout}, stderr={stderr}",
            output.status
        ))
    }
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

/// Integration name, used in TOML config keys and the integration registry.
pub const INTEGRATION_NAME: &str = "pfin";

/// Config for the pfin integration, deserialized from the merged
/// `[integrations.pfin]` + per-app `[integration_config.pfin]` TOML.
#[derive(Debug, Clone, Deserialize)]
pub struct PfinConfig {
    /// pfin binary — bare name (PATH) or absolute path.
    pub command: String,
    /// Environment for one-shot pfin subprocess invocations.
    #[serde(default)]
    pub env: HashMap<String, String>,
}

/// Factory for the pfin integration.
pub struct PfinFactory;

impl IntegrationFactory for PfinFactory {
    fn name(&self) -> &str {
        INTEGRATION_NAME
    }

    fn create(&self, config: Option<&toml::Value>) -> Arc<dyn Integration> {
        let config: PfinConfig = config
            .expect("pfin integration requires [integrations.pfin] config with command")
            .clone()
            .try_into()
            .expect("invalid pfin integration config");
        Arc::new(PfinIntegration { config })
    }

    fn tools(&self) -> Vec<Box<dyn AppTool>> {
        pfin_tools()
    }
}

pub struct PfinIntegration {
    pub config: PfinConfig,
}

#[async_trait]
impl Integration for PfinIntegration {
    fn name(&self) -> &str {
        INTEGRATION_NAME
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn virtual_tools(&self) -> Vec<brenn_lib::integration::VirtualToolDef> {
        pfin_virtual_tools()
    }

    /// Intercept pfin virtual tool Pre/PostToolUse events.
    ///
    /// Pre: always grant permission (skip CC's Permission prompt).
    /// Post BatchAssign: validate `user` before enrichment.
    /// Post others: proceed to brenn enrichment orchestration.
    async fn intercept_tool(
        &self,
        phase: ToolPhase,
        tool_name: &str,
        tool_input: &serde_json::Value,
    ) -> Option<IntegrationToolAction> {
        let is_pfin_tool = tool_name == MCP_PROPOSE_RECONCILIATION_TOOL
            || tool_name == MCP_BATCH_RECONCILE_TOOL
            || tool_name == MCP_BATCH_ASSIGN_TOOL;

        if !is_pfin_tool {
            return None;
        }

        match phase {
            ToolPhase::Pre => Some(IntegrationToolAction::GrantPermission),
            ToolPhase::Post => {
                if tool_name == MCP_BATCH_ASSIGN_TOOL {
                    // Validate `user` before enrichment to avoid spawning N pf show
                    // subprocesses for an input that will fail anyway.
                    match tool_input.get("user").and_then(|v| v.as_str()) {
                        Some(u) if !u.is_empty() => Some(IntegrationToolAction::Proceed),
                        Some(_) => Some(IntegrationToolAction::Reject {
                            message: "user must be a non-empty string".to_string(),
                        }),
                        None => Some(IntegrationToolAction::Reject {
                            message: "missing user in tool input".to_string(),
                        }),
                    }
                } else {
                    Some(IntegrationToolAction::Proceed)
                }
            }
        }
    }
}

/// Extract `PfinConfig` from an app's integration map.
///
/// Returns `None` if the app doesn't have the pfin integration enabled.
/// The WS handler calls this to get config for subprocess invocations.
pub fn pfin_config(app: &AppConfig) -> Option<&PfinConfig> {
    app.integrations
        .get(INTEGRATION_NAME)
        .and_then(|i| i.as_any().downcast_ref::<PfinIntegration>())
        .map(|p| &p.config)
}

/// Transaction JSON schema, shared between ProposeReconciliation and BatchReconcile.
fn transaction_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "description": "Full transaction to reconcile.",
        "properties": {
            "id": {
                "type": "string",
                "description": "Existing transaction ID to update. Omit to create a new transaction."
            },
            "date": {"type": "string", "description": "YYYY-MM-DD"},
            "description": {"type": "string"},
            "notes": {"type": "string"},
            "splits": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "account": {"type": "string"},
                        "amount": {"type": "string"},
                        "memo": {"type": "string"}
                    },
                    "required": ["account", "amount"]
                }
            }
        },
        "required": ["splits"]
    })
}

/// Virtual tool schemas for the pfin integration.
fn pfin_virtual_tools() -> Vec<brenn_lib::integration::VirtualToolDef> {
    use brenn_lib::integration::VirtualToolDef;

    let txn = transaction_schema();

    vec![
        VirtualToolDef {
            name: "ProposeReconciliation".to_string(),
            description: concat!(
                "Present multiple reconciliation proposals to the user for selection. ",
                "Use this when you have 2-5 plausible ways to categorize or reconcile ",
                "an imported transaction and want the user to choose. Each proposal ",
                "includes a label and a full transaction object. The user selects one ",
                "proposal (or denies all), and the selected transaction is automatically ",
                "reconciled \u{2014} you do NOT need to call reconcile separately."
            )
            .to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "import_id": {
                        "type": "string",
                        "description": "The pending import ID to reconcile."
                    },
                    "proposals": {
                        "type": "array",
                        "description": "2-5 reconciliation proposals for the user to choose from.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "label": {
                                    "type": "string",
                                    "description": "Short human-readable label for this option."
                                },
                                "transaction": txn.clone()
                            },
                            "required": ["label", "transaction"]
                        },
                        "minItems": 2,
                        "maxItems": 5
                    }
                },
                "required": ["import_id", "proposals"]
            }),
        },
        VirtualToolDef {
            name: "BatchReconcile".to_string(),
            description: concat!(
                "Present a batch of high-confidence reconciliation proposals for ",
                "the user to accept or reject individually. Each item pairs a ",
                "pending import with a single proposed transaction. Use this when ",
                "you're confident about most categorizations and want the user to ",
                "quickly confirm a batch rather than reviewing one at a time. ",
                "Accepted items are automatically reconciled \u{2014} you do NOT need to ",
                "call reconcile separately."
            )
            .to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "description": "Batch of reconciliation proposals.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "import_id": {
                                    "type": "string",
                                    "description": "The pending import ID to reconcile."
                                },
                                "transaction": txn,
                                "info": {
                                    "type": "string",
                                    "description": "Optional short note explaining your categorization rationale."
                                }
                            },
                            "required": ["import_id", "transaction"]
                        },
                        "minItems": 1,
                        "maxItems": 50
                    }
                },
                "required": ["items"]
            }),
        },
        VirtualToolDef {
            name: "BatchAssign".to_string(),
            description: concat!(
                "Present a batch of import-assignment proposals to the user for ",
                "accept/reject. All items are assigned to a single username. Use ",
                "this when many imports should go to the same person (e.g. inbox ",
                "triage). Accepted items are assigned via pfin \u{2014} you do NOT need ",
                "to call assign separately. Optional per-item `notes` is appended ",
                "to the import's existing notes. Rejected items are reported back ",
                "unchanged. Reassigning an already-assigned import is allowed and ",
                "overwrites the previous assignee."
            )
            .to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "user": {
                        "type": "string",
                        "description": "Username to assign all items to."
                    },
                    "items": {
                        "type": "array",
                        "description": "Pending imports to assign to `user`.",
                        "minItems": 1,
                        "maxItems": 50,
                        "items": {
                            "type": "object",
                            "properties": {
                                "import_id": {
                                    "type": "string",
                                    "description": "The pending import ID to assign."
                                },
                                "notes": {
                                    "type": "string",
                                    "description": "Optional note to append to the import (--notes)."
                                },
                                "info": {
                                    "type": "string",
                                    "description": "Optional short rationale for the assignment, shown to the user but NOT persisted to pfin."
                                }
                            },
                            "required": ["import_id"]
                        }
                    }
                },
                "required": ["user", "items"]
            }),
        },
    ]
}

/// Collect all pfin tools for the tool registry.
fn pfin_tools() -> Vec<Box<dyn AppTool>> {
    let mut tools: Vec<Box<dyn AppTool>> = vec![
        Box::new(ReconcileTool),
        Box::new(ProposeReconciliationTool),
        Box::new(BatchReconcileTool),
        Box::new(BatchAssignTool),
    ];

    // Read-only tools: auto-approved, no custom formatting.
    for name in READ_ONLY_TOOLS {
        tools.push(Box::new(AutoApproveTool(name)));
    }

    tools
}

/// pfin read-only MCP tools. Auto-approved because they can't mutate data.
///
/// `query` runs raw SQL but pfin enforces read-only at the SQLite connection
/// level (`SQLITE_OPEN_READ_ONLY`), so it's safe to auto-approve.
const READ_ONLY_TOOLS: &[&str] = &[
    "mcp__pfin__accounts",
    "mcp__pfin__pending",
    "mcp__pfin__search",
    "mcp__pfin__balance",
    "mcp__pfin__show",
    "mcp__pfin__context",
    "mcp__pfin__query",
    "mcp__pfin__email_search",
    "mcp__pfin__email_get",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pfin_factory_name() {
        assert_eq!(PfinFactory.name(), "pfin");
    }

    #[test]
    fn pfin_factory_tools_count() {
        let tools = PfinFactory.tools();
        // 1 reconcile + 1 propose + 1 batch + 1 batch_assign + 9 read-only
        assert_eq!(tools.len(), 13, "expected 13 tools, got {}", tools.len());
    }

    #[test]
    fn pfin_tools_are_namespaced() {
        let tools = PfinFactory.tools();
        for tool in &tools {
            assert!(
                tool.name().starts_with("mcp__pfin__") || tool.name().starts_with("mcp__brenn__"),
                "tool name should be MCP-namespaced: {}",
                tool.name()
            );
        }
    }

    #[test]
    fn read_only_tools_auto_approve() {
        let tools = PfinFactory.tools();
        let write_tools = [
            MCP_RECONCILE_TOOL,
            MCP_PROPOSE_RECONCILIATION_TOOL,
            MCP_BATCH_RECONCILE_TOOL,
            MCP_BATCH_ASSIGN_TOOL,
        ];
        for tool in &tools {
            if !write_tools.contains(&tool.name()) {
                assert!(
                    tool.auto_approve(),
                    "{} should be auto-approved",
                    tool.name()
                );
            }
        }
    }

    #[test]
    fn write_tools_do_not_auto_approve() {
        let tools = PfinFactory.tools();
        for name in [
            MCP_RECONCILE_TOOL,
            MCP_PROPOSE_RECONCILIATION_TOOL,
            MCP_BATCH_RECONCILE_TOOL,
            MCP_BATCH_ASSIGN_TOOL,
        ] {
            let tool = tools.iter().find(|t| t.name() == name);
            assert!(tool.is_some(), "{name} should be registered");
            assert!(
                !tool.unwrap().auto_approve(),
                "{name} should NOT be auto-approved"
            );
        }
    }

    fn make_integration() -> Arc<dyn Integration> {
        let toml_val: toml::Value = toml::from_str(r#"command = "pf""#).unwrap();
        PfinFactory.create(Some(&toml_val))
    }

    #[test]
    fn pfin_integration_provides_virtual_tools() {
        let integration = make_integration();
        let vtools = integration.virtual_tools();
        assert_eq!(vtools.len(), 3);
        assert_eq!(vtools[0].name, "ProposeReconciliation");
        assert_eq!(vtools[1].name, "BatchReconcile");
        assert_eq!(vtools[2].name, "BatchAssign");
    }

    #[test]
    fn pfin_config_deserializes_command_only() {
        let toml_val: toml::Value = toml::from_str(r#"command = "pf""#).unwrap();
        let config: PfinConfig = toml_val.try_into().unwrap();
        assert_eq!(config.command, "pf");
        assert!(config.env.is_empty());
    }

    #[test]
    fn pfin_config_deserializes_with_env() {
        let toml_val: toml::Value = toml::from_str(
            r#"
            command = "pf"
            [env]
            PFIN_DATA = "/data/pfin"
            PFIN_ENV_FILE = "/etc/pfin.env"
            "#,
        )
        .unwrap();
        let config: PfinConfig = toml_val.try_into().unwrap();
        assert_eq!(config.command, "pf");
        assert_eq!(
            config.env.get("PFIN_DATA").map(|s| s.as_str()),
            Some("/data/pfin")
        );
    }

    #[test]
    #[should_panic(expected = "pfin integration requires")]
    fn create_panics_without_config() {
        PfinFactory.create(None);
    }

    #[test]
    #[should_panic(expected = "invalid pfin integration config")]
    fn create_panics_on_invalid_config() {
        // Missing required `command` field.
        let toml_val: toml::Value = toml::from_str(r#"foo = "bar""#).unwrap();
        PfinFactory.create(Some(&toml_val));
    }

    #[test]
    fn as_any_downcast_works() {
        let integration = make_integration();
        let pfin = integration
            .as_any()
            .downcast_ref::<PfinIntegration>()
            .expect("downcast to PfinIntegration should succeed");
        assert_eq!(pfin.config.command, "pf");
    }

    #[test]
    fn as_any_downcast_fails_for_wrong_type() {
        let integration = make_integration();
        assert!(
            integration.as_any().downcast_ref::<String>().is_none(),
            "downcast to wrong type should return None"
        );
    }

    #[test]
    fn pfin_config_helper_returns_none_when_absent() {
        // Construct a minimal AppConfig with no pfin integration.
        let tmp = std::env::temp_dir().join("brenn-pfin-test");
        let state_dir = tmp.join(".brenn-state");
        let app = brenn_lib::config::AppConfig {
            slug: "test".into(),
            name: "test".into(),
            description: String::new(),
            icon: String::new(),
            working_dir: tmp.clone(),
            model: "sonnet".into(),
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout: None,
            compaction: None,
            idle_hook_secs: 0,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: std::collections::HashMap::new(),
            multiuser: false,
            prefix_username: false,
            prefix_timestamp: false,
            prefix_device: true,
            path_mapper: brenn_lib::config::PathMapper::Identity,
            container_spawn: None,
            start_hooks: brenn_lib::config::StartHooksConfig::default(),
            post_pull_hooks: brenn_lib::config::PostPullHooksConfig::default(),
            startup_hooks: brenn_lib::config::StartupHooksConfig::default(),
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: std::collections::HashMap::new(),
            mounts: vec![],
            history_replay_limit: 2000,
            frontmatter: brenn_lib::config::FrontmatterRenderConfig::default(),
            state_dir,
            messaging: None,
            messaging_default_send_budget: 100,
            policy: brenn_lib::access::AppPolicy::default(),
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
        };
        assert!(pfin_config(&app).is_none());
    }

    /// Return a validated `PathBuf` for `runtime_dir` in tests that call
    /// `validate_and_resolve` with a bare app. No env mutation — see design.
    fn test_runtime_dir() -> &'static std::path::PathBuf {
        brenn_lib::runtime_dir::test_runtime_dir_once()
    }

    /// End-to-end test: resolve a TOML config through `validate_and_resolve`
    /// with `PfinFactory` registered, then assert `pfin_config` returns `Some`
    /// with the correct values. This covers the resolve.rs → factory →
    /// `AppConfig.integrations` → downcast chain, which the factory-unit tests
    /// do not exercise.
    #[test]
    fn pfin_config_survives_resolve_chain_global_only() {
        use brenn_lib::config::{BrennConfig, validate_and_resolve};
        use brenn_lib::integration::IntegrationRegistry;

        let dir = tempfile::tempdir().unwrap();
        let registry = IntegrationRegistry::new(vec![Box::new(PfinFactory)]);
        let config: BrennConfig = toml::from_str(&format!(
            r#"
[integrations.pfin]
command = "pf"

[[app]]
slug = "myapp"
working_dir = "{}"
integrations = ["pfin"]
"#,
            dir.path().display(),
        ))
        .unwrap();
        let resolved = validate_and_resolve(&config, &registry, Some(test_runtime_dir()));
        let app = resolved.apps.get("myapp").unwrap();
        let cfg = pfin_config(app).expect("pfin_config should return Some after resolve");
        assert_eq!(cfg.command, "pf");
        assert!(
            cfg.env.is_empty(),
            "env should be empty when not configured"
        );
    }

    /// Same as above but with a per-app `[integration_config.pfin]` env stanza,
    /// verifying that the global+per-app merge produces the expected merged config.
    #[test]
    fn pfin_config_survives_resolve_chain_with_per_app_env() {
        use brenn_lib::config::{BrennConfig, validate_and_resolve};
        use brenn_lib::integration::IntegrationRegistry;

        let dir = tempfile::tempdir().unwrap();
        let registry = IntegrationRegistry::new(vec![Box::new(PfinFactory)]);
        let config: BrennConfig = toml::from_str(&format!(
            r#"
[integrations.pfin]
command = "pf"

[[app]]
slug = "myapp"
working_dir = "{}"
integrations = ["pfin"]

[app.integration_config.pfin]
env = {{ PFIN_DATA = "/data/pfin", PFIN_ENV_FILE = "/etc/pfin.env" }}
"#,
            dir.path().display()
        ))
        .unwrap();
        let resolved = validate_and_resolve(&config, &registry, Some(test_runtime_dir()));
        let app = resolved.apps.get("myapp").unwrap();
        let cfg = pfin_config(app).expect("pfin_config should return Some after resolve");
        assert_eq!(cfg.command, "pf");
        assert_eq!(
            cfg.env.get("PFIN_DATA").map(String::as_str),
            Some("/data/pfin")
        );
        assert_eq!(
            cfg.env.get("PFIN_ENV_FILE").map(String::as_str),
            Some("/etc/pfin.env")
        );
    }

    /// App opts in via `integrations = ["pfin"]` but no global `[integrations.pfin]`
    /// stanza is present. `validate_and_resolve` should panic at startup, not
    /// silently drop the integration.
    #[test]
    #[should_panic(expected = "pfin integration requires")]
    fn pfin_enabled_without_global_stanza_panics_at_startup() {
        use brenn_lib::config::{BrennConfig, validate_and_resolve};
        use brenn_lib::integration::IntegrationRegistry;

        let dir = tempfile::tempdir().unwrap();
        let registry = IntegrationRegistry::new(vec![Box::new(PfinFactory)]);
        // No [integrations.pfin] block — factory.create() should panic.
        let config: BrennConfig = toml::from_str(&format!(
            r#"
[[app]]
slug = "myapp"
working_dir = "{}"
integrations = ["pfin"]
"#,
            dir.path().display()
        ))
        .unwrap();
        // pfin factory panics before state_dir resolution; None is fine.
        let _ = validate_and_resolve(&config, &registry, None);
    }

    // -----------------------------------------------------------------------
    // intercept_tool unit tests — pure decision logic, no bridge coupling.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn intercept_tool_pre_propose_grants_permission() {
        let integration = make_integration();
        let action = integration
            .intercept_tool(
                ToolPhase::Pre,
                MCP_PROPOSE_RECONCILIATION_TOOL,
                &serde_json::json!({}),
            )
            .await;
        assert!(
            matches!(action, Some(IntegrationToolAction::GrantPermission)),
            "Pre ProposeReconciliation should GrantPermission, got {action:?}"
        );
    }

    #[tokio::test]
    async fn intercept_tool_pre_batch_reconcile_grants_permission() {
        let integration = make_integration();
        let action = integration
            .intercept_tool(
                ToolPhase::Pre,
                MCP_BATCH_RECONCILE_TOOL,
                &serde_json::json!({}),
            )
            .await;
        assert!(
            matches!(action, Some(IntegrationToolAction::GrantPermission)),
            "Pre BatchReconcile should GrantPermission, got {action:?}"
        );
    }

    #[tokio::test]
    async fn intercept_tool_pre_batch_assign_grants_permission() {
        let integration = make_integration();
        let action = integration
            .intercept_tool(
                ToolPhase::Pre,
                MCP_BATCH_ASSIGN_TOOL,
                &serde_json::json!({ "user": "wonder" }),
            )
            .await;
        assert!(
            matches!(action, Some(IntegrationToolAction::GrantPermission)),
            "Pre BatchAssign should GrantPermission, got {action:?}"
        );
    }

    #[tokio::test]
    async fn intercept_tool_post_propose_proceeds() {
        let integration = make_integration();
        let action = integration
            .intercept_tool(
                ToolPhase::Post,
                MCP_PROPOSE_RECONCILIATION_TOOL,
                &serde_json::json!({ "import_id": "imp-1", "proposals": [] }),
            )
            .await;
        assert!(
            matches!(action, Some(IntegrationToolAction::Proceed)),
            "Post ProposeReconciliation should Proceed, got {action:?}"
        );
    }

    #[tokio::test]
    async fn intercept_tool_post_batch_reconcile_proceeds() {
        let integration = make_integration();
        let action = integration
            .intercept_tool(
                ToolPhase::Post,
                MCP_BATCH_RECONCILE_TOOL,
                &serde_json::json!({ "items": [] }),
            )
            .await;
        assert!(
            matches!(action, Some(IntegrationToolAction::Proceed)),
            "Post BatchReconcile should Proceed, got {action:?}"
        );
    }

    #[tokio::test]
    async fn intercept_tool_post_batch_assign_valid_user_proceeds() {
        let integration = make_integration();
        let action = integration
            .intercept_tool(
                ToolPhase::Post,
                MCP_BATCH_ASSIGN_TOOL,
                &serde_json::json!({ "user": "wonder", "items": [] }),
            )
            .await;
        assert!(
            matches!(action, Some(IntegrationToolAction::Proceed)),
            "Post BatchAssign with valid user should Proceed, got {action:?}"
        );
    }

    #[tokio::test]
    async fn intercept_tool_post_batch_assign_missing_user_rejects() {
        let integration = make_integration();
        let action = integration
            .intercept_tool(
                ToolPhase::Post,
                MCP_BATCH_ASSIGN_TOOL,
                &serde_json::json!({ "items": [] }),
            )
            .await;
        match action {
            Some(IntegrationToolAction::Reject { message }) => {
                assert!(
                    message.contains("missing user"),
                    "should mention missing user: {message}"
                );
            }
            other => panic!("Post BatchAssign with missing user should Reject, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn intercept_tool_post_batch_assign_empty_user_rejects() {
        let integration = make_integration();
        let action = integration
            .intercept_tool(
                ToolPhase::Post,
                MCP_BATCH_ASSIGN_TOOL,
                &serde_json::json!({ "user": "", "items": [] }),
            )
            .await;
        match action {
            Some(IntegrationToolAction::Reject { message }) => {
                assert!(
                    message.contains("non-empty"),
                    "should mention non-empty requirement: {message}"
                );
            }
            other => panic!("Post BatchAssign with empty user should Reject, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn intercept_tool_non_pfin_tool_returns_none() {
        let integration = make_integration();
        let action = integration
            .intercept_tool(
                ToolPhase::Pre,
                "mcp__brenn__SomeOtherTool",
                &serde_json::json!({}),
            )
            .await;
        assert!(
            action.is_none(),
            "non-pfin tool should return None, got {action:?}"
        );
    }
}

use super::*;
use crate::config::ResolvedConfig;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// App config parsing
// -----------------------------------------------------------------------

#[test]
fn app_config_parses_minimal() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"
"#,
        dir.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    assert_eq!(config.apps.len(), 1);
    assert_eq!(config.apps[0].slug, "pfin");
    assert!(config.apps[0].name.is_none());
    assert!(config.apps[0].model.is_none());
    assert!(!config.apps[0].single_instance);
    assert!(config.apps[0].allowed_users.is_empty());
    assert!(config.apps[0].disabled_tools.is_empty());
}

#[test]
fn app_config_parses_full() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
name = "Personal Finance"
working_dir = "{}"
model = "opus"
single_instance = true
allowed_users = ["alice", "bob"]
disabled_tools = ["Edit", "Write"]
"#,
        dir.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    assert_eq!(config.apps[0].name.as_deref(), Some("Personal Finance"));
    assert_eq!(config.apps[0].model.as_deref(), Some("opus"));
    assert!(config.apps[0].single_instance);
    assert_eq!(config.apps[0].allowed_users, vec!["alice", "bob"]);
    assert_eq!(config.apps[0].disabled_tools, vec!["Edit", "Write"]);
}

/// Migration-forcing (access-control design §2.5.1 / §6.6 / §8 decision-2):
/// a config that still sets the removed `[app.messaging].enabled` authorization
/// boolean must FAIL to parse with a precise error naming the field — forcing the
/// operator to migrate to the explicit `grants` surface rather than silently
/// parsing into a deny-everything policy. End-to-end through the full `BrennConfig`
/// TOML parse (not just the `MessagingConfigRaw` unit), so the property holds for
/// real operator config (e.g. out-of-tree deployments).
#[test]
fn removed_messaging_enabled_field_fails_to_parse() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"

[app.messaging]
enabled = true
"#,
        dir.path().display()
    );
    let result: Result<BrennConfig, _> = toml::from_str(&toml);
    let err = result.expect_err("removed [app.messaging].enabled must fail to parse");
    let err_str = err.to_string();
    assert!(
        err_str.contains("enabled"),
        "error must name the offending `enabled` field; got: {err_str}"
    );
}

/// Migration-forcing counterpart for the removed `[app.pwa_push].enabled`
/// authorization boolean (access-control §2.5.1 / §6.6 / §8 decision-2).
#[test]
fn removed_pwa_push_enabled_field_fails_to_parse() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"

[app.pwa_push]
enabled = true
"#,
        dir.path().display()
    );
    let result: Result<BrennConfig, _> = toml::from_str(&toml);
    let err = result.expect_err("removed [app.pwa_push].enabled must fail to parse");
    let err_str = err.to_string();
    assert!(
        err_str.contains("enabled"),
        "error must name the offending `enabled` field; got: {err_str}"
    );
}

#[test]
fn multiple_apps_parse() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"

[[app]]
slug = "graf"
working_dir = "{}"
model = "opus"
"#,
        dir1.path().display(),
        dir2.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    assert_eq!(config.apps.len(), 2);
    assert_eq!(config.apps[0].slug, "pfin");
    assert_eq!(config.apps[1].slug, "graf");
}

#[test]
fn validate_preserves_declared_app_order() {
    // Reverse-alphabetical slugs to distinguish declared order from
    // sort order; six entries so a HashMap regression is unlikely to
    // land in declared order by coincidence.
    let slugs = ["zeta", "mike", "alpha", "delta", "papa", "bravo"];
    let dir = tempfile::tempdir().unwrap();

    let apps_raw: Vec<AppConfigRaw> = slugs
        .iter()
        .map(|s| AppConfigRaw {
            slug: (*s).to_string(),
            ..app_raw_with_targets(dir.path(), vec![])
        })
        .collect();

    let config = BrennConfig {
        apps: apps_raw,
        ..Default::default()
    };

    let ResolvedConfig { apps: resolved, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let observed: Vec<&str> = resolved.keys().map(String::as_str).collect();
    assert_eq!(observed, slugs);
}

/// `resolve_messaging_layer` populates `AppConfig::messaging` only
/// for apps that have a `[app.messaging]` block. Apps without one
/// keep `messaging: None` (review F23).
#[test]
fn validate_resolves_messaging_layer_for_apps_with_messaging_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let mut sender_app = app_raw_with_targets(dir.path(), vec![]);
    sender_app.slug = "sender".to_string();
    sender_app.singleton = true;
    sender_app.allowed_users = vec!["alice".to_string()];
    sender_app.compact_soft_pct = Some(75);
    sender_app.messaging = Some(crate::messaging::config::MessagingConfigRaw {
        subscribe: vec![],
        send_budget: None,
    });

    let mut quiet_app = app_raw_with_targets(dir.path(), vec![]);
    quiet_app.slug = "quiet".to_string();
    // No `messaging` block.

    let channel = crate::messaging::config::ChannelConfigRaw {
        uuid: "1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32".to_string(),
        address: "ch".to_string(),
        description: None,
        push_depth: None,
        retain_depth: None,
        standing_retain_depth: None,
        noise: None,
        sink: None,
        wake_min: None,
    };

    let config = BrennConfig {
        apps: vec![sender_app, quiet_app],
        channels: vec![channel],
        ..Default::default()
    };

    let ResolvedConfig { apps: resolved, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    assert!(
        resolved["sender"].messaging.is_some(),
        "sender app has [app.messaging] → resolved field should be Some"
    );
    assert!(
        resolved["quiet"].messaging.is_none(),
        "quiet app has no [app.messaging] → resolved field should be None"
    );
    // Both apps see the global default budget regardless.
    assert_eq!(resolved["sender"].messaging_default_send_budget, 100);
    assert_eq!(resolved["quiet"].messaging_default_send_budget, 100);
}

#[test]
fn app_config_unknown_field_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"
bogus = true
"#,
        dir.path().display()
    );
    assert!(toml::from_str::<BrennConfig>(&toml).is_err());
}

#[test]
fn app_config_missing_slug_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
working_dir = "{}"
"#,
        dir.path().display()
    );
    assert!(toml::from_str::<BrennConfig>(&toml).is_err());
}

#[test]
fn app_config_missing_working_dir_parses_ok() {
    // working_dir is now Option<PathBuf> — missing is valid at parse time.
    // Validation happens in validate_and_resolve (which checks that either
    // working_dir or a mount with working_dir=true is present).
    let toml = r#"
[[app]]
slug = "pfin"
"#;
    let config = toml::from_str::<BrennConfig>(toml).unwrap();
    assert!(config.apps[0].working_dir.is_none());
}

#[test]
fn validate_mcp_servers_carried_through() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            mcp_servers: HashMap::from([(
                "custom".to_string(),
                McpServerConfig {
                    command: "node".to_string(),
                    args: vec!["server.js".to_string()],
                    env: HashMap::new(),
                },
            )]),
            ..Default::default()
        }],
        ..Default::default()
    };
    let ResolvedConfig { apps, .. } = validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
    let app = &apps["pfin"];
    assert_eq!(app.mcp_servers.len(), 1);
    assert_eq!(app.mcp_servers["custom"].command, "node");
}

#[test]
fn multi_app_mcp_servers_parsed_correctly() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"

[app.mcp_servers.finance-tool]
command = "python3"
args = ["finance.py"]

[[app]]
slug = "graf"
working_dir = "{}"
"#,
        dir1.path().display(),
        dir2.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    assert_eq!(config.apps.len(), 2);

    // First app has the MCP server.
    assert_eq!(config.apps[0].slug, "pfin");
    assert_eq!(config.apps[0].mcp_servers.len(), 1);
    assert_eq!(
        config.apps[0].mcp_servers["finance-tool"].command,
        "python3"
    );

    // Second app has no MCP servers.
    assert_eq!(config.apps[1].slug, "graf");
    assert!(config.apps[1].mcp_servers.is_empty());
}

#[test]
fn multi_app_each_with_different_mcp_servers() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"

[app.mcp_servers.finance-tool]
command = "python3"
args = ["finance.py"]

[[app]]
slug = "graf"
working_dir = "{}"

[app.mcp_servers.graph-tool]
command = "node"
args = ["graph.js"]
env = {{ DB_URL = "postgres://localhost/graf" }}
"#,
        dir1.path().display(),
        dir2.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    assert_eq!(config.apps.len(), 2);

    // First app: finance-tool, no graph-tool.
    assert_eq!(config.apps[0].mcp_servers.len(), 1);
    assert!(config.apps[0].mcp_servers.contains_key("finance-tool"));
    assert!(!config.apps[0].mcp_servers.contains_key("graph-tool"));

    // Second app: graph-tool, no finance-tool.
    assert_eq!(config.apps[1].mcp_servers.len(), 1);
    assert!(config.apps[1].mcp_servers.contains_key("graph-tool"));
    assert!(!config.apps[1].mcp_servers.contains_key("finance-tool"));
    assert_eq!(
        config.apps[1].mcp_servers["graph-tool"]
            .env
            .get("DB_URL")
            .unwrap(),
        "postgres://localhost/graf"
    );
}

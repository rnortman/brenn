use super::*;
use crate::app::AppTool;
use crate::config::ResolvedConfig;
use crate::integration::{Integration, IntegrationFactory, IntegrationRegistry};
use std::collections::HashMap;
use std::sync::Arc;

// -----------------------------------------------------------------------
// Integration resolution
// -----------------------------------------------------------------------

struct TestFactory;

impl IntegrationFactory for TestFactory {
    fn name(&self) -> &str {
        "test-int"
    }

    fn create(&self, config: Option<&toml::Value>) -> Arc<dyn Integration> {
        let command = config
            .and_then(|c| c.get("command"))
            .and_then(|v| v.as_str())
            .unwrap_or("default-cmd")
            .to_string();
        Arc::new(TestIntegration { command })
    }

    fn tools(&self) -> Vec<Box<dyn AppTool>> {
        vec![]
    }
}

struct TestIntegration {
    command: String,
}

impl Integration for TestIntegration {
    fn name(&self) -> &str {
        "test-int"
    }

    fn mcp_servers(&self) -> Vec<(String, McpServerConfig)> {
        vec![(
            "test-mcp".to_string(),
            McpServerConfig {
                command: self.command.clone(),
                args: vec![],
                env: HashMap::new(),
            },
        )]
    }
}

#[test]
fn integration_enabled_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let registry = IntegrationRegistry::new(vec![Box::new(TestFactory)]);
    let config = BrennConfig {
        integrations: HashMap::from([(
            "test-int".to_string(),
            toml::Value::Table(toml::map::Map::from_iter([(
                "command".to_string(),
                toml::Value::String("my-cmd".to_string()),
            )])),
        )]),
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec!["test-int".to_string()],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    let ResolvedConfig { apps, .. } =
        validate_and_resolve(&config, &registry, Some(super::test_runtime_dir()));
    let app = apps.get("myapp").unwrap();
    assert!(app.integrations.contains_key("test-int"));

    // Integration should contribute its MCP server.
    let mcp_servers = app.integrations["test-int"].mcp_servers();
    assert_eq!(mcp_servers.len(), 1);
    assert_eq!(mcp_servers[0].0, "test-mcp");
    assert_eq!(mcp_servers[0].1.command, "my-cmd");
}

#[test]
fn integration_per_app_override() {
    let dir = tempfile::tempdir().unwrap();
    let registry = IntegrationRegistry::new(vec![Box::new(TestFactory)]);
    let config = BrennConfig {
        integrations: HashMap::from([(
            "test-int".to_string(),
            toml::Value::Table(toml::map::Map::from_iter([(
                "command".to_string(),
                toml::Value::String("global-cmd".to_string()),
            )])),
        )]),
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec![],
            // Per-app override implicitly enables the integration.
            integration_config: HashMap::from([(
                "test-int".to_string(),
                toml::Value::Table(toml::map::Map::from_iter([(
                    "command".to_string(),
                    toml::Value::String("overridden-cmd".to_string()),
                )])),
            )]),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    let ResolvedConfig { apps, .. } =
        validate_and_resolve(&config, &registry, Some(super::test_runtime_dir()));
    let app = apps.get("myapp").unwrap();
    let mcp_servers = app.integrations["test-int"].mcp_servers();
    // Per-app override should take effect.
    assert_eq!(mcp_servers[0].1.command, "overridden-cmd");
}

#[test]
#[should_panic(expected = "not registered")]
fn integration_unknown_name_panics() {
    let dir = tempfile::tempdir().unwrap();
    let registry = IntegrationRegistry::new(vec![]);
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec!["nonexistent".to_string()],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &registry, Some(super::test_runtime_dir()));
}

#[test]
#[should_panic(expected = "collides")]
fn integration_mcp_server_collision_panics() {
    let dir = tempfile::tempdir().unwrap();
    let registry = IntegrationRegistry::new(vec![Box::new(TestFactory)]);
    let config = BrennConfig {
        integrations: HashMap::from([(
            "test-int".to_string(),
            toml::Value::Table(toml::map::Map::from_iter([(
                "command".to_string(),
                toml::Value::String("cmd".to_string()),
            )])),
        )]),
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            // Collision: explicit mcp_server has same name as integration-contributed.
            mcp_servers: HashMap::from([(
                "test-mcp".to_string(),
                McpServerConfig {
                    command: "other".to_string(),
                    args: vec![],
                    env: HashMap::new(),
                },
            )]),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec!["test-int".to_string()],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &registry, Some(super::test_runtime_dir()));
}

// -----------------------------------------------------------------------
// shallow_merge_toml
// -----------------------------------------------------------------------

#[test]
fn shallow_merge_overrides_matching_keys() {
    let global = toml::Value::Table(toml::map::Map::from_iter([
        ("a".into(), toml::Value::String("global_a".into())),
        ("b".into(), toml::Value::String("global_b".into())),
    ]));
    let per_app = toml::Value::Table(toml::map::Map::from_iter([(
        "a".into(),
        toml::Value::String("override_a".into()),
    )]));
    let merged = shallow_merge_toml(&global, &per_app, "test", "int");
    let table = merged.as_table().unwrap();
    assert_eq!(table["a"].as_str().unwrap(), "override_a");
    assert_eq!(table["b"].as_str().unwrap(), "global_b");
}

#[test]
fn shallow_merge_adds_new_keys() {
    let global = toml::Value::Table(toml::map::Map::from_iter([(
        "a".into(),
        toml::Value::String("global_a".into()),
    )]));
    let per_app = toml::Value::Table(toml::map::Map::from_iter([(
        "new_key".into(),
        toml::Value::String("new_value".into()),
    )]));
    let merged = shallow_merge_toml(&global, &per_app, "test", "int");
    let table = merged.as_table().unwrap();
    assert_eq!(table.len(), 2);
    assert_eq!(table["a"].as_str().unwrap(), "global_a");
    assert_eq!(table["new_key"].as_str().unwrap(), "new_value");
}

#[test]
fn shallow_merge_empty_per_app_returns_global() {
    let global = toml::Value::Table(toml::map::Map::from_iter([(
        "a".into(),
        toml::Value::String("global".into()),
    )]));
    let per_app = toml::Value::Table(toml::map::Map::new());
    let merged = shallow_merge_toml(&global, &per_app, "test", "int");
    assert_eq!(merged, global);
}

#[test]
#[should_panic(expected = "must be a table")]
fn shallow_merge_panics_on_non_table_global() {
    let global = toml::Value::String("not a table".into());
    let per_app = toml::Value::Table(toml::map::Map::new());
    shallow_merge_toml(&global, &per_app, "test", "int");
}

#[test]
#[should_panic(expected = "must be a table")]
fn shallow_merge_panics_on_non_table_per_app() {
    let global = toml::Value::Table(toml::map::Map::new());
    let per_app = toml::Value::String("not a table".into());
    shallow_merge_toml(&global, &per_app, "test", "int");
}

// -----------------------------------------------------------------------
// TOML deserialization of integration fields
// -----------------------------------------------------------------------

#[test]
fn toml_parse_integration_fields() {
    let dir = tempfile::tempdir().unwrap();
    let toml_str = format!(
        r#"
[integrations.pfin]
command = "pf"

[integrations.graf]
command = "graf"

[[app]]
slug = "myapp"
working_dir = "{}"
integrations = ["pfin"]

[app.integration_config.graf]
extra_setting = "custom_value"
"#,
        dir.path().display()
    );

    let config: BrennConfig = toml::from_str(&toml_str).unwrap();

    // Global integrations parsed.
    assert_eq!(config.integrations.len(), 2);
    assert_eq!(
        config.integrations["pfin"]["command"].as_str().unwrap(),
        "pf"
    );
    assert_eq!(
        config.integrations["graf"]["command"].as_str().unwrap(),
        "graf"
    );

    // Per-app integration fields parsed.
    let app = &config.apps[0];
    assert_eq!(app.integrations, vec!["pfin"]);
    assert_eq!(app.integration_config.len(), 1);
    assert_eq!(
        app.integration_config["graf"]["extra_setting"]
            .as_str()
            .unwrap(),
        "custom_value"
    );
}

// -----------------------------------------------------------------------
// Inter-integration MCP server collision
// -----------------------------------------------------------------------

/// A second factory that contributes an MCP server with a configurable name.
struct CollisionFactory {
    factory_name: &'static str,
    server_name: &'static str,
}

impl IntegrationFactory for CollisionFactory {
    fn name(&self) -> &str {
        self.factory_name
    }

    fn create(&self, _config: Option<&toml::Value>) -> Arc<dyn Integration> {
        Arc::new(CollisionIntegration {
            name: self.factory_name,
            server_name: self.server_name,
        })
    }

    fn tools(&self) -> Vec<Box<dyn AppTool>> {
        vec![]
    }
}

struct CollisionIntegration {
    name: &'static str,
    server_name: &'static str,
}

impl Integration for CollisionIntegration {
    fn name(&self) -> &str {
        self.name
    }

    fn mcp_servers(&self) -> Vec<(String, McpServerConfig)> {
        vec![(
            self.server_name.to_string(),
            McpServerConfig {
                command: "cmd".to_string(),
                args: vec![],
                env: HashMap::new(),
            },
        )]
    }
}

#[test]
#[should_panic(expected = "both contribute")]
fn inter_integration_mcp_server_collision_panics() {
    let dir = tempfile::tempdir().unwrap();
    let registry = IntegrationRegistry::new(vec![
        Box::new(CollisionFactory {
            factory_name: "int-a",
            server_name: "shared-name",
        }),
        Box::new(CollisionFactory {
            factory_name: "int-b",
            server_name: "shared-name",
        }),
    ]);
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "myapp".to_string(),
            name: None,
            description: None,
            icon: None,
            working_dir: Some(dir.path().to_path_buf()),
            model: None,
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout_secs: None,
            compact_reminder_pct: None,
            compact_soft_pct: None,
            compact_red_pct: None,
            compact_hard_pct: None,
            compact_reminder_tokens: None,
            compact_soft_tokens: None,
            compact_red_tokens: None,
            compact_hard_tokens: None,
            compact_idle_secs: None,
            idle_hook_secs: None,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: None,
            prefix_timestamp: None,
            prefix_device: None,
            container: None,
            container_working_dir: None,
            start_hooks: None,
            post_pull_hooks: None,
            startup_hooks: None,
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: vec!["int-a".to_string(), "int-b".to_string()],
            integration_config: HashMap::new(),
            mounts: vec![],
            extra_mounts: vec![],
            history_replay_limit: None,
            frontmatter: FrontmatterRenderConfig::default(),
            messaging: None,
            pwa_push: None,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
            grants: vec![],
            acl: crate::access::raw::AppAclRaw::default(),
            tool_grants: vec![],
        }],
        ..Default::default()
    };
    validate_and_resolve(&config, &registry, Some(super::test_runtime_dir()));
}

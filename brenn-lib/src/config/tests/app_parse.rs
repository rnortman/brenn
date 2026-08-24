use super::*;
use crate::config::ResolvedConfig;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// App config loading
// -----------------------------------------------------------------------
//
// An app has no slug of its own to omit: the slug is the `new` instance name,
// so there is no document that states an app without one. That is why nothing
// here tests a missing slug.

#[test]
fn app_config_loads_minimal() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_from_dsl(&format!(
        r#"
agent Finance() {{ working_dir = "{}"; }}

new pfin: Finance();
"#,
        dir.path().display()
    ));
    assert_eq!(config.apps.len(), 1);
    assert_eq!(config.apps[0].slug, "pfin");
    assert!(config.apps[0].name.is_none());
    assert!(config.apps[0].model.is_none());
    assert!(!config.apps[0].single_instance);
    assert!(config.apps[0].allowed_users.is_empty());
    assert!(config.apps[0].disabled_tools.is_empty());
}

#[test]
fn app_config_loads_every_scalar_it_states() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_from_dsl(&format!(
        r#"
agent Finance() {{
    name = "Personal Finance";
    working_dir = "{}";
    model = "opus";
    single_instance = true;
    allowed_users = ["alice", "bob"];
    disabled_tools = ["Edit", "Write"];
}}

new pfin: Finance();
"#,
        dir.path().display()
    ));
    assert_eq!(config.apps[0].name.as_deref(), Some("Personal Finance"));
    assert_eq!(config.apps[0].model.as_deref(), Some("opus"));
    assert!(config.apps[0].single_instance);
    assert_eq!(config.apps[0].allowed_users, vec!["alice", "bob"]);
    assert_eq!(config.apps[0].disabled_tools, vec!["Edit", "Write"]);
}

#[test]
fn multiple_apps_load() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let config = config_from_dsl(&format!(
        r#"
agent Finance() {{ working_dir = "{}"; }}

agent Notes() {{
    working_dir = "{}";
    model = "opus";
}}

new pfin: Finance();
new graf: Notes();
"#,
        dir1.path().display(),
        dir2.path().display()
    ));
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
        server: super::test_server_config(),
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
/// keep `messaging: None`.
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
        send_rate: None,
        uuid: Some("1f6c6e3a-1d6e-4f7c-9b6a-12cb7e4a8d32".to_string()),
        address: Some("ch".to_string()),
        address_prefix: None,
        description: None,
        push_depth: Some(crate::messaging::config::Depth::Bounded(4)),
        retain_depth: Some(crate::messaging::config::Depth::Bounded(4)),
        standing_retain_depth: Some(crate::messaging::config::Depth::Bounded(4)),
        noise: None,
        sink: None,
        wake_min: None,
    };

    let config = BrennConfig {
        server: super::test_server_config(),
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
fn app_config_stray_key_refused() {
    let dir = tempfile::tempdir().unwrap();
    let refusal = sole_refusal(&format!(
        r#"
agent Finance() {{
    working_dir = "{}";
    bogus = true;
}}

new pfin: Finance();
"#,
        dir.path().display()
    ))
    .render();
    assert!(
        refusal.contains("bogus"),
        "the stray key is named: {refusal}"
    );
}

/// `enabled` is not an authorization boolean anywhere in the messaging
/// vocabulary. Authority is the
/// explicit `grants` surface, so a document reaching for the removed word is
/// refused rather than lowered into a silent deny-everything policy.
///
/// Per-app messaging has no block of its own in the agent vocabulary — the
/// subscriptions and the send budget are stated directly on the agent — so the
/// word's absence is stated where it could otherwise land: the global
/// `messaging` section, and an agent attr.
#[test]
fn removed_messaging_enabled_word_is_refused() {
    let section = sole_refusal("messaging { enabled = true; }").render();
    assert!(
        section.contains("enabled"),
        "the refusal names the removed word: {section}"
    );

    let attr = sole_refusal(
        r#"
agent Finance() { enabled = true; }

new pfin: Finance();
"#,
    )
    .render();
    assert!(
        attr.contains("enabled"),
        "the refusal names the removed word: {attr}"
    );
}

/// The counterpart for `pwa_push`: the per-app push authorization is a grant,
/// and `enabled` is not a word the section admits.
#[test]
fn removed_pwa_push_enabled_word_is_refused() {
    let refusal = sole_refusal("pwa_push { enabled = true; }").render();
    assert!(
        refusal.contains("enabled"),
        "the refusal names the removed word: {refusal}"
    );
}

#[test]
fn app_config_missing_working_dir_loads_ok() {
    // Loading does not validate working_dir; validate_and_resolve does.
    let config = config_from_dsl(
        r#"
agent Finance() {}

new pfin: Finance();
"#,
    );
    assert!(config.apps[0].working_dir.is_none());
}

#[test]
fn validate_mcp_servers_carried_through() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        server: super::test_server_config(),
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
fn multi_app_mcp_servers_load_per_app() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let config = config_from_dsl(&format!(
        r#"
agent Finance() {{
    working_dir = "{}";

    mcp_server finance-tool {{
        command = "python3";
        args = ["finance.py"];
    }}
}}

agent Notes() {{ working_dir = "{}"; }}

new pfin: Finance();
new graf: Notes();
"#,
        dir1.path().display(),
        dir2.path().display()
    ));
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
    let config = config_from_dsl(&format!(
        r#"
agent Finance() {{
    working_dir = "{}";

    mcp_server finance-tool {{
        command = "python3";
        args = ["finance.py"];
    }}
}}

agent Notes() {{
    working_dir = "{}";

    mcp_server graph-tool {{
        command = "node";
        args = ["graph.js"];
        env = {{ DB_URL = "postgres://localhost/graf" }};
    }}
}}

new pfin: Finance();
new graf: Notes();
"#,
        dir1.path().display(),
        dir2.path().display()
    ));
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

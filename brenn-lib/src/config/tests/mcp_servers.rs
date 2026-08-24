use super::*;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// MCP servers config
// -----------------------------------------------------------------------

#[test]
fn app_config_loads_mcp_servers() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_from_dsl(&format!(
        r#"
agent Assistant() {{
    working_dir = "{}";

    mcp_server custom-tool {{
        command = "python3";
        args = ["custom_mcp.py"];
        env = {{ API_KEY = "secret" }};
    }}
}}

new pfin: Assistant();
"#,
        dir.path().display()
    ));
    let servers = &config.apps[0].mcp_servers;
    assert_eq!(servers.len(), 1);
    let server = &servers["custom-tool"];
    assert_eq!(server.command, "python3");
    assert_eq!(server.args, vec!["custom_mcp.py"]);
    assert_eq!(server.env.get("API_KEY").unwrap(), "secret");
}

#[test]
fn app_config_mcp_servers_empty_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_from_dsl(&format!(
        r#"
agent Assistant() {{ working_dir = "{}"; }}

new pfin: Assistant();
"#,
        dir.path().display()
    ));
    assert!(config.apps[0].mcp_servers.is_empty());
}

#[test]
fn app_config_mcp_server_stray_key_refused() {
    let refusal = sole_refusal(
        r#"
agent Assistant() {
    mcp_server bad {
        command = "python3";
        args = [];
        bogus_field = true;
    }
}

new pfin: Assistant();
"#,
    )
    .render();
    assert!(
        refusal.contains("bogus_field"),
        "the stray key is named: {refusal}"
    );
}

#[test]
#[should_panic(expected = "reserved for the built-in")]
fn validate_mcp_server_reserved_name_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
        server: super::test_server_config(),
        apps: vec![AppConfigRaw {
            slug: "pfin".to_string(),
            working_dir: Some(dir.path().to_path_buf()),
            mcp_servers: HashMap::from([(
                "brenn".to_string(),
                McpServerConfig {
                    command: "python3".to_string(),
                    args: vec![],
                    env: HashMap::new(),
                },
            )]),
            ..Default::default()
        }],
        ..Default::default()
    };
    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

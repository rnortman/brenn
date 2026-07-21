use super::*;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// MCP servers config
// -----------------------------------------------------------------------

#[test]
fn app_config_parses_mcp_servers() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"

[app.mcp_servers.custom-tool]
command = "python3"
args = ["custom_mcp.py"]
env = {{ API_KEY = "secret" }}
"#,
        dir.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
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
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"
"#,
        dir.path().display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    assert!(config.apps[0].mcp_servers.is_empty());
}

#[test]
fn app_config_mcp_servers_unknown_field_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        r#"
[[app]]
slug = "pfin"
working_dir = "{}"

[app.mcp_servers.bad]
command = "python3"
args = []
bogus_field = true
"#,
        dir.path().display()
    );
    assert!(toml::from_str::<BrennConfig>(&toml).is_err());
}

#[test]
#[should_panic(expected = "reserved for the built-in")]
fn validate_mcp_server_reserved_name_panics() {
    let dir = tempfile::tempdir().unwrap();
    let config = BrennConfig {
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

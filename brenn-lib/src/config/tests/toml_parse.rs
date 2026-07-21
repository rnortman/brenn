use super::*;

// -----------------------------------------------------------------------
// TOML parsing: happy paths
// -----------------------------------------------------------------------

#[test]
fn parse_empty_toml_uses_defaults() {
    let config: BrennConfig = toml::from_str("").unwrap();
    assert!(config.server.secure_cookies);
    assert_eq!(config.logging.console_level, LevelFilter::INFO);
}

// Parse a checked-in config file and assert the messaging/public_url boot
// invariant. Full validation (validate_and_resolve) needs host-side paths to
// exist, so it only runs at server startup — but this invariant needs none of
// those paths: once any messaging is configured (a [[surface]] or
// [[ephemeral_channel]]), the message-source resolver requires a non-empty
// server.public_url or boot panics. Asserting it here guards the file against a
// regression that make check would otherwise miss (only a live server start
// would catch it — and make e2e is not part of make check).
fn assert_config_file_messaging_invariant(filename: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(filename);
    let contents = std::fs::read_to_string(&path).unwrap();
    let config: BrennConfig =
        toml::from_str(&contents).unwrap_or_else(|e| panic!("{filename} parse failed: {e}"));
    if !config.surfaces.is_empty() || !config.ephemeral_channels.is_empty() {
        let public_url = config.server.public_url.as_deref().unwrap_or_else(|| {
            panic!("{filename} configures messaging, so server.public_url is required")
        });
        assert!(
            !public_url.is_empty(),
            "server.public_url must be non-empty once messaging is configured"
        );
    }
}

#[test]
fn brenn_dev_toml_parses() {
    assert_config_file_messaging_invariant("brenn.dev.toml");
}

#[test]
fn brenn_e2e_toml_parses() {
    assert_config_file_messaging_invariant("brenn.e2e.toml");
}

#[test]
fn parse_partial_toml_overrides_only_specified_fields() {
    let toml = r#"
[server]
bind_address = "127.0.0.1:8080"
secure_cookies = false
"#;
    let config: BrennConfig = toml::from_str(toml).unwrap();
    assert_eq!(
        config.server.bind_address,
        "127.0.0.1:8080".parse().unwrap()
    );
    assert!(!config.server.secure_cookies);
    // Unspecified server field keeps default.
    assert_eq!(
        config.server.static_dir,
        PathBuf::from("/opt/brenn/frontend/dist")
    );
    // Unspecified sections keep defaults.
    assert_eq!(
        config.database.path,
        PathBuf::from("/var/lib/brenn/brenn.db")
    );
    assert_eq!(config.security.auth_rate_burst, 10);
}

#[test]
fn parse_full_toml_all_fields() {
    let dir = tempfile::tempdir().unwrap();
    let app_dir = dir.path().join("myapp");
    std::fs::create_dir(&app_dir).unwrap();

    let toml = format!(
        r#"
[server]
bind_address = "10.0.0.1:9090"
static_dir = "/srv/static"
secure_cookies = false

[database]
path = "/data/my.db"

[logging]
log_dir = "/tmp/logs"
console_level = "warn"
file_level = "trace"

[security]
auth_rate_interval_secs = 10
auth_rate_burst = 5
global_rate_interval_secs = 2
global_rate_burst = 50
auth_body_limit = 2048
global_body_limit = 512000

[alerting]
max_alerts = 10
window_secs = 60

[alerting.ntfy]
url = "https://ntfy.sh/brenn-alerts"

[claude_defaults]
mcp_script_path = "/opt/brenn/noop_mcp.py"
model = "opus"

[[app]]
slug = "myapp"
working_dir = "{}"
"#,
        app_dir.display()
    );
    let config: BrennConfig = toml::from_str(&toml).unwrap();
    // Server
    assert_eq!(config.server.bind_address, "10.0.0.1:9090".parse().unwrap());
    assert_eq!(config.server.static_dir, PathBuf::from("/srv/static"));
    assert!(!config.server.secure_cookies);
    // Database
    assert_eq!(config.database.path, PathBuf::from("/data/my.db"));
    // Logging
    assert_eq!(config.logging.log_dir, PathBuf::from("/tmp/logs"));
    assert_eq!(config.logging.console_level, LevelFilter::WARN);
    assert_eq!(config.logging.file_level, LevelFilter::TRACE);
    // Security
    assert_eq!(config.security.auth_rate_interval_secs, 10);
    assert_eq!(config.security.auth_rate_burst, 5);
    assert_eq!(config.security.global_rate_interval_secs, 2);
    assert_eq!(config.security.global_rate_burst, 50);
    assert_eq!(config.security.auth_body_limit, 2048);
    assert_eq!(config.security.global_body_limit, 512000);
    // Alerting
    let alert = config.alerting.as_ref().unwrap();
    assert!(alert.ntfy.is_some());
    assert_eq!(
        alert.ntfy.as_ref().unwrap().url,
        "https://ntfy.sh/brenn-alerts"
    );
    assert!(alert.mail.is_none());
    assert_eq!(alert.max_alerts, 10);
    assert_eq!(alert.window_secs, 60);
    // Claude defaults
    assert_eq!(
        config.claude_defaults.mcp_script_path,
        PathBuf::from("/opt/brenn/noop_mcp.py")
    );
    assert_eq!(config.claude_defaults.model, "opus");
    // Apps
    assert_eq!(config.apps.len(), 1);
    assert_eq!(config.apps[0].slug, "myapp");
}

#[test]
fn all_log_levels_parse() {
    for level in &["trace", "debug", "info", "warn", "error"] {
        let toml = format!("[logging]\nconsole_level = \"{level}\"\nfile_level = \"{level}\"");
        let config: BrennConfig = toml::from_str(&toml).unwrap();
        assert_eq!(
            config.logging.console_level.to_string().to_lowercase(),
            *level,
        );
    }
}

#[test]
fn log_levels_case_insensitive() {
    let toml = r#"
[logging]
console_level = "INFO"
file_level = "DEBUG"
"#;
    let config: BrennConfig = toml::from_str(toml).unwrap();
    assert_eq!(config.logging.console_level, LevelFilter::INFO);
    assert_eq!(config.logging.file_level, LevelFilter::DEBUG);
}

#[test]
fn off_log_level_parses() {
    let toml = r#"
[logging]
console_level = "off"
"#;
    let config: BrennConfig = toml::from_str(toml).unwrap();
    assert_eq!(config.logging.console_level, LevelFilter::OFF);
}

// -----------------------------------------------------------------------
// TOML parsing: rejection of invalid input
// -----------------------------------------------------------------------

#[test]
fn unknown_field_rejected() {
    let toml = r#"
[server]
bind_address = "127.0.0.1:3000"
bogus_field = true
"#;
    let err = toml::from_str::<BrennConfig>(toml).unwrap_err();
    assert!(err.to_string().contains("bogus_field"));
}

#[test]
fn unknown_section_rejected() {
    let toml = r#"
[bogus_section]
foo = "bar"
"#;
    assert!(toml::from_str::<BrennConfig>(toml).is_err());
}

/// Deployer configs that still contain the deleted `compact_check_turns`
/// or `compact_check_secs` keys must fail to parse (test-8).
///
/// Verifies that `deny_unknown_fields` on `AppConfigRaw` catches these
/// removed fields so that a deployer upgrading from an old config gets a
/// clear error rather than silently ignoring the now-dead keys.
#[test]
fn deleted_compact_check_turns_key_rejected() {
    let toml = r#"
[[app]]
slug = "pfin"
compact_check_turns = 5
"#;
    assert!(
        toml::from_str::<BrennConfig>(toml).is_err(),
        "compact_check_turns was deleted and must be rejected by deny_unknown_fields"
    );
}

#[test]
fn deleted_compact_check_secs_key_rejected() {
    let toml = r#"
[[app]]
slug = "pfin"
compact_check_secs = 600
"#;
    assert!(
        toml::from_str::<BrennConfig>(toml).is_err(),
        "compact_check_secs was deleted and must be rejected by deny_unknown_fields"
    );
}

/// Deployer configs that still contain the deleted `instance` key under
/// `[server]` must fail to parse.
///
/// `ServerConfig` has `deny_unknown_fields`; any TOML with `instance = ...`
/// after this change must produce a clear deserialization error rather than
/// silently succeeding.
#[test]
fn deleted_server_instance_key_rejected() {
    let toml = r#"
[server]
instance = "prod"
"#;
    assert!(
        toml::from_str::<BrennConfig>(toml).is_err(),
        "instance was deleted from ServerConfig and must be rejected by deny_unknown_fields"
    );
}

#[test]
fn old_claude_section_rejected() {
    let toml = r#"
[claude]
working_dir = "."
mcp_script_path = "noop_mcp.py"
model = "sonnet"
"#;
    assert!(
        toml::from_str::<BrennConfig>(toml).is_err(),
        "[claude] section should be rejected — use [claude_defaults] + [[app]]"
    );
}

#[test]
fn invalid_log_level_rejected() {
    let toml = r#"
[logging]
console_level = "banana"
"#;
    let err = toml::from_str::<BrennConfig>(toml).unwrap_err();
    assert!(err.to_string().contains("banana"));
}

#[test]
fn invalid_socket_address_rejected() {
    let toml = r#"
[server]
bind_address = "not-an-address"
"#;
    assert!(toml::from_str::<BrennConfig>(toml).is_err());
}

#[test]
fn wrong_type_for_field_rejected() {
    let toml = r#"
[server]
bind_address = 42
"#;
    assert!(toml::from_str::<BrennConfig>(toml).is_err());
}

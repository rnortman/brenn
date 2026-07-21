use super::*;

// -----------------------------------------------------------------------
// CC_KNOWN_TOOLS invariants
// -----------------------------------------------------------------------

/// Regression: the two tools added in CC 2.1.111 must stay in the list
/// or the runtime validator in `active_bridge.rs` will re-fire the
/// "Unknown CC tools detected" alert storm.
#[test]
fn cc_known_tools_includes_push_notification_and_schedule_wakeup() {
    assert!(
        CC_KNOWN_TOOLS.contains(&"PushNotification"),
        "PushNotification must stay in CC_KNOWN_TOOLS (added CC 2.1.111)"
    );
    assert!(
        CC_KNOWN_TOOLS.contains(&"ScheduleWakeup"),
        "ScheduleWakeup must stay in CC_KNOWN_TOOLS (added CC 2.1.111)"
    );
}

#[test]
fn cc_known_tools_no_mcp_prefix() {
    // MCP tools are managed separately and must never sneak into this list.
    for tool in CC_KNOWN_TOOLS {
        assert!(
            !tool.starts_with("mcp__"),
            "mcp__* tools must not be in CC_KNOWN_TOOLS: {tool}"
        );
    }
}

// -----------------------------------------------------------------------
// Default values
// -----------------------------------------------------------------------

#[test]
fn default_config_is_production_hardened() {
    let config = BrennConfig::default();
    // Server
    assert!(config.server.secure_cookies);
    assert_eq!(
        config.server.bind_address,
        SocketAddr::from(([0, 0, 0, 0], 3000))
    );
    assert_eq!(
        config.server.static_dir,
        PathBuf::from("/opt/brenn/frontend/dist")
    );
    // Database
    assert_eq!(
        config.database.path,
        PathBuf::from("/var/lib/brenn/brenn.db")
    );
    // Logging
    assert_eq!(config.logging.log_dir, PathBuf::from("/var/log/brenn"));
    assert_eq!(config.logging.console_level, LevelFilter::INFO);
    assert_eq!(config.logging.file_level, LevelFilter::DEBUG);
    // Alerting
    assert!(config.alerting.is_none());
    // Claude defaults
    assert_eq!(
        config.claude_defaults.mcp_script_path,
        PathBuf::from("/opt/brenn/noop_mcp.py")
    );
    assert_eq!(config.claude_defaults.model, "sonnet");
    // Apps
    assert!(config.apps.is_empty());
}

#[test]
fn default_security_config() {
    let sec = SecurityConfig::default();
    assert_eq!(sec.auth_rate_interval_secs, 6);
    assert_eq!(sec.auth_rate_burst, 10);
    assert_eq!(sec.global_rate_interval_secs, 1);
    assert_eq!(sec.global_rate_burst, 100);
    assert_eq!(sec.asset_rate_interval_secs, 1);
    assert_eq!(sec.asset_rate_burst, 2000);
    assert_eq!(sec.auth_body_limit, 4096);
    assert_eq!(sec.global_body_limit, 1024 * 1024);
    assert_eq!(sec.upload_body_limit, 25 * 1024 * 1024);
    assert_eq!(sec.max_image_long_edge, 2576);
}

#[test]
fn security_config_upload_fields_override_via_toml() {
    let toml = r#"
upload_body_limit = 10485760
max_image_long_edge = 1024
"#;
    let sec: SecurityConfig = toml::from_str(toml).unwrap();
    assert_eq!(sec.upload_body_limit, 10 * 1024 * 1024);
    assert_eq!(sec.max_image_long_edge, 1024);
}

#[test]
fn security_config_upload_fields_default_when_absent_from_toml() {
    // [security] present with other fields set; upload fields absent → defaults apply.
    let toml = r#"
auth_rate_interval_secs = 10
auth_body_limit = 2048
"#;
    let sec: SecurityConfig = toml::from_str(toml).unwrap();
    assert_eq!(sec.upload_body_limit, 25 * 1024 * 1024);
    assert_eq!(sec.max_image_long_edge, 2576);
}

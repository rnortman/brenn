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

/// The values behind the config modules' default functions, pinned once.
///
/// The lowering suite's expected literals *call* these functions, so they lock
/// which function fills a field and not what it returns. Two of them —
/// `default_tls_version_min` and `default_hmac_algorithm` — are security
/// defaults on an external ingress surface, so a silent weakening has to fail
/// somewhere.
#[test]
fn default_functions_return_the_values_they_are_relied_on_for() {
    // Repos
    assert!(crate::config::repo::default_true());
    // Containers
    assert_eq!(
        crate::config::container::default_container_home(),
        PathBuf::from("/home/user")
    );
    // Alerting
    assert_eq!(crate::config::alerting::default_subject_label(), "Brenn");
    // Attachments
    assert_eq!(crate::config::attachment::default_timeout_secs(), 60);
    // MQTT clients
    assert_eq!(
        crate::mqtt::config::default_client_urgency(),
        crate::messaging::Urgency::Normal
    );
    assert_eq!(crate::mqtt::config::default_tls_version_min(), "1.2");
    assert_eq!(
        crate::mqtt::config::default_inbound_payload_cap(),
        4 * 1024 * 1024
    );
    assert_eq!(crate::mqtt::config::default_backoff_initial(), 1);
    assert_eq!(crate::mqtt::config::default_backoff_max(), 60);
    assert_eq!(crate::mqtt::config::default_subscription_qos(), 1);
    // Webhook endpoints
    assert_eq!(
        crate::webhook::config::default_transport_ceiling(),
        1024 * 1024
    );
    assert_eq!(
        crate::webhook::config::default_content_type(),
        "application/json"
    );
    assert_eq!(
        crate::webhook::config::default_hmac_algorithm(),
        "hmac-sha256"
    );
}

#[test]
fn security_config_upload_fields_override() {
    let config = config_from_dsl(
        r#"
security {
    upload_body_limit = 10485760;
    max_image_long_edge = 1024;
}
"#,
    );
    assert_eq!(config.security.upload_body_limit, 10 * 1024 * 1024);
    assert_eq!(config.security.max_image_long_edge, 1024);
}

#[test]
fn security_config_upload_fields_default_when_absent() {
    // Section present with other keys stated; upload keys absent → defaults apply.
    let config = config_from_dsl(
        r#"
security {
    auth_rate_interval_secs = 10;
    auth_body_limit = 2048;
}
"#,
    );
    assert_eq!(config.security.upload_body_limit, 25 * 1024 * 1024);
    assert_eq!(config.security.max_image_long_edge, 2576);
}

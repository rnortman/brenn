use std::path::PathBuf;
use tracing::level_filters::LevelFilter;

/// Configuration for the observability subsystem.
pub struct ObsConfig {
    /// Directory for all log files.
    pub log_dir: PathBuf,
    /// Minimum level for console output.
    pub console_level: LevelFilter,
    /// Minimum level for the diagnostic file log.
    pub file_level: LevelFilter,
    /// Filename for the diagnostic log (stable name; rotation handled by logrotate).
    pub diagnostic_log_name: String,
    /// Filename for the security log (stable name; rotation handled by logrotate).
    pub security_log_name: String,
    /// Alert configuration. None disables alerting (development mode).
    pub alert: Option<AlertConfig>,
    /// Instance identifier for alert context (e.g., config file path).
    /// If set, included as "Instance" in every alert body. Combined with
    /// the auto-detected hostname, this uniquely identifies which Brenn
    /// process sent the alert.
    pub instance_name: Option<String>,
}

/// Configuration for the alerting subsystem.
pub struct AlertConfig {
    /// Which backend to use for sending alerts.
    pub backend: AlertBackend,
    /// Rate limiting for alerts.
    pub rate_limit: RateLimitConfig,
}

/// Alert delivery backend.
pub enum AlertBackend {
    /// HTTP POST to an ntfy topic.
    Ntfy { url: String },
    /// Shell out to the `mail` command.
    Mail { to: String, subject_label: String },
}

/// Rate limiting configuration for alerts.
pub struct RateLimitConfig {
    /// Maximum number of alerts within the window.
    pub max_alerts: u32,
    /// Window duration in seconds.
    pub window_secs: u64,
}

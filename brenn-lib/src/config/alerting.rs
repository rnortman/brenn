/// Alerting configuration. Rate limit fields are shared; exactly one backend
/// sub-table (`ntfy` or `mail`) must be present.
#[derive(Debug, PartialEq)]
pub struct AlertingConfig {
    /// Maximum number of alerts within the rate limit window.
    pub max_alerts: u32,
    /// Rate limit window duration in seconds.
    pub window_secs: u64,
    /// ntfy backend configuration.
    pub ntfy: Option<NtfyConfig>,
    /// Mail backend configuration.
    pub mail: Option<MailConfig>,
}

/// ntfy alerting backend.
#[derive(Debug, PartialEq)]
pub struct NtfyConfig {
    /// ntfy topic URL (e.g. "https://ntfy.sh/brenn-alerts").
    pub url: String,
}

/// Mail alerting backend. Shells out to the `mail` command.
#[derive(Debug, PartialEq)]
pub struct MailConfig {
    /// Destination email address.
    pub to: String,
    /// Label for the subject line prefix (e.g. "[Brenn] ...").
    pub subject_label: String,
}

pub(crate) fn default_subject_label() -> String {
    "Brenn".to_string()
}

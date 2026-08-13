//! `[automation]` configuration section.
//!
//! Deserialized by `BrennConfig` and stored on `AutomationEngine`.

use serde::Deserialize;

/// Global automation defaults from the `[automation]` config section.
///
/// All fields have sensible defaults; the section may be omitted.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AutomationGlobalConfig {
    /// Per-job cap on fires per hour. Fires beyond this are dropped and
    /// produce an error report. Default 60.
    pub max_fires_per_hour_per_job: u32,
    /// Per-job cap on error reports per hour. Overflow suppresses further
    /// reports and issues one human alert. Default 3.
    pub max_error_reports_per_hour_per_job: u32,
    /// Number of consecutive failures before a job is auto-disabled.
    /// Default 5.
    pub consecutive_failures_to_disable: u32,
    /// Maximum number of jobs an app can own (including disabled jobs). An LLM
    /// cannot circumvent this by disabling then re-creating; deleted jobs free
    /// slots. Default 50.
    pub max_jobs_per_app: u32,
}

impl Default for AutomationGlobalConfig {
    fn default() -> Self {
        Self {
            max_fires_per_hour_per_job: 60,
            max_error_reports_per_hour_per_job: 3,
            consecutive_failures_to_disable: 5,
            max_jobs_per_app: 50,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values() {
        let cfg = AutomationGlobalConfig::default();
        assert_eq!(cfg.max_fires_per_hour_per_job, 60);
        assert_eq!(cfg.max_error_reports_per_hour_per_job, 3);
        assert_eq!(cfg.consecutive_failures_to_disable, 5);
        assert_eq!(cfg.max_jobs_per_app, 50);
    }

    #[test]
    fn toml_deserialization_full() {
        let toml = r#"
max_fires_per_hour_per_job = 30
max_error_reports_per_hour_per_job = 5
consecutive_failures_to_disable = 10
max_jobs_per_app = 20
"#;
        let cfg: AutomationGlobalConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.max_fires_per_hour_per_job, 30);
        assert_eq!(cfg.max_error_reports_per_hour_per_job, 5);
        assert_eq!(cfg.consecutive_failures_to_disable, 10);
        assert_eq!(cfg.max_jobs_per_app, 20);
    }

    #[test]
    fn toml_deserialization_empty_uses_defaults() {
        let cfg: AutomationGlobalConfig = toml::from_str("").unwrap();
        assert_eq!(cfg.max_fires_per_hour_per_job, 60);
    }

    #[test]
    #[should_panic]
    fn toml_deserialization_rejects_unknown_fields() {
        let toml = r#"unknown_field = 42"#;
        toml::from_str::<AutomationGlobalConfig>(toml).unwrap();
    }
}

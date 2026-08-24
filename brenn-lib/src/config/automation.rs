//! `[automation]` configuration section.

/// Global automation defaults from the `[automation]` config section.
///
/// All fields have sensible defaults; the section may be omitted.
#[derive(Debug, Clone, PartialEq)]
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
}

//! Build `obs::ObsConfig` from loaded `BrennConfig` + config path.

use std::path::PathBuf;

use brenn_lib::config::BrennConfig;
use brenn_lib::obs;

/// Translate a `BrennConfig` + optional config file path into an
/// `obs::ObsConfig` ready for `obs::init`.
pub(crate) fn build(config: &BrennConfig, config_path: Option<&PathBuf>) -> obs::ObsConfig {
    obs::ObsConfig {
        log_dir: config.logging.log_dir.clone(),
        console_level: config.logging.console_level,
        file_level: config.logging.file_level,
        diagnostic_log_name: "brenn.log".to_string(),
        security_log_name: "security.log".to_string(),
        instance_name: config_path.map(|p| p.display().to_string()),
        alert: config.alerting.as_ref().map(|a| {
            let backend = match (&a.ntfy, &a.mail) {
                (Some(ntfy), None) => obs::config::AlertBackend::Ntfy {
                    url: ntfy.url.clone(),
                },
                (None, Some(mail)) => obs::config::AlertBackend::Mail {
                    to: mail.to.clone(),
                    subject_label: mail.subject_label.clone(),
                },
                (Some(_), Some(_)) => {
                    panic!(
                        "alerting config has both [alerting.ntfy] and [alerting.mail] — pick one"
                    )
                }
                (None, None) => {
                    panic!(
                        "alerting config has neither [alerting.ntfy] nor [alerting.mail] — need a backend"
                    )
                }
            };
            obs::config::AlertConfig {
                backend,
                rate_limit: obs::config::RateLimitConfig {
                    max_alerts: a.max_alerts,
                    window_secs: a.window_secs,
                },
            }
        }),
    }
}

#[cfg(test)]
mod tests {
    use brenn_lib::config::{AlertingConfig, BrennConfig, MailConfig, NtfyConfig};

    use super::build;

    fn config_with_alerting(alerting: AlertingConfig) -> BrennConfig {
        BrennConfig {
            alerting: Some(alerting),
            ..Default::default()
        }
    }

    #[test]
    #[should_panic(expected = "both [alerting.ntfy] and [alerting.mail]")]
    fn build_panics_when_both_ntfy_and_mail_set() {
        let cfg = config_with_alerting(AlertingConfig {
            ntfy: Some(NtfyConfig {
                url: "https://ntfy.sh/test".to_string(),
            }),
            mail: Some(MailConfig {
                to: "test@example.com".to_string(),
                subject_label: "Test".to_string(),
            }),
            max_alerts: 5,
            window_secs: 60,
        });
        build(&cfg, None);
    }

    #[test]
    #[should_panic(expected = "neither [alerting.ntfy] nor [alerting.mail]")]
    fn build_panics_when_neither_ntfy_nor_mail_set() {
        let cfg = config_with_alerting(AlertingConfig {
            ntfy: None,
            mail: None,
            max_alerts: 5,
            window_secs: 60,
        });
        build(&cfg, None);
    }
}

use super::*;

#[test]
fn alerting_ntfy_backend() {
    let toml = r#"
[alerting]
max_alerts = 10
window_secs = 60

[alerting.ntfy]
url = "https://ntfy.sh/test"
"#;
    let config: BrennConfig = toml::from_str(toml).unwrap();
    let alert = config.alerting.unwrap();
    assert!(alert.ntfy.is_some());
    assert!(alert.mail.is_none());
    assert_eq!(alert.ntfy.unwrap().url, "https://ntfy.sh/test");
}

#[test]
fn alerting_mail_backend() {
    let toml = r#"
[alerting]
max_alerts = 5
window_secs = 120

[alerting.mail]
to = "user@example.com"
subject_label = "MyServer"
"#;
    let config: BrennConfig = toml::from_str(toml).unwrap();
    let alert = config.alerting.unwrap();
    assert!(alert.ntfy.is_none());
    assert!(alert.mail.is_some());
    let mail = alert.mail.unwrap();
    assert_eq!(mail.to, "user@example.com");
    assert_eq!(mail.subject_label, "MyServer");
}

#[test]
fn alerting_mail_subject_label_defaults() {
    let toml = r#"
[alerting]
max_alerts = 5
window_secs = 120

[alerting.mail]
to = "user@example.com"
"#;
    let config: BrennConfig = toml::from_str(toml).unwrap();
    let mail = config.alerting.unwrap().mail.unwrap();
    assert_eq!(mail.subject_label, "Brenn");
}

#[test]
fn alerting_both_backends_parses_but_needs_runtime_validation() {
    // Both ntfy and mail sub-tables present. Serde accepts it (both are Option),
    // but runtime validation in main.rs will panic. This test documents that
    // the serde layer doesn't enforce mutual exclusivity.
    let toml = r#"
[alerting]
max_alerts = 10
window_secs = 60

[alerting.ntfy]
url = "https://ntfy.sh/test"

[alerting.mail]
to = "user@example.com"
"#;
    let config: BrennConfig = toml::from_str(toml).unwrap();
    let alert = config.alerting.unwrap();
    assert!(alert.ntfy.is_some());
    assert!(alert.mail.is_some());
}

#[test]
fn alerting_no_backend_parses_but_needs_runtime_validation() {
    // Neither backend sub-table present. Serde accepts it,
    // but runtime validation in main.rs will panic.
    let toml = r#"
[alerting]
max_alerts = 10
window_secs = 60
"#;
    let config: BrennConfig = toml::from_str(toml).unwrap();
    let alert = config.alerting.unwrap();
    assert!(alert.ntfy.is_none());
    assert!(alert.mail.is_none());
}

#[test]
fn alerting_requires_rate_limit_fields() {
    // Backend present but max_alerts and window_secs missing.
    let toml = r#"
[alerting]

[alerting.ntfy]
url = "https://ntfy.sh/test"
"#;
    assert!(toml::from_str::<BrennConfig>(toml).is_err());
}

#[test]
fn alerting_partial_rate_limit_rejected() {
    let toml = r#"
[alerting]
max_alerts = 10

[alerting.ntfy]
url = "https://ntfy.sh/test"
"#;
    assert!(toml::from_str::<BrennConfig>(toml).is_err());
}

#[test]
fn alerting_unknown_field_rejected() {
    let toml = r#"
[alerting]
max_alerts = 10
window_secs = 60
extra = true

[alerting.ntfy]
url = "https://ntfy.sh/test"
"#;
    assert!(toml::from_str::<BrennConfig>(toml).is_err());
}

#[test]
fn alerting_ntfy_unknown_field_rejected() {
    let toml = r#"
[alerting]
max_alerts = 10
window_secs = 60

[alerting.ntfy]
url = "https://ntfy.sh/test"
extra = true
"#;
    assert!(toml::from_str::<BrennConfig>(toml).is_err());
}

#[test]
fn alerting_mail_unknown_field_rejected() {
    let toml = r#"
[alerting]
max_alerts = 10
window_secs = 60

[alerting.mail]
to = "user@example.com"
extra = true
"#;
    assert!(toml::from_str::<BrennConfig>(toml).is_err());
}

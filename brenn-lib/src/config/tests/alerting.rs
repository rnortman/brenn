//! What an `alerting` section and its backend sub-block mean once loaded.

use super::*;

#[test]
fn alerting_ntfy_backend() {
    let config = config_from_dsl(
        r#"
alerting {
    max_alerts = 10;
    window_secs = 60;
    ntfy { url = "https://ntfy.example.com/alice-alerts"; }
}
"#,
    );
    let alert = config.alerting.unwrap();
    assert!(alert.ntfy.is_some());
    assert!(alert.mail.is_none());
    assert_eq!(
        alert.ntfy.unwrap().url,
        "https://ntfy.example.com/alice-alerts"
    );
}

#[test]
fn alerting_mail_backend() {
    let config = config_from_dsl(
        r#"
alerting {
    max_alerts = 5;
    window_secs = 120;
    mail {
        to = "alice@example.com";
        subject_label = "Alice's Brenn";
    }
}
"#,
    );
    let alert = config.alerting.unwrap();
    assert!(alert.ntfy.is_none());
    assert!(alert.mail.is_some());
    let mail = alert.mail.unwrap();
    assert_eq!(mail.to, "alice@example.com");
    assert_eq!(mail.subject_label, "Alice's Brenn");
}

#[test]
fn alerting_mail_subject_label_defaults() {
    let config = config_from_dsl(
        r#"
alerting {
    max_alerts = 5;
    window_secs = 120;
    mail { to = "alice@example.com"; }
}
"#,
    );
    let mail = config.alerting.unwrap().mail.unwrap();
    assert_eq!(mail.subject_label, "Brenn");
}

/// Both backends stated loads; mutual exclusivity is a runtime check in the
/// binary, not a config-loading one, and this pins that the loader is not the
/// place it happens.
#[test]
fn alerting_both_backends_load_and_need_runtime_validation() {
    let config = config_from_dsl(
        r#"
alerting {
    max_alerts = 10;
    window_secs = 60;
    ntfy { url = "https://ntfy.example.com/alice-alerts"; }
    mail { to = "alice@example.com"; }
}
"#,
    );
    let alert = config.alerting.unwrap();
    assert!(alert.ntfy.is_some());
    assert!(alert.mail.is_some());
}

/// Neither backend stated also loads — same reason as above.
#[test]
fn alerting_no_backend_loads_and_needs_runtime_validation() {
    let config = config_from_dsl(
        r#"
alerting {
    max_alerts = 10;
    window_secs = 60;
}
"#,
    );
    let alert = config.alerting.unwrap();
    assert!(alert.ntfy.is_none());
    assert!(alert.mail.is_none());
}

/// The two rate-limit keys are required: the section has no `Default` to fall
/// back to, so a document that omits them is refused rather than lowered to
/// zeros.
#[test]
fn alerting_requires_rate_limit_fields() {
    let refusal = sole_refusal(
        r#"
alerting {
    ntfy { url = "https://ntfy.example.com/alice-alerts"; }
}
"#,
    )
    .render();
    assert!(
        refusal.contains("max_alerts"),
        "the missing key is named: {refusal}"
    );
}

#[test]
fn alerting_partial_rate_limit_refused() {
    let refusal = sole_refusal(
        r#"
alerting {
    max_alerts = 10;
    ntfy { url = "https://ntfy.example.com/alice-alerts"; }
}
"#,
    )
    .render();
    assert!(
        refusal.contains("window_secs"),
        "the one missing key is named: {refusal}"
    );
}

#[test]
fn alerting_stray_key_refused() {
    let refusal = sole_refusal(
        r#"
alerting {
    max_alerts = 10;
    window_secs = 60;
    extra = true;
    ntfy { url = "https://ntfy.example.com/alice-alerts"; }
}
"#,
    )
    .render();
    assert!(
        refusal.contains("extra"),
        "the stray key is named: {refusal}"
    );
}

#[test]
fn alerting_ntfy_stray_key_refused() {
    let refusal = sole_refusal(
        r#"
alerting {
    max_alerts = 10;
    window_secs = 60;
    ntfy {
        url = "https://ntfy.example.com/alice-alerts";
        extra = true;
    }
}
"#,
    )
    .render();
    assert!(
        refusal.contains("extra"),
        "the stray key is named: {refusal}"
    );
}

#[test]
fn alerting_mail_stray_key_refused() {
    let refusal = sole_refusal(
        r#"
alerting {
    max_alerts = 10;
    window_secs = 60;
    mail {
        to = "alice@example.com";
        extra = true;
    }
}
"#,
    )
    .render();
    assert!(
        refusal.contains("extra"),
        "the stray key is named: {refusal}"
    );
}

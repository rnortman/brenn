use super::*;

/// The fixed git-webhook route and its `[[repo]].webhook_secret_file` key are
/// retired. `RepoDeclRaw` carries `#[serde(deny_unknown_fields)]`, so a config
/// still setting the old key now fails to parse at load time — the deliberate
/// no-shim cutover signal (operators move to per-forge `[[webhook_endpoint]]`
/// blocks and per-endpoint `[[webhook_endpoint.key]].secret_file`).
#[test]
fn stale_webhook_secret_file_key_is_rejected() {
    let toml = r#"
repo_dir = "/tmp/repos"

[[repo]]
slug = "myrepo"
remote = "https://example.com/r.git"
webhook_secret_file = "/etc/brenn/secrets/hook"

[[app]]
slug = "myapp"
working_dir = "/tmp/repos/myrepo"

[[app.mount]]
repo = "myrepo"
"#;
    let err = toml::from_str::<BrennConfig>(toml)
        .expect_err("webhook_secret_file must be rejected as an unknown field");
    let msg = err.to_string();
    assert!(
        msg.contains("webhook_secret_file"),
        "parse error should name the offending key, got: {msg}"
    );
}

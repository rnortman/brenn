use super::*;

/// The fixed git-webhook route and its per-repo `webhook_secret_file` key are
/// retired. The word is not in the `repo` vocabulary, so a document still
/// setting it is refused rather than silently ignored — the deliberate no-shim
/// cutover signal (operators move to per-forge `webhook_endpoint` blocks and
/// their `key` sub-blocks).
#[test]
fn stale_webhook_secret_file_key_is_refused() {
    let refusal = sole_refusal(
        r#"
repo myrepo {
    remote = "https://example.com/r.git";
    webhook_secret_file = "/etc/brenn/secrets/hook";
}
"#,
    )
    .render();
    assert!(
        refusal.contains("webhook_secret_file"),
        "the refusal names the offending key, got: {refusal}"
    );
}

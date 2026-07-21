use super::*;
use crate::integration::IntegrationRegistry;

// -----------------------------------------------------------------------
// validate_public_url tests — exercised via validate_and_resolve
// -----------------------------------------------------------------------

/// Minimal BrennConfig with one bare app for panic tests.
/// Panic tests fire before state_dir resolution, so no runtime_dir needed.
fn minimal_config_with_public_url(public_url: Option<&str>) -> BrennConfig {
    BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            ..Default::default()
        }],
        server: crate::config::server::ServerConfig {
            public_url: public_url.map(str::to_string),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Config with a real working_dir for happy-path tests that must survive XDG validation.
/// `working_dir` is passed in (not created here) so TempDir is dropped by the caller
/// after validate_and_resolve returns, not before.
fn valid_config_with_public_url(
    working_dir: std::path::PathBuf,
    public_url: Option<&str>,
) -> BrennConfig {
    BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            working_dir: Some(working_dir),
            ..Default::default()
        }],
        server: crate::config::server::ServerConfig {
            public_url: public_url.map(str::to_string),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn public_url_none_passes() {
    let dir = tempfile::tempdir().unwrap();
    // Should not panic.
    validate_and_resolve(
        &valid_config_with_public_url(dir.path().to_path_buf(), None),
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

#[test]
fn public_url_valid_https_passes() {
    let dir = tempfile::tempdir().unwrap();
    validate_and_resolve(
        &valid_config_with_public_url(dir.path().to_path_buf(), Some("https://brenn.example.com")),
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

#[test]
fn public_url_valid_https_with_port_passes() {
    let dir = tempfile::tempdir().unwrap();
    validate_and_resolve(
        &valid_config_with_public_url(
            dir.path().to_path_buf(),
            Some("https://brenn.example.com:8443"),
        ),
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

#[test]
#[should_panic(expected = "server.public_url is not a valid URL")]
fn public_url_malformed_panics() {
    validate_and_resolve(
        &minimal_config_with_public_url(Some("not a url")),
        &IntegrationRegistry::new(vec![]),
        None,
    );
}

#[test]
#[should_panic(expected = "server.public_url contains a control character")]
fn public_url_control_char_panics() {
    validate_and_resolve(
        &minimal_config_with_public_url(Some("https://brenn.example.com\x01")),
        &IntegrationRegistry::new(vec![]),
        None,
    );
}

#[test]
#[should_panic(expected = "server.public_url contains a control character")]
fn public_url_del_char_panics() {
    validate_and_resolve(
        &minimal_config_with_public_url(Some("https://brenn.example.com\x7F")),
        &IntegrationRegistry::new(vec![]),
        None,
    );
}

#[test]
#[should_panic(expected = "server.public_url is not a valid URL")]
fn public_url_empty_string_panics() {
    validate_and_resolve(
        &minimal_config_with_public_url(Some("")),
        &IntegrationRegistry::new(vec![]),
        None,
    );
}

// -----------------------------------------------------------------------
// trusted_proxy_hops tests (H2111)
// -----------------------------------------------------------------------

/// An unset `trusted_proxy_hops` defaults to `0` (no trusted proxy / use TCP peer).
#[test]
fn trusted_proxy_hops_defaults_to_zero() {
    let config: BrennConfig = toml::from_str("[server]\n").unwrap();
    assert_eq!(config.server.trusted_proxy_hops, 0);
}

/// A `trusted_proxy_hops` value above the cap (8) is rejected at config load.
#[test]
#[should_panic(expected = "server.trusted_proxy_hops")]
fn trusted_proxy_hops_over_cap_rejected() {
    let config = BrennConfig {
        apps: vec![AppConfigRaw {
            slug: "pa".to_string(),
            ..Default::default()
        }],
        server: crate::config::server::ServerConfig {
            trusted_proxy_hops: 9,
            ..Default::default()
        },
        ..Default::default()
    };
    validate_and_resolve(&config, &IntegrationRegistry::new(vec![]), None);
}

/// The boundary value `8` (== the cap) is accepted, not rejected. Pins the
/// inclusive upper bound so an accidental `>=` in the check (which would reject
/// a legitimately configured 8 hops) is caught.
#[test]
fn trusted_proxy_hops_cap_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = valid_config_with_public_url(dir.path().to_path_buf(), None);
    config.server.trusted_proxy_hops = 8;
    // Should not panic.
    validate_and_resolve(
        &config,
        &IntegrationRegistry::new(vec![]),
        Some(super::test_runtime_dir()),
    );
}

/// The old `trust_forwarded_headers` key was replaced by `trusted_proxy_hops`
/// (no compat shim). A stale config carrying it must fail to load via
/// `deny_unknown_fields` rather than be silently ignored.
#[test]
fn stale_trust_forwarded_headers_key_rejected() {
    let toml = "[server]\ntrust_forwarded_headers = true\n";
    assert!(
        toml::from_str::<BrennConfig>(toml).is_err(),
        "stale trust_forwarded_headers key must be rejected by deny_unknown_fields"
    );
}

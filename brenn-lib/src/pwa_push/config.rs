//! PWA push configuration types and validation.
//!
//! `PwaPushGlobalConfig` is the `[pwa_push]` TOML block on `BrennConfig`.
//! `AppPwaPushBlock` is the `[app.pwa_push]` block on `AppConfigRaw`.
//! `ResolvedPwaPushConfig` is produced by `resolve_pwa_push_layer` at startup.

use std::path::PathBuf;

use indexmap::IndexMap;
use serde::Deserialize;

use crate::config::AppConfig;
use crate::pwa_push::endpoint_validator::EndpointPolicy;

use super::vapid::VapidKeypair;

/// Default allowlist of known push service hosts.
///
/// Exact hostnames (no wildcards) for FCM, Mozilla autopush, and Apple:
/// - `fcm.googleapis.com` — Chromium-family browsers (Chrome, Edge, Brave, …).
/// - `updates.push.services.mozilla.com` — Firefox production.
/// - `web.push.apple.com` — Safari (macOS 13+ / iOS 16.4+).
fn default_endpoint_host_allowlist() -> Vec<String> {
    vec![
        "fcm.googleapis.com".to_string(),
        "updates.push.services.mozilla.com".to_string(),
        "web.push.apple.com".to_string(),
    ]
}

fn default_endpoint_host_allowlist_enforce() -> bool {
    true
}

/// Global `[pwa_push]` configuration block.
///
/// This block may be absent when no app holds the `PwaPush` grant;
/// in that case `PwaPushGlobalConfig::default()` provides safe zero-values.
#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct PwaPushGlobalConfig {
    /// Path to the VAPID keypair secrets file. Required when any app has the
    /// `PwaPush` grant (`pwa_push_enabled()`). If the file does not exist on
    /// first start, it is generated and written with mode 0600.
    pub keypair_file: Option<PathBuf>,
    /// VAPID `sub` claim — `mailto:...` or `https://...` URI.
    /// Required by FCM and Apple push services when any app gates pwa_push.
    pub subject: Option<String>,
    /// Exact hostnames permitted as push endpoint hosts.
    ///
    /// Defaults to `["fcm.googleapis.com", "updates.push.services.mozilla.com",
    /// "web.push.apple.com"]`. Operators who set this key override the default
    /// entirely (no merge). Self-hosted push services must add their hostname here.
    #[serde(default = "default_endpoint_host_allowlist")]
    pub endpoint_host_allowlist: Vec<String>,
    /// When `true` (the default), endpoints whose host is not in
    /// `endpoint_host_allowlist` are rejected at subscribe and delivery time.
    /// When `false`, mismatches produce a warning but the endpoint is accepted
    /// (IP-block rules still apply). Useful for soft-rollout on existing
    /// deployments.
    #[serde(default = "default_endpoint_host_allowlist_enforce")]
    pub endpoint_host_allowlist_enforce: bool,
}

impl Default for PwaPushGlobalConfig {
    fn default() -> Self {
        Self {
            keypair_file: None,
            subject: None,
            endpoint_host_allowlist: default_endpoint_host_allowlist(),
            endpoint_host_allowlist_enforce: default_endpoint_host_allowlist_enforce(),
        }
    }
}

/// Per-app `[app.pwa_push]` block.
///
/// The legacy `enabled` authorization boolean was removed (access-control design
/// §2.5.1 / §8 decision-2): push authorization is now decided by the app's
/// `AppPolicy` (`AppConfig::pwa_push_enabled()` reads the `PwaPush` grant). The
/// block is retained only for the non-authorization `default_title` delivery
/// setting. Because this struct carries `#[serde(deny_unknown_fields)]`, a stale
/// config that still sets `enabled` under `[app.pwa_push]` now fails to parse —
/// the intended migration-forcing.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default, deny_unknown_fields)]
pub struct AppPwaPushBlock {
    /// Default notification title when `PushSend` omits the `title` field.
    /// Falls back to the app's display name when absent.
    pub default_title: Option<String>,
}

/// Resolved, validated pwa_push configuration produced at startup.
///
/// Only produced when at least one app has the `PwaPush` grant
/// (`pwa_push_enabled()`) *and* all required global fields (`keypair_file`,
/// `subject`) are present.
#[derive(Debug, Clone)]
pub struct ResolvedPwaPushConfig {
    /// VAPID keypair (public key + key pair bytes for signing).
    pub vapid: VapidKeypair,
    /// VAPID `sub` claim URI.
    pub subject: String,
    /// Endpoint host validation policy (allowlist + enforcement flag).
    pub endpoint_policy: EndpointPolicy,
}

/// Validate the global `[pwa_push]` block and load/generate the VAPID keypair
/// iff some app actually has push capability. Returns `None` (keypair never
/// loaded) when no app does. Panics on any config error.
///
/// "Has push capability" is decided by `AppConfig::pwa_push_enabled()`, i.e. the
/// `PwaPush` policy grant — the single source of truth post-access-control
/// Phase 0 (§2.5.1/§2.7). This is deliberately the *same* gate the per-app
/// authorization checks use (`pwa_push_enabled()` at the WS dispatch handlers),
/// so the keypair-required decision and the per-app gate cannot diverge: an app
/// granted `pwa_push` always has a built `PwaPushService`, keeping the
/// `pwa_push_enabled() ⟹ AppState.pwa_push.is_some()` invariant the dispatch
/// `expect()`s rely on structurally true. (This requires the policy to be
/// populated first — the caller runs this *after* the access-policy resolution
/// phase; see `validate_and_resolve`.)
pub fn resolve_pwa_push_layer(
    raw_global: &PwaPushGlobalConfig,
    apps: &IndexMap<String, AppConfig>,
) -> Option<ResolvedPwaPushConfig> {
    let any_enabled = apps.values().any(|a| a.pwa_push_enabled());

    if !any_enabled {
        return None;
    }

    let subject = match raw_global.subject.as_deref() {
        None => panic!(
            "config: [pwa_push].subject is required when any app has the pwa_push grant \
             (must be a mailto: or https:// URI)"
        ),
        Some(s) if s.trim().is_empty() => panic!(
            "config: [pwa_push].subject must not be empty or whitespace-only \
             (must be a mailto: or https:// URI)"
        ),
        Some(s) => s.trim().to_string(),
    };
    assert!(
        subject.starts_with("mailto:") || subject.starts_with("https://"),
        "config: [pwa_push].subject must be a mailto: or https:// URI, got {subject:?}"
    );

    let keypair_file = raw_global.keypair_file.as_ref().unwrap_or_else(|| {
        panic!("config: [pwa_push].keypair_file is required when any app has the pwa_push grant")
    });

    let vapid = super::vapid::load_or_generate(keypair_file);

    let endpoint_policy = EndpointPolicy::new(
        raw_global.endpoint_host_allowlist.clone(),
        raw_global.endpoint_host_allowlist_enforce,
    );

    Some(ResolvedPwaPushConfig {
        vapid,
        subject,
        endpoint_policy,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::config::{AppConfig, FrontmatterRenderConfig, PathMapper};
    use crate::pwa_push::config::AppPwaPushBlock;

    fn make_app(slug: &str, pwa_push: Option<AppPwaPushBlock>, push_authorized: bool) -> AppConfig {
        let tempdir = tempfile::tempdir().expect("tempdir");
        // `resolve_pwa_push_layer` gates on the PwaPush *grant*
        // (`pwa_push_enabled()`). The legacy `[app.pwa_push].enabled` boolean was
        // removed (access-control §2.5.1), so push authorization is now an explicit
        // grant decoupled from block presence: `push_authorized` drives the grant
        // exactly as the operator's `pwa_push` grant would.
        let mut policy = crate::access::AppPolicy::default();
        if push_authorized {
            policy.grants.insert(crate::access::AppCapability::PwaPush);
        }
        // AppConfig is a large struct; use a minimal builder approach.
        AppConfig {
            slug: slug.to_string(),
            name: slug.to_string(),
            description: String::new(),
            icon: String::new(),
            working_dir: tempdir.path().to_path_buf(),
            model: "claude-sonnet".to_string(),
            single_instance: false,
            singleton: false,
            persistent: false,
            idle_timeout: None,
            compaction: None,
            idle_hook_secs: 0,
            allowed_users: vec![],
            disabled_tools: vec![],
            mcp_servers: HashMap::new(),
            multiuser: false,
            prefix_username: false,
            prefix_timestamp: false,
            prefix_device: true,
            path_mapper: PathMapper::Identity,
            container_spawn: None,
            start_hooks: Default::default(),
            post_pull_hooks: Default::default(),
            startup_hooks: Default::default(),
            cc_extra_args: vec![],
            approval_rules: vec![],
            attachment_targets: vec![],
            integrations: HashMap::new(),
            mounts: vec![],
            history_replay_limit: 2000,
            frontmatter: FrontmatterRenderConfig::default(),
            state_dir: tempdir.path().to_path_buf(),
            messaging: None,
            messaging_default_send_budget: 100,
            policy,
            pwa_push,
            webhook_subscriptions: vec![],
            mqtt_subscriptions: vec![],
        }
    }

    fn make_apps(entries: Vec<AppConfig>) -> IndexMap<String, AppConfig> {
        let mut map = IndexMap::new();
        for app in entries {
            map.insert(app.slug.clone(), app);
        }
        map
    }

    #[test]
    fn no_apps_gate_pwa_push_returns_none_even_with_global_block_set() {
        let global = PwaPushGlobalConfig {
            keypair_file: Some("/tmp/vapid.json".into()),
            subject: Some("mailto:admin@example.com".to_string()),
            ..Default::default()
        };
        // Neither app is push-authorized: app1 has no block and no grant; app2
        // has a delivery-settings block present but no `PwaPush` grant (the
        // legacy "block present ⇒ enabled" coupling was removed, §2.5.1).
        let apps = make_apps(vec![
            make_app("app1", None, false),
            make_app(
                "app2",
                Some(AppPwaPushBlock {
                    default_title: None,
                }),
                false,
            ),
        ]);
        let result = resolve_pwa_push_layer(&global, &apps);
        assert!(result.is_none());
    }

    #[test]
    #[should_panic(expected = "[pwa_push].subject is required")]
    fn apps_gate_pwa_push_but_no_subject_panics() {
        let global = PwaPushGlobalConfig {
            keypair_file: Some("/tmp/vapid.json".into()),
            subject: None,
            ..Default::default()
        };
        let apps = make_apps(vec![make_app(
            "graf",
            Some(AppPwaPushBlock {
                default_title: None,
            }),
            true,
        )]);
        let _ = resolve_pwa_push_layer(&global, &apps);
    }

    #[test]
    #[should_panic(expected = "must not be empty or whitespace-only")]
    fn apps_gate_pwa_push_empty_subject_panics() {
        let global = PwaPushGlobalConfig {
            keypair_file: Some("/tmp/vapid.json".into()),
            subject: Some("   ".to_string()),
            ..Default::default()
        };
        let apps = make_apps(vec![make_app(
            "graf",
            Some(AppPwaPushBlock {
                default_title: None,
            }),
            true,
        )]);
        let _ = resolve_pwa_push_layer(&global, &apps);
    }

    #[test]
    #[should_panic(expected = "must be a mailto: or https://")]
    fn subject_must_be_mailto_or_https() {
        let global = PwaPushGlobalConfig {
            keypair_file: Some("/tmp/vapid.json".into()),
            subject: Some("ftp://bad.example.com".to_string()),
            ..Default::default()
        };
        let apps = make_apps(vec![make_app(
            "graf",
            Some(AppPwaPushBlock {
                default_title: None,
            }),
            true,
        )]);
        let _ = resolve_pwa_push_layer(&global, &apps);
    }

    #[test]
    #[should_panic(expected = "[pwa_push].keypair_file is required")]
    fn apps_gate_pwa_push_but_no_keypair_file_panics() {
        let global = PwaPushGlobalConfig {
            keypair_file: None,
            subject: Some("mailto:admin@example.com".to_string()),
            ..Default::default()
        };
        let apps = make_apps(vec![make_app(
            "graf",
            Some(AppPwaPushBlock {
                default_title: None,
            }),
            true,
        )]);
        let _ = resolve_pwa_push_layer(&global, &apps);
    }

    #[test]
    fn apps_gate_pwa_push_subject_present_resolves_ok() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let keypair_path = tempdir.path().join("vapid.json");
        let global = PwaPushGlobalConfig {
            keypair_file: Some(keypair_path),
            subject: Some("mailto:admin@example.com".to_string()),
            ..Default::default()
        };
        let apps = make_apps(vec![make_app(
            "graf",
            Some(AppPwaPushBlock {
                default_title: None,
            }),
            true,
        )]);
        let result = resolve_pwa_push_layer(&global, &apps);
        assert!(result.is_some());
        let resolved = result.unwrap();
        assert_eq!(resolved.subject, "mailto:admin@example.com");
        // Public key should be 87 base64url chars (65-byte uncompressed P-256 key).
        assert_eq!(resolved.vapid.public_b64url.len(), 87);
    }

    #[test]
    fn apps_gate_pwa_push_resolve_round_trips_same_public_key() {
        // Calling resolve_pwa_push_layer twice on the same keypair_file must
        // return the same public key (second call reads the file; first
        // generates it). Guards against parse / consistency-check regressions.
        let tempdir = tempfile::tempdir().expect("tempdir");
        let keypair_path = tempdir.path().join("vapid.json");
        let global = PwaPushGlobalConfig {
            keypair_file: Some(keypair_path),
            subject: Some("mailto:admin@example.com".to_string()),
            ..Default::default()
        };
        let apps = make_apps(vec![make_app(
            "graf",
            Some(AppPwaPushBlock {
                default_title: None,
            }),
            true,
        )]);
        let r1 = resolve_pwa_push_layer(&global, &apps).unwrap();
        let r2 = resolve_pwa_push_layer(&global, &apps).unwrap();
        assert_eq!(
            r1.vapid.public_b64url, r2.vapid.public_b64url,
            "round-trip must return same public key"
        );
    }

    #[test]
    fn https_subject_also_accepted() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let keypair_path = tempdir.path().join("vapid.json");
        let global = PwaPushGlobalConfig {
            keypair_file: Some(keypair_path),
            subject: Some("https://example.com/push".to_string()),
            ..Default::default()
        };
        let apps = make_apps(vec![make_app(
            "graf",
            Some(AppPwaPushBlock {
                default_title: None,
            }),
            true,
        )]);
        let result = resolve_pwa_push_layer(&global, &apps).unwrap();
        assert_eq!(result.subject, "https://example.com/push");
    }

    #[test]
    fn serde_default_allowlist_when_key_absent_from_toml() {
        // Exercises the #[serde(default = "default_endpoint_host_allowlist")] path:
        // when the TOML block omits endpoint_host_allowlist, serde must call the
        // default function. Constructing PwaPushGlobalConfig::default() in Rust does
        // NOT exercise this code path.
        let toml_str = "[pwa_push]\n";
        let wrapper: toml::Table = toml::from_str(toml_str).expect("parse toml");
        let global: PwaPushGlobalConfig = wrapper["pwa_push"]
            .clone()
            .try_into()
            .expect("deserialize PwaPushGlobalConfig");
        assert!(
            global.endpoint_host_allowlist_enforce,
            "enforce must default to true"
        );
        assert!(
            global
                .endpoint_host_allowlist
                .contains(&"fcm.googleapis.com".to_string()),
            "default allowlist must contain FCM"
        );
        assert!(
            global
                .endpoint_host_allowlist
                .contains(&"updates.push.services.mozilla.com".to_string()),
            "default allowlist must contain Mozilla"
        );
        assert!(
            global
                .endpoint_host_allowlist
                .contains(&"web.push.apple.com".to_string()),
            "default allowlist must contain Apple"
        );
        assert_eq!(global.endpoint_host_allowlist.len(), 3);
    }

    #[test]
    fn default_allowlist_contains_three_vendor_hosts() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let keypair_path = tempdir.path().join("vapid.json");
        // Use Default::default() — omitting endpoint_host_allowlist key.
        let global = PwaPushGlobalConfig {
            keypair_file: Some(keypair_path),
            subject: Some("mailto:admin@example.com".to_string()),
            ..Default::default()
        };
        let apps = make_apps(vec![make_app(
            "graf",
            Some(AppPwaPushBlock {
                default_title: None,
            }),
            true,
        )]);
        let result = resolve_pwa_push_layer(&global, &apps).unwrap();
        assert!(result.endpoint_policy.enforce_allowlist);
        let list = &result.endpoint_policy.allowlist;
        assert!(list.contains(&"fcm.googleapis.com".to_string()));
        assert!(list.contains(&"updates.push.services.mozilla.com".to_string()));
        assert!(list.contains(&"web.push.apple.com".to_string()));
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn explicit_empty_allowlist_with_enforce_false_overrides_default() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let keypair_path = tempdir.path().join("vapid.json");
        let global = PwaPushGlobalConfig {
            keypair_file: Some(keypair_path),
            subject: Some("mailto:admin@example.com".to_string()),
            endpoint_host_allowlist: vec![],
            endpoint_host_allowlist_enforce: false,
        };
        let apps = make_apps(vec![make_app(
            "graf",
            Some(AppPwaPushBlock {
                default_title: None,
            }),
            true,
        )]);
        let result = resolve_pwa_push_layer(&global, &apps).unwrap();
        assert!(!result.endpoint_policy.enforce_allowlist);
        assert!(result.endpoint_policy.allowlist.is_empty());
    }
}

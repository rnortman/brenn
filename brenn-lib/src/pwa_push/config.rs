//! PWA push configuration types and validation.
//!
//! `PwaPushGlobalConfig` is the `pwa_push` section on `BrennConfig`.
//! `AppPwaPushBlock` is the `[app.pwa_push]` block on `AppConfigRaw`.
//! `ResolvedPwaPushConfig` is produced by `resolve_pwa_push_layer` at startup.
//! `EndpointPolicy` is the host-allowlist policy data.

use std::path::PathBuf;

use super::vapid::VapidKeypair;

/// Default allowlist of known push service hosts.
///
/// Exact hostnames (no wildcards) for FCM, Mozilla autopush, and Apple:
/// - `fcm.googleapis.com` — Chromium-family browsers (Chrome, Edge, Brave, …).
/// - `updates.push.services.mozilla.com` — Firefox production.
/// - `web.push.apple.com` — Safari (macOS 13+ / iOS 16.4+).
pub(crate) fn default_endpoint_host_allowlist() -> Vec<String> {
    vec![
        "fcm.googleapis.com".to_string(),
        "updates.push.services.mozilla.com".to_string(),
        "web.push.apple.com".to_string(),
    ]
}

pub(crate) fn default_endpoint_host_allowlist_enforce() -> bool {
    true
}

/// Global `[pwa_push]` configuration block.
///
/// This block may be absent when no app holds the `PwaPush` grant;
/// in that case `PwaPushGlobalConfig::default()` provides safe zero-values.
#[derive(Debug, Clone, PartialEq)]
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
    pub endpoint_host_allowlist: Vec<String>,
    /// When `true` (the default), endpoints whose host is not in
    /// `endpoint_host_allowlist` are rejected at subscribe and delivery time.
    /// When `false`, mismatches produce a warning but the endpoint is accepted
    /// (IP-block rules still apply). Useful for soft-rollout on existing
    /// deployments.
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
/// Push authorization is decided by the app's `AppPolicy`
/// (`AppConfig::pwa_push_enabled()` reads the `PwaPush` grant). This block
/// carries only the non-authorization `default_title` delivery setting.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AppPwaPushBlock {
    /// Default notification title when `PushSend` omits the `title` field.
    /// Falls back to the app's display name when absent.
    pub default_title: Option<String>,
}

/// Policy controlling which endpoint hosts are accepted.
#[derive(Debug, Clone)]
pub struct EndpointPolicy {
    /// Exact lowercased hostnames that are permitted when `enforce_allowlist = true`.
    pub allowlist: Vec<String>,
    /// When `true`, any host not in `allowlist` (including IP literals) is rejected.
    /// When `false`, mismatches are logged as warnings but the host is accepted
    /// (subject to the IP-block rules, which always apply).
    pub enforce_allowlist: bool,
}

impl EndpointPolicy {
    /// Construct an `EndpointPolicy`, lowercasing each allowlist entry.
    ///
    /// # Panics
    ///
    /// - If any allowlist entry is empty or whitespace-only (almost certainly a
    ///   config error).
    /// - If `enforce_allowlist = true` and the allowlist is empty. This
    ///   combination would silently reject every legitimate endpoint, which is
    ///   almost certainly a typo. To disable allowlist enforcement while keeping
    ///   only the IP-block rules, set `enforce_allowlist = false`.
    pub fn new(allowlist: Vec<String>, enforce_allowlist: bool) -> Self {
        let lowercased: Vec<String> = allowlist
            .into_iter()
            .map(|h| {
                let trimmed = h.trim().to_string();
                assert!(
                    !trimmed.is_empty(),
                    "config: endpoint_host_allowlist entry must not be empty or whitespace-only"
                );
                trimmed.to_lowercase()
            })
            .collect();

        if enforce_allowlist {
            assert!(
                !lowercased.is_empty(),
                "config: endpoint_host_allowlist_enforce = true but endpoint_host_allowlist is \
                 empty; this would reject every endpoint. To disable allowlist enforcement and \
                 rely only on IP-block rules, set endpoint_host_allowlist_enforce = false."
            );
        }

        Self {
            allowlist: lowercased,
            enforce_allowlist,
        }
    }
}

/// Resolved, validated pwa_push configuration produced at startup.
///
/// Produced iff the global `[pwa_push]` section is declared, i.e. `subject` is
/// set.
#[derive(Debug, Clone)]
pub struct ResolvedPwaPushConfig {
    /// VAPID keypair (public key + key pair bytes for signing).
    pub vapid: VapidKeypair,
    /// VAPID `sub` claim URI.
    pub subject: String,
    /// Endpoint host validation policy (allowlist + enforcement flag).
    pub endpoint_policy: EndpointPolicy,
}

/// Validate the global `[pwa_push]` block and load or generate the VAPID
/// keypair iff the section is declared. Returns `None` (keypair never loaded)
/// when it is not. Panics on any config error.
///
/// "Declared" is `subject` being set: the section's two required keys are
/// `subject` and `keypair_file`, and one without the other is a config error.
///
/// The layer's existence is deliberately a property of the *document's*
/// section, not of any app's `PwaPush` grant. Grants converge at reload while
/// the layer does not, so gating the layer on a grant would let the first app
/// to gain one arm the browser-reachable `expect` at the WS dispatch handlers
/// (`pwa_push_enabled() => AppState.pwa_push.is_some()`). The other side of that
/// invariant — a grant with no declared section — is refused in `resolve_apps`.
pub fn resolve_pwa_push_layer(raw_global: &PwaPushGlobalConfig) -> Option<ResolvedPwaPushConfig> {
    let subject = match raw_global.subject.as_deref() {
        None => return None,
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
        panic!("config: [pwa_push].keypair_file is required when [pwa_push] is declared")
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

    use super::*;

    // --- EndpointPolicy constructor panics ---

    #[test]
    #[should_panic(
        expected = "endpoint_host_allowlist_enforce = true but endpoint_host_allowlist is empty"
    )]
    fn policy_panics_on_enforce_true_empty_allowlist() {
        let _ = EndpointPolicy::new(vec![], true);
    }

    #[test]
    #[should_panic(expected = "must not be empty or whitespace-only")]
    fn policy_panics_on_empty_entry() {
        let _ = EndpointPolicy::new(vec!["".to_string()], false);
    }

    #[test]
    #[should_panic(expected = "must not be empty or whitespace-only")]
    fn policy_panics_on_whitespace_entry() {
        let _ = EndpointPolicy::new(vec!["   ".to_string()], false);
    }
    /// A declared `[pwa_push]` section is what produces the layer, whatever any
    /// app's grants say: no app is consulted at all.
    #[test]
    fn undeclared_section_returns_none() {
        // `keypair_file` alone is not a declaration — `subject` is.
        let global = PwaPushGlobalConfig {
            keypair_file: Some("/tmp/vapid.json".into()),
            subject: None,
            ..Default::default()
        };
        assert!(resolve_pwa_push_layer(&global).is_none());
    }

    #[test]
    fn declared_section_resolves_with_no_granted_app() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let global = PwaPushGlobalConfig {
            keypair_file: Some(tempdir.path().join("vapid.json")),
            subject: Some("mailto:admin@example.com".to_string()),
            ..Default::default()
        };
        let resolved = resolve_pwa_push_layer(&global)
            .expect("a declared section has a layer for the life of the process");
        assert_eq!(resolved.subject, "mailto:admin@example.com");
    }

    #[test]
    #[should_panic(expected = "must not be empty or whitespace-only")]
    fn empty_subject_panics() {
        let global = PwaPushGlobalConfig {
            keypair_file: Some("/tmp/vapid.json".into()),
            subject: Some("   ".to_string()),
            ..Default::default()
        };
        let _ = resolve_pwa_push_layer(&global);
    }

    #[test]
    #[should_panic(expected = "must be a mailto: or https://")]
    fn subject_must_be_mailto_or_https() {
        let global = PwaPushGlobalConfig {
            keypair_file: Some("/tmp/vapid.json".into()),
            subject: Some("ftp://bad.example.com".to_string()),
            ..Default::default()
        };
        let _ = resolve_pwa_push_layer(&global);
    }

    #[test]
    #[should_panic(expected = "[pwa_push].keypair_file is required")]
    fn declared_section_without_keypair_file_panics() {
        let global = PwaPushGlobalConfig {
            keypair_file: None,
            subject: Some("mailto:admin@example.com".to_string()),
            ..Default::default()
        };
        let _ = resolve_pwa_push_layer(&global);
    }

    #[test]
    fn subject_present_resolves_ok() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let keypair_path = tempdir.path().join("vapid.json");
        let global = PwaPushGlobalConfig {
            keypair_file: Some(keypair_path),
            subject: Some("mailto:admin@example.com".to_string()),
            ..Default::default()
        };
        let result = resolve_pwa_push_layer(&global);
        assert!(result.is_some());
        let resolved = result.unwrap();
        assert_eq!(resolved.subject, "mailto:admin@example.com");
        // Public key should be 87 base64url chars (65-byte uncompressed P-256 key).
        assert_eq!(resolved.vapid.public_b64url.len(), 87);
    }

    #[test]
    fn resolve_round_trips_same_public_key() {
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
        let r1 = resolve_pwa_push_layer(&global).unwrap();
        let r2 = resolve_pwa_push_layer(&global).unwrap();
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
        let result = resolve_pwa_push_layer(&global).unwrap();
        assert_eq!(result.subject, "https://example.com/push");
    }

    #[test]
    fn default_allowlist_contains_three_vendor_hosts() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let keypair_path = tempdir.path().join("vapid.json");
        let global = PwaPushGlobalConfig {
            keypair_file: Some(keypair_path),
            subject: Some("mailto:admin@example.com".to_string()),
            ..Default::default()
        };
        let result = resolve_pwa_push_layer(&global).unwrap();
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
        let result = resolve_pwa_push_layer(&global).unwrap();
        assert!(!result.endpoint_policy.enforce_allowlist);
        assert!(result.endpoint_policy.allowlist.is_empty());
    }
}

//! SSRF-safe endpoint URL validation for PWA push subscriptions.
//!
//! This module validates that a push endpoint URL:
//! 1. Parses as an absolute HTTPS URL.
//! 2. Has a non-empty host component.
//! 3. Does not refer to any private/reserved IP address space.
//! 4. Matches the configured push-service allowlist (when enforced).
//!
//! **DNS resolution is intentionally NOT performed.** A DNS name that resolves
//! to a private IP at delivery time is a residual risk. The primary mitigation
//! is the allowlist: with `enforce_allowlist = true` (the default), only the
//! three known push-service hostnames are accepted, so a DNS-rebinding attacker
//! would also need to clear the allowlist — which they cannot.
//!
//! **IPv4-mapped IPv6** (`::ffff:a.b.c.d`) is explicitly unwrapped and re-checked
//! against the IPv4 rules. **Teredo** (`2001::/32`) and **6to4** (`2002::/16`) are
//! blocked outright because they tunnel arbitrary IPv4 addresses inside IPv6 literals.

use std::net::IpAddr;

use brenn_lib::pwa_push::config::EndpointPolicy;
use brenn_lib::special_address::{is_special_purpose, is_special_use_name};
use url::Host;

/// Reason an endpoint URL was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// URL could not be parsed as an absolute URL, or the parsed URL has no host.
    UrlParse,
    /// Scheme is not exactly `https`.
    Scheme,
    /// Host is an IP literal in a blocked range, or a local-domain DNS name
    /// (`localhost`, `*.localhost`, `local`, `*.local`).
    PrivateHost,
    /// `enforce_allowlist = true` and the host is not in the configured allowlist.
    AllowlistMiss,
}

impl RejectReason {
    /// Short machine-readable code for logging and fail2ban matching.
    pub fn code(&self) -> &'static str {
        match self {
            Self::UrlParse => "url_parse",
            Self::Scheme => "scheme",
            Self::PrivateHost => "private_host",
            Self::AllowlistMiss => "allowlist_miss",
        }
    }
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

/// A push endpoint URL that has been validated by [`validate_endpoint`].
///
/// This newtype is the only way to produce a value accepted by
/// `upsert_subscription`, ensuring the invariant "endpoints stored in the DB
/// were validated" is enforced at compile time rather than by reviewer discipline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedEndpoint(String);

impl ValidatedEndpoint {
    /// Return the validated, normalized URL as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Construct a `ValidatedEndpoint` from a raw string **without validation**.
    ///
    /// Only available under `#[cfg(test)]` or when the `testutils` cargo feature
    /// is enabled. The `testutils` feature must never be enabled in production
    /// builds. Production code must go through `validate_endpoint` or
    /// `validate_push_subscribe_fields`.
    #[cfg(any(test, feature = "testutils"))]
    pub fn for_testing(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }
}

/// Validate a push endpoint URL against the given policy.
///
/// Returns `Ok(ValidatedEndpoint)` where the inner string is the URL as
/// serialized by `url::Url` after stripping any userinfo (credentials).
/// Storing and forwarding the normalized form — rather than the raw input —
/// ensures that the same parser is used at both validate-time and
/// deliver-time, eliminating parser-confusion SSRF bypass vectors
/// (`url::Url` vs `http::Uri`).
///
/// # Panics
///
/// Panics if `url::Host` returns a variant not known at compile time. This
/// represents a `url` crate API change (not user input) and aligns with the
/// project's fail-fast principle.
pub fn validate_endpoint(
    raw: &str,
    policy: &EndpointPolicy,
) -> Result<ValidatedEndpoint, RejectReason> {
    // Rule 1: must parse as an absolute URL.
    let mut url = url::Url::parse(raw).map_err(|_| RejectReason::UrlParse)?;

    // Rule 2: scheme must be exactly "https".
    if url.scheme() != "https" {
        return Err(RejectReason::Scheme);
    }

    // Rule 3: must have a non-empty host.
    let host = url.host().ok_or(RejectReason::UrlParse)?;

    // Rules 4 & 5: host-based checks.
    match &host {
        Host::Ipv4(v4) => {
            reject_special(IpAddr::V4(*v4))?;
        }
        Host::Ipv6(v6) => {
            reject_special(IpAddr::V6(*v6))?;
        }
        Host::Domain(name) => {
            check_domain(name)?;
        }
    }

    // Rule 6: allowlist check.
    check_allowlist(&host, policy)?;

    // Strip userinfo (credentials) before returning the normalized URL.
    // `url::Url` accepts URLs with embedded credentials (`user:pass@host`); the
    // validation above operates on `host()` alone and is not affected, but the
    // credentials would be forwarded verbatim to the push service if retained.
    // Stripping them here is defensive hygiene.
    let _ = url.set_username("");
    let _ = url.set_password(None);

    // Return the url::Url-normalized string wrapped in ValidatedEndpoint.
    // The caller MUST store and forward this normalized form (not the raw
    // input) so that deliver-time parsing (`http::Uri`) sees the same
    // authority as the validator did.
    Ok(ValidatedEndpoint(url.into()))
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Reject an IP literal in any IANA special-purpose range, IPv4-mapped IPv6
/// judged as its IPv4 address.
fn reject_special(addr: IpAddr) -> Result<(), RejectReason> {
    if is_special_purpose(addr) {
        Err(RejectReason::PrivateHost)
    } else {
        Ok(())
    }
}

/// Reject a DNS name that never denotes a public host.
///
/// This runs **before** the allowlist, so a listed host under such a name is
/// still rejected; no push service lives under one.
fn check_domain(name: &str) -> Result<(), RejectReason> {
    if is_special_use_name(name) {
        Err(RejectReason::PrivateHost)
    } else {
        Ok(())
    }
}

/// Allowlist check.
///
/// - `enforce = true`: host must be a `Domain` whose lowercased form is in the
///   allowlist. IP-literal hosts always fail (they can never match an exact
///   hostname entry).
/// - `enforce = false`: non-listed hosts are accepted (IP-block rules still
///   apply). If the allowlist is non-empty and the host misses, emit a warning
///   so operators see soft-rollout signal.
fn check_allowlist(host: &Host<&str>, policy: &EndpointPolicy) -> Result<(), RejectReason> {
    if policy.allowlist.is_empty() && !policy.enforce_allowlist {
        return Ok(());
    }

    match host {
        Host::Domain(name) => {
            // Allowlist entries are pre-lowercased at construction (EndpointPolicy::new).
            // Hosts from url::Url are ASCII (IDNA-normalized). ASCII case-insensitive
            // comparison avoids heap allocation vs to_lowercase().
            let in_list = policy
                .allowlist
                .iter()
                .any(|e| e.eq_ignore_ascii_case(name));
            if in_list {
                return Ok(());
            }
            if policy.enforce_allowlist {
                return Err(RejectReason::AllowlistMiss);
            }
            // enforce = false, non-empty allowlist: warn-only.
            if !policy.allowlist.is_empty() {
                tracing::warn!(
                    host = %name,
                    "endpoint_validator: host missed allowlist (enforce=false); allowing as soft-rollout"
                );
            }
            Ok(())
        }
        Host::Ipv4(_) | Host::Ipv6(_) => {
            // IP literals can never match an exact hostname allowlist entry.
            if policy.enforce_allowlist {
                return Err(RejectReason::AllowlistMiss);
            }
            // Non-enforced: IP passed IP-block rules already; warn and accept.
            if !policy.allowlist.is_empty() {
                tracing::warn!(
                    "endpoint_validator: IP-literal host passed IP-block rules but missed \
                     allowlist (enforce=false); allowing as soft-rollout"
                );
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Test helpers (shared with sibling test modules via pub(crate))
// ---------------------------------------------------------------------------

/// Shared test helpers for `EndpointPolicy` construction.
/// Isolated from `mod tests` so the test suite itself can remain private
/// while these factories are accessible from `pwa_push::db` tests.
#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;

    pub(crate) fn enforced_policy() -> EndpointPolicy {
        EndpointPolicy::new(
            vec![
                "fcm.googleapis.com".to_string(),
                "updates.push.services.mozilla.com".to_string(),
                "web.push.apple.com".to_string(),
            ],
            true,
        )
    }

    pub(crate) fn unenforced_policy() -> EndpointPolicy {
        EndpointPolicy::new(
            vec![
                "fcm.googleapis.com".to_string(),
                "updates.push.services.mozilla.com".to_string(),
                "web.push.apple.com".to_string(),
            ],
            false,
        )
    }

    pub(crate) fn empty_unenforced_policy() -> EndpointPolicy {
        EndpointPolicy::new(vec![], false)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use test_helpers::{empty_unenforced_policy, enforced_policy, unenforced_policy};

    // --- IPv4 private/reserved range rejections ---

    #[test]
    fn reject_ipv4_loopback() {
        assert_eq!(
            validate_endpoint("https://127.0.0.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_private_10() {
        assert_eq!(
            validate_endpoint("https://10.0.0.5/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_private_172_16() {
        assert_eq!(
            validate_endpoint("https://172.16.0.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_private_192_168() {
        assert_eq!(
            validate_endpoint("https://192.168.1.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_cgnat() {
        assert_eq!(
            validate_endpoint("https://100.64.0.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_link_local() {
        assert_eq!(
            validate_endpoint(
                "https://169.254.169.254/latest/meta-data",
                &enforced_policy()
            ),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_ietf_proto_assignments() {
        assert_eq!(
            validate_endpoint("https://192.0.0.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_documentation_1() {
        assert_eq!(
            validate_endpoint("https://192.0.2.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_documentation_2() {
        assert_eq!(
            validate_endpoint("https://198.51.100.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_documentation_3() {
        assert_eq!(
            validate_endpoint("https://203.0.113.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_benchmarking() {
        assert_eq!(
            validate_endpoint("https://198.18.0.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_multicast() {
        assert_eq!(
            validate_endpoint("https://224.0.0.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_class_e() {
        assert_eq!(
            validate_endpoint("https://240.0.0.1/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_broadcast() {
        assert_eq!(
            validate_endpoint("https://255.255.255.255/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv4_unspecified() {
        assert_eq!(
            validate_endpoint("https://0.0.0.0/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    // --- IPv6 range rejections ---

    #[test]
    fn reject_ipv6_unspecified() {
        assert_eq!(
            validate_endpoint("https://[::]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_loopback() {
        assert_eq!(
            validate_endpoint("https://[::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_mapped_loopback() {
        // ::ffff:127.0.0.1 must be unwrapped and re-checked as 127.0.0.1.
        assert_eq!(
            validate_endpoint("https://[::ffff:127.0.0.1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_nat64() {
        assert_eq!(
            validate_endpoint("https://[64:ff9b::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_discard_only() {
        assert_eq!(
            validate_endpoint("https://[100::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_teredo() {
        assert_eq!(
            validate_endpoint("https://[2001::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_benchmarking() {
        assert_eq!(
            validate_endpoint("https://[2001:2::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_documentation() {
        assert_eq!(
            validate_endpoint("https://[2001:db8::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_6to4_of_loopback() {
        // 2002:7f00:1:: encodes 127.0.0.1 via 6to4; reject outright on 2002::/16.
        assert_eq!(
            validate_endpoint("https://[2002:7f00:1::]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_ula_fc() {
        assert_eq!(
            validate_endpoint("https://[fc00::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_ula_fd() {
        assert_eq!(
            validate_endpoint("https://[fd00::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_link_local() {
        assert_eq!(
            validate_endpoint("https://[fe80::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_ipv6_multicast() {
        assert_eq!(
            validate_endpoint("https://[ff00::1]/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    // --- Scheme rejections ---

    #[test]
    fn reject_scheme_http() {
        assert_eq!(
            validate_endpoint("http://fcm.googleapis.com/x", &enforced_policy()),
            Err(RejectReason::Scheme)
        );
    }

    #[test]
    fn reject_scheme_ftp() {
        assert_eq!(
            validate_endpoint("ftp://fcm.googleapis.com/x", &enforced_policy()),
            Err(RejectReason::Scheme)
        );
    }

    // --- URL parse failures ---

    #[test]
    fn reject_parse_not_a_url() {
        assert_eq!(
            validate_endpoint("not a url", &enforced_policy()),
            Err(RejectReason::UrlParse)
        );
    }

    #[test]
    fn reject_parse_scheme_only() {
        assert_eq!(
            validate_endpoint("https://", &enforced_policy()),
            Err(RejectReason::UrlParse)
        );
    }

    #[test]
    fn reject_slash_slash_slash_path_as_allowlist_miss() {
        // "https:///path" does NOT produce a UrlParse error: the WHATWG URL
        // parser (used by the url crate) interprets the empty authority as
        // host="path" (the path segment becomes the host). The host "path" is a
        // single-label name (no dot) and is now rejected as PrivateHost by the
        // single-label blocklist rule. Correct security behavior either way.
        //
        // Note: the UrlParse path (genuinely empty host) is covered by
        // reject_parse_scheme_only ("https://").
        assert_eq!(
            validate_endpoint("https:///path", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    // --- Local-domain rejections ---

    #[test]
    fn reject_localhost_exact() {
        assert_eq!(
            validate_endpoint("https://localhost/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_localhost_uppercase() {
        assert_eq!(
            validate_endpoint("https://LOCALHOST/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_localhost_subdomain() {
        assert_eq!(
            validate_endpoint("https://foo.localhost/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_dot_local() {
        assert_eq!(
            validate_endpoint("https://printer.local/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_bare_local() {
        // Bare "local" is the mDNS TLD; must be blocked like "localhost".
        assert_eq!(
            validate_endpoint("https://local/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    // --- New domain blocklist entries (cycle 22) ---

    #[test]
    fn reject_bare_single_label_hostname() {
        // Any single-label hostname (no dot) must be rejected — could resolve
        // to a private address via /etc/hosts or mDNS.
        assert_eq!(
            validate_endpoint("https://intranet/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_dot_home_arpa() {
        // check_domain runs before check_allowlist so enforced_policy() is fine here.
        assert_eq!(
            validate_endpoint("https://device.home.arpa/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_dot_lan() {
        assert_eq!(
            validate_endpoint("https://printer.lan/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_dot_internal() {
        assert_eq!(
            validate_endpoint("https://api.internal/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_dot_corp() {
        assert_eq!(
            validate_endpoint("https://vpn.corp/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_dot_intranet() {
        assert_eq!(
            validate_endpoint("https://portal.intranet/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_dot_private() {
        assert_eq!(
            validate_endpoint("https://host.private/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_dot_localdomain() {
        assert_eq!(
            validate_endpoint("https://box.localdomain/x", &enforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    #[test]
    fn reject_trailing_dot_localhost() {
        // "localhost." (FQDN-absolute form) must not bypass the blocklist.
        // The trailing dot is stripped before suffix checks, collapsing it to
        // the "localhost" case caught by the single-label guard.
        assert_eq!(
            validate_endpoint("https://localhost./x", &empty_unenforced_policy()),
            Err(RejectReason::PrivateHost)
        );
    }

    // --- Allowlist rejections ---

    #[test]
    fn reject_allowlist_miss_public_domain() {
        assert_eq!(
            validate_endpoint("https://evil.example.com/x", &enforced_policy()),
            Err(RejectReason::AllowlistMiss)
        );
    }

    #[test]
    fn reject_allowlist_miss_public_ip_when_enforced() {
        // Public IP: passes IP-block rules but fails allowlist when enforced.
        assert_eq!(
            validate_endpoint("https://8.8.8.8/x", &enforced_policy()),
            Err(RejectReason::AllowlistMiss)
        );
    }

    // --- Accepted cases ---

    #[test]
    fn accept_fcm() {
        assert!(
            validate_endpoint(
                "https://fcm.googleapis.com/fcm/send/abc",
                &enforced_policy()
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_mozilla_autopush() {
        assert!(
            validate_endpoint(
                "https://updates.push.services.mozilla.com/wpush/v2/abc",
                &enforced_policy()
            )
            .is_ok()
        );
    }

    #[test]
    fn accept_apple_push() {
        assert!(validate_endpoint("https://web.push.apple.com/q/abc", &enforced_policy()).is_ok());
    }

    #[test]
    fn accept_case_insensitive_host() {
        // Allowlist match is case-insensitive.
        assert!(validate_endpoint("https://FCM.GoogleAPIs.com/x", &enforced_policy()).is_ok());
    }

    #[test]
    fn accept_public_host_empty_allowlist_enforce_false() {
        // Empty allowlist + enforce=false: IP-block rules only, no allowlist filtering.
        assert!(validate_endpoint("https://example.com/x", &empty_unenforced_policy()).is_ok());
    }

    #[test]
    fn accept_non_listed_public_host_enforce_false() {
        // Non-listed host accepted in non-enforced mode (warn-only).
        assert!(
            validate_endpoint("https://custom.push.example.org/x", &unenforced_policy()).is_ok()
        );
    }

    #[test]
    fn accept_returns_normalized_url() {
        // validate_endpoint returns the url::Url-normalized string (not the raw input).
        // This ensures callers store the parser-consistent form for delivery.
        let result = validate_endpoint(
            "https://FCM.GoogleAPIs.com/fcm/send/abc",
            &enforced_policy(),
        )
        .expect("should accept");
        // url::Url lowercases the host during parsing.
        assert!(result.as_str().starts_with("https://fcm.googleapis.com/"));
    }

    #[test]
    fn accept_strips_userinfo() {
        // Credentials in the URL are stripped from the normalized output.
        let result =
            validate_endpoint("https://user:pass@fcm.googleapis.com/x", &enforced_policy())
                .expect("should accept");
        assert!(!result.as_str().contains("user"));
        assert!(!result.as_str().contains("pass"));
        assert!(result.as_str().starts_with("https://fcm.googleapis.com/"));
    }

    // --- RejectReason::code ---

    #[test]
    fn reject_reason_codes() {
        assert_eq!(RejectReason::UrlParse.code(), "url_parse");
        assert_eq!(RejectReason::Scheme.code(), "scheme");
        assert_eq!(RejectReason::PrivateHost.code(), "private_host");
        assert_eq!(RejectReason::AllowlistMiss.code(), "allowlist_miss");
    }
}

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

use std::net::{Ipv4Addr, Ipv6Addr};

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
            check_ipv4(*v4)?;
        }
        Host::Ipv6(v6) => {
            // IPv4-mapped IPv6 (::ffff:a.b.c.d): unwrap and re-check as IPv4.
            if let Some(v4) = v6.to_ipv4_mapped() {
                check_ipv4(v4)?;
            } else {
                check_ipv6(*v6)?;
            }
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

/// Reject IPv4 addresses in any IANA special-purpose range.
///
/// Uses both the explicit CIDR table (for ranges not yet stabilised as
/// `std::net::Ipv4Addr` predicates) and every stable std predicate.
fn check_ipv4(addr: Ipv4Addr) -> Result<(), RejectReason> {
    // Stable std predicates.
    if addr.is_unspecified()
        || addr.is_loopback()
        || addr.is_private()
        || addr.is_link_local()
        || addr.is_multicast()
        || addr.is_broadcast()
        || addr.is_documentation()
    {
        return Err(RejectReason::PrivateHost);
    }

    // Explicit CIDR table for ranges not yet covered by stable predicates:
    // (mask, network_bits) — reject if `addr_u32 & mask == network_bits`.
    //
    // Sources: IANA IPv4 Special-Purpose Address Registry (RFC 6890 umbrella).
    //
    // Ranges already covered by std predicates above (loopback 127/8,
    // private 10/8 + 172.16/12 + 192.168/16, link-local 169.254/16,
    // multicast 224/4, broadcast, documentation) are omitted here.
    //
    // Ranges not yet stabilised as predicates:
    //  - "this network":       0.0.0.0/8     (is_unspecified covers 0.0.0.0/32 only)
    //  - CGNAT shared:         100.64.0.0/10 (is_shared — unstable)
    //  - IETF protocol assign: 192.0.0.0/24  (partial overlap with documentation)
    //  - Benchmarking:         198.18.0.0/15 (is_benchmarking — unstable)
    //  - Reserved class E:     240.0.0.0/4   (is_reserved — unstable)
    //
    // All documentation ranges are caught by is_documentation() (stable since
    // Rust 1.67): 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24.
    static IPV4_CIDR_TABLE: &[(u32, u32)] = &[
        // (mask, network_bits) — reject if `addr_u32 & mask == network_bits`.
        // "this network": 0.0.0.0/8 (RFC 1122); is_unspecified() covers only
        // 0.0.0.0/32 — the /8 block here is deliberate to cover the full range.
        (0xFF00_0000, 0x0000_0000),
        // CGNAT shared address space: 100.64.0.0/10
        (0xFFC0_0000, 0x6440_0000),
        // IETF protocol assignments: 192.0.0.0/24
        (0xFFFF_FF00, 0xC000_0000),
        // Benchmarking: 198.18.0.0/15
        (0xFFFE_0000, 0xC612_0000),
        // Reserved (class E): 240.0.0.0/4
        (0xF000_0000, 0xF000_0000),
    ];

    let addr_u32 = u32::from(addr);
    for &(mask, net) in IPV4_CIDR_TABLE {
        if addr_u32 & mask == net {
            return Err(RejectReason::PrivateHost);
        }
    }

    Ok(())
}

/// Reject IPv6 addresses in any IANA special-purpose range.
///
/// IPv4-mapped addresses (`::ffff:0:0/96`) are handled by the caller via
/// `to_ipv4_mapped()` before this function is reached — this function must
/// not be called with a mapped address.
fn check_ipv6(addr: Ipv6Addr) -> Result<(), RejectReason> {
    // Stable std predicates.
    if addr.is_unspecified() || addr.is_loopback() || addr.is_multicast() {
        return Err(RejectReason::PrivateHost);
    }

    // Explicit CIDR table for IPv6 special-purpose ranges.
    // Each entry: (128-bit prefix as u128, prefix_len in bits).
    // Reject if `addr_u128 >> (128 - prefix_len) == prefix_bits`.
    //
    // Sources: IANA IPv6 Special-Purpose Address Registry (RFC 6890 et al.).
    //
    // Ranges already covered by stable predicates (unspecified ::/128,
    // loopback ::1/128, multicast ff00::/8) are omitted.
    static IPV6_CIDR_TABLE: &[(u128, u32)] = &[
        // NAT64 translation: 64:ff9b::/96
        (0x0064_ff9b_0000_0000_0000_0000_0000_0000u128, 96),
        // NAT64 translation local-use: 64:ff9b:1::/48
        (0x0064_ff9b_0001_0000_0000_0000_0000_0000u128, 48),
        // Discard-only: 100::/64
        (0x0100_0000_0000_0000_0000_0000_0000_0000u128, 64),
        // IETF protocol assignments: 2001::/23 covers 2001:0000:: – 2001:01ff::
        // This catches Teredo (2001::/32) and benchmarking (2001:2::/48).
        (0x2001_0000_0000_0000_0000_0000_0000_0000u128, 23),
        // Documentation: 2001:db8::/32 (NOT inside 2001::/23; must be explicit)
        (0x2001_0db8_0000_0000_0000_0000_0000_0000u128, 32),
        // 6to4: 2002::/16
        (0x2002_0000_0000_0000_0000_0000_0000_0000u128, 16),
        // Documentation: 3fff::/20
        (0x3fff_0000_0000_0000_0000_0000_0000_0000u128, 20),
        // Unique-local: fc00::/7
        (0xfc00_0000_0000_0000_0000_0000_0000_0000u128, 7),
        // Link-local unicast: fe80::/10
        (0xfe80_0000_0000_0000_0000_0000_0000_0000u128, 10),
    ];

    let addr_u128 = u128::from(addr);
    for &(prefix, prefix_len) in IPV6_CIDR_TABLE {
        let shift = 128u32.saturating_sub(prefix_len);
        if addr_u128 >> shift == prefix >> shift {
            return Err(RejectReason::PrivateHost);
        }
    }

    Ok(())
}

/// Reject local-domain DNS names: `localhost` (case-insensitive exact),
/// any name ending in `.localhost`, `local` (bare mDNS TLD), or any name
/// ending in `.local`.
///
/// Note: this check runs **before** the allowlist. A host in the allowlist
/// that ends in `.local` or equals `localhost` is still rejected here.
/// In practice this is not a problem because `.local` is an mDNS-reserved
/// TLD (RFC 6762) and `localhost` is reserved by RFC 6761 — neither can
/// appear as a legitimate push service hostname.
fn check_domain(name: &str) -> Result<(), RejectReason> {
    // WHATWG URL (used by the url crate) normalizes host to ASCII via IDNA,
    // so the domain is already ASCII. Use ASCII-aware case-insensitive
    // comparisons to avoid heap-allocating a lowercased String on every call.
    //
    // Strip a trailing dot first: `localhost.` has a dot and passes the
    // single-label guard, but ends_with_label misses it because the boundary
    // check requires the byte *before* the suffix to be '.'. Trimming one
    // trailing dot (the FQDN-absolute form, not meaningful for push endpoints)
    // collapses the bypass to the forms already covered below.
    let name = name.trim_end_matches('.');

    let ends_with_label = |suffix: &str| -> bool {
        name.len() > suffix.len()
            && name[name.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
            && name.as_bytes()[name.len() - suffix.len() - 1] == b'.'
    };

    // Single-label hostnames (no dot) are never valid push endpoint FQDNs.
    // This blocks `ip6-localhost`, `ip6-loopback`, `intranet`, and any other
    // bare hostname that could resolve to a private address via /etc/hosts.
    if !name.contains('.') {
        return Err(RejectReason::PrivateHost);
    }

    // Block known private / local TLDs and specific hostnames.
    //
    // NOTE: `ends_with_label` only matches `*.suffix` (requires a dot before
    // the suffix). It does NOT match the bare name `suffix` itself (e.g., bare
    // "local" or "localhost"). That case is handled by the single-label guard
    // above, which rejects any name without a dot. The two guards are
    // complementary: single-label guard covers exact matches; ends_with_label
    // covers subdomain matches. If the single-label guard were ever removed,
    // bare blocked TLDs would slip through.
    if ends_with_label("localhost")
        || ends_with_label("local")
        || ends_with_label("home.arpa")
        || ends_with_label("lan")
        || ends_with_label("internal")
        || ends_with_label("corp")
        || ends_with_label("intranet")
        || ends_with_label("private")
        || ends_with_label("localdomain")
    {
        return Err(RejectReason::PrivateHost);
    }

    Ok(())
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

    // --- RejectReason::code ---

    #[test]
    fn reject_reason_codes() {
        assert_eq!(RejectReason::UrlParse.code(), "url_parse");
        assert_eq!(RejectReason::Scheme.code(), "scheme");
        assert_eq!(RejectReason::PrivateHost.code(), "private_host");
        assert_eq!(RejectReason::AllowlistMiss.code(), "allowlist_miss");
    }
}

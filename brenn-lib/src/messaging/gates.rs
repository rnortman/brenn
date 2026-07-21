//! Shared publish-gate building blocks.
//!
//! Each helper is one gate the durable and ephemeral publish pipelines both
//! apply; they are composed separately per pipeline (no merged result enum) so
//! each maps to its own outcome type. The helpers hold no orchestration — they
//! answer one question apiece (well-formed? sender resolves? ACL covers? body
//! within cap?) and leave sequencing and result mapping to the caller.

use indexmap::IndexMap;

use crate::access::{AppCapability, AppPolicy};
use crate::config::AppConfig;
use crate::messaging::{ChannelScheme, is_unreserved_char};

/// Strip `scheme`'s prefix from `addr` and validate the bare name: the prefix
/// must be present, the remainder non-empty, and every character in the
/// unreserved set (`is_unreserved_char`). Returns the bare name on success.
pub fn well_formed_name(addr: &str, scheme: ChannelScheme) -> Option<&str> {
    let name = addr.strip_prefix(scheme.prefix())?;
    if name.is_empty() {
        return None;
    }
    if !name.chars().all(is_unreserved_char) {
        return None;
    }
    Some(name)
}

/// Layer-1 sender gate: the app named `slug` must exist and hold `grant`.
/// Returns the resolved `AppConfig` so the caller can read its policy/budget.
pub fn resolve_publish_sender<'a>(
    apps: &'a IndexMap<String, AppConfig>,
    slug: &str,
    grant: AppCapability,
) -> Option<&'a AppConfig> {
    apps.get(slug).filter(|app| app.policy.has_grant(grant))
}

/// Layer-2 policy gate: dispatch on `scheme` to the matching publish-ACL check
/// against the bare channel `name`. Only the bus-publishable schemes carry a
/// per-scheme publish ACL; `mqtt:`/`webhook:`/`pwa_push:`/`local:` are not
/// publishable through this gate (mqtt egress has its own gate; `local:` never
/// crosses the wire, so no server-side publish reaches it) and deny here.
pub fn publish_acl_allows(policy: &AppPolicy, scheme: ChannelScheme, name: &str) -> bool {
    match scheme {
        ChannelScheme::Brenn => policy.allows_brenn_publish(name),
        ChannelScheme::Ephemeral => policy.allows_ephemeral_publish(name),
        ChannelScheme::Mqtt
        | ChannelScheme::Webhook
        | ChannelScheme::PwaPush
        | ChannelScheme::Local => false,
    }
}

/// Reply-target visibility gate: is `addr` (bare name `name`, scheme `scheme`)
/// within the sender's legitimate visibility — the union of its publish
/// allowlist and its delivery scope? A reply target is a channel the sender may
/// name in `to` or may legitimately receive deliveries on. Every reply_to gate
/// (durable publish, message edit, automation create/edit) composes visibility
/// through this one predicate so the rule has a single definition and the
/// oracle-closure invariant cannot drift between the create-time pre-checks and
/// the publish-time gate.
pub fn reply_to_visible(policy: &AppPolicy, scheme: ChannelScheme, name: &str, addr: &str) -> bool {
    publish_acl_allows(policy, scheme, name) || policy.allows_channel_access(addr)
}

/// The body exceeded the size cap. Carries the observed and permitted lengths
/// for the caller's outcome mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodySizeExceeded {
    pub len: usize,
    pub max: usize,
}

/// Body-size cap: `body.len() > max` is rejected (`len == max` is allowed).
pub fn check_body_size(body: &str, max: usize) -> Result<(), BodySizeExceeded> {
    let len = body.len();
    if len > max {
        return Err(BodySizeExceeded { len, max });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::acl::ChannelMatcher;
    use crate::messaging::test_support::test_app_config;

    #[test]
    fn well_formed_name_brenn_strips_prefix() {
        assert_eq!(
            well_formed_name("brenn:orders.new", ChannelScheme::Brenn),
            Some("orders.new")
        );
    }

    #[test]
    fn well_formed_name_ephemeral_strips_prefix() {
        assert_eq!(
            well_formed_name("ephemeral:protobar", ChannelScheme::Ephemeral),
            Some("protobar")
        );
    }

    #[test]
    fn well_formed_name_rejects_wrong_scheme() {
        assert_eq!(well_formed_name("brenn:x", ChannelScheme::Ephemeral), None);
        assert_eq!(well_formed_name("ephemeral:x", ChannelScheme::Brenn), None);
    }

    #[test]
    fn well_formed_name_rejects_missing_prefix() {
        assert_eq!(well_formed_name("orders", ChannelScheme::Brenn), None);
    }

    #[test]
    fn well_formed_name_rejects_empty_name() {
        assert_eq!(well_formed_name("brenn:", ChannelScheme::Brenn), None);
        assert_eq!(
            well_formed_name("ephemeral:", ChannelScheme::Ephemeral),
            None
        );
    }

    #[test]
    fn well_formed_name_rejects_disallowed_chars() {
        // Space, slash, and colon are outside the unreserved set.
        assert_eq!(well_formed_name("brenn:a b", ChannelScheme::Brenn), None);
        assert_eq!(well_formed_name("brenn:a/b", ChannelScheme::Brenn), None);
        assert_eq!(
            well_formed_name("ephemeral:a:b", ChannelScheme::Ephemeral),
            None
        );
    }

    #[test]
    fn well_formed_name_accepts_full_unreserved_set() {
        assert_eq!(
            well_formed_name("brenn:A-Za-z0-9._~", ChannelScheme::Brenn),
            Some("A-Za-z0-9._~")
        );
    }

    #[test]
    fn address_scheme_prefix() {
        assert_eq!(ChannelScheme::Brenn.prefix(), "brenn:");
        assert_eq!(ChannelScheme::Ephemeral.prefix(), "ephemeral:");
    }

    fn app_with_policy(slug: &str, policy: AppPolicy) -> (String, AppConfig) {
        let mut app = test_app_config(slug, None, vec![]);
        app.policy = policy;
        (slug.to_string(), app)
    }

    #[test]
    fn resolve_publish_sender_requires_grant() {
        let mut apps = IndexMap::new();
        let (slug, cfg) = app_with_policy(
            "publisher",
            AppPolicy::with_grants(&[AppCapability::EphemeralPublish]),
        );
        apps.insert(slug, cfg);

        // Present with the grant → resolved.
        assert!(
            resolve_publish_sender(&apps, "publisher", AppCapability::EphemeralPublish).is_some()
        );
        // Present without the requested grant → None (grant filtering).
        assert!(
            resolve_publish_sender(&apps, "publisher", AppCapability::MessagingPublish).is_none()
        );
        // Absent slug → None.
        assert!(
            resolve_publish_sender(&apps, "missing", AppCapability::EphemeralPublish).is_none()
        );
    }

    #[test]
    fn publish_acl_allows_dispatches_by_scheme() {
        let mut policy = AppPolicy::with_grants(&[
            AppCapability::MessagingPublish,
            AppCapability::EphemeralPublish,
        ]);
        policy.acls.brenn_publish = vec![ChannelMatcher::Exact("orders".to_string())];
        policy.acls.ephemeral_publish = vec![ChannelMatcher::Exact("protobar".to_string())];

        assert!(publish_acl_allows(&policy, ChannelScheme::Brenn, "orders"));
        assert!(!publish_acl_allows(
            &policy,
            ChannelScheme::Brenn,
            "protobar"
        ));
        assert!(publish_acl_allows(
            &policy,
            ChannelScheme::Ephemeral,
            "protobar"
        ));
        assert!(!publish_acl_allows(
            &policy,
            ChannelScheme::Ephemeral,
            "orders"
        ));
        // Non-bus schemes are never publishable through this gate, regardless of ACL.
        assert!(!publish_acl_allows(&policy, ChannelScheme::Mqtt, "orders"));
        assert!(!publish_acl_allows(
            &policy,
            ChannelScheme::Webhook,
            "orders"
        ));
        assert!(!publish_acl_allows(
            &policy,
            ChannelScheme::PwaPush,
            "orders"
        ));
    }

    #[test]
    fn check_body_size_boundary() {
        // len == max is allowed.
        assert_eq!(check_body_size("abcd", 4), Ok(()));
        // len < max is allowed.
        assert_eq!(check_body_size("ab", 4), Ok(()));
        // len == max + 1 is rejected (mirrors the durable `>` comparison).
        assert_eq!(
            check_body_size("abcde", 4),
            Err(BodySizeExceeded { len: 5, max: 4 })
        );
    }
}

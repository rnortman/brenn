//! Layer-2 ACLs: per-capability allowlists of matchers that *narrow* a granted
//! capability to specific clients / channels / topics.
//!
//! Deny-by-default end to end: a granted-but-scoped capability with **no** ACL
//! matcher reaches nothing — `.any(...)` over an empty `Vec` is `false` (design
//! §2.4). These are the resolved matcher types; the operator-authored raw forms
//! and the validation that produces them land in a later increment.
//!
//! Backend-only, like the rest of `access` — no `ts-rs` derive.

/// Layer-2 ACL block. Each field is a per-capability allowlist; an empty list
/// denies everything for that capability (design §2.2 / §2.4).
///
/// `PwaPush` has no per-channel scope today (design §2.4 / high-level failure
/// mode 9), so it has no field here — it is a pure, scope-less grant.
#[derive(Debug, Clone, Default)]
pub struct AclSet {
    /// MQTT subscribe allowlist: `(client, topic-filter)` pairs.
    pub mqtt_subscribe: Vec<MqttSubMatcher>,
    /// MQTT publish allowlist: client-slug only (publish targets a concrete
    /// topic, not a filter — topic-level publish ACLs are out of scope, design
    /// §2.1).
    pub mqtt_publish: Vec<MqttClientMatcher>,
    /// `brenn:` subscribe allowlist.
    pub brenn_subscribe: Vec<ChannelMatcher>,
    /// `brenn:` publish allowlist.
    pub brenn_publish: Vec<ChannelMatcher>,
    /// `ephemeral:` subscribe allowlist. Reuses `ChannelMatcher` unchanged.
    /// Matcher values are **bare channel names, no scheme**
    /// (`protobar-demo`, not `ephemeral:protobar-demo`) — the ACL list name
    /// carries the scheme, and `AppPolicy::allows_channel_access` strips it before
    /// matching.
    pub ephemeral_subscribe: Vec<ChannelMatcher>,
    /// `ephemeral:` publish allowlist. Same bare-name convention as
    /// `ephemeral_subscribe`.
    pub ephemeral_publish: Vec<ChannelMatcher>,
    /// Inbound webhook allowlist.
    pub webhook: Vec<WebhookMatcher>,
}

/// MQTT subscribe matcher: a `(client, topic_filter)` pair. The `topic_filter`
/// uses MQTT wildcard semantics and is subset-matched against a requested filter
/// by `mqtt_match::filter_covers` at enforcement time (design §2.3, §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttSubMatcher {
    /// MQTT client slug this matcher scopes to.
    pub client: String,
    /// Allowed MQTT topic filter (validated at resolution time).
    pub topic_filter: String,
}

/// MQTT publish matcher: client-slug only. Publish is client-scoped (design
/// §2.1); there is no topic dimension on the publish side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttClientMatcher {
    /// MQTT client slug this matcher scopes to.
    pub client: String,
}

/// `brenn:` channel matcher. Starts with `Exact` + `Prefix` only; full globs are
/// deliberately deferred (design §2.2, high-level failure mode 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelMatcher {
    /// Matches `channel` exactly.
    Exact(String),
    /// Matches any `channel` that starts with this prefix. This is a **byte**
    /// prefix at the type level: `matches` is `channel.starts_with(p)`, so in
    /// isolation `Prefix("alert")` would also match `"alerts"`. The narrowing that
    /// keeps that from over-granting lives at the resolution boundary
    /// (`access::resolve::build_app_policy` / `resolve_channel`), which requires an
    /// operator-authored prefix to be non-empty and to end at a segment boundary
    /// (`/` or `.`) before a `Prefix` ever reaches a live `AppPolicy`. The
    /// `matches` byte-prefix semantics here are deliberately kept simple; the
    /// validation is at the conversion site, not on this type.
    Prefix(String),
}

impl ChannelMatcher {
    /// Does this matcher cover `channel`? `Exact` requires equality; `Prefix`
    /// requires `channel` to start with the prefix. The live Phase-1 helper
    /// consumed by `allows_brenn_dynamic_subscribe` (design §2.3).
    pub fn matches(&self, channel: &str) -> bool {
        match self {
            ChannelMatcher::Exact(s) => channel == s,
            ChannelMatcher::Prefix(p) => channel.starts_with(p),
        }
    }
}

/// Inbound webhook matcher: an endpoint slug, matched exactly (design §2.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookMatcher {
    /// Webhook endpoint slug this matcher scopes to.
    pub endpoint: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_matcher_exact() {
        let m = ChannelMatcher::Exact("alerts".to_string());
        assert!(m.matches("alerts"));
        // Exact does not match a longer string that merely starts with it.
        assert!(!m.matches("alerts.high"));
        // Nor a shorter / different one.
        assert!(!m.matches("alert"));
        assert!(!m.matches("status"));
    }

    #[test]
    fn empty_exact_matches_only_empty_string() {
        // Edge: an empty exact matcher matches only the empty string. Pinned for
        // the same reason as `empty_prefix_matches_everything` — it documents the
        // raw matcher's behavior before resolution-time matcher-string validation
        // (a later increment) decides whether to reject empty matchers.
        let m = ChannelMatcher::Exact(String::new());
        assert!(m.matches(""));
        assert!(!m.matches("x"));
    }

    #[test]
    fn channel_matcher_prefix() {
        let m = ChannelMatcher::Prefix("alerts".to_string());
        // The prefix itself matches (starts_with is reflexive).
        assert!(m.matches("alerts"));
        // Longer strings sharing the prefix match.
        assert!(m.matches("alerts.high"));
        assert!(m.matches("alerts/anything"));
        // A non-prefixed string does not.
        assert!(!m.matches("status"));
        // A string that is a prefix *of* the matcher does not match.
        assert!(!m.matches("alert"));
    }

    #[test]
    fn prefix_has_no_segment_boundary() {
        // The *type-level* `matches` is a byte prefix, not a channel-namespace
        // prefix: `Prefix("alert")` matches `"alerts"`, `"alertXYZ"`, etc. This
        // pins the matcher's raw behavior in isolation. A `Prefix` like `"alert"`
        // can never reach a live `AppPolicy`, though — `build_app_policy`'s
        // `resolve_channel` rejects a non-segment-boundary prefix at resolution
        // (see `access::resolve` tests). The narrowing is at the conversion site,
        // so `matches` stays a simple byte prefix here.
        let m = ChannelMatcher::Prefix("alert".to_string());
        assert!(m.matches("alerts"));
        assert!(m.matches("alertXYZ"));
    }

    #[test]
    fn empty_prefix_matches_everything() {
        // Edge: an empty prefix is a universal match (every &str starts_with "").
        // This documents the raw matcher's behavior; an empty prefix is rejected
        // by `build_app_policy` at resolution, so it never reaches a live policy
        // (see `access::resolve::empty_brenn_prefix_panics`).
        let m = ChannelMatcher::Prefix(String::new());
        assert!(m.matches("anything"));
        assert!(m.matches(""));
    }

    #[test]
    fn acl_set_construction() {
        // API-surface pinning for the data-only matcher structs (no behavioral
        // logic yet — their `allows_*` consumers land with `AppPolicy` in a later
        // increment). Catches a field rename here rather than at the distant
        // enforcement-site consumer.
        let mut acl = AclSet::default();
        acl.mqtt_subscribe.push(MqttSubMatcher {
            client: "home".to_string(),
            topic_filter: "sensors/#".to_string(),
        });
        acl.mqtt_publish.push(MqttClientMatcher {
            client: "home".to_string(),
        });
        acl.webhook.push(WebhookMatcher {
            endpoint: "webhook-slug".to_string(),
        });
        // Ephemeral lists default empty — deny-by-default — and hold
        // bare-name `ChannelMatcher`s just like the `brenn_*` lists.
        assert!(acl.ephemeral_subscribe.is_empty());
        assert!(acl.ephemeral_publish.is_empty());
        acl.ephemeral_subscribe
            .push(ChannelMatcher::Exact("protobar-demo".to_string()));
        acl.ephemeral_publish
            .push(ChannelMatcher::Exact("protobar-demo".to_string()));
        assert_eq!(acl.mqtt_subscribe[0].client, "home");
        assert_eq!(acl.mqtt_subscribe[0].topic_filter, "sensors/#");
        assert_eq!(acl.mqtt_publish[0].client, "home");
        assert_eq!(acl.webhook[0].endpoint, "webhook-slug");
        assert_eq!(
            acl.ephemeral_subscribe[0],
            ChannelMatcher::Exact("protobar-demo".to_string())
        );
        assert_eq!(
            acl.ephemeral_publish[0],
            ChannelMatcher::Exact("protobar-demo".to_string())
        );
    }
}

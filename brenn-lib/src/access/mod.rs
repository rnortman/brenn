//! Unified access-control policy data model (backend-only).
//!
//! This module hosts the shared `AppPolicy` capability-grant + ACL model that
//! spans LLM conversations and WASM components (both modeled as "apps"). It is
//! operator-authored TOML consumed by backend enforcement; it never crosses the
//! backend↔frontend boundary, so it carries **no** `ts-rs` derive.
//!
//! The model and resolution land incrementally. So far this module hosts the
//! MQTT topic-filter subset matcher (`mqtt_match`), the layer-1 grant model
//! (`AppCapability`, `GrantSet`), the layer-2 ACL matchers (`acl`), the
//! `AppPolicy` struct + decision API, the operator-authored *raw* ACL config
//! shapes for LLM apps (`raw`), and the validation/conversion of those raw shapes
//! into a resolved `AppPolicy` (`resolve::build_app_policy`); the
//! enforcement-site wiring follows in later increments.

pub mod acl;
pub mod mqtt_match;
pub mod raw;
pub mod resolve;

use std::collections::{BTreeMap, BTreeSet};

use crate::access::acl::AclSet;
use crate::messaging::ChannelScheme;
use crate::tools::ResolvedToolGrant;

/// The unified, app-kind-independent record of "what an app may do": a layer-1
/// `GrantSet` (coarse capabilities) plus a layer-2 `AclSet` (per-capability
/// scope). Both LLM conversations and WASM components resolve their authoring
/// config *into* this one type, so enforcement is written once.
///
/// Deny-by-default end to end: a default (empty) policy grants nothing and
/// matches no ACL list. Backend-only — no `ts-rs` derive.
#[derive(Debug, Clone, Default)]
pub struct AppPolicy {
    /// Layer 1: coarse capability grants.
    pub grants: GrantSet,
    /// Layer 2: per-capability ACL allowlists narrowing each granted capability.
    pub acls: AclSet,
    /// Per-tool grants: which registry tools this participant may address, keyed
    /// by canonical tool name, each with its resolved ACL clauses and optional
    /// rate limit. Empty ⇒ no tool authorization (deny-by-default). Resolved from
    /// `[[*.tool_grant]]` config (plus an app's mount-derived `git-repo-pull`
    /// grant).
    pub tool_grants: BTreeMap<String, ResolvedToolGrant>,
}

impl AppPolicy {
    /// Test-only constructor: an `AppPolicy` holding exactly `grants` (each
    /// inserted into the `GrantSet`) and a default (empty) `AclSet`. Collapses
    /// the `default()` + repeated `.grants.insert(...)` idiom that test fixtures
    /// across `automation`/`messaging`/`integration`/`active_bridge` would
    /// otherwise hand-roll; a single-point edit if the construction convention
    /// changes.
    #[cfg(test)]
    pub fn with_grants(grants: &[AppCapability]) -> Self {
        let mut gs = GrantSet::default();
        for &cap in grants {
            gs.insert(cap);
        }
        AppPolicy {
            grants: gs,
            acls: AclSet::default(),
            tool_grants: BTreeMap::new(),
        }
    }

    /// Test-only constructor: a "messaging sender" policy that can publish to any
    /// `brenn:` channel — the `MessagingPublish` grant plus a universal
    /// `brenn_publish` matcher (`Prefix("")`). After Phase-2 Seam A (design §2.2),
    /// the grant alone no longer authorizes a publish; the publish path also needs
    /// a covering `brenn_publish` matcher. Automation fire/create fixtures that
    /// just want "this app may publish" use this instead of bare
    /// `with_grants(&[MessagingPublish])` so they exercise the happy path rather
    /// than the new deny-by-default ACL gate.
    #[cfg(test)]
    pub fn messaging_sender_policy() -> Self {
        let mut p = AppPolicy::with_grants(&[AppCapability::MessagingPublish]);
        p.acls
            .brenn_publish
            .push(crate::access::acl::ChannelMatcher::Prefix(String::new()));
        p
    }

    /// Layer-1 grant check only: is `cap` granted, ignoring any ACL scope?
    ///
    /// Named `has_grant` (not `grants`) to avoid shadowing the same-named
    /// `grants` field: a method and a field both called `grants` would read as
    /// two things at call sites (`p.grants(cap)` vs `p.grants.iter()` once a
    /// later phase's logging iterates the set). The design §2.3 sketch wrote
    /// `policy.grants(...)`; that sketch is not normative for the method name.
    pub fn has_grant(&self, cap: AppCapability) -> bool {
        self.grants.has(cap)
    }

    /// The resolved grant for registry tool `tool`, or `None` if this
    /// participant may not address it (deny-by-default). The presence of a grant
    /// is the authorization gate; its ACL clauses narrow which resources.
    pub fn tool_grant(&self, tool: &str) -> Option<&ResolvedToolGrant> {
        self.tool_grants.get(tool)
    }

    /// Does some `mqtt_subscribe` matcher cover `(client, requested_filter)`?
    ///
    /// The ACL-matcher half shared by `allows_mqtt_dynamic_subscribe` and
    /// `allows_mqtt_delivery`: the two differ only in whether they additionally
    /// require the `DynamicSubscribe` grant (design §2.2 / §2.3). With no matcher
    /// the `.any(...)` is `false` — deny-by-default.
    fn mqtt_acl_covers(&self, client: &str, requested_filter: &str) -> bool {
        self.acls.mqtt_subscribe.iter().any(|m| {
            m.client == client
                && crate::access::mqtt_match::filter_covers(&m.topic_filter, requested_filter)
        })
    }

    /// Does some matcher in `list` cover `channel`? The single covering-matcher
    /// primitive behind every channel-family two-factor decision (brenn +
    /// ephemeral, subscribe + publish). Centralized so matcher semantics live in
    /// one place (quality-4); an empty list is deny-by-default (`.any(...)` over
    /// an empty slice is `false`).
    fn acl_covers(list: &[crate::access::acl::ChannelMatcher], channel: &str) -> bool {
        list.iter().any(|m| m.matches(channel))
    }

    /// Does some `brenn_subscribe` matcher cover `channel`? Shared ACL-matcher
    /// half of the brenn dynamic-subscribe and delivery decisions (design §2.2).
    fn brenn_acl_covers(&self, channel: &str) -> bool {
        Self::acl_covers(&self.acls.brenn_subscribe, channel)
    }

    /// Does some `webhook` matcher exactly match `endpoint`? Shared ACL-matcher
    /// half of the webhook dynamic-subscribe and delivery decisions (design §2.2).
    fn webhook_acl_covers(&self, endpoint: &str) -> bool {
        self.acls.webhook.iter().any(|m| m.endpoint == endpoint)
    }

    /// May this app dynamically subscribe to `requested_filter` on MQTT `client`?
    ///
    /// Both layers must agree: the `DynamicSubscribe` grant (the runtime
    /// `MessageSubscribe` tool) **and** the `MqttSubscribe` transport grant
    /// **and** at least one `mqtt_subscribe` matcher whose `(client,
    /// allowed_filter)` covers the request (filter subset per `mqtt_match`). With
    /// no matcher the `.any(...)` is `false` — deny-by-default (design §2.3).
    ///
    /// Precondition: `requested_filter` is a *validated* MQTT topic filter; the
    /// enforcement site validates it before calling (design §3.2, §5.2).
    pub fn allows_mqtt_dynamic_subscribe(&self, client: &str, requested_filter: &str) -> bool {
        self.grants.has(AppCapability::DynamicSubscribe)
            && self.grants.has(AppCapability::MqttSubscribe)
            && self.mqtt_acl_covers(client, requested_filter)
    }

    /// May this app dynamically subscribe to `brenn:` `channel`? Requires the
    /// `DynamicSubscribe` grant, the `MessagingSubscribe` transport grant, and a
    /// `brenn_subscribe` matcher covering the channel (design §2.3).
    pub fn allows_brenn_dynamic_subscribe(&self, channel: &str) -> bool {
        self.grants.has(AppCapability::DynamicSubscribe)
            && self.grants.has(AppCapability::MessagingSubscribe)
            && self.brenn_acl_covers(channel)
    }

    /// May this app dynamically subscribe to webhook `endpoint`? Requires the
    /// `DynamicSubscribe` grant, the `Webhook` transport grant, and a `webhook`
    /// matcher for the endpoint (exact match, design §2.3).
    pub fn allows_webhook_dynamic_subscribe(&self, endpoint: &str) -> bool {
        self.grants.has(AppCapability::DynamicSubscribe)
            && self.grants.has(AppCapability::Webhook)
            && self.webhook_acl_covers(endpoint)
    }

    /// May this app still *receive a delivery* on MQTT `(client, topic)`?
    ///
    /// The subscription-**holding** decision (design §2.2): the `MqttSubscribe`
    /// transport grant **plus** a covering `mqtt_subscribe` matcher, but **not**
    /// the `DynamicSubscribe` grant — `DynamicSubscribe` governs the runtime
    /// `MessageSubscribe` *tool*, not holding a subscription, so a static
    /// subscriber (operator TOML) that never holds it must still be authorized to
    /// receive. This is the per-transport piece `allows_channel_access` composes.
    pub fn allows_mqtt_delivery(&self, client: &str, topic: &str) -> bool {
        self.grants.has(AppCapability::MqttSubscribe) && self.mqtt_acl_covers(client, topic)
    }

    /// May this app still receive a delivery on `brenn:` `channel`? The
    /// `MessagingSubscribe` transport grant + a covering `brenn_subscribe`
    /// matcher, **without** `DynamicSubscribe` (design §2.2).
    pub fn allows_brenn_delivery(&self, channel: &str) -> bool {
        self.grants.has(AppCapability::MessagingSubscribe) && self.brenn_acl_covers(channel)
    }

    /// May this app still receive a delivery on `ephemeral:` `channel`? The
    /// `EphemeralSubscribe` transport grant + a covering `ephemeral_subscribe`
    /// matcher. Deliberate asymmetry with the brenn family: the
    /// gating grant is `EphemeralSubscribe`, not `MessagingSubscribe` — the two
    /// delivery classes carry distinct transport grants. `channel` is
    /// the `ephemeral:`-stripped bare channel name (matcher values carry no
    /// scheme). No dynamic-subscribe analogue in v1.
    pub fn allows_ephemeral_delivery(&self, channel: &str) -> bool {
        self.grants.has(AppCapability::EphemeralSubscribe)
            && Self::acl_covers(&self.acls.ephemeral_subscribe, channel)
    }

    /// May this app still receive a delivery on webhook `endpoint`? The `Webhook`
    /// transport grant + an exact `webhook` matcher, **without** `DynamicSubscribe`
    /// (design §2.2).
    pub fn allows_webhook_delivery(&self, endpoint: &str) -> bool {
        self.grants.has(AppCapability::Webhook) && self.webhook_acl_covers(endpoint)
    }

    // --- Publish-side decisions (design §2.1): the send analogue of the
    //     delivery family. Layer-1 grant AND a covering layer-2 matcher; an empty
    //     matcher list is deny-by-default (`.any(...)` over an empty `Vec` is
    //     `false`). These are publish analogues, so they deliberately do NOT carry
    //     the `_delivery` suffix nor compose into `allows_channel_access`.

    /// May this app publish to `brenn:` `channel`? Requires the `MessagingPublish`
    /// grant **and** a covering `brenn_publish` matcher (`Exact`/`Prefix` via
    /// `ChannelMatcher::matches`). `channel` is the `brenn:`-stripped channel name,
    /// matching how `allows_brenn_delivery` treats it (design §2.1, §2.2).
    pub fn allows_brenn_publish(&self, channel: &str) -> bool {
        self.grants.has(AppCapability::MessagingPublish)
            && Self::acl_covers(&self.acls.brenn_publish, channel)
    }

    /// May this app publish to `ephemeral:` `channel`? Requires the
    /// `EphemeralPublish` grant **and** a covering `ephemeral_publish` matcher
    /// grant. `channel` is the `ephemeral:`-stripped bare channel name
    /// (matcher values carry no scheme). Publish analogue, so — like
    /// `allows_brenn_publish` — it does NOT compose into `allows_channel_access`.
    pub fn allows_ephemeral_publish(&self, channel: &str) -> bool {
        self.grants.has(AppCapability::EphemeralPublish)
            && Self::acl_covers(&self.acls.ephemeral_publish, channel)
    }

    /// May this app publish to MQTT `client`? Requires the `MqttPublish` grant
    /// **and** a `mqtt_publish` matcher for the client. Client-scoped only — there
    /// is no topic dimension on the publish side (`MqttClientMatcher` carries no
    /// topic; design §2.1, §2.4).
    pub fn allows_mqtt_publish(&self, client: &str) -> bool {
        self.grants.has(AppCapability::MqttPublish)
            && self.acls.mqtt_publish.iter().any(|m| m.client == client)
    }

    /// May `self` (any subscriber — static or dynamic) access this channel — hold
    /// a subscription, receive a delivery, discover it, or read its history? The
    /// one access-class decision the whole spec collapses read/see/subscribe/
    /// deliver into: classify the address with `ChannelScheme::split` and dispatch
    /// to the matching per-transport *subscription-holding* decision — the
    /// transport grant plus a covering ACL matcher, WITHOUT the `DynamicSubscribe`
    /// grant (which governs the runtime tool, not access). Used at delivery time,
    /// at discovery, and at read time (the `MessageChannelGet` gate).
    ///
    /// The match is exhaustive on `ChannelScheme`, so a future scheme variant
    /// fails compilation here rather than defaulting to allow — that is the "can't
    /// forget the match arm" guarantee. A malformed / unrecognized address returns
    /// `false` (deny-by-default; an address we cannot classify cannot be
    /// authorized).
    ///
    /// On a security delivery path reached by attacker-influenceable inbound
    /// traffic, a parse/validate failure denies rather than panics.
    pub fn allows_channel_access(&self, channel_address: &str) -> bool {
        match ChannelScheme::split(channel_address) {
            Some((ChannelScheme::Mqtt, _)) => {
                // The channel's own stored `mqtt:<client>:<topic>` address. Parse it
                // and re-validate the filter; `topic` is by construction covered by
                // itself, so `mqtt_acl_covers` re-confirms the operator's *current*
                // allowed matcher still covers it. A parse/validate failure is host
                // corruption, but on the delivery/read path we deny rather than panic.
                let Ok(addr) = crate::mqtt::address::parse_mqtt_address(channel_address) else {
                    return false;
                };
                if crate::mqtt::address::validate_topic_filter_str(&addr.topic).is_err() {
                    return false;
                }
                self.allows_mqtt_delivery(&addr.client, &addr.topic)
            }
            Some((ChannelScheme::Brenn, channel)) => self.allows_brenn_delivery(channel),
            // Strip-only, like the `brenn:` arm: ephemeral names share the brenn
            // charset and the same trust argument, so no re-validation. Distinct
            // transport grant inside (design asymmetry).
            Some((ChannelScheme::Ephemeral, channel)) => self.allows_ephemeral_delivery(channel),
            Some((ChannelScheme::Webhook, endpoint)) => self.allows_webhook_delivery(endpoint),
            // pwa_push is an egress-only protocol: nothing subscribes to it, nothing
            // is delivered from it, and there is no bus history to read. Its live
            // tool surfaces (PwaPushChannelGet / list_targets) are authorized
            // separately on the app's pwa_push config. A deliberate policy deny, not
            // a missing case.
            Some((ChannelScheme::PwaPush, _)) => false,
            // local: is page-local — the surface kernel's router is its sole
            // source of truth and the traffic never crosses the wire, so the
            // server mediates no access to it and has none to grant. There is no
            // bus history to read and nothing server-side to deliver. A
            // deliberate policy deny, not a missing case: reaching a local:
            // channel from outside the page takes an explicit bridge component
            // (subscribe brenn:, republish local:), which is authorized on its
            // brenn: side by the arm above.
            Some((ChannelScheme::Local, _)) => false,
            // Unclassifiable address — cannot classify, therefore cannot authorize.
            None => false,
        }
    }
}

/// Layer 1: the coarse, binary capabilities an app may be granted.
///
/// This is the **unified** capability enum spanning LLM conversations and WASM
/// components. The full variant set is defined now so later phases need not
/// widen it. Named `AppCapability` — **not** `Capability` — to avoid colliding
/// with `brenn-wasm`'s own `Capability` enum, which lives in a crate that does
/// not depend on `brenn-lib`.
///
/// `Ord` is derived so a `GrantSet` (a `BTreeSet<AppCapability>`) iterates in a
/// stable order once a later phase's logging needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppCapability {
    // messaging bus
    /// Publish to the `brenn:` bus.
    MessagingPublish,
    /// Hold any subscription to `brenn:`/`webhook:` (static or dynamic).
    MessagingSubscribe,
    /// Additionally gates the runtime `MessageSubscribe` tool (LLM apps only).
    DynamicSubscribe,
    /// Publish to the `ephemeral:` bus. A distinct transport grant
    /// from `MessagingPublish`: the two delivery classes are gated separately.
    EphemeralPublish,
    /// Hold a subscription to the `ephemeral:` bus. A distinct
    /// transport grant from `MessagingSubscribe`: an operator grants each
    /// delivery class explicitly. LLM-app-unauthorable in v1 (no ephemeral
    /// delivery path to a conversation); `build_app_policy` boot-panics on it.
    EphemeralSubscribe,
    // external transports
    /// Publish to MQTT.
    MqttPublish,
    /// Subscribe to MQTT.
    MqttSubscribe,
    /// Hold an inbound webhook subscription.
    Webhook,
    /// Receive PWA push notifications (no per-channel scope).
    PwaPush,
    // WASM host capabilities. Authored as `WasmGrant` on WASM components and
    // mapped to these variants internally; not part of the LLM `grants` token
    // vocabulary. Because `Deserialize` is derived for the whole enum, these
    // tokens (`"wasm_store"`, …) and `"integration"` *do* technically parse from
    // an LLM app's `grants` list — but `build_app_policy` (the resolution
    // boundary) rejects them for an LLM app (panic, operator config = fail-fast),
    // so an LLM `grants` list may carry only the LLM-authorable subset above.
    /// WASM host: key/value store access.
    WasmStore,
    /// WASM host: structured logging.
    WasmLog,
    /// WASM host: alert emission.
    WasmAlert,
    /// WASM host: config read access.
    WasmConfig,
    /// Surface: alert emission. Authored as `SurfaceGrant::Alert` on a
    /// `[[surface]]` and mapped internally (not part of the LLM `grants` token
    /// vocabulary). A capability distinct from `WasmAlert` so policy inspection
    /// keeps alert-grant provenance per boundary.
    SurfaceAlert,
    /// Surface: takeover (fullscreen overlay) emission. Authored as
    /// `SurfaceGrant::Takeover` on a `[[surface]]` and mapped internally (not
    /// part of the LLM `grants` token vocabulary). Surface-only, mirroring
    /// `SurfaceAlert`; the shell reads it to gate a takeover request.
    SurfaceTakeover,
    /// Integration access (pfin, graf, …). A bare variant with no associated
    /// `IntegrationKind` payload: the payload and its enforcement are reserved
    /// for a later phase. Bare so the token list deserializes from plain strings.
    Integration,
}

/// Layer-1 grant set. Deny-by-default: a capability not in the set is denied.
///
/// Backed by a `BTreeSet` so a later phase's logging can iterate grants in a
/// stable order. Only `has` and `insert` are exposed; `iter`/`is_empty` are
/// added with their first consumer.
#[derive(Debug, Clone, Default)]
pub struct GrantSet(BTreeSet<AppCapability>);

impl GrantSet {
    /// Is `cap` granted?
    pub fn has(&self, cap: AppCapability) -> bool {
        self.0.contains(&cap)
    }

    /// Insert `cap`; returns `true` if it was newly inserted (mirrors
    /// `BTreeSet::insert`, used by resolution to detect duplicate grants).
    pub fn insert(&mut self, cap: AppCapability) -> bool {
        self.0.insert(cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::acl::{ChannelMatcher, MqttClientMatcher, MqttSubMatcher, WebhookMatcher};

    /// Every `AppCapability` variant. The exhaustive `match` arm below is a
    /// compile-time guard: adding a variant to `AppCapability` without listing it
    /// here is a compile error, so the deny-by-default test (and any other test
    /// iterating all variants) cannot silently skip a new capability.
    const ALL_CAPABILITIES: &[AppCapability] = &[
        AppCapability::MessagingPublish,
        AppCapability::MessagingSubscribe,
        AppCapability::DynamicSubscribe,
        AppCapability::EphemeralPublish,
        AppCapability::EphemeralSubscribe,
        AppCapability::MqttPublish,
        AppCapability::MqttSubscribe,
        AppCapability::Webhook,
        AppCapability::PwaPush,
        AppCapability::WasmStore,
        AppCapability::WasmLog,
        AppCapability::WasmAlert,
        AppCapability::WasmConfig,
        AppCapability::SurfaceAlert,
        AppCapability::SurfaceTakeover,
        AppCapability::Integration,
    ];

    /// Forces `ALL_CAPABILITIES` to stay in sync with the enum: a new variant is
    /// a non-exhaustive-`match` compile error here until it is added above.
    fn assert_all_capabilities_exhaustive(cap: AppCapability) {
        let _: () = match cap {
            AppCapability::MessagingPublish
            | AppCapability::MessagingSubscribe
            | AppCapability::DynamicSubscribe
            | AppCapability::EphemeralPublish
            | AppCapability::EphemeralSubscribe
            | AppCapability::MqttPublish
            | AppCapability::MqttSubscribe
            | AppCapability::Webhook
            | AppCapability::PwaPush
            | AppCapability::WasmStore
            | AppCapability::WasmLog
            | AppCapability::WasmAlert
            | AppCapability::WasmConfig
            | AppCapability::SurfaceAlert
            | AppCapability::SurfaceTakeover
            | AppCapability::Integration => (),
        };
        assert!(
            ALL_CAPABILITIES.contains(&cap),
            "ALL_CAPABILITIES is missing {cap:?}"
        );
    }

    #[test]
    fn empty_grant_set_denies_everything() {
        let g = GrantSet::default();
        for &cap in ALL_CAPABILITIES {
            assert_all_capabilities_exhaustive(cap);
            assert!(!g.has(cap), "empty GrantSet must deny {cap:?}");
        }
    }

    #[test]
    fn insert_then_has_round_trip() {
        let mut g = GrantSet::default();
        assert!(g.insert(AppCapability::DynamicSubscribe));
        assert!(g.has(AppCapability::DynamicSubscribe));
        // A different capability is still denied.
        assert!(!g.has(AppCapability::MqttSubscribe));
        // Re-inserting the same capability reports "not newly inserted".
        assert!(!g.insert(AppCapability::DynamicSubscribe));
    }

    #[test]
    fn two_grants_are_independently_queryable() {
        // Directly exercises the two-grant pattern Phase 1's enforcement depends
        // on (e.g. `allows_mqtt_dynamic_subscribe` requires both
        // `DynamicSubscribe` and `MqttSubscribe`): inserting two distinct
        // capabilities leaves both present and an unrelated third absent.
        let mut g = GrantSet::default();
        assert!(g.insert(AppCapability::DynamicSubscribe));
        assert!(g.insert(AppCapability::MqttSubscribe));
        assert!(g.has(AppCapability::DynamicSubscribe));
        assert!(g.has(AppCapability::MqttSubscribe));
        assert!(!g.has(AppCapability::MessagingPublish));
    }

    #[test]
    fn deserializes_snake_case_tokens() {
        // The operator-facing `grants` token vocabulary (the LLM-authorable
        // subset). Pin every token so an accidental rename is caught.
        //
        // The WASM-host variants (`wasm_store`/`wasm_log`/`wasm_alert`/
        // `wasm_config`), `integration`, and `ephemeral_subscribe` are
        // deliberately *absent* from this table: they are not part of the LLM
        // `grants` vocabulary. The WASM/integration caps and
        // `ephemeral_subscribe` have no LLM enforcement point (no ephemeral
        // delivery path to a conversation in v1, so the grant would be dead
        // config). They still
        // deserialize (the derive covers the whole enum); the LLM-authorable
        // subset is enforced at the resolution boundary by `build_app_policy`
        // (`access/resolve.rs`), which panics if such a token appears in an LLM
        // app's `grants`. `ephemeral_publish` *is* LLM-authorable.
        // This test pins the *positive* token vocabulary only.
        let cases = [
            ("messaging_publish", AppCapability::MessagingPublish),
            ("messaging_subscribe", AppCapability::MessagingSubscribe),
            ("dynamic_subscribe", AppCapability::DynamicSubscribe),
            ("ephemeral_publish", AppCapability::EphemeralPublish),
            ("mqtt_publish", AppCapability::MqttPublish),
            ("mqtt_subscribe", AppCapability::MqttSubscribe),
            ("webhook", AppCapability::Webhook),
            ("pwa_push", AppCapability::PwaPush),
        ];
        for (token, expected) in cases {
            let parsed: AppCapability =
                serde_json::from_str(&format!("\"{token}\"")).expect("token parses");
            assert_eq!(parsed, expected, "token {token:?}");
        }
    }

    /// Build a policy with the given grants and ACL block. Helper for the
    /// decision tests below.
    fn policy(grants: &[AppCapability], acls: AclSet) -> AppPolicy {
        let mut gs = GrantSet::default();
        for &cap in grants {
            gs.insert(cap);
        }
        AppPolicy {
            grants: gs,
            acls,
            tool_grants: BTreeMap::new(),
        }
    }

    #[test]
    fn default_policy_denies_all_dynamic_subscribes() {
        let p = AppPolicy::default();
        assert!(!p.has_grant(AppCapability::DynamicSubscribe));
        assert!(!p.allows_mqtt_dynamic_subscribe("home", "sensors/temp"));
        assert!(!p.allows_brenn_dynamic_subscribe("alerts"));
        assert!(!p.allows_webhook_dynamic_subscribe("hook"));
    }

    #[test]
    fn mqtt_dynamic_subscribe_requires_both_grants() {
        let acls = AclSet {
            mqtt_subscribe: vec![MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "sensors/+/temp".to_string(),
            }],
            ..AclSet::default()
        };
        // Matcher covers the request, but missing the DynamicSubscribe grant.
        let only_transport = policy(&[AppCapability::MqttSubscribe], acls.clone());
        assert!(!only_transport.allows_mqtt_dynamic_subscribe("home", "sensors/kitchen/temp"));
        // Matcher covers the request, but missing the MqttSubscribe transport grant.
        let only_dynamic = policy(&[AppCapability::DynamicSubscribe], acls.clone());
        assert!(!only_dynamic.allows_mqtt_dynamic_subscribe("home", "sensors/kitchen/temp"));
        // Both grants + covering matcher ⇒ allowed.
        let both = policy(
            &[
                AppCapability::DynamicSubscribe,
                AppCapability::MqttSubscribe,
            ],
            acls,
        );
        assert!(both.allows_mqtt_dynamic_subscribe("home", "sensors/kitchen/temp"));
    }

    #[test]
    fn mqtt_dynamic_subscribe_empty_acl_denies() {
        // Both grants held, but no matcher on the list ⇒ deny-by-default
        // (design §2.4). This is the §6.1 deny-all-empty-ACL case.
        let p = policy(
            &[
                AppCapability::DynamicSubscribe,
                AppCapability::MqttSubscribe,
            ],
            AclSet::default(),
        );
        assert!(!p.allows_mqtt_dynamic_subscribe("home", "sensors/temp"));
    }

    #[test]
    fn mqtt_dynamic_subscribe_matcher_scoping() {
        let p = policy(
            &[
                AppCapability::DynamicSubscribe,
                AppCapability::MqttSubscribe,
            ],
            AclSet {
                mqtt_subscribe: vec![MqttSubMatcher {
                    client: "home".to_string(),
                    topic_filter: "sensors/+/temp".to_string(),
                }],
                ..AclSet::default()
            },
        );
        // Covered request on the right client.
        assert!(p.allows_mqtt_dynamic_subscribe("home", "sensors/kitchen/temp"));
        // Same request, wrong client ⇒ deny.
        assert!(!p.allows_mqtt_dynamic_subscribe("office", "sensors/kitchen/temp"));
        // Right client, broader request not covered by the allowed filter ⇒ deny
        // (the canonical over-grant trap: allow `sensors/+/temp`, request `#`).
        assert!(!p.allows_mqtt_dynamic_subscribe("home", "#"));
        assert!(!p.allows_mqtt_dynamic_subscribe("home", "sensors/+/humidity"));
    }

    #[test]
    fn brenn_dynamic_subscribe_grants_and_acl() {
        // Both matcher arms are exercised through the decision path (design §6.4:
        // "Prove both the Exact and Prefix matcher arm via ChannelMatcher"): a
        // `Prefix` and an `Exact` entry on the same list. The arms are not
        // behaviorally equivalent (`Exact` requires equality, `Prefix` does not),
        // so an inverted dispatch in `ChannelMatcher::matches` would be caught
        // here, not only in the acl.rs unit test of `matches` in isolation.
        let acls = AclSet {
            brenn_subscribe: vec![
                ChannelMatcher::Prefix("alerts".to_string()),
                ChannelMatcher::Exact("status.ready".to_string()),
            ],
            ..AclSet::default()
        };
        // Missing MessagingSubscribe transport grant.
        let only_dynamic = policy(&[AppCapability::DynamicSubscribe], acls.clone());
        assert!(!only_dynamic.allows_brenn_dynamic_subscribe("alerts.high"));
        // Missing DynamicSubscribe grant.
        let only_transport = policy(&[AppCapability::MessagingSubscribe], acls.clone());
        assert!(!only_transport.allows_brenn_dynamic_subscribe("alerts.high"));
        // Both grants present: each arm's happy path and its deny path.
        let both = policy(
            &[
                AppCapability::DynamicSubscribe,
                AppCapability::MessagingSubscribe,
            ],
            acls,
        );
        // Prefix arm: covered ⇒ allow; uncovered ⇒ deny.
        assert!(both.allows_brenn_dynamic_subscribe("alerts.high"));
        assert!(!both.allows_brenn_dynamic_subscribe("nope"));
        // Exact arm: exact channel ⇒ allow; a string merely sharing the prefix
        // of the Exact matcher ⇒ deny (proves Exact is not treated as Prefix).
        assert!(both.allows_brenn_dynamic_subscribe("status.ready"));
        assert!(!both.allows_brenn_dynamic_subscribe("status.ready.extra"));
    }

    #[test]
    fn brenn_dynamic_subscribe_empty_acl_denies() {
        let p = policy(
            &[
                AppCapability::DynamicSubscribe,
                AppCapability::MessagingSubscribe,
            ],
            AclSet::default(),
        );
        assert!(!p.allows_brenn_dynamic_subscribe("alerts"));
    }

    #[test]
    fn webhook_dynamic_subscribe_grants_and_acl() {
        let acls = AclSet {
            webhook: vec![WebhookMatcher {
                endpoint: "github".to_string(),
            }],
            ..AclSet::default()
        };
        // Missing Webhook transport grant.
        let only_dynamic = policy(&[AppCapability::DynamicSubscribe], acls.clone());
        assert!(!only_dynamic.allows_webhook_dynamic_subscribe("github"));
        // Missing DynamicSubscribe grant.
        let only_transport = policy(&[AppCapability::Webhook], acls.clone());
        assert!(!only_transport.allows_webhook_dynamic_subscribe("github"));
        // Both grants + exact endpoint ⇒ allowed; other endpoint ⇒ deny (exact).
        let both = policy(
            &[AppCapability::DynamicSubscribe, AppCapability::Webhook],
            acls,
        );
        assert!(both.allows_webhook_dynamic_subscribe("github"));
        assert!(!both.allows_webhook_dynamic_subscribe("gitlab"));
    }

    #[test]
    fn webhook_dynamic_subscribe_empty_acl_denies() {
        let p = policy(
            &[AppCapability::DynamicSubscribe, AppCapability::Webhook],
            AclSet::default(),
        );
        assert!(!p.allows_webhook_dynamic_subscribe("github"));
    }

    // --- Delivery-time decisions (design §2.2): subscription-holding
    //     authorization — transport grant + covering matcher, NO DynamicSubscribe.

    #[test]
    fn mqtt_delivery_requires_grant_and_matcher_not_dynamic() {
        let acls = AclSet {
            mqtt_subscribe: vec![MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "sensors/+/temp".to_string(),
            }],
            ..AclSet::default()
        };
        // Transport grant + covering matcher, WITHOUT DynamicSubscribe ⇒ allow.
        // This is the load-bearing assertion: delivery authorization must not
        // depend on the runtime-tool grant, or every static subscriber is denied.
        let no_dynamic = policy(&[AppCapability::MqttSubscribe], acls.clone());
        assert!(no_dynamic.allows_mqtt_delivery("home", "sensors/kitchen/temp"));
        // Missing the MqttSubscribe transport grant ⇒ deny (even with matcher).
        let no_grant = policy(&[AppCapability::DynamicSubscribe], acls.clone());
        assert!(!no_grant.allows_mqtt_delivery("home", "sensors/kitchen/temp"));
        // Grant held but matcher removed (empty ACL) ⇒ deny-by-default.
        let no_matcher = policy(&[AppCapability::MqttSubscribe], AclSet::default());
        assert!(!no_matcher.allows_mqtt_delivery("home", "sensors/kitchen/temp"));
        // Wrong client / over-broad request not covered ⇒ deny.
        let scoped = policy(&[AppCapability::MqttSubscribe], acls);
        assert!(!scoped.allows_mqtt_delivery("office", "sensors/kitchen/temp"));
        assert!(!scoped.allows_mqtt_delivery("home", "#"));
    }

    #[test]
    fn brenn_delivery_requires_grant_and_matcher_not_dynamic() {
        let acls = AclSet {
            brenn_subscribe: vec![ChannelMatcher::Prefix("alerts".to_string())],
            ..AclSet::default()
        };
        // Transport grant + matcher, no DynamicSubscribe ⇒ allow.
        let no_dynamic = policy(&[AppCapability::MessagingSubscribe], acls.clone());
        assert!(no_dynamic.allows_brenn_delivery("alerts.high"));
        // Missing MessagingSubscribe transport grant ⇒ deny.
        let no_grant = policy(&[AppCapability::DynamicSubscribe], acls.clone());
        assert!(!no_grant.allows_brenn_delivery("alerts.high"));
        // Grant held, matcher removed ⇒ deny.
        let no_matcher = policy(&[AppCapability::MessagingSubscribe], AclSet::default());
        assert!(!no_matcher.allows_brenn_delivery("alerts.high"));
        // Uncovered channel ⇒ deny.
        let scoped = policy(&[AppCapability::MessagingSubscribe], acls);
        assert!(!scoped.allows_brenn_delivery("status"));
    }

    #[test]
    fn webhook_delivery_requires_grant_and_matcher_not_dynamic() {
        let acls = AclSet {
            webhook: vec![WebhookMatcher {
                endpoint: "github".to_string(),
            }],
            ..AclSet::default()
        };
        // Transport grant + exact matcher, no DynamicSubscribe ⇒ allow.
        let no_dynamic = policy(&[AppCapability::Webhook], acls.clone());
        assert!(no_dynamic.allows_webhook_delivery("github"));
        // Missing Webhook transport grant ⇒ deny.
        let no_grant = policy(&[AppCapability::DynamicSubscribe], acls.clone());
        assert!(!no_grant.allows_webhook_delivery("github"));
        // Grant held, matcher removed ⇒ deny.
        let no_matcher = policy(&[AppCapability::Webhook], AclSet::default());
        assert!(!no_matcher.allows_webhook_delivery("github"));
        // Different endpoint ⇒ deny (exact match).
        let scoped = policy(&[AppCapability::Webhook], acls);
        assert!(!scoped.allows_webhook_delivery("gitlab"));
    }

    #[test]
    fn ephemeral_delivery_requires_grant_and_matcher_not_dynamic() {
        // Both matcher arms (Prefix + Exact) on the ephemeral_subscribe list,
        // exercised through the decision path. Bare-name matcher values, no scheme.
        let acls = AclSet {
            ephemeral_subscribe: vec![
                ChannelMatcher::Prefix("protobar".to_string()),
                ChannelMatcher::Exact("status.ready".to_string()),
            ],
            ..AclSet::default()
        };
        // Transport grant + covering matcher ⇒ allow.
        let granted = policy(&[AppCapability::EphemeralSubscribe], acls.clone());
        assert!(granted.allows_ephemeral_delivery("protobar-demo"));
        assert!(granted.allows_ephemeral_delivery("status.ready"));
        // Deliberate asymmetry: the gating grant is EphemeralSubscribe, NOT
        // MessagingSubscribe — the brenn-delivery grant must not authorize
        // ephemeral delivery.
        let wrong_grant = policy(&[AppCapability::MessagingSubscribe], acls.clone());
        assert!(!wrong_grant.allows_ephemeral_delivery("protobar-demo"));
        // Grant held, matcher removed ⇒ deny-by-default.
        let no_matcher = policy(&[AppCapability::EphemeralSubscribe], AclSet::default());
        assert!(!no_matcher.allows_ephemeral_delivery("protobar-demo"));
        // Covered vs uncovered, Exact-not-Prefix.
        let scoped = policy(&[AppCapability::EphemeralSubscribe], acls);
        assert!(!scoped.allows_ephemeral_delivery("nope"));
        assert!(!scoped.allows_ephemeral_delivery("status.ready.extra"));
    }

    #[test]
    fn ephemeral_publish_requires_grant_and_matcher() {
        let acls = AclSet {
            ephemeral_publish: vec![
                ChannelMatcher::Prefix("protobar".to_string()),
                ChannelMatcher::Exact("status.ready".to_string()),
            ],
            ..AclSet::default()
        };
        // Covering matcher present, but missing the EphemeralPublish grant ⇒ deny.
        // A different publish grant must not authorize ephemeral publish.
        let wrong_grant = policy(&[AppCapability::MessagingPublish], acls.clone());
        assert!(!wrong_grant.allows_ephemeral_publish("protobar-demo"));
        // Grant held but empty publish list ⇒ deny-by-default.
        let no_matcher = policy(&[AppCapability::EphemeralPublish], AclSet::default());
        assert!(!no_matcher.allows_ephemeral_publish("protobar-demo"));
        // Grant + covering matcher ⇒ allow; each arm's happy and deny path.
        let granted = policy(&[AppCapability::EphemeralPublish], acls);
        assert!(granted.allows_ephemeral_publish("protobar-demo"));
        assert!(!granted.allows_ephemeral_publish("nope"));
        assert!(granted.allows_ephemeral_publish("status.ready"));
        assert!(!granted.allows_ephemeral_publish("status.ready.extra"));
    }

    #[test]
    fn allows_channel_access_dispatches_by_prefix() {
        // One policy carrying a covering matcher + transport grant for each
        // transport; `allows_channel_access` must route each address to the right one.
        let acls = AclSet {
            mqtt_subscribe: vec![MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "sensors/+/temp".to_string(),
            }],
            brenn_subscribe: vec![ChannelMatcher::Prefix("alerts".to_string())],
            ephemeral_subscribe: vec![ChannelMatcher::Prefix("protobar".to_string())],
            webhook: vec![WebhookMatcher {
                endpoint: "github".to_string(),
            }],
            ..AclSet::default()
        };
        let p = policy(
            &[
                AppCapability::MqttSubscribe,
                AppCapability::MessagingSubscribe,
                AppCapability::EphemeralSubscribe,
                AppCapability::Webhook,
            ],
            acls,
        );
        // mqtt: routes through allows_mqtt_delivery (covered ⇒ allow, uncovered ⇒ deny).
        assert!(p.allows_channel_access("mqtt:home:sensors/kitchen/temp"));
        assert!(!p.allows_channel_access("mqtt:home:sensors/kitchen/humidity"));
        assert!(!p.allows_channel_access("mqtt:office:sensors/kitchen/temp"));
        // brenn: routes through allows_brenn_delivery.
        assert!(p.allows_channel_access("brenn:alerts.high"));
        assert!(!p.allows_channel_access("brenn:status"));
        // ephemeral: routes through allows_ephemeral_delivery (scheme stripped,
        // bare-name match; covered ⇒ allow, uncovered ⇒ deny).
        assert!(p.allows_channel_access("ephemeral:protobar-demo"));
        assert!(!p.allows_channel_access("ephemeral:status"));
        // webhook: routes through allows_webhook_delivery (exact).
        assert!(p.allows_channel_access("webhook:github"));
        assert!(!p.allows_channel_access("webhook:gitlab"));
    }

    #[test]
    fn allows_channel_access_denies_pwa_push_even_with_grant() {
        // pwa_push is egress-only: the access gate denies it unconditionally, even
        // for a policy that holds the PwaPush grant. Its tool surfaces are
        // authorized on the app's pwa_push config, not through this decision.
        let p = policy(&[AppCapability::PwaPush], AclSet::default());
        assert!(!p.allows_channel_access("pwa_push:alice"));
        assert!(!p.allows_channel_access("pwa_push:alice@laptop"));
    }

    #[test]
    fn allows_channel_access_denies_malformed_and_unknown_address() {
        // A policy that would grant broadly if the address parsed — proves the
        // deny comes from classification failure, not from a missing grant.
        let acls = AclSet {
            mqtt_subscribe: vec![MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "#".to_string(),
            }],
            brenn_subscribe: vec![ChannelMatcher::Prefix(String::new())],
            ephemeral_subscribe: vec![ChannelMatcher::Prefix(String::new())],
            ..AclSet::default()
        };
        let p = policy(
            &[
                AppCapability::MqttSubscribe,
                AppCapability::MessagingSubscribe,
                AppCapability::EphemeralSubscribe,
                AppCapability::Webhook,
            ],
            acls,
        );
        // Unknown / no prefix ⇒ deny.
        assert!(!p.allows_channel_access(""));
        assert!(!p.allows_channel_access("pwa:device"));
        assert!(!p.allows_channel_access("sensors/kitchen/temp"));
        // Malformed mqtt address (no client:topic separator) ⇒ deny, not panic.
        assert!(!p.allows_channel_access("mqtt:home-only-no-topic"));
        // Malformed mqtt address (empty topic) ⇒ deny.
        assert!(!p.allows_channel_access("mqtt:home:"));
        // A recognized `ephemeral:` prefix DOES classify and (with the broad
        // matcher + grant) is authorized — proving the denies above come from
        // classification failure, not a blanket deny of everything unlisted.
        assert!(p.allows_channel_access("ephemeral:anything"));
    }

    // --- Publish-side decisions (design §2.1): grant + covering matcher, the send
    //     analogue of the delivery family.

    #[test]
    fn brenn_publish_requires_grant_and_matcher() {
        // Both matcher arms (Exact + Prefix) on the same list, exercised through
        // the decision path — an inverted `ChannelMatcher::matches` dispatch would
        // be caught here, not only in the acl.rs unit test.
        let acls = AclSet {
            brenn_publish: vec![
                ChannelMatcher::Prefix("alerts".to_string()),
                ChannelMatcher::Exact("status.ready".to_string()),
            ],
            ..AclSet::default()
        };
        // Covering matcher present, but missing the MessagingPublish grant ⇒ deny.
        let no_grant = policy(&[AppCapability::MessagingSubscribe], acls.clone());
        assert!(!no_grant.allows_brenn_publish("alerts.high"));
        // Grant held but empty publish list ⇒ deny-by-default.
        let no_matcher = policy(&[AppCapability::MessagingPublish], AclSet::default());
        assert!(!no_matcher.allows_brenn_publish("alerts.high"));
        // Grant + covering matcher ⇒ allow; each arm's happy and deny path.
        let granted = policy(&[AppCapability::MessagingPublish], acls);
        // Prefix arm: covered ⇒ allow, uncovered ⇒ deny.
        assert!(granted.allows_brenn_publish("alerts.high"));
        assert!(!granted.allows_brenn_publish("status"));
        // Exact arm: exact channel ⇒ allow; a longer string sharing the prefix of
        // the Exact matcher ⇒ deny (proves Exact is not treated as Prefix).
        assert!(granted.allows_brenn_publish("status.ready"));
        assert!(!granted.allows_brenn_publish("status.ready.extra"));
    }

    #[test]
    fn mqtt_publish_requires_grant_and_matcher() {
        let acls = AclSet {
            mqtt_publish: vec![MqttClientMatcher {
                client: "home".to_string(),
            }],
            ..AclSet::default()
        };
        // Listed client, but missing the MqttPublish grant ⇒ deny.
        let no_grant = policy(&[AppCapability::MqttSubscribe], acls.clone());
        assert!(!no_grant.allows_mqtt_publish("home"));
        // Grant held but empty publish list ⇒ deny-by-default.
        let no_matcher = policy(&[AppCapability::MqttPublish], AclSet::default());
        assert!(!no_matcher.allows_mqtt_publish("home"));
        // Grant + listed client ⇒ allow; unlisted client ⇒ deny (client-scoped,
        // no topic dimension).
        let granted = policy(&[AppCapability::MqttPublish], acls);
        assert!(granted.allows_mqtt_publish("home"));
        assert!(!granted.allows_mqtt_publish("office"));
    }
}

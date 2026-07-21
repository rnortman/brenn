//! Resolution: build a resolved `AppPolicy` from an LLM app's operator-authored
//! explicit `grants` + `[app.acl.*]` config (access-control design §2.5.2/§2.5.3).
//!
//! This is the **explicit-config build**: the policy is constructed *solely* from
//! the operator's `grants`/`acl` (resolved OQ2 — no legacy-signal projection from
//! other resolved fields). Operator-authored config is validated fail-fast: a
//! duplicate grant, an invalid client slug, or a malformed topic filter **panics**
//! (CLAUDE.md robustness), mirroring WASM grant resolution
//! (`bootstrap/messaging.rs`).
//!
//! Backend-only, like the rest of `access`.

use indexmap::IndexMap;

use crate::access::acl::{
    AclSet, ChannelMatcher, MqttClientMatcher, MqttSubMatcher, WebhookMatcher,
};
use crate::access::raw::{AppAclRaw, ChannelMatcherRaw};
use crate::access::{AppCapability, AppPolicy, GrantSet};
use crate::mqtt::config::MqttClientConfig;

/// Build the resolved `AppPolicy` for an LLM app from its authored `grants` and
/// `[app.acl.*]` block.
///
/// `resolved_clients` is the already-resolved MQTT client map (Phase 6 output):
/// every `mqtt_subscribe`/`mqtt_publish` matcher's `client` slug is cross-checked
/// against it so an ACL referencing a nonexistent client fails fast at
/// resolution rather than silently never-matching at runtime (design §2.5.2).
///
/// # Panics
///
/// Panics (operator-authored config — fail-fast) on:
/// - a WASM-host (`Wasm*`) or `Integration` token in an LLM app's `grants` — these
///   are not part of the LLM-authorable grant vocabulary (design §2.2; authored
///   as `WasmGrant` on WASM components and mapped internally),
/// - a duplicate capability in `grants` (each grant must appear at most once),
/// - an MQTT subscribe/publish matcher with an invalid client slug or a slug that
///   does not name a configured MQTT client,
/// - an MQTT subscribe matcher with a malformed topic filter (bad wildcard
///   placement per `mqtt::address::validate_topic_filter_str`),
/// - a `brenn_subscribe`/`brenn_publish` channel matcher with an empty exact value
///   or empty/non-segment-boundary prefix,
/// - a `webhook` matcher with an empty endpoint slug.
///
/// `app_slug` is used only for diagnostic messages on panic.
pub fn build_app_policy(
    app_slug: &str,
    grants: &[AppCapability],
    acl: &AppAclRaw,
    resolved_clients: &IndexMap<String, MqttClientConfig>,
) -> AppPolicy {
    let mut grant_set = GrantSet::default();
    for &cap in grants {
        // Pin the LLM-authorable grant vocabulary: the unified `AppCapability`
        // enum derives `Deserialize` over the whole enum (so `grants =
        // ["wasm_store"]` parses), but the WASM-host caps and the reserved
        // `Integration` placeholder are not authorable on an LLM app — they are
        // authored as `WasmGrant` on WASM components and mapped internally, and
        // have no LLM-app enforcement (design §2.2). Reject them here (the
        // resolution boundary), fail-fast like every other operator-config error.
        match cap {
            AppCapability::WasmStore
            | AppCapability::WasmLog
            | AppCapability::WasmAlert
            | AppCapability::WasmConfig
            | AppCapability::Integration => panic!(
                "app {app_slug:?}: grant {cap:?} is not authorable on an LLM app's `grants` \
                 (WASM-host caps are authored as `WasmGrant`; `integration` is reserved) \
                 — design §2.2",
            ),
            // Surface-only capability, authored as `SurfaceGrant::Alert` on a
            // `[[surface]]` and mapped in `build_surface_policy`; it has no
            // enforcement point for an LLM app, so it is not authorable here.
            AppCapability::SurfaceAlert => panic!(
                "app {app_slug:?}: grant {cap:?} is not authorable on an LLM app's `grants` \
                 (surface alert is authored as `SurfaceGrant` on a `[[surface]]`)",
            ),
            // Surface-only capability, authored as `SurfaceGrant::Takeover` on a
            // `[[surface]]` and mapped in `build_surface_policy`; it has no
            // enforcement point for an LLM app, so it is not authorable here.
            AppCapability::SurfaceTakeover => panic!(
                "app {app_slug:?}: grant {cap:?} is not authorable on an LLM app's `grants` \
                 (surface takeover is authored as `SurfaceGrant` on a `[[surface]]`)",
            ),
            // `ephemeral_subscribe` is not authorable on an LLM app: no delivery
            // path carries ephemeral traffic to a conversation in v1, so the grant
            // has no enforcement point — dead config, and dead config is a lie we
            // fail fast on. Relaxing this is one line plus a delivery
            // path. `ephemeral_publish` *is* authorable (LLM apps may publish
            // ephemeral channels; a demo publisher depends on
            // it) — it falls into the OK arm below.
            AppCapability::EphemeralSubscribe => panic!(
                "app {app_slug:?}: grant {cap:?} is not authorable on an LLM app's `grants` \
                 (no ephemeral delivery path to a conversation in v1)",
            ),
            AppCapability::MessagingPublish
            | AppCapability::MessagingSubscribe
            | AppCapability::DynamicSubscribe
            | AppCapability::EphemeralPublish
            | AppCapability::MqttPublish
            | AppCapability::MqttSubscribe
            | AppCapability::Webhook
            | AppCapability::PwaPush => {}
        }
        let newly_inserted = grant_set.insert(cap);
        assert!(
            newly_inserted,
            "app {app_slug:?}: duplicate grant {cap:?} in `grants`",
        );
    }

    let owner = format!("app {app_slug:?}");
    let acls = AclSet {
        mqtt_subscribe: acl
            .mqtt_subscribe
            .iter()
            .map(|m| {
                validate_mqtt_client(app_slug, "mqtt_subscribe", &m.client, resolved_clients);
                crate::mqtt::address::validate_topic_filter_str(&m.topic_filter).unwrap_or_else(
                    |e| {
                        panic!(
                            "app {app_slug:?}: mqtt_subscribe matcher has invalid topic filter \
                             {:?}: {e}",
                            m.topic_filter,
                        )
                    },
                );
                MqttSubMatcher {
                    client: m.client.clone(),
                    topic_filter: m.topic_filter.clone(),
                }
            })
            .collect(),
        mqtt_publish: acl
            .mqtt_publish
            .iter()
            .map(|m| {
                validate_mqtt_client(app_slug, "mqtt_publish", &m.client, resolved_clients);
                MqttClientMatcher {
                    client: m.client.clone(),
                }
            })
            .collect(),
        brenn_subscribe: acl
            .brenn_subscribe
            .iter()
            .map(|m| resolve_channel(&owner, "brenn_subscribe", m))
            .collect(),
        brenn_publish: acl
            .brenn_publish
            .iter()
            .map(|m| resolve_channel(&owner, "brenn_publish", m))
            .collect(),
        // Ephemeral ACL lists. The `ephemeral_subscribe` grant
        // token boot-panics for LLM apps above and there is no
        // `ephemeral_subscribe` raw field (dead-config lockstep), so
        // the resolved `ephemeral_subscribe` list is always empty for an LLM app.
        // `ephemeral_publish` resolves from its raw field exactly like
        // `brenn_publish` — same `resolve_channel` validation, bare-name matcher
        // values.
        ephemeral_subscribe: Vec::new(),
        ephemeral_publish: acl
            .ephemeral_publish
            .iter()
            .map(|m| resolve_channel(&owner, "ephemeral_publish", m))
            .collect(),
        webhook: acl
            .webhook
            .iter()
            .map(|m| {
                assert!(
                    !m.endpoint.is_empty(),
                    "app {app_slug:?}: webhook matcher has an empty endpoint slug",
                );
                WebhookMatcher {
                    endpoint: m.endpoint.clone(),
                }
            })
            .collect(),
    };

    AppPolicy {
        grants: grant_set,
        acls,
        tool_grants: Default::default(),
    }
}

/// Build the resolved `AppPolicy` for a WASM component from its resolved
/// `WasmGrant` set and its authored `subscribe_acl`/`publish_acl` channel
/// matchers + `mqtt_publish_acl` client matchers (access-control design §2.5.4;
/// mqtt-egress-unify design §2.5).
///
/// The grant layer maps each `WasmGrant` onto its unified `AppCapability`
/// (`Ports → MessagingPublish`, `Store → WasmStore`, `Log → WasmLog`,
/// `Alert → WasmAlert`, `Config → WasmConfig`, `Mqtt → MqttPublish` —
/// design §2.5.4 / high-level §2.3).
/// This is a **separate** mapping from the `brenn-wasm::Capability` conversion at
/// the bootstrap linker seam (which Phase 0–1 do not touch); it exists so the
/// unified policy model spans both app kinds. WASM publishes to / subscribes from
/// `brenn:` channels (`brenn_subscribe`/`brenn_publish`, validated identically to
/// the LLM side via `resolve_channel`); with the `mqtt` grant, publishes MQTT
/// (`mqtt_publish`, the same `client`-keyed `MqttClientMatcher` as the LLM side —
/// mqtt-egress-unify design §2.5); and subscribes to inbound `mqtt:`/`webhook:`
/// channels (`mqtt_subscribe`/`webhook`, the same matcher types the LLM side
/// resolves into).
///
/// Like `subscribe_acl` (which derives `MessagingSubscribe`), a non-empty
/// `mqtt_subscribe_acl` derives the `MqttSubscribe` grant and a non-empty
/// `webhook_acl` derives the `Webhook` grant: there is no "subscribe" WIT
/// interface for these transports and therefore no `WasmGrant` token that maps to
/// them — a WASM consumer's inbound subscriptions are declared statically in
/// config, not exercised through a host function, so the presence of the ACL list
/// is the operator's statement that the consumer may hold such a subscription.
/// Empty list ⇒ no grant ⇒ deny-by-default at delivery.
///
/// `mqtt_publish_acl` and `mqtt_subscribe_acl` matcher client slugs are
/// charset-validated here (`is_valid_client_slug`); the cross-check that each slug
/// names a configured `[[mqtt_client]]` is a boot-time validation in `bootstrap`,
/// where the resolved client set is in scope — `build_wasm_policy` does not take a
/// `resolved_clients` map. `mqtt_subscribe_acl` topic filters are validated here
/// (`validate_topic_filter_str`).
///
/// The resolved policy backs **delivery-time ACL enforcement** over `Wasm(slug)`
/// subscribers: it is threaded into the `Messenger` (`wasm_policies`) and consulted
/// via `Messenger::subscriber_policy` at delivery (dynamic-sub-persistence design
/// §2.2). The broader WASM enforcement surface (linker-seam capabilities, etc.)
/// remains Phase 3 and is unaffected; only the delivery-time ACL gate consumes this
/// policy today (design §2.5.4).
///
/// The production caller (`resolve_wasm_consumers`, `bootstrap/messaging.rs`)
/// iterates an already-deduplicated `BTreeSet<WasmGrant>`, so a duplicate grant
/// cannot reach this function via the in-tree path. The duplicate check below is
/// nonetheless asserted (not silently absorbed) because this is a `pub`
/// `brenn-lib` API: an out-of-tree caller could pass a non-deduplicated iterator,
/// and silent idempotent insertion would mask the misconfiguration.
///
/// # Panics
///
/// Panics (operator-authored config — fail-fast) on:
/// - a duplicate `WasmGrant` in the supplied iterator (see above),
/// - a `brenn_subscribe`/`brenn_publish` channel matcher with an empty exact
///   value, an empty prefix, or a prefix that does not end at a segment boundary
///   (`/` or `.`) — same validation as the LLM channel matchers
///   (`resolve_channel`),
/// - an `mqtt_publish_acl` matcher with an invalid client slug (charset check,
///   same `is_valid_client_slug` rule the LLM side applies),
/// - an `mqtt_subscribe_acl` matcher with an invalid client slug (charset check)
///   or a malformed topic filter (`validate_topic_filter_str`),
/// - a `webhook_acl` matcher with an empty endpoint slug.
///
/// `slug` is used only for diagnostic messages on panic.
pub fn build_wasm_policy(
    slug: &str,
    grants: impl IntoIterator<Item = crate::messaging::config::WasmGrant>,
    acls: crate::access::raw::WasmAclsRaw<'_>,
) -> AppPolicy {
    use crate::messaging::config::WasmGrant;

    let crate::access::raw::WasmAclsRaw {
        subscribe: subscribe_acl,
        publish: publish_acl,
        mqtt_publish: mqtt_publish_acl,
        mqtt_subscribe: mqtt_subscribe_acl,
        webhook: webhook_acl,
    } = acls;

    let mut grant_set = GrantSet::default();
    for grant in grants {
        let cap = match grant {
            WasmGrant::Ports => AppCapability::MessagingPublish,
            WasmGrant::Store => AppCapability::WasmStore,
            WasmGrant::Log => AppCapability::WasmLog,
            WasmGrant::Alert => AppCapability::WasmAlert,
            WasmGrant::Config => AppCapability::WasmConfig,
            WasmGrant::Mqtt => AppCapability::MqttPublish,
        };
        // Fail-fast on a duplicate grant, mirroring `build_app_policy` above. The
        // production caller (`resolve_wasm_consumers`) iterates an already-deduped
        // `BTreeSet<WasmGrant>` and the grant→capability map is injective, so this
        // never fires on the in-tree path. It exists because this is a `pub`
        // `brenn-lib` API taking `impl IntoIterator` — an out-of-tree caller
        // (CLAUDE.md: out-of-tree components are first-class) could pass a
        // non-deduped iterator, and silent absorption would violate the project's
        // fail-fast posture.
        let newly_inserted = grant_set.insert(cap);
        assert!(
            newly_inserted,
            "wasm component {slug:?}: duplicate WasmGrant {grant:?} (maps to {cap:?}) \
             in grants iterator",
        );
    }

    // Imply the `MessagingSubscribe` transport grant when the operator authored any
    // `subscribe_acl` matcher. Unlike publish (which has the `Ports` WasmGrant →
    // `MessagingPublish`), there is no "subscribe" WIT interface and therefore no
    // `WasmGrant` that maps to `MessagingSubscribe`: a WASM consumer's input
    // subscriptions are declared statically in config, not exercised through a host
    // function. The delivery-time ACL gate (`allows_brenn_delivery`,
    // dynamic-sub-persistence design §2.2) requires *both* the `MessagingSubscribe`
    // grant *and* a covering `brenn_subscribe` matcher; without deriving the grant
    // here, every operator-blessed `[[wasm_consumer.subscription]]` would be denied
    // at delivery (no `WasmGrant` could ever satisfy the gate). The presence of a
    // `subscribe_acl` matcher is exactly the operator's statement that this consumer
    // may hold a subscription, so it is the right signal to derive the grant from.
    // Empty `subscribe_acl` ⇒ no grant ⇒ deny-by-default (a consumer with no declared
    // subscribe ACL has no subscription authorization), matching the design's
    // uniform delivery gate.
    if !subscribe_acl.is_empty() {
        grant_set.insert(AppCapability::MessagingSubscribe);
    }
    // Same derivation for the two inbound external transports: no `WasmGrant`
    // token maps to `MqttSubscribe`/`Webhook` (a WASM consumer's inbound
    // subscriptions are static config, not host-function calls), so the presence
    // of the ACL list is the operator's authorization signal — exactly as
    // `subscribe_acl` derives `MessagingSubscribe` above. Empty ⇒ no grant ⇒
    // deny-by-default at delivery.
    if !mqtt_subscribe_acl.is_empty() {
        grant_set.insert(AppCapability::MqttSubscribe);
    }
    if !webhook_acl.is_empty() {
        grant_set.insert(AppCapability::Webhook);
    }

    let owner = format!("wasm consumer {slug:?}");
    let acls = AclSet {
        brenn_subscribe: subscribe_acl
            .iter()
            .map(|m| resolve_channel(&owner, "subscribe_acl", m))
            .collect(),
        brenn_publish: publish_acl
            .iter()
            .map(|m| resolve_channel(&owner, "publish_acl", m))
            .collect(),
        // MQTT publish ACL: the same `client`-keyed `MqttClientMatcher` the LLM
        // side resolves into. Charset-validate the client slug here; the
        // configured-client cross-check is a boot validation where the resolved
        // client set is in scope.
        mqtt_publish: mqtt_publish_acl
            .iter()
            .map(|m| {
                assert!(
                    crate::mqtt::config::is_valid_client_slug(&m.client),
                    "wasm component {slug:?}: mqtt_publish_acl matcher has invalid client slug \
                     {:?}",
                    m.client,
                );
                MqttClientMatcher {
                    client: m.client.clone(),
                }
            })
            .collect(),
        // MQTT subscribe ACL: the same `(client, topic_filter)` `MqttSubMatcher`
        // the LLM side resolves into. Charset-validate the client slug and validate
        // the topic filter here; the configured-client cross-check is a boot
        // validation where the resolved client set is in scope.
        mqtt_subscribe: mqtt_subscribe_acl
            .iter()
            .map(|m| {
                assert!(
                    crate::mqtt::config::is_valid_client_slug(&m.client),
                    "wasm component {slug:?}: mqtt_subscribe_acl matcher has invalid client slug \
                     {:?}",
                    m.client,
                );
                crate::mqtt::address::validate_topic_filter_str(&m.topic_filter).unwrap_or_else(
                    |e| {
                        panic!(
                            "wasm component {slug:?}: mqtt_subscribe_acl matcher has invalid topic \
                             filter {:?}: {e}",
                            m.topic_filter,
                        )
                    },
                );
                MqttSubMatcher {
                    client: m.client.clone(),
                    topic_filter: m.topic_filter.clone(),
                }
            })
            .collect(),
        // Webhook subscribe ACL: the same endpoint-slug `WebhookMatcher` the LLM
        // side resolves into. Validate non-emptiness (parity with the LLM side).
        webhook: webhook_acl
            .iter()
            .map(|m| {
                assert!(
                    !m.endpoint.is_empty(),
                    "wasm component {slug:?}: webhook_acl matcher has an empty endpoint slug",
                );
                WebhookMatcher {
                    endpoint: m.endpoint.clone(),
                }
            })
            .collect(),
        ..AclSet::default()
    };

    AppPolicy {
        grants: grant_set,
        acls,
        tool_grants: Default::default(),
    }
}

/// Build the resolved `AppPolicy` for a `[[surface]]` bus participant from its
/// authored `SurfaceGrant` set and its four channel-matcher ACL lists.
///
/// This mirrors `build_wasm_policy` in placement and panic style but differs in
/// two deliberate ways:
///
/// - **Explicit grants, not derived.** `build_wasm_policy` derives
///   `MessagingSubscribe` from `subscribe_acl` presence because no `WasmGrant`
///   token maps to it. The surface grant vocabulary names all four transport
///   rights directly (`Subscribe → MessagingSubscribe`,
///   `Publish → MessagingPublish`, `EphemeralSubscribe → EphemeralSubscribe`,
///   `EphemeralPublish → EphemeralPublish`), so there is no missing-token gap
///   to paper over: a right is held iff its grant is authored, and
///   deny-by-default reads straight off the config.
/// - **Four ACL classes.** A surface may hold durable *and* ephemeral
///   subscribe/publish ACLs, so all four lists (`subscribe`/`publish` →
///   `brenn_subscribe`/`brenn_publish`; `ephemeral_subscribe`/`ephemeral_publish`
///   → the matching `AclSet` fields) resolve here, each via `resolve_channel`.
///
/// Grant/ACL inconsistency (a grant with no covering matcher, or matchers with
/// no grant) is **not** a boot panic — it matches today's LLM-app behavior,
/// where the two-factor policy check simply denies; the checks that matter
/// (binding coverage) live in surface boot-resolution.
///
/// # Panics
///
/// Panics (operator-authored config — fail-fast) on:
/// - a duplicate `SurfaceGrant` in the supplied iterator (each grant at most
///   once; mirrors `build_wasm_policy`, and this is a `pub` API an out-of-tree
///   caller could feed a non-deduplicated iterator),
/// - any of the four ACL lists containing a channel matcher with an empty exact
///   value, an empty prefix, or a non-segment-boundary prefix (same
///   `resolve_channel` validation as every other channel ACL).
///
/// `slug` is used only for diagnostic messages on panic.
pub fn build_surface_policy(
    slug: &str,
    grants: impl IntoIterator<Item = crate::messaging::config::SurfaceGrant>,
    subscribe_acl: &[ChannelMatcherRaw],
    publish_acl: &[ChannelMatcherRaw],
    ephemeral_subscribe_acl: &[ChannelMatcherRaw],
    ephemeral_publish_acl: &[ChannelMatcherRaw],
) -> AppPolicy {
    use crate::messaging::config::SurfaceGrant;

    let mut grant_set = GrantSet::default();
    for grant in grants {
        let cap = match grant {
            SurfaceGrant::Subscribe => AppCapability::MessagingSubscribe,
            SurfaceGrant::Publish => AppCapability::MessagingPublish,
            SurfaceGrant::EphemeralSubscribe => AppCapability::EphemeralSubscribe,
            SurfaceGrant::EphemeralPublish => AppCapability::EphemeralPublish,
            SurfaceGrant::Alert => AppCapability::SurfaceAlert,
            SurfaceGrant::Takeover => AppCapability::SurfaceTakeover,
        };
        // Fail-fast on a duplicate grant, mirroring `build_wasm_policy`. The
        // grant→capability map is injective, so a resolved (deduplicated) grant
        // set never trips this on the in-tree path; it exists because this is a
        // `pub` `brenn-lib` API taking `impl IntoIterator` — an out-of-tree
        // caller could pass a non-deduplicated iterator, and silent absorption
        // would violate the project's fail-fast posture.
        let newly_inserted = grant_set.insert(cap);
        assert!(
            newly_inserted,
            "surface {slug:?}: duplicate SurfaceGrant {grant:?} (maps to {cap:?}) \
             in grants iterator",
        );
    }

    let owner = format!("surface {slug:?}");
    let acls = AclSet {
        brenn_subscribe: subscribe_acl
            .iter()
            .map(|m| resolve_channel(&owner, "subscribe_acl", m))
            .collect(),
        brenn_publish: publish_acl
            .iter()
            .map(|m| resolve_channel(&owner, "publish_acl", m))
            .collect(),
        ephemeral_subscribe: ephemeral_subscribe_acl
            .iter()
            .map(|m| resolve_channel(&owner, "ephemeral_subscribe_acl", m))
            .collect(),
        ephemeral_publish: ephemeral_publish_acl
            .iter()
            .map(|m| resolve_channel(&owner, "ephemeral_publish_acl", m))
            .collect(),
        // A surface has no MQTT or webhook transports.
        ..AclSet::default()
    };

    AppPolicy {
        grants: grant_set,
        acls,
        tool_grants: Default::default(),
    }
}

/// Validate an MQTT matcher's client slug: correct charset **and** names a
/// configured MQTT client. Panics (operator-config, fail-fast) on either failure.
/// `list` names the ACL list for the diagnostic message.
fn validate_mqtt_client(
    app_slug: &str,
    list: &str,
    client: &str,
    resolved_clients: &IndexMap<String, MqttClientConfig>,
) {
    assert!(
        crate::mqtt::config::is_valid_client_slug(client),
        "app {app_slug:?}: {list} matcher has invalid client slug {client:?}",
    );
    assert!(
        resolved_clients.contains_key(client),
        "app {app_slug:?}: {list} matcher names unconfigured MQTT client {client:?} \
         (no matching [[mqtt_client]])",
    );
}

/// Convert a raw `brenn:` channel matcher into its resolved form, validating the
/// matcher string. `list` names the ACL list for the diagnostic message.
///
/// Panics (operator-config, fail-fast) on:
/// - an empty exact value (matches only the empty channel — never useful),
/// - an empty prefix (a universal match — silent over-grant on the
///   attacker-influenceable subscribe path, design §1.1),
/// - a prefix that does not end at a segment boundary (`/` or `.`) — a bare
///   byte-prefix like `"alert"` would match `"alerts"`, over-granting beyond the
///   intended `alert/…`/`alert.…` namespace (design §1.1, high-level fm4).
///
/// The `owner` argument is a pre-formatted participant label such as
/// `app "home"`, `wasm consumer "filter"`, or `surface "deskbar"`. The fail-fast
/// diagnostic embeds it so the panic points the operator at the exact config
/// block that owns the offending matcher, rather than a hardcoded `app` that
/// would mislead surface and wasm callers.
fn resolve_channel(owner: &str, list: &str, raw: &ChannelMatcherRaw) -> ChannelMatcher {
    match raw {
        ChannelMatcherRaw::Exact(s) => {
            assert!(!s.is_empty(), "{owner}: {list} exact matcher is empty",);
            ChannelMatcher::Exact(s.clone())
        }
        ChannelMatcherRaw::Prefix(p) => {
            assert!(
                !p.is_empty(),
                "{owner}: {list} prefix matcher is empty (would match every channel)",
            );
            assert!(
                p.ends_with('/') || p.ends_with('.'),
                "{owner}: {list} prefix matcher {p:?} must end at a segment boundary \
                 (`/` or `.`) so it cannot over-match a sibling namespace",
            );
            ChannelMatcher::Prefix(p.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::raw::{
        MqttClientMatcherRaw, MqttSubMatcherRaw, WasmAclsRaw, WebhookMatcherRaw,
    };

    /// A minimal resolved `MqttClientConfig` for the given slug, for the
    /// matcher-client cross-check (only the map key matters).
    fn test_client(slug: &str) -> MqttClientConfig {
        crate::mqtt::test_support::test_client_config(slug)
    }

    /// A resolved-client map containing the given slugs.
    fn clients(slugs: &[&str]) -> IndexMap<String, MqttClientConfig> {
        slugs
            .iter()
            .map(|s| (s.to_string(), test_client(s)))
            .collect()
    }

    #[test]
    fn builds_grants_and_every_matcher_kind() {
        let grants = vec![
            AppCapability::DynamicSubscribe,
            AppCapability::MqttSubscribe,
            AppCapability::MessagingSubscribe,
            AppCapability::Webhook,
        ];
        let acl = AppAclRaw {
            mqtt_subscribe: vec![MqttSubMatcherRaw {
                client: "home".to_string(),
                topic_filter: "sensors/+/temp".to_string(),
            }],
            mqtt_publish: vec![MqttClientMatcherRaw {
                client: "office".to_string(),
            }],
            brenn_subscribe: vec![
                ChannelMatcherRaw::Prefix("alerts.".to_string()),
                ChannelMatcherRaw::Exact("status.ready".to_string()),
            ],
            brenn_publish: vec![ChannelMatcherRaw::Exact("outbox".to_string())],
            ephemeral_publish: vec![ChannelMatcherRaw::Exact("protobar-demo".to_string())],
            webhook: vec![WebhookMatcherRaw {
                endpoint: "github".to_string(),
            }],
        };

        let policy = build_app_policy("home", &grants, &acl, &clients(&["home", "office"]));

        // Grants round-trip.
        assert!(policy.has_grant(AppCapability::DynamicSubscribe));
        assert!(policy.has_grant(AppCapability::MqttSubscribe));
        assert!(policy.has_grant(AppCapability::MessagingSubscribe));
        assert!(policy.has_grant(AppCapability::Webhook));
        assert!(!policy.has_grant(AppCapability::MqttPublish));

        // Matchers convert to their resolved forms.
        assert_eq!(
            policy.acls.mqtt_subscribe,
            vec![MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "sensors/+/temp".to_string(),
            }]
        );
        assert_eq!(
            policy.acls.mqtt_publish,
            vec![MqttClientMatcher {
                client: "office".to_string(),
            }]
        );
        assert_eq!(
            policy.acls.brenn_subscribe,
            vec![
                ChannelMatcher::Prefix("alerts.".to_string()),
                ChannelMatcher::Exact("status.ready".to_string()),
            ]
        );
        assert_eq!(
            policy.acls.brenn_publish,
            vec![ChannelMatcher::Exact("outbox".to_string())]
        );
        assert_eq!(
            policy.acls.webhook,
            vec![WebhookMatcher {
                endpoint: "github".to_string(),
            }]
        );

        // End-to-end: the built policy makes the right ALLOW decisions.
        assert!(policy.allows_mqtt_dynamic_subscribe("home", "sensors/kitchen/temp"));
        assert!(policy.allows_brenn_dynamic_subscribe("alerts.high"));
        assert!(policy.allows_webhook_dynamic_subscribe("github"));

        // ...and the right DENY decisions, so a regression that flips a decision
        // to always-allow (or ignores the client/filter scope) is caught at the
        // resolution-round-trip level, not only in the isolated model tests.
        // Wrong client (matcher is for `home`).
        assert!(!policy.allows_mqtt_dynamic_subscribe("other", "sensors/kitchen/temp"));
        // Requested filter broader than the allowed `sensors/+/temp`.
        assert!(!policy.allows_mqtt_dynamic_subscribe("home", "sensors/#"));
        // Brenn channel matching neither the `alerts.` prefix nor the exact entry.
        assert!(!policy.allows_brenn_dynamic_subscribe("noalerts.x"));
        // Webhook endpoint with no matcher.
        assert!(!policy.allows_webhook_dynamic_subscribe("gitlab"));
    }

    #[test]
    fn empty_grants_and_acl_yields_default_deny() {
        let policy = build_app_policy("home", &[], &AppAclRaw::default(), &clients(&["home"]));
        assert!(!policy.has_grant(AppCapability::DynamicSubscribe));
        assert!(policy.acls.mqtt_subscribe.is_empty());
        assert!(policy.acls.brenn_subscribe.is_empty());
        // Deny-by-default holds for every transport, not just MQTT (design §6.1).
        assert!(!policy.allows_mqtt_dynamic_subscribe("home", "sensors/temp"));
        assert!(!policy.allows_brenn_dynamic_subscribe("anything"));
        assert!(!policy.allows_webhook_dynamic_subscribe("anything"));
    }

    #[test]
    #[should_panic(expected = "duplicate grant")]
    fn duplicate_grant_panics() {
        let grants = vec![
            AppCapability::DynamicSubscribe,
            AppCapability::DynamicSubscribe,
        ];
        build_app_policy("home", &grants, &AppAclRaw::default(), &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "not authorable on an LLM app")]
    fn wasm_host_grant_token_in_llm_grants_panics() {
        // A WASM-host capability token deserializes from an LLM `grants` list (the
        // whole `AppCapability` enum derives Deserialize), but it is not part of
        // the LLM-authorable vocabulary — resolution must reject it (design §2.2).
        let grants = vec![AppCapability::WasmStore];
        build_app_policy("home", &grants, &AppAclRaw::default(), &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "not authorable on an LLM app")]
    fn integration_grant_token_in_llm_grants_panics() {
        // The reserved `Integration` placeholder likewise has no LLM-app
        // enforcement and must be rejected at the resolution boundary.
        let grants = vec![AppCapability::Integration];
        build_app_policy("home", &grants, &AppAclRaw::default(), &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "not authorable on an LLM app")]
    fn ephemeral_subscribe_grant_token_in_llm_grants_panics() {
        // `ephemeral_subscribe` deserializes from an LLM `grants` list but has no
        // enforcement point (no ephemeral delivery path to a conversation in v1) —
        // dead config, rejected at the resolution boundary.
        let grants = vec![AppCapability::EphemeralSubscribe];
        build_app_policy("home", &grants, &AppAclRaw::default(), &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "not authorable on an LLM app")]
    fn surface_alert_grant_token_in_llm_grants_panics() {
        // `SurfaceAlert` deserializes from an LLM `grants` list (whole enum derives
        // Deserialize) but is a surface-only capability authored as `SurfaceGrant`
        // on a `[[surface]]`; it has no LLM-app enforcement point and must be
        // rejected at the resolution boundary.
        let grants = vec![AppCapability::SurfaceAlert];
        build_app_policy("home", &grants, &AppAclRaw::default(), &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "not authorable on an LLM app")]
    fn surface_takeover_grant_token_in_llm_grants_panics() {
        // `SurfaceTakeover` deserializes from an LLM `grants` list but is a
        // surface-only capability authored as `SurfaceGrant` on a `[[surface]]`;
        // it has no LLM-app enforcement point and must be rejected at the
        // resolution boundary.
        let grants = vec![AppCapability::SurfaceTakeover];
        build_app_policy("home", &grants, &AppAclRaw::default(), &clients(&[]));
    }

    #[test]
    fn ephemeral_publish_grant_token_in_llm_grants_is_authorable() {
        // `ephemeral_publish` IS authorable on an LLM app (a demo publisher
        // depends on it): resolution must accept it without panicking.
        // The `ephemeral_publish` ACL *matcher* list resolves separately; the grant
        // alone (no matchers) simply denies by default — accepted here.
        let grants = vec![AppCapability::EphemeralPublish];
        let policy = build_app_policy("home", &grants, &AppAclRaw::default(), &clients(&[]));
        assert!(policy.has_grant(AppCapability::EphemeralPublish));
    }

    #[test]
    #[should_panic(expected = "invalid client slug")]
    fn invalid_mqtt_subscribe_client_slug_panics() {
        let acl = AppAclRaw {
            mqtt_subscribe: vec![MqttSubMatcherRaw {
                // Spaces are not in the valid slug charset.
                client: "not a slug".to_string(),
                topic_filter: "sensors/#".to_string(),
            }],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "unconfigured MQTT client")]
    fn mqtt_subscribe_matcher_names_unconfigured_client_panics() {
        // Syntactically valid slug, but no matching [[mqtt_client]]: a silent
        // never-match (deny where the operator intended allow) at runtime, so
        // reject at resolution (design §2.5.2).
        let acl = AppAclRaw {
            mqtt_subscribe: vec![MqttSubMatcherRaw {
                client: "nonexistent".to_string(),
                topic_filter: "sensors/#".to_string(),
            }],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&["home"]));
    }

    #[test]
    #[should_panic(expected = "invalid topic filter")]
    fn malformed_mqtt_subscribe_filter_panics() {
        let acl = AppAclRaw {
            mqtt_subscribe: vec![MqttSubMatcherRaw {
                client: "home".to_string(),
                // `#` must be terminal — this is malformed.
                topic_filter: "sensors/#/extra".to_string(),
            }],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&["home"]));
    }

    #[test]
    #[should_panic(expected = "invalid client slug")]
    fn invalid_mqtt_publish_client_slug_panics() {
        let acl = AppAclRaw {
            mqtt_publish: vec![MqttClientMatcherRaw {
                client: "bad slug".to_string(),
            }],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "unconfigured MQTT client")]
    fn mqtt_publish_matcher_names_unconfigured_client_panics() {
        let acl = AppAclRaw {
            mqtt_publish: vec![MqttClientMatcherRaw {
                client: "nonexistent".to_string(),
            }],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&["home"]));
    }

    #[test]
    #[should_panic(expected = "prefix matcher is empty")]
    fn empty_brenn_prefix_panics() {
        // An empty prefix is a universal match (every channel `starts_with ""`):
        // a silent over-grant on the attacker-influenceable subscribe path.
        let acl = AppAclRaw {
            brenn_subscribe: vec![ChannelMatcherRaw::Prefix(String::new())],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "must end at a segment boundary")]
    fn non_boundary_brenn_prefix_panics() {
        // `Prefix("alert")` byte-matches `"alerts"` — over-grants beyond the
        // intended `alert.`/`alert/` namespace. Require a trailing separator.
        let acl = AppAclRaw {
            brenn_subscribe: vec![ChannelMatcherRaw::Prefix("alert".to_string())],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&[]));
    }

    #[test]
    fn boundary_brenn_prefix_resolves() {
        // A prefix ending at a `/` or `.` boundary is accepted on both lists.
        let acl = AppAclRaw {
            brenn_subscribe: vec![ChannelMatcherRaw::Prefix("alerts.".to_string())],
            brenn_publish: vec![ChannelMatcherRaw::Prefix("outbox/".to_string())],
            ..AppAclRaw::default()
        };
        let policy = build_app_policy("home", &[], &acl, &clients(&[]));
        assert_eq!(
            policy.acls.brenn_subscribe,
            vec![ChannelMatcher::Prefix("alerts.".to_string())]
        );
        assert_eq!(
            policy.acls.brenn_publish,
            vec![ChannelMatcher::Prefix("outbox/".to_string())]
        );
    }

    #[test]
    #[should_panic(expected = "exact matcher is empty")]
    fn empty_brenn_exact_panics() {
        let acl = AppAclRaw {
            brenn_subscribe: vec![ChannelMatcherRaw::Exact(String::new())],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&[]));
    }

    #[test]
    fn ephemeral_publish_acl_resolves_and_authorizes() {
        // The LLM-authorable `ephemeral_publish` ACL list resolves through the same
        // `resolve_channel` path as `brenn_publish`, into `AclSet.ephemeral_publish`
        // with bare-name matcher values. Grant + covering matcher
        // authorize; deny-by-default holds for an unlisted channel.
        let grants = vec![AppCapability::EphemeralPublish];
        let acl = AppAclRaw {
            ephemeral_publish: vec![
                ChannelMatcherRaw::Exact("protobar-demo".to_string()),
                ChannelMatcherRaw::Prefix("bar.".to_string()),
            ],
            ..AppAclRaw::default()
        };
        let policy = build_app_policy("home", &grants, &acl, &clients(&[]));
        assert_eq!(
            policy.acls.ephemeral_publish,
            vec![
                ChannelMatcher::Exact("protobar-demo".to_string()),
                ChannelMatcher::Prefix("bar.".to_string()),
            ]
        );
        // The `ephemeral_subscribe` list stays empty (no raw field).
        assert!(policy.acls.ephemeral_subscribe.is_empty());
        // Grant + covering matcher ⇒ ALLOW; bare-name values match against the
        // scheme-stripped channel.
        assert!(policy.allows_ephemeral_publish("protobar-demo"));
        assert!(policy.allows_ephemeral_publish("bar.high"));
        // Unlisted channel ⇒ deny-by-default.
        assert!(!policy.allows_ephemeral_publish("other"));
    }

    #[test]
    fn ephemeral_publish_grant_without_matchers_denies_by_default() {
        // Grant held, empty matcher list ⇒ every channel denied (`.any` over empty
        // = false). An `ephemeral_publish` grant with no matchers is deny-by-default,
        // deliberately not a boot panic (grant/ACL inconsistency is denied, not
        // rejected — the two-factor policy check handles it at delivery/publish time).
        let grants = vec![AppCapability::EphemeralPublish];
        let policy = build_app_policy("home", &grants, &AppAclRaw::default(), &clients(&[]));
        assert!(policy.has_grant(AppCapability::EphemeralPublish));
        assert!(policy.acls.ephemeral_publish.is_empty());
        assert!(!policy.allows_ephemeral_publish("protobar-demo"));
    }

    #[test]
    #[should_panic(expected = "must end at a segment boundary")]
    fn non_boundary_ephemeral_publish_prefix_panics() {
        // The same channel-matcher validation `resolve_channel` applies to the
        // brenn lists applies to `ephemeral_publish`: a non-boundary
        // prefix over-matches a sibling namespace and must panic.
        let acl = AppAclRaw {
            ephemeral_publish: vec![ChannelMatcherRaw::Prefix("bar".to_string())],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "exact matcher is empty")]
    fn empty_ephemeral_publish_exact_panics() {
        let acl = AppAclRaw {
            ephemeral_publish: vec![ChannelMatcherRaw::Exact(String::new())],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&[]));
    }

    #[test]
    #[should_panic(expected = "empty endpoint slug")]
    fn empty_webhook_endpoint_panics() {
        let acl = AppAclRaw {
            webhook: vec![WebhookMatcherRaw {
                endpoint: String::new(),
            }],
            ..AppAclRaw::default()
        };
        build_app_policy("home", &[], &acl, &clients(&[]));
    }

    // --- WASM policy build (§2.5.4) ---

    use crate::messaging::config::WasmGrant;

    #[test]
    fn wasm_grants_map_to_unified_capabilities() {
        // Every WasmGrant maps to its unified AppCapability (§2.5.4 table); a grant
        // not in the set is denied (deny-by-default).
        let policy = build_wasm_policy(
            "proc",
            [
                WasmGrant::Ports,
                WasmGrant::Store,
                WasmGrant::Log,
                WasmGrant::Alert,
                WasmGrant::Config,
                WasmGrant::Mqtt,
            ],
            WasmAclsRaw::default(),
        );
        assert!(policy.has_grant(AppCapability::MessagingPublish)); // Ports
        assert!(policy.has_grant(AppCapability::WasmStore));
        assert!(policy.has_grant(AppCapability::WasmLog));
        assert!(policy.has_grant(AppCapability::WasmAlert));
        assert!(policy.has_grant(AppCapability::WasmConfig));
        assert!(policy.has_grant(AppCapability::MqttPublish)); // Mqtt
        // An ungranted capability is denied.
        assert!(!policy.has_grant(AppCapability::MessagingSubscribe));
        assert!(!policy.has_grant(AppCapability::DynamicSubscribe));
    }

    #[test]
    fn wasm_subset_of_grants_only_sets_those() {
        // A partial grant set maps only the granted variants.
        let policy = build_wasm_policy("proc", [WasmGrant::Log], WasmAclsRaw::default());
        assert!(policy.has_grant(AppCapability::WasmLog));
        assert!(!policy.has_grant(AppCapability::MessagingPublish));
        assert!(!policy.has_grant(AppCapability::WasmStore));
        assert!(!policy.has_grant(AppCapability::WasmAlert));
        assert!(!policy.has_grant(AppCapability::WasmConfig));
    }

    #[test]
    fn wasm_channel_acls_resolve_to_brenn_lists() {
        // subscribe_acl/publish_acl land on brenn_subscribe/brenn_publish; with no
        // mqtt_subscribe_acl/webhook_acl authored, those lists stay empty (the
        // separate mqtt_subscribe/webhook derivation is exercised below).
        let subscribe_acl = vec![
            ChannelMatcherRaw::Exact("inbox".to_string()),
            ChannelMatcherRaw::Prefix("alerts.".to_string()),
        ];
        let publish_acl = vec![ChannelMatcherRaw::Exact("outbox".to_string())];
        let policy = build_wasm_policy(
            "proc",
            [WasmGrant::Ports],
            WasmAclsRaw {
                subscribe: &subscribe_acl,
                publish: &publish_acl,
                ..Default::default()
            },
        );
        assert_eq!(
            policy.acls.brenn_subscribe,
            vec![
                ChannelMatcher::Exact("inbox".to_string()),
                ChannelMatcher::Prefix("alerts.".to_string()),
            ]
        );
        assert_eq!(
            policy.acls.brenn_publish,
            vec![ChannelMatcher::Exact("outbox".to_string())]
        );
        assert!(policy.acls.mqtt_subscribe.is_empty());
        assert!(policy.acls.mqtt_publish.is_empty());
        assert!(policy.acls.webhook.is_empty());
    }

    #[test]
    fn wasm_subscribe_acl_derives_messaging_subscribe_grant_and_passes_delivery() {
        // Production-path regression guard (security-2): a WASM consumer's policy
        // is built ONLY via `build_wasm_policy`. There is no `WasmGrant` that maps
        // to `MessagingSubscribe`, so before this fix a subscribed consumer could
        // never pass `allows_channel_access` and every `[[wasm_consumer.subscription]]`
        // silently stopped delivering. A non-empty `subscribe_acl` now derives the
        // `MessagingSubscribe` grant, and the delivery gate passes for a covered
        // channel.
        let subscribe_acl = vec![ChannelMatcherRaw::Exact("inbox".to_string())];
        let policy = build_wasm_policy(
            "proc",
            [],
            WasmAclsRaw {
                subscribe: &subscribe_acl,
                ..Default::default()
            },
        );
        assert!(
            policy.has_grant(AppCapability::MessagingSubscribe),
            "non-empty subscribe_acl must derive the MessagingSubscribe grant"
        );
        // The delivery gate (transport grant + covering matcher) now passes for a
        // brenn: channel the matcher covers, and denies one it does not.
        assert!(policy.allows_channel_access("brenn:inbox"));
        assert!(!policy.allows_channel_access("brenn:other"));
    }

    #[test]
    fn wasm_empty_yields_default_deny() {
        let policy = build_wasm_policy("proc", [], WasmAclsRaw::default());
        assert!(!policy.has_grant(AppCapability::MessagingPublish));
        // Empty subscribe_acl ⇒ no derived MessagingSubscribe grant ⇒ no
        // subscription authorization (deny-by-default at delivery, security-2).
        assert!(!policy.has_grant(AppCapability::MessagingSubscribe));
        assert!(!policy.allows_channel_access("brenn:anything"));
        assert!(policy.acls.brenn_subscribe.is_empty());
        assert!(policy.acls.brenn_publish.is_empty());
    }

    #[test]
    #[should_panic(expected = "duplicate WasmGrant")]
    fn wasm_duplicate_grant_panics() {
        // The in-tree caller passes a deduped BTreeSet, but this `pub` API also
        // accepts a raw iterator; a duplicate must fail fast rather than be
        // silently absorbed (mirrors build_app_policy's duplicate check).
        build_wasm_policy(
            "proc",
            [WasmGrant::Log, WasmGrant::Log],
            WasmAclsRaw::default(),
        );
    }

    #[test]
    #[should_panic(expected = "must end at a segment boundary")]
    fn wasm_non_boundary_subscribe_prefix_panics() {
        // The same channel-matcher validation as the LLM side applies to WASM ACLs.
        build_wasm_policy(
            "proc",
            [],
            WasmAclsRaw {
                subscribe: &[ChannelMatcherRaw::Prefix("alert".to_string())],
                ..Default::default()
            },
        );
    }

    #[test]
    #[should_panic(expected = "exact matcher is empty")]
    fn wasm_empty_publish_exact_panics() {
        build_wasm_policy(
            "proc",
            [],
            WasmAclsRaw {
                publish: &[ChannelMatcherRaw::Exact(String::new())],
                ..Default::default()
            },
        );
    }

    #[test]
    fn wasm_mqtt_publish_acl_resolves_to_mqtt_publish_list() {
        // A WASM consumer's `mqtt_publish_acl` resolves into the same
        // `AppPolicy.acls.mqtt_publish` list, with the same `client`-keyed
        // `MqttClientMatcher`, that the LLM `[[app.acl.mqtt_publish]]` block does
        // (mqtt-egress-unify design §2.5 / §4 "Policy resolution"). The matcher type
        // and field are identical across both caller kinds — the load-bearing
        // unification.
        let mqtt_publish_acl = vec![
            MqttClientMatcherRaw {
                client: "home".to_string(),
            },
            MqttClientMatcherRaw {
                client: "office".to_string(),
            },
        ];
        let policy = build_wasm_policy(
            "proc",
            [WasmGrant::Mqtt],
            WasmAclsRaw {
                mqtt_publish: &mqtt_publish_acl,
                ..Default::default()
            },
        );
        assert_eq!(
            policy.acls.mqtt_publish,
            vec![
                MqttClientMatcher {
                    client: "home".to_string(),
                },
                MqttClientMatcher {
                    client: "office".to_string(),
                },
            ]
        );
        // The grant + a covering matcher authorize that client; deny-by-default
        // holds for an unlisted one.
        assert!(policy.allows_mqtt_publish("home"));
        assert!(policy.allows_mqtt_publish("office"));
        assert!(!policy.allows_mqtt_publish("garage"));
    }

    #[test]
    fn wasm_mqtt_publish_acl_empty_denies_by_default() {
        // Grant held, empty matcher list ⇒ every client denied (`.any` over empty
        // = false), the deny-by-default invariant (design §3.4).
        let policy = build_wasm_policy("proc", [WasmGrant::Mqtt], WasmAclsRaw::default());
        assert!(policy.has_grant(AppCapability::MqttPublish));
        assert!(policy.acls.mqtt_publish.is_empty());
        assert!(!policy.allows_mqtt_publish("home"));
    }

    #[test]
    #[should_panic(expected = "invalid client slug")]
    fn wasm_mqtt_publish_acl_invalid_client_slug_panics() {
        // Charset validation at resolution, mirroring the LLM side's
        // `validate_mqtt_client` charset check (mqtt-egress-unify design §2.5).
        build_wasm_policy(
            "proc",
            [WasmGrant::Mqtt],
            WasmAclsRaw {
                mqtt_publish: &[MqttClientMatcherRaw {
                    client: "bad:slug".to_string(),
                }],
                ..Default::default()
            },
        );
    }

    #[test]
    fn wasm_mqtt_subscribe_acl_derives_grant_resolves_and_gates_delivery() {
        // A non-empty `mqtt_subscribe_acl` derives the `MqttSubscribe` grant (no
        // WasmGrant maps to it) and resolves into the same `(client, topic_filter)`
        // `MqttSubMatcher` the LLM side uses. The delivery gate passes for a covered
        // `mqtt:` channel and denies wrong-client / broader-filter requests.
        let mqtt_subscribe_acl = vec![MqttSubMatcherRaw {
            client: "home".to_string(),
            topic_filter: "sensors/+/temp".to_string(),
        }];
        let policy = build_wasm_policy(
            "proc",
            [],
            WasmAclsRaw {
                mqtt_subscribe: &mqtt_subscribe_acl,
                ..Default::default()
            },
        );
        assert!(
            policy.has_grant(AppCapability::MqttSubscribe),
            "non-empty mqtt_subscribe_acl must derive the MqttSubscribe grant"
        );
        assert_eq!(
            policy.acls.mqtt_subscribe,
            vec![MqttSubMatcher {
                client: "home".to_string(),
                topic_filter: "sensors/+/temp".to_string(),
            }]
        );
        // Covered inbound channel delivers; wrong client / broader filter deny.
        assert!(policy.allows_channel_access("mqtt:home:sensors/kitchen/temp"));
        assert!(!policy.allows_channel_access("mqtt:other:sensors/kitchen/temp"));
        assert!(!policy.allows_channel_access("mqtt:home:sensors/kitchen/humidity"));
    }

    #[test]
    fn wasm_mqtt_subscribe_acl_empty_yields_no_grant_and_default_deny() {
        // Empty list ⇒ no derived grant ⇒ deny-by-default at delivery.
        let policy = build_wasm_policy("proc", [], WasmAclsRaw::default());
        assert!(!policy.has_grant(AppCapability::MqttSubscribe));
        assert!(policy.acls.mqtt_subscribe.is_empty());
        assert!(!policy.allows_channel_access("mqtt:home:sensors/kitchen/temp"));
    }

    #[test]
    #[should_panic(expected = "invalid client slug")]
    fn wasm_mqtt_subscribe_acl_invalid_client_slug_panics() {
        build_wasm_policy(
            "proc",
            [],
            WasmAclsRaw {
                mqtt_subscribe: &[MqttSubMatcherRaw {
                    client: "bad:slug".to_string(),
                    topic_filter: "sensors/#".to_string(),
                }],
                ..Default::default()
            },
        );
    }

    #[test]
    #[should_panic(expected = "invalid topic filter")]
    fn wasm_mqtt_subscribe_acl_malformed_filter_panics() {
        build_wasm_policy(
            "proc",
            [],
            WasmAclsRaw {
                mqtt_subscribe: &[MqttSubMatcherRaw {
                    client: "home".to_string(),
                    // `#` must be terminal — malformed.
                    topic_filter: "sensors/#/extra".to_string(),
                }],
                ..Default::default()
            },
        );
    }

    #[test]
    fn wasm_webhook_acl_derives_grant_resolves_and_gates_delivery() {
        // A non-empty `webhook_acl` derives the `Webhook` grant (no WasmGrant maps
        // to it) and resolves into the same endpoint-slug `WebhookMatcher` the LLM
        // side uses. The delivery gate passes for a covered `webhook:` channel and
        // denies an uncovered endpoint.
        let webhook_acl = vec![WebhookMatcherRaw {
            endpoint: "push-alice".to_string(),
        }];
        let policy = build_wasm_policy(
            "proc",
            [],
            WasmAclsRaw {
                webhook: &webhook_acl,
                ..Default::default()
            },
        );
        assert!(
            policy.has_grant(AppCapability::Webhook),
            "non-empty webhook_acl must derive the Webhook grant"
        );
        assert_eq!(
            policy.acls.webhook,
            vec![WebhookMatcher {
                endpoint: "push-alice".to_string(),
            }]
        );
        assert!(policy.allows_channel_access("webhook:push-alice"));
        assert!(!policy.allows_channel_access("webhook:other"));
    }

    #[test]
    fn wasm_webhook_acl_empty_yields_no_grant_and_default_deny() {
        // Empty list ⇒ no derived grant ⇒ deny-by-default at delivery.
        let policy = build_wasm_policy("proc", [], WasmAclsRaw::default());
        assert!(!policy.has_grant(AppCapability::Webhook));
        assert!(policy.acls.webhook.is_empty());
        assert!(!policy.allows_channel_access("webhook:push-alice"));
    }

    #[test]
    #[should_panic(expected = "empty endpoint slug")]
    fn wasm_webhook_acl_empty_endpoint_panics() {
        build_wasm_policy(
            "proc",
            [],
            WasmAclsRaw {
                webhook: &[WebhookMatcherRaw {
                    endpoint: String::new(),
                }],
                ..Default::default()
            },
        );
    }

    // ---- build_surface_policy ----------------------------------------------

    use crate::messaging::config::SurfaceGrant;

    #[test]
    fn surface_grants_map_to_unified_capabilities() {
        // Every SurfaceGrant maps to its unified AppCapability directly (no
        // derivation): Subscribe→MessagingSubscribe, Publish→MessagingPublish,
        // EphemeralSubscribe→EphemeralSubscribe, EphemeralPublish→EphemeralPublish.
        let policy = build_surface_policy(
            "deskbar",
            [
                SurfaceGrant::Subscribe,
                SurfaceGrant::Publish,
                SurfaceGrant::EphemeralSubscribe,
                SurfaceGrant::EphemeralPublish,
            ],
            &[],
            &[],
            &[],
            &[],
        );
        assert!(policy.has_grant(AppCapability::MessagingSubscribe));
        assert!(policy.has_grant(AppCapability::MessagingPublish));
        assert!(policy.has_grant(AppCapability::EphemeralSubscribe));
        assert!(policy.has_grant(AppCapability::EphemeralPublish));
        // Ungranted capabilities remain denied.
        assert!(!policy.has_grant(AppCapability::DynamicSubscribe));
        assert!(!policy.has_grant(AppCapability::MqttPublish));
    }

    #[test]
    fn surface_alert_grant_maps_to_surface_alert_capability() {
        // SurfaceGrant::Alert maps to the surface-only AppCapability::SurfaceAlert
        // (distinct from WasmAlert), and carries no ACL — it is a pure capability
        // grant like the WASM alert host interface.
        let policy = build_surface_policy("deskbar", [SurfaceGrant::Alert], &[], &[], &[], &[]);
        assert!(policy.has_grant(AppCapability::SurfaceAlert));
        // It grants nothing else, and is not conflated with WasmAlert.
        assert!(!policy.has_grant(AppCapability::WasmAlert));
        assert!(!policy.has_grant(AppCapability::MessagingPublish));
    }

    #[test]
    fn surface_takeover_grant_maps_to_surface_takeover_capability() {
        // SurfaceGrant::Takeover maps to the surface-only
        // AppCapability::SurfaceTakeover (distinct from SurfaceAlert), carrying
        // no ACL — a pure capability grant like the alert plane.
        let policy = build_surface_policy("deskbar", [SurfaceGrant::Takeover], &[], &[], &[], &[]);
        assert!(policy.has_grant(AppCapability::SurfaceTakeover));
        // It grants nothing else, and is not conflated with the alert plane.
        assert!(!policy.has_grant(AppCapability::SurfaceAlert));
        assert!(!policy.has_grant(AppCapability::MessagingPublish));
    }

    #[test]
    #[should_panic(expected = "duplicate SurfaceGrant")]
    fn surface_duplicate_alert_grant_panics() {
        // Duplicate-grant rejection covers the new Alert grant too.
        build_surface_policy(
            "deskbar",
            [SurfaceGrant::Alert, SurfaceGrant::Alert],
            &[],
            &[],
            &[],
            &[],
        );
    }

    #[test]
    fn surface_subset_of_grants_only_sets_those() {
        // Explicit grants (not derived): granting only EphemeralSubscribe sets
        // exactly that capability, even though an ephemeral_subscribe_acl is
        // present — presence of a matcher list does NOT imply the grant (unlike
        // build_wasm_policy's subscribe_acl derivation).
        let policy = build_surface_policy(
            "deskbar",
            [SurfaceGrant::EphemeralSubscribe],
            &[],
            &[],
            &[ChannelMatcherRaw::Exact("protobar-demo".to_string())],
            &[],
        );
        assert!(policy.has_grant(AppCapability::EphemeralSubscribe));
        assert!(!policy.has_grant(AppCapability::MessagingSubscribe));
        assert!(!policy.has_grant(AppCapability::MessagingPublish));
        assert!(!policy.has_grant(AppCapability::EphemeralPublish));
    }

    #[test]
    fn surface_acls_resolve_to_matching_lists_and_authorize() {
        // The four ACL lists resolve onto brenn_subscribe/brenn_publish and
        // ephemeral_subscribe/ephemeral_publish respectively, with bare-name
        // matcher values; the two-factor delivery/publish
        // checks pass for a covered channel and deny an uncovered one.
        let policy = build_surface_policy(
            "deskbar",
            [
                SurfaceGrant::Subscribe,
                SurfaceGrant::Publish,
                SurfaceGrant::EphemeralSubscribe,
                SurfaceGrant::EphemeralPublish,
            ],
            &[ChannelMatcherRaw::Exact("alerts.high".to_string())],
            &[ChannelMatcherRaw::Exact("outbox".to_string())],
            &[ChannelMatcherRaw::Exact("protobar-demo".to_string())],
            &[ChannelMatcherRaw::Exact("telemetry".to_string())],
        );
        assert_eq!(
            policy.acls.brenn_subscribe,
            vec![ChannelMatcher::Exact("alerts.high".to_string())]
        );
        assert_eq!(
            policy.acls.brenn_publish,
            vec![ChannelMatcher::Exact("outbox".to_string())]
        );
        assert_eq!(
            policy.acls.ephemeral_subscribe,
            vec![ChannelMatcher::Exact("protobar-demo".to_string())]
        );
        assert_eq!(
            policy.acls.ephemeral_publish,
            vec![ChannelMatcher::Exact("telemetry".to_string())]
        );
        // MQTT/webhook transports are absent for a surface.
        assert!(policy.acls.mqtt_subscribe.is_empty());
        assert!(policy.acls.mqtt_publish.is_empty());
        assert!(policy.acls.webhook.is_empty());
        // Two-factor checks: grant + covering matcher authorize; miss denies.
        assert!(policy.allows_channel_access("brenn:alerts.high"));
        assert!(!policy.allows_channel_access("brenn:alerts.low"));
        assert!(policy.allows_brenn_publish("outbox"));
        assert!(!policy.allows_brenn_publish("other"));
        assert!(policy.allows_ephemeral_delivery("protobar-demo"));
        assert!(!policy.allows_ephemeral_delivery("other-demo"));
        assert!(policy.allows_ephemeral_publish("telemetry"));
        assert!(!policy.allows_ephemeral_publish("other"));
    }

    #[test]
    fn surface_empty_yields_default_deny() {
        // No grants, no ACLs ⇒ every right denied (deny-by-default).
        let policy = build_surface_policy("deskbar", [], &[], &[], &[], &[]);
        assert!(!policy.has_grant(AppCapability::MessagingSubscribe));
        assert!(!policy.has_grant(AppCapability::MessagingPublish));
        assert!(!policy.has_grant(AppCapability::EphemeralSubscribe));
        assert!(!policy.has_grant(AppCapability::EphemeralPublish));
        assert!(!policy.allows_channel_access("brenn:anything"));
        assert!(!policy.allows_ephemeral_delivery("anything"));
        assert!(policy.acls.brenn_subscribe.is_empty());
        assert!(policy.acls.ephemeral_subscribe.is_empty());
        assert!(policy.acls.ephemeral_publish.is_empty());
    }

    #[test]
    fn surface_grant_without_matchers_denies_by_default() {
        // A grant held with an empty matcher list ⇒ every channel denied
        // (`.any` over empty = false). Grant/ACL inconsistency is not a boot
        // panic — the two-factor check simply denies.
        let policy = build_surface_policy(
            "deskbar",
            [SurfaceGrant::EphemeralSubscribe],
            &[],
            &[],
            &[],
            &[],
        );
        assert!(policy.has_grant(AppCapability::EphemeralSubscribe));
        assert!(policy.acls.ephemeral_subscribe.is_empty());
        assert!(!policy.allows_ephemeral_delivery("protobar-demo"));
    }

    #[test]
    #[should_panic(expected = "duplicate SurfaceGrant")]
    fn surface_duplicate_grant_panics() {
        // Mirrors build_wasm_policy: this `pub` API accepts a raw iterator, so a
        // duplicate must fail fast rather than be silently absorbed.
        build_surface_policy(
            "deskbar",
            [SurfaceGrant::Subscribe, SurfaceGrant::Subscribe],
            &[],
            &[],
            &[],
            &[],
        );
    }

    #[test]
    #[should_panic(expected = "subscribe_acl prefix matcher")]
    fn surface_non_boundary_subscribe_prefix_panics() {
        build_surface_policy(
            "deskbar",
            [],
            &[ChannelMatcherRaw::Prefix("alert".to_string())],
            &[],
            &[],
            &[],
        );
    }

    #[test]
    #[should_panic(expected = "publish_acl exact matcher is empty")]
    fn surface_empty_publish_exact_panics() {
        build_surface_policy(
            "deskbar",
            [],
            &[],
            &[ChannelMatcherRaw::Exact(String::new())],
            &[],
            &[],
        );
    }

    #[test]
    #[should_panic(expected = "ephemeral_subscribe_acl prefix matcher")]
    fn surface_non_boundary_ephemeral_subscribe_prefix_panics() {
        build_surface_policy(
            "deskbar",
            [],
            &[],
            &[],
            &[ChannelMatcherRaw::Prefix("proto".to_string())],
            &[],
        );
    }

    #[test]
    #[should_panic(expected = "ephemeral_publish_acl exact matcher is empty")]
    fn surface_empty_ephemeral_publish_exact_panics() {
        build_surface_policy(
            "deskbar",
            [],
            &[],
            &[],
            &[],
            &[ChannelMatcherRaw::Exact(String::new())],
        );
    }
}

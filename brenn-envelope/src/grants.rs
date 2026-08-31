//! The capability vocabulary a component's `grants` list is written in.
//!
//! Single source of the words for every reader of them: the `.brenn`
//! configuration front end that parses them, the host that turns them into
//! wasmtime linker decisions, and the surface kernel that gates a page
//! component's privileged entries on them. All three name the same variants of
//! the same type, so a word cannot mean one thing on one side and another on
//! the other.
//!
//! A grant is one half of a statement. It names a capability; the ACL lists and
//! port bindings beside it say what that capability reaches. Deny-by-default in
//! both directions: an unlisted capability is absent, and a grant that reaches
//! nothing is refused as configuration nothing reads.
//!
//! Beside the two authored vocabularies sits [`AppCapability`], the unified
//! capability a resolved policy is written in. A grant word is what an operator
//! writes; an `AppCapability` is what every enforcement point asks about, and
//! the maps between them ([`ComponentGrant::app_capability`],
//! [`AttachGrant::app_capability`]) live here so no reader restates them.
//!
//! Two authored vocabularies live here, because there are two kinds of principal. A
//! *component* is code some host runs, and its grants name capabilities
//! ([`ComponentGrant`]). An *attacher* — a browser surface or a native daemon —
//! is a bus participant on the far end of a wire, and its grants name transport
//! rights ([`AttachGrant`]). The two attach-route principals hold one
//! vocabulary between them: a daemon and a page differ in how they
//! authenticate, not in which rights the wire can carry.
//!
//! Which grants a given host admits is stated here too, once, as
//! [`ComponentGrant::illegal_on`]: a page has no store, and a headless backend
//! consumer has no page to take over. It lives beside the words rather than in
//! the front end because three readers ask the question — the front end that
//! refuses the illegal word, and the two hosts that assert what they linked.

use serde::Deserialize;

use crate::ChannelScheme;

/// A capability a component may be granted, at either placement.
///
/// Most variants name a WIT interface in the component world, and a grant
/// selects whether that interface's host functions are linked at all.
/// [`ComponentGrant::Takeover`] is the exception: it names a page capability
/// with no interface behind it, gated at the binding instead.
///
/// Serde `kebab-case`, matching [`ComponentGrant::word`] — every word is the
/// interface name it links, spelled as WIT spells it, so the two spellings
/// cannot drift apart in shape and a test pins them equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ComponentGrant {
    /// `brenn:processor/ports` — publish and defer through the declared output
    /// ports.
    Ports,
    /// `brenn:processor/store` — KV store (also requires a store path in
    /// config).
    Store,
    /// `brenn:processor/log` — structured logging.
    Log,
    /// `brenn:processor/alert` — phone/operator alerting.
    Alert,
    /// `brenn:processor/config` — read-only operator config.
    Config,
    /// `brenn:processor/mqtt` — synchronous direct-to-broker MQTT publish.
    Mqtt,
    /// `brenn:processor/tools` — invoke registry tools, fast and async.
    Tools,
    /// Fullscreen takeover of the page the component is placed on. Names no WIT
    /// interface: the capability is a binding to a takeover-plane channel, and
    /// the grant is what consents to that binding.
    Takeover,
    /// `brenn:processor/dom` — read and mutate the instance's own element
    /// subtree, and wire gestures on it. Holding it is also what makes an
    /// instance mountable: an instance without it is headless.
    Dom,
    /// `brenn:processor/page-dom` — reach outside one's own subtree, into the
    /// surface root, the document body, and other instances' wrappers. A
    /// surface's designated chrome instance holds it and nothing else may.
    PageDom,
}

/// Where a component instance runs.
///
/// Not a second vocabulary: the words are one enum, and this selects which of
/// them a host can implement at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComponentHost {
    /// Placed on a surface, so it runs in the page.
    Surface,
    /// A top-level instance, so it runs in the backend host.
    TopLevel,
}

impl ComponentGrant {
    /// Every capability a component may be granted, in the order they are
    /// listed.
    pub const ALL: [ComponentGrant; 10] = [
        ComponentGrant::Ports,
        ComponentGrant::Store,
        ComponentGrant::Log,
        ComponentGrant::Alert,
        ComponentGrant::Config,
        ComponentGrant::Mqtt,
        ComponentGrant::Tools,
        ComponentGrant::Takeover,
        ComponentGrant::Dom,
        ComponentGrant::PageDom,
    ];

    /// The word this grant is written as, in configuration and on the wire.
    pub fn word(self) -> &'static str {
        match self {
            Self::Ports => "ports",
            Self::Store => "store",
            Self::Log => "log",
            Self::Alert => "alert",
            Self::Config => "config",
            Self::Mqtt => "mqtt",
            Self::Tools => "tools",
            Self::Takeover => "takeover",
            Self::Dom => "dom",
            Self::PageDom => "page-dom",
        }
    }

    /// The grant a word spells, or `None` when it spells none.
    pub fn parse(word: &str) -> Option<ComponentGrant> {
        Self::ALL.into_iter().find(|grant| grant.word() == word)
    }

    /// The WIT interface this grant links, at the host's version, or `None` for
    /// the one grant that links no interface.
    ///
    /// The single statement of the grant→import correspondence. Three readers
    /// need it and none of them may hold a table of its own: the host's linker
    /// gating, which decides whether an interface is added at all; the
    /// boot-time reflection check, which reads what an artifact actually
    /// imports; and the build-time parity check, which holds a specification's
    /// `requires` list equal to that same artifact's imports.
    ///
    /// `Takeover` is the exception for the reason it is everywhere else: it is
    /// consent to a binding, gated at the binding, with no interface behind it.
    ///
    /// The version suffix is the host's, so a reader comparing against a name
    /// scraped from an artifact built at a different version must strip both
    /// sides or match by semver rather than by bytes.
    pub fn wit_import(self) -> Option<&'static str> {
        Some(match self {
            Self::Ports => "brenn:processor/ports@0.1.0",
            Self::Store => "brenn:processor/store@0.1.0",
            Self::Log => "brenn:processor/log@0.1.0",
            Self::Alert => "brenn:processor/alert@0.1.0",
            Self::Config => "brenn:processor/config@0.1.0",
            Self::Mqtt => "brenn:processor/mqtt@0.1.0",
            Self::Tools => "brenn:processor/tools@0.1.0",
            Self::Dom => "brenn:processor/dom@0.1.0",
            Self::PageDom => "brenn:processor/page-dom@0.1.0",
            Self::Takeover => return None,
        })
    }

    /// Why this host cannot implement this capability, where it cannot.
    ///
    /// The one home for hosting legality, and deliberately the same split as
    /// the two hosts' WIT import lists (`SURFACE_IMPORTS` and `KNOWN_IMPORTS`
    /// in the surface server's processor-asset validation), modulo two deltas
    /// that side asserts by name: `types` is in both import lists and is no
    /// capability, and `takeover` names no import at all.
    pub fn illegal_on(self, host: ComponentHost) -> Option<&'static str> {
        match (host, self) {
            (ComponentHost::Surface, Self::Store) => Some(
                "`store` is backend-only in v1; a surface-hosted component cannot be granted it",
            ),
            (ComponentHost::Surface, Self::Mqtt) => Some(
                "`mqtt` is backend-only in v1; a surface-hosted component cannot be granted it",
            ),
            (ComponentHost::Surface, Self::Tools) => {
                Some("`tools` is backend-only in v1: the surface host links no tools interface")
            }
            (ComponentHost::TopLevel, Self::Takeover) => {
                Some("`takeover` is a page capability; a top-level consumer has no page")
            }
            (ComponentHost::TopLevel, Self::Dom) => {
                Some("`dom` is a page capability; a top-level consumer has no page to mutate")
            }
            (ComponentHost::TopLevel, Self::PageDom) => {
                Some("`page-dom` is a page capability; a top-level consumer has no page to arrange")
            }
            _ => None,
        }
    }

    /// The unified capability this grant becomes once a policy is built, or
    /// `None` for the one grant that names no capability.
    ///
    /// Four grants name none, for three reasons. `Takeover` is consent to a
    /// binding, gated at the binding, and no policy grant set carries it.
    /// `Tools` names authority the resolved tool-grant map carries in full, key
    /// by key, so there is no single capability it becomes. `Dom` and `PageDom`
    /// are gated in the page, by the kernel, on the grant word itself, and the
    /// backend policy vocabulary has no term for either. Whether a `None` here
    /// is a refusal or a skip belongs to the caller.
    pub fn app_capability(self) -> Option<AppCapability> {
        Some(match self {
            Self::Ports => AppCapability::MessagingPublish,
            Self::Store => AppCapability::WasmStore,
            Self::Log => AppCapability::WasmLog,
            Self::Alert => AppCapability::WasmAlert,
            Self::Config => AppCapability::WasmConfig,
            Self::Mqtt => AppCapability::MqttPublish,
            Self::Tools | Self::Takeover | Self::Dom | Self::PageDom => return None,
        })
    }
}

/// A transport right an attach-route principal may be granted.
///
/// One token per delivery class × direction, plus the alert plane. Every right
/// is named directly rather than derived from an ACL list's presence, so
/// deny-by-default reads straight off the config: a right is held iff its word
/// is written.
///
/// The vocabulary is shared by `[[surface]]` and `[[remote]]`. Both are
/// attach-route principals holding rights over the same two schemes, and the
/// rights a wire can carry do not depend on whether a browser or a daemon is at
/// the other end. Nothing page-shaped belongs here: what a page's components may
/// do *within* the page is a [`ComponentGrant`] on the component, not a right
/// over the wire.
///
/// Serde `snake_case`, matching [`AttachGrant::word`], so the multi-word
/// variants author as `ephemeral_subscribe`/`ephemeral_publish`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachGrant {
    /// Durable (`brenn:`) delivery to the attacher.
    Subscribe,
    /// Durable (`brenn:`) publish from the attacher.
    Publish,
    /// Ephemeral (`ephemeral:`) delivery to the attacher.
    EphemeralSubscribe,
    /// Ephemeral (`ephemeral:`) publish from the attacher.
    EphemeralPublish,
    /// Alert (phone/operator paging) emission from the attacher.
    Alert,
}

impl AttachGrant {
    /// Every transport right an attacher may be granted, in the order they are
    /// listed.
    pub const ALL: [AttachGrant; 5] = [
        AttachGrant::Subscribe,
        AttachGrant::Publish,
        AttachGrant::EphemeralSubscribe,
        AttachGrant::EphemeralPublish,
        AttachGrant::Alert,
    ];

    /// The word this grant is written as, in configuration.
    pub fn word(self) -> &'static str {
        match self {
            Self::Subscribe => "subscribe",
            Self::Publish => "publish",
            Self::EphemeralSubscribe => "ephemeral_subscribe",
            Self::EphemeralPublish => "ephemeral_publish",
            Self::Alert => "alert",
        }
    }

    /// The grant a word spells, or `None` when it spells none.
    pub fn parse(word: &str) -> Option<AttachGrant> {
        Self::ALL.into_iter().find(|grant| grant.word() == word)
    }

    /// The plane and delivery class this right names, or `None` for the one
    /// right that names no transport.
    ///
    /// `Alert` is that one: paging is an egress side channel, not a bus scheme.
    pub fn transport(self) -> Option<(Plane, ChannelScheme)> {
        Some(match self {
            Self::Subscribe => (Plane::Subscribe, ChannelScheme::Brenn),
            Self::Publish => (Plane::Publish, ChannelScheme::Brenn),
            Self::EphemeralSubscribe => (Plane::Subscribe, ChannelScheme::Ephemeral),
            Self::EphemeralPublish => (Plane::Publish, ChannelScheme::Ephemeral),
            Self::Alert => return None,
        })
    }

    /// The unified capability this right becomes once a policy is built. Total:
    /// every attach right has one, `Alert` included.
    pub fn app_capability(self) -> AppCapability {
        match self {
            Self::Subscribe => AppCapability::MessagingSubscribe,
            Self::Publish => AppCapability::MessagingPublish,
            Self::EphemeralSubscribe => AppCapability::EphemeralSubscribe,
            Self::EphemeralPublish => AppCapability::EphemeralPublish,
            Self::Alert => AppCapability::SurfaceAlert,
        }
    }
}

/// The two planes an authority statement names.
///
/// Envelope vocabulary rather than front-end vocabulary because transport shape
/// — which plane a right reaches, over which delivery class — is answered here,
/// by [`AppCapability::transport`] and [`AttachGrant::transport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Plane {
    Subscribe,
    Publish,
}

impl Plane {
    /// Both planes, in the order a diagnostic lists them.
    pub const ALL: [Plane; 2] = [Plane::Subscribe, Plane::Publish];

    /// The word this plane is written as.
    pub fn word(self) -> &'static str {
        match self {
            Plane::Subscribe => "subscribe",
            Plane::Publish => "publish",
        }
    }

    /// The plane a word spells, or `None` when it spells none.
    pub fn parse(word: &str) -> Option<Plane> {
        Self::ALL.into_iter().find(|plane| plane.word() == word)
    }
}

/// Which entity holds an authority, and with it which families it has at all.
///
/// The principal kinds the bus authorizes: the two attach-route participants, an
/// LLM agent, and a component instance at either placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityKind {
    Surface,
    Agent,
    /// A component instance, wherever it was placed. One kind for both hosts:
    /// what a component is given, and what it may be authorized to reach, is
    /// the same question on a surface and at top level. The host it runs on
    /// rides along because a few answers are the host's — which capabilities it
    /// can implement, and which schemes it can attach to.
    Component(ComponentHost),
    Remote,
}

impl EntityKind {
    /// What this kind is called in a diagnostic.
    ///
    /// A component keeps the name its placement gave it — the language calls a
    /// top-level instance a consumer, and an operator reading a refusal is
    /// looking at one word or the other in their own document.
    pub fn label(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Agent => "agent",
            Self::Component(ComponentHost::Surface) => "component",
            Self::Component(ComponentHost::TopLevel) => "consumer",
            Self::Remote => "remote",
        }
    }
}

/// The schemes an entity of this kind may bind a port to, on this plane.
///
/// Envelope vocabulary because two sides ask it: the configuration front end,
/// which refuses an inadmissible binding with a diagnostic, and the boot
/// builders, which panic on one. One table, two failure modes.
///
/// Not the same question as which ACL families an entity holds, and it deviates
/// from that in exactly three places, each deliberate:
///
/// - a surface may bind a `local:` channel although it holds no local family —
///   the page it is served to authorizes those frames, out of band;
/// - a wasm consumer holds an `mqtt_publish` family but names no `mqtt:` output —
///   its egress to a broker is its own block;
/// - an agent holds publish families but states no outbound position at all — it
///   publishes through the tools its grants admit.
///
/// Everywhere else this is the family table read through the plane.
pub fn bindable_schemes(kind: EntityKind, plane: Plane) -> &'static [ChannelScheme] {
    match (kind, plane) {
        (EntityKind::Surface, _) => &[
            ChannelScheme::Brenn,
            ChannelScheme::Ephemeral,
            ChannelScheme::Local,
        ],
        (EntityKind::Component(ComponentHost::TopLevel), Plane::Subscribe) => &[
            ChannelScheme::Brenn,
            ChannelScheme::Ephemeral,
            ChannelScheme::Local,
            ChannelScheme::Webhook,
            ChannelScheme::Mqtt,
        ],
        // A surface-placed instance attaches through the page, which reaches no
        // broker and no endpoint.
        (EntityKind::Component(ComponentHost::Surface), _)
        | (EntityKind::Component(ComponentHost::TopLevel), Plane::Publish) => &[
            ChannelScheme::Brenn,
            ChannelScheme::Ephemeral,
            ChannelScheme::Local,
        ],
        (EntityKind::Agent, Plane::Subscribe) => &[
            ChannelScheme::Brenn,
            ChannelScheme::Ephemeral,
            ChannelScheme::Webhook,
            ChannelScheme::Mqtt,
        ],
        // An agent states no outbound position: it publishes through the tools
        // its grants admit, against the authority its `acl publish` states.
        (EntityKind::Agent, Plane::Publish) | (EntityKind::Remote, _) => &[],
    }
}

/// Layer 1: the coarse, binary capabilities an app may be granted.
///
/// This is the **unified** capability enum spanning LLM conversations and WASM
/// components. The full variant set is defined now so later phases need not
/// widen it. Named `AppCapability` — **not** `Capability` — to avoid colliding
/// with the DSL's own narrower `Capability`, which spans the words an entity of
/// any kind may hold.
///
/// `Ord` is derived so a `GrantSet` (a `BTreeSet<AppCapability>`) iterates in a
/// stable order once a later phase's logging needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
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
    /// delivery class explicitly.
    EphemeralSubscribe,
    /// Publish to a confined `local:` channel. A distinct transport grant from
    /// `MessagingPublish`/`EphemeralPublish`: each delivery class is gated
    /// separately. A `local:` channel is process-local and non-transportable,
    /// but a backend publisher still needs authorization to reach one.
    LocalPublish,
    /// Hold a subscription to a confined `local:` channel. A distinct transport
    /// grant from `MessagingSubscribe`/`EphemeralSubscribe`: each delivery class
    /// is gated separately. LLM-app-unauthorable in v1 (no local delivery path
    /// to a conversation).
    LocalSubscribe,
    // external transports
    /// Publish to MQTT.
    MqttPublish,
    /// Subscribe to MQTT.
    MqttSubscribe,
    /// Hold an inbound webhook subscription.
    Webhook,
    /// Receive PWA push notifications (no per-channel scope).
    PwaPush,
    // carried authority
    /// Set `impetus` on a published message — the claim that live user
    /// interaction produced it. Orthogonal to channel ACL: a minting principal
    /// still needs publish authority over its target, and a publish carrying
    /// impetus without this grant is refused whole rather than stripped.
    ///
    /// TODO(chat-surface-mints-impetus): nothing in production mints yet, so a
    /// conversation's impetus pool is refilled only by the legacy websocket
    /// door. A user who reaches their conversations over the bus alone will see
    /// them exhaust and stall with no bus-side remedy. The chat surface is the
    /// first minter: it authors the grant mapping, carries the field on its
    /// publish frames, and derives it from a real user gesture.
    MintImpetus,
    // WASM host capabilities. Authored as `ComponentGrant` on WASM components and
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
    /// Attacher: alert emission. Authored as `AttachGrant::Alert` on a
    /// `[[surface]]` or `[[remote]]` and mapped internally (not part of the LLM
    /// `grants` token vocabulary). A capability distinct from `WasmAlert` so
    /// policy inspection keeps alert-grant provenance per boundary.
    SurfaceAlert,
    /// Integration access (pfin, graf, …). A bare variant with no associated
    /// `IntegrationKind` payload: the payload and its enforcement are reserved
    /// for a later phase. Bare so the token list deserializes from plain strings.
    Integration,
}

impl AppCapability {
    /// Every capability, in declaration order.
    ///
    /// Declaration order is what the derived vocabulary tables iterate, so it
    /// is the order tokens are written out in; the front end's expansion rows
    /// are pinned against it.
    pub const ALL: [AppCapability; 18] = [
        AppCapability::MessagingPublish,
        AppCapability::MessagingSubscribe,
        AppCapability::DynamicSubscribe,
        AppCapability::EphemeralPublish,
        AppCapability::EphemeralSubscribe,
        AppCapability::LocalPublish,
        AppCapability::LocalSubscribe,
        AppCapability::MqttPublish,
        AppCapability::MqttSubscribe,
        AppCapability::Webhook,
        AppCapability::PwaPush,
        AppCapability::MintImpetus,
        AppCapability::WasmStore,
        AppCapability::WasmLog,
        AppCapability::WasmAlert,
        AppCapability::WasmConfig,
        AppCapability::SurfaceAlert,
        AppCapability::Integration,
    ];

    /// The word this capability is written as in configuration.
    ///
    /// Equal to the serde spelling, which a test pins: config documents
    /// deserialize these words, so the two spellings are one contract and
    /// nothing may consume `word` that is not held to it.
    pub fn word(self) -> &'static str {
        match self {
            Self::MessagingPublish => "messaging_publish",
            Self::MessagingSubscribe => "messaging_subscribe",
            Self::DynamicSubscribe => "dynamic_subscribe",
            Self::EphemeralPublish => "ephemeral_publish",
            Self::EphemeralSubscribe => "ephemeral_subscribe",
            Self::LocalPublish => "local_publish",
            Self::LocalSubscribe => "local_subscribe",
            Self::MqttPublish => "mqtt_publish",
            Self::MqttSubscribe => "mqtt_subscribe",
            Self::Webhook => "webhook",
            Self::PwaPush => "pwa_push",
            Self::MintImpetus => "mint_impetus",
            Self::WasmStore => "wasm_store",
            Self::WasmLog => "wasm_log",
            Self::WasmAlert => "wasm_alert",
            Self::WasmConfig => "wasm_config",
            Self::SurfaceAlert => "surface_alert",
            Self::Integration => "integration",
        }
    }

    /// The capability a word spells, or `None` when it spells none.
    pub fn parse(word: &str) -> Option<AppCapability> {
        Self::ALL.into_iter().find(|cap| cap.word() == word)
    }

    /// Whether an LLM conversation's `grants` list may hold this capability,
    /// and why not where it may not.
    ///
    /// The whole enum derives `Deserialize`, so every word parses from any
    /// `grants` list; this method is the single authority on which of them are
    /// valid on an LLM app. The `Err` text is a human-readable reason for the
    /// refusal, suitable for panic messages and diagnostic output.
    pub fn llm_authorable(self) -> Result<(), &'static str> {
        match self {
            Self::MessagingPublish
            | Self::MessagingSubscribe
            | Self::DynamicSubscribe
            | Self::EphemeralPublish
            | Self::EphemeralSubscribe
            | Self::LocalPublish
            | Self::MqttPublish
            | Self::MqttSubscribe
            | Self::Webhook
            | Self::PwaPush => Ok(()),
            Self::WasmStore | Self::WasmLog | Self::WasmAlert | Self::WasmConfig => {
                Err("WASM-host caps are authored as `ComponentGrant` on a component")
            }
            Self::Integration => Err("`integration` is reserved"),
            Self::SurfaceAlert => Err(
                "attacher alert is authored as `AttachGrant` on a `[[surface]]` or `[[remote]]`",
            ),
            Self::MintImpetus => Err("no LLM-reachable API sets impetus"),
            Self::LocalSubscribe => Err("no local delivery path to a conversation in v1"),
        }
    }

    /// The plane and delivery class this capability names, or `None` for the
    /// nine that name no transport.
    ///
    /// Transport *shape*, not authority: [`AppCapability::LocalSubscribe`] has a
    /// shape here and is refused by [`AppCapability::llm_authorable`], and the
    /// two answers are deliberately independent.
    pub fn transport(self) -> Option<(Plane, ChannelScheme)> {
        Some(match self {
            Self::MessagingPublish => (Plane::Publish, ChannelScheme::Brenn),
            Self::MessagingSubscribe => (Plane::Subscribe, ChannelScheme::Brenn),
            Self::EphemeralPublish => (Plane::Publish, ChannelScheme::Ephemeral),
            Self::EphemeralSubscribe => (Plane::Subscribe, ChannelScheme::Ephemeral),
            Self::LocalPublish => (Plane::Publish, ChannelScheme::Local),
            Self::LocalSubscribe => (Plane::Subscribe, ChannelScheme::Local),
            Self::MqttPublish => (Plane::Publish, ChannelScheme::Mqtt),
            Self::MqttSubscribe => (Plane::Subscribe, ChannelScheme::Mqtt),
            Self::Webhook => (Plane::Subscribe, ChannelScheme::Webhook),
            Self::DynamicSubscribe
            | Self::PwaPush
            | Self::MintImpetus
            | Self::WasmStore
            | Self::WasmLog
            | Self::WasmAlert
            | Self::WasmConfig
            | Self::SurfaceAlert
            | Self::Integration => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_word_parses_back_to_its_grant() {
        for grant in ComponentGrant::ALL {
            assert_eq!(
                ComponentGrant::parse(grant.word()),
                Some(grant),
                "{grant:?} does not round-trip through its word"
            );
        }
    }

    #[test]
    fn all_holds_every_variant_once() {
        let mut words: Vec<&str> = ComponentGrant::ALL.iter().map(|g| g.word()).collect();
        let count = words.len();
        words.sort_unstable();
        words.dedup();
        assert_eq!(words.len(), count, "ALL lists a grant twice: {words:?}");
    }

    #[test]
    fn serde_spelling_is_the_word() {
        // The two spellings are separately declared (`rename_all` vs the `word`
        // match), and a config document authored against one must deserialize
        // through the other.
        for grant in ComponentGrant::ALL {
            let json = format!("\"{}\"", grant.word());
            let parsed: ComponentGrant =
                serde_json::from_str(&json).expect("word must deserialize");
            assert_eq!(parsed, grant);
        }
    }

    #[test]
    fn every_grant_is_legal_on_exactly_one_host_or_both() {
        // The table is a pair of rows over one vocabulary, so a word must be
        // legal somewhere: a grant no host can implement is a word with no
        // meaning rather than a placement rule.
        for grant in ComponentGrant::ALL {
            assert!(
                grant.illegal_on(ComponentHost::Surface).is_none()
                    || grant.illegal_on(ComponentHost::TopLevel).is_none(),
                "{grant:?} is legal on no host"
            );
        }
    }

    #[test]
    fn an_unknown_word_spells_no_grant() {
        assert_eq!(ComponentGrant::parse("Ports"), None);
        assert_eq!(ComponentGrant::parse(""), None);
    }
    #[test]
    fn every_capability_word_parses_back() {
        for cap in AppCapability::ALL {
            assert_eq!(
                AppCapability::parse(cap.word()),
                Some(cap),
                "{cap:?} does not round-trip through its word"
            );
        }
    }

    #[test]
    fn capability_all_lists_no_capability_twice() {
        let mut words: Vec<&str> = AppCapability::ALL.iter().map(|c| c.word()).collect();
        let count = words.len();
        words.sort_unstable();
        words.dedup();
        assert_eq!(
            words.len(),
            count,
            "ALL lists a capability twice: {words:?}"
        );
    }

    /// The arm list is exhaustive: a new variant stops compilation until it is
    /// written down here, next to the `ALL` row it also needs.
    fn assert_capability_listed(cap: AppCapability) {
        match cap {
            AppCapability::MessagingPublish
            | AppCapability::MessagingSubscribe
            | AppCapability::DynamicSubscribe
            | AppCapability::EphemeralPublish
            | AppCapability::EphemeralSubscribe
            | AppCapability::LocalPublish
            | AppCapability::LocalSubscribe
            | AppCapability::MqttPublish
            | AppCapability::MqttSubscribe
            | AppCapability::Webhook
            | AppCapability::PwaPush
            | AppCapability::MintImpetus
            | AppCapability::WasmStore
            | AppCapability::WasmLog
            | AppCapability::WasmAlert
            | AppCapability::WasmConfig
            | AppCapability::SurfaceAlert
            | AppCapability::Integration => {}
        }
        assert!(
            AppCapability::ALL.contains(&cap),
            "AppCapability::ALL is missing {cap:?}"
        );
    }

    fn assert_component_grant_listed(grant: ComponentGrant) {
        match grant {
            ComponentGrant::Ports
            | ComponentGrant::Store
            | ComponentGrant::Log
            | ComponentGrant::Alert
            | ComponentGrant::Config
            | ComponentGrant::Mqtt
            | ComponentGrant::Tools
            | ComponentGrant::Takeover
            | ComponentGrant::Dom
            | ComponentGrant::PageDom => {}
        }
        assert!(
            ComponentGrant::ALL.contains(&grant),
            "ComponentGrant::ALL is missing {grant:?}"
        );
    }

    fn assert_attach_grant_listed(grant: AttachGrant) {
        match grant {
            AttachGrant::Subscribe
            | AttachGrant::Publish
            | AttachGrant::EphemeralSubscribe
            | AttachGrant::EphemeralPublish
            | AttachGrant::Alert => {}
        }
        assert!(
            AttachGrant::ALL.contains(&grant),
            "AttachGrant::ALL is missing {grant:?}"
        );
    }

    fn assert_plane_listed(plane: Plane) {
        match plane {
            Plane::Subscribe | Plane::Publish => {}
        }
        assert!(
            Plane::ALL.contains(&plane),
            "Plane::ALL is missing {plane:?}"
        );
    }

    #[test]
    fn every_all_is_walked_by_its_exhaustive_guard() {
        for cap in AppCapability::ALL {
            assert_capability_listed(cap);
        }
        for grant in ComponentGrant::ALL {
            assert_component_grant_listed(grant);
        }
        for grant in AttachGrant::ALL {
            assert_attach_grant_listed(grant);
        }
        for plane in Plane::ALL {
            assert_plane_listed(plane);
        }
    }

    #[test]
    fn capability_serde_spelling_is_the_word() {
        // `word` is consumed as the config spelling by the front end's grant
        // tables; a config document is deserialized through `rename_all`. The
        // two are separately declared and must agree for every variant.
        for cap in AppCapability::ALL {
            let json = format!("\"{}\"", cap.word());
            let parsed: AppCapability = serde_json::from_str(&json).expect("word deserializes");
            assert_eq!(parsed, cap);
        }
    }

    #[test]
    fn ten_capabilities_are_llm_authorable() {
        let authorable: Vec<&str> = AppCapability::ALL
            .iter()
            .filter(|cap| cap.llm_authorable().is_ok())
            .map(|cap| cap.word())
            .collect();
        assert_eq!(
            authorable,
            [
                "messaging_publish",
                "messaging_subscribe",
                "dynamic_subscribe",
                "ephemeral_publish",
                "ephemeral_subscribe",
                "local_publish",
                "mqtt_publish",
                "mqtt_subscribe",
                "webhook",
                "pwa_push",
            ]
        );
        assert_eq!(
            AppCapability::ALL
                .iter()
                .filter(|cap| cap.llm_authorable().is_err())
                .count(),
            8
        );
    }

    #[test]
    fn nine_capabilities_are_transport_shaped() {
        let shaped: Vec<(&str, &str, ChannelScheme)> = AppCapability::ALL
            .iter()
            .filter_map(|cap| {
                cap.transport()
                    .map(|(plane, scheme)| (cap.word(), plane.word(), scheme))
            })
            .collect();
        assert_eq!(
            shaped,
            [
                ("messaging_publish", "publish", ChannelScheme::Brenn),
                ("messaging_subscribe", "subscribe", ChannelScheme::Brenn),
                ("ephemeral_publish", "publish", ChannelScheme::Ephemeral),
                ("ephemeral_subscribe", "subscribe", ChannelScheme::Ephemeral),
                ("local_publish", "publish", ChannelScheme::Local),
                ("local_subscribe", "subscribe", ChannelScheme::Local),
                ("mqtt_publish", "publish", ChannelScheme::Mqtt),
                ("mqtt_subscribe", "subscribe", ChannelScheme::Mqtt),
                ("webhook", "subscribe", ChannelScheme::Webhook),
            ]
        );
    }

    #[test]
    fn transport_shape_and_authorability_are_independent() {
        // `local_subscribe` is the pair that proves it: a real plane and scheme,
        // and no LLM app may hold it. A reader that fused the two answers would
        // drop the row from the transport table.
        assert_eq!(
            AppCapability::LocalSubscribe.transport(),
            Some((Plane::Subscribe, ChannelScheme::Local))
        );
        assert!(AppCapability::LocalSubscribe.llm_authorable().is_err());
    }

    #[test]
    fn eight_words_spell_an_agents_transport_rights() {
        // The filter the front end's agent vocabulary is built from. Pinned here
        // because the eight compound tokens it yields are what an operator sees
        // refused by name.
        let agent: Vec<&str> = AppCapability::ALL
            .iter()
            .filter(|cap| cap.transport().is_some() && cap.llm_authorable().is_ok())
            .map(|cap| cap.word())
            .collect();
        assert_eq!(
            agent,
            [
                "messaging_publish",
                "messaging_subscribe",
                "ephemeral_publish",
                "ephemeral_subscribe",
                "local_publish",
                "mqtt_publish",
                "mqtt_subscribe",
                "webhook",
            ]
        );
    }

    #[test]
    fn four_attach_grants_are_transport_shaped() {
        let shaped: Vec<(&str, &str, ChannelScheme)> = AttachGrant::ALL
            .iter()
            .filter_map(|grant| {
                grant
                    .transport()
                    .map(|(plane, scheme)| (grant.word(), plane.word(), scheme))
            })
            .collect();
        assert_eq!(
            shaped,
            [
                ("subscribe", "subscribe", ChannelScheme::Brenn),
                ("publish", "publish", ChannelScheme::Brenn),
                ("ephemeral_subscribe", "subscribe", ChannelScheme::Ephemeral),
                ("ephemeral_publish", "publish", ChannelScheme::Ephemeral),
            ]
        );
        assert_eq!(AttachGrant::Alert.transport(), None);
    }

    #[test]
    fn attach_grants_map_to_distinct_capabilities() {
        // Injective, which is what lets the policy builders treat a repeated
        // capability as a repeated grant.
        let mut caps: Vec<AppCapability> = AttachGrant::ALL
            .iter()
            .map(|grant| grant.app_capability())
            .collect();
        let count = caps.len();
        caps.sort_unstable();
        caps.dedup();
        assert_eq!(caps.len(), count);
    }

    #[test]
    fn four_component_grants_name_no_capability_and_the_rest_name_one() {
        for grant in ComponentGrant::ALL {
            let mapped = grant.app_capability();
            match grant {
                ComponentGrant::Takeover
                | ComponentGrant::Tools
                | ComponentGrant::Dom
                | ComponentGrant::PageDom => assert_eq!(mapped, None),
                _ => assert!(mapped.is_some(), "{grant:?} names no capability"),
            }
        }
    }

    /// Every grant but one links an interface, and the one that does not is
    /// `Takeover` — the same exception `app_capability` makes, for the same
    /// reason. A word added to the vocabulary without an import decision would
    /// pass the compiler (the match is exhaustive but a new arm can return
    /// `None` by hand) and silently leave the build-time parity check unable to
    /// see it, so the totality is asserted rather than assumed.
    #[test]
    fn every_grant_but_takeover_links_an_interface() {
        for grant in ComponentGrant::ALL {
            let import = grant.wit_import();
            if grant == ComponentGrant::Takeover {
                assert_eq!(import, None, "takeover names no interface");
                continue;
            }
            let import = import.unwrap_or_else(|| panic!("{grant:?} links no interface"));
            assert_eq!(
                import,
                format!("brenn:processor/{}@0.1.0", grant.word()),
                "{grant:?}'s interface is not named for its word"
            );
        }
    }

    /// Two grants naming one interface would make the parity check's set
    /// comparison lossy in one direction: a spec requiring either word would
    /// satisfy an artifact importing the shared interface.
    #[test]
    fn no_two_grants_link_the_same_interface() {
        let mut imports: Vec<&str> = ComponentGrant::ALL
            .iter()
            .filter_map(|grant| grant.wit_import())
            .collect();
        let count = imports.len();
        imports.sort_unstable();
        imports.dedup();
        assert_eq!(imports.len(), count, "two grants share an interface");
    }

    #[test]
    fn tools_is_a_backend_only_word() {
        assert!(
            ComponentGrant::Tools
                .illegal_on(ComponentHost::Surface)
                .is_some()
        );
        assert!(
            ComponentGrant::Tools
                .illegal_on(ComponentHost::TopLevel)
                .is_none()
        );
    }

    /// The inverse of `tools`: two words a page host links and a backend host
    /// refuses. Pinned by name because they are the first of their direction,
    /// and the import-list parity check on the other side of the seam carries a
    /// deviation class that exists only for them.
    #[test]
    fn dom_and_page_dom_are_surface_only_words() {
        for grant in [ComponentGrant::Dom, ComponentGrant::PageDom] {
            assert!(
                grant.illegal_on(ComponentHost::Surface).is_none(),
                "{grant:?} is refused on the only host that can implement it"
            );
            assert!(
                grant.illegal_on(ComponentHost::TopLevel).is_some(),
                "{grant:?} is admitted on a host with no page"
            );
        }
    }

    #[test]
    fn both_planes_round_trip_through_their_words() {
        for plane in Plane::ALL {
            assert_eq!(Plane::parse(plane.word()), Some(plane));
        }
        assert_eq!(Plane::parse("takeover"), None);
    }

    #[test]
    fn every_attach_word_parses_back_to_its_grant() {
        for grant in AttachGrant::ALL {
            assert_eq!(
                AttachGrant::parse(grant.word()),
                Some(grant),
                "{grant:?} does not round-trip through its word"
            );
        }
    }

    #[test]
    fn attach_all_holds_every_variant_once() {
        let mut words: Vec<&str> = AttachGrant::ALL.iter().map(|g| g.word()).collect();
        let count = words.len();
        words.sort_unstable();
        words.dedup();
        assert_eq!(words.len(), count, "ALL lists a grant twice: {words:?}");
    }

    #[test]
    fn attach_serde_spelling_is_the_word() {
        for grant in AttachGrant::ALL {
            let json = format!("\"{}\"", grant.word());
            let parsed: AttachGrant = serde_json::from_str(&json).expect("word must deserialize");
            assert_eq!(parsed, grant);
        }
    }

    #[test]
    fn an_attacher_states_no_page_capability() {
        // The two vocabularies overlap on `alert` and nowhere else. `takeover`
        // in particular is a page capability a component holds; an attacher
        // spelling it is naming a right the wire does not carry.
        assert_eq!(AttachGrant::parse("takeover"), None);
        assert_eq!(AttachGrant::parse("ports"), None);
        assert_eq!(AttachGrant::parse("store"), None);
    }

    /// Every kind × plane row, spelled out. A row edited in the table without
    /// intent fails here, which is the whole point of a shared table.
    #[test]
    fn bindable_rows_are_what_each_kind_attaches_through() {
        use ChannelScheme::{Brenn, Ephemeral, Local, Mqtt, Webhook};
        let pubsub: &[ChannelScheme] = &[Brenn, Ephemeral, Local];
        for plane in Plane::ALL {
            assert_eq!(bindable_schemes(EntityKind::Surface, plane), pubsub);
            assert_eq!(
                bindable_schemes(EntityKind::Component(ComponentHost::Surface), plane),
                pubsub,
            );
            assert_eq!(
                bindable_schemes(EntityKind::Remote, plane),
                &[] as &[ChannelScheme]
            );
        }
        assert_eq!(
            bindable_schemes(
                EntityKind::Component(ComponentHost::TopLevel),
                Plane::Subscribe
            ),
            &[Brenn, Ephemeral, Local, Webhook, Mqtt],
        );
        assert_eq!(
            bindable_schemes(
                EntityKind::Component(ComponentHost::TopLevel),
                Plane::Publish
            ),
            pubsub,
        );
        assert_eq!(
            bindable_schemes(EntityKind::Agent, Plane::Subscribe),
            &[Brenn, Ephemeral, Webhook, Mqtt],
        );
        assert_eq!(
            bindable_schemes(EntityKind::Agent, Plane::Publish),
            &[] as &[ChannelScheme],
        );
    }

    /// The three deviations the table's doc names, asserted rather than
    /// described: a surface binds `local:` with no local family; a top-level
    /// consumer names no `mqtt:` output although it may publish to a broker; an
    /// agent binds nothing outbound at all.
    #[test]
    fn the_three_deviations_from_the_family_table_hold() {
        assert!(
            bindable_schemes(EntityKind::Surface, Plane::Subscribe).contains(&ChannelScheme::Local)
        );
        assert!(
            !bindable_schemes(
                EntityKind::Component(ComponentHost::TopLevel),
                Plane::Publish
            )
            .contains(&ChannelScheme::Mqtt)
        );
        assert!(bindable_schemes(EntityKind::Agent, Plane::Publish).is_empty());
    }

    /// No row admits `pwa_push:`: it is an egress target a policy names, never
    /// an address a port is bound to.
    #[test]
    fn no_kind_binds_a_push_target() {
        for kind in [
            EntityKind::Surface,
            EntityKind::Agent,
            EntityKind::Remote,
            EntityKind::Component(ComponentHost::Surface),
            EntityKind::Component(ComponentHost::TopLevel),
        ] {
            for plane in Plane::ALL {
                assert!(!bindable_schemes(kind, plane).contains(&ChannelScheme::PwaPush));
            }
        }
    }

    #[test]
    fn a_component_is_labelled_by_its_placement() {
        assert_eq!(
            EntityKind::Component(ComponentHost::Surface).label(),
            "component"
        );
        assert_eq!(
            EntityKind::Component(ComponentHost::TopLevel).label(),
            "consumer"
        );
        assert_eq!(EntityKind::Agent.label(), "agent");
        assert_eq!(EntityKind::Surface.label(), "surface");
        assert_eq!(EntityKind::Remote.label(), "remote");
    }
}

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
//! Two vocabularies live here, because there are two kinds of principal. A
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

/// A capability a component may be granted, at either placement.
///
/// Most variants name a WIT interface in the component world, and a grant
/// selects whether that interface's host functions are linked at all.
/// [`ComponentGrant::Takeover`] is the exception: it names a page capability
/// with no interface behind it, gated at the binding instead.
///
/// Serde `lowercase`, matching [`ComponentGrant::word`] — every variant is one
/// word, so the two spellings cannot drift apart in shape, and a test pins them
/// equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
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
    /// Fullscreen takeover of the page the component is placed on. Names no WIT
    /// interface: the capability is a binding to a takeover-plane channel, and
    /// the grant is what consents to that binding.
    Takeover,
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
    pub const ALL: [ComponentGrant; 7] = [
        ComponentGrant::Ports,
        ComponentGrant::Store,
        ComponentGrant::Log,
        ComponentGrant::Alert,
        ComponentGrant::Config,
        ComponentGrant::Mqtt,
        ComponentGrant::Takeover,
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
            Self::Takeover => "takeover",
        }
    }

    /// The grant a word spells, or `None` when it spells none.
    pub fn parse(word: &str) -> Option<ComponentGrant> {
        Self::ALL.into_iter().find(|grant| grant.word() == word)
    }

    /// Why this host cannot implement this capability, where it cannot.
    ///
    /// The one home for hosting legality, and deliberately the same split as
    /// the two hosts' WIT import lists (`SURFACE_IMPORTS` and `KNOWN_IMPORTS`
    /// in the surface server's processor-asset validation), modulo three deltas
    /// that side asserts by name: `types` is in both import lists and is no
    /// capability, `takeover` names no import at all, and `tools` is derived
    /// from a connection rather than declared as a word.
    pub fn illegal_on(self, host: ComponentHost) -> Option<&'static str> {
        match (host, self) {
            (ComponentHost::Surface, Self::Store) => Some(
                "`store` is backend-only in v1; a surface-hosted component cannot be granted it",
            ),
            (ComponentHost::Surface, Self::Mqtt) => Some(
                "`mqtt` is backend-only in v1; a surface-hosted component cannot be granted it",
            ),
            (ComponentHost::TopLevel, Self::Takeover) => {
                Some("`takeover` is a page capability; a top-level consumer has no page")
            }
            _ => None,
        }
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
        assert_eq!(ComponentGrant::parse("tools"), None);
        assert_eq!(ComponentGrant::parse("Ports"), None);
        assert_eq!(ComponentGrant::parse(""), None);
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
}

//! The derived model: what a document means once authority, identity and the
//! channel model are concrete.
//!
//! A [`DerivedConfig`] wraps the resolved one rather than replacing it: nothing
//! resolution established is restated here, so there is one home for each fact
//! and no chance of the two disagreeing. What derivation adds rides beside it in
//! vectors parallel to the resolved ones, indexed the same way — a
//! [`crate::resolved::ChanId`] indexes `channel_uuids` exactly as it indexes
//! `channels`.
//!
//! Every vector's parallelism is asserted at construction rather than documented
//! and hoped for: a later pass reading position `i` of one and position `i` of
//! the other is the whole contract, and a length mismatch is a bug that would
//! otherwise surface as a wrong answer.

use fltk_cst_core::Span;
use fltk_serde_core::Spanned;
use uuid::Uuid;

use crate::resolved::ResolvedConfig;

/// A whole configuration, derived.
#[derive(Debug, PartialEq)]
pub struct DerivedConfig {
    pub resolved: ResolvedConfig,
    /// Parallel to `resolved.surfaces`.
    pub surfaces: Vec<DAuthority>,
    /// Parallel to `resolved.consumers`.
    pub consumers: Vec<DAuthority>,
    /// Parallel to `resolved.agents`.
    pub agents: Vec<DAuthority>,
    /// Parallel to `resolved.remotes`.
    pub remotes: Vec<DRemoteAuthority>,
    /// Parallel to `resolved.channels`; `Some` exactly for durable (`brenn:`)
    /// declarations. A non-durable channel carries no configured identity — the
    /// runtime derives one from its address — so lowering omits the field.
    pub channel_uuids: Vec<Option<Uuid>>,
    /// Parallel to `resolved.surfaces`; the inner vector parallel to that
    /// surface's `components`. The wire kind each instance's class folds to.
    pub surface_component_kinds: Vec<Vec<String>>,
}

/// Every entity's authority, in the four vectors the derived config holds them
/// in.
///
/// One argument rather than four: the vectors are built together by one pass and
/// a positional swap between two same-typed ones would hand a surface an agent's
/// rights.
#[derive(Debug, Default, PartialEq)]
pub struct DAuthorities {
    pub surfaces: Vec<DAuthority>,
    pub consumers: Vec<DAuthority>,
    pub agents: Vec<DAuthority>,
    pub remotes: Vec<DRemoteAuthority>,
}

/// What one principal may reach, by family.
#[derive(Debug, Default, PartialEq)]
pub struct DAuthority {
    /// The rights the entity states, in the spellings the runtime's own config
    /// uses: a plane word expanded into one token per scheme it has entries on,
    /// then the capability words as written. The span is the word that produced
    /// the token, so an expanded token points at the plane word it came from.
    pub grants: Vec<Spanned<String>>,
    pub acl: DAclSet,
}

/// One field per family the runtime keeps a list for anywhere.
///
/// Every entity type gets the whole struct and fills the families it has;
/// derivation refuses a statement naming a family its entity type lacks, so the
/// rest stay empty rather than carrying rights nothing reads.
#[derive(Debug, Default, PartialEq)]
pub struct DAclSet {
    pub brenn_subscribe: Vec<DMatcher>,
    pub brenn_publish: Vec<DMatcher>,
    pub ephemeral_subscribe: Vec<DMatcher>,
    pub ephemeral_publish: Vec<DMatcher>,
    pub local_subscribe: Vec<DMatcher>,
    pub local_publish: Vec<DMatcher>,
    pub mqtt_subscribe: Vec<DMqttSub>,
    pub mqtt_publish: Vec<DMqttClient>,
    pub webhook: Vec<DWebhook>,
}

/// One channel-family entry.
///
/// The pattern is bare — the scheme is stripped, because the list it lands in is
/// what says which scheme it is about, and that is how the runtime's own config
/// spells it. The span is where the entry came from: the matcher that was
/// written, or the binding that derived it.
#[derive(Debug, PartialEq)]
pub enum DMatcher {
    Exact(Spanned<String>),
    Prefix(Spanned<String>),
}

impl DMatcher {
    /// Where the entry came from.
    pub fn span(&self) -> &Span {
        match self {
            DMatcher::Exact(pattern) | DMatcher::Prefix(pattern) => pattern.span(),
        }
    }

    /// The bare pattern, whichever kind this is.
    pub fn pattern(&self) -> &str {
        match self {
            DMatcher::Exact(pattern) | DMatcher::Prefix(pattern) => pattern.value(),
        }
    }
}

/// One inbound MQTT entry: a topic filter, scoped to the client that carries it.
#[derive(Debug, PartialEq)]
pub struct DMqttSub {
    pub client: Spanned<String>,
    pub topic_filter: Spanned<String>,
}

/// One outbound MQTT entry. Publish is client-scoped: there is no topic
/// dimension to narrow.
#[derive(Debug, PartialEq)]
pub struct DMqttClient {
    pub client: Spanned<String>,
}

/// One inbound webhook entry, by endpoint slug.
#[derive(Debug, PartialEq)]
pub struct DWebhook {
    pub endpoint: Spanned<String>,
}

/// A remote's authority.
///
/// Its own shape because a remote is the one principal whose subscribe-side
/// entries carry ceilings: a network peer states how deep a subscription it may
/// hold, since it has no `channel` block of its own to inherit from.
#[derive(Debug, Default, PartialEq)]
pub struct DRemoteAuthority {
    /// As [`DAuthority::grants`].
    pub grants: Vec<Spanned<String>>,
    pub subscribe: Vec<DRemoteSubEntry>,
    pub ephemeral_subscribe: Vec<DRemoteSubEntry>,
    pub publish: Vec<DMatcher>,
    pub ephemeral_publish: Vec<DMatcher>,
}

/// One remote subscribe entry: a matcher and the depths a matching subscription
/// is capped at.
///
/// Plain counts, never the word for an unbounded window: an unbounded queue is
/// not an answer a network principal may be given.
#[derive(Debug, PartialEq)]
pub struct DRemoteSubEntry {
    pub m: DMatcher,
    pub push_depth: u64,
    pub retain_depth: u64,
}

impl DerivedConfig {
    /// Assemble a derived config, checking every parallel vector against what it
    /// is parallel to.
    pub fn new(
        resolved: ResolvedConfig,
        channel_uuids: Vec<Option<Uuid>>,
        authorities: DAuthorities,
        surface_component_kinds: Vec<Vec<String>>,
    ) -> DerivedConfig {
        assert_eq!(
            channel_uuids.len(),
            resolved.channels.len(),
            "one derived identity per declared channel"
        );
        assert_eq!(
            authorities.surfaces.len(),
            resolved.surfaces.len(),
            "one authority per surface"
        );
        assert_eq!(
            authorities.consumers.len(),
            resolved.consumers.len(),
            "one authority per consumer"
        );
        assert_eq!(
            authorities.agents.len(),
            resolved.agents.len(),
            "one authority per agent"
        );
        assert_eq!(
            authorities.remotes.len(),
            resolved.remotes.len(),
            "one authority per remote"
        );
        assert_eq!(
            surface_component_kinds.len(),
            resolved.surfaces.len(),
            "one kind list per surface"
        );
        for (surface, kinds) in resolved.surfaces.iter().zip(&surface_component_kinds) {
            assert_eq!(
                kinds.len(),
                surface.components.len(),
                "one folded kind per component instance of `{}`",
                surface.handle.dotted()
            );
        }
        let DAuthorities {
            surfaces,
            consumers,
            agents,
            remotes,
        } = authorities;
        DerivedConfig {
            resolved,
            surfaces,
            consumers,
            agents,
            remotes,
            channel_uuids,
            surface_component_kinds,
        }
    }
}
